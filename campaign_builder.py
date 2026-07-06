"""Campaign Builder — Phase 2 (P2).

A thin authoring/compiler layer on top of the existing voucher drop engine
in ``vouchers.py``. It does NOT reimplement claim logic, FCFS allocation,
eligibility evaluation, anti-abuse, the scheduler, or affiliate settlement.

Architecture:

    Campaign (draft, this module)
        -> Campaign Compiler (this module: compile_campaign)
        -> existing Voucher Drop(s) (vouchers.create_drop_from_spec)
        -> vouchers.py executes normally (claim/eligibility/scheduler untouched)

Collection: ``campaign_builder_campaigns`` (deliberately NOT named
``campaigns`` — that collection already exists and is owned by the legacy
segment-targeting engine in campaigns.py/campaign_engine.py, which the
"do not remove existing campaign dashboards" hard rule requires we keep
working exactly as-is. Sharing one collection between two different
document schemas would corrupt that dashboard's listing/filtering, so P2
uses its own collection instead.)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request
from pymongo import ASCENDING, DESCENDING

import database
from config import (
    BOT_SEGMENT_PROBABILITY_MAP,
    KL_TZ,
    SEGMENT_PROBABILITY_CONFIG,
)
from campaign_engine import VALID_SEGMENTS, preview_audience

logger = logging.getLogger(__name__)

campaign_builder_bp = Blueprint("campaign_builder", __name__)

FEATURE_VERSION = "P2"
COLLECTION_NAME = "campaign_builder_campaigns"

# ---------------------------------------------------------------------------
# Wizard vocabulary
# ---------------------------------------------------------------------------

CAMPAIGN_TYPES = [
    "smart_default",
    "public",
    "welcome",
    "segment",
    "affiliate",
    "personalised",
    "fcfs",
    "surprise",
    "test",
]

AUDIENCE_MODES = [
    "smart_segment_pct",
    "equal_chance",
    "no_segment_filter",
    "whitelist",
    "vip",
    "region",
    "admin_only",
]

RELEASE_STYLES = ["immediate", "scheduled"]

REWARD_TYPES = [
    "voucher_pool",
    "personalised_voucher",
    "affiliate_reward_pool",
    "xp",
    "combined",
]

CAMPAIGN_STATUSES = ["draft", "compiled", "active", "archived"]

# Template presets: what Steps 2-4 default to when a template is picked.
# These are only defaults for the wizard UI — every field can be overridden
# by the admin before compiling, except where noted (`locked`).
TEMPLATE_DEFAULTS = {
    "smart_default": {
        "audience_mode": "smart_segment_pct",
        "release_style": "immediate",
        "reward_type": "voucher_pool",
        "notes": "Uses the existing segment probability defaults from config.py as-is.",
    },
    "public": {
        "audience_mode": "no_segment_filter",
        "release_style": "immediate",
        "reward_type": "voucher_pool",
        "notes": "Plain public pooled drop, no restriction.",
    },
    "welcome": {
        "audience_mode": "no_segment_filter",
        "release_style": "immediate",
        "reward_type": "voucher_pool",
        "locked": True,
        "notes": "Reuses the existing new_joiner welcome-voucher shortcut untouched.",
    },
    "segment": {
        "audience_mode": "smart_segment_pct",
        "release_style": "immediate",
        "reward_type": "voucher_pool",
        "notes": "Restricts to chosen backend segments via resolved user-id whitelist.",
    },
    "affiliate": {
        "audience_mode": "whitelist",
        "release_style": "immediate",
        "reward_type": "affiliate_reward_pool",
        "notes": "Targets an admin-supplied affiliate-referred username list. Does not touch affiliate settlement.",
    },
    "personalised": {
        "audience_mode": "whitelist",
        "release_style": "immediate",
        "reward_type": "personalised_voucher",
        "locked": True,
        "notes": "1:1 assignment, identical to today's assignments path.",
    },
    "fcfs": {
        "audience_mode": "no_segment_filter",
        "release_style": "immediate",
        "reward_type": "voucher_pool",
        "notes": "Plain pooled drop; FCFS is a UI label, not new claim behavior.",
    },
    "surprise": {
        "audience_mode": "smart_segment_pct",
        "release_style": "scheduled",
        "reward_type": "voucher_pool",
        "notes": "P2 supports only Immediate/Scheduled release; drop stays 'upcoming' until startsAt via the existing scheduler sweep.",
    },
    "test": {
        "audience_mode": "admin_only",
        "release_style": "immediate",
        "reward_type": "voucher_pool",
        "locked": True,
        "notes": "eligibility.mode=admin_only — hidden from normal users, safe for real claim testing.",
    },
}

# The Smart Default segment probability table shown to the admin at Preview
# time is READ from config.py's single source of truth. It is never written
# by this module. (Note: config.py's `ghost` bot-segment default is 0.05, not
# the 0.30 figure sometimes quoted in product docs — this is a pre-existing
# discrepancy in config.py, out of scope for the campaign builder to silently
# "fix"; we display whatever config.py actually contains.)


def _require_admin():
    from vouchers import require_admin
    return require_admin()


def _col():
    return database.db[COLLECTION_NAME]


def _ensure_indexes() -> None:
    try:
        col = _col()
        col.create_index([("status", ASCENDING)], name="ix_cb_status")
        col.create_index([("created_at", DESCENDING)], name="ix_cb_created_at")
        col.create_index([("campaign_type", ASCENDING)], name="ix_cb_campaign_type")
    except Exception:
        logger.warning("[CAMPAIGN_BUILDER] Failed to create indexes", exc_info=True)


_ensure_indexes()


# ---------------------------------------------------------------------------
# Audience resolution helpers (read-only against existing collections)
# ---------------------------------------------------------------------------

def _resolve_segment_user_ids(db, segments: list[str]) -> list[int]:
    """Resolve backend segment names to user_ids using the latest snapshot week.

    Read-only against backend_segment_snapshots (already populated by
    backend_segment_engine.py). Does not modify eligibility evaluation code —
    the resulting ids are written as eligibility.mode="user_id" allow list,
    an enforcement path vouchers.py already implements.
    """
    valid = [s for s in segments if s in VALID_SEGMENTS]
    if not valid:
        return []
    snapshots_col = db["backend_segment_snapshots"]
    latest = snapshots_col.find_one(
        {"user_id": {"$ne": None}},
        sort=[("snapshot_week", -1)],
        projection={"snapshot_week": 1, "_id": 0},
    )
    if not latest:
        return []
    week = latest["snapshot_week"]
    cursor = snapshots_col.find(
        {"snapshot_week": week, "backend_segment": {"$in": valid}, "user_id": {"$ne": None}},
        projection={"user_id": 1, "_id": 0},
    )
    ids = []
    seen = set()
    for doc in cursor:
        uid = doc.get("user_id")
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            continue
        if uid not in seen:
            seen.add(uid)
            ids.append(uid)
    return ids


def _resolve_usernames_to_user_ids(db, usernames: list[str]) -> tuple[list[int], list[str]]:
    """Resolve @usernames to numeric user_ids via the users collection.

    Returns (resolved_ids, unresolved_usernames).
    """
    from vouchers import norm_username

    normed = []
    seen = set()
    for u in usernames or []:
        n = norm_username(u)
        if n and n not in seen:
            seen.add(n)
            normed.append(n)
    if not normed:
        return [], []

    users_col = db["users"]
    found = {}
    cursor = users_col.find(
        {"usernameLower": {"$in": normed}},
        projection={"usernameLower": 1, "user_id": 1, "_id": 0},
    )
    for doc in cursor:
        ul = doc.get("usernameLower")
        uid = doc.get("user_id")
        if ul and uid is not None:
            try:
                found[ul] = int(uid)
            except (TypeError, ValueError):
                continue

    resolved = [found[u] for u in normed if u in found]
    unresolved = [u for u in normed if u not in found]
    return resolved, unresolved


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------

def _now_kl_str() -> str:
    return datetime.now(KL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _default_duration_hours(campaign_type: str) -> int:
    return 24


def _build_audience(campaign_doc: dict, db) -> tuple[dict, dict, list[str]]:
    """Resolve Step-2 audience choice into (eligibility, audience, warnings).

    Reuses only evaluators vouchers.py already enforces:
      - eligibility.mode="user_id" + allow=[ids]   (restrictive whitelist)
      - eligibility.mode="tier" + allow=["VIP"]     (existing tier check)
      - eligibility.mode="admin_only"               (existing admin gate)
      - audience.regions=[...]                      (existing region check)
      - eligibility.mode="public" (default)          (existing default; segment
        probability weighting at claim time is applied automatically via the
        existing assign_public_pool_access_once / BOT_SEGMENT_PROBABILITY_MAP,
        unchanged by this module)
    """
    warnings: list[str] = []
    mode = campaign_doc.get("audience_mode") or "no_segment_filter"
    params = campaign_doc.get("audience_params") or {}

    eligibility: dict = {"mode": "public"}
    audience: dict = {}

    if mode in ("smart_segment_pct", "no_segment_filter"):
        pass  # public, no restriction; smart % is existing claim-time behavior
    elif mode == "equal_chance":
        warnings.append(
            "Equal Chance is currently a reporting label only: claim-time "
            "assignment still uses the existing segment probability tables. "
            "Bypassing segment-derived probability would require a new "
            "optional parameter on assign_public_pool_access_once, which is "
            "out of scope for P2 (claim engine is not to be modified)."
        )
    elif mode == "whitelist":
        usernames = params.get("usernames") or []
        ids, unresolved = _resolve_usernames_to_user_ids(db, usernames)
        if unresolved:
            warnings.append(f"Could not resolve {len(unresolved)} username(s) to a user_id: {unresolved[:10]}")
        if ids:
            eligibility = {"mode": "user_id", "allow": ids}
        else:
            warnings.append("Whitelist resolved to 0 users — drop will effectively allow no one until usernames are fixed.")
            eligibility = {"mode": "user_id", "allow": []}
    elif mode == "vip":
        tier = (params.get("tier") or "VIP").strip() or "VIP"
        eligibility = {"mode": "tier", "allow": [tier]}
    elif mode == "region":
        regions = [str(r).strip() for r in (params.get("regions") or []) if str(r).strip()]
        if not regions:
            warnings.append("No region selected — audience.regions is empty, drop will not be region-restricted.")
        audience["regions"] = regions
    elif mode == "admin_only":
        eligibility = {"mode": "admin_only"}
    else:
        warnings.append(f"Unknown audience_mode '{mode}', defaulting to public.")

    return eligibility, audience, warnings


def _build_reward(campaign_doc: dict) -> tuple[str, dict, list[str]]:
    """Resolve Step-4 reward choice into (drop_type, reward_fields, warnings)."""
    warnings: list[str] = []
    reward_type = campaign_doc.get("reward_type") or "voucher_pool"
    params = campaign_doc.get("reward_params") or {}
    fields: dict = {}

    if reward_type == "personalised_voucher":
        assignments = params.get("assignments") or []
        fields["assignments"] = assignments
        if not assignments:
            warnings.append("No assignments provided for personalised voucher reward.")
        return "personalised", fields, warnings

    if reward_type in ("voucher_pool", "affiliate_reward_pool", "combined"):
        codes = params.get("codes") or []
        fields["codes"] = codes
        fields["pool"] = params.get("pool") if params.get("pool") in ("public", "my") else "public"
        if not codes:
            warnings.append("No voucher codes provided.")
        if reward_type == "combined" and not params.get("xp_amount"):
            warnings.append("Combined reward selected but no xp_amount set — only the voucher pool will be compiled (XP grants require a resolved, finite audience; see audience_mode).")
        return "pooled", fields, warnings

    if reward_type == "xp":
        warnings.append("XP-only reward selected — no voucher drop is generated. Grant XP via the existing Add/Reduce XP admin tool for the resolved audience; this compiler only produces voucher drops.")
        return "none", fields, warnings

    warnings.append(f"Unknown reward_type '{reward_type}', defaulting to voucher_pool.")
    fields["codes"] = params.get("codes") or []
    fields["pool"] = "public"
    return "pooled", fields, warnings


def preview_campaign(campaign_doc: dict) -> dict:
    """Step-5 Preview: safety checks, expected drop count, estimated reach.

    Read-only. Does not write anything.
    """
    db = database.db
    warnings: list[str] = []

    eligibility, audience, audience_warnings = _build_audience(campaign_doc, db)
    warnings.extend(audience_warnings)
    drop_type, reward_fields, reward_warnings = _build_reward(campaign_doc)
    warnings.extend(reward_warnings)

    mode = campaign_doc.get("audience_mode") or "no_segment_filter"
    params = campaign_doc.get("audience_params") or {}

    # Expected number of drops the compiler would generate.
    segment_names: list[str] = []
    if campaign_doc.get("campaign_type") == "segment" or mode == "segment":
        segment_names = [s for s in (params.get("segments") or []) if s in VALID_SEGMENTS]
    expected_drop_count = max(1, len(segment_names))

    # Estimated reach.
    estimated_reach = 0
    segment_distribution: dict = {}
    if eligibility.get("mode") == "user_id":
        estimated_reach = len(eligibility.get("allow") or [])
    elif segment_names:
        ids_per_segment = {seg: len(_resolve_segment_user_ids(db, [seg])) for seg in segment_names}
        estimated_reach = sum(ids_per_segment.values())
        segment_distribution = ids_per_segment
    else:
        preview = preview_audience(db, {}, 0.0)
        estimated_reach = preview.get("audience_size", 0)
        segment_distribution = {
            seg: data.get("count", 0)
            for seg, data in (preview.get("segment_distribution") or {}).items()
        }

    expected_voucher_count = 0
    if drop_type == "personalised":
        expected_voucher_count = len(reward_fields.get("assignments") or [])
    elif drop_type == "pooled":
        expected_voucher_count = len(reward_fields.get("codes") or []) * expected_drop_count

    release_style = campaign_doc.get("release_style") or "immediate"
    release_params = campaign_doc.get("release_params") or {}

    smart_default_table = {
        "bot_segment_probability": dict(BOT_SEGMENT_PROBABILITY_MAP),
        "backend_segment_probability_pct": dict(SEGMENT_PROBABILITY_CONFIG),
    }

    return {
        "campaign_name": campaign_doc.get("campaign_name"),
        "campaign_type": campaign_doc.get("campaign_type"),
        "audience_mode": mode,
        "reward_type": campaign_doc.get("reward_type"),
        "release_style": release_style,
        "expected_drop_count": expected_drop_count,
        "expected_drop_names": (
            [f"{_slug(campaign_doc.get('campaign_name'))}_{i:03d}" for i in range(1, expected_drop_count + 1)]
            if expected_drop_count > 1
            else [_slug(campaign_doc.get("campaign_name"))]
        ),
        "estimated_reach": estimated_reach,
        "segment_distribution": segment_distribution,
        "expected_voucher_count": expected_voucher_count,
        "safety_checks": {
            "estimated_audience": estimated_reach,
            "segment_distribution": segment_distribution,
            "reward_count": expected_voucher_count,
            "campaign_duration_hours": _release_duration_hours(release_style, release_params),
            "estimated_voucher_usage_pct": (
                round(100.0 * expected_voucher_count / estimated_reach, 1)
                if estimated_reach and drop_type == "pooled"
                else None
            ),
            "generated_drop_count": expected_drop_count,
        },
        "smart_default_reference": smart_default_table,
        "warnings": warnings,
    }


def _release_duration_hours(release_style: str, release_params: dict) -> float | None:
    starts = release_params.get("startsAtLocal")
    ends = release_params.get("endsAtLocal")
    if not starts:
        return 24.0
    try:
        from vouchers import parse_kl_local
        s = parse_kl_local(starts)
        e = parse_kl_local(ends) if ends else s + timedelta(hours=24)
        return round((e - s).total_seconds() / 3600.0, 1)
    except Exception:
        return None


def _slug(name: str | None) -> str:
    name = (name or "campaign").strip()
    out = "".join(ch if ch.isalnum() else "_" for ch in name)
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_") or "campaign"


def compile_campaign(campaign_doc: dict) -> tuple[dict, int]:
    """Compile a draft campaign into one or more existing voucher drops.

    This is the ONLY write path from Campaign Builder into the voucher
    engine, and it writes exclusively through
    vouchers.create_drop_from_spec — the same insert primitive
    admin_create_drop uses. No claim, eligibility, scheduler, or affiliate
    settlement code is touched.
    """
    from vouchers import create_drop_from_spec

    db = database.db
    if campaign_doc.get("status") != "draft":
        return {"status": "error", "code": "not_draft"}, 400

    eligibility, audience, audience_warnings = _build_audience(campaign_doc, db)
    drop_type, reward_fields, reward_warnings = _build_reward(campaign_doc)

    if drop_type == "none":
        return {
            "status": "error",
            "code": "no_drop_reward",
            "warnings": audience_warnings + reward_warnings,
        }, 400

    mode = campaign_doc.get("audience_mode") or "no_segment_filter"
    params = campaign_doc.get("audience_params") or {}
    segment_names: list[str] = []
    if campaign_doc.get("campaign_type") == "segment" or mode == "segment":
        segment_names = [s for s in (params.get("segments") or []) if s in VALID_SEGMENTS]

    release_style = campaign_doc.get("release_style") or "immediate"
    release_params = campaign_doc.get("release_params") or {}
    if release_style == "scheduled" and release_params.get("startsAtLocal"):
        starts_at_local = release_params["startsAtLocal"]
    else:
        starts_at_local = _now_kl_str()
    ends_at_local = release_params.get("endsAtLocal")

    campaign_name = campaign_doc.get("campaign_name") or "Campaign"
    campaign_type = campaign_doc.get("campaign_type") or "smart_default"
    slug = _slug(campaign_name)

    specs: list[dict] = []
    drop_segment_map: list[str] = []
    if segment_names:
        for i, seg in enumerate(segment_names, start=1):
            seg_ids = _resolve_segment_user_ids(db, [seg])
            spec = _base_drop_spec(
                name=f"{slug}_{i:03d}",
                starts_at_local=starts_at_local,
                ends_at_local=ends_at_local,
                campaign_type=campaign_type,
                drop_type=drop_type,
                eligibility={"mode": "user_id", "allow": seg_ids},
                audience=audience,
                reward_fields=reward_fields,
            )
            specs.append(spec)
            drop_segment_map.append(seg)
    else:
        spec = _base_drop_spec(
            name=campaign_name,
            starts_at_local=starts_at_local,
            ends_at_local=ends_at_local,
            campaign_type=campaign_type,
            drop_type=drop_type,
            eligibility=eligibility,
            audience=audience,
            reward_fields=reward_fields,
        )
        if campaign_type == "welcome":
            spec["eligibility"] = {"mode": "new_joiner"}
            # Let create_drop_from_spec's new_joiner shortcut set
            # campaign_type="welcome_voucher" itself (existing welcome
            # recognition marker) instead of overwriting it with our own
            # "welcome" template id.
            spec.pop("campaign_type", None)
        specs.append(spec)

    compiled_drop_ids: list[str] = []
    drop_errors: list[dict] = []
    for spec in specs:
        result, code = create_drop_from_spec(spec)
        if code == 200 and result.get("status") == "ok":
            drop_id = result["dropId"]
            compiled_drop_ids.append(drop_id)
            try:
                # Note: campaign_type is deliberately NOT overwritten here —
                # create_drop_from_spec already set it (with the welcome
                # shortcut's "welcome_voucher" override intact where
                # applicable), and vouchers.py reads that field to recognize
                # welcome drops. Re-setting it here would silently break that
                # recognition for Welcome campaigns.
                db.drops.update_one(
                    {"_id": ObjectId(drop_id)},
                    {"$set": {
                        "campaign_id": str(campaign_doc.get("_id") or campaign_doc.get("id") or ""),
                        "campaign_name": campaign_name,
                    }},
                )
            except Exception:
                logger.warning("[CAMPAIGN_BUILDER] Failed to tag drop %s with campaign metadata", drop_id, exc_info=True)
        else:
            drop_errors.append({"spec_name": spec.get("name"), "error": result})

    now = datetime.now(timezone.utc)
    new_status = "active" if compiled_drop_ids else "draft"
    update = {
        "status": new_status,
        "compiled_drop_ids": compiled_drop_ids,
        "updated_at": now,
    }
    if compiled_drop_ids:
        update["launched_at"] = now

    _col().update_one({"_id": campaign_doc["_id"]}, {"$set": update})

    if drop_errors and not compiled_drop_ids:
        return {"status": "error", "code": "compile_failed", "errors": drop_errors}, 400

    return {
        "status": "ok",
        "compiled_drop_ids": compiled_drop_ids,
        "drop_segment_map": dict(zip(compiled_drop_ids, drop_segment_map)) if drop_segment_map else {},
        "errors": drop_errors,
        "warnings": audience_warnings + reward_warnings,
    }, 200


def _base_drop_spec(
    *,
    name: str,
    starts_at_local: str,
    ends_at_local: str | None,
    campaign_type: str,
    drop_type: str,
    eligibility: dict,
    audience: dict,
    reward_fields: dict,
) -> dict:
    spec: dict = {
        "name": name,
        "type": drop_type,
        "startsAtLocal": starts_at_local,
        "priority": 100,
        "eligibility": eligibility,
        "campaign_type": campaign_type,
    }
    if ends_at_local:
        spec["endsAtLocal"] = ends_at_local
    if audience.get("regions") is not None:
        spec["audience"] = {"regions": audience["regions"]}
    if drop_type == "personalised":
        spec["assignments"] = reward_fields.get("assignments") or []
    else:
        spec["codes"] = reward_fields.get("codes") or []
        spec["pool"] = reward_fields.get("pool") or "public"
    return spec


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

def _serialize(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for key in ("created_at", "updated_at", "launched_at"):
        if out.get(key):
            out[key] = out[key].isoformat()
    return out


@campaign_builder_bp.route("/api/admin/campaign-builder/meta", methods=["GET"])
def get_meta():
    _, err = _require_admin()
    if err:
        return err
    return jsonify({
        "status": "ok",
        "campaign_types": CAMPAIGN_TYPES,
        "audience_modes": AUDIENCE_MODES,
        "release_styles": RELEASE_STYLES,
        "reward_types": REWARD_TYPES,
        "template_defaults": TEMPLATE_DEFAULTS,
        "valid_segments": sorted(VALID_SEGMENTS),
        "smart_default_reference": {
            "bot_segment_probability": dict(BOT_SEGMENT_PROBABILITY_MAP),
            "backend_segment_probability_pct": dict(SEGMENT_PROBABILITY_CONFIG),
        },
    })


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns", methods=["GET"])
def list_campaigns():
    _, err = _require_admin()
    if err:
        return err
    status_filter = (request.args.get("status") or "").strip()
    query: dict = {}
    if status_filter and status_filter in CAMPAIGN_STATUSES:
        query["status"] = status_filter
    elif not status_filter:
        query["status"] = {"$ne": "archived"}
    docs = list(_col().find(query, sort=[("created_at", DESCENDING)], limit=200))
    return jsonify({"status": "ok", "campaigns": [_serialize(d) for d in docs]})


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns", methods=["POST"])
def create_campaign():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True) or {}
    campaign_name = (body.get("campaign_name") or "").strip()
    if not campaign_name:
        return jsonify({"status": "error", "code": "missing_campaign_name"}), 400

    campaign_type = (body.get("campaign_type") or "smart_default").strip()
    if campaign_type not in CAMPAIGN_TYPES:
        return jsonify({"status": "error", "code": "invalid_campaign_type"}), 400

    defaults = TEMPLATE_DEFAULTS.get(campaign_type, {})
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_name": campaign_name,
        "campaign_type": campaign_type,
        "status": "draft",
        "audience_mode": body.get("audience_mode") or defaults.get("audience_mode", "no_segment_filter"),
        "audience_params": body.get("audience_params") or {},
        "release_style": body.get("release_style") or defaults.get("release_style", "immediate"),
        "release_params": body.get("release_params") or {},
        "reward_type": body.get("reward_type") or defaults.get("reward_type", "voucher_pool"),
        "reward_params": body.get("reward_params") or {},
        "compiled_drop_ids": [],
        "created_at": now,
        "updated_at": now,
        "created_by": admin.get("usernameLower") or str(admin.get("id", "")),
        "feature_version": FEATURE_VERSION,
    }
    result = _col().insert_one(doc)
    doc["_id"] = result.inserted_id
    return jsonify({"status": "ok", "campaign": _serialize(doc)}), 201


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>", methods=["GET"])
def get_campaign(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    doc = _col().find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok", "campaign": _serialize(doc)})


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>", methods=["PUT"])
def update_campaign(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400

    existing = _col().find_one({"_id": oid})
    if not existing:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if existing.get("status") != "draft":
        return jsonify({"status": "error", "code": "not_draft"}), 400

    body = request.get_json(force=True) or {}
    updates: dict = {"updated_at": datetime.now(timezone.utc)}

    if "campaign_name" in body:
        name = (body["campaign_name"] or "").strip()
        if not name:
            return jsonify({"status": "error", "code": "missing_campaign_name"}), 400
        updates["campaign_name"] = name

    if "campaign_type" in body:
        ct = (body["campaign_type"] or "").strip()
        if ct not in CAMPAIGN_TYPES:
            return jsonify({"status": "error", "code": "invalid_campaign_type"}), 400
        updates["campaign_type"] = ct

    for field in ("audience_mode", "release_style", "reward_type"):
        if field in body:
            updates[field] = body[field]

    for field in ("audience_params", "release_params", "reward_params"):
        if field in body:
            updates[field] = body[field] or {}

    _col().update_one({"_id": oid}, {"$set": updates})
    doc = _col().find_one({"_id": oid})
    return jsonify({"status": "ok", "campaign": _serialize(doc)})


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>", methods=["DELETE"])
def delete_campaign(campaign_id):
    """Rollback: delete the campaign document only. Never touches generated
    voucher drops — those remain and keep functioning exactly like any
    manually-created drop (see docs rollback plan)."""
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result = _col().delete_one({"_id": oid})
    if result.deleted_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/preview", methods=["POST"])
def preview_campaign_route(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    doc = _col().find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok", "preview": preview_campaign(doc)})


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/compile", methods=["POST"])
def compile_campaign_route(campaign_id):
    """Launch: requires the literal confirmation text 'LAUNCH'."""
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400

    body = request.get_json(force=True) or {}
    confirm = (body.get("confirm") or "").strip()
    if confirm != "LAUNCH":
        return jsonify({"status": "error", "code": "confirmation_required", "expected": "LAUNCH"}), 400

    doc = _col().find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    result, status_code = compile_campaign(doc)
    return jsonify(result), status_code
