"""Campaign Builder — Phase 2 (P2) + Phase 3 (P3) Batch Release.

A thin authoring/compiler layer on top of the existing voucher drop engine
in ``vouchers.py``. It does NOT reimplement claim logic, FCFS allocation,
eligibility evaluation, anti-abuse, the scheduler, or affiliate settlement.

Architecture:

    Campaign (draft, this module)
        -> Campaign Compiler (this module: compile_campaign / compile_batch_campaign)
        -> existing Voucher Drop(s) (vouchers.create_drop_from_spec)
        -> vouchers.py executes normally (claim/eligibility/scheduler untouched)

Collection: ``campaign_builder_campaigns`` (deliberately NOT named
``campaigns`` — that collection already exists and is owned by the legacy
segment-targeting engine in campaigns.py/campaign_engine.py, which the
"do not remove existing campaign dashboards" hard rule requires we keep
working exactly as-is. Sharing one collection between two different
document schemas would corrupt that dashboard's listing/filtering, so P2
uses its own collection instead.)

P3 adds batch release campaigns on top of the same collection (additive
fields only, see ``RELEASE_TYPES``/``BATCH_STATUSES`` below): one parent
campaign compiles into many *child* voucher drops released over time
(every X minutes / hourly / daily / weekly / manual / custom schedule).
Each child drop is created through the exact same
``vouchers.create_drop_from_spec`` primitive P2 uses — no parallel claim
ledger, no new drop-insert path. Release timing is enforced by flipping a
child drop's own ``status`` between ``paused`` (not live) and whatever
``create_drop_from_spec``/the existing scheduler would have set it to —
the scheduler's ``reconcile_drop_statuses`` sweep already leaves
``paused`` drops alone, so P3 does not touch scheduler core.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone

from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request
from pymongo import ASCENDING, DESCENDING, ReturnDocument

import database
from config import (
    BOT_SEGMENT_PROBABILITY_MAP,
    KL_TZ,
    SEGMENT_PROBABILITY_CONFIG,
)
from campaign_engine import VALID_SEGMENTS, preview_audience
from effective_segment import effective_segment_query_for_segments

logger = logging.getLogger(__name__)

campaign_builder_bp = Blueprint("campaign_builder", __name__)

FEATURE_VERSION = "P2"
COLLECTION_NAME = "campaign_builder_campaigns"

# ---------------------------------------------------------------------------
# P3 — Batch Release Campaign vocabulary
# ---------------------------------------------------------------------------

RELEASE_TYPES = ["interval_minutes", "hourly", "daily", "weekly", "manual", "custom"]

# Bookkeeping-only status for the parent batch campaign. Deliberately kept
# separate from the existing `status` field (draft/compiled/active/archived)
# so non-batch P2 campaigns are entirely unaffected.
BATCH_STATUSES = [
    "draft",
    "compiling",
    "scheduled",
    "active",
    "paused",
    "completed",
    "cancelled",
]

# Bookkeeping-only status stamped on each child drop document (additive;
# does not replace the drop's own `status` field that vouchers.py/scheduler
# read for claim eligibility).
CHILD_BATCH_STATUSES = ["scheduled", "released", "paused", "cancelled"]

BATCH_LOCK_COLLECTION = "scheduler_locks"

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
        col.create_index([("batch_status", ASCENDING)], name="ix_cb_batch_status")
        col.create_index([("next_release_at", ASCENDING)], name="ix_cb_next_release_at")
        # Additive index on the existing drops collection to support batch
        # lookups (batch_parent_id + batch_index/batch_status) — does not
        # touch any existing drops index.
        database.db["drops"].create_index(
            [("batch_parent_id", ASCENDING), ("batch_index", ASCENDING)],
            name="ix_drops_batch_parent",
        )
    except Exception:
        logger.warning("[CAMPAIGN_BUILDER] Failed to create indexes", exc_info=True)


_ensure_indexes()


# ---------------------------------------------------------------------------
# Audience resolution helpers (read-only against existing collections)
# ---------------------------------------------------------------------------

def _resolve_segment_user_ids(db, segments: list[str]) -> list[int]:
    """Resolve segment names to user_ids for OPERATIONAL campaign eligibility.

    Canonical behavioral authority chain: Databot ``app/analytics/segments.py``
    classifier -> ``segment_snapshots`` -> Databot's ``segment_sync_job`` ->
    this repo's ``users.for_bot_segment`` / ``users.for_bot_segment_normalized``
    (the only fields that job writes). This is deliberately NOT read from
    ``backend_segment_snapshots`` (the shadow-only classifier in
    ``backend_segment_engine.py``) — that collection's thresholds differ from
    the canonical Databot classifier and must never gate live campaign
    eligibility.

    On top of that canonical field, this resolves the EFFECTIVE (operational)
    segment via ``effective_segment.effective_segment_query_for_segments`` --
    a Telegram identity flagged ``multi_account_voucher_hunter=True`` always
    resolves into "voucher_hunter" here (regardless of its canonical
    for_bot_segment_normalized) and is excluded from every other segment's
    resolution, WITHOUT for_bot_segment/for_bot_segment_normalized themselves
    ever being written to. See effective_segment.py's module docstring.

    The resulting ids are written as an ``eligibility.mode="user_id"`` allow
    list, an enforcement path vouchers.py already implements.
    """
    valid = [s for s in segments if s in VALID_SEGMENTS]
    if not valid:
        return []
    users_col = db["users"]
    cursor = users_col.find(
        {**effective_segment_query_for_segments(valid), "user_id": {"$ne": None}},
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
                # source="segment" marks this allow-list as segment-generated so
                # vouchers._is_probability_shaped_pooled_drop applies claim-time
                # segment probability shaping to it — unlike a hand-picked
                # "whitelist" audience allow-list (see _build_audience below),
                # which must NOT be re-gated by a random segment roll.
                eligibility={"mode": "user_id", "allow": seg_ids, "source": "segment"},
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
# P3 — Batch Release Campaign: schedule math
# ---------------------------------------------------------------------------

def compute_batch_count(total_vouchers: int, batch_size: int) -> int:
    if batch_size <= 0:
        return 0
    return math.ceil(total_vouchers / batch_size)


def compute_release_schedule(
    *,
    release_type: str,
    batch_count: int,
    first_release_at: datetime,
    release_interval_minutes: int | None = None,
    custom_schedule: list[str] | None = None,
) -> list[datetime | None]:
    """Return a list of UTC datetimes, one per batch (1-indexed conceptually,
    list is 0-indexed), or ``None`` entries for manual (never auto-released).

    Pure function, no I/O — safe to call repeatedly (idempotent by
    construction: same inputs always produce the same schedule).
    """
    if batch_count <= 0:
        return []

    if release_type == "manual":
        return [None] * batch_count

    if release_type == "custom":
        out: list[datetime | None] = []
        for item in custom_schedule or []:
            try:
                out.append(_as_utc_dt(item))
            except Exception:
                out.append(None)
        # Pad/truncate defensively; validation elsewhere should already
        # guarantee an exact match, this just avoids an IndexError.
        while len(out) < batch_count:
            out.append(None)
        return out[:batch_count]

    if release_type == "interval_minutes":
        step = timedelta(minutes=max(1, int(release_interval_minutes or 60)))
    elif release_type == "hourly":
        step = timedelta(hours=1)
    elif release_type == "daily":
        step = timedelta(days=1)
    elif release_type == "weekly":
        step = timedelta(weeks=1)
    else:
        step = timedelta(hours=1)

    return [first_release_at + (step * i) for i in range(batch_count)]


def _as_utc_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    from vouchers import parse_kl_local
    return parse_kl_local(str(value))


def split_codes_into_batches(codes: list[str], batch_size: int, batch_count: int) -> list[list[str]]:
    """Split codes into ``batch_count`` chunks of ``batch_size`` (last chunk
    holds the remainder — supports uneven splits, e.g. 525 codes / 50 => 11
    chunks, the last with 25)."""
    chunks: list[list[str]] = []
    for i in range(batch_count):
        start = i * batch_size
        chunks.append(list(codes[start:start + batch_size]))
    return chunks


def validate_batch_params(campaign_doc: dict) -> list[str]:
    """Return a list of blocking validation errors (empty == launchable)."""
    errors: list[str] = []
    release_type = campaign_doc.get("release_type")
    if release_type not in RELEASE_TYPES:
        errors.append(f"invalid_release_type:{release_type}")

    total_vouchers = campaign_doc.get("total_vouchers")
    batch_size = campaign_doc.get("batch_size")
    if not isinstance(total_vouchers, int) or total_vouchers <= 0:
        errors.append("invalid_total_vouchers")
    if not isinstance(batch_size, int) or batch_size <= 0:
        errors.append("invalid_batch_size")

    if not errors:
        _, reward_fields, _ = _build_reward(campaign_doc)
        drop_type = "personalised" if (campaign_doc.get("reward_type") == "personalised_voucher") else "pooled"
        if drop_type == "pooled":
            codes = reward_fields.get("codes") or []
            if len(codes) < total_vouchers:
                errors.append(
                    f"insufficient_codes:have={len(codes)}:need={total_vouchers}"
                )
        else:
            assignments = reward_fields.get("assignments") or []
            if len(assignments) < total_vouchers:
                errors.append(
                    f"insufficient_assignments:have={len(assignments)}:need={total_vouchers}"
                )

        if release_type == "custom":
            batch_count = compute_batch_count(total_vouchers, batch_size) if isinstance(batch_size, int) and batch_size > 0 else 0
            custom_schedule = campaign_doc.get("release_schedule") or []
            if len(custom_schedule) != batch_count:
                errors.append(
                    f"custom_schedule_length_mismatch:have={len(custom_schedule)}:need={batch_count}"
                )
        if release_type == "interval_minutes":
            if not isinstance(campaign_doc.get("release_interval_minutes"), int) or campaign_doc.get("release_interval_minutes") <= 0:
                errors.append("invalid_release_interval_minutes")

    return errors


def preview_batch_campaign(campaign_doc: dict) -> dict:
    """Safety Preview for a batch release campaign. Read-only."""
    warnings = list(validate_batch_params(campaign_doc))
    total_vouchers = campaign_doc.get("total_vouchers") or 0
    batch_size = campaign_doc.get("batch_size") or 0
    batch_count = compute_batch_count(total_vouchers, batch_size) if batch_size else 0
    release_type = campaign_doc.get("release_type")

    release_style = campaign_doc.get("release_style") or "immediate"
    release_params = campaign_doc.get("release_params") or {}
    if release_style == "scheduled" and release_params.get("startsAtLocal"):
        first_release_at = _as_utc_dt(release_params["startsAtLocal"])
    else:
        first_release_at = datetime.now(timezone.utc)

    schedule = compute_release_schedule(
        release_type=release_type,
        batch_count=batch_count,
        first_release_at=first_release_at,
        release_interval_minutes=campaign_doc.get("release_interval_minutes"),
        custom_schedule=campaign_doc.get("release_schedule"),
    )
    resolved_schedule = [dt.isoformat() if dt else None for dt in schedule]
    dated = [dt for dt in schedule if dt]

    eligibility, audience, audience_warnings = _build_audience(campaign_doc, database.db)
    warnings.extend(audience_warnings)
    drop_type, _, reward_warnings = _build_reward(campaign_doc)
    warnings.extend(reward_warnings)

    duration_hours = None
    if len(dated) >= 2:
        duration_hours = round((max(dated) - min(dated)).total_seconds() / 3600.0, 2)

    return {
        "campaign_name": campaign_doc.get("campaign_name"),
        "release_type": release_type,
        "total_vouchers": total_vouchers,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "release_interval_minutes": campaign_doc.get("release_interval_minutes"),
        "release_schedule": resolved_schedule,
        "first_release_at": resolved_schedule[0] if resolved_schedule else None,
        "last_release_at": resolved_schedule[-1] if resolved_schedule else None,
        "estimated_duration_hours": duration_hours,
        "audience_mode": campaign_doc.get("audience_mode"),
        "reward_type": campaign_doc.get("reward_type"),
        "drop_type": drop_type,
        "region_restriction": (audience.get("regions") or None),
        "warnings": warnings,
        "launchable": not any(
            e.startswith(("invalid_", "insufficient_", "custom_schedule_length_mismatch"))
            for e in warnings
        ),
    }


# ---------------------------------------------------------------------------
# P3 — Batch Release Campaign: compiler
# ---------------------------------------------------------------------------

COMPILE_LEASE_SECONDS = 30


def compile_batch_campaign(campaign_doc: dict) -> tuple[dict, int]:
    """Compile a draft batch campaign into N child voucher drops.

    Idempotent/crash-safe by construction:
      - Guarded by an atomic, leased compare-and-swap on batch_status
        draft->compiling (a single find_one_and_update whose result decides
        whether *this* call is the one allowed to proceed — not a
        write-then-reread, which cannot tell two concurrent winners apart).
        A repeated/concurrent LAUNCH click cannot start a second compile
        run: only the caller whose CAS actually matched proceeds; every
        other concurrent caller is rejected outright with
        "compile_in_progress" instead of racing ahead. The lease expires
        after COMPILE_LEASE_SECONDS so a genuinely crashed compile can still
        be retried and resumed (see below) once the lease goes stale.
      - Ground truth for "which batches already exist" is read from the
        `drops` collection itself (batch_parent_id + batch_index), not from
        the parent doc's cached child_drop_ids — so if the process crashes
        mid-way (some child drops inserted, parent doc not yet updated), a
        retried compile call (after the lease goes stale) resumes from the
        first missing batch index instead of duplicating already-created
        drops.
      - Writes exclusively through vouchers.create_drop_from_spec — the same
        insert primitive P2 and admin_create_drop use. No duplicate
        drop-insert logic.
    """
    from vouchers import create_drop_from_spec

    db = database.db
    campaign_id = campaign_doc["_id"]

    if campaign_doc.get("batch_status") not in ("draft", "compiling", None):
        return {"status": "error", "code": "not_draft"}, 400

    errors = validate_batch_params(campaign_doc)
    if errors:
        return {"status": "error", "code": "validation_failed", "errors": errors}, 400

    # Atomic leased CAS: only the caller whose update actually matches may
    # proceed. A concurrent second caller's filter will not match (the
    # first caller already flipped batch_status, and its lease is fresh),
    # so it is rejected here instead of both callers racing into the
    # drop-creation loop below.
    now0 = datetime.now(timezone.utc)
    stale_before = now0 - timedelta(seconds=COMPILE_LEASE_SECONDS)
    fresh = _col().find_one_and_update(
        {
            "_id": campaign_id,
            "$or": [
                {"batch_status": {"$in": ["draft", None]}},
                {"batch_status": "compiling", "compile_started_at": {"$lt": stale_before}},
            ],
        },
        {"$set": {"batch_status": "compiling", "compile_started_at": now0}},
        return_document=ReturnDocument.AFTER,
    )
    if fresh is None:
        current = _col().find_one({"_id": campaign_id}) or campaign_doc
        if current.get("batch_status") == "compiling":
            return {"status": "error", "code": "compile_in_progress"}, 409
        return {"status": "error", "code": "not_draft"}, 400

    eligibility, audience, audience_warnings = _build_audience(fresh, db)
    drop_type, reward_fields, reward_warnings = _build_reward(fresh)

    total_vouchers = fresh["total_vouchers"]
    batch_size = fresh["batch_size"]
    batch_count = compute_batch_count(total_vouchers, batch_size)
    release_type = fresh["release_type"]

    release_style = fresh.get("release_style") or "immediate"
    release_params = fresh.get("release_params") or {}
    if release_style == "scheduled" and release_params.get("startsAtLocal"):
        first_release_at = _as_utc_dt(release_params["startsAtLocal"])
    else:
        first_release_at = datetime.now(timezone.utc)

    schedule = compute_release_schedule(
        release_type=release_type,
        batch_count=batch_count,
        first_release_at=first_release_at,
        release_interval_minutes=fresh.get("release_interval_minutes"),
        custom_schedule=fresh.get("release_schedule"),
    )

    if drop_type == "personalised":
        chunks = split_codes_into_batches(reward_fields.get("assignments") or [], batch_size, batch_count)
    else:
        chunks = split_codes_into_batches(reward_fields.get("codes") or [], batch_size, batch_count)

    campaign_name = fresh.get("campaign_name") or "Campaign"
    campaign_type = fresh.get("campaign_type") or "smart_default"
    slug = _slug(campaign_name)

    # Ground truth for what's already been created (crash-safe resume).
    existing_children = list(db.drops.find(
        {"batch_parent_id": str(campaign_id)},
        projection={"_id": 1, "batch_index": 1},
    ))
    existing_indexes = {d.get("batch_index") for d in existing_children}

    child_drop_ids: list[str] = [None] * batch_count
    for d in existing_children:
        idx = d.get("batch_index")
        if isinstance(idx, int) and 1 <= idx <= batch_count:
            child_drop_ids[idx - 1] = str(d["_id"])

    drop_errors: list[dict] = []
    for i in range(1, batch_count + 1):
        if i in existing_indexes:
            continue
        release_at = schedule[i - 1]
        starts_at_local = _to_kl_local_str(release_at) if release_at else _to_kl_local_str(first_release_at)
        spec = _base_drop_spec(
            name=f"{slug}_{i:03d}",
            starts_at_local=starts_at_local,
            ends_at_local=None,
            campaign_type=campaign_type,
            drop_type=drop_type,
            eligibility=eligibility,
            audience=audience,
            reward_fields=(
                {"assignments": chunks[i - 1]} if drop_type == "personalised"
                else {"codes": chunks[i - 1], "pool": reward_fields.get("pool") or "public"}
            ),
        )
        result, code = create_drop_from_spec(spec)
        if code == 200 and result.get("status") == "ok":
            drop_id = result["dropId"]
            child_drop_ids[i - 1] = drop_id
            try:
                db.drops.update_one(
                    {"_id": ObjectId(drop_id)},
                    {"$set": {
                        "campaign_id": str(campaign_id),
                        "campaign_name": campaign_name,
                        "batch_parent_id": str(campaign_id),
                        "batch_index": i,
                        "batch_count": batch_count,
                        "batch_release_at": release_at,
                        "batch_status": "scheduled",
                        # All batches start non-live; release timing is
                        # enforced by _release_next_batch flipping status,
                        # not by the natural startsAt scheduler sweep — this
                        # is what makes Pause reliable without touching the
                        # scheduler core (reconcile_drop_statuses never
                        # auto-activates a "paused" drop).
                        "status": "paused",
                    }},
                )
            except Exception:
                logger.warning("[BATCH_RELEASE] Failed to tag child drop %s", drop_id, exc_info=True)
        else:
            drop_errors.append({"batch_index": i, "error": result})

    if any(x is None for x in child_drop_ids):
        # Partial failure: leave batch_status="compiling" so a retry resumes
        # from the missing indexes instead of silently reporting success.
        _col().update_one(
            {"_id": campaign_id},
            {"$set": {"child_drop_ids": [x for x in child_drop_ids if x], "updated_at": datetime.now(timezone.utc)}},
        )
        return {
            "status": "error",
            "code": "compile_incomplete",
            "compiled": [x for x in child_drop_ids if x],
            "errors": drop_errors,
        }, 400

    now = datetime.now(timezone.utc)
    _col().update_one(
        {"_id": campaign_id},
        {"$set": {
            "status": "active",
            "batch_status": "scheduled",
            "batch_count": batch_count,
            "child_drop_ids": child_drop_ids,
            "release_schedule": [dt.isoformat() if dt else None for dt in schedule],
            "released_batches": 0,
            "next_release_at": schedule[0] if schedule else None,
            "compiled_at": now,
            "updated_at": now,
        }},
    )

    # Immediately release any batch(es) already due (e.g. batch #1 of an
    # "immediate" release) so Launch feels instant instead of waiting for
    # the next tick.
    released_now = []
    fresh2 = _col().find_one({"_id": campaign_id})
    while fresh2 and fresh2.get("batch_status") in ("scheduled", "active"):
        nra = fresh2.get("next_release_at")
        if release_type != "manual" and nra and _as_utc_dt(nra) <= datetime.now(timezone.utc):
            released = _release_next_batch(campaign_id)
            if not released:
                break
            released_now.append(released)
            fresh2 = _col().find_one({"_id": campaign_id})
        else:
            break

    return {
        "status": "ok",
        "batch_count": batch_count,
        "child_drop_ids": child_drop_ids,
        "released_now": released_now,
        "warnings": audience_warnings + reward_warnings,
    }, 200


def _to_kl_local_str(dt: datetime) -> str:
    return dt.astimezone(KL_TZ).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# P3 — Batch Release Campaign: release / pause / resume / cancel
# ---------------------------------------------------------------------------

def _release_next_batch(campaign_id) -> str | None:
    """Atomically release the single next unreleased child drop.

    Idempotent: uses a compare-and-swap on the child drop's batch_status so
    a duplicate call (manual double-click, tick racing a manual release,
    scheduler rerun) releases the batch at most once.
    """
    db = database.db
    campaign_id_str = str(campaign_id)

    next_child = db.drops.find_one(
        {"batch_parent_id": campaign_id_str, "batch_status": "scheduled"},
        sort=[("batch_index", ASCENDING)],
    )
    if not next_child:
        _col().update_one(
            {"_id": ObjectId(campaign_id_str), "batch_status": {"$in": ["scheduled", "active"]}},
            {"$set": {"batch_status": "completed", "updated_at": datetime.now(timezone.utc)}},
        )
        return None

    now = datetime.now(timezone.utc)
    starts = next_child.get("startsAt")
    ends = next_child.get("endsAt")
    starts_aware = starts if (starts and starts.tzinfo) else (starts.replace(tzinfo=timezone.utc) if starts else None)
    ends_aware = ends if (ends and ends.tzinfo) else (ends.replace(tzinfo=timezone.utc) if ends else None)

    update_fields = {
        "batch_status": "released",
        "batch_actual_release_at": now,
    }
    if ends_aware and now >= ends_aware:
        # The window computed at compile time (startsAt/endsAt, default
        # 24h) has already elapsed by the time this batch is actually
        # released — e.g. a "manual" batch released well after compiling,
        # or any batch released long after a pause/resume. Re-anchor the
        # window to the actual release moment (preserving the originally
        # configured duration) so the voucher isn't dead on arrival; a
        # release action should mean "live now for its intended duration",
        # not "live during whatever window was guessed at compile time".
        duration = (ends_aware - starts_aware) if starts_aware else timedelta(hours=24)
        starts_aware = now
        ends_aware = now + duration
        update_fields["startsAt"] = starts_aware
        update_fields["endsAt"] = ends_aware

    update_fields["status"] = "active" if (starts_aware and starts_aware <= now and (not ends_aware or now < ends_aware)) else "upcoming"

    db.drops.update_one(
        {"_id": next_child["_id"], "batch_status": "scheduled"},
        {"$set": update_fields},
    )

    campaign = _col().find_one({"_id": ObjectId(campaign_id_str)}) or {}
    batch_count = campaign.get("batch_count") or 0
    remaining_next = db.drops.find_one(
        {"batch_parent_id": campaign_id_str, "batch_status": "scheduled"},
        sort=[("batch_index", ASCENDING)],
    )
    released_batches = db.drops.count_documents(
        {"batch_parent_id": campaign_id_str, "batch_status": {"$in": ["released", "paused", "cancelled"]}}
    )
    next_release_at = remaining_next.get("batch_release_at") if remaining_next else None
    new_batch_status = "completed" if not remaining_next else "active"

    _col().update_one(
        {"_id": ObjectId(campaign_id_str)},
        {"$set": {
            "released_batches": int(released_batches),
            "next_release_at": next_release_at,
            "batch_status": new_batch_status,
            "updated_at": now,
        }},
    )
    return str(next_child["_id"])


def release_next_batch_now(campaign_id) -> tuple[dict, int]:
    campaign = _col().find_one({"_id": ObjectId(str(campaign_id))})
    if not campaign:
        return {"status": "error", "code": "not_found"}, 404
    if campaign.get("batch_status") not in ("scheduled", "active"):
        return {"status": "error", "code": "not_releasable", "batch_status": campaign.get("batch_status")}, 400
    released = _release_next_batch(campaign["_id"])
    if not released:
        return {"status": "ok", "released_drop_id": None, "message": "no_unreleased_batches"}, 200
    return {"status": "ok", "released_drop_id": released}, 200


def pause_batch_campaign(campaign_id) -> tuple[dict, int]:
    oid = ObjectId(str(campaign_id))
    campaign = _col().find_one({"_id": oid})
    if not campaign:
        return {"status": "error", "code": "not_found"}, 404
    if campaign.get("batch_status") not in ("scheduled", "active"):
        return {"status": "error", "code": "not_pausable", "batch_status": campaign.get("batch_status")}, 400
    now = datetime.now(timezone.utc)
    _col().update_one({"_id": oid}, {"$set": {"batch_status": "paused", "paused_at": now, "updated_at": now}})
    return {"status": "ok", "batch_status": "paused"}, 200


def resume_batch_campaign(campaign_id) -> tuple[dict, int]:
    oid = ObjectId(str(campaign_id))
    campaign = _col().find_one({"_id": oid})
    if not campaign:
        return {"status": "error", "code": "not_found"}, 404
    if campaign.get("batch_status") != "paused":
        return {"status": "error", "code": "not_paused", "batch_status": campaign.get("batch_status")}, 400
    now = datetime.now(timezone.utc)
    new_status = "active" if (campaign.get("released_batches") or 0) > 0 else "scheduled"
    _col().update_one(
        {"_id": oid},
        {"$set": {"batch_status": new_status, "paused_at": None, "updated_at": now}},
    )
    # Overdue batches (missed while paused) are caught up by the next tick,
    # which loops "while next_release_at <= now" — no special-casing needed.
    return {"status": "ok", "batch_status": new_status}, 200


def cancel_batch_campaign(campaign_id) -> tuple[dict, int]:
    oid = ObjectId(str(campaign_id))
    campaign = _col().find_one({"_id": oid})
    if not campaign:
        return {"status": "error", "code": "not_found"}, 404
    if campaign.get("batch_status") in ("cancelled", "completed", None, "draft"):
        return {"status": "error", "code": "not_cancellable", "batch_status": campaign.get("batch_status")}, 400
    db = database.db
    campaign_id_str = str(oid)
    now = datetime.now(timezone.utc)
    # Only touch drops that never went live — already released/claimed
    # vouchers are left completely untouched (no parallel claim ledger, no
    # retroactive edits to live drops).
    for child in db.drops.find({"batch_parent_id": campaign_id_str, "batch_status": "scheduled"}):
        db.drops.update_one(
            {"_id": child["_id"]},
            {"$set": {"status": "expired", "batch_status": "cancelled"}},
        )
    _col().update_one(
        {"_id": oid},
        {"$set": {"batch_status": "cancelled", "cancelled_at": now, "updated_at": now}},
    )
    return {"status": "ok", "batch_status": "cancelled"}, 200


# ---------------------------------------------------------------------------
# P3 — Batch Release Campaign: scheduler tick
# ---------------------------------------------------------------------------

def _acquire_batch_lock(name: str, ttl_seconds: int) -> bool:
    """Small, self-contained lock reusing the existing `scheduler_locks`
    collection/pattern from main.py (find_one_and_update CAS + TTL index),
    duplicated here (not imported) to avoid a circular import between
    campaign_builder.py and main.py. Same collection, same semantics."""
    from pymongo.errors import DuplicateKeyError

    col = database.db[BATCH_LOCK_COLLECTION]
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)
    try:
        doc = col.find_one_and_update(
            {
                "_id": name,
                "$or": [{"expireAt": {"$lte": now}}, {"expireAt": {"$exists": False}}],
            },
            {
                "$set": {"expireAt": expires_at, "updatedAt": now},
                "$setOnInsert": {"createdAt": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return False
    return doc is not None


def batch_release_tick() -> dict:
    """Scan due batch campaigns and release/activate their due child drops.

    Idempotent, tolerant of reruns/missed ticks/partial failures:
      - Each batch release is itself a CAS on the child drop's batch_status
        (see _release_next_batch), so re-running this tick never
        double-releases a batch.
      - Missed ticks are caught up automatically: the inner while-loop keeps
        releasing overdue batches for a campaign until next_release_at is in
        the future (or there's nothing left to release), capped at
        batch_count iterations as a hard safety bound.
    """
    # TTL must stay under the cron cadence (main.py schedules this tick
    # every 60s via CronTrigger(minute="*/1")). A longer TTL (e.g. the 240s
    # originally used here) would hold the lock across several scheduled
    # firings, silently skipping them — which breaks "every X minutes"
    # release cadence for X below ~4. 50s still comfortably prevents two
    # overlapping runs of this lightweight tick while letting every
    # legitimate per-minute firing actually acquire the lock.
    if not _acquire_batch_lock("batch_release_tick", ttl_seconds=50):
        return {"skipped": "lock_not_acquired"}

    now = datetime.now(timezone.utc)
    due = list(_col().find({
        "batch_status": {"$in": ["scheduled", "active"]},
        "release_type": {"$ne": "manual"},
        "next_release_at": {"$lte": now},
    }))

    released_total = 0
    for campaign in due:
        campaign_id = campaign["_id"]
        cap = int(campaign.get("batch_count") or 0) + 1
        for _ in range(cap):
            fresh = _col().find_one({"_id": campaign_id})
            if not fresh or fresh.get("batch_status") not in ("scheduled", "active"):
                break
            nra = fresh.get("next_release_at")
            if not nra or _as_utc_dt(nra) > now:
                break
            released = _release_next_batch(campaign_id)
            if not released:
                break
            released_total += 1

    if due:
        logger.info("[BATCH_RELEASE] tick campaigns_due=%s released=%s", len(due), released_total)
    return {"campaigns_due": len(due), "released": released_total}


# ---------------------------------------------------------------------------
# P3 — Batch Release Campaign: analytics
# ---------------------------------------------------------------------------

def batch_campaign_analytics(campaign_id) -> dict | None:
    """Aggregate analytics purely from existing `drops`/`vouchers`
    collections — no parallel claim ledger."""
    db = database.db
    oid = ObjectId(str(campaign_id))
    campaign = _col().find_one({"_id": oid})
    if not campaign:
        return None
    campaign_id_str = str(oid)

    from vouchers import PERSONALISED_TYPE_ALIASES

    children = list(db.drops.find({"batch_parent_id": campaign_id_str}, sort=[("batch_index", ASCENDING)]))
    child_rows = []
    total_codes = 0
    claimed_codes = 0
    released_codes = 0
    for child in children:
        drop_id_str = str(child["_id"])
        drop_id_variants = [drop_id_str, child["_id"]]
        is_personalised = child.get("type") == "personalised"
        if is_personalised:
            total = db.vouchers.count_documents({"type": {"$in": list(PERSONALISED_TYPE_ALIASES)}, "dropId": {"$in": drop_id_variants}})
            claimed = db.vouchers.count_documents({"type": {"$in": list(PERSONALISED_TYPE_ALIASES)}, "dropId": {"$in": drop_id_variants}, "status": "claimed"})
        else:
            total = db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": drop_id_variants}})
            claimed = db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": drop_id_variants}, "status": {"$ne": "free"}})
        total_codes += total
        claimed_codes += claimed
        if child.get("batch_status") in ("released", "paused", "cancelled"):
            released_codes += total

        release_at = child.get("batch_release_at")
        child_rows.append({
            "batch_index": child.get("batch_index"),
            "drop_id": drop_id_str,
            "release_time": release_at.isoformat() if isinstance(release_at, datetime) else release_at,
            "status": child.get("batch_status"),
            "drop_status": child.get("status"),
            "total_codes": total,
            "claimed": claimed,
            "remaining": max(0, total - claimed),
        })

    batch_count = campaign.get("batch_count") or len(children)
    released_batches = campaign.get("released_batches") or 0
    next_release_at = campaign.get("next_release_at")
    return {
        "campaign_id": campaign_id_str,
        "campaign_name": campaign.get("campaign_name"),
        "batch_status": campaign.get("batch_status"),
        "total_vouchers": campaign.get("total_vouchers"),
        "released_vouchers": released_codes,
        "claimed_vouchers": claimed_codes,
        "remaining_vouchers": max(0, released_codes - claimed_codes),
        "released_batches": released_batches,
        "total_batches": batch_count,
        "next_release_at": next_release_at.isoformat() if isinstance(next_release_at, datetime) else next_release_at,
        "completion_pct": round(100.0 * released_batches / batch_count, 1) if batch_count else 0.0,
        "child_drops": child_rows,
    }


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
        "batch_release_types": RELEASE_TYPES,
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

    # P3: batch release fields are optional/additive. Presence of a valid
    # release_type is what marks a campaign as a batch campaign (routed to
    # compile_batch_campaign instead of the P2 compile_campaign).
    release_type = body.get("release_type")
    if release_type in RELEASE_TYPES:
        doc["release_type"] = release_type
        doc["batch_status"] = "draft"
        doc["total_vouchers"] = body.get("total_vouchers")
        doc["batch_size"] = body.get("batch_size")
        doc["release_interval_minutes"] = body.get("release_interval_minutes")
        doc["release_schedule"] = body.get("release_schedule") or []
        doc["child_drop_ids"] = []
        doc["released_batches"] = 0
        doc["next_release_at"] = None
        doc["compiled_at"] = None
        doc["paused_at"] = None
        doc["cancelled_at"] = None

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
    if existing.get("batch_status") not in (None, "draft"):
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

    if "release_type" in body:
        rt = body["release_type"]
        if rt is not None and rt not in RELEASE_TYPES:
            return jsonify({"status": "error", "code": "invalid_release_type"}), 400
        updates["release_type"] = rt
        if rt is not None and not existing.get("batch_status"):
            updates["batch_status"] = "draft"

    for field in ("total_vouchers", "batch_size", "release_interval_minutes"):
        if field in body:
            updates[field] = body[field]

    if "release_schedule" in body:
        updates["release_schedule"] = body["release_schedule"] or []

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
    if doc.get("release_type") in RELEASE_TYPES:
        return jsonify({"status": "ok", "preview": preview_batch_campaign(doc)})
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

    if doc.get("release_type") in RELEASE_TYPES:
        result, status_code = compile_batch_campaign(doc)
    else:
        result, status_code = compile_campaign(doc)
    return jsonify(result), status_code


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/pause", methods=["POST"])
def pause_campaign_route(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result, status_code = pause_batch_campaign(oid)
    return jsonify(result), status_code


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/resume", methods=["POST"])
def resume_campaign_route(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result, status_code = resume_batch_campaign(oid)
    return jsonify(result), status_code


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/cancel", methods=["POST"])
def cancel_campaign_route(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result, status_code = cancel_batch_campaign(oid)
    return jsonify(result), status_code


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/release-next", methods=["POST"])
def release_next_campaign_route(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result, status_code = release_next_batch_now(oid)
    return jsonify(result), status_code


@campaign_builder_bp.route("/api/admin/campaign-builder/campaigns/<campaign_id>/analytics", methods=["GET"])
def batch_analytics_route(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    analytics = batch_campaign_analytics(oid)
    if analytics is None:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok", "analytics": analytics})
