"""Campaign Centre — a generic marketing-campaign gateway for the Mini App.

Ships today with three campaign types (tournament, external subscription
verification, external website) but nothing here — collection name, module
name, route names, reward-rule engine — is tournament-specific. Future types
(lucky draw, referral contest, cashback, mission, survey, seasonal event,
partner campaign, VIP event, ...) are added by extending ``CAMPAIGN_TYPES``
and, if they need a new reward condition, ``reward_engine.CONDITION_TYPES`` —
not by redesigning this module.

This is intentionally a separate collection/namespace from the pre-existing
``campaigns`` collection (see campaigns.py / campaign_engine.py), which is an
unrelated segment-audience marketing/voucher-targeting tool. Reusing that
collection or its ``/api/admin/campaigns`` routes would collide with a
production feature, so the Campaign Centre admin API is namespaced under
``/api/admin/gc-campaigns`` (kept short and flat like every other admin API
here — the extra segment only exists because ``/api/admin/campaigns`` is
already taken). Public and integration endpoints use the exact paths from
the spec since those do not collide with anything existing.

Collection: ``gc_campaigns``
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument

import database
import reward_engine
from campaign_providers import get_provider, provider_is_usable_for_results

logger = logging.getLogger(__name__)

campaign_centre_bp = Blueprint("campaign_centre", __name__)
campaign_public_bp = Blueprint("campaign_public", __name__)

CAMPAIGN_TYPES = ["tournament", "external_subscription_verification", "external_website", "mission_pool"]
CAMPAIGN_STATUSES = ["draft", "scheduled", "live", "paused", "ended", "archived"]
OPEN_MODES = ["telegram_web_app", "external_url"]

# Campaign types whose publish gate requires at least one reward rule before
# going live. Extend this set (not the publish logic) when a new reward-
# driven campaign type is added.
_REWARD_DRIVEN_TYPES = {"tournament"}

# Campaign types whose rewards are produced by their own mechanic rather than
# by ``reward_config.rules``. Mission Pool allocates from a single configured
# voucher pool after winner selection (see mission_pool.py), so the
# rules-required publish gate above does not apply to it.
_SELF_REWARDING_TYPES = {"mission_pool"}

# Which open modes are valid for which campaign type.
_ALLOWED_OPEN_MODES_BY_TYPE = {
    "tournament": {"telegram_web_app", "external_url"},
    "external_subscription_verification": {"external_url"},
    "external_website": {"external_url", "telegram_web_app"},
    # Mission Pool is answered inside the Mini App; it has no external
    # provider and never opens an outside URL.
    "mission_pool": {"telegram_web_app"},
}

_VALID_STATUS_TRANSITIONS = {
    "draft": {"draft", "scheduled", "live", "archived"},
    "scheduled": {"scheduled", "live", "draft", "paused", "archived"},
    "live": {"live", "paused", "ended", "archived"},
    "paused": {"paused", "live", "ended", "archived"},
    "ended": {"ended", "archived"},
    "archived": {"archived"},
}

# Permanent deletion is only allowed once a campaign can no longer be
# publicly active — a live/paused campaign must be archived first (or run
# out its schedule to `ended`) so it can never be deleted while still
# reachable by players.
_DELETABLE_STATUSES = {"draft", "archived", "ended"}

# A deleted campaign is never removed from gc_campaigns — its document is
# atomically converted in place into a minimal tombstone (see
# delete_campaign) with status="deleted". This is what actually reserves the
# campaign_id: the collection's existing unique index on campaign_id
# (ux_gc_campaigns_campaign_id) can never be re-satisfied by a new insert
# once the tombstone exists, permanently and without a second collection
# that could itself fail to write and reopen the reuse window. Deleting a
# campaign deliberately keeps its historical registration/mission-entry/
# reward/tournament-result rows for traceability, all still keyed by
# campaign_id — e.g. campaign_registration.py's get_registration()/
# register_for_campaign() and mission_pool.py's already_submitted check both
# key purely on campaign_id, so if that id could ever be reused, the new
# campaign would silently inherit that old history.
#
# Every field here is stripped from the tombstone left behind in
# gc_campaigns (via $unset) — it keeps only campaign_id, name, type and the
# status/deleted_at/deleted_by/timestamp bookkeeping needed to identify it
# and explain why it can never be reactivated. None of these operational/
# config fields survive, so nothing about a tombstone can expose or
# re-activate the campaign (a destination URL, a reward pool, a mission
# config, ...).
_TOMBSTONE_UNSET_FIELDS = (
    "description", "button_text", "banner_url",
    "schedule", "telegram", "destination",
    "mission_config", "mission_pool", "registration", "reward_config",
    "mechanic",
)


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db["gc_campaigns"]
        col.create_index([("campaign_id", 1)], name="ux_gc_campaigns_campaign_id", unique=True)
        col.create_index([("status", 1)], name="ix_gc_campaigns_status")
        col.create_index([("type", 1)], name="ix_gc_campaigns_type")
        col.create_index([("schedule.starts_at", 1)], name="ix_gc_campaigns_starts_at")
        col.create_index([("schedule.ends_at", 1)], name="ix_gc_campaigns_ends_at")
        col.create_index([("priority", -1)], name="ix_gc_campaigns_priority")
        col.create_index([("destination.provider_id", 1)], name="ix_gc_campaigns_provider_id")
    except Exception:
        logger.warning("[CAMPAIGN_CENTRE] index creation failed", exc_info=True)


_ensure_indexes()


def _log_audit(action: str, admin: dict, campaign_id: str, details: dict | None = None) -> None:
    try:
        database.db["campaign_admin_audit_log"].insert_one({
            "action": action,
            "entity": "campaign",
            "entity_id": campaign_id,
            "admin": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
            "details": details or {},
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[CAMPAIGN_CENTRE] audit_write_failed", exc_info=True)


_EVENT_TOP_LEVEL_FIELDS = frozenset({
    "campaign_type", "provider_id", "submission_id", "reward_id", "pool_id",
    "source", "status", "reason", "event_id", "occurred_at",
})


def log_funnel_event(event: str, *, campaign_id: str, user_id: int | None = None, **extra) -> None:
    """Thin, call-site-compatible wrapper around the canonical
    campaign_events writer (campaign_events.emit_campaign_event) — every
    Campaign Centre module writes through this one function (or the writer
    directly), so campaign_events is the single ledger, not a per-module
    ad-hoc insert."""
    from campaign_events import emit_campaign_event

    top_level = {k: extra.pop(k) for k in list(extra) if k in _EVENT_TOP_LEVEL_FIELDS}
    emit_campaign_event(
        event_type=event,
        campaign_id=campaign_id,
        telegram_user_id=user_id,
        metadata=extra or None,
        **top_level,
    )


def get_campaign(campaign_id: str) -> dict | None:
    """The single lookup every caller (admin routes, public routes, and
    every other module — mission_pool, campaign_registration, tournament_*,
    subscription_verification_api, ...) goes through. A deleted campaign's
    document still physically exists as a tombstone (see delete_campaign /
    _TOMBSTONE_UNSET_FIELDS), so it must never be returned here — this one
    check is what makes a deleted campaign invisible/inert everywhere: it
    can't be published, registered against, submitted a mission entry for,
    or shown in any admin/public view that reads through this function."""
    doc = database.db["gc_campaigns"].find_one({"campaign_id": campaign_id})
    if doc is not None and doc.get("status") == "deleted":
        return None
    return doc


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalize a datetime to timezone-aware UTC.

    PyMongo returns naive ``datetime`` values on read even when they were
    written as UTC-aware, so any value pulled from ``gc_campaigns`` must be
    reinterpreted as UTC (not local server time) before comparison.
    """
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"Expected datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def is_publicly_active(campaign: dict, provider: dict | None, now: datetime | None = None) -> bool:
    """Server-side single source of truth for public visibility (Phase 2/3)."""
    if not campaign:
        return False
    now = _as_utc(now) or datetime.now(timezone.utc)

    if campaign.get("status") != "live":
        return False

    schedule = campaign.get("schedule") or {}
    starts_at = _as_utc(schedule.get("starts_at"))
    if not starts_at:
        return False
    if starts_at > now:
        return False

    ends_at = _as_utc(schedule.get("ends_at"))
    if ends_at:
        if now >= ends_at:
            return False

    destination = campaign.get("destination") or {}
    if not destination.get("ready"):
        return False

    if not provider or not provider_is_usable_for_results(provider):
        return False

    return True


def visibility_explanation(campaign: dict, provider: dict | None, now: datetime | None = None) -> dict:
    """Admin-preview-only breakdown of why a campaign is/isn't publicly visible."""
    now = _as_utc(now) or datetime.now(timezone.utc)
    schedule = campaign.get("schedule") or {}
    starts_at = _as_utc(schedule.get("starts_at"))
    ends_at = _as_utc(schedule.get("ends_at"))
    destination = campaign.get("destination") or {}

    reasons = []
    if campaign.get("status") != "live":
        reasons.append(f"status is '{campaign.get('status')}', not 'live'")
    if not starts_at:
        reasons.append("schedule.starts_at is not set")
    elif starts_at > now:
        reasons.append(f"scheduled to start at {starts_at.isoformat()}")
    if ends_at and now >= ends_at:
        reasons.append(f"ended at {ends_at.isoformat()}")
    if not destination.get("ready"):
        reasons.append("destination.ready is false")
    if not provider:
        reasons.append("linked provider does not exist")
    elif not provider_is_usable_for_results(provider):
        reasons.append("linked provider is inactive")

    return {
        "publicly_visible": len(reasons) == 0,
        "reasons": reasons,
    }


def _validate_schedule(schedule: dict) -> str | None:
    starts_at = schedule.get("starts_at")
    ends_at = schedule.get("ends_at")
    if not starts_at:
        return "missing_starts_at"
    if ends_at and ends_at <= starts_at:
        return "ends_at_before_starts_at"
    return None


def _validate_reward_rules(rules: list) -> str | None:
    """Structural validation delegates to reward_engine so every campaign
    type shares one rule-based reward engine instead of a rank-only check."""
    return reward_engine.validate_reward_rules(rules)


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _validate_body(body: dict, *, partial: bool = False) -> tuple[dict | None, str | None]:
    updates: dict = {}

    if not partial or "name" in body:
        name = (body.get("name") or "").strip()
        if not name:
            return None, "missing_name"
        updates["name"] = name

    if not partial or "type" in body:
        ctype = (body.get("type") or "").strip()
        if ctype not in CAMPAIGN_TYPES:
            return None, "invalid_type"
        updates["type"] = ctype

    campaign_type = updates.get("type") or (body.get("_existing_type"))

    for field in ("description", "button_text", "banner_url"):
        if field in body:
            updates[field] = (body.get(field) or "").strip()

    if "priority" in body:
        try:
            updates["priority"] = int(body["priority"])
        except (TypeError, ValueError):
            return None, "invalid_priority"

    if not partial or "schedule" in body:
        raw_schedule = body.get("schedule") or {}
        starts_at = _parse_dt(raw_schedule.get("starts_at"))
        ends_at = _parse_dt(raw_schedule.get("ends_at"))
        schedule = {
            "starts_at": starts_at,
            "ends_at": ends_at,
            "timezone": raw_schedule.get("timezone") or "Asia/Kuala_Lumpur",
        }
        err = _validate_schedule(schedule)
        if err:
            return None, err
        updates["schedule"] = schedule

    if not partial or "telegram" in body:
        raw_tg = body.get("telegram") or {}
        updates["telegram"] = {
            "require_identity": bool(raw_tg.get("require_identity", True)),
            "require_subscription": bool(raw_tg.get("require_subscription", True)),
            "channel_id": raw_tg.get("channel_id"),
            "channel_username": (raw_tg.get("channel_username") or "").strip(),
        }

    if not partial or "destination" in body:
        raw_dest = body.get("destination") or {}
        open_mode = (raw_dest.get("open_mode") or "telegram_web_app").strip()
        if open_mode not in OPEN_MODES:
            return None, "invalid_open_mode"
        if campaign_type and open_mode not in _ALLOWED_OPEN_MODES_BY_TYPE.get(campaign_type, OPEN_MODES):
            return None, "open_mode_not_allowed_for_type"
        provider_id = (raw_dest.get("provider_id") or "").strip()
        if provider_id:
            provider = get_provider(provider_id)
            if not provider:
                return None, "provider_not_found"
        updates["destination"] = {
            "provider_id": provider_id,
            "open_mode": open_mode,
            "path": (raw_dest.get("path") or "").strip(),
            "ready": bool(raw_dest.get("ready", False)),
        }

    # ---- Mission Pool (mechanic = mission_pool) ------------------------
    # The `mechanic` field is stamped SERVER-SIDE from the campaign type and
    # is never read from the request body: a client must not be able to turn
    # an arbitrary campaign into a Mission Pool one (or vice versa). Every
    # pre-existing campaign document has no `mechanic` field at all and
    # resolves to "standard_drop" through mission_pool.resolve_mechanic().
    if campaign_type:
        import mission_pool

        updates["mechanic"] = mission_pool.mechanic_for_type(campaign_type)

    if campaign_type == "mission_pool" or "mission_config" in body or "mission_pool" in body:
        import mission_pool

        if (updates.get("mechanic") or body.get("_existing_mechanic")) != mission_pool.MECHANIC_MISSION_POOL:
            return None, "mission_config_not_allowed_for_type"
        if not partial or "mission_config" in body:
            mission_config, code = mission_pool.validate_mission_config(body.get("mission_config"))
            if code:
                return None, code
            updates["mission_config"] = mission_config
        if not partial or "mission_pool" in body:
            pool_block, code = mission_pool.validate_mission_pool_config(body.get("mission_pool"))
            if code:
                return None, code
            updates["_mission_pool_validated"] = pool_block

    # ---- Campaign Registration (optional, orthogonal to `type`) --------
    # Any campaign can carry a registration config — it is not a campaign
    # type of its own, so this applies regardless of `campaign_type`. See
    # campaign_registration.py, which owns validation of this block the same
    # way mission_pool owns `mission_config`/`mission_pool` above.
    if "registration" in body:
        import campaign_registration

        registration, code = campaign_registration.validate_registration_config(body.get("registration"))
        if code:
            return None, code
        updates["registration"] = registration

    if "reward_config" in body:
        raw_reward = body.get("reward_config") or {}
        rules = raw_reward.get("rules") or []
        err = _validate_reward_rules(rules)
        if err:
            return None, err
        updates["reward_config"] = {
            "source": raw_reward.get("source") or "external_leaderboard",
            "approval_required": bool(raw_reward.get("approval_required", True)),
            "auto_allocate": bool(raw_reward.get("auto_allocate", False)),
            "rules": rules,
        }

    return updates, None


def _serialize(doc: dict) -> dict:
    """Returns a JSON-safe copy for the API response. Must never mutate
    ``doc`` itself — ``dict(doc)`` is a shallow copy, so nested dicts (like
    ``schedule``) are shared with the caller's original object; callers
    that still need the original datetime values (e.g. to also compute
    ``visibility_explanation``) would otherwise see them silently replaced
    with ISO strings."""
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for k in ("created_at", "updated_at"):
        if out.get(k):
            out[k] = out[k].isoformat()
    if out.get("schedule"):
        schedule = dict(out["schedule"])
        for k in ("starts_at", "ends_at"):
            if schedule.get(k):
                schedule[k] = schedule[k].isoformat()
        out["schedule"] = schedule
    return out


# ---------------------------------------------------------------------------
# Admin CRUD
# ---------------------------------------------------------------------------

@campaign_centre_bp.get("/api/admin/gc-campaigns")
def list_campaigns():
    _, err = _require_admin()
    if err:
        return err

    # Deleted campaigns are tombstoned in place (see delete_campaign) rather
    # than removed, so the default listing must explicitly exclude them —
    # status_filter can never request "deleted" itself since it's not in
    # CAMPAIGN_STATUSES, but it does overwrite this default when set.
    query: dict = {"status": {"$ne": "deleted"}}
    status_filter = (request.args.get("status") or "").strip()
    if status_filter:
        if status_filter not in CAMPAIGN_STATUSES:
            return jsonify({"status": "error", "code": "invalid_status"}), 400
        query["status"] = status_filter
    type_filter = (request.args.get("type") or "").strip()
    if type_filter:
        query["type"] = type_filter
    provider_filter = (request.args.get("provider_id") or "").strip()
    if provider_filter:
        query["destination.provider_id"] = provider_filter

    docs = list(database.db["gc_campaigns"].find(query, sort=[("priority", -1), ("created_at", -1)], limit=200))
    now = datetime.now(timezone.utc)

    # One aggregate query for every Mission Pool row on the page, not one per
    # row, so the Campaign Centre table can decide whether to show "End
    # Rewards" without an N+1 fan-out.
    import mission_pool

    mission_ids = [d["campaign_id"] for d in docs if mission_pool.is_mission_pool(d)]
    active_reward_counts = mission_pool.active_reward_counts(mission_ids) if mission_ids else {}

    import campaign_registration

    out = []
    for d in docs:
        item = _serialize(d)
        provider = get_provider((d.get("destination") or {}).get("provider_id") or "")
        item["effective_visibility"] = visibility_explanation(d, provider, now)
        if mission_pool.is_mission_pool(d):
            item["mission_active_rewards"] = active_reward_counts.get(d["campaign_id"], 0)
        if (d.get("registration") or {}).get("enabled"):
            item["registration_deep_link"] = campaign_registration.campaign_deep_link(d["campaign_id"])
        out.append(item)
    return jsonify({"status": "ok", "campaigns": out})


@campaign_centre_bp.post("/api/admin/gc-campaigns")
def create_campaign():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    campaign_id = (body.get("campaign_id") or "").strip()
    if not campaign_id:
        return jsonify({"status": "error", "code": "missing_campaign_id"}), 400
    # A previously-deleted campaign_id still occupies a (tombstoned) document
    # in gc_campaigns — checked explicitly so this case gets its own error
    # code; an id that's still actively in use falls through to the
    # unique-index/DuplicateKeyError handling below as before.
    existing_doc = database.db["gc_campaigns"].find_one({"campaign_id": campaign_id})
    if existing_doc is not None and existing_doc.get("status") == "deleted":
        return jsonify({"status": "error", "code": "campaign_id_previously_deleted"}), 409

    updates, code = _validate_body(body)
    if code:
        return jsonify({"status": "error", "code": code}), 400

    now = datetime.now(timezone.utc)
    mission_pool_block = updates.pop("_mission_pool_validated", None)
    if mission_pool_block is not None:
        import mission_pool

        updates["mission_pool"] = mission_pool.merge_mission_pool_config(None, mission_pool_block)
    doc = {
        "campaign_id": campaign_id,
        "status": "draft",
        "priority": updates.pop("priority", 100),
        "created_at": now,
        "updated_at": now,
        "created_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
        "updated_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
        **updates,
    }
    try:
        result = database.db["gc_campaigns"].insert_one(doc)
    except Exception as exc:
        if "duplicate" in str(exc).lower():
            return jsonify({"status": "error", "code": "duplicate_campaign_id"}), 409
        logger.exception("[CAMPAIGN_CENTRE] create_failed")
        return jsonify({"status": "error", "code": "internal_error"}), 500

    _log_audit("campaign_created", admin, campaign_id, {"type": doc.get("type")})
    log_funnel_event("campaign_created", campaign_id=campaign_id, campaign_type=doc.get("type"), source="admin")
    return jsonify({"status": "ok", "id": str(result.inserted_id), "campaign_id": campaign_id}), 201


@campaign_centre_bp.get("/api/admin/gc-campaigns/<campaign_id>")
def get_campaign_route(campaign_id: str):
    _, err = _require_admin()
    if err:
        return err
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    provider = get_provider((doc.get("destination") or {}).get("provider_id") or "")
    out = _serialize(doc)
    out["effective_visibility"] = visibility_explanation(doc, provider)
    # Mirrors list_campaigns' identical computation below — kept in sync so a
    # single-campaign fetch (e.g. the Campaign Detail admin page) never has to
    # fall back to the list endpoint just to learn a campaign's share link.
    if (doc.get("registration") or {}).get("enabled"):
        import campaign_registration

        out["registration_deep_link"] = campaign_registration.campaign_deep_link(doc["campaign_id"])
    return jsonify({"status": "ok", "campaign": out})


@campaign_centre_bp.put("/api/admin/gc-campaigns/<campaign_id>")
def update_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(force=True, silent=True) or {}
    if "type" not in body:
        body["_existing_type"] = doc.get("type")
    import mission_pool

    body["_existing_mechanic"] = mission_pool.resolve_mechanic(doc)
    updates, code = _validate_body(body, partial=True)
    if code:
        return jsonify({"status": "error", "code": code}), 400
    updates.pop("_existing_type", None)

    # Answers are graded at submission time (`is_correct` is stamped on the
    # entry) and never regraded, so changing the mission definition after
    # entries exist would let two identical answers get different eligibility
    # outcomes purely by arrival time. Freeze the definition instead. An
    # unchanged resubmission is allowed, because admin UIs routinely PUT the
    # whole document back.
    if "mission_config" in updates and updates["mission_config"] != doc.get("mission_config"):
        submitted = database.db[mission_pool.ENTRIES_COLLECTION].count_documents(
            {"campaign_id": campaign_id}
        )
        if submitted:
            return jsonify({
                "status": "error",
                "code": "mission_config_locked",
                "entries": submitted,
            }), 409

    mission_pool_block = updates.pop("_mission_pool_validated", None)
    if mission_pool_block is not None:
        # merge_mission_pool_config preserves every worker-owned processing
        # field (processing_generation, selection_seed, stage, counters), so
        # an admin edit can never break fencing or let a retry reshuffle an
        # already-selected winner set.
        updates["mission_pool"] = mission_pool.merge_mission_pool_config(
            doc.get("mission_pool"), mission_pool_block
        )

    if "status" in body:
        new_status = (body.get("status") or "").strip()
        if new_status not in CAMPAIGN_STATUSES:
            return jsonify({"status": "error", "code": "invalid_status"}), 400
        if new_status not in _VALID_STATUS_TRANSITIONS.get(doc.get("status", "draft"), set()):
            return jsonify({"status": "error", "code": "invalid_status_transition"}), 400
        updates["status"] = new_status

    updates["updated_at"] = datetime.now(timezone.utc)
    updates["updated_by"] = (admin or {}).get("usernameLower") or str((admin or {}).get("id", ""))

    # Guarded by status != "deleted" too, not just campaign_id: closes the
    # race where delete_campaign tombstones this exact campaign_id between
    # the get_campaign() read above and this write — without the guard the
    # update would land on the tombstone document itself (same campaign_id,
    # never removed from the collection) and could partially resurrect it.
    result = database.db["gc_campaigns"].update_one(
        {"campaign_id": campaign_id, "status": {"$ne": "deleted"}}, {"$set": updates}
    )
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    _log_audit("campaign_updated", admin, campaign_id, {"fields": list(updates.keys())})
    log_funnel_event("campaign_updated", campaign_id=campaign_id, campaign_type=doc.get("type"), source="admin")
    return jsonify({"status": "ok"})


def _transition(campaign_id: str, admin: dict, new_status: str, action: str):
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if new_status not in _VALID_STATUS_TRANSITIONS.get(doc.get("status", "draft"), set()):
        return jsonify({"status": "error", "code": "invalid_status_transition"}), 400
    if new_status == "live":
        if doc.get("type") in _SELF_REWARDING_TYPES:
            # Mission Pool has no external provider or destination URL; its
            # publish gate is its own mission/pool configuration instead.
            if not (doc.get("mission_config") or {}).get("mission_type"):
                return jsonify({"status": "error", "code": "mission_config_required"}), 400
            if not (doc.get("mission_pool") or {}).get("pool_id"):
                return jsonify({"status": "error", "code": "mission_pool_config_required"}), 400
        else:
            if doc.get("type") in _REWARD_DRIVEN_TYPES and not (doc.get("reward_config") or {}).get("rules"):
                return jsonify({"status": "error", "code": "reward_rules_required"}), 400
            # A campaign whose sole purpose is Campaign Registration (e.g. a
            # standalone lucky-draw sign-up) has no external destination to
            # be "ready" — campaign_registration.registration_is_open()
            # never consults destination/provider either. Requiring one here
            # would make such a campaign impossible to publish through the
            # very Admin Dashboard flow that creates it with
            # destination.ready defaulted to false.
            registration_only = bool((doc.get("registration") or {}).get("enabled"))
            if not registration_only:
                provider = get_provider((doc.get("destination") or {}).get("provider_id") or "")
                if not (doc.get("destination") or {}).get("ready"):
                    return jsonify({"status": "error", "code": "destination_not_ready"}), 400
                if not provider_is_usable_for_results(provider):
                    return jsonify({"status": "error", "code": "provider_inactive"}), 400
    # status != "deleted" closes the same race as update_campaign's guard
    # above: delete_campaign never removes the document, it tombstones it in
    # place, so a blind {"campaign_id": campaign_id} filter here could land
    # on — and partially resurrect — a tombstone that was written between
    # the get_campaign() read above and this write.
    result = database.db["gc_campaigns"].update_one(
        {"campaign_id": campaign_id, "status": {"$ne": "deleted"}},
        {"$set": {"status": new_status, "updated_at": datetime.now(timezone.utc)}},
    )
    if result.matched_count == 0:
        # The campaign was permanently deleted between the read above and
        # this write (see delete_campaign) — report not_found rather than a
        # false "ok" for a status change that never actually landed.
        return jsonify({"status": "error", "code": "not_found"}), 404
    _log_audit(action, admin, campaign_id, {"new_status": new_status})
    log_funnel_event(action, campaign_id=campaign_id, campaign_type=doc.get("type"), source="admin")
    return jsonify({"status": "ok", "campaign_status": new_status})


@campaign_centre_bp.post("/api/admin/gc-campaigns/<campaign_id>/publish")
def publish_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    return _transition(campaign_id, admin, "live", "campaign_published")


@campaign_centre_bp.post("/api/admin/gc-campaigns/<campaign_id>/pause")
def pause_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    return _transition(campaign_id, admin, "paused", "campaign_paused")


@campaign_centre_bp.post("/api/admin/gc-campaigns/<campaign_id>/archive")
def archive_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    return _transition(campaign_id, admin, "archived", "campaign_archived")


@campaign_centre_bp.post("/api/admin/gc-campaigns/<campaign_id>/duplicate")
def duplicate_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    body = request.get_json(force=True, silent=True) or {}
    new_campaign_id = (body.get("campaign_id") or f"{campaign_id}-copy").strip()
    # One raw lookup (not get_campaign(), which hides tombstones) covers
    # both conflict cases: an id still actively in use, and an id that was
    # permanently reserved by a previous deletion (see _TOMBSTONE_UNSET_FIELDS).
    existing_new = database.db["gc_campaigns"].find_one({"campaign_id": new_campaign_id})
    if existing_new is not None:
        if existing_new.get("status") == "deleted":
            return jsonify({"status": "error", "code": "campaign_id_previously_deleted"}), 409
        return jsonify({"status": "error", "code": "duplicate_campaign_id"}), 409

    new_doc = dict(doc)
    new_doc.pop("_id", None)
    now = datetime.now(timezone.utc)
    new_doc.update({
        "campaign_id": new_campaign_id,
        "status": "draft",
        "created_at": now,
        "updated_at": now,
        "created_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
        "updated_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
    })
    # Duplicated campaigns never inherit public readiness.
    dest = dict(new_doc.get("destination") or {})
    dest["ready"] = False
    new_doc["destination"] = dest

    # ...nor any worker-owned Mission Pool processing state. Copying the block
    # verbatim would give the new draft the source campaign's
    # processing_stage (`completed` for a finished one), its selection_seed
    # and its processing_generation — after which find_due_campaigns would
    # skip it forever and none of its entries would ever be rewarded.
    import mission_pool

    if mission_pool.is_mission_pool(doc):
        new_doc["mission_pool"] = mission_pool.duplicated_mission_pool_config(
            doc.get("mission_pool")
        )

    result = database.db["gc_campaigns"].insert_one(new_doc)
    _log_audit("campaign_duplicated", admin, new_campaign_id, {"source": campaign_id})
    return jsonify({"status": "ok", "id": str(result.inserted_id), "campaign_id": new_campaign_id}), 201


@campaign_centre_bp.delete("/api/admin/campaign-centre/campaigns/<campaign_id>")
def delete_campaign(campaign_id: str):
    """Permanently deletes a campaign. Only allowed once the campaign is no
    longer live/paused (see ``_DELETABLE_STATUSES``) — the status check and
    the delete happen in one atomic ``find_one_and_update`` (status="deleted"
    tombstone, see ``_TOMBSTONE_UNSET_FIELDS``) so a campaign cannot be
    published in the window between checking its status and deleting it.

    The document is never actually removed from ``gc_campaigns`` — it is
    atomically converted in place into a minimal tombstone. This is what
    makes the deletion, and the campaign_id reservation, one indivisible
    operation: the collection's pre-existing unique index on campaign_id
    (``ux_gc_campaigns_campaign_id``) permanently blocks any future insert
    under that id the instant the tombstone exists, with no second
    collection write that could itself fail and reopen a reuse window (see
    ``_TOMBSTONE_UNSET_FIELDS`` for why). ``get_campaign()`` treats a
    status="deleted" document as not found everywhere it's called from, so
    the campaign is simultaneously invisible/inert to every admin, public,
    and processing code path the moment this update lands.

    Historical winner/claim/reward/registration rows (``mission_entries``,
    ``mission_identity_claims``, ``campaign_rewards``, ``tournament_results``,
    ``campaign_registrations``) are deliberately left in place for
    traceability — they stay valid, campaign_id-keyed rows after the parent
    campaign becomes a tombstone. Only campaign-exclusive *operational* state
    that has no meaning without the campaign (``campaign_registration_state``
    — per-user dismissal/reminder state) is cascade-deleted, scoped by the
    exact campaign_id. Shared/provider-scoped collections (``gc_providers``,
    ``tournament_nonces``, ``campaign_provider_integration_status``,
    ``campaign_subscription_cache``) are untouched — they are keyed by
    provider/channel, not owned by any single campaign.
    """
    admin, err = _require_admin()
    if err:
        return err

    now = datetime.now(timezone.utc)
    admin_identity = (admin or {}).get("usernameLower") or str((admin or {}).get("id", ""))

    # The one atomic operation that both authorizes the deletion (status
    # must still be draft/archived/ended) and performs it. return_document=
    # BEFORE hands back the full pre-tombstone document — the correct
    # snapshot source below — while the write already in flight strips it
    # down to a minimal tombstone; there is no window between "checked
    # eligible" and "deleted" for another request to land in.
    doc = database.db["gc_campaigns"].find_one_and_update(
        {"campaign_id": campaign_id, "status": {"$in": sorted(_DELETABLE_STATUSES)}},
        {
            "$set": {
                "status": "deleted",
                "deleted_at": now,
                "deleted_by": admin_identity,
                "updated_at": now,
            },
            "$unset": {field: "" for field in _TOMBSTONE_UNSET_FIELDS},
        },
        return_document=ReturnDocument.BEFORE,
    )
    if doc is None:
        # No document matched the atomic filter — either the campaign_id
        # was never created, it's already a tombstone (status="deleted" is
        # not in _DELETABLE_STATUSES, so both look identical to the filter
        # above), or it's currently live/paused/scheduled. A raw read
        # (bypassing get_campaign(), which would itself hide the tombstone)
        # is needed to tell these apart for the right status code.
        existing = database.db["gc_campaigns"].find_one({"campaign_id": campaign_id})
        if existing is None or existing.get("status") == "deleted":
            return jsonify({"status": "error", "code": "not_found"}), 404
        return jsonify({
            "status": "error",
            "code": "invalid_status_for_deletion",
            "campaign_status": existing.get("status"),
        }), 409

    # The campaign is already tombstoned at this point — the atomic
    # find_one_and_update above is the only step that can turn this request
    # into a 404/409, and its returned `doc` (the state immediately BEFORE
    # the tombstone write, not a fresh read that could race with something
    # else) is the source of truth for everything logged below. Every
    # remaining step is best-effort bookkeeping for a deletion that has
    # already happened, so none of it may surface as a 500 — that would
    # tell the admin the delete failed and invite a retry of a campaign
    # that's already gone. Each step is isolated and any failure is logged
    # at error level for operational follow-up instead of raising.
    cleanup_warnings: list[str] = []

    try:
        database.db["campaign_registration_state"].delete_many({"campaign_id": campaign_id})
    except Exception:
        logger.error(
            "[CAMPAIGN_CENTRE] post_delete_registration_state_cleanup_failed campaign_id=%s",
            campaign_id, exc_info=True,
        )
        cleanup_warnings.append("registration_state_cleanup_failed")

    snapshot = _serialize(doc)
    try:
        _log_audit("campaign_deleted", admin, campaign_id, {
            "title": doc.get("name"),
            "previous_status": doc.get("status"),
            "snapshot": snapshot,
        })
    except Exception:
        # _log_audit already swallows its own exceptions and logs a warning;
        # this guard only covers that contract changing out from under us.
        logger.error(
            "[CAMPAIGN_CENTRE] post_delete_audit_log_failed campaign_id=%s",
            campaign_id, exc_info=True,
        )
        cleanup_warnings.append("audit_log_failed")

    try:
        log_funnel_event(
            "campaign_deleted", campaign_id=campaign_id, campaign_type=doc.get("type"),
            source="admin", previous_status=doc.get("status"),
        )
    except Exception:
        logger.error(
            "[CAMPAIGN_CENTRE] post_delete_funnel_event_failed campaign_id=%s",
            campaign_id, exc_info=True,
        )
        cleanup_warnings.append("funnel_event_failed")

    response = {"status": "ok", "campaign_id": campaign_id}
    if cleanup_warnings:
        # The campaign IS deleted — this only flags that some bookkeeping
        # (audit trail, funnel event, or owned-state cleanup) needs manual
        # follow-up. The admin must not read this as "try again".
        response["cleanup_warnings"] = cleanup_warnings
    return jsonify(response), 200


@campaign_centre_bp.get("/api/admin/gc-campaigns/<campaign_id>/preview")
def preview_campaign(campaign_id: str):
    """Admin-only preview. Never changes campaign visibility."""
    _, err = _require_admin()
    if err:
        return err
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    provider = get_provider((doc.get("destination") or {}).get("provider_id") or "")
    explanation = visibility_explanation(doc, provider)

    card = {
        "campaign_id": doc.get("campaign_id"),
        "name": doc.get("name"),
        "type": doc.get("type"),
        "description": doc.get("description", ""),
        "button_text": doc.get("button_text", ""),
        "banner_url": doc.get("banner_url", ""),
    }
    badges = []
    if doc.get("status") == "draft":
        badges.append("draft")
    if doc.get("status") == "scheduled":
        badges.append("scheduled")
    if not (doc.get("destination") or {}).get("ready"):
        badges.append("destination_not_ready")
    if provider and not provider_is_usable_for_results(provider):
        badges.append("provider_inactive")

    log_funnel_event("campaign_previewed", campaign_id=campaign_id, campaign_type=doc.get("type"), source="admin")
    return jsonify({
        "status": "ok",
        "card": card,
        "admin_badges": badges,
        "effective_visibility": explanation,
    })


# ---------------------------------------------------------------------------
# Public user-facing API
# ---------------------------------------------------------------------------

_PUBLIC_FIELDS = {"campaign_id", "name", "type", "description", "button_text", "banner_url", "priority"}


def _public_card(doc: dict) -> dict:
    card = {k: doc.get(k) for k in _PUBLIC_FIELDS}
    telegram = doc.get("telegram") or {}
    card["telegram"] = {
        "require_identity": bool(telegram.get("require_identity", True)),
        "require_subscription": bool(telegram.get("require_subscription", True)),
        "channel_username": telegram.get("channel_username", ""),
    }
    destination = doc.get("destination") or {}
    card["open_mode"] = destination.get("open_mode", "telegram_web_app")
    return card


@campaign_public_bp.get("/api/campaigns/active")
def list_active_campaigns():
    now = datetime.now(timezone.utc)
    # Explicitly scoped to the standard_drop mechanic. Mission Pool
    # campaigns have their own discovery endpoint (mission_pool.py) and must
    # never appear in this list, so the payload every existing Mini App build
    # receives here is byte-for-byte what it was before Mission Pool existed.
    docs = list(
        database.db["gc_campaigns"].find(
            {
                "status": "live",
                "type": {"$in": CAMPAIGN_TYPES},
                "$or": [{"mechanic": {"$exists": False}}, {"mechanic": "standard_drop"}],
            },
            sort=[("priority", -1), ("schedule.starts_at", 1)],
            limit=50,
        )
    )
    active = []
    for d in docs:
        provider = get_provider((d.get("destination") or {}).get("provider_id") or "")
        if is_publicly_active(d, provider, now):
            active.append(_public_card(d))
            # This endpoint is intentionally unauthenticated (public
            # visibility must not require identity), so campaign_view is
            # recorded without a telegram_user_id. The natural call
            # frequency is bounded by Mini App opens — the widget fetches
            # this once per load, never on a poll loop — so no additional
            # dedup/rate-limiting is needed to avoid event spam here.
            log_funnel_event("campaign_view", campaign_id=d["campaign_id"],
                              campaign_type=d.get("type"), source="miniapp")

    return jsonify({"status": "ok", "campaigns": active})


@campaign_public_bp.post("/api/campaigns/<campaign_id>/play")
def play_campaign(campaign_id: str):
    """Play/open button flow: resolves the caller against the authenticated
    Mini App session, verifies subscription server-side if required, then
    returns the destination URL to open. No raw UID/user_id from the client
    is ever trusted for identity — see miniapp_identity.py. For a tournament
    campaign the destination URL is the Phase-1 UID deep link; the tournament
    website never receives Telegram initData from this flow."""
    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    doc = get_campaign(campaign_id)
    provider = get_provider((doc or {}).get("destination", {}).get("provider_id") or "") if doc else None
    campaign_type = doc.get("type") if doc else None

    log_funnel_event("campaign_click", campaign_id=campaign_id, user_id=uid, campaign_type=campaign_type, source="miniapp")

    if not doc or not is_publicly_active(doc, provider, datetime.now(timezone.utc)):
        log_funnel_event("destination_blocked", campaign_id=campaign_id, user_id=uid, campaign_type=campaign_type,
                          source="miniapp", status="fail", reason="campaign_unavailable")
        return jsonify({"status": "error", "code": "campaign_unavailable"}), 404

    telegram_cfg = doc.get("telegram") or {}
    if telegram_cfg.get("require_subscription", True):
        from subscription_gate import verify_campaign_subscription

        force_refresh = bool((request.get_json(silent=True) or {}).get("force_refresh"))
        gate = verify_campaign_subscription(doc, uid, force_refresh=force_refresh)
        if not gate.get("subscribed"):
            log_funnel_event("subscription_fail", campaign_id=campaign_id, user_id=uid, reason=gate.get("reason"))
            log_funnel_event("destination_blocked", campaign_id=campaign_id, user_id=uid, campaign_type=campaign_type,
                              source="miniapp", status="fail", reason="subscription_required")
            return jsonify({
                "status": "error",
                "code": "subscription_required",
                "channel_username": telegram_cfg.get("channel_username", ""),
            }), 403
        log_funnel_event("subscription_pass", campaign_id=campaign_id, user_id=uid)

    from campaign_providers import build_effective_url

    destination_url = build_effective_url(provider, doc, uid) if provider else None
    if not destination_url:
        log_funnel_event("destination_blocked", campaign_id=campaign_id, user_id=uid, campaign_type=campaign_type,
                          source="miniapp", status="fail", reason="no_destination_url")
        return jsonify({"status": "error", "code": "campaign_unavailable"}), 404

    log_funnel_event("destination_open", campaign_id=campaign_id, user_id=uid, campaign_type=campaign_type, source="miniapp")
    return jsonify({
        "status": "ok",
        "open_mode": (doc.get("destination") or {}).get("open_mode", "telegram_web_app"),
        "url": destination_url,
    })


@campaign_public_bp.post("/api/campaigns/<campaign_id>/subscribe-check")
def subscribe_check(campaign_id: str):
    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "campaign_unavailable"}), 404

    from subscription_gate import verify_campaign_subscription

    force_refresh = bool((request.get_json(silent=True) or {}).get("force_refresh"))
    log_funnel_event("subscription_check", campaign_id=campaign_id, user_id=uid)
    gate = verify_campaign_subscription(doc, uid, force_refresh=force_refresh)
    log_funnel_event("subscription_pass" if gate.get("subscribed") else "subscription_fail",
                      campaign_id=campaign_id, user_id=uid)
    return jsonify({"status": "ok", "subscribed": gate.get("subscribed", False)})
