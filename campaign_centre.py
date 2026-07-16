"""Campaign Centre — AP-owned campaign gateway (tournaments, external
subscription-verification campaigns, external website campaigns).

This is intentionally a separate collection/namespace from the pre-existing
``campaigns`` collection (see campaigns.py / campaign_engine.py), which is an
unrelated segment-audience marketing/voucher-targeting tool. Reusing that
collection or its ``/api/admin/campaigns`` routes would collide with a
production feature, so the Campaign Centre admin API is namespaced under
``/api/admin/campaign-centre/...``. Public and integration endpoints use the
exact paths from the spec since those do not collide with anything existing.

Collection: ``gc_campaigns``
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import database
from campaign_providers import get_provider, provider_is_usable_for_results

logger = logging.getLogger(__name__)

campaign_centre_bp = Blueprint("campaign_centre", __name__)
campaign_public_bp = Blueprint("campaign_public", __name__)

CAMPAIGN_TYPES = ["tournament", "external_subscription_verification", "external_website"]
CAMPAIGN_STATUSES = ["draft", "scheduled", "live", "paused", "ended", "archived"]
OPEN_MODES = ["telegram_web_app", "external_url"]

# Which open modes are valid for which campaign type.
_ALLOWED_OPEN_MODES_BY_TYPE = {
    "tournament": {"telegram_web_app", "external_url"},
    "external_subscription_verification": {"external_url"},
    "external_website": {"external_url", "telegram_web_app"},
}

_VALID_STATUS_TRANSITIONS = {
    "draft": {"draft", "scheduled", "live", "archived"},
    "scheduled": {"scheduled", "live", "draft", "paused", "archived"},
    "live": {"live", "paused", "ended", "archived"},
    "paused": {"paused", "live", "ended", "archived"},
    "ended": {"ended", "archived"},
    "archived": {"archived"},
}


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


def log_funnel_event(event: str, *, campaign_id: str, user_id: int | None = None, **extra) -> None:
    try:
        database.db["campaign_events"].insert_one({
            "event": event,
            "campaign_id": campaign_id,
            "user_id": user_id,
            "at": datetime.now(timezone.utc),
            **extra,
        })
    except Exception:
        logger.warning("[CAMPAIGN_CENTRE] event_log_failed event=%s", event, exc_info=True)


def get_campaign(campaign_id: str) -> dict | None:
    return database.db["gc_campaigns"].find_one({"campaign_id": campaign_id})


def is_publicly_active(campaign: dict, provider: dict | None, now: datetime | None = None) -> bool:
    """Server-side single source of truth for public visibility (Phase 2/3)."""
    if not campaign:
        return False
    now = now or datetime.now(timezone.utc)

    if campaign.get("status") != "live":
        return False

    schedule = campaign.get("schedule") or {}
    starts_at = schedule.get("starts_at")
    if not starts_at:
        return False
    if isinstance(starts_at, datetime) and starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=timezone.utc)
    if starts_at > now:
        return False

    ends_at = schedule.get("ends_at")
    if ends_at:
        if isinstance(ends_at, datetime) and ends_at.tzinfo is None:
            ends_at = ends_at.replace(tzinfo=timezone.utc)
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
    now = now or datetime.now(timezone.utc)
    schedule = campaign.get("schedule") or {}
    starts_at = schedule.get("starts_at")
    ends_at = schedule.get("ends_at")
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
    if not isinstance(rules, list):
        return "invalid_rules"
    seen_ids = set()
    ranges = []
    for rule in rules:
        rule_id = rule.get("rule_id")
        if not rule_id or rule_id in seen_ids:
            return "duplicate_or_missing_rule_id"
        seen_ids.add(rule_id)
        try:
            min_rank = int(rule["min_rank"])
            max_rank = int(rule["max_rank"])
        except (KeyError, TypeError, ValueError):
            return "invalid_rank_range"
        if min_rank < 1 or max_rank < min_rank:
            return "invalid_rank_range"
        if not rule.get("pool_id"):
            return "missing_pool_id"
        ranges.append((min_rank, max_rank))

    ranges.sort()
    for i in range(1, len(ranges)):
        if ranges[i][0] <= ranges[i - 1][1]:
            return "overlapping_rank_ranges"
    return None


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
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for k in ("created_at", "updated_at"):
        if out.get(k):
            out[k] = out[k].isoformat()
    schedule = out.get("schedule") or {}
    for k in ("starts_at", "ends_at"):
        if schedule.get(k):
            schedule[k] = schedule[k].isoformat()
    return out


# ---------------------------------------------------------------------------
# Admin CRUD
# ---------------------------------------------------------------------------

@campaign_centre_bp.get("/api/admin/campaign-centre/campaigns")
def list_campaigns():
    _, err = _require_admin()
    if err:
        return err

    query: dict = {}
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
    out = []
    for d in docs:
        item = _serialize(d)
        provider = get_provider((d.get("destination") or {}).get("provider_id") or "")
        item["effective_visibility"] = visibility_explanation(d, provider, now)
        out.append(item)
    return jsonify({"status": "ok", "campaigns": out})


@campaign_centre_bp.post("/api/admin/campaign-centre/campaigns")
def create_campaign():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    campaign_id = (body.get("campaign_id") or "").strip()
    if not campaign_id:
        return jsonify({"status": "error", "code": "missing_campaign_id"}), 400

    updates, code = _validate_body(body)
    if code:
        return jsonify({"status": "error", "code": code}), 400

    now = datetime.now(timezone.utc)
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
    return jsonify({"status": "ok", "id": str(result.inserted_id), "campaign_id": campaign_id}), 201


@campaign_centre_bp.get("/api/admin/campaign-centre/campaigns/<campaign_id>")
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
    return jsonify({"status": "ok", "campaign": out})


@campaign_centre_bp.put("/api/admin/campaign-centre/campaigns/<campaign_id>")
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
    updates, code = _validate_body(body, partial=True)
    if code:
        return jsonify({"status": "error", "code": code}), 400
    updates.pop("_existing_type", None)

    if "status" in body:
        new_status = (body.get("status") or "").strip()
        if new_status not in CAMPAIGN_STATUSES:
            return jsonify({"status": "error", "code": "invalid_status"}), 400
        if new_status not in _VALID_STATUS_TRANSITIONS.get(doc.get("status", "draft"), set()):
            return jsonify({"status": "error", "code": "invalid_status_transition"}), 400
        updates["status"] = new_status

    updates["updated_at"] = datetime.now(timezone.utc)
    updates["updated_by"] = (admin or {}).get("usernameLower") or str((admin or {}).get("id", ""))

    database.db["gc_campaigns"].update_one({"campaign_id": campaign_id}, {"$set": updates})
    _log_audit("campaign_updated", admin, campaign_id, {"fields": list(updates.keys())})
    return jsonify({"status": "ok"})


def _transition(campaign_id: str, admin: dict, new_status: str, action: str):
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if new_status not in _VALID_STATUS_TRANSITIONS.get(doc.get("status", "draft"), set()):
        return jsonify({"status": "error", "code": "invalid_status_transition"}), 400
    if new_status == "live":
        provider = get_provider((doc.get("destination") or {}).get("provider_id") or "")
        if doc.get("type") == "tournament" and not (doc.get("reward_config") or {}).get("rules"):
            return jsonify({"status": "error", "code": "reward_rules_required"}), 400
        if not (doc.get("destination") or {}).get("ready"):
            return jsonify({"status": "error", "code": "destination_not_ready"}), 400
        if not provider_is_usable_for_results(provider):
            return jsonify({"status": "error", "code": "provider_inactive"}), 400
    database.db["gc_campaigns"].update_one(
        {"campaign_id": campaign_id},
        {"$set": {"status": new_status, "updated_at": datetime.now(timezone.utc)}},
    )
    _log_audit(action, admin, campaign_id, {"new_status": new_status})
    return jsonify({"status": "ok", "campaign_status": new_status})


@campaign_centre_bp.post("/api/admin/campaign-centre/campaigns/<campaign_id>/publish")
def publish_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    return _transition(campaign_id, admin, "live", "campaign_published")


@campaign_centre_bp.post("/api/admin/campaign-centre/campaigns/<campaign_id>/pause")
def pause_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    return _transition(campaign_id, admin, "paused", "campaign_paused")


@campaign_centre_bp.post("/api/admin/campaign-centre/campaigns/<campaign_id>/archive")
def archive_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    return _transition(campaign_id, admin, "archived", "campaign_archived")


@campaign_centre_bp.post("/api/admin/campaign-centre/campaigns/<campaign_id>/duplicate")
def duplicate_campaign(campaign_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = get_campaign(campaign_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    body = request.get_json(force=True, silent=True) or {}
    new_campaign_id = (body.get("campaign_id") or f"{campaign_id}-copy").strip()
    if get_campaign(new_campaign_id):
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

    result = database.db["gc_campaigns"].insert_one(new_doc)
    _log_audit("campaign_duplicated", admin, new_campaign_id, {"source": campaign_id})
    return jsonify({"status": "ok", "id": str(result.inserted_id), "campaign_id": new_campaign_id}), 201


@campaign_centre_bp.get("/api/admin/campaign-centre/campaigns/<campaign_id>/preview")
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
    docs = list(
        database.db["gc_campaigns"].find(
            {"status": "live", "type": {"$in": CAMPAIGN_TYPES}},
            sort=[("priority", -1), ("schedule.starts_at", 1)],
            limit=50,
        )
    )
    active = []
    for d in docs:
        provider = get_provider((d.get("destination") or {}).get("provider_id") or "")
        if is_publicly_active(d, provider, now):
            active.append(_public_card(d))

    return jsonify({"status": "ok", "campaigns": active})


def _verified_telegram_user_id() -> tuple[int | None, tuple | None]:
    """Derive the caller's Telegram user id from verified Mini App initData.
    Never trust a raw uid/user_id query or body parameter."""
    import json as _json

    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data

    init_data_raw = extract_raw_init_data_from_query(request) or (request.get_json(silent=True) or {}).get("init_data", "")
    if not init_data_raw:
        return None, (jsonify({"status": "error", "code": "missing_init_data"}), 400)
    ok, data, reason = verify_telegram_init_data(init_data_raw)
    if not ok:
        return None, (jsonify({"status": "error", "code": f"init_data_invalid:{reason}"}), 401)
    try:
        user_json = _json.loads(data.get("user", "{}"))
        uid = int(user_json.get("id"))
    except Exception:
        return None, (jsonify({"status": "error", "code": "invalid_user"}), 401)
    return uid, None


@campaign_public_bp.post("/api/campaigns/<campaign_id>/play")
def play_campaign(campaign_id: str):
    """Tournament button flow: verifies identity + subscription server-side,
    then returns the destination URL to open. No raw UID URLs are accepted
    from the client — the Telegram user id is derived from verified initData."""
    uid, err = _verified_telegram_user_id()
    if err:
        return err

    doc = get_campaign(campaign_id)
    provider = get_provider((doc or {}).get("destination", {}).get("provider_id") or "") if doc else None

    if not doc or not is_publicly_active(doc, provider, datetime.now(timezone.utc)):
        log_funnel_event("play_click", campaign_id=campaign_id, user_id=uid, result="unavailable")
        return jsonify({"status": "error", "code": "campaign_unavailable"}), 404

    log_funnel_event("play_click", campaign_id=campaign_id, user_id=uid, result="attempt")

    telegram_cfg = doc.get("telegram") or {}
    if telegram_cfg.get("require_subscription", True):
        from subscription_gate import verify_campaign_subscription

        force_refresh = bool((request.get_json(silent=True) or {}).get("force_refresh"))
        gate = verify_campaign_subscription(doc, uid, force_refresh=force_refresh)
        if not gate.get("subscribed"):
            log_funnel_event("subscription_fail", campaign_id=campaign_id, user_id=uid, reason=gate.get("reason"))
            return jsonify({
                "status": "error",
                "code": "subscription_required",
                "channel_username": telegram_cfg.get("channel_username", ""),
            }), 403
        log_funnel_event("subscription_pass", campaign_id=campaign_id, user_id=uid)

    from campaign_providers import build_effective_url

    destination_url = build_effective_url(provider, doc, uid) if provider else None
    if not destination_url:
        return jsonify({"status": "error", "code": "campaign_unavailable"}), 404

    log_funnel_event("destination_open", campaign_id=campaign_id, user_id=uid)
    return jsonify({
        "status": "ok",
        "open_mode": (doc.get("destination") or {}).get("open_mode", "telegram_web_app"),
        "url": destination_url,
    })


@campaign_public_bp.post("/api/campaigns/<campaign_id>/subscribe-check")
def subscribe_check(campaign_id: str):
    uid, err = _verified_telegram_user_id()
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
