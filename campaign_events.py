"""Canonical, generic, append-only Campaign Centre event ledger.

Collection: ``campaign_events``. Single source of truth for campaign
analytics and operational/admin audit reporting across every campaign type
(tournament today; referral/lucky_draw/mission/... in the future) — nothing
here is tournament-specific.

This replaces the ad-hoc ``database.db["campaign_events"].insert_one(...)``
calls that used to be scattered across campaign_centre.py/
subscription_gate.py/subscription_verification_api.py with one reusable,
sanitizing writer: ``emit_campaign_event``.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import database

logger = logging.getLogger(__name__)

EVENT_TYPES = frozenset({
    # Campaign lifecycle
    "campaign_created", "campaign_updated", "campaign_published",
    "campaign_paused", "campaign_archived", "campaign_previewed",
    # Public funnel
    "campaign_view", "campaign_click", "subscribe_click",
    "subscription_check", "subscription_pass", "subscription_fail",
    "destination_open", "destination_blocked",
    # Provider integration
    "provider_created", "provider_updated", "provider_activated",
    "provider_deactivated", "provider_signature_failed",
    "provider_nonce_replay", "provider_request_rejected",
    # Tournament result
    "leaderboard_received", "leaderboard_duplicate", "leaderboard_rejected",
    "leaderboard_approved", "leaderboard_correction_requested",
    "leaderboard_version_conflict",
    # Rewards
    "reward_created", "reward_rule_matched", "reward_rule_unmatched",
    "voucher_reserved", "voucher_assigned", "voucher_out_of_stock",
    "reward_viewed", "voucher_copied", "reward_expired",
    # Pool operations
    "reward_pool_created", "reward_pool_updated", "reward_pool_upload",
    "reward_pool_scope_rejected", "reward_pool_allocation_rejected",
    # Event banner (event_banner.py — image-only Mini App top banner)
    "event_banner_impression", "event_banner_click", "event_banner_image_error",
    # Platform Finder (post-voucher-claim "Find Where To Play" flow)
    "platform_finder_shown", "platform_finder_opened", "platform_search_copied",
    "platform_finder_help_clicked", "platform_search_google_clicked",
})

# Fields that must never be persisted, wherever they appear in metadata.
_SENSITIVE_KEYS = frozenset({
    "bot_token", "token", "secret", "provider_secret", "secret_env_var",
    "init_data", "initdata", "x-telegram-init-data", "signature",
    "x-signature", "admin_secret", "password", "authorization",
    "voucher_code", "code",
})

_MAX_METADATA_BYTES = 2000
_MAX_STRING_LEN = 300


def _sanitize_value(value):
    if isinstance(value, dict):
        return _sanitize_metadata(value)
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(v) for v in list(value)[:20]]
    if isinstance(value, str) and len(value) > _MAX_STRING_LEN:
        return value[:_MAX_STRING_LEN] + "…(truncated)"
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)[:_MAX_STRING_LEN]


def _sanitize_metadata(metadata: dict | None) -> dict:
    """Strip sensitive keys and bound overall size. Never raises — a
    sanitization bug must not take down the caller's real business action."""
    if not metadata or not isinstance(metadata, dict):
        return {}
    try:
        out = {}
        for key, value in metadata.items():
            if not isinstance(key, str):
                continue
            if key.lower() in _SENSITIVE_KEYS:
                continue
            out[key] = _sanitize_value(value)
            if len(out) >= 30:
                break
        # Bound total serialized size defensively (avoid huge documents).
        import json as _json

        try:
            if len(_json.dumps(out, default=str)) > _MAX_METADATA_BYTES:
                out = {"_truncated": True}
        except Exception:
            out = {"_truncated": True}
        return out
    except Exception:
        logger.warning("[CAMPAIGN_EVENTS] metadata_sanitize_failed", exc_info=True)
        return {}


def mask_code_suffix(code: str | None) -> str | None:
    """Only ever store a masked suffix of a voucher code, never the code
    itself — used by callers that want to reference "which code" in an
    event without persisting the redeemable value."""
    if not code:
        return None
    code = str(code)
    return f"***{code[-4:]}" if len(code) > 4 else "***"


def deterministic_event_id(*parts: str) -> str:
    """Stable id for idempotent events, e.g.
    deterministic_event_id("reward_created", reward_id)."""
    raw = "|".join(["campaign-event", *[str(p) for p in parts]])
    return "ce_" + hashlib.sha256(raw.encode()).hexdigest()[:24]


def _ensure_indexes() -> None:
    try:
        col = database.db["campaign_events"]
        col.create_index(
            [("event_id", 1)], name="ux_campaign_events_event_id",
            unique=True, partialFilterExpression={"event_id": {"$exists": True}},
        )
        col.create_index([("campaign_id", 1), ("occurred_at", -1)], name="ix_campaign_events_campaign_time")
        col.create_index([("event_type", 1), ("occurred_at", -1)], name="ix_campaign_events_type_time")
        col.create_index([("telegram_user_id", 1), ("occurred_at", -1)], name="ix_campaign_events_user_time")
        col.create_index([("provider_id", 1), ("occurred_at", -1)], name="ix_campaign_events_provider_time")
        col.create_index([("submission_id", 1), ("occurred_at", -1)], name="ix_campaign_events_submission_time")
        col.create_index([("reward_id", 1), ("occurred_at", -1)], name="ix_campaign_events_reward_time")
        col.create_index([("pool_id", 1), ("occurred_at", -1)], name="ix_campaign_events_pool_time")
        col.create_index([("source", 1), ("occurred_at", -1)], name="ix_campaign_events_source_time")
    except Exception:
        logger.warning("[CAMPAIGN_EVENTS] index creation failed", exc_info=True)


_ensure_indexes()


def emit_campaign_event(
    *,
    event_type: str,
    campaign_id: str | None = None,
    campaign_type: str | None = None,
    provider_id: str | None = None,
    telegram_user_id: int | None = None,
    submission_id: str | None = None,
    reward_id: str | None = None,
    pool_id: str | None = None,
    source: str | None = None,
    status: str = "success",
    reason: str | None = None,
    metadata: dict | None = None,
    event_id: str | None = None,
    occurred_at: datetime | None = None,
) -> None:
    """Write one event. Never raises — a logging failure must never break
    the caller's real business action (campaign publish, voucher
    allocation, etc.). Pass a deterministic ``event_id`` for events that
    must be idempotent (see ``deterministic_event_id``); append-only events
    (views/clicks) should omit it."""
    try:
        now = datetime.now(timezone.utc)
        occurred = occurred_at or now
        if isinstance(occurred, datetime) and occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=timezone.utc)

        doc = {
            "event_type": event_type,
            "campaign_id": campaign_id,
            "campaign_type": campaign_type,
            "provider_id": provider_id,
            "telegram_user_id": telegram_user_id,
            "submission_id": submission_id,
            "reward_id": reward_id,
            "pool_id": pool_id,
            "source": source,
            "status": status,
            "reason": reason,
            "metadata": _sanitize_metadata(metadata),
            "occurred_at": occurred,
            "created_at": now,
        }
        # Drop None-valued optional fields to keep documents lean; keep
        # required ones (event_type/status/occurred_at/created_at) always.
        doc = {k: v for k, v in doc.items() if v is not None or k in ("event_type", "status", "occurred_at", "created_at")}

        if event_id:
            doc["event_id"] = event_id
            database.db["campaign_events"].update_one(
                {"event_id": event_id},
                {"$setOnInsert": doc},
                upsert=True,
            )
        else:
            database.db["campaign_events"].insert_one(doc)
    except Exception:
        logger.warning("[CAMPAIGN_EVENTS] emit_failed event_type=%s", event_type, exc_info=True)


def _serialize_event(doc: dict) -> dict:
    out = dict(doc)
    out.pop("_id", None)
    for k in ("occurred_at", "created_at"):
        if isinstance(out.get(k), datetime):
            out[k] = out[k].isoformat()
    out.pop("metadata_raw", None)
    return out


def list_events(
    *,
    campaign_id=None, campaign_type=None, provider_id=None, event_type=None,
    telegram_user_id=None, submission_id=None, reward_id=None, pool_id=None,
    source=None, status=None, date_from=None, date_to=None,
    page=1, page_size=50,
) -> dict:
    query: dict = {}
    for field, value in (
        ("campaign_id", campaign_id), ("campaign_type", campaign_type),
        ("provider_id", provider_id), ("event_type", event_type),
        ("telegram_user_id", telegram_user_id), ("submission_id", submission_id),
        ("reward_id", reward_id), ("pool_id", pool_id),
        ("source", source), ("status", status),
    ):
        if value is not None:
            query[field] = value

    time_range: dict = {}
    if date_from is not None:
        time_range["$gte"] = date_from
    if date_to is not None:
        time_range["$lte"] = date_to
    if time_range:
        query["occurred_at"] = time_range

    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 50), 200))
    skip = (page - 1) * page_size

    col = database.db["campaign_events"]
    total = col.count_documents(query)
    docs = list(col.find(query, sort=[("occurred_at", -1)], skip=skip, limit=page_size))
    return {
        "events": [_serialize_event(d) for d in docs],
        "page": page,
        "page_size": page_size,
        "total": total,
    }


def _rate(numerator: int, denominator: int) -> float:
    if not denominator:
        return 0.0
    return round(numerator / denominator, 4)


def campaign_summary(campaign_id: str, *, date_from=None, date_to=None) -> dict:
    """Bounded, indexed aggregate — always filtered by campaign_id (and
    optionally a date range), never an unbounded full-collection scan."""
    match: dict = {"campaign_id": campaign_id}
    time_range: dict = {}
    if date_from is not None:
        time_range["$gte"] = date_from
    if date_to is not None:
        time_range["$lte"] = date_to
    if time_range:
        match["occurred_at"] = time_range

    col = database.db["campaign_events"]
    counts: dict = {}
    for event_type in (
        "campaign_view", "campaign_click", "subscription_check", "subscription_pass",
        "subscription_fail", "destination_open", "leaderboard_received",
        "voucher_assigned", "reward_viewed", "voucher_copied", "voucher_out_of_stock",
    ):
        counts[event_type] = col.count_documents({**match, "event_type": event_type})

    views = counts["campaign_view"]
    clicks = counts["campaign_click"]
    checks = counts["subscription_check"]
    passes = counts["subscription_pass"]
    opens = counts["destination_open"]

    return {
        "campaign_id": campaign_id,
        "views": views,
        "clicks": clicks,
        "subscription_checks": checks,
        "subscription_passes": passes,
        "subscription_fails": counts["subscription_fail"],
        "destination_opens": opens,
        "leaderboards_received": counts["leaderboard_received"],
        "rewards_assigned": counts["voucher_assigned"],
        "rewards_viewed": counts["reward_viewed"],
        "voucher_copies": counts["voucher_copied"],
        "out_of_stock": counts["voucher_out_of_stock"],
        "click_through_rate": _rate(clicks, views),
        "subscription_pass_rate": _rate(passes, checks),
        "destination_conversion_rate": _rate(opens, views),
    }


# ---------------------------------------------------------------------------
# Admin analytics API
# ---------------------------------------------------------------------------

campaign_events_bp = Blueprint("campaign_events", __name__)


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _parse_admin_date(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@campaign_events_bp.get("/api/admin/campaign-events")
def list_campaign_events():
    _, err = _require_admin()
    if err:
        return err

    args = request.args
    try:
        telegram_user_id = int(args["telegram_user_id"]) if args.get("telegram_user_id") else None
    except (TypeError, ValueError):
        return jsonify({"status": "error", "code": "invalid_telegram_user_id"}), 400

    date_from = _parse_admin_date(args.get("date_from"))
    if args.get("date_from") and date_from is None:
        return jsonify({"status": "error", "code": "invalid_date_from"}), 400
    date_to = _parse_admin_date(args.get("date_to"))
    if args.get("date_to") and date_to is None:
        return jsonify({"status": "error", "code": "invalid_date_to"}), 400

    try:
        page = int(args.get("page", 1) or 1)
        page_size = int(args.get("page_size", 50) or 50)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "code": "invalid_pagination"}), 400

    result = list_events(
        campaign_id=args.get("campaign_id"),
        campaign_type=args.get("campaign_type"),
        provider_id=args.get("provider_id"),
        event_type=args.get("event_type"),
        telegram_user_id=telegram_user_id,
        submission_id=args.get("submission_id"),
        reward_id=args.get("reward_id"),
        pool_id=args.get("pool_id"),
        source=args.get("source"),
        status=args.get("status"),
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
    return jsonify({"status": "ok", **result})


@campaign_events_bp.get("/api/admin/campaign-analytics/summary")
def campaign_analytics_summary():
    _, err = _require_admin()
    if err:
        return err

    campaign_id = request.args.get("campaign_id")
    if not campaign_id:
        return jsonify({"status": "error", "code": "missing_campaign_id"}), 400

    date_from = _parse_admin_date(request.args.get("date_from"))
    date_to = _parse_admin_date(request.args.get("date_to"))

    summary = campaign_summary(campaign_id, date_from=date_from, date_to=date_to)
    return jsonify({"status": "ok", **summary})
