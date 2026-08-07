"""Referral CTA / share engagement tracking.

Measures the Mini App + Creator Centre referral sharing funnel:

    section viewed -> CTA clicked -> link generated -> copied/shared
    -> invitee joined -> referral qualified

Events are written to ``referral_engagement_events``. This module never
creates a second source of truth for joins/qualified referrals -- those are
read (join-only, via ``user_id``) from the existing ``pending_referrals``
collection maintained by ``main.py`` / ``dashboard_panels.py``.

Auth: ``user_id`` is always derived from verified Telegram Mini App
``initData`` (Mini App banner) or the Creator Centre's existing authenticated
creator context (which itself is initData-derived) -- never from a
client-supplied field. Analytics writes are best-effort: a failure is logged
as ``[REFERRAL_ENGAGEMENT][WRITE_FAILED]`` and never blocks the caller's
underlying referral action (link generation, copy, share).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from flask import Blueprint, jsonify, request
from pymongo.errors import DuplicateKeyError, PyMongoError

import database
from config import KL_TZ

logger = logging.getLogger(__name__)

referral_engagement_bp = Blueprint("referral_engagement", __name__)

COLLECTION_NAME = "referral_engagement_events"

ALLOWED_EVENTS = {
    "referral_section_viewed",
    "referral_cta_clicked",
    "referral_link_generated",
    "referral_copy_clicked",
    "referral_share_clicked",
}
ALLOWED_SOURCES = {"miniapp", "creator_centre"}
COPY_OR_SHARE_EVENTS = ["referral_copy_clicked", "referral_share_clicked"]

MAX_SURFACE_LEN = 80
MAX_SESSION_ID_LEN = 128
MAX_REFERRAL_LINK_ID_LEN = 128
MAX_METADATA_BYTES = 2000

# Dedup window (seconds) per event. ``None`` means "dedup for the lifetime of
# the session" (no time bucket component -- e.g. one section-view per
# user/source/surface/session).
DEDUP_WINDOW_SECONDS: dict[str, int | None] = {
    "referral_section_viewed": None,
    "referral_cta_clicked": 3,
    "referral_link_generated": 1,
    "referral_copy_clicked": 2,
    "referral_share_clicked": 2,
}

# Tracking rollout marker -- the dashboard must never treat periods before
# this date as complete zero data. Override via env for real deployments;
# defaults to "now" the first time this module is imported in a process that
# has no override configured, so a fresh deploy never claims to have
# retroactive coverage.
_TRACKING_STARTED_AT_ENV = os.environ.get("REFERRAL_ENGAGEMENT_TRACKING_STARTED_AT_UTC")
if _TRACKING_STARTED_AT_ENV:
    try:
        TRACKING_STARTED_AT = datetime.fromisoformat(_TRACKING_STARTED_AT_ENV.replace("Z", "+00:00"))
        if TRACKING_STARTED_AT.tzinfo is None:
            TRACKING_STARTED_AT = TRACKING_STARTED_AT.replace(tzinfo=timezone.utc)
    except ValueError:
        TRACKING_STARTED_AT = datetime.now(timezone.utc)
else:
    TRACKING_STARTED_AT = datetime.now(timezone.utc)


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db[COLLECTION_NAME]
        database.safe_create_index(col, [("event_id", 1)], name="ux_referral_engagement_event_id", unique=True)
        database.safe_create_index(col, [("occurred_at", 1)], name="ix_referral_engagement_occurred_at")
        database.safe_create_index(
            col, [("day_kl", 1), ("source", 1), ("event", 1)], name="ix_referral_engagement_day_source_event"
        )
        database.safe_create_index(
            col, [("user_id", 1), ("occurred_at", -1)], name="ix_referral_engagement_user_occurred"
        )
    except Exception:
        logger.warning("[REFERRAL_ENGAGEMENT] index creation failed", exc_info=True)


_ensure_indexes()


# ---------------------------------------------------------------------------
# KL calendar helpers
# ---------------------------------------------------------------------------

def day_kl(dt_utc: datetime) -> str:
    return dt_utc.astimezone(KL_TZ).date().isoformat()


def week_key_kl(dt_utc: datetime) -> str:
    local_date = dt_utc.astimezone(KL_TZ).date()
    monday = local_date - timedelta(days=local_date.weekday())
    return monday.isoformat()


def _today_kl() -> date:
    return datetime.now(timezone.utc).astimezone(KL_TZ).date()


# ---------------------------------------------------------------------------
# Dedup / idempotency key
# ---------------------------------------------------------------------------

def _dedup_event_id(
    *,
    user_id: int,
    event: str,
    source: str,
    surface: str,
    session_id: str | None,
    referral_link_id: str | None,
    occurred_at: datetime,
) -> str:
    window = DEDUP_WINDOW_SECONDS.get(event)
    parts = [str(user_id), event, source, surface or ""]
    if event == "referral_section_viewed":
        # Once per user/source/surface/session -- no time bucket.
        parts.append(session_id or "")
    elif event == "referral_link_generated" and referral_link_id:
        # A concrete link id dedups a specific generation regardless of timing.
        parts.append(referral_link_id)
    elif window:
        bucket = int(occurred_at.timestamp() // window)
        parts.append(str(bucket))
    raw = "|".join(parts)
    return "evt_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------

def record_event(
    *,
    event: str,
    user_id: int,
    source: str,
    surface: str,
    session_id: str | None = None,
    referral_link_id: str | None = None,
    metadata: dict | None = None,
) -> tuple[bool, str]:
    """Best-effort write. Never raises. Returns (ok, reason)."""
    now = datetime.now(timezone.utc)
    event_id = _dedup_event_id(
        user_id=user_id,
        event=event,
        source=source,
        surface=surface,
        session_id=session_id,
        referral_link_id=referral_link_id,
        occurred_at=now,
    )
    doc = {
        "event_id": event_id,
        "event": event,
        "user_id": int(user_id),
        "source": source,
        "surface": surface,
        "session_id": session_id,
        "referral_link_id": referral_link_id,
        "occurred_at": now,
        "day_kl": day_kl(now),
        "week_key_kl": week_key_kl(now),
        "metadata": metadata or {},
    }
    try:
        database.db[COLLECTION_NAME].insert_one(doc)
        return True, "recorded"
    except DuplicateKeyError:
        return True, "deduped"
    except (PyMongoError, Exception):  # noqa: BLE001 -- analytics must never raise
        logger.warning(
            "[REFERRAL_ENGAGEMENT][WRITE_FAILED] event=%s user_id=%s source=%s surface=%s",
            event, user_id, source, surface, exc_info=True,
        )
        return False, "write_failed"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _authenticate(source: str) -> tuple[int | None, tuple[str, int] | None]:
    """Returns (user_id, error) where error is (code, http_status) or None.

    user_id always comes from verified Telegram initData -- never from the
    request body/query. Creator Centre uses its own existing authenticated
    creator context (which is itself initData-derived) so behavior stays in
    sync with that module if its auth logic changes.
    """
    if source == "creator_centre":
        from creator_share_centre import _extract_authenticated_user

        user_id, _username, err = _extract_authenticated_user()
        if err:
            return None, err
        return user_id, None

    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data

    init_data = extract_raw_init_data_from_query(request)
    if not init_data:
        return None, ("invalid_telegram_auth", 401)
    ok, parsed, _reason = verify_telegram_init_data(init_data)
    if not ok:
        return None, ("invalid_telegram_auth", 401)

    user_payload = (parsed or {}).get("user", {})
    if isinstance(user_payload, str):
        try:
            user_payload = json.loads(user_payload)
        except (TypeError, ValueError):
            user_payload = {}
    try:
        user_id = int((user_payload or {}).get("id"))
    except (TypeError, ValueError):
        user_id = None
    if not user_id:
        return None, ("invalid_telegram_auth", 401)
    return user_id, None


# ---------------------------------------------------------------------------
# Tracking endpoint
# ---------------------------------------------------------------------------

@referral_engagement_bp.post("/api/referral-engagement/events")
def track_referral_engagement_event():
    body = request.get_json(force=True, silent=True) or {}

    event = body.get("event")
    source = body.get("source")
    surface = body.get("surface")
    session_id = body.get("session_id")
    referral_link_id = body.get("referral_link_id")
    metadata = body.get("metadata")

    if event not in ALLOWED_EVENTS:
        return jsonify({"ok": False, "error": "unknown_event"}), 400
    if source not in ALLOWED_SOURCES:
        return jsonify({"ok": False, "error": "unknown_source"}), 400
    if not isinstance(surface, str) or not surface.strip() or len(surface) > MAX_SURFACE_LEN:
        return jsonify({"ok": False, "error": "invalid_surface"}), 400
    if session_id is not None and (not isinstance(session_id, str) or len(session_id) > MAX_SESSION_ID_LEN):
        return jsonify({"ok": False, "error": "invalid_session_id"}), 400
    if referral_link_id is not None and (
        not isinstance(referral_link_id, str) or len(referral_link_id) > MAX_REFERRAL_LINK_ID_LEN
    ):
        return jsonify({"ok": False, "error": "invalid_referral_link_id"}), 400
    if metadata is None:
        metadata = {}
    elif not isinstance(metadata, dict):
        return jsonify({"ok": False, "error": "invalid_metadata"}), 400
    else:
        try:
            size = len(json.dumps(metadata))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "invalid_metadata"}), 400
        if size > MAX_METADATA_BYTES:
            return jsonify({"ok": False, "error": "metadata_too_large"}), 400

    user_id, auth_err = _authenticate(source)
    if auth_err:
        code, http_status = auth_err
        return jsonify({"ok": False, "error": code}), http_status

    ok, reason = record_event(
        event=event,
        user_id=user_id,
        source=source,
        surface=surface.strip(),
        session_id=session_id,
        referral_link_id=referral_link_id,
        metadata=metadata,
    )
    # Always 200 once authenticated + validated -- a write failure is the
    # server's problem, never the caller's; the underlying referral action
    # (generate/copy/share) must not be gated on this response.
    return jsonify({"ok": True, "recorded": reason if ok else "write_failed"}), 200


# ---------------------------------------------------------------------------
# Admin analytics endpoint
# ---------------------------------------------------------------------------

def _safe_rate(numerator: int, denominator: int) -> float:
    if not denominator:
        return 0
    return round(numerator / denominator, 4)


def _parse_date_param(value: str | None, default: date) -> date:
    if not value:
        return default
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return default


def _distinct_users(col, match: dict, event: str | list[str]) -> set:
    ev_match = {"event": {"$in": event}} if isinstance(event, list) else {"event": event}
    return set(col.distinct("user_id", {**match, **ev_match}))


def _assisted_conversion(engaged_user_ids: set) -> dict:
    from dashboard_panels import _QUALIFIED_STATUSES

    engaged_user_ids = [uid for uid in engaged_user_ids if uid is not None]
    engaged = len(engaged_user_ids)
    if not engaged:
        return {
            "attribution_level": "referrer_level_assisted_conversion",
            "engaged_referrers": 0,
            "engaged_referrers_with_join": 0,
            "engaged_referrers_with_qualified_referral": 0,
            "engagement_to_join_rate": 0,
            "engagement_to_qualified_rate": 0,
        }
    pending_col = database.db["pending_referrals"]
    with_join = len(pending_col.distinct("inviter_user_id", {"inviter_user_id": {"$in": engaged_user_ids}}))
    with_qualified = len(
        pending_col.distinct(
            "inviter_user_id",
            {"inviter_user_id": {"$in": engaged_user_ids}, "status": {"$in": _QUALIFIED_STATUSES}},
        )
    )
    return {
        "attribution_level": "referrer_level_assisted_conversion",
        "engaged_referrers": engaged,
        "engaged_referrers_with_join": with_join,
        "engaged_referrers_with_qualified_referral": with_qualified,
        "engagement_to_join_rate": _safe_rate(with_join, engaged),
        "engagement_to_qualified_rate": _safe_rate(with_qualified, engaged),
    }


def _source_breakdown(col, base_match: dict, src: str) -> dict:
    m = {**base_match, "source": src}
    viewers = _distinct_users(col, m, "referral_section_viewed")
    clickers = _distinct_users(col, m, "referral_cta_clicked")
    generators = _distinct_users(col, m, "referral_link_generated")
    copy_share_users = _distinct_users(col, m, COPY_OR_SHARE_EVENTS)
    generated_count = col.count_documents({**m, "event": "referral_link_generated"})
    assisted = _assisted_conversion(generators)
    return {
        "source": src,
        "viewers": len(viewers),
        "clickers": len(clickers),
        "ctr": _safe_rate(len(clickers), len(viewers)),
        "generated": generated_count,
        "copy_share_users": len(copy_share_users),
        "assisted_joins": assisted["engaged_referrers_with_join"],
        "assisted_qualified_referrals": assisted["engaged_referrers_with_qualified_referral"],
    }


def _daily_series(col, base_match: dict, start: date, end: date) -> list[dict]:
    rows = col.aggregate(
        [
            {"$match": base_match},
            {"$group": {"_id": {"day": "$day_kl", "event": "$event", "user_id": "$user_id"}}},
        ]
    )
    by_day: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in rows:
        _id = r["_id"]
        by_day[_id["day"]][_id["event"]].add(_id["user_id"])

    daily = []
    d = start
    while d <= end:
        ds = d.isoformat()
        evs = by_day.get(ds, {})
        copy_or_share = set()
        for ev in COPY_OR_SHARE_EVENTS:
            copy_or_share |= evs.get(ev, set())
        daily.append(
            {
                "date": ds,
                "section_viewed": len(evs.get("referral_section_viewed", set())),
                "cta_clicked": len(evs.get("referral_cta_clicked", set())),
                "link_generated": len(evs.get("referral_link_generated", set())),
                "copy_or_share": len(copy_or_share),
            }
        )
        d += timedelta(days=1)
    return daily


def _top_users(col, base_match: dict, limit: int = 20) -> list[dict]:
    rows = col.aggregate(
        [
            {"$match": base_match},
            {
                "$group": {
                    "_id": "$user_id",
                    "events": {"$sum": 1},
                    "links_generated": {
                        "$sum": {"$cond": [{"$eq": ["$event", "referral_link_generated"]}, 1, 0]}
                    },
                    "copies": {"$sum": {"$cond": [{"$eq": ["$event", "referral_copy_clicked"]}, 1, 0]}},
                    "shares": {"$sum": {"$cond": [{"$eq": ["$event", "referral_share_clicked"]}, 1, 0]}},
                }
            },
            {"$sort": {"events": -1}},
            {"$limit": limit},
        ]
    )
    return [
        {
            "user_id": r["_id"],
            "events": r.get("events", 0),
            "links_generated": r.get("links_generated", 0),
            "copies": r.get("copies", 0),
            "shares": r.get("shares", 0),
        }
        for r in rows
    ]


@referral_engagement_bp.get("/api/admin/referral-engagement")
def admin_referral_engagement():
    admin, err = _require_admin()
    if err:
        return err

    today = _today_kl()
    default_start = today - timedelta(days=7)
    start = _parse_date_param(request.args.get("start_date"), default_start)
    end = _parse_date_param(request.args.get("end_date"), today)
    if start > end:
        start, end = end, start
    start_str = start.isoformat()
    end_str = end.isoformat()

    source_param = (request.args.get("source") or "all").strip().lower()
    if source_param not in ("all", "miniapp", "creator_centre"):
        source_param = "all"

    col = database.db[COLLECTION_NAME]
    base_match = {"day_kl": {"$gte": start_str, "$lte": end_str}}
    totals_match = dict(base_match)
    if source_param != "all":
        totals_match["source"] = source_param

    has_data = col.count_documents(base_match) > 0 if hasattr(col, "count_documents") else False

    viewers = _distinct_users(col, totals_match, "referral_section_viewed")
    clickers = _distinct_users(col, totals_match, "referral_cta_clicked")
    generators = _distinct_users(col, totals_match, "referral_link_generated")
    copiers = _distinct_users(col, totals_match, "referral_copy_clicked")
    sharers = _distinct_users(col, totals_match, "referral_share_clicked")
    copy_or_share_users = _distinct_users(col, totals_match, COPY_OR_SHARE_EVENTS)

    totals = {
        "unique_section_viewers": len(viewers),
        "unique_cta_clickers": len(clickers),
        "unique_link_generators": len(generators),
        "unique_copiers": len(copiers),
        "unique_sharers": len(sharers),
        "links_generated": col.count_documents({**totals_match, "event": "referral_link_generated"}),
        "copy_actions": col.count_documents({**totals_match, "event": "referral_copy_clicked"}),
        "share_actions": col.count_documents({**totals_match, "event": "referral_share_clicked"}),
        "unique_copy_or_share_users": len(copy_or_share_users),
    }
    rates = {
        "section_to_click": _safe_rate(len(clickers), len(viewers)),
        "click_to_generate": _safe_rate(len(generators), len(clickers)),
        "generate_to_copy_or_share": _safe_rate(len(copy_or_share_users), len(generators)),
        "section_to_copy_or_share": _safe_rate(len(copy_or_share_users), len(viewers)),
    }

    assisted_conversion = _assisted_conversion(generators)
    by_source = [_source_breakdown(col, base_match, src) for src in ("miniapp", "creator_centre")]
    daily = _daily_series(col, totals_match, start, end)
    top_users = _top_users(col, totals_match)

    return jsonify(
        {
            "period": {
                "start_date": start_str,
                "end_date": end_str,
                "tracking_started_at": TRACKING_STARTED_AT.isoformat(),
            },
            "source": source_param,
            "has_data": has_data,
            "totals": totals,
            "rates": rates,
            "assisted_conversion": assisted_conversion,
            "by_source": by_source,
            "daily": daily,
            "top_users": top_users,
        }
    )
