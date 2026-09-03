"""Event Banner — a single, reusable, dynamic image-only promotional banner
rendered above the Campaign Rewards section in the Telegram Mini App.

Collection: ``event_banners``. Deliberately a separate, lightweight
collection rather than being folded into Campaign Centre's ``gc_campaigns``
(campaign_centre.py) — that engine models reward-driven campaigns (reward
rules, verification providers, subscription gates) that an image-only
banner has no use for. It reuses the same conventions as the rest of the
app instead of inventing new ones: admin auth via ``vouchers.require_admin``,
Telegram identity via ``miniapp_identity.resolve_authenticated_telegram_user_id``,
region via the existing ``users.region`` field, and analytics via the
existing ``campaign_events.emit_campaign_event`` ledger.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from flask import Blueprint, jsonify, request

import database
from miniapp_identity import resolve_authenticated_telegram_user_id

logger = logging.getLogger(__name__)

event_banner_admin_bp = Blueprint("event_banner_admin", __name__)
event_banner_public_bp = Blueprint("event_banner_public", __name__)

STATUSES = ("active", "inactive")

DEFAULT_ALT_TEXT = "Current event promotion"


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db["event_banners"]
        col.create_index([("event_id", 1)], name="ux_event_banners_event_id", unique=True)
        col.create_index([("status", 1), ("priority", -1)], name="ix_event_banners_status_priority")
        col.create_index([("starts_at", 1), ("ends_at", 1)], name="ix_event_banners_window")
    except Exception:
        logger.warning("[EVENT_BANNER] index_creation_failed", exc_info=True)


_ensure_indexes()


def _validate_url(url: str, *, allow_tg: bool = True) -> bool:
    """https:// URLs are always accepted; tg:// deep links are accepted
    only when ``allow_tg`` is set (destinations, not images — a browser
    <img> cannot load a tg:// deep link). Never javascript:/data:/other
    executable schemes."""
    if not isinstance(url, str) or not url.strip():
        return False
    url = url.strip()
    lowered = url.lower()
    if lowered.startswith("javascript:") or lowered.startswith("data:"):
        return False
    if any(ch in url for ch in ("\r", "\n", "\t")):
        return False
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return bool(parsed.netloc)
    if allow_tg and parsed.scheme == "tg":
        return True
    return False


def _validate_image_url(url: str) -> bool:
    return _validate_url(url, allow_tg=False)


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _validate_body(body: dict, *, partial: bool = False, existing: dict | None = None) -> tuple[dict | None, str | None]:
    """Validates and normalizes an admin-supplied event banner payload.
    ``existing`` is the full doc being updated (used to exclude itself from
    the event_id-uniqueness check, and to fill in a window boundary omitted
    from a partial update); leave None when creating."""
    updates: dict = {}
    existing_id = (existing or {}).get("_id")

    if not partial or "event_id" in body:
        event_id = (body.get("event_id") or "").strip()
        if not event_id:
            return None, "missing_event_id"
        clash = database.db["event_banners"].find_one({"event_id": event_id})
        if clash and clash.get("_id") != existing_id:
            return None, "event_id_not_unique"
        updates["event_id"] = event_id

    if not partial or "image_url" in body:
        image_url = (body.get("image_url") or "").strip()
        if not _validate_image_url(image_url):
            return None, "invalid_image_url"
        updates["image_url"] = image_url

    if not partial or "destination_url" in body:
        destination_url = (body.get("destination_url") or "").strip()
        if not _validate_url(destination_url):
            return None, "invalid_destination_url"
        updates["destination_url"] = destination_url

    if not partial or "alt_text" in body:
        updates["alt_text"] = (body.get("alt_text") or "").strip() or DEFAULT_ALT_TEXT

    if not partial or "starts_at" in body or "ends_at" in body:
        starts_at = _parse_dt(body.get("starts_at")) if "starts_at" in body else _parse_dt((existing or {}).get("starts_at"))
        ends_at = _parse_dt(body.get("ends_at")) if "ends_at" in body else _parse_dt((existing or {}).get("ends_at"))
        if not starts_at:
            return None, "missing_starts_at"
        if not ends_at:
            return None, "missing_ends_at"
        if ends_at <= starts_at:
            return None, "ends_at_before_starts_at"
        updates["starts_at"] = starts_at
        updates["ends_at"] = ends_at

    if not partial or "status" in body:
        status = (body.get("status") or "inactive").strip()
        if status not in STATUSES:
            return None, "invalid_status"
        updates["status"] = status

    if not partial or "priority" in body:
        try:
            updates["priority"] = int(body.get("priority", 0))
        except (TypeError, ValueError):
            return None, "invalid_priority"

    if not partial or "regions" in body:
        regions = body.get("regions") or []
        if not isinstance(regions, list) or not all(isinstance(r, str) for r in regions):
            return None, "invalid_regions"
        updates["regions"] = [r.strip() for r in regions if r.strip()]

    return updates, None


def _effective_status(doc: dict, now: datetime) -> str:
    """Status as an admin should read it, distinct from the stored
    ``status`` field: an ``active`` banner is further split into
    scheduled/live/expired by its window so an expired banner never
    displays as plain "active"."""
    if doc.get("status") != "active":
        return "inactive"
    starts_at = doc.get("starts_at")
    ends_at = doc.get("ends_at")
    if not starts_at or not ends_at:
        return "inactive"
    if starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=timezone.utc)
    if ends_at.tzinfo is None:
        ends_at = ends_at.replace(tzinfo=timezone.utc)
    if now < starts_at:
        return "scheduled"
    if now >= ends_at:
        return "expired"
    return "live"


def _serialize(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    out["effective_status"] = _effective_status(doc, datetime.now(timezone.utc))
    for k in ("starts_at", "ends_at", "created_at", "updated_at"):
        v = out.get(k)
        if v:
            # database.py's MongoClient isn't tz_aware, so a value just read
            # back from Mongo comes back naive (UTC values, no tzinfo) — an
            # offset-less isoformat() string would then parse as local time
            # in the browser instead of UTC. Every timestamp in this
            # collection is UTC by convention, so a naive one always means
            # UTC.
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            out[k] = v.isoformat()
    return out


def _log_audit(action: str, admin: dict, event_id: str, details: dict | None = None) -> None:
    try:
        database.db["campaign_admin_audit_log"].insert_one({
            "action": action,
            "entity": "event_banner",
            "entity_id": event_id,
            "admin": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
            "details": details or {},
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[EVENT_BANNER] audit_write_failed", exc_info=True)


# ---------------------------------------------------------------------------
# Admin CRUD
# ---------------------------------------------------------------------------


@event_banner_admin_bp.get("/api/admin/event-banners")
def list_event_banners():
    _, err = _require_admin()
    if err:
        return err
    docs = list(database.db["event_banners"].find({}, sort=[("priority", -1), ("created_at", -1)], limit=200))
    return jsonify({"status": "ok", "banners": [_serialize(d) for d in docs]})


@event_banner_admin_bp.post("/api/admin/event-banners")
def create_event_banner():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    updates, code = _validate_body(body, partial=False)
    if code:
        logger.warning("[EVENT_BANNER][INVALID_CONFIG] reason=%s event_id=%s", code, body.get("event_id"))
        return jsonify({"status": "error", "code": code}), 400

    now = datetime.now(timezone.utc)
    doc = {
        **updates,
        "created_at": now,
        "updated_at": now,
        "created_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
    }
    try:
        result = database.db["event_banners"].insert_one(doc)
    except Exception as exc:
        if "duplicate" in str(exc).lower():
            return jsonify({"status": "error", "code": "event_id_not_unique"}), 409
        logger.exception("[EVENT_BANNER] create_failed")
        return jsonify({"status": "error", "code": "internal_error"}), 500

    doc["_id"] = result.inserted_id
    return jsonify({"status": "ok", "banner": _serialize(doc)}), 201


@event_banner_admin_bp.get("/api/admin/event-banners/<event_id>")
def get_event_banner(event_id: str):
    _, err = _require_admin()
    if err:
        return err
    doc = database.db["event_banners"].find_one({"event_id": event_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok", "banner": _serialize(doc)})


@event_banner_admin_bp.patch("/api/admin/event-banners/<event_id>")
def update_event_banner(event_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = database.db["event_banners"].find_one({"event_id": event_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(silent=True) or {}
    updates, code = _validate_body(body, partial=True, existing=doc)
    if code:
        logger.warning("[EVENT_BANNER][INVALID_CONFIG] reason=%s event_id=%s", code, event_id)
        return jsonify({"status": "error", "code": code}), 400

    updates["updated_at"] = datetime.now(timezone.utc)
    updates["updated_by"] = (admin or {}).get("usernameLower") or str((admin or {}).get("id", ""))
    database.db["event_banners"].update_one({"_id": doc["_id"]}, {"$set": updates})
    doc = database.db["event_banners"].find_one({"_id": doc["_id"]})
    return jsonify({"status": "ok", "banner": _serialize(doc)})


@event_banner_admin_bp.patch("/api/admin/event-banners/<event_id>/schedule")
def edit_event_banner_schedule(event_id: str):
    """Dedicated schedule-only edit: start/end/priority, nothing else —
    never touches event_id, image_url, destination_url, alt_text, status or
    regions. Requires ``confirm: true`` in the body when the banner is
    currently "live" (active status, within its window right now), since
    its audience is actively depending on its window."""
    admin, err = _require_admin()
    if err:
        return err
    doc = database.db["event_banners"].find_one({"event_id": event_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(silent=True) or {}

    starts_at = _parse_dt(body.get("starts_at")) if "starts_at" in body else _parse_dt(doc.get("starts_at"))
    ends_at = _parse_dt(body.get("ends_at")) if "ends_at" in body else _parse_dt(doc.get("ends_at"))
    if not starts_at:
        return jsonify({"status": "error", "code": "missing_starts_at"}), 400
    if not ends_at:
        return jsonify({"status": "error", "code": "missing_ends_at"}), 400
    if ends_at <= starts_at:
        return jsonify({"status": "error", "code": "ends_at_before_starts_at"}), 400

    if "priority" in body:
        try:
            priority = int(body.get("priority"))
        except (TypeError, ValueError):
            return jsonify({"status": "error", "code": "invalid_priority"}), 400
    else:
        priority = doc.get("priority", 0)

    if _effective_status(doc, datetime.now(timezone.utc)) == "live" and body.get("confirm") is not True:
        return jsonify({"status": "error", "code": "confirmation_required"}), 409

    previous_schedule = {
        "starts_at": doc["starts_at"].isoformat() if doc.get("starts_at") else None,
        "ends_at": doc["ends_at"].isoformat() if doc.get("ends_at") else None,
        "priority": doc.get("priority", 0),
    }
    updates = {
        "starts_at": starts_at,
        "ends_at": ends_at,
        "priority": priority,
        "updated_at": datetime.now(timezone.utc),
        "updated_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
    }
    database.db["event_banners"].update_one({"_id": doc["_id"]}, {"$set": updates})
    new_schedule = {
        "starts_at": starts_at.isoformat(),
        "ends_at": ends_at.isoformat(),
        "priority": priority,
    }
    _log_audit(
        "edit_schedule",
        admin,
        event_id,
        {"previous_schedule": previous_schedule, "new_schedule": new_schedule},
    )

    doc = database.db["event_banners"].find_one({"_id": doc["_id"]})
    return jsonify({"status": "ok", "banner": _serialize(doc)})


@event_banner_admin_bp.delete("/api/admin/event-banners/<event_id>")
def delete_event_banner(event_id: str):
    _, err = _require_admin()
    if err:
        return err
    database.db["event_banners"].delete_one({"event_id": event_id})
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Public resolution + analytics
# ---------------------------------------------------------------------------


def _window_active(doc: dict, now: datetime) -> bool:
    starts_at = doc.get("starts_at")
    ends_at = doc.get("ends_at")
    if not starts_at or not ends_at:
        return False
    if starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=timezone.utc)
    if ends_at.tzinfo is None:
        ends_at = ends_at.replace(tzinfo=timezone.utc)
    return starts_at <= now < ends_at


def _region_eligible(doc: dict, region: str | None) -> bool:
    regions = doc.get("regions") or []
    if not regions:
        return True
    return bool(region) and region in regions


def pick_eligible_banner(candidates: list, *, now: datetime, region: str | None) -> dict | None:
    """Candidates must already be sorted by priority desc; returns the
    first (highest-priority) doc that passes every visibility rule, or
    None. Never raises on a single malformed doc — it just skips it."""
    for doc in candidates:
        try:
            if doc.get("status") != "active":
                continue
            if not _window_active(doc, now):
                continue
            if not _region_eligible(doc, region):
                continue
            if not _validate_image_url(doc.get("image_url", "")) or not _validate_url(doc.get("destination_url", "")):
                logger.warning("[EVENT_BANNER][INVALID_CONFIG] event_id=%s", doc.get("event_id"))
                continue
            return doc
        except Exception:
            logger.warning("[EVENT_BANNER][INVALID_CONFIG] event_id=%s", doc.get("event_id"), exc_info=True)
            continue
    return None


@event_banner_public_bp.get("/api/event-banner")
def get_public_event_banner():
    uid = None
    try:
        resolved_uid, auth_err = resolve_authenticated_telegram_user_id()
        if not auth_err:
            uid = resolved_uid
    except Exception:
        uid = None

    region = None
    if uid is not None:
        try:
            user = database.db["users"].find_one({"user_id": uid})
            region = (user or {}).get("region")
        except Exception:
            region = None

    logger.info("[EVENT_BANNER][RESOLVE] user_id=%s region=%s", uid, region)

    resp_payload = {"status": "ok", "banner": None}
    try:
        now = datetime.now(timezone.utc)
        # Filter status/window/region in the query itself rather than after
        # a truncated fetch — otherwise a page of higher-priority but
        # expired/scheduled/other-region banners could push a genuinely
        # eligible lower-priority banner past the fetch limit.
        region_clause = [{"regions": []}, {"regions": {"$exists": False}}]
        if region:
            region_clause.append({"regions": region})
        query = {
            "status": "active",
            "starts_at": {"$lte": now},
            "ends_at": {"$gt": now},
            "$or": region_clause,
        }
        candidates = list(
            database.db["event_banners"].find(query, sort=[("priority", -1)], limit=200)
        )
        banner = pick_eligible_banner(candidates, now=now, region=region)
        if banner:
            logger.info("[EVENT_BANNER][SHOW] event_id=%s user_id=%s", banner.get("event_id"), uid)
            resp_payload["banner"] = {
                "event_id": banner.get("event_id"),
                "image_url": banner.get("image_url"),
                "destination_url": banner.get("destination_url"),
                "alt_text": banner.get("alt_text") or DEFAULT_ALT_TEXT,
            }
        else:
            logger.info("[EVENT_BANNER][NONE] user_id=%s region=%s", uid, region)
    except Exception:
        logger.warning("[EVENT_BANNER][API_ERROR] user_id=%s", uid, exc_info=True)
        resp_payload = {"status": "ok", "banner": None}

    resp = jsonify(resp_payload)
    resp.headers["Cache-Control"] = "no-store"
    return resp


_TRACK_EVENT_TYPES = {
    "impression": "event_banner_impression",
    "click": "event_banner_click",
    "image_error": "event_banner_image_error",
}


@event_banner_public_bp.post("/api/event-banner/track")
def track_event_banner():
    """Best-effort analytics write — never blocks or fails the caller.
    Always returns 200 so a frontend `sendBeacon`/`fetch` never has to
    branch on the response before continuing navigation. An unverified
    caller (missing/invalid Telegram initData) never reaches the ledger —
    without that, anyone could POST arbitrary event_ids and inflate
    impression/click counts indefinitely."""
    uid = None
    authenticated = False
    try:
        resolved_uid, auth_err = resolve_authenticated_telegram_user_id()
        if not auth_err:
            uid = resolved_uid
            authenticated = True
    except Exception:
        authenticated = False

    body = request.get_json(silent=True) or {}
    event_id = (body.get("event_id") or "").strip()
    kind = (body.get("type") or "").strip()
    event_type = _TRACK_EVENT_TYPES.get(kind)

    if authenticated and event_id and event_type:
        try:
            from campaign_events import emit_campaign_event

            emit_campaign_event(
                event_type=event_type,
                campaign_id=event_id,
                telegram_user_id=uid,
                source="miniapp_top",
                metadata={
                    "region": body.get("region"),
                    "placement": "miniapp_top",
                    "ui_version": body.get("ui_version"),
                },
            )
        except Exception:
            logger.warning("[EVENT_BANNER] analytics_write_failed", exc_info=True)

    return jsonify({"status": "ok"})
