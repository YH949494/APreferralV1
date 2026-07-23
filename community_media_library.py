"""Community Centre Media Library.

Lets admins send media (photo / GIF / video / approved-mime document)
directly to the bot in a private chat instead of hand-copying a Telegram
``file_id``. The bot captures the Telegram media object, stores it in
``community_media_library``, and the Admin Dashboard Composer can then pick
it from a Media Library modal instead of pasting a raw ``file_id``.

Telegram ``file_id`` is only ever guaranteed reusable by the bot that
originally received it, so this module only ever accepts media sent
directly to this bot (see ``extract_media_from_message``/the private-chat
MessageHandler in main.py) — it never accepts a client-supplied ``file_id``
as an alternative to a captured one for library storage. ``file_unique_id``
is stored purely for deduplication; it is never sendable and is never used
as a substitute for ``file_id`` when publishing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, request
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

import database
import community_centre_limits as limits

logger = logging.getLogger(__name__)

MEDIA_TYPES = ("photo", "animation", "video", "document")
MEDIA_STATUSES = ("active", "archived")

# Documents are only accepted for these MIME types (image/video only, per
# spec — arbitrary file uploads are never treated as Community Centre media).
DOCUMENT_ALLOWED_MIME_TYPES = {
    "image/jpeg", "image/png", "image/webp", "image/gif",
    "video/mp4", "video/quicktime",
}

# Reuses the Composer's own size ceilings so the library never accepts
# something the Composer would later reject at publish time.
_SIZE_LIMITS = dict(limits.MEDIA_MAX_SIZE_BYTES)
_SIZE_LIMITS.setdefault("document", 50 * 1024 * 1024)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _media():
    return database.db["community_media_library"]


def _posts():
    return database.db["community_posts"]


def _audit():
    return database.db["community_media_audit"]


def ensure_community_media_library_indexes(db_ref=None) -> None:
    from database import safe_create_index

    db_ref = db_ref if db_ref is not None else database.db
    media = db_ref["community_media_library"]
    safe_create_index(media, [("file_unique_id", 1), ("media_type", 1)], name="cml_media_unique", unique=True)
    safe_create_index(media, [("uploaded_at_utc", -1)], name="cml_media_uploaded_at")
    safe_create_index(media, [("status", 1), ("uploaded_at_utc", -1)], name="cml_media_status_uploaded")
    safe_create_index(media, [("uploaded_by", 1), ("uploaded_at_utc", -1)], name="cml_media_uploader_uploaded")

    audit = db_ref["community_media_audit"]
    safe_create_index(audit, [("media_id", 1), ("created_at_utc", -1)], name="cml_audit_media_created")


def to_object_id(raw) -> ObjectId | None:
    if isinstance(raw, ObjectId):
        return raw
    try:
        return ObjectId(str(raw))
    except (InvalidId, TypeError):
        return None


def record_audit(media_id, action: str, *, actor_id=None, before: dict | None = None, after: dict | None = None) -> None:
    try:
        _audit().insert_one({
            "media_id": media_id,
            "action": action,
            "actor_id": actor_id,
            "before": before,
            "after": after,
            "created_at_utc": now_utc(),
        })
    except Exception:
        logger.exception("[COMMUNITY_MEDIA] audit write failed media_id=%s action=%s", media_id, action)


# ---------------------------------------------------------------------------
# Extraction from a Telegram message (bot-side capture)
# ---------------------------------------------------------------------------

def _max_size_bytes(media_type: str, mime_type: str | None) -> int:
    if media_type == "document" and mime_type and mime_type.startswith("image/"):
        return _SIZE_LIMITS.get("photo", _SIZE_LIMITS["document"])
    return _SIZE_LIMITS.get(media_type, _SIZE_LIMITS["document"])


def extract_media_from_message(message) -> tuple[dict | None, str | None]:
    """Return (extracted_dict, error_code). error_code is one of
    "unsupported_media_type", "unsupported_mime" or "too_large" on failure."""
    extracted: dict[str, Any] | None = None

    if getattr(message, "photo", None):
        largest = message.photo[-1]
        extracted = {
            "file_id": largest.file_id,
            "file_unique_id": largest.file_unique_id,
            "media_type": "photo",
            "filename": None,
            "mime_type": "image/jpeg",
            "file_size": largest.file_size,
            "width": largest.width,
            "height": largest.height,
            "duration": None,
        }
    elif getattr(message, "animation", None):
        a = message.animation
        extracted = {
            "file_id": a.file_id,
            "file_unique_id": a.file_unique_id,
            "media_type": "animation",
            "filename": a.file_name,
            "mime_type": a.mime_type or "video/mp4",
            "file_size": a.file_size,
            "width": a.width,
            "height": a.height,
            "duration": a.duration,
        }
    elif getattr(message, "video", None):
        v = message.video
        extracted = {
            "file_id": v.file_id,
            "file_unique_id": v.file_unique_id,
            "media_type": "video",
            "filename": v.file_name,
            "mime_type": v.mime_type or "video/mp4",
            "file_size": v.file_size,
            "width": v.width,
            "height": v.height,
            "duration": v.duration,
        }
    elif getattr(message, "document", None):
        d = message.document
        mime_type = (d.mime_type or "").lower()
        if mime_type not in DOCUMENT_ALLOWED_MIME_TYPES:
            return None, "unsupported_mime"
        extracted = {
            "file_id": d.file_id,
            "file_unique_id": d.file_unique_id,
            "media_type": "document",
            "filename": d.file_name,
            "mime_type": mime_type,
            "file_size": d.file_size,
            "width": None,
            "height": None,
            "duration": None,
        }
    else:
        return None, "unsupported_media_type"

    max_size = _max_size_bytes(extracted["media_type"], extracted.get("mime_type"))
    if extracted.get("file_size") and extracted["file_size"] > max_size:
        return None, "too_large"

    return extracted, None


# ---------------------------------------------------------------------------
# Save / dedup
# ---------------------------------------------------------------------------

def _default_internal_name(extracted: dict) -> str:
    return extracted.get("filename") or f"{extracted['media_type']}-{extracted['file_unique_id'][:10]}"


def save_media(
    extracted: dict,
    *,
    uploaded_by: int,
    source_chat_id: int,
    source_message_id: int,
    caption: str | None = None,
) -> tuple[dict, bool]:
    """Insert-or-refresh by (file_unique_id, media_type). Returns (doc, created).

    On a repeat upload of the same media: the latest file_id is refreshed,
    the original record (internal_name, tags, uploaded_by/at) is preserved,
    and reupload_count is incremented — never a duplicate library row.
    """
    now = now_utc()
    col = _media()
    filter_q = {"file_unique_id": extracted["file_unique_id"], "media_type": extracted["media_type"]}
    refresh_set = {
        "file_id": extracted["file_id"],
        "file_size": extracted.get("file_size"),
        "width": extracted.get("width"),
        "height": extracted.get("height"),
        "duration": extracted.get("duration"),
        "telegram_source_chat_id": source_chat_id,
        "telegram_source_message_id": source_message_id,
        "last_uploaded_at_utc": now,
    }

    updated = col.find_one_and_update(
        filter_q,
        {"$set": refresh_set, "$inc": {"reupload_count": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if updated:
        return updated, False

    doc = {
        "file_id": extracted["file_id"],
        "file_unique_id": extracted["file_unique_id"],
        "media_type": extracted["media_type"],
        "filename": extracted.get("filename"),
        "mime_type": extracted.get("mime_type"),
        "file_size": extracted.get("file_size"),
        "width": extracted.get("width"),
        "height": extracted.get("height"),
        "duration": extracted.get("duration"),
        "caption": caption,
        "internal_name": _default_internal_name(extracted),
        "tags": [],
        "telegram_source_chat_id": source_chat_id,
        "telegram_source_message_id": source_message_id,
        "uploaded_by": uploaded_by,
        "uploaded_at_utc": now,
        "status": "active",
        "last_used_at_utc": None,
        "usage_count": 0,
        "reupload_count": 0,
    }
    try:
        result = col.insert_one(doc)
        doc["_id"] = result.inserted_id
        return doc, True
    except DuplicateKeyError:
        # Lost a race with a concurrent upload of the same media between the
        # find_one_and_update miss above and this insert.
        updated = col.find_one_and_update(
            filter_q,
            {"$set": refresh_set, "$inc": {"reupload_count": 1}},
            return_document=ReturnDocument.AFTER,
        )
        return updated, False


# ---------------------------------------------------------------------------
# CRUD / lookup
# ---------------------------------------------------------------------------

def get_media(media_id) -> dict | None:
    mid = to_object_id(media_id)
    if mid is None:
        return None
    return _media().find_one({"_id": mid})


def list_media(
    *,
    status: str | None = "active",
    media_type: str | None = None,
    search: str | None = None,
    limit: int = 50,
    skip: int = 0,
) -> tuple[list[dict], int]:
    query: dict[str, Any] = {}
    if status:
        query["status"] = status
    if media_type:
        query["media_type"] = media_type
    if search:
        needle = str(search).strip()
        if needle:
            import re as _re
            pattern = _re.escape(needle)
            regex_cond = {"$regex": pattern, "$options": "i"}
            query["$or"] = [
                {"internal_name": regex_cond},
                {"filename": regex_cond},
                {"caption": regex_cond},
            ]
    total = _media().count_documents(query)
    items = list(_media().find(query, sort=[("uploaded_at_utc", -1)], limit=limit, skip=skip))
    return items, total


def update_media(media_id, patch: dict, *, actor_id=None) -> tuple[dict | None, str | None]:
    mid = to_object_id(media_id)
    if mid is None:
        return None, "bad_id"
    before = _media().find_one({"_id": mid})
    if not before:
        return None, "not_found"

    updates: dict[str, Any] = {}
    if "internal_name" in patch:
        name = str(patch.get("internal_name") or "").strip()
        if not name:
            return None, "missing_internal_name"
        if limits.has_control_chars(name) or len(name) > 200:
            return None, "bad_internal_name"
        updates["internal_name"] = name
    if "tags" in patch:
        tags = patch.get("tags")
        if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags):
            return None, "bad_tags"
        updates["tags"] = [str(t).strip() for t in tags if str(t).strip()][:20]

    if not updates:
        return before, None

    updated = _media().find_one_and_update(
        {"_id": mid}, {"$set": updates}, return_document=ReturnDocument.AFTER
    )
    record_audit(mid, "rename" if "internal_name" in updates else "update", actor_id=actor_id, before=before, after=updated)
    return updated, None


def _set_status(media_id, status: str, *, actor_id=None, action: str) -> tuple[dict | None, str | None]:
    mid = to_object_id(media_id)
    if mid is None:
        return None, "bad_id"
    before = _media().find_one({"_id": mid})
    if not before:
        return None, "not_found"
    updated = _media().find_one_and_update(
        {"_id": mid}, {"$set": {"status": status}}, return_document=ReturnDocument.AFTER
    )
    record_audit(mid, action, actor_id=actor_id, before=before, after=updated)
    return updated, None


def archive_media(media_id, *, actor_id=None) -> tuple[dict | None, str | None]:
    return _set_status(media_id, "archived", actor_id=actor_id, action="archive")


def restore_media(media_id, *, actor_id=None) -> tuple[dict | None, str | None]:
    return _set_status(media_id, "active", actor_id=actor_id, action="restore")


_REFERENCING_STATUSES = ("draft", "pending_approval", "approved", "scheduled", "processing")


def _is_referenced(media_id) -> bool:
    mid = to_object_id(media_id)
    if mid is None:
        return False
    mid_str = str(mid)
    try:
        candidates = _posts().find({"status": {"$in": list(_REFERENCING_STATUSES)}})
        for post in candidates:
            for item in (post.get("media") or []):
                if str(item.get("media_library_id") or "") == mid_str:
                    return True
        return False
    except Exception:
        logger.exception("[COMMUNITY_MEDIA] reference check failed media_id=%s", media_id)
        return True  # fail closed: refuse deletion if we can't be sure


def delete_media(media_id, *, actor_id=None) -> tuple[bool, str | None]:
    mid = to_object_id(media_id)
    if mid is None:
        return False, "bad_id"
    before = _media().find_one({"_id": mid})
    if not before:
        return False, "not_found"
    if _is_referenced(mid):
        return False, "media_in_use"
    _media().delete_one({"_id": mid})
    record_audit(mid, "delete", actor_id=actor_id, before=before)
    return True, None


def resolve_for_publish(media_library_id, *, expected_media_type: str | None = None) -> tuple[dict | None, str | None]:
    """Server-side resolution used by the Composer/publisher. Never trusts a
    client-supplied file_id when a media_library_id is present."""
    mid = to_object_id(media_library_id)
    if mid is None:
        return None, "bad_media_library_id"
    doc = _media().find_one({"_id": mid})
    if not doc:
        return None, "media_not_found"
    if doc.get("status") != "active":
        return None, "media_not_active"
    if expected_media_type and doc.get("media_type") != expected_media_type:
        return None, "media_type_mismatch"
    return doc, None


def increment_usage(media_library_id) -> None:
    mid = to_object_id(media_library_id)
    if mid is None:
        return
    _media().update_one({"_id": mid}, {"$set": {"last_used_at_utc": now_utc()}, "$inc": {"usage_count": 1}})


# ---------------------------------------------------------------------------
# Admin API
# ---------------------------------------------------------------------------

community_media_bp = Blueprint("community_media_library", __name__)


def _require_admin():
    from vouchers import require_admin
    return require_admin()


def _actor_id(payload: dict):
    try:
        return int(payload.get("id") or 0)
    except (TypeError, ValueError):
        return 0


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, ObjectId):
        return str(value)
    return str(value)


def _ok(data: dict, status: int = 200):
    from flask import current_app
    import json as _json
    body = _json.dumps({"success": True, **data}, default=_json_default)
    resp = current_app.response_class(body, mimetype="application/json")
    resp.status_code = status
    return resp


def _err(code: str, status: int = 400):
    from flask import jsonify
    return jsonify({"success": False, "code": code}), status


def _public_media(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "file_id": doc.get("file_id"),
        "file_unique_id": doc.get("file_unique_id"),
        "media_type": doc.get("media_type"),
        "filename": doc.get("filename"),
        "mime_type": doc.get("mime_type"),
        "file_size": doc.get("file_size"),
        "width": doc.get("width"),
        "height": doc.get("height"),
        "duration": doc.get("duration"),
        "caption": doc.get("caption"),
        "internal_name": doc.get("internal_name"),
        "tags": doc.get("tags") or [],
        "uploaded_by": doc.get("uploaded_by"),
        "uploaded_at_utc": doc.get("uploaded_at_utc"),
        "status": doc.get("status"),
        "last_used_at_utc": doc.get("last_used_at_utc"),
        "usage_count": doc.get("usage_count", 0),
        "reupload_count": doc.get("reupload_count", 0),
    }


@community_media_bp.get("/api/admin/community/media")
def cm_list_media():
    payload, err = _require_admin()
    if err:
        return err
    status = request.args.get("status", "active")
    if status == "all":
        status = None
    media_type = request.args.get("media_type") or None
    if media_type and media_type not in MEDIA_TYPES:
        return _err("bad_media_type", 400)
    search = request.args.get("search") or None
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        limit = 50
    try:
        skip = max(int(request.args.get("skip", 0)), 0)
    except (TypeError, ValueError):
        skip = 0

    items, total = list_media(status=status, media_type=media_type, search=search, limit=limit, skip=skip)
    return _ok({"media": [_public_media(m) for m in items], "total": total})


@community_media_bp.get("/api/admin/community/media/<media_id>")
def cm_get_media(media_id):
    payload, err = _require_admin()
    if err:
        return err
    doc = get_media(media_id)
    if not doc:
        return _err("not_found", 404)
    return _ok({"media": _public_media(doc)})


@community_media_bp.patch("/api/admin/community/media/<media_id>")
def cm_patch_media(media_id):
    payload, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    doc, code = update_media(media_id, body, actor_id=_actor_id(payload))
    if code:
        return _err(code, 404 if code == "not_found" else 400)
    logger.info("[COMMUNITY_MEDIA] action=rename media_id=%s actor=%s", media_id, _actor_id(payload))
    return _ok({"media": _public_media(doc)})


@community_media_bp.post("/api/admin/community/media/<media_id>/archive")
def cm_archive_media(media_id):
    payload, err = _require_admin()
    if err:
        return err
    doc, code = archive_media(media_id, actor_id=_actor_id(payload))
    if code:
        return _err(code, 404 if code == "not_found" else 400)
    logger.info("[COMMUNITY_MEDIA] action=archive media_id=%s actor=%s", media_id, _actor_id(payload))
    return _ok({"media": _public_media(doc)})


@community_media_bp.post("/api/admin/community/media/<media_id>/restore")
def cm_restore_media(media_id):
    payload, err = _require_admin()
    if err:
        return err
    doc, code = restore_media(media_id, actor_id=_actor_id(payload))
    if code:
        return _err(code, 404 if code == "not_found" else 400)
    logger.info("[COMMUNITY_MEDIA] action=restore media_id=%s actor=%s", media_id, _actor_id(payload))
    return _ok({"media": _public_media(doc)})


@community_media_bp.delete("/api/admin/community/media/<media_id>")
def cm_delete_media(media_id):
    payload, err = _require_admin()
    if err:
        return err
    ok, code = delete_media(media_id, actor_id=_actor_id(payload))
    if code:
        status = 404 if code == "not_found" else 409 if code == "media_in_use" else 400
        return _err(code, status)
    logger.info("[COMMUNITY_MEDIA] action=delete media_id=%s actor=%s", media_id, _actor_id(payload))
    return _ok({})
