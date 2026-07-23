"""Community Centre: Telegram content composer, poll manager and scheduler.

Implements the Admin Dashboard -> Community Centre feature: drafting,
previewing, approving, scheduling and publishing Telegram content (text,
image, GIF/animation, video, media albums, regular polls and quizzes) with
inline buttons, plus poll stop/results and a restart-safe MongoDB-backed
worker.

Delivery scope (see PR description for the full phase breakdown):
  Phase 1 (this change, required): text/photo/animation/video/poll/quiz,
    inline buttons, draft -> preview -> publish-now/one-time-schedule,
    restart-safe worker, published history, failed view, manual stop-poll,
    poll-answer analytics, pin/unpin.
  Phase 2 (this change, partial): media albums, daily/weekly recurrence,
    approval workflow, campaign tags, basic calendar.
  Phase 3 (explicitly NOT implemented): rich-media polls, media inside poll
    options/quiz explanations. python-telegram-bot==20.8 predates that Bot
    API surface; bolting it on via raw HTTP calls scattered through the
    codebase is exactly what the spec forbids, so it is deferred to a
    controlled library-upgrade change.

Media storage note: this codebase has no object-storage (S3/GCS/etc)
abstraction to build on (verified: no boto3/storage layer anywhere). Rather
than invent a new binary-storage subsystem as a side effect of this feature,
media is referenced by HTTPS URL or a previously-obtained Telegram
``file_id`` — both of which python-telegram-bot's ``send_photo`` /
``send_animation`` / ``send_video`` accept natively without any local
upload. This is called out explicitly in the final implementation report.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytz
from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

import database
import community_centre_limits as limits
import community_media_library
import settings_service

logger = logging.getLogger(__name__)

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")
INSTANCE_ID = os.getenv("FLY_ALLOC_ID") or os.getenv("HOSTNAME") or f"pid-{os.getpid()}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------

def _posts():
    return database.db["community_posts"]


def _runs():
    return database.db["community_post_runs"]


def _poll_answers():
    return database.db["community_poll_answers"]


def _poll_snapshots():
    return database.db["community_poll_snapshots"]


def _audit():
    return database.db["community_post_audit"]


def _destinations():
    return database.db["community_destinations"]


def ensure_community_centre_indexes(db_ref=None) -> None:
    """Create every Community Centre index independently — one failure must
    never prevent the rest from being created (safe_create_index already
    isolates each call in its own try/except)."""
    from database import safe_create_index

    db_ref = db_ref if db_ref is not None else database.db

    posts = db_ref["community_posts"]
    safe_create_index(posts, [("status", 1), ("next_run_at_utc", 1)], name="cc_posts_status_next_run")
    safe_create_index(posts, [("processing_started_at_utc", 1)], name="cc_posts_processing_started")
    safe_create_index(posts, [("created_at", -1)], name="cc_posts_created_at")
    safe_create_index(posts, [("published_at", -1)], name="cc_posts_published_at")
    safe_create_index(posts, [("content_type", 1), ("published_at", -1)], name="cc_posts_content_type_published")
    safe_create_index(posts, [("destination_key", 1), ("scheduled_at_utc", 1)], name="cc_posts_destination_scheduled")
    safe_create_index(posts, [("poll_status", 1), ("published_at", -1)], name="cc_posts_poll_status_published")

    runs = db_ref["community_post_runs"]
    safe_create_index(runs, [("community_post_id", 1), ("run_key", 1)], name="cc_runs_post_runkey", unique=True)
    safe_create_index(runs, [("status", 1), ("scheduled_for_utc", 1)], name="cc_runs_status_scheduled")
    safe_create_index(runs, [("processing_started_at_utc", 1)], name="cc_runs_processing_started")

    answers = db_ref["community_poll_answers"]
    safe_create_index(answers, [("poll_id", 1), ("user_id", 1)], name="cc_answers_poll_user", unique=True)
    safe_create_index(answers, [("community_post_id", 1), ("updated_at_utc", -1)], name="cc_answers_post_updated")

    snapshots = db_ref["community_poll_snapshots"]
    safe_create_index(snapshots, [("poll_id", 1)], name="cc_snapshots_poll", unique=True)
    safe_create_index(snapshots, [("community_post_id", 1)], name="cc_snapshots_post")
    safe_create_index(snapshots, [("updated_at_utc", -1)], name="cc_snapshots_updated")

    audit = db_ref["community_post_audit"]
    safe_create_index(audit, [("community_post_id", 1), ("created_at_utc", -1)], name="cc_audit_post_created")
    safe_create_index(audit, [("actor_id", 1), ("created_at_utc", -1)], name="cc_audit_actor_created")

    dest = db_ref["community_destinations"]
    safe_create_index(dest, [("key", 1)], name="cc_destinations_key", unique=True)


# ---------------------------------------------------------------------------
# Audit ledger
# ---------------------------------------------------------------------------

def record_audit(
    post_id,
    action: str,
    *,
    actor_type: str = "admin",
    actor_id=None,
    before: dict | None = None,
    after: dict | None = None,
    run_key: str | None = None,
) -> None:
    try:
        _audit().insert_one({
            "community_post_id": post_id,
            "action": action,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "before": before,
            "after": after,
            "run_key": run_key,
            "created_at_utc": now_utc(),
        })
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] audit write failed post_id=%s action=%s", post_id, action)


# ---------------------------------------------------------------------------
# Destinations registry (Admin Settings -> approved posting destinations)
#
# Built-in destinations are derived, at read time, from the "Telegram
# Configuration" settings group (settings_service already resolves each
# field as Mongo override -> env var -> hardcoded default, so this stays
# backward-compatible with untouched deployments). Additional destinations
# created through the community_destinations collection are merged on top,
# so admins never have to re-enter the Official Channel / Main Group /
# #mywin IDs a second time in a separate section.
# ---------------------------------------------------------------------------

_DEST_OUTPUT_FIELDS = ("key", "name", "chat_id", "chat_type", "enabled", "allow_posts", "allow_polls", "allow_pin")


def _normalize_destination(dest: dict) -> dict:
    out = {field: dest.get(field) for field in _DEST_OUTPUT_FIELDS}
    if dest.get("username"):
        out["username"] = dest["username"]
    return out


def _valid_chat_id(raw: Any) -> int | None:
    """Only a valid, non-zero, negative Telegram chat id counts (Telegram
    groups/channels always use negative ids; 0/unset means unconfigured)."""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value < 0 else None


def resolve_community_destinations(settings_doc: dict) -> list[dict]:
    """Return the built-in Community Centre destinations derived from the
    Telegram Configuration settings document (as returned by
    ``settings_service.get_settings("telegram_config")``)."""
    out: list[dict] = []

    official_chat_id = _valid_chat_id(settings_doc.get("official_channel_id"))
    if official_chat_id is not None:
        out.append({
            "key": "official_channel",
            "name": "Official Channel",
            "chat_id": official_chat_id,
            "username": settings_doc.get("official_channel_username") or None,
            "chat_type": "channel",
            "enabled": True,
            "allow_posts": True,
            "allow_polls": True,
            "allow_pin": True,
        })

    main_group_chat_id = _valid_chat_id(settings_doc.get("main_group_id"))
    if main_group_chat_id is not None:
        out.append({
            "key": "main_group",
            "name": "Community Group",
            "chat_id": main_group_chat_id,
            "chat_type": "supergroup",
            "enabled": True,
            "allow_posts": True,
            "allow_polls": True,
            "allow_pin": True,
        })

    mywin_chat_id = _valid_chat_id(settings_doc.get("community_chat_id"))
    if mywin_chat_id is not None:
        out.append({
            "key": "mywin_group",
            "name": "#mywin Group",
            "chat_id": mywin_chat_id,
            "chat_type": "supergroup",
            "enabled": True,
            "allow_posts": True,
            "allow_polls": True,
            "allow_pin": True,
        })

    return out


def _merge_destinations(builtins: list[dict], custom: list[dict]) -> list[dict]:
    """Merge built-in destinations with admin-created custom ones,
    deduplicating by chat_id first, then by key. Built-in entries always win
    a collision since they mirror the canonical Telegram Configuration."""
    merged: list[dict] = []
    seen_chat_ids: set = set()
    seen_keys: set = set()
    for dest in builtins:
        merged.append(dest)
        seen_chat_ids.add(dest["chat_id"])
        seen_keys.add(dest["key"])
    for dest in custom:
        chat_id = dest.get("chat_id")
        key = dest.get("key")
        if chat_id in seen_chat_ids or key in seen_keys:
            continue
        merged.append(dest)
        seen_chat_ids.add(chat_id)
        seen_keys.add(key)
    return merged


def list_destinations(*, enabled_only: bool = False) -> list[dict]:
    # force_refresh: this deployment runs multiple workers (fly.toml), and
    # settings_service's in-process cache is only invalidated on the worker
    # that handled the Telegram Configuration save. Without this, a
    # different worker could keep serving a stale chat id off its own
    # 45s-old cache for a post-save Composer load. Destination lookups are
    # low-frequency (admin dashboard only), so paying for a fresh read here
    # is worth the correctness guarantee.
    builtins = resolve_community_destinations(settings_service.get_settings("telegram_config", force_refresh=True))
    custom = list(_destinations().find({}, sort=[("name", 1)]))
    merged = [_normalize_destination(d) for d in _merge_destinations(builtins, custom)]
    if enabled_only:
        merged = [d for d in merged if d.get("enabled")]
    return merged


def get_destination(key: str) -> dict | None:
    for dest in list_destinations():
        if dest.get("key") == key:
            return dest
    return None


def upsert_destination(payload: dict, *, actor_id=None) -> tuple[dict | None, str | None]:
    key = str(payload.get("key") or "").strip()
    name = str(payload.get("name") or "").strip()
    chat_id = payload.get("chat_id")
    chat_type = payload.get("chat_type")
    if not key or not re.match(r"^[a-z0-9_]{2,60}$", key):
        return None, "bad_key"
    if not name:
        return None, "missing_name"
    try:
        chat_id = int(chat_id)
    except (TypeError, ValueError):
        return None, "bad_chat_id"
    if chat_type not in ("channel", "group", "supergroup"):
        return None, "bad_chat_type"
    doc = {
        "key": key,
        "name": name,
        "chat_id": chat_id,
        "chat_type": chat_type,
        "enabled": bool(payload.get("enabled", True)),
        "allow_posts": bool(payload.get("allow_posts", True)),
        "allow_polls": bool(payload.get("allow_polls", True)),
        "allow_pin": bool(payload.get("allow_pin", False)),
        "updated_at": now_utc(),
        "updated_by": actor_id,
    }
    _destinations().update_one({"key": key}, {"$set": doc, "$setOnInsert": {"created_at": now_utc()}}, upsert=True)
    return _destinations().find_one({"key": key}), None


def delete_destination(key: str) -> bool:
    res = _destinations().delete_one({"key": key})
    return getattr(res, "deleted_count", 0) > 0


# ---------------------------------------------------------------------------
# Validation orchestration
# ---------------------------------------------------------------------------

def _clean_buttons(raw_buttons) -> tuple[list[dict], str | None]:
    if raw_buttons in (None, []):
        return [], None
    err = limits.validate_buttons(raw_buttons)
    if err:
        return [], err
    cleaned = []
    for b in raw_buttons:
        cleaned.append({
            "row": int(b["row"]),
            "position": int(b.get("position", 0)),
            "text": str(b["text"]).strip(),
            "type": b["type"],
            "value": b["value"],
        })
    cleaned.sort(key=lambda b: (b["row"], b["position"]))
    return cleaned, None


def _clean_media(raw_media, content_type: str) -> tuple[list[dict], str | None]:
    if content_type not in ("photo", "animation", "video", "media_group"):
        return [], None
    if not isinstance(raw_media, list) or not raw_media:
        return [], "missing_media"
    if content_type == "media_group":
        if len(raw_media) < limits.MEDIA_GROUP_MIN_ITEMS or len(raw_media) > limits.MEDIA_GROUP_MAX_ITEMS:
            return [], "bad_media_group_size"
        allowed_types = {"photo", "video"}
    else:
        if len(raw_media) != 1:
            return [], "single_media_required"
        allowed_types = {content_type}
    cleaned = []
    for idx, item in enumerate(raw_media):
        if not isinstance(item, dict):
            return [], "bad_media_item"
        mtype = item.get("type")
        if mtype not in allowed_types:
            return [], "bad_media_type"

        media_library_id = str(item.get("media_library_id") or "").strip() or None
        source_url = (item.get("source_url") or "").strip()
        file_id = (item.get("telegram_file_id") or "").strip() or None

        if media_library_id:
            # Priority 1: media_library_id. A client-supplied file_id/URL
            # riding along on the same item is display-only and is never
            # trusted — the sendable file_id is always resolved server-side.
            lib_doc, lib_err = community_media_library.resolve_for_publish(
                media_library_id, expected_media_type=mtype
            )
            if lib_err:
                return [], lib_err
            cleaned.append({
                "type": mtype,
                "storage_key": None,
                "telegram_file_id": lib_doc["file_id"],
                "media_library_id": str(lib_doc["_id"]),
                "filename": lib_doc.get("filename"),
                "mime_type": lib_doc.get("mime_type"),
                "size_bytes": lib_doc.get("file_size"),
                "position": idx,
            })
            continue

        if source_url and file_id:
            return [], "ambiguous_media_source"
        if not source_url and not file_id:
            return [], "missing_media_source"
        if source_url:
            if limits.has_control_chars(source_url):
                return [], "control_characters"
            if not source_url.lower().startswith("https://"):
                return [], "https_required"
        cleaned.append({
            "type": mtype,
            "storage_key": source_url or None,
            "telegram_file_id": file_id,
            "media_library_id": None,
            "filename": item.get("filename"),
            "mime_type": item.get("mime_type"),
            "size_bytes": item.get("size_bytes"),
            "position": idx,
        })
    return cleaned, None


def _clean_poll(raw_poll: dict | None, content_type: str, schedule_type: str) -> tuple[dict | None, str | None]:
    if content_type not in ("poll", "quiz"):
        return None, None
    if not isinstance(raw_poll, dict):
        return None, "missing_poll"

    question = str(raw_poll.get("question") or "").strip()
    err = limits.validate_text_len(question, limits.POLL_QUESTION_MAX_LEN)
    if err or not question:
        return None, err or "missing_question"

    raw_options = raw_poll.get("options") or []
    option_texts = [str(o.get("text") if isinstance(o, dict) else o).strip() for o in raw_options]
    err = limits.validate_poll_options(option_texts)
    if err:
        return None, err

    poll_type = "quiz" if content_type == "quiz" else "regular"
    is_anonymous = bool(raw_poll.get("is_anonymous", True))
    allows_multiple = bool(raw_poll.get("allows_multiple_answers", False))
    members_only = bool(raw_poll.get("members_only", False))

    correct_option_id = None
    explanation = None
    explanation_parse_mode = raw_poll.get("explanation_parse_mode") or "HTML"
    if poll_type == "quiz":
        if allows_multiple:
            return None, "quiz_cannot_allow_multiple"
        coi = raw_poll.get("correct_option_id")
        try:
            correct_option_id = int(coi)
        except (TypeError, ValueError):
            return None, "missing_correct_option"
        if correct_option_id < 0 or correct_option_id >= len(option_texts):
            return None, "invalid_correct_option"
        raw_explanation = raw_poll.get("explanation")
        if raw_explanation:
            err = limits.validate_text_len(str(raw_explanation), limits.QUIZ_EXPLANATION_MAX_LEN)
            if err:
                return None, err
            explanation, herr = limits.sanitize_telegram_html(str(raw_explanation))
            if herr:
                return None, herr
        allows_multiple = False

    close_mode = raw_poll.get("close_mode") or "manual"
    if close_mode not in limits.POLL_CLOSE_MODES:
        return None, "bad_close_mode"

    open_period_seconds = None
    close_at_utc = None
    if close_mode == "duration":
        try:
            open_period_seconds = int(raw_poll.get("open_period_seconds"))
        except (TypeError, ValueError):
            return None, "missing_open_period"
        if not (limits.POLL_OPEN_PERIOD_MIN_SECONDS <= open_period_seconds <= limits.POLL_OPEN_PERIOD_MAX_SECONDS):
            return None, "bad_open_period"
    elif close_mode == "date":
        if schedule_type == "recurring":
            return None, "fixed_close_date_not_allowed_for_recurring"
        raw_close = raw_poll.get("close_at_utc")
        close_at_utc = _parse_utc_datetime(raw_close)
        if close_at_utc is None:
            return None, "bad_close_date"

    return {
        "question": question,
        "type": poll_type,
        "options": [{"option_id": i, "text": t} for i, t in enumerate(option_texts)],
        "is_anonymous": is_anonymous,
        "allows_multiple_answers": allows_multiple,
        "correct_option_id": correct_option_id,
        "explanation": explanation,
        "explanation_parse_mode": explanation_parse_mode if explanation else None,
        "members_only": members_only,
        "close_mode": close_mode,
        "open_period_seconds": open_period_seconds,
        "close_at_utc": close_at_utc,
    }, None


def _parse_utc_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            s = str(value)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        dt = KL_TZ.localize(dt)
    return dt.astimezone(timezone.utc)


def validate_post_payload(payload: dict, *, existing: dict | None = None) -> tuple[dict | None, str | None]:
    """Validate + normalise a create/update payload. Returns (fields, error_code)."""
    content_type = payload.get("content_type") or (existing or {}).get("content_type")
    if content_type not in limits.CONTENT_TYPES:
        return None, "bad_content_type"

    title = str(payload.get("title") or "").strip()
    err = limits.validate_text_len(title, limits.INTERNAL_TITLE_MAX_LEN)
    if err or not title:
        return None, err or "missing_title"

    destination_key = payload.get("destination_key")
    destination = get_destination(destination_key) if destination_key else None
    if not destination or not destination.get("enabled"):
        return None, "invalid_destination"
    needs_posts = content_type in ("text", "photo", "animation", "video", "media_group")
    needs_polls = content_type in ("poll", "quiz")
    if needs_posts and not destination.get("allow_posts"):
        return None, "destination_disallows_posts"
    if needs_polls and not destination.get("allow_polls"):
        return None, "destination_disallows_polls"

    schedule_type = payload.get("schedule_type") or "once"
    if schedule_type not in ("once", "recurring"):
        return None, "bad_schedule_type"

    text = payload.get("text")
    parse_mode = payload.get("parse_mode") or "HTML"
    sanitized_text = None
    if content_type == "text":
        raw_text = str(text or "")
        err = limits.validate_text_len(raw_text, limits.TEXT_MAX_LEN)
        if err or not raw_text.strip():
            return None, err or "missing_text"
        sanitized_text, herr = limits.sanitize_telegram_html(raw_text) if parse_mode == "HTML" else (raw_text, None)
        if herr:
            return None, herr
    elif content_type in ("photo", "animation", "video", "media_group"):
        raw_caption = str(text or "")
        if raw_caption:
            err = limits.validate_text_len(raw_caption, limits.CAPTION_MAX_LEN)
            if err:
                return None, err
            sanitized_text, herr = limits.sanitize_telegram_html(raw_caption) if parse_mode == "HTML" else (raw_caption, None)
            if herr:
                return None, herr
        else:
            sanitized_text = ""

    media, err = _clean_media(payload.get("media"), content_type)
    if err:
        return None, err

    poll, err = _clean_poll(payload.get("poll"), content_type, schedule_type)
    if err:
        return None, err

    buttons, err = _clean_buttons(payload.get("buttons"))
    if err:
        return None, err
    if content_type in ("poll", "quiz") and buttons and not destination.get("allow_pin", True):
        pass  # buttons on polls don't require pin permission; no-op guard kept for clarity

    tags = payload.get("campaign_tags") or []
    err = limits.validate_campaign_tags(tags)
    if err:
        return None, err

    notes = payload.get("internal_notes")
    if notes:
        err = limits.validate_text_len(str(notes), limits.INTERNAL_NOTES_MAX_LEN)
        if err:
            return None, err

    pin_after_send = bool(payload.get("pin_after_send", False))
    if pin_after_send and not destination.get("allow_pin"):
        return None, "destination_disallows_pin"

    unpin_at_utc = _parse_utc_datetime(payload.get("unpin_at_utc")) if payload.get("unpin_at_utc") else None

    recurrence = None
    if schedule_type == "recurring":
        recurrence = _clean_recurrence(payload.get("recurrence"))
        if recurrence is None:
            return None, "bad_recurrence"

    fields = {
        "title": title,
        "destination_key": destination["key"],
        "destination_chat_id": destination["chat_id"],
        "destination_name": destination["name"],
        "content_type": content_type,
        "text": sanitized_text,
        "parse_mode": parse_mode if parse_mode in ("HTML", "MarkdownV2") else "HTML",
        "media": media,
        "poll": poll,
        "buttons": buttons,
        "disable_web_page_preview": bool(payload.get("disable_web_page_preview", False)),
        "disable_notification": bool(payload.get("disable_notification", False)),
        "protect_content": bool(payload.get("protect_content", False)),
        "pin_after_send": pin_after_send,
        "unpin_at_utc": unpin_at_utc,
        "schedule_type": schedule_type,
        "recurrence": recurrence,
        "campaign_tags": [str(t).strip() for t in tags],
        "internal_notes": str(notes).strip() if notes else None,
    }
    return fields, None


_WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _clean_recurrence(raw: dict | None) -> dict | None:
    if not isinstance(raw, dict):
        return None
    rtype = raw.get("type")
    if rtype == "daily":
        return {"type": "daily"}
    if rtype == "weekly":
        days = raw.get("weekdays") or []
        if not isinstance(days, list) or not days:
            return None
        norm = []
        for d in days:
            key = str(d).strip().lower()[:3]
            if key not in _WEEKDAYS:
                return None
            norm.append(_WEEKDAYS[key])
        return {"type": "weekly", "weekdays": sorted(set(norm))}
    return None


def compute_next_occurrence(recurrence: dict, after_utc: datetime) -> datetime | None:
    after_kl = after_utc.astimezone(KL_TZ)
    if recurrence["type"] == "daily":
        nxt = after_kl + timedelta(days=1)
        return nxt.astimezone(timezone.utc)
    if recurrence["type"] == "weekly":
        weekdays = sorted(recurrence.get("weekdays") or [])
        if not weekdays:
            return None
        for delta in range(1, 8):
            candidate = after_kl + timedelta(days=delta)
            if candidate.weekday() in weekdays:
                return candidate.astimezone(timezone.utc)
    return None


# ---------------------------------------------------------------------------
# CRUD / workflow
# ---------------------------------------------------------------------------

def _new_post_id():
    return ObjectId()


def to_object_id(raw) -> ObjectId | None:
    try:
        return ObjectId(str(raw))
    except (InvalidId, TypeError):
        return None


def create_post(payload: dict, *, actor_id, actor_username: str = "") -> tuple[dict | None, str | None]:
    fields, err = validate_post_payload(payload)
    if err:
        return None, err
    ts = now_utc()
    doc = {
        **fields,
        "status": "draft",
        "poll_status": "not_applicable",
        "scheduled_at_utc": None,
        "timezone": "Asia/Kuala_Lumpur",
        "next_run_at_utc": None,
        "last_run_at_utc": None,
        "telegram_message_ids": [],
        "telegram_poll_ids": [],
        "attempt_count": 0,
        "last_error_code": None,
        "last_error_message": None,
        "last_exception_class": None,
        "retryable": None,
        "last_attempt_at_utc": None,
        "latest_run_id": None,
        "created_by": actor_id,
        "created_by_username": actor_username,
        "approved_by": None,
        "approved_by_username": None,
        "created_at": ts,
        "updated_at": ts,
        "approved_at": None,
        "published_at": None,
        "cancelled_at": None,
        "unpinned_at_utc": None,
        "processing_owner": None,
        "processing_started_at_utc": None,
    }
    res = _posts().insert_one(doc)
    doc["_id"] = res.inserted_id
    record_audit(doc["_id"], "create", actor_id=actor_id, after=_public_view(doc))
    return doc, None


_EDITABLE_STATUSES = ("draft", "pending_approval", "scheduled")


def update_post(post_id, payload: dict, *, actor_id, actor_username: str = "", expected_updated_at=None) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] not in _EDITABLE_STATUSES:
        return None, "not_editable"
    if expected_updated_at is not None:
        stored = post.get("updated_at")
        expected_dt = _parse_utc_datetime(expected_updated_at)
        if stored is not None and expected_dt is not None:
            stored_utc = stored if stored.tzinfo else stored.replace(tzinfo=timezone.utc)
            if abs((stored_utc - expected_dt).total_seconds()) > 1:
                return None, "stale_edit"

    fields, err = validate_post_payload(payload, existing=post)
    if err:
        return None, err

    before = _public_view(post)
    ts = now_utc()
    updates = {**fields, "updated_at": ts}

    was_approved = post["status"] == "scheduled" and post.get("approved_at")
    if post["status"] == "pending_approval":
        # Editing pending content returns it to draft (spec section 20).
        updates["status"] = "draft"
    if was_approved:
        # Editing approved/scheduled content invalidates approval.
        updates["approved_by"] = None
        updates["approved_by_username"] = None
        updates["approved_at"] = None
        updates["status"] = "draft"
        updates["next_run_at_utc"] = None
        updates["scheduled_at_utc"] = None

    _posts().update_one({"_id": post_id}, {"$set": updates})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "edit", actor_id=actor_id, before=before, after=_public_view(post))
    return post, None


def get_post(post_id) -> dict | None:
    return _posts().find_one({"_id": post_id})


def list_posts(*, status=None, statuses=None, content_type=None, destination_key=None, poll_status=None,
                limit=50, skip=0, sort_field="updated_at", sort_dir=-1) -> list[dict]:
    query: dict[str, Any] = {}
    if status:
        query["status"] = status
    if statuses:
        query["status"] = {"$in": list(statuses)}
    if content_type:
        query["content_type"] = content_type
    if destination_key:
        query["destination_key"] = destination_key
    if poll_status:
        query["poll_status"] = poll_status
    return list(_posts().find(query, sort=[(sort_field, sort_dir)], limit=limit, skip=skip))


def duplicate_post(post_id, *, actor_id, actor_username: str = "") -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    ts = now_utc()
    media = [
        {**m, "telegram_file_id": m.get("telegram_file_id")}  # prefer reusing file_id; storage_key kept as fallback
        for m in (post.get("media") or [])
    ]
    poll = None
    if post.get("poll"):
        poll = {**post["poll"]}
        poll["close_at_utc"] = None if poll.get("close_mode") == "date" else poll.get("close_at_utc")
    doc = {
        "title": f"{post['title']} (copy)",
        "destination_key": post["destination_key"],
        "destination_chat_id": post["destination_chat_id"],
        "destination_name": post["destination_name"],
        "content_type": post["content_type"],
        "text": post.get("text"),
        "parse_mode": post.get("parse_mode", "HTML"),
        "media": media,
        "poll": poll,
        "buttons": post.get("buttons") or [],
        "disable_web_page_preview": post.get("disable_web_page_preview", False),
        "disable_notification": post.get("disable_notification", False),
        "protect_content": post.get("protect_content", False),
        "pin_after_send": post.get("pin_after_send", False),
        "unpin_at_utc": None,
        "status": "draft",
        "poll_status": "not_applicable",
        "schedule_type": "once",
        "scheduled_at_utc": None,
        "timezone": "Asia/Kuala_Lumpur",
        "recurrence": None,
        "next_run_at_utc": None,
        "last_run_at_utc": None,
        "telegram_message_ids": [],
        "telegram_poll_ids": [],
        "attempt_count": 0,
        "last_error_code": None,
        "last_error_message": None,
        "campaign_tags": post.get("campaign_tags") or [],
        "internal_notes": post.get("internal_notes"),
        "created_by": actor_id,
        "created_by_username": actor_username,
        "approved_by": None,
        "approved_by_username": None,
        "created_at": ts,
        "updated_at": ts,
        "approved_at": None,
        "published_at": None,
        "cancelled_at": None,
        "unpinned_at_utc": None,
        "processing_owner": None,
        "processing_started_at_utc": None,
        "duplicated_from": post_id,
    }
    res = _posts().insert_one(doc)
    doc["_id"] = res.inserted_id
    record_audit(doc["_id"], "duplicate", actor_id=actor_id, after=_public_view(doc))
    return doc, None


def delete_post(post_id, *, actor_id) -> tuple[bool, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return False, "not_found"
    if post["status"] not in ("draft", "cancelled", "failed"):
        return False, "not_deletable"
    _posts().delete_one({"_id": post_id})
    record_audit(post_id, "delete", actor_id=actor_id, before=_public_view(post))
    return True, None


def _approval_enabled() -> bool:
    try:
        from settings_service import get_setting
        return bool(get_setting("feature_flags", "community_post_approval_enabled"))
    except Exception:
        return False


def _self_approval_allowed() -> bool:
    try:
        from settings_service import get_setting
        return bool(get_setting("feature_flags", "community_post_self_approval_allowed"))
    except Exception:
        return False


def submit_for_approval(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] != "draft":
        return None, "not_draft"
    status = "pending_approval" if _approval_enabled() else "approved"
    _posts().update_one({"_id": post_id}, {"$set": {"status": status, "updated_at": now_utc()}})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "submit_for_approval", actor_id=actor_id, after=_public_view(post))
    return post, None


def approve_post(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] != "pending_approval":
        return None, "not_pending_approval"
    if post.get("created_by") == actor_id and not _self_approval_allowed():
        return None, "self_approval_not_allowed"
    ts = now_utc()
    _posts().update_one({"_id": post_id}, {"$set": {
        "status": "approved", "approved_by": actor_id, "approved_at": ts, "updated_at": ts,
    }})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "approve", actor_id=actor_id, after=_public_view(post))
    return post, None


def reject_post(post_id, *, actor_id, reason: str = "") -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] != "pending_approval":
        return None, "not_pending_approval"
    ts = now_utc()
    _posts().update_one({"_id": post_id}, {"$set": {"status": "draft", "updated_at": ts}})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "reject", actor_id=actor_id, after=_public_view(post), before={"reason": reason})
    return post, None


def _run_key(post_id, scheduled_for_utc: datetime) -> str:
    return f"community_post:{post_id}:{scheduled_for_utc.strftime('%Y%m%dT%H%M%S.%f')}"


def _validate_poll_close_vs_publish(poll: dict | None, scheduled_at_utc: datetime) -> str | None:
    """Shared by schedule_post and reschedule_post — a fixed poll close date
    must stay in the future, after the (possibly new) publish time, and
    within our own scheduling ceiling."""
    if not poll or poll.get("close_mode") != "date" or not poll.get("close_at_utc"):
        return None
    if poll["close_at_utc"] <= now_utc():
        return "poll_close_date_in_past"
    if poll["close_at_utc"] <= scheduled_at_utc:
        return "poll_closes_before_publish"
    lead = (poll["close_at_utc"] - scheduled_at_utc).total_seconds()
    if not (limits.POLL_CLOSE_DATE_MIN_LEAD_SECONDS <= lead <= limits.POLL_CLOSE_DATE_MAX_LEAD_SECONDS):
        return "poll_close_date_out_of_range"
    return None


def schedule_post(post_id, *, actor_id, scheduled_at_utc: datetime) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] not in ("draft", "approved"):
        return None, "not_schedulable"
    if _approval_enabled() and post["status"] != "approved":
        return None, "approval_required"
    if scheduled_at_utc <= now_utc():
        return None, "schedule_in_past"

    err = _validate_poll_close_vs_publish(post.get("poll"), scheduled_at_utc)
    if err:
        return None, err

    ts = now_utc()
    set_fields = {
        "status": "scheduled",
        "scheduled_at_utc": scheduled_at_utc,
        "next_run_at_utc": scheduled_at_utc,
        "updated_at": ts,
    }
    if post["content_type"] in ("poll", "quiz"):
        set_fields["poll_status"] = "scheduled"
    _posts().update_one({"_id": post_id}, {"$set": set_fields})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "schedule", actor_id=actor_id, after=_public_view(post))
    return post, None


def reschedule_post(post_id, *, actor_id, scheduled_at_utc: datetime) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] != "scheduled":
        return None, "not_scheduled"
    if scheduled_at_utc <= now_utc():
        return None, "schedule_in_past"
    err = _validate_poll_close_vs_publish(post.get("poll"), scheduled_at_utc)
    if err:
        return None, err
    before = _public_view(post)
    ts = now_utc()
    _posts().update_one({"_id": post_id}, {"$set": {
        "scheduled_at_utc": scheduled_at_utc, "next_run_at_utc": scheduled_at_utc, "updated_at": ts,
    }})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "reschedule", actor_id=actor_id, before=before, after=_public_view(post))
    return post, None


def publish_now(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] not in ("draft", "approved"):
        return None, "not_publishable"
    if _approval_enabled() and post["status"] != "approved":
        return None, "approval_required"
    ts = now_utc()
    set_fields = {"status": "scheduled", "scheduled_at_utc": ts, "next_run_at_utc": ts, "updated_at": ts}
    if post["content_type"] in ("poll", "quiz"):
        set_fields["poll_status"] = "scheduled"
    _posts().update_one({"_id": post_id}, {"$set": set_fields})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "publish_now", actor_id=actor_id, after=_public_view(post))
    # This (Flask/web) process only validates and marks the post due — it
    # does not execute the send itself. The worker process's short-interval
    # tick (see community_centre_tick / main.py run_worker()) is the only
    # code path that claims and sends, via the same atomic find_one_and_update
    # this function relies on for idempotency across instances.
    return post, None


def cancel_post(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] not in ("draft", "pending_approval", "approved", "scheduled", "failed"):
        return None, "not_cancellable"
    ts = now_utc()
    _posts().update_one({"_id": post_id}, {"$set": {
        "status": "cancelled", "cancelled_at": ts, "updated_at": ts, "next_run_at_utc": None,
    }})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "cancel", actor_id=actor_id, after=_public_view(post))
    return post, None


_RETRY_DELAY_SECONDS = 3


def retry_post(post_id, *, actor_id, force: bool = False) -> tuple[dict | None, str | None]:
    """Requeue a failed one-time/recurring post for immediate (re)delivery.

    Returns a specific conflict code instead of a generic failure so the
    admin UI can explain *why* — never a bare "not schedulable" (that code
    belongs to schedule_post's draft/approved transition, not this one).
    """
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "post_not_found"
    if post["status"] in ("published", "partially_published"):
        return None, "already_published"
    if post["status"] != "failed":
        return None, "not_failed"
    # retryable is server-computed at failure time and cannot be overridden
    # by the caller — force only bypasses the MAX_ATTEMPTS ceiling below.
    if post.get("retryable") is False:
        return None, "non_retryable_failure"

    attempt_count = post.get("attempt_count", 0)
    if not force and attempt_count >= limits.MAX_ATTEMPTS:
        return None, "retry_limit_reached"

    destination = get_destination(post.get("destination_key"))
    if not destination or not destination.get("enabled"):
        return None, "invalid_destination"

    # Duplicate-send protection: if the most recent run for this post
    # actually delivered to Telegram (send succeeded but post-side
    # finalisation crashed before flipping status), reconcile instead of
    # blindly resending the content a second time.
    last_run = _runs().find_one({"community_post_id": post_id}, sort=[("created_at_utc", -1)])
    if last_run and last_run.get("status") in ("published", "partially_published"):
        _reconcile_post_from_run(post, last_run)
        return None, "already_published"

    now = now_utc()
    stale_cutoff = now - timedelta(seconds=limits.PROCESSING_TIMEOUT_SECONDS)
    updated = _posts().find_one_and_update(
        {
            "_id": post_id,
            "status": "failed",
            "$or": [
                {"processing_started_at_utc": {"$exists": False}},
                {"processing_started_at_utc": None},
                {"processing_started_at_utc": {"$lte": stale_cutoff}},
            ],
        },
        {
            "$set": {
                "status": "scheduled",
                # Original scheduled_at_utc is left untouched — it stays as
                # the audit/history record of when this occurrence was first
                # due. next_run_at_utc (a different timestamp) is what
                # _run_key() hashes, so this retry gets a fresh run identity
                # rather than reclaiming the terminal failed run.
                "next_run_at_utc": now + timedelta(seconds=_RETRY_DELAY_SECONDS),
                "processing_owner": None,
                "processing_started_at_utc": None,
                "last_error_code": None,
                "last_error_message": None,
                "last_exception_class": None,
                "retryable": None,
                "updated_at": now,
            },
            "$inc": {"retry_count": 1},
        },
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        current = _posts().find_one({"_id": post_id})
        if not current:
            return None, "post_not_found"
        if current["status"] in ("published", "partially_published"):
            return None, "already_published"
        if current["status"] == "processing":
            return None, "already_processing"
        if current["status"] != "failed":
            return None, "not_failed"
        # Still "failed" but the staleness $or didn't match — a live
        # (non-stale) processing run owns this occurrence.
        return None, "already_processing"

    record_audit(post_id, "retry", actor_id=actor_id, after=_public_view(updated))
    return updated, None


def _public_view(post: dict) -> dict:
    """Strip internal-only Mongo bits before echoing into audit/API bodies."""
    if not post:
        return {}
    view = dict(post)
    view.pop("_id", None)
    return view


# ---------------------------------------------------------------------------
# Telegram gateway — the one place that talks HTTP to Telegram.
# ---------------------------------------------------------------------------

def _bot():
    from app_context import get_app_bot
    app_bot = get_app_bot()
    if app_bot is None or app_bot.bot is None:
        raise RuntimeError("bot_not_ready")
    return app_bot.bot


def _run_coro(coro, timeout: int = 20):
    from app_context import run_bot_coroutine
    return run_bot_coroutine(coro, timeout=timeout)


def categorize_telegram_error(exc: Exception) -> tuple[str, str]:
    """Map a raised exception to (error_code, sanitized_admin_message).

    Never returns a raw Telegram payload, token, or stack trace — only the
    fixed, sanitized messages below. `unknown_error` must never claim the
    failure was Telegram's fault; it means something failed on our side
    before/without a classifiable Telegram response.
    """
    try:
        from telegram.error import BadRequest, Forbidden, TimedOut, NetworkError, RetryAfter
    except Exception:
        BadRequest = Forbidden = TimedOut = NetworkError = RetryAfter = ()  # type: ignore

    if isinstance(exc, RuntimeError) and str(exc) == "bot_not_ready":
        return "bot_loop_not_running", "The publishing worker process is not currently running."
    # TypeError here almost always means a keyword argument passed to a
    # bot.send_* call isn't supported by the pinned python-telegram-bot
    # version — preserve the real message (never secrets/tokens, this is a
    # local Python error, not a Telegram payload) instead of collapsing it
    # into the generic unknown_error copy.
    if isinstance(exc, TypeError):
        return "local_type_error", f"Local error before Telegram was contacted: {str(exc)[:300]}"
    if RetryAfter and isinstance(exc, RetryAfter):
        return "telegram_rate_limited", "Rate limited by Telegram; will retry."
    if Forbidden and isinstance(exc, Forbidden):
        return "telegram_forbidden", "Bot is not allowed to post in this destination."
    # BadRequest subclasses NetworkError in python-telegram-bot — check it
    # before the generic TimedOut/NetworkError branch or every BadRequest
    # would misclassify as a transient network_timeout.
    if BadRequest and isinstance(exc, BadRequest):
        msg = str(exc).lower()
        if "chat not found" in msg:
            return "chat_not_found", "Destination chat not found."
        if "not enough rights" in msg or "have no rights" in msg or "administrator" in msg:
            return "bot_lacks_permission", "Bot lacks permission to post in this destination."
        if "message is too long" in msg or "text is too long" in msg:
            return "message_too_long", "Content exceeds Telegram's length limit."
        if "poll" in msg:
            return "invalid_poll", "Poll configuration was rejected by Telegram."
        if "file identifier" in msg or "file_id" in msg:
            return "invalid_media_file_id", "Saved media reference is no longer valid on Telegram."
        if "wrong file" in msg or "photo" in msg or "video" in msg or "animation" in msg:
            return "invalid_media", "Media could not be sent."
        if "wrong parameter" in msg or "url" in msg:
            return "invalid_url", "A button URL was rejected by Telegram."
        return "invalid_request", "Telegram rejected the request."
    if (TimedOut, NetworkError) and isinstance(exc, (TimedOut, NetworkError)):
        return "network_timeout", "Network timeout contacting Telegram."
    return "unknown_error", "Internal publish error before a Telegram response was received."


# ---------------------------------------------------------------------------
# Optional "Test destination" permission check (getChat / getChatMember).
# Manually triggered / cached — never run implicitly on Composer load.
# ---------------------------------------------------------------------------

_DEST_TEST_CACHE_TTL_SECONDS = 300
_dest_test_cache: dict[str, tuple[float, dict]] = {}


async def _do_test_destination(chat_id: int) -> dict:
    bot = _bot()
    chat = await bot.get_chat(chat_id)
    member = await bot.get_chat_member(chat_id, bot.id)
    status = getattr(member, "status", None)
    is_admin = status in ("administrator", "creator")
    # ChatMemberAdministrator's can_post_messages/can_edit_messages are
    # channel-only (None in groups/supergroups) and can_pin_messages is
    # groups/supergroups-only (None in channels) — a channel admin's pin
    # right is represented by can_edit_messages instead. Branch on the
    # actual chat type rather than reading fields that don't apply to it.
    is_channel = getattr(chat, "type", None) == "channel"
    if status == "creator":
        can_post, can_pin = True, True
    elif is_admin and is_channel:
        can_post = bool(getattr(member, "can_post_messages", False))
        can_pin = bool(getattr(member, "can_edit_messages", False))
    elif is_admin:
        can_post = True  # group/supergroup admins can always post
        can_pin = bool(getattr(member, "can_pin_messages", False))
    else:
        can_post, can_pin = False, False
    return {
        "reachable": True,
        "chat_title": getattr(chat, "title", None) or getattr(chat, "username", None),
        "is_admin": bool(is_admin),
        "can_post_messages": bool(can_post),
        "can_post_polls": bool(can_post),
        "can_pin_messages": bool(can_pin),
    }


def test_destination(key: str, *, force_refresh: bool = False) -> tuple[dict | None, str | None]:
    """Non-blocking permission probe for a destination. Cached for
    _DEST_TEST_CACHE_TTL_SECONDS so the Composer never triggers a live
    Telegram call just by loading."""
    dest = get_destination(key)
    if not dest:
        return None, "not_found"

    # Key by (key, chat_id): if the admin repoints this destination's chat id
    # in Telegram Configuration, that's a different cache entry, not a stale
    # hit for the chat that used to live under this key.
    cache_key = (key, dest["chat_id"])
    now = time.monotonic()
    if not force_refresh:
        cached = _dest_test_cache.get(cache_key)
        if cached and (now - cached[0]) < _DEST_TEST_CACHE_TTL_SECONDS:
            return cached[1], None

    try:
        result = _run_coro(_do_test_destination(dest["chat_id"]))
    except Exception as exc:
        _, msg = categorize_telegram_error(exc)
        result = {
            "reachable": False,
            "is_admin": False,
            "can_post_messages": False,
            "can_post_polls": False,
            "can_pin_messages": False,
            "error": msg,
        }
    _dest_test_cache[cache_key] = (now, result)
    return result, None


def build_reply_markup(buttons: list[dict]):
    if not buttons:
        return None
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

    rows_map: dict[int, list[dict]] = {}
    for b in buttons:
        rows_map.setdefault(b["row"], []).append(b)
    keyboard = []
    for row_idx in sorted(rows_map):
        row_buttons = sorted(rows_map[row_idx], key=lambda b: b["position"])
        row = []
        for b in row_buttons:
            if b["type"] in ("url", "telegram_link"):
                row.append(InlineKeyboardButton(text=b["text"], url=b["value"]))
            elif b["type"] == "webapp":
                row.append(InlineKeyboardButton(text=b["text"], web_app=WebAppInfo(url=b["value"])))
            elif b["type"] == "callback":
                row.append(InlineKeyboardButton(text=b["text"], callback_data=b["value"]))
        if row:
            keyboard.append(row)
    return InlineKeyboardMarkup(keyboard) if keyboard else None


def _media_source(item: dict):
    lib_id = item.get("media_library_id")
    if lib_id:
        # Re-resolve at send time so a reupload's refreshed file_id is
        # always used, never a possibly-stale snapshot from when the post
        # was drafted/scheduled. If the library entry has since been
        # archived/deleted, fall back to the file_id captured at
        # draft/schedule time rather than failing an already-approved post.
        lib_doc, lib_err = community_media_library.resolve_for_publish(lib_id, expected_media_type=item.get("type"))
        if lib_doc:
            return lib_doc["file_id"]
        logger.warning(
            "[COMMUNITY_CENTRE] media_library re-resolve failed at publish media_id=%s error=%s — using cached file_id",
            lib_id, lib_err,
        )
    return item.get("telegram_file_id") or item.get("storage_key")


def _bump_media_library_usage(post: dict) -> None:
    lib_ids = {m.get("media_library_id") for m in (post.get("media") or []) if m.get("media_library_id")}
    for lib_id in lib_ids:
        try:
            community_media_library.increment_usage(lib_id)
        except Exception:
            logger.exception("[COMMUNITY_CENTRE] usage_count increment failed media_id=%s", lib_id)


def _log_send_kwargs(post_id, kwargs: dict) -> None:
    logger.info("[COMMUNITY_PUBLISH][SEND_MESSAGE_ARGS] post_id=%s keys=%s", post_id, sorted(kwargs.keys()))


async def _do_send(post: dict, _step: dict | None = None) -> dict:
    """Send `post` to its destination and return
    {"message_ids": [...], "poll_id": str|None, "poll_message_id": None}.

    `_step` (optional) is a mutable dict the caller can inspect after a
    failure to learn how far publishing got before raising — see
    _execute_publish's except block, which persists it as
    last_failed_step so a failure that never reaches Telegram (bad bot
    context, unsupported kwarg, malformed keyboard, ...) is diagnosable
    from the run ledger instead of collapsing into a single opaque
    "unknown_error"."""
    step = _step if _step is not None else {}
    step["name"] = "get_bot"
    bot = _bot()
    chat_id = post["destination_chat_id"]
    step["name"] = "build_keyboard"
    reply_markup = build_reply_markup(post.get("buttons") or [])
    content_type = post["content_type"]
    common = {}
    if post.get("disable_notification"):
        common["disable_notification"] = True
    if post.get("protect_content"):
        common["protect_content"] = True

    if content_type == "text":
        step["name"] = "build_payload"
        kwargs = {"chat_id": chat_id, "text": post["text"], **common}
        if post.get("parse_mode"):
            kwargs["parse_mode"] = post["parse_mode"]
        if post.get("disable_web_page_preview"):
            kwargs["disable_web_page_preview"] = True
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        _log_send_kwargs(post["_id"], kwargs)
        step["name"] = "telegram_call"
        msg = await bot.send_message(**kwargs)
        step["name"] = "telegram_call_returned"
        return {"message_ids": [msg.message_id], "poll_id": None, "poll_message_id": None}

    if content_type in ("photo", "animation", "video"):
        step["name"] = "build_payload"
        media_item = post["media"][0]
        source = _media_source(media_item)
        kwargs = {"chat_id": chat_id, **common}
        if post.get("text"):
            kwargs["caption"] = post["text"]
            if post.get("parse_mode"):
                kwargs["parse_mode"] = post["parse_mode"]
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        _log_send_kwargs(post["_id"], kwargs)
        step["name"] = "telegram_call"
        if content_type == "photo":
            msg = await bot.send_photo(photo=source, **kwargs)
        elif content_type == "animation":
            msg = await bot.send_animation(animation=source, **kwargs)
        else:
            msg = await bot.send_video(video=source, **kwargs)
        step["name"] = "telegram_call_returned"
        return {"message_ids": [msg.message_id], "poll_id": None, "poll_message_id": None}

    if content_type == "media_group":
        from telegram import InputMediaPhoto, InputMediaVideo
        step["name"] = "build_payload"
        group = []
        for idx, item in enumerate(post["media"]):
            source = _media_source(item)
            kwargs = {}
            if idx == 0 and post.get("text"):
                kwargs["caption"] = post["text"]
                if post.get("parse_mode"):
                    kwargs["parse_mode"] = post["parse_mode"]
            if item["type"] == "photo":
                group.append(InputMediaPhoto(media=source, **kwargs))
            else:
                group.append(InputMediaVideo(media=source, **kwargs))
        _log_send_kwargs(post["_id"], {"chat_id": chat_id, "media": group, **common})
        step["name"] = "telegram_call"
        messages = await bot.send_media_group(chat_id=chat_id, media=group, **common)
        step["name"] = "telegram_call_returned"
        message_ids = [m.message_id for m in messages]
        buttons_failed = False
        buttons_error = None
        if reply_markup:
            # The album itself is already delivered at this point. If the
            # follow-up keyboard message fails, we must not lose the album's
            # message_ids by letting the exception propagate — that would
            # make _execute_publish treat the whole occurrence as failed and
            # resend the entire album on retry, duplicating already-live
            # content. Report the partial failure instead.
            try:
                follow_up = await bot.send_message(chat_id=chat_id, text="·", reply_markup=reply_markup, **common)
                message_ids.append(follow_up.message_id)
            except Exception as exc:
                buttons_failed = True
                buttons_error = str(exc)
        return {
            "message_ids": message_ids, "poll_id": None, "poll_message_id": None,
            "buttons_failed": buttons_failed, "buttons_error": buttons_error,
        }

    if content_type in ("poll", "quiz"):
        step["name"] = "build_payload"
        poll = post["poll"]
        kwargs = {
            "chat_id": chat_id,
            "question": poll["question"],
            "options": [o["text"] for o in poll["options"]],
            "is_anonymous": poll["is_anonymous"],
            "type": poll["type"],
            "allows_multiple_answers": poll["allows_multiple_answers"],
            **common,
        }
        if poll["type"] == "quiz":
            kwargs["correct_option_id"] = poll["correct_option_id"]
            if poll.get("explanation"):
                kwargs["explanation"] = poll["explanation"]
                kwargs["explanation_parse_mode"] = poll.get("explanation_parse_mode") or "HTML"
        # Telegram's native open_period/close_date parameters only accept a
        # 5-600 second window (Poll.MAX_OPEN_PERIOD in PTB 20.8). Configured
        # durations/dates within that range are passed straight through so
        # Telegram closes the poll itself; anything longer is sent as an
        # open-ended poll and closed by the worker's soft-close tick
        # (run_due_poll_closures) once poll.close_at_utc arrives instead —
        # see _execute_publish, which backfills that field after send.
        native_cap = limits.TELEGRAM_NATIVE_POLL_DURATION_MAX_SECONDS
        if poll.get("close_mode") == "duration" and poll.get("open_period_seconds"):
            if poll["open_period_seconds"] <= native_cap:
                kwargs["open_period"] = poll["open_period_seconds"]
        elif poll.get("close_mode") == "date" and poll.get("close_at_utc"):
            lead = (poll["close_at_utc"] - now_utc()).total_seconds()
            if limits.POLL_CLOSE_DATE_MIN_LEAD_SECONDS <= lead <= native_cap:
                kwargs["close_date"] = poll["close_at_utc"]
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        _log_send_kwargs(post["_id"], kwargs)
        step["name"] = "telegram_call"
        msg = await bot.send_poll(**kwargs)
        step["name"] = "telegram_call_returned"
        return {
            "message_ids": [msg.message_id],
            "poll_id": msg.poll.id if msg.poll else None,
            "poll_message_id": msg.message_id,
        }

    raise ValueError(f"unsupported_content_type:{content_type}")


async def _do_stop_poll(chat_id: int, message_id: int):
    bot = _bot()
    return await bot.stop_poll(chat_id=chat_id, message_id=message_id)


async def _do_pin(chat_id: int, message_id: int, *, disable_notification: bool = True):
    bot = _bot()
    return await bot.pin_chat_message(chat_id=chat_id, message_id=message_id, disable_notification=disable_notification)


async def _do_unpin(chat_id: int, message_id: int | None):
    bot = _bot()
    if message_id:
        return await bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
    return await bot.unpin_chat_message(chat_id=chat_id)


# ---------------------------------------------------------------------------
# Restart-safe worker
# ---------------------------------------------------------------------------

def _claim_next_due_post(*, only_post_id=None):
    query: dict[str, Any] = {"status": "scheduled", "next_run_at_utc": {"$lte": now_utc()}}
    if only_post_id is not None:
        query["_id"] = only_post_id
    return _posts().find_one_and_update(
        query,
        {
            "$set": {
                "status": "processing",
                "processing_started_at_utc": now_utc(),
                "processing_owner": INSTANCE_ID,
                "updated_at": now_utc(),
            },
            "$inc": {"attempt_count": 1},
        },
        sort=[("next_run_at_utc", 1)],
        return_document=ReturnDocument.AFTER,
    )


def _claim_or_reuse_run(post_id, run_key: str, scheduled_for: datetime) -> tuple[str, dict | None]:
    """Claim the run-ledger row for (post_id, run_key) — the hard idempotency
    boundary for this specific occurrence.

    Returns (outcome, existing_run):
      "claimed"            — safe to proceed and send.
      "already_processing" — another attempt currently owns this occurrence
                              (or a crashed one hasn't been recovered yet);
                              do NOT send.
      "already_done"       — this occurrence already published; do NOT
                              re-send (caller should reconcile from the run).

    Unlike a blind insert, an existing row in a terminal-but-retryable state
    (e.g. "failed" after recover_stale_processing marked a crashed attempt)
    is reclaimed via update rather than rejected — recover_stale_processing
    preserves the post's original next_run_at_utc specifically so a retry
    lands on this SAME run_key instead of manufacturing a new one, which
    would otherwise risk sending the content to Telegram a second time.
    """
    runs_col = _runs()
    now = now_utc()
    existing = runs_col.find_one({"community_post_id": post_id, "run_key": run_key})
    if existing is None:
        try:
            runs_col.insert_one({
                "community_post_id": post_id,
                "run_key": run_key,
                "scheduled_for_utc": scheduled_for,
                "status": "processing",
                "processing_owner": INSTANCE_ID,
                "processing_started_at_utc": now,
                "attempt_count": 1,
                "telegram_message_ids": [],
                "telegram_poll_id": None,
                "error_code": None,
                "error_message": None,
                "started_at_utc": now,
                "completed_at_utc": None,
                "created_at_utc": now,
                "updated_at_utc": now,
            })
            return "claimed", None
        except DuplicateKeyError:
            existing = runs_col.find_one({"community_post_id": post_id, "run_key": run_key})
    if existing is None:
        return "already_processing", None
    if existing["status"] in ("published", "partially_published"):
        return "already_done", existing
    if existing["status"] == "processing":
        return "already_processing", existing
    # "failed" (including a recovered stale crash) — retry this same
    # occurrence rather than starting a fresh one.
    runs_col.update_one({"_id": existing["_id"]}, {"$set": {
        "status": "processing", "processing_owner": INSTANCE_ID,
        "processing_started_at_utc": now, "updated_at_utc": now,
        "error_code": None, "error_message": None,
    }, "$inc": {"attempt_count": 1}})
    return "claimed", None


def _execute_publish(post: dict) -> None:
    post_id = post["_id"]
    scheduled_for = post.get("next_run_at_utc") or post.get("scheduled_at_utc") or now_utc()
    run_key = _run_key(post_id, scheduled_for)

    outcome, existing_run = _claim_or_reuse_run(post_id, run_key, scheduled_for)
    if outcome == "already_done":
        _reconcile_post_from_run(post, existing_run)
        return
    if outcome == "already_processing":
        # Another attempt currently owns this occurrence. Leave the post in
        # "processing" — recover_stale_processing() will reclaim it (and
        # this run row) if it's genuinely stuck past the timeout.
        return

    record_audit(post_id, "worker_claim", actor_type="worker", actor_id=INSTANCE_ID, run_key=run_key)

    step = {"name": "start"}
    try:
        result = _run_coro(_do_send(post, step))
    except Exception as exc:
        error_code, error_message = categorize_telegram_error(exc)
        # logger.exception (not .warning) so the full traceback lands in
        # worker logs — a failure at step != "telegram_call"/
        # "telegram_call_returned" happened locally, before/without a
        # Telegram response, and the traceback is the only way to see
        # exactly where.
        logger.exception(
            "[COMMUNITY_CENTRE][LOCAL_FAILURE] post_id=%s run_id=%s step=%s error_code=%s exception_class=%s message=%s",
            post_id, run_key, step["name"], error_code, exc.__class__.__name__, str(exc)[:300],
        )
        _fail_run(post, run_key, error_code, error_message, exc.__class__.__name__, step["name"])
        return

    message_ids = result["message_ids"]
    poll_id = result.get("poll_id")
    buttons_failed = bool(result.get("buttons_failed"))
    ts = now_utc()

    _bump_media_library_usage(post)

    pin_ok = None
    if post.get("pin_after_send") and message_ids:
        pin_target = result.get("poll_message_id") or message_ids[0]
        try:
            _run_coro(_do_pin(post["destination_chat_id"], pin_target))
            pin_ok = True
        except Exception:
            logger.exception("[COMMUNITY_CENTRE] pin failed post_id=%s", post_id)
            pin_ok = False

    status = "partially_published" if (pin_ok is False or buttons_failed) else "published"
    partial_reason = None
    if buttons_failed and pin_ok is False:
        partial_reason = "Album published; button keyboard and pin both failed."
    elif buttons_failed:
        partial_reason = "Album published; the button keyboard follow-up message failed to send."
    elif pin_ok is False:
        partial_reason = "Published; pin failed."

    _runs().update_one({"community_post_id": post_id, "run_key": run_key}, {"$set": {
        "status": status,
        "telegram_message_ids": message_ids,
        "telegram_poll_id": poll_id,
        "completed_at_utc": ts,
        "updated_at_utc": ts,
    }})

    post_updates = {
        "status": status,
        "poll_status": "open" if post["content_type"] in ("poll", "quiz") else post.get("poll_status", "not_applicable"),
        "telegram_message_ids": message_ids,
        "telegram_poll_ids": [poll_id] if poll_id else [],
        "published_at": ts,
        "last_run_at_utc": ts,
        "updated_at": ts,
        "last_error_code": "partial_delivery" if partial_reason else None,
        "last_error_message": partial_reason,
    }

    if post.get("schedule_type") == "recurring" and post.get("recurrence"):
        next_occurrence = compute_next_occurrence(post["recurrence"], scheduled_for)
        if next_occurrence:
            post_updates["next_run_at_utc"] = next_occurrence
            post_updates["scheduled_at_utc"] = next_occurrence
            post_updates["status"] = "scheduled"
        else:
            post_updates["next_run_at_utc"] = None
    else:
        post_updates["next_run_at_utc"] = None

    if poll_id and post.get("poll", {}).get("close_mode") == "duration":
        open_period = post["poll"].get("open_period_seconds")
        if open_period and open_period > limits.TELEGRAM_NATIVE_POLL_DURATION_MAX_SECONDS:
            # Exceeds Telegram's native ceiling — recalculated relative to
            # *this* occurrence's actual publish time, so recurring polls
            # each get their own correct absolute deadline (spec: "Relative
            # poll durations should be recalculated from each occurrence's
            # publish time"). run_due_poll_closures() stops it at this time.
            post_updates["poll.close_at_utc"] = ts + timedelta(seconds=open_period)

    if poll_id:
        try:
            _poll_snapshots().update_one(
                {"poll_id": poll_id},
                {"$set": {
                    "poll_id": poll_id,
                    "community_post_id": post_id,
                    "question": post["poll"]["question"],
                    "total_voter_count": 0,
                    "is_closed": False,
                    "options": [{"option_id": o["option_id"], "text": o["text"], "voter_count": 0} for o in post["poll"]["options"]],
                    "updated_at_utc": ts,
                }},
                upsert=True,
            )
        except Exception:
            logger.exception("[COMMUNITY_CENTRE] snapshot seed failed poll_id=%s", poll_id)

    _posts().update_one({"_id": post_id}, {"$set": post_updates})
    record_audit(
        post_id,
        "publish_success" if status == "published" else "partial_publish",
        actor_type="worker", actor_id=INSTANCE_ID, run_key=run_key,
        after={"telegram_message_ids": message_ids, "telegram_poll_id": poll_id, "pin_ok": pin_ok, "buttons_failed": buttons_failed},
    )


def _reconcile_post_from_run(post: dict, run: dict | None) -> None:
    if not run:
        return
    if run["status"] in ("published", "partially_published"):
        _posts().update_one({"_id": post["_id"]}, {"$set": {
            "status": run["status"],
            "telegram_message_ids": run.get("telegram_message_ids") or [],
            "telegram_poll_ids": [run["telegram_poll_id"]] if run.get("telegram_poll_id") else [],
            "published_at": run.get("completed_at_utc") or now_utc(),
            "next_run_at_utc": None,
            "updated_at": now_utc(),
        }})
    elif run["status"] == "failed":
        error_code = run.get("error_code")
        _posts().update_one({"_id": post["_id"]}, {"$set": {
            "status": "failed", "last_error_code": error_code,
            "last_error_message": run.get("error_message"),
            "last_exception_class": run.get("error_class"),
            "last_failed_step": run.get("failed_step"),
            "retryable": (error_code in limits.RETRYABLE_ERROR_CODES) if error_code else None,
            "last_attempt_at_utc": run.get("completed_at_utc") or now_utc(),
            "latest_run_id": str(run["_id"]),
            "updated_at": now_utc(),
            "processing_owner": None, "processing_started_at_utc": None,
        }})


def _fail_run(post: dict, run_key: str, error_code: str, error_message: str, error_class: str = "", failed_step: str = "") -> None:
    ts = now_utc()
    run = _runs().find_one({"community_post_id": post["_id"], "run_key": run_key}, {"_id": 1})
    _runs().update_one({"community_post_id": post["_id"], "run_key": run_key}, {"$set": {
        "status": "failed", "error_code": error_code, "error_message": error_message,
        "error_class": error_class, "failed_step": failed_step, "completed_at_utc": ts, "updated_at_utc": ts,
    }})
    is_retryable = error_code in limits.RETRYABLE_ERROR_CODES
    attempt_count = post.get("attempt_count", 1)
    common_fields = {
        "last_error_code": error_code,
        "last_error_message": error_message,
        "last_exception_class": error_class,
        "last_failed_step": failed_step,
        "retryable": is_retryable,
        "last_attempt_at_utc": ts,
        "latest_run_id": str(run["_id"]) if run else post.get("latest_run_id"),
        "updated_at": ts,
        "processing_owner": None,
        "processing_started_at_utc": None,
    }
    if is_retryable and attempt_count < limits.MAX_ATTEMPTS:
        backoff = limits.compute_backoff_seconds(attempt_count)
        next_try = ts + timedelta(seconds=backoff)
        _posts().update_one({"_id": post["_id"]}, {"$set": {
            **common_fields, "status": "scheduled", "next_run_at_utc": next_try,
        }})
    else:
        _posts().update_one({"_id": post["_id"]}, {"$set": {
            **common_fields, "status": "failed", "next_run_at_utc": None,
        }})
    record_audit(post["_id"], "publish_failure", actor_type="worker", actor_id=INSTANCE_ID, run_key=run_key,
                 after={"error_code": error_code, "retryable": is_retryable})


def run_due_posts(*, limit: int = 10, only_post_id=None) -> int:
    """Claim and execute up to `limit` due posts. Safe to call from multiple
    worker instances concurrently — the find_one_and_update claim and the
    unique run_key insert are the two idempotency boundaries."""
    processed = 0
    for _ in range(limit):
        post = _claim_next_due_post(only_post_id=only_post_id)
        if not post:
            break
        logger.info("[COMMUNITY_POSTS][DUE_POST_CLAIMED] post_id=%s attempt_count=%s", post.get("_id"), post.get("attempt_count"))
        try:
            _execute_publish(post)
        except Exception as exc:
            logger.exception("[COMMUNITY_CENTRE] unhandled error executing post_id=%s", post.get("_id"))
            _fail_run(
                post, _run_key(post["_id"], post.get("next_run_at_utc") or now_utc()),
                "unknown_error", "Internal publish error before a Telegram response was received.",
                exc.__class__.__name__, "before_execute_publish",
            )
        processed += 1
        if only_post_id is not None:
            break
    return processed


def recover_stale_processing() -> int:
    cutoff = now_utc() - timedelta(seconds=limits.PROCESSING_TIMEOUT_SECONDS)
    stale = list(_posts().find({"status": "processing", "processing_started_at_utc": {"$lte": cutoff}}))
    recovered = 0
    for post in stale:
        attempt_count = post.get("attempt_count", 1)
        ts = now_utc()
        # Mark the corresponding stale run-ledger row "failed" so a retry of
        # this SAME occurrence can reclaim it via _claim_or_reuse_run. Left
        # untouched, that row would stay "processing" forever and every
        # future retry would be refused as "already_processing" — the post
        # could never reach a terminal state (deadlock).
        _runs().update_one(
            {"community_post_id": post["_id"], "status": "processing", "processing_started_at_utc": {"$lte": cutoff}},
            {"$set": {
                "status": "failed", "error_code": "processing_timeout",
                "error_message": "Worker stopped responding while publishing.",
                "completed_at_utc": ts, "updated_at_utc": ts,
            }},
        )
        if attempt_count < limits.MAX_ATTEMPTS:
            # Deliberately do NOT bump next_run_at_utc to "now" — preserving
            # the original value keeps run_key identical on retry (see
            # _run_key), so the retry reclaims the same run-ledger row above
            # instead of manufacturing a new occurrence and risking a
            # duplicate Telegram send if the crashed attempt actually
            # succeeded before it could record that fact.
            _posts().update_one({"_id": post["_id"]}, {"$set": {
                "status": "scheduled", "updated_at": ts,
                "processing_owner": None, "processing_started_at_utc": None,
            }})
        else:
            _posts().update_one({"_id": post["_id"]}, {"$set": {
                "status": "failed", "next_run_at_utc": None, "updated_at": ts,
                "processing_owner": None, "processing_started_at_utc": None,
                "last_error_code": "processing_timeout", "last_error_message": "Worker stopped responding while publishing.",
                # A prior attempt on this same post may have left stale
                # last_exception_class/last_failed_step/retryable behind —
                # clear them so the retry modal doesn't attribute this
                # timeout to a previous attempt's local exception/step.
                "last_exception_class": "", "last_failed_step": "",
                "retryable": False,
            }})
        record_audit(post["_id"], "worker_claim", actor_type="system", after={"recovered_from": "stale_processing"})
        recovered += 1
    return recovered


def run_due_unpins() -> int:
    due = list(_posts().find({
        "pin_after_send": True,
        "unpin_at_utc": {"$lte": now_utc()},
        "unpinned_at_utc": None,
        "status": {"$in": ["published", "partially_published"]},
        "telegram_message_ids": {"$exists": True},
    }))
    count = 0
    for post in due:
        if not post.get("telegram_message_ids"):
            continue
        message_id = post["telegram_message_ids"][0]
        try:
            _run_coro(_do_unpin(post["destination_chat_id"], message_id))
        except Exception:
            logger.exception("[COMMUNITY_CENTRE] auto-unpin failed post_id=%s", post["_id"])
            continue
        _posts().update_one({"_id": post["_id"]}, {"$set": {"unpinned_at_utc": now_utc(), "updated_at": now_utc()}})
        record_audit(post["_id"], "auto_unpin", actor_type="worker", actor_id=INSTANCE_ID)
        count += 1
    return count


def run_due_poll_closures() -> int:
    """Auto-stop polls whose configured close time has arrived but which
    Telegram itself isn't closing natively — either a fixed close_at_utc
    further out than Telegram's native close_date range, or a "duration"
    poll whose open_period exceeded Telegram's native open_period ceiling
    (see _do_send / _execute_publish, which backfills poll.close_at_utc for
    that case). Polls Telegram closes natively are excluded automatically:
    their poll_status already flips to "closed" via handle_poll_update
    before this query would match them."""
    due = list(_posts().find({
        "poll_status": "open",
        "poll.close_mode": {"$in": ["date", "duration"]},
        "poll.close_at_utc": {"$lte": now_utc()},
    }))
    count = 0
    for post in due:
        if not post.get("telegram_message_ids"):
            continue
        message_id = post["telegram_message_ids"][0]
        try:
            final_poll = _run_coro(_do_stop_poll(post["destination_chat_id"], message_id))
        except Exception:
            logger.exception("[COMMUNITY_CENTRE] auto poll-close failed post_id=%s", post["_id"])
            continue
        ts = now_utc()
        _posts().update_one({"_id": post["_id"]}, {"$set": {
            "poll_status": "closed", "poll_closed_at_utc": ts, "updated_at": ts,
        }})
        try:
            options = getattr(final_poll, "options", None) or []
            poll_ids = post.get("telegram_poll_ids") or []
            if poll_ids:
                _poll_snapshots().update_one(
                    {"poll_id": poll_ids[0]},
                    {"$set": {
                        "total_voter_count": getattr(final_poll, "total_voter_count", 0),
                        "is_closed": True,
                        "options": [{"option_id": i, "text": o.text, "voter_count": o.voter_count} for i, o in enumerate(options)],
                        "updated_at_utc": ts,
                    }},
                    upsert=True,
                )
        except Exception:
            logger.exception("[COMMUNITY_CENTRE] auto poll-close snapshot save failed post_id=%s", post["_id"])
        record_audit(post["_id"], "auto_close_poll", actor_type="worker", actor_id=INSTANCE_ID)
        count += 1
    return count


def community_centre_tick() -> None:
    """Single APScheduler entry point: publish due posts, recover stale
    processing, auto-close due polls, run auto-unpins. Called on a short
    interval (see main.py)."""
    logger.debug("[COMMUNITY_POSTS][SCHEDULER_HEARTBEAT] instance=%s", INSTANCE_ID)
    try:
        run_due_posts(limit=20)
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] run_due_posts tick failed")
    try:
        recover_stale_processing()
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] recover_stale_processing tick failed")
    try:
        run_due_poll_closures()
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] run_due_poll_closures tick failed")
    try:
        run_due_unpins()
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] run_due_unpins tick failed")


# ---------------------------------------------------------------------------
# Stop poll / manual pin / unpin
# ---------------------------------------------------------------------------

def stop_poll_action(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["content_type"] not in ("poll", "quiz"):
        return None, "not_a_poll"
    if post.get("poll_status") == "closed":
        # Idempotent success: repeated stop requests never fail noisily.
        return post, None
    if post.get("poll_status") != "open" or not post.get("telegram_message_ids"):
        return None, "poll_not_open"

    message_id = post["telegram_message_ids"][0]
    try:
        final_poll = _run_coro(_do_stop_poll(post["destination_chat_id"], message_id))
    except Exception as exc:
        error_code, error_message = categorize_telegram_error(exc)
        if error_code == "invalid_request" and "already closed" in str(exc).lower():
            _posts().update_one({"_id": post_id}, {"$set": {"poll_status": "closed", "updated_at": now_utc()}})
            return _posts().find_one({"_id": post_id}), None
        return None, error_message

    ts = now_utc()
    _posts().update_one({"_id": post_id}, {"$set": {
        "poll_status": "closed", "poll_closed_at_utc": ts, "poll_closed_by": actor_id, "updated_at": ts,
    }})
    try:
        options = getattr(final_poll, "options", None) or []
        _poll_snapshots().update_one(
            {"poll_id": post["telegram_poll_ids"][0]},
            {"$set": {
                "total_voter_count": getattr(final_poll, "total_voter_count", 0),
                "is_closed": True,
                "options": [{"option_id": i, "text": o.text, "voter_count": o.voter_count} for i, o in enumerate(options)],
                "updated_at_utc": ts,
            }},
            upsert=True,
        )
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] final snapshot save failed post_id=%s", post_id)

    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "stop_poll", actor_id=actor_id, after=_public_view(post))
    return post, None


def pin_action(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if post["status"] not in ("published", "partially_published") or not post.get("telegram_message_ids"):
        return None, "not_published"
    dest = get_destination(post["destination_key"])
    if not dest or not dest.get("allow_pin"):
        return None, "destination_disallows_pin"
    try:
        _run_coro(_do_pin(post["destination_chat_id"], post["telegram_message_ids"][0]))
    except Exception as exc:
        _, msg = categorize_telegram_error(exc)
        return None, msg
    _posts().update_one({"_id": post_id}, {"$set": {"updated_at": now_utc()}})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "pin", actor_id=actor_id)
    return post, None


def unpin_action(post_id, *, actor_id) -> tuple[dict | None, str | None]:
    post = _posts().find_one({"_id": post_id})
    if not post:
        return None, "not_found"
    if not post.get("telegram_message_ids"):
        return None, "not_published"
    try:
        _run_coro(_do_unpin(post["destination_chat_id"], post["telegram_message_ids"][0]))
    except Exception as exc:
        _, msg = categorize_telegram_error(exc)
        return None, msg
    _posts().update_one({"_id": post_id}, {"$set": {"unpinned_at_utc": now_utc(), "updated_at": now_utc()}})
    post = _posts().find_one({"_id": post_id})
    record_audit(post_id, "unpin", actor_id=actor_id)
    return post, None


# ---------------------------------------------------------------------------
# Poll answer / poll update handlers (registered as PTB handlers in main.py)
# ---------------------------------------------------------------------------

def record_poll_answer(poll_id: str, user_id: int, selected_option_ids: list[int]) -> None:
    post = _posts().find_one({"telegram_poll_ids": poll_id})
    if not post or not post.get("poll") or post["poll"].get("is_anonymous"):
        # Never persist user-level votes for anonymous polls, and skip
        # entirely if we don't recognise this poll_id.
        return
    ts = now_utc()
    removed = len(selected_option_ids) == 0
    _poll_answers().update_one(
        {"poll_id": poll_id, "user_id": user_id},
        {"$set": {
            "poll_id": poll_id,
            "community_post_id": post["_id"],
            "user_id": user_id,
            "selected_option_ids": selected_option_ids,
            "updated_at_utc": ts,
            "removed_vote": removed,
        }},
        upsert=True,
    )


def record_poll_snapshot(poll_id: str, question: str, options: list[dict], total_voter_count: int, is_closed: bool) -> None:
    post = _posts().find_one({"telegram_poll_ids": poll_id})
    ts = now_utc()
    _poll_snapshots().update_one(
        {"poll_id": poll_id},
        {"$set": {
            "poll_id": poll_id,
            "community_post_id": post["_id"] if post else None,
            "question": question,
            "total_voter_count": total_voter_count,
            "is_closed": is_closed,
            "options": options,
            "updated_at_utc": ts,
        }},
        upsert=True,
    )
    if post and is_closed and post.get("poll_status") == "open":
        _posts().update_one({"_id": post["_id"]}, {"$set": {"poll_status": "closed", "updated_at": ts}})


async def handle_poll_answer_update(update, context) -> None:
    pa = update.poll_answer
    if pa is None:
        return
    user = pa.user
    if user is None:
        return
    try:
        record_poll_answer(pa.poll_id, user.id, list(pa.option_ids or []))
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] poll_answer handling failed poll_id=%s", pa.poll_id)


async def handle_poll_update(update, context) -> None:
    poll = update.poll
    if poll is None:
        return
    try:
        options = [{"option_id": i, "text": o.text, "voter_count": o.voter_count} for i, o in enumerate(poll.options)]
        record_poll_snapshot(poll.id, poll.question, options, poll.total_voter_count, poll.is_closed)
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] poll update handling failed poll_id=%s", poll.id)


def register_handlers(app_bot) -> None:
    try:
        from telegram.ext import PollAnswerHandler, PollHandler
        app_bot.add_handler(PollAnswerHandler(handle_poll_answer_update))
        app_bot.add_handler(PollHandler(handle_poll_update))
        logger.info("[COMMUNITY_CENTRE] poll handlers registered")
    except Exception:
        logger.exception("[COMMUNITY_CENTRE] poll handler registration failed")


# ---------------------------------------------------------------------------
# Poll results
# ---------------------------------------------------------------------------

def poll_results(post_id) -> dict | None:
    post = _posts().find_one({"_id": post_id})
    if not post or post["content_type"] not in ("poll", "quiz"):
        return None
    poll_id = (post.get("telegram_poll_ids") or [None])[0]
    snapshot = _poll_snapshots().find_one({"poll_id": poll_id}) if poll_id else None
    options = snapshot.get("options") if snapshot else [
        {"option_id": o["option_id"], "text": o["text"], "voter_count": 0} for o in (post["poll"]["options"] if post.get("poll") else [])
    ]
    total_voters = snapshot.get("total_voter_count", 0) if snapshot else 0
    correct_rate = None
    if post["content_type"] == "quiz" and post["poll"].get("correct_option_id") is not None and total_voters:
        correct = next((o for o in options if o["option_id"] == post["poll"]["correct_option_id"]), None)
        if correct:
            correct_rate = round((correct.get("voter_count", 0) / total_voters) * 100, 1)
    return {
        "post_id": str(post_id),
        "question": post["poll"]["question"] if post.get("poll") else None,
        "poll_type": post["poll"]["type"] if post.get("poll") else None,
        "destination_name": post.get("destination_name"),
        "published_at": post.get("published_at"),
        "poll_status": post.get("poll_status"),
        "is_anonymous": post["poll"]["is_anonymous"] if post.get("poll") else None,
        "allows_multiple_answers": post["poll"]["allows_multiple_answers"] if post.get("poll") else None,
        "total_voters": total_voters,
        "options": [
            {**o, "percentage": round((o.get("voter_count", 0) / total_voters) * 100, 1) if total_voters else 0.0}
            for o in options
        ],
        "correct_option_id": post["poll"].get("correct_option_id") if post.get("poll") else None,
        "correct_answer_rate": correct_rate,
        "closing_time": post["poll"].get("close_at_utc") if post.get("poll") else None,
        "closed_at": post.get("poll_closed_at_utc"),
        "created_by": post.get("created_by_username") or post.get("created_by"),
        "campaign_tags": post.get("campaign_tags") or [],
    }


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def build_preview(post: dict) -> dict:
    view = _public_view(post)
    view["_id"] = str(post["_id"])
    view["is_poll"] = post["content_type"] in ("poll", "quiz")
    view["button_style_note"] = "Preview style only. Actual Telegram button appearance is controlled by Telegram."
    return view


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

def calendar_entries(start_utc: datetime, end_utc: datetime) -> list[dict]:
    entries = []
    scheduled = _posts().find({
        "status": "scheduled",
        "next_run_at_utc": {"$gte": start_utc, "$lte": end_utc},
    })
    for post in scheduled:
        entries.append({
            "id": str(post["_id"]),
            "kind": "scheduled_post",
            "content_type": post["content_type"],
            "title": post["title"],
            "at_utc": post["next_run_at_utc"],
            "destination_name": post.get("destination_name"),
        })
    closing = _posts().find({
        "poll_status": "open",
        "poll.close_mode": {"$in": ["date", "duration"]},
        "poll.close_at_utc": {"$gte": start_utc, "$lte": end_utc},
    })
    for post in closing:
        entries.append({
            "id": str(post["_id"]),
            "kind": "poll_closing",
            "content_type": post["content_type"],
            "title": post["title"],
            "at_utc": post["poll"]["close_at_utc"],
            "destination_name": post.get("destination_name"),
        })
    unpins = _posts().find({
        "pin_after_send": True,
        "unpinned_at_utc": None,
        "unpin_at_utc": {"$gte": start_utc, "$lte": end_utc},
    })
    for post in unpins:
        entries.append({
            "id": str(post["_id"]),
            "kind": "auto_unpin",
            "content_type": post["content_type"],
            "title": post["title"],
            "at_utc": post["unpin_at_utc"],
            "destination_name": post.get("destination_name"),
        })
    return entries


# ---------------------------------------------------------------------------
# Flask blueprint — admin-protected JSON API
# ---------------------------------------------------------------------------

community_centre_bp = Blueprint("community_centre", __name__)


def _require_admin():
    from vouchers import require_admin
    payload, err = require_admin()
    if err:
        return None, err
    return payload, None


def _actor_id(payload: dict):
    try:
        return int(payload.get("id") or 0)
    except (TypeError, ValueError):
        return 0


def _actor_username(payload: dict) -> str:
    return payload.get("usernameLower") or payload.get("username") or ""


def _serialize_post_for_admin(post: dict) -> dict:
    """Adds a KL-local rendering of the last failure timestamp for the admin
    dashboard's retry modal — never mutates the stored document."""
    view = dict(post)
    attempt_at = view.get("last_attempt_at_utc")
    if attempt_at:
        if attempt_at.tzinfo is None:
            attempt_at = attempt_at.replace(tzinfo=timezone.utc)
        view["last_attempt_at_kl"] = attempt_at.astimezone(KL_TZ).isoformat()
    else:
        view["last_attempt_at_kl"] = None
    return view


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
    return jsonify({"success": False, "code": code}), status


def _resolve_post_id(raw):
    post_id = to_object_id(raw)
    if post_id is None:
        return None, _err("bad_id", 400)
    return post_id, None


@community_centre_bp.get("/api/admin/community/limits")
def cc_limits():
    payload, err = _require_admin()
    if err:
        return err
    return _ok({"limits": limits.limits_payload()})


@community_centre_bp.get("/api/admin/community/destinations")
def cc_list_destinations():
    payload, err = _require_admin()
    if err:
        return err
    return _ok({"destinations": list_destinations()})


@community_centre_bp.post("/api/admin/community/destinations")
def cc_upsert_destination():
    payload, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    dest, code = upsert_destination(body, actor_id=_actor_id(payload))
    if code:
        return _err(code, 400)
    return _ok({"destination": dest})


@community_centre_bp.delete("/api/admin/community/destinations/<key>")
def cc_delete_destination(key):
    payload, err = _require_admin()
    if err:
        return err
    ok = delete_destination(key)
    if not ok:
        return _err("not_found", 404)
    return _ok({})


@community_centre_bp.post("/api/admin/community/destinations/<key>/test")
def cc_test_destination(key):
    payload, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    result, code = test_destination(key, force_refresh=bool(body.get("force_refresh")))
    if code:
        return _err(code, 404)
    return _ok({"result": result})


@community_centre_bp.post("/api/admin/community/posts")
def cc_create_post():
    payload, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    post, code = create_post(body, actor_id=_actor_id(payload), actor_username=_actor_username(payload))
    if code:
        return _err(code, 400)
    return _ok({"post": post}, 201)


@community_centre_bp.get("/api/admin/community/posts")
def cc_list_posts():
    payload, err = _require_admin()
    if err:
        return err
    status = request.args.get("status")
    statuses = request.args.get("statuses")
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        skip = max(int(request.args.get("skip", 0)), 0)
    except ValueError:
        return _err("bad_pagination", 400)
    posts = list_posts(
        status=status,
        statuses=statuses.split(",") if statuses else None,
        content_type=request.args.get("content_type"),
        destination_key=request.args.get("destination_key"),
        poll_status=request.args.get("poll_status"),
        limit=limit,
        skip=skip,
    )
    return _ok({"posts": [_serialize_post_for_admin(p) for p in posts]})


@community_centre_bp.get("/api/admin/community/posts/<post_id>")
def cc_get_post(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    post = get_post(oid)
    if not post:
        return _err("not_found", 404)
    return _ok({"post": _serialize_post_for_admin(post)})


@community_centre_bp.patch("/api/admin/community/posts/<post_id>")
def cc_update_post(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    body = request.get_json(silent=True) or {}
    expected = body.pop("expected_updated_at", None)
    post, code = update_post(oid, body, actor_id=_actor_id(payload), actor_username=_actor_username(payload), expected_updated_at=expected)
    if code:
        status = 409 if code == "stale_edit" else (404 if code == "not_found" else 400)
        return _err(code, status)
    return _ok({"post": post})


@community_centre_bp.delete("/api/admin/community/posts/<post_id>")
def cc_delete_post(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    ok, code = delete_post(oid, actor_id=_actor_id(payload))
    if not ok:
        return _err(code, 404 if code == "not_found" else 400)
    return _ok({})


@community_centre_bp.post("/api/admin/community/posts/<post_id>/preview")
def cc_preview_post(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    post = get_post(oid)
    if not post:
        return _err("not_found", 404)
    return _ok({"preview": build_preview(post)})


_RETRY_ERROR_STATUS = {
    "post_not_found": 404,
    "already_published": 409,
    "already_processing": 409,
    "retry_limit_reached": 409,
    "non_retryable_failure": 409,
    "not_failed": 400,
    "invalid_destination": 400,
}


@community_centre_bp.post("/api/admin/community/posts/<post_id>/retry")
def cc_retry_post(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    body = request.get_json(silent=True) or {}
    post, code = retry_post(oid, actor_id=_actor_id(payload), force=bool(body.get("force")))
    if code:
        return _err(code, _RETRY_ERROR_STATUS.get(code, 400))
    return _ok({"post": _serialize_post_for_admin(post)})


def _simple_action(fn):
    def handler(post_id):
        payload, err = _require_admin()
        if err:
            return err
        oid, err_resp = _resolve_post_id(post_id)
        if err_resp:
            return err_resp
        post, code = fn(oid, actor_id=_actor_id(payload))
        if code:
            status = 404 if code == "not_found" else 409 if code in ("stale_edit",) else 400
            return _err(code, status)
        return _ok({"post": post})
    return handler


def _register_action(name: str, path_suffix: str, fn):
    handler = _simple_action(fn)
    handler.__name__ = f"cc_action_{name}"
    community_centre_bp.add_url_rule(
        f"/api/admin/community/posts/<post_id>/{path_suffix}",
        endpoint=f"cc_action_{name}",
        view_func=handler,
        methods=["POST"],
    )


_register_action("submit_approval", "submit-approval", lambda oid, actor_id: submit_for_approval(oid, actor_id=actor_id))
_register_action("approve", "approve", lambda oid, actor_id: approve_post(oid, actor_id=actor_id))
_register_action("publish_now", "publish-now", lambda oid, actor_id: publish_now(oid, actor_id=actor_id))
_register_action("duplicate", "duplicate", lambda oid, actor_id: duplicate_post(oid, actor_id=actor_id))
_register_action("cancel", "cancel", lambda oid, actor_id: cancel_post(oid, actor_id=actor_id))
_register_action("stop_poll", "stop-poll", lambda oid, actor_id: stop_poll_action(oid, actor_id=actor_id))
_register_action("pin", "pin", lambda oid, actor_id: pin_action(oid, actor_id=actor_id))
_register_action("unpin", "unpin", lambda oid, actor_id: unpin_action(oid, actor_id=actor_id))


@community_centre_bp.post("/api/admin/community/posts/<post_id>/reject")
def cc_reject(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    body = request.get_json(silent=True) or {}
    post, code = reject_post(oid, actor_id=_actor_id(payload), reason=str(body.get("reason") or ""))
    if code:
        return _err(code, 404 if code == "not_found" else 400)
    return _ok({"post": post})


@community_centre_bp.post("/api/admin/community/posts/<post_id>/schedule")
def cc_schedule(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    body = request.get_json(silent=True) or {}
    scheduled_at = _parse_utc_datetime(body.get("scheduled_at"))
    if scheduled_at is None:
        return _err("bad_scheduled_at", 400)
    post, code = schedule_post(oid, actor_id=_actor_id(payload), scheduled_at_utc=scheduled_at)
    if code:
        return _err(code, 404 if code == "not_found" else 400)
    return _ok({"post": post})


@community_centre_bp.post("/api/admin/community/posts/<post_id>/reschedule")
def cc_reschedule(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    body = request.get_json(silent=True) or {}
    scheduled_at = _parse_utc_datetime(body.get("scheduled_at"))
    if scheduled_at is None:
        return _err("bad_scheduled_at", 400)
    post, code = reschedule_post(oid, actor_id=_actor_id(payload), scheduled_at_utc=scheduled_at)
    if code:
        return _err(code, 404 if code == "not_found" else 400)
    return _ok({"post": post})


@community_centre_bp.get("/api/admin/community/calendar")
def cc_calendar():
    payload, err = _require_admin()
    if err:
        return err
    start = _parse_utc_datetime(request.args.get("start")) or (now_utc() - timedelta(days=1))
    end = _parse_utc_datetime(request.args.get("end")) or (now_utc() + timedelta(days=31))
    return _ok({"entries": calendar_entries(start, end)})


@community_centre_bp.get("/api/admin/community/polls")
def cc_polls():
    payload, err = _require_admin()
    if err:
        return err
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        skip = max(int(request.args.get("skip", 0)), 0)
    except ValueError:
        return _err("bad_pagination", 400)
    query: dict[str, Any] = {"content_type": {"$in": ["poll", "quiz"]}}
    poll_status = request.args.get("poll_status")
    if poll_status:
        query["poll_status"] = poll_status
    posts = list(_posts().find(query, sort=[("updated_at", -1)], limit=limit, skip=skip))
    return _ok({"posts": posts})


@community_centre_bp.get("/api/admin/community/polls/<post_id>/results")
def cc_poll_results(post_id):
    payload, err = _require_admin()
    if err:
        return err
    oid, err_resp = _resolve_post_id(post_id)
    if err_resp:
        return err_resp
    results = poll_results(oid)
    if results is None:
        return _err("not_found", 404)
    return _ok({"results": results})
