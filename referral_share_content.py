"""Referral Centre — Share Content.

Assembles the bot's Copy/Share caption from up to four parts (see
``build_referral_share_caption`` for the exact template):
  {random_active_hook}               <- omitted entirely if no hook is active
  {selected_active_playback_url}     <- omitted entirely if no playback is active

  Want more replays like this—and rewards too?   <- omitted if neither above is present

  👋 Welcome to AdvantPlay Community!
  Join our channel to get 👇
  🎟️ FREE Welcome Voucher — No deposit required
  ...benefits...   (always present — static)

  Start here 👇
  {user_canonical_invite_link}       <- always present

The benefits block + referral link are the only parts guaranteed to always
render: with no active hook and no active playback link, the caption is
still valid — never empty, never a lone separator, never "None". Missing
hook/playback is never silently papered over with substitute text; the
section is simply left out.

Collections: ``caption_hooks``, ``playback_pool``, ``share_generations``.

Playback links are share-content assets only — they are never written to
``invite_link_map`` and never participate in join attribution. The canonical
invite link is always obtained via the existing
``main.get_or_create_referral_invite_link_sync`` (imported lazily below to
avoid a circular import with ``main``); this module never creates or
substitutes a different link.
"""

from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timezone
from html import escape as html_escape
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

import database

logger = logging.getLogger(__name__)

referral_share_content_bp = Blueprint("referral_share_content", __name__)

PLAYBACK_HOST = "rx.apreplay.com"
PLAYBACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{5,100}$")
MAX_HOOK_TEXT_LEN = 500
MAX_GAME_NAME_LEN = 200
MAX_BULK_IMPORT_LINES = 2000
DEFAULT_FALLBACK_HOOK_TEXT = "🎬 Fresh replays just dropped!"


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    # Each index is created via database.safe_create_index so one failure
    # (e.g. pre-existing duplicate data blocking a unique index build) logs
    # and moves on instead of a single raised exception silently skipping
    # every index after it — the outer try/except below only guards against
    # database.db not being initialized yet (e.g. at import time in tests).
    try:
        database.safe_create_index(
            database.db["caption_hooks"], [("status", 1)], name="ix_caption_hooks_status"
        )

        playback_col = database.db["playback_pool"]
        database.safe_create_index(
            playback_col, [("playback_id", 1)], name="ux_playback_pool_playback_id", unique=True
        )
        database.safe_create_index(
            playback_col, [("playback_url", 1)], name="ux_playback_pool_playback_url", unique=True
        )
        database.safe_create_index(
            playback_col,
            [("status", 1), ("times_selected", 1), ("last_selected_at", 1)],
            name="ix_playback_pool_selection",
        )

        database.safe_create_index(
            database.db["share_generations"],
            [("user_id", 1), ("generated_at", -1)],
            name="ix_share_generations_user_generated",
        )
    except Exception:
        logger.warning("[SHARE_CONTENT] index creation failed", exc_info=True)


_ensure_indexes()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Playback ID / URL canonicalization + validation
# ---------------------------------------------------------------------------

def validate_playback_id(raw) -> str | None:
    """Accept only ``[A-Za-z0-9_-]{5,100}``. Case is preserved."""
    if not isinstance(raw, str):
        return None
    if not PLAYBACK_ID_PATTERN.match(raw):
        return None
    return raw


def canonical_playback_url(playback_id: str) -> str:
    return f"https://{PLAYBACK_HOST}/{playback_id}"


def parse_playback_url(raw) -> str | None:
    """Accept either a bare ``playback_id`` or a full canonical URL
    (``https://rx.apreplay.com/{playback_id}``). Returns the validated
    ``playback_id`` (original case preserved) or ``None``.

    Rejects: non-https scheme, any other host/subdomain, user info, custom
    ports, query strings, fragments, and empty or additional path segments.
    """
    if not isinstance(raw, str):
        return None
    candidate = raw.strip()
    if not candidate:
        return None
    # urlparse silently drops embedded tab/newline/CR characters anywhere in
    # the string (browser-style leniency), which would otherwise let a URL
    # containing control/whitespace chars quietly resolve to a *different*,
    # seemingly-valid playback_id instead of being rejected. Reject any
    # whitespace/control character up front so both input forms (bare ID and
    # full URL) apply the exact same charset rule.
    if any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7f for ch in candidate):
        return None

    if "://" not in candidate:
        return validate_playback_id(candidate)

    parsed = urlparse(candidate)
    if parsed.scheme != "https":
        return None
    if parsed.username or parsed.password:
        return None
    try:
        # .port raises ValueError (not returns None) for a malformed port,
        # e.g. "https://rx.apreplay.com:bad/Abc123" — treat that as rejected
        # input rather than letting it 500 the create/update/bulk-import APIs.
        has_port = parsed.port is not None
    except ValueError:
        return None
    if has_port:
        return None
    if (parsed.hostname or "").lower() != PLAYBACK_HOST:
        return None
    if parsed.query or parsed.fragment:
        return None
    path = parsed.path or ""
    if not path.startswith("/"):
        return None
    segments = path[1:].split("/")
    if len(segments) != 1 or not segments[0]:
        return None
    return validate_playback_id(segments[0])


# ---------------------------------------------------------------------------
# Fallback hook copy (Settings -> Share Content -> Fallback Hook Text)
# ---------------------------------------------------------------------------

def _default_hook_text() -> str:
    """Read the admin-configurable ``share_content.fallback_hook_text`` setting.

    NOT called anywhere in the normal generation path anymore.
    Historically ``generate_share_package`` called this to substitute a
    hook line whenever no caption hook was active, but that silently
    replaced "no active hook" with placeholder marketing copy — which is
    exactly the empty-state bug this module now guards against ("no active
    hook" must omit the hook section, not fabricate one). Kept only as a
    read accessor for the setting value (e.g. for an admin preview UI);
    it is not, by itself, empty-state protection — see
    ``build_referral_share_caption``.
    """
    try:
        from settings_service import get_setting

        value = get_setting("share_content", "fallback_hook_text")
        if value:
            return value
    except Exception:
        logger.warning("[SHARE_CONTENT] failed to read fallback_hook_text setting", exc_info=True)
    return DEFAULT_FALLBACK_HOOK_TEXT


# ---------------------------------------------------------------------------
# Selection — atomic, concurrency-safe
# ---------------------------------------------------------------------------

def select_hook(now: datetime | None = None) -> dict | None:
    """Pick a random active hook and atomically bump its usage counter.

    Selection itself has no fairness/no-repeat requirement (hooks may repeat
    freely), so a plain random pick among active `_id`s followed by a
    single-document `$inc`/`$set` by `_id` is sufficient — the counter
    update, not the pick, is what must be atomic, and a single-document
    `find_one_and_update` is always atomic in MongoDB.
    """
    now = now or now_utc()
    active_ids = [
        d["_id"] for d in database.db["caption_hooks"].find({"status": "active"}, projection={"_id": 1})
    ]
    if not active_ids:
        return None
    doc_id = random.choice(active_ids)
    return database.db["caption_hooks"].find_one_and_update(
        {"_id": doc_id},
        {"$inc": {"times_selected": 1}, "$set": {"last_selected_at": now}},
        return_document=ReturnDocument.AFTER,
    )


def _last_playback_record_id_for_user(user_id: int):
    last = database.db["share_generations"].find_one(
        {"user_id": user_id},
        sort=[("generated_at", -1)],
        projection={"playback_record_id": 1},
    )
    return (last or {}).get("playback_record_id")


def select_playback_for_user(user_id: int, now: datetime | None = None) -> dict | None:
    """Atomically claim the least-used active playback record, excluding the
    user's most recently generated one when an alternative exists.

    A single `find_one_and_update` performs the filter, the least-used +
    tie-break sort, and the `$inc`/`$set` mutation as one atomic MongoDB
    operation — there is no read-then-write window for two concurrent
    workers to race into (see `docs` / Phase 1 report for the full
    concurrency analysis). Never a separate find() followed by update().
    """
    now = now or now_utc()
    last_id = _last_playback_record_id_for_user(user_id)
    sort_order = [("times_selected", 1), ("last_selected_at", 1)]
    update = {"$inc": {"times_selected": 1}, "$set": {"last_selected_at": now}}

    if last_id is not None:
        doc = database.db["playback_pool"].find_one_and_update(
            {"status": "active", "_id": {"$ne": last_id}},
            update,
            sort=sort_order,
            return_document=ReturnDocument.AFTER,
        )
        if doc:
            return doc
        # Only one active record exists (or it *is* the excluded one) —
        # allow reuse rather than returning nothing.

    return database.db["playback_pool"].find_one_and_update(
        {"status": "active"},
        update,
        sort=sort_order,
        return_document=ReturnDocument.AFTER,
    )


# ---------------------------------------------------------------------------
# Shared caption template — the single source of truth for the referral
# share caption's text. Every active surface (bot deep-link reply, Telegram
# share-button prefill, Mini App copy/share) must render its caption through
# this function so they can never drift out of format with each other.
# ---------------------------------------------------------------------------

_FORMAT_MODES = ("plain", "telegram_html")


def build_referral_share_caption(
    *,
    hook_text: str | None,
    playback_url: str | None,
    referral_url: str | None,
    include_referral_link: bool = True,
    format_mode: str = "plain",
) -> str:
    """Assemble the referral-share caption from its parts.

    ``hook_text`` and ``playback_url`` are expected to already be the result
    of the existing pool selection (``select_hook`` / ``select_playback_for_user``)
    — this function does not pick or filter them, it only renders the text.
    Pass ``None``/``""`` for either one when there is no active hook and/or
    no active playback link; that section is then **omitted entirely**
    (no placeholder text, no orphan blank line, no stray separator). The
    static benefits block and the referral link always render, so the
    caption is never empty even when both are missing.

    ``referral_url`` is the user's canonical Telegram invite link and is
    always required: a caption is never built (or shared) with an empty
    referral URL.

    Set ``include_referral_link=False`` to render everything except the
    trailing link line, for surfaces (Telegram's ``share/url`` button) that
    pass the link via a separate ``url`` query param — including it in both
    places would show the link twice in the share sheet.

    ``format_mode`` selects the output rendering:
      - ``"plain"`` (default): plain text, for surfaces that cannot carry
        Telegram message entities (clipboard copy, the Mini App / API
        ``share_text``, Telegram's prefilled share-sheet text).
      - ``"telegram_html"``: HTML for bot-sent messages using
        ``parse_mode="HTML"``. Only the AdvantPlay benefits section is
        wrapped in ``<blockquote>`` — the hook, playback link, referral CTA,
        and referral URL are never inside the quote block. All dynamic
        values are HTML-escaped; the static ``<blockquote>``/``<b>`` tags
        are not.
    """
    if format_mode not in _FORMAT_MODES:
        raise ValueError(f"build_referral_share_caption: unsupported format_mode {format_mode!r}")

    referral_url = (referral_url or "").strip()
    if not referral_url:
        raise ValueError("build_referral_share_caption requires a non-empty referral_url")

    hook = (hook_text or "").strip()
    playback_url = (playback_url or "").strip()

    if format_mode == "telegram_html":
        hook_out = html_escape(hook) if hook else ""
        playback_out = html_escape(playback_url) if playback_url else ""
        referral_out = html_escape(referral_url)
        benefits = (
            "<blockquote><b>👋 Welcome to AdvantPlay Community!</b>\n"
            "Join our channel to get 👇\n\n"
            "🎟️ FREE Welcome Voucher — No deposit required\n"
            "⚡️ Daily voucher drops\n"
            "🎁 Bonus campaigns\n"
            "👑 VIP-only announcements\n"
            "🏆 Weekly ranking rewards</blockquote>"
        )
    else:
        hook_out = hook
        playback_out = playback_url
        referral_out = referral_url
        benefits = (
            "👋 Welcome to AdvantPlay Community!\n\n"
            "Join our channel to get 👇\n\n"
            "🎟️ FREE Welcome Voucher — No deposit required\n"
            "⚡️ Daily voucher drops\n"
            "🎁 Bonus campaigns\n"
            "👑 VIP-only announcements\n"
            "🏆 Weekly ranking rewards"
        )

    # Hook and playback are each optional and independently omitted. The
    # "Want more replays..." transition only makes sense when there is
    # something above it to transition from, so it's skipped too when both
    # are absent -- otherwise the caption would start with an orphan blank
    # line / dangling question with nothing above it.
    hook_and_playback = [line for line in (hook_out, playback_out) if line]

    lines = list(hook_and_playback)
    if hook_and_playback:
        lines.extend(["", "Want more replays like this—and rewards too?", ""])
    lines.append(benefits)
    lines.extend(["", "Start here 👇"])
    if include_referral_link:
        lines.append(referral_out)

    return "\n".join(lines)


def generate_share_package(
    user_id: int,
    username: str = "",
    *,
    generated_by: str = "bot",
    requested_by_admin: int | None = None,
) -> dict:
    """Assemble the share package for ``user_id``.

    The hook and the playback link are each optional: when either pool has
    no active record, that part of the package is simply omitted (``None``)
    rather than substituted or treated as a hard failure — the referral
    link is always generated and the caption always renders (static
    benefits + link, at minimum; see ``build_referral_share_caption``).
    Only a canonical-invite-link failure is a hard failure, since a caption
    is never sent without one. Writes ``share_generations`` only after that
    invite link is obtained. See module docstring + Phase 1 report for the
    full failure/rollback behaviour discussion.
    """
    now = now_utc()

    hook_doc = select_hook(now)
    hook_id = hook_doc["_id"] if hook_doc else None
    hook_text = hook_doc["text"] if hook_doc else None

    playback_doc = select_playback_for_user(user_id, now)
    playback_url = playback_doc["playback_url"] if playback_doc else None

    try:
        from main import get_or_create_referral_invite_link_sync

        invite_link = get_or_create_referral_invite_link_sync(user_id, username)
    except Exception as exc:
        # The playback (and possibly hook) usage counter has already been
        # incremented at this point and is NOT rolled back — this codebase
        # has no multi-document transactions anywhere (single-document
        # find_one_and_update atomicity is the established idiom), and
        # introducing the only transaction in the system to undo an
        # occasional counter over-count is not worth the added complexity.
        # The discrepancy is logged explicitly for visibility/audit.
        logger.error(
            "[SHARE_CONTENT][GENERATE_FAIL] reason=invite_link_failed user_id=%s "
            "playback_record_id=%s playback_counter_incremented=%s hook_counter_incremented=%s error=%s",
            user_id,
            playback_doc["_id"] if playback_doc else None,
            playback_doc is not None,
            hook_id is not None,
            exc,
        )
        return {"ok": False, "code": "invite_link_failed"}

    message = build_referral_share_caption(
        hook_text=hook_text,
        playback_url=playback_url,
        referral_url=invite_link,
    )

    doc = {
        "user_id": user_id,
        "hook_id": hook_id,
        "hook_text": hook_text,
        "playback_record_id": playback_doc["_id"] if playback_doc else None,
        "playback_id": playback_doc["playback_id"] if playback_doc else None,
        "playback_url": playback_url,
        "invite_link": invite_link,
        "generated_at": now,
        "generated_by": generated_by,
        "requested_by_admin": requested_by_admin,
    }
    database.db["share_generations"].insert_one(doc)
    logger.info(
        "[SHARE_CONTENT][GENERATE_OK] user_id=%s playback_record_id=%s hook_id=%s",
        user_id,
        playback_doc["_id"] if playback_doc else None,
        hook_id,
    )
    return {
        "ok": True,
        "message": message,
        "invite_link": invite_link,
        "playback_url": playback_url,
        "hook_text": hook_text,
    }


# ---------------------------------------------------------------------------
# Bulk import
# ---------------------------------------------------------------------------

def bulk_import_hooks(lines_blob: str, *, created_by: int | None = None) -> dict:
    candidates = [line.strip() for line in (lines_blob or "").splitlines()]
    candidates = [line for line in candidates if line]

    seen_in_batch: set[str] = set()
    results = []
    inserted = skipped = rejected = 0
    now = now_utc()

    for line in candidates:
        if len(line) > MAX_HOOK_TEXT_LEN:
            results.append({"line": line, "status": "rejected", "reason": "too_long"})
            rejected += 1
            continue
        if line in seen_in_batch:
            results.append({"line": line, "status": "skipped", "reason": "duplicate_in_batch"})
            skipped += 1
            continue
        if database.db["caption_hooks"].find_one({"text": line}):
            results.append({"line": line, "status": "skipped", "reason": "duplicate_existing"})
            skipped += 1
            continue
        seen_in_batch.add(line)
        database.db["caption_hooks"].insert_one({
            "text": line,
            "status": "active",
            "times_selected": 0,
            "last_selected_at": None,
            "created_at": now,
            "updated_at": now,
            "created_by": created_by,
        })
        results.append({"line": line, "status": "inserted"})
        inserted += 1

    return {"inserted": inserted, "skipped": skipped, "rejected": rejected, "results": results}


def bulk_import_playback(lines_blob: str, *, created_by: int | None = None) -> dict:
    candidates = [line.strip() for line in (lines_blob or "").splitlines()]
    candidates = [line for line in candidates if line]

    seen_ids_in_batch: set[str] = set()
    seen_urls_in_batch: set[str] = set()
    results = []
    inserted = skipped = rejected = 0
    now = now_utc()

    for line in candidates:
        playback_id = parse_playback_url(line)
        if not playback_id:
            results.append({"line": line, "status": "rejected", "reason": "invalid_format"})
            rejected += 1
            continue
        url = canonical_playback_url(playback_id)
        if playback_id in seen_ids_in_batch or url in seen_urls_in_batch:
            results.append({"line": line, "status": "skipped", "reason": "duplicate_in_batch"})
            skipped += 1
            continue
        if database.db["playback_pool"].find_one({"$or": [{"playback_id": playback_id}, {"playback_url": url}]}):
            results.append({"line": line, "status": "skipped", "reason": "duplicate_existing"})
            skipped += 1
            continue
        seen_ids_in_batch.add(playback_id)
        seen_urls_in_batch.add(url)
        try:
            database.db["playback_pool"].insert_one({
                "playback_id": playback_id,
                "playback_url": url,
                "game_name": "",
                "status": "active",
                "times_selected": 0,
                "last_selected_at": None,
                "created_at": now,
                "updated_at": now,
                "created_by": created_by,
            })
            results.append({"line": line, "status": "inserted"})
            inserted += 1
        except DuplicateKeyError:
            results.append({"line": line, "status": "skipped", "reason": "duplicate_existing"})
            skipped += 1

    return {"inserted": inserted, "skipped": skipped, "rejected": rejected, "results": results}


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def _serialize(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for k in ("created_at", "updated_at", "last_selected_at"):
        if out.get(k):
            out[k] = out[k].isoformat()
    return out


def _parse_object_id(raw: str) -> ObjectId | None:
    try:
        return ObjectId(raw)
    except (InvalidId, TypeError):
        return None


# ---------------------------------------------------------------------------
# Admin API — Caption Hooks
# ---------------------------------------------------------------------------

@referral_share_content_bp.get("/api/admin/referral/share-content/hooks")
def list_hooks():
    _, err = _require_admin()
    if err:
        return err
    status = request.args.get("status")
    q = (request.args.get("q") or "").strip()
    filt: dict = {}
    if status in ("active", "inactive"):
        filt["status"] = status
    if q:
        filt["text"] = {"$regex": re.escape(q), "$options": "i"}
    docs = list(database.db["caption_hooks"].find(filt, sort=[("created_at", -1)], limit=500))
    return jsonify({"status": "ok", "hooks": [_serialize(d) for d in docs]})


@referral_share_content_bp.post("/api/admin/referral/share-content/hooks")
def create_hook():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    text = (body.get("text") or "").strip()
    if not text:
        return jsonify({"status": "error", "code": "missing_text"}), 400
    if len(text) > MAX_HOOK_TEXT_LEN:
        return jsonify({"status": "error", "code": "text_too_long"}), 400
    status_val = body.get("status") if body.get("status") in ("active", "inactive") else "active"
    now = now_utc()
    doc = {
        "text": text,
        "status": status_val,
        "times_selected": 0,
        "last_selected_at": None,
        "created_at": now,
        "updated_at": now,
        "created_by": (admin or {}).get("id"),
    }
    result = database.db["caption_hooks"].insert_one(doc)
    return jsonify({"status": "ok", "id": str(result.inserted_id)}), 201


@referral_share_content_bp.put("/api/admin/referral/share-content/hooks/<hook_id>")
def update_hook(hook_id: str):
    admin, err = _require_admin()
    if err:
        return err
    oid = _parse_object_id(hook_id)
    if not oid:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    doc = database.db["caption_hooks"].find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(force=True, silent=True) or {}
    updates: dict = {}
    if "text" in body:
        text = (body.get("text") or "").strip()
        if not text:
            return jsonify({"status": "error", "code": "missing_text"}), 400
        if len(text) > MAX_HOOK_TEXT_LEN:
            return jsonify({"status": "error", "code": "text_too_long"}), 400
        updates["text"] = text
    if "status" in body and body["status"] in ("active", "inactive"):
        updates["status"] = body["status"]
    if not updates:
        return jsonify({"status": "error", "code": "no_changes"}), 400
    updates["updated_at"] = now_utc()
    database.db["caption_hooks"].update_one({"_id": oid}, {"$set": updates})
    return jsonify({"status": "ok"})


@referral_share_content_bp.post("/api/admin/referral/share-content/hooks/<hook_id>/activate")
def activate_hook(hook_id: str):
    return _set_hook_status(hook_id, "active")


@referral_share_content_bp.post("/api/admin/referral/share-content/hooks/<hook_id>/deactivate")
def deactivate_hook(hook_id: str):
    return _set_hook_status(hook_id, "inactive")


def _set_hook_status(hook_id: str, status_val: str):
    _, err = _require_admin()
    if err:
        return err
    oid = _parse_object_id(hook_id)
    if not oid:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result = database.db["caption_hooks"].update_one(
        {"_id": oid}, {"$set": {"status": status_val, "updated_at": now_utc()}}
    )
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


@referral_share_content_bp.delete("/api/admin/referral/share-content/hooks/<hook_id>")
def delete_hook(hook_id: str):
    _, err = _require_admin()
    if err:
        return err
    oid = _parse_object_id(hook_id)
    if not oid:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result = database.db["caption_hooks"].delete_one({"_id": oid})
    if result.deleted_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


def _bulk_import_line_count(blob: str) -> int:
    return sum(1 for line in (blob or "").splitlines() if line.strip())


@referral_share_content_bp.post("/api/admin/referral/share-content/hooks/bulk-import")
def bulk_import_hooks_route():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    blob = body.get("lines") or ""
    if _bulk_import_line_count(blob) > MAX_BULK_IMPORT_LINES:
        return jsonify({"status": "error", "code": "too_many_lines", "max_lines": MAX_BULK_IMPORT_LINES}), 400
    result = bulk_import_hooks(blob, created_by=(admin or {}).get("id"))
    return jsonify({"status": "ok", **result})


# ---------------------------------------------------------------------------
# Admin API — Playback Pool
# ---------------------------------------------------------------------------

@referral_share_content_bp.get("/api/admin/referral/share-content/playback")
def list_playback():
    _, err = _require_admin()
    if err:
        return err
    status = request.args.get("status")
    q = (request.args.get("q") or "").strip()
    filt: dict = {}
    if status in ("active", "inactive"):
        filt["status"] = status
    if q:
        rx = {"$regex": re.escape(q), "$options": "i"}
        filt["$or"] = [{"playback_id": rx}, {"playback_url": rx}, {"game_name": rx}]
    docs = list(database.db["playback_pool"].find(filt, sort=[("created_at", -1)], limit=500))
    return jsonify({"status": "ok", "playback": [_serialize(d) for d in docs]})


@referral_share_content_bp.post("/api/admin/referral/share-content/playback")
def create_playback():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    raw = (body.get("url") or body.get("playback_id") or "").strip()
    playback_id = parse_playback_url(raw)
    if not playback_id:
        return jsonify({"status": "error", "code": "invalid_playback_url"}), 400
    url = canonical_playback_url(playback_id)
    if database.db["playback_pool"].find_one({"$or": [{"playback_id": playback_id}, {"playback_url": url}]}):
        return jsonify({"status": "error", "code": "duplicate_playback"}), 409
    game_name = (body.get("game_name") or "").strip()
    if len(game_name) > MAX_GAME_NAME_LEN:
        return jsonify({"status": "error", "code": "game_name_too_long"}), 400
    status_val = body.get("status") if body.get("status") in ("active", "inactive") else "active"
    now = now_utc()
    doc = {
        "playback_id": playback_id,
        "playback_url": url,
        "game_name": game_name,
        "status": status_val,
        "times_selected": 0,
        "last_selected_at": None,
        "created_at": now,
        "updated_at": now,
        "created_by": (admin or {}).get("id"),
    }
    try:
        result = database.db["playback_pool"].insert_one(doc)
    except DuplicateKeyError:
        return jsonify({"status": "error", "code": "duplicate_playback"}), 409
    return jsonify({"status": "ok", "id": str(result.inserted_id)}), 201


@referral_share_content_bp.put("/api/admin/referral/share-content/playback/<playback_id>")
def update_playback(playback_id: str):
    admin, err = _require_admin()
    if err:
        return err
    oid = _parse_object_id(playback_id)
    if not oid:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    doc = database.db["playback_pool"].find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(force=True, silent=True) or {}
    updates: dict = {}
    if "url" in body or "playback_id" in body:
        raw = (body.get("url") or body.get("playback_id") or "").strip()
        new_playback_id = parse_playback_url(raw)
        if not new_playback_id:
            return jsonify({"status": "error", "code": "invalid_playback_url"}), 400
        new_url = canonical_playback_url(new_playback_id)
        conflict = database.db["playback_pool"].find_one({
            "_id": {"$ne": oid},
            "$or": [{"playback_id": new_playback_id}, {"playback_url": new_url}],
        })
        if conflict:
            return jsonify({"status": "error", "code": "duplicate_playback"}), 409
        updates["playback_id"] = new_playback_id
        updates["playback_url"] = new_url
    if "game_name" in body:
        new_game_name = (body.get("game_name") or "").strip()
        if len(new_game_name) > MAX_GAME_NAME_LEN:
            return jsonify({"status": "error", "code": "game_name_too_long"}), 400
        updates["game_name"] = new_game_name
    if "status" in body and body["status"] in ("active", "inactive"):
        updates["status"] = body["status"]
    if not updates:
        return jsonify({"status": "error", "code": "no_changes"}), 400
    updates["updated_at"] = now_utc()
    try:
        database.db["playback_pool"].update_one({"_id": oid}, {"$set": updates})
    except DuplicateKeyError:
        # The pre-check above has a race window between two concurrent
        # renames to the same playback_id/URL; the unique index is the real
        # guard, so a rare race that slips past the pre-check must still
        # surface as a clean 409, not an unhandled 500.
        return jsonify({"status": "error", "code": "duplicate_playback"}), 409
    return jsonify({"status": "ok"})


@referral_share_content_bp.post("/api/admin/referral/share-content/playback/<playback_id>/activate")
def activate_playback(playback_id: str):
    return _set_playback_status(playback_id, "active")


@referral_share_content_bp.post("/api/admin/referral/share-content/playback/<playback_id>/deactivate")
def deactivate_playback(playback_id: str):
    return _set_playback_status(playback_id, "inactive")


def _set_playback_status(playback_id: str, status_val: str):
    _, err = _require_admin()
    if err:
        return err
    oid = _parse_object_id(playback_id)
    if not oid:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result = database.db["playback_pool"].update_one(
        {"_id": oid}, {"$set": {"status": status_val, "updated_at": now_utc()}}
    )
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


@referral_share_content_bp.delete("/api/admin/referral/share-content/playback/<playback_id>")
def delete_playback(playback_id: str):
    _, err = _require_admin()
    if err:
        return err
    oid = _parse_object_id(playback_id)
    if not oid:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    result = database.db["playback_pool"].delete_one({"_id": oid})
    if result.deleted_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


@referral_share_content_bp.post("/api/admin/referral/share-content/playback/bulk-import")
def bulk_import_playback_route():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    blob = body.get("lines") or ""
    if _bulk_import_line_count(blob) > MAX_BULK_IMPORT_LINES:
        return jsonify({"status": "error", "code": "too_many_lines", "max_lines": MAX_BULK_IMPORT_LINES}), 400
    result = bulk_import_playback(blob, created_by=(admin or {}).get("id"))
    return jsonify({"status": "ok", **result})
