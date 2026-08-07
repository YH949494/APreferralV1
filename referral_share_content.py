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
import secrets
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

# Analytics-metadata-only source labels written to share_generations.generated_by.
# Purely descriptive of which surface generated the package -- never read by
# referral attribution, qualification, settlement, reward, or abuse logic
# (those all key off pending_referrals / invite_link_map, not this field).
MINIAPP_SHARE_SOURCE = "miniapp_general_share"
CREATOR_SHARE_SOURCE = "creator_generated_share"

# Creator Share Centre's fixed, compressed value-proposition block: three
# highest-priority benefits only (never the Mini App's full five-benefit
# block) -- see build_creator_share_text().
CREATOR_SHARE_TRANSITION_LINE = "Want more replays like this—and rewards too?"
CREATOR_SHARE_BENEFIT_LINES = (
    "🎟️ Free welcome voucher",
    "⚡️ Daily voucher drops",
    "🏆 Weekly rewards",
)

# Admin bulk-management (Hooks / Playback Links) — resource_type is always
# whitelisted against this map; the client can never supply a raw collection
# name. Hooks and playback links are always acted on independently: every
# bulk route below is scoped to exactly one of these two collections.
RESOURCE_COLLECTIONS = {"hook": "caption_hooks", "playback_link": "playback_pool"}
BULK_ACTIONS = ("activate_all", "deactivate_all", "delete_selected")


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


def build_creator_share_text(
    *, hook_text: str | None, playback_url: str | None, referral_link: str | None
) -> str:
    """Assemble the Creator Share Centre's copy-ready plain text.

    Fixed structure::

        {hook_text}
        {playback_url}

        Want more replays like this—and rewards too?
        Join AdvantPlay for:
        🎟️ Free welcome voucher
        ⚡️ Daily voucher drops
        🏆 Weekly rewards

        Start here 👇
        {canonical_referral_link}

    ``hook_text``/``playback_url`` are each independently omitted (no
    "None", no orphan blank line) when absent, mirroring
    ``build_referral_share_caption``'s empty-state handling. The transition
    line, the three fixed benefits, and the referral link always render, so
    a post is never a bare link with no value proposition.

    This is intentionally the *compressed* three-benefit block (voucher /
    daily drops / weekly rewards), not the Mini App's full five-benefit
    block (``build_referral_share_caption``) -- the two surfaces must never
    share the same benefits list. ``referral_link`` is required.
    """
    referral_link = (referral_link or "").strip()
    if not referral_link:
        raise ValueError("build_creator_share_text requires a non-empty referral_link")

    hook = (hook_text or "").strip()
    playback = (playback_url or "").strip()
    top = [line for line in (hook, playback) if line]

    lines = list(top)
    if top:
        lines.append("")
    lines.append(CREATOR_SHARE_TRANSITION_LINE)
    lines.append("Join AdvantPlay for:")
    lines.extend(CREATOR_SHARE_BENEFIT_LINES)
    lines.append("")
    lines.append("Start here 👇")
    lines.append(referral_link)
    return "\n".join(lines)


def generate_share_package(
    user_id: int,
    username: str = "",
    *,
    generated_by: str = "bot",
    requested_by_admin: int | None = None,
    platform: str | None = None,
    include_content_pools: bool = True,
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

    ``platform`` is an optional caller-supplied label (e.g. the Creator
    Share Centre's target platform) stored alongside the generation record;
    it never affects hook/playback selection or the invite link.

    Set ``include_content_pools=False`` for surfaces that must never draw
    from the caption-hook / playback-link pools at all (the Mini App's
    general referral caption) -- hook/playback are then always ``None``
    without ever calling ``select_hook``/``select_playback_for_user``, so a
    Mini App generation never consumes a Creator Share Centre pool record
    or perturbs its least-used rotation.
    """
    now = now_utc()

    if include_content_pools:
        hook_doc = select_hook(now)
        hook_id = hook_doc["_id"] if hook_doc else None
        hook_text = hook_doc["text"] if hook_doc else None

        playback_doc = select_playback_for_user(user_id, now)
        playback_url = playback_doc["playback_url"] if playback_doc else None
    else:
        hook_doc = None
        hook_id = None
        hook_text = None
        playback_doc = None
        playback_url = None

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

    playback_record_id = playback_doc["_id"] if playback_doc else None
    package_id = secrets.token_urlsafe(16)
    doc = {
        "user_id": user_id,
        "hook_id": hook_id,
        "hook_text": hook_text,
        "playback_record_id": playback_record_id,
        "playback_id": playback_doc["playback_id"] if playback_doc else None,
        "playback_url": playback_url,
        "invite_link": invite_link,
        "generated_at": now,
        "generated_by": generated_by,
        "requested_by_admin": requested_by_admin,
        "platform": platform,
        "package_id": package_id,
        "copied_at": None,
        "copy_count": 0,
        "share_clicked_at": None,
        "share_click_count": 0,
    }
    database.db["share_generations"].insert_one(doc)
    if include_content_pools and (hook_doc is None or playback_doc is None):
        # Admin-facing visibility only -- generation itself still succeeds
        # (see module docstring): an empty pool is never a hard failure, it
        # just omits that section of the caption. This is a deliberate
        # product decision, not an oversight: referral sharing must stay
        # available even with BOTH pools empty (still a valid post -- link
        # + benefits/CTA, never "undefined"/blank/malformed), because
        # disabling the whole Creator Centre over one missing optional
        # content component has more business impact than the post simply
        # omitting that component. Logged at WARNING, identifying each pool
        # independently, so an empty pool is easy to spot/alert on before it
        # drains completely -- without blocking generation.
        logger.warning(
            "[SHARE_CONTENT][POOL_EMPTY] user_id=%s hook_pool_empty=%s playback_pool_empty=%s package_id=%s",
            user_id, hook_doc is None, playback_doc is None, package_id,
        )
    logger.info(
        "[SHARE_CONTENT][GENERATE_OK] user_id=%s playback_record_id=%s hook_id=%s package_id=%s",
        user_id,
        playback_record_id,
        hook_id,
        package_id,
    )
    return {
        "ok": True,
        "message": message,
        "invite_link": invite_link,
        "playback_url": playback_url,
        "hook_text": hook_text,
        "hook_id": hook_id,
        "playback_record_id": playback_record_id,
        "package_id": package_id,
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
# Admin bulk-management — shared validation, deletion service, audit logging
# ---------------------------------------------------------------------------

def _collection_for(resource_type: str):
    name = RESOURCE_COLLECTIONS.get(resource_type)
    return database.db[name] if name else None


def _status_counts(collection) -> tuple[int, int]:
    """(active_count, total_count) for the given collection, independent of
    any list-endpoint pagination cap so the admin UI's "Active: X / Y"
    summary is always exact."""
    return collection.count_documents({"status": "active"}), collection.count_documents({})


def _audit_log(
    *, admin, resource_type: str, action: str, requested_ids=None, requested_count=None,
    matched_count: int = 0, result_count: int = 0, success: bool, reason: str | None = None,
) -> None:
    """Structured audit trail for every bulk/individual mutating admin action
    on hooks/playback links. Never logs tokens/credentials -- only the admin
    id, the action taken, and the ids/counts involved."""
    log_fn = logger.info if success else logger.warning
    log_fn(
        "[SHARE_CONTENT][ADMIN_ACTION] admin_id=%s resource_type=%s action=%s "
        "requested_count=%s requested_ids=%s matched_count=%s result_count=%s success=%s reason=%s ts=%s",
        (admin or {}).get("id"),
        resource_type,
        action,
        requested_count if requested_count is not None else len(requested_ids or []),
        requested_ids,
        matched_count,
        result_count,
        success,
        reason or "",
        now_utc().isoformat(),
    )


def _validate_selected_ids(collection, raw_ids) -> tuple[list[ObjectId] | None, str | None]:
    """Server-side validation of a client-supplied id selection. Never
    trusts frontend-reported ids/counts: every id is re-parsed and its
    existence in the *correct* (already resource-type-scoped) collection is
    re-checked here. Returns (parsed_ids, None) on success, or
    (None, error_code) on any of: empty selection, malformed id, duplicate
    id, or an id that doesn't exist in this collection (covers both unknown
    and cross-section ids, since the query is always scoped to one
    collection). No partial validation -- the first problem found aborts
    the whole request before any write happens.
    """
    if not isinstance(raw_ids, list) or not raw_ids:
        return None, "empty_selection"
    str_ids = []
    parsed: list[ObjectId] = []
    for raw in raw_ids:
        if not isinstance(raw, str):
            return None, "malformed_ids"
        str_ids.append(raw)
        oid = _parse_object_id(raw)
        if not oid:
            return None, "malformed_ids"
        parsed.append(oid)
    if len(set(str_ids)) != len(str_ids):
        return None, "duplicate_ids"
    existing = collection.count_documents({"_id": {"$in": parsed}})
    if existing != len(parsed):
        return None, "unknown_ids"
    return parsed, None


def delete_resource_ids(resource_type: str, raw_ids, *, admin) -> tuple[dict, int]:
    """Shared deletion service used by BOTH the bulk "delete selected" route
    and every individual delete route -- there is exactly one code path that
    ever calls delete_many on caption_hooks/playback_pool, so validation and
    deletion can never drift apart between the two surfaces.

    Performs one set-based delete_many (never a per-id loop). Any validation
    failure aborts before touching the database, so an invalid selection is
    never partially deleted.
    """
    collection = _collection_for(resource_type)
    requested_ids = [str(x) for x in raw_ids] if isinstance(raw_ids, list) else raw_ids
    parsed, error = _validate_selected_ids(collection, raw_ids)
    if error:
        _audit_log(
            admin=admin, resource_type=resource_type, action="delete_selected",
            requested_ids=requested_ids, matched_count=0, result_count=0,
            success=False, reason=error,
        )
        return {"status": "error", "code": error}, 400

    result = collection.delete_many({"_id": {"$in": parsed}})
    active_count, total_count = _status_counts(collection)
    _audit_log(
        admin=admin, resource_type=resource_type, action="delete_selected",
        requested_ids=requested_ids, matched_count=len(parsed), result_count=result.deleted_count,
        success=True,
    )
    return {
        "status": "ok",
        "matched_count": len(parsed),
        "deleted_count": result.deleted_count,
        "active_count": active_count,
        "total_count": total_count,
    }, 200


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
    active_count, total_count = _status_counts(database.db["caption_hooks"])
    return jsonify({
        "status": "ok", "hooks": [_serialize(d) for d in docs],
        "active_count": active_count, "total_count": total_count,
    })


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
    admin, err = _require_admin()
    if err:
        return err
    payload, status_code = delete_resource_ids("hook", [hook_id], admin=admin)
    if status_code != 200:
        # Preserve this route's pre-existing contract: a malformed id is a
        # 400 invalid_id, a well-formed but nonexistent id is a 404 not_found.
        code = payload.get("code")
        if code == "unknown_ids":
            return jsonify({"status": "error", "code": "not_found"}), 404
        if code == "malformed_ids":
            return jsonify({"status": "error", "code": "invalid_id"}), 400
        return jsonify(payload), status_code
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
    active_count, total_count = _status_counts(database.db["playback_pool"])
    return jsonify({
        "status": "ok", "playback": [_serialize(d) for d in docs],
        "active_count": active_count, "total_count": total_count,
    })


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
    admin, err = _require_admin()
    if err:
        return err
    payload, status_code = delete_resource_ids("playback_link", [playback_id], admin=admin)
    if status_code != 200:
        code = payload.get("code")
        if code == "unknown_ids":
            return jsonify({"status": "error", "code": "not_found"}), 404
        if code == "malformed_ids":
            return jsonify({"status": "error", "code": "invalid_id"}), 400
        return jsonify(payload), status_code
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


# ---------------------------------------------------------------------------
# Admin API — Bulk management (Hooks / Playback Links)
#
# One strictly-validated endpoint shared by both resource types. resource_type
# and action are each whitelisted against a fixed map/tuple -- the client
# never supplies (and this route never derives from client input) a raw
# collection name. Every operation is scoped to exactly one collection, so
# hooks and playback links can never be updated together in a single request.
# ---------------------------------------------------------------------------

@referral_share_content_bp.post("/api/admin/referral/share-content/bulk-action")
def share_content_bulk_action():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    resource_type = body.get("resource_type")
    action = body.get("action")

    if resource_type not in RESOURCE_COLLECTIONS:
        return jsonify({"status": "error", "code": "invalid_resource_type"}), 400
    if action not in BULK_ACTIONS:
        return jsonify({"status": "error", "code": "invalid_action"}), 400

    if action == "delete_selected":
        payload, status_code = delete_resource_ids(resource_type, body.get("selected_ids"), admin=admin)
        payload["resource_type"] = resource_type
        payload["action"] = action
        return jsonify(payload), status_code

    # activate_all / deactivate_all: a single set-based update_many scoped to
    # this resource type's collection only, and filtered to records that are
    # NOT already at the target status. Filtering the query itself (rather
    # than updating every document and relying on MongoDB's modified_count)
    # is what makes this idempotent: a record already at the target status
    # is never matched, so its updated_at is never touched, and a repeated
    # identical request always reports matched_count == modified_count == 0.
    collection = _collection_for(resource_type)
    target_status = "active" if action == "activate_all" else "inactive"
    total_count = collection.count_documents({})
    result = collection.update_many(
        {"status": {"$ne": target_status}}, {"$set": {"status": target_status, "updated_at": now_utc()}}
    )
    active_count, _total_count_after = _status_counts(collection)
    _audit_log(
        admin=admin, resource_type=resource_type, action=action,
        requested_count=total_count, matched_count=result.matched_count,
        result_count=result.modified_count, success=True,
    )
    return jsonify({
        "status": "ok",
        "resource_type": resource_type,
        "action": action,
        "total_count": total_count,
        "matched_count": result.matched_count,
        "eligible_count": result.matched_count,
        "modified_count": result.modified_count,
        "active_count": active_count,
    })
