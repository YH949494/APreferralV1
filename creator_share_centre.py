"""Creator Share Centre — creator-facing surface for the referral share package.

This module does NOT rebuild caption/pool/invite-link logic. It is a thin,
access-controlled wrapper around the existing ``referral_share_content``
module: hook/playback selection, the canonical invite link
(``main.get_or_create_referral_invite_link_sync``), and the
``share_generations`` collection are all reused as-is.

Adds:
  - ``creator_members`` collection + admin CRUD (approve/suspend/remove).
  - Creator-only Mini App APIs: status, generate, copied, share-clicked,
    results.
  - Telegram initData authentication + creator authorization, independent
    of admin auth (``vouchers.require_admin``) and independent of the
    general referral/XP system.

Access control: every ``/api/creator/...`` route requires (1) valid
Telegram Mini App initData, and (2) confirmed membership in the configured
Creator Access Chat (verified live, short-cached, against Telegram). A
``creator_members`` record is no longer a mandatory allowlist — it is an
override/profile collection: ``suspended``/``removed`` always denies (even
for a current chat member), an existing ``active`` record is honoured, and a
chat member with no record at all is granted access and gets one lazily
created (``status="active"``, ``approval_source="creator_access_chat_membership"``).
When membership verification is explicitly disabled by an admin
(``membership_check_enabled=false``), access falls back to requiring an
existing ``creator_members.status == "active"`` record — disabling
verification never opens access to users without a creator record. user_id
is always derived from verified initData; a ``user_id`` sent in the request
body/query is never trusted.
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

import database
from referral_rate_limit import consume_referral_rate_limits
from referral_share_content import (
    CREATOR_SHARE_SOURCE,
    build_creator_share_text,
    generate_share_package,
    now_utc,
)

# Legacy generated_by value written before the Mini App / Creator Share
# Centre content split -- kept only so historical share_generations
# documents stay visible in results()/creator_filter below; never written
# by new generations (see CREATOR_SHARE_SOURCE).
_LEGACY_CREATOR_SHARE_SOURCE = "creator_share_centre"

logger = logging.getLogger(__name__)

creator_share_bp = Blueprint("creator_share_centre", __name__)

ALLOWED_PLATFORMS = {"generic", "whatsapp", "facebook", "x", "telegram"}
ALLOWED_COPY_METHODS = {"auto", "manual"}
CREATOR_STATUSES = {"active", "suspended", "removed"}

GENERATE_HOURLY_LIMIT = 20
MAX_BULK_CREATOR_IMPORT = 2000


def _next_reward_tier(qualified_count: int) -> dict | None:
    """Returns the next unreached tier as {"qualified_needed", "reward_amount"},
    or None once the highest tier has been reached.

    ``qualified_count`` must be the same current-reward-month count
    ``scheduler.maybe_shout_referral_congrats`` evaluates against
    ``REFERRAL_CONGRATS_TIERS`` (see
    ``scheduler.current_month_qualified_referral_count``) -- reward totals
    reset monthly (per the "See Referral Rewards" card copy), so a lifetime
    count would let this promise progress the actual reward workflow
    already reset past.
    """
    from scheduler import REFERRAL_CONGRATS_TIERS

    for threshold, amount in REFERRAL_CONGRATS_TIERS:
        if qualified_count < threshold:
            return {"qualified_needed": threshold - qualified_count, "reward_amount": amount}
    return None

# Short cache period for a *confirmed* live membership check, and a shorter
# grace window used only when Telegram is temporarily unavailable (so a
# transient outage never immediately removes an otherwise-active creator —
# see module docstring + docs/creator-share-centre.md for the full policy).
MEMBERSHIP_VERIFY_CACHE_SEC = 900
MEMBERSHIP_UNRESOLVABLE_GRACE_SEC = 120

_TELEGRAM_MEMBER_STATUSES = {"member", "administrator", "creator"}
_TELEGRAM_NONMEMBER_STATUSES = {"left", "kicked"}

# ---------------------------------------------------------------------------
# Creator group access settings — reuses the existing app_settings collection
# (the same one settings_service.py writes to) via one canonical document,
# rather than reading os.environ directly. See get_creator_group_access_settings().
# ---------------------------------------------------------------------------

APP_SETTINGS_COLLECTION = "app_settings"
APP_SETTINGS_AUDIT_COLLECTION = "app_settings_audit"
CREATOR_GROUP_SETTINGS_KEY = "creator_group_access"

# Short TTL on the in-process settings read so a save on one process becomes
# visible to others (web/worker) without a redeploy, bounded by this window.
CREATOR_GROUP_SETTINGS_CACHE_TTL_SEC = 30

_settings_cache_lock = threading.Lock()
_settings_cache: dict = {"doc": None, "loaded_at": None}


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db["creator_members"]
        database.safe_create_index(col, [("user_id", 1)], name="ux_creator_members_user_id", unique=True)
        database.safe_create_index(col, [("status", 1)], name="ix_creator_members_status")
        database.safe_create_index(
            col, [("source_group_id", 1), ("status", 1)], name="ix_creator_members_group_status"
        )

        # Legacy (pre-Creator-Share-Centre) share_generations documents have
        # no package_id field at all; the partial filter keeps this index
        # from ever considering those documents, so they remain valid.
        database.safe_create_index(
            database.db["share_generations"],
            [("package_id", 1)],
            name="ux_share_generations_package_id",
            unique=True,
            partialFilterExpression={"package_id": {"$exists": True}},
        )

        rate_limit_col = database.db["creator_generation_rate_limits"]
        database.safe_create_index(
            rate_limit_col, [("key", 1)], name="ux_creator_gen_rate_limit_key", unique=True
        )
        # consume_referral_rate_limits() writes an "expireAt" on every bucket
        # (see referral_rate_limit.py) but never creates its own TTL index —
        # callers are expected to, same as the shared referral_rate_limits
        # collection does. Without this, every active creator accumulates up
        # to 24 hour-buckets/day forever.
        try:
            rate_limit_col.create_index([("expireAt", 1)], name="ttl_creator_gen_rate_limit", expireAfterSeconds=0)
        except Exception:
            logger.warning("[CREATOR_SHARE] rate limit TTL index creation failed", exc_info=True)
    except Exception:
        logger.warning("[CREATOR_SHARE] index creation failed", exc_info=True)


_ensure_indexes()


# ---------------------------------------------------------------------------
# get_creator_group_access_settings() — the single canonical reader. Creator
# access checks (and the admin settings API) must go through this, never
# os.environ directly, so the resolution order (DB setting -> env fallback ->
# unconfigured) and the short-TTL cross-process refresh stay in one place.
# ---------------------------------------------------------------------------

def _load_creator_group_doc(*, force_refresh: bool = False) -> dict | None:
    if not force_refresh:
        with _settings_cache_lock:
            loaded_at = _settings_cache["loaded_at"]
            if loaded_at is not None and (time.monotonic() - loaded_at) < CREATOR_GROUP_SETTINGS_CACHE_TTL_SEC:
                return _settings_cache["doc"]
    try:
        doc = database.db[APP_SETTINGS_COLLECTION].find_one({"_id": CREATOR_GROUP_SETTINGS_KEY})
    except Exception:
        logger.warning("[CREATOR_SHARE] failed to load creator_group_access setting", exc_info=True)
        doc = None
    with _settings_cache_lock:
        _settings_cache["doc"] = doc
        _settings_cache["loaded_at"] = time.monotonic()
    return doc


def invalidate_creator_group_settings_cache() -> None:
    """Called on every settings save. Also the mechanism by which a changed
    group takes effect quickly across the web/worker processes that share
    this Mongo-backed setting (each re-reads on its own TTL)."""
    with _settings_cache_lock:
        _settings_cache["doc"] = None
        _settings_cache["loaded_at"] = None


def get_creator_group_access_settings(*, force_refresh: bool = False) -> dict:
    """Resolution order: (1) an explicit MongoDB ``creator_group_access``
    document — once saved, authoritative even if it clears the chat ID back
    to ``null``; (2) the ``CREATOR_GROUP_CHAT_ID`` env var, only when no DB
    document has ever been saved; (3) unconfigured."""
    doc = _load_creator_group_doc(force_refresh=force_refresh)
    if doc is not None:
        chat_id = doc.get("creator_group_chat_id")
        membership_check_enabled = bool(doc.get("membership_check_enabled", False))
        source = "database"
    else:
        env_val = (os.environ.get("CREATOR_GROUP_CHAT_ID") or "").strip()
        try:
            chat_id = int(env_val) if env_val else None
        except ValueError:
            chat_id = None
        membership_check_enabled = chat_id is not None
        source = "env" if chat_id is not None else "unconfigured"

    return {
        "creator_group_chat_id": chat_id,
        "membership_check_enabled": membership_check_enabled,
        "chat_title": (doc or {}).get("chat_title"),
        "chat_type": (doc or {}).get("chat_type"),
        "bot_membership_status": (doc or {}).get("bot_membership_status"),
        "verified_at": (doc or {}).get("verified_at"),
        "updated_at": (doc or {}).get("updated_at"),
        "updated_by": (doc or {}).get("updated_by"),
        "config_version": int((doc or {}).get("config_version", 0) or 0),
        "source": source,
    }


def _serialize_creator_group_settings(settings: dict) -> dict:
    out = dict(settings)
    for key in ("verified_at", "updated_at"):
        if out.get(key):
            out[key] = out[key].isoformat()
    return out


# ---------------------------------------------------------------------------
# Telegram verification of a candidate creator group (getChat + the bot's
# own getChatMember) — used by both the "Verify Group" preview endpoint and
# the save endpoint. Never logs the bot token or full API responses.
# ---------------------------------------------------------------------------

def _validate_creator_group_chat_id(raw) -> tuple[int | None, str | None, bool]:
    """Returns (chat_id, error_code, prefix_warning). Only rejects zero/positive
    values outright; a negative ID not starting with "-100" is *not* rejected
    here (that's a soft warning) — real validation is the Telegram call."""
    try:
        chat_id = int(raw)
    except (TypeError, ValueError):
        return None, "invalid_creator_group_chat_id", False
    if isinstance(raw, bool) or chat_id >= 0:
        return None, "invalid_creator_group_chat_id", False
    prefix_warning = not str(chat_id).startswith("-100")
    return chat_id, None, prefix_warning


def _verify_telegram_group(chat_id: int) -> tuple[dict | None, str | None]:
    """Returns (info, error_code). ``info`` (when present) carries whatever
    was learned before a failure: chat_title/chat_type always once getChat
    succeeds, bot_membership_status only once getChatMember succeeds."""
    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        return None, "creator_group_verification_failed"

    try:
        chat_resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChat", params={"chat_id": chat_id}, timeout=8
        )
    except requests.RequestException:
        return None, "creator_group_verification_failed"

    if chat_resp.status_code == 400 or chat_resp.status_code == 404:
        return None, "creator_group_not_found"
    if chat_resp.status_code != 200:
        return None, "creator_group_verification_failed"
    try:
        chat_data = chat_resp.json()
    except ValueError:
        return None, "creator_group_verification_failed"
    if not chat_data.get("ok"):
        return None, "creator_group_not_found"

    chat_result = chat_data.get("result") or {}
    info = {
        "chat_title": chat_result.get("title"),
        "chat_type": chat_result.get("type"),
        "bot_membership_status": None,
    }
    if info["chat_type"] not in ("group", "supergroup", "channel"):
        return info, "creator_group_wrong_chat_type"

    try:
        me_resp = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=8)
        me_data = me_resp.json() if me_resp.status_code == 200 else {}
        bot_id = (me_data.get("result") or {}).get("id")
    except (requests.RequestException, ValueError):
        bot_id = None
    if not bot_id:
        return info, "creator_group_verification_failed"

    try:
        member_resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": chat_id, "user_id": bot_id},
            timeout=8,
        )
    except requests.RequestException:
        return info, "creator_group_verification_failed"
    if member_resp.status_code != 200:
        return info, "creator_group_bot_access_denied"
    try:
        member_data = member_resp.json()
    except ValueError:
        return info, "creator_group_verification_failed"
    if not member_data.get("ok"):
        return info, "creator_group_bot_access_denied"

    bot_status = str((member_data.get("result") or {}).get("status") or "")
    info["bot_membership_status"] = bot_status
    if bot_status not in _TELEGRAM_MEMBER_STATUSES:
        return info, "creator_group_bot_access_denied"

    return info, None


# ---------------------------------------------------------------------------
# Telegram Mini App auth (initData) — independent of admin auth.
# ---------------------------------------------------------------------------

def _extract_authenticated_user():
    """Returns (user_id, username, error) where error is (code, http_status) or None.

    user_id is always derived from verified Telegram initData — a user_id
    sent in JSON/query is never trusted.
    """
    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data

    init_data = extract_raw_init_data_from_query(request)
    if not init_data:
        return None, None, ("invalid_telegram_auth", 401)
    ok, parsed, _ = verify_telegram_init_data(init_data)
    if not ok:
        return None, None, ("invalid_telegram_auth", 401)

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
        return None, None, ("invalid_telegram_auth", 401)

    username = (user_payload or {}).get("username") or ""
    return user_id, username, None


# ---------------------------------------------------------------------------
# Creator authorization
# ---------------------------------------------------------------------------

def _as_aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _check_group_membership(user_id: int, chat_id) -> bool | None:
    """Return True/False for a definitive verdict, or None when Telegram is
    temporarily unavailable / the response is ambiguous (never treated as a
    confirmed "not a member" — see module docstring)."""
    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        return None
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": chat_id, "user_id": user_id},
            timeout=8,
        )
    except requests.RequestException:
        logger.warning("[CREATOR_SHARE] membership check network error user_id=%s", user_id)
        return None
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    if not data.get("ok"):
        return None
    result = data.get("result") or {}
    status = str(result.get("status") or "").lower()
    if status in _TELEGRAM_MEMBER_STATUSES:
        return True
    if status == "restricted":
        return bool(result.get("is_member", True))
    if status in _TELEGRAM_NONMEMBER_STATUSES:
        return False
    return None


def _verify_live_membership(record: dict, chat_id, config_version: int) -> bool | None:
    """True = confirmed member, False = confirmed left/kicked, None = keep
    the existing access (temporarily unresolvable, within the grace window).

    Every cached verdict (confirmed-member and unresolvable alike) is keyed
    to ``config_version``: a group change bumps the version, which
    immediately invalidates *all* previously cached verdicts for every
    creator — no stale access survives a group switch, and no request keeps
    re-querying Telegram inside the outage grace window either.
    """
    now = now_utc()

    last_verified = _as_aware_utc(record.get("last_membership_verified_at"))
    last_verified_version = record.get("last_membership_verified_config_version")
    if (
        last_verified
        and last_verified_version == config_version
        and (now - last_verified).total_seconds() < MEMBERSHIP_VERIFY_CACHE_SEC
    ):
        return True

    # Check the outage cache *before* hitting Telegram, so repeated requests
    # inside the grace window never each pay the (up to 8s) HTTP timeout.
    last_unresolvable = _as_aware_utc(record.get("last_membership_unresolvable_at"))
    last_unresolvable_version = record.get("last_membership_unresolvable_config_version")
    if (
        last_unresolvable
        and last_unresolvable_version == config_version
        and (now - last_unresolvable).total_seconds() < MEMBERSHIP_UNRESOLVABLE_GRACE_SEC
    ):
        return None

    verdict = _check_group_membership(record["user_id"], chat_id)
    if verdict is True:
        database.db["creator_members"].update_one(
            {"_id": record["_id"]},
            {
                "$set": {
                    "last_membership_verified_at": now,
                    "last_membership_verified_config_version": config_version,
                    "updated_at": now,
                },
                "$unset": {"last_membership_unresolvable_at": "", "last_membership_unresolvable_config_version": ""},
            },
        )
        return True
    if verdict is False:
        return False
    # Telegram lookup temporarily unavailable: use the short cache instead of
    # permanently removing the creator.
    database.db["creator_members"].update_one(
        {"_id": record["_id"]},
        {
            "$set": {
                "last_membership_unresolvable_at": now,
                "last_membership_unresolvable_config_version": config_version,
                "updated_at": now,
            }
        },
    )
    return None


def _lazy_ensure_creator_profile(
    user_id: int, username: str, chat_id, existing_record: dict | None, config_version: int
) -> dict:
    """Called only after a *confirmed* Creator Access Chat membership check.

    If ``existing_record`` is already present (it can only be an ``active``
    record here — ``suspended``/``removed`` are filtered out earlier), it is
    returned unchanged. Otherwise an ``active`` creator profile is created via
    ``$setOnInsert`` so concurrent first-access requests for the same user
    never race into duplicate documents (the unique index on ``user_id`` is
    the actual duplicate guard; ``$setOnInsert`` on an upsert just avoids a
    redundant write when the insert loses the race).

    ``last_membership_verified_config_version`` is stamped alongside
    ``last_membership_verified_at`` so the very next request reuses
    ``_verify_live_membership()``'s cache immediately, instead of every
    lazily-created profile paying for one extra, redundant Telegram call
    (and its failure mode, ``creator_membership_unresolvable``) on the
    request right after creation.
    """
    if existing_record is not None:
        return existing_record

    now = now_utc()
    try:
        database.db["creator_members"].update_one(
            {"user_id": user_id},
            {
                "$setOnInsert": {
                    "user_id": user_id,
                    "username": username or "",
                    "status": "active",
                    "creator_tier": "pilot",
                    "source_group_id": chat_id,
                    "approved_at": now,
                    "approval_source": "creator_access_chat_membership",
                    "created_at": now,
                    "updated_at": now,
                    "last_membership_verified_at": now,
                    "last_membership_verified_config_version": config_version,
                }
            },
            upsert=True,
        )
    except DuplicateKeyError:
        # A concurrent request won the insert race first; either way,
        # re-reading below returns the single surviving document.
        logger.info("[CREATOR_SHARE] lazy creator profile upsert raced for user_id=%s", user_id)

    return database.db["creator_members"].find_one({"user_id": user_id})


def _verify_creator_access(user_id: int, username: str = ""):
    """Returns (creator_doc, error) where error is (code, http_status) or None.

    See the module docstring for the full access model. In order:
      1. An existing ``suspended``/``removed`` ``creator_members`` record
         always denies, even if the user is still in the configured chat.
      2. When membership verification is enabled, a confirmed Creator Access
         Chat membership grants access — creating an ``active`` profile
         lazily if none exists yet.
      3. When membership verification is disabled, only an existing
         ``active`` record is honoured; a user with no record stays denied.
    """
    record = database.db["creator_members"].find_one({"user_id": user_id})
    if record and record.get("status") in ("suspended", "removed"):
        code = "creator_suspended" if record["status"] == "suspended" else "creator_not_authorized"
        return None, (code, 403)

    settings = get_creator_group_access_settings()
    if settings["membership_check_enabled"]:
        chat_id = settings["creator_group_chat_id"]
        if not chat_id:
            return None, ("creator_group_not_configured", 503)

        if record:
            verdict = _verify_live_membership(record, chat_id, settings["config_version"])
        else:
            verdict = _check_group_membership(user_id, chat_id)

        if verdict is False:
            if record:
                database.db["creator_members"].update_one(
                    {"_id": record["_id"]}, {"$set": {"status": "removed", "updated_at": now_utc()}}
                )
            return None, ("creator_membership_required", 403)
        if verdict is None:
            return None, ("creator_membership_unresolvable", 503)

        record = _lazy_ensure_creator_profile(user_id, username, chat_id, record, settings["config_version"])
        return record, None

    # Membership verification explicitly disabled: fall back to requiring an
    # existing active creator_members record. This never opens access to a
    # user with no record — that would defeat the point of the toggle.
    if record and record.get("status") == "active":
        return record, None
    return None, ("creator_not_authorized", 403)


def _authenticate_and_authorize():
    user_id, username, auth_err = _extract_authenticated_user()
    if auth_err:
        return None, None, None, auth_err
    record, access_err = _verify_creator_access(user_id, username)
    if access_err:
        logger.info("[CREATOR_SHARE][ACCESS_DENIED] user_id=%s reason_code=%s", user_id, access_err[0])
        return user_id, username, None, access_err
    logger.info("[CREATOR_SHARE][ACCESS_GRANTED] user_id=%s", user_id)
    return user_id, username, record, None


# ---------------------------------------------------------------------------
# Creator API
# ---------------------------------------------------------------------------

@creator_share_bp.get("/api/creator/share/status")
def creator_share_status():
    user_id, _username, record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status
    return jsonify(
        {
            "status": "ok",
            "creator": {
                "user_id": user_id,
                "creator_tier": record.get("creator_tier"),
                "access": True,
            },
        }
    )


def _current_week_start_utc(now: datetime) -> datetime:
    # Reuses the same Monday-00:00-Asia/Kuala_Lumpur weekly boundary as the
    # rest of the referral/affiliate system (affiliate_leaderboard.py), so
    # "This Week" here always agrees with every other weekly referral metric
    # instead of drifting during the 00:00-08:00 KL gap a naive UTC-Monday
    # boundary would create.
    from affiliate_leaderboard import affiliate_week_window_utc_from_reference

    week_start_utc, _week_end_utc, _week_start_local = affiliate_week_window_utc_from_reference(now)
    return week_start_utc


@creator_share_bp.post("/api/creator/share/generate")
def creator_share_generate():
    user_id, username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    body = request.get_json(force=True, silent=True) or {}
    platform = body.get("platform") or "generic"
    if platform not in ALLOWED_PLATFORMS:
        return jsonify({"status": "error", "code": "invalid_platform"}), 400

    allowed, _reason, _meta = consume_referral_rate_limits(
        database.db["creator_generation_rate_limits"],
        inviter_id=user_id,
        now_utc=now_utc(),
        hourly_limit=GENERATE_HOURLY_LIMIT,
        daily_limit=0,
    )
    if not allowed:
        logger.info("[CREATOR_SHARE][RATE_LIMITED] user_id=%s reason_code=creator_generation_rate_limited", user_id)
        return jsonify({"status": "error", "code": "creator_generation_rate_limited"}), 429

    result = generate_share_package(
        user_id, username, generated_by=CREATOR_SHARE_SOURCE, platform=platform
    )
    if not result.get("ok"):
        logger.warning(
            "[CREATOR_SHARE][GENERATION_FAILED] user_id=%s reason_code=%s", user_id, result.get("code")
        )
        return jsonify({"status": "error", "code": result.get("code") or "generation_failed"}), 502

    share_text = build_creator_share_text(
        hook_text=result.get("hook_text"),
        playback_url=result.get("playback_url"),
        referral_link=result.get("invite_link"),
    )

    logger.info(
        "[CREATOR_SHARE][GENERATED] user_id=%s package_id=%s hook_id=%s playback_record_id=%s platform=%s",
        user_id,
        result.get("package_id"),
        result.get("hook_id"),
        result.get("playback_record_id"),
        platform,
    )
    return jsonify(
        {
            "status": "ok",
            "package_id": result.get("package_id"),
            "hook_text": result.get("hook_text"),
            "playback_url": result.get("playback_url"),
            "referral_link": result.get("invite_link"),
            "share_text": share_text,
        }
    )


def _mutate_owned_package(package_id: str, user_id: int, update: dict):
    """Ownership-checked mutation of a share_generations document: the query
    filters on both package_id and the authenticated user_id, so a
    non-owner's request simply matches nothing (same 404 as a nonexistent
    package_id — existence is never leaked)."""
    return database.db["share_generations"].find_one_and_update(
        {"package_id": package_id, "user_id": user_id}, update, return_document=ReturnDocument.AFTER
    )


@creator_share_bp.post("/api/creator/share/<package_id>/copied")
def creator_share_copied(package_id):
    user_id, _username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    body = request.get_json(force=True, silent=True) or {}
    platform = body.get("platform") if body.get("platform") in ALLOWED_PLATFORMS else "generic"
    # Optional -- historical/older clients never send this, so existing
    # copy_count/copied_at semantics must keep working without it. Guard
    # with isinstance first: an unhashable value (dict/list) would raise on
    # the "in ALLOWED_COPY_METHODS" membership test below.
    raw_copy_method = body.get("copy_method")
    copy_method = raw_copy_method if isinstance(raw_copy_method, str) and raw_copy_method in ALLOWED_COPY_METHODS else None

    set_fields = {"copied_at": now_utc(), "latest_copy_platform": platform}
    if copy_method:
        set_fields["latest_copy_method"] = copy_method

    updated = _mutate_owned_package(
        package_id,
        user_id,
        {"$set": set_fields, "$inc": {"copy_count": 1}},
    )
    if not updated:
        return jsonify({"status": "error", "code": "not_found"}), 404
    logger.info(
        "[CREATOR_SHARE][COPIED] user_id=%s package_id=%s platform=%s copy_method=%s",
        user_id, package_id, platform, copy_method or "unknown",
    )
    return jsonify({"status": "ok"})


@creator_share_bp.post("/api/creator/share/<package_id>/share-clicked")
def creator_share_clicked(package_id):
    user_id, _username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    body = request.get_json(force=True, silent=True) or {}
    platform = body.get("platform") if body.get("platform") in ALLOWED_PLATFORMS else "generic"

    updated = _mutate_owned_package(
        package_id,
        user_id,
        {
            "$set": {"share_clicked_at": now_utc(), "latest_share_click_platform": platform},
            "$inc": {"share_click_count": 1},
        },
    )
    if not updated:
        return jsonify({"status": "error", "code": "not_found"}), 404
    logger.info(
        "[CREATOR_SHARE][SHARE_CLICKED] user_id=%s package_id=%s platform=%s", user_id, package_id, platform
    )
    return jsonify({"status": "ok"})


@creator_share_bp.get("/api/creator/share/results")
def creator_share_results():
    user_id, _username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    from dashboard_panels import _PENDING_STATUSES, _QUALIFIED_STATUSES, _REVOKED_STATUSES
    from scheduler import current_month_qualified_referral_count, current_month_window_utc

    pending_col = database.db["pending_referrals"]
    now = now_utc()
    week_start = _current_week_start_utc(now)
    month_start_utc, month_end_utc = current_month_window_utc(now)

    base_filter = {"inviter_user_id": user_id}
    week_filter = {**base_filter, "created_at_utc": {"$gte": week_start}}
    # "created_at_utc" is the same join timestamp field pending_referrals
    # docs are written with (main.py referral-join handler) and the same
    # field affiliate_leaderboard.py windows its own weekly "joins" count
    # on -- reused here, not a new/guessed timestamp field.
    month_filter = {**base_filter, "created_at_utc": {"$gte": month_start_utc, "$lt": month_end_utc}}

    total_referral_joins = pending_col.count_documents(base_filter)
    qualified_referrals = pending_col.count_documents({**base_filter, "status": {"$in": _QUALIFIED_STATUSES}})
    pending_referrals_count = pending_col.count_documents({**base_filter, "status": {"$in": _PENDING_STATUSES}})
    revoked_referrals = pending_col.count_documents({**base_filter, "status": {"$in": _REVOKED_STATUSES}})
    current_week_referrals = pending_col.count_documents(week_filter)
    current_week_qualified = pending_col.count_documents(
        {**week_filter, "status": {"$in": _QUALIFIED_STATUSES}}
    )
    current_month_referrals = pending_col.count_documents(month_filter)

    share_col = database.db["share_generations"]
    creator_filter = {
        "user_id": user_id,
        "generated_by": {"$in": [CREATOR_SHARE_SOURCE, _LEGACY_CREATOR_SHARE_SOURCE]},
    }
    latest_doc = share_col.find_one(
        creator_filter, sort=[("generated_at", -1)], projection={"generated_at": 1}
    )
    latest_generated_at = (
        latest_doc["generated_at"].isoformat() if latest_doc and latest_doc.get("generated_at") else None
    )
    total_packages_generated = share_col.count_documents(creator_filter)
    current_month_qualified = current_month_qualified_referral_count(user_id, now)

    return jsonify(
        {
            "status": "ok",
            "results": {
                "total_referral_joins": int(total_referral_joins),
                "qualified_referrals": int(qualified_referrals),
                "pending_referrals": int(pending_referrals_count),
                "revoked_referrals": int(revoked_referrals),
                "current_week_referrals": int(current_week_referrals),
                "current_week_qualified": int(current_week_qualified),
                # Current-reward-month figures (Asia/Kuala_Lumpur calendar
                # month) -- what the Money Room UI displays as "Invited" /
                # "Qualified". current_month_qualified is the exact same
                # value passed into next_reward_tier below, so the
                # displayed Qualified count and the reward-progress message
                # can never disagree.
                "current_month_referrals": int(current_month_referrals),
                "current_month_qualified": int(current_month_qualified),
                "latest_generated_at": latest_generated_at,
                "total_packages_generated": int(total_packages_generated),
                "next_reward_tier": _next_reward_tier(current_month_qualified),
            },
        }
    )


# ---------------------------------------------------------------------------
# Admin API — Creator Access
# ---------------------------------------------------------------------------

def _validate_telegram_user_id(raw) -> int | None:
    try:
        uid = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return uid if uid > 0 else None


def _serialize_creator(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for key in ("approved_at", "last_membership_verified_at", "last_membership_unresolvable_at", "created_at", "updated_at"):
        if out.get(key):
            out[key] = out[key].isoformat()
    return out


@creator_share_bp.get("/api/admin/referral/creators")
def admin_list_creators():
    _, err = _require_admin()
    if err:
        return err
    status = request.args.get("status")
    q = (request.args.get("q") or "").strip()
    filt: dict = {}
    if status in CREATOR_STATUSES:
        filt["status"] = status
    if q:
        or_clauses: list[dict] = [{"username": {"$regex": re.escape(q), "$options": "i"}}]
        uid = _validate_telegram_user_id(q)
        if uid:
            or_clauses.append({"user_id": uid})
        filt["$or"] = or_clauses
    docs = list(database.db["creator_members"].find(filt, sort=[("created_at", -1)], limit=500))
    active_count = database.db["creator_members"].count_documents({"status": "active"})
    return jsonify(
        {
            "status": "ok",
            "creators": [_serialize_creator(d) for d in docs],
            "active_count": active_count,
            "creator_group_configured": bool(get_creator_group_access_settings()["creator_group_chat_id"]),
        }
    )


@creator_share_bp.post("/api/admin/referral/creators")
def admin_create_creator():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    user_id = _validate_telegram_user_id(body.get("user_id"))
    if not user_id:
        return jsonify({"status": "error", "code": "invalid_user_id"}), 400
    username = (body.get("username") or "").strip()
    creator_tier = (body.get("creator_tier") or "pilot").strip() or "pilot"
    source_group_id = body.get("source_group_id")
    now = now_utc()
    admin_id = (admin or {}).get("id")

    existing = database.db["creator_members"].find_one({"user_id": user_id})
    if existing:
        database.db["creator_members"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": "active",
                    "username": username or existing.get("username", ""),
                    "creator_tier": creator_tier,
                    "source_group_id": source_group_id,
                    "approved_at": now,
                    "approved_by": admin_id,
                    "approval_source": "manual",
                    "updated_at": now,
                }
            },
        )
        return jsonify({"status": "ok", "user_id": user_id, "reactivated": True})

    doc = {
        "user_id": user_id,
        "status": "active",
        "username": username,
        "source_group_id": source_group_id,
        "creator_tier": creator_tier,
        "approved_at": now,
        "approved_by": admin_id,
        "approval_source": "manual",
        "last_membership_verified_at": None,
        "created_at": now,
        "updated_at": now,
    }
    database.db["creator_members"].insert_one(doc)
    return jsonify({"status": "ok", "user_id": user_id}), 201


@creator_share_bp.post("/api/admin/referral/creators/bulk")
def admin_bulk_creators():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    raw = body.get("user_ids") or body.get("lines") or ""
    if isinstance(raw, str):
        candidates = [c for c in re.split(r"[\s,]+", raw) if c.strip()]
    elif isinstance(raw, list):
        candidates = [str(c).strip() for c in raw if str(c).strip()]
    else:
        candidates = []

    if len(candidates) > MAX_BULK_CREATOR_IMPORT:
        return jsonify({"status": "error", "code": "too_many_lines", "max_lines": MAX_BULK_CREATOR_IMPORT}), 400

    now = now_utc()
    admin_id = (admin or {}).get("id")
    seen: set[int] = set()
    inserted = reactivated = skipped = rejected = 0
    results = []

    for raw_line in candidates:
        uid = _validate_telegram_user_id(raw_line)
        if not uid:
            results.append({"line": raw_line, "status": "rejected", "reason": "invalid_user_id"})
            rejected += 1
            continue
        if uid in seen:
            results.append({"line": raw_line, "status": "skipped", "reason": "duplicate_in_batch"})
            skipped += 1
            continue
        seen.add(uid)

        existing = database.db["creator_members"].find_one({"user_id": uid})
        if existing:
            database.db["creator_members"].update_one(
                {"user_id": uid},
                {
                    "$set": {
                        "status": "active",
                        "approved_at": now,
                        "approved_by": admin_id,
                        "approval_source": "bulk_import",
                        "updated_at": now,
                    }
                },
            )
            results.append({"line": raw_line, "status": "reactivated"})
            reactivated += 1
            continue

        database.db["creator_members"].insert_one(
            {
                "user_id": uid,
                "status": "active",
                "username": "",
                "source_group_id": None,
                "creator_tier": "pilot",
                "approved_at": now,
                "approved_by": admin_id,
                "approval_source": "bulk_import",
                "last_membership_verified_at": None,
                "created_at": now,
                "updated_at": now,
            }
        )
        results.append({"line": raw_line, "status": "inserted"})
        inserted += 1

    return jsonify(
        {
            "status": "ok",
            "inserted": inserted,
            "reactivated": reactivated,
            "skipped": skipped,
            "rejected": rejected,
            "results": results,
        }
    )


def _set_creator_status(raw_user_id: str, status_val: str):
    _, err = _require_admin()
    if err:
        return err
    uid = _validate_telegram_user_id(raw_user_id)
    if not uid:
        return jsonify({"status": "error", "code": "invalid_user_id"}), 400
    result = database.db["creator_members"].update_one(
        {"user_id": uid}, {"$set": {"status": status_val, "updated_at": now_utc()}}
    )
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


@creator_share_bp.post("/api/admin/referral/creators/<user_id>/suspend")
def admin_suspend_creator(user_id):
    return _set_creator_status(user_id, "suspended")


@creator_share_bp.post("/api/admin/referral/creators/<user_id>/activate")
def admin_activate_creator(user_id):
    return _set_creator_status(user_id, "active")


@creator_share_bp.delete("/api/admin/referral/creators/<user_id>")
def admin_remove_creator(user_id):
    # Soft-delete (status="removed") rather than a hard document delete, so
    # the action is reversible via the activate endpoint and history/audit
    # fields (approved_by/approved_at) are preserved.
    return _set_creator_status(user_id, "removed")


# ---------------------------------------------------------------------------
# Admin API — Creator Access Chat settings (internal key/route names remain
# "creator_group_*" / "creator-settings" for backward compatibility; only the
# admin-facing labels changed to reflect that a channel is now supported).
# ---------------------------------------------------------------------------

@creator_share_bp.get("/api/admin/referral/creator-settings")
def admin_get_creator_settings():
    _, err = _require_admin()
    if err:
        return err
    settings = get_creator_group_access_settings(force_refresh=True)
    return jsonify({"status": "ok", "settings": _serialize_creator_group_settings(settings)})


@creator_share_bp.post("/api/admin/referral/creator-settings/verify-group")
def admin_verify_creator_group():
    admin, err = _require_admin()
    if err:
        return err
    admin_id = (admin or {}).get("id")

    body = request.get_json(force=True, silent=True) or {}
    chat_id, code, prefix_warning = _validate_creator_group_chat_id(body.get("creator_group_chat_id"))
    if code:
        return jsonify({"status": "error", "code": code}), 400

    info, verify_err = _verify_telegram_group(chat_id)
    if verify_err:
        logger.warning(
            "[CREATOR_GROUP_SETTINGS][VERIFY_FAILED] admin_id=%s chat_id=%s reason_code=%s",
            admin_id, chat_id, verify_err,
        )
        return jsonify({"status": "error", "code": verify_err, **(info or {})}), 400

    logger.info("[CREATOR_GROUP_SETTINGS][VERIFIED] admin_id=%s chat_id=%s", admin_id, chat_id)
    return jsonify(
        {
            "status": "ok",
            "chat_title": info.get("chat_title"),
            "chat_type": info.get("chat_type"),
            "bot_membership_status": info.get("bot_membership_status"),
            "warning": "chat_id_prefix_unusual" if prefix_warning else None,
        }
    )


@creator_share_bp.put("/api/admin/referral/creator-settings")
def admin_update_creator_settings():
    admin, err = _require_admin()
    if err:
        return err
    admin_id = (admin or {}).get("id")

    body = request.get_json(force=True, silent=True) or {}
    membership_check_enabled = body.get("membership_check_enabled")
    if not isinstance(membership_check_enabled, bool):
        return jsonify({"status": "error", "code": "invalid_membership_check_enabled"}), 400
    force_save = bool(body.get("force_save"))

    prior_doc = database.db[APP_SETTINGS_COLLECTION].find_one({"_id": CREATOR_GROUP_SETTINGS_KEY}) or {}
    prior_chat_id = prior_doc.get("creator_group_chat_id")
    prior_version = int(prior_doc.get("config_version", 0) or 0)

    raw_chat_id = body.get("creator_group_chat_id")
    verify_err = None
    if raw_chat_id in (None, ""):
        chat_id = None
        chat_title = chat_type = bot_membership_status = None
        verified_at = None
    else:
        chat_id, code, _prefix_warning = _validate_creator_group_chat_id(raw_chat_id)
        if code:
            return jsonify({"status": "error", "code": code}), 400

        info, verify_err = _verify_telegram_group(chat_id)
        if verify_err and not force_save:
            # Verification failure never overwrites the last valid, already-saved
            # setting — return before any write happens.
            logger.warning(
                "[CREATOR_GROUP_SETTINGS][VERIFY_FAILED] admin_id=%s old_chat_id=%s new_chat_id=%s "
                "config_version=%s reason_code=%s",
                admin_id, prior_chat_id, chat_id, prior_version, verify_err,
            )
            return jsonify({"status": "error", "code": verify_err}), 400

        chat_title = (info or {}).get("chat_title")
        chat_type = (info or {}).get("chat_type")
        bot_membership_status = (info or {}).get("bot_membership_status")
        verified_at = None if verify_err else now_utc()
        if verify_err:
            logger.warning(
                "[CREATOR_GROUP_SETTINGS][VERIFY_FAILED] admin_id=%s old_chat_id=%s new_chat_id=%s "
                "config_version=%s reason_code=%s force_save=true",
                admin_id, prior_chat_id, chat_id, prior_version, verify_err,
            )
        else:
            logger.info("[CREATOR_GROUP_SETTINGS][VERIFIED] admin_id=%s chat_id=%s", admin_id, chat_id)

    now = now_utc()
    new_version = prior_version + 1
    doc = {
        "_id": CREATOR_GROUP_SETTINGS_KEY,
        "creator_group_chat_id": chat_id,
        "membership_check_enabled": membership_check_enabled,
        "chat_title": chat_title,
        "chat_type": chat_type,
        "bot_membership_status": bot_membership_status,
        "verified_at": verified_at,
        "updated_at": now,
        "updated_by": admin_id,
        "config_version": new_version,
    }
    database.db[APP_SETTINGS_COLLECTION].update_one({"_id": CREATOR_GROUP_SETTINGS_KEY}, {"$set": doc}, upsert=True)
    # Config-version bump (above) + cache clear (below) together ensure no
    # cached membership verdict from the old group can grant access after
    # this point — see _verify_live_membership's config_version comparison.
    invalidate_creator_group_settings_cache()

    audit_doc = {
        "group": CREATOR_GROUP_SETTINGS_KEY,
        "admin": admin_id,
        "old_chat_id": prior_chat_id,
        "new_chat_id": chat_id,
        "config_version": new_version,
        "force_save": force_save,
        "unverified": bool(verify_err),
        "verify_error": verify_err,
        "created_at": now,
    }
    try:
        database.db[APP_SETTINGS_AUDIT_COLLECTION].insert_one(audit_doc)
    except Exception:
        logger.warning("[CREATOR_SHARE] failed to write creator group settings audit log", exc_info=True)

    logger.info(
        "[CREATOR_GROUP_SETTINGS][UPDATED] admin_id=%s old_chat_id=%s new_chat_id=%s config_version=%s "
        "force_save=%s unverified=%s",
        admin_id, prior_chat_id, chat_id, new_version, force_save, bool(verify_err),
    )
    return jsonify(
        {"status": "ok", "settings": _serialize_creator_group_settings(get_creator_group_access_settings(force_refresh=True))}
    )
