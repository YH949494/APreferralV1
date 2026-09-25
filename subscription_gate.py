"""Shared Telegram official-channel subscription verification service.

Single reusable gate so tournament/campaign flows don't reimplement
getChatMember polling, retry/backoff, and caching. Existing welcome-journey
and voucher-claim subscription checks (vouchers.py: check_channel_subscribed)
are untouched — this is a parallel, campaign-aware service used only by the
new Campaign Centre / tournament flows, with its own cache collection so a
change here can never affect those existing flows.

Collection: ``campaign_subscription_cache``
"""

from __future__ import annotations

import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone

import requests

import database

logger = logging.getLogger(__name__)

SUBSCRIBED_STATUSES = {"member", "administrator", "creator"}
NOT_SUBSCRIBED_STATUSES = {"left", "kicked", "restricted"}

# Tri-state membership outcome carried on every verify_campaign_subscription
# result as ``state``. Only STATE_NOT_MEMBER is a positive Telegram
# confirmation that the user is outside the channel; every ambiguous case
# (missing config, network error, 429, non-200, bad JSON, unknown status)
# is STATE_UNAVAILABLE so callers never tell a user "not subscribed" because
# Telegram (or our own config) failed.
STATE_MEMBER = "member"
STATE_NOT_MEMBER = "not_member"
STATE_UNAVAILABLE = "unavailable"

_CHANNEL_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")
_CHANNEL_LINK_PREFIXES = ("https://", "http://", "t.me/", "telegram.me/")

DEFAULT_CACHE_TTL_S = int(os.getenv("CAMPAIGN_SUBSCRIPTION_CACHE_TTL_S", "300"))


def _ensure_indexes() -> None:
    try:
        col = database.db["campaign_subscription_cache"]
        col.create_index([("channel_id", 1), ("user_id", 1)], name="ux_campaign_sub_cache_channel_user", unique=True)
        col.create_index([("expires_at", 1)], name="ttl_campaign_sub_cache", expireAfterSeconds=0)
    except Exception:
        logger.warning("[SUBSCRIPTION_GATE] index creation failed", exc_info=True)


_ensure_indexes()


def _cache_get(channel_id, user_id: int) -> bool | None:
    try:
        doc = database.db["campaign_subscription_cache"].find_one({"channel_id": str(channel_id), "user_id": user_id})
    except Exception:
        return None
    if not doc:
        return None
    expires_at = doc.get("expires_at")
    if not expires_at or datetime.now(timezone.utc) >= expires_at:
        return None
    return bool(doc.get("subscribed"))


def _cache_set(channel_id, user_id: int, subscribed: bool, ttl_s: int) -> None:
    now = datetime.now(timezone.utc)
    try:
        database.db["campaign_subscription_cache"].update_one(
            {"channel_id": str(channel_id), "user_id": user_id},
            {"$set": {
                "channel_id": str(channel_id),
                "user_id": user_id,
                "subscribed": subscribed,
                "checked_at": now,
                "expires_at": now + timedelta(seconds=ttl_s),
            }},
            upsert=True,
        )
    except Exception:
        logger.warning("[SUBSCRIPTION_GATE] cache_write_failed", exc_info=True)


def _log_event(*, campaign_id: str, user_id: int, channel_id, result: str, tg_status: str | None,
                source: str, latency_ms: int, error: str | None = None) -> None:
    from campaign_events import emit_campaign_event

    emit_campaign_event(
        event_type=f"subscription_{result}",
        campaign_id=campaign_id,
        telegram_user_id=user_id,
        source=source,
        status="success" if result == "pass" else "fail",
        reason=error,
        metadata={"channel_id": str(channel_id), "telegram_status": tg_status, "latency_ms": latency_ms},
    )


def resolve_channel_chat_id(telegram_cfg: dict | None):
    """The ``chat_id`` to send to getChatMember for a campaign's telegram
    block, or None when nothing usable is configured.

    A numeric ``channel_id`` wins. Otherwise ``channel_username`` is
    normalized to Telegram's ``@username`` form: the admin wizard stores it
    bare (placeholder "mychannel"), and Telegram rejects a bare username
    with HTTP 400 "chat not found" — which previously surfaced to every user
    as "We couldn't verify your channel subscription". A pasted t.me link or
    a leading "@" is accepted too."""
    telegram_cfg = telegram_cfg or {}
    channel_id = telegram_cfg.get("channel_id")
    if isinstance(channel_id, int) and not isinstance(channel_id, bool) and channel_id:
        return channel_id
    if isinstance(channel_id, str) and channel_id.strip():
        raw_id = channel_id.strip()
        if raw_id.lstrip("-").isdigit():
            return raw_id
        # A username typed into the id field — normalize it like one.
        return _normalize_channel_username(raw_id)
    return _normalize_channel_username(telegram_cfg.get("channel_username"))


def _normalize_channel_username(raw) -> str | None:
    name = str(raw or "").strip()
    lowered = name.lower()
    for prefix in _CHANNEL_LINK_PREFIXES:
        if lowered.startswith(prefix):
            name = name[len(prefix):]
            lowered = name.lower()
    name = name.lstrip("@").split("/", 1)[0].split("?", 1)[0].strip()
    if not _CHANNEL_USERNAME_RE.match(name):
        return None
    return "@" + name


def _classify_member(member: dict | None) -> str:
    """Maps a getChatMember ``result`` object to a tri-state outcome.
    ``restricted`` is a real member unless Telegram says ``is_member`` is
    false (a restricted user who has since left the chat)."""
    member = member or {}
    status = member.get("status")
    if status in SUBSCRIBED_STATUSES:
        return STATE_MEMBER
    if status == "restricted":
        return STATE_MEMBER if member.get("is_member") is True else STATE_NOT_MEMBER
    if status in ("left", "kicked"):
        return STATE_NOT_MEMBER
    return STATE_UNAVAILABLE


def _get_chat_member_result(channel_id, user_id: int) -> tuple[dict | None, str | None]:
    """Returns (getChatMember result object, error). Handles 429 with
    bounded retries."""
    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        return None, "missing_bot_token"
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{token}/getChatMember",
                params={"chat_id": channel_id, "user_id": user_id},
                timeout=8,
            )
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                return None, f"network_error:{exc.__class__.__name__}"
            time.sleep(min(2.0, 0.4 * (2 ** (attempt - 1))) + random.uniform(0, 0.15))
            continue

        if resp.status_code == 429:
            if attempt >= max_attempts:
                return None, "rate_limited"
            time.sleep(min(2.0, 0.4 * (2 ** (attempt - 1))) + random.uniform(0, 0.15))
            continue

        if resp.status_code != 200:
            # Keep Telegram's description (e.g. "Bad Request: chat not
            # found") on the logged reason — it is the only thing that tells
            # a misconfigured channel apart from a Telegram outage.
            description = ""
            try:
                body = resp.json()
                if isinstance(body, dict):
                    description = str(body.get("description") or "")[:120]
            except Exception:
                description = ""
            return None, f"http_{resp.status_code}" + (f":{description}" if description else "")
        try:
            data = resp.json()
        except ValueError:
            return None, "bad_json"
        if not isinstance(data, dict):
            return None, "bad_json"
        if not data.get("ok"):
            return None, str(data.get("description") or "not_ok")
        result = data.get("result")
        if not isinstance(result, dict):
            return None, "bad_json"
        return result, None

    return None, "max_attempts_exceeded"


def verify_campaign_subscription(
    campaign: dict,
    telegram_user_id: int,
    *,
    force_refresh: bool = False,
) -> dict:
    """Confirm the given verified Telegram user id subscribes to a campaign's
    configured official channel. Returns a structured result dict; never
    raises for transient Telegram errors (fails closed to not_subscribed).

    ``subscribed`` stays the pass/fail boolean existing callers use;
    ``state`` (STATE_MEMBER / STATE_NOT_MEMBER / STATE_UNAVAILABLE) lets a
    caller distinguish a confirmed non-member from "could not verify"."""
    campaign_id = campaign.get("campaign_id", "")
    channel_id = resolve_channel_chat_id(campaign.get("telegram"))

    if not channel_id:
        return {"subscribed": False, "state": STATE_UNAVAILABLE,
                "reason": "channel_not_configured", "source": "config"}

    if not force_refresh:
        cached = _cache_get(channel_id, telegram_user_id)
        if cached is not None:
            _log_event(campaign_id=campaign_id, user_id=telegram_user_id, channel_id=channel_id,
                       result="pass" if cached else "fail", tg_status=None, source="cache", latency_ms=0)
            return {"subscribed": cached, "state": STATE_MEMBER if cached else STATE_NOT_MEMBER,
                    "reason": "cache", "source": "cache"}

    started = time.perf_counter()
    member, error = _get_chat_member_result(channel_id, telegram_user_id)
    latency_ms = int((time.perf_counter() - started) * 1000)
    status = (member or {}).get("status")

    if error:
        _log_event(campaign_id=campaign_id, user_id=telegram_user_id, channel_id=channel_id,
                   result="fail", tg_status=status, source="live", latency_ms=latency_ms, error=error)
        return {"subscribed": False, "state": STATE_UNAVAILABLE, "reason": error, "source": "live"}

    state = _classify_member(member)
    subscribed = state == STATE_MEMBER
    if subscribed:
        _cache_set(channel_id, telegram_user_id, True, DEFAULT_CACHE_TTL_S)
    _log_event(campaign_id=campaign_id, user_id=telegram_user_id, channel_id=channel_id,
               result="pass" if subscribed else "fail", tg_status=status, source="live", latency_ms=latency_ms,
               error=None if state != STATE_UNAVAILABLE else f"unknown_status:{status}")
    return {"subscribed": subscribed, "state": state, "reason": status or "unknown", "source": "live"}
