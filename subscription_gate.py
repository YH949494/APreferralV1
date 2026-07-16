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
import time
from datetime import datetime, timedelta, timezone

import requests

import database

logger = logging.getLogger(__name__)

SUBSCRIBED_STATUSES = {"member", "administrator", "creator"}
NOT_SUBSCRIBED_STATUSES = {"left", "kicked", "restricted"}

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
    try:
        database.db["campaign_events"].insert_one({
            "event": f"subscription_{result}",
            "campaign_id": campaign_id,
            "user_id": user_id,
            "channel_id": str(channel_id),
            "telegram_status": tg_status,
            "source": source,
            "latency_ms": latency_ms,
            "error": error,
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[SUBSCRIPTION_GATE] event_log_failed", exc_info=True)


def _get_chat_member(channel_id, user_id: int) -> tuple[str | None, str | None]:
    """Returns (status, error). Handles 429 with bounded retries."""
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
            return None, f"http_{resp.status_code}"
        try:
            data = resp.json()
        except ValueError:
            return None, "bad_json"
        if not data.get("ok"):
            return None, str(data.get("description") or "not_ok")
        return (data.get("result") or {}).get("status"), None

    return None, "max_attempts_exceeded"


def verify_campaign_subscription(
    campaign: dict,
    telegram_user_id: int,
    *,
    force_refresh: bool = False,
) -> dict:
    """Confirm the given verified Telegram user id subscribes to a campaign's
    configured official channel. Returns a structured result dict; never
    raises for transient Telegram errors (fails closed to not_subscribed)."""
    campaign_id = campaign.get("campaign_id", "")
    telegram_cfg = campaign.get("telegram") or {}
    channel_id = telegram_cfg.get("channel_id") or telegram_cfg.get("channel_username")

    if not channel_id:
        return {"subscribed": False, "reason": "channel_not_configured", "source": "config"}

    if not force_refresh:
        cached = _cache_get(channel_id, telegram_user_id)
        if cached is not None:
            _log_event(campaign_id=campaign_id, user_id=telegram_user_id, channel_id=channel_id,
                       result="pass" if cached else "fail", tg_status=None, source="cache", latency_ms=0)
            return {"subscribed": cached, "reason": "cache", "source": "cache"}

    started = time.perf_counter()
    status, error = _get_chat_member(channel_id, telegram_user_id)
    latency_ms = int((time.perf_counter() - started) * 1000)

    if error:
        _log_event(campaign_id=campaign_id, user_id=telegram_user_id, channel_id=channel_id,
                   result="fail", tg_status=status, source="live", latency_ms=latency_ms, error=error)
        return {"subscribed": False, "reason": error, "source": "live"}

    subscribed = status in SUBSCRIBED_STATUSES
    if subscribed:
        _cache_set(channel_id, telegram_user_id, True, DEFAULT_CACHE_TTL_S)
    _log_event(campaign_id=campaign_id, user_id=telegram_user_id, channel_id=channel_id,
               result="pass" if subscribed else "fail", tg_status=status, source="live", latency_ms=latency_ms)
    return {"subscribed": subscribed, "reason": status or "unknown", "source": "live"}
