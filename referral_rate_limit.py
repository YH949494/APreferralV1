from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pymongo import ReturnDocument


def utc_hour_key(now_utc: datetime) -> str:
    return now_utc.strftime("%Y%m%d%H")


def utc_day_key(now_utc: datetime) -> str:
    return now_utc.strftime("%Y%m%d")


def _next_hour(now_utc: datetime) -> datetime:
    base = now_utc.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=1)


def _next_day(now_utc: datetime) -> datetime:
    base = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    return base + timedelta(days=1)


def consume_referral_rate_limits(
    collection,
    *,
    inviter_id: int,
    now_utc: datetime,
    hourly_limit: int,
    daily_limit: int,
):
    now_utc = now_utc.astimezone(timezone.utc)
    hour = utc_hour_key(now_utc)
    day = utc_day_key(now_utc)
    hour_key = f"ref:hour:{int(inviter_id)}:{hour}"
    day_key = f"ref:day:{int(inviter_id)}:{day}"

    hour_count = None
    day_count = None

    if hourly_limit > 0:
        hour_doc = collection.find_one_and_update(
            {"key": hour_key},
            {
                "$inc": {"count": 1},
                "$setOnInsert": {
                    "key": hour_key,
                    "scope": "hour",
                    "inviter_id": int(inviter_id),
                    "hour": hour,
                    "createdAt": now_utc,
                    "expireAt": _next_hour(now_utc) + timedelta(hours=2),
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        hour_count = int((hour_doc or {}).get("count", 0))
        if hour_count > hourly_limit:
            return False, "rate_limited_hour", {"key": hour_key, "count": hour_count, "limit": hourly_limit}

    if daily_limit > 0:
        day_doc = collection.find_one_and_update(
            {"key": day_key},
            {
                "$inc": {"count": 1},
                "$setOnInsert": {
                    "key": day_key,
                    "scope": "day",
                    "inviter_id": int(inviter_id),
                    "day": day,
                    "createdAt": now_utc,
                    "expireAt": _next_day(now_utc) + timedelta(days=2),
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        day_count = int((day_doc or {}).get("count", 0))
        if day_count > daily_limit:
            return False, "rate_limited_day", {"key": day_key, "count": day_count, "limit": daily_limit}

    return True, None, {
        "hour": {"key": hour_key, "count": hour_count, "limit": hourly_limit},
        "day": {"key": day_key, "count": day_count, "limit": daily_limit},
    }
