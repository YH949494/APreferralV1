from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def tz_name(tz: Any) -> str:
    if tz is None:
        return "None"
    zone = getattr(tz, "zone", None)
    if zone:
        return zone
    key = getattr(tz, "key", None)
    if key:
        return key
    return str(tz)


def as_aware_utc(dt: datetime | str | None) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = dt.strip()
        if dt.endswith("Z"):
            dt = dt[:-1] + "+00:00"
        dt = datetime.fromisoformat(dt)        
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def expires_in_seconds(expire_at: datetime | None, now: datetime | None = None) -> float | None:
    if not expire_at:
        return None
    now_utc = as_aware_utc(now or datetime.now(timezone.utc))
    expires_utc = as_aware_utc(expire_at)
    if not now_utc or not expires_utc:
        return None
    return max(0.0, (expires_utc - now_utc).total_seconds())
