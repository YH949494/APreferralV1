"""Snapshot publication time for users XP/referral counters, read side.

Before this, every 5-minute tick stamped *every* users document
(``snapshot_updated_at`` / ``snapshot_published_at`` / ``$inc
snapshot_version``) three times just to say "the snapshot was published",
whether or not that user's numbers changed — one oplog entry per user per
stamp. Publication time is a property of the run, not of each user.

It is already recorded once per publish: ``admin_cache.snapshot_heartbeat``
(``ts_utc``) is written by the XP and referral publishers immediately after
every successful publish, with the same ``now_utc_ts`` the per-user stamp
used, and by nothing else. So the per-user stamp is now written only when
that user's counters change, and a user's effective snapshot time is
``max(users.snapshot_updated_at, heartbeat.ts_utc)`` — exactly the value the
full-collection stamp used to hold. Readers go through
:func:`effective_snapshot_updated_at`.

Fail-safe: if the heartbeat write fails (it is logged, never raised), users
read as older than they are, i.e. stale — never falsely fresh.
Rollback-safe: older code ignores this and resumes stamping every user.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

PUBLISH_DOC_ID = "snapshot_heartbeat"
PUBLISH_FIELD = "ts_utc"
_CACHE_TTL_S = 5.0
# id(db) -> (monotonic expiry, published_at). A few seconds of staleness is
# irrelevant against the 900 s freshness threshold and keeps API reads at
# zero extra round-trips under load.
_cache: dict[int, tuple[float, datetime | None]] = {}


def _aware(value) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def last_published_at(db, *, use_cache: bool = True) -> datetime | None:
    """Time of the last successful snapshot publish, or None when unknown."""
    key = id(db)
    now_mono = time.monotonic()
    if use_cache:
        hit = _cache.get(key)
        if hit is not None and hit[0] > now_mono:
            return hit[1]
    try:
        doc = db.admin_cache.find_one({"_id": PUBLISH_DOC_ID})
    except Exception:
        return None
    value = _aware(doc.get(PUBLISH_FIELD)) if isinstance(doc, dict) else None
    _cache[key] = (now_mono + _CACHE_TTL_S, value)
    return value


def effective_snapshot_updated_at(db, user_snapshot_ts):
    """``max(user stamp, last publication)``; either side may be missing."""
    user_ts = _aware(user_snapshot_ts)
    published = last_published_at(db)
    if published is None:
        return user_snapshot_ts
    if user_ts is None:
        return published
    return max(user_ts, published)


def reset_cache() -> None:
    _cache.clear()
