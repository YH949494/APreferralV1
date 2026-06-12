"""Cached Telegram member-count lookups for the admin dashboard.

Pure helper with no telegram/mongo imports: the caller injects a ``fetcher``
callable that performs the actual Telegram API call. This keeps the 1-hour,
per-chat-id cache and the graceful stale-fallback behaviour in one place that
can be unit-tested without a live bot or database.
"""

from __future__ import annotations

import time as _time
from datetime import datetime, timezone
from typing import Any, Callable

MEMBER_CACHE_TTL_S = 3600  # 1 hour


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def get_member_count_cached(
    chat_id: Any,
    fetcher: Callable[[Any], Any],
    *,
    cache: dict,
    ttl: int = MEMBER_CACHE_TTL_S,
    now: float | None = None,
    logger: Any = None,
) -> dict:
    """Return ``{"count", "stale", "cached_at"}`` for a chat's member count.

    - Serves a fresh cached value when within ``ttl`` (no fetcher call).
    - Otherwise calls ``fetcher(chat_id)`` and refreshes the cache on success.
    - On fetcher failure, returns the last cached value with ``stale=True``,
      or ``{count: None, stale: True}`` when nothing is cached.

    Never raises: failures degrade to stale/None so the dashboard stays up.
    The ``cache`` dict is keyed by ``chat_id`` so multiple chats are cached
    independently.
    """
    if not chat_id:
        return {"count": None, "stale": True, "cached_at": None}

    now_ts = _time.time() if now is None else now
    cached = cache.get(chat_id)
    if cached and (now_ts - cached["ts"]) < ttl:
        return {"count": cached["count"], "stale": False, "cached_at": _iso(cached["ts"])}

    try:
        count = int(fetcher(chat_id))
        cache[chat_id] = {"count": count, "ts": now_ts}
        return {"count": count, "stale": False, "cached_at": _iso(now_ts)}
    except Exception as exc:  # noqa: BLE001 - intentional broad fallback
        if logger is not None:
            logger.warning("[DASHBOARD] tg_member_count failed chat_id=%s: %s", chat_id, exc)
        if cached:
            return {"count": cached["count"], "stale": True, "cached_at": _iso(cached["ts"])}
        return {"count": None, "stale": True, "cached_at": None}
