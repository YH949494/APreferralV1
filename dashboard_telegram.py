"""Pure helpers for worker-cached Telegram member counts.

Telegram member counts can only be fetched from the worker process, where the
bot's asyncio polling loop is running. The web/gunicorn process has no loop, so
calling the Telegram API there raises ``RuntimeError("Bot loop not running
yet")``. To keep the admin dashboard fast and resilient, the worker periodically
refreshes the counts into ``admin_cache`` (doc id ``telegram_member_counts``)
and the dashboard reads that cached document only.

This module holds the pure logic for both sides — building the refreshed cache
document (preserving the last known count on per-metric failure) and reading a
cached entry into the dashboard's ``stale``/``cached_at`` shape — so it can be
unit-tested without a live bot or MongoDB.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable

# admin_cache document id holding the cached counts.
TELEGRAM_COUNTS_DOC_ID = "telegram_member_counts"

# A cached count older than this (or with ok=False) is reported as stale.
MEMBER_COUNT_STALE_AFTER_S = 7200  # 2 hours


def _as_dt(value: Any) -> datetime | None:
    """Coerce a stored ``updated_at`` (datetime or ISO string) to aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def refresh_member_counts(
    metrics: Iterable[tuple[str, Any]],
    fetcher: Callable[[Any], Any],
    *,
    existing: dict | None = None,
    now: datetime | None = None,
    logger: Any = None,
) -> dict:
    """Build the refreshed ``telegram_member_counts`` document.

    ``metrics`` is an iterable of ``(metric_name, chat_id)``. ``fetcher(chat_id)``
    returns the live count (and may raise). On a per-metric failure the previous
    count from ``existing`` is preserved and the entry is marked ``ok=False`` with
    the error recorded — a failing Telegram call never erases a known value.

    Returns a dict ``{"updated_at", "counts"}`` ready to ``$set`` into admin_cache.
    Never raises: each metric is isolated.
    """
    now = now or datetime.now(timezone.utc)
    existing_counts = (existing or {}).get("counts") or {}
    counts: dict[str, dict] = {}

    for name, chat_id in metrics:
        prev = existing_counts.get(name) or {}
        ok = True
        error = None
        try:
            if not chat_id:
                raise ValueError("chat_id not configured")
            count = int(fetcher(chat_id))
            counts[name] = {
                "chat_id": chat_id,
                "count": count,
                "updated_at": now,
                "ok": True,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001 - isolate per-metric failures
            ok = False
            error = str(exc)
            count = prev.get("count")
            counts[name] = {
                "chat_id": chat_id or prev.get("chat_id"),
                "count": count,  # keep last known value (may be None)
                "updated_at": prev.get("updated_at"),  # keep last success time
                "ok": False,
                "error": error,
                "error_at": now,
            }
        if logger is not None:
            logger.info(
                "[DASHBOARD_TG_REFRESH]\n"
                "metric=%s\n"
                "chat_id=%s\n"
                "ok=%s\n"
                "count=%s\n"
                "error=%s",
                name,
                chat_id,
                str(ok).lower(),
                count,
                error,
            )

    return {"updated_at": now, "counts": counts}


def read_member_count(
    entry: dict | None,
    *,
    now: datetime | None = None,
    stale_after_s: int = MEMBER_COUNT_STALE_AFTER_S,
) -> dict:
    """Read a single cached metric entry into the dashboard's display shape.

    Returns ``{"count", "stale", "cached_at", "status", "age_seconds"}`` where
    ``status`` is one of ``"missing" | "stale" | "ok"`` (for logging):

    - ``missing`` when there is no successful ``updated_at`` yet.
    - ``stale`` when the value is older than ``stale_after_s`` or ``ok`` is False.
    - ``ok`` when the value is fresh and the last refresh succeeded.
    """
    entry = entry or {}
    now = now or datetime.now(timezone.utc)
    count = entry.get("count")
    ok = bool(entry.get("ok", False))
    dt = _as_dt(entry.get("updated_at"))

    if dt is None:
        return {
            "count": count,
            "stale": True,
            "cached_at": None,
            "status": "missing",
            "age_seconds": None,
        }

    age = (now - dt).total_seconds()
    stale = age > stale_after_s or not ok
    return {
        "count": count,
        "stale": stale,
        "cached_at": _iso(dt),
        "status": "stale" if stale else "ok",
        "age_seconds": int(age),
    }
