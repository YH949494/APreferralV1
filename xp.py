"""Shared XP helpers.

This module centralizes XP grants so they are logged idempotently in
``xp_events`` while updating user counters atomically.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def grant_xp(
    db,
    uid: int,
    event_type: str,
    unique_key: str,
    amount: int,
    inc_weekly: bool = True,
    inc_monthly: bool = True,
) -> bool:
    """Idempotently grant XP and log the event.

    Returns ``True`` only when the XP was newly granted. Duplicate attempts are
    ignored and logged.
    """

    res = db.xp_events.update_one(
        {"user_id": uid, "unique_key": unique_key},
        {
            "$setOnInsert": {
                "user_id": uid,
                "unique_key": unique_key,
                "type": event_type,
                "xp": amount,
                "created_at": now_utc(),
            }
        },
        upsert=True,
    )

    if not getattr(res, "upserted_id", None):
        logger.info(
            "[XP] Duplicate grant ignored uid=%s key=%s type=%s",
            uid,
            unique_key,
            event_type,
        )
        return False

    inc = {"xp": amount}
    if inc_weekly:
        inc["weekly_xp"] = amount
    if inc_monthly:
        inc["monthly_xp"] = amount

    db.users.update_one({"user_id": uid}, {"$inc": inc})
    return True


def ensure_xp_indexes(db) -> None:
    """Ensure indexes used by XP bookkeeping are present."""

    # Backward compatibility: clean up any existing indexes on
    # (user_id, unique_key) so we can recreate the partial unique index with the
    # correct name and options. Older deployments might have had the default
    # index name or a different definition, which would cause create_index to
    # raise OperationFailure on startup.
    target_key = {"user_id": 1, "unique_key": 1}
    for ix in db.xp_events.list_indexes():
        if ix.get("key") == target_key:
            try:
                db.xp_events.drop_index(ix["name"])
                logger.info("[XP] Dropped legacy index %s", ix["name"])
            except Exception:
                logger.exception("[XP] Failed dropping legacy xp_events index")

    db.xp_events.create_index(
        [("user_id", 1), ("unique_key", 1)],
        name="uq_user_uniqueKey",
        unique=True,
        partialFilterExpression={"unique_key": {"$ne": None}},
    )
    db.xp_events.create_index(
        [("user_id", 1), ("created_at", -1)], name="user_createdAt"
    )
