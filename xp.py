"""Shared XP helpers.

This module centralizes XP grants so they are logged idempotently in
``xp_events``. Snapshot counters are updated by the scheduler worker.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from config import KL_TZ

logger = logging.getLogger(__name__)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_kl() -> datetime:
    return datetime.now(KL_TZ)

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
    
    user = db.users.find_one({"user_id": uid}, {"restrictions": 1})
    if user and user.get("restrictions", {}).get("no_xp"):
        return False

    # Hard gate on existing xp_events to avoid re-incrementing counters when
    # historical entries already exist but the ledger was missing.
    if db.xp_events.find_one({"user_id": uid, "unique_key": unique_key}):
        logger.info(
            "[XP] Duplicate grant ignored (existing event) uid=%s key=%s type=%s",
            uid,
            unique_key,
            event_type,
        )
        return False

    ledger_res = db.xp_ledger.update_one(
        {"user_id": uid, "source": event_type, "source_id": unique_key},
        {
            "$setOnInsert": {
                "user_id": uid,
                "source": event_type,
                "source_id": unique_key,
                "amount": amount,
                "created_at": now_kl(),
            }
        },
        upsert=True,
    )

    if not getattr(ledger_res, "upserted_id", None):
        logger.info(
            "[XP] Duplicate ledger insert ignored uid=%s key=%s type=%s",
            uid,
            unique_key,
            event_type,
        )
        return False
    
    res = db.xp_events.update_one(
        {"user_id": uid, "unique_key": unique_key},
        {
            "$setOnInsert": {
                "user_id": uid,
                "unique_key": unique_key,
                "type": event_type,
                "xp": amount,
                "created_at": now_kl(),
            }
        },
        upsert=True,
    )

    if not getattr(res, "upserted_id", None):
        logger.warning(
            "[XP] Ledger inserted but xp_event already existed uid=%s key=%s type=%s; rolling back ledger",
            uid,
            unique_key,
            event_type,
        )
        db.xp_ledger.delete_one({"user_id": uid, "source": event_type, "source_id": unique_key})       
        return False

    return True


def ensure_xp_indexes(db) -> None:
    """Ensure indexes used by XP bookkeeping are present."""

    def _dedupe_xp_events():
        dup_groups = db.xp_events.aggregate(
            [
                {"$match": {"unique_key": {"$exists": True}}},
                {
                    "$group": {
                        "_id": {"user_id": "$user_id", "unique_key": "$unique_key"},
                        "ids": {"$push": "$_id"},
                        "count": {"$sum": 1},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
            ]
        )

        removed = 0
        for group in dup_groups:
            to_delete = group["ids"][1:]
            if to_delete:
                db.xp_events.delete_many({"_id": {"$in": to_delete}})
                removed += len(to_delete)

        if removed:
            logger.warning("[XP] Removed %s duplicate xp_events", removed)
    
    _dedupe_xp_events()
    
    index_name = db.xp_events.create_index(
        [("user_id", 1), ("unique_key", 1)],
        name="uq_user_uniqueKey",
        unique=True,
        # Use $type to exclude null values from the unique constraint without
        # relying on $ne (which can be unsupported for partial indexes on some
        # MongoDB deployments).
        partialFilterExpression={"unique_key": {"$type": "string"}},
    )
    logger.info("[xp_indexes] ensure ok name=%s", index_name)
    index_name = db.xp_events.create_index(
        [("user_id", 1), ("created_at", -1)], name="user_createdAt"
    )
    logger.info("[xp_indexes] ensure ok name=%s", index_name)
    
    index_name = db.xp_ledger.create_index(
        [("user_id", 1), ("source", 1), ("source_id", 1)],
        name="uq_ledger_event",
        unique=True,
    )
    logger.info("[xp_indexes] ensure ok name=%s", index_name)
    index_name = db.xp_ledger.create_index(
        [("created_at", -1)], name="ledger_createdAt"
    )
    logger.info("[xp_indexes] ensure ok name=%s", index_name)
