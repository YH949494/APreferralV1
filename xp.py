"""Shared XP helpers.

This module centralizes XP grants so they are logged idempotently in
``xp_events``. Snapshot counters are updated by the scheduler worker.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from config import KL_TZ

logger = logging.getLogger(__name__)

def _safe_create_index(collection, keys, *, name: str, partialFilterExpression=None):
    from database import safe_create_index

    return safe_create_index(
        collection,
        keys,
        name=name,
        partialFilterExpression=partialFilterExpression,
    )


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

    ledger_inserted = bool(getattr(ledger_res, "upserted_id", None))
    event_amount = amount
    event_created_at = now_kl()

    if not ledger_inserted:
        # The ledger row already existed. Two very different situations look
        # identical here, and they must not be treated the same:
        #
        #   1. A concurrent grant_xp() for the same (uid, unique_key) won the
        #      ledger upsert microseconds ago. That caller owns the grant, so
        #      this one must return False without granting.
        #   2. An *earlier* attempt inserted the ledger row and then died
        #      before reaching the xp_events insert below (worker killed,
        #      gunicorn timeout, transient Mongo error). xp_events is the
        #      canonical source for users.total_xp/weekly_xp/monthly_xp
        #      (see xp_snapshot.settle_xp_snapshots_incremental), so that XP
        #      was never actually credited — and returning False here made
        #      the loss permanent: the xp_events gate above keeps passing,
        #      this gate keeps failing, and no retry can ever heal it.
        #
        # Re-read xp_events to tell them apart. If the event now exists the
        # grant genuinely landed (case 1, or a plain duplicate) and we stop.
        # If it still doesn't, fall through and let the xp_events upsert
        # below be the authority — it is guarded by the unique index on
        # (user_id, unique_key), so a racing pair still produces exactly one
        # insert and one `upserted_id`.
        if db.xp_events.find_one({"user_id": uid, "unique_key": unique_key}):
            logger.info(
                "[XP] Duplicate ledger insert ignored uid=%s key=%s type=%s",
                uid,
                unique_key,
                event_type,
            )
            return False
        # Reconstruct the event from the ledger row the interrupted attempt
        # already committed, rather than minting a fresh grant with this
        # retry's parameters. This is completing *that* grant, so it must
        # carry that grant's amount and timestamp:
        #   * settle_xp_snapshots_incremental buckets weekly/monthly XP by the
        #     event's created_at, so a repair that lands after a week/month
        #     boundary would otherwise credit last period's XP to this one.
        #   * the caller's `amount` can legitimately differ between attempts
        #     (streak-dependent check-in bonuses, a changed
        #     FIRST_CHECKIN_BONUS_XP), which would leave the canonical event
        #     disagreeing with the ledger.
        # Fall back to this call's values only if the legacy ledger row is
        # missing those fields.
        existing_ledger = (
            db.xp_ledger.find_one(
                {"user_id": uid, "source": event_type, "source_id": unique_key}
            )
            or {}
        )
        if existing_ledger.get("amount") is not None:
            event_amount = int(existing_ledger["amount"])
        if existing_ledger.get("created_at") is not None:
            event_created_at = existing_ledger["created_at"]
        logger.warning(
            "[XP] Orphaned ledger row without xp_event uid=%s key=%s type=%s amount=%s; "
            "completing the interrupted grant instead of dropping it",
            uid,
            unique_key,
            event_type,
            event_amount,
        )

    res = db.xp_events.update_one(
        {"user_id": uid, "unique_key": unique_key},
        {
            "$setOnInsert": {
                "user_id": uid,
                "unique_key": unique_key,
                "type": event_type,
                "xp": event_amount,
                "created_at": event_created_at,
            }
        },
        upsert=True,
    )

    if not getattr(res, "upserted_id", None):
        # The upsert matched instead of inserting, so an xp_event for this key
        # is live and the XP is credited exactly once. This call must not
        # grant again — but it must also NOT delete the ledger row, even the
        # one it inserted itself.
        #
        # With the repair path above, the winning event may have been
        # completed by a *different* concurrent call that did not insert its
        # own ledger row (it found this one already there and reconstructed
        # the event from it). This row is then that live event's only ledger
        # entry, and deleting it would strip the audit/rollback/reconstruction
        # trail from a real grant — the mirror image of the orphan this
        # function exists to repair. Owning the original insert does not make
        # deletion safe once someone else's event is live.
        #
        # Leaving the row is strictly the safer failure mode: a redundant
        # ledger row is reconcilable audit noise, a missing one is lost history.
        logger.warning(
            "[XP] xp_event already existed for uid=%s key=%s type=%s "
            "(ledger_inserted_by_this_call=%s); ledger row left intact for the live event",
            uid,
            unique_key,
            event_type,
            ledger_inserted,
        )
        return False

    return True


def ensure_xp_indexes(db) -> None:
    """Ensure indexes used by XP bookkeeping are present."""

    def _dedupe_xp_events():
        dup_groups = db.xp_events.aggregate(
            [
                {
                    "$match": {
                        "unique_key": {"$exists": True},
                        "user_id": {"$exists": True, "$ne": None},
                    }
                },
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
    index_name = _safe_create_index(
        db.xp_events,
        [("unique_key", 1), ("user_id", 1)],
        name="xp_events_unique_key_user_id_idx",
        partialFilterExpression={"unique_key": {"$exists": True}, "user_id": {"$exists": True}},
    )
    logger.info("[xp_indexes] ensure ok name=%s", index_name)
    index_name = _safe_create_index(
        db.xp_events,
        [("user_id", 1), ("created_at", 1), ("invalidated", 1)],
        name="xp_events_user_created_invalidated_idx",
        partialFilterExpression={"user_id": {"$exists": True}},
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
