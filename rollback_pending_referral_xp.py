"""Rollback XP granted for historical pending referrals during a rollback window.

This is a one-time maintenance script. It is idempotent and defaults to dry-run.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

from pymongo import MongoClient

from config import KL_TZ

ROLLBACK_START_AT = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
ROLLBACK_END_AT = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
ROLLBACK_CUTOFF = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

REFERRAL_SOURCES = {
    "referral",
    "ref_success",
    "ref_bonus_triplet",
    "ref_bonus",
}

INVALIDATION_REASON = "rollback_pending_referral"
AUDIT_COLLECTION = "xp_rollback_audit"


def _event_time_expr():
    return {"$ifNull": ["$created_at", "$ts"]}


def _week_window_utc(reference: datetime | None = None):
    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    start_local = (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_local = start_local + timedelta(days=7)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _month_window_utc(reference: datetime | None = None):
    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    start_local = ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start_local.month == 12:
        end_local = start_local.replace(year=start_local.year + 1, month=1)
    else:
        end_local = start_local.replace(month=start_local.month + 1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _valid_event_filter():
    return {"$or": [{"invalidated": {"$exists": False}}, {"invalidated": False}]}


def _parse_invitee_from_source_id(source_id) -> int | None:
    if not isinstance(source_id, str):
        return None
    parts = source_id.split(":")
    if len(parts) < 2:
        return None
    tail = parts[-1]
    if tail.isdigit():
        return int(tail)
    return None


def _pending_referral_sets(referrals: Iterable[dict]):
    pending_invitees = set()
    pending_referral_ids = set()
    for ref in referrals:
        invitee_id = ref.get("invitee_user_id") or ref.get("referred_user_id")
        if invitee_id is not None:
            pending_invitees.add(invitee_id)
        ref_id = ref.get("_id")
        if ref_id is not None:
            pending_referral_ids.add(ref_id)
            pending_referral_ids.add(str(ref_id))
    return pending_invitees, pending_referral_ids


def _is_linked_to_pending(entry, pending_invitees, pending_referral_ids) -> bool:
    source_id = entry.get("source_id")
    referral_id = entry.get("referral_id") or entry.get("referral") or entry.get("ref_id")
    invitee_id = entry.get("invitee_user_id") or entry.get("referred_user_id")

    if referral_id in pending_referral_ids or str(referral_id) in pending_referral_ids:
        return True
    if source_id in pending_referral_ids or str(source_id) in pending_referral_ids:
        return True
    if invitee_id in pending_invitees:
        return True

    invitee_from_key = _parse_invitee_from_source_id(source_id)
    return invitee_from_key in pending_invitees


def _find_xp_event(db, entry):
    xp_event_id = entry.get("xp_event_id")
    if xp_event_id:
        return db.xp_events.find_one({"_id": xp_event_id})

    user_id = entry.get("user_id")
    source_id = entry.get("source_id")
    if user_id is not None and source_id is not None:
        return db.xp_events.find_one({"user_id": user_id, "unique_key": source_id})
    return None


def _sum_xp(db, user_id: int) -> int:
    pipeline = [
        {"$match": {"user_id": user_id, **_valid_event_filter()}},
        {"$group": {"_id": "$user_id", "xp": {"$sum": "$xp"}}},
    ]
    rows = list(db.xp_events.aggregate(pipeline))
    return int(rows[0]["xp"]) if rows else 0


def _sum_xp_range(db, user_id: int, start_utc: datetime, end_utc: datetime) -> int:
    time_expr = {
        "$and": [
            {"$gte": [_event_time_expr(), start_utc]},
            {"$lt": [_event_time_expr(), end_utc]},
        ]
    }
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"user_id": user_id},
                    {"$expr": time_expr},
                    _valid_event_filter(),
                ]
            }
        },
        {"$group": {"_id": "$user_id", "xp": {"$sum": "$xp"}}},
    ]
    rows = list(db.xp_events.aggregate(pipeline))
    return int(rows[0]["xp"]) if rows else 0


def _recompute_user_totals(db, user_id: int, now_utc: datetime) -> dict:
    week_start, week_end = _week_window_utc(now_utc)
    month_start, month_end = _month_window_utc(now_utc)
    total_xp = _sum_xp(db, user_id)
    weekly_xp = _sum_xp_range(db, user_id, week_start, week_end)
    monthly_xp = _sum_xp_range(db, user_id, month_start, month_end)
    return {
        "xp": total_xp,
        "weekly_xp": weekly_xp,
        "monthly_xp": monthly_xp,
    }


def rollback_pending_referral_xp(db, dry_run: bool = True):
    if not (ROLLBACK_START_AT and ROLLBACK_END_AT and ROLLBACK_CUTOFF):
        raise SystemExit("Rollback window and cutoff must be configured")

    referrals = list(
        db.referrals.find(
            {
                "created_at": {"$lt": ROLLBACK_CUTOFF},
                "$or": [
                    {"status": {"$in": ["pending", "inactive"]}},
                    {"was_pending": True},
                ],
            }
        )
    )
    pending_invitees, pending_referral_ids = _pending_referral_sets(referrals)

    ledger_cursor = db.xp_ledger.find(
        {
            "source": {"$in": sorted(REFERRAL_SOURCES)},
            "created_at": {"$gte": ROLLBACK_START_AT, "$lte": ROLLBACK_END_AT},
            **_valid_event_filter(),
        }
    )

    now = datetime.now(timezone.utc)
    audit_collection = db[AUDIT_COLLECTION]
    to_invalidate = []
    affected_users = set()

    for entry in ledger_cursor:
        if not _is_linked_to_pending(entry, pending_invitees, pending_referral_ids):
            continue
        to_invalidate.append(entry)

    invalidated = 0
    xp_event_updates = 0
    audit_writes = 0

    for entry in to_invalidate:
        user_id = entry.get("user_id")
        if user_id is not None:
            affected_users.add(user_id)

        xp_event = _find_xp_event(db, entry)
        if dry_run:
            continue

        ledger_update = db.xp_ledger.update_one(
            {"_id": entry["_id"], **_valid_event_filter()},
            {
                "$set": {
                    "invalidated": True,
                    "invalidated_reason": INVALIDATION_REASON,
                    "invalidated_at": now,
                }
            },
        )
        if ledger_update.modified_count:
            invalidated += 1

        if xp_event:
            event_update = db.xp_events.update_one(
                {"_id": xp_event["_id"], **_valid_event_filter()},
                {
                    "$set": {
                        "invalidated": True,
                        "invalidated_reason": INVALIDATION_REASON,
                        "invalidated_at": now,
                    }
                },
            )
            if event_update.modified_count:
                xp_event_updates += 1

        audit_key_filter = {
            "xp_event_id": xp_event.get("_id") if xp_event else None,
            "reason": INVALIDATION_REASON,
        }
        if xp_event is None:
            audit_key_filter = {
                "ledger_id": entry["_id"],
                "reason": INVALIDATION_REASON,
            }

        audit_res = audit_collection.update_one(
            audit_key_filter,
            {
                "$setOnInsert": {
                    "user_id": user_id,
                    "xp_event_id": xp_event.get("_id") if xp_event else None,
                    "ledger_id": entry["_id"],
                    "reason": INVALIDATION_REASON,
                    "rolled_back_at": now,
                }
            },
            upsert=True,
        )
        if audit_res.upserted_id is not None:
            audit_writes += 1

    totals_updates = 0
    if not dry_run:
        for user_id in affected_users:
            totals = _recompute_user_totals(db, user_id, now)
            db.users.update_one({"user_id": user_id}, {"$set": totals})
            totals_updates += 1

    return {
        "dry_run": dry_run,
        "pending_referrals": len(referrals),
        "ledger_candidates": len(to_invalidate),
        "ledger_invalidated": invalidated,
        "xp_events_invalidated": xp_event_updates,
        "audit_records_written": audit_writes,
        "users_recomputed": totals_updates,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Rollback XP granted for historical pending referrals"
    )
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"))
    parser.add_argument("--commit", action="store_true", help="Apply changes")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("MONGO_URL is required")

    client = MongoClient(args.mongo_url)
    db = client["referral_bot"]

    summary = rollback_pending_referral_xp(db, dry_run=not args.commit)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
