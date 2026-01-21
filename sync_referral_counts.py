"""Audit users.total_referrals based on referral_events ledger.

Manual-run only. Safe to execute multiple times thanks to idempotent updates.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from heapq import nlargest

from pymongo import MongoClient, UpdateOne

def _flush_bulk(users_collection, ops, dry_run: bool) -> int:
    if dry_run or not ops:
        return 0
    result = users_collection.bulk_write(ops, ordered=False)
    return int(getattr(result, "modified_count", 0))


def sync_referral_counts(db, batch_size: int, dry_run: bool) -> dict:
    users_collection = db.users
    referral_events_collection = db.referral_events

    total_scanned = 0
    mismatched = 0
    fixed = 0
    deltas = []

    last_id = None
    while True:
        query = {"_id": {"$gt": last_id}} if last_id else {}
        batch = list(
            users_collection.find(
                query,
                {"_id": 1, "user_id": 1, "total_referrals": 1},
            )
            .sort("_id", 1)
            .limit(batch_size)
        )
        if not batch:
            break

        ops = []
        user_ids = [user.get("user_id") for user in batch if user.get("user_id") is not None]
        ledger_totals = {}
        if user_ids:
            pipeline = [
                {
                    "$match": {
                        "inviter_id": {"$in": user_ids},
                        "event": {"$in": ["referral_settled", "referral_revoked"]},
                    }
                },
                {
                    "$group": {
                        "_id": "$inviter_id",
                        "total": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$event", "referral_settled"]},
                                    1,
                                    -1,
                                ]
                            }
                        },
                    }
                },
            ]
            ledger_totals = {row["_id"]: int(row.get("total", 0)) for row in referral_events_collection.aggregate(pipeline)}        
        for user in batch:
            total_scanned += 1
            uid = user.get("user_id")
            if uid is None:
                continue
            computed_total = int(ledger_totals.get(uid, 0))
            stored_total = int(user.get("total_referrals", 0))
            if computed_total != stored_total:
                mismatched += 1
                delta = computed_total - stored_total
                deltas.append(
                    {
                        "user_id": uid,
                        "stored": stored_total,
                        "computed": computed_total,
                        "delta": delta,
                    }
                )
                if not dry_run:
                    ops.append(
                        UpdateOne(
                            {"_id": user["_id"]},
                            {
                                "$set": {
                                    "total_referrals": computed_total,
                                    "snapshot_updated_at": datetime.now(timezone.utc),
                                }
                            },
                        )
                    )

        fixed += _flush_bulk(users_collection, ops, dry_run)
        last_id = batch[-1]["_id"]

    top_deltas = nlargest(20, deltas, key=lambda item: abs(item["delta"]))

    return {
        "total_users_scanned": total_scanned,
        "users_mismatched": mismatched,
        "users_fixed": fixed,
        "top_20_deltas": top_deltas,
        "dry_run": dry_run,
    }


def main():
    parser = argparse.ArgumentParser(description="Sync users.total_referrals from referral_events ledger")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "referral_bot"), help="Mongo database name")
    parser.add_argument("--batch-size", type=int, default=300, help="Batch size for user scans")
    parser.add_argument("--commit", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("MONGO_URL is required")

    client = MongoClient(args.mongo_url)
    db = client[args.mongo_db]

    summary = sync_referral_counts(db, args.batch_size, dry_run=not args.commit)
    print(summary)


if __name__ == "__main__":
    main()
