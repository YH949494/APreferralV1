"""Repair users.referral_count based on valid referrals.

Manual-run only. Safe to execute multiple times thanks to idempotent updates.
"""

from __future__ import annotations

import argparse
import os
from heapq import nlargest

from pymongo import MongoClient, UpdateOne

from referral_rules import compute_referral_stats, ensure_referral_indexes


def _flush_bulk(users_collection, ops, dry_run: bool) -> int:
    if dry_run or not ops:
        return 0
    result = users_collection.bulk_write(ops, ordered=False)
    return int(getattr(result, "modified_count", 0))


def sync_referral_counts(db, batch_size: int, dry_run: bool) -> dict:
    users_collection = db.users
    referrals_collection = db.referrals
    ensure_referral_indexes(referrals_collection)

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
                {"_id": 1, "user_id": 1, "referral_count": 1},
            )
            .sort("_id", 1)
            .limit(batch_size)
        )
        if not batch:
            break

        ops = []
        for user in batch:
            total_scanned += 1
            uid = user.get("user_id")
            if uid is None:
                continue
            stats = compute_referral_stats(referrals_collection, uid)
            computed_total = int(stats.get("total_referrals", 0))
            stored_total = int(user.get("referral_count", 0))
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
                            {"$set": {"referral_count": computed_total}},
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
    parser = argparse.ArgumentParser(description="Sync users.referral_count from referrals collection")
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
