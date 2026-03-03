#!/usr/bin/env python3
"""One-time maintenance script to remove duplicate SIMULATED_PENDING affiliate ledgers.

Usage:
  MONGO_URL='mongodb://...' python scripts/dedupe_simulated_affiliate_ledgers.py [--db referral_bot] [--dry-run]
"""

from __future__ import annotations

import argparse
import os
from typing import Any

from pymongo import MongoClient


KEY_FIELDS = ["user_id", "invitee_user_id", "gate_day", "tier", "created_at"]


def _pick_keep_doc(docs: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        docs,
        key=lambda d: (
            d.get("evaluated_at_utc") or d.get("updated_at") or d.get("created_at"),
            d.get("_id"),
        ),
    )


def dedupe(*, mongo_url: str, db_name: str, dry_run: bool = False) -> tuple[int, int]:
    client = MongoClient(mongo_url)
    coll = client[db_name]["affiliate_ledger"]

    pipeline = [
        {
            "$match": {
                "simulate": True,
                "ledger_type": "AFFILIATE_SIMULATION",
                "status": "SIMULATED_PENDING",
                "user_id": {"$exists": True},
                "invitee_user_id": {"$exists": True},
                "gate_day": {"$exists": True},
                "tier": {"$exists": True},
                "created_at": {"$exists": True},
            }
        },
        {
            "$group": {
                "_id": {k: f"${k}" for k in KEY_FIELDS},
                "count": {"$sum": 1},
                "docs": {
                    "$push": {
                        "_id": "$_id",
                        "evaluated_at_utc": "$evaluated_at_utc",
                        "updated_at": "$updated_at",
                        "created_at": "$created_at",
                    }
                },
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]

    dup_groups = 0
    deleted = 0
    for group in coll.aggregate(pipeline, allowDiskUse=True):
        dup_groups += 1
        keep_doc = _pick_keep_doc(group.get("docs") or [])
        remove_ids = [d["_id"] for d in (group.get("docs") or []) if d.get("_id") != keep_doc.get("_id")]
        if remove_ids and not dry_run:
            res = coll.delete_many({"_id": {"$in": remove_ids}})
            deleted += int(res.deleted_count or 0)
        else:
            deleted += len(remove_ids)

    return dup_groups, deleted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="referral_bot")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is required")

    groups, deleted = dedupe(mongo_url=mongo_url, db_name=args.db, dry_run=args.dry_run)
    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"[{mode}] duplicate_groups={groups} deleted={deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
