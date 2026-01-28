"""One-time backfill for pooled drop remaining counters."""

from __future__ import annotations

import argparse
import json 
import os

from pymongo import MongoClient


def backfill(db, *, dry_run: bool = True) -> dict:
    updated = 0
    for drop in db.drops.find({"type": "pooled"}):
        drop_id = str(drop["_id"])
        public = db.vouchers.count_documents(
            {
                "dropId": drop_id,
                "status": "free",
                "$or": [{"pool": "public"}, {"pool": {"$exists": False}}],
            }
        )
        my = db.vouchers.count_documents(
            {
                "dropId": drop_id,
                "status": "free",
                "pool": "my",
            }
        )
        if not dry_run:
            db.drops.update_one(
                {"_id": drop["_id"]},
                {"$set": {"public_remaining": public, "my_remaining": my}},
            )
        updated += 1
    return {"updated": updated, "dry_run": dry_run}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill drop public_remaining/my_remaining fields")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--commit", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("MONGO_URL is required")

    client = MongoClient(args.mongo_url)
    db = client["referral_bot"]

    summary = backfill(db, dry_run=not args.commit)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
