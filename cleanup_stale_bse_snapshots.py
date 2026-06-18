"""Cleanup script: delete stale backend_segment_snapshots written before the
coupon_redeem_time fix (PRs #259–#261).

These records have snapshot_period_source in {"upload_time", "manual_period",
"manual_fallback"} AND were bucketed into the engine's run-time week/month
(typically 2026-W25 / 2026-06) instead of the actual coupon redeem week.

Safety guarantees:
  - Never deletes records where snapshot_period_source = "coupon_redeem_time".
  - Dry-run by default: pass --confirm-delete to actually delete.
  - Prints a segment/period distribution before any deletion.

Usage:
    # Step 1 — inspect (safe, no writes):
    MONGO_URL=... python cleanup_stale_bse_snapshots.py

    # Step 2 — delete after reviewing step-1 output:
    MONGO_URL=... python cleanup_stale_bse_snapshots.py --confirm-delete
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# --------------------------------------------------------------------------
# Stale record criteria
# --------------------------------------------------------------------------
# Records are considered stale when ALL of:
#   (a) snapshot_period_source is a non-redeem-time source, AND
#   (b) snapshot_week matches the expected run-time bucket (2026-W25), AND
#   (c) snapshot_month matches the expected run-time bucket (2026-06).
#
# Condition (b)+(c) are a safety net — they ensure we only touch records that
# were clearly mis-bucketed into the engine run-time period, not any legitimate
# manual-fallback records for a different period.
# --------------------------------------------------------------------------

STALE_PERIOD_SOURCES = {"upload_time", "manual_period", "manual_fallback"}
STALE_WEEK = "2026-W25"
STALE_MONTH = "2026-06"

# Any record with this source is never touched, regardless of week/month.
SAFE_SOURCE = "coupon_redeem_time"


def _stale_filter() -> dict:
    return {
        "snapshot_period_source": {"$in": list(STALE_PERIOD_SOURCES)},
        "snapshot_week": STALE_WEEK,
        "snapshot_month": STALE_MONTH,
    }


def _get_collection(mongo_url: str):
    client = MongoClient(mongo_url, serverSelectionTimeoutMS=10_000)
    db_name = client.get_default_database().name if hasattr(client.get_default_database(), "name") else None
    if not db_name:
        # Parse db name from URI manually as fallback
        from urllib.parse import urlparse
        path = urlparse(mongo_url).path.lstrip("/")
        db_name = path.split("?")[0] if path else None
    if not db_name:
        print("ERROR: Cannot determine database name from MONGO_URL. "
              "Include the database name in the URI: mongodb://host/dbname", file=sys.stderr)
        sys.exit(1)
    return client[db_name]["backend_segment_snapshots"]


def run(*, confirm_delete: bool, mongo_url: str) -> None:
    print("=" * 60)
    print("BSE Stale Snapshot Cleanup")
    print("=" * 60)
    print(f"Target criteria:")
    print(f"  snapshot_period_source IN {sorted(STALE_PERIOD_SOURCES)}")
    print(f"  snapshot_week          = {STALE_WEEK}")
    print(f"  snapshot_month         = {STALE_MONTH}")
    print(f"  (records with snapshot_period_source='coupon_redeem_time' are NEVER touched)")
    print()

    try:
        col = _get_collection(mongo_url)
    except PyMongoError as exc:
        print(f"ERROR: Failed to connect — {exc}", file=sys.stderr)
        sys.exit(1)

    filt = _stale_filter()

    # ── 1. Count ──────────────────────────────────────────────────────────
    total_stale = col.count_documents(filt)
    total_all = col.count_documents({})
    safe_count = col.count_documents({"snapshot_period_source": SAFE_SOURCE})

    print(f"Total records in backend_segment_snapshots : {total_all:,}")
    print(f"  coupon_redeem_time (safe, never deleted)  : {safe_count:,}")
    print(f"  Matching stale filter (candidates)        : {total_stale:,}")
    print()

    if total_stale == 0:
        print("No stale records found. Nothing to delete.")
        return

    # ── 2. Segment distribution of stale records ──────────────────────────
    print("Segment distribution of stale records:")
    seg_counts: Counter = Counter()
    source_counts: Counter = Counter()
    for doc in col.find(filt, {"backend_segment": 1, "snapshot_period_source": 1, "_id": 0}):
        seg_counts[doc.get("backend_segment", "unknown")] += 1
        source_counts[doc.get("snapshot_period_source", "unknown")] += 1

    for seg, count in sorted(seg_counts.items()):
        print(f"  {seg:<30} {count:>6,}")
    print()
    print("Period source breakdown of stale records:")
    for src, count in sorted(source_counts.items()):
        print(f"  {src:<30} {count:>6,}")
    print()

    # ── 3. Confirm these are NOT coupon_redeem_time ───────────────────────
    redeem_time_overlap = col.count_documents({
        **filt,
        "snapshot_period_source": SAFE_SOURCE,
    })
    if redeem_time_overlap > 0:
        print(f"SAFETY ABORT: {redeem_time_overlap} records matched the stale filter "
              f"but also have snapshot_period_source='coupon_redeem_time'. "
              f"This should never happen. Review the filter before proceeding.", file=sys.stderr)
        sys.exit(1)
    print("Safety check passed: 0 coupon_redeem_time records in the stale set.")
    print()

    # ── 4. Delete (only with --confirm-delete) ────────────────────────────
    if not confirm_delete:
        print("DRY RUN — no records deleted.")
        print(f"To delete {total_stale:,} stale records, re-run with --confirm-delete.")
        return

    print(f"DELETING {total_stale:,} stale records …")
    try:
        result = col.delete_many(filt)
        print(f"Deleted: {result.deleted_count:,} records.")
    except PyMongoError as exc:
        print(f"ERROR during deletion — {exc}", file=sys.stderr)
        sys.exit(1)

    remaining = col.count_documents({})
    print(f"Records remaining in backend_segment_snapshots: {remaining:,}")
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete stale BSE snapshots mis-bucketed before coupon_redeem_time fix."
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Actually delete records. Without this flag the script is a dry run.",
    )
    parser.add_argument(
        "--mongo-url",
        default=os.getenv("MONGO_URL"),
        help="MongoDB connection URI (default: $MONGO_URL env var).",
    )
    args = parser.parse_args()

    if not args.mongo_url:
        print("ERROR: MONGO_URL is not set. Pass --mongo-url or set the env var.", file=sys.stderr)
        sys.exit(1)

    run(confirm_delete=args.confirm_delete, mongo_url=args.mongo_url)


if __name__ == "__main__":
    main()
