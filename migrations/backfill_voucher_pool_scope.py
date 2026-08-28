#!/usr/bin/env python3
"""Manual migration: backfill pool_type/allocation_scope/pool_source onto
pre-existing db.voucher_pools rows that predate the explicit voucher-pool
scope model (voucher_pool_service.py).

Not run automatically anywhere (no startup hook calls this). Dry-run by
default; requires an explicit --commit to write anything.

Scope of what this touches:
  - Only rows that already have NO "allocation_scope" field are candidates.
  - Every candidate is classified by pool_id against the reserved legacy
    ids (WELCOME, T1-T5) — the only pool_ids this repo can currently prove
    are affiliate-owned without guessing.
  - Rows whose pool_id is NOT in that reserved set are reported as
    "ambiguous" and never modified by this script, per the requirement to
    never infer campaign ownership from pool name alone. Reconcile those
    manually (register the pool_id in voucher_pool_registry with the
    correct scope via the admin API, or migrate_pool_scope, before running
    a targeted --pool-id backfill for it if it genuinely is affiliate
    inventory).

Usage:
  MONGO_URL='mongodb://...' python migrations/backfill_voucher_pool_scope.py [--db referral_bot] [--pool-id T1] [--commit]

Rollback:
  This migration only ever $set's three new fields (pool_type,
  allocation_scope, pool_source) on rows that previously lacked
  allocation_scope. To roll back, $unset those three fields on the same
  rows (they can be re-selected via the same --pool-id filter combined
  with pool_source: "affiliate_legacy_backfill", which this script also
  stamps specifically so a rollback query can find exactly the rows it
  touched and nothing else):

    db.voucher_pools.update_many(
        {"pool_source": "affiliate_legacy_backfill"},
        {"$unset": {"pool_type": "", "allocation_scope": "", "pool_source": ""}},
    )
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from database import init_db, get_db  # noqa: E402
from affiliate_reward_plans import DENOMINATION_POOL_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
logger = logging.getLogger("backfill_voucher_pool_scope")

RESERVED_LEGACY_POOL_IDS = frozenset(
    {"WELCOME", "T1", "T2", "T3", "T4", "T5"} | set(DENOMINATION_POOL_IDS)
)
BACKFILL_SOURCE = "affiliate_legacy_backfill"


def run(*, mongo_url: str, db_name: str, pool_id: str | None, commit: bool) -> dict:
    init_db(mongo_url, db_name)
    db = get_db()
    col = db["voucher_pools"]

    query = {"allocation_scope": {"$exists": False}}
    if pool_id:
        query["pool_id"] = pool_id

    candidates = list(col.find(query, {"_id": 1, "pool_id": 1, "code": 1}))

    reserved_ids = {row["pool_id"] for row in candidates} & RESERVED_LEGACY_POOL_IDS
    ambiguous_ids = {row["pool_id"] for row in candidates} - RESERVED_LEGACY_POOL_IDS

    reserved_count = sum(1 for row in candidates if row["pool_id"] in reserved_ids)
    ambiguous_count = sum(1 for row in candidates if row["pool_id"] in ambiguous_ids)

    report = {
        "total_candidates": len(candidates),
        "reserved_pool_ids_found": sorted(reserved_ids),
        "reserved_rows_to_backfill": reserved_count,
        "ambiguous_pool_ids_skipped": sorted(ambiguous_ids),
        "ambiguous_rows_skipped": ambiguous_count,
        "committed": False,
        "modified_count": 0,
    }

    logger.info(
        "[BACKFILL] candidates=%s reserved_rows=%s ambiguous_rows=%s ambiguous_pool_ids=%s",
        len(candidates), reserved_count, ambiguous_count, sorted(ambiguous_ids),
    )

    if not reserved_ids:
        logger.info("[BACKFILL] nothing to do — no reserved-legacy-pool rows need backfilling")
        return report

    if not commit:
        logger.info("[BACKFILL] DRY-RUN — would set pool_type=affiliate, allocation_scope=affiliate_rewards on %s row(s). Re-run with --commit to apply.", reserved_count)
        return report

    result = col.update_many(
        {"allocation_scope": {"$exists": False}, "pool_id": {"$in": sorted(reserved_ids)}},
        {"$set": {
            "pool_type": "affiliate",
            "allocation_scope": "affiliate_rewards",
            "pool_source": BACKFILL_SOURCE,
        }},
    )
    report["committed"] = True
    report["modified_count"] = result.modified_count
    logger.info("[BACKFILL] APPLIED modified_count=%s", result.modified_count)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.getenv("MONGO_DB_NAME", "referral_bot"))
    parser.add_argument("--pool-id", default=None, help="Limit to a single pool_id")
    parser.add_argument("--commit", action="store_true", help="Apply changes (default: dry-run report only)")
    args = parser.parse_args()

    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        logger.error("MONGO_URL is not set")
        return 1

    report = run(mongo_url=mongo_url, db_name=args.db, pool_id=args.pool_id, commit=args.commit)
    mode = "APPLY" if args.commit else "DRY-RUN"
    logger.info("[BACKFILL] mode=%s report=%s", mode, report)
    if report["ambiguous_rows_skipped"]:
        logger.warning(
            "[BACKFILL] %s row(s) across pool_ids %s were NOT touched — not in the reserved legacy set, "
            "ownership cannot be inferred from pool name alone. Reconcile manually.",
            report["ambiguous_rows_skipped"], report["ambiguous_pool_ids_skipped"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
