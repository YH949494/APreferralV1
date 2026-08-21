#!/usr/bin/env python3
"""One-time correction for T1-T4 affiliate voucher batches whose schedule
window was hand-typed as an approximation of a calendar month (e.g.
"2026-08-01 00:01" -> "2026-08-31 23:59") instead of the canonical KL
calendar-month boundary the claimability rule requires (full containment:
start = 1st 00:00:00 KL, end = 1st of next month 00:00:00 KL, exclusive).

A batch off by even one minute on either edge fails
``_find_batches_for_period``'s full-containment check, so
``get_claimable_pool_inventory``/``_resolve_monthly_ledger_target`` treat the
entitlement month as having no batch at all — reporting 0 claimable despite
the batch holding available stock.

This script only rewrites each affected batch's ``starts_at``/``ends_at``
(and the same denormalized fields on its ``voucher_pools`` rows) via the
existing ``affiliate_voucher_batches.update_batch`` — the same function the
Admin Dashboard's "Edit Batch Schedule" action uses, so the same safety
checks apply:
  - a batch with any already-issued voucher is skipped (schedule edits are
    refused once real claims have happened against a window);
  - the corrected window is checked for overlap against other batches for
    the same tier before it's applied;
  - voucher codes, their status, ownership, ledger linkage, and the batch's
    uploaded/available/issued counters are never touched.

Usage:
  MONGO_URL='mongodb://...' python scripts/fix_affiliate_batch_month_boundaries.py [--db referral_bot] [--month 202608] [--dry-run]

--month restricts the correction to a single "YYYYMM" entitlement month
(inferred from each batch's current starts_at); omit it to scan every T1-T4
batch. --dry-run reports what would change without writing anything.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from pymongo import MongoClient  # noqa: E402

from affiliate_voucher_batches import (  # noqa: E402
    ENTITLEMENT_MONTH_POOL_IDS,
    _as_aware_utc,
    _entitlement_month_for_batch,
    canonical_entitlement_month_window,
    update_batch,
)


def find_misaligned_batches(db, *, month: str | None = None) -> list[dict]:
    """T1-T4 batches whose stored window does not exactly equal the
    canonical entitlement-month boundary implied by their own starts_at.
    """
    query = {"pool_id": {"$in": list(ENTITLEMENT_MONTH_POOL_IDS)}}
    out = []
    for batch in db.affiliate_voucher_batches.find(query):
        yyyymm = _entitlement_month_for_batch(batch)
        if not yyyymm:
            continue
        if month and yyyymm != month:
            continue
        canonical_start, canonical_end = canonical_entitlement_month_window(yyyymm)
        if canonical_start is None or canonical_end is None:
            continue
        starts_at = _as_aware_utc(batch.get("starts_at"))
        ends_at = _as_aware_utc(batch.get("ends_at"))
        if starts_at == canonical_start and ends_at == canonical_end:
            continue
        out.append({
            "batch": batch,
            "entitlement_month": yyyymm,
            "current_starts_at": starts_at,
            "current_ends_at": ends_at,
            "canonical_starts_at": canonical_start,
            "canonical_ends_at": canonical_end,
        })
    return out


def fix_batches(db, *, admin_identity: str, month: str | None = None, dry_run: bool = False) -> list[dict]:
    results = []
    for item in find_misaligned_batches(db, month=month):
        batch = item["batch"]
        batch_id = batch["_id"]
        label = f"{batch.get('pool_id')} / {batch.get('batch_name')} ({batch_id})"
        row = {
            "batch_id": str(batch_id),
            "label": label,
            "entitlement_month": item["entitlement_month"],
            "before": (item["current_starts_at"], item["current_ends_at"]),
            "after": (item["canonical_starts_at"], item["canonical_ends_at"]),
        }
        if dry_run:
            row["result"] = "dry_run"
            results.append(row)
            continue
        result = update_batch(
            db,
            batch_id,
            admin_identity=admin_identity,
            updates={"entitlement_month": item["entitlement_month"]},
            now_utc=datetime.now(timezone.utc),
        )
        row["result"] = "ok" if result.get("ok") else result.get("code")
        results.append(row)
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.environ.get("MONGO_DB_NAME", "referral_bot"))
    parser.add_argument("--month", default=None, help="Restrict to one entitlement month, e.g. 202608")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        print("MONGO_URL is required", file=sys.stderr)
        return 1

    client = MongoClient(mongo_url)
    db = client[args.db]

    results = fix_batches(
        db, admin_identity="migration:fix_affiliate_batch_month_boundaries",
        month=args.month, dry_run=args.dry_run,
    )
    if not results:
        print("No misaligned T1-T4 batches found.")
        return 0

    for row in results:
        before_s, before_e = row["before"]
        after_s, after_e = row["after"]
        print(
            f"[{row['result']}] {row['label']} month={row['entitlement_month']} "
            f"before=({before_s} -> {before_e}) after=({after_s} -> {after_e})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
