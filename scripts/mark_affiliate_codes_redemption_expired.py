#!/usr/bin/env python3
"""Mark already-uploaded affiliate voucher codes as redemption-expired.

Why
---
``voucher_pools`` rows carry no expiry of their own, and a batch's
``starts_at``/``ends_at`` is NOT a redemption expiry (an ended batch is still
legitimately replenished and issued from for pinned ledgers). So when the
vendor retires a set of codes, the system has no way to know -- the Pending
Manual shortage summary keeps counting them as stock and the allocator keeps
issuing them. This stamps ``redemption_expires_at`` on exactly those rows;
``affiliate_rewards._redemption_valid_clause`` (the one shared predicate) then
excludes them from every claim and every stock count.

What it touches
---------------
ONLY ``status == "available"``, unreserved rows with no existing
``redemption_expires_at``, of the denomination pool(s) and entitlement month
given, uploaded BEFORE ``--created-before``. It adds four metadata fields
(``redemption_expires_at``, ``redemption_expiry_marked_at``,
``redemption_expiry_marked_by``, ``redemption_expiry_reason``) and nothing
else: ``code``, ``status``, ``batch_id``, ``created_at`` ... are never
rewritten, no row is deleted, and issued rows are never touched. Re-running is
a no-op (rows already stamped are skipped, an existing expiry is never
overwritten).

``--created-before`` is mandatory so replacement codes uploaded after the
cutoff can never be caught by an over-broad run. ``--expires-at`` is mandatory
and must already be in the past (there is no "now" default): a bulk retry that
began before the stamp holds an earlier clock and would still treat a code
stamped "now" as valid.

DRY RUN BY DEFAULT (read-only connection semantics: no writes, no index
creation, no codes printed). ``--commit`` additionally requires
``--expect-count N`` -- the total matched by the dry run you reviewed.

Usage
-----
    MONGO_URL=... python scripts/mark_affiliate_codes_redemption_expired.py \\
        --entitlement-month 202609 --denomination 5 --denomination 10 \\
        --created-before 2026-10-07T00:00:00+08:00 --expires-at 2026-10-01T00:00:00+08:00
    MONGO_URL=... python scripts/mark_affiliate_codes_redemption_expired.py \\
        --entitlement-month 202609 --denomination 5 --denomination 10 \\
        --created-before 2026-10-07T00:00:00+08:00 --expires-at 2026-10-01T00:00:00+08:00 \\
        --commit --expect-count 68
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import affiliate_rewards as ar  # noqa: E402
from affiliate_reward_plans import (  # noqa: E402
    DENOMINATION_PLAN_FIRST_MONTH,
    DENOMINATION_POOL_IDS,
    normalize_month,
    pool_denomination,
)

DEFAULT_REASON = "vendor_codes_expired"


def _parse_dt(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        raise ValueError("timestamps must carry a UTC offset, e.g. 2026-10-07T00:00:00+08:00")
    return dt.astimezone(timezone.utc)


def _selector(batch_id, pool_id: str, created_before: datetime) -> dict:
    return {
        "batch_id": batch_id,
        "pool_id": pool_id,
        "status": "available",
        "created_at": {"$lt": created_before},
        ar.REDEMPTION_EXPIRY_FIELD: None,       # matches missing or null; never overwrites
        "$or": [
            {"issued_for_ledger_id": {"$exists": False}},
            {"issued_for_ledger_id": None},
        ],
    }


def mark_redemption_expired(
    db, *, entitlement_month, pool_ids, created_before: datetime, expires_at: datetime | None = None,
    reason: str = DEFAULT_REASON, admin_identity: str = "script", commit: bool = False,
    now_utc: datetime | None = None,
) -> dict:
    """Returns a report; ``matched`` is what a commit would stamp, ``stamped``
    what it did stamp (0 on a dry run)."""
    now_utc = now_utc or datetime.now(timezone.utc)
    # The expiry must already be in the past. An allocator (notably a bulk retry,
    # which captures ONE clock for up to 5000 ledgers) that started before this
    # stamp would otherwise see ``expires_at > its now`` and still issue the code.
    if expires_at is None:
        raise ValueError("an explicit expires_at (when the codes actually expired) is required")
    if expires_at > now_utc:
        raise ValueError("expires_at must not be in the future")
    month = normalize_month(entitlement_month)
    if month is None or month < DENOMINATION_PLAN_FIRST_MONTH:
        raise ValueError("entitlement month must be YYYYMM, 202609 or later")
    start, end = ar._month_window_from_yyyymm(month)

    report = {"entitlement_month": month, "dry_run": not commit, "expires_at": expires_at.isoformat(),
              "created_before": created_before.isoformat(), "pools": {}, "matched": 0, "stamped": 0}
    plans = []
    for pool_id in pool_ids:
        batches = ar._find_batches_for_period(db, pool_id=pool_id, period_start_utc=start, period_end_utc=end)
        entry = {"denomination": pool_denomination(pool_id), "batch_id": None, "matched": 0, "stamped": 0,
                 "available_rows_in_batch": 0}
        report["pools"][pool_id] = entry
        if len(batches) != 1:
            entry["skipped"] = "no_batch_for_entitlement_period" if not batches else "target_batch_ambiguous"
            continue
        batch_id = batches[0]["_id"]
        entry["batch_id"] = str(batch_id)
        entry["available_rows_in_batch"] = int(db.voucher_pools.count_documents(
            {"batch_id": batch_id, "pool_id": pool_id, "status": "available"}))
        entry["matched"] = int(db.voucher_pools.count_documents(_selector(batch_id, pool_id, created_before)))
        report["matched"] += entry["matched"]
        plans.append((pool_id, batch_id, entry))

    if commit:
        for pool_id, batch_id, entry in plans:
            res = db.voucher_pools.update_many(
                _selector(batch_id, pool_id, created_before),
                {"$set": {
                    ar.REDEMPTION_EXPIRY_FIELD: expires_at,
                    "redemption_expiry_marked_at": now_utc,
                    "redemption_expiry_marked_by": admin_identity,
                    "redemption_expiry_reason": reason,
                }},
            )
            entry["stamped"] = int(getattr(res, "modified_count", 0))
            report["stamped"] += entry["stamped"]
    return report


def _write_db():
    """Primary connection WITHOUT database.init_db() (which creates indexes)."""
    from pymongo import MongoClient

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise SystemExit("MONGO_URL is not configured")
    return MongoClient(mongo_url)[os.environ.get("MONGO_DB", "referral_bot")]


def main(argv=None, *, db_factory=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--entitlement-month", required=True, metavar="YYYYMM")
    parser.add_argument("--denomination", type=int, action="append", required=True, choices=[5, 10, 50],
                        help="repeatable: 5, 10, 50")
    parser.add_argument("--created-before", required=True, metavar="ISO8601",
                        help="only rows uploaded before this instant (with UTC offset)")
    parser.add_argument("--expires-at", required=True, metavar="ISO8601",
                        help="when the codes actually expired (must already be in the past)")
    parser.add_argument("--reason", default=DEFAULT_REASON)
    parser.add_argument("--admin", default=os.environ.get("USER", "script"))
    parser.add_argument("--commit", action="store_true", help="write (default: dry run)")
    parser.add_argument("--expect-count", type=int, default=None, help="required with --commit")
    args = parser.parse_args(argv)

    try:
        created_before = _parse_dt(args.created_before)
        expires_at = _parse_dt(args.expires_at)
    except ValueError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    pool_ids = [p for p in DENOMINATION_POOL_IDS if pool_denomination(p) in set(args.denomination)]
    if args.commit and args.expect_count is None:
        print("refused: --commit requires --expect-count N (the matched total from the dry run)", file=sys.stderr)
        return 2

    db = (db_factory or _write_db)()
    kwargs = dict(entitlement_month=args.entitlement_month, pool_ids=pool_ids, created_before=created_before,
                  expires_at=expires_at, reason=args.reason, admin_identity=args.admin)
    try:
        preview = mark_redemption_expired(db, commit=False, **kwargs)
    except ValueError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if args.commit:
        if preview["matched"] != args.expect_count:
            print(f"refused: matched {preview['matched']} row(s), expected {args.expect_count}; "
                  "re-run the dry run and review", file=sys.stderr)
            print(json.dumps(preview, indent=2, sort_keys=True))
            return 2
        preview = mark_redemption_expired(db, commit=True, **kwargs)
    print(json.dumps(preview, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
