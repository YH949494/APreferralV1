#!/usr/bin/env python3
"""Retention rollback: release affiliate tier rewards held by the 7-day gate.

DRY RUN BY DEFAULT. Without ``--commit`` this opens a read-only,
secondary-preferred connection (no index creation, no writes) and prints a
JSON report: held population, RELEASE / REVIEW_* / EXCLUDE_* counts, per-tier
and per-entitlement-month counts, voucher denomination demand, and claimable
stock in each entitlement month's own batch. No voucher codes are printed;
user ids are masked.

``--commit`` performs ``affiliate_reward_retention.release_retention_holds(
dry_run=False)``: RELEASE rows go held -> SETTLING -> canonical allocator,
REVIEW_* rows go to PENDING_REVIEW, EXCLUDE_* rows are untouched. It refuses
to run when:

* the retention gate is still enabled in THIS process's environment (run it
  inside the app machine, e.g. ``fly ssh console``, so that is the deployed
  configuration) — new holds would keep appearing behind the backfill;
* ``AFFILIATE_SIMULATE=1``;
* any EXCLUDE_INTEGRITY row exists (unless ``--allow-integrity-exclusions``);
* ``--expect-release N`` does not match the RELEASE count found now, so a
  commit only ever acts on the population an operator just reviewed.

Safe to re-run: a second commit finds nothing held to act on.

Usage
-----
    MONGO_URL=... python scripts/release_affiliate_retention_holds.py            # dry run
    MONGO_URL=... python scripts/release_affiliate_retention_holds.py --commit --expect-release 12
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import affiliate_reward_retention as rr  # noqa: E402
from affiliate_rewards import affiliate_retention_period  # noqa: E402


def _write_db():
    """Primary connection WITHOUT database.init_db() (which creates indexes)."""
    from pymongo import MongoClient

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise SystemExit("MONGO_URL is not configured")
    return MongoClient(mongo_url)[os.environ.get("MONGO_DB", "referral_bot")]


def _summary(report: dict) -> str:
    lines = [
        f"dry_run={report['dry_run']} include_broken={report['include_broken']} "
        f"simulate_mode={report['simulate_mode']} refused={report['refused']}",
        f"held_total={report['held_total']} by_status={report['held_by_status']}",
        "class_counts=" + json.dumps(report["class_counts"], sort_keys=True),
    ]
    for key, inv in sorted(report["inventory"].items()):
        lines.append(
            f"inventory {key}: required={inv['required']} "
            f"required_including_review={inv['required_including_review']} "
            f"available={inv['available']} shortfall={inv['shortfall']} source={inv['source']}"
        )
    if report.get("outcomes"):
        lines.append("outcomes=" + json.dumps(report["outcomes"], sort_keys=True))
    return "\n".join(lines)


def main(argv=None, *, db_factory=None, read_only_db_factory=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commit", action="store_true", help="perform the backfill (default: dry run)")
    parser.add_argument("--exclude-broken", action="store_true",
                        help="leave RETENTION_BROKEN rows held (default: include them)")
    parser.add_argument("--expect-release", type=int, default=None,
                        help="required with --commit: the RELEASE count from the reviewed dry run")
    parser.add_argument("--allow-integrity-exclusions", action="store_true",
                        help="commit even if EXCLUDE_INTEGRITY rows exist (they stay untouched)")
    parser.add_argument("--no-rows", action="store_true", help="omit per-row details from the JSON")
    parser.add_argument("--output", help="also write the JSON report to this path")
    args = parser.parse_args(argv)
    include_broken = not args.exclude_broken

    if not args.commit:
        if read_only_db_factory is None:
            from scripts.verify_affiliate_reward_plan import _read_only_db as read_only_db_factory
        report = rr.release_retention_holds(read_only_db_factory(), dry_run=True, include_broken=include_broken)
        return _emit(report, args, rc=0)

    if args.expect_release is None:
        print("refused: --commit requires --expect-release N (the RELEASE count from the dry run)", file=sys.stderr)
        return 2
    period = affiliate_retention_period()
    if period is not None:
        print(f"refused: retention gate still ENABLED here (AFFILIATE_REWARD_RETENTION_DAYS="
              f"{os.environ.get('AFFILIATE_REWARD_RETENTION_DAYS')!r}); disable it first", file=sys.stderr)
        return 2

    db = (db_factory or _write_db)()
    preview = rr.release_retention_holds(db, dry_run=True, include_broken=include_broken)
    counts = preview["class_counts"]
    if counts[rr.BACKFILL_EXCLUDE_INTEGRITY] and not args.allow_integrity_exclusions:
        print(f"refused: {counts[rr.BACKFILL_EXCLUDE_INTEGRITY]} EXCLUDE_INTEGRITY row(s); "
              "investigate first or pass --allow-integrity-exclusions", file=sys.stderr)
        return _emit(preview, args, rc=2)
    if counts[rr.BACKFILL_RELEASE] != args.expect_release:
        print(f"refused: RELEASE count is {counts[rr.BACKFILL_RELEASE]}, expected {args.expect_release}; "
              "re-run the dry run and review", file=sys.stderr)
        return _emit(preview, args, rc=2)

    report = rr.release_retention_holds(db, dry_run=False, include_broken=include_broken)
    rc = 0 if report["refused"] is None and not any(
        str(k).startswith("error_") for k in report.get("outcomes", {})
    ) else 1
    return _emit(report, args, rc=rc)


def _emit(report: dict, args, *, rc: int) -> int:
    payload = dict(report)
    if args.no_rows:
        payload.pop("rows", None)
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text)
    print(text)
    print(_summary(report), file=sys.stderr)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
