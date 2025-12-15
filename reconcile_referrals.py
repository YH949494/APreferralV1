"""Reconcile referral XP against referral successes.

Usage:
    python reconcile_referrals.py --output report.json

Outputs a JSON or CSV report with mismatches per user and prints a short
summary to stdout.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

from pymongo import MongoClient

from referral_rules import (
    BASE_REFERRAL_XP,
    REFERRAL_BONUS_EVENT,
    REFERRAL_BONUS_INTERVAL,
    REFERRAL_BONUS_XP,
    REFERRAL_SUCCESS_EVENT,
    reconcile_referrals,
)


def _load_collections(db):
    referrals = list(db.referrals.find({"status": "success"}))
    xp_events = list(
        db.xp_events.find(
            {"type": {"$in": [REFERRAL_SUCCESS_EVENT, REFERRAL_BONUS_EVENT, "ref_bonus"]}}
        )
    )
    return referrals, xp_events


def _write_report(path: Path, data):
    if path.suffix.lower() == ".csv":
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "user_id",
                    "ref_count",
                    "expected_xp",
                    "xp_from_referrals",
                    "delta",
                    "sample_refs",
                    "events",
                ],
            )
            writer.writeheader()
            for row in data:
                writer.writerow(row)
    else:
        with path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, default=str, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Reconcile referral XP events")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--output", type=Path, default=Path("reconciliation_report.json"))
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("MONGO_URL is required")

    client = MongoClient(args.mongo_url)
    db = client["referral_bot"]

    referrals, xp_events = _load_collections(db)
    mismatches = reconcile_referrals(referrals, xp_events)

    mismatches = sorted(mismatches, key=lambda r: abs(r.get("delta", 0)), reverse=True)
    _write_report(args.output, mismatches)

    user_count = len(mismatches)
    total_delta = sum(m.get("delta", 0) for m in mismatches)
    formula = (
        f"expected_xp = ref_count*{BASE_REFERRAL_XP} + "
        f"floor(ref_count/{REFERRAL_BONUS_INTERVAL})*{REFERRAL_BONUS_XP}"
    )

    print(
        json.dumps(
            {
                "mismatch_users": user_count,
                "total_delta": total_delta,
                "report_path": str(args.output),
                "expected_formula": formula,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
