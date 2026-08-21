"""Read-only historical audit: find AFFILIATE_MONTHLY / AFFILIATE_WEEKLY
ledgers that hold more issued ``voucher_pools`` rows than their tier's
configured bundle size (``affiliate_rewards.AFFILIATE_REWARD_BUNDLES``).

Background: ``_issue_affiliate_ledger_from_pool`` is the single choke point
for every issuance path (initial tier issuance, weekly/monthly settlement,
PENDING_MANUAL retry, SETTLING recovery, admin approval, scheduler retries —
see affiliate_rewards.py). Before commit b502192 ("Fix affiliate bundle
reconciliation for stuck PENDING_MANUAL ledgers"), a ledger that already had
a *partial* set of issued voucher_pools rows linked to it (e.g. from a crash
between claiming vouchers and finalizing the ledger) could fall through into
a fresh full-bundle claim on top of those stray rows, producing more issued
codes for that ledger than its tier's configured bundle size. That gap is
now closed (bundle-completeness reconciliation + PARTIAL_BUNDLE_BLOCK, plus
a hard per-ledger claim-time cap added alongside this audit script), but any
ledger over-issued *before* the fix would still carry the excess today.

This script never mutates, returns, invalidates, or recycles anything — it
only reports. Cross-check its findings against the code-level guards before
taking any corrective action on a flagged ledger.

Usage:
    python affiliate_voucher_overissuance_audit.py [--json] [--tier T3]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _connect_db():
    from database import init_db, db

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is not configured")
    init_db(mongo_url)
    return db


def _iso(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def find_overissued_ledgers(db, *, tier_filter: str | None = None) -> list[dict]:
    """Scan every AFFILIATE_MONTHLY/AFFILIATE_WEEKLY ledger, count the
    voucher_pools rows actually linked to it (matching either
    ``issued_for_ledger_id`` or the legacy ``ledger_id`` field — the same
    ``_pool_ledger_filter`` shape issuance itself uses), and flag any ledger
    whose *any-status* linked-row count exceeds its tier's configured
    ``voucher_count``. Rows of every status are counted (not just
    ``issued``) so a row that was later rolled back to ``available`` still
    surfaces as historical evidence of an over-claim, with its current
    status reported alongside.
    """
    from affiliate_rewards import AFFILIATE_REWARD_BUNDLES

    findings = []
    ledger_query = {"ledger_type": {"$in": ["AFFILIATE_MONTHLY", "AFFILIATE_WEEKLY"]}}
    if tier_filter:
        ledger_query["tier"] = str(tier_filter).strip().upper()

    for ledger in db.affiliate_ledger.find(ledger_query):
        tier = str(ledger.get("tier") or "").strip().upper()
        spec = AFFILIATE_REWARD_BUNDLES.get(tier)
        if not spec:
            continue  # not a bundle tier (or malformed) — out of scope for this check
        expected_count = int(spec["voucher_count"])

        ledger_id = ledger.get("_id")
        rows = list(
            db.voucher_pools.find(
                {
                    "$or": [
                        {"issued_for_ledger_id": str(ledger_id)},
                        {"ledger_id": ledger_id},
                    ],
                }
            )
        )
        rows.sort(key=lambda r: r.get("_id"))
        actual_count = len(rows)
        if actual_count <= expected_count:
            continue

        bundle_codes = {
            str(v.get("code") or "").strip()
            for v in (ledger.get("vouchers") or [])
            if v.get("code")
        }
        excess_rows = [r for r in rows if str(r.get("code") or "").strip() not in bundle_codes]
        # If the ledger's own bundle payload doesn't identify which codes
        # belong to it (older ledgers, or never finalized), fall back to
        # "everything past the first `expected_count` rows by _id order" so
        # the report still highlights the extras deterministically.
        if not bundle_codes:
            excess_rows = rows[expected_count:]

        findings.append(
            {
                "ledger_id": str(ledger_id),
                "user_id": ledger.get("user_id"),
                "year_month": ledger.get("year_month"),
                "tier": tier,
                "ledger_type": ledger.get("ledger_type"),
                "ledger_status": ledger.get("status"),
                "expected_count": expected_count,
                "actual_count": actual_count,
                "excess_count": actual_count - expected_count,
                "voucher_codes": [str(r.get("code") or "") for r in rows],
                "excess_voucher_codes": [str(r.get("code") or "") for r in excess_rows],
                "voucher_pool_statuses": [
                    {"code": str(r.get("code") or ""), "status": r.get("status"), "batch_id": str(r.get("batch_id")) if r.get("batch_id") else None}
                    for r in rows
                ],
            }
        )

    findings.sort(key=lambda f: (-f["excess_count"], f["tier"], f["ledger_id"]))
    return findings


def build_report(db, *, tier_filter: str | None = None) -> dict:
    findings = find_overissued_ledgers(db, tier_filter=tier_filter)
    by_tier = defaultdict(lambda: {"ledgers": 0, "excess_vouchers": 0})
    for f in findings:
        by_tier[f["tier"]]["ledgers"] += 1
        by_tier[f["tier"]]["excess_vouchers"] += f["excess_count"]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tier_filter": tier_filter,
        "over_issued_ledger_count": len(findings),
        "total_excess_vouchers": sum(f["excess_count"] for f in findings),
        "by_tier": dict(by_tier),
        "findings": findings,
    }


def _print_human(report: dict) -> None:
    print(f"Affiliate voucher over-issuance audit — {report['generated_at_utc']}")
    print(f"Over-issued ledgers found: {report['over_issued_ledger_count']}")
    print(f"Total excess voucher rows: {report['total_excess_vouchers']}")
    for tier, stats in sorted(report["by_tier"].items()):
        print(f"  {tier}: {stats['ledgers']} ledger(s), {stats['excess_vouchers']} excess voucher(s)")
    print()
    for f in report["findings"]:
        print(
            f"ledger_id={f['ledger_id']} user_id={f['user_id']} year_month={f['year_month']} "
            f"tier={f['tier']} status={f['ledger_status']} expected={f['expected_count']} "
            f"actual={f['actual_count']} excess={f['excess_count']}"
        )
        print(f"    all_codes={f['voucher_codes']}")
        print(f"    excess_codes={f['excess_voucher_codes']}")
        print(f"    pool_row_statuses={f['voucher_pool_statuses']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="print the report as JSON instead of human-readable text")
    parser.add_argument("--tier", default=None, help="limit the scan to one tier, e.g. T3")
    args = parser.parse_args()

    db = _connect_db()
    report = build_report(db, tier_filter=args.tier)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_human(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
