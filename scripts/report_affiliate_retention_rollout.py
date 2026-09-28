#!/usr/bin/env python3
"""READ-ONLY rollout report for the affiliate tier-reward retention gate.

Writes NOTHING — there is no apply mode, and no migration is required:

* Pre-deploy AFFILIATE_MONTHLY rows carry no ``retention_required_seconds``
  and keep their existing semantics exactly (ISSUED / OUT_OF_STOCK /
  REJECTED untouched; a non-final APPROVED / PENDING_MANUAL / SETTLING /
  PENDING_REVIEW row is still settled by the existing paths, WITHOUT the
  retention gate).
* Only entitlements created after deploy are gated.

So this reports how many existing non-final rows will keep settling
un-gated (the operator decision this rollout leaves open), plus the live
state of gated rows for post-deploy monitoring.

Usage
-----
    MONGO_URL=... python scripts/report_affiliate_retention_rollout.py
"""
from __future__ import annotations

import collections
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from affiliate_rewards import (  # noqa: E402
    FINAL_STATUSES,
    RETENTION_BROKEN_STATUS,
    RETENTION_HOLD_STATUSES,
    RETENTION_PENDING_STATUS,
    _as_aware_utc,
)


def build_report(db, *, now_utc: datetime | None = None) -> dict:
    now = _as_aware_utc(now_utc) or datetime.now(timezone.utc)
    legacy_non_final = collections.Counter()
    legacy_non_final_by_month = collections.Counter()
    gated = collections.Counter()
    matured_backlog = 0
    oldest_matured = None
    held_with_voucher = 0

    projection = {
        "status": 1, "year_month": 1, "retention_required_seconds": 1,
        "unlock_at": 1, "voucher_code": 1, "vouchers": 1,
    }
    for row in db.affiliate_ledger.find({"ledger_type": "AFFILIATE_MONTHLY"}, projection=projection):
        status = str(row.get("status") or "")
        if row.get("retention_required_seconds") is None:
            if status not in FINAL_STATUSES:
                legacy_non_final[status] += 1
                legacy_non_final_by_month[str(row.get("year_month"))] += 1
            continue
        gated[status] += 1
        if status in RETENTION_HOLD_STATUSES and (row.get("voucher_code") or row.get("vouchers")):
            held_with_voucher += 1
        unlock = _as_aware_utc(row.get("unlock_at"))
        if status == RETENTION_PENDING_STATUS and unlock is not None and unlock <= now:
            matured_backlog += 1
            oldest_matured = unlock if oldest_matured is None or unlock < oldest_matured else oldest_matured

    return {
        "generated_at": now.isoformat(),
        "pre_deploy_non_final_ungated": {
            "total": sum(legacy_non_final.values()),
            "by_status": dict(legacy_non_final),
            "by_year_month": dict(legacy_non_final_by_month),
        },
        "retention_gated": {
            "by_status": dict(gated),
            "pending_retention": gated.get(RETENTION_PENDING_STATUS, 0),
            "retention_broken": gated.get(RETENTION_BROKEN_STATUS, 0),
            "matured_backlog": matured_backlog,
            "oldest_matured_unlock_at": oldest_matured.isoformat() if oldest_matured else None,
        },
        "integrity": {"held_rows_with_voucher": held_with_voucher},
    }


def main() -> int:
    from scripts.verify_affiliate_reward_plan import _read_only_db

    report = build_report(_read_only_db())
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["integrity"]["held_rows_with_voucher"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
