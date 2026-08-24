"""Read-only historical audit: AFFILIATE_WEEKLY ledgers that leaked T1-T5
voucher bundles.

Background
----------
T1-T5 affiliate voucher bundles are a MONTHLY-only entitlement. Before the
fix in ``affiliate_rewards._issue_affiliate_ledger_from_pool`` /
``_claim_affiliate_bundle_from_pool``, the weekly reward evaluator
(``evaluate_weekly_affiliate_reward``) reused the same T1-T5 tier/pool
machinery as the monthly path and could issue a second full bundle to a
user who had already received (or would separately receive) the monthly
bundle for the same tier/month — e.g. user 8961231447 received 10 T3
vouchers (5 monthly + 5 weekly) instead of the intended maximum of 5.

This script is entirely separate from the production reward path. It is
never imported by scheduler.py/main.py/affiliate_rewards.py and never runs
automatically. It ONLY reads data — see "Explicitly not doing" below.

What it reports
----------------
Every ``affiliate_ledger`` row matching:
    ledger_type = "AFFILIATE_WEEKLY"
    tier/pool_id in {T1, T2, T3, T4, T5}
    status = "ISSUED"  OR  voucher_pools rows exist claiming ledger_id == this row

For each match:
    user_id
    weekly ledger_id
    week_key / entitlement week
    tier
    number of vouchers issued
    voucher codes
    issued_at
    whether the same user also had an AFFILIATE_MONTHLY ledger for the
    same tier/month (and if so, that ledger's status/voucher codes) —
    this is the "double bundle" signal from the confirmed production case.

Explicitly not doing
---------------------
This script never deletes voucher rows, recycles codes, modifies ledgers,
or reassigns/revokes anything. It is read-only in every mode. (There is no
--commit flag — nothing here writes to the database.)

Usage:
    python affiliate_weekly_tier_leak_audit.py
    python affiliate_weekly_tier_leak_audit.py --json
    python affiliate_weekly_tier_leak_audit.py --mongo-url mongodb://... --mongo-db referral_bot
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TIERS = ("T1", "T2", "T3", "T4", "T5")


def _pool_ledger_filter(ledger_id):
    """Same voucher-to-ledger match affiliate_rewards.py uses everywhere
    (see affiliate_rewards._pool_ledger_filter): a claimed voucher_pools
    row may be linked via the legacy `ledger_id` field or the canonical
    `issued_for_ledger_id` (stored as a string). Querying only one of the
    two silently misses rows linked through the other."""
    return {
        "$or": [
            {"issued_for_ledger_id": str(ledger_id)},
            {"ledger_id": ledger_id},
        ]
    }


def _iso(dt_value):
    if isinstance(dt_value, datetime):
        dt_value = dt_value if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc).isoformat()
    return dt_value


def _as_aware_utc(dt_value):
    if not isinstance(dt_value, datetime):
        return None
    return dt_value if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)


def _month_from_week_key(week_key: str | None) -> str | None:
    """Best-effort YYYYMM the weekly entitlement week *starts* in, derived
    from the week_key (an ISO date string, e.g. "2026-08-17"). Only used as
    a fallback when a ledger predates week_start_utc/week_end_utc being
    stored — see _entitlement_months_for_ledger for the real overlap
    calculation."""
    if not week_key:
        return None
    try:
        dt = datetime.fromisoformat(str(week_key))
    except ValueError:
        return None
    return f"{dt.year:04d}{dt.month:02d}"


def _entitlement_months_for_ledger(ledger: dict) -> list[str]:
    """Every calendar month (YYYYMM) touched by this weekly ledger's
    window. A week can straddle a month boundary (e.g. 2026-08-31 ..
    2026-09-07), so a single "which month is this week in" guess can miss
    the monthly counterpart entirely — check every month the window
    overlaps."""
    start = _as_aware_utc(ledger.get("week_start_utc"))
    end = _as_aware_utc(ledger.get("week_end_utc"))
    if start is None or end is None:
        single = _month_from_week_key(ledger.get("week_key"))
        return [single] if single else []

    months = []
    cursor = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # `end` is an exclusive window boundary; a week ending exactly at a
    # month start (e.g. 00:00:00 on the 1st) does not actually touch that
    # month, so stop before it.
    while cursor < end:
        yyyymm = f"{cursor.year:04d}{cursor.month:02d}"
        if yyyymm not in months:
            months.append(yyyymm)
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def find_leaked_weekly_ledgers(db) -> list[dict]:
    """Every AFFILIATE_WEEKLY ledger that carries (or claimed) a T1-T5
    bundle — status ISSUED, or with linked voucher_pools rows regardless of
    the ledger's current status field (covers a ledger whose status update
    lost a race after the claim succeeded)."""
    candidates = list(
        db.affiliate_ledger.find(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "$or": [
                    {"tier": {"$in": list(TIERS)}},
                    {"pool_id": {"$in": list(TIERS)}},
                ],
            }
        )
    )

    leaked = []
    for ledger in candidates:
        ledger_id = ledger.get("_id")
        vouchers = ledger.get("vouchers") or []
        voucher_codes = [v.get("code") for v in vouchers if v.get("code")]
        linked_pool_rows = list(
            db.voucher_pools.find(
                {"status": "issued", **_pool_ledger_filter(ledger_id)},
                {"code": 1, "pool_id": 1, "issued_at": 1},
            )
        )
        linked_codes = [row.get("code") for row in linked_pool_rows if row.get("code")]
        all_codes = sorted(set(voucher_codes) | set(linked_codes))

        is_issued_status = str(ledger.get("status") or "") == "ISSUED"
        has_linked_vouchers = bool(all_codes)
        if not is_issued_status and not has_linked_vouchers:
            continue

        leaked.append(
            {
                "ledger": ledger,
                "ledger_id": ledger_id,
                "voucher_codes": all_codes,
                "linked_pool_rows": linked_pool_rows,
            }
        )
    return leaked


def _issued_monthly_counterparts(db, *, user_id, tier: str, year_months: list[str]) -> list[dict]:
    """AFFILIATE_MONTHLY ledgers for this user/tier in any of the given
    months that actually issued a bundle (status ISSUED, or vouchers
    already linked despite a lagging status) — a PENDING_EOM,
    PENDING_MANUAL, OUT_OF_STOCK, or REJECTED-with-no-vouchers monthly
    ledger did not give this user a real double bundle and must not be
    reported as one."""
    if not year_months:
        return []
    candidates = list(
        db.affiliate_ledger.find(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": user_id,
                "tier": tier,
                "year_month": {"$in": year_months},
            }
        )
    )
    issued = []
    for monthly in candidates:
        monthly_vouchers = monthly.get("vouchers") or []
        linked_pool_rows = list(
            db.voucher_pools.find(
                {"status": "issued", **_pool_ledger_filter(monthly.get("_id"))},
                {"code": 1},
            )
        )
        codes = sorted(
            {v.get("code") for v in monthly_vouchers if v.get("code")}
            | {row.get("code") for row in linked_pool_rows if row.get("code")}
        )
        is_issued = str(monthly.get("status") or "") == "ISSUED" or bool(codes)
        if not is_issued:
            continue
        issued.append(
            {
                "ledger_id": str(monthly.get("_id")),
                "year_month": monthly.get("year_month"),
                "status": monthly.get("status"),
                "voucher_count": monthly.get("voucher_count") or len(codes),
                "voucher_codes": codes,
                "dedup_key": monthly.get("dedup_key"),
            }
        )
    return issued


def build_report(db, now_ts: datetime | None = None) -> dict:
    now_ts = now_ts or datetime.now(timezone.utc)
    leaked = find_leaked_weekly_ledgers(db)

    rows = []
    for item in leaked:
        ledger = item["ledger"]
        tier = str(ledger.get("tier") or ledger.get("pool_id") or "").strip().upper()
        user_id = ledger.get("user_id")
        week_key = ledger.get("week_key")
        entitlement_months = _entitlement_months_for_ledger(ledger)

        monthly_counterparts = _issued_monthly_counterparts(
            db, user_id=user_id, tier=tier, year_months=entitlement_months
        )

        rows.append(
            {
                "user_id": user_id,
                "weekly_ledger_id": str(item["ledger_id"]),
                "week_key": week_key,
                "entitlement_months_checked": entitlement_months,
                "tier": tier,
                "status": ledger.get("status"),
                "dedup_key": ledger.get("dedup_key"),
                "voucher_count": len(item["voucher_codes"]),
                "voucher_codes": item["voucher_codes"],
                "issued_at": _iso(ledger.get("issued_at") or ledger.get("updated_at")),
                "has_monthly_counterpart_same_tier_month": bool(monthly_counterparts),
                "monthly_counterparts": monthly_counterparts,
            }
        )

    rows.sort(key=lambda r: (str(r["user_id"]), r["week_key"] or "", r["tier"]))

    double_bundle_rows = [r for r in rows if r["has_monthly_counterpart_same_tier_month"]]

    return {
        "generated_at_utc": _iso(now_ts),
        "leaked_weekly_ledger_count": len(rows),
        "double_bundle_count": len(double_bundle_rows),
        "total_leaked_vouchers": sum(r["voucher_count"] for r in rows),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "referral_bot"), help="Mongo database name")
    parser.add_argument("--json", action="store_true", help="Print the report as JSON")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("--mongo-url or MONGO_URL is required")

    import database

    database.init_db(args.mongo_url, args.mongo_db)
    db = database.db

    report = build_report(db)

    if args.json:
        print(json.dumps(report, default=str, indent=2))
        return 0

    print("=== AFFILIATE_WEEKLY T1-T5 leak audit (read-only) ===")
    print(f"generated_at_utc: {report['generated_at_utc']}")
    print(f"leaked_weekly_ledger_count: {report['leaked_weekly_ledger_count']}")
    print(f"double_bundle_count (also has AFFILIATE_MONTHLY same tier/month): {report['double_bundle_count']}")
    print(f"total_leaked_vouchers: {report['total_leaked_vouchers']}")
    for row in report["rows"]:
        print(
            "  user_id={user_id} weekly_ledger_id={weekly_ledger_id} week_key={week_key} tier={tier} "
            "status={status} voucher_count={voucher_count} codes={voucher_codes} issued_at={issued_at} "
            "double_bundle={dbl}".format(
                user_id=row["user_id"],
                weekly_ledger_id=row["weekly_ledger_id"],
                week_key=row["week_key"],
                tier=row["tier"],
                status=row["status"],
                voucher_count=row["voucher_count"],
                voucher_codes=row["voucher_codes"],
                issued_at=row["issued_at"],
                dbl=row["has_monthly_counterpart_same_tier_month"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
