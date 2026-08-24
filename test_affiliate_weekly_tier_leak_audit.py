import unittest
from datetime import datetime, timezone

from test_affiliate_rewards import FakeDb

from affiliate_weekly_tier_leak_audit import build_report


class AffiliateWeeklyTierLeakAuditTests(unittest.TestCase):
    def test_finds_the_confirmed_production_double_bundle_case(self):
        # Mirrors the confirmed production case: user 8961231447 received a
        # correct monthly T3 bundle AND a second, invalid weekly T3 bundle
        # for the same month, for a total of 10 T3 vouchers instead of 5.
        db = FakeDb()
        uid = 8961231447
        now = datetime(2026, 8, 20, tzinfo=timezone.utc)

        monthly_vouchers = [{"code": f"MONTHLY-T3-{i}"} for i in range(1, 6)]
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": uid,
                "year_month": "202608",
                "tier": "T3",
                "pool_id": "T3",
                "status": "ISSUED",
                "dedup_key": f"AFF:{uid}:202608:T3",
                "voucher_count": 5,
                "vouchers": monthly_vouchers,
                "voucher_code": "MONTHLY-T3-1",
                "created_at": now,
                "updated_at": now,
            }
        )
        weekly_vouchers = [{"code": f"WEEKLY-T3-{i}"} for i in range(1, 6)]
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "user_id": uid,
                "week_key": "2026-08-17",
                "tier": "T3",
                "pool_id": "T3",
                "status": "ISSUED",
                "dedup_key": f"AFFW:{uid}:2026-08-17:T3",
                "voucher_count": 5,
                "vouchers": weekly_vouchers,
                "voucher_code": "WEEKLY-T3-1",
                "created_at": now,
                "updated_at": now,
            }
        )

        report = build_report(db, now_ts=now)

        self.assertEqual(report["leaked_weekly_ledger_count"], 1)
        self.assertEqual(report["double_bundle_count"], 1)
        self.assertEqual(report["total_leaked_vouchers"], 5)
        row = report["rows"][0]
        self.assertEqual(row["user_id"], uid)
        self.assertEqual(row["tier"], "T3")
        self.assertEqual(row["week_key"], "2026-08-17")
        self.assertEqual(sorted(row["voucher_codes"]), [f"WEEKLY-T3-{i}" for i in range(1, 6)])
        self.assertTrue(row["has_monthly_counterpart_same_tier_month"])
        self.assertEqual(row["monthly_counterpart"]["voucher_count"], 5)
        self.assertEqual(sorted(row["monthly_counterpart"]["voucher_codes"]), [f"MONTHLY-T3-{i}" for i in range(1, 6)])

    def test_blocked_weekly_ledger_after_fix_is_not_reported(self):
        # After the fix, a weekly T1-T5 ledger ends as REJECTED with no
        # vouchers linked — it must never show up in the leak report.
        db = FakeDb()
        uid = 111
        now = datetime(2026, 8, 20, tzinfo=timezone.utc)
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "user_id": uid,
                "week_key": "2026-08-17",
                "tier": "T1",
                "pool_id": "T1",
                "status": "REJECTED",
                "review_reason": "weekly_tier_pool_blocked",
                "dedup_key": f"AFFW:{uid}:2026-08-17:T1",
                "voucher_code": None,
                "created_at": now,
                "updated_at": now,
            }
        )

        report = build_report(db, now_ts=now)

        self.assertEqual(report["leaked_weekly_ledger_count"], 0)
        self.assertEqual(report["rows"], [])

    def test_pending_manual_weekly_ledger_without_vouchers_is_not_reported(self):
        db = FakeDb()
        uid = 222
        now = datetime(2026, 8, 20, tzinfo=timezone.utc)
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "user_id": uid,
                "week_key": "2026-08-17",
                "tier": "T2",
                "pool_id": "T2",
                "status": "PENDING_MANUAL",
                "dedup_key": f"AFFW:{uid}:2026-08-17:T2",
                "voucher_code": None,
                "created_at": now,
                "updated_at": now,
            }
        )

        report = build_report(db, now_ts=now)

        self.assertEqual(report["leaked_weekly_ledger_count"], 0)


if __name__ == "__main__":
    unittest.main()
