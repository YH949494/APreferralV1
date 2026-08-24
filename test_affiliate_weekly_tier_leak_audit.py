import unittest
from datetime import datetime, timedelta, timezone

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
        self.assertEqual(len(row["monthly_counterparts"]), 1)
        self.assertEqual(row["monthly_counterparts"][0]["voucher_count"], 5)
        self.assertEqual(
            sorted(row["monthly_counterparts"][0]["voucher_codes"]), [f"MONTHLY-T3-{i}" for i in range(1, 6)]
        )

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

    def test_finds_leaked_bundle_linked_only_via_issued_for_ledger_id(self):
        # affiliate_rewards._claim_voucher_from_pool links voucher_pools
        # rows via `issued_for_ledger_id` (string) as the canonical field,
        # with the legacy `ledger_id` as a fallback. A ledger whose own
        # status update lost a race (still SETTLING) but whose claim
        # actually landed must still be detected via the pool rows.
        db = FakeDb()
        uid = 333
        now = datetime(2026, 8, 20, tzinfo=timezone.utc)
        weekly = db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "user_id": uid,
                "week_key": "2026-08-17",
                "tier": "T2",
                "pool_id": "T2",
                "status": "SETTLING",
                "dedup_key": f"AFFW:{uid}:2026-08-17:T2",
                "voucher_code": None,
                "created_at": now,
                "updated_at": now,
            }
        )
        for i in range(1, 4):
            db.voucher_pools.insert_one(
                {
                    "pool_id": "T2",
                    "code": f"LOST-RACE-T2-{i}",
                    "status": "issued",
                    "issued_for_ledger_id": str(weekly["_id"]),
                }
            )

        report = build_report(db, now_ts=now)

        self.assertEqual(report["leaked_weekly_ledger_count"], 1)
        row = report["rows"][0]
        self.assertEqual(row["voucher_count"], 3)
        self.assertEqual(sorted(row["voucher_codes"]), [f"LOST-RACE-T2-{i}" for i in range(1, 4)])

    def test_non_issued_monthly_ledger_is_not_counted_as_a_double_bundle(self):
        # A PENDING_MANUAL/OUT_OF_STOCK/REJECTED monthly ledger with no
        # vouchers never actually gave the user a bundle — it must not
        # inflate double_bundle_count or be reported as a counterpart.
        db = FakeDb()
        uid = 444
        now = datetime(2026, 8, 20, tzinfo=timezone.utc)
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": uid,
                "year_month": "202608",
                "tier": "T1",
                "pool_id": "T1",
                "status": "PENDING_MANUAL",
                "dedup_key": f"AFF:{uid}:202608:T1",
                "voucher_code": None,
                "risk_flags": ["pool_empty"],
                "created_at": now,
                "updated_at": now,
            }
        )
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "user_id": uid,
                "week_key": "2026-08-17",
                "tier": "T1",
                "pool_id": "T1",
                "status": "ISSUED",
                "dedup_key": f"AFFW:{uid}:2026-08-17:T1",
                "voucher_count": 2,
                "vouchers": [{"code": "LEAK-T1-1"}, {"code": "LEAK-T1-2"}],
                "voucher_code": "LEAK-T1-1",
                "created_at": now,
                "updated_at": now,
            }
        )

        report = build_report(db, now_ts=now)

        self.assertEqual(report["leaked_weekly_ledger_count"], 1)
        self.assertEqual(report["double_bundle_count"], 0)
        row = report["rows"][0]
        self.assertFalse(row["has_monthly_counterpart_same_tier_month"])
        self.assertEqual(row["monthly_counterparts"], [])

    def test_cross_month_week_checks_both_overlapping_months(self):
        # A weekly window spanning Aug 31 - Sep 7 must check both August
        # and September for a monthly counterpart, not just the month the
        # week_key date falls in.
        db = FakeDb()
        uid = 555
        week_start = datetime(2026, 8, 31, tzinfo=timezone.utc)
        week_end = week_start + timedelta(days=7)
        now = datetime(2026, 9, 8, tzinfo=timezone.utc)

        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_WEEKLY",
                "user_id": uid,
                "week_key": "2026-08-31",
                "week_start_utc": week_start,
                "week_end_utc": week_end,
                "tier": "T1",
                "pool_id": "T1",
                "status": "ISSUED",
                "dedup_key": f"AFFW:{uid}:2026-08-31:T1",
                "voucher_count": 2,
                "vouchers": [{"code": "SEP-LEAK-T1-1"}, {"code": "SEP-LEAK-T1-2"}],
                "voucher_code": "SEP-LEAK-T1-1",
                "created_at": now,
                "updated_at": now,
            }
        )
        # The monthly counterpart is in September, not August.
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": uid,
                "year_month": "202609",
                "tier": "T1",
                "pool_id": "T1",
                "status": "ISSUED",
                "dedup_key": f"AFF:{uid}:202609:T1",
                "voucher_count": 2,
                "vouchers": [{"code": "SEP-MONTHLY-T1-1"}, {"code": "SEP-MONTHLY-T1-2"}],
                "voucher_code": "SEP-MONTHLY-T1-1",
                "created_at": now,
                "updated_at": now,
            }
        )

        report = build_report(db, now_ts=now)

        self.assertEqual(report["double_bundle_count"], 1)
        row = report["rows"][0]
        self.assertIn("202608", row["entitlement_months_checked"])
        self.assertIn("202609", row["entitlement_months_checked"])
        self.assertTrue(row["has_monthly_counterpart_same_tier_month"])
        self.assertEqual(row["monthly_counterparts"][0]["year_month"], "202609")


if __name__ == "__main__":
    unittest.main()
