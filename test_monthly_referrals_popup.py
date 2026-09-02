"""Regression tests for the profile/rank popup showing the current GMT+8
calendar month's referral count (monthly_referrals) instead of the lifetime
total (total_referrals).

Covers:
- settle_referral_snapshots() counts a same-month referral into
  monthly_referrals and excludes a previous-month referral.
- The Asia/Kuala_Lumpur (GMT+8) month boundary is honoured to the second.
- api_me_identity() returns monthly_referrals safely as 0 when the field is
  missing from the user doc, and still returns total_referrals unchanged
  (it stays stored/available, just no longer shown in the popup).
- The popup markup/JS in static/index.html renders the "Monthly Referrals"
  label from data.monthly_referrals, not the old "Total refs" /
  data.total_referrals wiring.
"""

import unittest
from datetime import datetime, timezone
from pathlib import Path

import scheduler
from test_referral_ledger_integrity import _FakeReferralEvents, _settled_doc
from test_identity_api import _load_symbols, _Req, _Users


def _fake_users():
    class _FakeUsersCol:
        def __init__(self):
            self.docs = {}

        def update_many(self, filt, update):
            if isinstance(update, list):
                for doc in self.docs.values():
                    for k, v in update[0].get("$set", {}).items():
                        doc[k] = doc.get(v[1:]) if isinstance(v, str) and v.startswith("$") else v
                return type("R", (), {"modified_count": len(self.docs)})()
            if "$set" in update:
                for doc in self.docs.values():
                    for k, v in update["$set"].items():
                        doc[k] = v
            if "$inc" in update:
                for doc in self.docs.values():
                    for k, v in update["$inc"].items():
                        doc[k] = doc.get(k, 0) + v
            return type("R", (), {"modified_count": len(self.docs)})()

        def bulk_write(self, updates, ordered=False):
            for op in updates:
                user_id = op._filter["user_id"]
                doc = self.docs.setdefault(user_id, {"user_id": user_id})
                for k, v in op._doc.get("$set", {}).items():
                    doc[k] = v

    return _FakeUsersCol()


class MonthlyReferralSnapshotBoundaryTests(unittest.TestCase):
    """Exercises the real settle_referral_snapshots() pipeline (the same
    _month_window_utc/_month_start_kl GMT+8 helper the affiliate reward
    system uses) rather than re-deriving the boundary in the test."""

    def setUp(self):
        self.orig_db = scheduler.db
        self.orig_now_utc = scheduler.now_utc

    def tearDown(self):
        scheduler.db = self.orig_db
        scheduler.now_utc = self.orig_now_utc

    def _run_settle(self, events, *, now_ts):
        users = _fake_users()
        users.docs[1] = {"user_id": 1}
        scheduler.db = type("DB", (), {"referral_events": events, "users": users})()
        scheduler.now_utc = lambda: now_ts
        scheduler.settle_referral_snapshots()
        return users.docs[1]

    def test_current_month_referral_is_counted(self):
        events = _FakeReferralEvents()
        # 2026-09-10 08:00 UTC == 2026-09-10 16:00 KL -- squarely inside Sept.
        events.insert_one(_settled_doc(1, 100, datetime(2026, 9, 10, 8, 0, tzinfo=timezone.utc)))
        doc = self._run_settle(events, now_ts=datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc))
        self.assertEqual(doc["monthly_referrals"], 1)
        self.assertEqual(doc["total_referrals"], 1)

    def test_previous_month_referral_is_excluded(self):
        events = _FakeReferralEvents()
        # 2026-08-20 -- previous month relative to a "now" in September.
        events.insert_one(_settled_doc(1, 100, datetime(2026, 8, 20, 8, 0, tzinfo=timezone.utc)))
        doc = self._run_settle(events, now_ts=datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc))
        self.assertEqual(doc["monthly_referrals"], 0)
        # Lifetime total is untouched by the month window.
        self.assertEqual(doc["total_referrals"], 1)

    def test_gmt8_month_boundary_is_honoured_to_the_second(self):
        events = _FakeReferralEvents()
        # 2026-08-31 15:59:59 UTC == 2026-08-31 23:59:59 KL -- last second of August.
        events.insert_one(_settled_doc(1, 100, datetime(2026, 8, 31, 15, 59, 59, tzinfo=timezone.utc)))
        # 2026-08-31 16:00:00 UTC == 2026-09-01 00:00:00 KL -- first instant of September.
        events.insert_one(_settled_doc(1, 101, datetime(2026, 8, 31, 16, 0, 0, tzinfo=timezone.utc)))
        doc = self._run_settle(events, now_ts=datetime(2026, 9, 1, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(doc["monthly_referrals"], 1)
        self.assertEqual(doc["total_referrals"], 2)


class IdentityApiMonthlyReferralsTests(unittest.TestCase):
    def test_monthly_referrals_present_and_matches_snapshot_field(self):
        env = _load_symbols()
        fn = env["api_me_identity"]
        env.update(
            {
                "request": _Req(),
                "extract_raw_init_data_from_query": lambda req: "ok",
                "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1001, "username": "tg_u"}}, "ok"),
                "users_collection": _Users(
                    {"user_id": 1001, "monthly_referrals": 7, "total_referrals": 165, "weekly_referrals": 21}
                ),
                "jsonify": lambda payload: payload,
                "json": __import__("json"),
            }
        )
        body = fn()
        self.assertEqual(body["monthly_referrals"], 7)
        # total_referrals stays in the payload (still stored/returned) -- the
        # popup simply stops rendering it, per requirement 6/req "remains stored".
        self.assertEqual(body["total_referrals"], 165)
        self.assertEqual(body["weekly_referrals"], 21)

    def test_missing_monthly_referrals_defaults_to_zero(self):
        env = _load_symbols()
        fn = env["api_me_identity"]
        env.update(
            {
                "request": _Req(),
                "extract_raw_init_data_from_query": lambda req: "ok",
                "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1001, "username": "tg_u"}}, "ok"),
                "users_collection": _Users({"user_id": 1001, "total_referrals": 40}),
                "jsonify": lambda payload: payload,
                "json": __import__("json"),
            }
        )
        body = fn()
        self.assertEqual(body["monthly_referrals"], 0)

    def test_missing_user_doc_defaults_monthly_referrals_to_zero(self):
        env = _load_symbols()
        fn = env["api_me_identity"]
        env.update(
            {
                "request": _Req(),
                "extract_raw_init_data_from_query": lambda req: "ok",
                "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 555, "username": "tg_user"}}, "ok"),
                "users_collection": _Users(None),
                "jsonify": lambda payload: payload,
                "json": __import__("json"),
            }
        )
        body = fn()
        self.assertEqual(body["monthly_referrals"], 0)


class IdentityPopupFrontendSourceTests(unittest.TestCase):
    """Source-scan of static/index.html: the popup element must be wired to
    monthly_referrals with the new label, and must no longer read
    data.total_referrals for that span."""

    def setUp(self):
        self.source = Path("static/index.html").read_text(encoding="utf-8")

    def test_popup_markup_label_is_monthly_referrals(self):
        self.assertIn('<span id="identity-total-ref">Monthly Referrals: 0</span>', self.source)
        self.assertNotIn('<span id="identity-total-ref">Total refs: 0</span>', self.source)

    def test_popup_js_renders_from_monthly_referrals_field(self):
        start = self.source.index("async function loadIdentityCard()")
        end = self.source.index("\n    }\n", start)
        body = self.source[start:end]
        self.assertIn(
            "totalRefEl.textContent = `Monthly Referrals: ${Number(data.monthly_referrals || 0)}`;",
            body,
        )
        self.assertNotIn("data.total_referrals || 0)}`", body)
        # Weekly Referrals / Total XP wiring is unchanged.
        self.assertIn(
            "weeklyRefEl.textContent = `Weekly Referrals: ${weeklyRef}`;",
            body,
        )
        self.assertIn(
            "totalXpEl.textContent = `Total XP: ${Number(data.total_xp || 0)}`;",
            body,
        )


if __name__ == "__main__":
    unittest.main()
