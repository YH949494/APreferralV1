import unittest
from datetime import datetime, timedelta, timezone

import scheduler


class _VoucherClaimsCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    def find_one(self, filt=None, projection=None):
        filt = filt or {}
        uid = filt.get("user_id")
        window = filt.get("created_at") or {}
        for doc in self.docs:
            created_at = doc.get("created_at")
            if doc.get("user_id") != uid:
                continue
            if "$gte" in window and (created_at is None or created_at < window["$gte"]):
                continue
            if "$lte" in window and (created_at is None or created_at > window["$lte"]):
                continue
            return {"created_at": created_at}
        return None


class _FakeDb:
    def __init__(self, claim_docs=None):
        self.voucher_claims = _VoucherClaimsCollection(claim_docs)


class SchedulerEngagementTests(unittest.TestCase):
    def setUp(self):
        self.join = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.now = self.join + timedelta(hours=25)

    def test_only_checkin_is_not_qualified(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={"first_checkin_at": self.join + timedelta(hours=1)},
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb(),
        )
        self.assertEqual(payload["score"], 2)
        self.assertFalse(payload["qualified"])

    def test_checkin_plus_visible_qualifies(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={
                "first_checkin_at": self.join + timedelta(hours=1),
                "last_visible_at": self.join + timedelta(hours=2),
            },
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb(),
        )
        self.assertEqual(payload["score"], 3)
        self.assertTrue(payload["qualified"])

    def test_checkin_plus_claim_attempt_qualifies(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={"first_checkin_at": self.join + timedelta(hours=1)},
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb([{"user_id": 22, "created_at": self.join + timedelta(hours=2)}]),
        )
        self.assertEqual(payload["score"], 4)
        self.assertTrue(payload["qualified"])

    def test_visible_plus_claim_without_checkin_not_qualified(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={"last_visible_at": self.join + timedelta(hours=2)},
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb([{"user_id": 22, "created_at": self.join + timedelta(hours=3)}]),
        )
        self.assertEqual(payload["score"], 3)
        self.assertFalse(payload["qualified"])

    def test_miniapp_only_not_qualified(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={"last_visible_at": self.join + timedelta(hours=2)},
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb(),
        )
        self.assertEqual(payload["score"], 1)
        self.assertFalse(payload["qualified"])

    def test_claim_only_not_qualified(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={},
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb([{"user_id": 22, "created_at": self.join + timedelta(hours=3)}]),
        )
        self.assertEqual(payload["score"], 2)
        self.assertFalse(payload["qualified"])

    def test_signals_before_join_are_ignored(self):
        payload = scheduler.evaluate_referral_engagement(
            invitee_user_id=22,
            invitee_doc={
                "first_checkin_at": self.join - timedelta(minutes=1),
                "last_visible_at": self.join - timedelta(minutes=2),
            },
            window_start=self.join,
            window_end=self.now,
            db_ref=_FakeDb([{"user_id": 22, "created_at": self.join - timedelta(minutes=3)}]),
        )
        self.assertEqual(payload["score"], 0)
        self.assertFalse(payload["qualified"])


if __name__ == "__main__":
    unittest.main()
