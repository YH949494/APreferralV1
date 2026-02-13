import unittest
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

import scheduler


class _FakeReferralEvents:
    def __init__(self):
        self.keys = set()

    def insert_one(self, doc):
        key = (doc.get("inviter_id"), doc.get("invitee_id"), doc.get("event"))
        if key in self.keys:
            raise DuplicateKeyError("duplicate")
        self.keys.add(key)


class _FakeUsers:
    def __init__(self):
        self.docs = {1: {"user_id": 1, "total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0}}

    def update_one(self, filt, update):
        doc = self.docs.setdefault(filt["user_id"], {"user_id": filt["user_id"]})
        for k, v in update.get("$inc", {}).items():
            doc[k] = doc.get(k, 0) + v
        for k, v in update.get("$max", {}).items():
            doc[k] = max(doc.get(k, 0), v)
        for k, v in update.get("$set", {}).items():
            doc[k] = v


class _FakeDB:
    def __init__(self):
        self.referral_events = _FakeReferralEvents()
        self.users = _FakeUsers()


class SchedulerReferralEventTests(unittest.TestCase):
    def test_duplicate_settled_does_not_double_increment_snapshot(self):
        original_db = scheduler.db
        fake_db = _FakeDB()
        scheduler.db = fake_db
        try:
            now = datetime.now(timezone.utc)
            first = scheduler._record_referral_event(1, 200, "referral_settled", now)
            second = scheduler._record_referral_event(1, 200, "referral_settled", now)
            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual(fake_db.users.docs[1]["total_referrals"], 1)
        finally:
            scheduler.db = original_db


if __name__ == "__main__":
    unittest.main()
