import unittest
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

import scheduler


class _FakeReferralEvents:
    def __init__(self):
        self.keys = set()
        self.docs = []

    def insert_one(self, doc):
        key = (doc.get("inviter_id"), doc.get("invitee_id"), doc.get("event"))
        if key in self.keys:
            raise DuplicateKeyError("duplicate")
        self.keys.add(key)
        self.docs.append(dict(doc))

    def aggregate(self, pipeline):
        totals = {}
        for doc in self.docs:
            inviter_id = doc.get("inviter_id")
            if inviter_id is None:
                continue
            event = doc.get("event")
            if event not in {"referral_settled", "referral_revoked"}:
                continue
            sign = 1 if event == "referral_settled" else -1
            row = totals.setdefault(
                inviter_id,
                {"_id": inviter_id, "total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0},
            )
            row["total_referrals"] += sign
            row["weekly_referrals"] += sign
            row["monthly_referrals"] += sign
        return list(totals.values())


class _FakeUsers:
    def __init__(self):
        self.docs = {}
        self.update_one_calls = []

    def update_one(self, filt, update):
        self.update_one_calls.append((filt, update))

    def update_many(self, filt, update):
        if isinstance(update, list):
            stage = update[0].get("$set", {}) if update else {}
            modified = 0
            for doc in self.docs.values():
                for k, v in stage.items():
                    if isinstance(v, str) and v.startswith("$"):
                        doc[k] = doc.get(v[1:])
                    else:
                        doc[k] = v
                modified += 1
            return type("Result", (), {"modified_count": modified})()

        if "$set" in update:
            for doc in self.docs.values():
                for k, v in update["$set"].items():
                    doc[k] = v
        if "$inc" in update:
            for doc in self.docs.values():
                for k, v in update["$inc"].items():
                    doc[k] = doc.get(k, 0) + v
        return type("Result", (), {"modified_count": len(self.docs)})()

    def bulk_write(self, updates, ordered=False):
        for op in updates:
            user_id = op._filter["user_id"]
            doc = self.docs.setdefault(user_id, {"user_id": user_id})
            for k, v in op._doc.get("$set", {}).items():
                doc[k] = v


class _FakeDB:
    def __init__(self):
        self.referral_events = _FakeReferralEvents()
        self.users = _FakeUsers()


class SchedulerReferralEventTests(unittest.TestCase):
    def test_award_event_does_not_update_user_counters_and_snapshot_publishes(self):
        original_db = scheduler.db
        original_heartbeat = scheduler._write_snapshot_heartbeat
        fake_db = _FakeDB()
        scheduler.db = fake_db
        scheduler._write_snapshot_heartbeat = lambda source, ts: None
        try:
            now = datetime.now(timezone.utc)
            first = scheduler._record_referral_event(1, 200, "referral_settled", now)
            second = scheduler._record_referral_event(1, 200, "referral_settled", now)
            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual(fake_db.users.update_one_calls, [])

            scheduler.settle_referral_snapshots()
            self.assertIn(1, fake_db.users.docs)
            self.assertEqual(fake_db.users.docs[1]["total_referrals"], 1)
        finally:
            scheduler.db = original_db
            scheduler._write_snapshot_heartbeat = original_heartbeat


if __name__ == "__main__":
    unittest.main()
