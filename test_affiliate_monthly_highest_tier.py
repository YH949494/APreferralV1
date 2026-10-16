# Run with: python -m unittest test_affiliate_monthly_highest_tier.py
import unittest
from datetime import datetime, timezone

import mongomock

from affiliate_rewards import (
    _month_window_utc,
    evaluate_monthly_affiliate_reward,
    T1_THRESHOLD,
    T2_THRESHOLD,
)


def FakeDb():
    """In-memory Mongo with this suite's uniqueness constraints.

    The previous hand-rolled fake treated any operator it did not model
    ($and/$or/$exists, ...) as a match and returned None from update_one, so it
    could not exercise the settle/claim queries in
    evaluate_monthly_affiliate_reward. mongomock gives real query semantics.
    """
    db = mongomock.MongoClient(tz_aware=True)["affiliate_monthly_highest_tier"]
    db.users.create_index([("user_id", 1)], unique=True)
    db.qualified_events.create_index([("invitee_id", 1)], unique=True)
    db.affiliate_ledger.create_index([("dedup_key", 1)], unique=True)
    db.voucher_pools.create_index([("pool_id", 1), ("code", 1)], unique=True)
    return db


class AffiliateMonthlyHighestTierTests(unittest.TestCase):
    def test_creates_per_tier_ledgers_without_duplicates(self):
        db = FakeDb()
        uid = 1001
        db.users.insert_one({"user_id": uid, "blocked": False})

        now = datetime.now(timezone.utc)
        start_utc, end_utc, yyyymm = _month_window_utc(now)
        event_time = start_utc if start_utc <= now < end_utc else now

        for i in range(1, T1_THRESHOLD + 1):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": uid, "qualified_at": event_time})

        db.voucher_pools.insert_one({"pool_id": "T1", "code": "H1", "status": "available"})
        first = evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now)
        self.assertEqual(first["tier"], "T1")

        for i in range(T1_THRESHOLD + 1, T2_THRESHOLD + 1):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": uid, "qualified_at": event_time})

        db.voucher_pools.insert_one({"pool_id": "T2", "code": "H2", "status": "available"})
        second = evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now)
        dedup_key_t1 = f"AFF:{uid}:{yyyymm}:T1"
        dedup_key_t2 = f"AFF:{uid}:{yyyymm}:T2"

        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": dedup_key_t1}), 1)
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": dedup_key_t2}), 1)
        self.assertEqual(second["tier"], "T2")
        self.assertEqual(second["dedup_key"], dedup_key_t2)

    def test_finalized_tier_ledger_does_not_duplicate(self):
        db = FakeDb()
        uid = 1002
        db.users.insert_one({"user_id": uid, "blocked": False})

        now = datetime.now(timezone.utc)
        start_utc, end_utc, yyyymm = _month_window_utc(now)
        event_time = start_utc if start_utc <= now < end_utc else now

        for i in range(1, T1_THRESHOLD + 1):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": uid, "qualified_at": event_time})

        db.voucher_pools.insert_one({"pool_id": "T1", "code": "F1", "status": "available"})
        created = evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now)
        self.assertEqual(created["tier"], "T1")

        db.affiliate_ledger.update_one({"_id": created["_id"]}, {"$set": {"status": "ISSUED"}})

        for i in range(T1_THRESHOLD + 1, T2_THRESHOLD + 1):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": uid, "qualified_at": event_time})

        db.voucher_pools.insert_one({"pool_id": "T2", "code": "F2", "status": "available"})
        after = evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now)
        dedup_key_t1 = f"AFF:{uid}:{yyyymm}:T1"

        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": dedup_key_t1}), 1)
        self.assertEqual(after["tier"], "T2")
        self.assertEqual(db.affiliate_ledger.find_one({"dedup_key": dedup_key_t1})["status"], "ISSUED")


if __name__ == "__main__":
    unittest.main()
