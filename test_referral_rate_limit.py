# Run with: python -m unittest test_referral_rate_limit.py
import unittest
from datetime import datetime, timezone

from referral_rate_limit import consume_referral_rate_limits


class _FakeRateLimitCollection:
    def __init__(self):
        self.docs = {}

    def find_one_and_update(self, filt, update, upsert=False, return_document=None):
        key = filt["key"]
        doc = self.docs.get(key)
        if doc is None and upsert:
            doc = {}
            for k, v in update.get("$setOnInsert", {}).items():
                doc[k] = v
            doc.setdefault("count", 0)
            self.docs[key] = doc
        if doc is None:
            return None

        for field, inc_value in update.get("$inc", {}).items():
            doc[field] = int(doc.get(field, 0)) + int(inc_value)
        return dict(doc)


class ReferralRateLimitTests(unittest.TestCase):
    def setUp(self):
        self.col = _FakeRateLimitCollection()
        self.now = datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc)

    def test_hourly_limit_allows_first_20_then_blocks_21st(self):
        inviter_id = 123

        for _ in range(20):
            allowed, reason, _meta = consume_referral_rate_limits(
                self.col,
                inviter_id=inviter_id,
                now_utc=self.now,
                hourly_limit=20,
                daily_limit=200,
            )
            self.assertTrue(allowed)
            self.assertIsNone(reason)

        allowed, reason, meta = consume_referral_rate_limits(
            self.col,
            inviter_id=inviter_id,
            now_utc=self.now,
            hourly_limit=20,
            daily_limit=200,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "rate_limited_hour")
        self.assertEqual(meta["count"], 21)
        self.assertEqual(meta["limit"], 20)

        day_key = "ref:day:123:20260115"
        self.assertEqual(self.col.docs[day_key]["count"], 20)

    def test_daily_limit_blocks_201st(self):
        inviter_id = 456

        for _ in range(200):
            allowed, reason, _meta = consume_referral_rate_limits(
                self.col,
                inviter_id=inviter_id,
                now_utc=self.now,
                hourly_limit=0,
                daily_limit=200,
            )
            self.assertTrue(allowed)
            self.assertIsNone(reason)

        allowed, reason, meta = consume_referral_rate_limits(
            self.col,
            inviter_id=inviter_id,
            now_utc=self.now,
            hourly_limit=0,
            daily_limit=200,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "rate_limited_day")
        self.assertEqual(meta["count"], 201)
        self.assertEqual(meta["limit"], 200)


if __name__ == "__main__":
    unittest.main()
