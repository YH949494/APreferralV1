import unittest
from datetime import datetime, timedelta, timezone

import vouchers as vouchers_module
from vouchers import check_rejoin_buffer_for_pooled_claim


class FakeUsersCollection:
    def __init__(self, docs=None):
        self.docs = {d["user_id"]: dict(d) for d in (docs or [])}

    def find_one(self, filt, projection=None):
        uid = filt.get("user_id")
        doc = self.docs.get(uid)
        if not doc:
            return None
        if not projection:
            return dict(doc)
        return {k: doc.get(k) for k in projection if k in doc}


class RejoinBufferHelperTests(unittest.TestCase):
    def setUp(self):
        self.orig_users = vouchers_module.users_collection

    def tearDown(self):
        vouchers_module.users_collection = self.orig_users

    def test_first_time_subscriber_no_buffer_allows_claim(self):
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 1},
        ])
        result = check_rejoin_buffer_for_pooled_claim(1, datetime.now(timezone.utc))
        self.assertTrue(result["ok"])

    def test_missing_user_doc_allows_claim(self):
        vouchers_module.users_collection = FakeUsersCollection([])
        result = check_rejoin_buffer_for_pooled_claim(999, datetime.now(timezone.utc))
        self.assertTrue(result["ok"])

    def test_expired_buffer_allows_claim(self):
        now = datetime.now(timezone.utc)
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 2, "rejoin_buffer_until": now - timedelta(hours=1)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(2, now)
        self.assertTrue(result["ok"])

    def test_active_buffer_blocks_claim(self):
        now = datetime.now(timezone.utc)
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 3, "rejoin_buffer_until": now + timedelta(hours=5)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(3, now)
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "rejoin_buffer_active")
        self.assertEqual(result["reason"], "rejoin_buffer_active")
        self.assertGreater(result["retry_after_sec"], 0)
        self.assertLessEqual(result["retry_after_sec"], 5 * 3600)
        self.assertIn("rejoined @AdvantPlayOfficial", result["message"])

    def test_none_uid_allows_claim(self):
        vouchers_module.users_collection = FakeUsersCollection([])
        result = check_rejoin_buffer_for_pooled_claim(None, datetime.now(timezone.utc))
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
