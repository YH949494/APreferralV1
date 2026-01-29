import re
import unittest

from xp import grant_xp
from database import update_user_xp

class FakeResult:
    def __init__(self, upserted_id):
        self.upserted_id = upserted_id


class FakeXPEvents:
    def __init__(self):
        self.store = {}
        self.counter = 0

    def find_one(self, filt, projection=None):  # noqa: ARG002 - projection unused
        return self.store.get((filt.get("user_id"), filt.get("unique_key")))
    
    def update_one(self, filt, update, upsert=False):  # noqa: ARG002 - upsert unused
        key = (filt.get("user_id"), filt.get("unique_key"))
        if key in self.store:
            return FakeResult(None)

        self.counter += 1
        doc = {**filt, **update.get("$setOnInsert", {}), "_id": self.counter}
        self.store[key] = doc
        return FakeResult(self.counter)


class FakeUsers:
    def __init__(self):
        self.store = {}

    def find_one(self, filt, projection=None):  # noqa: ARG002 - projection unused
        if "user_id" in filt:
            return self.store.get(filt.get("user_id"))
        username_filter = filt.get("username")
        if isinstance(username_filter, dict):
            pattern = username_filter.get("$regex")
            flags = re.IGNORECASE if "i" in username_filter.get("$options", "") else 0
            for doc in self.store.values():
                username = doc.get("username")
                if username and re.match(pattern, username, flags):
                    return doc
        return None

    def update_one(self, filt, update):  # noqa: ARG002 - filt unused in stub
        uid = filt.get("user_id")
        doc = self.store.setdefault(
            uid,
            {"user_id": uid, "xp": 0, "weekly_xp": 0, "monthly_xp": 0},
        )
        inc = update.get("$inc", {})
        for field, delta in inc.items():
            doc[field] = doc.get(field, 0) + delta

class FakeLedger:
    def __init__(self):
        self.store = {}
        self.counter = 0

    def update_one(self, filt, update, upsert=False):  # noqa: ARG002 - upsert unused
        key = (filt.get("user_id"), filt.get("source"), filt.get("source_id"))
        if key in self.store:
            return FakeResult(None)
        self.counter += 1
        doc = {**filt, **update.get("$setOnInsert", {}), "_id": self.counter}
        self.store[key] = doc
        return FakeResult(self.counter)

    def delete_one(self, filt):  # noqa: ARG002 - filt unused in stub
        key = (filt.get("user_id"), filt.get("source"), filt.get("source_id"))
        self.store.pop(key, None)

class FakeDB:
    def __init__(self, users=None):
        self.xp_events = FakeXPEvents()
        self.users = users or FakeUsers()
        self.xp_ledger = FakeLedger()

class GrantXPTests(unittest.TestCase):
    def test_idempotent_grant(self):
        db = FakeDB()

        first = grant_xp(db, 1, "checkin", "checkin:20250101", 15)
        second = grant_xp(db, 1, "checkin", "checkin:20250101", 15)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(db.xp_events.store), 1)
        self.assertEqual(len(db.xp_ledger.store), 1)


class FakeAdminCooldowns:
    def insert_one(self, doc):  # noqa: ARG002 - doc unused in stub
        return None

    def delete_one(self, filt):  # noqa: ARG002 - filt unused in stub
        return None


class AdminXPTests(unittest.TestCase):
    def setUp(self):
        self.fake_users = FakeUsers()
        self.fake_users.store[1] = {
            "user_id": 1,
            "username": "Alice",
            "xp": 0,
            "weekly_xp": 0,
            "monthly_xp": 0,
        }
        self.fake_db = FakeDB(users=self.fake_users)

        import database as database_module

        self.database_module = database_module
        self.original_db = database_module.db
        self.original_users_collection = database_module.users_collection
        self.original_admin_cooldowns = database_module.admin_xp_cooldowns

        database_module.db = self.fake_db
        database_module.users_collection = self.fake_users
        database_module.admin_xp_cooldowns = FakeAdminCooldowns()

    def tearDown(self):
        self.database_module.db = self.original_db
        self.database_module.users_collection = self.original_users_collection
        self.database_module.admin_xp_cooldowns = self.original_admin_cooldowns

    def test_admin_xp_allows_multiple_unique_keys(self):
        first = update_user_xp("alice", 10, "adminui:1:alice:10:1")
        second = update_user_xp("alice", 10, "adminui:1:alice:10:2")

        self.assertTrue(first[0])
        self.assertTrue(second[0])
        self.assertEqual(len(self.fake_db.xp_events.store), 2)
        self.assertEqual(len(self.fake_db.xp_ledger.store), 2)

    def test_admin_xp_rejects_duplicate_key(self):
        first = update_user_xp("alice", 10, "adminui:1:alice:10:1")
        second = update_user_xp("alice", 10, "adminui:1:alice:10:1")

        self.assertTrue(first[0])
        self.assertFalse(second[0])
        self.assertEqual(len(self.fake_db.xp_events.store), 1)
        self.assertEqual(len(self.fake_db.xp_ledger.store), 1)


if __name__ == "__main__":
    unittest.main()
