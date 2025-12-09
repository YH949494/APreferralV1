import unittest

from xp import grant_xp


class FakeResult:
    def __init__(self, upserted_id):
        self.upserted_id = upserted_id


class FakeXPEvents:
    def __init__(self):
        self.store = {}
        self.counter = 0

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

    def update_one(self, filt, update):  # noqa: ARG002 - filt unused in stub
        uid = filt.get("user_id")
        doc = self.store.setdefault(uid, {"xp": 0, "weekly_xp": 0, "monthly_xp": 0})
        inc = update.get("$inc", {})
        for field, delta in inc.items():
            doc[field] = doc.get(field, 0) + delta


class FakeDB:
    def __init__(self):
        self.xp_events = FakeXPEvents()
        self.users = FakeUsers()


class GrantXPTests(unittest.TestCase):
    def test_idempotent_grant(self):
        db = FakeDB()

        first = grant_xp(db, 1, "checkin", "checkin:20250101", 15)
        second = grant_xp(db, 1, "checkin", "checkin:20250101", 15)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(db.users.store[1]["xp"], 15)
        self.assertEqual(db.users.store[1]["weekly_xp"], 15)
        self.assertEqual(db.users.store[1]["monthly_xp"], 15)


if __name__ == "__main__":
    unittest.main()
