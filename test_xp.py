import re
import unittest

from xp import grant_xp
from database import update_user_xp
import xp as xp_module

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

    def test_ensure_xp_indexes_uses_safe_path_for_new_remediation_indexes(self):
        calls = []
        original_safe = xp_module._safe_create_index

        class _FakeAggEvents:
            def aggregate(self, pipeline):  # noqa: ARG002
                return []

            def delete_many(self, filt):  # noqa: ARG002
                return None

            def create_index(self, keys, **kwargs):
                return kwargs.get("name")

        class _FakeLedgerCollection:
            def create_index(self, keys, **kwargs):
                return kwargs.get("name")

        class _FakeIdxDB:
            def __init__(self):
                self.xp_events = _FakeAggEvents()
                self.xp_ledger = _FakeLedgerCollection()

        def _capture_safe(collection, keys, *, name, partialFilterExpression=None):
            calls.append((collection, tuple(keys), name, partialFilterExpression))
            return name

        xp_module._safe_create_index = _capture_safe
        try:
            xp_module.ensure_xp_indexes(_FakeIdxDB())
        finally:
            xp_module._safe_create_index = original_safe

        names = {c[2] for c in calls}
        self.assertIn("xp_events_unique_key_user_id_idx", names)
        self.assertIn("xp_events_user_created_invalidated_idx", names)


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


class GrantXPPartialFailureTests(unittest.TestCase):
    """Regression tests for the orphaned-ledger XP-loss defect.

    xp_events is the canonical source the snapshot worker sums into
    users.total_xp/weekly_xp/monthly_xp (xp_snapshot.settle_xp_snapshots_
    incremental). grant_xp() writes xp_ledger first and xp_events second, so
    a crash between the two leaves a ledger row with no event: the XP was
    never actually credited. Before the fix, every retry returned False on
    the "ledger already exists" check and the XP was lost forever.
    """

    def test_retry_after_crash_between_ledger_and_event_completes_the_grant(self):
        db = FakeDB()

        # Simulate the interrupted attempt: ledger row written, process died
        # before the xp_events insert.
        db.xp_ledger.update_one(
            {"user_id": 7, "source": "first_checkin", "source_id": "first_checkin"},
            {"$setOnInsert": {"amount": 200}},
            upsert=True,
        )
        self.assertEqual(len(db.xp_ledger.store), 1)
        self.assertEqual(len(db.xp_events.store), 0)

        granted = grant_xp(db, 7, "first_checkin", "first_checkin", 200)

        self.assertTrue(granted, "retry must complete the interrupted grant, not drop it")
        self.assertEqual(len(db.xp_events.store), 1)
        self.assertEqual(len(db.xp_ledger.store), 1, "the pre-existing ledger row must survive")
        event = db.xp_events.store[(7, "first_checkin")]
        self.assertEqual(event["xp"], 200)

    def test_repair_path_is_still_idempotent(self):
        db = FakeDB()
        db.xp_ledger.update_one(
            {"user_id": 7, "source": "checkin", "source_id": "checkin:20250101"},
            {"$setOnInsert": {"amount": 20}},
            upsert=True,
        )

        first = grant_xp(db, 7, "checkin", "checkin:20250101", 20)
        second = grant_xp(db, 7, "checkin", "checkin:20250101", 20)
        third = grant_xp(db, 7, "checkin", "checkin:20250101", 20)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertFalse(third)
        self.assertEqual(len(db.xp_events.store), 1)
        self.assertEqual(len(db.xp_ledger.store), 1)

    def test_concurrent_loser_does_not_delete_the_winners_ledger_row(self):
        """The repair path must not roll back a ledger row it did not create.

        Models the race: this call finds no xp_event and no free ledger slot
        (the winner took it), then loses the xp_events upsert too. It must
        return False and leave the winner's ledger row intact.
        """
        db = FakeDB()

        # Winner's completed grant.
        self.assertTrue(grant_xp(db, 9, "referral_award", "ref:9:123", 50))
        self.assertEqual(len(db.xp_ledger.store), 1)

        # Loser re-enters with a stale view: pretend the xp_event gate had not
        # yet observed the winner's event when it passed.
        real_find_one = db.xp_events.find_one
        calls = {"n": 0}

        def flaky_find_one(filt, projection=None):
            calls["n"] += 1
            if calls["n"] == 1:  # the initial duplicate gate only
                return None
            return real_find_one(filt, projection)

        db.xp_events.find_one = flaky_find_one
        granted = grant_xp(db, 9, "referral_award", "ref:9:123", 50)
        db.xp_events.find_one = real_find_one

        self.assertFalse(granted)
        self.assertEqual(len(db.xp_events.store), 1)
        self.assertEqual(
            len(db.xp_ledger.store), 1, "winner's ledger row must not be rolled back by the loser"
        )
