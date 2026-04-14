import unittest
from datetime import datetime, timedelta, timezone

import scheduler


class _UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class _DropsCollection:
    def __init__(self, docs):
        self.docs = [dict(doc) for doc in docs]

    def _matches(self, doc, filt):
        for key, value in filt.items():
            if isinstance(value, dict):
                if "$in" in value and doc.get(key) not in value["$in"]:
                    return False
                if "$ne" in value and doc.get(key) == value["$ne"]:
                    return False
                if "$lte" in value and not (doc.get(key) <= value["$lte"]):
                    return False
                if "$gt" in value and not (doc.get(key) > value["$gt"]):
                    return False
                continue
            if doc.get(key) != value:
                return False
        return True

    def update_many(self, filt, update):
        modified = 0
        for doc in self.docs:
            if not self._matches(doc, filt):
                continue
            before = dict(doc)
            for key, value in update.get("$set", {}).items():
                doc[key] = value
            if doc != before:
                modified += 1
        return _UpdateResult(modified)


class _Db:
    def __init__(self, docs):
        self.drops = _DropsCollection(docs)


class SchedulerDropStatusTests(unittest.TestCase):
    def test_reconcile_activates_upcoming_for_pooled_and_personalised(self):
        now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
        fake_db = _Db([
            {
                "_id": "pool-upcoming",
                "type": "pooled",
                "status": "upcoming",
                "startsAt": now - timedelta(minutes=1),
                "endsAt": now + timedelta(hours=1),
            },
            {
                "_id": "personal-upcoming",
                "type": "personalised",
                "status": "upcoming",
                "startsAt": now - timedelta(minutes=1),
                "endsAt": now + timedelta(hours=1),
            },
        ])

        orig_db = scheduler.db
        try:
            scheduler.db = fake_db
            out = scheduler.reconcile_drop_statuses(ref_now=now)
            self.assertEqual(out["activated"], 2)
            self.assertEqual(out["expired"], 0)
            statuses = {doc["_id"]: doc["status"] for doc in fake_db.drops.docs}
            self.assertEqual(statuses["pool-upcoming"], "active")
            self.assertEqual(statuses["personal-upcoming"], "active")
        finally:
            scheduler.db = orig_db

    def test_reconcile_expires_ended_drop(self):
        now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
        fake_db = _Db([
            {
                "_id": "already-ended",
                "type": "pooled",
                "status": "active",
                "startsAt": now - timedelta(hours=2),
                "endsAt": now - timedelta(seconds=1),
            }
        ])

        orig_db = scheduler.db
        try:
            scheduler.db = fake_db
            out = scheduler.reconcile_drop_statuses(ref_now=now)
            self.assertEqual(out["expired"], 1)
            self.assertEqual(fake_db.drops.docs[0]["status"], "expired")
        finally:
            scheduler.db = orig_db


if __name__ == "__main__":
    unittest.main()
