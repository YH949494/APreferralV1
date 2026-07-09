import unittest
from datetime import datetime, timedelta, timezone

import reactivation_journey as journey


class _Result:
    def __init__(self, upserted_id=None, modified_count=0):
        self.upserted_id = upserted_id
        self.modified_count = modified_count


class _Cursor(list):
    def limit(self, n):
        return _Cursor(self[:n])

    def sort(self, *args, **kwargs):  # noqa: ARG002
        return self


_MISSING = object()


def _value(doc, key):
    return doc[key] if key in doc else _MISSING


def _match_value(value, condition):
    if isinstance(condition, dict):
        exists = value is not _MISSING
        if "$exists" in condition and bool(condition["$exists"]) != exists:
            return False
        if "$ne" in condition:
            if not exists:
                return condition["$ne"] is not None
            if value == condition["$ne"]:
                return False
        if "$in" in condition and (not exists or value not in condition["$in"]):
            return False
        if "$nin" in condition and exists and value in condition["$nin"]:
            return False
        if "$gte" in condition and (not exists or value is None or value < condition["$gte"]):
            return False
        if "$lte" in condition and (not exists or value is None or value > condition["$lte"]):
            return False
        if "$lt" in condition and (not exists or value is None or value >= condition["$lt"]):
            return False
        return True
    return value == condition


def _matches(doc, filt):
    for key, value in (filt or {}).items():
        if key == "$and":
            if not all(_matches(doc, branch) for branch in value):
                return False
            continue
        if key == "$or":
            if not any(_matches(doc, branch) for branch in value):
                return False
            continue
        if not _match_value(_value(doc, key), value):
            return False
    return True


class _Collection:
    def __init__(self, unique_fields=None):
        self.docs = []
        self.next_id = 1
        self.unique_fields = unique_fields or []

    def create_index(self, *args, **kwargs):  # noqa: ARG002
        return kwargs.get("name") or "idx"

    def find_one(self, filt, projection=None):  # noqa: ARG002
        for doc in self.docs:
            if _matches(doc, filt):
                return dict(doc)
        return None

    def find(self, filt=None, projection=None):  # noqa: ARG002
        return _Cursor([dict(doc) for doc in self.docs if _matches(doc, filt or {})])

    def count_documents(self, filt):
        return len([doc for doc in self.docs if _matches(doc, filt)])

    def insert_one(self, doc):
        for fields in self.unique_fields:
            for existing in self.docs:
                if all(existing.get(field) == doc.get(field) for field in fields):
                    raise journey.DuplicateKeyError("duplicate")
        new_doc = dict(doc)
        new_doc.setdefault("_id", self.next_id)
        self.next_id += 1
        self.docs.append(new_doc)
        return _Result(upserted_id=new_doc["_id"])

    def update_one(self, filt, update, upsert=False):
        for doc in self.docs:
            if _matches(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key, value in update.get("$setOnInsert", {}).items():
                    doc.setdefault(key, value)
                return _Result(modified_count=1)
        if not upsert:
            return _Result()
        new_doc = dict(filt)
        for key, value in update.get("$setOnInsert", {}).items():
            new_doc[key] = value
        for key, value in update.get("$set", {}).items():
            new_doc[key] = value
        return self.insert_one(new_doc)

    def find_one_and_update(self, filt, update, sort=None, return_document=None):  # noqa: ARG002
        matches = [doc for doc in self.docs if _matches(doc, filt)]
        if sort:
            for field, direction in reversed(sort):
                matches.sort(key=lambda d: d.get(field), reverse=direction < 0)
        if not matches:
            return None
        doc = matches[0]
        for key, value in update.get("$set", {}).items():
            doc[key] = value
        return dict(doc)


class _DB:
    def __init__(self):
        self.users = _Collection(unique_fields=[("user_id",)])
        self.reactivation_journey = _Collection(unique_fields=[("user_id", "campaign_id")])
        self.voucher_pools = _Collection(unique_fields=[("pool_id", "code")])
        self.xp_events = _Collection()

    def __getitem__(self, name):
        return getattr(self, name)


class ReactivationJourneyTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
        self.db = _DB()
        self.db.users.insert_one({"user_id": 10, "status": "Normal"})

    def _add_checkins(self, uid, count, start=None):
        start = start or self.now
        for idx in range(count):
            self.db.xp_events.insert_one(
                {
                    "user_id": uid,
                    "type": "checkin",
                    "unique_key": f"checkin:{idx}",
                    "created_at": start + timedelta(days=idx, hours=1),
                }
            )

    def test_journey_created_once_after_verification(self):
        first = journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)
        second = journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)

        self.assertEqual(first["code"], "created")
        self.assertEqual(second["code"], "reset")
        self.assertEqual(self.db.reactivation_journey.count_documents({}), 1)

    def test_tier1_issued_once_after_first_checkin(self):
        journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)
        journey.upload_pool_codes(self.db, "COMEBACK_T1", ["A1"], now_ref=self.now)

        first = journey.handle_successful_checkin(self.db, 10, now_ref=self.now + timedelta(hours=1), send_fn=lambda *a, **k: (True, None, False))
        second = journey.handle_successful_checkin(self.db, 10, now_ref=self.now + timedelta(hours=2), send_fn=lambda *a, **k: (True, None, False))

        self.assertEqual(first["voucher_code"], "A1")
        self.assertEqual(second["code"], "tier1_already_done")
        self.assertEqual(self.db.voucher_pools.count_documents({"pool_id": "COMEBACK_T1", "status": "issued"}), 1)

    def test_tier2_requires_five_unique_checkin_days(self):
        journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)
        self.db.voucher_pools.insert_one({"pool_id": "COMEBACK_T2", "code": "B1", "status": "available", "created_at": self.now})
        self.db.reactivation_journey.update_one({"user_id": 10}, {"$set": {"tier1_completed_at": self.now}})
        self._add_checkins(10, 4)

        early = journey.evaluate_pending_journeys(self.db, now_ref=self.now + timedelta(days=8), membership_checker=lambda uid: (True, "member"), send_fn=lambda *a, **k: (True, None, False))
        self._add_checkins(10, 5)
        ready = journey.evaluate_pending_journeys(self.db, now_ref=self.now + timedelta(days=8), membership_checker=lambda uid: (True, "member"), send_fn=lambda *a, **k: (True, None, False))

        self.assertEqual(early["tier2_issued"], 0)
        self.assertEqual(ready["tier2_issued"], 1)

    def test_tier3_requires_twenty_unique_checkin_days(self):
        journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)
        self.db.voucher_pools.insert_one({"pool_id": "COMEBACK_T3", "code": "C1", "status": "available", "created_at": self.now})
        self.db.reactivation_journey.update_one({"user_id": 10}, {"$set": {"tier1_completed_at": self.now, "tier2_completed_at": self.now + timedelta(days=8)}})
        self._add_checkins(10, 20)

        result = journey.evaluate_pending_journeys(self.db, now_ref=self.now + timedelta(days=31), membership_checker=lambda uid: (True, "member"), send_fn=lambda *a, **k: (True, None, False))
        doc = self.db.reactivation_journey.find_one({"user_id": 10})

        self.assertEqual(result["tier3_issued"], 1)
        self.assertEqual(doc["tier3_voucher_code"], "C1")
        self.assertEqual(doc["status"], "completed")

    def test_banned_user_gets_skipped(self):
        journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)
        self.db.users.update_one({"user_id": 10}, {"$set": {"is_banned": True}})

        result = journey.complete_tier(self.db, 10, 1, now_ref=self.now)

        self.assertEqual(result["code"], "blocked")
        self.assertEqual(self.db.reactivation_journey.find_one({"user_id": 10})["status"], "blocked")

    def test_pool_out_of_stock_does_not_crash(self):
        journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)

        result = journey.handle_successful_checkin(self.db, 10, now_ref=self.now)
        doc = self.db.reactivation_journey.find_one({"user_id": 10})

        self.assertEqual(result["code"], "out_of_stock")
        self.assertEqual(doc["tier1_voucher_status"], "OUT_OF_STOCK")
        self.assertIsNone(doc.get("tier1_voucher_code"))

    def test_upload_pool_codes_deduplicates_duplicate_codes(self):
        result = journey.upload_pool_codes(self.db, "COMEBACK_T1", ["code", "D1", "D1", "D2"], now_ref=self.now)

        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["duplicates"], 1)

    def test_dashboard_summary_returns_counts(self):
        journey.create_or_update_journey(self.db, 10, verified_at=self.now, now_ref=self.now)
        journey.upload_pool_codes(self.db, "COMEBACK_T1", ["S1"], now_ref=self.now)
        journey.handle_successful_checkin(self.db, 10, now_ref=self.now, send_fn=lambda *a, **k: (True, None, False))

        summary = journey.journey_summary(self.db)

        self.assertEqual(summary["tier1_completed"], 1)
        self.assertEqual(summary["tier1_issued"], 1)
        self.assertEqual(summary["pools"][0]["issued"], 1)


if __name__ == "__main__":
    unittest.main()
