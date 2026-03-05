# Run with: python -m unittest test_affiliate_monthly_highest_tier.py
import unittest
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from affiliate_rewards import (
    _month_window_utc,
    evaluate_monthly_affiliate_reward,
    T1_THRESHOLD,
    T2_THRESHOLD,
)


class FakeCollection:
    def __init__(self, unique_fields=None):
        self.docs = []
        self._id = 1
        self.unique_fields = unique_fields or []

    def _match_value(self, value, cond):
        if isinstance(cond, dict):
            for op, expected in cond.items():
                if op == "$gte" and not (value is not None and value >= expected):
                    return False
                if op == "$lt" and not (value is not None and value < expected):
                    return False
                if op == "$in" and value not in expected:
                    return False
                if op == "$nin" and value in expected:
                    return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for k, v in (filt or {}).items():
            if not self._match_value(doc.get(k), v):
                return False
        return True

    def insert_one(self, doc):
        for fields in self.unique_fields:
            for existing in self.docs:
                if all(existing.get(f) == doc.get(f) for f in fields):
                    raise DuplicateKeyError("duplicate")
        row = dict(doc)
        row.setdefault("_id", self._id)
        self._id += 1
        self.docs.append(row)
        return row

    def find_one(self, filt=None, proj=None):
        for d in self.docs:
            if self._match(d, filt or {}):
                if not proj:
                    return dict(d)
                return {k: d.get(k) for k in proj.keys()}
        return None

    def update_one(self, filt, update, upsert=False):
        for d in self.docs:
            if self._match(d, filt):
                for k, v in update.get("$set", {}).items():
                    d[k] = v
                return
        if upsert:
            row = dict(filt)
            for k, v in update.get("$setOnInsert", {}).items():
                row.setdefault(k, v)
            for k, v in update.get("$set", {}).items():
                row[k] = v
            self.insert_one(row)

    def count_documents(self, filt):
        return sum(1 for d in self.docs if self._match(d, filt))

    def find_one_and_update(self, filt, update, sort=None, return_document=None):
        matches = [d for d in self.docs if self._match(d, filt)]
        if not matches:
            return None
        if sort:
            key, direction = sort[0]
            matches.sort(key=lambda x: x.get(key, 0), reverse=(direction < 0))
        d = matches[0]
        for k, v in update.get("$set", {}).items():
            d[k] = v
        return dict(d)

    def aggregate(self, pipeline):
        rows = list(self.docs)
        for stage in pipeline:
            if "$match" in stage:
                rows = [r for r in rows if self._match(r, stage["$match"])]
            elif "$group" in stage:
                key_field = stage["$group"]["_id"].lstrip("$")
                grouped = {}
                for r in rows:
                    key = r.get(key_field)
                    grouped.setdefault(key, set()).add(r.get("invitee_id"))
                rows = [{"_id": k, "invitees": list(v)} for k, v in grouped.items()]
            elif "$project" in stage:
                rows = [{"count": len(r.get("invitees") or [])} for r in rows]
        return rows


class FakeDb:
    def __init__(self):
        self.users = FakeCollection(unique_fields=[("user_id",)])
        self.qualified_events = FakeCollection(unique_fields=[("invitee_id",)])
        self.affiliate_ledger = FakeCollection(unique_fields=[("dedup_key",)])
        self.voucher_pools = FakeCollection(unique_fields=[("pool_id", "code")])
        self.referral_audit = FakeCollection()


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
