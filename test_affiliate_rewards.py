import unittest
from datetime import datetime, timedelta, timezone

from pymongo.errors import DuplicateKeyError

from affiliate_rewards import (
    evaluate_monthly_affiliate_reward,
    issue_welcome_bonus_if_eligible,
    mark_invitee_qualified,
)


class FakeCollection:
    def __init__(self, unique_fields=None):
        self.docs = []
        self._id = 1
        self.unique_fields = unique_fields or []

    def create_index(self, *args, **kwargs):
        return None

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
                if op == "$ne" and value == expected:
                    return False
                if op == "$exists":
                    exists = value is not None
                    if bool(expected) != exists:
                        return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for k, v in (filt or {}).items():
            if k == "$or":
                if not any(self._match(doc, sub) for sub in v):
                    return False
                continue
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
                for k, v in update.get("$inc", {}).items():
                    d[k] = d.get(k, 0) + v
                return
        if upsert:
            row = dict(filt)
            for k, v in update.get("$setOnInsert", {}).items():
                row.setdefault(k, v)
            for k, v in update.get("$set", {}).items():
                row[k] = v
            self.insert_one(row)

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

    def count_documents(self, filt):
        return sum(1 for d in self.docs if self._match(d, filt))

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

    def find(self, filt=None, proj=None):
        for d in self.docs:
            if self._match(d, filt or {}):
                if proj:
                    yield {k: d.get(k) for k in proj.keys()}
                else:
                    yield dict(d)


class FakeDb:
    def __init__(self):
        self.users = FakeCollection()
        self.voucher_pools = FakeCollection(unique_fields=[("pool_id", "code")])
        self.affiliate_ledger = FakeCollection(unique_fields=[("dedup_key",)])
        self.qualified_events = FakeCollection(unique_fields=[("invitee_id",)])
        self.user_last_seen = FakeCollection(unique_fields=[("user_id",)])
        self.referral_audit = FakeCollection()


class AffiliateRewardTests(unittest.TestCase):
    def test_welcome_once_with_dedup(self):
        db = FakeDb()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "W1", "status": "available"})
        out1 = issue_welcome_bonus_if_eligible(db, user_id=10, is_new_user=True)
        out2 = issue_welcome_bonus_if_eligible(db, user_id=10, is_new_user=True)
        self.assertEqual(out1["status"], "ISSUED")
        self.assertEqual(out2["status"], "ISSUED")
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "WELCOME:10"}), 1)

    def test_qualified_event_once_lifetime(self):
        db = FakeDb()
        first = mark_invitee_qualified(db, invitee_id=20, referrer_id=3)
        second = mark_invitee_qualified(db, invitee_id=20, referrer_id=3)
        self.assertTrue(first)
        self.assertFalse(second)

    def test_monthly_tier_and_dedup_key(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 8, "blocked": False})
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        for i in range(1, 8):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 8, "qualified_at": now})
        db.voucher_pools.insert_one({"pool_id": "T2", "code": "A", "status": "available"})
        row = evaluate_monthly_affiliate_reward(db, referrer_id=8, now_utc=now)
        self.assertEqual(row["tier"], "T2")
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "AFF:8:202601:T2"}), 1)

    def test_t2_risk_pending_review_and_t1_bypass(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 9, "blocked": False})
        now = datetime(2026, 1, 12, tzinfo=timezone.utc)
        for i in range(1, 8):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 9, "qualified_at": now, "ip": "1.1.1.1"})
        row = evaluate_monthly_affiliate_reward(db, referrer_id=9, now_utc=now)
        self.assertEqual(row["status"], "PENDING_REVIEW")

        db2 = FakeDb()
        db2.users.insert_one({"user_id": 5, "blocked": False})
        for i in range(1, 4):
            db2.qualified_events.insert_one({"invitee_id": i, "referrer_id": 5, "qualified_at": now, "ip": "1.1.1.1"})
        db2.voucher_pools.insert_one({"pool_id": "T1", "code": "TT1", "status": "available"})
        row2 = evaluate_monthly_affiliate_reward(db2, referrer_id=5, now_utc=now)
        self.assertEqual(row2["status"], "ISSUED")

    def test_out_of_stock_and_atomic_claim(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 7, "blocked": False})
        now = datetime.now(timezone.utc)
        for i in range(1, 4):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 7, "qualified_at": now})
        row = evaluate_monthly_affiliate_reward(db, referrer_id=7, now_utc=now)
        self.assertEqual(row["status"], "OUT_OF_STOCK")

        db.voucher_pools.insert_one({"pool_id": "T1", "code": "ONLY1", "status": "available"})
        db.users.insert_one({"user_id": 11, "blocked": False})
        for i in range(101, 104):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 11, "qualified_at": now})
        row_a = evaluate_monthly_affiliate_reward(db, referrer_id=11, now_utc=now)
        db.users.insert_one({"user_id": 12, "blocked": False})
        for i in range(201, 204):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 12, "qualified_at": now})
        row_b = evaluate_monthly_affiliate_reward(db, referrer_id=12, now_utc=now)
        issued = [r for r in (row_a, row_b) if r.get("status") == "ISSUED"]
        self.assertEqual(len(issued), 1)


if __name__ == "__main__":
    unittest.main()
