import unittest

from referral_rules import (
    BASE_REFERRAL_XP,
    REFERRAL_BONUS_EVENT,
    REFERRAL_BONUS_INTERVAL,
    REFERRAL_BONUS_XP,
    REFERRAL_SUCCESS_EVENT,
    grant_referral_rewards,
    reconcile_referrals,
    upsert_referral_and_update_user_count,
)


class FakeResult:
    def __init__(self, upserted_id=None, modified_count=0):
        self.upserted_id = upserted_id
        self.modified_count = modified_count


class FakeCollection:
    def __init__(self):
        self.docs = []
        self._id = 1

    def _match(self, doc, filt):
        if not filt:
            return True

        for k, v in filt.items():
            if k == "$or":
                return any(self._match(doc, cond) for cond in v)
            if k == "$and":
                return all(self._match(doc, cond) for cond in v)
            if isinstance(v, dict):
                if "$in" in v:
                    if doc.get(k) not in v.get("$in", []):
                        return False
                    continue
                if "$exists" in v:
                    exists = k in doc
                    if bool(v["$exists"]) != exists:
                        return False
                    continue
                if "$ne" in v:
                    if doc.get(k) == v["$ne"]:
                        return False
                    continue
            if doc.get(k) != v:
                return False
        return True

    def find_one(self, filt, projection=None):  # noqa: ARG002
        
        for doc in self.docs:
            if self._match(doc, filt):
                return dict(doc)
        return None

    def find(self, filt=None):
        filt = filt or {}
        return [dict(d) for d in self.docs if self._match(d, filt)]

    def update_one(self, filt, update, upsert=False):
        sets = update.get("$set", {})
        set_on_insert = update.get("$setOnInsert", {})
        incs = update.get("$inc", {})
        unsets = update.get("$unset", {})        
        max_ops = update.get("$max", {})

        for doc in self.docs:
            if self._match(doc, filt):
                for k, v in sets.items():
                    doc[k] = v
                for k, v in incs.items():
                    doc[k] = doc.get(k, 0) + v
                for k, v in max_ops.items():
                    doc[k] = max(doc.get(k, float("-inf")), v)
                for k in unsets.keys():
                    if k in doc:
                        del doc[k]                    
                return FakeResult(modified_count=1)

        if not upsert:
            return FakeResult()

        doc = {**filt, **set_on_insert}
        for k, v in sets.items():
            doc[k] = v
        for k, v in incs.items():
            doc[k] = doc.get(k, 0) + v
        for k in unsets.keys():
            doc.pop(k, None)            
        doc.setdefault("_id", self._id)
        self._id += 1
        self.docs.append(doc)
        return FakeResult(upserted_id=doc["_id"], modified_count=1)

    def count_documents(self, filt):
        return len(self.find(filt))


class FakeXPEvents(FakeCollection):
    def find_one(self, filt, projection=None):  # noqa: ARG002 - projection unused
        for doc in self.docs:
            if (doc.get("user_id"), doc.get("unique_key")) == (
                filt.get("user_id"),
                filt.get("unique_key"),
            ):
                return dict(doc)
        return None
    
    def update_one(self, filt, update, upsert=False):  # noqa: ARG002
        key = (filt.get("user_id"), filt.get("unique_key"))
        for doc in self.docs:
            if (doc.get("user_id"), doc.get("unique_key")) == key:
                return FakeResult()
        doc = {**filt, **update.get("$setOnInsert", {}), "_id": len(self.docs) + 1}
        self.docs.append(doc)
        return FakeResult(upserted_id=doc["_id"])

class FakeLedger(FakeCollection):
    def update_one(self, filt, update, upsert=False):  # noqa: ARG002
        key = (filt.get("user_id"), filt.get("source"), filt.get("source_id"))
        for doc in self.docs:
            if (doc.get("user_id"), doc.get("source"), doc.get("source_id")) == key:
                return FakeResult()
        doc = {**filt, **update.get("$setOnInsert", {}), "_id": len(self.docs) + 1}
        self.docs.append(doc)
        return FakeResult(upserted_id=doc["_id"])

    def delete_one(self, filt):  # noqa: ARG002
        key = (filt.get("user_id"), filt.get("source"), filt.get("source_id"))
        self.docs = [
            doc for doc in self.docs if (doc.get("user_id"), doc.get("source"), doc.get("source_id")) != key
        ]

class FakeDB:
    def __init__(self):
        self.users = FakeCollection()
        self.referrals = FakeCollection()
        self.xp_events = FakeXPEvents()
        self.xp_ledger = FakeLedger()    

class ReferralTests(unittest.TestCase):
    def test_award_exact_counts_and_bonus(self):
        db = FakeDB()
        uid = 10
        for i in range(3):
            result = upsert_referral_and_update_user_count(
                db.referrals,
                db.users,
                uid,
                100 + i,
            )
            if result.get("counted"):
                grant_referral_rewards(db, db.users, uid, 100 + i)
                
        user_doc = db.users.find_one({"user_id": uid})
        self.assertEqual(user_doc.get("total_referrals"), 3)
        total_xp = sum(ev["xp"] for ev in db.xp_events.docs)
        expected = 3 * BASE_REFERRAL_XP + REFERRAL_BONUS_XP
        self.assertEqual(total_xp, expected)

    def test_duplicate_referral_does_not_double_grant(self):
        db = FakeDB()
        uid = 22
        result = upsert_referral_and_update_user_count(
            db.referrals,
            db.users,
            uid,
            500,
        )
        if result.get("counted"):
            grant_referral_rewards(db, db.users, uid, 500)
        result = upsert_referral_and_update_user_count(
            db.referrals,
            db.users,
            uid,
            500,
        )
        if result.get("counted"):
            grant_referral_rewards(db, db.users, uid, 500)
        total_xp = sum(ev["xp"] for ev in db.xp_events.docs)
        self.assertEqual(total_xp, BASE_REFERRAL_XP)


    def test_reconciliation_no_mismatches(self):
        refs = [
            {"referrer_id": 1, "referrer_user_id": 1, "referred_user_id": 10, "invitee_user_id": 10, "status": "confirmed"},
            {"referrer_id": 1, "referrer_user_id": 1, "referred_user_id": 11, "invitee_user_id": 11, "status": "confirmed"},
            {"referrer_id": 1, "referrer_user_id": 1, "referred_user_id": 12, "invitee_user_id": 12, "status": "confirmed"},
        ]
        xp_events = [
            {"user_id": 1, "type": REFERRAL_SUCCESS_EVENT, "unique_key": "ref_success:10", "xp": BASE_REFERRAL_XP},
            {"user_id": 1, "type": REFERRAL_SUCCESS_EVENT, "unique_key": "ref_success:11", "xp": BASE_REFERRAL_XP},
            {"user_id": 1, "type": REFERRAL_SUCCESS_EVENT, "unique_key": "ref_success:12", "xp": BASE_REFERRAL_XP},
            {"user_id": 1, "type": REFERRAL_BONUS_EVENT, "unique_key": f"{REFERRAL_BONUS_EVENT}:1", "xp": REFERRAL_BONUS_XP},
        ]

        mismatches = reconcile_referrals(refs, xp_events)
        self.assertEqual(mismatches, [])


if __name__ == "__main__":
    unittest.main()
