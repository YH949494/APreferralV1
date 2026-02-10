from datetime import datetime, timedelta, timezone
import os
import unittest

from scheduler import _eligible_referrer_tiers
from ugc_growth_referral import (
    create_referral_token,
    apply_growth_credit,
    is_expansion_qualified,
    require_env,
    first_touch_claim,
    resolve_token,
    should_allow_viewer_reward,
)


class FakeCollection:
    def __init__(self):
        self.docs = []

    def insert_one(self, doc):
        self.docs.append(dict(doc))

    def find_one(self, filt, projection=None):
        _ = projection
        for d in self.docs:
            ok = True
            for k, v in filt.items():
                if isinstance(v, dict) and "$ne" in v:
                    if d.get(k) == v["$ne"]:
                        ok = False
                        break
                elif d.get(k) != v:
                    ok = False
                    break
            if ok:
                return dict(d)
        return None


class UGCGrowthReferralTests(unittest.TestCase):
    def test_first_touch_second_token_ignored(self):
        col = FakeCollection()
        now = datetime.now(timezone.utc)
        first_touch_claim(col, viewer_uid=10, owner_uid=1, token="a", ip_hash=None, subnet_hash=None, device_hash=None, now=now)
        doc2, created = first_touch_claim(col, viewer_uid=10, owner_uid=2, token="b", ip_hash=None, subnet_hash=None, device_hash=None, now=now)
        self.assertFalse(created)
        self.assertEqual(doc2["source_uid"], 1)

    def test_pending_claim_due_and_validation_defaults(self):
        col = FakeCollection()
        now = datetime.now(timezone.utc)
        doc, created = first_touch_claim(col, viewer_uid=11, owner_uid=1, token="a", ip_hash="ip", subnet_hash="sn", device_hash="dv", now=now)
        self.assertTrue(created)
        self.assertEqual(doc["status"], "pending")
        self.assertEqual(doc["due_at"], now + timedelta(hours=48))
        self.assertFalse(doc["viewer_reward_issued"])

    def test_reward_allowed_false_on_duplicate_hash(self):
        col = FakeCollection()
        col.insert_one({"viewer_uid": 20, "status": "validated", "ip_hash": "x"})
        claim = {"viewer_uid": 21, "ip_hash": "x", "subnet_hash": None, "device_hash": None}
        self.assertFalse(should_allow_viewer_reward(col, claim))

    def test_expansion_qualified_false_duplicate_subnet(self):
        claim = {"device_hash": "d1", "subnet_hash": "s1"}
        recent = [{"device_hash": "d2", "subnet_hash": "s1"}]
        self.assertFalse(is_expansion_qualified(100, 1, claim, recent))

    def test_throttle_24h(self):
        class FakeGrowthCollection(FakeCollection):
            def __init__(self):
                super().__init__()
                self.docs.append({"_id": 1, "credited_count": 0, "last_credit_at": None, "tier_awarded": {"s2": False, "s3": False, "s4": False}})

            def find_one_and_update(self, filt, update, upsert=False, return_document=None):
                _ = (upsert, return_document)
                row = self.find_one(filt)
                if row is None:
                    row = {"_id": filt.get("_id")}
                    self.docs.append(row)
                for k, v in (update.get("$set", {}) or {}).items():
                    row[k] = v
                for k, v in (update.get("$setOnInsert", {}) or {}).items():
                    row.setdefault(k, v)
                for k, v in (update.get("$inc", {}) or {}).items():
                    row[k] = int(row.get(k, 0)) + int(v)
                for idx, doc in enumerate(self.docs):
                    if doc.get("_id") == row.get("_id"):
                        self.docs[idx] = row
                        break
                return dict(row)

        col = FakeGrowthCollection()
        now = datetime.now(timezone.utc)
        credited, reason, doc = apply_growth_credit(col, 1, now)
        self.assertTrue(credited)
        self.assertEqual(reason, "credited")
        credited2, reason2, _ = apply_growth_credit(col, 1, now + timedelta(hours=1))
        self.assertFalse(credited2)
        self.assertEqual(reason2, "throttle")
        self.assertEqual(doc.get("credited_count"), 1)

    def test_tier_s2_s3_auto_s4_pending(self):
        self.assertEqual(_eligible_referrer_tiers(3, {"s2": False, "s3": False, "s4": False}), ["S2"])
        self.assertEqual(_eligible_referrer_tiers(7, {"s2": True, "s3": False, "s4": False}), ["S3"])
        self.assertEqual(_eligible_referrer_tiers(15, {"s2": True, "s3": True, "s4": False}), ["S4"])

    def test_referral_xp_rules_unchanged(self):
        from referral_rules import calc_referral_award

        self.assertEqual(calc_referral_award(1), (30, 0))
        self.assertEqual(calc_referral_award(3), (230, 200))

    def test_env_missing_fails_early(self):
        old1 = os.environ.pop("UGC_T1_SMALL_DROP_ID", None)
        old2 = os.environ.pop("UGC_T2_MID_DROP_ID", None)
        try:
            with self.assertRaises(RuntimeError):
                require_env()
        finally:
            if old1 is not None:
                os.environ["UGC_T1_SMALL_DROP_ID"] = old1
            if old2 is not None:
                os.environ["UGC_T2_MID_DROP_ID"] = old2

    def test_referral_token_shape(self):
        col = FakeCollection()
        now = datetime.now(timezone.utc)
        token, expires = create_referral_token(col, owner_uid=99, now=now)
        self.assertGreater(len(token), 8)
        self.assertLessEqual(len(f"r_{token}"), 64)
        self.assertEqual(expires, now + timedelta(hours=24))

    def test_resolve_token_expired(self):
        col = FakeCollection()
        now = datetime.now(timezone.utc)
        col.insert_one({"_id": "tok", "owner_uid": 5, "expires_at": now - timedelta(seconds=1)})
        self.assertIsNone(resolve_token(col, "tok", now=now))


if __name__ == "__main__":
    unittest.main()
