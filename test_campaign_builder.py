"""Unit tests for the P2 Campaign Builder compiler (campaign_builder.py).

These tests exercise the compiler pipeline end-to-end (audience resolution ->
reward resolution -> drop spec -> vouchers.create_drop_from_spec) against an
in-memory fake Mongo so no real database is required. They intentionally do
NOT touch vouchers.py's claim path, eligibility evaluation at claim time,
anti-abuse, scheduler, or affiliate settlement — only the drop-creation
primitive the compiler is allowed to call.
"""

import unittest
from datetime import datetime, timezone

from bson.objectid import ObjectId

import database
import campaign_builder


class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeInsertManyResult:
    def __init__(self, inserted_ids):
        self.inserted_ids = inserted_ids


def _matches(doc: dict, filt: dict) -> bool:
    for key, cond in (filt or {}).items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in cond):
                return False
            continue
        val = doc.get(key)
        if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
            for op, opval in cond.items():
                if op == "$ne":
                    if val == opval:
                        return False
                elif op == "$in":
                    if val not in opval:
                        return False
                else:
                    return False
        else:
            if val != cond:
                return False
    return True


class FakeCollection:
    def __init__(self):
        self.docs: list[dict] = []
        self._seq = 1

    def create_index(self, *a, **k):
        return "ix"

    def insert_one(self, doc):
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        self.docs.append(doc)
        return FakeInsertResult(doc["_id"])

    def insert_many(self, docs, ordered=True):
        ids = []
        for d in docs:
            d = dict(d)
            d.setdefault("_id", ObjectId())
            self.docs.append(d)
            ids.append(d["_id"])
        return FakeInsertManyResult(ids)

    def find_one(self, filt=None, sort=None, projection=None):
        results = [d for d in self.docs if _matches(d, filt or {})]
        if sort:
            key, direction = sort[0]
            results.sort(key=lambda d: d.get(key), reverse=(direction == -1))
        return dict(results[0]) if results else None

    def find(self, filt=None, sort=None, projection=None, limit=None):
        results = [dict(d) for d in self.docs if _matches(d, filt or {})]
        if sort:
            key, direction = sort[0]
            results.sort(key=lambda d: d.get(key), reverse=(direction == -1))
        if limit:
            results = results[:limit]
        return results

    def update_one(self, filt, update):
        for d in self.docs:
            if _matches(d, filt):
                if "$set" in update:
                    d.update(update["$set"])
                if "$inc" in update:
                    for k, v in update["$inc"].items():
                        d[k] = d.get(k, 0) + v
                return
        return None

    def delete_one(self, filt):
        for i, d in enumerate(self.docs):
            if _matches(d, filt):
                del self.docs[i]

                class R:
                    deleted_count = 1
                return R()

        class R:
            deleted_count = 0
        return R()

    def count_documents(self, filt=None):
        return len([d for d in self.docs if _matches(d, filt or {})])


class FakeDB:
    def __init__(self):
        self._cols: dict[str, FakeCollection] = {}

    def __getitem__(self, name):
        return self._cols.setdefault(name, FakeCollection())

    def __getattr__(self, name):
        return self[name]


class CampaignBuilderCompilerTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = FakeDB()
        self._orig_db = database._db
        database._db = self.fake_db

    def tearDown(self):
        database._db = self._orig_db

    def _seed_users(self):
        self.fake_db["users"].insert_one({"usernameLower": "alice", "user_id": 111})
        self.fake_db["users"].insert_one({"usernameLower": "bob", "user_id": 222})

    def _seed_segments(self):
        # Canonical source: users.for_bot_segment_normalized, as written by
        # Databot's segment_sync_job (never backend_segment_snapshots, the
        # shadow-only classifier).
        self.fake_db["users"].insert_one(
            {"user_id": 301, "for_bot_segment": "high_value", "for_bot_segment_normalized": "high_value"}
        )
        self.fake_db["users"].insert_one(
            {"user_id": 302, "for_bot_segment": "high_value", "for_bot_segment_normalized": "high_value"}
        )
        self.fake_db["users"].insert_one(
            {"user_id": 303, "for_bot_segment": "low_value", "for_bot_segment_normalized": "low_value"}
        )

    def _draft(self, **overrides):
        now = datetime.now(timezone.utc)
        doc = {
            "_id": ObjectId(),
            "campaign_name": "Weekend Surprise",
            "campaign_type": "public",
            "status": "draft",
            "audience_mode": "no_segment_filter",
            "audience_params": {},
            "release_style": "immediate",
            "release_params": {},
            "reward_type": "voucher_pool",
            "reward_params": {"codes": ["A1", "A2", "A3"], "pool": "public"},
            "compiled_drop_ids": [],
            "created_at": now,
            "updated_at": now,
            "feature_version": "P2",
        }
        doc.update(overrides)
        campaign_builder._col().insert_one(doc)
        return doc

    def test_public_campaign_compiles_one_pooled_drop(self):
        doc = self._draft()
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(result["compiled_drop_ids"]), 1)

        drop = self.fake_db["drops"].find_one({"_id": ObjectId(result["compiled_drop_ids"][0])})
        self.assertEqual(drop["type"], "pooled")
        self.assertEqual(drop["campaign_name"], "Weekend Surprise")
        self.assertEqual(drop["campaign_type"], "public")
        self.assertEqual(drop["eligibility"], {"mode": "public"})  # no restriction

        vouchers = self.fake_db["vouchers"].find({"dropId": result["compiled_drop_ids"][0]})
        self.assertEqual(len(vouchers), 3)

    def test_segment_campaign_compiles_one_drop_per_segment(self):
        self._seed_segments()
        doc = self._draft(
            campaign_type="segment",
            audience_mode="segment",
            audience_params={"segments": ["high_value", "low_value"]},
            reward_params={"codes": ["C1", "C2"], "pool": "public"},
        )
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        self.assertEqual(len(result["compiled_drop_ids"]), 2)

        drops = [self.fake_db["drops"].find_one({"_id": ObjectId(did)}) for did in result["compiled_drop_ids"]]
        allow_lists = [set(d["eligibility"]["allow"]) for d in drops]
        self.assertIn({301, 302}, allow_lists)
        self.assertIn({303}, allow_lists)
        # Segment-generated allow-lists must carry source="segment" so
        # vouchers._is_probability_shaped_pooled_drop applies claim-time
        # segment probability shaping — distinct from a hand-picked whitelist.
        for d in drops:
            self.assertEqual(d["eligibility"]["mode"], "user_id")
            self.assertEqual(d["eligibility"]["source"], "segment")

    def test_whitelist_campaign_resolves_usernames_to_user_ids(self):
        self._seed_users()
        doc = self._draft(
            audience_mode="whitelist",
            audience_params={"usernames": ["@alice", "@bob", "@ghost_user"]},
        )
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        drop = self.fake_db["drops"].find_one({"_id": ObjectId(result["compiled_drop_ids"][0])})
        self.assertEqual(drop["eligibility"], {"mode": "user_id", "allow": [111, 222]})
        self.assertTrue(any("ghost_user" in w for w in result["warnings"]))

    def test_personalised_campaign_compiles_assignment_drop(self):
        doc = self._draft(
            campaign_type="personalised",
            audience_mode="whitelist",
            reward_type="personalised_voucher",
            reward_params={"assignments": [{"username": "@alice", "code": "P1"}]},
        )
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        drop = self.fake_db["drops"].find_one({"_id": ObjectId(result["compiled_drop_ids"][0])})
        self.assertEqual(drop["type"], "personalised")
        vouchers = self.fake_db["vouchers"].find({"dropId": result["compiled_drop_ids"][0]})
        self.assertEqual(len(vouchers), 1)
        self.assertEqual(vouchers[0]["code"], "P1")

    def test_test_campaign_is_admin_only(self):
        doc = self._draft(campaign_type="test", audience_mode="admin_only")
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        drop = self.fake_db["drops"].find_one({"_id": ObjectId(result["compiled_drop_ids"][0])})
        self.assertEqual(drop["eligibility"], {"mode": "admin_only"})

    def test_welcome_campaign_reuses_new_joiner_shortcut(self):
        doc = self._draft(campaign_type="welcome", audience_mode="no_segment_filter")
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        drop = self.fake_db["drops"].find_one({"_id": ObjectId(result["compiled_drop_ids"][0])})
        self.assertEqual(drop.get("campaign_type"), "welcome_voucher")
        self.assertEqual((drop.get("audience") or {}).get("type"), "new_joiner")

    def test_xp_only_reward_produces_no_drop(self):
        doc = self._draft(reward_type="xp", reward_params={"xp_amount": 100})
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 400)
        self.assertEqual(result["code"], "no_drop_reward")
        self.assertEqual(self.fake_db["drops"].count_documents({}), 0)

    def test_compile_is_idempotent_guard_on_status(self):
        doc = self._draft()
        first, _ = campaign_builder.compile_campaign(doc)
        self.assertEqual(first["status"], "ok")
        stored = campaign_builder._col().find_one({"_id": doc["_id"]})
        self.assertEqual(stored["status"], "active")
        self.assertEqual(stored["compiled_drop_ids"], first["compiled_drop_ids"])

        second, code = campaign_builder.compile_campaign(stored)
        self.assertEqual(code, 400)
        self.assertEqual(second["code"], "not_draft")

    def test_rollback_deletes_campaign_but_not_generated_drops(self):
        doc = self._draft()
        result, _ = campaign_builder.compile_campaign(doc)
        drop_id = result["compiled_drop_ids"][0]

        campaign_builder._col().delete_one({"_id": doc["_id"]})

        self.assertIsNone(campaign_builder._col().find_one({"_id": doc["_id"]}))
        self.assertIsNotNone(self.fake_db["drops"].find_one({"_id": ObjectId(drop_id)}))

    def test_preview_reports_reach_and_expected_voucher_count(self):
        self._seed_segments()
        doc = self._draft(
            campaign_type="segment",
            audience_mode="segment",
            audience_params={"segments": ["high_value", "low_value"]},
            reward_params={"codes": ["C1", "C2"], "pool": "public"},
        )
        preview = campaign_builder.preview_campaign(doc)
        self.assertEqual(preview["expected_drop_count"], 2)
        self.assertEqual(preview["estimated_reach"], 3)
        self.assertEqual(preview["expected_voucher_count"], 4)  # 2 codes * 2 drops


if __name__ == "__main__":
    unittest.main()
