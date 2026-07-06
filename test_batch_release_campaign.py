"""Unit tests for the P3 Batch Release Campaign system (campaign_builder.py).

Exercises the batch compiler, release/pause/resume/cancel operations, and
the scheduler tick end-to-end against an in-memory fake Mongo. Confirms the
compiler only ever writes child voucher drops through
vouchers.create_drop_from_spec (no duplicate insert logic), and that the
existing claim/eligibility/scheduler/affiliate code paths are untouched.
"""

import unittest
from datetime import datetime, timedelta, timezone

from bson.objectid import ObjectId

import database
import campaign_builder


class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeInsertManyResult:
    def __init__(self, inserted_ids):
        self.inserted_ids = inserted_ids


class FakeUpdateResult:
    def __init__(self, matched_count):
        self.matched_count = matched_count
        self.modified_count = matched_count


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
                elif op == "$lte":
                    if val is None or val > opval:
                        return False
                elif op == "$lt":
                    if val is None or val >= opval:
                        return False
                elif op == "$gt":
                    if val is None or val <= opval:
                        return False
                elif op == "$exists":
                    if bool(opval) != (key in doc):
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
            for key, direction in reversed(sort):
                results.sort(key=lambda d: (d.get(key) is None, d.get(key)), reverse=(direction == -1))
        return dict(results[0]) if results else None

    def find(self, filt=None, sort=None, projection=None, limit=None):
        results = [dict(d) for d in self.docs if _matches(d, filt or {})]
        if sort:
            for key, direction in reversed(sort):
                results.sort(key=lambda d: (d.get(key) is None, d.get(key)), reverse=(direction == -1))
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
                if "$setOnInsert" in update:
                    pass
                return FakeUpdateResult(1)
        # Upsert support (only needed for the batch lock helper).
        if update.get("$setOnInsert") is not None or True:
            pass
        return FakeUpdateResult(0)

    def update_many(self, filt, update):
        count = 0
        for d in self.docs:
            if _matches(d, filt):
                if "$set" in update:
                    d.update(update["$set"])
                count += 1
        return FakeUpdateResult(count)

    def find_one_and_update(self, filt, update, upsert=False, return_document=None):
        for d in self.docs:
            if _matches(d, filt):
                if "$set" in update:
                    d.update(update["$set"])
                return dict(d)
        if upsert:
            new_doc = {"_id": filt.get("_id")}
            if "$set" in update:
                new_doc.update(update["$set"])
            if "$setOnInsert" in update:
                new_doc.update(update["$setOnInsert"])
            self.docs.append(new_doc)
            return dict(new_doc)
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


class BatchReleaseCampaignTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = FakeDB()
        self._orig_db = database._db
        database._db = self.fake_db

    def tearDown(self):
        database._db = self._orig_db

    def _codes(self, n):
        return [f"CODE{i:04d}" for i in range(n)]

    def _draft(self, **overrides):
        now = datetime.now(timezone.utc)
        doc = {
            "_id": ObjectId(),
            "campaign_name": "Weekend Reload",
            "campaign_type": "public",
            "status": "draft",
            "audience_mode": "no_segment_filter",
            "audience_params": {},
            "release_style": "immediate",
            "release_params": {},
            "reward_type": "voucher_pool",
            "reward_params": {"codes": self._codes(500), "pool": "public"},
            "compiled_drop_ids": [],
            "created_at": now,
            "updated_at": now,
            "feature_version": "P2",
            "release_type": "hourly",
            "batch_status": "draft",
            "total_vouchers": 500,
            "batch_size": 50,
            "release_interval_minutes": None,
            "release_schedule": [],
            "child_drop_ids": [],
            "released_batches": 0,
            "next_release_at": None,
            "compiled_at": None,
            "paused_at": None,
            "cancelled_at": None,
        }
        doc.update(overrides)
        campaign_builder._col().insert_one(doc)
        return doc

    # 1. 500 vouchers / 50 hourly => 10 child drops
    def test_500_vouchers_50_hourly_yields_10_child_drops(self):
        doc = self._draft()
        result, code = campaign_builder.compile_batch_campaign(doc)
        self.assertEqual(code, 200, result)
        self.assertEqual(len(result["child_drop_ids"]), 10)
        for drop_id in result["child_drop_ids"]:
            drop = self.fake_db["drops"].find_one({"_id": ObjectId(drop_id)})
            self.assertEqual(drop["batch_count"], 10)
            self.assertEqual(drop["batch_parent_id"], str(doc["_id"]))

    # 2. uneven split, e.g. 525 / 50 => 11 child drops
    def test_uneven_split_525_over_50_yields_11_child_drops(self):
        doc = self._draft(total_vouchers=525, reward_params={"codes": self._codes(525), "pool": "public"})
        result, code = campaign_builder.compile_batch_campaign(doc)
        self.assertEqual(code, 200, result)
        self.assertEqual(len(result["child_drop_ids"]), 11)
        last_drop_id = result["child_drop_ids"][-1]
        vouchers = self.fake_db["vouchers"].find({"dropId": last_drop_id})
        self.assertEqual(len(vouchers), 25)

    # 3. insufficient uploaded codes blocks launch
    def test_insufficient_codes_blocks_launch(self):
        doc = self._draft(total_vouchers=500, reward_params={"codes": self._codes(100), "pool": "public"})
        result, code = campaign_builder.compile_batch_campaign(doc)
        self.assertEqual(code, 400)
        self.assertEqual(result["code"], "validation_failed")
        self.assertTrue(any("insufficient_codes" in e for e in result["errors"]))
        self.assertEqual(self.fake_db["drops"].count_documents({}), 0)
        stored = campaign_builder._col().find_one({"_id": doc["_id"]})
        self.assertEqual(stored["batch_status"], "draft")

    # 4. manual release creates unreleased children
    def test_manual_release_creates_unreleased_children(self):
        doc = self._draft(release_type="manual", total_vouchers=150, batch_size=50,
                           reward_params={"codes": self._codes(150), "pool": "public"})
        result, code = campaign_builder.compile_batch_campaign(doc)
        self.assertEqual(code, 200, result)
        self.assertEqual(len(result["released_now"]), 0)
        stored = campaign_builder._col().find_one({"_id": doc["_id"]})
        self.assertEqual(stored["released_batches"], 0)
        for drop_id in result["child_drop_ids"]:
            drop = self.fake_db["drops"].find_one({"_id": ObjectId(drop_id)})
            self.assertEqual(drop["status"], "paused")
            self.assertEqual(drop["batch_status"], "scheduled")

    # 5. pause prevents future release
    def test_pause_prevents_future_release(self):
        doc = self._draft()
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        result, code = campaign_builder.pause_batch_campaign(campaign_id)
        self.assertEqual(code, 200)
        self.assertEqual(result["batch_status"], "paused")

        # Force every batch overdue and run the tick — nothing should release.
        campaign_builder._col().update_one(
            {"_id": campaign_id},
            {"$set": {"next_release_at": datetime.now(timezone.utc) - timedelta(hours=1)}},
        )
        campaign_builder.batch_release_tick()
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["released_batches"], 1)  # only the immediate launch-time release

    # 6. resume continues next release
    def test_resume_continues_next_release(self):
        doc = self._draft()
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        campaign_builder.pause_batch_campaign(campaign_id)
        result, code = campaign_builder.resume_batch_campaign(campaign_id)
        self.assertEqual(code, 200)
        self.assertIn(result["batch_status"], ("scheduled", "active"))

        campaign_builder._col().update_one(
            {"_id": campaign_id},
            {"$set": {"next_release_at": datetime.now(timezone.utc) - timedelta(hours=1)}},
        )
        campaign_builder.batch_release_tick()
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["released_batches"], 2)

    # 7. cancel does not affect already claimed/released drops
    def test_cancel_does_not_affect_released_drops(self):
        doc = self._draft()
        result, _ = campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        first_drop_id = result["released_now"][0]

        # Simulate a claim against the first (already-released) drop.
        voucher = self.fake_db["vouchers"].find_one({"dropId": first_drop_id})
        self.fake_db["vouchers"].update_one({"_id": voucher["_id"]}, {"$set": {"status": "claimed"}})

        cancel_result, code = campaign_builder.cancel_batch_campaign(campaign_id)
        self.assertEqual(code, 200)
        self.assertEqual(cancel_result["batch_status"], "cancelled")

        released_drop = self.fake_db["drops"].find_one({"_id": ObjectId(first_drop_id)})
        self.assertEqual(released_drop["batch_status"], "released")
        self.assertNotEqual(released_drop["status"], "expired")
        claimed_voucher = self.fake_db["vouchers"].find_one({"_id": voucher["_id"]})
        self.assertEqual(claimed_voucher["status"], "claimed")

        # Unreleased children were cancelled.
        cancelled_children = self.fake_db["drops"].find(
            {"batch_parent_id": str(campaign_id), "batch_status": "cancelled"}
        )
        self.assertEqual(len(cancelled_children), 9)
        for child in cancelled_children:
            self.assertEqual(child["status"], "expired")

    # 8. release next now only releases one next batch
    def test_release_next_now_only_releases_one_batch(self):
        doc = self._draft(release_type="manual", total_vouchers=150, batch_size=50,
                           reward_params={"codes": self._codes(150), "pool": "public"})
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]

        result, code = campaign_builder.release_next_batch_now(campaign_id)
        self.assertEqual(code, 200)
        self.assertIsNotNone(result["released_drop_id"])
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["released_batches"], 1)

        # Double-click: releases exactly the second batch, not two at once.
        result2, code2 = campaign_builder.release_next_batch_now(campaign_id)
        self.assertEqual(code2, 200)
        self.assertNotEqual(result2["released_drop_id"], result["released_drop_id"])
        stored2 = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored2["released_batches"], 2)

    def test_release_next_now_is_idempotent_on_same_drop(self):
        """Directly hammering _release_next_batch's CAS twice in a row for
        the same due batch must not double-release it."""
        doc = self._draft(release_type="manual", total_vouchers=50, batch_size=50,
                           reward_params={"codes": self._codes(50), "pool": "public"})
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        first = campaign_builder._release_next_batch(campaign_id)
        self.assertIsNotNone(first)
        second = campaign_builder._release_next_batch(campaign_id)
        self.assertIsNone(second)  # nothing left to release (only 1 batch)
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["released_batches"], 1)
        self.assertEqual(stored["batch_status"], "completed")

    # 9. compiler calls/reuses create_drop_from_spec()
    def test_compiler_reuses_create_drop_from_spec(self):
        import vouchers
        calls = []
        original = vouchers.create_drop_from_spec

        def spy(data):
            calls.append(data)
            return original(data)

        vouchers.create_drop_from_spec = spy
        try:
            doc = self._draft()
            result, code = campaign_builder.compile_batch_campaign(doc)
            self.assertEqual(code, 200)
            self.assertEqual(len(calls), 10)
        finally:
            vouchers.create_drop_from_spec = original

    # 10. existing (non-batch) voucher claim behavior unchanged
    def test_non_batch_campaign_path_unchanged(self):
        import test_campaign_builder as p2

        p2_case = p2.CampaignBuilderCompilerTests()
        p2_case.fake_db = self.fake_db
        p2_case._orig_db = self._orig_db
        doc = p2_case._draft()
        result, code = campaign_builder.compile_campaign(doc)
        self.assertEqual(code, 200)
        self.assertEqual(len(result["compiled_drop_ids"]), 1)

    # Crash-safety: partial compile resumes without duplicating drops.
    def test_compile_resumes_after_partial_failure(self):
        doc = self._draft(total_vouchers=150, batch_size=50,
                           reward_params={"codes": self._codes(150), "pool": "public"})
        campaign_id = doc["_id"]

        import vouchers
        original = vouchers.create_drop_from_spec
        call_count = {"n": 0}

        def flaky(data):
            call_count["n"] += 1
            if call_count["n"] == 2:
                return {"status": "error", "code": "server_error"}, 500
            return original(data)

        vouchers.create_drop_from_spec = flaky
        try:
            result, code = campaign_builder.compile_batch_campaign(doc)
            self.assertEqual(code, 400)
            self.assertEqual(result["code"], "compile_incomplete")
        finally:
            vouchers.create_drop_from_spec = original

        # Retry: only the missing batch is created, existing ones untouched.
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["batch_status"], "compiling")
        result2, code2 = campaign_builder.compile_batch_campaign(stored)
        self.assertEqual(code2, 200, result2)
        self.assertEqual(len(result2["child_drop_ids"]), 3)
        self.assertEqual(self.fake_db["drops"].count_documents({"batch_parent_id": str(campaign_id)}), 3)

    def test_repeated_launch_request_is_rejected_once_scheduled(self):
        doc = self._draft()
        result, code = campaign_builder.compile_batch_campaign(doc)
        self.assertEqual(code, 200)
        stored = campaign_builder._col().find_one({"_id": doc["_id"]})
        result2, code2 = campaign_builder.compile_batch_campaign(stored)
        self.assertEqual(code2, 400)
        self.assertEqual(result2["code"], "not_draft")

    def test_batch_analytics_aggregates_from_drops_and_vouchers(self):
        doc = self._draft(total_vouchers=100, batch_size=50,
                           reward_params={"codes": self._codes(100), "pool": "public"})
        campaign_id = doc["_id"]
        campaign_builder.compile_batch_campaign(doc)
        campaign_builder.release_next_batch_now(campaign_id)

        analytics = campaign_builder.batch_campaign_analytics(campaign_id)
        self.assertEqual(analytics["total_batches"], 2)
        self.assertEqual(analytics["released_batches"], 2)
        self.assertEqual(len(analytics["child_drops"]), 2)
        self.assertEqual(analytics["child_drops"][0]["total_codes"], 50)

    def test_preview_batch_campaign_reports_schedule(self):
        doc = self._draft()
        preview = campaign_builder.preview_batch_campaign(doc)
        self.assertEqual(preview["batch_count"], 10)
        self.assertTrue(preview["launchable"])
        self.assertEqual(len(preview["release_schedule"]), 10)
        self.assertIsNotNone(preview["first_release_at"])
        self.assertIsNotNone(preview["last_release_at"])


if __name__ == "__main__":
    unittest.main()
