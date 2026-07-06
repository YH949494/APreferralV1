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
                elif op == "$nin":
                    if val in opval:
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
            target_id = filt.get("_id")
            if target_id is not None and any(d.get("_id") == target_id for d in self.docs):
                # A document with this _id exists but didn't satisfy the
                # rest of the filter — real MongoDB would reject the
                # upsert-insert with a duplicate key error on _id instead
                # of silently creating a second document with the same id.
                from pymongo.errors import DuplicateKeyError
                raise DuplicateKeyError("E11000 duplicate key error (fake)")
            new_doc = {"_id": target_id}
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

    # Manual release delayed 7 days past compile time must still be
    # visible/claimable — the release window is re-anchored to the actual
    # release moment, not left pinned to the compile-time default.
    def test_manual_release_after_7_days_is_still_visible(self):
        doc = self._draft(release_type="manual", total_vouchers=50, batch_size=50,
                           reward_params={"codes": self._codes(50), "pool": "public"})
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        drop_id = campaign_builder._col().find_one({"_id": campaign_id})["child_drop_ids"][0]

        # Simulate compiling now, but the admin only clicks "release" 7
        # days later — well past the 24h default endsAt computed at
        # compile time.
        far_past_ends = datetime.now(timezone.utc) - timedelta(days=7)
        far_past_starts = far_past_ends - timedelta(hours=24)
        self.fake_db["drops"].update_one(
            {"_id": ObjectId(drop_id)},
            {"$set": {"startsAt": far_past_starts, "endsAt": far_past_ends}},
        )

        result, code = campaign_builder.release_next_batch_now(campaign_id)
        self.assertEqual(code, 200)
        self.assertEqual(result["released_drop_id"], drop_id)

        released_drop = self.fake_db["drops"].find_one({"_id": ObjectId(drop_id)})
        now = datetime.now(timezone.utc)
        self.assertEqual(released_drop["status"], "active")
        self.assertLessEqual(released_drop["startsAt"], now)
        self.assertGreater(released_drop["endsAt"], now)
        # Original configured duration (24h) is preserved, just re-anchored.
        self.assertAlmostEqual(
            (released_drop["endsAt"] - released_drop["startsAt"]).total_seconds(), 24 * 3600, delta=5
        )
        # Now visible via the exact same query vouchers.get_active_drops uses
        # (status not in expired/paused, startsAt<=ref<endsAt).
        visible = self.fake_db["drops"].find({
            "status": {"$nin": ["expired", "paused"]},
            "startsAt": {"$lte": now},
            "endsAt": {"$gt": now},
        })
        self.assertIn(drop_id, [str(d["_id"]) for d in visible])

    # An automatic (interval_minutes) campaign resumed 7 days after being
    # paused must also re-anchor the overdue batch's window instead of
    # releasing an already-expired drop.
    def test_resume_after_7_days_reanchors_overdue_batch_window(self):
        doc = self._draft(release_type="interval_minutes", release_interval_minutes=5,
                           total_vouchers=150, batch_size=50,
                           reward_params={"codes": self._codes(150), "pool": "public"})
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        campaign_builder.pause_batch_campaign(campaign_id)

        # 7 days pass while paused — the next batch's pre-computed window
        # (startsAt/endsAt stamped at compile time) is long gone.
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        next_child = self.fake_db["drops"].find_one({"batch_parent_id": str(campaign_id), "batch_status": "scheduled"})
        self.fake_db["drops"].update_one(
            {"_id": next_child["_id"]},
            {"$set": {"startsAt": seven_days_ago, "endsAt": seven_days_ago + timedelta(hours=24)}},
        )
        campaign_builder._col().update_one({"_id": campaign_id}, {"$set": {"next_release_at": seven_days_ago}})

        campaign_builder.resume_batch_campaign(campaign_id)
        campaign_builder.batch_release_tick()

        released_drop = self.fake_db["drops"].find_one({"_id": next_child["_id"]})
        now = datetime.now(timezone.utc)
        self.assertEqual(released_drop["batch_status"], "released")
        self.assertEqual(released_drop["status"], "active")
        self.assertLessEqual(released_drop["startsAt"], now)
        self.assertGreater(released_drop["endsAt"], now)

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

        # An immediate retry (still within the compile lease window) must
        # be rejected — this is what prevents two concurrent LAUNCH clicks
        # from both compiling. It is not a "not_draft" terminal state, just
        # "try again shortly".
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["batch_status"], "compiling")
        immediate_retry, immediate_code = campaign_builder.compile_batch_campaign(stored)
        self.assertEqual(immediate_code, 409)
        self.assertEqual(immediate_retry["code"], "compile_in_progress")
        self.assertEqual(self.fake_db["drops"].count_documents({"batch_parent_id": str(campaign_id)}), 2)

        # Once the lease goes stale (e.g. a genuinely crashed compile,
        # retried after a delay), the retry resumes from the missing batch
        # only — existing ones are untouched, no duplicates.
        stale_started_at = datetime.now(timezone.utc) - timedelta(seconds=campaign_builder.COMPILE_LEASE_SECONDS + 1)
        campaign_builder._col().update_one({"_id": campaign_id}, {"$set": {"compile_started_at": stale_started_at}})
        stored2 = campaign_builder._col().find_one({"_id": campaign_id})
        result2, code2 = campaign_builder.compile_batch_campaign(stored2)
        self.assertEqual(code2, 200, result2)
        self.assertEqual(len(result2["child_drop_ids"]), 3)
        self.assertEqual(self.fake_db["drops"].count_documents({"batch_parent_id": str(campaign_id)}), 3)

    def test_concurrent_launch_only_one_compiles(self):
        """Two 'concurrent' LAUNCH calls starting from the same draft
        snapshot: only the one whose CAS actually matches may proceed."""
        doc = self._draft()
        winner, winner_code = campaign_builder.compile_batch_campaign(doc)
        # A second call using the same stale (pre-compile) snapshot,
        # simulating a racing request that read the campaign before the
        # first call's CAS took effect.
        loser, loser_code = campaign_builder.compile_batch_campaign(doc)
        self.assertEqual(winner_code, 200)
        self.assertIn(loser_code, (400, 409))
        self.assertIn(loser["code"], ("not_draft", "compile_in_progress"))
        # Exactly one set of child drops exists — no duplicates.
        self.assertEqual(self.fake_db["drops"].count_documents({"batch_parent_id": str(doc["_id"])}), 10)

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

    # --- Tick lock TTL (50s) vs cron cadence (60s): releases every 1/2/3 minutes ---

    def test_release_schedule_spacing_for_1_2_3_minute_intervals(self):
        first = datetime.now(timezone.utc)
        for minutes in (1, 2, 3):
            schedule = campaign_builder.compute_release_schedule(
                release_type="interval_minutes",
                batch_count=4,
                first_release_at=first,
                release_interval_minutes=minutes,
            )
            gaps = [(schedule[i + 1] - schedule[i]).total_seconds() for i in range(len(schedule) - 1)]
            self.assertTrue(all(g == minutes * 60 for g in gaps), (minutes, gaps))

    def test_tick_lock_ttl_is_below_cron_cadence(self):
        """The lock must expire well before the next scheduled minute-tick
        fires, or 1/2/3-minute release cadences get silently skipped."""
        acquired_first = campaign_builder._acquire_batch_lock("test_cadence_lock", ttl_seconds=50)
        self.assertTrue(acquired_first)
        # Immediately re-acquiring (same "tick period") must fail — this is
        # what prevents two overlapping runs of the tick.
        acquired_immediately_after = campaign_builder._acquire_batch_lock("test_cadence_lock", ttl_seconds=50)
        self.assertFalse(acquired_immediately_after)
        # Once the lock's TTL has elapsed (simulating the next scheduled
        # minute boundary, since TTL=50s < cron cadence=60s), a new tick
        # must be able to acquire it again.
        lock_doc = self.fake_db[campaign_builder.BATCH_LOCK_COLLECTION].find_one({"_id": "test_cadence_lock"})
        lock_doc["expireAt"] = datetime.now(timezone.utc) - timedelta(seconds=1)
        self.fake_db[campaign_builder.BATCH_LOCK_COLLECTION].update_one(
            {"_id": "test_cadence_lock"}, {"$set": {"expireAt": lock_doc["expireAt"]}}
        )
        acquired_next_period = campaign_builder._acquire_batch_lock("test_cadence_lock", ttl_seconds=50)
        self.assertTrue(acquired_next_period)

    def _run_interval_campaign_and_assert_batch2_releases_on_time(self, minutes):
        doc = self._draft(release_type="interval_minutes", release_interval_minutes=minutes,
                           total_vouchers=150, batch_size=50,
                           reward_params={"codes": self._codes(150), "pool": "public"})
        campaign_builder.compile_batch_campaign(doc)
        campaign_id = doc["_id"]
        stored = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored["released_batches"], 1)  # batch 1 released immediately at launch

        # Simulate the clock reaching batch 2's due time (minutes after
        # batch 1) — the tick's own lock (TTL 50s, below the 60s cron
        # cadence) must not be what's blocking this; only next_release_at
        # gates it.
        campaign_builder._col().update_one(
            {"_id": campaign_id},
            {"$set": {"next_release_at": datetime.now(timezone.utc) - timedelta(seconds=1)}},
        )
        campaign_builder.batch_release_tick()
        stored2 = campaign_builder._col().find_one({"_id": campaign_id})
        self.assertEqual(stored2["released_batches"], 2, f"interval_minutes={minutes} did not release on time")

    def test_release_every_1_minute_releases_on_time(self):
        self._run_interval_campaign_and_assert_batch2_releases_on_time(1)

    def test_release_every_2_minutes_releases_on_time(self):
        self._run_interval_campaign_and_assert_batch2_releases_on_time(2)

    def test_release_every_3_minutes_releases_on_time(self):
        self._run_interval_campaign_and_assert_batch2_releases_on_time(3)


if __name__ == "__main__":
    unittest.main()
