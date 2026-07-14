import unittest
from datetime import datetime, timedelta, timezone

import scheduler
import xp_snapshot as snap


class _Result:
    def __init__(self, modified_count=0, upserted_id=None, matched_count=0):
        self.modified_count = modified_count
        self.upserted_id = upserted_id
        self.matched_count = matched_count


def _match_or_clause(doc, clause):
    for key, cond in clause.items():
        if key == "$exists":
            exists = key in clause  # unused, placeholder
        if not isinstance(cond, dict):
            if doc.get(key) != cond:
                return False
            continue
        for op, val in cond.items():
            actual = doc.get(key)
            if op == "$exists":
                if (key in doc) != val:
                    return False
            elif op == "$lt":
                if actual is None or not (actual < val):
                    return False
            elif op == "$gt":
                if actual is None or not (actual > val):
                    return False
            elif op == "$lte":
                if actual is None or not (actual <= val):
                    return False
            elif op == "$eq":
                if actual != val:
                    return False
            else:
                raise NotImplementedError(op)
    return True


def _matches(doc, filt):
    for key, cond in filt.items():
        if key == "$or":
            if not any(_match_or_clause(doc, clause) for clause in cond):
                return False
            continue
        if isinstance(cond, dict):
            for op, val in cond.items():
                actual = doc.get(key)
                if op == "$exists":
                    if (key in doc) != val:
                        return False
                elif op == "$lt":
                    if actual is None or not (actual < val):
                        return False
                elif op == "$gt":
                    if actual is None or not (actual > val):
                        return False
                elif op == "$lte":
                    if actual is None or not (actual <= val):
                        return False
                elif op == "$ne":
                    if actual == val:
                        return False
                else:
                    raise NotImplementedError(op)
        else:
            if doc.get(key) != cond:
                return False
    return True


class _Cursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction=1):
        self._docs.sort(key=lambda d: d.get(field), reverse=(direction < 0))
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeXPEvents:
    def __init__(self):
        self.docs = []
        self._next_id = 1

    def insert(self, **fields):
        doc = {"_id": self._next_id, **fields}
        self._next_id += 1
        self.docs.append(doc)
        return doc

    def find(self, filt=None, projection=None):
        filt = filt or {}
        return _Cursor([d for d in self.docs if _matches(d, filt)])

    def bulk_write(self, ops, ordered=False):
        for op in ops:
            filt, update = op._filter, op._doc
            for d in self.docs:
                if _matches(d, filt):
                    d.update(update.get("$set", {}))
        return _Result()

    def update_many(self, filt, update):
        modified = 0
        for d in self.docs:
            if _matches(d, filt):
                d.update(update.get("$set", {}))
                modified += 1
        return _Result(modified_count=modified)

    def aggregate(self, pipeline, allowDiskUse=False):
        # Purpose-built for scheduler._settle_xp_snapshots_full_rebuild's
        # groupby-user pipeline: total/weekly/monthly xp, skipping invalidated
        # and null user_id. Window bounds are read back out of the pipeline's
        # own $cond structure so this stays in sync with the real query.
        group_stage = pipeline[1]["$group"]
        week_cond = group_stage["weekly_xp"]["$sum"]["$cond"][0]
        month_cond = group_stage["monthly_xp"]["$sum"]["$cond"][0]
        week_start, week_end = week_cond["$and"][0]["$gte"][1], week_cond["$and"][1]["$lt"][1]
        month_start, month_end = month_cond["$and"][0]["$gte"][1], month_cond["$and"][1]["$lt"][1]

        totals = {}
        for d in self.docs:
            uid = d.get("user_id")
            if uid is None or d.get("invalidated"):
                continue
            ts = d.get("created_at") or d.get("ts")
            amount = int(d.get("xp", 0) or 0)
            row = totals.setdefault(uid, {"_id": uid, "total_xp": 0, "weekly_xp": 0, "monthly_xp": 0})
            row["total_xp"] += amount
            if ts is not None and week_start <= ts < week_end:
                row["weekly_xp"] += amount
            if ts is not None and month_start <= ts < month_end:
                row["monthly_xp"] += amount
        return list(totals.values())


class FakeUsers:
    def __init__(self):
        self.docs = {}

    def find_one(self, filt, projection=None):
        for d in self.docs.values():
            if _matches(d, filt):
                return d
        return None

    def update_one(self, filt, update, upsert=False):
        target = None
        for d in self.docs.values():
            if _matches(d, filt):
                target = d
                break
        if target is None:
            if not upsert:
                return _Result()
            uid = filt.get("user_id")
            target = {"user_id": uid}
            self.docs[uid] = target
            created = True
        else:
            created = False
        for k, v in update.get("$set", {}).items():
            target[k] = v
        for k, v in update.get("$inc", {}).items():
            target[k] = target.get(k, 0) + v
        if created:
            return _Result(upserted_id=target["user_id"])
        return _Result(modified_count=1)

    def update_many(self, filt, update):
        modified = 0
        if isinstance(update, list):
            stage = update[0].get("$set", {}) if update else {}
            for d in self.docs.values():
                for k, v in stage.items():
                    if isinstance(v, str) and v.startswith("$"):
                        d[k] = d.get(v[1:])
                    else:
                        d[k] = v
                modified += 1
            return _Result(modified_count=modified)
        for d in self.docs.values():
            for k, v in update.get("$set", {}).items():
                d[k] = v
            for k, v in update.get("$inc", {}).items():
                d[k] = d.get(k, 0) + v
            modified += 1
        return _Result(modified_count=modified)

    def bulk_write(self, ops, ordered=False):
        for op in ops:
            uid = op._filter.get("user_id")
            doc = self.docs.setdefault(uid, {"user_id": uid})
            for k, v in op._doc.get("$set", {}).items():
                doc[k] = v
            for k, v in op._doc.get("$inc", {}).items():
                doc[k] = doc.get(k, 0) + v
        return _Result()


class FakeSingleDocCollection:
    def __init__(self):
        self.docs = {}

    def find_one(self, filt):
        return self.docs.get(filt.get("_id"))

    def update_one(self, filt, update, upsert=False):
        doc_id = filt.get("_id")
        doc = self.docs.get(doc_id)
        if doc is None:
            if not upsert and doc_id not in self.docs:
                if "$setOnInsert" not in update and not upsert:
                    return _Result()
            doc = {"_id": doc_id}
            self.docs[doc_id] = doc
        else:
            # honor optimistic-concurrency guards like {"last_event_id": old_value}
            for k, v in filt.items():
                if k == "_id":
                    continue
                if doc.get(k) != v:
                    return _Result(modified_count=0)
        for k, v in update.get("$setOnInsert", {}).items():
            doc.setdefault(k, v)
        for k, v in update.get("$set", {}).items():
            doc[k] = v
        return _Result(modified_count=1)


class FakeDB:
    def __init__(self):
        self.xp_events = FakeXPEvents()
        self.users = FakeUsers()
        self.xp_snapshot_state = FakeSingleDocCollection()
        self.admin_cache = FakeSingleDocCollection()


def _patch_safe_create_index():
    # safe_create_index expects a real pymongo collection; no-op it for
    # these fakes since index creation isn't part of the test surface.
    original = snap.safe_create_index
    snap.safe_create_index = lambda *a, **k: "noop"
    return original


class XPSnapshotIncrementalTests(unittest.TestCase):
    def setUp(self):
        self._orig_safe_create_index = _patch_safe_create_index()
        self.db = FakeDB()
        self._orig_scheduler_db = scheduler.db
        self._orig_write_heartbeat = scheduler._write_snapshot_heartbeat
        scheduler.db = self.db
        scheduler._write_snapshot_heartbeat = lambda source, ts: None
        self.now = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)  # a Tuesday
        self.db.users.docs[1] = {"user_id": 1}
        self.db.users.docs[2] = {"user_id": 2}

    def tearDown(self):
        snap.safe_create_index = self._orig_safe_create_index
        scheduler.db = self._orig_scheduler_db
        scheduler._write_snapshot_heartbeat = self._orig_write_heartbeat

    def _grant(self, uid, amount, when=None, invalidated=False):
        return self.db.xp_events.insert(
            user_id=uid, xp=amount, created_at=when or self.now, invalidated=invalidated
        )

    def test_migration_bootstrap_preserves_existing_totals(self):
        self._grant(1, 100, when=self.now - timedelta(days=10))
        self._grant(1, 50, when=self.now)
        self._grant(2, 30, when=self.now)

        summary = snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)

        self.assertEqual(self.db.users.docs[1]["total_xp"], 150)
        self.assertEqual(self.db.users.docs[1]["weekly_xp"], 50)
        self.assertEqual(self.db.users.docs[2]["total_xp"], 30)
        # Bootstrap ran the full rebuild once; cursor now pins to the last event.
        cursor = self.db.xp_snapshot_state.find_one({"_id": snap.CURSOR_ID})
        self.assertIsNotNone(cursor["last_event_id"])
        self.assertEqual(summary["scanned"], 0)  # nothing newer than the bootstrap cursor

    def test_new_event_visible_next_run(self):
        self._grant(1, 100, when=self.now)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)
        self.assertEqual(self.db.users.docs[1]["total_xp"], 100)

        self._grant(1, 25, when=self.now)
        summary = snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)

        self.assertEqual(self.db.users.docs[1]["total_xp"], 125)
        self.assertEqual(summary["scanned"], 1)  # only the new event, not full history

    def test_same_batch_replayed_does_not_double_count(self):
        self._grant(1, 100, when=self.now)
        self._grant(2, 40, when=self.now)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)
        cursor_after = dict(self.db.xp_snapshot_state.docs[snap.CURSOR_ID])

        # Simulate a crash right before the cursor was persisted: roll it back
        # to the pre-batch value and rerun the exact same batch.
        self.db.xp_snapshot_state.docs[snap.CURSOR_ID]["last_event_id"] = None
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)

        self.assertEqual(self.db.users.docs[1]["total_xp"], 100)
        self.assertEqual(self.db.users.docs[2]["total_xp"], 40)
        # cursor healed back to the correct forward position
        self.assertEqual(self.db.xp_snapshot_state.docs[snap.CURSOR_ID]["last_event_id"], cursor_after["last_event_id"])

    def test_two_workers_racing_same_batch_no_double_count(self):
        self._grant(1, 60, when=self.now)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)
        stale_cursor = None  # first run already bootstrapped + applied

        # Worker B reads a stale (pre-advance) cursor concurrently and tries
        # to apply the same batch again.
        self.db.xp_snapshot_state.docs[snap.CURSOR_ID]["last_event_id"] = stale_cursor
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)

        self.assertEqual(self.db.users.docs[1]["total_xp"], 60)

    def test_weekly_monthly_lifetime_totals(self):
        # self.now is Tue 2026-07-14; the KL week starts Mon 2026-07-13.
        last_week = self.now - timedelta(days=8)  # 2026-07-06: same month, prior week
        last_month = self.now.replace(day=1) - timedelta(days=5)  # 2026-06-26: prior month
        self._grant(1, 10, when=last_month)
        self._grant(1, 20, when=last_week)
        self._grant(1, 30, when=self.now)

        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)

        self.assertEqual(self.db.users.docs[1]["total_xp"], 60)
        self.assertEqual(self.db.users.docs[1]["weekly_xp"], 30)
        self.assertEqual(self.db.users.docs[1]["monthly_xp"], 50)  # last_week + now, both July

    def test_invalidated_xp_reversed_and_not_double_reversed(self):
        ev = self._grant(1, 75, when=self.now)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)
        self.assertEqual(self.db.users.docs[1]["total_xp"], 75)

        # Retroactive invalidation (e.g. rollback script) after it was already counted.
        for d in self.db.xp_events.docs:
            if d["_id"] == ev["_id"]:
                d["invalidated"] = True
                d["invalidated_at"] = self.now + timedelta(minutes=1)

        summary = snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now + timedelta(minutes=2))
        self.assertEqual(self.db.users.docs[1]["total_xp"], 0)
        self.assertEqual(summary["corrections_applied"], 1)

        # Running again must not subtract a second time.
        summary2 = snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now + timedelta(minutes=3))
        self.assertEqual(self.db.users.docs[1]["total_xp"], 0)
        self.assertEqual(summary2["corrections_applied"], 0)

    def test_weekly_rollover_resets_only_weekly(self):
        self._grant(1, 40, when=self.now)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)
        self.assertEqual(self.db.users.docs[1]["weekly_xp"], 40)
        self.assertEqual(self.db.users.docs[1]["total_xp"], 40)

        next_week = self.now + timedelta(days=7)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=next_week)
        self.assertEqual(self.db.users.docs[1]["weekly_xp"], 0)
        self.assertEqual(self.db.users.docs[1]["total_xp"], 40)  # lifetime unaffected

    def test_query_volume_scales_with_new_events_not_history(self):
        for i in range(50):
            self._grant(1, 1, when=self.now)
        snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)

        self._grant(1, 1, when=self.now)
        summary = snap.settle_xp_snapshots_incremental(self.db, now_utc_ts=self.now)
        self.assertEqual(summary["scanned"], 1)  # not 51


class GrantXPDedupeStillHolds(unittest.TestCase):
    def test_duplicate_unique_key_produces_one_xp_event(self):
        # Snapshot layer relies on xp_events already being deduped by
        # grant_xp (xp.py); covered end-to-end in test_xp.py's
        # test_idempotent_grant. Sanity-check the invariant here too.
        from xp import grant_xp

        class _XPEvents:
            def __init__(self):
                self.store = {}

            def find_one(self, filt, projection=None):
                return self.store.get((filt.get("user_id"), filt.get("unique_key")))

            def update_one(self, filt, update, upsert=False):
                key = (filt.get("user_id"), filt.get("unique_key"))
                if key in self.store:
                    return _Result()
                self.store[key] = {**filt, **update.get("$setOnInsert", {})}
                return _Result(upserted_id=1)

        class _Ledger:
            def __init__(self):
                self.store = {}

            def update_one(self, filt, update, upsert=False):
                key = (filt.get("user_id"), filt.get("source"), filt.get("source_id"))
                if key in self.store:
                    return _Result()
                self.store[key] = {**filt}
                return _Result(upserted_id=1)

            def delete_one(self, filt):
                pass

        class _Users:
            def find_one(self, filt, projection=None):
                return None

        class _DB:
            def __init__(self):
                self.xp_events = _XPEvents()
                self.xp_ledger = _Ledger()
                self.users = _Users()

        db = _DB()
        first = grant_xp(db, 1, "checkin", "checkin:20260714", 20)
        second = grant_xp(db, 1, "checkin", "checkin:20260714", 20)
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(db.xp_events.store), 1)


class SettleXPSnapshotsDispatchTests(unittest.TestCase):
    def test_dispatches_to_incremental_by_default(self):
        called = {}

        def _fake_incremental(db):
            called["ran"] = True

        original_module = scheduler.__dict__.get("xp_snapshot")
        import xp_snapshot as real_module

        original_fn = real_module.settle_xp_snapshots_incremental
        real_module.settle_xp_snapshots_incremental = _fake_incremental
        original_db = scheduler.db
        scheduler.db = object()
        try:
            scheduler.settle_xp_snapshots()
        finally:
            real_module.settle_xp_snapshots_incremental = original_fn
            scheduler.db = original_db
        self.assertTrue(called.get("ran"))

    def test_rollback_flag_uses_legacy_full_rebuild(self):
        import os

        called = {}
        original = scheduler._settle_xp_snapshots_full_rebuild
        scheduler._settle_xp_snapshots_full_rebuild = lambda: called.setdefault("ran", True)
        os.environ["XP_SNAPSHOT_INCREMENTAL"] = "0"
        try:
            scheduler.settle_xp_snapshots()
        finally:
            scheduler._settle_xp_snapshots_full_rebuild = original
            os.environ.pop("XP_SNAPSHOT_INCREMENTAL", None)
        self.assertTrue(called.get("ran"))


if __name__ == "__main__":
    unittest.main()
