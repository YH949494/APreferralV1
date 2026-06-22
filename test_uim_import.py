import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

import uim_import


FIXED_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def _make_rows():
    headers = ["telegram_user_id"] + [""] * 124 + ["for_bot_segment"]
    return [
        headers,
        ["100"] + [""] * 124 + ["Voucher Hunter"],
        ["101"] + [""] * 124 + ["High Value"],
        ["999"] + [""] * 124 + ["Voucher Hunter"],
    ]


class FakeBulkResult:
    def __init__(self, modified_count=0, upserted_count=0):
        self.modified_count = modified_count
        self.upserted_count = upserted_count


class FakeUsersCol:
    def __init__(self, docs):
        self.docs = {int(d["user_id"]): dict(d) for d in docs}
        self.bulk_calls = 0

    def find(self, filt, projection=None):
        ids = filt.get("user_id", {}).get("$in", [])
        results = []
        for uid in ids:
            if uid in self.docs:
                results.append(dict(self.docs[uid]))
        return results

    def bulk_write(self, ops, ordered=False):
        self.bulk_calls += 1
        modified = 0
        for op in ops:
            filt = getattr(op, "_filter", {})
            update = getattr(op, "_doc", {})
            uid = int(filt.get("user_id", -1))
            if uid in self.docs:
                self.docs[uid].update(update.get("$set", {}))
                modified += 1
        return FakeBulkResult(modified_count=modified)


class FakeBatchesCol:
    def __init__(self):
        self.docs = {}

    def insert_one(self, doc):
        self.docs[doc["batch_id"]] = dict(doc)

    def find_one(self, filt):
        bid = filt.get("batch_id")
        return self.docs.get(bid)

    def update_one(self, filt, update):
        bid = filt.get("batch_id")
        if bid in self.docs:
            self.docs[bid].update(update.get("$set", {}))

    def find(self, filt=None, projection=None):
        return _FakeCursor(list(self.docs.values()))


class _FakeCursor:
    def __init__(self, items):
        self._items = items

    def sort(self, *a, **kw):
        return self

    def limit(self, n):
        return iter(self._items[:n])


class FakeSegSnapshotsCol:
    def __init__(self):
        self.docs = {}
        self.bulk_calls = 0

    def bulk_write(self, ops, ordered=False):
        self.bulk_calls += 1
        for op in ops:
            filt = getattr(op, "_filter", {})
            update = getattr(op, "_doc", {})
            uid = int(filt.get("user_id", -1))
            self.docs[uid] = update.get("$set", {})
        return FakeBulkResult(modified_count=len(ops))


class TestCommitBatch(unittest.TestCase):
    def _cols(self):
        users = FakeUsersCol([
            {"user_id": 100, "for_bot_segment": "", "for_bot_segment_normalized": "", "bot_segment_probability": 0.0},
            {"user_id": 101, "for_bot_segment": "", "for_bot_segment_normalized": "", "bot_segment_probability": 0.0},
        ])
        batches = FakeBatchesCol()
        snapshots = FakeSegSnapshotsCol()
        return users, batches, snapshots

    def test_commit_batch_success(self):
        users, batches, snapshots = self._cols()
        result = uim_import.commit_batch(
            _make_rows(),
            batches_col=batches,
            users_col=users,
            segment_snapshots_col=snapshots,
            now=FIXED_NOW,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "committed")
        self.assertIn("batch_id", result)
        self.assertEqual(result["seg_sync_status"], "pending")

    def test_commit_batch_rows_written(self):
        users, batches, snapshots = self._cols()
        result = uim_import.commit_batch(
            _make_rows(),
            batches_col=batches,
            users_col=users,
            segment_snapshots_col=snapshots,
            now=FIXED_NOW,
        )
        self.assertEqual(result["rows_written"], 2)
        self.assertEqual(result["users_updated"], 2)

    def test_seg_sync_status_after_commit(self):
        users, batches, snapshots = self._cols()
        result = uim_import.commit_batch(
            _make_rows(),
            batches_col=batches,
            users_col=users,
            segment_snapshots_col=snapshots,
            now=FIXED_NOW,
        )
        batch_doc = batches.find_one({"batch_id": result["batch_id"]})
        self.assertEqual(batch_doc["seg_sync_status"], "pending")
        self.assertIsNone(batch_doc["seg_sync_started_at"])
        self.assertIsNone(batch_doc["seg_sync_completed_at"])
        self.assertIsNone(batch_doc["seg_sync_error"])
        self.assertIsNone(batch_doc["seg_sync_rows_synced"])


class TestRunSegSync(unittest.TestCase):
    def _setup(self):
        users = FakeUsersCol([
            {"user_id": 100, "for_bot_segment": "Voucher Hunter", "for_bot_segment_normalized": "voucher_hunter", "bot_segment_probability": 0.1},
            {"user_id": 101, "for_bot_segment": "High Value", "for_bot_segment_normalized": "high_value", "bot_segment_probability": 0.0},
        ])
        batches = FakeBatchesCol()
        snapshots = FakeSegSnapshotsCol()
        batch_id = "test-batch-001"
        batches.docs[batch_id] = {
            "batch_id": batch_id,
            "status": "committed",
            "user_ids": [100, 101],
            "seg_sync_status": "pending",
        }
        return batch_id, users, batches, snapshots

    def test_run_seg_sync_success(self):
        batch_id, users, batches, snapshots = self._setup()
        result = uim_import.run_seg_sync(
            batch_id,
            batches_col=batches,
            users_col=users,
            segment_snapshots_col=snapshots,
            now=FIXED_NOW,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["seg_sync_status"], "completed")
        batch_doc = batches.find_one({"batch_id": batch_id})
        self.assertEqual(batch_doc["seg_sync_status"], "completed")
        self.assertIsNotNone(batch_doc["seg_sync_completed_at"])

    def test_run_seg_sync_failure(self):
        batch_id, users, batches, snapshots = self._setup()
        snapshots.bulk_write = MagicMock(side_effect=RuntimeError("mongo down"))
        result = uim_import.run_seg_sync(
            batch_id,
            batches_col=batches,
            users_col=users,
            segment_snapshots_col=snapshots,
            now=FIXED_NOW,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["seg_sync_status"], "failed")
        self.assertIn("mongo down", result["error"])
        batch_doc = batches.find_one({"batch_id": batch_id})
        self.assertEqual(batch_doc["seg_sync_status"], "failed")
        self.assertIn("mongo down", batch_doc["seg_sync_error"])


class TestRenderSegSyncStatus(unittest.TestCase):
    def test_render_seg_sync_status_missing(self):
        self.assertEqual(uim_import.render_seg_sync_status(None), "Not Started")

    def test_render_seg_sync_status_pending(self):
        self.assertEqual(uim_import.render_seg_sync_status("pending"), "Not Started")

    def test_render_seg_sync_status_running(self):
        self.assertEqual(uim_import.render_seg_sync_status("running"), "Syncing")

    def test_render_seg_sync_status_completed(self):
        self.assertEqual(uim_import.render_seg_sync_status("completed"), "Synced")

    def test_render_seg_sync_status_failed(self):
        self.assertEqual(uim_import.render_seg_sync_status("failed"), "Failed")


if __name__ == "__main__":
    unittest.main()
