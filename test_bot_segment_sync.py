import os
import unittest
from unittest.mock import patch

import bot_segment_sync as sync


class FakeBulkResult:
    def __init__(self, modified_count):
        self.modified_count = modified_count


class FakeUsersCollection:
    def __init__(self, docs):
        self.docs = {int(doc["user_id"]): dict(doc) for doc in docs}
        self.bulk_calls = 0

    def find(self, filt, projection=None):  # noqa: ARG002
        ids = filt.get("user_id", {}).get("$in", [])
        return [{"user_id": uid} for uid in ids if uid in self.docs]

    def bulk_write(self, ops, ordered=False):  # noqa: ARG002
        self.bulk_calls += 1
        modified = 0
        for op in ops:
            filt = getattr(op, "_filter", {})
            update = getattr(op, "_doc", {})
            uid = int(filt["user_id"])
            if uid in self.docs:
                self.docs[uid].update(update.get("$set", {}))
                modified += 1
        return FakeBulkResult(modified)


class BotSegmentSyncTests(unittest.TestCase):
    def _rows(self):
        headers = [""] * 126
        headers[0] = "telegram_user_id"
        headers[125] = "for_bot_segment"
        return [
            headers,
            ["100"] + [""] * 124 + ["Voucher Hunter"],
            ["101"] + [""] * 124 + [""],
            ["102"] + [""] * 124 + ["Unknown Segment"],
            ["missing"] + [""] * 124 + ["High Value"],
            ["999"] + [""] * 124 + ["High Value"],
        ]

    def test_dry_run_makes_no_writes_and_reports_counters(self):
        users = FakeUsersCollection([{"user_id": 100}, {"user_id": 101}, {"user_id": 102}])
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_bot_segments_from_sheet(dry_run=True, users_col=users, rows=self._rows())
        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_scanned"], 5)
        self.assertEqual(summary["valid_user_ids"], 4)
        self.assertEqual(summary["users_matched"], 3)
        self.assertEqual(summary["users_missing_in_db"], 1)
        self.assertEqual(summary["blank_segments"], 1)
        self.assertEqual(summary["unknown_segments"], 1)
        self.assertEqual(summary["invalid_user_ids"], 1)
        self.assertEqual(summary["users_write_attempted"], 0)
        self.assertEqual(summary["users_modified"], 0)
        self.assertEqual(users.bulk_calls, 0)
        self.assertNotIn("for_bot_segment_normalized", users.docs[100])

    def test_commit_updates_existing_users_only(self):
        users = FakeUsersCollection([{"user_id": 100}, {"user_id": 101}, {"user_id": 102}])
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_bot_segments_from_sheet(dry_run=False, users_col=users, rows=self._rows())
        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["users_write_attempted"], 3)
        self.assertEqual(summary["users_modified"], 3)
        self.assertEqual(summary["users_updated"], 3)
        self.assertEqual(users.docs[100]["for_bot_segment"], "Voucher Hunter")
        self.assertEqual(users.docs[100]["for_bot_segment_normalized"], "voucher_hunter")
        self.assertEqual(users.docs[100]["bot_segment_probability"], 0.10)
        self.assertEqual(users.docs[101]["for_bot_segment_normalized"], "unclassified")
        self.assertEqual(users.docs[101]["bot_segment_probability"], 0.70)
        self.assertEqual(users.docs[102]["for_bot_segment_normalized"], "unclassified")
        self.assertEqual(users.docs[102]["bot_segment_probability"], 0.70)
        self.assertNotIn(999, users.docs)

    def test_missing_credentials_fail_safely(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_bot_segments_from_sheet(dry_run=True, users_col=FakeUsersCollection([]), rows=None)
        init_db.assert_not_called()
        self.assertFalse(summary["ok"])
        self.assertIn("missing Google service account credentials", summary["error"])

    def test_default_collection_path_initializes_db_after_fetch(self):
        users = FakeUsersCollection([{"user_id": 100}])
        with patch.object(sync, "fetch_sheet_rows", return_value=self._rows()) as fetch_rows, patch.object(
            sync.database, "init_db"
        ) as init_db, patch.object(sync.database, "users_collection", users):
            summary = sync.sync_bot_segments_from_sheet(dry_run=True, users_col=None, rows=None)
        fetch_rows.assert_called_once()
        init_db.assert_called_once()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["users_matched"], 1)


if __name__ == "__main__":
    unittest.main()
