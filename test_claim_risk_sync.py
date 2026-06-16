import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import claim_risk_sync as sync


class FakeBulkResult:
    def __init__(self, modified_count=0, upserted_count=0):
        self.modified_count = modified_count
        self.upserted_count = upserted_count


class FakeUsersCollection:
    def __init__(self, docs):
        self.docs = {int(doc["user_id"]): dict(doc) for doc in docs}
        self.bulk_calls = 0

    def find(self, filt, projection=None):  # noqa: ARG002
        ids = filt.get("user_id", {}).get("$in", [])
        out = []
        for uid in ids:
            if uid in self.docs:
                doc = {"user_id": uid, "claim_risk_level": self.docs[uid].get("claim_risk_level")}
                out.append(doc)
        return out

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
        return FakeBulkResult(modified_count=modified)


class FakeHistoryCollection:
    def __init__(self):
        self.docs = {}
        self.bulk_calls = 0

    def bulk_write(self, ops, ordered=False):  # noqa: ARG002
        self.bulk_calls += 1
        upserted = 0
        for op in ops:
            filt = getattr(op, "_filter", {})
            update = getattr(op, "_doc", {})
            key = filt["_id"]
            if key not in self.docs:
                self.docs[key] = dict(update.get("$setOnInsert", {}))
                upserted += 1
        return FakeBulkResult(upserted_count=upserted)


class ClaimRiskSyncTests(unittest.TestCase):
    def _headers(self):
        headers = [""] * 4
        headers[0] = "user_id"
        headers[1] = "claim_risk_level"
        headers[2] = "claim_risk_reason"
        headers[3] = "shared_account_risk_level"
        return headers

    def _rows(self):
        return [
            self._headers(),
            ["100", "high_risk_review", "37 claims in one month", "high_risk"],
            ["101", "normal", "", ""],
            ["102", "", "no risk text", ""],
            ["missing", "high_risk_review", "bad id", ""],
            ["999", "medium_risk", "not in db", ""],
        ]

    def test_dry_run_makes_no_writes_and_reports_counters(self):
        users = FakeUsersCollection(
            [{"user_id": 100}, {"user_id": 101}, {"user_id": 102}]
        )
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_claim_risk_from_sheet(dry_run=True, users_col=users, rows=self._rows())
        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_scanned"], 5)
        # Row 102 has a blank claim_risk_level and is skipped (not a valid update).
        self.assertEqual(summary["valid_user_ids"], 3)
        self.assertEqual(summary["users_matched"], 2)
        self.assertEqual(summary["users_missing_in_db"], 1)
        self.assertEqual(summary["invalid_user_ids"], 1)
        self.assertEqual(summary["users_write_attempted"], 0)
        self.assertEqual(summary["users_modified"], 0)
        self.assertEqual(users.bulk_calls, 0)
        self.assertNotIn("claim_risk_level", users.docs[100])

    def test_commit_updates_existing_users_only(self):
        users = FakeUsersCollection(
            [{"user_id": 100}, {"user_id": 101}, {"user_id": 102}]
        )
        history = FakeHistoryCollection()
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_claim_risk_from_sheet(
                dry_run=False, users_col=users, history_col=history, rows=self._rows()
            )
        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["users_write_attempted"], 2)
        self.assertEqual(summary["users_modified"], 2)
        self.assertEqual(users.docs[100]["claim_risk_level"], "high_risk_review")
        self.assertEqual(users.docs[100]["claim_risk_reason"], "37 claims in one month")
        self.assertEqual(users.docs[100]["shared_account_risk_level"], "high_risk")
        self.assertEqual(users.docs[100]["claim_risk_source"], "UIM")
        self.assertEqual(users.docs[101]["claim_risk_level"], "normal")
        self.assertIsNone(users.docs[101]["claim_risk_reason"])
        self.assertNotIn(999, users.docs)
        self.assertNotIn("claim_risk_level", users.docs[102])
        # Both matched users had no prior claim_risk_level -> both are "changes".
        self.assertEqual(summary["history_written"], 2)
        self.assertEqual(len(history.docs), 2)

    def test_history_written_only_when_value_changes(self):
        users = FakeUsersCollection(
            [{"user_id": 100, "claim_risk_level": "high_risk_review"}, {"user_id": 101}]
        )
        history = FakeHistoryCollection()
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_claim_risk_from_sheet(
                dry_run=False, users_col=users, history_col=history, rows=self._rows()
            )
        self.assertTrue(summary["ok"])
        # user 100's level is unchanged (still high_risk_review) -> no history row.
        # user 101's level changed from None -> "normal" -> history row written.
        self.assertEqual(summary["history_written"], 1)
        self.assertEqual(len(history.docs), 1)
        entry = next(iter(history.docs.values()))
        self.assertEqual(entry["user_id"], 101)
        self.assertEqual(entry["old_claim_risk_level"], None)
        self.assertEqual(entry["new_claim_risk_level"], "normal")
        self.assertEqual(entry["source"], "UIM")

    def test_missing_claim_risk_level_column_skips_without_crash(self):
        rows = [
            ["user_id", "some_other_column"],
            ["100", "x"],
        ]
        users = FakeUsersCollection([{"user_id": 100}])
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_claim_risk_from_sheet(dry_run=False, users_col=users, rows=rows)
        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertIsNotNone(summary["skipped_reason"])
        self.assertIn("claim_risk_level", summary["skipped_reason"])
        self.assertFalse(summary["source_columns_present"]["claim_risk_level"])
        self.assertEqual(summary["users_modified"], 0)
        self.assertEqual(users.bulk_calls, 0)

    def test_missing_optional_columns_still_sync_required_field(self):
        rows = [
            ["user_id", "claim_risk_level"],
            ["100", "medium_risk"],
        ]
        users = FakeUsersCollection([{"user_id": 100}])
        history = FakeHistoryCollection()
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_claim_risk_from_sheet(
                dry_run=False, users_col=users, history_col=history, rows=rows
            )
        self.assertTrue(summary["ok"])
        self.assertIsNone(summary["skipped_reason"])
        self.assertEqual(users.docs[100]["claim_risk_level"], "medium_risk")
        self.assertIsNone(users.docs[100]["claim_risk_reason"])
        self.assertIsNone(users.docs[100]["shared_account_risk_level"])

    def test_default_collection_path_initializes_db_after_fetch(self):
        users = FakeUsersCollection([{"user_id": 100}])
        with patch.object(sync, "bot_segment_sync") as fake_bot_segment_sync, patch.object(
            sync.database, "init_db"
        ) as init_db, patch.object(sync.database, "users_collection", users):
            fake_bot_segment_sync.fetch_sheet_rows.return_value = self._rows()
            fake_bot_segment_sync.DEFAULT_BOT_SEGMENT_SHEET_ID = "sheet-id"
            fake_bot_segment_sync.DEFAULT_BOT_SEGMENT_SHEET_GID = "gid"
            summary = sync.sync_claim_risk_from_sheet(dry_run=True, users_col=None, rows=None)
        init_db.assert_called_once()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["users_matched"], 1)

    def test_env_override_takes_precedence_over_hardcoded_defaults(self):
        users = FakeUsersCollection([{"user_id": 100}])
        env = {
            "BOT_SEGMENT_SHEET_ID": "env-sheet-id",
            "BOT_SEGMENT_SHEET_GID": "env-gid",
        }
        with patch.dict(os.environ, env), patch.object(
            sync.bot_segment_sync, "fetch_sheet_rows", return_value=self._rows()
        ) as fetch_sheet_rows:
            summary = sync.sync_claim_risk_from_sheet(dry_run=True, users_col=users, rows=None)
        fetch_sheet_rows.assert_called_once_with(spreadsheet_id="env-sheet-id", worksheet_gid="env-gid")
        self.assertTrue(summary["ok"])

    def test_explicit_args_take_precedence_over_env_override(self):
        users = FakeUsersCollection([{"user_id": 100}])
        env = {
            "BOT_SEGMENT_SHEET_ID": "env-sheet-id",
            "BOT_SEGMENT_SHEET_GID": "env-gid",
        }
        with patch.dict(os.environ, env), patch.object(
            sync.bot_segment_sync, "fetch_sheet_rows", return_value=self._rows()
        ) as fetch_sheet_rows:
            summary = sync.sync_claim_risk_from_sheet(
                dry_run=True,
                users_col=users,
                rows=None,
                spreadsheet_id="explicit-sheet-id",
                worksheet_gid="explicit-gid",
            )
        fetch_sheet_rows.assert_called_once_with(spreadsheet_id="explicit-sheet-id", worksheet_gid="explicit-gid")
        self.assertTrue(summary["ok"])


if __name__ == "__main__":
    unittest.main()
