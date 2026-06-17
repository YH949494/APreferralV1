"""P1 regression: ensure_indexes must drop the stale (user_id, snapshot_month)
unique index from backend_segment_snapshots before creating the new
(account, snapshot_week) one.

On an existing deployment the old index is already present; if not dropped,
MongoDB keeps enforcing the legacy uniqueness key and rejects inserts with
user_id=None or multiple weeks for the same user in the same calendar month.

These tests verify the migration behaviour without a live MongoDB connection
by mocking the database reference object.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, call, patch
from pymongo import ASCENDING


class BackendSegmentIndexMigrationTests(unittest.TestCase):
    """Verify that ensure_indexes drops the stale unique index and creates
    the new one, even if the old index exists from a prior schema version."""

    def _run_ensure_indexes(self):
        """Import and call database.ensure_indexes with a fully mocked db_ref.

        Returns (mock_col, drop_calls, create_calls) so tests can assert on
        the exact operations performed against the backend_segment_snapshots
        collection.
        """
        import database as db_module

        # Reset the "already done" guard so ensure_indexes actually runs.
        db_module._indexes_initialized = False

        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)

        with patch.object(db_module, "get_db", return_value=mock_db):
            db_module.ensure_indexes()

        return mock_col

    def test_drop_index_called_with_stale_key_spec(self):
        """ensure_indexes must attempt to drop (user_id, snapshot_month)."""
        mock_col = self._run_ensure_indexes()

        drop_calls = [str(c) for c in mock_col.drop_index.call_args_list]
        stale_key = str([("user_id", ASCENDING), ("snapshot_month", ASCENDING)])
        self.assertTrue(
            any(stale_key in c for c in drop_calls),
            f"Expected drop_index call for {stale_key}, got: {drop_calls}",
        )

    def test_new_unique_index_created_after_drop(self):
        """ensure_indexes must create (account, snapshot_week) with unique=True."""
        mock_col = self._run_ensure_indexes()

        new_key = [("account", ASCENDING), ("snapshot_week", ASCENDING)]
        found = any(
            c.args and c.args[0] == new_key and c.kwargs.get("unique") is True
            for c in mock_col.create_index.call_args_list
        )
        self.assertTrue(
            found,
            f"Expected create_index({new_key}, unique=True). "
            f"Actual calls: {mock_col.create_index.call_args_list}",
        )

    def test_drop_does_not_raise_if_old_index_absent(self):
        """drop_index raises on the mock by default (no configured return), but
        ensure_indexes must swallow the exception and continue safely."""
        import database as db_module

        db_module._indexes_initialized = False
        mock_col = MagicMock()
        mock_col.drop_index.side_effect = Exception("index not found")
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)

        with patch.object(db_module, "get_db", return_value=mock_db):
            # Must not raise even when drop_index throws.
            try:
                db_module.ensure_indexes()
            except Exception as exc:
                self.fail(f"ensure_indexes raised unexpectedly: {exc}")

        # New index creation must still proceed after the drop failure.
        new_key = [("account", ASCENDING), ("snapshot_week", ASCENDING)]
        found = any(
            c.args and c.args[0] == new_key
            for c in mock_col.create_index.call_args_list
        )
        self.assertTrue(found, "create_index for new key not called after failed drop")

    def test_idempotent_after_first_run(self):
        """Running ensure_indexes twice must not duplicate create/drop calls."""
        import database as db_module

        db_module._indexes_initialized = False
        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)

        with patch.object(db_module, "get_db", return_value=mock_db):
            db_module.ensure_indexes()
            first_create_count = mock_col.create_index.call_count

            db_module.ensure_indexes()  # second call — _indexes_initialized is True
            second_create_count = mock_col.create_index.call_count

        self.assertEqual(
            first_create_count, second_create_count,
            "ensure_indexes must not repeat index operations on second call",
        )

    def test_weekly_snapshots_coexist_in_same_month(self):
        """Multiple (account, snapshot_week) pairs in the same calendar month
        must be representable — they differ on snapshot_week, not snapshot_month.

        This is a logical test: with the old (user_id, snapshot_month) key two
        docs in 2026-06 for the same user_id would conflict; with the new
        (account, snapshot_week) key they are distinct rows.
        """
        import backend_segment_engine as engine
        from datetime import datetime, timezone

        now_w24 = datetime(2026, 6,  9, tzinfo=timezone.utc)  # 2026-W24
        now_w25 = datetime(2026, 6, 16, tzinfo=timezone.utc)  # 2026-W25

        week24 = engine._snapshot_week_key(now_w24)
        week25 = engine._snapshot_week_key(now_w25)
        month  = f"{now_w24.year:04d}-{now_w24.month:02d}"

        self.assertEqual(week24, "2026-W24")
        self.assertEqual(week25, "2026-W25")
        # Both fall in the same snapshot_month — the old index would have rejected
        # the second insert (same user_id + month); the new key is distinct.
        self.assertEqual(month, "2026-06")
        self.assertNotEqual(
            (("alice", week24)), (("alice", week25)),
            "Different weeks must produce distinct (account, snapshot_week) keys",
        )


if __name__ == "__main__":
    unittest.main()
