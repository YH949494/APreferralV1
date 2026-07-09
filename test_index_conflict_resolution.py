"""Regression tests for MongoDB index-name conflicts on startup.

Production crashed with OperationFailure code 85 (IndexOptionsConflict)
because voucher_claims/voucher_pools already carried unique indexes on the
same key pattern under a different name than the code requested
(drop_id_1_user_id_1 vs uq_claim_drop_user, uniq_pool_code vs
uq_voucher_pool_code). These tests verify database._ensure_equivalent_index
reuses a compatible existing index instead of trying (and failing) to
create a duplicate, without ever dropping anything.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from pymongo.errors import OperationFailure

import database as db_module


def _index_doc(name, keys, unique=False, partial=None):
    doc = {"name": name, "key": dict(keys)}
    if unique:
        doc["unique"] = True
    if partial is not None:
        doc["partialFilterExpression"] = partial
    return doc


class EnsureEquivalentIndexTests(unittest.TestCase):
    def _mock_collection(self, existing_indexes):
        col = MagicMock()
        col.name = "test_collection"
        col.list_indexes.return_value = list(existing_indexes)
        return col

    def test_reuses_existing_index_with_same_keys_and_unique_flag(self):
        """voucher_pools already has uniq_pool_code; requesting
        uq_voucher_pool_code on the same keys must reuse it, not create."""
        col = self._mock_collection(
            [_index_doc("uniq_pool_code", [("pool_id", 1), ("code", 1)], unique=True)]
        )
        result = db_module._ensure_equivalent_index(
            col, [("pool_id", 1), ("code", 1)], unique=True, name="uq_voucher_pool_code"
        )
        self.assertEqual(result, "uniq_pool_code")
        col.create_index.assert_not_called()

    def test_reuses_existing_index_for_voucher_claims(self):
        """voucher_claims already has drop_id_1_user_id_1; requesting
        uq_claim_drop_user on the same keys must reuse it, not create."""
        col = self._mock_collection(
            [_index_doc("drop_id_1_user_id_1", [("drop_id", 1), ("user_id", 1)], unique=True)]
        )
        result = db_module._ensure_equivalent_index(
            col, [("drop_id", 1), ("user_id", 1)], unique=True, name="uq_claim_drop_user"
        )
        self.assertEqual(result, "drop_id_1_user_id_1")
        col.create_index.assert_not_called()

    def test_creates_index_when_no_equivalent_exists(self):
        col = self._mock_collection([])
        col.create_index.return_value = "uq_claim_drop_user"
        result = db_module._ensure_equivalent_index(
            col, [("drop_id", 1), ("user_id", 1)], unique=True, name="uq_claim_drop_user"
        )
        self.assertEqual(result, "uq_claim_drop_user")
        col.create_index.assert_called_once()

    def test_raises_on_incompatible_uniqueness(self):
        """Same keys but existing index is non-unique while unique is requested:
        this is a genuine conflict, not something safe to silently paper over."""
        col = self._mock_collection(
            [_index_doc("ix_drop_user", [("drop_id", 1), ("user_id", 1)], unique=False)]
        )
        with self.assertRaises(OperationFailure):
            db_module._ensure_equivalent_index(
                col, [("drop_id", 1), ("user_id", 1)], unique=True, name="uq_claim_drop_user"
            )
        col.create_index.assert_not_called()

    def test_never_calls_drop_index(self):
        col = self._mock_collection(
            [_index_doc("uniq_pool_code", [("pool_id", 1), ("code", 1)], unique=True)]
        )
        db_module._ensure_equivalent_index(
            col, [("pool_id", 1), ("code", 1)], unique=True, name="uq_voucher_pool_code"
        )
        col.drop_index.assert_not_called()

    def test_concurrent_create_race_is_reused_not_raised(self):
        """If create_index races with another worker and Mongo reports the
        index already exists (code 85), fall back to reusing it instead of
        crashing the whole process."""
        col = self._mock_collection([])
        col.create_index.side_effect = OperationFailure("conflict", code=85)
        # After the failed create, a second list_indexes() lookup (inside
        # _find_equivalent_index_name) reports the index that won the race.
        col.list_indexes.side_effect = [
            [],
            [_index_doc("uq_claim_drop_user", [("drop_id", 1), ("user_id", 1)], unique=True)],
        ]
        result = db_module._ensure_equivalent_index(
            col, [("drop_id", 1), ("user_id", 1)], unique=True, name="uq_claim_drop_user"
        )
        self.assertEqual(result, "uq_claim_drop_user")


class ReactivationJourneyIndexIntegrationTests(unittest.TestCase):
    """ensure_reactivation_journey_indexes() must not crash when
    voucher_pools already has uniq_pool_code on (pool_id, code)."""

    def test_startup_survives_pre_existing_uniq_pool_code(self):
        import reactivation_journey as journey

        journeys_col = MagicMock()
        journeys_col.list_indexes.return_value = []
        journeys_col.create_index.return_value = "ok"

        voucher_pools_col = MagicMock()
        voucher_pools_col.name = "voucher_pools"
        voucher_pools_col.list_indexes.return_value = [
            _index_doc("uniq_pool_code", [("pool_id", 1), ("code", 1)], unique=True)
        ]

        fake_db = {
            "reactivation_journey": journeys_col,
            "voucher_pools": voucher_pools_col,
        }

        # Should not raise even though the code asks for uq_voucher_pool_code
        # and the collection already enforces the same key/uniqueness under
        # a different name.
        journey.ensure_reactivation_journey_indexes(fake_db)

        voucher_pools_col.create_index.assert_any_call(
            [("pool_id", 1), ("status", 1)], name="ix_voucher_pool_status"
        )
        # The unique (pool_id, code) index must never be (re)created or dropped.
        for call in voucher_pools_col.create_index.call_args_list:
            args = call.args[0] if call.args else call.kwargs.get("keys")
            self.assertNotEqual(
                args, [("pool_id", 1), ("code", 1)],
                "must reuse existing uniq_pool_code, not attempt to create a duplicate",
            )
        voucher_pools_col.drop_index.assert_not_called()


if __name__ == "__main__":
    unittest.main()
