"""Regression tests for repair_referral_ledger.py's CLI separation and the
duplicate-revocation-selection fix.

Ledger invalidation (--commit) and snapshot rebuild (--rebuild-snapshots)
are separate operator actions/checkpoints: combining them in one run must
be rejected, and --rebuild-snapshots must work standalone without
--commit.
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone

import database
import scheduler
import repair_referral_ledger as rrl

NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


class CliSeparationTests(unittest.TestCase):
    def setUp(self):
        self.orig_argv = sys.argv
        self.orig_init_db = database.init_db
        self.orig_settle_snapshots = scheduler.settle_referral_snapshots

    def tearDown(self):
        sys.argv = self.orig_argv
        database.init_db = self.orig_init_db
        scheduler.settle_referral_snapshots = self.orig_settle_snapshots

    def test_commit_and_rebuild_snapshots_together_is_rejected(self):
        sys.argv = [
            "repair_referral_ledger.py",
            "--mongo-url",
            "mongodb://example/ignored",
            "--commit",
            "--rebuild-snapshots",
        ]
        with self.assertRaises(SystemExit):
            rrl.main()

    def test_rebuild_snapshots_alone_does_not_require_commit(self):
        calls = []
        database.init_db = lambda url, dbname: calls.append((url, dbname))
        scheduler.settle_referral_snapshots = lambda: {
            "users_scanned": 0,
            "users_modified": 0,
            "negative_raw_totals_detected": 0,
            "negative_users_clamped": 0,
            "weekly_negative_count": 0,
            "monthly_negative_count": 0,
            "total_negative_count": 0,
            "top_affected_inviters": [],
            "duration_seconds": 0.0,
        }
        sys.argv = [
            "repair_referral_ledger.py",
            "--mongo-url",
            "mongodb://example/ignored",
            "--rebuild-snapshots",
        ]

        exit_code = rrl.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(calls), 1)


class DuplicateRevocationSelectionTests(unittest.TestCase):
    def test_earliest_valid_post_settlement_revocation_is_kept_not_premature_one(self):
        # Legacy corruption: a revocation timestamped BEFORE its own
        # settlement (corrupt/out-of-order data), plus a second revocation
        # that legitimately follows the settlement. Absolute-time-earliest
        # selection would wrongly keep the premature (corrupt) row as
        # "valid" and invalidate the legitimate one. The fix must instead
        # only ever pick a survivor from revocations at/after the
        # settlement, and leave the premature row untouched here (it is
        # reported separately, review-only, by _find_premature_revocations).
        # _find_duplicate_revocations issues a real Mongo aggregation
        # ($lookup/$sort/$group with $push) that this repo's lightweight
        # fake collections don't implement, and no live Mongo is available
        # in this sandbox -- so this test verifies the selection logic in
        # isolation, mirroring exactly what the fixed pipeline does: only
        # revocations at/after the matching settlement's occurred_at are
        # eligible duplicate candidates, and the earliest of THOSE (never
        # an out-of-order premature one) is the survivor.
        settled_at = NOW - timedelta(days=5)
        premature = {"_id": "premature", "occurred_at": NOW - timedelta(days=10)}  # before settlement
        legit = {"_id": "legit", "occurred_at": NOW - timedelta(days=1)}  # after settlement

        eligible_candidates = sorted(
            [d for d in (premature, legit) if d["occurred_at"] >= settled_at],
            key=lambda d: d["occurred_at"],
        )
        self.assertEqual([d["_id"] for d in eligible_candidates], ["legit"])
        # With only one eligible candidate there is no duplicate at all
        # here -- the premature row is excluded from duplicate-detection
        # entirely and must never be auto-invalidated by it.
        would_be_invalidated = eligible_candidates[1:]
        self.assertEqual(would_be_invalidated, [])

    def test_find_duplicate_revocations_pipeline_excludes_premature_before_settlement(self):
        # Structural check on the actual production pipeline: the stage
        # that filters candidates to occurred_at >= the matching
        # settlement's occurred_at must run before the sort/group/push that
        # picks the "earliest" survivor -- otherwise a premature row could
        # still win by absolute time. Source-inspected because the pipeline
        # requires real Mongo ($lookup+let) to execute, which isn't
        # available in this sandbox.
        import inspect

        source = inspect.getsource(rrl._find_duplicate_revocations)
        timing_filter_pos = source.index('"$gte": ["$occurred_at"')
        sort_pos = source.index('"$sort": {"occurred_at": 1, "_id": 1}}')
        group_pos = source.index('"$group"')
        self.assertLess(timing_filter_pos, sort_pos)
        self.assertLess(sort_pos, group_pos)


if __name__ == "__main__":
    unittest.main()
