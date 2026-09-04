"""Regression tests for sync_referral_counts.py's write-safety and
invariant-violation gating (Issue 2 follow-up).

Covers: users.total_referrals is never written negative even when the
ledger computes a negative total (dry-run diagnostics still report the raw
negative value), default remains dry-run, and --commit refuses to run while
unresolved orphan/duplicate revocations exist unless the narrowly-named
override flag is passed.
"""

import sys
import unittest
from datetime import datetime, timezone

import sync_referral_counts
from test_referral_ledger_integrity import _FakeReferralEvents, _revoked_doc, _settled_doc

NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


class _FakeUsers:
    def __init__(self, docs):
        self._docs = list(docs)
        self._pending = []
        self.bulk_writes = []

    def find(self, query, projection):
        last_id = (query or {}).get("_id", {}).get("$gt") if query else None
        self._pending = [d for d in self._docs if last_id is None or d["_id"] > last_id]
        return self

    def sort(self, *a, **kw):
        return self

    def limit(self, *a, **kw):
        result, self._pending = self._pending, []
        return result

    def bulk_write(self, ops, ordered=False):
        self.bulk_writes.append(ops)
        for op in ops:
            for doc in self._docs:
                if doc["_id"] == op._filter["_id"]:
                    doc.update(op._doc.get("$set", {}))
        return type("R", (), {"modified_count": len(ops)})()


class NeverWritesNegativeTests(unittest.TestCase):
    def test_negative_ledger_total_is_clamped_to_zero_on_commit(self):
        events = _FakeReferralEvents()
        # Two orphan revocations, no settlement at all -> ledger nets -2.
        events.insert_one(_revoked_doc(1, 2, NOW))
        events.insert_one(_revoked_doc(1, 3, NOW))
        users = _FakeUsers([{"_id": "u1", "user_id": 1, "total_referrals": 0}])
        db = type("DB", (), {"users": users, "referral_events": events})()

        summary = sync_referral_counts.sync_referral_counts(db, batch_size=10, dry_run=False)

        # The diagnostic report shows the raw negative computed total...
        self.assertEqual(summary["top_20_deltas"][0]["computed"], -2)
        self.assertEqual(summary["negative_computed_count"], 1)
        # ...but the actual write to users.total_referrals is clamped to 0.
        self.assertEqual(users._docs[0]["total_referrals"], 0)
        self.assertGreaterEqual(users._docs[0]["total_referrals"], 0)

    def test_dry_run_default_never_writes(self):
        events = _FakeReferralEvents()
        events.insert_one(_revoked_doc(1, 2, NOW))
        users = _FakeUsers([{"_id": "u1", "user_id": 1, "total_referrals": 5}])
        db = type("DB", (), {"users": users, "referral_events": events})()

        summary = sync_referral_counts.sync_referral_counts(db, batch_size=10, dry_run=True)

        self.assertTrue(summary["dry_run"])
        self.assertEqual(users._docs[0]["total_referrals"], 5)
        self.assertEqual(users.bulk_writes, [])

    def test_positive_mismatch_still_fixed_normally(self):
        events = _FakeReferralEvents()
        events.insert_one(_settled_doc(1, 2, NOW))
        events.insert_one(_settled_doc(1, 3, NOW))
        users = _FakeUsers([{"_id": "u1", "user_id": 1, "total_referrals": 0}])
        db = type("DB", (), {"users": users, "referral_events": events})()

        summary = sync_referral_counts.sync_referral_counts(db, batch_size=10, dry_run=False)

        self.assertEqual(summary["negative_computed_count"], 0)
        self.assertEqual(users._docs[0]["total_referrals"], 2)

    def test_already_synced_negative_value_is_still_repaired(self):
        # An earlier (pre-clamp) run of this script already wrote
        # total_referrals = -2 to match the ledger's raw negative net. The
        # outer mismatch check (computed_total != stored_total) is false
        # here since both are -2, so the fix must not rely on that branch
        # alone to queue the clamped repair.
        events = _FakeReferralEvents()
        events.insert_one(_revoked_doc(1, 2, NOW))
        events.insert_one(_revoked_doc(1, 3, NOW))
        users = _FakeUsers([{"_id": "u1", "user_id": 1, "total_referrals": -2}])
        db = type("DB", (), {"users": users, "referral_events": events})()

        summary = sync_referral_counts.sync_referral_counts(db, batch_size=10, dry_run=False)

        self.assertEqual(users._docs[0]["total_referrals"], 0)
        self.assertEqual(len(users.bulk_writes), 1)


class CommitRefusalGateTests(unittest.TestCase):
    def setUp(self):
        self.orig_argv = sys.argv
        self.orig_mongo_client = sync_referral_counts.MongoClient
        self.orig_unresolved = sync_referral_counts.unresolved_invariant_violations
        self.orig_sync = sync_referral_counts.sync_referral_counts
        sync_referral_counts.MongoClient = lambda url: {"referral_bot": object()}

    def tearDown(self):
        sys.argv = self.orig_argv
        sync_referral_counts.MongoClient = self.orig_mongo_client
        sync_referral_counts.unresolved_invariant_violations = self.orig_unresolved
        sync_referral_counts.sync_referral_counts = self.orig_sync

    def test_commit_refused_when_unresolved_violations_exist(self):
        sync_referral_counts.unresolved_invariant_violations = lambda db: {
            "orphan_revocation_count": 3,
            "duplicate_revocation_count": 1,
            "total": 4,
        }
        called = []
        sync_referral_counts.sync_referral_counts = lambda *a, **kw: called.append(1)
        sys.argv = ["sync_referral_counts.py", "--mongo-url", "mongodb://example/ignored", "--commit"]

        with self.assertRaises(SystemExit):
            sync_referral_counts.main()
        self.assertEqual(called, [])

    def test_commit_proceeds_when_no_unresolved_violations(self):
        sync_referral_counts.unresolved_invariant_violations = lambda db: {
            "orphan_revocation_count": 0,
            "duplicate_revocation_count": 0,
            "total": 0,
        }
        called = []
        sync_referral_counts.sync_referral_counts = lambda *a, **kw: (called.append(1) or {"dry_run": False})
        sys.argv = ["sync_referral_counts.py", "--mongo-url", "mongodb://example/ignored", "--commit"]

        sync_referral_counts.main()
        self.assertEqual(called, [1])

    def test_override_flag_allows_commit_despite_unresolved_violations(self):
        sync_referral_counts.unresolved_invariant_violations = lambda db: {
            "orphan_revocation_count": 2,
            "duplicate_revocation_count": 0,
            "total": 2,
        }
        called = []
        sync_referral_counts.sync_referral_counts = lambda *a, **kw: (called.append(1) or {"dry_run": False})
        sys.argv = [
            "sync_referral_counts.py",
            "--mongo-url",
            "mongodb://example/ignored",
            "--commit",
            "--commit-with-unresolved-violations",
        ]

        sync_referral_counts.main()
        self.assertEqual(called, [1])

    def test_dry_run_never_checks_or_is_blocked_by_violations(self):
        checked = []
        sync_referral_counts.unresolved_invariant_violations = lambda db: checked.append(1)
        called = []
        sync_referral_counts.sync_referral_counts = lambda *a, **kw: (called.append(1) or {"dry_run": True})
        sys.argv = ["sync_referral_counts.py", "--mongo-url", "mongodb://example/ignored"]

        sync_referral_counts.main()
        self.assertEqual(checked, [])
        self.assertEqual(called, [1])


if __name__ == "__main__":
    unittest.main()
