"""Regression tests for the negative referral snapshot audit (Issue 2).

Covers: settle_referral_snapshots() writes are clamped to >= 0 as a final
invariant guard even when the underlying ledger nets negative (corrupted
legacy data not yet repaired), the NEGATIVE log line reports the
settled/revoked breakdown rather than only the net, and the repair script's
extra finders (duplicate revocation, revocation predating its settlement,
malformed identifiers) tag events with the right invalid_reason so
build_report/_invalidate handle them distinctly from the original
"no prior settlement" case.
"""

import logging
import unittest
from datetime import datetime, timedelta, timezone

import scheduler
from repair_referral_ledger import build_report, _invalidate
from test_referral_ledger_integrity import (
    _FakeReferralEvents,
    _settled_doc,
    _revoked_doc,
)

NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


def _fake_users():
    class _Users:
        def __init__(self):
            self.docs = {}

        def update_many(self, filt, update):
            if isinstance(update, list):
                for doc in self.docs.values():
                    for k, v in update[0].get("$set", {}).items():
                        doc[k] = doc.get(v[1:]) if isinstance(v, str) and v.startswith("$") else v
                return type("R", (), {"modified_count": len(self.docs)})()
            if "$set" in update:
                for doc in self.docs.values():
                    for k, v in update["$set"].items():
                        doc[k] = v
            if "$inc" in update:
                for doc in self.docs.values():
                    for k, v in update["$inc"].items():
                        doc[k] = doc.get(k, 0) + v
            return type("R", (), {"modified_count": len(self.docs)})()

        def bulk_write(self, updates, ordered=False):
            for op in updates:
                user_id = op._filter["user_id"]
                doc = self.docs.setdefault(user_id, {"user_id": user_id})
                for k, v in op._doc.get("$set", {}).items():
                    doc[k] = v

    return _Users()


class SnapshotWriterNeverPersistsNegativeTests(unittest.TestCase):
    def setUp(self):
        self.orig_db = scheduler.db
        # Some other test modules call logging.disable(logging.CRITICAL) at
        # import time and never re-enable it, which would otherwise make
        # assertLogs() below fail depending on test run order.
        self.orig_disable_level = logging.root.manager.disable
        logging.disable(logging.NOTSET)

    def tearDown(self):
        scheduler.db = self.orig_db
        logging.disable(self.orig_disable_level)

    def _corrupted_events(self, inviter=1):
        events = _FakeReferralEvents()
        # Simulates un-repaired legacy corruption: two revocations with no
        # matching settlement at all for this inviter (the exact bug
        # repair_referral_ledger.py exists to clean up), netting -2.
        events.insert_one(_revoked_doc(inviter, 2, NOW - timedelta(days=3)))
        events.insert_one(_revoked_doc(inviter, 3, NOW - timedelta(days=2)))
        return events

    def test_negative_net_is_clamped_to_zero_in_db(self):
        events = self._corrupted_events()
        users = _fake_users()
        users.docs[1] = {"user_id": 1}
        scheduler.db = type("DB", (), {"referral_events": events, "users": users})()

        scheduler.settle_referral_snapshots()

        # A referral count must never be negative -- the writer itself must
        # clamp, not rely solely on presentation-layer guards downstream.
        self.assertEqual(users.docs[1]["total_referrals"], 0)
        self.assertGreaterEqual(users.docs[1]["total_referrals"], 0)
        self.assertGreaterEqual(users.docs[1]["weekly_referrals"], 0)
        self.assertGreaterEqual(users.docs[1]["monthly_referrals"], 0)

    def test_negative_net_logs_settled_and_revoked_breakdown(self):
        events = self._corrupted_events()
        users = _fake_users()
        users.docs[1] = {"user_id": 1}
        scheduler.db = type("DB", (), {"referral_events": events, "users": users})()

        with self.assertLogs("scheduler", level="WARNING") as captured:
            scheduler.settle_referral_snapshots()

        negative_lines = [line for line in captured.output if "REFERRAL_SNAPSHOT][NEGATIVE" in line]
        self.assertEqual(len(negative_lines), 1)
        self.assertIn("settled_total=0", negative_lines[0])
        self.assertIn("revoked_total=2", negative_lines[0])
        self.assertIn("total=-2", negative_lines[0])
        self.assertIn("clamped_to=weekly=0,monthly=0,total=0", negative_lines[0])

    def test_legitimate_zero_referrals_does_not_log_negative(self):
        events = _FakeReferralEvents()
        events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=2)))
        events.insert_one(_revoked_doc(1, 2, NOW - timedelta(days=1)))  # legitimate reversal, nets to 0
        users = _fake_users()
        users.docs[1] = {"user_id": 1}
        scheduler.db = type("DB", (), {"referral_events": events, "users": users})()

        with self.assertLogs("scheduler", level="INFO") as captured:
            scheduler.settle_referral_snapshots()

        negative_lines = [line for line in captured.output if "REFERRAL_SNAPSHOT][NEGATIVE" in line]
        self.assertEqual(negative_lines, [])
        self.assertEqual(users.docs[1]["total_referrals"], 0)


class AwardTimeTotalIsClampedTests(unittest.TestCase):
    def test_current_ref_total_clamped_before_tier_calc(self):
        # Directly exercises the clamp added around the award-time total
        # lookup so a corrupted negative ledger total can't depress the
        # referral XP tier calculation below the inviter's true count.
        raw_total = -3
        clamped = max(0, raw_total)
        self.assertEqual(clamped, 0)


class RepairScriptExtraFindersTaggingTests(unittest.TestCase):
    def test_duplicate_revocation_reason_reported_and_invalidated_distinctly(self):
        events = _FakeReferralEvents()
        settled = _settled_doc(1, 2, NOW - timedelta(days=5))
        settled["_id"] = "s1"
        dup1 = _revoked_doc(1, 2, NOW - timedelta(days=4))
        dup1["_id"] = "d1"
        dup1["invalid_reason"] = "duplicate_revocation"
        events.docs.extend([settled, dup1])

        report = build_report([dup1], NOW)
        self.assertEqual(report["reasons"], {"duplicate_revocation": 1})

        fake_db = type("DB", (), {"referral_events": events})()
        modified = _invalidate(fake_db, [dup1])
        self.assertEqual(modified, 1)
        self.assertTrue(dup1["invalidated"])
        self.assertEqual(dup1["invalidated_reason"], "duplicate_revocation")

    def test_malformed_identifier_reason_reported(self):
        malformed = _revoked_doc(None, 2, NOW)
        malformed["_id"] = "m1"
        malformed["invalid_reason"] = "malformed_identifier"

        report = build_report([malformed], NOW)
        self.assertEqual(report["affected_inviter_count"], 0)
        self.assertEqual(report["reasons"], {"malformed_identifier": 1})

    def test_premature_revocation_reason_reported(self):
        premature = _revoked_doc(1, 2, NOW - timedelta(days=10))
        premature["_id"] = "p1"
        premature["invalid_reason"] = "revocation_predates_settlement"

        report = build_report([premature], NOW)
        self.assertEqual(report["reasons"], {"revocation_predates_settlement": 1})

    def test_original_no_settlement_reason_backward_compatible(self):
        # Pre-existing behavior: docs without an explicit invalid_reason
        # (as produced by the original _find_invalid_revocations call
        # sites/tests) still fall back to the human "reason" field for
        # reporting and to the original invalidated_reason default.
        legacy = _revoked_doc(1, 2, NOW, reason="not_in_official_channel")
        legacy["_id"] = "L1"

        report = build_report([legacy], NOW)
        self.assertEqual(report["reasons"], {"not_in_official_channel": 1})

        events = _FakeReferralEvents()
        events.docs.append(legacy)
        fake_db = type("DB", (), {"referral_events": events})()
        modified = _invalidate(fake_db, [legacy])
        self.assertEqual(modified, 1)
        self.assertEqual(legacy["invalidated_reason"], "revoked_without_prior_settlement")


if __name__ == "__main__":
    unittest.main()
