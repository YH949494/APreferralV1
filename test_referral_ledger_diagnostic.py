"""Regression tests for referral_ledger_diagnostic.py's read-only per-pair
report (Issue 2, task 3): grouped by inviter/invitee, showing settlement and
revocation counts, source collection, idempotency key, and reason.
"""

import unittest
from datetime import datetime, timedelta, timezone

import referral_ledger_diagnostic as diag
from test_referral_ledger_integrity import _FakeReferralEvents, _revoked_doc, _settled_doc

NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


class _FakeFlowEvents:
    def __init__(self, docs):
        self.docs = docs

    def find(self, filt):
        inviter_set = filt.get("referrer_id", {}).get("$in", [])
        events_set = filt.get("event", {}).get("$in", [])
        return [
            d
            for d in self.docs
            if d.get("referrer_id") in inviter_set and d.get("event") in events_set
        ]


class BuildPairDiagnosticTests(unittest.TestCase):
    def test_orphan_revocation_is_flagged_as_violation(self):
        events = _FakeReferralEvents()
        events.insert_one(_revoked_doc(1, 2, NOW))
        flow = _FakeFlowEvents(
            [{"_id": "rf|referral_revoked|1|2|" + NOW.isoformat(), "event": "referral_revoked",
              "referrer_id": 1, "invitee_id": 2, "ts_utc": NOW}]
        )
        db = type("DB", (), {"referral_events": events, "referral_flow_events": flow})()

        rows = diag.build_pair_diagnostic(db, [1])

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["inviter_id"], 1)
        self.assertEqual(row["invitee_id"], 2)
        self.assertEqual(row["settled_count"], 0)
        self.assertEqual(row["revoked_count_valid"], 1)
        self.assertEqual(row["net"], -1)
        self.assertTrue(row["violation"])
        self.assertEqual(row["ledger_rows"][0]["source_collection"], "referral_events")
        self.assertEqual(row["flow_event_rows"][0]["source_collection"], "referral_flow_events")
        self.assertEqual(row["flow_event_rows"][0]["idempotency_key"], "rf|referral_revoked|1|2|" + NOW.isoformat())

    def test_settle_then_revoke_nets_zero_and_is_not_a_violation(self):
        events = _FakeReferralEvents()
        events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=1)))
        events.insert_one(_revoked_doc(1, 2, NOW))
        flow = _FakeFlowEvents([])
        db = type("DB", (), {"referral_events": events, "referral_flow_events": flow})()

        rows = diag.build_pair_diagnostic(db, [1])

        row = rows[0]
        self.assertEqual(row["settled_count"], 1)
        self.assertEqual(row["revoked_count_valid"], 1)
        self.assertEqual(row["net"], 0)
        self.assertFalse(row["violation"])

    def test_invalidated_revocation_excluded_from_valid_count(self):
        events = _FakeReferralEvents()
        events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=2)))
        bad_revoke = _revoked_doc(1, 3, NOW, invalidated=True)
        events.docs.append(bad_revoke)
        flow = _FakeFlowEvents([])
        db = type("DB", (), {"referral_events": events, "referral_flow_events": flow})()

        rows = diag.build_pair_diagnostic(db, [1])
        by_invitee = {r["invitee_id"]: r for r in rows}

        self.assertEqual(by_invitee[3]["revoked_count_valid"], 0)
        self.assertEqual(by_invitee[3]["revoked_count_invalidated"], 1)
        self.assertFalse(by_invitee[3]["violation"])

    def test_no_inviter_ids_returns_empty(self):
        db = type("DB", (), {"referral_events": _FakeReferralEvents(), "referral_flow_events": _FakeFlowEvents([])})()
        self.assertEqual(diag.build_pair_diagnostic(db, []), [])


if __name__ == "__main__":
    unittest.main()
