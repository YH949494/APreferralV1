import unittest
from datetime import datetime, timedelta, timezone

import backend_segment_engine as engine


def _metrics(**overrides):
    base = {
        "after_bet_amount": None,
        "withdrawal_amount": None,
        "is_new_player": None,
        "claim_count": 0,
        "referral_count": 0,
        "checkin_count": 0,
        "xp": 0,
        "last_active_at": None,
    }
    base.update(overrides)
    return base


class SegmentRuleTests(unittest.TestCase):
    def test_high_value_rule(self):
        m = _metrics(after_bet_amount=800, withdrawal_amount=100)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "high_value")
        self.assertEqual(result["segment_reason"], "after_bet_multiple >= 8x")
        self.assertEqual(result["confidence"], "high")

    def test_high_value_boundary_exactly_8x(self):
        m = _metrics(after_bet_amount=800, withdrawal_amount=100)
        self.assertEqual(engine.classify_segment(m)["segment"], "high_value")

    def test_low_value_rule(self):
        m = _metrics(after_bet_amount=300, withdrawal_amount=100)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "low_value")
        self.assertEqual(result["segment_reason"], "after_bet_multiple < 8x")

    def test_normal_actual_rule(self):
        m = _metrics(after_bet_amount=500, withdrawal_amount=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "normal_actual")
        self.assertEqual(result["segment_reason"], "has play activity")

    def test_voucher_hunter_rule(self):
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, claim_count=3)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "voucher_hunter")
        self.assertEqual(result["segment_reason"], "repeat claims with no play")

    def test_voucher_hunter_requires_threshold(self):
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, claim_count=2)
        result = engine.classify_segment(m)
        self.assertNotEqual(result["segment"], "voucher_hunter")

    def test_ghost_rule(self):
        old = datetime.now(timezone.utc) - timedelta(days=45)
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, claim_count=0, referral_count=0, checkin_count=0, last_active_at=old)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "ghost")
        self.assertEqual(result["segment_reason"], "inactive user")

    def test_ghost_rule_not_triggered_within_30_days(self):
        recent = datetime.now(timezone.utc) - timedelta(days=10)
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, claim_count=0, referral_count=0, checkin_count=0, last_active_at=recent)
        result = engine.classify_segment(m)
        self.assertNotEqual(result["segment"], "ghost")

    def test_malformed_last_active_at_does_not_crash(self):
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, claim_count=0, referral_count=0, checkin_count=0, last_active_at="not-a-date")
        result = engine.classify_segment(m)
        self.assertNotEqual(result["segment"], "ghost")
        self.assertEqual(result["segment"], "unclassified")

    def test_new_player_fallback(self):
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, is_new_player=1)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "new_player")

    def test_old_player_fallback(self):
        m = _metrics(after_bet_amount=0, withdrawal_amount=0, is_new_player=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "old_player")

    def test_active_community_player_is_low_confidence(self):
        m = _metrics(xp=engine.ACTIVE_COMMUNITY_XP_THRESHOLD + 1)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "active_community_player")
        self.assertEqual(result["confidence"], "low")

    def test_missing_marketing_data_is_unclassified_low_confidence(self):
        m = _metrics()
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "unclassified")
        self.assertEqual(result["confidence"], "low")
        self.assertIn("missing marketing data", result["segment_reason"])


class ClaimRiskRuleTests(unittest.TestCase):
    def test_normal_below_10(self):
        level, reason = engine.classify_claim_risk(9)
        self.assertEqual(level, "normal")
        self.assertEqual(reason, "claim_count=9")

    def test_medium_risk_boundary(self):
        level, _ = engine.classify_claim_risk(10)
        self.assertEqual(level, "medium_risk")
        level, _ = engine.classify_claim_risk(13)
        self.assertEqual(level, "medium_risk")

    def test_high_risk_boundary(self):
        level, _ = engine.classify_claim_risk(20)
        self.assertEqual(level, "high_risk_review")
        level, _ = engine.classify_claim_risk(49)
        self.assertEqual(level, "high_risk_review")

    def test_abuse_freeze_boundary(self):
        level, reason = engine.classify_claim_risk(50)
        self.assertEqual(level, "abuse_freeze")
        self.assertEqual(reason, "claim_count=50")
        level, reason = engine.classify_claim_risk(51)
        self.assertEqual(level, "abuse_freeze")
        self.assertEqual(reason, "claim_count=51")


class SnapshotIdempotencyTests(unittest.TestCase):
    class _FakeBulkResult:
        def __init__(self, modified_count=0, upserted_count=0):
            self.modified_count = modified_count
            self.upserted_count = upserted_count

    class _FakeSnapshotsCollection:
        def __init__(self):
            self.docs = {}

        def bulk_write(self, ops, ordered=False):
            upserted = 0
            modified = 0
            for op in ops:
                filt = getattr(op, "_filter", {})
                update = getattr(op, "_doc", {})
                key = (filt["user_id"], filt["snapshot_month"])
                is_new = key not in self.docs
                self.docs[key] = update.get("$set", {})
                if is_new:
                    upserted += 1
                else:
                    modified += 1
            return SnapshotIdempotencyTests._FakeBulkResult(modified_count=modified, upserted_count=upserted)

    class _FakeUsersCollection:
        def __init__(self, docs):
            self._docs = docs

        def find(self, filt=None):
            return list(self._docs)

    class _FakeEmptyLookupCollection:
        """Stands in for voucher_claims_col / marketing_col with no data."""

        def find(self, filt=None):
            return []

        def aggregate(self, pipeline):
            return []

    def test_rerunning_same_month_replaces_not_duplicates(self):
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        users = self._FakeUsersCollection([{"user_id": 100, "total_referrals": 0, "for_bot_segment": "high_value"}])
        snapshots = self._FakeSnapshotsCollection()
        empty = self._FakeEmptyLookupCollection()

        summary1 = engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=empty, snapshots_col=snapshots, now=now
        )
        self.assertTrue(summary1["ok"])
        self.assertEqual(len(snapshots.docs), 1)

        summary2 = engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=empty, snapshots_col=snapshots, now=now
        )
        self.assertTrue(summary2["ok"])
        # Still exactly one doc for (user_id=100, snapshot_month) — idempotent.
        self.assertEqual(len(snapshots.docs), 1)

    def test_different_month_creates_separate_snapshot(self):
        users = self._FakeUsersCollection([{"user_id": 100, "total_referrals": 0}])
        snapshots = self._FakeSnapshotsCollection()
        empty = self._FakeEmptyLookupCollection()
        engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=empty, snapshots_col=snapshots,
            now=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=empty, snapshots_col=snapshots,
            now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(len(snapshots.docs), 2)


class UimComparisonTests(unittest.TestCase):
    def test_match_when_canonical_segments_equal(self):
        result = engine.compare_with_uim(backend_segment="high_value", uim_segment_raw="High Value")
        self.assertTrue(result["match"])
        self.assertEqual(result["uim_segment"], "high_value")
        self.assertEqual(result["backend_segment"], "high_value")

    def test_mismatch_when_segments_differ(self):
        result = engine.compare_with_uim(backend_segment="low_value", uim_segment_raw="high_value")
        self.assertFalse(result["match"])

    def test_new_player_alias_matches_uim_new_user(self):
        result = engine.compare_with_uim(backend_segment="new_player", uim_segment_raw="new_user")
        self.assertTrue(result["match"])

    def test_blank_uim_value_normalizes_to_unclassified(self):
        result = engine.compare_with_uim(backend_segment="unclassified", uim_segment_raw="")
        self.assertTrue(result["match"])


class MissingMarketingDataTests(unittest.TestCase):
    def test_snapshot_doc_reports_unclassified_and_low_confidence(self):
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        doc = engine.build_snapshot_doc(user_id=1, metrics=_metrics(), now=now)
        self.assertEqual(doc["backend_segment"], "unclassified")
        self.assertEqual(doc["confidence"], "low")
        self.assertEqual(doc["snapshot_month"], "2026-06")
        self.assertEqual(doc["claim_risk_level"], "normal")


if __name__ == "__main__":
    unittest.main()
