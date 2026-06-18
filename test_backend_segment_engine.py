import unittest
from datetime import datetime, timedelta, timezone

import backend_segment_engine as engine


def _metrics(**overrides):
    base = {
        "after_total_bet_amount": None,
        "withdraw_amount": None,
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
        m = _metrics(after_total_bet_amount=800, withdraw_amount=100)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "high_value")
        self.assertEqual(result["segment_reason"], "after_bet_multiple >= 8x")
        self.assertEqual(result["confidence"], "high")

    def test_high_value_boundary_exactly_8x(self):
        m = _metrics(after_total_bet_amount=800, withdraw_amount=100)
        self.assertEqual(engine.classify_segment(m)["segment"], "high_value")

    def test_low_value_rule(self):
        m = _metrics(after_total_bet_amount=300, withdraw_amount=100)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "low_value")
        self.assertEqual(result["segment_reason"], "after_bet_multiple < 8x")

    def test_normal_actual_rule(self):
        m = _metrics(after_total_bet_amount=500, withdraw_amount=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "normal_actual")
        self.assertEqual(result["segment_reason"], "has play activity")

    def test_normal_actual_no_withdrawal(self):
        # after_total_bet > 0 with no withdrawal is still normal_actual
        m = _metrics(after_total_bet_amount=100, withdraw_amount=0)
        self.assertEqual(engine.classify_segment(m)["segment"], "normal_actual")

    def test_voucher_hunter_rule(self):
        # VH v2: claim_count >= 10, after_bet < 100, referral_count < 20
        m = _metrics(after_total_bet_amount=0, withdraw_amount=0, claim_count=10)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "voucher_hunter")

    def test_voucher_hunter_requires_threshold(self):
        # claim_count=9 is one below the v2 threshold of 10
        m = _metrics(after_total_bet_amount=0, withdraw_amount=0, claim_count=9)
        result = engine.classify_segment(m)
        self.assertNotEqual(result["segment"], "voucher_hunter")

    def test_voucher_hunter_v2_small_after_bet(self):
        # Case B: small after_bet still qualifies if claim_count >= 10
        m = _metrics(after_total_bet_amount=50, withdraw_amount=0, claim_count=15, referral_count=3)
        self.assertEqual(engine.classify_segment(m)["segment"], "voucher_hunter")

    def test_voucher_hunter_v2_after_bet_at_threshold(self):
        # Case C: after_bet >= 100 → not VH → normal_actual
        m = _metrics(after_total_bet_amount=100, withdraw_amount=0, claim_count=15, referral_count=3)
        self.assertEqual(engine.classify_segment(m)["segment"], "normal_actual")

    def test_voucher_hunter_v2_referral_protection(self):
        # Case D: referral_count >= 20 → not VH → normal_actual (has play) or unclassified
        m = _metrics(after_total_bet_amount=50, withdraw_amount=0, claim_count=15, referral_count=20)
        self.assertNotEqual(engine.classify_segment(m)["segment"], "voucher_hunter")

    def test_voucher_hunter_v2_high_bet_not_vh(self):
        # Case E: after_bet >= 1000 → after_bet < 100 fails → normal_actual
        m = _metrics(after_total_bet_amount=5000, withdraw_amount=0, claim_count=15, referral_count=0)
        self.assertEqual(engine.classify_segment(m)["segment"], "normal_actual")

    def test_voucher_hunter_v2_withdrawal_high_value_not_vh(self):
        # Phase 7D: withdraw > 0 with ratio >= 8x → high_value takes priority over VH.
        m = _metrics(after_total_bet_amount=800, withdraw_amount=100, claim_count=15, referral_count=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "high_value")

    # --- Phase 7D tests ---

    def test_vh_rule_a_extreme_low_bet_per_claim(self):
        # Rule A: claim_count >= 20 AND after_bet_per_claim < 2 → VH
        # after_bet=10, claim_count=20 → bet_per_claim=0.5 < 2
        m = _metrics(after_total_bet_amount=10, withdraw_amount=0, claim_count=20, referral_count=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "voucher_hunter")
        self.assertIn("extreme_low_bet_per_claim", result["segment_reason"])

    def test_vh_rule_b_claim_and_withdraw_no_after_bet(self):
        # Rule B: claim_count >= 10, after_bet == 0, withdraw > 0 → VH
        m = _metrics(after_total_bet_amount=0, withdraw_amount=50, claim_count=12, referral_count=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "voucher_hunter")
        self.assertIn("claim_and_withdraw_no_after_bet", result["segment_reason"])

    def test_vh_rule_c_high_claim_low_play(self):
        # Rule C: claim_count >= 10, after_bet < 100, bet_per_claim < 10 → VH
        # after_bet=50, claim_count=10 → bet_per_claim=5.0 < 10
        m = _metrics(after_total_bet_amount=50, withdraw_amount=0, claim_count=10, referral_count=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "voucher_hunter")
        self.assertIn("high_claim_low_play", result["segment_reason"])

    def test_vh_rule_b_high_value_takes_priority(self):
        # Rule B would match (after_bet==0, withdraw>0, claim_count>=10) but
        # high_value (ratio >= 8x) is checked first — ratio undefined when after_bet=0
        # so ratio=0, which is < 8x → VH wins, NOT high_value.
        # Use after_bet > 0 with ratio >= 8x to confirm high_value wins.
        m = _metrics(after_total_bet_amount=800, withdraw_amount=100, claim_count=15, referral_count=0)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "high_value")

    def test_former_low_value_now_vh_rule_b(self):
        # Pre-7D: withdraw > 0 and after_bet < 100 → low_value.
        # Post-7D: claim_count >= 10 AND after_bet == 0 AND withdraw > 0 → VH (Rule B).
        m = _metrics(after_total_bet_amount=0, withdraw_amount=20, claim_count=10, referral_count=5)
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "voucher_hunter")

    def test_ghost_rule(self):
        m = _metrics(
            after_total_bet_amount=0, withdraw_amount=0,
            claim_count=0, referral_count=0, checkin_count=0,
        )
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "ghost")
        self.assertEqual(result["segment_reason"], "inactive user")

    def test_ghost_rule_no_last_active_check(self):
        # Phase 3: ghost does NOT require last_active_at check — a recently
        # active user with zero bet/referral/checkin is still ghost.
        recent = datetime.now(timezone.utc) - timedelta(days=1)
        m = _metrics(
            after_total_bet_amount=0, withdraw_amount=0,
            claim_count=0, referral_count=0, checkin_count=0,
            last_active_at=recent,
        )
        self.assertEqual(engine.classify_segment(m)["segment"], "ghost")

    def test_ghost_not_triggered_if_has_referrals(self):
        m = _metrics(
            after_total_bet_amount=0, withdraw_amount=0,
            claim_count=0, referral_count=1, checkin_count=0,
        )
        self.assertNotEqual(engine.classify_segment(m)["segment"], "ghost")

    def test_ghost_not_triggered_if_has_checkins(self):
        m = _metrics(
            after_total_bet_amount=0, withdraw_amount=0,
            claim_count=0, referral_count=0, checkin_count=1,
        )
        self.assertNotEqual(engine.classify_segment(m)["segment"], "ghost")

    def test_malformed_last_active_at_does_not_crash(self):
        # Phase 3: ghost is triggered by referral/checkin conditions only;
        # malformed last_active_at is ignored — no crash, ghost still returned.
        m = _metrics(
            after_total_bet_amount=0, withdraw_amount=0,
            claim_count=0, referral_count=0, checkin_count=0,
            last_active_at="not-a-date",
        )
        result = engine.classify_segment(m)
        self.assertEqual(result["segment"], "ghost")

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


class FieldNameAliasTests(unittest.TestCase):
    """Both Phase 3 and legacy field-name conventions must produce the same results."""

    def test_phase3_field_names_high_value(self):
        m = _metrics(after_total_bet_amount=800, withdraw_amount=100)
        self.assertEqual(engine.classify_segment(m)["segment"], "high_value")

    def test_legacy_field_names_high_value(self):
        # after_bet_amount / withdrawal_amount (Phase 6A names) still work
        m = _metrics(after_bet_amount=800, withdrawal_amount=100)
        self.assertEqual(engine.classify_segment(m)["segment"], "high_value")

    def test_phase3_field_names_low_value(self):
        m = _metrics(after_total_bet_amount=300, withdraw_amount=100)
        self.assertEqual(engine.classify_segment(m)["segment"], "low_value")

    def test_legacy_field_names_low_value(self):
        m = _metrics(after_bet_amount=300, withdrawal_amount=100)
        self.assertEqual(engine.classify_segment(m)["segment"], "low_value")


class PlayerAgeTypeTests(unittest.TestCase):
    def test_new_player_flag_1(self):
        self.assertEqual(engine.classify_player_age_type(1), "new_player")

    def test_new_player_flag_string_1(self):
        self.assertEqual(engine.classify_player_age_type("1"), "new_player")

    def test_new_player_flag_0(self):
        self.assertEqual(engine.classify_player_age_type(0), "old_player")

    def test_new_player_flag_none(self):
        self.assertEqual(engine.classify_player_age_type(None), "old_player")

    def test_new_player_flag_string_true(self):
        self.assertEqual(engine.classify_player_age_type("true"), "new_player")

    def test_player_age_type_stored_separately_from_segment(self):
        # A ghost user still has player_age_type stored independently
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        doc = engine.build_snapshot_doc(
            account="testuser",
            user_id=100,
            telegram_user_id=100,
            metrics=_metrics(
                after_total_bet_amount=0, withdraw_amount=0,
                is_new_player=1,
                referral_count=0, checkin_count=0,
            ),
            now=now,
            snapshot_week="2026-W25",
        )
        self.assertEqual(doc["backend_segment"], "ghost")
        self.assertEqual(doc["player_age_type"], "new_player")

    def test_old_player_with_high_value_segment(self):
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        doc = engine.build_snapshot_doc(
            account="alice",
            user_id=42,
            telegram_user_id=42,
            metrics=_metrics(after_total_bet_amount=800, withdraw_amount=100, is_new_player=0),
            now=now,
            snapshot_week="2026-W25",
        )
        self.assertEqual(doc["backend_segment"], "high_value")
        self.assertEqual(doc["player_age_type"], "old_player")


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

    def test_claim_risk_reason_example_14(self):
        _, reason = engine.classify_claim_risk(14)
        self.assertEqual(reason, "claim_count=14")

    def test_claim_risk_reason_example_53(self):
        level, reason = engine.classify_claim_risk(53)
        self.assertEqual(level, "abuse_freeze")
        self.assertEqual(reason, "claim_count=53")


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
                # Phase 3: unique key is (account, snapshot_week)
                key = (filt.get("account"), filt.get("snapshot_week"))
                is_new = key not in self.docs
                self.docs[key] = update.get("$set", {})
                if is_new:
                    upserted += 1
                else:
                    modified += 1
            return SnapshotIdempotencyTests._FakeBulkResult(
                modified_count=modified, upserted_count=upserted
            )

    class _FakeUsersCollection:
        def __init__(self, docs):
            self._docs = list(docs)

        def find(self, filt=None):
            return list(self._docs)

    class _FakeMarketingCollection:
        """Simulates marketing_raw_data with docs per snapshot_week."""

        def __init__(self, week_docs):
            # week_docs: dict[snapshot_week -> list[doc]]
            self._week_docs = week_docs

        def find(self, filt=None):
            filt = filt or {}
            week = filt.get("snapshot_week")
            if isinstance(week, str):
                return list(self._week_docs.get(week, []))
            return []

    class _FakeEmptyCollection:
        def find(self, filt=None):
            return []

        def aggregate(self, pipeline):
            return []

    def test_rerunning_same_week_replaces_not_duplicates(self):
        week = "2026-W25"
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        marketing = self._FakeMarketingCollection({
            week: [{"account": "alice", "after_total_bet_amount": 800,
                    "withdraw_amount": 100, "snapshot_week": week}]
        })
        users = self._FakeUsersCollection([
            {"user_id": 100, "username": "alice", "total_referrals": 0, "for_bot_segment": "high_value"}
        ])
        snapshots = self._FakeSnapshotsCollection()
        empty = self._FakeEmptyCollection()

        summary1 = engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=marketing,
            snapshots_col=snapshots, snapshot_week=week, now=now,
        )
        self.assertTrue(summary1["ok"])
        self.assertEqual(len(snapshots.docs), 1)

        summary2 = engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=marketing,
            snapshots_col=snapshots, snapshot_week=week, now=now,
        )
        self.assertTrue(summary2["ok"])
        # Still exactly one doc for (account=alice, snapshot_week) — idempotent.
        self.assertEqual(len(snapshots.docs), 1)

    def test_different_week_creates_separate_snapshot(self):
        week1 = "2026-W24"
        week2 = "2026-W25"
        mrow = {"account": "alice", "after_total_bet_amount": 100, "withdraw_amount": 0}
        marketing = self._FakeMarketingCollection({
            week1: [{**mrow, "snapshot_week": week1}],
            week2: [{**mrow, "snapshot_week": week2}],
        })
        users = self._FakeUsersCollection([{"user_id": 100, "username": "alice", "total_referrals": 0}])
        snapshots = self._FakeSnapshotsCollection()
        empty = self._FakeEmptyCollection()

        engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=marketing,
            snapshots_col=snapshots, snapshot_week=week1,
            now=datetime(2026, 6, 9, tzinfo=timezone.utc),
        )
        engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=marketing,
            snapshots_col=snapshots, snapshot_week=week2,
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        self.assertEqual(len(snapshots.docs), 2)

    def test_missing_marketing_data_returns_ok_zero_users(self):
        marketing = self._FakeMarketingCollection({})
        users = self._FakeUsersCollection([{"user_id": 100, "username": "alice"}])
        snapshots = self._FakeSnapshotsCollection()
        empty = self._FakeEmptyCollection()

        summary = engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty, marketing_col=marketing,
            snapshots_col=snapshots, snapshot_week="2026-W99",
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["users_evaluated"], 0)
        self.assertEqual(len(snapshots.docs), 0)


class ActualPlayersKPITests(unittest.TestCase):
    """actual_players KPI = high_value + low_value + normal_actual."""

    class _FakeSnapshotsCollection:
        def __init__(self):
            self.docs = {}

        def bulk_write(self, ops, ordered=False):
            for op in ops:
                filt = getattr(op, "_filter", {})
                update = getattr(op, "_doc", {})
                key = (filt.get("account"), filt.get("snapshot_week"))
                self.docs[key] = update.get("$set", {})
            return type("R", (), {"modified_count": 0, "upserted_count": len(ops)})()

    class _FakeEmptyCollection:
        def find(self, filt=None):
            return []

        def aggregate(self, pipeline):
            return []

    def _marketing_col(self, docs, week):
        class _MC:
            def find(self_, filt=None):
                filt = filt or {}
                if filt.get("snapshot_week") == week:
                    return list(docs)
                return []
        return _MC()

    def test_actual_players_kpi_components(self):
        week = "2026-W25"
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        marketing_docs = [
            {"account": "hv_user", "after_total_bet_amount": 800, "withdraw_amount": 100, "snapshot_week": week},
            {"account": "lv_user", "after_total_bet_amount": 200, "withdraw_amount": 100, "snapshot_week": week},
            {"account": "na_user", "after_total_bet_amount": 50, "withdraw_amount": 0, "snapshot_week": week},
            {"account": "gh_user", "after_total_bet_amount": 0, "withdraw_amount": 0, "snapshot_week": week},
        ]
        users = type("UC", (), {"find": lambda self, f=None: []})()
        snapshots = self._FakeSnapshotsCollection()
        empty = self._FakeEmptyCollection()

        summary = engine.run_shadow_segment_engine(
            users_col=users, voucher_claims_col=empty,
            marketing_col=self._marketing_col(marketing_docs, week),
            snapshots_col=snapshots,
            snapshot_week=week, now=now,
        )
        self.assertTrue(summary["ok"])
        dist = summary["segment_distribution"]
        self.assertEqual(dist.get("high_value", 0), 1)
        self.assertEqual(dist.get("low_value", 0), 1)
        self.assertEqual(dist.get("normal_actual", 0), 1)
        self.assertEqual(dist.get("ghost", 0), 1)
        actual_players = (
            dist.get("high_value", 0) + dist.get("low_value", 0) + dist.get("normal_actual", 0)
        )
        self.assertEqual(actual_players, 3)


class UimComparisonTests(unittest.TestCase):
    def test_match_when_canonical_segments_equal(self):
        result = engine.compare_with_uim(backend_segment="high_value", uim_segment_raw="High Value")
        self.assertTrue(result["match"])
        self.assertEqual(result["uim_segment"], "high_value")
        self.assertEqual(result["backend_segment"], "high_value")

    def test_mismatch_when_segments_differ(self):
        result = engine.compare_with_uim(backend_segment="low_value", uim_segment_raw="high_value")
        self.assertFalse(result["match"])

    def test_blank_uim_value_normalizes_to_unclassified(self):
        result = engine.compare_with_uim(backend_segment="unclassified", uim_segment_raw="")
        self.assertTrue(result["match"])


class MissingMarketingDataTests(unittest.TestCase):
    def test_snapshot_doc_reports_unclassified_and_low_confidence(self):
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        doc = engine.build_snapshot_doc(
            account="testuser",
            user_id=1,
            telegram_user_id=1,
            metrics=_metrics(),
            now=now,
            snapshot_week="2026-W25",
        )
        self.assertEqual(doc["backend_segment"], "unclassified")
        self.assertEqual(doc["confidence"], "low")
        self.assertEqual(doc["snapshot_week"], "2026-W25")
        self.assertEqual(doc["snapshot_month"], "2026-06")
        self.assertEqual(doc["claim_risk_level"], "normal")
        self.assertIn("account", doc)
        self.assertIn("player_age_type", doc)

    def test_snapshot_doc_includes_all_schema_fields(self):
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        doc = engine.build_snapshot_doc(
            account="alice",
            user_id=42,
            telegram_user_id=42,
            metrics=_metrics(after_total_bet_amount=800, withdraw_amount=100, is_new_player=1),
            now=now,
            snapshot_week="2026-W25",
        )
        for field in (
            "account", "user_id", "telegram_user_id", "backend_segment",
            "player_age_type", "claim_risk_level", "segment_reason",
            "claim_risk_reason", "confidence", "snapshot_week",
            "snapshot_month", "calculated_at",
        ):
            self.assertIn(field, doc, f"Missing schema field: {field}")


class AccountCasingRegressionTests(unittest.TestCase):
    """P2 regression: account field must be found regardless of CSV header casing.

    marketing_upload._normalize_header stores headers verbatim, so valid
    uploads may produce documents keyed as 'account', 'Account', 'ACCOUNT',
    or any other mixed case.  The engine must not silently skip these rows.
    """

    def _doc(self, key: str, value: str = "alice") -> dict:
        return {key: value, "after_total_bet_amount": 800, "withdraw_amount": 100}

    def test_lowercase_account_key(self):
        self.assertEqual(engine._doc_account(self._doc("account")), "alice")

    def test_titlecase_account_key(self):
        self.assertEqual(engine._doc_account(self._doc("Account")), "alice")

    def test_uppercase_account_key(self):
        self.assertEqual(engine._doc_account(self._doc("ACCOUNT")), "alice")

    def test_mixed_case_account_key(self):
        self.assertEqual(engine._doc_account(self._doc("AcCoUnT")), "alice")

    def test_missing_account_returns_empty_string(self):
        self.assertEqual(engine._doc_account({"other_field": "x"}), "")

    def test_marketing_rows_by_account_title_case(self):
        """All casing variants must appear in the returned dict."""
        week = "2026-W25"
        docs = [
            {"Account": "alice", "after_total_bet_amount": 800, "withdraw_amount": 100, "snapshot_week": week},
            {"ACCOUNT": "bob",   "after_total_bet_amount": 200, "withdraw_amount": 50,  "snapshot_week": week},
            {"account": "carol", "after_total_bet_amount": 50,  "withdraw_amount": 0,   "snapshot_week": week},
        ]

        class _FakeMktCol:
            def find(self, filt=None):
                filt = filt or {}
                if filt.get("snapshot_week") == week:
                    return list(docs)
                return []

        rows = engine._marketing_rows_by_account(_FakeMktCol(), week)
        self.assertIn("alice", rows)
        self.assertIn("bob", rows)
        self.assertIn("carol", rows)
        self.assertEqual(len(rows), 3)

    def test_all_casing_variants_produce_correct_segment(self):
        """End-to-end: engine classifies 'Account' header rows correctly."""
        week = "2026-W25"
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        docs_per_variant = [
            ("account", "u1", 800, 100),
            ("Account", "u2", 800, 100),
            ("ACCOUNT", "u3", 800, 100),
            ("AcCoUnT", "u4", 800, 100),
        ]
        mkt_docs = [
            {key: username, "after_total_bet_amount": bet, "withdraw_amount": wd, "snapshot_week": week}
            for key, username, bet, wd in docs_per_variant
        ]

        class _FakeMktCol:
            def find(self, filt=None):
                filt = filt or {}
                return list(mkt_docs) if filt.get("snapshot_week") == week else []

        class _FakeSnapshotsCol:
            def __init__(self):
                self.written = []

            def bulk_write(self, ops, ordered=False):
                self.written.extend(ops)
                return type("R", (), {"modified_count": 0, "upserted_count": len(ops)})()

        users_col = type("UC", (), {"find": lambda self, f=None: []})()
        snaps = _FakeSnapshotsCol()
        empty = type("EC", (), {
            "find": lambda self, f=None: [],
            "aggregate": lambda self, p: [],
        })()

        summary = engine.run_shadow_segment_engine(
            users_col=users_col,
            voucher_claims_col=empty,
            marketing_col=_FakeMktCol(),
            snapshots_col=snaps,
            snapshot_week=week,
            now=now,
        )
        self.assertTrue(summary["ok"])
        # All four rows must be evaluated — none silently dropped.
        self.assertEqual(summary["users_evaluated"], 4)
        self.assertEqual(summary["segment_distribution"].get("high_value", 0), 4)


class DocCouponTests(unittest.TestCase):
    """_doc_coupon tolerates mixed-case coupon_code headers."""

    def test_lowercase_key(self):
        self.assertEqual(engine._doc_coupon({"coupon_code": "ABC"}), "ABC")

    def test_title_case_key(self):
        self.assertEqual(engine._doc_coupon({"Coupon_Code": "DEF"}), "DEF")

    def test_uppercase_key(self):
        self.assertEqual(engine._doc_coupon({"COUPON_CODE": "GHI"}), "GHI")

    def test_arbitrary_mixed_case(self):
        self.assertEqual(engine._doc_coupon({"cOuPoN_cOdE": "JKL"}), "JKL")

    def test_missing_coupon_returns_none(self):
        self.assertIsNone(engine._doc_coupon({"account": "alice"}))

    def test_empty_coupon_returns_none(self):
        self.assertIsNone(engine._doc_coupon({"coupon_code": ""}))

    def test_whitespace_only_returns_none(self):
        self.assertIsNone(engine._doc_coupon({"coupon_code": "   "}))

    def test_strips_whitespace(self):
        self.assertEqual(engine._doc_coupon({"coupon_code": "  X1 "}), "X1")


class CouponIdentityResolutionTests(unittest.TestCase):
    """Engine resolves user identity via coupon_code → voucher_claims → user_id."""

    class _FakeVoucherClaims:
        """Supports find(filter, proj) and aggregate() for _claim_counts."""

        def __init__(self, claims):
            self._claims = list(claims)

        def find(self, filt=None, proj=None):
            filt = filt or {}
            results = list(self._claims)
            in_codes = (filt.get("voucher_code") or {}).get("$in")
            if in_codes is not None:
                results = [c for c in results if c.get("voucher_code") in in_codes]
            results = [c for c in results if c.get("user_id") is not None]
            return results

        def aggregate(self, pipeline):
            match_stage = next(
                (s["$match"] for s in pipeline if "$match" in s), {}
            )
            in_ids = (match_stage.get("user_id") or {}).get("$in", [])
            from collections import Counter
            counts = Counter(
                c["user_id"] for c in self._claims
                if c.get("user_id") in in_ids
            )
            return [{"_id": uid, "count": cnt} for uid, cnt in counts.items()]

    class _FakeUsers:
        def __init__(self, users):
            self._users = list(users)

        def find(self, filt=None, proj=None):
            filt = filt or {}
            results = list(self._users)
            in_ids = (filt.get("user_id") or {}).get("$in")
            if in_ids is not None:
                results = [u for u in results if u.get("user_id") in in_ids]
            return results

    class _FakeSnapshots:
        def __init__(self):
            self.docs = {}

        def bulk_write(self, ops, ordered=False):
            for op in ops:
                filt = getattr(op, "_filter", {})
                update = getattr(op, "_doc", {})
                key = (filt.get("account"), filt.get("snapshot_week"))
                self.docs[key] = update.get("$set", {})
            return type("R", (), {"modified_count": 0, "upserted_count": len(ops)})()

    class _FakeMkt:
        def __init__(self, docs, week):
            self._docs = docs
            self._week = week

        def find(self, filt=None):
            filt = filt or {}
            return list(self._docs) if filt.get("snapshot_week") == self._week else []

    def _run(self, mkt_docs, claims, users, week="2026-W25",
             now=None, dry_run=False):
        now = now or datetime(2026, 6, 16, tzinfo=timezone.utc)
        snaps = self._FakeSnapshots()
        summary = engine.run_shadow_segment_engine(
            users_col=self._FakeUsers(users),
            voucher_claims_col=self._FakeVoucherClaims(claims),
            marketing_col=self._FakeMkt(mkt_docs, week),
            snapshots_col=snaps,
            snapshot_week=week,
            now=now,
            dry_run=dry_run,
        )
        return summary, snaps

    def test_matched_row_resolves_user_id_and_username(self):
        week = "2026-W25"
        summary, snaps = self._run(
            mkt_docs=[{"account": "player1", "coupon_code": "ABC123",
                       "after_total_bet_amount": 800, "withdraw_amount": 100,
                       "snapshot_week": week}],
            claims=[{"voucher_code": "ABC123", "user_id": 999}],
            users=[{"user_id": 999, "username": "tguser", "total_referrals": 5,
                    "streak": 3, "total_xp": 1000}],
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["matched_rows"], 1)
        self.assertEqual(summary["unmatched_rows"], 0)
        self.assertEqual(summary["identity_match_rate"], 100.0)
        doc = snaps.docs[("player1", week)]
        self.assertEqual(doc["user_id"], 999)
        self.assertEqual(doc["username"], "tguser")
        self.assertEqual(doc["backend_segment"], "high_value")

    def test_unmatched_coupon_gives_none_user_id(self):
        week = "2026-W25"
        summary, snaps = self._run(
            mkt_docs=[{"account": "player2", "coupon_code": "NOMATCH",
                       "after_total_bet_amount": 100, "withdraw_amount": 0,
                       "snapshot_week": week}],
            claims=[],
            users=[],
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["matched_rows"], 0)
        self.assertEqual(summary["unmatched_rows"], 1)
        self.assertEqual(summary["identity_match_rate"], 0.0)
        doc = snaps.docs[("player2", week)]
        self.assertIsNone(doc["user_id"])
        self.assertIsNone(doc["username"])

    def test_row_without_coupon_code_gives_none_user_id(self):
        week = "2026-W25"
        summary, snaps = self._run(
            mkt_docs=[{"account": "player3", "after_total_bet_amount": 100,
                       "withdraw_amount": 0, "snapshot_week": week}],
            claims=[{"voucher_code": "ANYCODE", "user_id": 111}],
            users=[{"user_id": 111, "username": "tg3"}],
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["matched_rows"], 0)
        self.assertEqual(summary["unmatched_rows"], 1)
        doc = snaps.docs[("player3", week)]
        self.assertIsNone(doc["user_id"])

    def test_partial_match_rate(self):
        week = "2026-W25"
        mkt_docs = [
            {"account": "p1", "coupon_code": "C1", "after_total_bet_amount": 100,
             "withdraw_amount": 0, "snapshot_week": week},
            {"account": "p2", "coupon_code": "C2", "after_total_bet_amount": 100,
             "withdraw_amount": 0, "snapshot_week": week},
            {"account": "p3", "coupon_code": "NOEXIST", "after_total_bet_amount": 100,
             "withdraw_amount": 0, "snapshot_week": week},
            {"account": "p4", "after_total_bet_amount": 100,
             "withdraw_amount": 0, "snapshot_week": week},
        ]
        claims = [
            {"voucher_code": "C1", "user_id": 1},
            {"voucher_code": "C2", "user_id": 2},
        ]
        users = [
            {"user_id": 1, "username": "tg1", "total_referrals": 0, "streak": 0, "total_xp": 0},
            {"user_id": 2, "username": "tg2", "total_referrals": 0, "streak": 0, "total_xp": 0},
        ]
        summary, _ = self._run(mkt_docs=mkt_docs, claims=claims, users=users)
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["matched_rows"], 2)
        self.assertEqual(summary["unmatched_rows"], 2)
        self.assertEqual(summary["identity_match_rate"], 50.0)

    def test_checkin_count_reads_streak_field(self):
        """Regression: users.streak (not checkin_count/checkin_streak) is the check-in field."""
        week = "2026-W25"
        summary, snaps = self._run(
            mkt_docs=[{"account": "p1", "coupon_code": "CC1",
                       "after_total_bet_amount": 0, "withdraw_amount": 0,
                       "snapshot_week": week}],
            claims=[{"voucher_code": "CC1", "user_id": 5}],
            users=[{"user_id": 5, "username": "u5", "streak": 7,
                    "total_referrals": 0, "total_xp": 0}],
        )
        self.assertTrue(summary["ok"])
        doc = snaps.docs[("p1", week)]
        # streak=7 → checkin_count=7 → ghost rule requires checkin_count==0, so NOT ghost
        self.assertNotEqual(doc["backend_segment"], "ghost")

    def test_referral_count_from_user_doc(self):
        week = "2026-W25"
        summary, snaps = self._run(
            mkt_docs=[{"account": "p1", "coupon_code": "CC2",
                       "after_total_bet_amount": 0, "withdraw_amount": 0,
                       "snapshot_week": week}],
            claims=[{"voucher_code": "CC2", "user_id": 6}],
            users=[{"user_id": 6, "username": "u6", "streak": 0,
                    "total_referrals": 2, "total_xp": 0}],
        )
        doc = snaps.docs[("p1", week)]
        # referral_count=2 → not ghost (ghost needs referral_count==0)
        self.assertNotEqual(doc["backend_segment"], "ghost")

    def test_dry_run_reports_identity_stats_without_writing(self):
        week = "2026-W25"
        summary, snaps = self._run(
            mkt_docs=[{"account": "p1", "coupon_code": "DRY1",
                       "after_total_bet_amount": 100, "withdraw_amount": 0,
                       "snapshot_week": week}],
            claims=[{"voucher_code": "DRY1", "user_id": 77}],
            users=[{"user_id": 77, "username": "dry_user", "streak": 0,
                    "total_referrals": 0, "total_xp": 0}],
            dry_run=True,
        )
        self.assertTrue(summary["ok"])
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["matched_rows"], 1)
        self.assertEqual(summary["identity_match_rate"], 100.0)
        self.assertEqual(len(snaps.docs), 0)  # nothing written in dry run

    def test_snapshot_contains_username_field(self):
        """username is stored in snapshot so dashboards can display it."""
        week = "2026-W25"
        _, snaps = self._run(
            mkt_docs=[{"account": "mktacct", "coupon_code": "USR1",
                       "after_total_bet_amount": 100, "withdraw_amount": 0,
                       "snapshot_week": week}],
            claims=[{"voucher_code": "USR1", "user_id": 42}],
            users=[{"user_id": 42, "username": "tghandle",
                    "streak": 0, "total_referrals": 0, "total_xp": 0}],
        )
        doc = snaps.docs[("mktacct", week)]
        self.assertIn("username", doc)
        self.assertEqual(doc["username"], "tghandle")


if __name__ == "__main__":
    unittest.main()
