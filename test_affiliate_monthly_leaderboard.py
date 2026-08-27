"""Tests for the Mini App Affiliate leaderboard's switch from weekly
referral_flow_events/referral_events counts to the canonical monthly
qualified_events definition used by evaluate_monthly_affiliate_reward().
"""

from datetime import datetime, timedelta, timezone

import mongomock

from affiliate_leaderboard import (
    _compute_affiliate_monthly_rows,
    _build_affiliate_monthly_payload,
    affiliate_month_window_utc_from_reference,
    compute_affiliate_monthly_kpis_live,
)
from affiliate_rewards import _month_window_utc, evaluate_monthly_affiliate_reward


def _fresh_db():
    return mongomock.MongoClient().db


def _row_for(rows, referrer_id):
    for row in rows:
        if str(row.get("referrer_id")) == str(referrer_id):
            return row
    return None


class TestQualifiedCountMatchesCanonicalReward:
    def test_qualified_month_matches_evaluate_monthly_affiliate_reward(self):
        db = _fresh_db()
        db.users.insert_one({"user_id": 501, "blocked": False})
        now = datetime(2026, 2, 15, 3, 0, 0, tzinfo=timezone.utc)

        for i in range(1, 13):
            db.qualified_events.insert_one(
                {"invitee_id": i, "referrer_id": 501, "qualified_at": now}
            )

        start_utc, end_utc, _ = _month_window_utc(now)
        canonical_count = db.qualified_events.count_documents(
            {"referrer_id": 501, "qualified_at": {"$gte": start_utc, "$lt": end_utc}}
        )

        ledger = evaluate_monthly_affiliate_reward(db, referrer_id=501, now_utc=now)
        assert ledger is not None
        assert ledger["qualified_count"] == canonical_count

        month_start_utc, month_end_utc, _ = affiliate_month_window_utc_from_reference(now)
        assert month_start_utc == start_utc
        assert month_end_utc == end_utc

        rows = _compute_affiliate_monthly_rows(db, month_start_utc, month_end_utc)
        row = _row_for(rows, 501)
        assert row is not None
        assert row["qualified_month"] == canonical_count == 12


class TestKlMonthBoundary:
    def test_kl_month_boundary_excludes_previous_and_next_month(self):
        db = _fresh_db()
        # Feb 1 2026 00:00 KL == Jan 31 2026 16:00 UTC.
        # Mar 1 2026 00:00 KL == Feb 28 2026 16:00 UTC.
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        month_start_utc, month_end_utc, yyyymm = affiliate_month_window_utc_from_reference(ref)
        assert yyyymm == "202602"
        assert month_start_utc.isoformat() == "2026-01-31T16:00:00+00:00"
        assert month_end_utc.isoformat() == "2026-02-28T16:00:00+00:00"

        db.qualified_events.insert_one(
            {"invitee_id": 1, "referrer_id": 900, "qualified_at": month_start_utc}
        )
        # One microsecond before the KL month start: must not count.
        db.qualified_events.insert_one(
            {
                "invitee_id": 2,
                "referrer_id": 900,
                "qualified_at": month_start_utc.replace(microsecond=0) - timedelta(seconds=1),
            }
        )
        # Exactly at month end: must not count (exclusive upper bound).
        db.qualified_events.insert_one(
            {"invitee_id": 3, "referrer_id": 900, "qualified_at": month_end_utc}
        )

        rows = _compute_affiliate_monthly_rows(db, month_start_utc, month_end_utc)
        row = _row_for(rows, 900)
        assert row is not None
        assert row["qualified_month"] == 1


class TestJoinAndQualifyCohorts:
    def test_user_joined_last_month_but_qualified_this_month(self):
        db = _fresh_db()
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        month_start_utc, month_end_utc, _ = affiliate_month_window_utc_from_reference(ref)
        prev_month_start_utc, _, _ = affiliate_month_window_utc_from_reference(month_start_utc - timedelta(days=1))

        db.pending_referrals.insert_one(
            {
                "inviter_user_id": 10,
                "invitee_user_id": 200,
                "created_at_utc": prev_month_start_utc,
            }
        )
        db.qualified_events.insert_one(
            {"invitee_id": 200, "referrer_id": 10, "qualified_at": month_start_utc}
        )

        rows = _compute_affiliate_monthly_rows(db, month_start_utc, month_end_utc)
        row = _row_for(rows, 10)
        assert row is not None
        # Canonical reward count still counts it this month...
        assert row["qualified_month"] == 1
        # ...but the join happened in a different cohort, so joins_month is 0
        # and conversion must not be computed against a mismatched cohort.
        assert row["joins_month"] == 0
        assert row["conversion_month"] is None

    def test_user_joined_and_qualified_this_month(self):
        db = _fresh_db()
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        month_start_utc, month_end_utc, _ = affiliate_month_window_utc_from_reference(ref)

        db.pending_referrals.insert_one(
            {
                "inviter_user_id": 11,
                "invitee_user_id": 300,
                "created_at_utc": month_start_utc,
            }
        )
        db.qualified_events.insert_one(
            {"invitee_id": 300, "referrer_id": 11, "qualified_at": month_start_utc}
        )

        rows = _compute_affiliate_monthly_rows(db, month_start_utc, month_end_utc)
        row = _row_for(rows, 11)
        assert row is not None
        assert row["joins_month"] == 1
        assert row["qualified_month"] == 1
        assert row["conversion_month"] == 1.0


class TestZeroJoinsAndConversionCap:
    def test_zero_joins_zero_qualified_produces_no_row(self):
        db = _fresh_db()
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        month_start_utc, month_end_utc, _ = affiliate_month_window_utc_from_reference(ref)
        rows = _compute_affiliate_monthly_rows(db, month_start_utc, month_end_utc)
        assert rows == []

    def test_conversion_never_exceeds_100_percent(self):
        db = _fresh_db()
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        month_start_utc, month_end_utc, _ = affiliate_month_window_utc_from_reference(ref)

        # Reproduces the reported bug: 4 joins this month, but 36 qualified
        # events landed in this month's qualified_at window because most of
        # the invitees actually joined in earlier months.
        db.pending_referrals.insert_one(
            {"inviter_user_id": 20, "invitee_user_id": 1, "created_at_utc": month_start_utc}
        )
        db.pending_referrals.insert_one(
            {"inviter_user_id": 20, "invitee_user_id": 2, "created_at_utc": month_start_utc}
        )
        db.pending_referrals.insert_one(
            {"inviter_user_id": 20, "invitee_user_id": 3, "created_at_utc": month_start_utc}
        )
        db.pending_referrals.insert_one(
            {"inviter_user_id": 20, "invitee_user_id": 4, "created_at_utc": month_start_utc}
        )
        for i in range(1, 5):
            db.qualified_events.insert_one(
                {"invitee_id": i, "referrer_id": 20, "qualified_at": month_start_utc}
            )
        for i in range(5, 37):
            db.qualified_events.insert_one(
                {"invitee_id": i, "referrer_id": 20, "qualified_at": month_start_utc}
            )

        rows = _compute_affiliate_monthly_rows(db, month_start_utc, month_end_utc)
        row = _row_for(rows, 20)
        assert row is not None
        assert row["joins_month"] == 4
        assert row["qualified_month"] == 36
        # Cohort-based conversion caps at 100% since only same-cohort
        # invitees who both joined and qualified are counted.
        assert row["conversion_month"] == 1.0
        assert row["conversion_month"] <= 1.0


class TestMonthReset:
    def test_month_reset_gives_fresh_window_and_zero_counts(self):
        db = _fresh_db()
        feb_ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        feb_start, feb_end, feb_key = affiliate_month_window_utc_from_reference(feb_ref)
        db.qualified_events.insert_one(
            {"invitee_id": 1, "referrer_id": 30, "qualified_at": feb_start}
        )
        feb_rows = _compute_affiliate_monthly_rows(db, feb_start, feb_end)
        assert _row_for(feb_rows, 30)["qualified_month"] == 1

        mar_ref = datetime(2026, 3, 15, 0, 0, 0, tzinfo=timezone.utc)
        mar_start, mar_end, mar_key = affiliate_month_window_utc_from_reference(mar_ref)
        assert mar_key != feb_key
        assert mar_start == feb_end
        mar_rows = _compute_affiliate_monthly_rows(db, mar_start, mar_end)
        assert _row_for(mar_rows, 30) is None

    def test_live_cache_rebuilds_on_month_rollover(self, monkeypatch):
        import affiliate_leaderboard as mod

        db = _fresh_db()
        feb_ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        first = compute_affiliate_monthly_kpis_live(db, reference_utc=feb_ref)
        feb_start, _, _ = affiliate_month_window_utc_from_reference(feb_ref)
        first_start = mod._coerce_datetime_utc(first["month_start_utc"])
        assert first_start == feb_start

        mar_ref = datetime(2026, 3, 15, 0, 0, 0, tzinfo=timezone.utc)
        called = {"ok": False}
        real_build = mod._build_affiliate_monthly_payload

        def _spy_build(*args, **kwargs):
            called["ok"] = True
            return real_build(*args, **kwargs)

        monkeypatch.setattr(mod, "_build_affiliate_monthly_payload", _spy_build)
        second = compute_affiliate_monthly_kpis_live(db, reference_utc=mar_ref)
        assert called["ok"] is True
        mar_start, _, _ = affiliate_month_window_utc_from_reference(mar_ref)
        second_start = mod._coerce_datetime_utc(second["month_start_utc"])
        assert second_start == mar_start
        assert second_start != first_start


class TestMyStatsMatchesLeaderboardRow:
    def test_my_stats_row_matches_leaderboard_entry(self):
        db = _fresh_db()
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)

        db.pending_referrals.insert_one(
            {"inviter_user_id": 42, "invitee_user_id": 900, "created_at_utc": ref}
        )
        db.qualified_events.insert_one(
            {"invitee_id": 900, "referrer_id": 42, "qualified_at": ref}
        )

        _, payload = _build_affiliate_monthly_payload(db, reference_utc=ref)
        leaderboard_row = _row_for(payload["affiliate_leaderboard_month"], 42)
        my_stats_row = payload["affiliate_monthly_by_referrer"]["42"]

        assert leaderboard_row is not None
        assert leaderboard_row["joins_month"] == my_stats_row["joins_month"]
        assert leaderboard_row["qualified_month"] == my_stats_row["qualified_month"]
        assert leaderboard_row["conversion_month"] == my_stats_row["conversion_month"]
        assert leaderboard_row["quality_flag"] == my_stats_row["quality_flag"]

    def test_my_stats_fallback_not_dropped_when_many_referrers_have_activity(self):
        # A referrer who qualified this month but joined in an earlier month has
        # joins_month == 0. If affiliate_monthly_by_referrer were capped by
        # joins_month (as it once was), such a referrer could fall out of the
        # cache once enough other referrers had nonzero joins, and "My Stats"
        # would wrongly report zero instead of falling back correctly.
        db = _fresh_db()
        ref = datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc)
        month_start_utc, _, _ = affiliate_month_window_utc_from_reference(ref)
        prev_month_start_utc, _, _ = affiliate_month_window_utc_from_reference(
            month_start_utc - timedelta(days=1)
        )

        # Many other referrers with joins this month, to fill any join-ranked cap.
        for i in range(1, 505):
            db.pending_referrals.insert_one(
                {
                    "inviter_user_id": 1000 + i,
                    "invitee_user_id": 2000 + i,
                    "created_at_utc": ref,
                }
            )

        # This referrer joined last month and qualified this month: joins_month=0.
        db.pending_referrals.insert_one(
            {
                "inviter_user_id": 42,
                "invitee_user_id": 900,
                "created_at_utc": prev_month_start_utc,
            }
        )
        db.qualified_events.insert_one(
            {"invitee_id": 900, "referrer_id": 42, "qualified_at": ref}
        )

        _, payload = _build_affiliate_monthly_payload(db, reference_utc=ref)
        my_stats_row = payload["affiliate_monthly_by_referrer"].get("42")
        assert my_stats_row is not None
        assert my_stats_row["joins_month"] == 0
        assert my_stats_row["qualified_month"] == 1
