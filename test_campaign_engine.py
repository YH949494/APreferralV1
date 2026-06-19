"""Tests for campaign_engine.py targeting logic."""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from campaign_engine import (
    VALID_SEGMENTS,
    VALID_PLAYER_AGE_TYPES,
    VALID_CLAIM_RISK_LEVELS,
    _build_snapshot_match,
    _get_latest_snapshot_week,
    preview_audience,
    get_historical_performance,
    SEGMENT_CAMPAIGN_SUGGESTIONS,
)


# ---------------------------------------------------------------------------
# _build_snapshot_match
# ---------------------------------------------------------------------------

def test_build_snapshot_match_empty():
    match = _build_snapshot_match({})
    assert match == {"user_id": {"$ne": None}}


def test_build_snapshot_match_segments():
    match = _build_snapshot_match({"segments": ["high_value", "ghost"]})
    assert match["backend_segment"] == {"$in": ["high_value", "ghost"]}


def test_build_snapshot_match_invalid_segments_filtered():
    match = _build_snapshot_match({"segments": ["high_value", "FAKE_SEGMENT"]})
    assert match["backend_segment"] == {"$in": ["high_value"]}


def test_build_snapshot_match_all_invalid_segments_omitted():
    match = _build_snapshot_match({"segments": ["FAKE"]})
    assert "backend_segment" not in match


def test_build_snapshot_match_player_age_types():
    match = _build_snapshot_match({"player_age_types": ["new_player"]})
    assert match["player_age_type"] == {"$in": ["new_player"]}


def test_build_snapshot_match_claim_risk():
    match = _build_snapshot_match({"claim_risk_levels": ["normal", "medium_risk"]})
    assert match["claim_risk_level"] == {"$in": ["normal", "medium_risk"]}


def test_build_snapshot_match_referral_range():
    match = _build_snapshot_match({"referral_count_min": 5, "referral_count_max": 20})
    assert match["metrics_snapshot.referral_count"] == {"$gte": 5, "$lte": 20}


def test_build_snapshot_match_checkin_min_only():
    match = _build_snapshot_match({"checkin_count_min": 10})
    assert match["metrics_snapshot.checkin_count"] == {"$gte": 10}


def test_build_snapshot_match_combined():
    match = _build_snapshot_match({
        "segments": ["high_value"],
        "player_age_types": ["old_player"],
        "claim_risk_levels": ["normal"],
        "referral_count_min": 1,
        "checkin_count_max": 30,
    })
    assert match["backend_segment"] == {"$in": ["high_value"]}
    assert match["player_age_type"] == {"$in": ["old_player"]}
    assert match["claim_risk_level"] == {"$in": ["normal"]}
    assert match["metrics_snapshot.referral_count"] == {"$gte": 1}
    assert match["metrics_snapshot.checkin_count"] == {"$lte": 30}


# ---------------------------------------------------------------------------
# _get_latest_snapshot_week
# ---------------------------------------------------------------------------

def test_get_latest_snapshot_week_returns_week():
    col = MagicMock()
    col.find_one.return_value = {"snapshot_week": "2026-W24"}
    result = _get_latest_snapshot_week(col)
    assert result == "2026-W24"


def test_get_latest_snapshot_week_no_data():
    col = MagicMock()
    col.find_one.return_value = None
    result = _get_latest_snapshot_week(col)
    assert result is None


# ---------------------------------------------------------------------------
# preview_audience
# ---------------------------------------------------------------------------

def _mock_db(snapshot_docs=None, user_docs=None, agg_results=None):
    db = MagicMock()
    snapshots_col = MagicMock()
    users_col = MagicMock()
    campaigns_col = MagicMock()

    snapshots_col.find_one.return_value = {"snapshot_week": "2026-W24"}
    snapshots_col.aggregate.return_value = agg_results or []
    users_col.find.return_value = user_docs or []
    campaigns_col.find.return_value = []

    db.__getitem__ = lambda self, key: {
        "backend_segment_snapshots": snapshots_col,
        "users": users_col,
        "campaigns": campaigns_col,
    }.get(key, MagicMock())
    return db, snapshots_col, users_col, campaigns_col


def test_preview_audience_no_snapshot_data():
    db, snapshots_col, _, _ = _mock_db()
    snapshots_col.find_one.return_value = None
    result = preview_audience(db, {})
    assert result["audience_size"] == 0
    assert "warning" in result


def test_preview_audience_empty_result():
    db, _, _, _ = _mock_db(agg_results=[])
    result = preview_audience(db, {"segments": ["high_value"]})
    assert result["audience_size"] == 0
    assert result["segment_distribution"] == {}
    assert result["snapshot_week"] == "2026-W24"


def test_preview_audience_with_data():
    agg = [
        {"_id": "high_value", "count": 50, "avg_bet": 1200.0, "avg_claims": 3.0},
        {"_id": "ghost", "count": 20, "avg_bet": 0.0, "avg_claims": 0.0},
    ]
    db, _, _, _ = _mock_db(agg_results=agg)
    result = preview_audience(db, {}, voucher_value=10.0)

    assert result["audience_size"] == 70
    assert result["expected_voucher_cost"] == 700.0
    assert "high_value" in result["segment_distribution"]
    assert result["segment_distribution"]["high_value"]["count"] == 50
    assert result["segment_distribution"]["high_value"]["pct"] == pytest.approx(71.4, abs=0.1)
    assert result["segment_distribution"]["ghost"]["pct"] == pytest.approx(28.6, abs=0.1)


def test_preview_audience_recency_filter_no_active_users():
    db, snapshots_col, users_col, _ = _mock_db()
    users_col.find.return_value = []
    result = preview_audience(db, {"activity_recency_days": 7})
    assert result["audience_size"] == 0
    # When no active users, aggregate should NOT be called
    snapshots_col.aggregate.assert_not_called()


def test_preview_audience_suggestions_populated():
    agg = [{"_id": "voucher_hunter", "count": 5, "avg_bet": 50.0, "avg_claims": 12.0}]
    db, _, _, _ = _mock_db(agg_results=agg)
    result = preview_audience(db, {})
    dist = result["segment_distribution"]["voucher_hunter"]
    assert dist["suggestions"] == SEGMENT_CAMPAIGN_SUGGESTIONS["voucher_hunter"]["types"]
    assert dist["exposure"] == "minimal"


# ---------------------------------------------------------------------------
# Segment suggestions coverage
# ---------------------------------------------------------------------------

def test_all_valid_segments_have_suggestions():
    for seg in VALID_SEGMENTS:
        assert seg in SEGMENT_CAMPAIGN_SUGGESTIONS, f"Missing suggestions for {seg}"
        assert SEGMENT_CAMPAIGN_SUGGESTIONS[seg]["types"], f"Empty types for {seg}"


def test_segment_exposure_levels():
    assert SEGMENT_CAMPAIGN_SUGGESTIONS["high_value"]["exposure"] == "full"
    assert SEGMENT_CAMPAIGN_SUGGESTIONS["voucher_hunter"]["exposure"] == "minimal"
    assert SEGMENT_CAMPAIGN_SUGGESTIONS["ghost"]["exposure"] == "minimal"
    assert SEGMENT_CAMPAIGN_SUGGESTIONS["low_value"]["exposure"] == "reduced"
