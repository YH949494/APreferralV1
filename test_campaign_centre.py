"""Tests for campaign_centre.py: public visibility computation, CRUD
validation, and the public /api/campaigns/active endpoint."""

from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

import database
import campaign_centre as cc
from fake_mongo import FakeDb


def _campaign(**overrides):
    base = {
        "campaign_id": "july-tournament-2026",
        "name": "July Tournament",
        "type": "tournament",
        "status": "live",
        "schedule": {
            "starts_at": datetime.now(timezone.utc) - timedelta(hours=1),
            "ends_at": None,
        },
        "destination": {"provider_id": "mywin-tournament", "open_mode": "telegram_web_app", "path": "/x", "ready": True},
        "telegram": {"require_identity": True, "require_subscription": True, "channel_username": "advantplayofficial"},
    }
    base.update(overrides)
    return base


def _provider(**overrides):
    base = {"provider_id": "mywin-tournament", "active": True, "type": "tournament"}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# is_publicly_active
# ---------------------------------------------------------------------------

def test_draft_campaign_not_publicly_active():
    c = _campaign(status="draft")
    assert cc.is_publicly_active(c, _provider()) is False


def test_scheduled_future_campaign_not_active():
    c = _campaign(status="live", schedule={"starts_at": datetime.now(timezone.utc) + timedelta(days=1), "ends_at": None})
    assert cc.is_publicly_active(c, _provider()) is False


def test_live_before_start_absent():
    c = _campaign(schedule={"starts_at": datetime.now(timezone.utc) + timedelta(minutes=5), "ends_at": None})
    assert cc.is_publicly_active(c, _provider()) is False


def test_live_after_end_absent():
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": now - timedelta(days=2), "ends_at": now - timedelta(hours=1)})
    assert cc.is_publicly_active(c, _provider()) is False


def test_paused_campaign_absent():
    c = _campaign(status="paused")
    assert cc.is_publicly_active(c, _provider()) is False


def test_archived_campaign_absent():
    c = _campaign(status="archived")
    assert cc.is_publicly_active(c, _provider()) is False


def test_destination_not_ready_absent():
    c = _campaign()
    c["destination"]["ready"] = False
    assert cc.is_publicly_active(c, _provider()) is False


def test_inactive_provider_absent():
    c = _campaign()
    assert cc.is_publicly_active(c, _provider(active=False)) is False


def test_missing_provider_absent():
    c = _campaign()
    assert cc.is_publicly_active(c, None) is False


def test_fully_active_campaign_returned():
    c = _campaign()
    assert cc.is_publicly_active(c, _provider()) is True


def test_ended_field_absent_still_active_when_no_end():
    c = _campaign(schedule={"starts_at": datetime.now(timezone.utc) - timedelta(days=1), "ends_at": None})
    assert cc.is_publicly_active(c, _provider()) is True


def test_ends_at_in_future_still_active():
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": now - timedelta(days=1), "ends_at": now + timedelta(days=1)})
    assert cc.is_publicly_active(c, _provider()) is True


# ---------------------------------------------------------------------------
# visibility_explanation (admin preview)
# ---------------------------------------------------------------------------

def test_visibility_explanation_lists_reasons_for_draft():
    c = _campaign(status="draft")
    c["destination"]["ready"] = False
    explanation = cc.visibility_explanation(c, _provider(active=False))
    assert explanation["publicly_visible"] is False
    assert any("status" in r for r in explanation["reasons"])
    assert any("destination" in r for r in explanation["reasons"])
    assert any("inactive" in r for r in explanation["reasons"])


def test_visibility_explanation_clean_for_active_campaign():
    c = _campaign()
    explanation = cc.visibility_explanation(c, _provider())
    assert explanation == {"publicly_visible": True, "reasons": []}


# ---------------------------------------------------------------------------
# Reward rule validation
# ---------------------------------------------------------------------------

def test_reward_rules_overlap_rejected():
    rules = [
        {"rule_id": "a", "min_rank": 1, "max_rank": 3, "pool_id": "p1"},
        {"rule_id": "b", "min_rank": 3, "max_rank": 5, "pool_id": "p2"},
    ]
    assert cc._validate_reward_rules(rules) == "overlapping_rank_ranges"


def test_reward_rules_non_overlapping_ok():
    rules = [
        {"rule_id": "a", "min_rank": 1, "max_rank": 1, "pool_id": "p1"},
        {"rule_id": "b", "min_rank": 2, "max_rank": 3, "pool_id": "p2"},
    ]
    assert cc._validate_reward_rules(rules) is None


def test_reward_rules_missing_pool_rejected():
    rules = [{"rule_id": "a", "min_rank": 1, "max_rank": 1}]
    assert cc._validate_reward_rules(rules) == "missing_pool_id"


def test_reward_rules_duplicate_rule_id_rejected():
    rules = [
        {"rule_id": "a", "min_rank": 1, "max_rank": 1, "pool_id": "p1"},
        {"rule_id": "a", "min_rank": 2, "max_rank": 2, "pool_id": "p2"},
    ]
    assert cc._validate_reward_rules(rules) == "duplicate_or_missing_rule_id"


# ---------------------------------------------------------------------------
# Schedule validation
# ---------------------------------------------------------------------------

def test_schedule_missing_starts_at_rejected():
    assert cc._validate_schedule({"starts_at": None, "ends_at": None}) == "missing_starts_at"


def test_schedule_ends_before_starts_rejected():
    now = datetime.now(timezone.utc)
    assert cc._validate_schedule({"starts_at": now, "ends_at": now - timedelta(hours=1)}) == "ends_at_before_starts_at"


def test_schedule_valid_order_ok():
    now = datetime.now(timezone.utc)
    assert cc._validate_schedule({"starts_at": now, "ends_at": now + timedelta(hours=1)}) is None


# ---------------------------------------------------------------------------
# Public API endpoint: hides everything except fully active campaigns
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={"gc_campaigns": [("campaign_id",)], "gc_providers": [("provider_id",)]})
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cc, "database", database)
    import campaign_providers as cp
    monkeypatch.setattr(cp, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_public_bp)
    return app


def test_active_endpoint_hides_draft_and_returns_only_live(fake_db):
    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="draft-one", status="draft"))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="live-one", status="live"))

    client = _app().test_client()
    resp = client.get("/api/campaigns/active")
    body = resp.get_json()
    ids = [c["campaign_id"] for c in body["campaigns"]]
    assert ids == ["live-one"]


def test_active_endpoint_empty_when_nothing_active(fake_db):
    fake_db["gc_providers"].insert_one(_provider(active=False))
    fake_db["gc_campaigns"].insert_one(_campaign())

    client = _app().test_client()
    resp = client.get("/api/campaigns/active")
    assert resp.get_json()["campaigns"] == []


def test_active_endpoint_never_returns_internal_fields(fake_db):
    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(reward_config={"rules": [{"pool_id": "secret-pool"}]}))

    client = _app().test_client()
    resp = client.get("/api/campaigns/active")
    card = resp.get_json()["campaigns"][0]
    assert "reward_config" not in card
    assert "destination" not in card
    assert "created_by" not in card
