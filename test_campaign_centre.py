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


def test_status_scheduled_absent_even_with_past_starts_at():
    # status="scheduled" (not "live") must be excluded regardless of timing.
    c = _campaign(status="scheduled")
    assert cc.is_publicly_active(c, _provider()) is False


def test_status_ended_absent_even_within_schedule_window():
    # status="ended" (not "live") must be excluded even if starts_at/ends_at
    # would otherwise be "in window" — status is checked first.
    c = _campaign(status="ended")
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

def _rank_rule(rule_id, min_rank, max_rank, pool_id):
    return {"rule_id": rule_id, "condition_type": "rank", "params": {"min_rank": min_rank, "max_rank": max_rank}, "pool_id": pool_id}


def test_reward_rules_overlap_rejected():
    rules = [_rank_rule("a", 1, 3, "p1"), _rank_rule("b", 3, 5, "p2")]
    assert cc._validate_reward_rules(rules) == "overlapping_rank_ranges"


def test_reward_rules_non_overlapping_ok():
    rules = [_rank_rule("a", 1, 1, "p1"), _rank_rule("b", 2, 3, "p2")]
    assert cc._validate_reward_rules(rules) is None


def test_reward_rules_missing_pool_rejected():
    rules = [{"rule_id": "a", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 1}}]
    assert cc._validate_reward_rules(rules) == "missing_pool_id"


def test_reward_rules_duplicate_rule_id_rejected():
    rules = [_rank_rule("a", 1, 1, "p1"), _rank_rule("a", 2, 2, "p2")]
    assert cc._validate_reward_rules(rules) == "duplicate_or_missing_rule_id"


def test_reward_rules_non_rank_condition_types_supported():
    rules = [
        {"rule_id": "participation", "condition_type": "participation", "params": {}, "pool_id": "p1"},
        {"rule_id": "vip", "condition_type": "vip", "params": {}, "pool_id": "p2"},
    ]
    assert cc._validate_reward_rules(rules) is None


def test_reward_rules_invalid_condition_type_rejected():
    rules = [{"rule_id": "a", "condition_type": "not_a_real_type", "params": {}, "pool_id": "p1"}]
    assert cc._validate_reward_rules(rules) == "invalid_condition_type"


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


def test_active_endpoint_excludes_every_non_public_state_in_one_sweep(fake_db):
    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_providers"].insert_one(_provider(provider_id="inactive-provider", active=False))
    now = datetime.now(timezone.utc)

    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-draft", status="draft"))
    fake_db["gc_campaigns"].insert_one(_campaign(
        campaign_id="c-scheduled-future", status="live",
        schedule={"starts_at": now + timedelta(days=1), "ends_at": None}))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-paused", status="paused"))
    fake_db["gc_campaigns"].insert_one(_campaign(
        campaign_id="c-ended", status="live",
        schedule={"starts_at": now - timedelta(days=2), "ends_at": now - timedelta(hours=1)}))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-archived", status="archived"))
    fake_db["gc_campaigns"].insert_one(_campaign(
        campaign_id="c-provider-inactive", status="live",
        destination={"provider_id": "inactive-provider", "open_mode": "telegram_web_app", "path": "/x", "ready": True}))
    dest_not_ready = _campaign(campaign_id="c-dest-not-ready")
    dest_not_ready["destination"]["ready"] = False
    fake_db["gc_campaigns"].insert_one(dest_not_ready)
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-fully-active", status="live"))

    client = _app().test_client()
    resp = client.get("/api/campaigns/active")
    ids = [c["campaign_id"] for c in resp.get_json()["campaigns"]]
    assert ids == ["c-fully-active"]


def test_admin_list_still_shows_non_public_campaigns(fake_db):
    """Admins must be able to see draft/paused/archived/destination-not-ready
    campaigns — only the public endpoint filters them out."""
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider())
    for status in ("draft", "paused", "archived"):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-{status}", status=status))
    dest_not_ready = _campaign(campaign_id="c-not-ready")
    dest_not_ready["destination"]["ready"] = False
    fake_db["gc_campaigns"].insert_one(dest_not_ready)

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns")
    assert resp.status_code == 200
    ids = {c["campaign_id"] for c in resp.get_json()["campaigns"]}
    assert {"c-draft", "c-paused", "c-archived", "c-not-ready"}.issubset(ids)


def test_admin_get_single_campaign_does_not_500(fake_db):
    """Regression test: _serialize() must not mutate the shared schedule
    dict before visibility_explanation() reads it (previously caused a
    TypeError: '>' not supported between str and datetime, a 500 on every
    GET /api/admin/gc-campaigns and /api/admin/gc-campaigns/{id} call)."""
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c1", status="draft"))

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns/c1")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["campaign"]["campaign_id"] == "c1"
    assert isinstance(body["campaign"]["schedule"]["starts_at"], str)
    assert body["campaign"]["effective_visibility"]["publicly_visible"] is False


def test_serialize_never_mutates_the_original_document(fake_db):
    """Direct unit-level guard for the same class of bug: calling
    _serialize() must leave the source document's schedule datetimes
    intact for any code that reads it afterward."""
    campaign = _campaign(campaign_id="mutation-guard")
    fake_db["gc_campaigns"].insert_one(campaign)
    stored = fake_db["gc_campaigns"].find_one({"campaign_id": "mutation-guard"})
    original_starts_at = stored["schedule"]["starts_at"]
    assert isinstance(original_starts_at, datetime)

    cc._serialize(stored)

    assert isinstance(stored["schedule"]["starts_at"], datetime)
    assert stored["schedule"]["starts_at"] == original_starts_at
