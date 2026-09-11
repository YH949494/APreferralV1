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
# visibility_explanation() vs _transition()'s publish gate: a registration-
# enabled campaign (any type) and a mission_pool campaign never require a
# destination/provider in the real publish gate, so visibility_explanation
# must not report one as a "reason" either (P0.5a follow-up).
# ---------------------------------------------------------------------------

def test_visibility_explanation_registration_enabled_no_destination_no_provider():
    """Registration-enabled campaign with no destination and no provider,
    otherwise fully live/in-window: must NOT report a destination/provider
    blocker — _transition()'s registration_only bypass never requires one."""
    c = _campaign(
        registration={"enabled": True},
        destination={"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
    )
    explanation = cc.visibility_explanation(c, None)
    assert explanation == {"publicly_visible": True, "reasons": []}


def test_visibility_explanation_registration_enabled_destination_not_ready_no_blocker():
    c = _campaign(registration={"enabled": True})
    c["destination"]["ready"] = False
    explanation = cc.visibility_explanation(c, _provider())
    assert not any("destination" in r for r in explanation["reasons"])
    assert not any("provider" in r for r in explanation["reasons"])


def test_visibility_explanation_registration_disabled_destination_not_ready_still_blocks():
    """Normal (non-registration) campaign: destination.ready is still
    enforced exactly as before."""
    c = _campaign(registration={"enabled": False})
    c["destination"]["ready"] = False
    explanation = cc.visibility_explanation(c, _provider())
    assert explanation["publicly_visible"] is False
    assert any("destination.ready is false" in r for r in explanation["reasons"])


def test_visibility_explanation_registration_disabled_missing_provider_still_blocks():
    c = _campaign(registration={"enabled": False})
    explanation = cc.visibility_explanation(c, None)
    assert explanation["publicly_visible"] is False
    assert any("linked provider does not exist" in r for r in explanation["reasons"])


def test_visibility_explanation_registration_disabled_inactive_provider_still_blocks():
    c = _campaign(registration={"enabled": False})
    explanation = cc.visibility_explanation(c, _provider(active=False))
    assert explanation["publicly_visible"] is False
    assert any("linked provider is inactive" in r for r in explanation["reasons"])


def test_visibility_explanation_mission_pool_no_destination_no_provider():
    """Mission Pool has no destination/provider in its publish gate at all
    (mission_config + mission_pool.pool_id instead) — must never report a
    destination/provider blocker regardless of registration."""
    c = _campaign(
        type="mission_pool",
        mechanic="mission_pool",
        destination={"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
        registration={"enabled": False},
    )
    explanation = cc.visibility_explanation(c, None)
    assert explanation == {"publicly_visible": True, "reasons": []}


def test_visibility_explanation_mission_pool_still_reports_schedule_and_status_reasons():
    """Mission Pool's bypass is destination/provider-only — status/schedule
    reasons are unaffected."""
    c = _campaign(type="mission_pool", mechanic="mission_pool", status="draft")
    explanation = cc.visibility_explanation(c, None)
    assert explanation["publicly_visible"] is False
    assert any("status" in r for r in explanation["reasons"])
    assert not any("destination" in r for r in explanation["reasons"])
    assert not any("provider" in r for r in explanation["reasons"])


# ---------------------------------------------------------------------------
# _as_utc / naive-vs-aware datetime regression (production TypeError fix)
# ---------------------------------------------------------------------------

def test_as_utc_returns_none_for_none():
    assert cc._as_utc(None) is None


def test_as_utc_interprets_naive_as_utc():
    naive = datetime(2026, 1, 1, 12, 0, 0)
    result = cc._as_utc(naive)
    assert result.tzinfo is timezone.utc
    assert result == naive.replace(tzinfo=timezone.utc)


def test_as_utc_converts_aware_non_utc_to_utc():
    from datetime import timedelta as _td

    plus8 = timezone(_td(hours=8))
    aware = datetime(2026, 1, 1, 20, 0, 0, tzinfo=plus8)
    result = cc._as_utc(aware)
    assert result.tzinfo is timezone.utc
    assert result == aware.astimezone(timezone.utc)


def test_as_utc_rejects_non_datetime():
    with pytest.raises(TypeError):
        cc._as_utc("2026-01-01")


def test_visibility_explanation_naive_starts_at_with_aware_now_future():
    """MongoDB-shaped naive starts_at (interpreted as UTC) in the future
    against an aware `now` must not raise and must report not-yet-started."""
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": (now + timedelta(days=1)).replace(tzinfo=None), "ends_at": None})
    explanation = cc.visibility_explanation(c, _provider(), now)
    assert explanation["publicly_visible"] is False
    assert any("scheduled to start" in r for r in explanation["reasons"])


def test_visibility_explanation_naive_starts_at_active_campaign():
    """Naive starts_at in the past + aware now => campaign reads as active,
    matching the naive-UTC MongoDB read path, without raising."""
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": (now - timedelta(hours=1)).replace(tzinfo=None), "ends_at": None})
    explanation = cc.visibility_explanation(c, _provider(), now)
    assert explanation == {"publicly_visible": True, "reasons": []}


def test_visibility_explanation_naive_ends_at_ended_campaign():
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={
        "starts_at": (now - timedelta(days=2)).replace(tzinfo=None),
        "ends_at": (now - timedelta(hours=1)).replace(tzinfo=None),
    })
    explanation = cc.visibility_explanation(c, _provider(), now)
    assert explanation["publicly_visible"] is False
    assert any("ended at" in r for r in explanation["reasons"])


def test_visibility_explanation_all_aware_active_campaign():
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)})
    explanation = cc.visibility_explanation(c, _provider(), now)
    assert explanation == {"publicly_visible": True, "reasons": []}


def test_visibility_explanation_missing_starts_at():
    c = _campaign(schedule={"starts_at": None, "ends_at": None})
    explanation = cc.visibility_explanation(c, _provider())
    assert explanation["publicly_visible"] is False
    assert any("starts_at is not set" in r for r in explanation["reasons"])


def test_visibility_explanation_missing_ends_at_does_not_end():
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": now - timedelta(days=1), "ends_at": None})
    explanation = cc.visibility_explanation(c, _provider(), now)
    assert explanation == {"publicly_visible": True, "reasons": []}


def test_is_publicly_active_naive_starts_at_from_mongo_shape():
    """Same TypeError class guarded on the is_publicly_active() path used by
    the public /api/campaigns/active endpoint."""
    now = datetime.now(timezone.utc)
    c = _campaign(schedule={"starts_at": (now - timedelta(hours=1)).replace(tzinfo=None), "ends_at": None})
    assert cc.is_publicly_active(c, _provider(), now) is True


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


def test_admin_get_single_campaign_exposes_deep_link_when_registration_enabled(fake_db, monkeypatch):
    """Parity with list_campaigns: the single-campaign GET must expose the
    same registration_deep_link so the Campaign Detail page's Share section
    doesn't need a second request to the list endpoint (P0.5a)."""
    from unittest.mock import patch

    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(
        campaign_id="c-reg", status="draft", registration={"enabled": True}
    ))

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns/c-reg")
    body = resp.get_json()
    assert body["campaign"]["registration_deep_link"] == "https://t.me/AdvantPlayBot?startapp=campaign_c-reg"


def test_admin_get_single_campaign_omits_deep_link_when_registration_not_enabled(fake_db):
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-no-reg", status="draft"))

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns/c-no-reg")
    body = resp.get_json()
    assert "registration_deep_link" not in body["campaign"]


# ---------------------------------------------------------------------------
# gc-campaigns list: mission_active_rewards field for the Campaign Centre
# table's "End Rewards" visibility (no N+1 fan-out)
# ---------------------------------------------------------------------------

def _mission_campaign(**overrides):
    base = _campaign(
        campaign_id="mission-1",
        type="mission_pool",
        mechanic="mission_pool",
        mission_config={"mission_type": "keyword", "prompt": "?", "correct_answer": "a"},
        mission_pool={"pool_id": "MP-1", "winner_count": 1, "cancelled": False},
        destination={"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": True},
    )
    base.update(overrides)
    return base


def _mission_reward(**overrides):
    now = datetime.now(timezone.utc)
    base = {
        "reward_id": "rw-1", "campaign_id": "mission-1", "category": "mission_pool",
        "status": "assigned", "expires_at": now + timedelta(hours=1),
    }
    base.update(overrides)
    return base


def test_list_exposes_mission_active_rewards_for_mission_pool_rows(fake_db):
    from unittest.mock import patch

    fake_db["gc_campaigns"].insert_one(_mission_campaign())
    fake_db["campaign_rewards"].insert_one(_mission_reward())
    fake_db["campaign_rewards"].insert_one(_mission_reward(reward_id="rw-2"))

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns")
    card = resp.get_json()["campaigns"][0]
    assert card["mission_active_rewards"] == 2


def test_list_reports_zero_active_rewards_when_none_or_expired(fake_db):
    from unittest.mock import patch

    fake_db["gc_campaigns"].insert_one(_mission_campaign())
    fake_db["campaign_rewards"].insert_one(
        _mission_reward(reward_id="rw-expired", expires_at=datetime.now(timezone.utc) - timedelta(hours=1))
    )

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns")
    card = resp.get_json()["campaigns"][0]
    assert card["mission_active_rewards"] == 0


def test_list_never_adds_mission_active_rewards_to_non_mission_rows(fake_db):
    """P0 isolation: Standard Drop / tournament rows never get Mission-only
    fields, so the frontend's mechanic check is the only thing gating the
    Mission actions — there is no stray field to accidentally key off."""
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="standard-1", type="tournament"))

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns")
    card = resp.get_json()["campaigns"][0]
    assert "mission_active_rewards" not in card


def test_list_computes_mission_active_rewards_in_one_aggregate_call(fake_db, monkeypatch):
    """No N+1: however many Mission Pool rows are on the page, the reward
    count must come from a single aggregate query, not one per row."""
    from unittest.mock import patch

    fake_db["gc_campaigns"].insert_one(_mission_campaign())
    fake_db["gc_campaigns"].insert_one(_mission_campaign(campaign_id="mission-2"))
    fake_db["campaign_rewards"].insert_one(_mission_reward())
    fake_db["campaign_rewards"].insert_one(_mission_reward(reward_id="rw-2", campaign_id="mission-2"))

    calls = []
    real_aggregate = fake_db["campaign_rewards"].aggregate
    monkeypatch.setattr(fake_db["campaign_rewards"], "aggregate",
                         lambda pipeline: (calls.append(pipeline) or real_aggregate(pipeline)))

    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().get("/api/admin/gc-campaigns")
    cards = {c["campaign_id"]: c for c in resp.get_json()["campaigns"]}
    assert cards["mission-1"]["mission_active_rewards"] == 1
    assert cards["mission-2"]["mission_active_rewards"] == 1
    assert len(calls) == 1


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


# ---------------------------------------------------------------------------
# P0.14 — telegram.require_subscription default semantics
#
# Root cause: _validate_body used to hardcode
# `bool(raw_tg.get("require_subscription", True))`, so a wizard-created
# Tournament/External campaign (which sends `telegram: {}` — no channel, no
# explicit flag) silently got require_subscription=True with no channel
# configured. subscription_gate.verify_campaign_subscription then fails
# closed with channel_not_configured for every player, forever
# (test_missing_channel_config_fails_closed in test_subscription_gate.py).
# The fix is presence-sensitive: an explicit boolean always wins; absent
# that, default to whatever is actually enforceable (on iff a channel is
# configured).
# ---------------------------------------------------------------------------

def _min_body(telegram: dict, **overrides):
    body = {
        "name": "Telegram Default Test",
        "type": "tournament",
        "schedule": {"starts_at": datetime.now(timezone.utc).isoformat(), "ends_at": None},
        "telegram": telegram,
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
    }
    body.update(overrides)
    return body


def test_no_channel_flag_omitted_defaults_subscription_off():
    updates, code = cc._validate_body(_min_body({}))
    assert code is None
    assert updates["telegram"]["require_subscription"] is False


def test_channel_username_present_flag_omitted_defaults_subscription_on():
    updates, code = cc._validate_body(_min_body({"channel_username": "advantplayofficial"}))
    assert code is None
    assert updates["telegram"]["require_subscription"] is True


def test_channel_id_present_flag_omitted_defaults_subscription_on():
    updates, code = cc._validate_body(_min_body({"channel_id": -100123456}))
    assert code is None
    assert updates["telegram"]["require_subscription"] is True


def test_channel_present_explicit_false_is_respected():
    """Explicit false must win even though a channel is configured — never
    `bool(raw.get("require_subscription") or channel_username)`, which would
    make an explicit false indistinguishable from "not sent"."""
    updates, code = cc._validate_body(_min_body({
        "channel_username": "advantplayofficial", "require_subscription": False,
    }))
    assert code is None
    assert updates["telegram"]["require_subscription"] is False
    assert updates["telegram"]["channel_username"] == "advantplayofficial"


def test_no_channel_explicit_true_is_rejected_as_invalid_config():
    """An admin must never be able to save require_subscription=True with no
    channel configured — that config can never be satisfied by any player
    (subscription_gate fails closed with channel_not_configured). Reject it
    outright rather than silently saving a permanently-broken campaign."""
    updates, code = cc._validate_body(_min_body({"require_subscription": True}))
    assert updates is None
    assert code == "subscription_channel_required"


def test_update_route_full_telegram_block_preserves_untouched_siblings(fake_db):
    """Models the frontend's read-merge-PUT contract: sending back the
    complete existing block with only require_subscription flipped must
    leave every sibling field (channel_id, channel_username,
    require_identity) exactly as it was."""
    from unittest.mock import patch

    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-tg", telegram={
        "require_identity": True, "require_subscription": True,
        "channel_id": -100999, "channel_username": "existing_channel",
    }))
    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    client = admin_app.test_client()
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = client.put("/api/admin/gc-campaigns/c-tg", json={"telegram": {
            "require_identity": True, "require_subscription": False,
            "channel_id": -100999, "channel_username": "existing_channel",
        }})
    assert resp.status_code == 200
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": "c-tg"})
    assert doc["telegram"] == {
        "require_identity": True, "require_subscription": False,
        "channel_id": -100999, "channel_username": "existing_channel",
    }


def test_update_route_omitting_telegram_key_leaves_it_untouched(fake_db):
    from unittest.mock import patch

    original_tg = {
        "require_identity": True, "require_subscription": True,
        "channel_id": None, "channel_username": "existing_channel",
    }
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-tg2", telegram=original_tg))
    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    client = admin_app.test_client()
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = client.put("/api/admin/gc-campaigns/c-tg2", json={"name": "Renamed"})
    assert resp.status_code == 200
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": "c-tg2"})
    assert doc["telegram"] == original_tg


def test_update_route_rejects_invalid_subscription_config(fake_db):
    from unittest.mock import patch

    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-tg3", telegram={
        "require_identity": True, "require_subscription": False, "channel_id": None, "channel_username": "",
    }))
    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    client = admin_app.test_client()
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = client.put("/api/admin/gc-campaigns/c-tg3", json={"telegram": {"require_subscription": True}})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "subscription_channel_required"
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": "c-tg3"})
    assert doc["telegram"]["require_subscription"] is False  # unchanged — rejected write never lands


# ---------------------------------------------------------------------------
# P0.14 — open_mode validity per type (P1-1 legacy-create root cause)
# ---------------------------------------------------------------------------

def test_external_subscription_verification_accepts_external_url():
    updates, code = cc._validate_body(_min_body(
        {}, type="external_subscription_verification",
        destination={"provider_id": "", "open_mode": "external_url", "path": "", "ready": False},
    ))
    assert code is None
    assert updates["destination"]["open_mode"] == "external_url"


def test_external_subscription_verification_rejects_telegram_web_app():
    """This is the exact P1-1 bug: the legacy create form hardcoded
    open_mode="telegram_web_app" regardless of type, which this type has
    never allowed (_ALLOWED_OPEN_MODES_BY_TYPE)."""
    updates, code = cc._validate_body(_min_body(
        {}, type="external_subscription_verification",
        destination={"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
    ))
    assert updates is None
    assert code == "open_mode_not_allowed_for_type"


# ---------------------------------------------------------------------------
# P0.14 — player-side /play behavior: subscription gate honors the fixed
# default, and stays fully enforced when actually configured.
# ---------------------------------------------------------------------------

def _play_app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_public_bp)
    return app


def test_play_not_blocked_when_subscription_not_required(fake_db):
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider(base_url="https://tournament.example.com"))
    fake_db["gc_campaigns"].insert_one(_campaign(telegram={
        "require_identity": True, "require_subscription": False, "channel_username": "",
    }))
    client = _play_app().test_client()
    with patch("miniapp_identity.resolve_authenticated_telegram_user_id", return_value=(111, None)), \
         patch("subscription_gate.verify_campaign_subscription") as mock_gate:
        resp = client.post(f"/api/campaigns/{_campaign()['campaign_id']}/play")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["url"]
    mock_gate.assert_not_called()


def test_play_still_gated_when_subscription_required_and_configured(fake_db):
    """Fixing the broken default must never weaken enforcement for a
    correctly-configured campaign — require_subscription=True with a real
    channel still blocks an unsubscribed player."""
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider(base_url="https://tournament.example.com"))
    fake_db["gc_campaigns"].insert_one(_campaign())  # default fixture: require_subscription True + channel set
    client = _play_app().test_client()
    with patch("miniapp_identity.resolve_authenticated_telegram_user_id", return_value=(111, None)), \
         patch("subscription_gate.verify_campaign_subscription",
               return_value={"subscribed": False, "reason": "left"}):
        resp = client.post(f"/api/campaigns/{_campaign()['campaign_id']}/play")
    assert resp.status_code == 403
    assert resp.get_json()["code"] == "subscription_required"


def test_play_passes_when_subscription_required_and_subscribed(fake_db):
    from unittest.mock import patch

    fake_db["gc_providers"].insert_one(_provider(base_url="https://tournament.example.com"))
    fake_db["gc_campaigns"].insert_one(_campaign())
    client = _play_app().test_client()
    with patch("miniapp_identity.resolve_authenticated_telegram_user_id", return_value=(111, None)), \
         patch("subscription_gate.verify_campaign_subscription",
               return_value={"subscribed": True, "reason": "member"}):
        resp = client.post(f"/api/campaigns/{_campaign()['campaign_id']}/play")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"
