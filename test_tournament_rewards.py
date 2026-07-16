"""Tests for tournament_rewards.py: rank->pool mapping, atomic voucher
allocation idempotency, out-of-stock handling, and admin approval flow."""

from datetime import datetime, timezone

import pytest
from flask import Flask

import database
import campaign_centre as cc
import tournament_rewards as tr
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "campaign_voucher_pools": [("pool_id",)],
        "campaign_voucher_codes": [("pool_id", "code")],
        "tournament_rewards": [("tournament_id", "telegram_user_id")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(tr, "database", database)
    monkeypatch.setattr(cc, "database", database)
    return fdb


def _campaign():
    return {
        "campaign_id": "july-tournament-2026",
        "type": "tournament",
        "reward_config": {"rules": [
            {"rule_id": "rank-1", "min_rank": 1, "max_rank": 1, "pool_id": "gold", "reward_label": "Champion"},
            {"rule_id": "rank-2-3", "min_rank": 2, "max_rank": 3, "pool_id": "silver", "reward_label": "Top 3"},
        ]},
    }


def test_rank_to_rule_maps_correctly():
    campaign = _campaign()
    assert tr._rank_to_rule(campaign, 1)["pool_id"] == "gold"
    assert tr._rank_to_rule(campaign, 2)["pool_id"] == "silver"
    assert tr._rank_to_rule(campaign, 3)["pool_id"] == "silver"
    assert tr._rank_to_rule(campaign, 4) is None


def _reward(**overrides):
    base = {
        "reward_id": "rw_1",
        "campaign_id": "july-tournament-2026",
        "tournament_id": "t1",
        "telegram_user_id": 111,
        "pool_id": "gold",
        "status": "approved",
    }
    base.update(overrides)
    return base


def test_atomic_allocation_assigns_one_code(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now})
    fake_db["tournament_rewards"].insert_one(_reward())

    result = tr._atomic_allocate_voucher("gold", _reward())
    assert result["status"] == "assigned"
    assert result["voucher_code"] == "CODE-A"

    code_doc = fake_db["campaign_voucher_codes"].find_one({"code": "CODE-A"})
    assert code_doc["status"] == "assigned"
    assert code_doc["assigned_to_user_id"] == 111


def test_two_concurrent_allocators_cannot_assign_same_voucher(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now})
    fake_db["tournament_rewards"].insert_one(_reward(reward_id="rw_1", telegram_user_id=111))
    fake_db["tournament_rewards"].insert_one(_reward(reward_id="rw_2", telegram_user_id=222))

    r1 = tr._atomic_allocate_voucher("gold", _reward(reward_id="rw_1", telegram_user_id=111))
    r2 = tr._atomic_allocate_voucher("gold", _reward(reward_id="rw_2", telegram_user_id=222))

    assert r1["status"] == "assigned"
    assert r2["status"] == "out_of_stock"
    assert r1["voucher_code"] != r2.get("voucher_code")


def test_retry_does_not_assign_second_voucher(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now})
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-B", "status": "available", "created_at": now})
    fake_db["tournament_rewards"].insert_one(_reward())

    first = tr._atomic_allocate_voucher("gold", _reward())
    second = tr._atomic_allocate_voucher("gold", _reward())

    assert first["voucher_code"] == second["voucher_code"] == "CODE-A"
    available_left = fake_db["campaign_voucher_codes"].count_documents({"pool_id": "gold", "status": "available"})
    assert available_left == 1


def test_out_of_stock_when_pool_empty(fake_db):
    fake_db["tournament_rewards"].insert_one(_reward())
    result = tr._atomic_allocate_voucher("gold", _reward())
    assert result["status"] == "out_of_stock"
    assert result.get("voucher_code") is None


def test_assigned_voucher_remains_assigned_after_repeated_calls(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now})
    fake_db["tournament_rewards"].insert_one(_reward())

    tr._atomic_allocate_voucher("gold", _reward())
    reward_after_first = fake_db["tournament_rewards"].find_one({"reward_id": "rw_1"})
    tr._atomic_allocate_voucher("gold", reward_after_first)
    reward_after_second = fake_db["tournament_rewards"].find_one({"reward_id": "rw_1"})

    assert reward_after_first["voucher_code"] == reward_after_second["voucher_code"]


# ---------------------------------------------------------------------------
# Full approval flow (idempotency of double-click)
# ---------------------------------------------------------------------------

def _submission(**overrides):
    base = {
        "submission_id": "tr_1",
        "provider_id": "mywin-tournament",
        "campaign_id": "july-tournament-2026",
        "tournament_id": "t1",
        "status": "pending_review",
        "winners": [{"rank": 1, "telegram_user_id": 111, "score": 100}],
    }
    base.update(overrides)
    return base


def _app():
    app = Flask(__name__)
    app.register_blueprint(tr.tournament_rewards_bp)
    return app


def test_approval_double_click_is_idempotent(fake_db, monkeypatch):
    now = datetime.now(timezone.utc)
    fake_db["campaign_voucher_pools"].insert_one({"pool_id": "gold", "status": "active"})
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now})
    fake_db["gc_campaigns"].insert_one(_campaign())
    fake_db["gc_providers"].insert_one({"provider_id": "mywin-tournament", "active": True})
    fake_db["tournament_results"].insert_one(_submission())

    import campaign_providers as cp
    monkeypatch.setattr(cp, "database", database)
    from vouchers import require_admin as _unused  # noqa: F401 ensures vouchers importable in test env

    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))

    client = _app().test_client()
    r1 = client.post("/api/admin/tournament-results/tr_1/approve")
    r2 = client.post("/api/admin/tournament-results/tr_1/approve")

    assert r1.status_code == 200
    assert r2.status_code == 200

    codes_assigned = fake_db["campaign_voucher_codes"].count_documents({"pool_id": "gold", "status": "assigned"})
    assert codes_assigned == 1

    reward = fake_db["tournament_rewards"].find_one({"tournament_id": "t1", "telegram_user_id": 111})
    assert reward["status"] == "assigned"
    assert reward["voucher_code"] == "CODE-A"


def test_result_correction_never_silently_replaces_assigned_voucher(fake_db, monkeypatch):
    now = datetime.now(timezone.utc)
    fake_db["campaign_voucher_pools"].insert_one({"pool_id": "gold", "status": "active"})
    fake_db["campaign_voucher_codes"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now})
    fake_db["gc_campaigns"].insert_one(_campaign())
    fake_db["gc_providers"].insert_one({"provider_id": "mywin-tournament", "active": True})
    fake_db["tournament_results"].insert_one(_submission())

    import campaign_providers as cp
    monkeypatch.setattr(cp, "database", database)
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))

    client = _app().test_client()
    client.post("/api/admin/tournament-results/tr_1/approve")

    resp = client.post("/api/admin/tournament-results/tr_1/request-correction")
    assert resp.status_code == 200
    assert resp.get_json()["requires_manual_review"] is True

    reward = fake_db["tournament_rewards"].find_one({"tournament_id": "t1", "telegram_user_id": 111})
    assert reward["status"] == "assigned"
    assert reward["voucher_code"] == "CODE-A"
