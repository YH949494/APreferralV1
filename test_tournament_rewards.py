"""Tests for tournament_rewards.py: rule-based rank->pool mapping, atomic
voucher allocation against the shared Voucher Centre inventory (idempotency
under concurrency/retries/replay), and admin approval flow."""

from datetime import datetime, timezone

import pytest
from flask import Flask

import database
import campaign_centre as cc
import reward_engine
import tournament_rewards as tr
import voucher_pool_service as vps
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "voucher_pool_registry": [("pool_id",)],
        "voucher_pools": [("pool_id", "code")],
        "campaign_rewards": [("tournament_id", "telegram_user_id")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(tr, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(vps, "database", database)
    return fdb


def _campaign():
    return {
        "campaign_id": "july-tournament-2026",
        "type": "tournament",
        "reward_config": {"rules": [
            {"rule_id": "rank-1", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 1}, "pool_id": "gold", "reward_label": "Champion"},
            {"rule_id": "rank-2-3", "condition_type": "rank", "params": {"min_rank": 2, "max_rank": 3}, "pool_id": "silver", "reward_label": "Top 3"},
        ]},
    }


def test_rank_to_rule_maps_correctly():
    rules = _campaign()["reward_config"]["rules"]
    assert reward_engine.match_rule(rules, {"rank": 1})["pool_id"] == "gold"
    assert reward_engine.match_rule(rules, {"rank": 2})["pool_id"] == "silver"
    assert reward_engine.match_rule(rules, {"rank": 3})["pool_id"] == "silver"
    assert reward_engine.match_rule(rules, {"rank": 4}) is None


def _reward(**overrides):
    base = {
        "reward_id": "rw_1",
        "category": "tournament",
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
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    fake_db["campaign_rewards"].insert_one(_reward())

    result = tr._atomic_allocate_voucher("gold", _reward())
    assert result["status"] == "assigned"
    assert result["voucher_code"] == "CODE-A"

    code_doc = fake_db["voucher_pools"].find_one({"code": "CODE-A"})
    assert code_doc["status"] == "issued"
    assert code_doc["issued_to_user_id"] == 111


def test_two_concurrent_allocators_cannot_assign_same_voucher(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    fake_db["campaign_rewards"].insert_one(_reward(reward_id="rw_1", telegram_user_id=111))
    fake_db["campaign_rewards"].insert_one(_reward(reward_id="rw_2", telegram_user_id=222))

    r1 = tr._atomic_allocate_voucher("gold", _reward(reward_id="rw_1", telegram_user_id=111))
    r2 = tr._atomic_allocate_voucher("gold", _reward(reward_id="rw_2", telegram_user_id=222))

    assert r1["status"] == "assigned"
    assert r2["status"] == "out_of_stock"
    assert r1["voucher_code"] != r2.get("voucher_code")


def test_retry_does_not_assign_second_voucher(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "CODE-B", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    fake_db["campaign_rewards"].insert_one(_reward())

    first = tr._atomic_allocate_voucher("gold", _reward())
    second = tr._atomic_allocate_voucher("gold", _reward())

    assert first["voucher_code"] == second["voucher_code"] == "CODE-A"
    available_left = fake_db["voucher_pools"].count_documents({"pool_id": "gold", "status": "available"})
    assert available_left == 1


def test_out_of_stock_when_pool_empty(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward())
    result = tr._atomic_allocate_voucher("gold", _reward())
    assert result["status"] == "out_of_stock"
    assert result.get("voucher_code") is None


def test_assigned_voucher_remains_assigned_after_repeated_calls(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "CODE-A", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    fake_db["campaign_rewards"].insert_one(_reward())

    tr._atomic_allocate_voucher("gold", _reward())
    reward_after_first = fake_db["campaign_rewards"].find_one({"reward_id": "rw_1"})
    tr._atomic_allocate_voucher("gold", reward_after_first)
    reward_after_second = fake_db["campaign_rewards"].find_one({"reward_id": "rw_1"})

    assert reward_after_first["voucher_code"] == reward_after_second["voucher_code"]


def test_provider_cannot_access_voucher_code_via_pool_service(fake_db):
    # voucher_pool_service exposes stock counts only, never codes.
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "SECRET-CODE", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    stock = vps.pool_stock("gold")
    assert stock == {"available": 1, "issued": 0}
    assert "SECRET-CODE" not in str(stock)


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
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", campaign_id="july-tournament-2026")
    vps.upload_codes("gold", ["CODE-A"])
    fake_db["gc_campaigns"].insert_one(_campaign())
    fake_db["gc_providers"].insert_one({"provider_id": "mywin-tournament", "active": True})
    fake_db["tournament_results"].insert_one(_submission())

    import campaign_providers as cp
    monkeypatch.setattr(cp, "database", database)

    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))

    client = _app().test_client()
    r1 = client.post("/api/admin/tournament-results/tr_1/approve")
    r2 = client.post("/api/admin/tournament-results/tr_1/approve")

    assert r1.status_code == 200
    assert r2.status_code == 200

    codes_issued = fake_db["voucher_pools"].count_documents({"pool_id": "gold", "status": "issued"})
    assert codes_issued == 1

    reward = fake_db["campaign_rewards"].find_one({"tournament_id": "t1", "telegram_user_id": 111})
    assert reward["status"] == "assigned"
    assert reward["voucher_code"] == "CODE-A"
    assert reward["category"] == "tournament"


def test_result_correction_never_silently_replaces_assigned_voucher(fake_db, monkeypatch):
    now = datetime.now(timezone.utc)
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", campaign_id="july-tournament-2026")
    vps.upload_codes("gold", ["CODE-A"])
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

    reward = fake_db["campaign_rewards"].find_one({"tournament_id": "t1", "telegram_user_id": 111})
    assert reward["status"] == "assigned"
    assert reward["voucher_code"] == "CODE-A"


def test_reward_pool_endpoints_reuse_shared_inventory(fake_db, monkeypatch):
    """Uploading codes via the Campaign Centre reward-pools endpoint writes
    into the same db.voucher_pools table the Voucher Centre owns — no
    second inventory collection is created."""
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    client = _app().test_client()

    resp = client.post("/api/admin/reward-pools", json={"pool_id": "silver", "name": "Silver", "pool_type": "tournament_reward"})
    assert resp.status_code == 201

    resp = client.post("/api/admin/reward-pools/silver/upload-codes", json={"codes": ["S1", "S2", "S1"]})
    assert resp.get_json()["inserted"] == 2
    assert resp.get_json()["skipped_duplicates"] == 1

    # Rows land in voucher_pools, the pre-existing Voucher Centre collection.
    assert fake_db["voucher_pools"].count_documents({"pool_id": "silver", "status": "available"}) == 2
    # And no parallel "campaign_voucher_codes" collection was ever touched.
    assert "campaign_voucher_codes" not in fake_db._collections


def test_list_reward_pools_reconciles_stock_and_serializes_dates(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    client = _app().test_client()
    client.post("/api/admin/reward-pools", json={"pool_id": "bronze", "name": "Bronze"})
    client.post("/api/admin/reward-pools/bronze/upload-codes", json={"codes": ["B1", "B2", "B3"]})

    resp = client.get("/api/admin/reward-pools")
    pool = next(p for p in resp.get_json()["pools"] if p["pool_id"] == "bronze")
    assert pool["stock"] == {"available": 3, "issued": 0}
    assert isinstance(pool["created_at"], str)
    from datetime import datetime as _dt
    _dt.fromisoformat(pool["created_at"])  # must be ISO 8601, not an HTTP-date string
