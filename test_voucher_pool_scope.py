"""Tests for the explicit voucher-pool allocation-scope model (Part 1 of
the isolation refinement): registry validation, scope conflicts, migration,
and the admin endpoints' override-rejection guard."""

from datetime import datetime, timezone

import pytest
from flask import Flask

import database
import tournament_rewards as tr
import voucher_pool_service as vps
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "voucher_pool_registry": [("pool_id",)],
        "voucher_pools": [("pool_id", "code")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(vps, "database", database)
    monkeypatch.setattr(tr, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(tr.tournament_rewards_bp)
    return app


# ---------------------------------------------------------------------------
# register_pool validation
# ---------------------------------------------------------------------------

def test_register_pool_requires_valid_pool_type(fake_db):
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.register_pool("gold", name="Gold", pool_type="not_a_type", allocation_scope="campaign_rewards")
    assert exc.value.code == "invalid_pool_type"


def test_register_pool_requires_valid_allocation_scope(fake_db):
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="not_a_scope")
    assert exc.value.code == "invalid_allocation_scope"


def test_register_pool_defaults_match_tournament_reward_campaign_rewards(fake_db):
    pool = vps.register_pool("gold", name="Gold")
    assert pool["pool_type"] == "tournament_reward"
    assert pool["allocation_scope"] == "campaign_rewards"


def test_register_pool_affiliate_example(fake_db):
    pool = vps.register_pool("some-affiliate-pool", name="Affiliate Pool", pool_type="affiliate", allocation_scope="affiliate_rewards")
    assert pool["pool_type"] == "affiliate"
    assert pool["allocation_scope"] == "affiliate_rewards"


def test_register_pool_shared_example(fake_db):
    pool = vps.register_pool("shared-pool", name="Shared Pool", pool_type="other", allocation_scope="shared")
    assert pool["allocation_scope"] == "shared"


def test_register_pool_is_idempotent_for_plain_metadata(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    vps.register_pool("gold", name="Gold Renamed", pool_type="tournament_reward", allocation_scope="campaign_rewards",
                       reward_usage="rank_reward")
    pool = vps.get_pool("gold")
    assert pool["name"] == "Gold Renamed"
    assert pool["reward_usage"] == "rank_reward"


def test_register_pool_rejects_conflicting_scope_reregistration(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="shared")
    assert exc.value.code == "pool_scope_conflict"


def test_register_pool_still_rejects_reserved_legacy_ids(fake_db):
    with pytest.raises(vps.ReservedPoolIdError):
        vps.register_pool("T1", name="Collision")


# ---------------------------------------------------------------------------
# migrate_pool_scope — the explicit admin migration operation
# ---------------------------------------------------------------------------

def test_migrate_pool_scope_requires_pool_to_exist(fake_db):
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.migrate_pool_scope("ghost", allocation_scope="shared")
    assert exc.value.code == "pool_not_found"


def test_migrate_pool_scope_updates_when_no_inventory(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    pool = vps.migrate_pool_scope("gold", allocation_scope="shared")
    assert pool["allocation_scope"] == "shared"


def test_migrate_pool_scope_blocked_when_inventory_exists(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    vps.upload_codes("gold", ["A1"])
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.migrate_pool_scope("gold", allocation_scope="shared")
    assert exc.value.code == "pool_has_inventory"
    # scope must remain unchanged
    assert vps.get_pool("gold")["allocation_scope"] == "campaign_rewards"


def test_migrate_pool_scope_rejects_invalid_values(fake_db):
    vps.register_pool("gold", name="Gold")
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.migrate_pool_scope("gold", allocation_scope="not_a_scope")
    assert exc.value.code == "invalid_allocation_scope"


# ---------------------------------------------------------------------------
# upload_codes: canonical metadata stamping and pool-state guards
# ---------------------------------------------------------------------------

def test_upload_codes_requires_registered_pool(fake_db):
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.upload_codes("never-registered", ["A1"])
    assert exc.value.code == "pool_not_found"


def test_upload_codes_requires_active_pool(fake_db):
    vps.register_pool("gold", name="Gold")
    vps.set_pool_status("gold", "inactive")
    with pytest.raises(vps.VoucherPoolError) as exc:
        vps.upload_codes("gold", ["A1"])
    assert exc.value.code == "pool_inactive"


def test_upload_codes_stamps_canonical_metadata_from_registry(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    vps.upload_codes("gold", ["A1"])
    row = fake_db["voucher_pools"].find_one({"code": "A1"})
    assert row["pool_type"] == "tournament_reward"
    assert row["allocation_scope"] == "campaign_rewards"
    assert row["pool_source"] == "campaign_centre"


# ---------------------------------------------------------------------------
# Campaign allocation only ever consumes campaign_rewards/shared rows
# ---------------------------------------------------------------------------

def test_campaign_allocation_consumes_campaign_rewards_scope(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    vps.upload_codes("gold", ["A1"])
    result = vps.allocate_voucher("gold", reward_id="rw_1", telegram_user_id=111)
    assert result["code"] == "A1"


def test_campaign_allocation_consumes_shared_scope_when_explicitly_configured(fake_db):
    vps.register_pool("shared-pool", name="Shared", pool_type="other", allocation_scope="shared")
    vps.upload_codes("shared-pool", ["S1"])
    result = vps.allocate_voucher("shared-pool", reward_id="rw_1", telegram_user_id=111)
    assert result["code"] == "S1"


def test_campaign_allocation_rejects_affiliate_scope_rows(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "aff-pool", "code": "AFF-1", "status": "available", "created_at": now,
        "allocation_scope": "affiliate_rewards", "pool_source": "campaign_centre",
    })
    result = vps.allocate_voucher("aff-pool", reward_id="rw_1", telegram_user_id=111)
    assert result is None


def test_campaign_allocation_rejects_welcome_scope_rows(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "welcome-pool", "code": "W1", "status": "available", "created_at": now,
        "allocation_scope": "welcome_rewards", "pool_source": "campaign_centre",
    })
    result = vps.allocate_voucher("welcome-pool", reward_id="rw_1", telegram_user_id=111)
    assert result is None


def test_stock_counts_respect_allocation_scope(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", allocation_scope="campaign_rewards")
    vps.upload_codes("gold", ["A1", "A2"])
    # A row sharing pool_id but written outside this module (no pool_source)
    # must never inflate the reported stock.
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "ROGUE", "status": "available",
                                          "created_at": datetime.now(timezone.utc)})
    assert vps.pool_stock("gold") == {"available": 2, "issued": 0}


# ---------------------------------------------------------------------------
# Admin endpoint guards
# ---------------------------------------------------------------------------

def test_create_reward_pool_endpoint_rejects_invalid_pool_type(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    resp = _app().test_client().post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold", "pool_type": "nonsense"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_pool_type"


def test_create_reward_pool_endpoint_rejects_invalid_allocation_scope(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    resp = _app().test_client().post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold", "allocation_scope": "nonsense"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_allocation_scope"


def test_create_reward_pool_endpoint_defaults_tournament_scope(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    resp = _app().test_client().post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold"})
    assert resp.status_code == 201
    pool = vps.get_pool("gold")
    assert pool["pool_type"] == "tournament_reward"
    assert pool["allocation_scope"] == "campaign_rewards"


def test_upload_codes_endpoint_rejects_scope_override_attempt(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    client = _app().test_client()
    client.post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold"})
    resp = client.post("/api/admin/reward-pools/gold/upload-codes",
                        json={"codes": ["A1"], "allocation_scope": "shared"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "override_not_allowed"
    # nothing must have been inserted
    assert fake_db["voucher_pools"].count_documents({"pool_id": "gold"}) == 0


def test_upload_codes_endpoint_rejects_pool_type_override_attempt(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    client = _app().test_client()
    client.post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold"})
    resp = client.post("/api/admin/reward-pools/gold/upload-codes",
                        json={"codes": ["A1"], "pool_type": "affiliate"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "override_not_allowed"


def test_migrate_scope_endpoint_blocks_when_inventory_exists(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    client = _app().test_client()
    client.post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold"})
    client.post("/api/admin/reward-pools/gold/upload-codes", json={"codes": ["A1"]})
    resp = client.post("/api/admin/reward-pools/gold/migrate-scope", json={"allocation_scope": "shared"})
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "pool_has_inventory"


def test_admin_list_reward_pools_shows_scope_and_type(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    client = _app().test_client()
    client.post("/api/admin/reward-pools", json={"pool_id": "gold", "name": "Gold", "campaign_id": "camp-1"})
    resp = client.get("/api/admin/reward-pools")
    pool = resp.get_json()["pools"][0]
    assert pool["pool_type"] == "tournament_reward"
    assert pool["allocation_scope"] == "campaign_rewards"
    assert pool["campaign_id"] == "camp-1"
    assert "stock" in pool
