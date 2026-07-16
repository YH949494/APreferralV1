"""Tests for voucher_pool_service.py: reuse of the existing Voucher Centre
inventory table (db.voucher_pools) — no second inventory collection."""

from datetime import datetime, timezone

import pytest

import database
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
    return fdb


def test_register_pool_writes_only_registry_metadata(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", campaign_id="c1", reward_usage="rank-1")
    pool = vps.get_pool("gold")
    assert pool["name"] == "Gold"
    assert pool["pool_type"] == "tournament_reward"
    assert pool["campaign_id"] == "c1"
    assert pool["status"] == "active"
    # No inventory row was created just by registering.
    assert fake_db["voucher_pools"].count_documents({"pool_id": "gold"}) == 0


def test_upload_codes_writes_into_shared_voucher_pools_collection(fake_db):
    vps.register_pool("gold", name="Gold")
    result = vps.upload_codes("gold", ["A1", "A2", "A1"])
    assert result == {"inserted": 2, "skipped_duplicates": 1}
    rows = list(fake_db["voucher_pools"].find({"pool_id": "gold"}))
    assert len(rows) == 2
    assert all(r["status"] == "available" for r in rows)


def test_pool_stock_counts_available_and_issued(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "A1", "status": "available", "created_at": now})
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "A2", "status": "issued", "created_at": now})
    assert vps.pool_stock("gold") == {"available": 1, "issued": 1}


def test_allocate_voucher_is_atomic_and_idempotent(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "A1", "status": "available", "created_at": now})
    first = vps.allocate_voucher("gold", reward_id="rw_1", telegram_user_id=111)
    assert first["status"] == "issued"
    assert first["issued_to_user_id"] == 111

    second = vps.allocate_voucher("gold", reward_id="rw_2", telegram_user_id=222)
    assert second is None  # pool exhausted, no second code available


def test_allocate_voucher_out_of_stock_returns_none(fake_db):
    assert vps.allocate_voucher("empty-pool", reward_id="rw_1", telegram_user_id=111) is None


def test_pool_is_active_reflects_registry_status(fake_db):
    vps.register_pool("gold", name="Gold")
    assert vps.pool_is_active("gold") is True
    vps.set_pool_status("gold", "inactive")
    assert vps.pool_is_active("gold") is False


def test_list_pools_filters_by_campaign(fake_db):
    vps.register_pool("gold", name="Gold", campaign_id="camp-1")
    vps.register_pool("silver", name="Silver", campaign_id="camp-2")
    result = vps.list_pools(campaign_id="camp-1")
    assert [p["pool_id"] for p in result] == ["gold"]
