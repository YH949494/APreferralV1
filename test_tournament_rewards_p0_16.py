"""P0.16 §D — list_reward_pools() no longer calls voucher_pool_service.
pool_stock() once per pool. It now calls pool_stock_bulk() once for the
whole page, reusing the existing bulk aggregation (already used by the
Mission admin landing page) rather than inventing a second one. Stock math
itself is untouched — pool_stock_bulk's own semantics are covered by
test_voucher_pool_service.py; this file only covers the endpoint's use of
it and result parity with the old per-row pool_stock() calls.
"""

from datetime import datetime, timezone

import pytest
from flask import Flask

import database
import campaign_centre as cc
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
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(tr.tournament_rewards_bp)
    return app


def test_empty_pool_list_returns_empty_and_never_calls_stock_aggregate(fake_db, monkeypatch):
    calls = []
    real_aggregate = fake_db["voucher_pools"].aggregate
    monkeypatch.setattr(fake_db["voucher_pools"], "aggregate",
                         lambda pipeline: (calls.append(pipeline) or real_aggregate(pipeline)))
    resp = _app().test_client().get("/api/admin/reward-pools")
    assert resp.get_json()["pools"] == []
    assert len(calls) == 0


def test_single_pool_stock_matches_individual_pool_stock(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward")
    vps.upload_codes("gold", ["G1", "G2", "G3"])

    resp = _app().test_client().get("/api/admin/reward-pools")
    pool = resp.get_json()["pools"][0]
    assert pool["stock"] == vps.pool_stock("gold")
    assert pool["stock"] == {"available": 3, "issued": 0}


def test_multiple_pools_stock_matches_individual_pool_stock_and_uses_one_aggregate_call(fake_db, monkeypatch):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward")
    vps.register_pool("silver", name="Silver", pool_type="tournament_reward")
    vps.register_pool("bronze", name="Bronze", pool_type="tournament_reward")
    vps.upload_codes("gold", ["G1", "G2"])
    vps.upload_codes("silver", ["S1"])
    # bronze stays empty — mixed stock (some pools with codes, one with none)

    calls = []
    real_aggregate = fake_db["voucher_pools"].aggregate
    monkeypatch.setattr(fake_db["voucher_pools"], "aggregate",
                         lambda pipeline: (calls.append(pipeline) or real_aggregate(pipeline)))

    resp = _app().test_client().get("/api/admin/reward-pools")
    body = resp.get_json()
    assert len(calls) == 1, "must fetch stock for every pool on the page in one aggregate call, not one per pool"
    stock_by_pool = {p["pool_id"]: p["stock"] for p in body["pools"]}
    assert stock_by_pool["gold"] == vps.pool_stock("gold") == {"available": 2, "issued": 0}
    assert stock_by_pool["silver"] == vps.pool_stock("silver") == {"available": 1, "issued": 0}
    assert stock_by_pool["bronze"] == vps.pool_stock("bronze") == {"available": 0, "issued": 0}


def test_issued_vouchers_counted_correctly_in_bulk_stock(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward")
    vps.upload_codes("gold", ["G1", "G2"])
    database.db["voucher_pools"].update_one(
        {"pool_id": "gold", "code": "G1"}, {"$set": {"status": "issued"}}
    )

    resp = _app().test_client().get("/api/admin/reward-pools")
    pool = resp.get_json()["pools"][0]
    assert pool["stock"] == {"available": 1, "issued": 1}
    assert pool["stock"] == vps.pool_stock("gold")


def test_pool_source_scoping_preserved_in_bulk_lookup(fake_db):
    """A voucher_pools row from an unrelated legacy source (different
    pool_source, same pool_id) must never inflate this module's own stock
    count — pool_stock_bulk owns this scoping exactly like pool_stock does,
    and the endpoint must not bypass it."""
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward")
    vps.upload_codes("gold", ["G1"])
    database.db["voucher_pools"].insert_one({
        "pool_id": "gold", "code": "LEGACY-1", "status": "available", "pool_source": "legacy_module",
    })

    resp = _app().test_client().get("/api/admin/reward-pools")
    pool = resp.get_json()["pools"][0]
    assert pool["stock"] == {"available": 1, "issued": 0}
    assert pool["stock"] == vps.pool_stock("gold")
