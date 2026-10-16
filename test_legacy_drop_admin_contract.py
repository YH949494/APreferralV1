"""Contract guard for the legacy Mini App drop-management panel (static/index.html).

The admin dashboard is being redesigned. Until it reaches parity, the Mini App
panel is the only place that can do some of these jobs, and it talks to the
backend with the request shapes pinned below. These tests fail if a backend change
(or a dashboard-driven refactor) removes or renames something that panel depends on.

They deliberately assert the *contract* (accepted payload fields, ops, status
codes, response keys) and not behaviour that hotfixes intentionally changed
(for example what Start does to the scheduled end), so they hold before and after
the Phase 1 hotfix PRs. See docs/admin_legacy_drop_capability_inventory.md.
"""
from __future__ import annotations

from pathlib import Path

import mongomock
import pytest
from flask import Flask

import vouchers
from vouchers import vouchers_bp

ROOT = Path(__file__).parent
INDEX_HTML = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
PREVIEW_JS = (ROOT / "static" / "js" / "voucher_drop_preview.js").read_text(encoding="utf-8")


@pytest.fixture()
def mdb(monkeypatch):
    db = mongomock.MongoClient()["legacy_contract"]
    db.vouchers.create_index([("code", 1)], unique=True)
    db.vouchers.create_index(
        [("type", 1), ("dropId", 1), ("usernameLower", 1)], unique=True,
        partialFilterExpression={"type": {"$in": list(vouchers.PERSONALISED_TYPE_ALIASES)}},
    )
    monkeypatch.setattr(vouchers, "db", db)
    monkeypatch.setattr(vouchers, "voucher_claims_col", db["voucher_claims"])
    return db


@pytest.fixture()
def client(mdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    return app.test_client()


BASE = {"name": "Legacy", "startsAtLocal": "2035-01-01 10:00:00", "priority": 150}


def _create(client, **over):
    return client.post("/v2/miniapp/admin/drops", json={**BASE, **over})


# ---- the panel still calls exactly these endpoints / ops ------------------------------------
def test_panel_endpoints_and_ops_referenced_by_the_legacy_ui_still_exist():
    for fragment in ("/admin/drops_v2", "/admin/drops${tail}", "/codes${tail}", "/actions${tail}"):
        assert fragment in INDEX_HTML, fragment
    for op in ("start_now", "pause", "end_now", "set_dates", "delete"):
        assert op in INDEX_HTML, op
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    rules = {r.rule for r in app.url_map.iter_rules()}
    for rule in (
        "/v2/miniapp/admin/drops", "/v2/miniapp/admin/drops_v2",
        "/v2/miniapp/admin/drops/<drop_id>/codes", "/v2/miniapp/admin/drops/<drop_id>/actions",
    ):
        assert rule in rules, rule


def test_preview_helper_still_emits_every_field_the_create_endpoint_reads():
    for field in ("eligibility", "audience", "hero_title", "hero_subtitle", "hero_image", "whitelistUsernames"):
        assert f"payload.{field}" in PREVIEW_JS, field


# ---- create: every payload shape the panel can produce -------------------------------------------------
def test_pooled_create_with_whitelist_hero_and_pool(client, mdb):
    res = _create(
        client, type="pooled", codes=["P1", "P2"], pool="my", whitelistUsernames=["@Alice", "bob"],
        hero_title="T", hero_subtitle="S", hero_image="https://x/y.png",
    )
    body = res.get_json()
    assert res.status_code == 200 and body["status"] == "ok" and body["dropId"]
    drop = mdb.drops.find_one({})
    assert drop["hero_title"] == "T" and drop["hero_subtitle"] == "S" and drop["hero_image"] == "https://x/y.png"
    assert drop["priority"] == 150 and drop["type"] == "pooled"
    assert drop["my_remaining"] == 2
    assert {u.lower().lstrip("@") for u in drop["whitelistUsernames"]} == {"alice", "bob"}


def test_personalised_create_with_assignments(client, mdb):
    res = _create(client, type="personalised", assignments=[{"username": "@a", "code": "X1"}, {"username": "@b", "code": "X2"}])
    assert res.status_code == 200
    rows = list(mdb.vouchers.find({}))
    assert sorted(r["code"] for r in rows) == ["X1", "X2"]
    assert all(r.get("usernameLower") for r in rows)


@pytest.mark.parametrize(
    "eligibility, expected_mode",
    [
        ({"mode": "tier", "allow": ["VIP1", "vip2"]}, "tier"),
        ({"mode": "user_id", "allow": [1, "2"]}, "user_id"),
        ({"mode": "admin_only"}, "admin_only"),
        ({"mode": "public"}, "public"),
    ],
)
def test_every_eligibility_mode_the_panel_offers_is_accepted(client, mdb, eligibility, expected_mode):
    res = _create(client, type="pooled", codes=["E1"], eligibility=eligibility)
    assert res.status_code == 200
    assert mdb.drops.find_one({})["eligibility"]["mode"] == expected_mode


def test_welcome_new_joiner_shortcut_still_maps_to_public_pooled_new_joiner_audience(client, mdb):
    res = _create(client, type="personalised", codes=["W1"], eligibility={"mode": "new_joiner"}, audience={"type": "new_joiner"})
    assert res.status_code == 200
    drop = mdb.drops.find_one({})
    assert drop["type"] == "pooled"
    assert drop["eligibility"]["mode"] == "public"
    assert drop["audience"]["type"] == "new_joiner"


def test_allowed_regions_are_stored_on_the_audience(client, mdb):
    res = _create(client, type="pooled", codes=["R1"], audience={"regions": ["Malaysia", "Thailand"]})
    assert res.status_code == 200
    assert "Malaysia" in (mdb.drops.find_one({}).get("audience") or {}).get("regions", [])


def test_end_defaults_to_24h_after_start_when_omitted(client, mdb):
    assert _create(client, type="pooled", codes=["D1"]).status_code == 200
    drop = mdb.drops.find_one({})
    assert (drop["endsAt"] - drop["startsAt"]).total_seconds() == 24 * 3600


# ---- list ----------------------------------------------------------------------------------------------------------
def test_drops_v2_rows_carry_every_key_the_panel_renders(client, mdb):
    _create(client, type="pooled", codes=["L1", "L2"])
    items = client.get("/v2/miniapp/admin/drops_v2").get_json()["items"]
    assert len(items) == 1
    for key in ("dropId", "name", "type", "status", "priority", "startsAt", "endsAt", "codesFree", "codesTotal"):
        assert key in items[0], key
    _create(client, name="P", type="personalised", startsAtLocal="2035-02-01 10:00:00", assignments=[{"username": "@a", "code": "Q1"}])
    rows = client.get("/v2/miniapp/admin/drops_v2").get_json()["items"]
    personalised = next(r for r in rows if r["type"] == "personalised")
    for key in ("assigned", "claimed"):
        assert key in personalised, key


# ---- actions -------------------------------------------------------------------------------------------------------
def _drop_id(client, **over):
    return _create(client, type="pooled", codes=over.pop("codes", ["A1"]), **over).get_json()["dropId"]


def _act(client, drop_id, **body):
    return client.post(f"/v2/miniapp/admin/drops/{drop_id}/actions", json=body)


def test_start_pause_end_return_ok_and_set_a_status(client, mdb):
    did = _drop_id(client)
    for op, expected in (("start_now", "active"), ("pause", "paused"), ("end_now", "expired")):
        res = _act(client, did, op=op)
        assert res.status_code == 200 and res.get_json()["status"] == "ok", op
        assert mdb.drops.find_one({})["status"] == expected, op


def test_set_dates_renames_and_reschedules_with_the_panels_payload(client, mdb):
    did = _drop_id(client)
    res = _act(client, did, op="set_dates", name=" Renamed ", startsAt="2035-03-01 09:00:00", endsAt="2035-03-02 09:00:00")
    assert res.status_code == 200 and res.get_json()["status"] == "ok"
    drop = mdb.drops.find_one({})
    assert drop["name"] == "Renamed" and drop["endsAt"] > drop["startsAt"]
    assert _act(client, did, op="set_dates", startsAt="2035-03-02 09:00:00", endsAt="2035-03-01 09:00:00").status_code == 400
    assert _act(client, did, op="set_dates", startsAt="2035-03-01 09:00:00").status_code == 400


def test_delete_removes_the_drop_and_its_unclaimed_codes_and_reports_counts(client, mdb):
    did = _drop_id(client, codes=["Z1", "Z2"])
    res = _act(client, did, op="delete")
    body = res.get_json()
    assert res.status_code == 200 and body["status"] == "ok" and body.get("codesRemoved") == 2
    assert mdb.drops.count_documents({}) == 0 and mdb.vouchers.count_documents({}) == 0


def test_delete_refuses_a_drop_with_claimed_codes_with_codes_claimed(client, mdb):
    did = _drop_id(client, codes=["C1", "C2"])
    mdb.vouchers.update_one({"code": "C1"}, {"$set": {"status": "claimed", "claimedBy": 7}})
    res = _act(client, did, op="delete")
    body = res.get_json()
    assert res.status_code == 409 and body["code"] == "codes_claimed" and "claimedCount" in body
    assert mdb.drops.count_documents({}) == 1


def test_unknown_drop_is_404_for_non_delete_ops(client):
    assert _act(client, "000000000000000000000000", op="pause").status_code == 404


# ---- add data --------------------------------------------------------------------------------------------------------
def test_add_pooled_codes_to_a_pooled_drop_with_the_panels_payload(client, mdb):
    did = _drop_id(client, codes=["A1"])
    res = client.post(f"/v2/miniapp/admin/drops/{did}/codes", json={"type": "pooled", "pool": "public", "codes": ["N1", "N2"]})
    assert res.status_code == 200 and res.get_json()["inserted"] == 2
    assert mdb.vouchers.count_documents({"dropId": did}) == 3


@pytest.mark.xfail(
    strict=True,
    reason="KNOWN GAP (pre-existing): the Mini App 'Personalised assignments' add-data path posts "
           "{type:'personalised', assignments:[...]} to /codes, but the endpoint only accepts pooled drops "
           "(400 bad_type). Fixing it is feature work needing a product decision; see the inventory doc. "
           "If this starts passing, remove the xfail and update the doc.",
)
def test_known_gap_adding_personalised_assignments_to_an_existing_drop(client, mdb):
    did = _create(client, type="personalised", assignments=[{"username": "@a", "code": "Q1"}]).get_json()["dropId"]
    res = client.post(f"/v2/miniapp/admin/drops/{did}/codes", json={"type": "personalised", "assignments": [{"username": "@b", "code": "Q2"}]})
    assert res.status_code == 200
