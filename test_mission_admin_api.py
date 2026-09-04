"""Phase 2.1 admin API — the read-only endpoints the dedicated Mission
Reward Pool operator surface is built on (mission_pool_ux.py).

These cover the three things the new admin UX needs that the browser must
never do for itself:

  * ONE landing list with live counters, aggregated server-side (§2) — the
    admin UI is explicitly forbidden from scanning raw ``mission_entries``.
  * ONE authoritative "is there enough inventory to publish?" verdict (§8),
    shared by the create wizard and the edit view so the two screens can
    never disagree.
  * the reward/pool facts the read-only detail view renders, including the
    "your stored pool is no longer offered for new selection" case that must
    never cause the stored pool to be rewritten (§18).

Phase 1's engine and every existing Phase 2 contract are untouched; those
are asserted in test_mission_pool*.py and test_mission_pool_phase2.py.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
import mission_pool_ux as ux
import voucher_pool_service as vps
from fake_mongo import FakeDb

UID = 991001


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
        "voucher_pool_registry": [("pool_id",)],
        "voucher_pools": [("pool_id", "code")],
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys())
    monkeypatch.setattr(database, "db", fdb)
    for module in (mp, ux, cc, vps):
        monkeypatch.setattr(module, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(ux.mission_pool_ux_admin_bp)
    app.register_blueprint(cc.campaign_centre_bp)
    return app


def _admin_ok():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "ops"}, None))


def _admin_denied():
    return patch("vouchers.require_admin", return_value=(None, ("unauthorized", 401)))


def _mission(campaign_id, **overrides):
    now = datetime.now(timezone.utc)
    block = {
        "pool_id": "MISSION-OK",
        "pool_type": "voucher_drop",
        "winner_count": 3,
        "allocation_method": "random_qualified",
        "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
        "cancelled": False,
        "processing_stage": mp.STAGE_PENDING,
        "processing_generation": 0,
    }
    block.update(overrides.pop("mission_pool", {}))
    doc = {
        "campaign_id": campaign_id,
        "name": campaign_id.title(),
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": "live",
        "created_at": now,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {
            "mission_type": "multiple_choice",
            "prompt": "Which feature do you prefer?",
            "options": [{"id": "a", "label": "Free Spins"}, {"id": "b", "label": "Cashback"}],
            "correct_answer": "a",
        },
        "mission_pool": block,
    }
    doc.update(overrides)
    database.db["gc_campaigns"].insert_one(doc)
    return doc


def _entry(campaign_id, uid, status):
    now = datetime.now(timezone.utc)
    database.db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": campaign_id, "telegram_user_id": uid, "status": status,
        "answer": "a", "answer_normalized": "a", "is_correct": True,
        "submitted_at": now, "created_at": now, "updated_at": now,
    })


def _pool(pool_id, *, available=0, issued=0, scope="campaign_rewards",
          pool_type="voucher_drop", status="active"):
    now = datetime.now(timezone.utc)
    database.db["voucher_pool_registry"].insert_one({
        "pool_id": pool_id, "name": pool_id + " name", "pool_type": pool_type,
        "allocation_scope": scope, "status": status, "created_at": now,
    })
    for i in range(available):
        database.db["voucher_pools"].insert_one({
            "pool_id": pool_id, "code": f"{pool_id}-A{i}", "status": "available",
            "pool_source": "campaign_centre", "pool_type": pool_type,
            "allocation_scope": scope, "created_at": now,
        })
    for i in range(issued):
        database.db["voucher_pools"].insert_one({
            "pool_id": pool_id, "code": f"{pool_id}-I{i}", "status": "issued",
            "pool_source": "campaign_centre", "pool_type": pool_type,
            "allocation_scope": scope, "created_at": now,
        })


# ---------------------------------------------------------------------------
# Operational state derivation (§2, §19)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status,stage,cancelled,expected", [
    ("draft", mp.STAGE_PENDING, False, ux.OPS_DRAFT),
    ("scheduled", mp.STAGE_PENDING, False, ux.OPS_SCHEDULED),
    ("live", mp.STAGE_PENDING, False, ux.OPS_LIVE),
    ("paused", mp.STAGE_PENDING, False, ux.OPS_PAUSED),
    ("ended", mp.STAGE_PENDING, False, ux.OPS_CLOSED),
    ("ended", mp.STAGE_ALLOCATING_REWARDS, False, ux.OPS_PROCESSING),
    ("ended", mp.STAGE_COMPLETED, False, ux.OPS_COMPLETED),
    ("archived", mp.STAGE_COMPLETED, False, ux.OPS_COMPLETED),
    # Cancelled wins over everything: it is the state the operator must see.
    ("live", mp.STAGE_PENDING, True, ux.OPS_CANCELLED),
    ("ended", mp.STAGE_COMPLETED, True, ux.OPS_CANCELLED),
])
def test_operational_state_is_derived_from_the_two_owned_fields(status, stage, cancelled, expected):
    campaign = {"status": status, "mission_pool": {"processing_stage": stage, "cancelled": cancelled}}
    assert ux.operational_state(campaign) == expected
    assert ux.operational_state(campaign) in ux.OPERATIONAL_STATES


def test_operator_and_participant_state_vocabularies_stay_separate():
    """Two different questions, two different vocabularies. Several words
    overlap ("live", "scheduled", "paused", "cancelled") and mean the same
    thing today, so sharing a Python name would let a change to one silently
    redefine the other."""
    operator = {n for n in dir(ux) if n.startswith("OPS_")}
    participant = {n for n in dir(ux) if n.startswith("STATE_")}
    assert operator and participant
    assert not (operator & participant)
    assert set(ux.OPERATIONAL_STATES) == {getattr(ux, n) for n in operator}
    # The participant vocabulary is unchanged by Phase 2.1.
    assert set(ux.USER_STATES) == {getattr(ux, n) for n in participant}


def test_operational_state_never_invents_a_state_for_a_bare_document():
    # A campaign written before mission_pool existed has neither field.
    assert ux.operational_state({}) == ux.OPS_DRAFT
    assert ux.operational_state(None) == ux.OPS_DRAFT


# ---------------------------------------------------------------------------
# Landing list (§2)
# ---------------------------------------------------------------------------

def test_campaign_list_returns_counters_without_the_browser_seeing_entries(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    _pool("MISSION-OK", available=35, issued=5)
    _mission("september-feedback", name="September Feedback Mission",
             mission_pool={"pool_id": "MISSION-OK", "winner_count": 20})
    for i in range(3):
        _entry("september-feedback", UID + i, mp.ENTRY_STATUS_SUBMITTED)
    _entry("september-feedback", UID + 10, mp.ENTRY_STATUS_QUALIFIED)
    _entry("september-feedback", UID + 11, mp.ENTRY_STATUS_WINNER)
    _entry("september-feedback", UID + 12, mp.ENTRY_STATUS_REWARD_ALLOCATED)
    _entry("september-feedback", UID + 13, mp.ENTRY_STATUS_DISQUALIFIED)
    database.db["campaign_rewards"].insert_one({
        "reward_id": "r1", "campaign_id": "september-feedback", "category": "mission_pool",
        "status": "assigned", "notification_status": "sent",
    })

    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/campaigns").get_json()

    assert data["status"] == "ok"
    item = data["campaigns"][0]
    assert item["campaign_id"] == "september-feedback"
    assert item["name"] == "September Feedback Mission"
    assert item["state"] == ux.OPS_LIVE
    assert item["submissions"] == 7
    # qualified counts every post-eligibility status, exactly as the Phase 1
    # summary endpoint does.
    assert item["qualified"] == 3
    assert item["winners"] == 2
    assert item["disqualified"] == 1
    assert item["rewards_allocated"] == 1
    assert item["notifications_sent"] == 1
    assert item["pool_available"] == 35
    assert item["winner_count"] == 20


def test_campaign_list_excludes_non_mission_campaigns(fake_db):
    """Standard Drop / tournament campaigns must never appear on the Mission
    surface, and the Mission surface must never act on one."""
    _mission("m1")
    database.db["gc_campaigns"].insert_one({
        "campaign_id": "tourney", "type": "tournament", "mechanic": "standard_drop",
        "status": "live", "created_at": datetime.now(timezone.utc),
    })
    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/campaigns").get_json()
    assert [c["campaign_id"] for c in data["campaigns"]] == ["m1"]


def test_campaign_list_includes_a_mission_written_before_the_mechanic_field(fake_db):
    """resolve_mechanic treats a document with no `mechanic` as a standard
    drop, so the list matches on `type` too rather than silently losing a
    campaign an operator can see everywhere else."""
    database.db["gc_campaigns"].insert_one({
        "campaign_id": "legacy-mission", "type": "mission_pool", "status": "draft",
        "created_at": datetime.now(timezone.utc), "mission_pool": {"pool_id": "P", "winner_count": 1},
    })
    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/campaigns").get_json()
    assert [c["campaign_id"] for c in data["campaigns"]] == ["legacy-mission"]


def test_campaign_list_counts_only_this_campaigns_entries_and_mission_rewards(fake_db):
    _pool("MISSION-OK", available=4)
    _mission("a")
    _mission("b")
    _entry("a", UID, mp.ENTRY_STATUS_SUBMITTED)
    _entry("b", UID, mp.ENTRY_STATUS_SUBMITTED)
    _entry("b", UID + 1, mp.ENTRY_STATUS_SUBMITTED)
    # A tournament reward on the same campaign must not inflate the Mission
    # allocation count.
    database.db["campaign_rewards"].insert_one({
        "reward_id": "t1", "campaign_id": "a", "category": "tournament", "status": "assigned",
    })
    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/campaigns").get_json()
    by_id = {c["campaign_id"]: c for c in data["campaigns"]}
    assert by_id["a"]["submissions"] == 1
    assert by_id["b"]["submissions"] == 2
    assert by_id["a"]["rewards_allocated"] == 0


def test_campaign_list_is_empty_and_well_formed_with_no_missions(fake_db):
    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/campaigns").get_json()
    assert data == {"status": "ok", "states": list(ux.OPERATIONAL_STATES), "campaigns": []}


def test_campaign_list_requires_admin(fake_db):
    with _app().test_client() as client, _admin_denied():
        assert client.get("/api/admin/mission-pool/campaigns").status_code == 401


# ---------------------------------------------------------------------------
# Inventory gate (§8)
# ---------------------------------------------------------------------------

def test_inventory_check_is_the_single_publish_rule(fake_db):
    _pool("MISSION-OK", available=20)
    with _app().test_client() as client, _admin_ok():
        ok = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=10").get_json()
        exact = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=20").get_json()
        short = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=21").get_json()
    assert ok["sufficient"] is True and ok["available"] == 20 and ok["shortfall"] == 0
    # winner_count <= available_codes — the boundary is inclusive.
    assert exact["sufficient"] is True
    assert short["sufficient"] is False and short["shortfall"] == 1


def test_inventory_check_ignores_issued_codes(fake_db):
    _pool("MISSION-OK", available=2, issued=50)
    with _app().test_client() as client, _admin_ok():
        v = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=3").get_json()
    assert v["available"] == 2
    assert v["issued"] == 50
    assert v["sufficient"] is False


def test_inventory_check_reports_the_registrys_real_pool_type(fake_db):
    """The wizard submits mission_pool.pool_type from this answer, because the
    processor passes it to allocate_voucher as expected_pool_type — a guessed
    value makes every allocation miss while stock still looks available."""
    _pool("MISSION-VIP", available=5, pool_type="vip")
    with _app().test_client() as client, _admin_ok():
        v = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-VIP&winner_count=1").get_json()
    assert v["pool_type"] == "vip"
    assert v["pool_exists"] is True


def test_inventory_check_refuses_an_unknown_or_protected_pool(fake_db):
    _pool("AFFILIATE-NO", available=100, scope="affiliate_rewards")
    with _app().test_client() as client, _admin_ok():
        missing = client.get("/api/admin/mission-pool/inventory-check?pool_id=NOPE&winner_count=1").get_json()
        protected = client.get("/api/admin/mission-pool/inventory-check?pool_id=AFFILIATE-NO&winner_count=1").get_json()
    assert missing["pool_exists"] is False and missing["sufficient"] is False
    # It exists and has stock, but Campaign Rewards may not allocate from it,
    # so it is never offered for selection.
    assert protected["pool_exists"] is True
    assert protected["pool_selectable"] is False


def test_inventory_check_rejects_a_zero_or_junk_winner_count(fake_db):
    _pool("MISSION-OK", available=10)
    with _app().test_client() as client, _admin_ok():
        zero = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=0").get_json()
        junk = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=abc").get_json()
    assert zero["sufficient"] is False
    assert junk["winner_count"] == 0 and junk["sufficient"] is False


def test_inventory_check_requires_admin(fake_db):
    with _app().test_client() as client, _admin_denied():
        assert client.get("/api/admin/mission-pool/inventory-check?pool_id=x&winner_count=1").status_code == 401


# ---------------------------------------------------------------------------
# edit-state reward block (§12, §18)
# ---------------------------------------------------------------------------

def test_edit_state_reward_block_matches_the_inventory_endpoint(fake_db):
    """One rule, two screens: the detail/edit view and the create wizard must
    never disagree about whether inventory covers the winner target."""
    _pool("MISSION-OK", available=5)
    _mission("m1", mission_pool={"pool_id": "MISSION-OK", "winner_count": 10})
    with _app().test_client() as client, _admin_ok():
        state = client.get("/api/admin/mission-pool/m1/edit-state").get_json()
        direct = client.get("/api/admin/mission-pool/inventory-check?pool_id=MISSION-OK&winner_count=10").get_json()
    assert state["reward"]["sufficient"] is False
    assert state["reward"]["available"] == direct["available"]
    assert state["reward"]["sufficient"] == direct["sufficient"]
    assert state["reward"]["shortfall"] == direct["shortfall"]


def test_edit_state_keeps_a_stored_pool_visible_when_it_is_no_longer_selectable(fake_db):
    """§18: the admin UI must never rewrite a stored pool just because the
    current listing no longer offers it."""
    _pool("MOVED", available=7, scope="affiliate_rewards")
    _mission("m1", mission_pool={"pool_id": "MOVED", "winner_count": 2})
    with _app().test_client() as client, _admin_ok():
        state = client.get("/api/admin/mission-pool/m1/edit-state").get_json()
        pools = client.get("/api/admin/mission-pool/pools").get_json()
    assert state["reward"]["pool_id"] == "MOVED"
    assert state["reward"]["pool_selectable"] is False
    assert "MOVED" not in {p["pool_id"] for p in pools["pools"]}


def test_edit_state_locks_the_pool_once_allocation_has_started(fake_db):
    _pool("MISSION-OK", available=5)
    _mission("m1", status="ended",
             mission_pool={"pool_id": "MISSION-OK", "winner_count": 2,
                           "processing_stage": mp.STAGE_ALLOCATING_REWARDS})
    with _app().test_client() as client, _admin_ok():
        state = client.get("/api/admin/mission-pool/m1/edit-state").get_json()
    assert state["reward"]["allocation_started"] is True
    assert state["reward"]["pool_editable"] is False


def test_edit_state_locks_the_pool_when_a_mission_reward_row_exists(fake_db):
    """A reward row is proof a winner is already being paid from this pool,
    even if the stage field has not advanced yet."""
    _pool("MISSION-OK", available=5)
    _mission("m1", mission_pool={"pool_id": "MISSION-OK", "winner_count": 2})
    database.db["campaign_rewards"].insert_one({
        "reward_id": "r1", "campaign_id": "m1", "category": "mission_pool", "status": "assigned",
    })
    with _app().test_client() as client, _admin_ok():
        state = client.get("/api/admin/mission-pool/m1/edit-state").get_json()
    assert state["reward"]["allocation_started"] is True
    assert state["reward"]["pool_editable"] is False


def test_edit_state_leaves_the_pool_editable_before_allocation(fake_db):
    _pool("MISSION-OK", available=5)
    _mission("m1", mission_pool={"pool_id": "MISSION-OK", "winner_count": 2})
    with _app().test_client() as client, _admin_ok():
        state = client.get("/api/admin/mission-pool/m1/edit-state").get_json()
    assert state["reward"]["allocation_started"] is False
    assert state["reward"]["pool_editable"] is True
    assert state["state"] == ux.OPS_LIVE


def test_edit_state_still_reports_the_phase_1_freeze_independently(fake_db):
    """The new reward block is additive: the §26 freeze and the §27 schedule
    answer are unchanged."""
    _pool("MISSION-OK", available=5)
    _mission("m1", mission_pool={"pool_id": "MISSION-OK", "winner_count": 2})
    _entry("m1", UID, mp.ENTRY_STATUS_SUBMITTED)
    with _app().test_client() as client, _admin_ok():
        state = client.get("/api/admin/mission-pool/m1/edit-state").get_json()
    assert state["mission_config_locked"] is True
    assert set(state["locked_fields"]) == set(ux.MISSION_CONFIG_FIELDS)
    assert state["schedule_editable"] is True


# ---------------------------------------------------------------------------
# Pools endpoint (§6, §7)
# ---------------------------------------------------------------------------

def test_pools_endpoint_serves_the_backend_pool_type_vocabulary(fake_db):
    """The inline "create a new reward pool" form offers exactly what
    voucher_pool_service.register_pool accepts, rather than a list hardcoded
    in the admin UI."""
    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/pools").get_json()
    assert data["pool_types"] == list(vps.POOL_TYPES)


def test_pools_endpoint_reports_stock_per_pool(fake_db):
    _pool("MISSION-OK", available=54, issued=6)
    _pool("SHARED-OK", available=210, scope="shared")
    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/pools").get_json()
    by_id = {p["pool_id"]: p for p in data["pools"]}
    assert by_id["MISSION-OK"]["stock"] == {"available": 54, "issued": 6}
    assert by_id["SHARED-OK"]["stock"]["available"] == 210


# ---------------------------------------------------------------------------
# pool_stock_bulk (shared Voucher Centre inventory helper)
# ---------------------------------------------------------------------------

def test_pool_stock_bulk_matches_pool_stock_for_every_pool(fake_db):
    _pool("A", available=3, issued=1)
    _pool("B", available=0, issued=2)
    bulk = vps.pool_stock_bulk(["A", "B", "MISSING"])
    assert bulk["A"] == vps.pool_stock("A")
    assert bulk["B"] == vps.pool_stock("B")
    # A pool with no rows is reported as zero, never omitted — the caller
    # renders "0 available", not a blank.
    assert bulk["MISSING"] == {"available": 0, "issued": 0}


def test_pool_stock_bulk_ignores_rows_this_module_does_not_own(fake_db):
    """A legacy affiliate/welcome row that happens to share a pool_id must
    never inflate a Campaign Rewards stock report."""
    _pool("A", available=2)
    database.db["voucher_pools"].insert_one({
        "pool_id": "A", "code": "LEGACY-1", "status": "available",
    })
    assert vps.pool_stock_bulk(["A"])["A"]["available"] == 2


def test_pool_stock_bulk_handles_an_empty_request(fake_db):
    assert vps.pool_stock_bulk([]) == {}
    assert vps.pool_stock_bulk(None) == {}
