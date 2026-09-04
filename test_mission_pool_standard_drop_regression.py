"""P0 regression suite (spec §36/§51): adding Mission Pool must not change
any existing Standard Drop / Campaign Centre behaviour.

Scoped deliberately to the code Mission Pool actually touched — campaign_centre
(campaign types, validation, publish gate, public listing), campaign_rewards_api
(shared response shape), voucher_pool_service (shared inventory), fake_mongo
(shared test double) — plus the module-level assertion that vouchers.py, which
owns Standard Drop, is not imported or modified by the new code.
"""

import ast
import inspect
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import campaign_providers as cp
import campaign_rewards_api as cra
import database
import mission_pool as mp
import voucher_pool_service as vps
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb({
        "gc_campaigns": [("campaign_id",)],
        "voucher_pools": [("pool_id", "code")],
        "gc_providers": [("provider_id",)],
    })
    monkeypatch.setattr(database, "db", fdb)
    for module in (cc, cp, cra, mp, vps):
        monkeypatch.setattr(module, "database", database)
    return fdb


def _admin():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "gracy_ap"}, None))


def _verified(uid):
    return patch("vouchers.verify_telegram_init_data",
                 return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"))


def _cc_app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    app.register_blueprint(cc.campaign_public_bp)
    return app


# ---------------------------------------------------------------------------
# Standard Drop code is not touched at all
# ---------------------------------------------------------------------------

def test_mission_modules_never_import_the_standard_drop_claim_engine():
    """Mission Pool may reuse `vouchers.require_admin` and
    `vouchers.verify_telegram_init_data` (auth), but must never call into the
    drop/claim engine — no `claim_pooled`, `claim_personalised`,
    `user_visible_drops`, `assign_public_pool_access_once` or `db.drops`."""
    import mission_pool_processor as mpp

    def code_only(module) -> str:
        """Docstrings explain what Mission Pool deliberately does NOT touch,
        so they legitimately name Standard Drop internals. Strip them and
        scan the executable code."""
        tree = ast.parse(inspect.getsource(module))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                body = node.body
                if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                        and isinstance(body[0].value.value, str):
                    node.body = body[1:] or [ast.Pass()]
        return ast.unparse(tree)

    forbidden = (
        "claim_pooled", "claim_personalised", "claim_voucher_for_user",
        "user_visible_drops", "assign_public_pool_access_once",
        "reconcile_pooled_remaining", "_atomic_claim_pooled_voucher",
        "db.drops", "db.vouchers", '["drops"]', '["vouchers"]',
    )
    for module in (mp, mpp):
        source = code_only(module)
        for name in forbidden:
            assert name not in source, f"{module.__name__} references Standard Drop internal {name!r}"


def test_standard_drop_source_files_are_untouched_by_this_change():
    """vouchers.py owns every Standard Drop route, the pooled/personalised
    inventory and the anti-abuse claim gate. It must contain no Mission Pool
    reference at all."""
    source = open("vouchers.py").read()
    assert "mission_pool" not in source
    assert "mission_entries" not in source


# ---------------------------------------------------------------------------
# §36 mechanic-free legacy documents keep working
# ---------------------------------------------------------------------------

def test_legacy_campaigns_without_mechanic_still_list_as_active(fake_db):
    """An existing live campaign document has no `mechanic` field; the public
    listing must still return it."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "legacy-tournament", "name": "Legacy", "type": "tournament",
        "status": "live", "priority": 100,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "destination": {"provider_id": "p1", "open_mode": "telegram_web_app", "ready": True},
        "telegram": {"require_identity": True, "require_subscription": True, "channel_username": "c"},
    })
    fake_db["gc_providers"].insert_one({"provider_id": "p1", "active": True, "type": "tournament"})

    with _cc_app().test_client() as client:
        body = client.get("/api/campaigns/active").get_json()

    assert body["status"] == "ok"
    assert [c["campaign_id"] for c in body["campaigns"]] == ["legacy-tournament"]


def test_mission_campaigns_never_appear_in_the_standard_active_listing(fake_db):
    """The Mini App's existing /api/campaigns/active payload must be exactly
    what it was before Mission Pool existed."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "mission-x", "name": "Mission", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "live", "priority": 500,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "ready": True},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {"pool_id": "P", "winner_count": 1},
    })
    with _cc_app().test_client() as client:
        body = client.get("/api/campaigns/active").get_json()
    assert body["campaigns"] == []


# ---------------------------------------------------------------------------
# Campaign Centre admin CRUD is unchanged for existing types
# ---------------------------------------------------------------------------

def test_existing_campaign_types_still_validate_and_publish(fake_db):
    fake_db["gc_providers"].insert_one({"provider_id": "p1", "active": True, "type": "tournament"})
    now = datetime.now(timezone.utc)
    payload = {
        "campaign_id": "t-1", "name": "Tourney", "type": "tournament",
        "schedule": {"starts_at": now.isoformat(), "ends_at": (now + timedelta(days=1)).isoformat()},
        "destination": {"provider_id": "p1", "open_mode": "external_url", "ready": True},
        "reward_config": {"rules": [{"rule_id": "r1", "condition_type": "rank",
                                      "params": {"min_rank": 1, "max_rank": 1},
                                      "pool_id": "gold"}]},
    }
    with _cc_app().test_client() as client, _admin():
        created = client.post("/api/admin/gc-campaigns", json=payload)
        assert created.status_code == 201
        published = client.post("/api/admin/gc-campaigns/t-1/publish")
        assert published.status_code == 200, published.get_json()

    doc = fake_db["gc_campaigns"].find_one({"campaign_id": "t-1"})
    assert doc["status"] == "live"
    # A non-mission campaign is stamped standard_drop and carries no mission
    # fields at all.
    assert doc["mechanic"] == "standard_drop"
    assert "mission_config" not in doc
    assert "mission_pool" not in doc


def test_tournament_publish_gate_still_requires_reward_rules(fake_db):
    fake_db["gc_providers"].insert_one({"provider_id": "p1", "active": True, "type": "tournament"})
    now = datetime.now(timezone.utc)
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json={
            "campaign_id": "t-2", "name": "T", "type": "tournament",
            "schedule": {"starts_at": now.isoformat()},
            "destination": {"provider_id": "p1", "open_mode": "external_url", "ready": True},
        })
        resp = client.post("/api/admin/gc-campaigns/t-2/publish")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "reward_rules_required"


def test_tournament_publish_gate_still_requires_a_ready_destination(fake_db):
    fake_db["gc_providers"].insert_one({"provider_id": "p1", "active": True, "type": "tournament"})
    now = datetime.now(timezone.utc)
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json={
            "campaign_id": "t-3", "name": "T", "type": "tournament",
            "schedule": {"starts_at": now.isoformat()},
            "destination": {"provider_id": "p1", "open_mode": "external_url", "ready": False},
            "reward_config": {"rules": [{"rule_id": "r1", "condition_type": "participation",
                                          "params": {}, "pool_id": "gold"}]},
        })
        resp = client.post("/api/admin/gc-campaigns/t-3/publish")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "destination_not_ready"


def test_mission_config_cannot_be_attached_to_a_non_mission_campaign(fake_db):
    now = datetime.now(timezone.utc)
    with _cc_app().test_client() as client, _admin():
        resp = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": "t-4", "name": "T", "type": "external_website",
            "schedule": {"starts_at": now.isoformat()},
            "destination": {"open_mode": "external_url", "ready": True},
            "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        })
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "mission_config_not_allowed_for_type"


def test_mission_campaign_create_and_publish_gate(fake_db):
    now = datetime.now(timezone.utc)
    body = {
        "campaign_id": "m-1", "name": "Mission", "type": "mission_pool",
        "schedule": {"starts_at": now.isoformat(), "ends_at": (now + timedelta(days=1)).isoformat()},
        "mission_config": {"mission_type": "multiple_choice", "prompt": "Pick",
                            "options": [{"id": "a"}, {"id": "b"}], "correct_answer": "a"},
        "mission_pool": {"pool_id": "MP", "winner_count": 10},
    }
    with _cc_app().test_client() as client, _admin():
        assert client.post("/api/admin/gc-campaigns", json=body).status_code == 201
        assert client.post("/api/admin/gc-campaigns/m-1/publish").status_code == 200

    doc = fake_db["gc_campaigns"].find_one({"campaign_id": "m-1"})
    assert doc["mechanic"] == "mission_pool"
    assert doc["status"] == "live"
    assert doc["mission_pool"]["winner_count"] == 10
    assert doc["mission_pool"]["processing_stage"] == mp.STAGE_PENDING
    assert doc["mission_pool"]["processing_generation"] == 0


def test_mission_campaign_cannot_publish_without_a_pool(fake_db):
    now = datetime.now(timezone.utc)
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json={
            "campaign_id": "m-2", "name": "M", "type": "mission_pool",
            "schedule": {"starts_at": now.isoformat()},
            "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
            "mission_pool": {"pool_id": "MP", "winner_count": 1},
        })
        # Strip the pool config the way a bad migration might.
        fake_db["gc_campaigns"].update_one({"campaign_id": "m-2"}, {"$set": {"mission_pool": {}}})
        resp = client.post("/api/admin/gc-campaigns/m-2/publish")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "mission_pool_config_required"


# ---------------------------------------------------------------------------
# Campaign Rewards shared response shape (§27.4)
# ---------------------------------------------------------------------------

_LEGACY_REWARD_KEYS = {
    "reward_id", "category", "campaign_id", "campaign_name", "tournament_id",
    "rank", "reward_label", "voucher_code", "assigned_at", "expires_at", "status",
}


def test_tournament_reward_response_keeps_every_existing_field(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["campaign_rewards"].insert_one({
        "reward_id": "rw_1", "category": "tournament", "campaign_id": "c1",
        "tournament_id": "t1", "telegram_user_id": 111, "rank": 1,
        "reward_label": "Champion", "voucher_code": "ABC123", "status": "assigned",
        "assigned_at": now, "first_viewed_at": None, "copied_at": None,
    })
    app = Flask(__name__)
    app.register_blueprint(cra.campaign_rewards_bp)
    with app.test_client() as client, _verified(111):
        body = client.get("/api/campaign-rewards/me?init_data=stub").get_json()

    reward = body["rewards"][0]
    assert _LEGACY_REWARD_KEYS.issubset(reward.keys())
    assert reward["voucher_code"] == "ABC123"
    assert reward["rank"] == 1
    assert reward["category"] == "tournament"
    # Mission-only keys are absent for a tournament reward.
    assert "is_winner" not in reward
    assert "winner_popup_pending" not in reward
    # Additive mechanic marker resolves to standard_drop for legacy rows.
    assert reward["mechanic"] == "standard_drop"


def test_mission_reward_is_exposed_through_the_same_endpoint(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "m-1", "name": "Pilot Mission", "type": "mission_pool",
        "mechanic": "mission_pool",
    })
    fake_db["campaign_rewards"].insert_one({
        "reward_id": "rw_mp_1", "category": "mission_pool", "campaign_id": "m-1",
        "telegram_user_id": 222, "reward_label": "Pilot Mission",
        "voucher_code": "MP123", "status": "assigned", "assigned_at": now,
        "notification_status": "sent", "winner_popup_pending": True,
    })
    app = Flask(__name__)
    app.register_blueprint(cra.campaign_rewards_bp)
    with app.test_client() as client, _verified(222):
        body = client.get("/api/campaign-rewards/me?init_data=stub").get_json()

    reward = body["rewards"][0]
    assert _LEGACY_REWARD_KEYS.issubset(reward.keys())
    assert reward["voucher_code"] == "MP123"
    assert reward["campaign_name"] == "Pilot Mission"
    assert reward["mechanic"] == "mission_pool"
    assert reward["is_winner"] is True
    assert reward["winner_popup_pending"] is True


def test_winner_popup_is_shown_once_then_acknowledged(fake_db):
    """§27.3: the congratulation popup must not re-appear on every open."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({"campaign_id": "m-1", "name": "M",
                                         "type": "mission_pool", "mechanic": "mission_pool"})
    fake_db["campaign_rewards"].insert_one({
        "reward_id": "rw_mp_2", "category": "mission_pool", "campaign_id": "m-1",
        "telegram_user_id": 333, "voucher_code": "MP999", "status": "assigned",
        "assigned_at": now, "winner_popup_pending": True,
    })
    app = Flask(__name__)
    app.register_blueprint(cra.campaign_rewards_bp)

    with app.test_client() as client, _verified(333):
        first = client.get("/api/campaign-rewards/me?init_data=stub").get_json()["rewards"][0]
        assert first["winner_popup_pending"] is True
        assert client.post("/api/campaign-rewards/rw_mp_2/ack-popup?init_data=stub").status_code == 200
        second = client.get("/api/campaign-rewards/me?init_data=stub").get_json()["rewards"][0]

    assert second["winner_popup_pending"] is False
    # Acknowledging is presentational only — the voucher is untouched.
    assert second["voucher_code"] == "MP999"
    assert second["status"] == "assigned"


def test_popup_ack_requires_ownership(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["campaign_rewards"].insert_one({
        "reward_id": "rw_mp_3", "category": "mission_pool", "campaign_id": "m-1",
        "telegram_user_id": 444, "voucher_code": "X", "status": "assigned",
        "assigned_at": now, "winner_popup_pending": True,
    })
    app = Flask(__name__)
    app.register_blueprint(cra.campaign_rewards_bp)
    with app.test_client() as client, _verified(999):
        resp = client.post("/api/campaign-rewards/rw_mp_3/ack-popup?init_data=stub")
    assert resp.status_code == 404
    assert fake_db["campaign_rewards"].find_one({"reward_id": "rw_mp_3"})["winner_popup_pending"] is True


# ---------------------------------------------------------------------------
# Shared voucher inventory is not redefined
# ---------------------------------------------------------------------------

def test_mission_allocation_uses_the_existing_campaign_rewards_scope(fake_db):
    """Mission Pool draws from the same `db.voucher_pools` rows Campaign
    Centre already allocates from, under the same allocation_scope control —
    no new inventory collection and no widened scope."""
    fake_db["voucher_pools"].insert_one({
        "pool_id": "MP", "code": "C1", "status": "available", "pool_source": "campaign_centre",
        "pool_type": "voucher_drop", "allocation_scope": "campaign_rewards",
    })
    # An affiliate-scoped row in the same collection must stay untouchable.
    fake_db["voucher_pools"].insert_one({
        "pool_id": "MP", "code": "C2", "status": "available", "pool_source": "campaign_centre",
        "pool_type": "voucher_drop", "allocation_scope": "affiliate_rewards",
    })

    claimed = vps.allocate_voucher("MP", reward_id="rw_mp_x", telegram_user_id=1,
                                   expected_pool_type="voucher_drop")
    assert claimed["code"] == "C1"
    second = vps.allocate_voucher("MP", reward_id="rw_mp_y", telegram_user_id=2,
                                  expected_pool_type="voucher_drop")
    assert second is None
    assert fake_db["voucher_pools"].find_one({"code": "C2"})["status"] == "available"


def test_reserved_legacy_pool_ids_remain_refused():
    for pool_id in ("WELCOME", "T1", "T5"):
        with pytest.raises(vps.ReservedPoolIdError):
            vps.register_pool(pool_id, name="x")


# ---------------------------------------------------------------------------
# Codex review follow-ups
# ---------------------------------------------------------------------------

def _mission_body(campaign_id="m-dup", **overrides):
    now = datetime.now(timezone.utc)
    body = {
        "campaign_id": campaign_id, "name": "Mission", "type": "mission_pool",
        "schedule": {"starts_at": now.isoformat(),
                     "ends_at": (now + timedelta(days=1)).isoformat()},
        "mission_config": {"mission_type": "multiple_choice", "prompt": "Pick",
                            "options": [{"id": "a"}, {"id": "b"}], "correct_answer": "a"},
        "mission_pool": {"pool_id": "MP", "winner_count": 10},
    }
    body.update(overrides)
    return body


def test_duplicating_a_finished_mission_resets_all_processing_state(fake_db):
    """Copying the mission_pool block verbatim would hand the new draft the
    source campaign's processing_stage/seed/generation — after which
    find_due_campaigns would skip it forever and none of its entries would
    ever be rewarded."""
    with _cc_app().test_client() as client, _admin():
        assert client.post("/api/admin/gc-campaigns", json=_mission_body()).status_code == 201

        # Simulate a campaign that has already run to completion.
        fake_db["gc_campaigns"].update_one({"campaign_id": "m-dup"}, {"$set": {
            "mission_pool.processing_stage": mp.STAGE_COMPLETED,
            "mission_pool.processing_generation": 7,
            "mission_pool.processing_owner": "worker-1",
            "mission_pool.processing_lease_expires_at": datetime.now(timezone.utc),
            "mission_pool.selection_seed": "deadbeef",
            "mission_pool.qualified_count": 900,
            "mission_pool.winner_count_actual": 300,
            "mission_pool.cancelled": True,
            "mission_pool.closed_at": datetime.now(timezone.utc),
        }})

        resp = client.post("/api/admin/gc-campaigns/m-dup/duplicate",
                           json={"campaign_id": "m-dup-copy"})
    assert resp.status_code == 201

    copy = fake_db["gc_campaigns"].find_one({"campaign_id": "m-dup-copy"})["mission_pool"]
    # Operator configuration is carried over...
    assert copy["pool_id"] == "MP"
    assert copy["winner_count"] == 10
    assert copy["allocation_method"] == "random_qualified"
    # ...and every worker-owned field is reset.
    assert copy["processing_stage"] == mp.STAGE_PENDING
    assert copy["processing_generation"] == 0
    assert copy["processing_owner"] is None
    assert copy["processing_lease_expires_at"] is None
    assert copy["selection_seed"] is None
    assert copy["qualified_count"] is None
    assert copy["winner_count_actual"] is None
    assert copy["cancelled"] is False
    assert copy["closed_at"] is None
    # The source campaign is untouched.
    src = fake_db["gc_campaigns"].find_one({"campaign_id": "m-dup"})["mission_pool"]
    assert src["processing_stage"] == mp.STAGE_COMPLETED
    assert src["selection_seed"] == "deadbeef"


def test_duplicating_a_tournament_campaign_is_unchanged(fake_db):
    """The reset is scoped to mission campaigns; tournament duplication keeps
    behaving exactly as before."""
    fake_db["gc_providers"].insert_one({"provider_id": "p1", "active": True, "type": "tournament"})
    now = datetime.now(timezone.utc)
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json={
            "campaign_id": "t-dup", "name": "T", "type": "tournament",
            "schedule": {"starts_at": now.isoformat()},
            "destination": {"provider_id": "p1", "open_mode": "external_url", "ready": True},
            "reward_config": {"rules": [{"rule_id": "r1", "condition_type": "participation",
                                          "params": {}, "pool_id": "gold"}]},
        })
        resp = client.post("/api/admin/gc-campaigns/t-dup/duplicate",
                           json={"campaign_id": "t-dup-copy"})
    assert resp.status_code == 201
    copy = fake_db["gc_campaigns"].find_one({"campaign_id": "t-dup-copy"})
    assert copy["status"] == "draft"
    assert copy["destination"]["ready"] is False
    assert copy["reward_config"]["rules"][0]["rule_id"] == "r1"
    assert "mission_pool" not in copy


def test_mission_config_is_frozen_once_entries_exist(fake_db):
    """Answers are graded at submission time and never regraded, so editing
    the correct answer mid-campaign would grade identical answers differently
    by arrival time."""
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json=_mission_body("m-freeze"))
        fake_db[mp.ENTRIES_COLLECTION].insert_one({
            "campaign_id": "m-freeze", "telegram_user_id": 1, "answer_normalized": "a",
            "is_correct": True, "status": mp.ENTRY_STATUS_SUBMITTED,
        })
        changed = dict(_mission_body("m-freeze"))
        changed["mission_config"] = {"mission_type": "multiple_choice", "prompt": "Pick",
                                      "options": [{"id": "a"}, {"id": "b"}],
                                      "correct_answer": "b"}
        resp = client.put("/api/admin/gc-campaigns/m-freeze", json=changed)

    assert resp.status_code == 409
    assert resp.get_json()["code"] == "mission_config_locked"
    stored = fake_db["gc_campaigns"].find_one({"campaign_id": "m-freeze"})
    assert stored["mission_config"]["correct_answer"] == "a"


def test_mission_config_edits_are_allowed_before_any_submission(fake_db):
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json=_mission_body("m-open"))
        changed = dict(_mission_body("m-open"))
        changed["mission_config"] = {"mission_type": "multiple_choice", "prompt": "Pick",
                                      "options": [{"id": "a"}, {"id": "b"}],
                                      "correct_answer": "b"}
        resp = client.put("/api/admin/gc-campaigns/m-open", json=changed)
    assert resp.status_code == 200
    assert fake_db["gc_campaigns"].find_one(
        {"campaign_id": "m-open"})["mission_config"]["correct_answer"] == "b"


def test_unchanged_mission_config_resubmission_is_still_allowed(fake_db):
    """Admin UIs routinely PUT the whole document back; an identical config
    must not be treated as an edit."""
    with _cc_app().test_client() as client, _admin():
        client.post("/api/admin/gc-campaigns", json=_mission_body("m-noop"))
        fake_db[mp.ENTRIES_COLLECTION].insert_one({
            "campaign_id": "m-noop", "telegram_user_id": 1, "status": mp.ENTRY_STATUS_SUBMITTED,
        })
        body = dict(_mission_body("m-noop"))
        body["mission_pool"] = {"pool_id": "MP", "winner_count": 25}
        resp = client.put("/api/admin/gc-campaigns/m-noop", json=body)
    assert resp.status_code == 200
    assert fake_db["gc_campaigns"].find_one(
        {"campaign_id": "m-noop"})["mission_pool"]["winner_count"] == 25
