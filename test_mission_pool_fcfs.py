"""Tests for the live FCFS ("first_qualified") capacity mechanic.

Phase 2 on top of the existing Mission Pool engine (mission_pool.py /
mission_pool_processor.py, both covered by test_mission_pool*.py's 379+
tests, which stay green and unmodified). This file covers ONLY the new
behaviour:

  * arm_fcfs_campaign: one-time activation, capacity = min(winner_count,
    usable inventory), never touches an in-flight FCFS mission.
  * try_consume_fcfs_slot: the bounded, indexed reduced-eligibility check +
    atomic slot claim that runs inline in the submission hot path for an
    ARMED campaign only.
  * The Mongo concurrency invariant (single-document guarded $inc, no
    count -> compare -> update race) and the guarded compare-and-set close.
  * Scheduler recovery (_fcfs_maintenance) for a crash between the winning
    $inc and its own close, and for a campaign armed before this code ever
    shipped.
  * Zero behavioural change for random_qualified (Random Pool) and for a
    first_qualified mission that already had submissions before this
    mechanic existed.
"""

import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
import mission_pool_processor as mpp
import voucher_pool_service as vps
from fake_mongo import FakeDb

FCFS_CAMPAIGN_ID = "mission-fcfs-1"
FCFS_POOL_ID = "MISSION-FCFS"


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
        "voucher_pools": [("pool_id", "code")],
        "campaign_rewards": [
            ("reward_id",),
            (("campaign_id", "mission_entry_id"), {"category": "mission_pool"}),
            (("campaign_id", "identity_key"), {"category": "mission_pool"}),
        ],
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys())
    monkeypatch.setattr(database, "db", fdb)
    for module in (mp, mpp, vps, cc):
        monkeypatch.setattr(module, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    monkeypatch.setattr(mpp.mp, "mission_pool_enabled", lambda: True)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(mp.mission_pool_bp)
    app.register_blueprint(mp.mission_pool_admin_bp)
    return app


def _verified(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _stub_initdata():
    def verify(raw):
        return True, {"user": '{"id": %s}' % str(raw).split(":", 1)[-1]}, "ok"

    return patch("vouchers.verify_telegram_init_data", side_effect=verify)


def _run_concurrently(fn, n):
    barrier = threading.Barrier(n)
    results = [None] * n
    errors = []

    def worker(idx):
        try:
            barrier.wait(timeout=10)
            results[idx] = fn(idx)
        except Exception as exc:  # noqa: BLE001 - surfaced by the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, f"worker raised: {errors}"
    return results


def _submit(client, uid, answer="x", campaign_id=FCFS_CAMPAIGN_ID):
    with _verified(uid):
        return client.post(
            f"/api/mission-pool/{campaign_id}/submit?init_data=stub",
            json={"answer": answer},
        )


def _seed_pool(fake_db, count, pool_id=FCFS_POOL_ID):
    fake_db["voucher_pool_registry"].insert_one({
        "pool_id": pool_id, "name": pool_id, "pool_type": "voucher_drop",
        "allocation_scope": "campaign_rewards", "status": "active",
    })
    for i in range(count):
        fake_db["voucher_pools"].insert_one({
            "pool_id": pool_id, "code": f"{pool_id}-{i:04d}", "status": "available",
            "issued_to": None, "issued_at": None,
            "pool_source": "campaign_centre", "pool_type": "voucher_drop",
            "allocation_scope": "campaign_rewards",
        })


def _seed_fcfs(fake_db, *, capacity=None, claimed=0, armed=True, winner_count=3,
                status="live", ends_at=None, closed_at=None, pool_available=0,
                campaign_id=FCFS_CAMPAIGN_ID, pool_id=FCFS_POOL_ID):
    now = datetime.now(timezone.utc)
    if pool_available:
        _seed_pool(fake_db, pool_available, pool_id=pool_id)
    doc = {
        "campaign_id": campaign_id,
        "name": "FCFS Mission",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": status,
        "schedule": {"starts_at": now - timedelta(hours=1),
                     "ends_at": ends_at if ends_at is not None else now + timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": pool_id,
            "pool_type": "voucher_drop",
            "winner_count": winner_count,
            "allocation_method": mp.ALLOCATION_FIRST_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "closed_at": closed_at,
            "processing_stage": mp.STAGE_PENDING,
            "processing_generation": 0,
            "fcfs_armed": armed,
            "fcfs_capacity": capacity,
            "fcfs_claimed": claimed,
            "fcfs_armed_at": now if armed else None,
        },
    }
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _submitted_entry(fake_db, uid, now, is_correct=True, campaign_id=FCFS_CAMPAIGN_ID):
    entry_id = fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": campaign_id, "telegram_user_id": uid, "answer": "x",
        "answer_normalized": "x", "is_correct": is_correct, "status": mp.ENTRY_STATUS_SUBMITTED,
        "identity_key": None, "identity_type": None, "disqualification_reason": None,
        "reward_id": None, "submitted_at": now, "created_at": now, "updated_at": now,
    }).inserted_id
    return {"campaign_id": campaign_id, "telegram_user_id": uid, "is_correct": is_correct, "_id": entry_id}


# ---------------------------------------------------------------------------
# arm_fcfs_campaign
# ---------------------------------------------------------------------------

def test_capacity_is_min_of_configured_and_usable_inventory(fake_db):
    """configured 300 / usable 297 -> capacity 297."""
    _seed_fcfs(fake_db, capacity=None, armed=False, winner_count=300, pool_available=297)
    result = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)
    assert result == {"armed": True, "capacity": 297}
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_capacity"] == 297
    assert block["fcfs_claimed"] == 0
    assert block["fcfs_armed"] is True


def test_arm_initializes_missing_counters_explicitly(fake_db):
    """Missing-counter-initialization regression: a document written before
    this mechanic existed has no fcfs_* keys at all. Arming must explicitly
    stamp fcfs_claimed=0 rather than relying on any default that could be
    absent when `$lt`/`$inc` first touch it."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": FCFS_CAMPAIGN_ID, "name": "Legacy FCFS", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": FCFS_POOL_ID, "pool_type": "voucher_drop", "winner_count": 5,
            "allocation_method": mp.ALLOCATION_FIRST_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    _seed_pool(fake_db, 5)
    result = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)
    assert result["armed"] is True
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_claimed"] == 0
    assert block["fcfs_capacity"] == 5
    assert block["fcfs_armed"] is True


def test_existing_in_flight_mission_is_not_auto_armed(fake_db):
    """Decision 1: any FCFS mission with an existing mission_entries row —
    i.e. one already in flight before this mechanic existed — must never be
    armed, by this call or by the scheduler's own recovery pass."""
    _seed_fcfs(fake_db, capacity=None, armed=False, winner_count=5, pool_available=5)
    now = datetime.now(timezone.utc)
    fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": FCFS_CAMPAIGN_ID, "telegram_user_id": 1, "answer": "x",
        "answer_normalized": "x", "is_correct": True, "status": mp.ENTRY_STATUS_SUBMITTED,
        "identity_key": None, "identity_type": None, "disqualification_reason": None,
        "reward_id": None, "submitted_at": now, "created_at": now, "updated_at": now,
    })

    result = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)
    assert result == {"armed": False, "reason": "existing_entries"}
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert not block.get("fcfs_armed")

    # The scheduler's own backfill pass must respect the same guard.
    mpp._fcfs_maintenance(now)
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert not block.get("fcfs_armed")


def test_arm_is_idempotent_and_never_rearms(fake_db):
    _seed_fcfs(fake_db, capacity=None, armed=False, winner_count=5, pool_available=5)
    first = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)
    assert first["armed"] is True
    # Inventory changes after arming must never retroactively change capacity.
    _seed_pool(fake_db, 100, pool_id="OTHER-POOL")
    fake_db["voucher_pools"].insert_one({
        "pool_id": FCFS_POOL_ID, "code": "EXTRA", "status": "available",
        "issued_to": None, "issued_at": None, "pool_source": "campaign_centre",
        "pool_type": "voucher_drop", "allocation_scope": "campaign_rewards",
    })
    second = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)
    assert second == {"armed": False, "reason": "already_armed"}
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_capacity"] == 5


def test_admin_put_cannot_reset_fcfs_counters(fake_db):
    doc = _seed_fcfs(fake_db, capacity=10, claimed=4, armed=True)
    existing_block = doc["mission_pool"]
    validated, err = mp.validate_mission_pool_config({
        "pool_id": FCFS_POOL_ID, "winner_count": 999, "allocation_method": "first_qualified",
    })
    assert err is None
    merged = mp.merge_mission_pool_config(existing_block, validated)
    assert merged["fcfs_armed"] is True
    assert merged["fcfs_capacity"] == 10
    assert merged["fcfs_claimed"] == 4


def test_publish_transition_arms_a_fresh_fcfs_campaign(fake_db):
    """End-to-end: campaign_centre's publish action (draft -> live) is the
    real-world trigger for arm_fcfs_campaign on a brand-new mission."""
    _seed_pool(fake_db, 4)
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": FCFS_CAMPAIGN_ID, "name": "Fresh FCFS", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "draft",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": FCFS_POOL_ID, "pool_type": "voucher_drop", "winner_count": 4,
            "allocation_method": mp.ALLOCATION_FIRST_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    admin_app = Flask(__name__)
    admin_app.register_blueprint(cc.campaign_centre_bp)
    with patch("vouchers.require_admin", return_value=({"id": 1}, None)):
        resp = admin_app.test_client().post(f"/api/admin/gc-campaigns/{FCFS_CAMPAIGN_ID}/publish")
    assert resp.status_code == 200
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_armed"] is True
    assert block["fcfs_capacity"] == 4
    assert block["fcfs_claimed"] == 0


def test_arm_catches_up_an_entry_that_raced_the_cas(fake_db):
    """Codex P1: arm_fcfs_campaign's zero-entries guard and its CAS are two
    separate writes. A submission whose own fresh read lands in that gap can
    still insert an entry via the old (un-armed) path. arm_fcfs_campaign must
    feed any such straggler through try_consume_fcfs_slot itself rather than
    leaving it to be picked up later by _eligibility_pass, which would let it
    re-enter _select_winners alongside already-decided winners and risk
    exceeding fcfs_capacity."""
    _seed_fcfs(fake_db, capacity=None, armed=False, winner_count=2, pool_available=2)
    now = datetime.now(timezone.utc)
    straggler_uid = 42424242
    fake_db["users"].insert_one({"user_id": straggler_uid})
    # Simulates an entry inserted via the old path between arm's zero-entries
    # check and its CAS -- from arm's point of view this straggler exists
    # the whole time (the count check would see it), so to reproduce the
    # actual race we insert it, then call arm, and assert it is NOT left
    # behind at `submitted` once arming completes.
    fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": FCFS_CAMPAIGN_ID, "telegram_user_id": straggler_uid, "answer": "x",
        "answer_normalized": "x", "is_correct": True, "status": mp.ENTRY_STATUS_SUBMITTED,
        "identity_key": None, "identity_type": None, "disqualification_reason": None,
        "reward_id": None, "submitted_at": now, "created_at": now, "updated_at": now,
    })

    # arm_fcfs_campaign's own existing-entries guard would normally refuse to
    # arm at all here (this is exactly Decision 1's protection). Exercise the
    # catch-up mechanism directly, the way it runs right after a real CAS.
    result = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)
    assert result == {"armed": False, "reason": "existing_entries"}

    mpp._campaigns().update_one(
        {"campaign_id": FCFS_CAMPAIGN_ID},
        {"$set": {"mission_pool.fcfs_armed": True, "mission_pool.fcfs_capacity": 2,
                   "mission_pool.fcfs_claimed": 0, "mission_pool.fcfs_armed_at": now}},
    )
    mpp._catch_up_fcfs_race(FCFS_CAMPAIGN_ID, dict(mp.DEFAULT_ELIGIBILITY_POLICY), 2, now)

    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"telegram_user_id": straggler_uid})
    assert entry["status"] == mp.ENTRY_STATUS_WINNER
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["mission_pool"]["fcfs_claimed"] == 1


def test_publish_time_arm_catches_up_a_racing_submission(fake_db):
    """End-to-end version of the same race, through the real publish path:
    an entry that lands between the zero-entries guard and the arming CAS
    must come out of arm_fcfs_campaign already decided (winner or excluded),
    never sitting at `submitted`."""
    _seed_pool(fake_db, 2)
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": FCFS_CAMPAIGN_ID, "name": "Race Arm", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": FCFS_POOL_ID, "pool_type": "voucher_drop", "winner_count": 2,
            "allocation_method": mp.ALLOCATION_FIRST_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    # No pre-existing entries, so arm_fcfs_campaign itself will pass the
    # zero-entries guard; monkeypatch the inventory read to insert a
    # straggler entry right as arming computes capacity, reproducing the
    # exact gap between the guard and the CAS without needing real threads.
    original_pool_stock = vps.pool_stock

    def _pool_stock_with_race(pool_id):
        fake_db[mp.ENTRIES_COLLECTION].insert_one({
            "campaign_id": FCFS_CAMPAIGN_ID, "telegram_user_id": 909090, "answer": "x",
            "answer_normalized": "x", "is_correct": True, "status": mp.ENTRY_STATUS_SUBMITTED,
            "identity_key": None, "identity_type": None, "disqualification_reason": None,
            "reward_id": None, "submitted_at": datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc),
        })
        return original_pool_stock(pool_id)

    fake_db["users"].insert_one({"user_id": 909090})
    with patch.object(vps, "pool_stock", side_effect=_pool_stock_with_race):
        result = mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID)

    assert result["armed"] is True
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"telegram_user_id": 909090})
    assert entry["status"] != mp.ENTRY_STATUS_SUBMITTED
    assert entry["status"] == mp.ENTRY_STATUS_WINNER


def test_transient_claim_failure_defers_instead_of_permanently_disqualifying(fake_db):
    """Codex P2: a guarded-update failure that is NOT genuine capacity
    exhaustion (e.g. an admin paused the campaign between the submission's
    own pre-write check and this exact claim attempt) must not permanently
    disqualify an otherwise-winning entry as mission_full -- it must be left
    exactly as-is (`submitted`) for the ordinary eligibility pass to decide
    once the campaign resumes."""
    _seed_fcfs(fake_db, capacity=5, claimed=1, armed=True, winner_count=5, status="paused")
    now = datetime.now(timezone.utc)
    uid = 606060
    fake_db["users"].insert_one({"user_id": uid})
    entry = _submitted_entry(fake_db, uid, now)

    result = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, dict(mp.DEFAULT_ELIGIBILITY_POLICY), 5, entry, now)

    assert result == {"state": "deferred"}
    stored = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry["_id"]})
    assert stored["status"] == mp.ENTRY_STATUS_SUBMITTED
    assert stored["disqualification_reason"] is None
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["mission_pool"]["fcfs_claimed"] == 1  # unchanged -- no slot consumed

    # Once resumed, the SAME entry is fairly (re-)claimable -- the identity
    # claim already belongs to this entry_id and is idempotent to re-claim.
    fake_db["gc_campaigns"].update_one({"campaign_id": FCFS_CAMPAIGN_ID}, {"$set": {"status": "live"}})
    result2 = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, dict(mp.DEFAULT_ELIGIBILITY_POLICY), 5, entry, now)
    assert result2["state"] == "winner"


def test_genuine_capacity_exhaustion_still_permanently_disqualifies(fake_db):
    """The pause-vs-full distinction must not weaken the real mission_full
    path: once fcfs_claimed truly reaches capacity, the failed claim is
    still permanent."""
    _seed_fcfs(fake_db, capacity=1, claimed=1, armed=True, winner_count=1, status="live")
    now = datetime.now(timezone.utc)
    uid = 606061
    fake_db["users"].insert_one({"user_id": uid})
    entry = _submitted_entry(fake_db, uid, now)

    result = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, dict(mp.DEFAULT_ELIGIBILITY_POLICY), 1, entry, now)

    assert result == {"state": "full"}
    stored = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry["_id"]})
    assert stored["status"] == mp.ENTRY_STATUS_DISQUALIFIED
    assert stored["disqualification_reason"] == mp.REASON_MISSION_FULL


# ---------------------------------------------------------------------------
# try_consume_fcfs_slot — exclusions consume zero slots
# ---------------------------------------------------------------------------

def test_wrong_answer_never_reaches_slot_consumption(fake_db):
    _seed_fcfs(fake_db, capacity=3, claimed=0, armed=True)
    client = _app().test_client()
    resp = _submit(client, uid=1, answer="wrong")
    assert resp.status_code == 200
    assert resp.get_json()["state"] == "incorrect_retry"
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_claimed"] == 0
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_voucher_hunter_excluded_consumes_zero_slots(fake_db):
    _seed_fcfs(fake_db, capacity=3, claimed=0, armed=True)
    fake_db["users"].insert_one({"user_id": 2, "multi_account_voucher_hunter": True})
    client = _app().test_client()
    resp = _submit(client, uid=2, answer="x")
    assert resp.get_json()["state"] == "submitted"
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"telegram_user_id": 2})
    assert entry["status"] == mp.ENTRY_STATUS_DISQUALIFIED
    assert entry["disqualification_reason"] == mp.REASON_VOUCHER_HUNTER
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["mission_pool"]["fcfs_claimed"] == 0
    assert campaign["status"] == "live"


def test_blocked_and_risk_users_consume_zero_slots(fake_db):
    _seed_fcfs(fake_db, capacity=3, claimed=0, armed=True)
    fake_db["users"].insert_one({"user_id": 3, "blocked": True})
    fake_db["users"].insert_one({"user_id": 4, "multi_account_risk": True})
    client = _app().test_client()
    _submit(client, uid=3, answer="x")
    _submit(client, uid=4, answer="x")
    reasons = {
        e["telegram_user_id"]: e["disqualification_reason"]
        for e in fake_db[mp.ENTRIES_COLLECTION].find({})
    }
    assert reasons[3] == mp.REASON_BLOCKED
    assert reasons[4] == mp.REASON_MULTI_ACCOUNT_RISK
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_claimed"] == 0


def test_duplicate_gaming_identity_consumes_only_one_slot(fake_db):
    _seed_fcfs(fake_db, capacity=3, claimed=0, armed=True)
    fake_db["users"].insert_one({"user_id": 10, "linked_gaming_accounts": ["SHARED1"]})
    fake_db["users"].insert_one({"user_id": 11, "linked_gaming_accounts": ["SHARED1"]})
    client = _app().test_client()
    _submit(client, uid=10, answer="x")
    _submit(client, uid=11, answer="x")

    e1 = fake_db[mp.ENTRIES_COLLECTION].find_one({"telegram_user_id": 10})
    e2 = fake_db[mp.ENTRIES_COLLECTION].find_one({"telegram_user_id": 11})
    assert e1["status"] == mp.ENTRY_STATUS_WINNER
    assert e2["status"] == mp.ENTRY_STATUS_DISQUALIFIED
    assert e2["disqualification_reason"] == mp.REASON_DUPLICATE_GAMING_ACCOUNT
    block = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})["mission_pool"]
    assert block["fcfs_claimed"] == 1


# ---------------------------------------------------------------------------
# 299 stays live / 300th closes / a Voucher Hunter at 300 never closes it
# ---------------------------------------------------------------------------

def test_299_eligible_remain_live_300th_closes(fake_db):
    _seed_fcfs(fake_db, capacity=300, claimed=0, armed=True, winner_count=300)
    policy = dict(mp.DEFAULT_ELIGIBILITY_POLICY)
    now = datetime.now(timezone.utc)

    for i in range(299):
        uid = 100000 + i
        fake_db["users"].insert_one({"user_id": uid})
        entry = _submitted_entry(fake_db, uid, now)
        result = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, policy, 300, entry, now)
        assert result["state"] == "winner"

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["status"] == "live"
    assert campaign["mission_pool"]["fcfs_claimed"] == 299
    assert campaign["mission_pool"]["closed_at"] is None

    uid = 999999
    fake_db["users"].insert_one({"user_id": uid})
    entry = _submitted_entry(fake_db, uid, now)
    result = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, policy, 300, entry, now)
    assert result["state"] == "winner"
    assert result["claimed"] == 300

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["closed_at"] is not None
    assert campaign["mission_pool"]["fcfs_claimed"] == 300

    winners = fake_db[mp.ENTRIES_COLLECTION].count_documents({"status": mp.ENTRY_STATUS_WINNER})
    assert winners == 300


def test_voucher_hunter_at_final_position_does_not_close_mission(fake_db):
    _seed_fcfs(fake_db, capacity=300, claimed=299, armed=True, winner_count=300)
    now = datetime.now(timezone.utc)
    policy = dict(mp.DEFAULT_ELIGIBILITY_POLICY)

    hunter_uid = 555000
    fake_db["users"].insert_one({"user_id": hunter_uid, "multi_account_voucher_hunter": True})
    entry = _submitted_entry(fake_db, hunter_uid, now)
    result = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, policy, 300, entry, now)
    assert result == {"state": "excluded", "reason": mp.REASON_VOUCHER_HUNTER}

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["status"] == "live"
    assert campaign["mission_pool"]["fcfs_claimed"] == 299
    assert campaign["mission_pool"]["closed_at"] is None

    good_uid = 555001
    fake_db["users"].insert_one({"user_id": good_uid})
    entry2 = _submitted_entry(fake_db, good_uid, now)
    result2 = mpp.try_consume_fcfs_slot(FCFS_CAMPAIGN_ID, policy, 300, entry2, now)
    assert result2["state"] == "winner"
    assert result2["claimed"] == 300

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["closed_at"] is not None


# ---------------------------------------------------------------------------
# Concurrency: the Mongo invariant under real thread contention
# ---------------------------------------------------------------------------

def test_concurrent_requests_racing_the_final_slot_yield_exactly_one_winner(fake_db):
    """10+ real HTTP requests racing the single remaining slot: exactly one
    must win it, the rest must receive mission_full, fcfs_claimed must never
    exceed capacity, and the guarded close must fire exactly once."""
    _seed_fcfs(fake_db, capacity=5, claimed=4, armed=True, winner_count=5)
    app = _app()

    def submit(idx):
        client = app.test_client()
        uid = 800000 + idx
        resp = client.post(
            f"/api/mission-pool/{FCFS_CAMPAIGN_ID}/submit?init_data=uid:{uid}",
            json={"answer": "x"},
        )
        return resp.status_code, resp.get_json()

    with _stub_initdata():
        results = _run_concurrently(submit, 15)

    # Every racer is either accepted onto the hot path (200, `submitted` —
    # decided in-line as winner or excluded-for-capacity) or, once the
    # guarded close has already landed, bounced before ever touching the
    # entries collection (409 `mission_full`). No third outcome is possible.
    for status, body in results:
        assert (status == 200 and body["state"] == "submitted") or \
               (status == 409 and body["code"] == "mission_full"), (status, body)

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["mission_pool"]["fcfs_claimed"] == 5
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["closed_at"] is not None

    # Regardless of how many raced past the pre-insert check, the atomic
    # $inc guard is what actually decides the outcome: exactly one entry
    # ever reaches `winner`, and fcfs_claimed never exceeds capacity.
    winners = fake_db[mp.ENTRIES_COLLECTION].count_documents({"status": mp.ENTRY_STATUS_WINNER})
    assert winners == 1


def test_fcfs_claimed_never_exceeds_capacity_under_heavy_contention(fake_db):
    """Property check with a tighter margin: 25 racers, capacity 1."""
    _seed_fcfs(fake_db, capacity=1, claimed=0, armed=True, winner_count=1)
    app = _app()

    def submit(idx):
        client = app.test_client()
        uid = 810000 + idx
        return client.post(
            f"/api/mission-pool/{FCFS_CAMPAIGN_ID}/submit?init_data=uid:{uid}",
            json={"answer": "x"},
        ).get_json()

    with _stub_initdata():
        _run_concurrently(submit, 25)

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    claimed = campaign["mission_pool"]["fcfs_claimed"]
    capacity = campaign["mission_pool"]["fcfs_capacity"]
    assert claimed <= capacity
    assert claimed == 1
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({"status": mp.ENTRY_STATUS_WINNER}) == 1


# ---------------------------------------------------------------------------
# Later submissions after close / mid-race exhaustion get mission_full
# ---------------------------------------------------------------------------

def test_late_submission_after_capacity_close_receives_mission_full(fake_db):
    _seed_fcfs(fake_db, capacity=1, claimed=1, armed=True, winner_count=1,
               status="ended", closed_at=datetime.now(timezone.utc))
    client = _app().test_client()
    resp = _submit(client, uid=321)
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "mission_full"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_live_but_capacity_exhausted_returns_mission_full_before_insert(fake_db):
    """The narrow race window: fcfs_claimed already == capacity but the
    guarded close has not landed yet. A late arrival must still be bounced
    without ever inserting an entry."""
    _seed_fcfs(fake_db, capacity=1, claimed=1, armed=True, winner_count=1, status="live")
    client = _app().test_client()
    resp = _submit(client, uid=322)
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "mission_full"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# Existing in-flight FCFS mission stays on the old scheduled path
# ---------------------------------------------------------------------------

def test_unarmed_fcfs_mission_never_checks_capacity_on_submit(fake_db):
    """A first_qualified campaign that already has entries (Decision 1) is
    never armed, so submissions keep going through the exact old path:
    unconditionally accepted as `submitted`, no fcfs_claimed touched."""
    doc = _seed_fcfs(fake_db, capacity=None, armed=False, winner_count=2)
    now = datetime.now(timezone.utc)
    fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": FCFS_CAMPAIGN_ID, "telegram_user_id": 1, "answer": "x",
        "answer_normalized": "x", "is_correct": True, "status": mp.ENTRY_STATUS_SUBMITTED,
        "identity_key": None, "identity_type": None, "disqualification_reason": None,
        "reward_id": None, "submitted_at": now, "created_at": now, "updated_at": now,
    })
    assert mpp.arm_fcfs_campaign(FCFS_CAMPAIGN_ID) == {"armed": False, "reason": "existing_entries"}

    client = _app().test_client()
    for uid in (2, 3, 4, 5):
        resp = _submit(client, uid=uid)
        assert resp.get_json()["state"] == "submitted"

    # More submissions than winner_count are all accepted -- old behaviour,
    # capacity is never enforced for an unarmed mission.
    entries = fake_db[mp.ENTRIES_COLLECTION].find({"status": mp.ENTRY_STATUS_SUBMITTED})
    assert len(entries) == 5  # the pre-existing one + the 4 just submitted
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["status"] == "live"
    assert campaign["mission_pool"]["fcfs_claimed"] == 0
    assert campaign["mission_pool"]["fcfs_armed"] is False


# ---------------------------------------------------------------------------
# Scheduler recovery: crash between the winning $inc and the guarded close
# ---------------------------------------------------------------------------

def _seed_capacity_reached_but_unclosed(fake_db):
    now = datetime.now(timezone.utc)
    _seed_fcfs(fake_db, capacity=2, claimed=2, armed=True, winner_count=2,
               status="live", ends_at=now + timedelta(hours=5), pool_available=2)
    for i, uid in enumerate((900001, 900002)):
        fake_db["users"].insert_one({"user_id": uid, "linked_gaming_accounts": []})
        entry_id = fake_db[mp.ENTRIES_COLLECTION].insert_one({
            "campaign_id": FCFS_CAMPAIGN_ID, "telegram_user_id": uid, "answer": "x",
            "answer_normalized": "x", "is_correct": True, "status": mp.ENTRY_STATUS_WINNER,
            "identity_key": f"tg:{uid}", "identity_type": "telegram", "disqualification_reason": None,
            "reward_id": None, "submitted_at": now - timedelta(minutes=5 - i),
            "created_at": now, "updated_at": now,
        }).inserted_id
        fake_db[mp.IDENTITY_CLAIMS_COLLECTION].insert_one({
            "campaign_id": FCFS_CAMPAIGN_ID, "identity_key": f"tg:{uid}", "entry_id": entry_id,
            "identity_type": "telegram", "claimed_at": now,
        })


def test_scheduler_settles_early_closed_mission_after_crash_before_close(fake_db):
    _seed_capacity_reached_but_unclosed(fake_db)

    with patch("telegram_utils.send_telegram_http_message", return_value=(True, None, False)):
        mpp.run_mission_pool_processor()

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": FCFS_CAMPAIGN_ID})
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["closed_at"] is not None
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 2
    assert fake_db["campaign_rewards"].count_documents({"category": "mission_pool"}) == 2


def test_scheduler_retry_causes_no_duplicate_reward(fake_db):
    _seed_capacity_reached_but_unclosed(fake_db)

    with patch("telegram_utils.send_telegram_http_message", return_value=(True, None, False)):
        mpp.run_mission_pool_processor()
        mpp.run_mission_pool_processor()
        mpp.run_mission_pool_processor()

    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 2
    assert fake_db["campaign_rewards"].count_documents({"category": "mission_pool"}) == 2
    winners = fake_db[mp.ENTRIES_COLLECTION].find({"status": mp.ENTRY_STATUS_REWARD_ALLOCATED})
    assert len(winners) == 2
    codes = [w["code"] for w in fake_db["voucher_pools"].find({"status": "issued"})]
    assert len(set(codes)) == 2


# ---------------------------------------------------------------------------
# Random Pool: zero behavioural change
# ---------------------------------------------------------------------------

def test_random_qualified_campaign_never_touched_by_fcfs_maintenance(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "mission-random-1", "name": "Random Mission", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": "RANDOM-POOL", "pool_type": "voucher_drop", "winner_count": 5,
            "allocation_method": mp.ALLOCATION_RANDOM_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    before = fake_db["gc_campaigns"].find_one({"campaign_id": "mission-random-1"})
    mpp._fcfs_maintenance(now)
    after = fake_db["gc_campaigns"].find_one({"campaign_id": "mission-random-1"})
    assert before == after
    assert "fcfs_armed" not in after["mission_pool"]


def test_random_pool_settlement_end_to_end_unchanged(fake_db):
    """Golden regression: a random_qualified mission still accepts
    submissions until its normal closing condition and settles exactly as
    before — run_mission_pool_processor's new FCFS maintenance pass is a
    complete no-op for it."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "mission-random-2", "name": "Random Mission 2", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "ended",
        "schedule": {"starts_at": now - timedelta(hours=3), "ends_at": now - timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": "RANDOM-POOL-2", "pool_type": "voucher_drop", "winner_count": 2,
            "allocation_method": mp.ALLOCATION_RANDOM_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    _seed_pool(fake_db, 5, pool_id="RANDOM-POOL-2")
    for i in range(4):
        uid = 700000 + i
        fake_db["users"].insert_one({"user_id": uid})
        fake_db[mp.ENTRIES_COLLECTION].insert_one({
            "campaign_id": "mission-random-2", "telegram_user_id": uid, "answer": "x",
            "answer_normalized": "x", "is_correct": True, "status": mp.ENTRY_STATUS_SUBMITTED,
            "identity_key": None, "identity_type": None, "disqualification_reason": None,
            "reward_id": None, "submitted_at": now - timedelta(hours=2) + timedelta(seconds=i),
            "created_at": now, "updated_at": now,
        })

    with patch("telegram_utils.send_telegram_http_message", return_value=(True, None, False)):
        mpp.run_mission_pool_processor()

    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": "mission-random-2"})
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    assert campaign["mission_pool"]["winner_count_actual"] == 2
    assert campaign["mission_pool"].get("fcfs_armed") in (None, False)


def test_random_qualified_submission_hot_path_byte_for_byte_unaffected(fake_db):
    """A random_qualified campaign's /submit response and stored entry must
    be identical whether or not the FCFS module even exists — no eligibility
    work, no identity claim, no capacity field ever touched."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "mission-random-3", "name": "Random Mission 3", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": "RANDOM-POOL-3", "pool_type": "voucher_drop", "winner_count": 5,
            "allocation_method": mp.ALLOCATION_RANDOM_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    client = _app().test_client()
    resp = _submit(client, uid=42, campaign_id="mission-random-3")
    assert resp.get_json() == {"status": "ok", "submitted": True, "state": "submitted"}
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"telegram_user_id": 42})
    assert entry["status"] == mp.ENTRY_STATUS_SUBMITTED
    assert entry["identity_key"] is None
    assert entry["disqualification_reason"] is None
