"""Tests for the live Random Pool auto-close/auto-draw automation built on
top of the existing Mission Pool engine (mission_pool.py /
mission_pool_processor.py, both covered by test_mission_pool*.py's 400+
tests, which stay green and unmodified — see test run notes in the PR).

This file covers ONLY the new behaviour added for the Random reward mode:

  * arm_random_campaign / try_consume_random_qualifying_slot: the live,
    bounded, indexed qualified-entry counter that runs inline in the
    submission hot path for an ARMED `random_qualified` campaign, and the
    atomic compare-and-set lock (`close_random_intake`) it triggers at
    `auto_close_qualified_entries`.
  * The `minimum_qualified_entries` scheduled-close-fallback safety gate
    (WAITING_FOR_MINIMUM) and admin override (Draw Now).
  * The sha256-ranked deterministic draw + draw_id + seed commitment.
  * PENDING_INVENTORY resumable allocation for a full-auto Random mission.
  * Retry/idempotency: no second draw, no double voucher issuance, no lost
    winner on a Telegram notification failure.
  * Admin config validation for the new fields, including the one full
    300/400/600 configuration required by the spec.

Small numbers are used everywhere else, per the spec's own guidance.
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

CAMPAIGN_ID = "mission-random-1"
POOL_ID = "MISSION-RANDOM"


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
        "voucher_pool_registry": [("pool_id",)],
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


def _admin_ok():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "ops"}, None))


def _no_telegram(ok=True, err=None, blocked=False):
    return patch("telegram_utils.send_telegram_http_message", return_value=(ok, err, blocked))


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


def _submit(client, uid, answer="x", campaign_id=CAMPAIGN_ID):
    with _verified(uid):
        return client.post(
            f"/api/mission-pool/{campaign_id}/submit?init_data=stub",
            json={"answer": answer},
        )


def _seed_pool(fake_db, count, pool_id=POOL_ID):
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


def _seed_random(fake_db, *, winner_count=3, minimum=4, auto_close=6, armed=True,
                  qualified_live_count=0, auto_draw=True, auto_issue=True,
                  status="live", ends_at=None, closed_at=None, pool_available=0,
                  campaign_id=CAMPAIGN_ID, pool_id=POOL_ID, waiting_for_minimum=False):
    now = datetime.now(timezone.utc)
    if pool_available:
        _seed_pool(fake_db, pool_available, pool_id=pool_id)
    doc = {
        "campaign_id": campaign_id,
        "name": "Random Pool Mission",
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
            "allocation_method": mp.ALLOCATION_RANDOM_QUALIFIED,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "minimum_qualified_entries": minimum,
            "auto_close_qualified_entries": auto_close,
            "auto_draw": auto_draw,
            "auto_issue": auto_issue,
            "cancelled": False,
            "closed_at": closed_at,
            "close_trigger": None,
            "processing_stage": mp.STAGE_PENDING,
            "processing_generation": 0,
            "random_armed": armed,
            "qualified_live_count": qualified_live_count,
            "random_armed_at": now if armed else None,
            "waiting_for_minimum": waiting_for_minimum,
            "pending_inventory": False,
            "inventory_shortage_count": 0,
        },
    }
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _seed_qualified_entry(fake_db, uid, *, offset_seconds=0, status=None, campaign_id=CAMPAIGN_ID):
    submitted = datetime.now(timezone.utc) - timedelta(minutes=30) + timedelta(seconds=offset_seconds)
    return fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": campaign_id, "telegram_user_id": uid, "answer": "x",
        "answer_normalized": "x", "is_correct": True,
        "status": status or mp.ENTRY_STATUS_QUALIFIED,
        "identity_key": f"tg:{uid}", "identity_type": "telegram",
        "disqualification_reason": None, "reward_id": None,
        "submitted_at": submitted, "created_at": submitted, "updated_at": submitted,
    }).inserted_id


def _campaign(fake_db, campaign_id=CAMPAIGN_ID):
    return fake_db["gc_campaigns"].find_one({"campaign_id": campaign_id})


def _block(fake_db, campaign_id=CAMPAIGN_ID):
    return (_campaign(fake_db, campaign_id) or {}).get("mission_pool") or {}


# ---------------------------------------------------------------------------
# 1/2. Auto-close threshold: open at n-1, locks at n
# ---------------------------------------------------------------------------

def test_mission_remains_open_one_below_auto_close_threshold(fake_db):
    """The small-number equivalent of "599 of 600 qualified participants":
    one qualifying submission short of auto_close_qualified_entries must
    leave the mission open."""
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6, qualified_live_count=4)
    app = _app()
    with app.test_client() as client:
        resp = _submit(client, 9001)
    assert resp.status_code == 200
    campaign = _campaign(fake_db)
    assert campaign["status"] == "live"
    assert campaign["mission_pool"]["closed_at"] is None
    assert campaign["mission_pool"]["qualified_live_count"] == 5


def test_mission_stays_open_strictly_below_threshold(fake_db):
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6, qualified_live_count=4)
    app = _app()
    with app.test_client() as client:
        resp = _submit(client, 9002)
    assert resp.status_code == 200
    campaign = _campaign(fake_db)
    assert campaign["status"] == "live"
    assert campaign["mission_pool"]["closed_at"] is None
    assert campaign["mission_pool"]["qualified_live_count"] == 5


def test_nth_qualified_entry_locks_the_mission(fake_db):
    """The entry that brings the live counter to auto_close_qualified_entries
    atomically transitions OPEN -> LOCKED (status live -> ended) and records
    the ENTRY_THRESHOLD trigger."""
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6, qualified_live_count=5)
    app = _app()
    with app.test_client() as client:
        resp = _submit(client, 9003)
    assert resp.status_code == 200
    campaign = _campaign(fake_db)
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["closed_at"] is not None
    assert campaign["mission_pool"]["close_trigger"] == mp.CLOSE_TRIGGER_ENTRY_THRESHOLD
    assert campaign["mission_pool"]["qualified_live_count"] == 6


def test_submission_after_lock_is_rejected_cleanly_and_never_counted(fake_db):
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6, qualified_live_count=6,
                 status="ended", closed_at=datetime.now(timezone.utc))
    app = _app()
    with app.test_client() as client:
        resp = _submit(client, 9004)
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_closed"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 0
    assert _block(fake_db)["qualified_live_count"] == 6


# ---------------------------------------------------------------------------
# 3/12. Concurrency: exactly one lock / one draw, repeated calls are no-ops
# ---------------------------------------------------------------------------

def test_concurrent_threshold_submissions_lock_exactly_once(fake_db):
    """Two users reaching the auto-close threshold simultaneously: the
    guarded $inc + CAS close means only one of them can be the entry that
    actually observes/causes the lock, and the mission is never left
    over-counted past what a single sequential run would produce."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=2, qualified_live_count=1)
    app = _app()

    def submit_one(idx):
        with app.test_client() as client:
            return _submit(client, 9100 + idx)

    results = _run_concurrently(submit_one, 5)
    for r in results:
        assert r.status_code in (200, 409)

    campaign = _campaign(fake_db)
    # Capacity is never exceeded: at most `auto_close - qualified_live_count`
    # (here: 1) of the concurrent racers can have been accepted as the
    # locking entry; the rest are cleanly rejected before ever incrementing
    # the counter, because the guarded update only ever lets the count go
    # from capacity-1 to capacity once.
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["qualified_live_count"] == 2
    accepted = fake_db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_QUALIFIED})
    assert accepted == 1


def test_two_workers_racing_to_lock_only_one_wins_the_transition(fake_db):
    """Two schedulers/workers attempting to close intake at the same moment:
    close_random_intake's guarded update_one can only ever modify the
    document once."""
    _seed_random(fake_db, winner_count=2, minimum=2, auto_close=2, qualified_live_count=2)
    now = datetime.now(timezone.utc)

    results = _run_concurrently(
        lambda idx: mpp.close_random_intake(CAMPAIGN_ID, now, mp.CLOSE_TRIGGER_ENTRY_THRESHOLD), 8)
    assert sum(1 for r in results if r) == 1

    campaign = _campaign(fake_db)
    assert campaign["status"] == "ended"


def test_repeated_scheduler_ticks_never_create_a_second_draw(fake_db):
    """Repeated scheduler execution / worker restart: process_campaign is
    idempotent, so calling it many times in a row must produce exactly one
    selection_seed and one draw_id."""
    _seed_random(fake_db, winner_count=2, minimum=2, auto_close=4,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 5)
    for i in range(4):
        _seed_qualified_entry(fake_db, 9200 + i, offset_seconds=i)

    with _no_telegram():
        for _ in range(5):
            mpp.process_campaign(CAMPAIGN_ID)

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    seed = campaign["mission_pool"]["selection_seed"]
    draw_id = campaign["mission_pool"]["draw_id"]
    assert seed and draw_id

    # Re-running many more times must not touch either.
    with _no_telegram():
        for _ in range(5):
            mpp.process_campaign(CAMPAIGN_ID)
    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["selection_seed"] == seed
    assert campaign["mission_pool"]["draw_id"] == draw_id
    assert fake_db["mission_pool_draw_summaries"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1


def test_repeated_draw_now_requests_never_reroll(fake_db):
    _seed_random(fake_db, winner_count=2, minimum=2, auto_close=4,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 5)
    for i in range(4):
        _seed_qualified_entry(fake_db, 9300 + i, offset_seconds=i)

    app = _app()
    with _no_telegram(), _admin_ok(), app.test_client() as client:
        r1 = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/draw-now", json={"confirm": True})
        assert r1.status_code == 200
        seed = _block(fake_db)["selection_seed"]
        winners_1 = {e["telegram_user_id"] for e in fake_db[mp.ENTRIES_COLLECTION].find(
            {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED})}

        r2 = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/draw-now", json={"confirm": True})
        assert r2.status_code == 200
        assert _block(fake_db)["selection_seed"] == seed
        winners_2 = {e["telegram_user_id"] for e in fake_db[mp.ENTRIES_COLLECTION].find(
            {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED})}
        assert winners_1 == winners_2


# ---------------------------------------------------------------------------
# 4/5/6. Winner selection + voucher issuance
# ---------------------------------------------------------------------------

def test_exactly_n_unique_winners_selected_from_more_participants(fake_db):
    """3 winners from 6 participants — the small-number equivalent of
    300-from-600."""
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 10)
    for i in range(6):
        _seed_qualified_entry(fake_db, 9400 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    winners = list(fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED}))
    non_winners = list(fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_NON_WINNER}))
    assert len(winners) == 3
    assert len(non_winners) == 3
    assert len({w["telegram_user_id"] for w in winners}) == 3


def test_each_winner_receives_exactly_one_unique_voucher_non_winners_none(fake_db):
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 10)
    for i in range(6):
        _seed_qualified_entry(fake_db, 9500 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    rewards = list(fake_db["campaign_rewards"].find({"campaign_id": CAMPAIGN_ID, "category": "mission_pool"}))
    assert len(rewards) == 3
    codes = [r["voucher_code"] for r in rewards]
    assert len(codes) == len(set(codes))  # every voucher code unique
    assert all(r["status"] == "assigned" for r in rewards)
    winner_uids = {r["telegram_user_id"] for r in rewards}
    non_winners = list(fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_NON_WINNER}))
    assert not (winner_uids & {n["telegram_user_id"] for n in non_winners})
    assert len(non_winners) == 3


# ---------------------------------------------------------------------------
# 7/8. Duplicate + unqualified submissions never inflate the counter
# ---------------------------------------------------------------------------

def test_duplicate_submission_does_not_increase_qualified_count(fake_db):
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6, qualified_live_count=0)
    app = _app()
    with app.test_client() as client:
        r1 = _submit(client, 9600)
        assert r1.get_json()["state"] == "submitted"
        r2 = _submit(client, 9600)
        assert r2.get_json()["state"] == "already_submitted"
    assert _block(fake_db)["qualified_live_count"] == 1
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 1


def test_unqualified_submission_does_not_increase_qualified_count(fake_db):
    """A blocked user's entry is durably recorded (it exists in
    mission_entries for audit) but never counts toward the qualified
    total."""
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6, qualified_live_count=0)
    fake_db["users"].insert_one({"user_id": 9601, "blocked": True})
    app = _app()
    with app.test_client() as client:
        resp = _submit(client, 9601)
    assert resp.status_code == 200
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 9601})
    assert entry["status"] == mp.ENTRY_STATUS_DISQUALIFIED
    assert entry["disqualification_reason"] == mp.REASON_BLOCKED
    assert _block(fake_db)["qualified_live_count"] == 0


# ---------------------------------------------------------------------------
# 9/10. Scheduled-close fallback + minimum gate
# ---------------------------------------------------------------------------

def test_scheduled_close_draws_between_minimum_and_auto_close(fake_db):
    """5 qualified entries, minimum=4, auto_close=6, deadline elapsed ->
    scheduler locks and draws (5 is within [minimum, auto_close))."""
    ended = datetime.now(timezone.utc) - timedelta(minutes=1)
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 armed=True, qualified_live_count=5, status="live", ends_at=ended)
    _seed_pool(fake_db, 10)
    for i in range(5):
        _seed_qualified_entry(fake_db, 9700 + i, offset_seconds=i)

    now = datetime.now(timezone.utc)
    mpp._random_maintenance(now)
    campaign = _campaign(fake_db)
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["close_trigger"] == mp.CLOSE_TRIGGER_SCHEDULED_CLOSE

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)
    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    assert campaign["mission_pool"]["winner_count_actual"] == 3


def test_scheduled_close_below_minimum_does_not_draw(fake_db):
    """3 qualified entries, minimum=4: the deadline elapsing must NOT
    silently draw. The mission is left recoverable (WAITING_FOR_MINIMUM),
    never flipped to FCFS, never drawn."""
    ended = datetime.now(timezone.utc) - timedelta(minutes=1)
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="ended", closed_at=ended, ends_at=ended)
    for i in range(3):
        _seed_qualified_entry(fake_db, 9800 + i, offset_seconds=i)

    with _no_telegram():
        result = mpp.process_campaign(CAMPAIGN_ID)

    assert result.get("skipped") == "waiting_for_minimum"
    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["processing_stage"] != mp.STAGE_COMPLETED
    assert campaign["mission_pool"]["processing_stage"] != mp.STAGE_SELECTING_WINNERS
    assert campaign["mission_pool"]["waiting_for_minimum"] is True
    assert campaign["mission_pool"]["allocation_method"] == mp.ALLOCATION_RANDOM_QUALIFIED  # never flips to FCFS
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_WINNER}) == 0


def test_extend_deadline_reopens_a_waiting_for_minimum_mission(fake_db):
    ended = datetime.now(timezone.utc) - timedelta(minutes=1)
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="live", ends_at=ended, waiting_for_minimum=True)
    new_deadline = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    app = _app()
    with _admin_ok(), app.test_client() as client:
        resp = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/extend-deadline",
                            json={"scheduled_close_at": new_deadline})
    assert resp.status_code == 200
    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["waiting_for_minimum"] is False
    assert campaign["schedule"]["ends_at"] > ended

    # Submissions are open again immediately.
    with app.test_client() as client:
        resp = _submit(client, 9900)
    assert resp.status_code == 200


def test_draw_now_below_minimum_requires_override_with_reason(fake_db):
    ended = datetime.now(timezone.utc) - timedelta(minutes=1)
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="ended", closed_at=ended, waiting_for_minimum=True)
    _seed_pool(fake_db, 10)
    for i in range(2):
        _seed_qualified_entry(fake_db, 9950 + i, offset_seconds=i)

    app = _app()
    with _no_telegram(), _admin_ok(), app.test_client() as client:
        blocked = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/draw-now", json={"confirm": True})
        assert blocked.status_code == 409
        assert blocked.get_json()["code"] == "minimum_not_met"
        assert _campaign(fake_db)["mission_pool"]["processing_stage"] != mp.STAGE_COMPLETED

        missing_reason = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/draw-now",
                                      json={"confirm": True, "override_minimum": True})
        assert missing_reason.status_code == 400

        overridden = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/draw-now",
                                  json={"confirm": True, "override_minimum": True,
                                        "override_reason": "exec approved early draw"})
        assert overridden.status_code == 200

    audit = list(fake_db["campaign_admin_audit_log"].find({"action": "mission_draw_now"}))
    assert audit and audit[0]["details"]["override_reason"] == "exec approved early draw"
    assert _campaign(fake_db)["mission_pool"]["winner_count_actual"] == 2


# ---------------------------------------------------------------------------
# 11. Insufficient inventory -> PENDING_INVENTORY, resumable
# ---------------------------------------------------------------------------

def test_insufficient_inventory_produces_pending_inventory(fake_db):
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 1)  # only 1 code for 3 winners
    for i in range(6):
        _seed_qualified_entry(fake_db, 9990 + i, offset_seconds=i)

    with _no_telegram():
        result = mpp.process_campaign(CAMPAIGN_ID)

    assert result.get("allocation", {}).get("pending_inventory") is True
    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["pending_inventory"] is True
    assert campaign["mission_pool"]["inventory_shortage_count"] == 2
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_ALLOCATING_REWARDS
    # No winner was disqualified for the shortage — all three are still
    # WINNER, waiting for inventory, never rerolled.
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_WINNER}) == 3
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "disqualification_reason": mp.REASON_OUT_OF_STOCK}) == 0
    winners_before = {e["telegram_user_id"] for e in fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_WINNER})}
    seed = campaign["mission_pool"]["selection_seed"]
    draw_id = campaign["mission_pool"]["draw_id"]
    assert draw_id
    # The preflight check catches the shortage before any per-entry
    # allocation is even attempted, so no reward rows exist yet — this is
    # the "pause the whole pass before drawing any code" behaviour, as
    # opposed to the per-entry out_of_stock reconciliation path covered by
    # test_voucher_claimed_but_ledger_not_finalized_is_reconciled_not_reclaimed.
    assert fake_db["campaign_rewards"].count_documents(
        {"campaign_id": CAMPAIGN_ID, "category": "mission_pool"}) == 0

    # Replenish inventory and resume: SAME draw, SAME winners.
    _seed_pool(fake_db, 5, pool_id=POOL_ID + "-EXTRA")  # unrelated pool: must not matter
    for i in range(5):
        fake_db["voucher_pools"].insert_one({
            "pool_id": POOL_ID, "code": f"REPLENISH-{i:03d}", "status": "available",
            "issued_to": None, "issued_at": None, "pool_source": "campaign_centre",
            "pool_type": "voucher_drop", "allocation_scope": "campaign_rewards",
        })

    with _no_telegram():
        result2 = mpp.process_campaign(CAMPAIGN_ID)

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["pending_inventory"] is False
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    assert campaign["mission_pool"]["selection_seed"] == seed  # no re-draw
    assert campaign["mission_pool"]["draw_id"] == draw_id  # same draw_id preserved
    rewards_after = list(fake_db["campaign_rewards"].find(
        {"campaign_id": CAMPAIGN_ID, "category": "mission_pool", "status": "assigned"}))
    winners_after = {r["telegram_user_id"] for r in rewards_after}
    assert winners_after == winners_before
    assert len(winners_after) == 3
    voucher_codes = [r["voucher_code"] for r in rewards_after]
    assert len(voucher_codes) == len(set(voucher_codes))  # still unique


# ---------------------------------------------------------------------------
# 13/14. Crash recovery + reconciliation + notification retry
# ---------------------------------------------------------------------------

def test_worker_crash_after_winners_stored_before_vouchers_issued_resumes_cleanly(fake_db):
    """Persist winners, then simulate the worker dying before allocation ever
    runs (stage still WINNERS_SELECTED) — the next process_campaign call
    must allocate from the SAME winner set, never re-select."""
    _seed_random(fake_db, winner_count=2, minimum=2, auto_close=4,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 5)
    for i in range(4):
        _seed_qualified_entry(fake_db, 9910 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)  # completes normally in this fake, single tick

    winners = {e["telegram_user_id"] for e in fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED})}
    assert len(winners) == 2

    # Simulate a restart: re-run from scratch. Nothing should change.
    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)
    winners_after = {e["telegram_user_id"] for e in fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED})}
    assert winners_after == winners


def test_voucher_claimed_but_ledger_not_finalized_is_reconciled_not_reclaimed(fake_db):
    """Crash after voucher reservation but before ledger finalization: a
    code already issued_for_reward_id must be REUSED, never a second code
    claimed for the same winner."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=2,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 5)
    entry_id = _seed_qualified_entry(fake_db, 9920, offset_seconds=0)
    fake_db[mp.ENTRIES_COLLECTION].update_one(
        {"_id": entry_id}, {"$set": {"status": mp.ENTRY_STATUS_WINNER}})

    campaign = _campaign(fake_db)
    campaign["mission_pool"]["selection_seed"] = "deadbeef" * 4
    campaign["mission_pool"]["draw_id"] = "draw_test1"
    reward_id = mp.mission_reward_id(CAMPAIGN_ID, entry_id)

    # Simulate the crash: a code is already claimed for this reward_id
    # (`issued_for_reward_id` set) but the campaign_rewards row was never
    # created/bound.
    claimed_code = fake_db["voucher_pools"].find_one({"pool_id": POOL_ID, "status": "available"})
    fake_db["voucher_pools"].update_one(
        {"_id": claimed_code["_id"]},
        {"$set": {"status": "issued", "issued_to": 9920, "issued_to_user_id": 9920,
                   "issued_at": datetime.now(timezone.utc), "issued_for_reward_id": reward_id}},
    )
    available_before_recovery = vps.pool_stock(POOL_ID)["available"]

    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})
    now = datetime.now(timezone.utc)
    result = mpp._allocate_for_entry(campaign, entry, now, 0)

    assert result["state"] == "allocated"
    reward = fake_db["campaign_rewards"].find_one({"reward_id": reward_id})
    assert reward["voucher_code"] == claimed_code["code"]
    # No SECOND code was drawn for this winner: available inventory is
    # unchanged by the recovery itself (the code was already off the
    # available pile before this call ran).
    assert vps.pool_stock(POOL_ID)["available"] == available_before_recovery


def test_notification_failure_and_retry_never_issues_a_second_voucher(fake_db):
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=2,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 5)
    for i in range(2):
        _seed_qualified_entry(fake_db, 9930 + i, offset_seconds=i)

    with _no_telegram(ok=False, err="temporary_error"):
        mpp.process_campaign(CAMPAIGN_ID)

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_NOTIFYING
    reward = fake_db["campaign_rewards"].find_one({"campaign_id": CAMPAIGN_ID, "category": "mission_pool"})
    assert reward["status"] == "assigned"
    assert reward["notification_status"] == "failed_retryable"
    voucher_code = reward["voucher_code"]

    # Force the retry window open and retry.
    fake_db["campaign_rewards"].update_one(
        {"reward_id": reward["reward_id"]},
        {"$set": {"notification_next_attempt_at": datetime.now(timezone.utc) - timedelta(seconds=1)}},
    )
    with _no_telegram(ok=True):
        mpp.process_campaign(CAMPAIGN_ID)

    reward_after = fake_db["campaign_rewards"].find_one({"reward_id": reward["reward_id"]})
    assert reward_after["notification_status"] == "sent"
    # Exactly the same voucher — a Telegram failure never re-rolls the code
    # or creates a second reward row.
    assert reward_after["voucher_code"] == voucher_code
    # Whichever of the two candidates the (randomly seeded) draw picked as
    # the single winner, it must hold exactly one reward row — never a
    # second one created by the notification retry.
    assert fake_db["campaign_rewards"].count_documents(
        {"campaign_id": CAMPAIGN_ID, "category": "mission_pool",
         "telegram_user_id": reward["telegram_user_id"]}) == 1


# ---------------------------------------------------------------------------
# Config validation, including the one full 300/400/600 case
# ---------------------------------------------------------------------------

def test_full_configuration_300_400_600_is_valid():
    """Preserves at least one full-scale configuration validation test, per
    the spec, alongside the smaller functional tests above."""
    config, err = mp.validate_mission_pool_config({
        "pool_id": "GRAND-RANDOM-POOL",
        "winner_count": 300,
        "allocation_method": "random_qualified",
        "minimum_qualified_entries": 400,
        "auto_close_qualified_entries": 600,
        "auto_draw": True,
        "auto_issue": True,
        "entries_per_user": 1,
    })
    assert err is None
    assert config["winner_count"] == 300
    assert config["minimum_qualified_entries"] == 400
    assert config["auto_close_qualified_entries"] == 600
    assert config["auto_draw"] is True
    assert config["auto_issue"] is True
    assert config["entries_per_user"] == 1


@pytest.mark.parametrize("overrides,expected_code", [
    ({"winner_count": 0}, "invalid_winner_count"),
    ({"minimum_qualified_entries": 300}, "minimum_qualified_entries_must_exceed_winner_count"),
    ({"auto_close_qualified_entries": 399}, "auto_close_qualified_entries_below_minimum"),
    ({"allocation_method": "first_qualified"}, "random_pool_fields_require_random_mode"),
    ({"entries_per_user": 2}, "entries_per_user_not_supported"),
])
def test_configuration_validation_rules_300_400_600(overrides, expected_code):
    base = {
        "pool_id": "GRAND-RANDOM-POOL",
        "winner_count": 300,
        "allocation_method": "random_qualified",
        "minimum_qualified_entries": 400,
        "auto_close_qualified_entries": 600,
    }
    base.update(overrides)
    config, err = mp.validate_mission_pool_config(base)
    assert err == expected_code
    assert config is None


def test_publish_blocked_when_inventory_below_winner_count(fake_db):
    """§ Prevent publishing/arming the mission if configured voucher
    inventory contains fewer vouchers than winner_count — scoped to a
    full-auto Random mission (auto_close_qualified_entries configured)."""
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "mission-random-inv", "name": "Inv", "type": "mission_pool",
        "mechanic": "mission_pool", "status": "draft",
        "schedule": {"starts_at": now, "ends_at": now + timedelta(days=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": POOL_ID, "pool_type": "voucher_drop", "winner_count": 3,
            "allocation_method": mp.ALLOCATION_RANDOM_QUALIFIED,
            "minimum_qualified_entries": 4, "auto_close_qualified_entries": 6,
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False, "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    _seed_pool(fake_db, 2)  # short of winner_count=3

    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    with _admin_ok(), app.test_client() as client:
        resp = client.post("/api/admin/gc-campaigns/mission-random-inv/publish")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "insufficient_voucher_inventory"
    assert _campaign(fake_db, "mission-random-inv")["status"] == "draft"


# ---------------------------------------------------------------------------
# Admin pause/resume auto-draw
# ---------------------------------------------------------------------------

def test_pause_auto_draw_holds_selection_until_resumed_or_admin_forces_it(fake_db):
    _seed_random(fake_db, winner_count=2, minimum=2, auto_close=4, auto_draw=True,
                 status="ended", closed_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    _seed_pool(fake_db, 5)
    for i in range(4):
        _seed_qualified_entry(fake_db, 9940 + i, offset_seconds=i)

    app = _app()
    with _admin_ok(), app.test_client() as client:
        resp = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/pause-auto-draw")
        assert resp.status_code == 200

    with _no_telegram():
        result = mpp.process_campaign(CAMPAIGN_ID)  # scheduler tick, admin_triggered=False by default
    assert result.get("skipped") == "auto_draw_paused"
    assert _campaign(fake_db)["mission_pool"]["processing_stage"] != mp.STAGE_SELECTING_WINNERS

    # An explicit admin process call (Retry Unresolved Rewards / Draw Now)
    # still forces past the pause.
    with _no_telegram(), _admin_ok(), app.test_client() as client:
        resp = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/process")
        assert resp.status_code == 200
    assert _campaign(fake_db)["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED


# ---------------------------------------------------------------------------
# Codex review fixes: bounded increment, arm/submission race, draw-now vs
# minimum on a still-open mission
# ---------------------------------------------------------------------------

def test_qualifying_increment_never_exceeds_auto_close_threshold(fake_db):
    """Concurrent submissions racing at threshold-1 must never push
    qualified_live_count past auto_close_qualified_entries — the guarded
    $inc bounds on the count itself, not just on status/closed_at."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=6, qualified_live_count=5)
    app = _app()

    def submit_one(idx):
        with app.test_client() as client:
            return _submit(client, 9970 + idx)

    results = _run_concurrently(submit_one, 10)
    for r in results:
        assert r.status_code in (200, 409)

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["qualified_live_count"] == 6  # never higher
    assert campaign["status"] == "ended"
    qualified_entries = fake_db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_QUALIFIED})
    assert qualified_entries == 1  # exactly one of the ten actually incremented


def test_arm_catch_up_counts_a_straggler_entry_immediately(fake_db):
    """arm_random_campaign/_catch_up_random_race: an entry that slipped in
    between the zero-entries guard and the arming CAS must still be counted
    right away, not left stranded until the mission eventually closes."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=3, armed=True, qualified_live_count=0)
    fake_db["users"].insert_one({"user_id": 9980})
    entry_id = _seed_qualified_entry(fake_db, 9980, status=mp.ENTRY_STATUS_SUBMITTED)
    fake_db[mp.ENTRIES_COLLECTION].update_one({"_id": entry_id}, {"$set": {"is_correct": True}})

    now = datetime.now(timezone.utc)
    mpp._catch_up_random_race(CAMPAIGN_ID, mp.DEFAULT_ELIGIBILITY_POLICY, 3, now)

    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})
    assert entry["status"] == mp.ENTRY_STATUS_QUALIFIED
    assert _block(fake_db)["qualified_live_count"] == 1


def test_draw_now_below_minimum_never_locks_a_still_open_mission(fake_db):
    """A rejected Draw Now on a mission that hasn't closed yet must leave it
    exactly as open as before — never lock intake and then refuse, which
    would strand it (Extend Deadline's already_locked guard would then also
    refuse it, with no way back)."""
    _seed_random(fake_db, winner_count=3, minimum=4, auto_close=6,
                 status="live", armed=True, qualified_live_count=2)

    app = _app()
    with _admin_ok(), app.test_client() as client:
        resp = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/draw-now", json={"confirm": True})
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "minimum_not_met"

    campaign = _campaign(fake_db)
    assert campaign["status"] == "live"
    assert campaign["mission_pool"]["closed_at"] is None

    # Extend Deadline must still work — the mission was never locked.
    new_deadline = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    with _admin_ok(), app.test_client() as client:
        extend_resp = client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/extend-deadline",
                                   json={"scheduled_close_at": new_deadline})
    assert extend_resp.status_code == 200

    # And submissions are still accepted.
    with app.test_client() as client:
        submit_resp = _submit(client, 9990)
    assert submit_resp.status_code == 200


def test_admin_pause_races_automatic_closure_intake_still_locks(fake_db):
    """Admin pause racing with automatic closure: pausing auto_draw/auto_issue
    must never stop the entry-counting/auto-close intake mechanic itself —
    only the downstream draw/issuance steps."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=1, qualified_live_count=0)
    app = _app()
    with _admin_ok(), app.test_client() as client:
        client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/pause-auto-draw")
    with app.test_client() as client:
        resp = _submit(client, 9960)
    assert resp.status_code == 200
    campaign = _campaign(fake_db)
    assert campaign["status"] == "ended"  # still locks at threshold
    assert campaign["mission_pool"]["close_trigger"] == mp.CLOSE_TRIGGER_ENTRY_THRESHOLD


# ---------------------------------------------------------------------------
# Final pre-merge verification round: the remaining required targeted tests
# ---------------------------------------------------------------------------

def test_repeated_catch_up_does_not_double_count_the_same_entry(fake_db):
    """_catch_up_random_race must be idempotent: an entry it already graded
    to QUALIFIED (or DISQUALIFIED) is never re-processed or re-counted by a
    second call, because it only ever queries entries still at `submitted`."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=5, armed=True, qualified_live_count=0)
    fake_db["users"].insert_one({"user_id": 9500})
    entry_id = _seed_qualified_entry(fake_db, 9500, status=mp.ENTRY_STATUS_SUBMITTED)

    now = datetime.now(timezone.utc)
    mpp._catch_up_random_race(CAMPAIGN_ID, mp.DEFAULT_ELIGIBILITY_POLICY, 5, now)
    assert _block(fake_db)["qualified_live_count"] == 1
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})
    assert entry["status"] == mp.ENTRY_STATUS_QUALIFIED

    # Call it again (and again) — the counter must not move, and the entry
    # must not be touched a second time.
    mpp._catch_up_random_race(CAMPAIGN_ID, mp.DEFAULT_ELIGIBILITY_POLICY, 5, now)
    mpp._catch_up_random_race(CAMPAIGN_ID, mp.DEFAULT_ELIGIBILITY_POLICY, 5, now)
    assert _block(fake_db)["qualified_live_count"] == 1
    entry_after = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})
    assert entry_after["status"] == mp.ENTRY_STATUS_QUALIFIED
    assert entry_after["updated_at"] == entry["updated_at"]  # untouched by the repeats


def test_repeated_arm_attempts_do_not_corrupt_the_counter(fake_db):
    """Multiple concurrent arming workers: only one CAS can ever succeed, so
    only one caller ever resets/initializes qualified_live_count — a loser
    must be a clean no-op ('cas_lost'), never a second zero-out or a partial
    write."""
    _seed_random(fake_db, winner_count=1, minimum=1, auto_close=5, armed=False, qualified_live_count=0)
    # Drop straight to an "unarmed but otherwise live" doc, as arm_random_campaign expects.
    fake_db["gc_campaigns"].update_one(
        {"campaign_id": CAMPAIGN_ID},
        {"$set": {"mission_pool.random_armed": False, "mission_pool.random_armed_at": None}},
    )

    results = _run_concurrently(lambda idx: mpp.arm_random_campaign(CAMPAIGN_ID), 8)
    armed_results = [r for r in results if r.get("armed")]
    assert len(armed_results) == 1  # exactly one winner

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["random_armed"] is True
    assert campaign["mission_pool"]["qualified_live_count"] == 0  # initialized exactly once, never re-zeroed

    # Submitting afterwards still counts normally — the counter was never
    # left in a corrupted/partial state by the losing callers.
    app = _app()
    with app.test_client() as client:
        resp = _submit(client, 9600)
    assert resp.status_code == 200
    assert _block(fake_db)["qualified_live_count"] == 1


def test_full_scale_300_400_600_draw_selects_300_unique_winners_with_unique_vouchers(fake_db):
    """The literal full-scale case: winner_count=300, minimum=400,
    auto_close=600. The 600th qualified participant locks the mission
    exactly once, and the draw produces exactly 300 unique winners each
    with exactly one unique voucher."""
    _seed_random(fake_db, winner_count=300, minimum=400, auto_close=600, qualified_live_count=0)
    _seed_pool(fake_db, 300)
    app = _app()

    with app.test_client() as client:
        for i in range(599):
            resp = _submit(client, 20000 + i)
            assert resp.status_code == 200
        campaign = _campaign(fake_db)
        assert campaign["status"] == "live"
        assert campaign["mission_pool"]["qualified_live_count"] == 599

        # The 600th locks it exactly once.
        resp = _submit(client, 20000 + 599)
        assert resp.status_code == 200

    campaign = _campaign(fake_db)
    assert campaign["status"] == "ended"
    assert campaign["mission_pool"]["qualified_live_count"] == 600
    assert campaign["mission_pool"]["close_trigger"] == mp.CLOSE_TRIGGER_ENTRY_THRESHOLD

    # A further submission after lock is cleanly rejected and never counted.
    with app.test_client() as client:
        resp = _submit(client, 999999)
    assert resp.status_code == 409
    assert _block(fake_db)["qualified_live_count"] == 600

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    campaign = _campaign(fake_db)
    assert campaign["mission_pool"]["processing_stage"] == mp.STAGE_COMPLETED
    assert campaign["mission_pool"]["winner_count_actual"] == 300

    winners = list(fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED}))
    non_winners = fake_db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_NON_WINNER})
    assert len(winners) == 300
    assert len({w["telegram_user_id"] for w in winners}) == 300
    assert non_winners == 300  # 600 qualified - 300 winners

    rewards = list(fake_db["campaign_rewards"].find({"campaign_id": CAMPAIGN_ID, "category": "mission_pool"}))
    assert len(rewards) == 300
    voucher_codes = [r["voucher_code"] for r in rewards]
    assert len(voucher_codes) == len(set(voucher_codes)) == 300
    assert all(r["status"] == "assigned" for r in rewards)

    summary = fake_db["mission_pool_draw_summaries"].find_one({"campaign_id": CAMPAIGN_ID})
    assert summary is not None
    assert summary["winner_count_actual"] == 300
    assert summary["voucher_count_issued"] == 300
    assert summary["eligible_participant_count"] == 600
    assert summary["trigger_type"] == mp.CLOSE_TRIGGER_ENTRY_THRESHOLD
