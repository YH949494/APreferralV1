"""Real-contention tests for Mission Pool (spec §50).

These use actual threads racing against shared state, not sequential mocks.
The in-memory FakeCollection enforces unique indexes and serialises
find_one_and_update/update_one under one lock, exactly as MongoDB does, so
"exactly one wins" here means the same thing it means in production.
"""

import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import database
import mission_pool as mp
import mission_pool_processor as mpp
import voucher_pool_service as vps
from fake_mongo import FakeDb

CAMPAIGN_ID = "mission-race"
POOL_ID = "MISSION-RACE"


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
    for module in (mp, mpp, vps):
        monkeypatch.setattr(module, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    monkeypatch.setattr(mpp.mp, "mission_pool_enabled", lambda: True)
    return fdb


def _seed_campaign(fake_db, *, status="ended", winner_count=2, hours_ago=1):
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": CAMPAIGN_ID,
        "name": "Race Mission",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": status,
        "schedule": {
            "starts_at": now - timedelta(hours=3),
            "ends_at": now - timedelta(hours=hours_ago) if status == "ended" else now + timedelta(hours=1),
        },
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": POOL_ID, "pool_type": "voucher_drop",
            "winner_count": winner_count, "allocation_method": "random_qualified",
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })


def _seed_pool(fake_db, count):
    fake_db["voucher_pool_registry"].insert_one({
        "pool_id": POOL_ID, "name": "Race", "pool_type": "voucher_drop",
        "allocation_scope": "campaign_rewards", "status": "active",
    })
    for i in range(count):
        fake_db["voucher_pools"].insert_one({
            "pool_id": POOL_ID, "code": f"RACE{i:03d}", "status": "available",
            "issued_to": None, "issued_at": None, "pool_source": "campaign_centre",
            "pool_type": "voucher_drop", "allocation_scope": "campaign_rewards",
        })


def _seed_entry(fake_db, uid, *, offset=0, status=None, identity_key=None):
    submitted = datetime.now(timezone.utc) - timedelta(hours=2) + timedelta(seconds=offset)
    return fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": uid,
        "answer": "x", "answer_normalized": "x", "is_correct": True,
        "status": status or mp.ENTRY_STATUS_SUBMITTED,
        "identity_key": identity_key, "identity_type": "telegram" if identity_key else None,
        "disqualification_reason": None, "reward_id": None,
        "submitted_at": submitted, "created_at": submitted, "updated_at": submitted,
    }).inserted_id


def _run_concurrently(fn, n):
    """Start n threads on a barrier so they collide as hard as the runtime
    allows, and return their results in completion-independent order."""
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


# ---------------------------------------------------------------------------
# Duplicate submission race (§50)
# ---------------------------------------------------------------------------

def _stub_initdata():
    """Patch signature verification ONCE for the whole race and derive the uid
    from the init_data value. Patching inside each thread would let one
    thread's teardown unpatch the others mid-flight."""
    def verify(raw):
        return True, {"user": '{"id": %s}' % str(raw).split(":", 1)[-1]}, "ok"

    return patch("vouchers.verify_telegram_init_data", side_effect=verify)


def test_ten_concurrent_submissions_from_one_user_create_one_entry(fake_db):
    _seed_campaign(fake_db, status="live")
    app = Flask(__name__)
    app.register_blueprint(mp.mission_pool_bp)

    def submit(_idx):
        client = app.test_client()
        resp = client.post(
            f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=uid:4242", json={"answer": "x"}
        )
        return resp.status_code, resp.get_json()

    with _stub_initdata():
        results = _run_concurrently(submit, 10)

    assert all(code == 200 for code, _ in results)
    states = [body["state"] for _, body in results]
    assert states.count("submitted") == 1
    assert states.count("already_submitted") == 9
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1


def test_concurrent_submissions_from_distinct_users_all_land(fake_db):
    _seed_campaign(fake_db, status="live")
    app = Flask(__name__)
    app.register_blueprint(mp.mission_pool_bp)

    def submit(idx):
        client = app.test_client()
        return client.post(
            f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=uid:{7000 + idx}",
            json={"answer": "x"},
        ).get_json()

    with _stub_initdata():
        results = _run_concurrently(submit, 25)
    assert all(r["state"] == "submitted" for r in results)
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 25


# ---------------------------------------------------------------------------
# Campaign processor ownership race (§50)
# ---------------------------------------------------------------------------

def test_only_one_campaign_processor_wins(fake_db):
    _seed_campaign(fake_db)
    now = datetime.now(timezone.utc)

    fences = _run_concurrently(lambda _i: mpp._claim_campaign(CAMPAIGN_ID, now), 12)
    winners = [f for f in fences if f is not None]
    assert len(winners) == 1


def test_concurrent_process_campaign_produces_one_winner_set(fake_db):
    """Eight workers racing the whole state machine must still yield exactly
    winner_count winners, winner_count vouchers and winner_count rewards."""
    _seed_campaign(fake_db, winner_count=3)
    _seed_pool(fake_db, 20)
    for i in range(12):
        fake_db["users"].insert_one({"user_id": 8000 + i})
        _seed_entry(fake_db, 8000 + i, offset=i)

    def run(_idx):
        return mpp.process_campaign(CAMPAIGN_ID)

    with patch("telegram_utils.send_telegram_http_message", return_value=(True, None, False)):
        _run_concurrently(run, 8)
    # Drain any stage the losing workers did not get to run.
    with patch("telegram_utils.send_telegram_http_message", return_value=(True, None, False)):
        for _ in range(6):
            mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] == mp.STAGE_COMPLETED
    assert block["winner_count_actual"] == 3
    winners = fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_REWARD_ALLOCATED}
    )
    assert len(winners) == 3
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 3
    assert fake_db["campaign_rewards"].count_documents({"category": "mission_pool"}) == 3
    # One code per winner, no code handed to two people.
    issued = fake_db["voucher_pools"].find({"status": "issued"})
    assert len({v["code"] for v in issued}) == 3
    assert len({v["issued_to"] for v in issued}) == 3


# ---------------------------------------------------------------------------
# Voucher allocation race (§50)
# ---------------------------------------------------------------------------

def test_many_workers_racing_the_last_voucher_yield_exactly_one_winner(fake_db):
    """One code, ten concurrent allocations for ten different winners:
    exactly one gets it, the rest are out_of_stock. Inventory never goes
    negative and no code is issued twice."""
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 1)
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    entry_ids = [
        _seed_entry(fake_db, 9000 + i, offset=i, status=mp.ENTRY_STATUS_WINNER,
                    identity_key=f"tg:{9000 + i}")
        for i in range(10)
    ]

    def allocate(idx):
        entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_ids[idx]})
        return mpp._allocate_for_entry(campaign, entry, datetime.now(timezone.utc), 1)

    results = _run_concurrently(allocate, 10)
    states = [r["state"] for r in results]
    assert states.count("allocated") == 1
    assert states.count("out_of_stock") == 9
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 1
    assert fake_db["voucher_pools"].count_documents({"status": "available"}) == 0
    assigned = fake_db["campaign_rewards"].find({"status": "assigned"})
    assert len(assigned) == 1


def test_same_winner_allocated_concurrently_gets_exactly_one_voucher(fake_db):
    """The real production hazard: a worker whose lease expired mid-batch is
    still iterating (generation N) while the new owner starts the same entries
    (generation N+1). Twelve such racers on the SAME winner must converge on
    exactly ONE voucher — never twelve codes, and never a second code left
    stranded as `issued` to nobody."""
    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 20)
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    entry_id = _seed_entry(fake_db, 9500, status=mp.ENTRY_STATUS_WINNER, identity_key="tg:9500")

    def allocate(idx):
        entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})
        # Mixed generations, including a stale one racing a fresher one.
        return mpp._allocate_for_entry(campaign, entry, datetime.now(timezone.utc), 1 + (idx % 3))

    results = _run_concurrently(allocate, 12)
    assert all(r["state"] in ("allocated", "already_allocated", "claim_lost") for r in results)
    assert len({r["reward_id"] for r in results}) == 1
    assert fake_db["campaign_rewards"].count_documents({}) == 1

    reward = fake_db["campaign_rewards"].find_one({})
    assert reward["status"] == "assigned"
    assert reward["voucher_code"]
    # Exactly one code is issued, and it is the one bound to the winner.
    issued = fake_db["voucher_pools"].find({"status": "issued"})
    assert len(issued) == 1, f"expected 1 issued code, got {[v['code'] for v in issued]}"
    assert issued[0]["code"] == reward["voucher_code"]
    # No code is stranded as issued-to-nobody.
    assert fake_db["voucher_pools"].count_documents(
        {"status": "issued", "issued_for_reward_id": None}) == 0
    assert fake_db["voucher_pools"].count_documents({"status": "available"}) == 19


def test_two_drawn_codes_for_one_reward_converge_to_exactly_one(fake_db):
    """DETERMINISTIC reproduction of the double-allocation hazard.

    The dangerous interleaving cannot be produced reliably by racing threads,
    so it is staged exactly:

        A claims the reward (generation 1)
        A enters allocate_voucher ...
            ... and while A is inside it, B (generation 2) runs the WHOLE
            allocation to completion: claims, draws code Y, binds Y
        A's draw returns code X
        A tries to bind X

    Two live draws for one reward now exist. The requirement is absolute
    (§58 P0): exactly one code ends up issued, it is the code bound to the
    winner, and the loser's draw goes back to `available` instead of being
    stranded as issued-to-nobody.
    """
    import voucher_pool_service

    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 5)
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    entry_id = _seed_entry(fake_db, 9700, status=mp.ENTRY_STATUS_WINNER, identity_key="tg:9700")
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})

    real_allocate = voucher_pool_service.allocate_voucher
    inner_result = {}
    reentered = {"done": False}

    def allocate_and_let_b_finish(*args, **kwargs):
        code_doc = real_allocate(*args, **kwargs)
        if not reentered["done"]:
            reentered["done"] = True
            # B runs to completion while A is holding an unbound code.
            inner_result["b"] = mpp._allocate_for_entry(
                campaign, entry, datetime.now(timezone.utc), 2
            )
        return code_doc

    with patch.object(voucher_pool_service, "allocate_voucher",
                      side_effect=allocate_and_let_b_finish):
        a_result = mpp._allocate_for_entry(campaign, entry, datetime.now(timezone.utc), 1)

    assert reentered["done"], "the interleaving under test never happened"
    assert inner_result["b"]["state"] == "allocated"
    assert a_result["state"] == "already_allocated"

    reward = fake_db["campaign_rewards"].find_one({})
    assert reward["status"] == "assigned"
    assert reward["voucher_code"]

    issued = fake_db["voucher_pools"].find({"status": "issued"})
    assert len(issued) == 1, (
        f"double allocation: {[v['code'] for v in issued]} issued for one reward"
    )
    assert issued[0]["code"] == reward["voucher_code"]
    # The losing draw went back to inventory, not stranded.
    assert fake_db["voucher_pools"].count_documents({"status": "available"}) == 4
    assert fake_db["voucher_pools"].count_documents(
        {"status": "issued", "issued_for_reward_id": None}) == 0
    assert fake_db["campaign_rewards"].count_documents({}) == 1


def test_concurrent_identity_claims_admit_exactly_one_entry(fake_db):
    """Ten Telegram identities sharing one gaming account, claimed
    concurrently: the unique index admits exactly one."""
    _seed_campaign(fake_db)
    identity = {"identity_type": "gaming_account", "identity_key": "acct:SHARED",
                "account_keys": ["acct:SHARED"]}
    entry_ids = [_seed_entry(fake_db, 9600 + i, offset=i) for i in range(10)]

    def claim(idx):
        return mpp._claim_identity(CAMPAIGN_ID, entry_ids[idx], identity,
                                   datetime.now(timezone.utc))

    results = _run_concurrently(claim, 10)
    wins = [ok for ok, _reason in results]
    assert wins.count(True) == 1
    assert all(reason == mp.REASON_DUPLICATE_GAMING_ACCOUNT
               for ok, reason in results if not ok)
    assert fake_db[mp.IDENTITY_CLAIMS_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "identity_key": "acct:SHARED"}
    ) == 1


def test_stale_worker_cannot_mutate_while_a_fresh_worker_runs(fake_db):
    """§19: after ownership moves on, the previous owner's writes must all
    no-op — this is the guarantee the TTL-only scheduler lock cannot give."""
    _seed_campaign(fake_db)
    now = datetime.now(timezone.utc)
    stale = mpp._claim_campaign(CAMPAIGN_ID, now)
    later = now + timedelta(seconds=mpp.lease_seconds() + 1)
    fresh = mpp._claim_campaign(CAMPAIGN_ID, later)

    def mutate(idx):
        fence = stale if idx % 2 == 0 else fresh
        return fence is fresh, mpp._set_stage(fence, mp.STAGE_SELECTING_WINNERS, later)

    results = _run_concurrently(mutate, 10)
    for is_fresh, ok in results:
        assert ok is is_fresh, "a stale fence must never win a mutation"
