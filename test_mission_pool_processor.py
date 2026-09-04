"""Tests for mission_pool_processor.py — identity resolution + deduplication,
anti-abuse eligibility, winner selection, atomic voucher allocation, reward
idempotency, notification state separation, fencing and crash recovery
(spec §48 unit + §49 integration).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import database
import mission_pool as mp
import mission_pool_processor as mpp
import voucher_pool_service as vps
from fake_mongo import FakeDb

CAMPAIGN_ID = "mission-pilot-1"
POOL_ID = "MISSION-PILOT"


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
        "voucher_pools": [("pool_id", "code")],
        # Mirrors the production partial unique indexes created by
        # mission_pool_processor.ensure_mission_reward_indexes().
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


def _seed_campaign(fake_db, *, winner_count=2, allocation_method="random_qualified",
                   policy=None, stage=mp.STAGE_PENDING, cancelled=False, status="ended"):
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": CAMPAIGN_ID,
        "name": "Pilot Mission",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": status,
        "schedule": {"starts_at": now - timedelta(hours=3), "ends_at": now - timedelta(hours=1)},
        "mission_config": {"mission_type": "keyword", "prompt": "p", "correct_answer": "x"},
        "mission_pool": {
            "pool_id": POOL_ID,
            "pool_type": "voucher_drop",
            "winner_count": winner_count,
            "allocation_method": allocation_method,
            "eligibility_policy": policy or dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": cancelled,
            "processing_stage": stage,
            "processing_generation": 0,
        },
    })


def _seed_user(fake_db, uid, **fields):
    doc = {"user_id": uid}
    doc.update(fields)
    fake_db["users"].insert_one(doc)


def _seed_entry(fake_db, uid, *, is_correct=True, offset_seconds=0, status=None):
    submitted = datetime.now(timezone.utc) - timedelta(hours=2) + timedelta(seconds=offset_seconds)
    return fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID,
        "telegram_user_id": uid,
        "answer": "x",
        "answer_normalized": "x",
        "is_correct": is_correct,
        "status": status or mp.ENTRY_STATUS_SUBMITTED,
        "identity_key": None,
        "identity_type": None,
        "disqualification_reason": None,
        "reward_id": None,
        "submitted_at": submitted,
        "created_at": submitted,
        "updated_at": submitted,
    }).inserted_id


def _seed_pool(fake_db, count):
    fake_db["voucher_pool_registry"].insert_one({
        "pool_id": POOL_ID, "name": "Pilot", "pool_type": "voucher_drop",
        "allocation_scope": "campaign_rewards", "status": "active",
    })
    for i in range(count):
        fake_db["voucher_pools"].insert_one({
            "pool_id": POOL_ID, "code": f"CODE{i:03d}", "status": "available",
            "issued_to": None, "issued_at": None,
            "pool_source": "campaign_centre", "pool_type": "voucher_drop",
            "allocation_scope": "campaign_rewards",
        })


def _no_telegram(ok=True, err=None, blocked=False):
    return patch("telegram_utils.send_telegram_http_message", return_value=(ok, err, blocked))


def _entries(fake_db, **q):
    return fake_db[mp.ENTRIES_COLLECTION].find({"campaign_id": CAMPAIGN_ID, **q})


# ---------------------------------------------------------------------------
# §13 identity resolution precedence
# ---------------------------------------------------------------------------

def test_identity_prefers_gaming_account_then_telegram():
    with_account = mpp.resolve_identity({"linked_gaming_accounts": ["ZZZ9", "ABC1"]}, 123)
    assert with_account["identity_type"] == "gaming_account"
    # Deterministic primary: lexicographically smallest account id.
    assert with_account["identity_key"] == "acct:ABC1"
    assert with_account["account_keys"] == ["acct:ABC1", "acct:ZZZ9"]

    fallback = mpp.resolve_identity({"linked_gaming_accounts": []}, 123)
    assert fallback == {"identity_type": "telegram", "identity_key": "tg:123",
                        "account_keys": ["tg:123"]}
    assert mpp.resolve_identity(None, 123)["identity_type"] == "telegram"


def test_identity_ignores_blank_and_duplicate_account_ids():
    out = mpp.resolve_identity({"linked_gaming_accounts": ["A", "  ", "A", "", None, 7]}, 1)
    assert out["account_keys"] == ["acct:A"]


# ---------------------------------------------------------------------------
# §14 duplicate identity behaviour
# ---------------------------------------------------------------------------

def test_four_telegram_accounts_on_one_gaming_account_yield_one_qualified(fake_db):
    """TG_A..TG_D -> gaming_account_123 must produce exactly ONE eligible
    lottery identity, not four entries."""
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 10)
    for i, uid in enumerate([201, 202, 203, 204]):
        _seed_user(fake_db, uid, linked_gaming_accounts=["ACCT123"])
        _seed_entry(fake_db, uid, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    qualified = _entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)
    assert len(qualified) == 1
    # Earliest valid submission wins (§14).
    assert qualified[0]["telegram_user_id"] == 201
    dq = _entries(fake_db, status=mp.ENTRY_STATUS_DISQUALIFIED)
    assert {d["telegram_user_id"] for d in dq} == {202, 203, 204}
    assert {d["disqualification_reason"] for d in dq} == {mp.REASON_DUPLICATE_GAMING_ACCOUNT}
    # ...and only one voucher left the inventory.
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 1


def test_shared_account_detected_even_when_lists_differ(fake_db):
    """Two Telegram identities whose UIM cluster lists overlap only partially
    still collide, because every linked account is claimed, not just the
    primary one."""
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 10)
    _seed_user(fake_db, 301, linked_gaming_accounts=["AAA", "SHARED"])
    _seed_user(fake_db, 302, linked_gaming_accounts=["SHARED", "ZZZ"])
    _seed_entry(fake_db, 301, offset_seconds=0)
    _seed_entry(fake_db, 302, offset_seconds=5)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)) == 1
    loser = _entries(fake_db, telegram_user_id=302)[0]
    assert loser["disqualification_reason"] == mp.REASON_DUPLICATE_GAMING_ACCOUNT


def test_telegram_fallback_identity_is_still_deduplicated(fake_db):
    _seed_campaign(fake_db, winner_count=5)
    _seed_pool(fake_db, 5)
    _seed_user(fake_db, 401)
    _seed_entry(fake_db, 401)
    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)
    claims = fake_db[mp.IDENTITY_CLAIMS_COLLECTION].find({})
    assert [c["identity_key"] for c in claims] == ["tg:401"]


# ---------------------------------------------------------------------------
# §15/§16 anti-abuse eligibility (quality gate only, no probability)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("user_fields,expected", [
    ({"blocked": True}, mp.REASON_BLOCKED),
    ({"multi_account_voucher_hunter": True}, mp.REASON_VOUCHER_HUNTER),
    ({"for_bot_segment": "voucher_hunter"}, mp.REASON_VOUCHER_HUNTER),
    ({"multi_account_risk": True}, mp.REASON_MULTI_ACCOUNT_RISK),
    ({}, None),
    ({"for_bot_segment": "high_value"}, None),
])
def test_quality_eligibility_uses_canonical_production_flags(user_fields, expected):
    entry = {"is_correct": True}
    policy = dict(mp.DEFAULT_ELIGIBILITY_POLICY)
    assert mpp.evaluate_quality_eligibility(entry, user_fields, policy) == expected


def test_incorrect_answer_disqualifies_when_policy_requires_it():
    policy = dict(mp.DEFAULT_ELIGIBILITY_POLICY)
    assert mpp.evaluate_quality_eligibility({"is_correct": False}, {}, policy) == mp.REASON_INCORRECT_ANSWER
    lenient = {**policy, "require_correct_answer": False}
    assert mpp.evaluate_quality_eligibility({"is_correct": False}, {}, lenient) is None


def test_missing_gaming_account_only_disqualifies_when_required():
    policy = dict(mp.DEFAULT_ELIGIBILITY_POLICY)
    assert mpp.evaluate_quality_eligibility({"is_correct": True}, {}, policy) is None
    strict = {**policy, "require_gaming_account": True}
    assert mpp.evaluate_quality_eligibility({"is_correct": True}, {}, strict) == mp.REASON_MISSING_GAMING_ACCOUNT


def test_no_segment_probability_gate_is_applied(fake_db):
    """§16: Mission Pool must NOT inherit Standard Drop's probabilistic
    admission. A `normal` and a `high_value` user with identical answers must
    both qualify — participation is the gate, not odds."""
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 10)
    _seed_user(fake_db, 501, for_bot_segment="normal_actual", bot_segment_probability=0.02)
    _seed_user(fake_db, 502, for_bot_segment="high_value", bot_segment_probability=0.9)
    _seed_entry(fake_db, 501, offset_seconds=0)
    _seed_entry(fake_db, 502, offset_seconds=1)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)) == 2


def test_voucher_hunter_and_risk_exclusions_end_to_end(fake_db):
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 10)
    _seed_user(fake_db, 601)
    _seed_user(fake_db, 602, multi_account_voucher_hunter=True)
    _seed_user(fake_db, 603, multi_account_risk=True)
    _seed_user(fake_db, 604, blocked=True)
    for i, uid in enumerate([601, 602, 603, 604]):
        _seed_entry(fake_db, uid, offset_seconds=i)
    _seed_entry(fake_db, 605, is_correct=False, offset_seconds=9)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    by_uid = {e["telegram_user_id"]: e for e in _entries(fake_db)}
    assert by_uid[601]["status"] == mp.ENTRY_STATUS_REWARD_ALLOCATED
    assert by_uid[602]["disqualification_reason"] == mp.REASON_VOUCHER_HUNTER
    assert by_uid[603]["disqualification_reason"] == mp.REASON_MULTI_ACCOUNT_RISK
    assert by_uid[604]["disqualification_reason"] == mp.REASON_BLOCKED
    assert by_uid[605]["disqualification_reason"] == mp.REASON_INCORRECT_ANSWER
    # Every reason is machine-readable, never free text.
    for entry in _entries(fake_db, status=mp.ENTRY_STATUS_DISQUALIFIED):
        assert entry["disqualification_reason"] in mp.DISQUALIFICATION_REASONS


# ---------------------------------------------------------------------------
# §20/§21 winner selection
# ---------------------------------------------------------------------------

def test_winner_count_is_capped_and_seed_is_stored(fake_db):
    _seed_campaign(fake_db, winner_count=3)
    _seed_pool(fake_db, 10)
    for i in range(10):
        _seed_user(fake_db, 700 + i)
        _seed_entry(fake_db, 700 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["qualified_count"] == 10
    assert block["winner_count_requested"] == 3
    assert block["winner_count_actual"] == 3
    assert block["selection_seed"]
    assert block["selection_started_at"] and block["selection_completed_at"]
    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)) == 3
    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_NON_WINNER)) == 7


def test_fewer_qualified_than_winner_count_awards_all(fake_db):
    _seed_campaign(fake_db, winner_count=50)
    _seed_pool(fake_db, 50)
    for i in range(4):
        _seed_user(fake_db, 800 + i)
        _seed_entry(fake_db, 800 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["winner_count_requested"] == 50
    assert block["winner_count_actual"] == 4
    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)) == 4


def test_zero_qualified_participants_completes_cleanly(fake_db):
    _seed_campaign(fake_db, winner_count=5)
    _seed_pool(fake_db, 5)
    for i in range(3):
        _seed_entry(fake_db, 900 + i, is_correct=False, offset_seconds=i)

    with _no_telegram():
        result = mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] == mp.STAGE_COMPLETED
    assert block["winner_count_actual"] == 0
    assert result["completed"] is True
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 0


def test_first_qualified_allocation_is_submission_ordered(fake_db):
    _seed_campaign(fake_db, winner_count=2, allocation_method="first_qualified")
    _seed_pool(fake_db, 5)
    for i, uid in enumerate([1001, 1002, 1003, 1004]):
        _seed_user(fake_db, uid)
        _seed_entry(fake_db, uid, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    winners = {e["telegram_user_id"] for e in _entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)}
    assert winners == {1001, 1002}


def test_selection_is_reproducible_from_the_stored_seed(fake_db):
    """§21: given the frozen qualified set + the stored seed, the winner set
    is exactly recomputable for an internal audit."""
    import random as _random

    _seed_campaign(fake_db, winner_count=3)
    _seed_pool(fake_db, 10)
    for i in range(8):
        _seed_user(fake_db, 1100 + i)
        _seed_entry(fake_db, 1100 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    ordered = [e["_id"] for e in fake_db[mp.ENTRIES_COLLECTION].find(
        {"campaign_id": CAMPAIGN_ID, "status": {"$in": [
            mp.ENTRY_STATUS_REWARD_ALLOCATED, mp.ENTRY_STATUS_NON_WINNER]}},
        sort=[("submitted_at", 1), ("_id", 1)],
    )]
    shuffled = list(ordered)
    _random.Random(block["selection_seed"]).shuffle(shuffled)
    expected = set(shuffled[:3])
    actual = {e["_id"] for e in _entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)}
    assert actual == expected


def test_reprocessing_never_reshuffles_or_reallocates(fake_db):
    """§34: manual retry must not produce a second winner set or a second
    voucher."""
    _seed_campaign(fake_db, winner_count=3)
    _seed_pool(fake_db, 20)
    for i in range(10):
        _seed_user(fake_db, 1200 + i)
        _seed_entry(fake_db, 1200 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)
    first_winners = {e["telegram_user_id"] for e in _entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)}
    first_codes = {v["code"] for v in fake_db["voucher_pools"].find({"status": "issued"})}
    seed = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]["selection_seed"]

    for _ in range(3):
        with _no_telegram():
            mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["selection_seed"] == seed
    assert {e["telegram_user_id"] for e in _entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)} == first_winners
    assert {v["code"] for v in fake_db["voucher_pools"].find({"status": "issued"})} == first_codes
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 3
    assert fake_db["campaign_rewards"].count_documents({"category": "mission_pool"}) == 3


# ---------------------------------------------------------------------------
# §22-§25 allocation + reward idempotency
# ---------------------------------------------------------------------------

def test_rewards_land_in_campaign_rewards_not_a_new_collection(fake_db):
    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 3)
    _seed_user(fake_db, 1301)
    _seed_entry(fake_db, 1301)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    rewards = fake_db["campaign_rewards"].find({})
    assert len(rewards) == 1
    reward = rewards[0]
    assert reward["category"] == "mission_pool"
    assert reward["status"] == "assigned"
    assert reward["voucher_code"].startswith("CODE")
    assert reward["idempotency_key"] == mp.reward_idempotency_key(CAMPAIGN_ID, reward["mission_entry_id"])
    # No parallel Mission-only inventory was created.
    assert "mission_vouchers" not in fake_db._collections
    assert "mission_inventory" not in fake_db._collections


def test_inventory_shortage_marks_out_of_stock_and_never_goes_negative(fake_db):
    _seed_campaign(fake_db, winner_count=5)
    _seed_pool(fake_db, 2)
    for i in range(5):
        _seed_user(fake_db, 1400 + i)
        _seed_entry(fake_db, 1400 + i, offset_seconds=i)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 2
    assert fake_db["voucher_pools"].count_documents({"status": "available"}) == 0
    allocated = _entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)
    assert len(allocated) == 2
    starved = _entries(fake_db, disqualification_reason=mp.REASON_OUT_OF_STOCK)
    assert len(starved) == 3


def test_same_winner_allocation_retry_returns_the_same_code(fake_db):
    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 5)
    _seed_user(fake_db, 1501)
    entry_id = _seed_entry(fake_db, 1501)

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)
    entry = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": entry_id})
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    first = fake_db["campaign_rewards"].find_one({})["voucher_code"]

    for _ in range(5):
        out = mpp._allocate_for_entry(campaign, entry, datetime.now(timezone.utc), 1)
        assert out["state"] == "already_allocated"

    assert fake_db["campaign_rewards"].count_documents({}) == 1
    assert fake_db["campaign_rewards"].find_one({})["voucher_code"] == first
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 1


def test_one_identity_cannot_hold_two_mission_rewards(fake_db):
    """The (campaign_id, identity_key) partial unique index is the final
    protection even if two entries somehow both reached winner state."""
    _seed_campaign(fake_db, winner_count=5)
    _seed_pool(fake_db, 5)
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    now = datetime.now(timezone.utc)

    e1 = _seed_entry(fake_db, 1601, offset_seconds=0, status=mp.ENTRY_STATUS_WINNER)
    e2 = _seed_entry(fake_db, 1602, offset_seconds=1, status=mp.ENTRY_STATUS_WINNER)
    for eid in (e1, e2):
        fake_db[mp.ENTRIES_COLLECTION].update_one(
            {"_id": eid}, {"$set": {"identity_key": "acct:SAME", "identity_type": "gaming_account"}}
        )

    first = mpp._allocate_for_entry(campaign, fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": e1}), now, 1)
    second = mpp._allocate_for_entry(campaign, fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": e2}), now, 1)
    assert first["state"] == "allocated"
    assert second["state"] == "duplicate_identity"
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 1


# ---------------------------------------------------------------------------
# §26/§27 allocation vs notification separation
# ---------------------------------------------------------------------------

def test_telegram_failure_never_releases_or_reassigns_the_voucher(fake_db):
    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 3)
    _seed_user(fake_db, 1701)
    _seed_entry(fake_db, 1701)

    with _no_telegram(ok=False, err="rate_limited"):
        mpp.process_campaign(CAMPAIGN_ID)

    reward = fake_db["campaign_rewards"].find_one({})
    assert reward["status"] == "assigned"          # voucher still owned
    assert reward["voucher_code"]
    assert reward["notification_status"] == "failed_retryable"
    assert reward["notification_attempts"] == 1
    assert reward["winner_popup_pending"] is True
    # The code stays issued to this winner, not returned to inventory.
    issued = fake_db["voucher_pools"].find({"status": "issued"})
    assert len(issued) == 1
    assert issued[0]["code"] == reward["voucher_code"]
    assert fake_db["voucher_pools"].count_documents({"status": "available"}) == 2
    # Campaign is NOT completed while a retryable send is outstanding.
    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] == mp.STAGE_NOTIFYING


def test_blocked_bot_is_a_terminal_notification_failure(fake_db):
    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 3)
    _seed_user(fake_db, 1801)
    _seed_entry(fake_db, 1801)

    with _no_telegram(ok=False, err="bot_blocked", blocked=True):
        mpp.process_campaign(CAMPAIGN_ID)

    reward = fake_db["campaign_rewards"].find_one({})
    assert reward["notification_status"] == "failed_terminal"
    assert reward["status"] == "assigned"
    # A terminal failure lets the campaign finish; the reward is still
    # discoverable through Campaign Rewards.
    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] == mp.STAGE_COMPLETED


def test_notification_retry_eventually_succeeds_without_touching_the_voucher(fake_db):
    _seed_campaign(fake_db, winner_count=1)
    _seed_pool(fake_db, 3)
    _seed_user(fake_db, 1901)
    _seed_entry(fake_db, 1901)

    with _no_telegram(ok=False, err="rate_limited"):
        mpp.process_campaign(CAMPAIGN_ID)
    code_after_failure = fake_db["campaign_rewards"].find_one({})["voucher_code"]

    # Make the retry due, then let the send succeed.
    fake_db["campaign_rewards"].update_one(
        {"campaign_id": CAMPAIGN_ID},
        {"$set": {"notification_next_attempt_at": datetime.now(timezone.utc) - timedelta(seconds=1)}},
    )
    with _no_telegram(ok=True):
        mpp.process_campaign(CAMPAIGN_ID)

    reward = fake_db["campaign_rewards"].find_one({})
    assert reward["notification_status"] == "sent"
    assert reward["voucher_code"] == code_after_failure
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 1
    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] == mp.STAGE_COMPLETED


def test_a_telegram_exception_does_not_abort_the_batch(fake_db):
    _seed_campaign(fake_db, winner_count=2)
    _seed_pool(fake_db, 5)
    for i, uid in enumerate([2001, 2002]):
        _seed_user(fake_db, uid)
        _seed_entry(fake_db, uid, offset_seconds=i)

    with patch("telegram_utils.send_telegram_http_message", side_effect=RuntimeError("boom")):
        mpp.process_campaign(CAMPAIGN_ID)

    rewards = fake_db["campaign_rewards"].find({})
    assert len(rewards) == 2
    assert all(r["status"] == "assigned" for r in rewards)
    assert all(r["notification_status"] == "failed_retryable" for r in rewards)


# ---------------------------------------------------------------------------
# §19 fencing / stale worker
# ---------------------------------------------------------------------------

def test_second_worker_cannot_claim_a_live_lease(fake_db):
    _seed_campaign(fake_db)
    now = datetime.now(timezone.utc)
    first = mpp._claim_campaign(CAMPAIGN_ID, now)
    assert first is not None
    assert mpp._claim_campaign(CAMPAIGN_ID, now) is None


def test_generation_increments_and_invalidates_the_previous_owner(fake_db):
    _seed_campaign(fake_db)
    now = datetime.now(timezone.utc)
    stale = mpp._claim_campaign(CAMPAIGN_ID, now)
    # Lease expires; a second worker takes over.
    later = now + timedelta(seconds=mpp.lease_seconds() + 1)
    fresh = mpp._claim_campaign(CAMPAIGN_ID, later)
    assert fresh is not None
    assert fresh.generation == stale.generation + 1

    # The stale worker can no longer mutate anything.
    assert mpp._still_owner(stale) is False
    assert mpp._renew(stale, later) is False
    assert mpp._set_stage(stale, mp.STAGE_COMPLETED, later) is False
    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] != mp.STAGE_COMPLETED
    assert mpp._still_owner(fresh) is True


def test_process_campaign_skips_when_another_worker_holds_the_lease(fake_db):
    _seed_campaign(fake_db)
    mpp._claim_campaign(CAMPAIGN_ID, datetime.now(timezone.utc))
    assert mpp.process_campaign(CAMPAIGN_ID) == {"skipped": "not_owner"}


def test_lease_is_released_even_when_a_stage_raises(fake_db):
    _seed_campaign(fake_db)
    _seed_entry(fake_db, 2101)
    with patch.object(mpp, "_eligibility_pass", side_effect=RuntimeError("crash")):
        with pytest.raises(RuntimeError):
            mpp.process_campaign(CAMPAIGN_ID)
    # A later worker can take over immediately rather than waiting out a TTL.
    assert mpp._claim_campaign(CAMPAIGN_ID, datetime.now(timezone.utc)) is not None


# ---------------------------------------------------------------------------
# §47 crash recovery per stage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("crash_at", [
    "_eligibility_pass", "_select_winners", "_allocation_pass", "_notification_pass",
])
def test_crash_at_any_stage_resumes_without_duplicating_rewards(fake_db, crash_at):
    _seed_campaign(fake_db, winner_count=3)
    _seed_pool(fake_db, 10)
    for i in range(6):
        _seed_user(fake_db, 2200 + i)
        _seed_entry(fake_db, 2200 + i, offset_seconds=i)

    with patch.object(mpp, crash_at, side_effect=RuntimeError("worker died")):
        with pytest.raises(RuntimeError):
            mpp.process_campaign(CAMPAIGN_ID)

    # A fresh worker picks up from the persisted stage and finishes.
    with _no_telegram():
        for _ in range(3):
            mpp.process_campaign(CAMPAIGN_ID)

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    assert block["processing_stage"] == mp.STAGE_COMPLETED
    assert block["winner_count_actual"] == 3
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 3
    assert fake_db["campaign_rewards"].count_documents({"category": "mission_pool"}) == 3
    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)) == 3


def test_partial_eligibility_batch_resumes_from_remaining_submitted(fake_db):
    _seed_campaign(fake_db, winner_count=10, stage=mp.STAGE_PROCESSING_ELIGIBILITY)
    _seed_pool(fake_db, 10)
    for i in range(5):
        _seed_user(fake_db, 2300 + i)
        _seed_entry(fake_db, 2300 + i, offset_seconds=i)
    # Simulate a crash after two entries were decided.
    for uid in (2300, 2301):
        fake_db[mp.ENTRIES_COLLECTION].update_one(
            {"telegram_user_id": uid},
            {"$set": {"status": mp.ENTRY_STATUS_QUALIFIED, "identity_key": f"tg:{uid}",
                      "identity_type": "telegram"}},
        )
        fake_db[mp.IDENTITY_CLAIMS_COLLECTION].insert_one(
            {"campaign_id": CAMPAIGN_ID, "identity_key": f"tg:{uid}", "entry_id": uid}
        )

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    assert len(_entries(fake_db, status=mp.ENTRY_STATUS_REWARD_ALLOCATED)) == 5
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 5


# ---------------------------------------------------------------------------
# §30 kill switch during processing
# ---------------------------------------------------------------------------

def test_cancel_stops_new_allocation_but_keeps_issued_rewards(fake_db):
    _seed_campaign(fake_db, winner_count=5)
    _seed_pool(fake_db, 10)
    for i in range(5):
        _seed_user(fake_db, 2400 + i)
        _seed_entry(fake_db, 2400 + i, offset_seconds=i)

    # Allocate one winner, then cancel mid-flight.
    original = mpp._allocate_for_entry
    calls = {"n": 0}

    def cancel_after_first(campaign, entry, now, generation):
        out = original(campaign, entry, now, generation)
        calls["n"] += 1
        if calls["n"] == 1:
            fake_db["gc_campaigns"].update_one(
                {"campaign_id": CAMPAIGN_ID}, {"$set": {"mission_pool.cancelled": True}}
            )
        return out

    with _no_telegram(), patch.object(mpp, "_allocate_for_entry", side_effect=cancel_after_first):
        mpp.process_campaign(CAMPAIGN_ID)

    issued = fake_db["voucher_pools"].count_documents({"status": "issued"})
    assert 1 <= issued <= mpp.allocation_batch_size()
    # Already-issued rewards survive the cancel.
    assert fake_db["campaign_rewards"].count_documents({"status": "assigned"}) == issued
    # And no further processing happens.
    assert mpp.process_campaign(CAMPAIGN_ID) == {"skipped": "not_closed"}
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == issued


def test_paused_campaign_is_not_processed(fake_db):
    _seed_campaign(fake_db, status="paused")
    _seed_entry(fake_db, 2501)
    assert mpp.process_campaign(CAMPAIGN_ID) == {"skipped": "not_closed"}


def test_disabled_feature_flag_stops_the_processor(fake_db, monkeypatch):
    _seed_campaign(fake_db)
    _seed_entry(fake_db, 2601)
    monkeypatch.setattr(mpp.mp, "mission_pool_enabled", lambda: False)
    assert mpp.process_campaign(CAMPAIGN_ID) == {"skipped": "mission_pool_disabled"}
    assert mpp.run_mission_pool_processor() == {"skipped": "disabled"}


def test_find_due_campaigns_is_bounded_and_skips_completed(fake_db, monkeypatch):
    now = datetime.now(timezone.utc)
    for i in range(8):
        fake_db["gc_campaigns"].insert_one({
            "campaign_id": f"c{i}", "type": "mission_pool", "mechanic": "mission_pool",
            "status": "ended",
            "schedule": {"starts_at": now - timedelta(hours=2), "ends_at": now - timedelta(hours=1)},
            "mission_pool": {"processing_stage": mp.STAGE_PENDING, "cancelled": False},
        })
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "done", "type": "mission_pool", "mechanic": "mission_pool", "status": "ended",
        "schedule": {"starts_at": now - timedelta(hours=2), "ends_at": now - timedelta(hours=1)},
        "mission_pool": {"processing_stage": mp.STAGE_COMPLETED, "cancelled": False},
    })
    # A standard-drop campaign must never be picked up.
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "tourney", "type": "tournament", "status": "ended",
        "schedule": {"starts_at": now - timedelta(hours=2), "ends_at": now - timedelta(hours=1)},
    })

    monkeypatch.setenv("MISSION_POOL_PROCESSOR_MAX_CAMPAIGNS", "3")
    due = mpp.find_due_campaigns(now)
    assert len(due) == 3
    assert "done" not in due
    assert "tourney" not in due


def test_batch_sizes_are_bounded_by_env_clamps(monkeypatch):
    monkeypatch.setenv("MISSION_ELIGIBILITY_BATCH_SIZE", "999999")
    assert mpp.eligibility_batch_size() == 1000
    monkeypatch.setenv("MISSION_ELIGIBILITY_BATCH_SIZE", "-5")
    assert mpp.eligibility_batch_size() == 1
    monkeypatch.setenv("MISSION_ELIGIBILITY_BATCH_SIZE", "not-a-number")
    assert mpp.eligibility_batch_size() == 200


# ---------------------------------------------------------------------------
# Codex review follow-ups
# ---------------------------------------------------------------------------

def test_entries_submitted_after_the_close_cutoff_are_disqualified(fake_db):
    """The submission endpoint's state re-check is a read followed by an
    insert, so a close landing in that window can still let a row be written.
    The processor is where that is made harmless: submitted_at is stamped
    server-side, so a late entry is disqualified and can never win."""
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 10)
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    ends_at = campaign["schedule"]["ends_at"]

    _seed_user(fake_db, 3001)
    _seed_user(fake_db, 3002)
    in_time = _seed_entry(fake_db, 3001)
    late = _seed_entry(fake_db, 3002)
    fake_db[mp.ENTRIES_COLLECTION].update_one(
        {"_id": in_time}, {"$set": {"submitted_at": ends_at - timedelta(seconds=1)}})
    fake_db[mp.ENTRIES_COLLECTION].update_one(
        {"_id": late}, {"$set": {"submitted_at": ends_at + timedelta(seconds=1)}})

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    assert fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": in_time})["status"] == \
        mp.ENTRY_STATUS_REWARD_ALLOCATED
    late_doc = fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": late})
    assert late_doc["status"] == mp.ENTRY_STATUS_DISQUALIFIED
    assert late_doc["disqualification_reason"] == mp.REASON_SUBMITTED_AFTER_CLOSE
    assert fake_db["voucher_pools"].count_documents({"status": "issued"}) == 1


def test_an_early_admin_close_moves_the_cutoff_earlier(fake_db):
    """closed_at wins over a later scheduled ends_at."""
    _seed_campaign(fake_db, winner_count=10)
    _seed_pool(fake_db, 10)
    campaign = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    ends_at = campaign["schedule"]["ends_at"]
    closed_at = ends_at - timedelta(minutes=30)
    fake_db["gc_campaigns"].update_one(
        {"campaign_id": CAMPAIGN_ID}, {"$set": {"mission_pool.closed_at": closed_at}})

    _seed_user(fake_db, 3101)
    _seed_user(fake_db, 3102)
    before = _seed_entry(fake_db, 3101)
    after = _seed_entry(fake_db, 3102)
    fake_db[mp.ENTRIES_COLLECTION].update_one(
        {"_id": before}, {"$set": {"submitted_at": closed_at - timedelta(seconds=1)}})
    # Inside the scheduled window, but after the admin closed it.
    fake_db[mp.ENTRIES_COLLECTION].update_one(
        {"_id": after}, {"$set": {"submitted_at": closed_at + timedelta(seconds=1)}})

    with _no_telegram():
        mpp.process_campaign(CAMPAIGN_ID)

    assert fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": before})["status"] == \
        mp.ENTRY_STATUS_REWARD_ALLOCATED
    assert fake_db[mp.ENTRIES_COLLECTION].find_one({"_id": after})["disqualification_reason"] == \
        mp.REASON_SUBMITTED_AFTER_CLOSE


def test_close_cutoff_picks_the_earlier_of_ends_at_and_closed_at():
    now = datetime.now(timezone.utc)
    early, late = now - timedelta(hours=1), now
    assert mp.close_cutoff({"schedule": {"ends_at": late},
                            "mission_pool": {"closed_at": early}}) == early
    assert mp.close_cutoff({"schedule": {"ends_at": early},
                            "mission_pool": {"closed_at": late}}) == early
    assert mp.close_cutoff({"schedule": {"ends_at": late}}) == late
    assert mp.close_cutoff({}) is None


def test_open_campaigns_cannot_starve_a_closed_one_out_of_the_tick(fake_db, monkeypatch):
    """The closed predicate must live in the query, not in a Python filter
    after the cursor limit. A live mission with no ends_at sorts FIRST in
    Mongo (missing values order before dates ascending), so with the limit
    applied first these would push the one due campaign out of every tick."""
    now = datetime.now(timezone.utc)
    for i in range(30):
        fake_db["gc_campaigns"].insert_one({
            "campaign_id": f"open{i}", "type": "mission_pool", "mechanic": "mission_pool",
            "status": "live",
            "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": None},
            "mission_pool": {"processing_stage": mp.STAGE_PENDING, "cancelled": False},
        })
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "paused-one", "type": "mission_pool", "mechanic": "mission_pool",
        "status": "paused",
        "schedule": {"starts_at": now - timedelta(hours=3), "ends_at": now - timedelta(hours=2)},
        "mission_pool": {"processing_stage": mp.STAGE_PENDING, "cancelled": False},
    })
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "the-due-one", "type": "mission_pool", "mechanic": "mission_pool",
        "status": "ended",
        "schedule": {"starts_at": now - timedelta(hours=3), "ends_at": now - timedelta(hours=1)},
        "mission_pool": {"processing_stage": mp.STAGE_PENDING, "cancelled": False},
    })

    monkeypatch.setenv("MISSION_POOL_PROCESSOR_MAX_CAMPAIGNS", "3")
    due = mpp.find_due_campaigns(now)
    assert "the-due-one" in due
    assert "paused-one" not in due
    assert not any(c.startswith("open") for c in due)


def test_a_live_campaign_past_its_window_is_still_due(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["gc_campaigns"].insert_one({
        "campaign_id": "elapsed", "type": "mission_pool", "mechanic": "mission_pool",
        "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=3), "ends_at": now - timedelta(minutes=1)},
        "mission_pool": {"processing_stage": mp.STAGE_PENDING, "cancelled": False},
    })
    assert mpp.find_due_campaigns(now) == ["elapsed"]
