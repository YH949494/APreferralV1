"""Qualification invariants on an isolated LOCAL MongoDB, never app credentials.

QUALIFICATION_TEST_MONGO_URI=mongodb://127.0.0.1:27018/?directConnection=true
python -m pytest -q test_affiliate_qualification_rollout_real_mongo.py
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
import threading
from uuid import uuid4

import pytest
from pymongo import MongoClient
from pymongo.collection import Collection

import affiliate_qualification as aq
import affiliate_qualification_preview as preview
import scheduler
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_welcome_redemption_qualification import (
    CUTOFF, NOW, SOURCE_CONFIG, World, _ReadOnlyDb, _scheduler, _snapshot,
)


@pytest.fixture
def real_db():
    uri = os.getenv("QUALIFICATION_TEST_MONGO_URI")
    if not uri:
        pytest.skip("QUALIFICATION_TEST_MONGO_URI not set (isolated local MongoDB required)")
    assert uri.startswith(("mongodb://127.0.0.1:", "mongodb://localhost:")), "Use isolated local MongoDB only"
    client = MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")  # configured-but-unreachable is a failure, not a skip
    db = client["qualification_audit_" + uuid4().hex]
    try:
        yield db
    finally:
        client.drop_database(db.name)
        client.close()


def evidence(world, invitee, code, account):
    world.join(900, invitee, at=CUTOFF)
    world.welcome_code(invitee, code, issued_at=CUTOFF)
    world.upload([world.row(code, account, NOW - timedelta(hours=1))])
    aq.extract_committed_batches(world.db, config=SOURCE_CONFIG, now_utc=NOW)
    return world.db[aq.EVIDENCE_COLLECTION].find_one({"code_hash": aq.code_hash(code)})


def concurrent_verdicts(world, evidences):
    barrier = threading.Barrier(len(evidences))
    control = aq.get_control(world.db)

    def qualify(ev):
        barrier.wait(timeout=10)
        return aq.evaluate_evidence(world.db, ev, control=control, now_utc=NOW)

    with ThreadPoolExecutor(max_workers=len(evidences)) as pool:
        return list(pool.map(qualify, evidences))


def test_partial_unique_account_index_preserves_legacy_rows(real_db):
    real_db.qualified_events.insert_many([
        {"invitee_id": 1, "referrer_id": 900, "qualified_at": CUTOFF - timedelta(days=1)},
        {"invitee_id": 2, "referrer_id": 900, "qualified_at": CUTOFF - timedelta(days=2)},
    ])
    before = list(real_db.qualified_events.find().sort("_id", 1))
    aq.ensure_indexes(real_db)
    assert aq.index_readiness(real_db)["ok"]
    assert list(real_db.qualified_events.find().sort("_id", 1)) == before


def test_two_invitees_racing_for_one_account_produce_one_credit(real_db):
    world = World(db=real_db)
    evs = [evidence(world, uid, f"AUDIT-{uid}", "000SHARED") for uid in (901, 902)]
    results = concurrent_verdicts(world, evs)
    assert sorted(r["status"] for r in results) == sorted([aq.EV_QUALIFIED, aq.EV_DUPLICATE_ACCOUNT])
    assert real_db.qualified_events.count_documents({}) == 1
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({"state": aq.REG_COMMITTED}) == 1


def test_same_invitee_two_accounts_releases_losing_reservation(real_db):
    world = World(db=real_db)
    first = evidence(world, 901, "AUDIT-ONE", "000ONE")
    real_db.new_joiner_claims.insert_one({"uid": 901, "code": "AUDIT-TWO", "claimed_at": CUTOFF})
    world.upload([world.row("AUDIT-TWO", "000TWO", NOW - timedelta(hours=1))])
    aq.extract_committed_batches(real_db, config=SOURCE_CONFIG, now_utc=NOW)
    second = real_db[aq.EVIDENCE_COLLECTION].find_one({"code_hash": aq.code_hash("AUDIT-TWO")})
    concurrent_verdicts(world, [first, second])
    assert real_db.qualified_events.count_documents({}) == 1
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({}) == 1
    assert real_db[aq.REGISTRY_COLLECTION].find_one()["state"] == aq.REG_COMMITTED


def test_crash_after_reservation_is_recoverable_on_real_mongo(real_db, monkeypatch):
    world = World(db=real_db)
    ev = evidence(world, 901, "AUDIT-CRASH", "000CRASH")
    original = Collection.insert_one

    def interrupt(col, *args, **kwargs):
        if col.full_name == real_db.qualified_events.full_name:
            raise RuntimeError("simulated interruption after reservation")
        return original(col, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Collection, "insert_one", interrupt)
        with pytest.raises(RuntimeError, match="simulated interruption"):
            aq.evaluate_evidence(real_db, ev, control=aq.get_control(real_db), now_utc=NOW)
    assert real_db[aq.REGISTRY_COLLECTION].find_one()["state"] == aq.REG_RESERVED
    result = aq.evaluate_evidence(real_db, ev, control=aq.get_control(real_db), now_utc=NOW)
    assert result["status"] == aq.EV_QUALIFIED
    assert real_db.qualified_events.count_documents({}) == 1
    assert real_db[aq.REGISTRY_COLLECTION].find_one()["state"] == aq.REG_COMMITTED


def test_real_preview_and_migration_dry_run_write_nothing(real_db):
    world = World(db=real_db)
    evidence(world, 901, "AUDIT-READ", "000READ")
    before = _snapshot(real_db)
    result = preview.build_preview(_ReadOnlyDb(real_db), month="202610", now_utc=NOW)
    report = migration.run(_ReadOnlyDb(real_db), apply=False, seed_from_linkage=False, now_utc=NOW)
    assert result["totals"]["at_launch_eligible"] == 1
    assert report["history_unchanged"] and not report["seed_marker_written"]
    assert _snapshot(real_db) == before


def test_real_effects_retry_does_not_repeat_xp_or_qualification(real_db, monkeypatch):
    world = World(db=real_db)
    evidence(world, 901, "AUDIT-EFFECTS", "000EFFECTS")
    aq.process_pending_evidence(real_db, now_utc=NOW)
    monkeypatch.setattr(scheduler, "db", real_db)
    evaluate_tier = scheduler.evaluate_monthly_affiliate_reward

    def interrupted_tier(*args, **kwargs):
        raise RuntimeError("simulated interruption after XP/event writes")

    monkeypatch.setattr(scheduler, "evaluate_monthly_affiliate_reward", interrupted_tier)
    first = scheduler.apply_welcome_redemption_effects(now_utc_ts=NOW)
    assert first["applied"] == 0 and first["errors"] == 1
    assert real_db.xp_events.count_documents({"unique_key": "ref:901"}) == 1
    monkeypatch.setattr(scheduler, "evaluate_monthly_affiliate_reward", evaluate_tier)
    second = scheduler.apply_welcome_redemption_effects(now_utc_ts=NOW + timedelta(minutes=11))
    third = scheduler.apply_welcome_redemption_effects(now_utc_ts=NOW + timedelta(minutes=22))
    assert second["applied"] == 1 and second["errors"] == 0
    assert third["applied"] == 0 and third["errors"] == 0
    assert real_db.xp_events.count_documents({"unique_key": "ref:901"}) == 1
    assert real_db.referral_award_events.count_documents({"award_key": "ref:901"}) == 1
    assert real_db.referral_events.count_documents({"event": "referral_settled", "invitee_id": 901}) == 1
