"""Snapshot write-amplification fix against a real mongod.

Run against disposable localhost MongoDB via QUALIFICATION_TEST_MONGO_URI
(same fixture as the other *_real_mongo suites). The $expr / $nin /
pipeline-update semantics the referral publish now relies on are exactly what
fakes cannot prove, so they are checked here against an independent ledger
oracle, plus oplog volume when the server is a replica set.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest
from pymongo.errors import BulkWriteError

import scheduler
import xp_snapshot as snap
from config import KL_TZ
from test_affiliate_qualification_rollout_real_mongo import real_db  # noqa: F401  (fixture)

# Wed 2026-10-14 12:00 KL. Week starts Mon 2026-10-12 00:00 KL (= Sun 16:00 UTC).
NOW = datetime(2026, 10, 14, 4, 0, tzinfo=timezone.utc)
WEEK_START = datetime(2026, 10, 12, 0, 0, tzinfo=KL_TZ).astimezone(timezone.utc)
MONTH_START = datetime(2026, 10, 1, 0, 0, tzinfo=KL_TZ).astimezone(timezone.utc)
FIELDS = ("weekly_referrals", "monthly_referrals", "total_referrals")
NEXT = tuple(f + "_next" for f in FIELDS)


@pytest.fixture
def world(real_db, monkeypatch):  # noqa: F811
    monkeypatch.setattr(scheduler, "db", real_db)
    monkeypatch.setattr(scheduler, "now_utc", lambda: NOW)
    real_db.users.create_index("user_id", unique=True)
    return real_db


def _ev(inviter, invitee, event, at, **extra):
    return {"inviter_id": inviter, "invitee_id": invitee, "event": event, "occurred_at": at, **extra}


def _seed(db):
    # users 1..12; 99 is an inviter with no users doc yet (upsert path)
    db.users.insert_many([{"user_id": uid, "username": f"u{uid}"} for uid in range(1, 13)])
    # previous-run leftovers: live + staging counts that the ledger no longer supports
    db.users.update_one({"user_id": 6}, {"$set": dict(zip(FIELDS + NEXT, (2, 3, 4, 2, 3, 4)))})
    db.users.update_one({"user_id": 7}, {"$set": {"weekly_referrals": 5}})  # live only, no staging
    sunday_2359_kl = WEEK_START - timedelta(minutes=1)
    db.referral_events.insert_many([
        _ev(1, 101, "referral_settled", NOW - timedelta(hours=1)),        # this week
        _ev(1, 102, "referral_settled", MONTH_START + timedelta(days=1)),  # this month, not week
        _ev(1, 103, "referral_settled", NOW - timedelta(days=90)),         # lifetime only
        _ev(2, 201, "referral_settled", WEEK_START),                       # Mon 00:00 KL -> this week
        _ev(3, 301, "referral_settled", sunday_2359_kl),                   # Sun 23:59 KL -> last week
        _ev(4, 401, "referral_settled", NOW - timedelta(hours=2)),
        _ev(4, 401, "referral_revoked", NOW - timedelta(hours=1)),         # nets to zero
        _ev(5, 501, "referral_revoked", NOW - timedelta(hours=1)),         # negative net -> clamp 0
        _ev(6, 601, "referral_settled", NOW - timedelta(hours=1), invalidated=True),  # drops out
        _ev(99, 9901, "referral_settled", NOW - timedelta(hours=3)),
    ])


def _oracle(db):
    out = defaultdict(lambda: [0, 0, 0])
    for e in db.referral_events.find():
        if e.get("invalidated") is True or e.get("inviter_id") is None:
            continue
        sign = {"referral_settled": 1, "referral_revoked": -1}.get(e["event"], 0)
        at = e["occurred_at"]
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        row = out[e["inviter_id"]]
        row[2] += sign
        if WEEK_START <= at < WEEK_START + timedelta(days=7):
            row[0] += sign
        if at >= MONTH_START:
            row[1] += sign
    return {uid: [max(0, v) for v in vals] for uid, vals in out.items()}


def _assert_matches_oracle(db):
    expected = _oracle(db)
    docs = list(db.users.find({}, {"_id": 0}))
    assert len({d["user_id"] for d in docs}) == len(docs)  # no duplicate users
    for doc in docs:
        want = expected.get(doc["user_id"], [0, 0, 0])
        assert [doc.get(f) for f in FIELDS] == want, doc
        assert [doc.get(f) for f in NEXT] == want, doc  # staging mirrors live, as before


def _oplog_user_writes(db, fn):
    """(result, oplog entries on db.users) or (result, None) on a standalone."""
    local = db.client["local"]
    if "oplog.rs" not in local.list_collection_names():
        return fn(), None
    oplog = local["oplog.rs"]
    last = next(oplog.find().sort("$natural", -1).limit(1))["ts"]
    result = fn()
    n = oplog.count_documents({"ts": {"$gt": last}, "ns": f"{db.name}.users"})
    return result, n


def test_referral_snapshot_matches_ledger_and_gmt8_boundaries(world):
    _seed(world)
    out = scheduler.settle_referral_snapshots()
    _assert_matches_oracle(world)
    u = {d["user_id"]: d for d in world.users.find()}
    assert [u[1][f] for f in FIELDS] == [1, 2, 3]
    assert u[2]["weekly_referrals"] == 1  # Monday 00:00 KL belongs to the new week
    assert u[3]["weekly_referrals"] == 0 and u[3]["monthly_referrals"] == 1  # Sunday 23:59 KL
    assert [u[5][f] for f in FIELDS] == [0, 0, 0]  # negative net clamped
    assert [u[6][f] for f in FIELDS] == [0, 0, 0]  # invalidated-only inviter zeroed
    assert u[7]["weekly_referrals"] == 0
    assert [u[99][f] for f in FIELDS] == [1, 1, 1]  # missing users doc upserted
    assert out["negative_users_clamped"] == 1


def test_unchanged_rerun_writes_no_user_documents(world):
    _seed(world)
    scheduler.settle_referral_snapshots()
    before = {d["user_id"]: d for d in world.users.find()}

    out, oplog_entries = _oplog_user_writes(world, scheduler.settle_referral_snapshots)

    assert out["users_staging_zeroed"] == 0
    assert out["users_staging_changed"] == 0
    assert out["users_published_changed"] == 0
    if oplog_entries is not None:
        assert oplog_entries == 0
    after = {d["user_id"]: d for d in world.users.find()}
    assert after == before  # not a single field (incl. timestamps) rewritten
    _assert_matches_oracle(world)


def test_only_the_changed_inviter_is_written(world):
    _seed(world)
    scheduler.settle_referral_snapshots()
    world.referral_events.insert_one(_ev(2, 202, "referral_settled", NOW - timedelta(minutes=5)))

    out, oplog_entries = _oplog_user_writes(world, scheduler.settle_referral_snapshots)

    assert out["users_staging_changed"] == 1
    assert out["users_published_changed"] == 1
    if oplog_entries is not None:
        assert oplog_entries == 2  # stage + publish for user 2 only
    doc = world.users.find_one({"user_id": 2})
    assert [doc[f] for f in FIELDS] == [2, 2, 2]
    assert doc["snapshot_updated_at"].replace(tzinfo=timezone.utc) == NOW
    _assert_matches_oracle(world)


def test_revocation_and_invalidation_corrections_still_apply(world):
    _seed(world)
    scheduler.settle_referral_snapshots()
    world.referral_events.insert_one(_ev(1, 101, "referral_revoked", NOW - timedelta(minutes=1)))
    world.referral_events.update_one({"inviter_id": 99}, {"$set": {"invalidated": True}})
    scheduler.settle_referral_snapshots()
    _assert_matches_oracle(world)
    assert world.users.find_one({"user_id": 1})["weekly_referrals"] == 0
    assert world.users.find_one({"user_id": 99})["total_referrals"] == 0


class _FailingUsers:
    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def bulk_write(self, ops, ordered=False):
        raise BulkWriteError({"writeErrors": [{"index": 0, "code": 1, "errmsg": "injected"}]})


class _FailingDB:
    def __init__(self, real):
        self._real = real
        self.users = _FailingUsers(real.users)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_failed_staging_never_publishes_partial_results(world, monkeypatch):
    _seed(world)
    scheduler.settle_referral_snapshots()
    live_before = {d["user_id"]: [d.get(f) for f in FIELDS] for d in world.users.find()}
    world.referral_events.insert_one(_ev(2, 202, "referral_settled", NOW - timedelta(minutes=5)))

    monkeypatch.setattr(scheduler, "db", _FailingDB(world))
    with pytest.raises(BulkWriteError):
        scheduler.settle_referral_snapshots()
    live_after = {d["user_id"]: [d.get(f) for f in FIELDS] for d in world.users.find()}
    assert live_after == live_before  # nothing published

    monkeypatch.setattr(scheduler, "db", world)
    scheduler.settle_referral_snapshots()  # next tick recovers
    _assert_matches_oracle(world)


def test_concurrent_runs_converge_without_duplicates(world):
    _seed(world)
    errors = []
    barrier = threading.Barrier(3)

    def run():
        try:
            barrier.wait(timeout=10)
            scheduler.settle_referral_snapshots()
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    # A losing concurrent upsert of the brand-new users doc may raise
    # DuplicateKeyError (the unique index holds) - never a duplicate user.
    assert all("E11000" in str(e) for e in errors), errors
    scheduler.settle_referral_snapshots()
    _assert_matches_oracle(world)


def test_xp_incremental_rerun_does_not_rewrite_users(world, monkeypatch):
    monkeypatch.setattr(scheduler, "_write_snapshot_heartbeat", lambda source, ts: None)
    world.users.insert_many([{"user_id": uid} for uid in (1, 2, 3)])
    world.xp_events.insert_many([
        {"user_id": 1, "xp": 10, "created_at": NOW - timedelta(hours=1)},
        {"user_id": 2, "xp": 7, "created_at": NOW - timedelta(days=40)},
    ])
    snap.settle_xp_snapshots_incremental(world, now_utc_ts=NOW)  # bootstrap
    world.xp_events.insert_one({"user_id": 1, "xp": 5, "created_at": NOW})
    snap.settle_xp_snapshots_incremental(world, now_utc_ts=NOW + timedelta(minutes=5))
    before = {d["user_id"]: d for d in world.users.find()}

    _, oplog_entries = _oplog_user_writes(
        world, lambda: snap.settle_xp_snapshots_incremental(world, now_utc_ts=NOW + timedelta(minutes=10))
    )

    if oplog_entries is not None:
        assert oplog_entries == 0
    assert {d["user_id"]: d for d in world.users.find()} == before
    u1 = before[1]
    assert (u1["total_xp"], u1["weekly_xp"], u1["monthly_xp"]) == (15, 15, 15)
    assert (before[2]["total_xp"], before[2]["weekly_xp"], before[2]["monthly_xp"]) == (7, 0, 0)
    hb = world.admin_cache.find_one({"_id": "snapshot_heartbeat"})
    assert hb["ts_utc"].replace(tzinfo=timezone.utc) == NOW + timedelta(minutes=10)
