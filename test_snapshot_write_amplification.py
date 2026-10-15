"""Guards against reintroducing full-collection writes in the 5-minute snapshot.

The XP/referral snapshot used to stamp every users document three times per
tick (publish copy, $inc snapshot_version, XP "publish touch") even when no
counter changed — one oplog entry per user per stamp. These tests pin the
replacement: only changed users are written, publication time comes from the
existing snapshot heartbeat, and the read-side freshness contract is
unchanged. Database-free (fakes); test_snapshot_write_amplification_real_mongo.py
checks the same behaviour against a real mongod.
"""
import logging
import os
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock  # noqa: E402
import pytest  # noqa: E402

import database  # noqa: E402

try:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
except RuntimeError:
    import main  # noqa: E402

import scheduler  # noqa: E402
import snapshot_publish  # noqa: E402
import vouchers  # noqa: E402
import xp_snapshot as snap  # noqa: E402

NOW = datetime(2026, 10, 14, 4, 0, tzinfo=timezone.utc)  # a Wednesday


class _R:
    def __init__(self, modified=0, upserted_id=None):
        self.modified_count = modified
        self.matched_count = modified
        self.upserted_id = upserted_id
        self.upserted_count = 1 if upserted_id is not None else 0


class _SpyUsers:
    """Records every write; applies nothing. Enough to inspect write shapes."""

    def __init__(self):
        self.update_many_calls = []
        self.bulk_calls = []

    def update_many(self, filt, update):
        self.update_many_calls.append((filt, update))
        return _R()

    def bulk_write(self, ops, ordered=False):
        self.bulk_calls.append(ops)
        return _R()


class _Events:
    def __init__(self, rows):
        self.rows = rows

    def aggregate(self, pipeline):
        return list(self.rows)


class _SpyDB:
    def __init__(self, rows):
        self.users = _SpyUsers()
        self.referral_events = _Events(rows)


class _OneDoc:
    def __init__(self, doc=None, boom=False):
        self.doc = doc
        self.boom = boom

    def find_one(self, filt, projection=None):
        if self.boom:
            raise RuntimeError("down")
        return self.doc if self.doc and filt.get("_id") == self.doc.get("_id") else None


class _AdminDB:
    def __init__(self, heartbeat_ts=None, boom=False):
        doc = {"_id": "snapshot_heartbeat", "ts_utc": heartbeat_ts} if heartbeat_ts else None
        self.admin_cache = _OneDoc(doc, boom=boom)


@pytest.fixture(autouse=True)
def _fresh_cache():
    snapshot_publish.reset_cache()
    yield
    snapshot_publish.reset_cache()


def _run_referral(fake_db):
    original_db, original_hb = scheduler.db, scheduler._write_snapshot_heartbeat
    scheduler.db = fake_db
    scheduler._write_snapshot_heartbeat = lambda source, ts: None
    try:
        return scheduler.settle_referral_snapshots()
    finally:
        scheduler.db, scheduler._write_snapshot_heartbeat = original_db, original_hb


def test_referral_snapshot_never_writes_with_an_empty_filter():
    db = _SpyDB([{"_id": 7, "weekly_referrals": 1, "monthly_referrals": 2, "total_referrals": 3}])
    _run_referral(db)
    filters = [f for f, _ in db.users.update_many_calls]
    assert filters, "expected staged/publish update_many calls"
    assert all(f for f in filters), f"full-collection update_many issued: {filters}"
    # no $inc of snapshot_version across the whole collection
    assert not any(isinstance(u, dict) and "$inc" in u for _, u in db.users.update_many_calls)


def test_referral_publish_is_restricted_to_changed_documents():
    db = _SpyDB([{"_id": 7, "weekly_referrals": 1, "monthly_referrals": 2, "total_referrals": 3}])
    _run_referral(db)
    zero_filter, zero_update = db.users.update_many_calls[0]
    assert zero_filter["user_id"] == {"$nin": [7]}
    assert {tuple(c) for c in (d.keys() for d in zero_filter["$or"])} == {
        ("weekly_referrals_next",), ("monthly_referrals_next",), ("total_referrals_next",)
    }
    assert zero_update == {"$set": {"weekly_referrals_next": 0, "monthly_referrals_next": 0,
                                    "total_referrals_next": 0}}
    publish_filter, publish_update = db.users.update_many_calls[1]
    assert "$expr" in publish_filter
    stage = publish_update[0]["$set"]
    assert stage["weekly_referrals"] == "$weekly_referrals_next"
    assert stage["monthly_referrals"] == "$monthly_referrals_next"
    assert stage["total_referrals"] == "$total_referrals_next"
    # staging values are the clamped ledger aggregate, exactly as before
    (op,) = db.users.bulk_calls[0]
    assert op._doc["$set"] == {"weekly_referrals_next": 1, "monthly_referrals_next": 2, "total_referrals_next": 3}


def test_referral_bulk_failure_aborts_before_publish():
    db = _SpyDB([{"_id": 7, "weekly_referrals": 1, "monthly_referrals": 1, "total_referrals": 1}])

    def boom(ops, ordered=False):
        raise RuntimeError("write failed")

    db.users.bulk_write = boom
    with pytest.raises(RuntimeError):
        _run_referral(db)
    # only the staging-zero ran; nothing was published to live fields
    assert len(db.users.update_many_calls) == 1
    assert "$expr" not in db.users.update_many_calls[0][0]


def test_referral_snapshot_with_no_inviters_still_publishes_zeroes():
    db = _SpyDB([])
    out = _run_referral(db)
    assert db.users.update_many_calls[0][0]["user_id"] == {"$nin": []}
    assert "$expr" in db.users.update_many_calls[1][0]
    assert out["users_scanned"] == 0


def test_xp_incremental_steady_state_does_not_touch_every_user(monkeypatch):
    monkeypatch.setattr(snap, "safe_create_index", lambda *a, **k: "noop")
    from test_xp_snapshot import FakeDB

    db = FakeDB()
    monkeypatch.setattr(scheduler, "db", db)  # bootstrap's one-time full rebuild
    monkeypatch.setattr(scheduler, "_write_snapshot_heartbeat", lambda source, ts: None)
    db.users.docs[1] = {"user_id": 1}
    db.users.docs[2] = {"user_id": 2}
    db.xp_events.insert(user_id=1, xp=10, created_at=NOW)
    snap.settle_xp_snapshots_incremental(db, now_utc_ts=NOW)  # bootstrap

    user2_before = dict(db.users.docs[2])
    calls = []
    original = db.users.update_many
    db.users.update_many = lambda filt, update: calls.append(filt) or original(filt, update)
    db.xp_events.insert(user_id=1, xp=5, created_at=NOW + timedelta(minutes=4))
    later = NOW + timedelta(minutes=5)
    snap.settle_xp_snapshots_incremental(db, now_utc_ts=later)

    assert calls == []  # same week/month: no collection-wide write at all
    assert db.users.docs[1]["total_xp"] == 15
    assert db.users.docs[1]["snapshot_updated_at"] == later  # changed user stamped
    assert db.users.docs[2] == user2_before  # unchanged user: not rewritten at all
    assert db.admin_cache.docs["snapshot_heartbeat"]["ts_utc"] == later


def test_effective_snapshot_ts_is_max_of_user_stamp_and_heartbeat():
    old = NOW - timedelta(hours=3)
    db = _AdminDB(heartbeat_ts=NOW)
    assert snapshot_publish.effective_snapshot_updated_at(db, old) == NOW
    snapshot_publish.reset_cache()
    newer = NOW + timedelta(seconds=30)  # individually stamped after the publish
    assert snapshot_publish.effective_snapshot_updated_at(db, newer) == newer
    snapshot_publish.reset_cache()
    assert snapshot_publish.effective_snapshot_updated_at(db, None) == NOW


def test_effective_snapshot_ts_handles_naive_and_missing_heartbeat():
    naive_hb = NOW.replace(tzinfo=None)
    db = _AdminDB(heartbeat_ts=naive_hb)
    assert snapshot_publish.effective_snapshot_updated_at(db, NOW - timedelta(days=1)) == NOW
    snapshot_publish.reset_cache()
    stamp = NOW - timedelta(minutes=1)
    assert snapshot_publish.effective_snapshot_updated_at(_AdminDB(), stamp) == stamp
    snapshot_publish.reset_cache()
    assert snapshot_publish.effective_snapshot_updated_at(_AdminDB(boom=True), stamp) == stamp
    snapshot_publish.reset_cache()
    assert snapshot_publish.effective_snapshot_updated_at(_AdminDB(), None) is None


class _RefEvents:
    def __init__(self, settled=0, revoked=0):
        self.settled, self.revoked = settled, revoked
        self.calls = 0

    def count_documents(self, filt, **kwargs):
        self.calls += 1
        return self.settled if filt.get("event") == "referral_settled" else self.revoked


class _VouchersDB(_AdminDB):
    def __init__(self, heartbeat_ts, events):
        super().__init__(heartbeat_ts=heartbeat_ts)
        self._events = events

    def __getitem__(self, name):
        assert name == "referral_events"
        return self._events


def test_referral_progress_unchanged_user_reads_fresh_from_heartbeat(monkeypatch):
    events = _RefEvents(settled=9)
    now_ref = NOW + timedelta(minutes=2)
    monkeypatch.setattr(vouchers, "db", _VouchersDB(NOW, events))
    user = {"user_id": 5, "total_referrals": 4, "weekly_referrals": 1, "monthly_referrals": 2,
            "snapshot_updated_at": NOW - timedelta(days=2)}  # not re-stamped: numbers unchanged
    with main.app.app_context():
        out = vouchers.resolve_referral_counts_with_snapshot_fallback(5, user, now_ref)
    assert out["source"] == "snapshot"
    assert out["snapshot_updated_at"] == NOW
    assert out["snapshot_age_sec"] == 120
    assert out["stats"] == {"total_referrals": 4, "weekly_referrals": 1, "monthly_referrals": 2}
    assert events.calls == 0  # no stale-path ledger reads


def test_referral_progress_still_falls_back_when_publishing_stops(monkeypatch):
    events = _RefEvents(settled=9)
    stale_hb = NOW - timedelta(seconds=vouchers.REFERRAL_SNAPSHOT_STALE_SEC + 60)
    monkeypatch.setattr(vouchers, "db", _VouchersDB(stale_hb, events))
    user = {"user_id": 5, "total_referrals": 4, "weekly_referrals": 0, "monthly_referrals": 0,
            "snapshot_updated_at": stale_hb}
    with main.app.app_context():
        out = vouchers.resolve_referral_counts_with_snapshot_fallback(5, user, NOW)
    assert out["source"] == "ledger"
    assert out["fallback_reason"] == "snapshot_stale"


def test_freshness_check_uses_heartbeat_not_a_users_scan(monkeypatch, caplog):
    class _NoScanUsers:
        def find_one(self, *a, **k):
            raise AssertionError("per-user staleness scan must not run when the heartbeat exists")

    hb = _OneDoc({"_id": "snapshot_heartbeat", "ts_utc": NOW - timedelta(minutes=3)})
    monkeypatch.setattr(main, "RUNNER_MODE", "worker")
    monkeypatch.setattr(main, "admin_cache_col", hb)
    monkeypatch.setattr(main, "users_collection", _NoScanUsers())
    monkeypatch.setattr(main, "now_utc", lambda: NOW)
    with caplog.at_level(logging.ERROR):
        main._check_snapshot_freshness()
    assert "[SNAPSHOT][STALE]" not in caplog.text

    hb.doc["ts_utc"] = NOW - timedelta(minutes=20)
    with caplog.at_level(logging.ERROR):
        main._check_snapshot_freshness()
    assert caplog.text.count("[SNAPSHOT][STALE] age_sec=1200") == 1  # logged once, not twice


def test_freshness_check_keeps_legacy_scan_without_heartbeat(monkeypatch, caplog):
    seen = {}

    class _Users:
        def find_one(self, filt, projection=None):
            seen["filter"] = filt
            return {"snapshot_updated_at": NOW - timedelta(minutes=30)}

    empty = _OneDoc(None)
    monkeypatch.setattr(main, "RUNNER_MODE", "worker")
    monkeypatch.setattr(main, "admin_cache_col", empty)
    monkeypatch.setattr(main, "users_collection", _Users())
    monkeypatch.setattr(main, "now_utc", lambda: NOW)
    with caplog.at_level(logging.ERROR):
        main._check_snapshot_freshness()
    assert "snapshot_updated_at" in seen["filter"]
    assert "[SNAPSHOT][STALE] age_sec=1800" in caplog.text
