"""tick_5min lock lifecycle: released on completion/exception, token-guarded so an
expired run cannot free a newer run's lease, concurrent workers still excluded."""
import os
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock
import pytest

import database

if database._db is None:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
else:  # pragma: no cover
    import main  # noqa: E402

LOCK = "tick_5min"


@pytest.fixture(autouse=True)
def _clean():
    main.scheduler_locks_collection.delete_many({})
    yield
    main.scheduler_locks_collection.delete_many({})


def _held():
    doc = main.scheduler_locks_collection.find_one({"_id": LOCK})
    if doc is None:  # mongomock reaps TTL-expired docs on read, like Mongo's TTL monitor
        return False
    exp = doc["expireAt"]
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    return exp > datetime.now(timezone.utc)


def _run_tick(side_effect=None):
    """Run tick_5min with heavy steps stubbed; first step can be made to raise."""
    with mock.patch.object(main, "settle_pending_referrals_with_cache_clear"), \
         mock.patch.object(main, "settle_xp_snapshots"), \
         mock.patch.object(main, "settle_referral_snapshots_with_cache_clear"), \
         mock.patch.object(main, "_check_snapshot_freshness"), \
         mock.patch.object(main, "run_weekly_archive_catchup"), \
         mock.patch.object(main, "_clear_leaderboard_cache", side_effect=side_effect), \
         mock.patch.object(main, "compute_retention_kpis"):
        main.tick_5min()


def test_lock_released_after_normal_completion():
    _run_tick()
    assert not _held()
    # next 5-min tick can acquire immediately instead of waiting out the 900s TTL
    ok, _ = main.acquire_scheduler_lock(LOCK, 900)
    assert ok


def test_lock_released_when_tick_raises():
    with pytest.raises(RuntimeError):
        _run_tick(side_effect=RuntimeError("boom"))
    assert not _held()


def test_concurrent_worker_blocked_while_held():
    ok, doc = main.acquire_scheduler_lock(LOCK, 900)
    assert ok and doc["token"]
    ok2, _ = main.acquire_scheduler_lock(LOCK, 900)
    assert not ok2


def test_not_acquired_tick_does_not_release_other_workers_lock():
    ok, doc = main.acquire_scheduler_lock(LOCK, 900)
    assert ok
    with mock.patch.object(main, "compute_retention_kpis") as kpi:
        main.tick_5min()  # lock held -> skipped, must not touch it
    kpi.assert_not_called()
    assert _held()


def test_stale_owner_cannot_release_newer_lock():
    ok, old = main.acquire_scheduler_lock(LOCK, 900)
    assert ok
    # lease expires mid-run (tick longer than TTL) and a new run takes over
    main.scheduler_locks_collection.update_one(
        {"_id": LOCK}, {"$set": {"expireAt": datetime.now(timezone.utc) - timedelta(seconds=1)}}
    )
    ok2, new = main.acquire_scheduler_lock(LOCK, 900)
    assert ok2 and new["token"] != old["token"]

    assert main.release_scheduler_lock(LOCK, old) is False  # stale run finishes
    assert _held()  # newer run's lease intact
    assert main.release_scheduler_lock(LOCK, new) is True
    assert not _held()


def test_release_without_token_is_noop_and_only_touches_expireAt():
    assert main.release_scheduler_lock(LOCK, None) is False
    coll = mock.MagicMock()
    with mock.patch.object(main, "scheduler_locks_collection", coll):
        main.release_scheduler_lock(LOCK, {"token": "t1"})
    filt, upd = coll.update_one.call_args.args
    assert filt == {"_id": LOCK, "token": "t1"}
    assert list(upd) == ["$set"] and list(upd["$set"]) == ["expireAt"]  # updatedAt heartbeat kept


def test_release_failure_never_raises():
    coll = mock.MagicMock()
    coll.update_one.side_effect = RuntimeError("mongo down")
    with mock.patch.object(main, "scheduler_locks_collection", coll):
        assert main.release_scheduler_lock(LOCK, {"token": "t1"}) is False
