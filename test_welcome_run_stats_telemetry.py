"""Tests for the Welcome Check-in telemetry path: the heartbeat that
``main._record_welcome_run_stats`` writes to ``admin_cache`` after every
``welcome_progress_reminders`` scheduler run, and that
``runtime_status.build_pm_automation`` reads back to decide whether the
"Welcome Check-in D2 Reminder" / "D2 Follow-up" / "D3 Reminder" dashboard
rows show Online/Waiting vs. Offline.

Covers the production bug where a run completes successfully (per
APScheduler/worker logs) but the dashboard keeps showing Offline: the
writer and reader keys must agree, and the heartbeat write must never be
silently dropped by an unrelated failure (e.g. a legacy/corrupt
``recentRuns`` field breaking the old single combined $set+$push update).
"""

import importlib
import logging
import os
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock
import pytest

import database

logging.disable(logging.CRITICAL)

if database._db is None:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
else:  # pragma: no cover - only when another test module already imported main
    import main  # noqa: E402

import runtime_status
import settings_service  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_mongomock_db():
    database._client = mongomock.MongoClient()
    database._db = database._client["referral_bot"]
    settings_service.invalidate_cache()
    main.admin_cache_col = database._db["admin_cache"]
    main.scheduler_locks_collection = database._db["scheduler_locks"]
    yield


JOB_KEY = "welcome_progress_reminders"
DOC_ID = f"welcome_run_stats:{JOB_KEY}"


def _read_heartbeat():
    return main.admin_cache_col.find_one({"_id": DOC_ID})


def _bson_round(dt: datetime) -> datetime:
    """Mongo stores millisecond precision as naive UTC; mongomock mirrors
    that, so truncate the same way before comparing against a Python
    datetime built with microsecond precision."""
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000, tzinfo=None)


def test_zero_eligible_run_refreshes_heartbeat():
    """A run that finds zero eligible users must still write lastRunAt —
    the write is not gated on any per-user loop or a nonzero send count."""
    now = datetime.now(timezone.utc)
    stats = {
        "run_id": "run-zero",
        "scanned": 0,
        "eligible_20h": 0,
        "eligible_28h": 0,
        "eligible_day3": 0,
        "reminder_20h_sent": 0,
        "reminder_28h_sent": 0,
        "day2_reminder_sent": 0,
        "send_failed": 0,
    }
    main._record_welcome_run_stats(JOB_KEY, stats, 0.05, now)

    doc = _read_heartbeat()
    assert doc is not None
    assert doc["lastRunAt"] == _bson_round(now)
    assert doc["updatedAt"] == _bson_round(now)
    assert doc["status"] == "ok"
    assert doc["run_id"] == "run-zero"
    assert doc["lastRunStats"]["scanned"] == 0
    assert len(doc["recentRuns"]) == 1


def test_run_with_sends_and_failures_refreshes_heartbeat():
    """A run where some sends succeed and some user-level sends fail must
    still refresh the heartbeat (not just log the failure)."""
    now = datetime.now(timezone.utc)
    stats = {
        "run_id": "run-mixed",
        "scanned": 10,
        "eligible_20h": 3,
        "reminder_20h_sent": 2,
        "send_failed": 1,
    }
    main._record_welcome_run_stats(JOB_KEY, stats, 1.23, now)

    doc = _read_heartbeat()
    assert doc["lastRunAt"] == _bson_round(now)
    assert doc["status"] == "ok"
    assert doc["lastRunStats"]["send_failed"] == 1
    assert doc["lastRunDurationS"] == 1.23


def test_stale_legacy_recentruns_cannot_block_fresh_heartbeat():
    """Regression test for the root cause: a pre-existing doc whose
    ``recentRuns`` field is not an array (e.g. seeded by an older schema)
    used to make the combined $set+$push update_one throw, which was
    silently swallowed — freezing lastRunAt forever even on a fresh
    successful run. The heartbeat write must now succeed regardless."""
    stale_time = datetime.now(timezone.utc) - timedelta(hours=6)
    main.admin_cache_col.update_one(
        {"_id": DOC_ID},
        {"$set": {"lastRunAt": stale_time, "recentRuns": "not-an-array"}},
        upsert=True,
    )

    now = datetime.now(timezone.utc)
    stats = {"run_id": "run-after-corruption", "scanned": 5}
    main._record_welcome_run_stats(JOB_KEY, stats, 0.4, now)

    doc = _read_heartbeat()
    assert doc["lastRunAt"] == _bson_round(now)
    assert doc["status"] == "ok"
    assert doc["run_id"] == "run-after-corruption"


def test_scheduled_wrapper_persists_heartbeat_end_to_end():
    """The APScheduler-facing wrapper (welcome_progress_reminders_scheduled)
    must call through to persistence, not just the core reminder logic."""
    now_before = datetime.now(timezone.utc)
    with mock.patch.object(
        main, "process_welcome_reminders", return_value={"run_id": "run-e2e", "scanned": 0}
    ):
        main.welcome_progress_reminders_scheduled()

    doc = _read_heartbeat()
    assert doc is not None
    assert doc["lastRunAt"] >= _bson_round(now_before)
    assert doc["run_id"] == "run-e2e"


def test_dashboard_reads_same_canonical_job_key_as_writer():
    """Writer key (admin_cache._id) and reader key (runtime_status lookup)
    must be identical character-for-character."""
    now = datetime.now(timezone.utc)
    main._record_welcome_run_stats(JOB_KEY, {"run_id": "r1", "scanned": 1}, 0.1, now)

    doc = runtime_status._welcome_run_stats_doc({"admin_cache": main.admin_cache_col}, JOB_KEY)
    assert doc.get("lastRunAt") == _bson_round(now)
    assert doc.get("run_id") == "r1"


def test_all_three_stage_rows_online_or_waiting_from_shared_fresh_heartbeat():
    """All three Welcome Check-in stage rows (D2 reminder, D2 follow-up, D3
    reminder) share job_key='welcome_progress_reminders' — a single fresh
    heartbeat write must lift all three out of Offline."""
    now = datetime.now(timezone.utc)
    main._record_welcome_run_stats(JOB_KEY, {"run_id": "r2", "scanned": 0}, 0.2, now)

    collections = {
        "scheduler_locks": main.scheduler_locks_collection,
        "admin_cache": main.admin_cache_col,
        "users": database._db["users"],
        "referral_notifications": database._db["referral_notifications"],
        "welcome_eligibility": database._db["welcome_eligibility"],
        "welcome_analytics_events": database._db["welcome_analytics_events"],
        "reactivation_journey": database._db["reactivation_journey"],
    }
    feature_flags = {"welcome_journey": True}
    referral_config = {}

    pm_rows = runtime_status.build_pm_automation(collections, feature_flags, referral_config, now)
    stage_rows = {
        r["key"]: r
        for r in pm_rows
        if r["key"] in ("welcome_checkin_d2", "welcome_checkin_d2_followup", "welcome_checkin_d3")
    }
    assert len(stage_rows) == 3
    for key, row in stage_rows.items():
        assert row["status"] != runtime_status.OFFLINE, f"{key} unexpectedly Offline: {row['notes']}"
