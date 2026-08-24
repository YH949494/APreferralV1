"""Coverage for the Past Leaderboard archive lifecycle audit:

- reset_weekly_xp() archives the previous completed Mon->Sun week
  idempotently and only zeroes live counters once.
- run_boot_catchup() recovers a missing week on ANY boot day (not just
  Monday), using only the immutable xp_events/referral_events ledgers —
  never live users.weekly_* counters.
- /api/leaderboard/history/weeks always returns the newest completed week
  deterministically, even with malformed/missing archived_at.
- /api/admin/leaderboard/history/rebuild is admin-gated, defaults to a dry
  run, and never touches live counters.

Uses the same mongomock-import pattern as test_weekly_referral_post_integration.py.
"""
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


MONDAY_0803 = datetime(2026, 8, 3, 0, 0, 5, tzinfo=main.KL_TZ)  # matches the observed production incident


def _reset_history_state():
    main.history_collection.delete_many({})
    main.weekly_reset_markers_collection.delete_many({})
    main.users_collection.delete_many({})
    main.db["xp_events"].delete_many({})
    main.db["referral_events"].delete_many({})
    main.scheduler_locks_collection.delete_many({})


# 1. Monday scheduled archive creates 2026-07-27 -> 2026-08-02.
def test_monday_archive_creates_expected_week():
    _reset_history_state()
    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 50, "weekly_referrals": 2})

    with mock.patch.object(main, "acquire_scheduler_lock", return_value=(True, None)):
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
            result = main.reset_weekly_xp(run_id="t1")

    assert result["status"] == "created"
    assert result["week_start"] == "2026-07-27"
    assert result["week_end"] == "2026-08-02"
    assert result["reset_status"] == "done"

    doc = main.history_collection.find_one({"week_start": "2026-07-27"})
    assert doc is not None
    assert doc["week_end"] == "2026-08-02"

    user = main.users_collection.find_one({"user_id": 1})
    assert user["weekly_xp"] == 0
    assert user["weekly_referrals"] == 0


# 2. Re-running is idempotent and does not duplicate/reset twice.
def test_reset_weekly_xp_is_idempotent_on_rerun():
    _reset_history_state()
    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 50, "weekly_referrals": 2})

    with mock.patch.object(main, "acquire_scheduler_lock", return_value=(True, None)):
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
            first = main.reset_weekly_xp(run_id="run1")

            # Live counters pick up new-week activity before the (misfired) rerun.
            main.users_collection.update_one({"user_id": 1}, {"$set": {"weekly_xp": 15, "weekly_referrals": 1}})
            second = main.reset_weekly_xp(run_id="run2")

    assert first["status"] == "created"
    assert second["status"] == "already_exists"
    assert second["reset_status"] == "already_done"

    assert main.history_collection.count_documents({"week_start": "2026-07-27"}) == 1

    # Second run must NOT have zeroed the new week's already-accrued progress.
    user = main.users_collection.find_one({"user_id": 1})
    assert user["weekly_xp"] == 15
    assert user["weekly_referrals"] == 1


# 3. Worker boots Tuesday with missing history and detects/recovers it from the ledger.
def test_boot_catchup_tuesday_recovers_missing_week_from_ledger():
    _reset_history_state()
    tuesday = datetime(2026, 8, 4, 9, 0, 0, tzinfo=main.KL_TZ)

    # Live counters already contaminated with Tuesday's activity — must be ignored.
    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 999, "weekly_referrals": 999})

    checkin_ts = datetime(2026, 7, 28, 10, 0, 0, tzinfo=timezone.utc)
    main.db["xp_events"].insert_one({
        "user_id": 1, "unique_key": "checkin:20260728", "type": "checkin", "xp": 30, "created_at": checkin_ts,
    })
    main.db["referral_events"].insert_one({
        "inviter_id": 1, "invitee_id": 2, "event": "referral_settled", "week_key": "2026-07-27",
        "occurred_at": checkin_ts,
    })

    with mock.patch.object(main, "datetime", wraps=datetime) as dt:
        dt.now.side_effect = lambda tz=None: tuesday.astimezone(tz) if tz else tuesday
        main.run_boot_catchup()

    doc = main.history_collection.find_one({"week_start": "2026-07-27"})
    assert doc is not None
    assert doc["source"] == "ledger_rebuild"
    assert doc["checkin_leaderboard"] == [{"user_id": 1, "username": "a", "weekly_xp": 30}]
    assert doc["referral_leaderboard"] == [{"user_id": 1, "username": "a", "weekly_referrals": 1}]

    # Boot catch-up must never have touched live counters.
    user = main.users_collection.find_one({"user_id": 1})
    assert user["weekly_xp"] == 999
    assert user["weekly_referrals"] == 999


# 4. Worker boots Tuesday with existing history and skips.
def test_boot_catchup_skips_when_week_already_archived():
    _reset_history_state()
    main.history_collection.insert_one({
        "week_start": "2026-07-27", "week_end": "2026-08-02",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        "archived_at": datetime.now(timezone.utc), "source": "live_counters",
    })
    tuesday = datetime(2026, 8, 4, 9, 0, 0, tzinfo=main.KL_TZ)

    with mock.patch.object(main, "rebuild_week_from_ledger") as rebuild_mock:
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: tuesday.astimezone(tz) if tz else tuesday
            main.run_boot_catchup()
        rebuild_mock.assert_not_called()


# 5. Two simultaneous workers: only one archive/reset executes.
def test_concurrent_reset_weekly_xp_only_resets_once():
    _reset_history_state()
    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 50, "weekly_referrals": 2})

    with mock.patch.object(main, "acquire_scheduler_lock", return_value=(True, None)):
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
            r1 = main.reset_weekly_xp(run_id="workerA")
            r2 = main.reset_weekly_xp(run_id="workerB")

    statuses = sorted([r1["status"], r2["status"]])
    assert statuses == ["already_exists", "created"]
    reset_statuses = sorted([r1["reset_status"], r2["reset_status"]])
    assert reset_statuses == ["already_done", "done"]
    assert main.history_collection.count_documents({"week_start": "2026-07-27"}) == 1


# 6. Historical rebuild uses ledger windows, not current weekly counters.
def test_rebuild_week_from_ledger_ignores_live_counters():
    _reset_history_state()
    main.users_collection.insert_one({"user_id": 7, "username": "z", "weekly_xp": 12345, "weekly_referrals": 999})
    ts = datetime(2026, 7, 29, 3, 0, 0, tzinfo=timezone.utc)
    main.db["xp_events"].insert_one({"user_id": 7, "unique_key": "k1", "type": "checkin", "xp": 5, "created_at": ts})

    with mock.patch.object(main, "datetime", wraps=datetime) as dt:
        dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
        result = main.rebuild_week_from_ledger("2026-07-27", dry_run=False)

    assert result["status"] == "created"
    doc = main.history_collection.find_one({"week_start": "2026-07-27"})
    assert doc["checkin_leaderboard"] == [{"user_id": 7, "username": "z", "weekly_xp": 5}]
    # The live 12345/999 counters must never leak into the historical archive.
    for entry in doc["checkin_leaderboard"]:
        assert entry["weekly_xp"] != 12345


def test_rebuild_week_from_ledger_rejects_non_monday():
    result = main.rebuild_week_from_ledger("2026-07-28", dry_run=True)
    assert result["status"] == "failed"
    assert result["error"] == "not_a_monday"


def test_rebuild_week_from_ledger_rejects_incomplete_week():
    with mock.patch.object(main, "datetime", wraps=datetime) as dt:
        dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
        result = main.rebuild_week_from_ledger("2026-08-03", dry_run=True)
    assert result["status"] == "failed"
    assert result["error"] == "week_not_completed"


# 7. API returns newest week deterministically.
def test_history_weeks_api_sorts_by_week_start_not_archived_at():
    _reset_history_state()
    # archived_at is intentionally out of order / malformed relative to week_start.
    main.history_collection.insert_one({
        "week_start": "2026-07-27", "week_end": "2026-08-02",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        "archived_at": datetime(2026, 8, 3, 0, 0, 0, tzinfo=timezone.utc),
    })
    main.history_collection.insert_one({
        "week_start": "2026-07-20", "week_end": "2026-07-26",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        "archived_at": datetime(2026, 12, 1, 0, 0, 0, tzinfo=timezone.utc),  # edited later, misleadingly "newest"
    })

    client = main.app.test_client()
    resp = client.get("/api/leaderboard/history/weeks")
    body = resp.get_json()
    assert body["success"] is True
    assert body["weeks"][0]["week_start"] == "2026-07-27"


# 8. archived_at missing/malformed does not cause an older week to be selected.
def test_history_weeks_api_tolerates_missing_archived_at():
    _reset_history_state()
    main.history_collection.insert_one({
        "week_start": "2026-07-27", "week_end": "2026-08-02",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        # archived_at intentionally absent
    })
    main.history_collection.insert_one({
        "week_start": "2026-07-20", "week_end": "2026-07-26",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        "archived_at": datetime(2026, 7, 21, 0, 0, 0, tzinfo=timezone.utc),
    })

    client = main.app.test_client()
    resp = client.get("/api/leaderboard/history/weeks")
    body = resp.get_json()
    assert body["weeks"][0]["week_start"] == "2026-07-27"


# 9. No-store cache headers exist.
def test_history_endpoints_have_no_store_headers():
    _reset_history_state()
    main.history_collection.insert_one({
        "week_start": "2026-07-27", "week_end": "2026-08-02",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        "archived_at": datetime.now(timezone.utc),
    })
    client = main.app.test_client()

    resp = client.get("/api/leaderboard/history/weeks")
    assert "no-store" in resp.headers.get("Cache-Control", "")

    resp2 = client.get("/api/leaderboard/history/week/2026-07-27")
    assert "no-store" in resp2.headers.get("Cache-Control", "")


# 11. Missed Monday cron (worker never restarted) is recovered by the
# recurring 5-minute maintenance tick, not just boot.
def test_tick_5min_recovers_missing_week_without_restart():
    _reset_history_state()
    tuesday = datetime(2026, 8, 4, 9, 0, 0, tzinfo=main.KL_TZ)

    checkin_ts = datetime(2026, 7, 28, 10, 0, 0, tzinfo=timezone.utc)
    main.db["xp_events"].insert_one({
        "user_id": 1, "unique_key": "checkin:20260728", "type": "checkin", "xp": 30, "created_at": checkin_ts,
    })
    main.db["referral_events"].insert_one({
        "inviter_id": 1, "invitee_id": 2, "event": "referral_settled", "week_key": "2026-07-27",
        "occurred_at": checkin_ts,
    })
    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 5, "weekly_referrals": 0})

    with mock.patch.object(main, "acquire_scheduler_lock", return_value=(True, None)):
        with mock.patch.object(main, "settle_pending_referrals_with_cache_clear"):
            with mock.patch.object(main, "settle_xp_snapshots"):
                with mock.patch.object(main, "settle_referral_snapshots_with_cache_clear"):
                    with mock.patch.object(main, "_check_snapshot_freshness"):
                        with mock.patch.object(main, "compute_retention_kpis"):
                            with mock.patch.object(main, "datetime", wraps=datetime) as dt:
                                dt.now.side_effect = lambda tz=None: tuesday.astimezone(tz) if tz else tuesday
                                main.tick_5min()

    doc = main.history_collection.find_one({"week_start": "2026-07-27"})
    assert doc is not None
    assert doc["source"] == "ledger_rebuild"

    # tick_5min's catch-up must never reset live counters.
    user = main.users_collection.find_one({"user_id": 1})
    assert user["weekly_xp"] == 5


# 12. tick_5min is a no-op on the archive when the week already exists.
def test_tick_5min_skips_when_week_already_archived():
    _reset_history_state()
    main.history_collection.insert_one({
        "week_start": "2026-07-27", "week_end": "2026-08-02",
        "checkin_leaderboard": [], "referral_leaderboard": [],
        "archived_at": datetime.now(timezone.utc), "source": "live_counters",
    })
    tuesday = datetime(2026, 8, 4, 9, 0, 0, tzinfo=main.KL_TZ)

    with mock.patch.object(main, "acquire_scheduler_lock", return_value=(True, None)):
        with mock.patch.object(main, "settle_pending_referrals_with_cache_clear"):
            with mock.patch.object(main, "settle_xp_snapshots"):
                with mock.patch.object(main, "settle_referral_snapshots_with_cache_clear"):
                    with mock.patch.object(main, "_check_snapshot_freshness"):
                        with mock.patch.object(main, "compute_retention_kpis"):
                            with mock.patch.object(main, "rebuild_week_from_ledger") as rebuild_mock:
                                with mock.patch.object(main, "datetime", wraps=datetime) as dt:
                                    dt.now.side_effect = lambda tz=None: tuesday.astimezone(tz) if tz else tuesday
                                    main.tick_5min()
                                rebuild_mock.assert_not_called()

    assert main.history_collection.count_documents({"week_start": "2026-07-27"}) == 1


# Admin rebuild endpoint: admin-gated, defaults to dry_run, never resets counters.
def test_admin_rebuild_endpoint_requires_admin():
    _reset_history_state()
    client = main.app.test_client()
    resp = client.post("/api/admin/leaderboard/history/rebuild", json={"week_start": "2026-07-27"})
    assert resp.status_code in (400, 403)


def test_admin_rebuild_endpoint_defaults_to_dry_run():
    _reset_history_state()
    client = main.app.test_client()
    with mock.patch.object(main, "require_admin_from_query", return_value=(True, None)):
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
            resp = client.post("/api/admin/leaderboard/history/rebuild", json={"week_start": "2026-07-27"})
    body = resp.get_json()
    assert body["status"] == "dry_run"
    assert main.history_collection.find_one({"week_start": "2026-07-27"}) is None


def test_admin_rebuild_endpoint_write_mode_creates_archive():
    _reset_history_state()
    client = main.app.test_client()
    with mock.patch.object(main, "require_admin_from_query", return_value=(True, None)):
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
            resp = client.post(
                "/api/admin/leaderboard/history/rebuild",
                json={"week_start": "2026-07-27", "dry_run": False},
            )
    body = resp.get_json()
    assert body["status"] == "created"
    assert main.history_collection.find_one({"week_start": "2026-07-27"}) is not None


# 10. Existing live leaderboard / weekly snapshot logic remain unchanged.
def test_weekly_referral_post_guard_still_preserved_on_fresh_archive():
    _reset_history_state()
    doc_id = "weekly_referral_post:2026-07-27"
    main.db["weekly_referral_posts"].insert_one({
        "_id": doc_id, "week_key": "2026-07-27", "status": "sent", "message_id": 4242,
        "entries": [{"user_id": 1, "display_name": "@a", "weekly_referrals": 5}],
    })
    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 10, "weekly_referrals": 5})

    with mock.patch.object(main, "acquire_scheduler_lock", return_value=(True, None)):
        with mock.patch.object(main, "datetime", wraps=datetime) as dt:
            dt.now.side_effect = lambda tz=None: MONDAY_0803.astimezone(tz) if tz else MONDAY_0803
            main.reset_weekly_xp(run_id="test_run")

    kept = main.db["weekly_referral_posts"].find_one({"_id": doc_id})
    assert kept is not None
    assert kept["status"] == "sent"
    assert kept["message_id"] == 4242
