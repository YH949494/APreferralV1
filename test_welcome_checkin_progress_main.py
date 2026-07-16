"""Tests for the live /api/checkin -> main.process_checkin -> Welcome reminder wiring.

These exercise the production check-in path (main.py::process_checkin), not the
unused checkin.py::handle_checkin module, against an in-memory mongomock
database so no real Mongo/Telegram network access is required.
"""

import asyncio
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

import scheduler  # noqa: E402
import settings_service  # noqa: E402
import vouchers  # noqa: E402
from config import KL_TZ, XP_BASE_PER_CHECKIN  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_mongomock_db():
    """Give every test in this module its own in-memory Mongo.

    Other test modules in this suite reset ``database._db`` to ``None`` (or
    write different app-settings values into their own db) as part of their
    own setup/teardown, which would otherwise make these tests order-dependent
    on the full-suite run. Force a live mongomock db back in and drop the
    process-wide settings_service cache before each test regardless of what
    ran before it.

    One test module (test_initdata_logging.py) swaps ``sys.modules["database"]``
    for a stub and reloads ``vouchers`` against it, permanently rebinding
    ``vouchers``'s module-level collection handles to no-op stubs for the rest
    of the process. Reloading ``vouchers``/``scheduler`` here re-executes their
    top-level ``from database import ...`` against the real module, repairing
    those handles in place (module globals are mutated, not replaced, so
    functions already bound elsewhere pick up the fix too).
    """
    database._client = mongomock.MongoClient()
    database._db = database._client["referral_bot"]
    settings_service.invalidate_cache()
    importlib.reload(vouchers)
    importlib.reload(scheduler)
    yield


def _next_uid(_counter=[900000]):
    _counter[0] += 1
    return _counter[0]


def _make_eligible_user(uid, joined=None):
    joined = joined or datetime.now(KL_TZ)
    main.users_collection.update_one(
        {"user_id": uid},
        {"$set": {"user_id": uid, "joined_main_at": joined}},
        upsert=True,
    )
    return joined


def _eligible_patches():
    return (
        mock.patch.object(vouchers, "welcome_eligibility", lambda _uid, ref=None: (True, "ok", {"status": "issued"})),
        mock.patch.object(vouchers, "_has_current_subscription_evidence", lambda _uid: True),
    )


def test_first_successful_checkin_creates_day1_at():
    uid = _next_uid()
    _make_eligible_user(uid)

    p1, p2 = _eligible_patches()
    with p1, p2:
        result = asyncio.run(main.process_checkin(uid, "alice", None))

    assert result["success"] is True
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc is not None
    assert doc.get("day1_at") is not None


def test_second_qualifying_checkin_creates_day2_at():
    uid = _next_uid()
    joined = _make_eligible_user(uid)

    p1, p2 = _eligible_patches()
    with p1, p2:
        asyncio.run(main.process_checkin(uid, "alice", None))
        with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {"eligible": True, "completed": 2}):
            vouchers.record_welcome_checkin_progress(uid, now=joined + timedelta(days=1))

    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("day1_at") is not None
    assert doc.get("day2_at") is not None


def test_third_qualifying_checkin_completes_welcome_progress():
    uid = _next_uid()
    joined = _make_eligible_user(uid)

    events = []
    p1, p2 = _eligible_patches()
    with p1, p2:
        asyncio.run(main.process_checkin(uid, "alice", None))
        with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {"eligible": True, "completed": 2}):
            vouchers.record_welcome_checkin_progress(uid, now=joined + timedelta(days=1))

        with (
            mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {"eligible": True, "completed": 3}),
            mock.patch.object(vouchers, "log_welcome_event", lambda event, u, meta=None, **kw: events.append(event)),
        ):
            vouchers.record_welcome_checkin_progress(uid, now=joined + timedelta(days=2))

    assert "welcome_checkin_d3" in events
    assert "welcome_completed" in events


def test_already_checked_in_does_not_advance_welcome_progress():
    uid = _next_uid()
    _make_eligible_user(uid)

    calls = []
    orig = main.record_welcome_checkin_progress

    def spy(user_id, now=None):
        calls.append(user_id)
        return orig(user_id, now=now)

    p1, p2 = _eligible_patches()
    with p1, p2, mock.patch.object(main, "record_welcome_checkin_progress", spy):
        first = asyncio.run(main.process_checkin(uid, "alice", None))
        assert first["success"] is True
        assert len(calls) == 1

        second = asyncio.run(main.process_checkin(uid, "alice", None))
        assert second["success"] is False
        # No additional call for the already-checked-in response.
        assert len(calls) == 1


def test_failed_welcome_tracking_write_does_not_fail_checkin():
    uid = _next_uid()
    _make_eligible_user(uid)

    with mock.patch.object(main, "record_welcome_checkin_progress", side_effect=RuntimeError("boom")):
        result = asyncio.run(main.process_checkin(uid, "alice", None))

    assert result["success"] is True
    assert result["base_xp"] == XP_BASE_PER_CHECKIN


def test_existing_checkin_xp_and_streak_behavior_unchanged():
    uid = _next_uid()
    _make_eligible_user(uid)

    p1, p2 = _eligible_patches()
    with p1, p2:
        result = asyncio.run(main.process_checkin(uid, "alice", None))

    assert result["success"] is True
    assert result["base_xp"] == XP_BASE_PER_CHECKIN
    assert result["streak"] == 1
    user_doc = main.users_collection.find_one({"user_id": uid})
    assert user_doc["streak"] == 1


def test_process_welcome_reminders_finds_newly_created_candidate():
    uid = _next_uid()
    _make_eligible_user(uid)

    p1, p2 = _eligible_patches()
    with p1, p2:
        result = asyncio.run(main.process_checkin(uid, "alice", None))
    assert result["success"] is True

    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc is not None

    now_ref = datetime.now(timezone.utc)
    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {"completed": 1, "claimed": False, "expired": False}):
        scan_result = scheduler.process_welcome_reminders(
            now_ref=now_ref,
            db_ref=database.db,
            send_fn=lambda _uid, _text: (True, None, False),
        )

    assert scan_result["scanned"] >= 1
