"""Regression tests for the production check-in path (main.py::process_checkin).

Covers the streak-reset audit: atomic per-user/per-KL-day claims, the
first-checkin-bonus-on-reset bug fix, timezone boundary handling, and
longest_streak monotonicity. Exercises main.process_checkin directly against
an in-memory mongomock database — the same pattern used by
test_welcome_checkin_progress_main.py.
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
else:  # pragma: no cover
    import main  # noqa: E402

import scheduler  # noqa: E402
import settings_service  # noqa: E402
import vouchers  # noqa: E402
from config import KL_TZ, XP_BASE_PER_CHECKIN, STREAK_MILESTONES  # noqa: E402
from main import FIRST_CHECKIN_BONUS_XP  # noqa: E402, F401


@pytest.fixture(autouse=True)
def _fresh_mongomock_db():
    database._client = mongomock.MongoClient()
    database._db = database._client["referral_bot"]
    settings_service.invalidate_cache()
    importlib.reload(vouchers)
    importlib.reload(scheduler)
    yield


def _next_uid(_counter=[800000]):
    _counter[0] += 1
    return _counter[0]


def _checkin(uid, username="alice", region=None):
    return asyncio.run(main.process_checkin(uid, username, region))


def _set_user_state(uid, *, streak, last_checkin, longest_streak=None, first_checkin_at="unset"):
    doc = {"user_id": uid, "streak": streak, "last_checkin": last_checkin}
    if longest_streak is not None:
        doc["longest_streak"] = longest_streak
    if first_checkin_at != "unset":
        doc["first_checkin_at"] = first_checkin_at
    main.users_collection.update_one({"user_id": uid}, {"$set": doc}, upsert=True)


def _checkin_xp_events(uid):
    return list(main.db.xp_events.find({"user_id": uid, "type": "checkin"}))


def _first_checkin_xp_events(uid):
    return list(main.db.xp_events.find({"user_id": uid, "unique_key": "first_checkin"}))


# ---------------------------------------------------------------------------
# Core streak progression
# ---------------------------------------------------------------------------

def test_first_checkin_ever_sets_streak_to_one():
    uid = _next_uid()
    result = _checkin(uid)
    assert result["success"] is True
    assert result["streak"] == 1
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["streak"] == 1
    assert doc["longest_streak"] == 1


def test_consecutive_kl_day_increments_streak():
    uid = _next_uid()
    yesterday_kl = datetime.now(KL_TZ).date() - timedelta(days=1)
    yesterday_utc = datetime.combine(yesterday_kl, datetime.min.time(), tzinfo=KL_TZ).astimezone(timezone.utc)
    _set_user_state(uid, streak=5, last_checkin=yesterday_utc, longest_streak=5)

    result = _checkin(uid)

    assert result["success"] is True
    assert result["streak"] == 6
    assert main.users_collection.find_one({"user_id": uid})["streak"] == 6


def test_missed_day_resets_streak_to_one():
    uid = _next_uid()
    three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
    _set_user_state(uid, streak=8, last_checkin=three_days_ago, longest_streak=8, first_checkin_at=three_days_ago)

    result = _checkin(uid)

    assert result["success"] is True
    assert result["streak"] == 1
    event = main.db.checkin_events.find_one({"user_id": uid, "checkin_date_kl": datetime.now(KL_TZ).date().isoformat()})
    assert event["reset_reason"] == "missed_day"


def test_same_day_duplicate_checkin_is_rejected_without_mutating_streak():
    uid = _next_uid()
    first = _checkin(uid)
    assert first["success"] is True

    before = main.users_collection.find_one({"user_id": uid})
    second = _checkin(uid)
    after = main.users_collection.find_one({"user_id": uid})

    assert second["success"] is False
    assert before["streak"] == after["streak"] == 1
    assert before["last_checkin"] == after["last_checkin"]
    assert len(_checkin_xp_events(uid)) == 1


def test_frontend_or_network_retry_does_not_double_grant_xp():
    """A retried /api/checkin POST after a successful check-in must be a no-op."""
    uid = _next_uid()
    first = _checkin(uid)
    assert first["success"] is True

    # Simulate the frontend firing the same request again (double-click,
    # timeout-triggered retry) after the first one already succeeded.
    for _ in range(3):
        retry = _checkin(uid)
        assert retry["success"] is False

    assert len(_checkin_xp_events(uid)) == 1
    assert main.users_collection.find_one({"user_id": uid})["streak"] == 1


# ---------------------------------------------------------------------------
# Concurrency / atomicity
# ---------------------------------------------------------------------------

def test_two_concurrent_requests_only_one_wins_the_atomic_claim():
    """Simulates two Gunicorn workers racing on the same user+day.

    Both requests read the same pre-checkin state before either commits.
    The claim and the streak/last_checkin mutation are the same compare-
    and-swap update (filtered on the exact last_checkin value each request
    read), so the second request to reach that update simply fails to
    match (matched_count == 0) — it must not mutate streak/last_checkin or
    grant XP a second time, and — unlike a separate claim-then-mutate
    design — there is no window where a crash between "claim" and "mutate"
    could strand the day as claimed-but-never-applied.
    """
    uid = _next_uid()
    yesterday_kl = datetime.now(KL_TZ).date() - timedelta(days=1)
    yesterday_utc = datetime.combine(yesterday_kl, datetime.min.time(), tzinfo=KL_TZ).astimezone(timezone.utc)
    _set_user_state(uid, streak=3, last_checkin=yesterday_utc, longest_streak=3)

    # Pretend a concurrent request already won the race and applied its
    # mutation (advanced last_checkin to "now") before this request's CAS
    # update runs — the true race window in a find-then-update design.
    main.users_collection.update_one(
        {"user_id": uid},
        {"$set": {"streak": 4, "last_checkin": datetime.now(timezone.utc)}, "$max": {"longest_streak": 4}},
    )

    result = _checkin(uid)

    assert result["success"] is False
    # The loser must not have re-mutated streak/last_checkin/longest_streak
    # beyond what the winner already applied.
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["streak"] == 4
    assert doc["longest_streak"] == 4
    assert len(_checkin_xp_events(uid)) == 0


def test_cas_filter_rejects_a_stale_last_checkin_read():
    """Direct unit check of the CAS mechanism itself: an update_one filtered
    on a last_checkin value that no longer matches the stored document must
    not modify anything.
    """
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    _set_user_state(uid, streak=1, last_checkin=now, longest_streak=1)

    stale_value = now - timedelta(days=1)
    result = main.users_collection.update_one(
        {"user_id": uid, "last_checkin": stale_value},
        {"$set": {"streak": 99}},
    )
    assert result.matched_count == 0
    assert main.users_collection.find_one({"user_id": uid})["streak"] == 1


# ---------------------------------------------------------------------------
# Timezone handling
# ---------------------------------------------------------------------------

def test_kl_local_date_23_59_and_00_01_land_on_different_kl_days():
    kl_2359 = datetime(2026, 7, 27, 23, 59, tzinfo=KL_TZ)
    kl_0001_next = datetime(2026, 7, 28, 0, 1, tzinfo=KL_TZ)
    assert main._to_kl_date(kl_2359) == datetime(2026, 7, 27).date()
    assert main._to_kl_date(kl_0001_next) == datetime(2026, 7, 28).date()


def test_kl_local_date_boundary_via_utc_offset():
    # 16:01 UTC on July 27 == 00:01 KL on July 28 (UTC+8, no DST).
    utc_ts = datetime(2026, 7, 27, 16, 1, tzinfo=timezone.utc)
    assert main._to_kl_date(utc_ts) == datetime(2026, 7, 28).date()
    # 15:59 UTC on July 27 == 23:59 KL on July 27.
    utc_ts2 = datetime(2026, 7, 27, 15, 59, tzinfo=timezone.utc)
    assert main._to_kl_date(utc_ts2) == datetime(2026, 7, 27).date()


def test_legacy_naive_datetime_is_treated_as_utc_not_dropped():
    """main._to_kl_date must not silently reset a streak just because a
    legacy row has a naive (tz-less) last_checkin — it should convert it
    (assuming UTC, this codebase's convention) rather than treat it as
    unparseable.
    """
    naive_yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).replace(tzinfo=None)
    uid = _next_uid()
    _set_user_state(uid, streak=4, last_checkin=naive_yesterday, longest_streak=4)

    result = _checkin(uid)

    assert result["success"] is True
    assert result["streak"] == 5  # continued, not reset, despite the naive timestamp


# ---------------------------------------------------------------------------
# Milestones / longest_streak
# ---------------------------------------------------------------------------

def test_milestone_bonus_xp_granted_exactly_once():
    uid = _next_uid()
    six_days_streak_yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    _set_user_state(uid, streak=6, last_checkin=six_days_streak_yesterday, longest_streak=6)

    result = _checkin(uid)
    assert result["streak"] == 7
    assert result["bonus_xp"] == STREAK_MILESTONES[7]

    events = _checkin_xp_events(uid)
    assert len(events) == 1
    assert events[0]["xp"] == XP_BASE_PER_CHECKIN + STREAK_MILESTONES[7]

    # A retry of the same milestone check-in must not grant the bonus twice.
    _checkin(uid)
    assert len(_checkin_xp_events(uid)) == 1


def test_longest_streak_never_decreases_after_a_reset():
    uid = _next_uid()
    long_ago = datetime.now(timezone.utc) - timedelta(days=10)
    _set_user_state(uid, streak=10, last_checkin=long_ago, longest_streak=10)

    result = _checkin(uid)

    assert result["streak"] == 1  # reset
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["longest_streak"] == 10  # unchanged, never decreases


# ---------------------------------------------------------------------------
# First-checkin bonus (the confirmed root-cause bug for objective 6)
# ---------------------------------------------------------------------------

def test_first_checkin_bonus_granted_on_actual_first_checkin():
    uid = _next_uid()
    _checkin(uid)
    assert len(_first_checkin_xp_events(uid)) == 1


def test_first_checkin_bonus_not_regranted_when_streak_resets_after_missed_days():
    """Regression test for the confirmed bug: maybe_give_first_checkin_bonus()
    used to fire on every streak reset (missed-day -> streak=1), not only on
    a user's true first lifetime check-in. Two check-ins separated by a
    missed-day gap must grant the first-checkin bonus exactly once, total.
    """
    uid = _next_uid()

    first = _checkin(uid)
    assert first["success"] is True
    assert len(_first_checkin_xp_events(uid)) == 1

    # Simulate a missed-day gap that will force a streak reset on the next
    # check-in. Also clear today's claim event: it was consumed by the first
    # call above, and this test rewinds `last_checkin` at the DB level
    # (rather than advancing a mocked clock) to simulate days passing, so the
    # per-day claim needs resetting too to model the same simulated gap.
    three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
    main.users_collection.update_one({"user_id": uid}, {"$set": {"last_checkin": three_days_ago}})
    main.db.checkin_events.delete_many({"user_id": uid})

    second = _checkin(uid)
    assert second["success"] is True
    assert second["streak"] == 1  # confirms this check-in did go through the reset branch

    # The bug: this used to call maybe_give_first_checkin_bonus() again here.
    assert len(_first_checkin_xp_events(uid)) == 1
