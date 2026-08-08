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

def test_two_concurrent_requests_only_one_wins_the_streak_mutation():
    """Simulates two Gunicorn workers racing on the same user+day.

    Both requests read the same pre-checkin state before either commits.
    The claim and the streak/last_checkin mutation are the same compare-
    and-swap update (filtered on the exact last_checkin value each request
    read), so the second request to reach that update simply fails to
    match (matched_count == 0) — it must not re-mutate streak/last_checkin/
    longest_streak beyond what the winner already applied, even though (see
    test_losing_racer_heals_winners_missing_xp_grant below) it may still
    grant XP as a idempotent catch-up.
    """
    uid = _next_uid()
    yesterday_kl = datetime.now(KL_TZ).date() - timedelta(days=1)
    yesterday_utc = datetime.combine(yesterday_kl, datetime.min.time(), tzinfo=KL_TZ).astimezone(timezone.utc)
    _set_user_state(uid, streak=3, last_checkin=yesterday_utc, longest_streak=3)

    # Pretend a concurrent request already won the race, applied its streak
    # mutation, AND successfully granted XP (the common case) before this
    # request's CAS update runs.
    winner_now = datetime.now(timezone.utc)
    main.users_collection.update_one(
        {"user_id": uid},
        {"$set": {"streak": 4, "last_checkin": winner_now}, "$max": {"longest_streak": 4}},
    )
    main.db.xp_events.insert_one({"user_id": uid, "unique_key": "checkin:" + datetime.now(KL_TZ).strftime("%Y%m%d"), "type": "checkin", "xp": XP_BASE_PER_CHECKIN, "created_at": winner_now})
    main.db.xp_ledger.insert_one({"user_id": uid, "source": "checkin", "source_id": "checkin:" + datetime.now(KL_TZ).strftime("%Y%m%d"), "amount": XP_BASE_PER_CHECKIN, "created_at": winner_now})

    result = _checkin(uid)

    assert result["success"] is False
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["streak"] == 4
    assert doc["longest_streak"] == 4
    # Already granted by the "winner" above — the loser's idempotent
    # catch-up call must not create a second XP event.
    assert len(_checkin_xp_events(uid)) == 1


def test_losing_racer_heals_winners_missing_xp_grant():
    """Recovery scenario: the request that wins the streak CAS crashes (or
    Mongo raises) before it reaches grant_xp(). Streak/last_checkin are
    correctly committed, but XP for today is missing. A second request for
    the same user+day (a losing racer, or simply a frontend retry) must
    heal the missing XP rather than silently accept "already checked in"
    with the grant lost forever.
    """
    uid = _next_uid()
    yesterday_kl = datetime.now(KL_TZ).date() - timedelta(days=1)
    yesterday_utc = datetime.combine(yesterday_kl, datetime.min.time(), tzinfo=KL_TZ).astimezone(timezone.utc)
    _set_user_state(uid, streak=3, last_checkin=yesterday_utc, longest_streak=3)

    # Simulate the winner's streak CAS having committed, but its process
    # dying before grant_xp("checkin", ...) ran.
    main.users_collection.update_one(
        {"user_id": uid},
        {"$set": {"streak": 4, "last_checkin": datetime.now(timezone.utc)}, "$max": {"longest_streak": 4}},
    )
    assert len(_checkin_xp_events(uid)) == 0  # confirm the simulated partial failure

    result = _checkin(uid)

    assert result["success"] is False  # streak already advanced; not re-applied
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["streak"] == 4  # untouched by the healing call
    events = _checkin_xp_events(uid)
    assert len(events) == 1  # healed, exactly once
    assert events[0]["xp"] == XP_BASE_PER_CHECKIN

    # A third request (another retry) must not grant it again.
    _checkin(uid)
    assert len(_checkin_xp_events(uid)) == 1


def test_retry_after_incomplete_checkin_heals_first_checkin_bonus_too():
    """Same recovery scenario as above, but for a user's very first check-in
    ever — both the base check-in XP and the first-checkin bonus must be
    healed by a retry, and neither is granted twice by a subsequent one.
    """
    uid = _next_uid()
    # Simulate: streak CAS for a brand-new user's first check-in committed,
    # but grant_xp/record_first_checkin never ran.
    _set_user_state(uid, streak=1, last_checkin=datetime.now(timezone.utc), longest_streak=1)

    result = _checkin(uid)

    assert result["success"] is False
    assert len(_checkin_xp_events(uid)) == 1
    assert len(_first_checkin_xp_events(uid)) == 1

    _checkin(uid)
    assert len(_checkin_xp_events(uid)) == 1
    assert len(_first_checkin_xp_events(uid)) == 1


def test_retry_heals_bonus_after_first_checkin_marker_committed():
    """Narrower crash window than the test above: first_checkin_at was
    already committed by record_first_checkin() on a prior attempt (so
    record_first_checkin() now returns False, since its atomic $set only
    fires the one time), but the process crashed before the following
    grant_xp("first_checkin", ...) call. The healing path must not depend
    on record_first_checkin()'s return value — it must grant the bonus
    based on the authoritative state (first_checkin_at exists, and no
    first_checkin XP event exists yet), not on being told "I just set it".
    """
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    # Streak CAS committed, first_checkin_at committed (as record_first_checkin
    # would have left it), but grant_xp("checkin", ...) and
    # grant_xp("first_checkin", ...) never ran — the crash happened between
    # steps 3 and 4 of the documented operation order.
    _set_user_state(uid, streak=1, last_checkin=now, longest_streak=1, first_checkin_at=now)
    assert len(_checkin_xp_events(uid)) == 0
    assert len(_first_checkin_xp_events(uid)) == 0

    # Sanity-check the premise: record_first_checkin() really does return
    # False here, since first_checkin_at already exists.
    assert main.record_first_checkin(uid, ref=now) is False

    result = _checkin(uid)

    assert result["success"] is False  # streak already applied; not re-applied
    assert len(_checkin_xp_events(uid)) == 1
    assert len(_first_checkin_xp_events(uid)) == 1  # healed despite record_first_checkin() -> False

    # A further retry must not grant it a second time.
    _checkin(uid)
    assert len(_checkin_xp_events(uid)) == 1
    assert len(_first_checkin_xp_events(uid)) == 1


def test_healing_does_not_grant_xp_for_unrelated_stale_state():
    """The CAS-loss healing path must only fire when the committed
    last_checkin is actually today's KL date — never for some other change
    that merely happens to break our CAS filter's equality match.

    Uses a mocked find_one to force process_checkin to *read* a stale
    snapshot while the real stored document has already moved on to a
    still-not-today value — reproducing the ordering a genuine concurrent
    write would need (something no single-threaded test can otherwise
    arrange against a live find-then-update).
    """
    # database.ensure_indexes() is guarded by a module-level "already ran"
    # flag and so does not re-create the unique index on users.user_id
    # against this test's fresh per-test mongomock db (see
    # _fresh_mongomock_db above) — create it explicitly, since this test's
    # CAS-loses-via-stale-read scenario depends on that index to correctly
    # reject the resulting phantom "insert" rather than silently create a
    # second document for the same user_id.
    main.users_collection.create_index([("user_id", 1)], unique=True)

    uid = _next_uid()
    yesterday_kl = datetime.now(KL_TZ).date() - timedelta(days=1)
    yesterday_utc = datetime.combine(yesterday_kl, datetime.min.time(), tzinfo=KL_TZ).astimezone(timezone.utc)
    _set_user_state(uid, streak=3, last_checkin=yesterday_utc, longest_streak=3)

    stale_snapshot = main.users_collection.find_one({"user_id": uid})

    # After process_checkin's initial read, but before its CAS runs, some
    # other change lands — moving last_checkin to a *different* value that
    # is still not today's KL date.
    other_stale_value = yesterday_utc - timedelta(days=3)
    main.users_collection.update_one({"user_id": uid}, {"$set": {"last_checkin": other_stale_value}})

    real_find_one = main.users_collection.find_one
    call_count = {"n": 0}

    def _find_one_once_stale(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return stale_snapshot
        return real_find_one(*args, **kwargs)

    with mock.patch.object(main.users_collection, "find_one", side_effect=_find_one_once_stale):
        result = _checkin(uid)

    # The CAS filter (built from the stale read) no longer matches, so this
    # request loses the race — and since the actually-committed last_checkin
    # still isn't today, the healing call must not fire.
    assert len(_checkin_xp_events(uid)) == 0
    assert result["success"] is False
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["streak"] == 3


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


def test_concurrent_brand_new_user_race_does_not_500():
    """Two requests for the same never-seen-before user_id both read "no
    user" and both attempt the CAS as an upsert. Only one insert can win;
    the other must hit DuplicateKeyError on the unique users.user_id index,
    catch it, reload the now-committed user, and run the same-day healing
    path — not let the exception propagate into a 500.

    Reproduces the read-before-either-commits ordering (which a
    single-threaded test can't otherwise arrange against a live
    find-then-update) the same way test_healing_does_not_grant_xp_for_...
    does: mock the *first* find_one call to report "no user found" while a
    real competing insert has already landed.
    """
    # Same fixture gap as test_healing_does_not_grant_xp_for_unrelated_stale_state:
    # ensure_indexes()'s "already ran" guard means this test's fresh
    # mongomock db doesn't otherwise get the unique index this scenario
    # depends on.
    main.users_collection.create_index([("user_id", 1)], unique=True)

    uid = _next_uid()

    real_find_one = main.users_collection.find_one
    call_count = {"n": 0}

    def _find_one_first_call_missing(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Simulate: process_checkin's own read finds nothing, because the
            # winning concurrent request's insert hasn't landed yet from this
            # request's point of view.
            return None
        return real_find_one(*args, **kwargs)

    # Perform the "winning" concurrent request's own insert (streak=1,
    # last_checkin=now, first_checkin_at + first_checkin XP granted — a
    # fully completed competing check-in) before this request's CAS attempt
    # runs; the mock above makes this request's own read report "no user"
    # regardless, reproducing the race ordering.
    winner_now = datetime.now(timezone.utc)
    main.users_collection.insert_one({
        "user_id": uid, "streak": 1, "last_checkin": winner_now, "longest_streak": 1,
        "first_checkin_at": winner_now,
    })
    main.db.xp_events.insert_one({"user_id": uid, "unique_key": f"checkin:{datetime.now(KL_TZ).strftime('%Y%m%d')}", "type": "checkin", "xp": XP_BASE_PER_CHECKIN, "created_at": winner_now})
    main.db.xp_ledger.insert_one({"user_id": uid, "source": "checkin", "source_id": f"checkin:{datetime.now(KL_TZ).strftime('%Y%m%d')}", "amount": XP_BASE_PER_CHECKIN, "created_at": winner_now})

    # /api/checkin takes its identity from verified Telegram initData, so the
    # request must be authenticated as `uid`. This is orthogonal to the race
    # being exercised here (the auth step does not touch users_collection, so
    # the find_one call ordering the mock depends on is unchanged).
    with mock.patch.object(main.users_collection, "find_one", side_effect=_find_one_first_call_missing), \
         mock.patch.object(main, "_extract_verified_telegram_user_id", return_value=(uid, None)):
        client = main.app.test_client()
        response = client.post("/api/checkin", json={"user_id": uid, "username": "bob"})

    assert response.status_code == 200
    body = response.get_json()
    assert body["success"] is False

    # The loser's process_checkin call must have caught the DuplicateKeyError
    # (not propagated it) and healed against the committed winner state: the
    # winner already had XP granted, so no duplicate grant.
    assert len(_checkin_xp_events(uid)) == 1
    assert len(_first_checkin_xp_events(uid)) == 1
    doc = main.users_collection.find_one({"user_id": uid})
    assert doc["streak"] == 1  # untouched by the loser


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
