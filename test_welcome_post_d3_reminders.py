"""Regression tests for the post-D3 Welcome Voucher conversion patches:

  - Patch 1: immediate check-in response copy (1/3, 2/3, 3/3 unlock /
    3/3-channel-pending) sourced from get_welcome_progress, not users.streak.
  - Patch 2: immediate Telegram unlock push on the genuine
    checkins_completed < required -> >= required transition, exactly once.
  - Patch 3: ~4h-after-unlock "unclaimed" nudge inside process_welcome_reminders.
  - Patch 4: ~24h-before-expiry warning inside process_welcome_reminders.

Two layers are exercised:
  - main.process_checkin() end to end against mongomock (same harness as
    test_welcome_checkin_progress_main.py) for Patches 1 and 2.
  - scheduler.process_welcome_reminders() / trigger_welcome_unlock_push()
    directly against mongomock collections for Patches 3, 4 and the
    existing D1/D2/recovery stages (regression check that they still work
    once the post-D3 stages share the same cursor/loop).
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
from config import KL_TZ  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_mongomock_db():
    database._client = mongomock.MongoClient()
    database._db = database._client["referral_bot"]
    settings_service.invalidate_cache()
    importlib.reload(vouchers)
    importlib.reload(scheduler)
    yield


def _next_uid(_counter=[950000]):
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


def _eligible_patches(*, channel_joined=True):
    return (
        mock.patch.object(vouchers, "welcome_eligibility", lambda _uid, ref=None: (True, "ok", {"status": "issued"})),
        mock.patch.object(vouchers, "_has_current_subscription_evidence", lambda _uid: channel_joined),
    )


def _checkin_on(uid, day_offset, *, channel_joined=True):
    """Drive process_checkin for the given uid on KL-day (join + day_offset)."""
    joined = datetime.now(KL_TZ) - timedelta(days=day_offset)
    _make_eligible_user(uid, joined=joined)
    p1, p2 = _eligible_patches(channel_joined=channel_joined)
    with p1, p2:
        return asyncio.run(main.process_checkin(uid, "alice", None))


# ---------------------------------------------------------------------------
# Patch 1 / 5: immediate check-in response copy
# ---------------------------------------------------------------------------

def test_d1_checkin_returns_one_of_three_progress_copy():
    uid = _next_uid()
    with mock.patch.object(main, "_send_welcome_claim_cta_via_bot", lambda uid, text: True):
        result = _checkin_on(uid, 0)

    assert result["success"] is True
    assert result["welcome_celebration"] == "day1"
    assert result["welcome_progress"]["completed"] == 1
    assert "1/3 Complete" in result["welcome_message"]
    assert "2 check-ins left" in result["welcome_message"]


def test_process_checkin_fetches_welcome_progress_once_per_request():
    """Review fix: build_welcome_checkin_copy and trigger_welcome_unlock_push
    must share ONE get_welcome_progress() read rather than each fetching
    their own — that call can fall through to a live Telegram getChatMember
    lookup on a subscription cache miss, so a redundant fetch would double
    that cost on every check-in. record_welcome_checkin_progress (existing,
    unrelated code — it needs its own read to detect the day1/day2/day3
    transitions) accounts for the other call, so the total is 2, not 3."""
    uid = _next_uid()
    joined = datetime.now(KL_TZ)
    _make_eligible_user(uid, joined=joined)
    calls = []
    real_get_welcome_progress = vouchers.get_welcome_progress

    def counting_get_welcome_progress(u, now=None):
        calls.append(u)
        return real_get_welcome_progress(u, now=now)

    p1, p2 = _eligible_patches()
    with p1, p2, mock.patch.object(vouchers, "get_welcome_progress", counting_get_welcome_progress):
        result = asyncio.run(main.process_checkin(uid, "alice", None))

    assert result["success"] is True
    assert calls == [uid, uid]


def test_d2_checkin_returns_two_of_three_progress_copy():
    """Exercises main.build_welcome_checkin_copy directly (the function
    process_checkin calls for Patch 1/5 copy) rather than driving process_checkin
    through multiple KL calendar days, since process_checkin reads
    datetime.now(KL_TZ) internally and isn't day-injectable — see
    test_welcome_checkin_progress_main.py, which uses the same
    direct-progress-write pattern for day2/day3 for the same reason."""
    uid = _next_uid()
    progress = {"eligible": True, "claimed": False, "expired": False, "completed": 2, "required": 3, "channel_joined": True}
    result = main.build_welcome_checkin_copy(uid, user_doc={}, progress=progress)

    assert result["welcome_celebration"] == "day2"
    assert result["welcome_progress"]["completed"] == 2
    assert "2/3 Complete" in result["welcome_message"]
    assert "1 more check-in" in result["welcome_message"]


def test_d3_checkin_returns_unlock_copy_when_channel_joined():
    uid = _next_uid()
    progress = {"eligible": True, "claimed": False, "expired": False, "completed": 3, "required": 3, "channel_joined": True}
    result = main.build_welcome_checkin_copy(uid, user_doc={}, progress=progress)

    assert result["welcome_celebration"] == "unlock"
    assert result["welcome_progress"]["completed"] == 3
    assert "3/3 Complete" in result["welcome_message"]
    assert "Claim it now" in result["welcome_message"]


def test_d3_checkin_channel_missing_does_not_claim_unlockable():
    """3/3 check-ins done but channel subscription still missing must never
    say "claim it now" — Patch 1's explicit anti-false-positive requirement."""
    uid = _next_uid()
    progress = {"eligible": True, "claimed": False, "expired": False, "completed": 3, "required": 3, "channel_joined": False}
    result = main.build_welcome_checkin_copy(uid, user_doc={}, progress=progress)

    assert result["welcome_progress"]["completed"] == 3
    assert result["welcome_celebration"] == "unlock_pending_channel"
    assert "Claim it now" not in result["welcome_message"]
    assert "verify your Official Channel" in result["welcome_message"]


def test_users_outside_welcome_journey_keep_generic_checkin_behavior():
    uid = _next_uid()
    joined = datetime.now(KL_TZ)
    _make_eligible_user(uid, joined=joined)
    with mock.patch.object(vouchers, "welcome_eligibility", lambda _uid, ref=None: (False, "not_in_window", None)):
        result = asyncio.run(main.process_checkin(uid, "alice", None))

    assert result["success"] is True
    assert "welcome_celebration" not in result
    assert "welcome_message" not in result


# ---------------------------------------------------------------------------
# Patch 2: immediate unlock push, exactly once
# ---------------------------------------------------------------------------

def test_d3_checkin_triggers_immediate_unlock_push_once():
    """A single check-in call that lands the genuine <3 -> 3/3 transition
    (progress mocked deterministic, since process_checkin's day boundary
    isn't independently controllable across multiple real calls in-test)
    triggers exactly one Telegram unlock push."""
    uid = _next_uid()
    joined = datetime.now(KL_TZ)
    _make_eligible_user(uid, joined=joined)
    sent = []
    p1, p2 = _eligible_patches()
    with (
        p1, p2,
        mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
            "eligible": True, "claimed": False, "expired": False, "completed": 3, "required": 3, "channel_joined": True,
        }),
        mock.patch.object(main, "_send_welcome_claim_cta_via_bot", lambda u, text: (sent.append((u, text)), True)[-1]),
    ):
        result = asyncio.run(main.process_checkin(uid, "alice", None))

    assert result["success"] is True
    assert len(sent) == 1
    assert sent[0][0] == uid
    assert "unlocked" in sent[0][1].lower()
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("unlock_push_sent_at") is not None
    assert "unlock_push_claimed_at" not in doc


def test_duplicate_d3_checkin_request_does_not_duplicate_push():
    """A same-day retry/duplicate POST to /api/checkin after D3 must not
    re-trigger the unlock push (process_checkin's own CAS already rejects
    the retry as 'already checked in today', so trigger_welcome_unlock_push
    is only reached once — but assert the send-count directly)."""
    uid = _next_uid()
    joined = datetime.now(KL_TZ) - timedelta(days=2)
    _make_eligible_user(uid, joined=joined)
    sent = []
    p1, p2 = _eligible_patches()
    with p1, p2, mock.patch.object(main, "_send_welcome_claim_cta_via_bot", lambda u, text: (sent.append(u), True)[-1]):
        asyncio.run(main.process_checkin(uid, "alice", None))
        # advance two more KL days for D2/D3 via direct progress writes (mirrors
        # test_welcome_checkin_progress_main.py's own day-advance pattern)
        with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {"eligible": True, "completed": 2, "required": 3, "claimed": False, "expired": False, "channel_joined": True}):
            vouchers.record_welcome_checkin_progress(uid, now=joined + timedelta(days=1))
        with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {"eligible": True, "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True}):
            vouchers.record_welcome_checkin_progress(uid, now=joined + timedelta(days=2))
            scheduler.trigger_welcome_unlock_push(uid, bot_send_fn=main._send_welcome_claim_cta_via_bot)
            # Duplicate/retry attempt for the same already-unlocked user.
            scheduler.trigger_welcome_unlock_push(uid, bot_send_fn=main._send_welcome_claim_cta_via_bot)

    assert len(sent) == 1


def test_concurrent_d3_completion_does_not_double_send():
    """Two 'concurrent' callers racing trigger_welcome_unlock_push for the
    same user (e.g. the eager check-in-time push racing the hourly sweep)
    must only result in one atomic claim winner and one send."""
    uid = _next_uid()
    joined = datetime.now(KL_TZ) - timedelta(days=2)
    _make_eligible_user(uid, joined=joined)
    vouchers.welcome_reminders_col.update_one(
        {"user_id": uid}, {"$set": {"user_id": uid, "day1_at": joined, "day2_at": joined}}, upsert=True,
    )
    sent = []

    def fake_bot_send(u, text):
        sent.append(u)
        return True

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "eligible": True, "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True,
    }):
        r1 = scheduler.trigger_welcome_unlock_push(uid, bot_send_fn=fake_bot_send)
        r2 = scheduler.trigger_welcome_unlock_push(uid, bot_send_fn=fake_bot_send)

    assert len(sent) == 1
    assert {r1["status"], r2["status"]} == {"sent", "not_claimed"}


# ---------------------------------------------------------------------------
# Patch 3 / 4: process_welcome_reminders unclaimed + expiry stages
# ---------------------------------------------------------------------------

def _seed_reminder_doc(uid, **fields):
    doc = {
        "user_id": uid, "reminder_20h_sent": True, "reminder_28h_sent": True, "day2_reminder_sent": True,
        # Unlock push already handled in an earlier sweep/eager call, unless
        # the caller overrides this — keeps unclaimed/expiry tests isolated
        # to the one stage under test.
        "unlock_push_sent_at": datetime.now(timezone.utc) - timedelta(hours=6),
    }
    doc.update(fields)
    vouchers.welcome_reminders_col.insert_one(doc)
    return doc


def test_unclaimed_reminder_sends_once_after_4h():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    completed_at = now - timedelta(hours=5)
    _seed_reminder_doc(uid, completed_at=completed_at)
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True, "eligible_until": None,
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db,
            claim_send_fn=lambda u, text: (sent.append((u, text)), (True, None, False))[-1],
        )
        # Second sweep must not resend.
        result2 = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db,
            claim_send_fn=lambda u, text: (sent.append((u, text)), (True, None, False))[-1],
        )

    assert result["unclaimed_reminder_sent"] == 1
    assert result2["unclaimed_reminder_sent"] == 0
    assert len(sent) == 1
    assert "waiting" in sent[0][1].lower()
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("unclaimed_reminder_sent_at") is not None


def test_claimed_user_does_not_receive_unclaimed_reminder():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    _seed_reminder_doc(uid, completed_at=now - timedelta(hours=5))
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": True, "expired": False, "channel_joined": True, "eligible_until": None,
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, claim_send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    # No message is ever sent to a claimed user...
    assert result["unclaimed_reminder_sent"] == 0
    assert sent == []
    # ...but the terminal-exit fix (see test_terminal_claimed_user_leaves_the_
    # reminder_cursor) still stamps the field so this permanently-claimed doc
    # leaves the hourly cursor instead of being rescanned forever.
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("unclaimed_reminder_sent_at") is not None


def test_terminal_claimed_user_leaves_the_reminder_cursor():
    """Review fix: a permanently-claimed user must not keep matching the
    hourly cursor's $or-of-not-yet-sent-flags forever (it's unsorted and
    limit()-capped, so terminal users would otherwise crowd out users who
    still need a reminder)."""
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    vouchers.welcome_reminders_col.insert_one({
        "user_id": uid, "completed_at": now - timedelta(hours=5),
        "reminder_20h_sent": True, "reminder_28h_sent": True, "day2_reminder_sent": True,
    })

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": True, "expired": False, "channel_joined": True, "eligible_until": None,
    }):
        result1 = scheduler.process_welcome_reminders(now_ref=now, db_ref=database.db)
        result2 = scheduler.process_welcome_reminders(now_ref=now, db_ref=database.db)

    assert result1["scanned"] == 1
    assert result2["scanned"] == 0
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("unlock_push_sent_at") is not None
    assert doc.get("unclaimed_reminder_sent_at") is not None
    assert doc.get("expiry_warning_sent_at") is not None


def test_stale_unlock_push_claim_is_reclaimed():
    """Review fix: a claim left behind by a worker that died between the
    atomic claim and the finally-release (hard kill/OOM, not an exception)
    must not wedge the stage forever — it becomes reclaimable once older
    than WELCOME_CLAIM_STALE_MINUTES."""
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    stale_claim_at = now - timedelta(minutes=scheduler.WELCOME_CLAIM_STALE_MINUTES + 1)
    vouchers.welcome_reminders_col.insert_one({
        "user_id": uid, "completed_at": now - timedelta(hours=1), "unlock_push_claimed_at": stale_claim_at,
    })
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True, "eligible_until": None,
    }):
        result = scheduler.trigger_welcome_unlock_push(
            uid, now_ref=now, db_ref=database.db, bot_send_fn=lambda u, text: (sent.append(u), True)[-1],
        )

    assert result["status"] == "sent"
    assert sent == [uid]
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("unlock_push_sent_at") is not None


def test_fresh_unlock_push_claim_is_not_reclaimed_by_a_racer():
    """The flip side of the stale-claim test: a claim younger than
    WELCOME_CLAIM_STALE_MINUTES must still block a concurrent racer."""
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    fresh_claim_at = now - timedelta(minutes=1)
    vouchers.welcome_reminders_col.insert_one({
        "user_id": uid, "completed_at": now - timedelta(hours=1), "unlock_push_claimed_at": fresh_claim_at,
    })
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True, "eligible_until": None,
    }):
        result = scheduler.trigger_welcome_unlock_push(
            uid, now_ref=now, db_ref=database.db, bot_send_fn=lambda u, text: (sent.append(u), True)[-1],
        )

    assert result["status"] == "not_claimed"
    assert sent == []


def test_expiry_warning_sends_once():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    eligible_until = now + timedelta(hours=10)
    _seed_reminder_doc(uid, completed_at=now - timedelta(hours=1))
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True,
        "eligible_until": eligible_until.isoformat(),
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db,
            claim_send_fn=lambda u, text: (sent.append((u, text)), (True, None, False))[-1],
        )
        result2 = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db,
            claim_send_fn=lambda u, text: (sent.append((u, text)), (True, None, False))[-1],
        )

    assert result["expiry_warning_sent"] == 1
    assert result2["expiry_warning_sent"] == 0
    assert len(sent) == 1
    assert "expires soon" in sent[0][1].lower()


def test_claimed_user_does_not_receive_expiry_warning():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    eligible_until = now + timedelta(hours=10)
    _seed_reminder_doc(uid, completed_at=now - timedelta(hours=1))
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": True, "expired": False, "channel_joined": True,
        "eligible_until": eligible_until.isoformat(),
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, claim_send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    assert result["expiry_warning_sent"] == 0
    assert sent == []


def test_expired_user_does_not_receive_expiry_warning():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    eligible_until = now - timedelta(hours=1)  # already past
    _seed_reminder_doc(uid, completed_at=now - timedelta(hours=30))
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": False, "expired": True, "channel_joined": True,
        "eligible_until": eligible_until.isoformat(),
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, claim_send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    assert result["expiry_warning_sent"] == 0
    assert sent == []


def test_blocked_telegram_user_suppressed_for_post_d3_stages():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    _seed_reminder_doc(uid, completed_at=now - timedelta(hours=5))
    main.users_collection.update_one({"user_id": uid}, {"$set": {"user_id": uid, "pm_blocked": True}}, upsert=True)
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 3, "required": 3, "claimed": False, "expired": False, "channel_joined": True, "eligible_until": None,
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, claim_send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    assert sent == []
    assert result["skip_breakdown"]["bot_blocked"] >= 1
    doc = vouchers.welcome_reminders_col.find_one({"user_id": uid})
    assert doc.get("unclaimed_reminder_sent_at") is None


# ---------------------------------------------------------------------------
# Regression: existing D1/D2/recovery stages still work with the new cursor
# filter and the new post-D3 blocks sharing the same per-user loop.
# ---------------------------------------------------------------------------

def test_existing_d1_to_d2_reminder_still_works():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    _seed_reminder_doc(uid, day1_at=now - timedelta(hours=21), reminder_20h_sent=False, reminder_28h_sent=False, day2_reminder_sent=False)
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 1, "required": 3, "claimed": False, "expired": False, "channel_joined": False, "eligible_until": None,
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    assert result["reminder_20h_sent"] == 1
    assert sent == [uid]


def test_existing_d2_to_d3_reminder_still_works():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    _seed_reminder_doc(uid, day1_at=now - timedelta(days=1, hours=21), day2_at=now - timedelta(hours=21), day2_reminder_sent=False)
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 2, "required": 3, "claimed": False, "expired": False, "channel_joined": False, "eligible_until": None,
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    assert result["day2_reminder_sent"] == 1
    assert sent == [uid]


def test_existing_recovery_reminder_still_works():
    uid = _next_uid()
    now = datetime.now(timezone.utc)
    day1_at = now - timedelta(hours=90)
    _seed_reminder_doc(uid, day1_at=day1_at, reminder_20h_sent=True, reminder_28h_sent=True, recovery_sent=False)
    sent = []

    with mock.patch.object(vouchers, "get_welcome_progress", lambda u, now=None: {
        "completed": 1, "required": 3, "claimed": False, "expired": False, "channel_joined": False, "eligible_until": None,
    }):
        result = scheduler.process_welcome_reminders(
            now_ref=now, db_ref=database.db, send_fn=lambda u, text: (sent.append(u), (True, None, False))[-1],
        )

    assert result["recovery_sent"] == 1
    assert sent == [uid]


def test_get_welcome_progress_additive_fields_do_not_change_core_output():
    """Patch 1's channel_joined/eligible_until additions must be purely
    additive — existing eligibility/unlock math and keys stay unchanged."""
    uid = _next_uid()
    joined = datetime.now(KL_TZ)
    _make_eligible_user(uid, joined=joined)
    p1, p2 = _eligible_patches(channel_joined=True)
    with p1, p2:
        out = vouchers.get_welcome_progress(uid, now=joined)

    assert out["eligible"] is True
    assert out["completed"] == 0
    assert out["required"] == 3
    assert out["status"] == "in_progress"
    assert out["channel_joined"] is True
    assert "eligible_until" in out
