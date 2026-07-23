"""Tests for community_centre.py: Community Centre content composer, poll
manager and restart-safe scheduler.

Covers: validation (poll/quiz/button/media limits), KL<->UTC scheduling
conversions and poll-close-vs-publish validation, the atomic worker claim +
run-ledger idempotency boundary, stale-processing recovery, poll_answer
upsert semantics (change/remove vote, anonymous exclusion), stop-poll
idempotency, and admin-endpoint permission gating.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from flask import Flask
from pymongo.errors import DuplicateKeyError as RealDuplicateKeyError

import database
import community_centre as cc
from fake_mongo import FakeDb


# Telegram Configuration settings-doc shape with every builtin-destination
# field unset/invalid, so tests that don't care about the Telegram
# Configuration -> Community Centre resolver see exactly the custom
# destinations they create via _make_destination(), same as before that
# resolver existed. Tests that DO care override settings_service.get_settings
# themselves (see the resolver tests below).
NO_BUILTIN_TELEGRAM_CONFIG = {
    "official_channel_id": "",
    "official_channel_username": "",
    "main_group_id": "0",
    "community_chat_id": "0",
}


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "community_post_runs": [("community_post_id", "run_key")],
        "community_poll_answers": [("poll_id", "user_id")],
        "community_poll_snapshots": [("poll_id",)],
        "community_destinations": [("key",)],
    })
    monkeypatch.setattr(database, "db", fdb)
    # community_centre.py catches pymongo's DuplicateKeyError specifically;
    # make fake_mongo's raise the real class so the except clause matches.
    monkeypatch.setattr("fake_mongo.DuplicateKeyError", RealDuplicateKeyError, raising=False)
    # Isolate from the real app_settings collection / DatabaseProxy (which
    # settings_service binds at import time via `from database import db`,
    # so patching database.db above does not reach it).
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: dict(NO_BUILTIN_TELEGRAM_CONFIG))
    return fdb


def _make_destination(key="official_channel", **overrides):
    payload = {
        "key": key,
        "name": "Official Channel",
        "chat_id": -1001234567890,
        "chat_type": "channel",
        "enabled": True,
        "allow_posts": True,
        "allow_polls": True,
        "allow_pin": True,
    }
    payload.update(overrides)
    dest, err = cc.upsert_destination(payload, actor_id=1)
    assert err is None, err
    return dest


def _text_payload(**overrides):
    payload = {
        "title": "Weekend Announcement",
        "content_type": "text",
        "destination_key": "official_channel",
        "text": "Hello world",
        "parse_mode": "HTML",
    }
    payload.update(overrides)
    return payload


def _poll_payload(**overrides):
    payload = {
        "title": "Reward Poll",
        "content_type": "poll",
        "destination_key": "official_channel",
        "poll": {
            "question": "Which reward do you prefer?",
            "options": [{"text": "Voucher"}, {"text": "XP"}],
            "is_anonymous": True,
            "allows_multiple_answers": False,
            "close_mode": "manual",
        },
    }
    payload.update(overrides)
    return payload


def _quiz_payload(**overrides):
    payload = {
        "title": "Trivia Quiz",
        "content_type": "quiz",
        "destination_key": "official_channel",
        "poll": {
            "question": "2 + 2 = ?",
            "options": [{"text": "3"}, {"text": "4"}, {"text": "5"}],
            "is_anonymous": True,
            "correct_option_id": 1,
            "close_mode": "manual",
        },
    }
    payload.update(overrides)
    return payload


# ---------------------------------------------------------------------------
# Validation: standard content
# ---------------------------------------------------------------------------

def test_create_text_post_draft(fake_db):
    _make_destination()
    post, err = cc.create_post(_text_payload(), actor_id=1, actor_username="admin")
    assert err is None
    assert post["status"] == "draft"
    assert post["text"] == "Hello world"
    assert post["created_by"] == 1


def test_arbitrary_destination_rejected(fake_db):
    _make_destination()
    post, err = cc.create_post(_text_payload(destination_key="not_configured"), actor_id=1)
    assert post is None
    assert err == "invalid_destination"


def test_destination_disallows_posts(fake_db):
    _make_destination(allow_posts=False)
    post, err = cc.create_post(_text_payload(), actor_id=1)
    assert err == "destination_disallows_posts"


def test_caption_html_sanitized(fake_db):
    _make_destination()
    payload = _text_payload(text="<b>bold</b><script>alert(1)</script>")
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert "<script>" not in post["text"]
    assert "<b>bold</b>" in post["text"]


def test_media_group_size_bounds(fake_db):
    _make_destination()
    payload = {
        "title": "Album",
        "content_type": "media_group",
        "destination_key": "official_channel",
        "media": [{"type": "photo", "source_url": "https://example.com/1.jpg"}],
    }
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "bad_media_group_size"


def test_media_requires_https(fake_db):
    _make_destination()
    payload = {
        "title": "Photo",
        "content_type": "photo",
        "destination_key": "official_channel",
        "media": [{"type": "photo", "source_url": "http://example.com/1.jpg"}],
    }
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "https_required"


# ---------------------------------------------------------------------------
# Buttons
# ---------------------------------------------------------------------------

def test_button_invalid_url_rejected(fake_db):
    _make_destination()
    payload = _text_payload(buttons=[{"row": 0, "position": 0, "text": "Click", "type": "url", "value": "javascript:alert(1)"}])
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "disallowed_protocol"


def test_button_unapproved_callback_rejected(fake_db):
    _make_destination()
    payload = _text_payload(buttons=[{"row": 0, "position": 0, "text": "Go", "type": "callback", "value": "delete_everything"}])
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "unapproved_callback"


def test_button_multi_row_reorder(fake_db):
    _make_destination()
    payload = _text_payload(buttons=[
        {"row": 1, "position": 0, "text": "Second row", "type": "url", "value": "https://example.com"},
        {"row": 0, "position": 0, "text": "First row", "type": "url", "value": "https://example.com"},
    ])
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert [b["text"] for b in post["buttons"]] == ["First row", "Second row"]


def test_button_colour_not_a_sending_parameter(fake_db):
    _make_destination()
    payload = _text_payload(buttons=[{"row": 0, "position": 0, "text": "Go", "type": "url", "value": "https://example.com", "colour": "red"}])
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert "colour" not in post["buttons"][0]
    assert set(post["buttons"][0].keys()) == {"row", "position", "text", "type", "value"}


# ---------------------------------------------------------------------------
# Regular polls
# ---------------------------------------------------------------------------

def test_regular_poll_single_answer(fake_db):
    _make_destination()
    post, err = cc.create_post(_poll_payload(), actor_id=1)
    assert err is None
    assert post["poll"]["type"] == "regular"
    assert post["poll"]["allows_multiple_answers"] is False


def test_regular_poll_multiple_answer(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["allows_multiple_answers"] = True
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert post["poll"]["allows_multiple_answers"] is True


def test_poll_invalid_option_count(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["options"] = [{"text": "Only one"}]
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "too_few_options"


def test_poll_duplicate_options_rejected(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["options"] = [{"text": "Same"}, {"text": "same"}]
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "duplicate_option"


def test_poll_duration_close_mode(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["close_mode"] = "duration"
    payload["poll"]["open_period_seconds"] = 300
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert post["poll"]["open_period_seconds"] == 300


def test_poll_date_close_mode(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["close_mode"] = "date"
    payload["poll"]["close_at_utc"] = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert post["poll"]["close_at_utc"] is not None


def test_poll_invalid_close_date_rejected(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["close_mode"] = "date"
    payload["poll"]["close_at_utc"] = "not-a-date"
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "bad_close_date"


def test_poll_with_buttons(fake_db):
    _make_destination()
    payload = _poll_payload(buttons=[{"row": 0, "position": 0, "text": "Info", "type": "url", "value": "https://example.com"}])
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert len(post["buttons"]) == 1


# ---------------------------------------------------------------------------
# Quizzes
# ---------------------------------------------------------------------------

def test_quiz_correct_answer_index(fake_db):
    _make_destination()
    post, err = cc.create_post(_quiz_payload(), actor_id=1)
    assert err is None
    assert post["poll"]["type"] == "quiz"
    assert post["poll"]["correct_option_id"] == 1
    assert post["poll"]["allows_multiple_answers"] is False


def test_quiz_missing_correct_answer(fake_db):
    _make_destination()
    payload = _quiz_payload()
    del payload["poll"]["correct_option_id"]
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "missing_correct_option"


def test_quiz_multiple_answers_rejected(fake_db):
    _make_destination()
    payload = _quiz_payload()
    payload["poll"]["allows_multiple_answers"] = True
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "quiz_cannot_allow_multiple"


def test_quiz_explanation_html(fake_db):
    _make_destination()
    payload = _quiz_payload()
    payload["poll"]["explanation"] = "<b>Because math.</b><script>bad()</script>"
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert "<script>" not in post["poll"]["explanation"]
    assert "<b>Because math.</b>" in post["poll"]["explanation"]


def test_quiz_explanation_too_long(fake_db):
    _make_destination()
    payload = _quiz_payload()
    payload["poll"]["explanation"] = "x" * 500
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "too_long"


# ---------------------------------------------------------------------------
# Scheduling: KL timezone, UTC storage, poll-close-vs-publish validation
# ---------------------------------------------------------------------------

def test_kl_local_naive_datetime_converted_to_utc(fake_db):
    # 3pm Kuala Lumpur (UTC+8) == 7am UTC
    kl_naive = datetime(2026, 8, 10, 15, 0)
    utc_dt = cc._parse_utc_datetime(kl_naive)
    assert utc_dt.hour == 7
    assert utc_dt.tzinfo == timezone.utc


def test_schedule_post_once(fake_db):
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=1)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    scheduled, err = cc.schedule_post(post["_id"], actor_id=1, scheduled_at_utc=future)
    assert err is None
    assert scheduled["status"] == "scheduled"
    assert scheduled["next_run_at_utc"] == future


def test_schedule_in_past_rejected(fake_db):
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=1)
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    _, err = cc.schedule_post(post["_id"], actor_id=1, scheduled_at_utc=past)
    assert err == "schedule_in_past"


def test_poll_close_before_publish_rejected(fake_db):
    """Publish 10 Aug 3pm, close 10 Aug 2pm KL — must be rejected."""
    _make_destination()
    publish_at = cc._parse_utc_datetime(datetime(2026, 8, 10, 15, 0))
    close_at = cc._parse_utc_datetime(datetime(2026, 8, 10, 14, 0))
    payload = _poll_payload()
    payload["poll"]["close_mode"] = "date"
    payload["poll"]["close_at_utc"] = close_at.isoformat()
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    _, err = cc.schedule_post(post["_id"], actor_id=1, scheduled_at_utc=publish_at)
    assert err == "poll_closes_before_publish"


def test_recurring_poll_with_fixed_close_date_rejected(fake_db):
    _make_destination()
    payload = _poll_payload(schedule_type="recurring", recurrence={"type": "daily"})
    payload["poll"]["close_mode"] = "date"
    payload["poll"]["close_at_utc"] = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    _, err = cc.create_post(payload, actor_id=1)
    assert err == "fixed_close_date_not_allowed_for_recurring"


def test_compute_next_occurrence_daily(fake_db):
    after = cc._parse_utc_datetime(datetime(2026, 8, 10, 15, 0))
    nxt = cc.compute_next_occurrence({"type": "daily"}, after)
    nxt_kl = nxt.astimezone(cc.KL_TZ)
    assert nxt_kl.day == 11
    assert nxt_kl.hour == 15


def test_compute_next_occurrence_weekly(fake_db):
    # 2026-08-10 is a Monday; next selected weekday (Wed=2) should be 2026-08-12.
    after = cc._parse_utc_datetime(datetime(2026, 8, 10, 15, 0))
    nxt = cc.compute_next_occurrence({"type": "weekly", "weekdays": [2]}, after)
    nxt_kl = nxt.astimezone(cc.KL_TZ)
    assert nxt_kl.day == 12
    assert nxt_kl.weekday() == 2


# ---------------------------------------------------------------------------
# Restart-safe worker: atomic claim, run-ledger idempotency, stale recovery
# ---------------------------------------------------------------------------

def _schedule_due_post(payload=None):
    _make_destination()
    post, _ = cc.create_post(payload or _text_payload(), actor_id=1)
    # schedule_post() requires a future time (by design — see
    # test_schedule_in_past_rejected); simulate "time has since passed" the
    # same way production does: schedule normally, then let the clock move
    # on by back-dating next_run_at_utc directly, same as a real due post.
    near_future = datetime.now(timezone.utc) + timedelta(seconds=5)
    scheduled, err = cc.schedule_post(post["_id"], actor_id=1, scheduled_at_utc=near_future)
    assert err is None, err
    past_due = datetime.now(timezone.utc) - timedelta(seconds=5)
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {
        "next_run_at_utc": past_due, "scheduled_at_utc": past_due,
    }})
    return cc.get_post(post["_id"])


def _fake_run_coro(monkeypatch, send_result=None, side_effect=None):
    async def fake_send(post):
        if side_effect:
            raise side_effect
        return send_result or {"message_ids": [111], "poll_id": None, "poll_message_id": None}

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_do_send", fake_send)
    monkeypatch.setattr(cc, "_run_coro", sync_run)


def test_worker_publishes_due_post(fake_db, monkeypatch):
    _fake_run_coro(monkeypatch)
    post = _schedule_due_post()
    processed = cc.run_due_posts(limit=5)
    assert processed == 1
    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "published"
    assert fresh["telegram_message_ids"] == [111]
    assert fresh["next_run_at_utc"] is None


def test_worker_claim_is_idempotent_across_concurrent_workers(fake_db, monkeypatch):
    """Two 'workers' racing for the same due post: only one send happens,
    and the second worker's run-ledger insert hits the unique index."""
    send_calls = {"n": 0}

    async def fake_send(post):
        send_calls["n"] += 1
        return {"message_ids": [222], "poll_id": None, "poll_message_id": None}

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_do_send", fake_send)
    monkeypatch.setattr(cc, "_run_coro", sync_run)

    post = _schedule_due_post()

    # find_one_and_update already prevents a second *claim* of the same
    # document (status flips to "processing" atomically) — simulate two
    # workers by calling run_due_posts twice; the second call finds nothing
    # left to claim.
    processed_1 = cc.run_due_posts(limit=5)
    processed_2 = cc.run_due_posts(limit=5)
    assert processed_1 == 1
    assert processed_2 == 0
    assert send_calls["n"] == 1

    # Directly exercise the run-ledger idempotency boundary too: re-running
    # _execute_publish for the same occurrence must not send twice.
    fresh = cc.get_post(post["_id"])
    fresh["status"] = "processing"
    cc._execute_publish(fresh)
    assert send_calls["n"] == 1


def test_stale_processing_is_recovered(fake_db, monkeypatch):
    post = _schedule_due_post()
    claimed = cc._claim_next_due_post()
    assert claimed["status"] == "processing"
    # Simulate a worker that died mid-publish: push processing_started_at_utc
    # into the past beyond the timeout.
    stale_time = datetime.now(timezone.utc) - timedelta(seconds=cc.limits.PROCESSING_TIMEOUT_SECONDS + 10)
    fake_db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {"processing_started_at_utc": stale_time}})
    recovered = cc.recover_stale_processing()
    assert recovered == 1
    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "scheduled"


def test_retryable_failure_reschedules_with_backoff(fake_db, monkeypatch):
    from telegram.error import TimedOut
    _fake_run_coro(monkeypatch, side_effect=TimedOut())
    post = _schedule_due_post()
    cc.run_due_posts(limit=5)
    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "scheduled"
    assert fresh["last_error_code"] == "network_timeout"
    assert fresh["next_run_at_utc"] > datetime.now(timezone.utc)


def test_permanent_failure_marks_failed(fake_db, monkeypatch):
    from telegram.error import Forbidden
    _fake_run_coro(monkeypatch, side_effect=Forbidden("bot was blocked"))
    post = _schedule_due_post()
    cc.run_due_posts(limit=5)
    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "failed"
    assert fresh["last_error_code"] == "bot_removed"


def test_max_attempts_stops_retrying(fake_db, monkeypatch):
    from telegram.error import TimedOut
    _fake_run_coro(monkeypatch, side_effect=TimedOut())
    post = _schedule_due_post()
    fake_db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {"attempt_count": cc.limits.MAX_ATTEMPTS}})
    cc.run_due_posts(limit=5)
    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "failed"


def test_retry_after_failure_requeues(fake_db, monkeypatch):
    from telegram.error import Forbidden
    _fake_run_coro(monkeypatch, side_effect=Forbidden("bot was blocked"))
    post = _schedule_due_post()
    cc.run_due_posts(limit=5)
    failed = cc.get_post(post["_id"])
    assert failed["status"] == "failed"
    retried, err = cc.retry_post(post["_id"], actor_id=1)
    assert err is None
    assert retried["status"] == "scheduled"
    # Original scheduled_at_utc is preserved for history; only next_run_at_utc
    # (which drives the run_key) moves — this retry gets a fresh run identity.
    assert retried["scheduled_at_utc"] == failed["scheduled_at_utc"]
    assert retried["next_run_at_utc"] > datetime.now(timezone.utc)
    assert retried["next_run_at_utc"] != failed.get("next_run_at_utc")
    assert retried["retry_count"] == 1
    # The worker claims it like any other due post once next_run_at_utc arrives.
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {
        "next_run_at_utc": datetime.now(timezone.utc) - timedelta(seconds=1),
    }})
    _fake_run_coro(monkeypatch, send_result={"message_ids": [999], "poll_id": None, "poll_message_id": None})
    processed = cc.run_due_posts(limit=5)
    assert processed == 1
    assert cc.get_post(post["_id"])["status"] == "published"


# ---------------------------------------------------------------------------
# Retry state machine: valid transitions, conflict codes, duplicate-send guard
# ---------------------------------------------------------------------------

def _failed_post(monkeypatch, error_side_effect=None):
    from telegram.error import Forbidden
    _fake_run_coro(monkeypatch, side_effect=error_side_effect or Forbidden("bot was blocked"))
    post = _schedule_due_post()
    cc.run_due_posts(limit=5)
    failed = cc.get_post(post["_id"])
    assert failed["status"] == "failed"
    return failed


def test_retry_post_not_found_returns_specific_code(fake_db):
    from bson import ObjectId
    post, err = cc.retry_post(ObjectId(), actor_id=1)
    assert post is None
    assert err == "post_not_found"


def test_retry_non_failed_post_rejected(fake_db):
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=1)
    result, err = cc.retry_post(post["_id"], actor_id=1)
    assert result is None
    assert err == "not_failed"


def test_retry_published_post_rejected(fake_db, monkeypatch):
    _fake_run_coro(monkeypatch)
    post = _schedule_due_post()
    cc.run_due_posts(limit=5)
    published = cc.get_post(post["_id"])
    assert published["status"] == "published"
    result, err = cc.retry_post(post["_id"], actor_id=1)
    assert result is None
    assert err == "already_published"


def test_retry_active_processing_post_rejected(fake_db, monkeypatch):
    failed = _failed_post(monkeypatch)
    database.db["community_posts"].update_one({"_id": failed["_id"]}, {"$set": {
        "processing_owner": "other-worker",
        "processing_started_at_utc": datetime.now(timezone.utc),
    }})
    result, err = cc.retry_post(failed["_id"], actor_id=1)
    assert result is None
    assert err == "already_processing"


def test_retry_stale_processing_failed_post_succeeds(fake_db, monkeypatch):
    failed = _failed_post(monkeypatch)
    stale = datetime.now(timezone.utc) - timedelta(seconds=cc.limits.PROCESSING_TIMEOUT_SECONDS + 30)
    database.db["community_posts"].update_one({"_id": failed["_id"]}, {"$set": {
        "processing_owner": "crashed-worker",
        "processing_started_at_utc": stale,
    }})
    retried, err = cc.retry_post(failed["_id"], actor_id=1)
    assert err is None
    assert retried["status"] == "scheduled"
    assert retried["processing_owner"] is None
    assert retried["processing_started_at_utc"] is None


def test_retry_limit_enforced(fake_db, monkeypatch):
    failed = _failed_post(monkeypatch)
    database.db["community_posts"].update_one({"_id": failed["_id"]}, {"$set": {
        "attempt_count": cc.limits.MAX_ATTEMPTS,
    }})
    result, err = cc.retry_post(failed["_id"], actor_id=1)
    assert result is None
    assert err == "retry_limit_reached"
    # An explicit admin force bypasses the limit.
    forced, err2 = cc.retry_post(failed["_id"], actor_id=1, force=True)
    assert err2 is None
    assert forced["status"] == "scheduled"


def test_retry_destination_disabled_after_failure_rejected(fake_db, monkeypatch):
    failed = _failed_post(monkeypatch)
    _make_destination(enabled=False)
    result, err = cc.retry_post(failed["_id"], actor_id=1)
    assert result is None
    assert err == "invalid_destination"


def test_retry_existing_telegram_message_id_prevents_duplicate_resend(fake_db, monkeypatch):
    """Simulates a send that actually succeeded but crashed before the post
    document was finalised as published — the run-ledger row already shows
    delivered content, so retry must reconcile, not resend."""
    failed = _failed_post(monkeypatch)
    run = database.db["community_post_runs"].find_one({"community_post_id": failed["_id"]})
    assert run is not None
    database.db["community_post_runs"].update_one({"_id": run["_id"]}, {"$set": {
        "status": "published", "telegram_message_ids": [555],
    }})
    result, err = cc.retry_post(failed["_id"], actor_id=1)
    assert result is None
    assert err == "already_published"
    reconciled = cc.get_post(failed["_id"])
    assert reconciled["status"] == "published"
    assert reconciled["telegram_message_ids"] == [555]


def test_double_click_retry_only_one_succeeds(fake_db, monkeypatch):
    failed = _failed_post(monkeypatch)
    first, err1 = cc.retry_post(failed["_id"], actor_id=1)
    second, err2 = cc.retry_post(failed["_id"], actor_id=1)
    assert err1 is None
    assert first["status"] == "scheduled"
    assert second is None
    assert err2 == "not_failed"


def test_retry_api_returns_specific_conflict_codes(fake_db):
    """Exercises the HTTP layer directly to guard against the historical bug
    where every retry rejection collapsed into schedule_post's generic
    "not_schedulable" code."""
    app = Flask(__name__)
    app.register_blueprint(cc.community_centre_bp)
    app.config["TESTING"] = True

    def fake_require_admin():
        return {"id": 1, "username": "admin"}, None

    cc._require_admin = fake_require_admin
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=1)
    client = app.test_client()
    resp = client.post(f"/api/admin/community/posts/{post['_id']}/retry")
    body = resp.get_json()
    assert resp.status_code == 400
    assert body["code"] == "not_failed"
    assert body["code"] != "not_schedulable"


# ---------------------------------------------------------------------------
# Poll answer / snapshot analytics
# ---------------------------------------------------------------------------

def _published_poll_post(is_anonymous=False):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["is_anonymous"] = is_anonymous
    post, _ = cc.create_post(payload, actor_id=1)
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {
        "status": "published", "poll_status": "open", "telegram_poll_ids": ["poll_abc"],
        "telegram_message_ids": [500],
    }})
    return cc.get_post(post["_id"])


def test_poll_answer_creates_record_for_non_anonymous(fake_db):
    post = _published_poll_post(is_anonymous=False)
    cc.record_poll_answer("poll_abc", 555, [0])
    answer = database.db["community_poll_answers"].find_one({"poll_id": "poll_abc", "user_id": 555})
    assert answer["selected_option_ids"] == [0]
    assert answer["removed_vote"] is False


def test_poll_answer_changes_vote_not_cumulative(fake_db):
    post = _published_poll_post(is_anonymous=False)
    cc.record_poll_answer("poll_abc", 555, [0])
    cc.record_poll_answer("poll_abc", 555, [1])
    answers = database.db["community_poll_answers"].find({"poll_id": "poll_abc", "user_id": 555})
    assert len(answers) == 1
    assert answers[0]["selected_option_ids"] == [1]


def test_poll_answer_empty_selection_is_removed_vote(fake_db):
    post = _published_poll_post(is_anonymous=False)
    cc.record_poll_answer("poll_abc", 555, [0])
    cc.record_poll_answer("poll_abc", 555, [])
    answer = database.db["community_poll_answers"].find_one({"poll_id": "poll_abc", "user_id": 555})
    assert answer["removed_vote"] is True
    assert answer["selected_option_ids"] == []


def test_anonymous_poll_does_not_record_user_answers(fake_db):
    _published_poll_post(is_anonymous=True)
    cc.record_poll_answer("poll_abc", 555, [0])
    assert database.db["community_poll_answers"].count_documents({}) == 0


def test_poll_answer_repeated_update_is_idempotent(fake_db):
    _published_poll_post(is_anonymous=False)
    cc.record_poll_answer("poll_abc", 555, [0])
    cc.record_poll_answer("poll_abc", 555, [0])
    assert database.db["community_poll_answers"].count_documents({"poll_id": "poll_abc", "user_id": 555}) == 1


def test_poll_snapshot_update(fake_db):
    post = _published_poll_post()
    options = [{"option_id": 0, "text": "Voucher", "voter_count": 3}, {"option_id": 1, "text": "XP", "voter_count": 1}]
    cc.record_poll_snapshot("poll_abc", "Which reward do you prefer?", options, 4, False)
    snap = database.db["community_poll_snapshots"].find_one({"poll_id": "poll_abc"})
    assert snap["total_voter_count"] == 4
    assert snap["is_closed"] is False


def test_poll_snapshot_closed_updates_post_poll_status(fake_db):
    post = _published_poll_post()
    cc.record_poll_snapshot("poll_abc", "Q", [], 0, True)
    fresh = cc.get_post(post["_id"])
    assert fresh["poll_status"] == "closed"


# ---------------------------------------------------------------------------
# Stop poll
# ---------------------------------------------------------------------------

def test_stop_poll(fake_db, monkeypatch):
    post = _published_poll_post()

    fake_option = SimpleNamespace(text="Voucher", voter_count=3)
    fake_final_poll = SimpleNamespace(options=[fake_option], total_voter_count=3)

    async def fake_stop(chat_id, message_id):
        return fake_final_poll

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_do_stop_poll", fake_stop)
    monkeypatch.setattr(cc, "_run_coro", sync_run)

    stopped, err = cc.stop_poll_action(post["_id"], actor_id=1)
    assert err is None
    assert stopped["poll_status"] == "closed"
    assert stopped["poll_closed_by"] == 1


def test_stop_poll_already_closed_is_idempotent_success(fake_db):
    post = _published_poll_post()
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {"poll_status": "closed"}})
    result, err = cc.stop_poll_action(post["_id"], actor_id=1)
    assert err is None
    assert result["poll_status"] == "closed"


def test_stop_poll_not_open_rejected(fake_db):
    _make_destination()
    post, _ = cc.create_post(_poll_payload(), actor_id=1)  # still draft, poll_status not_applicable
    _, err = cc.stop_poll_action(post["_id"], actor_id=1)
    assert err == "poll_not_open"


# ---------------------------------------------------------------------------
# Approval workflow
# ---------------------------------------------------------------------------

def test_creator_self_approval_restricted(fake_db, monkeypatch):
    monkeypatch.setattr("settings_service.get_setting", lambda group, field: True if field == "community_post_approval_enabled" else False)
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=42)
    submitted, _ = cc.submit_for_approval(post["_id"], actor_id=42)
    assert submitted["status"] == "pending_approval"
    _, err = cc.approve_post(post["_id"], actor_id=42)
    assert err == "self_approval_not_allowed"


def test_reject_returns_to_draft(fake_db, monkeypatch):
    monkeypatch.setattr("settings_service.get_setting", lambda group, field: True if field == "community_post_approval_enabled" else False)
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=42)
    cc.submit_for_approval(post["_id"], actor_id=42)
    rejected, err = cc.reject_post(post["_id"], actor_id=99, reason="typo")
    assert err is None
    assert rejected["status"] == "draft"


def test_scheduling_unapproved_post_fails_when_approval_required(fake_db, monkeypatch):
    monkeypatch.setattr("settings_service.get_setting", lambda group, field: True if field == "community_post_approval_enabled" else False)
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=42)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    _, err = cc.schedule_post(post["_id"], actor_id=42, scheduled_at_utc=future)
    assert err == "approval_required"


# ---------------------------------------------------------------------------
# Editing rules
# ---------------------------------------------------------------------------

def test_editing_published_post_rejected(fake_db):
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=1)
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {"status": "published"}})
    _, err = cc.update_post(post["_id"], _text_payload(text="edited"), actor_id=1)
    assert err == "not_editable"


def test_editing_approved_scheduled_post_invalidates_approval(fake_db):
    _make_destination()
    post, _ = cc.create_post(_text_payload(), actor_id=1)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {
        "status": "scheduled", "approved_at": datetime.now(timezone.utc), "approved_by": 2,
        "next_run_at_utc": future, "scheduled_at_utc": future,
    }})
    updated, err = cc.update_post(post["_id"], _text_payload(text="edited"), actor_id=1)
    assert err is None
    assert updated["status"] == "draft"
    assert updated["approved_by"] is None
    assert updated["next_run_at_utc"] is None


def test_duplicate_resets_publish_state(fake_db):
    post = _published_poll_post()
    dup, err = cc.duplicate_post(post["_id"], actor_id=7)
    assert err is None
    assert dup["status"] == "draft"
    assert dup["telegram_message_ids"] == []
    assert dup["telegram_poll_ids"] == []
    assert dup["poll_status"] == "not_applicable"


# ---------------------------------------------------------------------------
# Admin endpoint permissions
# ---------------------------------------------------------------------------

def _app():
    app = Flask(__name__)
    app.register_blueprint(cc.community_centre_bp)
    return app


def test_unauthenticated_request_rejected(fake_db, monkeypatch):
    from flask import jsonify as flask_jsonify
    monkeypatch.setattr(cc, "_require_admin", lambda: (None, (flask_jsonify({"success": False, "code": "auth_failed"}), 401)))
    client = _app().test_client()
    resp = client.get("/api/admin/community/posts")
    assert resp.status_code == 401


def test_authenticated_request_lists_posts(fake_db, monkeypatch):
    monkeypatch.setattr(cc, "_require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    _make_destination()
    cc.create_post(_text_payload(), actor_id=1)
    client = _app().test_client()
    resp = client.get("/api/admin/community/posts")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert len(body["posts"]) == 1


def test_arbitrary_chat_id_not_accepted_via_api(fake_db, monkeypatch):
    """The API only ever accepts a destination_key from the approved
    allowlist — there is no field through which a raw chat_id can be
    supplied by the frontend."""
    monkeypatch.setattr(cc, "_require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    _make_destination()
    client = _app().test_client()
    payload = _text_payload()
    payload["destination_chat_id"] = -999999  # attempted override, must be ignored
    payload["destination_key"] = "not_configured"
    resp = client.post("/api/admin/community/posts", json=payload)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_destination"


# ---------------------------------------------------------------------------
# Poll duration beyond Telegram's native ceiling (codex review fix)
# ---------------------------------------------------------------------------

def test_telegram_native_poll_duration_cap_is_600_seconds():
    # Regression guard for the 600 vs 600000 mixup: PTB 20.8's Poll.MAX_OPEN_PERIOD.
    from telegram import Poll
    assert cc.limits.TELEGRAM_NATIVE_POLL_DURATION_MAX_SECONDS == Poll.MAX_OPEN_PERIOD == 600


def test_poll_duration_within_native_cap_passed_to_telegram(fake_db, monkeypatch):
    captured = {}

    async def fake_send_poll(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(message_id=1, poll=SimpleNamespace(id="poll_xyz"))

    monkeypatch.setattr(cc, "_bot", lambda: SimpleNamespace(send_poll=fake_send_poll))

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_run_coro", sync_run)

    _make_destination()
    payload = _poll_payload()
    payload["poll"]["close_mode"] = "duration"
    payload["poll"]["open_period_seconds"] = 300
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None, err
    result = sync_run(cc._do_send(post))
    assert captured.get("open_period") == 300
    assert result["poll_id"] == "poll_xyz"


def test_poll_duration_beyond_native_cap_not_sent_to_telegram_and_soft_closed(fake_db, monkeypatch):
    """Durations Telegram itself can't accept (>600s) must never be passed as
    open_period (Telegram would reject the whole send) — instead the poll is
    sent open-ended and the worker's soft-close tick stops it later."""
    captured = {}

    async def fake_send_poll(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(message_id=1, poll=SimpleNamespace(id="poll_soft"))

    monkeypatch.setattr(cc, "_bot", lambda: SimpleNamespace(send_poll=fake_send_poll))

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_run_coro", sync_run)

    payload = _poll_payload()
    payload["poll"]["close_mode"] = "duration"
    payload["poll"]["open_period_seconds"] = 1800  # 30 minutes — exceeds Telegram's 600s ceiling
    post = _schedule_due_post(payload)

    processed = cc.run_due_posts(limit=5)
    assert processed == 1
    assert "open_period" not in captured

    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "published"
    assert fresh["poll"]["close_at_utc"] is not None
    assert fresh["poll_status"] == "open"


def test_run_due_poll_closures_stops_soft_close_poll(fake_db, monkeypatch):
    post = _published_poll_post()
    database.db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {
        "poll.close_mode": "duration",
        "poll.close_at_utc": datetime.now(timezone.utc) - timedelta(seconds=5),
    }})

    fake_final_poll = SimpleNamespace(options=[SimpleNamespace(text="Voucher", voter_count=1)], total_voter_count=1)

    async def fake_stop(chat_id, message_id):
        return fake_final_poll

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_do_stop_poll", fake_stop)
    monkeypatch.setattr(cc, "_run_coro", sync_run)

    closed = cc.run_due_poll_closures()
    assert closed == 1
    fresh = cc.get_post(post["_id"])
    assert fresh["poll_status"] == "closed"


# ---------------------------------------------------------------------------
# Stale-processing recovery must not deadlock or duplicate-send (codex review fix)
# ---------------------------------------------------------------------------

def test_stale_recovery_preserves_run_key_for_safe_retry(fake_db, monkeypatch):
    """A worker that crashes mid-send must, on retry, reclaim the SAME
    run_key (not manufacture a new occurrence) — otherwise a genuinely
    successful-but-uncommitted send would be repeated to Telegram."""
    post = _schedule_due_post()
    original_next_run = post["next_run_at_utc"]

    claimed = cc._claim_next_due_post()
    assert claimed["status"] == "processing"
    run_key = cc._run_key(claimed["_id"], claimed["next_run_at_utc"])
    # Simulate _execute_publish having inserted the run row, then crashing
    # before it could be marked complete.
    outcome, _ = cc._claim_or_reuse_run(claimed["_id"], run_key, claimed["next_run_at_utc"])
    assert outcome == "claimed"

    stale_time = datetime.now(timezone.utc) - timedelta(seconds=cc.limits.PROCESSING_TIMEOUT_SECONDS + 10)
    fake_db["community_posts"].update_one({"_id": post["_id"]}, {"$set": {"processing_started_at_utc": stale_time}})
    fake_db["community_post_runs"].update_one({"community_post_id": post["_id"], "run_key": run_key}, {"$set": {"processing_started_at_utc": stale_time}})

    recovered = cc.recover_stale_processing()
    assert recovered == 1

    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "scheduled"
    # next_run_at_utc must be unchanged so the retry's run_key matches.
    assert fresh["next_run_at_utc"] == original_next_run

    stale_run = fake_db["community_post_runs"].find_one({"community_post_id": post["_id"], "run_key": run_key})
    assert stale_run["status"] == "failed"

    # Retry now succeeds instead of deadlocking as "already_processing".
    _fake_run_coro(monkeypatch)
    processed = cc.run_due_posts(limit=5)
    assert processed == 1
    final = cc.get_post(post["_id"])
    assert final["status"] == "published"


def test_media_group_button_followup_failure_is_partial_not_lost(fake_db, monkeypatch):
    """The album itself already sent successfully — a failing follow-up
    button message must not discard those message_ids or make the whole
    occurrence retry (which would resend the live album)."""
    _make_destination()
    payload = {
        "title": "Album",
        "content_type": "media_group",
        "destination_key": "official_channel",
        "media": [
            {"type": "photo", "source_url": "https://example.com/1.jpg"},
            {"type": "photo", "source_url": "https://example.com/2.jpg"},
        ],
        "buttons": [{"row": 0, "position": 0, "text": "Info", "type": "url", "value": "https://example.com"}],
    }

    async def fake_send(post):
        return {"message_ids": [10, 11], "poll_id": None, "poll_message_id": None, "buttons_failed": True}

    def sync_run(coro, timeout=20):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(cc, "_do_send", fake_send)
    monkeypatch.setattr(cc, "_run_coro", sync_run)

    post = _schedule_due_post(payload)
    cc.run_due_posts(limit=5)
    fresh = cc.get_post(post["_id"])
    assert fresh["status"] == "partially_published"
    assert fresh["telegram_message_ids"] == [10, 11]


def test_reschedule_revalidates_poll_close_date(fake_db):
    _make_destination()
    payload = _poll_payload()
    payload["poll"]["close_mode"] = "date"
    close_at = datetime.now(timezone.utc) + timedelta(hours=2)
    payload["poll"]["close_at_utc"] = close_at.isoformat()
    post, _ = cc.create_post(payload, actor_id=1)
    ok_time = datetime.now(timezone.utc) + timedelta(hours=1)
    scheduled, err = cc.schedule_post(post["_id"], actor_id=1, scheduled_at_utc=ok_time)
    assert err is None

    # Reschedule to publish AFTER the poll's fixed close time — must be rejected.
    bad_time = close_at + timedelta(hours=1)
    _, err = cc.reschedule_post(post["_id"], actor_id=1, scheduled_at_utc=bad_time)
    assert err == "poll_closes_before_publish"


# ---------------------------------------------------------------------------
# Destination resolver: Telegram Configuration -> Community Centre
# destinations (Admin Dashboard bug fix — Composer must read the existing
# Settings -> Integrations -> Telegram Configuration fields instead of a
# separate, never-populated destination list).
# ---------------------------------------------------------------------------

FULL_TELEGRAM_CONFIG = {
    "official_channel_id": "-1002396761021",
    "official_channel_username": "advantplayofficial",
    "main_group_id": "-1002304653063",
    "community_chat_id": "-1002743212540",
}


def test_resolve_creates_all_three_builtin_destinations():
    dests = cc.resolve_community_destinations(FULL_TELEGRAM_CONFIG)
    keys = [d["key"] for d in dests]
    assert keys == ["official_channel", "main_group", "mywin_group"]


def test_resolve_official_channel_id():
    dests = cc.resolve_community_destinations(FULL_TELEGRAM_CONFIG)
    official = next(d for d in dests if d["key"] == "official_channel")
    assert official["chat_id"] == -1002396761021
    assert official["username"] == "advantplayofficial"
    assert official["chat_type"] == "channel"
    assert official["enabled"] is True
    assert official["allow_posts"] and official["allow_polls"] and official["allow_pin"]


def test_resolve_main_group_id():
    dests = cc.resolve_community_destinations(FULL_TELEGRAM_CONFIG)
    main_group = next(d for d in dests if d["key"] == "main_group")
    assert main_group["chat_id"] == -1002304653063
    assert main_group["name"] == "Community Group"
    assert main_group["chat_type"] == "supergroup"


def test_resolve_mywin_chat_id():
    dests = cc.resolve_community_destinations(FULL_TELEGRAM_CONFIG)
    mywin = next(d for d in dests if d["key"] == "mywin_group")
    assert mywin["chat_id"] == -1002743212540
    assert mywin["name"] == "#mywin Group"


def test_resolve_invalid_and_zero_ids_excluded():
    dests = cc.resolve_community_destinations({
        "official_channel_id": "",       # unset
        "main_group_id": "0",            # zero
        "community_chat_id": "not_a_number",  # garbage
    })
    assert dests == []


def test_resolve_positive_chat_id_excluded():
    # Telegram groups/channels are always negative ids; a positive id is
    # never a valid destination even if it happens to be non-zero.
    dests = cc.resolve_community_destinations({"official_channel_id": "12345"})
    assert dests == []


def test_resolve_mongo_override_takes_precedence(monkeypatch):
    import settings_service as ss

    monkeypatch.setattr(ss, "_cache", {})  # isolate from the process-global settings cache
    stored = {"official_channel_id": "-1009999999999"}
    fake_settings_col_data = {"_id": "telegram_config", **stored}

    class _FakeCol:
        def find_one(self, query):
            return dict(fake_settings_col_data) if query.get("_id") == "telegram_config" else None

    class _FakeDbRef:
        def __getitem__(self, name):
            return _FakeCol()

    monkeypatch.setenv("OFFICIAL_CHANNEL_ID", "-1008888888888")
    merged = ss.get_settings("telegram_config", db_ref=_FakeDbRef(), force_refresh=True)
    assert merged["official_channel_id"] == "-1009999999999"  # Mongo wins over env
    dests = cc.resolve_community_destinations(merged)
    official = next(d for d in dests if d["key"] == "official_channel")
    assert official["chat_id"] == -1009999999999


def test_resolve_env_fallback_when_no_mongo_override(monkeypatch):
    import settings_service as ss

    monkeypatch.setattr(ss, "_cache", {})

    class _FakeCol:
        def find_one(self, query):
            return None  # nothing saved in Mongo yet

    class _FakeDbRef:
        def __getitem__(self, name):
            return _FakeCol()

    monkeypatch.setenv("OFFICIAL_CHANNEL_ID", "-1007777777777")
    merged = ss.get_settings("telegram_config", db_ref=_FakeDbRef(), force_refresh=True)
    assert merged["official_channel_id"] == "-1007777777777"
    dests = cc.resolve_community_destinations(merged)
    official = next(d for d in dests if d["key"] == "official_channel")
    assert official["chat_id"] == -1007777777777


def test_resolve_application_default_fallback_when_untouched(monkeypatch):
    import settings_service as ss

    monkeypatch.setattr(ss, "_cache", {})

    class _FakeCol:
        def find_one(self, query):
            return None

    class _FakeDbRef:
        def __getitem__(self, name):
            return _FakeCol()

    monkeypatch.delenv("MAIN_GROUP_ID", raising=False)
    merged = ss.get_settings("telegram_config", db_ref=_FakeDbRef(), force_refresh=True)
    # Untouched deployment: falls all the way to the hardcoded application
    # default that shipped before this feature existed.
    assert merged["main_group_id"] == "-1002304653063"
    dests = cc.resolve_community_destinations(merged)
    main_group = next(d for d in dests if d["key"] == "main_group")
    assert main_group["chat_id"] == -1002304653063


def test_builtin_and_custom_destinations_merge(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: FULL_TELEGRAM_CONFIG)
    _make_destination(key="vip_lounge", chat_id=-1005550001111, name="VIP Lounge")
    dests = cc.list_destinations()
    keys = {d["key"] for d in dests}
    assert keys == {"official_channel", "main_group", "mywin_group", "vip_lounge"}


def test_merge_dedupes_by_chat_id(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: FULL_TELEGRAM_CONFIG)
    # A custom destination pointing at the same chat as the builtin official
    # channel must not produce a duplicate entry; the builtin wins.
    _make_destination(key="dup_by_chat_id", chat_id=-1002396761021, name="Duplicate")
    dests = cc.list_destinations()
    matching_chat = [d for d in dests if d["chat_id"] == -1002396761021]
    assert len(matching_chat) == 1
    assert matching_chat[0]["key"] == "official_channel"


def test_merge_dedupes_by_key(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: FULL_TELEGRAM_CONFIG)
    # A custom destination reusing a builtin's key (e.g. stale legacy data)
    # must not shadow or duplicate the canonical builtin.
    _make_destination(key="official_channel", chat_id=-1009990009990, name="Legacy Official")
    dests = cc.list_destinations()
    matching_key = [d for d in dests if d["key"] == "official_channel"]
    assert len(matching_key) == 1
    assert matching_key[0]["chat_id"] == -1002396761021


def test_official_channel_is_first_when_configured(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: FULL_TELEGRAM_CONFIG)
    dests = cc.list_destinations(enabled_only=True)
    assert dests[0]["key"] == "official_channel"


def test_empty_destination_state_has_no_valid_destination(fake_db):
    # NO_BUILTIN_TELEGRAM_CONFIG (from the fixture) + no custom destinations
    # created -> nothing for the Composer to select, matching the disabled
    # "No valid Telegram destination configured" placeholder state.
    assert cc.list_destinations(enabled_only=True) == []
    post, err = cc.create_post(_text_payload(destination_key="official_channel"), actor_id=1)
    assert post is None
    assert err == "invalid_destination"


def test_settings_update_reflected_without_duplicating_config(fake_db, monkeypatch):
    # Simulates saving Settings -> Integrations -> Telegram Configuration:
    # Community Centre must pick up the new values immediately, with no
    # separate community_destinations document ever written for it.
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: NO_BUILTIN_TELEGRAM_CONFIG)
    assert cc.get_destination("official_channel") is None

    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: FULL_TELEGRAM_CONFIG)
    official = cc.get_destination("official_channel")
    assert official is not None
    assert official["chat_id"] == -1002396761021
    assert cc._destinations().find_one({"key": "official_channel"}) is None


def test_arbitrary_frontend_chat_id_is_ignored(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: FULL_TELEGRAM_CONFIG)
    payload = _text_payload(destination_key="official_channel")
    payload["destination_chat_id"] = -1  # client-supplied; must never be trusted
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert post["destination_chat_id"] == -1002396761021


def test_disabled_custom_destination_cannot_publish(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: NO_BUILTIN_TELEGRAM_CONFIG)
    _make_destination(key="paused_channel", enabled=False)
    post, err = cc.create_post(_text_payload(destination_key="paused_channel"), actor_id=1)
    assert post is None
    assert err == "invalid_destination"


def test_poll_permission_validation_via_resolved_destination(fake_db, monkeypatch):
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: NO_BUILTIN_TELEGRAM_CONFIG)
    _make_destination(allow_polls=False)
    post, err = cc.create_post(_poll_payload(), actor_id=1)
    assert post is None
    assert err == "destination_disallows_polls"
