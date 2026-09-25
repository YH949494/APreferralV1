"""Regression tests for scheduler.run_invitee_subscription_audit's cache
policy: a Telegram/API error must never downgrade a known-good cached
positive to subscribed=false, a confirmed non-member gets a short negative
TTL (not the 14-day positive one), and the candidate population now also
covers recently-active Miniapp users (users.last_visible_at), not just
recent referral invitees — see vouchers.py/scheduler.py channel-subscription
cache audit patch.
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

with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
    import scheduler  # noqa: E402


def _fresh_db():
    client = mongomock.MongoClient()
    return client["referral_bot"]


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def _member_payload(status="member"):
    return {"ok": True, "result": {"status": status}}


def _aware(dt_value):
    # mongomock (like real Mongo) round-trips datetimes as naive UTC.
    if dt_value is not None and dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)
    return dt_value


@pytest.fixture(autouse=True)
def _patch_channel_and_settings():
    with mock.patch.object(scheduler, "OFFICIAL_CHANNEL_ID", -1001234567890), \
         mock.patch.object(scheduler, "BOT_TOKEN", "123:ABC"), \
         mock.patch.object(scheduler, "INVITEE_SUB_AUDIT_ENABLED", True), \
         mock.patch.object(scheduler, "_get_setting", None), \
         mock.patch.object(scheduler, "TG_REQUEST_SLEEP_MS", 0):
        yield


def test_tg_error_never_overwrites_known_good_positive():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 4242

    old_expire = now + timedelta(days=10)
    db_ref.subscription_cache.insert_one({
        "_id": f"sub:{uid}",
        "user_id": uid,
        "subscribed": True,
        "checked_at": now - timedelta(days=8),
        "expireAt": old_expire,
        "first_subscribed_at_utc": now - timedelta(days=30),
    })
    db_ref.pending_referrals.insert_one({
        "invitee_user_id": uid,
        "created_at_utc": now - timedelta(hours=1),
    })

    with mock.patch.object(scheduler.requests, "get", side_effect=scheduler.RequestException("boom")):
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["errors"] == 1
    doc = db_ref.subscription_cache.find_one({"_id": f"sub:{uid}"})
    # subscribed / expireAt / first_subscribed_at_utc are exactly as before —
    # the transient Telegram failure only recorded an attempt, never
    # downgraded the known-good positive cache.
    assert doc["subscribed"] is True
    assert _aware(doc["expireAt"]) == old_expire
    assert doc["last_error"] == "boom"
    assert "last_check_attempt" in doc


def test_confirmed_not_subscribed_gets_short_negative_ttl():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 5151
    db_ref.pending_referrals.insert_one({
        "invitee_user_id": uid,
        "created_at_utc": now - timedelta(hours=1),
    })

    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("left"))):
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["subscribed_false"] == 1
    doc = db_ref.subscription_cache.find_one({"_id": f"sub:{uid}"})
    assert doc["subscribed"] is False
    ttl_seconds = (_aware(doc["expireAt"]) - now).total_seconds()
    # Must be the short negative TTL, nowhere near the 14-day positive one.
    assert 0 < ttl_seconds <= scheduler.SUB_CACHE_NEGATIVE_TTL_SECONDS + 5
    assert ttl_seconds < timedelta(days=1).total_seconds()


def test_confirmed_subscribed_gets_long_positive_ttl():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 6161
    db_ref.pending_referrals.insert_one({
        "invitee_user_id": uid,
        "created_at_utc": now - timedelta(hours=1),
    })

    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))):
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["subscribed_true"] == 1
    doc = db_ref.subscription_cache.find_one({"_id": f"sub:{uid}"})
    assert doc["subscribed"] is True
    ttl_days = (_aware(doc["expireAt"]) - now).days
    assert ttl_days == scheduler.SUB_CACHE_TTL_DAYS


def test_candidate_population_includes_recently_active_miniapp_users():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    referral_uid = 7171
    active_uid = 7272

    db_ref.pending_referrals.insert_one({
        "invitee_user_id": referral_uid,
        "created_at_utc": now - timedelta(hours=1),
    })
    # Never referred (no pending_referrals row) but opened the Mini App
    # recently — must still be picked up so its cache stays fresh for
    # voucher-card gating.
    db_ref.users.insert_one({
        "user_id": active_uid,
        "last_visible_at": now - timedelta(hours=2),
    })

    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))):
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == 2
    assert db_ref.subscription_cache.find_one({"_id": f"sub:{referral_uid}"}) is not None
    assert db_ref.subscription_cache.find_one({"_id": f"sub:{active_uid}"}) is not None


def test_stale_positive_cache_is_refreshed_without_recent_activity():
    # Closes the gap a Codex review flagged on this PR: a user cached
    # subscribed=True (trusted at claim time for up to SUB_CACHE_TTL_DAYS=14
    # without recontacting Telegram) who then unsubscribes and never shows
    # up again in pending_referrals or users.last_visible_at must still get
    # re-verified well before that 14-day trust window elapses.
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 9191
    # No pending_referrals row, no users doc at all — only a stale positive
    # subscription_cache entry from a week ago.
    db_ref.subscription_cache.insert_one({
        "_id": f"sub:{uid}",
        "user_id": uid,
        "subscribed": True,
        "checked_at": now - timedelta(days=7),
        "expireAt": now + timedelta(days=7),
    })

    # Telegram now reports they left.
    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("left"))):
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == 1
    assert result["subscribed_false"] == 1
    doc = db_ref.subscription_cache.find_one({"_id": f"sub:{uid}"})
    assert doc["subscribed"] is False


def test_stale_positive_cache_is_not_starved_by_large_referral_volume():
    # Regression for the exact scenario a pre-deploy audit flagged: on a
    # large community, recent-referral + recent-active-user volume alone can
    # exceed SUB_AUDIT_BATCH_SIZE every run. If those sources were
    # scanned before the stale-positive-cache source, they could consume the
    # entire per-run budget and starve it to zero forever, silently breaking
    # the "positive cache gets refreshed before it goes stale" property this
    # whole mechanism exists for. The stale-cache source must always get
    # first claim on the budget.
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    stale_uid = 9292

    db_ref.subscription_cache.insert_one({
        "_id": f"sub:{stale_uid}",
        "user_id": stale_uid,
        "subscribed": True,
        "checked_at": now - timedelta(days=7),
        "expireAt": now + timedelta(days=7),
    })
    # Far more recent-referral candidates than the per-run budget.
    for i in range(20):
        db_ref.pending_referrals.insert_one({
            "invitee_user_id": 10_000 + i,
            "created_at_utc": now - timedelta(hours=1, minutes=i),
        })

    with mock.patch.object(scheduler, "SUB_AUDIT_BATCH_SIZE", 3), \
         mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))) as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == 3
    assert mocked_get.call_count == 3
    # The stale positive entry must be among the (budget-limited) uids
    # actually checked this run, not crowded out by the 20 referral rows.
    doc = db_ref.subscription_cache.find_one({"_id": f"sub:{stale_uid}"})
    assert _aware(doc["checked_at"]) == now


def test_recently_checked_uid_is_skipped_not_rechecked():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 8181
    db_ref.pending_referrals.insert_one({
        "invitee_user_id": uid,
        "created_at_utc": now - timedelta(hours=1),
    })
    db_ref.subscription_cache.insert_one({
        "_id": f"sub:{uid}",
        "user_id": uid,
        "subscribed": True,
        "checked_at": now - timedelta(hours=1),
        "expireAt": now + timedelta(days=14),
    })

    with mock.patch.object(scheduler.requests, "get") as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    mocked_get.assert_not_called()
    assert result["skipped_recent"] == 1
    assert result["checked"] == 0


# ---------------------------------------------------------------------------
# Batch-worker behavior (SUB_AUDIT_BATCH_SIZE / SUB_AUDIT_POSITIVE_REFRESH_HOURS /
# SUB_AUDIT_MAX_RUNTIME_SECONDS / 429 backoff) — converts the audit from a
# once-weekly bounded sweep into a small recurring batch worker.
# ---------------------------------------------------------------------------

def _insert_stale_positive(db_ref, uid, checked_at):
    db_ref.subscription_cache.insert_one({
        "_id": f"sub:{uid}",
        "user_id": uid,
        "subscribed": True,
        "checked_at": checked_at,
        "expireAt": checked_at + timedelta(days=14),
    })


def test_batch_size_caps_telegram_calls_regardless_of_backlog_size():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    backlog = 5000
    for i in range(backlog):
        _insert_stale_positive(db_ref, 20_000 + i, now - timedelta(days=7, seconds=i))

    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))) as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == scheduler.SUB_AUDIT_BATCH_SIZE
    assert mocked_get.call_count == scheduler.SUB_AUDIT_BATCH_SIZE
    assert result["checked"] < backlog


def test_stale_positives_processed_oldest_checked_at_first():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    oldest_uid, middle_uid, newest_uid = 31_001, 31_002, 31_003
    _insert_stale_positive(db_ref, oldest_uid, now - timedelta(days=10))
    _insert_stale_positive(db_ref, middle_uid, now - timedelta(days=8))
    _insert_stale_positive(db_ref, newest_uid, now - timedelta(days=6))

    with mock.patch.object(scheduler, "SUB_AUDIT_BATCH_SIZE", 1), \
         mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))) as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == 1
    assert mocked_get.call_count == 1
    assert _aware(db_ref.subscription_cache.find_one({"_id": f"sub:{oldest_uid}"})["checked_at"]) == now
    # middle/newest untouched — only the oldest-checked entry was refreshed.
    assert _aware(db_ref.subscription_cache.find_one({"_id": f"sub:{middle_uid}"})["checked_at"]) == now - timedelta(days=8)
    assert _aware(db_ref.subscription_cache.find_one({"_id": f"sub:{newest_uid}"})["checked_at"]) == now - timedelta(days=6)


def test_next_run_continues_with_next_oldest_entries():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid_a, uid_b, uid_c = 32_001, 32_002, 32_003
    _insert_stale_positive(db_ref, uid_a, now - timedelta(days=10))
    _insert_stale_positive(db_ref, uid_b, now - timedelta(days=9))
    _insert_stale_positive(db_ref, uid_c, now - timedelta(days=8))

    with mock.patch.object(scheduler, "SUB_AUDIT_BATCH_SIZE", 1), \
         mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))):
        first = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)
        second = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)
        third = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert first["checked"] == second["checked"] == third["checked"] == 1
    # Each run refreshed a different (the then-next-oldest) uid — after three
    # runs with batch_size=1 and three candidates, all three are now "now".
    for uid in (uid_a, uid_b, uid_c):
        assert _aware(db_ref.subscription_cache.find_one({"_id": f"sub:{uid}"})["checked_at"]) == now
    # A fourth run has nothing left to refresh.
    with mock.patch.object(scheduler, "SUB_AUDIT_BATCH_SIZE", 1), \
         mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))) as mocked_get:
        fourth = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)
    assert fourth["checked"] == 0
    mocked_get.assert_not_called()


def test_fresh_positive_within_refresh_window_is_not_selected():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 33_001
    # Checked 1 hour ago — nowhere near SUB_AUDIT_POSITIVE_REFRESH_HOURS
    # (120h default) and not present in any other candidate source, so it
    # must not be selected at all this run (not even a skip — never queried).
    _insert_stale_positive(db_ref, uid, now - timedelta(hours=1))

    with mock.patch.object(scheduler.requests, "get") as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    mocked_get.assert_not_called()
    assert result["checked"] == 0
    assert result["scanned"] == 0


def test_near_expiry_positive_entry_is_selected():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 34_001
    # 13 days old — inside the 14-day hard TTL but well past the 5-day
    # (120h) proactive-refresh threshold, so it must be picked up now rather
    # than left to expire naturally.
    _insert_stale_positive(db_ref, uid, now - timedelta(days=13))

    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))) as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == 1
    mocked_get.assert_called_once()


def test_duplicate_uid_across_candidate_sources_checked_once():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    uid = 35_001
    _insert_stale_positive(db_ref, uid, now - timedelta(days=7))
    db_ref.pending_referrals.insert_one({
        "invitee_user_id": uid,
        "created_at_utc": now - timedelta(hours=1),
    })
    db_ref.users.insert_one({
        "user_id": uid,
        "last_visible_at": now - timedelta(hours=1),
    })

    with mock.patch.object(scheduler.requests, "get", return_value=_Resp(200, _member_payload("member"))) as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    assert result["checked"] == 1
    mocked_get.assert_called_once()


def test_telegram_429_stops_batch_and_preserves_positive_cache():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    first_uid, second_uid = 36_001, 36_002
    _insert_stale_positive(db_ref, first_uid, now - timedelta(days=10))
    _insert_stale_positive(db_ref, second_uid, now - timedelta(days=9))

    rate_limited_resp = _Resp(429, {"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 7}})
    with mock.patch.object(scheduler.requests, "get", return_value=rate_limited_resp) as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    # Stopped after the very first 429 — never even attempted the second uid.
    assert mocked_get.call_count == 1
    assert result["rate_limited"] == 1
    assert result["stop_reason"] == "telegram_429"
    assert result["checked"] == 1  # attempted, not silently dropped from the count
    # The 429'd uid's known-good positive cache must survive untouched.
    doc = db_ref.subscription_cache.find_one({"_id": f"sub:{first_uid}"})
    assert doc["subscribed"] is True
    assert doc["last_error"] == "http_429"
    # The second uid was never reached this run — its checked_at is untouched.
    assert _aware(db_ref.subscription_cache.find_one({"_id": f"sub:{second_uid}"})["checked_at"]) == now - timedelta(days=9)


def test_runtime_limit_exits_cleanly_before_any_telegram_call():
    db_ref = _fresh_db()
    now = datetime(2026, 1, 8, 4, 30, tzinfo=timezone.utc)
    _insert_stale_positive(db_ref, 37_001, now - timedelta(days=10))
    _insert_stale_positive(db_ref, 37_002, now - timedelta(days=9))

    with mock.patch.object(scheduler, "SUB_AUDIT_MAX_RUNTIME_SECONDS", 0), \
         mock.patch.object(scheduler.requests, "get") as mocked_get:
        result = scheduler.run_invitee_subscription_audit(now_utc_ts=now, db_ref=db_ref)

    mocked_get.assert_not_called()
    assert result["checked"] == 0
    assert result["stop_reason"] == "runtime_limit"
