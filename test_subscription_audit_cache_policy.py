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
