from datetime import datetime, timedelta, timezone
from flask import Flask

import vouchers as m
from config import KL_TZ


class FakeUsers:
    def __init__(self, doc):
        self.doc = doc

    def find_one(self, filt, projection=None):  # noqa: ARG002
        if filt.get("user_id") == self.doc.get("user_id"):
            return dict(self.doc)
        return None


class FakeEvents:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find(self, filt, projection=None):  # noqa: ARG002
        start = filt.get("created_at", {}).get("$gte")
        end = filt.get("created_at", {}).get("$lte")
        out = []
        for doc in self.docs:
            if doc.get("user_id") != filt.get("user_id"):
                continue
            if "type" in filt and doc.get("type") != filt.get("type"):
                continue
            if "source" in filt and doc.get("source") != filt.get("source"):
                continue
            created = m._as_aware_utc(doc.get("created_at"))
            if start and created < m._as_aware_utc(start):
                continue
            if end and created > m._as_aware_utc(end):
                continue
            out.append(dict(doc))
        return out


class FakeDb:
    def __init__(self, events):
        self.xp_events = FakeEvents(events)
        self.xp_ledger = FakeEvents([])


def _checkin(uid, joined, day_offset):
    return {
        "user_id": uid,
        "type": "checkin",
        "unique_key": f"checkin:{day_offset}",
        "created_at": joined + timedelta(days=day_offset, hours=1),
    }


def _run_progress(monkeypatch, *, uid=42, joined=None, events=None, subscribed=True, allowed=True, now=None):
    joined = joined or datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    now = now or joined + timedelta(days=3)
    app = Flask(__name__)
    monkeypatch.setattr(m, "users_collection", FakeUsers({"user_id": uid, "joined_main_at": joined}))
    monkeypatch.setattr(m, "db", FakeDb(events or []))
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: subscribed)
    monkeypatch.setattr(m, "welcome_eligibility", lambda _uid, ref=None: (allowed, "ok" if allowed else "blocked", {}))
    with app.app_context():
        return m.get_welcome_reward_progress(uid, now=now)


def test_joined_channel_three_checkins_within_7_days_unlocks(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 0), _checkin(42, joined, 1), _checkin(42, joined, 2)],
        subscribed=True,
    )
    assert progress["unlocked"] is True
    assert progress["checkins_completed"] == 3
    assert progress["hide"] is False


def test_non_consecutive_three_checkins_within_7_days_unlocks(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 0), _checkin(42, joined, 3), _checkin(42, joined, 6)],
        subscribed=True,
    )
    assert progress["unlocked"] is True
    assert progress["checkins_completed"] == 3


def test_two_checkins_only_locked(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 0), _checkin(42, joined, 2)],
        subscribed=True,
    )
    assert progress["unlocked"] is False
    assert progress["hide"] is False
    assert progress["checkins_completed"] == 2


def test_three_checkins_after_7_day_window_hidden_not_unlocked(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 8), _checkin(42, joined, 9), _checkin(42, joined, 10)],
        subscribed=True,
        allowed=False,
        now=joined + timedelta(days=10),
    )
    assert progress["unlocked"] is False
    assert progress["hide"] is True
    assert progress["expired"] is True
    assert progress["checkins_completed"] == 0


def test_day_6_start_hides_after_expiry_when_incomplete(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 6)],
        subscribed=True,
        allowed=False,
        now=joined + timedelta(days=8),
    )
    assert progress["hide"] is True
    assert progress["expired"] is True
    assert progress["checkins_completed"] == 1


def test_not_subscribed_locked_even_with_three_checkins(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 0), _checkin(42, joined, 1), _checkin(42, joined, 2)],
        subscribed=False,
    )
    assert progress["unlocked"] is False
    assert progress["channel_joined"] is False
    assert progress["hide"] is False


def test_existing_abuse_gate_blocked_does_not_unlock(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    progress = _run_progress(
        monkeypatch,
        joined=joined,
        events=[_checkin(42, joined, 0), _checkin(42, joined, 1), _checkin(42, joined, 2)],
        subscribed=True,
        allowed=False,
    )
    assert progress["unlocked"] is False
    assert progress["hide"] is True


class FakeDrops:
    def __init__(self, doc):
        self.doc = doc

    def find_one(self, filt):
        return dict(self.doc) if filt.get("_id") == self.doc.get("_id") else {}


class FakeApiDb:
    def __init__(self, drop):
        self.drops = FakeDrops(drop)


def test_claim_attempt_while_locked_does_not_acquire_claim_lock(monkeypatch):
    now = datetime.now(timezone.utc)
    drop = {
        "_id": "welcome-drop",
        "name": "Welcome",
        "type": "pooled",
        "status": "active",
        "startsAt": now - timedelta(minutes=5),
        "endsAt": now + timedelta(minutes=5),
        "public_remaining": 10,
        "my_remaining": 0,
        "audience": "new_joiner",
    }
    locked_progress = {
        "eligible": True,
        "unlocked": False,
        "expired": False,
        "hide": False,
        "channel_joined": True,
        "checkins_completed": 2,
        "checkins_required": 3,
        "eligible_until": (now + timedelta(days=4)).isoformat(),
        "days_remaining": 4,
    }
    app = Flask(__name__)
    monkeypatch.setattr(m, "db", FakeApiDb(drop))
    monkeypatch.setattr(m, "extract_raw_init_data_from_query", lambda _request: "init")
    monkeypatch.setattr(m, "verify_telegram_init_data", lambda _init: (True, {"user": '{"id": 42, "username": "u"}'}, "ok"))
    monkeypatch.setattr(m, "is_drop_active", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(m, "users_collection", FakeUsers({"user_id": 42, "joined_main_at": now, "region": "th"}))
    monkeypatch.setattr(m, "load_user_context", lambda **_kwargs: {})
    monkeypatch.setattr(m, "is_drop_allowed", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(m, "is_user_eligible_for_drop", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(m, "_acquire_request_dedup_lock", lambda **_kwargs: True)
    monkeypatch.setattr(m, "_pooled_claimability_state", lambda **_kwargs: {"claimable": True, "sold_out": False, "remaining": 1})
    monkeypatch.setattr(m, "_should_enforce_session_cooldown", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(m, "_find_existing_claim_for_drop", lambda **_kwargs: None)
    monkeypatch.setattr(m, "_check_kill_switch", lambda **_kwargs: (True, "ok", 0))
    monkeypatch.setattr(m, "_check_cooldown", lambda **_kwargs: (True, "ok", 0))
    monkeypatch.setattr(m, "is_public_pool", lambda _drop: False)
    monkeypatch.setattr(m, "check_channel_subscribed", lambda _uid: True)
    monkeypatch.setattr(m, "welcome_eligibility", lambda _uid, ref=None: (True, "ok", {}))
    monkeypatch.setattr(m, "get_welcome_reward_progress", lambda *_args, **_kwargs: locked_progress)
    monkeypatch.setattr(m, "_acquire_claim_lock", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("claim lock should not be acquired while welcome is locked")))

    with app.test_request_context(
        "/vouchers/claim",
        method="POST",
        json={"dropId": "welcome-drop", "init_data": "init"},
        headers={"User-Agent": "pytest"},
    ):
        response, status = m.api_claim()

    payload = response.get_json()
    assert status == 403
    assert payload["code"] == "welcome_locked"
    assert payload["progress"]["checkins_completed"] == 2
