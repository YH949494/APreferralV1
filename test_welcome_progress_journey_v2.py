from datetime import datetime, timedelta, timezone

from flask import Flask

import dashboard_panels
import scheduler
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
        out = []
        for doc in self.docs:
            if doc.get("user_id") != filt.get("user_id"):
                continue
            if "type" in filt and doc.get("type") != filt.get("type"):
                continue
            if "source" in filt and doc.get("source") != filt.get("source"):
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


def _patch_progress(monkeypatch, *, uid=42, joined, events, subscribed=True, allowed=True):
    app = Flask(__name__)
    monkeypatch.setattr(m, "users_collection", FakeUsers({"user_id": uid, "joined_main_at": joined}))
    monkeypatch.setattr(m, "db", FakeDb(events or []))
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: subscribed)
    monkeypatch.setattr(m, "welcome_eligibility", lambda _uid, ref=None: (allowed, "ok" if allowed else "blocked", {}))
    return app


def test_get_welcome_progress_state_before_first_checkin(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    app = _patch_progress(monkeypatch, joined=joined, events=[])
    with app.app_context():
        out = m.get_welcome_progress(42, now=joined + timedelta(hours=1))
    assert out["eligible"] is True
    assert out["completed"] == 0
    assert out["required"] == 3
    assert out["progress_pct"] == 0
    assert out["next_required_day"] == 1
    assert out["status"] == "in_progress"


def test_get_welcome_progress_after_day1_cannot_checkin_same_day(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    app = _patch_progress(monkeypatch, joined=joined, events=[_checkin(42, joined, 0)])
    with app.app_context():
        out = m.get_welcome_progress(42, now=joined + timedelta(hours=2))
    assert out["completed"] == 1
    assert out["progress_pct"] == 33
    assert out["next_required_day"] == 2
    assert out["status"] == "in_progress"


def test_get_welcome_progress_completed_status(monkeypatch):
    joined = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
    events = [_checkin(42, joined, 0), _checkin(42, joined, 1), _checkin(42, joined, 2)]
    app = _patch_progress(monkeypatch, joined=joined, events=events)
    with app.app_context():
        out = m.get_welcome_progress(42, now=joined + timedelta(days=3))
    assert out["completed"] == 3
    assert out["progress_pct"] == 100
    assert out["status"] == "completed"


class FakeCursor(list):
    def limit(self, _n):
        return self


class FakeReminderCollection:
    def __init__(self, docs):
        self._docs = docs

    def find(self, filt):  # noqa: ARG002
        return FakeCursor(self._docs)

    def update_one(self, filt, update):
        for doc in self._docs:
            if doc.get("_id") == filt.get("_id"):
                doc.update(update.get("$set", {}))


class FakeUsersCol:
    def find_one(self, filt, projection=None):  # noqa: ARG002
        return {}


class FakeReminderDb:
    def __init__(self, docs):
        self.welcome_reminders = FakeReminderCollection(docs)
        self.users = FakeUsersCol()


def test_process_welcome_reminders_sends_20h_reminder_once(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    day1_at = now - timedelta(hours=21)
    doc = {
        "_id": "r1",
        "user_id": 42,
        "day1_at": day1_at,
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    sent = []

    def fake_send(uid, text):
        sent.append((uid, text))
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)

    assert result["reminder_20h_sent"] == 1
    assert doc["reminder_20h_sent"] is True
    assert len(sent) == 1
    assert sent[0][0] == 42

    # Second run must not resend.
    sent.clear()
    result2 = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)
    assert result2["reminder_20h_sent"] == 0
    assert sent == []


def test_process_welcome_reminders_skips_claimed_users(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r2",
        "user_id": 43,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    sent = []

    def fake_send(uid, text):
        sent.append((uid, text))
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": True, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)
    assert result["skipped_abuse"] == 1
    assert result["skip_breakdown"]["already_claimed"] == 1
    assert sent == []


def test_process_welcome_reminders_tallies_eligible_and_missing_data(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    eligible_doc = {
        "_id": "r5",
        "user_id": 45,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    missing_uid_doc = {
        "_id": "r6",
        "user_id": None,
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([eligible_doc, missing_uid_doc])

    def fake_send(uid, text):
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)
    assert result["scanned"] == 2
    assert result["eligible_20h"] == 1
    assert result["reminder_20h_sent"] == 1
    assert result["skip_breakdown"]["missing_data"] == 1


def test_process_welcome_reminders_bot_blocked_counts_as_blocked_users(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r7",
        "user_id": 46,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])

    def fake_send(uid, text):
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})
    monkeypatch.setattr(
        scheduler,
        "_welcome_reminder_anti_abuse_blocked",
        lambda uid, db_ref, progress: "telegram_blocked",
    )

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)
    assert result["skip_breakdown"]["bot_blocked"] == 1
    assert result["blocked_users"] == 1


def test_process_welcome_reminders_day2_uses_bot_send_fn_when_available(monkeypatch):
    now = datetime(2026, 1, 4, 12, 0, tzinfo=timezone.utc)
    day2_at = now - timedelta(hours=21)
    doc = {
        "_id": "r3",
        "user_id": 44,
        "day1_at": now - timedelta(days=1, hours=21),
        "day2_at": day2_at,
        "reminder_20h_sent": True,
        "reminder_28h_sent": True,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    bot_sent = []
    http_sent = []

    def fake_bot_send(uid, text):
        bot_sent.append((uid, text))
        return True

    def fake_http_send(uid, text):
        http_sent.append((uid, text))
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 2, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_http_send, bot_send_fn=fake_bot_send)

    assert result["day2_reminder_sent"] == 1
    assert doc["day2_reminder_sent"] is True
    assert bot_sent == [(44, scheduler._WELCOME_PROGRESS_REMINDER_DAY2)]
    assert http_sent == []


def test_process_welcome_reminders_day2_falls_back_to_http_when_bot_send_fails(monkeypatch):
    now = datetime(2026, 1, 4, 12, 0, tzinfo=timezone.utc)
    day2_at = now - timedelta(hours=21)
    doc = {
        "_id": "r4",
        "user_id": 45,
        "day1_at": now - timedelta(days=1, hours=21),
        "day2_at": day2_at,
        "reminder_20h_sent": True,
        "reminder_28h_sent": True,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    http_sent = []

    def fake_bot_send(uid, text):  # noqa: ARG001
        raise RuntimeError("bot loop not running")

    def fake_http_send(uid, text):
        http_sent.append((uid, text))
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 2, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_http_send, bot_send_fn=fake_bot_send)

    assert result["day2_reminder_sent"] == 1
    assert doc["day2_reminder_sent"] is True
    assert http_sent == [(45, scheduler._WELCOME_PROGRESS_REMINDER_DAY2)]


def test_process_welcome_reminders_sends_28h_reminder_with_button(monkeypatch):
    now = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)
    day1_at = now - timedelta(hours=29)
    doc = {
        "_id": "r28",
        "user_id": 46,
        "day1_at": day1_at,
        "reminder_20h_sent": True,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    bot_sent = []

    def fake_bot_send(uid, text):
        bot_sent.append((uid, text))
        return True

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, bot_send_fn=fake_bot_send)

    assert result["reminder_28h_sent"] == 1
    assert doc["reminder_28h_sent"] is True
    assert bot_sent == [(46, scheduler._WELCOME_PROGRESS_REMINDER_28H)]


def test_process_welcome_reminders_20h_not_sent_before_threshold(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    day1_at = now - timedelta(hours=19)  # under the 20h threshold
    doc = {
        "_id": "r_early",
        "user_id": 47,
        "day1_at": day1_at,
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    sent = []

    def fake_send(uid, text):
        sent.append((uid, text))
        return True, None, False

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)

    assert result["reminder_20h_sent"] == 0
    assert result["eligible_20h"] == 0
    assert sent == []
    assert doc["reminder_20h_sent"] is False


def test_process_welcome_reminders_progress_advanced_suppresses_20h_and_28h(monkeypatch):
    """A user who already reached 2/3 must not receive the 1/3-stage nudges,
    even though the raw day1_at age would otherwise qualify for both."""
    now = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r_advanced",
        "user_id": 48,
        "day1_at": now - timedelta(hours=30),
        "day2_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    sent = []

    def fake_bot_send(uid, text):
        sent.append((uid, text))
        return True

    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 2, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, bot_send_fn=fake_bot_send)

    assert result["reminder_20h_sent"] == 0
    assert result["reminder_28h_sent"] == 0
    assert result["day2_reminder_sent"] == 1
    assert sent == [(48, scheduler._WELCOME_PROGRESS_REMINDER_DAY2)]


def test_welcome_http_send_fn_includes_miniapp_button(monkeypatch):
    """Regression test for the HTTP-fallback path silently stripping the
    Mini-App button: the default send_fn used when bot_send_fn is absent or
    fails must still carry a reply_markup with a web_app button."""
    captured = {}

    def fake_send_telegram_http_message(uid, text, *, reply_markup=None, **kwargs):  # noqa: ARG001
        captured["uid"] = uid
        captured["text"] = text
        captured["reply_markup"] = reply_markup
        return True, None, False

    monkeypatch.setattr(scheduler, "send_telegram_http_message", fake_send_telegram_http_message)

    ok, err, blocked = scheduler._welcome_http_send_fn(49, "hello")

    assert ok is True
    assert err is None
    assert blocked is False
    markup = captured["reply_markup"]
    assert markup is not None
    button = markup["inline_keyboard"][0][0]
    assert "web_app" in button
    assert button["web_app"]["url"].startswith("https://")


def test_process_welcome_reminders_uses_button_http_fallback_when_bot_send_absent(monkeypatch):
    """End-to-end: when no bot_send_fn is supplied at all, the reminder must
    still go out with a working Mini-App button via the HTTP fallback."""
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    day1_at = now - timedelta(hours=21)
    doc = {
        "_id": "r_fallback",
        "user_id": 50,
        "day1_at": day1_at,
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    captured = {}

    def fake_send_telegram_http_message(uid, text, *, reply_markup=None, **kwargs):  # noqa: ARG001
        captured["reply_markup"] = reply_markup
        return True, None, False

    monkeypatch.setattr(scheduler, "send_telegram_http_message", fake_send_telegram_http_message)
    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db)

    assert result["reminder_20h_sent"] == 1
    assert doc["reminder_20h_sent"] is True
    assert captured["reply_markup"] is not None
    assert "web_app" in captured["reply_markup"]["inline_keyboard"][0][0]


class FakeAnalyticsCollection:
    """Minimal stand-in for ``welcome_analytics_events_col`` supporting both
    plain appends (``insert_one``) and the dedup upsert path used for skip
    events (``update_one`` with ``$setOnInsert`` + ``upsert=True``)."""

    def __init__(self):
        self.docs = []

    def insert_one(self, doc):
        self.docs.append(dict(doc))

    def update_one(self, filt, update, upsert=False):
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in filt.items()):
                return
        if upsert and "$setOnInsert" in update:
            self.docs.append(dict(update["$setOnInsert"]))


def test_process_welcome_reminders_records_stage_and_status_on_send_failure(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r8",
        "user_id": 47,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    fake_events = FakeAnalyticsCollection()
    monkeypatch.setattr(m, "welcome_analytics_events_col", fake_events)
    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    def fake_send(uid, text):
        return False, "boom", False

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)

    assert result["send_failed"] == 1
    failed_events = [e for e in fake_events.docs if e["event"] == "welcome_reminder_failed"]
    assert len(failed_events) == 1
    assert failed_events[0]["stage"] == "20h"
    assert failed_events[0]["status"] == "failed"
    assert failed_events[0]["reason"] == "boom"
    assert failed_events[0]["run_id"] == result["run_id"]


def test_process_welcome_reminders_success_events_carry_stage_and_run_id(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r9",
        "user_id": 48,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    fake_events = FakeAnalyticsCollection()
    monkeypatch.setattr(m, "welcome_analytics_events_col", fake_events)
    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=lambda uid, text: (True, None, False))

    sent_events = [e for e in fake_events.docs if e["event"] == "welcome_reminder_20h_sent"]
    assert len(sent_events) == 1
    assert sent_events[0]["stage"] == "20h"
    assert sent_events[0]["status"] == "sent"
    assert sent_events[0]["run_id"] == result["run_id"]
    assert result["run_id"]


def test_process_welcome_reminders_skip_event_stage_matches_candidate_stage(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r10",
        "user_id": 49,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    fake_events = FakeAnalyticsCollection()
    monkeypatch.setattr(m, "welcome_analytics_events_col", fake_events)
    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})
    monkeypatch.setattr(scheduler, "_welcome_reminder_anti_abuse_blocked", lambda uid, db_ref, progress: "multi_account")

    scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=lambda uid, text: (True, None, False))

    skip_events = [e for e in fake_events.docs if e["event"] == "welcome_reminder_skipped"]
    assert len(skip_events) == 1
    assert skip_events[0]["stage"] == "20h"
    assert skip_events[0]["status"] == "skipped"
    assert skip_events[0]["reason"] == "multi_account"


def test_process_welcome_reminders_skip_events_dedupe_within_same_day(monkeypatch):
    """The hourly job re-evaluates the same permanently-blocked user every
    run; the skip event must not accumulate one row per hour."""
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r11",
        "user_id": 50,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    fake_events = FakeAnalyticsCollection()
    monkeypatch.setattr(m, "welcome_analytics_events_col", fake_events)
    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})
    monkeypatch.setattr(scheduler, "_welcome_reminder_anti_abuse_blocked", lambda uid, db_ref, progress: "multi_account")

    # All three runs land in the same KL calendar day and stay under the 28h
    # threshold (21h, 22h, 23h since day1_at), so only the "20h" stage is
    # ever a candidate — isolating this test to dedup behavior alone.
    scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=lambda uid, text: (True, None, False))
    scheduler.process_welcome_reminders(now_ref=now + timedelta(hours=1), db_ref=fake_db, send_fn=lambda uid, text: (True, None, False))
    scheduler.process_welcome_reminders(now_ref=now + timedelta(hours=2), db_ref=fake_db, send_fn=lambda uid, text: (True, None, False))

    skip_events = [e for e in fake_events.docs if e["event"] == "welcome_reminder_skipped"]
    assert len(skip_events) == 1

    # A skip on a later *local* calendar day must still be recorded (not
    # permanently suppressed) — the dedup key includes the KL day bucket.
    # +5h keeps elapsed time (26h) under the 28h threshold so "20h" is still
    # the only candidate stage, while crossing the KL midnight boundary.
    scheduler.process_welcome_reminders(now_ref=now + timedelta(hours=5), db_ref=fake_db, send_fn=lambda uid, text: (True, None, False))
    skip_events_after_next_day = [e for e in fake_events.docs if e["event"] == "welcome_reminder_skipped"]
    assert len(skip_events_after_next_day) == 2


def test_log_welcome_event_stage_values_are_normalized():
    assert set(m.WELCOME_REMINDER_STAGES) == {"20h", "28h", "day3", "completed"}


def test_process_welcome_reminders_isolates_malformed_user_and_processes_rest(monkeypatch):
    """Reproduces the production incident: one user's progress lookup
    raises (e.g. malformed/legacy check-in data reaching
    ``get_welcome_reward_progress`` via ``get_welcome_progress``). That
    single failure must not abort the batch — later, valid users must
    still be scanned and reminded, and the run must report the failure
    instead of raising out of ``process_welcome_reminders`` entirely."""
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    broken_doc = {
        "_id": "r-broken",
        "user_id": 91,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    healthy_doc = {
        "_id": "r-healthy",
        "user_id": 92,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([broken_doc, healthy_doc])
    sent = []

    def fake_send(uid, text):
        sent.append(uid)
        return True, None, False

    def fake_get_welcome_progress(uid, now=None):
        if uid == 91:
            # Stand-in for the production exception: a malformed legacy
            # check-in timestamp blowing up deep inside
            # get_welcome_reward_progress, surfaced through get_welcome_progress.
            raise TypeError(
                "get_welcome_reward_progress() got an unexpected keyword argument 'now'"
            )
        return {"completed": 1, "claimed": False, "expired": False}

    monkeypatch.setattr(m, "get_welcome_progress", fake_get_welcome_progress)

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)

    # The batch did not abort: the run completed and returned normally.
    assert result["scanned"] == 2
    assert result["failed_count"] == 1
    assert result["status"] == "partial_failure"
    assert result["failed_users"][0]["user_id"] == 91
    assert result["failed_users"][0]["run_id"] == result["run_id"]
    assert "TypeError" in result["failed_users"][0]["error"]

    # The later, valid user was still processed and reminded.
    assert sent == [92]
    assert healthy_doc["reminder_20h_sent"] is True
    assert result["reminder_20h_sent"] == 1

    # The broken user was never marked sent, so it remains retryable.
    assert broken_doc["reminder_20h_sent"] is False


def test_process_welcome_reminders_failed_user_is_retried_next_run_without_duplicating_others(monkeypatch):
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    broken_doc = {
        "_id": "r-broken2",
        "user_id": 93,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    healthy_doc = {
        "_id": "r-healthy2",
        "user_id": 94,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([broken_doc, healthy_doc])
    sent = []

    def fake_send(uid, text):
        sent.append(uid)
        return True, None, False

    calls = {"n": 0}

    def fake_get_welcome_progress(uid, now=None):
        if uid == 93 and calls["n"] == 0:
            raise ValueError("malformed check-in timestamp")
        return {"completed": 1, "claimed": False, "expired": False}

    monkeypatch.setattr(m, "get_welcome_progress", fake_get_welcome_progress)

    result1 = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=fake_send)
    assert result1["failed_count"] == 1
    assert sent == [94]
    assert healthy_doc["reminder_20h_sent"] is True

    # Second hourly run: the transient condition has cleared, and the
    # previously-healthy user must not receive a duplicate reminder.
    calls["n"] = 1
    sent.clear()
    result2 = scheduler.process_welcome_reminders(now_ref=now + timedelta(hours=1), db_ref=fake_db, send_fn=fake_send)
    assert result2["failed_count"] == 0
    assert sent == [93]
    assert broken_doc["reminder_20h_sent"] is True


def test_process_welcome_reminders_send_failure_mid_stage_is_isolated_with_stage_recorded(monkeypatch):
    """Exercises isolation for a failure raised mid-processing (during
    send) rather than during the progress lookup, confirming the
    recorded ``stage`` reflects where the failure occurred."""
    now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "r-send-crash",
        "user_id": 95,
        "day1_at": now - timedelta(hours=21),
        "reminder_20h_sent": False,
        "reminder_28h_sent": False,
        "day2_reminder_sent": False,
    }
    fake_db = FakeReminderDb([doc])
    monkeypatch.setattr(m, "get_welcome_progress", lambda uid, now=None: {"completed": 1, "claimed": False, "expired": False})

    def crashing_send(uid, text):
        raise ConnectionError("telegram api unreachable")

    result = scheduler.process_welcome_reminders(now_ref=now, db_ref=fake_db, send_fn=crashing_send)

    assert result["failed_count"] == 1
    assert result["failed_users"][0]["stage"] == "20h"
    assert doc["reminder_20h_sent"] is False


class FakeDistinctCollection:
    def __init__(self, docs):
        self.docs = docs

    def distinct(self, field, filt):
        out = set()
        for doc in self.docs:
            if filt.get("event") and doc.get("event") != filt.get("event"):
                continue
            if "user_id" in filt and isinstance(filt["user_id"], dict):
                if doc.get("user_id") not in filt["user_id"].get("$in", []):
                    continue
            out.add(doc.get(field))
        return list(out)

    def count_documents(self, filt):
        return sum(1 for _ in self.docs)


def test_welcome_journey_panel_computes_rates():
    events = [
        {"event": "welcome_checkin_d1", "user_id": 1},
        {"event": "welcome_checkin_d1", "user_id": 2},
        {"event": "welcome_checkin_d2", "user_id": 1},
        {"event": "welcome_completed", "user_id": 1},
    ]
    events_col = FakeDistinctCollection(events)

    class FakeEligCol:
        def count_documents(self, filt):
            return 2

    panel = dashboard_panels.build_welcome_journey_panel(
        welcome_eligibility_col=FakeEligCol(),
        welcome_analytics_events_col=events_col,
        now=datetime(2026, 1, 5, tzinfo=timezone.utc),
        window="all",
    )
    summary = panel["summary"]
    assert summary["welcome_eligible_users"]["value"] == 2
    assert summary["welcome_d2_rate_pct"]["value"] == 50.0
    assert summary["welcome_completion_rate_pct"]["value"] == 50.0
