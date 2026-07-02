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
    assert sent == []


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
