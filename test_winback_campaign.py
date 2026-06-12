from datetime import datetime, timedelta, timezone

import scheduler


class _Users:
    def __init__(self, docs):
        self.docs = docs
        self.updated = []

    def find(self, *args, **kwargs):  # noqa: ARG002
        return _Cursor(self.docs)

    def update_one(self, query, update):
        self.updated.append((query, update))


class _Cursor:
    def __init__(self, docs):
        self.docs = docs

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __iter__(self):
        return iter(self.docs)


class _DB:
    def __init__(self, docs):
        self.users = _Users(docs)


class _Resp:
    def __init__(self, ok=True):
        self.content = b"{}"
        self._ok = ok

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError("http")

    def json(self):
        return {"ok": self._ok}


def _setup(monkeypatch, docs, send_ok=True, pref_allowed=True):
    fake_db = _DB(docs)
    monkeypatch.setattr(scheduler, "db", fake_db)
    monkeypatch.setattr(scheduler, "pm_allowed", lambda *args, **kwargs: pref_allowed)
    monkeypatch.setattr(
        scheduler,
        "requests",
        type("R", (), {"post": lambda *args, **kwargs: _Resp(ok=send_ok)})(),
    )
    return fake_db


def test_disabled_config_noop_wrapper(monkeypatch):
    monkeypatch.setattr(scheduler, "WINBACK_ENABLED", False)
    out = scheduler.run_winback_campaign_if_enabled()
    assert out["scanned"] == 0
    assert out["sent"] == 0


def test_inactive_user_eligible_send(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [{"user_id": 1, "updated_at": now_ts - timedelta(days=20)}]
    db = _setup(monkeypatch, docs)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts)
    assert out["eligible"] == 1 and out["sent"] == 1
    assert len(db.users.updated) == 1


def test_recent_user_skipped(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [{"user_id": 1, "updated_at": now_ts - timedelta(days=1)}]
    _setup(monkeypatch, docs)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts)
    assert out["eligible"] == 0 and out["suppressed"] == 1


def test_pm_preference_false_suppressed(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [{"user_id": 1, "updated_at": now_ts - timedelta(days=20)}]
    _setup(monkeypatch, docs, pref_allowed=False)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts)
    assert out["sent"] == 0 and out["suppressed"] == 1


def test_cooldown_user_skipped(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [{"user_id": 1, "updated_at": now_ts - timedelta(days=20), "last_winback_pm_at": now_ts - timedelta(days=3)}]
    _setup(monkeypatch, docs)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts)
    assert out["sent"] == 0 and out["suppressed"] == 1


def test_dry_run_no_send_no_update(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [{"user_id": 1, "updated_at": now_ts - timedelta(days=20)}]
    db = _setup(monkeypatch, docs)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts, dry_run=True)
    assert out["eligible"] == 1 and out["sent"] == 0 and out["dry_run"] is True
    assert not db.users.updated


def test_malformed_dates_do_not_crash(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [{"user_id": 1, "updated_at": "bad-date"}]
    _setup(monkeypatch, docs)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts)
    assert out["suppressed"] == 1


def test_batch_limit_respected(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [
        {"user_id": 1, "updated_at": now_ts - timedelta(days=20)},
        {"user_id": 2, "updated_at": now_ts - timedelta(days=20)},
    ]
    _setup(monkeypatch, docs)
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts, batch_limit=1)
    assert out["scanned"] == 1


def test_send_failure_increments_failed_and_continues(monkeypatch):
    now_ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
    docs = [
        {"user_id": 1, "updated_at": now_ts - timedelta(days=20)},
        {"user_id": 2, "updated_at": now_ts - timedelta(days=20)},
    ]
    state = {"count": 0}

    def _post(*args, **kwargs):  # noqa: ARG001
        state["count"] += 1
        return _Resp(ok=state["count"] != 1)

    fake_db = _DB(docs)
    monkeypatch.setattr(scheduler, "db", fake_db)
    monkeypatch.setattr(scheduler, "pm_allowed", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler, "requests", type("R", (), {"post": _post})())
    out = scheduler.run_winback_campaign(now_utc_ts=now_ts)
    assert out["failed"] == 1 and out["sent"] == 1
