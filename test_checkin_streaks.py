from datetime import datetime, timezone
from unittest.mock import patch

from flask import Flask

import checkin
from config import KL_TZ, STREAK_MILESTONES, XP_BASE_PER_CHECKIN


class FakeUsersCollection:
    def __init__(self):
        self.docs = {}

    def find_one(self, filt):
        return self.docs.get(filt.get("user_id"))

    def update_one(self, filt, update, upsert=False):  # noqa: ARG002
        user_id = filt["user_id"]
        current = self.docs.get(user_id, {"user_id": user_id})
        for k, v in update.get("$set", {}).items():
            current[k] = v
        if user_id not in self.docs:
            for k, v in update.get("$setOnInsert", {}).items():
                current.setdefault(k, v)
        self.docs[user_id] = current


class FixedDatetime(datetime):
    _now = None

    @classmethod
    def now(cls, tz=None):
        if tz is not None:
            return cls._now.astimezone(tz)
        return cls._now


def _run_checkin(client, user_id=1001, username="alice"):
    response = client.get(f"/checkin?user_id={user_id}&username={username}")
    return response, response.get_json()


def test_first_ever_checkin_sets_streak_one_and_grants_xp_once():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()
    xp_calls = []

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: xp_calls.append(args) or True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        FixedDatetime._now = datetime(2026, 1, 1, 8, 0, tzinfo=KL_TZ)
        client = app.test_client()
        response, payload = _run_checkin(client)

    assert response.status_code == 200
    assert payload["success"] is True
    assert users.docs[1001]["streak"] == 1
    assert xp_calls[0][4] == XP_BASE_PER_CHECKIN


def test_same_kl_day_duplicate_does_not_increment_or_grant_twice():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()
    xp_calls = []

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: xp_calls.append(args) or True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        client = app.test_client()
        FixedDatetime._now = datetime(2026, 1, 1, 8, 0, tzinfo=KL_TZ)
        _run_checkin(client)
        first_streak = users.docs[1001]["streak"]

        FixedDatetime._now = datetime(2026, 1, 1, 23, 30, tzinfo=KL_TZ)
        response, payload = _run_checkin(client)

    assert response.status_code == 200
    assert payload["success"] is False
    assert users.docs[1001]["streak"] == first_streak == 1
    assert len([c for c in xp_calls if c[2] == "checkin"]) == 1


def test_consecutive_kl_day_increments_streak_by_one():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        client = app.test_client()
        FixedDatetime._now = datetime(2026, 1, 1, 10, 0, tzinfo=KL_TZ)
        _run_checkin(client)
        FixedDatetime._now = datetime(2026, 1, 2, 10, 0, tzinfo=KL_TZ)
        _run_checkin(client)

    assert users.docs[1001]["streak"] == 2


def test_missed_day_resets_streak_to_one():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        client = app.test_client()
        FixedDatetime._now = datetime(2026, 1, 1, 10, 0, tzinfo=KL_TZ)
        _run_checkin(client)
        FixedDatetime._now = datetime(2026, 1, 2, 10, 0, tzinfo=KL_TZ)
        _run_checkin(client)
        FixedDatetime._now = datetime(2026, 1, 4, 10, 0, tzinfo=KL_TZ)
        _run_checkin(client)

    assert users.docs[1001]["streak"] == 1


def test_7_day_milestone_bonus_is_granted_once_only():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()
    xp_calls = []

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: xp_calls.append(args) or True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        client = app.test_client()
        for day in range(1, 8):
            FixedDatetime._now = datetime(2026, 1, day, 10, 0, tzinfo=KL_TZ)
            _run_checkin(client)
        FixedDatetime._now = datetime(2026, 1, 7, 12, 0, tzinfo=KL_TZ)
        _run_checkin(client)

    milestone_total = XP_BASE_PER_CHECKIN + STREAK_MILESTONES[7]
    checkin_amounts = [c[4] for c in xp_calls if c[2] == "checkin"]
    assert checkin_amounts.count(milestone_total) == 1


def test_timezone_boundary_uses_kl_day_not_utc_day():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        client = app.test_client()
        # 2026-01-01 16:05 UTC == 2026-01-02 00:05 KL
        FixedDatetime._now = datetime(2026, 1, 1, 16, 5, tzinfo=timezone.utc)
        _run_checkin(client)
        # next UTC day but still same KL calendar day
        FixedDatetime._now = datetime(2026, 1, 1, 23, 50, tzinfo=timezone.utc)
        _, payload = _run_checkin(client)

    assert payload["success"] is False
    assert users.docs[1001]["streak"] == 1


def test_to_aware_utc_naive_datetime_interpreted_as_kl_local():
    naive_dt = datetime(2026, 1, 2, 0, 5, 0)
    aware_utc = checkin._to_aware_utc(naive_dt)
    assert aware_utc is not None
    assert aware_utc.tzinfo == timezone.utc
    assert aware_utc.hour == 16
    assert aware_utc.day == 1


def test_to_aware_utc_accepts_iso_z_string():
    aware_utc = checkin._to_aware_utc("2026-01-01T16:05:00Z")
    assert aware_utc is not None
    assert aware_utc.tzinfo == timezone.utc
    assert aware_utc.isoformat() == "2026-01-01T16:05:00+00:00"


def test_bad_last_checkin_string_does_not_500_and_resets_streak():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()
    users.docs[1001] = {"user_id": 1001, "last_checkin": "bad-date", "streak": 12}

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        FixedDatetime._now = datetime(2026, 1, 10, 9, 0, tzinfo=KL_TZ)
        client = app.test_client()
        response, payload = _run_checkin(client)

    assert response.status_code == 200
    assert payload["success"] is True
    assert users.docs[1001]["streak"] == 1


def test_bad_last_checkin_none_does_not_500():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()
    users.docs[1001] = {"user_id": 1001, "last_checkin": None, "streak": 4}

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        FixedDatetime._now = datetime(2026, 1, 10, 9, 0, tzinfo=KL_TZ)
        client = app.test_client()
        response, payload = _run_checkin(client)

    assert response.status_code == 200
    assert payload["success"] is True
    assert users.docs[1001]["streak"] == 1


def test_bad_last_checkin_unsupported_type_does_not_500():
    app = Flask(__name__)
    app.add_url_rule("/checkin", "checkin", checkin.handle_checkin)
    users = FakeUsersCollection()
    users.docs[1001] = {"user_id": 1001, "last_checkin": 12345, "streak": 9}

    with (
        patch.object(checkin, "users_collection", users),
        patch.object(checkin, "record_user_last_seen", lambda *args, **kwargs: None),
        patch.object(checkin, "record_first_checkin", lambda *args, **kwargs: None),
        patch.object(checkin, "grant_xp", lambda *args: True),
        patch.object(checkin, "datetime", FixedDatetime),
    ):
        FixedDatetime._now = datetime(2026, 1, 10, 9, 0, tzinfo=KL_TZ)
        client = app.test_client()
        response, payload = _run_checkin(client)

    assert response.status_code == 200
    assert payload["success"] is True
    assert users.docs[1001]["streak"] == 1
