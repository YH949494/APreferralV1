import os
from datetime import datetime, timezone
from unittest.mock import patch

from pymongo.errors import DuplicateKeyError

import scheduler


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, keys):
        docs = list(self._docs)
        for field, direction in reversed(keys):
            docs.sort(key=lambda d, f=field: d.get(f), reverse=(direction == -1))
        self._docs = docs
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeUsersCollection:
    def __init__(self, docs):
        self.docs = docs
        self.find_calls = []

    def find(self, filt, proj=None):
        self.find_calls.append(filt)
        gt = ((filt or {}).get("weekly_referrals") or {}).get("$gt")
        matched = [d for d in self.docs if gt is None or d.get("weekly_referrals", 0) > gt]
        if proj:
            matched = [{k: d.get(k) for k in proj.keys()} for d in matched]
        return _FakeCursor(matched)


class _FakePostsCollection:
    def __init__(self):
        self.docs = {}

    def find_one(self, filt, proj=None):
        d = self.docs.get(filt.get("_id"))
        return dict(d) if d else None

    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("dup")
        self.docs[doc["_id"]] = dict(doc)

    def update_one(self, filt, update, upsert=False):
        _id = filt.get("_id")
        if _id in self.docs:
            self.docs[_id].update(update.get("$set", {}))


class _FakeHistoryCollection:
    def __init__(self, by_week_start=None):
        self.by_week_start = by_week_start or {}

    def find_one(self, filt):
        d = self.by_week_start.get(filt.get("week_start"))
        return dict(d) if d else None


class _FakeDb:
    def __init__(self, users_docs=None, history=None):
        self.users = _FakeUsersCollection(users_docs or [])
        self.weekly_referral_posts = _FakePostsCollection()
        self.weekly_leaderboard_history = _FakeHistoryCollection(history)


class _OkResp:
    status_code = 200
    content = b"1"

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True, "result": {"message_id": 555}}


class _FailResp:
    status_code = 200
    content = b"1"

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": False, "description": "boom"}


NOW = datetime(2026, 7, 27, 13, 0, tzinfo=timezone.utc)  # Monday 21:00 KL is Sunday's evening; pick a Monday UTC ts within week of 2026-07-20


def _env(**extra):
    base = {"WEEKLY_REF_POST_CHAT_ID": "-100999"}
    base.update(extra)
    return patch.dict(os.environ, base, clear=False)


def test_top5_ordering_and_tiebreak_and_zero_excluded():
    users = [
        {"user_id": 5, "username": "e", "weekly_referrals": 7},
        {"user_id": 1, "username": "a", "weekly_referrals": 21},
        {"user_id": 2, "username": "b", "weekly_referrals": 14},
        {"user_id": 3, "username": "c", "weekly_referrals": 10},
        {"user_id": 4, "username": "d", "weekly_referrals": 10},
        {"user_id": 9, "username": "zero", "weekly_referrals": 0},
        {"user_id": 8, "username": "extra", "weekly_referrals": 3},
    ]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()):
        doc = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")

    entries = doc["entries"]
    assert [e["user_id"] for e in entries] == [1, 2, 3, 4, 5]
    assert entries[2]["weekly_referrals"] == entries[3]["weekly_referrals"] == 10
    assert entries[2]["user_id"] < entries[3]["user_id"]  # tie-break: user_id ascending
    assert all(e["user_id"] != 9 for e in entries)  # zero excluded
    assert len(entries) == 5  # only top 5 kept, "extra" dropped


def test_fewer_than_five_users_supported():
    users = [
        {"user_id": 1, "username": "a", "weekly_referrals": 3},
        {"user_id": 2, "username": "b", "weekly_referrals": 1},
    ]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()) as post_mock:
        doc = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert len(doc["entries"]) == 2
    assert doc["status"] == "sent"
    assert post_mock.called


def test_empty_week_skips_safely():
    fake_db = _FakeDb(users_docs=[])
    with _env(), patch.object(scheduler.requests, "post") as post_mock:
        doc = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert doc["status"] == "empty"
    post_mock.assert_not_called()


def test_historical_text_format_and_escaping():
    users = [
        {"user_id": 1, "username": "a<b", "weekly_referrals": 21},
        {"user_id": 2, "first_name": "Bee", "weekly_referrals": 14},
        {"user_id": 3, "weekly_referrals": 10},
        {"user_id": 4, "username": "d", "weekly_referrals": 10},
        {"user_id": 5, "username": "e", "weekly_referrals": 7},
    ]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()) as post_mock:
        scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    text = post_mock.call_args.kwargs["json"]["text"]
    assert "<b>🏆 Top 5 Growth Leaders This Week</b>" in text
    assert "🥇 @a&lt;b — 21 qualified invites" in text
    assert "🥈 Bee — 14 qualified invites" in text
    assert "🥉 Member #3 — 10 qualified invites" in text
    assert "#4 @d — 10 qualified invites" in text
    assert "#5 @e — 7 qualified invites" in text
    assert "Invite more qualified members, join our affiliate program, and earn up to <b>$450/month</b>." in text


def test_telegram_success_records_message_id():
    users = [{"user_id": 1, "username": "a", "weekly_referrals": 5}]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()):
        doc = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert doc["status"] == "sent"
    assert doc["message_id"] == 555
    assert doc["sent_at"] is not None


def test_telegram_failure_remains_retryable():
    users = [{"user_id": 1, "username": "a", "weekly_referrals": 5}]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_FailResp()):
        doc = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert doc["status"] == "failed"
    assert doc["message_id"] is None
    assert doc["failure_reason"]


def test_frozen_ranking_unchanged_between_retries():
    users = [{"user_id": 1, "username": "a", "weekly_referrals": 5}]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_FailResp()):
        first = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert first["status"] == "failed"

    # Underlying data changes before the retry - frozen ranking must not move.
    fake_db.users.docs.append({"user_id": 2, "username": "new", "weekly_referrals": 99})

    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()) as post_mock:
        second = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")

    assert second["status"] == "sent"
    assert [e["user_id"] for e in second["entries"]] == [1]
    text = post_mock.call_args.kwargs["json"]["text"]
    assert "new" not in text


def test_duplicate_invocation_does_not_duplicate_post():
    users = [{"user_id": 1, "username": "a", "weekly_referrals": 5}]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()) as post_mock:
        scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
        second = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert post_mock.call_count == 1
    assert second["status"] == "sent"
    assert second["message_id"] == 555


def test_missing_destination_fails_safely():
    users = [{"user_id": 1, "username": "a", "weekly_referrals": 5}]
    fake_db = _FakeDb(users_docs=users)
    with patch.dict(os.environ, {"WEEKLY_REF_POST_CHAT_ID": ""}, clear=False), patch.object(scheduler.requests, "post") as post_mock:
        doc = scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert doc["status"] == "failed"
    assert doc["failure_reason"] == "missing_chat_id"
    post_mock.assert_not_called()


def test_uses_weekly_referrals_snapshot_not_raw_joins():
    users = [{"user_id": 1, "username": "a", "weekly_referrals": 5}]
    fake_db = _FakeDb(users_docs=users)
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()):
        scheduler.publish_weekly_referral_post(db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-20")
    assert fake_db.users.find_calls, "expected a direct users.find() call"
    assert fake_db.users.find_calls[0] == {"weekly_referrals": {"$gt": 0}}
    assert not hasattr(fake_db, "qualified_events")  # not sourced from raw qualified_events aggregation


def test_historical_repair_reads_pre_reset_archive_not_current_counters():
    # Simulate a week that has already been reset: live users have no
    # meaningful weekly_referrals, but the pre-reset archive still has it.
    fake_db = _FakeDb(
        users_docs=[{"user_id": 1, "username": "stale", "weekly_referrals": 0}],
        history={
            "2026-07-13": {
                "week_start": "2026-07-13",
                "week_end": "2026-07-19",
                "referral_leaderboard": [
                    {"user_id": 10, "username": "hist1", "weekly_referrals": 8},
                    {"user_id": 11, "username": "hist2", "weekly_referrals": 3},
                ],
            }
        },
    )
    with _env(), patch.object(scheduler.requests, "post", return_value=_OkResp()):
        doc = scheduler.publish_weekly_referral_post(
            db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-13", source="archive"
        )
    assert [e["user_id"] for e in doc["entries"]] == [10, 11]
    assert not fake_db.users.find_calls  # never touched live users collection


def test_repair_dry_run_previews_without_sending():
    fake_db = _FakeDb(
        history={
            "2026-07-13": {
                "week_start": "2026-07-13",
                "referral_leaderboard": [{"user_id": 10, "username": "hist1", "weekly_referrals": 8}],
            }
        }
    )
    with _env(), patch.object(scheduler.requests, "post") as post_mock:
        doc = scheduler.publish_weekly_referral_post(
            db_ref=fake_db, now_utc_ts=NOW, week_key="2026-07-13", dry_run=True, source="archive"
        )
    post_mock.assert_not_called()
    assert "preview_text" in doc
    assert "🥇 @hist1 — 8 qualified invites" in doc["preview_text"]
    assert fake_db.weekly_referral_posts.docs["weekly_referral_post:2026-07-13"]["status"] != "sent"
