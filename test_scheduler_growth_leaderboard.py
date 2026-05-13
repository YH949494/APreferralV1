import os
from datetime import datetime, timezone
from unittest.mock import patch

import scheduler


class _FakeAggregateCollection:
    def __init__(self, rows):
        self.rows = rows

    def aggregate(self, pipeline):
        return list(self.rows)


class _FakeUsers:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, filt, proj=None):
        uid = filt.get("user_id")
        d = self.docs.get(uid)
        if not d:
            return None
        if proj:
            return {k: d.get(k) for k in proj.keys()}
        return dict(d)


class _FakePosts:
    def __init__(self, existing=False):
        self.doc = {"week_key": "2026-05-11"} if existing else None

    def find_one(self, filt, proj=None):
        if self.doc and self.doc.get("week_key") == filt.get("week_key"):
            return dict(self.doc)
        return None

    def update_one(self, filt, update, upsert=False):
        if "$setOnInsert" in update and upsert:
            if self.doc is not None:
                return type("_R", (), {"upserted_id": None})()
            self.doc = dict(update["$setOnInsert"])
            return type("_R", (), {"upserted_id": "ins"})()
        if self.doc and self.doc.get("week_key") == filt.get("week_key"):
            self.doc.update(update.get("$set", {}))
        return type("_R", (), {"upserted_id": None})()


class _FakeDb:
    def __init__(self, rows, users=None, existing=False):
        self.qualified_events = _FakeAggregateCollection(rows)
        self.users = _FakeUsers(users or {})
        self.growth_leaderboard_posts = _FakePosts(existing=existing)


def test_growth_top5_order_and_format_and_escape():
    rows = [
        {"_id": 1, "qualified_count": 43},
        {"_id": 2, "qualified_count": 31},
        {"_id": 3, "qualified_count": 29},
        {"_id": 4, "qualified_count": 18},
        {"_id": 5, "qualified_count": 12},
        {"_id": 6, "qualified_count": 9},
    ]
    users = {1: {"username": "a<b"}, 2: {"first_name": "Bee"}, 3: {}, 4: {"username": "D"}, 5: {"username": "E"}}
    fake_db = _FakeDb(rows, users=users)
    now_ts = datetime(2026, 5, 13, tzinfo=timezone.utc)

    class _Resp:
        status_code = 200
        content = b"1"

        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True, "result": {"message_id": 99}}

    with patch.dict(os.environ, {"GROWTH_LEADERBOARD_CHANNEL_ID": "-100123", "GROWTH_LEADERBOARD_TIMEZONE": "Asia/Kuala_Lumpur"}, clear=False), patch.object(
        scheduler.requests, "post", return_value=_Resp()
    ) as post_mock:
        out = scheduler.post_growth_leaderboard_weekly(db_ref=fake_db, now_utc_ts=now_ts)

    assert out is True
    sent = post_mock.call_args.kwargs["json"]["text"]
    assert "<b>🏆 Top 5 Growth Leaders This Week</b>" in sent
    assert "🥇 a&lt;b — 43 qualified invites" in sent
    assert "🥈 Bee — 31 qualified invites" in sent
    assert "🥉 User 3 — 29 qualified invites" in sent
    assert "#4 D — 18 qualified invites" in sent
    assert "#5 E — 12 qualified invites" in sent
    assert "<i>Invite more qualified members, join our affiliate program, and earn up to <b>$450/month</b>.</i>" in sent


def test_growth_empty_skips_posting():
    fake_db = _FakeDb([])
    now_ts = datetime(2026, 5, 13, tzinfo=timezone.utc)
    with patch.dict(os.environ, {"GROWTH_LEADERBOARD_CHANNEL_ID": "-100123"}, clear=False), patch.object(scheduler.requests, "post") as post_mock:
        out = scheduler.post_growth_leaderboard_weekly(db_ref=fake_db, now_utc_ts=now_ts)
    assert out is False
    post_mock.assert_not_called()


def test_growth_duplicate_week_skips_posting():
    fake_db = _FakeDb([{"_id": 1, "qualified_count": 1}], existing=True)
    now_ts = datetime(2026, 5, 13, tzinfo=timezone.utc)
    with patch.dict(os.environ, {"GROWTH_LEADERBOARD_CHANNEL_ID": "-100123"}, clear=False), patch.object(scheduler.requests, "post") as post_mock:
        out = scheduler.post_growth_leaderboard_weekly(db_ref=fake_db, now_utc_ts=now_ts)
    assert out is False
    post_mock.assert_not_called()
