from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from apscheduler.triggers.cron import CronTrigger

from telegram_stats import (
    aggregate_telegram_post_stats_weekly,
    refresh_telegram_channel_stats,
    telegram_week_window_kl,
)


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find(self, query=None, projection=None):
        query = query or {}
        out = []
        for doc in self.docs:
            ok = True
            posted_filter = (query.get("posted_at_utc") or {})
            gte = posted_filter.get("$gte")
            lte = posted_filter.get("$lte")
            if gte and doc.get("posted_at_utc") < gte:
                ok = False
            if lte and doc.get("posted_at_utc") > lte:
                ok = False
            if ok:
                out.append(dict(doc))
        return out

    def update_one(self, filt, update, upsert=False):
        payload = update.get("$set", {})
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in filt.items()):
                doc.update(payload)
                return
        if upsert:
            row = dict(filt)
            row.update(payload)
            self.docs.append(row)


class _FakeDB:
    def __init__(self, collections=None):
        self._collections = collections or {}

    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = _FakeCollection()
        return self._collections[name]

    def list_collection_names(self):
        return list(self._collections.keys())


class _FakeBot:
    def __init__(self):
        self.used_statistics_api = False

    async def get_chat(self, *, chat_id):
        return SimpleNamespace(type="channel", title="Demo Channel", username="demo_channel")

    async def get_chat_member_count(self, *, chat_id):
        return 1234

    async def get_chat_administrators(self, *, chat_id):
        return [SimpleNamespace(user=SimpleNamespace(id=1)), SimpleNamespace(user=SimpleNamespace(id=2))]

    async def get_chat_statistics(self, *, chat_id):
        self.used_statistics_api = True
        raise AssertionError("unsupported API should not be used")


def test_unsupported_get_chat_statistics_path_removed():
    bot = _FakeBot()
    db = _FakeDB({"telegram_channel_stats": _FakeCollection()})

    out = asyncio.run(refresh_telegram_channel_stats(bot, -100111, db_ref=db))

    assert out["chat_id"] == -100111
    assert out["member_count"] == 1234
    assert out["administrator_count"] == 2
    assert bot.used_statistics_api is False


def test_week_window_kl_is_fixed_monday_to_sunday():
    ref = datetime(2026, 4, 12, 15, 55, tzinfo=timezone.utc)  # Sunday 23:55 KL
    window = telegram_week_window_kl(ref)

    assert window["week_start_kl"].isoformat() == "2026-04-06T00:00:00+08:00"
    assert window["week_end_kl"].isoformat() == "2026-04-12T23:59:59.999999+08:00"


def test_weekly_aggregation_counts_only_monday_to_sunday_posts():
    raw = _FakeCollection(
        [
            {"chat_id": -1001, "message_id": 10, "posted_at_utc": datetime(2026, 4, 5, 15, 59, 59, tzinfo=timezone.utc), "views": 10},
            {"chat_id": -1001, "message_id": 11, "posted_at_utc": datetime(2026, 4, 6, 0, 0, 0, tzinfo=timezone.utc), "views": 20, "forwards": 2},
            {"chat_id": -1001, "message_id": 12, "posted_at_utc": datetime(2026, 4, 12, 15, 59, 59, tzinfo=timezone.utc), "views": 30, "reactions_total": 3},
            {"chat_id": -1001, "message_id": 13, "posted_at_utc": datetime(2026, 4, 12, 16, 0, 0, tzinfo=timezone.utc), "views": 999},
        ]
    )
    weekly = _FakeCollection()
    db = _FakeDB({"telegram_post_stats_raw": raw, "telegram_post_stats_weekly": weekly})

    out = aggregate_telegram_post_stats_weekly(db_ref=db, reference_utc=datetime(2026, 4, 12, 15, 55, tzinfo=timezone.utc))

    assert out["posts"] == 2
    assert out["chats"] == 1
    assert len(weekly.docs) == 1
    row = weekly.docs[0]
    assert row["posts"] == 2
    assert row["views"] == 50
    assert row["forwards"] == 2
    assert row["reactions_total"] == 3


def test_weekly_scheduler_registration():
    trigger = CronTrigger(day_of_week="sun", hour=23, minute=55, timezone="Asia/Kuala_Lumpur")
    assert str(trigger.fields[4]) == "sun"
    assert str(trigger.fields[5]) == "23"
    assert str(trigger.fields[6]) == "55"
