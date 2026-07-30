"""Integration coverage for the Sunday weekly referral post restoration:

- reset_weekly_xp() (Monday 00:00 KL) must not delete an existing
  weekly_referral_posts record for the week it is archiving.
- The Sunday 21:00 KL scheduler job must be registered with a stable id.

Uses the same mongomock-import pattern as test_checkin_streak_integrity.py.
"""
import os
import re
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock
import pytest

import database

if database._db is None:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
else:  # pragma: no cover
    import main  # noqa: E402


def test_monday_reset_does_not_remove_weekly_referral_post_history():
    now = datetime.now(main.KL_TZ)
    week_end_date = (now - timedelta(days=1)).date()
    week_start_date = week_end_date - timedelta(days=6)
    doc_id = f"weekly_referral_post:{week_start_date.isoformat()}"

    posts = main.db["weekly_referral_posts"]
    posts.insert_one(
        {
            "_id": doc_id,
            "week_key": week_start_date.isoformat(),
            "status": "sent",
            "message_id": 4242,
            "entries": [{"user_id": 1, "display_name": "@a", "weekly_referrals": 5}],
        }
    )

    main.users_collection.insert_one({"user_id": 1, "username": "a", "weekly_xp": 10, "weekly_referrals": 5})

    main.reset_weekly_xp(run_id="test_run")

    kept = posts.find_one({"_id": doc_id})
    assert kept is not None
    assert kept["status"] == "sent"
    assert kept["message_id"] == 4242
    assert kept["entries"] == [{"user_id": 1, "display_name": "@a", "weekly_referrals": 5}]

    # counters for the *new* week must still reset as before
    user = main.users_collection.find_one({"user_id": 1})
    assert user["weekly_referrals"] == 0
    assert user["weekly_xp"] == 0


def test_sunday_2100_scheduler_job_registered_in_source():
    src = open(os.path.join(os.path.dirname(main.__file__), "main.py"), encoding="utf-8").read()
    # Find the weekly_referral_post add_job block and assert its trigger.
    m = re.search(
        r'scheduler\.add_job\(\s*_guarded_job\("weekly_referral_post".*?\).*?id="weekly_referral_post"',
        src,
        re.S,
    )
    assert m, "weekly_referral_post job registration not found"
    block = m.group(0)
    assert 'day_of_week="sun"' in block
    assert "hour=21" in block
    assert "minute=0" in block
    assert "timezone=KL_TZ" in block
