"""GET /api/affiliate/leaderboard: "My Stats (This Month)" carries the
backend-owned tier-progress fields and the retention-gated reward status,
and keeps the pre-existing analytics fields.

Uses the same mongomock-import pattern as test_campaign_activity_integration.py.
"""
import os
import unittest.mock as mock
from datetime import datetime, timezone

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

from affiliate_rewards import evaluate_monthly_affiliate_reward

REFERRER = 818181


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "7")
    monkeypatch.delenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", raising=False)
    _cleanup()
    yield main.app.test_client()
    _cleanup()


def _cleanup():
    main.db.users.delete_many({"user_id": REFERRER})
    main.db.qualified_events.delete_many({"referrer_id": REFERRER})
    main.db.affiliate_ledger.delete_many({"user_id": REFERRER})
    # The live monthly payload is cached in Mongo; never read a stale one.
    main.db.affiliate_monthly_kpis_live.delete_many({})


def _seed(qualified):
    now = datetime.now(timezone.utc)
    main.db.users.insert_one({"user_id": REFERRER, "first_name": "Aff", "official_channel_currently_subscribed": True})
    for i in range(qualified):
        main.db.qualified_events.insert_one(
            {"invitee_id": REFERRER * 1000 + i, "referrer_id": REFERRER, "qualified_at": now}
        )
    return now


def _my_stats(client):
    resp = client.get(f"/api/affiliate/leaderboard?window=month&user_id={REFERRER}")
    assert resp.status_code == 200
    body = resp.get_json()
    return body, body["my_stats"]


def test_my_stats_carries_next_tier_progress(client):
    _seed(18)
    body, my = _my_stats(client)
    assert my["qualified_month"] == 18
    assert my["next_tier"] == "T2"
    assert my["qualified_left"] == 7
    assert my["next_reward_value"] == main.affiliate_next_tier_progress(18, entitlement_month=body["month_key"])["next_reward_value"]
    assert my["max_tier_reached"] is False
    # Existing analytics fields are preserved for current consumers.
    assert "joins_month" in my and "conversion_month" in my
    assert my["reward_entitlements"] == []


def test_my_stats_lists_retention_gated_reward_without_codes(client):
    now = _seed(10)
    evaluate_monthly_affiliate_reward(main.db, referrer_id=REFERRER, now_utc=now)
    _, my = _my_stats(client)
    entitlements = my["reward_entitlements"]
    assert [e["tier"] for e in entitlements] == ["T1"]
    item = entitlements[0]
    assert item["retention_state"] == "pending_retention"
    assert 0 < item["remaining_seconds"] <= 7 * 86400
    assert "vouchers" not in item and "voucher_code" not in item
