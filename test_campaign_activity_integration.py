"""Integration coverage: one campaign_display_overrides document must power
identical ranks and qualified totals across every public surface --
Affiliate page (GET /api/affiliate/leaderboard), the dedicated public
campaign endpoints (GET /api/campaign/<id>/activity and
GET /api/campaign/active/activity, which Money Room's
static/creator-share.html calls directly), and the admin campaign
announcement preview (GET /api/admin/campaign/announcement-preview) --
since they all call the same campaign_display_override.build_public_campaign_activity().

Uses the same mongomock-import pattern as test_weekly_leaderboard_archive_recovery.py.
"""
import os
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

from campaign_display_override import CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION

CAMPAIGN_ID = "integration_test_campaign"


@pytest.fixture
def client():
    return main.app.test_client()


def _seed_campaign(now: datetime):
    main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].delete_many({"_id": CAMPAIGN_ID})
    main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].insert_one(
        {
            "_id": CAMPAIGN_ID,
            "campaign_id": CAMPAIGN_ID,
            "enabled": True,
            "starts_at": now - timedelta(days=1),
            "ends_at": now + timedelta(days=1),
            "participants": [
                {"entry_id": "seed-001", "display_name": "A***n", "qualified_count": 7, "visible": True},
                {"entry_id": "seed-002", "display_name": "J***8", "qualified_count": 2, "visible": True},
            ],
            "created_at": now,
            "updated_at": now,
        }
    )


def _seed_genuine_referrer(now: datetime, referrer_id: int, qualified_count: int, first_name: str):
    main.db.users.delete_many({"user_id": referrer_id})
    main.db.qualified_events.delete_many({"referrer_id": referrer_id})
    main.db.users.insert_one({"user_id": referrer_id, "username": None, "first_name": first_name})
    for i in range(qualified_count):
        main.db.qualified_events.insert_one(
            {"invitee_id": referrer_id * 100000 + i, "referrer_id": referrer_id, "qualified_at": now}
        )


def _cleanup():
    main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].delete_many({"_id": CAMPAIGN_ID})
    main.db.users.delete_many({"user_id": 909090})
    main.db.qualified_events.delete_many({"referrer_id": 909090})


def test_affiliate_endpoint_and_dedicated_campaign_endpoint_agree(client):
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    _seed_genuine_referrer(now, 909090, 3, "GenuineFan")
    try:
        affiliate_resp = client.get(f"/api/affiliate/leaderboard?window=month&campaign_id={CAMPAIGN_ID}")
        assert affiliate_resp.status_code == 200
        affiliate_activity = affiliate_resp.get_json()["campaign_activity"]

        dedicated_resp = client.get(f"/api/campaign/{CAMPAIGN_ID}/activity")
        assert dedicated_resp.status_code == 200
        dedicated_activity = dedicated_resp.get_json()

        assert affiliate_activity["qualified_total"] == dedicated_activity["qualified_total"]
        assert affiliate_activity["leaderboard"] == dedicated_activity["leaderboard"]
        assert affiliate_activity["qualified_total"] == 7 + 2 + 3
    finally:
        _cleanup()


def test_active_campaign_and_announcement_preview_agree_with_no_query_param(client, monkeypatch):
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    _seed_genuine_referrer(now, 909090, 3, "GenuineFan")
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", CAMPAIGN_ID)
    try:
        # Affiliate page: no campaign_id query param at all -- must still
        # auto-resolve via CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID.
        affiliate_resp = client.get("/api/affiliate/leaderboard?window=month")
        assert affiliate_resp.status_code == 200
        affiliate_activity = affiliate_resp.get_json()["campaign_activity"]
        assert affiliate_activity["campaign_id"] == CAMPAIGN_ID

        # Money Room calls this exact endpoint, with no campaign_id either.
        active_resp = client.get("/api/campaign/active/activity")
        assert active_resp.status_code == 200
        active_activity = active_resp.get_json()

        with mock.patch.object(main, "require_admin_from_query", return_value=(True, None)):
            preview_resp = client.get("/api/admin/campaign/announcement-preview")
        assert preview_resp.status_code == 200
        preview_activity = preview_resp.get_json()

        assert affiliate_activity["qualified_total"] == active_activity["qualified_total"] == preview_activity["qualified_total"]
        assert affiliate_activity["leaderboard"] == active_activity["leaderboard"] == preview_activity["leaderboard"]

        # The announcement text is rendered from that same combined result --
        # every name/count that appears in the leaderboard must appear in it.
        preview_text = preview_activity["preview_text"]
        for row in preview_activity["leaderboard"]:
            assert row["display_name"] in preview_text
            assert str(row["qualified_count"]) in preview_text
    finally:
        _cleanup()


def test_active_campaign_endpoint_reports_no_active_campaign_when_unset(client, monkeypatch):
    monkeypatch.delenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", raising=False)
    resp = client.get("/api/campaign/active/activity")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["state"] == "no_active_campaign"
    assert body["campaign_id"] is None
    assert body["leaderboard"] == []


def test_disabling_campaign_removes_it_from_all_surfaces_immediately(client, monkeypatch):
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", CAMPAIGN_ID)
    try:
        before = client.get("/api/campaign/active/activity").get_json()
        assert before["state"] == "active"

        main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"enabled": False}})

        after_active_endpoint = client.get("/api/campaign/active/activity").get_json()
        after_affiliate = client.get("/api/affiliate/leaderboard?window=month").get_json()["campaign_activity"]
        assert after_active_endpoint["state"] == "genuine_only"
        assert after_affiliate["state"] == "genuine_only"
    finally:
        _cleanup()
