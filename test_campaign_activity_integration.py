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


def test_actual_monthly_leaderboard_uses_monthly_snapshot_not_campaign_window(client, monkeypatch):
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", CAMPAIGN_ID)
    snapshot = {"affiliate_leaderboard_month": [
        {"referrer_id": 909090, "qualified_month": 4, "joins_month": 10,
         "conversion_month": 0.4, "rank": 1}], "generated_at": now, "month_key": "202610"}
    monkeypatch.setattr(main, "compute_affiliate_monthly_kpis_live", lambda db: snapshot)
    try:
        body = client.get("/api/affiliate/leaderboard?window=month").get_json()
        assert [row["qualified_month"] for row in body["leaderboard"]] == [7, 4, 2]
        genuine = next(row for row in body["leaderboard"] if row.get("referrer_id") == 909090)
        assert (genuine["joins_month"], genuine["qualified_month"], genuine["conversion_month"]) == (10, 4, 0.4)
        assert snapshot["affiliate_leaderboard_month"][0]["rank"] == 1
        main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"enabled": False}})
        restored = client.get("/api/affiliate/leaderboard?window=month").get_json()
        assert len(restored["leaderboard"]) == 1
        assert restored["leaderboard"][0]["qualified_month"] == 4
    finally:
        _cleanup()


def test_actual_xp_ref_endpoint_merges_manual_scores_without_cache_or_user_changes(client, monkeypatch):
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID},
        {"$set": {"participants.0.xp_count": 900, "participants.0.ref_count": 25}})
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", CAMPAIGN_ID)
    monkeypatch.setattr(main, "get_app_setting", lambda *args, **kwargs: True)
    monkeypatch.setattr(main, "_get_user_snapshot", lambda uid: ({"weekly_xp": 100, "weekly_referrals": 4}, None, None))
    monkeypatch.setattr(main, "format_username", lambda user, *args: user.get("username"))
    cache = {"checkin": [{"user_id": 42, "username": "A***n", "weekly_xp": 100}],
             "referral": [{"user_id": 42, "username": "A***n", "weekly_referrals": 4}]}
    from copy import deepcopy
    before = deepcopy(cache)
    week = main._week_window_utc()[2].date().isoformat()
    monkeypatch.setattr(main, "LEADERBOARD_CACHE", {f"leaderboard|{week}|15": {"ts": main.time.time(), "payload": cache}})
    try:
        body = client.get("/api/leaderboard").get_json()
        assert [row["xp"] for row in body["leaderboard"]["checkin"]] == [900, 100]
        assert [row["referrals"] for row in body["leaderboard"]["referral"]] == [25, 4]
        assert body["leaderboard"]["checkin"][0]["entry_id"].startswith("manual:")
        assert body["leaderboard"]["checkin"][1]["entry_id"] == "genuine:42"
        assert (body["user"]["xp"], body["user"]["referrals"]) == (100, 4)
        assert cache == before
        main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"enabled": False}})
        restored = client.get("/api/leaderboard").get_json()
        assert [row["xp"] for row in restored["leaderboard"]["checkin"]] == [100]
        assert [row["referrals"] for row in restored["leaderboard"]["referral"]] == [4]
    finally:
        _cleanup()


@pytest.mark.parametrize("admin", [False, True])
@pytest.mark.parametrize("name", ["SameHandle123", "@SameHandle123", "Short", "ab", "A***n", "J***8", "@A***n", "Same***23"])
def test_manual_regular_leaderboard_names_follow_genuine_masking(client, monkeypatch, admin, name):
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID},
        {"$set": {"participants": [{"entry_id": "manual-name", "display_name": name,
                    "joins_count": 30, "qualified_count": 20, "xp_count": 900,
                    "ref_count": 25, "visible": True}]}})
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", CAMPAIGN_ID)
    monkeypatch.setattr(main, "get_app_setting", lambda *args, **kwargs: True)
    # Replace the module reference, not a method on CollectionProxy. Restoring
    # a dynamically resolved bound method on that proxy pins the old database
    # and breaks later suites that switch database._db to fresh fixtures.
    viewer_collection = mock.Mock(wraps=main.users_collection)
    viewer_collection.find_one.return_value = {"is_admin": admin}
    monkeypatch.setattr(main, "users_collection", viewer_collection)
    monkeypatch.setattr(main, "_get_user_snapshot", lambda uid: ({}, None, None))
    monkeypatch.setattr(main, "compute_affiliate_monthly_kpis_live", lambda db: {
        "affiliate_leaderboard_month": [], "generated_at": now, "month_key": "202610"})
    week = main._week_window_utc()[2].date().isoformat()
    cache = {"checkin": [{"user_id": 43, "username": name, "weekly_xp": 100}],
             "referral": [{"user_id": 43, "username": name, "weekly_referrals": 4}]}
    monkeypatch.setattr(main, "LEADERBOARD_CACHE", {f"leaderboard|{week}|15": {"ts": main.time.time(), "payload": cache}})
    try:
        expected = name.lstrip("@") if admin or "***" in name else main.mask_username(name)
        monthly = client.get("/api/affiliate/leaderboard?user_id=42").get_json()["leaderboard"]
        weekly = client.get("/api/leaderboard?user_id=42").get_json()["leaderboard"]
        assert monthly[0]["display_name"] == expected
        for ranking in ("checkin", "referral"):
            assert weekly[ranking][0]["username"] == expected
            # Genuine records still follow their original formatter; only
            # legacy manual display names get the already-masked exemption.
            genuine_expected = name.lstrip("@") if admin else main.mask_username(name)
            assert weekly[ranking][1]["username"] == genuine_expected
            assert weekly[ranking][0]["entry_id"] == "manual:manual-name"
            assert weekly[ranking][1]["entry_id"] == "genuine:43"
        assert main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"][0]["display_name"] == name
        assert cache["checkin"][0]["username"] == name
    finally:
        _cleanup()


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


def test_joins_and_qualified_are_identical_across_every_public_surface(client, monkeypatch):
    """Manual 30/20 row + legacy row (joins unknown) + genuine row: Affiliate
    page, Money Room endpoint and announcement preview must carry identical
    joins/conversion values and an incomplete joins total."""
    now = datetime.now(timezone.utc)
    _seed_campaign(now)
    main.db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one(
        {"_id": CAMPAIGN_ID},
        {"$set": {"participants": [
            {"entry_id": "m-new", "display_name": "N***w", "joins_count": 30, "qualified_count": 20, "visible": True},
            {"entry_id": "m-old", "display_name": "O***d", "qualified_count": 7, "visible": True},
        ]}},
    )
    _seed_genuine_referrer(now, 909090, 3, "GenuineFan")
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", CAMPAIGN_ID)
    try:
        affiliate = client.get("/api/affiliate/leaderboard?window=month").get_json()["campaign_activity"]
        money_room = client.get("/api/campaign/active/activity").get_json()
        with mock.patch.object(main, "require_admin_from_query", return_value=(True, None)):
            preview = client.get("/api/admin/campaign/announcement-preview").get_json()

        assert affiliate["leaderboard"] == money_room["leaderboard"] == preview["leaderboard"]
        for key in ("qualified_total", "joins_total", "joins_total_complete"):
            assert affiliate[key] == money_room[key] == preview[key]
        assert affiliate["joins_total"] is None and affiliate["joins_total_complete"] is False

        rows = {r["display_name"]: r for r in affiliate["leaderboard"]}
        assert (rows["N***w"]["joins_count"], rows["N***w"]["qualified_count"], rows["N***w"]["conversion_rate"]) == (30, 20, 0.6667)
        assert rows["O***d"]["joins_count"] is None and rows["O***d"]["conversion_rate"] is None
        assert "N***w — 30 joins · 20 qualified invites · 66.7% conversion" in preview["preview_text"]
        assert "O***d — — joins · 7 qualified invites · — conversion" in preview["preview_text"]
    finally:
        _cleanup()
