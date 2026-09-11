"""P0.16 — operational/admin usability follow-up to the P0.13 audit.

Backend-only coverage for:
  A. list_campaigns() reports total/truncated alongside the still-200-row-
     capped campaigns array, computed with count_documents() against the
     SAME filter used for the list query (including the "deleted" exclusion
     and any ?status= filter).
  B. get_campaign_route() now also exposes mission_active_rewards for a
     Mission Pool campaign (same mission_pool.active_reward_counts helper
     list_campaigns already uses), so Campaign Detail's own overflow menu
     can decide "End Rewards" with the exact same legality the Campaigns
     list uses — never a looser/different rule for a single-campaign fetch.
  C. list_campaigns() no longer calls get_provider() once per row — a
     single bulk `$in` query against gc_providers builds a
     {provider_id: provider_doc} map reused across every row, preserving
     effective_visibility/missing-provider/inactive-provider behavior
     exactly.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import database
import campaign_centre as cc
import campaign_providers as cp
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "gc_campaigns": [("campaign_id",)],
        "gc_providers": [("provider_id",)],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(cp, "database", database)
    return fdb


def _admin_app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    return app


def _admin_patch():
    return patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None))


def _campaign(**overrides):
    base = {
        "campaign_id": "c-1",
        "name": "Campaign",
        "type": "tournament",
        "status": "live",
        "priority": 100,
        "created_at": datetime.now(timezone.utc),
        "schedule": {"starts_at": datetime.now(timezone.utc) - timedelta(hours=1), "ends_at": None},
        "destination": {"provider_id": "prov-1", "open_mode": "telegram_web_app", "path": "/x", "ready": True},
    }
    base.update(overrides)
    return base


def _provider(**overrides):
    base = {"provider_id": "prov-1", "active": True, "type": "tournament"}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# A — total/truncated
# ---------------------------------------------------------------------------

def test_total_and_truncated_absent_when_under_the_cap(fake_db):
    for i in range(5):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-{i}"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    body = resp.get_json()
    assert body["total"] == 5
    assert body["truncated"] is False
    assert len(body["campaigns"]) == 5


def test_total_199_not_truncated(fake_db):
    for i in range(199):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-{i}"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    body = resp.get_json()
    assert body["total"] == 199
    assert body["truncated"] is False
    assert len(body["campaigns"]) == 199


def test_total_exactly_200_not_truncated(fake_db):
    for i in range(200):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-{i}"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    body = resp.get_json()
    assert body["total"] == 200
    assert body["truncated"] is False
    assert len(body["campaigns"]) == 200


def test_total_237_reports_truncated_with_200_rows_returned(fake_db):
    for i in range(237):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-{i:03d}"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    body = resp.get_json()
    assert body["total"] == 237
    assert body["truncated"] is True
    assert len(body["campaigns"]) == 200


def test_deleted_campaigns_excluded_from_total(fake_db):
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-live"))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-deleted", status="deleted"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    body = resp.get_json()
    assert body["total"] == 1
    assert [c["campaign_id"] for c in body["campaigns"]] == ["c-live"]


def test_status_filtered_total_reflects_the_filtered_status_only(fake_db):
    for i in range(3):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-live-{i}", status="live"))
    for i in range(2):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-draft-{i}", status="draft"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns?status=live")
    body = resp.get_json()
    assert body["total"] == 3
    assert body["truncated"] is False
    assert len(body["campaigns"]) == 3

    with _admin_patch():
        resp2 = _admin_app().test_client().get("/api/admin/gc-campaigns?status=draft")
    body2 = resp2.get_json()
    assert body2["total"] == 2


def test_status_filtered_truncation_past_the_200_cap(fake_db):
    for i in range(210):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-live-{i:03d}", status="live"))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-draft", status="draft"))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns?status=live")
    body = resp.get_json()
    assert body["total"] == 210
    assert body["truncated"] is True
    assert len(body["campaigns"]) == 200


def test_ordering_unchanged_by_the_total_count_addition(fake_db):
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-low-priority", priority=1))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-high-priority", priority=999))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    ids = [c["campaign_id"] for c in resp.get_json()["campaigns"]]
    assert ids == ["c-high-priority", "c-low-priority"]


# ---------------------------------------------------------------------------
# B — mission_active_rewards parity on the single-campaign GET
# ---------------------------------------------------------------------------

def _mission_campaign(**overrides):
    base = _campaign(
        campaign_id="mission-1", type="mission_pool", mechanic="mission_pool", status="live",
        mission_config={"mission_type": "keyword", "prompt": "?", "correct_answer": "a"},
        mission_pool={"pool_id": "MP-1", "winner_count": 1, "cancelled": False},
        destination={"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": True},
    )
    base.update(overrides)
    return base


def test_detail_get_exposes_mission_active_rewards_same_as_list(fake_db):
    fake_db["gc_campaigns"].insert_one(_mission_campaign())
    fake_db["campaign_rewards"].insert_one({
        "reward_id": "rw-1", "campaign_id": "mission-1", "category": "mission_pool",
        "status": "assigned", "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
    })
    with _admin_patch():
        list_resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
        detail_resp = _admin_app().test_client().get("/api/admin/gc-campaigns/mission-1")
    list_card = list_resp.get_json()["campaigns"][0]
    detail_card = detail_resp.get_json()["campaign"]
    assert list_card["mission_active_rewards"] == 1
    assert detail_card["mission_active_rewards"] == list_card["mission_active_rewards"]


def test_detail_get_omits_mission_active_rewards_for_non_mission_campaigns(fake_db):
    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign())
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns/c-1")
    assert "mission_active_rewards" not in resp.get_json()["campaign"]


# ---------------------------------------------------------------------------
# C — provider bulk lookup (no per-row get_provider() fan-out)
# ---------------------------------------------------------------------------

def test_list_resolves_providers_in_one_bulk_query_not_per_row(fake_db, monkeypatch):
    fake_db["gc_providers"].insert_one(_provider())
    for i in range(5):
        fake_db["gc_campaigns"].insert_one(_campaign(campaign_id=f"c-{i}"))

    calls = []
    real_find = fake_db["gc_providers"].find
    monkeypatch.setattr(fake_db["gc_providers"], "find",
                         lambda *a, **kw: (calls.append((a, kw)) or real_find(*a, **kw)))

    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    assert resp.status_code == 200
    assert len(calls) == 1, "must resolve every row's provider in a single bulk query, not one per row"
    for card in resp.get_json()["campaigns"]:
        assert card["effective_visibility"]["publicly_visible"] is True


def test_list_provider_lookup_covers_multiple_distinct_providers_in_one_query(fake_db, monkeypatch):
    fake_db["gc_providers"].insert_one(_provider(provider_id="prov-a"))
    fake_db["gc_providers"].insert_one(_provider(provider_id="prov-b"))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-a", destination={"provider_id": "prov-a", "open_mode": "telegram_web_app", "path": "/x", "ready": True}))
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-b", destination={"provider_id": "prov-b", "open_mode": "telegram_web_app", "path": "/x", "ready": True}))

    calls = []
    real_find = fake_db["gc_providers"].find
    monkeypatch.setattr(fake_db["gc_providers"], "find",
                         lambda *a, **kw: (calls.append((a, kw)) or real_find(*a, **kw)))

    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    assert len(calls) == 1
    cards = {c["campaign_id"]: c for c in resp.get_json()["campaigns"]}
    assert cards["c-a"]["effective_visibility"]["publicly_visible"] is True
    assert cards["c-b"]["effective_visibility"]["publicly_visible"] is True


def test_list_missing_provider_reports_the_same_reason_as_before(fake_db):
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-missing-provider",
        destination={"provider_id": "does-not-exist", "open_mode": "telegram_web_app", "path": "/x", "ready": True}))
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    card = resp.get_json()["campaigns"][0]
    assert card["effective_visibility"]["publicly_visible"] is False
    assert "linked provider does not exist" in card["effective_visibility"]["reasons"]


def test_list_inactive_provider_reports_the_same_reason_as_before(fake_db):
    fake_db["gc_providers"].insert_one(_provider(active=False))
    fake_db["gc_campaigns"].insert_one(_campaign())
    with _admin_patch():
        resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
    card = resp.get_json()["campaigns"][0]
    assert card["effective_visibility"]["publicly_visible"] is False
    assert "linked provider is inactive" in card["effective_visibility"]["reasons"]


def test_list_provider_result_parity_with_previous_per_row_lookup(fake_db):
    """Bulk-map result must match what the old get_provider()-per-row loop
    would have produced: campaigns with no destination.provider_id at all
    (e.g. registration-enabled/mission types) must never spuriously match
    some other row's provider, and every row's effective_visibility must
    come out exactly as get_campaign_route (still single-lookup) computes
    it for the same document."""
    fake_db["gc_providers"].insert_one(_provider())
    fake_db["gc_campaigns"].insert_one(_campaign(campaign_id="c-with-provider"))
    fake_db["gc_campaigns"].insert_one(_campaign(
        campaign_id="c-no-destination", type="mission_pool",
        mission_config={"mission_type": "keyword", "prompt": "?", "correct_answer": "a"},
        mission_pool={"pool_id": "MP-1", "winner_count": 1},
        destination={"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": True},
    ))

    with _admin_patch():
        list_resp = _admin_app().test_client().get("/api/admin/gc-campaigns")
        detail_a = _admin_app().test_client().get("/api/admin/gc-campaigns/c-with-provider")
        detail_b = _admin_app().test_client().get("/api/admin/gc-campaigns/c-no-destination")

    list_cards = {c["campaign_id"]: c for c in list_resp.get_json()["campaigns"]}
    assert list_cards["c-with-provider"]["effective_visibility"] == detail_a.get_json()["campaign"]["effective_visibility"]
    assert list_cards["c-no-destination"]["effective_visibility"] == detail_b.get_json()["campaign"]["effective_visibility"]
