"""GET /api/mission-pool/active — the Mini App "Live Missions" discovery list.

This is the ambient counterpart to mission_pool_ux.mission_view (§view):
that endpoint only ever answers for a campaign already named by a deep link,
so a Mission campaign that goes LIVE in Campaign Centre had no way to be
*discovered* from a normal Mini App open — only admins could see it, in the
"Existing Drops"/Mission landing list. This file covers the new listing
endpoint's eligibility filter, its reuse of the KL/UTC datetime-normalisation
helpers already used everywhere else in this module, its participation-state
mapping, and that it never leaks unsafe fields.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
import mission_pool_ux as ux
from fake_mongo import FakeDb

CAMPAIGN_ID = "live-mission-1"
UID = 900001
OTHER_UID = 900002


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys())
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(mp, "database", database)
    monkeypatch.setattr(ux, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(ux.mission_pool_ux_bp)
    app.register_blueprint(mp.mission_pool_bp)
    return app


def _verified(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _campaign(campaign_id=CAMPAIGN_ID, **overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": campaign_id,
        "name": "Answer & Win",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {
            "mission_type": "multiple_choice",
            "prompt": "Which game?",
            "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "correct_answer": "a",
        },
        "mission_pool": {
            "pool_id": "MISSION-PILOT",
            "pool_type": "voucher_drop",
            "winner_count": 5,
            "allocation_method": "random_qualified",
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "processing_stage": mp.STAGE_PENDING,
            "processing_generation": 0,
        },
    }
    doc.update(overrides)
    return doc


def _insert(fake_db, doc):
    database.db["gc_campaigns"].insert_one(doc)


def _active(client, uid=UID):
    resp = client.get(f"/api/mission-pool/active?init_data=x")
    return resp


# ---------------------------------------------------------------------------
# Eligibility filter (§ "Display eligible missions")
# ---------------------------------------------------------------------------

def test_live_mission_is_returned(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        resp = _active(client)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert len(data["missions"]) == 1
    row = data["missions"][0]
    assert row["campaign_id"] == CAMPAIGN_ID
    assert row["campaign_name"] == "Answer & Win"
    assert row["prompt"] == "Which game?"
    assert row["winner_count"] == 5
    assert row["user_state"] == ux.STATE_LIVE
    assert row["already_submitted"] is False


@pytest.mark.parametrize("overrides", [
    {"status": "draft"},
    {"status": "scheduled"},
    {"status": "paused"},
    {"status": "ended"},
    {"status": "archived"},
], ids=["draft", "scheduled", "paused", "ended", "archived"])
def test_non_live_status_is_hidden(fake_db, overrides):
    _insert(fake_db, _campaign(**overrides))
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


def test_upcoming_mission_is_hidden(fake_db):
    """A mission whose starts_at is still in the future must not leak into
    Live Missions, even with status=live (no "Upcoming" product rule exists
    here — see submission_state's own interval convention)."""
    now = datetime.now(timezone.utc)
    _insert(fake_db, _campaign(schedule={"starts_at": now + timedelta(hours=1),
                                          "ends_at": now + timedelta(hours=2)}))
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


def test_past_end_time_is_hidden_even_if_status_still_says_live(fake_db):
    now = datetime.now(timezone.utc)
    _insert(fake_db, _campaign(schedule={"starts_at": now - timedelta(hours=2),
                                          "ends_at": now - timedelta(minutes=1)}))
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


def test_cancelled_mission_is_hidden(fake_db):
    doc = _campaign()
    doc["mission_pool"]["cancelled"] = True
    _insert(fake_db, doc)
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


def test_deleted_mission_is_hidden(fake_db):
    doc = _campaign(status="deleted")
    _insert(fake_db, doc)
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


def test_standard_drop_campaign_never_appears(fake_db):
    _insert(fake_db, {
        "campaign_id": "std-1", "name": "Standard Drop", "type": "tournament",
        "mechanic": "standard_drop", "status": "live",
        "schedule": {"starts_at": datetime.now(timezone.utc) - timedelta(hours=1),
                     "ends_at": datetime.now(timezone.utc) + timedelta(hours=1)},
    })
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


def test_disabled_kill_switch_returns_empty_list_not_an_error(fake_db, monkeypatch):
    _insert(fake_db, _campaign())
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: False)
    with _app().test_client() as client, _verified(UID):
        resp = _active(client)
    assert resp.status_code == 200
    assert resp.get_json()["missions"] == []


def test_active_list_requires_authentication(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, patch(
        "vouchers.verify_telegram_init_data", return_value=(False, None, "bad_signature")
    ):
        resp = client.get("/api/mission-pool/active?init_data=forged")
    assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Naive vs aware datetimes (§ "Use the existing KL/UTC normalisation helpers")
# ---------------------------------------------------------------------------

def test_naive_mongo_datetime_is_treated_as_utc(fake_db):
    """PyMongo returns naive datetimes on read even when written as
    UTC-aware; mission_pool._as_utc (reused via submission_state) is what
    makes this listing endpoint compare correctly instead of silently
    excluding every live mission."""
    now_naive = datetime.utcnow()
    doc = _campaign(schedule={
        "starts_at": now_naive - timedelta(hours=1),
        "ends_at": now_naive + timedelta(hours=1),
    })
    _insert(fake_db, doc)
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert len(data["missions"]) == 1


def test_aware_utc_datetime_also_works(fake_db):
    now = datetime.now(timezone.utc)
    doc = _campaign(schedule={"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)})
    _insert(fake_db, doc)
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert len(data["missions"]) == 1


# ---------------------------------------------------------------------------
# Participation state -> CTA mapping (§ "User states and CTA")
# ---------------------------------------------------------------------------

def test_not_started_maps_to_join(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        row = _active(client).get_json()["missions"][0]
    assert row["user_state"] == ux.STATE_LIVE
    assert row["already_submitted"] is False


def test_submitted_maps_to_disabled_submitted(fake_db):
    _insert(fake_db, _campaign())
    now = datetime.now(timezone.utc)
    database.db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID, "status": mp.ENTRY_STATUS_SUBMITTED,
        "answer": "a", "answer_normalized": "a", "is_correct": True,
        "submitted_at": now, "created_at": now, "updated_at": now,
    })
    with _app().test_client() as client, _verified(UID):
        row = _active(client).get_json()["missions"][0]
    assert row["user_state"] == ux.STATE_SUBMITTED
    assert row["already_submitted"] is True


def test_another_users_submission_never_leaks_into_my_state(fake_db):
    """Only the authenticated caller's own participation state is returned —
    never another user's submission."""
    _insert(fake_db, _campaign())
    now = datetime.now(timezone.utc)
    database.db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": OTHER_UID, "status": mp.ENTRY_STATUS_SUBMITTED,
        "answer": "a", "answer_normalized": "a", "is_correct": True,
        "submitted_at": now, "created_at": now, "updated_at": now,
    })
    with _app().test_client() as client, _verified(UID):
        row = _active(client).get_json()["missions"][0]
    assert row["user_state"] == ux.STATE_LIVE
    assert row["already_submitted"] is False


# ---------------------------------------------------------------------------
# Ended-while-listed / winner rewards never duplicate the Live Missions card
# (§ "Ended -> remove from Live Missions", § "Avoid duplicate reward/drop UI")
# ---------------------------------------------------------------------------

def test_completed_winner_campaign_is_removed_from_live_list(fake_db):
    """Once a mission finishes processing it is no longer `status=live`, so
    it drops out of Live Missions entirely rather than rendering a second,
    duplicate "reward ready" card alongside the compact Campaign Rewards
    row that already owns that surface."""
    doc = _campaign(status="ended")
    doc["mission_pool"]["processing_stage"] = mp.STAGE_COMPLETED
    _insert(fake_db, doc)
    now = datetime.now(timezone.utc)
    database.db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID, "status": mp.ENTRY_STATUS_WINNER,
        "answer": "a", "answer_normalized": "a", "is_correct": True,
        "submitted_at": now, "created_at": now, "updated_at": now,
    })
    with _app().test_client() as client, _verified(UID):
        data = _active(client).get_json()
    assert data["missions"] == []


# ---------------------------------------------------------------------------
# No unsafe fields leak (§ "Data/API")
# ---------------------------------------------------------------------------

def test_response_never_includes_unsafe_fields(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        row = _active(client).get_json()["missions"][0]
    forbidden = {"correct_answer", "pool_id", "eligibility_policy", "allocation_method",
                 "identity_key", "voucher_code", "admin", "processing_owner"}
    assert forbidden.isdisjoint(row.keys())
    # And no nested config that could carry them either.
    assert "mission_config" not in row
    assert "mission_pool" not in row


# ---------------------------------------------------------------------------
# Idempotent submission (§ "Abuse and correctness")
# ---------------------------------------------------------------------------

def test_repeated_submission_does_not_duplicate_the_entry_or_the_listing(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        r1 = client.post(f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=x", json={"answer": "a"})
        r2 = client.post(f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=x", json={"answer": "a"})
        listing = _active(client).get_json()
    assert r1.get_json()["status"] == "ok"
    assert r2.get_json()["state"] == "already_submitted"
    assert database.db[mp.ENTRIES_COLLECTION].count_documents(
        {"campaign_id": CAMPAIGN_ID, "telegram_user_id": UID}
    ) == 1
    assert len(listing["missions"]) == 1
    assert listing["missions"][0]["already_submitted"] is True


# ---------------------------------------------------------------------------
# Mission ending between list load and CTA tap (§ "Abuse and correctness")
# ---------------------------------------------------------------------------

def test_mission_ending_before_cta_tap_is_rejected_safely_by_view_and_submit(fake_db):
    """The Live Missions card's "Join Mission" action re-fetches /view (and
    ultimately /submit) rather than trusting the list snapshot, so a mission
    that ends in the gap between page load and the tap resolves to the
    server-authoritative closed/ended state instead of a stale form."""
    doc = _campaign()
    _insert(fake_db, doc)
    with _app().test_client() as client, _verified(UID):
        listing = _active(client).get_json()
        assert len(listing["missions"]) == 1

        # Campaign ends between the list load and the CTA tap.
        database.db["gc_campaigns"].update_one(
            {"campaign_id": CAMPAIGN_ID},
            {"$set": {"status": "ended"}},
        )

        view = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x").get_json()
        assert view["user_state"] in (ux.STATE_ENDED, ux.STATE_CLOSED_PROCESSING)
        assert view["submissions_open"] is False

        submit = client.post(f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=x", json={"answer": "a"})
        assert submit.status_code == 409
        assert submit.get_json()["code"] == "campaign_closed"
