"""Tests for the permanent Campaign Centre delete endpoint:

    DELETE /api/admin/campaign-centre/campaigns/<campaign_id>

Uses the same standalone-blueprint + mongomock pattern as
test_campaign_display_admin.py (a lightweight Flask app registering only
campaign_centre_bp, vouchers.require_admin patched for auth) so the status
check + delete run through mongomock's real find_one_and_delete, exercising
the same atomicity the production code relies on.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import mongomock
import pytest
from flask import Flask

import database
import campaign_centre as cc

CAMPAIGN_ID = "summer-lucky-draw-2026"


@pytest.fixture
def fake_db(monkeypatch):
    fdb = mongomock.MongoClient().db
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    return app


@pytest.fixture
def client():
    return _app().test_client()


def _mock_admin(uid: int = 42):
    return patch("vouchers.require_admin", return_value=({"id": uid, "usernameLower": "admin"}, None))


def _mock_unauthenticated():
    from flask import jsonify

    def _err():
        return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

    return patch("vouchers.require_admin", side_effect=_err)


def _seed_campaign(fake_db, **overrides):
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Summer Lucky Draw",
        "type": "external_website",
        "status": "draft",
        "priority": 100,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
    }
    doc.update(overrides)
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _delete(client, campaign_id=CAMPAIGN_ID):
    return client.delete(f"/api/admin/campaign-centre/campaigns/{campaign_id}")


# ---------------------------------------------------------------------------
# Deletable statuses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["draft", "archived", "ended"])
def test_deletable_status_can_be_deleted(fake_db, client, status):
    _seed_campaign(fake_db, status=status)
    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["campaign_id"] == CAMPAIGN_ID
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID}) is None


# ---------------------------------------------------------------------------
# Non-deletable statuses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["live", "paused"])
def test_live_or_paused_campaign_returns_409_and_is_not_deleted(fake_db, client, status):
    _seed_campaign(fake_db, status=status)
    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["status"] == "error"
    assert body["code"] == "invalid_status_for_deletion"
    assert body["campaign_status"] == status
    # campaign must remain intact, untouched
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert doc is not None
    assert doc["status"] == status


def test_unknown_campaign_returns_404(fake_db, client):
    with _mock_admin():
        resp = _delete(client, campaign_id="does-not-exist")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"


def test_unauthenticated_request_is_rejected(fake_db, client):
    _seed_campaign(fake_db, status="draft")
    with _mock_unauthenticated():
        resp = _delete(client)
    assert resp.status_code == 401
    # untouched
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID}) is not None


# ---------------------------------------------------------------------------
# Owned config vs. historical/traceability records
# ---------------------------------------------------------------------------

def test_owned_registration_state_is_removed(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    fake_db["campaign_registration_state"].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": 111, "dismissed": True,
    })
    other_campaign_state = {"campaign_id": "other-campaign", "telegram_user_id": 222, "dismissed": True}
    fake_db["campaign_registration_state"].insert_one(dict(other_campaign_state))

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200

    assert fake_db["campaign_registration_state"].count_documents({"campaign_id": CAMPAIGN_ID}) == 0
    # a different campaign's state must never be touched by this delete
    remaining = fake_db["campaign_registration_state"].find_one({"campaign_id": "other-campaign"})
    assert remaining is not None
    assert remaining["telegram_user_id"] == 222


def test_historical_records_are_preserved(fake_db, client):
    _seed_campaign(fake_db, status="ended")
    fake_db["campaign_registrations"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111})
    fake_db["mission_entries"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111, "is_correct": True})
    fake_db["mission_identity_claims"].insert_one({"campaign_id": CAMPAIGN_ID, "identity_hash": "abc"})
    fake_db["campaign_rewards"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111, "status": "assigned"})
    fake_db["tournament_results"].insert_one({"campaign_id": CAMPAIGN_ID, "submission_id": "s1"})

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200

    assert fake_db["campaign_registrations"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["mission_entries"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["mission_identity_claims"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["campaign_rewards"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["tournament_results"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1


def test_audit_log_written_and_survives_deletion(fake_db, client):
    _seed_campaign(fake_db, status="draft", name="Summer Lucky Draw")
    with _mock_admin(uid=99):
        resp = _delete(client)
    assert resp.status_code == 200

    audit = fake_db["campaign_admin_audit_log"].find_one({"action": "campaign_deleted", "entity_id": CAMPAIGN_ID})
    assert audit is not None
    assert audit["details"]["title"] == "Summer Lucky Draw"
    assert audit["details"]["previous_status"] == "draft"
    assert audit["details"]["snapshot"]["campaign_id"] == CAMPAIGN_ID
    assert audit["admin"] == "admin"
    assert audit["at"] is not None

    event = fake_db["campaign_events"].find_one({"event_type": "campaign_deleted", "campaign_id": CAMPAIGN_ID})
    assert event is not None


# ---------------------------------------------------------------------------
# campaign_id reuse after deletion (preserved history must never be
# silently inherited by a new campaign reusing the same id)
# ---------------------------------------------------------------------------

def test_create_campaign_rejects_a_previously_deleted_campaign_id(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": CAMPAIGN_ID, "name": "Reused id", "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
        })
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_id_previously_deleted"
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID}) is None


def test_duplicate_campaign_rejects_a_previously_deleted_target_id(fake_db, client):
    _seed_campaign(fake_db, campaign_id="source-campaign", status="draft")
    _seed_campaign(fake_db, status="archived")  # CAMPAIGN_ID, will become the delete target
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post(
            "/api/admin/gc-campaigns/source-campaign/duplicate",
            json={"campaign_id": CAMPAIGN_ID},
        )
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_id_previously_deleted"


def test_deleted_campaign_id_does_not_leak_into_a_fresh_campaign_with_the_same_id(fake_db, client):
    """Regression guard for the underlying risk the reuse-block exists for:
    even if the id were allowed to be reused, a stale registration row under
    that campaign_id must never be reachable by a new campaign. This asserts
    the reuse itself is blocked (the actual fix), rather than depending on
    every downstream consumer's own campaign_id scoping being correct."""
    _seed_campaign(fake_db, status="ended")
    fake_db["campaign_registrations"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111})
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": CAMPAIGN_ID, "name": "Reused id", "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
        })
    assert resp.status_code == 409
    # the old registration is still there (preserved for traceability) but
    # no new campaign document was ever allowed to claim its campaign_id
    assert fake_db["campaign_registrations"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID}) is None


# ---------------------------------------------------------------------------
# Concurrency / atomicity
# ---------------------------------------------------------------------------

def test_publish_loses_race_with_delete_reports_not_found_not_false_success(fake_db, client):
    """If publish/pause/archive has already read the campaign as eligible
    but delete_campaign removes it before the status-transition's update_one
    lands, that update must report the miss (not_found) instead of a false
    'ok' for a status change that never landed on any document."""
    _seed_campaign(fake_db, status="draft")
    with _mock_admin():
        # Simulate the race directly: the campaign is gone by the time the
        # transition's update_one runs, exactly as if delete_campaign had
        # interleaved between _transition's read and its write.
        assert _delete(client).status_code == 200
        resp = client.post(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}/publish")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"
    # no audit entry for a publish that never actually happened
    assert fake_db["campaign_admin_audit_log"].find_one({"action": "campaign_published"}) is None


def test_concurrent_publish_during_delete_cannot_both_succeed(fake_db, client):
    """The status check and the delete happen in one atomic
    find_one_and_delete filtered on status — a campaign that flips to
    'live' between an admin's status read and their delete click must
    never be silently deleted anyway. Simulate the race by flipping the
    campaign to 'live' right before calling delete."""
    _seed_campaign(fake_db, status="draft")
    fake_db["gc_campaigns"].update_one({"campaign_id": CAMPAIGN_ID}, {"$set": {"status": "live"}})

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 409
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "live"


def test_double_delete_second_call_returns_404(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        first = _delete(client)
        second = _delete(client)
    assert first.status_code == 200
    assert second.status_code == 404
    assert second.get_json()["code"] == "not_found"
