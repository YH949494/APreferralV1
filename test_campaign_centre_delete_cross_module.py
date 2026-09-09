"""Cross-module proof that a deleted (tombstoned) Campaign Centre campaign
is inert everywhere, not just behind its own admin routes.

campaign_centre.get_campaign() is the single lookup campaign_registration.py
and mission_pool.py both go through (see campaign_centre.py's docstring on
get_campaign and delete_campaign) — it returns None for a status="deleted"
tombstone, so registration and mission-submission reject it the same way
they'd reject any unknown campaign_id. This file proves that end to end
through the real HTTP routes of those two modules, not just by reading the
call graph.

Uses the same FakeDb + Flask test-client harness as test_campaign_registration.py
and test_mission_pool.py (both already establish this pattern for their own
modules); this file combines campaign_centre_bp with each of them in turn.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import campaign_registration as cr
import database
import mission_pool as mp
from fake_mongo import FakeDb

CAMPAIGN_ID = "cross-module-delete-check"
UID = 555001


def _mock_admin(uid: int = 42):
    return patch("vouchers.require_admin", return_value=({"id": uid, "usernameLower": "admin"}, None))


def _verified(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _delete(client, campaign_id=CAMPAIGN_ID):
    with _mock_admin():
        return client.delete(f"/api/admin/campaign-centre/campaigns/{campaign_id}")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def _unique_keys_registration():
    return {
        "gc_campaigns": [("campaign_id",)],
        cr.REGISTRATIONS_COLLECTION: [("campaign_id", "telegram_user_id")],
        cr.STATE_COLLECTION: [("campaign_id", "telegram_user_id")],
    }


@pytest.fixture
def registration_fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys_registration())
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cr, "database", database)
    monkeypatch.setattr(cc, "database", database)
    return fdb


def _registration_app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    app.register_blueprint(cr.campaign_registration_bp)
    return app


def _registration_campaign(**overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Cross Module Registration Campaign",
        "type": "external_website",
        "status": "draft",  # deletable; flipped to "live" below where needed
        "priority": 100,
        "created_at": now,
        "updated_at": now,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(days=30)},
        "telegram": {"require_identity": True, "require_subscription": False, "channel_username": "advantplayofficial"},
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
        "registration": cr.default_registration_config(),
    }
    doc["registration"]["enabled"] = True
    doc.update(overrides)
    return doc


def _valid_registration_payload(**overrides):
    body = {
        "full_name": "Player One",
        "contact_number": "+60 12-345 6789",
        "country_region": "Malaysia",
        "delivery_address": "1 Main St",
    }
    body.update(overrides)
    return body


def test_deleted_campaign_rejects_registration_attempt(registration_fake_db):
    fdb = registration_fake_db
    fdb["gc_campaigns"].insert_one(_registration_campaign(status="draft"))

    with _registration_app().test_client() as client:
        assert _delete(client).status_code == 200

        # sanity: registration would have succeeded before deletion
        assert fdb["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "deleted"

        with _verified(UID):
            resp = client.post(
                f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                json=_valid_registration_payload(),
            )

    assert resp.status_code == 404
    assert resp.get_json()["code"] == "registration_unavailable"
    assert fdb[cr.REGISTRATIONS_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 0


def test_live_campaign_registers_then_deletion_blocks_further_registrations(registration_fake_db):
    """Delete requires an already-deletable status (draft/archived/ended);
    this proves the full lifecycle — a campaign that was live and taking
    registrations, once archived and deleted, stops accepting them, while
    the registration it already collected survives as history."""
    fdb = registration_fake_db
    fdb["gc_campaigns"].insert_one(_registration_campaign(status="live"))

    with _registration_app().test_client() as client:
        with _verified(UID):
            first = client.post(
                f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                json=_valid_registration_payload(),
            )
        assert first.status_code == 201
        assert fdb[cr.REGISTRATIONS_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 1

        with _mock_admin():
            assert client.post(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}/archive").status_code == 200
        assert _delete(client).status_code == 200

        with _verified(555002):
            second = client.post(
                f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                json=_valid_registration_payload(),
            )

    assert second.status_code == 404
    assert second.get_json()["code"] == "registration_unavailable"
    # the first, pre-deletion registration is preserved for traceability
    assert fdb[cr.REGISTRATIONS_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 1


# ---------------------------------------------------------------------------
# Mission Pool submission
# ---------------------------------------------------------------------------

def _unique_keys_mission():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
    }


@pytest.fixture
def mission_fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys_mission())
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(mp, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _mission_app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    app.register_blueprint(mp.mission_pool_bp)
    return app


def _mission_campaign(**overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Cross Module Mission Campaign",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": "draft",  # deletable; flipped to "live" below where needed
        "priority": 100,
        "created_at": now,
        "updated_at": now,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {
            "mission_type": "multiple_choice",
            "prompt": "Which game?",
            "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "correct_answer": "a",
        },
        "mission_pool": {
            "pool_id": "MISSION-CROSS-MODULE",
            "pool_type": "voucher_drop",
            "winner_count": 3,
            "allocation_method": "random_qualified",
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "processing_stage": mp.STAGE_PENDING,
            "processing_generation": 0,
        },
    }
    doc.update(overrides)
    return doc


def _submit(client, uid=UID, answer="a", campaign_id=CAMPAIGN_ID):
    with _verified(uid):
        return client.post(
            f"/api/mission-pool/{campaign_id}/submit?init_data=stub",
            json={"answer": answer},
        )


def test_deleted_mission_campaign_rejects_submission(mission_fake_db):
    fdb = mission_fake_db
    fdb["gc_campaigns"].insert_one(_mission_campaign(status="draft"))

    with _mission_app().test_client() as client:
        assert _delete(client).status_code == 200
        assert fdb["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "deleted"

        resp = _submit(client)

    assert resp.status_code == 404
    assert resp.get_json()["code"] == "campaign_not_found"
    assert fdb[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 0


def test_live_mission_campaign_accepts_entry_then_deletion_blocks_further_entries(mission_fake_db):
    """Same full-lifecycle shape as the registration test above: a mission
    campaign that was live and collecting entries, once ended and deleted,
    stops accepting submissions, while the entry it already recorded is
    preserved as history (never touched by delete_campaign's cascade)."""
    fdb = mission_fake_db
    fdb["gc_campaigns"].insert_one(_mission_campaign(status="live"))

    with _mission_app().test_client() as client:
        first = _submit(client, uid=UID)
        assert first.status_code == 200
        assert first.get_json()["state"] == "submitted"
        assert fdb[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 1

        with _mock_admin():
            assert client.post(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}/archive").status_code == 200
        assert _delete(client).status_code == 200

        second = _submit(client, uid=555003)

    assert second.status_code == 404
    assert second.get_json()["code"] == "campaign_not_found"
    # the pre-deletion entry is preserved for traceability
    assert fdb[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
