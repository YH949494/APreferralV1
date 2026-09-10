"""P0.3 regression: editing an existing campaign must never change its
campaign_id, even if a client sends one in the request body.

The admin dashboard's Create Campaign form (P0.3) now derives campaign_id
from the campaign name client-side and only applies it on creation — this
file exists to pin the backend contract that change relies on: PUT
/api/admin/gc-campaigns/<campaign_id> takes the id from the
URL only, never from the body, so a stray campaign_id in an edit payload
(or a future edit UI reusing this form's fields) can never rewrite a
campaign's identity or collide with another campaign's row.

Uses the same standalone-blueprint + mongomock pattern as
test_campaign_centre_delete.py.
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


def test_update_ignores_a_campaign_id_in_the_body(fake_db, client):
    _seed_campaign(fake_db)
    with _mock_admin():
        resp = client.put(
            f"/api/admin/gc-campaigns/{CAMPAIGN_ID}",
            json={"name": "Summer Lucky Draw", "campaign_id": "a-different-id"},
        )
    assert resp.status_code == 200, resp.get_json()
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert doc is not None, "original campaign_id must still resolve"
    assert doc["campaign_id"] == CAMPAIGN_ID
    assert fake_db["gc_campaigns"].find_one({"campaign_id": "a-different-id"}) is None, \
        "a campaign_id sent in the edit body must never create/rename a row"


def test_name_change_does_not_touch_campaign_id(fake_db, client):
    _seed_campaign(fake_db)
    with _mock_admin():
        resp = client.put(
            f"/api/admin/gc-campaigns/{CAMPAIGN_ID}",
            json={"name": "October Lucky Draw"},
        )
    assert resp.status_code == 200, resp.get_json()
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert doc["name"] == "October Lucky Draw"
    assert doc["campaign_id"] == CAMPAIGN_ID, "renaming a campaign must never regenerate its slug/id"


def test_create_then_edit_roundtrip_preserves_the_created_slug(fake_db, client):
    """End-to-end version of the two tests above: create via the same
    endpoint the dashboard's slug-generated payload hits, then edit the
    name and confirm the id from creation survives untouched."""
    with _mock_admin():
        create_resp = client.post(
            "/api/admin/gc-campaigns",
            json={
                "campaign_id": "october-lucky-draw",
                "name": "October Lucky Draw",
                "type": "external_website",
                "schedule": {"starts_at": "2026-10-01T00:00:00Z", "ends_at": "2026-10-31T00:00:00Z"},
                "telegram": {"channel_username": ""},
                "destination": {"provider_id": "", "path": "", "open_mode": "telegram_web_app", "ready": False},
            },
        )
        assert create_resp.status_code == 201, create_resp.get_json()

        edit_resp = client.put(
            "/api/admin/gc-campaigns/october-lucky-draw",
            json={"name": "October Lucky Draw — Extended"},
        )
    assert edit_resp.status_code == 200, edit_resp.get_json()
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": "october-lucky-draw"})
    assert doc["campaign_id"] == "october-lucky-draw"
    assert doc["name"] == "October Lucky Draw — Extended"
