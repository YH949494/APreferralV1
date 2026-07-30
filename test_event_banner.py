"""Tests for event_banner.py: the dynamic image-only Mini App top banner.

Covers admin CRUD validation and the public resolution endpoint's
visibility rules (status/window/region/priority/URL-validity), plus the
best-effort analytics endpoint.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import database
import event_banner as eb
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(eb, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(eb.event_banner_admin_bp)
    app.register_blueprint(eb.event_banner_public_bp)
    return app


def _mock_verified_user(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _mock_admin():
    return patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None))


def _banner_doc(**overrides):
    now = datetime.now(timezone.utc)
    base = {
        "event_id": "weekend_tournament_202608",
        "image_url": "https://cdn.example.com/banner.webp",
        "destination_url": "https://example.com/event",
        "alt_text": "Weekend tournament event",
        "starts_at": now - timedelta(hours=1),
        "ends_at": now + timedelta(hours=1),
        "status": "active",
        "priority": 100,
        "regions": [],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Admin CRUD / validation
# ---------------------------------------------------------------------------


def test_create_event_banner_requires_admin(fake_db):
    app = _app()
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.post("/api/admin/event-banners", json=_banner_serialized())
    assert resp.status_code == 401


def _banner_serialized():
    doc = _banner_doc()
    doc["starts_at"] = doc["starts_at"].isoformat()
    doc["ends_at"] = doc["ends_at"].isoformat()
    return doc


def test_create_event_banner_success(fake_db):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=_banner_serialized())
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["banner"]["event_id"] == "weekend_tournament_202608"


def test_create_event_banner_duplicate_event_id_rejected(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc())
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=_banner_serialized())
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "event_id_not_unique"


def test_create_event_banner_end_before_start_rejected(fake_db):
    body = _banner_serialized()
    body["starts_at"], body["ends_at"] = body["ends_at"], body["starts_at"]
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "ends_at_before_starts_at"


def test_create_event_banner_bad_priority_rejected(fake_db):
    body = _banner_serialized()
    body["priority"] = "not-a-number"
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_priority"


@pytest.mark.parametrize("field", ["image_url", "destination_url"])
def test_create_event_banner_malformed_url_rejected(fake_db, field):
    body = _banner_serialized()
    body[field] = "javascript:alert(1)"
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=body)
    assert resp.status_code == 400
    assert "invalid" in resp.get_json()["code"]


def test_update_event_banner_status(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc(status="inactive"))
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608", json={"status": "active"}
        )
    assert resp.status_code == 200
    assert resp.get_json()["banner"]["status"] == "active"


# ---------------------------------------------------------------------------
# Public resolution — visibility rules
# ---------------------------------------------------------------------------


def test_active_eligible_banner_is_returned(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc())
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["banner"]["event_id"] == "weekend_tournament_202608"
    assert body["banner"]["destination_url"] == "https://example.com/event"
    assert resp.headers.get("Cache-Control") == "no-store"


def test_future_banner_is_hidden(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["event_banners"].insert_one(
        _banner_doc(starts_at=now + timedelta(hours=1), ends_at=now + timedelta(hours=2))
    )
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json() == {"status": "ok", "banner": None}


def test_expired_banner_is_hidden(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["event_banners"].insert_one(
        _banner_doc(starts_at=now - timedelta(hours=2), ends_at=now - timedelta(hours=1))
    )
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json() == {"status": "ok", "banner": None}


def test_inactive_banner_is_hidden(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc(status="inactive"))
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json() == {"status": "ok", "banner": None}


def test_region_mismatch_is_hidden(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc(regions=["Thailand", "Indonesia"]))
    fake_db["users"].insert_one({"user_id": 111, "region": "Malaysia"})
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json() == {"status": "ok", "banner": None}


def test_region_match_is_shown(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc(regions=["Malaysia", "Thailand"]))
    fake_db["users"].insert_one({"user_id": 111, "region": "Malaysia"})
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json()["banner"]["event_id"] == "weekend_tournament_202608"


def test_highest_priority_banner_wins(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc(event_id="low_prio", priority=10))
    fake_db["event_banners"].insert_one(_banner_doc(event_id="high_prio", priority=999))
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json()["banner"]["event_id"] == "high_prio"


def test_malformed_url_is_rejected_at_resolution(fake_db):
    doc = _banner_doc()
    # Bypass admin validation to simulate a legacy/corrupted stored doc.
    doc["destination_url"] = "javascript:alert(1)"
    fake_db["event_banners"].insert_one(doc)
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json() == {"status": "ok", "banner": None}


def test_unauthenticated_context_is_handled_safely(fake_db):
    fake_db["event_banners"].insert_one(_banner_doc(regions=[]))
    app = _app()
    client = app.test_client()
    with patch("vouchers.verify_telegram_init_data", return_value=(False, {}, "bad_signature")):
        resp = client.get("/api/event-banner")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    # No region-restricted eligibility issue since regions is empty — still visible.
    assert body["banner"]["event_id"] == "weekend_tournament_202608"


def test_no_banner_response_returns_banner_null(fake_db):
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json() == {"status": "ok", "banner": None}


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------


def test_track_endpoint_never_fails_on_bad_input(fake_db):
    app = _app()
    client = app.test_client()
    resp = client.post("/api/event-banner/track", json={})
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


def test_track_endpoint_writes_campaign_event(fake_db, monkeypatch):
    import campaign_events

    monkeypatch.setattr(campaign_events, "database", database)
    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.post(
            "/api/event-banner/track",
            json={"event_id": "weekend_tournament_202608", "type": "impression", "init_data": "x"},
        )
    assert resp.status_code == 200
    events = list(fake_db["campaign_events"].find({"event_type": "event_banner_impression"}))
    assert len(events) == 1
    assert events[0]["campaign_id"] == "weekend_tournament_202608"
    assert events[0]["telegram_user_id"] == 111
