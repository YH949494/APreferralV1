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


def test_patch_with_only_starts_at_preserves_existing_ends_at(fake_db):
    doc = _banner_doc()
    fake_db["event_banners"].insert_one(doc)
    new_starts = (doc["starts_at"] + timedelta(minutes=10)).isoformat()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608", json={"starts_at": new_starts}
        )
    assert resp.status_code == 200
    body = resp.get_json()["banner"]
    assert body["starts_at"] == new_starts
    assert body["ends_at"] == doc["ends_at"].isoformat()


def test_patch_with_only_ends_at_preserves_existing_starts_at(fake_db):
    doc = _banner_doc()
    fake_db["event_banners"].insert_one(doc)
    new_ends = (doc["ends_at"] + timedelta(hours=5)).isoformat()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608", json={"ends_at": new_ends}
        )
    assert resp.status_code == 200
    body = resp.get_json()["banner"]
    assert body["starts_at"] == doc["starts_at"].isoformat()
    assert body["ends_at"] == new_ends


def test_create_event_banner_tg_image_url_rejected(fake_db):
    body = _banner_serialized()
    body["image_url"] = "tg://resolve?domain=advantplay"
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_image_url"


def test_create_event_banner_tg_destination_url_accepted(fake_db):
    body = _banner_serialized()
    body["destination_url"] = "tg://resolve?domain=advantplay"
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/event-banners", json=body)
    assert resp.status_code == 201


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


def test_eligible_low_priority_banner_not_hidden_by_expired_high_priority_ones(fake_db):
    now = datetime.now(timezone.utc)
    # 60 expired/high-priority banners that should never be candidates, all
    # ranked above the one genuinely-eligible low-priority banner — proves
    # the query filters status/window itself instead of truncating a
    # priority-sorted fetch before eligibility is applied.
    for i in range(60):
        fake_db["event_banners"].insert_one(
            _banner_doc(
                event_id=f"expired_{i}",
                priority=1000 - i,
                starts_at=now - timedelta(days=2),
                ends_at=now - timedelta(days=1),
            )
        )
    fake_db["event_banners"].insert_one(_banner_doc(event_id="eligible_low_prio", priority=1))

    app = _app()
    client = app.test_client()
    with _mock_verified_user(111):
        resp = client.get("/api/event-banner?init_data=x")
    assert resp.get_json()["banner"]["event_id"] == "eligible_low_prio"


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


def test_track_endpoint_discards_unauthenticated_event(fake_db, monkeypatch):
    import campaign_events

    monkeypatch.setattr(campaign_events, "database", database)
    app = _app()
    client = app.test_client()
    with patch("vouchers.verify_telegram_init_data", return_value=(False, {}, "bad_signature")):
        resp = client.post(
            "/api/event-banner/track",
            json={"event_id": "weekend_tournament_202608", "type": "impression"},
        )
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}
    assert list(fake_db["campaign_events"].find({})) == []


# ---------------------------------------------------------------------------
# Edit Schedule
# ---------------------------------------------------------------------------


def test_edit_schedule_on_scheduled_banner(fake_db):
    now = datetime.now(timezone.utc)
    doc = _banner_doc(starts_at=now + timedelta(days=1), ends_at=now + timedelta(days=2))
    fake_db["event_banners"].insert_one(doc)
    new_starts = (now + timedelta(days=3)).isoformat()
    new_ends = (now + timedelta(days=4)).isoformat()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": new_starts, "ends_at": new_ends, "priority": 50},
        )
    assert resp.status_code == 200
    body = resp.get_json()["banner"]
    assert body["starts_at"] == new_starts
    assert body["ends_at"] == new_ends
    assert body["priority"] == 50
    # event_id, image_url etc. are untouched
    assert body["event_id"] == "weekend_tournament_202608"
    assert body["image_url"] == doc["image_url"]


def test_edit_schedule_on_live_banner_requires_confirmation(fake_db):
    now = datetime.now(timezone.utc)
    doc = _banner_doc(starts_at=now - timedelta(hours=1), ends_at=now + timedelta(hours=1))
    fake_db["event_banners"].insert_one(doc)
    new_starts = (now + timedelta(hours=2)).isoformat()
    new_ends = (now + timedelta(hours=3)).isoformat()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": new_starts, "ends_at": new_ends},
        )
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "confirmation_required"

    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": new_starts, "ends_at": new_ends, "confirm": True},
        )
    assert resp.status_code == 200
    assert resp.get_json()["banner"]["starts_at"] == new_starts


def test_edit_schedule_rejects_end_before_start(fake_db):
    doc = _banner_doc(status="inactive")
    fake_db["event_banners"].insert_one(doc)
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": doc["ends_at"].isoformat(), "ends_at": doc["starts_at"].isoformat()},
        )
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "ends_at_before_starts_at"


def test_edit_schedule_kl_time_converted_to_utc(fake_db):
    doc = _banner_doc(status="inactive")
    fake_db["event_banners"].insert_one(doc)
    # 2026-09-10 20:00 in Kuala Lumpur (UTC+8) == 2026-09-10 12:00 UTC.
    kl_starts = "2026-09-10T20:00:00+08:00"
    kl_ends = "2026-09-11T20:00:00+08:00"
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": kl_starts, "ends_at": kl_ends},
        )
    assert resp.status_code == 200
    body = resp.get_json()["banner"]
    assert body["starts_at"] == "2026-09-10T12:00:00+00:00"
    assert body["ends_at"] == "2026-09-11T12:00:00+00:00"


def test_edit_schedule_unauthorized_admin_rejected(fake_db):
    doc = _banner_doc(status="inactive")
    fake_db["event_banners"].insert_one(doc)
    app = _app()
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": doc["starts_at"].isoformat(), "ends_at": doc["ends_at"].isoformat()},
        )
    assert resp.status_code == 401


def test_edit_schedule_unknown_event_id_rejected(fake_db):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/does_not_exist/schedule",
            json={"starts_at": "2026-01-01T00:00:00+00:00", "ends_at": "2026-01-02T00:00:00+00:00"},
        )
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"


def test_edit_schedule_writes_activity_log(fake_db):
    doc = _banner_doc(status="inactive", priority=100)
    fake_db["event_banners"].insert_one(doc)
    new_starts = (doc["starts_at"] + timedelta(hours=1)).isoformat()
    new_ends = (doc["ends_at"] + timedelta(hours=1)).isoformat()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            "/api/admin/event-banners/weekend_tournament_202608/schedule",
            json={"starts_at": new_starts, "ends_at": new_ends, "priority": 250},
        )
    assert resp.status_code == 200
    entries = list(fake_db["campaign_admin_audit_log"].find({"entity": "event_banner"}))
    assert len(entries) == 1
    entry = entries[0]
    assert entry["action"] == "edit_schedule"
    assert entry["entity_id"] == "weekend_tournament_202608"
    assert entry["admin"] == "admin"
    assert entry["details"]["previous_schedule"]["priority"] == 100
    assert entry["details"]["new_schedule"]["priority"] == 250
    assert entry["details"]["new_schedule"]["starts_at"] == new_starts
    assert "at" in entry


# ---------------------------------------------------------------------------
# Effective status
# ---------------------------------------------------------------------------


def test_effective_status_expired_banner_not_shown_as_active(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["event_banners"].insert_one(
        _banner_doc(starts_at=now - timedelta(hours=2), ends_at=now - timedelta(hours=1), status="active")
    )
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.get("/api/admin/event-banners")
    banner = resp.get_json()["banners"][0]
    assert banner["status"] == "active"
    assert banner["effective_status"] == "expired"


def test_effective_status_scheduled_and_live_and_inactive(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["event_banners"].insert_one(
        _banner_doc(event_id="future", starts_at=now + timedelta(hours=1), ends_at=now + timedelta(hours=2), status="active")
    )
    fake_db["event_banners"].insert_one(
        _banner_doc(event_id="now", starts_at=now - timedelta(hours=1), ends_at=now + timedelta(hours=1), status="active")
    )
    fake_db["event_banners"].insert_one(
        _banner_doc(event_id="off", status="inactive")
    )
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.get("/api/admin/event-banners")
    by_id = {b["event_id"]: b["effective_status"] for b in resp.get_json()["banners"]}
    assert by_id["future"] == "scheduled"
    assert by_id["now"] == "live"
    assert by_id["off"] == "inactive"


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
