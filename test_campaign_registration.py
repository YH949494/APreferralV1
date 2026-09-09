"""Tests for campaign_registration.py: active-campaign resolution, the
register/dismiss public API, deep-link parsing, and the admin
list/summary/export surface.

Uses the same FakeDb + Flask test-client harness as test_mission_pool_phase2.py.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import campaign_registration as cr
import database
import mission_pool_ux as mpux
from fake_mongo import FakeDb

CAMPAIGN_ID = "community_lucky_draw_oct2026"
UID = 555001
OTHER_UID = 555002


def _unique_keys():
    return {
        "gc_campaigns": [("campaign_id",)],
        cr.REGISTRATIONS_COLLECTION: [("campaign_id", "telegram_user_id")],
        cr.STATE_COLLECTION: [("campaign_id", "telegram_user_id")],
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys())
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cr, "database", database)
    monkeypatch.setattr(cc, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cr.campaign_registration_bp)
    app.register_blueprint(cr.campaign_registration_admin_bp)
    return app


def _verified(uid: int, username: str = "playerone"):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": '{"id": %d, "username": "%s"}' % (uid, username)}, "ok"),
    )


def _admin_ok():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "ops"}, None))


def _admin_denied():
    return patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401)))


def _campaign(**overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Community Lucky Draw",
        "type": "external_website",
        "status": "live",
        "priority": 100,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(days=30)},
        "telegram": {"require_identity": True, "require_subscription": False, "channel_username": "advantplayofficial"},
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
        "registration": cr.default_registration_config(),
    }
    doc["registration"]["enabled"] = True
    doc.update(overrides)
    return doc


def _insert_campaign(fake_db, **overrides):
    doc = _campaign(**overrides)
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


# ---------------------------------------------------------------------------
# 1/2/9 — should_prompt resolution
# ---------------------------------------------------------------------------

def test_active_campaign_unregistered_should_prompt(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(UID):
        r = client.get("/api/campaign-registration/active?init_data=x")
        data = r.get_json()
        assert data["should_prompt"] is True
        assert data["registered"] is False
        assert data["campaign"]["campaign_id"] == CAMPAIGN_ID


def test_registered_user_no_prompt(fake_db):
    _insert_campaign(fake_db)
    fake_db[cr.REGISTRATIONS_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID, "telegram_username": "playerone",
        "full_name": "A", "contact_number": "+60123456789", "country_region": "Malaysia",
        "delivery_address": "1 Main St", "channel_verified": False, "base_entries": 1,
        "status": "registered", "registered_at": datetime.now(timezone.utc),
    })
    with _app().test_client() as client, _verified(UID):
        r = client.get("/api/campaign-registration/active?init_data=x")
        data = r.get_json()
        assert data["registered"] is True
        assert data["should_prompt"] is False


def test_closed_campaign_no_prompt(fake_db):
    _insert_campaign(fake_db, status="draft")
    with _app().test_client() as client, _verified(UID):
        data = client.get("/api/campaign-registration/active?init_data=x").get_json()
        assert data["campaign"] is None
        assert data["should_prompt"] is False


def test_future_campaign_no_prompt(fake_db):
    now = datetime.now(timezone.utc)
    _insert_campaign(fake_db, schedule={"starts_at": now + timedelta(days=1), "ends_at": None})
    with _app().test_client() as client, _verified(UID):
        data = client.get("/api/campaign-registration/active?init_data=x").get_json()
        assert data["campaign"] is None


# ---------------------------------------------------------------------------
# 3/4/5 — validation, single insert, idempotent retry
# ---------------------------------------------------------------------------

def _valid_payload(**overrides):
    body = {
        "full_name": "Jane Doe",
        "contact_number": "+65 8123 4567",
        "country_region": "Singapore",
        "delivery_address": "10 Orchard Rd, #01-01",
    }
    body.update(overrides)
    return body


def test_missing_required_field_400_no_insert(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(UID):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                         json=_valid_payload(full_name=""))
        assert r.status_code == 400
        assert r.get_json()["code"] == "missing_full_name"
        assert fake_db[cr.REGISTRATIONS_COLLECTION].count_documents({}) == 0


def test_successful_registration_inserted_once(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(UID):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x", json=_valid_payload())
        assert r.status_code == 201
        data = r.get_json()
        assert data["status"] == "ok"
        assert data["registration"]["base_entries"] == 1
        assert fake_db[cr.REGISTRATIONS_COLLECTION].count_documents({}) == 1


def test_duplicate_retry_no_duplicate_row(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(UID):
        r1 = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x", json=_valid_payload())
        assert r1.status_code == 201
        r2 = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x", json=_valid_payload())
        assert r2.status_code == 200
        assert r2.get_json()["already_registered"] is True
        assert fake_db[cr.REGISTRATIONS_COLLECTION].count_documents({}) == 1


def test_same_user_can_register_different_campaign(fake_db):
    _insert_campaign(fake_db)
    _insert_campaign(fake_db, campaign_id="second-campaign")
    with _app().test_client() as client, _verified(UID):
        r1 = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x", json=_valid_payload())
        r2 = client.post("/api/campaign-registration/second-campaign/register?init_data=x", json=_valid_payload())
        assert r1.status_code == 201
        assert r2.status_code == 201
        assert fake_db[cr.REGISTRATIONS_COLLECTION].count_documents({"telegram_user_id": UID}) == 2


# ---------------------------------------------------------------------------
# 7/8 — dismissal suppression + reminder expiry
# ---------------------------------------------------------------------------

def test_dismiss_suppresses_until_next_prompt_at(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(UID):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/dismiss?init_data=x")
        assert r.status_code == 200
        state = fake_db[cr.STATE_COLLECTION].find_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": UID})
        assert state is not None
        assert state["next_prompt_at"] > datetime.now(timezone.utc)

        data = client.get("/api/campaign-registration/active?init_data=x").get_json()
        assert data["should_prompt"] is False
        assert data.get("dismissed_until")


def test_prompt_eligible_again_after_reminder_interval(fake_db):
    _insert_campaign(fake_db)
    fake_db[cr.STATE_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID,
        "dismissed_at": datetime.now(timezone.utc) - timedelta(hours=25),
        "next_prompt_at": datetime.now(timezone.utc) - timedelta(hours=1),
    })
    with _app().test_client() as client, _verified(UID):
        data = client.get("/api/campaign-registration/active?init_data=x").get_json()
        assert data["should_prompt"] is True


# ---------------------------------------------------------------------------
# 10 — all-regions campaign accepts non-restricted region
# ---------------------------------------------------------------------------

def test_all_regions_campaign_accepts_non_malaysia_region(fake_db):
    _insert_campaign(fake_db)  # default audience scope = "all"
    with _app().test_client() as client, _verified(UID):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                         json=_valid_payload(country_region="Indonesia"))
        assert r.status_code == 201


def test_selected_region_audience_blocks_ineligible_region(fake_db):
    _insert_campaign(fake_db, registration={
        **cr.default_registration_config(),
        "enabled": True,
        "audience": {"scope": "selected", "regions": ["Malaysia"]},
    })
    with _app().test_client() as client, _verified(UID):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                         json=_valid_payload(country_region="Thailand"))
        assert r.status_code == 403
        assert r.get_json()["code"] == "region_not_eligible"


# ---------------------------------------------------------------------------
# 11 — TG user id cannot be spoofed through the request body
# ---------------------------------------------------------------------------

def test_telegram_user_id_not_spoofable_via_body(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(UID):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x",
                         json=_valid_payload(telegram_user_id=OTHER_UID, telegram_username="attacker"))
        assert r.status_code == 201
        stored = fake_db[cr.REGISTRATIONS_COLLECTION].find_one({"campaign_id": CAMPAIGN_ID})
        assert stored["telegram_user_id"] == UID
        assert stored["telegram_username"] == "playerone"


# ---------------------------------------------------------------------------
# Channel subscription: distinguishes system error from confirmed not-subscribed
# ---------------------------------------------------------------------------

def test_require_channel_subscription_blocks_unsubscribed(fake_db):
    _insert_campaign(fake_db, registration={**cr.default_registration_config(), "enabled": True, "require_channel_subscription": True})
    with _app().test_client() as client, _verified(UID), patch(
        "subscription_gate.verify_campaign_subscription",
        return_value={"subscribed": False, "reason": "left", "source": "live"},
    ):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x", json=_valid_payload())
        assert r.status_code == 403
        assert r.get_json()["code"] == "channel_subscription_required"


def test_subscription_system_error_is_not_treated_as_unsubscribed(fake_db):
    _insert_campaign(fake_db, registration={**cr.default_registration_config(), "enabled": True, "require_channel_subscription": True})
    with _app().test_client() as client, _verified(UID), patch(
        "subscription_gate.verify_campaign_subscription",
        return_value={"subscribed": False, "reason": "network_error:Timeout", "source": "live"},
    ):
        r = client.post(f"/api/campaign-registration/{CAMPAIGN_ID}/register?init_data=x", json=_valid_payload())
        assert r.status_code == 503
        assert r.get_json()["code"] == "subscription_check_failed"


# ---------------------------------------------------------------------------
# 12/13 — admin export protected + CSV fields
# ---------------------------------------------------------------------------

def test_admin_export_requires_admin(fake_db):
    with _app().test_client() as client, _admin_denied():
        r = client.get("/api/admin/campaign-registrations/export")
        assert r.status_code == 401


def test_admin_list_requires_admin(fake_db):
    with _app().test_client() as client, _admin_denied():
        r = client.get("/api/admin/campaign-registrations")
        assert r.status_code == 401


def test_csv_export_contains_expected_fields(fake_db):
    fake_db[cr.REGISTRATIONS_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID, "telegram_username": "playerone",
        "full_name": "Jane Doe", "contact_number": "+65 8123 4567", "country_region": "Singapore",
        "delivery_address": "10 Orchard Rd", "channel_verified": True, "base_entries": 1,
        "status": "registered", "registered_at": datetime.now(timezone.utc),
    })
    with _app().test_client() as client, _admin_ok():
        r = client.get("/api/admin/campaign-registrations/export")
        assert r.status_code == 200
        assert "text/csv" in r.headers["Content-Type"]
        body = r.get_data(as_text=True)
        header = body.splitlines()[0]
        for field in cr._CSV_FIELDS:
            assert field in header
        assert "Jane Doe" in body
        assert str(UID) in body


def test_admin_list_and_summary(fake_db):
    fake_db[cr.REGISTRATIONS_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID, "telegram_username": "playerone",
        "full_name": "Jane Doe", "contact_number": "+65 8123 4567", "country_region": "Singapore",
        "delivery_address": "10 Orchard Rd", "channel_verified": True, "base_entries": 1,
        "status": "registered", "registered_at": datetime.now(timezone.utc),
    })
    with _app().test_client() as client, _admin_ok():
        r = client.get("/api/admin/campaign-registrations")
        data = r.get_json()
        assert data["total"] == 1
        assert data["registrations"][0]["full_name"] == "Jane Doe"

        summary = client.get(f"/api/admin/campaign-registrations/summary?campaign_id={CAMPAIGN_ID}").get_json()
        assert summary["total_registrations"] == 1
        assert summary["channel_verified"] == 1


# ---------------------------------------------------------------------------
# 14/15 — deep link resolution + no collision with Mission startapp parsing
# ---------------------------------------------------------------------------

def test_campaign_deep_link_round_trips():
    assert cr.parse_campaign_start_param(cr.campaign_start_param(CAMPAIGN_ID)) == CAMPAIGN_ID


def test_campaign_and_mission_start_params_do_not_collide():
    mission_param = mpux.mission_start_param("some-mission")
    campaign_param = cr.campaign_start_param("some-campaign")
    assert cr.parse_campaign_start_param(mission_param) is None
    assert mpux.parse_mission_start_param(campaign_param) is None
    assert cr.parse_campaign_start_param(campaign_param) == "some-campaign"
    assert mpux.parse_mission_start_param(mission_param) == "some-mission"


# ---------------------------------------------------------------------------
# Registration config validation (admin CRUD delegate, campaign_centre.py)
# ---------------------------------------------------------------------------

def test_validate_registration_config_defaults():
    cfg, err = cr.validate_registration_config({"enabled": True})
    assert err is None
    assert cfg["enabled"] is True
    assert cfg["required_fields"] == cr.REQUIRED_FIELD_KEYS
    assert cfg["reminder_hours"] == cr.DEFAULT_REMINDER_HOURS


def test_validate_registration_config_selected_scope_requires_regions():
    cfg, err = cr.validate_registration_config({"audience": {"scope": "selected", "regions": []}})
    assert cfg is None
    assert err == "audience_regions_required"


def test_campaign_centre_create_accepts_registration_block(fake_db):
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    with app.test_client() as client, _admin_ok():
        r = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": "reg-via-cc",
            "name": "Reg via CC",
            "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
            "registration": {"enabled": True, "reminder_hours": 12},
        })
        assert r.status_code == 201
        doc = fake_db["gc_campaigns"].find_one({"campaign_id": "reg-via-cc"})
        assert doc["registration"]["enabled"] is True
        assert doc["registration"]["reminder_hours"] == 12
