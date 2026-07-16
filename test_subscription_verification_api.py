"""Tests for subscription_verification_api.py after switching its event
logging to the canonical campaign_events writer."""

from unittest.mock import patch

import pytest
from flask import Flask

import database
import campaign_centre as cc
import campaign_events as ce
import subscription_verification_api as sva
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(ce, "database", database)
    import campaign_providers as cp
    monkeypatch.setattr(cp, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(sva.subscription_verification_bp)
    return app


def _campaign():
    return {
        "campaign_id": "sub-voucher-july",
        "type": "external_subscription_verification",
        "status": "live",
        "schedule": {"starts_at": __import__("datetime").datetime(2020, 1, 1, tzinfo=__import__("datetime").timezone.utc)},
        "destination": {"provider_id": "ext-site", "ready": True},
        "telegram": {"channel_username": "advantplayofficial"},
    }


def test_verify_emits_subscription_pass_event(fake_db):
    fake_db["gc_campaigns"].insert_one(_campaign())
    fake_db["gc_providers"].insert_one({"provider_id": "ext-site", "active": True})

    with patch("vouchers.verify_telegram_init_data", return_value=(True, {"user": '{"id": 111}'}, "ok")), \
         patch("subscription_gate.verify_campaign_subscription", return_value={"subscribed": True, "reason": "member"}):
        resp = _app().test_client().post("/api/integrations/subscription/verify", json={
            "campaign_id": "sub-voucher-july", "init_data": "raw",
        })
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["subscribed"] is True
    assert body["telegram_user_id"] == 111
    assert "voucher_code" not in body
    assert fake_db["campaign_events"].count_documents({"event_type": "subscription_pass"}) == 1


def test_verify_never_returns_voucher_code(fake_db):
    fake_db["gc_campaigns"].insert_one(_campaign())
    fake_db["gc_providers"].insert_one({"provider_id": "ext-site", "active": True})
    with patch("vouchers.verify_telegram_init_data", return_value=(True, {"user": '{"id": 111}'}, "ok")), \
         patch("subscription_gate.verify_campaign_subscription", return_value={"subscribed": False, "reason": "left"}):
        resp = _app().test_client().post("/api/integrations/subscription/verify", json={
            "campaign_id": "sub-voucher-july", "init_data": "raw",
        })
    assert "code" not in resp.get_json()


def test_verify_wrong_campaign_type_rejected(fake_db):
    campaign = _campaign()
    campaign["type"] = "tournament"
    fake_db["gc_campaigns"].insert_one(campaign)
    resp = _app().test_client().post("/api/integrations/subscription/verify", json={
        "campaign_id": "sub-voucher-july", "init_data": "raw",
    })
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "wrong_campaign_type"
