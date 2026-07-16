"""Tests for subscription_gate.py: the shared Telegram channel-membership
verification service used by campaign/tournament flows."""

from unittest.mock import MagicMock, patch

import pytest

import database
import subscription_gate as sg
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(sg, "database", database)
    return fdb


def _campaign():
    return {"campaign_id": "c1", "telegram": {"channel_id": -100123, "channel_username": "advantplayofficial"}}


def _mock_response(status_code, json_body):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    return resp


@pytest.mark.parametrize("status", ["member", "administrator", "creator"])
def test_subscribed_statuses_pass(fake_db, status, monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "t")
    with patch("subscription_gate.requests.get", return_value=_mock_response(200, {"ok": True, "result": {"status": status}})):
        result = sg.verify_campaign_subscription(_campaign(), 111)
    assert result["subscribed"] is True


@pytest.mark.parametrize("status", ["left", "kicked", "restricted"])
def test_not_subscribed_statuses_fail(fake_db, status, monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "t")
    with patch("subscription_gate.requests.get", return_value=_mock_response(200, {"ok": True, "result": {"status": status}})):
        result = sg.verify_campaign_subscription(_campaign(), 111)
    assert result["subscribed"] is False


def test_telegram_429_fails_closed_after_retries(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "t")
    monkeypatch.setattr("subscription_gate.time.sleep", lambda *_: None)
    with patch("subscription_gate.requests.get", return_value=_mock_response(429, {})):
        result = sg.verify_campaign_subscription(_campaign(), 111)
    assert result["subscribed"] is False
    assert result["reason"] == "rate_limited"


def test_network_timeout_fails_closed(fake_db, monkeypatch):
    import requests

    monkeypatch.setenv("BOT_TOKEN", "t")
    monkeypatch.setattr("subscription_gate.time.sleep", lambda *_: None)
    with patch("subscription_gate.requests.get", side_effect=requests.exceptions.Timeout("boom")):
        result = sg.verify_campaign_subscription(_campaign(), 111)
    assert result["subscribed"] is False
    assert "network_error" in result["reason"]


def test_cache_hit_skips_live_call(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "t")
    sg._cache_set(-100123, 111, True, 300)
    with patch("subscription_gate.requests.get") as mock_get:
        result = sg.verify_campaign_subscription(_campaign(), 111)
    mock_get.assert_not_called()
    assert result["subscribed"] is True
    assert result["source"] == "cache"


def test_forced_refresh_bypasses_cache(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "t")
    sg._cache_set(-100123, 111, True, 300)
    with patch("subscription_gate.requests.get", return_value=_mock_response(200, {"ok": True, "result": {"status": "left"}})) as mock_get:
        result = sg.verify_campaign_subscription(_campaign(), 111, force_refresh=True)
    mock_get.assert_called_once()
    assert result["subscribed"] is False


def test_missing_channel_config_fails_closed(fake_db):
    campaign = {"campaign_id": "c1", "telegram": {}}
    result = sg.verify_campaign_subscription(campaign, 111)
    assert result["subscribed"] is False
    assert result["reason"] == "channel_not_configured"
