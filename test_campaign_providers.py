"""Tests for campaign_providers.py: UID deep-link URL building and
validation. No initData/signed-token logic is expected in Phase 1."""

from unittest.mock import patch

import pytest
from flask import Flask

import campaign_providers as cp
import database
from fake_mongo import FakeDb


def _provider(**overrides):
    base = {"provider_id": "mywin-tournament", "base_url": "https://tournament.example.com",
            "url_mode": "query_parameter", "active": True}
    base.update(overrides)
    return base


def test_query_parameter_mode_with_path():
    provider = _provider(url_mode="query_parameter")
    campaign = {"campaign_id": "july-tournament-2026", "destination": {"path": "/july-tournament"}}
    url = cp.build_effective_url(provider, campaign, 123456789)
    assert url == "https://tournament.example.com/july-tournament?uid=123456789"


def test_query_parameter_mode_default_path():
    provider = _provider(url_mode="query_parameter")
    campaign = {"campaign_id": "c1", "destination": {"path": ""}}
    url = cp.build_effective_url(provider, campaign, 42)
    assert url == "https://tournament.example.com/play?uid=42"


def test_path_parameter_mode():
    provider = _provider(url_mode="path_parameter")
    campaign = {"campaign_id": "c1", "destination": {"path": "ignored"}}
    url = cp.build_effective_url(provider, campaign, 123456789)
    assert url == "https://tournament.example.com/123456789"


def test_custom_template_mode():
    provider = _provider(url_mode="custom_template",
                          url_template="{base_url}/play/{telegram_uid}/campaign/{campaign_id}")
    campaign = {"campaign_id": "july-tournament-2026", "destination": {"path": ""}}
    url = cp.build_effective_url(provider, campaign, 123456789)
    assert url == "https://tournament.example.com/play/123456789/campaign/july-tournament-2026"


def test_no_uid_ever_taken_from_client_supplied_url():
    # build_effective_url only ever accepts a telegram_user_id argument that
    # the caller must have derived from verified initData server-side; there
    # is no code path here that reads a uid from request args.
    import inspect

    src = inspect.getsource(cp.build_effective_url)
    assert "request.args" not in src
    assert "request.get_json" not in src


def test_missing_base_url_returns_none():
    provider = _provider(base_url="")
    campaign = {"campaign_id": "c1", "destination": {"path": ""}}
    assert cp.build_effective_url(provider, campaign, 1) is None


def test_valid_https_url_accepts_https():
    assert cp._valid_https_url("https://tournament.example.com") is True


def test_valid_https_url_rejects_http_in_production(monkeypatch):
    monkeypatch.delenv("FLASK_ENV", raising=False)
    assert cp._valid_https_url("http://tournament.example.com") is False


def test_provider_is_usable_requires_active_true():
    assert cp.provider_is_usable_for_results({"active": True}) is True
    assert cp.provider_is_usable_for_results({"active": False}) is False
    assert cp.provider_is_usable_for_results(None) is False


def test_provider_secret_never_hardcoded_reads_from_env(monkeypatch):
    monkeypatch.setenv("CAMPAIGN_PROVIDER_SECRET_TEST", "s3cr3t")
    provider = {"secret_env_var": "CAMPAIGN_PROVIDER_SECRET_TEST"}
    assert cp.provider_secret(provider) == "s3cr3t"


def test_provider_secret_missing_env_var_returns_empty():
    provider = {"secret_env_var": ""}
    assert cp.provider_secret(provider) == ""


# ---------------------------------------------------------------------------
# P0.17 §C — provider_has_valid_destination: the shared usability rule an
# active provider must satisfy to ever serve a player-facing destination.
# Distinct from provider_is_usable_for_results (active-only, used for
# server-to-server result crediting/HMAC verification — see
# tournament_rewards.py/tournament_integration.py, unaffected by base_url).
# ---------------------------------------------------------------------------

def test_provider_has_valid_destination_requires_base_url_even_when_active():
    assert cp.provider_has_valid_destination({"active": True, "base_url": ""}) is False
    assert cp.provider_has_valid_destination({"active": True}) is False


def test_provider_has_valid_destination_true_for_active_with_https_base_url():
    assert cp.provider_has_valid_destination({"active": True, "base_url": "https://tournament.example.com"}) is True


def test_provider_has_valid_destination_false_when_inactive_even_with_base_url():
    assert cp.provider_has_valid_destination({"active": False, "base_url": "https://tournament.example.com"}) is False


def test_provider_has_valid_destination_false_for_none():
    assert cp.provider_has_valid_destination(None) is False


# ---------------------------------------------------------------------------
# P0.17 §C1/§C2 — provider activation guard: an inactive/draft provider may
# be saved without a base_url, but activation must reject one.
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={"gc_providers": [("provider_id",)]})
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cp, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cp.campaign_providers_bp)
    return app


def _admin():
    return patch("vouchers.require_admin", return_value=({"id": 1}, None))


def test_create_allows_blank_base_url_for_a_draft_inactive_provider(fake_db):
    with _app().test_client() as client, _admin():
        resp = client.post("/api/admin/providers", json={
            "provider_id": "draft-1", "name": "Draft Provider", "type": "tournament",
            "base_url": "", "auth_mode": "none",
        })
    assert resp.status_code == 201
    doc = fake_db["gc_providers"].find_one({"provider_id": "draft-1"})
    assert doc["active"] is False
    assert doc["base_url"] == ""


def test_activation_rejected_without_base_url(fake_db):
    fake_db["gc_providers"].insert_one({
        "provider_id": "draft-2", "name": "Draft", "type": "tournament",
        "base_url": "", "auth_mode": "none", "active": False,
    })
    with _app().test_client() as client, _admin():
        resp = client.post("/api/admin/providers/draft-2/activate")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "provider_base_url_required"
    doc = fake_db["gc_providers"].find_one({"provider_id": "draft-2"})
    assert doc["active"] is False  # never actually activated


def test_activation_accepted_with_valid_https_base_url(fake_db):
    fake_db["gc_providers"].insert_one({
        "provider_id": "ready-1", "name": "Ready", "type": "tournament",
        "base_url": "https://tournament.example.com", "auth_mode": "none", "active": False,
    })
    with _app().test_client() as client, _admin():
        resp = client.post("/api/admin/providers/ready-1/activate")
    assert resp.status_code == 200
    doc = fake_db["gc_providers"].find_one({"provider_id": "ready-1"})
    assert doc["active"] is True


def test_activation_rejected_when_hmac_secret_missing_even_with_base_url(fake_db):
    """Pre-existing secret_not_configured guard must keep working alongside
    the new base_url guard — neither one silently supersedes the other."""
    fake_db["gc_providers"].insert_one({
        "provider_id": "no-secret", "name": "No Secret", "type": "tournament",
        "base_url": "https://tournament.example.com", "auth_mode": "hmac_sha256",
        "secret_env_var": "", "active": False,
    })
    with _app().test_client() as client, _admin():
        resp = client.post("/api/admin/providers/no-secret/activate")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "secret_not_configured"
