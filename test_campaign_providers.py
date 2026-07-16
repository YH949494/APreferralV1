"""Tests for campaign_providers.py: UID deep-link URL building and
validation. No initData/signed-token logic is expected in Phase 1."""

import campaign_providers as cp


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
