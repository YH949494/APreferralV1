"""
Tests for Databot Phase 1 integration layer.

Coverage:
  - disabled config  (DATABOT_ENABLED=false)
  - missing base URL
  - timeout
  - invalid response (non-JSON)
  - HTTP error response
  - successful response
  - fallback behavior (service layer returns None, logs fallback_used)
  - each service method (get_user_segment, get_segment_probability_config,
    get_segment_roi_summary, get_campaign_preview)
"""
import pytest
from unittest.mock import MagicMock, patch

import requests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(json_data=None, status_code=200, raise_for_status=None, json_raises=None):
    resp = MagicMock()
    resp.status_code = status_code
    if json_raises:
        resp.json.side_effect = json_raises
    else:
        resp.json.return_value = json_data or {}
    if raise_for_status:
        resp.raise_for_status.side_effect = raise_for_status
    else:
        resp.raise_for_status.return_value = None
    return resp


# ===========================================================================
# databot_client — low-level HTTP layer
# ===========================================================================

class TestDatabotClientDisabled:
    def test_get_raises_when_disabled(self):
        import databot_client as dc
        with patch.object(dc, "DATABOT_ENABLED", False):
            from databot_client import DatabotUnavailableError, databot_get
            with pytest.raises(DatabotUnavailableError, match="disabled"):
                databot_get("/api/v1/segments/probability-config")

    def test_post_raises_when_disabled(self):
        import databot_client as dc
        with patch.object(dc, "DATABOT_ENABLED", False):
            from databot_client import DatabotUnavailableError, databot_post
            with pytest.raises(DatabotUnavailableError, match="disabled"):
                databot_post("/api/v1/campaigns/preview", {})

    def test_get_raises_when_base_url_missing(self):
        import databot_client as dc
        with patch.object(dc, "DATABOT_ENABLED", True), patch.object(dc, "DATABOT_BASE_URL", ""):
            from databot_client import DatabotUnavailableError, databot_get
            with pytest.raises(DatabotUnavailableError, match="DATABOT_BASE_URL"):
                databot_get("/api/v1/segments/probability-config")


class TestDatabotClientTimeout:
    def test_get_timeout_raises_unavailable(self):
        import databot_client as dc
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.get", side_effect=requests.exceptions.Timeout("timed out")),
        ):
            from databot_client import DatabotUnavailableError, databot_get
            with pytest.raises(DatabotUnavailableError, match="timeout"):
                databot_get("/api/v1/segments/probability-config")

    def test_post_timeout_raises_unavailable(self):
        import databot_client as dc
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.post", side_effect=requests.exceptions.Timeout("timed out")),
        ):
            from databot_client import DatabotUnavailableError, databot_post
            with pytest.raises(DatabotUnavailableError, match="timeout"):
                databot_post("/api/v1/campaigns/preview", {})


class TestDatabotClientInvalidResponse:
    def test_get_non_json_raises_unavailable(self):
        import databot_client as dc
        resp = _mock_response(json_raises=requests.exceptions.JSONDecodeError("bad json", "", 0))
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.get", return_value=resp),
        ):
            from databot_client import DatabotUnavailableError, databot_get
            with pytest.raises(DatabotUnavailableError, match="non-JSON"):
                databot_get("/api/v1/segments/probability-config")

    def test_get_http_error_raises_unavailable(self):
        import databot_client as dc
        resp = _mock_response(
            status_code=500,
            raise_for_status=requests.exceptions.HTTPError("500 Server Error"),
        )
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.get", return_value=resp),
        ):
            from databot_client import DatabotUnavailableError, databot_get
            with pytest.raises(DatabotUnavailableError, match="HTTP error"):
                databot_get("/api/v1/segments/probability-config")

    def test_post_non_json_raises_unavailable(self):
        import databot_client as dc
        resp = _mock_response(json_raises=requests.exceptions.JSONDecodeError("bad json", "", 0))
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.post", return_value=resp),
        ):
            from databot_client import DatabotUnavailableError, databot_post
            with pytest.raises(DatabotUnavailableError, match="non-JSON"):
                databot_post("/api/v1/campaigns/preview", {})


class TestDatabotClientSuccess:
    def test_get_returns_parsed_json(self):
        import databot_client as dc
        payload = {"high_value": 55, "ghost": 5}
        resp = _mock_response(json_data=payload)
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.get", return_value=resp),
        ):
            from databot_client import databot_get
            result = databot_get("/api/v1/segments/probability-config")
        assert result == payload

    def test_post_returns_parsed_json(self):
        import databot_client as dc
        payload = {"audience_size": 200, "expected_voucher_cost": 2000.0}
        resp = _mock_response(json_data=payload)
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.post", return_value=resp),
        ):
            from databot_client import databot_post
            result = databot_post("/api/v1/campaigns/preview", {"segments": ["high_value"]})
        assert result == payload

    def test_get_sends_authorization_header(self):
        import databot_client as dc
        resp = _mock_response(json_data={})
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch.object(dc, "DATABOT_API_KEY", "secret-key"),
            patch("databot_client.requests.get", return_value=resp) as mock_get,
        ):
            from databot_client import databot_get
            databot_get("/api/v1/segments/roi-summary")
        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer secret-key"

    def test_get_respects_timeout_setting(self):
        import databot_client as dc
        resp = _mock_response(json_data={})
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch.object(dc, "DATABOT_TIMEOUT_SECONDS", 3),
            patch("databot_client.requests.get", return_value=resp) as mock_get,
        ):
            from databot_client import databot_get
            databot_get("/api/v1/segments/probability-config")
        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == 3


class TestDatabotClientLogging:
    def test_success_logs_started_and_success(self, caplog):
        import databot_client as dc
        resp = _mock_response(json_data={"ok": True})
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.get", return_value=resp),
            caplog.at_level("INFO", logger="databot_client"),
        ):
            from databot_client import databot_get
            databot_get("/api/v1/segments/roi-summary")
        messages = " ".join(caplog.messages)
        assert "databot_request_started" in messages
        assert "databot_request_success" in messages

    def test_timeout_logs_request_failed(self, caplog):
        import databot_client as dc
        with (
            patch.object(dc, "DATABOT_ENABLED", True),
            patch.object(dc, "DATABOT_BASE_URL", "http://databot.internal"),
            patch("databot_client.requests.get", side_effect=requests.exceptions.Timeout("timed out")),
            caplog.at_level("WARNING", logger="databot_client"),
        ):
            from databot_client import DatabotUnavailableError, databot_get
            with pytest.raises(DatabotUnavailableError):
                databot_get("/api/v1/segments/roi-summary")
        assert "databot_request_failed" in " ".join(caplog.messages)


# ===========================================================================
# databot_service — fallback / service layer
# ===========================================================================

class TestDatabotServiceFallback:
    """All service methods must return None and log fallback_used when Databot is down."""

    def test_get_user_segment_fallback(self, caplog):
        with (
            patch("databot_service.databot_get", side_effect=Exception("databot_client import short-circuit")),
            patch(
                "databot_service.databot_get",
                side_effect=__import__("databot_client").DatabotUnavailableError("down"),
            ),
            caplog.at_level("INFO", logger="databot_service"),
        ):
            from databot_service import get_user_segment
            result = get_user_segment("user123")
        assert result is None
        assert "fallback_used" in " ".join(caplog.messages)

    def test_get_segment_probability_config_fallback(self, caplog):
        with (
            patch(
                "databot_service.databot_get",
                side_effect=__import__("databot_client").DatabotUnavailableError("down"),
            ),
            caplog.at_level("INFO", logger="databot_service"),
        ):
            from databot_service import get_segment_probability_config
            result = get_segment_probability_config()
        assert result is None
        assert "fallback_used" in " ".join(caplog.messages)

    def test_get_segment_roi_summary_fallback(self, caplog):
        with (
            patch(
                "databot_service.databot_get",
                side_effect=__import__("databot_client").DatabotUnavailableError("down"),
            ),
            caplog.at_level("INFO", logger="databot_service"),
        ):
            from databot_service import get_segment_roi_summary
            result = get_segment_roi_summary()
        assert result is None
        assert "fallback_used" in " ".join(caplog.messages)

    def test_get_campaign_preview_fallback(self, caplog):
        with (
            patch(
                "databot_service.databot_post",
                side_effect=__import__("databot_client").DatabotUnavailableError("down"),
            ),
            caplog.at_level("INFO", logger="databot_service"),
        ):
            from databot_service import get_campaign_preview
            result = get_campaign_preview({"segments": ["high_value"]})
        assert result is None
        assert "fallback_used" in " ".join(caplog.messages)

    def test_disabled_databot_causes_fallback(self, caplog):
        """End-to-end: DATABOT_ENABLED=false triggers fallback in service layer."""
        import databot_client as dc
        with (
            patch.object(dc, "DATABOT_ENABLED", False),
            caplog.at_level("INFO", logger="databot_service"),
        ):
            from databot_service import get_segment_probability_config
            result = get_segment_probability_config()
        assert result is None
        assert "fallback_used" in " ".join(caplog.messages)


class TestDatabotServiceSuccess:
    """Service methods return Databot payload on success."""

    def test_get_user_segment_returns_payload(self):
        payload = {"account_id": "u1", "segment": "high_value", "confidence": 0.9}
        with patch("databot_service.databot_get", return_value=payload):
            from databot_service import get_user_segment
            result = get_user_segment("u1")
        assert result == payload

    def test_get_segment_probability_config_returns_payload(self):
        payload = {"high_value": 55, "ghost": 4}
        with patch("databot_service.databot_get", return_value=payload):
            from databot_service import get_segment_probability_config
            result = get_segment_probability_config()
        assert result == payload

    def test_get_segment_roi_summary_returns_payload(self):
        payload = {"segments": {"high_value": {"avg_roi": 4.2}}}
        with patch("databot_service.databot_get", return_value=payload):
            from databot_service import get_segment_roi_summary
            result = get_segment_roi_summary()
        assert result == payload

    def test_get_campaign_preview_returns_payload(self):
        payload = {"audience_size": 500, "expected_voucher_cost": 5000.0}
        with patch("databot_service.databot_post", return_value=payload):
            from databot_service import get_campaign_preview
            result = get_campaign_preview({"segments": ["normal_actual"]})
        assert result == payload

    def test_get_campaign_preview_passes_params_to_post(self):
        params = {"segments": ["high_value"], "voucher_value": 10.0}
        with patch("databot_service.databot_post", return_value={}) as mock_post:
            from databot_service import get_campaign_preview
            get_campaign_preview(params)
        mock_post.assert_called_once_with("/api/v1/campaigns/preview", params)


# ===========================================================================
# Config constants
# ===========================================================================

class TestDatabotConfig:
    def test_defaults_are_safe(self):
        from config import DATABOT_ENABLED, DATABOT_BASE_URL, DATABOT_TIMEOUT_SECONDS
        # DATABOT_ENABLED must default to False so shadow mode is opt-in
        assert DATABOT_ENABLED is False
        # Base URL must default to empty (not a live host)
        assert DATABOT_BASE_URL == ""
        # Timeout must be a positive integer
        assert isinstance(DATABOT_TIMEOUT_SECONDS, int)
        assert DATABOT_TIMEOUT_SECONDS > 0
