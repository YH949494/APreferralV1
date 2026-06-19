"""
Databot HTTP client — Phase 1 (shadow / read-only).

All network calls are fire-and-observe only.  APReferral never blocks on
Databot: every caller catches DatabotUnavailableError and falls back to
existing local logic.
"""
import logging

import requests

from config import (
    DATABOT_API_KEY,
    DATABOT_BASE_URL,
    DATABOT_ENABLED,
    DATABOT_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


class DatabotUnavailableError(Exception):
    """Raised when Databot is disabled, misconfigured, or unreachable."""


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {DATABOT_API_KEY}",
        "Content-Type": "application/json",
    }


def _check_enabled() -> None:
    if not DATABOT_ENABLED:
        raise DatabotUnavailableError("Databot integration is disabled (DATABOT_ENABLED=false)")
    if not DATABOT_BASE_URL:
        raise DatabotUnavailableError("DATABOT_BASE_URL is not configured")


def databot_get(path: str) -> dict:
    """
    Issue a GET request to the Databot API.

    Raises DatabotUnavailableError on any failure so callers can fall back
    cleanly without extra try/except nesting.
    """
    _check_enabled()
    url = f"{DATABOT_BASE_URL}{path}"
    logger.info("[DATABOT] databot_request_started method=GET path=%s", path)
    try:
        resp = requests.get(url, headers=_headers(), timeout=DATABOT_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "[DATABOT] databot_request_success method=GET path=%s status=%s",
            path,
            resp.status_code,
        )
        return data
    except requests.exceptions.Timeout as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=GET path=%s reason=timeout err=%s",
            path,
            exc,
        )
        raise DatabotUnavailableError(f"Databot timeout on GET {path}: {exc}") from exc
    except requests.exceptions.JSONDecodeError as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=GET path=%s reason=invalid_json err=%s",
            path,
            exc,
        )
        raise DatabotUnavailableError(f"Databot returned non-JSON on GET {path}: {exc}") from exc
    except requests.exceptions.HTTPError as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=GET path=%s reason=http_error err=%s",
            path,
            exc,
        )
        raise DatabotUnavailableError(f"Databot HTTP error on GET {path}: {exc}") from exc
    except Exception as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=GET path=%s reason=%s err=%s",
            path,
            type(exc).__name__,
            exc,
        )
        raise DatabotUnavailableError(f"Databot unexpected error on GET {path}: {exc}") from exc


def databot_post(path: str, payload: dict) -> dict:
    """
    Issue a POST request to the Databot API (used for preview/estimate endpoints
    that accept filter parameters in the body).

    Same error contract as databot_get.
    """
    _check_enabled()
    url = f"{DATABOT_BASE_URL}{path}"
    logger.info("[DATABOT] databot_request_started method=POST path=%s", path)
    try:
        resp = requests.post(
            url, json=payload, headers=_headers(), timeout=DATABOT_TIMEOUT_SECONDS
        )
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "[DATABOT] databot_request_success method=POST path=%s status=%s",
            path,
            resp.status_code,
        )
        return data
    except requests.exceptions.Timeout as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=POST path=%s reason=timeout err=%s",
            path,
            exc,
        )
        raise DatabotUnavailableError(f"Databot timeout on POST {path}: {exc}") from exc
    except requests.exceptions.JSONDecodeError as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=POST path=%s reason=invalid_json err=%s",
            path,
            exc,
        )
        raise DatabotUnavailableError(f"Databot returned non-JSON on POST {path}: {exc}") from exc
    except requests.exceptions.HTTPError as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=POST path=%s reason=http_error err=%s",
            path,
            exc,
        )
        raise DatabotUnavailableError(f"Databot HTTP error on POST {path}: {exc}") from exc
    except Exception as exc:
        logger.warning(
            "[DATABOT] databot_request_failed method=POST path=%s reason=%s err=%s",
            path,
            type(exc).__name__,
            exc,
        )
        raise DatabotUnavailableError(f"Databot unexpected error on POST {path}: {exc}") from exc
