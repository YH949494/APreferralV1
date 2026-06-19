"""
Databot read-only service layer — Phase 1 (shadow / read-only).

Each public function:
  1. Calls Databot in shadow mode (observe-only — no side effects on APReferral).
  2. Returns None (or an explicit fallback value) when Databot is unavailable.
  3. Logs fallback_used so operators can monitor shadow coverage.

IMPORTANT: Nothing in APReferral may gate voucher eligibility, segment
assignment, or claim probability on the return values from this module yet.
That coupling is deferred to a later phase.
"""
import logging
from typing import Optional

from databot_client import DatabotUnavailableError, databot_get, databot_post

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fallback(operation: str, default=None):
    logger.info("[DATABOT] fallback_used operation=%s", operation)
    return default


# ---------------------------------------------------------------------------
# Public service methods
# ---------------------------------------------------------------------------

def get_user_segment(account_id: str) -> Optional[dict]:
    """
    Fetch the Databot-computed segment for a single account.

    Returns a dict like::

        {
            "account_id": "...",
            "segment": "high_value",
            "confidence": 0.87,
            "computed_at": "2026-06-19T10:00:00Z"
        }

    Returns None when Databot is unavailable — APReferral must continue
    using its existing local segment (bot_segment / backend_segment).
    """
    try:
        return databot_get(f"/api/v1/segments/user/{account_id}")
    except DatabotUnavailableError:
        return _fallback("get_user_segment")


def get_segment_probability_config() -> Optional[dict]:
    """
    Fetch Databot's recommended probability weights per segment.

    Returns a dict like::

        {
            "high_value": 55,
            "normal_actual": 32,
            ...
        }

    Returns None when Databot is unavailable — APReferral must continue
    using SEGMENT_PROBABILITY_CONFIG from config.py.
    """
    try:
        return databot_get("/api/v1/segments/probability-config")
    except DatabotUnavailableError:
        return _fallback("get_segment_probability_config")


def get_segment_roi_summary() -> Optional[dict]:
    """
    Fetch Databot's ROI summary across all segments.

    Returns a dict like::

        {
            "generated_at": "2026-06-19T10:00:00Z",
            "segments": {
                "high_value": {"avg_roi": 4.2, "avg_spend": 1500.0},
                ...
            }
        }

    Returns None when Databot is unavailable.
    """
    try:
        return databot_get("/api/v1/segments/roi-summary")
    except DatabotUnavailableError:
        return _fallback("get_segment_roi_summary")


def get_campaign_preview(campaign_params: dict) -> Optional[dict]:
    """
    Request a campaign audience estimate from Databot.

    ``campaign_params`` is passed as-is to Databot — typical keys::

        {
            "segments": ["high_value", "normal_actual"],
            "player_age_types": ["old_player"],
            "voucher_value": 10.0
        }

    Returns a dict like::

        {
            "audience_size": 1240,
            "expected_voucher_cost": 12400.0,
            "segment_distribution": {...}
        }

    Returns None when Databot is unavailable — APReferral continues using
    its own campaign_engine.preview_audience().
    """
    try:
        return databot_post("/api/v1/campaigns/preview", campaign_params)
    except DatabotUnavailableError:
        return _fallback("get_campaign_preview")
