"""Canonical multi-account-risk modifier for voucher probability gating.

Single source of truth for turning a segment-derived BASE voucher
probability into a FINAL, risk-adjusted probability when a Telegram
identity carries ``multi_account_risk``. This module is consumed directly
by ``vouchers.assign_public_pool_access_once`` -- the actual claim-time
gate that decides real voucher allocation -- so there is exactly one
implementation of "how does multi-account risk change the odds a user
was already going to get for their segment", not one copy here and
another, independently maintained, copy in Databot's dashboard.

Databot's dashboard (``app/services/voucher_eligibility.py`` in the
Databot repo) reads the SAME ``voucher_risk_modifiers`` / ``pool_
probabilities`` documents from the shared ``app_settings`` Mongo
collection this module reads via ``settings_service`` -- so the
probability a dashboard shows for a given Telegram user is always
computed from the identical configuration this live gate uses, never a
second hardcoded table that can drift out of sync.

Deliberately does NOT resolve a canonical segment or a base probability
itself -- callers (``vouchers.assign_public_pool_access_once``) keep
doing that exactly as before, from whichever segment source they already
trust (``for_bot_segment`` normally, ``backend_segment`` when populated).
This keeps canonical behavioral segmentation and multiple-account risk
independent: this module only ever multiplies/clamps an already-resolved
``base_probability``, and it never mutates ``for_bot_segment`` or any
other segment field.

``multi_account_risk`` must be the value already synced onto
``users.multi_account_risk`` (Telegram-level, written exclusively by
Databot's ``segment_sync_job.py`` Path B fan-out from canonical 90-day
snapshot evidence) -- never the unbounded all-time linked-account
diagnostic, and never derived here by joining a Telegram identity to a
gaming/UIM account id directly.
"""
from __future__ import annotations

import logging
from typing import Any

from settings_service import get_setting

logger = logging.getLogger(__name__)

RISK_CATEGORY_NONE = "none"
RISK_CATEGORY_MULTI_ACCOUNT_ONLY = "multi_account_only"
RISK_CATEGORY_BEHAVIORAL_AND_MULTI_ACCOUNT = "behavioral_and_multi_account"

SETTINGS_GROUP = "voucher_risk_modifiers"

# Kept in sync with settings_service.SETTINGS_SCHEMA["voucher_risk_modifiers"]
# defaults -- used both as the schema default AND as the safe fallback when a
# stored value is missing, non-numeric, or out of the valid [0, 100] range.
_DEFAULTS: dict[str, float] = {
    "multi_account_only_modifier_pct": 25.0,
    "behavioral_and_multi_account_modifier_pct": 100.0,
    "behavioral_and_multi_account_min_pct": 5.0,
    "behavioral_and_multi_account_max_pct": 10.0,
}


def _safe_pct(field: str) -> float:
    """Read a 0-100 percentage setting, failing safely to the documented
    default on ANY missing/invalid/out-of-range value. A malformed stored
    value (e.g. 250, -1, non-numeric text) must never silently change
    claim behaviour -- it is logged and the default is used instead."""
    default = _DEFAULTS[field]
    try:
        raw = get_setting(SETTINGS_GROUP, field)
    except Exception:
        logger.exception("[VOUCHER_RISK] settings lookup failed field=%s, using default=%s", field, default)
        return default
    if raw is None:
        return default
    try:
        val = float(raw)
    except (TypeError, ValueError):
        logger.warning("[VOUCHER_RISK] non-numeric %s=%r, using default=%s", field, raw, default)
        return default
    if val != val or val < 0.0 or val > 100.0:  # NaN, or outside [0, 100]
        logger.warning("[VOUCHER_RISK] out-of-range %s=%r, using default=%s", field, raw, default)
        return default
    return val


def apply_risk_modifier(
    base_probability: float,
    *,
    behavioral_voucher_hunter: bool,
    multi_account_risk: bool,
) -> dict[str, Any]:
    """Apply the multi-account-risk modifier to an already-resolved
    segment base probability. Never touches canonical segment; only
    returns a (possibly) reduced final probability plus the modifier and
    reason that produced it.
    """
    try:
        base_probability = max(0.0, min(1.0, float(base_probability)))
    except (TypeError, ValueError):
        base_probability = 0.0
    is_behavioral_vh = bool(behavioral_voucher_hunter)
    has_risk = bool(multi_account_risk)

    if not has_risk:
        return {
            "base_probability": base_probability,
            "risk_modifier": 1.0,
            "final_probability": base_probability,
            "risk_category": RISK_CATEGORY_NONE,
            "gate_reason": "No multiple-account risk; segment-derived probability applies.",
        }

    if not is_behavioral_vh:
        modifier = _safe_pct("multi_account_only_modifier_pct") / 100.0
        final_probability = round(base_probability * modifier, 6)
        return {
            "base_probability": base_probability,
            "risk_modifier": modifier,
            "final_probability": final_probability,
            "risk_category": RISK_CATEGORY_MULTI_ACCOUNT_ONLY,
            "gate_reason": f"Linked to a qualifying multiple-account cluster; probability reduced by ×{modifier:g}.",
        }

    modifier = _safe_pct("behavioral_and_multi_account_modifier_pct") / 100.0
    lo = _safe_pct("behavioral_and_multi_account_min_pct") / 100.0
    hi = _safe_pct("behavioral_and_multi_account_max_pct") / 100.0
    if lo > hi:
        logger.warning("[VOUCHER_RISK] min_pct > max_pct, falling back to schema defaults for the band")
        lo = _DEFAULTS["behavioral_and_multi_account_min_pct"] / 100.0
        hi = _DEFAULTS["behavioral_and_multi_account_max_pct"] / 100.0
    raw = base_probability * modifier
    final_probability = round(min(max(raw, lo), hi), 6)
    return {
        "base_probability": base_probability,
        "risk_modifier": modifier,
        "final_probability": final_probability,
        "risk_category": RISK_CATEGORY_BEHAVIORAL_AND_MULTI_ACCOUNT,
        "gate_reason": (
            "Behavioral Voucher Hunter AND multiple-account risk; strongest configured "
            f"restriction applied ({lo:.0%}-{hi:.0%})."
        ),
    }
