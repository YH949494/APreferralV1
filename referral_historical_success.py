"""Historical-success guard for referral attribution.

Before a new pending referral is created for an invitee, the caller must
know whether that invitee has *ever* had a successful referral in the
past (in either destination), so a fresh pending row can never be created
for someone who already has a completed referral on record. Because this
check spans three independent collections (`qualified_events`,
`referral_events`, `referral_award_events`), any one of which can fail a
lookup (network blip, replica-set failover, etc.), the result is
deliberately tri-state rather than a plain boolean: a lookup failure must
be distinguishable from "no history found" so the caller can fail closed
instead of treating a database outage as a clean invitee.
"""

from __future__ import annotations

import logging
from enum import Enum

logger = logging.getLogger(__name__)


class HistoricalSuccessResult(str, Enum):
    FOUND = "found"
    NOT_FOUND = "not_found"
    LOOKUP_FAILED = "lookup_failed"


def has_historical_success(db, *, invitee_user_id: int) -> HistoricalSuccessResult:
    """Check whether ``invitee_user_id`` has any prior successful referral.

    Returns ``FOUND`` if a historical success is on record, ``NOT_FOUND``
    if none of the three sources have one, or ``LOOKUP_FAILED`` if any of
    the underlying queries raised — callers must treat ``LOOKUP_FAILED``
    as "unknown, do not proceed", never as "not found".
    """
    try:
        if db.qualified_events.find_one({"invitee_id": invitee_user_id}, {"_id": 1}):
            return HistoricalSuccessResult.FOUND
    except Exception:
        logger.exception(
            "[REFERRAL][HISTORY] qualified_events_lookup_failed invitee=%s",
            invitee_user_id,
        )
        return HistoricalSuccessResult.LOOKUP_FAILED

    try:
        if db.referral_events.find_one(
            {"invitee_id": invitee_user_id, "event": "referral_settled"}, {"_id": 1}
        ):
            return HistoricalSuccessResult.FOUND
    except Exception:
        logger.exception(
            "[REFERRAL][HISTORY] referral_events_lookup_failed invitee=%s",
            invitee_user_id,
        )
        return HistoricalSuccessResult.LOOKUP_FAILED

    try:
        if db.referral_award_events.find_one(
            {"invitee_user_id": invitee_user_id}, {"_id": 1}
        ):
            return HistoricalSuccessResult.FOUND
    except Exception:
        logger.exception(
            "[REFERRAL][HISTORY] referral_award_events_lookup_failed invitee=%s",
            invitee_user_id,
        )
        return HistoricalSuccessResult.LOOKUP_FAILED

    return HistoricalSuccessResult.NOT_FOUND
