"""Referral XP rules and helpers.

This module centralizes the referral reward calculation so the Flask app, bots,
and maintenance scripts all agree on what constitutes a valid referral and how
much XP to award. All helpers are intentionally database-agnostic: callers
provide the collections they already use (``users``, ``referrals``,
``xp_events``) and any gating predicate (e.g., channel membership).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Callable, Dict, Iterable, List

from xp import grant_xp


logger = logging.getLogger(__name__)

# Base referral reward per successful referred user.
BASE_REFERRAL_XP = int(os.getenv("REFERRAL_REWARD_XP", "30"))

# Bonus is awarded every ``REFERRAL_BONUS_INTERVAL`` successful referrals.
REFERRAL_BONUS_INTERVAL = int(os.getenv("REFERRAL_BONUS_INTERVAL", "3"))
REFERRAL_BONUS_XP = int(os.getenv("REFERRAL_BONUS_XP", "200"))

# Event types used across the app and reconciliation scripts.
REFERRAL_SUCCESS_EVENT = "ref_success"
REFERRAL_BONUS_EVENT = "ref_bonus_triplet"
REFERRAL_NEW_USER_EVENT = "ref_newuser"


def expected_referral_xp(referral_count: int) -> int:
    """Return the expected XP for ``referral_count`` successful referrals."""

    bonus_chunks = referral_count // REFERRAL_BONUS_INTERVAL
    return referral_count * BASE_REFERRAL_XP + bonus_chunks * REFERRAL_BONUS_XP


def current_bonus_count(xp_events_collection, user_id: int) -> int:
    """Count awarded referral bonus events (legacy + new schema)."""

    return xp_events_collection.count_documents(
        {
            "user_id": user_id,
            "type": {"$in": ["ref_bonus", REFERRAL_BONUS_EVENT]},
        }
    )


def bonus_events_missing(referral_count: int, awarded_bonus_events: int) -> int:
    """Return how many bonus events should exist based on ``referral_count``."""

    expected_bonus_events = referral_count // REFERRAL_BONUS_INTERVAL
    return max(0, expected_bonus_events - awarded_bonus_events)


def _apply_update(update):
    """Utility used in fake collections during unit tests."""

    sets = update.get("$set", {})
    set_on_insert = update.get("$setOnInsert", {})
    incs = update.get("$inc", {})
    return sets, set_on_insert, incs


def record_pending_referral(referrals_collection, referrer_id: int | None, referred_user_id: int):
    """Upsert a pending referral record."""

    if not referrer_id or referrer_id == referred_user_id:
        return

    now = datetime.utcnow()
    referrals_collection.update_one(
        {"referred_user_id": referred_user_id},
        {
            "$setOnInsert": {
                "referrer_id": referrer_id,
                "referred_user_id": referred_user_id,
                "status": "pending",
                "created_at": now,
            }
        },
        upsert=True,
    )


def grant_referral_rewards(
    db, users_collection, referrer_id: int | None, referred_user_id: int
) -> Dict[str, int]:
    """Grant base and bonus XP for a successful referral.

    Returns a dict with ``base_granted`` (0/1) and ``bonuses_awarded`` counts.
    The base award is idempotent via ``unique_key=ref_success:<referred>``.
    Bonus XP is granted for every interval reached (e.g., every 3 referrals).
    Missing bonuses are backfilled if earlier attempts were skipped.
    """

    result = {"base_granted": 0, "bonuses_awarded": 0}

    if not referrer_id or referrer_id == referred_user_id:
        return result

    unique_key = f"{REFERRAL_SUCCESS_EVENT}:{referred_user_id}"
    granted = grant_xp(
        db,
        referrer_id,
        REFERRAL_SUCCESS_EVENT,
        unique_key,
        BASE_REFERRAL_XP,
    )
    if not granted:
        logger.info(
            "[Referral] Duplicate base grant ignored referrer=%s referred=%s key=%s",
            referrer_id,
            referred_user_id,
            unique_key,
        )
        return result

    result["base_granted"] = 1

    users_collection.update_one(
        {"user_id": referrer_id},
        {
            "$inc": {"referral_count": 1, "weekly_referral_count": 1},
            "$setOnInsert": {"user_id": referrer_id},
        },
        upsert=True,
    )

    referrer = users_collection.find_one({"user_id": referrer_id}) or {}
    total_referrals = int(referrer.get("referral_count", 0))

    awarded_bonus_events = current_bonus_count(db.xp_events, referrer_id)
    missing = bonus_events_missing(total_referrals, awarded_bonus_events)
    result["bonuses_awarded"] = missing

    for i in range(missing):
        # Award sequential bonus indexes starting from already-awarded count
        bonus_index = awarded_bonus_events + i + 1
        bonus_key = f"{REFERRAL_BONUS_EVENT}:{bonus_index}"
        grant_xp(
            db,
            referrer_id,
            REFERRAL_BONUS_EVENT,
            bonus_key,
            REFERRAL_BONUS_XP,
        )

    return result


def validate_referral_if_eligible(
    db,
    referrals_collection,
    users_collection,
    referred_user_id: int,
    membership_check: Callable[[int], bool],
):
    """Transition a pending referral to success if gates pass.

    ``membership_check`` must be a callable that returns ``True`` when the user
    satisfies the production gate (e.g., channel membership). The gate is reused
    for both the referral status update and XP grant to avoid drift.
    """

    ref_doc = referrals_collection.find_one(
        {"referred_user_id": referred_user_id, "status": "pending"}
    )
    if not ref_doc:
        return False

    if not membership_check(referred_user_id):
        return False

    updated = referrals_collection.update_one(
        {"_id": ref_doc.get("_id"), "status": "pending"},
        {"$set": {"status": "success", "validated_at": datetime.utcnow()}},
    )

    if getattr(updated, "modified_count", 0) != 1:
        return False

    outcome = grant_referral_rewards(
        db, users_collection, ref_doc.get("referrer_id"), referred_user_id
    )
    return bool(outcome.get("base_granted"))


def reconcile_referrals(
    referrals: Iterable[Dict],
    xp_events: Iterable[Dict],
) -> List[Dict]:
    """Compute referral XP mismatches for the provided datasets.

    ``referrals`` must contain documents with ``referrer_id`` and ``referred_user_id``
    fields and ``status == 'success'``. ``xp_events`` are xp_events documents.
    """

    referral_counts: Dict[int, List[int]] = {}
    for ref in referrals:
        if ref.get("status") != "success":
            continue
        referrer_id = ref.get("referrer_id")
        referred_user_id = ref.get("referred_user_id")
        if referrer_id is None or referred_user_id is None:
            continue
        referral_counts.setdefault(referrer_id, []).append(referred_user_id)

    xp_totals: Dict[int, Dict] = {}
    for ev in xp_events:
        if ev.get("type") not in {REFERRAL_SUCCESS_EVENT, REFERRAL_BONUS_EVENT, "ref_bonus"}:
            continue
        uid = ev.get("user_id")
        if uid is None:
            continue
        entry = xp_totals.setdefault(uid, {"xp": 0, "events": []})
        entry["xp"] += int(ev.get("xp", 0))
        entry["events"].append({"type": ev.get("type"), "uk": ev.get("unique_key"), "xp": ev.get("xp")})

    mismatches: List[Dict] = []
    all_users = set(referral_counts.keys()) | set(xp_totals.keys())
    for uid in sorted(all_users):
        refs = referral_counts.get(uid, [])
        ref_count = len(set(refs))  # dedupe referred_user_id to avoid double-counting
        expected = expected_referral_xp(ref_count)
        actual = xp_totals.get(uid, {}).get("xp", 0)

        if expected != actual:
            mismatches.append(
                {
                    "user_id": uid,
                    "ref_count": ref_count,
                    "expected_xp": expected,
                    "xp_from_referrals": actual,
                    "delta": expected - actual,
                    "sample_refs": refs[:5],
                    "events": xp_totals.get(uid, {}).get("events", []),
                }
            )

    return mismatches


def ensure_referral_indexes(referrals_collection):
    """Create indexes used for referral workflows."""

    referrals_collection.create_index([("referrer_id", 1), ("status", 1)])
    referrals_collection.create_index([("referred_user_id", 1)], unique=True)
    referrals_collection.create_index([("status", 1), ("created_at", -1)])

    try:
        referrals_collection.create_index(
            [("referrer_id", 1), ("referred_user_id", 1)],
            name="uq_ref_success",
            unique=True,
            partialFilterExpression={"status": "success"},
        )
    except Exception:
        logger.exception("[Referral] Failed to ensure partial unique index for successes")
