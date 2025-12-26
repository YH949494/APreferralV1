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
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List
from xp import grant_xp

KL_TZ = timezone(timedelta(hours=8))

logger = logging.getLogger(__name__)

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@advantplayofficial")

# Base referral reward per successful referred user.
BASE_REFERRAL_XP = int(os.getenv("REFERRAL_REWARD_XP", "50"))

# Bonus is awarded every ``REFERRAL_BONUS_INTERVAL`` successful referrals.
REFERRAL_BONUS_INTERVAL = int(os.getenv("REFERRAL_BONUS_INTERVAL", "3"))
REFERRAL_BONUS_XP = int(os.getenv("REFERRAL_BONUS_XP", "300"))

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

def grant_referral_rewards(
    db, users_collection, referrer_id: int | None, invitee_user_id: int
) -> Dict[str, int]:
    """Grant base and bonus XP for a successful referral.

    Returns a dict with ``base_granted`` (0/1) and ``bonuses_awarded`` counts.
    The base award is idempotent via ``unique_key=ref_success:<referred>``.
    Bonus XP is granted for every interval reached (e.g., every 3 referrals).
    Missing bonuses are backfilled if earlier attempts were skipped.
    """

    result = {"base_granted": 0, "bonuses_awarded": 0}

    if not referrer_id or referrer_id == invitee_user_id:
        return result

    unique_key = f"{REFERRAL_SUCCESS_EVENT}:{invitee_user_id}"
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
            invitee_user_id,
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

def reconcile_referrals(
    referrals: Iterable[Dict],
    xp_events: Iterable[Dict],
) -> List[Dict]:
    """Compute referral XP mismatches for the provided datasets.

    ``referrals`` must contain documents with ``referrer_user_id`` (or legacy
    ``referrer_id``) and ``invitee_user_id`` (or legacy ``referred_user_id``)
    fields. Any entries explicitly marked as pending/inactive are excluded.
    """

    referral_counts: Dict[int, List[int]] = {}
    for ref in referrals:
        if ref.get("status") in {"pending", "inactive"}:
            continue
        referrer_id = ref.get("referrer_user_id") or ref.get("referrer_id")
        referred_user_id = ref.get("invitee_user_id") or ref.get("referred_user_id")
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

    invitee_migration = referrals_collection.update_many(
        {
            "$or": [
                {"invitee_user_id": {"$exists": False}},
                {"invitee_user_id": None},
            ],
            "referred_user_id": {"$exists": True, "$ne": None},
        },
        [{"$set": {"invitee_user_id": "$referred_user_id"}}],
    )
    referrer_migration = referrals_collection.update_many(
        {
            "$or": [
                {"referrer_user_id": {"$exists": False}},
                {"referrer_user_id": None},
            ],
            "referrer_id": {"$exists": True, "$ne": None},
        },
        [{"$set": {"referrer_user_id": "$referrer_id"}}],
    )

    logger.info(
        "[Referral] migrated invitee_user_id on %s docs (matched=%s)",
        getattr(invitee_migration, "modified_count", 0),
        getattr(invitee_migration, "matched_count", 0),
    )
    logger.info(
        "[Referral] migrated referrer_user_id on %s docs (matched=%s)",
        getattr(referrer_migration, "modified_count", 0),
        getattr(referrer_migration, "matched_count", 0),
    )
    
    referrals_collection.create_index([("referrer_user_id", 1)])
    referrals_collection.create_index([("invitee_user_id", 1)], unique=True)
    referrals_collection.create_index([("created_at", -1)])    
    referrals_collection.create_index([("status", 1), ("created_at", -1)])


def _referral_time_expr():
    return {"$ifNull": ["$confirmed_at", {"$ifNull": ["$validated_at", "$created_at"]}]}


def _week_window_utc(reference: datetime | None = None):
    ref_local = (reference or datetime.now(timezone.utc)).astimezone(KL_TZ)
    start_local = (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_local = start_local + timedelta(days=7)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _month_window_utc(reference: datetime | None = None):
    ref_local = (reference or datetime.now(timezone.utc)).astimezone(KL_TZ)
    start_local = ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start_local.month == 12:
        end_local = start_local.replace(year=start_local.year + 1, month=1)
    else:
        end_local = start_local.replace(month=start_local.month + 1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def compute_referral_stats(referrals_collection, user_id: int, window=None):
    """Return awarded referral counts for a user."""

    base_match = {
        "$and": [
            {"$or": [
                {"status": {"$exists": False}},
                {"status": {"$nin": ["pending", "inactive"]}},
            ]},
            {"$or": [
                {"referrer_user_id": user_id},
                {"referrer_id": user_id},
            ]},
        ]
    }

    def _count_between(start, end):
        return referrals_collection.count_documents(
            {
                "$and": [
                    base_match,
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [_referral_time_expr(), start]},
                                {"$lt": [_referral_time_expr(), end]},
                            ]
                        }
                    },
                ]
            }
        )

    try:
        total = referrals_collection.count_documents(base_match)
        if window:
            start_utc, end_utc = window
            weekly = _count_between(start_utc, end_utc)
            monthly = weekly
        else:
            week_start, week_end = _week_window_utc()
            month_start, month_end = _month_window_utc()
            weekly = _count_between(week_start, week_end)
            monthly = _count_between(month_start, month_end)

        return {
            "total_referrals": total,
            "weekly_referrals": weekly,
            "monthly_referrals": monthly,
        }
    except Exception:
        logger.exception("[referral_stats] failed uid=%s", user_id)
        return {
            "total_referrals": 0,
            "weekly_referrals": 0,
            "monthly_referrals": 0,
        }
