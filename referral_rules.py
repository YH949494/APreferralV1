"""Referral XP rules and helpers.

This module centralizes the referral reward calculation so the Flask app, bots,
and maintenance scripts all agree on what constitutes a valid referral and how
much XP to award. All helpers are intentionally database-agnostic: callers
provide the collections they already use (``users``, ``referrals``,
``xp_events``) and any gating predicate (e.g., channel membership).
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List
from pymongo.errors import DuplicateKeyError
from config import KL_TZ
from xp import grant_xp, now_utc

logger = logging.getLogger(__name__)

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@advantplayofficial")

# Base referral reward per successful referred user.
BASE_REFERRAL_XP = int(os.getenv("REFERRAL_REWARD_XP", "30"))

# Bonus is awarded every ``REFERRAL_BONUS_INTERVAL`` successful referrals.
REFERRAL_BONUS_INTERVAL = int(os.getenv("REFERRAL_BONUS_INTERVAL", "3"))
REFERRAL_BONUS_XP = int(os.getenv("REFERRAL_BONUS_XP", "200"))

# Event types used across the app and reconciliation scripts.
REFERRAL_SUCCESS_EVENT = "ref_success"
REFERRAL_BONUS_EVENT = "ref_bonus_triplet"
REFERRAL_NEW_USER_EVENT = "ref_newuser"

def _get_db():
    try:
        from database import db as database_db
    except Exception:
        logger.exception("[referral][validate_channel] db_unavailable")
        return None
    return database_db


def try_validate_referral_by_channel(uid: int, is_member: bool) -> Dict[str, int]:
    """Validate pending referrals based on channel membership."""

    result = {"validated": 0, "counted": 0, "base_granted": 0, "bonuses_awarded": 0}
    if not uid:
        logger.info("[referral][validate_channel] skip reason=missing_uid")
        return result
    if not is_member:
        logger.info("[referral][validate_channel] skip uid=%s reason=not_member", uid)
        return result

    db = _get_db()
    if db is None:
        return result

    referrals_collection = db["referrals"]
    users_collection = db["users"]

    pending_ref = referrals_collection.find_one({"invitee_user_id": uid, "status": "pending"})
    if not pending_ref:
        return result

    referrer_id = pending_ref.get("referrer_user_id") or pending_ref.get("referrer_id")
    if not referrer_id:
        logger.info("[referral][validate_channel] skip uid=%s reason=no_referrer", uid)
        return result

    now = now_utc()
    update_result = upsert_referral_and_update_user_count(
        referrals_collection,
        users_collection,
        referrer_user_id=referrer_id,
        invitee_user_id=uid,
        success_at=now,
        referrer_username=None,
        context={"subscribed_official_at": now},
    )
    result["validated"] = 1
    result["counted"] = int(update_result.get("counted") or 0)
    if update_result.get("counted"):
        outcome = grant_referral_rewards(
            db,
            users_collection,
            referrer_id,
            uid,
        )
        result["base_granted"] = int(outcome.get("base_granted", 0))
        result["bonuses_awarded"] = int(outcome.get("bonuses_awarded", 0))
        referrer_doc = users_collection.find_one({"user_id": referrer_id}) or {}
        total_referrals = int(referrer_doc.get("referral_count", 0))
        logger.info(
            "[xp] uid=%s add=%s bonus=%s total_referrals=%s upsert=%s",
            referrer_id,
            outcome.get("base_granted"),
            outcome.get("bonuses_awarded"),
            total_referrals,
            "inserted" if update_result.get("upserted") else "updated",
        )
        logger.info(
            "[ref] uid=%s resolved_referrer=%s reason=channel_join",
            uid,
            referrer_id,
        )

    return result


async def try_validate_referral_by_channel_async(uid: int, is_member: bool) -> Dict[str, int]:
    """Executor wrapper for channel-based referral validation."""

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, try_validate_referral_by_channel, uid, is_member)


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


def upsert_referral_success(
    referrals_collection,
    referrer_user_id: int | None,
    invitee_user_id: int | None,
    success_at: datetime | None = None,
    referrer_username: str | None = None,
    context: Dict | None = None,
    *,
    chat_id: int | None = None,
    invite_link: str | None = None,    
) -> Dict[str, int | None]:
    """Idempotently mark a referral as successful for an invitee."""

    if not invitee_user_id:
        logger.info("[referral_drop] missing invitee_user_id referrer=%s", referrer_user_id)
        return {"matched": 0, "modified": 0, "upserted": None}
    if not referrer_user_id or referrer_user_id == invitee_user_id:
        logger.info(
            "[referral_drop] invalid referrer invitee=%s referrer=%s",
            invitee_user_id,
            referrer_user_id,
        )
        return {"matched": 0, "modified": 0, "upserted": None}

    now = success_at or now_utc()
    proof_fields = {}
    if context:
        for key in ("joined_chat_at", "subscribed_official_at"):
            value = context.get(key)
            if value is not None:
                proof_fields[key] = value
                
    kl_local = now.astimezone(KL_TZ)
    week_key = kl_local.strftime("%Y-%W")
    month_key = kl_local.strftime("%Y-%m")

    set_fields = {
        "status": "success",
        "success_at": now,
        "confirmed_at": now,
        "week_key": week_key,
        "month_key": month_key,
        "referred_user_id": invitee_user_id,
        "inviter_id": referrer_user_id,
        "invite_link": invite_link,
        "ts": now,
        **proof_fields,
    }
    if chat_id is not None:
        set_fields["chat_id"] = chat_id
    
    update = {
        "$setOnInsert": {
            "invitee_user_id": invitee_user_id,
            "referrer_user_id": referrer_user_id,
            "referrer_username": referrer_username,
            "created_at": now,
        },
        "$set": set_fields,
    }
    if chat_id is not None:
        update["$setOnInsert"]["chat_id"] = chat_id
    if invitee_user_id is not None:
        update["$setOnInsert"]["referred_user_id"] = invitee_user_id

    if chat_id is not None:
        upsert_filter = {"chat_id": chat_id, "referred_user_id": invitee_user_id}
    else:
        upsert_filter = {"referred_user_id": invitee_user_id}
        
    result = referrals_collection.update_one(
        upsert_filter,
        update,
        upsert=True,
    )

    matched = int(getattr(result, "matched_count", 0))
    modified = int(getattr(result, "modified_count", 0))
    upserted = getattr(result, "upserted_id", None)
    logger.info(
        "[referral_success] invitee=%s referrer=%s matched=%s modified=%s upserted=%s",
        invitee_user_id,
        referrer_user_id,
        matched,
        modified,
        upserted,
    )
    return {"matched": matched, "modified": modified, "upserted": upserted}

def upsert_referral_and_update_user_count(
    referrals_collection,
    users_collection,
    referrer_user_id: int | None,
    invitee_user_id: int | None,
    success_at: datetime | None = None,
    referrer_username: str | None = None,
    context: Dict | None = None,
    *,
    chat_id: int | None = None,
    invite_link: str | None = None,    
) -> Dict[str, int | None]:
    """Upsert a successful referral and increment user counts once."""

    existing = None
    if invitee_user_id:
        existing = referrals_collection.find_one(
            {
                "$or": [
                    {"invitee_user_id": invitee_user_id},
                    {"referred_user_id": invitee_user_id},
                ]
            },
            {"status": 1, "valid_counted": 1, "referrer_user_id": 1, "referrer_id": 1},
        )

    result = upsert_referral_success(
        referrals_collection,
        referrer_user_id=referrer_user_id,
        invitee_user_id=invitee_user_id,
        success_at=success_at,
        referrer_username=referrer_username,
        context=context,
        chat_id=chat_id,
        invite_link=invite_link,        
    )
    counted = 0

    if not invitee_user_id or not referrer_user_id or referrer_user_id == invitee_user_id:
        return {**result, "counted": counted}

    should_count = False
    if existing is None:
        should_count = True
    else:
        prior_status = existing.get("status")
        prior_counted = existing.get("valid_counted")
        prior_referrer = existing.get("referrer_user_id") or existing.get("referrer_id")
        if prior_referrer and prior_referrer != referrer_user_id:
            should_count = False
        elif prior_status in {None, "pending", "inactive"}:
            should_count = True
        elif prior_counted is False:
            should_count = True

    if not should_count:
        return {**result, "counted": counted}

    count_filter = {
        "$and": [
            {
                "$or": [
                    {"invitee_user_id": invitee_user_id},
                    {"referred_user_id": invitee_user_id},
                ]
            },
            {
                "$or": [
                    {"referrer_user_id": referrer_user_id},
                    {"referrer_id": referrer_user_id},
                ]
            },
            {"status": "success"},
            {
                "$or": [
                    {"valid_counted": {"$exists": False}},
                    {"valid_counted": False},
                ]
            },
        ]
    }
    if chat_id is not None:
        count_filter["$and"].append({"chat_id": chat_id})
    
    count_update = referrals_collection.update_one(
        count_filter,
        {"$set": {"valid_counted": True, "counted_at": now_utc()}},
    )

    if int(getattr(count_update, "modified_count", 0)):
        for user_id in (referrer_user_id, invitee_user_id):
            if not user_id:
                continue
            users_collection.update_one(
                {"user_id": user_id},
                {"$setOnInsert": {"user_id": user_id}},
                upsert=True,
            )        
        users_collection.update_one(
            {"user_id": referrer_user_id},
            {
                "$inc": {"referral_count": 1, "weekly_referral_count": 1},
            },
        )
        counted = 1

    return {**result, "counted": counted}

def record_referral_success(
    referrals_collection,
    invitee_user_id: int | None,
    referrer_user_id: int | None,
    referrer_username: str | None = None,
    context: Dict | None = None,
) -> Dict[str, int | None]:
    """Idempotently mark a referral as successful for an invitee."""

    return upsert_referral_success(
        referrals_collection,
        referrer_user_id=referrer_user_id,
        invitee_user_id=invitee_user_id,
        success_at=now_utc(),
        referrer_username=referrer_username,
        context=context,
    )

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
        logger.info(
            "[Referral] Invalid referrer for invitee=%s referrer=%s",
            invitee_user_id,
            referrer_id,
        )        
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

    xp_delta = result["base_granted"] * BASE_REFERRAL_XP + result["bonuses_awarded"] * REFERRAL_BONUS_XP
    logger.info(
        "[Referral] XP awarded invitee=%s referrer=%s base=%s bonuses=%s xp_delta=%s total_referrals=%s",
        invitee_user_id,
        referrer_id,
        result["base_granted"],
        result["bonuses_awarded"],
        xp_delta,
        total_referrals,
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


def cleanup_null_referred_user_ids(
    referrals_collection,
    *,
    delete: bool = False,
) -> int:
    query = {"referred_user_id": {"$type": "null"}}
    count = referrals_collection.count_documents(query)
    if not count:
        return 0
    if delete:
        result = referrals_collection.delete_many(query)
        deleted = int(getattr(result, "deleted_count", 0))
        logger.warning("[Referral][cleanup] deleted referrals with null referred_user_id=%s", deleted)
        return deleted
    logger.warning("[Referral][warn] found referrals with null referred_user_id=%s", count)
    return count


def ensure_referral_indexes(referrals_collection, *, chat_id: int | None = None):
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


    referred_migration = referrals_collection.update_many(
        {
            "$or": [
                {"referred_user_id": {"$exists": False}},
                {"referred_user_id": None},
            ],
            "invitee_user_id": {"$exists": True, "$ne": None},
        },
        [{"$set": {"referred_user_id": "$invitee_user_id"}}],
    )
    logger.info(
        "[Referral] migrated referred_user_id on %s docs (matched=%s)",
        getattr(referred_migration, "modified_count", 0),
        getattr(referred_migration, "matched_count", 0),
    )

    if chat_id is not None:
        chat_migration = referrals_collection.update_many(
            {
                "$or": [
                    {"chat_id": {"$exists": False}},
                    {"chat_id": None},
                ]
            },
            {"$set": {"chat_id": chat_id}},
        )
        logger.info(
            "[Referral] migrated chat_id on %s docs (matched=%s)",
            getattr(chat_migration, "modified_count", 0),
            getattr(chat_migration, "matched_count", 0),
        )

    cleanup_null_referred_user_ids(referrals_collection, delete=False)

    for ix in referrals_collection.list_indexes():
        if ix.get("key") == {"referred_user_id": 1} and ix.get("unique"):
            try:
                referrals_collection.drop_index(ix["name"])
                logger.info("[Referral][index] dropped legacy referred_user_id index=%s", ix["name"])
            except Exception:
                logger.exception(
                    "[Referral][index] failed dropping legacy referred_user_id index=%s",
                    ix.get("name"),
                )
                
    referrals_collection.create_index([("referrer_user_id", 1)])
    try:
        referrals_collection.create_index(
            [("chat_id", 1), ("referred_user_id", 1)],
            unique=True,
            name="uq_chat_referred_user_id",
            partialFilterExpression={
                "chat_id": {"$type": "number"},
                "referred_user_id": {"$type": "number"},
            },
        )
    except DuplicateKeyError:
        logger.warning("[Referral][index] duplicate_detected chat_id/referred_user_id")
    except Exception:
        logger.exception("[Referral][index] failed chat_id/referred_user_id index") 
    invitee_index_ok = False    
    try:
        referrals_collection.create_index(
            [("invitee_user_id", 1)],
            unique=True,
            name="uq_invitee_user_id_not_null",
            partialFilterExpression={"invitee_user_id": {"$type": "number"}},
        )
        invitee_index_ok = True        
    except DuplicateKeyError:
        logger.warning("[index] duplicate_detected invitee_user_id null/dup")
    except Exception:
        logger.exception("[index] duplicate_detected invitee_user_id null/dup")
    if not invitee_index_ok:
        referrals_collection.create_index([("invitee_user_id", 1)])
    referrals_collection.create_index([("created_at", -1)])
    referrals_collection.create_index([("status", 1)])
    referrals_collection.create_index([("status", 1), ("created_at", -1)])


def _referral_time_expr():
    return {
        "$ifNull": [
            "$success_at",
            {"$ifNull": ["$confirmed_at", {"$ifNull": ["$validated_at", "$created_at"]}]},
        ]
    }

def _week_window_utc(reference: datetime | None = None):
    ref_local = (reference or datetime.now(timezone.utc)).astimezone(KL_TZ)
    start_local = (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_local = start_local + timedelta(days=7)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    logger.info(
        "[week_window] local_start=%s local_end=%s utc_start=%s utc_end=%s",
        start_local.isoformat(),
        end_local.isoformat(),
        start_utc.isoformat(),
        end_utc.isoformat(),
    )
    return start_utc, end_utc

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
            {"status": "success"},
            {"$or": [
                {"referrer_user_id": user_id},
                {"referrer_id": user_id},
            ]},
            {"$or": [
                {"invitee_user_id": {"$exists": True, "$ne": None}},
                {"referred_user_id": {"$exists": True, "$ne": None}},
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

        stats = {
            "total_referrals": total,
            "weekly_referrals": weekly,
            "monthly_referrals": monthly,
        }
        logger.info(
            "[referral_stats] uid=%s total=%s weekly=%s monthly=%s",
            user_id,
            total,
            weekly,
            monthly,
        )
        return stats
        
    except Exception:
        logger.exception("[referral_stats] failed uid=%s", user_id)
        return {
            "total_referrals": 0,
            "weekly_referrals": 0,
            "monthly_referrals": 0,
        }
