from datetime import datetime, timedelta, timezone
import logging
import os
import pytz
import requests
from pymongo import ReturnDocument
from database import db
from referral_rules import calc_referral_award

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GROUP_ID = int(os.environ.get("GROUP_ID", "-1002304653063"))
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
REFERRAL_HOLD_HOURS = 12
TG_VERIFY_TTL_MINUTES = int(os.environ.get("TG_VERIFY_TTL_MINUTES", "30"))

_RAW_OFFICIAL_CHANNEL_ID = os.getenv("OFFICIAL_CHANNEL_ID")
try:
    OFFICIAL_CHANNEL_ID = int(_RAW_OFFICIAL_CHANNEL_ID) if _RAW_OFFICIAL_CHANNEL_ID not in (None, "") else None
except (TypeError, ValueError):
    OFFICIAL_CHANNEL_ID = None
OFFICIAL_CHANNEL_USERNAME = os.getenv("OFFICIAL_CHANNEL_USERNAME", "").strip()

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

logger = logging.getLogger(__name__)
subscription_cache_col = db["subscription_cache"]
profile_photo_cache_col = db["profile_photo_cache"]
tg_verification_queue_col = db["tg_verification_queue"]
system_flags_col = db["system_flags"]

ALLOWED_CHANNEL_STATUSES = {"member", "administrator", "creator"}

def _official_channel_identifier():
    if OFFICIAL_CHANNEL_ID is not None:
        return OFFICIAL_CHANNEL_ID
    if OFFICIAL_CHANNEL_USERNAME:
        return OFFICIAL_CHANNEL_USERNAME
    return None

def _fetch_chat_member(uid: int) -> bool:
    if not BOT_TOKEN:
        raise RuntimeError("missing_bot_token")
    chat_id = _official_channel_identifier()
    if not chat_id:
        raise RuntimeError("missing_channel_id")
    resp = requests.get(
        f"{API_BASE}/getChatMember",
        params={"chat_id": chat_id, "user_id": uid},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"getChatMember_not_ok:{data.get('description')}")
    status = (data.get("result") or {}).get("status")
    return status in ALLOWED_CHANNEL_STATUSES

def _fetch_profile_photo(uid: int) -> bool:
    if not BOT_TOKEN:
        raise RuntimeError("missing_bot_token")
    resp = requests.get(
        f"{API_BASE}/getUserProfilePhotos",
        params={"user_id": uid, "limit": 1},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"getUserProfilePhotos_not_ok:{data.get('description')}")
    total = data.get("result", {}).get("total_count", 0)
    return bool(total and total > 0)

def _get_chat_member_status(user_id: int) -> str | None:
    if not BOT_TOKEN:
        raise RuntimeError("missing_bot_token")
    resp = requests.get(
        f"{API_BASE}/getChatMember",
        params={"chat_id": GROUP_ID, "user_id": user_id},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"getChatMember_not_ok:{data.get('description')}")
    return (data.get("result") or {}).get("status")

def _coerce_utc(dt_value) -> datetime | None:
    if not dt_value:
        return None
    if isinstance(dt_value, datetime):
        if dt_value.tzinfo:
            return dt_value.astimezone(timezone.utc)
        return dt_value.replace(tzinfo=timezone.utc)
    if isinstance(dt_value, str):
        try:
            parsed = datetime.fromisoformat(dt_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc)
        return parsed.replace(tzinfo=timezone.utc)
    return None

def _format_kl(dt_value: datetime | None) -> str | None:
    if not dt_value:
        return None
    return dt_value.astimezone(KL_TZ).isoformat()

def set_peak_mode(*, enabled: bool, duration_minutes: int = 15, reason: str = "drop_active") -> None:
    now_utc = datetime.now(timezone.utc)
    flag_doc = system_flags_col.find_one({"_id": "peak_mode"}) or {}
    prev_until = _coerce_utc(flag_doc.get("untilUtc"))
    if enabled:
        candidate_until = now_utc + timedelta(minutes=duration_minutes)
        until_utc = max(prev_until, candidate_until) if prev_until else candidate_until
        system_flags_col.update_one(
            {"_id": "peak_mode"},
            {
                "$set": {
                    "enabled": True,
                    "untilUtc": until_utc,
                    "reason": reason,
                    "updatedAtUtc": now_utc,
                }
            },
            upsert=True,
        )
        logger.info(
            "[PEAK_MODE][ENABLE] reason=%s until=%s prev_until=%s",
            reason,
            _format_kl(until_utc),
            _format_kl(prev_until),
        )
        return
    system_flags_col.update_one(
        {"_id": "peak_mode"},
        {"$set": {"enabled": False, "reason": reason, "updatedAtUtc": now_utc}},
        upsert=True,
    )
    logger.info(
        "[PEAK_MODE][DISABLE] reason=%s until=%s now=%s",
        reason,
        _format_kl(prev_until),
        _format_kl(now_utc),
    )

def sweep_peak_mode() -> None:
    now_utc = datetime.now(timezone.utc)
    flag_doc = system_flags_col.find_one({"_id": "peak_mode"}) or {}
    enabled = bool(flag_doc.get("enabled"))
    until_utc = _coerce_utc(flag_doc.get("untilUtc"))
    if enabled and until_utc and until_utc <= now_utc:
        system_flags_col.update_one(
            {"_id": "peak_mode", "enabled": True},
            {"$set": {"enabled": False, "reason": "auto_expired", "updatedAtUtc": now_utc}},
        )
        logger.info(
            "[PEAK_MODE][DISABLE] reason=auto_expired until=%s now=%s",
            _format_kl(until_utc),
            _format_kl(now_utc),
        )
        enabled = False
    elif enabled and not until_utc:
        system_flags_col.update_one(
            {"_id": "peak_mode"},
            {"$set": {"enabled": False, "reason": "auto_expired", "updatedAtUtc": now_utc}},
        )
        logger.info(
            "[PEAK_MODE][DISABLE] reason=auto_expired until=%s now=%s",
            _format_kl(until_utc),
            _format_kl(now_utc),
        )
        enabled = False

    recent_window_start = now_utc - timedelta(minutes=5)
    active_drop = db.drops.find_one(
        {
            "status": {"$nin": ["paused", "expired"]},
            "startsAt": {"$lte": now_utc, "$gte": recent_window_start},
            "endsAt": {"$gt": now_utc},
        },
        {"_id": 1, "startsAt": 1, "endsAt": 1},
    )
    if active_drop:
        drop_id = str(active_drop.get("_id") or "")
        set_peak_mode(enabled=True, duration_minutes=15, reason=f"drop:{drop_id}" if drop_id else "drop_active")
        return
    if not enabled:
        stale_drop = db.drops.find_one(
            {
                "status": {"$nin": ["paused", "expired"]},
                "startsAt": {"$lte": now_utc},
                "endsAt": {"$gt": now_utc},
            },
            {"_id": 1},
        )
        if stale_drop:
            logger.info(
                "[PEAK_MODE][TODO] active drop detected after enable window; consider wiring activation path drop=%s",
                str(stale_drop.get("_id")),
            )


def process_tg_verification_queue(batch_limit: int = 50) -> None:
    now_utc = datetime.now(timezone.utc)
    pending = list(
        tg_verification_queue_col.find(
            {
                "status": "pending",
                "next_run_at": {"$lte": now_utc},
                "$or": [{"locked_at": {"$exists": False}}, {"locked_at": None}],
            }
        )
        .sort("next_run_at", 1)
        .limit(batch_limit)
    )

    done = 0
    retry = 0
    errors = 0
    for doc in pending:
        locked = tg_verification_queue_col.find_one_and_update(
            {"_id": doc["_id"], "status": "pending", "locked_at": None},
            {"$set": {"locked_at": now_utc, "status": "processing"}},
            return_document=ReturnDocument.AFTER,
        )
        if not locked:
            continue
        uid = locked.get("user_id")
        types = locked.get("types") or []
        results = {}
        try:
            if "sub" in types:
                subscribed = _fetch_chat_member(uid)
                subscription_cache_col.update_one(
                    {"_id": f"sub:{uid}"},
                    {
                        "$set": {
                            "user_id": uid,
                            "subscribed": subscribed,
                            "checked_at": now_utc,
                            "updated_at": now_utc,
                            "expireAt": now_utc + timedelta(minutes=TG_VERIFY_TTL_MINUTES),
                        }
                    },
                    upsert=True,
                )
                results["sub"] = subscribed
            if "pic" in types:
                has_pic = _fetch_profile_photo(uid)
                profile_photo_cache_col.update_one(
                    {"uid": uid},
                    {
                        "$set": {
                            "user_id": uid,
                            "has_profile_pic": has_pic,
                            "hasPhoto": has_pic,
                            "checked_at": now_utc,
                            "updated_at": now_utc,
                            "expiresAt": now_utc + timedelta(minutes=TG_VERIFY_TTL_MINUTES),
                        }
                    },
                    upsert=True,
                )
                results["pic"] = has_pic
            tg_verification_queue_col.update_one(
                {"_id": locked["_id"]},
                {
                    "$set": {
                        "status": "done",
                        "done_at": now_utc,
                        "locked_at": None,
                        "results": results,
                    }
                },
            )
            done += 1
            logger.info("[TG_VERIFY][UPDATE] uid=%s types=%s results=%s", uid, types, results)
        except Exception as exc:
            attempts = int(locked.get("attempts", 0) or 0) + 1
            backoff_minutes = min(2 ** attempts, 30)
            tg_verification_queue_col.update_one(
                {"_id": locked["_id"]},
                {
                    "$set": {
                        "status": "pending",
                        "locked_at": None,
                        "attempts": attempts,
                        "last_error": str(exc),
                        "next_run_at": now_utc + timedelta(minutes=backoff_minutes),
                        "updated_at": now_utc,
                    }
                },
            )
            retry += 1
            errors += 1
            logger.warning(
                "[TG_VERIFY][ERROR] uid=%s types=%s attempts=%s err=%s",
                uid,
                types,
                attempts,
                exc,
            )

    if pending:
        logger.info(
            "[TG_VERIFY][RUN] picked=%s done=%s retry=%s errors=%s",
            len(pending),
            done,
            retry,
            errors,
        )
        
def sweep_expired_drops():
    """
    Marks voucher drops as expired once their endsAt passes.
    Safe to run every minute. No side effects beyond status flip.
    """
    now = datetime.now(timezone.utc)
    # Use the same collection name as in vouchers.py ("drops" or "voucher_drops")
    db.drops.update_many(  # change to db.voucher_drops if you renamed it
        {"endsAt": {"$lte": now}, "status": {"$ne": "expired"}},
        {"$set": {"status": "expired"}}
    )

def archive_weekly_leaderboard():
    """
    Snapshots weekly leaderboards and resets weekly counters.
    Trigger this at Monday 00:00 KL (schedule in main.py).
    """
    now_utc = datetime.now(timezone.utc)
    now_kl = now_utc.astimezone(KL_TZ)
    week_key = now_kl.strftime("%Y-%W")  # e.g., "2025-42"

    checkin_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_xp": 1}))
    referral_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_referral_count": 1}))

    snapshot = {
        "week": week_key,
        "timestampUtc": now_utc,
        "timestampKl": now_kl.isoformat(),
        "checkin": checkin_list,
        "referral": referral_list,
    }

    db.leaderboard_weekly.insert_one(snapshot)
    logger.info(
        "[LEADERBOARD] weekly_snapshot week=%s checkin_count=%s referral_count=%s",
        week_key,
        len(checkin_list),
        len(referral_list),
    )    
    db.users.update_many({}, {"$set": {"weekly_xp": 0, "weekly_referral_count": 0}})
    logger.info("[RESET][WEEKLY] weekly_xp_ref_reset ok")


def reset_monthly_referrals():
    """
    Reset monthly referral counters (run on the first day of the month 00:00 KL).
    """
    result = db.users.update_many({}, {"$set": {"monthly_referral_count": 0}})
    logger.info("[RESET][MONTHLY] monthly_referral_reset modified=%s", result.modified_count)


def settle_pending_referrals(batch_limit: int = 200) -> None:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=REFERRAL_HOLD_HOURS)
    pending_cursor = db.pending_referrals.find(
        {"status": "pending", "created_at_utc": {"$lte": cutoff}}
    ).sort("created_at_utc", 1).limit(batch_limit)
    pending_docs = list(pending_cursor)

    scanned = len(pending_docs)
    awarded = 0
    revoked = 0
    for pending in pending_docs:
        pending_id = pending.get("_id")
        invitee_user_id = pending.get("invitee_user_id")
        inviter_user_id = pending.get("inviter_user_id")
        step = "lock"
        locked = db.pending_referrals.find_one_and_update(
            {"_id": pending_id, "status": "pending"},
            {"$set": {"status": "processing", "processing_at": now_utc}},
            return_document=ReturnDocument.BEFORE,
        )
        if not locked:
            continue
        try:
            if not invitee_user_id or not inviter_user_id:
                step = "validate_ids"
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "invalid_ids",
                            "revoked_at": now_utc,
                        }
                    },
                )
                revoked += 1
                continue
            if invitee_user_id == inviter_user_id:
                step = "self_invite"
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "self_invite",
                            "revoked_at": now_utc,
                        }
                    },
                )
                revoked += 1
                continue

            step = "check_membership"
            try:
                status = _get_chat_member_status(invitee_user_id)
            except Exception as exc:
                logger.exception(
                    "[REFERRAL][ERROR] step=check_membership inviter=%s invitee=%s err=%s",
                    inviter_user_id,
                    invitee_user_id,
                    exc,
                )
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {"$set": {"status": "pending"}, "$unset": {"processing_at": ""}},
                )
                continue
            if status not in {"member", "administrator", "creator"}:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "not_in_group",
                            "revoked_at": now_utc,
                        }
                    },
                )
                revoked += 1
                continue

            step = "check_new_user"
            invitee_doc = db.users.find_one(
                {"user_id": invitee_user_id},
                {"created_at": 1, "joined_main_at": 1},
            )
            if not invitee_doc:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "no_user_doc",
                            "revoked_at": now_utc,
                        }
                    },
                )
                revoked += 1
                continue
            join_seen = pending.get("created_at_utc")
            join_seen_utc = _coerce_utc(join_seen)
            joined_main_at = _coerce_utc(invitee_doc.get("joined_main_at"))
            created_at = _coerce_utc(invitee_doc.get("created_at"))
            reference_time = joined_main_at or created_at
            if not reference_time or not join_seen_utc:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "missing_join_time",
                            "revoked_at": now_utc,
                        }
                    },
                )
                revoked += 1
                continue
            if reference_time < (join_seen_utc - timedelta(minutes=10)):
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "already_in_db",
                            "revoked_at": now_utc,
                        }
                    },
                )
                revoked += 1
                continue

            step = "award"
            inviter_doc = db.users.find_one_and_update(
                {"user_id": inviter_user_id},
                {
                    "$inc": {
                        "ref_count_total": 1,
                        "weekly_referral_count": 1,
                        "monthly_referral_count": 1,
                    },
                    "$setOnInsert": {
                        "user_id": inviter_user_id,
                        "created_at": now_utc.astimezone(KL_TZ),
                    },
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            ref_total = int((inviter_doc or {}).get("ref_count_total", 1))
            weekly_ref = int((inviter_doc or {}).get("weekly_referral_count", 1))
            xp_added, bonus_added = calc_referral_award(ref_total)
            db.users.update_one(
                {"user_id": inviter_user_id},
                {"$inc": {"xp_total": xp_added, "weekly_xp": xp_added, "monthly_xp": xp_added}},
            )
            db.pending_referrals.update_one(
                {"_id": pending_id},
                {
                    "$set": {
                        "status": "awarded",
                        "awarded_at": now_utc,
                        "xp_added": xp_added,
                        "bonus_added": bonus_added,
                        "ref_count_total_after": ref_total,
                    }
                },
            )
            awarded += 1
            logger.info(
                "[REFERRAL][AWARD] inviter=%s invitee=%s ref_total=%s weekly_ref=%s xp_added=%s bonus_added=%s hold_hours=%s",
                inviter_user_id,
                invitee_user_id,
                ref_total,
                weekly_ref,
                xp_added,
                bonus_added,
                REFERRAL_HOLD_HOURS,
            )
        except Exception as exc:
            logger.exception(
                "[REFERRAL][ERROR] step=%s inviter=%s invitee=%s err=%s",
                step,
                inviter_user_id,
                invitee_user_id,
                exc,
            )
            db.pending_referrals.update_one(
                {"_id": pending_id},
                {"$set": {"status": "pending"}, "$unset": {"processing_at": ""}},
            )

    logger.info(
        "[REFERRAL][SETTLE] scanned=%s pending=%s awarded=%s revoked=%s",
        scanned,
        scanned,
        awarded,
        revoked,
    )
