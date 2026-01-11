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

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

logger = logging.getLogger(__name__)


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
