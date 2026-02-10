from datetime import datetime, timedelta, timezone
import logging
import os
import pytz
import requests
import socket
from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError
from requests import RequestException
from database import db
from referral_rules import calc_referral_award
from ugc_growth_referral import apply_growth_credit, is_expansion_qualified, should_allow_viewer_reward
from vouchers import claim_voucher_for_user
from xp import grant_xp, now_utc, now_kl

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GROUP_ID = int(os.environ.get("GROUP_ID", "-1002304653063"))
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
REFERRAL_HOLD_HOURS = 12
UGC_T1_SMALL_DROP_ID = (os.environ.get("UGC_T1_SMALL_DROP_ID") or "").strip()
UGC_T2_MID_DROP_ID = (os.environ.get("UGC_T2_MID_DROP_ID") or "").strip()

referral_claims_col = db["referral_claims"]
growth_credit_col = db["growth_credit"]
voucher_ledger_col = db["voucher_ledger"]
users_col = db["users"]

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

logger = logging.getLogger(__name__)
INSTANCE_ID = os.getenv("FLY_ALLOC_ID") or f"{socket.gethostname()}:{os.getpid()}"
PROCESSING_TIMEOUT = timedelta(minutes=10)
RETRY_RELEASE_DELAY = timedelta(minutes=2)
# SNAPSHOT FIELDS — ONLY WRITTEN BY WORKER
# weekly_xp, monthly_xp, total_xp, weekly_referrals, monthly_referrals, total_referrals, vip_tier, vip_month
# DEPRECATED — DO NOT USE (ledger-based referrals only)
# weekly_referral_count, total_referral_count, ref_count_total, monthly_referral_count

class ReferralRetryableError(RuntimeError):
    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after

def _get_chat_member_status(user_id: int) -> str | None:
    if not BOT_TOKEN:
        raise RuntimeError("missing_bot_token")
    resp = requests.get(
        f"{API_BASE}/getChatMember",
        params={"chat_id": GROUP_ID, "user_id": user_id},
        timeout=10,
    )
    if resp.status_code == 429:
        retry_after = None
        try:
            payload = resp.json()
            retry_after = (payload.get("parameters") or {}).get("retry_after")
        except Exception:
            retry_after = None
        raise ReferralRetryableError("telegram_rate_limited", retry_after=retry_after)    
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"getChatMember_not_ok:{data.get('description')}")
    return (data.get("result") or {}).get("status")

def _send_dm(user_id: int, text: str) -> None:
    if not BOT_TOKEN:
        return
    try:
        requests.post(
            f"{API_BASE}/sendMessage",
            json={"chat_id": user_id, "text": text},
            timeout=10,
        )
    except Exception:
        logger.exception("[SCHED][UGC_REFERRAL] dm_failed uid=%s", user_id)


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



def _compute_backoff_seconds(retry_count: int, *, base: int, cap: int) -> int:
    try:
        retry_count = int(retry_count)
    except (TypeError, ValueError):
        retry_count = 0
    return min(cap, base * (2**retry_count))


def _release_for_retry(pending_id, now_utc_ts: datetime, retry_after_seconds: int, reason: str) -> None:
    next_retry = now_utc_ts + timedelta(seconds=retry_after_seconds)
    db.pending_referrals.update_one(
        {"_id": pending_id},
        {
            "$set": {
                "status": "pending",
                "next_retry_at_utc": next_retry,
                "retry_last_reason": reason,
            },
            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
            "$inc": {"retry_count": 1},
        },
    )

def _week_start_kl(reference: datetime | None = None) -> datetime:
    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    return (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

def _week_end_kl(reference: datetime | None = None) -> datetime:
    return _week_start_kl(reference) + timedelta(days=7)

def _month_start_kl(reference: datetime | None = None) -> datetime:
    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    return ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _month_end_kl(reference: datetime | None = None) -> datetime:
    start_local = _month_start_kl(reference)
    if start_local.month == 12:
        return start_local.replace(year=start_local.year + 1, month=1)
    return start_local.replace(month=start_local.month + 1)

def _maybe_send_near_miss_dm_web(inviter_user_id: int, total_referrals_after: int) -> None:
    if os.getenv("RUNNER_MODE") != "web":
        return
    try:
        from main import _maybe_send_near_miss_dm
    except Exception:
        logger.exception(
            "[SCHED][REFERRAL] near_miss_import_failed inviter=%s",
            inviter_user_id,
        )
        return
    try:
        _maybe_send_near_miss_dm(inviter_user_id, total_referrals_after)
    except Exception:
        logger.exception(
            "[SCHED][REFERRAL] near_miss_failed inviter=%s",
            inviter_user_id,
        )
        
def _week_window_utc(reference: datetime | None = None) -> tuple[datetime, datetime]:
    start_local = _week_start_kl(reference)
    end_local = _week_end_kl(reference)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def _month_window_utc(reference: datetime | None = None) -> tuple[datetime, datetime]:
    start_local = _month_start_kl(reference)
    end_local = _month_end_kl(reference)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
    
def _referral_event_doc(inviter_id: int, invitee_id: int, event: str, occurred_at: datetime) -> dict:
    week_key = _week_start_kl(occurred_at).date().isoformat()
    month_key = _month_start_kl(occurred_at).date().isoformat()
    return {
        "inviter_id": inviter_id,
        "invitee_id": invitee_id,
        "event": event,
        "occurred_at": occurred_at,
        "week_key": week_key,
        "month_key": month_key,
    }

def _record_referral_event(inviter_id: int, invitee_id: int, event: str, occurred_at: datetime) -> None:
    if inviter_id is None or invitee_id is None:
        return
    try:
        db.referral_events.insert_one(_referral_event_doc(inviter_id, invitee_id, event, occurred_at))
    except DuplicateKeyError:
        logger.info(
            "[SCHED][REFERRAL_LEDGER] duplicate inviter=%s invitee=%s action=%s",
            inviter_id,
            invitee_id,
            event,
        )
        return
    logger.info(
        "[SCHED][REFERRAL_LEDGER] inviter=%s invitee=%s action=%s",
        inviter_id,
        invitee_id,
        "settled" if event == "referral_settled" else "revoked",
    )


def maybe_handle_first_referral(uid: int, old_total: int, new_total: int, now_utc_ts: datetime) -> None:
    if old_total != 0 or new_total < 1:
        return
    try:
        from onboarding import record_first_referral, maybe_unlock_vip1
    except Exception:
        logger.exception("[FIRST_REFERRAL] import_failed uid=%s", uid)
        return
    created = record_first_referral(uid, ref=now_utc_ts)
    if created:
        maybe_unlock_vip1(uid)
        
def _xp_time_expr():
    return {"$ifNull": ["$created_at", "$ts"]}

def _write_snapshot_heartbeat(source: str, now_utc_ts: datetime) -> None:
    try:
        db.admin_cache.update_one(
            {"_id": "snapshot_heartbeat"},
            {
                "$set": {
                    "ts_utc": now_utc_ts,
                    "ts_kl": now_utc_ts.astimezone(KL_TZ),
                    "source": source,
                }
            },
            upsert=True,
        )
        logger.info(
            "[SNAPSHOT][HEARTBEAT] type=%s ts=%s",
            source,
            now_utc_ts.isoformat(),
        )
    except Exception:
        logger.exception("[SNAPSHOT][HEARTBEAT] failed type=%s", source)
        
def settle_xp_snapshots() -> None:
    now_utc_ts = now_utc()
    week_start_utc, week_end_utc = _week_window_utc(now_utc_ts)
    month_start_utc, month_end_utc = _month_window_utc(now_utc_ts)
    logger.info(
        "[SNAPSHOT] rebuild_start kind=xp week_start=%s month_start=%s",
        week_start_utc.isoformat(),
        month_start_utc.isoformat(),
    )
    
    week_cond = {
        "$and": [
            {"$gte": [_xp_time_expr(), week_start_utc]},
            {"$lt": [_xp_time_expr(), week_end_utc]},
        ]
    }

    month_cond = {
        "$and": [
            {"$gte": [_xp_time_expr(), month_start_utc]},
            {"$lt": [_xp_time_expr(), month_end_utc]},
        ]
    }

    db.users.update_many(
        {},
        {
            "$set": {
                "weekly_xp_next": 0,
                "monthly_xp_next": 0,
                "total_xp_next": 0,
            }
        },
    )

    pipeline = [
        {
            "$match": {
                "user_id": {"$ne": None},
                "$or": [{"invalidated": {"$exists": False}}, {"invalidated": False}],
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_xp": {"$sum": "$xp"},
                "weekly_xp": {"$sum": {"$cond": [week_cond, "$xp", 0]}},
                "monthly_xp": {"$sum": {"$cond": [month_cond, "$xp", 0]}},
            }
        },
    ]
    results = list(db.xp_events.aggregate(pipeline))
    if results:
        updates = []
        for row in results:
            uid = row.get("_id")
            if uid is None:
                continue
            total_xp = int(row.get("total_xp", 0))
            weekly_xp = int(row.get("weekly_xp", 0))
            monthly_xp = int(row.get("monthly_xp", 0))
            updates.append(
                UpdateOne(
                    {"user_id": uid},
                    {
                        "$set": {
                            "total_xp_next": total_xp,
                            "weekly_xp_next": weekly_xp,
                            "monthly_xp_next": monthly_xp,
                        }
                    },
                    upsert=True,
                )
            )
        if updates:
            db.users.bulk_write(updates, ordered=False)

    publish_result = db.users.update_many(
        {},
        [
            {
                "$set": {
                    "total_xp": "$total_xp_next",
                    "weekly_xp": "$weekly_xp_next",
                    "monthly_xp": "$monthly_xp_next",
                    "xp": "$total_xp_next",
                    "snapshot_published_at": now_utc_ts,
                    "snapshot_updated_at": now_utc_ts,                    
                }
            }
        ],
    )
    db.users.update_many({}, {"$inc": {"snapshot_version": 1}})
    logger.info(
        "[SNAPSHOT] publish_done users=%s version_inc=1",
        publish_result.modified_count,
    )    
    _write_snapshot_heartbeat("xp", now_utc_ts)    
    for row in results:
        uid = row.get("_id")
        if uid is None:
            continue

        logger.info(
            "[SCHED][SNAPSHOT_WRITE] uid=%s weekly_xp=%s",
            uid,
            int(row.get("weekly_xp", 0)),
        )
        if int(row.get("monthly_xp", 0)) >= 800:
            try:
                from onboarding import maybe_unlock_vip1
            except Exception:
                logger.exception("[VIP][CHECK] import_failed uid=%s", uid)
            else:
                maybe_unlock_vip1(uid)        

def _referral_sign_expr():
    return {
        "$cond": [
            {"$eq": ["$event", "referral_settled"]},
            1,
            {
                "$cond": [
                    {"$eq": ["$event", "referral_revoked"]},
                    -1,
                    0,
                ]
            },
        ]
    }



def _should_pass_growth_throttle(last_credit_at: datetime | None, now_utc_ts: datetime) -> bool:
    return last_credit_at is None or last_credit_at <= (now_utc_ts - timedelta(hours=24))


def _eligible_referrer_tiers(credited_count: int, tier_awarded: dict | None) -> list[str]:
    tier_awarded = tier_awarded or {}
    out = []
    if credited_count >= 3 and not tier_awarded.get("s2"):
        out.append("S2")
    if credited_count >= 7 and not tier_awarded.get("s3"):
        out.append("S3")
    if credited_count >= 15 and not tier_awarded.get("s4"):
        out.append("S4")
    return out
    
def settle_referral_snapshots() -> None:
    now_utc_ts = now_utc()
    week_start_utc, week_end_utc = _week_window_utc(now_utc_ts)
    month_start_utc, month_end_utc = _month_window_utc(now_utc_ts)
    logger.info(
        "[SNAPSHOT] rebuild_start kind=referral week_start=%s month_start=%s",
        week_start_utc.isoformat(),
        month_start_utc.isoformat(),
    )
    
    week_cond = {
        "$and": [
            {"$gte": ["$occurred_at", week_start_utc]},
            {"$lt": ["$occurred_at", week_end_utc]},
        ]
    }
    month_cond = {
        "$and": [
            {"$gte": ["$occurred_at", month_start_utc]},
            {"$lt": ["$occurred_at", month_end_utc]},
        ]
    }

    db.users.update_many(
        {},
        {
            "$set": {
                "weekly_referrals_next": 0,
                "monthly_referrals_next": 0,
                "total_referrals_next": 0,
            }
        },
    )

    pipeline = [
        {
            "$match": {
                "inviter_id": {"$ne": None},
                "event": {"$in": ["referral_settled", "referral_revoked"]},
            }
        },
        {
            "$group": {
                "_id": "$inviter_id",
                "total_referrals": {"$sum": _referral_sign_expr()},
                "weekly_referrals": {"$sum": {"$cond": [week_cond, _referral_sign_expr(), 0]}},
                "monthly_referrals": {"$sum": {"$cond": [month_cond, _referral_sign_expr(), 0]}},
            }
        },
    ]
    results = list(db.referral_events.aggregate(pipeline))
    if results:
        updates = []
        for row in results:
            uid = row.get("_id")
            if uid is None:
                continue
            total_referrals = int(row.get("total_referrals", 0))
            weekly_referrals = int(row.get("weekly_referrals", 0))
            monthly_referrals = int(row.get("monthly_referrals", 0))
            updates.append(
                UpdateOne(
                    {"user_id": uid},
                    {
                        "$set": {
                            "weekly_referrals_next": weekly_referrals,
                            "monthly_referrals_next": monthly_referrals,
                            "total_referrals_next": total_referrals,
                        }
                    },
                    upsert=True,
                )
            )
        if updates:
            db.users.bulk_write(updates, ordered=False)

    publish_result = db.users.update_many(
        {},
        [
            {
                "$set": {
                    "weekly_referrals": "$weekly_referrals_next",
                    "monthly_referrals": "$monthly_referrals_next",
                    "total_referrals": "$total_referrals_next",
                    "snapshot_published_at": now_utc_ts,
                    "snapshot_updated_at": now_utc_ts,                    
                }
            }
        ],
    )
    db.users.update_many({}, {"$inc": {"snapshot_version": 1}})
    logger.info(
        "[SNAPSHOT] publish_done users=%s version_inc=1",
        publish_result.modified_count,
    )    
    _write_snapshot_heartbeat("referral", now_utc_ts)    


def settle_ugc_referral_claims(batch_limit: int = 100) -> None:
    now_utc_ts = now_utc()
    processed = 0
    while processed < batch_limit:
        claim = referral_claims_col.find_one_and_update(
            {"status": "pending", "due_at": {"$lte": now_utc_ts}},
            {"$set": {"status": "processing", "processing_at": now_utc_ts}},
            sort=[("due_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not claim:
            break
        processed += 1
        viewer_uid = claim.get("viewer_uid")
        source_uid = claim.get("source_uid")
        claim_id = claim.get("_id")
        try:
            status = _get_chat_member_status(int(viewer_uid))
            if status not in {"member", "administrator", "creator"}:
                referral_claims_col.update_one(
                    {"viewer_uid": viewer_uid},
                    {"$set": {"status": "rejected", "credit_reason": "left_before_48h", "updated_at": now_utc_ts}},
                )
                continue

            reward_allowed = should_allow_viewer_reward(referral_claims_col, claim)
            if not reward_allowed:
                referral_claims_col.update_one(
                    {"viewer_uid": viewer_uid},
                    {"$set": {"status": "rejected", "credit_reason": "abuse_gate", "updated_at": now_utc_ts}},
                )
                continue

            drop_id = UGC_T1_SMALL_DROP_ID
            existing_reward = voucher_ledger_col.find_one({"kind": "ugc_viewer_reward", "claim_id": str(claim_id)})
            if existing_reward and existing_reward.get("voucher_code"):
                issued = {"code": existing_reward.get("voucher_code")}
            else:
                username_doc = users_col.find_one({"user_id": viewer_uid}, {"usernameLower": 1, "username": 1}) or {}
                username = username_doc.get("usernameLower") or username_doc.get("username") or ""
                issued = claim_voucher_for_user(user_id=str(viewer_uid), drop_id=drop_id, username=username)
                try:
                    voucher_ledger_col.insert_one(
                        {
                            "kind": "ugc_viewer_reward",
                            "claim_id": str(claim_id),
                            "viewer_uid": viewer_uid,
                            "source_uid": source_uid,
                            "drop_id": drop_id,
                            "voucher_code": issued.get("code"),
                            "created_at": now_utc_ts,
                        }
                    )
                except DuplicateKeyError:
                    pass
            _send_dm(int(viewer_uid), f"Your voucher is ready: {issued.get('code', '')}")
            referral_claims_col.update_one(
                {"viewer_uid": viewer_uid},
                {
                    "$set": {
                        "status": "validated",
                        "validated_at": now_utc_ts,
                        "viewer_reward_issued": True,
                        "viewer_reward_drop_id": drop_id,
                        "credit_reason": "not_expansion_or_throttle",
                        "updated_at": now_utc_ts,
                    }
                },
            )

            if source_uid:
                recent_claims = list(
                    referral_claims_col.find(
                        {
                            "source_uid": source_uid,
                            "status": "validated",
                            "viewer_uid": {"$ne": viewer_uid},
                            "validated_at": {"$gte": now_utc_ts - timedelta(days=7)},
                        }
                    ).limit(20)
                )
                expansion_qualified = is_expansion_qualified(int(viewer_uid), int(source_uid), claim, recent_claims)
                if expansion_qualified:
                    credited, _reason, gc = apply_growth_credit(growth_credit_col, int(source_uid), now_utc_ts)
                else:
                    credited, _reason, gc = (False, "not_expansion", growth_credit_col.find_one({"_id": int(source_uid)}) or {})
                if credited:
                    referral_claims_col.update_one(
                        {"viewer_uid": viewer_uid},
                        {"$set": {"credited": True, "credit_reason": "credited", "updated_at": now_utc_ts}},
                    )

                    tier_awarded = (gc or {}).get("tier_awarded") or {}
                    credited_count = int((gc or {}).get("credited_count", 0))
                    for tier in _eligible_referrer_tiers(credited_count, tier_awarded):
                        if tier == "S2":
                            
                            try:
                                voucher_ledger_col.insert_one({"kind": "ugc_referrer_reward", "tier": "S2", "source_uid": source_uid, "drop_id": UGC_T1_SMALL_DROP_ID, "created_at": now_utc_ts, "status": "issued"})
                            except DuplicateKeyError:
                                pass
                            growth_credit_col.update_one({"_id": int(source_uid)}, {"$set": {"tier_awarded.s2": True}})
                        elif tier == "S3":
                            
                            try:
                                voucher_ledger_col.insert_one({"kind": "ugc_referrer_reward", "tier": "S3", "source_uid": source_uid, "drop_id": UGC_T2_MID_DROP_ID, "created_at": now_utc_ts, "status": "issued"})
                            except DuplicateKeyError:
                                pass
                            growth_credit_col.update_one({"_id": int(source_uid)}, {"$set": {"tier_awarded.s3": True}})
                        elif tier == "S4":
                            try:
                                voucher_ledger_col.insert_one({"kind": "ugc_referrer_reward", "tier": "S4", "source_uid": source_uid, "drop_id": None, "created_at": now_utc_ts, "status": "pending_admin"})
                            except DuplicateKeyError:
                                pass
                            growth_credit_col.update_one({"_id": int(source_uid)}, {"$set": {"tier_awarded.s4": True}})
                            _send_dm(int(source_uid), "You unlocked a large reward pending admin approval.")
                else:
                    referral_claims_col.update_one(
                        {"viewer_uid": viewer_uid},
                        {"$set": {"credit_reason": "not_expansion_or_throttle", "updated_at": now_utc_ts}},
                    )
        except Exception as exc:
            logger.exception("[SCHED][UGC_REFERRAL] process_failed viewer_uid=%s err=%s", viewer_uid, exc)
            referral_claims_col.update_one({"viewer_uid": viewer_uid}, {"$set": {"status": "pending", "updated_at": now_utc_ts}})
            
def _recover_stale_processing(now_utc_ts: datetime) -> int:
    cutoff = now_utc_ts - PROCESSING_TIMEOUT
    result = db.pending_referrals.update_many(
        {
            "status": "processing",
            "$or": [
                {"processing_at_utc": {"$lte": cutoff}},
                {"processing_at": {"$lte": cutoff}},
            ],
        },
        {
            "$set": {"status": "pending", "next_retry_at_utc": now_utc_ts + RETRY_RELEASE_DELAY},
            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
            "$inc": {"retry_count": 1},
        },
    )
    return result.modified_count
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
    referral_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_referrals": 1}))

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
    db.users.update_many({}, {"$set": {"weekly_xp": 0, "weekly_referrals": 0}})
    logger.info("[RESET][WEEKLY] weekly_xp_ref_reset ok")

def settle_pending_referrals(batch_limit: int = 200) -> None:
    now_utc_ts = now_utc()
    cutoff = now_utc_ts - timedelta(hours=REFERRAL_HOLD_HOURS)
    recovered = _recover_stale_processing(now_utc_ts)
    if recovered:
        logger.info("[SCHED][REFERRAL] recovered_stale_processing=%s", recovered)

    scanned = 0
    awarded = 0
    revoked = 0
    
    while scanned < batch_limit:
        pending = db.pending_referrals.find_one_and_update(
            {
                "status": "pending",
                "created_at_utc": {"$lte": cutoff},
                "$or": [
                    {"next_retry_at_utc": {"$exists": False}},
                    {"next_retry_at_utc": {"$lte": now_utc_ts}},
                ],
            },
            {
                "$set": {
                    "status": "processing",
                    "processing_by": INSTANCE_ID,
                    "processing_at_utc": now_utc_ts,
                }
            },
            sort=[("created_at_utc", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not pending:
            break
        scanned += 1
        pending_id = pending.get("_id")
        invitee_user_id = pending.get("invitee_user_id")
        inviter_user_id = pending.get("inviter_user_id")   
        step = "validate"
        retry_count = pending.get("retry_count", 0) or 0
        try:
            if not invitee_user_id or not inviter_user_id:
                step = "validate_ids"
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "invalid_ids",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)                
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
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)                
                revoked += 1
                continue

            step = "check_membership"
            try:
                status = _get_chat_member_status(invitee_user_id)
            except ReferralRetryableError as exc:
                retry_after = exc.retry_after
                backoff = (
                    int(retry_after)
                    if retry_after is not None
                    else _compute_backoff_seconds(retry_count, base=5, cap=300)
                )
                logger.warning(
                    "[SCHED][REFERRAL] retryable=telegram_rate_limited inviter=%s invitee=%s retry_after=%s",
                    inviter_user_id,
                    invitee_user_id,
                    backoff,
                )
                _release_for_retry(pending_id, now_utc_ts, backoff, "telegram_429")
                continue
            except RequestException as exc:
                backoff = _compute_backoff_seconds(retry_count, base=30, cap=120)
                logger.warning(
                    "[SCHED][REFERRAL] retryable=telegram_request_failed inviter=%s invitee=%s err=%s",
                    inviter_user_id,
                    invitee_user_id,
                    exc,
                )
                _release_for_retry(pending_id, now_utc_ts, backoff, "telegram_request_failed")        
                continue
            if status not in {"member", "administrator", "creator"}:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "not_in_group",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)                
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
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)                
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
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)                
                revoked += 1
                continue
            if reference_time < (join_seen_utc - timedelta(minutes=10)):
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "already_in_db",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)                
                revoked += 1
                continue

            step = "award"
            group_id = pending.get("group_id") or GROUP_ID
            award_key = f"ref:{group_id}:{invitee_user_id}"
            award_doc = {
                "award_key": award_key,
                "group_id": group_id,
                "inviter_user_id": inviter_user_id,
                "invitee_user_id": invitee_user_id,
                "pending_id": pending_id,
                "created_at_utc": now_utc_ts,
                "awarded_at_utc": now_utc_ts,
                "status": "awarded",
            }
            try:
                db.referral_award_events.insert_one(award_doc)
            except DuplicateKeyError:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "awarded",
                            "awarded_at_utc": now_utc_ts,
                            "awarded_at_kl": now_kl().isoformat(),
                            "award_key": award_key,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                logger.info(
                    "[SCHED][REFERRAL] duplicate_award inviter=%s invitee=%s award_key=%s",
                    inviter_user_id,
                    invitee_user_id,
                    award_key,
                )
                continue

            total_pipeline = [
                {
                    "$match": {
                        "inviter_id": inviter_user_id,
                        "event": {"$in": ["referral_settled", "referral_revoked"]},
                    }
                },
                {"$group": {"_id": None, "total": {"$sum": _referral_sign_expr()}}},
            ]
            total_rows = list(db.referral_events.aggregate(total_pipeline))
            current_ref_total = int((total_rows[0]["total"] if total_rows else 0) or 0)
            new_ref_total = current_ref_total + 1
            xp_added, bonus_added = calc_referral_award(new_ref_total)
            xp_granted = grant_xp(db, inviter_user_id, "referral_award", award_key, xp_added)
            ref_total = current_ref_total
            actual_xp_added = 0
            actual_bonus_added = 0

            if xp_granted:
                actual_xp_added = xp_added
                actual_bonus_added = bonus_added
            _record_referral_event(inviter_user_id, invitee_user_id, "referral_settled", now_utc_ts)
            ref_total = new_ref_total
            maybe_handle_first_referral(inviter_user_id, current_ref_total, new_ref_total, now_utc_ts)
            _maybe_send_near_miss_dm_web(inviter_user_id, ref_total)
            
            db.pending_referrals.update_one(
                {"_id": pending_id},
                {
                    "$set": {
                        "status": "awarded",
                        "awarded_at_utc": now_utc_ts,
                        "awarded_at_kl": now_kl().isoformat(),
                        "xp_added": actual_xp_added,
                        "bonus_added": actual_bonus_added,
                        "total_referrals_after": ref_total,
                        "award_key": award_key,
                    },
                    "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                },
            )
            awarded += 1
            logger.info(
                "[SCHED][REFERRAL] award_ok inviter=%s invitee=%s ref_total=%s xp_added=%s bonus_added=%s hold_hours=%s",
                inviter_user_id,
                invitee_user_id,
                ref_total,
                actual_xp_added,
                actual_bonus_added,
                REFERRAL_HOLD_HOURS,
            )
        except Exception as exc:
            logger.exception(
                "[SCHED][REFERRAL] error step=%s inviter=%s invitee=%s err=%s",
                step,
                inviter_user_id,
                invitee_user_id,
                exc,
            )
            backoff = _compute_backoff_seconds(retry_count, base=30, cap=120)
            _release_for_retry(pending_id, now_utc_ts, backoff, f"exception:{step}")

    logger.info(
        "[SCHED][REFERRAL] settle scanned=%s awarded=%s revoked=%s batch_limit=%s",
        scanned,
        awarded,
        revoked,
        batch_limit,        
    )
