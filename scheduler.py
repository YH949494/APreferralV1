from datetime import datetime, timedelta, timezone
import logging
import os
import pytz
import requests
import socket
import time
from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError
from requests import RequestException
from database import db
from referral_rules import calc_referral_award
from xp import grant_xp, now_utc, now_kl
from affiliate_rewards import mark_invitee_qualified

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
_RAW_GROUP_ID = os.environ.get("MAIN_GROUP_ID") or os.environ.get("GROUP_ID") or "-1002304653063"
try:
    GROUP_ID = int(_RAW_GROUP_ID)
except (TypeError, ValueError):
    GROUP_ID = -1002304653063
_RAW_OFFICIAL_CHANNEL_ID = os.environ.get("OFFICIAL_CHANNEL_ID")
try:
    OFFICIAL_CHANNEL_ID = int(_RAW_OFFICIAL_CHANNEL_ID) if _RAW_OFFICIAL_CHANNEL_ID not in (None, "") else None
except (TypeError, ValueError):
    OFFICIAL_CHANNEL_ID = None
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
REFERRAL_HOLD_HOURS = 12
REFERRAL_CHANNEL_RETRY_HOURS = 12
REFERRAL_CHANNEL_EXPIRE_DAYS = 7

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


def _check_official_channel_subscribed_sync(uid: int) -> tuple[bool, str]:
    if not uid:
        return False, "missing_uid"
    if OFFICIAL_CHANNEL_ID is None:
        return False, "channel_unset"
    if not BOT_TOKEN:
        return False, "missing_token"

    def _fetch_once():
        try:
            resp = requests.get(
                f"{API_BASE}/getChatMember",
                params={"chat_id": OFFICIAL_CHANNEL_ID, "user_id": uid},
                timeout=10,
            )
        except requests.RequestException as exc:
            return None, None, str(exc)
        try:
            payload = resp.json()
        except ValueError:
            return resp.status_code, None, "bad_json"
        return resp.status_code, payload, None

    status_code, payload, err = _fetch_once()
    if err:
        return False, err

    for attempt in range(2):
        error_code = (payload or {}).get("error_code")
        if status_code == 429 or ((payload or {}).get("ok") is False and error_code == 429):
            if attempt == 1:
                return False, "rate_limited"
            retry_after = ((payload or {}).get("parameters") or {}).get("retry_after", 1)
            try:
                retry_after = int(retry_after)
            except (TypeError, ValueError):
                retry_after = 1
            time.sleep(max(0, min(retry_after, 5)))
            status_code, payload, err = _fetch_once()
            if err:
                return False, err
            continue
        break

    if not isinstance(payload, dict):
        return False, "bad_json"
    if not payload.get("ok"):
        return False, f"tg_not_ok:{payload.get('description', 'unknown')}"
    status = (payload.get("result") or {}).get("status")
    if status in {"member", "administrator", "creator"}:
        return True, f"status:{status}"
    return False, f"status:{status}"

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


def _round_rate(value: float) -> float:
    return round(float(value), 4)


def _utc_day_bounds(day_utc: str) -> tuple[datetime, datetime]:
    day_start = datetime.strptime(day_utc, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return day_start, day_start + timedelta(days=1)


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return _round_rate(float(numerator) / float(denominator))


def ensure_affiliate_daily_kpi_indexes(db_ref=None) -> None:
    db_ref = db_ref or db
    db_ref.affiliate_daily_kpis.create_index([("day_utc", 1)], unique=True, name="uniq_affiliate_daily_kpi_day")


def compute_affiliate_daily_kpi(day_utc: str, *, db_ref=None, now_utc_ts: datetime | None = None) -> dict:
    db_ref = db_ref or db
    now_utc_ts = now_utc_ts or now_utc()
    day_start, day_end = _utc_day_bounds(day_utc)

    referrals = list(
        db_ref.pending_referrals.find(
            {"created_at_utc": {"$gte": day_start, "$lt": day_end}},
            {"invitee_user_id": 1, "created_at_utc": 1},
        )
    )
    new_referrals = int(len(referrals))
    qualified = int(
        db_ref.qualified_events.count_documents(
            {"qualified_at": {"$gte": day_start, "$lt": day_end}}
        )
    )

    checkin_hits = 0
    claim_hits = 0
    invitee_first_referral_at: dict[int, datetime] = {}
    for row in referrals:
        invitee_user_id = row.get("invitee_user_id")
        created_at_utc = _coerce_utc(row.get("created_at_utc"))
        if invitee_user_id is None or not created_at_utc:
            continue
        prev_created_at = invitee_first_referral_at.get(invitee_user_id)
        if prev_created_at is None or created_at_utc < prev_created_at:
            invitee_first_referral_at[invitee_user_id] = created_at_utc
        cutoff = created_at_utc + timedelta(hours=72)

        user_doc = db_ref.users.find_one({"user_id": invitee_user_id}, {"first_checkin_at": 1}) or {}
        first_checkin_at = _coerce_utc(user_doc.get("first_checkin_at"))
        if first_checkin_at and first_checkin_at <= cutoff:
            checkin_hits += 1

        claim_doc = db_ref.new_joiner_claims.find_one({"uid": invitee_user_id}, {"claimed_at": 1}) or {}
        claimed_at = _coerce_utc(claim_doc.get("claimed_at"))
        if claimed_at and claimed_at <= cutoff:
            claim_hits += 1

    checkin_72h_rate = _safe_rate(checkin_hits, new_referrals)
    claim_proxy_72h_rate = _safe_rate(claim_hits, new_referrals)

    subscribed_72h_hits = 0
    invitee_ids = list(invitee_first_referral_at.keys())
    subscription_by_uid: dict[int, datetime | None] = {}

    if invitee_ids:
        try:
            cursor = db_ref.subscription_cache.find(
                {
                    "user_id": {"$in": invitee_ids},
                    "subscribed": {"$ne": False},
                },
                {
                    "user_id": 1,
                    "first_subscribed_at_utc": 1,
                    "checked_at": 1,
                    "updated_at": 1,
                },
            )
            for doc in cursor:
                uid = doc.get("user_id")
                if uid is None:
                    continue
                first_seen = _coerce_utc(doc.get("first_subscribed_at_utc"))
                if not first_seen:
                    first_seen = _coerce_utc(doc.get("checked_at"))
                subscription_by_uid[int(uid)] = first_seen
        except Exception:
            subscription_by_uid = {}

    for uid, referral_created_at in invitee_first_referral_at.items():
        first_seen = subscription_by_uid.get(uid)
        if first_seen and first_seen <= (referral_created_at + timedelta(hours=72)):
            subscribed_72h_hits += 1

    invitee_channel_sub_72h_rate = _safe_rate(subscribed_72h_hits, max(new_referrals, 1))

    window_7d_start = day_start - timedelta(days=6)
    window_filter = {"day_utc": {"$gte": window_7d_start.date().isoformat(), "$lte": day_utc}}
    snapshot_rows = list(
        db_ref.affiliate_daily_kpis.find(
            window_filter,
            {"new_referrals": 1, "qualified": 1},
        )
    )
    if len(snapshot_rows) == 7:
        new_referrals_7d = int(sum(int(r.get("new_referrals", 0) or 0) for r in snapshot_rows))
        qualified_7d = int(sum(int(r.get("qualified", 0) or 0) for r in snapshot_rows))
    else:
        new_referrals_7d = int(
            db_ref.pending_referrals.count_documents(
                {"created_at_utc": {"$gte": window_7d_start, "$lt": day_end}}
            )
        )
        qualified_7d = int(
            db_ref.qualified_events.count_documents(
                {"qualified_at": {"$gte": window_7d_start, "$lt": day_end}}
            )
        )

    quality_rate_7d = _safe_rate(qualified_7d, new_referrals_7d)
    active_referrers_7d = int(
        len(
            db_ref.qualified_events.distinct(
                "referrer_id",
                {
                    "qualified_at": {"$gte": window_7d_start, "$lt": day_end},
                    "referrer_id": {"$ne": None},
                },
            )
        )
    )

    payload = {
        "day_utc": day_utc,
        "new_referrals": new_referrals,
        "qualified": qualified,
        "checkin_72h_rate": checkin_72h_rate,
        "claim_proxy_72h_rate": claim_proxy_72h_rate,
        "invitee_channel_sub_72h_rate": invitee_channel_sub_72h_rate,
        "new_referrals_7d": new_referrals_7d,
        "qualified_7d": qualified_7d,
        "quality_rate_7d": quality_rate_7d,
        "active_referrers_7d": active_referrers_7d,
        "computed_at_utc": now_utc_ts,
    }
    db_ref.affiliate_daily_kpis.update_one({"day_utc": day_utc}, {"$set": payload}, upsert=True)
    logger.info(
        "[AFF_KPI] day=%s new=%s qualified=%s checkin72=%s claim72=%s q7=%s qr7=%s",
        day_utc,
        new_referrals,
        qualified,
        checkin_72h_rate,
        claim_proxy_72h_rate,
        qualified_7d,
        quality_rate_7d,
    )
    return payload


def compute_affiliate_daily_kpi_yesterday() -> dict:
    ensure_affiliate_daily_kpi_indexes()
    target_day = (now_utc() - timedelta(days=1)).date().isoformat()
    return compute_affiliate_daily_kpi(target_day)



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

def _record_referral_event(inviter_id: int, invitee_id: int, event: str, occurred_at: datetime) -> bool:
    if inviter_id is None or invitee_id is None:
        return False
    try:
        event_doc = _referral_event_doc(inviter_id, invitee_id, event, occurred_at)
        db.referral_events.insert_one(event_doc)
    except DuplicateKeyError:
        logger.info(
            "[SCHED][REFERRAL_LEDGER] duplicate inviter=%s invitee=%s action=%s",
            inviter_id,
            invitee_id,
            event,
        )
        return False

    logger.info(
        "[SCHED][REFERRAL_LEDGER] inviter=%s invitee=%s action=%s",
        inviter_id,
        invitee_id,
        "settled" if event == "referral_settled" else "revoked",
    )
    return True


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
    for row in results:
        uid = row.get("_id")
        if uid is None:
            continue

        logger.info(
            "[SCHED][REFERRAL_SNAPSHOT] uid=%s weekly=%s monthly=%s total=%s",
            uid,
            int(row.get("weekly_referrals", 0)),
            int(row.get("monthly_referrals", 0)),
            int(row.get("total_referrals", 0)),
        )

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



def _has_severe_deny(invitee_user_id: int) -> bool:
    reason = db.pending_referrals.find_one(
        {"invitee_user_id": int(invitee_user_id), "revoked_reason": {"$in": ["blocked", "abuse", "deny_severe"]}},
        {"_id": 1},
    )
    return bool(reason)


def _resolve_referrer_id(invitee_user_id: int) -> int | None:
    row = db.pending_referrals.find_one(
        {"invitee_user_id": int(invitee_user_id), "status": "awarded"},
        {"inviter_user_id": 1},
    )
    inviter = (row or {}).get("inviter_user_id")
    return int(inviter) if inviter is not None else None


def _simulate_pool_for_gate_day(gate_day: int) -> str:
    if gate_day == 3:
        return "T1"
    if gate_day == 7:
        return "T2"
    if gate_day == 15:
        return "T3"
    return "T4"


def _simulated_ledger_dedup_key(*, inviter_user_id: int, invitee_user_id: int, gate_day: int, tier: str) -> str:
    return f"SIM_GATE:{int(inviter_user_id)}:{int(invitee_user_id)}:{int(gate_day)}:{tier}"


def _derive_abuse_flags_for_invitee(invitee_user_id: int, now_utc_ts: datetime) -> list[str]:
    flags = []
    try:
        deny_count = db.referral_audit.count_documents(
            {
                "invitee_user_id": int(invitee_user_id),
                "created_at": {"$gte": now_utc_ts - timedelta(days=7), "$lt": now_utc_ts},
                "reason": {"$in": ["deny", "deny_severe", "blocked", "abuse"]},
            }
        )
        if int(deny_count or 0) > 0:
            flags.append("referral_audit_deny_7d")
    except Exception:
        pass

    try:
        cooldown_key = f"cooldown:uid:{int(invitee_user_id)}"
        if db.claim_rate_limits.find_one({"key": cooldown_key}, {"_id": 1}):
            flags.append("cooldown_uid")
    except Exception:
        pass

    try:
        kill_key = f"kill:uid:{int(invitee_user_id)}"
        if db.claim_rate_limits.find_one({"key": kill_key}, {"_id": 1}):
            flags.append("kill_uid")
    except Exception:
        pass

    return flags


def evaluate_affiliate_simulated_ledgers(batch_limit: int = 500) -> int:
    if str(os.getenv("AFFILIATE_SIMULATE", "0")).strip() != "1":
        return 0

    now_utc_ts = now_utc()
    now_kl_date = now_utc_ts.astimezone(KL_TZ).date()
    created_or_updated = 0
    final_statuses = {"ISSUED", "OUT_OF_STOCK", "REJECTED"}

    rows = db.pending_referrals.find(
        {"status": "awarded", "invitee_user_id": {"$exists": True}, "inviter_user_id": {"$exists": True}},
        {"invitee_user_id": 1, "inviter_user_id": 1},
    ).limit(batch_limit)

    for row in rows:
        invitee_user_id = row.get("invitee_user_id")
        inviter_user_id = row.get("inviter_user_id")
        if not isinstance(invitee_user_id, int) or not isinstance(inviter_user_id, int):
            continue

        user_doc = db.users.find_one({"user_id": invitee_user_id}, {"joined_main_at": 1, "total_xp": 1, "monthly_xp": 1}) or {}
        joined_main_at = _coerce_utc(user_doc.get("joined_main_at"))
        if not joined_main_at:
            continue
        age_days = (now_kl_date - joined_main_at.astimezone(KL_TZ).date()).days
        for gate_day in (3, 7, 15, 30):
            if age_days < gate_day:
                continue

            tier = _simulate_pool_for_gate_day(gate_day)

            try:
                status = _get_chat_member_status(invitee_user_id)
                still_in_group = status in {"member", "administrator", "creator"}
            except Exception:
                still_in_group = False

            dedup_key = _simulated_ledger_dedup_key(
                inviter_user_id=inviter_user_id,
                invitee_user_id=invitee_user_id,
                gate_day=gate_day,
                tier=tier,
            )
            db.affiliate_ledger.update_one(
                {"dedup_key": dedup_key},
                {
                    "$setOnInsert": {
                        "ledger_type": "AFFILIATE_SIMULATION",
                        "user_id": int(inviter_user_id),
                        "invitee_user_id": int(invitee_user_id),
                        "year_month": None,
                        "tier": tier,
                        "pool_id": tier,
                        "status": "SIMULATED_PENDING",
                        "dedup_key": dedup_key,
                        "voucher_code": None,
                        "risk_flags": [],
                        "created_at": now_utc_ts,
                    },
                },
                upsert=True,
            )

            ledger_doc = db.affiliate_ledger.find_one({"dedup_key": dedup_key}, {"status": 1}) or {}
            if ledger_doc.get("status") in final_statuses:
                logger.info(
                    "[SCHED][AFF_SIM] action=skip_final_status dedup_key=%s status=%s",
                    dedup_key,
                    ledger_doc.get("status"),
                )
                continue

            db.affiliate_ledger.update_one(
                {
                    "dedup_key": dedup_key,
                    "status": {"$in": [None, "SIMULATED_PENDING", "PENDING_MANUAL", "PENDING_REVIEW", "APPROVED"]},
                },
                {
                    "$set": {
                        "status": "SIMULATED_PENDING",
                        "simulate": True,
                        "would_issue_pool": tier,
                        "gate_day": int(gate_day),
                        "evaluated_at_utc": now_utc_ts,
                        "still_in_group": bool(still_in_group),
                        "xp_total": user_doc.get("total_xp"),
                        "monthly_xp": user_doc.get("monthly_xp"),
                        "abuse_flags": _derive_abuse_flags_for_invitee(invitee_user_id, now_utc_ts),
                        "updated_at": now_utc_ts,
                    },
                },
            )
            created_or_updated += 1

    if created_or_updated:
        logger.info("[SCHED][AFF_SIM] action=processed_ledgers count=%s", created_or_updated)
    return created_or_updated


def confirm_qualified_invitees(batch_limit: int = 200) -> int:
    now_utc_ts = now_utc()
    cutoff = now_utc_ts - timedelta(hours=48)
    rows = db.users.find(
        {
            "first_checkin_at": {"$exists": True},
            "joined_main_at": {"$lte": cutoff},
            "blocked": {"$ne": True},
        },
        {"user_id": 1},
    ).limit(batch_limit)

    created = 0
    for row in rows:
        invitee_user_id = row.get("user_id")
        if not isinstance(invitee_user_id, int):
            continue
        if db.qualified_events.find_one({"invitee_id": invitee_user_id}, {"_id": 1}):
            continue
        if _has_severe_deny(invitee_user_id):
            continue

        try:
            status = _get_chat_member_status(invitee_user_id)
        except Exception:
            continue
        if status not in {"member", "administrator", "creator"}:
            continue

        referrer_id = _resolve_referrer_id(invitee_user_id)
        if mark_invitee_qualified(db, invitee_id=invitee_user_id, referrer_id=referrer_id, now_utc=now_utc_ts):
            created += 1

    if created:
        logger.info("[SCHED][QUALIFIED] created=%s", created)
    return created

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
                "$or": [
                    {
                        "status": "pending",
                        "created_at_utc": {"$lte": cutoff},
                        "$or": [
                            {"next_retry_at_utc": {"$exists": False}},
                            {"next_retry_at_utc": {"$lte": now_utc_ts}},
                        ],
                    },
                    {
                        "status": "pending_channel",
                        "$or": [
                            {"next_retry_at_utc": {"$exists": False}},
                            {"next_retry_at_utc": {"$lte": now_utc_ts}},
                        ],
                    },
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
            return_document=ReturnDocument.BEFORE,
        )
        if not pending:
            break
        scanned += 1
        pending_id = pending.get("_id")
        invitee_user_id = pending.get("invitee_user_id")
        inviter_user_id = pending.get("inviter_user_id")
        pending_status = pending.get("status")
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

            step = "check_channel"
            is_subscribed, channel_reason = _check_official_channel_subscribed_sync(invitee_user_id)
            if not is_subscribed:
                if pending_status == "pending_channel":
                    pending_channel_since = _coerce_utc(pending.get("pending_channel_since_utc"))
                    if pending_channel_since and now_utc_ts > pending_channel_since + timedelta(days=REFERRAL_CHANNEL_EXPIRE_DAYS):
                        db.pending_referrals.update_one(
                            {"_id": pending_id},
                            {
                                "$set": {
                                    "status": "revoked",
                                    "revoked_reason": "channel_not_subscribed_expired",
                                    "revoked_at": now_utc_ts,
                                },
                                "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                            },
                        )
                        _record_referral_event(inviter_user_id, invitee_user_id, "referral_revoked", now_utc_ts)
                        revoked += 1
                        logger.info(
                            "[SCHED][REFERRAL] revoked inviter=%s invitee=%s reason=channel_not_subscribed_expired",
                            inviter_user_id,
                            invitee_user_id,
                        )
                        continue

                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "pending_channel",
                            "pending_channel_since_utc": _coerce_utc(pending.get("pending_channel_since_utc")) or now_utc_ts,
                            "next_retry_at_utc": now_utc_ts + timedelta(hours=REFERRAL_CHANNEL_RETRY_HOURS),
                            "last_fail_reason": "channel_not_subscribed",
                            "last_channel_check_reason": channel_reason,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        "$inc": {"retry_count": 1},
                    },
                )
                logger.info(
                    "[SCHED][REFERRAL] pending_channel inviter=%s invitee=%s retry_in_h=%s",
                    inviter_user_id,
                    invitee_user_id,
                    REFERRAL_CHANNEL_RETRY_HOURS,
                )
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
                "[SCHED][REFERRAL] awarded inviter=%s invitee=%s",
                inviter_user_id,
                invitee_user_id,
            )
            logger.info(
                "[SCHED][REFERRAL] award_ok inviter=%s invitee=%s ref_total=%s xp_added=%s bonus_added=%s hold_hours=%s users_counter_update_attempted=%s",
                inviter_user_id,
                invitee_user_id,
                ref_total,
                actual_xp_added,
                actual_bonus_added,
                REFERRAL_HOLD_HOURS,
                False,
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

    confirm_qualified_invitees()
    logger.info(
        "[SCHED][REFERRAL] settle scanned=%s awarded=%s revoked=%s batch_limit=%s",
        scanned,
        awarded,
        revoked,
        batch_limit,        
    )
