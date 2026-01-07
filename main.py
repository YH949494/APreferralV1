from flask import (
    Flask, request, jsonify, send_from_directory,
    render_template, redirect, url_for, flash, g, Blueprint
)
from flask_cors import CORS
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.constants import ParseMode
from html import escape as html_escape
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatJoinRequestHandler, ChatMemberHandler,
    CallbackQueryHandler, ContextTypes, MessageHandler, filters
)
from telegram.error import BadRequest
from datetime import datetime, timedelta, timezone
from werkzeug.exceptions import HTTPException

from config import (
    KL_TZ,
    STREAK_MILESTONES,
    XP_BASE_PER_CHECKIN,
    WEEKLY_XP_BUCKET,
    WEEKLY_REFERRAL_BUCKET,
)

from referral_rules import (
    compute_referral_stats,
    ensure_referral_indexes,
    grant_referral_rewards,
    upsert_referral_and_update_user_count,
)

from bson.json_util import dumps
from xp import ensure_xp_indexes, grant_xp, now_utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from vouchers import vouchers_bp, ensure_voucher_indexes

from pymongo import MongoClient, DESCENDING, ASCENDING  # keep if used elsewhere
import os, asyncio, traceback, csv, io, requests, logging
import pytz

FIRST_CHECKIN_BONUS_XP = int(os.getenv("FIRST_CHECKIN_BONUS_XP", "200"))
WELCOME_BONUS_XP = int(os.getenv("WELCOME_BONUS_XP", "20"))
WELCOME_WINDOW_HOURS = int(os.getenv("WELCOME_WINDOW_HOURS", "48"))
WELCOME_WINDOW_DAYS = 7

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"
GROUP_ID = -1002304653063
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ----------------------------
# Xmas Gift Delight placeholders (easy to adjust)
# ----------------------------
CHANNEL_USERNAME = "@advantplayofficial"
CHANNEL_ID = -1002396761021
_RAW_OFFICIAL_CHANNEL_ID = os.getenv("OFFICIAL_CHANNEL_ID")
try:
    OFFICIAL_CHANNEL_ID = int(_RAW_OFFICIAL_CHANNEL_ID) if _RAW_OFFICIAL_CHANNEL_ID not in (None, "") else CHANNEL_ID
except (TypeError, ValueError):
    OFFICIAL_CHANNEL_ID = CHANNEL_ID

XMAS_CAMPAIGN_START = datetime(2025, 12, 1)
XMAS_CAMPAIGN_END = datetime(2025, 12, 31, 23, 59, 59)

def _to_kl_date(dt_any):
    """Accepts aware/naive datetime or ISO string and returns date in KL."""
    if dt_any is None:
        return None
    if isinstance(dt_any, str):
        s = dt_any.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    elif isinstance(dt_any, datetime):
        dt = dt_any
    else:
        return None
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    return dt.astimezone(KL_TZ).date()


def _week_window_utc(reference: datetime | None = None):
    """Return (start_utc, end_utc, start_local) for the current week (Mon 00:00)."""

    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    start_local = (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_local = start_local + timedelta(days=7)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), start_local


def _month_window_utc(reference: datetime | None = None):
    """Return (start_utc, end_utc, start_local) for the month containing ``reference``."""

    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    start_local = ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start_local.month == 12:
        end_local = start_local.replace(year=start_local.year + 1, month=1)
    else:
        end_local = start_local.replace(month=start_local.month + 1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), start_local


def _previous_month_window_utc(reference: datetime | None = None):
    """Return (start_utc, end_utc, start_local) for the month that just ended."""

    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    this_month_start = ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = this_month_start
    prev_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
    return (
        prev_month_start.astimezone(timezone.utc),
        prev_month_end.astimezone(timezone.utc),
        prev_month_start,
    )


def _event_time_expr():
    return {"$ifNull": ["$created_at", "$ts"]}


def _referral_time_expr():
    return {
        "$ifNull": [
            "$success_at",
            {"$ifNull": ["$confirmed_at", {"$ifNull": ["$validated_at", "$created_at"]}]},
        ]
    }


def recompute_xp_totals(start_utc, end_utc, limit: int | None = None, user_id: int | None = None):
    """Aggregate XP from xp_events between the given UTC boundaries."""

    time_expr = {
        "$and": [
            {"$gte": [_event_time_expr(), start_utc]},
            {"$lt": [_event_time_expr(), end_utc]},
        ]
    }
    match_filters = [
        {"$expr": time_expr},
        {"user_id": {"$ne": None}},
        {"$or": [{"invalidated": {"$exists": False}}, {"invalidated": False}]},
    ]
    if user_id is not None:
        match_filters.append({"user_id": user_id})

    pipeline = [
        {"$match": {"$and": match_filters}},
        {"$group": {"_id": "$user_id", "xp": {"$sum": "$xp"}}},
        {"$sort": {"xp": -1}},
    ]
    if limit:
        pipeline.append({"$limit": limit})

    logger.info(
        "[xp_recompute] start=%s end=%s limit=%s user=%s",
        start_utc.isoformat(),
        end_utc.isoformat(),
        limit,
        user_id,
    )
    return list(xp_events_collection.aggregate(pipeline))


def recompute_referral_counts(start_utc, end_utc, limit: int | None = None, user_id: int | None = None):
    """Aggregate successful referrals within the window."""

    time_expr = {
        "$and": [
            {"$gte": [_referral_time_expr(), start_utc]},
            {"$lt": [_referral_time_expr(), end_utc]},
        ]
    }
    base_match = [
        {"$expr": time_expr},
        {"status": "success"},
        {"$or": [
            {"referrer_user_id": {"$exists": True}},
            {"referrer_id": {"$exists": True}},
        ]},
        {"$or": [
            {"referrer_user_id": {"$ne": None}},
            {"referrer_id": {"$ne": None}},
        ]},
        {"$or": [
            {"invitee_user_id": {"$exists": True, "$ne": None}},
            {"referred_user_id": {"$exists": True, "$ne": None}},
        ]},        
    ]
    if user_id is not None:
        base_match.append({"$or": [
            {"referrer_user_id": user_id},
            {"referrer_id": user_id},
        ]})
        
    pipeline = [
        {"$match": {"$and": base_match}},
        {
            "$group": {
                "_id": {"$ifNull": ["$referrer_user_id", "$referrer_id"]},
                "total_valid": {"$sum": 1},
            }
        },
        {"$sort": {"total_valid": -1, "_id": 1}},
    ]
    if limit:
        pipeline.append({"$limit": limit})
    pipeline.extend(
        [
            {
                "$lookup": {
                    "from": "users",
                    "localField": "_id",
                    "foreignField": "user_id",
                    "as": "user",
                }
            },
            {"$unwind": {"path": "$user", "preserveNullAndEmptyArrays": True}},
        ]
    )
    
    logger.info(
        "[xp_recompute] referrals start=%s end=%s limit=%s user=%s",
        start_utc.isoformat(),
        end_utc.isoformat(),
        limit,
        user_id,
    )
    return list(referrals_collection.aggregate(pipeline))

# ----------------------------
# MongoDB Setup
# ----------------------------
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
history_collection = db["weekly_leaderboard_history"]
bonus_voucher_collection = db["bonus_voucher"]
admin_cache_col = db["admin_cache"]
xp_events_collection = db["xp_events"]
referrals_collection = db["referrals"]
welcome_eligibility_collection = db["welcome_eligibility"]
monthly_xp_history_collection = db["monthly_xp_history"]
monthly_xp_history_collection.create_index([("month", ASCENDING)])
monthly_xp_history_collection.create_index([("user_id", ASCENDING), ("month", ASCENDING)], unique=True)
audit_events_collection = db["audit_events"]

def call_bot_in_loop(coro, timeout=15):
    loop = getattr(app_bot, "_running_loop", None)
    if loop is None:
        raise RuntimeError("Bot loop not running yet")
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=timeout)

def _format_mention(u: dict) -> str:
    """Return a HTML-safe mention for announcements."""
    user_id = u.get("user_id")
    if u.get("username"):
        label = f"@{u['username']}"
    elif u.get("first_name"):
        label = u["first_name"]
    else:
        label = "player"

    safe_label = html_escape(label)
    if user_id:
        return f'<a href="tg://user?id={int(user_id)}">{safe_label}</a>'
    return safe_label

def _announce_text(u: dict, which: str, value: int) -> str:
    who = _format_mention(u)
    if which == "weekly_xp":
        return f"🎉 {who} just hit <b>{value:,} weekly XP</b>! On a streak! ⚡"
    else:  # which == "weekly_ref"
        return f"🚀 {who} reached <b>{value} weekly referrals</b>! Absolute legend! 🏆"

def _send_group_message_sync(text: str):
    try:
        call_bot_in_loop(
            app_bot.bot.send_message(chat_id=GROUP_ID, text=text, parse_mode="HTML")
        )
        return
    except Exception as e:
        print(f"[announce] primary send failed: {e}; falling back to HTTP API")

    try:
        resp = requests.post(
            f"{API_BASE}/sendMessage",
            json={"chat_id": GROUP_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        if not resp.ok or not data.get("ok"):
            raise RuntimeError(data.get("description") or resp.text)
    except Exception as http_err:
        print(f"[announce] fallback send failed: {http_err}")
        
def _too_soon(u: dict, gap_minutes=2) -> bool:
    ts = u.get("last_shout_at")
    if not ts:
        return False
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    return datetime.now(pytz.UTC) - ts < timedelta(minutes=gap_minutes)

def maybe_shout_milestones(user_id: int):
    """
    Announce:
      - every +WEEKLY_XP_BUCKET of weekly_xp
      - every +WEEKLY_REFERRAL_BUCKET of weekly_referral_count
    """
    u = users_collection.find_one({"user_id": user_id})
    if not u:
        return

    weekly_xp = int(u.get("weekly_xp", 0))
    weekly_ref = int(u.get("weekly_referral_count", 0))

    # current buckets
    xp_bucket_now = weekly_xp // WEEKLY_XP_BUCKET
    ref_bucket_now = weekly_ref // WEEKLY_REFERRAL_BUCKET

    # last announced buckets
    xp_bucket_prev = int(u.get("xp_weekly_milestone_bucket", 0))
    ref_bucket_prev = int(u.get("ref_weekly_milestone_bucket", 0))

    # Determine if new buckets were crossed
    xp_hit  = xp_bucket_now  > xp_bucket_prev and xp_bucket_now  > 0
    ref_hit = ref_bucket_now > ref_bucket_prev and ref_bucket_now > 0

    # Always persist bucket progress immediately (so we never "lose" a milestone)
    updates = {}
    if xp_hit:
        updates["xp_weekly_milestone_bucket"] = xp_bucket_now
    if ref_hit:
        updates["ref_weekly_milestone_bucket"] = ref_bucket_now
    if updates:
        users_collection.update_one({"user_id": user_id}, {"$set": updates})

    # Throttle only the sending to the group (not the state update above)
    sent_any = False
    if xp_hit or ref_hit:
        if not _too_soon(u):
            if xp_hit:
                _send_group_message_sync(
                    _announce_text(u, "weekly_xp", xp_bucket_now * WEEKLY_XP_BUCKET)
                )
                sent_any = True
            if ref_hit:
                _send_group_message_sync(
                    _announce_text(u, "weekly_ref", ref_bucket_now * WEEKLY_REFERRAL_BUCKET)
                )
                sent_any = True
            if sent_any:
                users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"last_shout_at": datetime.utcnow()}}
                )
        else:
            # Optional: keep a lightweight log to spot suppressed sends in logs
            print(f"[Milestone] Suppressed (throttle) user_id={user_id} "
                  f"xp_hit={xp_hit} ref_hit={ref_hit}")

def maybe_give_first_checkin_bonus(user_id: int):
    grant_xp(db, user_id, "first_checkin", "first_checkin", FIRST_CHECKIN_BONUS_XP)

def _find_referral_record(invitee_user_id: int):
    return referrals_collection.find_one(
        {"invitee_user_id": invitee_user_id}
    ) or referrals_collection.find_one(
        {"referred_user_id": invitee_user_id}
    )

def _resolve_referrer_id_from_invite_link(invite_link) -> int | None:
    if not invite_link:
        return None
    invite_name = getattr(invite_link, "name", None)
    if invite_name and invite_name.startswith("ref-"):
        try:
            return int(invite_name.split("ref-")[1])
        except (IndexError, ValueError):
            return None
    invite_url = getattr(invite_link, "invite_link", None)
    if invite_url:
        ref_doc = users_collection.find_one({"referral_invite_link": invite_url})
        if ref_doc:
            return ref_doc.get("user_id")
    return None

def _ensure_welcome_eligibility(uid: int) -> dict | None:
    if not uid:
        return None
    now = datetime.now(KL_TZ)        
    user_doc = users_collection.find_one({"user_id": uid}, {"joined_main_at": 1})
    joined_main_at = user_doc.get("joined_main_at") if user_doc else None
    if not joined_main_at:
        logger.info(
            "[WELCOME] eligibility_skip uid=%s reason=missing_joined_main_at",
            uid,
        )
        return None
    joined_main_kl = joined_main_at.astimezone(KL_TZ) if joined_main_at.tzinfo else joined_main_at.replace(tzinfo=KL_TZ)
    if joined_main_kl < (now - timedelta(days=WELCOME_WINDOW_DAYS)):
        logger.info(
            "[WELCOME] eligibility_skip uid=%s reason=not_new_user joined_main_at=%s",
            uid,
            joined_main_kl.isoformat(),
        )
        return None
    eligible_until = joined_main_kl + timedelta(days=WELCOME_WINDOW_DAYS)
    welcome_eligibility_collection.update_one(
        {"uid": uid},
        {
            "$setOnInsert": {
                "uid": uid,
                "first_seen_at": now,
                "claimed": False,
                "claimed_at": None,
            },
            "$set": {"eligible_until": eligible_until},
        },
        upsert=True,
    )
    return welcome_eligibility_collection.find_one({"uid": uid})

async def _check_official_channel_subscribed(bot, uid: int) -> tuple[bool, str]:
    if not uid:
        return False, "missing_uid"
    if OFFICIAL_CHANNEL_ID is None:
        return False, "channel_unset"
    try:
        member = await bot.get_chat_member(chat_id=OFFICIAL_CHANNEL_ID, user_id=uid)
    except BadRequest as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)
    status = getattr(member, "status", None)
    return status in ("member", "administrator", "creator"), ""

def _check_official_channel_subscribed_sync(uid: int) -> tuple[bool, str]:
    if not uid:
        return False, "missing_uid"
    if OFFICIAL_CHANNEL_ID is None:
        return False, "channel_unset"
    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        return False, "missing_token"
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": OFFICIAL_CHANNEL_ID, "user_id": uid},
            timeout=5,
        )
    except requests.RequestException as e:
        return False, str(e)
    if resp.status_code != 200:
        return False, f"http_{resp.status_code}"
    try:
        data = resp.json()
    except ValueError:
        return False, "bad_json"
    if not data.get("ok"):
        return False, "not_ok"
    status = (data.get("result") or {}).get("status")
    return status in ("member", "administrator", "creator"), ""

def _upsert_pending_referral(invitee_user_id: int, referrer_user_id: int, joined_at: datetime):
    referrals_collection.update_one(
        {"invitee_user_id": invitee_user_id},
        {
            "$setOnInsert": {
                "invitee_user_id": invitee_user_id,
                "referrer_user_id": referrer_user_id,
                "created_at": joined_at,
            },
            "$set": {
                "status": "pending",
                "updated_at": joined_at,
                "joined_chat_at": joined_at,
            },
        },
        upsert=True,
    )

async def handle_user_join(
    uid: int,
    username: str | None,
    chat_id: int | None,
    *,
    source: str,
    invite_link=None,
    old_status: str | None = None,
    new_status: str | None = None,
    context: ContextTypes.DEFAULT_TYPE,
):
    logger.info(
        "[join] source=%s chat_id=%s uid=%s uname=%s old=%s new=%s",
        source,
        chat_id,
        uid,
        username or "",
        old_status or "",
        new_status or "",
    )

    if chat_id != GROUP_ID:
        return
    if not uid:
        return

    existing_user = users_collection.find_one({"user_id": uid})
    if existing_user and existing_user.get("joined_once") and existing_user.get("joined_main_at"):
        return

    users_collection.update_one(
        {"user_id": uid},
        {
            "$set": {"username": username, "joined_once": True},
            "$setOnInsert": {
                "xp": 0,
                "referral_count": 0,
                "weekly_referral_count": 0,
                "weekly_xp": 0,
                "monthly_xp": 0,
                "last_checkin": None,
                "status": "Normal",
            },
        },
        upsert=True,
    )
    joined_at = datetime.now(KL_TZ)
    users_collection.update_one(
        {"user_id": uid, "joined_main_at": {"$exists": False}},
        {"$set": {"joined_main_at": joined_at, "joined_at_source": "join_event"}},
    )
    _ensure_welcome_eligibility(uid)
    logger.info(
        "[WELCOME] join_recorded uid=%s joined_main_at=%s",
        uid,
        joined_at.isoformat(),
    )
    
    referrer_id = _resolve_referrer_id_from_invite_link(invite_link)

    existing_referral = _find_referral_record(uid)
    stored_referrer_id = None
    if existing_referral:
        stored_referrer_id = existing_referral.get("referrer_user_id") or existing_referral.get("referrer_id")

    final_referrer_id = referrer_id or stored_referrer_id
    if not final_referrer_id:
        logger.info("[ref] uid=%s resolved_referrer=None reason=no_referrer", uid)
        return

    joined_at = datetime.now(KL_TZ)

    result = upsert_referral_and_update_user_count(
        referrals_collection,
        users_collection,        
        referrer_user_id=final_referrer_id,
        invitee_user_id=uid,
        success_at=joined_at,
        referrer_username=None,
        context={"joined_chat_at": joined_at},
    )
    logger.info(
        "[REFERRAL] record uid=%s referrer=%s matched=%s modified=%s upserted=%s counted=%s",
        uid,
        final_referrer_id,
        result.get("matched"),
        result.get("modified"),
        result.get("upserted"),
        result.get("counted"),
    )    
    invite_link_url = getattr(invite_link, "invite_link", None)
    outcome = {}    
    if result.get("counted"):
        outcome = grant_referral_rewards(
            db,
            users_collection,
            final_referrer_id,
            uid,
        )
        if outcome.get("base_granted"):
            try:
                maybe_shout_milestones(int(final_referrer_id))
            except Exception:
                pass
        logger.info(
            "[REFERRAL] xp_award uid=%s referrer=%s base=%s bonuses=%s",
            uid,
            final_referrer_id,
            outcome.get("base_granted"),
            outcome.get("bonuses_awarded"),
        )                
        referrer_doc = users_collection.find_one({"user_id": final_referrer_id}) or {}
        total_referrals = int(referrer_doc.get("referral_count", 0))
        logger.info(
            "[REFERRAL] success uid=%s referrer=%s counted=%s",
            uid,
            final_referrer_id,
            result.get("counted"),
        )        
        logger.info(
            "[xp] uid=%s add=%s bonus=%s total_referrals=%s upsert=%s",
            final_referrer_id,
            outcome.get("base_granted"),
            outcome.get("bonuses_awarded"),
            total_referrals,
            "inserted" if result.get("upserted") else "updated",
        )
    else:
        referrer_doc = users_collection.find_one({"user_id": final_referrer_id}) or {}
        total_referrals = int(referrer_doc.get("referral_count", 0))
        logger.info(
            "[REFERRAL] success uid=%s referrer=%s counted=%s",
            uid,
            final_referrer_id,
            result.get("counted"),
        )        
        logger.info(
            "[xp] uid=%s add=0 bonus=0 total_referrals=%s upsert=noop",
            final_referrer_id,
            total_referrals,
        )
    logger.info(
        "[referral_award] invitee=%s referrer=%s counted=%s base_granted=%s bonuses_awarded=%s invite_link=%s",
        uid,
        final_referrer_id,
        result.get("counted"),
        outcome.get("base_granted", 0),
        outcome.get("bonuses_awarded", 0),
        invite_link_url,
    )        
    logger.info(
        "[ref] uid=%s resolved_referrer=%s reason=success",
        uid,
        final_referrer_id,
    )

def _has_joined_group(invitee_user_id: int) -> bool:
    user_doc = users_collection.find_one(
        {"user_id": invitee_user_id, "joined_once": True},
        {"_id": 1},
    )

    if user_doc:
        return True
    return bool(
        db.joins.find_one(
            {"user_id": invitee_user_id, "event": "join", "chat_id": GROUP_ID},
            {"_id": 1},
        )
    )

def resolve_legacy_pending_referrals():
    pending_refs = list(referrals_collection.find({"status": "pending"}))
    if not pending_refs:
        return
        
    now = datetime.utcnow()
    for ref in pending_refs:
        invitee_id = ref.get("invitee_user_id") or ref.get("referred_user_id")
        referrer_id = ref.get("referrer_user_id") or ref.get("referrer_id")

        if not invitee_id or not referrer_id:
            referrals_collection.update_one(
                {"_id": ref.get("_id")},
                {"$set": {"status": "inactive", "inactive_at": now}},
            )
            continue
            
        if _has_joined_group(invitee_id):
            result = upsert_referral_and_update_user_count(
                referrals_collection,
                users_collection,
                referrer_id,
                invitee_id,
                success_at=now,                
            )
            if result.get("counted"):
                grant_referral_rewards(
                    db,
                    users_collection,
                    referrer_id,
                    invitee_id,
                )            
        else:
            referrals_collection.update_one(
                {"_id": ref.get("_id")},
                {"$set": {"status": "inactive", "inactive_at": now}},
            )

def ensure_indexes():
    """
    Ensure TTL index on bonus_voucher.end_time so docs auto-expire exactly at end_time.
    If an old index exists with different options, drop and recreate.
    """
    def _dedupe_xp_events_unique_keys():
        dup_groups = xp_events_collection.aggregate(
            [
                {"$match": {"unique_key": {"$exists": True}}},
                {
                    "$group": {
                        "_id": {"user_id": "$user_id", "unique_key": "$unique_key"},
                        "count": {"$sum": 1},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
            ]
        )

        removed = 0
        for group in dup_groups:
            crit = {
                "user_id": group["_id"].get("user_id"),
                "unique_key": group["_id"].get("unique_key"),
            }
            dup_docs = list(
                xp_events_collection.find(crit).sort([("ts", 1), ("_id", 1)])
            )
            to_delete = [d["_id"] for d in dup_docs[1:]]
            if to_delete:
                xp_events_collection.delete_many({"_id": {"$in": to_delete}})
                removed += len(to_delete)

        if removed:
            print(f"🔧 Removed {removed} duplicate xp_events with duplicate unique_key")

    idx_name = "ttl_end_time"
    try:
        bonus_voucher_collection.create_index(
            [("end_time", 1)],
            expireAfterSeconds=0,
            name=idx_name,
        )
        print("✅ TTL index ensured on bonus_voucher.end_time")
    except Exception as e:
        # If an index exists with different options, fix it
        msg = str(e)
        if "already exists with different options" in msg or "ExpireAfterSeconds" in msg or "expireAfterSeconds" in msg:
            try:
                bonus_voucher_collection.drop_index(idx_name)
            except Exception:
                # fallback: find index by key
                for ix in bonus_voucher_collection.list_indexes():
                    if ix.get("key") == {"end_time": 1}:
                        bonus_voucher_collection.drop_index(ix["name"])
                        break
            bonus_voucher_collection.create_index(
                [("end_time", 1)],
                expireAfterSeconds=0,
                name=idx_name,
            )
            print("🔁 Recreated TTL index on bonus_voucher.end_time")
        else:
            print("⚠️ ensure_indexes error:", e)

    # --- joins tracking ---
    db.joins.create_index([("user_id", 1), ("chat_id", 1), ("joined_at", -1)])
    db.joins.create_index([("chat_id", 1), ("joined_at", -1)])
    db.joins.create_index([("via_invite", 1)])

    # --- optional welcome eligibility ---
    db.welcome_eligibility.create_index([("user_id", 1)], unique=True)
    db.welcome_eligibility.create_index([("expires_at", 1)], expireAfterSeconds=0)
    db.welcome_tickets.create_index([("uid", 1)], unique=True)
    db.welcome_tickets.create_index([("cleanup_at", 1)], expireAfterSeconds=0)
    
    xp_events_collection.create_index([("user_id", 1), ("reason", 1)])
    ensure_xp_indexes(db)
            
    ensure_referral_indexes(referrals_collection)
    resolve_legacy_pending_referrals()
    
ensure_indexes()

def get_or_create_referral_invite_link_sync(user_id: int, username: str = "") -> str:
    """
    Create (or reuse) a unique Telegram chat invite link for this user.
    Uses Telegram HTTP API (sync), so no asyncio/event loop issues.
    Caches the link in Mongo to avoid rate limits.
    """
    # 1) Reuse if we already created one
    doc = users_collection.find_one({"user_id": user_id}) or {}
    if doc.get("referral_invite_link"):
        return doc["referral_invite_link"]

    # 2) Create a named invite link: name="ref-<user_id>"
    #    Bot MUST be admin in GROUP_ID with "Invite users via link" permission
    name = f"ref-{user_id}"
    payload = {
        "chat_id": GROUP_ID,
        "name": name,
        # optional controls:
        # "expire_date": int(time.time()) + 30*24*3600,  # 30d expiry
        # "member_limit": 0,  # 0 = unlimited
        # "creates_join_request": False
    }
    r = requests.post(f"{API_BASE}/createChatInviteLink", json=payload, timeout=10)
    data = r.json()
    if not data.get("ok"):
        # Fallback: if creation fails (permissions, etc.), return deep-link so flow still works
        bot_username = os.environ.get("BOT_USERNAME", "")
        deeplink = f"https://t.me/{bot_username}?start=ref{user_id}" if bot_username else ""
        raise RuntimeError(f"createChatInviteLink failed: {data.get('description','unknown')}\n"
                           f"Fallback deeplink: {deeplink}")

    invite = data["result"]
    invite_link = invite["invite_link"]

    # 3) Cache in Mongo
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "username": username or doc.get("username"),
            "referral_invite_link": invite_link,
            "referral_invite_name": name,
            "referral_invite_id": invite.get("invite_link")  # not a numeric id, but keep for auditing
        }},
        upsert=True
    )
    return invite_link

def require_admin_from_query():
    caller_id = request.args.get("user_id", type=int)
    if not caller_id:
        return False, ("Missing user_id", 400)

    doc = admin_cache_col.find_one({"_id": "admins"}) or {}
    ids = set()
    for raw in doc.get("ids", []):
        try:
            ids.add(int(raw))
        except (TypeError, ValueError):
            continue
    if caller_id not in ids:
        return False, ("Admins only", 403)

    return True, None

# Flask app must exist BEFORE blueprint registration
app = Flask(__name__, static_folder="static")
CORS(app, resources={r"/*": {"origins": "*"}})
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")
app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
admin_bp = Blueprint("admin", __name__)


@admin_bp.get("/api/admin/joins/daily")
def joins_daily():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    chat_id = request.args.get("chat_id", type=int)
    if chat_id is None:
        return jsonify({"success": False, "message": "Missing chat_id"}), 400

    days = request.args.get("days", default=14, type=int)
    if days is None or days <= 0:
        return jsonify({"success": False, "message": "days must be positive"}), 400

    since = datetime.utcnow() - timedelta(days=days)
    pipeline = [
        {"$match": {"chat_id": chat_id, "event": "join", "joined_at": {"$gte": since}}},
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$joined_at"}}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    rows = list(db.joins.aggregate(pipeline))
    return jsonify({"chat_id": chat_id, "days": days, "data": rows})


@admin_bp.get("/api/admin/joins/export")
def joins_export():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    chat_id = request.args.get("chat_id", type=int)
    if chat_id is None:
        return jsonify({"success": False, "message": "Missing chat_id"}), 400

    date_from_raw = request.args.get("from")
    date_to_raw = request.args.get("to")
    if not date_from_raw or not date_to_raw:
        return jsonify({"success": False, "message": "from/to required"}), 400

    try:
        date_from = datetime.fromisoformat(date_from_raw)
        date_to = datetime.fromisoformat(date_to_raw)
    except ValueError:
        return jsonify({"success": False, "message": "Invalid date format"}), 400

    cur = db.joins.find(
        {"chat_id": chat_id, "joined_at": {"$gte": date_from, "$lt": date_to}, "event": "join"},
        {
            "_id": 0,
            "user_id": 1,
            "username": 1,
            "first_name": 1,
            "last_name": 1,
            "joined_at": 1,
            "via_invite": 1,
            "invite_name": 1,
        },
    )
    return jsonify(list(cur))


app.register_blueprint(admin_bp)
    
# ---- Always return JSON on errors (prevents "Invalid JSON") ----
@app.errorhandler(HTTPException)
def _json_http_exc(e):
    code = e.code or 500
    return jsonify({"code": "http_error", "status": code, "message": e.description}), code

@app.errorhandler(Exception)
def _json_any_exc(e):
    try:
        import traceback; traceback.print_exc()
    except Exception:
        pass
    return jsonify({"code": "server_error", "message": str(e)}), 500
    
# Telegram bot
app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
@app.route("/api/is_admin")
def api_is_admin():
    try:
        user_id = int(request.args.get("user_id"))

        doc = admin_cache_col.find_one({"_id": "admins"}) or {}
        ids = set(doc.get("ids", []))
        is_admin = user_id in ids

        # optional: cache a per-user flag for faster UI checks
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"is_admin": is_admin, "is_admin_checked_at": datetime.utcnow()}},
            upsert=True
        )

        return jsonify({
            "success": True,
            "is_admin": is_admin,
            "source": "cache",
            "refreshed_at": doc.get("refreshed_at")
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

async def refresh_admin_ids(context: ContextTypes.DEFAULT_TYPE):
    try:
        admins = await context.bot.get_chat_administrators(chat_id=GROUP_ID)
        ids = [a.user.id for a in admins]
        admin_cache_col.update_one(
            {"_id": "admins"},
            {"$set": {"ids": ids, "refreshed_at": datetime.utcnow()}},
            upsert=True,
        )
        print(f"👑 Admin cache refreshed: {len(ids)} IDs")
    except Exception as e:
        print(f"⚠️ refresh_admin_ids error: {e}")

# -------------------------------
# ✅ Daily Check-in Logic
# -------------------------------
def streak_progress_bar(streak: int) -> str:
    milestones_sorted = sorted(STREAK_MILESTONES.keys())
    next_m = next((m for m in milestones_sorted if streak < m), milestones_sorted[-1])
    filled = min(streak, next_m)
    boxes = int((filled / next_m) * 10)
    return f"[{'■'*boxes}{'□'*(10-boxes)}] {filled}/{next_m} days ➜ next: {next_m}d"

async def process_checkin(user_id, username, region, update=None):
    """Daily check-in with repeatable milestones. Day boundary = KL time."""
    now_kl = datetime.now(KL_TZ)
    today_kl = now_kl.date()

    user = users_collection.find_one({"user_id": user_id}) or {}
    last = user.get("last_checkin")
    streak = int(user.get("streak", 0))

    last_kl_date = _to_kl_date(last)

    # Same-day guard
    if last_kl_date == today_kl:
        msg = f"⚠️ Already checked in today. 🔥 Streak: {streak} days."
        if update and getattr(update, "message", None):
            await update.message.reply_text(msg)
        return {"success": False, "message": msg}

    # Advance/reset streak
    if last_kl_date == (today_kl - timedelta(days=1)):
        streak += 1
    else:
        streak = 1

        maybe_give_first_checkin_bonus(int(user_id))


    base_xp = XP_BASE_PER_CHECKIN
    bonus_xp = STREAK_MILESTONES.get(streak, 0)

    now_utc_ts = now_utc()
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "region": region,
                "last_checkin": now_utc_ts,
                "streak": streak
            },
            "$max": {"longest_streak": streak},
            "$setOnInsert": {
                "weekly_xp": 0,
                "monthly_xp": 0,
                "xp": 0,
                "status": "Normal",                
            },
        },
        upsert=True
    )

    checkin_key = f"checkin:{today_kl.strftime('%Y%m%d')}"
    grant_xp(db, user_id, "checkin", checkin_key, base_xp + bonus_xp)
    
    maybe_shout_milestones(int(user_id))

    labels = {7: "🎉 7-day streak bonus!", 14: "🔥 14-day streak bonus!", 28: "🏆 28-day streak bonus!"}
    lines = [
        f"✅ Check-in successful! (+{base_xp} XP)",
        f"🔥 Current streak: {streak} days."
    ]
    if bonus_xp:
        lines.append(f"{labels[streak]} +{bonus_xp} XP")
    lines.append(streak_progress_bar(streak))

    msg = "\n".join(lines)
    if update and getattr(update, "message", None):
        await update.message.reply_text(msg)

    return {"success": True, "message": msg}

@app.route("/api/streak/<int:user_id>")
def api_streak(user_id):
    u = users_collection.find_one({"user_id": user_id}) or {}
    streak = int(u.get("streak", 0))
    longest = int(u.get("longest_streak", 0))
    return jsonify({
        "success": True,
        "streak": streak,
        "longest_streak": longest,
        "bar": streak_progress_bar(streak)
    })
    
# -------------------------------
# ✅ API Route for Frontend
# -------------------------------
@app.route("/api/checkin", methods=["POST"])
def api_checkin():
    """Mini-app triggers this after region is set"""
    try:
        data = request.get_json(silent=True) or {}
        user_id = data.get("user_id")
        username = data.get("username", "unknown")

        if not user_id:
            return jsonify({"success": False, "error": "Missing user_id"}), 400
 
        user = users_collection.find_one({"user_id": int(user_id)})
        if not user or "region" not in user:
            return jsonify({"success": False, "error": "Region not set"}), 400

        # ✅ Call check-in logic
        result = asyncio.run(
            process_checkin(int(user_id), username, user["region"])
        )

        # ✅ Always calculate next reset time (12AM UTC+8)
        tz_utc8 = pytz.timezone("Asia/Kuala_Lumpur")  # or Asia/Singapore
        now_utc8 = datetime.now(tz_utc8)
        tomorrow_midnight = (now_utc8 + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        response = {
            "next_checkin_time": tomorrow_midnight.astimezone(pytz.UTC).isoformat()
        }

        # Merge success/error message from process_checkin
        if result and result.get("success"):
            response.update(result)
        else:
            response.update({"success": False, "message": "⚠️ Already checked in today."})

        return jsonify(response)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
        
@app.route("/api/region-status/<int:user_id>", methods=["GET"])
def api_region_status(user_id):
    """Check if user already has region set"""
    user = users_collection.find_one({"user_id": user_id})
    if user and "region" in user:
        return jsonify({"region": user["region"], "locked": True})
    return jsonify({"region": None, "locked": False})

@app.route("/api/set-region/<int:user_id>", methods=["POST"])
def api_set_region(user_id):
    """Set region only if not already set"""
    data = request.json
    region = data.get("region")

    if not region:
        return jsonify({"success": False, "error": "Region required"}), 400

    user = users_collection.find_one({"user_id": user_id})
    if user and "region" in user:
        return jsonify({"success": False, "error": "Region already set", "locked": True})

    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {"region": region}},
        upsert=True
    )
    return jsonify({"success": True, "region": region, "locked": True})

@app.route("/")
def home():
    return "Bot is alive!"

@app.route("/miniapp")
def serve_mini_app():
    return send_from_directory("static", "index.html")

@app.route("/api/referral")
def api_referral():
    user_id_raw = request.args.get("user_id")
    try:
        user_id = int(user_id_raw)
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "user_id is required"}), 400

    username = request.args.get("username") or "unknown"
    logger.info("[api_referral] uid=%s username=%s", user_id, username)
    
    success = True
    link = None
    error = None
    stats = {"total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0}
    
    try:
        link = get_or_create_referral_invite_link_sync(user_id, username)
        logger.info("[api_referral] link_generated uid=%s", user_id)        
    except Exception as e:
        success = False
        error = str(e)
        bot_username = os.environ.get("BOT_USERNAME", "")
        link = f"https://t.me/{bot_username}?start=ref{user_id}" if bot_username else None
        logger.warning("[api_referral] link_generation_failed uid=%s error=%s", user_id, e)

    try:
        stats = compute_referral_stats(referrals_collection, user_id)
    except Exception:
        logger.exception("[api_referral] stats_failed uid=%s", user_id)

    
    pending_ref = referrals_collection.find_one({"invitee_user_id": user_id, "status": "pending"})
    if pending_ref:
        referrer_id = pending_ref.get("referrer_user_id") or pending_ref.get("referrer_id")
        is_member, err = _check_official_channel_subscribed_sync(user_id)
        logger.info(
            "[sub] uid=%s channel=%s is_member=%s err=%s",
            user_id,
            OFFICIAL_CHANNEL_ID,
            str(is_member).lower(),
            err or "",
        )
        if is_member and referrer_id:
            now = datetime.utcnow()
            result = upsert_referral_and_update_user_count(
                referrals_collection,
                users_collection,                
                referrer_user_id=referrer_id,
                invitee_user_id=user_id,
                success_at=now,
                referrer_username=None,
                context={"subscribed_official_at": now},
            )
            if result.get("counted"):
                outcome = grant_referral_rewards(
                    db,
                    users_collection,
                    referrer_id,
                    user_id,
                )
                referrer_doc = users_collection.find_one({"user_id": referrer_id}) or {}
                total_referrals = int(referrer_doc.get("referral_count", 0))
                logger.info(
                    "[xp] uid=%s add=%s bonus=%s total_referrals=%s upsert=%s",
                    referrer_id,
                    outcome.get("base_granted"),
                    outcome.get("bonuses_awarded"),
                    total_referrals,
                    "inserted" if result.get("upserted") else "updated",
                )
                logger.info(
                    "[ref] uid=%s resolved_referrer=%s reason=subscription_recheck",
                    user_id,
                    referrer_id,
                )
                
    payload = {"success": success, "referral_link": link, **stats}
    if error:
        payload["error"] = error
    if not link:
        payload["referral_link"] = None

    return jsonify(payload), 200

def mask_username(username):
    if not username:
        return "********"

    username = username[:8]  # Limit to max 8 chars

    if len(username) <= 2:
        masked = username[0] + "*" * (len(username) - 1)
    else:
        masked = username[:2] + "*" * (len(username) - 2)

    return masked.ljust(8, "*")

# Format usernames depending on admin or own account
def format_username(u, current_user_id, is_admin):
    name = None
    if u.get("username"):
        name = f"@{u['username']}"
    elif u.get("first_name"):
        name = u["first_name"]

    if not name:
        return None

    # Mask if not admin & not own account
    if not is_admin and u.get("user_id") != current_user_id:
        raw_name = name.lstrip("@")
        masked = mask_username(raw_name)
        return f"@{masked}" if name.startswith("@") else masked

    # Admin or own account → show full name
    return name

@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        current_user_id = int(request.args.get("user_id", 0))
        user_record = users_collection.find_one({"user_id": current_user_id}) or {}
        is_admin = bool(user_record.get("is_admin", False))

        week_start_utc, week_end_utc, week_start_local = _week_window_utc()
        week_end_local = week_start_local + timedelta(days=7)
        logger.info(
            "[LEADERBOARD] week_window local_start=%s local_end=%s",
            week_start_local.isoformat(),
            week_end_local.isoformat(),
        )
        logger.info("[LEADERBOARD] pipeline_source=referrals")        
        month_start_utc, month_end_utc, _ = _month_window_utc()

        def safe_format(u):
            # Guarantee user_id exists
            if "user_id" not in u:
                u["user_id"] = 0
            return format_username(u, current_user_id, is_admin)
            
        xp_query = {"user_id": {"$ne": None}}
        logger.info(
            "[lb_query] users.find filter=%s sort=weekly_xp:-1 limit=%s",
            xp_query,
            15,
        )
        xp_rows = list(
            users_collection
            .find(xp_query, {"user_id": 1, "username": 1, "weekly_xp": 1})
            .sort("weekly_xp", DESCENDING)
            .limit(15)
        )
        xp_map = {
            row.get("user_id"): int(row.get("weekly_xp", 0))
            for row in xp_rows
            if row.get("user_id") is not None
        }
        
        referral_rows = recompute_referral_counts(week_start_utc, week_end_utc, limit=15)
        referral_map = {row["_id"]: int(row.get("total_valid", 0)) for row in referral_rows}
        if referral_rows:
            top_row = referral_rows[0]
            logger.info(
                "[LEADERBOARD] result_count=%s top1=(%s,%s)",
                len(referral_rows),
                top_row.get("_id"),
                int(top_row.get("total_valid", 0)),
            )
        else:
            logger.info("[LEADERBOARD] result_count=0 top1=(none)")
            
        leaderboard_user_ids = set(xp_map.keys()) | set(referral_map.keys())
        if current_user_id:
            leaderboard_user_ids.add(current_user_id)

        extra_user_docs = users_collection.find({"user_id": {"$in": list(leaderboard_user_ids)}})
        user_map = {u.get("user_id"): u for u in extra_user_docs}

        top_checkins = []
        for row in xp_rows:
            uid = row.get("user_id")
            user_doc = user_map.get(uid, {"user_id": uid})
            formatted = safe_format(user_doc)
            if not formatted:
                continue
            top_checkins.append({"username": formatted, "xp": int(row.get("weekly_xp", 0))})

        referral_board = []
        total_all_map: dict[int, int] = {}
        if is_admin:
            total_all_rows = referrals_collection.aggregate(
                [
                    {"$match": {"$or": [
                        {"status": {"$exists": False}},
                        {"status": {"$nin": ["pending", "inactive"]}},
                    ]}},
                    {
                        "$group": {
                            "_id": {"$ifNull": ["$referrer_user_id", "$referrer_id"]},
                            "total_all": {"$sum": 1},
                        }
                    },
                ]
            )
            total_all_map = {r["_id"]: r.get("total_all", 0) for r in total_all_rows}

        for row in referral_rows:
            user_doc = row.get("user") or user_map.get(row["_id"], {"user_id": row["_id"]})
            formatted = safe_format(user_doc)
            if not formatted:
                continue
            entry = {
                "username": formatted,
                "total_valid": row.get("total_valid", 0),
                "referrals": row.get("total_valid", 0),
            }
            if is_admin:
                entry["total_all"] = total_all_map.get(row["_id"], row.get("total_valid", 0))

            referral_board.append(entry)
            
        leaderboard = {
            "checkin": top_checkins,
            "referral": referral_board,
        }
        
        user_weekly_xp = xp_map.get(current_user_id, int(user_record.get("weekly_xp", 0)))

        user_referral_stats = {"weekly_referrals": 0, "monthly_referrals": 0, "total_referrals": 0}
        if current_user_id:
            user_referral_stats = compute_referral_stats(referrals_collection, current_user_id)

        user_weekly_referrals = user_referral_stats.get("weekly_referrals", 0)
        
        user_monthly_row = []
        if current_user_id:
            user_monthly_row = recompute_xp_totals(month_start_utc, month_end_utc, user_id=current_user_id)
        monthly_xp_value = int(user_monthly_row[0].get("xp", 0)) if user_monthly_row else 0

        lifetime_valid_refs = user_referral_stats.get("total_referrals", 0)

        if user_record:
            stored_weekly = int(user_record.get("weekly_xp", 0))
            stored_refs = int(user_record.get("weekly_referral_count", 0))            
            if stored_weekly != user_weekly_xp:
                logger.debug("[lb_mismatch] uid=%s weekly_xp stored=%s leaderboard=%s week_start=%s", current_user_id, stored_weekly, user_weekly_xp, week_start_local.isoformat())
            if stored_refs != user_weekly_referrals:
                logger.debug(
                    "[lb_mismatch] uid=%s weekly_referrals stored=%s computed=%s week_start=%s",
                    current_user_id,
                    stored_refs,
                    user_weekly_referrals,
                    week_start_local.isoformat(),
                )
            stored_total_refs = int(user_record.get("referral_count", 0))
            if stored_total_refs != lifetime_valid_refs:
                logger.debug(
                    "[lb_mismatch] uid=%s total_referrals stored=%s computed=%s", current_user_id, stored_total_refs, lifetime_valid_refs
                )
                
        user_stats = {
            "xp": user_weekly_xp,
            "monthly_xp": monthly_xp_value,
            "referrals": user_weekly_referrals,
            "total_valid": user_weekly_referrals,
            "status": user_record.get("status", "Normal"),
            "lifetime_valid": lifetime_valid_refs,
        }

        logger.info(
            "[lb_debug] uid=%s weekly_xp=%s awarded_referrals=%s",
            current_user_id,
            user_weekly_xp,
            user_weekly_referrals,
        )
        
        return jsonify({
            "success": True,
            "leaderboard": leaderboard,
            "user": user_stats
        })

    except Exception:
        logger.exception("[LEADERBOARD] failed")
        return jsonify(
            {
                "success": True,
                "leaderboard": {"checkin": [], "referral": []},
                "user": {},
            }
        ), 200


@app.route("/api/checkin-status/<int:user_id>", methods=["GET"])
def api_checkin_status(user_id):
    """Return whether the user can check in now and the next reset time."""
    tz_utc8 = pytz.timezone("Asia/Kuala_Lumpur")
    now_utc8 = datetime.now(tz_utc8)
    tomorrow_midnight = (now_utc8 + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    next_reset_iso = tomorrow_midnight.astimezone(pytz.UTC).isoformat()

    user = users_collection.find_one({"user_id": int(user_id)}) or {}
    last = user.get("last_checkin")
    streak = int(user.get("streak", 0))

    today_kl = datetime.now(KL_TZ).date()
    last_kl_date = _to_kl_date(last)

    if last_kl_date == today_kl:
        # Already checked in today → show countdown to next midnight
        return jsonify({
            "success": True,
            "can_check_in": False,
            "message": f"⚠️ Already checked in today. 🔥 Streak: {streak} days.",
            "next_checkin_time": next_reset_iso,
        })

    # Not checked in yet today
    return jsonify({
        "success": True,
        "can_check_in": True,
        "message": "🎉 You can check in now!",
        "next_checkin_time": None,
    })

@app.route("/api/leaderboard/history/weeks", methods=["GET"])
def get_all_weeks():
    """Return list of archived weeks available."""
    try:
        weeks = history_collection.find(
            {}, {"week_start": 1, "week_end": 1, "_id": 0}
        ).sort("archived_at", DESCENDING)

        return jsonify({
            "success": True,
            "weeks": list(weeks)
        }), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/leaderboard/history/week/<week_start>", methods=["GET"])
def get_week_history(week_start):
    """Return archived leaderboard for a given week_start (format YYYY-MM-DD)."""
    try:
        doc = history_collection.find_one({"week_start": week_start}, {"_id": 0})
        if not doc:
            return jsonify({"success": False, "error": "No record found for that week"}), 404

        # normalize old vs new formats
        checkin_data = doc.get("checkin") or doc.get("checkin_leaderboard") or []
        referral_data = doc.get("referral") or doc.get("referral_leaderboard") or []

        # Map to consistent fields
        checkin = [
            {
                "username": u.get("username") or u.get("first_name") or "Unknown",
                "xp": u.get("xp") or u.get("weekly_xp") or 0
            }
            for u in checkin_data
        ]

        referral = [
            {
                "username": u.get("username") or u.get("first_name") or "Unknown",
                "referrals": u.get("referrals") or u.get("referral_count") or u.get("weekly_referral_count") or 0
            }
            for u in referral_data
        ]

        return jsonify({
            "success": True,
            "history": {
                "week_start": doc.get("week_start"),
                "week_end": doc.get("week_end"),
                "checkin": checkin,
                "referral": referral
            }
        }), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/bonus_voucher", methods=["GET"])
def get_bonus_voucher():
    try:
        user_id = int(request.args.get("user_id"))
        user = users_collection.find_one({"user_id": user_id})
        if not user:
            return jsonify({"code": None})

        is_admin = user.get("is_admin", False)
        is_vip = user.get("status") == "VIP1"
        if not is_vip and not is_admin:
            return jsonify({"code": None})

        now = datetime.utcnow().replace(tzinfo=pytz.UTC)

        voucher = bonus_voucher_collection.find_one()
        if not voucher:
            return jsonify({"code": None})

        start = voucher["start_time"]
        end = voucher["end_time"]
        if start.tzinfo is None: start = start.replace(tzinfo=pytz.UTC)
        if end.tzinfo is None:   end   = end.replace(tzinfo=pytz.UTC)

        if start <= now <= end:
            return jsonify({"code": voucher["code"]})
        return jsonify({"code": None})
    except Exception as e:
        return jsonify({"code": None, "error": str(e)}), 500

@app.route("/api/add_xp", methods=["POST"])
def api_add_xp():
    # --- Admin gate ---
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    from database import update_user_xp  # import here to avoid circular import
    data = request.json
    user_input = data.get("user_id")
    amount = int(data.get("xp", 0))

    if not user_input or amount == 0:
        return jsonify({"success": False, "message": "Missing username or amount."}), 400

    if isinstance(user_input, str) and user_input.startswith("@"):
        username = user_input[1:]
    elif isinstance(user_input, str):
        username = user_input
    else:
        return jsonify({"success": False, "message": "Use @username format."}), 400

    idempotency_key = data.get("idempotency_key") or data.get("unique_key")
    success, message = update_user_xp(username, amount, idempotency_key)
    return jsonify({"success": success, "message": message})

@app.route("/api/join_requests")
def api_join_requests():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    try:
        requests = call_bot_in_loop(app_bot.bot.get_chat_join_requests(chat_id=GROUP_ID))
        result = [{"user_id": r.from_user.id, "username": r.from_user.username} for r in requests]
        return jsonify({"success": True, "requests": result})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/starterpack", methods=["POST"])
def api_starterpack():
    try:
        data = request.get_json(silent=True) or {}
        user_id = data.get("user_id")
        username = data.get("username", "unknown")

        if not user_id:
            return jsonify({"success": False, "error": "Missing user_id"}), 400

        user = users_collection.find_one({"user_id": int(user_id)}) or {}

        # Check if already claimed
        if user.get("welcome_xp_claimed"):
            return jsonify({"success": False, "message": "⚠️ Starter Pack already claimed."})

        users_collection.update_one(
            {"user_id": int(user_id)},
            {
                "$set": {"username": username, "welcome_xp_claimed": True},
                "$setOnInsert": {
                    "xp": 0,
                    "weekly_xp": 0,
                    "monthly_xp": 0,
                    "status": "Normal",
                },
            },
            upsert=True,
        )

        granted = grant_xp(
            db,
            int(user_id),
            "welcome_bonus",
            "welcome_bonus",
            WELCOME_BONUS_XP,
        )
        if not granted:
            return jsonify({"success": False, "message": "⚠️ Starter Pack already claimed."})
       
        return jsonify({
            "success": True,
            "message": f"🎁 Starter Pack claimed! +{WELCOME_BONUS_XP} XP"
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/admin/set_bonus", methods=["POST"])
def api_admin_set_bonus():
    """
    Create/replace a single active VIP voucher (admins only).
    Body JSON: {"code": "ABC123", "release_time": "2025-09-19T07:00:00Z"}
    Query:     ?user_id=<admin_telegram_id>  (frontend must pass this)
    """
    try:
        # Admin gate
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"status": "error", "message": msg}), code

        data = request.get_json(silent=True) or {}
        code = (data.get("code") or "").strip()
        release_iso = data.get("release_time")

        if not code or not release_iso:
            return jsonify({"status": "error", "message": "Missing code or release_time"}), 400

        # Window: start at release_time, end +6h (adjust as needed)
        start = datetime.fromisoformat(release_iso.replace("Z", "+00:00"))
        end = start + timedelta(hours=6)

        # Upsert a single voucher doc
        bonus_voucher_collection.update_one(
            {},
            {"$set": {"code": code, "start_time": start, "end_time": end}},
            upsert=True
        )

        return jsonify({"status": "success", "message": "Voucher scheduled"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/export_csv")
def export_csv():
    # --- Admin gate ---
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    try:
        users = users_collection.find()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "username", "xp", "weekly_xp", "referral_count", "weekly_referral_count", "monthly_xp", "status"])
        for u in users:
            writer.writerow([
                u.get("user_id"),
                u.get("username", ""),
                u.get("xp", 0),
                u.get("weekly_xp", 0),
                u.get("referral_count", 0),
                u.get("weekly_referral_count", 0),
                u.get("monthly_xp", 0),
                u.get("status", "Normal"),
            ])
        output.seek(0)
        return output.getvalue()
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/admin/backfill-status", methods=["POST"])
def api_admin_backfill_status():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    modified = backfill_missing_statuses()
    return jsonify({"success": True, "modified": modified})


# ----------------------------
# Weekly XP Reset Job
# ----------------------------
def reset_weekly_xp():
    now = datetime.now(KL_TZ)

    # Last full week [Mon..Sun], assuming this runs every Monday 00:00 KL
    week_end_date = (now - timedelta(days=1)).date()      # Sunday
    week_start_date = week_end_date - timedelta(days=6)   # Monday

    proj = {"user_id": 1, "username": 1, "weekly_xp": 1, "weekly_referral_count": 1}
    top_checkin   = list(users_collection.find({}, proj).sort("weekly_xp", DESCENDING).limit(100))
    top_referrals = list(users_collection.find({}, proj).sort("weekly_referral_count", DESCENDING).limit(100))

    history_collection.insert_one({
        "week_start": week_start_date.isoformat(),
        "week_end":   week_end_date.isoformat(),
        "checkin_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_xp": u.get("weekly_xp", 0)}
            for u in top_checkin
        ],
        "referral_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_referral_count": u.get("weekly_referral_count", 0)}
            for u in top_referrals
        ],
        # store as UTC so later math is safe
        "archived_at": datetime.utcnow()
    })

    users_collection.update_many({}, {
        "$set": {"weekly_xp": 0, "weekly_referral_count": 0, "xp_weekly_milestone_bucket": 0,
        "ref_weekly_milestone_bucket": 0,}
    })

    print(f"✅ Weekly XP & referrals reset complete at {now}")

meta = db["meta"]

def backfill_missing_statuses():
    res = users_collection.update_many(
        {"status": {"$exists": False}}, {"$set": {"status": "Normal"}}
    )
    logger.info("[xp_recompute] backfill_status modified=%s", getattr(res, "modified_count", 0))
    return getattr(res, "modified_count", 0)

def one_time_fix_monthly_xp():
    # run once ever
    if meta.find_one({"_id": "fix_monthly_xp_done"}):
        return
    res = users_collection.update_many(
        {"monthly_xp": {"$exists": False}},
        {"$set": {"monthly_xp": 0}}
    )
    meta.update_one(
        {"_id": "fix_monthly_xp_done"},
        {"$set": {"done_at": datetime.utcnow(), "modified": res.modified_count}},
        upsert=True
    )
    print(f"🔧 monthly_xp backfilled on first boot. Modified: {res.modified_count}")

def run_boot_catchup():
    now = datetime.now(KL_TZ)

    try:
        # weekly catch-up (only on Monday)
        last_history = history_collection.find_one(sort=[("archived_at", DESCENDING)])
        if last_history:
            last_raw = last_history["archived_at"]
            if last_raw.tzinfo is None:
                last_reset = last_raw.replace(tzinfo=pytz.UTC).astimezone(KL_TZ)
            else:
                last_reset = last_raw.astimezone(KL_TZ)
            days_since = (now - last_reset).days
        else:
            days_since = 999

        if now.weekday() == 0 and days_since >= 6:
            print("⚠️ Missed weekly reset. Running now...")
            reset_weekly_xp()
        else:
            print("✅ No weekly catch-up needed.")

        # monthly catch-up (only on the 1st)
        sample_user = users_collection.find_one(
            {"last_status_update": {"$exists": True}},
            sort=[("last_status_update", DESCENDING)]
        )
        last_month = sample_user["last_status_update"].month if sample_user else None
        last_year  = sample_user["last_status_update"].year  if sample_user else None
        if now.day == 1 and (not sample_user or last_month != now.month or last_year != now.year):
            print("⚠️ Missed monthly VIP update. Running now...")
            update_monthly_vip_status()
        else:
            print("✅ No monthly catch-up needed.")

        # one-time migration instead of scanning every boot
        one_time_fix_monthly_xp()
        
        if os.getenv("BACKFILL_STATUS_ON_BOOT", "false").lower() == "true":
            backfill_missing_statuses()

    except Exception as e:
        print(f"❌ Boot-time catch-up failed: {e}")

def apply_monthly_tier_update(run_time: datetime | None = None):
    run_at_local = run_time.astimezone(KL_TZ) if run_time else datetime.now(KL_TZ)
    start_utc, end_utc, start_local = _previous_month_window_utc(run_at_local)
    end_local = end_utc.astimezone(KL_TZ)
    month_key = start_local.strftime("%Y-%m")
    now_utc = datetime.now(timezone.utc)
   
    xp_rows = recompute_xp_totals(start_utc, end_utc)
    xp_map = {row["_id"]: int(row.get("xp", 0)) for row in xp_rows}

    promoted: list[int] = []
    demoted: list[int] = []   
    processed = 0
    
    for user in users_collection.find():
        uid = user.get("user_id")
        if uid is None:
            continue
        previous_month_xp = xp_map.get(uid, 0)
        next_status = "VIP1" if previous_month_xp >= 800 else "Normal"
        current_status = user.get("status", "Normal")

        if next_status != current_status:
            (promoted if next_status == "VIP1" else demoted).append(uid)

        monthly_xp_history_collection.update_one(
            {"user_id": uid, "month": month_key},
            {
                "$set": {
                    "user_id": uid,
                    "username": user.get("username"),
                    "month": month_key,
                    "monthly_xp": previous_month_xp,
                    "status_before_reset": current_status,
                    "status_after_reset": next_status,
                    "captured_at_utc": now_utc,
                    "captured_at_kl": run_at_local.isoformat(),
                }
            },
            upsert=True,
        )

        users_collection.update_one(
            {"user_id": uid},
            {
                "$set": {
                    "status": next_status,
                    "last_status_update": end_local,
                    "monthly_xp": 0,
                }
            },
        )
        processed += 1

    audit_doc = {
        "type": "monthly_tier_update",
        "month": month_key,
        "run_at_utc": now_utc,
        "run_at_tz": run_at_local.isoformat(),
        "promoted_count": len(promoted),
        "demoted_count": len(demoted),
        "promoted_sample": promoted[:5],
        "demoted_sample": demoted[:5],
        "total_processed": processed,
    }

    audit_events_collection.update_one(
        {"type": "monthly_tier_update", "month": month_key},
        {"$set": audit_doc},
        upsert=True,
    )
    audit_events_collection.update_one(
        {"_id": "monthly_job:last_run"},
        {"$set": {"run_at_utc": now_utc, "run_at_tz": run_at_local.isoformat(), "month": month_key}},
        upsert=True,
    )

    logger.info(
        "[monthly_job] ran_at=%s tz=GMT+8 month=%s promoted=%s demoted=%s",
        run_at_local.isoformat(),
        month_key,
        len(promoted),
        len(demoted),
    )

def update_monthly_vip_status():
    return apply_monthly_tier_update()
    
# ----------------------------
# Telegram Bot Handlers
# ----------------------------

# Xmas Gift Delight helpers -----------------
def _xmas_keyboard() -> InlineKeyboardMarkup:
    """Inline keyboard for the Xmas Gift Delight flow."""
    channel_url = f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 Join Channel", url=channel_url),
            InlineKeyboardButton("✅ Check-in Now", callback_data="xmas_checkin"),
        ]
    ])


async def _send_xmas_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, user) -> None:
    """Send the Xmas Gift Delight entry message and log campaign source."""
    now = datetime.utcnow()
    existing_user = users_collection.find_one({"user_id": user.id})

    if not existing_user:
        is_new_joiner = True
    else:
        first_seen_at = existing_user.get("first_seen_at")
        if first_seen_at and first_seen_at < XMAS_CAMPAIGN_START:
            is_new_joiner = False
        else:
            is_new_joiner = True

    users_collection.update_one(
        {"user_id": user.id},
        {
            "$setOnInsert": {
                "user_id": user.id,
                "first_seen_at": now,
                 "status": "Normal",              
            },
            "$set": {
                "xmas_entry_source": "popup",
                "xmas_is_new_joiner": is_new_joiner,
                "updated_at": now,
            },
        },
        upsert=True,
    )

    message = (
        "🎄 Xmas Gift Delight\n"
        "Join our official community & do one check-in to enter this week’s Christmas Gift Draw 🎁"
    )

    keyboard = _xmas_keyboard()
    target_message = update.effective_message
    if target_message:
        await target_message.reply_text(message, reply_markup=keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    
    # Handle deep links for the Xmas Gift Delight campaign
    if context.args and len(context.args) > 0 and context.args[0].lower() == "xmasgift":
        if user:
            await _send_xmas_flow(update, context, user)
        return

    if user:
        users_collection.update_one(
            {"user_id": user.id},
            {"$setOnInsert": {
                "username": user.username,
                "xp": 0,
                "weekly_xp": 0,
                "monthly_xp": 0,
                "referral_count": 0,
                "last_checkin": None,
                "status": "Normal",
            }},
            upsert=True
        )
        user_doc = users_collection.find_one({"user_id": user.id}, {"joined_main_at": 1})
        if not (user_doc or {}).get("joined_main_at"):
            logger.info(
                "[WELCOME][JOIN_BACKFILL_DISABLED] uid=%s joined_main_at_missing",
                user.id,
            ) 
        keyboard = [[
            InlineKeyboardButton("🚀 Open AdvantPlay Mini-App", web_app=WebAppInfo(url=WEBAPP_URL))
        ]]
        if message:
            await message.reply_text(
            "👋 Welcome to AdvantPlay Community!\n\n"
            "Tap below to enter the Mini-App and:\n"
            "• Check-in daily to earn XP\n"
            "• Unlock your referral link\n"
            "• Claim voucher code\n\n"
            "Start your journey here 👇",
            reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_xmas_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xmas Gift Delight check-in flow triggered via inline keyboard."""
    query = update.callback_query
    user = query.from_user

    await query.answer()

    async def _safe_edit_message(text: str, **kwargs):
        try:
            await query.edit_message_text(text, **kwargs)
        except BadRequest as e:
            # Ignore Telegram's "message is not modified" error when users tap repeatedly
            if "message is not modified" not in str(e).lower():
                raise
    
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user.id)
        status = member.status
    except Exception as e:
        status = None
        print(f"[xmas_checkin] get_chat_member error: {e}")

    allowed_statuses = {"member", "administrator", "creator"}
    if status not in allowed_statuses:
        warning_text = (
            "👋 You haven’t joined our channel yet.\n"
            "Please tap Join Channel first, then press Check-in Now again to enter this week’s Xmas Gift Draw 🎁"
        )
        await _safe_edit_message(
            warning_text,
            reply_markup=_xmas_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    now = datetime.utcnow()
    iso_calendar = now.isocalendar()

    users_collection.update_one(
        {"user_id": user.id},
        {
            "$set": {
                "xmas_checked_in": True,
                "xmas_year": iso_calendar.year,
                "xmas_week": iso_calendar.week,
                "xmas_checkin_at": now,
                "updated_at": now,
            }
        },
    )

    success_text = (
        "✅ Check-in successful!\n\n"
        "You’ve entered this week’s Xmas Gift Delight draw 🎄\n"
        "60-75 new players will be selected and contacted by this bot. Good luck! 🍀"
    )
    await _safe_edit_message(success_text)
    
async def member_update_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    member = update.chat_member or update.my_chat_member
    if not member:
        return

    old_status = getattr(member.old_chat_member, "status", None)
    new_status = getattr(member.new_chat_member, "status", None)

    if old_status in ("left", "kicked", "restricted") and new_status in ("member", "administrator", "creator"):
        user = member.new_chat_member.user
        if user.is_bot:
            return
            
        try:
            await handle_user_join(
                user.id,
                user.username,
                member.chat.id,
                source="chat_member",
                invite_link=member.invite_link,
                old_status=old_status,
                new_status=new_status,
                context=context,
            )
        except Exception:
            logger.exception("[join] chat_member error uid=%s", user.id)

async def new_chat_members_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.new_chat_members:
        return
    for user in message.new_chat_members:
        if user.is_bot:
            continue
        try:
            await handle_user_join(
                user.id,
                user.username,
                message.chat.id,
                source="new_chat_members",
                invite_link=getattr(message, "invite_link", None),
                context=context,
            )
        except Exception:
            logger.exception("[join] new_chat_members error uid=%s", user.id)

async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    join_request = update.chat_join_request
    if not join_request:
        return

    try:
        if join_request.chat.id != GROUP_ID:
            return

        user = getattr(join_request, "from_user", None) or getattr(join_request, "user", None)
        if not user or user.is_bot:
            return

        invite_link = getattr(join_request, "invite_link", None)
        referrer_id = _resolve_referrer_id_from_invite_link(invite_link)
        if not referrer_id:
            logger.info(
                "[join_request] uid=%s resolved_referrer=None reason=no_referrer",
                user.id,
            )
            return

        joined_at = datetime.now(KL_TZ)
        _upsert_pending_referral(user.id, referrer_id, joined_at)
        logger.info(
            "[join_request] uid=%s referrer=%s invite_link=%s status=pending",
            user.id,
            referrer_id,
            getattr(invite_link, "invite_link", None),
        )
    except Exception:
        logger.exception("[join_request] error uid=%s", getattr(join_request.from_user, "id", None))
        
async def button_handler(update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if query.data == "checkin":
        user = users_collection.find_one({"user_id": user_id})
        if user and user.get("welcome_xp_claimed"):
            await query.answer("⚠️ You already claimed your welcome XP!", show_alert=True)
        else:
            users_collection.update_one(
                {"user_id": user_id},
                {
                    "$set": {"welcome_xp_claimed": True},
                    "$setOnInsert": {
                        "xp": 0,
                        "weekly_xp": 0,
                        "monthly_xp": 0,
                        "status": "Normal",
                    },
                },
                upsert=True
            )
            granted = grant_xp(
                db, user_id, "welcome_bonus", "welcome_bonus", WELCOME_BONUS_XP
            )
            if granted:
                await query.edit_message_text(
                    f"✅ You received +{WELCOME_BONUS_XP} XP welcome bonus!"
                )
            else:
                await query.answer(
                    "⚠️ You already claimed your welcome XP!", show_alert=True
                )
                
    elif query.data == "referral":
        from functools import partial
        loop = asyncio.get_running_loop()
        link = await loop.run_in_executor(
            None, partial(get_or_create_referral_invite_link_sync, user_id, query.from_user.username or "")
        )
        await query.edit_message_text(f"👥 Your referral link:\n{link}")

# ----------------------------
# Run Bot + Flask + Scheduler
# ----------------------------
if __name__ == "__main__":
    try:
        ensure_voucher_indexes()
        print("Voucher indexes ensured.")
    except Exception as e:
        print("Failed to register vouchers blueprint / ensure indexes:", e)
        raise

    # 2) Catch up maintenance before bot handlers start
    try:
        run_boot_catchup()
    except Exception as e:
        print("run_boot_catchup error:", e)

    # 3) Telegram handlers
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))    
    app_bot.add_handler(ChatMemberHandler(member_update_handler, ChatMemberHandler.CHAT_MEMBER))
    app_bot.add_handler(ChatMemberHandler(member_update_handler, ChatMemberHandler.MY_CHAT_MEMBER))
    app_bot.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_members_handler))    
    app_bot.add_handler(CallbackQueryHandler(handle_xmas_checkin, pattern="^xmas_checkin$"))
    app_bot.add_handler(CallbackQueryHandler(button_handler))

    # 4) Scheduler (KL time for human-facing schedules)
    scheduler = BackgroundScheduler(
        timezone=KL_TZ,
        job_defaults={"coalesce": True, "misfire_grace_time": 3600, "max_instances": 1}
    )
    scheduler.add_job(
        reset_weekly_xp,
        trigger=CronTrigger(day_of_week="mon", hour=0, minute=0, timezone=KL_TZ),
        id="weekly_reset",
        name="Weekly XP Reset",
        replace_existing=True,
    )
    scheduler.add_job(
        apply_monthly_tier_update,
        trigger=CronTrigger(day=1, hour=0, minute=0, timezone=KL_TZ),
        id="monthly_vip",
        name="Monthly VIP Status Update",
        replace_existing=True,
    )
    scheduler.start()

    # 5) Background jobs on the bot's job_queue
    app_bot.job_queue.run_once(refresh_admin_ids, when=0)
    app_bot.job_queue.run_repeating(refresh_admin_ids, interval=timedelta(minutes=10), first=timedelta(seconds=0))

    print("✅ Bot & Scheduler wired. Starting servers...")

    # 6) Start Flask AFTER routes/handlers/scheduler are wired
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080), daemon=True).start()  

    try:
        app_bot.run_polling(
            poll_interval=5,
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member", "chat_join_request"]
        )
    finally:
        scheduler.shutdown(wait=False)

# Test plan (internal):
# 1) Generate referral link for user A.
# 2) User B joins via that link (join request flow) and is approved.
# 3) Verify users.referral_count/weekly_referral_count increment and xp_events include ref_success:<B> and bonus at 3.
# 4) Ensure rejoin does not double count or double XP.
        
