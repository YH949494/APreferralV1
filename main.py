from flask import (
    Flask, request, jsonify, send_from_directory, make_response,
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
from telegram.error import BadRequest, Forbidden
from telegram.request import HTTPXRequest
from datetime import datetime, timedelta, timezone
from werkzeug.exceptions import HTTPException
from urllib.parse import urlencode

from config import (
    KL_TZ,
    STREAK_MILESTONES,
    XP_BASE_PER_CHECKIN,
    WEEKLY_XP_BUCKET,
    WEEKLY_REFERRAL_BUCKET,
    MINIAPP_VERSION,
    ADMIN_UIDS,
)
from time_utils import expires_in_seconds, tz_name

from bson.json_util import dumps
from bson.objectid import ObjectId
from xp import ensure_xp_indexes, grant_xp, now_utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

from app_context import set_app_bot, set_bot, set_scheduler
from onboarding import MYWIN_CHAT_ID, onboarding_due_tick, record_first_mywin, record_first_checkin
from vouchers import vouchers_bp, ensure_voucher_indexes, process_verification_queue, verify_telegram_init_data
from scheduler import settle_pending_referrals, settle_referral_snapshots, settle_xp_snapshots
from affiliate_rewards import (
    ensure_affiliate_indexes,
    issue_welcome_bonus_if_eligible,
    record_user_last_seen,
    approve_affiliate_ledger,
    reject_affiliate_ledger,
)
from telegram_utils import safe_reply_text

from pymongo import DESCENDING, ASCENDING, ReturnDocument  # keep if used elsewhere
from pymongo.errors import DuplicateKeyError, CursorNotFound
import os, asyncio, traceback, csv, io, requests, logging, time, uuid, socket
import pytz
from database import init_db, db

FIRST_CHECKIN_BONUS_XP = int(os.getenv("FIRST_CHECKIN_BONUS_XP", "200"))
WELCOME_BONUS_XP = int(os.getenv("WELCOME_BONUS_XP", "20"))
WELCOME_WINDOW_HOURS = int(os.getenv("WELCOME_WINDOW_HOURS", "48"))
WELCOME_WINDOW_DAYS = 7

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
INSTANCE_ID = (os.getenv("FLY_MACHINE_ID") or os.getenv("FLY_ALLOC_ID") or f"{socket.gethostname()}:{os.getpid()}")

LEADERBOARD_CACHE = {}  # key -> {"ts": epoch_seconds, "payload": dict}
CACHE_TTL_SECONDS = 300

def _running_under_gunicorn():
    return "gunicorn" in os.environ.get("SERVER_SOFTWARE", "").lower() or os.environ.get("GUNICORN_CMD_ARGS") is not None

def _new_run_id() -> str:
    return uuid.uuid4().hex[:8]

class JobTimer:
    def __enter__(self):
        self._start = time.monotonic()
        self.elapsed_s = 0.0
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.elapsed_s = time.monotonic() - self._start
        return False

def _job_prefix(job_id: str) -> str:
    if job_id == "tick_5min":
        return "[JOB][5MIN]"
    if job_id == "weekly_reset":
        return "[JOB][WEEKLY]"
    if job_id == "monthly_vip":
        return "[JOB][MONTHLY]"
    return "[JOB][SCHED]"

RUNNER_MODE = os.getenv("RUNNER_MODE")
if not RUNNER_MODE:
    RUNNER_MODE = "web" if _running_under_gunicorn() else "worker"

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
BASE_WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"
WEBAPP_URL = f"{BASE_WEBAPP_URL}?v={MINIAPP_VERSION}"
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

DEPRECATED_REFERRAL_FIELDS = {
    "weekly_referral_count",
    "total_referral_count",
    "ref_count_total",
    "monthly_referral_count",
    "referral_count",
}

def _warn_if_deprecated_referral_fields(user_doc: dict | None, context: str) -> None:
    if not user_doc:
        return
    seen = [field for field in DEPRECATED_REFERRAL_FIELDS if field in user_doc]
    if seen:
        logger.warning("[REFERRAL][DEPRECATED_READ] context=%s fields=%s", context, ",".join(seen))

def compute_referral_stats(user_id: int, window=None):
    """Return referral stats using users snapshot fields."""

    if not user_id:
        return {"total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0}

    user_doc = users_collection.find_one(
        {"user_id": user_id},
        {"total_referrals": 1, "weekly_referrals": 1, "monthly_referrals": 1},
    ) or {}
    _warn_if_deprecated_referral_fields(user_doc, "compute_referral_stats")
    total = int(user_doc.get("total_referrals", 0))
    weekly = int(user_doc.get("weekly_referrals", 0))
    monthly = int(user_doc.get("monthly_referrals", 0))
    return {"total_referrals": total, "weekly_referrals": weekly, "monthly_referrals": monthly}

def _normalize_snapshot_updated_at(updated_at: datetime | None) -> datetime | None:
    if not updated_at:
        return None
    if updated_at.tzinfo is None:
        return updated_at.replace(tzinfo=timezone.utc)
    return updated_at.astimezone(timezone.utc)

def _snapshot_meta(updated_at: datetime | None, now_utc_ts: datetime) -> tuple[str | None, int | None]:
    normalized = _normalize_snapshot_updated_at(updated_at)
    if not normalized:
        return None, None
    age_sec = int((now_utc_ts - normalized).total_seconds())
    snapshot_ts = normalized.astimezone(KL_TZ).isoformat()
    return snapshot_ts, age_sec


def _get_user_snapshot(user_id: int) -> tuple[dict | None, str | None, int | None]:
    if not user_id:
        return None, None, None
    now_utc_ts = now_utc()
    user_doc = users_collection.find_one(
        {"user_id": user_id},
        {
            "weekly_xp": 1,
            "monthly_xp": 1,
            "total_xp": 1,
            "xp": 1,
            "weekly_referrals": 1,
            "monthly_referrals": 1,
            "total_referrals": 1,
            "vip_tier": 1,
            "vip_month": 1,
            "status": 1,
            "snapshot_updated_at": 1,
        },
    )
    if not user_doc:
        return None, None, None
    _warn_if_deprecated_referral_fields(user_doc, "_get_user_snapshot")        
    snapshot_ts, snapshot_age_sec = _snapshot_meta(user_doc.get("snapshot_updated_at"), now_utc_ts)
    snapshot = {
        "user_id": user_id,
        "weekly_xp": int(user_doc.get("weekly_xp", 0)),
        "monthly_xp": int(user_doc.get("monthly_xp", 0)),
        "total_xp": int(user_doc.get("total_xp", user_doc.get("xp", 0))),
        "weekly_referrals": int(user_doc.get("weekly_referrals", 0)),
        "monthly_referrals": int(user_doc.get("monthly_referrals", 0)),
        "total_referrals": int(user_doc.get("total_referrals", 0)),
        "vip_tier": user_doc.get("vip_tier") or user_doc.get("status"),
        "vip_month": user_doc.get("vip_month"),
    }
    logger.info("[SNAPSHOT][READ] uid=%s age=%ss", user_id, snapshot_age_sec)
    return snapshot, snapshot_ts, snapshot_age_sec

def _current_month_window_utc(reference: datetime | None = None):
    """Return (start_utc, end_utc, start_local, end_local) for the current month."""

    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    month_start = ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if month_start.month == 12:
        next_month_start = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month_start = month_start.replace(month=month_start.month + 1)
    return (
        month_start.astimezone(timezone.utc),
        next_month_start.astimezone(timezone.utc),
        month_start,
        next_month_start,
    )


def _event_time_expr():
    return {"$ifNull": ["$created_at", "$ts"]}

def recompute_xp_totals(start_utc, end_utc, limit: int | None = None, user_id: int | None = None):
    """Aggregate XP from xp_events between the given UTC boundaries."""
    if RUNNER_MODE == "web":
        logger.error("[GUARD][WEB] recompute_xp_totals blocked uid=%s", user_id)
        return []
        
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

def settle_pending_referrals_with_cache_clear():
    settle_pending_referrals()

def _clear_leaderboard_cache(source: str) -> None:
    LEADERBOARD_CACHE.clear()
    logger.info("[LEADERBOARD][CACHE_CLEAR] source=%s", source)

def settle_xp_snapshots_with_cache_clear():
    settle_xp_snapshots()
    _clear_leaderboard_cache("snapshot_publish")


def settle_xp_snapshots_scheduled():
    logger.info("[SNAPSHOT][XP] start reason=scheduled_5min")
    settle_with_cache_clear = globals().get("settle_xp_snapshots_with_cache_clear")
    if callable(settle_with_cache_clear):
        settle_with_cache_clear()
    else:
        settle_xp_snapshots()
        _clear_leaderboard_cache("snapshot_publish")
    _check_snapshot_freshness()        
    logger.info("[SNAPSHOT][XP] done")


def settle_referral_snapshots_with_cache_clear():
    settle_referral_snapshots()
    _clear_leaderboard_cache("snapshot_publish")

# ----------------------------
# Scheduler Locking + Ticks
# ----------------------------
def acquire_scheduler_lock(name: str, ttl_seconds: int) -> tuple[bool, dict | None]:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)
    try:
        doc = scheduler_locks_collection.find_one_and_update(
            {
                "_id": name,
                "$or": [
                    {"expireAt": {"$lte": now}},
                    {"expireAt": {"$exists": False}},
                ],
            },
            {
                "$set": {"expireAt": expires_at, "owner": INSTANCE_ID, "updatedAt": now},
                "$setOnInsert": {"createdAt": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        doc = scheduler_locks_collection.find_one({"_id": name})
        return False, doc
    return doc is not None, doc
    return doc is not None


def tick_5min() -> None:
    run_id = _new_run_id()
    now_local = datetime.now(KL_TZ)
    acquired, lock_doc = acquire_scheduler_lock("tick_5min", ttl_seconds=900)
    if not acquired:
        expires_in_s = expires_in_seconds((lock_doc or {}).get("expireAt"))
        logger.info(
            "[JOB][5MIN] lock_not_acquired owner=%s expires_in_s=%s run_id=%s instance=%s",
            (lock_doc or {}).get("owner"),
            expires_in_s,
            run_id,
            INSTANCE_ID,
        )
        return
    logger.info(
        "[JOB][5MIN] lock_acquired owner=%s ttl_s=%s run_id=%s instance=%s",
        INSTANCE_ID,
        900,
        run_id,
        INSTANCE_ID,
    )
    logger.info(
        "[JOB][5MIN] start window=5min run_id=%s instance=%s tz=%s ts=%s",
        run_id,
        INSTANCE_ID,
        tz_name(KL_TZ),
        now_local.isoformat(),
    )
    try:
        with JobTimer() as total_timer:
            with JobTimer() as step_timer:
                logger.info(
                    "[JOB][5MIN] progress step=settle_pending_referrals run_id=%s",
                    run_id,
                )
                settle_pending_referrals_with_cache_clear()
            logger.info(
                "[JOB][5MIN] step_done name=settle_pending_referrals elapsed_s=%.2f run_id=%s",
                step_timer.elapsed_s,
                run_id,
            )

            with JobTimer() as step_timer:
                logger.info(
                    "[JOB][5MIN] progress step=settle_xp_snapshots run_id=%s",
                    run_id,
                )
                settle_xp_snapshots()
                _check_snapshot_freshness()
            logger.info(
                "[JOB][5MIN] step_done name=settle_xp_snapshots elapsed_s=%.2f run_id=%s",
                step_timer.elapsed_s,
                run_id,
            )

            with JobTimer() as step_timer:
                logger.info(
                    "[JOB][5MIN] progress step=settle_referral_snapshots run_id=%s",
                    run_id,
                )
                settle_referral_snapshots_with_cache_clear()
            logger.info(
                "[JOB][5MIN] step_done name=settle_referral_snapshots elapsed_s=%.2f run_id=%s",
                step_timer.elapsed_s,
                run_id,
            )

            _clear_leaderboard_cache("tick_5min")
            logger.info(
                "[JOB][5MIN] done elapsed_s=%.2f run_id=%s",
                total_timer.elapsed_s,
                run_id,
            )
    except Exception as exc:
        logger.error(
            "[JOB][5MIN] failed run_id=%s instance=%s err=%s msg=%s",
            run_id,
            INSTANCE_ID,
            exc.__class__.__name__,
            str(exc),
        )
        raise


def process_verification_queue_scheduled(batch_limit: int = 50) -> None:
    acquired, _lock_doc = acquire_scheduler_lock("verification_queue", ttl_seconds=300)
    if not acquired:
        logger.info("[SCHEDULER][VERIFY] lock_not_acquired")
        return
    logger.info("[SCHEDULER][VERIFY] start batch_limit=%s", batch_limit)
    start_time = time.time()
    process_verification_queue(batch_limit=batch_limit)
    logger.info(
        "[SCHEDULER][VERIFY] done elapsed=%.2fs",
        time.time() - start_time,
    )

# ----------------------------
# MongoDB Setup
# ----------------------------
init_db(MONGO_URL)
users_collection = db["users"]
# SNAPSHOT FIELDS — ONLY WRITTEN BY WORKER
# weekly_xp, monthly_xp, total_xp, weekly_referrals, monthly_referrals, total_referrals, vip_tier, vip_month
# DEPRECATED — DO NOT USE (ledger-based referrals only)
# weekly_referral_count, total_referral_count, ref_count_total, monthly_referral_count
history_collection = db["weekly_leaderboard_history"]
bonus_voucher_collection = db["bonus_voucher"]
admin_cache_col = db["admin_cache"]
xp_events_collection = db["xp_events"]
referral_award_events_collection = db["referral_award_events"]
referral_events_collection = db["referral_events"]
welcome_eligibility_collection = db["welcome_eligibility"]
monthly_xp_history_collection = db["monthly_xp_history"]
monthly_xp_history_collection.create_index([("month", ASCENDING)])
monthly_xp_history_collection.create_index([("user_id", ASCENDING), ("month", ASCENDING)], unique=True)
audit_events_collection = db["audit_events"]
invite_link_map_collection = db["invite_link_map"]
unknown_invite_links_collection = db["unknown_invite_links"]
referral_audit_collection = db["referral_audit"]
unknown_invite_audit_collection = db["unknown_invite_audit"]
pending_referrals_collection = db["pending_referrals"]
qualified_events_collection = db["qualified_events"]
affiliate_ledger_collection = db["affiliate_ledger"]
voucher_pools_collection = db["voucher_pools"]
tg_verification_queue_collection = db["tg_verification_queue"]
scheduler_locks_collection = db["scheduler_locks"]
try:
    scheduler_locks_collection.create_index([("expireAt", ASCENDING)], expireAfterSeconds=0)
except Exception:
    logger.warning("[SCHEDULER][LOCK] failed to create TTL index", exc_info=True)
    
REFERRAL_HOLD_HOURS = 12

REFERRAL_INCREMENT_GUARD_FIELDS = {
    "weekly_referrals",
    "monthly_referrals",
    "total_referrals",
    "weekly_referral_count",
    "total_referral_count",
    "ref_count_total",
    "monthly_referral_count",
    "referral_count",
}

def _check_snapshot_freshness() -> None:
    if RUNNER_MODE != "worker":
        return
    now_utc_ts = now_utc()
    cutoff = now_utc_ts - timedelta(minutes=15)
    heartbeat_doc = admin_cache_col.find_one({"_id": "snapshot_heartbeat"}, {"ts_utc": 1})
    heartbeat_ts = _normalize_snapshot_updated_at((heartbeat_doc or {}).get("ts_utc"))
    if heartbeat_ts is None:
        logger.error("[SNAPSHOT][STALE] age_sec=missing action=investigate")
    else:
        heartbeat_age_sec = int((now_utc_ts - heartbeat_ts).total_seconds())
        if heartbeat_age_sec > 900:
            logger.error("[SNAPSHOT][STALE] age_sec=%s action=investigate", heartbeat_age_sec)
    stale_user = users_collection.find_one(
        {"snapshot_updated_at": {"$lt": cutoff}},
        {"snapshot_updated_at": 1},
    )
    if stale_user:
        _, snapshot_age_sec = _snapshot_meta(stale_user.get("snapshot_updated_at"), now_utc_ts)
        if snapshot_age_sec is None:
            logger.error("[SNAPSHOT][STALE] age_sec=missing action=investigate")
        elif snapshot_age_sec > 900:
            logger.error("[SNAPSHOT][STALE] age_sec=%s action=investigate", snapshot_age_sec)

def _log_referral_increment_attempt(update_doc: dict | None, context: str) -> None:
    if RUNNER_MODE != "web" or not update_doc:
        return
    inc_doc = update_doc.get("$inc") or {}
    if not isinstance(inc_doc, dict):
        return
    fields = [field for field in inc_doc.keys() if field in REFERRAL_INCREMENT_GUARD_FIELDS]
    if fields:
        logger.error("[REFERRAL][ERROR] increment_attempt context=%s fields=%s", context, ",".join(fields))

def _users_update_one(filter_doc: dict, update_doc: dict, *, context: str, **kwargs):
    _log_referral_increment_attempt(update_doc, context)
    return users_collection.update_one(filter_doc, update_doc, **kwargs)

def _users_update_many(filter_doc: dict, update_doc: dict, *, context: str, **kwargs):
    _log_referral_increment_attempt(update_doc, context)
    return users_collection.update_many(filter_doc, update_doc, **kwargs)

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

def _short_invite_link(invite_link: str | None) -> str:
    if not invite_link:
        return ""
    if len(invite_link) <= 36:
        return invite_link
    return f"{invite_link[:20]}...{invite_link[-8:]}"

def _truncate_invite_link(invite_link: str | None) -> str | None:
    if not invite_link:
        return None
    if len(invite_link) <= 40:
        return invite_link
    return f"{invite_link[:40]}..."

def _write_referral_audit(
    *,
    status: str,
    reason: str,
    chat_id: int | None,
    invitee_user_id: int | None,
    invitee_username: str | None,
    invite_link: str | None,
    inviter_user_id: int | None = None,
    error: str | None = None,
    extra: dict | None = None,
):
    payload = {
        "ts_utc": datetime.now(timezone.utc),
        "chat_id": chat_id,
        "invitee_user_id": invitee_user_id,
        "invitee_username": invitee_username,
        "invite_link": invite_link,
        "status": status,
        "reason": reason,
        "inviter_user_id": inviter_user_id,
        "error": error,
    }
    if extra:
        payload.update(extra)
    try:
        referral_audit_collection.insert_one(payload)
    except Exception:
        logger.exception(
            "[REFERRAL][ERROR] audit_write_failed invitee=%s inviter=%s reason=%s",
            invitee_user_id,
            inviter_user_id,
            reason,
        )

def _maybe_send_near_miss_dm(inviter_user_id: int, total_referrals_after: int) -> None:
    if RUNNER_MODE != "web":
        return
    if not inviter_user_id:
        return
    progress = total_referrals_after % 3
    if progress != 2:
        return
    user_doc = users_collection.find_one(
        {"user_id": inviter_user_id},
        {"last_near_miss_dm_at": 1},
    )
    last_sent_at = user_doc.get("last_near_miss_dm_at") if user_doc else None
    if isinstance(last_sent_at, str):
        try:
            last_sent_at = datetime.fromisoformat(last_sent_at.replace("Z", "+00:00"))
        except ValueError:
            last_sent_at = None
    if isinstance(last_sent_at, datetime):
        if last_sent_at.tzinfo is None:
            last_sent_at = last_sent_at.replace(tzinfo=timezone.utc)
        else:
            last_sent_at = last_sent_at.astimezone(timezone.utc)
    else:
        last_sent_at = None
    now_ts = now_utc()
    if last_sent_at and now_ts - last_sent_at < timedelta(hours=24):
        return
    text = (
        "⚡ Almost there!\n"
        "You’re 1 referral away from unlocking +200 XP.\n"
        "⏳ Complete within 24 hours."
    )
    try:
        call_bot_in_loop(app_bot.bot.send_message(chat_id=inviter_user_id, text=text))
    except (Forbidden, BadRequest) as exc:
        logger.warning(
            "[REFERRAL][NEAR_MISS_DM] send_failed inviter=%s err=%s",
            inviter_user_id,
            exc,
        )
        return
    except Exception:
        logger.exception(
            "[REFERRAL][NEAR_MISS_DM] send_error inviter=%s",
            inviter_user_id,
        )
        return
    users_collection.update_one(
        {"user_id": inviter_user_id},
        {"$set": {"last_near_miss_dm_at": now_ts}},
    )

def maybe_shout_milestones(user_id: int):
    """
    Announce:
      - every +WEEKLY_XP_BUCKET of weekly_xp
      - every +WEEKLY_REFERRAL_BUCKET of weekly_referrals
    """
    u = users_collection.find_one({"user_id": user_id})
    if not u:
        return

    weekly_xp = int(u.get("weekly_xp", 0))
    weekly_ref = int(u.get("weekly_referrals", 0))

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
        _users_update_one({"user_id": user_id}, {"$set": updates}, context="milestones_update")

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
                _users_update_one(
                    {"user_id": user_id},
                    {"$set": {"last_shout_at": datetime.now(timezone.utc)}},
                    context="milestones_last_shout",
                )
        else:
            # Optional: keep a lightweight log to spot suppressed sends in logs
            print(f"[Milestone] Suppressed (throttle) user_id={user_id} "
                  f"xp_hit={xp_hit} ref_hit={ref_hit}")

def maybe_give_first_checkin_bonus(user_id: int):
    grant_xp(db, user_id, "first_checkin", "first_checkin", FIRST_CHECKIN_BONUS_XP)

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
    if not isinstance(uid, int):
        logger.error("[WELCOME][ELIGIBILITY] skip uid_missing uid=%s", uid)
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
    try:
        welcome_eligibility_collection.update_one(
            {"uid": uid},
            {
                "$setOnInsert": {
                    "uid": uid,                    
                    "created_at": now,
                    "joined_main_at": joined_main_at or now,
                    "source": "main_join",
                },
            },
            upsert=True,
        )
    except DuplicateKeyError:
        logger.info("[WELCOME][ELIGIBILITY] dup uid=%s (already inserted)", uid)
        return None
    except Exception:
        logger.exception("[WELCOME][ELIGIBILITY] write_failed uid=%s", uid)
        return None
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

    _users_update_one(
        {"user_id": uid},
        {
            "$set": {"username": username, "joined_once": True},
            "$setOnInsert": {
                "last_checkin": None,
                "status": "Normal",
                "created_at": datetime.now(KL_TZ),                
            },
        },
        upsert=True,
        context="handle_user_join",        
    )
    joined_at = datetime.now(KL_TZ)
    _users_update_one(
        {"user_id": uid, "joined_main_at": {"$exists": False}},
        {
            "$set": {
                "joined_main_at": joined_at,
                "joined_at_source": "join_event",
                "first_join_at": joined_at,
            }
        },
        context="join_main_at",        
    )
    _ensure_welcome_eligibility(uid)
    logger.info(
        "[WELCOME] join_recorded uid=%s joined_main_at=%s",
        uid,
        joined_at.isoformat(),
    )
    
def _confirm_referral_on_main_join(
    invitee_user_id: int,
    *,
    invitee_username: str | None = None,    
    invite_link=None,
    chat_id: int | None = None,
):
    if not isinstance(invitee_user_id, int):
        logger.info(
            "[REFERRAL][SKIP] reason=invalid_uid invitee=%s chat_id=%s",
            invitee_user_id,
            chat_id or GROUP_ID,
        )
        return
    
    if isinstance(invite_link, str):
        invite_link_url = invite_link
    else:
        invite_link_url = getattr(invite_link, "invite_link", None) if invite_link else None
    invite_link_log = _truncate_invite_link(invite_link_url)

    if not invite_link_url:
        _write_referral_audit(
            status="skipped",
            reason="no_invite_link",
            chat_id=chat_id or GROUP_ID,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=None,
        )        
        logger.info(
            "[REFERRAL][SKIP] reason=no_invite_link invitee=%s chat_id=%s",
            invitee_user_id,
            chat_id or GROUP_ID,            
        )
        return

    mapping = invite_link_map_collection.find_one(
        {
            "invite_link": invite_link_url,
            "chat_id": chat_id or GROUP_ID,
            "is_active": {"$ne": False},
        },
        {"inviter_id": 1},
    )
    referrer_id = (mapping or {}).get("inviter_id")
    if not referrer_id:
        _write_referral_audit(
            status="skipped",
            reason="unknown_invite_link",
            chat_id=chat_id or GROUP_ID,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
        )
        try:
            unknown_invite_audit_collection.insert_one(
                {
                    "ts_utc": datetime.now(timezone.utc),
                    "chat_id": chat_id or GROUP_ID,
                    "invitee_user_id": invitee_user_id,
                    "invitee_username": invitee_username,
                    "invite_link": invite_link_url,
                    "status": "skipped",
                    "reason": "unknown_invite_link",
                }
            )
        except Exception:
            logger.exception(
                "[REFERRAL][ERROR] unknown_link_audit_failed invitee=%s invite_link=%s",
                invitee_user_id,
                invite_link_log,
            )        
        logger.info(
            "[REFERRAL][UNKNOWN_LINK] reason=unknown_invite_link invitee=%s invite_link=%s",
            invitee_user_id,
            invite_link_log,
        )
        return

    if referrer_id == invitee_user_id:
        _write_referral_audit(
            status="skipped",
            reason="self_invite",
            chat_id=chat_id or GROUP_ID,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
        )        
        logger.info(
            "[REFERRAL][SKIP] reason=self_invite inviter=%s invitee=%s",
            referrer_id,
            invitee_user_id,
        )
        return


    try:
        created_at_utc = now_utc()
        created_at_kl = created_at_utc.astimezone(KL_TZ).isoformat()
        result = pending_referrals_collection.update_one(
            {"group_id": chat_id or GROUP_ID, "invitee_user_id": invitee_user_id},
            {
                "$setOnInsert": {
                    "group_id": chat_id or GROUP_ID,
                    "invitee_user_id": invitee_user_id,
                    "inviter_user_id": referrer_id,
                    "invite_link": invite_link_url,
                    "created_at_utc": created_at_utc,
                    "created_at_kl": created_at_kl,
                    "status": "pending",               
                }
            },
            upsert=True,
        )
        if getattr(result, "upserted_id", None):
            logger.info(
                "[REFERRAL][PENDING] inviter=%s invitee=%s invite_link=%s hold_hours=%s",
                referrer_id,
                invitee_user_id,
                invite_link_log,
                REFERRAL_HOLD_HOURS,
            )
        else:            
            logger.info(
                "[REFERRAL][PENDING_SKIP] reason=exists inviter=%s invitee=%s",
                referrer_id,
                invitee_user_id,
            )

    except Exception as e:
        _write_referral_audit(
            status="failed",
            reason="error",
            chat_id=chat_id or GROUP_ID,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
            error=str(e),
        )        
        logger.exception(
            "[REFERRAL][ERROR] step=create_pending inviter=%s invitee=%s err=%s",
            step,
            referrer_id,
            invitee_user_id,
            e,
        )
    return

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
    try:
        users_collection.create_index(
            [("user_id", 1)],
            unique=True,
            name="uniq_user_id",
            sparse=True,
        )
    except Exception as e:
        msg = str(e)
        if "already exists with different options" in msg:
            try:
                users_collection.drop_index("uniq_user_id")
            except Exception:
                for ix in users_collection.list_indexes():
                    if ix.get("key") == {"user_id": 1}:
                        users_collection.drop_index(ix["name"])
                        break
            users_collection.create_index(
                [("user_id", 1)],
                unique=True,
                name="uniq_user_id",
                sparse=True,
            )
        else:
            print("⚠️ ensure_indexes error:", e)    
    try:
        invite_link_map_collection.create_index(
            [("chat_id", 1), ("invite_link", 1)],
            unique=True,
            name="uniq_chat_invite_link",
        )
    except Exception as e:
        msg = str(e)
        if "already exists with different options" in msg:
            try:
                invite_link_map_collection.drop_index("uniq_chat_invite_link")
            except Exception:
                for ix in invite_link_map_collection.list_indexes():
                    if ix.get("key") == {"chat_id": 1, "invite_link": 1}:
                        invite_link_map_collection.drop_index(ix["name"])
                        break
            invite_link_map_collection.create_index(
                [("chat_id", 1), ("invite_link", 1)],
                unique=True,
                name="uniq_chat_invite_link",
            )
        else:
            print("⚠️ ensure_indexes error:", e)
    invite_link_map_collection.create_index(
        [("invite_link", 1)],
        name="idx_invite_link",
    )            
    unknown_invite_links_collection.create_index(
        [("chat_id", 1), ("invite_link", 1), ("invitee_id", 1)],
        unique=True,
        name="uniq_unknown_invite",
    )
    referral_award_events_collection.create_index(
        [("award_key", 1)],
        unique=True,
        name="uniq_referral_award_key",
    )
    referral_events_collection.create_index(
        [("event", 1), ("inviter_id", 1), ("invitee_id", 1)],
        unique=True,
        name="uniq_referral_event",
    )
    referral_events_collection.create_index(
        [("inviter_id", 1), ("occurred_at", 1)],
        name="referral_events_by_inviter_time",
    )    
    pending_referrals_collection.create_index(
        [("group_id", 1), ("invitee_user_id", 1)],
        unique=True,
        name="uniq_pending_invitee",
    )
    pending_referrals_collection.create_index(
        [("status", 1), ("created_at_utc", 1)],
        name="pending_by_time",
    )
    pending_referrals_collection.create_index(
        [("status", 1), ("next_retry_at_utc", 1)],
        name="pending_by_retry",
    )
    pending_referrals_collection.create_index(
        [("inviter_user_id", 1), ("status", 1)],
        name="pending_by_inviter",
    )    

    # --- optional welcome eligibility ---
    db.welcome_eligibility.create_index([("uid", 1)], unique=True)
    db.welcome_eligibility.create_index([("expires_at", 1)], expireAfterSeconds=0)
    db.welcome_tickets.create_index([("uid", 1)], unique=True)
    db.welcome_tickets.create_index([("cleanup_at", 1)], expireAfterSeconds=0)
    
    xp_events_collection.create_index([("user_id", 1), ("reason", 1)])
    ensure_xp_indexes(db)


    try:
        for legacy_name in ("uniq_tg_verify_user_id", "uniq_user_checks"):
            try:
                tg_verification_queue_collection.drop_index(legacy_name)
            except Exception:
                pass
        tg_verification_queue_collection.create_index(
            [("user_id", 1)],
            unique=True,
            name="uq_tg_verif_user_id_nonnull",
            partialFilterExpression={"user_id": {"$exists": True, "$ne": None}},
        )
        tg_verification_queue_collection.create_index(
            [("status", 1), ("created_at", 1)],
            name="ix_verif_status_created",
        )
    except Exception as e:
        print("⚠️ ensure_indexes error:", e)

    ensure_affiliate_indexes(db)
        
ensure_indexes()

def _cleanup_tg_verification_queue_bad_docs():
    if os.getenv("VERIFY_QUEUE_CLEANUP") != "1":
        return
    try:
        result = tg_verification_queue_collection.delete_many(
            {"$or": [{"user_id": None}, {"user_id": {"$exists": False}}]}
        )
        logger.info(
            "[VERIFY_QUEUE] cleanup_bad_docs deleted=%s",
            result.deleted_count,
        )
    except Exception:
        logger.exception("[VERIFY_QUEUE] cleanup_bad_docs_failed")

_cleanup_tg_verification_queue_bad_docs()

def _cleanup_welcome_null_uid():
    if os.getenv("WELCOME_CLEANUP_BAD_UID") != "1":
        return
    try:
        result = welcome_eligibility_collection.delete_many(
            {"$or": [{"uid": None}, {"uid": {"$exists": False}}]}
        )
        logger.info(
            "[WELCOME][ELIGIBILITY] cleanup_bad_uid deleted=%s",
            result.deleted_count,
        )
    except Exception:
        logger.exception("[WELCOME][ELIGIBILITY] cleanup_bad_uid_failed")

_cleanup_welcome_null_uid()

def get_or_create_referral_invite_link_sync(user_id: int, username: str = "") -> str:
    """
    Create (or reuse) a unique Telegram chat invite link for this user.
    Uses Telegram HTTP API (sync), so no asyncio/event loop issues.
    Caches the link in Mongo to avoid rate limits.
    """
    # 1) Reuse latest active invite link from DB if available
    latest_link_doc = invite_link_map_collection.find_one(
        {"chat_id": GROUP_ID, "inviter_id": user_id, "is_active": True},
        sort=[("created_at", -1)],
    )
    if latest_link_doc and latest_link_doc.get("invite_link"):
        invite_link = latest_link_doc["invite_link"]
        logger.info(
            "[REFERRAL][LINK_REUSE] inviter=%s chat_id=%s invite_link=%s db=hit",
            user_id,
            GROUP_ID,
            _short_invite_link(invite_link),
        )
        return invite_link

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

    try:
        invite_link_map_collection.insert_one(
            {
                "inviter_id": user_id,
                "chat_id": GROUP_ID,
                "invite_link": invite_link,
                "is_active": True,
                "created_at": datetime.now(KL_TZ),
            }
        )
        logger.info(
            "[REFERRAL][LINK_CREATE_OK] inviter=%s chat_id=%s invite_link=%s db=ok",
            user_id,
            GROUP_ID,
            _short_invite_link(invite_link),
        )
    except DuplicateKeyError:
        logger.info(
            "[REFERRAL][LINK_REUSE] inviter=%s chat_id=%s invite_link=%s db=duplicate",
            user_id,
            GROUP_ID,
            _short_invite_link(invite_link),
        )
    except Exception as e:
        logger.exception(
            "[REFERRAL][LINK_CREATE_DB_FAIL] inviter=%s chat_id=%s invite_link=%s err=%s",
            user_id,
            GROUP_ID,
            _short_invite_link(invite_link),
            e,
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

    since = datetime.now(timezone.utc) - timedelta(days=days)
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
httpx_request = HTTPXRequest(
    connect_timeout=10,
    read_timeout=20,
    write_timeout=20,
    pool_timeout=10,
    connection_pool_size=8,
)
app_bot = ApplicationBuilder().token(BOT_TOKEN).request(httpx_request).build()


def _admin_secret_ok() -> bool:
    configured = (os.getenv("ADMIN_PANEL_SECRET") or "").strip()
    if not configured:
        return True
    sent = (request.headers.get("X-Admin-Secret") or request.args.get("admin_secret") or "").strip()
    return bool(sent and sent == configured)


@app.route("/admin/pools/upload", methods=["POST"])
def admin_pools_upload():
    if not _admin_secret_ok():
        return jsonify({"status": "error", "reason": "unauthorized"}), 403
    data = request.get_json(silent=True) or {}
    pool_id = str(data.get("pool_id") or "").strip().upper()
    if pool_id not in {"WELCOME", "T1", "T2", "T3", "T4"}:
        return jsonify({"status": "error", "reason": "bad_pool_id"}), 400
    codes_text = str(data.get("codes_text") or "")
    rows = [line.strip() for line in codes_text.replace("\r", "\n").split("\n") if line.strip()]
    if not rows:
        return jsonify({"status": "error", "reason": "empty_codes"}), 400
    now_ts = now_utc()
    inserted = 0
    for code in rows:
        try:
            voucher_pools_collection.insert_one({
                "pool_id": pool_id,
                "code": code,
                "status": "available",
                "issued_to": None,
                "issued_at": None,
                "ledger_id": None,
                "display_label": data.get("display_label"),
                "value_hint": data.get("value_hint"),
                "currency": data.get("currency"),
                "created_at": now_ts,
            })
            inserted += 1
        except DuplicateKeyError:
            continue
    return jsonify({"status": "ok", "inserted": inserted, "received": len(rows), "pool_id": pool_id})


@app.route("/admin/pools/summary", methods=["GET"])
def admin_pools_summary():
    if not _admin_secret_ok():
        return jsonify({"status": "error", "reason": "unauthorized"}), 403
    out = []
    for pool_id in ("WELCOME", "T1", "T2", "T3", "T4"):
        available = voucher_pools_collection.count_documents({"pool_id": pool_id, "status": "available"})
        issued = voucher_pools_collection.count_documents({"pool_id": pool_id, "status": "issued"})
        sample = voucher_pools_collection.find_one({"pool_id": pool_id}, {"display_label": 1, "value_hint": 1, "currency": 1}) or {}
        out.append({
            "pool_id": pool_id,
            "available": int(available),
            "issued": int(issued),
            "display_label": sample.get("display_label"),
            "value_hint": sample.get("value_hint"),
            "currency": sample.get("currency"),
        })
    return jsonify({"status": "ok", "items": out})


@app.route("/admin/affiliate/pending", methods=["GET"])
def admin_affiliate_pending():
    if not _admin_secret_ok():
        return jsonify({"status": "error", "reason": "unauthorized"}), 403
    status = str(request.args.get("status") or "PENDING_REVIEW").strip().upper()
    if status not in {"PENDING_REVIEW", "PENDING_MANUAL"}:
        return jsonify({"status": "error", "reason": "bad_status"}), 400
    rows = list(affiliate_ledger_collection.find({"status": status}).sort("created_at", 1).limit(200))
    items = []
    for row in rows:
        items.append({
            "ledger_id": str(row.get("_id")),
            "user_id": row.get("user_id"),
            "year_month": row.get("year_month"),
            "tier": row.get("tier"),
            "pool_id": row.get("pool_id"),
            "status": row.get("status"),
            "risk_flags": row.get("risk_flags") or [],
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        })
    return jsonify({"status": "ok", "items": items})


@app.route("/admin/affiliate/<ledger_id>/approve", methods=["POST"])
def admin_affiliate_approve(ledger_id):
    if not _admin_secret_ok():
        return jsonify({"status": "error", "reason": "unauthorized"}), 403
    try:
        oid = ObjectId(ledger_id)
    except Exception:
        return jsonify({"status": "error", "reason": "bad_ledger_id"}), 400
    ledger = approve_affiliate_ledger(db, ledger_id=oid, now_utc=now_utc())
    if not ledger:
        return jsonify({"status": "error", "reason": "not_found"}), 404
    return jsonify({"status": "ok", "ledger_status": ledger.get("status"), "voucher_code": ledger.get("voucher_code")})


@app.route("/admin/affiliate/<ledger_id>/reject", methods=["POST"])
def admin_affiliate_reject(ledger_id):
    if not _admin_secret_ok():
        return jsonify({"status": "error", "reason": "unauthorized"}), 403
    try:
        oid = ObjectId(ledger_id)
    except Exception:
        return jsonify({"status": "error", "reason": "bad_ledger_id"}), 400
    data = request.get_json(silent=True) or {}
    reject_affiliate_ledger(db, ledger_id=oid, reason=data.get("reason"), now_utc=now_utc())
    return jsonify({"status": "ok"})
@app.route("/api/is_admin")
def api_is_admin():
    try:
        user_id = int(request.args.get("user_id"))

        doc = admin_cache_col.find_one({"_id": "admins"}) or {}
        ids = set(doc.get("ids", []))
        is_admin = user_id in ids

        # optional: cache a per-user flag for faster UI checks
        _users_update_one(
            {"user_id": user_id},
            {"$set": {"is_admin": is_admin, "is_admin_checked_at": datetime.now(timezone.utc)}},
            upsert=True,
            context="admin_flag",
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


@app.route("/me", methods=["GET"])
def api_me():
    init_data_raw = (request.headers.get("X-Tg-InitData") or "").strip()
    if not init_data_raw:
        return jsonify({"ok": True, "user_id": None, "is_admin": False})

    try:
        ok, parsed, _ = verify_telegram_init_data(init_data_raw)
        if not ok:
            return jsonify({"ok": True, "user_id": None, "is_admin": False})

        user_payload = parsed.get("user")
        if isinstance(user_payload, str):
            user_payload = json.loads(user_payload)
        if not isinstance(user_payload, dict):
            user_payload = {}

        try:
            user_id = int(user_payload.get("id"))
        except (TypeError, ValueError):
            user_id = None

        return jsonify({
            "ok": True,
            "user_id": user_id,
            "is_admin": bool(user_id is not None and user_id in set(ADMIN_UIDS)),
        })
    except Exception:
        return jsonify({"ok": True, "user_id": None, "is_admin": False})

async def refresh_admin_ids(context: ContextTypes.DEFAULT_TYPE):
    try:
        admins = await context.bot.get_chat_administrators(chat_id=GROUP_ID)
        ids = [a.user.id for a in admins]
        admin_cache_col.update_one(
            {"_id": "admins"},
            {"$set": {"ids": ids, "refreshed_at": datetime.now(timezone.utc)}},
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
    is_new_user = not bool(user)
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
    _users_update_one(
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
                "status": "Normal",                
            },
        },
        upsert=True,
        context="checkin_update",
    )

    checkin_key = f"checkin:{today_kl.strftime('%Y%m%d')}"
    grant_xp(db, user_id, "checkin", checkin_key, base_xp + bonus_xp)
    record_first_checkin(int(user_id), ref=now_utc_ts)

    welcome_issue = issue_welcome_bonus_if_eligible(
        db,
        user_id=int(user_id),
        is_new_user=is_new_user,
        blocked=bool(user.get("blocked")),
        now_utc=now_utc_ts,
    )
    
    maybe_shout_milestones(int(user_id))

    labels = {7: "🎉 7-day streak bonus!", 14: "🔥 14-day streak bonus!", 28: "🏆 28-day streak bonus!"}
    lines = [
        f"✅ Check-in successful! (+{base_xp} XP)",
        f"🔥 Current streak: {streak} days."
    ]
    if bonus_xp:
        lines.append(f"{labels[streak]} +{bonus_xp} XP")
    if welcome_issue.get("status") == "ISSUED" and welcome_issue.get("voucher_code"):
        lines.append(f"🎁 Welcome voucher: `{welcome_issue['voucher_code']}`")
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

        record_user_last_seen(
            db,
            user_id=int(user_id),
            ip=request.headers.get("Fly-Client-IP") or request.remote_addr,
            subnet=request.headers.get("X-Forwarded-For"),
            session=request.headers.get("X-Session-Id") or request.cookies.get("session") or request.headers.get("User-Agent"),
        )

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

    _users_update_one(
        {"user_id": user_id},
        {"$set": {"region": region}},
        upsert=True,
        context="set_region",
    )
    return jsonify({"success": True, "region": region, "locked": True})

@app.route("/")
def home():
    return "Bot is alive!"

def _apply_no_store_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/miniapp")
def serve_mini_app():
    req_v = request.args.get("v")
    if req_v != MINIAPP_VERSION:
        query_params = request.args.to_dict(flat=False)
        query_params["v"] = MINIAPP_VERSION
        query_string = urlencode(query_params, doseq=True)
        redirect_response = make_response(redirect(f"{request.path}?{query_string}", code=302))
        _apply_no_store_headers(redirect_response)
        logger.info("[MINIAPP][REDIRECT] from_v=%s to_v=%s", req_v, MINIAPP_VERSION)
        return redirect_response    
    response = make_response(send_from_directory("static", "index.html"))
    _apply_no_store_headers(response)
    logger.info("[MINIAPP] served static/index.html v=%s", MINIAPP_VERSION)
    return response

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

    snapshot, snapshot_ts, snapshot_age_sec = _get_user_snapshot(user_id)
    if snapshot:
        stats = {
            "total_referrals": int(snapshot.get("total_referrals", 0)),
            "weekly_referrals": int(snapshot.get("weekly_referrals", 0)),
            "monthly_referrals": int(snapshot.get("monthly_referrals", 0)),
        }
    payload = {
        "success": success,
        "referral_link": link,
        "snapshot_ts": snapshot_ts,
        "snapshot_age_sec": snapshot_age_sec,
        **stats,
    }
    if error:
        payload["error"] = error
    if not link:
        payload["referral_link"] = None

    return jsonify(payload), 200

def mask_username(username: str) -> str:
    if not username:
        return "********"
    u = str(username).lstrip("@")

    # too short: keep as-is or minimal mask
    if len(u) <= 2:
        return u[0] + "*" * (len(u) - 1)
    if len(u) <= 6:
        # keep a bit readable without leaking full
        return u[:2] + "***"
    # main rule: front4 + *** + last2
    return f"{u[:4]}***{u[-2:]}"


# Format usernames depending on admin or own account
def format_username(u, current_user_id, is_admin):
    name = None
    if u.get("username"):
        name = str(u["username"]).lstrip("@")   # pure username
    elif u.get("first_name"):
        name = str(u["first_name"])

    if not name:
        return None

    # normalize ids to avoid "self masked" due to str/int mismatch
    try:
        uid = int(u.get("user_id")) if u.get("user_id") is not None else None
    except Exception:
        uid = None
    try:
        cur = int(current_user_id) if current_user_id is not None else None
    except Exception:
        cur = None

    # Mask if not admin & not own account
    if (not is_admin) and (uid != cur):
        return mask_username(name)

    # Admin or own account → show full name
    return name

@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        raw_user_id = request.args.get("user_id")
        try:
            current_user_id = int(raw_user_id) if raw_user_id not in (None, "", "undefined") else 0
        except (TypeError, ValueError):
            current_user_id = 0

        week_start_utc, week_end_utc, week_start_local = _week_window_utc()
        week_end_local = week_start_local + timedelta(days=7)
        leaderboard_limit = 15
        cache_key = f"leaderboard|{week_start_local.date().isoformat()}|{leaderboard_limit}"
        cached_entry = LEADERBOARD_CACHE.get(cache_key)
        now_ts = time.time()

        user_record = users_collection.find_one({"user_id": current_user_id}, {"is_admin": 1}) or {}
        is_admin = bool(user_record.get("is_admin", False))
        logger.info(
            "[LEADERBOARD] week_window local_start=%s local_end=%s",
            week_start_local.isoformat(),
            week_end_local.isoformat(),
        )
        logger.info("[LEADERBOARD] pipeline_source=users")

        if cached_entry and (now_ts - cached_entry["ts"]) < CACHE_TTL_SECONDS:
            age = int(now_ts - cached_entry["ts"])
            logger.info("[LEADERBOARD][CACHE_HIT] key=%s age=%ss", cache_key, age)
            cached_payload = cached_entry["payload"]
        else:
            xp_query = {"user_id": {"$ne": None}}
            logger.info(
                "[lb_query] users.find filter=%s sort=weekly_xp:-1 limit=%s",
                xp_query,
                leaderboard_limit,
            )
            xp_rows = list(
                users_collection
                .find(xp_query, {"user_id": 1, "username": 1, "weekly_xp": 1})
                .sort("weekly_xp", DESCENDING)
                .limit(leaderboard_limit)
            )

            referral_rows = list(
                users_collection.find(
                    {"user_id": {"$ne": None}},
                    {
                        "user_id": 1,
                        "username": 1,
                        "weekly_referrals": 1,
                        "total_referrals": 1,
                    },
                )
                .sort("weekly_referrals", DESCENDING)
                .limit(leaderboard_limit)
            )
            if referral_rows:
                top_row = referral_rows[0]
                logger.info(
                    "[LEADERBOARD] result_count=%s top1=(%s,%s)",
                    len(referral_rows),
                    top_row.get("user_id"),
                    int(top_row.get("weekly_referrals", 0)),
                )
            else:
                logger.info("[LEADERBOARD] result_count=0 top1=(none)")

            cached_payload = {
                "checkin": [
                    {
                        "user_id": row.get("user_id"),
                        "username": row.get("username"),
                        "weekly_xp": int(row.get("weekly_xp", 0)),
                    }
                    for row in xp_rows
                ],
                "referral": [
                    {
                        "user_id": row.get("user_id"),
                        "username": row.get("username"),
                        "weekly_referrals": int(row.get("weekly_referrals", 0)),
                        "total_referrals": int(row.get("total_referrals", 0)),
                    }
                    for row in referral_rows
                ],
            }
            LEADERBOARD_CACHE[cache_key] = {"ts": time.time(), "payload": cached_payload}
            logger.info("[LEADERBOARD][CACHE_SET] key=%s ttl=%s", cache_key, CACHE_TTL_SECONDS)
        
        def safe_format(u):
            if "user_id" not in u:
                u["user_id"] = 0
            return format_username(u, current_user_id, is_admin)
            
        top_checkins = []
        for row in cached_payload.get("checkin", []):
            formatted = safe_format({"user_id": row.get("user_id"), "username": row.get("username")})
            if not formatted:
                continue
            top_checkins.append({"username": formatted, "xp": int(row.get("weekly_xp", 0))})

        referral_board = []
        for row in cached_payload.get("referral", []):
            formatted = safe_format({"user_id": row.get("user_id"), "username": row.get("username")})
            if not formatted:
                continue
            entry = {
                "username": formatted,
                "total_valid": int(row.get("weekly_referrals", 0)),
                "referrals": int(row.get("weekly_referrals", 0)),
            }
            if is_admin:
                entry["total_all"] = int(row.get("total_referrals", 0))

            referral_board.append(entry)
            
        leaderboard = {
            "checkin": top_checkins,
            "referral": referral_board,
        }
        
        snapshot, snapshot_ts, snapshot_age_sec = _get_user_snapshot(current_user_id)
        if snapshot:
            user_weekly_xp = int(snapshot.get("weekly_xp", 0))
            user_weekly_referrals = int(snapshot.get("weekly_referrals", 0))
            monthly_xp_value = int(snapshot.get("monthly_xp", 0))
            lifetime_valid_refs = int(snapshot.get("total_referrals", 0))
            monthly_referrals_value = int(snapshot.get("monthly_referrals", 0))
            user_status = snapshot.get("vip_tier") or "Normal"
        else:
            user_weekly_xp = 0
            user_weekly_referrals = 0
            monthly_xp_value = 0
            lifetime_valid_refs = 0
            monthly_referrals_value = 0            
            user_status = "Normal"
                
        user_stats = {
            "xp": user_weekly_xp,
            "monthly_xp": monthly_xp_value,
            "referrals": user_weekly_referrals,
            "total_valid": user_weekly_referrals,
            "weekly_referrals": user_weekly_referrals,
            "monthly_referrals": monthly_referrals_value,
            "total_referrals": lifetime_valid_refs,            
            "status": user_status,
            "lifetime_valid": lifetime_valid_refs,
        }

        logger.info(
            "[lb_debug] uid=%s weekly_xp=%s awarded_referrals=%s",
            current_user_id,
            user_weekly_xp,
            user_weekly_referrals,
        )
        
        payload = {
            "success": True,
            "leaderboard": leaderboard,
            "user": user_stats,
            "snapshot_ts": snapshot_ts,
            "snapshot_age_sec": snapshot_age_sec,
        }
        return jsonify(payload)

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
                "referrals": u.get("weekly_referrals") or u.get("referrals") or 0
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
        is_vip = (user.get("vip_tier") or user.get("status")) == "VIP1"
        if not is_vip and not is_admin:
            return jsonify({"code": None})

        now = datetime.now(timezone.utc)

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
    result = update_user_xp(username, amount, idempotency_key)
    if isinstance(result, dict):
        return (
            jsonify({"success": False, "message": result["message"], "code": result["code"]}),
            429,
        )
    success, message = result
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

        _users_update_one(
            {"user_id": int(user_id)},
            {
                "$set": {"username": username, "welcome_xp_claimed": True},
                "$setOnInsert": {
                    "status": "Normal",
                },
            },
            upsert=True,
            context="starterpack",            
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
        writer.writerow([
            "user_id",
            "username",
            "total_xp",
            "weekly_xp",
            "total_referrals",
            "weekly_referrals",
            "monthly_referrals",
            "monthly_xp",
            "vip_tier",
        ])
        for u in users:
            writer.writerow([
                u.get("user_id"),
                u.get("username", ""),
                u.get("total_xp", u.get("xp", 0)),
                u.get("weekly_xp", 0),
                u.get("total_referrals", 0),
                u.get("weekly_referrals", 0),
                u.get("monthly_referrals", 0),
                u.get("monthly_xp", 0),
                u.get("vip_tier", u.get("status", "Normal")),
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
def reset_weekly_xp(run_id: str | None = None):
    run_id = run_id or _new_run_id()
    now = datetime.now(KL_TZ)

    # Last full week [Mon..Sun], assuming this runs every Monday 00:00 KL
    week_end_date = (now - timedelta(days=1)).date()      # Sunday
    week_start_date = week_end_date - timedelta(days=6)   # Monday
    logger.info(
        "[JOB][WEEKLY] start week_start=%s week_end=%s run_id=%s instance=%s tz=%s",
        week_start_date.isoformat(),
        week_end_date.isoformat(),
        run_id,
        INSTANCE_ID,
        tz_name(KL_TZ),
    )
    try:
        with JobTimer() as timer:
            proj = {"user_id": 1, "username": 1, "weekly_xp": 1, "weekly_referrals": 1}
            top_checkin = list(users_collection.find({}, proj).sort("weekly_xp", DESCENDING).limit(100))
            top_referrals = list(users_collection.find({}, proj).sort("weekly_referrals", DESCENDING).limit(100))

            history_collection.insert_one({
                "week_start": week_start_date.isoformat(),
                "week_end":   week_end_date.isoformat(),
                "checkin_leaderboard": [
                    {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_xp": u.get("weekly_xp", 0)}
                    for u in top_checkin
                ],
                "referral_leaderboard": [
                    {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_referrals": u.get("weekly_referrals", 0)}
                    for u in top_referrals
                ],
                # store as UTC so later math is safe
                "archived_at": datetime.now(timezone.utc)
            })

            _users_update_many(
                {},
                {
                    "$set": {
                        "weekly_xp": 0,
                        "weekly_referrals": 0,
                        "xp_weekly_milestone_bucket": 0,
                        "ref_weekly_milestone_bucket": 0,
                    }
                },
                context="weekly_reset",
            )

        logger.info(
            "[JOB][WEEKLY] done processed=%s elapsed_s=%.2f run_id=%s",
            len(top_checkin),
            timer.elapsed_s,
            run_id,
        )
    except Exception as exc:
        logger.error(
            "[JOB][WEEKLY] failed run_id=%s instance=%s err=%s msg=%s",
            run_id,
            INSTANCE_ID,
            exc.__class__.__name__,
            str(exc),
        )
        raise

meta = db["meta"]

def backfill_missing_statuses():
    res = _users_update_many(
        {"status": {"$exists": False}},
        {"$set": {"status": "Normal"}},
        context="backfill_status",
    )
    logger.info("[xp_recompute] backfill_status modified=%s", getattr(res, "modified_count", 0))
    return getattr(res, "modified_count", 0)

def one_time_fix_monthly_xp():
    # run once ever
    if meta.find_one({"_id": "fix_monthly_xp_done"}):
        return
    res = _users_update_many(
        {"monthly_xp": {"$exists": False}},
        {"$set": {"monthly_xp": 0}},
        context="backfill_monthly_xp",
    )
    meta.update_one(
        {"_id": "fix_monthly_xp_done"},
        {"$set": {"done_at": datetime.now(timezone.utc), "modified": res.modified_count}},
        upsert=True
    )
    print(f"🔧 monthly_xp backfilled on first boot. Modified: {res.modified_count}")

def run_boot_catchup():
    now = datetime.now(KL_TZ)
    run_id = _new_run_id()
    logger.info(
        "[BOOT][CATCHUP] start run_id=%s instance=%s tz=%s",
        run_id,
        INSTANCE_ID,
        tz_name(KL_TZ),
    )
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
        last_reset = None
        days_since = 999

    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    if now.weekday() == 0 and days_since >= 6:
        logger.warning(
            "[BOOT][CATCHUP] missed_weekly expected=%s last_run=%s",
            week_start.isoformat(),
            last_reset.isoformat() if last_reset else None,
        )
        logger.info("[BOOT][CATCHUP] running job=weekly run_id=%s", run_id)
        try:
            with JobTimer() as timer:
                reset_weekly_xp(run_id=run_id)
            logger.info(
                "[BOOT][CATCHUP] done job=weekly result=ok elapsed_s=%.2f run_id=%s",
                timer.elapsed_s,
                run_id,
            )
        except Exception as exc:
            logger.error(
                "[BOOT][CATCHUP] failed job=weekly err=%s msg=%s run_id=%s",
                exc.__class__.__name__,
                str(exc),
                run_id,
            )
    else:
        reason = "not_monday" if now.weekday() != 0 else "already_ran"
        logger.info("[BOOT][CATCHUP] skipped job=weekly reason=%s run_id=%s", reason, run_id)

    # monthly catch-up (only on the 1st)
    sample_user = users_collection.find_one(
        {"last_status_update": {"$exists": True}},
        sort=[("last_status_update", DESCENDING)]
    )
    last_month = sample_user["last_status_update"].month if sample_user else None
    last_year = sample_user["last_status_update"].year if sample_user else None
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now.day == 1 and (not sample_user or last_month != now.month or last_year != now.year):
        logger.warning(
            "[BOOT][CATCHUP] missed_monthly expected=%s last_run=%s",
            month_start.isoformat(),
            sample_user["last_status_update"].isoformat() if sample_user else None,
        )
        logger.info("[BOOT][CATCHUP] running job=monthly run_id=%s", run_id)
        try:
            with JobTimer() as timer:
                update_monthly_vip_status(run_id=run_id)
            logger.info(
                "[BOOT][CATCHUP] done job=monthly result=ok elapsed_s=%.2f run_id=%s",
                timer.elapsed_s,
                run_id,
            )
        except Exception as exc:
            logger.error(
                "[BOOT][CATCHUP] failed job=monthly err=%s msg=%s run_id=%s",
                exc.__class__.__name__,
                str(exc),
                run_id,
            )
    else:
        reason = "not_first_day" if now.day != 1 else "already_ran"
        logger.info("[BOOT][CATCHUP] skipped job=monthly reason=%s run_id=%s", reason, run_id)
        
    # one-time migration instead of scanning every boot
    one_time_fix_monthly_xp()

    if os.getenv("BACKFILL_STATUS_ON_BOOT", "false").lower() == "true":
        backfill_missing_statuses()

def apply_monthly_tier_update(run_time: datetime | None = None, run_id: str | None = None):
    run_id = run_id or _new_run_id()
    run_at_local = run_time.astimezone(KL_TZ) if run_time else datetime.now(KL_TZ)
    start_utc, end_utc, start_local, end_local = _current_month_window_utc(run_at_local)
    end_local = end_utc.astimezone(KL_TZ)
    month_key = start_local.strftime("%Y-%m")
    now_utc = datetime.now(timezone.utc)

    promoted: list[int] = []
    demoted: list[int] = []
    processed = 0
    updated = 0
    skipped = 0

    total_users = users_collection.count_documents({})
    logger.info(
        "[JOB][MONTHLY] start month=%s run_id=%s instance=%s tz=%s window=%s..%s users=%s",
        month_key,
        run_id,
        INSTANCE_ID,
        tz_name(KL_TZ),
        start_local.isoformat(),
        end_local.isoformat(),
        total_users,
    )    
    logger.info(
        "[VIP][MONTHLY] window=%s..%s users=%s",
        start_local.isoformat(),
        end_local.isoformat(),
        total_users,
    )

    tier_rank = {"Normal": 0, "VIP1": 1}

    def _tier_from_monthly_xp(monthly_total: int) -> str:
        return "VIP1" if monthly_total >= 800 else "Normal"

    def _tier_rank(value) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            return tier_rank.get(value, 0)
        return 0    
    def iter_users_paged(projection: dict, batch_size: int = 500, start_after=None):
        last_id = start_after
        while True:
            query = {"_id": {"$gt": last_id}} if last_id else {}
            batch = list(
                users_collection.find(query, projection=projection)
                .sort("_id", ASCENDING)
                .limit(batch_size)
            )
            if not batch:
                break
            for doc in batch:
                yield doc
            last_id = batch[-1].get("_id")

    projection = {
        "user_id": 1,
        "monthly_xp": 1,
        "status": 1,
        "vip_month": 1,
        "vip_tier": 1,
        "username": 1,
    }
    batch_size = 500
    cache_key = "vip_monthly:last_id"
    cached_state = admin_cache_col.find_one({"_id": cache_key}, {"last_id": 1}) or {}
    last_id = cached_state.get("last_id")
    retries = 0
    batch_processed = 0

    success = False
    try:
        with JobTimer() as total_timer:
            while retries < 3:
                try:
                    for user in iter_users_paged(projection, batch_size=batch_size, start_after=last_id):
                        uid = user.get("user_id")
                        if uid is None:
                            continue
                        last_id = user.get("_id")
                        monthly_total = int(user.get("monthly_xp", 0))
                        # monthly_xp derived from snapshot ledger settles
                        computed_tier = _tier_from_monthly_xp(monthly_total)
                        current_status = user.get("status", "Normal")
                        existing_month = user.get("vip_month")
                        existing_tier = user.get("vip_tier")
                        existing_rank = _tier_rank(existing_tier) if existing_tier is not None else -1
                        computed_rank = _tier_rank(computed_tier)

                        if existing_month == month_key:
                            # VIP should not downgrade within a month
                            if existing_rank > computed_rank:
                                final_tier = existing_tier
                                logger.info(
                                    "[VIP][MONTHLY] keep_tier uid=%s month=%s existing=%s computed=%s",
                                    uid,
                                    month_key,
                                    existing_tier,
                                    computed_tier,
                                )
                            else:
                                final_tier = computed_tier
                                if computed_rank > existing_rank:
                                    logger.info(
                                        "[VIP][MONTHLY] upgrade uid=%s month=%s from=%s to=%s",
                                        uid,
                                        month_key,
                                        existing_tier,
                                        computed_tier,
                                    )
                        else:
                            final_tier = computed_tier

                        if final_tier != current_status:
                            updated += 1
                            if _tier_rank(final_tier) > _tier_rank(current_status):
                                promoted.append(uid)
                            else:
                                demoted.append(uid)
                        else:
                            skipped += 1

                        monthly_xp_history_collection.update_one(
                            {"user_id": uid, "month": month_key},
                            {
                                "$set": {
                                    "user_id": uid,
                                    "username": user.get("username"),
                                    "month": month_key,
                                    "monthly_xp": monthly_total,
                                    "status_before_reset": current_status,
                                    "status_after_reset": final_tier,
                                    "captured_at_utc": now_utc,
                                    "captured_at_kl": run_at_local.isoformat(),
                                }
                            },
                            upsert=True,
                        )

                        _users_update_one(
                            {"user_id": uid},
                            {
                                "$set": {
                                    "status": final_tier,
                                    "last_status_update": run_at_local,
                                    "monthly_xp": monthly_total,
                                    "vip_month": month_key,
                                    "vip_tier": final_tier,
                                    "vip_updated_at": now_utc,
                                    "snapshot_updated_at": now_utc,
                                }
                            },
                            context="monthly_tier_update",
                        )
                        processed += 1
                        batch_processed += 1
                        
                        if batch_processed >= batch_size:
                            logger.info(
                                "[JOB][MONTHLY] progress processed=%s last_id=%s updated=%s skipped=%s run_id=%s",
                                processed,
                                last_id,
                                updated,
                                skipped,
                                run_id,
                            )
                            logger.info(
                                "[VIP][MONTHLY] processed=%s last_id=%s",
                                processed,
                                last_id,
                            )
                            admin_cache_col.update_one(
                                {"_id": cache_key},
                                {"$set": {"last_id": last_id, "updated_at": now_utc}},
                                upsert=True,
                            )
                            batch_processed = 0
                    if batch_processed:
                        logger.info(
                            "[JOB][MONTHLY] progress processed=%s last_id=%s updated=%s skipped=%s run_id=%s",
                            processed,
                            last_id,
                            updated,
                            skipped,
                            run_id,
                        )
                        logger.info(
                            "[VIP][MONTHLY] processed=%s last_id=%s",
                            processed,
                            last_id,
                        )
                        admin_cache_col.update_one(
                            {"_id": cache_key},
                            {"$set": {"last_id": last_id, "updated_at": now_utc}},
                            upsert=True,
                        )
                    admin_cache_col.delete_one({"_id": cache_key})
                    success = True
                    break
                except CursorNotFound as exc:
                    retries += 1
                    logger.warning(
                        "[VIP][MONTHLY] cursor_not_found retry=%s last_id=%s",
                        retries,
                        last_id,
                        exc_info=True,                        
                    )
                    logger.error(
                        "[JOB][MONTHLY] failed err=%s msg=%s run_id=%s",
                        exc.__class__.__name__,
                        str(exc),
                        run_id,
                    )
                    if last_id is not None:
                        admin_cache_col.update_one(
                            {"_id": cache_key},
                            {"$set": {"last_id": last_id, "updated_at": now_utc}},
                            upsert=True,
                        )
                    if retries >= 3:
                        raise
    except Exception as exc:
        logger.error(
            "[JOB][MONTHLY] failed err=%s msg=%s run_id=%s",
            exc.__class__.__name__,
            str(exc),
            run_id,
        )
        raise

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
    if success:
        logger.info(
            "[JOB][MONTHLY] done processed=%s updated=%s elapsed_s=%.2f run_id=%s",
            processed,
            updated,
            total_timer.elapsed_s,
            run_id,
        )
def update_monthly_vip_status(run_id: str | None = None):
    return apply_monthly_tier_update(run_id=run_id)
    
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
    now = datetime.now(timezone.utc)
    existing_user = users_collection.find_one({"user_id": user.id})

    if not existing_user:
        is_new_joiner = True
    else:
        first_seen_at = existing_user.get("first_seen_at")
        if first_seen_at and first_seen_at < XMAS_CAMPAIGN_START:
            is_new_joiner = False
        else:
            is_new_joiner = True

    _users_update_one(
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
        context="xmas_flow",        
    )

    message = (
        "🎄 Xmas Gift Delight\n"
        "Join our official community & do one check-in to enter this week’s Christmas Gift Draw 🎁"
    )

    keyboard = _xmas_keyboard()
    target_message = update.effective_message
    if target_message:
        await safe_reply_text(
            target_message,
            message,
            reply_markup=keyboard,
            uid=user.id,
            send_type="xmas_entry",
            raise_on_non_transient=False,
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    
    # Handle deep links for the Xmas Gift Delight campaign
    if context.args and len(context.args) > 0 and context.args[0].lower() == "xmasgift":
        if user:
            await _send_xmas_flow(update, context, user)
        return

    if user:
        _users_update_one(
            {"user_id": user.id},
            {"$setOnInsert": {
                "username": user.username,
                "last_checkin": None,
                "status": "Normal",
            }},
            upsert=True,
            context="start_user_insert",
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
            await safe_reply_text(
                message,
                "👋 Welcome to AdvantPlay Community!\n\n"
                "Tap below to enter the Mini-App and:\n"
                "• Check-in daily to earn XP\n"
                "• Unlock your referral link\n"
                "• Claim voucher code\n\n"
                "Start your journey here 👇",
                reply_markup=InlineKeyboardMarkup(keyboard),
                uid=user.id,
                send_type="start",
                raise_on_non_transient=False,
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

    now = datetime.now(timezone.utc)
    iso_calendar = now.isocalendar()

    _users_update_one(
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
        context="xmas_checkin",        
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

    chat_id = member.chat.id
    old_status = getattr(member.old_chat_member, "status", None)
    new_status = getattr(member.new_chat_member, "status", None)
    allowed_statuses = {"member", "administrator", "creator"}
    left_group = old_status in allowed_statuses and new_status in ("left", "kicked")
    
    # 只处理 “变成成员” 的事件
    became_member = (new_status in allowed_statuses) and (
        old_status not in allowed_statuses
    )

    user = member.new_chat_member.user
    if not user or user.is_bot:
        return
    if left_group and chat_id == GROUP_ID and isinstance(user.id, int):
        now = now_utc()
        pending_doc = pending_referrals_collection.find_one_and_update(
            {
                "group_id": GROUP_ID,
                "invitee_user_id": user.id,
                "status": "pending",
            },
            {
                "$set": {
                    "status": "revoked",
                    "revoked_reason": "left_before_hold",
                    "revoked_at": now,
                }
            },
            return_document=ReturnDocument.BEFORE,
        )
        if pending_doc:
            logger.info(
                "[REFERRAL][REVOKE] reason=left_before_hold invitee=%s inviter=%s",
                user.id,
                pending_doc.get("inviter_user_id"),
            )
        return
    if not became_member:
        return
        
    if chat_id == GROUP_ID:
        _confirm_referral_on_main_join(
            user.id,
            invitee_username=user.username,
            invite_link=getattr(member, "invite_link", None),
            chat_id=member.chat.id,
        )
    
    # 1) 先记录 join（保持你原本逻辑：哪个 chat 触发就记录哪个 chat）
    try:
        await handle_user_join(
            user.id,
            user.username,
            chat_id,
            source="chat_member",
            invite_link=getattr(member, "invite_link", None),
            old_status=old_status,
            new_status=new_status,
            context=context,
        )
    except Exception:
        logger.exception("[join] chat_member error uid=%s chat_id=%s", user.id, chat_id)

def _is_mywin_message(message) -> bool:
    if not message:
        return False
    text = message.text or message.caption or ""
    if "#mywin" not in text.lower():
        return False
    if message.photo:
        return True
    if message.document:
        return True
    return False

async def mywin_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if chat.id != MYWIN_CHAT_ID:
        return
    if not _is_mywin_message(message):
        return
    record_first_mywin(user.id, chat.id, message.message_id)

async def new_chat_members_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.new_chat_members:
        return
    if message.chat.id == GROUP_ID:
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

        logger.info(
            "[join_request] uid=%s referrer=%s invite_link=%s status=ignored",
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
            _users_update_one(
                {"user_id": user_id},
                {
                    "$set": {"welcome_xp_claimed": True},
                    "$setOnInsert": {
                        "status": "Normal",
                    },
                },
                upsert=True,
                context="welcome_bonus_button",
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
def run_worker():
    try:
        ensure_voucher_indexes()
        print("Voucher indexes ensured.")
    except Exception as e:
        print("Failed to register vouchers blueprint / ensure indexes:", e)
        raise
    set_app_bot(app_bot)
    set_bot(app_bot.bot)
    
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
    app_bot.add_handler(MessageHandler(filters.Chat(MYWIN_CHAT_ID), mywin_message_handler))    
    app_bot.add_handler(CallbackQueryHandler(handle_xmas_checkin, pattern="^xmas_checkin$"))
    app_bot.add_handler(CallbackQueryHandler(button_handler))

    # 4) Scheduler (KL time for human-facing schedules)
    scheduler = BackgroundScheduler(
        timezone=KL_TZ,
        job_defaults={"coalesce": True, "misfire_grace_time": 3600, "max_instances": 1}
    )
    set_scheduler(scheduler)
    def _log_scheduler_event(event) -> None:
        prefix = _job_prefix(event.job_id)
        if event.code == EVENT_JOB_MISSED:
            logger.warning(
                "%s misfire job_id=%s scheduled=%s",
                prefix,
                event.job_id,
                getattr(event, "scheduled_run_time", None),
            )
        elif event.code == EVENT_JOB_ERROR:
            exc = event.exception
            logger.error(
                "%s failed job_id=%s err=%s msg=%s",
                prefix,
                event.job_id,
                exc.__class__.__name__ if exc else None,
                str(exc) if exc else None,
            )
    scheduler.add_listener(_log_scheduler_event, EVENT_JOB_MISSED | EVENT_JOB_ERROR)    
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
    scheduler.add_job(
        tick_5min,
        trigger=CronTrigger(minute="*/5", timezone=KL_TZ),
        id="tick_5min",
        name="Tick 5min (Settlement)",
        replace_existing=True,
    )
    scheduler.add_job(
        process_verification_queue_scheduled,
        trigger=CronTrigger(minute="*/2", timezone=KL_TZ),
        id="process_verification_queue",
        name="Process Verification Queue",
        replace_existing=True,
        kwargs={"batch_limit": 50},
    )    
    scheduler.add_job(
        onboarding_due_tick,
        trigger=CronTrigger(minute="*/1", timezone=KL_TZ),
        id="onboarding_due_tick",
        name="Onboarding Due Tick",
        replace_existing=True,
    )    
    scheduler.start()

    # 5) Background jobs on the bot's job_queue
    app_bot.job_queue.run_once(refresh_admin_ids, when=0)
    app_bot.job_queue.run_repeating(refresh_admin_ids, interval=timedelta(minutes=10), first=timedelta(seconds=0))

    print("✅ Bot & Scheduler wired. Starting servers...")

    try:
        app_bot.run_polling(
            poll_interval=5,
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member", "chat_join_request"]
        )
    finally:
        scheduler.shutdown(wait=False)


def run_web():
    try:
        ensure_voucher_indexes()
        print("Voucher indexes ensured.")
    except Exception as e:
        print("Failed to register vouchers blueprint / ensure indexes:", e)
        raise
    print("[BOOT] web mode: Flask app ready")


if __name__ == "__main__":
    if RUNNER_MODE == "worker":
        run_worker()
    else:
        run_web()
        if not _running_under_gunicorn():
            app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
            
# Test plan (internal):
# 1) Generate referral link for user A.
# 2) User B joins via that link (join request flow) and is approved.
# 3) Verify users.weekly_referrals/total_referrals snapshot updates and xp_events include ref_success:<B> and bonus at 3.
# 4) Ensure rejoin does not double count or double XP.
        
