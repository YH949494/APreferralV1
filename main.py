from flask import (
    Flask, request, jsonify, send_from_directory, make_response,
    render_template, redirect, url_for, flash, g, Blueprint
)
from flask_cors import CORS 
from threading import Thread 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.constants import ChatType, ParseMode
from html import escape as html_escape
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatJoinRequestHandler, ChatMemberHandler,
    CallbackQueryHandler, ContextTypes, MessageHandler, filters
)
from telegram.error import BadRequest, Forbidden, NetworkError
from telegram.request import HTTPXRequest
from datetime import datetime, timedelta, timezone
from werkzeug.exceptions import HTTPException
from urllib.parse import urlencode, quote
from typing import Any

from config import (
    KL_TZ,
    STREAK_MILESTONES,
    XP_BASE_PER_CHECKIN,
    WEEKLY_XP_BUCKET,
    WEEKLY_REFERRAL_BUCKET,
    MINIAPP_VERSION,
    STREAK_FREEZE_MAX_TOKENS,
    GROWTH_LEADERBOARD_ENABLED,
    GROWTH_LEADERBOARD_CHANNEL_ID,
    GROWTH_LEADERBOARD_CRON_DAY,
    GROWTH_LEADERBOARD_CRON_HOUR,
    GROWTH_LEADERBOARD_CRON_MINUTE,
    GROWTH_LEADERBOARD_TIMEZONE,
)
from time_utils import expires_in_seconds, tz_name

from bson.json_util import dumps
from xp import ensure_xp_indexes, grant_xp, now_utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

from app_context import set_app_bot, set_bot, set_scheduler
from onboarding import MYWIN_CHAT_ID, onboarding_due_tick, record_first_mywin, record_first_checkin
from retention_kpis import RETENTION_COLLECTION, compute_retention_kpis, ensure_retention_indexes
from funnel_dashboard import compute_funnel
from vouchers import (
    vouchers_bp,
    ensure_voucher_indexes,
    process_verification_queue,
    check_channel_subscribed,
    extract_raw_init_data_from_query,
    verify_telegram_init_data,
    _get_admin_secret,
    _admin_secret_ok,
    resolve_referral_counts_with_snapshot_fallback,
    welcome_eligibility,
    build_welcome_progress_response,
    get_rejoin_buffer_settings,
    record_welcome_checkin_progress,
)
from admin_auth import admin_auth_bp, configure_admin_session
from referral_rules import calc_referral_progress, REFERRAL_XP_PER_SUCCESS, REFERRAL_BONUS_INTERVAL, REFERRAL_BONUS_XP, build_public_referral_status
from scheduler import settle_pending_referrals, settle_referral_snapshots, settle_xp_snapshots, evaluate_affiliate_simulated_ledgers, compute_affiliate_daily_kpi_yesterday, run_invitee_subscription_audit, reconcile_drop_statuses, post_growth_leaderboard_weekly, process_welcome_voucher_lifecycle, process_welcome_reminders
from affiliate_dashboard_export import run_affiliate_dashboard_export_monthly_scheduled
from referral_rate_limit import consume_referral_rate_limits
from affiliate_leaderboard import (
    should_count_referral_join,
    ensure_affiliate_leaderboard_indexes,
    ensure_affiliate_snapshot_indexes,
    compute_affiliate_weekly_kpis_live,
    compute_affiliate_weekly_kpis_final,
    build_affiliate_leaderboard_snapshot,
    affiliate_previous_completed_week_window_kl,
    affiliate_week_window_from_week_key_kl,
    affiliate_week_window_utc_from_reference,
    serialize_affiliate_snapshot_entries_for_viewer,
    week_window_utc,
)
from affiliate_rewards import (
    ensure_affiliate_indexes,
    issue_welcome_bonus_if_eligible,
    record_user_last_seen,
    settle_previous_month_affiliate_rewards,
    issue_current_week_affiliate_rewards,
    issue_previous_week_affiliate_rewards,
    retry_current_month_pending_manual_ledgers,
    catch_up_missing_current_month_affiliate_ledgers,
)
from telegram_utils import safe_reply_text, safe_send_message
from channel_reactivation import set_campaign_active, campaign_summary as channel_reactivation_summary, process_reactivation_campaign, verify_reactivation_claim, check_official_channel_subscribed, VERIFY_CALLBACK_DATA
from reactivation_journey import ensure_reactivation_journey_indexes, evaluate_pending_journeys, handle_successful_checkin, journey_summary, journey_users, upload_pool_codes, get_journey_config, update_journey_config, compute_journey_status, now_utc as journey_now_utc

from pymongo import DESCENDING, ASCENDING, ReturnDocument  # keep if used elsewhere
from pymongo.errors import DuplicateKeyError, CursorNotFound, OperationFailure, PyMongoError
import os, asyncio, traceback, csv, io, requests, logging, time, uuid, socket, subprocess, hashlib, re
import httpx
import pytz
import json
from database import init_db, db, safe_create_index
import settings_service
from settings_service import get_settings as get_app_settings, get_setting as get_app_setting, update_settings as update_app_settings, list_schema as list_settings_schema, all_settings as get_all_app_settings

FIRST_CHECKIN_BONUS_XP = int(os.getenv("FIRST_CHECKIN_BONUS_XP", "200"))
WELCOME_BONUS_XP = int(os.getenv("WELCOME_BONUS_XP", "20"))
WELCOME_WINDOW_HOURS = int(os.getenv("WELCOME_WINDOW_HOURS", "48"))
WELCOME_WINDOW_DAYS = 7
INVITEE_SUB_AUDIT_HOURS = int(os.getenv("INVITEE_SUB_AUDIT_HOURS", "1"))
AFFILIATE_CURRENT_MONTH_BATCH_LIMIT = int(os.getenv("AFFILIATE_CURRENT_MONTH_BATCH_LIMIT", "500"))
AFFILIATE_PREVIOUS_WEEK_BATCH_LIMIT = int(os.getenv("AFFILIATE_PREVIOUS_WEEK_BATCH_LIMIT", "500"))
AFFILIATE_CURRENT_WEEK_BATCH_LIMIT = int(os.getenv("AFFILIATE_CURRENT_WEEK_BATCH_LIMIT", "500"))
QUERY_TELEMETRY_LOGS = os.getenv("QUERY_TELEMETRY_LOGS", "0") == "1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
INSTANCE_ID = (os.getenv("FLY_MACHINE_ID") or os.getenv("FLY_ALLOC_ID") or f"{socket.gethostname()}:{os.getpid()}")

LEADERBOARD_CACHE = {}  # key -> {"ts": epoch_seconds, "payload": dict}
CACHE_TTL_SECONDS = 300
REGION_IP_CACHE = {}  # ip -> {"ts": epoch_seconds, "region": str|None, "source": str}
REGION_IP_CACHE_TTL_SECONDS = 3600
DAILY_GAME_SLOTS = [
    {"id": "dragon_chi_s_quest_2", "name": "Dragon Chi's Quest 2", "tag": "Med", "maxwin": "100000x", "weight": 2},
    {"id": "piggy_bank_gold_2", "name": "Piggy Bank Gold 2", "tag": "High-Med", "maxwin": "150000x"},
    {"id": "zeustrike_xmas", "name": "Zeustrike Xmas", "tag": "High", "maxwin": "30000x"},
    {"id": "aztec_bonus_hunt_2_xmas", "name": "Aztec: Bonus Hunt 2 Xmas", "tag": "High-Med", "maxwin": "12000x"},
    {"id": "zeustrike", "name": "Zeustrike", "tag": "High", "maxwin": "30000x", "weight": 2},
    {"id": "fighting_bull", "name": "Fighting Bull", "tag": "Med", "maxwin": "8000x"},
    {"id": "cat_mouse", "name": "Cat & Mouse", "tag": "High-Med", "maxwin": "5000x"},
    {"id": "pinata_fest", "name": "Pinata Fest", "tag": "Med", "maxwin": "80000x"},
    {"id": "buffalo_rush_highroller", "name": "Buffalo Rush HIGHROLLER", "tag": "High", "maxwin": "15120x"},
    {"id": "golden_egypt", "name": "Golden Egypt", "tag": "Med", "maxwin": "6000x"},
    {"id": "mahjong_roar", "name": "Mahjong Roar", "tag": "Med", "maxwin": "2500x"},
    {"id": "maya_elemental_totem_2", "name": "Maya: Elemental Totem 2", "tag": "High-Med", "maxwin": "2500x"},
    {"id": "big_net_bass", "name": "Big Net Bass", "tag": "Med", "maxwin": "16000x"},
    {"id": "sugar_crush", "name": "Sugar Crush", "tag": "Med", "maxwin": "20000x"},
    {"id": "disco_777_hold_and_win", "name": "Disco 777 Hold and Win", "tag": "High-Med", "maxwin": "512000x"},
    {"id": "piggy_bank_gold", "name": "Piggy Bank Gold", "tag": "Med", "maxwin": "30000x"},
    {"id": "leprechaun_s_fortune", "name": "Leprechaun's Fortune", "tag": "Med", "maxwin": "28500x"},
    {"id": "blackjack_21", "name": "BlackJack 21", "tag": "Low-Med", "maxwin": "100000x"},
    {"id": "pirate_treasure_hunt", "name": "Pirate Treasure Hunt", "tag": "Low-Med", "maxwin": "1500x"},
    {"id": "aztec_gold_temple", "name": "Aztec: Gold Temple", "tag": "Med", "maxwin": "10000x"},
    {"id": "cai_shen_fortune", "name": "Cai Shen Fortune", "tag": "High-Med", "maxwin": "8262x"},
    {"id": "crazy_bounty_jackpot", "name": "Crazy Bounty: Jackpot", "tag": "High-Med", "maxwin": "50000x"},
    {"id": "rush_hour_gold", "name": "Rush Hour Gold", "tag": "Med", "maxwin": "1500x"},
    {"id": "buffalo_rush", "name": "Buffalo Rush", "tag": "Med", "maxwin": "4915x"},
    {"id": "jumanji_bonanza", "name": "Jumanji Bonanza", "tag": "Low", "maxwin": "150x"},
    {"id": "phantom_multiplier", "name": "Phantom Multiplier", "tag": "High-Med", "maxwin": "120000x"},
    {"id": "starry_adventure", "name": "Starry Adventure", "tag": "Low-Med", "maxwin": "25000x"},
    {"id": "rhapsody_of_muertos", "name": "Rhapsody of Muertos", "tag": "High-Med", "maxwin": "250000x"},
    {"id": "kingyo_riches", "name": "Kingyo Riches", "tag": "High-Med", "maxwin": "18600x"},
    {"id": "fish_prawn_crab_bonanza", "name": "Fish Prawn Crab Bonanza", "tag": "High-Med", "maxwin": "20000x"},
    {"id": "ramakien_blessing", "name": "Ramakien Blessing", "tag": "Med", "maxwin": "100x"},
    {"id": "aztec_bonus_hunt_2", "name": "Aztec: Bonus Hunt 2", "tag": "High-Med", "maxwin": "12000x", "weight": 2},
    {"id": "football_fever", "name": "Football Fever", "tag": "High", "maxwin": "70000x"},
    {"id": "firefly_hunter", "name": "Firefly Hunter", "tag": "High-Med", "maxwin": "4027x"},
    {"id": "dark_ritual", "name": "Dark Ritual", "tag": "High", "maxwin": "20000x"},
    {"id": "hungry_slime", "name": "Hungry Slime", "tag": "High-Med", "maxwin": "50000x"},
    {"id": "crazy_bounty", "name": "Crazy Bounty", "tag": "Med", "maxwin": "10000x"},
    {"id": "maya_elemental_totem", "name": "Maya: Elemental Totem", "tag": "Med", "maxwin": "1180x"},
    {"id": "dragon_chi_s_quest", "name": "Dragon Chi's Quest", "tag": "Med", "maxwin": "80000x"},
    {"id": "xmas_gift_delight", "name": "Xmas Gift Delight", "tag": "Med", "maxwin": "20000x"},
    {"id": "cookie_hunter", "name": "Cookie Hunter", "tag": "Low-Med", "maxwin": "268x"},
    {"id": "xiang_qi_ways_2", "name": "Xiang Qi Ways 2", "tag": "Med", "maxwin": "2500x"},
    {"id": "dj_fever", "name": "DJ Fever", "tag": "Med", "maxwin": "5000x"},
    {"id": "mace_of_hercules", "name": "Mace of Hercules", "tag": "High-Med", "maxwin": "16128x"},
    {"id": "jewel_mastermind", "name": "Jewel Mastermind", "tag": "Med", "maxwin": "162x"},
    {"id": "last_samurai", "name": "Last Samurai", "tag": "High-Med", "maxwin": "15000x"},
    {"id": "scale_of_heaven_anubis", "name": "Scale of Heaven: Anubis", "tag": "High-Med", "maxwin": "1000x"},
    {"id": "infinity_ocean", "name": "Infinity Ocean", "tag": "High-Med", "maxwin": "250000x"},
    {"id": "fantastic_beast", "name": "Fantastic Beast", "tag": "Med", "maxwin": "1200x"},
    {"id": "aztec_bonus_hunt", "name": "Aztec: Bonus Hunt", "tag": "Med", "maxwin": "800x"},
    {"id": "bunny_to_the_moon", "name": "Bunny to the Moon", "tag": "Med", "maxwin": "1100x"},
    {"id": "genie_mystery", "name": "Genie Mystery", "tag": "High", "maxwin": "15000x"},
    {"id": "boom_of_prosperity", "name": "Boom of Prosperity", "tag": "Med", "maxwin": "730x"},
    {"id": "slotto_4d", "name": "Slotto 4D", "tag": "Med", "maxwin": "10050x"},
    {"id": "world_cup_final", "name": "World Cup Final", "tag": "Med", "maxwin": "1180x"},
    {"id": "disco_777", "name": "Disco 777", "tag": "Med", "maxwin": "28500x"},
]

def build_daily_game_pool(slots):
    pool = []
    for slot in (slots or []):
        weight = slot.get("weight", 1)
        if not isinstance(weight, int) or weight <= 0:
            weight = 1
        for _ in range(weight):
            pool.append(slot)
    return pool if pool else list(slots or [])

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


def _extract_client_ip(req):
    fly_ip = (req.headers.get("Fly-Client-IP") or "").strip()
    if fly_ip:
        return fly_ip, "fly-client-ip"

    xff = (req.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        first_ip = xff.split(",", 1)[0].strip()
        if first_ip:
            return first_ip, "x-forwarded-for"

    remote_ip = (req.remote_addr or "").strip()
    if remote_ip:
        return remote_ip, "remote-addr"

    return None, "unknown"


def _map_country_to_region(country_code):
    code = (country_code or "").strip().upper()
    if code == "MY":
        return "Malaysia"
    if code == "TH":
        return "Thailand"
    if code == "ID":
        return "Indonesia"
    if code:
        return "Other"
    return None


def _get_region_from_ip(ip):
    if not ip:
        return None, "no-ip"

    now_ts = time.time()
    cached = REGION_IP_CACHE.get(ip)
    if cached and (now_ts - cached.get("ts", 0) < REGION_IP_CACHE_TTL_SECONDS):
        return cached.get("region"), f"cache:{cached.get('source', 'ipapi')}"

    region = None
    source = "ipapi"
    try:
        res = requests.get(f"https://ipapi.co/{ip}/json/", timeout=1.5)
        if res.ok:
            payload = res.json() or {}
            region = _map_country_to_region(payload.get("country_code"))
        else:
            source = f"ipapi-http-{res.status_code}"
    except Exception:
        source = "ipapi-error"

    REGION_IP_CACHE[ip] = {"ts": now_ts, "region": region, "source": source}
    return region, source

def _is_private_chat(update):
    chat = getattr(update, "effective_chat", None)
    return bool(chat and chat.type == ChatType.PRIVATE)


def _ensure_index_if_missing(col, name, keys, **kwargs):
    """Create index only when missing by name; safe for concurrent startup calls."""
    try:
        for idx in col.list_indexes():
            if idx.get("name") == name:
                return name
    except PyMongoError:
        raise

    try:
        return col.create_index(keys, name=name, **kwargs)
    except OperationFailure as exc:
        # MongoDB may raise index conflict/exists codes during concurrent startup:
        # 68=IndexAlreadyExists, 85=IndexOptionsConflict, 86=IndexKeySpecsConflict.
        if exc.code in (68, 85, 86) or "already exists" in str(exc).lower():
            return name
        raise

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
REFERRAL_WEBAPP_URL = f"{WEBAPP_URL}&action=generate_referral"
OFFICIAL_CHANNEL_URL = "https://t.me/+Zy3UGGkE17kyNDA9"
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ----------------------------
# Channel config
# ----------------------------
CHANNEL_USERNAME = "@advantplayofficial"
# GROUP_ID / OFFICIAL_CHANNEL_ID are resolved once in referral_destination.py so
# main.py and scheduler.py can never disagree on chat identity. CHANNEL_ID is
# kept as an alias for backward compatibility with any external references.
from referral_destination import (
    COMMUNITY_GROUP_ID as GROUP_ID,
    OFFICIAL_CHANNEL_ID,
    get_referral_destination,
)
CHANNEL_ID = OFFICIAL_CHANNEL_ID

# Rejoin buffer applied to public/pooled voucher claims after a user leaves and
# rejoins the official channel. The buffer duration is admin-editable (see
# vouchers.get_rejoin_buffer_settings / the "Rejoin Buffer" admin dashboard
# control); this is only the fallback used if that lookup fails.
REJOIN_CLAIM_BUFFER_HOURS_FALLBACK = 12.0

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

IDENTITY_TIERS = [
    {"name": "Legend", "icon": "🐉", "min_referrals": 100, "min_xp": 100000},
    {"name": "Elite", "icon": "👑", "min_referrals": 50, "min_xp": 40000},
    {"name": "Captain", "icon": "⚔️", "min_referrals": 20, "min_xp": 15000},
    {"name": "Silver", "icon": "🥈", "min_referrals": 5, "min_xp": 5000},
    {"name": "Bronze", "icon": "🥉", "min_referrals": 1, "min_xp": 1500},
    {"name": "Rookie", "icon": "🌱", "min_referrals": 0, "min_xp": 0},
]


def _safe_non_negative_int(value) -> int:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return 0
    if n != n or n in (float("inf"), float("-inf")):
        return 0
    try:
        return max(0, int(n))
    except OverflowError:
        return 0


def derive_identity_tier(total_referrals: int, total_xp: int) -> dict:
    refs = _safe_non_negative_int(total_referrals)
    xp_total = _safe_non_negative_int(total_xp)
    for tier in IDENTITY_TIERS:
        if refs >= tier["min_referrals"] or xp_total >= tier["min_xp"]:
            return tier
    return IDENTITY_TIERS[-1]


def compute_next_tier_progress(total_referrals: int, total_xp: int, current_tier_name: str) -> tuple[str | None, int, str]:
    ordered = list(reversed(IDENTITY_TIERS))
    idx = next((i for i, t in enumerate(ordered) if t["name"] == current_tier_name), len(ordered) - 1)
    if idx >= len(ordered) - 1:
        return None, 100, "Top tier unlocked"

    next_tier = ordered[idx + 1]
    refs = _safe_non_negative_int(total_referrals)
    xp_total = _safe_non_negative_int(total_xp)
    ref_pct = int(min(100, (refs / max(1, next_tier["min_referrals"])) * 100))
    xp_pct = int(min(100, (xp_total / max(1, next_tier["min_xp"])) * 100))
    progress_pct = max(ref_pct, xp_pct)
    need_refs = max(0, next_tier["min_referrals"] - refs)
    need_xp = max(0, next_tier["min_xp"] - xp_total)
    hint = f"{need_refs:,} more referrals or {need_xp:,} XP to {next_tier['name']}"
    return next_tier["name"], progress_pct, hint


def compute_weekly_rank(user_id: int, weekly_xp: int, weekly_referrals: int, updated_at) -> int | None:
    wxp = _safe_non_negative_int(weekly_xp)
    wref = _safe_non_negative_int(weekly_referrals)
    if wxp <= 0 and wref <= 0:
        return None
    if users_collection.find_one({"user_id": user_id}, {"_id": 1}) is None:
        return None
    ts = updated_at if isinstance(updated_at, datetime) else datetime.min
    higher_count = users_collection.count_documents(
        {
            "$or": [
                {"weekly_xp": {"$gt": wxp}},
                {"weekly_xp": wxp, "weekly_referrals": {"$gt": wref}},
                {"weekly_xp": wxp, "weekly_referrals": wref, "updated_at": {"$gt": ts}},
            ]
        }
    )
    return int(higher_count) + 1


def _extract_verified_telegram_user_id() -> tuple[int | None, tuple[dict, int] | None]:
    init_data = extract_raw_init_data_from_query(request)
    if not init_data:
        return None, ({"ok": False, "error": "Missing init_data"}, 400)

    ok, parsed, _ = verify_telegram_init_data(init_data)
    if not ok:
        return None, ({"ok": False, "error": "Unauthorized"}, 403)

    user_payload = (parsed or {}).get("user", {})
    if isinstance(user_payload, str):
        try:
            user_payload = json.loads(user_payload)
        except Exception:
            user_payload = {}

    try:
        user_id = int((user_payload or {}).get("id"))
    except (TypeError, ValueError):
        user_id = None

    if not user_id:
        return None, ({"ok": False, "error": "Unauthorized"}, 403)
    return user_id, None


def choose_share_rank_achievement(rank, weekly_xp: int, streak: int, total_referrals: int) -> tuple[str, str]:
    rank_value = _safe_non_negative_int(rank) if rank is not None else 0
    weekly_xp_value = _safe_non_negative_int(weekly_xp)
    streak_value = _safe_non_negative_int(streak)
    total_referrals_value = _safe_non_negative_int(total_referrals)

    if rank_value == 1:
        return "Leader of the Pack", "Currently Ranked #1"
    if 0 < rank_value <= 3:
        return "Podium Holder", "Top 3 This Week"
    if 0 < rank_value <= 10:
        return "Elite Player", "Top 10 This Week"
    if streak_value >= 56:
        return "Iron Will", "56-Day Check-in Streak"
    if streak_value >= 28:
        return "Unstoppable", "28-Day Check-in Streak"
    if streak_value >= 14:
        return "Consistent Challenger", "14-Day Check-in Streak"
    if streak_value >= 7:
        return "Hot Streak", "7-Day Check-in Streak"
    if total_referrals_value >= 10:
        return "Community Builder", "10 Successful Referrals"
    if total_referrals_value >= 3:
        return "Referral Machine", "3 Successful Referrals"
    if weekly_xp_value >= 1000:
        return "Momentum Builder", "Earned 1000+ XP This Week"
    if weekly_xp_value >= 500:
        return "XP Hunter", "Earned 500+ XP This Week"
    return "Rising Star", "Climbing the Leaderboard"


def build_share_rank_caption(rank, weekly_xp: int, title: str, highlight: str) -> str:
    rank_line = f"🏆 Currently Ranked #{int(rank)}" if rank is not None else "🏆 Not Ranked Yet"
    weekly_xp_value = _safe_non_negative_int(weekly_xp)
    return (
        "My current rank 👇\n\n"
        f"{rank_line}\n\n"
        f"⚡ Weekly XP: {weekly_xp_value}\n"
        f"✨ {highlight}\n\n"
        "🎁 Subscribe to @AdvantPlayOfficial for Voucher Drops"
    )


def _load_share_rank_user_snapshot(user_id: int) -> dict | None:
    return users_collection.find_one(
        {"user_id": user_id},
        {
            "user_id": 1,
            "weekly_xp": 1,
            "weekly_referrals": 1,
            "total_referrals": 1,
            "streak": 1,
            "streak_days": 1,
            "checkin_streak": 1,
            "updated_at": 1,
        },
    )


def _compute_share_rank(user_id: int, user_doc: dict) -> int | None:
    weekly_xp = _safe_non_negative_int((user_doc or {}).get("weekly_xp", 0))
    weekly_referrals = _safe_non_negative_int((user_doc or {}).get("weekly_referrals", 0))
    try:
        return compute_weekly_rank(user_id, weekly_xp, weekly_referrals, (user_doc or {}).get("updated_at"))
    except PyMongoError:
        raise
    except Exception:
        logger.exception("[SHARE_RANK][FAIL] stage=rank_calculation uid=%s", user_id)
        return None

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



def bot_segment_sheet_sync_scheduled() -> None:
    acquired, lock_doc = acquire_scheduler_lock("bot_segment_sheet_sync", ttl_seconds=1800)
    if not acquired:
        logger.info(
            "[BOT_SEGMENT_SYNC] lock_not_acquired owner=%s expires_in_s=%s",
            (lock_doc or {}).get("owner"),
            expires_in_seconds((lock_doc or {}).get("expireAt")),
        )
        return
    logger.info("[BOT_SEGMENT_SYNC] start")
    try:
        from bot_segment_sync import sync_bot_segments_from_sheet

        summary = sync_bot_segments_from_sheet(dry_run=False)
        if summary.get("ok"):
            logger.info("[BOT_SEGMENT_SYNC] done summary=%s", summary)
        else:
            logger.error(
                "[BOT_SEGMENT_SYNC] failed err=%s summary=%s",
                summary.get("error"),
                summary,
            )
    except Exception as exc:
        logger.exception("[BOT_SEGMENT_SYNC] failed err=%s", str(exc))


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
                logger.info("[JOB][5MIN] progress step=retention_kpis_daily run_id=%s", run_id)
                try:
                    now_utc = datetime.now(timezone.utc)
                    today_key = now_utc.strftime("%Y-%m-%d")
                    cache_key = "retention_kpis:last_daily_compute"
                    cache_doc = admin_cache_col.find_one({"_id": cache_key}, {"day": 1}) or {}
                    if cache_doc.get("day") != today_key:
                        compute_retention_kpis(db, months=12, now_utc=now_utc)
                        admin_cache_col.update_one(
                            {"_id": cache_key},
                            {"$set": {"day": today_key, "computed_at_utc": now_utc}},
                            upsert=True,
                        )
                except Exception as exc:
                    logger.exception("[JOB][5MIN] step_error name=retention_kpis_daily run_id=%s err=%s", run_id, exc)
            logger.info(
                "[JOB][5MIN] step_done name=retention_kpis_daily elapsed_s=%.2f run_id=%s",
                step_timer.elapsed_s,
                run_id,
            )

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

            with JobTimer() as step_timer:
                logger.info(
                    "[JOB][5MIN] progress step=affiliate_snapshot_check run_id=%s",
                    run_id,
                )
                try:
                    build_affiliate_leaderboard_snapshot(
                        db,
                        mode="scheduler",
                        force=False,
                        user_identity_loader=_affiliate_user_identity_map,
                    )
                except Exception as exc:
                    target = affiliate_previous_completed_week_window_kl().get("week_key")
                    logger.exception("[AFF_SNAPSHOT][ERROR] week_key=%s err=%s", target, exc)
            logger.info(
                "[JOB][5MIN] step_done name=affiliate_snapshot_check elapsed_s=%.2f run_id=%s",
                step_timer.elapsed_s,
                run_id,
            )

            with JobTimer() as step_timer:
                logger.info(
                    "[JOB][5MIN] progress step=retry_pending_manual_vouchers run_id=%s",
                    run_id,
                )
                try:
                    if str(os.getenv("AFFILIATE_SIMULATE", "0")).strip() != "1":
                        catch_up_missing_current_month_affiliate_ledgers(
                            db,
                            now_utc=datetime.now(timezone.utc),
                            batch_limit=AFFILIATE_CURRENT_MONTH_BATCH_LIMIT,
                        )
                        retry_current_month_pending_manual_ledgers(
                            db,
                            now_utc=datetime.now(timezone.utc),
                            batch_limit=AFFILIATE_CURRENT_MONTH_BATCH_LIMIT,
                        )
                except Exception as exc:
                    logger.exception("[JOB][5MIN] step_error name=retry_pending_manual_vouchers run_id=%s err=%s", run_id, exc)
            logger.info(
                "[JOB][5MIN] step_done name=retry_pending_manual_vouchers elapsed_s=%.2f run_id=%s",
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


def affiliate_monthly_settle_scheduled() -> None:
    acquired, lock_doc = acquire_scheduler_lock("affiliate_monthly_settle", ttl_seconds=1800)
    if not acquired:
        logger.info(
            "[JOB][AFFILIATE_MONTHLY] lock_not_acquired owner=%s expires_in_s=%s instance=%s",
            (lock_doc or {}).get("owner"),
            expires_in_seconds((lock_doc or {}).get("expireAt")),
            INSTANCE_ID,
        )
        return
    settle_previous_month_affiliate_rewards(db, now_utc=datetime.now(timezone.utc), batch_limit=1000)


def affiliate_weekly_settle_scheduled() -> None:
    acquired, lock_doc = acquire_scheduler_lock("affiliate_weekly_settle", ttl_seconds=1800)
    if not acquired:
        logger.info(
            "[JOB][AFFILIATE_WEEKLY] lock_not_acquired owner=%s expires_in_s=%s instance=%s",
            (lock_doc or {}).get("owner"),
            expires_in_seconds((lock_doc or {}).get("expireAt")),
            INSTANCE_ID,
        )
        return
    result = issue_previous_week_affiliate_rewards(
        db,
        now_utc=datetime.now(timezone.utc),
        batch_limit=AFFILIATE_PREVIOUS_WEEK_BATCH_LIMIT,
    )
    logger.info("[JOB][AFFILIATE_WEEKLY] done result=%s", result)


def affiliate_current_week_issue_scheduled() -> None:
    acquired, lock_doc = acquire_scheduler_lock("affiliate_current_week_issue", ttl_seconds=1500)
    if not acquired:
        logger.info(
            "[JOB][AFFILIATE_CURRENT_WEEK] lock_not_acquired owner=%s expires_in_s=%s instance=%s",
            (lock_doc or {}).get("owner"),
            expires_in_seconds((lock_doc or {}).get("expireAt")),
            INSTANCE_ID,
        )
        return
    result = issue_current_week_affiliate_rewards(
        db,
        now_utc=datetime.now(timezone.utc),
        batch_limit=AFFILIATE_CURRENT_WEEK_BATCH_LIMIT,
    )
    logger.info("[JOB][AFFILIATE_CURRENT_WEEK] done result=%s", result)


def process_verification_queue_scheduled(batch_limit: int | None = None) -> None:
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


def _record_welcome_run_stats(job_name: str, stats: dict, duration_s: float, now: datetime) -> None:
    """Persist the per-run stats dict so the Welcome Journey Runtime dashboard
    has real numbers instead of just a heartbeat timestamp. Written to
    ``admin_cache`` (not ``scheduler_locks``) because ``scheduler_locks`` has
    a TTL index on ``expireAt`` (main.py's ``create_index(..., expireAfterSeconds=0)``)
    and ``acquire_scheduler_lock`` lets that field pass once a job stops
    running — Mongo would then delete the whole lock doc, wiping this history
    right when the dashboard most needs to show it. Purely additive
    observability; never touches reminder/voucher logic."""
    doc_id = f"welcome_run_stats:{job_name}"
    duration_s = round(duration_s, 3)
    # The heartbeat ($set) and history append ($push) are two separate
    # update_one calls on purpose: if they were combined into one update
    # document and $push ever hit a doc where recentRuns had drifted to a
    # non-array shape (stale/legacy data), Mongo rejects the *whole* update
    # atomically — silently freezing lastRunAt/status even though this run
    # actually completed, since the failure is only visible in a log line.
    # Writing the heartbeat first, on its own, means a corrupt history array
    # can never take the dashboard offline.
    try:
        admin_cache_col.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "lastRunStats": stats,
                    "lastRunAt": now,
                    "updatedAt": now,
                    "lastRunDurationS": duration_s,
                    "status": stats.get("status") or "ok",
                    "run_id": stats.get("run_id"),
                },
            },
            upsert=True,
        )
    except Exception:
        logger.exception("[WELCOME_RUNTIME] failed to persist heartbeat job=%s", job_name)
        return
    try:
        run_record = {"at": now, "duration_s": duration_s, "stats": stats}
        admin_cache_col.update_one(
            {"_id": doc_id},
            {"$push": {"recentRuns": {"$each": [run_record], "$slice": -20}}},
        )
    except Exception:
        logger.exception("[WELCOME_RUNTIME] failed to append recentRuns job=%s", job_name)


def welcome_voucher_lifecycle_scheduled(**kwargs) -> None:
    acquired, _lock_doc = acquire_scheduler_lock("welcome_voucher_lifecycle", ttl_seconds=1800)
    if not acquired:
        logger.info("[SCHEDULER][WELCOME_LIFECYCLE] lock_not_acquired")
        return
    with JobTimer() as timer:
        stats = process_welcome_voucher_lifecycle(**kwargs)
    _record_welcome_run_stats("welcome_voucher_lifecycle", stats or {}, timer.elapsed_s, datetime.now(timezone.utc))


def welcome_progress_reminders_scheduled(**kwargs) -> None:
    acquired, _lock_doc = acquire_scheduler_lock("welcome_progress_reminders", ttl_seconds=3600)
    if not acquired:
        logger.info("[SCHEDULER][WELCOME_PROGRESS] lock_not_acquired")
        return
    with JobTimer() as timer:
        stats = process_welcome_reminders(**kwargs)
    _record_welcome_run_stats("welcome_progress_reminders", stats or {}, timer.elapsed_s, datetime.now(timezone.utc))

# ----------------------------
# MongoDB Setup
# ----------------------------
init_db(MONGO_URL)
users_collection = db["users"]
segment_snapshots_collection = db["segment_snapshots"]
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
referral_rate_limits_collection = db["referral_rate_limits"]
referral_notifications_collection = db["referral_notifications"]
qualified_events_collection = db["qualified_events"]
affiliate_ledger_collection = db["affiliate_ledger"]
voucher_pools_collection = db["voucher_pools"]
tg_verification_queue_collection = db["tg_verification_queue"]
scheduler_locks_collection = db["scheduler_locks"]
try:
    scheduler_locks_collection.create_index([("expireAt", ASCENDING)], expireAfterSeconds=0)
except Exception:
    logger.warning("[SCHEDULER][LOCK] failed to create TTL index", exc_info=True)
ensure_retention_indexes(db)
    
REFERRAL_HOLD_HOURS = int(os.getenv("REFERRAL_QUALIFY_HOURS", os.getenv("REFERRAL_HOLD_HOURS", "48")))
REFERRAL_HOURLY_LIMIT = int(os.getenv("REFERRAL_HOURLY_LIMIT", "20"))
REFERRAL_DAILY_LIMIT = int(os.getenv("REFERRAL_DAILY_LIMIT", "200"))


def _referral_hold_hours() -> int:
    try:
        value = get_app_setting("referral_config", "qualify_hold_hours")
        return int(value) if value is not None else REFERRAL_HOLD_HOURS
    except Exception:
        return REFERRAL_HOLD_HOURS

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
    pref_allowed = pm_allowed(
        inviter_user_id,
        "referral_updates",
        default=True,
        users_collection=users_collection,
        logger=logger,
    )
    if not pref_allowed:
        logger.info(
            "[PM_PREF][SUPPRESSED] uid=%s key=%s type=%s",
            inviter_user_id,
            "referral_updates",
            "ref_near_miss",
        )
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

def _maybe_send_referral_join_ack_dm(
    inviter_user_id: int | None,
    invitee_user_id: int | None,
    invitee_username: str | None = None,
) -> None:
    if RUNNER_MODE != "web":
        return
    if not inviter_user_id or not invitee_user_id:
        return
    dedupe_key = f"ref_join_ack:{int(inviter_user_id)}:{int(invitee_user_id)}"
    now_ts = now_utc()
    pref_allowed = pm_allowed(
        int(inviter_user_id),
        "referral_updates",
        default=True,
        users_collection=users_collection,
        logger=logger,
    )
    set_on_insert = {
        "key": dedupe_key,
        "type": "ref_join_ack",
        "inviter_user_id": int(inviter_user_id),
        "invitee_user_id": int(invitee_user_id),
        "invitee_username": invitee_username,
        "created_at": now_ts,
    }
    if not pref_allowed:
        set_on_insert["suppressed"] = True
        set_on_insert["suppressed_reason"] = "pm_preference"
    result = referral_notifications_collection.update_one(
        {"key": dedupe_key},
        {
            "$setOnInsert": set_on_insert
        },
        upsert=True,
    )
    if not getattr(result, "upserted_id", None):
        return
    if not pref_allowed:
        logger.info(
            "[PM_PREF][SUPPRESSED] uid=%s key=%s type=%s",
            inviter_user_id,
            "referral_updates",
            "ref_join_ack",
        )
        return
    invitee_label = f"@{invitee_username}" if invitee_username else "Someone"
    text = (
        f"🎉 {invitee_label} joined using your invite link!\n"
        "They’re now being checked. Qualified referrals count after the hold period."
    )
    try:
        call_bot_in_loop(app_bot.bot.send_message(chat_id=int(inviter_user_id), text=text))
    except (Forbidden, BadRequest) as exc:
        logger.warning(
            "[REFERRAL_ACK_DM_FAILED] inviter=%s invitee=%s err=%s",
            inviter_user_id,
            invitee_user_id,
            exc,
        )
    except Exception:
        logger.exception(
            "[REFERRAL_ACK_DM_FAILED] inviter=%s invitee=%s",
            inviter_user_id,
            invitee_user_id,
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
        existing = welcome_eligibility_collection.find_one({"$or": [{"uid": uid}, {"user_id": uid}]}, {"_id": 1, "uid": 1, "user_id": 1})
        if existing:
            welcome_eligibility_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": {"uid": uid, "user_id": uid}},
            )
            if existing.get("uid") != uid or existing.get("user_id") != uid:
                logger.info("[WELCOME][ELIGIBILITY] normalized uid=%s doc_id=%s", uid, existing.get("_id"))
        else:
            welcome_eligibility_collection.insert_one(
                {
                    "uid": uid,
                    "user_id": uid,
                    "created_at": now,
                    "joined_main_at": joined_main_at or now,
                    "source": "main_join",
                }
            )
    except DuplicateKeyError:
        existing = welcome_eligibility_collection.find_one({"$or": [{"uid": uid}, {"user_id": uid}]}, {"_id": 1})
        if existing:
            welcome_eligibility_collection.update_one({"_id": existing["_id"]}, {"$set": {"uid": uid, "user_id": uid}})
            logger.info("[WELCOME][ELIGIBILITY] tolerant_lookup_matched_legacy uid=%s doc_id=%s", uid, existing.get("_id"))
        else:
            logger.info("[WELCOME][ELIGIBILITY] dup uid=%s (already inserted)", uid)
            return None
    except Exception:
        logger.exception("[WELCOME][ELIGIBILITY] write_failed uid=%s", uid)
        return None
    return welcome_eligibility_collection.find_one({"$or": [{"uid": uid}, {"user_id": uid}]})

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
    try:
        blocked = (users_collection.find_one({"user_id": uid}, {"blocked": 1}) or {}).get("blocked", False)
        wb_result = issue_welcome_bonus_if_eligible(db, user_id=uid, is_new_user=True, blocked=bool(blocked))
        logger.info("[WELCOME] bonus_issued uid=%s result=%s", uid, wb_result)
    except Exception:
        logger.exception("[WELCOME] bonus_issue_failed uid=%s", uid)
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
    # Local imports (not module globals) so this function stays fully
    # self-contained: it is exercised standalone (isolated exec of just this
    # function's AST) by test_main_referral_error.py, which does not provide
    # these names in its fake globals dict.
    from referral_destination import destination_type_for_chat_id
    import referral_invitee_lock

    event_chat_id = chat_id or GROUP_ID
    destination_type = destination_type_for_chat_id(event_chat_id)

    if not isinstance(invitee_user_id, int):
        logger.info(
            "[REFERRAL][SKIP] reason=invalid_uid invitee=%s chat_id=%s destination_type=%s",
            invitee_user_id,
            event_chat_id,
            destination_type,
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
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=None,
        )
        logger.info(
            "[REFERRAL][NO_INVITE_LINK] invitee=%s chat_id=%s destination_type=%s",
            invitee_user_id,
            event_chat_id,
            destination_type,
        )
        return

    # Exact lookup preserved: invite_link + event chat_id + is_active != False.
    # chat_id comes straight off the Telegram event, so this naturally works
    # for both community-group and official-channel joins without change.
    mapping = invite_link_map_collection.find_one(
        {
            "invite_link": invite_link_url,
            "chat_id": event_chat_id,
            "is_active": {"$ne": False},
        },
        {"inviter_id": 1},
    )
    referrer_id = (mapping or {}).get("inviter_id")
    logger.info(
        "[REFERRAL][LINK_RESOLVED] invitee=%s chat_id=%s destination_type=%s invite_link=%s resolved=%s",
        invitee_user_id,
        event_chat_id,
        destination_type,
        invite_link_log,
        bool(referrer_id),
    )
    if not referrer_id:
        _write_referral_audit(
            status="skipped",
            reason="unknown_invite_link",
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
        )
        try:
            unknown_invite_audit_collection.insert_one(
                {
                    "ts_utc": datetime.now(timezone.utc),
                    "chat_id": event_chat_id,
                    "destination_type": destination_type,
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
            "[REFERRAL][LINK_UNKNOWN] reason=unknown_invite_link invitee=%s chat_id=%s destination_type=%s invite_link=%s",
            invitee_user_id,
            event_chat_id,
            destination_type,
            invite_link_log,
        )
        return

    if referrer_id == invitee_user_id:
        _write_referral_audit(
            status="skipped",
            reason="self_invite",
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
        )
        logger.info(
            "[REFERRAL][SKIP] reason=self_invite inviter=%s invitee=%s chat_id=%s destination_type=%s",
            referrer_id,
            invitee_user_id,
            event_chat_id,
            destination_type,
        )
        return

    limiter_now_utc = now_utc()
    try:
        allowed, blocked_reason, limit_meta = consume_referral_rate_limits(
            referral_rate_limits_collection,
            inviter_id=int(referrer_id),
            now_utc=limiter_now_utc,
            hourly_limit=REFERRAL_HOURLY_LIMIT,
            daily_limit=REFERRAL_DAILY_LIMIT,
        )
    except Exception:
        logger.exception(
            "[REFERRAL][ERROR] step=rate_limit inviter=%s invitee=%s",
            referrer_id,
            invitee_user_id,
        )
        allowed, blocked_reason, limit_meta = True, None, {}

    if not allowed:
        _write_referral_audit(
            status="skipped",
            reason=blocked_reason,
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
        )
        logger.info(
            "[REFERRAL][RATE_LIMIT] inviter=%s key=%s count=%s limit=%s",
            referrer_id,
            limit_meta.get("key"),
            limit_meta.get("count"),
            limit_meta.get("limit"),
        )
        return

    # Historical-success guard: block a new referral outright if this
    # invitee has ANY prior successful-referral evidence, across every
    # collection/key format that has ever recorded one (qualified_events,
    # settled referral_events, structured/legacy/new referral_award_events
    # keys) — not just qualified_events. This catches invitees who were
    # qualified/settled/awarded before the referral_invitee_locks
    # collection existed, so they have no lock row for claim() to see.
    created_at_utc = limiter_now_utc
    try:
        historical_success = referral_invitee_lock.has_historical_success(
            db, invitee_user_id=invitee_user_id
        )
    except Exception:
        # Fail-open: a lookup outage must not block all referral attribution.
        logger.exception(
            "[REFERRAL][ERROR] step=historical_success_guard inviter=%s invitee=%s",
            referrer_id,
            invitee_user_id,
        )
        historical_success = False
    if historical_success:
        _write_referral_audit(
            status="skipped",
            reason="historical_success_guard",
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
        )
        logger.info(
            "[REFERRAL][PENDING_DUPLICATE] inviter=%s invitee=%s chat_id=%s destination_type=%s invite_link=%s reason=historical_success_guard",
            referrer_id,
            invitee_user_id,
            event_chat_id,
            destination_type,
            invite_link_log,
        )
        return

    # Cross-destination duplicate guard (P0-4): one invitee must never carry
    # more than one active/awarded referral across the group and channel
    # destinations at once. This is an atomic claim (unique index + upsert
    # with a non-blocking-status filter), not a pre-check + separate insert.
    try:
        lock_claimed = referral_invitee_lock.claim(
            db,
            invitee_user_id=invitee_user_id,
            inviter_user_id=referrer_id,
            chat_id=event_chat_id,
            destination_type=destination_type,
            now_utc_ts=created_at_utc,
        )
    except Exception:
        # Fail-open: a lock-collection outage must not block all referral
        # attribution. Falls back to the pre-existing pending_referrals
        # uniqueness (group_id, invitee_user_id) for same-destination dedup.
        logger.exception(
            "[REFERRAL][ERROR] step=invitee_lock_claim inviter=%s invitee=%s",
            referrer_id,
            invitee_user_id,
        )
        lock_claimed = True
    if not lock_claimed:
        _write_referral_audit(
            status="skipped",
            reason="cross_destination_duplicate",
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
        )
        logger.info(
            "[REFERRAL][PENDING_DUPLICATE] inviter=%s invitee=%s chat_id=%s destination_type=%s invite_link=%s reason=cross_destination_duplicate",
            referrer_id,
            invitee_user_id,
            event_chat_id,
            destination_type,
            invite_link_log,
        )
        return

    try:
        created_at_kl = created_at_utc.astimezone(KL_TZ).isoformat()
        result = pending_referrals_collection.update_one(
            {"group_id": event_chat_id, "invitee_user_id": invitee_user_id},
            {
                "$setOnInsert": {
                    "group_id": event_chat_id,
                    "destination_chat_id": event_chat_id,
                    "destination_type": destination_type,
                    "referral_join_seen_at_utc": created_at_utc,
                    "schema_version": 2,
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
            try:
                from affiliate_leaderboard import emit_referral_flow_event
                emit_referral_flow_event(
                    db,
                    event="join",
                    referrer_id=int(referrer_id),
                    invitee_id=int(invitee_user_id),
                    ts_utc=created_at_utc,
                    meta={"chat_id": event_chat_id},
                    idempotency_key=f"rf|join|{int(referrer_id)}|{int(invitee_user_id)}|{created_at_utc.strftime('%Y-%m-%d')}",
                )
                counted, reason = should_count_referral_join(db, int(referrer_id), created_at_utc)
                if counted:
                    emit_referral_flow_event(
                        db,
                        event="join_counted",
                        referrer_id=int(referrer_id),
                        invitee_id=int(invitee_user_id),
                        ts_utc=created_at_utc,
                        meta={"chat_id": event_chat_id},
                        idempotency_key=f"rf|join_counted|{int(referrer_id)}|{int(invitee_user_id)}|{created_at_utc.strftime('%Y-%m-%d')}",
                    )
                    logger.info("[AFFILIATE][JOIN_COUNT] inviter=%s invitee=%s counted=1", referrer_id, invitee_user_id)
                else:
                    emit_referral_flow_event(
                        db,
                        event="join_ignored",
                        referrer_id=int(referrer_id),
                        invitee_id=int(invitee_user_id),
                        ts_utc=created_at_utc,
                        meta={"chat_id": event_chat_id, "reason": reason or "cooldown"},
                        idempotency_key=f"rf|join_ignored|{int(referrer_id)}|{int(invitee_user_id)}|{created_at_utc.strftime('%Y-%m-%d')}",
                    )
                    logger.info("[AFFILIATE][JOIN_COUNT] inviter=%s invitee=%s counted=0 reason=%s", referrer_id, invitee_user_id, reason or "cooldown")
            except Exception:
                logger.exception("[AFFILIATE][JOIN_COUNT] audit_failed inviter=%s invitee=%s", referrer_id, invitee_user_id)
            logger.info(
                "[REFERRAL][PENDING_CREATED] inviter=%s invitee=%s chat_id=%s destination_type=%s invite_link=%s hold_hours=%s",
                referrer_id,
                invitee_user_id,
                event_chat_id,
                destination_type,
                invite_link_log,
                _referral_hold_hours(),
            )
            _maybe_send_referral_join_ack_dm(
                int(referrer_id),
                int(invitee_user_id),
                invitee_username=invitee_username,
            )
        else:
            logger.info(
                "[REFERRAL][PENDING_DUPLICATE] inviter=%s invitee=%s chat_id=%s destination_type=%s reason=exists",
                referrer_id,
                invitee_user_id,
                event_chat_id,
                destination_type,
            )

    except Exception as e:
        # The invitee lock was already claimed above (fail-open on its own
        # errors); since no pending row was actually created, release it so
        # this invitee is not blocked from every future referral attempt.
        referral_invitee_lock.release(
            db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=created_at_utc
        )
        _write_referral_audit(
            status="failed",
            reason="error",
            chat_id=event_chat_id,
            invitee_user_id=invitee_user_id,
            invitee_username=invitee_username,
            invite_link=invite_link_url,
            inviter_user_id=referrer_id,
            error=str(e),
        )
        logger.exception(
            "[REFERRAL][ERROR] step=create_pending inviter=%s invitee=%s err=%s",
            referrer_id,
            invitee_user_id,
            e,
        )
    return


def _confirm_referral_join(
    invitee_user_id: int,
    *,
    invitee_username: str | None = None,
    invite_link=None,
    chat_id: int | None = None,
):
    """Destination-neutral alias for _confirm_referral_on_main_join.

    Preferred name for new call sites (Phase 3 of the referral-channel
    migration); the original name is kept as the primary implementation
    because test_main_referral_error.py extracts it by name via AST.
    """
    return _confirm_referral_on_main_join(
        invitee_user_id,
        invitee_username=invitee_username,
        invite_link=invite_link,
        chat_id=chat_id,
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
    # Note: the {chat_id, inviter_id, is_active, created_at} lookup this
    # destination-scoped generator relies on is already covered by
    # invite_link_map_chat_inviter_active_created_desc_idx below via
    # safe_create_index (added for the mongo_query_targeting_2026_05 pass) —
    # no new index needed here.
    try:
        # Cross-destination duplicate-referral guard (P0-4). Isolated so a
        # failure here cannot block any index created after it.
        import referral_invitee_lock
        referral_invitee_lock.ensure_indexes(db)
    except Exception as e:
        print("⚠️ ensure_indexes error (referral_invitee_lock):", e)
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
    try:
        # Backs settle_pending_referrals' any-prior-award lookup by invitee,
        # which catches a pre-migration award under the legacy
        # destination-scoped award_key format that the new invitee-scoped
        # key (and referral_invitee_locks, a new collection) cannot see on
        # its own. Isolated so a failure here cannot block any index
        # created after it.
        referral_award_events_collection.create_index(
            [("invitee_user_id", 1)],
            name="idx_referral_award_events_invitee",
        )
    except Exception as e:
        print("⚠️ ensure_indexes error (idx_referral_award_events_invitee):", e)
    referral_events_collection.create_index(
        [("event", 1), ("inviter_id", 1), ("invitee_id", 1)],
        unique=True,
        name="uniq_referral_event",
    )
    referral_events_collection.create_index(
        [("inviter_id", 1), ("occurred_at", 1)],
        name="referral_events_by_inviter_time",
    )
    referral_events_collection.create_index(
        [("inviter_id", 1), ("event", 1)],
        name="referral_events_by_inviter_event",
    )
    referral_events_collection.create_index(
        [("inviter_id", 1), ("week_key", 1)],
        name="referral_events_by_inviter_week",
    )
    referral_events_collection.create_index(
        [("inviter_id", 1), ("month_key", 1)],
        name="referral_events_by_inviter_month",
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
    pending_referrals_collection.create_index(
        [("inviter_user_id", 1), ("created_at_utc", -1)],
        name="pending_by_inviter_created_desc",
    )
    qualified_events_collection.create_index(
        [("referrer_id", 1), ("invitee_id", 1)],
        name="qualified_by_referrer_invitee",
    )
    referral_events_collection.create_index(
        [("inviter_id", 1), ("invitee_id", 1), ("event", 1)],
        name="referral_events_by_inviter_invitee_event",
    )
    db.referral_tier_congrats.create_index(
        [("user_id", 1), ("month_key", 1), ("tier", 1)],
        unique=True,
        name="uniq_referral_tier_congrats",
    )
    referral_rate_limits_collection.create_index(
        [("key", 1)],
        unique=True,
        name="uniq_referral_rate_limit_key",
    )
    referral_rate_limits_collection.create_index(
        [("expireAt", 1)],
        expireAfterSeconds=0,
        name="ttl_referral_rate_limit_expire",
    )
    ensure_affiliate_leaderboard_indexes(db)
    ensure_affiliate_snapshot_indexes(db)

    # --- optional welcome eligibility ---
    db.welcome_eligibility.create_index([("uid", 1)], unique=True)
    db.welcome_eligibility.create_index([("expires_at", 1)], expireAfterSeconds=0)
    db.welcome_tickets.create_index([("uid", 1)], unique=True)
    db.welcome_tickets.create_index([("cleanup_at", 1)], expireAfterSeconds=0)
    safe_create_index(
        db.miniapp_sessions_daily,
        [("date_utc", 1), ("user_id", 1)],
        name="miniapp_sessions_daily_date_utc_user_id_uidx",
        unique=True,
    )
    safe_create_index(users_collection, [("weekly_xp", DESCENDING)], name="users_weekly_xp_desc_idx")
    safe_create_index(users_collection, [("weekly_referrals", DESCENDING)], name="users_weekly_referrals_desc_idx")
    safe_create_index(
        users_collection,
        [("weekly_xp", DESCENDING), ("weekly_referrals", DESCENDING), ("updated_at", DESCENDING)],
        name="users_weekly_rank_sort_idx",
    )
    safe_create_index(
        invite_link_map_collection,
        [("chat_id", ASCENDING), ("inviter_id", ASCENDING), ("is_active", ASCENDING), ("created_at", DESCENDING)],
        name="invite_link_map_chat_inviter_active_created_desc_idx",
    )
    safe_create_index(
        invite_link_map_collection,
        [("inviter_id", ASCENDING), ("created_at", DESCENDING)],
        name="invite_link_map_inviter_created_idx",
        partialFilterExpression={"inviter_id": {"$exists": True}},
    )
    safe_create_index(
        users_collection,
        [("snapshot_updated_at", ASCENDING)],
        name="users_snapshot_updated_at_idx",
        partialFilterExpression={"snapshot_updated_at": {"$exists": True}},
    )
    safe_create_index(
        db.referral_audit,
        [("inviter_user_id", ASCENDING), ("created_at", ASCENDING)],
        name="referral_audit_inviter_created_idx",
        partialFilterExpression={"inviter_user_id": {"$exists": True}},
    )
    # Supports is_user_blocked_for_self_invite's find_one({"invitee_user_id": ...}),
    # which previously ran as an unindexed find({}) full-collection scan on every
    # welcome-bonus eligibility check (see docs/xp_snapshot_incremental.md).
    safe_create_index(
        db.referral_audit,
        [("invitee_user_id", ASCENDING)],
        name="referral_audit_invitee_user_id_idx",
        partialFilterExpression={"invitee_user_id": {"$exists": True}},
    )
    safe_create_index(db.miniapp_sessions_daily, [("date_utc", ASCENDING)], name="miniapp_sessions_daily_date_utc_idx")
    safe_create_index(db.miniapp_sessions_daily, [("date", ASCENDING)], name="miniapp_sessions_daily_date_idx")
    safe_create_index(
        users_collection,
        [("pm1_due_at_utc", ASCENDING), ("pm1_sent_at_utc", ASCENDING), ("pm1_disabled", ASCENDING)],
        name="users_pm1_due_pending_idx",
        partialFilterExpression={"pm1_due_at_utc": {"$exists": True}},
    )
    safe_create_index(
        users_collection,
        [("pm2_due_at_utc", ASCENDING), ("pm2_sent_at_utc", ASCENDING), ("pm2_disabled", ASCENDING)],
        name="users_pm2_due_pending_idx",
        partialFilterExpression={"pm2_due_at_utc": {"$exists": True}},
    )
    safe_create_index(
        users_collection,
        [("pm3_due_at_utc", ASCENDING), ("pm3_sent_at_utc", ASCENDING), ("pm3_disabled", ASCENDING)],
        name="users_pm3_due_pending_idx",
        partialFilterExpression={"pm3_due_at_utc": {"$exists": True}},
    )
    safe_create_index(
        users_collection,
        [("pm4_due_at_utc", ASCENDING), ("pm4_sent_at_utc", ASCENDING), ("pm4_disabled", ASCENDING)],
        name="users_pm4_due_pending_idx",
        partialFilterExpression={"pm4_due_at_utc": {"$exists": True}},
    )
    safe_create_index(
        users_collection,
        [("mywin7_due_at_utc", ASCENDING), ("mywin7_sent_at_utc", ASCENDING), ("mywin7_disabled", ASCENDING)],
        name="users_mywin7_due_pending_idx",
        partialFilterExpression={"mywin7_due_at_utc": {"$exists": True}},
    )
    safe_create_index(
        users_collection,
        [("mywin14_due_at_utc", ASCENDING), ("mywin14_sent_at_utc", ASCENDING), ("mywin14_disabled", ASCENDING)],
        name="users_mywin14_due_pending_idx",
        partialFilterExpression={"mywin14_due_at_utc": {"$exists": True}},
    )
    db.voucher_ledger.create_index([("status", 1), ("created_at", 1)])
    db.qualified_events.create_index([("created_at", 1)])
    users_collection.create_index([("first_checkin_at", 1)])
    
    xp_events_collection.create_index([("user_id", 1), ("reason", 1)])
    ensure_xp_indexes(db)


    try:
        for legacy_name in ("uniq_tg_verify_user_id", "uniq_user_checks", "uq_tg_verif_user_id_sparse"):
            try:
                tg_verification_queue_collection.drop_index(legacy_name)
            except Exception:
                pass

        _ensure_index_if_missing(
            tg_verification_queue_collection,
            "uq_tg_verif_user_id_nonnull",
            [("user_id", 1)],
            unique=True,
            partialFilterExpression={"user_id": {"$type": "number"}},
        )
        _ensure_index_if_missing(
            tg_verification_queue_collection,
            "ix_verif_status_created",
            [("status", 1), ("created_at", 1)],
        )
    except Exception as e:
        print("⚠️ ensure_indexes error:", e)

    ensure_affiliate_indexes(db)
    _ensure_index_if_missing(
        db["ad_attribution"],
        "uq_ad_attribution_token",
        [("token", 1)],
        unique=True,
        sparse=True,
    )
        
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
    Create (or reuse) a unique Telegram chat invite link for this user,
    pointing at whichever chat get_referral_destination() currently
    resolves to (community group by default, or the official channel when
    REFERRAL_DESTINATION_MODE=official_channel).
    Uses Telegram HTTP API (sync), so no asyncio/event loop issues.
    Caches the link in Mongo to avoid rate limits.
    """
    dest_chat_id, destination_type = get_referral_destination()

    # 1) Reuse latest active invite link for this destination from DB if available.
    #    Scoping by chat_id means a destination-mode switch never reuses a link
    #    generated for the previous destination.
    if QUERY_TELEMETRY_LOGS:
        with JobTimer() as invite_query_timer:
            logger.info("[QUERY][invite_link_lookup] collection=invite_link_map filter_fields=chat_id,inviter_id,is_active sort_fields=created_at limit=1")
            latest_link_doc = invite_link_map_collection.find_one(
                {"chat_id": dest_chat_id, "inviter_id": user_id, "is_active": True},
                sort=[("created_at", -1)],
            )
        logger.info("[QUERY][invite_link_lookup] duration_ms=%s returned=%s", invite_query_timer.ms, 1 if latest_link_doc else 0)
    else:
        latest_link_doc = invite_link_map_collection.find_one(
            {"chat_id": dest_chat_id, "inviter_id": user_id, "is_active": True},
            sort=[("created_at", -1)],
        )
    if latest_link_doc and latest_link_doc.get("invite_link"):
        invite_link = latest_link_doc["invite_link"]
        logger.info(
            "[REFERRAL][LINK_REUSED] inviter_id=%s chat_id=%s destination_type=%s invite_link=%s",
            user_id,
            dest_chat_id,
            destination_type,
            _short_invite_link(invite_link),
        )
        return invite_link

    # 2) Create a named invite link: name="ref-<user_id>"
    #    Bot MUST be admin in the destination chat with "Invite users via link" permission
    name = f"ref-{user_id}"
    payload = {
        "chat_id": dest_chat_id,
        "name": name,
        "creates_join_request": False,
        # optional controls:
        # "expire_date": int(time.time()) + 30*24*3600,  # 30d expiry
        # "member_limit": 0,  # 0 = unlimited
    }
    r = requests.post(f"{API_BASE}/createChatInviteLink", json=payload, timeout=10)
    data = r.json()
    if not data.get("ok"):
        logger.error(
            "[REFERRAL][LINK_CREATE_FAILED] inviter_id=%s chat_id=%s destination_type=%s reason=%s",
            user_id,
            dest_chat_id,
            destination_type,
            data.get("description", "unknown"),
        )
        # No functional fallback exists: a t.me/<bot>?start=ref<uid> deep-link is not
        # parsed by the /start handler and never reaches attribution, so it is only
        # included here for ops visibility in logs, not as a link to hand to callers.
        bot_username = os.environ.get("BOT_USERNAME", "")
        deeplink = f"https://t.me/{bot_username}?start=ref{user_id}" if bot_username else ""
        raise RuntimeError(f"createChatInviteLink failed: {data.get('description','unknown')}\n"
                           f"Non-functional fallback deeplink (for ops reference only): {deeplink}")

    invite = data["result"]
    invite_link = invite["invite_link"]

    try:
        invite_link_map_collection.insert_one(
            {
                "inviter_id": user_id,
                "chat_id": dest_chat_id,
                "destination_type": destination_type,
                "invite_link": invite_link,
                "is_active": True,
                "created_at": datetime.now(KL_TZ),
                "schema_version": 2,
            }
        )
        logger.info(
            "[REFERRAL][LINK_CREATED] inviter_id=%s chat_id=%s destination_type=%s invite_link=%s",
            user_id,
            dest_chat_id,
            destination_type,
            _short_invite_link(invite_link),
        )
    except DuplicateKeyError:
        logger.info(
            "[REFERRAL][LINK_REUSED] inviter_id=%s chat_id=%s destination_type=%s invite_link=%s reason=duplicate_key",
            user_id,
            dest_chat_id,
            destination_type,
            _short_invite_link(invite_link),
        )
    except Exception as e:
        logger.exception(
            "[REFERRAL][LINK_CREATE_FAILED] inviter_id=%s chat_id=%s destination_type=%s reason=db_write_failed err=%s",
            user_id,
            dest_chat_id,
            destination_type,
            e,
        )
    return invite_link

def require_admin_from_query():
    admin_secret = _get_admin_secret(request)
    if _admin_secret_ok(admin_secret):
        return True, None

    # Phase B: browser Telegram Login session — wrapped so any failure falls through
    try:
        from admin_auth import session_admin
        if session_admin():
            return True, None
    except Exception:
        pass

    init_data = extract_raw_init_data_from_query(request)
    if not init_data:
        return False, ("Missing init_data", 400)

    ok, parsed, _ = verify_telegram_init_data(init_data)
    if not ok:
        return False, ("Admins only", 403)

    user_payload = (parsed or {}).get("user", {})
    if isinstance(user_payload, str):
        try:
            user_payload = json.loads(user_payload)
        except Exception:
            user_payload = {}
    try:
        caller_id = int((user_payload or {}).get("id"))
    except Exception:
        caller_id = None

    if not caller_id:
        return False, ("Admins only", 403)

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
# Phase 2A: weekly Marketing raw-data upload (CSV/XLSX), 50MB cap.
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024
app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
configure_admin_session(app)
app.register_blueprint(admin_auth_bp)

from campaigns import campaigns_bp
app.register_blueprint(campaigns_bp)

from campaign_builder import campaign_builder_bp, batch_release_tick
app.register_blueprint(campaign_builder_bp)

from campaign_performance import campaign_performance_bp
app.register_blueprint(campaign_performance_bp)

from campaign_intelligence import campaign_intelligence_bp
app.register_blueprint(campaign_intelligence_bp)

# Campaign Centre — generic marketing-campaign gateway (tournament,
# external subscription verification, external website, ... future types).
# Admin campaign CRUD lives at /api/admin/gc-campaigns (not /api/admin/
# campaigns) because that flat path is already taken by the pre-existing
# segment-audience campaigns_bp; providers/rewards/results use flat paths
# since those don't collide with anything existing.
from campaign_providers import campaign_providers_bp
app.register_blueprint(campaign_providers_bp)

from campaign_centre import campaign_centre_bp, campaign_public_bp
app.register_blueprint(campaign_centre_bp)
app.register_blueprint(campaign_public_bp)

from subscription_verification_api import subscription_verification_bp
app.register_blueprint(subscription_verification_bp)

from tournament_integration import tournament_integration_bp
app.register_blueprint(tournament_integration_bp)

from tournament_rewards import tournament_rewards_bp
app.register_blueprint(tournament_rewards_bp)

from campaign_rewards_api import campaign_rewards_bp
app.register_blueprint(campaign_rewards_bp)

from campaign_events import campaign_events_bp
app.register_blueprint(campaign_events_bp)

from referral_share_content import referral_share_content_bp
app.register_blueprint(referral_share_content_bp)

from community_centre import community_centre_bp, ensure_community_centre_indexes
app.register_blueprint(community_centre_bp)
try:
    ensure_community_centre_indexes()
except Exception:
    logger.exception("[COMMUNITY_CENTRE] index setup failed at startup")

admin_bp = Blueprint("admin", __name__)


def _admin_error_response(err):
    msg, code = err
    return jsonify({"success": False, "message": msg}), code



@admin_bp.post("/api/admin/channel-reactivation/start")
def api_admin_channel_reactivation_start():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    data = request.get_json(silent=True) or {}
    per_run_limit = data.get("per_run_limit", request.args.get("per_run_limit"))
    return jsonify(set_campaign_active(db, True, per_run_limit=per_run_limit))


@admin_bp.post("/api/admin/channel-reactivation/pause")
def api_admin_channel_reactivation_pause():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    return jsonify(set_campaign_active(db, False))


@admin_bp.get("/api/admin/reactivation/journey/summary")
def api_admin_reactivation_journey_summary():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    return jsonify(journey_summary(db))


@admin_bp.get("/api/admin/reactivation/journey/users")
def api_admin_reactivation_journey_users():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    limit = request.args.get("limit", default=100, type=int)
    return jsonify({
        "success": True,
        "items": journey_users(db, status=request.args.get("status"), tier=request.args.get("tier"), limit=limit),
    })


def _decorate_journey_config(cfg: dict) -> dict:
    now_ts = journey_now_utc()
    cfg["test_user_ids"] = sorted(cfg.get("test_user_ids", set()))
    cfg["computed_status"] = compute_journey_status(cfg, now_ref=now_ts)
    cfg["server_now_utc"] = now_ts.isoformat()
    cfg["server_now_kl"] = now_ts.astimezone(KL_TZ).isoformat()
    return cfg


@admin_bp.get("/api/admin/reactivation/journey/config")
def api_admin_reactivation_journey_config_get():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    cfg = _decorate_journey_config(get_journey_config(db))
    return jsonify({"success": True, "config": cfg})


@admin_bp.post("/api/admin/reactivation/journey/config")
def api_admin_reactivation_journey_config_set():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    data = request.get_json(silent=True) or {}
    result = update_journey_config(db, data)
    if result.get("success"):
        result["config"] = _decorate_journey_config(result["config"])
    status_code = 200 if result.get("success") else 400
    return jsonify(result), status_code


@admin_bp.post("/api/admin/reactivation/journey/pools/upload")
def api_admin_reactivation_journey_pools_upload():
    ok, err = require_admin_from_query()
    if not ok:
        return _admin_error_response(err)
    data = request.get_json(silent=True) or {}
    codes = data.get("codes")
    if isinstance(codes, str):
        codes = [line.strip() for line in codes.replace("\r", "\n").split("\n")]
    result = upload_pool_codes(db, str(data.get("pool_id") or ""), list(codes or []))
    status_code = 200 if result.get("success") else 400
    return jsonify(result), status_code


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


@admin_bp.get("/api/admin/retention-kpis")
def retention_kpis_api():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    months = request.args.get("months", default=12, type=int) or 12
    months = max(1, min(months, 24))
    rows = list(db[RETENTION_COLLECTION].find({}, {"_id": 0}).sort("cohort_month", -1).limit(months))
    return jsonify({"success": True, "months": months, "data": rows})


@admin_bp.get("/api/admin/retention-kpis/export")
def retention_kpis_export():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    months = request.args.get("months", default=12, type=int) or 12
    months = max(1, min(months, 24))
    rows = list(db[RETENTION_COLLECTION].find({}, {"_id": 0}).sort("cohort_month", -1).limit(months))
    import csv
    import io
    from flask import Response
    out = io.StringIO()
    w = csv.writer(out)
    cols = ["cohort_month", "cohort_size", "d7_eligible", "d7_retained", "d7_retention_rate", "d7_claim_retained", "d7_claim_retention_rate", "d14_eligible", "d14_retained", "d14_retention_rate", "d14_claim_retained", "d14_claim_retention_rate", "d30_eligible", "d30_retained", "d30_retention_rate", "d30_claim_retained", "d30_claim_retention_rate", "diagnosis", "computed_at_utc"]
    w.writerow(cols)
    for r in rows:
        w.writerow([r.get(c, "") for c in cols])
    filename = f"retention_cohort_kpis_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
    return Response(out.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@admin_bp.post("/api/admin/retention-kpis/recompute")
def retention_kpis_recompute():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    months = request.args.get("months", default=12, type=int) or 12
    months = max(1, min(months, 24))
    rows = compute_retention_kpis(db, months=months, now_utc=datetime.now(timezone.utc))
    return jsonify({"success": True, "months": months, "count": len(rows)})


@admin_bp.get("/api/admin/funnel-dashboard")
def funnel_dashboard_api():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    window = request.args.get("window", "7d").strip().lower()
    date_from_raw = request.args.get("date_from", "").strip()
    date_to_raw = request.args.get("date_to", "").strip()
    refresh = request.args.get("refresh") == "1"

    now = _utc_now()

    if window == "custom" and date_from_raw and date_to_raw:
        try:
            start = datetime.fromisoformat(date_from_raw.replace("Z", "+00:00"))
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            # Normalize date-only date_to to midnight of the *next* day so that
            # queries using $lt (exclusive) include the entire selected day.
            if len(date_to_raw.strip()) == 10:
                end = (
                    datetime.strptime(date_to_raw.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    + timedelta(days=1)
                )
            else:
                end = datetime.fromisoformat(date_to_raw.replace("Z", "+00:00"))
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
            if end > now:
                end = now
        except (ValueError, TypeError):
            return jsonify({"success": False, "message": "Invalid date_from or date_to format."}), 400
        window_label = f"{date_from_raw[:10]} to {date_to_raw[:10]}"
        cache_key = f"funnel_v2:custom:{date_from_raw[:10]}:{date_to_raw[:10]}"
    else:
        if window not in ("7d", "30d", "all"):
            window = "7d"
        start = _admin_dashboard_window_start(window, now)
        end = now
        window_label = _admin_dashboard_window_label(window)
        cache_key = f"funnel_v2:{window}"

    if not refresh:
        cached = _dashboard_cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

    try:
        result = compute_funnel(db, start, end, now)
    except Exception as exc:
        logger.exception("[FUNNEL_DASHBOARD] compute failed: %s", exc)
        return jsonify({"success": False, "message": f"Funnel compute error: {exc}"}), 500

    payload = {
        "success": True,
        "window": window,
        "window_label": window_label,
        "window_start": start.isoformat() if start else None,
        "window_end": end.isoformat(),
        "as_of": now.isoformat(),
        "generated_at": now.isoformat(),
        **result,
    }
    _dashboard_cache_set(cache_key, payload)
    return jsonify(payload)


# ======================================================================
# Admin Dashboard APIs (read-only operational visibility)
# Phases 0/1/2: Executive Summary, Activation Funnel, Abuse overview.
# No business logic — pure aggregation over existing collections.
# ======================================================================

_DASHBOARD_CACHE: dict[str, tuple[float, dict]] = {}
_DASHBOARD_CACHE_TTL_S = 300  # 5 minutes

# Telegram member count is cached separately with a 1-hour TTL, keyed by
# chat_id so the official channel and chatroom are cached independently.
from dashboard_telegram import (
    TELEGRAM_COUNTS_DOC_ID,
    MEMBER_COUNT_STALE_AFTER_S,
    refresh_member_counts,
    read_member_count,
    sanitize_telegram_counts_cache,
)

# Env override for community chat; falls back to the main group.
_COMMUNITY_CHAT_ID: int | None = None
try:
    _raw_comm = os.environ.get("COMMUNITY_CHAT_ID") or os.environ.get("MYWIN_CHAT_ID", "")
    if _raw_comm:
        _COMMUNITY_CHAT_ID = int(_raw_comm)
except (TypeError, ValueError):
    pass
if _COMMUNITY_CHAT_ID is None:
    try:
        from onboarding import MYWIN_CHAT_ID as _ONBOARDING_CHAT_ID
        _COMMUNITY_CHAT_ID = _ONBOARDING_CHAT_ID
    except Exception:
        pass


def _telegram_count_metrics() -> list[tuple[str, int | None]]:
    """The (metric_name, chat_id) pairs cached for the dashboard."""
    return [
        ("official_channel_subscribers", OFFICIAL_CHANNEL_ID),
        ("chatroom_members", _COMMUNITY_CHAT_ID),
    ]


def _fetch_chat_member_count_http(chat_id: int) -> int:
    """Fetch a Telegram chat member count through the Bot HTTP API."""
    bot_token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN missing for Telegram member count refresh")

    url = f"https://api.telegram.org/bot{bot_token}/getChatMemberCount"
    try:
        resp = requests.get(url, params={"chat_id": chat_id}, timeout=10)
    except Exception as exc:  # noqa: BLE001 - surface as per-metric refresh failure
        raise RuntimeError(f"Telegram HTTP request failed: {exc}") from exc

    if resp.status_code != 200:
        raise RuntimeError(
            f"Telegram HTTP status {resp.status_code}: {getattr(resp, 'text', '')}"
        )

    try:
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001 - malformed Telegram response
        raise RuntimeError(f"Telegram HTTP invalid JSON: {exc}") from exc

    if not payload.get("ok"):
        description = payload.get("description") or payload
        raise RuntimeError(f"Telegram ok=false: {description}")

    if "result" not in payload:
        raise RuntimeError("Telegram response missing result")

    try:
        return int(payload["result"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Telegram response result is not an int: {payload.get('result')}") from exc


def refresh_telegram_member_counts() -> dict:
    """Worker-side refresh: fetch live counts and cache them in admin_cache.

    Calls Telegram's HTTP API from the worker scheduler, preserving the previous
    count per-metric on failure, and upserts the result into ``admin_cache`` doc
    ``telegram_member_counts``. The dashboard reads this document only and never
    calls Telegram itself. Never raises.
    """
    logger.info(
        "[DASHBOARD_TG_REFRESH][START]\n"
        f"runner_mode={RUNNER_MODE}\n"
        f"official_channel_id={OFFICIAL_CHANNEL_ID}\n"
        f"community_chat_id={_COMMUNITY_CHAT_ID}"
    )
    try:
        existing = admin_cache_col.find_one({"_id": TELEGRAM_COUNTS_DOC_ID}) or {}
        doc = refresh_member_counts(
            _telegram_count_metrics(),
            _fetch_chat_member_count_http,
            existing=existing,
            logger=logger,
        )
        result = admin_cache_col.update_one(
            {"_id": TELEGRAM_COUNTS_DOC_ID},
            {"$set": {"updated_at": doc["updated_at"], "counts": doc["counts"]}},
            upsert=True,
        )
        logger.info(
            "[DASHBOARD_TG_REFRESH][WRITE]\n"
            f"matched={getattr(result, 'matched_count', None)}\n"
            f"modified={getattr(result, 'modified_count', None)}\n"
            f"upserted_id={getattr(result, 'upserted_id', None)}\n"
            "counts_keys=official_channel_subscribers,chatroom_members"
        )
        return doc
    except Exception:
        logger.exception("[DASHBOARD_TG_REFRESH] refresh failed")
        return {}


def _dashboard_cache_get(key: str):
    entry = _DASHBOARD_CACHE.get(key)
    if not entry:
        return None
    ts, payload = entry
    if (time.time() - ts) > _DASHBOARD_CACHE_TTL_S:
        return None
    return payload


def _dashboard_cache_set(key: str, payload: dict) -> None:
    _DASHBOARD_CACHE[key] = (time.time(), payload)
    if len(_DASHBOARD_CACHE) > 256:
        _DASHBOARD_CACHE.clear()


def _utc_now():
    return datetime.now(timezone.utc)


def _utc_today_start(now=None):
    now = now or _utc_now()
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)


def _date_str(d) -> str:
    return d.strftime("%Y-%m-%d")


def _safe_count(fn):
    """Run a count query, returning (value, None) or (None, error_str)."""
    try:
        return int(fn()), None
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[ADMIN_DASHBOARD] count failed: %s", exc)
        return None, str(exc)


# Segment label matchers (abuse module). Stored labels are free-form, so we
# match case-insensitively against the canonical and common alias spellings.
_SEG_VOUCHER_HUNTER_RE = re.compile(r"^\s*voucher[\s_\-]?hunters?\s*$", re.IGNORECASE)
_SEG_WELCOME_ABUSE_RE = re.compile(r"^\s*welcome[\s_\-]?abus(?:e|er|ers)\s*$", re.IGNORECASE)


def _count_segment(regex) -> int:
    return int(
        users_collection.count_documents(
            {"$or": [{"for_bot_segment": regex}, {"bot_segment": regex}]}
        )
    )


def _dashboard_window_start(window, now):
    window = (window or "7d").strip().lower()
    if window not in {"all", "today", "7d", "30d", "90d"}:
        window = "7d"
    if window == "all":
        return window, None
    if window == "today":
        return window, _utc_today_start(now)
    return window, now - timedelta(days=int(window[:-1]))


def _dashboard_claim_time_filter(*, status, start, success):
    query = {"status": status}
    if start is None:
        return query
    primary = "claimed_at" if success else "updated_at"
    query["$or"] = [
        {primary: {"$gte": start}},
        {primary: {"$exists": False}, "created_at": {"$gte": start}},
    ]
    return query


def _dashboard_drop_id_variants(value):
    variants = []
    if value is not None:
        variants.append(value)
        text = str(value)
        if text not in variants:
            variants.append(text)
    return variants


def _dashboard_doc_user_id(doc: dict):
    value = (doc or {}).get("uid")
    if value is None:
        value = (doc or {}).get("user_id")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _dashboard_dt(value):
    return value.isoformat() if isinstance(value, datetime) else None


_ADMIN_DASHBOARD_WINDOWS = {"7d": 7, "30d": 30, "all": None}
_DEFAULT_ADMIN_DASHBOARD_WINDOW = "7d"


def _normalize_admin_dashboard_window(window):
    value = str(window or _DEFAULT_ADMIN_DASHBOARD_WINDOW).strip().lower()
    return value if value in _ADMIN_DASHBOARD_WINDOWS else _DEFAULT_ADMIN_DASHBOARD_WINDOW


def _admin_dashboard_window_start(window, now):
    window = _normalize_admin_dashboard_window(window)
    days = _ADMIN_DASHBOARD_WINDOWS[window]
    return None if days is None else now - timedelta(days=days)


def _admin_dashboard_window_label(window):
    return {"7d": "last 7 days", "30d": "last 30 days", "all": "all time"}[
        _normalize_admin_dashboard_window(window)
    ]


def _admin_dashboard_time_filter(field, window_start):
    return {} if window_start is None else {field: {"$gte": window_start}}


@admin_bp.get("/api/admin/dashboard/summary")
def dashboard_summary():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    window = _normalize_admin_dashboard_window(request.args.get("window"))
    cache_key = f"summary:{window}"
    cached = _dashboard_cache_get(cache_key)
    if cached is not None and request.args.get("refresh") != "1":
        return jsonify(cached)

    now = _utc_now()
    today_start = _utc_today_start(now)
    d7 = now - timedelta(days=7)
    d30 = now - timedelta(days=30)
    window_start = _admin_dashboard_window_start(window, now)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    sessions = db["miniapp_sessions_daily"]
    claims = db["voucher_claims"]
    tickets = db["welcome_tickets"]
    drops = db["drops"]
    vouchers = db["vouchers"]

    def _active_since(days_back: int) -> int:
        start = _date_str((now - timedelta(days=days_back)).date())
        return len(sessions.distinct("user_id", {"date_utc": {"$gte": start}}))

    errors: list[str] = []

    def grab(fn):
        val, e = _safe_count(fn)
        if e:
            errors.append(e)
        return val

    # ---- Telegram counts (worker-cached; read-only here) ----
    # The worker refreshes these into admin_cache; the dashboard never calls the
    # Telegram API directly (the bot loop only exists in the worker process).
    # Each metric falls back to its last cached value and is flagged stale.
    tg_cache_doc = admin_cache_col.find_one({"_id": TELEGRAM_COUNTS_DOC_ID}) or {}
    tg_counts = tg_cache_doc.get("counts") or {}

    def _tg_metric(label):
        res = read_member_count(tg_counts.get(label), now=now)
        status = res.get("status")
        if status != "ok":
            logger.info(
                "[DASHBOARD_TG_CACHE]\nmetric=%s\nstatus=%s\nage_seconds=%s",
                label,
                status,
                res.get("age_seconds"),
            )
            errors.append(f"{label}: telegram unavailable, serving cached value")
        return res

    official_subs = _tg_metric("official_channel_subscribers")
    chatroom_members = _tg_metric("chatroom_members")

    # ---- Users ----
    registered_users = grab(lambda: users_collection.count_documents({}))
    active_today = grab(lambda: len(sessions.distinct("user_id", {"date_utc": _date_str(now.date())})))
    active_7d = grab(lambda: _active_since(6))
    active_30d = grab(lambda: _active_since(29))
    active_selected = grab(
        lambda: len(
            sessions.distinct(
                "user_id",
                {} if window_start is None else {"date_utc": {"$gte": _date_str(window_start.date())}},
            )
        )
    )

    # ---- Community (check-ins) ----
    checkins_today = grab(lambda: xp_events_collection.count_documents({"type": "checkin", "created_at": {"$gte": today_start}}))
    checkins_7d = grab(lambda: xp_events_collection.count_documents({"type": "checkin", "created_at": {"$gte": d7}}))
    checkins_selected = grab(
        lambda: xp_events_collection.count_documents(
            {"type": "checkin", **_admin_dashboard_time_filter("created_at", window_start)}
        )
    )

    # ---- Referrals ----
    referral_window = _admin_dashboard_time_filter("created_at_utc", window_start)
    pending_referrals = grab(lambda: pending_referrals_collection.count_documents({**referral_window, "status": {"$in": ["pending", "pending_channel", "processing"]}}))
    qualified_total = grab(lambda: qualified_events_collection.count_documents({}))
    qualified_7d = grab(lambda: qualified_events_collection.count_documents({"qualified_at": {"$gte": d7}}))
    qualified_selected = grab(lambda: qualified_events_collection.count_documents(_admin_dashboard_time_filter("qualified_at", window_start)))
    revoked_referrals = grab(lambda: pending_referrals_collection.count_documents({**referral_window, "status": {"$in": ["revoked", "failed", "rejected", "expired"]}}))

    # ---- Vouchers ----
    campaign_window = {}
    if window_start is not None:
        campaign_window = {"startsAt": {"$lte": now}, "endsAt": {"$gte": window_start}}
    active_campaigns = grab(lambda: drops.count_documents({"status": "active", **campaign_window}))
    claims_today = grab(lambda: claims.count_documents({"status": "claimed", "created_at": {"$gte": today_start}}))
    claims_selected = grab(lambda: claims.count_documents({"status": "claimed", **_admin_dashboard_time_filter("claimed_at", window_start)}))
    remaining_codes = grab(lambda: vouchers.count_documents({"status": "unclaimed"}))

    # ---- Welcome ----
    def _welcome_eligible_count():
        if window_start is None:
            return welcome_eligibility_collection.count_documents({})
        ts = window_start
        return welcome_eligibility_collection.count_documents({
            "$or": [{"created_at": {"$gte": ts}}, {"first_seen_at": {"$gte": ts}}]
        })
    welcome_eligible = grab(_welcome_eligible_count)
    welcome_claimed = grab(lambda: tickets.count_documents({"status": "claimed", **_admin_dashboard_time_filter("claimed_at", window_start)}))
    welcome_conversion = None
    if welcome_eligible and welcome_claimed is not None and welcome_eligible > 0:
        welcome_conversion = round(100.0 * welcome_claimed / welcome_eligible, 1)

    # ---- Affiliate ----
    affiliate_window = _admin_dashboard_time_filter("updated_at", window_start)
    affiliate_pending = grab(lambda: affiliate_ledger_collection.count_documents({**affiliate_window, "status": {"$in": ["PENDING_REVIEW", "PENDING_MANUAL"]}}))
    affiliate_approved_month = grab(lambda: affiliate_ledger_collection.count_documents({"status": {"$in": ["APPROVED", "ISSUED"]}, "updated_at": {"$gte": month_start}}))
    affiliate_approved_selected = grab(lambda: affiliate_ledger_collection.count_documents({**affiliate_window, "status": {"$in": ["APPROVED", "ISSUED"]}}))

    # ---- System / worker health ----
    heartbeat = admin_cache_col.find_one({"_id": "snapshot_heartbeat"}, {"ts_utc": 1}) or {}
    hb_ts = _normalize_snapshot_updated_at(heartbeat.get("ts_utc"))
    if hb_ts is None:
        snapshot_age = None
        worker_status = "unknown"
    else:
        snapshot_age = int((now - hb_ts).total_seconds())
        worker_status = "healthy" if snapshot_age <= 900 else "stale"
    last_run_doc = audit_events_collection.find_one({"_id": "monthly_job:last_run"}, {"run_at_utc": 1}) or {}
    last_scheduler_run = last_run_doc.get("run_at_utc")

    payload = {
        "success": True,
        "as_of": now.isoformat(),
        "generated_at": now.isoformat(),
        "window": window,
        "window_label": _admin_dashboard_window_label(window),
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": now.isoformat(),
        "data_source": "UIM",
        "cache_ttl_s": _DASHBOARD_CACHE_TTL_S,
        "users": {
            "official_channel_subscribers": official_subs["count"],
            "official_channel_subscribers_stale": official_subs["stale"],
            "official_channel_subscribers_cached_at": official_subs["cached_at"],
            "chatroom_members": chatroom_members["count"],
            "chatroom_members_stale": chatroom_members["stale"],
            "chatroom_members_cached_at": chatroom_members["cached_at"],
            "official_channel_id": OFFICIAL_CHANNEL_ID,
            "community_chat_id": _COMMUNITY_CHAT_ID,
            "registered": registered_users,
            "active_selected": active_selected,
            "active_today": active_today,
            "active_7d": active_7d,
            "active_30d": active_30d,
        },
        "community": {
            "checkins_selected": checkins_selected,
            "checkins_today": checkins_today,
            "checkins_7d": checkins_7d,
        },
        "referrals": {
            "pending": pending_referrals,
            "qualified": qualified_selected,
            "qualified_total": qualified_total,
            "qualified_7d": qualified_7d,
            "revoked": revoked_referrals,
        },
        "vouchers": {
            "active_campaigns": active_campaigns,
            "claims": claims_selected,
            "claims_today": claims_today,
            "remaining_codes": remaining_codes,
        },
        "welcome": {
            "eligible": welcome_eligible,
            "claimed": welcome_claimed,
            "conversion_pct": welcome_conversion,
        },
        "affiliate": {
            "pending_review": affiliate_pending,
            "approved": affiliate_approved_selected,
            "approved_this_month": affiliate_approved_month,
        },
        "system": {
            "worker_status": worker_status,
            "snapshot_age_seconds": snapshot_age,
            "last_snapshot_publish": hb_ts.isoformat() if hb_ts else None,
            "last_scheduler_run": last_scheduler_run.isoformat() if isinstance(last_scheduler_run, datetime) else None,
        },
        "partial_errors": errors or None,
    }
    _dashboard_cache_set(cache_key, payload)
    return jsonify(payload)


@admin_bp.get("/api/admin/dashboard/telegram-counts/cache")
def dashboard_telegram_counts_cache():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    doc = admin_cache_col.find_one({"_id": TELEGRAM_COUNTS_DOC_ID})
    return jsonify(sanitize_telegram_counts_cache(doc))


@admin_bp.post("/api/admin/dashboard/telegram-counts/refresh")
def dashboard_telegram_counts_refresh():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    if RUNNER_MODE != "worker":
        return jsonify(
            {
                "success": False,
                "code": "worker_only",
                "message": "Telegram count refresh runs in worker; check scheduler logs.",
            }
        )

    doc = refresh_telegram_member_counts()
    return jsonify({"success": bool(doc), "cache": sanitize_telegram_counts_cache(doc)})


@admin_bp.get("/api/admin/dashboard/funnel")
def dashboard_funnel():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    window = _normalize_admin_dashboard_window(request.args.get("window"))

    cache_key = f"funnel:{window}"
    if request.args.get("refresh") != "1":
        cached = _dashboard_cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

    now = _utc_now()
    start = _admin_dashboard_window_start(window, now)

    window_end = now
    tickets = db["welcome_tickets"]
    affiliate_ledger = db["affiliate_ledger"]
    new_joiner_claims = db["new_joiner_claims"]

    def _cohort_uid(value):
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return value

    def _doc_uid(doc):
        return _cohort_uid((doc or {}).get("uid") or (doc or {}).get("user_id"))

    def _count_stage(name, count, *, data_quality="exact", note=None):
        out = {"name": name, "count": int(count), "data_quality": data_quality}
        if note:
            out["note"] = note
        if join_count <= 0:
            out["conversion_pct"] = 0.0
            out["dropoff_pct"] = 0.0
            return out
        conversion = round(100.0 * int(count) / join_count, 1)
        out["conversion_pct"] = conversion
        out["dropoff_pct"] = round(100.0 - conversion, 1)
        if int(count) > join_count:
            out["data_quality"] = "invalid"
            out["note"] = "Stage count exceeds join cohort; query needs audit."
        return out

    cohort_query = _admin_dashboard_time_filter("joined_main_at", start)
    cohort_docs = users_collection.find(cohort_query, {"user_id": 1, "joined_main_at": 1})
    cohort_user_ids = {
        uid for uid in (_cohort_uid((doc or {}).get("user_id")) for doc in cohort_docs)
        if uid is not None
    }
    cohort_user_id_list = list(cohort_user_ids)
    join_count = len(cohort_user_ids)

    # Every later stage is intersected with the Join Group cohort.
    pm_start_count = int(
        users_collection.count_documents(
            {
                "user_id": {"$in": cohort_user_id_list},
                "first_private_interaction_at": {"$exists": True, "$ne": None} if start is None else {"$gte": start},
            }
        )
    ) if cohort_user_ids else 0

    checkin_users = set()
    if cohort_user_ids:
        checkin_users = {
            _cohort_uid(uid)
            for uid in xp_events_collection.distinct(
                "user_id",
                {
                    "user_id": {"$in": cohort_user_id_list},
                    **_admin_dashboard_time_filter("created_at", start),
                    "$or": [
                        {"type": "checkin"},
                        {"reason": "checkin"},
                        {"unique_key": {"$regex": r"^checkin:"}},
                    ],
                },
            )
        }
        checkin_users.discard(None)

    eligible_user_ids = set()
    if cohort_user_ids:
        for doc in welcome_eligibility_collection.find(
            {
                **_admin_dashboard_time_filter("created_at", start),
                "$or": [
                    {"uid": {"$in": cohort_user_id_list}},
                    {"user_id": {"$in": cohort_user_id_list}},
                ],
            },
            {"uid": 1, "user_id": 1},
        ):
            uid = _doc_uid(doc)
            if uid in cohort_user_ids:
                eligible_user_ids.add(uid)

    claim_user_ids = set()
    if cohort_user_ids:
        for doc in affiliate_ledger.find(
            {
                "status": "ISSUED",
                **_admin_dashboard_time_filter("updated_at", start),
                "user_id": {"$in": cohort_user_id_list},
                "$or": [
                    {"ledger_type": "WELCOME"},
                    {"tier": "WELCOME"},
                    {"pool_id": "WELCOME"},
                ],
            },
            {"user_id": 1},
        ):
            uid = _doc_uid(doc)
            if uid in cohort_user_ids:
                claim_user_ids.add(uid)
        for collection, query in (
            (
                welcome_eligibility_collection,
                {
                    "claimed": True,
                    **_admin_dashboard_time_filter("claimed_at", start),
                    "$or": [
                        {"uid": {"$in": cohort_user_id_list}},
                        {"user_id": {"$in": cohort_user_id_list}},
                    ],
                },
            ),
            (
                new_joiner_claims,
                {
                    **_admin_dashboard_time_filter("claimed_at", start),
                    "$or": [
                        {"uid": {"$in": cohort_user_id_list}},
                        {"user_id": {"$in": cohort_user_id_list}},
                    ],
                },
            ),
            (
                tickets,
                {
                    "status": "claimed",
                    **_admin_dashboard_time_filter("claimed_at", start),
                    "$or": [
                        {"uid": {"$in": cohort_user_id_list}},
                        {"user_id": {"$in": cohort_user_id_list}},
                    ],
                },
            ),
        ):
            for doc in collection.find(query, {"uid": 1, "user_id": 1}):
                uid = _doc_uid(doc)
                if uid in cohort_user_ids:
                    claim_user_ids.add(uid)

    raw_stages = [
        _count_stage("Join Group", join_count),
        _count_stage(
            "Welcome Eligible",
            len(eligible_user_ids & cohort_user_ids),
            note="Eligibility record created on join; not final unlock state.",
        ),
        _count_stage(
            "PM Start",
            pm_start_count,
            note="Uses first_private_interaction_at from private /start or first private-message handlers.",
        ),
        _count_stage("Check-in", len(checkin_users & cohort_user_ids)),
        _count_stage(
            "Welcome Claim",
            len(claim_user_ids & cohort_user_ids),
            note="Current source includes affiliate_ledger WELCOME ISSUED plus legacy welcome claim sources.",
        ),
        {"name": "First Play", "count": None, "data_quality": "missing",
         "note": "No first-play signal exists. Requires game-backend instrumentation."},
    ]

    stages = []
    for s in raw_stages:
        out = dict(s)
        if s["count"] is None:
            out["conversion_pct"] = None
            out["dropoff_pct"] = None
        stages.append(out)

    payload = {
        "success": True,
        "window": window,
        "as_of": now.isoformat(),
        "generated_at": now.isoformat(),
        "window_label": _admin_dashboard_window_label(window),
        "data_source": "UIM",
        "method": "join_cohort",
        "cohort_size": join_count,
        "window_start": start.isoformat() if start else None,
        "window_end": window_end.isoformat(),
        "stages": stages,
    }
    _dashboard_cache_set(cache_key, payload)
    return jsonify(payload)


@admin_bp.get("/api/admin/dashboard/abuse")
def dashboard_abuse():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    window = _normalize_admin_dashboard_window(request.args.get("window"))
    cache_key = f"abuse:{window}"
    if request.args.get("refresh") != "1":
        cached = _dashboard_cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

    now = _utc_now()
    window_start = _admin_dashboard_window_start(window, now)
    claims = db["voucher_claims"]
    rate_limits = db["claim_rate_limits"]
    errors: list[str] = []

    # Repeat claimers: users with more than one successful claim.
    repeat_claimers = None
    try:
        agg = claims.aggregate([
            {"$match": {"status": "claimed", **_admin_dashboard_time_filter("claimed_at", window_start)}},
            {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
            {"$match": {"n": {"$gt": 1}}},
            {"$count": "c"},
        ])
        agg_list = list(agg)
        repeat_claimers = int(agg_list[0]["c"]) if agg_list else 0
    except Exception as exc:
        errors.append(f"repeat_claimers: {exc}")

    # Blocked IPs: rate-limit records with an active block window.
    blocked_ips = None
    try:
        blocked_ips = int(rate_limits.count_documents({"blockedUntil": {"$gt": now}}))
    except Exception as exc:
        errors.append(f"blocked_ips: {exc}")

    # Suspicious referrers (heuristic): referrers with >=5 invites and 0 qualified.
    suspicious_referrers = None
    try:
        agg = pending_referrals_collection.aggregate([
            {"$match": _admin_dashboard_time_filter("created_at_utc", window_start)},
            {"$group": {
                "_id": "$inviter_user_id",
                "invited": {"$sum": 1},
                "qualified": {"$sum": {"$cond": [{"$in": ["$status", ["qualified", "awarded", "settled", "success"]]}, 1, 0]}},
            }},
            {"$match": {"invited": {"$gte": 5}, "qualified": 0}},
            {"$count": "c"},
        ])
        agg_list = list(agg)
        suspicious_referrers = int(agg_list[0]["c"]) if agg_list else 0
    except Exception as exc:
        errors.append(f"suspicious_referrers: {exc}")

    voucher_hunter_count = None
    welcome_abuse_count = None
    try:
        voucher_hunter_count = _count_segment(_SEG_VOUCHER_HUNTER_RE)
    except Exception as exc:
        errors.append(f"voucher_hunter: {exc}")
    try:
        welcome_abuse_count = _count_segment(_SEG_WELCOME_ABUSE_RE)
    except Exception as exc:
        errors.append(f"welcome_abuse: {exc}")

    payload = {
        "success": True,
        "as_of": now.isoformat(),
        "generated_at": now.isoformat(),
        "window": window,
        "window_label": _admin_dashboard_window_label(window),
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": now.isoformat(),
        "data_source": "UIM",
        "metrics": {
            "repeat_claimers": {"value": repeat_claimers, "data_quality": "exact",
                                "note": f"Users with >1 successful voucher claim ({_admin_dashboard_window_label(window)})."},
            "blocked_ips": {"value": blocked_ips, "data_quality": "exact",
                            "note": "Claim rate-limit records with an active block window."},
            "suspicious_referrers": {"value": suspicious_referrers, "data_quality": "heuristic",
                                     "note": f">=5 invites with 0 qualified ({_admin_dashboard_window_label(window)}). Heuristic, not a confirmed fraud signal."},
            "voucher_hunter_count": {"value": voucher_hunter_count, "data_quality": "approx",
                                     "note": "Users labelled voucher_hunter by bot segmentation."},
            "welcome_abuse_count": {"value": welcome_abuse_count, "data_quality": "approx",
                                    "note": "Users labelled welcome_abuse by bot segmentation."},
        },
        "partial_errors": errors or None,
    }
    _dashboard_cache_set(cache_key, payload)
    return jsonify(payload)


# ---------------------------------------------------------------------------
# Admin dashboard panels (Phase C): Vouchers / Referrals / Affiliate / Audit /
# User drilldown / Settings. All read-only — they only report on data the bot
# already produced and never alter voucher, referral, affiliate, XP/check-in,
# MiniApp or scheduler behaviour. Query/shaping logic lives in the pure,
# unit-tested ``dashboard_panels`` module; these routes are thin wrappers that
# inject the live collections and apply the shared admin guard + cache.
# ---------------------------------------------------------------------------
import dashboard_panels as _panels  # noqa: E402
import uim_validation as _uim_validation  # noqa: E402


def _panel_cached(key, builder, *, ttl_key=True):
    """Run a panel builder behind the existing dashboard cache + admin guard."""
    if request.args.get("refresh") != "1":
        cached = _dashboard_cache_get(key)
        if cached is not None:
            return jsonify(cached)
    payload = builder()
    generated_at = _utc_now().isoformat()
    payload.setdefault("generated_at", payload.get("as_of") or generated_at)
    payload.setdefault("data_source", "UIM")
    if "window" not in payload:
        payload.setdefault("window", "all")
        payload.setdefault("window_label", "all time")
        payload.setdefault("window_start", None)
    _dashboard_cache_set(key, payload)
    return jsonify(payload)


@admin_bp.get("/api/admin/dashboard/segments")
def dashboard_segments():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    month = request.args.get("month") or None
    mode = _panels._normalize_segment_mode(request.args.get("mode") or ("month" if month else None))
    segment_filter = request.args.get("segment") or None
    cache_key = f"panel:segments:{mode}:{month or ''}:{segment_filter or ''}"
    return _panel_cached(
        cache_key,
        lambda: _panels.build_segments_panel(
            users_col=users_collection,
            now=_utc_now(),
            mode=mode,
            segment_filter=segment_filter,
            month=month,
            segment_snapshots_col=segment_snapshots_collection,
        ),
    )


@admin_bp.get("/api/admin/dashboard/validation")
def dashboard_validation():
    """Phase 5: read-only UIM (Google Sheet) vs backend KPI comparison.

    Debug/compare tool only — never writes to ``users``, never touches
    segment classification, voucher allocation, public-pool probability or
    reward logic. Always compares current live UIM values vs current live
    backend values; there is no historical/period mode in this release.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    now = _utc_now()

    def _build():
        uim_result = _uim_validation.fetch_uim_validation_metrics()
        return _panels.build_validation_panel(
            users_col=users_collection,
            uim_result=uim_result,
            now=now,
        )

    return _panel_cached("panel:validation", _build)


# ---------------------------------------------------------------------------
# Runtime Status dashboard (read-only). Answers "is this feature actually
# running in production right now" — derived from live scheduler locks,
# feature-flag settings, and counts of recently-written tracking fields.
# Never writes to the database and never touches bot business logic.
# ---------------------------------------------------------------------------
import runtime_status as _runtime_status  # noqa: E402

_RUNTIME_STATUS_TELEGRAM_CACHE: dict[str, Any] = {"ok": None, "checked_at": None}


def _runtime_status_mongo_ping() -> bool:
    db.command("ping")
    return True


def _runtime_status_telegram_ping() -> bool:
    now = _utc_now()
    cached_at = _RUNTIME_STATUS_TELEGRAM_CACHE.get("checked_at")
    if cached_at and (now - cached_at).total_seconds() < 60:
        return bool(_RUNTIME_STATUS_TELEGRAM_CACHE.get("ok"))
    ok = False
    try:
        resp = requests.get(f"{API_BASE}/getMe", timeout=5)
        ok = bool(resp.ok and (resp.json() or {}).get("ok"))
    except Exception:
        ok = False
    _RUNTIME_STATUS_TELEGRAM_CACHE["ok"] = ok
    _RUNTIME_STATUS_TELEGRAM_CACHE["checked_at"] = now
    return ok


def _runtime_status_git_commit() -> str | None:
    return (
        os.getenv("GITHUB_SHA")
        or os.getenv("FLY_IMAGE_REF")
        or os.getenv("FLY_MACHINE_VERSION")
        or None
    )


@admin_bp.get("/api/admin/dashboard/runtime-status")
def dashboard_runtime_status():
    """Phase: Runtime Status. Read-only rollup of what is actually executing
    in production right now — scheduler jobs, PM automation, queues, and
    worker/infra health — reusing existing settings/lock/heartbeat state.
    Every status is computed at request time; nothing here is hardcoded."""
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    now = _utc_now()

    def _build():
        collections = {
            "scheduler_locks": scheduler_locks_collection,
            "audit_events": audit_events_collection,
            "admin_cache": admin_cache_col,
            "users": users_collection,
            "referral_notifications": referral_notifications_collection,
            "welcome_eligibility": welcome_eligibility_collection,
            "welcome_analytics_events": db["welcome_analytics_events"],
            "reactivation_journey": db["reactivation_journey"],
            "tg_verification_queue": tg_verification_queue_collection,
            "affiliate_ledger": affiliate_ledger_collection,
        }
        scheduler_settings = settings_service.get_settings("scheduler")
        feature_flags = settings_service.get_settings("feature_flags")
        referral_config = settings_service.get_settings("referral_config")

        scheduler_rows = _runtime_status.build_scheduler_health(
            collections, scheduler_settings, feature_flags, now
        )
        pm_rows = _runtime_status.build_pm_automation(
            collections, feature_flags, referral_config, now
        )
        queue_rows = _runtime_status.build_queue_status(collections, now)
        worker_health = _runtime_status.build_worker_health(
            collections,
            now,
            mongo_ping=_runtime_status_mongo_ping,
            telegram_get_me=_runtime_status_telegram_ping,
            deployment_version=MINIAPP_VERSION,
            git_commit=_runtime_status_git_commit(),
        )
        feature_rows = _runtime_status.build_feature_overview(
            scheduler_rows, pm_rows, queue_rows, worker_health, now
        )
        return {
            "generated_at": now.isoformat(),
            "features": feature_rows,
            "scheduler": scheduler_rows,
            "pm_automation": pm_rows,
            "queues": queue_rows,
            "worker_health": worker_health,
        }

    return _panel_cached("panel:runtime_status", _build)


@admin_bp.get("/api/admin/dashboard/backend-segment-engine")
def dashboard_backend_segment_engine():
    """Phase 6A: read-only summary of the shadow-mode backend segment engine.

    Reads ``backend_segment_snapshots`` only — never queries or writes
    ``users``, never touches segment classification, voucher allocation,
    public-pool probability, or reward logic. UIM remains production truth.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    period_type = (request.args.get("period_type") or "").strip().lower() or None
    period = (request.args.get("period") or "").strip() or None
    month = request.args.get("month") or None
    snapshot_week = request.args.get("snapshot_week") or None
    if period_type == "weekly" and period:
        snapshot_week = period
        month = None
    elif period_type == "monthly" and period:
        month = period
        snapshot_week = None
    elif period_type not in (None, "", "weekly", "monthly"):
        return jsonify({"success": False, "message": "period_type must be weekly or monthly"}), 400
    resolved_period_type = "weekly" if snapshot_week else "monthly"
    resolved_period = snapshot_week or month or "latest"
    now = _utc_now()
    cache_key = f"panel:backend_segment_engine:{resolved_period_type}:{resolved_period}"
    return _panel_cached(
        cache_key,
        lambda: _panels.build_backend_segment_engine_panel(
            snapshots_col=db["backend_segment_snapshots"],
            segment_snapshots_col=segment_snapshots_collection,
            now=now,
            month=month,
            snapshot_week=snapshot_week,
        ),
    )


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/available-periods")
def backend_segment_engine_available_periods():
    """Return distinct snapshot_week and snapshot_month values from segment_snapshots.

    Read-only. Does not affect UIM, bot segment, voucher allocation, or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    col = db["backend_segment_snapshots"]
    raw_weeks = sorted(
        (w for w in col.distinct("snapshot_week") if w),
        reverse=True,
    )
    raw_months = sorted(
        (m for m in col.distinct("snapshot_month") if m),
        reverse=True,
    )
    return jsonify({
        "ok": True,
        "snapshot_weeks": raw_weeks,
        "snapshot_months": raw_months,
    })


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/takeover-readiness")
def backend_segment_engine_takeover_readiness():
    """Phase 7A: read-only backend segment takeover migration report.

    Discovery/planning endpoint only. Does not write to users or snapshots and
    does not alter segment, voucher allocation, campaign, or reward behavior.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    snapshot_week = request.args.get("snapshot_week") or None
    cache_key = f"panel:backend_segment_engine:takeover_readiness:{snapshot_week or 'latest'}"
    return _panel_cached(
        cache_key,
        lambda: _panels.build_backend_segment_takeover_readiness_panel(
            users_col=users_collection,
            snapshots_col=db["backend_segment_snapshots"],
            snapshot_week=snapshot_week,
            now=_utc_now(),
        ),
    )


import re as _re
_SNAPSHOT_WEEK_RE = _re.compile(r"^\d{4}-W(?:0[1-9]|[1-4]\d|5[0-3])$")


def _bse_run_background(job_id: str, snapshot_week: str, dry_run: bool, admin_identity: str) -> None:
    """Background thread: run the segment engine and update job status in Mongo."""
    runs_col = db["backend_segment_engine_runs"]
    start_ts = datetime.now(timezone.utc)
    try:
        runs_col.update_one(
            {"job_id": job_id},
            {"$set": {"status": "running", "started_at": start_ts}},
        )
        import backend_segment_engine as _bse

        def _progress(rows_done: int, total: int) -> None:
            elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
            try:
                runs_col.update_one(
                    {"job_id": job_id},
                    {"$set": {
                        "rows_processed": rows_done,
                        "total_rows": total,
                        "elapsed_seconds": elapsed,
                        "last_progress_at": datetime.now(timezone.utc),
                    }},
                )
            except Exception:
                pass

        summary = _bse.run_shadow_segment_engine(
            snapshot_week=snapshot_week, dry_run=dry_run, progress_cb=_progress
        )

        if not dry_run and summary.get("ok"):
            for k in list(LEADERBOARD_CACHE.keys()):
                if k.startswith("panel:backend_segment_engine"):
                    LEADERBOARD_CACHE.pop(k, None)

        status = "success" if summary.get("ok") else "failed"
        elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
        runs_col.update_one(
            {"job_id": job_id},
            {"$set": {
                "status": status,
                "finished_at": datetime.now(timezone.utc),
                "elapsed_seconds": elapsed,
                "rows_processed": summary.get("rows_processed", 0),
                "total_rows": summary.get("total_rows", 0),
                "summary": {
                    "users_evaluated": summary.get("users_evaluated", 0),
                    "snapshots_written": summary.get("snapshots_written", 0),
                    "segment_distribution": summary.get("segment_distribution", {}),
                    "claim_risk_distribution": summary.get("claim_risk_distribution", {}),
                },
                "error": summary.get("error"),
            }},
        )
        logger.info(
            "[BSE_RUN] done job_id=%s admin=%s snapshot_week=%s dry_run=%s status=%s elapsed=%.1fs",
            job_id, admin_identity, snapshot_week, dry_run, status, elapsed,
        )
    except Exception as exc:
        logger.error("[BSE_RUN] background error job_id=%s err=%s", job_id, str(exc))
        try:
            elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
            runs_col.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "failed",
                    "finished_at": datetime.now(timezone.utc),
                    "elapsed_seconds": elapsed,
                    "error": str(exc),
                }},
            )
        except Exception:
            pass


@admin_bp.post("/api/admin/dashboard/backend-segment-engine/run")
def backend_segment_engine_run():
    """Phase 3C: Admin-triggered execution of the backend segment engine.

    Returns immediately with a job_id; the engine runs in a background thread.
    Poll GET /run-status?job_id=... for completion.

    Shadow mode only — writes to backend_segment_snapshots, never to
    users.bot_segment, voucher allocation, or reward logic.

    POST body: {"snapshot_week": "YYYY-Www"|null, "dry_run": true|false}

    snapshot_week is optional. When omitted or null, all uploaded marketing rows
    across all periods are processed and each snapshot's period is derived from
    the row's own coupon_redeem_time-based snapshot_week field.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    body = request.get_json(silent=True) or {}
    snapshot_week = (body.get("snapshot_week") or "").strip() or None
    dry_run = bool(body.get("dry_run", True))

    # Validate format only when a value is explicitly provided.
    if snapshot_week and not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week format '{snapshot_week}'. Expected YYYY-Www (e.g. 2026-W25)."}), 400

    # Fail fast if no marketing data exists for the given filter (or at all).
    mkt_query = {"snapshot_week": snapshot_week} if snapshot_week else {}
    mkt_count = db["marketing_raw_data"].count_documents(mkt_query)
    if mkt_count == 0:
        scope = f"snapshot_week '{snapshot_week}'" if snapshot_week else "any snapshot_week"
        return jsonify({"ok": False, "error": f"No marketing_raw_data found for {scope}. Upload data first."}), 422

    admin_identity = _current_admin_identity()

    # Jobs stuck queued/running for longer than this are assumed stranded (web
    # worker recycled before the thread could finish) and are expired on the
    # next POST so the admin can re-run without manual intervention.
    _BSE_JOB_STALE_S = 900  # 15 minutes
    runs_col = db["backend_segment_engine_runs"]
    stale_cutoff = datetime.now(timezone.utc) - timedelta(seconds=_BSE_JOB_STALE_S)

    # Expire any stale in-progress jobs for this (snapshot_week, dry_run).
    runs_col.update_many(
        {
            "snapshot_week": snapshot_week,
            "dry_run": dry_run,
            "status": {"$in": ["queued", "running"]},
            "queued_at": {"$lte": stale_cutoff},
        },
        {"$set": {
            "status": "failed",
            "finished_at": datetime.now(timezone.utc),
            "error": "Job timed out — web worker was recycled before completion. Re-run to retry.",
        }},
    )

    # Reject duplicate fresh in-progress job for the same (snapshot_week, dry_run).
    existing = runs_col.find_one({
        "snapshot_week": snapshot_week,
        "dry_run": dry_run,
        "status": {"$in": ["queued", "running"]},
        "queued_at": {"$gt": stale_cutoff},
    })
    if existing:
        return jsonify({
            "ok": False,
            "error": (
                f"A {'dry run' if dry_run else 'commit run'} for "
                f"{snapshot_week or 'all periods'} "
                f"is already in progress (job_id={existing['job_id']})."
            ),
            "job_id": existing["job_id"],
        }), 409

    job_id = str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc)
    runs_col.insert_one({
        "job_id": job_id,
        "admin_user": admin_identity,
        "snapshot_week": snapshot_week,
        "dry_run": dry_run,
        "status": "queued",
        "queued_at": now_ts,
        "started_at": None,
        "finished_at": None,
        "total_rows": 0,
        "rows_processed": 0,
        "elapsed_seconds": 0.0,
        "last_progress_at": None,
        "summary": None,
        "error": None,
    })
    logger.info(
        "[BSE_RUN] queued job_id=%s admin=%s snapshot_week=%s dry_run=%s mkt_rows=%d",
        job_id, admin_identity, snapshot_week, dry_run, mkt_count,
    )

    # Non-daemon so gunicorn's graceful shutdown waits for the thread to finish
    # rather than killing it mid-run (hard SIGKILL is still unrecoverable, but
    # the 15-minute stale-expiry above handles that case on the next POST).
    t = Thread(target=_bse_run_background, args=(job_id, snapshot_week, dry_run, admin_identity), daemon=False)
    t.start()

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "status": "queued",
        "snapshot_week": snapshot_week,
        "dry_run": dry_run,
    })


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/run-status")
def backend_segment_engine_run_status():
    """Phase 3C: Poll status of an async backend segment engine job.

    GET ?job_id=<uuid>
    Returns job document with status, summary, and error fields.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    job_id = request.args.get("job_id", "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "job_id is required"}), 400

    doc = db["backend_segment_engine_runs"].find_one({"job_id": job_id}, {"_id": 0})
    if doc is None:
        return jsonify({"ok": False, "error": f"Job not found: {job_id}"}), 404

    for field in ("queued_at", "started_at", "finished_at", "last_progress_at"):
        if isinstance(doc.get(field), datetime):
            doc[field] = doc[field].isoformat()

    return jsonify({"ok": True, **doc})


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/uim-comparison")
def backend_segment_engine_uim_comparison():
    """Phase 5: Backend vs UIM segment comparison analysis.

    GET ?snapshot_week=YYYY-Www
        &backend_segment=<optional filter>
        &uim_segment=<optional filter>
        &match=true|false (optional; omit for all)
        &claim_risk_level=<optional filter>
        &page=1 &per_page=200

    Returns comparison summary, mismatch matrix, paginated detail rows,
    and rule audit (per-segment avg metrics). Shadow mode — read-only.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    filter_backend_segment = request.args.get("backend_segment", "").strip() or None
    filter_uim_segment     = request.args.get("uim_segment", "").strip() or None
    filter_claim_risk      = request.args.get("claim_risk_level", "").strip() or None

    match_param = request.args.get("match", "").strip().lower()
    filter_match: bool | None = None
    if match_param == "true":
        filter_match = True
    elif match_param == "false":
        filter_match = False

    try:
        page     = max(1, int(request.args.get("page", 1)))
        per_page = max(1, min(500, int(request.args.get("per_page", 200))))
    except (TypeError, ValueError):
        page, per_page = 1, 200

    return jsonify(_panels.build_uim_comparison_panel(
        snapshots_col=db["backend_segment_snapshots"],
        segment_snapshots_col=db["segment_snapshots"],
        snapshot_week=snapshot_week,
        filter_backend_segment=filter_backend_segment,
        filter_uim_segment=filter_uim_segment,
        filter_match=filter_match,
        filter_claim_risk_level=filter_claim_risk,
        page=page,
        per_page=per_page,
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/uim-comparison/export")
def backend_segment_engine_uim_comparison_export():
    """Phase 5: Export uim-comparison detail rows as CSV (all pages, up to 10 000 rows).

    Accepts same filter params as /uim-comparison (except page/per_page).
    Returns CSV with Content-Disposition: attachment.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    filter_backend_segment = request.args.get("backend_segment", "").strip() or None
    filter_uim_segment     = request.args.get("uim_segment", "").strip() or None
    filter_claim_risk      = request.args.get("claim_risk_level", "").strip() or None

    match_param = request.args.get("match", "").strip().lower()
    filter_match: bool | None = None
    if match_param == "true":
        filter_match = True
    elif match_param == "false":
        filter_match = False

    result = _panels.build_uim_comparison_panel(
        snapshots_col=db["backend_segment_snapshots"],
        segment_snapshots_col=db["segment_snapshots"],
        snapshot_week=snapshot_week,
        filter_backend_segment=filter_backend_segment,
        filter_uim_segment=filter_uim_segment,
        filter_match=filter_match,
        filter_claim_risk_level=filter_claim_risk,
        page=1,
        per_page=10_000,
    )

    _CSV_FIELDS = [
        "account", "backend_segment", "uim_segment", "match",
        "confidence", "reason",
        "after_total_bet_amount", "withdraw_amount",
        "claim_count", "referral_count", "checkin_count",
        "player_age_type", "claim_risk_level",
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in result.get("details", []):
        writer.writerow({k: ("" if row.get(k) is None else row[k]) for k in _CSV_FIELDS})

    filename = f"uim_comparison_{snapshot_week}.csv"
    from flask import Response as _FlaskResponse
    return _FlaskResponse(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/voucher-hunter-mismatch-audit")
def backend_segment_engine_voucher_hunter_mismatch_audit():
    """Phase 5B: read-only audit of users where uim_segment=voucher_hunter
    but backend classified them differently.

    GET ?snapshot_week=YYYY-Www&sample_limit=20

    Returns segment breakdown, summary table, and per-group sample users.
    Never writes, never modifies segments or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    try:
        sample_limit = max(1, min(100, int(request.args.get("sample_limit", 20))))
    except (TypeError, ValueError):
        sample_limit = 20

    return jsonify(_panels.build_voucher_hunter_mismatch_audit(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        sample_limit=sample_limit,
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/vh-priority-impact")
def backend_segment_engine_vh_priority_impact():
    """Phase 7C: read-only simulation of promoting VH above Low Value.

    GET ?snapshot_week=YYYY-Www&candidate_limit=200

    Returns summary, migration breakdown, low-value impact, extreme-case
    count, candidate table, and decision metrics.
    Never writes, never modifies segments or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    try:
        candidate_limit = max(1, min(500, int(request.args.get("candidate_limit", 200))))
    except (TypeError, ValueError):
        candidate_limit = 200

    return jsonify(_panels.build_vh_priority_impact(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        candidate_limit=candidate_limit,
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/voucher-hunter-rule-simulator")
def backend_segment_engine_voucher_hunter_rule_simulator():
    """Phase 6A: read-only simulation of a refined voucher_hunter rule.

    GET ?snapshot_week=YYYY-Www
        &claim_threshold=10&after_bet_threshold=100&referral_threshold=20
        &withdrawal_protection=true&high_bet_protection=true

    Simulates rule in memory only. Never writes, never modifies segments.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    def _int(key, default):
        try:
            return int(request.args.get(key, default))
        except (TypeError, ValueError):
            return default

    def _float(key, default):
        try:
            return float(request.args.get(key, default))
        except (TypeError, ValueError):
            return default

    def _bool(key, default):
        v = request.args.get(key, "").lower()
        if v in ("true", "1", "yes"):
            return True
        if v in ("false", "0", "no"):
            return False
        return default

    return jsonify(_panels.build_voucher_hunter_rule_simulator(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        claim_threshold=_int("claim_threshold", 10),
        after_bet_threshold=_float("after_bet_threshold", 100.0),
        referral_threshold=_int("referral_threshold", 20),
        withdrawal_protection=_bool("withdrawal_protection", True),
        high_bet_protection=_bool("high_bet_protection", True),
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/voucher-hunter-false-positive-analysis")
def backend_segment_engine_voucher_hunter_false_positive_analysis():
    """Phase 5E-FP: false-positive analysis for uim_segment=voucher_hunter.

    GET ?snapshot_week=YYYY-Www&top_n=50

    Returns after_bet/withdrawal/referral/checkin distributions, top-N false
    positive candidates, and a backend-segment evidence matrix.
    Never writes, never modifies segments or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    try:
        top_n = max(1, min(200, int(request.args.get("top_n", 50))))
    except (TypeError, ValueError):
        top_n = 50

    return jsonify(_panels.build_voucher_hunter_false_positive_analysis(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        top_n=top_n,
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/voucher-hunter-quality-analysis")
def backend_segment_engine_voucher_hunter_quality_analysis():
    """Phase 5E: read-only analysis of users where uim_segment=voucher_hunter.

    GET ?snapshot_week=YYYY-Www&top_n=20

    Groups by backend_segment with avg metrics, top-N lists by claims/after_bet/
    referrals, and claim threshold breakdown to determine UIM over-classification.
    Never writes, never modifies segments or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    try:
        top_n = max(1, min(100, int(request.args.get("top_n", 20))))
    except (TypeError, ValueError):
        top_n = 20

    return jsonify(_panels.build_voucher_hunter_quality_analysis(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        top_n=top_n,
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/segment-rule-simulator")
def backend_segment_engine_segment_rule_simulator():
    """Phase 5D: read-only simulation of segment rule changes.

    GET ?snapshot_week=YYYY-Www
        &ghost_max_checkins=0&ghost_max_referrals=0&ghost_max_claims=0
        &vh_min_claims=3&vh_max_after_bet=0&vh_max_checkins=9999
        &ac_min_checkins=14&ac_min_referrals=1

    Reclassifies backend_segment_snapshots in memory using the provided
    thresholds and returns a distribution comparison, match rate impact,
    top segment movements, and production impact summary.
    Never writes, never modifies segments or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    def _int(key: str, default: int) -> int:
        try:
            return int(request.args.get(key, default))
        except (TypeError, ValueError):
            return default

    def _float(key: str, default: float) -> float:
        try:
            return float(request.args.get(key, default))
        except (TypeError, ValueError):
            return default

    return jsonify(_panels.build_segment_rule_simulator(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        ghost_max_checkins=_int("ghost_max_checkins", 0),
        ghost_max_referrals=_int("ghost_max_referrals", 0),
        ghost_max_claims=_int("ghost_max_claims", 0),
        vh_min_claims=_int("vh_min_claims", 3),
        vh_max_after_bet=_float("vh_max_after_bet", 0.0),
        vh_max_checkins=_int("vh_max_checkins", 9999),
        ac_min_checkins=_int("ac_min_checkins", 14),
        ac_min_referrals=_int("ac_min_referrals", 1),
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/unclassified-audit")
def backend_segment_engine_unclassified_audit():
    """Phase 5C: read-only audit explaining why users fell into 'unclassified'.

    GET ?snapshot_week=YYYY-Www&sample_limit=20

    Returns summary KPIs, claim risk breakdown, activity buckets, top reasons,
    and sample users per activity bucket.
    Never writes, never modifies segments or rewards.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"ok": False, "error": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"ok": False, "error": "snapshot_week is required"}), 400
    if not _SNAPSHOT_WEEK_RE.match(snapshot_week):
        return jsonify({"ok": False, "error": f"Invalid snapshot_week '{snapshot_week}'"}), 400

    try:
        sample_limit = max(1, min(100, int(request.args.get("sample_limit", 20))))
    except (TypeError, ValueError):
        sample_limit = 20

    return jsonify(_panels.build_unclassified_audit(
        snapshots_col=db["backend_segment_snapshots"],
        snapshot_week=snapshot_week,
        sample_limit=sample_limit,
    ))


@admin_bp.get("/api/admin/dashboard/backend-segment-engine/identity-match-audit")
def backend_segment_engine_identity_match_audit():
    """Identity resolution audit: coupon_code → voucher_claims.voucher_code → user_id.

    Queries live from marketing_raw_data + voucher_claims for the given snapshot_week.
    Returns total / matched / unmatched / match_rate_pct plus up to 20 sample rows.
    Read-only — never writes anything.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    snapshot_week = request.args.get("snapshot_week", "").strip()
    if not snapshot_week:
        return jsonify({"success": False, "message": "snapshot_week is required"}), 400

    marketing_col = db["marketing_raw_data"]
    vc_col = db["voucher_claims"]

    def _get_coupon(doc: dict) -> str | None:
        for key in ("coupon_code", "Coupon_Code", "COUPON_CODE"):
            v = doc.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
        for k, v in doc.items():
            if k.lower().replace(" ", "_") == "coupon_code" and v is not None and str(v).strip():
                return str(v).strip()
        return None

    def _get_account(doc: dict) -> str:
        for key in ("account", "Account", "ACCOUNT"):
            v = doc.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
        for k, v in doc.items():
            if k.lower() == "account" and v is not None and str(v).strip():
                return str(v).strip()
        return ""

    marketing_proj = {"_id": 0, "account": 1, "Account": 1, "ACCOUNT": 1,
                      "coupon_code": 1, "Coupon_Code": 1, "COUPON_CODE": 1}
    marketing_docs = list(marketing_col.find({"snapshot_week": snapshot_week}, marketing_proj))
    total_rows = len(marketing_docs)

    all_coupons = [c for c in (_get_coupon(d) for d in marketing_docs) if c]
    _BATCH = 500
    coupon_to_user: dict[str, int] = {}
    for i in range(0, len(all_coupons), _BATCH):
        batch = all_coupons[i: i + _BATCH]
        for claim in vc_col.find(
            {"voucher_code": {"$in": batch}, "user_id": {"$ne": None}},
            {"_id": 0, "voucher_code": 1, "user_id": 1},
        ):
            code = claim.get("voucher_code")
            uid = claim.get("user_id")
            if code and uid is not None:
                coupon_to_user[code] = uid

    matched_rows: list[dict] = []
    unmatched_rows: list[dict] = []
    for doc in marketing_docs:
        acct = _get_account(doc)
        coupon = _get_coupon(doc)
        uid = coupon_to_user.get(coupon) if coupon else None
        row = {
            "account": acct,
            "coupon_code": coupon,
            "user_id": str(uid) if uid is not None else None,
        }
        if uid is not None:
            matched_rows.append(row)
        else:
            unmatched_rows.append(row)

    matched = len(matched_rows)
    unmatched = len(unmatched_rows)

    return jsonify({
        "ok": True,
        "snapshot_week": snapshot_week,
        "total_rows": total_rows,
        "matched_rows": matched,
        "unmatched_rows": unmatched,
        "identity_match_rate": round(matched / total_rows * 100, 2) if total_rows else 0.0,
        "sample_matched": matched_rows[:20],
        "sample_unmatched": unmatched_rows[:20],
    })


@admin_bp.get("/api/admin/dashboard/kpi-gap-report")
def dashboard_kpi_gap_report():
    """Phase 5B: read-only UIM formula mapping / backend KPI gap report.

    Pure documentation endpoint — does not query any collection, does not
    fetch the live UIM sheet, and never touches segment classification,
    voucher allocation, public-pool probability or reward logic. Explains
    *why* each Validation-page metric is red/gray, not just that it is.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    now = _utc_now()
    return _panel_cached("panel:kpi_gap_report", lambda: _panels.build_kpi_gap_report_panel(now=now))


import marketing_upload as _marketing_upload  # noqa: E402


def _current_admin_identity() -> str:
    """Best-effort admin identity for ``uploaded_by`` — never raises."""
    try:
        from admin_auth import session_admin

        admin = session_admin()
        if admin and admin.get("username"):
            return str(admin["username"])
    except Exception:
        pass
    return "admin"


@admin_bp.post("/api/admin/data/upload-player-performance")
def upload_player_performance():
    """Phase 2A: weekly Marketing raw-data upload (CSV/XLSX), ingestion only.

    Stores every uploaded column verbatim into ``marketing_raw_data`` (one
    weekly snapshot per upload, never overwritten) plus an audit row into
    ``marketing_upload_batches``. Does not calculate segments, does not
    touch ``users.bot_segment``/``for_bot_segment``, and does not change
    bot/voucher/reward behaviour in any way.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    upload = request.files.get("file")
    if upload is None or not upload.filename:
        return jsonify({"success": False, "message": "missing file"}), 400

    content = upload.read()
    manual_period = (
        request.form.get("manual_period")
        or request.form.get("period")
        or request.form.get("snapshot_period")
        or None
    )
    summary = _marketing_upload.ingest_upload(
        content=content,
        file_name=upload.filename,
        uploaded_by=_current_admin_identity(),
        now=_utc_now(),
        manual_period=manual_period,
    )
    if not summary.get("ok"):
        return jsonify({"success": False, "message": summary.get("error") or "upload failed", **summary}), 400
    return jsonify({"success": True, **summary})


@admin_bp.get("/api/admin/data/upload-history")
def data_upload_history():
    """Phase 2A: most recent weekly marketing-data upload batches."""
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    limit = min(max(int(request.args.get("limit", 50) or 50), 1), 200)
    batches = _marketing_upload.get_upload_history(limit=limit)
    for b in batches:
        b["_id"] = str(b.get("_id"))
        if b.get("uploaded_at") is not None:
            b["uploaded_at"] = b["uploaded_at"].isoformat() if hasattr(b["uploaded_at"], "isoformat") else b["uploaded_at"]
    return jsonify({"success": True, "batches": batches})


import marketing_explorer as _marketing_explorer  # noqa: E402


@admin_bp.get("/api/admin/data/raw-explorer")
def data_raw_explorer():
    """Phase 2B: read-only validation and exploration of uploaded marketing data.

    Returns summary cards, campaign/platform/currency breakdowns, upload snapshot
    history, and data-quality checks for the marketing_raw_data collection.

    Does NOT calculate segments, does NOT touch users.bot_segment or
    for_bot_segment, and does NOT modify bot/voucher/reward behaviour.

    Query parameters:
        snapshot_week  – ISO week e.g. ``2024-W20`` (takes precedence over month).
        snapshot_month – ISO month e.g. ``2024-05``.
        If neither is provided the latest uploaded snapshot is used.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    snapshot_week = (request.args.get("snapshot_week") or "").strip() or None
    snapshot_month = (request.args.get("snapshot_month") or "").strip() or None
    period_type = (request.args.get("period_type") or "").strip().lower() or None
    period = (request.args.get("period") or "").strip() or None
    if period_type not in (None, "weekly", "monthly"):
        return jsonify({"success": False, "message": "period_type must be weekly or monthly"}), 400

    try:
        payload = _marketing_explorer.get_raw_explorer(
            snapshot_week=snapshot_week,
            snapshot_month=snapshot_month,
            period_type=period_type,
            period=period,
        )
        return jsonify({"success": True, **payload})
    except Exception as exc:
        logger.exception("[RAW_EXPLORER] unexpected error: %s", exc)
        return jsonify({"success": False, "message": "explorer query failed"}), 500


@admin_bp.get("/api/admin/dashboard/vouchers")
def dashboard_vouchers():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    window = _panels._normalize_dashboard_window(request.args.get("window"))
    return _panel_cached(
        f"panel:vouchers:{window}",
        lambda: _panels.build_vouchers_panel(
            drops_col=db["drops"],
            vouchers_col=db["vouchers"],
            voucher_claims_col=db["voucher_claims"],
            welcome_eligibility_col=welcome_eligibility_collection,
            now=_utc_now(),
            window=window,
        ),
    )


@admin_bp.get("/api/admin/dashboard/welcome-journey")
def dashboard_welcome_journey():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    window = _panels._normalize_dashboard_window(request.args.get("window"))
    return _panel_cached(
        f"panel:welcome_journey:{window}",
        lambda: _panels.build_welcome_journey_panel(
            welcome_eligibility_col=welcome_eligibility_collection,
            welcome_analytics_events_col=db["welcome_analytics_events"],
            now=_utc_now(),
            window=window,
        ),
    )


@admin_bp.get("/api/admin/dashboard/welcome-journey-runtime")
def dashboard_welcome_journey_runtime():
    """Welcome Journey Runtime (observability only). Rolls up scheduler
    health (reusing Runtime Status' scheduler-health builder), the latest
    persisted reminder-run stats, recent runs, the existing Welcome Journey
    funnel panel, and derived alerts. Read-only; never touches reminder
    timing, eligibility, voucher rules, XP or scheduler behaviour."""
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    window = _panels._normalize_dashboard_window(request.args.get("window"))
    now = _utc_now()

    def _build():
        collections = {
            "scheduler_locks": scheduler_locks_collection,
            "admin_cache": admin_cache_col,
        }
        scheduler_settings = settings_service.get_settings("scheduler")
        feature_flags = settings_service.get_settings("feature_flags")
        scheduler_rows = _runtime_status.build_scheduler_health(collections, scheduler_settings, feature_flags, now)
        scheduler = _runtime_status.build_welcome_journey_scheduler(collections, scheduler_rows, now)
        last_run = _runtime_status.build_welcome_journey_last_run(collections)
        recent_runs = _runtime_status.build_welcome_journey_recent_runs(collections)

        funnel_panel = _panels.build_welcome_journey_panel(
            welcome_eligibility_col=welcome_eligibility_collection,
            welcome_analytics_events_col=db["welcome_analytics_events"],
            now=now,
            window=window,
        )
        funnel_summary = funnel_panel.get("summary", {})

        alerts = _runtime_status.build_welcome_journey_alerts(
            now, scheduler=scheduler, last_run=last_run, funnel_summary=funnel_summary,
        )

        return {
            "generated_at": now.isoformat(),
            "scheduler": scheduler,
            "last_run": last_run,
            "recent_runs": recent_runs,
            "funnel": funnel_panel,
            "alerts": alerts,
        }

    return _panel_cached(f"panel:welcome_journey_runtime:{window}", _build)


@admin_bp.get("/api/admin/dashboard/referrals")
def dashboard_referrals():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    window = _panels._normalize_referral_window(request.args.get("window"))
    return _panel_cached(
        f"panel:referrals:{window}",
        lambda: _panels.build_referrals_panel(
            pending_referrals_col=pending_referrals_collection,
            qualified_events_col=qualified_events_collection,
            users_col=users_collection,
            welcome_eligibility_col=welcome_eligibility_collection,
            now=_utc_now(),
            window=window,
        ),
    )


@admin_bp.get("/api/admin/dashboard/referrals/detail")
def dashboard_referrals_detail():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    try:
        referrer_id = int(request.args.get("user_id", ""))
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "user_id required"}), 400
    return jsonify(
        _panels.build_referral_detail(
            referrer_id=referrer_id,
            pending_referrals_col=pending_referrals_collection,
            users_col=users_collection,
            welcome_eligibility_col=welcome_eligibility_collection,
            now=_utc_now(),
        )
    )


@admin_bp.get("/api/admin/dashboard/affiliate")
def dashboard_affiliate():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    return _panel_cached(
        "panel:affiliate",
        lambda: _panels.build_affiliate_panel(
            affiliate_ledger_col=affiliate_ledger_collection,
            voucher_pools_col=voucher_pools_collection,
            now=_utc_now(),
        ),
    )


@admin_bp.get("/api/admin/dashboard/affiliate/detail")
def dashboard_affiliate_detail():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    try:
        user_id = int(request.args.get("user_id", ""))
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "user_id required"}), 400
    return jsonify(
        _panels.build_affiliate_detail(
            user_id=user_id,
            affiliate_ledger_col=affiliate_ledger_collection,
            now=_utc_now(),
        )
    )


@admin_bp.get("/api/admin/dashboard/audit")
def dashboard_audit():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    return _panel_cached(
        "panel:audit",
        lambda: _panels.build_audit_panel(
            admin_login_audit_col=db["admin_login_audit"],
            audit_events_col=audit_events_collection,
            referral_audit_col=referral_audit_collection,
            admin_cache_col=admin_cache_col,
            now=_utc_now(),
        ),
    )


@admin_bp.get("/api/admin/dashboard/user")
def dashboard_user_drilldown():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    query = request.args.get("query") or request.args.get("q") or ""
    return jsonify(
        _panels.build_user_drilldown(
            query=query,
            users_col=users_collection,
            welcome_eligibility_col=welcome_eligibility_collection,
            voucher_claims_col=db["voucher_claims"],
            affiliate_ledger_col=affiliate_ledger_collection,
            pending_referrals_col=pending_referrals_collection,
            qualified_events_col=qualified_events_collection,
            now=_utc_now(),
        )
    )


@admin_bp.get("/api/admin/dashboard/settings")
def dashboard_settings():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    import config as _cfg

    constants = {
        "XP_BASE_PER_CHECKIN": getattr(_cfg, "XP_BASE_PER_CHECKIN", None),
        "FIRST_CHECKIN_BONUS": getattr(_cfg, "FIRST_CHECKIN_BONUS", None),
        "STREAK_MILESTONES": getattr(_cfg, "STREAK_MILESTONES", None),
        "STREAK_FREEZE_DEFAULT_TOKENS": getattr(_cfg, "STREAK_FREEZE_DEFAULT_TOKENS", None),
        "STREAK_FREEZE_MAX_TOKENS": getattr(_cfg, "STREAK_FREEZE_MAX_TOKENS", None),
        "WEEKLY_XP_BUCKET": getattr(_cfg, "WEEKLY_XP_BUCKET", None),
        "WEEKLY_REFERRAL_BUCKET": getattr(_cfg, "WEEKLY_REFERRAL_BUCKET", None),
        "GROUP_ID": GROUP_ID,
        "OFFICIAL_CHANNEL_ID": OFFICIAL_CHANNEL_ID,
        "COMMUNITY_CHAT_ID": _COMMUNITY_CHAT_ID,
        "CHANNEL_USERNAME": CHANNEL_USERNAME,
        "MINIAPP_VERSION": getattr(_cfg, "MINIAPP_VERSION", None),
    }
    return jsonify(_panels.build_settings_panel(os.environ, constants=constants))


def _admin_identity_for_settings() -> str:
    try:
        from admin_auth import session_admin
        admin = session_admin()
        if admin:
            return admin.get("username") or str(admin.get("id") or "admin")
    except Exception:
        pass
    return "admin"


@admin_bp.get("/api/admin/settings/schema")
def admin_settings_schema():
    """Field-level schema (labels/types/defaults/bounds) for every managed
    settings group — drives the auto-generated Settings UI."""
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    return jsonify({"success": True, "schema": list_settings_schema()})


@admin_bp.get("/api/admin/settings")
def admin_settings_all():
    """Current values (Mongo -> env -> default, cached) for every managed
    settings group, alongside the schema, for the dashboard Settings page."""
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    return jsonify({
        "success": True,
        "schema": list_settings_schema(),
        "settings": get_all_app_settings(),
    })


@admin_bp.get("/api/admin/settings/<group>")
def admin_settings_get(group: str):
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    if group not in list_settings_schema():
        return jsonify({"success": False, "message": "unknown_group"}), 404
    return jsonify({"success": True, "group": group, "settings": get_app_settings(group)})


@admin_bp.post("/api/admin/settings/<group>")
def admin_settings_update(group: str):
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    if group not in list_settings_schema():
        return jsonify({"success": False, "message": "unknown_group"}), 404
    payload = request.get_json(silent=True) or {}
    updates = payload.get("settings") if isinstance(payload.get("settings"), dict) else payload
    if not isinstance(updates, dict):
        return jsonify({"success": False, "message": "bad_payload"}), 400
    result = update_app_settings(group, updates, updated_by=_admin_identity_for_settings())
    if not result.get("success"):
        return jsonify(result), 400
    return jsonify(result)


@admin_bp.get("/api/admin/settings/audit-log")
def admin_settings_audit_log():
    """Recent settings change history: admin, group, changed fields, old/new
    values, timestamp. Read-only."""
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    group = request.args.get("group")
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
    except (TypeError, ValueError):
        limit = 50
    query = {"group": group} if group else {}
    entries = list(
        db[settings_service.AUDIT_COLLECTION_NAME]
        .find(query)
        .sort("created_at", -1)
        .limit(limit)
    )
    for entry in entries:
        entry["_id"] = str(entry["_id"])
        if entry.get("created_at"):
            entry["created_at"] = entry["created_at"].isoformat()
    return jsonify({"success": True, "entries": entries})


@admin_bp.get("/api/admin/dashboard/segment-probability-config")
def dashboard_segment_probability_config():
    """Read-only panel: segment probability configuration.

    Returns the active SEGMENT_PROBABILITY_CONFIG so admins can verify which
    probability is applied per backend segment. No editing — configuration
    lives in config.py.
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    import config as _cfg

    rows = []
    for seg, pct in _cfg.SEGMENT_PROBABILITY_CONFIG.items():
        rows.append({
            "segment": seg,
            "probability_pct": pct,
            "description": _cfg.SEGMENT_PROBABILITY_DESCRIPTIONS.get(seg, ""),
        })
    return jsonify({
        "success": True,
        "rows": rows,
        "new_player_override": {
            "probability_pct": 100,
            "condition": "player_age_type == new_player AND assignment_count < 3",
            "description": "First 3 eligible SVD/public opportunities for new players",
        },
        "source": "config.SEGMENT_PROBABILITY_CONFIG",
    })


@admin_bp.get("/api/admin/dashboard/segment-roi")
def dashboard_segment_roi():
    """Segment ROI Dashboard — read-only aggregation of backend_segment_snapshots.

    Aggregates per-segment metrics (bet amount, claims, withdrawals, referrals,
    check-ins) and computes derived ROI metrics.  Never writes, never classifies
    users, never touches voucher/reward/public-pool logic.

    Query params:
      snapshot_month  – YYYY-MM  (default: current month)
      snapshot_week   – YYYY-Www (overrides snapshot_month when supplied)
      trend_months    – int 1-12 (default 3)
    """
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    snapshot_week = (request.args.get("snapshot_week") or "").strip() or None
    snapshot_month = (request.args.get("snapshot_month") or "").strip() or None
    try:
        trend_months = max(1, min(12, int(request.args.get("trend_months") or 3)))
    except (ValueError, TypeError):
        trend_months = 3

    cache_key = (
        f"panel:segment_roi:{snapshot_week or ''}:{snapshot_month or ''}:{trend_months}"
    )
    return _panel_cached(
        cache_key,
        lambda: _panels.build_segment_roi_panel(
            snapshots_col=db["backend_segment_snapshots"],
            snapshot_month=snapshot_month,
            snapshot_week=snapshot_week,
            now=_utc_now(),
            trend_months=trend_months,
        ),
    )


import uim_import as _uim_import


@admin_bp.post("/api/admin/data/uim-import/commit")
def uim_import_commit():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    body = request.get_json(silent=True) or {}
    rows = body.get("rows")
    if not rows or not isinstance(rows, list):
        return jsonify({"success": False, "message": "rows required"}), 400
    try:
        result = _uim_import.commit_batch(rows)
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500
    _uim_import.trigger_seg_sync_background(result["batch_id"])
    return jsonify({"success": True, **result})


@admin_bp.get("/api/admin/data/uim-import/history")
def uim_import_history():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    limit = request.args.get("limit", default=50, type=int) or 50
    batches = _uim_import.get_import_history(limit=limit)
    return jsonify({"success": True, "batches": batches})


@admin_bp.post("/api/admin/data/uim-import/<batch_id>/resync")
def uim_import_resync(batch_id: str):
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code
    result = _uim_import.run_seg_sync(batch_id)
    return jsonify({"success": result.get("ok", False), **result})


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
    connect_timeout=15,
    read_timeout=65,          # must be > polling timeout
    write_timeout=30,
    pool_timeout=20,
    connection_pool_size=16,  # increase pool stability
)
app_bot = ApplicationBuilder().token(BOT_TOKEN).request(httpx_request).build()


@app.route("/api/is_admin")
def api_is_admin():
    try:
        admin_secret = _get_admin_secret(request)
        if _admin_secret_ok(admin_secret):
            return jsonify({"success": True, "is_admin": True, "source": "secret"})

        # Phase B: browser Telegram Login session
        try:
            from admin_auth import session_admin
            if session_admin():
                return jsonify({"success": True, "is_admin": True, "source": "session"})
        except Exception:
            pass

        init_data = extract_raw_init_data_from_query(request)
        if not init_data:
            return jsonify({"success": False, "is_admin": False, "error": "Missing init_data"}), 400

        ok, parsed, _ = verify_telegram_init_data(init_data)
        if not ok:
            return jsonify({"success": False, "is_admin": False, "error": "Admins only"}), 403

        user_payload = (parsed or {}).get("user", {})
        if isinstance(user_payload, str):
            try:
                user_payload = json.loads(user_payload)
            except Exception:
                user_payload = {}

        user_id = int((user_payload or {}).get("id"))

        doc = admin_cache_col.find_one({"_id": "admins"}) or {}
        ids = set()
        for raw in doc.get("ids", []):
            try:
                ids.add(int(raw))
            except (TypeError, ValueError):
                continue
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
        return jsonify({"success": False, "is_admin": False, "error": str(e)}), 500




@app.route("/api/me/identity", methods=["GET"])
def api_me_identity():
    try:
        init_data = extract_raw_init_data_from_query(request)
        if not init_data:
            return jsonify({"success": False, "error": "Missing init_data"}), 400
        ok, parsed, _ = verify_telegram_init_data(init_data)
        if not ok:
            return jsonify({"success": False, "error": "Unauthorized"}), 403
        user_payload = (parsed or {}).get("user", {})
        if isinstance(user_payload, str):
            try:
                user_payload = json.loads(user_payload)
            except Exception:
                user_payload = {}
        user_id = int((user_payload or {}).get("id"))
    except Exception:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    user_doc = users_collection.find_one(
        {"user_id": user_id},
        {
            "user_id": 1,
            "username": 1,
            "display_name": 1,
            "name": 1,
            "first_name": 1,
            "last_name": 1,
            "weekly_xp": 1,
            "monthly_xp": 1,
            "total_xp": 1,
            "lifetime_xp": 1,
            "xp": 1,
            "weekly_referrals": 1,
            "monthly_referrals": 1,
            "total_referrals": 1,
            "lifetime_referrals": 1,
            "streak": 1,
            "streak_days": 1,
            "checkin_streak": 1,
            "streak_freeze_tokens": 1,
            "vip_tier": 1,
            "status": 1,
            "updated_at": 1,
        },
    ) or {}
    display_name = (
        user_doc.get("display_name")
        or user_doc.get("name")
        or user_doc.get("first_name")
        or user_doc.get("username")
        or user_payload.get("username")
        or "User"
    )
    total_xp = _safe_non_negative_int(user_doc.get("total_xp", user_doc.get("lifetime_xp", user_doc.get("xp", 0))))
    total_referrals = _safe_non_negative_int(user_doc.get("total_referrals", user_doc.get("lifetime_referrals", 0)))
    derived_tier = derive_identity_tier(total_referrals, total_xp)
    next_tier_name, next_tier_progress_pct, next_tier_hint = compute_next_tier_progress(
        total_referrals,
        total_xp,
        derived_tier["name"],
    )
    weekly_xp = _safe_non_negative_int(user_doc.get("weekly_xp", 0))
    weekly_referrals = _safe_non_negative_int(user_doc.get("weekly_referrals", 0))
    streak_freeze_tokens = _safe_non_negative_int(user_doc.get("streak_freeze_tokens", 0))
    if streak_freeze_tokens > STREAK_FREEZE_MAX_TOKENS:
        streak_freeze_tokens = STREAK_FREEZE_MAX_TOKENS
    weekly_rank = compute_weekly_rank(user_id, weekly_xp, weekly_referrals, user_doc.get("updated_at"))
    return jsonify(
        {
            "user_id": user_id,
            "display_name": display_name,
            "tier_name": derived_tier["name"],
            "tier_icon": derived_tier["icon"],
            "weekly_xp": weekly_xp,
            "monthly_xp": _safe_non_negative_int(user_doc.get("monthly_xp", 0)),
            "total_xp": total_xp,
            "weekly_referrals": weekly_referrals,
            "weekly_rank": weekly_rank,
            "monthly_referrals": _safe_non_negative_int(user_doc.get("monthly_referrals", 0)),
            "total_referrals": total_referrals,
            "streak_days": _safe_non_negative_int(user_doc.get("streak_days", user_doc.get("checkin_streak", user_doc.get("streak", 0)))),
            "streak": _safe_non_negative_int(user_doc.get("streak", user_doc.get("streak_days", user_doc.get("checkin_streak", 0)))),
            "streak_freeze_tokens": streak_freeze_tokens,
            "streak_freeze_max_tokens": STREAK_FREEZE_MAX_TOKENS,
            "next_tier_name": next_tier_name,
            "next_tier_progress_pct": int(next_tier_progress_pct),
            "next_tier_hint": next_tier_hint,
            "source_vip_tier": user_doc.get("vip_tier") or user_doc.get("status"),
        }
    )


def _coerce_utc_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _map_referral_status(raw_status):
    s = str(raw_status or "").strip().lower()
    if s in {"pending", "pending_channel", "processing"}:
        return "pending"
    if s in {"awarded", "qualified", "settled", "success"}:
        return "qualified"
    if s in {"revoked", "failed", "rejected", "expired"}:
        return "failed"
    return "pending"


def build_public_referral_user_label(row):
    row = row or {}

    def normalize_user_id_display(value):
        raw = str(value or "").strip()
        if raw.lower().startswith("uid:"):
            raw = raw.split(":", 1)[1].strip()
        return raw

    def _clean_username(value):
        if value is None:
            return ""
        text = str(value).strip()
        if text.startswith("@"):
            text = text[1:]
        return text.strip()

    username = _clean_username(row.get("username"))
    if not username:
        username = _clean_username(row.get("invitee_username"))
    if not username:
        username = _clean_username(row.get("usernameLower"))
    if username:
        return f"@{username}"

    for key in ("first_name", "invitee_first_name", "display_name", "invitee_display_name", "name"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for key in ("invitee_user_id", "user_id", "uid"):
        value = row.get(key)
        if value is None:
            continue
        normalized = normalize_user_id_display(value)
        if normalized:
            return normalized

    return "User"


def _build_referral_status_payload(user_id: int, now_utc: datetime):
    rows = list(
        pending_referrals_collection.find(
            {"inviter_user_id": int(user_id)},
            {
                "_id": 0,
                "invitee_user_id": 1,
                "invitee_username": 1,
                "first_name": 1,
                "invitee_first_name": 1,
                "display_name": 1,
                "invitee_display_name": 1,
                "name": 1,
                "username": 1,
                "usernameLower": 1,
                "status": 1,
                "revoked_reason": 1,
                "created_at_utc": 1,
            },
        ).sort("created_at_utc", -1).limit(50)
    )
    invitee_ids = [r.get("invitee_user_id") for r in rows if r.get("invitee_user_id") is not None]
    qualified_pairs = {
        (int(d.get("referrer_id")), int(d.get("invitee_id")))
        for d in qualified_events_collection.find(
            {"referrer_id": int(user_id), "invitee_id": {"$in": invitee_ids}},
            {"_id": 0, "referrer_id": 1, "invitee_id": 1},
        )
        if d.get("referrer_id") is not None and d.get("invitee_id") is not None
    }
    revoked_pairs = {
        (int(d.get("inviter_id")), int(d.get("invitee_id")))
        for d in referral_events_collection.find(
            {"inviter_id": int(user_id), "invitee_id": {"$in": invitee_ids}, "event": "referral_revoked"},
            {"_id": 0, "inviter_id": 1, "invitee_id": 1},
        )
        if d.get("inviter_id") is not None and d.get("invitee_id") is not None
    }

    referrals = []
    for row in rows:
        invitee_user_id = row.get("invitee_user_id")
        pair_key = (int(user_id), int(invitee_user_id)) if invitee_user_id is not None else None
        status = _map_referral_status(row.get("status"))
        if pair_key and pair_key in qualified_pairs:
            status = "qualified"
        elif pair_key and pair_key in revoked_pairs and status != "qualified":
            status = "failed"
        created_at = _coerce_utc_datetime(row.get("created_at_utc"))
        age_hours = 0
        if created_at is not None:
            age_hours = max(0, int((now_utc - created_at).total_seconds() // 3600))
        remaining_hold_hours = max(0, _referral_hold_hours() - age_hours) if status == "pending" else 0
        public_status = build_public_referral_status({"status": status, "revoked_reason": row.get("revoked_reason")}, logger=logger)
        label = build_public_referral_user_label(row)
        referrals.append(
            {
                "invitee_user_id": invitee_user_id,
                "invitee_username": row.get("invitee_username"),
                "display_label": label,
                "invitee_label": label,
                "status": public_status["status"],
                "status_label": public_status["label"],
                "status_icon": public_status["icon"],
                "status_tone": public_status["tone"],
                "created_at": created_at.isoformat() if created_at else None,
                "age_hours": age_hours,
                "remaining_hold_hours": remaining_hold_hours,
                "qualified_at": None,
            }
        )
    user_doc = users_collection.find_one(
        {"user_id": int(user_id)},
        {"total_referrals": 1, "weekly_referrals": 1, "monthly_referrals": 1, "snapshot_updated_at": 1, "user_id": 1},
    )
    resolved = resolve_referral_counts_with_snapshot_fallback(int(user_id), user_doc, now_utc)
    stats = resolved.get("stats") or {}
    total_referrals = int(stats.get("total_referrals", 0))
    progress = calc_referral_progress(total_referrals, milestone_size=REFERRAL_BONUS_INTERVAL)
    payload = {"ok": True, "hold_hours": _referral_hold_hours(), "referrals": referrals}
    payload.update(
        {
            "total_referrals": total_referrals,
            "weekly_referrals": int(stats.get("weekly_referrals", 0)),
            "monthly_referrals": int(stats.get("monthly_referrals", 0)),
            "progress": int(progress.get("progress", 0)),
            "remaining": int(progress.get("remaining", REFERRAL_BONUS_INTERVAL)),
            "progress_pct": float(progress.get("progress_pct", 0)),
            "near_miss": bool(progress.get("near_miss", False)),
            "next_bonus_xp": REFERRAL_BONUS_XP,
            "base_referral_xp": REFERRAL_XP_PER_SUCCESS,
            "bonus_interval": REFERRAL_BONUS_INTERVAL,
            "bonus_xp": REFERRAL_BONUS_XP,
        }
    )
    snapshot_ts = resolved.get("snapshot_updated_at")
    if snapshot_ts is not None:
        payload["snapshot_updated_at"] = snapshot_ts.isoformat()
    if resolved.get("snapshot_age_sec") is not None:
        payload["snapshot_age_sec"] = int(resolved.get("snapshot_age_sec"))
    logger.info("[REF_STATUS][UNIFIED_PAYLOAD] uid=%s source=%s", user_id, resolved.get("source"))
    return payload


@app.route("/api/referral/status", methods=["GET"])
def api_referral_status():
    try:
        init_data = extract_raw_init_data_from_query(request)
        if not init_data:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        ok, parsed, _ = verify_telegram_init_data(init_data)
        if not ok:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        user_payload = (parsed or {}).get("user", {})
        if isinstance(user_payload, str):
            try:
                user_payload = json.loads(user_payload)
            except Exception:
                user_payload = {}
        user_id = int((user_payload or {}).get("id"))
    except Exception:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    payload = _build_referral_status_payload(user_id, datetime.now(timezone.utc))
    return jsonify(payload)

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
    set_fields = {
        "username": username,
        "last_checkin": now_utc_ts,
        "streak": streak,
    }
    if region:
        set_fields["region"] = region
    _users_update_one(
        {"user_id": user_id},
        {
            "$set": set_fields,
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

    try:
        record_welcome_checkin_progress(int(user_id), now=now_utc_ts)
    except Exception:
        logger.exception("[WELCOME_PROGRESS] record_failed uid=%s", user_id)

    try:
        check_channel_subscribed(int(user_id))
    except Exception:
        pass

    maybe_shout_milestones(int(user_id))

    reactivation_journey_result = None
    try:
        reactivation_journey_result = handle_successful_checkin(db, int(user_id), now_ref=now_utc_ts)
    except Exception:
        logger.exception("[REACT_JOURNEY][ERROR] uid=%s tier=1 reason=checkin_hook_failed", user_id)

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

    result_payload = {
        "success": True,
        "message": msg,
        "base_xp": base_xp,
        "bonus_xp": bonus_xp,
        "total_xp": base_xp + bonus_xp,
        "streak": streak,
        "streak_label": labels.get(streak, "") if bonus_xp else "",
    }
    if reactivation_journey_result and reactivation_journey_result.get("voucher_code"):
        result_payload["reactivation_journey"] = {"tier": 1, "voucher_code": reactivation_journey_result.get("voucher_code")}
    return result_payload

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
    """Mini-app triggers check-in (region is optional)"""
    try:
        data = request.get_json(silent=True) or {}
        user_id = data.get("user_id")
        username = data.get("username", "unknown")

        if not user_id:
            return jsonify({"success": False, "error": "Missing user_id"}), 400

        user = users_collection.find_one({"user_id": int(user_id)}) or {}

        record_user_last_seen(
            db,
            user_id=int(user_id),
            ip=request.headers.get("Fly-Client-IP") or request.remote_addr,
            subnet=request.headers.get("X-Forwarded-For"),
            session=request.headers.get("X-Session-Id") or request.cookies.get("session") or request.headers.get("User-Agent"),
        )

        # ✅ Call check-in logic — process_checkin upserts, region is optional
        result = asyncio.run(
            process_checkin(int(user_id), username, user.get("region"))
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


@app.route("/api/region-by-ip", methods=["GET"])
def api_region_by_ip():
    ip, ip_source = _extract_client_ip(request)
    region, geo_source = _get_region_from_ip(ip)
    return jsonify({
        "success": True,
        "region": region,
        "source": f"{ip_source}:{geo_source}",
    })

@app.route("/v2/miniapp/daily-game", methods=["GET"])
def api_daily_game():
    now_kl = datetime.now(KL_TZ)
    date_kl = now_kl.strftime("%Y-%m-%d")
    pool = build_daily_game_pool(DAILY_GAME_SLOTS)
    if not pool:
        return jsonify({"ok": False, "error": "daily-game-slots-empty"}), 503
    digest = hashlib.sha256(date_kl.encode("utf-8")).hexdigest()
    slot_idx = int(digest[:8], 16) % len(pool)
    slot = pool[slot_idx].copy()
    slot.pop("weight", None)
    # TODO: extend slot payload with reward_hint, mission_flag, tracking_key when rewards flow is enabled.
    return jsonify({
        "ok": True,
        "date_kl": date_kl,
        "slot": slot,
    })

@app.route("/api/set-region/<int:user_id>", methods=["POST"])
def api_set_region(user_id):
    """Set region only if not already set"""
    if get_app_setting("feature_flags", "region_selection") is False:
        return jsonify({"success": False, "error": "feature_disabled"}), 200
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

@app.route("/go")
def go():
    bot_username = (os.environ.get("BOT_USERNAME") or "").strip()
    if not bot_username:
        logger.error("[GO] missing BOT_USERNAME")
        return "BOT_USERNAME is not configured", 500

    ad_doc = {
        "fbclid": request.args.get("fbclid"),
        "ttclid": request.args.get("ttclid"),
        "_fbp": request.cookies.get("_fbp"),
        "_fbc": request.cookies.get("_fbc"),
        "created_at": datetime.now(timezone.utc),
    }
    token = None
    for _ in range(3):
        candidate = uuid.uuid4().hex[:10]
        try:
            db["ad_attribution"].insert_one({"token": candidate, **ad_doc})
            token = candidate
            break
        except DuplicateKeyError:
            continue
    if not token:
        logger.error("[GO] failed to allocate unique token")
        return "unable to allocate token", 500

    telegram_url = f"https://t.me/{bot_username}?startapp=attr_{token}"
    return redirect(telegram_url, code=302)

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
    mode = None
    error = None
    stats = {"total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0}

    try:
        link = get_or_create_referral_invite_link_sync(user_id, username)
        mode = "invite_link"
        logger.info("[api_referral] link_generated uid=%s", user_id)
    except Exception as e:
        # No usable fallback: a bot /start deep-link (e.g. t.me/<bot>?start=ref<uid>)
        # is not parsed anywhere and never reaches the group-join attribution
        # pipeline (_confirm_referral_on_main_join keys off invite_link_map, which
        # is only populated for real createChatInviteLink results). Returning it as
        # referral_link would silently hand users a broken, unattributed link.
        success = False
        link = None
        error = str(e)
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
        "mode": mode,
        "referral_link": link,
        "snapshot_ts": snapshot_ts,
        "snapshot_age_sec": snapshot_age_sec,
        **stats,
    }
    if error:
        payload["error"] = error

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


def _affiliate_user_identity_map(user_ids: list[int]) -> dict[str, dict]:
    ids = []
    for raw in user_ids:
        try:
            ids.append(int(raw))
        except Exception:
            continue
    if not ids:
        return {}

    out = {}
    for u in users_collection.find({"user_id": {"$in": ids}}, {"user_id": 1, "username": 1, "first_name": 1}):
        try:
            uid = int(u.get("user_id"))
        except Exception:
            continue
        out[str(uid)] = {
            "username": (str(u.get("username")).lstrip("@") if u.get("username") else None),
            "display_name": format_username(u, uid, True),
        }
    return out

@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        if get_app_setting("feature_flags", "leaderboard") is False:
            return jsonify({"leaderboard": {"checkin": [], "referral": []}, "disabled": True})
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
            if QUERY_TELEMETRY_LOGS:
                with JobTimer() as xp_timer:
                    logger.info("[QUERY][leaderboard_xp] collection=users filter_fields=user_id sort_fields=weekly_xp limit=%s", leaderboard_limit)
                    xp_rows = list(
                        users_collection
                        .find(xp_query, {"user_id": 1, "username": 1, "weekly_xp": 1, "vip_tier": 1, "status": 1})
                        .sort("weekly_xp", DESCENDING)
                        .limit(leaderboard_limit)
                    )
                logger.info("[QUERY][leaderboard_xp] duration_ms=%s returned=%s", xp_timer.ms, len(xp_rows))
            else:
                xp_rows = list(
                    users_collection
                    .find(xp_query, {"user_id": 1, "username": 1, "weekly_xp": 1, "vip_tier": 1, "status": 1})
                    .sort("weekly_xp", DESCENDING)
                    .limit(leaderboard_limit)
                )
            if QUERY_TELEMETRY_LOGS:
                with JobTimer() as ref_timer:
                    logger.info("[QUERY][leaderboard_referrals] collection=users filter_fields=user_id sort_fields=weekly_referrals limit=%s", leaderboard_limit)
                    referral_rows = list(
                        users_collection.find(
                            {"user_id": {"$ne": None}},
                            {
                                "user_id": 1,
                                "username": 1,
                                "weekly_referrals": 1,
                                "total_referrals": 1,
                                "vip_tier": 1,
                                "status": 1,
                            },
                        )
                        .sort("weekly_referrals", DESCENDING)
                        .limit(leaderboard_limit)
                    )
                logger.info("[QUERY][leaderboard_referrals] duration_ms=%s returned=%s", ref_timer.ms, len(referral_rows))
            else:
                referral_rows = list(
                    users_collection.find(
                        {"user_id": {"$ne": None}},
                        {
                            "user_id": 1,
                            "username": 1,
                            "weekly_referrals": 1,
                            "total_referrals": 1,
                            "vip_tier": 1,
                            "status": 1,
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
                        "is_vip1": (row.get("vip_tier") == "VIP1" or row.get("status") == "VIP1"),
                    }
                    for row in xp_rows
                ],
                "referral": [
                    {
                        "user_id": row.get("user_id"),
                        "username": row.get("username"),
                        "weekly_referrals": int(row.get("weekly_referrals", 0)),
                        "total_referrals": int(row.get("total_referrals", 0)),
                        "is_vip1": (row.get("vip_tier") == "VIP1" or row.get("status") == "VIP1"),
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
            top_checkins.append({
                "username": formatted,
                "xp": int(row.get("weekly_xp", 0)),
                "is_vip1": bool(row.get("is_vip1", False)),
            })

        referral_board = []
        for row in cached_payload.get("referral", []):
            formatted = safe_format({"user_id": row.get("user_id"), "username": row.get("username")})
            if not formatted:
                continue
            entry = {
                "username": formatted,
                "total_valid": int(row.get("weekly_referrals", 0)),
                "referrals": int(row.get("weekly_referrals", 0)),
                "is_vip1": bool(row.get("is_vip1", False)),
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



@app.route("/api/share-rank-caption", methods=["POST"])
def api_share_rank_caption():
    user_id = None
    try:
        logger.info("[SHARE_RANK][REQUEST] remote_addr=%s", request.remote_addr)
        user_id, auth_error = _extract_verified_telegram_user_id()
        if auth_error:
            body, status = auth_error
            logger.warning("[SHARE_RANK][FAIL] stage=auth status=%s error=%s", status, body.get("error"))
            return jsonify(body), status

        user_doc = _load_share_rank_user_snapshot(user_id)
        if not user_doc:
            logger.warning("[SHARE_RANK][FAIL] stage=user_lookup uid=%s status=404", user_id)
            return jsonify({"ok": False, "error": "User not found"}), 404

        weekly_xp = _safe_non_negative_int(user_doc.get("weekly_xp", 0))
        total_referrals = _safe_non_negative_int(user_doc.get("total_referrals", 0))
        streak = _safe_non_negative_int(user_doc.get("streak_days", user_doc.get("checkin_streak", user_doc.get("streak", 0))))
        rank = _compute_share_rank(user_id, user_doc)
        title, highlight = choose_share_rank_achievement(rank, weekly_xp, streak, total_referrals)
        caption = build_share_rank_caption(rank, weekly_xp, title, highlight)

        logger.info(
            "[SHARE_RANK][SUCCESS] uid=%s rank=%s weekly_xp=%s title=%s",
            user_id,
            rank if rank is not None else "unranked",
            weekly_xp,
            title,
        )
        return jsonify(
            {
                "ok": True,
                "caption": caption,
                "rank": rank,
                "weekly_xp": weekly_xp,
                "title": title,
                "highlight": highlight,
            }
        )
    except Exception:
        logger.exception("[SHARE_RANK][FAIL] stage=unexpected uid=%s", user_id)
        return jsonify({"ok": False, "error": "Unable to create rank caption"}), 500

@app.route("/api/referral/share-content", methods=["POST"])
def api_referral_share_content():
    user_id = None
    try:
        init_data = extract_raw_init_data_from_query(request)
        if not init_data:
            return jsonify({"ok": False, "error": "Missing init_data"}), 400
        ok, parsed, _ = verify_telegram_init_data(init_data)
        if not ok:
            return jsonify({"ok": False, "error": "Unauthorized"}), 403
        user_payload = (parsed or {}).get("user", {})
        if isinstance(user_payload, str):
            try:
                user_payload = json.loads(user_payload)
            except Exception:
                user_payload = {}
        try:
            user_id = int((user_payload or {}).get("id"))
        except (TypeError, ValueError):
            user_id = None
        if not user_id:
            return jsonify({"ok": False, "error": "Unauthorized"}), 403
        username = (user_payload or {}).get("username") or ""

        from referral_share_content import generate_share_package

        result = generate_share_package(user_id, username, generated_by="miniapp")
        if not result.get("ok"):
            code = result.get("code")
            if code == "no_active_playback":
                return jsonify({"ok": False, "error": "No playback is currently available. Please try again later."}), 503
            logger.error("[SHARE_CONTENT][API_FAIL] uid=%s code=%s", user_id, code)
            return jsonify({"ok": False, "error": "Unable to generate your referral link right now."}), 500

        logger.info("[SHARE_CONTENT][API_OK] uid=%s", user_id)
        return jsonify(
            {
                "ok": True,
                "message": result["message"],
                "invite_link": result["invite_link"],
                "playback_url": result["playback_url"],
                "hook_text": result["hook_text"],
            }
        )
    except Exception:
        logger.exception("[SHARE_CONTENT][API_FAIL] stage=unexpected uid=%s", user_id)
        return jsonify({"ok": False, "error": "Unable to generate your referral link right now."}), 500


@app.route("/api/affiliate/leaderboard", methods=["GET"])
def get_affiliate_leaderboard_week():
    if get_app_setting("feature_flags", "affiliate") is False:
        return jsonify({"success": False, "error": "feature_disabled"}), 200
    window = (request.args.get("window") or "week").strip().lower()
    if window != "week":
        return jsonify({"success": False, "error": "unsupported_window"}), 400

    snapshot = compute_affiliate_weekly_kpis_live(db)
    rows = list(snapshot.get("affiliate_leaderboard_week") or [])

    raw_user_id = request.args.get("user_id")
    try:
        current_user_id = int(raw_user_id) if raw_user_id not in (None, "", "undefined") else 0
    except (TypeError, ValueError):
        current_user_id = 0
    user_record = users_collection.find_one({"user_id": current_user_id}, {"is_admin": 1}) or {}
    is_admin = bool(user_record.get("is_admin", False))

    ids = []
    for item in rows:
        try:
            ids.append(int(item.get("referrer_id")))
        except Exception:
            continue

    users_by_id = {}
    if ids:
        for u in users_collection.find({"user_id": {"$in": ids}}, {"user_id": 1, "username": 1, "first_name": 1}):
            users_by_id[int(u.get("user_id"))] = u

    leaderboard = []
    my_stats = None
    for item in rows:
        row = dict(item)
        referrer_id = row.get("referrer_id")
        try:
            referrer_id_int = int(referrer_id)
        except Exception:
            referrer_id_int = None
        if referrer_id_int is not None and referrer_id_int in users_by_id:
            display = format_username(users_by_id[referrer_id_int], current_user_id, is_admin)
            if display:
                row["display_name"] = display
        leaderboard.append(row)
        if current_user_id and str(referrer_id) == str(current_user_id):
            my_stats = {
                "joins_week_raw": int(row.get("joins_week_raw", 0) or 0),
                "joins_week_counted": int(row.get("joins_week_counted", 0) or 0),
                "qualified_week": int(row.get("qualified_week", 0) or 0),
                "conversion_week": float(row.get("conversion_week", 0.0) or 0.0),
                "quality_flag": row.get("quality_flag") or "new",
            }

    if current_user_id and my_stats is None:
        snapshot_by_referrer = snapshot.get("affiliate_weekly_by_referrer") or {}
        if isinstance(snapshot_by_referrer, dict):
            cached_stats = snapshot_by_referrer.get(str(current_user_id))
            if isinstance(cached_stats, dict):
                my_stats = {
                    "joins_week_raw": int(cached_stats.get("joins_week_raw", 0) or 0),
                    "joins_week_counted": int(cached_stats.get("joins_week_counted", 0) or 0),
                    "qualified_week": int(cached_stats.get("qualified_week", 0) or 0),
                    "conversion_week": float(cached_stats.get("conversion_week", 0.0) or 0.0),
                    "quality_flag": cached_stats.get("quality_flag") or "new",
                }

    if current_user_id and my_stats is None:
        week_start_utc, week_end_utc, _ = affiliate_week_window_utc_from_reference()
        joins_week_raw = int(
            pending_referrals_collection.count_documents(
                {
                    "inviter_user_id": current_user_id,
                    "created_at_utc": {"$gte": week_start_utc, "$lt": week_end_utc},
                }
            )
        )
        joins_week_counted = int(
            db.referral_flow_events.count_documents(
                {
                    "event": "join_counted",
                    "referrer_id": current_user_id,
                    "ts_utc": {"$gte": week_start_utc, "$lt": week_end_utc},
                }
            )
        )
        # Use find_one() for existence check instead of count_documents(limit=1)
        # because some PyMongo versions reject the limit parameter.
        has_flow_settled = (
            db.referral_flow_events.find_one(
                {
                    "event": "referral_settled",
                    "ts_utc": {"$gte": week_start_utc, "$lt": week_end_utc},
                    "referrer_id": {"$ne": None},
                },
                {"_id": 1},
            )
            is not None
        )
        if has_flow_settled:
            qualified_week = int(
                db.referral_flow_events.count_documents(
                    {
                        "event": "referral_settled",
                        "referrer_id": current_user_id,
                        "ts_utc": {"$gte": week_start_utc, "$lt": week_end_utc},
                    }
                )
            )
        else:
            qualified_week = int(
                db.referral_events.count_documents(
                    {
                        "event": "referral_settled",
                        "inviter_id": current_user_id,
                        "occurred_at": {"$gte": week_start_utc, "$lt": week_end_utc},
                    }
                )
            )
        conversion_week = float(qualified_week / joins_week_raw) if joins_week_raw > 0 else 0.0
        quality_flag = "new" if joins_week_raw < 10 else ("low_quality" if conversion_week < 0.20 else "ok")
        my_stats = {
            "joins_week_raw": joins_week_raw,
            "joins_week_counted": joins_week_counted,
            "qualified_week": qualified_week,
            "conversion_week": round(conversion_week, 4),
            "quality_flag": quality_flag,
        }

    if not is_admin:
        for row in leaderboard:
            row.pop("quality_flag", None)
        if isinstance(my_stats, dict):
            my_stats.pop("quality_flag", None)

    return jsonify(
        {
            "generated_at": (snapshot.get("generated_at") or datetime.now(timezone.utc)).isoformat(),
            "week_start_utc": snapshot.get("week_start_utc").isoformat() if snapshot.get("week_start_utc") else None,
            "week_end_utc": snapshot.get("week_end_utc").isoformat() if snapshot.get("week_end_utc") else None,
            "rules": snapshot.get("rules") or {},
            "leaderboard": leaderboard,
            "my_stats": my_stats,
            "is_admin": is_admin,
        }
    ), 200


def _serialize_affiliate_snapshot_item(doc: dict) -> dict:
    return {
        "week_key": doc.get("week_key"),
        "week_start_local": doc.get("week_start_local"),
        "week_end_local": doc.get("week_end_local"),
        "entry_count": int(doc.get("entry_count", 0) or 0),
        "snapshot_at": doc.get("snapshot_at").isoformat() if doc.get("snapshot_at") else None,
        "generated_by": doc.get("generated_by"),
    }


def _affiliate_viewer_context() -> tuple[int, bool]:
    raw_user_id = request.args.get("user_id")
    try:
        current_user_id = int(raw_user_id) if raw_user_id not in (None, "", "undefined") else 0
    except (TypeError, ValueError):
        current_user_id = 0
    user_record = users_collection.find_one({"user_id": current_user_id}, {"is_admin": 1}) or {}
    return current_user_id, bool(user_record.get("is_admin", False))


@app.route("/api/leaderboard/affiliate/snapshots", methods=["GET"])
def api_affiliate_snapshot_list():
    current_user_id, is_admin = _affiliate_viewer_context()
    if not is_admin:
        print(f"[AFF_LEADERBOARD][PAST_DENY] user_id={current_user_id} reason=admin_required")
        return jsonify({"status": "error", "reason": "admin_required"}), 403

    limit = request.args.get("limit", default=20, type=int)
    if limit is None or limit <= 0:
        limit = 20
    limit = min(limit, 52)
    docs = list(
        db.affiliate_leaderboard_snapshots.find(
            {},
            {
                "_id": 0,
                "week_key": 1,
                "week_start_local": 1,
                "week_end_local": 1,
                "entry_count": 1,
                "snapshot_at": 1,
                "generated_by": 1,
            },
        ).sort("week_key", -1).limit(limit)
    )
    return jsonify({"ok": True, "items": [_serialize_affiliate_snapshot_item(doc) for doc in docs]})


@app.route("/api/leaderboard/affiliate/snapshot", methods=["GET"])
def api_affiliate_snapshot_get():
    week_key = (request.args.get("week_key") or request.args.get("week_start") or "").strip()
    if not week_key:
        return jsonify({"ok": False, "error": "missing_week_key"}), 400
    if affiliate_week_window_from_week_key_kl(week_key) is None:
        return jsonify({"ok": False, "error": "invalid_week_key"}), 400

    current_user_id, is_admin = _affiliate_viewer_context()
    _, _, current_week_start_local = affiliate_week_window_utc_from_reference()
    current_week_key = current_week_start_local.date().isoformat()
    is_past_week = week_key != current_week_key
    if is_past_week and not is_admin:
        print(f"[AFF_LEADERBOARD][PAST_DENY] user_id={current_user_id} reason=admin_required")
        return jsonify({"status": "error", "reason": "admin_required"}), 403
    if is_past_week and is_admin:
        print(f"[AFF_LEADERBOARD][PAST_ADMIN_VIEW] user_id={current_user_id} week_key={week_key}")

    doc = db.affiliate_leaderboard_snapshots.find_one({"week_key": week_key}, {"_id": 0})
    if not doc:
        return jsonify({"ok": False, "error": "snapshot_not_found"}), 404

    entries = serialize_affiliate_snapshot_entries_for_viewer(
        list(doc.get("entries") or []),
        current_user_id=current_user_id,
        is_admin=is_admin,
        format_username_fn=format_username,
        mask_username_fn=mask_username,
    )

    return jsonify(
        {
            "ok": True,
            "week_key": doc.get("week_key"),
            "week_start_local": doc.get("week_start_local"),
            "week_end_local": doc.get("week_end_local"),
            "snapshot_at": doc.get("snapshot_at").isoformat() if doc.get("snapshot_at") else None,
            "entry_count": int(doc.get("entry_count", 0) or 0),
            "metric_name": doc.get("metric_name"),
            "entries": entries,
        }
    )


@app.route("/api/admin/leaderboard/affiliate/snapshot/regenerate", methods=["POST"])
def api_admin_affiliate_snapshot_regenerate():
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    payload = request.get_json(silent=True) or {}
    week_key = (payload.get("week_key") or "").strip()
    force = bool(payload.get("force", False))
    if not week_key:
        return jsonify({"ok": False, "error": "week_key_required"}), 400
    week_window = affiliate_week_window_from_week_key_kl(week_key)
    if week_window is None:
        return jsonify({"ok": False, "error": "invalid_week_key"}), 400
    result = build_affiliate_leaderboard_snapshot(
        db,
        week_window=week_window,
        force=force,
        mode="admin_manual",
        user_identity_loader=_affiliate_user_identity_map,
    )
    return jsonify(
        {
            "ok": True,
            "week_key": week_key,
            "status": result.get("status"),
            "entry_count": result.get("entry_count"),
        }
    )


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

@app.route("/api/welcome-progress/<int:user_id>", methods=["GET"])
def welcome_progress_api(user_id):
    try:
        payload = build_welcome_progress_response(user_id)
    except Exception:
        logger.exception("[WELCOME_PROGRESS_API] failed uid=%s", user_id)
        payload = {"visible": False, "status": "not_eligible"}
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


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
        user_id = None
        init_data = extract_raw_init_data_from_query(request)
        ok, parsed, _ = verify_telegram_init_data(init_data)
        if ok:
            user_payload = (parsed or {}).get("user", {})
            if isinstance(user_payload, str):
                try:
                    user_payload = json.loads(user_payload)
                except Exception:
                    user_payload = {}
            try:
                user_id = int((user_payload or {}).get("id"))
            except Exception:
                user_id = None

        if user_id is None and _admin_secret_ok(_get_admin_secret(request)):
            user_id = request.args.get("user_id", type=int)

        if user_id is None:
            return jsonify({"code": None})

        def _mask_voucher_code(raw):
            code = (raw or "").strip()
            if not code:
                return ""
            if len(code) <= 8:
                return f"{code[:2]}...{code[-2:]}"
            return f"{code[:4]}...{code[-4:]}"

        affiliate_doc = affiliate_ledger_collection.find_one(
            {
                "user_id": user_id,
                "status": "ISSUED",
                "reward_type": {"$ne": "affiliate_bundle"},
                "voucher_code": {"$exists": True, "$nin": [None, ""]},
            },
            sort=[("updated_at", DESCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        )
        affiliate_code = ((affiliate_doc or {}).get("voucher_code") or "").strip()
        if affiliate_code:
            logger.info(
                "[BONUS][AFFILIATE_HIT] user_id=%s code=%s",
                user_id,
                _mask_voucher_code(affiliate_code),
            )
            return jsonify({"code": affiliate_code})
        logger.info("[BONUS][AFFILIATE_MISS] user_id=%s", user_id)
        return jsonify({"code": None})
    except Exception as e:
        logger.exception("[BONUS_VOUCHER][AFFILIATE_ERROR] %s", e)
        return jsonify({"code": None, "error": str(e)}), 500


@app.route("/api/affiliate_bonus_vouchers", methods=["GET"])
def get_affiliate_bonus_vouchers():
    try:
        user_id = None
        init_data = extract_raw_init_data_from_query(request)
        ok, parsed, _ = verify_telegram_init_data(init_data)
        if ok:
            user_payload = (parsed or {}).get("user", {})
            if isinstance(user_payload, str):
                try:
                    user_payload = json.loads(user_payload)
                except Exception:
                    user_payload = {}
            try:
                user_id = int((user_payload or {}).get("id"))
            except Exception:
                user_id = None

        if user_id is None and _admin_secret_ok(_get_admin_secret(request)):
            user_id = request.args.get("user_id", type=int)

        if user_id is None:
            return jsonify({"rewards": []})

        def _mask_voucher_code(raw):
            code = (raw or "").strip()
            if not code:
                return ""
            if len(code) <= 8:
                return f"{code[:2]}...{code[-2:]}"
            return f"{code[:4]}...{code[-4:]}"

        rows = list(
            affiliate_ledger_collection.find(
                {
                    "user_id": user_id,
                    "status": "ISSUED",
                    "reward_type": {"$ne": "affiliate_bundle"},
                    "voucher_code": {"$exists": True, "$nin": [None, ""]},
                }
            ).sort([("updated_at", DESCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)])
        )

        rewards = []
        seen = set()
        for row in rows:
            code = (row.get("voucher_code") or "").strip()
            if not code:
                continue
            tier = row.get("tier") or row.get("reward_tier") or ""
            dedup_key = (str(tier), code)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            issued_at = row.get("updated_at") or row.get("created_at")
            item = {"tier": str(tier) if tier else "", "code": code}
            if issued_at is not None:
                item["issued_at"] = issued_at.isoformat() if hasattr(issued_at, "isoformat") else str(issued_at)
            rewards.append(item)
            logger.info(
                "[BONUS][AFFILIATE_HISTORY_ITEM] user_id=%s tier=%s code=%s",
                user_id,
                item["tier"] or "-",
                _mask_voucher_code(code),
            )

        return jsonify({"rewards": rewards})
    except Exception as e:
        logger.exception("[BONUS_VOUCHER][AFFILIATE_HISTORY_ERROR] %s", e)
        return jsonify({"rewards": [], "error": str(e)}), 500


@app.route("/api/campaign_bonus_voucher", methods=["GET"])
def get_campaign_bonus_voucher():
    try:
        init_data = extract_raw_init_data_from_query(request)
        ok, _, _ = verify_telegram_init_data(init_data)
        if not ok:
            return jsonify({"code": None})

        now = datetime.now(timezone.utc)
        voucher = bonus_voucher_collection.find_one()
        if not voucher:
            logger.info("[BONUS][CAMPAIGN_MISS] reason=no_voucher")
            return jsonify({"code": None})

        code = (voucher.get("code") or "").strip()
        if not code:
            logger.info("[BONUS][CAMPAIGN_MISS] reason=blank_code")
            return jsonify({"code": None})

        release = voucher.get("release_time") or voucher.get("start_time")
        expiry = voucher.get("expiry") or voucher.get("end_time")
        if release is None:
            logger.info("[BONUS][CAMPAIGN_MISS] reason=missing_release_time")
            return jsonify({"code": None})
        if release.tzinfo is None:
            release = release.replace(tzinfo=pytz.UTC)
        if expiry is not None and expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=pytz.UTC)

        if release <= now and (expiry is None or now <= expiry):
            masked = code[:2] + "..." + code[-2:] if len(code) <= 8 else code[:4] + "..." + code[-4:]
            logger.info("[BONUS][CAMPAIGN_HIT] code=%s", masked)
            return jsonify({"code": code})

        logger.info("[BONUS][CAMPAIGN_MISS] reason=not_live")
        return jsonify({"code": None})
    except Exception as e:
        logger.exception("[BONUS][CAMPAIGN_ERROR] %s", e)
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


def _mark_private_interaction(uid: int, username: str | None = None) -> None:
    now_ts = now_utc()
    update_doc = {
        "$set": {
            "pm_reachable": True,
            "last_private_interaction_at": now_ts,
            "last_private_interaction_source": "private_chat",
        },
        "$setOnInsert": {
            "status": "Normal",
        },
        "$unset": {
            "pm_blocked": "",
        },
    }
    if username:
        update_doc["$set"]["username"] = username
    existing = users_collection.find_one({"user_id": uid}, {"bot_started_at": 1, "first_private_interaction_at": 1}) or {}
    if not existing.get("bot_started_at"):
        update_doc["$set"]["bot_started_at"] = now_ts
    if not existing.get("first_private_interaction_at"):
        update_doc["$set"]["first_private_interaction_at"] = now_ts
    _users_update_one({"user_id": uid}, update_doc, upsert=True, context="private_interaction")


def _welcome_bonus_claimed(uid: int) -> bool:
    final_statuses_upper = {"ISSUED", "CLAIMED", "CONSUMED", "REDEEMED", "USED"}
    final_statuses_lower = {s.lower() for s in final_statuses_upper}

    def _has_text(value) -> bool:
        return isinstance(value, str) and bool(value.strip())

    try:
        eligibility = welcome_eligibility_collection.find_one({"$or": [{"uid": uid}, {"user_id": uid}]}, {"claimed_at": 1, "consumed_at": 1, "issued_at": 1}) or {}
        if eligibility.get("claimed_at") or eligibility.get("consumed_at") or eligibility.get("issued_at"):
            logger.info("[WELCOME][CLAIMED_DETECTED] uid=%s source=welcome_eligibility_collection status=claimed_like", uid)
            return True
    except Exception:
        logger.exception("[WELCOME][CLAIMED_CHECK_ERROR] uid=%s source=welcome_eligibility_collection", uid)

    try:
        ticket = db["welcome_tickets"].find_one({"$or": [{"uid": uid}, {"user_id": uid}]}, {"status": 1, "claimed_at": 1, "consumed_at": 1, "issued_at": 1}) or {}
        ticket_status = str(ticket.get("status") or "").lower()
        if ticket.get("claimed_at") or ticket.get("consumed_at") or ticket.get("issued_at") or ticket_status in {"claimed", "consumed", "issued"}:
            logger.info("[WELCOME][CLAIMED_DETECTED] uid=%s source=welcome_tickets status=%s", uid, ticket_status or "claimed_like")
            return True
    except Exception:
        logger.exception("[WELCOME][CLAIMED_CHECK_ERROR] uid=%s source=welcome_tickets", uid)

    try:
        affiliate_doc = db["affiliate_ledger"].find_one(
            {
                "user_id": uid,
                "$or": [
                    {"ledger_type": "WELCOME"},
                    {"tier": "WELCOME"},
                    {"pool_id": "WELCOME"},
                ],
            },
            {"status": 1, "voucher_code": 1},
        ) or {}
        status = str(affiliate_doc.get("status") or "").upper()
        voucher_code = affiliate_doc.get("voucher_code")
        if status in final_statuses_upper or _has_text(voucher_code):
            logger.info("[WELCOME][CLAIMED_DETECTED] uid=%s source=affiliate_ledger status=%s", uid, status or "voucher_code")
            return True
    except Exception:
        logger.exception("[WELCOME][CLAIMED_CHECK_ERROR] uid=%s source=affiliate_ledger", uid)

    try:
        voucher_claim_doc = db["voucher_claims"].find_one(
            {"$or": [{"user_id": uid}, {"uid": uid}]},
            {"claimed_at": 1, "pool_id": 1, "drop_id": 1, "dropId": 1, "category": 1, "audience": 1, "type": 1, "status": 1},
        ) or {}
        is_welcome = str(voucher_claim_doc.get("pool_id") or "").upper() == "WELCOME"
        if not is_welcome:
            for k in ("drop_id", "dropId", "category", "audience", "type"):
                val = str(voucher_claim_doc.get(k) or "").lower()
                if "welcome" in val or "new_joiner" in val:
                    is_welcome = True
                    break
        claim_status = str(voucher_claim_doc.get("status") or "").lower()
        if is_welcome and (voucher_claim_doc.get("claimed_at") or claim_status in final_statuses_lower):
            logger.info("[WELCOME][CLAIMED_DETECTED] uid=%s source=voucher_claims status=%s", uid, claim_status or "claimed_at")
            return True
    except Exception:
        logger.exception("[WELCOME][CLAIMED_CHECK_ERROR] uid=%s source=voucher_claims", uid)

    try:
        pool_doc = db["voucher_pools"].find_one(
            {
                "pool_id": "WELCOME",
                "$or": [{"issued_to": uid}, {"issued_to_user_id": uid}],
            },
            {"status": 1},
        ) or {}
        pool_status = str(pool_doc.get("status") or "").lower()
        if pool_doc and pool_status in final_statuses_lower:
            logger.info("[WELCOME][CLAIMED_DETECTED] uid=%s source=voucher_pools status=%s", uid, pool_status)
            return True
    except Exception:
        logger.exception("[WELCOME][CLAIMED_CHECK_ERROR] uid=%s source=voucher_pools", uid)

    try:
        joiner_doc = db["new_joiner_claims"].find_one(
            {"$or": [{"uid": uid}, {"user_id": uid}]},
            {"claimed_at": 1, "status": 1},
        ) or {}
        joiner_status = str(joiner_doc.get("status") or "").lower()
        if joiner_doc.get("claimed_at") or joiner_status in final_statuses_lower:
            logger.info("[WELCOME][CLAIMED_DETECTED] uid=%s source=new_joiner_claims status=%s", uid, joiner_status or "claimed_at")
            return True
    except Exception:
        logger.exception("[WELCOME][CLAIMED_CHECK_ERROR] uid=%s source=new_joiner_claims", uid)
    return False


async def _send_welcome_unclaimed_reminder_if_needed(context: ContextTypes.DEFAULT_TYPE, uid: int, source: str = "private_message") -> bool:
    if source == "start":
        logger.info("[PM][WELCOME_UNCLAIMED][SKIP] uid=%s reason=start_reply_already_contains_miniapp", uid)
        return False
    user = users_collection.find_one({"user_id": uid}, {"pm_reachable": 1, "welcome_unclaimed_reminder_sent_at": 1}) or {}
    if not user.get("pm_reachable"):
        return False
    allowed, reason, _ticket = welcome_eligibility(uid, ref=now_utc())
    if not allowed:
        logger.info("[PM][WELCOME_UNCLAIMED][SKIP] uid=%s reason=%s", uid, reason)
        return False
    if _welcome_bonus_claimed(uid):
        logger.info("[PM][WELCOME_UNCLAIMED][SKIP] uid=%s reason=already_claimed", uid)
        return False
    last_sent = user.get("welcome_unclaimed_reminder_sent_at")
    if isinstance(last_sent, datetime) and (now_utc() - last_sent) < timedelta(hours=24):
        logger.info("[PM][WELCOME_UNCLAIMED][SKIP] uid=%s reason=cooldown", uid)
        return False

    reminder_text = "You still have a welcome bonus waiting. Open the mini-app to claim it before it expires."
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Claim in Mini-App", web_app=WebAppInfo(url=WEBAPP_URL))]])
    try:
        ok, err = await safe_send_message(context.bot, chat_id=uid, text=reminder_text, reply_markup=keyboard, uid=uid, send_type="welcome_unclaimed", raise_on_non_transient=False, return_error=True)
    except (Forbidden, BadRequest) as exc:
        err = str(exc)
        ok = False
    if ok:
        _users_update_one({"user_id": uid}, {"$set": {"welcome_unclaimed_reminder_sent_at": now_utc()}}, context="welcome_unclaimed_sent")
        return True

    err_text = str(err or "unknown")
    if "forbidden" in err_text.lower() or "can't initiate conversation" in err_text.lower() or "bot was blocked" in err_text.lower():
        _users_update_one({"user_id": uid}, {"$set": {"pm_blocked": True, "last_pm_forbidden_at": now_utc()}}, context="welcome_unclaimed_forbidden")
    logger.warning("[PM][UNREACHABLE] uid=%s type=welcome_unclaimed reason=%s", uid, err_text)
    return False


def _send_welcome_reminder_via_bot(uid: int, text: str) -> bool:
    """Send a Welcome reminder with a Mini-App button via the live bot.

    Used as ``bot_send_fn`` by the sync APScheduler Welcome reminder jobs in
    scheduler.py (voucher lifecycle + progress journey); a falsy/exception
    result there falls back to the plain-text HTTP path.
    """
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Open Mini-App", web_app=WebAppInfo(url=WEBAPP_URL))]])
    ok, _err = call_bot_in_loop(
        safe_send_message(app_bot.bot, chat_id=uid, text=text, reply_markup=keyboard, uid=uid, send_type="welcome_reminder", raise_on_non_transient=False, return_error=True)
    )
    return bool(ok)


def _ensure_user_registered(user) -> None:
    """Minimum user registration/upsert shared by the normal /start flow and
    the referral deep-link route. Idempotent: $setOnInsert is a no-op for a
    user who already exists.
    """
    user_id = user.id
    _mark_private_interaction(user_id, user.username)
    _users_update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "username": user.username,
            "last_checkin": None,
            "status": "Normal",
        }},
        upsert=True,
        context="start_user_insert",
    )


async def ensure_user_initialized_for_referral(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Registers the user (if missing) before the /start?start=referral deep
    link generates a referral link. Reuses the same upsert logic as the
    normal /start flow; safe to call for existing users.
    """
    user = update.effective_user
    if not user:
        return
    _ensure_user_registered(user)


async def send_referral_link_with_share_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends the user's Share Content package (caption hook + playback URL +
    canonical invite link) with a single Share button. Used exclusively by
    the /start?start=referral deep-link route; does not send the normal
    /start welcome message or keyboard.
    """
    user = update.effective_user
    uid = user.id
    username = user.username or ""

    from referral_share_content import generate_share_package

    try:
        result = await asyncio.to_thread(generate_share_package, uid, username)
    except Exception:
        logger.exception("[REFERRAL][DEEPLINK_FAILED] uid=%s", uid)
        await safe_reply_text(
            update.effective_message,
            "Unable to generate your referral link right now. Please try again.",
            uid=uid,
            send_type="referral_deep_link_error",
            raise_on_non_transient=False,
        )
        return

    if not result.get("ok"):
        code = result.get("code")
        if code == "no_active_playback":
            text = "No playback is currently available. Please try again later."
        else:
            text = "Unable to generate your referral link right now. Please try again."
        logger.error("[REFERRAL][DEEPLINK_FAILED] uid=%s code=%s", uid, code)
        await safe_reply_text(
            update.effective_message,
            text,
            uid=uid,
            send_type="referral_deep_link_error",
            raise_on_non_transient=False,
        )
        return

    invite_link = result["invite_link"]
    caption = result["message"]

    # Telegram's share/url composes the prefilled message as
    # "{url}\n{text}" (url first, then text) -- not the other way around.
    # So the link goes only in the url param, and text carries the hook +
    # playback URL *without* the trailing arrow/link line, otherwise the
    # invite link would appear twice in the share sheet.
    share_text = f"{result['hook_text']}\n{result['playback_url']}\n\nMore player replays and rewards inside AdvantPlay:"
    share_url = (
        "https://t.me/share/url"
        f"?url={quote(invite_link, safe='')}"
        f"&text={quote(share_text, safe='')}"
    )
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📤 Share Referral Link", url=share_url)]]
    )

    await safe_reply_text(
        update.effective_message,
        f"<blockquote>{html_escape(caption)}</blockquote>",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=keyboard,
        uid=uid,
        send_type="referral_deep_link",
        raise_on_non_transient=False,
    )
    logger.info("[REFERRAL][DEEPLINK_OK] uid=%s", uid)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private_chat(update):
        logger.info(
            "[GUARD] ignore_non_private cmd=%s chat_type=%s chat_id=%s uid=%s",
            update.message.text if update.message else "",
            update.effective_chat.type if update.effective_chat else "",
            update.effective_chat.id if update.effective_chat else "",
            update.effective_user.id if update.effective_user else "",
        )
        return

    payload = context.args[0] if context.args else None
    if payload == "referral":
        await ensure_user_initialized_for_referral(update, context)
        await send_referral_link_with_share_button(update, context)
        return

    user = update.effective_user
    message = update.effective_message

    if user:
        user_id = user.id
        user_doc_before = users_collection.find_one(
            {"user_id": user_id},
            {"pm_sent.pm0_welcome": 1},
        ) or {}
        first_time_pm0 = not (user_doc_before.get("pm_sent") or {}).get("pm0_welcome")

        _ensure_user_registered(user)
        user_doc = users_collection.find_one({"user_id": user_id}, {"joined_main_at": 1})
        if not (user_doc or {}).get("joined_main_at"):
            logger.info(
                "[WELCOME][JOIN_BACKFILL_DISABLED] uid=%s joined_main_at_missing",
                user_id,
            )
        welcome_keyboard = [
            [InlineKeyboardButton("📢 Join Official Channel", url=OFFICIAL_CHANNEL_URL)],
            [InlineKeyboardButton("🚀 Open AdvantPlay Mini-App", web_app=WebAppInfo(url=WEBAPP_URL))],
        ]
        normal_keyboard = [
            [InlineKeyboardButton("📢 Join Official Channel", url=OFFICIAL_CHANNEL_URL)],
            [InlineKeyboardButton("🚀 Open AdvantPlay Mini-App", web_app=WebAppInfo(url=WEBAPP_URL))],
        ]
        logger.info("[START][NORMAL_KEYBOARD_SHOWN] uid=%s", user_id)
        if message:
            if first_time_pm0:
                sent = await safe_reply_text(
                    message,
                    "🎁 Your Welcome Voucher Is Waiting\n\n"
                    "It is not activated yet.\n\n"
                    "Complete these steps to unlock it:\n\n"
                    "1️⃣ Follow @AdvantPlayOfficial\n"
                    "2️⃣ Check in 3 days within 7 days\n"
                    "3️⃣ Claim your Welcome Voucher\n\n"
                    "✅ No deposit required\n"
                    "⏱ Less than 1 minute per day\n\n"
                    "👇 Start now",
                    reply_markup=InlineKeyboardMarkup(welcome_keyboard),
                    uid=user_id,
                    send_type="start",
                    raise_on_non_transient=False,
                )
                if sent:
                    _users_update_one(
                        {"user_id": user_id},
                        {"$set": {"pm_sent.pm0_welcome": now_utc()}},
                        context="pm0_welcome_sent",
                    )
                    logger.info("[PM0][SENT] uid=%s type=welcome_first_time", user_id)
            else:
                sent = await safe_reply_text(
                    message,
                    "👋 Welcome to AdvantPlay Community!\n\n"
                    "Join our channel to get: 👇:\n\n"      
                    "⚡ Daily voucher drops\n"
                    "🎁 Bonus campaigns\n"
                    "👑 VIP-only announcements\n"
                    "🏆 Weekly ranking rewards\n\n"                

                    "Start here 👇",
                    reply_markup=InlineKeyboardMarkup(normal_keyboard),
                    uid=user_id,
                    send_type="start",
                    raise_on_non_transient=False,
                )
                if sent:
                    logger.info("[PM0][SENT] uid=%s type=normal_returning", user_id)

        await _send_welcome_unclaimed_reminder_if_needed(context, user.id, source="start")
            
async def member_update_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    member = update.chat_member or update.my_chat_member
    if not member:
        return

    chat_id = member.chat.id
    old_status = getattr(member.old_chat_member, "status", None)
    new_status = getattr(member.new_chat_member, "status", None)
    allowed_statuses = {"member", "administrator", "creator"}
    left_group = old_status in allowed_statuses and new_status in ("left", "kicked")

    # A user already present in the chat in some capacity is not a brand-new
    # join when their status is later lifted to "member". "restricted" is
    # only "present" when Telegram's own is_member flag says so — a
    # restricted member with is_member=False is NOT currently in the chat
    # (e.g. banned-with-restrictions), so restricted(is_member=False) ->
    # member is a real join and must not be suppressed.
    old_is_member = getattr(member.old_chat_member, "is_member", None)
    was_present = old_status in {"member", "administrator", "creator"} or (
        old_status == "restricted" and old_is_member is True
    )
    was_absent = not was_present
    # 只处理 “变成成员” 的事件
    became_member = (new_status in allowed_statuses) and was_absent

    # The chat id currently configured as the referral destination — lets a
    # REFERRAL_DESTINATION_CHAT_ID override (a channel id different from
    # OFFICIAL_CHANNEL_ID) be treated the same as the official channel for
    # attribution, subscription bookkeeping, and leave handling.
    try:
        live_dest_chat_id, live_dest_type = get_referral_destination()
    except Exception:
        live_dest_chat_id, live_dest_type = None, None
    is_channel_chat_id = chat_id == OFFICIAL_CHANNEL_ID or (
        live_dest_type == "official_channel" and chat_id == live_dest_chat_id
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
                "status": {"$in": ["pending", "pending_channel"]},
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
            import referral_invitee_lock

            referral_invitee_lock.release(
                db, invitee_user_id=user.id, status="revoked", now_utc_ts=now
            )
        return

    if left_group and is_channel_chat_id and isinstance(user.id, int):
        now = now_utc()
        users_collection.update_one(
            {"user_id": user.id},
            {
                "$set": {
                    "left_official_channel_at": now,
                    "official_channel_currently_subscribed": False,
                }
            },
            upsert=True,
        )
        logger.info("[CHANNEL][LEAVE] uid=%s chat_id=%s", user.id, chat_id)
        return

    if not became_member:
        return

    logger.info(
        "[REFERRAL][JOIN_UPDATE] invitee=%s chat_id=%s old_status=%s new_status=%s",
        user.id,
        chat_id,
        old_status,
        new_status,
    )

    if chat_id == GROUP_ID:
        # Group joins: run referral attribution, then the existing
        # group-only welcome/onboarding join handling below.
        _confirm_referral_join(
            user.id,
            invitee_username=user.username,
            invite_link=getattr(member, "invite_link", None),
            chat_id=member.chat.id,
        )

    elif is_channel_chat_id and isinstance(user.id, int):
        # Official-channel joins (including a REFERRAL_DESTINATION_CHAT_ID
        # override chat): run referral attribution when the exact
        # mapped invite link is present, then update channel-subscription
        # bookkeeping. handle_user_join() is intentionally never called for
        # channel events — it is (and must remain) group-only onboarding.
        _confirm_referral_join(
            user.id,
            invitee_username=user.username,
            invite_link=getattr(member, "invite_link", None),
            chat_id=member.chat.id,
        )
        now = now_utc()
        existing_user_doc = users_collection.find_one(
            {"user_id": user.id},
            {"left_official_channel_at": 1, "official_channel_first_subscribed_at": 1},
        ) or {}
        had_left = bool(existing_user_doc.get("left_official_channel_at"))
        is_first_subscribe = not existing_user_doc.get("official_channel_first_subscribed_at")
        set_fields = {
            "rejoined_official_channel_at": now,
            "official_channel_currently_subscribed": True,
        }
        if is_first_subscribe:
            set_fields["official_channel_first_subscribed_at"] = now
        buffer_until = None
        if had_left:
            try:
                buffer_hours = get_rejoin_buffer_settings().get("hours") or REJOIN_CLAIM_BUFFER_HOURS_FALLBACK
            except Exception:
                buffer_hours = REJOIN_CLAIM_BUFFER_HOURS_FALLBACK
            buffer_until = now + timedelta(hours=buffer_hours)
            set_fields["rejoin_buffer_until"] = buffer_until
        users_collection.update_one(
            {"user_id": user.id},
            {"$set": set_fields},
            upsert=True,
        )
        if had_left:
            logger.info(
                "[CHANNEL][REJOIN] uid=%s chat_id=%s buffer_until=%s",
                user.id,
                chat_id,
                buffer_until,
            )
        elif is_first_subscribe:
            logger.info("[CHANNEL][FIRST_JOIN] uid=%s chat_id=%s", user.id, chat_id)

    # 1) 先记录 join（保持你原本逻辑：哪个 chat 触发就记录哪个 chat）
    # handle_user_join() is group-only (it early-returns for any other
    # chat_id anyway) — the explicit guard here keeps the official-channel
    # path from ever calling it, per the referral-channel migration rules.
    if chat_id == GROUP_ID:
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

async def private_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private_chat(update):
        return
    user = update.effective_user
    if not user or user.is_bot:
        return
    _mark_private_interaction(user.id, user.username)
    await _send_welcome_unclaimed_reminder_if_needed(context, user.id)


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

    if query.data == "pm1_subscribed":
        try:
            is_subscribed, _reason = await _check_official_channel_subscribed(context.bot, user_id)
        except Exception:
            logger.exception("[PM1][SUB_VERIFY] uid=%s err=get_chat_member_failed", user_id)
            await query.answer("Try again in 10s.", show_alert=True)
            return

        if not is_subscribed:
            await query.answer("Subscribe first", show_alert=True)
            return

        success_text = (
            "✅ Subscription verified!\n\n"
            "You’re now eligible for the latest news + reward updates.\n"
            "Tap below to open the Mini-App 👇"
        )
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🚀 Open AdvantPlay Mini-App", web_app=WebAppInfo(url=WEBAPP_URL))]]
        )
        try:
            await query.edit_message_text(success_text, reply_markup=keyboard)
        except Exception:
            try:
                await context.bot.send_message(chat_id=user_id, text=success_text, reply_markup=keyboard)
            except Exception:
                logger.exception("[PM1][SUB_VERIFY] uid=%s err=send_failed", user_id)
        return

    if query.data == VERIFY_CALLBACK_DATA:
        result = verify_reactivation_claim(db, int(user_id), membership_checker=check_official_channel_subscribed, now_ref=datetime.now(timezone.utc))
        if result.get("success"):
            await query.answer("Verified", show_alert=True)
        else:
            await query.answer(result.get("message") or "Unable to verify", show_alert=True)
        try:
            await query.edit_message_text(result.get("message") or "Verification updated.")
        except Exception:
            pass
        return

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


_referral_link_generation_last_attempt: dict[int, float] = {}
_REFERRAL_LINK_GENERATION_COOLDOWN_SECONDS = 5


async def generate_referral_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the '🔗 Generate My Referral Link' / Copy-Share button on /start:
    assembles the Share Content package (caption hook + playback URL + the
    user's canonical referral invite link) and replies in-chat with the
    exact copyable message, plus a Share button prefilled with that message.
    """
    query = update.callback_query
    await query.answer()

    user = query.from_user
    uid = user.id
    username = user.username or ""

    logger.info("[REFERRAL][START_CALLBACK] uid=%s", uid)

    now = time.monotonic()
    last_attempt = _referral_link_generation_last_attempt.get(uid)
    if last_attempt is not None and (now - last_attempt) < _REFERRAL_LINK_GENERATION_COOLDOWN_SECONDS:
        logger.info("[REFERRAL][START_CALLBACK_RATE_LIMITED] uid=%s", uid)
        await safe_send_message(
            context.bot,
            chat_id=uid,
            text="⏳ Your referral link is being prepared. Please try again in a moment.",
            uid=uid,
            send_type="referral_link_rate_limited",
            raise_on_non_transient=False,
        )
        return
    _referral_link_generation_last_attempt[uid] = now

    from referral_share_content import generate_share_package

    try:
        result = await asyncio.to_thread(generate_share_package, uid, username)
    except Exception as e:
        logger.error("[REFERRAL][START_CALLBACK_FAILED] uid=%s error=%s", uid, type(e).__name__)
        await safe_send_message(
            context.bot,
            chat_id=uid,
            text=(
                "❌ We couldn’t generate your referral link right now.\n\n"
                "Please tap the button again shortly."
            ),
            uid=uid,
            send_type="referral_link_failed",
            raise_on_non_transient=False,
        )
        return

    if not result.get("ok"):
        code = result.get("code")
        if code == "no_active_playback":
            text = "⏳ No playback is currently available. Please try again later."
        else:
            text = (
                "❌ We couldn’t generate your referral link right now.\n\n"
                "Please tap the button again shortly."
            )
        logger.error("[REFERRAL][START_CALLBACK_FAILED] uid=%s code=%s", uid, code)
        await safe_send_message(
            context.bot,
            chat_id=uid,
            text=text,
            uid=uid,
            send_type="referral_link_failed",
            raise_on_non_transient=False,
        )
        return

    message = result["message"]
    invite_link = result["invite_link"]

    # Telegram's share/url composes the prefilled message as "{url}\n{text}"
    # (url first, then text) — so the link goes only in the url param, and
    # text carries the hook + playback URL *without* the trailing arrow/link
    # line, otherwise the invite link would appear twice in the share sheet.
    share_text = f"{result['hook_text']}\n{result['playback_url']}\n\nMore player replays and rewards inside AdvantPlay:"
    share_params = urlencode({"url": invite_link, "text": share_text})
    share_keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📤 Share Referral Link", url=f"https://t.me/share/url?{share_params}")]]
    )

    await safe_send_message(
        context.bot,
        chat_id=uid,
        text=message,
        disable_web_page_preview=True,
        reply_markup=share_keyboard,
        uid=uid,
        send_type="referral_link",
        raise_on_non_transient=False,
    )
    logger.info("[REFERRAL][START_CALLBACK_OK] uid=%s", uid)

# ----------------------------
# Run Bot + Flask + Scheduler
# ----------------------------
def run_worker():
    transient_polling_errors = (
        NetworkError,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        TimeoutError,
    )
    polling_backoff_seconds = (3, 5, 10, 15, 30)
    stable_start_reset_seconds = 60
    logger.info("[BOOT] worker mode starting")
    try:
        ensure_voucher_indexes()
        ensure_reactivation_journey_indexes(db)
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
    app_bot.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, private_message_handler))
    app_bot.add_handler(CallbackQueryHandler(generate_referral_link_callback, pattern=r"^generate_referral_link$"))
    app_bot.add_handler(CallbackQueryHandler(button_handler))
    from community_centre import register_handlers as _register_community_centre_handlers
    _register_community_centre_handlers(app_bot)

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

    def _scheduler_job_enabled(job_key: str, default: bool = True) -> bool:
        """Live-checked 'Enabled' toggle for a scheduler job, sourced from the
        Admin Dashboard Scheduler settings (falls back to enabled=True)."""
        try:
            job_cfg = get_app_setting("scheduler", job_key)
            if isinstance(job_cfg, dict) and "enabled" in job_cfg:
                return bool(job_cfg["enabled"])
        except Exception:
            logger.exception("[SETTINGS_SCHEDULER] failed to read enabled flag for job_key=%s", job_key)
        return default

    def _guarded_job(job_key: str, fn, *, default: bool = True, feature_flag: str | None = None):
        """Wrap a scheduler job callable so it no-ops when disabled from Settings,
        without needing to unregister/re-register the underlying APScheduler job.

        If ``feature_flag`` is given, the job also no-ops when that
        feature_flags.<flag> setting is off (in addition to its own
        scheduler.<job_key>.enabled toggle)."""
        def _wrapped(*args, **kwargs):
            if not _scheduler_job_enabled(job_key, default):
                logger.info("[SETTINGS_SCHEDULER] skip job_key=%s reason=disabled", job_key)
                return None
            if feature_flag and get_app_setting("feature_flags", feature_flag) is False:
                logger.info("[SETTINGS_SCHEDULER] skip job_key=%s reason=feature_flag_off flag=%s", job_key, feature_flag)
                return None
            return fn(*args, **kwargs)
        _wrapped.__name__ = getattr(fn, "__name__", job_key)
        return _wrapped

    def _sync_scheduler_cron_from_settings() -> None:
        """Every minute, pick up cron changes saved from the Admin Dashboard and
        reschedule the matching APScheduler job(s) — no redeploy required."""
        job_id_map = {
            "xp_snapshot": ["weekly_reset"],
            "pending_referral_settlement": ["tick_5min"],
            "verification_queue": ["process_verification_queue"],
            "welcome_reminder": ["welcome_voucher_lifecycle", "welcome_progress_reminders"],
            "reactivation_journey": ["reactivation_journey_evaluate"],
            "affiliate_monthly_settlement": ["affiliate_monthly_settle"],
            "bot_segment_sheet_sync": ["bot_segment_sheet_sync"],
            "growth_leaderboard_weekly": ["growth_leaderboard_weekly"],
        }
        try:
            scheduler_cfg = settings_service.get_settings("scheduler", force_refresh=True)
        except Exception:
            logger.exception("[SETTINGS_SCHEDULER] failed to load scheduler settings for cron sync")
            return
        for job_key, job_ids in job_id_map.items():
            job_cfg = scheduler_cfg.get(job_key) or {}
            cron = job_cfg.get("cron")
            if not cron:
                continue
            try:
                new_trigger = CronTrigger.from_crontab(cron, timezone=KL_TZ)
            except Exception:
                logger.warning("[SETTINGS_SCHEDULER] invalid cron job_key=%s cron=%s", job_key, cron)
                continue
            for job_id in job_ids:
                job = scheduler.get_job(job_id)
                if not job:
                    continue
                if str(job.trigger) != str(new_trigger):
                    try:
                        scheduler.reschedule_job(job_id, trigger=new_trigger)
                        logger.info("[SETTINGS_SCHEDULER] rescheduled job=%s cron=%s", job_id, cron)
                    except Exception:
                        logger.exception("[SETTINGS_SCHEDULER] failed to reschedule job=%s cron=%s", job_id, cron)

    scheduler.add_job(
        _guarded_job("xp_snapshot", reset_weekly_xp),
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
        _guarded_job("affiliate_monthly_settlement", affiliate_monthly_settle_scheduled),
        trigger=CronTrigger(day=1, hour=0, minute=10, timezone=KL_TZ),
        id="affiliate_monthly_settle",
        name="Affiliate Monthly Settle (Prev Month)",
        replace_existing=True,
    )
    scheduler.add_job(
        affiliate_weekly_settle_scheduled,
        trigger=CronTrigger(day_of_week="mon", hour=0, minute=15, timezone=KL_TZ),
        id="affiliate_weekly_settle",
        name="Affiliate Weekly Settle (Prev Week)",
        replace_existing=True,
    )
    scheduler.add_job(
        affiliate_current_week_issue_scheduled,
        trigger=CronTrigger(minute="*/30", timezone=KL_TZ),
        id="affiliate_current_week_issue",
        name="Affiliate Current Week Issue",
        replace_existing=True,
    )
    scheduler.add_job(
        _guarded_job("pending_referral_settlement", tick_5min),
        trigger=CronTrigger(minute="*/5", timezone=KL_TZ),
        id="tick_5min",
        name="Tick 5min (Settlement)",
        replace_existing=True,
    )
    scheduler.add_job(
        _guarded_job("verification_queue", process_verification_queue_scheduled),
        trigger=CronTrigger(minute="*/2", timezone=KL_TZ),
        id="process_verification_queue",
        name="Process Verification Queue",
        replace_existing=True,
        kwargs={"batch_limit": None},
    )
    scheduler.add_job(
        onboarding_due_tick,
        trigger=CronTrigger(minute="*/1", timezone=KL_TZ),
        id="onboarding_due_tick",
        name="Onboarding Due Tick",
        replace_existing=True,
    )
    scheduler.add_job(
        _guarded_job("welcome_reminder", welcome_voucher_lifecycle_scheduled, feature_flag="welcome_reward"),
        trigger=CronTrigger(minute="*/30", timezone=KL_TZ),
        id="welcome_voucher_lifecycle",
        name="Welcome Voucher Lifecycle",
        replace_existing=True,
        kwargs={"bot_send_fn": _send_welcome_reminder_via_bot},
    )
    scheduler.add_job(
        _guarded_job("welcome_reminder", welcome_progress_reminders_scheduled, feature_flag="welcome_journey"),
        trigger=CronTrigger(minute=0, timezone=KL_TZ),
        id="welcome_progress_reminders",
        name="Welcome Voucher Progress Reminders",
        replace_existing=True,
        kwargs={"bot_send_fn": _send_welcome_reminder_via_bot},
    )
    scheduler.add_job(
        _guarded_job("reactivation_journey", lambda: evaluate_pending_journeys(db, membership_checker=check_official_channel_subscribed, now_ref=datetime.now(timezone.utc), batch_limit=int((get_app_setting("scheduler", "reactivation_journey") or {}).get("batch_size") or 300)), feature_flag="reactivation"),
        trigger=CronTrigger(minute="*/30", timezone=KL_TZ),
        id="reactivation_journey_evaluate",
        name="Reactivation Journey Evaluate",
        replace_existing=True,
    )
    scheduler.add_job(
        reconcile_drop_statuses,
        trigger=CronTrigger(minute="*/1", timezone=KL_TZ),
        id="drop_status_reconcile",
        name="Drop Status Reconcile",
        replace_existing=True,
    )
    scheduler.add_job(
        batch_release_tick,
        trigger=CronTrigger(minute="*/1", timezone=KL_TZ),
        id="batch_release_tick",
        name="Batch Release Campaign Tick",
        replace_existing=True,
    )
    scheduler.add_job(
        evaluate_affiliate_simulated_ledgers,
        trigger=CronTrigger(hour=1, minute=15, timezone=KL_TZ),
        id="affiliate_simulate_daily",
        name="Affiliate Simulation Daily",
        replace_existing=True,
        kwargs={"batch_limit": 1000},
    )
    aff_tz_name = os.getenv("SCHEDULER_CRON_TIMEZONE", "Asia/Kuala_Lumpur")
    aff_tz = pytz.timezone(aff_tz_name)
    scheduler.add_job(
        run_affiliate_dashboard_export_monthly_scheduled,
        trigger=CronTrigger(day=1, hour=8, minute=0, timezone=aff_tz),
        id="affiliate_dashboard_monthly_export",
        name="Affiliate Dashboard Monthly Export",
        replace_existing=True,
    )
    scheduler.add_job(
        compute_affiliate_daily_kpi_yesterday,
        trigger=CronTrigger(hour=0, minute=20, timezone=timezone.utc),
        id="affiliate_daily_kpi",
        name="Affiliate Daily KPI Snapshot",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: compute_affiliate_weekly_kpis_final(db, reference_utc=datetime.now(timezone.utc) - timedelta(seconds=1)),
        trigger=CronTrigger(day_of_week="mon", hour=0, minute=5, timezone=timezone.utc),
        id="affiliate_weekly_kpi",
        name="Affiliate Weekly KPI Snapshot",
        replace_existing=True,
    )

    # Always registered; "Enabled" + cron are live-controlled from Settings ->
    # Scheduler so toggling/rescheduling this job never requires a redeploy.
    scheduler.add_job(
        _guarded_job("bot_segment_sheet_sync", bot_segment_sheet_sync_scheduled, default=os.getenv("BOT_SEGMENT_SYNC_ENABLED", "1") == "1"),
        trigger=CronTrigger(
            day_of_week=os.getenv("BOT_SEGMENT_SYNC_DAY_OF_WEEK", "wed"),
            hour=int(os.getenv("BOT_SEGMENT_SYNC_HOUR", "9")),
            minute=int(os.getenv("BOT_SEGMENT_SYNC_MINUTE", "30")),
            timezone=KL_TZ,
        ),
        id="bot_segment_sheet_sync",
        name="Bot Segment Sheet Sync",
        replace_existing=True,
    )

    def _guarded_growth_leaderboard():
        if not GROWTH_LEADERBOARD_CHANNEL_ID:
            logger.warning("[GROWTH_LEADERBOARD] enabled but missing GROWTH_LEADERBOARD_CHANNEL_ID")
            return None
        return post_growth_leaderboard_weekly()

    growth_tz = pytz.timezone(GROWTH_LEADERBOARD_TIMEZONE)
    scheduler.add_job(
        _guarded_job("growth_leaderboard_weekly", _guarded_growth_leaderboard, default=GROWTH_LEADERBOARD_ENABLED, feature_flag="growth_leaderboard"),
        trigger=CronTrigger(
            day_of_week=GROWTH_LEADERBOARD_CRON_DAY.lower(),
            hour=GROWTH_LEADERBOARD_CRON_HOUR,
            minute=GROWTH_LEADERBOARD_CRON_MINUTE,
            timezone=growth_tz,
        ),
        id="growth_leaderboard_weekly",
        name="Growth Leaderboard Weekly",
        replace_existing=True,
    )
    # Telegram member counts: refreshed only in the worker (where the bot loop
    # exists) and cached in admin_cache for the dashboard to read. First run is
    # delayed so app_bot.run_polling has started its loop; then on an interval.
    tg_refresh_minutes = int((get_app_setting("scheduler", "telegram_member_counts_refresh") or {}).get("interval_minutes") or os.getenv("TELEGRAM_COUNT_REFRESH_MINUTES", "60"))
    tg_first_run_at = datetime.now(timezone.utc) + timedelta(seconds=60)
    scheduler.add_job(
        _guarded_job("telegram_member_counts_refresh", refresh_telegram_member_counts),
        trigger="interval",
        minutes=tg_refresh_minutes,
        next_run_time=tg_first_run_at,
        id="telegram_member_counts_refresh",
        name="Telegram Member Counts Refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info(
        "[DASHBOARD_TG_REFRESH][REGISTERED]\n"
        f"runner_mode={RUNNER_MODE}\n"
        f"official_channel_id={OFFICIAL_CHANNEL_ID}\n"
        f"community_chat_id={_COMMUNITY_CHAT_ID}\n"
        f"interval_minutes={tg_refresh_minutes}\n"
        f"first_run_at={tg_first_run_at.isoformat()}"
    )

    # Community Centre: restart-safe worker tick — publishes due posts via an
    # atomic Mongo claim (safe across multiple instances), recovers stale
    # "processing" posts, and runs auto-unpins. Mongo is the scheduling
    # source of truth, not this in-memory job — a missed/late tick just
    # means a due post waits for the next one.
    from community_centre import community_centre_tick
    scheduler.add_job(
        _guarded_job("community_centre_tick", community_centre_tick),
        trigger="interval",
        seconds=20,
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
        id="community_centre_tick",
        name="Community Centre Tick",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # subscription audit disabled — subscription_cache refreshed via claim + check-in events
    try:
        reconcile_drop_statuses()
        logger.info("[DROP_STATUS] startup_reconcile_ok")
    except Exception as exc:
        logger.exception(
            "[DROP_STATUS] startup_reconcile_failed err=%s msg=%s",
            exc.__class__.__name__,
            str(exc),
        )
    scheduler.start()

    autoscale_state = {"last_target": None}

    def autoscale_web_for_drop() -> None:
        try:
            autoscale_job_cfg = get_app_setting("scheduler", "autoscale_web_for_drop") or {}
            autoscale_enabled = bool(autoscale_job_cfg.get("enabled", os.getenv("AUTOSCALE_ENABLED", "1") == "1"))
            autoscale_lead_minutes = int(os.getenv("AUTOSCALE_LEAD_MINUTES", "2"))
            autoscale_duration_minutes = int(os.getenv("AUTOSCALE_DURATION_MINUTES", "10"))
            autoscale_peak_web = int(os.getenv("AUTOSCALE_PEAK_WEB", "5"))
            autoscale_base_web = int(os.getenv("AUTOSCALE_BASE_WEB", "1"))
            fly_app_name = os.getenv("FLY_APP_NAME", "apreferralv1")
            if not autoscale_enabled:
                return

            now = datetime.now(timezone.utc)
            lead_td = timedelta(minutes=autoscale_lead_minutes)
            dur_td = timedelta(minutes=autoscale_duration_minutes)            
            drop = db.drops.find_one(
                {"startsAt": {"$gte": now - dur_td, "$lte": now + lead_td}},
                sort=[("startsAt", DESCENDING)],
                projection={"startsAt": 1, "name": 1},
            )

            starts_at = None
            if not drop or not drop.get("startsAt"):
                target = autoscale_base_web
                reason = "NO_UPCOMING_DROP"
                window_start = None
                window_end = None
            else:
                starts_at = drop["startsAt"]
                if starts_at.tzinfo is None:
                    starts_at = starts_at.replace(tzinfo=timezone.utc)
                else:
                    starts_at = starts_at.astimezone(timezone.utc)

                window_start = starts_at - timedelta(minutes=autoscale_lead_minutes)
                window_end = starts_at + timedelta(minutes=autoscale_duration_minutes)

                if window_start <= now <= window_end:
                    target = autoscale_peak_web
                    reason = "PEAK_WINDOW"
                else:
                    target = autoscale_base_web
                    reason = "OUTSIDE_WINDOW"

            if target != autoscale_state["last_target"]:
                subprocess.check_call([
                    "flyctl", "scale", "count", str(target),
                    "--process-group", "web",
                    "--app", fly_app_name,
                    "--yes",                    
                ])
                autoscale_state["last_target"] = target
                logger.info(
                    "[AUTOSCALE] web=>%s reason=%s now=%s startsAt=%s window=%s..%s",
                    target,
                    reason,
                    now.isoformat(),
                    starts_at.isoformat() if starts_at else None,
                    window_start.isoformat() if window_start else None,
                    window_end.isoformat() if window_end else None,
                )
        except Exception:
            logger.exception("[AUTOSCALE] autoscale_web_for_drop failed")

    autoscale_interval_seconds = int((get_app_setting("scheduler", "autoscale_web_for_drop") or {}).get("interval_seconds") or os.getenv("AUTOSCALE_INTERVAL_SECONDS", "30"))
    scheduler.add_job(
        autoscale_web_for_drop,
        trigger="interval",
        seconds=autoscale_interval_seconds,
        id="autoscale_web_for_drop",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _sync_scheduler_cron_from_settings,
        trigger="interval",
        minutes=1,
        id="settings_scheduler_sync",
        name="Settings Scheduler Sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # 5) Background jobs on the bot's job_queue
    app_bot.job_queue.run_once(refresh_admin_ids, when=0)
    app_bot.job_queue.run_repeating(refresh_admin_ids, interval=timedelta(minutes=10), first=timedelta(seconds=0))

    print("✅ Bot & Scheduler wired. Starting servers...")

    try:
        attempt = 0
        while True:
            try:
                logger.info("[WORKER] polling start attempt=%s", attempt + 1)
                started_at = time.monotonic()
                app_bot.run_polling(
                    poll_interval=5,
                    allowed_updates=["message", "callback_query", "chat_member", "my_chat_member", "chat_join_request"],
                    close_loop=False,
                )
                logger.info("[WORKER] polling exited cleanly")
                break
            except transient_polling_errors as exc:
                elapsed = time.monotonic() - started_at
                if elapsed >= stable_start_reset_seconds:
                    attempt = 0
                delay = polling_backoff_seconds[min(attempt, len(polling_backoff_seconds) - 1)]
                logger.warning(
                    "[WORKER] transient polling failure err=%s msg=%s elapsed_s=%.1f retry_in_s=%s",
                    exc.__class__.__name__,
                    str(exc),
                    elapsed,
                    delay,
                )
                attempt += 1
                time.sleep(delay)
                continue
            except Exception as exc:
                logger.exception(
                    "[WORKER] fatal polling crash err=%s msg=%s",
                    exc.__class__.__name__,
                    str(exc),
                )
                raise
    finally:
        scheduler.shutdown(wait=False)


def run_web():
    try:
        ensure_voucher_indexes()
        ensure_reactivation_journey_indexes(db)
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
        
