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
    ApplicationBuilder, CommandHandler, ChatMemberHandler,
    CallbackQueryHandler, ContextTypes
)
from datetime import datetime, timedelta, timezone
from werkzeug.exceptions import HTTPException

from config import (
    KL_TZ,
    STREAK_MILESTONES,
    XP_BASE_PER_CHECKIN,
    WEEKLY_XP_BUCKET,
    WEEKLY_REFERRAL_BUCKET,
)
from bson.json_util import dumps

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from vouchers import vouchers_bp, ensure_voucher_indexes

from pymongo import MongoClient, DESCENDING, ASCENDING  # keep if used elsewhere
import os, asyncio, traceback, csv, io, requests
import pytz

FIRST_CHECKIN_BONUS_XP = int(os.getenv("FIRST_CHECKIN_BONUS_XP", "200"))

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
monthly_xp_history_collection = db["monthly_xp_history"]
monthly_xp_history_collection.create_index([("month", ASCENDING)])
monthly_xp_history_collection.create_index([("user_id", ASCENDING), ("month", ASCENDING)], unique=True)

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


def add_xp(user_id: int, amount: int, reason: str):
    now_utc = datetime.now(pytz.UTC)
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$inc": {"xp": amount, "weekly_xp": amount, "monthly_xp": amount},
            "$setOnInsert": {"user_id": user_id},
        },
        upsert=True,
    )
    xp_events_collection.insert_one(
        {"user_id": user_id, "amount": amount, "reason": reason, "ts": now_utc}
    )


def maybe_give_first_checkin_bonus(user_id: int):
    already = xp_events_collection.find_one(
        {"user_id": user_id, "reason": "first_checkin_bonus"}
    )
    if already:
        return

    add_xp(user_id, FIRST_CHECKIN_BONUS_XP, reason="first_checkin_bonus")
        
def ensure_indexes():
    """
    Ensure TTL index on bonus_voucher.end_time so docs auto-expire exactly at end_time.
    If an old index exists with different options, drop and recreate.
    """
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

    xp_events_collection.create_index([("user_id", 1), ("reason", 1)])

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

    now_utc = datetime.now(pytz.UTC)
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "region": region,
                "last_checkin": now_utc,
                "streak": streak
            },
            "$inc": {
                "xp": base_xp + bonus_xp,
                "weekly_xp": base_xp + bonus_xp,
                "monthly_xp": base_xp + bonus_xp
            },
            "$max": {"longest_streak": streak}
        },
        upsert=True
    )
    
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
    try:
        user_id = int(request.args.get("user_id"))
        username = request.args.get("username") or "unknown"
        link = get_or_create_referral_invite_link_sync(user_id, username)
        return jsonify({"success": True, "referral_link": link})
    except Exception as e:
        # optional: include a fallback deeplink if available
        bot_username = os.environ.get("BOT_USERNAME", "")
        fallback = f"https://t.me/{bot_username}?start=ref{request.args.get('user_id')}" if bot_username else ""
        return jsonify({"success": False, "error": str(e), "fallback": fallback}), 500

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

        visible_filter = {
            "$or": [
                {"username": {"$exists": True, "$ne": None, "$ne": ""}},
                {"first_name": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        }

        def safe_format(u):
            # Guarantee user_id exists
            if "user_id" not in u:
                u["user_id"] = 0
            return format_username(u, current_user_id, is_admin)

        top_checkins = users_collection.find(visible_filter).sort("weekly_xp", -1).limit(15)
        top_referrals = users_collection.find(visible_filter).sort("weekly_referral_count", -1).limit(15)

        leaderboard = {
            "checkin": [
                {"username": formatted, "xp": u.get("weekly_xp", 0)}
                for u in top_checkins
                if (formatted := safe_format(u))
            ],
            "referral": [
                {"username": formatted, "referrals": u.get("weekly_referral_count", 0)}
                for u in top_referrals
                if (formatted := safe_format(u))
            ]
        }

        user_stats = {
            "xp": user_record.get("weekly_xp", 0),
            "monthly_xp": user_record.get("monthly_xp", 0),
            "referrals": user_record.get("weekly_referral_count", 0),
            "status": user_record.get("status", "Normal")
        }

        return jsonify({
            "success": True,
            "leaderboard": leaderboard,
            "user": user_stats
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


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

    success, message = update_user_xp(username, amount)
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

        # Give reward
        users_collection.update_one(
            {"user_id": int(user_id)},
            {
                "$set": {"username": username, "welcome_xp_claimed": True},
                "$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20},
            },
            upsert=True,
        )

        return jsonify({
            "success": True,
            "message": "🎁 Starter Pack claimed! +20 XP"
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

    except Exception as e:
        print(f"❌ Boot-time catch-up failed: {e}")

def _previous_month_key(dt):
    """Return YYYY-MM string for the month that just ended."""
    year = dt.year
    month = dt.month - 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}-{month:02d}"
    
def update_monthly_vip_status():
    now_kl = datetime.now(KL_TZ)
    now_utc = datetime.now(timezone.utc)
    print(f"🔁 Running monthly VIP status update at {now_kl}")

    snapshot_month = _previous_month_key(now_kl)    
    all_users = users_collection.find()
  
    processed = 0
    for user in users_collection.find():
        current_monthly_xp = user.get("monthly_xp", 0)
        next_status = "VIP1" if current_monthly_xp >= 800 else "Normal"

        monthly_xp_history_collection.update_one(
            {"user_id": user["user_id"], "month": snapshot_month},
            {
                "$set": {
                    "user_id": user["user_id"],
                    "username": user.get("username"),
                    "month": snapshot_month,
                    "monthly_xp": current_monthly_xp,
                    "status_before_reset": user.get("status", "Normal"),
                    "status_after_reset": next_status,
                    "captured_at_utc": now_utc,
                    "captured_at_kl": now_kl.isoformat(),
                }
            },
            upsert=True,
        )

       
        users_collection.update_one(
            {"user_id": user["user_id"]},
            {
                "$set": {
                    "status": next_status,
                    "last_status_update": now_kl,
                    "monthly_xp": 0,
                }
            },
        )
        processed += 1

    print(
        "✅ Monthly VIP status update complete. "
        f"Processed {processed} users and stored history for {snapshot_month}."
    )
    
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
    users_collection.update_one(
        {"user_id": user.id},
        {
            "$set": {
                "xmas_entry_source": "popup",
                "updated_at": datetime.utcnow(),
            },
            "$setOnInsert": {"user_id": user.id},
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
                "last_checkin": None
            }},
            upsert=True
        )
        keyboard = [[
            InlineKeyboardButton("🚀 Open Check-in & Referral", web_app=WebAppInfo(url=WEBAPP_URL))
        ]]
        if message:
            await message.reply_text(
                "👋 Welcome! Tap the button below to check-in and get your referral link 👇",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

async def handle_xmas_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xmas Gift Delight check-in flow triggered via inline keyboard."""
    query = update.callback_query
    user = query.from_user

    await query.answer()

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
        await query.edit_message_text(
            warning_text,
            reply_markup=_xmas_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    now = datetime.utcnow()
    iso_calendar = now.isocalendar()
    updates = {
        "xmas_checked_in": True,
        "xmas_year": iso_calendar.year,
        "xmas_week": iso_calendar.week,
        "xmas_checkin_at": now,
        "updated_at": now,
    }
    if now.month == 12:
        updates["is_new_in_dec"] = True

    users_collection.update_one(
        {"user_id": user.id},
        {
            "$set": updates,
            "$setOnInsert": {"user_id": user.id},
        },
        upsert=True,
    )

    success_text = (
        "✅ Check-in successful!\n\n"
        "You’ve entered this week’s Xmas Gift Delight draw 🎄\n"
        "20 new players will be selected and contacted by this bot. Good luck! 🍀"
    )
    await query.edit_message_text(success_text)
    
async def member_update_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    member = update.chat_member
    user = member.new_chat_member.user

    # Only run when user just joined (status = "member")
    if member.old_chat_member.status not in ("member", "administrator", "creator") \
       and member.new_chat_member.status == "member":
        
        invite_link = member.invite_link  # ✅ Works if they joined via your generated referral link

        existing_user = users_collection.find_one({"user_id": user.id})
        if existing_user and existing_user.get("joined_once"):
            print(f"[Skip XP] {user.username} already joined before.")
            return

        # Insert / update new user
        users_collection.update_one(
            {"user_id": user.id},
            {
                "$set": {"username": user.username, "joined_once": True},
                "$setOnInsert": {
                    "xp": 0,
                    "referral_count": 0,
                    "weekly_referral_count": 0,
                    "weekly_xp": 0,
                    "monthly_xp": 0,
                    "last_checkin": None
                }
            },
            upsert=True
        )

        # Detect referrer same as before
        referrer_id = None
        if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
            referrer_id = int(invite_link.name.split("-")[1])
        elif invite_link and invite_link.invite_link:
            ref_doc = users_collection.find_one({"referral_invite_link": invite_link.invite_link})

            if ref_doc:
                referrer_id = ref_doc["user_id"]

        if referrer_id:
            users_collection.update_one(
                {"user_id": referrer_id},
                {
                    "$inc": {
                        "referral_count": 1,
                        "weekly_referral_count": 1,
                        "xp": 30,
                        "weekly_xp": 30,
                        "monthly_xp": 30
                    }
                }
            )
            
            referrer = users_collection.find_one({"user_id": referrer_id})
            
            total_referrals = referrer.get("referral_count", 0)

            if total_referrals % 3 == 0:
                users_collection.update_one(
                    {"user_id": referrer_id},
                    {"$inc": {"xp": 200, "weekly_xp": 200, "monthly_xp": 200}}
                )
                try:
                    await context.bot.send_message(
                        referrer_id,
                        f"🎉 Congrats! You earned +200 XP bonus for reaching {total_referrals} referrals!"
                    )
                except Exception as e:
                    print(f"[Referral Bonus] Failed to send message: {e}")

            try:
                maybe_shout_milestones(int(referrer_id))
            except (TypeError, ValueError):
                pass

            print(f"[Referral] {user.username} referred by {referrer_id}")
        else:
            print(f"[Referral] No referrer found for {user.username}")

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
                    "$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20},
                    "$set": {"welcome_xp_claimed": True}
                },
                upsert=True
            )
            await query.edit_message_text("✅ You received +20 XP welcome bonus!")

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
    app_bot.add_handler(ChatMemberHandler(member_update_handler, ChatMemberHandler.CHAT_MEMBER))
    app_bot.add_handler(CallbackQueryHandler(handle_xmas_checkin, pattern="^xmas_checkin$"))
    app_bot.add_handler(CallbackQueryHandler(button_handler))

    # 4) Scheduler (KL time for human-facing schedules)
    scheduler = BackgroundScheduler(
        timezone=KL_TZ,
        job_defaults={"coalesce": True, "misfire_grace_time": 3600, "max_instances": 1}
    )
    scheduler.add_job(
        reset_weekly_xp,
        trigger=CronTrigger(day_of_week="mon", hour=0, minute=0),
        id="weekly_reset",
        name="Weekly XP Reset",
        replace_existing=True,
    )
    scheduler.add_job(
        update_monthly_vip_status,
        trigger=CronTrigger(day=1, hour=0, minute=0),
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
            allowed_updates=["message", "callback_query", "chat_member"]
        )
    finally:
        scheduler.shutdown(wait=False)
