from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.ext import ChatMemberHandler, CallbackQueryHandler
from checkin import handle_checkin
from referral import get_or_create_referral_link
from pymongo import MongoClient, DESCENDING
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from datetime import datetime, timedelta
from bson.json_util import dumps
import os
import asyncio
import traceback
import csv
import io
import pytz

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"
GROUP_ID = -1002304653063

# ----------------------------
# MongoDB Setup
# ----------------------------
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
history_collection = db["weekly_leaderboard_history"]
bonus_voucher_collection = db["bonus_voucher"]
admin_cache_col = db["admin_cache"]

app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

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

ensure_indexes()

def require_admin_from_query():
    caller_id = request.args.get("user_id", type=int)
    if not caller_id:
        return False, ("Missing user_id", 400)

    user = users_collection.find_one({"user_id": caller_id}) or {}
    # Trust the cached is_admin populated by /api/is_admin
    if not user.get("is_admin", False):
        return False, ("Admins only", 403)

    return True, None
    
def is_user_admin(user_id):
    try:
        admins = asyncio.run(app_bot.bot.get_chat_administrators(chat_id=GROUP_ID))
        return any(admin.user.id == user_id for admin in admins)
    except Exception as e:
        print("[Admin Check Error]", e)
        return False

def get_admin_ids_cached(ttl_secs=900):
    doc = admin_cache_col.find_one({"_id": "admins"})
    now = datetime.utcnow()
    if doc and (now - doc["refreshed_at"]).total_seconds() < ttl_secs:
        return set(doc["ids"])

    # refresh from Telegram once
    admins = asyncio.run(app_bot.bot.get_chat_administrators(chat_id=GROUP_ID))
    ids = [a.user.id for a in admins]
    admin_cache_col.update_one(
        {"_id": "admins"},
        {"$set": {"ids": ids, "refreshed_at": now}},
        upsert=True
    )
    return set(ids)
    
# ----------------------------
# Flask App
# ----------------------------
app = Flask(__name__, static_folder="static")
CORS(app, resources={r"/*": {"origins": "*"}})

import pytz
from datetime import datetime, timedelta
from flask import request, jsonify

# -------------------------------
# ✅ Daily Check-in Logic
# -------------------------------
async def process_checkin(user_id, username, region, update=None):
    """Daily check-in logic once region is known (reset at 12AM UTC+8)."""
    tz_utc8 = pytz.timezone("Asia/Kuala_Lumpur")  # or Asia/Singapore
    now_utc8 = datetime.now(tz_utc8)
    today_utc8 = now_utc8.date()

    user = users_collection.find_one({"user_id": user_id}) or {}
    last = user.get("last_checkin")

    if isinstance(last, datetime):
        # Convert last check-in into UTC+8 for comparison
        last_utc8 = last.astimezone(tz_utc8).date()
        if last_utc8 == today_utc8:
            if update and getattr(update, "message", None):
                await update.message.reply_text("⚠️ You already checked in today.")
            return {"success": False, "message": "⚠️ Already checked in today."}

    # ✅ Save last_checkin in UTC (for consistency in DB)
    now_utc = datetime.now(pytz.UTC)

    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "region": region,
                "last_checkin": now_utc,
            },
            "$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20},
        },
        upsert=True,
    )

    if update and getattr(update, "message", None):
        await update.message.reply_text(
            f"✅ Check-in successful! (+20 XP)\n📍 Region: {region}"
        )

    return {"success": True, "message": "✅ Check-in successful! +20 XP"}


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
        referral_link = asyncio.run(get_or_create_referral_link(app_bot.bot, user_id, username))
        return jsonify({"success": True, "referral_link": referral_link})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/is_admin")
def api_is_admin():
    try:
        user_id = int(request.args.get("user_id"))
        is_admin = user_id in get_admin_ids_cached()

        # also cache per-user for UI speed (optional)
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"is_admin": is_admin, "is_admin_checked_at": datetime.utcnow()}},
            upsert=True
        )
        return jsonify({"success": True, "is_admin": is_admin})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# Helper to mask usernames for non-admin views
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
    # --- Admin gate ---
    ok, err = require_admin_from_query()
    if not ok:
        msg, code = err
        return jsonify({"success": False, "message": msg}), code

    try:
        requests = asyncio.run(app_bot.bot.get_chat_join_requests(chat_id=GROUP_ID))
        result = [{"user_id": req.from_user.id, "username": req.from_user.username} for req in requests]
        return jsonify({"success": True, "requests": result})
    except Exception as e:
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
    now = datetime.now(tz)  # tz = Asia/Kuala_Lumpur

    # Last full week [Mon..Sun], assuming this runs every Monday 00:00 KL
    week_end_date = (now - timedelta(days=1)).date()      # Sunday
    week_start_date = week_end_date - timedelta(days=6)   # Monday

    proj = {"user_id": 1, "username": 1, "weekly_xp": 1, "weekly_referral_count": 1}
    top_checkin   = list(users_collection.find({}, proj).sort("weekly_xp", DESCENDING).limit(50))
    top_referrals = list(users_collection.find({}, proj).sort("weekly_referral_count", DESCENDING).limit(50))

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
        "$set": {"weekly_xp": 0, "weekly_referral_count": 0}
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
    tz_kl = timezone("Asia/Kuala_Lumpur")
    now = datetime.now(tz_kl)

    try:
        # weekly catch-up (only on Monday)
        last_history = history_collection.find_one(sort=[("archived_at", DESCENDING)])
        if last_history:
            last_raw = last_history["archived_at"]
            if last_raw.tzinfo is None:
                last_reset = last_raw.replace(tzinfo=pytz.UTC).astimezone(tz_kl)
            else:
                last_reset = last_raw.astimezone(tz_kl)
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
        
def update_monthly_vip_status():
    now = datetime.now(tz)
    print(f"🔁 Running monthly VIP status update at {now}")

    all_users = users_collection.find()

    for user in all_users:
        current_monthly_xp = user.get("monthly_xp", 0)  # ✅ Check monthly XP
        next_status = "VIP1" if current_monthly_xp >= 800 else "Normal"

        users_collection.update_one(
            {"user_id": user["user_id"]},
            {
                "$set": {
                    "status": next_status,
                    "last_status_update": now,
                    "monthly_xp": 0  # ✅ Reset monthly XP
                }
            }
        )

    print("✅ Monthly VIP status update complete.")
    
# ----------------------------
# Telegram Bot Handlers
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
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
        await update.message.reply_text(
    "👋 Welcome! Tap the button below to check-in and get your referral link 👇",
    reply_markup=InlineKeyboardMarkup(keyboard)
)

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
            ref_doc = users_collection.find_one({"referral_link": invite_link.invite_link})
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
        referral_link = await get_or_create_referral_link(
            context.bot,
            user_id,
            query.from_user.username or ""
        )
        await query.edit_message_text(f"👥 Your referral link:\n{referral_link}")

# ----------------------------
# Run Bot + Flask + Scheduler
# ----------------------------
if __name__ == "__main__":
    # Serve Flask in a side thread
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080), daemon=True).start()

    # Catch up if a reset was missed while the app was down
    run_boot_catchup()

    # Telegram handlers
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(ChatMemberHandler(member_update_handler, ChatMemberHandler.CHAT_MEMBER))
    app_bot.add_handler(CallbackQueryHandler(button_handler))

    # Single scheduler with resilience settings
    scheduler = BackgroundScheduler(
        timezone=tz,
        job_defaults={
            "coalesce": True,           # merge missed runs into one
            "misfire_grace_time": 3600, # run if missed by <= 1 hour
            "max_instances": 1
        }
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

    print("✅ Bot & Scheduler running...")

    try:
        app_bot.run_polling(
            poll_interval=5,
            allowed_updates=["message", "callback_query", "chat_member"]
        )
    finally:
        scheduler.shutdown(wait=False)

