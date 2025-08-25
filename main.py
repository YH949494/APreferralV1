from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ChatJoinRequestHandler, ContextTypes
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

def is_user_admin(user_id):
    try:
        admins = asyncio.run(app_bot.bot.get_chat_administrators(chat_id=GROUP_ID))
        return any(admin.user.id == user_id for admin in admins)
    except Exception as e:
        print("[Admin Check Error]", e)
        return False
        
# ----------------------------
# Flask App
# ----------------------------
app = Flask(__name__, static_folder="static")
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route("/")
def home():
    return "Bot is alive!"

@app.route("/miniapp")
def serve_mini_app():
    return send_from_directory("static", "index.html")

@app.route("/api/checkin")
def api_checkin():
    return handle_checkin()

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
        user_record = users_collection.find_one({"user_id": user_id}) or {}
        is_admin = bool(user_record.get("is_admin", False))

        # ✅ Store admin status in MongoDB for later use
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"is_admin": is_admin}},
            upsert=True
        )

        return jsonify({"success": True, "is_admin": is_admin})
    except Exception as e:
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

@app.route("/history/weeks", methods=["GET"])
def get_all_weeks():
    """Return list of archived weeks available."""
    weeks = history_collection.find({}, {"week_start": 1, "week_end": 1, "_id": 0}).sort("archived_at", DESCENDING)
    return dumps(list(weeks)), 200

@app.route("/history/week/<week_start>", methods=["GET"])
def get_week_history(week_start):
    """Return archived leaderboard for a given week_start (format YYYY-MM-DD)."""
    history = history_collection.find_one({"week_start": week_start}, {"_id": 0})
    if not history:
        return jsonify({"error": "No record found for that week"}), 404
    return dumps(history), 200

@app.route("/api/admin/set_bonus", methods=["POST"])
def set_bonus_voucher():
    data = request.json
    code = data.get("code")
    release_time_str = data.get("release_time")  # e.g. "2025-08-10T04:00:00Z"

    try:
        release_time = datetime.strptime(release_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
        end_time = release_time + timedelta(hours=24)

        bonus_voucher_collection.delete_many({})
        bonus_voucher_collection.insert_one({
            "code": code,
            "start_time": release_time,
            "end_time": end_time
        })

        return jsonify({"status": "success", "message": "Bonus voucher set successfully."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route("/api/bonus_voucher", methods=["GET"])
def get_bonus_voucher():
    try:
        user_id = int(request.args.get("user_id"))
        user = users_collection.find_one({"user_id": user_id})

        if not user:
            return jsonify({"code": None})

        # Check admin status correctly using helper
        is_admin = user.get("is_admin", False)
        is_vip = user.get("status") == "VIP1"

        # Only allow VIP1 or admin to proceed
        if not is_vip and not is_admin:
            print("[VOUCHER] User not eligible (not VIP1 or admin).")
            return jsonify({"code": None})

        now = datetime.utcnow().replace(tzinfo=pytz.UTC)

        # Auto-delete expired vouchers
        bonus_voucher_collection.delete_many({"end_time": {"$lt": now}})

        voucher = bonus_voucher_collection.find_one()
        if not voucher:
            print("[VOUCHER] No voucher found.")
            return jsonify({"code": None})

        start = voucher["start_time"]
        end = voucher["end_time"]
        if start.tzinfo is None:
            start = start.replace(tzinfo=pytz.UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=pytz.UTC)

        print(f"[VOUCHER] Current server time: {now.isoformat()}")
        print(f"[VOUCHER] Voucher start: {start.isoformat()}, end: {end.isoformat()}")

        if start <= now <= end:
            print("[VOUCHER] Voucher is active and user is eligible.")
            return jsonify({"code": voucher["code"]})
        else:
            print("[VOUCHER] Voucher not active.")
            return jsonify({"code": None})
    except Exception as e:
        print("[VOUCHER] Exception:", e)
        return jsonify({"code": None, "error": str(e)}), 500

# ✅ Add/Reduce XP endpoint
@app.route("/api/add_xp", methods=["POST"])
def api_add_xp():
    from database import update_user_xp  # ✅ import here to avoid circular import
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
    try:
        requests = asyncio.run(app_bot.bot.get_chat_join_requests(chat_id=GROUP_ID))
        result = [{"user_id": req.from_user.id, "username": req.from_user.username} for req in requests]
        return jsonify({"success": True, "requests": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/export_csv")
def export_csv():
    try:
        users = users_collection.find()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "username", "xp", "weekly_xp", "referral_count"])
        for u in users:
            writer.writerow([
                u.get("user_id"),
                u.get("username", ""),
                u.get("xp", 0),
                u.get("weekly_xp", 0),
                u.get("referral_count", 0),
            ])
        output.seek(0)
        return output.getvalue()
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ----------------------------
# Weekly XP Reset Job
# ----------------------------
tz = timezone("Asia/Kuala_Lumpur")

def reset_weekly_xp():
    now = datetime.now(tz)
    top_checkin = list(users_collection.find().sort("weekly_xp", DESCENDING).limit(50))
    top_referrals = list(users_collection.find().sort("weekly_referral_count", DESCENDING).limit(50))

    history_collection.insert_one({
        "week_start": (now - timedelta(days=7)).strftime('%Y-%m-%d'),
        "week_end": now.strftime('%Y-%m-%d'),
        "checkin_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_xp": u.get("weekly_xp", 0)}
            for u in top_checkin
        ],
        "referral_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_referral_count": u.get("weekly_referral_count", 0)}
            for u in top_referrals
        ],
        "archived_at": now
    })

    users_collection.update_many({}, {
        "$set": {
            "weekly_xp": 0,
            "weekly_referral_count": 0  # ✅ add this for weekly leaderboard
        }
    })

    print(f"✅ Weekly XP & referrals reset complete at {now}")

from pymongo import DESCENDING
from pytz import timezone
from datetime import datetime

def fix_user_monthly_xp(user_id):
    user = users_collection.find_one({"user_id": user_id})
    if user and "monthly_xp" not in user:
        # calculate XP somehow or set to 0
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"monthly_xp": 0}}
        )
        print(f"Set missing monthly_xp for user {user_id} to 0")
        return True
    return False

def run_boot_catchup():
    """Run weekly and monthly catch-up if missed due to downtime and fix missing XP for old users."""
    tz_kl = timezone("Asia/Kuala_Lumpur")
    now = datetime.now(tz_kl)

    try:
        # --- Weekly catch-up ---
        last_history = history_collection.find_one(sort=[("archived_at", DESCENDING)])
        if last_history:
            last_reset = last_history["archived_at"].astimezone(tz_kl)
            days_since = (now - last_reset).days
            print(f"📅 Last weekly reset: {last_reset}, {days_since} days ago.")
        else:
            days_since = 999
            print("⚠️ No weekly reset history found.")

        if now.weekday() == 0 and days_since >= 6:
            print("⚠️ Missed weekly reset. Running now...")
            reset_weekly_xp()
        else:
            print("✅ No weekly catch-up needed.")

        # --- Monthly catch-up ---
        sample_user = users_collection.find_one(
            {"last_status_update": {"$exists": True}},
            sort=[("last_status_update", DESCENDING)]
        )

        if (
            not sample_user
            or sample_user["last_status_update"].month != now.month
            or sample_user["last_status_update"].year != now.year
        ):
            print("⚠️ Missed monthly VIP update. Running now...")
            update_monthly_vip_status()
        else:
            print("✅ No monthly catch-up needed.")

        # --- Auto-fix missing XP for old users ---
        print("🔄 Starting auto-fix for missing XP on all users...")
        all_users = users_collection.find({})
        fixed_weekly_count = 0
        fixed_monthly_count = 0
        for user in all_users:
            user_id = user["user_id"]
            if not user_id:
                continue  # Skip if no user_id present
            if fix_user_weekly_xp(user_id):
                fixed_weekly_count += 1
            if fix_user_monthly_xp(user_id):
                fixed_monthly_count += 1
        print(f"✅ Auto-fix completed. Weekly XP fixed for {fixed_weekly_count} users, Monthly XP fixed for {fixed_monthly_count} users.")

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
        await update.message.reply_text("👋 Welcome! Tap the button below to check-in and get your referral link 👇", reply_markup=InlineKeyboardMarkup(keyboard))

async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user
    invite_link = update.chat_join_request.invite_link

    existing_user = users_collection.find_one({"user_id": user.id})
    if existing_user and existing_user.get("joined_once"):
        print(f"[Skip XP] {user.username} already joined before.")
        return

    # New user, insert/update profile
    users_collection.update_one(
        {"user_id": user.id},
        {
            "$set": {"username": user.username, "joined_once": True},
            "$setOnInsert": {
                "xp": 0,
                "referral_count": 0,
                "weekly_referral_count": 0,   # <-- add this so new users have it
                "weekly_xp": 0,
                "monthly_xp": 0,
                "last_checkin": None
            }
        },
        upsert=True
    )

    referrer_id = None

    # Case 1: custom bot referral link (with name = "ref-<id>")
    if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
        referrer_id = int(invite_link.name.split("-")[1])

    # Case 2: normal Telegram invite link (match by referral_link stored in DB)
    elif invite_link and invite_link.invite_link:
        ref_doc = users_collection.find_one({"referral_link": invite_link.invite_link})
        if ref_doc:
            referrer_id = ref_doc["user_id"]

    if referrer_id:
        users_collection.update_one(
            {"user_id": referrer_id},
            {
                "$inc": {
                    "referral_count": 1,        # lifetime total
                    "weekly_referral_count": 1, # ✅ now increments properly
                    "xp": 30,
                    "weekly_xp": 30,
                    "monthly_xp": 30
                }
            }
        )

        # Fetch updated referral count
        referrer = users_collection.find_one({"user_id": referrer_id})
        total_referrals = referrer.get("referral_count", 0)

        # Bonus every 3 referrals
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

    await send_starter_pack(user, context)

# Starter Pack
# ----------------------------
def starter_pack_keyboard(user_id):
    keyboard = [
        [InlineKeyboardButton("📲 Follow @advantplayofficial", url="https://t.me/advantplayofficial")],
        [InlineKeyboardButton("✅ Check-in", callback_data="checkin")],
        [InlineKeyboardButton("👥 My Referral Link", callback_data=f"referral_{user_id}")],
        [InlineKeyboardButton("▶️ Start Bot", url=f"https://t.me/YOUR_BOT_USERNAME?start={user_id}")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def send_starter_pack(user, context):
    text = (
        "🎉 Welcome to AdvantPlay Chat Room!\n\n"
        "🚦 Rules:\n"
        "1. Be kind & respectful\n"
        "2. No spam\n"
        "3. Have fun\n\n"
        "🎮 Earn XP:\n"
        "✅ Check-in: +20 XP\n"
        "👥 Referral: +30 XP\n"
        "🌟 Community = Voucher drops, Tips, Q&A & Help\n\n"
        "⚡ Start now 👇"
    )
    try:
        await context.bot.send_message(
            chat_id=user.id,
            text=text,
            reply_markup=starter_pack_keyboard(user.id)
        )
        print(f"✅ Starter pack sent to {user.id}")
    except Exception as e:
        print(f"❌ Cannot PM {user.id}: {e}")

async def new_member(update, context):
    for member in update.chat_member.new_chat_members:
        user_id = member.id

        # check MongoDB if already joined
        existing = users_collection.find_one({"user_id": user_id})
        if existing and existing.get("joined_once"):
            print(f"⏩ Skip starter pack for {user_id}, already joined.")
            continue

        # send starter pack
        await send_starter_pack(member, context)

        # mark as joined once
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"joined_once": True}},
            upsert=True
        )

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
                {"$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20},
                 "$set": {"welcome_xp_claimed": True}},
                upsert=True
            )
            await query.edit_message_text("✅ You received +20 XP welcome bonus!")

    elif query.data.startswith("referral_"):
        user_id_ref = query.data.split("_")[1]
        referral_link = f"https://t.me/APreferralV1_bot?start={user_id_ref}"
        await query.edit_message_text(f"👥 Your referral link:\n{referral_link}")
        
# ----------------------------
# Run Bot + Flask + Scheduler
# ----------------------------
if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080)).start()
    
    # Boot-time catch-up for missed weekly resets
    run_boot_catchup()

    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))
    app_bot.add_handler(ChatMemberHandler(new_member, ChatMemberHandler.CHAT_MEMBER))
    app_bot.add_handler(CallbackQueryHandler(button_handler))

    scheduler = BackgroundScheduler(timezone=tz)
    scheduler.add_job(reset_weekly_xp, trigger=CronTrigger(day_of_week="mon", hour=0, minute=0), name="Weekly XP Reset")
    scheduler.add_job(update_monthly_vip_status, trigger=CronTrigger(day=1, hour=0, minute=0), name="Monthly VIP Status Update")
    scheduler.start()

    print("✅ Bot & Scheduler running...")
    app_bot.run_polling()
