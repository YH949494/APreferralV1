from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ChatJoinRequestHandler, ContextTypes
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

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"
GROUP_ID = -1002723991859

# ----------------------------
# MongoDB Setup
# ----------------------------
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
history_collection = db["weekly_leaderboard_history"]

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
        admins = asyncio.run(app_bot.bot.get_chat_administrators(chat_id=GROUP_ID))
        is_admin = any(admin.user.id == user_id for admin in admins)
        return jsonify({"success": True, "is_admin": is_admin})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        user_id = int(request.args.get("user_id", 0))

        def format_username(u):
            if u.get("username"):
                return f"@{u['username']}"
            elif u.get("first_name"):
                return u["first_name"]
            else:
                return f"ID:{u.get('user_id', 'N/A')}"

        top_checkins = list(users_collection.find().sort("weekly_xp", -1).limit(10))
        top_referrals = list(users_collection.find().sort("referral_count", -1).limit(10))

        leaderboard = {
            "checkin": [{"username": format_username(u), "xp": u.get("weekly_xp", 0)} for u in top_checkins],
            "referral": [{"username": format_username(u), "referrals": u.get("referral_count", 0)} for u in top_referrals]
        }

        user = users_collection.find_one({"user_id": user_id})
        user_stats = {
            "xp": user.get("weekly_xp", 0) if user else 0,
            "referrals": user.get("referral_count", 0) if user else 0
        }

        return jsonify({"success": True, "leaderboard": leaderboard, "user": user_stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
        
@app.route("/api/leaderboard/history")
def get_leaderboard_history():
    try:
        last_record = history_collection.find().sort("archived_at", DESCENDING).limit(1).next()
        return jsonify({
            "success": True,
            "week_start": last_record.get("week_start"),
            "week_end": last_record.get("week_end"),
            "checkin": last_record.get("checkin_leaderboard", []),
            "referral": last_record.get("referral_leaderboard", [])
        })
    except StopIteration:
        return jsonify({"success": False, "message": "No history found."}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

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
    top_referrals = list(users_collection.find().sort("referral_count", DESCENDING).limit(50))

    history_collection.insert_one({
        "week_start": (now - timedelta(days=7)).strftime('%Y-%m-%d'),
        "week_end": now.strftime('%Y-%m-%d'),
        "checkin_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username", "unknown"), "weekly_xp": u.get("weekly_xp", 0)}
            for u in top_checkin
        ],
        "referral_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username", "unknown"), "referral_count": u.get("referral_count", 0)}
            for u in top_referrals
        ],
        "archived_at": now
    })

    users_collection.update_many({}, {"$set": {"weekly_xp": 0, "referral_count": 0}})
    print(f"✅ Weekly XP and referral reset complete at {now}")

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
    else:
        users_collection.update_one(
            {"user_id": user.id},
            {"$set": {"username": user.username, "joined_once": True},
             "$setOnInsert": {"xp": 0, "referral_count": 0, "weekly_xp": 0, "last_checkin": None}},
            upsert=True
        )

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
                {"$inc": {"referral_count": 1, "xp": 20, "weekly_xp": 20}}
            )
            print(f"[Referral] {user.username} referred by {referrer_id}")
        else:
            print(f"[Referral] No referrer found for {user.username}")

    await context.bot.approve_chat_join_request(update.chat_join_request.chat.id, user.id)

# ----------------------------
# Run Bot + Flask + Scheduler
# ----------------------------
if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080)).start()

    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))

    scheduler = BackgroundScheduler(timezone=tz)
    scheduler.add_job(reset_weekly_xp, trigger=CronTrigger(day_of_week="mon", hour=0, minute=0), name="Weekly XP Reset")
    scheduler.start()

    print("✅ Bot & Scheduler running...")
    app_bot.run_polling()
