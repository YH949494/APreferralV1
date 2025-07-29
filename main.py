from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatJoinRequestHandler, ChatMemberHandler, ContextTypes
)
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

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"

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
        referral_link = asyncio.run(
            get_or_create_referral_link(app_bot.bot, user_id, username)
        )
        return jsonify({"success": True, "referral_link": referral_link})
    except Exception as e:
        print("[API Referral Error]")
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        top_checkins = list(users_collection.find().sort("weekly_xp", -1).limit(10))
        top_referrals = list(users_collection.find().sort("referral_count", -1).limit(10))
        leaderboard = {
            "checkin": [
                {"username": u.get("username", "unknown"), "xp": u.get("weekly_xp", 0)}
                for u in top_checkins
            ],
            "referral": [
                {"username": u.get("username", "unknown"), "referrals": u.get("referral_count", 0)}
                for u in top_referrals
            ]
        }
        return jsonify({"success": True, "leaderboard": leaderboard})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/leaderboard/history")
def get_leaderboard_history():
    try:
        last_entry = history_collection.find().sort("archived_at", DESCENDING).limit(1)
        last_record = next(last_entry, None)
        if not last_record:
            return jsonify({"success": False, "message": "No leaderboard history found."}), 404

        return jsonify({
            "success": True,
            "week_start": last_record.get("week_start"),
            "week_end": last_record.get("week_end"),
            "checkin": last_record.get("checkin_leaderboard", []),
            "referral": last_record.get("referral_leaderboard", [])
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ----------------------------
# Weekly XP Reset Job
# ----------------------------
tz = timezone("Asia/Kuala_Lumpur")

def reset_weekly_xp():
    now = datetime.now(tz)
    print(f"[{now}] 🔄 Resetting weekly XP & archiving leaderboard...")

    top_checkin = list(users_collection.find().sort("weekly_xp", DESCENDING).limit(50))
    top_referrals = list(users_collection.find().sort("referral_count", DESCENDING).limit(50))

    history_collection.insert_one({
        "week_start": (now - timedelta(days=7)).strftime('%Y-%m-%d'),
        "week_end": now.strftime('%Y-%m-%d'),
        "checkin_leaderboard": [
            {
                "user_id": u["user_id"],
                "username": u.get("username", "unknown"),
                "weekly_xp": u.get("weekly_xp", 0)
            } for u in top_checkin
        ],
        "referral_leaderboard": [
            {
                "user_id": u["user_id"],
                "username": u.get("username", "unknown"),
                "referral_count": u.get("referral_count", 0)
            } for u in top_referrals
        ],
        "archived_at": now
    })

    result = users_collection.update_many({}, {"$set": {"weekly_xp": 0}})
    print(f"✅ Archived and reset weekly XP for {result.modified_count} users.")

# ----------------------------
# Telegram Bot Handlers
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    if not users_collection.find_one({"user_id": user.id}):
        users_collection.insert_one({
            "user_id": user.id,
            "username": user.username,
            "xp": 0,
            "weekly_xp": 0,
            "referral_count": 0,
            "last_checkin": None,
            "joined_once": False
        })

    keyboard = [[
        InlineKeyboardButton(
            text="🚀 Open Check-in & Referral",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )
    ]]
    await update.message.reply_text(
        "👋 Welcome! Tap the button below to check-in and get your referral link 👇",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user
    invite_link = update.chat_join_request.invite_link
    existing_user = users_collection.find_one({"user_id": user.id})

    if existing_user and existing_user.get("joined_once"):
        print(f"[No XP] {user.username} has already joined before.")
    else:
        users_collection.update_one(
            {"user_id": user.id},
            {
                "$set": {
                    "username": user.username,
                    "joined_once": True,
                    "join_time": datetime.utcnow()
                },
                "$setOnInsert": {
                    "xp": 0,
                    "referral_count": 0,
                    "weekly_xp": 0,
                    "last_checkin": None
                }
            },
            upsert=True
        )

        try:
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
                print(f"[Referral] {user.username} joined using {referrer_id}'s link.")
            else:
                print(f"[Referral] Could not determine referrer for {user.username}")
        except Exception as e:
            print(f"[Referral Error] {e}")

    await context.bot.approve_chat_join_request(update.chat_join_request.chat.id, user.id)

async def monitor_leaving(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_change = update.chat_member
    user = status_change.from_user

    if status_change.old_chat_member.status in ["member", "restricted"] and status_change.new_chat_member.status == "left":
        user_data = users_collection.find_one({"user_id": user.id})
        if not user_data:
            return

        join_time = user_data.get("join_time")
        if join_time:
            elapsed = datetime.utcnow() - join_time
            if elapsed.total_seconds() < 3600:
                print(f"[ABUSE FLAGGED] {user.username} left group after {elapsed.total_seconds():.0f}s")
                users_collection.update_one(
                    {"user_id": user.id},
                    {"$set": {"abuse_flagged": True}}
                )

# ----------------------------
# Run Bot + Flask + Scheduler
# ----------------------------
if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080)).start()

    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))
    app_bot.add_handler(ChatMemberHandler(monitor_leaving, chat_member_types=["member"]))

    scheduler = BackgroundScheduler(timezone=tz)
    scheduler.add_job(
        reset_weekly_xp,
        trigger=CronTrigger(day_of_week="mon", hour=0, minute=0),
        name="Weekly XP Reset"
    )
    scheduler.start()

    print("✅ Bot & Scheduler running...")
    app_bot.run_polling()
