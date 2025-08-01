import os
import logging
import asyncio
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory
from telegram import Update, Bot, WebAppInitData
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import MongoClient
import csv

from checkin import handle_checkin
from referral import get_or_create_referral_link

# === Environment & Setup ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

bot = Bot(BOT_TOKEN)
app = Flask(__name__, static_folder="static")

# === MongoDB ===
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
weekly_collection = db["weekly_leaderboards"]

# === Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Telegram Bot Handlers ===

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Welcome! Use the button below to open the app.", reply_markup=None)

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user
    inviter_id = context.bot_data.get(str(user.id))
    if inviter_id:
        existing = users_collection.find_one({"user_id": user.id})
        if not existing:
            users_collection.update_one(
                {"user_id": inviter_id},
                {"$inc": {"xp": 50, "weekly_xp": 50, "referral_count": 1}}
            )
            logger.info(f"User {inviter_id} referred {user.id}")
    await update.chat_join_request.approve()

def reset_weekly_xp():
    now = datetime.utcnow()
    leaderboard = list(users_collection.find({}, {"_id": 0, "user_id": 1, "username": 1, "weekly_xp": 1}))
    for user in leaderboard:
        user["week"] = now.strftime("%Y-%m-%d")
    if leaderboard:
        weekly_collection.insert_many(leaderboard)
    users_collection.update_many({}, {"$set": {"weekly_xp": 0}})
    logger.info("✅ Weekly XP reset complete.")

# === Flask Web Server Routes ===

@app.route("/")
def home():
    return "Bot is running."

@app.route("/miniapp")
def serve_index():
    return send_from_directory("static", "index.html")

@app.route("/checkin")
def checkin():
    return handle_checkin()

@app.route("/referral")
def referral():
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    link = get_or_create_referral_link(user_id)
    return jsonify({"referral_link": link})

@app.route("/api/leaderboard")
def leaderboard():
    top_users = list(users_collection.find().sort("weekly_xp", -1).limit(10))
    leaderboard_data = [
        {
            "username": user.get("username", "N/A"),
            "xp": user.get("xp", 0),
            "weekly_xp": user.get("weekly_xp", 0),
            "referrals": user.get("referral_count", 0)
        } for user in top_users
    ]
    return jsonify(leaderboard_data)

@app.route("/api/leaderboard/history")
def leaderboard_history():
    records = list(weekly_collection.find({}, {"_id": 0}))
    return jsonify(records)

@app.route("/api/admin/check", methods=["POST"])
def is_admin():
    data = request.json
    init_data = data.get("initData")
    if not init_data:
        return jsonify({"error": "Missing initData"}), 400

    try:
        decoded = WebAppInitData.parse(init_data, BOT_TOKEN)
        user_id = decoded.user.id
        chat_id = decoded.chat.id if decoded.chat else None

        if not chat_id:
            return jsonify({"is_admin": False}), 200

        member = asyncio.get_event_loop().run_until_complete(
            bot.get_chat_member(chat_id, user_id)
        )
        is_admin = member.status in ("administrator", "creator")
        return jsonify({"is_admin": is_admin})
    except Exception as e:
        logger.error(f"Admin check failed: {e}")
        return jsonify({"is_admin": False})

@app.route("/api/admin/update_xp", methods=["POST"])
def update_xp():
    data = request.json
    init_data = data.get("initData")
    target_username = data.get("username")
    amount = data.get("amount", 0)
    action = data.get("action")

    if not all([init_data, target_username, action]):
        return jsonify({"error": "Missing parameters"}), 400

    try:
        decoded = WebAppInitData.parse(init_data, BOT_TOKEN)
        user_id = decoded.user.id
        chat_id = decoded.chat.id if decoded.chat else None

        if not chat_id:
            return jsonify({"error": "Not in group context"}), 403

        member = asyncio.get_event_loop().run_until_complete(
            bot.get_chat_member(chat_id, user_id)
        )
        if member.status not in ("administrator", "creator"):
            return jsonify({"error": "Not authorized"}), 403

        user = users_collection.find_one({"username": target_username})
        if not user:
            return jsonify({"error": "User not found"}), 404

        if action == "add":
            users_collection.update_one(
                {"username": target_username},
                {"$inc": {"xp": amount, "weekly_xp": amount}}
            )
        elif action == "remove":
            xp = max(user.get("xp", 0) - amount, 0)
            weekly_xp = max(user.get("weekly_xp", 0) - amount, 0)
            users_collection.update_one(
                {"username": target_username},
                {"$set": {"xp": xp, "weekly_xp": weekly_xp}}
            )
        else:
            return jsonify({"error": "Invalid action"}), 400

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Admin XP update failed: {e}")
        return jsonify({"error": "Server error"}), 500

@app.route("/api/admin/export_csv")
def export_csv():
    users = list(users_collection.find({}, {"_id": 0}))
    csv_path = "/tmp/users.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "username", "xp", "weekly_xp", "referral_count"])
        writer.writeheader()
        writer.writerows(users)
    return send_from_directory("/tmp", "users.csv", as_attachment=True)

# === App Initialization ===

def start_bot():
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.StatusUpdate.JOIN_REQUEST, handle_join_request))
    application.run_polling()

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(reset_weekly_xp, "cron", day_of_week="sun", hour=23, minute=59)
    scheduler.start()

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, start_bot)

    app.run(host="0.0.0.0", port=8080)
