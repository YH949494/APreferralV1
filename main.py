import os
import logging
from flask import Flask, request, jsonify, send_file
from pymongo import MongoClient, DESCENDING
from telegram import Bot
from telegram.ext import Application, CommandHandler
from telegram.constants import ChatMemberStatus
from apscheduler.schedulers.background import BackgroundScheduler
from io import StringIO
import csv
import asyncio
from datetime import datetime

from checkin import handle_checkin
from referral import get_or_create_referral_link

# Setup logging
logging.basicConfig(level=logging.INFO)

# Telegram bot and group setup
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
bot = Bot(BOT_TOKEN)
app_bot = Application.builder().token(BOT_TOKEN).build()

# Flask app
app = Flask(__name__)

# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
history_collection = db["weekly_history"]
pending_referrals = {}

# ========== MINI APP ROUTES ==========

@app.route("/miniapp")
def miniapp_home():
    return app.send_static_file("index.html")

@app.route("/api/checkin")
def api_checkin():
    return handle_checkin()

@app.route("/api/referral")
def api_referral():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")
    return get_or_create_referral_link(user_id, username)

@app.route("/api/leaderboard")
def get_leaderboard():
    top_checkins = list(users_collection.find().sort("weekly_xp", -1).limit(10))
    top_referrals = list(users_collection.find().sort("referral_count", -1).limit(10))
    leaderboard = {
        "checkin": [{"username": u.get("username", "unknown"), "xp": u.get("weekly_xp", 0)} for u in top_checkins],
        "referral": [{"username": u.get("username", "unknown"), "referrals": u.get("referral_count", 0)} for u in top_referrals]
    }
    return jsonify(leaderboard)

@app.route("/api/weekly-history")
def get_weekly_history():
    history = list(history_collection.find().sort("week_start", DESCENDING).limit(5))
    for entry in history:
        entry["_id"] = str(entry["_id"])
    return jsonify(history)

# ========== ADMIN ROUTES ==========

@app.route("/api/admin/check")
def admin_check():
    user_id = request.args.get("user_id", type=int)
    try:
        loop = asyncio.get_event_loop()
        admins = loop.run_until_complete(bot.get_chat_administrators(chat_id=GROUP_ID))
        is_admin = any(admin.user.id == user_id for admin in admins)
        return jsonify({"is_admin": is_admin})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/xp", methods=["POST"])
def admin_xp():
    try:
        data = request.json
        admin_id = int(data.get("admin_id"))
        username = data.get("username")
        amount = int(data.get("amount"))
        action = data.get("action")

        # Validate admin
        loop = asyncio.get_event_loop()
        admins = loop.run_until_complete(bot.get_chat_administrators(chat_id=GROUP_ID))
        if not any(admin.user.id == admin_id for admin in admins):
            return jsonify({"success": False, "message": "Unauthorized"}), 403

        user = users_collection.find_one({"username": username})
        if not user:
            return jsonify({"success": False, "message": "User not found"}), 404

        change = amount if action == "add" else -amount
        users_collection.update_one(
            {"username": username},
            {"$inc": {"xp": change, "weekly_xp": change}}
        )
        return jsonify({"success": True, "message": f"{action.title()}ed {amount} XP to {username}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/admin/export")
def admin_export():
    admin_id = request.args.get("admin_id", type=int)
    try:
        loop = asyncio.get_event_loop()
        admins = loop.run_until_complete(bot.get_chat_administrators(chat_id=GROUP_ID))
        if not any(admin.user.id == admin_id for admin in admins):
            return jsonify({"error": "Unauthorized"}), 403

        users = list(users_collection.find())
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Username", "XP", "Weekly XP", "Referrals"])
        for u in users:
            writer.writerow([
                u.get("username", ""),
                u.get("xp", 0),
                u.get("weekly_xp", 0),
                u.get("referral_count", 0)
            ])
        output.seek(0)
        return send_file(output, mimetype="text/csv", download_name="users_export.csv", as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ========== TELEGRAM EVENT HANDLERS ==========

@app_bot.chat_join_request_handler()
async def handle_join_request(update, context):
    user = update.from_user
    referrer_id = pending_referrals.pop(user.id, None)
    if referrer_id and referrer_id != user.id:
        referrer = users_collection.find_one({"user_id": referrer_id})
        if referrer:
            already_referred = users_collection.find_one({
                "user_id": user.id,
                "referred_by": referrer_id
            })
            if not already_referred:
                users_collection.update_one(
                    {"user_id": referrer_id},
                    {"$inc": {"referral_count": 1}}
                )
                users_collection.update_one(
                    {"user_id": user.id},
                    {"$set": {"referred_by": referrer_id}},
                    upsert=True
                )
    await bot.send_message(
        chat_id=user.id,
        text="✅ Welcome! Follow our channel 👉 https://t.me/advantplayofficial for surprise voucher drops & updates!"
    )

# ========== SCHEDULED TASK ==========

def reset_weekly_xp():
    week_start = datetime.utcnow()
    snapshot = list(users_collection.find().sort("weekly_xp", -1).limit(50))
    for entry in snapshot:
        entry["_id"] = str(entry["_id"])
    history_collection.insert_one({
        "week_start": week_start,
        "leaderboard": snapshot
    })
    users_collection.update_many({}, {"$set": {"weekly_xp": 0}})

scheduler = BackgroundScheduler()
scheduler.add_job(reset_weekly_xp, "cron", day_of_week="mon", hour=0, minute=0)
scheduler.start()

# ========== STARTUP ==========

@app.route("/")
def home():
    return "Bot is running!"

def run_bot():
    app_bot.run_polling()

if __name__ == "__main__":
    import threading
    threading.Thread(target=run_bot).start()
    app.run(host="0.0.0.0", port=8080) 
