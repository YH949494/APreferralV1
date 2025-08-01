import os
import logging
import asyncio
from flask import Flask, send_from_directory, request, jsonify
from threading import Thread
from pymongo import MongoClient
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from checkin import handle_checkin
from referral import get_or_create_referral_link, handle_new_join
from database import (
    users_collection,
    ensure_user,
    add_xp,
    get_leaderboard,
    get_streak_badges,
    reset_weekly_xp,
)

from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
GROUP_ID = int(os.getenv("GROUP_ID"))  # Make sure this is set in your Fly.io secrets

app = Flask(__name__)

# === Flask Mini App Routes ===
@app.route("/miniapp")
def miniapp():
    return send_from_directory("static", "index.html")

@app.route("/miniapp/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

@app.route("/api/checkin", methods=["GET"])
def api_checkin():
    return handle_checkin()

@app.route("/api/leaderboard")
def api_leaderboard():
    leaderboard = get_leaderboard()
    return jsonify(leaderboard)

@app.route("/api/badges", methods=["GET"])
def api_badges():
    user_id = request.args.get("user_id", type=int)
    badges = get_streak_badges(user_id)
    return jsonify(badges)

@app.route("/api/admin/check", methods=["POST"])
def check_admin():
    data = request.get_json()
    user_id = data.get("user_id")
    chat_id = data.get("chat_id")

    if not user_id or not chat_id:
        return jsonify({"error": "Missing user_id or chat_id"}), 400

    async def is_admin():
        application = app.bot_app
        chat_administrators = await application.bot.get_chat_administrators(chat_id)
        return any(admin.user.id == user_id for admin in chat_administrators)

    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(is_admin())
    loop.close()
    return jsonify({"is_admin": result})

@app.route("/api/admin/xp", methods=["POST"])
def api_admin_xp():
    data = request.get_json()
    user_id = data.get("user_id")
    xp = data.get("xp")

    if user_id is None or xp is None:
        return jsonify({"error": "Missing user_id or xp"}), 400

    user_id = int(user_id)
    xp = int(xp)

    add_xp(user_id, xp)
    return jsonify({"success": True})

# === Telegram Bot Handlers ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if chat.type == "private":
        ensure_user(user.id, user.username or "")
        link = get_or_create_referral_link()
        welcome_msg = (
            "👋 Welcome! Here’s your personal referral link:\n\n"
            f"{link}\n\n"
            "✅ Invite friends to earn rewards!\n"
            "🔔 Join our channel for the latest drops:\n"
            "👉 https://t.me/advantplayofficial"
        )
        await update.message.reply_text(welcome_msg)

async def referral_join_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await handle_new_join(update, context, GROUP_ID)

# === Weekly Reset with APScheduler ===
scheduler = BackgroundScheduler()
scheduler.add_job(reset_weekly_xp, "cron", day_of_week="mon", hour=0, minute=0)
scheduler.start()

# === Launch Telegram Bot ===
def run_telegram_bot():
    app.bot_app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.bot_app.add_handler(CommandHandler("start", start))
    app.bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, referral_join_handler))

    # Run the bot inside existing event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(app.bot_app.initialize())
    loop.run_until_complete(app.bot_app.start())
    loop.run_forever()

# === Run Flask + Telegram Bot ===
if __name__ == "__main__":
    Thread(target=run_telegram_bot).start()
    app.run(host="0.0.0.0", port=8080)
