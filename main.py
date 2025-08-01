import os
import logging
from threading import Thread
from flask import Flask, request, jsonify, send_from_directory
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
)
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from checkin import handle_checkin
from referral import get_or_create_referral_link
from database import (
    get_user_data,
    get_leaderboard_data,
    update_user_xp,
    get_all_users,
    save_leaderboard_snapshot
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Flask app
app = Flask(__name__, static_folder='frontend')

@app.route("/")
def home():
    return "Bot is running!"

@app.route("/miniapp")
def serve_index():
    return send_from_directory("frontend", "index.html")

@app.route("/miniapp/<path:path>")
def serve_static(path):
    return send_from_directory("frontend", path)

@app.route("/api/checkin", methods=["POST"])
async def api_checkin():
    data = request.json
    user_id = data.get("user_id")
    username = data.get("username")
    return await handle_checkin(user_id, username)

@app.route("/api/referral_link", methods=["POST"])
async def api_generate_referral_link():
    data = request.json
    user_id = data.get("user_id")
    return await get_or_create_referral_link(user_id)

@app.route("/api/leaderboard", methods=["GET"])
def api_leaderboard():
    return jsonify(get_leaderboard_data())

@app.route("/api/user", methods=["POST"])
def api_user_data():
    data = request.json
    user_id = data.get("user_id")
    return jsonify(get_user_data(user_id))

@app.route("/api/admin/export", methods=["GET"])
def api_export():
    return jsonify(get_all_users())

@app.route("/api/admin/leaderboard_snapshot", methods=["POST"])
def api_snapshot():
    save_leaderboard_snapshot()
    return jsonify({"status": "ok"})

def run_flask():
    app.run(host="0.0.0.0", port=8080)

# Telegram bot
async def telegram_bot():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Welcome! Use the Mini App to check in and refer friends.")

    application.add_handler(CommandHandler("start", start))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    logger.info("Telegram bot started.")

# Start APScheduler for weekly leaderboard reset
scheduler = BackgroundScheduler()
scheduler.add_job(save_leaderboard_snapshot, 'cron', day_of_week='sun', hour=23, minute=59)
scheduler.start()

if __name__ == "__main__":
    Thread(target=run_flask).start()
    loop = asyncio.get_event_loop()
    loop.create_task(telegram_bot())
    loop.run_forever()
