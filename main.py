import os
import logging
import asyncio
from flask import Flask, request, send_from_directory, jsonify
from threading import Thread
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
)
from checkin import handle_checkin
from referral import get_or_create_referral_link
from database import (
    get_user_data, get_leaderboard_data, update_user_xp,
    get_all_users, save_leaderboard_snapshot
)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ENV
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Flask App
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
async def api_generate_link():
    data = request.json
    user_id = data.get("user_id")
    return await get_or_create_referral_link(user_id)

@app.route("/api/leaderboard", methods=["GET"])
def api_leaderboard():
    leaderboard = get_leaderboard_data()
    return jsonify(leaderboard)

@app.route("/api/user", methods=["POST"])
def api_user():
    data = request.json
    user_id = data.get("user_id")
    user_data = get_user_data(user_id)
    return jsonify(user_data)

@app.route("/api/admin/xp", methods=["POST"])
def api_admin_xp():
    data = request.json
    user_id = data.get("user_id")
    xp = data.get("xp")
    update_user_xp(user_id, xp)
    return jsonify({"status": "XP updated"})

@app.route("/api/admin/export", methods=["GET"])
def api_admin_export():
    users = get_all_users()
    return jsonify(users)

@app.route("/api/admin/leaderboard_snapshot", methods=["POST"])
def api_leaderboard_snapshot():
    save_leaderboard_snapshot()
    return jsonify({"status": "Snapshot saved"})

# Flask server runs in a thread
def run_flask():
    app.run(host="0.0.0.0", port=8080)

# Telegram bot logic
async def telegram_bot():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Welcome! Use the Mini App to check in and refer friends.")

    application.add_handler(CommandHandler("start", start))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    logger.info("Telegram bot started.")

# Main entry point
if __name__ == "__main__":
    # Start Flask in separate thread
    Thread(target=run_flask).start()

    # Start Telegram bot in current event loop
    loop = asyncio.get_event_loop()
    loop.create_task(telegram_bot())
    loop.run_forever()
