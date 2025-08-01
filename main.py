import os
import logging
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import asyncio

from checkin import handle_checkin
from referral import get_or_create_referral_link
from database import (
    get_user_data,
    get_leaderboard,
    get_weekly_history,
    update_user_xp,
    remove_xp,
    reset_weekly_xp,
    log_join_request,
)

from telegram_login import WebAppInitData  # Ensure this file exists

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask
app = Flask(__name__)
CORS(app)

# Telegram bot setup
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GROUP_ID = os.environ.get("GROUP_ID")
bot = Bot(BOT_TOKEN)
application = ApplicationBuilder().token(BOT_TOKEN).build()

# Serve Telegram Mini App
@app.route('/miniapp')
def miniapp():
    return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def static_proxy(path):
    return send_from_directory('.', path)

# Check-in API
@app.route('/api/checkin', methods=['POST'])
def api_checkin():
    data = request.get_json()
    user_id = data.get("user_id")
    username = data.get("username")
    return jsonify(handle_checkin(user_id, username))

# Referral API
@app.route('/api/referral', methods=['POST'])
def api_referral():
    data = request.get_json()
    user_id = data.get("user_id")
    username = data.get("username")
    return jsonify(get_or_create_referral_link(user_id, username, bot))

# Get user data (XP, streak, referrals)
@app.route('/api/user', methods=['GET'])
def api_user():
    user_id = request.args.get("user_id")
    return jsonify(get_user_data(user_id))

# Get leaderboard
@app.route('/api/leaderboard', methods=['GET'])
def api_leaderboard():
    return jsonify(get_leaderboard())

# Get weekly leaderboard history
@app.route('/api/leaderboard/history', methods=['GET'])
def api_leaderboard_history():
    return jsonify(get_weekly_history())

# Admin check via initData
@app.route('/api/admin/check', methods=['POST'])
def api_admin_check():
    init_data = request.json.get("initData", "")
    try:
        parsed = WebAppInitData.parse(init_data, BOT_TOKEN)
        user_id = parsed.user.id
        chat = bot.get_chat(GROUP_ID)
        is_admin = any(admin.user.id == user_id for admin in chat.get_administrators())
        return jsonify({"ok": True, "is_admin": is_admin})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

# Admin: Add XP
@app.route('/api/admin/add_xp', methods=['POST'])
def api_admin_add_xp():
    data = request.get_json()
    user_id = data.get("user_id")
    amount = data.get("amount")
    success = update_user_xp(user_id, amount)
    return jsonify({"ok": success})

# Admin: Remove XP
@app.route('/api/admin/remove_xp', methods=['POST'])
def api_admin_remove_xp():
    data = request.get_json()
    user_id = data.get("user_id")
    amount = data.get("amount")
    success = remove_xp(user_id, amount)
    return jsonify({"ok": success})

# Telegram bot command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Welcome! Use the Mini App to check-in and refer friends.")

# Handle join request for referral tracking
async def join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user
    group_id = update.chat_join_request.chat.id
    await log_join_request(user, group_id)

# Register handlers
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("checkin", start))
application.add_handler(CommandHandler("referral", start))
application.add_handler(CommandHandler("leaderboard", start))
application.add_handler(CommandHandler("history", start))
application.add_handler(CommandHandler("admin", start))

# NOTE: join request should use .add_chat_join_request_handler()
application.add_chat_join_request_handler(join_request)

# Schedule weekly XP reset
scheduler = BackgroundScheduler()
scheduler.add_job(reset_weekly_xp, 'cron', day_of_week='sun', hour=23, minute=59)
scheduler.start()

# Run Flask + Telegram bot
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.start())
    app.run(host='0.0.0.0', port=8080)
