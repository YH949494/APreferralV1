import os
import asyncio
from flask import Flask, render_template, request, jsonify
from threading import Thread
from telegram import Update, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from referral import get_or_create_referral_link
from database import (
    ensure_user,
    can_checkin,
    checkin_user,
    get_user_stats
)

# === Flask App Setup ===
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running."

@app.route("/miniapp")
def serve_miniapp():
    return render_template("index.html")

@app.route("/api/checkin", methods=["POST"])
def api_checkin():
    user_id = int(request.json.get("user_id"))
    username = request.json.get("username", "")
    if not can_checkin(user_id):
        return jsonify({"success": False, "message": "⏳ Come back after 24 hours to check in again."})
    checkin_user(user_id)
    return jsonify({"success": True, "message": "✅ Check-in successful! +20 XP"})

@app.route("/api/user", methods=["GET"])
def api_user_stats():
    user_id = int(request.args.get("user_id"))
    stats = get_user_stats(user_id)
    return jsonify(stats)

@app.route("/api/referral", methods=["GET"])
def api_referral():
    user_id = int(request.args.get("user_id"))
    username = request.args.get("username", "")
    link = get_or_create_referral_link(user_id, username)
    return jsonify({"referral_link": link})

# === Telegram Bot Setup ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
application = ApplicationBuilder().token(BOT_TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username)
    await update.message.reply_text("✅ You’re all set! Use the Mini App to check-in and get your referral link.")

application.add_handler(CommandHandler("start", start))

# === Run Flask in separate thread ===
def run_flask():
    app.run(host="0.0.0.0", port=8080)

# === Run Telegram Bot ===
def run_telegram():
    async def runner():
        await application.initialize()
        await application.start()
        await application.bot.set_my_commands([
            BotCommand("start", "Start the bot and activate your account")
        ])
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(runner())

if __name__ == "__main__":
    Thread(target=run_flask).start()
    run_telegram()
