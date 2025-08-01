import os
import asyncio
from flask import Flask, render_template, request, jsonify
from threading import Thread
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from referral import generate_referral_link
from database import (
    ensure_user,
    can_checkin,
    checkin_user,
    get_user_stats,
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
    ensure_user(user_id, username)
    if not can_checkin(user_id):
        return jsonify({"success": False, "message": "⏳ Come back after 24 hours to check in again."})
    checkin_user(user_id)
    return jsonify({"success": True, "message": "✅ Check-in successful! +20 XP"})

@app.route("/api/user_stats")
def api_user_stats():
    user_id = int(request.args.get("user_id"))
    stats = get_user_stats(user_id)
    return jsonify(stats)

@app.route("/api/referral")
def api_referral():
    user_id = int(request.args.get("user_id"))
    username = request.args.get("username", "")
    return jsonify({
        "link": generate_referral_link(user_id, username)
    })

# === Telegram Bot Setup ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"  # <-- your Mini App URL

application = ApplicationBuilder().token(BOT_TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username)
    keyboard = [
        [InlineKeyboardButton("🌟 Open Leaderboard & Check-in", web_app={"url": WEBAPP_URL})]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Welcome! Tap below to open the Mini App 👇",
        reply_markup=reply_markup
    )

application.add_handler(CommandHandler("start", start))

# === Run Flask in separate thread ===
def run_flask():
    app.run(host="0.0.0.0", port=8080)

# === Run Telegram Bot ===
def run_telegram():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.start())
    loop.run_until_complete(application.bot.set_my_commands([
        BotCommand("start", "Open Mini App"),
    ]))
    loop.run_forever()

if __name__ == "__main__":
    Thread(target=run_flask).start()
    run_telegram()
