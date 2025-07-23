from flask import Flask, request, jsonify, send_from_directory
from threading import Thread
from referral import get_or_create_referral_link
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, ChatJoinRequestHandler
from pymongo import MongoClient
import os
import datetime
import asyncio

# ----------------------------
# Environment Variables
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
GROUP_ID = -1002723991859  # Replace with your actual group ID
WEBAPP_URL = "https://apreferralv1.fly.dev/miniapp"  # ✅ Mini App URL

# ----------------------------
# MongoDB setup
# ----------------------------
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# ----------------------------
# Flask App Setup
# ----------------------------
app = Flask(__name__, static_folder="static")

@app.route("/")
def home():
    return "Bot is alive!"

@app.route("/miniapp")
def serve_mini_app():
    return send_from_directory("static", "index.html")

@app.route("/api/checkin")
def api_checkin():
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    from checkin import update_checkin_xp
    message = update_checkin_xp(user_id, request.args.get("username"))
    return jsonify({"message": message})

@app.route("/api/referral")
def api_referral():
    from referral import get_or_create_referral_link

    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username")

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        referral_link = loop.run_until_complete(get_or_create_referral_link(app_bot.bot, user_id, username, "webapp"))

        if referral_link:
            return jsonify({"referral_link": referral_link})
        else:
            return jsonify({"error": "Failed to create referral link"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ----------------------------
# Telegram Bot Logic
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None:
        return

    # Ensure user exists in database
    user_data = users_collection.find_one({"user_id": user.id})
    if not user_data:
        users_collection.insert_one({
            "user_id": user.id,
            "username": user.username,
            "xp": 0,
            "referral_count": 0,
            "last_checkin": None
        })

    # Send Mini App button (Web App)
    keyboard = [[
        InlineKeyboardButton(
            text="🚀 Open Check-in & Referral",
            web_app=WebAppInfo(url="https://apreferralv1.fly.dev/miniapp")
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "👋 Welcome! Tap the button below to check-in and view your referral link 👇",
        reply_markup=reply_markup
    )

async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    invite_link = update.chat_join_request.invite_link

    if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
        referrer_id = int(invite_link.name.split("-")[1])

        # Increment referral count
        users_collection.update_one(
    {"user_id": user.id},
    {
        "$set": {
            "username": user.username,
        },
        "$setOnInsert": {
            "xp": 0,
            "referral_count": 0,
            "last_checkin": None
        }
    },
    upsert=True
)

        # Approve the join request
        await context.bot.approve_chat_join_request(
            update.chat_join_request.chat.id,
            update.chat_join_request.from_user.id
        )

# ----------------------------
# Run Telegram Bot & Flask
# ----------------------------
if __name__ == '__main__':
    # Run Flask in a thread
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080)).start()

    # Build and run Telegram Bot
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("referral", start))
    app_bot.add_handler(CommandHandler("invite", start))
    app_bot.add_handler(CommandHandler("getlink", start))
    app_bot.add_handler(CommandHandler("link", start))
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))

    app_bot.run_polling()
