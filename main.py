from flask import Flask, request, jsonify, send_from_directory
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ChatJoinRequestHandler, ContextTypes
from checkin import checkin_handler, handle_checkin
from referral import get_or_create_referral_link
from pymongo import MongoClient
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

# ----------------------------
# Flask App
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
    return handle_checkin()

@app.route("/api/referral")
def api_referral():
    try:
        user_id = int(request.args.get("user_id"))
        username = request.args.get("username")
        referral_link = asyncio.run(get_or_create_referral_link(app_bot.bot, user_id, username))
        return jsonify({"success": True, "referral_link": referral_link})
    except Exception as e:
        print("[API Referral Error]")
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ----------------------------
# Telegram Bot Handlers
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None:
        return

    # Ensure user exists
    if not users_collection.find_one({"user_id": user.id}):
        users_collection.insert_one({
            "user_id": user.id,
            "username": user.username,
            "xp": 0,
            "weekly_xp": 0,
            "referral_count": 0,
            "last_checkin": None
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

    if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
        referrer_id = int(invite_link.name.split("-")[1])

        users_collection.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referral_count": 1}}
        )

    users_collection.update_one(
        {"user_id": user.id},
        {
            "$set": {"username": user.username},
            "$setOnInsert": {"xp": 0, "referral_count": 0, "weekly_xp": 0, "last_checkin": None}
        },
        upsert=True
    )

    await context.bot.approve_chat_join_request(update.chat_join_request.chat.id, user.id)

# ----------------------------
# Run Bot + Flask
# ----------------------------
if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080)).start()

    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))
    app_bot.run_polling()
