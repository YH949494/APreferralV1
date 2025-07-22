import os
from flask import Flask
from threading import Thread
from telegram import Update, ChatMemberUpdated
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatMemberHandler,
    ContextTypes, filters
)
from pymongo import MongoClient
from datetime import datetime, timedelta

# Environment Variables
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URL = os.environ["MONGO_URL"]
PORT = int(os.environ.get("PORT", 8080))

# Flask setup for Fly.io health check
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

# MongoDB setup
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users = db["users"]

# --- Telegram Handlers ---

# /start handler (referral logic)
async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args
    referred_by = int(args[0]) if args else None

    if not users.find_one({"user_id": user.id}):
        users.insert_one({
            "user_id": user.id,
            "username": user.username,
            "referrals": 0,
            "referred_by": referred_by,
            "joined_at": datetime.utcnow()
        })

        if referred_by and referred_by != user.id:
            users.update_one(
                {"user_id": referred_by},
                {"$inc": {"referrals": 1}}
            )

    link = f"https://t.me/{context.bot.username}?start={user.id}"
    await update.message.reply_text(f"Your referral link:\n{link}")

# /stats - total referrals
async def handle_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    record = users.find_one({"user_id": user.id})
    count = record.get("referrals", 0) if record else 0
    await update.message.reply_text(f"You have referred {count} users.")

# /weekly - referrals in past 7 days
async def handle_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    now = datetime.utcnow()
    one_week_ago = now - timedelta(days=7)

    count = users.count_documents({
        "referred_by": user.id,
        "joined_at": {"$gte": one_week_ago}
    })

    await update.message.reply_text(f"You referred {count} users in the past 7 days.")

# /monthly - referrals in past 30 days
async def handle_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    now = datetime.utcnow()
    one_month_ago = now - timedelta(days=30)

    count = users.count_documents({
        "referred_by": user.id,
        "joined_at": {"$gte": one_month_ago}
    })

    await update.message.reply_text(f"You referred {count} users in the past 30 days.")

# Group join/leave logging
async def handle_member_update(update: ChatMemberUpdated, context: ContextTypes.DEFAULT_TYPE):
    old_status = update.chat_member.old_chat_member.status
    new_status = update.chat_member.new_chat_member.status
    user = update.chat_member.from_user

    if old_status in ("left", "kicked") and new_status == "member":
        print(f"{user.username or user.id} joined.")
    elif old_status == "member" and new_status in ("left", "kicked"):
        print(f"{user.username or user.id} left.")

# Start everything
def run_bot():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    app_bot.add_handler(CommandHandler("start", handle_start))
    app_bot.add_handler(CommandHandler("stats", handle_stats))
    app_bot.add_handler(CommandHandler("weekly", handle_weekly))
    app_bot.add_handler(CommandHandler("monthly", handle_monthly))
    app_bot.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))

    app_bot.run_polling()

if __name__ == "__main__":
    Thread(target=run_flask).start()
    run_bot()
