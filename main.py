import os
from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatMemberHandler, ContextTypes
)
from pymongo import MongoClient

# Load environment variables
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URL = os.environ["MONGO_URL"]
PORT = int(os.environ.get("PORT", 8080))

# Flask health check
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

# Telegram handlers
async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args
    referred_by = int(args[0]) if args else None

    if not users.find_one({"user_id": user.id}):
        users.insert_one({
            "user_id": user.id,
            "username": user.username,
            "referrals": 0,
            "referred_by": referred_by
        })
        if referred_by and referred_by != user.id:
            users.update_one(
                {"user_id": referred_by},
                {"$inc": {"referrals": 1}}
            )

    ref_link = f"https://t.me/{context.bot.username}?start={user.id}"
    group_link = "https://t.me/advantplayofficial"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Share Your Link", url=ref_link)],
        [InlineKeyboardButton("📢 Join Our Group", url=group_link)]
    ])

    await update.message.reply_text(
        f"👋 Welcome, {user.first_name}!\n\n"
        f"Here’s your referral link:\n{ref_link}\n\n"
        f"📣 Share this with friends to earn bonus XP!\n\n"
        f"👉 After inviting, don’t forget to join our group:\n{group_link}",
        reply_markup=keyboard
    )

async def handle_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    record = users.find_one({"user_id": user.id})
    count = record.get("referrals", 0) if record else 0
    await update.message.reply_text(f"📊 You have referred {count} users.")

async def handle_member_update(update: ChatMemberUpdated, context: ContextTypes.DEFAULT_TYPE):
    old_status = update.chat_member.old_chat_member.status
    new_status = update.chat_member.new_chat_member.status
    user = update.chat_member.from_user

    if old_status in ("left", "kicked") and new_status == "member":
        print(f"{user.username or user.id} joined.")
    elif old_status == "member" and new_status in ("left", "kicked"):
        print(f"{user.username or user.id} left.")

def run_bot():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", handle_start))
    app_bot.add_handler(CommandHandler("stats", handle_stats))
    app_bot.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))
    app_bot.run_polling()

if __name__ == "__main__":
    Thread(target=run_flask).start()
    run_bot()
