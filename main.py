import os
import logging
from pymongo import MongoClient
from telegram import Update, ChatMember
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes, ChatMemberHandler
)

# Enable logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB Setup
MONGO_URL = os.environ.get("MONGO_URL")
mongo_client = MongoClient(MONGO_URL)
db = mongo_client["referral_bot"]
users = db["users"]

# Your bot token
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Get personal referral link
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args
    user_id = user.id

    # Check if already in DB
    existing_user = users.find_one({"user_id": user_id})
    if not existing_user:
        referrer = int(args[0]) if args else None
        users.insert_one({
            "user_id": user_id,
            "username": user.username,
            "referrer": referrer,
            "joined": True,
            "referrals": 0
        })

        if referrer:
            users.update_one({"user_id": referrer}, {"$inc": {"referrals": 1}})

    link = f"https://t.me/{context.bot.username}?start={user_id}"
    await update.message.reply_text(
        f"Welcome {user.first_name}!\n"
        f"Here’s your referral link:\n{link}"
    )

# Track join and leave
async def track_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result: ChatMember = update.chat_member
    status = result.new_chat_member.status
    user_id = result.from_user.id

    if status == "left":
        users.update_one({"user_id": user_id}, {"$set": {"joined": False}})
    elif status in ["member", "administrator"]:
        users.update_one({"user_id": user_id}, {"$set": {"joined": True}})

# Basic handler to avoid bot deletion
async def handle_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass  # can be extended later

# Run bot
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(ChatMemberHandler(track_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ALL, handle_messages))

    app.run_polling()

if __name__ == "__main__":
    main() 
