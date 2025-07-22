from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ChatJoinRequestHandler,
    filters,
)
from pymongo import MongoClient
import os
import datetime

# Load environment variables
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
GROUP_ID = -1002723991859  # Update with your group ID

# MongoDB setup
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# Flask server for uptime pinging
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

# /start or /ref command
async def send_invite_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user:
        return

    # Save user if not exist
    users_collection.update_one(
        {"user_id": user.id},
        {
            "$setOnInsert": {
                "username": user.username or "",
                "referral_count": 0
            }
        },
        upsert=True
    )

    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            member_limit=0,
            creates_join_request=True,  # Allow tracking via join request
            expire_date=datetime.datetime.utcnow() + datetime.timedelta(hours=24),
            name=f"ref-{user.id}"
        )
    except Exception as e:
        await message.reply_text("❌ Failed to generate invite link. Make sure the bot is admin.")
        return

    keyboard = [
        [InlineKeyboardButton("👉 Join Group", url=invite_link.invite_link)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await message.reply_text(
        f"👋 Welcome! Here is your personal invite link (valid 24h):\n\n{invite_link.invite_link}\n\n"
        "Share this with friends. If they join, you’ll earn bonus XP!",
        reply_markup=reply_markup
    )

# Handle join request to approve + count ref
async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    join_request = update.chat_join_request
    invite_link = join_request.invite_link

    if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
        referrer_id = int(invite_link.name.split("-")[1])
        users_collection.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referral_count": 1}}
        )

    await context.bot.approve_chat_join_request(
        chat_id=join_request.chat.id,
        user_id=join_request.from_user.id
    )

# Start server & bot
if __name__ == '__main__':
    # Start Flask app in background
    Thread(target=lambda: app.run(host="0.0.0.0", port=8080)).start()

    # Start Telegram bot
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register commands
    for cmd in ["start", "ref", "referral", "invite", "getlink", "link"]:
        app_bot.add_handler(CommandHandler(cmd, send_invite_link, filters.ChatType.PRIVATE | filters.ChatType.GROUPS))

    # Register join request handler
    app_bot.add_handler(ChatJoinRequestHandler(handle_join_request))

    print("Bot is running...")
    app_bot.run_polling()
