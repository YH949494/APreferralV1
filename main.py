from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import os
from pymongo import MongoClient
import datetime

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
GROUP_ID = os.environ.get("GROUP_ID")  # example: -1001234567890

app = Flask(__name__)

# MongoDB setup
mongo_client = MongoClient(MONGO_URL)
db = mongo_client["referral_bot"]
invite_collection = db["invites"]

# Health check
@app.route("/")
def home():
    return "Bot is alive!"

# /start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Welcome! Use /getlink to create your referral invite.")

# /getlink command
async def getlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot = context.bot

    try:
        # Create a unique link valid for 1 day (86400 seconds)
        invite_link = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            member_limit=0,
            expire_date=datetime.datetime.now() + datetime.timedelta(hours=24),
            name=f"referral_{user.id}"
        )

        # Save to DB
        invite_collection.insert_one({
            "user_id": user.id,
            "username": user.username,
            "invite_link": invite_link.invite_link,
            "created_at": datetime.datetime.utcnow(),
            "expires_at": invite_link.expire_date
        })

        # Send link with button
        button = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Join Group", url=invite_link.invite_link)]
        ])

        await update.message.reply_text(
            f"Here's your personal invite link:\n{invite_link.invite_link}\n\n"
            f"This link is valid for 24 hours.",
            reply_markup=button
        )

    except Exception as e:
        print(f"Error creating invite link: {e}")
        await update.message.reply_text("❌ Failed to generate invite link. Is the bot an admin in the group?")

# Background thread for Flask
def run():
    app.run(host="0.0.0.0", port=8080)

# Start everything
def main():
    app_thread = Thread(target=run)
    app_thread.start()

    telegram_app = ApplicationBuilder().token(BOT_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("getlink", getlink))

    telegram_app.run_polling()

if __name__ == "__main__":
    main()
