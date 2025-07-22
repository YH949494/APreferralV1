from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes
)
import os
from pymongo import MongoClient

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
GROUP_LINK = "https://t.me/advantplayofficial"

client = MongoClient(MONGO_URL)
db = client["referral_db"]
collection = db["referrals"]

app = Flask(__name__)

# Flask health check route
@app.route("/")
def home():
    return "Bot is alive!"

# Run Flask in a separate thread
def run_flask():
    app.run(host="0.0.0.0", port=8080)

def start_flask():
    thread = Thread(target=run_flask)
    thread.start()

# /start handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or f"user{user_id}"
    
    args = context.args
    referrer_id = args[0] if args else None

    # Check if user already in DB
    existing_user = collection.find_one({"user_id": user_id})
    if not existing_user:
        data = {
            "user_id": user_id,
            "username": username,
            "referrer_id": referrer_id,
            "joined_at": update.message.date,
        }
        collection.insert_one(data)

    # Generate referral link
    referral_link = f"https://t.me/{context.bot.username}?start={user_id}"

    # Message
    message = (
        f"👋 Welcome, {username}!\n\n"
        f"🎁 Here's your referral link:\n{referral_link}\n\n"
        f"🔗 Share this link with friends and invite them!"
    )

    # Inline button to join group
    keyboard = [
        [InlineKeyboardButton("🚪 Join the Group", url=GROUP_LINK)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=message,
        reply_markup=reply_markup
    )

# Main run
if __name__ == "__main__":
    start_flask()

    app_telegram = ApplicationBuilder().token(BOT_TOKEN).build()
    app_telegram.add_handler(CommandHandler("start", start))
    app_telegram.run_polling()
