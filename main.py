import os
import time
import logging
from flask import Flask
from threading import Thread
from pymongo import MongoClient
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ChatInviteLink
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === Config ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
GROUP_ID = -1002119266334  # Replace with your group ID (negative number)

# === Logging ===
logging.basicConfig(level=logging.INFO)

# === Flask App for UptimeRobot ===
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running."

# === MongoDB Setup ===
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_col = db["users"]
links_col = db["invite_links"]

# === Bot Handlers ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or f"user{user_id}"

    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"username": username}},
        upsert=True
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📣 Join Our Group", url="https://t.me/advantplayofficial")
        ]
    ])

    await update.message.reply_text(
        f"👋 Welcome, {username}!
Here’s your referral mission:

🎁 Share your referral link and earn rewards!",
        reply_markup=keyboard
    )


async def getlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or f"user{user_id}"

    record = links_col.find_one({"user_id": user_id})
    now = int(time.time())

    if record and "expires_at" in record and record["expires_at"] > now:
        invite_link = record["link"]
        expires_at = record["expires_at"]
    else:
        expires_at = now + 86400  # 24 hours
        link_obj: ChatInviteLink = await context.bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=f"ref_{user_id}",
            expire_date=expires_at,
            creates_join_request=False
        )
        invite_link = link_obj.invite_link

        links_col.update_one(
            {"user_id": user_id},
            {"$set": {
                "username": username,
                "link": invite_link,
                "expires_at": expires_at
            }},
            upsert=True
        )

    expires_at_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expires_at))
    await update.message.reply_text(
        f"🔗 Your 24h personal invite link:\n{invite_link}\n\n"
        f"This link will expire on:\n🕒 {expires_at_str}"
    )

# === Flask Thread ===
def run_flask():
    app.run(host="0.0.0.0", port=8080)

# === Main Bot App ===
async def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("getlink", getlink))
    await app_bot.initialize()
    await app_bot.start()
    await app_bot.updater.start_polling()
    await app_bot.updater.idle()

if __name__ == '__main__':
    Thread(target=run_flask).start()

    import asyncio
    asyncio.run(main())
