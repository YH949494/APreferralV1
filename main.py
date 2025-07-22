import os
from datetime import datetime, timedelta
from pymongo import MongoClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatInviteLink
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from flask import Flask
from threading import Thread

# --- Config ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")

GROUP_ID = -1002723991859  # ← your Telegram group ID

# --- DB Setup ---
client = MongoClient(MONGO_URL)
db = client["referral_db"]
referrals = db["referrals"]

# --- Flask app for uptime monitoring ---
app = Flask(__name__)
@app.route("/")
def home():
    return "Bot is running!"
def run_flask():
    app.run(host="0.0.0.0", port=8080)

# --- Telegram Bot Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"👋 Welcome {user.first_name}!\n\n"
        "Send /getlink to receive your unique group invitation link."
    )

async def getlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    try:
        # Set expiration 24 hours from now
        expire_time = datetime.utcnow() + timedelta(hours=24)

        # Create invite link (one per user)
        invite: ChatInviteLink = await context.bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            expire_date=expire_time,
            member_limit=1,
            creates_join_request=False,
            name=f"Invite by {user.username or user.id}"
        )

        # Store tracking info in MongoDB
        referrals.insert_one({
            "user_id": user.id,
            "username": user.username,
            "invite_link": invite.invite_link,
            "created_at": datetime.utcnow(),
            "expires_at": expire_time,
            "group_id": GROUP_ID
        })

        keyboard = [
            [InlineKeyboardButton("✅ Join Group Now", url=invite.invite_link)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f"🎉 Here's your unique invite link (valid for 24 hours):\n{invite.invite_link}",
            reply_markup=reply_markup
        )

    except Exception as e:
        print(f"Error: {e}")
        await update.message.reply_text("❌ Failed to generate invite link. Is the bot an admin in the group?")

# --- Start everything ---
def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("getlink", getlink))

    # Start Flask in a separate thread
    Thread(target=run_flask).start()

    # Run the bot
    app_bot.run_polling()

if __name__ == "__main__":
    main()
