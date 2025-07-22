import os
from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatInviteLink, ChatMemberUpdated
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatMemberHandler, ContextTypes, filters
)
from pymongo import MongoClient

# === ENV VARS ===
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URL = os.environ["MONGO_URL"]
GROUP_ID = os.environ.get("GROUP_ID")  # Example: -1001234567890
PORT = int(os.environ.get("PORT", 8080))

# === FLASK FOR HEALTHCHECK ===
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

# === MONGODB ===
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users = db["users"]
links = db["invite_links"]

# === COMMAND HANDLERS ===
async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    telegram_id = user.id

    if not users.find_one({"user_id": telegram_id}):
        users.insert_one({
            "user_id": telegram_id,
            "username": user.username,
            "referrals": 0
        })

    # Check if already has a link
    link_data = links.find_one({"owner_id": telegram_id})
    if not link_data:
        invite: ChatInviteLink = await context.bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=f"ref_{telegram_id}",
            creates_join_request=False
        )
        links.insert_one({
            "owner_id": telegram_id,
            "link": invite.invite_link
        })
        invite_link = invite.invite_link
    else:
        invite_link = link_data["link"]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👉 Join Group", url=invite_link)]
    ])

    await update.message.reply_text(
        f"👋 Welcome! Here’s your referral link:

🔗 {invite_link}

Share this to invite others and earn rewards!",
        reply_markup=keyboard
    )

async def handle_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    record = users.find_one({"user_id": user.id})
    count = record.get("referrals", 0) if record else 0
    await update.message.reply_text(f"📊 You have referred {count} users.")

# === TRACK GROUP JOINS ===
async def handle_member_update(update: ChatMemberUpdated, context: ContextTypes.DEFAULT_TYPE):
    new_status = update.chat_member.new_chat_member.status
    user = update.chat_member.from_user

    if new_status == "member":
        invite_link_used = update.chat_member.invite_link
        if invite_link_used:
            ref_owner = links.find_one({"link": invite_link_used.invite_link})
            if ref_owner and ref_owner["owner_id"] != user.id:
                users.update_one(
                    {"user_id": ref_owner["owner_id"]},
                    {"$inc": {"referrals": 1}}
                )
                print(f"✅ {user.username or user.id} joined via {ref_owner['owner_id']}")

# === RUN ===
def run_bot():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", handle_start))
    app_bot.add_handler(CommandHandler("stats", handle_stats))
    app_bot.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))
    app_bot.run_polling()

if __name__ == "__main__":
    Thread(target=run_flask).start()
    run_bot()
