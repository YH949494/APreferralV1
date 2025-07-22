from telegram import Update, ChatInviteLink
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, ChatMemberHandler
)
import os
from pymongo import MongoClient
from datetime import datetime, timedelta

# --- Configuration ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")  # e.g. mongodb+srv://...
GROUP_ID = -1002723991859  # your target Telegram group ID

# --- MongoDB Setup ---
mongo = MongoClient(MONGO_URI)
db = mongo["telegram_referrals"]
referrals = db["referral_links"]

# --- Command to get your unique invite link ---
async def get_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # Check if user already has a valid link
    existing = referrals.find_one({"user_id": user.id})
    if existing:
        await update.message.reply_text(
            f"🔗 Your invite link:\n{existing['invite_link']}\n\n"
            f"🧲 Referral count: {existing.get('referral_count', 0)}"
        )
        return

    try:
        # Create new invite link (24 hours)
        invite: ChatInviteLink = await context.bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=f"Invite-{user.id}",
            expire_date=datetime.utcnow() + timedelta(hours=24),
            creates_join_request=False,
            member_limit=0
        )

        # Save to DB
        referrals.insert_one({
            "user_id": user.id,
            "username": user.username,
            "invite_link": invite.invite_link,
            "referral_count": 0,
            "referred_users": [],
            "created_at": datetime.utcnow()
        })

        await update.message.reply_text(
            f"✅ Here is your unique invite link (valid for 24h):\n{invite.invite_link}"
        )

    except Exception as e:
        print(f"Error generating invite: {e}")
        await update.message.reply_text("❌ Failed to generate invite link. Please try again later.")

# --- Track who joined using which invite ---
async def track_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member = update.chat_member
    user = chat_member.new_chat_member.user

    # Only process joins
    if chat_member.old_chat_member.status in ("left", "kicked") and chat_member.new_chat_member.status == "member":
        invite = chat_member.invite_link
        if not invite:
            return

        # Match invite in MongoDB
        record = referrals.find_one({"invite_link": invite.invite_link})
        if record:
            referrals.update_one(
                {"_id": record["_id"]},
                {
                    "$inc": {"referral_count": 1},
                    "$push": {"referred_users": {
                        "user_id": user.id,
                        "username": user.username
                    }}
                }
            )
            print(f"✅ {user.username} joined via {record['username']}'s link")

# --- Main App ---
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", get_link))
    app.add_handler(ChatMemberHandler(track_join, chat_member_types=["member"]))

    print("Bot is running...")
    app.run_polling()
