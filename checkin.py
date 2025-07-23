from telegram import Update
from telegram.ext import ContextTypes
from datetime import datetime
from database import users_collection

CHECKIN_EXP = 20

# For Telegram command usage
async def checkin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if user is None:
        return

    user_id = user.id
    today = datetime.utcnow().date()

    user_data = users_collection.find_one({"user_id": user_id})

    if user_data:
        last_checkin = user_data.get("last_checkin")

        if last_checkin and datetime.strptime(last_checkin, "%Y-%m-%d").date() == today:
            await update.message.reply_text("✅ You’ve already checked in today!")
            return

        users_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {"last_checkin": today.strftime("%Y-%m-%d")},
                "$inc": {
                    "xp": CHECKIN_EXP,
                    "weekly_xp": CHECKIN_EXP
                }
            }
        )
    else:
        users_collection.insert_one({
            "user_id": user_id,
            "username": user.username,
            "xp": CHECKIN_EXP,
            "weekly_xp": CHECKIN_EXP,
            "last_checkin": today.strftime("%Y-%m-%d"),
            "referral_count": 0
        })

    await update.message.reply_text(f"🎉 Check-in successful! You earned {CHECKIN_EXP} XP.")

# For Mini App API usage (non-async)
def update_checkin_xp(user_id: int) -> str:
    today = datetime.utcnow().date()
    user_data = users_collection.find_one({"user_id": user_id})

    if user_data:
        last_checkin = user_data.get("last_checkin")
        if last_checkin and datetime.strptime(last_checkin, "%Y-%m-%d").date() == today:
            return "✅ You’ve already checked in today!"

        users_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {"last_checkin": today.strftime("%Y-%m-%d")},
                "$inc": {
                    "xp": CHECKIN_EXP,
                    "weekly_xp": CHECKIN_EXP
                }
            }
        )
    else:
        users_collection.insert_one({
            "user_id": user_id,
            "username": None,  # Mini App may not have username
            "xp": CHECKIN_EXP,
            "weekly_xp": CHECKIN_EXP,
            "last_checkin": today.strftime("%Y-%m-%d"),
            "referral_count": 0
        })

    return f"✅ Check-in successful! You earned {CHECKIN_EXP} XP."
