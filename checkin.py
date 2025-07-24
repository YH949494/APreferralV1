from datetime import datetime, timedelta
from flask import request, jsonify
from telegram import Update
from telegram.ext import ContextTypes
from database import users_collection

CHECKIN_EXP = 20

# For Telegram command usage
async def checkin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if user is None:
        return

    user_id = user.id
    username = user.username
    today = datetime.utcnow().date()

    user_data = users_collection.find_one({"user_id": user_id})

    if user_data:
        last_checkin_str = user_data.get("last_checkin")

        if last_checkin_str:
            last_checkin_date = datetime.strptime(last_checkin_str, "%Y-%m-%d").date()
            if last_checkin_date == today:
                await update.message.reply_text("✅ You’ve already checked in today!")
                return

        users_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "last_checkin": now.isoformat() + "Z",
                    "username": username
                },
                "$inc": {
                    "xp": CHECKIN_EXP,
                    "weekly_xp": CHECKIN_EXP
                }
            }
        )
    else:
        users_collection.insert_one({
            "user_id": user_id,
            "username": username,
            "xp": CHECKIN_EXP,
            "weekly_xp": CHECKIN_EXP,
            "last_checkin": today.strftime("%Y-%m-%d"),
            "referral_count": 0
        })

    await update.message.reply_text(f"🎉 Check-in successful! You earned {CHECKIN_EXP} XP.")

# For Mini App API usage
def handle_checkin():
    try:
        user_id = int(request.args.get("user_id"))
        username = request.args.get("username")
        now = datetime.utcnow()
        today = now.date()

        user = users_collection.find_one({"user_id": user_id})

        if not user:
            next_checkin_time = now + timedelta(days=1)

            users_collection.insert_one({
                "user_id": user_id,
                "username": username,
                "xp": CHECKIN_EXP,
                "weekly_xp": CHECKIN_EXP,
                "last_checkin": today.strftime("%Y-%m-%d"),
                "referral_count": 0
            })

            return jsonify({
                "success": True,
                "message": f"🎉 Check-in successful! +{CHECKIN_EXP} XP",
                "can_checkin": False,
                "next_checkin_time": next_checkin_time.isoformat() + "Z"
            })

        last_checkin_str = user.get("last_checkin")
        if last_checkin_str:
            last_checkin_date = datetime.strptime(last_checkin_str, "%Y-%m-%d").date()
            if last_checkin_date == today:
                # Already checked in today
                next_checkin_time = datetime.combine(today + timedelta(days=1), datetime.min.time())

                return jsonify({
                    "success": False,
                    "message": "✅ You’ve already checked in today!",
                    "can_checkin": False,
                    "next_checkin_time": next_checkin_time.isoformat() + "Z"
                })

        # Successful check-in
        next_checkin_time = now + timedelta(days=1)

        users_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "last_checkin": today.strftime("%Y-%m-%d"),
                    "username": username
                },
                "$inc": {
                    "xp": CHECKIN_EXP,
                    "weekly_xp": CHECKIN_EXP
                }
            }
        )

        return jsonify({
            "success": True,
            "message": f"🎉 Check-in successful! +{CHECKIN_EXP} XP",
            "can_checkin": False,
            "next_checkin_time": next_checkin_time.isoformat() + "Z"
        })

    except Exception as e:
        print(f"[Check-in Error] {e}")
        return jsonify({"success": False, "message": "❌ Server error"})
