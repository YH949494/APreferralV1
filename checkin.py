from flask import request, jsonify
from pymongo import MongoClient
import os
import pytz
from datetime import datetime, timedelta, time

# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# Timezone and XP settings
tz = pytz.timezone("Asia/Kuala_Lumpur")
CHECKIN_XP = 20

def handle_checkin():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    now = datetime.now(tz)
    today = now.date()
    next_midnight = tz.localize(datetime.combine(today + timedelta(days=1), time()))

    user = users_collection.find_one({"user_id": user_id})

    if not user:
        # First-time user
        users_collection.insert_one({
            "user_id": user_id,
            "username": username,
            "xp": CHECKIN_XP,
            "weekly_xp": CHECKIN_XP,
            "referral_count": 0,
            "weekly_referral_count": 0,
            "last_checkin": now,
            "streak": 1
        })
        return jsonify({
            "success": True,
            "message": f"✅ First check-in! +{CHECKIN_XP} XP",
            "next_checkin_time": next_midnight.isoformat()
        })

    last_checkin = user.get("last_checkin")
    streak = user.get("streak", 0)

    if last_checkin:
        last_date = last_checkin.astimezone(tz).date()

        if last_date == today:
            # Already checked in today
            return jsonify({
                "success": False,
                "message": "⏳ You’ve already checked in today!",
                "next_checkin_time": next_midnight.isoformat()
            })

        if (today - last_date) == timedelta(days=1):
            streak += 1  # Continue streak
        else:
            streak = 1  # Missed a day → reset streak
    else:
        streak = 1  # First check-in ever

    # Bonus XP for streaks
    bonus_xp = 0
    if streak == 7:
        bonus_xp = 50
    elif streak == 14:
        bonus_xp = 100
    elif streak == 28:
        bonus_xp = 200

    total_xp = CHECKIN_XP + bonus_xp

    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "last_checkin": now,
                "streak": streak
            },
            "$inc": {
                "xp": total_xp,
                "weekly_xp": total_xp,
                "monthly_xp": total_xp
            }
        }
    )

    bonus_text = f" 🎉 Streak Bonus: +{bonus_xp} XP!" if bonus_xp else ""
    return jsonify({
        "success": True,
        "message": f"✅ Check-in successful! +{CHECKIN_XP} XP{bonus_text}",
        "next_checkin_time": next_midnight.isoformat()
    })
