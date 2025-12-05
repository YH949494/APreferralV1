import os
from datetime import datetime, timedelta, time

from flask import jsonify, request
from pymongo import MongoClient

from config import KL_TZ, XP_BASE_PER_CHECKIN, STREAK_MILESTONES, FIRST_CHECKIN_BONUS


# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# Timezone & XP come from a single source of truth (config.py)

def handle_checkin():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    now = datetime.now(KL_TZ)
    today = now.date()
    # next local midnight in KL
    next_midnight = datetime.combine(today + timedelta(days=1), time(0, 0, 0, tzinfo=KL_TZ))

    user = users_collection.find_one({"user_id": user_id})

    if not user:
        # First-time user
        total_xp = XP_BASE_PER_CHECKIN + FIRST_CHECKIN_BONUS

        users_collection.insert_one({
            "user_id": user_id,
            "username": username,
            "xp": total_xp,
            "weekly_xp": total_xp,
            "monthly_xp": total_xp,
            "referral_count": 0,
            "weekly_referral_count": 0,
            "last_checkin": now,
            "streak": 1
        })
        return jsonify({
            "success": True,
            "message": (
                f"✅ First check-in! +{XP_BASE_PER_CHECKIN} XP "
                f"🎁 First-time bonus: +{FIRST_CHECKIN_BONUS} XP"
            ),
            "next_checkin_time": next_midnight.isoformat()
        })

    last_checkin = user.get("last_checkin")
    streak = user.get("streak", 0)

    if last_checkin:
        last_date = last_checkin.astimezone(KL_TZ).date()

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

    # Bonus XP from unified milestone table
    bonus_xp = STREAK_MILESTONES.get(streak, 0)

    total_xp = XP_BASE_PER_CHECKIN + bonus_xp

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
        "message": f"✅ Check-in successful! +{XP_BASE_PER_CHECKIN} XP{bonus_text}",
        "next_checkin_time": next_midnight.isoformat()
    })
