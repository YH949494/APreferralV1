from flask import request, jsonify
from pymongo import MongoClient
import os
from datetime import datetime, timedelta

# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

def handle_checkin():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    user = users_collection.find_one({"user_id": user_id})
    now = datetime.utcnow()

    if not user:
        user = {
            "user_id": user_id,
            "username": username,
            "xp": 0,
            "weekly_xp": 0,
            "referral_count": 0,
            "last_checkin": None,
            "streak_count": 0,
            "badges": []
        }

    last_checkin = user.get("last_checkin")
    streak_count = user.get("streak_count", 0)
    badges = user.get("badges", [])

    if last_checkin:
        elapsed = now - last_checkin
        if elapsed < timedelta(hours=24):
            remaining = timedelta(hours=24) - elapsed
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            return jsonify({
                "success": False,
                "message": f"⏳ Come back in {hours}h {minutes}m to check in again!"
            })
        elif elapsed < timedelta(hours=48):
            streak_count += 1
        else:
            # Missed a day, reset
            streak_count = 1
            badges = []

    else:
        streak_count = 1

    # Award badges
    if streak_count >= 28 and "gold" not in badges:
        badges.append("gold")
    elif streak_count >= 14 and "silver" not in badges:
        if "gold" in badges:
            pass
        else:
            badges.append("silver")
    elif streak_count >= 7 and "bronze" not in badges:
        if "gold" in badges or "silver" in badges:
            pass
        else:
            badges.append("bronze")

    # Update DB
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "last_checkin": now,
                "streak_count": streak_count,
                "badges": badges
            },
            "$inc": {
                "xp": 20,
                "weekly_xp": 20
            }
        },
        upsert=True
    )

    return jsonify({
        "success": True,
        "message": f"✅ Check-in successful! +20 XP\n🔥 Current streak: {streak_count} days"
    })

    # Update XP
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "last_checkin": now
            },
            "$inc": {
                "xp": 20,
                "weekly_xp": 20
            }
        },
        upsert=True
    )

    return jsonify({
        "success": True,
        "message": "✅ Check-in successful! +20 XP"
    })
