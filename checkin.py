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
            "last_checkin": None
        }

    last_checkin = user.get("last_checkin")
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
