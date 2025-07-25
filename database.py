from pymongo import MongoClient
import os
import datetime

MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["telegram_bot"]
leaderboard_collection = db["weekly_leaderboard"]

# === USERS COLLECTION ===
users_collection = db["users"]

# Initialize or update user info
def init_user(user_id, username):
    users_collection.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "username": username,
            "xp": 0,
            "last_checkin": None,
            "referral_count": 0,
        }},
        upsert=True
    )

# === CHECK-IN LOGIC ===
def can_checkin(user_id):
    user = users_collection.find_one({"user_id": user_id})
    now = datetime.datetime.utcnow()

    if not user:
        return True  # User not found, treat as first time

    last = user.get("last_checkin")
    if not last:
        return True

    # Allow once every 24h
    return (now - last).total_seconds() >= 86400

def checkin_user(user_id):
    now = datetime.datetime.utcnow()
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {"last_checkin": now},
            "$inc": {"xp": 20}
        }
    )

# === REFERRAL LOGIC ===
def increment_referral(referrer_id):
    users_collection.update_one(
        {"user_id": referrer_id},
        {"$inc": {"referral_count": 1}}
    )

# === RETRIEVE STATS ===
def get_user_stats(user_id):
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return {"xp": 0, "referral_count": 0}
    return {
        "xp": user.get("xp", 0),
        "referral_count": user.get("referral_count", 0)
    }

