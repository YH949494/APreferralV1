from pymongo import MongoClient
import os
import datetime

MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["telegram_bot"]
leaderboard_collection = db["weekly_leaderboard"]
weekly_history_collection = db["weekly_history"]

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

# === ADMIN XP UPDATE ===
def update_user_xp(username, amount):
    user = users_collection.find_one({"username": {"$regex": f"^{username}$", "$options": "i"}})
    if not user:
        return False, f"User @{username} not found."

    new_xp = max(0, user.get("xp", 0) + amount)
    new_weekly_xp = max(0, user.get("weekly_xp", 0) + amount)

    users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "xp": new_xp,
            "weekly_xp": new_weekly_xp
        }}
    )
    return True, f"{'Added' if amount > 0 else 'Removed'} {abs(amount)} XP to @{username}."

# === GET USER DATA ===
def get_user_data(user_id):
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return {
            "user_id": user_id,
            "username": "",
            "xp": 0,
            "referral_count": 0,
            "last_checkin": None
        }
    return {
        "user_id": user["user_id"],
        "username": user.get("username", ""),
        "xp": user.get("xp", 0),
        "referral_count": user.get("referral_count", 0),
        "last_checkin": user.get("last_checkin")
    }

# === LEADERBOARD ===
def get_leaderboard(limit=20):
    top_users = users_collection.find().sort("xp", -1).limit(limit)
    return [
        {
            "username": user.get("username", ""),
            "xp": user.get("xp", 0),
            "referral_count": user.get("referral_count", 0)
        }
        for user in top_users
    ]
    
# === WEEKLY HISTORY ===
def get_weekly_history():
    history = weekly_history_collection.find().sort("week_start", -1).limit(4)
    return [
        {
            "week_start": h.get("week_start"),
            "leaderboard": h.get("leaderboard", [])
        }
        for h in history
    ]
