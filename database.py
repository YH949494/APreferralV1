from pymongo import MongoClient
import os
import datetime

MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["telegram_bot"]

users_collection = db["users"]
leaderboard_collection = db["weekly_leaderboard"]
weekly_history_collection = db["weekly_history"]

# === USERS ===
def init_user(user_id, username):
    users_collection.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "username": username,
            "xp": 0,
            "weekly_xp": 0,
            "last_checkin": None,
            "referral_count": 0,
        }},
        upsert=True
    )

# === CHECK-IN ===
def can_checkin(user_id):
    user = users_collection.find_one({"user_id": user_id})
    now = datetime.datetime.utcnow()
    last = user.get("last_checkin") if user else None
    return not last or (now - last).total_seconds() >= 86400

def checkin_user(user_id):
    now = datetime.datetime.utcnow()
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {"last_checkin": now},
            "$inc": {"xp": 20, "weekly_xp": 20}
        }
    )

# === REFERRALS ===
def increment_referral(referrer_id):
    users_collection.update_one(
        {"user_id": referrer_id},
        {"$inc": {"referral_count": 1, "xp": 10, "weekly_xp": 10}}
    )

# === ADMIN XP CONTROL ===
def update_user_xp(username, amount):
    user = users_collection.find_one({"username": {"$regex": f"^{username}$", "$options": "i"}})
    if not user:
        return False, f"User @{username} not found."

    new_xp = max(0, user.get("xp", 0) + amount)
    new_weekly = max(0, user.get("weekly_xp", 0) + amount)

    users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "xp": new_xp,
            "weekly_xp": new_weekly
        }}
    )
    action = "Added" if amount > 0 else "Removed"
    return True, f"{action} {abs(amount)} XP for @{username}."

def remove_xp(username, amount):
    return update_user_xp(username, -abs(amount))

# === RETRIEVE USER DATA ===
def get_user_stats(user_id):
    user = users_collection.find_one({"user_id": user_id})
    return {
        "xp": user.get("xp", 0),
        "referral_count": user.get("referral_count", 0)
    } if user else {"xp": 0, "referral_count": 0}

def get_user_data(user_id):
    user = users_collection.find_one({"user_id": user_id})
    return {
        "user_id": user_id,
        "username": user.get("username", ""),
        "xp": user.get("xp", 0),
        "referral_count": user.get("referral_count", 0),
        "last_checkin": user.get("last_checkin"),
        "weekly_xp": user.get("weekly_xp", 0)
    } if user else {
        "user_id": user_id,
        "username": "",
        "xp": 0,
        "referral_count": 0,
        "last_checkin": None,
        "weekly_xp": 0
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

def get_weekly_leaderboard(limit=20):
    top_users = users_collection.find().sort("weekly_xp", -1).limit(limit)
    return [
        {
            "username": user.get("username", ""),
            "weekly_xp": user.get("weekly_xp", 0),
            "referral_count": user.get("referral_count", 0)
        }
        for user in top_users
    ]

# === HISTORY ===
def get_weekly_history():
    history = weekly_history_collection.find().sort("week_start", -1).limit(4)
    return [
        {
            "week_start": h.get("week_start"),
            "leaderboard": h.get("leaderboard", [])
        }
        for h in history
    ]

def reset_weekly_xp():
    users_collection.update_many({}, {"$set": {"weekly_xp": 0}})

# === LOG JOIN REQUEST ===
def log_join_request(user_id, referrer_id):
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {"referred_by": referrer_id},
            "$inc": {"xp": 20}  # Optional: reward for being referred
        },
        upsert=True
    )

    users_collection.update_one(
        {"user_id": referrer_id},
        {"$inc": {"referral_count": 1, "xp": 20}}  # Optional: reward referrer
    )
