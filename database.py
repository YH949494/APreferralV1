from pymongo import MongoClient
import os
import datetime

MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
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
            "xp": 0,                  # Lifetime XP
            "weekly_xp": 0,           # Weekly XP
            "monthly_xp": 0,          # Monthly XP ✅
            "last_checkin": None,
            "referral_count": 0,
            "status": "Normal",       # or "VIP1"
            "next_status": "VIP1",    # scheduled for next month
            "last_status_update": "2025-08-01"
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
            "$inc": {
                "xp": 20,           # Lifetime XP
                "weekly_xp": 20,    # Weekly XP ✅
                "monthly_xp": 20    # Monthly XP ✅
            }
        }
    )

# === REFERRAL LOGIC ===
def increment_referral(referrer_id):
    users_collection.update_one(
        {"user_id": referrer_id},
        {
            "$inc": {
                "referral_count": 1,
                "xp": 20,           # Lifetime XP
                "weekly_xp": 20,    # Weekly XP ✅
                "monthly_xp": 20    # Monthly XP ✅
            }
        }
    )

# === RETRIEVE STATS ===
def get_user_stats(user_id):
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return {"xp": 0, "weekly_xp": 0, "monthly_xp": 0, "referral_count": 0}
    return {
        "xp": user.get("xp", 0),                     # Lifetime XP
        "weekly_xp": user.get("weekly_xp", 0),       # Weekly XP
        "monthly_xp": user.get("monthly_xp", 0),     # Monthly XP ✅
        "referral_count": user.get("referral_count", 0)
    }

# === ADMIN XP CONTROL ===
def update_user_xp(username, amount):
    # Match username case-insensitively
    user = users_collection.find_one({
        "username": { "$regex": f"^{username}$", "$options": "i" }
    })

    if not user:
        return False, "User not found."

    users_collection.update_one(
        { "username": { "$regex": f"^{username}$", "$options": "i" } },
        {
            "$inc": {
                "xp": amount,          # Lifetime XP
                "weekly_xp": amount,   # Weekly XP ✅
                "monthly_xp": amount   # Monthly XP ✅
            }
        }
    )

    return True, f"XP {'added' if amount > 0 else 'reduced'} by {abs(amount)}."

def save_weekly_snapshot():
    leaderboard = {
        "week_start": (datetime.datetime.utcnow() - datetime.timedelta(days=7)).strftime("%Y-%m-%d"),
        "week_end": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
        "checkin_leaderboard": list(
            users_collection.find({}, {"username": 1, "weekly_xp": 1})
            .sort("weekly_xp", -1).limit(15)
        ),
        "referral_leaderboard": list(
            users_collection.find({}, {"username": 1, "referral_count": 1})
            .sort("referral_count", -1).limit(15)
        ),
        "created_at": datetime.datetime.utcnow()
    }
    db.leaderboard_history.insert_one(leaderboard)

    # ✅ Reset for new week
    users_collection.update_many({}, {"$set": {"weekly_xp": 0, "referral_count": 0}})
