from pymongo import MongoClient
import os
import datetime

# === MongoDB Setup ===
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["telegram_bot"]
users_collection = db["users"]
leaderboard_collection = db["weekly_leaderboard"]

# === USER INITIALIZATION ===
def ensure_user(user_id, username):
    users_collection.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "username": username,
            "xp": 0,
            "last_checkin": None,
            "referral_count": 0,
            "checkin_streak": 0,
            "last_checkin_date": None,
            "badges": {
                "streak_7": False,
                "streak_14": False,
                "streak_28": False
            }
        }},
        upsert=True
    )

# === CHECK-IN ELIGIBILITY ===
def can_checkin(user_id):
    user = users_collection.find_one({"user_id": user_id})
    now = datetime.datetime.utcnow()

    if not user:
        return True  # New user, allow check-in

    last = user.get("last_checkin")
    if not last:
        return True

    return (now - last).total_seconds() >= 86400  # 24 hours

# === HANDLE CHECK-IN + STREAKS ===
def checkin_user(user_id):
    now = datetime.datetime.utcnow()
    today = now.date()

    user = users_collection.find_one({"user_id": user_id})
    last_checkin_date = user.get("last_checkin_date")
    streak = user.get("checkin_streak", 0)
    badges = user.get("badges", {
        "streak_7": False,
        "streak_14": False,
        "streak_28": False
    })

    # Handle streak logic
    if last_checkin_date:
        last_date = datetime.datetime.strptime(last_checkin_date, "%Y-%m-%d").date()
        delta_days = (today - last_date).days
        if delta_days == 1:
            streak += 1
        elif delta_days > 1:
            streak = 1  # Reset streak if missed a day
    else:
        streak = 1  # First ever check-in

    # Update badges
    if streak >= 7:
        badges["streak_7"] = True
    if streak >= 14:
        badges["streak_14"] = True
    if streak >= 28:
        badges["streak_28"] = True

    # Update database
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "last_checkin": now,
                "checkin_streak": streak,
                "last_checkin_date": today.strftime("%Y-%m-%d"),
                "badges": badges
            },
            "$inc": {
                "xp": 20
            }
        }
    )

# === REFERRALS ===
def increment_referral(referrer_id):
    users_collection.update_one(
        {"user_id": referrer_id},
        {"$inc": {"referral_count": 1}}
    )

# === GET USER STATS FOR FRONTEND ===
def get_user_stats(user_id):
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return {
            "xp": 0,
            "referral_count": 0,
            "checkin_streak": 0,
            "badges": {
                "streak_7": False,
                "streak_14": False,
                "streak_28": False
            }
        }

    return {
        "xp": user.get("xp", 0),
        "referral_count": user.get("referral_count", 0),
        "checkin_streak": user.get("checkin_streak", 0),
        "badges": user.get("badges", {
            "streak_7": False,
            "streak_14": False,
            "streak_28": False
        })
    }

# === MANUAL XP ADD ===
def add_xp(user_id, amount):
    users_collection.update_one(
        {"user_id": user_id},
        {"$inc": {"xp": amount}}
    )
