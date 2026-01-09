from pymongo import MongoClient, ASCENDING 
import os
import datetime
import logging
import pytz  # use pytz (no ZoneInfo here)
from xp import grant_xp

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")
logger = logging.getLogger(__name__)

MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
leaderboard_collection = db["weekly_leaderboard"]

voucher_whitelist = db["voucher_whitelist"]
voucher_whitelist.create_index([("code", ASCENDING)], unique=True)
voucher_whitelist.create_index([("username", ASCENDING), ("start_at", ASCENDING)])
voucher_whitelist.create_index([("end_at", ASCENDING)])

# === USERS COLLECTION ===
users_collection = db["users"]
users_collection.create_index([("user_id", ASCENDING)], unique=True)
users_collection.create_index([("username", ASCENDING)])

monthly_xp_history_collection = db["monthly_xp_history"]
monthly_xp_history_collection.create_index([("user_id", ASCENDING), ("month", ASCENDING)], unique=True)
monthly_xp_history_collection.create_index([("month", ASCENDING)])                              

channel_subscription_cache = db["channel_subscription_cache"]
channel_subscription_cache.create_index([("user_id", ASCENDING)], unique=True)
channel_subscription_cache.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)

def init_user(user_id, username):
    """Create user if missing; keep username in sync if it changed."""
    users_collection.update_one(
        {"user_id": user_id},
        {
            # keep username updated on subsequent calls
            "$set": {"username": username},
            # only set these on first insert
            "$setOnInsert": {
                "user_id": user_id,
                "username": username,
                "xp": 0,                  # Lifetime XP
                "weekly_xp": 0,           # Weekly XP
                "monthly_xp": 0,          # Monthly XP ✅
                "weekly_referral_count": 0,                
                "last_checkin": None,
                "referral_count": 0,
                "status": "Normal",       # or "VIP1"
                "next_status": "VIP1",    # scheduled for next month
                "last_status_update": "2025-08-01"
            }
        },
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
            "$setOnInsert": {"xp": 0, "weekly_xp": 0, "monthly_xp": 0, "status": "Normal"},
        },
        upsert=True,
    )
    grant_xp(db, user_id, "checkin", f"checkin:{now.strftime('%Y%m%d')}", 20)
    
# === REFERRAL LOGIC ===
def increment_referral(referrer_id, referred_user_id=None):
    raise RuntimeError(
        "Legacy referral function removed: use users-based referral flow in main.py"
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
def update_user_xp(username, amount, unique_key: str | None = None):
    # Match username case-insensitively
    user = users_collection.find_one({
        "username": { "$regex": f"^{username}$", "$options": "i" }
    })

    if not user:
        return False, "User not found."
    key = unique_key or f"admin:{user['user_id']}:{username.lower()}:{amount}"
    
    granted = grant_xp(db, user["user_id"], "admin_adjust", key, amount)
    if not granted:
        return False, "Duplicate admin XP grant ignored."
        
    return True, f"XP {'added' if amount > 0 else 'reduced'} by {abs(amount)}."

def save_weekly_snapshot():
    now = datetime.datetime.utcnow()
    week_start = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    week_end = now.strftime("%Y-%m-%d")

    # Top lists (limit can be adjusted)
    top_checkins = list(
        users_collection.find({}, {"user_id": 1, "username": 1, "weekly_xp": 1})
        .sort("weekly_xp", -1).limit(50)
    )
    top_referrals = list(
        users_collection.find({}, {"user_id": 1, "username": 1, "weekly_referral_count": 1})
        .sort("weekly_referral_count", -1).limit(50)
    )

    # ✅ Match main app's collection & fields
    db["weekly_leaderboard_history"].insert_one({
        "week_start": week_start,
        "week_end": week_end,
        "checkin_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username"), "weekly_xp": u.get("weekly_xp", 0)}
            for u in top_checkins
        ],
        "referral_leaderboard": [
            {"user_id": u["user_id"], "username": u.get("username"), "weekly_referral_count": u.get("weekly_referral_count", 0)}
            for u in top_referrals
        ],
        "archived_at": now
    })

    # ✅ Reset weekly counters for the new week
    users_collection.update_many({}, {
        "$set": {"weekly_xp": 0, "weekly_referral_count": 0}
    })
