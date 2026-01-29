from pymongo import MongoClient, ASCENDING 
from pymongo.errors import DuplicateKeyError
import os
import datetime
from datetime import timezone, timedelta
import logging
import pytz  # use pytz (no ZoneInfo here)
from xp import grant_xp

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")
logger = logging.getLogger(__name__)

_client = None
_db = None
_indexes_initialized = False


class CollectionProxy:
    def __init__(self, name: str):
        self._name = name

    def _collection(self):
        return get_db()[self._name]

    def __getattr__(self, item):
        return getattr(self._collection(), item)

    def __repr__(self) -> str:
        return f"<CollectionProxy name={self._name}>"


class DatabaseProxy:
    def __getitem__(self, name: str):
        return CollectionProxy(name)

    def __getattr__(self, item):
        return getattr(get_db(), item)

    def __repr__(self) -> str:
        return "<DatabaseProxy>"


def init_db(mongo_url: str | None = None, db_name: str = "referral_bot") -> None:
    global _client, _db
    if _db is not None:
        return
    mongo_url = mongo_url or os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is not configured")
    _client = MongoClient(mongo_url)
    _db = _client[db_name]
    ensure_indexes()


def get_db():
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


def get_collection(name: str) -> CollectionProxy:
    return CollectionProxy(name)


def ensure_indexes() -> None:
    global _indexes_initialized
    if _indexes_initialized:
        return
    db_ref = get_db()
    db_ref["voucher_whitelist"].create_index([("code", ASCENDING)], unique=True)
    db_ref["voucher_whitelist"].create_index([("username", ASCENDING), ("start_at", ASCENDING)])
    db_ref["voucher_whitelist"].create_index([("end_at", ASCENDING)])

    db_ref["users"].create_index([("user_id", ASCENDING)], unique=True)
    db_ref["users"].create_index([("username", ASCENDING)])

    db_ref["user_snapshots"].create_index([("user_id", ASCENDING)], unique=True)

    db_ref["monthly_xp_history"].create_index([("user_id", ASCENDING), ("month", ASCENDING)], unique=True)
    db_ref["monthly_xp_history"].create_index([("month", ASCENDING)])

    db_ref["channel_subscription_cache"].create_index([("user_id", ASCENDING)], unique=True)
    db_ref["channel_subscription_cache"].create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)

    try:
        db_ref["admin_xp_cooldowns"].create_index([("expireAt", ASCENDING)], expireAfterSeconds=0)
    except Exception:
        logger.warning(
            "[ADMIN_XP] Failed to create TTL index for admin_xp_cooldowns",
            exc_info=True,
        )
    
    _indexes_initialized = True


db = DatabaseProxy()
leaderboard_collection = get_collection("weekly_leaderboard")
voucher_whitelist = get_collection("voucher_whitelist")

# === USERS COLLECTION ===
users_collection = get_collection("users")

# SNAPSHOT FIELDS — ONLY WRITTEN BY WORKER
# weekly_xp, monthly_xp, total_xp, weekly_referrals, monthly_referrals, total_referrals, vip_tier, vip_month
# DEPRECATED — DO NOT USE (ledger-based referrals only)
# weekly_referral_count, total_referral_count, ref_count_total, monthly_referral_count

user_snapshots_col = get_collection("user_snapshots")
                                
monthly_xp_history_collection = get_collection("monthly_xp_history")                          

channel_subscription_cache = get_collection("channel_subscription_cache")
admin_xp_cooldowns = get_collection("admin_xp_cooldowns")

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
                "last_checkin": None,                
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
            "$setOnInsert": {"status": "Normal"},
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
        return {
            "xp": 0,
            "weekly_xp": 0,
            "monthly_xp": 0,
            "weekly_referrals": 0,
            "monthly_referrals": 0,
            "total_referrals": 0,
        }
    return {
        "xp": user.get("xp", 0),                     # Lifetime XP
        "weekly_xp": user.get("weekly_xp", 0),       # Weekly XP
        "monthly_xp": user.get("monthly_xp", 0),     # Monthly XP ✅
        "weekly_referrals": user.get("weekly_referrals", 0),
        "monthly_referrals": user.get("monthly_referrals", 0),
        "total_referrals": user.get("total_referrals", 0),
    }

# === ADMIN XP CONTROL ===
def _acquire_admin_xp_cooldown_lock(uid: int, amount: int, *, cooldown_seconds: int) -> bool:
    now = datetime.datetime.now(timezone.utc)
    expire_at = now + timedelta(seconds=cooldown_seconds)
    lock_id = f"admin_xp:{uid}:{amount}"
    try:
        admin_xp_cooldowns.insert_one({
            "_id": lock_id,
            "uid": uid,
            "amount": amount,
            "createdAt": now,
            "expireAt": expire_at,
        })
        return True
    except DuplicateKeyError:
        return False
    
def update_user_xp(username, amount, unique_key: str | None = None):
    # Match username case-insensitively
    user = users_collection.find_one({
        "username": { "$regex": f"^{username}$", "$options": "i" }
    })

    if not user:
        return False, "User not found."

    cooldown_seconds = int(os.getenv("ADMIN_XP_COOLDOWN_SECONDS", "60"))
    if unique_key is None and cooldown_seconds > 0:
        amount_int = int(amount)
        ok = _acquire_admin_xp_cooldown_lock(
            user["user_id"],
            amount_int,
            cooldown_seconds=cooldown_seconds,
        )
        if not ok:
            logger.info("[ADMIN_XP] cooldown_hit uid=%s amount=%s", user["user_id"], amount)
            return {
                "ok": False,
                "code": "cooldown",
                "message": f"Please wait {cooldown_seconds} seconds before granting again.",
            }
        lock_id = f"admin_xp:{user['user_id']}:{amount_int}"
    if unique_key:
        key = unique_key
    else:
        timestamp = int(datetime.datetime.now(timezone.utc).timestamp())
        key = f"admin:{user['user_id']}:{username.lower()}:{amount}:{timestamp}"
    
    granted = grant_xp(db, user["user_id"], "admin_adjust", key, amount)
    if not granted:
        logger.info("[ADMIN_XP][DUPLICATE] uid=%s key=%s", user["user_id"], key)        
        if unique_key is None and cooldown_seconds > 0:
            admin_xp_cooldowns.delete_one({"_id": lock_id})  
        return False, "Duplicate admin XP grant ignored."

    logger.info(
        "[ADMIN_XP][GRANTED] uid=%s amount=%s key=%s",
        user["user_id"],
        amount,
        key,
    )        
    return True, f"XP {'added' if amount > 0 else 'reduced'} by {abs(amount)}."

def save_weekly_snapshot():
    # DEPRECATED - do not use (rolling UTC window, replaced by KL windows + ledger snapshots)
    if os.environ.get("ENABLE_LEGACY_WEEKLY_SNAPSHOT") != "1":
        logger.warning("[SNAPSHOT][DEPRECATED] save_weekly_snapshot disabled")
        return  
    now = datetime.datetime.utcnow()
    week_start = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    week_end = now.strftime("%Y-%m-%d")

    # Top lists (limit can be adjusted)
    top_checkins = list(
        users_collection.find({}, {"user_id": 1, "username": 1, "weekly_xp": 1})
        .sort("weekly_xp", -1).limit(50)
    )
    top_referrals = list(
        users_collection.find({}, {"user_id": 1, "username": 1, "weekly_referrals": 1})
        .sort("weekly_referrals", -1).limit(50)
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
            {"user_id": u["user_id"], "username": u.get("username"), "weekly_referrals": u.get("weekly_referrals", 0)}
            for u in top_referrals
        ],
        "archived_at": now
    })

    # ✅ Reset weekly counters for the new week
    users_collection.update_many({}, {
        "$set": {"weekly_xp": 0, "weekly_referrals": 0}
    })
