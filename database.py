from pymongo import MongoClient, ASCENDING 
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError
import os
import datetime
from datetime import timezone, timedelta
import logging
import pytz  # use pytz (no ZoneInfo here)
from xp import grant_xp
from time_utils import as_aware_utc

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


def _normalize_index_keys(keys):
    """Normalize Mongo index key specs to a comparable tuple format."""
    if isinstance(keys, dict):
        items = keys.items()
    else:
        items = keys
    out = []
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            out.append((item[0], item[1]))
    return tuple(out)


def _find_equivalent_index_name(collection, keys):
    requested = _normalize_index_keys(keys)
    for idx in collection.list_indexes():
        idx_keys = idx.get("key")
        if _normalize_index_keys(idx_keys) == requested:
            return idx.get("name")
    return None


def safe_create_index(collection, keys, *, name: str, unique: bool = False, partialFilterExpression=None):
    try:
        existing = collection.index_information()
        if name in existing:
            logger.info("[DB][INDEX] exists collection=%s name=%s", getattr(collection, "name", "unknown"), name)
            return name
        kwargs = {"name": name, "unique": unique}
        if partialFilterExpression is not None:
            kwargs["partialFilterExpression"] = partialFilterExpression
        created_name = collection.create_index(keys, **kwargs)
        logger.info("[DB][INDEX] created collection=%s name=%s", getattr(collection, "name", "unknown"), name)
        return created_name or name
    except OperationFailure as exc:
        if exc.code in (68, 85, 86):
            existing_name = _find_equivalent_index_name(collection, keys)
            if existing_name:
                logger.info(
                    "[DB][INDEX] already_exists_equivalent collection=%s requested=%s existing=%s",
                    getattr(collection, "name", "unknown"),
                    name,
                    existing_name,
                )
                return existing_name
        logger.warning(
            "[DB][INDEX] create_failed collection=%s name=%s error=%s",
            getattr(collection, "name", "unknown"),
            name,
            exc,
        )
        return False
    except PyMongoError as exc:
        logger.warning(
            "[DB][INDEX] create_failed collection=%s name=%s error=%s",
            getattr(collection, "name", "unknown"),
            name,
            exc,
        )
        return False


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
    # Case-insensitive username index used by the backend segment engine's
    # batched $in lookup (locale+strength must match the query collation exactly
    # or MongoDB falls back to a full collection scan).
    try:
        db_ref["users"].create_index(
            [("username", ASCENDING)],
            name="users_username_ci",
            collation={"locale": "en", "strength": 2},
        )
    except Exception:
        logger.warning("[DB][INDEX] Failed to create users_username_ci", exc_info=True)

    db_ref["user_snapshots"].create_index([("user_id", ASCENDING)], unique=True)

    db_ref["segment_snapshots"].create_index(
        [("user_id", ASCENDING), ("snapshot_week", ASCENDING)], unique=True
    )
    db_ref["segment_snapshots"].create_index([("snapshot_month", ASCENDING)])
    db_ref["segment_snapshots"].create_index([("snapshot_week", ASCENDING)])

    db_ref["user_claim_risk_history"].create_index([("user_id", ASCENDING), ("synced_at", ASCENDING)])

    # Phase 3 — backend-owned segment engine, shadow mode only. Idempotent
    # per (account, snapshot_week); never read by the bot, never written by
    # bot_segment_sync/claim_risk_sync.
    #
    # Drop the Phase 6A unique index (user_id, snapshot_month) if it exists on
    # this deployment before creating the new one, otherwise MongoDB keeps
    # enforcing the old key and rejects inserts with user_id=None or multiple
    # weeks in the same month for the same user.
    try:
        db_ref["backend_segment_snapshots"].drop_index(
            [("user_id", ASCENDING), ("snapshot_month", ASCENDING)]
        )
        logger.info("[DB] Dropped stale backend_segment_snapshots index (user_id, snapshot_month)")
    except Exception:
        pass  # index didn't exist — nothing to drop
    db_ref["backend_segment_snapshots"].create_index(
        [("account", ASCENDING), ("snapshot_week", ASCENDING)], unique=True
    )
    db_ref["backend_segment_snapshots"].create_index([("snapshot_month", ASCENDING)])
    db_ref["backend_segment_snapshots"].create_index([("snapshot_week", ASCENDING)])

    # Phase 3C — async job tracking for backend segment engine runs
    db_ref["backend_segment_engine_runs"].create_index([("job_id", ASCENDING)], unique=True)
    db_ref["backend_segment_engine_runs"].create_index(
        [("snapshot_week", ASCENDING), ("dry_run", ASCENDING), ("status", ASCENDING)]
    )

    # voucher_claims.user_id — critical for run_shadow_segment_engine's aggregate
    db_ref["voucher_claims"].create_index([("user_id", ASCENDING)])

    # Phase 2A — weekly marketing raw-data upload. dedupe_key prevents
    # re-uploading the same weekly file from creating duplicate rows;
    # upload_batch_id is indexed for upload-history lookups.
    db_ref["marketing_raw_data"].create_index([("dedupe_key", ASCENDING)], unique=True)
    db_ref["marketing_raw_data"].create_index([("upload_batch_id", ASCENDING)])
    db_ref["marketing_raw_data"].create_index([("snapshot_week", ASCENDING)])
    # Phase 2B — Raw Data Explorer aggregation indexes.
    db_ref["marketing_raw_data"].create_index([("snapshot_month", ASCENDING)])
    db_ref["marketing_raw_data"].create_index([("account", ASCENDING)])
    db_ref["marketing_raw_data"].create_index([("campaign_id", ASCENDING)])

    db_ref["marketing_upload_batches"].create_index([("upload_batch_id", ASCENDING)], unique=True)
    db_ref["marketing_upload_batches"].create_index([("uploaded_at", ASCENDING)])

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
segment_snapshots_col = get_collection("segment_snapshots")
user_claim_risk_history_col = get_collection("user_claim_risk_history")

# Phase 6A — backend segment engine shadow output. Reference/audit only;
# nothing in the bot reads this collection yet (see backend_segment_engine.py).
backend_segment_snapshots_col = get_collection("backend_segment_snapshots")
backend_segment_engine_runs_col = get_collection("backend_segment_engine_runs")

# Phase 2A — weekly Marketing raw-data upload (data ingestion only; no
# segment calculation, no users.bot_segment writes). See marketing_upload.py.
# Also used by the Phase 6A backend segment engine for marketing field lookups.
marketing_raw_data_col = get_collection("marketing_raw_data")
marketing_upload_batches_col = get_collection("marketing_upload_batches")

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
    now = datetime.datetime.now(timezone.utc)

    if not user:
        return True  # User not found, treat as first time

    last = user.get("last_checkin")
    if not last:
        return True

    # Allow once every 24h
    last_utc = as_aware_utc(last)
    if not last_utc:
        return True
    return (now - last_utc).total_seconds() >= 86400

def checkin_user(user_id):
    now = datetime.datetime.now(timezone.utc)
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
    now = datetime.datetime.now(timezone.utc)
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
