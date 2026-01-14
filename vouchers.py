from flask import Blueprint, request, jsonify, current_app
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import OperationFailure, DuplicateKeyError
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone
import requests
from config import KL_TZ
import time
import hmac, hashlib, urllib.parse, os, json
import config as _cfg 
 
from database import db, users_collection
 
admin_cache_col = db["admin_cache"]
new_joiner_claims_col = db["new_joiner_claims"]
claim_rate_limits_col = db["claim_rate_limits"]
profile_photo_cache_col = db["profile_photo_cache"]
welcome_tickets_col = db["welcome_tickets"]
subscription_cache_col = db["subscription_cache"]
welcome_eligibility_col = db["welcome_eligibility"]

BYPASS_ADMIN = os.getenv("BYPASS_ADMIN", "0").lower() in ("1", "true", "yes", "on")
HARDCODED_ADMIN_USERNAMES = {"gracy_ap", "teohyaohui"}  # allow manual overrides if cache is empty
WELCOME_WINDOW_HOURS = int(getattr(_cfg, "WELCOME_WINDOW_HOURS", os.getenv("WELCOME_WINDOW_HOURS", "48")))
WELCOME_WINDOW_DAYS = 7
PROFILE_PHOTO_CACHE_TTL_SECONDS = 60
CLAIM_RATE_LIMIT_SECONDS = 5
_claim_rate_limit_cache = {}

_RAW_OFFICIAL_CHANNEL_ID = getattr(_cfg, "OFFICIAL_CHANNEL_ID", os.getenv("OFFICIAL_CHANNEL_ID"))
try:
    OFFICIAL_CHANNEL_ID = int(str(_RAW_OFFICIAL_CHANNEL_ID).strip()) if _RAW_OFFICIAL_CHANNEL_ID not in (None, "") else None
except (TypeError, ValueError):
    OFFICIAL_CHANNEL_ID = None

_RAW_OFFICIAL_CHANNEL_USERNAME = getattr(_cfg, "OFFICIAL_CHANNEL_USERNAME", os.getenv("OFFICIAL_CHANNEL_USERNAME"))
OFFICIAL_CHANNEL_USERNAME = (str(_RAW_OFFICIAL_CHANNEL_USERNAME).strip() or "") if _RAW_OFFICIAL_CHANNEL_USERNAME is not None else ""
_RAW_MAIN_GROUP_ID = getattr(_cfg, "MAIN_GROUP_ID", os.getenv("MAIN_GROUP_ID", "-1002304653063"))
try:
    MAIN_GROUP_ID = int(str(_RAW_MAIN_GROUP_ID).strip()) if _RAW_MAIN_GROUP_ID not in (None, "") else None
except (TypeError, ValueError):
    MAIN_GROUP_ID = None
 
def _load_admin_ids() -> set:
    try:
        doc = admin_cache_col.find_one({"_id": "admins"}) or {}
    except Exception as e:
        print(f"[admin] failed to load cache: {e}")
        return set()

    ids = set()
    for raw in doc.get("ids", []):
        try:
            ids.add(int(raw))
        except (TypeError, ValueError):
            continue
    return ids


def _is_cached_admin(user_json: dict):
    if not isinstance(user_json, dict):
        return False, None

    try:
        user_id = int(user_json.get("id"))
    except (TypeError, ValueError):
        user_id = None

    admin_ids = _load_admin_ids()
    if user_id is not None and user_id in admin_ids:
        return True, "cache"

    username_lower = norm_username(user_json.get("username", ""))
    if username_lower in HARDCODED_ADMIN_USERNAMES:
        return True, "allowlist"

    return False, None

def _payload_for_admin_query(req) -> dict | None:
    try:
        caller_id = req.args.get("user_id", type=int)
    except Exception:
        caller_id = None

    if not caller_id:
        return None

    admin_ids = _load_admin_ids()
    if caller_id not in admin_ids:
        return None

    payload = {"id": caller_id, "adminSource": "cache"}
    username_hint = norm_username(req.args.get("username") or req.args.get("admin_username") or "")
    if username_hint:
        payload["usernameLower"] = username_hint

    return payload

def _clean_token_list(raw: str):
    return [tok.strip() for tok in raw.split(",") if tok.strip()]

_ADMIN_PANEL_SECRET = getattr(_cfg, "ADMIN_PANEL_SECRET", os.getenv("ADMIN_PANEL_SECRET", ""))
_ADMIN_USER_IDS = set()
try:
    raw_admin_ids = getattr(_cfg, "ADMIN_USER_IDS", os.getenv("ADMIN_USER_IDS", ""))
    if isinstance(raw_admin_ids, (list, tuple, set)):
        iter_ids = raw_admin_ids
    else:
        iter_ids = [x.strip() for x in str(raw_admin_ids).split(",") if x.strip()]
    for rid in iter_ids:
        try:
            _ADMIN_USER_IDS.add(int(rid))
        except (TypeError, ValueError):
            continue
except Exception:
    pass
 
vouchers_bp = Blueprint("vouchers", __name__)

BYPASS_ADMIN = False
def _admin_secret_ok(value: str) -> bool:
    # If config defines a helper, defer to it
    if hasattr(_cfg, "admin_secret_ok") and callable(getattr(_cfg, "admin_secret_ok")):
        return _cfg.admin_secret_ok(value)
    return bool(value and _ADMIN_PANEL_SECRET and value.strip() == _ADMIN_PANEL_SECRET.strip())

def _get_bearer_secret(req) -> str:
    auth = req.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return ""

def _get_admin_secret(req) -> str:
    return (
        _get_bearer_secret(req)
        or req.headers.get("X-Admin-Secret", "")
        or req.args.get("admin_secret", "")
    )

def extract_raw_init_data_from_query(request):
    qs = request.query_string  # bytes, raw
    if not qs:
        return ""
    key = b"init_data="
    idx = qs.find(key)
    if idx == -1:
        return ""
    start = idx + len(key)
    end = qs.find(b"&", start)
    raw = qs[start:] if end == -1 else qs[start:end]
    return raw.decode("utf-8", errors="strict")
 
def _user_ctx_or_preview(req, *, init_data_raw: str, verification: tuple | None = None):
    """
    Returns (ctx: dict, admin_preview: bool) or (None, False) if unauthorized.
    SAFE: will not throw if env/config is missing.
    """
    # Admin preview?
    secret = _get_admin_secret(req)
    if _admin_secret_ok(secret):
        return ({"user_id": 0, "username": "admin-preview"}, True)

    payload = _payload_for_admin_query(req)
    if payload:
        return (payload, True)
  
    ok, parsed, _ = verification if verification is not None else verify_telegram_init_data(init_data_raw)
    if ok:
        return (parsed, False)

    return (None, False)


def _legacy_user_ctx(req):
    """Best-effort fallback for legacy web clients that pass ?user_id=...&username=..."""

    try:
        user_id = req.args.get("user_id", type=int)
    except Exception:
        user_id = None

    username_hint = norm_username(
        req.args.get("username")
        or req.args.get("user")
        or req.headers.get("X-Admin-Username", "")
    )

    doc = None
    if user_id:
        doc = users_collection.find_one({"user_id": user_id})

    if not doc and username_hint:
        doc = users_collection.find_one({
            "username": {"$regex": f"^{username_hint}$", "$options": "i"}
        })

    if not doc:
        return None

    doc_username = norm_username(doc.get("username"))
    if username_hint and doc_username and username_hint != doc_username:
        return None

    username_lower = doc_username or username_hint
    if not username_lower:
        return None

    return {
        "usernameLower": username_lower,
        "legacyUserId": doc.get("user_id") or user_id or 0,
        "legacySource": "query"
    }


def _ctx_to_user(ctx: dict) -> dict:
    if not isinstance(ctx, dict):
        return {"usernameLower": "", "source": "", "userId": ""}

    result = {"usernameLower": "", "source": "", "userId": ""}
  
    if "usernameLower" in ctx:
        result["usernameLower"] = ctx.get("usernameLower", "") or ""
        result["source"] = ctx.get("legacySource") or ctx.get("adminSource") or ctx.get("source") or ""
        user_id = ctx.get("userId") or ctx.get("user_id") or ctx.get("legacyUserId")
        if user_id is not None:
            result["userId"] = str(user_id).strip()
        return result
     
    raw_user = ctx.get("user")
    if isinstance(raw_user, str):
        try:
            user_json = json.loads(raw_user)
        except Exception:
            user_json = {}
    elif isinstance(raw_user, dict):
        user_json = raw_user
    else:
        user_json = {}

    if not isinstance(user_json, dict):
        user_json = {}

    user_id = str(user_json.get("id") or "").strip()
    username_lower = norm_username(user_json.get("username", ""))

    result["usernameLower"] = username_lower
    result["source"] = "telegram"
    result["userId"] = user_id
    return result
 
def now_utc():
    return datetime.now(timezone.utc)

def now_kl():
    return datetime.now(KL_TZ)

def ensure_voucher_indexes():
    db.drops.create_index([("startsAt", ASCENDING)])
    db.drops.create_index([("endsAt", ASCENDING)])
    db.drops.create_index([("priority", DESCENDING)])
    db.drops.create_index([("status", ASCENDING)])
    db.vouchers.create_index([("code", ASCENDING)], unique=True)
    db.vouchers.create_index([("dropId", ASCENDING), ("type", ASCENDING), ("status", ASCENDING)])
    db.vouchers.create_index([("dropId", ASCENDING), ("usernameLower", ASCENDING)])
    db.vouchers.create_index([("dropId", ASCENDING), ("claimedBy", ASCENDING)])
    db.vouchers.create_index(
        [("dropId", ASCENDING), ("claimedByKey", ASCENDING)],
        name="claimed_by_key_per_drop",
        partialFilterExpression={"status": "claimed", "claimedByKey": {"$exists": True}},
    ) 
    # Prevent multiple rows for the same user in a personalised drop
    try:
        db.vouchers.drop_index("uniq_personalised_assignment")
    except OperationFailure:
        pass
     
    db.vouchers.create_index(
        [("type", ASCENDING), ("dropId", ASCENDING), ("usernameLower", ASCENDING)],
        unique=True,
        name="uniq_personalised_assignment",
        partialFilterExpression={"type": "personalised"}
    )
    new_joiner_claims_col.create_index([("uid", ASCENDING)], unique=True)
    claim_rate_limits_col.create_index([("key", ASCENDING)], unique=True)
    claim_rate_limits_col.create_index([("expiresAt", ASCENDING)], expireAfterSeconds=0)
    claim_rate_limits_col.create_index([("scope", ASCENDING), ("ip", ASCENDING), ("day", ASCENDING)])
    profile_photo_cache_col.create_index([("uid", ASCENDING)], unique=True)
    profile_photo_cache_col.create_index([("expiresAt", ASCENDING)], expireAfterSeconds=0)
    welcome_tickets_col.create_index([("uid", ASCENDING)], unique=True)
    welcome_tickets_col.create_index([("cleanup_at", ASCENDING)], expireAfterSeconds=0)
    welcome_eligibility_col.create_index([("uid", ASCENDING)], unique=True)
    subscription_cache_col.create_index([("expireAt", ASCENDING)], expireAfterSeconds=0)
 
def parse_kl_local(dt_str: str):
    """Parse 'YYYY-MM-DD HH:MM:SS' in Kuala Lumpur local time to UTC (aware)."""
    if not dt_str:
        raise ValueError("datetime string required")
    dt_local = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KL_TZ)
    return dt_local.astimezone(timezone.utc)

def _as_aware_utc(dt):
    """Return a timezone-aware UTC datetime from dt (datetime or ISO string)."""
    if dt is None:
        return None
    # Already datetime?
    if isinstance(dt, datetime):
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            # Treat naive as UTC (legacy docs)
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    # ISO string (best-effort)
    if isinstance(dt, str):
        try:
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None

def _as_aware_kl(dt):
    """Return a timezone-aware Kuala Lumpur datetime from dt (datetime or ISO string)."""
    aware = _as_aware_utc(dt)
    if not aware:
        return None
    return aware.astimezone(KL_TZ)

def _isoformat_kl(dt):
    """Return an ISO string in Kuala Lumpur time for the given datetime/ISO value."""
    aware = _as_aware_utc(dt)
    if not aware:
        return None
    return aware.astimezone(KL_TZ).isoformat()
 
def norm_username(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()

def norm_uname(s):
    if not s:
        return ""
    s = s.strip()
    if s.startswith("@"):
        s = s[1:]
    return s.lower()

def _welcome_window_for_user(uid: int | None, *, ref: datetime | None = None, user_doc: dict | None = None):
    if uid is None:
        return None
    user_doc = user_doc or users_collection.find_one({"user_id": uid}, {"joined_main_at": 1})
    joined_main_at = (user_doc or {}).get("joined_main_at")
    if not joined_main_at:
        return None
    joined_main_kl = _as_aware_kl(joined_main_at)
    if not joined_main_kl:
        return None
    ref_kl = (ref or now_kl()).astimezone(KL_TZ)
    if joined_main_kl < (ref_kl - timedelta(days=WELCOME_WINDOW_DAYS)):
        return None
    return {
        "joined_main_at": joined_main_kl,
        "eligible_until": joined_main_kl + timedelta(days=WELCOME_WINDOW_DAYS),
    }

def _get_client_ip(req=None) -> str:
    req = req or request
    if not req:
        return ""
    forwarded = req.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return req.remote_addr or ""

def _guess_username(req, body=None) -> str:
    body = body or {}

    candidates = [
        body.get("username"),
        body.get("user"),
        req.args.get("username"),
        req.args.get("user"),
        req.headers.get("X-User-Username"),
        req.headers.get("X-Admin-Username"),
    ]

    for candidate in candidates:
        normalized = norm_username(candidate)
        if normalized:
            return normalized
    return ""

def _guess_user_id(req, body=None) -> str:
    body = body or {}

    keys = ("user_id", "userId", "id")
    for key in keys:
        value = body.get(key)
        if value:
            return str(value)

    for key in keys:
        value = req.args.get(key)
        if value:
            return str(value)

    header_candidates = [
        req.headers.get("X-Admin-User-Id"),
        req.headers.get("X-User-Id"),
    ]
    for value in header_candidates:
        if value:
            return str(value)

    return ""


def load_user_context(*, uid=None, username: str | None = None, username_lower: str | None = None) -> dict:
    """
    Load user context from MongoDB using either user_id or usernameLower.
    Returns a dict with safe default values when no record is found.
    """

    try:
        uid_int = int(uid) if uid is not None else None
    except (TypeError, ValueError):
        uid_int = None

    username_lower = norm_username(username_lower or username or "")

    doc = None
    if uid_int is not None:
        doc = users_collection.find_one({"user_id": uid_int})

    if not doc and username_lower:
        doc = users_collection.find_one({"usernameLower": username_lower})
        if not doc:
            doc = users_collection.find_one({"username": {"$regex": f"^{username_lower}$", "$options": "i"}})

    ctx = {
        "user_id": uid_int,
        "usernameLower": username_lower,
        "status": "",
        "region": "",
        "monthly_xp": 0,
        "weekly_xp": 0,
        "xp": 0,
    }

    if doc:
        ctx.update({
            "user_id": doc.get("user_id", ctx["user_id"]),
            "usernameLower": norm_username(doc.get("usernameLower") or doc.get("username") or ctx["usernameLower"]),
            "status": doc.get("status", ctx["status"]),
            "region": doc.get("region", ctx["region"]),
            "monthly_xp": doc.get("monthly_xp", ctx["monthly_xp"]),
            "weekly_xp": doc.get("weekly_xp", ctx["weekly_xp"]),
            "xp": doc.get("xp", ctx["xp"]),
        })

    return ctx

def _drop_audience_type(drop: dict) -> str:
    """Return drop audience type: public|vip1|new_joiner|new_joiner_48h (case-insensitive)."""
    if not isinstance(drop, dict):
        return "public"

    audience = drop.get("audience")
    if isinstance(audience, str):
        atype = audience.strip().lower()
        if atype:
            return atype
    elif isinstance(audience, dict):
        atype = (audience.get("type") or audience.get("audience") or "").strip().lower()
        if atype:
            return atype

    wl = drop.get("whitelistUsernames") or []
    for item in wl:
        if isinstance(item, str):
            lowered = item.strip().lower()
            if lowered in ("new_joiner_48h", "new joiner 48h", "newjoiner48h"):
                return "new_joiner_48h"
            if lowered in ("new_joiner", "new joiner", "newjoiner"):
                return "new_joiner"

    return "public"

def _is_new_joiner_audience(audience_type: str) -> bool:
    return audience_type in ("new_joiner", "new_joiner_48h")

def _is_pool_drop(drop: dict, audience_type: str | None = None) -> bool:
    if not isinstance(drop, dict):
        return False
    atype = (audience_type or _drop_audience_type(drop) or "").strip().lower()
    if atype == "public_pool":
        return True
    category = drop.get("category")
    if isinstance(category, str) and category.strip().lower() == "pool":
        return True
    return bool(drop.get("is_pool") is True)

WELCOME_BONUS_RETENTION_MESSAGE = (
    "🎉 Welcome Bonus unlocked!\n"
    "More exclusive vouchers and surprise drops are released in our channel.\n"
    "Stay subscribed so you won’t miss the next one."
)

POOL_VOUCHER_RETENTION_MESSAGE = (
    "🎁 Voucher claimed successfully!\n"
    "More voucher drops are coming soon — channel subscribers only.\n"
    "Stay subscribed to catch the next drop."
)

def _append_retention_message(payload: dict, retention_message: str) -> None:
    if not retention_message:
        return
    existing = (payload.get("message") or "").strip()
    if retention_message in existing:
        return
    if existing:
        payload["message"] = f"{existing}\n{retention_message}"
    else:
        payload["message"] = retention_message

def _check_claim_rate_limit(uid: int) -> bool:
    if uid is None:
        return True
    now = time.monotonic()
    expires_at = _claim_rate_limit_cache.get(uid)
    if expires_at and expires_at > now:
        return False
    _claim_rate_limit_cache[uid] = now + CLAIM_RATE_LIMIT_SECONDS
    if len(_claim_rate_limit_cache) > 5000:
        for key, exp in list(_claim_rate_limit_cache.items()):
            if exp <= now:
                _claim_rate_limit_cache.pop(key, None)
    return True

def _rate_limit_key(prefix: str, *parts):
    return ":".join([prefix] + [str(p) for p in parts if p is not None])


def _check_new_joiner_rate_limits(*, uid: int, ip: str, now: datetime | None = None):
    """
    Enforce:
      - per-UID: max 3 attempts per minute
      - per-IP distinct UIDs: max 10 per day
      (per-IP successful claims handled post-success)
    """
    now = now or now_kl()
    minute_key = now.strftime("%Y%m%d%H%M")
    day_key = now.strftime("%Y%m%d")

    # per-UID attempts per minute
    key_uid_min = _rate_limit_key("uid", uid, "min", minute_key)
    doc = claim_rate_limits_col.find_one_and_update(
        {"key": key_uid_min},
        {
            "$setOnInsert": {
                "scope": "uid_min",
                "uid": uid,
                "minute": minute_key,
                "expiresAt": now + timedelta(hours=2),
            },
            "$inc": {"count": 1},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if (doc or {}).get("count", 0) > 3:
        current_app.logger.info("[rate_limit] block uid_min uid=%s minute=%s count=%s", uid, minute_key, doc.get("count"))
        return False, "uid_attempts_per_minute"

    # per-IP distinct UIDs per day
    ip = ip or "unknown"
    key_ip_uid = _rate_limit_key("ip_uid", ip, uid, "day", day_key)
    claim_rate_limits_col.find_one_and_update(
        {"key": key_ip_uid},
        {
            "$setOnInsert": {
                "scope": "ip_uid",
                "ip": ip,
                "uid": uid,
                "day": day_key,
                "expiresAt": now + timedelta(days=2),
            },
            "$inc": {"count": 1},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    distinct = claim_rate_limits_col.count_documents({"scope": "ip_uid", "ip": ip, "day": day_key})
    if distinct > 10:
        current_app.logger.info("[rate_limit] block ip_uid ip=%s day=%s distinct=%s", ip, day_key, distinct)
        return False, "ip_uidset_per_day"

    return True, None


def _increment_ip_claim_success(*, ip: str, now: datetime | None = None):
    now = now or now_utc()
    day_key = now.strftime("%Y%m%d")
    ip = ip or "unknown"
    key_ip_day = _rate_limit_key("ip", ip, "day", day_key)
    doc = claim_rate_limits_col.find_one_and_update(
        {"key": key_ip_day},
        {
            "$setOnInsert": {
                "scope": "ip_claim",
                "ip": ip,
                "day": day_key,
                "expiresAt": now + timedelta(days=2),
            },
            "$inc": {"count": 1},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return (doc or {}).get("count", 0)


def _has_profile_photo(uid: int, *, force_refresh: bool = False):
    if uid is None:
        return False

    now = now_kl()
    if not force_refresh:
        try:
            cached = profile_photo_cache_col.find_one({"uid": uid})
            exp = (cached or {}).get("expiresAt")
            exp_aware = _as_aware_kl(exp) or _as_aware_utc(exp)
            if exp_aware and exp_aware > now:
                has_photo = bool(cached.get("hasPhoto"))
                current_app.logger.info(
                    "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=%s source=cache",
                    uid,
                    str(has_photo).lower(),
                )
                return has_photo
             
        except Exception as e:
            current_app.logger.warning("[tg] profile photo cache read/compare failed uid=%s err=%s", uid, e)

    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        current_app.logger.warning("[tg] missing BOT_TOKEN for profile photo check")
        current_app.logger.info(
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUserProfilePhotos",
            params={"user_id": uid, "limit": 1},
            timeout=5,
        )
    except requests.RequestException as e:
        current_app.logger.warning("[tg] getUserProfilePhotos network error uid=%s err=%s", uid, e)
        current_app.logger.info(
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    if resp.status_code != 200:
        current_app.logger.warning("[tg] getUserProfilePhotos http status=%s uid=%s", resp.status_code, uid)
        current_app.logger.info(
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    try:
        data = resp.json()
    except ValueError:
        current_app.logger.warning("[tg] getUserProfilePhotos bad json uid=%s", uid)
        current_app.logger.info(
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    if not data.get("ok"):
        current_app.logger.warning("[tg] getUserProfilePhotos not ok uid=%s err=%s", uid, data)
        current_app.logger.info(
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    total = data.get("result", {}).get("total_count", 0)
    has_photo = bool(total and total > 0)
    profile_photo_cache_col.update_one(
        {"uid": uid},
        {
            "$set": {
                "hasPhoto": has_photo,
                "checkedAt": now,
                "expiresAt": now + timedelta(seconds=PROFILE_PHOTO_CACHE_TTL_SECONDS),
            },
            "$setOnInsert": {"uid": uid},
        },
        upsert=True,
    )
    current_app.logger.info(
        "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=%s source=telegram",
        uid,
        str(has_photo).lower(),
    ) 
    return has_photo

ALLOWED_CHANNEL_STATUSES = {"member", "administrator", "creator"}
SUB_CHECK_TTL_SECONDS = int(os.getenv("SUB_CHECK_TTL_SECONDS", "120"))

def is_existing_user(uid: int) -> bool:
    if uid is None:
        return False
    return bool(db.users.find_one({"user_id": uid}, {"_id": 1}))

def _get_welcome_eligibility(uid: int) -> dict | None:
    if uid is None:
        return None
    return welcome_eligibility_col.find_one({"user_id": uid})

def ensure_welcome_eligibility(uid: int, *, users_exists: bool | None = None) -> dict | None:
    if not uid:
        current_app.logger.warning("[WELCOME][ELIGIBILITY] skip upsert: missing uid")
        return None
    window = _welcome_window_for_user(uid)
    if not window:
        return None
    now = now_kl()
    eligible_until = window["eligible_until"]
    try:
        welcome_eligibility_col.update_one(
            {"user_id": uid},
            {
                "$setOnInsert": {
                    "user_id": uid,
                    "uid": uid,
                    "first_seen_at": now,
                    "claimed": False,
                    "claimed_at": None,
                },
                "$set": {"eligible_until": eligible_until},
            },
            upsert=True,
        )
    except Exception:
        current_app.logger.exception("[visible][WELCOME_ELIGIBILITY] write failed", extra={"uid": uid})
        return None
    return welcome_eligibility_col.find_one({"user_id": uid})

def _welcome_eligible_now(uid: int | None, doc: dict | None, *, ref: datetime | None = None, user_doc: dict | None = None) -> bool:
    if uid is None or not doc:
        return False
    if doc.get("claimed"):
        return False
    window = _welcome_window_for_user(uid, ref=ref, user_doc=user_doc)
    if not window:
        return False
    ref_kl = (ref or now_kl()).astimezone(KL_TZ)
    return ref_kl <= window["eligible_until"]

def _official_channel_identifier():
    if OFFICIAL_CHANNEL_ID is not None:
        return OFFICIAL_CHANNEL_ID
    if OFFICIAL_CHANNEL_USERNAME:
        return OFFICIAL_CHANNEL_USERNAME
    return None

def _subscription_cache_key(uid: int) -> str:
    return f"sub:{uid}"

def get_cached_subscription(uid: int) -> bool | None:
    if uid is None:
        return None
    try:
        doc = subscription_cache_col.find_one(
            {"_id": _subscription_cache_key(uid)},
            {"subscribed": 1, "expireAt": 1},
        )
    except Exception:
        return None
    if not doc or not doc.get("subscribed"):
        return None
    expire_at = _as_aware_utc(doc.get("expireAt"))
    if not expire_at:
        return None
    if now_utc() >= expire_at:
        return None
    return True


def set_cached_subscription_true(uid: int, ttl_seconds: int) -> None:
    if uid is None:
        return
    now = now_utc()
    expire_at = now + timedelta(seconds=ttl_seconds)
  
    try:
        subscription_cache_col.update_one(
            {"_id": _subscription_cache_key(uid)},
            {
                "$set": {
                    "user_id": uid,
                    "subscribed": True,
                    "expireAt": expire_at,
                    "updated_at": now,
                }
            },
            upsert=True,
        )
    except Exception:
        pass

def check_channel_subscribed(uid: int) -> bool:
    if uid is None:
        return False

    cached = get_cached_subscription(uid)
    if cached:
        current_app.logger.info("[SUB_CACHE][HIT] uid=%s", uid)     
        return True
    current_app.logger.info("[SUB_CACHE][MISS] uid=%s", uid)
 
    chat_id = _official_channel_identifier()
    token = os.environ.get("BOT_TOKEN", "")
    if not chat_id or not token:
        current_app.logger.info("[welcome] gate_fail uid=%s reason=channel_unset", uid)
        return False

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": chat_id, "user_id": uid},
            timeout=5,
        )
    except requests.RequestException as e:
        current_app.logger.warning("[welcome] gate_fail uid=%s reason=channel_check_error err=%s", uid, e)
        return False

    if resp.status_code != 200:
        current_app.logger.info("[welcome] gate_fail uid=%s reason=channel_http_%s", uid, resp.status_code)
        return False

    try:
        data = resp.json()
    except ValueError:
        current_app.logger.info("[welcome] gate_fail uid=%s reason=channel_bad_json", uid)
        return False

    if not data.get("ok"):
        current_app.logger.info("[welcome] gate_fail uid=%s reason=channel_not_ok err=%s", uid, data)
        return False

    status = (data.get("result") or {}).get("status")
    is_subscribed = status in ALLOWED_CHANNEL_STATUSES
    if is_subscribed:
        set_cached_subscription_true(uid, SUB_CHECK_TTL_SECONDS)
        current_app.logger.info("[SUB_CACHE][SET] uid=%s ttl=%s", uid, SUB_CHECK_TTL_SECONDS)
        return True

    return False


def check_has_profile_photo(uid: int) -> bool:
    return _has_profile_photo(uid)


def check_username_present(tg_user: dict) -> bool:
    if not isinstance(tg_user, dict):
        return False
    return bool(norm_uname(tg_user.get("username")))


def get_or_issue_welcome_ticket(uid: int):
    if uid is None:
        return None

    eligibility = ensure_welcome_eligibility(uid)
    if not eligibility:
        current_app.logger.info("[welcome] eligibility_missing uid=%s deny", uid)
        return None

    now = now_kl()
    expires_at = _as_aware_kl(eligibility.get("eligible_until"))
    if not expires_at:
        current_app.logger.info("[welcome] eligibility_invalid uid=%s deny", uid)
        return None
    if eligibility.get("claimed"):
        current_app.logger.info("[welcome] eligibility_claimed uid=%s deny", uid)
        return None
    if now > expires_at:
        current_app.logger.info("[welcome] eligibility_expired uid=%s deny", uid)
        return None

    cleanup_at = expires_at + timedelta(days=30)

    try:
        ticket = welcome_tickets_col.find_one_and_update(
            {"uid": uid},
            {
                "$set": {"cleanup_at": cleanup_at},
                "$setOnInsert": {
                    "uid": uid,
                    "issued_at": now,
                    "expires_at": expires_at,
                    "status": "active",
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except Exception:
        current_app.logger.exception("[visible][WELCOME_TICKET] write failed", extra={"uid": uid})
        return None

    issued_at = _as_aware_utc(ticket.get("issued_at"))
    just_created = False
    if issued_at:
        try:
            just_created = abs((issued_at - now).total_seconds()) < 2
        except Exception:
            just_created = False

    ticket_expires_at = _as_aware_kl(ticket.get("expires_at")) or expires_at
    if ticket_expires_at and now > ticket_expires_at and ticket.get("status") not in ("expired", "claimed"):
        welcome_tickets_col.update_one(
            {"uid": uid},
            {"$set": {"status": "expired", "reason_last_fail": "expired"}},
        )
        ticket["status"] = "expired"
        ticket["expires_at"] = ticket_expires_at

    if just_created:
        current_app.logger.info("[welcome] ticket_issued uid=%s expires_at=%s", uid, expires_at.isoformat())
    else:
        current_app.logger.info("[welcome] ticket_exists uid=%s status=%s", uid, ticket.get("status"))
    return ticket


def is_drop_allowed(drop: dict, tg_uid, tg_uname_lower: str, user_ctx: dict | None) -> bool:
    audience = drop.get("audience") or {}
    if not audience:
        return True

    user_ctx = user_ctx or {}
    drop_id = str(drop.get("_id") or drop.get("dropId") or "")

    uid = tg_uid
    if uid is None:
        try:
            uid = int(user_ctx.get("user_id"))
        except Exception:
            uid = None

    denylist = set()
    for raw in audience.get("denylist_user_ids", []) or []:
        try:
            denylist.add(int(raw))
        except (TypeError, ValueError):
            continue

    if uid is not None and uid in denylist:
        print(f"[audience] blocked drop_id={drop_id} uid={uid} reason=denylist_user_ids")
        return False

    allowlist = set()
    for raw in audience.get("allowlist_user_ids", []) or []:
        try:
            allowlist.add(int(raw))
        except (TypeError, ValueError):
            continue

    if uid is not None and uid in allowlist:
        return True

    statuses = audience.get("statuses") or []
    user_status = user_ctx.get("status") or ""
    if statuses and user_status not in statuses:
        print(f"[audience] blocked drop_id={drop_id} uid={uid} reason=statuses")
        return False

    regions = audience.get("regions") or []
    user_region = user_ctx.get("region") or ""
    if regions and user_region not in regions:
        print(f"[audience] blocked drop_id={drop_id} uid={uid} reason=regions")
        return False

    min_monthly_xp = audience.get("min_monthly_xp")
    try:
        min_monthly_xp_int = int(min_monthly_xp) if min_monthly_xp is not None else None
    except (TypeError, ValueError):
        min_monthly_xp_int = None

    if min_monthly_xp_int is not None:
        monthly_xp = user_ctx.get("monthly_xp") or 0
        try:
            monthly_xp = int(monthly_xp)
        except (TypeError, ValueError):
            monthly_xp = 0

        if monthly_xp < min_monthly_xp_int:
            print(f"[audience] blocked drop_id={drop_id} uid={uid} reason=min_monthly_xp")
            return False

    return True

# Eligibility examples for quick testing:
#   {"eligibility": {"mode": "public"}}
#   {"eligibility": {"mode": "tier", "allow": ["VIP1", "VIP2"]}, "audience": {"regions": ["Thailand"]}}
#   {"eligibility": {"mode": "user_id", "allow": [12345]}}
#   {"eligibility": {"mode": "admin_only"}}
def is_user_eligible_for_drop(user_doc: dict, tg_user: dict, drop: dict) -> bool:
    tg_user = tg_user or {}
    drop = drop or {}

    try:
        uid = int(tg_user.get("id"))
    except Exception:
        uid = None

    drop_id = str(drop.get("_id") or drop.get("dropId") or "")

    audience = drop.get("audience") or {}
    eligibility = drop.get("eligibility") or {"mode": "public"}
    mode = eligibility.get("mode") or "public"
    allow = eligibility.get("allow") or []

    user_region = (user_doc or {}).get("region")
    user_status = (user_doc or {}).get("status") or ""

    def _log(result: bool, reason: str):
        print(
            f"[elig] drop={drop_id} mode={mode} uid={uid} "
            f"region={user_region} status={user_status} => "
            f"{'allow' if result else 'deny'} reason={reason}"
        )

    # Admin-only preview drops
    if mode == "admin_only":
        if uid is None or uid not in _ADMIN_USER_IDS:
            _log(False, "admin_only")
            return False

    # Region filter (applies to all modes when specified)
    regions = audience.get("regions") or []
    if regions:
        if not user_region:
            _log(False, "region_missing")
            return False
        if user_region not in regions:
            _log(False, "region_mismatch")
            return False

    if mode == "tier":
        if not allow or user_status not in allow:
            _log(False, "tier_mismatch")
            return False
    elif mode == "user_id":
        allowed_ids = set()
        for raw in allow:
            try:
                allowed_ids.add(int(raw))
            except (TypeError, ValueError):
                continue
        if not allowed_ids or uid not in allowed_ids:
            _log(False, "user_not_whitelisted")
            return False
    elif mode == "admin_only":
        pass  # already checked admin set
    else:
        mode = "public"

    _log(True, "ok")
    return True

def _extract_admin_secret() -> str:
    """Return the admin secret supplied via headers or query params."""

    header_secret = request.headers.get("X-Admin-Secret")
    if header_secret:
        return header_secret.strip()

    auth_header = request.headers.get("Authorization", "")
    if isinstance(auth_header, str):
        try:
            scheme, value = auth_header.strip().split(" ", 1)
        except ValueError:
            pass
        else:
            if scheme.lower() == "bearer" and value.strip():
                return value.strip()

    query_secret = request.args.get("admin_secret")
    if query_secret:
        return query_secret.strip()

    return ""


def _normalize_codes(codes):
    """Strip whitespace/punctuation and drop duplicates while preserving order."""

    cleaned = []
    seen = set()

    for raw in codes or []:
        code = str(raw or "").strip().strip(",")
        if not code or code in seen:
            continue
        seen.add(code)
        cleaned.append(code)

    return cleaned


def _has_valid_admin_secret() -> bool:
    expected = (_ADMIN_PANEL_SECRET or "").strip()
    if not expected:
        return False

    provided = _extract_admin_secret()
    if not provided:
        return False
    try:
        return hmac.compare_digest(str(provided), expected)
    except Exception:
        return False


def _payload_from_admin_secret() -> dict:
    username_hint = (request.headers.get("X-Admin-Username")
                     or request.args.get("username")
                     or "")
    user_id_hint = (request.headers.get("X-Admin-User-Id")
                    or request.args.get("user_id"))

    payload = {
        "usernameLower": norm_username(username_hint) or "admin_secret",
        "adminSource": "secret"
    }

    if user_id_hint:
        try:
            payload["id"] = int(user_id_hint)
        except (TypeError, ValueError):
            pass

    return payload
    
def parse_init_data(raw: str) -> dict:
    pairs = urllib.parse.parse_qsl(raw, keep_blank_values=True)
    return {k: v for k, v in pairs}

def verify_telegram_init_data(init_data_raw: str):
    import os, hmac, hashlib, time, json, urllib.parse

    print("[initdata] verifier=canonical_parse_qsl")

    raw = init_data_raw or ""
    decoded = urllib.parse.unquote_plus(raw)
    print(f"[initdata] raw_prefix={raw[:40]}")
    print(f"[initdata] decoded_prefix={decoded[:40]}")
 
    # 1. Parse & decode ONCE (Telegram spec compliant)
    pairs = urllib.parse.parse_qsl(
        decoded,
        keep_blank_values=True,
        strict_parsing=False,
    )

    data = dict(pairs)
    provided_hash = data.get("hash", "").lower()
         
    print(
        f"[initdata] has_user={'user' in data} "
        f"has_auth_date={'auth_date' in data} "
        f"has_hash={'hash' in data}"
    )
 
    if not provided_hash or len(provided_hash) != 64:
        return False, {}, "invalid_hash"

    # 2. Build data_check_string (sorted, hash excluded)
    data_check_string = "\n".join(
        f"{k}={v}"
        for k, v in sorted(pairs)
        if k != "hash"
    )
 
    dcs_sha = hashlib.sha256(data_check_string.encode()).hexdigest()[:8]
 
    # 3. Compute HMAC
    token = os.environ.get("BOT_TOKEN", "")
    secret = hmac.new(
        b"WebAppData",
        token.encode(),
        hashlib.sha256
    ).digest()
 
    computed = hmac.new(
        secret,
        data_check_string.encode(),
        hashlib.sha256
    ).hexdigest()
 
    print(
        f"[initdata] hash_check "
        f"bot_tail={token[-4:]} "
        f"dcs_sha={dcs_sha} "
        f"provided={provided_hash[:8]} "
        f"computed={computed[:8]}"
    )
 
    if not hmac.compare_digest(computed, provided_hash):
        return False, {}, "hash_mismatch"

    # 4. Freshness check (24h)
    try:
        auth_date = int(data.get("auth_date", "0"))
        if time.time() - auth_date > 86400:
            return False, {}, "auth_date_expired"
    except Exception:
        pass
     
    # 5. Parse Telegram user JSON (already decoded)
    try:
        user = json.loads(data.get("user", "{}"))
    except Exception:
        return False, {}, "user_parse_error"
     
    if not user.get("id"):
        return False, {}, "missing_user_id"

    data["user"] = json.dumps(user)
    return True, data, "ok"
 
def _require_admin_via_query():
    payload = _payload_for_admin_query(request)
    if payload:
        return payload, None
    try:
        caller_id = request.args.get("user_id", type=int)
    except Exception:
        caller_id = None
     
    if not caller_id:
        return None, (jsonify({"status": "error", "code": "missing_user_id"}), 400)
        return None, (jsonify({"status": "error", "code": "forbidden"}), 403)

def require_admin():
    if BYPASS_ADMIN:
        print("[admin] BYPASS_ADMIN=1 — skipping admin auth")
        return {"usernameLower": "bypass_admin"}, None
     
    payload, err = _require_admin_via_query()
    if err:
        return None, err
    if payload:
        return payload, None

    # Fallback: treat as missing credentials
    return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)
 
def _is_admin_preview(init_data_raw: str) -> bool:
    # Safe best-effort: if verify fails, just return False (don’t break visible)
    if _has_valid_admin_secret():
        return True
    ok, data, _ = verify_telegram_init_data(init_data_raw)
    if not ok:
        return False
    try:
        user_json = json.loads(data.get("user", "{}"))
        if not isinstance(user_json, dict):
            user_json = {}
    except Exception:
        user_json = {}
    is_admin, _ = _is_cached_admin(user_json)
    return is_admin
    
# ---- Core visibility logic ----
def is_drop_active(doc: dict, ref: datetime) -> bool:
    starts = _as_aware_utc(doc.get("startsAt"))
    ends   = _as_aware_utc(doc.get("endsAt"))
    if not starts or not ends:
        return False
    if doc.get("status") in ("paused", "expired"):
        return False
    return starts <= ref < ends

def get_active_drops(ref: datetime):
    return list(db.drops.find({
        "status": {"$nin": ["expired", "paused"]},
        "startsAt": {"$lte": ref},
        "endsAt": {"$gt": ref}
    }))

def user_visible_drops(user: dict, ref: datetime, *, tg_user: dict | None = None):
    usernameLower = norm_username(user.get("usernameLower", ""))
    user_id = str(user.get("userId") or "").strip() if isinstance(user, dict) else ""
    claim_key = usernameLower or user_id
    pooled_claim_key = f"uid:{user_id}" if user_id else ""
    drops = get_active_drops(ref)

    ref_kl = ref.astimezone(KL_TZ) if ref.tzinfo else ref.replace(tzinfo=timezone.utc).astimezone(KL_TZ) 
    personal_cards = []
    pooled_cards = []

    raw_username = "" 
    uname = ""
    uid = None
    if isinstance(tg_user, dict):
        try:
            uid = int(tg_user["id"])
        except Exception:
            uid = None
        raw_username = tg_user.get("username") or ""
        uname = norm_uname(raw_username)
     
        if uname and not usernameLower:
            usernameLower = uname
            claim_key = claim_key or usernameLower

        if not pooled_claim_key and uid is not None:
            pooled_claim_key = f"uid:{uid}"

    ctx_uid = uid
    if ctx_uid is None:
        try:
            ctx_uid = int(user_id)
        except (TypeError, ValueError):
            ctx_uid = None

    user_ctx = load_user_context(uid=ctx_uid, username=raw_username if tg_user else usernameLower, username_lower=usernameLower or uname)

    user_doc = None
    if ctx_uid is not None:
        user_doc = users_collection.find_one({"user_id": ctx_uid})
    if not user_doc and usernameLower:
        user_doc = users_collection.find_one({"usernameLower": usernameLower})

    uid_for_new_joiner = ctx_uid if ctx_uid is not None else None
    already_new_joiner_claim = False
    if uid_for_new_joiner is not None:
        already_new_joiner_claim = bool(new_joiner_claims_col.find_one({"uid": uid_for_new_joiner}))
    welcome_eligibility = ensure_welcome_eligibility(uid_for_new_joiner) if uid_for_new_joiner is not None else None
 
    # Only hide personalised vouchers when the caller truly has no username
    allow_personalised = bool(usernameLower)
    claim_key = usernameLower or user_id
 
    logged_hidden = False
 
    for d in drops:
        drop_id = str(d["_id"])
        drop_id_variants = _drop_id_variants(d.get("_id"))
        dtype = d.get("type", "pooled")
        is_active = is_drop_active(d, ref)
        audience_type = _drop_audience_type(d)
        is_welcome_drop = _is_new_joiner_audience(audience_type)

        if not is_drop_allowed(d, ctx_uid, usernameLower or uname, user_ctx):
            continue

        if not is_user_eligible_for_drop(user_doc, tg_user or {}, d):
            continue    

        welcome_ticket = None
        if is_welcome_drop:
            if uid is None:
                continue
            if not _welcome_eligible_now(uid, welcome_eligibility, ref=ref, user_doc=user_doc):
                continue
            if already_new_joiner_claim:
                continue
            welcome_ticket = get_or_issue_welcome_ticket(uid)
            if not welcome_ticket:
                continue
            expires_at = _as_aware_kl(welcome_ticket.get("expires_at"))
            if expires_at and ref_kl > expires_at:
                welcome_tickets_col.update_one(
                    {"uid": uid},
                    {"$set": {"status": "expired", "expires_at": expires_at}},
                )
                continue
     
        base = {
            "dropId": drop_id,
            "name": d.get("name"),
            "type": dtype,
            "startsAt": _as_aware_utc(d.get("startsAt")).isoformat() if d.get("startsAt") else None,
            "endsAt": _as_aware_utc(d.get("endsAt")).isoformat() if d.get("endsAt") else None,
            "priority": d.get("priority", 100),
            "isActive": is_active,
            "userClaimed": False
        }

        if dtype in ("personalised", "personalized"):
            if not allow_personalised:
                if not logged_hidden:
                    print(f"[personalised] hidden_no_username uid={uid}")
                    logged_hidden = True
                continue
            if dtype == "personalized":
                v_uid   = d.get("assigned_to_user_id")
                v_uname = norm_uname(d.get("assigned_to_username"))
             
                if v_uid is not None:
                    eligible = (uid is not None and uid == int(v_uid))
                elif v_uname:
                    eligible = (uname != "" and uname == v_uname)
                else:
                    eligible = False

                if not eligible:
                    continue

            # user must have an unclaimed OR claimed row to show the card (claimed -> show claimed state)
            row = db.vouchers.find_one({
                "type": {"$in": ["personalised", "personalized"]},
                "dropId": {"$in": drop_id_variants},
                "usernameLower": usernameLower
            })
            if row and is_active:
                base["userClaimed"] = (row.get("status") == "claimed")
                if base["userClaimed"]:
                    base["code"] = row.get("code")
                    claimed_at = _isoformat_kl(row.get("claimedAt"))
                    if claimed_at:
                        base["claimedAt"] = claimed_at
                personal_cards.append(base)
        else:
            if not is_active:
                continue
            # Need at least one free code OR user already claimed (so they can see their code state)
            already = db.vouchers.find_one({
                "type": "pooled",
                "dropId": {"$in": drop_id_variants},
                "$or": [
                    {"claimedByKey": pooled_claim_key},
                    {"claimedBy": pooled_claim_key},
                ]
            })
            if already:
                base["userClaimed"] = True
                base["code"] = already.get("code")
                claimed_at = _isoformat_kl(already.get("claimedAt"))
                if claimed_at:
                    base["claimedAt"] = claimed_at
                if welcome_ticket:
                    expires_at = _as_aware_kl(welcome_ticket.get("expires_at"))
                    if expires_at:
                        remaining = int((expires_at - ref_kl).total_seconds())
                        base["welcomeRemainingSeconds"] = max(0, remaining)                 
                base["remainingApprox"] = max(0, db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": drop_id_variants}, "status": "free"}))
                pooled_cards.append(base)
            else:
                free_exists = db.vouchers.find_one({
                    "type": "pooled",
                    "dropId": {"$in": drop_id_variants},
                    "status": "free"
                }, projection={"_id": 1})
                if free_exists:
                    base["remainingApprox"] = max(0, db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": drop_id_variants}, "status": "free"}))
                    if welcome_ticket:
                        expires_at = _as_aware_kl(welcome_ticket.get("expires_at"))
                        if expires_at:
                            remaining = int((expires_at - ref_kl).total_seconds())
                            base["welcomeRemainingSeconds"] = max(0, remaining)                 
                    pooled_cards.append(base)

    # Sort: personalised first; then pooled by priority desc, startsAt asc
    personal_cards.sort(key=lambda x: (-x["priority"], x["startsAt"]))
    pooled_cards.sort(key=lambda x: (-x["priority"], x["startsAt"]))

    # Stacked: return all (cap optional)
    return personal_cards + pooled_cards

# ---- Claim handlers ----
def claim_personalised(drop_id: str, usernameLower: str, ref: datetime):
    drop_id_variants = _drop_id_variants(drop_id)
 
    # Return existing if already claimed
    existing = db.vouchers.find_one({
        "type": "personalised",
        "dropId": {"$in": drop_id_variants},
        "usernameLower": usernameLower,
        "status": "claimed"
    })
    if existing:
        return {"ok": True, "code": existing["code"], "claimedAt": _isoformat_kl(existing.get("claimedAt"))}

    # Claim the assigned row atomically
    doc = db.vouchers.find_one_and_update(
        {
            "type": "personalised",
            "dropId": {"$in": drop_id_variants},
            "usernameLower": usernameLower,
            "status": "unclaimed"
        },
        {
            "$set": {
                "status": "claimed",
                "claimedBy": usernameLower,
                "claimedAt": ref
            }
        },
        return_document=ReturnDocument.AFTER
    )
    if not doc:
        # Maybe there is no assignment or it was already claimed; try fetching claimed again to idempotently return
        already = db.vouchers.find_one({
            "type": "personalised",
            "dropId": {"$in": drop_id_variants},
            "usernameLower": usernameLower
        })
        if already and already.get("status") == "claimed":
            return {"ok": True, "code": already["code"], "claimedAt": _isoformat_kl(already.get("claimedAt"))}
        return {"ok": False, "err": "not_eligible"}
    return {"ok": True, "code": doc["code"], "claimedAt": _isoformat_kl(doc.get("claimedAt"))}

def claim_pooled(drop_id: str, claim_key: str, ref: datetime):
    drop_id_variants = _drop_id_variants(drop_id)
 

    # Atomically reserve a free code
    try:
        doc = db.vouchers.find_one_and_update(
            {
                "type": "pooled",
                "dropId": {"$in": drop_id_variants},
                "status": "free"
            },
            {
                "$set": {
                    "status": "claimed",
                    "claimedBy": claim_key,
                    "claimedByKey": claim_key,
                    "claimedAt": ref
                }
            },
            sort=[("_id", ASCENDING)],
            return_document=ReturnDocument.AFTER
        )
    except DuplicateKeyError:
        current_app.logger.info(
            "[CLAIM][ATOMIC] drop=%s claim_key=%s outcome=duplicate_key",
            drop_id,
            claim_key,
        )
        existing = db.vouchers.find_one({
            "type": "pooled",
            "dropId": {"$in": drop_id_variants},
            "$or": [
                {"claimedByKey": claim_key},
                {"claimedBy": claim_key},
            ]
        })
        if existing and existing.get("status") == "claimed":
            return {"ok": True, "code": existing["code"], "claimedAt": _isoformat_kl(existing.get("claimedAt"))}
        return {"ok": False, "err": "already_claimed"}
    if not doc:
        existing = db.vouchers.find_one({
            "type": "pooled",
            "dropId": {"$in": drop_id_variants},
            "$or": [
                {"claimedByKey": claim_key},
                {"claimedBy": claim_key},
            ]
        })
        if existing and existing.get("status") == "claimed":
            return {"ok": True, "code": existing["code"], "claimedAt": _isoformat_kl(existing.get("claimedAt"))}     
        return {"ok": False, "err": "sold_out"}
    return {"ok": True, "code": doc["code"], "claimedAt": _isoformat_kl(doc.get("claimedAt"))}

# ---- Public API routes ----
@vouchers_bp.route("/vouchers/visible", methods=["GET"])
def api_visible():
    """
    GET /v2/miniapp/vouchers/visible
    - Admin preview (Authorization: Bearer / X-Admin-Secret): active-only by default; pass ?all=1 for history
    - Normal users: only ACTIVE drops they’re eligible to see
    Always returns JSON (401/200/500).
    """
    ref = now_utc()
    user_id = None 
    try:
        init_data = extract_raw_init_data_from_query(request)

        print(
            "[initdata] source=query_string_raw "
            f"len={len(init_data)} prefix={init_data[:20]}"
        )

        if not init_data:
            return jsonify({"status": "error", "code": "missing_init_data"}), 400

        ok, parsed, reason = verify_telegram_init_data(init_data)
        ctx, admin_preview = _user_ctx_or_preview(
            request, init_data_raw=init_data, verification=(ok, parsed, reason)
        )

        if not ok and not admin_preview:
            return jsonify({"code": "auth_failed", "why": str(reason)}), 401
     
        if not admin_preview and not ctx:
            return jsonify({"code": "auth_failed", "why": "missing_or_invalid_init_data"}), 401

        # ---------- Admin preview ----------
        if admin_preview:
            show_all = request.args.get("all") in ("1", "true", "yes")
            q = {}
            if not show_all:
                q = {
                    "status": {"$nin": ["expired", "paused"]},
                    "startsAt": {"$lte": ref},
                    "endsAt": {"$gt": ref},
                }

            items = []
            for d in db.drops.find(q).sort([("priority", DESCENDING), ("startsAt", ASCENDING)]):
                drop_id = str(d["_id"])
                base = {
                    "dropId": drop_id,
                    "name": d.get("name"),
                    "type": d.get("type", "pooled"),
                    "startsAt": _as_aware_utc(d.get("startsAt")).isoformat() if d.get("startsAt") else None,
                    "endsAt": _as_aware_utc(d.get("endsAt")).isoformat() if d.get("endsAt") else None,
                    "priority": d.get("priority", 100),
                    "status": d.get("status", "upcoming"),
                    "isActive": is_drop_active(d, ref),
                    "userClaimed": False,
                    "adminPreview": True,
                }
                if base["type"] == "pooled":
                    free = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"})
                    total = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id})
                    base["remainingApprox"] = free
                    base["codesTotal"] = total
                items.append(base)

            return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": items}), 200

        # ---------- Normal user flow ----------
        user = {}
        tg_user = {}
     
        if ctx:
            user = _ctx_to_user(ctx)

            raw_user = ctx.get("user") if isinstance(ctx, dict) else None
            if isinstance(raw_user, str):
                try:
                    tg_user = json.loads(raw_user)
                except Exception:
                    tg_user = {}
            elif isinstance(raw_user, dict):
                tg_user = raw_user
     
        if user.get("source") == "telegram":
            user_id = (user.get("userId") or "").strip()
            if not user_id:
                return jsonify({"code": "auth_failed", "why": "missing_or_invalid_init_data"}), 401
        elif not user.get("usernameLower"):
            username = _guess_username(request)
            if not username:
                return jsonify({"code": "auth_failed", "why": "missing_or_invalid_init_data"}), 401
            user = {"usernameLower": username, "source": "fallback"}

        uid = None
        if user_id and isinstance(tg_user, dict):
            try:
                uid = int(tg_user.get("id"))
            except Exception:
                uid = None
        users_exists = bool(users_collection.find_one({"user_id": uid}, {"_id": 1})) if uid is not None else False
        welcome_doc = None
        user_doc = None  
        created = False
        if user_id and uid is not None:
            user_doc = users_collection.find_one({"user_id": uid}, {"joined_main_at": 1})
            if not (user_doc or {}).get("joined_main_at"):
                current_app.logger.info(
                    "[WELCOME][JOIN_BACKFILL_DISABLED] uid=%s joined_main_at_missing",
                    uid,
                )         
            existing = welcome_eligibility_col.find_one({"uid": uid})
            welcome_doc = ensure_welcome_eligibility(uid)
            created = bool(not existing and welcome_doc)
            eligible_until = _as_aware_utc((welcome_doc or {}).get("eligible_until"))
            current_app.logger.info(
                "[WELCOME] uid=%s created=%s eligible_until=%s",
                uid,
                str(created).lower(),
                eligible_until.isoformat() if eligible_until else None,
            )

        welcome_eligible = _welcome_eligible_now(uid, welcome_doc, ref=ref, user_doc=user_doc)
        current_app.logger.info(
            "[WELCOME] visible uid=%s ok_init=%s welcome_eligible=%s claimed=%s until=%s joined_main_at=%s",
            uid,
            str(ok).lower(),
            str(welcome_eligible).lower(),
            str(bool((welcome_doc or {}).get("claimed"))).lower() if welcome_doc else "false",
            _as_aware_utc((welcome_doc or {}).get("eligible_until")).isoformat() if welcome_doc else None,
            _isoformat_kl((user_doc or {}).get("joined_main_at")),         
        )
     
        drops = user_visible_drops(user, ref, tg_user=tg_user)
        return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": drops}), 200

    except Exception:
        current_app.logger.exception("[visible] unhandled", extra={"user_id": user_id})
        raise

class AlreadyClaimed(Exception):
    pass

class NoCodesLeft(Exception):
    pass

class NotEligible(Exception):
    pass
 
def claim_voucher_for_user(*, user_id: str, drop_id: str, username: str) -> dict:
    """
    Returns {"code": "...", "claimedAt": "..."} on success.
    Raises:
      - AlreadyClaimed
      - NoCodesLeft
      - NotEligible
    """
    user_doc = None
    user_id_str = str(user_id or "").strip()
 
    try:
        uid_int = int(user_id_str)
    except (TypeError, ValueError):
        uid_int = None

    if uid_int is not None:
        user_doc = users_collection.find_one({"user_id": uid_int}, {"restrictions": 1})

    if user_doc and user_doc.get("restrictions", {}).get("no_campaign"):
        raise NotEligible("not_eligible")
 
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        raise NotEligible("drop_not_found")

    dtype = drop.get("type", "pooled")
    usernameLower = norm_username(username)
    claim_key = f"uid:{user_id_str}"
    ref = now_utc()

    if dtype in ("personalised", "personalized"):
        if not usernameLower:
            raise NotEligible("not_eligible")     
        res = claim_personalised(drop_id=drop_id, usernameLower=usernameLower, ref=ref)
        if res.get("ok"):
            return {"code": res["code"], "claimedAt": res["claimedAt"]}
        if res.get("err") == "not_eligible":
            raise NotEligible("not_eligible")
        raise AlreadyClaimed("already_claimed")

    # pooled
    claim_key = f"uid:{user_id_str}" 
    res = claim_pooled(drop_id=drop_id, claim_key=claim_key, ref=ref)
    if res.get("ok"):
        return {"code": res["code"], "claimedAt": res["claimedAt"]}
    if res.get("err") == "already_claimed":
        raise AlreadyClaimed("already_claimed")     
    if res.get("err") == "sold_out":
        raise NoCodesLeft("sold_out")
    raise NotEligible("not_eligible")


@vouchers_bp.route("/vouchers/claim", methods=["POST"])
def api_claim():
    # Accept both header names + query param
    body = request.get_json(silent=True) or {}
 
    drop_id = body.get("dropId") or body.get("drop_id")

    init_data = extract_raw_init_data_from_query(request)

    print(
        "[initdata] source=query_string_raw "
        f"len={len(init_data)} prefix={init_data[:20]}"
    )

    if not init_data:
        return jsonify({"status": "error", "code": "missing_init_data"}), 400

    ok, data, why = verify_telegram_init_data(init_data)
 
    # Admin preview (for Postman/admin panel testing)
    admin_secret = (
        request.args.get("admin_secret")
        or request.headers.get("X-Admin-Secret")
        or body.get("admin_secret")
    )
    if (not ok) and _admin_secret_ok(admin_secret):
        data = {
            "user": json.dumps({
                "id": int(os.environ.get("PREVIEW_USER_ID", "999")),
                "username": os.environ.get("PREVIEW_USERNAME", "admin_preview"),
            })
        }
        ok, why = True, "ok"

    if not ok:
        return jsonify({"status": "error", "code": "auth_failed", "why": str(why)}), 401
         
    # Parse Telegram user
    try:
        user_raw = json.loads(data.get("user", "{}"))
    except Exception:
        user_raw = {}
         
    tg_user = user_raw

    user_id = str(user_raw.get("id") or "").strip()
    username = user_raw.get("username") or ""
 
    if not user_id:
        return jsonify({"status": "error", "code": "auth_failed", "why": "missing_user_id"}), 401
     
    try:
        uid = int(tg_user["id"])
    except Exception:
        uid = None
    uname = norm_uname(tg_user.get("username"))

    if uid is not None and not _check_claim_rate_limit(uid):
        current_app.logger.info("[CLAIM][RL] uid=%s", uid)
        return jsonify({"status": "error", "code": "rate_limited", "reason": "rate_limited"}), 429

    drop = db.drops.find_one({"_id": _coerce_id(drop_id)}) if drop_id else {}
    drop_type = drop.get("type", "pooled")
 
    user_doc = None
    if uid is not None:
        user_doc = users_collection.find_one({"user_id": uid})
    if not user_doc and uname:
        user_doc = users_collection.find_one({"usernameLower": uname})
 
    fallback_username = _guess_username(request, body)
    fallback_user_id = _guess_user_id(request, body)

    if not drop_id:
        return jsonify({"status": "error", "code": "missing_drop_id"}), 400

    voucher = drop or {}

    v_uid   = voucher.get("assigned_to_user_id")
    v_uname = norm_uname(voucher.get("assigned_to_username"))

    drop_type = voucher.get("type", drop_type)
 
    username_missing = not (username and username.strip())
    if drop_type in ("personalised", "personalized") and username_missing:
        return jsonify({"status": "error", "code": "not_eligible"}), 403

    allowed = True
    if drop_type in ("personalised", "personalized"):
        if drop_type == "personalized":
            allowed = False
            if v_uid is not None:
                allowed = (uid is not None and uid == int(v_uid))
            elif v_uname:
                allowed = (uname != "" and uname == v_uname)
            else:
                allowed = False

    if not allowed:
        print(f"[claim401] uid={uid} uname={uname} v_uid={v_uid} v_uname={v_uname}")
        return jsonify({"status": "error", "code": "not_eligible"}), 403

    user_ctx = load_user_context(uid=uid, username=username, username_lower=uname or username)

    if not is_drop_allowed(voucher, uid, uname, user_ctx):
        return jsonify({"status": "error", "code": "not_eligible"}), 403

    if not is_user_eligible_for_drop(user_doc, tg_user, voucher):
        return jsonify({"status": "error", "code": "not_eligible"}), 403
 
    if not username:
        username = fallback_username or ""
   
    if not user_id:
        user_id = fallback_user_id or username or ""

    audience_type = _drop_audience_type(voucher)
    check_only = bool(body.get("check_only") or body.get("checkOnly")) 
    client_ip = _get_client_ip(request)
    is_pool_drop = _is_pool_drop(voucher, audience_type)

    if is_pool_drop and not _is_new_joiner_audience(audience_type):
         current_app.logger.warning(
            "[CLAIM][TODO] uid=%s missing cache refresh for subscription gate",
            uid,
        )    
        if not check_channel_subscribed(uid):
            current_app.logger.info("[claim] deny drop=%s uid=%s reason=not_subscribed", drop_id, uid)
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=not_subscribed",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )         
            return jsonify({
                "status": "error",
                "code": "not_subscribed",
                "message": "Please subscribe to @advantplayofficial to claim this voucher."
            }), 403

    if check_only:
        return jsonify({"status": "ok", "check_only": True, "subscribed": True}), 200
 
    if _is_new_joiner_audience(audience_type):
        current_app.logger.info(
            "[claim] entry drop=%s audience=%s uid=%s ip=%s", drop_id, audience_type, uid, client_ip
        )
        if drop_type != "pooled":
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=bad_drop_type",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )         
            current_app.logger.info("[claim] deny drop=%s uid=%s reason=bad_drop_type type=%s", drop_id, uid, drop_type)
            return jsonify({"status": "error", "code": "not_eligible", "reason": "not_supported_for_audience"}), 403
        if uid is None:
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=missing_uid",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )         
            current_app.logger.info("[claim] deny drop=%s reason=missing_uid", drop_id)
            return jsonify({"status": "error", "code": "not_eligible", "reason": "missing_uid"}), 403
        current_app.logger.info("[WELCOME] claim_attempt uid=%s drop_id=%s", uid, drop_id)
        eligibility = ensure_welcome_eligibility(uid)
        now_ts = now_kl()
        if not _welcome_eligible_now(uid, eligibility, ref=now_ts, user_doc=user_doc):
            reason = "missing_joined_main_at"
            if eligibility and eligibility.get("claimed"):
                reason = "already_claimed"
            else:
                joined_main_at = (user_doc or users_collection.find_one({"user_id": uid}, {"joined_main_at": 1}) or {}).get("joined_main_at")
                window = _welcome_window_for_user(uid, ref=now_ts, user_doc=user_doc)
                if joined_main_at and window is None:
                    reason = "not_new_user"
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=%s",
                uid,
                drop_id,
                drop_type,
                audience_type,
                reason,
            )
            return jsonify({"status": "error", "code": "not_eligible", "reason": reason}), 403
        if new_joiner_claims_col.find_one({"uid": uid}):
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=already_claimed_lifetime",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )  
            current_app.logger.info("[claim] deny drop=%s uid=%s reason=already_claimed_lifetime", drop_id, uid)
            return jsonify({"status": "error", "code": "not_eligible", "reason": "already_claimed_lifetime"}), 403

        ok_rl, rl_reason = _check_new_joiner_rate_limits(uid=uid, ip=client_ip, now=now_ts)
        if not ok_rl:
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=%s",
                uid,
                drop_id,
                drop_type,
                audience_type,
                rl_reason,
            )         
            return jsonify({"status": "error", "code": "rate_limited", "reason": rl_reason}), 429

        # per-IP successful claims per day pre-check
        day_key = now_ts.strftime("%Y%m%d")
        ip_claim_doc = claim_rate_limits_col.find_one({"scope": "ip_claim", "ip": client_ip or "unknown", "day": day_key})
        if ip_claim_doc and (ip_claim_doc.get("count", 0) or 0) >= 3:
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=ip_claims_per_day",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )    
            current_app.logger.info("[claim] deny drop=%s uid=%s reason=ip_claims_per_day ip=%s", drop_id, uid, client_ip)
            return jsonify({"status": "error", "code": "rate_limited", "reason": "ip_claims_per_day"}), 429

        ticket = get_or_issue_welcome_ticket(uid)
        if not ticket:
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=not_eligible",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )       
            return jsonify({"status": "error", "code": "not_eligible", "reason": "not_eligible"}), 403

        expires_at = _as_aware_kl(ticket.get("expires_at"))
        if expires_at and now_ts > expires_at:
            welcome_tickets_col.update_one({"uid": uid}, {"$set": {"status": "expired", "reason_last_fail": "expired"}})
            current_app.logger.info("[welcome] gate_fail uid=%s reason=expired", uid)
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=expired",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )         
            return jsonify({"status": "error", "code": "not_eligible", "reason": "expired"}), 403

        current_app.logger.warning(
            "[CLAIM][TODO] uid=%s missing cache refresh for profile photo gate",
            uid,
        )     
        has_photo = check_has_profile_photo(uid)
        if not has_photo:
            welcome_tickets_col.update_one({"uid": uid}, {"$set": {"reason_last_fail": "no_photo"}})
            current_app.logger.info("[welcome] gate_fail uid=%s reason=no_photo", uid)
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=missing_profile_photo",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )         
            return jsonify({
               "status": "error",
               "ok": False,                       
               "code": "not_eligible",
               "reason": "missing_profile_photo",
               "error_code": "NO_PROFILE_PIC",             
               "message": "Please set a Telegram profile picture to claim the Welcome Bonus."
            }), 403

         
    # Claim
    try:
        result = claim_voucher_for_user(user_id=user_id, drop_id=drop_id, username=username)
        current_app.logger.info("[CLAIM][ATOMIC] uid=%s drop=%s outcome=success", uid, drop_id)     
    except AlreadyClaimed:
        current_app.logger.info("[CLAIM][ATOMIC] uid=%s drop=%s outcome=already_claimed", uid, drop_id)     
        return jsonify({"status": "error", "code": "already_claimed"}), 409
    except NoCodesLeft:
        current_app.logger.info("[CLAIM][ATOMIC] uid=%s drop=%s outcome=sold_out", uid, drop_id)     
        return jsonify({"status": "error", "code": "sold_out"}), 410
    except NotEligible:
        current_app.logger.info("[CLAIM][ATOMIC] uid=%s drop=%s outcome=not_eligible", uid, drop_id)     
        return jsonify({"status": "error", "code": "not_eligible"}), 403
    except Exception:
        current_app.logger.exception("[CLAIM][ERROR] step=claim_voucher uid=%s drop=%s", uid, drop_id)     
        current_app.logger.exception("claim failed")
        return jsonify({"status": "error", "code": "server_error"}), 500

    if _is_new_joiner_audience(audience_type):
        try:
            new_joiner_claims_col.update_one(
                {"uid": uid},
                {
                    "$setOnInsert": {
                        "uid": uid,
                        "drop_id": _coerce_id(drop_id),
                        "claimed_at": now_kl(),
                        "code": result.get("code"),
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            return jsonify({"status": "error", "code": "not_eligible", "reason": "already_claimed_lifetime"}), 403

        ip_claim_count = _increment_ip_claim_success(ip=client_ip)
        if ip_claim_count > 3:
            current_app.logger.info("[claim] deny post-claim drop=%s uid=%s reason=ip_claims_per_day ip=%s", drop_id, uid, client_ip)
            return jsonify({"status": "error", "code": "rate_limited", "reason": "ip_claims_per_day"}), 429

        welcome_tickets_col.update_one(
            {"uid": uid},
            {"$set": {"status": "claimed", "claimed_at": now_kl(), "reason_last_fail": None}},
        )
        if not uid:
            return jsonify({"status": "error", "code": "not_eligible", "reason": "missing_uid"}), 403     
        welcome_eligibility_col.update_one(
            {"uid": uid},
            {"$set": {"claimed": True, "claimed_at": now_kl()}},
        )     
        current_app.logger.info("[WELCOME] gate_pass uid=%s", uid)
        current_app.logger.info("[WELCOME] claim_success uid=%s drop_id=%s", uid, drop_id)
        current_app.logger.info("[claim] success drop=%s audience=%s uid=%s ip=%s", drop_id, audience_type, uid, client_ip)
        current_app.logger.info(
            "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=allowed reason=success",
            uid,
            drop_id,
            drop_type,
            audience_type,
        )
     
    response_payload = {"status": "ok", "voucher": result}
    if _is_new_joiner_audience(audience_type):
        _append_retention_message(response_payload, WELCOME_BONUS_RETENTION_MESSAGE)
    elif is_pool_drop:
        _append_retention_message(response_payload, POOL_VOUCHER_RETENTION_MESSAGE)
    if not _is_new_joiner_audience(audience_type):
        current_app.logger.info(
            "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=allowed reason=success",
            uid,
            drop_id,
            drop_type,
            audience_type,
        )     
    return jsonify(response_payload), 200
 
# ---- Admin endpoints ----
def _coerce_id(x):
    if isinstance(x, ObjectId):
        return x
    try:
        return ObjectId(x) if ObjectId.is_valid(x) else x
    except Exception:
        return x


def _drop_id_variants(drop_id):
    variants = []
    if drop_id is None:
        return [None]

    variants.append(drop_id)
    if isinstance(drop_id, ObjectId):
        variants.append(str(drop_id))

    coerced = _coerce_id(drop_id)
    if coerced != drop_id:
        variants.append(coerced)

    seen = set()
    unique = []
    for v in variants:
        if v in seen:
            continue
        seen.add(v)
        unique.append(v)
    return unique

@vouchers_bp.route("/admin/drops", methods=["POST"])
def admin_create_drop():
    """
    Body (personalised):
    { "name":"VIP", "type":"personalised", "startsAtLocal":"2025-10-13 12:00:00", "priority":200,
      "assignments":[{"username":"@a","code":"X"}, ...]
    }
    Body (pooled):
    { "name":"Pool", "type":"pooled", "startsAtLocal":"2025-10-13 18:00:00", "priority":150,
      "whitelistUsernames":["@a","@b"], "codes":["A","B","C"]
    }
    """
    user, err = require_admin()
    if err: return err
    data = request.get_json(force=True)
    name = data.get("name")
    dtype = data.get("type", "pooled")
    startsAtLocal = data.get("startsAtLocal")
    if not (name and startsAtLocal):
        return jsonify({"status": "error", "code": "bad_request"}), 400

    try:
        startsAt = parse_kl_local(startsAtLocal)
    except ValueError:
        return jsonify({"status": "error", "code": "bad_start"}), 400

    endsAtLocal = data.get("endsAtLocal")
    if endsAtLocal:
        try:
            endsAt = parse_kl_local(endsAtLocal)
        except ValueError:
            return jsonify({"status": "error", "code": "bad_end"}), 400
    else:
        endsAt = startsAt + timedelta(hours=24)

    if endsAt <= startsAt:
        return jsonify({"status": "error", "code": "end_before_start"}), 400

    priority = int(data.get("priority", 100))
    
    now = now_utc()
    status = "active" if startsAt <= now < endsAt else "upcoming"

    eligibility_raw = data.get("eligibility") or {}
    audience_raw = data.get("audience") or {}

    elig_mode = eligibility_raw.get("mode") or "public"
    elig_allow_raw = eligibility_raw.get("allow") or []
    clean_eligibility = {"mode": elig_mode}

    if elig_mode == "tier":
        allow = []
        for item in elig_allow_raw:
            val = (item or "").strip() if isinstance(item, str) else str(item).strip()
            if val:
                allow.append(val)
        if allow:
            clean_eligibility["allow"] = allow
    elif elig_mode == "user_id":
        allow = []
        for item in elig_allow_raw:
            try:
                allow.append(int(item))
            except (TypeError, ValueError):
                continue
        if allow:
            clean_eligibility["allow"] = allow
    elif elig_mode == "admin_only":
        clean_eligibility = {"mode": "admin_only"}
    else:
        clean_eligibility = {"mode": "public"}

    audience_clean = {}
    audience_type = None
    if isinstance(audience_raw, dict):
        audience_type_raw = (audience_raw.get("type") or audience_raw.get("audience") or "").strip().lower()
        if audience_type_raw:
            audience_type = audience_type_raw
    regions_raw = audience_raw.get("regions") if isinstance(audience_raw, dict) else None
    if regions_raw is not None:
        regions = []
        for r in regions_raw or []:
            if r:
                regions.append(str(r))
        audience_clean["regions"] = regions
    if audience_type:
        audience_clean["type"] = audience_type     
      
    drop_doc = {
        "name": name,
        "type": dtype,
        "startsAt": startsAt,
        "endsAt": endsAt,
        "priority": priority,
        "visibilityMode": "stacked",
        "status": status
    }
 
    if data.get("eligibility") is not None or clean_eligibility.get("mode") != "public" or clean_eligibility.get("allow"):
        drop_doc["eligibility"] = clean_eligibility

    if dtype == "pooled":
        wl_raw = data.get("whitelistUsernames") or []
        wl_clean = []
        marker_new_joiner = False
        marker_new_joiner_48h = False 
        for item in wl_raw:
            u = norm_username(item)
            if u == "new_joiner_48h":
                marker_new_joiner_48h = True
                continue
            if u == "new_joiner":
                marker_new_joiner = True
                continue         
            if u and u not in wl_clean:
                wl_clean.append(u)
        if marker_new_joiner_48h and not audience_type:
            audience_clean["type"] = "new_joiner_48h"
        elif marker_new_joiner and not audience_type:
            audience_clean["type"] = "new_joiner"             
        drop_doc["whitelistUsernames"] = wl_clean

    if audience_clean:
        drop_doc["audience"] = audience_clean     
     
    res = db.drops.insert_one(drop_doc)
    drop_id = res.inserted_id

    # Insert vouchers
    if dtype == "personalised":
        assignments = data.get("assignments") or []
        docs = []
        for item in assignments:
            u = norm_username(item.get("username", ""))
            c = (item.get("code") or "").strip()
            if not u or not c:
                continue
            docs.append({
                "type": "personalised",
                "dropId": str(drop_id),
                "usernameLower": u,
                "code": c,
                "status": "unclaimed",
                "claimedBy": None,
                "claimedAt": None
            })
        if docs:
            db.vouchers.insert_many(docs, ordered=False)
    else:
        codes = _normalize_codes(data.get("codes"))
        docs = []
        for c in codes:
            docs.append({
                "type": "pooled",
                "dropId": str(drop_id),
                "code": c,
                "status": "free",
                "claimedBy": None,
                "claimedAt": None
            })
        if docs:
            db.vouchers.insert_many(docs, ordered=False)

    return jsonify({"status": "ok", "dropId": str(drop_id)})
    
def _admin_drop_summary(doc: dict, *, ref=None, skip_expired=False):
    """Return a normalised representation of a drop for admin surfaces."""
    ref = ref or now_utc()

    starts = _as_aware_utc(doc.get("startsAt"))
    ends = _as_aware_utc(doc.get("endsAt"))

    status = doc.get("status", "upcoming")
    if status not in ("paused", "expired"):
        if ends and ref >= ends:
            status = "expired"
        elif starts and starts <= ref < (ends or starts):
            status = "active"
        else:
            status = "upcoming"

    if skip_expired and status == "expired":
        return None

    drop_id = str(doc["_id"])
    drop_id_variants = _drop_id_variants(doc.get("_id"))
    summary = {
        "dropId": drop_id,
        "name": doc.get("name"),
        "type": doc.get("type", "pooled"),
        "status": status,
        "priority": doc.get("priority", 100),
        "startsAt": starts.isoformat() if starts else None,
        "endsAt": ends.isoformat() if ends else None,
    }

    audience_type = _drop_audience_type(doc)
    if audience_type and audience_type != "public":
        summary["audienceType"] = audience_type
    if doc.get("audience"):
        summary["audience"] = doc.get("audience")
     
    if summary["type"] == "personalised":
        assigned = db.vouchers.count_documents({"type": "personalised", "dropId": {"$in": drop_id_variants}})
        claimed = db.vouchers.count_documents({"type": "personalised", "dropId": {"$in": drop_id_variants}, "status": "claimed"})
        summary.update({"assigned": assigned, "claimed": claimed})
    else:
        total = db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": drop_id_variants}})
        free = db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": drop_id_variants}, "status": "free"})
        summary.update({"codesTotal": total, "codesFree": free})

    for key in ("whitelistUsernames", "visibilityMode"):
        if key in doc:
            summary[key] = doc[key]

    return summary

@vouchers_bp.route("/admin/drops_v2", methods=["GET"])
def list_drops_v2():
    if BYPASS_ADMIN:
        pass
    else:
        _, err = require_admin()
        if err:
            return err

    items = []
    ref = now_utc()
    cursor = db.drops.find({}).sort([("priority", -1), ("startsAt", -1)])
    for d in cursor:
        row = _admin_drop_summary(d, ref=ref, skip_expired=True)
        if row:
            items.append(row)
    return jsonify({"status": "ok", "items": items}), 200
    
@vouchers_bp.route("/admin/drops/<drop_id>/codes", methods=["POST"])
def admin_add_codes(drop_id):
    user, err = require_admin()
    if err: return err
    data = request.get_json(force=True)
    dtype = data.get("type")  # optional override
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404
    dtype = dtype or drop.get("type", "pooled")

    if dtype == "personalised":
        assignments = data.get("assignments") or []
        docs = []
        for item in assignments:
            u = norm_username(item.get("username", ""))
            c = (item.get("code") or "").strip()
            if not u or not c:
                continue
            docs.append({
                "type": "personalised",
                "dropId": str(drop["_id"]),
                "usernameLower": u,
                "code": c,
                "status": "unclaimed",
                "claimedBy": None,
                "claimedAt": None
            })
        if docs:
            db.vouchers.insert_many(docs, ordered=False)
    else:
        codes = _normalize_codes(data.get("codes"))
        docs = []
        for c in codes:
            docs.append({
                "type": "pooled",
                "dropId": str(drop["_id"]),
                "code": c,
                "status": "free",
                "claimedBy": None,
                "claimedAt": None
            })
        if docs:
            db.vouchers.insert_many(docs, ordered=False)

        whitelist_updates = data.get("whitelistUsernames") or []
        whitelist_mode = data.get("whitelistMode", "append")
        if whitelist_updates or whitelist_mode == "replace":
            merged = []
            if whitelist_mode == "replace":
                source = whitelist_updates
            else:
                source = (drop.get("whitelistUsernames") or []) + whitelist_updates
            for item in source:
                u = norm_username(item)
                if u and u not in merged:
                    merged.append(u)
            db.drops.update_one(
                {"_id": drop["_id"]},
                {"$set": {"whitelistUsernames": merged}}
            )

    return jsonify({"status": "ok"})

@vouchers_bp.route("/admin/drops/<drop_id>/actions", methods=["POST"])
def admin_drop_actions(drop_id):
    user, err = require_admin()
    if err: return err
    data = request.get_json(force=True)
    op = data.get("op")
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404

    if op == "start_now":
        now = now_utc()
        db.drops.update_one({"_id": drop["_id"]}, {"$set": {"startsAt": now, "endsAt": now + timedelta(hours=24), "status": "active"}})
    elif op == "pause":
        db.drops.update_one({"_id": drop["_id"]}, {"$set": {"status": "paused"}})
    elif op == "end_now":
        db.drops.update_one({"_id": drop["_id"]}, {"$set": {"endsAt": now_utc(), "status": "expired"}})
    else:
        return jsonify({"status": "error", "code": "bad_request"}), 400

    return jsonify({"status": "ok"})

@vouchers_bp.route("/admin/drops", methods=["GET"])
def admin_list_drops():
    user, err = require_admin()
    if err: return err

    items = []
    ref = now_utc()

    for d in db.drops.find().sort([("priority", DESCENDING), ("startsAt", ASCENDING)]):
        row = _admin_drop_summary(d, ref=ref)
        if row:
            items.append(row)

    return jsonify({"status": "ok", "items": items})
