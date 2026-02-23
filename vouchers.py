from flask import Blueprint, request, jsonify, current_app
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import OperationFailure, PyMongoError, DuplicateKeyError, BulkWriteError
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone
import threading
import requests
import time
from config import KL_TZ
import hmac, hashlib, urllib.parse, os, json
import config as _cfg 
 
from database import db, users_collection
from time_utils import as_aware_utc
from onboarding import record_onboarding_start, record_visible_ping
from affiliate_rewards import approve_affiliate_ledger, reject_affiliate_ledger

admin_cache_col = db["admin_cache"]
new_joiner_claims_col = db["new_joiner_claims"]
claim_rate_limits_col = db["claim_rate_limits"]
profile_photo_cache_col = db["profile_photo_cache"]
welcome_tickets_col = db["welcome_tickets"]
subscription_cache_col = db["subscription_cache"]
welcome_eligibility_col = db["welcome_eligibility"]
tg_verification_queue_col = db["tg_verification_queue"]
voucher_claims_col = db["voucher_claims"]
invite_link_map_collection = db["invite_link_map"]
miniapp_sessions_daily_col = db["miniapp_sessions_daily"]
REFERRAL_SNAPSHOT_STALE_SEC = 21600


def _week_key_kl(reference: datetime) -> str:
    local_ref = as_aware_utc(reference).astimezone(KL_TZ)
    week_start = (local_ref - timedelta(days=local_ref.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return week_start.date().isoformat()


def _month_key_kl(reference: datetime) -> str:
    local_ref = as_aware_utc(reference).astimezone(KL_TZ)
    month_start = local_ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return month_start.date().isoformat()


def _ledger_referral_counts(referral_events_col, inviter_id: int, now_ref: datetime) -> dict:
    week_key = _week_key_kl(now_ref)
    month_key = _month_key_kl(now_ref)
    settled_total = int(referral_events_col.count_documents({"inviter_id": inviter_id, "event": "referral_settled"}))
    revoked_total = int(referral_events_col.count_documents({"inviter_id": inviter_id, "event": "referral_revoked"}))
    settled_week = int(
        referral_events_col.count_documents(
            {"inviter_id": inviter_id, "event": "referral_settled", "week_key": week_key}
        )
    )
    revoked_week = int(
        referral_events_col.count_documents(
            {"inviter_id": inviter_id, "event": "referral_revoked", "week_key": week_key}
        )
    )
    settled_month = int(
        referral_events_col.count_documents(
            {"inviter_id": inviter_id, "event": "referral_settled", "month_key": month_key}
        )
    )
    revoked_month = int(
        referral_events_col.count_documents(
            {"inviter_id": inviter_id, "event": "referral_revoked", "month_key": month_key}
        )
    )
    return {
        "total_referrals": max(0, settled_total - revoked_total),
        "weekly_referrals": max(0, settled_week - revoked_week),
        "monthly_referrals": max(0, settled_month - revoked_month),
    }


def _ensure_index_if_missing(col, name, keys, **kwargs):
    """Create index only when missing by name; safe for concurrent startup calls."""
    try:
        for idx in col.list_indexes():
            if idx.get("name") == name:
                return name
    except PyMongoError:
        raise

    try:
        return col.create_index(keys, name=name, **kwargs)
    except OperationFailure as exc:
        # MongoDB may raise index conflict/exists codes during concurrent startup:
        # 68=IndexAlreadyExists, 85=IndexOptionsConflict, 86=IndexKeySpecsConflict.
        if exc.code in (68, 85, 86) or "already exists" in str(exc).lower():
            return name
        raise

BYPASS_ADMIN = os.getenv("BYPASS_ADMIN", "0").lower() in ("1", "true", "yes", "on")
HARDCODED_ADMIN_USERNAMES = {"gracy_ap", "teohyaohui"}  # allow manual overrides if cache is empty
WELCOME_WINDOW_HOURS = int(getattr(_cfg, "WELCOME_WINDOW_HOURS", os.getenv("WELCOME_WINDOW_HOURS", "48")))
WELCOME_WINDOW_DAYS = 7
PROFILE_PHOTO_CACHE_TTL_SECONDS = 60
VERIFY_QUEUE_MAX_ATTEMPTS = int(os.getenv("VERIFY_QUEUE_MAX_ATTEMPTS", "3"))
VERIFY_QUEUE_BACKOFF_BASE_SECONDS = int(os.getenv("VERIFY_QUEUE_BACKOFF_BASE_SECONDS", "30"))
VERIFY_QUEUE_BACKOFF_MAX_SECONDS = int(os.getenv("VERIFY_QUEUE_BACKOFF_MAX_SECONDS", "300"))
IP_KILL_WINDOW_SECONDS = int(os.getenv("IP_KILL_WINDOW_SECONDS", "600"))
IP_KILL_MAX_SUCCESSES = int(os.getenv("IP_KILL_MAX_SUCCESSES", "2"))
SUBNET_KILL_MAX_SUCCESSES = int(os.getenv("SUBNET_KILL_MAX_SUCCESSES", "4"))
KILL_BLOCK_SECONDS = int(os.getenv("KILL_BLOCK_SECONDS", "86400"))
CLAIM_COOLDOWN_SECONDS = int(os.getenv("CLAIM_COOLDOWN_SECONDS", "180"))
SESSION_COOLDOWN_SEC = int(os.getenv("SESSION_COOLDOWN_SEC", "30"))
_BYPASS_WARNING_LOGGED = False
DROP_MODE_CACHE_TTL_SECONDS = 2
_DROP_MODE_CACHE = {"payload": None, "expires_at": None}
_DROP_MODE_CACHE_LOCK = threading.Lock()
_DROP_MODE_LAST_LOGGED_UNTIL = None

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


def _miniapp_no_store_json(payload: dict, status_code: int = 200):
    resp = jsonify(payload)
    resp.status_code = status_code
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp
 
def now_utc():
    return datetime.now(timezone.utc)

def now_kl():
    return datetime.now(KL_TZ)


def _safe_log(level: str, message: str, *args) -> None:
    try:
        logger = current_app.logger
    except Exception:
        logger = None
    if logger:
        getattr(logger, level)(message, *args)
    else:
        try:
            formatted = message % args if args else message
        except Exception:
            formatted = message
        print(formatted)

def _warn_bypass_disabled_once():
    global _BYPASS_WARNING_LOGGED
    if _BYPASS_WARNING_LOGGED:
        return
    msg = "[VERIFY_QUEUE] bypass_document_validation disabled; using schema-compliant writes"
    try:
        current_app.logger.warning(msg)
    except Exception:
        print(msg)
    _BYPASS_WARNING_LOGGED = True


def _maybe_migrate_verification_queue_uid():
    if os.getenv("VERIFY_QUEUE_MIGRATE") != "1":
        return
    matched = modified = skipped = 0
    try:
        cursor = tg_verification_queue_col.find(
            {"user_id": {"$exists": False}, "uid": {"$exists": True}},
            {"uid": 1},
        )
        for doc in cursor:
            matched += 1
            raw_uid = doc.get("uid")
            try:
                user_id = int(raw_uid)
            except (TypeError, ValueError):
                skipped += 1
                continue
            res = tg_verification_queue_col.update_one(
                {"_id": doc["_id"], "user_id": {"$exists": False}},
                {"$set": {"user_id": user_id}},
            )
            modified += res.modified_count
        msg = (
            "[VERIFY_QUEUE] migrate_uid_to_user_id "
            f"matched={matched} modified={modified} skipped={skipped}"
        )
        try:
            current_app.logger.info(msg)
        except Exception:
            print(msg)
    except Exception:
        try:
            current_app.logger.exception("[VERIFY_QUEUE] migrate_uid_to_user_id_failed")
        except Exception:
            print("[VERIFY_QUEUE] migrate_uid_to_user_id_failed")
         
def ensure_voucher_indexes():
    _warn_bypass_disabled_once() 
    db.drops.create_index([("startsAt", ASCENDING)])
    db.drops.create_index([("endsAt", ASCENDING)])
    db.drops.create_index([("priority", DESCENDING)])
    db.drops.create_index([("status", ASCENDING)])
    db.vouchers.create_index([("code", ASCENDING)], unique=True)
    db.vouchers.create_index([("dropId", ASCENDING), ("type", ASCENDING), ("status", ASCENDING)])
    db.vouchers.create_index(
        [("dropId", ASCENDING), ("type", ASCENDING), ("status", ASCENDING), ("pool", ASCENDING)],
        name="ix_drop_type_status_pool",
    ) 
    db.vouchers.create_index([("dropId", ASCENDING), ("usernameLower", ASCENDING)])
    db.vouchers.create_index([("dropId", ASCENDING), ("claimedBy", ASCENDING)])
    try:
        db.vouchers.create_index(
            [("dropId", ASCENDING), ("claimedByKey", ASCENDING)],
            name="claimed_by_key_per_drop",
            partialFilterExpression={"status": "claimed", "claimedByKey": {"$exists": True}},
        )
    except (OperationFailure, PyMongoError):
        try:
            current_app.logger.exception("[indexes] failed to ensure claimed_by_key_per_drop")
        except Exception:
            print("[indexes] failed to ensure claimed_by_key_per_drop")
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
    try:
        voucher_claims_col.create_index(
            [("drop_id", ASCENDING), ("user_id", ASCENDING)],
            unique=True,
            name="uq_claim_drop_user",
        )
    except OperationFailure:
        pass
    new_joiner_claims_col.create_index([("uid", ASCENDING)], unique=True)
    db.affiliate_daily_kpis.create_index([("day_utc", ASCENDING)], unique=True, name="uniq_affiliate_daily_kpi_day")
    claim_rate_limits_col.create_index([("key", ASCENDING)], unique=True)
    claim_rate_limits_col.create_index([("expiresAt", ASCENDING)], expireAfterSeconds=0)
    claim_rate_limits_col.create_index([("scope", ASCENDING), ("ip", ASCENDING), ("day", ASCENDING)])
    profile_photo_cache_col.create_index([("uid", ASCENDING)], unique=True)
    profile_photo_cache_col.create_index([("expiresAt", ASCENDING)], expireAfterSeconds=0)
    welcome_tickets_col.create_index([("uid", ASCENDING)], unique=True)
    welcome_tickets_col.create_index([("cleanup_at", ASCENDING)], expireAfterSeconds=0)
    welcome_eligibility_col.create_index([("uid", ASCENDING)], unique=True)
    subscription_cache_col.create_index([("expireAt", ASCENDING)], expireAfterSeconds=0)
    try:
        for legacy_name in ("uniq_user_checks", "uniq_tg_verify_user_id", "uq_tg_verif_user_id_sparse"):
            try:
                tg_verification_queue_col.drop_index(legacy_name)
            except OperationFailure:
                pass     
        _ensure_index_if_missing(
            tg_verification_queue_col,
            "uq_tg_verif_user_id_nonnull",
            [("user_id", ASCENDING)],
            unique=True,
            partialFilterExpression={"user_id": {"$type": "number"}},
        )
        _ensure_index_if_missing(
            tg_verification_queue_col,
            "ix_verif_status_created",
            [("status", ASCENDING), ("created_at", ASCENDING)],
        )
    except OperationFailure:
        try:
            current_app.logger.exception(
                "[VERIFY_QUEUE] failed to ensure verification queue indexes"
            )
        except Exception:
            print("[VERIFY_QUEUE] failed to ensure verification queue indexes")

    _maybe_migrate_verification_queue_uid()

    if os.getenv("VERIFY_QUEUE_CLEANUP") == "1":
        try:
            result = tg_verification_queue_col.delete_many(
                {"$or": [{"user_id": None}, {"user_id": {"$exists": False}}]}
            )
            msg = f"[VERIFY_QUEUE] cleanup_bad_docs deleted={result.deleted_count}"
            try:
                current_app.logger.info(msg)
            except Exception:
                print(msg)
        except Exception:
            try:
                current_app.logger.exception("[VERIFY_QUEUE] cleanup_bad_docs_failed")
            except Exception:
                print("[VERIFY_QUEUE] cleanup_bad_docs_failed")
             
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

    candidates = []
    fly_client_ip = req.headers.get("Fly-Client-IP")
    if fly_client_ip:
        candidates.append(fly_client_ip)     
    forwarded = req.headers.get("X-Forwarded-For")
    if forwarded:
        candidates.append(forwarded.split(",")[0])
    real_ip = req.headers.get("X-Real-IP")
    if real_ip:
        candidates.append(real_ip)
    if req.remote_addr:
        candidates.append(req.remote_addr)

    cleaned = [value.strip() for value in candidates if value and value.strip()]
    for value in cleaned:
        if not _is_private_ipv4(value):
            return value
    return cleaned[0] if cleaned else ""

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

def _rate_limit_key(prefix: str, *parts):
    return ":".join([prefix] + [str(p) for p in parts if p is not None])

def _session_key_prefix(session_key: str, length: int = 8) -> str:
    if not session_key:
        return "unknown"
    return session_key[:length]


def _derive_session_key(
    *,
    init_data_raw: str | None,
    uid: int | None,
    auth_date: str | None,
    query_id: str | None,
) -> str:
    if init_data_raw:
        return hashlib.sha256(init_data_raw.encode("utf-8")).hexdigest()[:32]
    salt = (
        os.environ.get("BOT_TOKEN")
        or os.environ.get("SECRET_KEY")
        or getattr(_cfg, "SECRET_KEY", "")
    )
    seed = f"{uid or ''}:{auth_date or ''}:{query_id or ''}:{salt}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]


def _should_enforce_session_cooldown(subnet: str | None, ip: str | None) -> bool:
    if _is_unknown_subnet(subnet):
        return True
    if not ip or not _is_public_ipv4(ip):
        return True
    return False


def _check_session_cooldown(
    *,
    session_key: str,
    now: datetime | None = None,
    rate_limits_col=claim_rate_limits_col,
):
    if not session_key:
        return True, None, 0
    now = now or now_utc()
    key = _rate_limit_key("cooldown", "session", session_key)
    doc = rate_limits_col.find_one({"key": key})
    expires_at = _as_aware_utc((doc or {}).get("expiresAt"))
    if expires_at and expires_at > now:
        retry_seconds = _seconds_until(expires_at, now)
        _safe_log(
            "info",
            "[cooldown] scope=session action=deny session=%s until=%s",
            _session_key_prefix(session_key),
            expires_at,
        )
        return False, "session_cooldown", retry_seconds
    return True, None, 0


def _set_session_cooldown(
    *,
    session_key: str,
    now: datetime | None = None,
    rate_limits_col=claim_rate_limits_col,
    cooldown_seconds: int = SESSION_COOLDOWN_SEC,
):
    if not session_key:
        return
    now = now or now_utc()
    expires_at = now + timedelta(seconds=cooldown_seconds)
    key = _rate_limit_key("cooldown", "session", session_key)
    rate_limits_col.update_one(
        {"key": key},
        {
            "$set": {
                "key": key,
                "scope": "cooldown_session",
                "session": session_key,
                "expiresAt": expires_at,
            }
        },
        upsert=True,
    )
    _safe_log(
        "info",
        "[cooldown] scope=session action=set session=%s exp=%s",
        _session_key_prefix(session_key),
        expires_at,
    )


def _session_cooldown_payload(retry_seconds: int) -> dict:
    return {
        "ok": False,
        "code": "rate_limited",
        "reason": "session_cooldown",
        "retry_after_sec": int(retry_seconds),
        "message": f"Please try again in {int(retry_seconds)} seconds.",
    }
 

def _parse_ipv4(value: str) -> list[int] | None:
    parts = value.split(".")
    if len(parts) != 4:
        return None
    try:
        octets = [int(p) for p in parts]
    except ValueError:
        return None
    if any(o < 0 or o > 255 for o in octets):
        return None
    return octets


def _is_private_ipv4(value: str) -> bool:
    octets = _parse_ipv4(value)
    if not octets:
        return False
    first, second = octets[0], octets[1]
    if first == 10:
        return True
    if first == 127:
        return True
    if first == 192 and second == 168:
        return True
    if first == 172 and 16 <= second <= 31:
        return True
    return False


def _is_public_ipv4(value: str) -> bool:
    if not value:
        return False
    return _parse_ipv4(value) is not None and not _is_private_ipv4(value)


def _is_unknown_subnet(subnet: str | None) -> bool:
    return not subnet or subnet == "unknown"


def _compute_subnet_key(ip: str) -> str:
    octets = _parse_ipv4(ip or "")
    if not octets:     
        return "unknown"
    return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"

def _seconds_until(target: datetime | None, now: datetime) -> int:
    if not target:
        return 0
    delta = (target - now).total_seconds()
    return max(0, int(delta))

def _check_kill_switch(
    *,
    ip: str,
    subnet: str,
    now: datetime | None = None,
    rate_limits_col=claim_rate_limits_col,
):
    now = now or now_utc()
    ip_key = _rate_limit_key("kill", "ip", ip or "unknown")
    ip_doc = rate_limits_col.find_one({"key": ip_key})
    blocked_until = _as_aware_utc((ip_doc or {}).get("blockedUntil"))
    if blocked_until and blocked_until > now:
        retry_seconds = _seconds_until(blocked_until, now)
        _safe_log(
            "info",
            "[kill] action=deny scope=ip ip=%s until=%s",
            ip or "unknown",
            blocked_until,
        )
        return False, "ip_killed", retry_seconds

    if not _is_unknown_subnet(subnet):
        subnet_key = _rate_limit_key("kill", "subnet", subnet)
        subnet_doc = rate_limits_col.find_one({"key": subnet_key})
        blocked_until = _as_aware_utc((subnet_doc or {}).get("blockedUntil"))
        if blocked_until and blocked_until > now:
            retry_seconds = _seconds_until(blocked_until, now)
            _safe_log(
                "info",
                "[kill] action=deny scope=subnet subnet=%s until=%s",
                subnet,
                blocked_until,
            )
            return False, "subnet_killed", retry_seconds

    return True, None, 0

def _apply_kill_counter(
    *,
    scope: str,
    key: str,
    identifier: str,
    threshold: int,
    window_seconds: int,
    block_seconds: int,
    now: datetime,
    rate_limits_col=claim_rate_limits_col,
):
    doc = rate_limits_col.find_one({"key": key}) or {}
    first_at = _as_aware_utc(doc.get("firstAt"))
    if not first_at or (now - first_at).total_seconds() > window_seconds:
        first_at = now
        count = 1
    else:
        count = int(doc.get("count", 0) or 0) + 1

    blocked_until = None
    if count >= threshold:
        blocked_until = now + timedelta(seconds=block_seconds)
        _safe_log(
            "info",
            "[kill] action=block scope=%s %s=%s count=%s window=%s until=%s",
            scope,
            "ip" if scope == "kill_ip" else "subnet",
            identifier,
            count,
            window_seconds,
            blocked_until,
        )

    expires_at = now + timedelta(seconds=block_seconds if blocked_until else window_seconds)
    update_doc = {
        "key": key,
        "scope": scope,
        "count": count,
        "firstAt": first_at,
        "expiresAt": expires_at,
    }
    if scope == "kill_ip":
        update_doc["ip"] = identifier
    else:
        update_doc["subnet"] = identifier

    update = {"$set": update_doc}
    if blocked_until:
        update["$set"]["blockedUntil"] = blocked_until
    else:
        update["$unset"] = {"blockedUntil": ""}

    rate_limits_col.update_one({"key": key}, update, upsert=True)

def _apply_kill_success(
    *,
    ip: str,
    subnet: str,
    now: datetime | None = None,
    rate_limits_col=claim_rate_limits_col,
    ip_threshold: int = IP_KILL_MAX_SUCCESSES,
    subnet_threshold: int = SUBNET_KILL_MAX_SUCCESSES,
    window_seconds: int = IP_KILL_WINDOW_SECONDS,
    block_seconds: int = KILL_BLOCK_SECONDS,
):
    now = now or now_utc()
    ip_value = ip or "unknown"
    subnet_value = subnet or "unknown"
    _apply_kill_counter(
        scope="kill_ip",
        key=_rate_limit_key("kill", "ip", ip_value),
        identifier=ip_value,
        threshold=ip_threshold,
        window_seconds=window_seconds,
        block_seconds=block_seconds,
        now=now,
        rate_limits_col=rate_limits_col,
    )
    if not _is_unknown_subnet(subnet_value):
        _apply_kill_counter(
            scope="kill_subnet",
            key=_rate_limit_key("kill", "subnet", subnet_value),
            identifier=subnet_value,
            threshold=subnet_threshold,
            window_seconds=window_seconds,
            block_seconds=block_seconds,
            now=now,
            rate_limits_col=rate_limits_col,
        )

def _check_cooldown(
    *,
    ip: str,
    subnet: str,
    uid: str | int | None = None, 
    now: datetime | None = None,
    rate_limits_col=claim_rate_limits_col,
):
    now = now or now_utc()
    uid_value = str(uid).strip() if uid is not None else ""
    if uid_value:
        uid_key = _rate_limit_key("cooldown", "uid", uid_value)
        uid_doc = rate_limits_col.find_one({"key": uid_key})
        uid_expires = _as_aware_utc((uid_doc or {}).get("expiresAt"))
        if uid_expires and uid_expires > now:
            retry_seconds = _seconds_until(uid_expires, now)
            _safe_log(
                "info",
                "[cooldown] action=deny scope=uid uid=%s until=%s",
                uid_value,
                uid_expires,
            )
            return False, "cooldown", retry_seconds

    if _is_unknown_subnet(subnet):
        _safe_log(
            "info",
            "[cooldown] scope=subnet subnet=unknown => bypass_unknown_subnet=true",
        )
        if _is_public_ipv4(ip):
            ip_key = _rate_limit_key("cooldown", "ip", ip)
            ip_doc = rate_limits_col.find_one({"key": ip_key})
            ip_expires = _as_aware_utc((ip_doc or {}).get("expiresAt"))
            if ip_expires and ip_expires > now:
                retry_seconds = _seconds_until(ip_expires, now)
                _safe_log(
                    "info",
                    "[cooldown] action=deny scope=ip ip=%s until=%s",
                    ip,
                    ip_expires,
                )
                return False, "cooldown", retry_seconds
        return True, None, 0
 
    ip_key = _rate_limit_key("cooldown", "ip", ip or "unknown")
    ip_doc = rate_limits_col.find_one({"key": ip_key})
    ip_expires = _as_aware_utc((ip_doc or {}).get("expiresAt"))
    if ip_expires and ip_expires > now:
        retry_seconds = _seconds_until(ip_expires, now)
        _safe_log(
            "info",
            "[cooldown] action=deny scope=ip ip=%s until=%s",
            ip or "unknown",
            ip_expires,
        )
        return False, "cooldown", retry_seconds

    subnet_key = _rate_limit_key("cooldown", "subnet", subnet)
    subnet_doc = rate_limits_col.find_one({"key": subnet_key})
    subnet_expires = _as_aware_utc((subnet_doc or {}).get("expiresAt"))
    if subnet_expires and subnet_expires > now:
        retry_seconds = _seconds_until(subnet_expires, now)
        _safe_log(
            "info",
            "[cooldown] action=deny scope=subnet subnet=%s until=%s",
            subnet,
            subnet_expires,
        )
        return False, "cooldown", retry_seconds

    return True, None, 0

def _set_cooldown(
    *,
    ip: str,
    subnet: str,
    uid: str | int | None = None,
    now: datetime | None = None,
    rate_limits_col=claim_rate_limits_col,
    cooldown_seconds: int = CLAIM_COOLDOWN_SECONDS,
):
    now = now or now_utc()
    ip_value = ip or "unknown"
    subnet_value = subnet or "unknown"
    expires_at = now + timedelta(seconds=cooldown_seconds)
    uid_value = str(uid).strip() if uid is not None else ""
    if _is_unknown_subnet(subnet_value):
        if uid_value:
            rate_limits_col.update_one(
                {"key": _rate_limit_key("cooldown", "uid", uid_value)},
                {
                    "$set": {
                        "key": _rate_limit_key("cooldown", "uid", uid_value),
                        "scope": "cooldown_uid",
                        "uid": uid_value,
                        "expiresAt": expires_at,
                    }
                },
                upsert=True,
            )
            _safe_log("info", "[cooldown] scope=uid uid=%s => applied", uid_value)
        if _is_public_ipv4(ip):
            rate_limits_col.update_one(
                {"key": _rate_limit_key("cooldown", "ip", ip)},
                {
                    "$set": {
                        "key": _rate_limit_key("cooldown", "ip", ip),
                        "scope": "cooldown_ip",
                        "ip": ip,
                        "expiresAt": expires_at,
                    }
                },
                upsert=True,
            )
        return 
    rate_limits_col.update_one(
        {"key": _rate_limit_key("cooldown", "ip", ip_value)},
        {
            "$set": {
                "key": _rate_limit_key("cooldown", "ip", ip_value),
                "scope": "cooldown_ip",
                "ip": ip_value,
                "expiresAt": expires_at,
            }
        },
        upsert=True,
    )
    rate_limits_col.update_one(
        {"key": _rate_limit_key("cooldown", "subnet", subnet_value)},
        {
            "$set": {
                "key": _rate_limit_key("cooldown", "subnet", subnet_value),
                "scope": "cooldown_subnet",
                "subnet": subnet_value,
                "expiresAt": expires_at,
            }
        },
        upsert=True,
    )

def _build_idempotent_claim_response(existing_claim: dict | None):
    existing_code = (existing_claim or {}).get("voucher_code")
    if not existing_code:
        return None
    claimed_at = (existing_claim or {}).get("claimed_at")
    return {
        "status": "ok",
        "voucher": {
            "code": existing_code,
            "claimedAt": _isoformat_kl(claimed_at) if claimed_at else None,
        },
    }

def _acquire_claim_lock(
    *,
    drop_id,
    user_id,
    client_ip: str,
    user_agent: str,
    now: datetime | None = None,
    claims_col=voucher_claims_col,
):
    now = now or now_utc()
    claim_doc_id = None
    existing_claim = None
    claim_doc = {
        "drop_id": drop_id,
        "user_id": user_id,
        "status": "claimed_pending_code",
        "created_at": now,
        "updated_at": now,
        "ip": client_ip,
        "ua": user_agent,
    }
    try:
        insert_res = claims_col.insert_one(claim_doc)
        claim_doc_id = insert_res.inserted_id
    except DuplicateKeyError:
        existing_claim = claims_col.find_one({"drop_id": drop_id, "user_id": user_id})
        if existing_claim and existing_claim.get("status") == "failed":
            retry_claim = claims_col.find_one_and_update(
                {"_id": existing_claim["_id"], "status": "failed"},
                {"$set": {"status": "claimed_pending_code", "updated_at": now, "error": None}},
                return_document=ReturnDocument.AFTER,
            )
            if retry_claim:
                claim_doc_id = retry_claim["_id"]
    return claim_doc_id, existing_claim

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
                _safe_log(
                    "info",
                    "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=%s source=cache",
                    uid,
                    str(has_photo).lower(),
                )
                return has_photo
             
        except Exception as e:
            _safe_log("warning", "[tg] profile photo cache read/compare failed uid=%s err=%s", uid, e)

    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        _safe_log("warning", "[tg] missing BOT_TOKEN for profile photo check")
        _safe_log(
            "info",
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
        _safe_log("warning", "[tg] getUserProfilePhotos network error uid=%s err=%s", uid, e)
        _safe_log(
            "info",
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    if resp.status_code != 200:
        _safe_log("warning", "[tg] getUserProfilePhotos http status=%s uid=%s", resp.status_code, uid)
        _safe_log(
            "info",
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    try:
        data = resp.json()
    except ValueError:
        _safe_log("warning", "[tg] getUserProfilePhotos bad json uid=%s", uid)
        _safe_log(
            "info",
            "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=false source=telegram",
            uid,
        )
        return False

    if not data.get("ok"):
        _safe_log("warning", "[tg] getUserProfilePhotos not ok uid=%s err=%s", uid, data)
        _safe_log(
            "info",
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
    _safe_log(
        "info",
        "[WELCOME][PROFILE_PIC_CHECK] uid=%s has_photo=%s source=telegram",
        uid,
        str(has_photo).lower(),
    )
    return has_photo

def _profile_photo_cache_status(uid: int) -> tuple[bool | None, bool]:
    if uid is None:
        return None, True
    try:
        cached = profile_photo_cache_col.find_one({"uid": uid})
    except Exception:
        return None, True
    if not cached:
        return None, True
    exp = (cached or {}).get("expiresAt")
    exp_aware = _as_aware_kl(exp) or _as_aware_utc(exp)
    if not exp_aware or exp_aware <= now_kl():
        return None, True
    return bool(cached.get("hasPhoto")), False
 
def enqueue_verification(*, user_id: int, verify_type: str, payload: dict | None = None) -> tuple[bool, str | None]:
    """
    Enqueue verification task safely.
    Returns (ok, error_reason).
    """
    if user_id is None or not verify_type:
        current_app.logger.warning(
            "[VERIFY_QUEUE] enqueue skipped: missing user_id/type user_id=%s type=%s",
            user_id,
            verify_type,
        )
        return False, "missing_fields"
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        current_app.logger.warning(
            "[VERIFY_QUEUE] enqueue skipped: invalid user_id=%s type=%s",
            user_id,
            verify_type,
        )
        return False, "invalid_user_id"

    now = now_utc()
    verify_type = str(verify_type).strip().lower()
    update_doc = {
        "user_id": user_id,
        "type": verify_type,
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "attempts": 0,
        "last_error": None,
    }
    if payload is not None:
        update_doc["payload"] = payload
    try:
        existing = tg_verification_queue_col.find_one(
            {"user_id": user_id, "type": verify_type, "status": {"$in": ["queued", "processing"]}},
            {"_id": 1},
        )
        if existing:
            current_app.logger.info(
                "[VERIFY_QUEUE] skip duplicate enqueue user_id=%s type=%s",
                user_id,
                verify_type,
            )
            return True, "already_queued"

        tg_verification_queue_col.update_one(
            {"user_id": user_id, "type": verify_type},
            {"$set": update_doc},
            upsert=True,
        )
        current_app.logger.info(
            "[VERIFY_QUEUE] enqueued user_id=%s type=%s",
            user_id,
            verify_type,
        )
        return True, None
    except OperationFailure as e:
        current_app.logger.exception(
            "[VERIFY_QUEUE] enqueue failed user_id=%s type=%s err_class=%s collection=%s err=%s",
            user_id,
            verify_type,
            e.__class__.__name__,
            tg_verification_queue_col.name,         
            e,
        )
    except PyMongoError as e:
        current_app.logger.exception(
            "[VERIFY_QUEUE] enqueue failed user_id=%s type=%s err_class=%s collection=%s err=%s",
            user_id,
            verify_type,
            e.__class__.__name__,
            tg_verification_queue_col.name,         
            e,
        )
    return False, "db_error"

def _verification_backoff_seconds(attempts: int) -> int:
    if attempts <= 0:
        return VERIFY_QUEUE_BACKOFF_BASE_SECONDS
    backoff = VERIFY_QUEUE_BACKOFF_BASE_SECONDS * (2 ** (attempts - 1))
    return min(backoff, VERIFY_QUEUE_BACKOFF_MAX_SECONDS)

def process_verification_queue(batch_limit: int = 50) -> None:
    scanned = dequeued = processed = failed = errors = 0
    now = now_utc()

    for _ in range(batch_limit):
        try:
            claimed_doc = tg_verification_queue_col.find_one_and_update(
                {
                    "status": "queued",
                    "$or": [
                        {"next_attempt_at": {"$exists": False}},
                        {"next_attempt_at": {"$lte": now}},
                    ],
                },
                {
                    "$set": {"status": "processing", "updated_at": now},
                    "$inc": {"attempts": 1},
                },
                sort=[("created_at", ASCENDING)],            
                return_document=ReturnDocument.AFTER,
            )
        except Exception as exc:
            try:
                current_app.logger.exception("[VERIFY_QUEUE] scan_failed err=%s", exc)
            except Exception:
                print(f"[VERIFY_QUEUE] scan_failed err={exc}")
            break
         
        if not claimed_doc:
            break
        scanned += 1
        dequeued += 1
        uid = claimed_doc.get("user_id")
        vtype = (claimed_doc.get("type") or "").strip().lower()
        _safe_log(
            "info",
            "[VERIFY_QUEUE] dequeue_ok user_id=%s type=%s",
            uid,
            vtype or "unknown",
        )

        if uid is None:
            now = now_utc()
            last_error = "missing_user_id"         
            try:
                tg_verification_queue_col.update_one(
                    {"_id": claimed_doc["_id"]},
                    {
                        "$set": {
                            "status": "failed",
                            "updated_at": now,
                            "last_error": last_error,
                        },
                        "$unset": {"next_attempt_at": ""},
                        }
                )
            except Exception:
                try:
                    current_app.logger.exception(
                        "[VERIFY_QUEUE] process_fail user_id=None err=%s",
                        last_error,
                    )
                except Exception:
                    print(f"[VERIFY_QUEUE] process_fail user_id=None err={last_error}")
                raise
            failed += 1
            _safe_log(
                "info",
                "[VERIFY_QUEUE] process_fail user_id=None err=%s",
                last_error,
            )
            continue     
        try:
            result = "ok"
            if vtype == "pic":
                has_photo = _has_profile_photo(uid, force_refresh=True)
                if not has_photo:
                    result = "missing_profile_photo"

            now = now_utc()
            update_doc = {
                "status": "done",
                "updated_at": now,
                "last_error": None,
                "payload": {
                    "result": result,
                    "verified_at": now,
                },
            }
            tg_verification_queue_col.update_one(
                {"_id": claimed_doc["_id"]},
                {"$set": update_doc, "$unset": {"next_attempt_at": ""}},
            )
            processed += 1
            _safe_log(
                "info",
                "[VERIFY_QUEUE] process_ok user_id=%s result=%s",
                uid,
                result,
            )
        except Exception as exc:
            now = now_utc()
            errors += 1
            attempts = int(claimed_doc.get("attempts", 1))
            if attempts >= VERIFY_QUEUE_MAX_ATTEMPTS:
                update = {
                    "$set": {
                        "status": "failed",
                        "updated_at": now,
                        "last_error": str(exc),
                    },
                    "$unset": {"next_attempt_at": ""},
                }
            else:
                backoff_seconds = _verification_backoff_seconds(attempts)
                next_attempt = now + timedelta(seconds=backoff_seconds)
                update = {
                    "$set": {
                        "status": "queued",
                        "updated_at": now,
                        "last_error": str(exc),
                        "next_attempt_at": next_attempt,
                    }
                }
            try:
                tg_verification_queue_col.update_one(
                    {"_id": claimed_doc["_id"]},
                    update,
                )
            except Exception:
                try:
                    current_app.logger.exception(
                        "[VERIFY_QUEUE] process_fail user_id=%s err=%s",
                        uid,
                        exc,
                    )
                except Exception:
                    print(f"[VERIFY_QUEUE] process_fail user_id={uid} err={exc}")
                raise
            failed += 1
            _safe_log(
                "exception",
                "[VERIFY_QUEUE] process_fail user_id=%s err=%s",
                uid,
                exc,
            )
    _safe_log(
        "info",
        "[VERIFY_QUEUE] scanned=%s dequeued=%s processed=%s failed=%s errors=%s",
        scanned,
        dequeued,
        processed,
        failed,
        errors,
    )
     
ALLOWED_CHANNEL_STATUSES = {"member", "administrator", "creator"}
SUB_CHECK_TTL_SECONDS = int(os.getenv("SUB_CHECK_TTL_SECONDS", "120"))


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

def welcome_eligibility(uid: int | None, *, ref: datetime | None = None) -> tuple[bool, str, dict | None]:
    # Welcome eligibility relies on joined_main_at being within 7 days and an active ticket.
    # users_collection may already contain a doc while the user is still eligible.
    # Missing/old joined_main_at or claimed/expired tickets permanently hides the welcome drop.
    if uid is None:
        reason = "no_uid"
        current_app.logger.info("[WELCOME][ELIG] deny uid=%s reason=%s", uid, reason)
        return (False, reason, None)

    window = _welcome_window_for_user(uid, ref=ref)
    if window is None:
        reason = "not_in_welcome_window"
        current_app.logger.info("[WELCOME][ELIG] deny uid=%s reason=%s", uid, reason)
        return (False, reason, None)

    ticket = get_or_issue_welcome_ticket(uid)
    if not ticket:
        reason = "no_ticket"
        current_app.logger.info("[WELCOME][ELIG] deny uid=%s reason=%s", uid, reason)
        return (False, reason, None)

    status = ticket.get("status")
    if status in ("claimed", "expired"):
        reason = f"ticket_{status}"
        current_app.logger.info("[WELCOME][ELIG] deny uid=%s reason=%s", uid, reason)
        return (False, reason, ticket)

    now_ref = (ref or now_kl()).astimezone(KL_TZ)
    expires_at = _as_aware_kl(ticket.get("expires_at"))
    if expires_at and now_ref > expires_at:
        welcome_tickets_col.update_one(
            {"uid": uid},
            {"$set": {"status": "expired", "reason_last_fail": "expired"}},
        )
        ticket["status"] = "expired"
        reason = "ticket_expired"
        current_app.logger.info("[WELCOME][ELIG] deny uid=%s reason=%s", uid, reason)
        return (False, reason, ticket)

    current_app.logger.info("[WELCOME][ELIG] ok uid=%s exp=%s", uid, ticket.get("expires_at"))
    return (True, "ok", ticket)

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
                },
                "$setOnInsert": {"first_subscribed_at_utc": now},
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

    if mode == "welcome_new_user_7d":
        allowed, reason, _ticket = welcome_eligibility(uid)
        if not allowed:
            _log(False, f"welcome_{reason}")
            return False
    elif mode == "tier":
        # Normalize both stored tiers and user status so matching is case/spacing safe.
        normalized_allow = []
        for item in allow:
            norm = _normalize_tier_value(item)
            if norm:
                normalized_allow.append(norm)
        if not normalized_allow or _normalize_tier_value(user_status) not in normalized_allow:
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

def _normalize_tier_value(value) -> str:
    # Tier normalization is intentionally strict and minimal: trim + uppercase only.
    return (str(value) if value is not None else "").strip().upper()


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
    raw_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8] if raw else "none"
    decoded_hash = hashlib.sha256(decoded.encode("utf-8")).hexdigest()[:8] if decoded else "none"
    print(f"[initdata] raw_len={len(raw)} raw_sha={raw_hash}")
    print(f"[initdata] decoded_len={len(decoded)} decoded_sha={decoded_hash}")
 
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

    try:
        user_id = int(user.get("id"))
        date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        miniapp_sessions_daily_col.update_one(
            {"date_utc": date_utc, "user_id": user_id},
            {"$setOnInsert": {"date_utc": date_utc, "user_id": user_id}},
            upsert=True,
        )
    except Exception:
        pass

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

def _is_my_region(region: str | None) -> bool:
    if not region:
        return False
    r = str(region).strip().lower()
    return r in ("my", "malaysia")

def _normalize_region(region: str | None) -> str:
    if not region:
        return ""
    return str(region).strip().lower()

def _region_matches_pool(user_region: str | None, pool: str) -> bool:
    if pool == "public":
        return True
    if not user_region:
        return False
    user_norm = _normalize_region(user_region)
    pool_norm = _normalize_region(pool)
    if _is_my_region(user_region):
        return pool_norm in ("my", "malaysia")
    return user_norm == pool_norm

def _drop_pool_remaining_fields(drop: dict) -> dict[str, str]:
    fields = {"public": "public_remaining"}
    if not isinstance(drop, dict):
        return fields
    for key in drop.keys():
        if not isinstance(key, str):
            continue
        if key.endswith("_remaining") and key != "public_remaining":
            pool = key[: -len("_remaining")]
            if pool:
                fields[pool.lower()] = key
    return fields

def _pool_remaining(drop: dict, pool: str) -> int:
    field = _drop_pool_remaining_fields(drop).get(pool, "")
    if not field:
        return 0
    return max(0, int((drop or {}).get(field) or 0))

def get_claimable_pools(user_region: str | None, drop: dict) -> list[str]:
    fields = _drop_pool_remaining_fields(drop)
    reserved = [pool for pool in fields.keys() if pool != "public" and _region_matches_pool(user_region, pool)]
    return reserved + ["public"]

def get_visible_pools(user_region: str | None, drop: dict, *, uid: int | None = None) -> list[str]:
    fields = _drop_pool_remaining_fields(drop)
    visible = ["public"]
    for pool in fields.keys():
        if pool == "public":
            continue
        if _region_matches_pool(user_region, pool):
            visible.append(pool)
            continue
        remaining = _pool_remaining(drop, pool)
        if remaining > 0:
            try:
                current_app.logger.info(
                    "[VISIBILITY] exclude pool=%s reason=region_mismatch uid=%s",
                    pool.upper(),
                    uid,
                )
            except Exception:
                print(f"[VISIBILITY] exclude pool={pool.upper()} reason=region_mismatch uid={uid}")
    return visible

def _compute_visible_remaining(user_region: str | None, drop: dict, *, uid: int | None = None) -> int:
    return sum(_pool_remaining(drop, pool) for pool in get_visible_pools(user_region, drop, uid=uid))

def _compute_claimable_remaining(user_region: str | None, drop: dict) -> int:
    return sum(_pool_remaining(drop, pool) for pool in get_claimable_pools(user_region, drop))
 
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

    referral_events_col = db["referral_events"]
    user_doc = None
    if ctx_uid is not None:
        user_doc = users_collection.find_one({"user_id": ctx_uid})
    if not user_doc and usernameLower:
        user_doc = users_collection.find_one({"usernameLower": usernameLower})
    user_region = user_doc.get("region") if user_doc else None
    is_my_user = _is_my_region(user_region)
    claim_user_id = None
    if ctx_uid is not None:
        claim_user_id = ctx_uid
    elif user_id:
        try:
            claim_user_id = int(user_id)
        except (TypeError, ValueError):
            claim_user_id = None
         
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

        if _is_new_joiner_audience(audience_type):
            allowed, reason, _ticket = welcome_eligibility(ctx_uid, ref=ref)
            if not allowed:
                current_app.logger.info("[visible][WELCOME] hide uid=%s reason=%s", ctx_uid, reason)
                continue
    
        if not is_drop_allowed(d, ctx_uid, usernameLower or uname, user_ctx):
            continue

        if not is_user_eligible_for_drop(user_doc, tg_user or {}, d):
            continue    

        priority = d.get("priority", 100)     
        base = {
            "dropId": drop_id,
            "name": d.get("name"),
            "startsAt": _as_aware_utc(d.get("startsAt")).isoformat() if d.get("startsAt") else None,
            "endsAt": _as_aware_utc(d.get("endsAt")).isoformat() if d.get("endsAt") else None,
            "isActive": is_active,
            "userClaimed": False,
            "_priority": priority,
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
            public_remaining = max(0, int(d.get("public_remaining") or 0))
            my_remaining = max(0, int(d.get("my_remaining") or 0))
            visible_remaining = public_remaining + my_remaining if is_my_user else public_remaining
            already = None
            if claim_user_id is not None:
                already = voucher_claims_col.find_one(
                    {"drop_id": _coerce_id(d.get("_id")), "user_id": claim_user_id, "status": "claimed"}
                )
            if already:
                base["userClaimed"] = True
                base["code"] = already.get("voucher_code")
                claimed_at = _isoformat_kl(already.get("claimed_at"))
                if claimed_at:
                    base["claimedAt"] = claimed_at       
                base["visible_remaining"] = visible_remaining
                pooled_cards.append(base)
            else:
                if is_my_user or public_remaining > 0:
                    base["visible_remaining"] = visible_remaining
                    pooled_cards.append(base)

    # Sort: personalised first; then pooled by priority desc, startsAt asc
    personal_cards.sort(key=lambda x: (-x["_priority"], x["startsAt"]))
    pooled_cards.sort(key=lambda x: (-x["_priority"], x["startsAt"]))

    for card in personal_cards + pooled_cards:
        card.pop("_priority", None)

    # Stacked: return all (cap optional)
    return personal_cards + pooled_cards, user_region

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

def reconcile_pooled_remaining(drop_id: str) -> dict:
    drop_id_variants = _drop_id_variants(drop_id)
    actual_free_public = db.vouchers.count_documents({
        "type": "pooled",
        "dropId": {"$in": drop_id_variants},
        "status": "free",
        "$or": [{"pool": "public"}, {"pool": {"$exists": False}}],
    })
    actual_free_my = db.vouchers.count_documents({
        "type": "pooled",
        "dropId": {"$in": drop_id_variants},
        "status": "free",
        "pool": "my",
    })
    db.drops.update_one(
        {"_id": _coerce_id(drop_id)},
        {"$set": {"public_remaining": actual_free_public, "my_remaining": actual_free_my}},
    )
    try:
        current_app.logger.info(
            "[pool_reconcile] drop=%s public=%s my=%s",
            drop_id,
            actual_free_public,
            actual_free_my,
        )
    except Exception:
        print(f"[pool_reconcile] drop={drop_id} public={actual_free_public} my={actual_free_my}")
    return {"actual_free_public": actual_free_public, "actual_free_my": actual_free_my}

def claim_pooled(drop_id: str, claim_key: str, ref: datetime, *, pools: list[str] | None = None):
    drop_id_variants = _drop_id_variants(drop_id)
    drop_obj_id = _coerce_id(drop_id)

    # Idempotent: if already claimed, return same code
    existing = db.vouchers.find_one({
        "type": "pooled",
        "dropId": {"$in": drop_id_variants},
        "$or": [
            {"claimedByKey": claim_key},
            {"claimedBy": claim_key},
        ]
    })
    if existing:
        return {"ok": True, "code": existing["code"], "claimedAt": _isoformat_kl(existing.get("claimedAt"))}

    pool_order = pools or ["public"]
    reconciled = False
 
    for pool in pool_order:
        if pool == "my":
            remaining_field = "my_remaining"
            criteria = {
                "type": "pooled",
                "dropId": {"$in": drop_id_variants},
                "status": "free",
                "pool": "my",
            }
        elif pool == "public":
            remaining_field = "public_remaining"
            criteria = {
                "type": "pooled",
                "dropId": {"$in": drop_id_variants},
                "status": "free",
                # legacy vouchers missing "pool" are treated as public
                "$or": [{"pool": "public"}, {"pool": {"$exists": False}}],
            }
        else:
            continue

        # 1) Fast pre-check: if remaining counter already 0, skip querying vouchers.
        #    (Avoids pointless voucher query work during peak)
        try:
            rem = int(db.drops.find_one({"_id": drop_obj_id}, projection={remaining_field: 1}).get(remaining_field, 0))
        except Exception:
            rem = 0
        if rem <= 0:
            continue

        # 2) Try to claim a voucher FIRST (real scarce resource).
        doc = db.vouchers.find_one_and_update(
            criteria,
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
        if not doc and not reconciled:
            reconciliation = reconcile_pooled_remaining(drop_id)
            reconciled = True
            actual_remaining = (
                reconciliation["actual_free_my"]
                if pool == "my"
                else reconciliation["actual_free_public"]
            )
            if actual_remaining > 0:
                doc = db.vouchers.find_one_and_update(
                    criteria,
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
        if not doc:
            # No free voucher in this pool, try next pool
            continue

        # 3) Now decrement drop-level remaining counter.
        #    If we can't decrement (counter is stale/0), rollback voucher to free.
        updated = db.drops.update_one(
            {"_id": drop_obj_id, remaining_field: {"$gt": 0}},
            {"$inc": {remaining_field: -1}}
        )

        if updated.modified_count != 1:
            # Rollback voucher claim to avoid counter/voucher mismatch
            try:
                db.vouchers.update_one(
                    {"_id": doc["_id"], "status": "claimed", "claimedByKey": claim_key},
                    {
                        "$set": {"status": "free"},
                        "$unset": {"claimedBy": "", "claimedByKey": "", "claimedAt": ""}
                    }
                )
            except Exception:
                # If rollback fails, log it; better to surface sold_out than hand out inconsistent state
                try:
                    current_app.logger.exception("[claim][rollback_failed] drop=%s pool=%s", drop_id, pool)
                except Exception:
                    print(f"[claim][rollback_failed] drop={drop_id} pool={pool}")
            # Try next pool (or end)
            continue

        return {"ok": True, "code": doc["code"], "claimedAt": _isoformat_kl(doc.get("claimedAt"))}

    return {"ok": False, "err": "sold_out"}
 
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

        init_hash = hashlib.sha256(init_data.encode("utf-8")).hexdigest()[:8] if init_data else "none"     
        print(
            "[initdata] source=query_string_raw "
            f"len={len(init_data)} sha={init_hash}"
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
                    public_remaining = max(0, int(d.get("public_remaining") or 0))
                    base["remainingApprox"] = public_remaining
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
        try:
            record_visible_ping(uid)
            record_onboarding_start(uid, source="visible")
        except Exception as exc:
            current_app.logger.warning(
                "[E0][TELEMETRY][WARN] uid=%s err=%s",
                uid,
                exc,
            )
     
        drops, user_region = user_visible_drops(user, ref, tg_user=tg_user)
        return jsonify({
            "visibilityMode": "stacked",
            "nowUtc": ref.isoformat(),
            "userRegion": user_region,
            "drops": drops,
        }), 200
     
    except Exception:
        current_app.logger.exception("[visible] unhandled", extra={"user_id": user_id})
        raise

@vouchers_bp.route("/referral/progress", methods=["GET"])
@vouchers_bp.route("/v2/miniapp/referral/progress", methods=["GET"])
def api_referral_progress():
    init_data = extract_raw_init_data_from_query(request)
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

    user = _ctx_to_user(ctx or {})
    raw_user = ctx.get("user") if isinstance(ctx, dict) else None
    if isinstance(raw_user, str):
        try:
            raw_user = json.loads(raw_user)
        except Exception:
            raw_user = {}
    elif not isinstance(raw_user, dict):
        raw_user = {}

    uid = None
    username_lower = user.get("usernameLower") or ""
    if user.get("source") == "telegram":
        user_id = (user.get("userId") or "").strip()
        if not user_id:
            return jsonify({"code": "auth_failed", "why": "missing_or_invalid_init_data"}), 401
        try:
            uid = int(raw_user.get("id") or user_id)
        except Exception:
            uid = None

    if not uid and not username_lower:
        return jsonify({"code": "auth_failed", "why": "missing_or_invalid_init_data"}), 401

    referral_events_col = db["referral_events"]
    user_doc = None
    if uid is not None:
        user_doc = users_collection.find_one(
            {"user_id": uid},
            {
                "total_referrals": 1,
                "weekly_referrals": 1,
                "monthly_referrals": 1,
                "snapshot_updated_at": 1,
                "referral_generated_at": 1,
                "user_id": 1,
            },
        )
    if not user_doc and username_lower:
        user_doc = users_collection.find_one(
            {"usernameLower": username_lower},
            {
                "total_referrals": 1,
                "weekly_referrals": 1,
                "monthly_referrals": 1,
                "snapshot_updated_at": 1,
                "referral_generated_at": 1,
                "user_id": 1,
            },
        )
    if not user_doc and username_lower:
        user_doc = users_collection.find_one(
            {"username": {"$regex": f"^{username_lower}$", "$options": "i"}},
            {
                "total_referrals": 1,
                "weekly_referrals": 1,
                "monthly_referrals": 1,
                "snapshot_updated_at": 1,
                "referral_generated_at": 1,
                "user_id": 1,
            },
        )

    now_ref = as_aware_utc(datetime.now(timezone.utc))
    inviter_id = uid if uid is not None else (user_doc or {}).get("user_id")
    snapshot_total = int((user_doc or {}).get("total_referrals", 0) or 0)
    snapshot_weekly = int((user_doc or {}).get("weekly_referrals", 0) or 0)
    snapshot_monthly = int((user_doc or {}).get("monthly_referrals", 0) or 0)
    snapshot_ts = as_aware_utc((user_doc or {}).get("snapshot_updated_at"))
    snapshot_age_sec = None if not snapshot_ts else max(0, int((now_ref - snapshot_ts).total_seconds()))

    source = "snapshot"
    fallback_reason = None
    if inviter_id is not None:
        if not user_doc:
            source = "ledger"
            fallback_reason = "snapshot_missing"
        elif snapshot_total <= 0:
            settled_exists = referral_events_col.count_documents(
                {"inviter_id": inviter_id, "event": "referral_settled"},
                limit=1,
            )
            if int(settled_exists) > 0:
                source = "ledger"
                fallback_reason = "mismatch_detected"
        elif snapshot_ts is not None and snapshot_age_sec is not None and snapshot_age_sec > REFERRAL_SNAPSHOT_STALE_SEC:
            ledger_total = max(
                0,
                int(referral_events_col.count_documents({"inviter_id": inviter_id, "event": "referral_settled"}))
                - int(referral_events_col.count_documents({"inviter_id": inviter_id, "event": "referral_revoked"})),
            )
            if ledger_total != snapshot_total:
                source = "ledger"
                fallback_reason = "snapshot_stale"

    if source == "ledger" and inviter_id is not None:
        stats = _ledger_referral_counts(referral_events_col, int(inviter_id), now_ref)
    else:
        stats = {
            "total_referrals": max(0, snapshot_total),
            "weekly_referrals": max(0, snapshot_weekly),
            "monthly_referrals": max(0, snapshot_monthly),
        }

    total_referrals = int(stats.get("total_referrals", 0))
    weekly_referrals = int(stats.get("weekly_referrals", 0))
    monthly_referrals = int(stats.get("monthly_referrals", 0))
    current_app.logger.info(
        "[REF_PROGRESS] uid=%s source=%s reason=%s total=%s weekly=%s monthly=%s snapshot_age_sec=%s",
        inviter_id,
        source,
        fallback_reason or "",
        total_referrals,
        weekly_referrals,
        monthly_referrals,
        snapshot_age_sec,
    )

    milestone_size = 3
    progress = total_referrals % milestone_size
    remaining = milestone_size if progress == 0 else milestone_size - progress
    near_miss = progress == milestone_size - 1
    progress_pct = (progress / milestone_size) * 100

    link_expires_in_seconds = None
    latest_link_doc = None
    if inviter_id is not None:
        latest_link_doc = invite_link_map_collection.find_one(
            {"inviter_id": inviter_id},
            sort=[("created_at", -1)],
        )

    created_at = None
    if latest_link_doc and latest_link_doc.get("created_at"):
        created_at = latest_link_doc.get("created_at")
    elif user_doc and user_doc.get("referral_generated_at"):
        created_at = user_doc.get("referral_generated_at")

    if created_at:
        created_at_utc = as_aware_utc(created_at)
        now_utc_value = as_aware_utc(now_utc())
        if created_at_utc and now_utc_value:
            elapsed = (now_utc_value - created_at_utc).total_seconds()
            remaining_seconds = max(0.0, 86400 - elapsed)
            link_expires_in_seconds = min(86400.0, remaining_seconds)  # cap at 24h

    return jsonify(
        {
            "total_referrals": total_referrals,
            "weekly_referrals": weekly_referrals,
            "monthly_referrals": monthly_referrals,
            "milestone_size": milestone_size,
            "progress": progress,
            "remaining": remaining,
            "near_miss": near_miss,
            "progress_pct": progress_pct,
            "link_expires_in_seconds": link_expires_in_seconds,
        }
    ), 200


@vouchers_bp.route("/drop_mode", methods=["GET"])
@vouchers_bp.route("/v2/miniapp/drop_mode", methods=["GET"])
def api_drop_mode():
    init_data = extract_raw_init_data_from_query(request)
    if not init_data:
        return _miniapp_no_store_json({"status": "error", "code": "missing_init_data"}, 400)

    ok, parsed, reason = verify_telegram_init_data(init_data)
    ctx, admin_preview = _user_ctx_or_preview(
        request, init_data_raw=init_data, verification=(ok, parsed, reason)
    )

    if not ok and not admin_preview:
        return _miniapp_no_store_json({"code": "auth_failed", "why": str(reason)}, 401)
    if not admin_preview and not ctx:
        return _miniapp_no_store_json({"code": "auth_failed", "why": "missing_or_invalid_init_data"}, 401)

    uid_for_log = _ctx_to_user(ctx or {}).get("userId") or "unknown"

    now = now_utc()
    with _DROP_MODE_CACHE_LOCK:
        expires_at = _DROP_MODE_CACHE.get("expires_at")
        payload = _DROP_MODE_CACHE.get("payload")
        if payload and expires_at and now < expires_at:
            cached = dict(payload)
            cached["server_time_utc"] = now.isoformat()
            current_app.logger.info(
                "[DROP_MODE] hit uid=%s status=%s reason=%s",
                uid_for_log,
                "enabled" if cached.get("drop_mode") else "disabled",
                cached.get("reason") or "",
            )
            return _miniapp_no_store_json(cached, 200)

    active_drop = db.drops.find_one(
        {
            "startsAt": {"$lte": now},
            "endsAt": {"$gt": now},
        },
        sort=[("priority", DESCENDING), ("startsAt", DESCENDING)],
    )

    drop_mode = False
    reason_value = ""
    active_drop_id = None
    until_iso = None

    if active_drop:
        drop_type = str(active_drop.get("type") or "pooled").strip().lower()
        if drop_type == "pooled":
            public_remaining = max(0, int(active_drop.get("public_remaining") or 0))
            my_remaining = max(0, int(active_drop.get("my_remaining") or 0))
            total_remaining = max(0, int(active_drop.get("remaining") or 0))
            if (public_remaining + my_remaining + total_remaining) <= 0:
                active_drop = None

    if active_drop:
        starts_at = _as_aware_utc(active_drop.get("startsAt"))
        ends_at = _as_aware_utc(active_drop.get("endsAt"))
        if starts_at and ends_at:
            drop_mode_until = min(starts_at + timedelta(minutes=10), ends_at)
            if now < drop_mode_until:
                drop_mode = True
                reason_value = "active_drop_first_10m"
                active_drop_id = str(active_drop.get("_id"))
                until_iso = drop_mode_until.isoformat()

    payload = {
        "status": "ok",
        "drop_mode": drop_mode,
        "reason": reason_value,
        "active_drop_id": active_drop_id,
        "until_utc": until_iso,
        "server_time_utc": now.isoformat(),
    }

    with _DROP_MODE_CACHE_LOCK:
        _DROP_MODE_CACHE["payload"] = {
            "status": payload["status"],
            "drop_mode": payload["drop_mode"],
            "reason": payload["reason"],
            "active_drop_id": payload["active_drop_id"],
            "until_utc": payload["until_utc"],
            "server_time_utc": payload["server_time_utc"],
        }
        _DROP_MODE_CACHE["expires_at"] = now + timedelta(seconds=DROP_MODE_CACHE_TTL_SECONDS)

    global _DROP_MODE_LAST_LOGGED_UNTIL
    if drop_mode and until_iso and _DROP_MODE_LAST_LOGGED_UNTIL != until_iso:
        _DROP_MODE_LAST_LOGGED_UNTIL = until_iso
        current_app.logger.info("[DROP_MODE] ON drop_id=%s until=%s", active_drop_id, until_iso)
    elif not drop_mode:
        _DROP_MODE_LAST_LOGGED_UNTIL = None

    current_app.logger.info(
        "[DROP_MODE] hit uid=%s status=%s reason=%s",
        uid_for_log,
        "enabled" if payload.get("drop_mode") else "disabled",
        payload.get("reason") or "",
    )

    return _miniapp_no_store_json(payload, 200)

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
        user_doc = users_collection.find_one({"user_id": uid_int}, {"restrictions": 1, "region": 1})
    if not user_doc:
        uname = norm_username(username)
        if uname:
            user_doc = users_collection.find_one({"usernameLower": uname}, {"restrictions": 1, "region": 1})

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
    user_region = user_doc.get("region") if user_doc else None
    pools = get_claimable_pools(user_region, drop)
    res = claim_pooled(drop_id=drop_id, claim_key=claim_key, ref=ref, pools=pools)
    if res.get("ok"):
        return {"code": res["code"], "claimedAt": res["claimedAt"]}
    if res.get("err") == "sold_out":
        raise NoCodesLeft("sold_out")
    raise NotEligible("not_eligible")


@vouchers_bp.route("/vouchers/claim", methods=["POST"])
def api_claim():
    # Accept both header names + query param
    body = request.get_json(silent=True) or {}
 
    drop_id = body.get("dropId") or body.get("drop_id")
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)}) if drop_id else {}
    drop_type = drop.get("type", "pooled")

    init_data = extract_raw_init_data_from_query(request)

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
    session_key = _derive_session_key(
        init_data_raw=init_data,
        uid=uid,
        auth_date=(data or {}).get("auth_date"),
        query_id=(data or {}).get("query_id"),
    )
 
    user_doc = None
    if uid is not None:
        user_doc = users_collection.find_one({"user_id": uid})
    if not user_doc and uname:
        user_doc = users_collection.find_one({"usernameLower": uname})
    user_region = user_doc.get("region") if user_doc else None
    is_my_user = _is_my_region(user_region)
 
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
        return jsonify({
            "status": "error",
            "code": "not_eligible",
            "ok": False,
            "eligible": False,
            "reason": "missing_username",
            "checks_key": None,
        }), 403

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
        return jsonify({
            "status": "error",
            "code": "not_eligible",
            "ok": False,
            "eligible": False,
            "reason": "not_eligible",
            "checks_key": None,
        }), 403

    user_ctx = load_user_context(uid=uid, username=username, username_lower=uname or username)

    if not is_drop_allowed(voucher, uid, uname, user_ctx):
        return jsonify({
            "status": "error",
            "code": "not_eligible",
            "ok": False,
            "eligible": False,
            "reason": "not_eligible",
            "checks_key": None,
        }), 403

    if not is_user_eligible_for_drop(user_doc, tg_user, voucher):
        return jsonify({
            "status": "error",
            "code": "not_eligible",
            "ok": False,
            "eligible": False,
            "reason": "not_eligible",
            "checks_key": None,
        }), 403
     
    if not username:
        username = fallback_username or ""
   
    if not user_id:
        user_id = fallback_user_id or username or ""

    audience_type = _drop_audience_type(voucher)
    check_only = bool(body.get("check_only") or body.get("checkOnly")) 
    client_ip = _get_client_ip(request)
    client_subnet = _compute_subnet_key(client_ip) 
    is_pool_drop = _is_pool_drop(voucher, audience_type)
    current_app.logger.info(
        "[claim][client_ip] remote_addr=%s fly_client_ip=%s x_forwarded_for=%s x_real_ip=%s chosen_ip=%s subnet=%s",
        request.remote_addr,
        request.headers.get("Fly-Client-IP"),
        request.headers.get("X-Forwarded-For"),
        request.headers.get("X-Real-IP"),
        client_ip,
        client_subnet,
    )
 
    if is_pool_drop and not _is_new_joiner_audience(audience_type):
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
                "ok": False,
                "eligible": False,
                "reason": "not_subscribed",
                "checks_key": None,             
                "message": "Please subscribe to @advantplayofficial to claim this voucher."
            }), 403

    if check_only:
        return jsonify({"status": "ok", "check_only": True, "subscribed": True}), 200

    if is_pool_drop:
        claimable_remaining = _compute_claimable_remaining(user_region, voucher)
        if claimable_remaining <= 0:
            return jsonify({"status": "error", "code": "sold_out"}), 410     

    if _should_enforce_session_cooldown(client_subnet, client_ip):
        session_now = now_utc()
        session_ok, _, session_retry = _check_session_cooldown(
            session_key=session_key,
            now=session_now,
        )
        if not session_ok:
            return jsonify(_session_cooldown_payload(session_retry)), 429
        _set_session_cooldown(session_key=session_key, now=session_now)
 
    claim_drop_id = _coerce_id(drop_id)
    claim_user_id = uid if uid is not None else user_id
    existing_claim = voucher_claims_col.find_one({"drop_id": claim_drop_id, "user_id": claim_user_id})
    idempotent_payload = _build_idempotent_claim_response(existing_claim)
    if idempotent_payload:
        current_app.logger.info(
            "[claim][IDEMPOTENT_RETURN] drop=%s uid=%s code=%s",
            drop_id,
            uid,
            (existing_claim or {}).get("voucher_code"),
        )
        return jsonify(idempotent_payload), 200

    kill_ok, kill_reason, kill_retry = _check_kill_switch(
        ip=client_ip,
        subnet=client_subnet,
        now=now_utc(),
    )
    if not kill_ok:
        return jsonify({
            "status": "error",
            "code": "rate_limited",
            "reason": kill_reason,
            "message": f"Please try again in {kill_retry} seconds.",
        }), 429

    cooldown_ok, cooldown_reason, cooldown_retry = _check_cooldown(
        ip=client_ip,
        subnet=client_subnet,
        uid=claim_user_id,     
        now=now_utc(),
    )
    if not cooldown_ok:
        return jsonify({
            "status": "error",
            "code": "rate_limited",
            "reason": cooldown_reason,
            "message": f"Please try again in {cooldown_retry} seconds.",
        }), 429
  
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
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "not_supported_for_audience",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403
        if uid is None:
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=missing_uid",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )         
            current_app.logger.info("[claim] deny drop=%s reason=missing_uid", drop_id)
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "missing_uid",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403
        now_ts = now_kl()
        allowed, reason, _ticket = welcome_eligibility(uid, ref=now_ts)
        if not allowed:
            current_app.logger.info("[claim] deny drop=%s uid=%s reason=%s", drop_id, uid, reason)
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": reason,
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403
        cache_has_photo, cache_stale = _profile_photo_cache_status(uid)
        if cache_stale:
            ok, _err = enqueue_verification(
                user_id=uid,
                verify_type="pic",
                payload={"source": "welcome_claim"},
            )
            if not ok:
                return jsonify({
                    "ok": False,
                    "reason": "verify_enqueue_failed",
                    "message": "Verification temporarily unavailable. Please try again later.",
                }), 503
            return jsonify({
                "ok": True,
                "status": "verification_in_progress",
                "reason": "verify_pic",
            }), 202
        if not cache_has_photo:
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
               "eligible": False,             
               "reason": "missing_profile_photo",
               "checks_key": "pic",             
               "error_code": "NO_PROFILE_PIC",
               "message": "Please set a Telegram profile picture to claim the Welcome Bonus."
            }), 403         
        current_app.logger.info("[WELCOME] claim_attempt uid=%s drop_id=%s", uid, drop_id)

        if new_joiner_claims_col.find_one({"uid": uid}):
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=already_claimed_lifetime",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )  
            current_app.logger.info("[claim] deny drop=%s uid=%s reason=already_claimed_lifetime", drop_id, uid)
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "already_claimed_lifetime",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403

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
            return jsonify({
                "status": "error",
                "code": "rate_limited",
                "reason": rl_reason,
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 429

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
            return jsonify({
                "status": "error",
                "code": "rate_limited",
                "reason": "ip_claims_per_day",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 429

        ticket = get_or_issue_welcome_ticket(uid)
        if not ticket:
            current_app.logger.info(
                "[claim] uid=%s drop_id=%s dtype=%s audience=%s decision=blocked reason=not_eligible",
                uid,
                drop_id,
                drop_type,
                audience_type,
            )       
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "not_eligible",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403

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
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "expired",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403

    # Claim lock (idempotent per drop+user) before issuing code
    claim_now = now_utc()
    user_agent = request.headers.get("User-Agent", "")
    claim_doc_id, existing_claim = _acquire_claim_lock(
        drop_id=claim_drop_id,
        user_id=claim_user_id,
        client_ip=client_ip,
        user_agent=user_agent,
        now=claim_now,
    )
    if claim_doc_id is None:
        idempotent_payload = _build_idempotent_claim_response(existing_claim)
        if idempotent_payload:
            current_app.logger.info(
                "[claim][IDEMPOTENT_RETURN] drop=%s uid=%s code=%s",
                drop_id,
                uid,
                (existing_claim or {}).get("voucher_code"),
            )
            return jsonify(idempotent_payload), 200
        current_app.logger.info("[claim][DEDUP] drop=%s uid=%s", drop_id, uid)
        return jsonify({"status": "error", "code": "already_claimed"}), 409

    # Claim
    try:
        result = claim_voucher_for_user(user_id=user_id, drop_id=drop_id, username=username)
    except AlreadyClaimed as exc:
        voucher_claims_col.update_one(
            {"_id": claim_doc_id},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": now_utc()}},
        )
        return jsonify({"status": "error", "code": "already_claimed"}), 409
    except NoCodesLeft as exc:
        voucher_claims_col.update_one(
            {"_id": claim_doc_id},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": now_utc()}},
        )
        payload = {"status": "error", "code": "sold_out"}
        if is_my_user:
            payload["message"] = "This voucher pool is fully redeemed. Please refresh."
        else:
            payload["message"] = "All vouchers have been fully redeemed."
        return jsonify(payload), 410
    except NotEligible as exc:
        voucher_claims_col.update_one(
            {"_id": claim_doc_id},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": now_utc()}},
        )
        return jsonify({
            "status": "error",
            "code": "not_eligible",
            "ok": False,
            "eligible": False,
            "reason": "not_eligible",
            "checks_key": None,
        }), 403
    except Exception as exc:
        voucher_claims_col.update_one(
            {"_id": claim_doc_id},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": now_utc()}},
        )
        current_app.logger.exception("claim failed")
        return jsonify({"status": "error", "code": "server_error"}), 500

    else:
        voucher_claims_col.update_one(
            {"_id": claim_doc_id},
            {
                "$set": {
                    "status": "claimed",
                    "voucher_code": result.get("code"),
                    "claimed_at": now_utc(),
                    "updated_at": now_utc(),
                }
            },
        )
        current_app.logger.info("[claim][OK] drop=%s uid=%s code=%s", drop_id, uid, result.get("code"))
        _apply_kill_success(ip=client_ip, subnet=client_subnet, now=now_utc())
        _set_cooldown(ip=client_ip, subnet=client_subnet, uid=claim_user_id, now=now_utc())      
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
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "already_claimed_lifetime",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403

        ip_claim_count = _increment_ip_claim_success(ip=client_ip)
        if ip_claim_count > 3:
            current_app.logger.info("[claim] deny post-claim drop=%s uid=%s reason=ip_claims_per_day ip=%s", drop_id, uid, client_ip)
            return jsonify({
                "status": "error",
                "code": "rate_limited",
                "reason": "ip_claims_per_day",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 429

        welcome_tickets_col.update_one(
            {"uid": uid},
            {"$set": {"status": "claimed", "claimed_at": now_kl(), "reason_last_fail": None}},
        )
        if not uid:
            return jsonify({
                "status": "error",
                "code": "not_eligible",
                "reason": "missing_uid",
                "ok": False,
                "eligible": False,
                "checks_key": None,
            }), 403     
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
            # Persist normalized tiers so future checks are consistent.
            val = _normalize_tier_value(item)
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
        drop_doc["public_remaining"] = 0
        drop_doc["my_remaining"] = 0     
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
        pool_value = data.get("pool") or data.get("voucherPool") or "public"
        if pool_value not in ("public", "my"):
            return jsonify({"status": "error", "code": "bad_request", "reason": "bad_pool"}), 400     
        codes = _normalize_codes(data.get("codes"))
        if not codes:
            return jsonify({"status": "error", "code": "bad_request", "reason": "empty_codes"}), 400     
        docs = []
        for c in codes:
            docs.append({
                "type": "pooled",
                "dropId": str(drop_id),
                "code": c,
                "status": "free",
                "claimedBy": None,
                "claimedAt": None,
                "pool": pool_value,
            })
        if docs:
            try:
                result = db.vouchers.insert_many(docs, ordered=False)
            except DuplicateKeyError:
                return jsonify({"status": "error", "code": "duplicate_code"}), 409
            except BulkWriteError as exc:
                # ordered=False may throw BulkWriteError on duplicates; normalize to 409
                details = exc.details or {}
                write_errors = details.get("writeErrors") or []
                dup_count = sum(1 for err in write_errors if err.get("code") == 11000)
                if dup_count:
                    inserted = details.get("nInserted") or 0
                    if inserted:
                        remaining_field = "public_remaining" if pool_value == "public" else "my_remaining"
                        db.drops.update_one({"_id": drop_id}, {"$inc": {remaining_field: inserted}})                 
                    return jsonify({
                        "status": "error",
                        "code": "duplicate_code",
                        "inserted": inserted,
                        "duplicates": dup_count,
                    }), 409
                try:
                    current_app.logger.exception("[admin][drops] insert_many_failed")
                except Exception:
                    print("[admin][drops] insert_many_failed")
                return jsonify({"status": "error", "code": "server_error"}), 500
            else:
                inserted = len(result.inserted_ids)
                if inserted:
                    remaining_field = "public_remaining" if pool_value == "public" else "my_remaining"
                    db.drops.update_one({"_id": drop_id}, {"$inc": {remaining_field: inserted}})

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
        total_public = db.vouchers.count_documents({
            "type": "pooled",
            "dropId": {"$in": drop_id_variants},
            "$or": [{"pool": "public"}, {"pool": {"$exists": False}}],
        })
        total_my = db.vouchers.count_documents({
            "type": "pooled",
            "dropId": {"$in": drop_id_variants},
            "pool": "my",
        })
        free_public = db.vouchers.count_documents({
            "type": "pooled",
            "dropId": {"$in": drop_id_variants},
            "status": "free",
            "$or": [{"pool": "public"}, {"pool": {"$exists": False}}],
        })
        free_my = db.vouchers.count_documents({
            "type": "pooled",
            "dropId": {"$in": drop_id_variants},
            "status": "free",
            "pool": "my",
        })
        summary.update({
            "codesTotal": total,
            "codesFree": free,
            "codesTotalPublic": total_public,
            "codesTotalMy": total_my,
            "codesFreePublic": free_public,
            "codesFreeMy": free_my,
        })

    for key in ("whitelistUsernames", "visibilityMode"):
        if key in doc:
            summary[key] = doc[key]

    return summary



@vouchers_bp.route("/admin/pools/upload", methods=["POST"])
def admin_pools_upload_v2():
    _, err = require_admin()
    if err:
        return err

    data = request.get_json(silent=True) or {}
    pool_id = str(data.get("pool_id") or "").strip().upper()
    if pool_id not in {"WELCOME", "T1", "T2", "T3", "T4"}:
        return jsonify({"status": "error", "reason": "bad_pool_id"}), 400

    codes_text = str(data.get("codes_text") or "")
    rows = [line.strip() for line in codes_text.replace("\r", "\n").split("\n") if line.strip()]
    if not rows:
        return jsonify({"status": "error", "reason": "empty_codes"}), 400

    now_ts = datetime.now(timezone.utc)
    inserted = 0
    for code in rows:
        try:
            db.voucher_pools.insert_one(
                {
                    "pool_id": pool_id,
                    "code": code,
                    "status": "available",
                    "issued_to": None,
                    "issued_at": None,
                    "ledger_id": None,
                    "display_label": data.get("display_label"),
                    "value_hint": data.get("value_hint"),
                    "currency": data.get("currency"),
                    "created_at": now_ts,
                }
            )
            inserted += 1
        except DuplicateKeyError:
            continue

    return jsonify({"status": "ok", "inserted": inserted, "received": len(rows), "pool_id": pool_id})


@vouchers_bp.route("/admin/pools/summary", methods=["GET"])
def admin_pools_summary_v2():
    _, err = require_admin()
    if err:
        return err

    out = []
    for pool_id in ("WELCOME", "T1", "T2", "T3", "T4"):
        available = db.voucher_pools.count_documents({"pool_id": pool_id, "status": "available"})
        issued = db.voucher_pools.count_documents({"pool_id": pool_id, "status": "issued"})
        sample = db.voucher_pools.find_one({"pool_id": pool_id}, {"display_label": 1, "value_hint": 1, "currency": 1}) or {}
        out.append(
            {
                "pool_id": pool_id,
                "available": int(available),
                "issued": int(issued),
                "display_label": sample.get("display_label"),
                "value_hint": sample.get("value_hint"),
                "currency": sample.get("currency"),
            }
        )
    return jsonify({"status": "ok", "items": out})


@vouchers_bp.route("/admin/affiliate/kpis", methods=["GET"])
def admin_affiliate_kpis_v2():
    _, err = require_admin()
    if err:
        return err

    days = request.args.get("days", default=14, type=int)
    if days is None:
        days = 14
    days = max(1, min(int(days), 60))

    rows = list(db.affiliate_daily_kpis.find({}, {"_id": 0}).sort("day_utc", -1).limit(days))

    with_sub_cache = 0
    row_days = sorted({str(r.get("day_utc")) for r in rows if r.get("day_utc")})

    if row_days:
        min_day = datetime.fromisoformat(row_days[-1]).replace(tzinfo=timezone.utc)
        max_day = datetime.fromisoformat(row_days[0]).replace(tzinfo=timezone.utc) + timedelta(days=1)

        invitee_ids = set()
        try:
            cursor = db.pending_referrals.find(
                {
                    "created_at_utc": {"$gte": min_day, "$lt": max_day}
                },
                {"invitee_user_id": 1},
            )
            for ref in cursor:
                iid = ref.get("invitee_user_id")
                if iid is not None:
                    invitee_ids.add(iid)

            if invitee_ids:
                with_sub_cache = subscription_cache_col.count_documents(
                    {"user_id": {"$in": list(invitee_ids)}}
                )
        except Exception:
            with_sub_cache = 0

    out_rows = []
    for row in rows:
        item = dict(row)
        computed_at = item.get("computed_at_utc")
        if isinstance(computed_at, datetime):
            item["computed_at_utc"] = computed_at.isoformat()
        raw_sub_rate = item.get("invitee_channel_sub_72h_rate", 0.0)
        try:
            item["invitee_channel_sub_72h_rate"] = f"{(float(raw_sub_rate or 0.0) * 100.0):.1f}%"
        except (TypeError, ValueError):
            item["invitee_channel_sub_72h_rate"] = "0.0%"
        out_rows.append(item)
    current_app.logger.info("[ADMIN_AFF_KPIS] days=%s rows=%s with_sub_cache=%s", days, len(out_rows), with_sub_cache)
    return jsonify({"days": days, "rows": out_rows})


@vouchers_bp.route("/admin/metrics/daily", methods=["GET"])
def admin_metrics_daily_v2():
    admin_user, err = require_admin()
    if err:
        return err

    days = request.args.get("days", default=14, type=int)
    if days is None:
        days = 14
    days = max(1, min(int(days), 60))

    now_utc = datetime.now(timezone.utc)
    start_day = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc) - timedelta(days=days - 1)

    rows = []
    for offset in range(days):
        day_start = start_day + timedelta(days=offset)
        day_end = day_start + timedelta(days=1)
        date_utc = day_start.strftime("%Y-%m-%d")
        rows.append(
            {
                "date": date_utc,
                "miniapp_unique_users": int(miniapp_sessions_daily_col.count_documents({"date_utc": date_utc})),
                "voucher_claims_issued": int(
                    db.voucher_ledger.count_documents(
                        {"status": "issued", "created_at": {"$gte": day_start, "$lt": day_end}}
                    )
                ),
                "first_checkins": int(
                    users_collection.count_documents(
                        {"first_checkin_at": {"$gte": day_start, "$lt": day_end}}
                    )
                ),
                "qualified_events": int(
                    db.qualified_events.count_documents(
                        {"created_at": {"$gte": day_start, "$lt": day_end}}
                    )
                ),
                "affiliate_rewards_issued": int(
                    db.affiliate_ledger.count_documents(
                        {"status": "issued", "created_at": {"$gte": day_start, "$lt": day_end}}
                    )
                ),
            }
        )

    print(f"[ADMIN_METRICS] days={days} by_uid={admin_user.get('id')} rows={len(rows)}")
    return jsonify({"days": days, "tz": "UTC", "rows": rows})


@vouchers_bp.route("/admin/affiliate/pending", methods=["GET"])
def admin_affiliate_pending_v2():
    _, err = require_admin()
    if err:
        return err

    status = str(request.args.get("status") or "PENDING_REVIEW").strip().upper()
    if status not in {"PENDING_REVIEW", "PENDING_MANUAL", "SIMULATED_PENDING"}:
        return jsonify({"status": "error", "reason": "bad_status"}), 400

    rows = list(db.affiliate_ledger.find({"status": status}).sort("created_at", 1).limit(200))
    items = []
    for row in rows:
        items.append(
            {
                "ledger_id": str(row.get("_id")),
                "user_id": row.get("user_id"),
                "year_month": row.get("year_month"),
                "tier": row.get("tier"),
                "pool_id": row.get("pool_id"),
                "status": row.get("status"),
                "risk_flags": row.get("risk_flags") or [],
                "simulate": bool(row.get("simulate")),
                "would_issue_pool": row.get("would_issue_pool"),
                "gate_day": row.get("gate_day"),
                "still_in_group": row.get("still_in_group"),
                "xp_total": row.get("xp_total"),
                "monthly_xp": row.get("monthly_xp"),
                "abuse_flags": row.get("abuse_flags") or [],
                "evaluated_at_utc": row.get("evaluated_at_utc").isoformat() if row.get("evaluated_at_utc") else None,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            }
        )
    return jsonify({"status": "ok", "items": items})


@vouchers_bp.route("/admin/affiliate/<ledger_id>/approve", methods=["POST"])
def admin_affiliate_approve_v2(ledger_id):
    _, err = require_admin()
    if err:
        return err

    try:
        oid = ObjectId(ledger_id)
    except Exception:
        return jsonify({"status": "error", "reason": "bad_ledger_id"}), 400

    ledger = approve_affiliate_ledger(db, ledger_id=oid, now_utc=datetime.now(timezone.utc))
    if not ledger:
        return jsonify({"status": "error", "reason": "not_found"}), 404
    return jsonify({"status": "ok", "ledger_status": ledger.get("status"), "voucher_code": ledger.get("voucher_code")})


@vouchers_bp.route("/admin/affiliate/<ledger_id>/reject", methods=["POST"])
def admin_affiliate_reject_v2(ledger_id):
    _, err = require_admin()
    if err:
        return err

    try:
        oid = ObjectId(ledger_id)
    except Exception:
        return jsonify({"status": "error", "reason": "bad_ledger_id"}), 400

    data = request.get_json(silent=True) or {}
    reject_affiliate_ledger(db, ledger_id=oid, reason=data.get("reason"), now_utc=datetime.now(timezone.utc))
    return jsonify({"status": "ok"})

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
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404
    dtype = drop.get("type", "pooled")
    if dtype != "pooled":
        return jsonify({"status": "error", "code": "bad_request", "reason": "bad_type"}), 400
    pool_value = data.get("pool") or data.get("voucherPool") or "public"
    if pool_value not in ("public", "my"):
        return jsonify({"status": "error", "code": "bad_request", "reason": "bad_pool"}), 400

    codes = _normalize_codes(data.get("codes"))
    if not codes:
        return jsonify({"status": "error", "code": "bad_request", "reason": "empty_codes"}), 400

    docs = []
    for c in codes:
        docs.append({
            "type": "pooled",
            "dropId": str(drop["_id"]),
            "code": c,
            "status": "free",
            "claimedBy": None,
            "claimedAt": None,
            "pool": pool_value,
        })
    try:
        result = db.vouchers.insert_many(docs, ordered=False)
    except DuplicateKeyError:
        return jsonify({"status": "error", "code": "duplicate_code"}), 409
    except BulkWriteError as exc:
        # ordered=False may throw BulkWriteError on duplicates; normalize to 409
        details = exc.details or {}
        write_errors = details.get("writeErrors") or []
        dup_count = sum(1 for err in write_errors if err.get("code") == 11000)
        if dup_count:
            inserted = details.get("nInserted") or 0
            if inserted:
                remaining_field = "public_remaining" if pool_value == "public" else "my_remaining"
                db.drops.update_one({"_id": drop["_id"]}, {"$inc": {remaining_field: inserted}})         
            return jsonify({
                "status": "error",
                "code": "duplicate_code",
                "inserted": inserted,
                "duplicates": dup_count,
            }), 409
        try:
            current_app.logger.exception("[admin][drops] append_codes_failed")
        except Exception:
            print("[admin][drops] append_codes_failed")
        return jsonify({"status": "error", "code": "server_error"}), 500

    inserted = len(result.inserted_ids)
    if inserted:
        remaining_field = "public_remaining" if pool_value == "public" else "my_remaining"
        db.drops.update_one({"_id": drop["_id"]}, {"$inc": {remaining_field: inserted}})
    return jsonify({"status": "ok", "dropId": str(drop["_id"]), "inserted": len(result.inserted_ids)}) 

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


# Manual test checklist:
# 1) Create pooled drop with pool="my" and 2 codes => 200
# 2) Append pool="public" codes => inserted count returned
# 3) Append with a duplicate code => get 409 duplicate_code (not 500)
# 4) Admin drop summary shows codesTotalPublic includes legacy pool-missing rows
