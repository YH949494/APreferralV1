from flask import Blueprint, request, jsonify, current_app
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import OperationFailure
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone
from config import KL_TZ
import hmac, hashlib, urllib.parse, os, json
import config as _cfg 
 
from database import db, users_collection
 
admin_cache_col = db["admin_cache"]

BYPASS_ADMIN = os.getenv("BYPASS_ADMIN", "0").lower() in ("1", "true", "yes", "on")
HARDCODED_ADMIN_USERNAMES = {"gracy_ap", "teohyaohui"}  # allow manual overrides if cache is empty

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
_BOT_TOKEN = getattr(_cfg, "BOT_TOKEN", os.getenv("BOT_TOKEN", ""))
_BOT_TOKEN_FALLBACKS = getattr(
    _cfg, "BOT_TOKEN_FALLBACKS",
    [t.strip() for t in os.getenv("BOT_TOKEN_FALLBACKS", "").split(",") if t.strip()]
)
_ADMIN_USER_IDS = set()
try:
    raw_admin_ids = getattr(_cfg, "ADMIN_USER_IDS", os.getenv("ADMIN_USER_IDS", ""))
    if isinstance(raw_admin_ids, (list, tuple, set)):
        iter_ids = raw_admin_ids
    else:
        iter_ids = [x.strip() for x in str(raw_admin_ids).split(",") if x.strip()]
    for rid in iter_ids:
        _ADMIN_USER_IDS.add(str(rid))
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

def _extract_init_data(req, *, body: dict | None = None) -> str:
    body = body or {}

    for candidate in (
        req.headers.get("X-Telegram-Web-App-Data"),
        req.headers.get("X-Telegram-Init-Data"),
        req.headers.get("X-Telegram-Init"),
    ):
        if candidate:
            return str(candidate)

    # Extract raw query parameter without pre-decoding to avoid double unquoting
    raw_qs = (req.query_string or b"").decode("utf-8", "ignore")
    if raw_qs:
        for part in raw_qs.split("&"):
            if part.startswith("init_data="):
                return part.split("=", 1)[1]
            if part.startswith("initData="):
                return part.split("=", 1)[1]

    for key in ("init_data", "initData"):
        if key in body:
            try:
                return str(body.get(key) or "")
            except Exception:
                return ""

    return ""
 
def _extract_init_data_raw_from_query(request):
    qs = request.query_string or b""
    for key in (b"init_data=", b"initData="):
        i = qs.find(key)
        if i == -1:
            continue
        i += len(key)
        break
    else:
        return ""
    j = qs.find(b"&", i)
    val = qs[i:] if j == -1 else qs[i:j]
    try:
        return val.decode("utf-8", errors="strict")
    except Exception:
        return val.decode("utf-8", errors="ignore")

def _verify_telegram_init_data(init_data: str) -> dict | None:
    """Legacy helper used by the voucher blueprint for Telegram auth."""
    if not init_data:
        return None
     
    try:
        ok, parsed, _ = verify_telegram_init_data(init_data)

    except Exception:
        return None
     
    if not ok:
        return None

    cleaned = dict(parsed)
    cleaned.pop("hash", None)
    cleaned.pop("signature", None)
    return cleaned

def _user_ctx_or_preview(req):
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
  
# Telegram path
    parsed = _verify_telegram_init_data(_extract_init_data(req))
    if parsed:
        return (parsed, False)

    legacy = _legacy_user_ctx(req)
    if legacy:
        return (legacy, False)

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

def _candidate_bot_tokens():
    tokens = []
    seen = set()

    for token in [_BOT_TOKEN, *_BOT_TOKEN_FALLBACKS]:
        if not token or token in seen:
            continue
        tokens.append(token)
        seen.add(token)

    return tokens

def verify_telegram_init_data(init_data_raw: str):
    import urllib.parse, hmac, hashlib, time, os
 
    print("[initdata] verifier_v=spec_qsl")
 
    def _log(reason: str, extra: str = ""):
        suffix = f" {extra}".rstrip()
        print(f"[initdata] {reason}{suffix}")

    raw_init_data = init_data_raw or ""

    parsed_pairs = urllib.parse.parse_qsl(
        raw_init_data,
        keep_blank_values=True,
        strict_parsing=False,
    )
 
    parsed = {}
    provided_hash = ""
    for k, v in parsed_pairs:       
        parsed.setdefault(k, []).append(v)
        if not provided_hash and k == "hash":
            provided_hash = v
         
    print(
        f"[initdata] raw_len={len(raw_init_data)} has_user={'user' in parsed} "
        f"has_auth_date={'auth_date' in parsed} has_hash={'hash' in parsed} "
        f"has_sig={'signature' in parsed}"
    )
 
    if not provided_hash:
        _log("missing_hash")
        return False, {}, "missing_hash"

    provided_hash = provided_hash.strip()
    try:
        if len(provided_hash) != 64:
            raise ValueError("bad_len")
        int(provided_hash, 16)
    except Exception:
        _log("invalid_hash_format")
        return False, {}, "invalid_hash_format"

    provided_hash = provided_hash.lower()

    filtered_pairs = [
        (k, v) for k, v in parsed_pairs if k not in ("hash", "signature")
    ]
    filtered_pairs.sort(key=lambda kv: kv[0])
    data_check_string = "\n".join(f"{k}={v}" for k, v in filtered_pairs)

    sorted_keys = sorted({k for k, _ in filtered_pairs})
    print(f"[initdata] keys={sorted_keys}")
    print(f"[initdata] dcs_len={len(data_check_string)}")
    print(f"[initdata] dcs_preview={data_check_string[:120]}")
 
    candidates = _candidate_bot_tokens()

    primary_env = (os.environ.get("BOT_TOKEN") or "").strip()
    if primary_env and primary_env not in candidates:
        candidates.append(primary_env)

    fallbacks_env = os.environ.get("BOT_TOKEN_FALLBACKS", "")
    for t in (x.strip() for x in fallbacks_env.split(",") if x.strip()):
        if t not in candidates:
            candidates.append(t)

    if not candidates:
        _log("bot_token_missing")
        return False, {}, "bot_token_missing"

    ok = False
    reason = "hash_mismatch_final"
    computed_hash = ""

    data_bytes = data_check_string.encode()
    for idx, tok in enumerate(candidates):
        secret_key = hmac.new(b"WebAppData", tok.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_bytes, hashlib.sha256).hexdigest()
        print(
            "[initdata] hash_check",
            f"provided_hash_prefix={provided_hash[:8]}",
            f"calc_prefix={computed_hash[:8]}",
        )
        if hmac.compare_digest(computed_hash, provided_hash):
            ok = True
            reason = "ok"
            print(f"[initdata] using_bot_tail={tok[-4:]}")
            break
             
    if not ok:
        _log("hash_mismatch_final")
        return False, {}, reason

    print("[initdata] verification=OK")
 
    # Optional freshness check (24h)
    try:
        auth_date = int(parsed.get("auth_date", ["0"])[0])
        if time.time() - auth_date > 24 * 3600:
            _log("auth_date_expired")
            return False, {}, "expired_auth_date"
    except Exception:
        pass

    # Parse Telegram user JSON after integrity validation
    user_raw = parsed.get("user", ["{}"])[0]
    try:
        user_json = json.loads(user_raw)
    except Exception:
        try:
            user_json = json.loads(urllib.parse.unquote_plus(user_raw))
        except Exception:
            _log("user_parse_error")         
            user_json = {}

    if not isinstance(user_json, dict):
        user_json = {}

    user_id = str(user_json.get("id") or "").strip()
    if not user_id:
        _log("missing_user_id")
        return False, {}, "missing_user_id"

    parsed_flat = {k: v[0] for k, v in parsed.items()}
    parsed_flat["user"] = json.dumps(user_json)

    return True, parsed_flat, reason
 
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

    personal_cards = []
    pooled_cards = []

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
 
    # Only hide personalised vouchers when the caller truly has no username
    allow_personalised = bool(usernameLower)
    claim_key = usernameLower or user_id
 
    logged_hidden = False
 
    for d in drops:
        drop_id = str(d["_id"])
        drop_id_variants = _drop_id_variants(d.get("_id"))    
        dtype = d.get("type", "pooled")
        is_active = is_drop_active(d, ref)

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

    # Atomically reserve a free code
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
    if not doc:
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
    try:
        # Use the safe universal helper you already have
        ctx, admin_preview = _user_ctx_or_preview(request)
     
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
         
        drops = user_visible_drops(user, ref, tg_user=tg_user)
        return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": drops}), 200

    except Exception as e:
        print("[visible] unhandled:", repr(e))
        return jsonify({"code": "server_error", "message": str(e)}), 500

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

    raw_init = _extract_init_data_raw_from_query(request)
    init_data = raw_init

    print(
        f"[initdata] claim_source=query_string_raw "
        f"raw_len={len(init_data) if init_data else 0} "
        f"prefix={(init_data[:20] if init_data else '')}"
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
        # Some clients double-encode init_data, so the `user` field may still
        # contain percent-escape sequences (e.g. "%7B...%7D"). Try to decode
        # once more before giving up so we don't reject otherwise valid users.
        try:
            decoded_user = urllib.parse.unquote_plus(data.get("user", "{}"))
            user_raw = json.loads(decoded_user)
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
 
    if not username:
        username = fallback_username or ""
   
    if not user_id:
        user_id = fallback_user_id or username or ""

    # Claim
    try:
        result = claim_voucher_for_user(user_id=user_id, drop_id=drop_id, username=username)
    except AlreadyClaimed:
        return jsonify({"status": "error", "code": "already_claimed"}), 409
    except NoCodesLeft:
        return jsonify({"status": "error", "code": "sold_out"}), 410
    except NotEligible:
        return jsonify({"status": "error", "code": "not_eligible"}), 403
    except Exception:
        current_app.logger.exception("claim failed")
        return jsonify({"status": "error", "code": "server_error"}), 500

    return jsonify({"status": "ok", "voucher": result}), 200
    
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

    drop_doc = {
        "name": name,
        "type": dtype,
        "startsAt": startsAt,
        "endsAt": endsAt,
        "priority": priority,
        "visibilityMode": "stacked",
        "status": status
    }
    if dtype == "pooled":
        wl_raw = data.get("whitelistUsernames") or []
        wl_clean = []
        for item in wl_raw:
            u = norm_username(item)
            if u and u not in wl_clean:
                wl_clean.append(u)
        drop_doc["whitelistUsernames"] = wl_clean

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

