from flask import Blueprint, request, jsonify, current_app
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from bson.objectid import ObjectId 
from datetime import datetime, timedelta, timezone
from config import KL_TZ
import hmac, hashlib, urllib.parse, os, json
from urllib.parse import parse_qs
import config as _cfg 
 
from database import db
 
admin_cache_col = db["admin_cache"]

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

def _get_init_data(req) -> str:
    return (
        req.headers.get("X-Telegram-Init-Data")
        or req.headers.get("X-Telegram-Init")
        or req.args.get("init_data", "")
    )

def _verify_telegram_init_data(init_data: str) -> dict | None:
    """DOES NOT crash if tokens are missing; simply returns None."""
    if not init_data:
        return None
    try:
        data_map = {k: v[0] for k, v in parse_qs(init_data, keep_blank_values=True).items()}
        received_hash = data_map.pop("hash", "")
        data_check_pairs = [f"{k}={data_map[k]}" for k in sorted(data_map.keys())]
        data_check_string = "\n".join(data_check_pairs)

        def ok_for_token(token: str) -> bool:
            if not token:
                return False
            secret_key = hashlib.sha256(token.encode()).digest()
            h = hmac.new(secret_key, msg=data_check_string.encode(), digestmod=hashlib.sha256).hexdigest()
            return h == received_hash

        tokens = [t for t in [_BOT_TOKEN] + _BOT_TOKEN_FALLBACKS if t]
        return data_map if any(ok_for_token(t) for t in tokens) else None
    except Exception:
        return None

def _user_ctx_or_preview(req):
    """
    Returns (ctx: dict, admin_preview: bool) or (None, False) if unauthorized.
    SAFE: will not throw if env/config is missing.
    """
    # Admin preview?
    secret = _get_admin_secret(req)
    if _admin_secret_ok(secret):
        return ({"user_id": 0, "username": "admin-preview"}, True)
    # Telegram path
    parsed = _verify_telegram_init_data(_get_init_data(req))
    if not parsed:
        return (None, False)
    return (parsed, False)
    
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
    # Prevent multiple rows for the same user in a personalised drop
    db.vouchers.create_index(
        [("type", ASCENDING), ("dropId", ASCENDING), ("usernameLower", ASCENDING)],
        unique=True,
        name="uniq_personalised_assignment"
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
 
def norm_username(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()

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

    parsed = urllib.parse.parse_qs(init_data_raw or "", keep_blank_values=True)
    provided_hash = parsed.get("hash", [""])[0]
    if not provided_hash:
        return False, {}, "missing_hash"

    # Build the data_check_string from all params except 'hash' and 'signature'
    pairs = []
    for k in sorted(parsed.keys()):
        if k in ("hash", "signature"):
            continue
        v = parsed[k][0]
        pairs.append(f"{k}={v}")
    data_check_string = "\n".join(pairs)

    candidates = _candidate_bot_tokens()

    primary_env = (os.environ.get("BOT_TOKEN") or "").strip()
    if primary_env and primary_env not in candidates:
        candidates.append(primary_env)

    fallbacks_env = os.environ.get("BOT_TOKEN_FALLBACKS", "")
    for t in (x.strip() for x in fallbacks_env.split(",") if x.strip()):
        if t not in candidates:
            candidates.append(t)

    if not candidates:
        return False, {}, "bot_token_missing"

    ok = False
    for tok in candidates:
        secret_key = hashlib.sha256(tok.encode()).digest()
        calc = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(calc, provided_hash):
            ok = True
            break

    if not ok:
        return False, {}, "bad_signature"

    # Optional freshness check (24h)
    try:
        auth_date = int(parsed.get("auth_date", ["0"])[0])
        if time.time() - auth_date > 24 * 3600:
            return False, {}, "expired_auth_date"
    except Exception:
        pass

    return True, {k: v[0] for k, v in parsed.items()}, "ok"
    
def require_admin():
    if BYPASS_ADMIN:
        # allow everything; pretend it's an admin user
        return {"usernameLower": "bypass_admin"}, None

    if _has_valid_admin_secret():
        return _payload_from_admin_secret(), None

    init_data = (
        request.args.get("init_data")
        or request.headers.get("X-Telegram-Init")
        or request.headers.get("X-Telegram-Init-Data")
        or ""
    )
    ok, data, reason = verify_telegram_init_data(init_data)
    if not ok:
        reason_suffix = f"; reason={reason}" if reason else ""
        print(f"[admin] auth_failed; init_len: {len(init_data)}{reason_suffix}")
        return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

    # parse Telegram user
    try:
        user_json = json.loads(data.get("user", "{}"))
        if not isinstance(user_json, dict):
            user_json = {}
    except Exception:
        user_json = {}

    username_lower = norm_username(user_json.get("username", ""))
    user_id = user_json.get("id")

    is_admin, source = _is_cached_admin(user_json)
    admin_source = source

    if not is_admin:
        if user_id is not None and str(user_id) in _ADMIN_USER_IDS:
            is_admin = True
            admin_source = source or "env"

    if not is_admin:
        user_id_dbg = user_json.get("id")
        print(f"[admin] forbidden for: {username_lower} (id={user_id_dbg})")
        return None, (jsonify({"status": "error", "code": "forbidden"}), 403)

    try:
        user_id_val = int(user_id)
    except (TypeError, ValueError):
        user_id_val = user_id

    payload = {"usernameLower": username_lower}
    if user_id_val is not None:
        payload["id"] = user_id_val
    if admin_source:
        payload["adminSource"] = admin_source
        
    return payload, None
    
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

def user_visible_drops(user: dict, ref: datetime):
    usernameLower = user.get("usernameLower", "")
    drops = get_active_drops(ref)

    personal_cards = []
    pooled_cards = []

    for d in drops:
        drop_id = str(d["_id"])
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

        if dtype == "personalised":
            # user must have an unclaimed OR claimed row to show the card (claimed -> show claimed state)
            row = db.vouchers.find_one({
                "type": "personalised",
                "dropId": drop_id,
                "usernameLower": usernameLower
            })
            if row and is_active:
                base["userClaimed"] = (row.get("status") == "claimed")
                if base["userClaimed"]:
                    base["code"] = row.get("code")
                    claimed_at = row.get("claimedAt")
                    if claimed_at:
                        base["claimedAt"] = claimed_at.isoformat() if hasattr(claimed_at, "isoformat") else str(claimed_at)
                personal_cards.append(base)
        else:
            # pooled: if whitelist empty -> public; else usernameLower must be in whitelist
            wl_raw = d.get("whitelistUsernames") or []
            wl_norm = {norm_username(w) for w in wl_raw}  # strips @ and lowercases
            eligible = (len(wl_norm) == 0) or (usernameLower in wl_norm)
            if not eligible or not is_active:
                continue
            # Need at least one free code OR user already claimed (so they can see their code state)
            already = db.vouchers.find_one({
                "type": "pooled",
                "dropId": drop_id,
                "claimedBy": usernameLower
            })
            if already:
                base["userClaimed"] = True
                base["code"] = already.get("code")
                claimed_at = already.get("claimedAt")
                if claimed_at:
                    base["claimedAt"] = claimed_at.isoformat() if hasattr(claimed_at, "isoformat") else str(claimed_at)
                base["remainingApprox"] = max(0, db.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"}))
                pooled_cards.append(base)
            else:
                free_exists = db.vouchers.find_one({
                    "type": "pooled",
                    "dropId": drop_id,
                    "status": "free"
                }, projection={"_id": 1})
                if free_exists:
                    base["remainingApprox"] = max(0, db.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"}))
                    pooled_cards.append(base)

    # Sort: personalised first; then pooled by priority desc, startsAt asc
    personal_cards.sort(key=lambda x: (-x["priority"], x["startsAt"]))
    pooled_cards.sort(key=lambda x: (-x["priority"], x["startsAt"]))

    # Stacked: return all (cap optional)
    return personal_cards + pooled_cards

# ---- Claim handlers ----
def claim_personalised(drop_id: str, usernameLower: str, ref: datetime):
    # Return existing if already claimed
    existing = db.vouchers.find_one({
        "type": "personalised",
        "dropId": drop_id,
        "usernameLower": usernameLower,
        "status": "claimed"
    })
    if existing:
        return {"ok": True, "code": existing["code"], "claimedAt": existing["claimedAt"].isoformat()}

    # Claim the assigned row atomically
    doc = db.vouchers.find_one_and_update(
        {
            "type": "personalised",
            "dropId": drop_id,
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
            "dropId": drop_id,
            "usernameLower": usernameLower
        })
        if already and already.get("status") == "claimed":
            return {"ok": True, "code": already["code"], "claimedAt": already["claimedAt"].isoformat()}
        return {"ok": False, "err": "not_eligible"}
    return {"ok": True, "code": doc["code"], "claimedAt": doc["claimedAt"].isoformat()}

def claim_pooled(drop_id: str, usernameLower: str, ref: datetime):
    # Idempotent: if already claimed, return same code
    existing = db.vouchers.find_one({
        "type": "pooled",
        "dropId": drop_id,
        "claimedBy": usernameLower
    })
    if existing:
        return {"ok": True, "code": existing["code"], "claimedAt": existing["claimedAt"].isoformat()}

    # Atomically reserve a free code
    doc = db.vouchers.find_one_and_update(
        {
            "type": "pooled",
            "dropId": drop_id,
            "status": "free"
        },
        {
            "$set": {
                "status": "claimed",
                "claimedBy": usernameLower,
                "claimedAt": ref
            }
        },
        sort=[("_id", ASCENDING)],
        return_document=ReturnDocument.AFTER
    )
    if not doc:
        return {"ok": False, "err": "sold_out"}
    return {"ok": True, "code": doc["code"], "claimedAt": doc["claimedAt"].isoformat()}

# ---- Public API routes ----
@vouchers_bp.route("/miniapp/vouchers/visible", methods=["GET"])
def api_visible():
    """
    GET /v2/miniapp/vouchers/visible
    - Normal users: only active drops
    - Admin preview (ADMIN_PANEL_SECRET / X-Admin-Secret): active-only by default; pass ?all=1 to include history
    """
    try:
        ref = now_utc()
        admin_preview = _is_admin_preview(request)

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
            cursor = db.drops.find(q).sort([("priority", DESCENDING), ("startsAt", ASCENDING)])
            for d in cursor:
                drop_id = str(d["_id"])
                starts_at = _as_aware_utc(d.get("startsAt")).isoformat() if d.get("startsAt") else None
                ends_at = _as_aware_utc(d.get("endsAt")).isoformat() if d.get("endsAt") else None
                base = {
                    "dropId": drop_id,
                    "name": d.get("name"),
                    "type": d.get("type", "pooled"),
                    "startsAt": starts_at,
                    "endsAt": ends_at,
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
            return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": items})

        # ---------- Normal users ----------
        user_ctx = _require_user_from_telegram(request)  # raises on 401
        user_id = user_ctx["user_id"]

        # Only include active drops, whitelisted for user if applicable
        q = {
            "status": {"$nin": ["expired", "paused"]},
            "startsAt": {"$lte": ref},
            "endsAt": {"$gt": ref},
        }
        items = []
        cursor = db.drops.find(q).sort([("priority", DESCENDING), ("startsAt", ASCENDING)])
        for d in cursor:
            if not _is_drop_visible_to_user(d, user_ctx):
                continue
            drop_id = str(d["_id"])
            starts_at = _as_aware_utc(d.get("startsAt")).isoformat() if d.get("startsAt") else None
            ends_at = _as_aware_utc(d.get("endsAt")).isoformat() if d.get("endsAt") else None
            base = {
                "dropId": drop_id,
                "name": d.get("name"),
                "type": d.get("type", "pooled"),
                "startsAt": starts_at,
                "endsAt": ends_at,
                "priority": d.get("priority", 100),
                "status": d.get("status", "upcoming"),
                "isActive": is_drop_active(d, ref),
                "userClaimed": _user_claimed_drop(user_id, drop_id),
            }
            if base["type"] == "pooled":
                free = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"})
                total = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id})
                base["remainingApprox"] = free
                base["codesTotal"] = total
            items.append(base)
        return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": items})

    except AuthError as e:
        return jsonify({"code": "auth_failed", "why": str(e)}), 401
    except Exception as e:
        # keep existing error style if you already have one
        print("[visible] unhandled:", repr(e))
        return jsonify({"code": "server_error"}), 500
 
    # Normal user path
    drops = user_visible_drops(user, ref)
    return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": drops})

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
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        raise NotEligible("drop_not_found")

    dtype = drop.get("type", "pooled")
    usernameLower = norm_username(username)
    ref = now_utc()

    if dtype == "personalised":
        res = claim_personalised(drop_id=drop_id, usernameLower=usernameLower, ref=ref)
        if res.get("ok"):
            return {"code": res["code"], "claimedAt": res["claimedAt"]}
        if res.get("err") == "not_eligible":
            raise NotEligible("not_eligible")
        raise AlreadyClaimed("already_claimed")

    # pooled
    res = claim_pooled(drop_id=drop_id, usernameLower=usernameLower, ref=ref)
    if res.get("ok"):
        return {"code": res["code"], "claimedAt": res["claimedAt"]}
    if res.get("err") == "sold_out":
        raise NoCodesLeft("sold_out")
    raise NotEligible("not_eligible")


@vouchers_bp.route("/miniapp/vouchers/claim", methods=["POST"])
def api_claim():
    # Accept both header names + query param
    init_data = (
        request.args.get("init_data")
        or request.headers.get("X-Telegram-Init")
        or request.headers.get("X-Telegram-Init-Data")
        or ""
    )
    ok, data, why = verify_telegram_init_data(init_data)

    # Admin preview (for Postman/admin panel testing)
    admin_secret = request.args.get("admin_secret") or request.headers.get("X-Admin-Secret")
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

    user_id = str(user_raw.get("id") or "")
    username = user_raw.get("username") or ""
    if not user_id:
        return jsonify({"status": "error", "code": "no_user"}), 400

    # Body
    body = request.get_json(silent=True) or {}
    drop_id = body.get("dropId") or body.get("drop_id")
    if not drop_id:
        return jsonify({"status": "error", "code": "missing_drop_id"}), 400

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
    return ObjectId(x) if ObjectId.is_valid(x) else x

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
        codes = data.get("codes") or []
        docs = []
        for c in codes:
            c = (str(c) or "").strip()
            if not c:
                continue
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
    
def _drop_to_json(d: dict) -> dict:
    out = dict(d)
    out["_id"] = str(out["_id"])
    for k in ("startsAt", "endsAt"):
        if k in out:
            norm = _as_aware_utc(out[k])
            out[k] = norm.isoformat() if norm else None
    return out

@vouchers_bp.route("/admin/drops_v2", methods=["GET"])
def list_drops_v2():
    """
    NEW endpoint for testing admin bearer/X-Admin-Secret without touching the old one.
    """
    if not _admin_secret_ok(_get_admin_secret(request)):
        return jsonify({"error": "unauthorized"}), 401

    items = []
    cursor = db.drops.find({}).sort([("priority", -1), ("startsAt", -1)])
    for d in cursor:
        items.append(_drop_to_json(d))
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
        codes = data.get("codes") or []
        docs = []
        for c in codes:
            c = (str(c) or "").strip()
            if not c:
                continue
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
        starts = _as_aware_utc(d.get("startsAt"))
        ends   = _as_aware_utc(d.get("endsAt"))

        status = d.get("status", "upcoming")
        if status not in ("paused", "expired"):
            if ends and ref >= ends:
                status = "expired"
            elif starts and starts <= ref < (ends or starts):
                status = "active"
            else:
                status = "upcoming"

        row = {
            "dropId": str(d["_id"]),
            "name": d.get("name"),
            "type": d.get("type", "pooled"),
            "status": status,
            "priority": d.get("priority", 100),
            "startsAt": starts.isoformat() if starts else None,
            "endsAt": ends.isoformat() if ends else None,
        }
        if row["type"] == "personalised":
            assigned = db.vouchers.count_documents({"type": "personalised", "dropId": row["dropId"]})
            claimed  = db.vouchers.count_documents({"type": "personalised", "dropId": row["dropId"], "status": "claimed"})
            row.update({"assigned": assigned, "claimed": claimed})
        else:
            total = db.vouchers.count_documents({"type": "pooled", "dropId": row["dropId"]})
            free  = db.vouchers.count_documents({"type": "pooled", "dropId": row["dropId"], "status": "free"})
            row.update({"codesTotal": total, "codesFree": free})

        items.append(row)

    return jsonify({"status": "ok", "items": items})

