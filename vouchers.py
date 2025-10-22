from flask import Blueprint, request, jsonify
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from bson.objectid import ObjectId 
from datetime import datetime, timedelta, timezone
from config import KL_TZ
import hmac, hashlib, urllib.parse, os, json

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

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_PANEL_SECRET = os.environ.get("ADMIN_PANEL_SECRET", "")

vouchers_bp = Blueprint("vouchers", __name__)

BYPASS_ADMIN = False

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


def norm_username(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()



def _has_valid_admin_secret() -> bool:
    if not ADMIN_PANEL_SECRET:
        return False
    provided = (request.headers.get("X-Admin-Secret")
                or request.args.get("admin_secret")
                or "")
    if not provided:
        return False
    try:
        return hmac.compare_digest(str(provided), ADMIN_PANEL_SECRET)
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
    """Return (ok: bool, data: dict) using the global BOT_TOKEN."""
    try:
        if not init_data_raw or not BOT_TOKEN:
            return False, {}
        data = dict(urllib.parse.parse_qsl(init_data_raw, keep_blank_values=True))
        check_hash = data.pop("hash", None)
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
        secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()   # <- correct var
        calc_hash  = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        import hmac as _hmac
        return _hmac.compare_digest(calc_hash, check_hash), data
    except Exception:
        return False, {}
        
def require_admin():
    if BYPASS_ADMIN:
        # allow everything; pretend it's an admin user
        return {"usernameLower": "bypass_admin"}, None

    if _has_valid_admin_secret():
        return _payload_from_admin_secret(), None
    
    init_data = request.headers.get("X-Telegram-Init") or request.args.get("init_data") or ""
    ok, data = verify_telegram_init_data(init_data)
    if not ok:
        print("[admin] auth_failed; init_len:", len(init_data))
        return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

    # parse Telegram user
    try:
        user_json = json.loads(data.get("user", "{}"))
        if not isinstance(user_json, dict):
            user_json = {}
    except Exception:
        user_json = {}

    username_lower = norm_username(user_json.get("username", ""))

    is_admin, source = _is_cached_admin(user_json)
    if not is_admin:
        user_id = user_json.get("id")
        print(f"[admin] forbidden for: {username_lower} (id={user_id})")
        return None, (jsonify({"status": "error", "code": "forbidden"}), 403)

    user_id = user_json.get("id")
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        pass

    payload = {"usernameLower": username_lower}
    if user_id is not None:
        payload["id"] = user_id
    if source:
        payload["adminSource"] = source

    return payload, None
    
def _is_admin_preview(init_data_raw: str) -> bool:
    # Safe best-effort: if verify fails, just return False (don’t break visible)
    if _has_valid_admin_secret():
        return True
    ok, data = verify_telegram_init_data(init_data_raw)
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
    return (doc.get("status") != "paused" 
            and doc.get("status") != "expired"
            and doc["startsAt"] <= ref < doc["endsAt"])

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
            "startsAt": d["startsAt"].isoformat(),
            "endsAt": d["endsAt"].isoformat(),
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
    admin_secret_ok = _has_valid_admin_secret()
    if admin_secret_ok:
        init_data = ""
        user_raw = {
            "username": request.headers.get("X-Admin-Username")
                        or request.args.get("username"),
            "id": request.headers.get("X-Admin-User-Id")
                  or request.args.get("user_id"),
        }
    else:
        init_data = request.args.get("init_data") or request.headers.get("X-Telegram-Init") or ""
    ok, data = verify_telegram_init_data(init_data)

    # Admin preview fallback (mirror admin panel behavior)
    admin_secret = request.args.get("admin_secret") or request.headers.get("X-Admin-Secret")
    if not ok and admin_secret and admin_secret == os.environ.get("ADMIN_PANEL_SECRET"):
        # fabricate a minimal 'user' so UI can render
        data = {"user": {"id": int(os.environ.get("PREVIEW_USER_ID", "999"))}, "username": "admin_preview"}
        ok = True

    if not ok:
        # Optional: return empty OK to avoid scary toast in UI
        return jsonify({"status": "ok", "items": [], "note": "no_auth"}), 200
        # user object (username may be empty for some TG users)
        try:
            user_raw = json.loads(data.get("user", "{}"))
        except Exception:
            user_raw = {}

    user = {
        "usernameLower": norm_username((user_raw or {}).get("username", "")),
        "id": (user_raw or {}).get("id")
    }

    ref = now_utc()
    admin_preview = admin_secret_ok or _is_admin_preview(init_data)

    if admin_preview:
        # Return *all* drops, any status, ignoring whitelist; tag them.
        items = []
        for d in db.drops.find().sort([("priority", DESCENDING), ("startsAt", ASCENDING)]):
            drop_id = str(d["_id"])
            base = {
                "dropId": drop_id,
                "name": d.get("name"),
                "type": d.get("type", "pooled"),
                "startsAt": d["startsAt"].isoformat(),
                "endsAt": d["endsAt"].isoformat(),
                "priority": d.get("priority", 100),
                "status": d.get("status", "upcoming"),
                "isActive": is_drop_active(d, ref),
                "userClaimed": False,
                "adminPreview": True   # <-- tell the UI
            }
            # For pooled, show approx remaining
            if base["type"] == "pooled":
                free = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"})
                total = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id})
                base["remainingApprox"] = free
                base["codesTotal"] = total
            items.append(base)
        # Keep the same envelope
        return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": items})

    # normal user path
    drops = user_visible_drops(user, ref)
    return jsonify({"visibilityMode": "stacked", "nowUtc": ref.isoformat(), "drops": drops})

@vouchers_bp.route("/miniapp/vouchers/claim", methods=["POST"])
def api_claim():
    init_data = request.args.get("init_data") or request.headers.get("X-Telegram-Init") or ""
    ok, data = verify_telegram_init_data(init_data)
    if not ok:
        return jsonify({"status": "error", "code": "auth_failed"}), 401

    try:
        user_raw = json.loads(data.get("user", "{}"))
    except Exception:
        user_raw = {}

    user = {
        "id": user_raw.get("id"),
        "username": user_raw.get("username", ""),
        "usernameLower": norm_username(user_raw.get("username", "")),
        "first_name": user_raw.get("first_name", "")
    }

    body = request.get_json(silent=True) or {}
    drop_id = body.get("dropId")
    if not drop_id:
        return jsonify({"status": "error", "code": "bad_request"}), 400

    # Fetch drop & validate window
    drop = db.drops.find_one({"_id": ObjectId(drop_id)}) if ObjectId.is_valid(drop_id) else db.drops.find_one({"_id": drop_id})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404

    ref = now_utc()
    if drop.get("status") in ("paused", "expired") or not (drop["startsAt"] <= ref < drop["endsAt"]):
        return jsonify({"status": "error", "code": "not_active"}), 400

    dtype = drop.get("type", "pooled")
    usernameLower = user.get("usernameLower", "")

    if dtype == "personalised":
        out = claim_personalised(str(drop["_id"]), usernameLower, ref)
    else:
        # Pooled needs whitelist check (if any)
        wl = drop.get("whitelistUsernames") or []
        if len(wl) > 0:
            wl_norm = set([norm_username(x) for x in wl])
            if usernameLower not in wl_norm:
                # Also accept "@name" format
                wl_with_at = set([w[1:].lower() if w.startswith("@") else w.lower() for w in wl])
                if usernameLower not in wl_with_at:
                    return jsonify({"status": "error", "code": "not_eligible"}), 403
        out = claim_pooled(str(drop["_id"]), usernameLower, ref)

    if not out.get("ok"):
        return jsonify({"status": "error", "code": out.get("err", "unknown")}), 400

    return jsonify({
        "status": "ok",
        "dropId": str(drop["_id"]),
        "code": out["code"],
        "claimedAt": out["claimedAt"]
    })

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

    # personalised stats
    for d in db.drops.find().sort([("priority", DESCENDING), ("startsAt", ASCENDING)]):
        drop_id = str(d["_id"])
        dtype = d.get("type", "pooled")
        status = d.get("status", "upcoming")
        if status not in ("paused", "expired"):
            if d["endsAt"] <= ref:
                status = "expired"
            elif d["startsAt"] <= ref:
                status = "active"
            else:
                status = "upcoming"
        row = {
            "dropId": drop_id,
            "name": d.get("name"),
            "type": dtype,
            "status": status,
            "priority": d.get("priority", 100),
            "startsAt": d["startsAt"].isoformat(),
            "endsAt": d["endsAt"].isoformat(),
        }
        if dtype == "personalised":
            assigned = db.vouchers.count_documents({"type": "personalised", "dropId": drop_id})
            claimed  = db.vouchers.count_documents({"type": "personalised", "dropId": drop_id, "status": "claimed"})
            row.update({"assigned": assigned, "claimed": claimed})
        else:
            total = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id})
            free  = db.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"})
            row.update({"codesTotal": total, "codesFree": free})
        items.append(row)

    return jsonify({"status": "ok", "items": items})

