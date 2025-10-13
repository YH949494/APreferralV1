# vouchers.py
import hmac
import hashlib
import base64
import urllib.parse
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify, current_app
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId
import os
import re

vouchers_bp = Blueprint("vouchers", __name__)

# ---- Mongo helpers ----
def get_db():
    # If you already have a global client/db, import and reuse that instead.
    from pymongo import MongoClient
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    dbname = os.environ.get("MONGO_DB", "apbot")
    return client[dbname]

DB = get_db()

# ---- Time helpers ----
KL_TZ = timezone(timedelta(hours=8))

def now_utc():
    return datetime.now(timezone.utc)

def parse_kl_local(dt_str):
    # Expecting "YYYY-MM-DD HH:MM:SS"
    # Convert KL local time to UTC ISO
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=KL_TZ)
    return dt.astimezone(timezone.utc)

# ---- Username normaliser ----
def norm_username(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()

# ---- Telegram initData verification ----
def parse_init_data(raw: str) -> dict:
    # raw is the exact query string received from Telegram Mini App (initData)
    # Example: "user=%7B%22id%22%3A123%2C%22username%22%3A%22yaohui%22%7D&auth_date=..."
    pairs = urllib.parse.parse_qsl(raw, keep_blank_values=True)
    data = {k: v for k, v in pairs}
    return data

def verify_telegram_init_data(init_data_raw: str, bot_token: str) -> dict | None:
    """
    Verifies Telegram Mini App initData.
    Returns the `user` dict on success, else None.
    """
    if not init_data_raw or not bot_token:
        return None
    data = parse_init_data(init_data_raw)
    if "hash" not in data:
        return None
    recv_hash = data.pop("hash")
    # Build data_check_string sorted by key
    data_check_list = []
    for k in sorted(data.keys()):
        data_check_list.append(f"{k}={data[k]}")
    data_check_string = "\n".join(data_check_list)

    secret_key = hashlib.sha256(bot_token.encode()).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, recv_hash):
        return None

    # Extract user JSON
    user_json = data.get("user")
    if not user_json:
        return None
    try:
        import json
        user = json.loads(user_json)
        # normalize username
        user["usernameLower"] = norm_username(user.get("username", ""))
        return user
    except Exception:
        return None

# ---- Index bootstrap ----
def ensure_indexes():
    DB.drops.create_index([("startsAt", ASCENDING)])
    DB.drops.create_index([("endsAt", ASCENDING)])
    DB.drops.create_index([("priority", DESCENDING)])
    DB.drops.create_index([("status", ASCENDING)])

    # vouchers shared collection for both types
    DB.vouchers.create_index([("code", ASCENDING)], unique=True)
    DB.vouchers.create_index([("dropId", ASCENDING), ("type", ASCENDING), ("status", ASCENDING)])
    DB.vouchers.create_index([("dropId", ASCENDING), ("usernameLower", ASCENDING)])
    # For quick "already claimed by user" in pooled
    DB.vouchers.create_index([("dropId", ASCENDING), ("claimedBy", ASCENDING)])

ensure_indexes()

# ---- Core visibility logic ----
def is_drop_active(doc: dict, ref: datetime) -> bool:
    return (doc.get("status") != "paused" 
            and doc.get("status") != "expired"
            and doc["startsAt"] <= ref < doc["endsAt"])

def get_active_drops(ref: datetime):
    return list(DB.drops.find({
        "status": {"$nin": ["expired"]},
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
        base = {
            "dropId": drop_id,
            "name": d.get("name"),
            "type": dtype,
            "startsAt": d["startsAt"].isoformat(),
            "endsAt": d["endsAt"].isoformat(),
            "priority": d.get("priority", 100),
            "isActive": is_drop_active(d, ref),
            "userClaimed": False
        }

        if dtype == "personalised":
            # user must have an unclaimed OR claimed row to show the card (claimed -> show claimed state)
            row = DB.vouchers.find_one({
                "type": "personalised",
                "dropId": drop_id,
                "usernameLower": usernameLower
            })
            if row:
                base["userClaimed"] = (row.get("status") == "claimed")
                personal_cards.append(base)
        else:
            # pooled: if whitelist empty -> public; else usernameLower must be in whitelist
            wl = d.get("whitelistUsernames") or []
            eligible = (len(wl) == 0) or (("@" + usernameLower) in [w.lower() if not w.startswith("@") else w.lower() for w in [w.strip() for w in wl]] or usernameLower in [norm_username(w) for w in wl])
            if not eligible:
                continue
            # Need at least one free code OR user already claimed (so they can see their code state)
            already = DB.vouchers.find_one({
                "type": "pooled",
                "dropId": drop_id,
                "claimedBy": usernameLower
            })
            if already:
                base["userClaimed"] = True
                base["remainingApprox"] = max(0, DB.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"}))
                pooled_cards.append(base)
            else:
                free_exists = DB.vouchers.find_one({
                    "type": "pooled",
                    "dropId": drop_id,
                    "status": "free"
                }, projection={"_id": 1})
                if free_exists:
                    base["remainingApprox"] = max(0, DB.vouchers.count_documents({"type": "pooled", "dropId": drop_id, "status": "free"}))
                    pooled_cards.append(base)

    # Sort: personalised first; then pooled by priority desc, startsAt asc
    personal_cards.sort(key=lambda x: (-x["priority"], x["startsAt"]))
    pooled_cards.sort(key=lambda x: (-x["priority"], x["startsAt"]))

    # Stacked: return all (cap optional)
    return personal_cards + pooled_cards

# ---- Claim handlers ----
def claim_personalised(drop_id: str, usernameLower: str, ref: datetime):
    # Return existing if already claimed
    existing = DB.vouchers.find_one({
        "type": "personalised",
        "dropId": drop_id,
        "usernameLower": usernameLower,
        "status": "claimed"
    })
    if existing:
        return {"ok": True, "code": existing["code"], "claimedAt": existing["claimedAt"].isoformat()}

    # Claim the assigned row atomically
    doc = DB.vouchers.find_one_and_update(
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
        already = DB.vouchers.find_one({
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
    existing = DB.vouchers.find_one({
        "type": "pooled",
        "dropId": drop_id,
        "claimedBy": usernameLower
    })
    if existing:
        return {"ok": True, "code": existing["code"], "claimedAt": existing["claimedAt"].isoformat()}

    # Atomically reserve a free code
    doc = DB.vouchers.find_one_and_update(
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
    # Expect Telegram initData either in query param ?init_data=... or header X-Telegram-Init
    bot_token = os.environ.get("BOT_TOKEN", "")
    init_data = request.args.get("init_data") or request.headers.get("X-Telegram-Init")
    user = verify_telegram_init_data(init_data, bot_token)
    if not user:
        return jsonify({"status": "error", "code": "auth_failed"}), 401

    ref = now_utc()
    drops = user_visible_drops(user, ref)
    return jsonify({
        "visibilityMode": "stacked",
        "nowUtc": ref.isoformat(),
        "drops": drops
    })

@vouchers_bp.route("/miniapp/vouchers/claim", methods=["POST"])
def api_claim():
    bot_token = os.environ.get("BOT_TOKEN", "")
    init_data = request.args.get("init_data") or request.headers.get("X-Telegram-Init")
    user = verify_telegram_init_data(init_data, bot_token)
    if not user:
        return jsonify({"status": "error", "code": "auth_failed"}), 401

    body = request.get_json(silent=True) or {}
    drop_id = body.get("dropId")
    if not drop_id:
        return jsonify({"status": "error", "code": "bad_request"}), 400

    # Fetch drop & validate window
    drop = DB.drops.find_one({"_id": ObjectId(drop_id)}) if ObjectId.is_valid(drop_id) else DB.drops.find_one({"_id": drop_id})
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
    data = request.get_json(force=True)
    name = data.get("name")
    dtype = data.get("type", "pooled")
    startsAtLocal = data.get("startsAtLocal")
    if not (name and startsAtLocal):
        return jsonify({"status": "error", "code": "bad_request"}), 400

    startsAt = parse_kl_local(startsAtLocal)
    endsAt = startsAt + timedelta(hours=24)
    priority = int(data.get("priority", 100))

    drop_doc = {
        "name": name,
        "type": dtype,
        "startsAt": startsAt,
        "endsAt": endsAt,
        "priority": priority,
        "visibilityMode": "stacked",
        "status": "upcoming"
    }
    if dtype == "pooled":
        wl = data.get("whitelistUsernames") or []
        drop_doc["whitelistUsernames"] = wl

    res = DB.drops.insert_one(drop_doc)
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
            DB.vouchers.insert_many(docs, ordered=False)
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
            DB.vouchers.insert_many(docs, ordered=False)

    return jsonify({"status": "ok", "dropId": str(drop_id)})

@vouchers_bp.route("/admin/drops/<drop_id>/codes", methods=["POST"])
def admin_add_codes(drop_id):
    data = request.get_json(force=True)
    dtype = data.get("type")  # optional override
    drop = DB.drops.find_one({"_id": _coerce_id(drop_id)})
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
            DB.vouchers.insert_many(docs, ordered=False)
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
            DB.vouchers.insert_many(docs, ordered=False)

    return jsonify({"status": "ok"})

@vouchers_bp.route("/admin/drops/<drop_id>/whitelist", methods=["POST"])
def admin_update_whitelist(drop_id):
    data = request.get_json(force=True)
    mode = data.get("mode", "replace")
    usernames = data.get("usernames") or []
    drop = DB.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if drop.get("type") != "pooled":
        return jsonify({"status": "error", "code": "invalid_type"}), 400

    if mode == "append":
        new_list = list(set((drop.get("whitelistUsernames") or []) + usernames))
    else:
        new_list = usernames

    DB.drops.update_one({"_id": drop["_id"]}, {"$set": {"whitelistUsernames": new_list}})
    return jsonify({"status": "ok"})

@vouchers_bp.route("/admin/drops/<drop_id>/actions", methods=["POST"])
def admin_drop_actions(drop_id):
    data = request.get_json(force=True)
    op = data.get("op")
    drop = DB.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404

    if op == "start_now":
        now = now_utc()
        DB.drops.update_one({"_id": drop["_id"]}, {"$set": {"startsAt": now, "endsAt": now + timedelta(hours=24), "status": "active"}})
    elif op == "pause":
        DB.drops.update_one({"_id": drop["_id"]}, {"$set": {"status": "paused"}})
    elif op == "end_now":
        DB.drops.update_one({"_id": drop["_id"]}, {"$set": {"endsAt": now_utc(), "status": "expired"}})
    else:
        return jsonify({"status": "error", "code": "bad_request"}), 400

    return jsonify({"status": "ok"})
