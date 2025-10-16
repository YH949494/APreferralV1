from flask import Blueprint, request, jsonify
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone
import hmac, hashlib, urllib.parse, os, json

from database import db

BOT_TOKEN = os.environ.get("BOT_TOKEN")

vouchers_bp = Blueprint("vouchers", __name__)
KL_TZ = timezone(timedelta(hours=8))

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

def parse_kl_local(dt_str: str):
    # "YYYY-MM-DD HH:MM:SS" in KL → UTC datetime
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=KL_TZ)
    return dt.astimezone(timezone.utc)

def norm_username(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()

def parse_init_data(raw: str) -> dict:
    pairs = urllib.parse.parse_qsl(raw, keep_blank_values=True)
    return {k: v for k, v in pairs}

def verify_telegram_init_data(init_data_raw):
    try:
        data = dict(urllib.parse.parse_qsl(init_data_raw, keep_blank_values=True))
        check_hash = data.pop("hash", None)
        data_check_string = "\n".join([f"{k}={v}" for k, v in sorted(data.items())])
        secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
        h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        return h == check_hash, data
    except Exception as e:
        print("verify_telegram_init_data error:", e)
        return False, {}
        
def _admin_allowed_usernames():
    raw = os.getenv("ADMIN_USERNAMES", "")
    return {norm_username(x) for x in raw.split(",") if x.strip()}

def require_admin():
    # ⚙️ Disable backend admin authentication entirely.
    # This trusts your Telegram Mini App frontend (which already hides admin panel).
    # All /v2/admin/... endpoints will accept requests without verification.
    return {"username": "frontend_trusted"}, None
    
# ---- Core visibility logic ----
def is_drop_active(doc: dict, ref: datetime) -> bool:
    return (doc.get("status") != "paused" 
            and doc.get("status") != "expired"
            and doc["startsAt"] <= ref < doc["endsAt"])

def get_active_drops(ref: datetime):
    return list(db.drops.find({
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
            row = db.vouchers.find_one({
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
            already = db.vouchers.find_one({
                "type": "pooled",
                "dropId": drop_id,
                "claimedBy": usernameLower
            })
            if already:
                base["userClaimed"] = True
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

    return jsonify({"status": "ok"})

@vouchers_bp.route("/admin/drops/<drop_id>/whitelist", methods=["POST"])
def admin_update_whitelist(drop_id):
    user, err = require_admin()
    if err: return err
    data = request.get_json(force=True)
    mode = data.get("mode", "replace")
    usernames = data.get("usernames") or []
    drop = db.drops.find_one({"_id": _coerce_id(drop_id)})
    if not drop:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if drop.get("type") != "pooled":
        return jsonify({"status": "error", "code": "invalid_type"}), 400

    if mode == "append":
        new_list = list(set((drop.get("whitelistUsernames") or []) + usernames))
    else:
        new_list = usernames

    db.drops.update_one({"_id": drop["_id"]}, {"$set": {"whitelistUsernames": new_list}})
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
    # personalised stats
    for d in db.drops.find().sort([("priority", DESCENDING), ("startsAt", ASCENDING)]):
        drop_id = str(d["_id"])
        dtype = d.get("type", "pooled")
        row = {
            "dropId": drop_id,
            "name": d.get("name"),
            "type": dtype,
            "status": d.get("status", "upcoming"),
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

