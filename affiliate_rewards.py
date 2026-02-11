import os
from datetime import datetime, timedelta, timezone

import pytz
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

TIERS = ("T1", "T2", "T3", "T4")
POOL_IDS = ("WELCOME",) + TIERS

T1_THRESHOLD = int(os.getenv("AFF_T1_THRESHOLD", "3"))
T2_THRESHOLD = int(os.getenv("AFF_T2_THRESHOLD", "7"))
T3_THRESHOLD = int(os.getenv("AFF_T3_THRESHOLD", "15"))
T4_THRESHOLD = int(os.getenv("AFF_T4_THRESHOLD", "30"))


def ensure_affiliate_indexes(db):
    db.qualified_events.create_index([("invitee_id", ASCENDING)], unique=True, name="uniq_invitee_id")
    db.qualified_events.create_index([("referrer_id", ASCENDING), ("qualified_at", ASCENDING)], name="qualified_by_referrer_time")

    db.voucher_pools.create_index([("pool_id", ASCENDING), ("code", ASCENDING)], unique=True, name="uniq_pool_code")
    db.voucher_pools.create_index([("pool_id", ASCENDING), ("status", ASCENDING)], name="pool_status")

    db.affiliate_ledger.create_index([("dedup_key", ASCENDING)], unique=True, name="uniq_affiliate_dedup")
    db.affiliate_ledger.create_index([("status", ASCENDING), ("created_at", ASCENDING)], name="affiliate_status_created")
    db.affiliate_ledger.create_index([("user_id", ASCENDING), ("year_month", ASCENDING)], name="affiliate_user_month")

    db.user_last_seen.create_index([("user_id", ASCENDING)], unique=True, name="uniq_user_last_seen")


def _month_window_utc(reference_utc: datetime | None = None):
    now_utc = reference_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    ref_kl = now_utc.astimezone(KL_TZ)
    start_kl = ref_kl.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start_kl.month == 12:
        end_kl = start_kl.replace(year=start_kl.year + 1, month=1)
    else:
        end_kl = start_kl.replace(month=start_kl.month + 1)
    return start_kl.astimezone(timezone.utc), end_kl.astimezone(timezone.utc), start_kl.strftime("%Y%m")


def _tier_for_count(count: int) -> str | None:
    if count >= T4_THRESHOLD:
        return "T4"
    if count >= T3_THRESHOLD:
        return "T3"
    if count >= T2_THRESHOLD:
        return "T2"
    if count >= T1_THRESHOLD:
        return "T1"
    return None


def _claim_voucher_from_pool(db, *, pool_id: str, ledger_id, user_id: int, now_utc: datetime):
    return db.voucher_pools.find_one_and_update(
        {"pool_id": pool_id, "status": "available"},
        {
            "$set": {
                "status": "issued",
                "issued_to": user_id,
                "issued_at": now_utc,
                "ledger_id": ledger_id,
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def _affiliate_simulate_enabled() -> bool:
    return str(os.getenv("AFFILIATE_SIMULATE", "0")).strip() == "1"


def record_user_last_seen(db, *, user_id: int, ip: str | None = None, subnet: str | None = None, session: str | None = None, seen_at: datetime | None = None):
    now_utc = seen_at or datetime.now(timezone.utc)
    db.user_last_seen.update_one(
        {"user_id": int(user_id)},
        {
            "$set": {
                "user_id": int(user_id),
                "ip": ip,
                "subnet": subnet,
                "session": session,
                "seen_at": now_utc,
            }
        },
        upsert=True,
    )


def issue_welcome_bonus_if_eligible(db, *, user_id: int, is_new_user: bool, blocked: bool = False, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    if not is_new_user or blocked:
        return {"created": False, "status": "SKIPPED"}

    dedup_key = f"WELCOME:{int(user_id)}"
    db.affiliate_ledger.update_one(
        {"dedup_key": dedup_key},
        {
            "$setOnInsert": {
                "ledger_type": "WELCOME",
                "user_id": int(user_id),
                "year_month": None,
                "tier": "WELCOME",
                "pool_id": "WELCOME",
                "status": "PENDING_MANUAL",
                "dedup_key": dedup_key,
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now_utc,
                "updated_at": now_utc,
            }
        },
        upsert=True,
    )
    ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key})
    if not ledger:
        return {"created": False, "status": "ERROR"}
    if _affiliate_simulate_enabled():
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {
                "$set": {
                    "status": "SIMULATED_PENDING",
                    "simulate": True,
                    "would_issue_pool": "WELCOME",
                    "evaluated_at_utc": now_utc,
                    "updated_at": now_utc,
                }
            },
        )
        return {"created": True, "status": "SIMULATED_PENDING"}
    if ledger.get("status") in {"ISSUED", "OUT_OF_STOCK"}:
        return {"created": False, "status": ledger.get("status"), "voucher_code": ledger.get("voucher_code")}

    voucher = _claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id=ledger.get("_id"), user_id=int(user_id), now_utc=now_utc)
    if voucher:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
        )
        return {"created": True, "status": "ISSUED", "voucher_code": voucher.get("code")}

    db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}})
    return {"created": True, "status": "OUT_OF_STOCK"}


def _risk_flags_for_referrer_month(db, *, referrer_id: int, start_utc: datetime, end_utc: datetime):
    flags = []
    for row in db.qualified_events.aggregate([
        {"$match": {"referrer_id": int(referrer_id), "qualified_at": {"$gte": start_utc, "$lt": end_utc}, "ip": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$ip", "invitees": {"$addToSet": "$invitee_id"}}},
        {"$project": {"count": {"$size": "$invitees"}}},
    ]):
        if int(row.get("count", 0)) >= 4:
            flags.append("ip_cluster")
            break

    for row in db.qualified_events.aggregate([
        {"$match": {"referrer_id": int(referrer_id), "qualified_at": {"$gte": start_utc, "$lt": end_utc}, "subnet": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$subnet", "invitees": {"$addToSet": "$invitee_id"}}},
        {"$project": {"count": {"$size": "$invitees"}}},
    ]):
        if int(row.get("count", 0)) >= 6:
            flags.append("subnet_cluster")
            break

    deny_from = end_utc - timedelta(days=7)
    deny_count = db.referral_audit.count_documents(
        {
            "inviter_user_id": int(referrer_id),
            "created_at": {"$gte": deny_from, "$lt": end_utc},
            "reason": {"$in": ["deny", "deny_severe", "blocked", "abuse"]},
        }
    )
    if int(deny_count) >= 3:
        flags.append("deny_count_7d")
    return flags


def evaluate_monthly_affiliate_reward(db, *, referrer_id: int, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    user_doc = db.users.find_one({"user_id": int(referrer_id)}, {"blocked": 1}) or {}
    if user_doc.get("blocked"):
        return None

    start_utc, end_utc, yyyymm = _month_window_utc(now_utc)
    qualified_count = db.qualified_events.count_documents(
        {"referrer_id": int(referrer_id), "qualified_at": {"$gte": start_utc, "$lt": end_utc}}
    )
    tier = _tier_for_count(int(qualified_count))
    if not tier:
        return None

    dedup_key = f"AFF:{int(referrer_id)}:{yyyymm}:{tier}"
    db.affiliate_ledger.update_one(
        {"dedup_key": dedup_key},
        {
            "$setOnInsert": {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": int(referrer_id),
                "year_month": yyyymm,
                "tier": tier,
                "pool_id": tier,
                "status": "PENDING_MANUAL",
                "dedup_key": dedup_key,
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now_utc,
                "updated_at": now_utc,
            }
        },
        upsert=True,
    )
    ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key})
    if not ledger:
        return None
    if _affiliate_simulate_enabled():
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {
                "$set": {
                    "status": "SIMULATED_PENDING",
                    "simulate": True,
                    "would_issue_pool": tier,
                    "evaluated_at_utc": now_utc,
                    "updated_at": now_utc,
                }
            },
        )
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})
    if ledger.get("status") in {"ISSUED", "OUT_OF_STOCK", "PENDING_REVIEW", "PENDING_MANUAL", "REJECTED", "APPROVED"} and ledger.get("voucher_code"):
        return ledger

    if tier == "T4":
        db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": {"status": "PENDING_MANUAL", "updated_at": now_utc}})
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})

    if tier in {"T2", "T3"}:
        flags = _risk_flags_for_referrer_month(db, referrer_id=int(referrer_id), start_utc=start_utc, end_utc=end_utc)
        if flags:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {"$set": {"status": "PENDING_REVIEW", "risk_flags": flags, "updated_at": now_utc}},
            )
            return db.affiliate_ledger.find_one({"_id": ledger["_id"]})

    voucher = _claim_voucher_from_pool(db, pool_id=tier, ledger_id=ledger.get("_id"), user_id=int(referrer_id), now_utc=now_utc)
    if voucher:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
        )
    else:
        db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}})
    return db.affiliate_ledger.find_one({"_id": ledger["_id"]})


def mark_invitee_qualified(db, *, invitee_id: int, referrer_id: int | None, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    last_seen = db.user_last_seen.find_one({"user_id": int(invitee_id)}) or {}
    doc = {
        "invitee_id": int(invitee_id),
        "referrer_id": int(referrer_id) if referrer_id is not None else None,
        "qualified_at": now_utc,
        "ip": last_seen.get("ip"),
        "subnet": last_seen.get("subnet"),
        "session": last_seen.get("session"),
    }
    try:
        db.qualified_events.insert_one(doc)
    except DuplicateKeyError:
        return False
    if referrer_id is not None:
        evaluate_monthly_affiliate_reward(db, referrer_id=int(referrer_id), now_utc=now_utc)
    return True


def approve_affiliate_ledger(db, *, ledger_id, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    ledger = db.affiliate_ledger.find_one_and_update(
        {"_id": ledger_id, "status": {"$in": ["PENDING_REVIEW", "PENDING_MANUAL", "APPROVED"]}},
        {"$set": {"status": "APPROVED", "updated_at": now_utc}},
        return_document=ReturnDocument.AFTER,
    )
    if not ledger:
        return None
    if ledger.get("voucher_code"):
        return ledger
    voucher = _claim_voucher_from_pool(
        db,
        pool_id=ledger.get("pool_id"),
        ledger_id=ledger.get("_id"),
        user_id=int(ledger.get("user_id")),
        now_utc=now_utc,
    )
    if voucher:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
        )
    else:
        db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}})
    return db.affiliate_ledger.find_one({"_id": ledger["_id"]})


def reject_affiliate_ledger(db, *, ledger_id, reason: str | None = None, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    db.affiliate_ledger.update_one(
        {"_id": ledger_id},
        {"$set": {"status": "REJECTED", "review_reason": reason, "updated_at": now_utc}},
    )
