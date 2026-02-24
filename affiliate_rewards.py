import logging
import os
from datetime import datetime, timedelta, timezone

import pytz
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

TIERS = ("T1", "T2", "T3", "T4")
POOL_IDS = ("WELCOME",) + TIERS
FINAL_STATUSES = {"ISSUED", "OUT_OF_STOCK", "REJECTED"}
SETTLING_STATUS = "SETTLING"
PROTECTED_STATUSES = {"PENDING_REVIEW", "PENDING_MANUAL", "APPROVED"}

T1_THRESHOLD = int(os.getenv("AFF_T1_THRESHOLD", "3"))
T2_THRESHOLD = int(os.getenv("AFF_T2_THRESHOLD", "7"))
T3_THRESHOLD = int(os.getenv("AFF_T3_THRESHOLD", "15"))
T4_THRESHOLD = int(os.getenv("AFF_T4_THRESHOLD", "30"))
logger = logging.getLogger(__name__)

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

    # One ledger per user per month (highest tier only)
    dedup_key = f"AFF:{int(referrer_id)}:{yyyymm}"
    db.affiliate_ledger.update_one(
        {"dedup_key": dedup_key},
        {
            "$setOnInsert": {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": int(referrer_id),
                "year_month": yyyymm,
                "tier": None,
                "pool_id": None,
                "qualified_count": 0,
                "status": "PENDING_EOM",
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
    cur_tier = ledger.get("tier")
    tier_rank = {"T1": 1, "T2": 2, "T3": 3, "T4": 4}
    should_upgrade = (cur_tier is None) or (tier_rank.get(tier, 0) > tier_rank.get(cur_tier, 0))
    base_update = {
        "qualified_count": int(qualified_count),
        "updated_at": now_utc,
    }
    if should_upgrade:
        base_update.update({"tier": tier, "pool_id": tier})

    if _affiliate_simulate_enabled():
        simulate_update = dict(base_update)
        simulate_update.update(
            {
                "status": "SIMULATED_PENDING",
                "simulate": True,
                "would_issue_pool": tier,
                "evaluated_at_utc": now_utc,
            }
        )
        db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": simulate_update})
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})

    status = ledger.get("status")
    if status in FINAL_STATUSES or status == SETTLING_STATUS:
        return ledger

    if status in PROTECTED_STATUSES:
        db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": base_update})
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})

    next_status = "PENDING_EOM"
    next_flags = []
    if tier == "T4":
        next_status = "PENDING_MANUAL"
    elif tier in {"T2", "T3"}:
        flags = _risk_flags_for_referrer_month(db, referrer_id=int(referrer_id), start_utc=start_utc, end_utc=end_utc)
        if flags:
            next_status = "PENDING_REVIEW"
            next_flags = flags
    update_fields = dict(base_update)
    update_fields["status"] = next_status
    update_fields["risk_flags"] = next_flags

    db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": update_fields})
    return db.affiliate_ledger.find_one({"_id": ledger["_id"]})


def settle_previous_month_affiliate_rewards(db, *, now_utc: datetime | None = None, batch_limit: int = 500):
    now_utc = now_utc or datetime.now(timezone.utc)
    start_utc, _, _ = _month_window_utc(now_utc)
    prev_ref = start_utc - timedelta(seconds=1)
    _, _, prev_yyyymm = _month_window_utc(prev_ref)

    logger.info("affiliate_monthly_settle start prev_yyyymm=%s batch_limit=%s", prev_yyyymm, int(batch_limit))

    stale_cutoff = now_utc - timedelta(minutes=15)
    processed = 0
    settled = 0
    while processed < int(batch_limit):
        ledger = db.affiliate_ledger.find_one_and_update(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "year_month": prev_yyyymm,
                "$or": [
                    {"status": "PENDING_EOM"},
                    {"status": SETTLING_STATUS, "updated_at": {"$lt": stale_cutoff}},
                ],
            },
            {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not ledger:
            break

        processed += 1
        tier = ledger.get("tier")
        uid = int(ledger.get("user_id"))
        if not tier:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {"$set": {"status": "REJECTED", "review_reason": "no_tier", "updated_at": now_utc}},
            )
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "REJECTED")
            continue
        if tier == "T4":
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {"$set": {"status": "PENDING_MANUAL", "updated_at": now_utc, "risk_flags": []}},
            )
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "PENDING_MANUAL")
            continue

        kl_dt = KL_TZ.localize(datetime(int(prev_yyyymm[:4]), int(prev_yyyymm[4:6]), 15, 12, 0, 0))
        m_start_utc, m_end_utc, _ = _month_window_utc(kl_dt.astimezone(timezone.utc))
        if tier in {"T2", "T3"}:
            flags = _risk_flags_for_referrer_month(db, referrer_id=uid, start_utc=m_start_utc, end_utc=m_end_utc)
            if flags:
                db.affiliate_ledger.update_one(
                    {"_id": ledger["_id"]},
                    {"$set": {"status": "PENDING_REVIEW", "risk_flags": flags, "updated_at": now_utc}},
                )
                logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "PENDING_REVIEW")
                continue

        voucher = _claim_voucher_from_pool(db, pool_id=tier, ledger_id=ledger.get("_id"), user_id=uid, now_utc=now_utc)
        if voucher:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
            )
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "ISSUED")
        else:
            db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}})
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "OUT_OF_STOCK")
        settled += 1

    logger.info("affiliate_monthly_settle end prev_yyyymm=%s processed=%s settled=%s", prev_yyyymm, processed, settled)
    return {"prev_yyyymm": prev_yyyymm, "processed": processed, "settled": settled}


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
