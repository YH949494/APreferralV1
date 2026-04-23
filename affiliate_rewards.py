import logging
import os
from datetime import datetime, timedelta, timezone

import pytz
import requests
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError
from telegram_utils import send_telegram_http_message
from config import (
    AFFILIATE_GROUP_INVITE_TEXT,
    AFFILIATE_GROUP_INVITE_URL,
    AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS,
)

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

TIERS = ("T1", "T2", "T3", "T4", "T5")
POOL_IDS = ("WELCOME",) + TIERS
FINAL_STATUSES = {"ISSUED", "OUT_OF_STOCK", "REJECTED"}
SETTLING_STATUS = "SETTLING"
PROTECTED_STATUSES = {"APPROVED"}

T1_THRESHOLD = int(os.getenv("AFF_T1_THRESHOLD", "10"))
T2_THRESHOLD = int(os.getenv("AFF_T2_THRESHOLD", "25"))
T3_THRESHOLD = int(os.getenv("AFF_T3_THRESHOLD", "50"))
T4_THRESHOLD = int(os.getenv("AFF_T4_THRESHOLD", "150"))
T5_THRESHOLD = int(os.getenv("AFF_T5_THRESHOLD", "250"))
logger = logging.getLogger(__name__)


def _is_official_channel_subscribed(user_id: int) -> bool:
    channel_id_raw = os.getenv("OFFICIAL_CHANNEL_ID")
    token = os.getenv("BOT_TOKEN", "")
    if not channel_id_raw or not token:
        return False
    try:
        channel_id = int(str(channel_id_raw).strip())
    except (TypeError, ValueError):
        return False

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": channel_id, "user_id": int(user_id)},
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json() or {}
        if not payload.get("ok"):
            return False
        status = ((payload.get("result") or {}).get("status") or "").strip()
        return status in ("member", "administrator", "creator")
    except Exception:
        return False

def ensure_affiliate_indexes(db):
    db.qualified_events.create_index([("invitee_id", ASCENDING)], unique=True, name="uniq_invitee_id")
    db.qualified_events.create_index([("referrer_id", ASCENDING), ("qualified_at", ASCENDING)], name="qualified_by_referrer_time")

    db.voucher_pools.create_index([("pool_id", ASCENDING), ("code", ASCENDING)], unique=True, name="uniq_pool_code")
    db.voucher_pools.create_index([("pool_id", ASCENDING), ("status", ASCENDING)], name="pool_status")

    db.affiliate_ledger.create_index([("dedup_key", ASCENDING)], unique=True, name="uniq_affiliate_dedup")
    db.affiliate_ledger.create_index([("status", ASCENDING), ("created_at", ASCENDING)], name="affiliate_status_created")
    db.affiliate_ledger.create_index([("user_id", ASCENDING), ("year_month", ASCENDING)], name="affiliate_user_month")
    db.affiliate_ledger.create_index(
        [("user_id", ASCENDING), ("invitee_user_id", ASCENDING), ("gate_day", ASCENDING), ("tier", ASCENDING), ("created_at", ASCENDING)],
        unique=True,
        name="uniq_affiliate_simulated_natural",
        partialFilterExpression={"simulate": True, "ledger_type": "AFFILIATE_SIMULATION"},
    )

    db.user_last_seen.create_index([("user_id", ASCENDING)], unique=True, name="uniq_user_last_seen")
    db.affiliate_group_invites.create_index(
        [("user_id", ASCENDING), ("week_key", ASCENDING)],
        unique=True,
        name="uniq_affiliate_group_invite_user_week",
    )


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
    if count >= T5_THRESHOLD:
        return "T5"
    if count >= T4_THRESHOLD:
        return "T4"
    if count >= T3_THRESHOLD:
        return "T3"
    if count >= T2_THRESHOLD:
        return "T2"
    if count >= T1_THRESHOLD:
        return "T1"
    return None


def _tier_rank(tier: str | None) -> int:
    ranks = {"T1": 1, "T2": 2, "T3": 3, "T4": 4, "T5": 5}
    key = (tier or "").strip().upper()  # Normalize legacy/case-variant tier values before ranking.
    return ranks.get(key, 0)


def _pool_exists(db, pool_id: str) -> bool:
    return db.voucher_pools.find_one({"pool_id": str(pool_id)}, {"_id": 1}) is not None


def _mark_missing_pool_config(db, *, ledger_id, now_utc: datetime):
    db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": {"$in": [SETTLING_STATUS, "APPROVED", "PENDING_REVIEW", "PENDING_MANUAL"]}, **_no_voucher_filter()},
        {
            "$set": {"status": "PENDING_MANUAL", "updated_at": now_utc},
            "$addToSet": {"risk_flags": "missing_pool_config"},
        },
    )


def _claim_voucher_from_pool(db, *, pool_id: str, ledger_id, user_id: int, now_utc: datetime):
    return db.voucher_pools.find_one_and_update(
        {
            "pool_id": pool_id,
            "status": "available",
            "$or": [
                {"issued_for_ledger_id": {"$exists": False}},
                {"issued_for_ledger_id": None},
            ],
        },
        {
            "$set": {
                "status": "issued",
                "issued_to": user_id,
                "issued_to_user_id": user_id,
                "issued_at": now_utc,
                "ledger_id": ledger_id,
                "issued_for_ledger_id": str(ledger_id),
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def _no_voucher_filter():
    return {"$or": [{"voucher_code": None}, {"voucher_code": {"$exists": False}}]}


def _finalize_issued_if_voucher_exists(db, *, ledger, now_utc: datetime):
    if not ledger:
        return None
    voucher_code = ledger.get("voucher_code")
    if not voucher_code:
        return ledger
    if ledger.get("status") != "ISSUED":
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "voucher_code": voucher_code, "status": {"$ne": "ISSUED"}},
            {"$set": {"status": "ISSUED", "updated_at": now_utc}},
        )
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})
    return ledger


def _reconcile_ledger_from_issued_pool(db, *, ledger_id, now_utc: datetime):
    ledger = db.affiliate_ledger.find_one({"_id": ledger_id})
    if not ledger or ledger.get("status") == "SIMULATED_PENDING":
        return None

    pool_row = db.voucher_pools.find_one(
        {
            "status": "issued",
            "$or": [
                {"issued_for_ledger_id": str(ledger_id)},
                {"ledger_id": ledger_id},
            ],
        }
    )
    if not pool_row or not pool_row.get("code"):
        return None

    issue_claim = db.affiliate_ledger.update_one(
        {
            "_id": ledger_id,
            "status": {"$in": ["PENDING_MANUAL", "PENDING_REVIEW", "APPROVED", SETTLING_STATUS]},
            **_no_voucher_filter(),
        },
        {"$set": {"status": "ISSUED", "voucher_code": pool_row.get("code"), "updated_at": now_utc}},
    )
    if issue_claim.modified_count == 0:
        latest = db.affiliate_ledger.find_one({"_id": ledger_id})
        return _finalize_issued_if_voucher_exists(db, ledger=latest, now_utc=now_utc)
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _has_issued_pool_voucher_for_ledger(db, *, ledger_id) -> bool:
    return (
        db.voucher_pools.find_one(
            {
                "status": "issued",
                "$or": [
                    {"issued_for_ledger_id": str(ledger_id)},
                    {"ledger_id": ledger_id},
                ],
            },
            {"_id": 1},
        )
        is not None
    )


def _affiliate_simulate_enabled() -> bool:
    return str(os.getenv("AFFILIATE_SIMULATE", "0")).strip() == "1"


def _week_start_kl(reference: datetime | None = None) -> datetime:
    ref = reference or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    ref_kl = ref.astimezone(KL_TZ)
    return (ref_kl - timedelta(days=ref_kl.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def _current_week_key_kl(reference: datetime | None = None) -> str:
    return _week_start_kl(reference).date().isoformat()


def _weekly_valid_referral_count_for_referrer(db, *, referrer_id: int, week_key: str) -> int:
    settled = int(
        db.referral_events.count_documents(
            {"inviter_id": int(referrer_id), "event": "referral_settled", "week_key": week_key}
        )
    )
    revoked = int(
        db.referral_events.count_documents(
            {"inviter_id": int(referrer_id), "event": "referral_revoked", "week_key": week_key}
        )
    )
    return max(0, settled - revoked)


def _send_affiliate_group_invite_dm(referrer_id: int, *, invite_url: str) -> tuple[bool, str | None]:
    text = AFFILIATE_GROUP_INVITE_TEXT.format(invite_url=invite_url)
    ok, err, _ = send_telegram_http_message(int(referrer_id), text)
    return ok, err


def _maybe_trigger_affiliate_group_invite(db, *, referrer_id: int | None, now_utc: datetime) -> None:
    if referrer_id is None:
        return
    invite_url = (AFFILIATE_GROUP_INVITE_URL or "").strip()
    if not invite_url:
        logger.info("[AFF_GROUP][SKIP] reason=missing_invite_url referrer=%s", referrer_id)
        return
    if _affiliate_simulate_enabled():
        logger.info("[AFF_GROUP][SKIP] reason=simulate_mode referrer=%s", referrer_id)
        return

    week_key = _current_week_key_kl(now_utc)
    count = _weekly_valid_referral_count_for_referrer(db, referrer_id=int(referrer_id), week_key=week_key)
    threshold = int(AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS)
    logger.info("[AFF_GROUP][CHECK] referrer=%s week_key=%s count=%s", referrer_id, week_key, count)
    if count < threshold:
        logger.info("[AFF_GROUP][SKIP] reason=below_threshold referrer=%s count=%s", referrer_id, count)
        return
    if count > threshold:
        logger.info("[AFF_GROUP][SKIP] reason=above_threshold_no_backfill referrer=%s count=%s", referrer_id, count)
        return

    row_filter = {"user_id": int(referrer_id), "week_key": week_key}
    existing = db.affiliate_group_invites.find_one(row_filter) or {}
    prev_status = existing.get("status")
    if prev_status == "sent":
        logger.info("[AFF_GROUP][SKIP] reason=already_sent referrer=%s week_key=%s", referrer_id, week_key)
        return
    if existing.get("sending") is True:
        logger.info("[AFF_GROUP][SKIP] reason=inflight referrer=%s week_key=%s", referrer_id, week_key)
        return
    if prev_status in {"failed", "pending", "skipped"}:
        logger.info("[AFF_GROUP][RETRY] referrer=%s week_key=%s prev_status=%s", referrer_id, week_key, prev_status)
    elif prev_status not in {None, ""}:
        logger.info("[AFF_GROUP][RETRY] referrer=%s week_key=%s prev_status=%s", referrer_id, week_key, prev_status)

    claim_filter = {
        "user_id": int(referrer_id),
        "week_key": week_key,
        "$and": [
            {"$or": [{"status": {"$ne": "sent"}}, {"status": {"$exists": False}}]},
            {"$or": [{"sending": {"$ne": True}}, {"sending": {"$exists": False}}]},
        ],
    }
    try:
        claimed = db.affiliate_group_invites.find_one_and_update(
            claim_filter,
            {
                "$setOnInsert": {
                    "user_id": int(referrer_id),
                    "week_key": week_key,
                    "created_at": now_utc,
                },
                "$set": {
                    "trigger_count": int(count),
                    "invite_url": invite_url,
                    "status": "pending",
                    "sending": True,
                    "send_attempted_at": now_utc,
                    "sent_at": None,
                    "error": None,
                    "updated_at": now_utc,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        existing = db.affiliate_group_invites.find_one(row_filter) or {}
        if existing.get("status") == "sent":
            logger.info("[AFF_GROUP][SKIP] reason=already_sent referrer=%s week_key=%s", referrer_id, week_key)
            return
        if existing.get("sending") is True:
            logger.info("[AFF_GROUP][SKIP] reason=inflight referrer=%s week_key=%s", referrer_id, week_key)
            return
        logger.info("[AFF_GROUP][RETRY] referrer=%s week_key=%s prev_status=%s", referrer_id, week_key, existing.get("status"))
        claimed = db.affiliate_group_invites.find_one_and_update(
            claim_filter,
            {
                "$set": {
                    "trigger_count": int(count),
                    "invite_url": invite_url,
                    "status": "pending",
                    "sending": True,
                    "send_attempted_at": now_utc,
                    "sent_at": None,
                    "error": None,
                    "updated_at": now_utc,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    if not claimed:
        latest = db.affiliate_group_invites.find_one(row_filter) or {}
        if latest.get("status") == "sent":
            logger.info("[AFF_GROUP][SKIP] reason=already_sent referrer=%s week_key=%s", referrer_id, week_key)
            return
        logger.info("[AFF_GROUP][SKIP] reason=inflight referrer=%s week_key=%s", referrer_id, week_key)
        return

    ok, err = _send_affiliate_group_invite_dm(int(referrer_id), invite_url=invite_url)
    if ok:
        db.affiliate_group_invites.update_one(
            {"user_id": int(referrer_id), "week_key": week_key},
            {
                "$set": {"status": "sent", "sending": False, "sent_at": now_utc, "updated_at": now_utc},
                "$unset": {"error": ""},
            },
        )
        logger.info("[AFF_GROUP][SENT] referrer=%s week_key=%s count=%s", referrer_id, week_key, count)
        return

    db.affiliate_group_invites.update_one(
        {"user_id": int(referrer_id), "week_key": week_key},
        {"$set": {"status": "failed", "sending": False, "error": err, "updated_at": now_utc}},
    )
    logger.error("[AFF_GROUP][FAIL] referrer=%s week_key=%s err=%s", referrer_id, week_key, err)


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
    if not _is_official_channel_subscribed(int(user_id)):
        return {"created": False, "status": "NOT_SUBSCRIBED"}

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

    issue_claim = db.affiliate_ledger.update_one(
        {"_id": ledger["_id"], "status": {"$in": ["PENDING_MANUAL", "PENDING_REVIEW", "APPROVED", SETTLING_STATUS]}, **_no_voucher_filter()},
        {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
    )
    if issue_claim.modified_count == 0:
        latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        latest = _finalize_issued_if_voucher_exists(db, ledger=latest, now_utc=now_utc)
        if latest and not latest.get("voucher_code") and latest.get("status") != "SIMULATED_PENDING":
            latest = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or latest
        if latest and latest.get("status") == "ISSUED":
            return {"created": False, "status": "ISSUED", "voucher_code": latest.get("voucher_code")}
        return {"created": False, "status": (latest or {}).get("status")}

    if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
        latest = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        if latest and latest.get("status") == "ISSUED":
            return {"created": False, "status": "ISSUED", "voucher_code": latest.get("voucher_code")}

    voucher = _claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id=ledger.get("_id"), user_id=int(user_id), now_utc=now_utc)
    if voucher:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
            {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
        )
        return {"created": True, "status": "ISSUED", "voucher_code": voucher.get("code")}

    oos_claim = db.affiliate_ledger.update_one(
        {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
        {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}},
    )
    if oos_claim.modified_count == 0:
        latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]}) or {}
        return {"created": False, "status": latest.get("status"), "voucher_code": latest.get("voucher_code")}
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
    eligible_tiers = [tier_name for tier_name, threshold in (
        ("T1", T1_THRESHOLD),
        ("T2", T2_THRESHOLD),
        ("T3", T3_THRESHOLD),
        ("T4", T4_THRESHOLD),
        ("T5", T5_THRESHOLD),
    ) if int(qualified_count) >= int(threshold)]
    risk_flags = _risk_flags_for_referrer_month(db, referrer_id=int(referrer_id), start_utc=start_utc, end_utc=end_utc)

    last_ledger = None
    for eligible_tier in eligible_tiers:
        dedup_key = f"AFF:{int(referrer_id)}:{yyyymm}:{eligible_tier}"
        db.affiliate_ledger.update_one(
            {"dedup_key": dedup_key},
            {
                "$setOnInsert": {
                    "ledger_type": "AFFILIATE_MONTHLY",
                    "user_id": int(referrer_id),
                    "year_month": yyyymm,
                    "tier": eligible_tier,
                    "pool_id": eligible_tier,
                    "status": "APPROVED",
                    "dedup_key": dedup_key,
                    "voucher_code": None,
                    "risk_flags": list(risk_flags),
                    "created_at": now_utc,
                    "updated_at": now_utc,
                },
                "$set": {
                    "qualified_count": int(qualified_count),
                    "risk_flags": list(risk_flags),
                    "updated_at": now_utc,
                },
            },
            upsert=True,
        )
        ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key})
        if not ledger:
            continue

        status = ledger.get("status")
        if status in FINAL_STATUSES:
            last_ledger = ledger
            continue

        if _affiliate_simulate_enabled():
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {
                    "$set": {
                        "status": "SIMULATED_PENDING",
                        "simulate": True,
                        "would_issue_pool": eligible_tier,
                        "evaluated_at_utc": now_utc,
                        "qualified_count": int(qualified_count),
                        "risk_flags": list(risk_flags),
                        "updated_at": now_utc,
                    }
                },
            )
            last_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            continue

        # If stuck in SETTLING, check whether pool claim already completed and reconcile;
        # if the pool claim never ran, fall through to retry it now.
        if status == SETTLING_STATUS:
            if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
                last_ledger = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or ledger
                continue
            # Pool claim didn't complete — fall through to claim below

        # Transition any non-final, non-settling status to SETTLING before claiming.
        if status != SETTLING_STATUS:
            settle_res = db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": {"$nin": list(FINAL_STATUSES)}, **_no_voucher_filter()},
                {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
            )
            if settle_res.modified_count == 0:
                refreshed = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
                last_ledger = _finalize_issued_if_voucher_exists(db, ledger=refreshed, now_utc=now_utc)
                if last_ledger and not last_ledger.get("voucher_code") and last_ledger.get("status") != "SIMULATED_PENDING":
                    last_ledger = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or last_ledger
                continue

        if eligible_tier not in POOL_IDS:
            logger.warning("[AFFILIATE][INVALID_TIER] uid=%s ledger_id=%s tier=%s pool_id=%s", referrer_id, ledger.get("_id"), eligible_tier, eligible_tier)
            _mark_missing_pool_config(db, ledger_id=ledger["_id"], now_utc=now_utc)
            last_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            continue
        if eligible_tier != "WELCOME" and not _pool_exists(db, eligible_tier):
            logger.warning("[AFFILIATE][POOL_MISSING] uid=%s ledger_id=%s tier=%s pool_id=%s", referrer_id, ledger.get("_id"), eligible_tier, eligible_tier)
            _mark_missing_pool_config(db, ledger_id=ledger["_id"], now_utc=now_utc)
            last_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            continue
        if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
            last_ledger = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            continue
        voucher = _claim_voucher_from_pool(db, pool_id=eligible_tier, ledger_id=ledger.get("_id"), user_id=int(referrer_id), now_utc=now_utc)
        if voucher:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
                {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
            )
        else:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
                {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}},
            )
        last_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})

    return last_ledger


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
                    {"status": "APPROVED", "updated_at": {"$lt": stale_cutoff}},
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
        latest_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        latest_ledger = _finalize_issued_if_voucher_exists(db, ledger=latest_ledger, now_utc=now_utc)
        if latest_ledger and latest_ledger.get("voucher_code"):
            settled += 1
            continue
        reconciled = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc)
        if reconciled and reconciled.get("voucher_code"):
            settled += 1
            continue
        tier = ledger.get("tier")
        uid = int(ledger.get("user_id"))
        if not tier:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {"$set": {"status": "REJECTED", "review_reason": "no_tier", "updated_at": now_utc}},
            )
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "REJECTED")
            continue
        if tier not in POOL_IDS:
            logger.warning("[AFFILIATE][INVALID_TIER] uid=%s ledger_id=%s tier=%s pool_id=%s", uid, ledger.get("_id"), tier, tier)
            _mark_missing_pool_config(db, ledger_id=ledger["_id"], now_utc=now_utc)
            continue
        if tier != "WELCOME" and not _pool_exists(db, tier):
            logger.warning("[AFFILIATE][POOL_MISSING] uid=%s ledger_id=%s tier=%s pool_id=%s", uid, ledger.get("_id"), tier, tier)
            _mark_missing_pool_config(db, ledger_id=ledger["_id"], now_utc=now_utc)
            continue
        kl_dt = KL_TZ.localize(datetime(int(prev_yyyymm[:4]), int(prev_yyyymm[4:6]), 15, 12, 0, 0))
        m_start_utc, m_end_utc, _ = _month_window_utc(kl_dt.astimezone(timezone.utc))
        flags = _risk_flags_for_referrer_month(db, referrer_id=uid, start_utc=m_start_utc, end_utc=m_end_utc)
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"risk_flags": flags, "updated_at": now_utc}},
        )

        if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
            reconciled = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc)
            if reconciled and reconciled.get("voucher_code"):
                settled += 1
            continue
        voucher = _claim_voucher_from_pool(db, pool_id=tier, ledger_id=ledger.get("_id"), user_id=uid, now_utc=now_utc)
        if voucher:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
                {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
            )
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "ISSUED")
        else:
            oos_claim = db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
                {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}},
            )
            if oos_claim.modified_count == 1:
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
    try:
        from affiliate_leaderboard import emit_referral_flow_event
        emit_referral_flow_event(
            db,
            event="affiliate_qualified",
            referrer_id=int(referrer_id) if referrer_id is not None else None,
            invitee_id=int(invitee_id),
            ts_utc=now_utc,
            meta={},
            idempotency_key=f"rf|affiliate_qualified|{int(referrer_id) if referrer_id is not None else None}|{int(invitee_id)}|{now_utc.isoformat()}",
        )
    except Exception:
        logger.exception("affiliate_qualified_event_emit_failed invitee=%s referrer=%s", invitee_id, referrer_id)
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
    ledger = _finalize_issued_if_voucher_exists(db, ledger=ledger, now_utc=now_utc)
    if ledger.get("status") == "ISSUED":
        return ledger
    issue_claim = db.affiliate_ledger.update_one(
        {"_id": ledger["_id"], "status": {"$in": ["APPROVED", SETTLING_STATUS]}, **_no_voucher_filter()},
        {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
    )
    if issue_claim.modified_count == 0:
        latest = _finalize_issued_if_voucher_exists(db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc)
        if latest and not latest.get("voucher_code") and latest.get("status") != "SIMULATED_PENDING":
            latest = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or latest
        return latest
    pool_id = str(ledger.get("pool_id") or "").strip().upper()
    tier = str(ledger.get("tier") or "").strip().upper()
    if pool_id not in POOL_IDS:
        logger.warning("[AFFILIATE][INVALID_TIER] uid=%s ledger_id=%s tier=%s pool_id=%s", ledger.get("user_id"), ledger.get("_id"), tier, pool_id)
        _mark_missing_pool_config(db, ledger_id=ledger["_id"], now_utc=now_utc)
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})
    if pool_id != "WELCOME" and not _pool_exists(db, pool_id):
        logger.warning("[AFFILIATE][POOL_MISSING] uid=%s ledger_id=%s tier=%s pool_id=%s", ledger.get("user_id"), ledger.get("_id"), tier, pool_id)
        _mark_missing_pool_config(db, ledger_id=ledger["_id"], now_utc=now_utc)
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})
    if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
        return _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or db.affiliate_ledger.find_one({"_id": ledger["_id"]})
    voucher = _claim_voucher_from_pool(
        db,
        pool_id=pool_id,
        ledger_id=ledger.get("_id"),
        user_id=int(ledger.get("user_id")),
        now_utc=now_utc,
    )
    if voucher:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
            {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc}},
        )
    else:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
            {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}},
        )
    return db.affiliate_ledger.find_one({"_id": ledger["_id"]})


def reject_affiliate_ledger(db, *, ledger_id, reason: str | None = None, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    db.affiliate_ledger.update_one(
        {"_id": ledger_id},
        {"$set": {"status": "REJECTED", "review_reason": reason, "updated_at": now_utc}},
    )
