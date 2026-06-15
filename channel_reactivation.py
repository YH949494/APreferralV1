from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Callable

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - production requirements include requests
    requests = None
try:
    from pymongo.errors import DuplicateKeyError
except ModuleNotFoundError:  # pragma: no cover - production requirements include pymongo
    class DuplicateKeyError(Exception):
        pass

from xp import grant_xp

logger = logging.getLogger(__name__)

CAMPAIGN_ID = "official_channel_reactivation_phase1"
VERIFY_CALLBACK_DATA = "reactivate_verify"
REWARD_XP = int(os.getenv("CHANNEL_REACTIVATION_REWARD_XP", "50"))
DAILY_SEND_LIMIT = int(os.getenv("CHANNEL_REACTIVATION_DAILY_LIMIT", "1000"))
MINUTE_SEND_LIMIT = int(os.getenv("CHANNEL_REACTIVATION_MINUTE_LIMIT", "20"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OFFICIAL_CHANNEL_USERNAME = os.getenv("OFFICIAL_CHANNEL_USERNAME", "@advantplayofficial")
OFFICIAL_CHANNEL_URL = os.getenv(
    "OFFICIAL_CHANNEL_URL",
    f"https://t.me/{OFFICIAL_CHANNEL_USERNAME.lstrip('@')}",
)
_RAW_OFFICIAL_CHANNEL_ID = os.getenv("OFFICIAL_CHANNEL_ID")
try:
    OFFICIAL_CHANNEL_ID = int(_RAW_OFFICIAL_CHANNEL_ID) if _RAW_OFFICIAL_CHANNEL_ID not in (None, "") else -1002396761021
except (TypeError, ValueError):
    OFFICIAL_CHANNEL_ID = -1002396761021


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _day_key(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).date().isoformat()


def _campaigns(db):
    return db.channel_reactivation_campaigns


def _messages(db):
    return db.channel_reactivation_messages


def _rewards(db):
    return db.channel_reactivation_rewards


def ensure_channel_reactivation_indexes(db) -> None:
    _campaigns(db).create_index([("_id", 1)], unique=True)
    _messages(db).create_index([("campaign_id", 1), ("user_id", 1)], unique=True, name="uniq_reactivation_message_user")
    _messages(db).create_index([("campaign_id", 1), ("status", 1), ("sent_day", 1)], name="reactivation_messages_status_day")
    _messages(db).create_index([("campaign_id", 1), ("created_at", -1)], name="reactivation_messages_created")
    _rewards(db).create_index([("campaign_id", 1), ("user_id", 1)], unique=True, name="uniq_reactivation_reward_user")
    _rewards(db).create_index([("campaign_id", 1), ("claimed_at", -1)], name="reactivation_rewards_claimed")


def set_campaign_active(db, active: bool, *, actor: str = "admin") -> dict:
    ts = now_utc()
    _campaigns(db).update_one(
        {"_id": CAMPAIGN_ID},
        {
            "$set": {
                "active": bool(active),
                "updated_at": ts,
                "updated_by": actor,
            },
            "$setOnInsert": {
                "_id": CAMPAIGN_ID,
                "created_at": ts,
            },
        },
        upsert=True,
    )
    return campaign_summary(db)


def is_campaign_active(db) -> bool:
    doc = _campaigns(db).find_one({"_id": CAMPAIGN_ID}, {"active": 1}) or {}
    return bool(doc.get("active"))


def _registered_not_rewarded_filter() -> dict:
    return {
        "$and": [
            {
                "$or": [
                    {"telegram_user_id": {"$exists": True, "$ne": None}},
                    {"user_id": {"$exists": True, "$ne": None}},
                ]
            },
            {"blocked": {"$ne": True}},
            {"banned": {"$ne": True}},
            {"is_banned": {"$ne": True}},
            {"status": {"$ne": "banned"}},
            {"reactivation_reward_claimed": {"$ne": True}},
            {"reactivation_dm_sent_at": {"$exists": False}},
        ]
    }


def _telegram_uid(user_doc: dict) -> int | None:
    raw = user_doc.get("telegram_user_id") or user_doc.get("user_id")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def campaign_summary(db) -> dict:
    today = _day_key(now_utc())
    campaign = _campaigns(db).find_one({"_id": CAMPAIGN_ID}) or {}
    eligible = db.users.count_documents(_registered_not_rewarded_filter())
    sent = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "sent"})
    sent_today = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "sent", "sent_day": today})
    verified = _rewards(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "claimed"})
    xp_rows = _rewards(db).find({"campaign_id": CAMPAIGN_ID, "status": "claimed"}, {"xp_awarded": 1})
    xp_awarded = sum(int(row.get("xp_awarded", 0) or 0) for row in xp_rows)
    failed = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "failed"})
    skipped_subscribed = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "skipped_subscribed"})
    return {
        "success": True,
        "campaign_id": CAMPAIGN_ID,
        "active": bool(campaign.get("active")),
        "eligible_users": int(eligible),
        "messages_sent": int(sent),
        "messages_sent_today": int(sent_today),
        "successful_verifications": int(verified),
        "xp_awarded": int(xp_awarded),
        "send_failures": int(failed),
        "skipped_already_subscribed": int(skipped_subscribed),
        "daily_limit": DAILY_SEND_LIMIT,
        "minute_limit": MINUTE_SEND_LIMIT,
        "updated_at": campaign.get("updated_at"),
    }


def check_official_channel_subscribed(uid: int, *, token: str | None = None, channel_id: int | None = None) -> tuple[bool, str]:
    bot_token = token or BOT_TOKEN
    chat_id = OFFICIAL_CHANNEL_ID if channel_id is None else channel_id
    if not uid:
        return False, "missing_uid"
    if not bot_token:
        return False, "missing_bot_token"
    if chat_id is None:
        return False, "missing_channel_id"
    if requests is None:
        return False, "missing_requests"
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{bot_token}/getChatMember",
            params={"chat_id": chat_id, "user_id": int(uid)},
            timeout=10,
        )
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        return False, f"{exc.__class__.__name__}: {exc}"
    if not payload.get("ok"):
        return False, f"telegram_not_ok:{payload.get('description', 'unknown')}"
    status = (payload.get("result") or {}).get("status")
    return status in {"member", "administrator", "creator"}, f"status:{status}"


def _message_text() -> str:
    return (
        "🎁 Welcome Back!\n\n"
        "Daily voucher drops, exclusive promotions, and special event rewards are available in our Official Channel.\n\n"
        "🔥 Daily Voucher Opportunities\n"
        "🔥 Exclusive Promotions\n"
        "🔥 Priority Updates\n"
        f"🔥 +{REWARD_XP} XP Bonus After Verification"
    )


def _reply_markup() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "Join Official Channel", "url": OFFICIAL_CHANNEL_URL}],
            [{"text": "Verify & Claim", "callback_data": VERIFY_CALLBACK_DATA}],
        ]
    }


def send_reactivation_dm(uid: int, *, token: str | None = None) -> tuple[bool, str | None]:
    bot_token = token or BOT_TOKEN
    if not bot_token:
        return False, "missing_bot_token"
    if requests is None:
        return False, "missing_requests"
    payload = {
        "chat_id": int(uid),
        "text": _message_text(),
        "reply_markup": _reply_markup(),
    }
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json=payload,
            timeout=10,
        )
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return False, f"{exc.__class__.__name__}: {exc}"
    if resp.status_code == 200 and data.get("ok"):
        return True, None
    return False, data.get("description") or f"telegram_http_{resp.status_code}"


def _record_message(db, uid: int, status: str, ts: datetime, **fields) -> None:
    update = {
        "$set": {
            "status": status,
            "updated_at": ts,
            **fields,
        },
        "$setOnInsert": {
            "campaign_id": CAMPAIGN_ID,
            "user_id": int(uid),
            "created_at": ts,
        },
    }
    _messages(db).update_one({"campaign_id": CAMPAIGN_ID, "user_id": int(uid)}, update, upsert=True)


def process_reactivation_campaign(
    *,
    db_ref,
    batch_limit: int | None = None,
    membership_checker: Callable[[int], tuple[bool, str]] | None = None,
    send_fn: Callable[[int], tuple[bool, str | None]] | None = None,
    now_ref: datetime | None = None,
) -> dict:
    ts = now_ref or now_utc()
    stats = {"active": is_campaign_active(db_ref), "scanned": 0, "sent": 0, "skipped_subscribed": 0, "failed": 0}
    if not stats["active"]:
        return stats
    checker = membership_checker or check_official_channel_subscribed
    sender = send_fn or send_reactivation_dm
    today = _day_key(ts)
    sent_today = _messages(db_ref).count_documents({"campaign_id": CAMPAIGN_ID, "status": "sent", "sent_day": today})
    remaining_today = max(0, DAILY_SEND_LIMIT - int(sent_today))
    per_run_limit = min(int(batch_limit or MINUTE_SEND_LIMIT), MINUTE_SEND_LIMIT, remaining_today)
    if per_run_limit <= 0:
        stats["daily_limit_reached"] = True
        return stats

    cursor = db_ref.users.find(
        _registered_not_rewarded_filter(),
        {"user_id": 1, "telegram_user_id": 1, "username": 1},
    ).limit(per_run_limit)
    for user_doc in cursor:
        stats["scanned"] += 1
        uid = _telegram_uid(user_doc)
        if not uid:
            continue
        try:
            if _rewards(db_ref).find_one({"campaign_id": CAMPAIGN_ID, "user_id": uid}):
                db_ref.users.update_one(
                    {"user_id": user_doc.get("user_id", uid)},
                    {"$set": {"reactivation_reward_claimed": True, "reactivation_reward_claimed_at": ts}},
                )
                continue
            subscribed, reason = checker(uid)
            if subscribed:
                _record_message(db_ref, uid, "skipped_subscribed", ts, reason=reason, skipped_at=ts)
                db_ref.users.update_one({"user_id": user_doc.get("user_id", uid)}, {"$set": {"reactivation_skipped_subscribed_at": ts}})
                stats["skipped_subscribed"] += 1
                continue
            ok, err = sender(uid)
            if ok:
                _record_message(db_ref, uid, "sent", ts, sent_at=ts, sent_day=today)
                db_ref.users.update_one({"user_id": user_doc.get("user_id", uid)}, {"$set": {"reactivation_dm_sent_at": ts}})
                stats["sent"] += 1
            else:
                _record_message(db_ref, uid, "failed", ts, error=err, failed_at=ts)
                logger.warning("[CHANNEL_REACTIVATION] send_failed uid=%s err=%s", uid, err)
                stats["failed"] += 1
        except Exception as exc:  # noqa: BLE001
            _record_message(db_ref, uid, "failed", ts, error=f"{exc.__class__.__name__}: {exc}", failed_at=ts)
            logger.exception("[CHANNEL_REACTIVATION] failure uid=%s", uid)
            stats["failed"] += 1
    return stats


def verify_reactivation_claim(
    db,
    uid: int,
    *,
    membership_checker: Callable[[int], tuple[bool, str]] | None = None,
    now_ref: datetime | None = None,
) -> dict:
    ts = now_ref or now_utc()
    uid = int(uid)
    if _rewards(db).find_one({"campaign_id": CAMPAIGN_ID, "user_id": uid}):
        return {"success": False, "code": "already_claimed", "message": "You already claimed this XP bonus."}
    user_doc = db.users.find_one(
        {"user_id": uid},
        {"reactivation_reward_claimed": 1, "blocked": 1, "banned": 1, "is_banned": 1, "status": 1},
    ) or {}
    if (
        user_doc.get("blocked")
        or user_doc.get("banned")
        or user_doc.get("is_banned")
        or str(user_doc.get("status", "")).lower() == "banned"
    ):
        return {"success": False, "code": "ineligible", "message": "This reward is not available for your account."}
    if user_doc.get("reactivation_reward_claimed"):
        return {"success": False, "code": "already_claimed", "message": "You already claimed this XP bonus."}

    checker = membership_checker or check_official_channel_subscribed
    subscribed, reason = checker(uid)
    if not subscribed:
        return {
            "success": False,
            "code": "not_subscribed",
            "message": "Please join the Official Channel first, then tap Verify & Claim again.",
            "reason": reason,
        }

    unique_key = f"{CAMPAIGN_ID}:{uid}"
    granted = grant_xp(db, uid, "official_channel_reactivation", unique_key, REWARD_XP)
    if not granted:
        return {"success": False, "code": "already_claimed", "message": "You already claimed this XP bonus."}

    reward_doc = {
        "campaign_id": CAMPAIGN_ID,
        "user_id": uid,
        "status": "claimed",
        "claimed_at": ts,
        "xp_awarded": REWARD_XP,
        "membership_reason": reason,
    }
    try:
        _rewards(db).insert_one(reward_doc)
    except DuplicateKeyError:
        logger.info("[CHANNEL_REACTIVATION] duplicate_reward_after_xp uid=%s", uid)

    db.users.update_one(
        {"user_id": uid},
        {"$set": {"reactivation_reward_claimed": True, "reactivation_reward_claimed_at": ts}},
    )
    return {"success": True, "code": "claimed", "message": f"Verified! +{REWARD_XP} XP has been added.", "xp_awarded": REWARD_XP}


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
