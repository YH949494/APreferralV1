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
from reactivation_journey import create_or_update_journey

logger = logging.getLogger(__name__)

CAMPAIGN_ID = "official_channel_reactivation_phase1"
VERIFY_CALLBACK_DATA = "reactivate_verify"
REWARD_XP = int(os.getenv("CHANNEL_REACTIVATION_REWARD_XP", "50"))
DAILY_SEND_LIMIT = int(os.getenv("CHANNEL_REACTIVATION_DAILY_LIMIT", "1000"))
MINUTE_SEND_LIMIT = int(os.getenv("CHANNEL_REACTIVATION_MINUTE_LIMIT", "20"))
MAX_PER_RUN_LIMIT = int(os.getenv("CHANNEL_REACTIVATION_MAX_PER_RUN_LIMIT", "1000"))
REWARD_HOLD_HOURS = int(os.getenv("CHANNEL_REACTIVATION_REWARD_HOLD_HOURS", "72"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OFFICIAL_CHANNEL_USERNAME = os.getenv("OFFICIAL_CHANNEL_USERNAME", "@advantplayofficial")
OFFICIAL_CHANNEL_URL = os.getenv(
    "OFFICIAL_CHANNEL_URL",
    f"https://t.me/{OFFICIAL_CHANNEL_USERNAME.lstrip('@')}",
)
VERIFICATION_PENDING_MESSAGE = (
    f"✅ Subscription verified.\n\n"
    f"Stay subscribed for {REWARD_HOLD_HOURS} hours to receive your +{REWARD_XP} XP reward.\n\n"
    "Vouchers can drop at any time.."
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
    _messages(db).create_index([("campaign_id", 1), ("user_id", 1)], unique=True, name="uniq_reactivation_message_user")
    _messages(db).create_index([("campaign_id", 1), ("status", 1), ("sent_day", 1)], name="reactivation_messages_status_day")
    _messages(db).create_index([("campaign_id", 1), ("created_at", -1)], name="reactivation_messages_created")
    _rewards(db).create_index([("campaign_id", 1), ("user_id", 1)], unique=True, name="uniq_reactivation_reward_user")
    _rewards(db).create_index([("campaign_id", 1), ("status", 1), ("reward_due_at", 1)], name="reactivation_rewards_status_due")
    _rewards(db).create_index([("campaign_id", 1), ("rewarded_at", -1)], name="reactivation_rewards_rewarded")


def _coerce_per_run_limit(value, *, default: int | None = None) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        limit = int(default or MINUTE_SEND_LIMIT)
    return max(1, min(limit, MAX_PER_RUN_LIMIT))


def set_campaign_active(db, active: bool, *, actor: str = "admin", per_run_limit: int | None = None) -> dict:
    ts = now_utc()
    set_fields = {
        "active": bool(active),
        "updated_at": ts,
        "updated_by": actor,
    }
    if per_run_limit is not None:
        set_fields["per_run_limit"] = _coerce_per_run_limit(per_run_limit)
    _campaigns(db).update_one(
        {"_id": CAMPAIGN_ID},
        {
            "$set": set_fields,
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
            {"reactivation_failed_blocked_at": {"$exists": False}},
            {"reactivation_dm_sent_at": {"$exists": False}},
            {"reactivation_skipped_subscribed_at": {"$exists": False}},
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
    verified = _rewards(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": {"$in": ["pending", "rewarded"]}})
    pending = _rewards(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "pending"})
    rewarded = _rewards(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "rewarded"})
    cancelled = _rewards(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "cancelled"})
    xp_rows = _rewards(db).find({"campaign_id": CAMPAIGN_ID, "status": "rewarded"}, {"xp_awarded": 1})
    xp_awarded = sum(int(row.get("xp_awarded", 0) or 0) for row in xp_rows)
    failed = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "failed"})
    failed_blocked = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "failed_blocked"})
    skipped_subscribed = _messages(db).count_documents({"campaign_id": CAMPAIGN_ID, "status": "skipped_subscribed"})
    return {
        "success": True,
        "campaign_id": CAMPAIGN_ID,
        "active": bool(campaign.get("active")),
        "eligible_users": int(eligible),
        "messages_sent": int(sent),
        "messages_sent_today": int(sent_today),
        "successful_verifications": int(verified),
        "pending_rewards": int(pending),
        "rewarded": int(rewarded),
        "cancelled_rewards": int(cancelled),
        "xp_awarded": int(xp_awarded),
        "send_failures": int(failed),
        "failed_blocked": int(failed_blocked),
        "skipped_already_subscribed": int(skipped_subscribed),
        "daily_limit": DAILY_SEND_LIMIT,
        "minute_limit": MINUTE_SEND_LIMIT,
        "per_run_limit": _coerce_per_run_limit(campaign.get("per_run_limit"), default=MINUTE_SEND_LIMIT),
        "max_per_run_limit": MAX_PER_RUN_LIMIT,
        "reward_hold_hours": REWARD_HOLD_HOURS,
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
    if resp.status_code == 403 or data.get("error_code") == 403:
        return False, "bot_blocked"
    return False, data.get("description") or f"telegram_http_{resp.status_code}"


def _is_blocked_send_error(err: str | None) -> bool:
    msg = str(err or "").lower()
    return "bot_blocked" in msg or "blocked by the user" in msg or "forbidden" in msg


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
    reward_stats = process_pending_reactivation_rewards(
        db_ref=db_ref,
        membership_checker=membership_checker,
        now_ref=ts,
    )
    stats = {
        "active": is_campaign_active(db_ref),
        "scanned": 0,
        "sent": 0,
        "skipped_subscribed": 0,
        "failed": 0,
        "failed_blocked": 0,
        "rewards": reward_stats,
    }
    if not stats["active"]:
        return stats
    checker = membership_checker or check_official_channel_subscribed
    sender = send_fn or send_reactivation_dm
    campaign_doc = _campaigns(db_ref).find_one({"_id": CAMPAIGN_ID}, {"per_run_limit": 1}) or {}
    stored_limit = campaign_doc.get("per_run_limit")
    if stored_limit is not None:
        configured_limit = batch_limit if batch_limit is not None else stored_limit
    elif batch_limit is not None:
        configured_limit = min(int(batch_limit), MINUTE_SEND_LIMIT)
    else:
        configured_limit = MINUTE_SEND_LIMIT
    today = _day_key(ts)
    sent_today = _messages(db_ref).count_documents({"campaign_id": CAMPAIGN_ID, "status": "sent", "sent_day": today})
    remaining_today = max(0, DAILY_SEND_LIMIT - int(sent_today))
    per_run_limit = min(_coerce_per_run_limit(configured_limit, default=MINUTE_SEND_LIMIT), remaining_today)
    stats["per_run_limit"] = per_run_limit
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
            elif _is_blocked_send_error(err):
                _record_message(db_ref, uid, "failed_blocked", ts, error=err, failed_at=ts)
                db_ref.users.update_one(
                    {"user_id": user_doc.get("user_id", uid)},
                    {"$set": {"reactivation_failed_blocked_at": ts, "reactivation_failure_reason": err}},
                )
                logger.warning("[CHANNEL_REACTIVATION] send_blocked uid=%s err=%s", uid, err)
                stats["failed_blocked"] += 1
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
    existing_reward = _rewards(db).find_one({"campaign_id": CAMPAIGN_ID, "user_id": uid})
    if existing_reward and existing_reward.get("status") == "pending":
        return {
            "success": True,
            "code": "pending",
            "message": VERIFICATION_PENDING_MESSAGE,
            "reward_due_at": existing_reward.get("reward_due_at"),
        }
    if existing_reward and existing_reward.get("status") == "cancelled":
        return {"success": False, "code": "cancelled", "message": "This reward was cancelled because the subscription requirement was not maintained."}
    if existing_reward and existing_reward.get("status") == "rewarded":
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

    reward_due_at = ts + timedelta(hours=REWARD_HOLD_HOURS)
    reward_doc = {
        "campaign_id": CAMPAIGN_ID,
        "user_id": uid,
        "status": "pending",
        "verified_at": ts,
        "reward_due_at": reward_due_at,
        "rewarded_at": None,
        "cancelled_at": None,
        "xp_awarded": 0,
        "membership_reason": reason,
    }
    try:
        _rewards(db).insert_one(reward_doc)
    except DuplicateKeyError:
        return {
            "success": True,
            "code": "pending",
            "message": VERIFICATION_PENDING_MESSAGE,
        }

    db.users.update_one(
        {"user_id": uid},
        {"$set": {"reactivation_verified_at": ts, "reactivation_reward_due_at": reward_due_at}},
    )
    if hasattr(db, "reactivation_journey") or hasattr(db, "__getitem__"):
        try:
            create_or_update_journey(db, uid, campaign_id=CAMPAIGN_ID, verified_at=ts, now_ref=ts)
        except Exception:
            logger.exception("[REACT_JOURNEY][ERROR] uid=%s campaign_id=%s reason=create_failed", uid, CAMPAIGN_ID)
    return {
        "success": True,
        "code": "pending",
        "message": VERIFICATION_PENDING_MESSAGE,
        "reward_due_at": reward_due_at,
    }


def process_pending_reactivation_rewards(
    *,
    db_ref,
    membership_checker: Callable[[int], tuple[bool, str]] | None = None,
    now_ref: datetime | None = None,
    batch_limit: int = 200,
) -> dict:
    ts = now_ref or now_utc()
    checker = membership_checker or check_official_channel_subscribed
    stats = {"scanned": 0, "rewarded": 0, "cancelled": 0, "failed": 0}
    cursor = _rewards(db_ref).find(
        {
            "campaign_id": CAMPAIGN_ID,
            "status": "pending",
            "reward_due_at": {"$lte": ts},
        },
        {"user_id": 1, "reward_due_at": 1, "verified_at": 1},
    ).limit(batch_limit)

    for reward_doc in cursor:
        stats["scanned"] += 1
        uid = int(reward_doc.get("user_id"))
        try:
            subscribed, reason = checker(uid)
            if not subscribed:
                _rewards(db_ref).update_one(
                    {"campaign_id": CAMPAIGN_ID, "user_id": uid, "status": "pending"},
                    {
                        "$set": {
                            "status": "cancelled",
                            "cancelled_at": ts,
                            "rewarded_at": None,
                            "xp_awarded": 0,
                            "membership_reason": reason,
                            "updated_at": ts,
                        }
                    },
                )
                db_ref.users.update_one(
                    {"user_id": uid},
                    {"$set": {"reactivation_reward_status": "cancelled", "reactivation_cancelled_at": ts}},
                )
                stats["cancelled"] += 1
                continue

            unique_key = f"{CAMPAIGN_ID}:{uid}"
            granted = grant_xp(db_ref, uid, "official_channel_reactivation", unique_key, REWARD_XP)
            if not granted:
                _rewards(db_ref).update_one(
                    {"campaign_id": CAMPAIGN_ID, "user_id": uid, "status": "pending"},
                    {
                        "$set": {
                            "status": "rewarded",
                            "rewarded_at": ts,
                            "cancelled_at": None,
                            "xp_awarded": 0,
                            "membership_reason": reason,
                            "updated_at": ts,
                        }
                    },
                )
                stats["rewarded"] += 1
                continue

            _rewards(db_ref).update_one(
                {"campaign_id": CAMPAIGN_ID, "user_id": uid, "status": "pending"},
                {
                    "$set": {
                        "status": "rewarded",
                        "rewarded_at": ts,
                        "cancelled_at": None,
                        "xp_awarded": REWARD_XP,
                        "membership_reason": reason,
                        "updated_at": ts,
                    }
                },
            )
            db_ref.users.update_one(
                {"user_id": uid},
                {
                    "$set": {
                        "reactivation_reward_claimed": True,
                        "reactivation_reward_claimed_at": ts,
                        "reactivation_reward_status": "rewarded",
                    }
                },
            )
            stats["rewarded"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("[CHANNEL_REACTIVATION] pending_reward_failed uid=%s", uid)
            _rewards(db_ref).update_one(
                {"campaign_id": CAMPAIGN_ID, "user_id": uid, "status": "pending"},
                {"$set": {"last_error": f"{exc.__class__.__name__}: {exc}", "updated_at": ts}},
            )
            stats["failed"] += 1
    return stats


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
