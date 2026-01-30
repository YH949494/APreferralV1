from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone

from apscheduler.triggers.date import DateTrigger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app_context import get_bot, get_scheduler, run_bot_coroutine
from database import users_collection

logger = logging.getLogger(__name__)

MYWIN_CHAT_ID = -1002743212540
MYWIN_INVITE_LINK = "https://t.me/+tgGbOPvp1p05NjA9"

PM1_TEXT = (
    "Quick tip 👋\n\n"
    "You’ve already started your New Member Path.\n"
    "A daily check-in is the easiest way to make progress.\n\n"
    "Most members do this once a day."
)
PM2_TEXT = (
    "You’re making progress 👍\n\n"
    "Sharing your first win gives a solid boost\n"
    "and helps you move closer to VIP 1.\n\n"
    "A screenshot in #mywin is enough."
)
PM3_TEXT = (
    "You’re almost there.\n\n"
    "Inviting just one friend completes your\n"
    "New Member Path and unlocks VIP 1.\n\n"
    "Your invite link is ready in the mini app."
)
PM4_TEXT = (
    "Welcome to VIP 1 🎉\n\n"
    "You’re now part of our core members.\n"
    "This is where better access and opportunities begin."
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _remove_job(job_id: str) -> None:
    scheduler = get_scheduler()
    if not scheduler:
        return
    try:
        scheduler.remove_job(job_id)
    except Exception:
        return


def _schedule_job(job_id: str, run_at: datetime, func, uid: int) -> None:
    scheduler = get_scheduler()
    if not scheduler:
        logger.info("[PM][SCHEDULE_SKIPPED] uid=%s reason=no_scheduler", uid)
        return
    _remove_job(job_id)
    scheduler.add_job(
        func,
        trigger=DateTrigger(run_date=run_at),
        id=job_id,
        replace_existing=True,
        kwargs={"uid": uid},
        misfire_grace_time=3600,
        max_instances=1,
    )


def _vip_field(doc: dict | None) -> str | None:
    if not doc:
        return None
    return doc.get("vip_tier") or doc.get("status")


def _is_vip1(doc: dict | None) -> bool:
    return (_vip_field(doc) or "").upper() == "VIP1"


def send_pm1_if_needed(uid: int) -> None:
    user = users_collection.find_one(
        {"user_id": uid},
        {"first_checkin_at": 1, "pm_sent.pm_checkin_tip": 1, "vip_tier": 1, "status": 1},
    )
    if not user:
        logger.info("[PM1][SKIP] uid=%s reason=missing_user", uid)
        return
    if user.get("first_checkin_at"):
        logger.info("[PM1][SKIP] uid=%s reason=already_checkin", uid)
        return
    if (user.get("pm_sent") or {}).get("pm_checkin_tip"):
        logger.info("[PM1][SKIP] uid=%s reason=already_sent", uid)
        return
    if _is_vip1(user):
        logger.info("[PM1][SKIP] uid=%s reason=already_vip", uid)
        return
    bot = get_bot()
    if not bot:
        logger.info("[PM1][SKIP] uid=%s reason=missing_bot", uid)
        return
    run_bot_coroutine(bot.send_message(chat_id=uid, text=PM1_TEXT))
    users_collection.update_one(
        {"user_id": uid},
        {"$set": {"pm_sent.pm_checkin_tip": now_utc()}},
    )
    logger.info("[PM1][SENT] uid=%s", uid)


def send_pm2_if_needed(uid: int) -> None:
    user = users_collection.find_one(
        {"user_id": uid},
        {"first_mywin_at": 1, "pm_sent.pm_mywin_tip": 1, "vip_tier": 1, "status": 1},
    )
    if not user:
        logger.info("[PM2][SKIP] uid=%s reason=missing_user", uid)
        return
    if user.get("first_mywin_at"):
        logger.info("[PM2][SKIP] uid=%s reason=already_mywin", uid)
        return
    if (user.get("pm_sent") or {}).get("pm_mywin_tip"):
        logger.info("[PM2][SKIP] uid=%s reason=already_sent", uid)
        return
    if _is_vip1(user):
        logger.info("[PM2][SKIP] uid=%s reason=already_vip", uid)
        return
    bot = get_bot()
    if not bot:
        logger.info("[PM2][SKIP] uid=%s reason=missing_bot", uid)
        return
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Open #mywin", url=MYWIN_INVITE_LINK)]]
    )
    run_bot_coroutine(bot.send_message(chat_id=uid, text=PM2_TEXT, reply_markup=keyboard))
    users_collection.update_one(
        {"user_id": uid},
        {"$set": {"pm_sent.pm_mywin_tip": now_utc()}},
    )
    logger.info("[PM2][SENT] uid=%s", uid)


def send_pm3_if_needed(uid: int) -> None:
    user = users_collection.find_one(
        {"user_id": uid},
        {
            "first_referral_at": 1,
            "total_referrals": 1,
            "pm_sent.pm_referral_tip": 1,
            "vip_tier": 1,
            "status": 1,
        },
    )
    if not user:
        logger.info("[PM3][SKIP] uid=%s reason=missing_user", uid)
        return
    if user.get("first_referral_at") or int(user.get("total_referrals", 0)) >= 1:
        logger.info("[PM3][SKIP] uid=%s reason=already_referral", uid)
        return
    if (user.get("pm_sent") or {}).get("pm_referral_tip"):
        logger.info("[PM3][SKIP] uid=%s reason=already_sent", uid)
        return
    if _is_vip1(user):
        logger.info("[PM3][SKIP] uid=%s reason=already_vip", uid)
        return
    bot = get_bot()
    if not bot:
        logger.info("[PM3][SKIP] uid=%s reason=missing_bot", uid)
        return
    run_bot_coroutine(bot.send_message(chat_id=uid, text=PM3_TEXT))
    users_collection.update_one(
        {"user_id": uid},
        {"$set": {"pm_sent.pm_referral_tip": now_utc()}},
    )
    logger.info("[PM3][SENT] uid=%s", uid)


def send_pm4_if_needed(uid: int) -> None:
    user = users_collection.find_one(
        {"user_id": uid},
        {"pm_sent.pm_vip_unlocked": 1},
    )
    if not user:
        logger.info("[PM4][SKIP] uid=%s reason=missing_user", uid)
        return
    if (user.get("pm_sent") or {}).get("pm_vip_unlocked"):
        logger.info("[PM4][SKIP] uid=%s reason=already_sent", uid)
        return
    bot = get_bot()
    if not bot:
        logger.info("[PM4][SKIP] uid=%s reason=missing_bot", uid)
        return
    run_bot_coroutine(bot.send_message(chat_id=uid, text=PM4_TEXT))
    users_collection.update_one(
        {"user_id": uid},
        {"$set": {"pm_sent.pm_vip_unlocked": now_utc()}},
    )
    for job_id in (f"pm1:{uid}", f"pm2:{uid}", f"pm3:{uid}"):
        _remove_job(job_id)
    logger.info("[PM4][SENT] uid=%s", uid)


def schedule_pm1(uid: int, ref: datetime | None = None) -> datetime:
    run_at = (ref or now_utc()) + timedelta(hours=2)
    _schedule_job(f"pm1:{uid}", run_at, send_pm1_if_needed, uid)
    logger.info("[PM1][SCHEDULED] uid=%s run_at=%s", uid, run_at.isoformat())
    return run_at


def schedule_pm2(uid: int, ref: datetime | None = None) -> datetime:
    delay_hours = random.uniform(8, 12)
    run_at = (ref or now_utc()) + timedelta(hours=delay_hours)
    _schedule_job(f"pm2:{uid}", run_at, send_pm2_if_needed, uid)
    logger.info("[PM2][SCHEDULED] uid=%s run_at=%s", uid, run_at.isoformat())
    return run_at


def schedule_pm3(uid: int, ref: datetime | None = None) -> datetime:
    run_at = (ref or now_utc()) + timedelta(hours=24)
    _schedule_job(f"pm3:{uid}", run_at, send_pm3_if_needed, uid)
    logger.info("[PM3][SCHEDULED] uid=%s run_at=%s", uid, run_at.isoformat())
    return run_at


def record_onboarding_start(uid: int, *, ref: datetime | None = None) -> bool:
    now = ref or now_utc()
    res = users_collection.update_one(
        {"user_id": uid, "onboarding_started_at": {"$exists": False}},
        {"$set": {"onboarding_started_at": now, "onboarding_source": "tg_initdata"}},
        upsert=True,
    )
    created = bool(getattr(res, "modified_count", 0)) or bool(
        getattr(res, "upserted_id", None)
    )
    logger.info("[ONBOARDING_START] uid=%s created=%s", uid, 1 if created else 0)
    if created:
        schedule_pm1(uid, now)
    return created


def record_first_checkin(uid: int, *, ref: datetime | None = None) -> bool:
    now = ref or now_utc()
    res = users_collection.update_one(
        {"user_id": uid, "first_checkin_at": {"$exists": False}},
        {"$set": {"first_checkin_at": now}},
    )
    created = bool(getattr(res, "modified_count", 0))
    logger.info("[FIRST_CHECKIN] uid=%s created=%s", uid, 1 if created else 0)
    if created:
        _remove_job(f"pm1:{uid}")
        logger.info("[PM1][CANCEL] uid=%s", uid)
        schedule_pm2(uid, now)
    return created


def record_first_mywin(uid: int, chat_id: int, message_id: int, *, ref: datetime | None = None) -> bool:
    now = ref or now_utc()
    res = users_collection.update_one(
        {"user_id": uid, "first_mywin_at": {"$exists": False}},
        {
            "$set": {
                "first_mywin_at": now,
                "first_mywin_chat_id": chat_id,
                "first_mywin_msg_id": message_id,
            }
        },
    )
    created = bool(getattr(res, "modified_count", 0))
    logger.info(
        "[FIRST_MYWIN] uid=%s created=%s chat_id=%s",
        uid,
        1 if created else 0,
        chat_id,
    )
    if created:
        _remove_job(f"pm2:{uid}")
        logger.info("[PM2][CANCEL] uid=%s", uid)
        schedule_pm3(uid, now)
    return created


def record_first_referral(uid: int, *, ref: datetime | None = None) -> bool:
    now = ref or now_utc()
    res = users_collection.update_one(
        {"user_id": uid, "first_referral_at": {"$exists": False}},
        {"$set": {"first_referral_at": now}},
    )
    created = bool(getattr(res, "modified_count", 0))
    logger.info("[FIRST_REFERRAL] uid=%s created=%s", uid, 1 if created else 0)
    if created:
        _remove_job(f"pm3:{uid}")
        logger.info("[PM3][CANCEL] uid=%s", uid)
    return created


def maybe_unlock_vip1(uid: int) -> bool:
    user = users_collection.find_one(
        {"user_id": uid},
        {
            "onboarding_started_at": 1,
            "first_checkin_at": 1,
            "first_mywin_at": 1,
            "first_referral_at": 1,
            "monthly_xp": 1,
            "vip_tier": 1,
            "status": 1,
            "pm_sent.pm_vip_unlocked": 1,
        },
    )
    if not user:
        return False
    if _is_vip1(user):
        return False
    if not (
        user.get("onboarding_started_at")
        and user.get("first_checkin_at")
        and user.get("first_mywin_at")
        and user.get("first_referral_at")
    ):
        return False
    if int(user.get("monthly_xp", 0)) < 800:
        return False
    now = now_utc()
    users_collection.update_one(
        {"user_id": uid},
        {"$set": {"status": "VIP1", "vip_tier": "VIP1", "vip_unlocked_at": now}},
    )
    logger.info("[VIP][UNLOCKED] uid=%s", uid)
    send_pm4_if_needed(uid)
    return True
