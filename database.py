from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

from apscheduler.triggers.date import DateTrigger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app_context import get_bot, get_scheduler, run_bot_coroutine
from telegram_utils import safe_send_message, send_telegram_http_message
from database import get_collection, users_collection
from time_utils import as_aware_utc
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)
onboarding_events = get_collection("onboarding_events")

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


def send_pm1_if_needed(
    uid: int,
    *,
    return_error: bool = False,
) -> bool | tuple[bool, str | None, str | None]:
    user = users_collection.find_one(
        {"user_id": uid},
        {"first_checkin_at": 1, "pm_sent.pm_checkin_tip": 1, "vip_tier": 1, "status": 1},
    )
    if not user:
        logger.info("[PM1][SKIP] uid=%s reason=missing_user", uid)
        return (True, None, "missing_user") if return_error else False
    if user.get("first_checkin_at"):
        logger.info("[PM1][SKIP] uid=%s reason=already_checkin", uid)
        return (True, None, "already_checkin") if return_error else False
    if (user.get("pm_sent") or {}).get("pm_checkin_tip"):
        logger.info("[PM1][SKIP] uid=%s reason=already_sent", uid)
        return (True, None, "already_sent") if return_error else False
    if _is_vip1(user):
        logger.info("[PM1][SKIP] uid=%s reason=already_vip", uid)
        return (True, None, "already_vip") if return_error else False
    bot = get_bot()
    if bot:
        try:
            ok, err = run_bot_coroutine(
                safe_send_message(
                    bot,
                    chat_id=uid,
                    text=PM1_TEXT,
                    uid=uid,
                    send_type="pm1",
                    raise_on_non_transient=False,
                    return_error=True,
                ),
                timeout=70,
            )
        except RuntimeError as exc:
            if "Bot loop not running yet" not in str(exc):
                raise
            ok, err, blocked = send_telegram_http_message(uid, PM1_TEXT)
            if blocked:
                err = "bot_blocked"
    else:
        ok, err, blocked = send_telegram_http_message(uid, PM1_TEXT)
        if blocked:
            err = "bot_blocked"
    if ok:
        users_collection.update_one(
            {"user_id": uid},
            {"$set": {"pm_sent.pm_checkin_tip": now_utc()}},
        )
        logger.info("[PM1][SENT] uid=%s", uid)
        return (True, None, None) if return_error else True        
    else:
        logger.warning("[PM1][SEND_FAILED] uid=%s", uid)
        return (False, err, None) if return_error else False        


def send_pm2_if_needed(
    uid: int,
    *,
    return_error: bool = False,
) -> bool | tuple[bool, str | None, str | None]:
    user = users_collection.find_one(
        {"user_id": uid},
        {"first_mywin_at": 1, "pm_sent.pm_mywin_tip": 1, "vip_tier": 1, "status": 1},
    )
    if not user:
        logger.info("[PM2][SKIP] uid=%s reason=missing_user", uid)
        return (True, None, "missing_user") if return_error else False
    if user.get("first_mywin_at"):
        logger.info("[PM2][SKIP] uid=%s reason=already_mywin", uid)
        return (True, None, "already_mywin") if return_error else False
    if (user.get("pm_sent") or {}).get("pm_mywin_tip"):
        logger.info("[PM2][SKIP] uid=%s reason=already_sent", uid)
        return (True, None, "already_sent") if return_error else False
    if _is_vip1(user):
        logger.info("[PM2][SKIP] uid=%s reason=already_vip", uid)
        return (True, None, "already_vip") if return_error else False
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Open #mywin", url=MYWIN_INVITE_LINK)]]
    )
    bot = get_bot()
    if bot:
        try:
            ok, err = run_bot_coroutine(
                safe_send_message(
                    bot,
                    chat_id=uid,
                    text=PM2_TEXT,
                    reply_markup=keyboard,
                    uid=uid,
                    send_type="pm2",
                    raise_on_non_transient=False,
                    return_error=True,
                ),
                timeout=70,
            )
        except RuntimeError as exc:
            if "Bot loop not running yet" not in str(exc):
                raise
            ok, err, blocked = send_telegram_http_message(uid, PM2_TEXT)
            if blocked:
                err = "bot_blocked"
    else:
        ok, err, blocked = send_telegram_http_message(uid, PM2_TEXT)
        if blocked:
            err = "bot_blocked"
    if ok:
        users_collection.update_one(
            {"user_id": uid},
            {"$set": {"pm_sent.pm_mywin_tip": now_utc()}},
        )
        logger.info("[PM2][SENT] uid=%s", uid)
        return (True, None, None) if return_error else True        
    else:
        logger.warning("[PM2][SEND_FAILED] uid=%s", uid)
        return (False, err, None) if return_error else False

def send_pm3_if_needed(
    uid: int,
    *,
    return_error: bool = False,
) -> bool | tuple[bool, str | None, str | None]:
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
        return (True, None, "missing_user") if return_error else False
    if user.get("first_referral_at") or int(user.get("total_referrals", 0)) >= 1:
        logger.info("[PM3][SKIP] uid=%s reason=already_referral", uid)
        return (True, None, "already_referral") if return_error else False
    if (user.get("pm_sent") or {}).get("pm_referral_tip"):
        logger.info("[PM3][SKIP] uid=%s reason=already_sent", uid)
        return (True, None, "already_sent") if return_error else False
    if _is_vip1(user):
        logger.info("[PM3][SKIP] uid=%s reason=already_vip", uid)
        return (True, None, "already_vip") if return_error else False
    bot = get_bot()
    if bot:
        try:
            ok, err = run_bot_coroutine(
                safe_send_message(
                    bot,
                    chat_id=uid,
                    text=PM3_TEXT,
                    uid=uid,
                    send_type="pm3",
                    raise_on_non_transient=False,
                    return_error=True,
                ),
                timeout=70,
            )
        except RuntimeError as exc:
            if "Bot loop not running yet" not in str(exc):
                raise
            ok, err, blocked = send_telegram_http_message(uid, PM3_TEXT)
            if blocked:
                err = "bot_blocked"
    else:
        ok, err, blocked = send_telegram_http_message(uid, PM3_TEXT)
        if blocked:
            err = "bot_blocked"
    if ok:
        users_collection.update_one(
            {"user_id": uid},
            {"$set": {"pm_sent.pm_referral_tip": now_utc()}},
        )
        logger.info("[PM3][SENT] uid=%s", uid)
        return (True, None, None) if return_error else True        
    else:
        logger.warning("[PM3][SEND_FAILED] uid=%s", uid)
        return (False, err, None) if return_error else False

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
    ok = run_bot_coroutine(
        safe_send_message(
            bot,
            chat_id=uid,
            text=PM4_TEXT,
            uid=uid,
            send_type="pm4",
            raise_on_non_transient=False,
        ),
        timeout=70,
    )
    if ok:
        users_collection.update_one(
            {"user_id": uid},
            {"$set": {"pm_sent.pm_vip_unlocked": now_utc()}},
        )
        for job_id in (f"pm1:{uid}", f"pm2:{uid}", f"pm3:{uid}"):
            _remove_job(job_id)
        logger.info("[PM4][SENT] uid=%s", uid)
    else:
        logger.warning("[PM4][SEND_FAILED] uid=%s", uid)

def _onboarding_test_mode() -> bool:
    return os.getenv("ONBOARDING_TEST_MODE", "").lower() in {"1", "true", "yes"}


def _schedule_due_at(uid: int, due_field: str, sent_field: str, run_at: datetime) -> datetime | None:
    res = users_collection.update_one(
        {
            "user_id": uid,
            due_field: {"$exists": False},
            sent_field: {"$exists": False},
        },
        {"$set": {due_field: run_at}},
    )
    if getattr(res, "modified_count", 0):
        return run_at
    existing = users_collection.find_one({"user_id": uid}, {due_field: 1, sent_field: 1})
    if existing and existing.get(sent_field):
        return None
    return existing.get(due_field) if existing else None


def schedule_pm1(uid: int, ref: datetime | None = None) -> datetime:
    base = ref or now_utc()
    delay = timedelta(minutes=2) if _onboarding_test_mode() else timedelta(hours=2)
    run_at = base + delay
    stored = _schedule_due_at(uid, "pm1_due_at_utc", "pm1_sent_at_utc", run_at)
    if stored:
        _schedule_job(f"pm1:{uid}", stored, send_pm1_if_needed, uid)
        logger.info("[PM1][SCHEDULED] uid=%s run_at=%s", uid, stored.isoformat())
        return stored
    logger.info("[PM1][SCHEDULED] uid=%s run_at=existing", uid)
    return run_at


def schedule_pm2(uid: int, ref: datetime | None = None) -> datetime:
    base = ref or now_utc()
    if _onboarding_test_mode():
        run_at = base + timedelta(minutes=3)
    else:
        delay_hours = random.uniform(8, 12)
        run_at = base + timedelta(hours=delay_hours)
    stored = _schedule_due_at(uid, "pm2_due_at_utc", "pm2_sent_at_utc", run_at)
    if stored:
        _schedule_job(f"pm2:{uid}", stored, send_pm2_if_needed, uid)
        logger.info("[PM2][SCHEDULED] uid=%s run_at=%s", uid, stored.isoformat())
        return stored
    logger.info("[PM2][SCHEDULED] uid=%s run_at=existing", uid)
    return run_at


def schedule_pm3(uid: int, ref: datetime | None = None) -> datetime:
    base = ref or now_utc()
    delay = timedelta(minutes=4) if _onboarding_test_mode() else timedelta(hours=24)
    run_at = base + delay
    stored = _schedule_due_at(uid, "pm3_due_at_utc", "pm3_sent_at_utc", run_at)
    if stored:
        _schedule_job(f"pm3:{uid}", stored, send_pm3_if_needed, uid)
        logger.info("[PM3][SCHEDULED] uid=%s run_at=%s", uid, stored.isoformat())
        return stored
    logger.info("[PM3][SCHEDULED] uid=%s run_at=existing", uid)
    return run_at

def _acquire_onboarding_lock(ttl_seconds: int = 90) -> tuple[bool, dict | None]:
    from main import acquire_scheduler_lock

    return acquire_scheduler_lock("onboarding_due_tick", ttl_seconds=ttl_seconds)


def onboarding_due_tick() -> None:
    now = now_utc()
    acquired, _lock_doc = _acquire_onboarding_lock(ttl_seconds=90)
    if not acquired:
        logger.info("[ONBOARD][TICK] lock_not_acquired")
        return

    def _due_filter(due_field: str, sent_field: str, disabled_field: str) -> dict:
        return {
            due_field: {"$lte": now},
            sent_field: {"$exists": False},
            disabled_field: {"$ne": True},
            "onboarding_pm_blocked": {"$ne": True},
        }

    pm1_filter = _due_filter("pm1_due_at_utc", "pm1_sent_at_utc", "pm1_disabled")
    pm2_filter = _due_filter("pm2_due_at_utc", "pm2_sent_at_utc", "pm2_disabled")
    pm3_filter = _due_filter("pm3_due_at_utc", "pm3_sent_at_utc", "pm3_disabled")

    pm1_due_total = users_collection.count_documents(pm1_filter)
    pm2_due_total = users_collection.count_documents(pm2_filter)
    pm3_due_total = users_collection.count_documents(pm3_filter)
    due_total = pm1_due_total + pm2_due_total + pm3_due_total
    logger.info(
        "[ONBOARD][TICK] due_total=%s pm1=%s pm2=%s pm3=%s",
        due_total,
        pm1_due_total,
        pm2_due_total,
        pm3_due_total,
    )

    def _process_due(
        pm_name: str,
        due_field: str,
        sent_field: str,
        err_field: str,
        err_ts_field: str,
        disabled_field: str,
        send_func,
        filt: dict,
    ) -> None:
        docs = list(
            users_collection.find(filt, {"user_id": 1, due_field: 1})
            .sort(due_field, 1)
            .limit(50)
        )
        for doc in docs:
            uid = doc.get("user_id")
            if not uid:
                continue
            started = time.perf_counter()
            ok, err, skipped_reason = send_func(uid, return_error=True)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if ok:
                users_collection.update_one(
                    {"user_id": uid},
                    {
                        "$set": {sent_field: now},
                        "$unset": {err_field: "", err_ts_field: ""},
                    },
                )
                logger.info(
                    "[ONBOARD][SEND] pm=%s uid=%s ok=1 elapsed_ms=%s%s",
                    pm_name,
                    uid,
                    elapsed_ms,
                    f" skipped={skipped_reason}" if skipped_reason else "",
                )
                continue
            update = {"$set": {err_field: err or "unknown_error", err_ts_field: now}}
            if err == "bot_blocked":
                update["$set"]["onboarding_pm_blocked"] = True
                update["$set"][disabled_field] = True
            users_collection.update_one({"user_id": uid}, update)
            logger.info(
                "[ONBOARD][SEND] pm=%s uid=%s ok=0 elapsed_ms=%s err=%s",
                pm_name,
                uid,
                elapsed_ms,
                err,
            )

    try:
        _process_due(
            "pm1",
            "pm1_due_at_utc",
            "pm1_sent_at_utc",
            "pm1_last_error",
            "pm1_last_error_at_utc",
            "pm1_disabled",
            send_pm1_if_needed,
            pm1_filter,
        )
        _process_due(
            "pm2",
            "pm2_due_at_utc",
            "pm2_sent_at_utc",
            "pm2_last_error",
            "pm2_last_error_at_utc",
            "pm2_disabled",
            send_pm2_if_needed,
            pm2_filter,
        )
        _process_due(
            "pm3",
            "pm3_due_at_utc",
            "pm3_sent_at_utc",
            "pm3_last_error",
            "pm3_last_error_at_utc",
            "pm3_disabled",
            send_pm3_if_needed,
            pm3_filter,
        )
    except RuntimeError as exc:
        if "Bot loop not running yet" in str(exc):
            logger.info("[ONBOARD][TICK] skipped_reason=bot_loop_not_ready")
            return
        raise

def record_visible_ping(
    uid: int | None, *, ref: datetime | None = None, interval_seconds: int = 60
) -> dict:
    if not uid:
        logger.warning("[E0][VISIBLE_PING][WARN] uid missing")
        return {"ok": False, "action": "missing_uid"}
    now = as_aware_utc(ref) or now_utc()
    threshold = now - timedelta(seconds=interval_seconds)
    try:
        existing = users_collection.find_one(
            {"user_id": uid}, {"_id": 1, "last_visible_at": 1}
        )
        if existing:
            last_visible = as_aware_utc(existing.get("last_visible_at"))
            if last_visible and last_visible >= threshold:
                logger.info("[E0][VISIBLE_PING][THROTTLED] uid=%s", uid)
                return {"ok": True, "action": "throttled"}
            users_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": {"last_visible_at": now}},
                upsert=False,
            )
            logger.info("[E0][VISIBLE_PING][UPDATED] uid=%s", uid)
            return {"ok": True, "action": "updated"}
        try:
            users_collection.insert_one(
                {"user_id": uid, "created_at": now, "last_visible_at": now}
            )
            logger.info("[E0][VISIBLE_PING][CREATED_MINIMAL] uid=%s", uid)
            return {"ok": True, "action": "created_minimal"}
        except DuplicateKeyError:
            existing = users_collection.find_one(
                {"user_id": uid}, {"_id": 1, "last_visible_at": 1}
            )
            if not existing:
                logger.warning(
                    "[E0][VISIBLE_PING][WARN] uid=%s err=duplicate_without_doc",
                    uid,
                )
                return {"ok": False, "action": "warn"}
            users_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": {"last_visible_at": now}},
                upsert=False,
            )
            logger.info("[E0][VISIBLE_PING][RACE_RECOVERED] uid=%s", uid)
            return {"ok": True, "action": "race_recovered"}
    except Exception as exc:
        logger.warning("[E0][VISIBLE_PING][WARN] uid=%s err=%s", uid, exc)
        return {"ok": False, "action": "warn"}


def _write_onboarding_event(uid: int, *, source: str, ts: datetime) -> None:
    try:
        onboarding_events.insert_one(
            {
                "_id": f"E0:{uid}",
                "uid": uid,
                "type": "E0_onboarding_start",
                "source": source,
                "ts": ts,
            }
        )
        logger.info("[E0][EVENT_WRITE][OK] uid=%s", uid)
    except DuplicateKeyError:
        logger.info("[E0][EVENT_WRITE][OK] uid=%s", uid)
    except Exception as exc:
        logger.warning("[E0][EVENT_WRITE][WARN] uid=%s err=%s", uid, exc)


def record_onboarding_start(
    uid: int | None, *, source: str = "visible", ref: datetime | None = None
) -> bool:
    if not uid:
        logger.warning("[E0][ONBOARDING_START][WARN] uid missing source=%s", source)
        return False
    now = ref or now_utc()
    fields = {"onboarding_started_at": now, "onboarding_source": source}
    try:
        existing = users_collection.find_one(
            {"user_id": uid}, {"_id": 1, "onboarding_started_at": 1}
        )
        if existing:
            if existing.get("onboarding_started_at"):
                logger.info(
                    "[E0][ONBOARDING_START][EXISTING] uid=%s source=%s",
                    uid,
                    source,
                )
                return False
            res = users_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": fields},
                upsert=False,
            )
            created = bool(getattr(res, "modified_count", 0))
            if created:
                logger.info(
                    "[E0][ONBOARDING_START][CREATED] uid=%s source=%s",
                    uid,
                    source,
                )
                schedule_pm1(uid, now)
                _write_onboarding_event(uid, source=source, ts=now)
                return True
            logger.info(
                "[E0][ONBOARDING_START][EXISTING] uid=%s source=%s",
                uid,
                source,
            )
            return False
        try:
            users_collection.insert_one({"user_id": uid, **fields})
            logger.info(
                "[E0][ONBOARDING_START][CREATED] uid=%s source=%s",
                uid,
                source,
            )
            schedule_pm1(uid, now)
            _write_onboarding_event(uid, source=source, ts=now)
            return True
        except DuplicateKeyError:
            existing = users_collection.find_one(
                {"user_id": uid}, {"_id": 1, "onboarding_started_at": 1}
            )
            if not existing:
                logger.warning(
                    "[E0][ONBOARDING_START][WARN] uid=%s err=duplicate_without_doc source=%s",
                    uid,
                    source,
                )
                return False
            if existing.get("onboarding_started_at"):
                logger.info(
                    "[E0][ONBOARDING_START][EXISTING] uid=%s source=%s",
                    uid,
                    source,
                )
                return False
            res = users_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": fields},
                upsert=False,
            )
            created = bool(getattr(res, "modified_count", 0))
            if created:
                logger.info(
                    "[E0][ONBOARDING_START][CREATED] uid=%s source=%s",
                    uid,
                    source,
                )
                schedule_pm1(uid, now)
                _write_onboarding_event(uid, source=source, ts=now)
                return True
            logger.info(
                "[E0][ONBOARDING_START][EXISTING] uid=%s source=%s",
                uid,
                source,
            )
            return False
    except Exception as exc:
        logger.warning(
            "[E0][ONBOARDING_START][WARN] uid=%s source=%s err=%s",
            uid,
            source,
            exc,
        )
        return False


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
