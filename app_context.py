from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

_app_bot = None
_bot = None
_scheduler = None
_reactivation_handler_registered = False
_reactivation_job_registered = False
logger = logging.getLogger(__name__)


def set_app_bot(app_bot) -> None:
    global _app_bot
    _app_bot = app_bot
    _register_reactivation_callback(app_bot)


def get_app_bot():
    return _app_bot


def set_bot(bot) -> None:
    global _bot
    _bot = bot


def get_bot():
    return _bot


def set_scheduler(scheduler) -> None:
    global _scheduler
    _scheduler = scheduler
    _register_reactivation_job(scheduler)


def get_scheduler():
    return _scheduler


def _register_reactivation_callback(app_bot) -> None:
    global _reactivation_handler_registered
    if _reactivation_handler_registered or app_bot is None:
        return
    try:
        from telegram.ext import ApplicationHandlerStop, CallbackQueryHandler
        from channel_reactivation import VERIFY_CALLBACK_DATA, verify_reactivation_claim
        from database import db

        async def _handler(update, context):
            query = update.callback_query
            if not query or query.data != VERIFY_CALLBACK_DATA:
                return
            user_id = query.from_user.id
            result = verify_reactivation_claim(db, user_id)
            message = result.get("message") or "Unable to verify right now."
            await query.answer(message, show_alert=True)
            if result.get("success"):
                try:
                    await query.edit_message_text(message)
                except Exception:
                    await context.bot.send_message(chat_id=user_id, text=message)
            raise ApplicationHandlerStop

        app_bot.add_handler(
            CallbackQueryHandler(_handler, pattern=f"^{VERIFY_CALLBACK_DATA}$"),
            group=-1,
        )
        _reactivation_handler_registered = True
        logger.info("[CHANNEL_REACTIVATION] callback handler registered")
    except Exception:
        logger.exception("[CHANNEL_REACTIVATION] callback handler registration failed")


def _register_reactivation_job(scheduler) -> None:
    global _reactivation_job_registered
    if _reactivation_job_registered or scheduler is None:
        return
    try:
        import pytz
        from apscheduler.triggers.cron import CronTrigger
        from channel_reactivation import ensure_channel_reactivation_indexes, process_reactivation_campaign
        from database import db

        ensure_channel_reactivation_indexes(db)
        tz_name = os.getenv("SCHEDULER_CRON_TIMEZONE", "Asia/Kuala_Lumpur")
        scheduler.add_job(
            process_reactivation_campaign,
            trigger=CronTrigger(minute="*/1", timezone=pytz.timezone(tz_name)),
            id="channel_reactivation_campaign",
            name="Channel Re-Activation Campaign",
            replace_existing=True,
            kwargs={"db_ref": db},
            max_instances=1,
            coalesce=True,
        )
        _reactivation_job_registered = True
        logger.info("[CHANNEL_REACTIVATION] scheduler job registered")
    except Exception:
        logger.exception("[CHANNEL_REACTIVATION] scheduler job registration failed")


def run_bot_coroutine(coro, *, timeout: int = 15) -> Any:
    app_bot = get_app_bot()
    loop = getattr(app_bot, "_running_loop", None) if app_bot else None
    if loop is None:
        raise RuntimeError("Bot loop not running yet")
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=timeout)
