from __future__ import annotations

import asyncio
from typing import Any

_app_bot = None
_bot = None
_scheduler = None


def set_app_bot(app_bot) -> None:
    global _app_bot
    _app_bot = app_bot


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


def get_scheduler():
    return _scheduler


def run_bot_coroutine(coro, *, timeout: int = 15) -> Any:
    app_bot = get_app_bot()
    loop = getattr(app_bot, "_running_loop", None) if app_bot else None
    if loop is None:
        raise RuntimeError("Bot loop not running yet")
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=timeout)
