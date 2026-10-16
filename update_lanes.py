"""Update processor that keeps /start off the shared, serialized update lane.

python-telegram-bot's default processor handles updates strictly one at a time,
so a single slow handler (chat_member / join_request bursts, Telegram retries,
a stalled DB call) delays every later update — including /start — until it
returns. Running *everything* concurrently would change ordering guarantees the
referral/join handlers may rely on, so this processor only changes one thing:

  * /start commands are processed immediately, in their own task;
  * every other update still runs strictly one-at-a-time, in arrival order
    (an asyncio.Lock is FIFO), exactly as before.

It is a single Application / single poller; nothing about polling changes.
"""
from __future__ import annotations

import asyncio

from telegram import Update
from telegram.ext import BaseUpdateProcessor

# Large on purpose: updates parked on the serial lock hold a semaphore slot, and
# the bound must never be reachable by a backlog or /start would queue again.
MAX_PARKED_UPDATES = 100_000


def is_start_command(update: object) -> bool:
    message = getattr(update, "message", None) if isinstance(update, Update) else None
    text = getattr(message, "text", None)
    if not text:
        return False
    first = text.split(None, 1)[0]
    return first.split("@", 1)[0].lower() == "/start"


class StartFastLaneUpdateProcessor(BaseUpdateProcessor):
    def __init__(self, max_concurrent_updates: int = MAX_PARKED_UPDATES):
        super().__init__(max_concurrent_updates)
        self._serial_lock: asyncio.Lock | None = None

    async def initialize(self) -> None:
        self._serial_lock = asyncio.Lock()

    async def shutdown(self) -> None:
        self._serial_lock = None

    async def do_process_update(self, update, coroutine) -> None:
        if is_start_command(update):
            await coroutine
            return
        if self._serial_lock is None:  # not initialised (e.g. used standalone)
            self._serial_lock = asyncio.Lock()
        async with self._serial_lock:
            await coroutine
