from __future__ import annotations

import asyncio
import logging
import random
import time
import os
from typing import Any

import httpx
import requests
from telegram.error import BadRequest, Forbidden, NetworkError

logger = logging.getLogger(__name__)

TRANSIENT_EXCEPTIONS = (
    NetworkError,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    TimeoutError,
)

def send_telegram_http_message(
    chat_id: int,
    text: str,
    *,
    parse_mode: str | None = None,
    token: str | None = None,
    timeout: int = 10,
    log: logging.Logger = logger,
) -> tuple[bool, str | None, bool]:
    bot_token = token or os.environ.get("BOT_TOKEN", "")
    if not bot_token:
        return False, "missing_bot_token", False
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        log.warning("[E2][PM_SEND_HTTP][FAIL] chat_id=%s err=%s", chat_id, exc)
        return False, f"{exc.__class__.__name__}: {exc}", False
    data = None
    try:
        data = resp.json()
    except ValueError:
        data = None
    if resp.status_code == 200 and isinstance(data, dict) and data.get("ok"):
        return True, None, False
    error_code = data.get("error_code") if isinstance(data, dict) else None
    description = data.get("description") if isinstance(data, dict) else None
    if resp.status_code == 403 or error_code == 403:
        return False, "bot_blocked", True
    if resp.status_code == 429 or error_code == 429:
        return False, "rate_limited", False
    err = description or f"telegram_http_{resp.status_code}"
    return False, err, False

def _resolve_chat_id(message: Any, chat_id: int | None) -> int | None:
    if chat_id is not None:
        return chat_id
    if not message:
        return None
    return getattr(message, "chat_id", None) or getattr(getattr(message, "chat", None), "id", None)


def _resolve_uid(message: Any, uid: int | None) -> int | None:
    if uid is not None:
        return uid
    if not message:
        return None
    return getattr(getattr(message, "from_user", None), "id", None)


async def safe_reply_text(message, text: str, **kwargs) -> bool:
    """Safely reply with retries for transient Telegram/httpx errors."""
    uid = _resolve_uid(message, kwargs.pop("uid", None))
    chat_id = _resolve_chat_id(message, kwargs.pop("chat_id", None))
    send_type = kwargs.pop("send_type", "unknown")
    attempts = int(kwargs.pop("attempts", 3))
    backoffs = tuple(kwargs.pop("backoffs", (0.5, 1.0, 2.0)))
    jitter = float(kwargs.pop("jitter", 0.1))
    raise_on_non_transient = bool(kwargs.pop("raise_on_non_transient", False))
    return_error = bool(kwargs.pop("return_error", False))    
    fallback_on_bad_request = bool(kwargs.pop("fallback_on_bad_request", True))
    log = kwargs.pop("logger", logger)

    started = time.perf_counter()
    attempt = 1
    while attempt <= attempts:
        log.info(
            "[E2][PM_SEND][ATTEMPT] uid=%s chat_id=%s type=%s attempt=%s",
            uid,
            chat_id,
            send_type,
            attempt,
        )
        try:
            await message.reply_text(text, **kwargs)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            log.info(
                "[E2][PM_SEND][OK] uid=%s chat_id=%s type=%s elapsed_ms=%s",
                uid,
                chat_id,
                send_type,
                elapsed_ms,
            )
            return (True, None) if return_error else True
        except BadRequest as exc:
            if fallback_on_bad_request and "reply_markup" in kwargs:
                log.warning(
                    "[E2][PM_SEND][RETRY] uid=%s chat_id=%s type=%s attempt=%s err=%s",
                    uid,
                    chat_id,
                    send_type,
                    attempt,
                    exc,
                )
                kwargs.pop("reply_markup", None)
                fallback_on_bad_request = False
                attempt += 1
                continue
            log.error(
                "[E2][PM_SEND][FAIL] uid=%s chat_id=%s type=%s attempts=%s err=%s",
                uid,
                chat_id,
                send_type,
                attempt,
                exc,
            )
            if raise_on_non_transient:
                raise
            if return_error:
                err = "bot_blocked" if _is_bot_blocked_error(exc) else f"{exc.__class__.__name__}: {exc}"
                return False, err                
            return False
        except TRANSIENT_EXCEPTIONS as exc:
            if attempt >= attempts:
                log.error(
                    "[E2][PM_SEND][FAIL] uid=%s chat_id=%s type=%s attempts=%s err=%s",
                    uid,
                    chat_id,
                    send_type,
                    attempt,
                    exc,
                )
                if return_error:
                    return False, f"{exc.__class__.__name__}: {exc}"                
                return False
            next_attempt = attempt + 1
            log.warning(
                "[E2][PM_SEND][RETRY] uid=%s chat_id=%s type=%s attempt=%s err=%s",
                uid,
                chat_id,
                send_type,
                next_attempt,
                exc,
            )
            backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
            await asyncio.sleep(backoff + random.uniform(0, jitter))
            attempt += 1
            continue
        except Exception as exc:  # noqa: BLE001
            log.error(
                "[E2][PM_SEND][FAIL] uid=%s chat_id=%s type=%s attempts=%s err=%s",
                uid,
                chat_id,
                send_type,
                attempt,
                exc,
            )
            if raise_on_non_transient:
                raise
            if return_error:
                return False, f"{exc.__class__.__name__}: {exc}"                
            return False
    return False


def _is_bot_blocked_error(exc: Exception) -> bool:
    if isinstance(exc, Forbidden):
        return True
    message = str(exc).lower()
    return "bot was blocked" in message or "forbidden" in message


async def safe_send_message(
    bot, chat_id: int, text: str, **kwargs
) -> bool | tuple[bool, str | None]:
    """Safely send a message with retries for transient Telegram/httpx errors."""
    uid = kwargs.pop("uid", None) or chat_id
    send_type = kwargs.pop("send_type", "unknown")
    attempts = int(kwargs.pop("attempts", 3))
    backoffs = tuple(kwargs.pop("backoffs", (0.5, 1.0, 2.0)))
    jitter = float(kwargs.pop("jitter", 0.1))
    raise_on_non_transient = bool(kwargs.pop("raise_on_non_transient", False))
    return_error = bool(kwargs.pop("return_error", False))    
    fallback_on_bad_request = bool(kwargs.pop("fallback_on_bad_request", True))
    log = kwargs.pop("logger", logger)

    started = time.perf_counter()
    attempt = 1
    while attempt <= attempts:
        log.info(
            "[E2][PM_SEND][ATTEMPT] uid=%s chat_id=%s type=%s attempt=%s",
            uid,
            chat_id,
            send_type,
            attempt,
        )
        try:
            await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            log.info(
                "[E2][PM_SEND][OK] uid=%s chat_id=%s type=%s elapsed_ms=%s",
                uid,
                chat_id,
                send_type,
                elapsed_ms,
            )
            return (True, None) if return_error else True
        except BadRequest as exc:
            if fallback_on_bad_request and "reply_markup" in kwargs:
                log.warning(
                    "[E2][PM_SEND][RETRY] uid=%s chat_id=%s type=%s attempt=%s err=%s",
                    uid,
                    chat_id,
                    send_type,
                    attempt,
                    exc,
                )
                kwargs.pop("reply_markup", None)
                fallback_on_bad_request = False
                attempt += 1
                continue
            log.error(
                "[E2][PM_SEND][FAIL] uid=%s chat_id=%s type=%s attempts=%s err=%s",
                uid,
                chat_id,
                send_type,
                attempt,
                exc,
            )
            if raise_on_non_transient:
                raise
            if return_error:
                err = "bot_blocked" if _is_bot_blocked_error(exc) else f"{exc.__class__.__name__}: {exc}"
                return False, err                
            return False
        except TRANSIENT_EXCEPTIONS as exc:
            if attempt >= attempts:
                log.error(
                    "[E2][PM_SEND][FAIL] uid=%s chat_id=%s type=%s attempts=%s err=%s",
                    uid,
                    chat_id,
                    send_type,
                    attempt,
                    exc,
                )
                if return_error:
                    return False, f"{exc.__class__.__name__}: {exc}"                
                return False
            next_attempt = attempt + 1
            log.warning(
                "[E2][PM_SEND][RETRY] uid=%s chat_id=%s type=%s attempt=%s err=%s",
                uid,
                chat_id,
                send_type,
                next_attempt,
                exc,
            )
            backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
            await asyncio.sleep(backoff + random.uniform(0, jitter))
            attempt += 1
            continue
        except Exception as exc:  # noqa: BLE001
            log.error(
                "[E2][PM_SEND][FAIL] uid=%s chat_id=%s type=%s attempts=%s err=%s",
                uid,
                chat_id,
                send_type,
                attempt,
                exc,
            )
            if raise_on_non_transient:
                raise
            if return_error:
                err = "bot_blocked" if _is_bot_blocked_error(exc) else f"{exc.__class__.__name__}: {exc}"
                return False, err
            if return_error:
                return False, f"{exc.__class__.__name__}: {exc}"                
            return False
    return (False, "unknown_failure") if return_error else False
