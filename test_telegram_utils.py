import asyncio
import logging
from unittest.mock import patch

import requests
from telegram.error import BadRequest, Forbidden, NetworkError

from telegram_utils import (
    _classify_permanent_telegram_error,
    safe_send_message,
    send_telegram_http_message,
    safe_reply_text,
)


class DummyMessage:
    def __init__(self):
        self.chat_id = 123
        self.from_user = type("User", (), {"id": 456})
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) < 3:
            raise NetworkError("temporary")
        return True


class BadMarkupMessage:
    def __init__(self):
        self.chat_id = 999
        self.from_user = type("User", (), {"id": 111})
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append(kwargs)
        if "reply_markup" in kwargs:
            raise BadRequest("Bad Request: can't parse keyboard")
        return True


def test_safe_reply_text_retries_then_succeeds():
    msg = DummyMessage()
    ok = asyncio.run(
        safe_reply_text(
            msg,
            "hello",
            send_type="start",
            backoffs=(0, 0, 0),
            jitter=0.0,
            logger=logging.getLogger("test"),
        )
    )
    assert ok is True
    assert len(msg.calls) == 3


def test_safe_reply_text_fallback_removes_markup():
    msg = BadMarkupMessage()
    ok = asyncio.run(
        safe_reply_text(
            msg,
            "hello",
            send_type="start",
            reply_markup={"bad": "markup"},
            backoffs=(0, 0, 0),
            jitter=0.0,
            logger=logging.getLogger("test"),
        )
    )
    assert ok is True
    assert len(msg.calls) == 2
    assert "reply_markup" in msg.calls[0]
    assert "reply_markup" not in msg.calls[1]


def test_send_telegram_http_message_retries_then_succeeds():
    calls = {"n": 0}

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"ok": True}

    def _fake_post(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.ConnectionError("reset")
        return _Resp()

    with patch("telegram_utils.requests.post", side_effect=_fake_post), patch("telegram_utils.time.sleep", return_value=None):
        ok, err, blocked = send_telegram_http_message(123, "hello", token="t")

    assert ok is True
    assert err is None
    assert blocked is False
    assert calls["n"] == 3


def test_classify_permanent_telegram_error_bot_blocked():
    assert _classify_permanent_telegram_error(Forbidden("bot was blocked")) == "bot_blocked"


def test_classify_permanent_telegram_error_chat_not_found():
    exc = BadRequest("Bad Request: chat not found")
    assert _classify_permanent_telegram_error(exc) == "chat_not_found"


def test_classify_permanent_telegram_error_user_deactivated():
    exc = BadRequest("Bad Request: user is deactivated")
    assert _classify_permanent_telegram_error(exc) == "user_deactivated"


def test_classify_permanent_telegram_error_transient_is_none():
    assert _classify_permanent_telegram_error(NetworkError("connection reset")) is None


def test_send_telegram_http_message_classifies_chat_not_found():
    class _Resp:
        status_code = 400

        @staticmethod
        def json():
            return {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}

    with patch("telegram_utils.requests.post", return_value=_Resp()):
        ok, err, blocked = send_telegram_http_message(123, "hello", token="t")

    assert ok is False
    assert err == "chat_not_found"
    assert blocked is False


def test_safe_send_message_bad_request_chat_not_found_returns_permanent_code():
    class ChatNotFoundMessageBot:
        async def send_message(self, chat_id=None, text=None, **kwargs):  # noqa: ARG002
            raise BadRequest("Bad Request: chat not found")

    ok, err = asyncio.run(
        safe_send_message(
            ChatNotFoundMessageBot(),
            chat_id=123,
            text="hello",
            uid=123,
            send_type="mywin14",
            return_error=True,
            raise_on_non_transient=False,
            fallback_on_bad_request=False,
            logger=logging.getLogger("test"),
        )
    )
    assert ok is False
    assert err == "chat_not_found"
