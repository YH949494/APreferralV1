import ast
import asyncio
from pathlib import Path

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes


class _Logger:
    def __init__(self):
        self.infos = []

    def info(self, *args, **kwargs):
        self.infos.append(args)

    def exception(self, *args, **kwargs):
        pass


class _UsersCollection:
    def find_one(self, filt, proj=None):  # noqa: ARG002
        return {}


def _load_start_func():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "start"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {
        "Update": Update,
        "ContextTypes": ContextTypes,
        "InlineKeyboardButton": InlineKeyboardButton,
        "InlineKeyboardMarkup": InlineKeyboardMarkup,
        "WebAppInfo": WebAppInfo,
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["start"]


class _FakeUser:
    def __init__(self, user_id, username="tester"):
        self.id = user_id
        self.username = username


class _FakeChat:
    type = "private"
    id = 555


class _FakeMessage:
    text = "/start"


class _FakeUpdate:
    def __init__(self, user_id):
        self.effective_user = _FakeUser(user_id)
        self.effective_message = _FakeMessage()
        self.effective_chat = _FakeChat()
        self.message = _FakeMessage()


@pytest.fixture
def start_fn():
    fn = _load_start_func()
    logger = _Logger()
    captured = {}

    async def fake_safe_reply_text(message, text, reply_markup=None, **kwargs):  # noqa: ARG001
        captured["reply_markup"] = reply_markup
        captured["text"] = text
        return True

    async def fake_send_welcome_unclaimed_reminder_if_needed(*args, **kwargs):  # noqa: ARG001
        return None

    fn.__globals__.update(
        {
            "logger": logger,
            "_is_private_chat": lambda update: True,
            "users_collection": _UsersCollection(),
            "_mark_private_interaction": lambda *a, **k: None,
            "_users_update_one": lambda *a, **k: None,
            "safe_reply_text": fake_safe_reply_text,
            "now_utc": lambda: None,
            "WEBAPP_URL": "https://apreferralv1.fly.dev/miniapp?v=test",
            "REFERRAL_WEBAPP_URL": "https://apreferralv1.fly.dev/miniapp?v=test&action=generate_referral",
            "_send_welcome_unclaimed_reminder_if_needed": fake_send_welcome_unclaimed_reminder_if_needed,
        }
    )
    fn._logger = logger
    fn._captured = captured
    return fn


def _button_rows(reply_markup):
    return [[b for b in row] for row in reply_markup.inline_keyboard]


def test_start_contains_referral_button_and_keeps_miniapp_button(start_fn):
    update = _FakeUpdate(user_id=42)
    asyncio.run(start_fn(update, context=None))

    rows = _button_rows(start_fn._captured["reply_markup"])
    flat = [btn for row in rows for btn in row]

    miniapp_btns = [b for b in flat if "Mini-App" in b.text]
    referral_btns = [b for b in flat if b.text == "🔗 Generate My Referral Link"]

    assert len(miniapp_btns) == 1
    assert miniapp_btns[0].web_app.url == "https://apreferralv1.fly.dev/miniapp?v=test"

    assert len(referral_btns) == 1
    assert referral_btns[0].callback_data == "generate_referral_link"
    assert referral_btns[0].web_app is None


def test_start_logs_referral_button_shown(start_fn):
    update = _FakeUpdate(user_id=99)
    asyncio.run(start_fn(update, context=None))

    logged = [args for args in start_fn._logger.infos if args and args[0] == "[START][REFERRAL_BUTTON_SHOWN] uid=%s"]
    assert logged
    assert logged[-1][1] == 99
