import ast
import asyncio
from pathlib import Path

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes


class _Logger:
    def __init__(self):
        self.infos = []

    def info(self, *args, **kwargs):  # noqa: ARG002
        self.infos.append(args)


def _load_env():
    """Isolate send_creator_share_entry_point() via AST, mirroring the
    isolation pattern used by the other main.py unit tests in this repo.
    """
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {"send_creator_share_entry_point"}
    fn_nodes = [
        node
        for node in module.body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name in wanted
    ]
    isolated = ast.Module(body=fn_nodes, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {
        "Update": Update,
        "ContextTypes": ContextTypes,
        "InlineKeyboardButton": InlineKeyboardButton,
        "InlineKeyboardMarkup": InlineKeyboardMarkup,
        "WebAppInfo": WebAppInfo,
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env


class _FakeUser:
    def __init__(self, user_id, username="tester"):
        self.id = user_id
        self.username = username


class _FakeMessage:
    text = "/creator"


class _FakeUpdate:
    def __init__(self, user_id, username="tester"):
        self.effective_user = _FakeUser(user_id, username)
        self.effective_message = _FakeMessage()


class _FakeContext:
    args = []


@pytest.fixture
def creator_env(monkeypatch):
    env = _load_env()
    fn = env["send_creator_share_entry_point"]
    logger = _Logger()
    replies = []

    async def fake_safe_reply_text(message, text, reply_markup=None, **kwargs):  # noqa: ARG001
        replies.append({"text": text, "reply_markup": reply_markup, **kwargs})
        return True

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class _FakeAsyncioModule:
        to_thread = staticmethod(fake_to_thread)

    state = {"access_err": None}

    def fake_verify_creator_access(uid, username):  # noqa: ARG001
        return None, state["access_err"]

    fake_creator_share_centre = type(
        "module", (), {"_verify_creator_access": staticmethod(fake_verify_creator_access)}
    )
    monkeypatch.setitem(__import__("sys").modules, "creator_share_centre", fake_creator_share_centre)

    fn.__globals__.update(
        {
            "logger": logger,
            "safe_reply_text": fake_safe_reply_text,
            "asyncio": _FakeAsyncioModule(),
            "CREATOR_SHARE_WEBAPP_URL": "https://apreferralv1.fly.dev/creator-share",
        }
    )
    fn._replies = replies
    fn._state = state
    return fn


def _flat_buttons(reply_markup):
    if reply_markup is None:
        return []
    return [b for row in reply_markup.inline_keyboard for b in row]


def test_creator_share_entry_point_sends_updated_copy_and_button(creator_env):
    update = _FakeUpdate(user_id=301)
    asyncio.run(creator_env(update, _FakeContext()))

    assert len(creator_env._replies) == 1
    reply = creator_env._replies[0]
    assert reply["text"] == "Your next referral could be worth cash. Start sharing 👇"

    buttons = _flat_buttons(reply["reply_markup"])
    assert len(buttons) == 1
    assert buttons[0].text == "💰 Turn Shares Into Cash"
    assert buttons[0].web_app.url == "https://apreferralv1.fly.dev/creator-share"


def test_creator_share_entry_point_old_copy_is_gone(creator_env):
    update = _FakeUpdate(user_id=302)
    asyncio.run(creator_env(update, _FakeContext()))

    reply = creator_env._replies[0]
    assert "Open your Creator Share Centre" not in reply["text"]
    buttons = _flat_buttons(reply["reply_markup"])
    assert all(b.text != "🎬 Creator Share Centre" for b in buttons)


def test_creator_share_entry_point_access_denied_message_unchanged(creator_env):
    creator_env._state["access_err"] = ("not_member", "denied")
    update = _FakeUpdate(user_id=303)
    asyncio.run(creator_env(update, _FakeContext()))

    assert len(creator_env._replies) == 1
    reply = creator_env._replies[0]
    assert reply["text"] == "This entry point is for approved AdvantPlay creators only."
    assert reply["reply_markup"] is None
