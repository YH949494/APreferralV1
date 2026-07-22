import ast
import asyncio
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from telegram import Update
from telegram.ext import ContextTypes

import referral_share_content as rsc


class _Logger:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, *args, **kwargs):
        self.infos.append(args)

    def error(self, *args, **kwargs):
        self.errors.append(args)

    def exception(self, *args, **kwargs):
        self.errors.append(args)


def _load_callback_func():
    """Extract generate_referral_link_callback (plus its module-level cooldown
    state) from main.py via AST, mirroring the isolation pattern used
    elsewhere in this repo (see test_start_referral_button.py), so we avoid
    importing main.py's heavy module-level side effects. The callback's
    `from referral_share_content import generate_share_package` statement is
    a real import executed inside the function body at call time, so tests
    control its behaviour by monkeypatching `referral_share_content.
    generate_share_package` directly rather than via fn.__globals__.
    """
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)

    wanted_assign_names = {
        "_referral_link_generation_last_attempt",
        "_REFERRAL_LINK_GENERATION_COOLDOWN_SECONDS",
    }
    assign_nodes = [
        node
        for node in module.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id in wanted_assign_names
    ] + [
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in wanted_assign_names
    ]
    fn_nodes = [
        node
        for node in module.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "generate_referral_link_callback"
    ]

    isolated = ast.Module(body=[*assign_nodes, *fn_nodes], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {
        "Update": Update,
        "ContextTypes": ContextTypes,
        "InlineKeyboardButton": __import__("telegram", fromlist=["InlineKeyboardButton"]).InlineKeyboardButton,
        "InlineKeyboardMarkup": __import__("telegram", fromlist=["InlineKeyboardMarkup"]).InlineKeyboardMarkup,
        "urlencode": __import__("urllib.parse", fromlist=["urlencode"]).urlencode,
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["generate_referral_link_callback"]


class _FakeUser:
    def __init__(self, user_id, username="tester"):
        self.id = user_id
        self.username = username


class _FakeQuery:
    def __init__(self, user_id):
        self.from_user = _FakeUser(user_id)
        self.answered = False

    async def answer(self, *args, **kwargs):  # noqa: ARG002
        self.answered = True


class _FakeUpdate:
    def __init__(self, user_id):
        self.callback_query = _FakeQuery(user_id)


class _FakeBot:
    pass


class _FakeContext:
    def __init__(self):
        self.bot = _FakeBot()


@pytest.fixture
def callback_env(monkeypatch):
    fn = _load_callback_func()
    logger = _Logger()
    sent_messages = []

    async def fake_safe_send_message(bot, chat_id, text, **kwargs):  # noqa: ARG001
        sent_messages.append({"chat_id": chat_id, "text": text, **kwargs})
        return True

    state = {
        "result": {
            "ok": True,
            "message": "Hook line\nhttps://rx.apreplay.com/Abc12345\n\n"
            "More player replays and rewards inside AdvantPlay:\n"
            "👉 https://t.me/+realinvitehash",
            "invite_link": "https://t.me/+realinvitehash",
            "playback_url": "https://rx.apreplay.com/Abc12345",
            "hook_text": "Hook line",
        },
        "raise_error": None,
    }

    def fake_generate_share_package(user_id, username=""):  # noqa: ARG001
        if state["raise_error"] is not None:
            raise state["raise_error"]
        return state["result"]

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class _FakeAsyncioModule:
        to_thread = staticmethod(fake_to_thread)

        @staticmethod
        def get_running_loop():
            raise NotImplementedError

    monkeypatch.setattr(rsc, "generate_share_package", fake_generate_share_package)
    fn.__globals__.update(
        {
            "logger": logger,
            "safe_send_message": fake_safe_send_message,
            "asyncio": _FakeAsyncioModule(),
            "time": __import__("time"),
        }
    )
    fn._logger = logger
    fn._sent = sent_messages
    fn._state = state
    return fn


def _run(fn, uid):
    update = _FakeUpdate(uid)
    context = _FakeContext()
    asyncio.run(fn(update, context))
    return update.callback_query


def test_callback_answers_immediately(callback_env):
    query = _run(callback_env, uid=101)
    assert query.answered is True


def test_callback_success_sends_exact_generated_message(callback_env):
    callback_env._state["result"] = {
        "ok": True,
        "message": (
            "🔥 Big wins today!\n"
            "https://rx.apreplay.com/Play00001\n\n"
            "More player replays and rewards inside AdvantPlay:\n"
            "👉 https://t.me/+abcDEF123"
        ),
        "invite_link": "https://t.me/+abcDEF123",
        "playback_url": "https://rx.apreplay.com/Play00001",
        "hook_text": "🔥 Big wins today!",
    }
    _run(callback_env, uid=102)

    assert len(callback_env._sent) == 1
    msg = callback_env._sent[0]
    assert msg["text"] == (
        "🔥 Big wins today!\n"
        "https://rx.apreplay.com/Play00001\n\n"
        "More player replays and rewards inside AdvantPlay:\n"
        "👉 https://t.me/+abcDEF123"
    )
    assert msg["chat_id"] == 102
    assert msg.get("disable_web_page_preview") is True
    assert "parse_mode" not in msg or msg.get("parse_mode") is None


def test_callback_share_button_prefilled_with_complete_package(callback_env):
    message = (
        "Hook\nhttps://rx.apreplay.com/Xyz00001\n\n"
        "More player replays and rewards inside AdvantPlay:\n"
        "👉 https://t.me/+abcDEF123?x=1&y=2"
    )
    callback_env._state["result"] = {
        "ok": True,
        "message": message,
        "invite_link": "https://t.me/+abcDEF123?x=1&y=2",
        "playback_url": "https://rx.apreplay.com/Xyz00001",
        "hook_text": "Hook",
    }
    _run(callback_env, uid=111)

    msg = callback_env._sent[0]
    reply_markup = msg["reply_markup"]
    share_btn = next(b for row in reply_markup.inline_keyboard for b in row if b.text == "📤 Share Referral Link")

    parsed = urlparse(share_btn.url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "t.me"
    assert parsed.path == "/share/url"

    params = parse_qs(parsed.query)
    assert params["url"] == ["https://t.me/+abcDEF123?x=1&y=2"]
    assert params["text"] == [message]


def test_callback_success_includes_share_button(callback_env):
    _run(callback_env, uid=103)

    msg = callback_env._sent[0]
    reply_markup = msg["reply_markup"]
    buttons = [b for row in reply_markup.inline_keyboard for b in row]
    share_btns = [b for b in buttons if b.text == "📤 Share Referral Link"]
    assert len(share_btns) == 1
    assert share_btns[0].url.startswith("https://t.me/share/url?")


def test_callback_no_active_playback_shows_retryable_message_no_link(callback_env):
    callback_env._state["result"] = {"ok": False, "code": "no_active_playback"}
    _run(callback_env, uid=106)

    assert len(callback_env._sent) == 1
    msg = callback_env._sent[0]
    assert "no playback is currently available" in msg["text"].lower()
    assert "please try again later" in msg["text"].lower()
    assert "https://" not in msg["text"]

    logged_failed = [args for args in callback_env._logger.errors if args and args[0].startswith("[REFERRAL][START_CALLBACK_FAILED]")]
    assert logged_failed


def test_callback_invite_link_failure_shows_generic_retry_message_no_link(callback_env):
    callback_env._state["result"] = {"ok": False, "code": "invite_link_failed"}
    _run(callback_env, uid=112)

    msg = callback_env._sent[0]
    assert "couldn" in msg["text"].lower() or "couldn’t" in msg["text"]
    assert "https://" not in msg["text"]


def test_callback_unexpected_exception_shows_generic_retry_message(callback_env):
    callback_env._state["raise_error"] = RuntimeError("boom")
    _run(callback_env, uid=107)

    assert len(callback_env._sent) == 1
    msg = callback_env._sent[0]
    assert "couldn" in msg["text"].lower() or "couldn’t" in msg["text"]
    assert "https://" not in msg["text"]

    logged_failed = [args for args in callback_env._logger.errors if args and args[0].startswith("[REFERRAL][START_CALLBACK_FAILED]")]
    assert logged_failed


def test_callback_rate_limits_rapid_repeated_taps(callback_env):
    _run(callback_env, uid=108)
    assert len(callback_env._sent) == 1

    # Second tap immediately after should be rate limited, not generate again.
    _run(callback_env, uid=108)
    assert len(callback_env._sent) == 2
    second_msg = callback_env._sent[1]
    assert "please try again in a moment" in second_msg["text"].lower()

    logged_rl = [args for args in callback_env._logger.infos if args and args[0] == "[REFERRAL][START_CALLBACK_RATE_LIMITED] uid=%s"]
    assert logged_rl
    assert logged_rl[-1][1] == 108


def test_callback_logs_start_with_uid_only(callback_env):
    _run(callback_env, uid=109)
    logged_start = [args for args in callback_env._logger.infos if args and args[0] == "[REFERRAL][START_CALLBACK] uid=%s"]
    assert logged_start
    assert logged_start[-1][1] == 109
