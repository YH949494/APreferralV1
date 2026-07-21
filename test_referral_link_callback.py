import ast
import asyncio
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes


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


class _InviteLinkMapCollection:
    def __init__(self, existing_doc=None):
        self._existing_doc = existing_doc

    def find_one(self, filt, sort=None):  # noqa: ARG002
        return self._existing_doc


def _load_callback_func():
    """Extract the generate_referral_link_callback function (plus its module-level
    cooldown state) from main.py via AST, mirroring the isolation pattern used by
    test_start_referral_button.py, so we avoid importing main.py's heavy module-level
    side effects."""
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
    fn_node = next(
        node
        for node in module.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "generate_referral_link_callback"
    )

    isolated = ast.Module(body=[*assign_nodes, fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {
        "Update": Update,
        "ContextTypes": ContextTypes,
        "InlineKeyboardButton": InlineKeyboardButton,
        "InlineKeyboardMarkup": InlineKeyboardMarkup,
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
def callback_env():
    fn = _load_callback_func()
    logger = _Logger()
    sent_messages = []

    async def fake_safe_send_message(bot, chat_id, text, **kwargs):  # noqa: ARG001
        sent_messages.append({"chat_id": chat_id, "text": text, **kwargs})
        return True

    state = {"invite_doc": None, "link": "https://t.me/+realinvitehash", "raise_error": None}

    def fake_get_or_create(user_id, username=""):  # noqa: ARG001
        if state["raise_error"] is not None:
            raise state["raise_error"]
        return state["link"]

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class _FakeAsyncioModule:
        to_thread = staticmethod(fake_to_thread)

        @staticmethod
        def get_running_loop():
            raise NotImplementedError

    fn.__globals__.update(
        {
            "logger": logger,
            "safe_send_message": fake_safe_send_message,
            "asyncio": _FakeAsyncioModule(),
            "get_or_create_referral_invite_link_sync": fake_get_or_create,
            "invite_link_map_collection": _InviteLinkMapCollection(),
            "GROUP_ID": -100999,
            "time": __import__("time"),
            "urlencode": __import__("urllib.parse", fromlist=["urlencode"]).urlencode,
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


def test_callback_success_replies_with_real_invite_link(callback_env):
    callback_env._state["link"] = "https://t.me/+abcDEF123"
    _run(callback_env, uid=102)

    assert len(callback_env._sent) == 1
    msg = callback_env._sent[0]
    assert "https://t.me/+abcDEF123" in msg["text"]
    assert "Valid for 24 hours" in msg["text"]
    assert msg["chat_id"] == 102


def test_callback_success_includes_share_button(callback_env):
    callback_env._state["link"] = "https://t.me/+shareLinkHash"
    _run(callback_env, uid=103)

    msg = callback_env._sent[0]
    reply_markup = msg["reply_markup"]
    buttons = [b for row in reply_markup.inline_keyboard for b in row]
    share_btns = [b for b in buttons if b.text == "📤 Share Referral Link"]
    assert len(share_btns) == 1
    assert share_btns[0].url.startswith("https://t.me/share/url?")


def test_callback_share_url_is_correctly_encoded(callback_env):
    callback_env._state["link"] = "https://t.me/+abc?def=1"
    _run(callback_env, uid=104)

    msg = callback_env._sent[0]
    reply_markup = msg["reply_markup"]
    share_btn = next(b for row in reply_markup.inline_keyboard for b in row if b.text == "📤 Share Referral Link")

    parsed = urlparse(share_btn.url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "t.me"
    assert parsed.path == "/share/url"

    params = parse_qs(parsed.query)
    assert params["url"] == ["https://t.me/+abc?def=1"]
    assert params["text"] == ["Join me on AdvantPlay!"]


def test_callback_reuses_existing_active_link(callback_env):
    callback_env.__globals__["invite_link_map_collection"] = _InviteLinkMapCollection(
        existing_doc={"invite_link": "https://t.me/+existingHash"}
    )
    _run(callback_env, uid=105)

    logged_ok = [args for args in callback_env._logger.infos if args and args[0].startswith("[REFERRAL][START_CALLBACK_OK]")]
    assert logged_ok
    assert "reused=True" in logged_ok[-1][0] % logged_ok[-1][1:]


def test_callback_generation_failure_shows_error_and_no_link(callback_env):
    callback_env._state["raise_error"] = RuntimeError("createChatInviteLink failed: boom")
    _run(callback_env, uid=106)

    assert len(callback_env._sent) == 1
    msg = callback_env._sent[0]
    assert "couldn" in msg["text"].lower() or "couldn’t" in msg["text"]
    assert "t.me/+" not in msg["text"]
    assert "https://" not in msg["text"]

    logged_failed = [args for args in callback_env._logger.errors if args and args[0].startswith("[REFERRAL][START_CALLBACK_FAILED]")]
    assert logged_failed


def test_callback_no_bot_deeplink_fallback_ever_shown(callback_env):
    callback_env._state["raise_error"] = RuntimeError(
        "createChatInviteLink failed: bad request\n"
        "Non-functional fallback deeplink (for ops reference only): https://t.me/somebot?start=ref106"
    )
    _run(callback_env, uid=107)

    msg = callback_env._sent[0]
    assert "?start=ref" not in msg["text"]


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
