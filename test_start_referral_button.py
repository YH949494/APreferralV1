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
    """Extract start() plus the helper functions it now delegates to
    (_ensure_user_registered, ensure_user_initialized_for_referral,
    send_referral_link_with_share_button, _generate_referral_link_for_user)
    from main.py via AST, so the routing added at the top of start() has all
    the names it calls available in the isolated exec environment.
    """
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {
        "start",
        "_ensure_user_registered",
        "ensure_user_initialized_for_referral",
        "send_referral_link_with_share_button",
        "_generate_referral_link_for_user",
    }
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


class _FakeContext:
    def __init__(self, args=None):
        self.args = args or []


@pytest.fixture
def start_env():
    env = _load_start_func()
    fn = env["start"]
    logger = _Logger()
    captured = {}
    referral_calls = {"generate": 0}

    async def fake_safe_reply_text(message, text, reply_markup=None, **kwargs):  # noqa: ARG001
        captured.setdefault("replies", []).append({"text": text, "reply_markup": reply_markup, **kwargs})
        captured["reply_markup"] = reply_markup
        captured["text"] = text
        return True

    async def fake_send_welcome_unclaimed_reminder_if_needed(*args, **kwargs):  # noqa: ARG001
        return None

    def fake_get_or_create(user_id, username=""):  # noqa: ARG001
        referral_calls["generate"] += 1
        return "https://t.me/+shouldNotBeCalled"

    class _InviteLinkMapCollection:
        def find_one(self, filt, sort=None):  # noqa: ARG002
            return None

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class _FakeAsyncioModule:
        to_thread = staticmethod(fake_to_thread)

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
            "OFFICIAL_CHANNEL_URL": "https://t.me/+Zy3UGGkE17kyNDA9",
            "_send_welcome_unclaimed_reminder_if_needed": fake_send_welcome_unclaimed_reminder_if_needed,
            "asyncio": _FakeAsyncioModule(),
            "get_or_create_referral_invite_link_sync": fake_get_or_create,
            "invite_link_map_collection": _InviteLinkMapCollection(),
            "GROUP_ID": -100999,
        }
    )
    fn._logger = logger
    fn._captured = captured
    fn._referral_calls = referral_calls
    return fn


def _button_rows(reply_markup):
    return [[b for b in row] for row in reply_markup.inline_keyboard]


def test_plain_start_shows_welcome_message(start_env):
    update = _FakeUpdate(user_id=42)
    asyncio.run(start_env(update, _FakeContext()))

    assert start_env._captured.get("text")


def test_plain_start_has_join_official_channel_button(start_env):
    update = _FakeUpdate(user_id=42)
    asyncio.run(start_env(update, _FakeContext()))

    rows = _button_rows(start_env._captured["reply_markup"])
    flat = [btn for row in rows for btn in row]
    join_btns = [b for b in flat if b.text == "📢 Join Official Channel"]
    assert len(join_btns) == 1
    assert join_btns[0].url == "https://t.me/+Zy3UGGkE17kyNDA9"


def test_plain_start_has_open_miniapp_button(start_env):
    update = _FakeUpdate(user_id=42)
    asyncio.run(start_env(update, _FakeContext()))

    rows = _button_rows(start_env._captured["reply_markup"])
    flat = [btn for row in rows for btn in row]
    miniapp_btns = [b for b in flat if b.text == "🚀 Open AdvantPlay Mini-App"]
    assert len(miniapp_btns) == 1
    assert miniapp_btns[0].web_app.url == "https://apreferralv1.fly.dev/miniapp?v=test"

    claim_btns = [b for b in flat if b.text == "🎁 Claim Welcome Reward"]
    assert not claim_btns


def test_plain_start_has_exactly_two_buttons_no_referral_button(start_env):
    update = _FakeUpdate(user_id=42)
    asyncio.run(start_env(update, _FakeContext()))

    rows = _button_rows(start_env._captured["reply_markup"])
    flat = [btn for row in rows for btn in row]
    assert len(flat) == 2

    referral_btns = [b for b in flat if "Referral" in b.text or "referral" in (b.callback_data or "")]
    assert not referral_btns


def test_plain_start_does_not_call_referral_generator(start_env):
    update = _FakeUpdate(user_id=42)
    asyncio.run(start_env(update, _FakeContext()))

    assert start_env._referral_calls["generate"] == 0


def test_plain_start_with_empty_args_list_behaves_like_no_payload(start_env):
    update = _FakeUpdate(user_id=43)
    asyncio.run(start_env(update, _FakeContext(args=[])))

    rows = _button_rows(start_env._captured["reply_markup"])
    flat = [btn for row in rows for btn in row]
    assert len(flat) == 2
    assert start_env._referral_calls["generate"] == 0
