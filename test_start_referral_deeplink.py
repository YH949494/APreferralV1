import ast
import asyncio
import html
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ParseMode
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


def _load_env():
    """Extract start() and its referral-deep-link helpers together via AST so
    they share one globals dict and can call each other, mirroring the
    isolation pattern used by the other main.py unit tests in this repo.
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
        "ParseMode": ParseMode,
        "html_escape": html.escape,
        "quote": quote,
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
    text = "/start referral"


class _FakeUpdate:
    def __init__(self, user_id, username="tester"):
        self.effective_user = _FakeUser(user_id, username)
        self.effective_message = _FakeMessage()
        self.effective_chat = _FakeChat()
        self.message = _FakeMessage()


class _FakeContext:
    def __init__(self, args=None):
        self.args = args if args is not None else ["referral"]


class _UsersCollection:
    def __init__(self, existing_user_ids=None):
        self._existing = set(existing_user_ids or [])

    def find_one(self, filt, proj=None):  # noqa: ARG002
        uid = filt.get("user_id")
        if uid in self._existing:
            return {"user_id": uid}
        return {}


class _InviteLinkMapCollection:
    def __init__(self, existing_doc=None):
        self._existing_doc = existing_doc

    def find_one(self, filt, sort=None):  # noqa: ARG002
        return self._existing_doc


@pytest.fixture
def deeplink_env():
    env = _load_env()
    fn = env["start"]
    logger = _Logger()
    calls = {"upsert": 0, "mark_interaction": 0, "generate": 0}
    replies = []

    async def fake_safe_reply_text(message, text, reply_markup=None, **kwargs):  # noqa: ARG001
        replies.append({"text": text, "reply_markup": reply_markup, **kwargs})
        return True

    async def fake_send_welcome_unclaimed_reminder_if_needed(*args, **kwargs):  # noqa: ARG001
        return None

    def fake_mark_private_interaction(*a, **k):  # noqa: ARG001
        calls["mark_interaction"] += 1

    def fake_users_update_one(*a, **k):  # noqa: ARG001
        calls["upsert"] += 1

    state = {"link": "https://t.me/+uniqueReferralHash", "raise_error": None}

    def fake_get_or_create(user_id, username=""):  # noqa: ARG001
        calls["generate"] += 1
        if state["raise_error"] is not None:
            raise state["raise_error"]
        return state["link"]

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class _FakeAsyncioModule:
        to_thread = staticmethod(fake_to_thread)

    fn.__globals__.update(
        {
            "logger": logger,
            "_is_private_chat": lambda update: True,
            "users_collection": _UsersCollection(),
            "_mark_private_interaction": fake_mark_private_interaction,
            "_users_update_one": fake_users_update_one,
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
    fn._calls = calls
    fn._replies = replies
    fn._state = state
    fn._env = env
    return fn


def _flat_buttons(reply_markup):
    if reply_markup is None:
        return []
    return [b for row in reply_markup.inline_keyboard for b in row]


# ---------------------------------------------------------------------------
# New user opens /start referral
# ---------------------------------------------------------------------------

def test_new_user_referral_deeplink_initializes_user(deeplink_env):
    update = _FakeUpdate(user_id=201)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert deeplink_env._calls["upsert"] == 1
    assert deeplink_env._calls["mark_interaction"] == 1


def test_new_user_referral_deeplink_calls_canonical_generator(deeplink_env):
    update = _FakeUpdate(user_id=202)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert deeplink_env._calls["generate"] == 1


_CAPTION_LINES = [
    "Join AdvantPlay 👇",
    "✔️ Daily XP rewards",
    "✔️ Surprise voucher drops",
    "✔️ Weekly bonus for Top 10",
    "Active players win more.",
]


def test_new_user_referral_deeplink_returns_unique_link_and_share_button(deeplink_env):
    deeplink_env._state["link"] = "https://t.me/+brandNewHash"
    update = _FakeUpdate(user_id=203)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert len(deeplink_env._replies) == 1
    reply = deeplink_env._replies[0]
    assert "https://t.me/+brandNewHash" in reply["text"]
    for line in _CAPTION_LINES:
        assert line in reply["text"]
    assert reply["text"].startswith("<blockquote>")
    assert reply["text"].endswith("</blockquote>")
    assert "👉 https://t.me/+brandNewHash" in reply["text"]

    buttons = _flat_buttons(reply["reply_markup"])
    share_btns = [b for b in buttons if b.text == "📤 Share Referral Link"]
    assert len(share_btns) == 1
    assert share_btns[0].url.startswith("https://t.me/share/url?")


def test_new_user_referral_deeplink_does_not_send_normal_welcome_keyboard(deeplink_env):
    update = _FakeUpdate(user_id=204)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert len(deeplink_env._replies) == 1
    buttons = _flat_buttons(deeplink_env._replies[0]["reply_markup"])
    button_texts = {b.text for b in buttons}
    assert "📢 Join Official Channel" not in button_texts
    assert "🚀 Open AdvantPlay Mini-App" not in button_texts
    assert len(buttons) == 1  # only the Share button


# ---------------------------------------------------------------------------
# Existing user opens /start referral
# ---------------------------------------------------------------------------

def test_existing_user_referral_deeplink_returns_link_and_share_button(deeplink_env):
    deeplink_env.__globals__["users_collection"] = _UsersCollection(existing_user_ids={205})
    deeplink_env._state["link"] = "https://t.me/+existingUserHash"
    update = _FakeUpdate(user_id=205)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert deeplink_env._calls["generate"] == 1
    reply = deeplink_env._replies[0]
    assert "https://t.me/+existingUserHash" in reply["text"]

    buttons = _flat_buttons(reply["reply_markup"])
    share_btn = next(b for b in buttons if b.text == "📤 Share Referral Link")
    params = parse_qs(urlparse(share_btn.url).query)
    assert params["url"] == ["https://t.me/+existingUserHash"]


def test_referral_deeplink_share_button_encodes_full_caption(deeplink_env):
    deeplink_env._state["link"] = "https://t.me/+shareCaptionHash"
    update = _FakeUpdate(user_id=215)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    buttons = _flat_buttons(reply["reply_markup"])
    share_btn = next(b for b in buttons if b.text == "📤 Share Referral Link")
    params = parse_qs(urlparse(share_btn.url).query)

    for line in _CAPTION_LINES:
        assert line in params["text"][0]
    assert params["url"] == ["https://t.me/+shareCaptionHash"]

    # The link must not appear inside the "text" param -- Telegram's
    # share/url endpoint already appends the "url" param on its own, so
    # duplicating it in "text" would show the link twice in the share sheet.
    assert "https://t.me/+shareCaptionHash" not in params["text"][0]


def test_existing_user_referral_deeplink_no_normal_start_keyboard(deeplink_env):
    deeplink_env.__globals__["users_collection"] = _UsersCollection(existing_user_ids={206})
    update = _FakeUpdate(user_id=206)
    asyncio.run(deeplink_env(update, _FakeContext()))

    buttons = _flat_buttons(deeplink_env._replies[0]["reply_markup"])
    assert all(b.text == "📤 Share Referral Link" for b in buttons)


# ---------------------------------------------------------------------------
# Existing valid link is reused / expired link regenerated
# (business logic lives in get_or_create_referral_invite_link_sync; here we
# only assert the deep-link route delegates to it rather than duplicating
# reuse/expiry logic itself.)
# ---------------------------------------------------------------------------

def test_referral_deeplink_reuses_existing_active_link_via_canonical_generator(deeplink_env):
    deeplink_env.__globals__["invite_link_map_collection"] = _InviteLinkMapCollection(
        existing_doc={"invite_link": "https://t.me/+reusedHash"}
    )
    deeplink_env._state["link"] = "https://t.me/+reusedHash"
    update = _FakeUpdate(user_id=207)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert deeplink_env._calls["generate"] == 1
    logged_ok = [a for a in deeplink_env._logger.infos if a and a[0].startswith("[REFERRAL][DEEPLINK_OK]")]
    assert logged_ok
    assert "reused=True" in (logged_ok[-1][0] % logged_ok[-1][1:])


def test_referral_deeplink_creates_new_link_when_missing(deeplink_env):
    deeplink_env.__globals__["invite_link_map_collection"] = _InviteLinkMapCollection(existing_doc=None)
    deeplink_env._state["link"] = "https://t.me/+freshlyCreatedHash"
    update = _FakeUpdate(user_id=208)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    assert "https://t.me/+freshlyCreatedHash" in reply["text"]
    logged_ok = [a for a in deeplink_env._logger.infos if a and a[0].startswith("[REFERRAL][DEEPLINK_OK]")]
    assert "reused=False" in (logged_ok[-1][0] % logged_ok[-1][1:])


# ---------------------------------------------------------------------------
# Repeated deep-link clicks: no duplicate mapping records / business events.
# The canonical generator itself owns dedup (invite_link_map upserts); the
# deep-link route must call it exactly once per click without side effects
# of its own that could double-create a mapping.
# ---------------------------------------------------------------------------

def test_repeated_referral_deeplink_clicks_call_generator_once_per_click_no_extra_writes(deeplink_env):
    update1 = _FakeUpdate(user_id=209)
    asyncio.run(deeplink_env(update1, _FakeContext()))
    update2 = _FakeUpdate(user_id=209)
    asyncio.run(deeplink_env(update2, _FakeContext()))

    assert deeplink_env._calls["generate"] == 2  # one canonical-generator call per click
    assert len(deeplink_env._replies) == 2
    # Both replies carry the same canonical link (generator fake always
    # returns the fixed state["link"]) -- the route does not mint its own.
    assert deeplink_env._replies[0]["text"] == deeplink_env._replies[1]["text"]


# ---------------------------------------------------------------------------
# Referral generation failure
# ---------------------------------------------------------------------------

def test_referral_deeplink_generation_failure_sends_retryable_error(deeplink_env):
    deeplink_env._state["raise_error"] = RuntimeError("createChatInviteLink failed")
    update = _FakeUpdate(user_id=210)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert len(deeplink_env._replies) == 1
    reply = deeplink_env._replies[0]
    assert "try again" in reply["text"].lower()
    assert "t.me/+" not in reply["text"]


def test_referral_deeplink_generation_failure_sends_no_normal_keyboard(deeplink_env):
    deeplink_env._state["raise_error"] = RuntimeError("boom")
    update = _FakeUpdate(user_id=211)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    assert reply["reply_markup"] is None


def test_referral_deeplink_generation_failure_logs_exception(deeplink_env):
    deeplink_env._state["raise_error"] = RuntimeError("boom")
    update = _FakeUpdate(user_id=212)
    asyncio.run(deeplink_env(update, _FakeContext()))

    logged_failed = [a for a in deeplink_env._logger.errors if a and a[0] == "[REFERRAL][DEEPLINK_FAILED] uid=%s"]
    assert logged_failed
    assert logged_failed[-1][1] == 212


# ---------------------------------------------------------------------------
# Reserved "referral" payload must not be treated as an inviter id/code.
# main.py's /start currently has no inviter-payload attribution parsing at
# all (attribution is done via Telegram's native invite-link join tracking,
# not via /start context.args) -- this test locks in that a payload of
# "referral" is consumed exclusively by the deep-link route and never reaches
# any user-registration path as if it were arbitrary/inviter data.
# ---------------------------------------------------------------------------

def test_referral_payload_is_not_forwarded_as_arbitrary_start_argument(deeplink_env):
    update = _FakeUpdate(user_id=213)
    asyncio.run(deeplink_env(update, _FakeContext(args=["referral"])))

    # Only the referral route's own registration + generator ran; nothing
    # else consumed "referral" as inviter/attribution data.
    assert deeplink_env._calls["upsert"] == 1
    assert deeplink_env._calls["generate"] == 1


def test_non_referral_payload_falls_through_to_normal_start(deeplink_env):
    update = _FakeUpdate(user_id=214)
    asyncio.run(deeplink_env(update, _FakeContext(args=["someInviterPayload123"])))

    # Falls through to the normal welcome flow; referral generator is not
    # invoked just because *some* payload was present.
    assert deeplink_env._calls["generate"] == 0
    assert len(deeplink_env._replies) == 1
    buttons = _flat_buttons(deeplink_env._replies[0]["reply_markup"])
    button_texts = {b.text for b in buttons}
    assert "📢 Join Official Channel" in button_texts
    assert "🚀 Open AdvantPlay Mini-App" in button_texts
