import ast
import asyncio
import html
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ParseMode
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


@pytest.fixture
def deeplink_env(monkeypatch):
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

    state = {
        "result": {
            "ok": True,
            "message": "irrelevant -- the deep-link route renders through build_miniapp_referral_text(), not this field",
            "invite_link": "https://t.me/+uniqueReferralHash",
            "playback_url": None,
            "hook_text": None,
        },
        "raise_error": None,
    }
    generate_calls = []

    def fake_generate_share_package(user_id, username="", **kwargs):
        calls["generate"] += 1
        generate_calls.append({"user_id": user_id, "username": username, **kwargs})
        if state["raise_error"] is not None:
            raise state["raise_error"]
        return state["result"]

    monkeypatch.setattr(rsc, "generate_share_package", fake_generate_share_package)

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
            "GROUP_ID": -100999,
        }
    )
    fn._logger = logger
    fn._calls = calls
    fn._replies = replies
    fn._state = state
    fn._env = env
    fn._generate_calls = generate_calls
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


def test_new_user_referral_deeplink_calls_share_package_generator(deeplink_env):
    update = _FakeUpdate(user_id=202)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert deeplink_env._calls["generate"] == 1


def test_new_user_referral_deeplink_returns_share_package_and_share_button(deeplink_env):
    # Hook/playback populated on the (mocked) share package -- as they would
    # be with live content pools -- must never leak into this Mini App
    # surface. See test_referral_deeplink_never_renders_hook_or_playback for
    # the direct regression check.
    deeplink_env._state["result"] = {
        "ok": True,
        "message": "irrelevant -- rendered via build_miniapp_referral_text(), not this field",
        "invite_link": "https://t.me/+brandNewHash",
        "playback_url": "https://cdn.example.com/playback/xyz",
        "hook_text": "🎬 New hook!",
    }
    update = _FakeUpdate(user_id=203)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert len(deeplink_env._replies) == 1
    reply = deeplink_env._replies[0]
    assert "https://t.me/+brandNewHash" in reply["text"]
    assert "🎬 New hook!" not in reply["text"]
    assert "https://cdn.example.com/playback/xyz" not in reply["text"]

    # Only the benefits section is wrapped in a blockquote.
    assert reply["text"].count("<blockquote>") == 1
    assert reply["text"].count("</blockquote>") == 1
    quote_start = reply["text"].index("<blockquote>")
    quote_end = reply["text"].index("</blockquote>") + len("</blockquote>")
    quoted = reply["text"][quote_start:quote_end]
    assert "Join our channel to get" in quoted
    assert "🎟️ FREE Welcome Voucher — No deposit required" in quoted
    assert "⚡️ Daily voucher drops" in quoted
    assert quoted.index("FREE Welcome Voucher") < quoted.index("Daily voucher drops")
    assert "https://t.me/+brandNewHash" in reply["text"][quote_end:]
    # Nothing precedes the blockquote -- no hook/playback/CTA line above it.
    assert reply["text"][:quote_start] == ""

    assert reply.get("parse_mode") == ParseMode.HTML
    assert reply.get("disable_web_page_preview") is not True

    # Old hard-coded caption must never appear.
    assert "Daily XP rewards" not in reply["text"]
    assert "Active players win more" not in reply["text"]
    assert "Want more replays like this—and rewards too?" not in reply["text"]

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
    deeplink_env._state["result"] = {
        "ok": True,
        "message": "hook\nurl\n\nMore player replays and rewards inside AdvantPlay:\n👉 https://t.me/+existingUserHash",
        "invite_link": "https://t.me/+existingUserHash",
        "playback_url": "url",
        "hook_text": "hook",
    }
    update = _FakeUpdate(user_id=205)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert deeplink_env._calls["generate"] == 1
    reply = deeplink_env._replies[0]
    assert "https://t.me/+existingUserHash" in reply["text"]

    buttons = _flat_buttons(reply["reply_markup"])
    share_btn = next(b for b in buttons if b.text == "📤 Share Referral Link")
    params = parse_qs(urlparse(share_btn.url).query)
    assert params["url"] == ["https://t.me/+existingUserHash"]


def test_referral_deeplink_share_button_never_encodes_hook_or_playback(deeplink_env):
    deeplink_env._state["result"] = {
        "ok": True,
        "message": "irrelevant -- rendered via build_miniapp_referral_text(), not this field",
        "invite_link": "https://t.me/+shareCaptionHash",
        "playback_url": "https://cdn.example.com/pb",
        "hook_text": "hook-text",
    }
    update = _FakeUpdate(user_id=215)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    buttons = _flat_buttons(reply["reply_markup"])
    share_btn = next(b for b in buttons if b.text == "📤 Share Referral Link")
    params = parse_qs(urlparse(share_btn.url).query)

    assert "hook-text" not in params["text"][0]
    assert "https://cdn.example.com/pb" not in params["text"][0]
    assert params["url"] == ["https://t.me/+shareCaptionHash"]
    assert "👋 Welcome to AdvantPlay Community!" in params["text"][0]

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
# Repeated deep-link clicks: no duplicate mapping records / business events.
# The canonical generator itself owns dedup (invite_link_map upserts); the
# deep-link route must call it exactly once per click without side effects
# of its own that could double-create a mapping.
# ---------------------------------------------------------------------------

def test_repeated_referral_deeplink_clicks_call_generator_once_per_click(deeplink_env):
    update1 = _FakeUpdate(user_id=209)
    asyncio.run(deeplink_env(update1, _FakeContext()))
    update2 = _FakeUpdate(user_id=209)
    asyncio.run(deeplink_env(update2, _FakeContext()))

    assert deeplink_env._calls["generate"] == 2  # one canonical-generator call per click
    assert len(deeplink_env._replies) == 2
    # Both replies carry the same canonical package (generator fake always
    # returns the fixed state["result"]) -- the route does not mint its own.
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
    assert "Join our channel" not in reply["text"]


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


def test_referral_deeplink_no_active_playback_returns_retryable_error_not_old_caption(deeplink_env):
    # generate_share_package no longer returns this code (no active playback
    # is now an omitted section, not a hard failure -- see
    # referral_share_content.generate_share_package). This branch in
    # send_referral_link_with_share_button is kept as defensive handling for
    # an unexpected/legacy error code; this test locks in that if it were
    # ever hit, it still degrades safely rather than sending a malformed
    # caption.
    deeplink_env._state["result"] = {"ok": False, "code": "no_active_playback"}
    update = _FakeUpdate(user_id=216)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    assert reply["text"] == "No playback is currently available. Please try again later."
    assert "Join our channel" not in reply["text"]
    assert reply["reply_markup"] is None


def test_referral_deeplink_no_hook_no_playback_sends_static_fallback_caption(deeplink_env):
    """Entry point #2 (/start?start=referral): with no active hook and no
    active playback link, generate_share_package now returns ok=True with
    both omitted -- the deep-link route must still send a valid, non-empty
    caption (static benefits + referral link) and a working Share button,
    never an error message."""
    deeplink_env._state["result"] = {
        "ok": True,
        "message": (
            "👋 Welcome to AdvantPlay Community!\n\n"
            "Join our channel to get 👇\n\n"
            "🎟️ FREE Welcome Voucher — No deposit required\n"
            "⚡️ Daily voucher drops\n"
            "🎁 Bonus campaigns\n"
            "👑 VIP-only announcements\n"
            "🏆 Weekly ranking rewards\n\n"
            "Start here 👇\n"
            "https://t.me/+fallbackOnlyHash"
        ),
        "invite_link": "https://t.me/+fallbackOnlyHash",
        "playback_url": None,
        "hook_text": None,
    }
    update = _FakeUpdate(user_id=217)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    assert "https://t.me/+fallbackOnlyHash" in reply["text"]
    assert "Join our channel to get" in reply["text"]
    assert "None" not in reply["text"]
    assert "\n\n\n" not in reply["text"]
    assert reply.get("parse_mode") == ParseMode.HTML

    buttons = _flat_buttons(reply["reply_markup"])
    share_btns = [b for b in buttons if b.text == "📤 Share Referral Link"]
    assert len(share_btns) == 1
    params = parse_qs(urlparse(share_btns[0].url).query)
    assert params["url"] == ["https://t.me/+fallbackOnlyHash"]
    assert "None" not in params["text"][0]


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


# ---------------------------------------------------------------------------
# Regression: production bug where /start referral rendered Creator-style
# content (hook + playback URL + "Want more replays like this—and rewards
# too?") glued onto the Mini App's five-benefit block. Reproduces the bug by
# having the (mocked) share package return active hook/playback content --
# exactly like it would with live hook/playback pools populated -- and
# asserts none of it reaches the sent message or the Telegram Share button.
# ---------------------------------------------------------------------------

def test_referral_deeplink_never_renders_hook_or_playback(deeplink_env):
    deeplink_env._state["result"] = {
        "ok": True,
        "message": "Don't skip the ending 👇\nhttps://rx.apreplay.com/Abc12345\n\n"
        "Want more replays like this—and rewards too?\n[full Mini App benefit caption]\n"
        "https://t.me/+regressionHash",
        "invite_link": "https://t.me/+regressionHash",
        "playback_url": "https://rx.apreplay.com/Abc12345",
        "hook_text": "Don't skip the ending 👇",
    }
    update = _FakeUpdate(user_id=220)
    asyncio.run(deeplink_env(update, _FakeContext()))

    reply = deeplink_env._replies[0]
    buttons = _flat_buttons(reply["reply_markup"])
    share_btn = next(b for b in buttons if b.text == "📤 Share Referral Link")
    share_text = parse_qs(urlparse(share_btn.url).query)["text"][0]

    for surface_text in (reply["text"], share_text):
        assert "Don't skip the ending" not in surface_text
        assert "rx.apreplay.com" not in surface_text
        assert "Want more replays like this—and rewards too?" not in surface_text

    assert "👋 Welcome to AdvantPlay Community!" in reply["text"]
    assert "🎟️ FREE Welcome Voucher — No deposit required" in reply["text"]
    assert "⚡️ Daily voucher drops" in reply["text"]
    assert "🎁 Bonus campaigns" in reply["text"]
    assert "👑 VIP-only announcements" in reply["text"]
    assert "🏆 Weekly ranking rewards" in reply["text"]
    assert reply["text"].count("https://t.me/+regressionHash") == 1


def test_referral_deeplink_requests_share_package_without_creator_content_pools(deeplink_env):
    update = _FakeUpdate(user_id=221)
    asyncio.run(deeplink_env(update, _FakeContext()))

    assert len(deeplink_env._generate_calls) == 1
    call = deeplink_env._generate_calls[0]
    assert call["include_content_pools"] is False
    assert call["generated_by"] == rsc.MINIAPP_SHARE_SOURCE
