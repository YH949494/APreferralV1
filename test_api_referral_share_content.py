"""Tests for POST /api/referral/share-content (Mini App "Generate Link" /
copy flow, entry point #4). Isolates api_referral_share_content() from
main.py via AST extraction (same pattern as test_api_referral_contract.py /
test_share_rank_caption.py) and drives it against the real
referral_share_content.generate_share_package with a fake DB, monkeypatching
only the canonical invite-link creator -- referral attribution/XP/etc. are
untouched by this endpoint and out of scope here.

The Mini App is a plain referral tool: regardless of what's active in the
caption-hook / playback-link pools (Creator Share Centre content), its
output is always the fixed five-benefit caption + the user's canonical
referral link -- never a hook, never a playback URL. See
referral_share_content.generate_share_package(include_content_pools=False).
"""
import ast
import json as json_module
from pathlib import Path

import pytest

import database
import referral_share_content as rsc
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _hook(fake_db, text="Fresh replays!", status="active"):
    now = rsc.now_utc()
    doc = {
        "text": text, "status": status, "times_selected": 0, "last_selected_at": None,
        "created_at": now, "updated_at": now, "created_by": None,
    }
    return fake_db["caption_hooks"].insert_one(doc).inserted_id


def _playback(fake_db, playback_id="Abc123", status="active"):
    now = rsc.now_utc()
    doc = {
        "playback_id": playback_id,
        "playback_url": rsc.canonical_playback_url(playback_id),
        "game_name": "", "status": status, "times_selected": 0,
        "last_selected_at": None, "created_at": now, "updated_at": now, "created_by": None,
    }
    return fake_db["playback_pool"].insert_one(doc).inserted_id


class _FakeRequest:
    query_string = b"init_data=ok"


class _Logger:
    def info(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def exception(self, *a, **k):
        pass


def _identity_jsonify(payload):
    return payload


def _load_endpoint(monkeypatch, *, invite_link="https://t.me/+minAppHash", user_id=777, username="miniuser"):
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "api_referral_share_content"
    )
    fn_node.decorator_list = []
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)

    env = {
        "request": _FakeRequest(),
        "jsonify": _identity_jsonify,
        "json": json_module,
        "logger": _Logger(),
        "extract_raw_init_data_from_query": lambda req: "init_data=ok",
        "verify_telegram_init_data": lambda init_data: (
            True, {"user": {"id": user_id, "username": username}}, None,
        ),
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102

    import types
    import sys

    fake_main = types.ModuleType("main")

    def _fake_get_or_create(uid, uname=""):  # noqa: ARG001
        return invite_link

    fake_main.get_or_create_referral_invite_link_sync = _fake_get_or_create
    monkeypatch.setitem(sys.modules, "main", fake_main)

    return env["api_referral_share_content"]


FIXED_CAPTION_LINES = (
    "👋 Welcome to AdvantPlay Community!",
    "Join our channel to get 👇",
    "🎟️ FREE Welcome Voucher — No deposit required",
    "Daily voucher drops",
    "🎁 Bonus campaigns",
    "👑 VIP-only announcements",
    "🏆 Weekly ranking rewards",
    "Start here 👇",
)


class TestApiReferralShareContent:
    def _assert_fixed_caption(self, message, invite_link):
        for line in FIXED_CAPTION_LINES:
            assert line in message
        assert message.endswith(invite_link)
        assert message.count(invite_link) == 1

    def test_active_hook_and_active_playback_never_leak_into_miniapp_output(self, fake_db, monkeypatch):
        """Even when the Creator Share Centre pools have active content, the
        Mini App's output must never include it -- entirely separate
        content surfaces."""
        _hook(fake_db, "Hook!")
        _playback(fake_db, "Play00001")
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+bothActiveHash")

        result = fn()
        payload = result[0] if isinstance(result, tuple) else result
        assert payload["ok"] is True
        assert payload["invite_link"] == "https://t.me/+bothActiveHash"
        assert "hook_text" not in payload
        assert "playback_url" not in payload
        assert "Hook!" not in payload["message"]
        assert "rx.apreplay.com" not in payload["message"]
        self._assert_fixed_caption(payload["message"], "https://t.me/+bothActiveHash")

    def test_empty_pools_still_return_fixed_caption(self, fake_db, monkeypatch):
        """Entry point #4: no active hooks/playback links in the Admin
        Dashboard -- the endpoint must still return ok:True with the fixed
        caption (never the 503 'no playback available' error this bug used
        to trigger)."""
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+onlyLinkHash")

        result = fn()
        payload = result[0] if isinstance(result, tuple) else result
        assert payload["ok"] is True
        assert payload["message"].strip() != ""
        assert "None" not in payload["message"]
        assert "\n\n\n" not in payload["message"]
        assert payload["invite_link"] == "https://t.me/+onlyLinkHash"
        self._assert_fixed_caption(payload["message"], "https://t.me/+onlyLinkHash")
        # share_text (used for the Telegram share/url button) must also be
        # non-empty, consistent, and never carry the link a second time.
        assert payload["share_text"].strip() != ""
        assert "https://t.me/+onlyLinkHash" not in payload["share_text"]

    def test_generation_never_consumes_hook_or_playback_pool_counters(self, fake_db, monkeypatch):
        """The Mini App must never draw from the Creator Share Centre's
        rotating pools -- not even silently in the background -- so their
        usage counters/selection state stay untouched by Mini App traffic."""
        hook_id = _hook(fake_db, "Hook!")
        playback_id = _playback(fake_db, "Play00001")
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+counterHash")

        fn()

        hook_doc = fake_db["caption_hooks"].find_one({"_id": hook_id})
        playback_doc = fake_db["playback_pool"].find_one({"_id": playback_id})
        assert hook_doc["times_selected"] == 0
        assert hook_doc["last_selected_at"] is None
        assert playback_doc["times_selected"] == 0
        assert playback_doc["last_selected_at"] is None

    def test_share_generations_record_tagged_with_miniapp_source(self, fake_db, monkeypatch):
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+sourceHash")
        fn()

        doc = fake_db["share_generations"].find_one({"invite_link": "https://t.me/+sourceHash"})
        assert doc is not None
        assert doc["generated_by"] == "miniapp_general_share"
        assert doc["hook_id"] is None
        assert doc["playback_record_id"] is None

    def test_no_hardcoded_referral_url_in_module(self):
        """The Mini App output must always be the backend-provided
        referral_url -- never a hardcoded/example link substituted in."""
        source = Path("referral_share_content.py").read_text(encoding="utf-8")
        assert "t.me/+" not in source
        assert "https://t.me/joinchat" not in source
