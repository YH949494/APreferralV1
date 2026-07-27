"""Tests for POST /api/referral/share-content (Mini App "Generate Link" /
copy flow, entry point #4). Isolates api_referral_share_content() from
main.py via AST extraction (same pattern as test_api_referral_contract.py /
test_share_rank_caption.py) and drives it against the real
referral_share_content.generate_share_package with a fake DB, monkeypatching
only the canonical invite-link creator -- referral attribution/XP/etc. are
untouched by this endpoint and out of scope here.
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


class TestApiReferralShareContent:
    def test_active_hook_and_active_playback(self, fake_db, monkeypatch):
        _hook(fake_db, "Hook!")
        _playback(fake_db, "Play00001")
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+bothActiveHash")

        result = fn()
        payload = result[0] if isinstance(result, tuple) else result
        assert payload["ok"] is True
        assert payload["hook_text"] == "Hook!"
        assert payload["playback_url"] == rsc.canonical_playback_url("Play00001")
        assert payload["invite_link"] == "https://t.me/+bothActiveHash"
        assert payload["message"].endswith("https://t.me/+bothActiveHash")

    def test_no_active_hook_no_active_playback_returns_static_fallback(self, fake_db, monkeypatch):
        """Entry point #4: all hooks and playback links deactivated in the
        Admin Dashboard -- the endpoint must still return ok:True with a
        valid, non-empty message (never the 503 'no playback available'
        error this bug used to trigger)."""
        _hook(fake_db, "Inactive", status="inactive")
        _playback(fake_db, "InactivePB", status="inactive")
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+onlyLinkHash")

        result = fn()
        payload = result[0] if isinstance(result, tuple) else result
        assert payload["ok"] is True
        assert payload["message"].strip() != ""
        assert "None" not in payload["message"]
        assert "\n\n\n" not in payload["message"]
        assert payload["message"].endswith("https://t.me/+onlyLinkHash")
        assert payload["invite_link"] == "https://t.me/+onlyLinkHash"
        assert payload["hook_text"] is None
        assert payload["playback_url"] is None
        # share_text (used for the Telegram share/url button) must also be
        # non-empty and consistent with the same builder.
        assert payload["share_text"].strip() != ""
        assert "https://t.me/+onlyLinkHash" not in payload["share_text"]

    def test_no_active_playback_only_omits_playback_section(self, fake_db, monkeypatch):
        _hook(fake_db, "Hook only")
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+hookOnlyHash")

        result = fn()
        payload = result[0] if isinstance(result, tuple) else result
        assert payload["ok"] is True
        assert payload["hook_text"] == "Hook only"
        assert payload["playback_url"] is None
        assert "rx.apreplay.com" not in payload["message"]

    def test_no_active_hook_only_omits_hook_section(self, fake_db, monkeypatch):
        _playback(fake_db, "Play00002")
        fn = _load_endpoint(monkeypatch, invite_link="https://t.me/+pbOnlyHash")

        result = fn()
        payload = result[0] if isinstance(result, tuple) else result
        assert payload["ok"] is True
        assert payload["hook_text"] is None
        assert payload["playback_url"] == rsc.canonical_playback_url("Play00002")
        assert payload["message"].startswith(rsc.canonical_playback_url("Play00002"))
