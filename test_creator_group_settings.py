"""Tests for the admin-configurable Creator Group Access settings:
GET/PUT /api/admin/referral/creator-settings, POST .../verify-group, the
canonical get_creator_group_access_settings() reader (DB -> env -> default
config resolution order), and cache invalidation on save."""

from __future__ import annotations

import sys
import types

import pytest
from flask import Flask

import creator_share_centre as csc
import database
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    csc.invalidate_creator_group_settings_cache()
    monkeypatch.setenv("BOT_TOKEN", "test-bot-token")
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _fake_vouchers_module(monkeypatch, *, admin=True, user_id=555, username="creator1"):
    fake_vouchers = types.ModuleType("vouchers")

    def _require_admin():
        if admin:
            return {"id": 1, "usernameLower": "admin"}, None
        from flask import jsonify

        return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

    def _extract_raw_init_data_from_query(request):
        return "init_data=ok"

    def _verify_telegram_init_data(init_data):
        return True, {"user": {"id": user_id, "username": username}}, None

    fake_vouchers.require_admin = _require_admin
    fake_vouchers.extract_raw_init_data_from_query = _extract_raw_init_data_from_query
    fake_vouchers.verify_telegram_init_data = _verify_telegram_init_data
    monkeypatch.setitem(sys.modules, "vouchers", fake_vouchers)


@pytest.fixture
def app(fake_db):
    flask_app = Flask(__name__)
    flask_app.register_blueprint(csc.creator_share_bp)
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


class _FakeResp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _telegram_ok(bot_id=999, bot_status="administrator", chat_title="Creators HQ", chat_type="supergroup"):
    def _get(url, params=None, timeout=None):
        if url.endswith("/getChat"):
            return _FakeResp(200, {"ok": True, "result": {"id": params["chat_id"], "title": chat_title, "type": chat_type}})
        if url.endswith("/getMe"):
            return _FakeResp(200, {"ok": True, "result": {"id": bot_id}})
        if url.endswith("/getChatMember"):
            return _FakeResp(200, {"ok": True, "result": {"status": bot_status}})
        raise AssertionError("unexpected telegram call: " + url)

    return _get


def _telegram_not_found():
    def _get(url, params=None, timeout=None):
        if url.endswith("/getChat"):
            return _FakeResp(400, {"ok": False, "description": "Bad Request: chat not found"})
        raise AssertionError("unexpected telegram call: " + url)

    return _get


def _telegram_bot_access_denied():
    def _get(url, params=None, timeout=None):
        if url.endswith("/getChat"):
            return _FakeResp(200, {"ok": True, "result": {"id": -1001234567890, "title": "Creators HQ", "type": "supergroup"}})
        if url.endswith("/getMe"):
            return _FakeResp(200, {"ok": True, "result": {"id": 999}})
        if url.endswith("/getChatMember"):
            return _FakeResp(400, {"ok": False, "description": "Forbidden: bot is not a member"})
        raise AssertionError("unexpected telegram call: " + url)

    return _get


def _telegram_wrong_type():
    def _get(url, params=None, timeout=None):
        if url.endswith("/getChat"):
            return _FakeResp(200, {"ok": True, "result": {"id": -1001234567890, "title": "Private DM", "type": "private"}})
        raise AssertionError("unexpected telegram call: " + url)

    return _get


def _telegram_channel_ok(bot_id=999, bot_status="administrator", chat_title="AdvantPlay Channel", chat_id=-1003820861717):
    def _get(url, params=None, timeout=None):
        if url.endswith("/getChat"):
            return _FakeResp(200, {"ok": True, "result": {"id": params["chat_id"], "title": chat_title, "type": "channel"}})
        if url.endswith("/getMe"):
            return _FakeResp(200, {"ok": True, "result": {"id": bot_id}})
        if url.endswith("/getChatMember"):
            return _FakeResp(200, {"ok": True, "result": {"status": bot_status}})
        raise AssertionError("unexpected telegram call: " + url)

    return _get


def _telegram_channel_bot_access_denied(chat_id=-1003820861717):
    def _get(url, params=None, timeout=None):
        if url.endswith("/getChat"):
            return _FakeResp(200, {"ok": True, "result": {"id": params["chat_id"], "title": "AdvantPlay Channel", "type": "channel"}})
        if url.endswith("/getMe"):
            return _FakeResp(200, {"ok": True, "result": {"id": 999}})
        if url.endswith("/getChatMember"):
            return _FakeResp(400, {"ok": False, "description": "Forbidden: bot is not a member"})
        raise AssertionError("unexpected telegram call: " + url)

    return _get


class TestAdminSettingsAuth:
    def test_admin_can_read_settings(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        resp = client.get("/api/admin/referral/creator-settings")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"

    def test_non_admin_denied(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=False)
        resp = client.get("/api/admin/referral/creator-settings")
        assert resp.status_code == 401


class TestVerifyAndSave:
    def test_valid_supergroup_verifies_and_saves(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok())

        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["settings"]["creator_group_chat_id"] == -1001234567890
        assert body["settings"]["chat_title"] == "Creators HQ"
        assert body["settings"]["chat_type"] == "supergroup"
        assert body["settings"]["bot_membership_status"] == "administrator"
        assert body["settings"]["verified_at"] is not None
        assert body["settings"]["config_version"] == 1

        doc = fake_db["app_settings"].find_one({"_id": "creator_group_access"})
        assert doc["updated_by"] == 1
        assert doc["config_version"] == 1

    def test_positive_id_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "invalid_creator_group_chat_id"

    def test_zero_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "0", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "invalid_creator_group_chat_id"

    def test_non_integer_id_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "not-a-number", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "invalid_creator_group_chat_id"

    def test_nonexistent_group_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_not_found())
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1009999999999", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "creator_group_not_found"
        assert fake_db["app_settings"].find_one({"_id": "creator_group_access"}) is None

    def test_bot_without_access_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_bot_access_denied())
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "creator_group_bot_access_denied"

    def test_wrong_chat_type_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_wrong_type())
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "creator_group_wrong_chat_type"

    def test_verify_group_endpoint_previews_without_saving(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok())
        resp = client.post(
            "/api/admin/referral/creator-settings/verify-group",
            json={"creator_group_chat_id": "-1001234567890"},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["chat_title"] == "Creators HQ"
        assert fake_db["app_settings"].find_one({"_id": "creator_group_access"}) is None

    def test_verification_failure_does_not_overwrite_last_valid_setting(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok())
        client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001111111111", "membership_check_enabled": True},
        )
        original = fake_db["app_settings"].find_one({"_id": "creator_group_access"})
        assert original["creator_group_chat_id"] == -1001111111111

        monkeypatch.setattr(csc.requests, "get", _telegram_not_found())
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1002222222222", "membership_check_enabled": True},
        )
        assert resp.status_code == 400

        unchanged = fake_db["app_settings"].find_one({"_id": "creator_group_access"})
        assert unchanged["creator_group_chat_id"] == -1001111111111
        assert unchanged["config_version"] == 1

    def test_force_save_persists_unverified_group_and_is_audited(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_not_found())

        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1009999999999", "membership_check_enabled": True, "force_save": True},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["settings"]["creator_group_chat_id"] == -1009999999999
        assert body["settings"]["verified_at"] is None

        audit = fake_db["app_settings_audit"].find_one({"group": "creator_group_access"})
        assert audit is not None
        assert audit["force_save"] is True
        assert audit["unverified"] is True
        assert audit["verify_error"] == "creator_group_not_found"


class TestChatTypeSupport:
    """group / supergroup / channel are all accepted chat types for the
    Creator Access Chat; only unsupported types (e.g. private) are rejected
    with creator_group_wrong_chat_type."""

    def test_group_accepted(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok(chat_type="group"))
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 200
        assert resp.get_json()["settings"]["chat_type"] == "group"

    def test_supergroup_accepted(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok(chat_type="supergroup"))
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 200
        assert resp.get_json()["settings"]["chat_type"] == "supergroup"

    def test_channel_accepted(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok(chat_type="channel"))
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["settings"]["chat_type"] == "channel"
        assert body["settings"]["creator_group_chat_id"] == -1001234567890

    def test_private_chat_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_wrong_type())
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001234567890", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "creator_group_wrong_chat_type"

    def test_specific_channel_verifies_successfully_when_bot_is_administrator(self, fake_db, monkeypatch, client):
        # The real channel referenced in the Creator Access Chat rollout:
        # -1003820861717, type=channel, bot is an administrator there.
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_channel_ok(chat_id=-1003820861717))
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1003820861717", "membership_check_enabled": True},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["settings"]["creator_group_chat_id"] == -1003820861717
        assert body["settings"]["chat_type"] == "channel"
        assert body["settings"]["bot_membership_status"] == "administrator"
        assert body["settings"]["verified_at"] is not None

    def test_channel_bot_without_access_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, admin=True)
        monkeypatch.setattr(csc.requests, "get", _telegram_channel_bot_access_denied(chat_id=-1003820861717))
        resp = client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1003820861717", "membership_check_enabled": True},
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "creator_group_bot_access_denied"


class TestResolutionOrder:
    def test_database_setting_overrides_environment_fallback(self, fake_db, monkeypatch):
        monkeypatch.setenv("CREATOR_GROUP_CHAT_ID", "-1009999999999")
        fake_db["app_settings"].insert_one(
            {
                "_id": "creator_group_access",
                "creator_group_chat_id": -1001234567890,
                "membership_check_enabled": True,
                "chat_title": "DB Group",
                "chat_type": "supergroup",
                "bot_membership_status": "administrator",
                "verified_at": csc.now_utc(),
                "updated_at": csc.now_utc(),
                "updated_by": 1,
                "config_version": 1,
            }
        )
        settings = csc.get_creator_group_access_settings(force_refresh=True)
        assert settings["creator_group_chat_id"] == -1001234567890
        assert settings["source"] == "database"

    def test_environment_fallback_before_any_db_setting_exists(self, fake_db, monkeypatch):
        monkeypatch.setenv("CREATOR_GROUP_CHAT_ID", "-1009999999999")
        settings = csc.get_creator_group_access_settings(force_refresh=True)
        assert settings["creator_group_chat_id"] == -1009999999999
        assert settings["membership_check_enabled"] is True
        assert settings["source"] == "env"

    def test_no_env_no_db_is_unconfigured(self, fake_db, monkeypatch):
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        settings = csc.get_creator_group_access_settings(force_refresh=True)
        assert settings["creator_group_chat_id"] is None
        assert settings["membership_check_enabled"] is False
        assert settings["source"] == "unconfigured"

    def test_explicit_db_setting_authoritative_even_when_cleared(self, fake_db, monkeypatch):
        # Once a DB doc has been saved (even one that clears the chat ID back
        # to null), it is authoritative -- the env var must not "win back".
        monkeypatch.setenv("CREATOR_GROUP_CHAT_ID", "-1009999999999")
        fake_db["app_settings"].insert_one(
            {
                "_id": "creator_group_access",
                "creator_group_chat_id": None,
                "membership_check_enabled": False,
                "chat_title": None,
                "chat_type": None,
                "bot_membership_status": None,
                "verified_at": None,
                "updated_at": csc.now_utc(),
                "updated_by": 1,
                "config_version": 2,
            }
        )
        settings = csc.get_creator_group_access_settings(force_refresh=True)
        assert settings["creator_group_chat_id"] is None
        assert settings["source"] == "database"


class TestAccessGating:
    def _creator(self, fake_db, user_id=555, status="active"):
        now = csc.now_utc()
        fake_db["creator_members"].insert_one(
            {
                "user_id": user_id,
                "status": status,
                "source_group_id": None,
                "creator_tier": "pilot",
                "approved_at": now,
                "approved_by": 1,
                "last_membership_verified_at": None,
                "created_at": now,
                "updated_at": now,
            }
        )

    def test_disabled_membership_checking_uses_creator_members_only(self, fake_db, monkeypatch):
        self._creator(fake_db, 555)
        fake_db["app_settings"].insert_one(
            {
                "_id": "creator_group_access",
                "creator_group_chat_id": -1001234567890,
                "membership_check_enabled": False,
                "chat_title": "Creators HQ",
                "chat_type": "supergroup",
                "bot_membership_status": "administrator",
                "verified_at": csc.now_utc(),
                "updated_at": csc.now_utc(),
                "updated_by": 1,
                "config_version": 1,
            }
        )
        # Telegram would be called if membership checking were enabled; make
        # any such call fail the test loudly.
        def _boom(*a, **kw):
            raise AssertionError("Telegram should not be called when membership_check_enabled=False")

        monkeypatch.setattr(csc.requests, "get", _boom)

        record, err = csc._verify_creator_access(555)
        assert err is None
        assert record is not None

    def test_enabled_but_missing_configuration_fails_closed(self, fake_db, monkeypatch):
        self._creator(fake_db, 555)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        fake_db["app_settings"].insert_one(
            {
                "_id": "creator_group_access",
                "creator_group_chat_id": None,
                "membership_check_enabled": True,
                "chat_title": None,
                "chat_type": None,
                "bot_membership_status": None,
                "verified_at": None,
                "updated_at": csc.now_utc(),
                "updated_by": 1,
                "config_version": 1,
            }
        )
        record, err = csc._verify_creator_access(555)
        assert record is None
        assert err == ("creator_group_not_configured", 503)


class TestCacheInvalidationOnGroupChange:
    def _creator(self, fake_db, user_id=555):
        now = csc.now_utc()
        fake_db["creator_members"].insert_one(
            {
                "user_id": user_id,
                "status": "active",
                "source_group_id": None,
                "creator_tier": "pilot",
                "approved_at": now,
                "approved_by": 1,
                "last_membership_verified_at": None,
                "created_at": now,
                "updated_at": now,
            }
        )

    def test_changing_group_clears_cached_membership_verdicts(self, fake_db, monkeypatch, client):
        self._creator(fake_db, 555)
        _fake_vouchers_module(monkeypatch, admin=True, user_id=555)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok())

        client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001111111111", "membership_check_enabled": True},
        )

        calls = {"n": 0}
        monkeypatch.setattr(csc, "_check_group_membership", lambda uid, chat_id: (calls.__setitem__("n", calls["n"] + 1), True)[1])

        record, err = csc._verify_creator_access(555)
        assert err is None
        assert calls["n"] == 1

        # Cached -> a second check within the request would not re-hit Telegram.
        record, err = csc._verify_creator_access(555)
        assert err is None
        assert calls["n"] == 1

        # Now the admin points the setting at a different group.
        client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1002222222222", "membership_check_enabled": True},
        )

        # The previously cached "True" verdict must not survive the group
        # change -- Telegram must be consulted again.
        record, err = csc._verify_creator_access(555)
        assert calls["n"] == 2

    def test_old_group_members_cannot_use_cached_access_after_change(self, fake_db, monkeypatch, client):
        self._creator(fake_db, 555)
        _fake_vouchers_module(monkeypatch, admin=True, user_id=555)
        monkeypatch.setattr(csc.requests, "get", _telegram_ok())

        client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1001111111111", "membership_check_enabled": True},
        )
        monkeypatch.setattr(csc, "_check_group_membership", lambda uid, chat_id: True)
        record, err = csc._verify_creator_access(555)
        assert err is None

        # Admin switches to a new group; user 555 is not in it.
        monkeypatch.setattr(csc.requests, "get", _telegram_ok())
        client.put(
            "/api/admin/referral/creator-settings",
            json={"creator_group_chat_id": "-1002222222222", "membership_check_enabled": True},
        )
        monkeypatch.setattr(csc, "_check_group_membership", lambda uid, chat_id: False)

        record, err = csc._verify_creator_access(555)
        assert record is None
        assert err == ("creator_membership_required", 403)
