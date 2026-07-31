"""Tests for creator_share_centre.py access control: Telegram initData
authentication + creator_members authorization, independent of admin auth
and independent of the general referral/XP system."""

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
    # The module caches the creator_group_access Mongo doc in-process for a
    # short TTL; without resetting it, a doc cached by an earlier test's
    # (now-discarded) FakeDb instance would leak into this test.
    csc.invalidate_creator_group_settings_cache()
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _fake_vouchers_module(monkeypatch, *, verified=True, user_id=555, username="creator1"):
    fake_vouchers = types.ModuleType("vouchers")

    def _extract_raw_init_data_from_query(request):
        return "init_data=ok" if verified is not None else ""

    def _verify_telegram_init_data(init_data):
        if not verified:
            return False, None, None
        return True, {"user": {"id": user_id, "username": username}}, None

    def _require_admin():
        return {"id": 1, "usernameLower": "admin"}, None

    fake_vouchers.extract_raw_init_data_from_query = _extract_raw_init_data_from_query
    fake_vouchers.verify_telegram_init_data = _verify_telegram_init_data
    fake_vouchers.require_admin = _require_admin
    monkeypatch.setitem(sys.modules, "vouchers", fake_vouchers)


@pytest.fixture
def app(fake_db):
    flask_app = Flask(__name__)
    flask_app.register_blueprint(csc.creator_share_bp)
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def _creator(fake_db, user_id=555, status="active", **extra):
    now = csc.now_utc()
    doc = {
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
    doc.update(extra)
    fake_db["creator_members"].insert_one(doc)
    return doc


class TestAccess:
    def test_valid_creator_accepted(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 555)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.get("/api/creator/share/status?init_data=ok")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["creator"]["access"] is True
        assert body["creator"]["user_id"] == 555

    def test_user_without_creator_record_denied(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=999)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.get("/api/creator/share/status?init_data=ok")
        assert resp.status_code == 403
        assert resp.get_json()["code"] == "creator_not_authorized"

    def test_suspended_creator_denied(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 555, status="suspended")
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.get("/api/creator/share/status?init_data=ok")
        assert resp.status_code == 403
        assert resp.get_json()["code"] == "creator_suspended"

    def test_removed_creator_denied_as_not_authorized(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 555, status="removed")
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.get("/api/creator/share/status?init_data=ok")
        assert resp.status_code == 403
        assert resp.get_json()["code"] == "creator_not_authorized"

    def test_invalid_telegram_init_data_denied(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, verified=False, user_id=555)
        _creator(fake_db, 555)

        resp = client.get("/api/creator/share/status?init_data=bad")
        assert resp.status_code == 401
        assert resp.get_json()["code"] == "invalid_telegram_auth"

    def test_missing_init_data_denied(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, verified=None, user_id=555)
        _creator(fake_db, 555)

        resp = client.get("/api/creator/share/status")
        assert resp.status_code == 401
        assert resp.get_json()["code"] == "invalid_telegram_auth"

    def test_spoofed_json_user_id_ignored(self, fake_db, monkeypatch, client):
        # The authenticated user (555, from verified initData) has no
        # creator record; a JSON body claiming to be a different
        # (approved) user_id must never grant access.
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 999, status="active")
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.post(
            "/api/creator/share/generate?init_data=ok",
            json={"platform": "generic", "user_id": 999},
        )
        assert resp.status_code == 403
        assert resp.get_json()["code"] == "creator_not_authorized"

    def test_temporary_membership_failure_uses_short_cache_not_denial(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 555)
        monkeypatch.setenv("CREATOR_GROUP_CHAT_ID", "-1001234567890")

        # Telegram lookup is temporarily unavailable (network error) -> the
        # very first check has no cache yet, so it is reported as
        # unresolvable rather than a confirmed non-member denial.
        monkeypatch.setattr(csc, "_check_group_membership", lambda uid, chat_id: None)

        resp = client.get("/api/creator/share/status?init_data=ok")
        assert resp.status_code == 503
        assert resp.get_json()["code"] == "creator_membership_unresolvable"

        # Immediately retrying within the grace window still does not flip
        # to a hard denial and does not remove the creator record.
        resp2 = client.get("/api/creator/share/status?init_data=ok")
        assert resp2.status_code == 503
        record = fake_db["creator_members"].find_one({"user_id": 555})
        assert record["status"] == "active"

    def test_confirmed_left_group_denies_and_marks_removed(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 555)
        monkeypatch.setenv("CREATOR_GROUP_CHAT_ID", "-1001234567890")
        monkeypatch.setattr(csc, "_check_group_membership", lambda uid, chat_id: False)

        resp = client.get("/api/creator/share/status?init_data=ok")
        assert resp.status_code == 403
        assert resp.get_json()["code"] == "creator_membership_required"
        record = fake_db["creator_members"].find_one({"user_id": 555})
        assert record["status"] == "removed"

    def test_confirmed_member_cached_for_subsequent_requests(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch, user_id=555)
        _creator(fake_db, 555)
        monkeypatch.setenv("CREATOR_GROUP_CHAT_ID", "-1001234567890")

        calls = {"n": 0}

        def _check(uid, chat_id):
            calls["n"] += 1
            return True

        monkeypatch.setattr(csc, "_check_group_membership", _check)

        resp1 = client.get("/api/creator/share/status?init_data=ok")
        assert resp1.status_code == 200
        resp2 = client.get("/api/creator/share/status?init_data=ok")
        assert resp2.status_code == 200
        # Second call reuses the cached verification instead of hitting
        # Telegram again.
        assert calls["n"] == 1


class TestAdminAuthNotCreatorAuth:
    def test_admin_routes_require_admin_not_creator_membership(self, fake_db, monkeypatch, app):
        fake_vouchers = types.ModuleType("vouchers")

        def _require_admin_denied():
            from flask import jsonify

            return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

        fake_vouchers.require_admin = _require_admin_denied
        fake_vouchers.extract_raw_init_data_from_query = lambda request: ""
        fake_vouchers.verify_telegram_init_data = lambda init_data: (False, None, None)
        monkeypatch.setitem(sys.modules, "vouchers", fake_vouchers)

        client = app.test_client()
        resp = client.get("/api/admin/referral/creators")
        assert resp.status_code == 401
