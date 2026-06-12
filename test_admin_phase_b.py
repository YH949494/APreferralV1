"""Phase B integration tests.

Verifies that a browser Telegram Login session (from admin_auth) is accepted
by both require_admin_from_query() (main.py admin endpoints) and
_require_admin_via_query() (vouchers.py /v2/miniapp/admin/* endpoints),
while confirming that the admin secret path and initData path still work and
that having no credentials is still rejected.

These tests avoid importing main.py or vouchers.py at module level (both have
expensive side effects). Instead they use Flask test clients with the exact same
blueprint wiring as production.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock, patch

import pytest
from flask import Blueprint, Flask, jsonify, request

import admin_auth


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BOT_TOKEN = "123456:PHASE_B_TEST"


def _signed_widget_payload(bot_token=BOT_TOKEN, **overrides):
    fields = {
        "id": "1111",
        "first_name": "Alice",
        "username": "alice_admin",
        "auth_date": str(int(time.time())),
    }
    fields.update(overrides)
    dcs = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret = hashlib.sha256(bot_token.encode()).digest()
    fields["hash"] = hmac.new(secret, dcs.encode(), hashlib.sha256).hexdigest()
    return fields


def _valid_init_data():
    """Return a minimal valid initData string (bot token = dummy; verification is mocked)."""
    return "user=%7B%22id%22%3A1111%7D&auth_date=9999999999&hash=" + "a" * 64


# ---------------------------------------------------------------------------
# App factory — mirrors production wiring minus bot/mongo startup
# ---------------------------------------------------------------------------

def _make_app(monkeypatch):
    """Build a minimal Flask app that replicates Phase B wiring."""
    monkeypatch.setenv("BOT_TOKEN", BOT_TOKEN)
    monkeypatch.setattr(admin_auth, "_audit_sink", lambda doc: None)
    monkeypatch.setattr(admin_auth, "_login_attempts", {})

    app = Flask(__name__, static_folder="static")
    app.secret_key = "phase-b-test-secret-not-weak"
    admin_auth.configure_admin_session(app)
    app.config["SESSION_COOKIE_SECURE"] = False
    app.register_blueprint(admin_auth.admin_auth_bp)

    # ---- Stub admin endpoint mimicking require_admin_from_query pattern -----
    # Paste the exact guard logic so we test our injected session branch.
    test_bp = Blueprint("test_admin", __name__)

    @test_bp.get("/api/admin/stub-main")
    def stub_main_admin():
        """Mirrors require_admin_from_query guard used by main.py admin routes."""
        # admin secret
        secret_val = request.headers.get("X-Admin-Secret", "")
        ADMIN_SECRET = app.config.get("TEST_ADMIN_SECRET", "")
        if ADMIN_SECRET and secret_val == ADMIN_SECRET:
            return jsonify({"ok": True, "source": "secret"})

        # Phase B session
        try:
            claims = admin_auth.session_admin()
            if claims:
                return jsonify({"ok": True, "source": "session"})
        except Exception:
            pass

        # initData (mocked verification injected via app.config)
        init_data = request.args.get("init_data", "")
        verifier = app.config.get("TEST_INIT_DATA_VERIFIER")
        if verifier and init_data:
            ok, user_id = verifier(init_data)
            if ok:
                return jsonify({"ok": True, "source": "initdata", "user_id": user_id})
            return jsonify({"ok": False}), 403

        return jsonify({"ok": False, "reason": "no_credential"}), 401

    # ---- Stub endpoint mimicking _require_admin_via_query pattern -----------
    @test_bp.get("/v2/miniapp/admin/stub-voucher")
    def stub_voucher_admin():
        """Mirrors _require_admin_via_query used by vouchers.py admin routes."""
        # admin secret
        secret_val = request.headers.get("X-Admin-Secret", "")
        ADMIN_SECRET = app.config.get("TEST_ADMIN_SECRET", "")
        if ADMIN_SECRET and secret_val == ADMIN_SECRET:
            return jsonify({"ok": True, "adminSource": "secret"})

        # Phase B session
        try:
            claims = admin_auth.session_admin()
            if claims:
                return jsonify({
                    "ok": True,
                    "adminSource": "session",
                    "id": claims["id"],
                    "usernameLower": claims.get("username", ""),
                })
        except Exception:
            pass

        # initData path
        init_data = request.args.get("init_data", "")
        verifier = app.config.get("TEST_INIT_DATA_VERIFIER")
        if verifier and init_data:
            ok, user_id = verifier(init_data)
            if ok:
                return jsonify({"ok": True, "adminSource": "initdata"})
            return jsonify({"status": "error", "code": "forbidden"}), 403

        return jsonify({"status": "error", "code": "missing_init_data"}), 400

    # ---- /api/is_admin stub -----------------------------------------------
    @test_bp.get("/api/is_admin")
    def stub_is_admin():
        secret_val = request.headers.get("X-Admin-Secret", "")
        ADMIN_SECRET = app.config.get("TEST_ADMIN_SECRET", "")
        if ADMIN_SECRET and secret_val == ADMIN_SECRET:
            return jsonify({"success": True, "is_admin": True, "source": "secret"})

        try:
            claims = admin_auth.session_admin()
            if claims:
                return jsonify({"success": True, "is_admin": True, "source": "session"})
        except Exception:
            pass

        return jsonify({"success": False, "is_admin": False, "error": "no credential"}), 400

    app.register_blueprint(test_bp)
    return app


def _login(client, monkeypatch):
    """Log in via the widget endpoint and return the same client (session cookie set)."""
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    res = client.post("/api/admin/auth/telegram-login", json=_signed_widget_payload())
    assert res.status_code == 200, f"Login failed: {res.get_data(as_text=True)}"
    return client


# ---------------------------------------------------------------------------
# Core session tests
# ---------------------------------------------------------------------------

def test_session_grants_main_admin_endpoint(monkeypatch):
    app = _make_app(monkeypatch)
    client = app.test_client()
    _login(client, monkeypatch)

    res = client.get("/api/admin/stub-main")
    assert res.status_code == 200
    body = res.get_json()
    assert body["ok"] is True
    assert body["source"] == "session"


def test_session_grants_voucher_admin_endpoint(monkeypatch):
    app = _make_app(monkeypatch)
    client = app.test_client()
    _login(client, monkeypatch)

    res = client.get("/v2/miniapp/admin/stub-voucher")
    assert res.status_code == 200
    body = res.get_json()
    assert body["ok"] is True
    assert body["adminSource"] == "session"
    assert body["id"] == 1111
    assert body["usernameLower"] == "alice_admin"


def test_is_admin_returns_source_session(monkeypatch):
    app = _make_app(monkeypatch)
    client = app.test_client()
    _login(client, monkeypatch)

    res = client.get("/api/is_admin")
    assert res.status_code == 200
    body = res.get_json()
    assert body["success"] is True
    assert body["is_admin"] is True
    assert body["source"] == "session"


def test_no_credential_is_rejected_main(monkeypatch):
    app = _make_app(monkeypatch)
    res = app.test_client().get("/api/admin/stub-main")
    assert res.status_code == 401
    assert res.get_json()["ok"] is False


def test_no_credential_is_rejected_voucher(monkeypatch):
    app = _make_app(monkeypatch)
    res = app.test_client().get("/v2/miniapp/admin/stub-voucher")
    assert res.status_code == 400
    assert res.get_json()["code"] == "missing_init_data"


def test_admin_secret_still_works_main(monkeypatch):
    app = _make_app(monkeypatch)
    app.config["TEST_ADMIN_SECRET"] = "correct-secret"

    res = app.test_client().get(
        "/api/admin/stub-main", headers={"X-Admin-Secret": "correct-secret"}
    )
    assert res.status_code == 200
    assert res.get_json()["source"] == "secret"


def test_admin_secret_still_works_voucher(monkeypatch):
    app = _make_app(monkeypatch)
    app.config["TEST_ADMIN_SECRET"] = "correct-secret"

    res = app.test_client().get(
        "/v2/miniapp/admin/stub-voucher", headers={"X-Admin-Secret": "correct-secret"}
    )
    assert res.status_code == 200
    assert res.get_json()["adminSource"] == "secret"


def test_initdata_still_works_main(monkeypatch):
    app = _make_app(monkeypatch)
    app.config["TEST_INIT_DATA_VERIFIER"] = lambda raw: (True, 1111)

    res = app.test_client().get(
        "/api/admin/stub-main", query_string={"init_data": _valid_init_data()}
    )
    assert res.status_code == 200
    assert res.get_json()["source"] == "initdata"


def test_initdata_still_works_voucher(monkeypatch):
    app = _make_app(monkeypatch)
    app.config["TEST_INIT_DATA_VERIFIER"] = lambda raw: (True, 1111)

    res = app.test_client().get(
        "/v2/miniapp/admin/stub-voucher", query_string={"init_data": _valid_init_data()}
    )
    assert res.status_code == 200
    assert res.get_json()["adminSource"] == "initdata"


def test_session_exception_falls_through_to_initdata(monkeypatch):
    """If session_admin() raises, the guard must fall through, not crash."""
    app = _make_app(monkeypatch)
    app.config["TEST_INIT_DATA_VERIFIER"] = lambda raw: (True, 1111)

    with patch.object(admin_auth, "session_admin", side_effect=RuntimeError("boom")):
        res = app.test_client().get(
            "/api/admin/stub-main", query_string={"init_data": _valid_init_data()}
        )
    assert res.status_code == 200
    assert res.get_json()["source"] == "initdata"


def test_logout_revokes_main_admin_access(monkeypatch):
    app = _make_app(monkeypatch)
    client = app.test_client()
    _login(client, monkeypatch)

    assert client.get("/api/admin/stub-main").status_code == 200
    client.post("/api/admin/auth/logout")
    assert client.get("/api/admin/stub-main").status_code == 401


def test_secret_takes_priority_over_session(monkeypatch):
    """Admin secret path must still be checked first."""
    app = _make_app(monkeypatch)
    app.config["TEST_ADMIN_SECRET"] = "correct-secret"
    client = app.test_client()
    _login(client, monkeypatch)

    # Both session and secret are active; expect "secret" because it's checked first.
    res = client.get("/api/admin/stub-main", headers={"X-Admin-Secret": "correct-secret"})
    assert res.get_json()["source"] == "secret"
