import hashlib
import hmac
import time

from flask import Flask

import admin_auth

BOT_TOKEN = "123456:TEST_TOKEN"


def _signed_payload(bot_token=BOT_TOKEN, auth_date=None, tamper=None, **overrides):
    fields = {
        "id": "1111",
        "first_name": "Alice",
        "username": "alice_admin",
        "auth_date": str(int(auth_date if auth_date is not None else time.time())),
    }
    fields.update(overrides)
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret = hashlib.sha256(bot_token.encode()).digest()
    fields["hash"] = hmac.new(secret, data_check_string.encode(), hashlib.sha256).hexdigest()
    if tamper:
        fields.update(tamper)
    return fields


def _make_app(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", BOT_TOKEN)
    monkeypatch.setattr(admin_auth, "_audit_sink", lambda doc: None)
    monkeypatch.setattr(admin_auth, "_login_attempts", {})
    app = Flask(__name__, static_folder="static")
    app.secret_key = "unit-test-secret-key-not-weak"
    admin_auth.configure_admin_session(app)
    app.config["SESSION_COOKIE_SECURE"] = False  # test client uses http
    app.register_blueprint(admin_auth.admin_auth_bp)
    return app


# ---- verify_login_widget_payload ----

def test_verify_valid_payload():
    ok, user, reason = admin_auth.verify_login_widget_payload(_signed_payload(), BOT_TOKEN)
    assert ok is True
    assert reason == "ok"
    assert user["id"] == 1111
    assert user["username"] == "alice_admin"


def test_verify_rejects_tampered_field():
    payload = _signed_payload(tamper={"id": "9999"})
    ok, _, reason = admin_auth.verify_login_widget_payload(payload, BOT_TOKEN)
    assert ok is False
    assert reason == "invalid_hash"


def test_verify_rejects_wrong_token():
    ok, _, reason = admin_auth.verify_login_widget_payload(_signed_payload(), "999:OTHER")
    assert ok is False
    assert reason == "invalid_hash"


def test_verify_rejects_expired_auth_date():
    payload = _signed_payload(auth_date=time.time() - 3600)
    ok, _, reason = admin_auth.verify_login_widget_payload(payload, BOT_TOKEN)
    assert ok is False
    assert reason == "expired"


def test_verify_rejects_missing_hash():
    payload = _signed_payload()
    payload.pop("hash")
    ok, _, reason = admin_auth.verify_login_widget_payload(payload, BOT_TOKEN)
    assert ok is False
    assert reason == "missing_hash"


def test_verify_rejects_missing_bot_token():
    ok, _, reason = admin_auth.verify_login_widget_payload(_signed_payload(), "")
    assert ok is False
    assert reason == "missing_bot_token"


# ---- endpoints ----

def test_me_requires_session(monkeypatch):
    app = _make_app(monkeypatch)
    res = app.test_client().get("/api/admin/auth/me")
    assert res.status_code == 401
    assert res.get_json()["code"] == "no_session"


def test_login_post_success_creates_session(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    client = app.test_client()

    res = client.post("/api/admin/auth/telegram-login", json=_signed_payload())
    assert res.status_code == 200
    body = res.get_json()
    assert body["success"] is True
    assert body["user"]["id"] == 1111

    me = client.get("/api/admin/auth/me")
    assert me.status_code == 200
    assert me.get_json()["admin"]["id"] == 1111


def test_login_denies_non_whitelisted_user(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (False, None))
    client = app.test_client()

    res = client.post("/api/admin/auth/telegram-login", json=_signed_payload())
    assert res.status_code == 403
    assert res.get_json()["code"] == "not_admin"
    assert client.get("/api/admin/auth/me").status_code == 401


def test_login_rejects_invalid_hash(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    res = app.test_client().post(
        "/api/admin/auth/telegram-login", json=_signed_payload(tamper={"username": "evil"})
    )
    assert res.status_code == 401
    assert res.get_json()["code"] == "invalid_hash"


def test_login_get_redirect_mode(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    client = app.test_client()

    res = client.get("/api/admin/auth/telegram-login", query_string=_signed_payload())
    assert res.status_code == 302
    assert res.headers["Location"].endswith("/admin")

    bad = client.get("/api/admin/auth/telegram-login", query_string={"id": "1"})
    assert bad.status_code == 302
    assert "login_error=" in bad.headers["Location"]


def test_login_disabled_with_weak_secret(monkeypatch):
    app = _make_app(monkeypatch)
    app.secret_key = "dev-secret"
    res = app.test_client().post("/api/admin/auth/telegram-login", json=_signed_payload())
    assert res.status_code == 503
    assert res.get_json()["code"] == "disabled"


def test_logout_clears_session(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    client = app.test_client()

    client.post("/api/admin/auth/telegram-login", json=_signed_payload())
    assert client.get("/api/admin/auth/me").status_code == 200

    res = client.post("/api/admin/auth/logout")
    assert res.status_code == 200
    assert client.get("/api/admin/auth/me").status_code == 401


def test_login_rate_limited(monkeypatch):
    app = _make_app(monkeypatch)
    client = app.test_client()
    for _ in range(admin_auth._LOGIN_RATE_MAX_ATTEMPTS):
        client.post("/api/admin/auth/telegram-login", json={"id": "1"})
    res = client.post("/api/admin/auth/telegram-login", json={"id": "1"})
    assert res.status_code == 429
    assert res.get_json()["code"] == "rate_limited"


def test_admin_page_serves_login_without_session(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setenv("BOT_USERNAME", "test_bot")
    res = app.test_client().get("/admin")
    assert res.status_code == 200
    assert res.headers["Cache-Control"].startswith("no-store")
    html = res.get_data(as_text=True)
    # Bot username injected into the JS variable (widget rendered dynamically)
    assert 'ADMIN_BOT_USERNAME = "test_bot"' in html
    # Correct widget script URL (NOT telegram-web-app.js / widget-login.js)
    assert "telegram-widget.js" in html
    # MiniApp SDK must not be loaded (comments mentioning it are fine)
    assert 'src="https://telegram.org/js/telegram-web-app.js"' not in html


def test_admin_page_serves_index_with_session(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    client = app.test_client()
    client.post("/api/admin/auth/telegram-login", json=_signed_payload())

    res = client.get("/admin")
    assert res.status_code == 200
    assert res.headers["Cache-Control"].startswith("no-store")
    # Served the shared miniapp/admin UI, not the login page
    assert "widget-login.js" not in res.get_data(as_text=True)


def test_session_cookie_flags(monkeypatch):
    app = _make_app(monkeypatch)
    app.config["SESSION_COOKIE_SECURE"] = True
    monkeypatch.setattr(admin_auth, "_is_whitelisted_admin", lambda user: (True, "cache"))
    client = app.test_client()

    res = client.post(
        "/api/admin/auth/telegram-login", json=_signed_payload(), base_url="https://localhost"
    )
    cookie = res.headers.get("Set-Cookie", "")
    assert "ap_admin_session=" in cookie
    assert "HttpOnly" in cookie
    assert "Secure" in cookie
    assert "SameSite=Lax" in cookie
