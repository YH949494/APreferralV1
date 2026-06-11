"""Telegram Login Widget authentication for browser access to the admin panel.

Phase A scope: login, session creation, and introspection only. No existing
admin guard accepts these sessions yet — admin APIs continue to require the
admin secret or Telegram MiniApp initData exactly as before.

Routes:
  GET  /admin                          login page, or the existing admin UI
                                       (static/index.html) once a session exists
  GET/POST /api/admin/auth/telegram-login  Login Widget callback
  POST /api/admin/auth/logout
  GET  /api/admin/auth/me
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timezone

from flask import (
    Blueprint,
    current_app,
    jsonify,
    make_response,
    redirect,
    request,
    send_from_directory,
    session,
)

logger = logging.getLogger(__name__)

admin_auth_bp = Blueprint("admin_auth", __name__)

ADMIN_WEB_LOGIN_ENABLED = os.getenv("ADMIN_WEB_LOGIN_ENABLED", "1") == "1"
ADMIN_SESSION_TTL_S = int(os.getenv("ADMIN_SESSION_TTL_S", "43200"))  # 12h
ADMIN_LOGIN_MAX_AGE_S = int(os.getenv("ADMIN_LOGIN_MAX_AGE_S", "300"))

# Sessions signed with a guessable key are forgeable, so login stays disabled
# until FLASK_SECRET_KEY is set to a real value.
_WEAK_SECRETS = {None, "", "dev-secret"}

_LOGIN_RATE_WINDOW_S = 60
_LOGIN_RATE_MAX_ATTEMPTS = 10
_login_attempts: dict[str, list[float]] = {}  # per-worker best effort


def configure_admin_session(app) -> None:
    """Harden Flask's signed-cookie session (unused elsewhere in this app)."""
    app.config.update(
        SESSION_COOKIE_NAME="ap_admin_session",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=os.getenv("FLASK_ENV") != "development",
        SESSION_COOKIE_SAMESITE="Lax",
        PERMANENT_SESSION_LIFETIME=ADMIN_SESSION_TTL_S,
    )
    if (app.secret_key or "") in _WEAK_SECRETS:
        logger.error(
            "[admin_login] FLASK_SECRET_KEY missing or weak — admin web login disabled"
        )


def verify_login_widget_payload(
    data: dict, bot_token: str, *, max_age_s: int | None = None, now: float | None = None
):
    """Verify a Telegram Login Widget payload.

    The Login Widget HMAC key is SHA256(bot_token) — a different derivation
    from MiniApp initData, which uses HMAC("WebAppData", bot_token).
    Returns (ok, user, reason).
    """
    max_age_s = ADMIN_LOGIN_MAX_AGE_S if max_age_s is None else max_age_s
    now = time.time() if now is None else now

    if not bot_token:
        return False, {}, "missing_bot_token"

    fields = {k: str(v) for k, v in (data or {}).items() if k != "hash" and v is not None}
    provided_hash = str((data or {}).get("hash") or "").lower()
    if len(provided_hash) != 64:
        return False, {}, "missing_hash"

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret = hashlib.sha256(bot_token.encode()).digest()
    computed = hmac.new(secret, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, provided_hash):
        return False, {}, "invalid_hash"

    try:
        auth_date = int(fields.get("auth_date", "0"))
    except (TypeError, ValueError):
        auth_date = 0
    if auth_date <= 0 or (now - auth_date) > max_age_s:
        return False, {}, "expired"

    try:
        user_id = int(fields.get("id"))
    except (TypeError, ValueError):
        return False, {}, "missing_user_id"

    user = {
        "id": user_id,
        "first_name": fields.get("first_name", ""),
        "last_name": fields.get("last_name", ""),
        "username": fields.get("username", ""),
        "photo_url": fields.get("photo_url", ""),
        "auth_date": auth_date,
    }
    return True, user, "ok"


def _is_whitelisted_admin(user: dict):
    """Authorize against the existing miniapp admin model (admin_cache + allowlist).

    Same whitelist the miniapp uses, so browser and Telegram can never disagree
    about who is an admin. Returns (is_admin, source) like _is_cached_admin.
    """
    try:
        from vouchers import _is_cached_admin  # lazy: keeps this module import-light

        return _is_cached_admin({"id": user.get("id"), "username": user.get("username", "")})
    except Exception:
        logger.exception("[admin_login] whitelist check failed")
        return False, None


def _secret_ok() -> bool:
    try:
        return (current_app.secret_key or "") not in _WEAK_SECRETS
    except Exception:
        return False


def session_admin():
    """Return {id, username, via, iat} for a valid admin session, else None."""
    if not _secret_ok():
        return None
    try:
        tg_id = session.get("admin_tg_id")
        if not tg_id:
            return None
        iat = int(session.get("admin_iat") or 0)
        if iat <= 0 or (time.time() - iat) > ADMIN_SESSION_TTL_S:
            return None
        return {
            "id": int(tg_id),
            "username": session.get("admin_username", ""),
            "via": session.get("admin_via", "widget"),
            "iat": iat,
        }
    except Exception:
        return None


def _client_ip() -> str:
    return request.headers.get("Fly-Client-IP") or request.remote_addr or "unknown"


def _login_rate_limited(ip: str) -> bool:
    now = time.time()
    attempts = [t for t in _login_attempts.get(ip, []) if now - t < _LOGIN_RATE_WINDOW_S]
    limited = len(attempts) >= _LOGIN_RATE_MAX_ATTEMPTS
    attempts.append(now)
    _login_attempts[ip] = attempts
    if len(_login_attempts) > 10000:
        _login_attempts.clear()
    return limited


def _audit_sink(doc: dict) -> None:
    try:
        from database import db

        db["admin_login_audit"].insert_one(doc)
    except Exception:
        pass  # audit storage is best-effort; the log line above is authoritative


def _audit(event: str, *, user_id=None, username: str = "", reason: str = "") -> None:
    ip = _client_ip()
    logger.info(
        "[admin_login] event=%s uid=%s username=%s reason=%s ip=%s",
        event,
        user_id,
        username,
        reason,
        ip,
    )
    _audit_sink(
        {
            "event": event,
            "user_id": user_id,
            "username": username,
            "reason": reason,
            "ip": ip,
            "user_agent": request.headers.get("User-Agent", "")[:300],
            "at": datetime.now(timezone.utc),
        }
    )


def _no_store(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def _login_page(notice: str = ""):
    bot_username = (os.environ.get("BOT_USERNAME") or "").strip()
    if not bot_username and not notice:
        notice = "BOT_USERNAME is not configured on the server."
    path = os.path.join(current_app.static_folder or "static", "admin-login.html")
    with open(path, encoding="utf-8") as f:
        html = f.read()
    html = html.replace("__BOT_USERNAME__", bot_username).replace("__SERVER_NOTICE__", notice)
    resp = make_response(html)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    return _no_store(resp)


@admin_auth_bp.get("/admin")
def admin_panel():
    admin = session_admin()
    if admin:
        # Same UI as /miniapp — one admin frontend for both entry points.
        resp = make_response(
            send_from_directory(current_app.static_folder or "static", "index.html")
        )
        return _no_store(resp)
    if not ADMIN_WEB_LOGIN_ENABLED:
        return _login_page(notice="Admin web login is currently disabled.")
    if not _secret_ok():
        return _login_page(notice="Admin web login is not configured on the server.")
    return _login_page()


@admin_auth_bp.route("/api/admin/auth/telegram-login", methods=["GET", "POST"])
def telegram_login():
    # GET = the widget's data-auth-url redirect mode; POST = JSON onauth mode.
    redirect_mode = request.method == "GET"

    def fail(code: str, http_status: int):
        if redirect_mode:
            return redirect(f"/admin?login_error={code}")
        return jsonify({"success": False, "code": code}), http_status

    if not ADMIN_WEB_LOGIN_ENABLED or not _secret_ok():
        _audit("login_disabled", reason="disabled_or_weak_secret")
        return fail("disabled", 503)

    if _login_rate_limited(_client_ip()):
        _audit("login_rate_limited")
        return fail("rate_limited", 429)

    payload = request.args.to_dict() if redirect_mode else (request.get_json(silent=True) or {})

    ok, user, reason = verify_login_widget_payload(payload, os.environ.get("BOT_TOKEN", ""))
    if not ok:
        _audit("login_rejected", reason=reason)
        return fail(reason, 401)

    is_admin, source = _is_whitelisted_admin(user)
    if not is_admin:
        _audit("login_denied", user_id=user["id"], username=user["username"], reason="not_admin")
        return fail("not_admin", 403)

    session.clear()
    session.permanent = True
    session["admin_tg_id"] = user["id"]
    session["admin_username"] = user.get("username", "")
    session["admin_via"] = "widget"
    session["admin_iat"] = int(time.time())

    _audit("login_ok", user_id=user["id"], username=user["username"], reason=source or "cache")

    if redirect_mode:
        return redirect("/admin")
    return jsonify(
        {
            "success": True,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "first_name": user["first_name"],
            },
        }
    )


@admin_auth_bp.post("/api/admin/auth/logout")
def admin_logout():
    admin = session_admin()
    if admin:
        _audit("logout", user_id=admin["id"], username=admin["username"])
    session.clear()
    return jsonify({"success": True})


@admin_auth_bp.get("/api/admin/auth/me")
def admin_me():
    admin = session_admin()
    if not admin:
        return jsonify({"success": False, "code": "no_session"}), 401
    expires_at = datetime.fromtimestamp(admin["iat"] + ADMIN_SESSION_TTL_S, tz=timezone.utc)
    return jsonify(
        {
            "success": True,
            "admin": {
                "id": admin["id"],
                "username": admin["username"],
                "via": admin["via"],
                "expires_at": expires_at.isoformat(),
            },
        }
    )
