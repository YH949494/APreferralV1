"""Tests for miniapp_identity.py: the shared authenticated-Mini-App-user
resolver. Still verifies Telegram initData under the hood — this only
centralizes where that logic lives, it does not weaken the guarantee that
identity is always verified server-side."""

from unittest.mock import patch

from flask import Flask

from miniapp_identity import resolve_authenticated_telegram_user_id


def _app():
    return Flask(__name__)


def test_resolves_uid_from_verified_init_data():
    app = _app()
    with app.test_request_context("/x?init_data=raw"):
        with patch("vouchers.verify_telegram_init_data", return_value=(True, {"user": '{"id": 111}'}, "ok")):
            uid, err = resolve_authenticated_telegram_user_id()
    assert uid == 111
    assert err is None


def test_rejects_missing_init_data():
    app = _app()
    with app.test_request_context("/x"):
        uid, err = resolve_authenticated_telegram_user_id()
    assert uid is None
    assert err[1] == 401


def test_rejects_invalid_init_data():
    app = _app()
    with app.test_request_context("/x?init_data=bad"):
        with patch("vouchers.verify_telegram_init_data", return_value=(False, {}, "hash_mismatch")):
            uid, err = resolve_authenticated_telegram_user_id()
    assert uid is None
    assert err[1] == 401


def test_never_trusts_raw_uid_query_param():
    app = _app()
    with app.test_request_context("/x?uid=999&user_id=999"):
        uid, err = resolve_authenticated_telegram_user_id()
    assert uid is None  # no init_data present, raw uid/user_id params are ignored
    assert err[1] == 401
