import hashlib
import hmac
import importlib
import json
import logging
import os
import sys
import time
import types
from unittest.mock import patch

import pytest
from flask import Flask


def build_init_data(token: str, payload: dict) -> str:
    pairs = []
    for key in sorted(payload.keys()):
        pairs.append(f"{key}={payload[key]}")
    check_string = "\n".join(pairs)
    secret_key = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    signature = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
    query = payload.copy()
    query["hash"] = signature
    return "&".join(f"{k}={query[k]}" for k in query)


@pytest.fixture
def vouchers_module(monkeypatch):
    class _FakeCollection:
        def update_one(self, *args, **kwargs):
            return None

        def __getattr__(self, _):
            return lambda *args, **kwargs: None

    class _FakeDB:
        def __getitem__(self, _):
            return _FakeCollection()

        def __getattr__(self, _):
            return _FakeCollection()

    fake_database_module = types.ModuleType("database")
    fake_database_module.db = _FakeDB()
    fake_database_module.users_collection = _FakeCollection()
    fake_database_module.get_collection = lambda name: _FakeCollection()
    fake_database_module._ensure_equivalent_index = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "database", fake_database_module)

    if "vouchers" in sys.modules:
        mod = importlib.reload(sys.modules["vouchers"])
    else:
        mod = importlib.import_module("vouchers")
    return mod


def test_initdata_success_no_verbose_logs_by_default(vouchers_module, monkeypatch, caplog):
    monkeypatch.delenv("DEBUG_INITDATA", raising=False)
    monkeypatch.setenv("BOT_TOKEN", "123:ABC")
    vouchers = importlib.reload(vouchers_module)

    payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": 42})}
    init_data = build_init_data("123:ABC", payload)

    with caplog.at_level(logging.INFO):
        ok, _, _ = vouchers.verify_telegram_init_data(init_data)

    assert ok is True
    logs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "[initdata] raw_len=" not in logs
    assert "[initdata] decoded_len=" not in logs
    assert "[initdata] hash_check" not in logs
    # Routine successful verification must not appear at INFO (or above) level.
    assert "[initdata] verify_ok" not in logs


def test_initdata_success_verify_ok_logs_at_debug(vouchers_module, monkeypatch, caplog):
    monkeypatch.delenv("DEBUG_INITDATA", raising=False)
    monkeypatch.setenv("BOT_TOKEN", "123:ABC")
    vouchers = importlib.reload(vouchers_module)

    payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": 42})}
    init_data = build_init_data("123:ABC", payload)

    with caplog.at_level(logging.DEBUG):
        ok, _, _ = vouchers.verify_telegram_init_data(init_data)

    assert ok is True
    debug_records = [rec for rec in caplog.records if rec.levelno == logging.DEBUG]
    debug_logs = "\n".join(rec.getMessage() for rec in debug_records)
    assert "[initdata] verify_ok" in debug_logs
    # Never leak the raw init_data, signature, or hash in the success log line.
    assert init_data not in debug_logs
    assert payload["user"] not in debug_logs


def test_initdata_success_verbose_logs_when_debug_enabled(vouchers_module, monkeypatch, caplog):
    monkeypatch.setenv("DEBUG_INITDATA", "1")
    monkeypatch.setenv("BOT_TOKEN", "123:ABC")
    vouchers = importlib.reload(vouchers_module)

    payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": 42})}
    init_data = build_init_data("123:ABC", payload)

    with caplog.at_level(logging.INFO):
        ok, _, _ = vouchers.verify_telegram_init_data(init_data)

    assert ok is True
    logs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "[initdata] raw_len=" in logs
    assert "[initdata] decoded_len=" in logs
    assert "[initdata] hash_check" in logs


def test_user_ctx_or_preview_reuses_precomputed_verification(vouchers_module, monkeypatch):
    """A route that already verified init_data and passes the result through
    must not trigger a second verify_telegram_init_data call (and therefore a
    second round of verification logging) inside _user_ctx_or_preview."""
    monkeypatch.delenv("DEBUG_INITDATA", raising=False)
    monkeypatch.setenv("BOT_TOKEN", "123:ABC")
    vouchers = importlib.reload(vouchers_module)

    payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": 42})}
    init_data = build_init_data("123:ABC", payload)

    app = Flask(__name__)
    verification = vouchers.verify_telegram_init_data(init_data)
    assert verification[0] is True

    with patch.object(vouchers, "verify_telegram_init_data") as mock_verify:
        with app.test_request_context(f"/vouchers/visible?init_data={init_data}"):
            ctx, admin_preview = vouchers._user_ctx_or_preview(
                vouchers.request, init_data_raw=init_data, verification=verification
            )

    mock_verify.assert_not_called()
    assert admin_preview is False
    assert ctx == verification[1]


def test_initdata_invalid_hash_logs_failure_reason(vouchers_module, monkeypatch, caplog):
    monkeypatch.delenv("DEBUG_INITDATA", raising=False)
    monkeypatch.setenv("BOT_TOKEN", "123:ABC")
    vouchers = importlib.reload(vouchers_module)

    payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": 42})}
    valid_init_data = build_init_data("123:ABC", payload)
    # Flip the final hex digit of the hash so the signature is always wrong.
    # Replacing it with a fixed "0" would be a no-op whenever the correct hash
    # already ends in "0", making the test pass only ~15/16 of the time.
    init_data = valid_init_data[:-1] + ("1" if valid_init_data[-1] == "0" else "0")

    with caplog.at_level(logging.WARNING):
        ok, _, _ = vouchers.verify_telegram_init_data(init_data)

    assert ok is False
    logs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "reason=bad_hash" in logs
