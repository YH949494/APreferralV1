import hashlib
import hmac
import importlib
import json
import logging
import os
import sys
import time
import types

import pytest


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


def test_initdata_invalid_hash_logs_failure_reason(vouchers_module, monkeypatch, caplog):
    monkeypatch.delenv("DEBUG_INITDATA", raising=False)
    monkeypatch.setenv("BOT_TOKEN", "123:ABC")
    vouchers = importlib.reload(vouchers_module)

    payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": 42})}
    init_data = build_init_data("123:ABC", payload)[:-1] + "0"

    with caplog.at_level(logging.WARNING):
        ok, _, _ = vouchers.verify_telegram_init_data(init_data)

    assert ok is False
    logs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "reason=bad_hash" in logs
