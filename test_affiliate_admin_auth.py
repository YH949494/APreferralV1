import json

from flask import Flask

import vouchers
from vouchers import vouchers_bp


class _FakeVoucherPools:
    def count_documents(self, _filt):
        return 0

    def find_one(self, _filt, _proj=None):
        return {}


class _FakeAdminCache:
    def __init__(self):
        self.ids = []

    def find_one(self, _filt):
        return {"_id": "admins", "ids": self.ids}


class _FakeDb:
    voucher_pools = _FakeVoucherPools()


def _fake_verify(init_data_raw):
    """Stand-in for Telegram initData HMAC verification.

    ``signed:<uid>`` verifies as user <uid>; anything else fails verification,
    the way a forged or tampered initData string would.
    """
    if init_data_raw.startswith("signed:"):
        uid = int(init_data_raw.split(":", 1)[1])
        return True, {"user": json.dumps({"id": uid})}, "ok"
    return False, {}, "bad_hash"


def _client(monkeypatch, admin_ids=None):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _FakeDb())
    monkeypatch.setattr(vouchers, "BYPASS_ADMIN", False)
    monkeypatch.setattr(vouchers, "verify_telegram_init_data", _fake_verify)
    admin_cache = _FakeAdminCache()
    admin_cache.ids = list(admin_ids or [])
    monkeypatch.setattr(vouchers, "admin_cache_col", admin_cache)
    return app.test_client()


# Admin identity comes ONLY from verified Telegram initData (or the admin
# secret / browser session). A bare ``user_id`` query parameter is caller-
# controlled and must never authenticate anyone.

def test_affiliate_admin_route_missing_user_id(monkeypatch):
    client = _client(monkeypatch)
    res = client.get("/v2/miniapp/admin/pools/summary")
    assert res.status_code == 400
    assert res.get_json().get("code") == "missing_init_data"


def test_affiliate_admin_route_forbidden_user_id(monkeypatch):
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get("/v2/miniapp/admin/pools/summary?init_data=signed:67890")
    assert res.status_code == 403
    assert res.get_json().get("code") == "forbidden"


def test_affiliate_admin_route_allows_admin_cache_user_id(monkeypatch):
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get("/v2/miniapp/admin/pools/summary?init_data=signed:12345")
    assert res.status_code == 200
    assert res.get_json().get("status") == "ok"


def test_affiliate_admin_route_rejects_unsigned_admin_user_id(monkeypatch):
    """Regression: an admin's id in the query string, with no initData, is
    not authentication (the legacy tests assumed it was)."""
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get("/v2/miniapp/admin/pools/summary?user_id=12345")
    assert res.status_code == 400
    assert res.get_json().get("code") == "missing_init_data"


def test_affiliate_admin_route_rejects_unverified_init_data_for_admin_id(monkeypatch):
    """Regression: initData that fails signature verification is rejected even
    when the claimed user/user_id is a cached admin."""
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get("/v2/miniapp/admin/pools/summary?init_data=forged:12345&user_id=12345")
    assert res.status_code == 403
    assert res.get_json().get("code") == "forbidden"
