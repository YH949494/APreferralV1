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


def _client(monkeypatch, admin_ids=None):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _FakeDb())
    admin_cache = _FakeAdminCache()
    admin_cache.ids = list(admin_ids or [])
    monkeypatch.setattr(vouchers, "admin_cache_col", admin_cache)
    return app.test_client()


def test_affiliate_admin_route_missing_user_id(monkeypatch):
    client = _client(monkeypatch)
    res = client.get("/v2/miniapp/admin/pools/summary")
    assert res.status_code in (400, 401)
    assert res.get_json().get("code") in ("missing_user_id", "auth_failed")


def test_affiliate_admin_route_forbidden_user_id(monkeypatch):
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get("/v2/miniapp/admin/pools/summary?user_id=67890")
    assert res.status_code in (401, 403)
    assert res.get_json().get("code") in ("forbidden", "auth_failed")


def test_affiliate_admin_route_allows_admin_cache_user_id(monkeypatch):
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get("/v2/miniapp/admin/pools/summary?user_id=12345")
    assert res.status_code == 200
    assert res.get_json().get("status") == "ok"
