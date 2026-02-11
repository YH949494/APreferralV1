from flask import Flask

import vouchers
from vouchers import vouchers_bp


class _FakeVoucherPools:
    def count_documents(self, _filt):
        return 0

    def find_one(self, _filt, _proj=None):
        return {}


class _FakeDb:
    voucher_pools = _FakeVoucherPools()



def _client(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _FakeDb())
    monkeypatch.setattr(vouchers, "_ADMIN_PANEL_SECRET", "shared-secret")
    return app.test_client()


def test_affiliate_admin_route_requires_same_gate(monkeypatch):
    client = _client(monkeypatch)
    res = client.get("/v2/miniapp/admin/pools/summary")
    assert res.status_code == 401
    assert res.get_json().get("code") == "auth_failed"


def test_affiliate_admin_route_accepts_x_admin_secret(monkeypatch):
    client = _client(monkeypatch)
    res = client.get(
        "/v2/miniapp/admin/pools/summary",
        headers={"X-Admin-Secret": "shared-secret"},
    )
    assert res.status_code == 200
    assert res.get_json().get("status") == "ok"


def test_affiliate_admin_route_accepts_bearer_secret(monkeypatch):
    client = _client(monkeypatch)
    res = client.get(
        "/v2/miniapp/admin/pools/summary",
        headers={"Authorization": "Bearer shared-secret"},
    )
    assert res.status_code == 200
    assert res.get_json().get("status") == "ok"
