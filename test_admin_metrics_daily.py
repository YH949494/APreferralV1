from flask import Flask

import vouchers
from vouchers import vouchers_bp


class _FakeCountCollection:
    def count_documents(self, _filt):
        return 0


class _FakeAdminCache:
    def __init__(self):
        self.ids = []

    def find_one(self, _filt):
        return {"_id": "admins", "ids": self.ids}


class _FakeDb:
    voucher_ledger = _FakeCountCollection()
    qualified_events = _FakeCountCollection()
    affiliate_ledger = _FakeCountCollection()


def _client(monkeypatch, admin_ids=None):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")

    monkeypatch.setattr(vouchers, "db", _FakeDb())
    monkeypatch.setattr(vouchers, "users_collection", _FakeCountCollection())
    monkeypatch.setattr(vouchers, "miniapp_sessions_daily_col", _FakeCountCollection())

    admin_cache = _FakeAdminCache()
    admin_cache.ids = list(admin_ids or [])
    monkeypatch.setattr(vouchers, "admin_cache_col", admin_cache)

    return app.test_client()


def test_admin_metrics_daily_requires_admin(monkeypatch):
    client = _client(monkeypatch)
    res = client.get("/v2/miniapp/admin/metrics/daily")
    assert res.status_code in (400, 401)


def test_admin_metrics_daily_returns_rows_and_keys(monkeypatch):
    days = 5
    client = _client(monkeypatch, admin_ids=[12345])
    res = client.get(f"/v2/miniapp/admin/metrics/daily?user_id=12345&days={days}")

    assert res.status_code == 200
    payload = res.get_json()
    assert payload.get("days") == days
    assert payload.get("tz") == "UTC"
    assert len(payload.get("rows") or []) == days

    row = payload["rows"][0]
    assert set(row.keys()) == {
        "date",
        "miniapp_unique_users",
        "voucher_claims_issued",
        "first_checkins",
        "qualified_events",
        "affiliate_rewards_issued",
    }
