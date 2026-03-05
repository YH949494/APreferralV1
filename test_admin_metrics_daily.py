from datetime import datetime, timedelta, timezone

from flask import Flask

import vouchers
from vouchers import vouchers_bp


class _FakeCountCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def _match_value(self, value, cond):
        if isinstance(cond, dict):
            for op, expected in cond.items():
                if op == "$gte" and not (value is not None and value >= expected):
                    return False
                if op == "$lt" and not (value is not None and value < expected):
                    return False
                if op == "$exists":
                    exists = value is not None
                    if bool(expected) != exists:
                        return False
                if op == "$nin" and value in expected:
                    return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for key, cond in (filt or {}).items():
            if not self._match_value(doc.get(key), cond):
                return False
        return True

    def count_documents(self, filt):
        return sum(1 for d in self.docs if self._match(d, filt))


class _FakeAdminCache:
    def __init__(self):
        self.ids = []

    def find_one(self, _filt):
        return {"_id": "admins", "ids": self.ids}


class _FakeDb:
    def __init__(self, voucher_pool_docs=None):
        self.voucher_ledger = _FakeCountCollection()
        self.qualified_events = _FakeCountCollection()
        self.affiliate_ledger = _FakeCountCollection()
        self.voucher_pools = _FakeCountCollection(voucher_pool_docs)


def _client(monkeypatch, admin_ids=None, voucher_pool_docs=None):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")

    monkeypatch.setattr(vouchers, "db", _FakeDb(voucher_pool_docs=voucher_pool_docs))
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


def test_admin_metrics_daily_affiliate_rewards_uses_voucher_pool_issued_at(monkeypatch):
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    inside_1 = day_start + timedelta(hours=1)
    inside_2 = day_start + timedelta(hours=5)
    outside = day_start - timedelta(hours=2)

    client = _client(
        monkeypatch,
        admin_ids=[12345],
        voucher_pool_docs=[
            {"pool_id": "T1", "code": "A", "status": "issued", "issued_at": inside_1, "issued_for_ledger_id": "l1"},
            {"pool_id": "T1", "code": "B", "status": "issued", "issued_at": inside_2, "issued_for_ledger_id": "l2"},
            {"pool_id": "T1", "code": "C", "status": "issued", "issued_at": outside, "issued_for_ledger_id": "l3"},
            {"pool_id": "T1", "code": "D", "status": "issued", "issued_at": inside_1, "issued_for_ledger_id": None},
        ],
    )

    res = client.get("/v2/miniapp/admin/metrics/daily?user_id=12345&days=1")
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["rows"][0]["affiliate_rewards_issued"] == 2


def test_admin_metrics_daily_qualified_events_uses_qualified_at(monkeypatch):
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    inside = day_start + timedelta(hours=2)
    outside = day_start - timedelta(hours=1)

    class _DbWithQualified(_FakeDb):
        def __init__(self):
            super().__init__()
            self.qualified_events = _FakeCountCollection([
                {"qualified_at": inside, "created_at": outside},
                {"qualified_at": outside, "created_at": inside},
            ])

    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _DbWithQualified())
    monkeypatch.setattr(vouchers, "users_collection", _FakeCountCollection())
    monkeypatch.setattr(vouchers, "miniapp_sessions_daily_col", _FakeCountCollection())
    admin_cache = _FakeAdminCache()
    admin_cache.ids = [12345]
    monkeypatch.setattr(vouchers, "admin_cache_col", admin_cache)

    res = app.test_client().get("/v2/miniapp/admin/metrics/daily?user_id=12345&days=1")
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["rows"][0]["qualified_events"] == 1
