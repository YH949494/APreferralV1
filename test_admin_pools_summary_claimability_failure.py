"""When affiliate_rewards.get_claimable_pool_inventory raises inside
admin_pools_summary_v2 (vouchers.py), the endpoint must never fall back to
raw_available as if it were claimable — that would recreate the exact bug
this endpoint exists to fix (showing a pool as available/Healthy when
whether issuance can actually claim from it is unknown).
"""

from flask import Flask

import affiliate_rewards
import vouchers
from vouchers import vouchers_bp


class _FakeVoucherPools:
    def __init__(self, available_by_pool, issued_by_pool=None):
        self._available = dict(available_by_pool or {})
        self._issued = dict(issued_by_pool or {})

    def count_documents(self, filt):
        pool_id = filt.get("pool_id")
        status = filt.get("status")
        if status == "available":
            return int(self._available.get(pool_id, 0))
        if status == "issued":
            return int(self._issued.get(pool_id, 0))
        return 0

    def find_one(self, _filt, _proj=None):
        return {}


class _FakeDb:
    def __init__(self, voucher_pools):
        self.voucher_pools = voucher_pools


def _client(monkeypatch, voucher_pools):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _FakeDb(voucher_pools))
    # Bypass admin auth entirely — the auth path itself is exercised by
    # test_affiliate_admin_auth.py; this test is only about the
    # claimability-check-failure fallback behavior.
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "admin"}, None))
    return app.test_client()


def test_helper_raises_never_reports_healthy_or_claimable(monkeypatch):
    # T3 has plenty of raw stock — exactly the scenario that must NOT be
    # reported as claimable/Healthy when the claimability check itself
    # fails.
    pools = _FakeVoucherPools(available_by_pool={"T3": 95}, issued_by_pool={"T3": 23})
    client = _client(monkeypatch, pools)

    def _raise(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(affiliate_rewards, "get_claimable_pool_inventory", _raise)

    res = client.get("/v2/miniapp/admin/pools/summary")
    assert res.status_code == 200
    body = res.get_json()
    t3 = next(item for item in body["items"] if item["pool_id"] == "T3")

    # Never a number implying the bot can issue right now.
    assert t3["claimable_available"] is None
    assert t3["blocking_reason"] == "claimability_check_failed"
    # raw_available is preserved purely as diagnostic information.
    assert t3["raw_available"] == 95
    assert t3["issued"] == 23
