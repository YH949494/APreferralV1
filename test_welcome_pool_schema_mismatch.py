"""Regression tests for _pooled_claimability_state against the exact
db.vouchers filter shape used at claim time
(_build_atomic_pooled_voucher_filter). These exercise the real counting
query (not a mocked reason), so a schema mismatch between how codes are
uploaded and how they are counted/allocated shows up as a test failure
instead of only in production.

Note: the Welcome Voucher Progress card no longer allocates from db.drops/
db.vouchers via this function (see build_welcome_progress_response /
_issue_or_get_welcome_voucher, which now issue from voucher_pools
pool_id="WELCOME"). _pooled_claimability_state is still the allocator for
other (non-Welcome) pooled/public voucher drops, so this coverage remains
relevant there.
"""
from datetime import datetime, timedelta, timezone

import vouchers as m

DROP_ID = "drop-welcome-1"
NOW = datetime(2026, 8, 1, tzinfo=timezone.utc)


def _drop(**overrides):
    drop = {
        "_id": DROP_ID,
        "type": "pooled",
        "audience": {"type": "new_joiner"},
        "startsAt": NOW - timedelta(days=1),
        "endsAt": NOW + timedelta(days=30),
        "status": "active",
    }
    drop.update(overrides)
    return drop


class FakeVouchers:
    """Minimal count_documents supporting the exact filter shapes used by
    _pooled_claimability_state / _build_atomic_pooled_voucher_filter."""

    def __init__(self, rows):
        self.rows = rows

    def count_documents(self, query):
        return sum(1 for row in self.rows if self._matches(row, query))

    @staticmethod
    def _matches(row, query):
        for key, cond in query.items():
            if key == "$or":
                if not any(FakeVouchers._matches(row, sub) for sub in cond):
                    return False
                continue
            if isinstance(cond, dict) and "$in" in cond:
                if row.get(key) not in cond["$in"]:
                    return False
                continue
            if isinstance(cond, dict) and "$exists" in cond:
                has_key = key in row
                if has_key != cond["$exists"]:
                    return False
                continue
            if row.get(key) != cond:
                return False
        return True


class FakeDb:
    def __init__(self, rows):
        self.vouchers = FakeVouchers(rows)


def _claimable(monkeypatch, rows, *, uid=1, user_region=None):
    monkeypatch.setattr(m, "db", FakeDb(rows))
    monkeypatch.setattr(m, "is_retained_3d", lambda user: False)
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: True)
    return m._pooled_claimability_state(
        drop=_drop(), drop_id=DROP_ID, user_region=user_region, uid=uid,
        is_my_user=False, ref=NOW,
    )


def test_matching_free_pooled_code_is_claimable(monkeypatch):
    rows = [{"type": "pooled", "dropId": DROP_ID, "status": "free", "code": "W1"}]
    state = _claimable(monkeypatch, rows)
    assert state["claimable"] is True
    assert state["reason"] == "ok"


def test_zero_real_stock_reports_pool_empty_not_ok(monkeypatch):
    rows = [{"type": "pooled", "dropId": DROP_ID, "status": "claimed", "code": "W1"}]
    state = _claimable(monkeypatch, rows)
    assert state["claimable"] is False
    assert state["reason"] == "pool_empty"


def test_status_case_mismatch_is_treated_as_no_stock(monkeypatch):
    """Codes uploaded/updated with status="AVAILABLE"/"Free" instead of the
    lowercase "free" the allocator filters on are invisible to the count —
    this must fail loudly (assertion) rather than silently pass, so a schema
    drift shows up here instead of only as a live NO_FREE_CODES incident."""
    rows = [{"type": "pooled", "dropId": DROP_ID, "status": "AVAILABLE", "code": "W1"}]
    state = _claimable(monkeypatch, rows)
    assert state["claimable"] is False
    assert state["reason"] == "pool_empty"


def test_dropid_type_mismatch_is_treated_as_no_stock(monkeypatch):
    """A code row referencing a stale/differently-typed dropId (e.g. leftover
    from a recreated drop) must not be counted against the currently active
    drop's variants."""
    rows = [{"type": "pooled", "dropId": "some-other-drop", "status": "free", "code": "W1"}]
    state = _claimable(monkeypatch, rows)
    assert state["claimable"] is False
    assert state["reason"] == "pool_empty"


def test_pool_field_mismatch_excludes_public_count(monkeypatch):
    """A code tagged pool="WELCOME" (neither "public" nor absent nor "my")
    never matches the public-pool $or clause and is invisible to claimants,
    even though the row physically exists — this is the schema-mismatch
    class explicitly called out in the Welcome Voucher audit."""
    rows = [{"type": "pooled", "dropId": DROP_ID, "status": "free", "code": "W1", "pool": "WELCOME"}]
    state = _claimable(monkeypatch, rows)
    assert state["claimable"] is False
    assert state["reason"] == "pool_empty"


def test_public_pooled_drop_with_stale_dropid_inventory_is_sold_out(monkeypatch):
    """A public/pooled drop (non-Welcome) whose inventory rows reference a
    stale/different dropId — e.g. left over from a recreated drop — must be
    reported sold out for the currently active drop, not silently claimable."""
    rows = [{"type": "pooled", "dropId": "stale-drop-id", "status": "free", "code": "W1"}]
    state = _claimable(monkeypatch, rows, user_region="my")
    assert state["claimable"] is False
    assert state["reason"] == "pool_empty"
