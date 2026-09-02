"""The surplus sweep must be bounded at the DATABASE and must eventually
inspect every eligible ledger.

`list(find(query))[:batch_limit]` was neither: it pulled the whole eligible
set into memory, and with no ordering it re-inspected the same first N
ledgers on every five-minute run while the rest were never looked at once.
"""
from __future__ import annotations

import collections
from datetime import datetime, timedelta, timezone

import pytest

import affiliate_rewards as ar
import affiliate_reward_plans as arp
from fake_mongo import FakeDb

SEP = datetime(2026, 9, 10, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}


class _CursorSpy:
    """Wraps FakeDb and records exactly how affiliate_ledger.find was called
    and how many documents it returned."""

    def __init__(self, inner):
        self._inner = inner
        self.find_calls = []
        self.returned_counts = []

    def __getattr__(self, name):
        col = getattr(self._inner, name)
        if name != "affiliate_ledger":
            return col
        spy = self

        class _Col:
            def find(self, query=None, **kwargs):
                rows = col.find(query, **kwargs)
                spy.find_calls.append({"query": query, "kwargs": kwargs})
                spy.returned_counts.append(len(rows))
                return rows

            def __getattr__(self, item):
                return getattr(col, item)

        return _Col()

    def __getitem__(self, name):
        return getattr(self, name)


def _seed(db, count, *, month="202609"):
    """`count` ISSUED denomination ledgers, all clean."""
    for i in range(count):
        recipe = arp.tier_recipe(month, "T1")
        db.affiliate_ledger.insert_one({
            "_id": f"L{i:04d}",
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": 1000 + i,
            "status": "ISSUED",
            "tier": "T1",
            "year_month": month,
            "entitlement_month": month,
            "reward_plan": arp.DENOMINATION_PLAN_ID,
            "bundle_recipe": recipe,
            "dedup_key": f"AFF:{1000+i}:{month}:T1",
            "voucher_code": f"CODE{i:04d}",
            "vouchers": [{"code": f"CODE{i:04d}", "value": 10, "pool_id": "AFFILIATE_10"}],
            "updated_at": SEP,
        })
        db.voucher_pools.insert_one({
            "pool_id": "AFFILIATE_10", "code": f"CODE{i:04d}", "status": "issued",
            "voucher_value": 10, "issued_to_user_id": 1000 + i,
            "ledger_id": f"L{i:04d}", "issued_for_ledger_id": f"L{i:04d}",
        })


def _add_surplus(db, ledger_id, code):
    """An extra code linked to the ledger beyond its recipe, stamped with the
    ledger's OWN user — a row belonging to a different user is `foreign`, not
    surplus, and is deliberately never released."""
    owner = db.affiliate_ledger.find_one({"_id": ledger_id})
    db.voucher_pools.insert_one({
        "pool_id": "AFFILIATE_10", "code": code, "status": "issued",
        "voucher_value": 10, "issued_to_user_id": owner["user_id"],
        "ledger_id": ledger_id, "issued_for_ledger_id": ledger_id,
    })


class TestBoundedAtTheDatabase:
    def test_cursor_is_limited_and_sorted_server_side(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        spy = _CursorSpy(db)
        ar.reconcile_surplus_denomination_allocations(spy, now_utc=SEP, batch_limit=5)

        assert spy.find_calls, "the sweep never queried affiliate_ledger"
        call = spy.find_calls[0]
        assert call["kwargs"].get("limit") == 5, (
            f"the ledger query must carry limit=5 server-side; got {call['kwargs']}"
        )
        assert call["kwargs"].get("sort") == [("surplus_checked_at", 1), ("_id", 1)], (
            f"the ledger query must be sorted for a deterministic, "
            f"starvation-free scan; got {call['kwargs'].get('sort')}"
        )

    def test_never_materializes_more_than_the_batch_limit(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        spy = _CursorSpy(db)
        ar.reconcile_surplus_denomination_allocations(spy, now_utc=SEP, batch_limit=5)
        assert spy.returned_counts[0] <= 5, (
            f"the sweep pulled {spy.returned_counts[0]} ledgers into memory for a "
            "batch_limit of 5"
        )

    def test_one_run_scans_at_most_the_batch_limit(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)
        assert stats["scanned"] == 5


class TestStarvationFreeProgression:
    def test_every_ledger_is_eventually_inspected(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        # 60 ledgers / 5 per run = 12 runs to cover them all.
        for _ in range(12):
            ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)

        unchecked = [
            d["_id"] for d in db.affiliate_ledger.find({})
            if d.get("surplus_checked_at") is None
        ]
        assert unchecked == [], (
            f"{len(unchecked)} ledgers were never inspected after 12 runs: "
            f"{unchecked[:10]}"
        )

    def test_runs_do_not_re_inspect_the_same_first_n_forever(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        seen = []
        for _ in range(4):
            before = {d["_id"]: d.get("surplus_checked_at") for d in db.affiliate_ledger.find({})}
            ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)
            after = {d["_id"]: d.get("surplus_checked_at") for d in db.affiliate_ledger.find({})}
            seen.append({k for k in after if after[k] != before[k]})
        # Four runs of five must touch twenty DISTINCT ledgers.
        assert len(set().union(*seen)) == 20, (
            f"runs overlapped; distinct ledgers touched: {len(set().union(*seen))}"
        )

    def test_a_newly_created_ledger_is_prioritised_not_starved(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        # Walk the whole set once so every existing ledger is stamped.
        for _ in range(12):
            ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)

        # A brand-new ledger arrives, carrying surplus, sorting LAST by _id
        # so only the never-checked ordering can reach it.
        db.affiliate_ledger.insert_one({
            "_id": "ZZZ_NEWEST", "ledger_type": "AFFILIATE_MONTHLY", "user_id": 99999,
            "status": "ISSUED", "tier": "T1", "year_month": "202609",
            "entitlement_month": "202609", "reward_plan": arp.DENOMINATION_PLAN_ID,
            "bundle_recipe": arp.tier_recipe("202609", "T1"),
            "dedup_key": "AFF:99999:202609:T1", "voucher_code": "NEWCODE",
            "vouchers": [{"code": "NEWCODE", "value": 10, "pool_id": "AFFILIATE_10"}],
            "updated_at": SEP,
        })
        db.voucher_pools.insert_one({
            "pool_id": "AFFILIATE_10", "code": "NEWCODE", "status": "issued",
            "voucher_value": 10, "issued_to_user_id": 99999,
            "ledger_id": "ZZZ_NEWEST", "issued_for_ledger_id": "ZZZ_NEWEST",
        })
        _add_surplus(db, "ZZZ_NEWEST", "SURPLUS_NEW")

        # ONE run must reach it: never-checked sorts ahead of everything.
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)
        assert stats["surplus_released"] == 1, (
            "the newest ledger starved behind older, already-clean ledgers"
        )
        assert db.voucher_pools.find_one({"code": "SURPLUS_NEW"})["status"] == "available"

    def test_surplus_on_the_last_ledger_is_eventually_found_and_repaired(self):
        db = FakeDb(UNIQUE)
        _seed(db, 60)
        _add_surplus(db, "L0059", "SURPLUS_LAST")
        released = 0
        for _ in range(12):
            released += ar.reconcile_surplus_denomination_allocations(
                db, now_utc=SEP, batch_limit=5
            )["surplus_released"]
        assert released == 1
        assert db.voucher_pools.find_one({"code": "SURPLUS_LAST"})["status"] == "available"

    def test_a_failing_ledger_does_not_starve_the_rest(self):
        db = FakeDb(UNIQUE)
        _seed(db, 20)
        _add_surplus(db, "L0019", "SURPLUS_TAIL")

        original = ar._classify_issued_pool_rows
        def explode(db_, ledger, *, recipe=None):
            if ledger.get("_id") == "L0000":
                raise RuntimeError("malformed ledger")
            return original(db_, ledger, recipe=recipe)

        ar._classify_issued_pool_rows = explode
        try:
            total_errors = 0
            released = 0
            for _ in range(4):
                st = ar.reconcile_surplus_denomination_allocations(
                    db, now_utc=SEP, batch_limit=5
                )
                total_errors += st["errors"]
                released += st["surplus_released"]
        finally:
            ar._classify_issued_pool_rows = original

        assert total_errors >= 1, "the failing ledger should have been recorded"
        assert released == 1, "a failing ledger blocked the rest of the scan"
        assert db.voucher_pools.find_one({"code": "SURPLUS_TAIL"})["status"] == "available"
        # And it is still retryable, not silently dropped.
        assert db.affiliate_ledger.find_one({"_id": "L0000"})["surplus_checked_at"] is not None

    def test_repeated_sweeps_remain_idempotent(self):
        db = FakeDb(UNIQUE)
        _seed(db, 20)
        _add_surplus(db, "L0005", "SURPLUS_ONCE")
        total = 0
        for _ in range(10):
            total += ar.reconcile_surplus_denomination_allocations(
                db, now_utc=SEP, batch_limit=5
            )["surplus_released"]
        assert total == 1, f"the same surplus was released {total} times"

    def test_counters_and_logging_survive(self):
        db = FakeDb(UNIQUE)
        _seed(db, 10)
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)
        for key in ("scanned", "surplus_found", "surplus_released",
                    "protected_not_released", "integrity_conflicts", "errors"):
            assert key in stats
