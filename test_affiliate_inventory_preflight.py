"""Strict inventory preflight: September denomination stock must come from a
scheduled batch on the exact canonical KL month, and must be sufficient.

The runtime resolver refuses to issue a denomination reward from undated
stock, so the preflight must never report undated stock as valid coverage —
otherwise it would greenlight a launch that then parks every entitlement in
PENDING_MANUAL.
"""
from __future__ import annotations

import affiliate_voucher_batches as batches
from fake_mongo import FakeDb
from scripts.verify_affiliate_reward_plan import Findings, check_inventory

UK = {"voucher_pools": [("pool_id", "code")]}
POOLS = ("AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50")


def _db():
    return FakeDb(UK)


def _batch(db, pool_id, count, *, month="202609", prefix="C"):
    res = batches.create_batch(
        db, admin_identity="test", batch_name=f"{pool_id} {month}",
        pool_id=pool_id, entitlement_month=month,
        codes=[f"{pool_id}-{prefix}{i}" for i in range(count)],
    )
    assert res["ok"] is True, res
    return res


def _undated(db, pool_id, count):
    for i in range(count):
        db.voucher_pools.insert_one(
            {"pool_id": pool_id, "code": f"{pool_id}-UND{i}", "status": "available"}
        )


class TestCoverage:
    def test_zero_batch_coverage_fails_even_with_plenty_of_undated_stock(self):
        db = _db()
        for pool in POOLS:
            _undated(db, pool, 500)
        f = Findings()
        check_inventory(db, f, "202609")
        assert len(f.items) == 3, f.items
        for item in f.items:
            assert "NO scheduled batch covers 202609" in item
            assert "NOT usable" in item

    def test_full_coverage_passes(self):
        db = _db()
        for pool in POOLS:
            _batch(db, pool, 20)
        f = Findings()
        check_inventory(db, f, "202609")
        assert f.items == []

    def test_partial_coverage_fails_for_the_missing_pool(self):
        db = _db()
        _batch(db, "AFFILIATE_5", 20)
        _batch(db, "AFFILIATE_10", 20)
        f = Findings()
        check_inventory(db, f, "202609")
        assert len(f.items) == 1
        assert "AFFILIATE_50" in f.items[0]

    def test_a_batch_for_a_different_month_is_not_coverage(self):
        db = _db()
        for pool in POOLS:
            _batch(db, pool, 20, month="202610")
        f = Findings()
        check_inventory(db, f, "202609")
        assert len(f.items) == 3
        assert all("NO scheduled batch" in i for i in f.items)

    def test_non_canonical_boundary_fails(self):
        db = _db()
        for pool in POOLS:
            _batch(db, pool, 20)
        # Nudge one batch off the canonical boundary by a single minute.
        row = db.affiliate_voucher_batches.find_one({"pool_id": "AFFILIATE_10"})
        from datetime import timedelta

        db.affiliate_voucher_batches.update_one(
            {"_id": row["_id"]},
            {"$set": {"starts_at": row["starts_at"] + timedelta(minutes=1)}},
        )
        f = Findings()
        check_inventory(db, f, "202609")
        # A one-minute nudge breaks full-month containment, so the batch no
        # longer covers the month at all -- either finding is a hard fail.
        assert len(f.items) == 1
        assert "AFFILIATE_10" in f.items[0]

    def test_ambiguous_coverage_fails(self):
        db = _db()
        for pool in POOLS:
            _batch(db, pool, 20)
        dup = db.affiliate_voucher_batches.find_one({"pool_id": "AFFILIATE_50"})
        db.affiliate_voucher_batches.insert_one(
            {k: v for k, v in dup.items() if k != "_id"} | {"batch_name": "dup"}
        )
        f = Findings()
        check_inventory(db, f, "202609")
        assert any("ambiguous" in i for i in f.items)

    def test_disabled_or_unready_batch_fails(self):
        db = _db()
        for pool in POOLS:
            _batch(db, pool, 20)
        row = db.affiliate_voucher_batches.find_one({"pool_id": "AFFILIATE_5"})
        db.affiliate_voucher_batches.update_one(
            {"_id": row["_id"]}, {"$set": {"distribution_disabled": True}}
        )
        f = Findings()
        check_inventory(db, f, "202609")
        assert any("distribution is disabled" in i for i in f.items)

    def test_empty_batch_fails(self):
        db = _db()
        _batch(db, "AFFILIATE_5", 5)
        _batch(db, "AFFILIATE_10", 5)
        _batch(db, "AFFILIATE_50", 5)
        db.voucher_pools.update_one(
            {"pool_id": "AFFILIATE_50"}, {"$set": {"status": "issued"}}
        )
        for row in db.voucher_pools.find({"pool_id": "AFFILIATE_50"}):
            db.voucher_pools.update_one({"_id": row["_id"]}, {"$set": {"status": "issued"}})
        f = Findings()
        check_inventory(db, f, "202609")
        assert any("no available stock" in i for i in f.items)


class TestDemandSizing:
    def _stocked(self, five, ten, fifty):
        db = _db()
        _batch(db, "AFFILIATE_5", five)
        _batch(db, "AFFILIATE_10", ten)
        _batch(db, "AFFILIATE_50", fifty)
        return db

    def test_required_inventory_matches_the_confirmed_recipes(self):
        # One of every tier = 1x$5, 7x$10, 11x$50.
        db = self._stocked(1, 7, 11)
        f = Findings()
        check_inventory(db, f, "202609",
                        {"T1": 1, "T2": 1, "T3": 1, "T4": 1, "T5": 1})
        assert f.items == [], f.items

    def test_one_short_of_demand_fails(self):
        db = self._stocked(1, 7, 10)  # one $50 short
        f = Findings()
        check_inventory(db, f, "202609",
                        {"T1": 1, "T2": 1, "T3": 1, "T4": 1, "T5": 1})
        assert len(f.items) == 1
        assert "AFFILIATE_50" in f.items[0]
        assert "required=11" in f.items[0] and "shortage=1" in f.items[0]
        assert "INSUFFICIENT" in f.items[0]

    def test_scaled_demand_multiplies_correctly(self):
        # 10 users each earning every tier = 10x$5, 70x$10, 110x$50.
        db = self._stocked(10, 70, 110)
        f = Findings()
        check_inventory(db, f, "202609",
                        {"T1": 10, "T2": 10, "T3": 10, "T4": 10, "T5": 10})
        assert f.items == []

        short = self._stocked(10, 70, 109)
        f2 = Findings()
        check_inventory(short, f2, "202609",
                        {"T1": 10, "T2": 10, "T3": 10, "T4": 10, "T5": 10})
        assert any("shortage=1" in i for i in f2.items)

    def test_per_tier_demand_uses_that_tier_recipe_only(self):
        # 5 x T5 needs 35 x $50 and nothing else.
        db = self._stocked(1, 1, 35)
        f = Findings()
        check_inventory(db, f, "202609", {"T5": 5})
        assert f.items == []

    def test_undated_stock_never_counts_toward_demand(self):
        db = self._stocked(1, 7, 5)
        _undated(db, "AFFILIATE_50", 500)
        f = Findings()
        check_inventory(db, f, "202609",
                        {"T1": 1, "T2": 1, "T3": 1, "T4": 1, "T5": 1})
        assert any("AFFILIATE_50" in i and "INSUFFICIENT" in i for i in f.items), (
            "undated stock was counted as valid September coverage"
        )

    def test_august_month_checks_the_legacy_tier_pools(self):
        db = _db()
        f = Findings()
        check_inventory(db, f, "202608", {"T3": 1})
        # Legacy plan -> per-tier pools are what gets checked, not denominations.
        assert any(i.startswith("T") or "T1" in i or "T3" in i for i in f.items)
        assert not any("AFFILIATE_" in i for i in f.items)
