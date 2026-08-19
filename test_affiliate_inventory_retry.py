"""Tests for the inventory-only auto-recovery path on AFFILIATE_MONTHLY
ledgers stuck in PENDING_MANUAL purely because of a stale legacy/batch pin
with no usable stock (see affiliate_rewards._monthly_ledger_eligible_for_inventory_retry
and _reresolve_monthly_ledger_target_for_retry).

Invariant under test throughout: a tier reward NEVER falls back to another
tier's voucher pool. T2 can only ever be issued from T2 inventory.
"""

from datetime import datetime, timezone

import fake_mongo
import affiliate_rewards as ar
import affiliate_voucher_batches as avb


def _db():
    return fake_mongo.FakeDb({
        "voucher_pools": [("pool_id", "code")],
        "affiliate_ledger": [("dedup_key",)],
    })


def _create_batch(db, *, pool_id="T2", name="Batch", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00",
                   codes=None, now=None):
    return avb.create_batch(
        db,
        admin_identity="admin1",
        batch_name=name,
        pool_id=pool_id,
        starts_at_local=starts,
        ends_at_local=ends,
        timezone_name="Asia/Kuala_Lumpur",
        codes=codes if codes is not None else ["A1", "A2", "A3"],
        notes=None,
        now_utc=now or datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


def _monthly_ledger(db, *, user_id=1, tier="T2", year_month="202608", risk_flags=None):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY",
        "user_id": user_id,
        "year_month": year_month,
        "tier": tier,
        "pool_id": tier,
        "status": "APPROVED",
        "dedup_key": f"AFF:{user_id}:{year_month}:{tier}",
        "voucher_code": None,
        "created_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
        "risk_flags": list(risk_flags or []),
    }
    ledger_id = db.affiliate_ledger.insert_one(doc).inserted_id
    return db.affiliate_ledger.find_one({"_id": ledger_id})


AUG_MID = datetime(2026, 8, 15, tzinfo=timezone.utc)
AUG_LATE = datetime(2026, 8, 20, tzinfo=timezone.utc)


def _legacy_stock(db, pool_id: str, codes: list[str]):
    for code in codes:
        db.voucher_pools.insert_one({"pool_id": pool_id, "code": code, "status": "available"})


class TestLegacyToBatchRecovery:
    def test_stuck_legacy_t2_recovers_once_batch_uploaded(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", year_month="202608")

        # First attempt: no T2 batch exists, no legacy T2 stock either ->
        # resolves to legacy, claim fails, parked PENDING_MANUAL.
        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)
        assert result["status"] == "PENDING_MANUAL"
        assert result["target_mode"] == "legacy"
        assert "pool_empty" in result["risk_flags"]

        # Ops uploads a T2 batch covering the entitlement month.
        _create_batch(db, pool_id="T2", codes=["T2A", "T2B", "T2C"])

        # Retry — same ledger, no evaluate_monthly_affiliate_reward re-run.
        pending = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        retried = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_LATE)

        assert retried["status"] == "ISSUED"
        assert retried["target_mode"] == "batch"
        assert retried["voucher_count"] == 3
        # Stale inventory flag cleared on success.
        assert "pool_empty" not in (retried.get("risk_flags") or [])
        # Still the very same ledger row — no duplicate created.
        assert db.affiliate_ledger.count_documents({"tier": "T2", "year_month": "202608", "user_id": 1}) == 1


class TestNoCrossTierFallback:
    def test_t2_empty_never_touches_t1_or_t3_pools(self):
        db = _db()
        # T2: no batch, no legacy stock. T1/T3: plenty of legacy stock.
        _legacy_stock(db, "T1", [f"T1-{i}" for i in range(100)])
        _legacy_stock(db, "T3", [f"T3-{i}" for i in range(100)])
        ledger = _monthly_ledger(db, tier="T2", year_month="202608")

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)

        assert result["status"] == "PENDING_MANUAL"
        assert result.get("target_mode") == "legacy"
        assert db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}) == 100
        assert db.voucher_pools.count_documents({"pool_id": "T3", "status": "available"}) == 100
        assert db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}) == 0
        assert db.voucher_pools.count_documents({"pool_id": "T3", "status": "issued"}) == 0
        assert not result.get("vouchers")

    def test_t2_refill_later_still_never_touches_t1_or_t3(self):
        db = _db()
        _legacy_stock(db, "T1", [f"T1-{i}" for i in range(100)])
        _legacy_stock(db, "T3", [f"T3-{i}" for i in range(100)])
        ledger = _monthly_ledger(db, tier="T2", year_month="202608")

        first = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)
        assert first["status"] == "PENDING_MANUAL"

        # T2 stock becomes sufficient later.
        _create_batch(db, pool_id="T2", codes=["T2A", "T2B", "T2C"])
        pending = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        second = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_LATE)

        assert second["status"] == "ISSUED"
        assert db.affiliate_ledger.count_documents({"tier": "T2", "year_month": "202608", "user_id": 1}) == 1
        # T1/T3 pools completely untouched throughout.
        assert db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}) == 100
        assert db.voucher_pools.count_documents({"pool_id": "T3", "status": "available"}) == 100


class TestRiskRowExcludedFromAutoRecovery:
    def test_risk_flagged_ledger_is_not_reresolved_even_with_new_batch(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", year_month="202608", risk_flags=["ip_cluster"])

        first = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)
        assert first["status"] == "PENDING_MANUAL"
        assert set(first["risk_flags"]) == {"ip_cluster", "pool_empty"}
        assert first["target_mode"] == "legacy"

        # New T2 batch now covers the month — but the ledger still carries
        # an abuse/risk-review flag, so it must NOT be auto-recovered.
        _create_batch(db, pool_id="T2", codes=["T2A", "T2B", "T2C"])
        pending = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        retried = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_LATE)

        assert retried["status"] == "PENDING_MANUAL"
        assert retried["target_mode"] == "legacy"  # never re-pointed at the batch
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 0
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "available"}) == 3


class TestDuplicateProtection:
    def test_retry_after_issued_does_not_consume_more_stock(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", year_month="202608")
        ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)  # -> PENDING_MANUAL

        _create_batch(db, pool_id="T2", codes=["T2A", "T2B", "T2C", "T2D", "T2E", "T2F"])
        pending = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        issued_once = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_LATE)
        assert issued_once["status"] == "ISSUED"
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 3

        latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        issued_twice = ar._issue_affiliate_ledger_from_pool(db, ledger=latest, now_utc=AUG_LATE)
        assert issued_twice["status"] == "ISSUED"
        # No additional vouchers consumed on the second call.
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 3
        assert db.affiliate_ledger.count_documents({"tier": "T2", "year_month": "202608", "user_id": 1}) == 1


class TestBundleAtomicity:
    def test_partial_stock_never_partially_claims(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", year_month="202608")
        ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)  # -> PENDING_MANUAL

        # T2 requires a 3-voucher bundle; only 2 are claimable.
        _create_batch(db, pool_id="T2", codes=["T2A", "T2B"])
        pending = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        retried = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_LATE)

        assert retried["status"] == "PENDING_MANUAL"
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "available"}) == 2
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 0
