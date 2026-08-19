"""Regression tests for bundle-aware reconciliation of AFFILIATE_MONTHLY
ledgers stuck in PENDING_MANUAL despite a complete same-tier voucher bundle
already sitting in ``voucher_pools`` as ``status=issued`` (see
affiliate_rewards._find_complete_issued_affiliate_bundle,
_reconcile_affiliate_bundle_from_issued_pool, and
_retry_stuck_pending_manual_affiliate_ledgers).

Confirmed production case this guards against: a T2 ledger stuck
PENDING_MANUAL while all 3 T2 voucher_pools rows for it are already
status=issued — reconciliation must flip the ledger to ISSUED without
claiming any additional voucher, and must never let a different tier or a
different user's issued rows complete the bundle.
"""

from datetime import datetime, timezone

import fake_mongo
import affiliate_rewards as ar


def _db():
    return fake_mongo.FakeDb({
        "voucher_pools": [("pool_id", "code")],
        "affiliate_ledger": [("dedup_key",)],
    })


def _monthly_ledger(db, *, user_id=8961231447, tier="T2", year_month="202608", risk_flags=None, status="PENDING_MANUAL"):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY",
        "user_id": user_id,
        "year_month": year_month,
        "tier": tier,
        "pool_id": tier,
        "status": status,
        "dedup_key": f"AFF:{user_id}:{year_month}:{tier}",
        "voucher_code": None,
        "created_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
        "risk_flags": list(risk_flags or []),
    }
    ledger_id = db.affiliate_ledger.insert_one(doc).inserted_id
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _issued_row(db, *, pool_id, code, ledger_id, user_id):
    db.voucher_pools.insert_one({
        "pool_id": pool_id,
        "code": code,
        "status": "issued",
        "issued_to": user_id,
        "issued_to_user_id": user_id,
        "issued_at": datetime(2026, 8, 5, tzinfo=timezone.utc),
        "ledger_id": ledger_id,
        "issued_for_ledger_id": str(ledger_id),
    })


NOW = datetime(2026, 8, 20, tzinfo=timezone.utc)


class TestCompleteBundleReconciliation:
    def test_stuck_t2_with_complete_bundle_reconciles_to_issued(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", risk_flags=["pool_empty"])
        for code in ("A1", "A2", "A3"):
            _issued_row(db, pool_id="T2", code=code, ledger_id=ledger["_id"], user_id=ledger["user_id"])

        out = ar.retry_current_month_pending_manual_ledgers(db, now_utc=NOW)
        assert out is not None

        result = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert result["status"] == "ISSUED"
        assert result["voucher_count"] == 3
        assert "pool_empty" not in (result.get("risk_flags") or [])
        # Zero additional vouchers consumed — still exactly the 3 pre-existing rows.
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 3
        assert db.voucher_pools.count_documents({"pool_id": "T2"}) == 3


class TestRepeatedRetryIsNoOp:
    def test_second_retry_does_not_change_voucher_count(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2")
        for code in ("A1", "A2", "A3"):
            _issued_row(db, pool_id="T2", code=code, ledger_id=ledger["_id"], user_id=ledger["user_id"])

        ar.retry_current_month_pending_manual_ledgers(db, now_utc=NOW)
        first = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert first["status"] == "ISSUED"

        ar.retry_current_month_pending_manual_ledgers(db, now_utc=NOW)
        second = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert second["status"] == "ISSUED"
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 3


class TestWrongTierNeverReconciles:
    def test_one_linked_row_from_another_tier_blocks_reconciliation(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2")
        _issued_row(db, pool_id="T2", code="A1", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _issued_row(db, pool_id="T2", code="A2", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _issued_row(db, pool_id="T1", code="A3", ledger_id=ledger["_id"], user_id=ledger["user_id"])

        bundle = ar._find_complete_issued_affiliate_bundle(db, ledger)
        assert bundle is None

        ar.retry_current_month_pending_manual_ledgers(db, now_utc=NOW)
        result = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert result["status"] != "ISSUED"


class TestWrongUserNeverReconciles:
    def test_rows_belonging_to_another_user_block_reconciliation(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", user_id=111)
        for code in ("A1", "A2", "A3"):
            _issued_row(db, pool_id="T2", code=code, ledger_id=ledger["_id"], user_id=222)

        bundle = ar._find_complete_issued_affiliate_bundle(db, ledger)
        assert bundle is None

        ar.retry_current_month_pending_manual_ledgers(db, now_utc=NOW)
        result = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert result["status"] != "ISSUED"


class TestIncompleteBundleNeverFinalizedOrDoubleClaimed:
    def test_two_of_three_linked_rows_stays_pending_and_does_not_claim_a_fresh_bundle(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2")
        _issued_row(db, pool_id="T2", code="A1", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _issued_row(db, pool_id="T2", code="A2", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        # Extra unclaimed T2 stock available — must NOT be auto-claimed into
        # a second bundle on top of the 2 already-issued rows.
        db.voucher_pools.insert_one({"pool_id": "T2", "code": "B1", "status": "available"})
        db.voucher_pools.insert_one({"pool_id": "T2", "code": "B2", "status": "available"})
        db.voucher_pools.insert_one({"pool_id": "T2", "code": "B3", "status": "available"})

        bundle = ar._find_complete_issued_affiliate_bundle(db, ledger)
        assert bundle is None

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=NOW)
        assert result["status"] != "ISSUED"
        # Still only the original 2 issued rows — no blind 5-voucher claim.
        assert db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}) == 2


class TestRiskReviewPolicyPreserved:
    def test_abuse_risk_flag_blocks_auto_reconciliation_even_with_complete_bundle(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T2", risk_flags=["ip_cluster", "pool_empty"])
        for code in ("A1", "A2", "A3"):
            _issued_row(db, pool_id="T2", code=code, ledger_id=ledger["_id"], user_id=ledger["user_id"])

        ar.retry_current_month_pending_manual_ledgers(db, now_utc=NOW)
        result = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        # The direct-scan retry must never auto-finalize a risk-flagged ledger.
        assert result["status"] == "PENDING_MANUAL"
        assert set(result["risk_flags"]) == {"ip_cluster", "pool_empty"}
