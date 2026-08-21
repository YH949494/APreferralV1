"""Tests for affiliate_rewards.get_claimable_pool_inventory — the shared
source of truth for "how many <tier> vouchers can the bot actually issue
right now", used by both the Admin Dashboard Pool Summary and affiliate
issuance so they can never disagree about the same tier at the same moment.
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


def _create_batch(db, *, pool_id="T3", name="Batch", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00",
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


def _legacy_stock(db, pool_id: str, codes: list[str]):
    for code in codes:
        db.voucher_pools.insert_one({"pool_id": pool_id, "code": code, "status": "available"})


AUG_MID = datetime(2026, 8, 15, tzinfo=timezone.utc)


class TestRawEqualsClaimable:
    def test_legacy_stock_before_any_batch_is_fully_claimable(self):
        db = _db()
        _legacy_stock(db, "T3", [f"T3-{i}" for i in range(95)])

        inv = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)

        assert inv == {
            "pool_id": "T3",
            "claimable_available": 95,
            "raw_available": 95,
            "issued": 0,
            "blocking_reason": None,
        }

    def test_active_batch_fully_covers_raw_stock(self):
        db = _db()
        _create_batch(db, pool_id="T3", codes=[f"T3-{i}" for i in range(95)])

        inv = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)

        assert inv["raw_available"] == 95
        assert inv["claimable_available"] == 95
        assert inv["blocking_reason"] is None


class TestRawPositiveButNotClaimable:
    def test_codes_exist_but_no_batch_covers_entitlement_period(self):
        db = _db()
        # 95 codes physically exist, but the tier already left legacy mode
        # (an earlier batch's window has started), and no active batch
        # currently covers `now_utc` -> nothing is claimable right now.
        _create_batch(
            db, pool_id="T3", codes=["OLD1"],
            starts="2026-06-01 00:00:00", ends="2026-07-01 00:00:00",
            now=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )
        _legacy_stock(db, "T3", [f"T3-{i}" for i in range(95)])

        inv = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)

        # 95 legacy + the 1 still-"available" OLD1 row from the earlier,
        # now-expired batch — all physically present, none claimable.
        assert inv["raw_available"] == 96
        assert inv["claimable_available"] == 0
        assert inv["blocking_reason"] == "no_batch_for_entitlement_period"

    def test_active_batch_disabled_blocks_claim_despite_raw_stock(self):
        db = _db()
        _create_batch(db, pool_id="T3", codes=[f"T3-{i}" for i in range(95)])
        batch_doc = db.affiliate_voucher_batches.find_one({"pool_id": "T3"})
        db.affiliate_voucher_batches.update_one(
            {"_id": batch_doc["_id"]}, {"$set": {"distribution_disabled": True}}
        )

        inv = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)

        assert inv["raw_available"] == 95
        assert inv["claimable_available"] == 0
        assert inv["blocking_reason"] == "target_batch_disabled"

    def test_active_batch_fully_issued_blocks_claim_despite_other_raw_stock(self):
        db = _db()
        _create_batch(db, pool_id="T3", codes=["T3A"])
        # Consume the only code in the active batch.
        db.voucher_pools.update_one(
            {"pool_id": "T3", "code": "T3A"},
            {"$set": {"status": "issued", "issued_to_user_id": 999}},
        )
        # Unrelated legacy stock exists for the tier but must not count as
        # claimable while an active (now-empty) batch is in scope.
        _legacy_stock(db, "T3", ["LEGACY1"])

        inv = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)

        assert inv["raw_available"] == 1  # LEGACY1 only; T3A is issued
        assert inv["claimable_available"] == 0
        assert inv["blocking_reason"] == "target_batch_empty"


class TestInventoryIncreasesAfterValidBatchAdded:
    def test_claimable_goes_from_zero_to_positive_once_batch_covers_period(self):
        db = _db()
        before = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)
        assert before["claimable_available"] == 0
        assert before["blocking_reason"] == "pool_empty"

        _create_batch(db, pool_id="T3", codes=["T3A", "T3B", "T3C"])

        after = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)
        assert after["claimable_available"] == 3
        assert after["raw_available"] == 3
        assert after["blocking_reason"] is None


class TestIssuanceAgreesWithDashboardHelper:
    def test_dashboard_zero_claimable_matches_issuance_pool_empty(self):
        db = _db()
        _create_batch(
            db, pool_id="T3", codes=["OLD1"],
            starts="2026-06-01 00:00:00", ends="2026-07-01 00:00:00",
            now=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )
        _legacy_stock(db, "T3", [f"T3-{i}" for i in range(95)])

        inv = ar.get_claimable_pool_inventory(db, pool_id="T3", now_utc=AUG_MID)
        assert inv["claimable_available"] == 0

        ledger_doc = {
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": 1,
            "year_month": "202608",
            "tier": "T3",
            "pool_id": "T3",
            "status": "APPROVED",
            "dedup_key": "AFF:1:202608:T3",
            "voucher_code": None,
            "created_at": AUG_MID,
            "risk_flags": [],
        }
        ledger_id = db.affiliate_ledger.insert_one(ledger_doc).inserted_id
        ledger = db.affiliate_ledger.find_one({"_id": ledger_id})

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)

        assert result["status"] == "PENDING_MANUAL"
        assert "pool_empty" in result["risk_flags"] or "no_batch_for_entitlement_period" in result["risk_flags"]


class TestSuccessfulRetryClearsStaleInventoryFlagOnly:
    def test_stale_pool_empty_cleared_genuine_risk_flag_kept(self):
        db = _db()
        ledger_doc = {
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": 1,
            "year_month": "202608",
            "tier": "T3",
            "pool_id": "T3",
            "status": "APPROVED",
            "dedup_key": "AFF:1:202608:T3",
            "voucher_code": None,
            "created_at": AUG_MID,
            "risk_flags": ["ip_cluster"],
        }
        ledger_id = db.affiliate_ledger.insert_one(ledger_doc).inserted_id
        ledger = db.affiliate_ledger.find_one({"_id": ledger_id})

        first = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)
        assert first["status"] == "PENDING_MANUAL"
        assert set(first["risk_flags"]) == {"ip_cluster", "pool_empty"}

        # Genuine risk flag present -> auto-recovery must not kick in even
        # once stock exists.
        _create_batch(db, pool_id="T3", codes=["T3A", "T3B", "T3C", "T3D", "T3E"])
        pending = db.affiliate_ledger.find_one({"_id": ledger_id})
        retried = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_MID)

        assert retried["status"] == "PENDING_MANUAL"
        assert "ip_cluster" in retried["risk_flags"]

    def test_stale_pool_empty_cleared_after_valid_retry_no_risk_flag(self):
        db = _db()
        ledger_doc = {
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": 1,
            "year_month": "202608",
            "tier": "T3",
            "pool_id": "T3",
            "status": "APPROVED",
            "dedup_key": "AFF:1:202608:T3",
            "voucher_code": None,
            "created_at": AUG_MID,
            "risk_flags": [],
        }
        ledger_id = db.affiliate_ledger.insert_one(ledger_doc).inserted_id
        ledger = db.affiliate_ledger.find_one({"_id": ledger_id})

        first = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=AUG_MID)
        assert first["status"] == "PENDING_MANUAL"
        assert "pool_empty" in first["risk_flags"]

        _create_batch(db, pool_id="T3", codes=["T3A", "T3B", "T3C", "T3D", "T3E"])
        pending = db.affiliate_ledger.find_one({"_id": ledger_id})
        retried = ar._issue_affiliate_ledger_from_pool(db, ledger=pending, now_utc=AUG_MID)

        assert retried["status"] == "ISSUED"
        assert "pool_empty" not in (retried.get("risk_flags") or [])
