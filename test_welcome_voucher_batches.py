"""Tests extending the scheduled voucher batch feature to the WELCOME pool.

WELCOME reuses every piece of the T1-T4 batch infrastructure (the
``affiliate_voucher_batches`` collection, the admin batch API, overlap
protection, upload lifecycle, active-window claim gating, authoritative
batch disablement, reconciliation and the legacy-fallback cutover) — see
``affiliate_voucher_batches.py`` and ``affiliate_rewards.py``. The one
WELCOME-specific addition is deterministic target-batch pinning
(``ar._resolve_welcome_ledger_target``), covered here in detail since it
has no T1-T4 analogue beyond ``_resolve_monthly_ledger_target``.
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


def _create(db, *, pool_id="WELCOME", name="Welcome Batch", starts="2026-08-01 00:00:00",
            ends="2026-09-01 00:00:00", codes=None, now=None, notes=None):
    return avb.create_batch(
        db,
        admin_identity="admin1",
        batch_name=name,
        pool_id=pool_id,
        starts_at_local=starts,
        ends_at_local=ends,
        timezone_name="Asia/Kuala_Lumpur",
        codes=codes if codes is not None else ["W1", "W2", "W3"],
        notes=notes,
        now_utc=now or datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


def _welcome_ledger(db, *, user_id=1, created_at=None):
    created_at = created_at or datetime.now(timezone.utc)
    doc = {
        "ledger_type": "WELCOME",
        "user_id": user_id,
        "year_month": None,
        "tier": "WELCOME",
        "pool_id": "WELCOME",
        "status": "APPROVED",
        "dedup_key": f"WELCOME:{user_id}",
        "voucher_code": None,
        "risk_flags": [],
        "created_at": created_at,
        "updated_at": created_at,
    }
    ledger_id = db.affiliate_ledger.insert_one(doc).inserted_id
    return db.affiliate_ledger.find_one({"_id": ledger_id})


# ---------------------------------------------------------------------------
# 1. Pool acceptance / API / dashboard plumbing
# ---------------------------------------------------------------------------

class TestWelcomePoolAccepted:
    def test_welcome_is_a_valid_schedulable_pool(self):
        db = _db()
        res = _create(db)
        assert res["ok"] is True
        assert res["batch"]["pool_id"] == "WELCOME"

    def test_welcome_batches_appear_in_pool_filtered_listing(self):
        db = _db()
        _create(db)
        _create(db, pool_id="T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["T1-A"])
        listing = avb.list_batches(db, pool_id="WELCOME")
        assert listing["ok"] is True
        assert len(listing["items"]) == 1
        assert listing["items"][0]["pool_id"] == "WELCOME"

    def test_welcome_status_badges_derived_same_as_t1_t4(self):
        db = _db()
        res = _create(db)
        during = datetime(2026, 8, 15, tzinfo=timezone.utc)
        assert avb.derive_batch_status(
            db.affiliate_voucher_batches.find_one({"_id": avb._as_object_id(res["batch"]["batch_id"])}), during
        ) == "active"

    def test_welcome_reconcile_action_works(self):
        db = _db()
        res = _create(db)
        out = avb.reconcile_batch(db, res["batch"]["batch_id"], admin_identity="admin1")
        assert out["ok"] is True
        assert out["batch"]["available_count"] == 3

    def test_failed_welcome_batch_cannot_be_reenabled(self):
        db = _db()
        res = _create(db, codes=["   "])  # no valid codes -> create fails, nothing to reconcile from
        assert res["ok"] is False
        # Simulate a failed batch directly (crash-mid-upload case) and confirm re-enable is blocked.
        batch_id = db.affiliate_voucher_batches.insert_one({
            "batch_name": "Broken", "pool_id": "WELCOME",
            "starts_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
            "ends_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
            "upload_status": "failed", "distribution_disabled": True,
            "available_count": 0, "issued_count": 0, "uploaded_count": 0,
        }).inserted_id
        out = avb.set_batch_distribution_disabled(db, batch_id, admin_identity="admin1", disabled=False)
        assert out["ok"] is False
        assert out["code"] == "target_batch_failed_cannot_enable"


# ---------------------------------------------------------------------------
# 2. Scheduling / active-window claim gating (mirrors TestActiveWindowClaiming)
# ---------------------------------------------------------------------------

class TestWelcomeScheduling:
    def test_future_welcome_batch_cannot_distribute(self):
        db = _db()
        _create(db)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 7, 15, tzinfo=timezone.utc))
        assert voucher is None

    def test_active_welcome_batch_distributes(self):
        db = _db()
        _create(db)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert voucher is not None
        assert voucher["code"] in ("W1", "W2", "W3")

    def test_expired_welcome_batch_cannot_distribute(self):
        db = _db()
        _create(db)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 9, 15, tzinfo=timezone.utc))
        assert voucher is None

    def test_disabled_welcome_batch_cannot_distribute(self):
        db = _db()
        res = _create(db)
        avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="a", disabled=True, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert voucher is None

    def test_staging_welcome_batch_cannot_distribute(self):
        db = _db()
        res = _create(db)
        db.affiliate_voucher_batches.update_one({"_id": avb._as_object_id(res["batch"]["batch_id"])}, {"$set": {"upload_status": "staging"}})
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert voucher is None

    def test_failed_welcome_batch_cannot_distribute(self):
        db = _db()
        res = _create(db)
        db.affiliate_voucher_batches.update_one({"_id": avb._as_object_id(res["batch"]["batch_id"])}, {"$set": {"upload_status": "failed"}})
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert voucher is None

    def test_reenabled_ready_welcome_batch_distributes(self):
        db = _db()
        res = _create(db)
        during = datetime(2026, 8, 15, tzinfo=timezone.utc)
        avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="a", disabled=True, now_utc=during)
        avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="a", disabled=False, now_utc=during)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=during)
        assert voucher is not None

    def test_exact_start_boundary_is_claimable(self):
        db = _db()
        _create(db)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 8, 1, tzinfo=timezone.utc))
        assert voucher is not None

    def test_exact_end_boundary_is_not_claimable(self):
        db = _db()
        _create(db)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 9, 1, tzinfo=timezone.utc))
        assert voucher is None

    def test_adjacent_welcome_batches_accepted(self):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        res2 = _create(db, name="September", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["S1"])
        assert res2["ok"] is True

    def test_overlapping_welcome_batches_rejected(self):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        res2 = _create(db, name="Late August", starts="2026-08-20 00:00:00", ends="2026-09-20 00:00:00", codes=["A2"])
        assert res2["ok"] is False
        assert res2["code"] == "batch_window_overlap"

    def test_welcome_may_overlap_t1_t4(self):
        db = _db()
        res1 = _create(db, pool_id="WELCOME", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["W1"])
        res2 = _create(db, pool_id="T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["T1-A"])
        assert res1["ok"] is True
        assert res2["ok"] is True


# ---------------------------------------------------------------------------
# 3. Target pinning
# ---------------------------------------------------------------------------

class TestWelcomeTargetPinning:
    def test_resolves_using_entitlement_reference_time(self):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 15, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert resolved["target_mode"] == "batch"

    def test_august_entitlement_cannot_receive_september_code(self):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        september = _create(db, name="September", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["S1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 31, 10, 0, tzinfo=timezone.utc))
        # Retry happens after the September rollover.
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 9, 2, tzinfo=timezone.utc))
        assert str(resolved.get("target_batch_id")) != september["batch"]["batch_id"]

    def test_retry_never_changes_persisted_target_batch_id(self):
        db = _db()
        august = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        _create(db, name="September", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["S1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 31, tzinfo=timezone.utc))
        first = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 31, tzinfo=timezone.utc))
        assert str(first["target_batch_id"]) == august["batch"]["batch_id"]
        second = ar._resolve_welcome_ledger_target(
            db, db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=datetime(2026, 9, 10, tzinfo=timezone.utc)
        )
        assert second["target_batch_id"] == first["target_batch_id"]

    def test_disabled_target_remains_target_without_fallthrough(self):
        db = _db()
        august = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 5, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 5, tzinfo=timezone.utc))
        avb.set_batch_distribution_disabled(db, august["batch"]["batch_id"], admin_identity="a", disabled=True)
        voucher, reason = ar._claim_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="WELCOME", ledger_id=ledger["_id"], user_id=1,
            now_utc=datetime(2026, 8, 6, tzinfo=timezone.utc),
        )
        assert voucher is None
        assert reason == "target_batch_disabled"

    def test_exhausted_target_remains_target_without_fallthrough(self):
        db = _db()
        august = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 5, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 5, tzinfo=timezone.utc))
        # Drain the only code in the batch.
        ar._claim_from_target_batch(db, batch_id=resolved["target_batch_id"], pool_id="WELCOME", ledger_id="OTHER", user_id=2, now_utc=datetime(2026, 8, 5, tzinfo=timezone.utc))
        voucher, reason = ar._claim_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="WELCOME", ledger_id=ledger["_id"], user_id=1,
            now_utc=datetime(2026, 8, 6, tzinfo=timezone.utc),
        )
        assert voucher is None
        assert reason == "target_batch_empty"

    def test_expired_target_remains_target_without_fallthrough(self):
        db = _db()
        august = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 31, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 31, tzinfo=timezone.utc))
        voucher, reason = ar._claim_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="WELCOME", ledger_id=ledger["_id"], user_id=1,
            now_utc=datetime(2026, 9, 2, tzinfo=timezone.utc),
        )
        assert voucher is None
        assert reason == "target_batch_expired_unissued"

    def test_no_matching_target_returns_stable_unresolved_reason(self):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        # A gap month with no batch, after scheduled mode already began.
        ledger = _welcome_ledger(db, created_at=datetime(2026, 10, 15, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 10, 15, tzinfo=timezone.utc))
        assert resolved.get("target_mode") is None
        assert resolved.get("target_batch_id") is None

    def test_multiple_matching_welcome_batches_fail_safely(self):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        # Force a second (structurally-shouldn't-happen) overlapping batch directly, bypassing create_batch's guard.
        db.affiliate_voucher_batches.insert_one({
            "batch_name": "Duplicate August", "pool_id": "WELCOME",
            "starts_at": datetime(2026, 8, 1, tzinfo=timezone.utc), "ends_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
            "upload_status": "ready", "distribution_disabled": False,
            "available_count": 1, "issued_count": 0, "uploaded_count": 1,
        })
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 15, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert resolved.get("target_mode") is None

    def test_already_issued_welcome_ledger_remains_unchanged(self):
        db = _db()
        august = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        ledger = _welcome_ledger(db, created_at=datetime(2026, 8, 5, tzinfo=timezone.utc))
        resolved = ar._resolve_welcome_ledger_target(db, ledger, now_utc=datetime(2026, 8, 5, tzinfo=timezone.utc))
        db.affiliate_ledger.update_one({"_id": ledger["_id"]}, {"$set": {"status": "ISSUED", "voucher_code": "A1"}})
        issued = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        # Re-resolving an already-resolved ledger is a strict no-op.
        re_resolved = ar._resolve_welcome_ledger_target(db, issued, now_utc=datetime(2026, 9, 5, tzinfo=timezone.utc))
        assert re_resolved["target_batch_id"] == resolved["target_batch_id"]
        assert re_resolved["voucher_code"] == "A1"


# ---------------------------------------------------------------------------
# 4. Legacy fallback policy
# ---------------------------------------------------------------------------

class TestWelcomeLegacyFallback:
    def test_legacy_welcome_works_before_scheduled_mode(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "LEGACY1", "status": "available"})
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 7, 1, tzinfo=timezone.utc))
        assert voucher is not None

    def test_future_batch_uploaded_early_does_not_block_legacy(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "LEGACY1", "status": "available"})
        _create(db, name="Future September", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["S1"], now=datetime(2026, 7, 1, tzinfo=timezone.utc))
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 7, 15, tzinfo=timezone.utc))
        assert voucher is not None
        assert voucher["code"] == "LEGACY1"

    def test_legacy_welcome_blocked_after_scheduled_mode_begins(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "LEGACY1", "status": "available"})
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        # No batch covers this instant (gap after August ends), but scheduled mode already began.
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime(2026, 9, 15, tzinfo=timezone.utc))
        assert voucher is None

    def test_empty_active_welcome_batch_does_not_fall_back_to_legacy(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "LEGACY1", "status": "available"})
        res = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        during = datetime(2026, 8, 5, tzinfo=timezone.utc)
        ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="X", user_id=99, now_utc=during)  # drain A1
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=during)
        assert voucher is None

    def test_disabled_active_welcome_batch_does_not_fall_back_to_legacy(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "LEGACY1", "status": "available"})
        res = _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        during = datetime(2026, 8, 5, tzinfo=timezone.utc)
        avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="a", disabled=True, now_utc=during)
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=during)
        assert voucher is None


# ---------------------------------------------------------------------------
# 5. End-to-end issue_welcome_bonus_if_eligible integration
# ---------------------------------------------------------------------------

class TestWelcomeBonusIssuanceWithBatches:
    def test_issues_from_scheduled_batch_and_persists_target(self, monkeypatch):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        monkeypatch.setattr(ar, "_is_official_channel_subscribed", lambda uid: True)
        now = datetime(2026, 8, 15, tzinfo=timezone.utc)
        out = ar.issue_welcome_bonus_if_eligible(db, user_id=42, is_new_user=True, now_utc=now)
        assert out["status"] == "ISSUED"
        ledger = db.affiliate_ledger.find_one({"dedup_key": "WELCOME:42"})
        assert ledger["target_mode"] == "batch"
        assert ledger["voucher_code"] == "A1"

    def test_out_of_stock_when_target_batch_empty(self, monkeypatch):
        db = _db()
        _create(db, name="August", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        monkeypatch.setattr(ar, "_is_official_channel_subscribed", lambda uid: True)
        now = datetime(2026, 8, 15, tzinfo=timezone.utc)
        ar.issue_welcome_bonus_if_eligible(db, user_id=1, is_new_user=True, now_utc=now)  # drains A1
        out = ar.issue_welcome_bonus_if_eligible(db, user_id=2, is_new_user=True, now_utc=now)
        assert out["status"] == "OUT_OF_STOCK"
        ledger = db.affiliate_ledger.find_one({"dedup_key": "WELCOME:2"})
        assert "welcome_target_batch_empty" in ledger.get("risk_flags", [])
