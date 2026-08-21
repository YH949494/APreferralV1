"""Tests for the narrowly-scoped ``--allow-active-boundary-repair`` maintenance
bypass in ``scripts/fix_affiliate_batch_month_boundaries.py``.

Context: the normal corrective path goes through
``affiliate_voucher_batches.update_batch``, which correctly refuses any
schedule edit on a batch that already has issued vouchers
(``active_batch_edit_restricted``). That guard must stay global — it is
never weakened here. This script instead offers a narrow, opt-in bypass
that performs the boundary correction directly, and only for a batch that
matches the exact known malformed shape (first-of-month 00:01:00 KL ->
last-of-month 23:59:00 KL) for the entitlement month pinned by --month,
with no resulting overlap. Everything else about the batch (codes, status,
ownership, ledger linkage, counters) must stay untouched, and the
denormalized boundary fields on voucher_pools rows must move in lockstep.
"""

from datetime import datetime, timezone
from bson import ObjectId

import fake_mongo
import affiliate_voucher_batches as avb
from scripts.fix_affiliate_batch_month_boundaries import fix_batches


def _db():
    return fake_mongo.FakeDb({
        "voucher_pools": [("pool_id", "code")],
        "affiliate_ledger": [("dedup_key",)],
    })


def _create_raw_window(db, *, pool_id="T1", name="Batch", starts, ends, codes=None, now=None):
    return avb.create_batch(
        db,
        admin_identity="admin1",
        batch_name=name,
        pool_id=pool_id,
        starts_at_local=starts,
        ends_at_local=ends,
        timezone_name="Asia/Kuala_Lumpur",
        codes=codes if codes is not None else ["A1", "A2", "A3"],
        now_utc=now or datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


def _mark_issued(db, code, *, user_id=999, ledger_id="L1"):
    db.voucher_pools.update_one(
        {"code": code},
        {"$set": {"status": "issued", "issued_to": user_id, "ledger_id": ledger_id}},
    )


def _malformed_august(db, *, pool_id="T1", codes=None):
    return _create_raw_window(
        db, pool_id=pool_id, starts="2026-08-01 00:01:00", ends="2026-08-31 23:59:00",
        codes=codes if codes is not None else [f"{pool_id}-1"],
    )


class TestActiveBatchNoFlagStillRestricted:
    def test_active_malformed_batch_without_flag_remains_restricted(self):
        db = _db()
        created = _malformed_august(db, pool_id="T1")
        _mark_issued(db, "T1-1")

        results = fix_batches(db, admin_identity="migration", month="202608")
        assert len(results) == 1
        assert results[0]["result"] == "active_batch_edit_restricted"

        batch = db.affiliate_voucher_batches.find_one({"_id": ObjectId(created["batch"]["batch_id"])})
        assert batch["starts_at"] != datetime(2026, 7, 31, 16, 0, tzinfo=timezone.utc)


class TestActiveBatchWithFlagRepaired:
    def test_active_malformed_batch_with_flag_is_repaired(self):
        db = _db()
        created = _malformed_august(db, pool_id="T1", codes=["T1-1", "T1-2"])
        _mark_issued(db, "T1-1")
        batch_id = ObjectId(created["batch"]["batch_id"])

        results = fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )
        assert len(results) == 1
        assert results[0]["result"] == "maintenance_boundary_repair"

        batch = db.affiliate_voucher_batches.find_one({"_id": batch_id})
        assert batch["starts_at"] == datetime(2026, 7, 31, 16, 0, tzinfo=timezone.utc)  # Aug 1 00:00 KL
        assert batch["ends_at"] == datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc)  # Sep 1 00:00 KL

        rows = {row["code"]: dict(row) for row in db.voucher_pools.find({"batch_id": batch_id})}
        assert rows["T1-1"]["starts_at"] == batch["starts_at"]
        assert rows["T1-1"]["ends_at"] == batch["ends_at"]
        assert rows["T1-2"]["starts_at"] == batch["starts_at"]
        assert rows["T1-2"]["ends_at"] == batch["ends_at"]

    def test_dry_run_with_flag_reports_without_writing(self):
        db = _db()
        created = _malformed_august(db, pool_id="T1", codes=["T1-1"])
        _mark_issued(db, "T1-1")
        batch_id = ObjectId(created["batch"]["batch_id"])
        before = db.affiliate_voucher_batches.find_one({"_id": batch_id})

        results = fix_batches(
            db, admin_identity="migration", month="202608",
            dry_run=True, allow_active_boundary_repair=True,
        )
        assert len(results) == 1
        assert results[0]["result"] == "dry_run_maintenance_boundary_repair"

        after = db.affiliate_voucher_batches.find_one({"_id": batch_id})
        assert after == before


class TestUnrelatedScheduleRefused:
    def test_active_batch_with_non_standard_shape_is_refused_even_with_flag(self):
        db = _db()
        # Off by two minutes on each edge instead of exactly one — within
        # the general misalignment-detection tolerance, but NOT the known
        # malformed shape this bypass targets, so it must be refused.
        created = _create_raw_window(
            db, pool_id="T1", starts="2026-08-01 00:02:00", ends="2026-08-31 23:58:00",
            codes=["T1-1"],
        )
        _mark_issued(db, "T1-1")
        batch_id = ObjectId(created["batch"]["batch_id"])
        before = db.affiliate_voucher_batches.find_one({"_id": batch_id})

        results = fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )
        assert len(results) == 1
        assert results[0]["result"] == "active_batch_edit_restricted:not_known_malformed_shape"

        after = db.affiliate_voucher_batches.find_one({"_id": batch_id})
        assert after == before


class TestCanonicalBatchNoOp:
    def test_canonical_batch_is_a_no_op(self):
        db = _db()
        created = _create_raw_window(
            db, pool_id="T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00",
            codes=["T1-1"],
        )
        _mark_issued(db, "T1-1")

        results = fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )
        # Already canonical -- never even surfaces as misaligned.
        assert results == []


class TestOverlappingTargetRefused:
    def test_overlap_with_target_window_is_refused(self):
        db = _db()
        created = _malformed_august(db, pool_id="T1", codes=["T1-1"])
        _mark_issued(db, "T1-1")
        batch_id = ObjectId(created["batch"]["batch_id"])

        # Doesn't overlap the malformed batch's *current* window (ends
        # Aug 31 23:59:00), but does overlap the *canonical target* window
        # (ends Sep 1 00:00:00 exclusive) the repair would move it to.
        other = _create_raw_window(
            db, pool_id="T1", name="Sept sliver", starts="2026-08-31 23:59:30", ends="2026-09-02 00:00:00",
            codes=["T1-OTHER"],
        )
        assert other["ok"] is True

        before = db.affiliate_voucher_batches.find_one({"_id": batch_id})
        results = fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )
        assert len(results) == 1
        assert results[0]["result"] == "active_batch_edit_restricted:batch_window_overlap"

        after = db.affiliate_voucher_batches.find_one({"_id": batch_id})
        assert after == before


class TestIssuedVoucherDataUntouched:
    def test_issued_voucher_ownership_status_code_unchanged(self):
        db = _db()
        created = _malformed_august(db, pool_id="T1", codes=["T1-1", "T1-2"])
        _mark_issued(db, "T1-1", user_id=42, ledger_id="LEDGER-42")
        batch_id = ObjectId(created["batch"]["batch_id"])

        before_rows = {row["code"]: dict(row) for row in db.voucher_pools.find({"batch_id": batch_id})}

        results = fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )
        assert results[0]["result"] == "maintenance_boundary_repair"

        after_rows = {row["code"]: dict(row) for row in db.voucher_pools.find({"batch_id": batch_id})}
        assert set(after_rows) == set(before_rows)
        for code, before_row in before_rows.items():
            after_row = after_rows[code]
            assert after_row["code"] == before_row["code"]
            assert after_row["status"] == before_row["status"]
            assert after_row.get("issued_to") == before_row.get("issued_to")
            assert after_row.get("ledger_id") == before_row.get("ledger_id")
        assert after_rows["T1-1"]["status"] == "issued"
        assert after_rows["T1-1"]["issued_to"] == 42
        assert after_rows["T1-1"]["ledger_id"] == "LEDGER-42"
        assert after_rows["T1-2"]["status"] == "available"


class TestVoucherPoolsBoundaryOnlyUpdated:
    def test_only_boundary_fields_move_on_voucher_pools(self):
        db = _db()
        created = _malformed_august(db, pool_id="T1", codes=["T1-1"])
        _mark_issued(db, "T1-1")
        batch_id = ObjectId(created["batch"]["batch_id"])
        before_row = db.voucher_pools.find_one({"code": "T1-1"})

        fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )

        after_row = db.voucher_pools.find_one({"code": "T1-1"})
        assert after_row["starts_at"] == datetime(2026, 7, 31, 16, 0, tzinfo=timezone.utc)
        assert after_row["ends_at"] == datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc)
        for key, value in before_row.items():
            if key in ("starts_at", "ends_at"):
                continue
            assert after_row.get(key) == value, f"unexpected change to {key}"


class TestWelcomeNeverUsesBypass:
    def test_welcome_batches_are_never_touched_by_the_bypass(self):
        db = _db()
        # WELCOME batches aren't entitlement-month scoped and don't even
        # participate in find_misaligned_batches (which only scans
        # ENTITLEMENT_MONTH_POOL_IDS), so this must be a strict no-op
        # regardless of the flag.
        created = _create_raw_window(
            db, pool_id="WELCOME", starts="2026-08-01 00:01:00", ends="2026-08-31 23:59:00",
            codes=["W-1"],
        )
        assert created["ok"] is True
        _mark_issued(db, "W-1")
        batch_id = ObjectId(created["batch"]["batch_id"])
        before = db.affiliate_voucher_batches.find_one({"_id": batch_id})

        results = fix_batches(
            db, admin_identity="migration", month="202608",
            allow_active_boundary_repair=True,
        )
        assert results == []

        after = db.affiliate_voucher_batches.find_one({"_id": batch_id})
        assert after == before
