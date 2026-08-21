"""Regression tests for the T1-T4 entitlement-month batch boundary fix.

Root cause: admins hand-typed T1-T4 batch windows as an approximation of a
calendar month ("2026-08-01 00:01" -> "2026-08-31 23:59") instead of the
canonical KL-calendar-month boundary
(``affiliate_rewards._month_window_from_yyyymm``) that
``_find_batches_for_period``/``_resolve_monthly_ledger_target``/
``get_claimable_pool_inventory`` require via full-month containment. A
window off by even one minute on either edge makes the batch invisible to
that containment check, so the tier reports 0 claimable despite having
available stock.

Covers:
  - the exact canonical window is claimable end-to-end;
  - a window shifted by one minute on either edge is NOT claimable, even
    though it "looks like August";
  - ``create_batch``/``update_batch`` derive the exact canonical window from
    an ``entitlement_month`` (never an admin-typed approximation);
  - the KL month boundary does not leak entitlement across the month edge;
  - WELCOME's free-form start/end scheduling is untouched;
  - the ``scripts/fix_affiliate_batch_month_boundaries.py`` corrective path
    (built on ``update_batch``) fixes an existing off-by-one-minute batch
    without touching voucher codes, ownership, or counters.
"""

from datetime import datetime, timedelta, timezone

import fake_mongo
import affiliate_rewards as ar
import affiliate_voucher_batches as avb
from scripts.fix_affiliate_batch_month_boundaries import find_misaligned_batches, fix_batches


def _db():
    return fake_mongo.FakeDb({
        "voucher_pools": [("pool_id", "code")],
        "affiliate_ledger": [("dedup_key",)],
    })


def _create_raw_window(db, *, pool_id="T1", name="Batch", starts, ends, codes=None, now=None):
    """Bypasses entitlement_month to construct a specific, possibly
    non-canonical, window — used only to reproduce the exact bug windows.
    """
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


def _create_from_month(db, *, pool_id="T1", name="Batch", entitlement_month, codes=None, now=None):
    return avb.create_batch(
        db,
        admin_identity="admin1",
        batch_name=name,
        pool_id=pool_id,
        entitlement_month=entitlement_month,
        codes=codes if codes is not None else ["A1", "A2", "A3"],
        now_utc=now or datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


AUG_MID = datetime(2026, 8, 15, tzinfo=timezone.utc)
SEP_MID = datetime(2026, 9, 15, tzinfo=timezone.utc)


class TestExactMonthWindowClaimable:
    def test_exact_canonical_window_is_claimable(self):
        db = _db()
        res = _create_raw_window(db, starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1", "A2"])
        assert res["ok"] is True

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        assert inv["claimable_available"] == 2
        assert inv["blocking_reason"] is None

    def test_start_one_minute_late_is_not_valid(self):
        db = _db()
        res = _create_raw_window(db, starts="2026-08-01 00:01:00", ends="2026-09-01 00:00:00", codes=["A1", "A2"])
        assert res["ok"] is True

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        # Not fully claimable — the batch's window doesn't fully contain the
        # KL calendar month, so it's invisible to the full-containment
        # check regardless of which specific "why zero" reason applies.
        assert inv["claimable_available"] == 0
        assert inv["raw_available"] == 2
        assert inv["blocking_reason"] in ("no_batch_for_entitlement_period", "pool_empty")

    def test_end_one_minute_early_is_not_valid(self):
        db = _db()
        res = _create_raw_window(db, starts="2026-08-01 00:00:00", ends="2026-08-31 23:59:00", codes=["A1", "A2"])
        assert res["ok"] is True

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        assert inv["claimable_available"] == 0
        assert inv["blocking_reason"] == "no_batch_for_entitlement_period"

    def test_original_bug_window_00_01_to_23_59_is_not_valid(self):
        # The exact real-world shape reported: 2026-08-01 00:01 -> 2026-08-31 23:59.
        db = _db()
        res = _create_raw_window(db, starts="2026-08-01 00:01:00", ends="2026-08-31 23:59:00", codes=["A1", "A2"])
        assert res["ok"] is True

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        assert inv["claimable_available"] == 0
        assert inv["raw_available"] == 2
        assert inv["blocking_reason"] in ("no_batch_for_entitlement_period", "pool_empty")

    def test_batch_spanning_more_than_full_month_still_resolves_via_full_containment(self):
        # July 15 -> Sep 15 fully *contains* August, so the monthly ledger
        # target resolver (full-containment, not exact-equality) still pins
        # to it — matching _resolve_monthly_ledger_target's own semantics.
        db = _db()
        res = _create_raw_window(db, starts="2026-07-15 00:00:00", ends="2026-09-15 00:00:00", codes=["A1", "A2"])
        assert res["ok"] is True

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        assert inv["claimable_available"] == 2
        assert inv["blocking_reason"] is None

    def test_month_boundary_does_not_leak_august_into_september(self):
        db = _db()
        res = _create_raw_window(db, starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1", "A2"])
        assert res["ok"] is True

        aug_inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        sep_inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=SEP_MID)
        assert aug_inv["claimable_available"] == 2
        assert sep_inv["claimable_available"] == 0
        assert sep_inv["blocking_reason"] == "no_batch_for_entitlement_period"


class TestEntitlementMonthDerivesCanonicalWindow:
    def test_create_batch_from_entitlement_month_is_exactly_canonical(self):
        db = _db()
        res = _create_from_month(db, entitlement_month="202608", codes=["A1", "A2"])
        assert res["ok"] is True
        batch = res["batch"]
        assert batch["starts_at_kl"].startswith("2026-08-01T00:00:00")
        assert batch["ends_at_kl"].startswith("2026-09-01T00:00:00")

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        assert inv["claimable_available"] == 2
        assert inv["blocking_reason"] is None

    def test_create_batch_invalid_entitlement_month_rejected(self):
        db = _db()
        res = _create_from_month(db, entitlement_month="not-a-month", codes=["A1"])
        assert res["ok"] is False
        assert res["code"] == "invalid_entitlement_month"

    def test_update_batch_entitlement_month_overrides_manual_window(self):
        db = _db()
        created = _create_raw_window(db, starts="2026-08-01 00:01:00", ends="2026-08-31 23:59:00", codes=["A1", "A2"])
        assert created["ok"] is True
        batch_id = created["batch"]["batch_id"]

        out = avb.update_batch(
            db, batch_id, admin_identity="admin1", updates={"entitlement_month": "202608"},
        )
        assert out["ok"] is True
        assert out["batch"]["starts_at_kl"].startswith("2026-08-01T00:00:00")
        assert out["batch"]["ends_at_kl"].startswith("2026-09-01T00:00:00")

        inv = ar.get_claimable_pool_inventory(db, pool_id="T1", now_utc=AUG_MID)
        assert inv["claimable_available"] == 2
        assert inv["blocking_reason"] is None


class TestWelcomeUnaffected:
    def test_welcome_batch_keeps_free_form_window_and_instant_active_semantics(self):
        db = _db()
        # A WELCOME batch that does NOT align to a calendar month at all —
        # this must still work exactly as before, since WELCOME has no
        # monthly-entitlement concept.
        res = _create_raw_window(
            db, pool_id="WELCOME", starts="2026-08-05 09:30:00", ends="2026-08-20 18:00:00", codes=["W1", "W2"],
        )
        assert res["ok"] is True

        during = datetime(2026, 8, 10, tzinfo=timezone.utc)
        before = datetime(2026, 8, 4, tzinfo=timezone.utc)
        after = datetime(2026, 8, 21, tzinfo=timezone.utc)

        assert ar.get_claimable_pool_inventory(db, pool_id="WELCOME", now_utc=during)["claimable_available"] == 2
        assert ar.get_claimable_pool_inventory(db, pool_id="WELCOME", now_utc=before)["claimable_available"] == 0
        assert ar.get_claimable_pool_inventory(db, pool_id="WELCOME", now_utc=after)["claimable_available"] == 0

    def test_welcome_batch_creation_does_not_require_entitlement_month(self):
        db = _db()
        res = _create_raw_window(db, pool_id="WELCOME", starts="2026-08-05 00:00:00", ends="2026-08-06 00:00:00", codes=["W1"])
        assert res["ok"] is True
        assert res["batch"]["entitlement_month"] is None or isinstance(res["batch"]["entitlement_month"], str)


class TestExistingBatchCorrectionScript:
    def test_fix_script_corrects_off_by_one_minute_batch_without_touching_codes_or_counters(self):
        db = _db()
        created = _create_raw_window(
            db, pool_id="T2", name="Affiliate August 2026 - T2",
            starts="2026-08-01 00:01:00", ends="2026-08-31 23:59:00", codes=["T2-1", "T2-2", "T2-3"],
        )
        assert created["ok"] is True
        batch_id = created["batch"]["batch_id"]

        before_rows = {row["code"]: dict(row) for row in db.voucher_pools.find({"batch_id": {"$exists": True}})}
        assert len(before_rows) == 3
        assert all(row["status"] == "available" for row in before_rows.values())

        misaligned = find_misaligned_batches(db, month="202608")
        assert len(misaligned) == 1
        assert str(misaligned[0]["batch"]["_id"]) == batch_id

        results = fix_batches(db, admin_identity="migration", month="202608")
        assert len(results) == 1
        assert results[0]["result"] == "ok"

        # Codes/status/ownership/counters are untouched — only the schedule window moved.
        after_rows = {row["code"]: dict(row) for row in db.voucher_pools.find({"batch_id": {"$exists": True}})}
        assert set(after_rows.keys()) == set(before_rows.keys())
        for code, before_row in before_rows.items():
            after_row = after_rows[code]
            assert after_row["status"] == before_row["status"] == "available"
            assert after_row.get("issued_to") is None
            assert after_row.get("ledger_id") is None

        updated_batch = db.affiliate_voucher_batches.find_one({"_id": created and __import__("bson").ObjectId(batch_id)})
        assert updated_batch["uploaded_count"] == 3
        assert updated_batch["available_count"] == 3
        assert updated_batch["issued_count"] == 0

        # Now claimable exactly like the canonical window.
        inv = ar.get_claimable_pool_inventory(db, pool_id="T2", now_utc=AUG_MID)
        assert inv["claimable_available"] == 3
        assert inv["blocking_reason"] is None

    def test_fix_script_skips_batch_with_already_issued_vouchers(self):
        db = _db()
        created = _create_raw_window(
            db, pool_id="T3", starts="2026-08-01 00:01:00", ends="2026-08-31 23:59:00", codes=["T3-1"],
        )
        assert created["ok"] is True
        batch_id = created["batch"]["batch_id"]

        # Manually mark the one code issued, bypassing the (intentionally
        # blocked, since this batch isn't claimable) claim path — simulates
        # a batch that somehow already had a real issuance before correction.
        db.voucher_pools.update_one(
            {"code": "T3-1"},
            {"$set": {"status": "issued", "issued_to": 999, "ledger_id": "L1"}},
        )

        results = fix_batches(db, admin_identity="migration", month="202608")
        assert len(results) == 1
        assert results[0]["result"] == "active_batch_edit_restricted"

        batch = db.affiliate_voucher_batches.find_one({"_id": __import__("bson").ObjectId(batch_id)})
        assert batch["starts_at"] != datetime(2026, 7, 31, 16, 0, tzinfo=timezone.utc)  # unchanged, still misaligned
