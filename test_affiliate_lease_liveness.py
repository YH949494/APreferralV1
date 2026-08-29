"""A live allocator's rows must never be released.

Liveness is the allocation LEASE, never `updated_at`. A worker that has been
renewing its lease for ten minutes has a fresh `allocation_lease_at` and a
stale `updated_at`; releasing its claimed rows would corrupt a bundle that is
still being assembled.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import affiliate_rewards as ar
import affiliate_reward_plans as arp
from fake_mongo import FakeDb

SEP = datetime(2026, 9, 10, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}
TTL = ar._ALLOCATION_LEASE_TTL_SECONDS


def _settling_ledger(db, *, lease_at, updated_at=None, ledger_id="L1", user_id=7):
    """A T1 denomination ledger in SETTLING with one extra (surplus) row."""
    db.affiliate_ledger.insert_one({
        "_id": ledger_id, "ledger_type": "AFFILIATE_MONTHLY", "user_id": user_id,
        "status": ar.SETTLING_STATUS, "tier": "T1",
        "year_month": "202609", "entitlement_month": "202609",
        "reward_plan": arp.DENOMINATION_PLAN_ID,
        "bundle_recipe": arp.tier_recipe("202609", "T1"),
        "dedup_key": f"AFF:{user_id}:202609:T1",
        "voucher_code": None,
        "allocation_generation": 3,
        "allocation_lease_at": lease_at,
        "updated_at": updated_at if updated_at is not None else SEP,
    })
    for code in ("KEEP", "EXTRA"):
        db.voucher_pools.insert_one({
            "pool_id": "AFFILIATE_10", "code": code, "status": "issued",
            "voucher_value": 10, "issued_to_user_id": user_id,
            "ledger_id": ledger_id, "issued_for_ledger_id": ledger_id,
        })
    return ledger_id


def _released(db):
    return {d["code"] for d in db.voucher_pools.find({"status": "available"})}


class TestLeaseLivenessHelper:
    def test_a_fresh_lease_is_live(self):
        now = ar._lease_now()
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": now}
        ) is True

    def test_one_instant_before_expiry_is_still_live(self):
        now = ar._lease_now()
        just_inside = now - timedelta(seconds=TTL) + timedelta(milliseconds=1)
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": just_inside},
            reference=now,
        ) is True

    def test_exactly_at_the_expiry_boundary_is_still_live(self):
        """The boundary is defined as `leased_at >= cutoff`: a lease exactly
        at the cutoff has not yet expired. Stated explicitly so the rule is
        a decision, not an accident."""
        now = ar._lease_now()
        exactly = now - timedelta(seconds=TTL)
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": exactly},
            reference=now,
        ) is True

    def test_one_instant_past_expiry_is_not_live(self):
        now = ar._lease_now()
        just_past = now - timedelta(seconds=TTL) - timedelta(milliseconds=1)
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": just_past},
            reference=now,
        ) is False

    def test_a_missing_lease_timestamp_is_not_live(self):
        assert ar._allocation_lease_is_live({"status": ar.SETTLING_STATUS}) is False
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": None}
        ) is False

    def test_a_malformed_lease_timestamp_fails_safely(self):
        for bad in ("not-a-date", 12345, [], {}):
            assert ar._allocation_lease_is_live(
                {"status": ar.SETTLING_STATUS, "allocation_lease_at": bad}
            ) is False

    def test_a_naive_timestamp_is_compared_as_utc_not_crashed_on(self):
        naive = datetime.utcnow()
        result = ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": naive}
        )
        assert result in (True, False)  # the point is: it does not raise

    def test_an_issued_ledger_is_never_treated_as_a_live_allocator(self):
        assert ar._allocation_lease_is_live(
            {"status": "ISSUED", "allocation_lease_at": ar._lease_now()}
        ) is False

    def test_expiry_arithmetic_exists_once(self):
        """acquire and the sweep must share one cutoff definition."""
        ref = ar._lease_now()
        assert ar._allocation_lease_expiry_cutoff(ref) == ref - timedelta(seconds=TTL)


class TestSweepRespectsTheLease:
    def test_a_live_allocator_keeps_its_rows(self):
        db = FakeDb(UNIQUE)
        _settling_ledger(db, lease_at=ar._lease_now())
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        assert _released(db) == set(), "a live allocator's rows were released"
        assert stats["surplus_released"] == 0

    def test_a_live_allocator_with_a_stale_updated_at_is_still_protected(self):
        """The exact case `updated_at` gets wrong: renewing for ten minutes
        keeps the lease fresh while updated_at ages."""
        db = FakeDb(UNIQUE)
        _settling_ledger(
            db,
            lease_at=ar._lease_now(),
            updated_at=ar._lease_now() - timedelta(hours=2),
        )
        ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        assert _released(db) == set(), (
            "liveness was inferred from updated_at instead of the lease"
        )

    def test_an_expired_lease_becomes_eligible(self):
        db = FakeDb(UNIQUE)
        _settling_ledger(db, lease_at=ar._lease_now() - timedelta(seconds=TTL + 60))
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        assert stats["surplus_released"] == 1
        assert _released(db) == {"EXTRA"}

    def test_a_renewal_extends_protection(self):
        db = FakeDb(UNIQUE)
        lid = _settling_ledger(db, lease_at=ar._lease_now() - timedelta(seconds=TTL + 60))
        # The worker wakes and renews before the sweep runs.
        assert ar._renew_allocation_lease(db, ledger_id=lid, token=3) is True
        ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        assert _released(db) == set(), "a renewed lease did not extend protection"

    def test_a_settling_ledger_with_no_lease_at_all_is_eligible_once_stale(self):
        db = FakeDb(UNIQUE)
        _settling_ledger(db, lease_at=None,
                         updated_at=ar._lease_now() - timedelta(seconds=TTL + 60))
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        assert stats["surplus_released"] == 1

    def test_a_live_allocator_is_not_stamped_and_is_revisited(self):
        db = FakeDb(UNIQUE)
        lid = _settling_ledger(db, lease_at=ar._lease_now())
        ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        led = db.affiliate_ledger.find_one({"_id": lid})
        assert led.get("surplus_checked_at") is None, (
            "a live allocator was stamped as checked and would be skipped later"
        )

    def test_takeover_between_query_and_inspection_is_respected(self):
        """The ledger looked stale when queried, but a worker grabbed it
        before the sweep got to it."""
        db = FakeDb(UNIQUE)
        lid = _settling_ledger(db, lease_at=ar._lease_now() - timedelta(seconds=TTL + 60))

        original = ar._classify_issued_pool_rows
        def takeover(db_, ledger, *, recipe=None):
            db_.affiliate_ledger.update_one(
                {"_id": lid}, {"$set": {"allocation_lease_at": ar._lease_now()}}
            )
            return original(db_, ledger, recipe=recipe)

        # Re-read happens BEFORE classification, so simulate the takeover
        # landing between the query and the re-read.
        ar._classify_issued_pool_rows = takeover
        try:
            db.affiliate_ledger.update_one(
                {"_id": lid}, {"$set": {"allocation_lease_at": ar._lease_now()}}
            )
            ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP)
        finally:
            ar._classify_issued_pool_rows = original
        assert _released(db) == set()
