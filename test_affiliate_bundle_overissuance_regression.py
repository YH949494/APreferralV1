"""Regression coverage for the affiliate voucher over-issuance audit.

Bundle sizes are read from ``affiliate_rewards.AFFILIATE_REWARD_BUNDLES``
(the single source of truth — see that dict's docstring/comment trail) so
this suite never hardcodes tier counts independently of production config:
  T1=2, T2=3, T3=5, T4=3, T5=5 as of this writing.

Covers, per tier:
  - 0/max already issued -> claim issues exactly `max`
  - partial (1/max) already issued -> a fresh claim/reconcile pass never
    bolts a second full bundle on top; the low-level claim functions only
    ever top up the exact shortfall
  - exact max already issued -> no additional voucher claimed
  - already over max (historical over-issued ledger) -> no additional
    voucher claimed, and the hard per-ledger invariant guard blocks any
    further claim attempt outright
  - repeated retry -> stays at max, never grows
  - two concurrent claim attempts on the same ledger -> cannot exceed max

Also reproduces the production T3 symptom (a ledger accumulating more
voucher_pools rows than its tier's configured bundle size) as a targeted
regression test, and exercises the read-only audit script
(affiliate_voucher_overissuance_audit.py) against a synthetic over-issued
ledger.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest

import fake_mongo
import affiliate_rewards as ar
import affiliate_voucher_overissuance_audit as audit

NOW = datetime(2026, 8, 20, tzinfo=timezone.utc)
TIERS = list(ar.AFFILIATE_REWARD_BUNDLES.keys())


def _db():
    return fake_mongo.FakeDb({
        "voucher_pools": [("pool_id", "code")],
        "affiliate_ledger": [("dedup_key",)],
    })


def _seed_pool(db, *, tier: str, count: int, prefix: str = "AVAIL"):
    for i in range(count):
        db.voucher_pools.insert_one({"pool_id": tier, "code": f"{prefix}-{tier}-{i}", "status": "available"})


def _issued_row(db, *, tier: str, code: str, ledger_id, user_id: int):
    db.voucher_pools.insert_one({
        "pool_id": tier,
        "code": code,
        "status": "issued",
        "issued_to": user_id,
        "issued_to_user_id": user_id,
        "issued_at": NOW,
        "ledger_id": ledger_id,
        "issued_for_ledger_id": str(ledger_id),
    })


def _monthly_ledger(db, *, tier: str, user_id=900001, year_month="202608", status="PENDING_MANUAL", risk_flags=None):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY",
        "user_id": user_id,
        "year_month": year_month,
        "tier": tier,
        "pool_id": tier,
        "status": status,
        "dedup_key": f"AFF:{user_id}:{year_month}:{tier}",
        "voucher_code": None,
        "created_at": NOW,
        "risk_flags": list(risk_flags or []),
    }
    ledger_id = db.affiliate_ledger.insert_one(doc).inserted_id
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _issued_count(db, tier: str) -> int:
    return db.voucher_pools.count_documents({"pool_id": tier, "status": "issued"})


class TestZeroToMaxIssuesExactlyMax:
    @pytest.mark.parametrize("tier", TIERS)
    def test_fresh_ledger_claims_exactly_the_configured_bundle(self, tier):
        required = ar.AFFILIATE_REWARD_BUNDLES[tier]["voucher_count"]
        db = _db()
        _seed_pool(db, tier=tier, count=required + 5)
        ledger = _monthly_ledger(db, tier=tier, status="APPROVED")

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=NOW)

        assert result["status"] == "ISSUED"
        assert result["voucher_count"] == required
        assert _issued_count(db, tier) == required


class TestPartialNeverBecomesDoubleClaim:
    @pytest.mark.parametrize("tier", TIERS)
    def test_one_of_max_already_issued_never_gets_a_fresh_full_bundle_on_top(self, tier):
        """Reproduces the production failure shape: N/required already
        issued (e.g. from a crashed prior attempt), plenty of stock still
        available. A retry must never claim a fresh `required`-sized bundle
        on top of the stray row(s) — the ledger stays pending for manual
        review instead of silently growing past its entitlement.
        """
        required = ar.AFFILIATE_REWARD_BUNDLES[tier]["voucher_count"]
        db = _db()
        ledger = _monthly_ledger(db, tier=tier)
        _issued_row(db, tier=tier, code=f"STRAY-{tier}-0", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _seed_pool(db, tier=tier, count=required + 5)

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=NOW)

        assert result["status"] != "ISSUED"
        assert _issued_count(db, tier) == 1
        assert _issued_count(db, tier) <= required


class TestExactMaxIssuesNothingMore:
    @pytest.mark.parametrize("tier", TIERS)
    def test_complete_bundle_reconciles_without_consuming_more_inventory(self, tier):
        required = ar.AFFILIATE_REWARD_BUNDLES[tier]["voucher_count"]
        db = _db()
        ledger = _monthly_ledger(db, tier=tier, risk_flags=["pool_empty"])
        for i in range(required):
            _issued_row(db, tier=tier, code=f"BUN-{tier}-{i}", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _seed_pool(db, tier=tier, count=required)  # extra stock must stay untouched

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=NOW)

        assert result["status"] == "ISSUED"
        assert result["voucher_count"] == required
        assert _issued_count(db, tier) == required
        assert db.voucher_pools.count_documents({"pool_id": tier, "status": "available"}) == required


class TestAlreadyOverMaxNeverClaimsMore:
    @pytest.mark.parametrize("tier", TIERS)
    def test_historical_over_issued_ledger_claims_nothing_further(self, tier):
        required = ar.AFFILIATE_REWARD_BUNDLES[tier]["voucher_count"]
        db = _db()
        ledger = _monthly_ledger(db, tier=tier)
        for i in range(required + 2):  # already over-issued by 2
            _issued_row(db, tier=tier, code=f"OVER-{tier}-{i}", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _seed_pool(db, tier=tier, count=required + 5)

        result = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=NOW)

        assert _issued_count(db, tier) == required + 2
        # The low-level claim guard also refuses outright if ever invoked
        # directly against an already-over-budget ledger.
        claimed = ar._claim_affiliate_bundle_from_pool(
            db, pool_id=tier, ledger_id=ledger["_id"], user_id=ledger["user_id"], now_utc=NOW, voucher_count=required,
        )
        assert claimed is None
        assert _issued_count(db, tier) == required + 2


class TestRepeatedRetryStaysAtMax:
    @pytest.mark.parametrize("tier", TIERS)
    def test_repeated_retries_never_grow_past_max(self, tier):
        required = ar.AFFILIATE_REWARD_BUNDLES[tier]["voucher_count"]
        db = _db()
        _seed_pool(db, tier=tier, count=required + 5)
        ledger = _monthly_ledger(db, tier=tier, status="APPROVED")

        first = ar._issue_affiliate_ledger_from_pool(db, ledger=ledger, now_utc=NOW)
        assert first["status"] == "ISSUED"

        for _ in range(3):
            latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            again = ar._issue_affiliate_ledger_from_pool(db, ledger=latest, now_utc=NOW)
            assert again["status"] == "ISSUED"
            assert again["voucher_count"] == required
            assert _issued_count(db, tier) == required


class TestConcurrentClaimsCannotExceedMax:
    @pytest.mark.parametrize("tier", TIERS)
    def test_two_concurrent_claim_attempts_cannot_double_issue(self, tier):
        required = ar.AFFILIATE_REWARD_BUNDLES[tier]["voucher_count"]
        db = _db()
        _seed_pool(db, tier=tier, count=required * 3)
        ledger = _monthly_ledger(db, tier=tier, status="APPROVED")

        barrier = threading.Barrier(2)
        results = [None, None]
        errors = []

        def worker(idx):
            try:
                barrier.wait(timeout=5)
                fresh = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
                results[idx] = ar._issue_affiliate_ledger_from_pool(db, ledger=fresh, now_utc=NOW)
            except Exception as exc:  # pragma: no cover - surfaced via assertion below
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=(0,))
        t2 = threading.Thread(target=worker, args=(1,))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert not errors, f"worker thread(s) raised: {errors}"
        # Exactly one worker's SETTLING transition wins the atomic
        # find_one_and_update race; the other either sees SETTLING_PROCEED
        # (no-op, waits for the winner's result) or a benign non-issued
        # status. The invariant that must hold regardless of interleaving:
        # never more than `required` issued voucher rows for this ledger.
        assert _issued_count(db, tier) == required
        final = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert final["status"] == "ISSUED"
        assert final["voucher_count"] == required


class TestProductionT3RegressionShape:
    def test_t3_ledger_with_stray_partial_row_never_exceeds_configured_bundle(self):
        """Direct analogue of the production evidence: a T3 ledger with
        voucher_pools rows already attached to it. Whatever the historical
        count was, a retry today must never push it past
        AFFILIATE_REWARD_BUNDLES['T3']['voucher_count'] (currently 5).
        """
        required = ar.AFFILIATE_REWARD_BUNDLES["T3"]["voucher_count"]
        db = _db()
        ledger = _monthly_ledger(db, tier="T3", user_id=555555)
        # Simulate the pre-fix crash scenario: 2 stray issued rows already
        # linked to the ledger, nothing recorded on the ledger doc itself.
        _issued_row(db, tier="T3", code="STRAY-T3-0", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _issued_row(db, tier="T3", code="STRAY-T3-1", ledger_id=ledger["_id"], user_id=ledger["user_id"])
        _seed_pool(db, tier="T3", count=required + 5)

        for _ in range(3):
            latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            ar._issue_affiliate_ledger_from_pool(db, ledger=latest, now_utc=NOW)
            assert _issued_count(db, "T3") <= required

        # Never silently "completes" a mismatched partial bundle either —
        # it must stay parked for manual review, not flip to ISSUED with a
        # wrong voucher_count.
        final = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        assert final["status"] != "ISSUED"


class TestOverIssuanceAuditScript:
    def test_audit_flags_a_synthetic_over_issued_ledger_and_leaves_data_untouched(self):
        required = ar.AFFILIATE_REWARD_BUNDLES["T3"]["voucher_count"]
        db = _db()
        ledger = _monthly_ledger(db, tier="T3", user_id=777777, status="ISSUED")
        codes = [f"AUDIT-T3-{i}" for i in range(required + 2)]
        for code in codes:
            _issued_row(db, tier="T3", code=code, ledger_id=ledger["_id"], user_id=ledger["user_id"])
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"vouchers": [{"code": c, "value": 10} for c in codes[:required]], "voucher_count": required}},
        )

        before_snapshot = list(db.voucher_pools.find({}))
        report = audit.build_report(db, tier_filter="T3")

        assert report["over_issued_ledger_count"] == 1
        finding = report["findings"][0]
        assert finding["ledger_id"] == str(ledger["_id"])
        assert finding["expected_count"] == required
        assert finding["actual_count"] == required + 2
        assert finding["excess_count"] == 2
        assert set(finding["excess_voucher_codes"]) == set(codes[required:])
        # Read-only: nothing in voucher_pools changed shape/count/status.
        after_snapshot = list(db.voucher_pools.find({}))
        assert len(before_snapshot) == len(after_snapshot)
        assert {r["code"] for r in after_snapshot} == {r["code"] for r in before_snapshot}

    def test_audit_reports_nothing_for_correctly_sized_bundles(self):
        db = _db()
        ledger = _monthly_ledger(db, tier="T3", user_id=888888, status="ISSUED")
        for i in range(ar.AFFILIATE_REWARD_BUNDLES["T3"]["voucher_count"]):
            _issued_row(db, tier="T3", code=f"OK-T3-{i}", ledger_id=ledger["_id"], user_id=ledger["user_id"])

        report = audit.build_report(db, tier_filter="T3")

        assert report["over_issued_ledger_count"] == 0
        assert report["findings"] == []
