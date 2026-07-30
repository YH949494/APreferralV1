"""Follow-up production-safety tests for the affiliate voucher batch
feature, covering the four risks audited after the initial rollout:

1. Legacy undated-voucher fallback must not bypass a scheduled batch.
2. An affiliate ledger's entitlement month must be pinned to exactly one
   voucher source (a specific batch, or transitionally the legacy pool)
   and never drift onto a later batch.
3. Batch uploads must have an authoritative lifecycle (staging/ready/
   failed) so an interrupted upload is recoverable and never claimable.
4. The batch document — not a denormalized voucher-row field — must be
   the authority for distribution_disabled/upload_status.
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


def _create(db, *, pool_id="T1", name="Batch", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00",
            codes=None, now=None, notes=None):
    return avb.create_batch(
        db,
        admin_identity="admin1",
        batch_name=name,
        pool_id=pool_id,
        starts_at_local=starts,
        ends_at_local=ends,
        timezone_name="Asia/Kuala_Lumpur",
        codes=codes if codes is not None else ["A1", "A2", "A3"],
        notes=notes,
        now_utc=now or datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


def _monthly_ledger(db, *, user_id=1, tier="T1", year_month="202607", now_utc=None):
    now_utc = now_utc or datetime.now(timezone.utc)
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY",
        "user_id": user_id,
        "year_month": year_month,
        "tier": tier,
        "pool_id": tier,
        "status": "APPROVED",
        "dedup_key": f"AFF:{user_id}:{year_month}:{tier}",
        "voucher_code": None,
        "created_at": now_utc,
        "risk_flags": [],
    }
    ledger_id = db.affiliate_ledger.insert_one(doc).inserted_id
    return db.affiliate_ledger.find_one({"_id": ledger_id})


# ---------------------------------------------------------------------------
# Risk 1: legacy fallback must not bypass scheduled batches
# ---------------------------------------------------------------------------

class TestLegacyFallbackPolicy:
    def test_legacy_works_before_scheduled_batch_mode_begins(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY1", "status": "available"})
        # No T1 batch has ever been created — pure pre-feature state.
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=datetime.now(timezone.utc))
        assert voucher is not None
        assert voucher["code"] == "LEGACY1"

    def test_legacy_blocked_once_scheduled_batch_mode_begins(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY1", "status": "available"})
        # A T1 batch existed (now expired) — the tier has permanently
        # entered scheduled-batch mode, even though no batch is active now.
        _create(db, starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["USED1"])
        after_batch_expired = datetime(2026, 10, 1, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=after_batch_expired)
        assert voucher is None
        # Legacy row must still exist untouched — never deleted.
        assert db.voucher_pools.count_documents({"code": "LEGACY1", "status": "available"}) == 1

    def test_active_batch_empty_does_not_fall_back_to_legacy(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY1", "status": "available"})
        res = _create(db, codes=["ONLYCODE"])
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        # Exhaust the active batch.
        first = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert first is not None
        # A second claim must NOT reach for the legacy code — the active
        # batch is empty, that's pool-empty, not a legacy-eligible gap.
        second = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L2", user_id=2, now_utc=during_window)
        assert second is None
        assert db.voucher_pools.find_one({"code": "LEGACY1"})["status"] == "available"

    def test_future_batch_uploaded_early_does_not_block_current_legacy_stock(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY1", "status": "available"})
        # Admin uploads NEXT month's batch early.
        _create(db, name="Sept batch", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["SEP1"])
        # "Now" is still in August, before that batch's window starts —
        # legacy must remain usable.
        still_before_batch_starts = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=still_before_batch_starts)
        assert voucher is not None
        assert voucher["code"] == "LEGACY1"

    def test_dashboard_reports_legacy_fallback_status_per_tier(self):
        db = _db()
        result_before = avb.list_batches(db, pool_id="T1", now_utc=datetime(2026, 7, 15, tzinfo=timezone.utc))
        assert result_before["legacy_fallback"] == [{"pool_id": "T1", "entered_scheduled_mode": False, "legacy_fallback_allowed": True}]

        _create(db, starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00")
        result_after = avb.list_batches(db, pool_id="T1", now_utc=datetime(2026, 10, 1, tzinfo=timezone.utc))
        assert result_after["legacy_fallback"] == [{"pool_id": "T1", "entered_scheduled_mode": True, "legacy_fallback_allowed": False}]


# ---------------------------------------------------------------------------
# Risk 2: entitlement-to-batch alignment
# ---------------------------------------------------------------------------

class TestEntitlementBatchAlignment:
    def test_july_ledger_cannot_claim_august_batch(self):
        db = _db()
        august = _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        # Evaluate late, after the August batch is already active. As of
        # the July *entitlement period* (not "now"), no batch existed yet
        # for T1 at all, so this ledger resolves to the transitional legacy
        # bucket — it must never resolve to (or claim from) August.
        now_utc = datetime(2026, 8, 15, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        assert resolved["target_mode"] == "legacy"
        assert resolved.get("target_batch_id") != avb._as_object_id(august["batch"]["batch_id"])

        vouchers = ar._claim_affiliate_bundle_from_pool(
            db, pool_id="T1", ledger_id=ledger["_id"], user_id=1, now_utc=now_utc, voucher_count=1, legacy_only=True,
        )
        # No legacy codes exist either — must come back empty, never AUG1.
        assert vouchers is None
        assert db.voucher_pools.find_one({"code": "AUG1"})["status"] == "available"

    def test_july_ledger_receives_only_the_july_target_batch(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 15, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        assert resolved["target_mode"] == "batch"
        assert str(resolved["target_batch_id"]) == july["batch"]["batch_id"]

        vouchers, reason = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="T1", ledger_id=ledger["_id"],
            user_id=1, now_utc=now_utc, voucher_count=1,
        )
        assert reason is None
        assert vouchers[0]["code"] == "JUL1"

    def test_missing_july_batch_does_not_fall_through_to_august(self):
        db = _db()
        # Scheduled-batch mode for T1 began in June — before which no July
        # batch exists. That's a real gap (drift risk), not a legacy case,
        # and must never fall through to the August batch either.
        _create(db, name="June T1", starts="2026-06-01 00:00:00", ends="2026-07-01 00:00:00", codes=["JUN1"])
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 8, 15, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        assert resolved.get("target_mode") is None
        assert resolved.get("target_batch_id") is None

    def test_exhausted_july_batch_stays_unresolved_to_manual_review(self):
        db = _db()
        _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        assert resolved["target_mode"] == "batch"
        # Exhaust it.
        ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="OTHER", user_id=99, now_utc=now_utc)

        vouchers, reason = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="T1", ledger_id=ledger["_id"],
            user_id=1, now_utc=now_utc, voucher_count=1,
        )
        assert vouchers is None
        assert reason == "target_batch_empty"

    def test_disabled_july_batch_stays_unresolved_to_manual_review(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        avb.set_batch_distribution_disabled(db, july["batch"]["batch_id"], admin_identity="admin1", disabled=True, now_utc=now_utc)

        vouchers, reason = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="T1", ledger_id=ledger["_id"],
            user_id=1, now_utc=now_utc, voucher_count=1,
        )
        assert vouchers is None
        assert reason == "target_batch_disabled"

    def test_retry_of_same_ledger_does_not_change_target_batch(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1", "JUL2"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)

        first_resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        first_target = first_resolved["target_batch_id"]

        # A brand-new, later T1 batch shows up before the retry.
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        later_now = datetime(2026, 8, 20, tzinfo=timezone.utc)
        second_resolved = ar._resolve_monthly_ledger_target(db, db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=later_now)

        assert second_resolved["target_batch_id"] == first_target
        assert str(second_resolved["target_batch_id"]) == july["batch"]["batch_id"]

    def test_concurrent_retries_cannot_issue_twice(self):
        db = _db()
        _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        batch_id = resolved["target_batch_id"]

        first, reason1 = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=batch_id, pool_id="T1", ledger_id=ledger["_id"], user_id=1, now_utc=now_utc, voucher_count=1,
        )
        second, reason2 = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=batch_id, pool_id="T1", ledger_id=ledger["_id"], user_id=1, now_utc=now_utc, voucher_count=1,
        )
        assert first is not None
        assert second is None
        assert reason2 == "target_batch_empty"
        assert db.voucher_pools.count_documents({"code": "JUL1", "status": "issued"}) == 1

    def test_already_issued_monthly_ledger_untouched_by_resolution(self):
        db = _db()
        _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"status": "ISSUED", "voucher_code": "JUL1"}},
        )
        issued_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        # Re-resolving an already-issued ledger must be a no-op (target
        # already pinned) — it must never re-point at a different batch.
        re_resolved = ar._resolve_monthly_ledger_target(db, issued_ledger, now_utc=datetime(2026, 9, 1, tzinfo=timezone.utc))
        assert re_resolved["target_batch_id"] == resolved["target_batch_id"]
        assert re_resolved["status"] == "ISSUED"
        assert re_resolved["voucher_code"] == "JUL1"


# ---------------------------------------------------------------------------
# Final pre-deploy verification: a July entitlement must match only a batch
# that fully contains July — never a batch that merely overlaps part of it.
# ---------------------------------------------------------------------------

JULY_START, JULY_END = ar._month_window_from_yyyymm("202607")


class TestFullMonthContainmentMatching:
    def test_exact_full_month_batch_matches(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        matches = ar._find_batches_for_period(db, pool_id="T1", period_start_utc=JULY_START, period_end_utc=JULY_END)
        assert len(matches) == 1
        assert str(matches[0]["_id"]) == july["batch"]["batch_id"]

    def test_wider_batch_covering_full_month_matches(self):
        db = _db()
        wide = _create(db, name="Wide", starts="2026-06-30 00:00:00", ends="2026-08-02 00:00:00", codes=["W1"])
        matches = ar._find_batches_for_period(db, pool_id="T1", period_start_utc=JULY_START, period_end_utc=JULY_END)
        assert len(matches) == 1
        assert str(matches[0]["_id"]) == wide["batch"]["batch_id"]

    def test_batch_starting_after_month_start_does_not_match(self):
        db = _db()
        _create(db, name="Late start", starts="2026-07-15 00:00:00", ends="2026-08-20 00:00:00", codes=["LS1"])
        matches = ar._find_batches_for_period(db, pool_id="T1", period_start_utc=JULY_START, period_end_utc=JULY_END)
        assert matches == []

    def test_batch_ending_before_month_end_does_not_match(self):
        db = _db()
        _create(db, name="Early end", starts="2026-07-01 00:00:00", ends="2026-07-25 00:00:00", codes=["EE1"])
        matches = ar._find_batches_for_period(db, pool_id="T1", period_start_utc=JULY_START, period_end_utc=JULY_END)
        assert matches == []

    def test_partial_cross_month_overlap_does_not_match(self):
        db = _db()
        _create(db, name="Cross-month", starts="2026-07-20 00:00:00", ends="2026-08-20 00:00:00", codes=["CM1"])
        matches = ar._find_batches_for_period(db, pool_id="T1", period_start_utc=JULY_START, period_end_utc=JULY_END)
        assert matches == []

    def test_adjacent_next_month_batch_does_not_match_prior_month(self):
        db = _db()
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        matches = ar._find_batches_for_period(db, pool_id="T1", period_start_utc=JULY_START, period_end_utc=JULY_END)
        assert matches == []

    def test_july_entitlement_cannot_receive_august_batch_via_resolution(self):
        db = _db()
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        # No July-covering batch exists and the tier had not entered
        # scheduled mode as of July (August batch starts after July) —
        # this is the legitimate transitional-legacy case, never August.
        assert resolved.get("target_mode") in (None, "legacy")
        if resolved.get("target_batch_id") is not None:
            pytest.fail("must never resolve to the August batch")

    def test_missing_valid_july_batch_returns_unresolved_when_scheduled_mode_active(self):
        db = _db()
        _create(db, name="June T1", starts="2026-06-01 00:00:00", ends="2026-07-01 00:00:00", codes=["JUN1"])
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert resolved.get("target_mode") is None
        assert resolved.get("target_batch_id") is None

    def test_two_full_containment_matches_return_ambiguous_manual_review(self, caplog):
        db = _db()
        # Overlap protection at creation time should prevent this in
        # practice; simulate it directly at the data layer to prove the
        # resolver never guesses between conflicting matches.
        first = _create(db, name="First", starts="2026-06-01 00:00:00", ends="2026-09-01 00:00:00", codes=["F1"])
        db.affiliate_voucher_batches.insert_one({
            "batch_name": "Second",
            "pool_id": "T1",
            "starts_at": datetime(2026, 6, 15, tzinfo=timezone.utc),
            "ends_at": datetime(2026, 8, 15, tzinfo=timezone.utc),
            "uploaded_count": 1,
            "available_count": 1,
            "issued_count": 0,
            "distribution_disabled": False,
            "upload_status": "ready",
        })
        ledger = _monthly_ledger(db, year_month="202607")
        with caplog.at_level("ERROR"):
            resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=datetime(2026, 7, 15, tzinfo=timezone.utc))
        assert resolved.get("target_mode") is None
        assert resolved.get("target_batch_id") is None
        assert any("TARGET_BATCH_AMBIGUOUS" in rec.message for rec in caplog.records)

    def test_disabled_valid_july_batch_remains_target_no_fallthrough(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        avb.set_batch_distribution_disabled(db, july["batch"]["batch_id"], admin_identity="admin1", disabled=True, now_utc=now_utc)

        # Re-resolving later (even after August is active) must not move
        # off the disabled July target.
        re_resolved = ar._resolve_monthly_ledger_target(
            db, db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc)
        )
        assert re_resolved["target_batch_id"] == resolved["target_batch_id"]

        vouchers, reason = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=re_resolved["target_batch_id"], pool_id="T1", ledger_id=ledger["_id"],
            user_id=1, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc), voucher_count=1,
        )
        assert vouchers is None
        assert reason == "target_batch_disabled"
        assert db.voucher_pools.find_one({"code": "AUG1"})["status"] == "available"

    def test_exhausted_valid_july_batch_remains_target_no_fallthrough(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        _create(db, name="August T1", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["AUG1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        # Exhaust July's only code via another ledger.
        ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="OTHER", user_id=99, now_utc=now_utc)

        vouchers, reason = ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="T1", ledger_id=ledger["_id"],
            user_id=1, now_utc=datetime(2026, 7, 25, tzinfo=timezone.utc), voucher_count=1,
        )
        assert vouchers is None
        assert reason == "target_batch_empty"
        assert db.voucher_pools.find_one({"code": "AUG1"})["status"] == "available"

    def test_persisted_target_batch_id_never_replaced_on_retry(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        first = ar._resolve_monthly_ledger_target(db, ledger, now_utc=datetime(2026, 7, 5, tzinfo=timezone.utc))
        # A wider "better" match shows up later — must never switch to it.
        _create(db, name="Wider replacement attempt", pool_id="T2", starts="2026-06-01 00:00:00", ends="2026-09-01 00:00:00", codes=["WIDE1"])
        second = ar._resolve_monthly_ledger_target(
            db, db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=datetime(2026, 7, 20, tzinfo=timezone.utc)
        )
        assert second["target_batch_id"] == first["target_batch_id"]
        assert str(second["target_batch_id"]) == july["batch"]["batch_id"]

    def test_kl_month_boundaries_convert_correctly_to_utc(self):
        start_utc, end_utc = ar._month_window_from_yyyymm("202607")
        assert start_utc == datetime(2026, 6, 30, 16, 0, tzinfo=timezone.utc)  # 2026-07-01 00:00 KL
        assert end_utc == datetime(2026, 7, 31, 16, 0, tzinfo=timezone.utc)    # 2026-08-01 00:00 KL

    def test_already_issued_ledger_unaffected_by_containment_fix(self):
        db = _db()
        _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]}, {"$set": {"status": "ISSUED", "voucher_code": "JUL1"}},
        )
        issued = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        re_resolved = ar._resolve_monthly_ledger_target(db, issued, now_utc=datetime(2026, 9, 1, tzinfo=timezone.utc))
        assert re_resolved == issued  # untouched, byte-for-byte


# ---------------------------------------------------------------------------
# Risk 3: upload lifecycle
# ---------------------------------------------------------------------------

class TestUploadLifecycle:
    def test_staging_batch_cannot_distribute(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = avb._as_object_id(res["batch"]["batch_id"])
        db.affiliate_voucher_batches.update_one({"_id": batch_id}, {"$set": {"upload_status": "staging"}})
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is None

    def test_failed_batch_cannot_distribute(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = avb._as_object_id(res["batch"]["batch_id"])
        db.affiliate_voucher_batches.update_one({"_id": batch_id}, {"$set": {"upload_status": "failed"}})
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is None

    def test_ready_batch_can_distribute(self):
        db = _db()
        res = _create(db, codes=["A1"])
        assert res["batch"]["upload_status"] == "ready"
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None

    def test_crash_after_partial_insert_leaves_non_claimable_staging_batch(self):
        db = _db()
        real_insert_one = db.voucher_pools.insert_one
        call_count = {"n": 0}

        def crash_after_first(doc):
            call_count["n"] += 1
            if call_count["n"] == 2:
                # Simulate the process dying here — no exception handling
                # runs in the caller at all (unlike the RuntimeError case,
                # which at least gets to mark the batch "failed"). The
                # batch is left exactly as create_batch wrote it initially:
                # upload_status="staging".
                raise SystemExit("simulated hard crash")
            return real_insert_one(doc)

        db.voucher_pools.insert_one = crash_after_first
        try:
            try:
                _create(db, codes=["A1", "A2", "A3"])
            except SystemExit:
                pass
        finally:
            db.voucher_pools.insert_one = real_insert_one

        batch = db.affiliate_voucher_batches.find_one({})
        assert batch["upload_status"] == "staging"
        assert avb.derive_batch_status(batch) == "uploading"
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is None

    def test_reconciliation_repairs_valid_staging_batch(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        # Force it back to "staging" as if a crash happened right after the
        # rows were inserted but before the final "ready" transition.
        avb._as_object_id(batch_id)
        db.affiliate_voucher_batches.update_one(
            {"_id": avb._as_object_id(batch_id)}, {"$set": {"upload_status": "staging"}}
        )
        result = avb.reconcile_batch(db, batch_id, admin_identity="admin1")
        assert result["ok"] is True
        assert result["batch"]["upload_status"] == "ready"
        assert result["batch"]["available_count"] == 2

        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None

    def test_reconciliation_marks_empty_staging_batch_failed(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = avb._as_object_id(res["batch"]["batch_id"])
        db.affiliate_voucher_batches.update_one({"_id": batch_id}, {"$set": {"upload_status": "staging"}})
        db.voucher_pools.delete_one({"code": "A1"})  # simulate: nothing actually landed

        result = avb.reconcile_batch(db, res["batch"]["batch_id"], admin_identity="admin1")
        assert result["ok"] is True
        assert result["batch"]["upload_status"] == "failed"

    def test_dashboard_derives_uploading_and_failed_statuses(self):
        db = _db()
        staging_res = _create(db, name="Staging", codes=["A1"])
        failed_res = _create(db, name="Failed", pool_id="T2", codes=["B1"])
        db.affiliate_voucher_batches.update_one(
            {"_id": avb._as_object_id(staging_res["batch"]["batch_id"])}, {"$set": {"upload_status": "staging"}}
        )
        db.affiliate_voucher_batches.update_one(
            {"_id": avb._as_object_id(failed_res["batch"]["batch_id"])}, {"$set": {"upload_status": "failed"}}
        )
        now_utc = datetime(2026, 8, 15, tzinfo=timezone.utc)
        result = avb.list_batches(db, include_expired=True, now_utc=now_utc)
        by_name = {item["batch_name"]: item["status"] for item in result["items"]}
        assert by_name["Staging"] == "uploading"
        assert by_name["Failed"] == "failed"

    def test_cannot_reenable_a_failed_batch(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = avb._as_object_id(res["batch"]["batch_id"])
        db.affiliate_voucher_batches.update_one(
            {"_id": batch_id}, {"$set": {"upload_status": "failed", "distribution_disabled": True}}
        )
        out = avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="admin1", disabled=False)
        assert out["ok"] is False
        assert out["code"] == "target_batch_failed_cannot_enable"


# ---------------------------------------------------------------------------
# Regression: existing reward mechanics untouched
# ---------------------------------------------------------------------------

class TestRegressionUnaffected:
    def test_bundle_voucher_counts_and_tier_thresholds_unchanged(self):
        assert ar.AFFILIATE_REWARD_BUNDLES["T1"]["voucher_count"] == 2
        assert ar.AFFILIATE_REWARD_BUNDLES["T4"]["voucher_count"] == 3
        assert ar.T1_THRESHOLD > 0 and ar.T4_THRESHOLD > ar.T1_THRESHOLD

    def test_dedup_key_format_unchanged(self):
        db = _db()
        ledger = _monthly_ledger(db, user_id=42, tier="T2", year_month="202607")
        assert ledger["dedup_key"] == "AFF:42:202607:T2"

    def test_welcome_pool_never_affected_by_batch_policy(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "W1", "status": "available"})
        # WELCOME never has batches — legacy fallback always applies,
        # regardless of any T1-T4 batch activity elsewhere.
        _create(db, pool_id="T1", codes=["T1CODE"])
        voucher = ar._claim_voucher_from_pool(db, pool_id="WELCOME", ledger_id="L1", user_id=1, now_utc=datetime.now(timezone.utc))
        assert voucher is not None
        assert voucher["code"] == "W1"

    def test_issued_voucher_row_unchanged_by_target_batch_resolution(self):
        db = _db()
        july = _create(db, name="July T1", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["JUL1"])
        ledger = _monthly_ledger(db, year_month="202607")
        now_utc = datetime(2026, 7, 5, tzinfo=timezone.utc)
        resolved = ar._resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        ar._claim_affiliate_bundle_from_target_batch(
            db, batch_id=resolved["target_batch_id"], pool_id="T1", ledger_id=ledger["_id"], user_id=1, now_utc=now_utc, voucher_count=1,
        )
        issued_row = db.voucher_pools.find_one({"code": "JUL1"})
        assert issued_row["status"] == "issued"
        # Later, well after expiry, re-resolving/re-fetching must not touch it.
        after_expiry = datetime(2026, 9, 1, tzinfo=timezone.utc)
        avb.get_batch_detail(db, july["batch"]["batch_id"], now_utc=after_expiry)
        issued_row_again = db.voucher_pools.find_one({"code": "JUL1"})
        assert issued_row_again == issued_row
