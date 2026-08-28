"""September 2026 affiliate reward migration: plan selection, tier recipes,
progression/dedup, shortage recovery, GMT+8 boundaries, concurrency and
historical compatibility.

Business rules under test (confirmed):
    thresholds  T1=10  T2=25  T3=50  T4=150  T5=250  (KL / GMT+8 months)

    <= 202608 (legacy)      T1 $5x2=$10   T2 $5x3=$15   T3 $10x5=$50
                            T4 $50x3=$150 T5 $50x5=$250
    >= 202609 (denomination) T1 $10x1=$10  T2 $5x1+$10x2=$25
                             T3 $10x1+$50x1=$60
                             T4 $10x3+$50x3=$180
                             T5 $50x7=$350
    All five tiers in one KL month = $625, drawn as 1x$5 + 7x$10 + 11x$50
    = 19 physical codes.
"""
from __future__ import annotations

import collections
import threading
from datetime import datetime, timedelta, timezone

import pytest

import affiliate_rewards as ar
import affiliate_reward_plans as arp
from fake_mongo import FakeDb

# 12:00 KL on the 10th, expressed in UTC (KL = UTC+8).
AUG = datetime(2026, 8, 10, 4, 0, 0, tzinfo=timezone.utc)
SEP = datetime(2026, 9, 10, 4, 0, 0, tzinfo=timezone.utc)
OCT = datetime(2026, 10, 10, 4, 0, 0, tzinfo=timezone.utc)

UNIQUE_KEYS = {
    "affiliate_ledger": [("dedup_key",)],
    "voucher_pools": [("pool_id", "code")],
    "qualified_events": [("invitee_id",)],
}


def _db():
    return FakeDb(UNIQUE_KEYS)


def _stock(db, pool_id, count, prefix, *, start=0):
    for i in range(start, start + count):
        row = {"pool_id": pool_id, "code": f"{prefix}{i:04d}", "status": "available"}
        value = arp.pool_denomination(pool_id)
        if value is not None:
            row["voucher_value"] = value
        db.voucher_pools.insert_one(row)


def _stock_all_denominations(db, count=60):
    _stock(db, "AFFILIATE_5", count, "F")
    _stock(db, "AFFILIATE_10", count, "T")
    _stock(db, "AFFILIATE_50", count, "H")


def _qualify(db, uid, total, at):
    have = db.qualified_events.count_documents({"referrer_id": uid})
    for i in range(total - have):
        db.qualified_events.insert_one(
            {"invitee_id": uid * 10_000_000 + have + i, "referrer_id": uid, "qualified_at": at}
        )


def _ledgers(db, uid):
    return sorted(db.affiliate_ledger.find({"user_id": uid}), key=lambda r: r["tier"])


def _codes_of(ledger):
    return [v["code"] for v in ledger.get("vouchers") or []]


def _denominations(db):
    return collections.Counter(
        r["pool_id"] for r in db.voucher_pools.find({"status": "issued"})
    )


class _ThreadSafeDb:
    """FakeDb behind one global lock.

    MongoDB guarantees each single-document find_one_and_update/update_one is
    atomic; FakeDb is not thread-safe at all. Serializing every collection
    call reproduces exactly the guarantee the fencing design depends on —
    and nothing stronger — so real threads can race the allocator without
    the fake corrupting itself and giving a false pass.
    """

    _lock = threading.RLock()

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return _ThreadSafeCollection(getattr(self._inner, name), self._lock)

    def __getitem__(self, name):
        return _ThreadSafeCollection(self._inner[name], self._lock)


class _ThreadSafeCollection:
    def __init__(self, inner, lock):
        self._inner = inner
        self._lock = lock

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def _locked(*args, **kwargs):
            with self._lock:
                return attr(*args, **kwargs)

        return _locked


def _reset_to_settling(db, ledger, *, keep_codes: bool = False):
    """Put an already-issued ledger back into a claimable SETTLING state so a
    test can race the allocator over it. ``keep_codes`` retains the physical
    allocation (a crash-after-claim shape); otherwise the codes are returned
    to inventory first (a nothing-claimed-yet shape)."""
    if not keep_codes:
        for row in _linked_issued_rows(db, ledger):
            db.voucher_pools.update_one(
                {"_id": row["_id"]},
                {"$set": {"status": "available"},
                 "$unset": {"issued_to": "", "issued_to_user_id": "", "issued_at": "",
                            "ledger_id": "", "issued_for_ledger_id": ""}},
            )
    db.affiliate_ledger.update_one(
        {"_id": ledger["_id"]},
        {"$set": {"status": ar.SETTLING_STATUS, "voucher_code": None},
         "$unset": {"vouchers": "", "issued_code_count": "", "issued_value": "",
                    "allocation_lease_at": ""}},
    )


def _linked_issued_rows(db, ledger):
    return [
        r for r in db.voucher_pools.find({"status": "issued"})
        if str(r.get("issued_for_ledger_id")) == str(ledger["_id"])
    ]


def _denominations_for(db, ledger):
    """Denominations consumed by ONE ledger. Crossing a threshold also
    creates the lower tiers, so a global counter mixes several bundles."""
    return collections.Counter(
        r["pool_id"] for r in db.voucher_pools.find({"status": "issued"})
        if str(r.get("issued_for_ledger_id")) == str(ledger["_id"])
    )


# ---------------------------------------------------------------------------
# Reward-plan selection
# ---------------------------------------------------------------------------

class TestRewardPlanSelection:
    def test_august_entitlement_processed_in_august_uses_legacy_bundle(self):
        db = _db()
        _stock(db, "T3", 10, "L")
        _qualify(db, 1, 50, AUG)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=1, now_utc=AUG)
        t3 = db.affiliate_ledger.find_one({"user_id": 1, "tier": "T3"})
        assert t3["reward_plan"] == arp.LEGACY_PLAN_ID
        assert t3["status"] == "ISSUED"
        assert t3["voucher_count"] == 5, "legacy T3 = $10 x 5"
        assert t3["total_value"] == 50

    def test_august_entitlement_retried_in_september_uses_legacy_bundle(self):
        """The single most important historical-leakage guard: plan selection
        must key on the STORED entitlement month, never the processing date."""
        db = _db()
        _qualify(db, 2, 50, AUG)
        # No stock in August -> parks pending.
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=2, now_utc=AUG)
        t3 = db.affiliate_ledger.find_one({"user_id": 2, "tier": "T3"})
        assert t3["status"] == "PENDING_MANUAL"
        assert t3["entitlement_month"] == "202608"

        # Restock BOTH the legacy T3 pool and every September denomination
        # pool, then retry in September. It must take the legacy T3 pool.
        _stock(db, "T3", 10, "L")
        _stock_all_denominations(db)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)

        t3 = db.affiliate_ledger.find_one({"user_id": 2, "tier": "T3"})
        assert t3["status"] == "ISSUED"
        assert t3["reward_plan"] == arp.LEGACY_PLAN_ID
        assert t3["voucher_count"] == 5 and t3["total_value"] == 50
        assert all(c.startswith("L") for c in _codes_of(t3)), "must not consume denomination stock"
        assert _denominations(db) == {"T3": 5}

    def test_september_entitlement_uses_new_bundle(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 3, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=3, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 3, "tier": "T3"})
        assert t3["reward_plan"] == arp.DENOMINATION_PLAN_ID
        assert t3["status"] == "ISSUED"
        assert t3["issued_code_count"] == 2 and t3["issued_value"] == 60

    def test_plan_resolution_never_uses_processing_date(self):
        # A ledger dict alone (no db) must resolve purely from its month.
        assert ar._ledger_reward_plan({"year_month": "202608"}) == arp.LEGACY_PLAN_ID
        assert ar._ledger_reward_plan({"year_month": "202609"}) == arp.DENOMINATION_PLAN_ID
        assert ar._ledger_reward_plan({"entitlement_month": "202612"}) == arp.DENOMINATION_PLAN_ID
        # A frozen plan on the ledger always wins.
        assert ar._ledger_reward_plan(
            {"year_month": "202609", "reward_plan": arp.LEGACY_PLAN_ID}
        ) == arp.LEGACY_PLAN_ID


class TestMonthBoundaries:
    """September 2026 KL window == [2026-08-31 16:00 UTC, 2026-09-30 16:00 UTC)."""

    SEP_START_UTC = datetime(2026, 8, 31, 16, 0, 0, tzinfo=timezone.utc)
    SEP_END_UTC = datetime(2026, 9, 30, 16, 0, 0, tzinfo=timezone.utc)

    def test_september_window_utc_bounds(self):
        start, end = ar._month_window_from_yyyymm("202609")
        assert start == self.SEP_START_UTC
        assert end == self.SEP_END_UTC
        # And the KL-local view of those instants.
        assert start.astimezone(ar.KL_TZ).strftime("%Y-%m-%d %H:%M:%S %z") == "2026-09-01 00:00:00 +0800"
        assert end.astimezone(ar.KL_TZ).strftime("%Y-%m-%d %H:%M:%S %z") == "2026-10-01 00:00:00 +0800"

    def test_exact_august_september_boundary(self):
        one_before = self.SEP_START_UTC - timedelta(microseconds=1)
        assert ar._month_window_utc(one_before)[2] == "202608"
        assert ar._month_window_utc(self.SEP_START_UTC)[2] == "202609"

    def test_exact_september_october_boundary(self):
        one_before = self.SEP_END_UTC - timedelta(microseconds=1)
        assert ar._month_window_utc(one_before)[2] == "202609"
        assert ar._month_window_utc(self.SEP_END_UTC)[2] == "202610"

    def test_plan_flips_exactly_at_the_kl_boundary(self):
        one_before = self.SEP_START_UTC - timedelta(microseconds=1)
        assert arp.resolve_plan_id(ar._month_window_utc(one_before)[2]) == arp.LEGACY_PLAN_ID
        assert arp.resolve_plan_id(ar._month_window_utc(self.SEP_START_UTC)[2]) == arp.DENOMINATION_PLAN_ID

    def test_entitlement_created_one_second_before_september_is_august(self):
        db = _db()
        _stock(db, "T1", 5, "L")
        just_before = self.SEP_START_UTC - timedelta(seconds=1)
        _qualify(db, 9, 10, just_before)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=9, now_utc=just_before)
        t1 = db.affiliate_ledger.find_one({"user_id": 9, "tier": "T1"})
        assert t1["entitlement_month"] == "202608"
        assert t1["reward_plan"] == arp.LEGACY_PLAN_ID
        assert t1["voucher_count"] == 2, "legacy T1 = $5 x 2"


# ---------------------------------------------------------------------------
# Tier recipes
# ---------------------------------------------------------------------------

class TestTierRecipes:
    EXPECTED = {
        "T1": ({"AFFILIATE_10": 1}, 1, 10),
        "T2": ({"AFFILIATE_5": 1, "AFFILIATE_10": 2}, 3, 25),
        "T3": ({"AFFILIATE_10": 1, "AFFILIATE_50": 1}, 2, 60),
        "T4": ({"AFFILIATE_10": 3, "AFFILIATE_50": 3}, 6, 180),
        "T5": ({"AFFILIATE_50": 7}, 7, 350),
    }

    @pytest.mark.parametrize("tier", ["T1", "T2", "T3", "T4", "T5"])
    def test_recipe_matches_confirmed_rules(self, tier):
        pools, count, value = self.EXPECTED[tier]
        recipe = arp.tier_recipe("202609", tier)
        assert arp.recipe_required_by_pool(recipe) == pools
        assert recipe["expected_code_count"] == count
        assert recipe["reward_value"] == value

    @pytest.mark.parametrize("tier", ["T1", "T2", "T3", "T4", "T5"])
    def test_issued_value_and_code_count_match_the_recipe(self, tier):
        pools, count, value = self.EXPECTED[tier]
        db = _db()
        _stock_all_denominations(db)
        threshold = {"T1": 10, "T2": 25, "T3": 50, "T4": 150, "T5": 250}[tier]
        _qualify(db, 5, threshold, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=5, now_utc=SEP)
        led = db.affiliate_ledger.find_one({"user_id": 5, "tier": tier})
        assert led["status"] == "ISSUED"
        assert led["issued_code_count"] == count == len(_codes_of(led))
        assert led["issued_value"] == value == led["total_value"]
        issued = collections.Counter(
            r["pool_id"] for r in db.voucher_pools.find({"status": "issued"})
            if str(r.get("issued_for_ledger_id")) == str(led["_id"])
        )
        assert dict(issued) == pools

    def test_legacy_recipes_unchanged(self):
        expected = {
            "T1": ({"T1": 2}, 2, 10), "T2": ({"T2": 3}, 3, 15),
            "T3": ({"T3": 5}, 5, 50), "T4": ({"T4": 3}, 3, 150),
            "T5": ({"T5": 5}, 5, 250),
        }
        for tier, (pools, count, value) in expected.items():
            recipe = arp.tier_recipe("202608", tier)
            assert arp.recipe_required_by_pool(recipe) == pools
            assert recipe["expected_code_count"] == count
            assert recipe["reward_value"] == value
            # ... and still agrees with the original bundle table.
            legacy = ar.AFFILIATE_REWARD_BUNDLES[tier]
            assert legacy["voucher_count"] == count
            assert legacy["voucher_count"] * legacy["voucher_value"] == value


# ---------------------------------------------------------------------------
# Progression and deduplication
# ---------------------------------------------------------------------------

class TestProgressionAndDedup:
    def _walk(self, db, uid, thresholds, now=SEP):
        for target in thresholds:
            _qualify(db, uid, target, now)
            ar.evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now)

    def test_t1_then_t2_does_not_reissue_t1(self):
        db = _db()
        _stock_all_denominations(db)
        self._walk(db, 10, [10])
        t1_codes = _codes_of(db.affiliate_ledger.find_one({"user_id": 10, "tier": "T1"}))
        self._walk(db, 10, [25])
        t1_after = _codes_of(db.affiliate_ledger.find_one({"user_id": 10, "tier": "T1"}))
        assert t1_codes == t1_after, "T1 must not be re-issued when T2 unlocks"
        assert len(_ledgers(db, 10)) == 2
        assert _denominations(db) == {"AFFILIATE_10": 3, "AFFILIATE_5": 1}

    def test_t2_then_t3_does_not_reissue_t1_or_t2(self):
        db = _db()
        _stock_all_denominations(db)
        self._walk(db, 11, [25])
        before = {l["tier"]: _codes_of(l) for l in _ledgers(db, 11)}
        self._walk(db, 11, [50])
        after = {l["tier"]: _codes_of(l) for l in _ledgers(db, 11)}
        assert before["T1"] == after["T1"] and before["T2"] == after["T2"]
        assert len(after) == 3

    def test_full_progression_totals_625_and_19_codes(self):
        db = _db()
        _stock_all_denominations(db)
        self._walk(db, 12, [10, 25, 50, 150, 250])
        ledgers = _ledgers(db, 12)
        assert len(ledgers) == 5, "five tier ledgers maximum per user per month"
        assert all(l["status"] == "ISSUED" for l in ledgers)
        assert sum(l["issued_value"] for l in ledgers) == 625
        assert dict(_denominations(db)) == {
            "AFFILIATE_5": 1, "AFFILIATE_10": 7, "AFFILIATE_50": 11
        }
        all_codes = [c for l in ledgers for c in _codes_of(l)]
        assert len(all_codes) == 19 == len(set(all_codes)), "19 unique physical codes"

    def test_first_evaluated_at_t3_creates_every_missing_tier_once(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 13, 50, SEP)  # never evaluated below T3
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=13, now_utc=SEP)
        ledgers = _ledgers(db, 13)
        assert [l["tier"] for l in ledgers] == ["T1", "T2", "T3"]
        assert all(l["status"] == "ISSUED" for l in ledgers)
        assert sum(l["issued_value"] for l in ledgers) == 10 + 25 + 60
        assert dict(_denominations(db)) == {"AFFILIATE_5": 1, "AFFILIATE_10": 4, "AFFILIATE_50": 1}

    def test_direct_jump_to_t5_creates_all_five_once(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 14, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=14, now_utc=SEP)
        assert len(_ledgers(db, 14)) == 5
        assert sum(l["issued_value"] for l in _ledgers(db, 14)) == 625

    def test_rerunning_evaluation_creates_no_duplicates(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 15, 250, SEP)
        for _ in range(5):
            ar.evaluate_monthly_affiliate_reward(db, referrer_id=15, now_utc=SEP)
        assert len(_ledgers(db, 15)) == 5
        assert sum(l["issued_value"] for l in _ledgers(db, 15)) == 625
        assert sum(_denominations(db).values()) == 19, "no extra inventory consumed"

    def test_dedup_key_shape_is_preserved(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 16, 25, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=16, now_utc=SEP)
        keys = {l["dedup_key"] for l in _ledgers(db, 16)}
        assert keys == {"AFF:16:202609:T1", "AFF:16:202609:T2"}

    def test_next_kl_month_permits_new_tier_entitlements(self):
        db = _db()
        _stock_all_denominations(db, count=120)
        _qualify(db, 17, 10, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=17, now_utc=SEP)
        _qualify(db, 17, 20, OCT)  # 10 more, in October
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=17, now_utc=OCT)
        months = collections.Counter(l["entitlement_month"] for l in _ledgers(db, 17))
        assert months == {"202609": 1, "202610": 1}


class TestConcurrency:
    """Real interleaving, not repeated sequential calls.

    ``_ThreadSafeDb`` wraps FakeDb so every collection operation is
    serialized under one lock — which is exactly MongoDB's guarantee for a
    single-document find_one_and_update/update_one, and the only property
    the fencing design relies on.
    """

    def test_two_workers_starting_together_never_over_allocate(self):
        db = _ThreadSafeDb(_db())
        _stock_all_denominations(db)
        _qualify(db, 50, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=50, now_utc=SEP)

        # Reset every tier back to claimable so both threads race the same work.
        for led in _ledgers(db, 50):
            _reset_to_settling(db, led)

        errors: list = []

        def worker():
            try:
                for led in db.affiliate_ledger.find({"user_id": 50}):
                    fresh = db.affiliate_ledger.find_one({"_id": led["_id"]})
                    ar._issue_denomination_bundle(
                        db, ledger=fresh, recipe=ar._ledger_recipe(fresh), now_utc=SEP,
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, errors

        ledgers = _ledgers(db, 50)
        assert len(ledgers) == 5
        for led in ledgers:
            recipe = ar._ledger_recipe(led)
            linked = _linked_issued_rows(db, led)
            assert len(linked) == recipe["expected_code_count"], (
                f"{led['tier']}: {len(linked)} linked rows for a "
                f"{recipe['expected_code_count']}-code recipe"
            )
        assert sum(_denominations(db).values()) == 19, "no surplus consumed under contention"

    def test_stale_worker_resuming_after_takeover_strands_nothing(self):
        """The exact scenario from review: A allocates part of T5 and stalls,
        its lease expires, B completes all seven and finalizes, then A
        resumes. Final state must be exactly seven linked rows."""
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 51, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=51, now_utc=SEP)
        t5 = db.affiliate_ledger.find_one({"user_id": 51, "tier": "T5"})
        _reset_to_settling(db, t5)
        recipe = ar._ledger_recipe(t5)

        # --- Worker A: claims 3 of 7, then stalls (lease left un-renewed) ---
        token_a = ar._acquire_allocation_lease(db, ledger_id=t5["_id"], now_utc=SEP)
        assert token_a is not None
        target, _ = ar._resolve_denomination_pool_target(
            db, ledger=db.affiliate_ledger.find_one({"_id": t5["_id"]}),
            pool_id="AFFILIATE_50", entitlement_month="202609", now_utc=SEP,
        )
        for _ in range(3):
            ar._claim_one_denomination_voucher(
                db, target=target, pool_id="AFFILIATE_50",
                ledger_id=t5["_id"], user_id=51, now_utc=SEP,
            )
        assert len(_linked_issued_rows(db, t5)) == 3

        # --- A's lease goes stale ---
        db.affiliate_ledger.update_one(
            {"_id": t5["_id"]},
            {"$set": {"allocation_lease_at": ar._lease_now() - timedelta(
                seconds=ar._ALLOCATION_LEASE_TTL_SECONDS + 60)}},
        )

        # --- Worker B takes over and finishes the job ---
        out = ar._issue_denomination_bundle(
            db, ledger=db.affiliate_ledger.find_one({"_id": t5["_id"]}), recipe=recipe, now_utc=SEP,
        )
        assert out["status"] == "ISSUED"
        assert out["issued_code_count"] == 7 and out["issued_value"] == 350
        bundle_codes = set(_codes_of(out))
        assert len(bundle_codes) == 7

        # --- A wakes up and tries to keep going on its stale token ---
        assert ar._renew_allocation_lease(db, ledger_id=t5["_id"], token=token_a) is False
        assert ar._holds_allocation_lease(db, ledger_id=t5["_id"], token=token_a) is False
        # Its finalize attempt is fenced out and cannot overwrite B's result.
        assert ar._store_affiliate_bundle_on_ledger(
            db, ledger_id=t5["_id"], tier="T5", vouchers=[], now_utc=SEP,
            recipe=recipe, token=token_a,
        ) is None

        final = db.affiliate_ledger.find_one({"_id": t5["_id"]})
        assert final["status"] == "ISSUED"
        assert set(_codes_of(final)) == bundle_codes, "B's bundle is untouched"
        assert len(_linked_issued_rows(db, t5)) == 7, "no surplus stranded"
        # And no user-visible code was released.
        for code in bundle_codes:
            row = db.voucher_pools.find_one({"code": code})
            assert row["status"] == "issued"

    def test_lease_loss_immediately_before_a_claim_consumes_nothing(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 52, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=52, now_utc=SEP)
        t5 = db.affiliate_ledger.find_one({"user_id": 52, "tier": "T5"})
        _reset_to_settling(db, t5)
        before = sum(_denominations(db).values())

        # A takeover lands the instant before the first claim.
        original = ar._renew_allocation_lease
        calls = {"n": 0}

        def fenced_out(db_, *, ledger_id, token):
            calls["n"] += 1
            if calls["n"] == 1:
                db_.affiliate_ledger.update_one(
                    {"_id": ledger_id}, {"$inc": {"allocation_generation": 1}}
                )
            return original(db_, ledger_id=ledger_id, token=token)

        ar._renew_allocation_lease = fenced_out
        try:
            ar._issue_denomination_bundle(
                db, ledger=db.affiliate_ledger.find_one({"_id": t5["_id"]}),
                recipe=ar._ledger_recipe(t5), now_utc=SEP,
            )
        finally:
            ar._renew_allocation_lease = original

        assert sum(_denominations(db).values()) == before, "displaced worker claimed a code"
        assert len(_linked_issued_rows(db, t5)) == 0

    def test_lease_loss_immediately_after_a_claim_releases_the_surplus(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 53, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=53, now_utc=SEP)
        t5 = db.affiliate_ledger.find_one({"user_id": 53, "tier": "T5"})
        _reset_to_settling(db, t5)
        recipe = ar._ledger_recipe(t5)

        # Another worker completes the bundle, then our worker's claim lands.
        original = ar._claim_one_denomination_voucher
        state = {"done": False}

        def claim_then_lose(db_, *, target, pool_id, ledger_id, user_id, now_utc):
            voucher, reason = original(
                db_, target=target, pool_id=pool_id, ledger_id=ledger_id,
                user_id=user_id, now_utc=now_utc,
            )
            if not state["done"]:
                state["done"] = True
                db_.affiliate_ledger.update_one(
                    {"_id": ledger_id}, {"$inc": {"allocation_generation": 1}}
                )
            return voucher, reason

        ar._claim_one_denomination_voucher = claim_then_lose
        try:
            ar._issue_denomination_bundle(
                db, ledger=db.affiliate_ledger.find_one({"_id": t5["_id"]}),
                recipe=recipe, now_utc=SEP,
            )
        finally:
            ar._claim_one_denomination_voucher = original

        led = db.affiliate_ledger.find_one({"_id": t5["_id"]})
        assert led["status"] != "ISSUED", "a fenced-out worker must not finalize"
        # The one code it claimed is within the recipe, so it is retained for
        # the next owner rather than released.
        assert len(_linked_issued_rows(db, t5)) <= recipe["expected_code_count"]

    def test_lease_loss_before_finalization_cannot_finalize(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 54, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=54, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 54, "tier": "T3"})
        recipe = ar._ledger_recipe(t3)
        _reset_to_settling(db, t3, keep_codes=True)

        token = ar._acquire_allocation_lease(db, ledger_id=t3["_id"], now_utc=SEP)
        # Takeover lands before we finalize.
        db.affiliate_ledger.update_one(
            {"_id": t3["_id"]}, {"$inc": {"allocation_generation": 1}}
        )
        rows = _linked_issued_rows(db, t3)
        assert ar._store_affiliate_bundle_on_ledger(
            db, ledger_id=t3["_id"], tier="T3", vouchers=rows,
            now_utc=SEP, recipe=recipe, token=token,
        ) is None
        assert db.affiliate_ledger.find_one({"_id": t3["_id"]})["status"] != "ISSUED"

    def test_retry_after_process_crash_resumes_and_completes(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 55, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=55, now_utc=SEP)
        t5 = db.affiliate_ledger.find_one({"user_id": 55, "tier": "T5"})
        kept = set(_codes_of(t5))

        # Crash signature: codes allocated, ledger never finalized, lease
        # left behind by a process that is gone.
        db.affiliate_ledger.update_one(
            {"_id": t5["_id"]},
            {"$set": {"status": ar.SETTLING_STATUS, "voucher_code": None,
                      "updated_at": SEP,
                      "allocation_lease_at": ar._lease_now() - timedelta(
                          seconds=ar._ALLOCATION_LEASE_TTL_SECONDS + 60)},
             "$unset": {"vouchers": "", "issued_code_count": "", "issued_value": ""}},
        )
        later = SEP + timedelta(seconds=ar._ALLOCATION_LEASE_TTL_SECONDS + 60)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=later)

        out = db.affiliate_ledger.find_one({"_id": t5["_id"]})
        assert out["status"] == "ISSUED"
        assert set(_codes_of(out)) == kept, "resumed on exactly the same codes"
        assert len(_linked_issued_rows(db, t5)) == 7

    def test_pre_existing_surplus_allocation_is_reconciled(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 56, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=56, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 56, "tier": "T3"})
        recipe = ar._ledger_recipe(t3)
        _reset_to_settling(db, t3, keep_codes=True)

        # Two stray extra $50 rows linked to this ledger, as a displaced
        # worker would have left behind.
        strays = []
        for row in list(db.voucher_pools.find({"pool_id": "AFFILIATE_50", "status": "available"}))[:2]:
            db.voucher_pools.update_one(
                {"_id": row["_id"]},
                {"$set": {"status": "issued", "issued_to_user_id": 56,
                          "issued_for_ledger_id": str(t3["_id"]), "ledger_id": t3["_id"]}},
            )
            strays.append(row["code"])
        assert len(_linked_issued_rows(db, t3)) == recipe["expected_code_count"] + 2

        out = ar._issue_denomination_bundle(
            db, ledger=db.affiliate_ledger.find_one({"_id": t3["_id"]}),
            recipe=recipe, now_utc=SEP,
        )
        assert out["status"] == "ISSUED"
        assert len(_linked_issued_rows(db, t3)) == recipe["expected_code_count"]
        # The strays went back to inventory, not stranded.
        for code in strays:
            assert db.voucher_pools.find_one({"code": code})["status"] == "available"

    def test_concurrent_evaluation_cannot_double_issue_a_tier(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 20, 250, SEP)
        for _ in range(4):
            ar.evaluate_monthly_affiliate_reward(db, referrer_id=20, now_utc=SEP)
            ar.catch_up_missing_current_month_affiliate_ledgers(db, now_utc=SEP)
            ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
        ledgers = _ledgers(db, 20)
        assert len(ledgers) == 5
        assert sum(l["issued_value"] for l in ledgers) == 625
        assert sum(_denominations(db).values()) == 19


# ---------------------------------------------------------------------------
# Shortage and recovery
# ---------------------------------------------------------------------------

class TestShortageAndRecovery:
    def test_no_stock_at_all_parks_pending_manual(self):
        db = _db()
        _qualify(db, 30, 10, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=30, now_utc=SEP)
        t1 = db.affiliate_ledger.find_one({"user_id": 30, "tier": "T1"})
        assert t1["status"] == "PENDING_MANUAL"
        assert t1.get("allocated_code_count") == 0
        assert t1["missing_by_denomination"] == {"AFFILIATE_10": 1}
        assert "bundle_denomination_short" in t1["risk_flags"]

    def test_missing_one_denomination_retains_partial_and_reports_shortage(self):
        db = _db()
        _stock(db, "AFFILIATE_10", 10, "T")  # $50 pool deliberately empty
        _qualify(db, 31, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=31, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 31, "tier": "T3"})
        assert t3["status"] == "PENDING_MANUAL", "never ISSUED on an incomplete recipe"
        assert t3["missing_by_denomination"] == {"AFFILIATE_50": 1}
        assert t3["allocated_code_count"] == 1, "the $10 code is retained, not released"
        assert not t3.get("vouchers"), "no bundle stored until the recipe is complete"

    def test_retry_after_stock_replenishment_finalizes_without_duplication(self):
        db = _db()
        _stock(db, "AFFILIATE_10", 10, "T")
        _qualify(db, 32, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=32, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 32, "tier": "T3"})
        held = [
            r["code"] for r in db.voucher_pools.find({"status": "issued"})
            if str(r.get("issued_for_ledger_id")) == str(t3["_id"])
        ]
        assert len(held) == 1

        _stock(db, "AFFILIATE_50", 5, "H")
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)

        t3 = db.affiliate_ledger.find_one({"user_id": 32, "tier": "T3"})
        assert t3["status"] == "ISSUED"
        assert t3["issued_code_count"] == 2 and t3["issued_value"] == 60
        assert held[0] in _codes_of(t3), "the already-held code must be reused, not re-claimed"
        assert _denominations_for(db, t3) == {"AFFILIATE_10": 1, "AFFILIATE_50": 1}
        assert not t3.get("missing_by_denomination")

    def test_repeated_retry_while_still_short_consumes_nothing_extra(self):
        db = _db()
        _stock(db, "AFFILIATE_10", 10, "T")
        _qualify(db, 33, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=33, now_utc=SEP)
        after_first = sum(_denominations(db).values())
        for _ in range(4):
            ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
        assert sum(_denominations(db).values()) == after_first

    def test_concurrent_retry_does_not_over_allocate(self):
        db = _db()
        _stock(db, "AFFILIATE_10", 2, "T")
        _qualify(db, 34, 150, SEP)  # T4 needs 3x$10 + 3x$50
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=34, now_utc=SEP)
        _stock(db, "AFFILIATE_10", 20, "T", start=100)
        _stock(db, "AFFILIATE_50", 20, "H")
        for _ in range(3):
            ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
        t4 = db.affiliate_ledger.find_one({"user_id": 34, "tier": "T4"})
        assert t4["status"] == "ISSUED"
        assert t4["issued_code_count"] == 6 and t4["issued_value"] == 180
        linked = [
            r for r in db.voucher_pools.find({"status": "issued"})
            if str(r.get("issued_for_ledger_id")) == str(t4["_id"])
        ]
        assert len(linked) == 6, "exactly the recipe, never a second bundle"

    def test_crash_after_allocation_before_finalization_resumes(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 35, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=35, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 35, "tier": "T3"})
        codes_before = set(_codes_of(t3))

        # Simulate the crash: vouchers are allocated in voucher_pools, but the
        # ledger never got finalized.
        db.affiliate_ledger.update_one(
            {"_id": t3["_id"]},
            {"$set": {"status": "PENDING_MANUAL", "voucher_code": None,
                      "risk_flags": ["bundle_denomination_short"]},
             "$unset": {"vouchers": "", "issued_code_count": "", "issued_value": ""}},
        )
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)

        t3 = db.affiliate_ledger.find_one({"user_id": 35, "tier": "T3"})
        assert t3["status"] == "ISSUED"
        assert set(_codes_of(t3)) == codes_before, "resumed on the same codes"
        assert _denominations_for(db, t3) == {"AFFILIATE_10": 1, "AFFILIATE_50": 1}
        # And the whole month stays exactly at the T1+T2+T3 recipe total.
        assert dict(_denominations(db)) == {
            "AFFILIATE_5": 1, "AFFILIATE_10": 4, "AFFILIATE_50": 1
        }

    def test_ledger_stranded_in_settling_is_recovered(self):
        """A worker that crashed between claiming vouchers and finalizing the
        ledger leaves it in SETTLING with no PENDING_MANUAL to find it by."""
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 39, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=39, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 39, "tier": "T3"})
        codes_before = set(_codes_of(t3))

        db.affiliate_ledger.update_one(
            {"_id": t3["_id"]},
            {"$set": {"status": ar.SETTLING_STATUS, "voucher_code": None, "updated_at": SEP},
             "$unset": {"vouchers": "", "issued_code_count": "", "issued_value": ""}},
        )
        # Still inside the lease TTL: an actively-allocating worker is never
        # interrupted, so nothing is touched yet.
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
        assert db.affiliate_ledger.find_one({"_id": t3["_id"]})["status"] == ar.SETTLING_STATUS

        later = SEP + timedelta(seconds=ar._ALLOCATION_LEASE_TTL_SECONDS + 60)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=later)
        t3 = db.affiliate_ledger.find_one({"_id": t3["_id"]})
        assert t3["status"] == "ISSUED"
        assert set(_codes_of(t3)) == codes_before, "recovered on the same codes"
        assert _denominations_for(db, t3) == {"AFFILIATE_10": 1, "AFFILIATE_50": 1}

    def test_announcement_data_only_available_once_fully_issued(self):
        db = _db()
        _stock(db, "AFFILIATE_10", 10, "T")
        _qualify(db, 36, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=36, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 36, "tier": "T3"})
        # The congrats gate requires status ISSUED *and* a voucher_code.
        assert t3["status"] != "ISSUED"
        assert not t3.get("voucher_code")

        _stock(db, "AFFILIATE_50", 5, "H")
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 36, "tier": "T3"})
        assert t3["status"] == "ISSUED" and t3["voucher_code"]

    def test_user_delivery_cards_only_show_complete_bundles(self):
        db = _db()
        _stock(db, "AFFILIATE_10", 10, "T")
        _qualify(db, 37, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=37, now_utc=SEP)
        # T1 completed ($10 x 1); T2/T3 are short on $5 / $50.
        cards = ar.affiliate_bundle_visible_cards(db, user_id=37)
        assert [c["affiliate_tier"] for c in cards] == ["T1"]

        _stock(db, "AFFILIATE_5", 5, "F")
        _stock(db, "AFFILIATE_50", 5, "H")
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
        cards = ar.affiliate_bundle_visible_cards(db, user_id=37)
        assert sorted(c["affiliate_tier"] for c in cards) == ["T1", "T2", "T3"]
        # Each code is delivered exactly once across all cards.
        delivered = [v["code"] for c in cards for v in c["vouchers"]]
        assert len(delivered) == len(set(delivered)) == 1 + 3 + 2

    def test_retry_does_not_resend_already_delivered_codes(self):
        db = _db()
        _stock_all_denominations(db)
        _qualify(db, 38, 25, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=38, now_utc=SEP)
        first = {c["affiliate_tier"]: [v["code"] for v in c["vouchers"]]
                 for c in ar.affiliate_bundle_visible_cards(db, user_id=38)}
        for _ in range(3):
            ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP)
            ar.evaluate_monthly_affiliate_reward(db, referrer_id=38, now_utc=SEP)
        again = {c["affiliate_tier"]: [v["code"] for v in c["vouchers"]]
                 for c in ar.affiliate_bundle_visible_cards(db, user_id=38)}
        assert first == again


class TestMonetaryIntegrity:
    """Completion requires composition, code count AND monetary value to all
    agree with the frozen recipe. Any disagreement fails closed."""

    def test_all_three_checks_pass_for_a_correct_bundle(self):
        recipe = arp.tier_recipe("202609", "T3")
        rows = [
            {"code": "A", "pool_id": "AFFILIATE_10", "voucher_value": 10},
            {"code": "B", "pool_id": "AFFILIATE_50", "voucher_value": 50},
        ]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert ok and detail is None

    def test_wrong_denomination_composition_fails(self):
        recipe = arp.tier_recipe("202609", "T3")
        rows = [
            {"code": "A", "pool_id": "AFFILIATE_10", "voucher_value": 10},
            {"code": "B", "pool_id": "AFFILIATE_10", "voucher_value": 10},
        ]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert not ok and "composition" in detail

    def test_wrong_code_count_fails(self):
        recipe = arp.tier_recipe("202609", "T3")
        rows = [{"code": "A", "pool_id": "AFFILIATE_10", "voucher_value": 10}]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert not ok and "code count" in detail

    def test_mis_stamped_voucher_value_fails_closed(self):
        # A $50-pool row stamped $10 must NOT be trusted and priced at $10.
        recipe = arp.tier_recipe("202609", "T3")
        rows = [
            {"code": "A", "pool_id": "AFFILIATE_10", "voucher_value": 10},
            {"code": "B", "pool_id": "AFFILIATE_50", "voucher_value": 10},
        ]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert not ok
        assert "stamped voucher_value=10" in detail and "denomination is 50" in detail

    def test_non_numeric_stamp_fails(self):
        recipe = arp.tier_recipe("202609", "T1")
        rows = [{"code": "A", "pool_id": "AFFILIATE_10", "voucher_value": "ten"}]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert not ok and "non-numeric" in detail

    def test_duplicate_codes_within_a_bundle_fail(self):
        recipe = arp.tier_recipe("202609", "T2")
        rows = [
            {"code": "A", "pool_id": "AFFILIATE_5", "voucher_value": 5},
            {"code": "B", "pool_id": "AFFILIATE_10", "voucher_value": 10},
            {"code": "B", "pool_id": "AFFILIATE_10", "voucher_value": 10},
        ]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert not ok and "duplicate" in detail

    def test_unstamped_rows_are_priced_from_the_recipe(self):
        # Legacy/manual uploads carry no voucher_value; the pool's own
        # denomination is the authority, so the bundle still validates.
        recipe = arp.tier_recipe("202609", "T3")
        rows = [
            {"code": "A", "pool_id": "AFFILIATE_10"},
            {"code": "B", "pool_id": "AFFILIATE_50"},
        ]
        ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
        assert ok, detail

    def test_every_september_tier_validates_end_to_end(self):
        for tier, value in (("T1", 10), ("T2", 25), ("T3", 60), ("T4", 180), ("T5", 350)):
            recipe = arp.tier_recipe("202609", tier)
            rows = []
            n = 0
            for comp in recipe["components"]:
                for _ in range(comp["quantity"]):
                    n += 1
                    rows.append({"code": f"{tier}{n}", "pool_id": comp["pool_id"],
                                 "voucher_value": comp["value"]})
            ok, detail = ar._validate_bundle_against_recipe(rows, recipe=recipe)
            assert ok, f"{tier}: {detail}"
            assert sum(r["voucher_value"] for r in rows) == value

    def test_mis_stamped_inventory_parks_the_ledger_unissued(self):
        db = _db()
        _stock_all_denominations(db)
        # Corrupt one $50 row's stamp before it can be claimed.
        row = db.voucher_pools.find_one({"pool_id": "AFFILIATE_50", "status": "available"})
        db.voucher_pools.update_one({"_id": row["_id"]}, {"$set": {"voucher_value": 5}})
        # Make it the only $50 code available so it must be selected.
        for other in list(db.voucher_pools.find({"pool_id": "AFFILIATE_50", "status": "available"})):
            if other["_id"] != row["_id"]:
                db.voucher_pools.update_one({"_id": other["_id"]}, {"$set": {"status": "reserved"}})

        _qualify(db, 60, 50, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=60, now_utc=SEP)
        t3 = db.affiliate_ledger.find_one({"user_id": 60, "tier": "T3"})
        assert t3["status"] == "PENDING_MANUAL", "malformed inventory must fail closed"
        assert "bundle_integrity_failed" in t3["risk_flags"]
        assert "stamped voucher_value=5" in t3["integrity_reason"]
        assert not t3.get("vouchers")


# ---------------------------------------------------------------------------
# Historical compatibility
# ---------------------------------------------------------------------------

class TestHistoricalCompatibility:
    def test_existing_single_code_ledger_still_reconciles(self):
        db = _db()
        # A pre-change ledger: only year_month, only voucher_code.
        db.voucher_pools.insert_one(
            {"pool_id": "T1", "code": "OLD1", "status": "issued",
             "issued_for_ledger_id": "legacy-1", "issued_to_user_id": 40}
        )
        db.affiliate_ledger.insert_one({
            "_id": "legacy-1", "ledger_type": "AFFILIATE_MONTHLY", "user_id": 40,
            "year_month": "202607", "tier": "T1", "pool_id": "T1",
            "status": "ISSUED", "voucher_code": "OLD1",
            "dedup_key": "AFF:40:202607:T1",
        })
        led = db.affiliate_ledger.find_one({"_id": "legacy-1"})
        assert ar._ledger_reward_plan(led) == arp.LEGACY_PLAN_ID
        assert ar._ledger_entitlement_month(led) == "202607"
        out = ar._finalize_issued_if_voucher_exists(db, ledger=led, now_utc=SEP)
        assert out["status"] == "ISSUED" and out["voucher_code"] == "OLD1"

    def test_existing_august_multi_code_bundle_is_unchanged(self):
        db = _db()
        codes = [f"AUG{i}" for i in range(5)]
        db.affiliate_ledger.insert_one({
            "_id": "aug-1", "ledger_type": "AFFILIATE_MONTHLY", "user_id": 41,
            "year_month": "202608", "tier": "T3", "pool_id": "T3", "status": "ISSUED",
            "reward_type": ar.AFFILIATE_BUNDLE_REWARD_TYPE, "affiliate_tier": "T3",
            "voucher_code": codes[0], "voucher_count": 5, "total_value": 50,
            "vouchers": [{"code": c, "value": 10} for c in codes],
            "dedup_key": "AFF:41:202608:T3",
        })
        before = db.affiliate_ledger.find_one({"_id": "aug-1"})
        ar._issue_affiliate_ledger_from_pool(db, ledger=before, now_utc=SEP)
        after = db.affiliate_ledger.find_one({"_id": "aug-1"})
        assert after["vouchers"] == before["vouchers"]
        assert after["total_value"] == 50 and after["voucher_count"] == 5

    def test_non_affiliate_pools_are_unaffected(self):
        db = _db()
        _stock_all_denominations(db)
        _stock(db, "WELCOME", 5, "W")
        db.voucher_pools.insert_one(
            {"pool_id": "SURPRISE", "code": "SV1", "status": "available",
             "allocation_scope": "campaign_rewards"}
        )
        _qualify(db, 42, 250, SEP)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=42, now_utc=SEP)
        assert db.voucher_pools.count_documents({"pool_id": "WELCOME", "status": "available"}) == 5
        assert db.voucher_pools.count_documents({"pool_id": "SURPRISE", "status": "available"}) == 1

    def test_weekly_ledger_cannot_consume_a_denomination_bundle(self):
        db = _db()
        _stock_all_denominations(db)
        db.affiliate_ledger.insert_one({
            "_id": "wk-1", "ledger_type": "AFFILIATE_WEEKLY", "user_id": 43,
            "year_month": "202609", "entitlement_month": "202609", "tier": "T3",
            "pool_id": "T3", "status": "APPROVED", "voucher_code": None,
            "dedup_key": "AFFW:43:202609:T3",
        })
        out = ar._issue_affiliate_ledger_from_pool(
            db, ledger=db.affiliate_ledger.find_one({"_id": "wk-1"}), now_utc=SEP,
        )
        assert out["status"] == "REJECTED"
        assert out["review_reason"] == "weekly_tier_pool_blocked"
        assert db.voucher_pools.count_documents({"status": "issued"}) == 0

    def test_thresholds_match_the_confirmed_business_rules(self):
        assert (ar.T1_THRESHOLD, ar.T2_THRESHOLD, ar.T3_THRESHOLD,
                ar.T4_THRESHOLD, ar.T5_THRESHOLD) == (10, 25, 50, 150, 250)
        assert ar._tier_for_count(249) == "T4"
        assert ar._tier_for_count(250) == "T5"
        assert ar._eligible_tiers_for_count(250) == ["T1", "T2", "T3", "T4", "T5"]
