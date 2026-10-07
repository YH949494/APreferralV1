"""Pending Manual summary: a disabled historical batch must not erase demand.

Production symptom: 40 September ledgers carrying ``bundle_denomination_short``
were summarised as ``required 0 / available 0 / need 0`` for every
denomination, because the summary classified them ``target_batch_disabled``
(BLOCKED) and the aggregate only ever counted ``STOCK_SHORT`` ledgers.

Contract under test -- demand and issuability are separate questions:

* ``denominations[*].required``            what the ledgers still owe (recipe
                                           minus codes already held); NEVER
                                           depends on a batch gate.
* ``denominations[*].available_compatible`` unissued stock in the batch each
                                           ledger is bound to; no other batch.
* ``denominations[*].shortage``            required - compatible, per batch.
* ``issuable_now`` / ``issuable_after_stock_replenishment`` / ``issuance_blockers``
                                           the gate, reported separately.

The ledgers here are produced by the real allocator (``approve_affiliate_ledger``
against a batch that is disabled at that moment), so the flag / pinned-target /
``shortage_reasons`` state is the one production actually has.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId

import affiliate_denomination_shortage as ds
import affiliate_reward_plans as arp
import affiliate_rewards as ar
import affiliate_voucher_batches as av
from fake_mongo import FakeDb

SEP_START = datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc)  # 2026-09-01 00:00 KL
SEP = datetime(2026, 9, 15, 4, 0, tzinfo=timezone.utc)
OCT = datetime(2026, 10, 7, 4, 0, tzinfo=timezone.utc)         # September is over
UNIQUE = {
    "affiliate_ledger": [("dedup_key",)],
    "voucher_pools": [("pool_id", "code")],
    av.HISTORICAL_REPLENISH_LOCK_COLLECTION: [("_id",)],
    ds.BULK_RETRY_LOCK_COLLECTION: [("_id",)],
}
P5, P10, P50 = "AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50"
TAG = {P5: "5", P10: "10", P50: "50"}
RECIPE = {  # canonical per-tier demand (affiliate_reward_plans)
    "T1": {5: 0, 10: 1, 50: 0}, "T2": {5: 1, 10: 2, 50: 0}, "T3": {5: 0, 10: 1, 50: 1},
    "T4": {5: 0, 10: 3, 50: 3}, "T5": {5: 0, 10: 0, 50: 7},
}
VALUE = {"T1": 10, "T2": 25, "T3": 60, "T4": 180, "T5": 350}


def _db():
    return FakeDb(UNIQUE)


def _batch(db, pool_id, month, n, *, now=SEP_START):
    codes = [f"C{TAG[pool_id]}-{month}-{i}" for i in range(max(n, 1))]
    out = av.create_batch(db, admin_identity="seed", batch_name=f"{pool_id}-{month}", pool_id=pool_id,
                          entitlement_month=month, codes=codes, now_utc=now)
    assert out["ok"], out
    bid = ObjectId(out["batch"]["batch_id"])
    if n == 0:
        db.voucher_pools.update_one({"batch_id": bid}, {"$set": {"status": "issued", "issued_for_ledger_id": "seed"}})
    return bid


def _stock(db, month="202609", *, p5=0, p10=0, p50=0, now=SEP_START):
    return {P5: _batch(db, P5, month, p5, now=now), P10: _batch(db, P10, month, p10, now=now),
            P50: _batch(db, P50, month, p50, now=now)}


def _set_disabled(db, ids, disabled, pools=(P5, P10, P50)):
    for pool in pools:
        out = av.set_batch_distribution_disabled(db, ids[pool], admin_identity="admin", disabled=disabled, now_utc=SEP)
        assert out["ok"], out


def _ledger(db, *, user_id, tier, month="202609", status="PENDING_REVIEW", **extra):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY", "user_id": user_id, "status": status, "tier": tier, "pool_id": tier,
        "year_month": month, "entitlement_month": month, "reward_plan": arp.resolve_plan_id(month),
        "bundle_recipe": arp.tier_recipe(month, tier),
        "dedup_key": f"AFF:{user_id}:{month}:{tier}", "voucher_code": None, "risk_flags": [],
        "created_at": SEP + timedelta(minutes=user_id), "updated_at": SEP,
    }
    doc.update(extra)
    return db.affiliate_ledger.insert_one(doc).inserted_id


def _park(db, *, user_id, tier, month="202609", now=SEP):
    """Run the REAL allocator against the current batch state; whatever it
    cannot fill parks as PENDING_MANUAL / bundle_denomination_short."""
    lid = _ledger(db, user_id=user_id, tier=tier, month=month)
    row = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=now)
    assert row["status"] == "PENDING_MANUAL", row
    return lid


def _scenario(tiers, *, now_check=OCT, stock=None):
    """September ledgers parked by the allocator while EVERY September batch
    is disabled; returns (db, batch ids, ledger ids)."""
    db = _db()
    ids = _stock(db, **(stock or {}))
    _set_disabled(db, ids, True)
    lids = [_park(db, user_id=i, tier=t) for i, t in enumerate(tiers, start=1)]
    return db, ids, lids


def _den(s, v):
    return s["denominations"][str(v)]


def _sum(tiers, denom):
    return sum(RECIPE[t][denom] for t in tiers)


def _summary(db, now=OCT):
    return ds.summarize_pending_manual_shortage(db, now_utc=now)


# --------------------------------------------------------------------------
# Root cause, pinned: the flag is coarse, the reason is live batch state
# --------------------------------------------------------------------------
def test_allocator_flags_a_disabled_batch_as_bundle_denomination_short_not_target_batch_disabled():
    db, _, (lid,) = _scenario(["T4"])
    row = db.affiliate_ledger.find_one({"_id": lid})
    # The allocator collapses EVERY claim failure into this one flag...
    assert row["risk_flags"] == ["bundle_denomination_short"]
    # ...and keeps the specific reason (a snapshot of that attempt) beside it.
    assert row["shortage_reasons"] == {P10: "target_batch_disabled", P50: "target_batch_disabled"}
    assert row["missing_by_denomination"] == {P10: 3, P50: 3}
    # The ledger was pinned to its September batches before the claim failed.
    assert {k: v["mode"] for k, v in row["pool_targets"].items()} == {P10: "batch", P50: "batch"}


def test_the_summary_reports_both_facts_without_contradiction():
    db, _, _ = _scenario(["T4"])
    s = _summary(db)
    assert s["pending_count"] == 1                                  # it IS a bundle_denomination_short ledger
    assert s["issuance_blockers"] == {"target_batch_disabled": 1}   # and it IS gated by the live batch state
    assert _den(s, 10)["required"] == 3 and _den(s, 50)["required"] == 3


# --------------------------------------------------------------------------
# 1-5  demand survives a disabled historical batch, per tier
# --------------------------------------------------------------------------
@pytest.mark.parametrize("tier", ["T1", "T2", "T3", "T4", "T5"])
def test_disabled_historical_batch_still_computes_denomination_demand(tier):
    db, ids, _ = _scenario([tier])
    s = _summary(db)
    assert s["pending_count"] == 1 and s["total_reward_value"] == VALUE[tier]
    for denom in (5, 10, 50):
        d = _den(s, denom)
        want = RECIPE[tier][denom]
        assert (d["required"], d["available_compatible"], d["shortage"]) == (want, 0, want), (tier, denom)
    assert s["issuable_now"] == 0 and s["issuable_after_stock_replenishment"] == 0
    assert s["issuance_blockers"] == {"target_batch_disabled": 1}


# 6 -------------------------------------------------------------------------
def test_mixed_tiers_with_disabled_batch_sum_to_the_t1_t5_demand():
    tiers = ["T1", "T2", "T3", "T4", "T5"]
    db, _, _ = _scenario(tiers)
    s = _summary(db)
    assert (_den(s, 5)["required"], _den(s, 10)["required"], _den(s, 50)["required"]) == (1, 7, 11)
    assert s["total_reward_value"] == 625 and s["pending_count"] == 5
    assert s["issuance_blockers"] == {"target_batch_disabled": 5}


# 7 -------------------------------------------------------------------------
def test_required_demand_is_identical_whether_the_batch_is_enabled_or_disabled():
    """The batch gate may change issuability, never the demand figure."""
    tiers = ["T1", "T2", "T3", "T4", "T4", "T5"] * 3                   # 18 ledgers
    db, ids, _ = _scenario(tiers)
    disabled = _summary(db)
    _set_disabled(db, ids, False)
    enabled = _summary(db)
    for denom in (5, 10, 50):
        want = _sum(tiers, denom)
        assert _den(disabled, denom)["required"] == _den(enabled, denom)["required"] == want, denom
    assert _sum(tiers, 10) > 0 and _sum(tiers, 50) > 0
    assert disabled["total_reward_value"] == enabled["total_reward_value"]


def test_forty_ledger_production_shape_is_not_zero():
    tiers = ["T4"] * 14 + ["T3"] * 10 + ["T2"] * 8 + ["T1"] * 4 + ["T5"] * 4          # 40 ledgers
    db, _, _ = _scenario(tiers)
    s = _summary(db)
    assert s["pending_count"] == 40 and s["issuance_blockers"] == {"target_batch_disabled": 40}
    assert s["issuable_after_stock_replenishment"] == 0
    assert (_den(s, 5)["required"], _den(s, 10)["required"], _den(s, 50)["required"]) == (
        _sum(tiers, 5), _sum(tiers, 10), _sum(tiers, 50))
    assert all(_den(s, v)["required"] > 0 for v in (5, 10, 50))
    assert s["total_reward_value"] == sum(VALUE[t] for t in tiers)


# 8 -------------------------------------------------------------------------
def test_issuable_now_stays_zero_when_the_batch_is_disabled_even_with_stock_in_it():
    db, ids, lids = _scenario(["T1", "T3", "T4"], stock={"p5": 5, "p10": 20, "p50": 20})
    s = _summary(db)
    assert s["issuable_now"] == 0 and s["issuable_after_stock_replenishment"] == 0
    # The codes are compatible (right batch) so nothing needs uploading...
    assert _den(s, 10)["available_compatible"] == 5 and _den(s, 10)["shortage"] == 0
    # ...but a retry must not touch them while the gate is closed.
    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 0 and out["blocked"] == 3
    for lid in lids:
        assert db.affiliate_ledger.find_one({"_id": lid})["status"] == "PENDING_MANUAL"
    assert db.voucher_pools.count_documents({"status": "issued", "issued_for_ledger_id": {"$ne": "seed"}}) == 0


# 9 -------------------------------------------------------------------------
def test_batch_blocker_is_reported_separately_from_the_stock_shortage():
    db, _, _ = _scenario(["T4", "T4"])
    s = _summary(db)
    assert s["issuance_blockers"] == {"target_batch_disabled": 2}
    assert s["still_blocked"] == 2 and s["blocked_breakdown"] == s["issuance_blockers"]   # legacy alias
    assert _den(s, 10)["shortage"] == 6 and _den(s, 50)["shortage"] == 6
    # Uploading cannot fix a disabled batch, so nothing is "uploadable" yet and
    # the per-month view says why.
    assert _den(s, 10)["uploadable_shortage"] == 0 and _den(s, 50)["uploadable_shortage"] == 0
    m = s["by_month"]["202609"]["10"]
    assert (m["required"], m["shortage"], m["uploadable_shortage"]) == (6, 6, 0)
    assert m["blockers"] == {"target_batch_disabled": 6} and m["historical"] is True
    up = ds.upload_codes_for_denomination(_scenario(["T4"])[0], admin_identity="a", entitlement_month="202609",
                                          denomination=10, codes="X1", now_utc=OCT)
    assert up["status"] == "error" and up["reason"] == "batch_disabled"


def test_only_the_disabled_denominations_batch_blocks_but_all_demand_is_counted():
    db = _db()
    ids = _stock(db, p10=0, p50=0)
    _set_disabled(db, ids, True, pools=(P10,))                     # $50 batch stays enabled
    for i in (1, 2):
        _park(db, user_id=i, tier="T4")
    s = _summary(db)
    assert s["issuance_blockers"] == {"target_batch_disabled": 2}
    assert (_den(s, 10)["required"], _den(s, 50)["required"]) == (6, 6)
    assert _den(s, 10)["uploadable_shortage"] == 0                  # gated
    assert _den(s, 50)["uploadable_shortage"] == 0                  # its ledgers are gated by the $10 batch too


# 10 ------------------------------------------------------------------------
def test_enabling_the_compatible_batch_makes_the_same_demand_retryable():
    tiers = ["T4", "T3", "T1"]
    db, ids, lids = _scenario(tiers, stock={"p10": 10, "p50": 10})
    before = _summary(db)
    assert before["issuable_now"] == 0 and before["issuance_blockers"] == {"target_batch_disabled": 3}

    _set_disabled(db, ids, False)                                   # the manual step: re-enable September
    after = _summary(db)
    assert after["issuance_blockers"] == {} and after["still_blocked"] == 0
    assert after["issuable_now"] == after["issuable_after_stock_replenishment"] == 3
    for denom in (5, 10, 50):                                       # demand did not move
        assert _den(after, denom)["required"] == _den(before, denom)["required"]

    out = ds.retry_all_eligible_pending(db, admin_identity="a", now_utc=OCT)   # ended month, pinned ledgers
    assert (out["scanned"], out["issued"], out["blocked"], out["still_short"], out["errors"]) == (3, 3, 0, 0, 0)
    for lid, tier in zip(lids, tiers):
        row = db.affiliate_ledger.find_one({"_id": lid})
        assert row["status"] == "ISSUED" and row["issued_value"] == VALUE[tier]
        assert row["entitlement_month"] == "202609" and ds.SHORTAGE_FLAG not in row["risk_flags"]
    assert _summary(db)["pending_count"] == 0


def test_enabled_batch_without_enough_stock_becomes_a_pure_stock_shortage():
    db, ids, _ = _scenario(["T4"], stock={"p10": 1, "p50": 0})
    _set_disabled(db, ids, False)
    s = _summary(db)
    assert s["issuable_now"] == 0 and s["issuable_after_stock_replenishment"] == 1 and s["issuance_blockers"] == {}
    assert (_den(s, 10)["required"], _den(s, 10)["available_compatible"], _den(s, 10)["shortage"]) == (3, 1, 2)
    assert (_den(s, 50)["required"], _den(s, 50)["available_compatible"], _den(s, 50)["shortage"]) == (3, 0, 3)
    assert _den(s, 10)["uploadable_shortage"] == 2                  # now an upload CAN fix it


# 11 ------------------------------------------------------------------------
def test_no_incompatible_or_current_month_stock_is_counted_or_consumed():
    db, ids, lids = _scenario(["T4"])
    oct_ids = _stock(db, "202610", p10=50, p50=50, now=SEP)          # October stock, enabled and plentiful
    s = _summary(db)
    assert _den(s, 10)["available_compatible"] == 0 and _den(s, 50)["available_compatible"] == 0
    assert _den(s, 10)["shortage"] == 3
    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 0 and out["blocked"] == 1
    assert db.voucher_pools.count_documents({"batch_id": oct_ids[P10], "status": "available"}) == 50
    assert db.voucher_pools.count_documents({"batch_id": oct_ids[P50], "status": "available"}) == 50
    assert db.voucher_pools.count_documents({"issued_for_ledger_id": str(lids[0])}) == 0

    # Even once September is re-enabled but short, October stock still must not be used.
    _set_disabled(db, ids, False)
    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 0 and out["still_short"] == 1
    assert db.voucher_pools.count_documents({"batch_id": oct_ids[P10], "status": "available"}) == 50


# 12 ------------------------------------------------------------------------
def test_codes_a_ledger_already_holds_and_issued_ledgers_are_excluded_from_demand():
    db, ids, lids = _scenario(["T4"], stock={"p10": 3})
    # Give the parked T4 two of its three $10s (partial allocation is retained by design).
    held = list(db.voucher_pools.find({"batch_id": ids[P10], "status": "available"}))[:2]
    for row in held:
        db.voucher_pools.update_one({"_id": row["_id"]}, {"$set": {
            "status": "issued", "issued_for_ledger_id": str(lids[0]), "ledger_id": lids[0], "issued_to_user_id": 1}})
    # An already-ISSUED ledger and one carrying a bundle must not add demand.
    _ledger(db, user_id=2, tier="T4", status="ISSUED", risk_flags=["bundle_denomination_short"], dedup_key="x:2")
    _ledger(db, user_id=3, tier="T5", status="PENDING_MANUAL", risk_flags=["bundle_denomination_short"], dedup_key="x:3",
            reward_type="affiliate_bundle", vouchers=[{"code": "ALREADY", "value": 350}], voucher_code="ALREADY")
    s = _summary(db)
    assert s["pending_count"] == 1
    assert _den(s, 10)["required"] == 1           # 3 owed - 2 held
    assert _den(s, 50)["required"] == 3
    assert s["partially_reserved_ledgers"] == 0   # blocked ledgers are not "short"; held codes are still netted
    assert s["total_reward_value"] == 180


# 13 ------------------------------------------------------------------------
def test_retry_is_idempotent_while_blocked_and_after_enabling():
    db, ids, lids = _scenario(["T1", "T1"], stock={"p10": 2})
    snap = lambda: sorted((r["code"], r.get("issued_for_ledger_id")) for r in db.voucher_pools.find({}))  # noqa: E731
    base = snap()
    for _ in range(3):                                              # blocked: nothing moves, however often it runs
        out = ds.retry_all_eligible_pending(db, now_utc=OCT)
        assert out["issued"] == 0 and out["blocked"] == 2
    assert snap() == base

    _set_disabled(db, ids, False)
    first = ds.retry_all_eligible_pending(db, now_utc=OCT)
    issued_snap = snap()
    second = ds.retry_all_eligible_pending(db, now_utc=OCT)
    third = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert first["issued"] == 2 and second["scanned"] == third["scanned"] == 0
    assert snap() == issued_snap
    for lid in lids:
        assert db.voucher_pools.count_documents({"issued_for_ledger_id": str(lid)}) == 1
