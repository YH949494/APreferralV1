"""Pending Manual denomination shortage summary, bulk upload and bulk retry.

The contract under test: the operator uploads the TOTAL shortage once, then
one bulk retry completes every ledger that can now be completed -- all-or-
nothing per ledger, never double-issuing, never reaching into another
entitlement month's stock.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId
from flask import Flask

import affiliate_denomination_shortage as ds
import affiliate_reward_plans as arp
import affiliate_rewards as ar
import affiliate_voucher_batches as av
import vouchers
from fake_mongo import FakeDb
from vouchers import vouchers_bp

SEP_START = datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc)  # 2026-09-01 00:00 KL
SEP = datetime(2026, 9, 15, 4, 0, tzinfo=timezone.utc)
OCT = datetime(2026, 10, 2, 4, 0, tzinfo=timezone.utc)
UNIQUE = {
    "affiliate_ledger": [("dedup_key",)],
    "voucher_pools": [("pool_id", "code")],
    av.HISTORICAL_REPLENISH_LOCK_COLLECTION: [("_id",)],
    ds.BULK_RETRY_LOCK_COLLECTION: [("_id",)],
}
P5, P10, P50 = "AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50"
TAG = {P5: "5", P10: "10", P50: "50"}


# --------------------------------------------------------------------------
# builders
# --------------------------------------------------------------------------
def _batch(db, pool_id, month, n, *, now=SEP_START):
    """A ready batch holding exactly ``n`` available codes (n == 0 is built
    by issuing the single seed code the batch API insists on)."""
    codes = [f"C{TAG[pool_id]}-{month}-{i}" for i in range(max(n, 1))]
    out = av.create_batch(
        db, admin_identity="seed", batch_name=f"{pool_id}-{month}", pool_id=pool_id,
        entitlement_month=month, codes=codes, now_utc=now,
    )
    assert out["ok"], out
    bid = ObjectId(out["batch"]["batch_id"])
    if n == 0:
        db.voucher_pools.update_one({"batch_id": bid}, {"$set": {"status": "issued", "issued_for_ledger_id": "seed"}})
    return bid


def _stock(db, month="202609", *, p5=0, p10=0, p50=0, now=SEP_START):
    return {
        P5: _batch(db, P5, month, p5, now=now),
        P10: _batch(db, P10, month, p10, now=now),
        P50: _batch(db, P50, month, p50, now=now),
    }


def _short(db, *, user_id, tier, month="202609", status="PENDING_MANUAL", flags=(ds.SHORTAGE_FLAG,),
           ledger_type="AFFILIATE_MONTHLY", **extra):
    doc = {
        "ledger_type": ledger_type, "user_id": user_id, "status": status, "tier": tier, "pool_id": tier,
        "year_month": month, "entitlement_month": month,
        "reward_plan": arp.resolve_plan_id(month), "bundle_recipe": arp.tier_recipe(month, tier),
        "dedup_key": f"AFF:{user_id}:{month}:{tier}:{ledger_type}:{status}", "voucher_code": None,
        "risk_flags": list(flags), "created_at": SEP + timedelta(minutes=user_id), "updated_at": SEP,
    }
    doc.update(extra)
    return db.affiliate_ledger.insert_one(doc).inserted_id


def _db():
    return FakeDb(UNIQUE)


def _den(summary, value):
    return summary["denominations"][str(value)]


def _issued_codes(db):
    return [r["code"] for r in db.voucher_pools.find({"status": "issued", "issued_for_ledger_id": {"$ne": "seed"}})]


def _linked(db, lid):
    return db.voucher_pools.find({"issued_for_ledger_id": str(lid)})


# --------------------------------------------------------------------------
# 1-5  per-tier shortage calculation (straight from the canonical recipe)
# --------------------------------------------------------------------------
@pytest.mark.parametrize("tier,expected,value", [
    ("T1", {5: 0, 10: 1, 50: 0}, 10),
    ("T2", {5: 1, 10: 2, 50: 0}, 25),
    ("T3", {5: 0, 10: 1, 50: 1}, 60),
    ("T4", {5: 0, 10: 3, 50: 3}, 180),
    ("T5", {5: 0, 10: 0, 50: 7}, 350),
])
def test_single_tier_shortage(tier, expected, value):
    db = _db()
    _stock(db)  # empty September batches
    _short(db, user_id=1, tier=tier)
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 1 and s["total_reward_value"] == value
    assert s["issuable_after_replenishment"] == 1 and s["still_blocked"] == 0
    for denom, qty in expected.items():
        d = _den(s, denom)
        assert (d["required"], d["available"], d["shortage"]) == (qty, 0, qty), (tier, denom)


# 6 -------------------------------------------------------------------------
def test_mixed_tiers_sum_to_full_t1_t5_625():
    db = _db()
    _stock(db)
    for i, tier in enumerate(("T1", "T2", "T3", "T4", "T5"), start=1):
        _short(db, user_id=i, tier=tier)
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert (_den(s, 5)["shortage"], _den(s, 10)["shortage"], _den(s, 50)["shortage"]) == (1, 7, 11)
    assert s["total_reward_value"] == 625 and s["pending_count"] == 5


# 7 -------------------------------------------------------------------------
def test_available_stock_partially_offsets_required():
    db = _db()
    _stock(db, p50=5)
    _short(db, user_id=1, tier="T5")
    d = _den(ds.summarize_pending_manual_shortage(db, now_utc=SEP), 50)
    assert (d["required"], d["available"], d["shortage"]) == (7, 5, 2)


# 8 -------------------------------------------------------------------------
def test_zero_shortage_when_stock_covers_everything():
    db = _db()
    _stock(db, p5=5, p10=20, p50=20)
    for i, tier in enumerate(("T2", "T3", "T4"), start=1):
        _short(db, user_id=i, tier=tier)
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert all(_den(s, v)["shortage"] == 0 for v in (5, 10, 50))
    # `available` is the stock USABLE by these ledgers, never more than required.
    assert _den(s, 10)["required"] == 6 and _den(s, 10)["available"] == 6 and _den(s, 10)["available_total"] == 20
    assert s["issuable_now"] == 3


# 9 -------------------------------------------------------------------------
def test_issued_ledgers_excluded():
    db = _db()
    _stock(db)
    _short(db, user_id=1, tier="T1", status="ISSUED")
    _short(db, user_id=2, tier="T1", voucher_code="ALREADY", reward_type="affiliate_bundle",
           vouchers=[{"code": "ALREADY", "value": 10}])
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 0 and _den(s, 10)["required"] == 0


# 10 ------------------------------------------------------------------------
def test_rejected_ledgers_excluded():
    db = _db()
    _stock(db)
    _short(db, user_id=1, tier="T3", status="REJECTED")
    _short(db, user_id=2, tier="T3", status="OUT_OF_STOCK")
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 0 and _den(s, 50)["required"] == 0


# 11 ------------------------------------------------------------------------
def test_simulated_pending_excluded():
    db = _db()
    _stock(db)
    _short(db, user_id=1, tier="T3", status="SIMULATED_PENDING", ledger_type="AFFILIATE_SIMULATION", simulate=True)
    _short(db, user_id=2, tier="T3", simulate=True)  # even a mislabelled PENDING_MANUAL sim row
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 0 and s["excluded"] == {"simulated": 1}


# 12 ------------------------------------------------------------------------
def test_unrelated_manual_reasons_excluded():
    db = _db()
    _stock(db)
    _short(db, user_id=1, tier="T2", flags=("blocked_user", ds.SHORTAGE_FLAG))   # abuse flag alongside
    _short(db, user_id=2, tier="T2", flags=("ip_cluster",))                       # no shortage flag at all
    _short(db, user_id=3, tier="T2", flags=("partial_bundle_conflict",))
    _short(db, user_id=4, tier="T2")                                              # the only real one
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 1 and _den(s, 5)["required"] == 1
    assert s["excluded"] == {"other_risk_flags": 1}  # only flagged-short rows are even scanned


# 13 ------------------------------------------------------------------------
def test_reserved_and_issued_rows_never_count_as_available_and_partial_holders_ask_only_for_the_rest():
    db = _db()
    ids = _stock(db, p5=2, p10=3)
    # Reserved-but-still-'available' row, and a plain issued row: neither is stock.
    db.voucher_pools.update_one({"batch_id": ids[P10], "code": f"C10-202609-0"},
                                {"$set": {"issued_for_ledger_id": "someone-else"}})
    db.voucher_pools.update_one({"batch_id": ids[P10], "code": f"C10-202609-1"}, {"$set": {"status": "issued"}})
    # A T2 that already holds its $5 and one $10 (partial allocation is retained by design).
    lid = _short(db, user_id=1, tier="T2")
    for pool, code in ((P5, "C5-202609-0"), (P10, "C10-202609-2")):
        db.voucher_pools.update_one({"code": code}, {"$set": {
            "status": "issued", "issued_for_ledger_id": str(lid), "ledger_id": lid, "issued_to_user_id": 1}})
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert _den(s, 5)["required"] == 0                      # its $5 is already held
    assert _den(s, 10)["required"] == 1                     # only the missing $10
    assert _den(s, 10)["available_total"] == 0              # reserved + issued rows are not stock
    assert _den(s, 10)["shortage"] == 1 and s["partially_reserved_ledgers"] == 1


def test_ledger_holding_its_whole_bundle_needs_no_stock_and_is_excluded_from_the_count():
    db = _db()
    ids = _stock(db, p10=1)
    lid = _short(db, user_id=1, tier="T1")
    db.voucher_pools.update_one({"batch_id": ids[P10]}, {"$set": {
        "status": "issued", "issued_for_ledger_id": str(lid), "ledger_id": lid, "issued_to_user_id": 1}})
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 0 and s["excluded"] == {"reserved_complete": 1}
    # ...but the bulk retry finalizes it without needing any stock.
    out = ds.retry_all_eligible_pending(db, now_utc=SEP)
    assert out["issued"] == 1
    assert db.affiliate_ledger.find_one({"_id": lid})["status"] == "ISSUED"


def test_duplicate_ledger_for_same_user_month_tier_is_not_double_counted():
    db = _db()
    _stock(db)
    _short(db, user_id=1, tier="T1")
    _short(db, user_id=1, tier="T1", status="ISSUED", flags=())  # same entitlement already issued
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 0 and s["excluded"] == {"duplicate": 1}


def test_blocked_by_non_stock_reason_is_reported_not_required():
    db = _db()
    _stock(db, month="202609")   # a September batch exists...
    _short(db, user_id=1, tier="T1")
    _short(db, user_id=2, tier="T1", month="202611")  # ...but no November batch at all
    s = ds.summarize_pending_manual_shortage(db, now_utc=SEP)
    assert s["pending_count"] == 2 and s["issuable_after_replenishment"] == 1 and s["still_blocked"] == 1
    assert s["blocked_breakdown"] == {"no_batch_for_entitlement_period": 1}
    assert _den(s, 10)["required"] == 1  # the unfixable one asks for nothing


# --------------------------------------------------------------------------
# 14  upload validation
# --------------------------------------------------------------------------
def test_upload_rejects_duplicates_wrong_bucket_and_reports_counts():
    db = _db()
    _stock(db, p10=1)
    _short(db, user_id=1, tier="T1")
    db.voucher_pools.insert_one({"pool_id": P50, "code": "WRONG-BUCKET", "status": "available"})
    out = ds.upload_codes_for_denomination(
        db, admin_identity="a", entitlement_month="202609", denomination=10, now_utc=SEP,
        codes="NEW-1\nNEW-1\nNEW-2\nC10-202609-0\nWRONG-BUCKET\nbad code\n\n  \n",
    )
    assert out["status"] == "ok", out
    assert out["inserted"] == 2                    # NEW-1, NEW-2
    assert out["duplicates"] == 3                  # in-upload NEW-1, existing C10-..., existing WRONG-BUCKET
    assert out["wrong_bucket"] == 1 and out["invalid"] == 1
    row = db.voucher_pools.find_one({"code": "NEW-1"})
    assert row["pool_id"] == P10 and row["voucher_value"] == 10 and row["status"] == "available"
    assert db.voucher_pools.find_one({"code": "WRONG-BUCKET"})["pool_id"] == P50  # untouched


def test_upload_validation_errors():
    db = _db()
    _stock(db)
    f = lambda **kw: ds.upload_codes_for_denomination(  # noqa: E731
        db, admin_identity="a", now_utc=SEP, **{"entitlement_month": "202609", "denomination": 10, "codes": "X-1", **kw})
    assert f(denomination=25)["reason"] == "invalid_denomination"
    assert f(entitlement_month="202608")["reason"] == "invalid_entitlement_month"   # legacy plan month
    assert f(entitlement_month="garbage")["reason"] == "invalid_entitlement_month"
    assert f(codes="\n \n")["reason"] == "empty_codes"
    assert f(entitlement_month="202612")["reason"] == "no_batch_for_entitlement_period"
    assert db.voucher_pools.count_documents({"code": "X-1"}) == 0


def test_upload_does_not_log_codes(caplog):
    db = _db()
    _stock(db)
    with caplog.at_level("DEBUG"):
        ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609", denomination=5,
                                         codes="SECRET-CODE-123", now_utc=SEP)
    assert "SECRET-CODE-123" not in caplog.text


# --------------------------------------------------------------------------
# 15-19  bulk retry
# --------------------------------------------------------------------------
def test_bulk_retry_issues_every_ledger_the_stock_now_covers():
    db = _db()
    _stock(db, p5=1, p10=4, p50=1)
    ids = {t: _short(db, user_id=i, tier=t) for i, t in enumerate(("T1", "T2", "T3"), start=1)}
    out = ds.retry_all_eligible_pending(db, admin_identity="a", now_utc=SEP)
    assert (out["scanned"], out["issued"], out["still_short"], out["already_issued"], out["errors"]) == (3, 3, 0, 0, 0)
    values = {"T1": 10, "T2": 25, "T3": 60}
    for tier, lid in ids.items():
        row = db.affiliate_ledger.find_one({"_id": lid})
        assert row["status"] == "ISSUED" and row["issued_value"] == values[tier]
        assert row["entitlement_month"] == "202609" and row["created_at"] == SEP + timedelta(minutes=row["user_id"])
        assert ds.SHORTAGE_FLAG not in row["risk_flags"]
    codes = _issued_codes(db)
    assert len(codes) == len(set(codes)) == 6  # T1 1 + T2 (1+2)... = 1 + 3 + 2
    assert ds.summarize_pending_manual_shortage(db, now_utc=SEP)["pending_count"] == 0


def test_bulk_retry_leaves_ledgers_stock_cannot_cover_untouched():
    db = _db()
    ids = _stock(db, p10=2)
    a = _short(db, user_id=1, tier="T1")
    b = _short(db, user_id=2, tier="T3")   # needs a $50 that does not exist
    c = _short(db, user_id=3, tier="T1")
    out = ds.retry_all_eligible_pending(db, now_utc=SEP)
    assert (out["scanned"], out["issued"], out["still_short"]) == (3, 2, 1)
    assert db.affiliate_ledger.find_one({"_id": a})["status"] == "ISSUED"
    assert db.affiliate_ledger.find_one({"_id": c})["status"] == "ISSUED"
    skipped = db.affiliate_ledger.find_one({"_id": b})
    assert skipped["status"] == "PENDING_MANUAL" and ds.SHORTAGE_FLAG in skipped["risk_flags"]
    assert _linked(db, b) == []     # not a single code was parked on it


def test_bulk_retry_is_idempotent():
    db = _db()
    _stock(db, p10=3)
    for i in (1, 2):
        _short(db, user_id=i, tier="T1")
    first = ds.retry_all_eligible_pending(db, now_utc=SEP)
    snapshot = sorted((r["code"], r["issued_for_ledger_id"]) for r in db.voucher_pools.find({"status": "issued"}))
    second = ds.retry_all_eligible_pending(db, now_utc=SEP)
    third = ds.retry_all_eligible_pending(db, now_utc=SEP)
    assert first["issued"] == 2
    assert second["scanned"] == third["scanned"] == 0 and second["issued"] == 0
    assert snapshot == sorted((r["code"], r["issued_for_ledger_id"]) for r in db.voucher_pools.find({"status": "issued"}))


def test_a_ledger_issued_between_scan_and_attempt_is_counted_not_reissued(monkeypatch):
    db = _db()
    _stock(db, p10=2)
    lid = _short(db, user_id=1, tier="T1")
    real = ds._attempt_issue

    def sweep_wins_first(db_, ledger_id, *, now_utc):
        ar._issue_affiliate_ledger_from_pool(db_, ledger=db_.affiliate_ledger.find_one({"_id": ledger_id}), now_utc=now_utc)
        return real(db_, ledger_id, now_utc=now_utc)

    monkeypatch.setattr(ds, "_attempt_issue", sweep_wins_first)
    out = ds.retry_all_eligible_pending(db, now_utc=SEP)
    assert out["already_issued"] == 1 and out["issued"] == 0
    assert len(_linked(db, lid)) == 1  # one code, not two


def test_two_bulk_runs_cannot_both_hold_the_lock():
    db = _db()
    _stock(db, p10=1)
    _short(db, user_id=1, tier="T1")
    assert ds._acquire_bulk_lock(db, "holder-a") is True
    out = ds.retry_all_eligible_pending(db, now_utc=SEP)
    assert out["status"] == "error" and out["reason"] == "retry_in_progress"
    assert db.affiliate_ledger.find_one({})["status"] == "PENDING_MANUAL"


def test_concurrent_bulk_retries_never_double_issue(monkeypatch):
    """Both runs get past the run-level lock (forced) so the per-ledger
    transition and the allocator's fenced lease are what is being tested."""
    db = _db()
    _stock(db, p10=20)
    n = 8
    ids = [_short(db, user_id=i, tier="T1") for i in range(1, n + 1)]
    monkeypatch.setattr(ds, "_acquire_bulk_lock", lambda *_a, **_k: True)
    monkeypatch.setattr(ds, "_renew_bulk_lock", lambda *_a, **_k: True)
    barrier = threading.Barrier(2)
    results = []

    def run():
        barrier.wait()
        results.append(ds.retry_all_eligible_pending(db, now_utc=SEP))

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r["issued"] for r in results) == n
    assert sum(r["errors"] for r in results) == 0
    codes = _issued_codes(db)
    assert len(codes) == len(set(codes)) == n          # one code each, none shared
    for lid in ids:
        row = db.affiliate_ledger.find_one({"_id": lid})
        assert row["status"] == "ISSUED" and row["issued_code_count"] == 1
        assert len(_linked(db, lid)) == 1


def test_a_bundle_is_never_partially_issued():
    db = _db()
    ids = _stock(db, p10=3, p50=2)       # T4 owes 3 x $10 + 3 x $50
    lid = _short(db, user_id=1, tier="T4")
    out = ds.retry_all_eligible_pending(db, now_utc=SEP)
    assert out["issued"] == 0 and out["still_short"] == 1
    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "PENDING_MANUAL" and not row.get("voucher_code") and not row.get("vouchers")
    assert _linked(db, lid) == []        # pre-flight kept the $10s free for ledgers that can use them
    assert db.voucher_pools.count_documents({"batch_id": ids[P10], "status": "available"}) == 3

    # Even when the allocator itself runs short mid-bundle (a race the
    # pre-flight cannot see), it keeps the codes parked but NEVER marks ISSUED.
    ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=SEP)
    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "PENDING_MANUAL" and not row.get("vouchers")
    assert row["missing_by_denomination"] == {P50: 1}


# --------------------------------------------------------------------------
# 20  historical batch compatibility
# --------------------------------------------------------------------------
@pytest.fixture
def historical():
    """Two September T2s that secured their $5 and ran the September $10
    batch dry. An October $10 batch WITH stock exists and must stay out of it."""
    db = _db()
    sep5 = _batch(db, P5, "202609", 3)
    sep10 = _batch(db, P10, "202609", 1)
    oct10 = _batch(db, P10, "202610", 2, now=SEP)
    lids = []
    for uid in (21, 22):
        lid = _short(db, user_id=uid, tier="T2", status="PENDING_REVIEW", flags=())
        out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=SEP)
        assert out["status"] == "PENDING_MANUAL"
        lids.append(lid)
    return {"db": db, "sep5": sep5, "sep10": sep10, "oct10": oct10, "lids": lids}


def test_historical_summary_counts_only_the_ledgers_own_month(historical):
    s = ds.summarize_pending_manual_shortage(historical["db"], now_utc=OCT)
    d10 = _den(s, 10)
    assert (d10["required"], d10["available"], d10["shortage"]) == (3, 0, 3)   # not 2 from October
    assert _den(s, 5)["shortage"] == 0
    assert list(s["by_month"]) == ["202609"] and s["by_month"]["202609"]["10"]["historical"] is True
    assert s["by_month"]["202609"]["10"]["batch_id"] == str(historical["sep10"])
    assert s["issuable_after_replenishment"] == 2 and s["issuable_now"] == 0


def test_historical_upload_is_capped_to_the_shortage_then_retry_uses_only_september_stock(historical):
    db = historical["db"]
    over = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609",
                                            denomination=10, codes="H1\nH2\nH3\nH4", now_utc=OCT)
    assert over["reason"] == "quantity_exceeds_shortage" and over["replenishable"] == 3
    assert db.voucher_pools.count_documents({"code": {"$in": ["H1", "H2", "H3", "H4"]}}) == 0

    up = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609",
                                          denomination=10, codes="H1\nH2\nH3", now_utc=OCT)
    assert up["status"] == "ok" and up["inserted"] == 3 and up["historical"] is True
    assert up["batch_id"] == str(historical["sep10"])
    assert db.voucher_pools.find_one({"code": "H1"})["batch_id"] == historical["sep10"]

    s = ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    assert _den(s, 10)["shortage"] == 0 and s["issuable_now"] == 2

    out = ds.retry_all_eligible_pending(db, admin_identity="a", now_utc=OCT)
    assert (out["scanned"], out["issued"], out["still_short"], out["errors"]) == (2, 2, 0, 0)
    for lid in historical["lids"]:
        row = db.affiliate_ledger.find_one({"_id": lid})
        assert row["status"] == "ISSUED" and row["entitlement_month"] == "202609" and row["issued_value"] == 25
        assert {v["code"][:1] for v in row["vouchers"] if v["pool_id"] == P10} <= {"H", "C"}
    # October stock untouched.
    assert db.voucher_pools.count_documents({"batch_id": historical["oct10"], "status": "available"}) == 2


def test_never_pinned_ledger_of_an_ended_month_is_blocked_not_fed_from_other_stock():
    db = _db()
    _stock(db, "202609", p10=0)
    _batch(db, P10, "202610", 5, now=SEP)
    lid = _short(db, user_id=1, tier="T1")   # September, never pinned, evaluated in October
    s = ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    assert s["still_blocked"] == 1 and s["blocked_breakdown"] == {"target_batch_expired_unissued": 1}
    assert _den(s, 10)["required"] == 0
    assert ds.retry_all_eligible_pending(db, now_utc=OCT)["blocked"] == 1
    assert db.affiliate_ledger.find_one({"_id": lid})["status"] == "PENDING_MANUAL"
    assert db.voucher_pools.count_documents({"status": "issued", "issued_for_ledger_id": str(lid)}) == 0


def test_historical_insert_failure_is_reported_not_success(historical, monkeypatch):
    db = historical["db"]
    real = db.voucher_pools.insert_one
    calls = {"n": 0}

    def flaky(doc):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("write failed")
        return real(doc)

    monkeypatch.setattr(db.voucher_pools, "insert_one", flaky)
    out = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609",
                                           denomination=10, codes="F1\nF2\nF3", now_utc=OCT)
    assert out["status"] == "error" and out["reason"] == "database_error" and out["inserted"] == 1
    assert db.voucher_pools.count_documents({"code": "F1"}) == 1  # partial rows are kept, and reported


def test_retry_all_default_limit_matches_the_summary_scan():
    import inspect

    assert inspect.signature(ds.retry_all_eligible_pending).parameters["limit"].default == ds.DEFAULT_SCAN_LIMIT


def test_retry_all_flags_truncation():
    db = _db()
    _stock(db, p10=5)
    for i in (1, 2, 3):
        _short(db, user_id=i, tier="T1")
    out = ds.retry_all_eligible_pending(db, now_utc=SEP, limit=2)
    assert out["scanned"] == 2 and out["issued"] == 2 and out["scan_truncated"] is True
    again = ds.retry_all_eligible_pending(db, now_utc=SEP, limit=2)
    assert again["scanned"] == 1 and again["issued"] == 1 and again["scan_truncated"] is False


def test_current_month_upload_goes_through_the_normal_add_codes_flow():
    db = _db()
    ids = _stock(db, "202610", now=SEP)
    _short(db, user_id=1, tier="T1", month="202610")
    up = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202610",
                                          denomination=10, codes="N1\nN2", now_utc=OCT)
    assert up["status"] == "ok" and up["inserted"] == 2 and up["historical"] is False
    assert db.voucher_pools.find_one({"code": "N1"})["batch_id"] == ids[P10]


# --------------------------------------------------------------------------
# routes
# --------------------------------------------------------------------------
@pytest.fixture
def client(monkeypatch):
    db = _db()
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", db)
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "route_admin"}, None))
    return app.test_client(), db


class _FrozenDatetime(datetime):
    """The routes stamp ``datetime.now``; pin it so the test is clock-free."""

    @classmethod
    def now(cls, tz=None):
        return SEP


def test_routes_summary_upload_retry(client, monkeypatch):
    c, db = client
    monkeypatch.setattr(vouchers, "datetime", _FrozenDatetime)
    month = "202609"
    _stock(db, month, p10=0)
    _short(db, user_id=1, tier="T1", month=month)
    body = c.get("/v2/miniapp/admin/affiliate/pending-manual/summary").get_json()
    assert body["status"] == "ok" and body["pending_count"] == 1
    assert body["denominations"]["10"] == {"pool_id": P10, "required": 1, "available": 0, "available_total": 0, "shortage": 1}
    up = c.post("/v2/miniapp/admin/affiliate/pending-manual/upload-codes",
                json={"denomination": 10, "entitlement_month": month, "codes": "R1\nR1"})
    assert up.status_code == 200 and up.get_json()["inserted"] == 1 and up.get_json()["duplicates"] == 1
    bad = c.post("/v2/miniapp/admin/affiliate/pending-manual/upload-codes",
                 json={"denomination": 7, "entitlement_month": month, "codes": "R2"})
    assert bad.status_code == 400 and bad.get_json()["reason"] == "invalid_denomination"
    res = c.post("/v2/miniapp/admin/affiliate/pending-manual/retry-all")
    assert res.status_code == 200
    out = res.get_json()
    assert {"scanned", "issued", "still_short", "already_issued", "errors"} <= set(out)
    assert out["scanned"] == 1 and out["issued"] == 1 and out["errors"] == 0
    assert db.affiliate_ledger.find_one({})["status"] == "ISSUED"


def test_routes_require_admin(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _db())
    monkeypatch.setattr(vouchers, "require_admin", lambda: (None, ({"status": "error"}, 401)))
    c = app.test_client()
    assert c.get("/v2/miniapp/admin/affiliate/pending-manual/summary").status_code == 401
    assert c.post("/v2/miniapp/admin/affiliate/pending-manual/upload-codes", json={}).status_code == 401
    assert c.post("/v2/miniapp/admin/affiliate/pending-manual/retry-all").status_code == 401
