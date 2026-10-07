"""Expired voucher codes must never count as stock and never be issued.

Context: September's $5/$10 codes were retired by the vendor but still sit in
``voucher_pools`` as ``status: available``. A row's redemption validity is the
row-level ``redemption_expires_at`` (absent == no known expiry); it is NOT the
batch window -- an ended batch is still replenished and issued from for pinned
ledgers. The Pending Manual summary, the bulk-retry pre-flight, the historical
upload cap and the allocator's claim all share ``_redemption_valid_clause``.
"""
from __future__ import annotations

import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId
from pymongo import ReturnDocument

import affiliate_denomination_shortage as ds
import affiliate_rewards as ar
import affiliate_voucher_batches as av
import test_affiliate_pending_manual_shortage as base
from scripts import mark_affiliate_codes_redemption_expired as mark

P5, P10, P50 = base.P5, base.P10, base.P50
SEP, OCT = base.SEP, base.OCT
# 2026-10-01 00:00 KL -- the September batches have ended by OCT, and the vendor
# expiry has lapsed. Both facts are independent (see test_ended_batch_alone...).
EXPIRED_AT = datetime(2026, 9, 30, 16, 0, tzinfo=timezone.utc)
F = ar.REDEMPTION_EXPIRY_FIELD


# --------------------------------------------------------------------------
# builders
# --------------------------------------------------------------------------
def _expire(db, batch_id, when=EXPIRED_AT, *, only_available=True):
    q = {"batch_id": batch_id}
    if only_available:
        q["status"] = "available"
    db.voucher_pools.update_many(q, {"$set": {F: when}})


def _pinned(db, ids, *, user_id, tier, month="202609", **extra):
    """A PENDING_MANUAL ledger already pinned to the September batches, as the
    real ones are (the first allocation attempt pinned them)."""
    targets = {p: {"mode": "batch", "batch_id": b} for p, b in ids.items()}
    return base._short(db, user_id=user_id, tier=tier, month=month, pool_targets=targets, **extra)


def _rows(db, batch_id):
    return sorted(db.voucher_pools.find({"batch_id": batch_id}), key=lambda r: r["_id"])


def _snapshot(db, batch_id):
    return [deepcopy(r) for r in _rows(db, batch_id)]


def _expired_codes(db):
    return {r["code"] for r in db.voucher_pools.find({F: {"$ne": None}}) if r.get(F) is not None}


# --------------------------------------------------------------------------
# 1-3  each denomination: expired stock is not stock
# --------------------------------------------------------------------------
@pytest.mark.parametrize("pool,tier,denom,stock_kw,qty", [
    (P5, "T2", 5, "p5", 1),
    (P10, "T1", 10, "p10", 1),
    (P50, "T5", 50, "p50", 7),
])
def test_expired_stock_is_excluded_per_denomination(pool, tier, denom, stock_kw, qty):
    db = base._db()
    ids = base._stock(db, **{stock_kw: qty})
    _expire(db, ids[pool])
    _pinned(db, ids, user_id=1, tier=tier)
    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT), denom)
    own = d["required"]
    assert d["raw_available"] == qty
    assert d["usable_available"] == 0 and d["available"] == 0 and d["available_compatible"] == 0
    assert d["expired_excluded"] == qty
    assert d["shortage"] == own and d["uploadable_shortage"] == own   # expired rows do NOT reduce Need To Upload


# --------------------------------------------------------------------------
# the exact production picture, key for key
# --------------------------------------------------------------------------
def test_production_september_case_matches_the_requested_response():
    db = base._db()
    ids = base._stock(db, p5=9, p10=59, p50=0)
    _expire(db, ids[P5])
    _expire(db, ids[P10])
    uid = 0
    for tier, n in (("T2", 9), ("T1", 17), ("T3", 24)):   # 9x$5 ; 18+17+24 = 59x$10 ; 24x$50
        for _ in range(n):
            uid += 1
            _pinned(db, ids, user_id=uid, tier=tier)
    s = ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    got = {k: {f: v[f] for f in ("required", "raw_available", "usable_available", "expired_excluded", "shortage")}
           for k, v in s["denominations"].items()}
    assert got == {
        "5": {"required": 9, "raw_available": 9, "usable_available": 0, "expired_excluded": 9, "shortage": 9},
        "10": {"required": 59, "raw_available": 59, "usable_available": 0, "expired_excluded": 59, "shortage": 59},
        "50": {"required": 24, "raw_available": 0, "usable_available": 0, "expired_excluded": 0, "shortage": 24},
    }
    assert s["issuable_now"] == 0
    # per-month view carries the same split
    m = s["by_month"]["202609"]
    assert (m["5"]["raw_available"], m["5"]["usable_available"], m["5"]["expired_excluded"]) == (9, 0, 9)


# --------------------------------------------------------------------------
# 4-5  mixed expired + valid; raw != usable
# --------------------------------------------------------------------------
def test_mixed_expired_and_valid_stock_and_raw_differs_from_usable():
    db = base._db()
    ids = base._stock(db, p50=10)
    rows = _rows(db, ids[P50])
    for r in rows[:6]:                                   # oldest six expired, newest four valid
        db.voucher_pools.update_one({"_id": r["_id"]}, {"$set": {F: EXPIRED_AT}})
    _pinned(db, ids, user_id=1, tier="T5")               # owes 7 x $50
    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT), 50)
    assert d["raw_available"] == 10 and d["usable_available"] == 4 and d["expired_excluded"] == 6
    assert d["raw_available"] == d["available_total"] + d["expired_excluded"]
    assert (d["required"], d["shortage"]) == (7, 3)      # 4 valid cover 4; the 6 expired cover nothing


def test_unissued_filter_still_excludes_reserved_and_issued_rows():
    db = base._db()
    ids = base._stock(db, p10=4)
    r = _rows(db, ids[P10])
    db.voucher_pools.update_one({"_id": r[0]["_id"]}, {"$set": {"issued_for_ledger_id": "someone"}})
    db.voucher_pools.update_one({"_id": r[1]["_id"]}, {"$set": {"status": "issued"}})
    db.voucher_pools.update_one({"_id": r[2]["_id"]}, {"$set": {F: EXPIRED_AT}})
    _pinned(db, ids, user_id=1, tier="T4")
    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT), 10)
    assert (d["raw_available"], d["usable_available"], d["expired_excluded"]) == (2, 1, 1)


def test_expiry_boundary_is_exclusive_and_future_expiry_is_still_usable():
    db = base._db()
    ids = base._stock(db, p10=2)
    r = _rows(db, ids[P10])
    db.voucher_pools.update_one({"_id": r[0]["_id"]}, {"$set": {F: OCT}})                       # expires exactly now
    db.voucher_pools.update_one({"_id": r[1]["_id"]}, {"$set": {F: OCT + timedelta(days=30)}})  # still valid
    _pinned(db, ids, user_id=1, tier="T1")
    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT), 10)
    assert (d["available_total"], d["expired_excluded"]) == (1, 1)
    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT - timedelta(seconds=1)), 10)
    assert (d["available_total"], d["expired_excluded"]) == (2, 0)   # one second earlier it had not lapsed yet


def test_ended_batch_alone_does_not_make_codes_expired():
    """Batch ``ends_at`` and redemption expiry are different concepts: codes
    with no expiry metadata in an ENDED batch stay usable for pinned ledgers."""
    db = base._db()
    ids = base._stock(db, p10=3)
    _pinned(db, ids, user_id=1, tier="T1")
    s = ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    d = base._den(s, 10)
    assert s["by_month"]["202609"]["10"]["historical"] is True
    assert (d["raw_available"], d["usable_available"], d["expired_excluded"], d["shortage"]) == (3, 1, 0, 0)
    assert F not in db.voucher_pools.find_one({"batch_id": ids[P10]})
    assert ds.retry_all_eligible_pending(db, now_utc=OCT)["issued"] == 1


# --------------------------------------------------------------------------
# 6  expired codes stay stored for audit
# --------------------------------------------------------------------------
def test_expired_codes_remain_stored_untouched_through_summary_upload_and_retry():
    db = base._db()
    ids = base._stock(db, p5=3)
    _expire(db, ids[P5])
    before = _snapshot(db, ids[P5])
    for i in range(1, 4):
        _pinned(db, ids, user_id=i, tier="T2")
    ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert _snapshot(db, ids[P5]) == before
    assert len(before) == 3 and all(r["status"] == "available" and r[F] == EXPIRED_AT for r in before)


# --------------------------------------------------------------------------
# 7  the allocator / retry path refuses expired codes
# --------------------------------------------------------------------------
def test_retry_never_issues_an_expired_code():
    db = base._db()
    ids = base._stock(db, p10=3)
    _expire(db, ids[P10])
    lids = [_pinned(db, ids, user_id=i, tier="T1") for i in (1, 2, 3)]

    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 0 and out["still_short"] == 3 and out["errors"] == 0
    assert db.voucher_pools.count_documents({"status": "issued", "issued_for_ledger_id": {"$ne": "seed"}}) == 0

    # The allocator itself (row-level Approve / 5-minute sweep) refuses too, even
    # when the pre-flight is bypassed.
    for lid in lids:
        res = ar._issue_affiliate_ledger_from_pool(db, ledger=db.affiliate_ledger.find_one({"_id": lid}), now_utc=OCT)
        assert res["status"] == "PENDING_MANUAL" and not res.get("vouchers")
    assert all(r["status"] == "available" for r in _rows(db, ids[P10]))

    # And the primitive claim, under the narrow "continue a pinned ended batch" exception.
    voucher, reason = ar._claim_from_target_batch(
        db, batch_id=ids[P10], pool_id=P10, ledger_id=lids[0], user_id=1, now_utc=OCT, allow_expired_pinned=True)
    assert voucher is None and reason == "target_batch_empty"


def test_retry_skips_expired_codes_that_sort_before_valid_ones():
    db = base._db()
    ids = base._stock(db, p10=5)
    rows = _rows(db, ids[P10])
    for r in rows[:3]:
        db.voucher_pools.update_one({"_id": r["_id"]}, {"$set": {F: EXPIRED_AT}})
    expired = {r["code"] for r in rows[:3]}
    valid = {r["code"] for r in rows[3:]}
    for i in (1, 2):
        _pinned(db, ids, user_id=i, tier="T1")
    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 2
    issued = set(base._issued_codes(db))
    assert issued == valid and not (issued & expired)


def test_legacy_undated_claim_also_refuses_expired_codes():
    db = base._db()
    db.voucher_pools.insert_one({"pool_id": "T1", "code": "L-OLD", "status": "available", F: EXPIRED_AT})
    db.voucher_pools.insert_one({"pool_id": "T1", "code": "L-NEW", "status": "available"})
    v = ar._claim_legacy_voucher(db, pool_id="T1", ledger_id=ObjectId(), user_id=1, now_utc=OCT)
    assert v["code"] == "L-NEW"
    assert ar._claim_legacy_voucher(db, pool_id="T1", ledger_id=ObjectId(), user_id=2, now_utc=OCT) is None
    assert ar._available_pool_count(db, pool_id="T1", now_utc=OCT, legacy_only=True) == 0


def test_claimable_inventory_counts_ignore_expired_codes():
    db = base._db()
    ids = base._stock(db, p10=3)
    rows = _rows(db, ids[P10])
    db.voucher_pools.update_one({"_id": rows[0]["_id"]}, {"$set": {F: EXPIRED_AT}})
    batch = db.affiliate_voucher_batches.find_one({"_id": ids[P10]})
    assert ar._batch_claimable_available_count(db, batch, OCT) == 2
    assert ar._batch_claimable_available_count(db, batch, EXPIRED_AT - timedelta(seconds=1)) == 3


# --------------------------------------------------------------------------
# 8-9  replacement codes become usable; old rows are never mutated
# --------------------------------------------------------------------------
def test_replacement_upload_is_usable_and_never_mutates_the_expired_codes():
    db = base._db()
    ids = base._stock(db, p5=2, p10=4)                    # T2 = 1 x $5 + 2 x $10; the $10 side is valid stock
    _expire(db, ids[P5])
    old = _snapshot(db, ids[P5])
    for i in (1, 2):
        _pinned(db, ids, user_id=i, tier="T2")
    s = ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    assert base._den(s, 5)["shortage"] == 2 and base._den(s, 5)["expired_excluded"] == 2

    # The historical-upload cap is the USABLE shortage: expired rows do not shrink it.
    up = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609", denomination=5,
                                          codes="NEW5-1\nNEW5-2", now_utc=OCT)
    assert up["status"] == "ok" and up["inserted"] == 2 and up["historical"] is True
    over = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609", denomination=5,
                                            codes="NEW5-3", now_utc=OCT)
    assert over["reason"] == "quantity_exceeds_shortage" and over["replenishable"] == 0

    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT), 5)
    assert (d["raw_available"], d["usable_available"], d["expired_excluded"], d["shortage"]) == (4, 2, 2, 0)

    # Old rows byte-identical; the new ones carry no expiry.
    assert [r for r in _snapshot(db, ids[P5]) if r["code"] in {o["code"] for o in old}] == old
    fresh = [r for r in _rows(db, ids[P5]) if r["code"].startswith("NEW5")]
    assert len(fresh) == 2 and all(F not in r and r["status"] == "available" for r in fresh)

    # Re-uploading a retired code is refused as a duplicate: old codes are never reused.
    again = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609", denomination=5,
                                             codes=old[0]["code"], now_utc=OCT)
    assert again["status"] == "error" and again["reason"] == "duplicate_code"

    # Retry issues the replacements only.
    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 2 and out["errors"] == 0
    issued5 = {r["code"] for r in db.voucher_pools.find({"pool_id": P5, "status": "issued"})}
    assert issued5 == {"NEW5-1", "NEW5-2"}
    assert [r for r in _snapshot(db, ids[P5]) if r["code"] in {o["code"] for o in old}] == old


def test_replacement_codes_complete_the_bundle_end_to_end():
    """T1 owes one $10. Old $10 expired -> upload one fresh $10 -> exactly that code is issued."""
    db = base._db()
    ids = base._stock(db, p10=2)
    _expire(db, ids[P10])
    old = _snapshot(db, ids[P10])
    lid = _pinned(db, ids, user_id=1, tier="T1")
    assert ds.retry_all_eligible_pending(db, now_utc=OCT)["issued"] == 0

    up = ds.upload_codes_for_denomination(db, admin_identity="a", entitlement_month="202609", denomination=10,
                                          codes="FRESH-10", now_utc=OCT)
    assert up["inserted"] == 1
    s = ds.summarize_pending_manual_shortage(db, now_utc=OCT)
    d = base._den(s, 10)
    assert (d["usable_available"], d["expired_excluded"], d["shortage"], s["issuable_now"]) == (1, 2, 0, 1)

    out = ds.retry_all_eligible_pending(db, now_utc=OCT)
    assert out["issued"] == 1 and out["errors"] == 0
    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "ISSUED" and [v["code"] for v in row["vouchers"]] == ["FRESH-10"]
    assert _snapshot(db, ids[P10])[:2] == old and _snapshot(db, ids[P10])[:2][0]["status"] == "available"


def test_historical_replenish_gate_is_not_fooled_by_expired_codes():
    """The per-ledger modal path must offer replenishment, not 'stock already sufficient'."""
    db = base._db()
    ids = base._stock(db, p10=3)
    _expire(db, ids[P10])
    lid = _pinned(db, ids, user_id=1, tier="T1")
    ctx = av.historical_replenish_context(db, lid, now_utc=OCT)
    pool = next(p for p in ctx["pools"] if p["pool_id"] == P10)
    assert pool["available_in_batch"] == 0 and pool["replenishable"] == 1 and pool["blocked_reason"] is None


# --------------------------------------------------------------------------
# 10  concurrency
# --------------------------------------------------------------------------
def test_concurrent_retries_never_double_issue_and_never_issue_expired(monkeypatch):
    db = base._db()
    n = 6
    ids = base._stock(db, p10=n + 6)
    rows = _rows(db, ids[P10])
    for r in rows[:6]:                                    # lowest _ids -- first in line for any claimer
        db.voucher_pools.update_one({"_id": r["_id"]}, {"$set": {F: EXPIRED_AT}})
    expired = {r["code"] for r in rows[:6]}
    lids = [_pinned(db, ids, user_id=i, tier="T1") for i in range(1, n + 1)]
    monkeypatch.setattr(ds, "_acquire_bulk_lock", lambda *_a, **_k: True)
    monkeypatch.setattr(ds, "_renew_bulk_lock", lambda *_a, **_k: True)
    barrier = threading.Barrier(2)
    results = []

    def run():
        barrier.wait()
        results.append(ds.retry_all_eligible_pending(db, now_utc=OCT))

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r["issued"] for r in results) == n and sum(r["errors"] for r in results) == 0
    codes = base._issued_codes(db)
    assert len(codes) == len(set(codes)) == n and not (set(codes) & expired)
    for lid in lids:
        assert len(base._linked(db, lid)) == 1
    assert all(r["status"] == "available" and r[F] == EXPIRED_AT for r in _rows(db, ids[P10])[:6])


def test_concurrent_claims_on_a_batch_with_one_valid_code_issue_it_once():
    db = base._db()
    ids = base._stock(db, p10=3)
    rows = _rows(db, ids[P10])
    for r in rows[:2]:
        db.voucher_pools.update_one({"_id": r["_id"]}, {"$set": {F: EXPIRED_AT}})
    barrier = threading.Barrier(4)
    won = []

    def claim(uid):
        barrier.wait()
        v, _ = ar._claim_from_target_batch(db, batch_id=ids[P10], pool_id=P10, ledger_id=ObjectId(), user_id=uid,
                                           now_utc=OCT, allow_expired_pinned=True)
        if v:
            won.append(v["code"])

    threads = [threading.Thread(target=claim, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert won == [rows[2]["code"]]


# --------------------------------------------------------------------------
# operator stamping script
# --------------------------------------------------------------------------
def _script_fixture():
    db = base._db()
    ids = base._stock(db, p5=3, p10=4)
    cutoff = datetime(2026, 10, 7, tzinfo=timezone.utc)
    # one issued + one reserved row must be left alone
    r10 = _rows(db, ids[P10])
    db.voucher_pools.update_one({"_id": r10[0]["_id"]}, {"$set": {"status": "issued"}})
    db.voucher_pools.update_one({"_id": r10[1]["_id"]}, {"$set": {"issued_for_ledger_id": "x"}})
    return db, ids, cutoff


def _kw(db_cutoff, **over):
    kw = dict(entitlement_month="202609", pool_ids=[P5, P10], created_before=db_cutoff,
              expires_at=EXPIRED_AT, admin_identity="tester", now_utc=OCT)
    kw.update(over)
    return kw


def test_mark_script_dry_run_writes_nothing():
    db, ids, cutoff = _script_fixture()
    before = _snapshot(db, ids[P5]) + _snapshot(db, ids[P10])
    rep = mark.mark_redemption_expired(db, **_kw(cutoff))
    assert rep["dry_run"] is True and rep["matched"] == 3 + 2 and rep["stamped"] == 0
    assert _snapshot(db, ids[P5]) + _snapshot(db, ids[P10]) == before


def test_mark_script_commit_stamps_only_old_available_unreserved_rows_and_is_idempotent():
    db, ids, cutoff = _script_fixture()
    # a replacement code uploaded AFTER the cutoff must survive
    db.voucher_pools.insert_one({"pool_id": P5, "code": "REPL-5", "batch_id": ids[P5], "status": "available",
                                 "created_at": cutoff + timedelta(hours=1)})
    before = {r["code"]: deepcopy(r) for r in db.voucher_pools.find({})}
    rep = mark.mark_redemption_expired(db, commit=True, **_kw(cutoff))
    assert rep["matched"] == rep["stamped"] == 5
    for r in db.voucher_pools.find({}):
        old = before[r["code"]]
        stamped = F in r and r.get(F) is not None
        if r["code"] == "REPL-5" or old["status"] == "issued" or old.get("issued_for_ledger_id"):
            assert not stamped and r == old
        else:
            assert stamped and r[F] == EXPIRED_AT and r["redemption_expiry_marked_by"] == "tester"
            core = {k: v for k, v in r.items() if not k.startswith("redemption_")}
            assert core == old                           # nothing but the four metadata fields changed
    again = mark.mark_redemption_expired(db, commit=True, **_kw(cutoff))
    assert again["matched"] == again["stamped"] == 0
    s_after = db.voucher_pools.count_documents({})
    assert s_after == len(before)                        # no deletes, no inserts

    # and the summary now treats exactly those rows as expired
    _pinned(db, ids, user_id=1, tier="T2")
    d = base._den(ds.summarize_pending_manual_shortage(db, now_utc=OCT), 5)
    assert (d["raw_available"], d["usable_available"], d["expired_excluded"]) == (4, 1, 3)


def test_mark_script_cli_requires_expect_count_and_validates(capsys):
    db, ids, cutoff = _script_fixture()
    args = ["--entitlement-month", "202609", "--denomination", "5", "--denomination", "10",
            "--created-before", "2026-10-07T00:00:00+00:00"]
    assert mark.main(args + ["--commit"], db_factory=lambda: db) == 2                    # no --expect-count
    assert mark.main(args + ["--commit", "--expect-count", "99"], db_factory=lambda: db) == 2
    assert db.voucher_pools.count_documents({F: {"$ne": None}}) == 0
    assert mark.main(["--entitlement-month", "202609", "--denomination", "5",
                      "--created-before", "2026-10-07T00:00:00"], db_factory=lambda: db) == 2   # naive timestamp
    assert mark.main(args, db_factory=lambda: db) == 0                                    # dry run
    assert db.voucher_pools.count_documents({F: {"$ne": None}}) == 0
    assert mark.main(args + ["--commit", "--expect-count", "5"], db_factory=lambda: db) == 0
    assert db.voucher_pools.count_documents({F: {"$ne": None}}) == 5
