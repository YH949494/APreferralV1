"""Admin historical pinned-batch replenishment.

A September denomination entitlement whose pinned September batch ran dry
may ONLY be completed from new physical codes added to that exact September
batch -- never from October stock, never by re-pinning, never by moving the
batch's window. Replenish and Approve stay two separate admin actions.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

import pytest
from bson import ObjectId
from flask import Flask

import affiliate_rewards as ar
import affiliate_reward_plans as arp
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
    # Real MongoDB enforces _id uniqueness on every collection; the fake only
    # enforces what it is told to.
    av.HISTORICAL_REPLENISH_LOCK_COLLECTION: [("_id",)],
}


def _batch(db, pool_id, month, codes, *, now):
    out = av.create_batch(
        db, admin_identity="seed", batch_name=f"{pool_id}-{month}", pool_id=pool_id,
        entitlement_month=month, codes=codes, now_utc=now,
    )
    assert out["ok"], out
    return ObjectId(out["batch"]["batch_id"])


def _ledger(db, *, user_id=21, tier="T2", month="202609", status="PENDING_REVIEW", **extra):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY", "user_id": user_id, "status": status, "tier": tier,
        "pool_id": tier, "year_month": month, "entitlement_month": month,
        "reward_plan": arp.resolve_plan_id(month), "bundle_recipe": arp.tier_recipe(month, tier),
        "dedup_key": f"AFF:{user_id}:{month}:{tier}", "voucher_code": None,
        "risk_flags": [], "created_at": SEP, "updated_at": SEP,
    }
    doc.update(extra)
    return db.affiliate_ledger.insert_one(doc).inserted_id


@pytest.fixture
def world():
    """The production shape: a September T2 ($5x1 + $10x2) that secured the
    $5 and one $10 in September, then ran the September $10 batch dry. An
    October $10 batch with stock exists and must stay untouched."""
    db = FakeDb(UNIQUE)
    sep5 = _batch(db, "AFFILIATE_5", "202609", ["S5-1", "S5-2", "S5-3"], now=SEP_START)
    sep10 = _batch(db, "AFFILIATE_10", "202609", ["S10-1"], now=SEP_START)
    oct10 = _batch(db, "AFFILIATE_10", "202610", ["O10-1", "O10-2"], now=SEP)
    lid = _ledger(db)
    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=SEP)
    assert out["status"] == "PENDING_MANUAL"
    assert out["missing_by_denomination"] == {"AFFILIATE_10": 1}
    assert out["shortage_reasons"] == {"AFFILIATE_10": "target_batch_empty"}
    assert out["pool_targets"]["AFFILIATE_10"]["batch_id"] == sep10
    return {"db": db, "lid": lid, "sep5": sep5, "sep10": sep10, "oct10": oct10}


def _replenish(w, codes, *, ledger_id=None, now=OCT, **kw):
    kw.setdefault("pool_id", "AFFILIATE_10")
    return av.replenish_historical_pinned_batch(
        w["db"], ledger_id or w["lid"], admin_identity="ops_admin", codes=codes, now_utc=now, **kw,
    )


def _linked(db, lid):
    return sorted(
        (r["pool_id"], r["code"]) for r in db.voucher_pools.find({"status": "issued", "issued_for_ledger_id": str(lid)})
    )


def _available(db, batch_id):
    return sorted(r["code"] for r in db.voucher_pools.find({"batch_id": batch_id, "status": "available"}))


# 1 ---------------------------------------------------------------------------
def test_replenish_pinned_september_batch_succeeds(world):
    db = world["db"]
    before_batch = db.affiliate_voucher_batches.find_one({"_id": world["sep10"]})

    out = _replenish(world, ["NEW10-1"])

    assert out["status"] == "ok", out
    assert out["ledger_id"] == str(world["lid"])
    assert out["batch_id"] == str(world["sep10"])
    assert out["entitlement_month"] == "202609"
    assert out["inserted"] == 1 and out["duplicates"] == 0
    assert out["denominations_added"] == {"10": 1}
    assert out["message"] == "Historical September 2026 batch replenished. Retry Approve to complete issuance."
    assert _available(db, world["sep10"]) == ["NEW10-1"]
    # Ledger is not touched: still PENDING_MANUAL, nothing issued yet.
    ledger = db.affiliate_ledger.find_one({"_id": world["lid"]})
    assert ledger["status"] == "PENDING_MANUAL"
    assert not ledger.get("voucher_code")
    # Batch identity/window are never moved.
    after_batch = db.affiliate_voucher_batches.find_one({"_id": world["sep10"]})
    for key in ("pool_id", "batch_name", "starts_at", "ends_at", "created_at", "created_by",
                "upload_status", "distribution_disabled"):
        assert after_batch.get(key) == before_batch.get(key), key
    assert after_batch["available_count"] == 1
    assert db.affiliate_voucher_batches.count_documents({}) == 3


# 2, 3, 4 ---------------------------------------------------------------------
def test_retry_after_replenish_issues_full_bundle_reusing_partial_allocation(world):
    db = world["db"]
    partial_before = _linked(db, world["lid"])
    assert partial_before == [("AFFILIATE_10", "S10-1"), ("AFFILIATE_5", "S5-1")]
    partial_ids = sorted(str(r["_id"]) for r in db.voucher_pools.find({"issued_for_ledger_id": str(world["lid"])}))

    assert _replenish(world, ["NEW10-1"])["status"] == "ok"
    out = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT)

    assert out["status"] == "ISSUED"
    assert [v["code"] for v in out["vouchers"]] == ["S5-1", "S10-1", "NEW10-1"]
    assert out["issued_value"] == 25 and out["issued_code_count"] == 3
    # Existing partial allocation rows are the same rows, untouched.
    still = {str(r["_id"]) for r in db.voucher_pools.find({"issued_for_ledger_id": str(world["lid"])})}
    assert set(partial_ids) <= still and len(still) == 3
    # Only the missing denomination was consumed; spare $5 and October stock intact.
    assert _available(db, world["sep5"]) == ["S5-2", "S5-3"]
    assert _available(db, world["sep10"]) == []
    assert _available(db, world["oct10"]) == ["O10-1", "O10-2"]


# 5 ---------------------------------------------------------------------------
def test_duplicate_codes_are_skipped_safely(world):
    db = world["db"]
    # Exists (available) in the October batch -> duplicate, never re-inserted.
    out = _replenish(world, ["O10-1"])
    assert out["status"] == "error" and out["reason"] == "duplicate_code"
    assert _available(db, world["sep10"]) == []

    # Already issued anywhere -> code_already_issued.
    out = _replenish(world, ["S5-1"])
    assert out["status"] == "error" and out["reason"] == "code_already_issued"

    # Mixed: duplicate skipped, new one inserted, deterministic counts.
    out = _replenish(world, "O10-1\nNEW10-1\nNEW10-1")
    assert out["status"] == "ok"
    assert out["inserted"] == 1 and out["duplicates"] == 2
    assert _available(db, world["sep10"]) == ["NEW10-1"]
    assert db.voucher_pools.count_documents({"code": "O10-1"}) == 1


def test_quantity_is_capped_at_the_shortfall(world):
    out = _replenish(world, ["A", "B"])
    assert out["status"] == "error" and out["reason"] == "quantity_exceeds_shortage"
    assert out["replenishable"] == 1
    assert _available(world["db"], world["sep10"]) == []


def test_second_replenish_reports_stock_already_sufficient(world):
    assert _replenish(world, ["NEW10-1"])["status"] == "ok"
    out = _replenish(world, ["NEW10-2"])
    assert out["status"] == "error" and out["reason"] == "stock_already_sufficient"
    assert _available(world["db"], world["sep10"]) == ["NEW10-1"]


def test_invalid_input_rejected_before_any_write(world):
    db = world["db"]
    assert _replenish(world, "")["reason"] == "empty_codes"
    assert _replenish(world, "BAD CODE")["reason"] == "invalid_code"
    assert _replenish(world, ["X"], pool_id=None, denomination="20")["reason"] == "invalid_denomination"
    # $50 is not part of a T2 bundle; $5 is part of it but not short.
    assert _replenish(world, ["X"], pool_id="AFFILIATE_50")["reason"] == "invalid_denomination"
    assert _replenish(world, ["X"], pool_id="AFFILIATE_5")["reason"] == "denomination_not_short"
    # denomination is accepted as an alternative to pool_id.
    assert _replenish(world, ["X"], pool_id=None, denomination=10)["status"] == "ok"
    assert db[av.HISTORICAL_REPLENISH_LOCK_COLLECTION].count_documents({}) == 0


# 6, 7 ------------------------------------------------------------------------
@pytest.mark.parametrize("status,reason", [("ISSUED", "already_issued"), ("REJECTED", "rejected"),
                                           ("SETTLING", "invalid_status"), ("APPROVED", "invalid_status"),
                                           ("PENDING_REVIEW", "invalid_status")])
def test_non_replenishable_statuses_denied(world, status, reason):
    db = world["db"]
    db.affiliate_ledger.update_one({"_id": world["lid"]}, {"$set": {"status": status}})
    out = _replenish(world, ["NEW10-1"])
    assert out["status"] == "error" and out["reason"] == reason
    assert _available(db, world["sep10"]) == []


def test_voucher_code_attached_is_already_issued(world):
    world["db"].affiliate_ledger.update_one({"_id": world["lid"]}, {"$set": {"voucher_code": "X"}})
    assert _replenish(world, ["NEW10-1"])["reason"] == "already_issued"


# 8 ---------------------------------------------------------------------------
def test_ledger_without_pinned_batch_denied(world):
    db = world["db"]
    db.affiliate_ledger.update_one({"_id": world["lid"]}, {"$unset": {"pool_targets": ""}})
    out = _replenish(world, ["NEW10-1"])
    assert out["reason"] == "missing_pinned_batch"
    assert db.voucher_pools.count_documents({"code": "NEW10-1"}) == 0


def test_never_creates_a_historical_batch(world):
    db = world["db"]
    lid = _ledger(db, user_id=22, tier="T1", status="PENDING_MANUAL", month="202609")
    db.affiliate_voucher_batches.delete_one({"_id": world["sep10"]})
    db.affiliate_ledger.update_one({"_id": lid}, {"$set": {"pool_targets.AFFILIATE_10": {
        "mode": "batch", "batch_id": world["sep10"]}}})
    out = _replenish(world, ["NEW10-1"], ledger_id=lid)
    assert out["reason"] == "batch_not_found"
    assert db.affiliate_voucher_batches.count_documents({}) == 2


# 9 ---------------------------------------------------------------------------
def test_batch_month_mismatch_denied(world):
    db = world["db"]
    # A (corrupt) pin onto the October batch must never be topped up here.
    db.affiliate_ledger.update_one({"_id": world["lid"]},
                                   {"$set": {"pool_targets.AFFILIATE_10.batch_id": world["oct10"]}})
    out = _replenish(world, ["NEW10-1"])
    assert out["reason"] == "batch_month_mismatch"
    assert _available(db, world["oct10"]) == ["O10-1", "O10-2"]


def test_pinned_batch_of_other_pool_denied(world):
    world["db"].affiliate_ledger.update_one({"_id": world["lid"]},
                                            {"$set": {"pool_targets.AFFILIATE_10.batch_id": world["sep5"]}})
    assert _replenish(world, ["NEW10-1"])["reason"] == "batch_not_pinned_to_ledger"


# 10, 11 ----------------------------------------------------------------------
def test_client_supplied_other_batch_id_is_denied(world):
    db = world["db"]
    out = _replenish(world, ["NEW10-1"], batch_id=str(world["oct10"]))
    assert out["reason"] == "batch_not_pinned_to_ledger"
    assert db.voucher_pools.count_documents({"code": "NEW10-1"}) == 0
    # The matching pinned id is accepted (it is only ever compared).
    assert _replenish(world, ["NEW10-1"], batch_id=str(world["sep10"]))["batch_id"] == str(world["sep10"])


def test_october_stock_never_satisfies_september_entitlement(world):
    db = world["db"]
    out = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT)
    assert out["status"] == "PENDING_MANUAL"
    assert _available(db, world["oct10"]) == ["O10-1", "O10-2"]
    assert _linked(db, world["lid"]) == [("AFFILIATE_10", "S10-1"), ("AFFILIATE_5", "S5-1")]


def test_current_month_ledger_is_not_historical(world):
    db = world["db"]
    lid = _ledger(db, user_id=23, tier="T1", month="202610", status="PENDING_MANUAL",
                  pool_targets={"AFFILIATE_10": {"mode": "batch", "batch_id": world["oct10"]}})
    out = _replenish(world, ["NEW10-1"], ledger_id=lid)
    assert out["reason"] == "ledger_not_historical"
    assert _available(db, world["oct10"]) == ["O10-1", "O10-2"]


# 12 --------------------------------------------------------------------------
def test_welcome_weekly_and_legacy_ledgers_denied(world):
    db = world["db"]
    welcome = db.affiliate_ledger.insert_one({
        "ledger_type": "WELCOME", "user_id": 30, "tier": "WELCOME", "pool_id": "WELCOME",
        "status": "OUT_OF_STOCK", "dedup_key": "WELCOME:30", "voucher_code": None,
    }).inserted_id
    weekly = _ledger(db, user_id=31, ledger_type="AFFILIATE_WEEKLY", status="PENDING_MANUAL")
    legacy = _ledger(db, user_id=32, tier="T1", month="202608", status="PENDING_MANUAL")
    assert _replenish(world, ["N1"], ledger_id=welcome)["reason"] == "unsupported_ledger_type"
    assert _replenish(world, ["N1"], ledger_id=weekly)["reason"] == "unsupported_ledger_type"
    assert _replenish(world, ["N1"], ledger_id=legacy)["reason"] == "not_denomination_plan"
    assert _replenish(world, ["N1"], ledger_id=ObjectId())["reason"] == "ledger_not_found"
    assert db.voucher_pools.count_documents({"code": "N1"}) == 0


def test_out_of_stock_pinned_ledger_with_zero_allocation(world):
    db = world["db"]
    lid = _ledger(db, user_id=24, tier="T1", status="OUT_OF_STOCK",
                  risk_flags=["bundle_denomination_short"],
                  pool_targets={"AFFILIATE_10": {"mode": "batch", "batch_id": world["sep10"]}})
    out = _replenish(world, ["NEW10-1"], ledger_id=lid)
    assert out["status"] == "ok"
    issued = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)
    assert issued["status"] == "ISSUED"
    assert [v["code"] for v in issued["vouchers"]] == ["NEW10-1"]
    assert _available(db, world["oct10"]) == ["O10-1", "O10-2"]


# 13 --------------------------------------------------------------------------
def test_concurrent_replenish_same_batch_inserts_once(world, monkeypatch):
    db = world["db"]
    barrier = threading.Barrier(2)
    real = av._acquire_replenish_lock

    def racing_lock(db_, *, key, holder):
        barrier.wait(5)
        return real(db_, key=key, holder=holder)

    monkeypatch.setattr(av, "_acquire_replenish_lock", racing_lock)
    results = {}

    def run(name, code):
        results[name] = _replenish(world, [code])

    threads = [threading.Thread(target=run, args=("a", "RACE-A")), threading.Thread(target=run, args=("b", "RACE-B"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    oks = [r for r in results.values() if r["status"] == "ok"]
    assert len(oks) == 1
    loser = next(r for r in results.values() if r["status"] != "ok")
    assert loser["reason"] in ("replenish_in_progress", "stock_already_sufficient")
    assert len(_available(db, world["sep10"])) == 1
    assert db[av.HISTORICAL_REPLENISH_LOCK_COLLECTION].count_documents({}) == 0


def test_same_code_twice_concurrently_never_duplicates(world, monkeypatch):
    """Even with the batch lock bypassed, the unique (pool_id, code) index
    keeps one physical row per code."""
    db = world["db"]
    lid_b = _ledger(db, user_id=25, tier="T1", status="PENDING_MANUAL",
                    pool_targets={"AFFILIATE_10": {"mode": "batch", "batch_id": world["sep10"]}})
    monkeypatch.setattr(av, "_acquire_replenish_lock", lambda db_, *, key, holder: True)
    barrier = threading.Barrier(2)
    real_find = db.voucher_pools.find
    prechecks = []

    def find(query=None, *a, **k):
        out = real_find(query, *a, **k)
        if isinstance(query, dict) and "code" in query and len(prechecks) < 2:
            prechecks.append(1)
            barrier.wait(5)  # both pass the duplicate pre-check before either inserts
        return out

    monkeypatch.setattr(db.voucher_pools, "find", find)
    results = []
    threads = [threading.Thread(target=lambda lid=lid: results.append(_replenish(world, ["SAME-1"], ledger_id=lid)))
               for lid in (world["lid"], lid_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert db.voucher_pools.count_documents({"code": "SAME-1"}) == 1
    assert sorted(r["status"] for r in results) == ["error", "ok"]
    assert next(r for r in results if r["status"] == "error")["reason"] == "duplicate_code"


# 14 --------------------------------------------------------------------------
def test_replenish_during_in_flight_approval_is_refused(world, monkeypatch):
    db = world["db"]
    seen = {}
    real_claim = ar._claim_one_denomination_voucher

    def claim(*a, **k):
        if "replenish" not in seen:
            seen["replenish"] = _replenish(world, ["MID-1"])
        return real_claim(*a, **k)

    monkeypatch.setattr(ar, "_claim_one_denomination_voucher", claim)
    out = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT)

    assert seen["replenish"]["reason"] == "invalid_status"  # ledger was SETTLING
    assert out["status"] == "PENDING_MANUAL"
    assert db.voucher_pools.count_documents({"code": "MID-1"}) == 0
    assert len(_linked(db, world["lid"])) == 2


def test_approval_landing_inside_replenish_then_retry_issues_once(world, monkeypatch):
    db = world["db"]
    calls = {"n": 0}
    real_gate = av._historical_pool_gate

    def gate(*a, **k):
        out = real_gate(*a, **k)
        calls["n"] += 1
        if calls["n"] == 2:  # under the lock, just before insert
            interim = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT)
            assert interim["status"] == "PENDING_MANUAL"
        return out

    monkeypatch.setattr(av, "_historical_pool_gate", gate)
    assert _replenish(world, ["NEW10-1"])["status"] == "ok"
    monkeypatch.setattr(av, "_historical_pool_gate", real_gate)

    out = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT)
    assert out["status"] == "ISSUED"
    assert len(_linked(db, world["lid"])) == 3
    assert db.voucher_pools.count_documents({"issued_for_ledger_id": str(world["lid"])}) == 3


def test_threaded_replenish_and_approve_never_over_allocate(world):
    db = world["db"]
    barrier = threading.Barrier(2)
    results = {}

    def do_replenish():
        barrier.wait(5)
        results["r"] = _replenish(world, ["NEW10-1"])

    def do_approve():
        barrier.wait(5)
        results["a"] = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT)

    threads = [threading.Thread(target=do_replenish), threading.Thread(target=do_approve)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    final = ar.approve_affiliate_ledger(db, ledger_id=world["lid"], now_utc=OCT) \
        if db.affiliate_ledger.find_one({"_id": world["lid"]})["status"] == "PENDING_MANUAL" else None
    row = db.affiliate_ledger.find_one({"_id": world["lid"]})
    linked = _linked(db, world["lid"])
    assert len(linked) <= 3
    assert len({c for _, c in linked}) == len(linked)
    if results["r"]["status"] == "ok":
        assert row["status"] == "ISSUED" and len(linked) == 3
    else:
        assert results["r"]["reason"] == "invalid_status"
        assert row["status"] == "PENDING_MANUAL" and len(linked) == 2
    assert final is None or final["status"] in ("ISSUED", "PENDING_MANUAL")
    assert _available(db, world["oct10"]) == ["O10-1", "O10-2"]


# 15, 16 ----------------------------------------------------------------------
def test_normal_add_codes_expiry_rule_unchanged(world):
    out = av.add_codes_to_batch(world["db"], str(world["sep10"]), admin_identity="x", codes=["LATE-1"], now_utc=OCT)
    assert out["ok"] is False and out["code"] == "batch_expired"
    assert world["db"].voucher_pools.count_documents({"code": "LATE-1"}) == 0


def test_current_month_add_codes_unchanged(world):
    out = av.add_codes_to_batch(world["db"], str(world["oct10"]), admin_identity="x", codes=["O10-3"], now_utc=OCT)
    assert out["ok"] is True and out["inserted_count"] == 1
    assert _available(world["db"], world["oct10"]) == ["O10-1", "O10-2", "O10-3"]


def test_replenished_row_shape_matches_add_codes_rows(world):
    db = world["db"]
    av.add_codes_to_batch(db, str(world["oct10"]), admin_identity="x", codes=["SHAPE-A"], now_utc=OCT)
    assert _replenish(world, ["SHAPE-B"])["status"] == "ok"
    a = db.voucher_pools.find_one({"code": "SHAPE-A"})
    b = db.voucher_pools.find_one({"code": "SHAPE-B"})
    assert set(b) - set(a) == {"upload_source", "historical_replenish_audit_id"}
    assert set(a) <= set(b)
    assert b["voucher_value"] == 10 and b["batch_id"] == world["sep10"]
    assert b["starts_at"] == db.affiliate_voucher_batches.find_one({"_id": world["sep10"]})["starts_at"]


# 17 --------------------------------------------------------------------------
def test_pending_review_issuance_unchanged(world):
    db = world["db"]
    lid = _ledger(db, user_id=26, tier="T1", month="202610")
    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)
    assert out["status"] == "ISSUED"
    assert [v["code"] for v in out["vouchers"]] == ["O10-1"]


# Audit / logging -------------------------------------------------------------
def test_audit_record_and_masked_logs(world, caplog):
    db = world["db"]
    with caplog.at_level(logging.INFO):
        out = _replenish(world, ["SECRETCODE123"])
    assert out["status"] == "ok"
    audit = db[av.HISTORICAL_REPLENISH_AUDIT_COLLECTION].find_one({"_id": ObjectId(out["audit_id"])})
    assert audit["source"] == "admin_historical_replenish"
    assert audit["state"] == "completed"
    assert audit["ledger_id"] == world["lid"] and audit["user_id"] == 21
    assert audit["entitlement_month"] == "202609"
    assert audit["batch_id"] == world["sep10"]
    assert audit["admin_identity"] == "ops_admin"
    assert audit["denominations_added"] == {"10": 1}
    assert audit["created_at"] == OCT
    assert "SECRETCODE123" not in str(audit)
    assert "SECRETCODE123" not in caplog.text
    assert "admin_historical_replenish" in caplog.text


def test_context_reports_pinned_batch_and_shortage(world):
    ctx = av.historical_replenish_context(world["db"], world["lid"], now_utc=OCT)
    assert ctx["status"] == "ok" and ctx["eligible"] is True
    assert ctx["entitlement_month"] == "202609"
    assert ctx["missing_by_denomination"] == {"AFFILIATE_10": 1}
    assert ctx["shortage_reasons"] == {"AFFILIATE_10": "target_batch_empty"}
    by_pool = {p["pool_id"]: p for p in ctx["pools"]}
    assert by_pool["AFFILIATE_10"]["pinned_batch_id"] == str(world["sep10"])
    assert by_pool["AFFILIATE_10"]["replenishable"] == 1
    assert by_pool["AFFILIATE_5"]["missing"] == 0 and by_pool["AFFILIATE_5"]["replenishable"] == 0


# Admin API -------------------------------------------------------------------
class _Cursor(list):
    def sort(self, *_a, **_k):
        return self

    def limit(self, n):
        return _Cursor(self[:n])


class _RouteDb:
    def __init__(self, inner):
        self._inner = inner

    def __getitem__(self, name):
        return self._inner[name]

    def __getattr__(self, name):
        coll = getattr(self._inner, name)
        if name != "affiliate_ledger":
            return coll

        class _Ledger:
            def __getattr__(self, attr):
                return getattr(coll, attr)

            def find(self, query=None, *a, **k):
                return _Cursor(coll.find(query, *a, **k))

        return _Ledger()


@pytest.fixture
def client(world, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _RouteDb(world["db"]))
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "route_admin"}, None))
    return app.test_client()


def test_route_replenish_then_approve(world, client):
    lid = world["lid"]
    ctx = client.get(f"/v2/miniapp/admin/affiliate/{lid}/historical-batch")
    assert ctx.status_code == 200 and ctx.get_json()["eligible"] is True

    res = client.post(f"/v2/miniapp/admin/affiliate/{lid}/historical-batch/replenish",
                      json={"pool_id": "AFFILIATE_10", "codes": "NEW10-1"})
    body = res.get_json()
    assert res.status_code == 200
    assert {k: body[k] for k in ("status", "ledger_id", "batch_id", "entitlement_month", "inserted",
                                 "duplicates", "denominations_added")} == {
        "status": "ok", "ledger_id": str(lid), "batch_id": str(world["sep10"]), "entitlement_month": "202609",
        "inserted": 1, "duplicates": 0, "denominations_added": {"10": 1},
    }
    audit = world["db"][av.HISTORICAL_REPLENISH_AUDIT_COLLECTION].find_one({})
    assert audit["admin_identity"] == "route_admin"

    approve = client.post(f"/v2/miniapp/admin/affiliate/{lid}/approve").get_json()
    assert approve["ledger_status"] == "ISSUED" and approve["issued"] is True


def test_route_error_codes(world, client):
    lid = world["lid"]
    url = f"/v2/miniapp/admin/affiliate/{lid}/historical-batch/replenish"
    assert client.post("/v2/miniapp/admin/affiliate/nope/historical-batch/replenish", json={}).status_code == 400
    res = client.post(f"/v2/miniapp/admin/affiliate/{ObjectId()}/historical-batch/replenish",
                      json={"pool_id": "AFFILIATE_10", "codes": "X"})
    assert res.status_code == 404 and res.get_json()["reason"] == "ledger_not_found"
    res = client.post(url, json={"pool_id": "AFFILIATE_10", "codes": "X", "batch_id": str(world["oct10"])})
    assert res.status_code == 400 and res.get_json()["reason"] == "batch_not_pinned_to_ledger"
    world["db"].affiliate_ledger.update_one({"_id": lid}, {"$set": {"status": "REJECTED"}})
    res = client.post(url, json={"pool_id": "AFFILIATE_10", "codes": "X"})
    assert res.status_code == 409 and res.get_json()["reason"] == "rejected"
    ctx = client.get(f"/v2/miniapp/admin/affiliate/{lid}/historical-batch").get_json()
    assert ctx["eligible"] is False and ctx["reason"] == "rejected"


def test_pending_list_exposes_replenish_fields(world, client):
    items = client.get("/v2/miniapp/admin/affiliate/pending?status=PENDING_MANUAL").get_json()["items"]
    it = next(i for i in items if i["ledger_id"] == str(world["lid"]))
    assert it["ledger_type"] == "AFFILIATE_MONTHLY"
    assert it["entitlement_month"] == "202609"
    assert it["pinned_pools"] == ["AFFILIATE_10", "AFFILIATE_5"]
    assert it["missing_by_denomination"] == {"AFFILIATE_10": 1}
    assert it["shortage_reasons"] == {"AFFILIATE_10": "target_batch_empty"}
