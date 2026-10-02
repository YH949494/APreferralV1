"""Admin retry of OUT_OF_STOCK affiliate ledgers (approve_affiliate_ledger).

OUT_OF_STOCK stays in FINAL_STATUSES for every automatic path. Only the
explicit admin Approve action may reopen it, only for an AFFILIATE_MONTHLY
ledger with no voucher attached, and it must never put a second voucher
bundle on a ledger.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

from flask import Flask

import affiliate_rewards as ar
import affiliate_reward_plans as arp
import vouchers
from fake_mongo import FakeDb
from vouchers import vouchers_bp

SEP = datetime(2026, 9, 15, 4, 0, tzinfo=timezone.utc)
OCT = datetime(2026, 10, 2, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}


def _db():
    return FakeDb(UNIQUE)


def _legacy_ledger(db, *, status="OUT_OF_STOCK", user_id=11, tier="T1", year_month="202608",
                   ledger_type="AFFILIATE_MONTHLY", **extra):
    """A legacy-plan monthly ledger (T1 = 2 codes from the undated T1 pool)."""
    doc = {
        "ledger_type": ledger_type,
        "user_id": user_id,
        "year_month": year_month,
        "tier": tier,
        "pool_id": tier,
        "status": status,
        "dedup_key": f"AFF:{user_id}:{year_month}:{tier}",
        "voucher_code": None,
        "risk_flags": ["pool_empty"] if status == "OUT_OF_STOCK" else [],
        "created_at": SEP,
        "updated_at": SEP,
    }
    doc.update(extra)
    return db.affiliate_ledger.insert_one(doc).inserted_id


def _undated_stock(db, pool_id, codes):
    for code in codes:
        db.voucher_pools.insert_one({"pool_id": pool_id, "code": code, "status": "available"})


def _issued_rows(db, ledger_id):
    return db.voucher_pools.find({"status": "issued", "issued_for_ledger_id": str(ledger_id)})


def _available(db, pool_id):
    return db.voucher_pools.count_documents({"pool_id": pool_id, "status": "available"})


# 1. OUT_OF_STOCK + stock + Approve -> ISSUED with exactly one bundle.
def test_out_of_stock_with_stock_issues_exactly_one_bundle():
    db = _db()
    lid = _legacy_ledger(db)
    _undated_stock(db, "T1", ["T1-A", "T1-B", "T1-C", "T1-D"])

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "ISSUED"
    assert [v["code"] for v in out["vouchers"]] == ["T1-A", "T1-B"]
    assert out["voucher_code"] == "T1-A"
    assert len(_issued_rows(db, lid)) == 2
    assert _available(db, "T1") == 2
    assert "pool_empty" not in (out.get("risk_flags") or [])
    assert out["admin_retry_at"] == OCT


# 2. OUT_OF_STOCK + no stock -> back to OUT_OF_STOCK, nothing consumed.
def test_out_of_stock_without_stock_returns_to_out_of_stock():
    db = _db()
    lid = _legacy_ledger(db)
    _undated_stock(db, "T1", ["ONLY-ONE"])  # T1 needs 2: insufficient

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "OUT_OF_STOCK"
    assert not out.get("voucher_code")
    assert "admin_retry_from_status" not in out
    assert _issued_rows(db, lid) == []
    assert _available(db, "T1") == 1


def test_out_of_stock_stays_final_for_automatic_retry_sweep():
    db = _db()
    assert "OUT_OF_STOCK" in ar.FINAL_STATUSES
    lid = _legacy_ledger(db, year_month="202610")
    _undated_stock(db, "T1", ["T1-A", "T1-B"])

    ar.retry_current_month_pending_manual_ledgers(db, now_utc=OCT, batch_limit=50)

    assert db.affiliate_ledger.find_one({"_id": lid})["status"] == "OUT_OF_STOCK"
    assert _available(db, "T1") == 2


# 3. OUT_OF_STOCK with a voucher_code already attached -> never claims.
def test_out_of_stock_with_existing_voucher_code_is_not_reissued():
    db = _db()
    lid = _legacy_ledger(db, voucher_code="EXISTING")
    _undated_stock(db, "T1", ["T1-A", "T1-B"])

    assert ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT) is None

    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "OUT_OF_STOCK"
    assert row["voucher_code"] == "EXISTING"
    assert _available(db, "T1") == 2
    assert ar.affiliate_approve_refusal_reason(row) == "voucher_already_attached"


# 4. ISSUED + Approve -> no second voucher.
def test_issued_ledger_is_not_reissued():
    db = _db()
    lid = _legacy_ledger(db, status="ISSUED", voucher_code="DONE")
    _undated_stock(db, "T1", ["T1-A", "T1-B"])

    assert ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT) is None

    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "ISSUED" and row["voucher_code"] == "DONE"
    assert _available(db, "T1") == 2
    assert ar.affiliate_approve_refusal_reason(row) == "already_issued"


# 5. REJECTED + Approve -> stays rejected.
def test_rejected_ledger_is_not_reopened():
    db = _db()
    lid = _legacy_ledger(db, status="REJECTED")
    _undated_stock(db, "T1", ["T1-A", "T1-B"])

    assert ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT) is None

    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "REJECTED"
    assert _available(db, "T1") == 2
    assert ar.affiliate_approve_refusal_reason(row) == "rejected"


def test_welcome_out_of_stock_is_not_routed_into_tier_issuance():
    db = _db()
    lid = db.affiliate_ledger.insert_one({
        "ledger_type": "WELCOME", "user_id": 12, "tier": "WELCOME", "pool_id": "WELCOME",
        "status": "OUT_OF_STOCK", "dedup_key": "WELCOME:12", "voucher_code": None,
        "risk_flags": ["welcome_target_batch_empty"], "created_at": SEP,
    }).inserted_id
    _undated_stock(db, "WELCOME", ["W-1"])

    assert ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT) is None

    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "OUT_OF_STOCK"
    assert "missing_pool_config" not in row["risk_flags"]
    assert _available(db, "WELCOME") == 1
    assert ar.affiliate_approve_refusal_reason(row) == "retry_not_supported_for_ledger_type"


# 6. Double / concurrent approval -> exactly one bundle.
def test_double_click_approve_issues_exactly_one_bundle():
    db = _db()
    lid = _legacy_ledger(db)
    _undated_stock(db, "T1", ["T1-A", "T1-B", "T1-C", "T1-D"])

    first = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)
    second = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert first["status"] == "ISSUED"
    assert second is None
    assert len(_issued_rows(db, lid)) == 2
    assert _available(db, "T1") == 2


def test_concurrent_approvals_only_one_enters_issuance(monkeypatch):
    """Interleaving: A reopens, B passes its own first step, A takes
    APPROVED->SETTLING, then B reaches the same step. B must not join A's
    in-flight issuance (it previously could, because SETTLING was accepted)."""
    db = _db()
    lid = _legacy_ledger(db)
    _undated_stock(db, "T1", ["T1-A", "T1-B", "T1-C", "T1-D"])

    a_step1, b_step1, a_issuing, b_done = (threading.Event() for _ in range(4))
    entered = []
    real_finalize = ar._finalize_issued_if_voucher_exists
    real_issue = ar._issue_affiliate_ledger_from_pool
    first_finalize_seen = set()

    def finalize(db_, *, ledger, now_utc):
        name = threading.current_thread().name
        if name not in first_finalize_seen:  # the call right after step 1
            first_finalize_seen.add(name)
            if name == "A":
                a_step1.set()
                assert b_step1.wait(5)
            elif name == "B":
                b_step1.set()
                assert a_issuing.wait(5)
        return real_finalize(db_, ledger=ledger, now_utc=now_utc)

    def issue(db_, ledger, now_utc):
        name = threading.current_thread().name
        entered.append(name)
        if name == "A":
            a_issuing.set()
            assert b_done.wait(5)
        return real_issue(db_, ledger, now_utc)

    monkeypatch.setattr(ar, "_finalize_issued_if_voucher_exists", finalize)
    monkeypatch.setattr(ar, "_issue_affiliate_ledger_from_pool", issue)

    results = {}

    def run(name):
        try:
            results[name] = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)
        finally:
            if name == "B":
                b_done.set()

    ta = threading.Thread(target=run, args=("A",), name="A")
    tb = threading.Thread(target=run, args=("B",), name="B")
    ta.start()
    assert a_step1.wait(5)
    tb.start()
    ta.join(10)
    tb.join(10)

    assert entered == ["A"]
    assert results["A"]["status"] == "ISSUED"
    assert results["B"]["status"] == ar.SETTLING_STATUS  # observed A in flight
    assert len(_issued_rows(db, lid)) == 2
    assert _available(db, "T1") == 2


# 7. Pool already linked to the ledger but ledger lacks voucher_code.
def test_reconciles_already_issued_pool_rows_instead_of_claiming_new():
    db = _db()
    lid = _legacy_ledger(db)
    for code in ("PRE-1", "PRE-2"):
        db.voucher_pools.insert_one({
            "pool_id": "T1", "code": code, "status": "issued", "issued_to_user_id": 11,
            "ledger_id": lid, "issued_for_ledger_id": str(lid), "issued_at": SEP,
        })
    _undated_stock(db, "T1", ["FRESH-1", "FRESH-2"])

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "ISSUED"
    assert sorted(v["code"] for v in out["vouchers"]) == ["PRE-1", "PRE-2"]
    assert _available(db, "T1") == 2


def test_partial_linked_bundle_is_parked_not_topped_up_or_reclosed():
    db = _db()
    lid = _legacy_ledger(db)
    db.voucher_pools.insert_one({
        "pool_id": "T1", "code": "PRE-1", "status": "issued", "issued_to_user_id": 11,
        "ledger_id": lid, "issued_for_ledger_id": str(lid), "issued_at": SEP,
    })
    _undated_stock(db, "T1", ["FRESH-1", "FRESH-2"])

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    # Linked codes exist, so it stays PENDING_MANUAL for review rather than
    # going back to a final OUT_OF_STOCK that would strand PRE-1.
    assert out["status"] == "PENDING_MANUAL"
    assert "partial_bundle_conflict" in out["risk_flags"]
    assert _available(db, "T1") == 2


# 8. Existing PENDING_REVIEW / PENDING_MANUAL behaviour unchanged.
def test_pending_review_approval_still_issues():
    db = _db()
    lid = _legacy_ledger(db, status="PENDING_REVIEW")
    _undated_stock(db, "T1", ["T1-A", "T1-B"])

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "ISSUED"
    assert "admin_retry_from_status" not in out


def test_pending_manual_without_stock_stays_pending_manual():
    db = _db()
    lid = _legacy_ledger(db, status="PENDING_MANUAL")

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "PENDING_MANUAL"
    assert "admin_retry_from_status" not in out
    assert ar.affiliate_approve_outcome_reason(out) == "no_stock"


# Denomination plan (September 2026+): the entitlement month's own batch is
# the only valid source, even when retried in October.
def _denomination_ledger(db, *, pool_targets=None):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY", "user_id": 21, "status": "OUT_OF_STOCK", "tier": "T1",
        "pool_id": "T1", "year_month": "202609", "entitlement_month": "202609",
        "reward_plan": arp.DENOMINATION_PLAN_ID, "bundle_recipe": arp.tier_recipe("202609", "T1"),
        "dedup_key": "AFF:21:202609:T1", "voucher_code": None,
        "risk_flags": ["bundle_denomination_short"], "created_at": SEP, "updated_at": SEP,
    }
    if pool_targets:
        doc["pool_targets"] = pool_targets
    return db.affiliate_ledger.insert_one(doc).inserted_id


def _month_batch(db, *, batch_id, month, codes):
    starts, ends = ar._month_window_from_yyyymm(month)
    db.affiliate_voucher_batches.insert_one({
        "_id": batch_id, "pool_id": "AFFILIATE_10", "starts_at": starts, "ends_at": ends,
        "upload_status": "ready", "distribution_disabled": False,
    })
    for code in codes:
        db.voucher_pools.insert_one({
            "pool_id": "AFFILIATE_10", "code": code, "status": "available",
            "batch_id": batch_id, "voucher_value": 10,
        })
    return starts, ends


def test_september_denomination_retry_issues_from_its_pinned_batch():
    db = _db()
    starts, ends = _month_batch(db, batch_id="SEP10", month="202609", codes=["S10-1"])
    lid = _denomination_ledger(db, pool_targets={
        "AFFILIATE_10": {"mode": "batch", "batch_id": "SEP10", "window_start": starts, "window_end": ends},
    })

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "ISSUED"
    assert [v["code"] for v in out["vouchers"]] == ["S10-1"]


def test_october_stock_does_not_satisfy_a_september_entitlement():
    db = _db()
    _month_batch(db, batch_id="OCT10", month="202610", codes=["O10-1", "O10-2"])
    lid = _denomination_ledger(db)

    out = ar.approve_affiliate_ledger(db, ledger_id=lid, now_utc=OCT)

    assert out["status"] == "OUT_OF_STOCK"
    assert out["shortage_reasons"] == {"AFFILIATE_10": "no_batch_for_entitlement_period"}
    assert _available(db, "AFFILIATE_10") == 2


# Admin API: clear reasons instead of a blanket not_found.
class _Cursor(list):
    def sort(self, *_a, **_k):
        return self

    def limit(self, n):
        return _Cursor(self[:n])


class _RouteDb:
    def __init__(self, inner):
        self._inner = inner

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


def _client(monkeypatch, db):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", _RouteDb(db))
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "admin"}, None))
    return app.test_client()


def test_route_reports_reasons(monkeypatch):
    from bson import ObjectId

    db = _db()
    issued = ObjectId()
    rejected = ObjectId()
    oos = ObjectId()
    _legacy_ledger(db, _id=issued, user_id=31, status="ISSUED", voucher_code="DONE")
    _legacy_ledger(db, _id=rejected, user_id=32, status="REJECTED")
    _legacy_ledger(db, _id=oos, user_id=33)
    client = _client(monkeypatch, db)

    res = client.post(f"/v2/miniapp/admin/affiliate/{ObjectId()}/approve")
    assert res.status_code == 404 and res.get_json()["reason"] == "not_found"

    res = client.post(f"/v2/miniapp/admin/affiliate/{issued}/approve")
    assert res.status_code == 409
    assert res.get_json() == {"status": "error", "reason": "already_issued", "ledger_status": "ISSUED"}

    res = client.post(f"/v2/miniapp/admin/affiliate/{rejected}/approve")
    assert res.status_code == 409 and res.get_json()["reason"] == "rejected"

    res = client.post(f"/v2/miniapp/admin/affiliate/{oos}/approve")
    body = res.get_json()
    assert res.status_code == 200
    assert body["status"] == "ok" and body["issued"] is False
    assert body["ledger_status"] == "OUT_OF_STOCK" and body["reason"] == "no_stock"

    _undated_stock(db, "T1", ["T1-A", "T1-B"])
    body = client.post(f"/v2/miniapp/admin/affiliate/{oos}/approve").get_json()
    assert body["status"] == "ok" and body["issued"] is True
    assert body["ledger_status"] == "ISSUED" and body["voucher_code"] == "T1-A"
    assert body["reason"] is None


def test_pending_list_shows_monthly_out_of_stock_only(monkeypatch):
    db = _db()
    _legacy_ledger(db, user_id=41)
    db.affiliate_ledger.insert_one({
        "ledger_type": "WELCOME", "user_id": 42, "tier": "WELCOME", "pool_id": "WELCOME",
        "status": "OUT_OF_STOCK", "dedup_key": "WELCOME:42", "voucher_code": None,
    })
    client = _client(monkeypatch, db)

    res = client.get("/v2/miniapp/admin/affiliate/pending?status=OUT_OF_STOCK")

    assert res.status_code == 200
    assert [it["user_id"] for it in res.get_json()["items"]] == [41]
