"""POST /admin/affiliate/reject-bulk: loop over reject_affiliate_ledger with a
status guard that cannot be raced past an approval/issuance."""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

from bson import ObjectId
from flask import Flask

import affiliate_rewards as ar
import vouchers
from fake_mongo import FakeDb
from vouchers import vouchers_bp

SEP = datetime(2026, 9, 15, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}
URL = "/v2/miniapp/admin/affiliate/reject-bulk"
APPROVE_URL = "/v2/miniapp/admin/affiliate/approve-bulk"


def _db():
    return FakeDb(UNIQUE)


def _ledger(db, uid, status="PENDING_REVIEW", **extra):
    doc = {
        "ledger_type": "AFFILIATE_MONTHLY", "user_id": uid, "year_month": "202608", "tier": "T1",
        "pool_id": "T1", "status": status, "dedup_key": f"AFF:{uid}:202608:T1", "voucher_code": None,
        "risk_flags": [], "created_at": SEP, "updated_at": SEP,
    }
    doc.update(extra)
    return db.affiliate_ledger.insert_one(doc).inserted_id


def _stock(db, n, pool="T1"):
    for i in range(n):
        db.voucher_pools.insert_one({"pool_id": pool, "code": f"{pool}-{i}", "status": "available"})


def _issued_for(db, lid):
    return db.voucher_pools.count_documents({"status": "issued", "issued_for_ledger_id": str(lid)})


def _status(db, lid):
    return db.affiliate_ledger.find_one({"_id": lid})["status"]


def _client(monkeypatch, db, admin=True):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", db)
    if admin:
        monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    else:
        monkeypatch.setattr(vouchers, "require_admin", lambda: (None, ({"status": "error"}, 403)))
    return app.test_client()


def _post(client, ids, **extra):
    return client.post(URL, json={"ledger_ids": [str(i) for i in ids], **extra})


def _by_id(body):
    return {r["ledger_id"]: r for r in body["results"]}


def test_rejects_five_pending_rows(monkeypatch, caplog):
    db = _db()
    ids = [_ledger(db, 100 + i, status="PENDING_REVIEW" if i % 2 else "PENDING_MANUAL") for i in range(5)]
    with caplog.at_level(logging.INFO):
        res = _post(_client(monkeypatch, db), ids)
    body = res.get_json()
    assert res.status_code == 200
    assert (body["requested"], body["processed"], body["rejected"], body["already_final"],
            body["skipped"], body["failed"]) == (5, 5, 5, 0, 0, 0)
    for lid in ids:
        row = db.affiliate_ledger.find_one({"_id": lid})
        assert row["status"] == "REJECTED" and row["review_reason"] == "bulk_admin_reject"
    for r in body["results"]:
        assert set(r) == {"ledger_id", "result", "previous_status", "current_status"}
        assert r["result"] == "rejected" and r["current_status"] == "REJECTED"
    assert any("[AFFILIATE][BULK_REJECT] requested=5 processed=5 rejected=5 already_final=0 skipped=0 failed=0" in r.getMessage()
               for r in caplog.records)


def test_approved_status_is_rejectable_and_custom_reason_stored(monkeypatch):
    db = _db()
    a = _ledger(db, 1, status="APPROVED")
    body = _post(_client(monkeypatch, db), [a], reason="fraud ring").get_json()
    assert body["rejected"] == 1 and _by_id(body)[str(a)]["previous_status"] == "APPROVED"
    assert db.affiliate_ledger.find_one({"_id": a})["review_reason"] == "fraud ring"


def test_duplicate_ids_processed_once(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    body = _post(_client(monkeypatch, db), [a, a, a]).get_json()
    assert (body["requested"], body["processed"], body["rejected"], len(body["results"])) == (1, 1, 1, 1)


def test_invalid_object_id_rejects_request_without_side_effects(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    res = _client(monkeypatch, db).post(URL, json={"ledger_ids": [str(a), "not-an-oid"]})
    assert res.status_code == 400 and res.get_json()["reason"] == "bad_ledger_id"
    assert _status(db, a) == "PENDING_REVIEW"


def test_missing_empty_or_non_list_ledger_ids(monkeypatch):
    client = _client(monkeypatch, _db())
    assert client.post(URL, json={}).status_code == 400
    assert client.post(URL, json={"ledger_ids": []}).status_code == 400
    assert client.post(URL, json={"ledger_ids": "abc"}).status_code == 400


def test_more_than_200_ids_rejected(monkeypatch):
    db = _db()
    client = _client(monkeypatch, db)
    res = _post(client, [ObjectId() for _ in range(201)])
    assert res.status_code == 400 and res.get_json()["reason"] == "too_many_ledger_ids"
    assert _post(client, [ObjectId() for _ in range(200)]).status_code == 200


def test_already_rejected_is_idempotent_and_untouched(monkeypatch):
    db = _db()
    a = _ledger(db, 1, status="REJECTED", review_reason="earlier")
    body = _post(_client(monkeypatch, db), [a]).get_json()
    assert (body["processed"], body["rejected"], body["already_final"], body["skipped"]) == (1, 0, 1, 0)
    assert _by_id(body)[str(a)]["result"] == "already_rejected"
    assert db.affiliate_ledger.find_one({"_id": a})["review_reason"] == "earlier"  # not overwritten


def test_protected_statuses_never_touched(monkeypatch):
    db = _db()
    issued = _ledger(db, 1, status="ISSUED", voucher_code="SECRET-1")
    oos = _ledger(db, 2, status="OUT_OF_STOCK", risk_flags=["pool_empty"])
    sim = _ledger(db, 3, status="SIMULATED_PENDING", simulate=True)
    settling = _ledger(db, 4, status="SETTLING")
    body = _post(_client(monkeypatch, db), [issued, oos, sim, settling]).get_json()
    assert (body["rejected"], body["processed"], body["skipped"], body["failed"]) == (0, 0, 4, 0)
    res = _by_id(body)
    assert res[str(issued)]["reason"] == "already_issued"
    assert res[str(settling)]["reason"] == "in_progress"
    assert res[str(oos)]["reason"] == res[str(sim)]["reason"] == "status_not_rejectable"
    assert [_status(db, i) for i in (issued, oos, sim, settling)] == ["ISSUED", "OUT_OF_STOCK", "SIMULATED_PENDING", "SETTLING"]
    assert db.affiliate_ledger.find_one({"_id": issued})["voucher_code"] == "SECRET-1"
    assert "SECRET-1" not in str(body)  # never leaks codes
    assert all(r["current_status"] == r["previous_status"] for r in body["results"])


def test_mixed_status_batch_and_unknown_id(monkeypatch):
    db = _db()
    pend = _ledger(db, 1)
    manual = _ledger(db, 2, status="PENDING_MANUAL")
    rej = _ledger(db, 3, status="REJECTED")
    issued = _ledger(db, 4, status="ISSUED", voucher_code="C")
    ghost = ObjectId()
    body = _post(_client(monkeypatch, db), [pend, manual, rej, issued, ghost]).get_json()
    assert (body["requested"], body["processed"], body["rejected"], body["already_final"],
            body["skipped"], body["failed"]) == (5, 3, 2, 1, 1, 1)
    assert body["processed"] + body["skipped"] + body["failed"] == body["requested"]
    assert _by_id(body)[str(ghost)] == {"ledger_id": str(ghost), "result": "failed", "previous_status": None,
                                        "current_status": None, "error": "not_found"}
    assert _status(db, issued) == "ISSUED"


def test_repeated_bulk_reject_is_retry_safe(monkeypatch):
    db = _db()
    ids = [_ledger(db, i) for i in range(4)]
    client = _client(monkeypatch, db)
    first = _post(client, ids, reason="first").get_json()
    second = _post(client, ids, reason="second").get_json()
    assert first["rejected"] == 4 and second["rejected"] == 0 and second["already_final"] == 4
    assert second["failed"] == 0 and second["skipped"] == 0
    assert all(db.affiliate_ledger.find_one({"_id": i})["review_reason"] == "first" for i in ids)


def test_concurrent_identical_requests_reject_each_ledger_once(monkeypatch):
    db = _db()
    ids = [_ledger(db, i) for i in range(10)]
    client = _client(monkeypatch, db)
    out = []
    threads = [threading.Thread(target=lambda: out.append(_post(client, ids).get_json())) for _ in range(2)]
    [t.start() for t in threads]
    [t.join(15) for t in threads]
    assert len(out) == 2
    assert sum(b["rejected"] for b in out) == 10  # each ledger won by exactly one request
    assert all(b["failed"] == 0 and b["rejected"] + b["already_final"] == 10 for b in out)


def test_auth_required(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    res = _post(_client(monkeypatch, db, admin=False), [a])
    assert res.status_code == 403 and _status(db, a) == "PENDING_REVIEW"


def test_one_failure_does_not_abort_batch(monkeypatch):
    db = _db()
    ids = [_ledger(db, 10 + i) for i in range(3)]
    real = vouchers.reject_affiliate_ledger

    def flaky(db_, *, ledger_id, **kw):
        if ledger_id == ids[1]:
            raise RuntimeError("boom")
        return real(db_, ledger_id=ledger_id, **kw)

    monkeypatch.setattr(vouchers, "reject_affiliate_ledger", flaky)
    body = _post(_client(monkeypatch, db), ids).get_json()
    assert (body["rejected"], body["failed"]) == (2, 1)
    assert _by_id(body)[str(ids[1])]["error"] == "exception" and "boom" not in str(body)
    assert [_status(db, i) for i in ids] == ["REJECTED", "PENDING_REVIEW", "REJECTED"]


# ---- races -----------------------------------------------------------------

def test_ledger_issued_between_precheck_and_reject_is_not_reversed(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    b = _ledger(db, 2)
    real = vouchers.reject_affiliate_ledger

    def issue_then_reject(db_, *, ledger_id, **kw):
        if ledger_id == a:  # approver wins the instant before our conditional update
            db_.affiliate_ledger.update_one({"_id": a}, {"$set": {"status": "ISSUED", "voucher_code": "WON"}})
        return real(db_, ledger_id=ledger_id, **kw)

    monkeypatch.setattr(vouchers, "reject_affiliate_ledger", issue_then_reject)
    body = _post(_client(monkeypatch, db), [a, b]).get_json()
    res = _by_id(body)
    assert res[str(a)]["result"] == "skipped" and res[str(a)]["reason"] == "already_issued"
    assert (body["rejected"], body["skipped"]) == (1, 1)
    row = db.affiliate_ledger.find_one({"_id": a})
    assert row["status"] == "ISSUED" and row["voucher_code"] == "WON"


def test_ledger_entering_settling_mid_run_is_not_rejected(monkeypatch):
    db = _db()
    a = _ledger(db, 1, status="APPROVED")
    real = vouchers.reject_affiliate_ledger

    def settle_then_reject(db_, *, ledger_id, **kw):
        db_.affiliate_ledger.update_one({"_id": a}, {"$set": {"status": "SETTLING"}})
        return real(db_, ledger_id=ledger_id, **kw)

    monkeypatch.setattr(vouchers, "reject_affiliate_ledger", settle_then_reject)
    body = _post(_client(monkeypatch, db), [a]).get_json()
    assert body["skipped"] == 1 and _by_id(body)[str(a)]["reason"] == "in_progress"
    assert _status(db, a) == "SETTLING"


def test_reject_after_approve_claimed_but_before_settling_blocks_issuance(monkeypatch):
    """Reject lands in the APPROVED window: approve's APPROVED->SETTLING claim
    then fails, so no voucher is allocated to a rejected ledger."""
    db = _db()
    a = _ledger(db, 1)
    _stock(db, 4)
    real_finalize = ar._finalize_issued_if_voucher_exists
    fired = []

    def finalize_then_reject_once(db_, *, ledger, now_utc):
        out = real_finalize(db_, ledger=ledger, now_utc=now_utc)
        if not fired and out and out.get("status") == "APPROVED":
            fired.append(1)
            assert ar.reject_affiliate_ledger(db_, ledger_id=a, reason="admin") is not None
        return out

    monkeypatch.setattr(ar, "_finalize_issued_if_voucher_exists", finalize_then_reject_once)
    result = ar.approve_affiliate_ledger(db, ledger_id=a, now_utc=SEP)
    assert fired and (result or {}).get("status") == "REJECTED"
    assert _status(db, a) == "REJECTED" and _issued_for(db, a) == 0
    assert db.voucher_pools.count_documents({"status": "available"}) == 4


def test_approve_all_vs_reject_all_never_leaves_rejected_with_voucher(monkeypatch):
    for _ in range(15):
        db = _db()
        ids = [_ledger(db, 10 + i) for i in range(8)]
        _stock(db, 40)
        client = _client(monkeypatch, db)
        out = {}
        t1 = threading.Thread(target=lambda: out.setdefault("a", client.post(APPROVE_URL, json={"ledger_ids": [str(i) for i in ids]}).get_json()))
        t2 = threading.Thread(target=lambda: out.setdefault("r", _post(client, ids).get_json()))
        t1.start(); t2.start(); t1.join(20); t2.join(20)
        assert out["a"]["failed"] == 0 and out["r"]["failed"] == 0
        for lid in ids:
            row = db.affiliate_ledger.find_one({"_id": lid})
            if row["status"] == "ISSUED":
                assert _issued_for(db, lid) == 2 and row.get("voucher_code")
            else:
                assert row["status"] == "REJECTED", row["status"]
                assert _issued_for(db, lid) == 0 and not row.get("voucher_code")
        assert out["r"]["rejected"] == sum(1 for i in ids if _status(db, i) == "REJECTED")


def test_ledger_with_issued_pool_voucher_is_protected(monkeypatch):
    db = _db()
    a = _ledger(db, 1, status="PENDING_MANUAL")
    db.voucher_pools.insert_one({"pool_id": "T1", "code": "T1-x", "status": "issued", "issued_for_ledger_id": str(a)})
    body = _post(_client(monkeypatch, db), [a]).get_json()
    assert body["skipped"] == 1 and _by_id(body)[str(a)]["reason"] == "voucher_already_attached"
    assert _status(db, a) == "PENDING_MANUAL"


# ---- reject_affiliate_ledger itself / individual endpoint --------------------

def test_reject_function_is_status_guarded():
    db = _db()
    for status in ("ISSUED", "SETTLING", "REJECTED"):
        lid = _ledger(db, hash(status) % 10_000, status=status)
        assert ar.reject_affiliate_ledger(db, ledger_id=lid, reason="x") is None
        assert _status(db, lid) == status
    voucher_row = _ledger(db, 77, status="PENDING_MANUAL", voucher_code="V")
    assert ar.reject_affiliate_ledger(db, ledger_id=voucher_row, reason="x") is None
    ok = _ledger(db, 78, status="OUT_OF_STOCK")
    assert ar.reject_affiliate_ledger(db, ledger_id=ok, reason="x")["status"] == "OUT_OF_STOCK"
    assert _status(db, ok) == "REJECTED"
    # allowed_statuses can only narrow, never widen into protected statuses.
    iss = _ledger(db, 79, status="ISSUED")
    assert ar.reject_affiliate_ledger(db, ledger_id=iss, allowed_statuses=["ISSUED"]) is None


def test_individual_reject_still_works_and_refuses_issued(monkeypatch):
    db = _db()
    client = _client(monkeypatch, db)
    for status in ("PENDING_REVIEW", "SIMULATED_PENDING", "OUT_OF_STOCK"):
        lid = _ledger(db, abs(hash(status)) % 10_000, status=status)
        res = client.post(f"/v2/miniapp/admin/affiliate/{lid}/reject", json={"reason": "x"})
        assert res.status_code == 200 and res.get_json()["status"] == "ok"
        assert _status(db, lid) == "REJECTED"
        again = client.post(f"/v2/miniapp/admin/affiliate/{lid}/reject", json={"reason": "x"})
        assert again.status_code == 200 and again.get_json()["already_rejected"] is True
    issued = _ledger(db, 5, status="ISSUED", voucher_code="KEEP")
    res = client.post(f"/v2/miniapp/admin/affiliate/{issued}/reject", json={})
    assert res.status_code == 409 and res.get_json()["reason"] == "already_issued"
    assert _status(db, issued) == "ISSUED"
    assert client.post(f"/v2/miniapp/admin/affiliate/{ObjectId()}/reject", json={}).status_code == 404
