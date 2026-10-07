"""POST /admin/affiliate/approve-bulk: thin loop over approve_affiliate_ledger."""
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
URL = "/v2/miniapp/admin/affiliate/approve-bulk"


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


def _available(db):
    return db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"})


def _issued_for(db, lid):
    return db.voucher_pools.count_documents({"status": "issued", "issued_for_ledger_id": str(lid)})


def _client(monkeypatch, db, admin=True):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", db)
    if admin:
        monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    else:
        monkeypatch.setattr(vouchers, "require_admin", lambda: (None, ({"status": "error"}, 403)))
    return app.test_client()


def _post(client, ids):
    return client.post(URL, json={"ledger_ids": [str(i) for i in ids]})


def test_bulk_approves_five_pending_rows(monkeypatch, caplog):
    db = _db()
    ids = [_ledger(db, 100 + i) for i in range(5)]
    _stock(db, 10)
    client = _client(monkeypatch, db)
    with caplog.at_level(logging.INFO):
        res = _post(client, ids)
    body = res.get_json()
    assert res.status_code == 200
    assert (body["requested"], body["processed"], body["issued"], body["out_of_stock"],
            body["already_final"], body["failed"]) == (5, 5, 5, 0, 0, 0)
    assert all(db.affiliate_ledger.find_one({"_id": i})["status"] == "ISSUED" for i in ids)
    assert _available(db) == 0
    assert "T1-" not in str(body)  # no codes leak
    assert any("[AFFILIATE][BULK_APPROVE] requested=5 processed=5 issued=5 out_of_stock=0 already_final=0 failed=0 admin=boss" in r.getMessage() for r in caplog.records)


def test_mix_pending_and_already_issued_untouched(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    done = _ledger(db, 2, status="ISSUED", voucher_code="DONE")
    _stock(db, 4)
    body = _post(_client(monkeypatch, db), [a, done]).get_json()
    assert (body["processed"], body["issued"], body["already_final"]) == (1, 1, 1)
    row = db.affiliate_ledger.find_one({"_id": done})
    assert row["status"] == "ISSUED" and row["voucher_code"] == "DONE"
    assert _available(db) == 2


def test_invalid_object_id_rejects_request_without_side_effects(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    _stock(db, 4)
    res = _client(monkeypatch, db).post(URL, json={"ledger_ids": [str(a), "not-an-oid"]})
    assert res.status_code == 400 and res.get_json()["reason"] == "bad_ledger_id"
    assert db.affiliate_ledger.find_one({"_id": a})["status"] == "PENDING_REVIEW"
    assert _available(db) == 4


def test_missing_or_empty_ledger_ids(monkeypatch):
    client = _client(monkeypatch, _db())
    assert client.post(URL, json={}).status_code == 400
    assert client.post(URL, json={"ledger_ids": []}).status_code == 400
    assert client.post(URL, json={"ledger_ids": "abc"}).status_code == 400


def test_duplicate_ids_processed_once(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    _stock(db, 6)
    body = _post(_client(monkeypatch, db), [a, a, a]).get_json()
    assert (body["requested"], body["processed"], body["issued"]) == (1, 1, 1)
    assert _issued_for(db, a) == 2  # T1 bundle = 2 codes, once
    assert _available(db) == 4


def test_more_than_200_ids_rejected(monkeypatch):
    db = _db()
    res = _post(_client(monkeypatch, db), [ObjectId() for _ in range(201)])
    assert res.status_code == 400 and res.get_json()["reason"] == "too_many_ledger_ids"
    assert _post(_client(monkeypatch, db), [ObjectId() for _ in range(200)]).status_code == 200


def test_never_touches_simulated_rejected_out_of_stock(monkeypatch):
    db = _db()
    sim = _ledger(db, 1, status="SIMULATED_PENDING", simulate=True)
    rej = _ledger(db, 2, status="REJECTED")
    oos = _ledger(db, 3, status="OUT_OF_STOCK", risk_flags=["pool_empty"])
    _stock(db, 6)
    body = _post(_client(monkeypatch, db), [sim, rej, oos]).get_json()
    assert (body["processed"], body["already_final"], body["issued"]) == (0, 3, 0)
    assert db.affiliate_ledger.find_one({"_id": sim})["status"] == "SIMULATED_PENDING"
    assert db.affiliate_ledger.find_one({"_id": rej})["status"] == "REJECTED"
    assert db.affiliate_ledger.find_one({"_id": oos})["status"] == "OUT_OF_STOCK"
    assert _available(db) == 6


def test_pool_runs_out_halfway(monkeypatch):
    db = _db()
    ids = [_ledger(db, 10 + i) for i in range(4)]
    _stock(db, 4)  # T1 bundle = 2 codes -> only 2 ledgers can be filled
    body = _post(_client(monkeypatch, db), ids).get_json()
    assert body["processed"] == 4 and body["issued"] == 2 and body["out_of_stock"] == 2 and body["failed"] == 0
    assert _available(db) == 0
    for lid in ids:
        assert _issued_for(db, lid) in (0, 2)  # never a partial bundle


def test_repeated_same_request_is_idempotent(monkeypatch):
    db = _db()
    ids = [_ledger(db, 10 + i) for i in range(4)]
    _stock(db, 6)  # 3 fill, 1 OOS-ish
    client = _client(monkeypatch, db)
    first = _post(client, ids).get_json()
    snapshot = {i: dict(db.affiliate_ledger.find_one({"_id": i})) for i in ids}
    avail = _available(db)
    second = _post(client, ids).get_json()
    assert first["issued"] == 3
    # ISSUED rows are final and skipped; the one stock-short row (PENDING_MANUAL
    # or OUT_OF_STOCK) may be re-attempted but cannot allocate anything new.
    assert second["issued"] == 0 and second["failed"] == 0
    assert second["already_final"] >= 3
    assert second["processed"] + second["already_final"] == len(ids)
    assert _available(db) == avail
    for i in ids:
        row = db.affiliate_ledger.find_one({"_id": i})
        assert row["status"] == snapshot[i]["status"]
        assert row.get("voucher_code") == snapshot[i].get("voucher_code")


def test_concurrent_bulk_requests_never_double_allocate(monkeypatch):
    db = _db()
    ids = [_ledger(db, 10 + i) for i in range(6)]
    _stock(db, 20)
    client = _client(monkeypatch, db)
    out = []

    def run():
        out.append(_post(client, ids).get_json())

    threads = [threading.Thread(target=run) for _ in range(2)]
    [t.start() for t in threads]
    [t.join(15) for t in threads]
    assert len(out) == 2
    for lid in ids:
        assert db.affiliate_ledger.find_one({"_id": lid})["status"] == "ISSUED"
        assert _issued_for(db, lid) == 2  # exactly one bundle per ledger
    assert _available(db) == 20 - 12
    assert sum(b["issued"] for b in out) <= 12  # no ledger issued by both


def test_replayed_approval_after_bulk_is_refused(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    _stock(db, 4)
    client = _client(monkeypatch, db)
    _post(client, [a])
    assert ar.approve_affiliate_ledger(db, ledger_id=a, now_utc=SEP) is None
    assert _issued_for(db, a) == 2 and _available(db) == 2
    # Individual endpoints keep working: reject still flips a pending row.
    b = _ledger(db, 2)
    assert client.post(f"/v2/miniapp/admin/affiliate/{b}/reject", json={"reason": "x"}).status_code == 200
    assert db.affiliate_ledger.find_one({"_id": b})["status"] == "REJECTED"
    c = _ledger(db, 3)
    r = client.post(f"/v2/miniapp/admin/affiliate/{c}/approve")
    assert r.status_code == 200 and r.get_json()["issued"] is True


def test_admin_auth_required(monkeypatch):
    db = _db()
    a = _ledger(db, 1)
    _stock(db, 4)
    res = _post(_client(monkeypatch, db, admin=False), [a])
    assert res.status_code == 403
    assert db.affiliate_ledger.find_one({"_id": a})["status"] == "PENDING_REVIEW"
    assert _available(db) == 4


def test_one_failure_does_not_stop_the_rest(monkeypatch):
    db = _db()
    ids = [_ledger(db, 10 + i) for i in range(3)]
    _stock(db, 10)
    real = vouchers.approve_affiliate_ledger

    def flaky(db_, *, ledger_id, now_utc=None):
        if ledger_id == ids[1]:
            raise RuntimeError("boom")
        return real(db_, ledger_id=ledger_id, now_utc=now_utc)

    monkeypatch.setattr(vouchers, "approve_affiliate_ledger", flaky)
    body = _post(_client(monkeypatch, db), ids).get_json()
    assert (body["processed"], body["issued"], body["failed"]) == (2, 2, 1)
    assert db.affiliate_ledger.find_one({"_id": ids[0]})["status"] == "ISSUED"
    assert db.affiliate_ledger.find_one({"_id": ids[2]})["status"] == "ISSUED"
    assert db.affiliate_ledger.find_one({"_id": ids[1]})["status"] == "PENDING_REVIEW"


def test_unknown_id_counted_failed_not_found(monkeypatch):
    body = _post(_client(monkeypatch, _db()), [ObjectId()]).get_json()
    assert body["failed"] == 1 and body["results"][0]["reason"] == "not_found"
