"""POST /admin/affiliate/<id>/reject requires a real reason (P0 hotfix).

Root cause being pinned: the dashboard mapped Cancel on the reason prompt to
"" and still POSTed, and the server accepted any/no reason, so a cancelled
prompt irreversibly rejected an affiliate reward. The server now refuses a
blank reason so no client (including an older cached dashboard script) can
reject without one. Bulk reject keeps its documented default-reason fallback.
"""
from __future__ import annotations

from datetime import datetime, timezone

from bson import ObjectId
from flask import Flask

import vouchers
from fake_mongo import FakeDb
from vouchers import vouchers_bp

SEP = datetime(2026, 9, 15, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}


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


def _client(monkeypatch, db):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "db", db)
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    return app.test_client()


def _reject(client, lid, **body):
    return client.post(f"/v2/miniapp/admin/affiliate/{lid}/reject", json=body)


def _status(db, lid):
    return db.affiliate_ledger.find_one({"_id": lid})["status"]


def test_missing_reason_is_refused_and_ledger_untouched(monkeypatch):
    db = _db()
    lid = _ledger(db, 1)
    res = _client(monkeypatch, db).post(f"/v2/miniapp/admin/affiliate/{lid}/reject")
    assert res.status_code == 400
    assert res.get_json() == {"status": "error", "reason": "reason_required"}
    assert _status(db, lid) == "PENDING_REVIEW"


def test_blank_and_non_string_reasons_are_refused(monkeypatch):
    db = _db()
    client = _client(monkeypatch, db)
    for i, bad in enumerate(["", "   ", "\n\t", None, 0, 123, ["x"], {"a": 1}, False]):
        lid = _ledger(db, 10 + i)
        res = _reject(client, lid, reason=bad)
        assert res.status_code == 400, bad
        assert res.get_json()["reason"] == "reason_required", bad
        assert _status(db, lid) == "PENDING_REVIEW", bad


def test_the_exact_request_a_cancelled_prompt_used_to_send_is_refused(monkeypatch):
    # Old dashboard: prompt(...) || "" -> {"reason": ""} on Cancel.
    db = _db()
    lid = _ledger(db, 1)
    res = _reject(_client(monkeypatch, db), lid, reason="")
    assert res.status_code == 400
    assert _status(db, lid) == "PENDING_REVIEW"
    assert db.affiliate_ledger.find_one({"_id": lid}).get("review_reason") is None


def test_valid_reason_rejects_and_is_stored_trimmed(monkeypatch):
    db = _db()
    lid = _ledger(db, 1)
    res = _reject(_client(monkeypatch, db), lid, reason="  Suspected fake referrals  ")
    assert res.status_code == 200 and res.get_json() == {"status": "ok"}
    row = db.affiliate_ledger.find_one({"_id": lid})
    assert row["status"] == "REJECTED"
    assert row["review_reason"] == "Suspected fake referrals"


def test_one_character_reason_still_accepted_for_api_compat(monkeypatch):
    # test_affiliate_bulk_approve posts {"reason": "x"}; the server only needs "non-blank".
    db = _db()
    lid = _ledger(db, 1)
    assert _reject(_client(monkeypatch, db), lid, reason="x").status_code == 200
    assert _status(db, lid) == "REJECTED"


def test_reason_is_capped_at_200_characters(monkeypatch):
    db = _db()
    lid = _ledger(db, 1)
    assert _reject(_client(monkeypatch, db), lid, reason="r" * 500).status_code == 200
    assert len(db.affiliate_ledger.find_one({"_id": lid})["review_reason"]) == 200


def test_bad_ledger_id_is_still_reported_first(monkeypatch):
    res = _reject(_client(monkeypatch, _db()), "not-an-object-id", reason="")
    assert res.status_code == 400 and res.get_json()["reason"] == "bad_ledger_id"


def test_double_click_with_reason_stays_idempotent(monkeypatch):
    db = _db()
    lid = _ledger(db, 1)
    client = _client(monkeypatch, db)
    assert _reject(client, lid, reason="dup request").status_code == 200
    again = _reject(client, lid, reason="dup request")
    assert again.status_code == 200 and again.get_json().get("already_rejected") is True
    assert _status(db, lid) == "REJECTED"


def test_issued_ledger_is_never_rejected_even_with_a_reason(monkeypatch):
    db = _db()
    lid = _ledger(db, 1, status="ISSUED", voucher_code="DONE")
    res = _reject(_client(monkeypatch, db), lid, reason="too late")
    assert res.status_code == 409
    assert _status(db, lid) == "ISSUED"


def test_unknown_ledger_with_reason_is_404(monkeypatch):
    res = _reject(_client(monkeypatch, _db()), ObjectId(), reason="nobody home")
    assert res.status_code == 404


def test_bulk_reject_contract_is_unchanged_default_reason_fallback(monkeypatch):
    db = _db()
    lid = _ledger(db, 1)
    res = _client(monkeypatch, db).post("/v2/miniapp/admin/affiliate/reject-bulk", json={"ledger_ids": [str(lid)]})
    assert res.status_code == 200
    assert _status(db, lid) == "REJECTED"
    assert db.affiliate_ledger.find_one({"_id": lid})["review_reason"] == "bulk_admin_reject"


def test_blank_reason_on_a_non_rejectable_ledger_keeps_its_existing_answer(monkeypatch):
    """The reason requirement only matters when a rejection would happen."""
    db = _db()
    client = _client(monkeypatch, db)
    issued = _ledger(db, 1, status="ISSUED", voucher_code="KEEP")
    res = _reject(client, issued)
    assert res.status_code == 409 and res.get_json()["reason"] == "already_issued"
    assert _status(db, issued) == "ISSUED"
    assert client.post(f"/v2/miniapp/admin/affiliate/{ObjectId()}/reject", json={}).status_code == 404
    done = _ledger(db, 2, status="REJECTED")
    res = _reject(client, done)
    assert res.status_code == 200 and res.get_json().get("already_rejected") is True


def test_blank_reason_never_writes_anything(monkeypatch):
    db = _db()
    ids = [_ledger(db, 20 + i, status=s) for i, s in enumerate(
        ["PENDING_REVIEW", "PENDING_MANUAL", "SIMULATED_PENDING", "OUT_OF_STOCK"])]
    client = _client(monkeypatch, db)
    before = [db.affiliate_ledger.find_one({"_id": i}) for i in ids]
    for lid in ids:
        assert _reject(client, lid, reason="  ").status_code == 400
    assert [db.affiliate_ledger.find_one({"_id": i}) for i in ids] == before
