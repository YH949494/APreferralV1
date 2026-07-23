"""Regression tests for the lifetime-dedupe race in api_claim.

Previously, when new_joiner_claims_col.update_one raised DuplicateKeyError
(two concurrent claims from the same uid both passing the earlier per-drop
checks), api_claim returned already_claimed_lifetime without rolling back
the voucher code it had just reserved/marked claimed, leaking a real
voucher_claims/vouchers row. The DuplicateKeyError branch must roll back
the pooled voucher reservation and release claim ownership, mirroring the
existing rollback done on claim_record_write_failed.

Also covers a follow-up gap in the shared rollback helper itself: claim_pooled
decrements the drop's public_remaining/my_remaining counter on claim, but
_rollback_pooled_voucher_claim only freed the voucher and never restored that
counter, permanently undercounting inventory on every rollback.
"""
import inspect

import pytest

import vouchers as m
from fake_mongo import FakeDb


def test_lifetime_dedupe_conflict_rolls_back_voucher_reservation():
    src = inspect.getsource(m.api_claim)
    dup_branch = src.split("except DuplicateKeyError:", 1)[1]
    dup_branch = dup_branch.split("return jsonify({", 1)[0]

    assert "_rollback_pooled_voucher_claim" in dup_branch
    assert "_release_claim_ownership" in dup_branch
    assert 'reason="already_claimed_lifetime"' in dup_branch


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(m, "db", fdb)
    return fdb


def test_rollback_restores_pool_counter_and_frees_voucher(fake_db):
    drop_id = fake_db.drops.insert_one({
        "name": "Welcome",
        "public_remaining": 4,
    }).inserted_id

    fake_db.vouchers.insert_one({
        "type": "pooled",
        "dropId": drop_id,
        "pool": "public",
        "code": "CODE1",
        "status": "claimed",
        "claimedBy": "uid:1",
        "claimedByKey": "uid:1",
    })

    ok = m._rollback_pooled_voucher_claim(drop_id=drop_id, code="CODE1", claim_key="uid:1")

    assert ok is True
    voucher = fake_db.vouchers.find_one({"code": "CODE1"})
    assert voucher["status"] == "free"
    drop = fake_db.drops.find_one({"_id": drop_id})
    assert drop["public_remaining"] == 5
