"""Regression test for the lifetime-dedupe race in api_claim.

Previously, when new_joiner_claims_col.update_one raised DuplicateKeyError
(two concurrent claims from the same uid both passing the earlier per-drop
checks), api_claim returned already_claimed_lifetime without rolling back
the voucher code it had just reserved/marked claimed, leaking a real
voucher_claims/vouchers row. The DuplicateKeyError branch must roll back
the pooled voucher reservation and release claim ownership, mirroring the
existing rollback done on claim_record_write_failed.
"""
import inspect

import vouchers as m


def test_lifetime_dedupe_conflict_rolls_back_voucher_reservation():
    src = inspect.getsource(m.api_claim)
    dup_branch = src.split("except DuplicateKeyError:", 1)[1]
    dup_branch = dup_branch.split("return jsonify({", 1)[0]

    assert "_rollback_pooled_voucher_claim" in dup_branch
    assert "_release_claim_ownership" in dup_branch
    assert 'reason="already_claimed_lifetime"' in dup_branch
