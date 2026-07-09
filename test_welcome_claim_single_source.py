"""Regression tests for the Welcome Voucher single-source-of-truth claimability fix.

Covers:
  - profile-photo requirement fully removed from the claim flow
  - _welcome_claim_drop_id / _welcome_claim_drop_reason agree with the same
    audience/region gate that /vouchers/visible and /vouchers/claim enforce
    (the root cause of "Ready to Claim" with no claimable card/403 at claim time)
"""
import vouchers as m


def _new_joiner_drop(drop_id="d1", **extra):
    drop = {"_id": drop_id, "audience": {"type": "new_joiner"}, "name": "Welcome"}
    drop.update(extra)
    return drop


# ---- A. profile photo requirement fully removed ----------------------------

def test_profile_photo_helpers_removed():
    assert not hasattr(m, "_profile_photo_cache_status")
    assert not hasattr(m, "enqueue_verification")


def test_claim_endpoint_source_has_no_profile_photo_reason():
    import inspect
    src = inspect.getsource(m.api_claim)
    assert "missing_profile_photo" not in src
    assert "verification_in_progress" not in src
    assert "_profile_photo_cache_status" not in src
    assert "enqueue_verification" not in src


# ---- B/C. welcome-progress claimability agrees with the audience/region gate ----

def test_claim_drop_id_none_when_region_mismatched(monkeypatch):
    drop = _new_joiner_drop(audience={"type": "new_joiner", "regions": ["Thailand"]})
    monkeypatch.setattr(m, "get_active_drops", lambda ref: [drop])
    monkeypatch.setattr(m, "_pooled_claimability_state", lambda **kwargs: {"claimable": True, "sold_out": False, "remaining": 1})

    user_doc = {"region": "Malaysia"}
    assert m._welcome_claim_drop_id(uid=1, user_doc=user_doc) is None
    assert m._welcome_claim_drop_reason(uid=1, user_doc=user_doc) == "REGION_MISMATCH"


def test_claim_drop_id_present_when_region_matches(monkeypatch):
    drop = _new_joiner_drop(audience={"type": "new_joiner", "regions": ["Thailand"]})
    monkeypatch.setattr(m, "get_active_drops", lambda ref: [drop])
    monkeypatch.setattr(m, "_pooled_claimability_state", lambda **kwargs: {"claimable": True, "sold_out": False, "remaining": 1})

    user_doc = {"region": "Thailand"}
    assert m._welcome_claim_drop_id(uid=1, user_doc=user_doc) == "d1"


def test_claim_drop_id_none_when_denylisted(monkeypatch):
    drop = _new_joiner_drop(audience={"type": "new_joiner", "denylist_user_ids": [1]})
    monkeypatch.setattr(m, "get_active_drops", lambda ref: [drop])
    monkeypatch.setattr(m, "_pooled_claimability_state", lambda **kwargs: {"claimable": True, "sold_out": False, "remaining": 1})

    assert m._welcome_claim_drop_id(uid=1, user_doc=None) is None
    assert m._welcome_claim_drop_reason(uid=1, user_doc=None) == "AUDIENCE_MISMATCH"


def test_pool_claimability_state_exception_fails_closed(monkeypatch):
    """A drop lookup must never report 'ready' when the pool check itself errors."""
    drop = _new_joiner_drop()
    monkeypatch.setattr(m, "get_active_drops", lambda ref: [drop])

    def _boom(**kwargs):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(m, "_pooled_claimability_state", _boom)
    assert m._welcome_claim_drop_id(uid=1, user_doc=None) is None
