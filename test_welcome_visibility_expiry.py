from datetime import datetime, timedelta, timezone
from flask import Flask

import vouchers as m


def _mk_drop(now):
    return {
        "_id": "drop-welcome",
        "name": "Welcome",
        "type": "pooled",
        "status": "active",
        "startsAt": now - timedelta(minutes=5),
        "endsAt": now + timedelta(minutes=5),
        "public_remaining": 10,
        "my_remaining": 0,
        "audience": "new_joiner",
    }


class C:
    def __init__(self, docs=None): self.docs = docs or []
    def find_one(self, q=None, proj=None):
        for d in self.docs:
            ok = True
            for k, v in (q or {}).items():
                if isinstance(v, dict) and "$or" in q: break
                if d.get(k) != v: ok = False
            if ok: return d
        return None


def test_unclaimed_visible_within_7_days():
    now = datetime.now(timezone.utc)
    app = Flask(__name__)
    orig = m.welcome_eligibility, m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state
    try:
        m.welcome_eligibility = lambda *_a, **_k: (True, "ok", {})
        m.get_welcome_reward_progress = lambda *_a, **_k: {"eligible": True, "unlocked": True, "expired": False, "hide": False, "channel_joined": True, "checkins_completed": 3, "checkins_required": 3, "eligible_until": None, "days_remaining": 7}
        m.get_active_drops = lambda _r: [_mk_drop(now)]
        m.voucher_claims_col = C([])
        m.users_collection = C([{"user_id": 42, "region": "th"}])
        m.load_user_context = lambda **_k: {}
        m.is_drop_allowed = lambda *a, **k: True
        m.is_user_eligible_for_drop = lambda *a, **k: True
        m._pooled_claimability_state = lambda **_k: {"claimable": True, "sold_out": False, "remaining": 1}
        with app.app_context():
            cards, _ = m.user_visible_drops({"usernameLower": "u", "userId": "42"}, now, tg_user={"id": 42, "username": "u"})
        assert len(cards) == 1
        assert "code" not in cards[0]
    finally:
        m.welcome_eligibility, m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state = orig


def test_claimed_visibility_cutoff_and_idempotent():
    now = datetime.now(timezone.utc)
    claim_recent = {"voucher_code": "WEL-1", "claimed_at": now - timedelta(days=2, hours=23)}
    claim_old = {"voucher_code": "WEL-2", "claimed_at": now - timedelta(days=3, seconds=1)}
    p1 = m._build_idempotent_claim_response(claim_recent, enforce_welcome_visibility=True, ref=now)
    p2 = m._build_idempotent_claim_response(claim_old, enforce_welcome_visibility=True, ref=now)
    assert p1 and p1.get("voucher_code") == "WEL-1"
    assert p2 and p2.get("status") == "already_claimed_expired"
    assert "voucher_code" not in p2


def test_claimed_visible_endpoint_within_3_days_and_hidden_after():
    now = datetime.now(timezone.utc)
    app = Flask(__name__)
    orig = m.welcome_eligibility, m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state
    try:
        m.welcome_eligibility = lambda *_a, **_k: (True, "ok", {})
        m.get_welcome_reward_progress = lambda *_a, **_k: {"eligible": True, "unlocked": True, "expired": False, "hide": False, "channel_joined": True, "checkins_completed": 3, "checkins_required": 3, "eligible_until": None, "days_remaining": 7}
        m.get_active_drops = lambda _r: [_mk_drop(now)]
        m.users_collection = C([{"user_id": 42, "region": "th"}])
        m.load_user_context = lambda **_k: {}
        m.is_drop_allowed = lambda *a, **k: True
        m.is_user_eligible_for_drop = lambda *a, **k: True
        m._pooled_claimability_state = lambda **_k: {"claimable": True, "sold_out": False, "remaining": 1}

        m.voucher_claims_col = C([{"drop_id": "drop-welcome", "user_id": 42, "status": "claimed", "voucher_code": "RECENT", "claimed_at": now - timedelta(days=2)}])
        with app.app_context():
            cards, _ = m.user_visible_drops({"usernameLower": "u", "userId": "42"}, now, tg_user={"id": 42, "username": "u"})
        assert len(cards) == 1 and cards[0].get("code") == "RECENT"

        m.voucher_claims_col = C([{"drop_id": "drop-welcome", "user_id": 42, "status": "claimed", "voucher_code": "OLD", "claimed_at": now - timedelta(days=4)}])
        with app.app_context():
            cards2, _ = m.user_visible_drops({"usernameLower": "u", "userId": "42"}, now, tg_user={"id": 42, "username": "u"})
        assert cards2 == []
    finally:
        m.welcome_eligibility, m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state = orig


def test_boundary_exactly_3_days_visible_then_hidden():
    now = datetime.now(timezone.utc)
    assert m._is_welcome_claim_still_visible(now - timedelta(days=3), ref=now)
    assert not m._is_welcome_claim_still_visible(now - timedelta(days=3, microseconds=1), ref=now)


def test_locked_welcome_card_visible_without_code():
    now = datetime.now(timezone.utc)
    app = Flask(__name__)
    orig = m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state
    try:
        m.get_welcome_reward_progress = lambda *_a, **_k: {"eligible": True, "unlocked": False, "expired": False, "hide": False, "channel_joined": True, "checkins_completed": 2, "checkins_required": 3, "eligible_until": now.isoformat(), "days_remaining": 5}
        m.get_active_drops = lambda _r: [_mk_drop(now)]
        m.voucher_claims_col = C([])
        m.users_collection = C([{"user_id": 42, "region": "th"}])
        m.load_user_context = lambda **_k: {}
        m.is_drop_allowed = lambda *a, **k: True
        m.is_user_eligible_for_drop = lambda *a, **k: True
        m._pooled_claimability_state = lambda **_k: {"claimable": True, "sold_out": False, "remaining": 1}
        with app.app_context():
            cards, _ = m.user_visible_drops({"usernameLower": "u", "userId": "42"}, now, tg_user={"id": 42, "username": "u"})
        assert len(cards) == 1
        assert cards[0]["state"] == "welcome_locked"
        assert cards[0]["welcome_progress"]["checkins_completed"] == 2
        assert "code" not in cards[0]
    finally:
        m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state = orig


def test_expired_incomplete_welcome_card_hidden():
    now = datetime.now(timezone.utc)
    app = Flask(__name__)
    orig = m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state
    try:
        m.get_welcome_reward_progress = lambda *_a, **_k: {"eligible": False, "unlocked": False, "expired": True, "hide": True, "channel_joined": True, "checkins_completed": 2, "checkins_required": 3, "eligible_until": now.isoformat(), "days_remaining": 0}
        m.get_active_drops = lambda _r: [_mk_drop(now)]
        m.voucher_claims_col = C([])
        m.users_collection = C([{"user_id": 42, "region": "th"}])
        m.load_user_context = lambda **_k: {}
        m.is_drop_allowed = lambda *a, **k: True
        m.is_user_eligible_for_drop = lambda *a, **k: True
        m._pooled_claimability_state = lambda **_k: {"claimable": True, "sold_out": False, "remaining": 1}
        with app.app_context():
            cards, _ = m.user_visible_drops({"usernameLower": "u", "userId": "42"}, now, tg_user={"id": 42, "username": "u"})
        assert cards == []
    finally:
        m.get_welcome_reward_progress, m.get_active_drops, m.voucher_claims_col, m.load_user_context, m.is_drop_allowed, m.is_user_eligible_for_drop, m.users_collection, m._pooled_claimability_state = orig
