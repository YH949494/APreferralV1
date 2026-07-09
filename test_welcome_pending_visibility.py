"""Tests for hide_welcome_card / welcome_pending_reason logic in build_welcome_progress_response."""
from datetime import datetime, timedelta, timezone
from flask import Flask

import vouchers as m
from config import KL_TZ


UID = 99
JOINED = datetime(2026, 1, 1, 9, 0, tzinfo=KL_TZ)
NOW = JOINED + timedelta(days=3)


class FakeUsers:
    def find_one(self, filt, projection=None):
        return {"user_id": UID, "joined_main_at": JOINED}


class FakeEvents:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self, filt, projection=None):
        start = filt.get("created_at", {}).get("$gte")
        end = filt.get("created_at", {}).get("$lte")
        out = []
        for doc in self.docs:
            if doc.get("user_id") != filt.get("user_id"):
                continue
            if "type" in filt and doc.get("type") != filt.get("type"):
                continue
            if "source" in filt and doc.get("source") != filt.get("source"):
                continue
            created = m._as_aware_utc(doc.get("created_at"))
            if start and created < m._as_aware_utc(start):
                continue
            if end and created > m._as_aware_utc(end):
                continue
            out.append(dict(doc))
        return out


class FakeDb:
    def __init__(self, events):
        self.xp_events = FakeEvents(events)
        self.xp_ledger = FakeEvents([])


def _checkin(day_offset):
    return {
        "user_id": UID,
        "type": "checkin",
        "created_at": JOINED + timedelta(days=day_offset, hours=1),
    }


THREE_CHECKINS = [_checkin(0), _checkin(1), _checkin(2)]


def _build(monkeypatch, *, subscribed=True, allowed=True, eligibility_reason="ok",
           claim_drop_id=None, ticket_status="active", claimed_doc=False):
    app = Flask(__name__)
    monkeypatch.setattr(m, "users_collection", FakeUsers())
    monkeypatch.setattr(m, "db", FakeDb(THREE_CHECKINS))
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: subscribed)
    monkeypatch.setattr(m, "welcome_eligibility",
                        lambda _uid, ref=None: (allowed, eligibility_reason, {}))
    monkeypatch.setattr(m, "_welcome_claim_drop_id",
                        lambda now_ref=None, uid=None, user_doc=None: claim_drop_id)
    monkeypatch.setattr(m, "_get_welcome_ticket",
                        lambda uid: {"status": ticket_status})
    monkeypatch.setattr(m, "_get_welcome_eligibility",
                        lambda uid: {"claimed": claimed_doc})
    monkeypatch.setattr(m, "new_joiner_claims_col",
                        type("C", (), {"find_one": staticmethod(lambda filt, proj=None: None)})())
    with app.app_context():
        return m.build_welcome_progress_response(UID, now=NOW)


# ── Test 1: CHANNEL_NOT_JOINED ──────────────────────────────────────────────
def test_channel_not_joined_card_visible_no_claim_btn(monkeypatch):
    data = _build(monkeypatch, subscribed=False, allowed=True, eligibility_reason="ok")
    assert data["hide_welcome_card"] is False
    assert data["welcome_pending_reason"] == "CHANNEL_NOT_JOINED"
    assert data.get("claim_drop_id") is None


# ── Test 2: ELIGIBILITY_FAILED hides card ───────────────────────────────────
def test_eligibility_failed_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="blocked")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "ELIGIBILITY_FAILED"


# ── Test 3: AUDIENCE_MISMATCH hides card ────────────────────────────────────
def test_audience_mismatch_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="audience_mismatch")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "AUDIENCE_MISMATCH"


# ── Test 4: WINDOW_EXPIRED hides card ───────────────────────────────────────
def test_window_expired_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="ticket_expired")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "WINDOW_EXPIRED"


# ── Test 5: ALREADY_CLAIMED hides card ──────────────────────────────────────
def test_already_claimed_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="ticket_claimed")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "ALREADY_CLAIMED"


# ── Test 6: NO_FREE_CODES card visible, no claim button ─────────────────────
def test_no_free_codes_card_visible_no_claim(monkeypatch):
    monkeypatch.setattr(m, "_welcome_claim_drop_reason",
                        lambda now_ref=None, uid=None, user_doc=None: "NO_FREE_CODES")
    data = _build(monkeypatch, subscribed=True, allowed=True, eligibility_reason="ok",
                  claim_drop_id=None)
    assert data["hide_welcome_card"] is False
    assert data["welcome_pending_reason"] == "NO_FREE_CODES"
    assert data.get("claim_drop_id") is None


# ── Test 7: valid claim_drop_id — card visible, claim button shown ───────────
def test_valid_claim_drop_id_card_visible(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=True, eligibility_reason="ok",
                  claim_drop_id="drop123")
    assert data["hide_welcome_card"] is False
    assert data["welcome_pending_reason"] is None
    assert data["claim_drop_id"] == "drop123"


# ── Test 8: completed_days < 3 — progress card visible, no pending reason ───
def test_incomplete_checkins_card_visible_no_pending_reason(monkeypatch):
    app = Flask(__name__)
    monkeypatch.setattr(m, "users_collection", FakeUsers())
    monkeypatch.setattr(m, "db", FakeDb([_checkin(0), _checkin(1)]))  # only 2 check-ins
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: True)
    monkeypatch.setattr(m, "welcome_eligibility",
                        lambda _uid, ref=None: (True, "ok", {}))
    monkeypatch.setattr(m, "_welcome_claim_drop_id",
                        lambda now_ref=None, uid=None, user_doc=None: None)
    monkeypatch.setattr(m, "_get_welcome_ticket",
                        lambda uid: {"status": "active"})
    monkeypatch.setattr(m, "_get_welcome_eligibility", lambda uid: {})
    monkeypatch.setattr(m, "new_joiner_claims_col",
                        type("C", (), {"find_one": staticmethod(lambda filt, proj=None: None)})())
    with app.app_context():
        data = m.build_welcome_progress_response(UID, now=NOW)
    assert data["hide_welcome_card"] is False
    assert data["welcome_pending_reason"] is None
    assert data["completed_days"] == 2
