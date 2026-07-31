"""Tests for build_welcome_progress_response's pool/ledger-backed WELCOME issuance.

Covers the Welcome Voucher Progress -> voucher_pools/affiliate_ledger
(pool_id="WELCOME") migration: hide_welcome_card / welcome_pending_reason
plus the issued/claimed/NO_ACTIVE_WELCOME_BATCH/NO_FREE_CODES/SETTLING states
returned once check-ins are complete.
"""
from datetime import datetime, timedelta
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


class FakeLedgerCol:
    def __init__(self, doc=None):
        self.doc = doc

    def find_one(self, filt, projection=None):
        return dict(self.doc) if self.doc else None


class FakeDb:
    def __init__(self, events, ledger_doc=None):
        self.xp_events = FakeEvents(events)
        self.xp_ledger = FakeEvents([])
        self.affiliate_ledger = FakeLedgerCol(ledger_doc)


def _checkin(day_offset):
    return {
        "user_id": UID,
        "type": "checkin",
        "created_at": JOINED + timedelta(days=day_offset, hours=1),
    }


THREE_CHECKINS = [_checkin(0), _checkin(1), _checkin(2)]


def _no_find_claims_col():
    return type("C", (), {"find_one": staticmethod(lambda filt, proj=None: None)})()


def _build(monkeypatch, *, subscribed=True, allowed=True, eligibility_reason="ok",
           issue_result=None, ticket_status="active", claimed_doc=False,
           checkins=THREE_CHECKINS, ledger_doc=None, claims_col=None):
    app = Flask(__name__)
    monkeypatch.setattr(m, "users_collection", FakeUsers())
    monkeypatch.setattr(m, "db", FakeDb(checkins, ledger_doc=ledger_doc))
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: subscribed)
    monkeypatch.setattr(m, "welcome_eligibility",
                         lambda _uid, ref=None: (allowed, eligibility_reason, {}))
    if issue_result is not None:
        monkeypatch.setattr(
            m, "issue_welcome_bonus_if_eligible",
            lambda db, *, user_id, is_new_user, blocked=False, now_utc=None: issue_result,
        )
    monkeypatch.setattr(m, "_get_welcome_ticket", lambda uid: {"status": ticket_status})
    monkeypatch.setattr(m, "_get_welcome_eligibility", lambda uid: {"claimed": claimed_doc})
    monkeypatch.setattr(m, "new_joiner_claims_col", claims_col or _no_find_claims_col())
    with app.app_context():
        return m.build_welcome_progress_response(UID, now=NOW)


# ── CHANNEL_NOT_JOINED (not yet subscribed) ─────────────────────────────────
def test_channel_not_joined_card_visible_no_issuance(monkeypatch):
    data = _build(monkeypatch, subscribed=False, allowed=True, eligibility_reason="ok")
    assert data["hide_welcome_card"] is False
    assert data["welcome_pending_reason"] == "CHANNEL_NOT_JOINED"
    assert data["status"] == "in_progress"


# ── ELIGIBILITY_FAILED / AUDIENCE_MISMATCH / WINDOW_EXPIRED / ALREADY_CLAIMED / self-invite ──
def test_eligibility_failed_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="no_ticket")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "ELIGIBILITY_FAILED"


def test_audience_mismatch_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="audience_mismatch")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "AUDIENCE_MISMATCH"


def test_window_expired_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="ticket_expired")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "WINDOW_EXPIRED"
    assert data["visible"] is False
    assert data["eligible"] is False
    assert data["reason_code"] == "welcome_expired"


def test_outside_join_window_hides_card_as_not_new_user(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="not_in_welcome_window")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "WINDOW_EXPIRED"
    assert data["visible"] is False
    assert data["reason_code"] == "welcome_not_new_user"


def test_already_claimed_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="ticket_claimed")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "ALREADY_CLAIMED"
    assert data["visible"] is False
    assert data["reason_code"] == "welcome_already_claimed"


def test_self_invite_blocked_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=False, eligibility_reason="self_invite_blocked")
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "RISK_BLOCKED"
    assert data["visible"] is False
    assert data["reason_code"] == "welcome_blocked"


# ── completed + eligible + subscribed: pool/ledger issuance path ───────────
def test_issued_shows_ready_state_with_voucher_code(monkeypatch):
    data = _build(
        monkeypatch, subscribed=True, allowed=True,
        issue_result={"created": True, "status": "ISSUED", "voucher_code": "WELC-ABC123"},
    )
    assert data["status"] == "issued"
    assert data["hide_welcome_card"] is False
    assert data["voucher_code"] == "WELC-ABC123"


def test_no_active_batch_stays_visible_not_hidden(monkeypatch):
    data = _build(
        monkeypatch, subscribed=True, allowed=True,
        issue_result={"created": True, "status": "OUT_OF_STOCK"},
        ledger_doc={"status": "OUT_OF_STOCK", "risk_flags": ["no_welcome_batch_for_entitlement_time"]},
    )
    assert data["status"] == "NO_ACTIVE_WELCOME_BATCH"
    assert data["hide_welcome_card"] is False
    assert data.get("voucher_code") is None


def test_no_stock_maps_to_no_free_codes(monkeypatch):
    data = _build(
        monkeypatch, subscribed=True, allowed=True,
        issue_result={"created": True, "status": "OUT_OF_STOCK"},
        ledger_doc={"status": "OUT_OF_STOCK", "risk_flags": []},
    )
    assert data["status"] == "NO_FREE_CODES"
    assert data["hide_welcome_card"] is False


def test_settling_reported_as_settling(monkeypatch):
    data = _build(
        monkeypatch, subscribed=True, allowed=True,
        issue_result={"created": False, "status": "SETTLING"},
    )
    assert data["status"] == "SETTLING"
    assert data["hide_welcome_card"] is False


# ── existing issued WELCOME ledger short-circuits before calling the allocator ──
def test_existing_issued_ledger_returns_claimed_without_calling_allocator(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("issue_welcome_bonus_if_eligible must not be called for an already-ISSUED ledger")

    monkeypatch.setattr(m, "issue_welcome_bonus_if_eligible", _boom)
    data = _build(
        monkeypatch, subscribed=True, allowed=True,
        ledger_doc={"status": "ISSUED", "voucher_code": "WELC-XYZ999"},
    )
    assert data["status"] == "claimed"
    assert data["voucher_code"] == "WELC-XYZ999"
    assert data["visible"] is False
    assert data["eligible"] is False
    assert data["hide_welcome_card"] is True
    assert data["reason_code"] == "welcome_already_issued"


# ── existing legacy new_joiner_claims blocks a second issuance ─────────────
def test_legacy_new_joiner_claim_blocks_reissuance(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("must not attempt issuance when a legacy claim already exists")

    monkeypatch.setattr(m, "issue_welcome_bonus_if_eligible", _boom)
    claims_col = type("C", (), {"find_one": staticmethod(lambda filt, proj=None: {"_id": 1})})()
    data = _build(monkeypatch, subscribed=True, allowed=True, claims_col=claims_col)
    assert data["status"] == "claimed"
    assert data["visible"] is False
    assert data["eligible"] is False
    assert data["hide_welcome_card"] is True
    assert data["reason_code"] == "welcome_already_processed"


# ── welcome_tickets status=claimed hides the card (not just status="claimed") ──
def test_ticket_status_claimed_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=True, ticket_status="claimed")
    assert data["status"] == "claimed"
    assert data["visible"] is False
    assert data["eligible"] is False
    assert data["hide_welcome_card"] is True
    assert data["reason_code"] == "welcome_already_claimed"


# ── welcome_eligibility doc claimed=True hides the card ─────────────────────
def test_eligibility_doc_claimed_hides_card(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=True, claimed_doc=True)
    assert data["status"] == "claimed"
    assert data["visible"] is False
    assert data["eligible"] is False
    assert data["hide_welcome_card"] is True


# ── eligible + in-progress: visible=True and eligible=True (frontend double-gate) ──
def test_in_progress_reports_visible_and_eligible_true(monkeypatch):
    data = _build(monkeypatch, subscribed=True, allowed=True, checkins=[_checkin(0)])
    assert data["visible"] is True
    assert data["eligible"] is True
    assert data["reason_code"] == "welcome_eligible"


# ── a canonical ISSUED ledger overrides a legacy welcome_eligibility that
# still looks "active" -- canonical WELCOME ledger status is authoritative ──
def test_canonical_ledger_issued_overrides_legacy_active_eligibility_doc(monkeypatch):
    data = _build(
        monkeypatch, subscribed=True, allowed=True,
        ledger_doc={"status": "ISSUED", "voucher_code": "WELC-LEGACY"},
        claimed_doc=False,  # legacy doc still looks "active"/unclaimed
        ticket_status="active",
    )
    assert data["visible"] is False
    assert data["eligible"] is False
    assert data["hide_welcome_card"] is True


# ── claimed users never touch welcome_eligibility()/get_welcome_reward_progress
# (which upsert welcome_eligibility/welcome_tickets) -- a read-only progress
# poll for an already-claimed user must not create/refresh eligibility state ──
def test_claimed_user_never_calls_welcome_eligibility(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("must not call welcome_eligibility for an already-claimed user")

    app = Flask(__name__)
    monkeypatch.setattr(m, "users_collection", FakeUsers())
    monkeypatch.setattr(m, "db", FakeDb(THREE_CHECKINS, ledger_doc={"status": "ISSUED", "voucher_code": "WELC-1"}))
    monkeypatch.setattr(m, "welcome_eligibility", _boom)
    monkeypatch.setattr(m, "_get_welcome_ticket", lambda uid: {"status": "active"})
    monkeypatch.setattr(m, "_get_welcome_eligibility", lambda uid: {"claimed": False})
    monkeypatch.setattr(m, "new_joiner_claims_col", _no_find_claims_col())
    with app.app_context():
        data = m.build_welcome_progress_response(UID, now=NOW)
    assert data["visible"] is False
    assert data["status"] == "claimed"


# ── incomplete check-ins: never attempts issuance ───────────────────────────
def test_incomplete_checkins_card_visible_no_pending_reason(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("must not attempt issuance before check-ins are complete")

    monkeypatch.setattr(m, "issue_welcome_bonus_if_eligible", _boom)
    data = _build(monkeypatch, subscribed=True, allowed=True, checkins=[_checkin(0), _checkin(1)])
    assert data["hide_welcome_card"] is False
    assert data["welcome_pending_reason"] is None
    assert data["completed_days"] == 2
