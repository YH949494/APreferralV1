"""End-to-end coverage for the Welcome Voucher Progress -> voucher_pools/
affiliate_ledger (pool_id="WELCOME") migration.

Unlike test_welcome_pending_visibility.py (which mocks
issue_welcome_bonus_if_eligible to test build_welcome_progress_response's
routing logic in isolation), these tests run the real
affiliate_rewards.issue_welcome_bonus_if_eligible allocator against an
in-memory fake_mongo database, driven through
vouchers.build_welcome_progress_response — proving the two systems really
are wired together end to end, not just individually correct.
"""
from datetime import datetime, timedelta, timezone

from flask import Flask

import fake_mongo
import vouchers as m
from config import KL_TZ
import affiliate_voucher_batches as avb
import affiliate_rewards as ar


UID = 555
JOINED = datetime(2026, 8, 1, 9, 0, tzinfo=KL_TZ)
NOW = JOINED + timedelta(days=3)
NOW_UTC = NOW.astimezone(timezone.utc)


class FakeUsers:
    def __init__(self, doc=None):
        self._doc = doc or {"user_id": UID, "joined_main_at": JOINED, "blocked": False}

    def find_one(self, filt, projection=None):
        return dict(self._doc)


class FakeEvents:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self, filt, projection=None):
        out = []
        for doc in self.docs:
            if doc.get("user_id") != filt.get("user_id"):
                continue
            if "type" in filt and doc.get("type") != filt.get("type"):
                continue
            out.append(dict(doc))
        return out


def _checkin(day_offset):
    return {"user_id": UID, "type": "checkin", "created_at": JOINED + timedelta(days=day_offset, hours=1)}


THREE_CHECKINS = [_checkin(0), _checkin(1), _checkin(2)]


def _wire(monkeypatch, db, *, checkins=THREE_CHECKINS, user_doc=None, subscribed=True):
    monkeypatch.setattr(m, "db", db)
    monkeypatch.setattr(m, "users_collection", FakeUsers(user_doc))
    # xp_events/xp_ledger are read straight off vouchers.db in
    # _count_welcome_checkin_days, so give the fake db real find()-able data.
    db.xp_events = FakeEvents(checkins)
    db.xp_ledger = FakeEvents([])
    monkeypatch.setattr(m, "_has_current_subscription_evidence", lambda _uid: subscribed)
    # issue_welcome_bonus_if_eligible (affiliate_rewards.py) independently
    # re-verifies channel subscription via a live Telegram API call before
    # issuing — mock it the same way the check-in progress gate is mocked
    # here so both authoritative subscription checks agree in tests.
    monkeypatch.setattr(ar, "_is_official_channel_subscribed", lambda _uid: subscribed)
    monkeypatch.setattr(m, "welcome_eligibility", lambda _uid, ref=None: (True, "ok", {}))
    monkeypatch.setattr(m, "_get_welcome_ticket", lambda uid: {"status": "active"})
    monkeypatch.setattr(m, "_get_welcome_eligibility", lambda uid: {})
    monkeypatch.setattr(m, "new_joiner_claims_col",
                         type("C", (), {"find_one": staticmethod(lambda filt, proj=None: None)})())


def _create_batch(db, *, codes, starts="2026-07-01 00:00:00", ends="2026-09-01 00:00:00"):
    return avb.create_batch(
        db, admin_identity="admin1", batch_name="Welcome Batch", pool_id="WELCOME",
        starts_at_local=starts, ends_at_local=ends, timezone_name="Asia/Kuala_Lumpur",
        codes=codes, now_utc=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


def _call(monkeypatch, db, **wire_kwargs):
    _wire(monkeypatch, db, **wire_kwargs)
    app = Flask(__name__)
    with app.app_context():
        return m.build_welcome_progress_response(UID, now=NOW)


# ── 5. completed + eligible + active batch + stock -> exactly one voucher ──
def test_completed_eligible_active_batch_with_stock_issues_one_voucher(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1", "WLC2"])
    data = _call(monkeypatch, db)
    assert data["status"] == "issued"
    assert data["voucher_code"] == "WLC1"
    ledger = db.affiliate_ledger.find_one({"dedup_key": f"WELCOME:{UID}"})
    assert ledger["status"] == "ISSUED"
    assert ledger["voucher_code"] == "WLC1"


# ── 6. repeat request -> same voucher, no duplicate ─────────────────────────
def test_repeat_request_returns_same_voucher_no_duplicate(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1", "WLC2"])
    first = _call(monkeypatch, db)
    second = _call(monkeypatch, db)
    assert first["voucher_code"] == second["voucher_code"] == "WLC1"
    assert second["status"] == "claimed"
    ledgers = list(db.affiliate_ledger.find({"dedup_key": f"WELCOME:{UID}"}))
    assert len(ledgers) == 1
    issued_rows = [r for r in db.voucher_pools.find({"issued_to": UID}) if r.get("status") == "issued"]
    assert len(issued_rows) == 1


# ── 7. two sequential "concurrent" requests on a fresh ledger -> one voucher ─
def test_two_requests_on_fresh_ledger_never_double_issue(monkeypatch):
    """Simulates a race: two requests both see no ledger yet. The atomic,
    status-filtered update_one inside issue_welcome_bonus_if_eligible (not a
    second allocator here) is what prevents a double-claim.
    """
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    outcomes = [_call(monkeypatch, db) for _ in range(2)]
    issued_codes = {o.get("voucher_code") for o in outcomes if o.get("voucher_code")}
    assert issued_codes == {"WLC1"}
    issued_rows = [r for r in db.voucher_pools.find({"status": "issued"})]
    assert len(issued_rows) == 1


# ── 8. existing ISSUED ledger -> claimed, allocator not re-invoked ──────────
def test_existing_issued_ledger_short_circuits(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1", "WLC2"])
    db.affiliate_ledger.insert_one({
        "ledger_type": "WELCOME", "user_id": UID, "tier": "WELCOME", "pool_id": "WELCOME",
        "status": "ISSUED", "dedup_key": f"WELCOME:{UID}", "voucher_code": "PRIOR-CODE",
        "risk_flags": [], "created_at": NOW_UTC, "updated_at": NOW_UTC,
    })
    data = _call(monkeypatch, db)
    assert data["status"] == "claimed"
    assert data["voucher_code"] == "PRIOR-CODE"
    # The batch's stock must be untouched — no second voucher claimed.
    assert db.voucher_pools.count_documents({"status": "issued"}) == 0


# ── 9. legacy new_joiner_claims row blocks a second (pool-based) issuance ──
def test_legacy_new_joiner_claim_blocks_pool_issuance(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    _wire(monkeypatch, db)
    monkeypatch.setattr(m, "new_joiner_claims_col",
                         type("C", (), {"find_one": staticmethod(lambda filt, proj=None: {"_id": 1})})())
    app = Flask(__name__)
    with app.app_context():
        data = m.build_welcome_progress_response(UID, now=NOW)
    assert data["status"] == "claimed"
    assert db.affiliate_ledger.count_documents({"dedup_key": f"WELCOME:{UID}"}) == 0
    assert db.voucher_pools.count_documents({"status": "issued"}) == 0


# ── 10. no active WELCOME batch -> explicit NO_ACTIVE_WELCOME_BATCH ────────
def test_no_active_batch_returns_explicit_reason(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"], starts="2026-10-01 00:00:00", ends="2026-11-01 00:00:00")
    data = _call(monkeypatch, db)
    assert data["status"] == "NO_ACTIVE_WELCOME_BATCH"
    assert data["hide_welcome_card"] is False


# ── 11. active batch with zero stock -> NO_FREE_CODES ──────────────────────
def test_active_batch_zero_stock_returns_no_free_codes(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    # Drain the only code with a different user first.
    _wire(monkeypatch, db)
    ar.issue_welcome_bonus_if_eligible(db, user_id=1, is_new_user=True, now_utc=NOW_UTC)
    data = _call(monkeypatch, db)
    assert data["status"] == "NO_FREE_CODES"
    assert data["hide_welcome_card"] is False


# ── 4. 3/3 checkins + unsubscribed -> no issuance ───────────────────────────
def test_completed_checkins_unsubscribed_does_not_issue(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    data = _call(monkeypatch, db, subscribed=False)
    assert data["welcome_pending_reason"] == "CHANNEL_NOT_JOINED"
    assert db.affiliate_ledger.count_documents({"dedup_key": f"WELCOME:{UID}"}) == 0
    assert db.voucher_pools.count_documents({"status": "issued"}) == 0


# ── 3. 2/3 checkins -> no ledger issuance ───────────────────────────────────
def test_two_of_three_checkins_does_not_issue(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    data = _call(monkeypatch, db, checkins=[_checkin(0), _checkin(1)])
    assert data["completed_days"] == 2
    assert db.affiliate_ledger.count_documents({"dedup_key": f"WELCOME:{UID}"}) == 0
    assert db.voucher_pools.count_documents({"status": "issued"}) == 0


# ── 2. self-invite gate still blocks pool issuance ──────────────────────────
def test_self_invite_blocks_pool_issuance(monkeypatch):
    db = fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    db.referral_audit.insert_one({"invitee_user_id": UID, "inviter_user_id": UID, "reason": "self_invite"})
    data = _call(monkeypatch, db)
    assert data["status"] == "not_eligible"
    assert data["hide_welcome_card"] is True
    assert data["welcome_pending_reason"] == "RISK_BLOCKED"
    assert db.affiliate_ledger.count_documents({"dedup_key": f"WELCOME:{UID}"}) == 0
    assert db.voucher_pools.count_documents({"status": "issued"}) == 0


# ── 14. Welcome Progress no longer touches db.drops/db.vouchers ────────────
def test_build_welcome_progress_response_never_touches_drops_or_vouchers_collections(monkeypatch):
    class ExplodingDb(fake_mongo.FakeDb):
        def __getattr__(self, name):
            if name in ("drops", "vouchers"):
                raise AssertionError(f"build_welcome_progress_response must not touch db.{name}")
            return super().__getattr__(name)

    db = ExplodingDb({"voucher_pools": [("pool_id", "code")], "affiliate_ledger": [("dedup_key",)]})
    _create_batch(db, codes=["WLC1"])
    data = _call(monkeypatch, db)
    assert data["status"] == "issued"


def test_no_drop_helpers_remain_on_vouchers_module():
    assert not hasattr(m, "_welcome_claim_drop_id")
    assert not hasattr(m, "_welcome_claim_drop_reason")
    assert not hasattr(m, "_welcome_drop_gate_reason")
