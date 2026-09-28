"""Affiliate tier-reward continuous-retention gate + My Stats tier progress.

Business rules under test:

* Referral qualification, qualified counts and tier achievement are
  immediate. The AFFILIATE_MONTHLY entitlement is created immediately, but
  its voucher bundle is only issued after the referrer has stayed subscribed
  to the Official Channel for 7 CONTINUOUS days (``unlock_at =
  retention_started_at + 7d``, exact timedelta — not calendar days).
* Any leave before issuance breaks the window (RETENTION_BROKEN, never
  REJECTED); a rejoin restarts a fresh 7-day window from the rejoin.
* Telegram errors are UNKNOWN: no issuance, no reset, retried later.
* A delayed reward stays tied to the month/tier it was earned in.
* WELCOME and every other voucher system are untouched.

Inventory is real denomination-plan (Sep 2026+) stock loaded through
affiliate_voucher_batches.create_batch, exactly as production stocks it.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

import affiliate_rewards as ar
import affiliate_reward_retention as rr
import affiliate_voucher_batches as batches
from fake_mongo import FakeDb

UTC = timezone.utc
DAY = timedelta(days=1)
# 12:00 KL on Sep 28 2026 (KL = UTC+8) — still inside the 202609 KL month.
SEP_28 = datetime(2026, 9, 28, 4, 0, 0, tzinfo=UTC)
SEP_10 = datetime(2026, 9, 10, 4, 0, 0, tzinfo=UTC)

UNIQUE_KEYS = {
    "affiliate_ledger": [("dedup_key",)],
    "voucher_pools": [("pool_id", "code")],
    "qualified_events": [("invitee_id",)],
}

UID = 4242
# Sep 2026 recipes: T1 = 1x$10 (1 code, $10); T2 = 1x$5 + 2x$10 (3 codes, $25).
T1_CODES, T2_CODES = 1, 3


@pytest.fixture(autouse=True)
def _production_retention(monkeypatch):
    """Production default: the 7-day gate is ON for every test here."""
    monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "7")
    monkeypatch.delenv("AFFILIATE_SIMULATE", raising=False)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _db():
    return FakeDb(UNIQUE_KEYS)


def _stock(db, month="202609", count=30, now=SEP_10):
    for pool_id, prefix in (("AFFILIATE_5", "F"), ("AFFILIATE_10", "T"), ("AFFILIATE_50", "H")):
        result = batches.create_batch(
            db,
            admin_identity="test",
            batch_name=f"{pool_id} {month}",
            pool_id=pool_id,
            entitlement_month=month,
            codes=[f"{prefix}{month}{i:04d}" for i in range(count)],
            now_utc=now,
        )
        assert result["ok"] is True, result


def _user(db, uid=UID, *, subscribed=True):
    doc = {"user_id": uid, "blocked": False}
    if subscribed is not None:
        doc["official_channel_currently_subscribed"] = subscribed
    if subscribed is False:
        doc["left_official_channel_at"] = SEP_10
    db.users.insert_one(doc)


def _qualify(db, uid, total, at):
    have = db.qualified_events.count_documents({"referrer_id": uid})
    for i in range(total - have):
        db.qualified_events.insert_one(
            {"invitee_id": uid * 100_000 + have + i, "referrer_id": uid, "qualified_at": at}
        )


def _earn(db, uid=UID, total=25, at=SEP_28):
    _qualify(db, uid, total, at)
    return ar.evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=at)


def _ledger(db, tier, uid=UID):
    return db.affiliate_ledger.find_one({"user_id": uid, "tier": tier, "ledger_type": "AFFILIATE_MONTHLY"})


def _issued(db):
    return list(db.voucher_pools.find({"status": "issued"}))


def _aware(value):
    return ar._as_aware_utc(value)


class Checker:
    """Stand-in for the canonical tri-state getChatMember adapter."""

    def __init__(self, state=rr.MEMBERSHIP_MEMBER, reason=None, retry_after=None):
        self.state, self.reason, self.retry_after = state, reason, retry_after
        self.calls = []

    def __call__(self, uid):
        self.calls.append(uid)
        return self.state, self.reason, self.retry_after


def _run(db, at, checker=None, **kwargs):
    return rr.process_affiliate_retention_entitlements(
        db, now_utc=at, membership_checker=checker or Checker(), **kwargs
    )


def _leave(db, at, uid=UID):
    """What main.member_update_handler records on an Official Channel leave."""
    db.users.update_one(
        {"user_id": uid},
        {"$set": {"left_official_channel_at": at, "official_channel_currently_subscribed": False}},
        upsert=True,
    )
    return rr.on_official_channel_leave(db, user_id=uid, event_at=at, now_utc=at)


def _rejoin(db, at, uid=UID, *, processed_at=None):
    db.users.update_one(
        {"user_id": uid},
        {"$set": {"rejoined_official_channel_at": processed_at or at, "official_channel_currently_subscribed": True}},
        upsert=True,
    )
    return rr.on_official_channel_join(db, user_id=uid, event_at=at, now_utc=processed_at or at)


# ---------------------------------------------------------------------------
# MY STATS — next-tier progress (backend-owned)
# ---------------------------------------------------------------------------

class TestMyStatsProgress:
    def test_18_qualified_is_7_left_to_t2_worth_25(self):
        out = ar.affiliate_next_tier_progress(18, entitlement_month="202609")
        assert out["next_tier"] == "T2"
        assert out["qualified_left"] == 7
        assert out["next_reward_value"] == 25
        assert out["next_tier_threshold"] == 25
        assert out["current_tier"] == "T1"
        assert out["max_tier_reached"] is False

    def test_exact_threshold_counts_as_reached_and_advances(self):
        out = ar.affiliate_next_tier_progress(25, entitlement_month="202609")
        assert out["current_tier"] == "T2"
        assert out["next_tier"] == "T3"
        assert out["qualified_left"] == 50 - 25
        assert out["next_reward_value"] == 60

    @pytest.mark.parametrize("qualified", [250, 251, 9999])
    def test_max_tier_has_no_next_and_never_negative(self, qualified):
        out = ar.affiliate_next_tier_progress(qualified, entitlement_month="202609")
        assert out["next_tier"] == "MAX"
        assert out["qualified_left"] is None
        assert out["max_tier_reached"] is True
        assert out["next_reward_value"] == 350  # T5 = $50 x 7 (current/max-tier reward)

    @pytest.mark.parametrize("qualified,tier,left", [(0, "T1", 10), (None, "T1", 10), (-5, "T1", 10), (149, "T4", 1)])
    def test_left_is_never_negative(self, qualified, tier, left):
        out = ar.affiliate_next_tier_progress(qualified, entitlement_month="202609")
        assert (out["next_tier"], out["qualified_left"]) == (tier, left)

    def test_reward_value_follows_the_plan_of_the_month(self):
        # Same source of truth as issuance: Aug 2026 is the legacy plan.
        assert ar.affiliate_next_tier_progress(18, entitlement_month="202608")["next_reward_value"] == 15
        assert ar.affiliate_next_tier_progress(18, entitlement_month="202609")["next_reward_value"] == 25

    def test_thresholds_are_the_evaluators_own(self, monkeypatch):
        # Moving the evaluator's threshold moves My Stats with it.
        monkeypatch.setattr(ar, "T2_THRESHOLD", 30)
        out = ar.affiliate_next_tier_progress(25, entitlement_month="202609")
        assert (out["next_tier"], out["qualified_left"]) == ("T2", 5)


# ---------------------------------------------------------------------------
# RETENTION CREATION
# ---------------------------------------------------------------------------

class TestEntitlementCreation:
    def test_tier_reached_creates_held_entitlement_immediately(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)

        assert db.qualified_events.count_documents({"referrer_id": UID}) == 25
        for tier, value in (("T1", 10), ("T2", 25)):
            row = _ledger(db, tier)
            assert row["status"] == ar.RETENTION_PENDING_STATUS
            assert row["voucher_code"] is None and not row.get("vouchers")
            assert row["year_month"] == row["entitlement_month"] == "202609"
            assert row["qualified_count"] == 25
            assert row["reward_value"] == value
            assert row["bundle_recipe"]["reward_value"] == value  # frozen at earn
            assert _aware(row["earned_at"]) == SEP_28
            assert _aware(row["retention_started_at"]) == SEP_28
            assert _aware(row["unlock_at"]) == SEP_28 + timedelta(days=7)
            assert row["retention_required_seconds"] == 7 * 86400
            assert row["last_leave_at"] is None and row["last_rejoin_at"] is None
        assert _issued(db) == []  # inventory untouched while held

    def test_before_unlock_worker_does_nothing(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db)
        checker = Checker()
        stats = _run(db, SEP_28 + timedelta(days=7) - timedelta(seconds=1), checker)
        assert stats["candidates"] == 0 and stats["issued"] == 0
        assert checker.calls == []
        assert _issued(db) == []
        assert _ledger(db, "T2")["status"] == ar.RETENTION_PENDING_STATUS

    def test_already_out_of_channel_at_earn_starts_broken(self):
        db = _db()
        _stock(db)
        _user(db, subscribed=False)
        _earn(db, total=10)
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_BROKEN_STATUS
        assert row["unlock_at"] is None
        assert row["retention_broken_reason"] == "not_subscribed_at_earn"

    def test_re_evaluation_never_retimes_an_existing_entitlement(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        _qualify(db, UID, 12, SEP_28 + DAY)
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=SEP_28 + DAY)
        row = _ledger(db, "T1")
        assert _aware(row["retention_started_at"]) == SEP_28
        assert _aware(row["unlock_at"]) == SEP_28 + 7 * DAY
        assert row["qualified_count"] == 12  # progress still updates immediately

    def test_retention_setting_is_frozen_per_entitlement(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "1")
        _leave(db, SEP_28 + DAY)
        _rejoin(db, SEP_28 + 2 * DAY)
        # The rejoin window uses the entitlement's own frozen 7 days.
        assert _aware(_ledger(db, "T1")["unlock_at"]) == SEP_28 + 9 * DAY

    def test_gate_disabled_restores_immediate_issuance(self, monkeypatch):
        monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "0")
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        row = _ledger(db, "T1")
        assert row["status"] == "ISSUED"
        assert "retention_started_at" not in row


# ---------------------------------------------------------------------------
# CONTINUOUS RETENTION
# ---------------------------------------------------------------------------

class TestContinuousRetention:
    def test_seven_continuous_days_issues_exactly_once(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)
        checker = Checker()

        stats = _run(db, SEP_28 + 7 * DAY, checker)
        assert stats["issued"] == 2
        assert _ledger(db, "T1")["status"] == "ISSUED"
        assert _ledger(db, "T2")["status"] == "ISSUED"
        assert len(_ledger(db, "T2")["vouchers"]) == T2_CODES
        assert len(_issued(db)) == T1_CODES + T2_CODES
        assert _aware(_ledger(db, "T2")["retention_completed_at"]) == SEP_28 + 7 * DAY

        # Repeated scheduler runs are safe.
        again = _run(db, SEP_28 + 7 * DAY + timedelta(hours=1), checker)
        assert again["candidates"] == 0
        assert len(_issued(db)) == T1_CODES + T2_CODES
        assert len(checker.calls) == 2

    def test_leave_on_day_2_breaks_and_original_unlock_cannot_issue(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)
        assert _leave(db, SEP_28 + 2 * DAY) == 2
        for tier in ("T1", "T2"):
            row = _ledger(db, tier)
            assert row["status"] == ar.RETENTION_BROKEN_STATUS
            assert row["unlock_at"] is None
            assert _aware(row["last_leave_at"]) == SEP_28 + 2 * DAY
        # Even with a (wrong) "member" answer the original Day-7 cannot issue.
        stats = _run(db, SEP_28 + 7 * DAY, Checker(rr.MEMBERSHIP_MEMBER))
        assert stats["issued"] == 0 and _issued(db) == []

    def test_leave_rejoin_leave_rejoin_timeline(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)

        _leave(db, SEP_28 + 2 * DAY)                       # Day 2
        _rejoin(db, SEP_28 + 3 * DAY)                      # Day 3
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["retention_started_at"]) == SEP_28 + 3 * DAY
        assert _aware(row["last_rejoin_at"]) == SEP_28 + 3 * DAY
        assert _aware(row["unlock_at"]) == SEP_28 + 10 * DAY

        _leave(db, SEP_28 + 5 * DAY)                       # Day 5
        assert _ledger(db, "T1")["status"] == ar.RETENTION_BROKEN_STATUS
        _rejoin(db, SEP_28 + 6 * DAY)                      # Day 6
        assert _aware(_ledger(db, "T1")["unlock_at"]) == SEP_28 + 13 * DAY

        assert _run(db, SEP_28 + 10 * DAY)["issued"] == 0  # Day-10 unlock invalidated
        assert _issued(db) == []
        assert _run(db, SEP_28 + 13 * DAY)["issued"] == 1
        assert _ledger(db, "T1")["status"] == "ISSUED"

    def test_rejoin_just_before_original_day_7_must_restart(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        _leave(db, SEP_28 + 1 * DAY)
        rejoin_at = SEP_28 + 7 * DAY - timedelta(hours=1)
        _rejoin(db, rejoin_at)
        assert _run(db, SEP_28 + 7 * DAY)["issued"] == 0
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["unlock_at"]) == rejoin_at + 7 * DAY
        assert _issued(db) == []

    def test_subscribed_at_unlock_but_leave_recorded_after_start_does_not_issue(self):
        """The hooks never ran (process restart); only the canonical users
        record shows the exit/rejoin. Current membership alone must not pass."""
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        db.users.update_one(
            {"user_id": UID},
            {"$set": {
                "left_official_channel_at": SEP_28 + 3 * DAY,
                "rejoined_official_channel_at": SEP_28 + 3 * DAY + timedelta(hours=12),
                "official_channel_currently_subscribed": True,
            }},
        )
        checker = Checker(rr.MEMBERSHIP_MEMBER)
        stats = _run(db, SEP_28 + 7 * DAY, checker)
        assert stats["issued"] == 0 and _issued(db) == []
        assert checker.calls == []  # history check fails before any Telegram call
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_BROKEN_STATUS
        assert row["retention_broken_reason"] == "leave_recorded"

        # Next tick: the recovery sweep restarts from the recorded rejoin,
        # so a fresh full window is still required.
        _run(db, SEP_28 + 7 * DAY + timedelta(minutes=5))
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["unlock_at"]) == SEP_28 + 10 * DAY + timedelta(hours=12)
        assert _run(db, SEP_28 + 10 * DAY)["issued"] == 0
        assert _run(db, SEP_28 + 10 * DAY + timedelta(hours=12))["issued"] == 1

    def test_not_member_at_unlock_breaks_without_rejecting(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        stats = _run(db, SEP_28 + 7 * DAY, Checker(rr.MEMBERSHIP_NOT_MEMBER, "left"))
        assert stats["broken"] == 1 and _issued(db) == []
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_BROKEN_STATUS
        # Recoverable: a later rejoin restarts it.
        _rejoin(db, SEP_28 + 8 * DAY)
        assert _run(db, SEP_28 + 15 * DAY)["issued"] == 1


# ---------------------------------------------------------------------------
# MEMBERSHIP ERROR HANDLING
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, status_code, payload=None, *, bad_json=False):
        self.status_code = status_code
        self._payload = payload
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._payload


@pytest.fixture
def telegram(monkeypatch):
    """Drive the REAL canonical checker (scheduler._get_official_channel_member_status)."""
    import scheduler

    monkeypatch.setattr(scheduler, "BOT_TOKEN", "123:ABC")
    monkeypatch.setattr(scheduler, "OFFICIAL_CHANNEL_ID", -1001234)
    box = {}

    def fake_get(url, params=None, timeout=None):
        box.setdefault("calls", []).append((url, params))
        outcome = box["outcome"]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(scheduler.requests, "get", fake_get)
    return box


class TestMembershipTriState:
    @pytest.mark.parametrize("result,expected", [
        ({"status": "member"}, rr.MEMBERSHIP_MEMBER),
        ({"status": "administrator"}, rr.MEMBERSHIP_MEMBER),
        ({"status": "creator"}, rr.MEMBERSHIP_MEMBER),
        ({"status": "restricted", "is_member": True}, rr.MEMBERSHIP_MEMBER),
        ({"status": "restricted", "is_member": False}, rr.MEMBERSHIP_NOT_MEMBER),
        ({"status": "left"}, rr.MEMBERSHIP_NOT_MEMBER),
        ({"status": "kicked"}, rr.MEMBERSHIP_NOT_MEMBER),
        ({"status": "something_new"}, rr.MEMBERSHIP_UNKNOWN),
    ])
    def test_status_mapping(self, telegram, result, expected):
        telegram["outcome"] = _Resp(200, {"ok": True, "result": result})
        assert rr.official_channel_membership_state(UID)[0] == expected
        assert telegram["calls"][0][1]["chat_id"] == -1001234  # canonical channel config

    def test_timeout_is_unknown(self, telegram):
        telegram["outcome"] = requests.Timeout("read timed out")
        assert rr.official_channel_membership_state(UID)[:2] == (rr.MEMBERSHIP_UNKNOWN, "telegram_timeout")

    def test_429_is_unknown_with_retry_after(self, telegram):
        telegram["outcome"] = _Resp(429, {"ok": False, "error_code": 429, "parameters": {"retry_after": 42}})
        assert rr.official_channel_membership_state(UID) == (rr.MEMBERSHIP_UNKNOWN, rr.RATE_LIMITED_REASON, 42)

    @pytest.mark.parametrize("resp", [
        _Resp(200, bad_json=True),
        _Resp(502, {"ok": False}),
        _Resp(400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}),
        _Resp(200, {"ok": True, "result": {}}),
    ])
    def test_invalid_or_failed_responses_are_unknown(self, telegram, resp):
        telegram["outcome"] = resp
        assert rr.official_channel_membership_state(UID)[0] == rr.MEMBERSHIP_UNKNOWN

    def test_network_error_is_unknown(self, telegram):
        telegram["outcome"] = requests.ConnectionError("reset")
        assert rr.official_channel_membership_state(UID)[0] == rr.MEMBERSHIP_UNKNOWN


class TestUnknownMembershipIsSafe:
    @pytest.mark.parametrize("reason", ["telegram_timeout", rr.RATE_LIMITED_REASON, "telegram_malformed_response_200"])
    def test_unknown_never_issues_never_resets_and_retries(self, reason):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        unlock = SEP_28 + 7 * DAY
        stats = _run(db, unlock, Checker(rr.MEMBERSHIP_UNKNOWN, reason))
        assert stats["issued"] == 0 and stats["membership_retry"] == 1
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["retention_started_at"]) == SEP_28  # streak kept
        assert _aware(row["unlock_at"]) == unlock
        assert row["retention_last_check_reason"] == reason
        assert _issued(db) == []

        # Backed off: not re-checked on the very next tick...
        retry = Checker(rr.MEMBERSHIP_MEMBER)
        assert _run(db, unlock + timedelta(minutes=1), retry)["candidates"] == 0
        # ...but retried once the backoff passes, and then issues.
        assert _run(db, unlock + timedelta(minutes=6), retry)["issued"] == 1
        assert retry.calls == [UID]

    def test_429_stops_the_telegram_phase_for_this_tick(self):
        db = _db()
        _stock(db)
        for uid in (1, 2, 3):
            _user(db, uid)
            _earn(db, uid=uid, total=10)
        checker = Checker(rr.MEMBERSHIP_UNKNOWN, rr.RATE_LIMITED_REASON, 30)
        stats = _run(db, SEP_28 + 7 * DAY, checker)
        assert stats["rate_limited"] is True
        assert len(checker.calls) == 1

    def test_runtime_budget_bounds_the_tick(self):
        db = _db()
        _stock(db)
        for uid in (1, 2, 3):
            _user(db, uid)
            _earn(db, uid=uid, total=10)
        ticks = iter([0, 0, 999, 999, 999])
        checker = Checker()
        stats = _run(db, SEP_28 + 7 * DAY, checker, max_runtime_seconds=10, clock=lambda: next(ticks))
        assert stats["budget_exhausted"] is True
        assert len(checker.calls) == 1


# ---------------------------------------------------------------------------
# REJOIN RECOVERY
# ---------------------------------------------------------------------------

class TestRejoinRecovery:
    def test_broken_user_rejoins_and_completes_new_window(self):
        db = _db()
        _stock(db)
        _user(db, subscribed=False)
        _earn(db, total=10)
        rejoin_at = SEP_28 + 2 * DAY
        _rejoin(db, rejoin_at)
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["retention_started_at"]) == rejoin_at
        assert _aware(row["unlock_at"]) == rejoin_at + 7 * DAY
        assert _run(db, rejoin_at + 7 * DAY - timedelta(seconds=1))["issued"] == 0
        assert _run(db, rejoin_at + 7 * DAY)["issued"] == 1

    def test_recovery_sweep_restarts_when_the_rejoin_hook_never_ran(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        _leave(db, SEP_28 + DAY)
        # users record updated, affiliate hook lost (e.g. crash between writes)
        db.users.update_one(
            {"user_id": UID},
            {"$set": {"rejoined_official_channel_at": SEP_28 + 2 * DAY, "official_channel_currently_subscribed": True}},
        )
        stats = _run(db, SEP_28 + 2 * DAY + timedelta(minutes=5))
        assert stats["recovered"] == 1
        assert _aware(_ledger(db, "T1")["unlock_at"]) == SEP_28 + 9 * DAY
        assert _run(db, SEP_28 + 9 * DAY)["issued"] == 1

    def test_recovery_sweep_ignores_a_rejoin_older_than_the_leave(self):
        db = _db()
        _stock(db)
        _user(db)
        db.users.update_one({"user_id": UID}, {"$set": {"rejoined_official_channel_at": SEP_10}})
        _earn(db, total=10)
        _leave(db, SEP_28 + DAY)
        db.users.update_one({"user_id": UID}, {"$set": {"official_channel_currently_subscribed": True}})
        assert _run(db, SEP_28 + 2 * DAY)["recovered"] == 0
        assert _ledger(db, "T1")["status"] == ar.RETENTION_BROKEN_STATUS


# ---------------------------------------------------------------------------
# MONTH BOUNDARY
# ---------------------------------------------------------------------------

class TestMonthBoundary:
    def test_sep_28_earned_issues_on_oct_5_from_september_stock(self):
        db = _db()
        _stock(db, "202609")
        _stock(db, "202610", now=datetime(2026, 9, 30, tzinfo=UTC))
        _user(db)
        _earn(db, total=25, at=SEP_28)
        oct_5 = SEP_28 + 7 * DAY
        assert oct_5.astimezone(ar.KL_TZ).month == 10

        # October evaluation (no October qualification) touches nothing.
        assert ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=oct_5) is None

        assert _run(db, oct_5)["issued"] == 2
        sep_batch_ids = {b["_id"] for b in db.affiliate_voucher_batches.find({}) if "202609" in b["batch_name"]}
        for tier in ("T1", "T2"):
            row = _ledger(db, tier)
            assert row["status"] == "ISSUED"
            assert row["year_month"] == row["entitlement_month"] == "202609"
            assert row["tier"] == tier
        issued = _issued(db)
        assert len(issued) == T1_CODES + T2_CODES
        assert {r["batch_id"] for r in issued} <= sep_batch_ids  # never October stock
        assert sum(int(r.get("voucher_value") or 0) for r in issued) == 10 + 25

    def test_sep_28_leave_sep_30_rejoin_oct_2_issues_september_reward_oct_9(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25, at=SEP_28)
        _leave(db, SEP_28 + 2 * DAY)
        oct_2 = SEP_28 + 4 * DAY
        _rejoin(db, oct_2)
        oct_9 = oct_2 + 7 * DAY
        assert _aware(_ledger(db, "T2")["unlock_at"]) == oct_9
        assert _run(db, oct_9 - timedelta(minutes=1))["issued"] == 0
        assert _run(db, oct_9)["issued"] == 2
        row = _ledger(db, "T2")
        assert row["status"] == "ISSUED" and row["year_month"] == "202609"
        assert row["issued_value"] == 25


# ---------------------------------------------------------------------------
# CONCURRENCY / RECONCILIATION
# ---------------------------------------------------------------------------

class _LockedDb:
    """FakeDb with MongoDB's per-operation atomicity (see the migration suite)."""

    _lock = threading.RLock()

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return _LockedCollection(getattr(self._inner, name), self._lock)

    def __getitem__(self, name):
        return _LockedCollection(self._inner[name], self._lock)


class _LockedCollection:
    def __init__(self, inner, lock):
        self._inner, self._lock = inner, lock

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def _locked(*args, **kwargs):
            with self._lock:
                return attr(*args, **kwargs)

        return _locked


class TestConcurrency:
    def test_two_workers_only_one_acquires_settling(self):
        raw = _db()
        _stock(raw)
        _user(raw)
        _earn(raw, total=10)
        db = _LockedDb(raw)
        barrier = threading.Barrier(2, timeout=10)

        def checker(uid):
            barrier.wait()  # both workers have passed every pre-check
            return rr.MEMBERSHIP_MEMBER, None, None

        results = []
        workers = [
            threading.Thread(target=lambda: results.append(_run(db, SEP_28 + 7 * DAY, checker)))
            for _ in range(2)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=20)

        assert sorted(r["released"] for r in results) == [0, 1]
        assert sorted(r["lost_race"] for r in results) == [0, 1]
        assert len(_issued(raw)) == T1_CODES
        assert _ledger(raw, "T1")["status"] == "ISSUED"

    def test_crash_after_pool_assignment_reconciles_without_duplicate(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)
        real_store = ar._store_affiliate_bundle_on_ledger

        def crash(*args, **kwargs):
            raise RuntimeError("ledger write lost")

        monkeypatch.setattr(ar, "_store_affiliate_bundle_on_ledger", crash)
        stats = _run(db, SEP_28 + 7 * DAY)
        assert stats["errors"] == 2
        assert _ledger(db, "T2")["status"] == ar.SETTLING_STATUS
        stranded = len(_issued(db))
        assert stranded == T1_CODES + T2_CODES  # pool marked issued, ledger not

        monkeypatch.setattr(ar, "_store_affiliate_bundle_on_ledger", real_store)
        # The retention worker never re-touches it (no longer held)...
        assert _run(db, SEP_28 + 7 * DAY + timedelta(minutes=5))["candidates"] == 0
        # ...the existing stale-SETTLING recovery reconciles it.
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP_28 + 7 * DAY + timedelta(minutes=10))
        assert _ledger(db, "T1")["status"] == "ISSUED"
        assert _ledger(db, "T2")["status"] == "ISSUED"
        assert len(_issued(db)) == stranded  # no second bundle

    def test_leave_recorded_during_release_reverts_before_inventory(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        calls = {"n": 0}
        real = rr._membership_user_doc

        def racing_user_doc(db_, uid):
            calls["n"] += 1
            if calls["n"] == 2:  # the post-acquire re-read
                db_.users.update_one({"user_id": uid}, {"$set": {"left_official_channel_at": SEP_28 + 7 * DAY}})
            return real(db_, uid)

        monkeypatch.setattr(rr, "_membership_user_doc", racing_user_doc)
        stats = _run(db, SEP_28 + 7 * DAY + timedelta(seconds=1))
        assert stats["broken"] == 1 and stats["released"] == 0
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_BROKEN_STATUS
        assert "retention_completed_at" not in row
        assert _issued(db) == []


# ---------------------------------------------------------------------------
# EVENT IDEMPOTENCY
# ---------------------------------------------------------------------------

class TestEventIdempotency:
    def test_duplicate_leave_event(self):
        db = _db()
        _user(db)
        _earn(db, total=10)
        leave_at = SEP_28 + 2 * DAY
        assert _leave(db, leave_at) == 1
        assert _leave(db, leave_at) == 0
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_BROKEN_STATUS
        assert _aware(row["last_leave_at"]) == leave_at
        assert row["retention_break_count"] == 1

    def test_duplicate_rejoin_event_does_not_push_unlock_later(self):
        db = _db()
        _user(db)
        _earn(db, total=10)
        _leave(db, SEP_28 + 2 * DAY)
        rejoin_at = SEP_28 + 3 * DAY
        assert _rejoin(db, rejoin_at) == 1
        # Same Telegram update redelivered an hour later.
        assert _rejoin(db, rejoin_at, processed_at=rejoin_at + timedelta(hours=1)) == 0
        row = _ledger(db, "T1")
        assert _aware(row["unlock_at"]) == rejoin_at + 7 * DAY
        assert row["retention_restart_count"] == 1

    def test_out_of_order_leave_older_than_window_is_ignored(self):
        db = _db()
        _user(db)
        _earn(db, total=10)
        _leave(db, SEP_28 + 5 * DAY)
        _rejoin(db, SEP_28 + 6 * DAY)
        # A stale leave for Day 5 processed after the Day-6 rejoin.
        rr.on_official_channel_leave(db, user_id=UID, event_at=SEP_28 + 5 * DAY, now_utc=SEP_28 + 6 * DAY)
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["unlock_at"]) == SEP_28 + 13 * DAY

    def test_join_during_open_window_restarts_it(self):
        # A became-member transition means the user was absent before it,
        # even if that leave was never recorded.
        db = _db()
        _user(db)
        _earn(db, total=10)
        _rejoin(db, SEP_28 + 6 * DAY)
        assert _aware(_ledger(db, "T1")["unlock_at"]) == SEP_28 + 13 * DAY


# ---------------------------------------------------------------------------
# main.py wiring — the REAL member_update_handler drives the hooks
# ---------------------------------------------------------------------------

def _load_main_function(name, extra_globals):
    module = ast.parse(Path("main.py").read_text(encoding="utf-8"))
    node = next(n for n in module.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name)
    isolated = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = dict(extra_globals)
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env[name]


class _NullLogger:
    def __getattr__(self, _name):
        return lambda *a, **k: None


CHANNEL_ID, GROUP_ID = -100777, -100888


def _handler(db, clock):
    return _load_main_function(
        "member_update_handler",
        {
            "GROUP_ID": GROUP_ID,
            "OFFICIAL_CHANNEL_ID": CHANNEL_ID,
            "get_referral_destination": lambda: (CHANNEL_ID, "official_channel"),
            "now_utc": lambda: clock["now"],
            "logger": _NullLogger(),
            "db": db,
            "users_collection": db.users,
            "_confirm_referral_join": lambda *a, **k: None,
            "get_rejoin_buffer_settings": lambda: {"hours": 24},
            "REJOIN_CLAIM_BUFFER_HOURS_FALLBACK": 24,
            "timedelta": timedelta,
            "affiliate_retention_on_channel_leave": rr.on_official_channel_leave,
            "affiliate_retention_on_channel_join": rr.on_official_channel_join,
        },
    )


def _chat_member_update(old, new, at, uid=UID, chat_id=CHANNEL_ID):
    user = SimpleNamespace(id=uid, is_bot=False, username="aff")
    member = SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        old_chat_member=SimpleNamespace(status=old, is_member=old == "member"),
        new_chat_member=SimpleNamespace(status=new, user=user),
        date=at,
        invite_link=None,
    )
    return SimpleNamespace(chat_member=member, my_chat_member=None)


class TestMemberUpdateHandlerWiring:
    def test_channel_leave_and_rejoin_drive_retention_through_the_real_handler(self):
        db = _db()
        _user(db)
        _earn(db, total=10)
        clock = {"now": SEP_28 + 2 * DAY}
        handler = _handler(db, clock)

        asyncio.run(handler(_chat_member_update("member", "left", SEP_28 + 2 * DAY), None))
        assert _ledger(db, "T1")["status"] == ar.RETENTION_BROKEN_STATUS
        assert db.users.find_one({"user_id": UID})["official_channel_currently_subscribed"] is False

        # Processed 2 minutes after Telegram's own event time.
        clock["now"] = SEP_28 + 3 * DAY + timedelta(minutes=2)
        asyncio.run(handler(_chat_member_update("left", "member", SEP_28 + 3 * DAY), None))
        row = _ledger(db, "T1")
        assert row["status"] == ar.RETENTION_PENDING_STATUS
        assert _aware(row["unlock_at"]) == SEP_28 + 10 * DAY  # keyed on the event time

        # Redelivery of the same update is a no-op.
        clock["now"] += timedelta(hours=1)
        asyncio.run(handler(_chat_member_update("left", "member", SEP_28 + 3 * DAY), None))
        assert _aware(_ledger(db, "T1")["unlock_at"]) == SEP_28 + 10 * DAY

    def test_group_leave_never_touches_affiliate_retention(self):
        db = _db()
        _user(db)
        _earn(db, total=10)
        db.pending_referrals.insert_one({"_placeholder": True})
        handler = _load_main_function(
            "member_update_handler",
            {
                "GROUP_ID": GROUP_ID, "OFFICIAL_CHANNEL_ID": CHANNEL_ID,
                "get_referral_destination": lambda: (CHANNEL_ID, "official_channel"),
                "now_utc": lambda: SEP_28 + DAY, "logger": _NullLogger(), "db": db,
                "users_collection": db.users, "pending_referrals_collection": db.pending_referrals,
                "ReturnDocument": __import__("pymongo").ReturnDocument,
                "affiliate_retention_on_channel_leave": rr.on_official_channel_leave,
                "affiliate_retention_on_channel_join": rr.on_official_channel_join,
            },
        )
        asyncio.run(handler(_chat_member_update("member", "left", SEP_28 + DAY, chat_id=GROUP_ID), None))
        assert _ledger(db, "T1")["status"] == ar.RETENTION_PENDING_STATUS

    def test_retention_worker_runs_isolated_in_the_5min_tick(self):
        source = Path("main.py").read_text(encoding="utf-8")
        tick = source[source.index("def tick_5min"):source.index("def affiliate_monthly_settle_scheduled")]
        catch_up = tick.index("catch_up_missing_current_month_affiliate_ledgers(")
        release = tick.index("process_affiliate_retention_entitlements(")
        retry = tick.index("retry_current_month_pending_manual_ledgers(")
        assert catch_up < release < retry
        # Own try/except: a failure cannot skip the retry/reconcile passes.
        assert "step_error name=affiliate_retention_release" in tick[release:retry]


# ---------------------------------------------------------------------------
# NO OTHER ISSUANCE PATH CAN RELEASE A HELD ENTITLEMENT
# ---------------------------------------------------------------------------

class TestHeldEntitlementsAreUnreachableElsewhere:
    def _held(self, db=None):
        db = db if db is not None else _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)
        return db

    def test_evaluator_bulk_jobs_and_retry_sweep_leave_held_rows_alone(self):
        # mongomock: issue_current_month_affiliate_rewards passes a positional
        # projection, which FakeDb's find() does not model.
        import mongomock

        db = self._held(mongomock.MongoClient().db)
        ar.issue_current_month_affiliate_rewards(db, now_utc=SEP_28 + DAY)
        ar.catch_up_missing_current_month_affiliate_ledgers(db, now_utc=SEP_28 + DAY)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP_28 + 2 * DAY)
        ar.settle_previous_month_affiliate_rewards(db, now_utc=SEP_28 + 7 * DAY)  # October: settles 202609
        assert {_ledger(db, t)["status"] for t in ("T1", "T2")} == {ar.RETENTION_PENDING_STATUS}
        assert _issued(db) == []

    def test_surplus_sweep_leaves_held_rows_alone(self):
        db = self._held()  # FakeDb models the sweep's index hint
        stats = ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP_28 + 2 * DAY)
        assert stats.get("errors", 0) == 0
        assert {_ledger(db, t)["status"] for t in ("T1", "T2")} == {ar.RETENTION_PENDING_STATUS}
        assert _issued(db) == []

    def test_simulate_mode_never_converts_a_held_row(self, monkeypatch):
        # SIMULATED_PENDING rows are later settled by the month-end job, so a
        # held row rewritten to it would be issued without retention.
        db = self._held()
        monkeypatch.setenv("AFFILIATE_SIMULATE", "1")
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=SEP_28 + DAY)
        assert {_ledger(db, t)["status"] for t in ("T1", "T2")} == {ar.RETENTION_PENDING_STATUS}

    def test_admin_approve_cannot_release_a_held_row(self):
        db = self._held()
        assert ar.approve_affiliate_ledger(db, ledger_id=_ledger(db, "T2")["_id"], now_utc=SEP_28 + DAY) is None
        assert _ledger(db, "T2")["status"] == ar.RETENTION_PENDING_STATUS
        assert _issued(db) == []

    @pytest.mark.parametrize("status", sorted(ar.RETENTION_HOLD_STATUSES))
    def test_issuance_choke_point_refuses_held_rows(self, status):
        db = self._held()
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T2")["_id"]}, {"$set": {"status": status}})
        out = ar._issue_affiliate_ledger_from_pool(db, _ledger(db, "T2"), SEP_28 + 8 * DAY)
        assert out["status"] == status
        assert _issued(db) == []

    def test_pre_deploy_rows_keep_their_existing_semantics(self):
        """Existing non-final rows without retention fields are NOT re-gated."""
        db = _db()
        _stock(db)
        _user(db)
        _qualify(db, UID, 10, SEP_28)
        db.affiliate_ledger.insert_one({
            "ledger_type": "AFFILIATE_MONTHLY", "user_id": UID, "year_month": "202609",
            "entitlement_month": "202609", "tier": "T1", "pool_id": "T1",
            "reward_plan": "denomination_2026_09", "bundle_recipe": ar.tier_recipe("202609", "T1"),
            "status": "PENDING_MANUAL", "risk_flags": ["bundle_denomination_short"],
            "dedup_key": f"AFF:{UID}:202609:T1", "voucher_code": None, "created_at": SEP_10,
            "updated_at": SEP_10,
        })
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP_28)
        assert _ledger(db, "T1")["status"] == "ISSUED"


# ---------------------------------------------------------------------------
# USER-FACING STATUS (never exposes codes)
# ---------------------------------------------------------------------------

class TestRetentionStatusView:
    def test_states_and_countdown(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)
        view = rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=SEP_28 + 2 * DAY + timedelta(hours=12))
        t2 = next(v for v in view if v["tier"] == "T2")
        assert t2["retention_state"] == "pending_retention"
        assert t2["reward_value"] == 25
        assert t2["remaining_seconds"] == int((4 * DAY + timedelta(hours=12)).total_seconds())  # "4d 12h"
        assert t2["retention_days"] == 7
        assert t2["window_restarted"] is False
        assert "last_leave_at" not in t2 and "last_rejoin_at" not in t2 and "retention_started_at" not in t2

        _leave(db, SEP_28 + 3 * DAY)
        t2 = next(v for v in rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=SEP_28 + 3 * DAY) if v["tier"] == "T2")
        assert t2["retention_state"] == "retention_broken"
        assert t2["unlock_at"] is None and t2["remaining_seconds"] is None

        _rejoin(db, SEP_28 + 4 * DAY)
        t2 = next(v for v in rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=SEP_28 + 4 * DAY) if v["tier"] == "T2")
        assert t2["retention_state"] == "pending_retention" and t2["window_restarted"] is True
        _run(db, SEP_28 + 11 * DAY)
        # Still listed in October (previous month's entitlement).
        view = rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=SEP_28 + 11 * DAY)
        assert {v["retention_state"] for v in view} == {"issued"}
        for item in view:
            assert "vouchers" not in item and "voucher_code" not in item and "code" not in item
            assert "F2026" not in repr(item) and "T2026" not in repr(item)


# ---------------------------------------------------------------------------
# REGRESSIONS — Welcome and every other voucher system
# ---------------------------------------------------------------------------

class TestWelcomeVoucherUnchanged:
    def _welcome(self, monkeypatch, retention_days):
        monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", retention_days)
        monkeypatch.setattr(ar, "_is_official_channel_subscribed", lambda uid: True)
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "WELCOME-0001", "status": "available"})
        out = ar.issue_welcome_bonus_if_eligible(db, user_id=77, is_new_user=True, now_utc=SEP_28)
        return out, db.affiliate_ledger.find_one({"dedup_key": "WELCOME:77"})

    def test_welcome_issues_immediately_with_the_gate_on(self, monkeypatch):
        out, ledger = self._welcome(monkeypatch, "7")
        assert out == {"created": True, "status": "ISSUED", "voucher_code": "WELCOME-0001"}
        assert ledger["ledger_type"] == "WELCOME" and ledger["status"] == "ISSUED"
        for field in ("retention_started_at", "unlock_at", "retention_required_seconds", "earned_at"):
            assert field not in ledger

    def test_welcome_result_is_identical_with_gate_on_or_off(self, monkeypatch):
        on_out, on_ledger = self._welcome(monkeypatch, "7")
        off_out, off_ledger = self._welcome(monkeypatch, "0")
        assert on_out == off_out
        strip = lambda d: {k: v for k, v in d.items() if k != "_id"}  # noqa: E731
        assert strip(on_ledger) == strip(off_ledger)

    def test_welcome_path_shares_no_retention_logic(self):
        src = inspect.getsource(ar.issue_welcome_bonus_if_eligible)
        assert "RETENTION" not in src and "retention" not in src


# Modules that own public / pooled / personalised drops, VIP, Surprise,
# Mission, Campaign, tournament and admin-issued vouchers. None of them may
# learn about the affiliate retention gate.
_OTHER_VOUCHER_MODULES = [
    "vouchers.py", "voucher_pool_service.py", "voucher_risk_eligibility.py",
    "mission_pool.py", "mission_pool_processor.py", "campaign_engine.py",
    "campaign_rewards_api.py", "campaign_registration.py", "tournament_rewards.py",
    "lucky_games.py", "reactivation_journey.py", "channel_reactivation.py",
    "checkin.py", "onboarding.py",
]
_RETENTION_MARKERS = (
    "affiliate_reward_retention", "PENDING_RETENTION", "RETENTION_BROKEN",
    "RETENTION_HOLD_STATUSES", "affiliate_retention_period", "AFFILIATE_REWARD_RETENTION_DAYS",
)


class TestOtherVoucherSystemsUnchanged:
    @pytest.mark.parametrize("module", _OTHER_VOUCHER_MODULES)
    def test_no_other_voucher_system_references_the_gate(self, module):
        source = Path(module).read_text(encoding="utf-8")
        for marker in _RETENTION_MARKERS:
            assert marker not in source, f"{module} references {marker}"

    @pytest.mark.parametrize("helper", [
        "_claim_from_target_batch", "_claim_voucher_from_pool", "_claim_legacy_voucher",
        "_claim_one_denomination_voucher", "_claim_affiliate_bundle_from_pool",
    ])
    def test_shared_allocation_helpers_are_not_gated(self, helper):
        src = inspect.getsource(getattr(ar, helper))
        assert "RETENTION" not in src and "retention" not in src

    def test_weekly_affiliate_path_is_not_gated(self):
        for fn in (ar.evaluate_weekly_affiliate_reward, ar.issue_weekly_affiliate_rewards_for_window):
            assert "retention" not in inspect.getsource(fn).lower()


# ---------------------------------------------------------------------------
# INDEXES
# ---------------------------------------------------------------------------

def test_worker_indexes_are_declared_and_unique_by_key_pattern():
    created = []

    class _Recorder:
        def __init__(self, name):
            self.name = name

        def create_index(self, keys, **kwargs):
            created.append((self.name, tuple(keys), kwargs.get("name")))
            return kwargs.get("name")

        def list_indexes(self):
            return []

    class _RecorderDb:
        def __getattr__(self, name):
            return _Recorder(name)

    ar.ensure_affiliate_indexes(_RecorderDb())
    ledger = [(keys, name) for coll, keys, name in created if coll == "affiliate_ledger"]
    assert ((("ledger_type", 1), ("status", 1), ("unlock_at", 1)), "affiliate_type_status_unlock_at") in ledger
    assert (
        (("ledger_type", 1), ("status", 1), ("retention_checked_at", 1), ("_id", 1)),
        "affiliate_type_status_retention_checked",
    ) in ledger
    patterns = [keys for keys, _ in ledger]
    assert len(patterns) == len(set(patterns))


# ---------------------------------------------------------------------------
# ROLLOUT REPORT (read-only)
# ---------------------------------------------------------------------------

def test_rollout_report_separates_pre_deploy_rows_and_writes_nothing():
    from scripts.report_affiliate_retention_rollout import build_report

    db = _db()
    _stock(db)
    _user(db)
    _earn(db, total=25)
    for status, month in (("PENDING_MANUAL", "202608"), ("APPROVED", "202609"), ("ISSUED", "202608")):
        db.affiliate_ledger.insert_one({
            "ledger_type": "AFFILIATE_MONTHLY", "user_id": 1, "year_month": month, "tier": "T1",
            "status": status, "dedup_key": f"AFF:1:{month}:{status}", "voucher_code": None,
        })
    before = [dict(r) for r in db.affiliate_ledger.find({})]
    report = build_report(db, now_utc=SEP_28 + 7 * DAY)
    assert report["pre_deploy_non_final_ungated"]["total"] == 2
    assert report["pre_deploy_non_final_ungated"]["by_status"] == {"PENDING_MANUAL": 1, "APPROVED": 1}
    assert report["retention_gated"]["pending_retention"] == 2
    assert report["retention_gated"]["matured_backlog"] == 2
    assert report["integrity"]["held_rows_with_voucher"] == 0
    assert [dict(r) for r in db.affiliate_ledger.find({})] == before


def test_retention_sweeps_are_on_the_explain_checklist():
    """Same contract as test_affiliate_query_plan_checklist.py: every bounded
    sweep query must be verified by `verify_affiliate_reward_plan --check
    query-plans` against production-shaped data."""
    from scripts.verify_affiliate_reward_plan import _query_plan_specs

    def shape(value):
        if isinstance(value, dict):
            return {k: shape(v) for k, v in value.items()}
        if isinstance(value, list):
            return [shape(v) for v in value]
        return "<datetime>" if isinstance(value, datetime) else value

    captured = []
    raw = _db()

    class _Spy:
        def __getattr__(self, name):
            col = getattr(raw, name)
            if name != "affiliate_ledger":
                return col

            class _Col:
                def find(self, query=None, **kwargs):
                    if kwargs.get("sort"):
                        captured.append((shape(query), [tuple(s) for s in kwargs["sort"]]))
                    return col.find(query, **kwargs)

                def __getattr__(self, item):
                    return getattr(col, item)

            return _Col()

    _run(_Spy(), SEP_28)
    assert len(captured) == 2
    specs = {str(shape(q)): ([tuple(s) for s in sort or []], idx) for _l, _c, q, sort, idx, _h in _query_plan_specs("202609")}
    expected_index = {
        "unlock_at": "affiliate_type_status_unlock_at",
        "retention_checked_at": "affiliate_type_status_retention_checked",
    }
    for query, sort in captured:
        assert str(query) in specs, f"sweep query missing from explain() checklist: {query}"
        assert specs[str(query)][0] == sort
        assert specs[str(query)][1] == expected_index[sort[0][0]]
