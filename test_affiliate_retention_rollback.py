"""Retention rollback: immediate tier issuance + one-shot release of held rows.

Business rules under test:

* With AFFILIATE_REWARD_RETENTION_DAYS unset, reaching T1..T5 creates the
  monthly entitlement APPROVED and drives it APPROVED -> SETTLING -> ISSUED
  inside the same evaluation (the pre-PR-505 behaviour). The env override
  still re-enables the gate.
* ``release_retention_holds`` drains PENDING_RETENTION / RETENTION_BROKEN rows
  created while the gate was on, exactly once, through the canonical
  allocator — never a second allocator — keeping each row's entitlement
  month and its own month's batch.
* Blocked / abuse-flagged rows go to PENDING_REVIEW, never issued. Inventory
  shortage keeps the allocator's PENDING_MANUAL semantics.
* One user + one entitlement month + one tier = at most one issued bundle,
  under re-runs, concurrent evaluation, evaluator/backfill/worker races and
  crash recovery.

Inventory is real denomination-plan stock loaded through
affiliate_voucher_batches.create_batch, exactly as production stocks it.
"""
from __future__ import annotations

import ast
import copy
import io
import json
import threading
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import affiliate_rewards as ar
import affiliate_reward_retention as rr
import affiliate_voucher_batches as batches
from fake_mongo import FakeDb

UTC = timezone.utc
DAY = timedelta(days=1)
SEP_10 = datetime(2026, 9, 10, 4, 0, 0, tzinfo=UTC)
SEP_28 = datetime(2026, 9, 28, 4, 0, 0, tzinfo=UTC)   # 12:00 KL, inside 202609
SEP_30 = datetime(2026, 9, 30, 4, 0, 0, tzinfo=UTC)   # 12:00 KL, last day of 202609
OCT_01 = datetime(2026, 10, 1, 4, 0, 0, tzinfo=UTC)   # 12:00 KL, inside 202610
OCT_02 = datetime(2026, 10, 2, 4, 0, 0, tzinfo=UTC)

UNIQUE_KEYS = {
    "affiliate_ledger": [("dedup_key",)],
    "voucher_pools": [("pool_id", "code")],
    "qualified_events": [("invitee_id",)],
}
UID = 7_001_234
TOTALS = {"T1": 10, "T2": 25, "T3": 50, "T4": 150, "T5": 250}
# Sep 2026+ plan: (codes, $value) per tier.
RECIPE = {"T1": (1, 10), "T2": (3, 25), "T3": (2, 60), "T4": (6, 180), "T5": (7, 350)}
RETENTION_FIELDS = (
    "retention_started_at", "retention_required_seconds", "unlock_at", "earned_at",
    "retention_chat_id", "last_leave_at", "last_rejoin_at", "retention_completed_at",
    "retention_waived_at",
)


@pytest.fixture(autouse=True)
def _production_default(monkeypatch):
    """Production after the rollback: the env var is UNSET."""
    monkeypatch.delenv("AFFILIATE_REWARD_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("AFFILIATE_SIMULATE", raising=False)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _db():
    return FakeDb(UNIQUE_KEYS)


def _stock(db, month="202609", count=40, now=SEP_10, pools=("AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50")):
    prefixes = {"AFFILIATE_5": "F", "AFFILIATE_10": "T", "AFFILIATE_50": "H"}
    for pool_id in pools:
        result = batches.create_batch(
            db,
            admin_identity="test",
            batch_name=f"{pool_id} {month}",
            pool_id=pool_id,
            entitlement_month=month,
            codes=[f"{prefixes[pool_id]}{month}{i:04d}" for i in range(count)] if count else [],
            now_utc=now,
        )
        assert result["ok"] is True, result


def _user(db, uid=UID, *, subscribed=True, blocked=False):
    doc = {"user_id": uid, "blocked": blocked, "official_channel_currently_subscribed": subscribed}
    if subscribed is False:
        doc["left_official_channel_at"] = SEP_10
    db.users.insert_one(doc)


def _qualify(db, uid, total, at):
    have = db.qualified_events.count_documents({"referrer_id": uid})
    for i in range(total - have):
        db.qualified_events.insert_one(
            {"invitee_id": uid * 1_000 + have + i, "referrer_id": uid, "qualified_at": at}
        )


def _earn(db, uid=UID, total=10, at=SEP_28):
    _qualify(db, uid, total, at)
    return ar.evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=at)


def _hold(db, monkeypatch, *, uid=UID, total=25, at=SEP_28):
    """Create rows exactly as production did while the 7-day gate was on."""
    monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "7")
    _earn(db, uid=uid, total=total, at=at)
    monkeypatch.delenv("AFFILIATE_REWARD_RETENTION_DAYS")
    held = list(db.affiliate_ledger.find({"user_id": uid, "status": {"$in": sorted(ar.RETENTION_HOLD_STATUSES)}}))
    assert held, "fixture failed to create held rows"
    return held


def _ledger(db, tier, uid=UID, month=None):
    q = {"user_id": uid, "tier": tier, "ledger_type": "AFFILIATE_MONTHLY"}
    if month:
        q["year_month"] = month
    return db.affiliate_ledger.find_one(q)


def _issued(db, uid=None):
    q = {"status": "issued"}
    if uid is not None:
        q["issued_to_user_id"] = uid
    return list(db.voucher_pools.find(q))


def _batch_ids(db, month):
    return {b["_id"] for b in db.affiliate_voucher_batches.find({}) if month in b["batch_name"]}


def _backfill(db, now=OCT_02, **kw):
    return rr.release_retention_holds(db, now_utc=now, **kw)


def _snapshot(db):
    return {name: copy.deepcopy(coll._docs) for name, coll in db._collections.items()}


def _assert_one_bundle_per_tier(db, uid=UID):
    """The invariant: one user + month + tier = at most one issued bundle."""
    seen = {}
    for row in _issued(db, uid):
        ledger = db.affiliate_ledger.find_one({"_id": row["ledger_id"]})
        key = (ledger["user_id"], ledger["year_month"], ledger["tier"])
        seen.setdefault(key, set()).add(row["ledger_id"])
    for key, ledger_ids in seen.items():
        assert len(ledger_ids) == 1, key
    for ledger in db.affiliate_ledger.find({"user_id": uid, "status": "ISSUED"}):
        codes, value = RECIPE[ledger["tier"]]
        linked = [r for r in _issued(db, uid) if r["ledger_id"] == ledger["_id"]]
        assert len(linked) == codes == len(ledger["vouchers"]), ledger["tier"]
        assert ledger["issued_value"] == value


class _LockedDb:
    """FakeDb with MongoDB's per-operation atomicity."""

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


# ---------------------------------------------------------------------------
# 1-7: immediate issuance for future achievements
# ---------------------------------------------------------------------------

class TestImmediateIssuance:
    def test_default_gate_is_off_and_env_override_still_works(self, monkeypatch):
        assert ar.AFFILIATE_RETENTION_DEFAULT_DAYS == 0
        assert ar.affiliate_retention_period() is None
        monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "0")
        assert ar.affiliate_retention_period() is None
        monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "7")
        assert ar.affiliate_retention_period() == timedelta(days=7)
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        assert _ledger(db, "T1")["status"] == ar.RETENTION_PENDING_STATUS  # override honoured

    @pytest.mark.parametrize("tier", ["T1", "T2", "T3", "T4", "T5"])
    def test_tier_issues_immediately_at_exact_threshold(self, tier):
        db = _db()
        _stock(db)
        _user(db)
        coll = db.affiliate_ledger
        statuses = []
        real_update, real_fau = coll.update_one, coll.find_one_and_update

        def spy_update(query, update, upsert=False):
            new = (update.get("$set") or {}).get("status") or (update.get("$setOnInsert") or {}).get("status")
            if new:
                statuses.append(new)
            return real_update(query, update, upsert=upsert)

        def spy_fau(query, update, **kw):
            new = (update.get("$set") or {}).get("status")
            if new:
                statuses.append(new)
            return real_fau(query, update, **kw)

        coll.update_one, coll.find_one_and_update = spy_update, spy_fau

        _earn(db, total=TOTALS[tier])

        row = _ledger(db, tier)
        assert row["status"] == "ISSUED"
        assert row["entitlement_month"] == row["year_month"] == "202609"
        assert row["dedup_key"] == f"AFF:{UID}:202609:{tier}"
        codes, value = RECIPE[tier]
        assert row["issued_code_count"] == codes and row["issued_value"] == value
        # Created APPROVED, then SETTLING, then ISSUED — in this one call.
        assert statuses[0] == "APPROVED"
        assert "SETTLING" in statuses and statuses[-1] == "ISSUED"
        assert ar.RETENTION_PENDING_STATUS not in statuses and ar.RETENTION_BROKEN_STATUS not in statuses
        # Every lower tier reached in the month is its own issued milestone.
        for lower in ar.TIERS[: ar.TIERS.index(tier) + 1]:
            assert _ledger(db, lower)["status"] == "ISSUED"
        _assert_one_bundle_per_tier(db)

    def test_tenth_referral_via_mark_invitee_qualified_issues_t1(self):
        db = _db()
        _stock(db)
        _user(db)
        _qualify(db, UID, 9, SEP_28)
        assert ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=SEP_28) is None
        assert ar.mark_invitee_qualified(db, invitee_id=99_999_001, referrer_id=UID, now_utc=SEP_28) is True
        assert _ledger(db, "T1")["status"] == "ISSUED"
        assert len(_issued(db, UID)) == RECIPE["T1"][0]

    def test_unsubscribed_referrer_is_not_held_either(self):
        db = _db()
        _stock(db)
        _user(db, subscribed=False)
        _earn(db, total=10)
        assert _ledger(db, "T1")["status"] == "ISSUED"

    def test_below_t1_creates_nothing(self):
        db = _db()
        _stock(db)
        _user(db)
        assert _earn(db, total=9) is None
        assert db.affiliate_ledger.count_documents({}) == 0
        assert _issued(db) == []

    def test_new_rows_carry_no_retention_fields(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=25)
        for tier in ("T1", "T2"):
            row = _ledger(db, tier)
            for field in RETENTION_FIELDS:
                assert field not in row, (tier, field)

    def test_later_tier_issues_only_the_new_tier(self):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)
        t1 = _ledger(db, "T1")
        _earn(db, total=25)
        assert _ledger(db, "T1")["vouchers"] == t1["vouchers"]
        assert _ledger(db, "T2")["status"] == "ISSUED"
        assert len(_issued(db, UID)) == RECIPE["T1"][0] + RECIPE["T2"][0]
        assert db.affiliate_ledger.count_documents({"user_id": UID}) == 2


# ---------------------------------------------------------------------------
# 8-11: backfill basics
# ---------------------------------------------------------------------------

class TestBackfill:
    def test_existing_issued_row_is_untouched(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _earn(db, total=10)                       # T1 issued immediately
        before = copy.deepcopy(_ledger(db, "T1"))
        pool_before = copy.deepcopy(_issued(db))
        report = _backfill(db, dry_run=False)
        assert report["held_total"] == 0
        assert _ledger(db, "T1") == before
        assert _issued(db) == pool_before

    def test_held_rows_backfill_once(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        assert _issued(db) == []

        report = _backfill(db, dry_run=False)
        assert report["class_counts"]["RELEASE"] == 2
        assert report["outcomes"] == {"issued": 2}
        for tier in ("T1", "T2"):
            row = _ledger(db, tier)
            assert row["status"] == "ISSUED"
            assert row["retention_waived_at"] == row["retention_completed_at"] == OCT_02
            assert row["retention_release_source"] == "retention_rollback"
            assert "unlock_at" not in row and "retention_next_check_at" not in row
        _assert_one_bundle_per_tier(db)

    def test_second_run_changes_nothing(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        _backfill(db, dry_run=False)
        snap = _snapshot(db)
        report = _backfill(db, now=OCT_02 + DAY, dry_run=False)
        assert report["held_total"] == 0
        assert set(report["class_counts"].values()) == {0}
        assert report["outcomes"] == {}
        assert _snapshot(db) == snap

    def test_retention_broken_row_released_without_new_window(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db, subscribed=False)              # not subscribed at earn -> RETENTION_BROKEN
        held = _hold(db, monkeypatch, total=10)
        assert {r["status"] for r in held} == {ar.RETENTION_BROKEN_STATUS}
        report = _backfill(db, dry_run=False)
        assert report["class_counts"]["RELEASE"] == 1
        assert _ledger(db, "T1")["status"] == "ISSUED"

    def test_include_broken_false_leaves_broken_rows_held(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db, subscribed=False)
        _hold(db, monkeypatch, total=10)
        report = _backfill(db, dry_run=False, include_broken=False)
        assert report["held_total"] == 0
        assert _ledger(db, "T1")["status"] == ar.RETENTION_BROKEN_STATUS

    def test_dry_run_writes_nothing_and_reports(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        snap = _snapshot(db)
        report = _backfill(db)                    # default dry_run=True
        assert _snapshot(db) == snap
        assert report["dry_run"] is True
        assert report["held_total"] == 2
        assert report["held_by_status"] == {ar.RETENTION_PENDING_STATUS: 2}
        assert report["class_counts"]["RELEASE"] == 2
        assert set(report["class_counts"]) == set(rr.BACKFILL_CLASSES)
        assert report["by_tier"]["T1"]["RELEASE"] == 1 and report["by_tier"]["T2"]["RELEASE"] == 1
        assert report["by_entitlement_month"]["202609"]["RELEASE"] == 2
        # T1 = 1x$10; T2 = 1x$5 + 2x$10.
        assert report["demand_release"] == {"202609:AFFILIATE_10": 3, "202609:AFFILIATE_5": 1}
        inv = report["inventory"]["202609:AFFILIATE_10"]
        assert inv["source"] == "entitlement_batch" and inv["required"] == 3
        assert inv["available"] == 40 and inv["shortfall"] == 0
        assert inv["batch_window_closed"] is True   # September batch, reported in October
        # User-safe: masked ids, no codes.
        text = json.dumps(report, default=str)
        assert str(UID) not in text
        assert "T202609" not in text and "F202609" not in text
        assert all("outcome" not in r for r in report["rows"])


# ---------------------------------------------------------------------------
# 12-14: review routing and inventory semantics
# ---------------------------------------------------------------------------

def _park_already_issued(db, row, month):
    db.affiliate_ledger.update_one({"_id": row["_id"]}, {"$set": {"voucher_code": "X-ALREADY"}})


def _park_duplicate(db, row, month):
    db.affiliate_ledger.insert_one({
        "ledger_type": "AFFILIATE_MONTHLY", "user_id": UID, "year_month": month, "tier": "T1",
        "status": "REJECTED", "dedup_key": f"LEGACY-DUP-{month}"})


def _park_below_threshold(db, row, month):
    db.qualified_events.delete_one({"referrer_id": UID})


def _park_invalid(db, row, month):
    # Keeps the canonical dedup_key, so the evaluator still finds the row.
    db.affiliate_ledger.update_one({"_id": row["_id"]}, {"$set": {"entitlement_month": "209912"}})


def _PARK_INTEGRITY(db, row):
    db.voucher_pools.update_one(
        {"status": "available", "pool_id": "AFFILIATE_10"},
        {"$set": {"status": "issued", "issued_for_ledger_id": str(row["_id"]), "ledger_id": row["_id"]}})


_PARK_MUTATIONS = [
    ("EXCLUDE_ALREADY_ISSUED", _park_already_issued),
    ("EXCLUDE_DUPLICATE_TIER", _park_duplicate),
    ("EXCLUDE_BELOW_THRESHOLD", _park_below_threshold),
    ("EXCLUDE_INVALID", _park_invalid),
]


class TestReviewAndInventory:
    def test_blocked_held_user_goes_to_pending_review(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=10)
        db.users.update_one({"user_id": UID}, {"$set": {"blocked": True}})
        report = _backfill(db, dry_run=False)
        assert report["class_counts"]["REVIEW_BLOCKED"] == 1
        row = _ledger(db, "T1")
        assert row["status"] == "PENDING_REVIEW"
        assert row["review_reason"] == "retention_rollback_blocked_user"
        assert "blocked_user" in row["risk_flags"]
        assert row["tier"] == "T1" and row["year_month"] == "202609"   # entitlement kept
        assert _issued(db) == []
        # An admin can still approve it, from its own (now closed) September batch.
        db.users.update_one({"user_id": UID}, {"$set": {"blocked": False}})
        out = ar.approve_affiliate_ledger(db, ledger_id=row["_id"], now_utc=OCT_02)
        assert out["status"] == "ISSUED"
        assert {r["batch_id"] for r in _issued(db)} <= _batch_ids(db, "202609")

    def test_abuse_flagged_held_user_goes_to_pending_review(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=10)
        for i in range(3):   # deny_count_7d: >=3 denies in the month's last 7 days
            db.referral_audit.insert_one({
                "inviter_user_id": UID, "created_at": SEP_28 + timedelta(hours=i), "reason": "deny",
            })
        report = _backfill(db, dry_run=False)
        assert report["class_counts"]["REVIEW_RISK"] == 1
        row = _ledger(db, "T1")
        assert row["status"] == "PENDING_REVIEW"
        assert row["review_reason"] == "retention_rollback_risk_flags"
        assert "deny_count_7d" in row["risk_flags"]          # risk flags recorded
        assert _issued(db) == []

    def test_stored_abuse_flag_is_review_but_inventory_flag_is_not(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T1")["_id"]}, {"$set": {"risk_flags": ["ip_cluster"]}})
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T2")["_id"]}, {"$set": {"risk_flags": ["pool_empty"]}})
        report = _backfill(db, dry_run=False)
        assert report["class_counts"]["REVIEW_RISK"] == 1 and report["class_counts"]["RELEASE"] == 1
        assert _ledger(db, "T1")["status"] == "PENDING_REVIEW"
        t2 = _ledger(db, "T2")
        assert t2["status"] == "ISSUED"
        assert "pool_empty" not in t2["risk_flags"]           # cleared by the allocator on success

    def test_shortage_lands_in_existing_pending_manual(self, monkeypatch):
        db = _db()
        _stock(db)
        db.voucher_pools.delete_many({"pool_id": "AFFILIATE_50"})   # September $50 batch is empty
        _user(db)
        _hold(db, monkeypatch, total=50)
        report = _backfill(db, dry_run=False)
        t3 = _ledger(db, "T3")
        assert t3["status"] == "PENDING_MANUAL"
        assert "bundle_denomination_short" in t3["risk_flags"]
        assert t3["status"] != "OUT_OF_STOCK"
        assert _ledger(db, "T1")["status"] == _ledger(db, "T2")["status"] == "ISSUED"
        assert report["outcomes"] == {"issued": 2, "pending_manual": 1}
        assert report["inventory"]["202609:AFFILIATE_50"]["shortfall"] == 1
        # The existing retry sweep finishes it once September stock is replenished.
        sep_50 = next(b for b in db.affiliate_voucher_batches.find({}) if b["batch_name"] == "AFFILIATE_50 202609")
        db.voucher_pools.insert_one({
            "pool_id": "AFFILIATE_50", "code": "H-REPLENISH", "status": "available",
            "batch_id": sep_50["_id"], "voucher_value": 50,
        })
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=OCT_02 + timedelta(minutes=10))
        assert _ledger(db, "T3")["status"] == "ISSUED"
        _assert_one_bundle_per_tier(db)

    @pytest.mark.parametrize("cls, mutate", _PARK_MUTATIONS)
    def test_unsafe_exclusions_are_parked_for_review_not_issued(self, monkeypatch, cls, mutate):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=10)
        mutate(db, _ledger(db, "T1"), "202609")
        before = copy.deepcopy(_ledger(db, "T1"))
        issued_before = copy.deepcopy(_issued(db))

        report = _backfill(db, dry_run=False)
        assert report["class_counts"][cls] == 1 and report["class_counts"]["RELEASE"] == 0
        assert report["outcomes"] == {"pending_review": 1}
        row = _ledger(db, "T1")
        assert row["status"] == "PENDING_REVIEW"
        assert row["review_reason"] == f"retention_rollback_{cls.lower()}"
        assert row["retention_rollback_reasons"]
        # Only status, reason and retention bookkeeping changed.
        for field in ("tier", "year_month", "entitlement_month", "dedup_key", "voucher_code",
                      "vouchers", "risk_flags", "bundle_recipe", "qualified_count"):
            assert row.get(field) == before.get(field), field
        assert _issued(db) == issued_before

        # No automatic path releases it any more — not the retention worker
        # at what was its unlock time, nor any issuance sweep.
        matured = SEP_28 + 8 * DAY
        stats = rr.process_affiliate_retention_entitlements(
            db, now_utc=matured, membership_checker=lambda u, c=None: (rr.MEMBERSHIP_MEMBER, None, None))
        assert stats["candidates"] == 0
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=matured)
        ar.settle_previous_month_affiliate_rewards(db, now_utc=OCT_02)
        ar.catch_up_missing_current_month_affiliate_ledgers(db, now_utc=OCT_02)
        assert _ledger(db, "T1")["status"] == "PENDING_REVIEW"
        assert _issued(db) == issued_before
        # A second run does not select it again.
        assert _backfill(db, now=OCT_02 + DAY, dry_run=False)["held_total"] == 0

    def test_integrity_conflict_refuses_the_whole_commit(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _user(db, uid=UID + 1)
        _hold(db, monkeypatch, total=10)
        _hold(db, monkeypatch, uid=UID + 1, total=10)            # an otherwise releasable row
        _PARK_INTEGRITY(db, _ledger(db, "T1"))
        snap = _snapshot(db)
        report = _backfill(db, dry_run=False)
        assert report["refused"] == "integrity_conflicts"
        assert report["class_counts"]["EXCLUDE_INTEGRITY"] == 1 and report["class_counts"]["RELEASE"] == 1
        assert report["outcomes"] == {}
        assert _snapshot(db) == snap
        dry = _backfill(db)
        assert dry["refused"] is None and dry["class_counts"]["EXCLUDE_INTEGRITY"] == 1

    def test_rejected_and_normal_review_rows_are_never_selected(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T1")["_id"]}, {"$set": {"status": "REJECTED"}})
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T2")["_id"]}, {"$set": {"status": "PENDING_REVIEW"}})
        report = _backfill(db, dry_run=False)
        assert report["held_total"] == 0
        assert _ledger(db, "T1")["status"] == "REJECTED"
        assert _ledger(db, "T2")["status"] == "PENDING_REVIEW"
        assert _issued(db) == []


class TestRollbackReviewIsNeverAutoSettled:
    """A current-month row the rollback parked in PENDING_REVIEW must survive
    the 5-minute catch-up evaluation: only an admin approve/reject moves it.
    (Without the evaluator guard the next evaluation issued it.)"""

    def _october_held(self, monkeypatch, *, uid=UID):
        db = _db()
        _stock(db, "202610", now=SEP_30)
        _user(db, uid=uid)
        _hold(db, monkeypatch, uid=uid, total=10, at=OCT_01)
        return db

    def _sweep(self, db, rounds=3):
        for i in range(rounds):
            at = OCT_02 + timedelta(minutes=5 * (i + 1))
            ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=at)
            ar.catch_up_missing_current_month_affiliate_ledgers(db, now_utc=at)
            ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=at)

    def test_review_risk_current_month(self, monkeypatch):
        db = self._october_held(monkeypatch)
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T1")["_id"]}, {"$set": {"risk_flags": ["ip_cluster"]}})
        assert _backfill(db, dry_run=False)["outcomes"] == {"pending_review": 1}
        self._sweep(db)
        assert _ledger(db, "T1")["status"] == "PENDING_REVIEW"
        assert _issued(db) == []
        # The admin path still works.
        out = ar.approve_affiliate_ledger(db, ledger_id=_ledger(db, "T1")["_id"], now_utc=OCT_02 + DAY)
        assert out["status"] == "ISSUED"
        _assert_one_bundle_per_tier(db)

    def test_review_blocked_then_unblocked_current_month(self, monkeypatch):
        db = self._october_held(monkeypatch)
        db.users.update_one({"user_id": UID}, {"$set": {"blocked": True}})
        _backfill(db, dry_run=False)
        db.users.update_one({"user_id": UID}, {"$set": {"blocked": False}})
        self._sweep(db)
        assert _ledger(db, "T1")["status"] == "PENDING_REVIEW"
        assert _issued(db) == []

    @pytest.mark.parametrize("cls, mutate", [m for m in _PARK_MUTATIONS if m[0] != "EXCLUDE_BELOW_THRESHOLD"])
    def test_parked_exclusion_current_month(self, monkeypatch, cls, mutate):
        db = self._october_held(monkeypatch)
        mutate(db, _ledger(db, "T1"), "202610")
        report = _backfill(db, dry_run=False)
        assert report["class_counts"][cls] == 1
        self._sweep(db)
        row = db.affiliate_ledger.find_one({"dedup_key": f"AFF:{UID}:202610:T1"})
        assert row["status"] == "PENDING_REVIEW"
        assert _issued(db) == []

    def test_write_time_guard_holds_when_the_read_missed_the_park(self, monkeypatch):
        """The evaluator's settle CAS re-asserts the rule, so a stale read
        (the row parked between the evaluator's read and its write) still
        cannot settle a parked row."""
        db = self._october_held(monkeypatch)
        db.affiliate_ledger.update_one({"_id": _ledger(db, "T1")["_id"]}, {"$set": {"risk_flags": ["ip_cluster"]}})
        _backfill(db, dry_run=False)
        monkeypatch.setattr(ar, "_is_rollback_review_parked", lambda ledger: False)
        self._sweep(db, rounds=1)
        assert _ledger(db, "T1")["status"] == "PENDING_REVIEW"
        assert _issued(db) == []

    def test_ordinary_blocked_review_still_settles_after_unblock(self):
        """Narrowness: a blocked-user PENDING_REVIEW row NOT written by the
        rollback keeps its existing semantics (issued once unblocked)."""
        db = _db()
        _stock(db, "202610", now=SEP_30)
        _user(db, blocked=True)
        _earn(db, total=10, at=OCT_01)
        assert _ledger(db, "T1")["status"] == "PENDING_REVIEW"
        assert _ledger(db, "T1")["review_reason"] == "blocked_user"
        db.users.update_one({"user_id": UID}, {"$set": {"blocked": False}})
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=OCT_02)
        assert _ledger(db, "T1")["status"] == "ISSUED"


# ---------------------------------------------------------------------------
# 15-16: month boundary
# ---------------------------------------------------------------------------

class TestMonthBoundary:
    def test_sep_30_entitlement_released_in_october_uses_september_batch(self, monkeypatch):
        db = _db()
        _stock(db, "202609")
        _stock(db, "202610", now=SEP_30)
        _user(db)
        _hold(db, monkeypatch, total=25, at=SEP_30)
        assert OCT_02.astimezone(ar.KL_TZ).month == 10

        report = _backfill(db, now=OCT_02, dry_run=False)
        assert report["outcomes"] == {"issued": 2}
        sep_batches, oct_batches = _batch_ids(db, "202609"), _batch_ids(db, "202610")
        for tier in ("T1", "T2"):
            row = _ledger(db, tier)
            assert row["status"] == "ISSUED"
            assert row["year_month"] == row["entitlement_month"] == "202609"
            assert row["bundle_recipe"]["entitlement_month"] == "202609"
        issued = _issued(db)
        assert {r["batch_id"] for r in issued} <= sep_batches
        assert not ({r["batch_id"] for r in issued} & oct_batches)

    def test_september_entitlement_never_creates_october_rows(self, monkeypatch):
        db = _db()
        _stock(db, "202609")
        _stock(db, "202610", now=SEP_30)
        _user(db)
        _hold(db, monkeypatch, total=25, at=SEP_30)
        _backfill(db, now=OCT_02, dry_run=False)
        # October evaluation with no October qualification grants nothing.
        assert ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=OCT_02) is None
        assert db.affiliate_ledger.count_documents({"user_id": UID, "year_month": "202610"}) == 0
        assert db.affiliate_ledger.count_documents({"user_id": UID}) == 2


# ---------------------------------------------------------------------------
# 17-20: concurrency, races, crash recovery
# ---------------------------------------------------------------------------

class TestConcurrency:
    def test_concurrent_evaluations_issue_one_bundle(self, monkeypatch):
        raw = _db()
        _stock(raw)
        _user(raw)
        _qualify(raw, UID, 25, SEP_28)
        db = _LockedDb(raw)
        barrier = threading.Barrier(2, timeout=5)
        real_issue = ar._issue_affiliate_ledger_from_pool

        def rendezvous(db_, ledger, now_utc):
            try:
                barrier.wait()   # both evaluators reach the allocator together
            except threading.BrokenBarrierError:
                pass
            return real_issue(db_, ledger, now_utc)

        monkeypatch.setattr(ar, "_issue_affiliate_ledger_from_pool", rendezvous)
        errors = []

        def run():
            try:
                ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=SEP_28)
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        workers = [threading.Thread(target=run) for _ in range(2)]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=30)
        assert errors == []
        monkeypatch.setattr(ar, "_issue_affiliate_ledger_from_pool", real_issue)
        # A loser that saw LEASE_BUSY is completed by the next normal pass.
        ar.evaluate_monthly_affiliate_reward(raw, referrer_id=UID, now_utc=SEP_28 + timedelta(minutes=5))
        assert _ledger(raw, "T1")["status"] == _ledger(raw, "T2")["status"] == "ISSUED"
        assert len(_issued(raw, UID)) == RECIPE["T1"][0] + RECIPE["T2"][0]
        _assert_one_bundle_per_tier(raw)

    def test_evaluator_between_backfill_cas_and_allocation(self, monkeypatch):
        db = _db()
        _stock(db, "202610", now=SEP_30)
        _user(db)
        _hold(db, monkeypatch, total=10, at=OCT_01)   # October row: the evaluator re-reads it
        real_issue = rr._issue_affiliate_ledger_from_pool

        def evaluator_lands_first(db_, ledger, now_utc):
            assert ledger["status"] == ar.SETTLING_STATUS            # backfill CAS already won
            ar.evaluate_monthly_affiliate_reward(db_, referrer_id=UID, now_utc=now_utc)
            return real_issue(db_, ledger, now_utc)

        monkeypatch.setattr(rr, "_issue_affiliate_ledger_from_pool", evaluator_lands_first)
        report = _backfill(db, now=OCT_02, dry_run=False)
        assert report["outcomes"] == {"issued": 1}
        assert _ledger(db, "T1")["status"] == "ISSUED"
        assert len(_issued(db, UID)) == RECIPE["T1"][0]
        _assert_one_bundle_per_tier(db)

    def test_evaluator_while_backfill_holds_the_lease(self, monkeypatch):
        db = _db()
        _stock(db, "202610", now=SEP_30)
        _user(db)
        _hold(db, monkeypatch, total=25, at=OCT_01)
        real_claim = ar._claim_one_denomination_voucher
        fired = {"n": 0}

        def claim_then_race(*args, **kwargs):
            if fired["n"] == 0:
                fired["n"] += 1
                # The evaluator runs mid-allocation: its allocator call must
                # find the lease taken and walk away without claiming.
                ar.evaluate_monthly_affiliate_reward(db, referrer_id=UID, now_utc=OCT_02)
            return real_claim(*args, **kwargs)

        monkeypatch.setattr(ar, "_claim_one_denomination_voucher", claim_then_race)
        _backfill(db, now=OCT_02, dry_run=False)
        monkeypatch.setattr(ar, "_claim_one_denomination_voucher", real_claim)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=OCT_02 + timedelta(minutes=10))
        assert _ledger(db, "T1")["status"] == _ledger(db, "T2")["status"] == "ISSUED"
        assert len(_issued(db, UID)) == RECIPE["T1"][0] + RECIPE["T2"][0]
        _assert_one_bundle_per_tier(db)

    def test_backfill_while_retention_worker_releases(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=10)
        matured = SEP_28 + 7 * DAY
        backfill_reports = []

        def checker(uid, chat_id=None):
            # Worker passed every pre-check; the backfill lands right now.
            backfill_reports.append(rr.release_retention_holds(db, now_utc=matured, dry_run=False))
            return rr.MEMBERSHIP_MEMBER, None, None

        stats = rr.process_affiliate_retention_entitlements(db, now_utc=matured, membership_checker=checker)
        assert backfill_reports[0]["outcomes"] == {"issued": 1}
        assert stats["lost_race"] == 1 and stats["released"] == 0
        assert len(_issued(db, UID)) == RECIPE["T1"][0]
        _assert_one_bundle_per_tier(db)

    def test_threaded_backfill_vs_worker_vs_rerun(self, monkeypatch):
        raw = _db()
        _stock(raw)
        _user(raw)
        _hold(raw, monkeypatch, total=25)
        db = _LockedDb(raw)
        matured = SEP_28 + 7 * DAY
        jobs = [
            lambda: rr.release_retention_holds(db, now_utc=matured, dry_run=False),
            lambda: rr.release_retention_holds(db, now_utc=matured, dry_run=False),
            lambda: rr.process_affiliate_retention_entitlements(
                db, now_utc=matured, membership_checker=lambda u, c=None: (rr.MEMBERSHIP_MEMBER, None, None)),
        ]
        errors = []

        def run(job):
            try:
                job()
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(j,)) for j in jobs]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert errors == []
        ar._retry_stuck_pending_manual_affiliate_ledgers(raw, now_utc=matured + timedelta(minutes=10))
        assert _ledger(raw, "T1")["status"] == _ledger(raw, "T2")["status"] == "ISSUED"
        assert len(_issued(raw, UID)) == RECIPE["T1"][0] + RECIPE["T2"][0]
        _assert_one_bundle_per_tier(raw)

    @pytest.mark.parametrize("blocked", [False, True])
    def test_row_rejected_after_classification_is_never_written(self, monkeypatch, blocked):
        """An admin reject landing between the read and the CAS wins: the
        backfill must not resurrect a REJECTED row (release or review)."""
        db = _db()
        _stock(db)
        _user(db, blocked=False)
        _hold(db, monkeypatch, total=10)
        if blocked:
            db.users.update_one({"user_id": UID}, {"$set": {"blocked": True}})
        real_classify = rr._classify_held_row

        def classify_then_reject(db_, row):
            out = real_classify(db_, row)
            ar.reject_affiliate_ledger(db_, ledger_id=row["_id"], reason="admin", now_utc=OCT_02)
            return out

        monkeypatch.setattr(rr, "_classify_held_row", classify_then_reject)
        report = _backfill(db, dry_run=False)
        assert report["outcomes"] == {"lost_race": 1}
        row = _ledger(db, "T1")
        assert row["status"] == "REJECTED" and "retention_waived_at" not in row
        assert _issued(db) == []

    def test_crash_after_pool_allocation_recovers_without_new_codes(self, monkeypatch):
        db = _db()
        _stock(db, "202609")
        _stock(db, "202610", now=SEP_30)
        _user(db)
        _hold(db, monkeypatch, total=25, at=SEP_30)
        real_store = ar._store_affiliate_bundle_on_ledger

        def crash(*args, **kwargs):
            raise RuntimeError("process died before the ledger write")

        monkeypatch.setattr(ar, "_store_affiliate_bundle_on_ledger", crash)
        report = _backfill(db, now=OCT_02, dry_run=False)
        assert report["outcomes"] == {"error_RuntimeError": 2}
        stranded = _issued(db, UID)
        assert len(stranded) == RECIPE["T1"][0] + RECIPE["T2"][0]   # codes reserved, ledgers not
        assert {_ledger(db, t)["status"] for t in ("T1", "T2")} == {ar.SETTLING_STATUS}

        monkeypatch.setattr(ar, "_store_affiliate_bundle_on_ledger", real_store)
        assert _backfill(db, now=OCT_02 + timedelta(minutes=1), dry_run=False)["held_total"] == 0
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=OCT_02 + timedelta(minutes=10))
        assert {_ledger(db, t)["status"] for t in ("T1", "T2")} == {"ISSUED"}
        assert sorted(r["code"] for r in _issued(db, UID)) == sorted(r["code"] for r in stranded)
        assert {r["batch_id"] for r in _issued(db)} <= _batch_ids(db, "202609")
        _assert_one_bundle_per_tier(db)


# ---------------------------------------------------------------------------
# 21: simulation
# ---------------------------------------------------------------------------

class TestSimulation:
    def test_simulation_issues_zero_real_vouchers(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _user(db, uid=UID + 1)
        _hold(db, monkeypatch, uid=UID + 1, total=10)
        monkeypatch.setenv("AFFILIATE_SIMULATE", "1")
        _earn(db, total=25)
        assert {_ledger(db, t)["status"] for t in ("T1", "T2")} == {"SIMULATED_PENDING"}
        snap = _snapshot(db)
        refused = _backfill(db, dry_run=False)
        assert refused["refused"] == "affiliate_simulate_enabled"
        assert _snapshot(db) == snap
        dry = _backfill(db)
        assert dry["simulate_mode"] is True and dry["class_counts"]["RELEASE"] == 1
        assert _issued(db) == []


# ---------------------------------------------------------------------------
# 22-24: user-facing surfaces
# ---------------------------------------------------------------------------

class TestUserFacing:
    def test_visible_exposes_backfilled_reward(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        assert ar.affiliate_bundle_visible_cards(db, user_id=UID) == []
        _backfill(db, dry_run=False)
        cards = {c["affiliate_tier"]: c for c in ar.affiliate_bundle_visible_cards(db, user_id=UID)}
        assert set(cards) == {"T1", "T2"}
        assert cards["T2"]["voucher_count"] == 3 and cards["T2"]["total_value"] == 25
        assert len(cards["T2"]["vouchers"]) == 3 and all(v["code"] for v in cards["T2"]["vouchers"])

    def test_visible_route_still_appends_affiliate_cards(self):
        tree = ast.parse(Path(__file__).with_name("vouchers.py").read_text(encoding="utf-8"))
        calls = {
            node.func.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert "affiliate_bundle_visible_cards" in calls

    def test_my_stats_shows_issued_after_backfill(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        held = rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=OCT_02)
        assert {v["retention_state"] for v in held} == {"pending_retention"}
        _backfill(db, dry_run=False)
        view = rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=OCT_02)
        assert {v["tier"]: v["retention_state"] for v in view} == {"T1": "issued", "T2": "issued"}
        for item in view:
            assert item["unlock_at"] is None and item["remaining_seconds"] is None
            assert "vouchers" not in item and "T202609" not in repr(item)

    def test_my_stats_review_row_shows_under_review(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=10)
        db.users.update_one({"user_id": UID}, {"$set": {"blocked": True}})
        _backfill(db, dry_run=False)
        view = rr.affiliate_reward_retention_view(db, user_id=UID, now_utc=OCT_02)
        assert [v["retention_state"] for v in view] == ["under_review"]


# ---------------------------------------------------------------------------
# Production script (defaults to dry run; guarded commit)
# ---------------------------------------------------------------------------

class TestScript:
    def _main(self, argv, db):
        from scripts import release_affiliate_retention_holds as script

        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            rc = script.main(argv, db_factory=lambda: db, read_only_db_factory=lambda: db)
        return rc, out.getvalue(), err.getvalue()

    def test_default_is_dry_run(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        snap = _snapshot(db)
        rc, out, err = self._main([], db)
        assert rc == 0 and _snapshot(db) == snap
        report = json.loads(out)
        assert report["dry_run"] is True and report["class_counts"]["RELEASE"] == 2
        assert "class_counts=" in err and "inventory 202609:AFFILIATE_10" in err

    def test_commit_requires_expected_count(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        snap = _snapshot(db)
        assert self._main(["--commit"], db)[0] == 2
        assert self._main(["--commit", "--expect-release", "3"], db)[0] == 2
        assert _snapshot(db) == snap

    def test_commit_refused_while_gate_enabled(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        snap = _snapshot(db)
        monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "7")
        rc, _, err = self._main(["--commit", "--expect-release", "2"], db)
        assert rc == 2 and "ENABLED" in err
        assert _snapshot(db) == snap

    def test_commit_refused_on_integrity_conflict(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        t1 = _ledger(db, "T1")
        db.voucher_pools.update_one(
            {"status": "available", "pool_id": "AFFILIATE_10"},
            {"$set": {"status": "issued", "issued_for_ledger_id": str(t1["_id"]), "ledger_id": t1["_id"]}},
        )
        snap = _snapshot(db)
        rc, _, err = self._main(["--commit", "--expect-release", "1"], db)
        assert rc == 2 and "EXCLUDE_INTEGRITY" in err
        with pytest.raises(SystemExit) as exc:   # the override flag no longer exists
            self._main(["--commit", "--expect-release", "1", "--allow-integrity-exclusions"], db)
        assert exc.value.code == 2
        assert _snapshot(db) == snap

    def test_commit_releases_and_rerun_is_noop(self, monkeypatch):
        db = _db()
        _stock(db)
        _user(db)
        _hold(db, monkeypatch, total=25)
        rc, out, _ = self._main(["--commit", "--expect-release", "2", "--no-rows"], db)
        report = json.loads(out)
        assert rc == 0 and report["outcomes"] == {"issued": 2} and "rows" not in report
        rc, out, _ = self._main(["--commit", "--expect-release", "0"], db)
        assert rc == 0 and json.loads(out)["held_total"] == 0
        _assert_one_bundle_per_tier(db)
