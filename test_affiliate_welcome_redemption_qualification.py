"""welcome_redemption_v1: verified Welcome redemption + lifetime account dedupe.

Runs against mongomock with the real unique / partial-unique indexes from
affiliate_qualification.INDEX_SPECS, so every dedupe guarantee asserted here
is enforced by the index, not by a pre-check.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from pymongo.errors import DuplicateKeyError

import affiliate_qualification as aq
import affiliate_rewards as ar
import referral_invitee_lock
import scheduler
from referral_rules import build_public_referral_status
from scripts import affiliate_qualification_admin as admin_cli
from scripts import migrate_affiliate_account_dedupe as migration

KL = aq.KL_TZ
CUTOFF = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 20, 4, 0, tzinfo=timezone.utc)
SOURCE_CONFIG = {
    "source": aq.SOURCE_MARKETING,
    "code_column": "coupon_code",
    "account_column": "account",
    "redeemed_at_column": "coupon_redeem_time",
    "status_column": "redeem_status",
    "success_values": ["success"],
    "source_timezone": "Asia/Kuala_Lumpur",
    "account_namespace": "advantplay",
    "campaign_ids": [],
}


def kl(*args) -> datetime:
    return KL.localize(datetime(*args)).astimezone(timezone.utc)


def utc(value):
    return aq._aware_utc(value)


def kl_text(moment: datetime) -> str:
    return moment.astimezone(KL).strftime("%Y-%m-%d %H:%M:%S")


class World:
    def __init__(self, *, mode=aq.MODE_ACTIVE, cutoff=CUTOFF, indexes=True):
        self.db = mongomock.MongoClient().db
        db = self.db
        if indexes:
            aq.ensure_indexes(db)
        db.referral_events.create_index([("event", 1), ("inviter_id", 1), ("invitee_id", 1)], unique=True)
        db.referral_award_events.create_index([("award_key", 1)], unique=True)
        db.affiliate_ledger.create_index([("dedup_key", 1)], unique=True)
        db.voucher_pools.create_index([("pool_id", 1), ("code", 1)], unique=True)
        referral_invitee_lock.ensure_indexes(db)
        control = {"_id": aq.CONTROL_ID, "mode": mode, "source_config": dict(SOURCE_CONFIG),
                   "seed_completed_at": CUTOFF, "seeded_identity_config": aq.identity_config(SOURCE_CONFIG)}
        if cutoff is not None:
            control["launch_cutoff_utc"] = cutoff
        db[aq.CONTROL_COLLECTION].insert_one(control)
        self._batch = 0

    def user(self, uid, *, linked=None, created=datetime(2026, 9, 1, tzinfo=timezone.utc)):
        doc = {"user_id": uid, "created_at": created, "joined_main_at": created, "username": f"u{uid}"}
        if linked is not None:
            doc["linked_gaming_accounts"] = list(linked)
        self.db.users.update_one({"user_id": uid}, {"$set": doc}, upsert=True)

    def join(self, inviter, invitee, *, at=datetime(2026, 10, 2, tzinfo=timezone.utc), group=-100, status="pending",
             revoked_reason=None):
        self.user(invitee, created=at)
        doc = {"group_id": group, "invitee_user_id": invitee, "inviter_user_id": inviter, "created_at_utc": at,
               "status": status, "destination_type": "community_group"}
        if revoked_reason:
            doc["revoked_reason"] = revoked_reason
        return self.db.pending_referrals.insert_one(doc).inserted_id

    def welcome_code(self, uid, code):
        self.db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": code, "status": "issued",
                                          "issued_to_user_id": uid})
        self.db.affiliate_ledger.update_one({"dedup_key": f"WELCOME:{uid}"},
                                            {"$set": {"voucher_code": code, "status": "ISSUED",
                                                      "ledger_type": "WELCOME", "user_id": uid}}, upsert=True)

    def upload(self, rows, *, status="completed"):
        self._batch += 1
        batch_id = f"batch{self._batch}"
        for row in rows:
            self.db.marketing_raw_data.insert_one({"upload_batch_id": batch_id, "campaign_id": "c1",
                                                   "campaign_name": "welcome", **row})
        self.db.marketing_upload_batches.insert_one({"upload_batch_id": batch_id, "status": status,
                                                     "uploaded_at": NOW})
        return batch_id

    @staticmethod
    def row(code, account, at, status="success"):
        return {"coupon_code": code, "account": account, "coupon_redeem_time": kl_text(at), "redeem_status": status}

    def qualified(self, **filt):
        return list(self.db.qualified_events.find(filt))


@pytest.fixture(autouse=True)
def _scheduler(monkeypatch):
    calls = {"dm": [], "tier": [], "first": [], "group": [], "congrats": []}
    monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "0")
    monkeypatch.setattr(scheduler, "_maybe_send_referral_qualified_dm", lambda *a, **k: calls["dm"].append(a))
    monkeypatch.setattr(scheduler, "maybe_handle_first_referral", lambda *a, **k: calls["first"].append(a))
    monkeypatch.setattr(scheduler, "maybe_unlock_affiliate_group", lambda **k: calls["group"].append(k))
    monkeypatch.setattr(scheduler, "maybe_shout_referral_congrats", lambda *a, **k: calls["congrats"].append(a))
    monkeypatch.setattr(scheduler, "evaluate_monthly_affiliate_reward",
                        lambda db, **k: calls["tier"].append(k))
    monkeypatch.setattr(scheduler, "now_kl", lambda: NOW.astimezone(KL))
    monkeypatch.setattr(scheduler, "now_utc", lambda: NOW)
    monkeypatch.setattr("xp.now_kl", lambda: NOW.astimezone(KL))
    return calls


def run(world, monkeypatch, now=NOW):
    monkeypatch.setattr(scheduler, "db", world.db)
    return scheduler.run_welcome_redemption_qualification(now_utc_ts=now)


def ev_status(world):
    return sorted((e["status"], e.get("reason")) for e in world.db[aq.EVIDENCE_COLLECTION].find())


# 1 ---------------------------------------------------------------------------

def test_five_joins_one_valid_redemption_gives_one_qualification(monkeypatch, _scheduler):
    w = World()
    for invitee in range(11, 16):
        w.join(1, invitee)
        w.welcome_code(invitee, f"WELC{invitee}")
    w.upload([w.row("WELC13", "0001234", datetime(2026, 10, 5, 6, tzinfo=timezone.utc))])
    out = run(w, monkeypatch)
    assert out["evidence"]["by_status"] == {aq.EV_QUALIFIED: 1}
    assert w.db.pending_referrals.count_documents({"inviter_user_id": 1}) == 5
    q = w.qualified()
    assert [(d["invitee_id"], d["referrer_id"]) for d in q] == [(13, 1)]
    assert q[0]["account_key"] == "advantplay:0001234"  # leading zeros preserved
    assert utc(q[0]["qualified_at"]) == utc(q[0]["redeemed_at"]) == datetime(2026, 10, 5, 6, tzinfo=timezone.utc)
    assert q[0]["rule_version"] == aq.RULE_VERSION and utc(q[0]["processed_at"]) == NOW
    assert q[0]["redemption_source"] == aq.SOURCE_MARKETING and q[0]["redemption_ref"]
    assert w.db.xp_events.count_documents({"user_id": 1, "unique_key": "ref:13"}) == 1
    assert w.db.referral_events.count_documents({"event": "referral_settled", "invitee_id": 13}) == 1
    assert w.db.pending_referrals.find_one({"invitee_user_id": 13})["status"] == "awarded"
    reg = w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:0001234"})
    assert reg["state"] == aq.REG_COMMITTED and reg["invitee_id"] == 13


# 2 ---------------------------------------------------------------------------

def test_claim_or_checkin_without_redemption_never_qualifies_after_cutoff(monkeypatch, _scheduler):
    w = World()
    pid = w.join(1, 21, at=NOW - timedelta(days=3))
    w.welcome_code(21, "WELC21")  # issued + claimed, never redeemed
    w.db.checkins.insert_one({"user_id": 21, "ts": NOW - timedelta(days=1)})
    monkeypatch.setattr(scheduler, "db", w.db)
    monkeypatch.setattr(scheduler, "_get_official_channel_member_status",
                        lambda *a: pytest.fail("legacy channel check must not run after cutoff"))
    monkeypatch.setattr(scheduler, "evaluate_referral_engagement",
                        lambda **k: pytest.fail("legacy engagement check must not run after cutoff"))
    monkeypatch.setattr(scheduler, "confirm_qualified_invitees", lambda: 0)
    scheduler.settle_pending_referrals()
    row = w.db.pending_referrals.find_one({"_id": pid})
    assert row["status"] == aq.PENDING_AWAITING_REDEMPTION
    run(w, monkeypatch)
    assert w.qualified() == []
    assert w.db.xp_events.count_documents({}) == 0
    assert w.db.referral_award_events.count_documents({}) == 0
    assert build_public_referral_status({"status": aq.PENDING_AWAITING_REDEMPTION})["status"] == "pending"


def test_legacy_settlement_unchanged_before_cutoff():
    control = {"mode": aq.MODE_ACTIVE, "launch_cutoff_utc": CUTOFF}
    assert aq.legacy_award_allowed(None, NOW) is True
    assert aq.legacy_award_allowed(control, CUTOFF - timedelta(seconds=1)) is True
    assert aq.legacy_award_allowed(control, CUTOFF) is False
    # Pausing never reactivates the legacy rule.
    assert aq.legacy_award_allowed({**control, "mode": aq.MODE_PAUSED}, NOW) is False
    assert aq.new_rule_active({**control, "mode": aq.MODE_PAUSED}, NOW) is False


# 3 / 4 -----------------------------------------------------------------------

def test_two_telegram_users_same_account_one_global_qualification(monkeypatch):
    w = World()
    w.join(1, 31)
    w.join(2, 32)
    w.welcome_code(31, "WELC31")
    w.welcome_code(32, "WELC32")
    w.upload([w.row("WELC31", "acct9", datetime(2026, 10, 5, tzinfo=timezone.utc)),
              w.row("WELC32", "acct9", datetime(2026, 10, 6, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    assert [d["invitee_id"] for d in w.qualified()] == [31]
    assert (aq.EV_DUPLICATE_ACCOUNT, "account_already_credited") in ev_status(w)


def test_same_account_across_affiliates_and_months_never_requalifies(monkeypatch):
    w = World()
    w.join(1, 41)
    w.welcome_code(41, "WELC41")
    w.upload([w.row("WELC41", "acctX", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    later = datetime(2026, 12, 3, tzinfo=timezone.utc)
    w.join(7, 42, at=datetime(2026, 11, 20, tzinfo=timezone.utc))
    w.welcome_code(42, "WELC42")
    w.upload([w.row("WELC42", "acctX", datetime(2026, 12, 2, tzinfo=timezone.utc))])
    run(w, monkeypatch, now=later)
    assert w.db.qualified_events.count_documents({}) == 1
    # Case and leading zeros are distinctions, not duplicates.
    assert aq.canonical_account_key("0007", "ns")[0] != aq.canonical_account_key("7", "ns")[0]
    assert aq.canonical_account_key(7, "ns") == (None, "account_numeric_coerced")


# 5 ---------------------------------------------------------------------------

def test_uniqueness_is_enforced_by_the_database_not_a_precheck():
    w = World()
    w.db.qualified_events.insert_one({"invitee_id": 1, "account_key": "ns:a"})
    with pytest.raises(DuplicateKeyError):
        w.db.qualified_events.insert_one({"invitee_id": 2, "account_key": "ns:a"})
    with pytest.raises(DuplicateKeyError):
        w.db.qualified_events.insert_one({"invitee_id": 1})
    w.db.qualified_events.insert_one({"invitee_id": 3})  # historical rows: no account_key, no conflict
    w.db.qualified_events.insert_one({"invitee_id": 4})
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "ns:b", "state": aq.REG_SEEDED})
    with pytest.raises(DuplicateKeyError):
        w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "ns:b", "state": aq.REG_RESERVED})


def test_concurrent_duplicate_imports_one_qualification_one_xp(monkeypatch, _scheduler):
    w = World()
    w.join(1, 51)
    w.join(2, 52)
    w.welcome_code(51, "WELC51")
    w.welcome_code(52, "WELC52")
    redeemed = datetime(2026, 10, 5, tzinfo=timezone.utc)
    rows = [w.row("WELC51", "acctC", redeemed), w.row("WELC52", "acctC", redeemed)]
    first, second = w.upload(rows), w.upload(rows)  # same file uploaded twice
    aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW)
    assert w.db[aq.EVIDENCE_COLLECTION].count_documents({}) == 2
    ev51 = w.db[aq.EVIDENCE_COLLECTION].find_one({"recipient_resolution.user_id": 51})
    ev52 = w.db[aq.EVIDENCE_COLLECTION].find_one({"recipient_resolution.user_id": 52})
    assert sorted(ev51["source_batch_ids"]) == [first, second]
    # Worker A reserved acctC and stalled; worker B races it on the same account.
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "advantplay:acctC", "state": aq.REG_RESERVED,
                                             "invitee_id": 51, "inviter_id": 1, "evidence_id": ev51["_id"],
                                             "reserved_at": NOW})
    control = aq.get_control(w.db)
    assert aq.evaluate_evidence(w.db, ev52, control=control, now_utc=NOW)["status"] == aq.EV_DUPLICATE_ACCOUNT
    assert aq.evaluate_evidence(w.db, ev51, control=control, now_utc=NOW)["status"] == aq.EV_QUALIFIED
    # Two effect workers on the same event: lease + idempotent keys => one XP.
    monkeypatch.setattr(scheduler, "db", w.db)
    scheduler.apply_welcome_redemption_effects(now_utc_ts=NOW)
    w.db.qualified_events.update_many({}, {"$unset": {"effects_applied_at": ""}})  # force a replay
    scheduler.apply_welcome_redemption_effects(now_utc_ts=NOW + timedelta(minutes=11))
    assert w.db.qualified_events.count_documents({}) == 1
    assert w.db.xp_events.count_documents({"unique_key": "ref:51"}) == 1
    assert w.db.xp_ledger.count_documents({"source_id": "ref:51"}) == 1
    assert w.db.referral_events.count_documents({"event": "referral_settled"}) == 1
    assert w.db.referral_award_events.count_documents({}) == 1


# 6 ---------------------------------------------------------------------------

def test_crash_after_reservation_resumes_without_losing_or_duplicating(monkeypatch):
    w = World()
    w.join(1, 61)
    w.welcome_code(61, "WELC61")
    w.upload([w.row("WELC61", "acctR", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    real_insert = w.db.qualified_events.insert_one
    state = {"crash": True}

    def crashing_insert(doc, *a, **k):
        if state.pop("crash", False):
            raise RuntimeError("process died after reservation")
        return real_insert(doc, *a, **k)

    monkeypatch.setattr(w.db.qualified_events, "insert_one", crashing_insert)
    run(w, monkeypatch)
    reg = w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:acctR"})
    assert reg["state"] == aq.REG_RESERVED and w.qualified() == []
    # Reconcile must NOT release a reservation whose evidence will resume.
    assert aq.reconcile(w.db, now_utc=NOW + timedelta(hours=1))["left_for_resume"] == 1
    run(w, monkeypatch, now=NOW + timedelta(minutes=6))  # lease expired -> resume own reservation
    assert [d["invitee_id"] for d in w.qualified()] == [61]
    assert w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:acctR"})["state"] == aq.REG_COMMITTED


def test_crash_after_event_insert_is_committed_by_reconcile():
    w = World()
    ev_id = w.db[aq.EVIDENCE_COLLECTION].insert_one({"status": aq.EV_RECEIVED, "source": "s", "source_ref": "r",
                                                     "lease_until": NOW + timedelta(minutes=5)}).inserted_id
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "ns:z", "state": aq.REG_RESERVED, "invitee_id": 9,
                                             "evidence_id": ev_id, "reserved_at": NOW})
    w.db.qualified_events.insert_one({"invitee_id": 9, "account_key": "ns:z", "evidence_id": ev_id,
                                      "rule_version": aq.RULE_VERSION})
    out = aq.reconcile(w.db, now_utc=NOW + timedelta(hours=1))
    assert out["committed"] == 1
    assert w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "ns:z"})["state"] == aq.REG_COMMITTED
    assert w.db[aq.EVIDENCE_COLLECTION].find_one({"_id": ev_id})["status"] == aq.EV_QUALIFIED


def test_orphan_reservation_is_released_never_permanently_consumed():
    w = World()
    ev_id = w.db[aq.EVIDENCE_COLLECTION].insert_one({"status": aq.EV_REJECTED, "source": "s",
                                                     "source_ref": "r"}).inserted_id
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "ns:o", "state": aq.REG_RESERVED, "invitee_id": 9,
                                             "evidence_id": ev_id, "reserved_at": NOW})
    assert aq.reconcile(w.db, now_utc=NOW + timedelta(hours=1))["released"] == 1
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 0


def test_reimport_is_idempotent(monkeypatch):
    w = World()
    w.join(1, 62)
    w.welcome_code(62, "WELC62")
    batch = w.upload([w.row("WELC62", "acctI", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    w.db.marketing_upload_batches.update_one({"upload_batch_id": batch}, {"$unset": {"welcome_evidence_extracted_at": ""}})
    run(w, monkeypatch)
    assert w.db[aq.EVIDENCE_COLLECTION].count_documents({}) == 1
    assert w.db.qualified_events.count_documents({}) == 1


# 7 ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "setup,expected",
    [
        ("unknown_code", ("not_recorded", "unknown_code")),
        ("non_welcome_code", ("not_recorded", "non_welcome_code")),
        ("failed_redemption", (aq.EV_REJECTED, "redemption_not_successful")),
        ("missing_account", (aq.EV_REJECTED, "missing_account")),
        ("ambiguous_recipient", (aq.EV_REVIEW, "ambiguous_recipient")),
        ("missing_inviter", (aq.EV_REJECTED, "no_referral_attribution")),
        ("self_referral", (aq.EV_REJECTED, "self_referral")),
        ("self_referral_account", (aq.EV_REJECTED, "self_referral_account")),
        ("conflicting_ownership", (aq.EV_REVIEW, "account_not_linked_to_invitee")),
        ("linked_to_other_user", (aq.EV_REVIEW, "account_linked_to_other_users")),
        ("referral_after_redemption", (aq.EV_REJECTED, "referral_after_redemption")),
        ("missing_time", (aq.EV_REVIEW, "missing_redeemed_at")),
    ],
)
def test_missing_ambiguous_or_self_referral_evidence_never_qualifies(monkeypatch, setup, expected):
    w = World()
    redeemed = datetime(2026, 10, 5, tzinfo=timezone.utc)
    row = w.row("WELC71", "acct71", redeemed)
    if setup != "missing_inviter":
        w.join(71 if setup == "self_referral" else 1, 71,
               at=redeemed + timedelta(days=1) if setup == "referral_after_redemption" else datetime(2026, 10, 2, tzinfo=timezone.utc))
    if setup not in ("unknown_code", "non_welcome_code"):
        w.welcome_code(71, "WELC71")
    if setup == "non_welcome_code":
        w.db.voucher_pools.insert_one({"pool_id": "T1", "code": "WELC71", "status": "issued", "issued_to_user_id": 5})
    if setup == "failed_redemption":
        row["redeem_status"] = "failed"
    if setup == "missing_account":
        row["account"] = ""
    if setup == "missing_time":
        row["coupon_redeem_time"] = ""
    if setup == "ambiguous_recipient":
        w.db.new_joiner_claims.insert_one({"uid": 99, "code": "WELC71"})
    if setup == "self_referral_account":
        w.user(1, linked=["acct71"])
    if setup == "conflicting_ownership":
        w.user(71, linked=["someOtherAcct"], created=datetime(2026, 10, 2, tzinfo=timezone.utc))
    if setup == "linked_to_other_user":
        w.user(555, linked=["acct71"])
    w.upload([row])
    run(w, monkeypatch)
    if expected[0] == "not_recorded":
        # Non-Welcome coupon rows are rejected at extraction and only counted.
        assert ev_status(w) == []
        assert w.db.marketing_upload_batches.find_one()["welcome_evidence_summary"][expected[1]] == 1
    else:
        assert ev_status(w) == [expected]
    assert w.qualified() == []
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 0


def test_verified_linkage_is_recorded_as_ownership_basis(monkeypatch):
    w = World()
    w.join(1, 72)
    w.welcome_code(72, "WELC72")
    w.user(72, linked=["acct72"], created=datetime(2026, 10, 2, tzinfo=timezone.utc))
    w.upload([w.row("WELC72", "acct72", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    assert w.qualified()[0]["ownership_basis"] == "verified_linkage"


def test_attribution_is_frozen_to_the_original_referral_record(monkeypatch):
    w = World()
    w.join(1, 73, at=datetime(2026, 10, 2, tzinfo=timezone.utc), status="revoked",
           revoked_reason="insufficient_engagement")  # legacy qualification failure: still the original record
    w.join(2, 73, at=datetime(2026, 10, 3, tzinfo=timezone.utc), group=-200)
    w.welcome_code(73, "WELC73")
    w.upload([w.row("WELC73", "acct73", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    assert w.qualified()[0]["referrer_id"] == 1


# 8 / 9 / 10 ------------------------------------------------------------------

def test_migration_seeds_history_then_credited_account_cannot_requalify(monkeypatch):
    w = World(indexes=False)
    # mongomock ignores partialFilterExpression when BUILDING an index over
    # existing documents (MongoDB honours it; insert-time enforcement works in
    # both), so this one index is created before the historical rows exist.
    # ensure_indexes() then recognises it as present and builds the rest.
    spec = next(s for s in aq.INDEX_SPECS if s[1] == "uniq_qualified_account_key_v1")
    w.db.qualified_events.create_index(spec[2], name=spec[1], **spec[3])
    hist_at = datetime(2026, 8, 10, tzinfo=timezone.utc)
    w.db.qualified_events.insert_one({"invitee_id": 81, "referrer_id": 1, "qualified_at": hist_at})
    w.db.qualified_events.insert_one({"invitee_id": 82, "referrer_id": 2, "qualified_at": hist_at + timedelta(days=1)})
    w.db.qualified_events.insert_one({"invitee_id": 83, "referrer_id": 2, "qualified_at": hist_at})
    w.db.affiliate_ledger.insert_one({"dedup_key": "AFF:1:202608:T1", "status": "ISSUED", "created_at": hist_at})
    for uid in (81, 82, 83):
        w.welcome_code(uid, f"WELC{uid}")
    w.upload([w.row("WELC81", "acctH", hist_at), w.row("WELC82", "acctH", hist_at),
              w.row("WELC83", "acctA", hist_at), w.row("WELC83", "acctB", hist_at)])
    ledgers_before = list(w.db.affiliate_ledger.find({}, {"_id": 0}))

    dry = migration.run(w.db, apply=False, seed_from_linkage=False, now_utc=NOW)
    assert dry["plan"]["accounts_to_seed"] == 1 and w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 0
    assert dry["plan"]["historical_duplicate_accounts"] == 1
    assert dry["plan"]["counts"]["conflicting_multiple_accounts"] == 1
    assert "WELC" not in str(dry) and "acctH" not in str(dry)  # masked output only

    applied = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW)
    assert applied["history_unchanged"] and applied["indexes_after"]["ok"] and applied["seed_marker_written"]
    assert applied["seed_result"]["seeded"] == 1
    again = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW)
    assert again["seed_result"] == {"seeded": 0, "already_seeded": 1, "held_by_other": 0}
    reg = w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:acctH"})
    assert reg["invitee_id"] == 81 and reg["historical_qualification_count"] == 2
    # Historical counts / rewards untouched.
    assert w.db.qualified_events.count_documents({}) == 3
    assert list(w.db.affiliate_ledger.find({}, {"_id": 0})) == ledgers_before
    assert w.db.qualified_events.count_documents({"account_key": {"$exists": True}}) == 0

    w.db.marketing_upload_batches.update_many({}, {"$set": {"welcome_evidence_extracted_at": NOW}})
    w.join(9, 84)
    w.welcome_code(84, "WELC84")
    w.upload([w.row("WELC84", "acctH", datetime(2026, 10, 7, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({"invitee_id": 84}) == 0
    assert (aq.EV_DUPLICATE_ACCOUNT, "account_already_credited") in ev_status(w)


def test_existing_invitee_qualification_is_never_counted_again(monkeypatch, _scheduler):
    w = World()
    w.db.qualified_events.insert_one({"invitee_id": 91, "referrer_id": 1, "qualified_at": datetime(2026, 9, 3, tzinfo=timezone.utc)})
    w.join(1, 91, status="awarded")
    w.welcome_code(91, "WELC91")
    w.upload([w.row("WELC91", "acct91", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({"invitee_id": 91}) == 1
    assert ev_status(w) == [(aq.EV_INVITEE_ALREADY_QUALIFIED, "invitee_already_qualified")]
    # The account is now known-credited (rule 9, live): nobody else can use it.
    assert w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:acct91"})["state"] == aq.REG_CREDITED_LEGACY
    assert w.db.xp_events.count_documents({}) == 0 and _scheduler["tier"] == []


# 11 --------------------------------------------------------------------------

def test_gmt8_month_boundary_and_settled_month_handling(monkeypatch, _scheduler):
    w = World()
    processing = kl(2026, 11, 3, 10, 0)
    late_oct = kl(2026, 10, 31, 23, 30)   # 15:30 UTC Oct 31 -> October in GMT+8
    early_nov = kl(2026, 11, 1, 0, 10)    # 16:10 UTC Oct 31 -> November in GMT+8
    for uid, code in ((101, "WELC101"), (102, "WELC102")):
        w.join(1, uid)
        w.welcome_code(uid, code)
    w.upload([w.row("WELC101", "acctO", late_oct), w.row("WELC102", "acctN", early_nov)])
    run(w, monkeypatch, now=processing)
    by_invitee = {d["invitee_id"]: d for d in w.qualified()}
    assert aq.kl_month_key(by_invitee[101]["qualified_at"]) == "202610"
    assert aq.kl_month_key(by_invitee[102]["qualified_at"]) == "202611"
    calls = {aq.kl_month_key(c["month_reference_utc"]): c["closed_month_review"] for c in _scheduler["tier"]}
    assert calls == {"202610": True, "202611": False}


def test_prelaunch_redemption_imported_late_is_clamped_to_launch(monkeypatch):
    w = World()
    w.join(1, 111, at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    w.welcome_code(111, "WELC111")
    w.upload([w.row("WELC111", "acctP", datetime(2026, 9, 20, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    q = w.qualified()[0]
    assert utc(q["redeemed_at"]) == datetime(2026, 9, 20, tzinfo=timezone.utc)
    assert utc(q["qualified_at"]) == CUTOFF and q["attribution_basis"] == "launch_cutoff_clamp"


def test_closed_month_tier_entitlement_goes_to_review_and_is_not_auto_issued(monkeypatch):
    monkeypatch.setenv("AFFILIATE_REWARD_RETENTION_DAYS", "0")
    db = mongomock.MongoClient().db
    month_ref = kl(2026, 10, 20, 12, 0)
    for i in range(ar.T1_THRESHOLD):
        db.qualified_events.insert_one({"invitee_id": 1000 + i, "referrer_id": 5, "qualified_at": month_ref})
    now = kl(2026, 11, 5, 12, 0)
    for _ in range(2):  # re-evaluation is idempotent
        ar.evaluate_monthly_affiliate_reward(db, referrer_id=5, now_utc=now, month_reference_utc=month_ref,
                                             closed_month_review=True)
    rows = list(db.affiliate_ledger.find({"user_id": 5}))
    assert len(rows) == 1
    assert rows[0]["year_month"] == "202610" and rows[0]["status"] == "PENDING_REVIEW"
    assert rows[0]["review_reason"] == ar.LATE_REDEMPTION_REVIEW_REASON and not rows[0].get("voucher_code")
    ar.settle_previous_month_affiliate_rewards(db, now_utc=now)
    assert db.affiliate_ledger.find_one({"user_id": 5})["status"] == "PENDING_REVIEW"


# 12 --------------------------------------------------------------------------

def test_dashboard_leaderboard_and_tier_counts_agree(monkeypatch):
    from affiliate_leaderboard import _compute_affiliate_monthly_rows

    w = World()
    for uid in (121, 122, 123):
        w.join(1, uid)
        w.welcome_code(uid, f"WELC{uid}")
    w.upload([w.row(f"WELC{uid}", f"acct{uid}", datetime(2026, 10, 5 + i, tzinfo=timezone.utc))
              for i, uid in enumerate((121, 122, 123))])
    run(w, monkeypatch)
    start, end, _ = ar._month_window_utc(NOW)
    tier_count = w.db.qualified_events.count_documents({"referrer_id": 1, "qualified_at": {"$gte": start, "$lt": end}})
    board = {r["referrer_id"]: r for r in _compute_affiliate_monthly_rows(w.db, start, end)}
    monkeypatch.setattr(scheduler, "db", w.db)
    referral_month = scheduler.current_month_qualified_referral_count(1, NOW)
    assert tier_count == board["1"]["qualified_month"] == referral_month == 3
    assert board["1"]["joins_month"] == 3


# 13 --------------------------------------------------------------------------

def test_welcome_issuance_and_join_tracking_unchanged_when_rule_active(monkeypatch):
    w = World()
    monkeypatch.setattr(ar, "_is_official_channel_subscribed", lambda uid: True)
    monkeypatch.setattr(ar, "_affiliate_simulate_enabled", lambda: False)
    w.db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "FRESH1", "status": "available"})
    out = ar.issue_welcome_bonus_if_eligible(w.db, user_id=131, is_new_user=True, now_utc=NOW)
    assert out["status"] == "ISSUED" and out["voucher_code"] == "FRESH1"
    # Parking leaves the referral record and its join intact and the invitee
    # lock non-blocking, exactly like a legacy revocation.
    pid = w.join(1, 132)
    referral_invitee_lock.claim(w.db, invitee_user_id=132, inviter_user_id=1, chat_id=-100,
                                destination_type="community_group", now_utc_ts=NOW)
    aq.park_pending_referral(w.db, pending_id=pid, invitee_user_id=132, inviter_user_id=1, now_utc=NOW)
    row = w.db.pending_referrals.find_one({"_id": pid})
    assert row["inviter_user_id"] == 1 and utc(row["created_at_utc"]) == datetime(2026, 10, 2, tzinfo=timezone.utc)
    assert referral_invitee_lock.claim(w.db, invitee_user_id=132, inviter_user_id=2, chat_id=-200,
                                       destination_type="official_channel", now_utc_ts=NOW) is True


# 14 --------------------------------------------------------------------------

def test_rollback_before_processing_voids_evidence(monkeypatch):
    w = World()
    w.join(1, 141)
    w.welcome_code(141, "WELC141")
    batch = w.upload([w.row("WELC141", "acctV", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW)
    assert aq.void_upload_batch(w.db, upload_batch_id=batch, reason="bad export", now_utc=NOW)["voided"] == 1
    run(w, monkeypatch)
    assert w.qualified() == [] and ev_status(w) == [(aq.EV_VOIDED, None)]


def test_rollback_after_qualification_flags_but_never_revokes_or_releases(monkeypatch):
    w = World()
    w.join(1, 142)
    w.welcome_code(142, "WELC142")
    batch = w.upload([w.row("WELC142", "acctW", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    out = aq.void_upload_batch(w.db, upload_batch_id=batch, reason="corrected", now_utc=NOW)
    assert out["flagged_qualified"] == 1
    q = w.qualified()[0]
    assert q["evidence_status"] == "voided_pending_reconciliation"
    assert w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:acctW"})["state"] == aq.REG_COMMITTED


def test_corrected_source_observation_goes_to_review(monkeypatch):
    w = World()
    w.join(1, 143)
    w.welcome_code(143, "WELC143")
    w.upload([w.row("WELC143", "acctK", datetime(2026, 10, 5, tzinfo=timezone.utc), status="failed")])
    aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW)
    w.upload([w.row("WELC143", "acctK", datetime(2026, 10, 5, tzinfo=timezone.utc), status="success")])
    run(w, monkeypatch)
    assert ev_status(w) == [(aq.EV_REVIEW, "conflicting_source_observations")]
    assert w.qualified() == []


# Safety rails -----------------------------------------------------------------

def test_refuses_to_process_without_unique_indexes(monkeypatch):
    w = World(indexes=False)
    w.join(1, 151)
    w.welcome_code(151, "WELC151")
    w.upload([w.row("WELC151", "acct151", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    out = run(w, monkeypatch)
    assert out["evidence"]["skipped"] == "indexes_not_ready" and w.qualified() == []


def test_disabled_or_paused_rule_does_nothing(monkeypatch):
    for mode, cutoff in ((aq.MODE_DISABLED, None), (aq.MODE_PAUSED, CUTOFF)):
        w = World(mode=mode, cutoff=cutoff)
        w.join(1, 152)
        w.welcome_code(152, "WELC152")
        w.upload([w.row("WELC152", "acct152", datetime(2026, 10, 5, tzinfo=timezone.utc))])
        run(w, monkeypatch)
        assert w.db[aq.EVIDENCE_COLLECTION].count_documents({}) == 0 and w.qualified() == []


def test_claim_time_column_and_naive_utc_are_refused():
    assert "redeemed_at_column_is_a_claim_time" in aq.validate_source_config({**SOURCE_CONFIG, "redeemed_at_column": "claim time"})
    assert aq.parse_redeemed_at("2026-10-31 23:30:00", "Asia/Kuala_Lumpur") == datetime(2026, 10, 31, 15, 30, tzinfo=timezone.utc)
    assert aq.parse_redeemed_at("2026-10-31", "Asia/Kuala_Lumpur") is None


def test_admin_cli_activation_guards(capsys):
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$unset": {"seed_completed_at": ""}})
    factory = lambda: w.db  # noqa: E731
    now = lambda: NOW  # noqa: E731
    # Preflight fails (not seeded, no committed batches) -> refused, nothing written.
    rc = admin_cli.main(["activate", "--cutoff", "2026-11-01T00:00:00+08:00", "--commit"], db_factory=factory, now_fn=now)
    assert rc == 2 and aq.launch_cutoff(aq.get_control(w.db)) is None
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {
        "seed_completed_at": NOW, "seeded_identity_config": aq.identity_config(SOURCE_CONFIG)}})
    w.upload([w.row("X", "acct", NOW)])
    # Dry run writes nothing.
    assert admin_cli.main(["activate", "--cutoff", "2026-11-01T00:00:00+08:00"], db_factory=factory, now_fn=now) == 0
    assert aq.launch_cutoff(aq.get_control(w.db)) is None
    assert admin_cli.main(["activate", "--cutoff", "2026-11-01T00:00:00+08:00", "--commit"], db_factory=factory,
                          now_fn=now) == 0
    assert aq.launch_cutoff(aq.get_control(w.db)) == kl(2026, 11, 1, 0, 0)
    # The cutoff is immutable; rollback = pause, which keeps legacy disabled.
    assert admin_cli.main(["activate", "--cutoff", "2026-12-01T00:00:00+08:00", "--commit"], db_factory=factory,
                          now_fn=now) == 2
    assert admin_cli.main(["pause", "--reason", "repair", "--commit"], db_factory=factory, now_fn=now) == 0
    control = aq.get_control(w.db)
    assert control["mode"] == aq.MODE_PAUSED
    assert aq.legacy_award_allowed(control, kl(2026, 11, 2, 0, 0)) is False


def test_same_invitee_two_accounts_racing_releases_the_losers_reservation():
    w = World()
    w.join(1, 161)
    w.welcome_code(161, "WELC161")
    w.db.new_joiner_claims.insert_one({"uid": 161, "code": "LEGACY161"})  # second Welcome code, same recipient
    redeemed = datetime(2026, 10, 5, tzinfo=timezone.utc)
    w.upload([w.row("WELC161", "acctY1", redeemed), w.row("LEGACY161", "acctY2", redeemed)])
    aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW)
    ev1, ev2 = list(w.db[aq.EVIDENCE_COLLECTION].find().sort("_id", 1))
    # Worker B reserved its account before worker A's event insert landed.
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": ev2["account_key"], "state": aq.REG_RESERVED,
                                             "invitee_id": 161, "inviter_id": 1, "evidence_id": ev2["_id"],
                                             "reserved_at": NOW})
    control = aq.get_control(w.db)
    assert aq.evaluate_evidence(w.db, ev1, control=control, now_utc=NOW)["status"] == aq.EV_QUALIFIED
    assert aq.evaluate_evidence(w.db, ev2, control=control, now_utc=NOW)["status"] == aq.EV_INVITEE_ALREADY_QUALIFIED
    assert w.db.qualified_events.count_documents({"invitee_id": 161}) == 1
    # acctY2 is not consumed by a qualification that never happened.
    assert w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": ev2["account_key"]}) is None


def test_large_upload_is_extracted_in_budgeted_resumable_slices():
    w = World()
    rows = []
    for uid in range(171, 178):
        w.welcome_code(uid, f"WELC{uid}")
        rows.append(w.row(f"WELC{uid}", f"acct{uid}", datetime(2026, 10, 5, tzinfo=timezone.utc)))
    w.upload(rows)
    progress = [aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW, max_rows=3) for _ in range(3)]
    assert [p["rows"] for p in progress] == [3, 3, 1]
    assert [p["batches_completed"] for p in progress] == [0, 0, 1]
    batch = w.db.marketing_upload_batches.find_one()
    assert batch["welcome_evidence_summary"]["recorded"] == 7 and batch.get("welcome_evidence_extracted_at")
    assert w.db[aq.EVIDENCE_COLLECTION].count_documents({}) == 7
    assert aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW)["rows"] == 0


def test_future_redemption_timestamp_goes_to_review(monkeypatch, _scheduler):
    w = World()
    w.join(1, 181)
    w.welcome_code(181, "WELC181")
    w.upload([w.row("WELC181", "acct181", NOW + timedelta(days=20))])
    run(w, monkeypatch)
    assert ev_status(w) == [(aq.EV_REVIEW, "redeemed_at_in_future")]
    assert w.qualified() == [] and _scheduler["tier"] == []


def test_requeue_re_resolves_recipient_after_data_fix(monkeypatch):
    w = World()
    w.join(1, 182)
    w.welcome_code(182, "WELC182")
    w.db.new_joiner_claims.insert_one({"uid": 99, "code": "WELC182"})  # bad legacy row -> ambiguous
    w.upload([w.row("WELC182", "acct182", datetime(2026, 10, 5, tzinfo=timezone.utc))])
    run(w, monkeypatch)
    assert ev_status(w) == [(aq.EV_REVIEW, "ambiguous_recipient")]
    w.db.new_joiner_claims.delete_many({"uid": 99})  # operator fixes the source data
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one()
    assert aq.requeue_evidence(w.db, evidence_id=ev["_id"], now_utc=NOW) is True
    run(w, monkeypatch)
    assert [d["invitee_id"] for d in w.qualified()] == [182]


def test_account_identity_config_is_frozen_after_seeding():
    w = World(mode=aq.MODE_PAUSED)
    base = ["configure-source", "--code-column", "coupon_code", "--account-column", "account",
            "--redeemed-at-column", "coupon_redeem_time", "--status-column", "redeem_status",
            "--success-values", "success", "--commit"]
    factory = lambda: w.db  # noqa: E731
    assert admin_cli.main(base + ["--account-namespace", "othertenant"], db_factory=factory, now_fn=lambda: NOW) == 2
    assert aq.get_control(w.db)["source_config"]["account_namespace"] == "advantplay"
    # Non-identity fields (e.g. success values) may still be corrected.
    assert admin_cli.main(base + ["--account-namespace", "advantplay", "--success-values", "success,ok"],
                          db_factory=factory, now_fn=lambda: NOW) == 0
    # And a changed identity can never pass preflight against the seeded registry.
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID},
                                           {"$set": {"source_config.account_namespace": "othertenant"}})
    assert admin_cli.preflight_report(w.db, now_utc=NOW)["identity_config_matches_seed"] is False
