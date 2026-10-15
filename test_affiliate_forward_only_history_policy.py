"""forward_only_from_activation_cutoff: cohort, duplicates, migration, writer lease, reporting.

Synthetic mongomock databases only; nothing here touches production.
"""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal

import pytest

import affiliate_forward_only as fwd
import affiliate_qualification as aq
import affiliate_qualification_preview as preview
import affiliate_v2_report
from scripts import affiliate_qualification_admin as admin
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_forward_only_rollout import forward_inputs
from test_affiliate_qualification_gates import databot_world
from test_affiliate_welcome_redemption_qualification import (
    CUTOFF, NOW, World, _ReadOnlyDb, _scheduler, _snapshot, kl_text, run,
)
from test_uim_redemption_sync import sync

HISTORY_COLLECTIONS = ("qualified_events", "referral_events", "affiliate_ledger", "xp_events", "referral_award_events")
OLD_JOIN = CUTOFF - timedelta(days=40)


def forward_world(*, historical=(999,), linked=None, redeemed=None, seed_from_linkage=False, snapshot=True, db=None):
    """Disabled rule, deterministic forward-only seeding applied, optionally the cohort snapshot built."""
    w = World(mode=aq.MODE_DISABLED, cutoff=None, db=db)
    for uid in historical:
        w.db.qualified_events.insert_one({"invitee_id": uid, "referrer_id": 900, "qualified_at": CUTOFF})
        w.db.xp_events.insert_one({"user_id": 900, "unique_key": f"ref:{uid}", "type": "referral_award"})
        w.db.referral_events.insert_one({"event": "referral_settled", "inviter_id": 900, "invitee_id": uid,
                                         "occurred_at": CUTOFF})
        w.db.affiliate_ledger.insert_one({"dedup_key": f"HIST:{uid}", "status": "ISSUED", "created_at": CUTOFF})
    for uid, accounts in (linked or {}).items():
        w.user(uid, linked=accounts)
    for uid, account in (redeemed or {}).items():  # deterministic identity from verified redemption evidence
        w.welcome_code(uid, f"HCODE-{uid}", issued_at=CUTOFF - timedelta(days=3))
        w.upload([w.row(f"HCODE-{uid}", account, CUTOFF - timedelta(days=1))])
    w.upload([w.row("coverage-only", "coverage-account", NOW)])
    result = migration.run(w.db, apply=True, seed_from_linkage=seed_from_linkage, now_utc=NOW,
                           history_policy=aq.HISTORY_FORWARD, acknowledge_unknown_history=True,
                           forward_only=forward_inputs(w.db, seed_from_linkage=seed_from_linkage))
    assert result["seed_marker_written"], result
    if snapshot:
        assert admin.main(["snapshot-launch-cohort", "--commit"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 0
    return w


def add(w, uid, account, *, amount="80", flag="1", status="manual", joined=OLD_JOIN, redeemed=None,
        issued=CUTOFF, upload=True, inviter=900):
    redeemed = redeemed or NOW - timedelta(days=1)
    w.join(inviter, uid, at=joined, status=status)
    w.welcome_code(uid, f"CODE-{uid}", issued_at=issued)
    if upload:
        w.upload([{**w.row(f"CODE-{uid}", account, redeemed), "after_total_bet_amount": amount,
                   "coupon_new_player": flag}])


def activate(w, *, cutoff=NOW, commit=True):
    return admin.main(["activate", "--cutoff", cutoff.isoformat(), "--acknowledge-unknown-history",
                       *(["--commit"] if commit else [])], db_factory=lambda: w.db, now_fn=lambda: NOW)


def go(w, monkeypatch, *, now=NOW):
    assert activate(w) == 0
    return run(w, monkeypatch, now=now)


def ev(w, uid):
    return w.db[aq.EVIDENCE_COLLECTION].find_one({"recipient_resolution.user_id": uid})


def history(w):
    return {name: sorted(map(str, w.db[name].find())) for name in HISTORY_COLLECTIONS}


# --- cohort ------------------------------------------------------------------

def historic_rows(w, uid):
    return {"qualified_events": list(w.db.qualified_events.find({"invitee_id": uid})),
            "referral_events": list(w.db.referral_events.find({"invitee_id": uid})),
            "xp_events": list(w.db.xp_events.find({"unique_key": f"ref:{uid}"})),
            "affiliate_ledger": list(w.db.affiliate_ledger.find({"dedup_key": f"HIST:{uid}"}))}


def test_existing_qualified_referral_excluded_and_unchanged(monkeypatch):
    w = forward_world()
    add(w, 999, "fresh-account-for-999", status="pending", joined=CUTOFF - timedelta(days=21))  # already qualified
    add(w, 101, "acct-101")
    before = historic_rows(w, 999)
    assert all(before.values())
    go(w, monkeypatch)
    assert historic_rows(w, 999) == before  # qualification, settlement, XP and ledger rows are byte-identical
    assert ev(w, 999)["status"] == aq.EV_INVITEE_ALREADY_QUALIFIED
    assert w.db[aq.COHORT_COLLECTION].find_one({"_id": 999}) is None
    assert w.db.qualified_events.count_documents({"invitee_id": 999}) == 1
    assert w.db.xp_events.count_documents({"unique_key": "ref:999"}) == 1
    # Only the genuinely new V2 qualification added effects.
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 1
    assert w.db.referral_events.count_documents({"invitee_id": 999}) == 1


def test_existing_pending_manual_included_and_preactivation_uim_evidence_usable(monkeypatch):
    w = forward_world()
    # Evidence uploaded BEFORE activation, redeemed before the cutoff; joins from long before it.
    add(w, 101, "acct-101", status="manual", joined=OLD_JOIN, redeemed=CUTOFF + timedelta(days=2))
    add(w, 102, "acct-102", status="pending_channel", joined=OLD_JOIN, redeemed=NOW - timedelta(days=3))
    go(w, monkeypatch)
    assert {q["invitee_id"] for q in w.db.qualified_events.find({"rule_version": aq.RULE_VERSION})} == {101, 102}
    q101 = w.db.qualified_events.find_one({"invitee_id": 101})
    assert q101["qualified_at"].replace(tzinfo=None) == NOW.replace(tzinfo=None)  # clamped to the cutoff
    assert w.db[aq.COHORT_COLLECTION].count_documents({}) == 2
    assert aq.get_control(w.db)["launch_cohort"]["invitees"] == 2


def test_future_referral_included_under_v2(monkeypatch):
    w = forward_world()
    go(w, monkeypatch)
    later = NOW + timedelta(minutes=1)
    add(w, 301, "acct-301", status="pending", joined=later, issued=later, redeemed=later + timedelta(minutes=1))
    run(w, monkeypatch, now=NOW + timedelta(minutes=10))
    assert w.db.qualified_events.find_one({"invitee_id": 301})["rule_version"] == aq.RULE_VERSION


def test_cohort_snapshot_counts_each_invitee_once_and_is_idempotent():
    w = forward_world()
    for status in ("pending", "manual", "error"):  # three rows for one invitee
        w.join(900, 101, status=status, at=OLD_JOIN)
    w.join(900, 102, status="pending", at=OLD_JOIN)
    w.join(900, 999, status="pending", at=OLD_JOIN)  # already qualified history
    first = fwd.snapshot_launch_cohort(w.db, now_utc=NOW)
    again = fwd.snapshot_launch_cohort(w.db, now_utc=NOW + timedelta(minutes=1))
    assert first["invitees"] == again["invitees"] == 2 and again["newly_snapshotted"] == 0
    assert first["excluded_already_qualified"] == 1
    assert len(w.db[aq.COHORT_COLLECTION].find_one({"_id": 101})["pending_ids"]) == 3


def test_interrupted_snapshot_is_not_ready_and_resumes():
    w = forward_world(snapshot=False)
    add(w, 101, "acct-101")
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"launch_cohort": {"state": fwd.STATE_BUILDING}}})
    report = admin.preflight_report(w.db, now_utc=NOW)
    assert "launch_cohort_snapshot_not_ready" in report["forward_only"]["blockers"] and not report["ok"]
    fwd.snapshot_launch_cohort(w.db, now_utc=NOW)
    assert fwd.cohort_status(w.db, aq.get_control(w.db))["ready"]


# --- decisions ---------------------------------------------------------------

@pytest.mark.parametrize("amount,flag,status,reason", [
    ("79.99", "1", aq.EV_REJECTED, "seven_day_turnover_below_80"),
    ("80.00", "1", aq.EV_QUALIFIED, None),
    ("80", "1", aq.EV_QUALIFIED, None),
    (str(Decimal("79.9999999999999999999999")), "1", aq.EV_REJECTED, "seven_day_turnover_below_80"),
    (str(Decimal("80.0000000000000000000001")), "1", aq.EV_QUALIFIED, None),
    ("500", "0", aq.EV_REJECTED, "not_a_new_coupon_player"),
    (None, "1", aq.EV_REVIEW, "missing_or_invalid_qualification_fields"),
])
def test_decision_rules_exact_decimal(amount, flag, status, reason, monkeypatch):
    w = forward_world()
    add(w, 101, "acct-101", amount=amount, flag=flag)
    go(w, monkeypatch)
    got = ev(w, 101)
    assert (got["status"], got.get("reason")) == (status, reason)
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == int(status == aq.EV_QUALIFIED)
    if status != aq.EV_QUALIFIED:  # only a confirmed pass earns anything
        assert w.db.xp_events.count_documents({}) == 1 and w.db.referral_events.count_documents({}) == 1


def test_missing_evidence_remains_pending(monkeypatch):
    w = forward_world()
    add(w, 101, "acct-101", upload=False)
    go(w, monkeypatch)
    assert ev(w, 101) is None
    assert w.db.pending_referrals.find_one({"invitee_user_id": 101})["status"] == aq.PENDING_AWAITING_REDEMPTION
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 0


def test_temporary_feed_error_remains_pending(monkeypatch):
    w, feed = databot_world()
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"history_policy": aq.HISTORY_FORWARD}})
    feed.fail_after = 0
    sync(w, feed, now=NOW + timedelta(seconds=1))
    out = run(w, monkeypatch, now=NOW + timedelta(seconds=1))
    assert out["evidence"]["skipped"] == "source_evidence_incomplete"
    assert w.db.qualified_events.count_documents({}) == 0
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["status"] == aq.EV_RECEIVED


def test_no_second_seven_day_wait_inside_apreferral(monkeypatch):
    w = forward_world()
    joined = NOW - timedelta(hours=3)  # a young join: the completed 7-day turnover is already in the report
    add(w, 101, "acct-101", status="pending", joined=joined, issued=joined, redeemed=NOW - timedelta(hours=1))
    go(w, monkeypatch)
    assert w.db.qualified_events.find_one({"invitee_id": 101})["rule_version"] == aq.RULE_VERSION


# --- duplicates ---------------------------------------------------------------

def test_known_historical_account_blocked_but_case_and_zeros_are_preserved(monkeypatch):
    w = forward_world(historical=(998, 999), redeemed={998: "000Known"})
    assert w.db[aq.REGISTRY_COLLECTION].find_one()["account_key"] == "advantplay:000Known"
    add(w, 101, "000Known")     # same account as deterministic history -> blocked
    add(w, 102, "000known")     # different case -> a different account
    add(w, 103, "00Known")      # different leading zeros -> a different account
    go(w, monkeypatch)
    assert ev(w, 101)["status"] == aq.EV_DUPLICATE_ACCOUNT
    assert ev(w, 102)["status"] == ev(w, 103)["status"] == aq.EV_QUALIFIED
    assert not w.db.qualified_events.count_documents({"invitee_id": 101})


def test_duplicate_among_pending_manual_candidates_earliest_redemption_owns_it(monkeypatch):
    w = forward_world()
    # The LATER redemption is ingested first; arrival order must not decide ownership.
    add(w, 102, "shared-acct", redeemed=NOW - timedelta(days=1))
    add(w, 101, "shared-acct", redeemed=NOW - timedelta(days=2))
    pv = preview.build_preview(_ReadOnlyDb(w.db), month=None, now_utc=NOW)["activation_cohort"]
    assert (pv["pass"], pv["duplicate_account_excluded"]) == (1, 1)
    go(w, monkeypatch)
    assert ev(w, 101)["status"] == aq.EV_QUALIFIED and ev(w, 102)["status"] == aq.EV_DUPLICATE_ACCOUNT
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 1


def test_duplicate_after_activation(monkeypatch):
    w = forward_world()
    add(w, 101, "shared-acct")
    go(w, monkeypatch)
    later = NOW + timedelta(minutes=1)
    add(w, 301, "shared-acct", status="pending", joined=later, issued=later, redeemed=later + timedelta(minutes=1))
    run(w, monkeypatch, now=NOW + timedelta(minutes=10))
    assert ev(w, 101)["status"] == aq.EV_QUALIFIED and ev(w, 301)["status"] == aq.EV_DUPLICATE_ACCOUNT


# --- migration ----------------------------------------------------------------

def test_forward_only_limitation_recorded_honestly():
    w = forward_world(historical=(998, 999, 997), linked={998: ["000Known"]}, seed_from_linkage=True)
    control = aq.get_control(w.db)
    marker = control["forward_only_marker"]
    assert control["history_policy"] == aq.HISTORY_FORWARD == "forward_only_from_activation_cutoff"
    assert (marker["version"], marker["unresolved_count"], marker["qualified_history_count"], marker["seeded_count"]) == (
        aq.FORWARD_MARKER_VERSION, 2, 3, 1)
    text = control["history_limitation"]
    assert "outside guaranteed duplicate coverage" in text and "NOT complete pre-cutoff lifetime" in text
    assert "coupon_new_player=1 remains mandatory" in text
    assert marker["activation_cutoff_kl"].endswith("+08:00") and marker["operator"] and marker["reason"]
    assert migration.completion_readiness(w.db, control)["ok"]
    assert admin.preflight_report(w.db, now_utc=NOW)["forward_only"]["unresolved_historical_identities"] == 2


@pytest.mark.parametrize("override,blocker", [
    ({"report_fingerprint": "0" * 64}, "forward_only_report_fingerprint_mismatch"),
    ({"unresolved_count": 5}, "forward_only_unresolved_count_mismatch"),
    ({"qualified_history_count": 5}, "forward_only_qualified_history_count_mismatch"),
    ({"seeded_count": 5}, "forward_only_seeded_count_mismatch"),
    ({"account_namespace": "other-brand"}, "forward_only_account_namespace_mismatch"),
    ({"reason": None}, "forward_only_missing_reason"),
    ({"operator": ""}, "forward_only_missing_operator"),
    ({"activation_cutoff": None}, "forward_only_missing_activation_cutoff"),
    ({"activation_cutoff": "not-a-date"}, "forward_only_activation_cutoff_invalid"),
])
def test_forward_apply_refuses_unreviewed_inputs_and_writes_nothing(override, blocker):
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "coverage-account", NOW)])
    inputs = {**forward_inputs(w.db), **override}
    before = _snapshot(w.db)
    out = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                        acknowledge_unknown_history=True, forward_only=inputs)
    assert blocker in out["apply_refused"] and not out["seed_marker_written"]
    assert _snapshot(w.db) == before


def test_apply_without_acknowledgement_or_inputs_is_refused():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    out = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                        acknowledge_unknown_history=False)
    assert "forward_only_requires_unknown_history_acknowledgement" in out["apply_refused"]
    assert "forward_only_missing_report_fingerprint" in out["apply_refused"]


def test_naive_cutoff_is_kuala_lumpur_wall_clock():
    assert fwd.kl_cutoff("2026-11-01T00:00:00") == fwd.kl_cutoff("2026-10-31T16:00:00+00:00")
    assert fwd.kl_cutoff("2026-11-01T00:00:00+08:00").isoformat() == "2026-10-31T16:00:00+00:00"


def test_historical_count_change_during_migration_is_refused(monkeypatch):
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "coverage-account", NOW)])
    inputs = forward_inputs(w.db)
    real, calls = migration.history_snapshot, []

    def drifting(db, *, as_of):
        calls.append(1)
        snap = real(db, as_of=as_of)
        if len(calls) > 1:
            snap["xp_events_created_before"] += 1
        return snap

    monkeypatch.setattr(migration, "history_snapshot", drifting)
    out = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                        acknowledge_unknown_history=True, forward_only=inputs)
    assert out["apply_refused"] == ["historical_counts_changed"] and not out["seed_marker_written"]
    assert not aq.get_control(w.db).get("seed_completed_at")


def test_interrupted_migration_retries_idempotently(monkeypatch):
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    for uid, acct in ((997, "000A"), (998, "000B"), (999, "000C")):
        w.db.qualified_events.insert_one({"invitee_id": uid, "referrer_id": 900, "qualified_at": CUTOFF})
        w.user(uid, linked=[acct])
    w.upload([w.row("coverage", "coverage-account", NOW)])
    inputs = forward_inputs(w.db, seed_from_linkage=True)
    kwargs = dict(apply=True, seed_from_linkage=True, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                  acknowledge_unknown_history=True, forward_only=inputs)
    real = migration.apply_seeds

    def crash_after_one(db, seeds, *, now_utc):
        real(db, seeds[:1], now_utc=now_utc)
        raise KeyboardInterrupt("process killed mid-seed")

    monkeypatch.setattr(migration, "apply_seeds", crash_after_one)
    with pytest.raises(KeyboardInterrupt):
        migration.run(w.db, **kwargs)
    control = aq.get_control(w.db)
    assert not control.get("seed_completed_at") and "forward_only_marker" not in control
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 1
    monkeypatch.setattr(migration, "apply_seeds", real)
    retry = migration.run(w.db, **kwargs)
    assert retry["seed_marker_written"] and retry["seed_result"] == {"seeded": 2, "already_seeded": 1, "held_by_other": 0}
    again = migration.run(w.db, **kwargs)
    assert again["seed_marker_written"] and again["seed_result"]["seeded"] == 0
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 3 and again["history_unchanged"]


def test_hard_killed_migration_lease_expires_then_retry_succeeds():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "coverage-account", NOW)])
    kwargs = dict(apply=True, seed_from_linkage=False, history_policy=aq.HISTORY_FORWARD,
                  acknowledge_unknown_history=True, forward_only=forward_inputs(w.db))
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"migration_lease_token": "dead",
                                                          "migration_lease_until": NOW + timedelta(minutes=30)}})
    blocked = migration.run(w.db, now_utc=NOW, **kwargs)
    assert blocked["apply_refused"] == ["migration_lease_unavailable_or_control_changed"]
    assert migration.run(w.db, now_utc=NOW + timedelta(minutes=31), **kwargs)["seed_marker_written"]


def test_cutoff_mismatch_refuses_activation_and_preflight(capsys):
    w = forward_world()
    wrong = NOW + timedelta(hours=1)
    before = deepcopy(aq.get_control(w.db))
    assert activate(w, cutoff=wrong) == 2
    assert aq.get_control(w.db) == before
    report = admin.preflight_report(w.db, now_utc=NOW, cutoff=wrong)
    assert "activation_cutoff_mismatch" in report["forward_only"]["blockers"] and not report["ok"]
    assert admin.preflight_report(w.db, now_utc=NOW, cutoff=NOW)["forward_only"]["ok"]


def test_historical_counts_unchanged_through_activation_and_processing(monkeypatch):
    w = forward_world(historical=(997, 998, 999))
    add(w, 101, "acct-101")
    add(w, 999, "acct-999-again", status="pending")
    before = {uid: historic_rows(w, uid) for uid in (997, 998, 999)}
    counts = migration.history_snapshot(w.db, as_of=NOW)
    go(w, monkeypatch)
    assert migration.history_snapshot(w.db, as_of=NOW) == counts  # qualified, settled, XP, award, ledger counts
    assert {uid: historic_rows(w, uid) for uid in (997, 998, 999)} == before
    assert w.db.qualified_events.count_documents({"rule_version": {"$ne": aq.RULE_VERSION}}) == 3


# --- legacy writer lease --------------------------------------------------------

def _set_token(w, **fields):
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"legacy_writer_token": "tok", **fields}})


def test_genuinely_active_writer_blocks_everything_and_is_never_recovered():
    w = forward_world()
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as lease:
        assert lease and aq.legacy_writer_state(aq.get_control(w.db), NOW)["state"] == aq.WRITER_ACTIVE
        later = NOW + aq.LEGACY_WRITER_LEASE - timedelta(seconds=1)
        assert not aq.recover_legacy_writer(w.db, now_utc=later)["recovered"]
        with aq.legacy_writer_guard(w.db, now_utc=later) as second:
            assert not second  # overlapping legacy writer refused
        assert activate(w) == 2 and admin.preflight_report(w.db, now_utc=NOW)["legacy_writer_in_flight"]
        assert lease.renew(NOW + timedelta(minutes=5))  # heartbeat extends the lease
        assert aq.legacy_writer_state(aq.get_control(w.db), NOW + timedelta(minutes=14))["state"] == aq.WRITER_ACTIVE


def test_stale_lease_recovers_only_after_expiry_plus_grace_and_old_writer_is_fenced():
    w = forward_world()
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as old:
        expired = NOW + aq.LEGACY_WRITER_LEASE + timedelta(seconds=1)
        assert aq.legacy_writer_state(aq.get_control(w.db), expired)["state"] == aq.WRITER_EXPIRED_GRACE
        assert not aq.recover_legacy_writer(w.db, now_utc=expired)["recovered"]
        recoverable = NOW + aq.LEGACY_WRITER_LEASE + aq.LEGACY_WRITER_RECOVERY_GRACE + timedelta(seconds=1)
        assert aq.legacy_writer_state(aq.get_control(w.db), recoverable)["state"] == aq.WRITER_EXPIRED_RECOVERABLE
        # The next legacy run self-heals it - and the suspended old writer can never award again.
        with aq.legacy_writer_guard(w.db, now_utc=recoverable) as new:
            assert new and new.token != old.token
            assert not old.renew(recoverable) and old.lost
            assert not aq.invalidate_history_seed(w.db, old)
            assert aq.get_control(w.db)["legacy_writer_recoveries"][0]["token"] == old.token
        # Releasing the stale handle's token must not clear the new owner's token either.
    assert "legacy_writer_token" not in aq.get_control(w.db)


def test_expired_lease_can_be_recovered_by_operator_command_then_activation_proceeds():
    w = forward_world()
    _set_token(w, legacy_writer_started_at=NOW - timedelta(hours=5),
               legacy_writer_lease_until=NOW - timedelta(hours=4))
    assert activate(w) == 2  # activation waits: the token is still present
    argv = ["recover-legacy-writer", "--token", "tok", "--commit"]
    assert admin.main(argv, db_factory=lambda: w.db, now_fn=lambda: NOW) == 0
    assert activate(w) == 0


def test_pre_lease_token_needs_age_and_operator_acknowledgement():
    w = forward_world()
    _set_token(w, legacy_writer_started_at=NOW - timedelta(minutes=10))
    info = aq.legacy_writer_state(aq.get_control(w.db), NOW)
    assert info["state"] == aq.WRITER_UNLEASED_RECENT and not info["recoverable"]
    assert not aq.recover_legacy_writer(w.db, now_utc=NOW, acknowledged_stopped=True)["recovered"]
    stale = NOW + aq.LEGACY_UNLEASED_MIN_AGE
    assert aq.legacy_writer_state(aq.get_control(w.db), stale)["state"] == aq.WRITER_UNLEASED_STALE
    assert aq.recover_legacy_writer(w.db, now_utc=stale)["reason"] == "requires_acknowledgement_writer_stopped"
    with aq.legacy_writer_guard(w.db, now_utc=stale) as lease:
        assert not lease  # never auto-recovered: liveness cannot be proven
    assert aq.recover_legacy_writer(w.db, now_utc=stale, acknowledged_stopped=True, operator="ops")["recovered"]


def test_scheduler_stops_claiming_rows_after_losing_the_lease(monkeypatch):
    import scheduler

    w = forward_world()
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"mode": aq.MODE_DISABLED}})
    w.join(900, 501, status="pending", at=NOW - timedelta(days=3))
    monkeypatch.setattr(scheduler, "db", w.db)
    monkeypatch.setattr(scheduler, "_referral_hold_hours", lambda: 0)

    class Lost:
        def renew(self, *_a):
            return False

    scheduler._settle_pending_referrals(10, lease=Lost())
    assert w.db.pending_referrals.find_one({"invitee_user_id": 501})["status"] == "pending"  # never claimed


# --- reporting -----------------------------------------------------------------

def test_status_preflight_and_databot_report_show_forward_only_facts(capsys, monkeypatch):
    w = forward_world(historical=(998, 999), linked={998: ["000Known"]}, seed_from_linkage=True)
    add(w, 101, "acct-101")
    expected = {"history_policy": aq.HISTORY_FORWARD, "historical_qualified_count": 2,
                "deterministic_historical_identities_seeded": 1, "unresolved_historical_identities": 1}
    status = admin.status_report(w.db, now_utc=NOW)
    pre = admin.preflight_report(w.db, now_utc=NOW, cutoff=NOW)
    databot = affiliate_v2_report.build_report(w.db, start_date="2026-10-01", end_date="2026-10-31", now_utc=NOW)
    for block in (status["forward_only"], pre["forward_only"], databot["forward_only"]):
        assert {k: block[k] for k in expected} == expected
        assert "outside guaranteed duplicate coverage" in block["limitation"]
        assert block["v2_mode"] == "preview" and block["pending_manual_cohort"]["ready"]
    assert status["forward_only"]["pending_manual_cohort"]["active_not_yet_snapshotted"] == 1
    assert "source_coverage" in pre["coverage"] or pre["coverage"]["applicable"] is False
    assert "legacy_writer" in pre
    go(w, monkeypatch)
    assert admin.status_report(w.db, now_utc=NOW)["forward_only"]["v2_mode"] == "actual"
    assert admin.status_report(w.db, now_utc=NOW)["forward_only"]["pending_manual_cohort"]["size"] == 1
    prev = preview.build_preview(_ReadOnlyDb(w.db), month=None, now_utc=NOW)
    assert prev["forward_only"]["history_policy"] == aq.HISTORY_FORWARD


# --- Codex review findings on 9ba0ed7 ---------------------------------------------

def test_fence_is_one_cas_on_exact_token_and_never_touches_the_seed_for_a_stale_writer():
    w = forward_world()
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as old:
        # recovery cleared the old token and a replacement writer owns the barrier
        w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"legacy_writer_token": "replacement",
                                                             "legacy_writer_lease_until": NOW + timedelta(hours=1)}})
        seeded_at = aq.get_control(w.db)["seed_completed_at"]
        assert aq.invalidate_history_seed(w.db, old, NOW) is False and old.lost
        assert aq.get_control(w.db)["seed_completed_at"] == seeded_at       # stale writer changed nothing
        w.db[aq.CONTROL_COLLECTION].update_one({}, {"$unset": {"legacy_writer_token": ""}})
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"legacy_writer_token": "replacement"}})  # not ours to release


@pytest.mark.parametrize("how", ["none", "admission_closed", "row_claim_cleared"])
def test_scheduler_award_aborts_when_activation_lands_after_the_fence(how, monkeypatch):
    import scheduler

    w = forward_world()
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$unset": {"seed_completed_at": ""}})
    w.user(601, created=NOW - timedelta(days=3))
    pid = w.db.pending_referrals.insert_one({
        "invitee_user_id": 601, "inviter_user_id": 900, "status": "pending", "destination_type": "official_channel",
        "created_at_utc": NOW - timedelta(days=3)}).inserted_id
    monkeypatch.setattr(scheduler, "db", w.db)
    monkeypatch.setattr(scheduler, "_referral_hold_hours", lambda: 0)
    monkeypatch.setattr(scheduler, "_get_official_channel_member_status", lambda *a, **k: "member")
    real = aq.invalidate_history_seed

    def fence_then_activation_lands(db, lease=None, now_utc=None):
        ok = real(db, lease, now_utc)  # fence passes, then the (recovered + activated) V2 rule takes over
        if how == "admission_closed":
            db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"legacy_admission_closed": True}})
        elif how == "row_claim_cleared":
            db.pending_referrals.update_one({"_id": pid}, {"$set": {"status": aq.PENDING_AWAITING_REDEMPTION},
                                                           "$unset": {"processing_by": ""}})
        return ok

    monkeypatch.setattr(aq, "invalidate_history_seed", fence_then_activation_lands)
    scheduler.settle_pending_referrals(batch_limit=5)
    awarded = w.db.referral_award_events.count_documents({"invitee_user_id": 601})
    # Control case proves the scenario really reaches the award; the aborts prove nothing is written.
    assert awarded == (1 if how == "none" else 0)
    if how != "none":
        assert w.db.xp_events.count_documents({"unique_key": "ref:601"}) == 0


def test_requeue_after_a_rule_rejection_keeps_rollout_membership_but_absence_rejection_stays_closed(monkeypatch):
    w = forward_world()
    add(w, 101, "acct-101", amount="79.99")
    add(w, 102, "acct-102", amount="79.99")
    go(w, monkeypatch)
    assert ev(w, 101)["reason"] == ev(w, 102)["reason"] == "seven_day_turnover_below_80"
    assert w.db.pending_referrals.find_one({"invitee_user_id": 101})["status"] == "rejected"
    # 102's rejection was absence-based (the one decision never reopened automatically)
    w.db.pending_referrals.update_one({"invitee_user_id": 102}, {"$set": {
        "qualification_reason": aq.ABSENCE_REJECTION_REASON}})
    for uid in (101, 102):  # corrected data is ingested, operator requeues
        e = ev(w, uid)
        w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": e["_id"]}, {"$set": {
            "qualification_fields.after_total_bet_amount": "80"}})
        assert aq.requeue_evidence(w.db, evidence_id=e["_id"], now_utc=NOW)
    run(w, monkeypatch)
    assert ev(w, 101)["status"] == aq.EV_QUALIFIED
    assert (ev(w, 102)["status"], ev(w, 102)["reason"]) == (aq.EV_REVIEW, "referral_not_active_at_rollout")


def test_migration_and_legacy_writers_are_mutually_exclusive_for_the_whole_interval():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "coverage-account", NOW)])
    kwargs = dict(apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                  acknowledge_unknown_history=True, forward_only=forward_inputs(w.db))
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as writer:       # a legacy writer is active
        assert writer
        out = migration.run(w.db, **kwargs)
        assert out["apply_refused"] == ["migration_lease_unavailable_or_control_changed"]
        assert not out["seed_marker_written"]
    # a live migration lease keeps legacy admission out; an expired one does not
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"migration_lease_token": "m",
                                                         "migration_lease_until": NOW + timedelta(minutes=20)}})
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as blocked:
        assert not blocked
    with aq.legacy_writer_guard(w.db, now_utc=NOW + timedelta(minutes=31)) as admitted:
        assert admitted
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$unset": {"migration_lease_token": "", "migration_lease_until": ""}})
    assert migration.run(w.db, **kwargs)["seed_marker_written"]


def test_activation_cutoff_is_bound_to_the_reviewed_fingerprint():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "coverage-account", NOW)])
    reviewed = forward_inputs(w.db, cutoff=NOW)
    other = {**reviewed, "activation_cutoff": (NOW + timedelta(days=30)).isoformat()}  # same counts and fingerprint
    before = _snapshot(w.db)
    out = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                        acknowledge_unknown_history=True, forward_only=other)
    assert out["apply_refused"] == ["forward_only_report_fingerprint_mismatch"] and _snapshot(w.db) == before
    assert forward_inputs(w.db, cutoff=NOW + timedelta(days=30))["report_fingerprint"] != reviewed["report_fingerprint"]
    ok = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
                       acknowledge_unknown_history=True, forward_only=reviewed)
    assert ok["seed_marker_written"] and migration.completion_readiness(w.db, aq.get_control(w.db))["ok"]
