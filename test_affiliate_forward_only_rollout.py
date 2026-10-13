"""Forward-only v2 rollout: synthetic databases only, no external writes."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

import affiliate_qualification as aq
import affiliate_qualification_preview as preview
import scheduler
from scripts import affiliate_qualification_admin as admin
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_welcome_redemption_qualification import (
    CUTOFF, NOW, World, SOURCE_CONFIG, _ReadOnlyDb, _scheduler, _snapshot, run,
)
from test_affiliate_qualification_gates import databot_world
from test_uim_redemption_sync import DATABOT_CONFIG, sync, urow


@pytest.mark.parametrize("amount,status", [
    ("79.99", aq.EV_REJECTED), ("80", aq.ELIGIBLE), ("80.00", aq.ELIGIBLE),
    (Decimal("79.999999999999999999999999999999"), aq.EV_REJECTED),
    (Decimal("80.000000000000000000000000000001"), aq.ELIGIBLE),
    ("NaN", aq.EV_REVIEW), (None, aq.EV_REVIEW), ("bad", aq.EV_REVIEW),
])
def test_exact_80_boundary_and_unknown(amount, status):
    assert aq.assess_qualification_fields({"coupon_new_player": "1", "after_total_bet_amount": amount})["status"] == status


@pytest.mark.parametrize("flag", [True, "true", "1", 1])
def test_supported_true_and_one(flag):
    assert aq.assess_qualification_fields({"coupon_new_player": flag, "after_total_bet_amount": "80"})["status"] == aq.ELIGIBLE


def test_explicit_incomplete_never_rejects_a_partial_turnover():
    assert aq.assess_qualification_fields({"coupon_new_player": "1", "after_total_bet_amount": "1",
                                           "seven_day_complete": False}) == {
        "status": aq.EV_REVIEW, "reason": "seven_day_data_incomplete"}


def prepared_world(*, forward=False, db=None):
    w = World(mode=aq.MODE_DISABLED, cutoff=None, db=db)
    # Unknown history must never be retroactively changed or guessed.
    if forward:
        w.db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage-only", "coverage-account", NOW)])
    result = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW,
                           history_policy=aq.HISTORY_FORWARD if forward else aq.HISTORY_STRICT,
                           acknowledge_unknown_history=forward)
    assert result["seed_marker_written"], result
    return w


def activate(w, *extra, commit=True, cutoff=NOW):
    return admin.main(["activate", "--cutoff", cutoff.isoformat(), *extra, *(["--commit"] if commit else [])],
                      db_factory=lambda: w.db, now_fn=lambda: NOW)


def test_forward_policy_dry_run_and_apply_require_explicit_acknowledgement():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db.qualified_events.insert_one({"invitee_id": 999, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "not-seeded", NOW)])
    before = _snapshot(w.db)
    refused = migration.run(w.db, apply=True, seed_from_linkage=False, now_utc=NOW,
                            history_policy=aq.HISTORY_FORWARD)
    assert "forward_only_requires_unknown_history_acknowledgement" in refused["apply_refused"]
    assert _snapshot(w.db) == before
    with pytest.raises(SystemExit) as exc:
        migration.main(["--history-policy", aq.HISTORY_FORWARD], read_only_db_factory=lambda: w.db)
    assert exc.value.code == 2
    strict = migration.build_plan(w.db, config=SOURCE_CONFIG, seed_from_linkage=False, now_utc=NOW)
    assert strict["history_policy"] == aq.HISTORY_STRICT
    assert "unresolved_historical_identities" in strict["apply_blockers"]


def test_forward_seeds_only_known_accounts_and_preserves_unknown_history():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    for uid in (998, 999):
        w.db.qualified_events.insert_one({"invitee_id": uid, "referrer_id": 900, "qualified_at": CUTOFF})
    w.user(998, linked=["000Known"])
    w.upload([w.row("coverage", "coverage-only", NOW)])
    before = list(w.db.qualified_events.find())
    first = migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW,
                         history_policy=aq.HISTORY_FORWARD, acknowledge_unknown_history=True)
    assert first["seed_marker_written"] and first["plan"]["unresolved_events"] == 1
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 1
    assert w.db[aq.REGISTRY_COLLECTION].find_one()["account_key"] == "advantplay:000Known"
    assert list(w.db.qualified_events.find()) == before
    control = aq.get_control(w.db)
    assert control["history_limitation"] == aq.HISTORY_LIMITATION
    assert migration.completion_readiness(w.db, control)["ok"]
    second = migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW,
                          history_policy=aq.HISTORY_FORWARD, acknowledge_unknown_history=True)
    assert second["seed_result"]["seeded"] == 0 and second["seed_marker_written"]
    assert list(w.db.qualified_events.find()) == before


def test_activation_includes_all_pending_manual_dates_and_leaves_qualified_history(monkeypatch):
    w = prepared_world(forward=True)
    for uid, status in enumerate(aq.ACTIVE_PENDING_STATUSES, start=100):
        w.join(900, uid, at=CUTOFF - timedelta(days=20), status=status)
    w.join(900, 999, at=CUTOFF - timedelta(days=21), status="awarded")
    history = list(w.db.qualified_events.find())
    assert activate(w) == 2  # forward mode requires another explicit activation acknowledgement
    before = _snapshot(w.db)
    assert activate(w, "--acknowledge-unknown-history", commit=False) == 0
    assert _snapshot(w.db) == before
    assert activate(w, "--acknowledge-unknown-history") == 0
    assert w.db.pending_referrals.count_documents({"status": aq.PENDING_AWAITING_REDEMPTION}) == len(aq.ACTIVE_PENDING_STATUSES)
    assert w.db.pending_referrals.find_one({"invitee_user_id": 999})["status"] == "awarded"
    assert list(w.db.qualified_events.find()) == history
    assert aq.mark_activation_cohort(w.db, control=aq.get_control(w.db), now_utc=NOW) == 0
    monkeypatch.setattr(scheduler, "db", w.db)
    monkeypatch.setattr(scheduler, "_get_official_channel_member_status", lambda *a: pytest.fail("legacy ran"))
    scheduler.settle_pending_referrals()
    assert w.db.xp_events.count_documents({}) == w.db.referral_events.count_documents({}) == 0


def test_atomic_barrier_blocks_activation_and_new_legacy_admission():
    w = prepared_world()
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as admitted:
        assert admitted
        before = aq.get_control(w.db)
        assert activate(w) == 2
        assert aq.get_control(w.db) == before
    assert activate(w) == 0
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as admitted:
        assert not admitted
    assert not aq.legacy_award_allowed(aq.get_control(w.db), NOW)


def test_scheduled_activation_closes_admission_early_for_race_safety():
    w = prepared_world()
    assert activate(w, cutoff=NOW + timedelta(days=1)) == 0
    assert not aq.new_rule_active(aq.get_control(w.db), NOW)
    assert not aq.legacy_award_allowed(aq.get_control(w.db), NOW)
    with aq.legacy_writer_guard(w.db, now_utc=NOW) as admitted:
        assert not admitted


def test_crash_between_cutoff_and_mark_is_repaired_without_double_award(monkeypatch):
    w = prepared_world()
    w.join(900, 101, at=CUTOFF - timedelta(days=1), status="manual")
    w.welcome_code(101, "OLD-PENDING", issued_at=CUTOFF)
    w.upload([w.row("OLD-PENDING", "000101", NOW - timedelta(days=1))])
    original = aq.mark_activation_cohort
    monkeypatch.setattr(aq, "mark_activation_cohort", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(RuntimeError, match="crash"):
        activate(w)
    assert aq.launch_cutoff(aq.get_control(w.db)) == NOW
    assert not aq.legacy_award_allowed(aq.get_control(w.db), NOW)
    monkeypatch.setattr(aq, "mark_activation_cohort", original)
    run(w, monkeypatch)
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({"invitee_id": 101}) == 1
    assert aq._aware_utc(w.db.qualified_events.find_one()["qualified_at"]) == NOW
    assert w.db.xp_events.count_documents({"type": "referral_award"}) == 1
    assert w.db.referral_events.count_documents({"event": "referral_settled"}) == 1


@pytest.mark.parametrize("change", [
    {"mode": aq.MODE_ACTIVE}, {"mode": aq.MODE_PAUSED}, {"launch_cutoff_utc": CUTOFF},
    {"legacy_writer_token": "running"},
])
def test_rule_transition_refuses_unsafe_control(change):
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"rule_version": aq.LEGACY_RULE_VERSION, **change}})
    before = _snapshot(w.db)
    assert admin.main(["upgrade-rule", "--commit"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 2
    assert _snapshot(w.db) == before


def test_rule_transition_preserves_source_and_requires_commit_and_zero_v1_events():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$unset": {"rule_version": ""}})
    source = deepcopy(aq.get_control(w.db)["source_config"])
    before = _snapshot(w.db)
    assert admin.main(["upgrade-rule"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 0
    assert _snapshot(w.db) == before
    qid = w.db.qualified_events.insert_one({"invitee_id": 999, "rule_version": aq.LEGACY_RULE_VERSION}).inserted_id
    assert admin.main(["upgrade-rule", "--commit"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 2
    w.db.qualified_events.delete_one({"_id": qid})  # synthetic fixture only
    assert admin.main(["upgrade-rule", "--commit"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 0
    control = aq.get_control(w.db)
    assert control["_id"] == aq.LEGACY_RULE_VERSION
    assert control["source_config"] == source
    assert control["rule_revision_transitions"][0]["to"] == aq.RULE_VERSION
    assert admin.main(["upgrade-rule", "--commit"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 0


def test_v1_fingerprint_evidence_and_extraction_do_not_supply_v2_verdict(monkeypatch):
    w = World()
    w.join(1, 11, at=CUTOFF)
    w.welcome_code(11, "VERSION-ISOLATION", issued_at=CUTOFF)
    batch = w.upload([{**w.row("VERSION-ISOLATION", "00011", NOW - timedelta(days=1)),
                       "after_total_bet_amount": "10"}])
    obs = aq.parse_source_row(w.db.marketing_raw_data.find_one(), SOURCE_CONFIG)
    recipient = aq.resolve_welcome_recipient(w.db, obs["code"])
    with monkeypatch.context() as old:
        old.setattr(aq, "RULE_VERSION", aq.LEGACY_RULE_VERSION)
        old.setattr(aq, "QUALIFICATION_GATE_VERSION", "coupon_new_player_turnover_v1")
        old_doc = aq.build_evidence_doc(source=aq.SOURCE_MARKETING, recipient=recipient, now_utc=NOW, **obs)
    old_doc["status"] = aq.EV_QUALIFIED
    w.db[aq.EVIDENCE_COLLECTION].insert_one(old_doc)
    w.db.marketing_upload_batches.update_one({"upload_batch_id": batch}, {"$set": {
        "welcome_evidence_extracted_at": NOW, "welcome_evidence_cursor": "old-cursor"}})
    assert aq.assess_evidence(old_doc, control=aq.get_control(w.db), now_utc=NOW, lookup=aq.DbLookup(w.db))["reason"] == "evidence_rule_version_mismatch"
    run(w, monkeypatch)
    new = w.db[aq.EVIDENCE_COLLECTION].find_one({"rule_version": aq.RULE_VERSION})
    assert new["status"] == aq.EV_REJECTED
    assert new["source_ref"] != old_doc["source_ref"]
    assert new["observation_fingerprint"] != old_doc["observation_fingerprint"]
    assert w.db[aq.EVIDENCE_COLLECTION].find_one({"_id": old_doc["_id"]})["status"] == aq.EV_QUALIFIED
    assert w.db.marketing_upload_batches.find_one()["welcome_evidence_cursor"] == "old-cursor"
    assert w.db.qualified_events.count_documents({}) == 0


@pytest.mark.parametrize("amount,expected", [("80.00", aq.EV_QUALIFIED), ("79.99", aq.EV_REJECTED)])
def test_incomplete_evidence_retries_after_later_authoritative_arrival(amount, expected, monkeypatch):
    w, feed = databot_world(fields={"after_total_bet_amount": None})
    run(w, monkeypatch)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["status"] == aq.EV_REVIEW
    feed.add_batch([{**urow("GATE11", "000CaseID", "2026-10-05 10:00:00"), "after_total_bet_amount": amount}])
    sync(w, feed, now=NOW + timedelta(minutes=1))
    shadow = preview.build_preview(w.db, month=None, now_utc=NOW + timedelta(minutes=1))
    assert shadow["activation_cohort"]["pass"] == int(expected == aq.EV_QUALIFIED)
    run(w, monkeypatch, now=NOW + timedelta(minutes=1))
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["status"] == expected
    assert w.db.qualified_events.count_documents({}) == int(expected == aq.EV_QUALIFIED)
    run(w, monkeypatch, now=NOW + timedelta(minutes=2))
    assert w.db.referral_events.count_documents({"event": "referral_settled"}) == int(expected == aq.EV_QUALIFIED)
    assert w.db.referral_events.count_documents({"event": "referral_revoked"}) == 0


def test_temporary_feed_failure_holds_without_any_failed_or_earned_side_effect(monkeypatch):
    w, feed = databot_world()
    feed.fail_after = 0
    sync(w, feed, now=NOW + timedelta(seconds=1))
    result = run(w, monkeypatch, now=NOW + timedelta(seconds=1))
    assert result["evidence"]["skipped"] == "source_evidence_incomplete"
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["status"] == aq.EV_RECEIVED
    for name in (aq.REGISTRY_COLLECTION, "qualified_events", "xp_events", "referral_events", "referral_award_events"):
        assert w.db[name].count_documents({}) == 0
    feed.fail_after = None
    sync(w, feed, now=NOW + timedelta(seconds=2))
    run(w, monkeypatch, now=NOW + timedelta(seconds=2))
    assert w.db.qualified_events.count_documents({}) == 1


def test_coverage_and_missing_source_row_hold_instead_of_rejecting(monkeypatch):
    w, _ = databot_world(fields={"after_total_bet_amount": "1"})
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one()
    control = {**aq.get_control(w.db), "source_coverage_end_utc": CUTOFF}
    assert aq.assess_evidence(ev, control=control, now_utc=NOW, lookup=aq.DbLookup(w.db))["reason"] == "outside_source_coverage"
    w.db.uim_redemption_rows.delete_many({})  # synthetic source disappearance
    result = aq.evaluate_evidence(w.db, ev, control=aq.get_control(w.db), now_utc=NOW)
    assert result == {"status": aq.EV_REVIEW, "reason": "source_row_unavailable"}
    assert w.db.referral_events.count_documents({}) == w.db.qualified_events.count_documents({}) == 0


def test_activation_preview_includes_pre_cutoff_missing_evidence_and_preserves_reads():
    w = prepared_world()
    for uid, amount in ((101, "80"), (102, "79.99"), (103, None)):
        w.join(900, uid, at=CUTOFF - timedelta(days=40), status="manual")
        w.welcome_code(uid, f"PREVIEW-{uid}", issued_at=CUTOFF)
        w.upload([{**w.row(f"PREVIEW-{uid}", f"000{uid}", NOW - timedelta(days=1)),
                   "after_total_bet_amount": amount}])
    w.join(900, 104, at=NOW, status="pending")  # source has no row yet
    before = _snapshot(w.db)
    result = preview.build_preview(_ReadOnlyDb(w.db), month=None, now_utc=NOW)
    assert _snapshot(w.db) == before
    cohort = result["activation_cohort"]
    assert (cohort["population"], cohort["pass"], cohort["reject"], cohort["pending_review"]) == (4, 1, 1, 2)


def test_duplicate_account_reservation_race_and_restart_are_exactly_once(monkeypatch):
    w = World()
    for uid in (101, 102):
        w.join(900, uid, at=CUTOFF, status="manual")
        w.welcome_code(uid, f"RACE-{uid}", issued_at=CUTOFF)
        w.upload([w.row(f"RACE-{uid}", "000SHARED", NOW - timedelta(days=1))])
    aq.extract_committed_batches(w.db, config=SOURCE_CONFIG, now_utc=NOW)
    barrier = threading.Barrier(2)
    def evaluate(ev):
        barrier.wait(timeout=5)
        return aq.evaluate_evidence(w.db, ev, control=aq.get_control(w.db), now_utc=NOW)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(evaluate, w.db[aq.EVIDENCE_COLLECTION].find()))
    assert sorted(r["status"] for r in results) == sorted([aq.EV_QUALIFIED, aq.EV_DUPLICATE_ACCOUNT])
    run(w, monkeypatch)
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({}) == w.db.xp_events.count_documents({"type": "referral_award"}) == 1
    assert w.db.referral_events.count_documents({"event": "referral_revoked"}) == 0


def test_crashed_legacy_writer_requires_exact_stopped_writer_acknowledgement():
    w = prepared_world()
    w.db[aq.CONTROL_COLLECTION].update_one({}, {"$set": {"legacy_writer_token": "crashed"}})
    assert activate(w) == 2
    base = ["release-legacy-writer", "--token", "crashed", "--commit"]
    assert admin.main(base, db_factory=lambda: w.db, now_fn=lambda: NOW) == 2
    assert admin.main([*base, "--acknowledge-writer-stopped"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 0
    assert activate(w) == 0


@pytest.mark.parametrize("status", [None, "", "pending", "unknown"])
def test_unknown_redemption_status_never_becomes_confirmed_failure(status, monkeypatch):
    w = World()
    w.join(900, 101, at=CUTOFF)
    w.welcome_code(101, "STATUS-UNKNOWN", issued_at=CUTOFF)
    w.upload([w.row("STATUS-UNKNOWN", "000101", NOW - timedelta(days=1), status=status)])
    run(w, monkeypatch)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["reason"] == "redemption_status_unknown"
    assert w.db.xp_events.count_documents({}) == w.db.referral_events.count_documents({}) == 0


def test_old_terminal_referral_is_excluded_without_changing_history(monkeypatch):
    w = World()
    w.join(900, 101, at=CUTOFF - timedelta(days=10), status="revoked", revoked_reason="insufficient_engagement")
    w.welcome_code(101, "OLD-TERMINAL", issued_at=CUTOFF)
    w.upload([w.row("OLD-TERMINAL", "000101", NOW - timedelta(days=1))])
    run(w, monkeypatch)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["reason"] == "referral_not_active_at_rollout"
    assert w.db.pending_referrals.find_one()["status"] == "revoked"
    assert w.db.qualified_events.count_documents({}) == 0


def test_cancel_before_future_cutoff_restores_prior_pending_statuses():
    w = prepared_world()
    for uid, status in enumerate(aq.ACTIVE_PENDING_STATUSES, start=100):
        w.join(900, uid, at=CUTOFF, status=status)
    assert activate(w, cutoff=NOW + timedelta(days=1)) == 0
    assert admin.main(["cancel-scheduled", "--commit"], db_factory=lambda: w.db, now_fn=lambda: NOW) == 0
    assert aq.legacy_award_allowed(aq.get_control(w.db), NOW)
    assert {r["status"] for r in w.db.pending_referrals.find()} == set(aq.ACTIVE_PENDING_STATUSES)


def test_explicit_immature_source_report_retries_when_completed(monkeypatch):
    w, feed = databot_world(fields={"after_total_bet_amount": "1", "seven_day_complete": False})
    run(w, monkeypatch)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["reason"] == "seven_day_data_incomplete"
    assert w.db.pending_referrals.find_one()["status"] == aq.PENDING_AWAITING_REDEMPTION
    feed.add_batch([{**urow("GATE11", "000CaseID", "2026-10-05 10:00:00"),
                     "after_total_bet_amount": "80", "seven_day_complete": True}])
    sync(w, feed, now=NOW + timedelta(minutes=1))
    run(w, monkeypatch, now=NOW + timedelta(minutes=1))
    assert w.db.qualified_events.count_documents({}) == 1


def test_unavailable_preview_and_preflight_report_pending_population_and_coverage():
    w, feed = databot_world()
    w.join(900, 101, at=NOW, status="manual")
    maturity = admin.preflight_report(w.db, now_utc=NOW)["pending_maturity"]
    assert maturity["source_coverage_end_gmt8"] == "2026-10-05T10:00:00+08:00"
    assert maturity["joined_after_available_coverage"] == 1
    feed.fail_after = 0
    sync(w, feed, now=NOW + timedelta(hours=1))
    result = preview.build_preview(w.db, month=None, now_utc=NOW + timedelta(hours=1))
    assert result["activation_cohort"]["pending_review"] == 2
    assert not result["activation_cohort"]["source_ready"]


def test_v1_event_arriving_after_transition_blocks_activation_without_rewriting_it():
    w = prepared_world()
    w.db.qualified_events.insert_one({"invitee_id": 999, "rule_version": aq.LEGACY_RULE_VERSION,
                                      "qualified_at": CUTOFF, "referrer_id": 900})
    before = _snapshot(w.db)
    assert admin.preflight_report(w.db, now_utc=NOW)["v1_qualified_events"] == 1
    assert activate(w) == 2
    assert _snapshot(w.db) == before


def test_activation_refuses_even_slightly_backdated_cutoff():
    w = prepared_world()
    before = _snapshot(w.db)
    assert activate(w, cutoff=NOW - timedelta(seconds=1)) == 2
    assert _snapshot(w.db) == before


def test_later_exact_account_identity_is_not_conflicted_by_prior_absence(monkeypatch):
    w, feed = databot_world(fields={"account": None})
    run(w, monkeypatch)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()["reason"] == "missing_account"
    feed.add_batch([urow("GATE11", "000CaseID", "2026-10-05 10:00:00")])
    sync(w, feed, now=NOW + timedelta(minutes=1))
    shadow = preview.build_preview(w.db, month=None, now_utc=NOW + timedelta(minutes=1))
    assert shadow["activation_cohort"]["pass"] == 1
    run(w, monkeypatch, now=NOW + timedelta(minutes=1))
    assert w.db.qualified_events.count_documents({}) == 1
    assert w.db.qualified_events.find_one()["account_key"] == "uim_global:000CaseID"
