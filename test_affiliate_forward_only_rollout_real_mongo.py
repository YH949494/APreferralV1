"""Run against disposable localhost MongoDB via QUALIFICATION_TEST_MONGO_URI."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import threading

import pytest

import affiliate_qualification as aq
import affiliate_qualification_preview as preview
from scripts import affiliate_qualification_admin as admin
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_qualification_rollout_real_mongo import real_db
from test_affiliate_forward_only_rollout import prepared_world, activate, forward_inputs
from test_affiliate_welcome_redemption_qualification import NOW, CUTOFF, _scheduler, run
from test_affiliate_qualification_gates import databot_world
from test_uim_redemption_sync import sync, urow


def test_real_activation_cas_loses_to_writer_admitted_after_preflight(real_db, monkeypatch):
    w = prepared_world(db=real_db)
    validated, admitted, release = threading.Event(), threading.Event(), threading.Event()
    original = admin.preflight_report
    def preflight(*args, **kwargs):
        report = original(*args, **kwargs)
        assert report["ok"]
        validated.set()
        assert admitted.wait(timeout=10)
        return report
    monkeypatch.setattr(admin, "preflight_report", preflight)
    def writer():
        assert validated.wait(timeout=10)
        with aq.legacy_writer_guard(real_db, now_utc=NOW) as allowed:
            assert allowed
            admitted.set()
            assert release.wait(timeout=10)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        try:
            assert activate(w) == 2
            assert aq.launch_cutoff(aq.get_control(real_db)) is None
        finally:
            release.set()
            future.result(timeout=10)
    monkeypatch.setattr(admin, "preflight_report", original)
    assert activate(w) == 0
    with aq.legacy_writer_guard(real_db, now_utc=NOW) as allowed:
        assert not allowed


def test_finished_legacy_writer_invalidates_stale_preflight(real_db, monkeypatch):
    w = prepared_world(db=real_db)
    original = admin.preflight_report
    def preflight(*args, **kwargs):
        report = original(*args, **kwargs)
        assert report["ok"]
        with aq.legacy_writer_guard(real_db, now_utc=NOW) as allowed:
            assert allowed
            aq.invalidate_history_seed(real_db)
            real_db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": NOW})
        return report
    monkeypatch.setattr(admin, "preflight_report", preflight)
    assert activate(w) == 2
    assert aq.launch_cutoff(aq.get_control(real_db)) is None
    assert real_db.qualified_events.count_documents({}) == 1


@pytest.mark.parametrize("amount,expected", [("80.00", aq.EV_QUALIFIED), ("79.99", aq.EV_REJECTED)])
def test_real_incomplete_correction_and_preview_live_parity(real_db, amount, expected, monkeypatch):
    w, feed = databot_world(db=real_db, fields={"after_total_bet_amount": None})
    run(w, monkeypatch)
    assert real_db[aq.EVIDENCE_COLLECTION].find_one()["status"] == aq.EV_REVIEW
    feed.add_batch([{**urow("GATE11", "000CaseID", "2026-10-05 10:00:00"), "after_total_bet_amount": amount}])
    sync(w, feed, now=NOW + timedelta(minutes=1))
    report = preview.build_preview(real_db, month=None, now_utc=NOW + timedelta(minutes=1))
    assert report["activation_cohort"]["pass"] == int(expected == aq.EV_QUALIFIED)
    run(w, monkeypatch, now=NOW + timedelta(minutes=1))
    run(w, monkeypatch, now=NOW + timedelta(minutes=2))
    assert real_db[aq.EVIDENCE_COLLECTION].find_one()["status"] == expected
    assert real_db.xp_events.count_documents({"type": "referral_award"}) == int(expected == aq.EV_QUALIFIED)
    assert real_db.referral_events.count_documents({"event": "referral_revoked"}) == 0


def test_real_forward_seed_manifest_detects_lost_registry_ownership(real_db):
    w = prepared_world(db=real_db)
    real_db.qualified_events.insert_one({"invitee_id": 999, "qualified_at": CUTOFF, "referrer_id": 900})
    w.user(999, linked=["000Known"])
    result = migration.run(real_db, apply=True, seed_from_linkage=True, now_utc=NOW,
                           history_policy=aq.HISTORY_FORWARD, acknowledge_unknown_history=True,
                           forward_only=forward_inputs(real_db, seed_from_linkage=True))
    assert result["seed_marker_written"]
    control = aq.get_control(real_db)
    assert migration.completion_readiness(real_db, control)["ok"]
    real_db[aq.REGISTRY_COLLECTION].delete_many({})  # disposable local fault injection
    assert "seeded_registry_ownership_changed" in migration.completion_readiness(real_db, control)["problems"]
    assert activate(w, "--acknowledge-unknown-history") == 2


def test_real_old_completed_sync_cannot_lose_explicit_incomplete_marker(real_db, monkeypatch):
    w, feed = databot_world(db=real_db, fields={"seven_day_complete": False})
    real_db.uim_redemption_rows.update_many({}, {"$unset": {
        "seven_day_complete": "", "seven_day_completeness_retained_version": ""}})
    assert real_db.uim_redemption_batches.find_one()["rows_complete"]
    sync(w, feed)
    run(w, monkeypatch)
    assert real_db[aq.EVIDENCE_COLLECTION].find_one()["status"] == aq.EV_REVIEW
    assert real_db.qualified_events.count_documents({}) == real_db.xp_events.count_documents({}) == 0
    assert preview.build_preview(real_db, month=None, now_utc=NOW)["activation_cohort"]["pass"] == 0


def test_real_mark_and_cancel_are_serialized_and_stale_mark_is_refused(real_db, monkeypatch):
    w = prepared_world(db=real_db)
    assert activate(w, cutoff=NOW + timedelta(days=1)) == 0
    w.join(900, 101, at=NOW, status="pending_channel")
    stale = aq.get_control(real_db)
    entered, release = threading.Event(), threading.Event()
    original = aq._mark_activation_cohort
    def delayed_mark(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=10)
        return original(*args, **kwargs)
    monkeypatch.setattr(aq, "_mark_activation_cohort", delayed_mark)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(aq.mark_activation_cohort, real_db, control=stale, now_utc=NOW)
        assert entered.wait(timeout=10)
        try:
            assert admin.main(["cancel-scheduled", "--commit"], db_factory=lambda: real_db, now_fn=lambda: NOW) == 2
        finally:
            release.set()
        assert future.result(timeout=10) == 1
    monkeypatch.setattr(aq, "_mark_activation_cohort", original)
    assert admin.main(["cancel-scheduled", "--commit"], db_factory=lambda: real_db, now_fn=lambda: NOW) == 0
    assert aq.mark_activation_cohort(real_db, control=stale, now_utc=NOW) == 0
    assert real_db.pending_referrals.find_one()["status"] == "pending_channel"
