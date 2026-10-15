"""Run against disposable localhost MongoDB via QUALIFICATION_TEST_MONGO_URI."""
from datetime import timedelta

import affiliate_forward_only as fwd
import affiliate_qualification as aq
from scripts import affiliate_qualification_admin as admin
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_forward_only_history_policy import activate, add, ev, forward_world, historic_rows
from test_affiliate_qualification_rollout_real_mongo import real_db
from test_affiliate_welcome_redemption_qualification import NOW, _scheduler, run


def test_real_cohort_snapshot_activation_and_decisions(real_db, monkeypatch):
    w = forward_world(db=real_db, historical=(998, 999), redeemed={998: "000Known"})
    add(w, 101, "acct-101")
    add(w, 102, "000Known")
    add(w, 103, "acct-103", amount="79.99")
    add(w, 104, "acct-104", upload=False)
    add(w, 999, "acct-999-new", status="pending")
    before = historic_rows(w, 999)
    first = fwd.snapshot_launch_cohort(real_db, now_utc=NOW)
    again = fwd.snapshot_launch_cohort(real_db, now_utc=NOW)
    assert first["invitees"] == again["invitees"] == 4 and again["newly_snapshotted"] == 0
    assert activate(w) == 0
    run(w, monkeypatch)
    assert ev(w, 101)["status"] == aq.EV_QUALIFIED
    assert ev(w, 102)["status"] == aq.EV_DUPLICATE_ACCOUNT
    assert ev(w, 103)["reason"] == "seven_day_turnover_below_80"
    assert ev(w, 104) is None
    assert real_db.pending_referrals.find_one({"invitee_user_id": 104})["status"] == aq.PENDING_AWAITING_REDEMPTION
    assert historic_rows(w, 999) == before
    assert real_db[aq.COHORT_COLLECTION].count_documents({}) == 4


def test_real_lease_cas_recovery_and_fence(real_db):
    w = forward_world(db=real_db)
    with aq.legacy_writer_guard(real_db, now_utc=NOW) as old:
        assert old.renew(NOW + timedelta(minutes=5))
        assert not aq.recover_legacy_writer(real_db, now_utc=NOW + timedelta(minutes=14))["recovered"]
        later = NOW + aq.LEGACY_WRITER_LEASE + aq.LEGACY_WRITER_RECOVERY_GRACE + timedelta(minutes=6)
        with aq.legacy_writer_guard(real_db, now_utc=later) as new:
            assert new and not old.renew(later)
    assert "legacy_writer_token" not in aq.get_control(real_db)
    assert admin.main(["snapshot-launch-cohort", "--commit"], db_factory=lambda: real_db, now_fn=lambda: NOW) == 0


def test_real_interrupted_migration_marker_and_retry(real_db):
    from test_affiliate_forward_only_rollout import forward_inputs
    from test_affiliate_welcome_redemption_qualification import World, CUTOFF
    w = World(mode=aq.MODE_DISABLED, cutoff=None, db=real_db)
    real_db.qualified_events.insert_one({"invitee_id": 999, "referrer_id": 900, "qualified_at": CUTOFF})
    w.upload([w.row("coverage", "coverage-account", NOW)])
    kw = dict(apply=True, seed_from_linkage=False, now_utc=NOW, history_policy=aq.HISTORY_FORWARD,
              acknowledge_unknown_history=True, forward_only=forward_inputs(real_db))
    assert migration.run(real_db, **kw)["seed_marker_written"]
    again = migration.run(real_db, **kw)
    assert again["seed_marker_written"] and again["history_unchanged"]
    assert migration.completion_readiness(real_db, aq.get_control(real_db))["ok"]
