"""Regression coverage for rollout bypasses found in the handoff audit."""
from datetime import timedelta

import pytest

import affiliate_qualification as aq
import affiliate_qualification_preview as preview
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_welcome_redemption_qualification import CUTOFF, NOW, SOURCE_CONFIG, World


def issuance_world(*, issued_at, legacy=False):
    world = World()
    world.join(900, 901, at=CUTOFF)
    if legacy:
        world.db.new_joiner_claims.insert_one({"uid": 901, "code": "AUDIT-WELCOME", "claimed_at": issued_at})
    else:
        world.welcome_code(901, "AUDIT-WELCOME")
        world.db.voucher_pools.update_one({"code": "AUDIT-WELCOME"}, {"$set": {"issued_at": issued_at}})
        world.db.affiliate_ledger.update_one({"dedup_key": "WELCOME:901"}, {"$set": {"issued_at": issued_at}})
    world.upload([world.row("AUDIT-WELCOME", "000901", NOW - timedelta(hours=2))])
    return world


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("issued_at, reason", [
    (NOW - timedelta(hours=1), "redemption_before_welcome_issuance"),
    (None, "missing_welcome_issued_at"),
])
def test_live_and_preview_do_not_credit_unproven_issuance_order(legacy, issued_at, reason):
    world = issuance_world(issued_at=issued_at, legacy=legacy)
    shadow = preview.build_preview(world.db, month="202610", now_utc=NOW)
    assert shadow["totals"]["at_launch_eligible"] == 0
    assert shadow["evidence_summary"]["review_reasons"][reason] == 1
    aq.extract_committed_batches(world.db, config=SOURCE_CONFIG, now_utc=NOW)
    aq.process_pending_evidence(world.db, now_utc=NOW)
    evidence = world.db[aq.EVIDENCE_COLLECTION].find_one({})
    assert (evidence["status"], evidence["reason"]) == (aq.EV_REVIEW, reason)
    assert world.db.qualified_events.count_documents({}) == 0
    assert world.db[aq.REGISTRY_COLLECTION].count_documents({}) == 0


@pytest.mark.parametrize("legacy", [False, True])
def test_valid_issuance_order_is_shared_by_live_and_preview(legacy):
    world = issuance_world(issued_at=NOW - timedelta(hours=3), legacy=legacy)
    shadow = preview.build_preview(world.db, month="202610", now_utc=NOW)
    assert shadow["totals"]["at_launch_eligible"] == 1
    aq.extract_committed_batches(world.db, config=SOURCE_CONFIG, now_utc=NOW)
    aq.process_pending_evidence(world.db, now_utc=NOW)
    event = world.db.qualified_events.find_one({})
    assert event["invitee_id"] == 901
    assert event["account_key"] == "advantplay:000901"


def test_migration_does_not_seed_redemption_before_own_issuance():
    world = issuance_world(issued_at=NOW - timedelta(hours=1))
    world.db.qualified_events.insert_one({"invitee_id": 901, "referrer_id": 900,
                                          "qualified_at": CUTOFF - timedelta(days=1)})
    plan = migration.build_plan(world.db, config=SOURCE_CONFIG, seed_from_linkage=False, now_utc=NOW)
    assert plan["accounts_to_seed"] == 0
    assert world.db.qualified_events.count_documents({}) == 1


@pytest.mark.parametrize("minutes_ahead, expected_seeds", [(5, 1), (6, 0)])
def test_migration_uses_live_future_time_tolerance(minutes_ahead, expected_seeds):
    world = World()
    world.welcome_code(901, "FUTURE-CODE", issued_at=CUTOFF)
    world.db.qualified_events.insert_one({"invitee_id": 901, "referrer_id": 900,
                                         "qualified_at": CUTOFF - timedelta(days=1)})
    world.upload([world.row("FUTURE-CODE", "000901", NOW + timedelta(minutes=minutes_ahead))])
    report = migration.run(world.db, apply=False, seed_from_linkage=False, now_utc=NOW)
    assert report["plan"]["accounts_to_seed"] == expected_seeds
    assert report["history_unchanged"] is True


@pytest.mark.parametrize("mode", [aq.MODE_ACTIVE, aq.MODE_PAUSED, aq.MODE_DISABLED])
def test_legacy_xp_backfill_refuses_after_cutoff_even_when_paused(mode, monkeypatch):
    import backfill_referrals
    world = World(mode=mode)
    world.db.referrals.insert_one({"referrer_user_id": 900, "invitee_user_id": 901, "status": "success"})
    awarded = []
    monkeypatch.setattr(backfill_referrals, "grant_referral_rewards", lambda *args: awarded.append(args))
    monkeypatch.setattr(backfill_referrals, "grant_xp", lambda *args: awarded.append(args))
    with pytest.raises(RuntimeError, match="legacy.*disabled"):
        backfill_referrals.backfill(world.db, dry_run=False)
    assert awarded == []


def test_legacy_xp_backfill_stops_if_rule_launches_during_pass(monkeypatch):
    import backfill_referrals
    world = World(mode=aq.MODE_DISABLED, cutoff=None)
    for invitee in (901, 902):
        world.db.referrals.insert_one({"referrer_user_id": 900, "invitee_user_id": invitee, "status": "success"})
    awarded = []

    def award(*args):
        awarded.append(args[-1])
        world.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID},
            {"$set": {"launch_cutoff_utc": CUTOFF, "mode": aq.MODE_ACTIVE}})

    monkeypatch.setattr(backfill_referrals, "grant_referral_rewards", award)
    with pytest.raises(RuntimeError, match="legacy.*disabled"):
        backfill_referrals.backfill(world.db, dry_run=False)
    assert awarded == [901]


def test_backfill_dry_run_remains_read_only_after_launch(monkeypatch):
    import backfill_referrals
    world = World(mode=aq.MODE_PAUSED)
    world.db.referrals.insert_one({"referrer_user_id": 900, "invitee_user_id": 901, "status": "success"})
    monkeypatch.setattr(backfill_referrals, "grant_referral_rewards", lambda *a: pytest.fail("dry-run award"))
    assert backfill_referrals.backfill(world.db)["missing_base"] == 1


def test_prelaunch_backfill_is_idempotent_with_existing_reward_keys():
    import backfill_referrals
    world = World(mode=aq.MODE_DISABLED, cutoff=None)
    world.db.users.insert_one({"user_id": 900, "total_referrals": 1})
    world.db.referrals.insert_one({"referrer_user_id": 900, "invitee_user_id": 901, "status": "success"})
    assert backfill_referrals.backfill(world.db, dry_run=False)["missing_base"] == 1
    assert backfill_referrals.backfill(world.db, dry_run=False)["missing_base"] == 0
    assert world.db.xp_events.count_documents({"unique_key": "ref_success:901"}) == 1


def test_matching_ledger_issuance_is_usable_but_generic_dates_are_not():
    world = issuance_world(issued_at=NOW - timedelta(hours=3))
    world.db.voucher_pools.update_one({"code": "AUDIT-WELCOME"}, {"$unset": {"issued_at": ""}})
    recipient = aq.resolve_welcome_recipient(world.db, "AUDIT-WELCOME")
    assert aq.welcome_issuance_problem(recipient, NOW - timedelta(hours=2)) is None
    world.db.affiliate_ledger.update_one({"dedup_key": "WELCOME:901"},
        {"$unset": {"issued_at": ""}, "$set": {"created_at": CUTOFF, "updated_at": CUTOFF}})
    recipient = aq.resolve_welcome_recipient(world.db, "AUDIT-WELCOME")
    assert aq.welcome_issuance_problem(recipient, NOW - timedelta(hours=2)) == "missing_welcome_issued_at"
