"""Prospective gates, transport compatibility and no-side-effect regressions."""
from collections import Counter
from datetime import timedelta
from decimal import Decimal

import pytest

import affiliate_qualification as aq
import affiliate_qualification_preview as preview
import uim_redemption_sync as syncer
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_welcome_redemption_qualification import (
    CUTOFF, NOW, World, _ReadOnlyDb, _scheduler, _snapshot, run,
)
from test_uim_redemption_sync import DATABOT_CONFIG, FakeFeed, disabled_world, sync, urow


@pytest.mark.parametrize('flag,amount,status,reason', [
    ('1', '10', aq.ELIGIBLE, None), (1, 10, aq.ELIGIBLE, None),
    ('1', '9.99', aq.EV_REJECTED, 'seven_day_turnover_does_not_exceed_9_99'),
    ('1', '9.98', aq.EV_REJECTED, 'seven_day_turnover_does_not_exceed_9_99'),
    ('0', '100000', aq.EV_REJECTED, 'not_a_new_coupon_player'),
    (0, '100000', aq.EV_REJECTED, 'not_a_new_coupon_player'),
    ('1', '9.9900000000000000000000001', aq.ELIGIBLE, None),
    ('1', '9.9899999999999999999999999', aq.EV_REJECTED, 'seven_day_turnover_does_not_exceed_9_99'),
    ('1', Decimal('1E1'), aq.ELIGIBLE, None),
    *[(flag, '10', aq.EV_REVIEW, 'missing_or_invalid_qualification_fields')
      for flag in (None, '', 'yes', 'true', True, False, '1.0', 2, {}, [])],
    *[('1', amount, aq.EV_REVIEW, 'missing_or_invalid_qualification_fields')
      for amount in (None, '', True, False, 'NaN', 'Infinity', '-1', float('nan'),
                     float('inf'), '1_000', '$10', '1,000', {}, [])],
])
def test_decimal_and_flag_contract(flag, amount, status, reason):
    assert aq.assess_qualification_fields({'coupon_new_player': flag, 'after_total_bet_amount': amount}) == {
        'status': status, 'reason': reason}


def databot_world(*, fields=None, db=None):
    w = World(db=db) if db is not None else disabled_world()
    w.db[aq.CONTROL_COLLECTION].update_one({'_id': aq.CONTROL_ID}, {'$set': {
        'source_config': dict(DATABOT_CONFIG), 'mode': aq.MODE_ACTIVE, 'launch_cutoff_utc': CUTOFF,
        'seed_completed_at': CUTOFF, 'seeded_identity_config': aq.identity_config(DATABOT_CONFIG),
    }})
    w.join(1, 11, at=CUTOFF)
    w.welcome_code(11, 'GATE11', issued_at=CUTOFF)
    w.user(11, linked=['000CaseID'], created=CUTOFF)
    feed = FakeFeed()
    row = urow('GATE11', '000CaseID', '2026-10-05 10:00:00')
    if fields is not None:
        row.update(fields)
    feed.add_batch([row])
    sync(w, feed)
    return w, feed


@pytest.mark.parametrize('fields,status,reason', [
    ({'coupon_new_player': '1', 'after_total_bet_amount': '10'}, aq.EV_QUALIFIED, None),
    ({'after_total_bet_amount': '9.99'}, aq.EV_REJECTED, 'seven_day_turnover_does_not_exceed_9_99'),
    ({'after_total_bet_amount': '9.98'}, aq.EV_REJECTED, 'seven_day_turnover_does_not_exceed_9_99'),
    ({'coupon_new_player': '0', 'after_total_bet_amount': '100'}, aq.EV_REJECTED, 'not_a_new_coupon_player'),
    ({'coupon_new_player': None, 'is_new_player': 1}, aq.EV_REVIEW, 'missing_or_invalid_qualification_fields'),
    ({'after_total_bet_amount': None}, aq.EV_REVIEW, 'missing_or_invalid_qualification_fields'),
    ({'after_total_bet_amount': 'bad'}, aq.EV_REVIEW, 'missing_or_invalid_qualification_fields'),
])
def test_live_preview_parity_and_no_failed_effects(fields, status, reason, monkeypatch, _scheduler):
    w, feed = databot_world(fields=fields)
    before = _snapshot(w.db)
    out = preview.build_preview(_ReadOnlyDb(w.db), month='202610', now_utc=NOW)
    assert _snapshot(w.db) == before
    assert out['totals']['at_launch_eligible'] == int(status == aq.EV_QUALIFIED)
    run(w, monkeypatch)
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one()
    assert (ev['status'], ev.get('reason')) == (status, reason)
    assert w.db.qualified_events.count_documents({}) == int(status == aq.EV_QUALIFIED)
    if status != aq.EV_QUALIFIED:
        for coll in (aq.REGISTRY_COLLECTION, 'xp_events', 'referral_events', 'referral_award_events'):
            assert w.db[coll].count_documents({}) == 0
        assert w.db.affiliate_ledger.count_documents({'ledger_type': {'$ne': 'WELCOME'}}) == 0
        assert all(not calls for calls in _scheduler.values())
    else:
        q = w.db.qualified_events.find_one()
        assert q['qualification_gate_version'] == aq.QUALIFICATION_GATE_VERSION
        assert q['account_key'] == 'uim_global:000CaseID'
        # No seven-day timer: NOW can be only a minute after the redemption.
    again = run(w, monkeypatch)
    assert again['evidence']['processed'] == 0


def test_newest_authoritative_report_replaces_stale_pass_and_replay_cannot_restore_it():
    w, feed = databot_world()
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    feed.add_batch([dict(urow('GATE11', '000CaseID', '2026-10-05 10:00:00'), after_total_bet_amount='9.98')])
    sync(w, feed)
    out = preview.build_preview(w.db, month='202610', now_utc=NOW)
    assert out['totals']['at_launch_eligible'] == 0
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one()
    assert ev['qualification_fields']['after_total_bet_amount'] == '9.98'
    first = feed.batches[0]['batch_id']
    aq.extract_marketing_batch(w.db, upload_batch_id=first, config=DATABOT_CONFIG, now_utc=NOW)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()['qualification_fields']['after_total_bet_amount'] == '9.98'
    aq.process_pending_evidence(w.db, now_utc=NOW)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()['reason'] == 'seven_day_turnover_does_not_exceed_9_99'
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 0


def test_repeated_reports_are_neither_summed_nor_maximized():
    w, feed = databot_world(fields={'after_total_bet_amount': '5'})
    for _ in range(3):
        feed.add_batch([dict(urow('GATE11', '000CaseID', '2026-10-05 10:00:00'), after_total_bet_amount='5')])
    sync(w, feed)
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    aq.process_pending_evidence(w.db, now_utc=NOW)
    assert w.db[aq.EVIDENCE_COLLECTION].count_documents({}) == 1
    assert w.db.qualified_events.count_documents({}) == 0


def test_earned_qualification_correction_is_manual_and_preserves_effects(monkeypatch):
    w, feed = databot_world()
    run(w, monkeypatch)
    before = list(w.db.xp_events.find())
    feed.add_batch([dict(urow('GATE11', '000CaseID', '2026-10-05 10:00:00'), coupon_new_player='0')])
    sync(w, feed)
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({}) == 1
    assert w.db.qualified_events.find_one()['evidence_status'] == 'disputed_pending_reconciliation'
    assert list(w.db.xp_events.find()) == before
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({'state': aq.REG_COMMITTED}) == 1


def test_partial_extraction_cannot_credit_older_passing_report():
    w, feed = databot_world()
    feed.add_batch([dict(urow('GATE11', '000CaseID', '2026-10-05 10:00:00'), after_total_bet_amount='1')])
    sync(w, feed)
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW, max_rows=1)
    assert aq.process_pending_evidence(w.db, now_utc=NOW)['skipped'] == 'source_extraction_incomplete'
    assert w.db.qualified_events.count_documents({}) == 0


def test_existing_insert_only_sync_rows_are_not_silently_enriched():
    w, feed = databot_world()
    original = w.db[syncer.ROWS_COLLECTION].find_one()
    w.db[syncer.ROWS_COLLECTION].update_one({'_id': original['_id']}, {'$unset': {
        'coupon_new_player': '', 'after_total_bet_amount': '', 'qualification_fields_version': ''}})
    sync(w, feed)
    assert 'coupon_new_player' not in w.db[syncer.ROWS_COLLECTION].find_one()
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    aq.process_pending_evidence(w.db, now_utc=NOW)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()['reason'] == 'missing_or_invalid_qualification_fields'


def test_historical_seed_ignores_new_gates_and_preserves_earned_history():
    w, _ = databot_world(fields={'coupon_new_player': '0', 'after_total_bet_amount': '0'})
    w.db.qualified_events.insert_one({'invitee_id': 11, 'referrer_id': 1, 'qualified_at': CUTOFF - timedelta(days=1)})
    before = list(w.db.qualified_events.find())
    plan = migration.build_plan(w.db, config=DATABOT_CONFIG, seed_from_linkage=False, now_utc=NOW)
    assert plan['accounts_to_seed'] == 1
    assert list(w.db.qualified_events.find()) == before


def test_completed_report_does_not_wait_seven_more_days():
    w, _ = databot_world()
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one()
    verdict = aq.assess_evidence(ev, control=aq.get_control(w.db),
                                now_utc=ev['redeemed_at'] + timedelta(minutes=1), lookup=aq.DbLookup(w.db))
    assert verdict['status'] == aq.ELIGIBLE


def test_enrichment_plan_is_bounded_read_only_and_reports_incomplete():
    from scripts.plan_uim_qualification_enrichment import build_plan
    w, _ = databot_world()
    for i in range(3):
        w.db[syncer.ROWS_COLLECTION].insert_one({'_id': f'old-{i}', 'batch_id': 'legacy'})
    before = _snapshot(w.db)
    plan = build_plan(_ReadOnlyDb(w.db), limit=2)
    assert plan['dry_run'] and plan['writes'] == 0
    assert plan['incomplete'] and plan['candidate_count_in_page'] == 2
    assert len(plan['candidates']) == 2
    assert _snapshot(w.db) == before


def test_stale_worker_snapshot_uses_newest_gate_values():
    w, feed = databot_world()
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    stale = w.db[aq.EVIDENCE_COLLECTION].find_one()
    feed.add_batch([dict(urow('GATE11', '000CaseID', '2026-10-05 10:00:00'), after_total_bet_amount='9.98')])
    sync(w, feed)
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    result = aq.evaluate_evidence(w.db, stale, control=aq.get_control(w.db), now_utc=NOW)
    assert result['reason'] == 'seven_day_turnover_does_not_exceed_9_99'
    assert w.db.qualified_events.count_documents({}) == 0


def test_databot_rejects_platform_namespace_configuration():
    cfg = dict(DATABOT_CONFIG, account_namespace='platform_specific')
    assert 'databot_requires_global_account_namespace' in aq.validate_source_config(cfg)


def test_ambiguous_core_correction_cannot_requeue_stale_passing_values():
    w, feed = databot_world()
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    feed.add_batch([urow('GATE11', '000CaseID', '2026-10-06 10:00:00')])
    sync(w, feed)
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one()
    assert aq.requeue_evidence(w.db, evidence_id=ev['_id'], now_utc=NOW, config=DATABOT_CONFIG)
    aq.process_pending_evidence(w.db, now_utc=NOW)
    assert w.db[aq.EVIDENCE_COLLECTION].find_one()['status'] == aq.EV_REVIEW
    assert w.db.qualified_events.count_documents({}) == 0
