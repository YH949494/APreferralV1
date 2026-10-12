"""Deterministic historical identity planning and fail-closed completion."""
from datetime import datetime, timedelta, timezone
import json

import pytest

import affiliate_qualification as aq
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_welcome_redemption_qualification import NOW, CUTOFF, World, SOURCE_CONFIG, _ReadOnlyDb, _snapshot
from test_uim_redemption_sync import DATABOT_CONFIG, FakeFeed, disabled_world, urow, sync


def world():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db[aq.CONTROL_COLLECTION].update_one({'_id': aq.CONTROL_ID}, {'$unset': {'seed_completed_at': ''}})
    return w


def historical(w, uid=100001, at=CUTOFF):
    return w.db.qualified_events.insert_one({'invitee_id': uid, 'referrer_id': 900001,
                                            'qualified_at': at}).inserted_id


def coverage(w):
    # This authoritative committed row extends the complete source snapshot.
    w.upload([w.row('NONWELCOME', 'UnusedCoverageAccount', NOW - timedelta(minutes=1))])


def plan(w, links=True):
    return migration.build_plan(w.db, config=aq.get_control(w.db)['source_config'],
                                seed_from_linkage=links, now_utc=NOW)


@pytest.mark.parametrize('source', ['pool', 'new_joiner', 'ledger'])
def test_supported_issuance_sources_join_committed_redemption(source):
    w = world()
    historical(w)
    if source == 'pool':
        w.welcome_code(100001, 'PRIVATE-CODE', issued_at=CUTOFF - timedelta(days=1))
    elif source == 'new_joiner':
        w.db.new_joiner_claims.insert_one({'uid': 100001, 'code': 'PRIVATE-CODE',
                                          'claimed_at': CUTOFF - timedelta(days=1)})
    else:
        w.db.affiliate_ledger.insert_one({'dedup_key': 'WELCOME:100001', 'user_id': 100001,
                                         'status': 'ISSUED', 'voucher_code': 'PRIVATE-CODE',
                                         'issued_at': CUTOFF - timedelta(days=1)})
    w.upload([w.row('PRIVATE-CODE', '000ExactCase', CUTOFF)])
    result = plan(w, links=False)
    assert result['accounts_to_seed'] == 1
    assert result['_seeds'][0]['account_key'] == 'advantplay:000ExactCase'
    assert result['resolution_sources'] == {'redemption_evidence': 1}


def test_current_linkage_without_any_welcome_code_is_deterministic():
    w = world()
    historical(w)
    w.user(100001, linked=['000ExactCase'])
    coverage(w)
    result = plan(w)
    assert result['apply_safe']
    assert result['source_diagnostics']['without_welcome_code'] == 1
    assert result['resolution_sources'] == {'verified_linkage_single_account': 1}
    assert result['_seeds'][0]['account_key'] == 'advantplay:000ExactCase'
    assert plan(w, links=False)['accounts_to_seed'] == 0


def test_credited_registry_requires_exact_historical_event_owner():
    w = world()
    event = historical(w)
    coverage(w)
    w.db[aq.REGISTRY_COLLECTION].insert_one({'account_key': 'advantplay:000ExactCase',
        'invitee_id': 100001, 'qualified_event_id': event, 'state': aq.REG_CREDITED_LEGACY,
        'rule_version': aq.RULE_VERSION})
    assert plan(w, links=False)['resolution_sources'] == {'existing_credited_registry': 1}
    w.db[aq.REGISTRY_COLLECTION].update_one({}, {'$set': {'qualified_event_id': 'foreign'}})
    assert plan(w, links=False)['accounts_to_seed'] == 0


@pytest.mark.parametrize('links', [['A', 'B'], ['000ExactCase', 123], '000ExactCase', ['１２３']])
def test_multiple_or_malformed_linkage_is_never_guessed(links):
    w = world()
    historical(w)
    w.db.users.insert_one({'user_id': 100001, 'linked_gaming_accounts': links})
    assert plan(w)['accounts_to_seed'] == 0
    assert not plan(w)['apply_safe']


def test_redemption_linkage_and_registry_contradictions_remain_unresolved():
    w = world()
    event = historical(w)
    w.welcome_code(100001, 'PRIVATE-CODE', issued_at=CUTOFF - timedelta(days=1))
    w.upload([w.row('PRIVATE-CODE', 'AccountA', CUTOFF)])
    w.user(100001, linked=['AccountB'])
    w.db[aq.REGISTRY_COLLECTION].insert_one({'account_key': 'advantplay:AccountC',
        'invitee_id': 100001, 'qualified_event_id': event, 'state': aq.REG_SEEDED,
        'rule_version': aq.RULE_VERSION})
    result = plan(w)
    assert result['accounts_to_seed'] == 0
    assert result['counts']['conflicting_multiple_accounts'] == 1


def test_verified_linkage_can_reserve_history_despite_unverified_redemption_time():
    w = disabled_world()
    historical(w)
    w.welcome_code(100001, 'PRIVATE-CODE', issued_at=CUTOFF - timedelta(days=1))
    w.user(100001, linked=['000ExactCase'])
    feed = FakeFeed()
    feed.add_batch([urow('PRIVATE-CODE', '000ExactCase', '2026-10-05 10:00:00', basis='unknown_legacy')])
    sync(w, feed)
    result = plan(w)
    assert result['resolution_sources'] == {'verified_linkage_single_account': 1}
    # No timestamp policy or qualification gate is changed by identity reservation.
    assert aq.get_control(w.db)['source_config']['legacy_time_basis'] == aq.LEGACY_TIME_REVIEW
    w.db.users.update_one({'user_id': 100001}, {'$set': {'linked_gaming_accounts': ['OtherAccount']}})
    assert plan(w)['accounts_to_seed'] == 0


def test_stale_coverage_blocks_apply_before_any_write_and_reports_exact_gap():
    w = disabled_world()
    latest = aq.KL_TZ.localize(datetime(2026, 10, 1, 12)).astimezone(timezone.utc)
    historical(w, at=latest)
    w.user(100001, linked=['000ExactCase'])
    feed = FakeFeed()
    feed.add_batch([urow('UNKNOWN', 'CoverageAccount', '2026-09-27 12:00:00')])
    sync(w, feed)
    before = _snapshot(w.db)
    result = migration.run(_ReadOnlyDb(w.db), apply=True, seed_from_linkage=True, now_utc=NOW)
    assert result['plan']['source_coverage']['gap_seconds'] == 4 * 86400
    assert 'source_coverage_older_than_history' in result['apply_refused']
    assert not result['seed_marker_written']
    assert _snapshot(w.db) == before


def test_unresolved_user_blocks_completion_even_if_feed_coverage_is_current():
    w = world()
    historical(w)
    coverage(w)
    before = _snapshot(w.db)
    result = migration.run(_ReadOnlyDb(w.db), apply=True, seed_from_linkage=True, now_utc=NOW)
    assert 'unresolved_historical_identities' in result['apply_refused']
    assert not result['seed_marker_written']
    assert result['history_unchanged']
    assert _snapshot(w.db) == before


def test_earliest_duplicate_reserves_once_and_rerun_changes_no_earned_history():
    w = world()
    later = historical(w, uid=100002, at=CUTOFF + timedelta(hours=1))
    earliest = historical(w, uid=100001)
    for uid in (100001, 100002):
        w.user(uid, linked=['000ExactCase'])
    coverage(w)
    w.db.xp_events.insert_one({'user_id': 900001, 'created_at': CUTOFF, 'amount': 20})
    w.db.referral_events.insert_one({'invitee_id': 100001, 'event': 'referral_settled', 'occurred_at': CUTOFF})
    w.db.affiliate_ledger.insert_one({'status': 'ISSUED', 'created_at': CUTOFF, 'reward': 'earned'})
    collections = ('qualified_events', 'referral_events', 'affiliate_ledger', 'xp_events', 'referral_award_events')
    before = {name: list(w.db[name].find()) for name in collections}
    first = migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert first['seed_marker_written'], first
    row = w.db[aq.REGISTRY_COLLECTION].find_one()
    assert row['qualified_event_id'] == earliest
    assert row['invitee_id'] == 100001
    assert row['historical_qualification_count'] == 2
    assert first['plan']['historical_duplicate_extra_qualifications'] == 1
    second = migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert second['seed_marker_written']
    assert second['seed_result']['seeded'] == 0
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 1
    assert {name: list(w.db[name].find()) for name in collections} == before
    text = json.dumps(first, default=str)
    assert '000ExactCase' not in text and '100001' not in text and '100002' not in text


def test_old_or_stale_seed_markers_fail_preflight():
    from scripts.affiliate_qualification_admin import preflight_report
    w = world()
    historical(w)
    w.user(100001, linked=['000ExactCase'])
    coverage(w)
    w.db[aq.CONTROL_COLLECTION].update_one({'_id': aq.CONTROL_ID}, {'$set': {'seed_completed_at': NOW}})
    assert not preflight_report(w.db, now_utc=NOW)['migration_completion']['ok']
    assert migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW)['seed_marker_written']
    assert preflight_report(w.db, now_utc=NOW)['migration_completion']['ok']
    historical(w, uid=100002, at=CUTOFF + timedelta(days=1))
    report = preflight_report(w.db, now_utc=NOW)
    assert not report['ok']
    assert 'historical_qualifications_changed_since_seed' in report['migration_completion']['problems']


def test_existing_later_owner_is_reported_without_reassigning_earned_registry():
    w = world()
    earliest = historical(w)
    later = historical(w, uid=100002, at=CUTOFF + timedelta(days=1))
    for uid in (100001, 100002):
        w.user(uid, linked=['000ExactCase'])
    coverage(w)
    w.db[aq.REGISTRY_COLLECTION].insert_one({'account_key': 'advantplay:000ExactCase',
        'invitee_id': 100002, 'qualified_event_id': later, 'state': aq.REG_CREDITED_LEGACY,
        'rule_version': aq.RULE_VERSION})
    before = list(w.db[aq.REGISTRY_COLLECTION].find())
    report = migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert report['seed_result']['held_by_other'] == 1
    assert not report['seed_marker_written']
    assert list(w.db[aq.REGISTRY_COLLECTION].find()) == before


def test_crash_after_partial_registry_inserts_has_no_marker_and_retry_completes(monkeypatch):
    w = world()
    for uid, account in ((100001, '000One'), (100002, '000Two')):
        historical(w, uid=uid)
        w.user(uid, linked=[account])
    coverage(w)
    collection = w.db[aq.REGISTRY_COLLECTION]
    original = collection.insert_one
    calls = 0
    def crash(doc):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError('isolated crash')
        return original(doc)
    monkeypatch.setattr(collection, 'insert_one', crash)
    with pytest.raises(RuntimeError, match='isolated crash'):
        migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert collection.count_documents({}) == 1
    assert 'seed_completed_at' not in aq.get_control(w.db)
    assert 'migration_lease_token' not in aq.get_control(w.db)
    monkeypatch.setattr(collection, 'insert_one', original)
    report = migration.run(w.db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert report['seed_marker_written']
    assert report['seed_result']['seeded'] == 1 and report['seed_result']['already_seeded'] == 1
    assert w.db.qualified_events.count_documents({}) == 2


def test_fresh_missing_and_malformed_databot_coverage_are_fail_closed():
    w = disabled_world()
    historical(w)
    w.user(100001, linked=['000ExactCase'])
    for raw, expected in [(None, 'source_coverage_unknown'), ('bad', 'source_coverage_unknown'),
                          ('2026-10-05T10:00:00', None)]:
        w.db.uim_redemption_batches.delete_many({})
        w.db.uim_redemption_batches.insert_one({'_id': 'b', 'status': 'committed', 'rows_complete': True,
                                              'coverage_max_redeemed_at': raw})
        w.db.uim_redemption_sync_state.update_one({'_id': 'databot_uim'}, {'$set': {'last_success_at': NOW}}, upsert=True)
        result = plan(w)
        if expected:
            assert expected in result['apply_blockers']
        else:
            assert result['source_coverage']['gap_seconds'] == 0


def test_default_cli_uses_only_read_connection_and_never_writes(capsys):
    w = world()
    historical(w)
    before = _snapshot(w.db)
    def forbidden_primary():
        raise AssertionError('dry-run must never open the write connection')
    assert migration.main([], db_factory=forbidden_primary,
                          read_only_db_factory=lambda: _ReadOnlyDb(w.db)) == 0
    report = json.loads(capsys.readouterr().out)
    assert not report['apply'] and not report['seed_marker_written']
    assert _snapshot(w.db) == before


def test_unverified_snapshot_fields_do_not_fabricate_account_identity():
    w = world()
    event = historical(w)
    w.db.qualified_events.update_one({'_id': event}, {'$set': {'gaming_account': '000Unknown'}})
    w.db.referral_events.insert_one({'invitee_id': 100001, 'event': 'referral_settled',
                                    'account_id': '000Unknown', 'occurred_at': CUTOFF})
    assert plan(w)['accounts_to_seed'] == 0


def test_apply_cli_returns_failure_and_never_exposes_raw_error_values(capsys):
    def failed_connection():
        raise RuntimeError('token=PRIVATE_SECRET AccountID=000Sensitive uid=100001')
    assert migration.main(['--apply'], db_factory=failed_connection) == 1
    output = capsys.readouterr().out
    assert json.loads(output)['migration_failed'] == 'RuntimeError'
    for private in ('PRIVATE_SECRET', '000Sensitive', '100001'):
        assert private not in output
