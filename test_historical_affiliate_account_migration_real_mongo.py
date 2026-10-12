"""Real isolated MongoDB index, lease, crash and retry validation."""
from pymongo.collection import Collection
import pytest

import affiliate_qualification as aq
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_qualification_rollout_real_mongo import real_db
from test_affiliate_welcome_redemption_qualification import World, NOW, _scheduler
from test_historical_affiliate_account_migration import historical, coverage


def real_world(db):
    w = World(db=db, mode=aq.MODE_DISABLED, cutoff=None)
    db[aq.CONTROL_COLLECTION].update_one({'_id': aq.CONTROL_ID}, {'$unset': {'seed_completed_at': ''}})
    for uid, account in ((100001, '000One'), (100002, '000Two')):
        historical(w, uid=uid)
        w.user(uid, linked=[account])
    coverage(w)
    return w


def test_real_partial_insert_crash_and_idempotent_retry(real_db, monkeypatch):
    w = real_world(real_db)
    original = Collection.insert_one
    inserted = 0
    def crash(self, doc, *args, **kwargs):
        nonlocal inserted
        if self.name == aq.REGISTRY_COLLECTION:
            inserted += 1
            if inserted == 2:
                raise RuntimeError('isolated interruption')
        return original(self, doc, *args, **kwargs)
    monkeypatch.setattr(Collection, 'insert_one', crash)
    with pytest.raises(RuntimeError, match='isolated interruption'):
        migration.run(real_db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({}) == 1
    assert 'seed_completed_at' not in aq.get_control(real_db)
    monkeypatch.setattr(Collection, 'insert_one', original)
    done = migration.run(real_db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert done['seed_marker_written'] and done['history_unchanged']
    again = migration.run(real_db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert again['seed_marker_written'] and again['seed_result']['seeded'] == 0
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({}) == 2
    assert real_db.qualified_events.count_documents({}) == 2


def test_real_lease_prevents_concurrent_apply_and_cannot_erase_other_lease(real_db):
    w = real_world(real_db)
    from datetime import timedelta
    real_db[aq.CONTROL_COLLECTION].update_one({'_id': aq.CONTROL_ID}, {'$set': {
        'migration_lease_token': 'other-migration', 'migration_lease_until': NOW + timedelta(minutes=10)}})
    report = migration.run(real_db, apply=True, seed_from_linkage=True, now_utc=NOW)
    assert report['apply_refused'] == ['migration_lease_unavailable_or_control_changed']
    assert not report['seed_marker_written']
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({}) == 0
    assert aq.get_control(real_db)['migration_lease_token'] == 'other-migration'
