"""Gate decisions and concurrent corrections against isolated local MongoDB."""
import pytest
from pymongo.collection import Collection

import affiliate_qualification as aq
from test_affiliate_qualification_rollout_real_mongo import real_db
from test_affiliate_qualification_gates import databot_world
from test_affiliate_welcome_redemption_qualification import NOW, _scheduler
from test_uim_redemption_sync import DATABOT_CONFIG, sync, urow


@pytest.mark.parametrize('fields', [
    {'coupon_new_player': '0', 'after_total_bet_amount': '100'},
    {'coupon_new_player': False, 'after_total_bet_amount': '100'},
    {'coupon_new_player': '1', 'after_total_bet_amount': '9.99'},
    {'coupon_new_player': '1', 'after_total_bet_amount': None},
    {'coupon_new_player': '1', 'after_total_bet_amount': '80', 'qualification_fields_version': None},
    {'coupon_new_player': '1', 'after_total_bet_amount': '80', 'qualification_fields_version': 2},
])
def test_real_failed_gates_never_reserve_or_create_events(real_db, fields):
    w, _ = databot_world(fields=fields, db=real_db)
    aq.extract_committed_batches(real_db, config=DATABOT_CONFIG, now_utc=NOW)
    aq.process_pending_evidence(real_db, now_utc=NOW)
    assert real_db.qualified_events.count_documents({}) == 0
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({}) == 0
    assert real_db.referral_events.count_documents({}) == 0
    assert real_db.xp_events.count_documents({}) == 0


def test_real_correction_between_assessment_and_atomic_decision_blocks_stale_pass(real_db, monkeypatch):
    w, feed = databot_world(db=real_db)
    aq.extract_committed_batches(real_db, config=DATABOT_CONFIG, now_utc=NOW)
    ev = real_db[aq.EVIDENCE_COLLECTION].find_one()
    original = Collection.find_one_and_update
    changed = False

    def race(self, filt, update, *args, **kwargs):
        nonlocal changed
        if (self.name == aq.EVIDENCE_COLLECTION and 'qualification_decision_at' in update.get('$set', {})
                and not changed):
            changed = True
            feed.add_batch([dict(urow('GATE11', '000CaseID', '2026-10-05 10:00:00'), after_total_bet_amount='9.98')])
            sync(w, feed)
            aq.extract_committed_batches(real_db, config=DATABOT_CONFIG, now_utc=NOW)
        return original(self, filt, update, *args, **kwargs)

    monkeypatch.setattr(Collection, 'find_one_and_update', race)
    aq.evaluate_evidence(real_db, ev, control=aq.get_control(real_db), now_utc=NOW)
    assert changed
    assert real_db[aq.REGISTRY_COLLECTION].count_documents({}) == 0
    assert real_db.qualified_events.count_documents({}) == 0
    result = aq.evaluate_evidence(real_db, ev, control=aq.get_control(real_db), now_utc=NOW)
    assert result['reason'] == 'seven_day_turnover_below_80'
