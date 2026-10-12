#!/usr/bin/env python3
"""Read-only, bounded compatibility inventory. No apply/requeue/cursor-reset option.

Run on the APReferral host with its existing read-only Mongo binding:
    python scripts/plan_uim_qualification_enrichment.py --limit 1000

Output references identify only candidate observations, not recoverable values.
See docs/affiliate_qualification_gates.md before preparing an enrichment manifest.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import affiliate_qualification as aq
import uim_redemption_sync as syncer

FIELDS = ('coupon_new_player', 'after_total_bet_amount', 'qualification_fields_version')


def build_plan(db, *, limit=1000):
    limit = max(1, min(int(limit), 5000))
    query = {'$or': [{field: {'$exists': False}} for field in FIELDS] +
                    [{field: None} for field in FIELDS]}
    # limit+1 explicitly proves whether this manifest covers all candidates.
    docs = list(db[syncer.ROWS_COLLECTION].find(query, {
        '_id': 1, 'batch_id': 1, 'provenance': 1, **{f: 1 for f in FIELDS},
    }).sort('_id', 1).limit(limit + 1))
    return {
        'dry_run': True, 'writes': 0, 'qualification_gate_version': aq.QUALIFICATION_GATE_VERSION,
        'candidate_count_in_page': min(len(docs), limit), 'incomplete': len(docs) > limit,
        'next_after': str(docs[limit - 1]['_id']) if len(docs) > limit else None,
        'candidates': [{'source_row_ref': str(d['_id']), 'batch_id': d.get('batch_id'),
                        'provenance': d.get('provenance'),
                        'missing_fields': [f for f in FIELDS if d.get(f) is None]} for d in docs[:limit]],
        'required_review': 'Join each ref to the immutable committed Databot log; '
                           'legacy floats cannot reconstruct exact source turnover. '
                           'No evidence, cursor or earned event is changed by this plan.',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=1000)
    args = parser.parse_args()
    from scripts.verify_affiliate_reward_plan import _read_only_db
    print(json.dumps(build_plan(_read_only_db(), limit=args.limit), indent=2, default=str))


if __name__ == '__main__':
    main()
