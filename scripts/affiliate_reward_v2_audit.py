#!/usr/bin/env python3
"""HYPOTHETICAL RETROSPECTIVE audit of pending affiliate reward requests under
welcome_redemption_v2. READ-ONLY by construction.

Covers PENDING_MANUAL AFFILIATE_MONTHLY ledgers flagged
``bundle_denomination_short`` and answers, per request: if its qualified
invitees were judged by V2 (own Welcome redemption, coupon_new_player = 1,
after_total_bet_amount >= 80, canonical Account ID uniqueness), would the
claimed tier still be reached?

There is no --apply / --commit flag. The connection is secondary-preferred,
never calls init_db()/ensure_indexes(), and only find/count_documents run.
Operator CLI only: do not wire this into an HTTP route.

Usage
-----
    MONGO_URL=... python scripts/affiliate_reward_v2_audit.py --month 202610
    MONGO_URL=... python scripts/affiliate_reward_v2_audit.py --month 202610 --format json
    MONGO_URL=... python scripts/affiliate_reward_v2_audit.py --month 202610 --user-id 123456
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import affiliate_reward_v2_audit as audit  # noqa: E402


def _read_only_db():
    from scripts.verify_affiliate_reward_plan import _read_only_db as factory

    return factory()


def main(argv=None, *, db_factory=None, now_fn=None, out=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="Entitlement month, YYYYMM (GMT+8), e.g. 202610")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--user-id", type=int, help="Restrict to one affiliate")
    parser.add_argument("--max-requests", type=int, default=audit.DEFAULT_MAX_REQUESTS)
    parser.add_argument("--max-events-per-affiliate", type=int, default=audit.DEFAULT_MAX_EVENTS)
    parser.add_argument("--sample-size", type=int, default=audit.DEFAULT_SAMPLE_SIZE,
                        help="Masked samples per non-pass class per request")
    args = parser.parse_args(argv)
    out = out or sys.stdout
    for name in ("max_requests", "max_events_per_affiliate", "sample_size"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be >= 1")

    db = (db_factory or _read_only_db)()
    now = (now_fn or (lambda: datetime.now(timezone.utc)))()
    try:
        report = audit.build_audit(
            db, month=args.month, now_utc=now, max_requests=args.max_requests,
            max_events_per_affiliate=args.max_events_per_affiliate, sample_size=args.sample_size,
            user_id=args.user_id,
        )
    except ValueError as exc:
        parser.error(str(exc))
    out.write((json.dumps(report, indent=2, sort_keys=True, default=str) if args.format == "json"
               else audit.render_text(report)) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
