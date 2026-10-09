"""One-time referral XP backfill.

Adds missing ``ref_success`` and bonus events for successful referrals. Safe to
run multiple times thanks to ``grant_xp`` idempotency. Defaults to dry-run.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

from pymongo import MongoClient
import affiliate_qualification as qualification

from referral_rules import (
    REFERRAL_BONUS_EVENT,
    REFERRAL_BONUS_INTERVAL,
    REFERRAL_BONUS_XP,
    REFERRAL_SUCCESS_EVENT,
    grant_referral_rewards,
)
from xp import grant_xp


def current_bonus_count(events, user_id):
    return events.count_documents({"user_id": user_id, "type": REFERRAL_BONUS_EVENT})


def _require_legacy_awards_allowed(db):
    # Read failures propagate before any award. A persisted cutoff disables
    # this old-rule writer even if the new rule is paused/disabled afterwards.
    if not qualification.legacy_award_allowed(qualification.get_control(db), datetime.now(timezone.utc)):
        raise RuntimeError("legacy referral XP backfill is disabled after the qualification launch cutoff")


def _collect_referral_counts(referrals):
    ref_counts = defaultdict(list)
    for ref in referrals:
        if ref.get("status") in {"pending", "inactive"}:
            continue
        referrer_id = ref.get("referrer_user_id") or ref.get("referrer_id")
        invitee_id = ref.get("invitee_user_id") or ref.get("referred_user_id")
        ref_counts[referrer_id].append(invitee_id)
    return {uid: len(set(referred)) for uid, referred in ref_counts.items()}


def backfill(db, dry_run: bool = True):
    if not dry_run:
        _require_legacy_awards_allowed(db)
    referrals = list(
        db.referrals.find(
            {"$or": [{"status": {"$exists": False}}, {"status": {"$nin": ["pending", "inactive"]}}]}
        )
    )
    ref_counts = _collect_referral_counts(referrals)

    missing_base = []
    for ref in referrals:
        referrer_id = ref.get("referrer_user_id") or ref.get("referrer_id")
        referred_user_id = ref.get("invitee_user_id") or ref.get("referred_user_id")
        if not referrer_id or not referred_user_id:
            continue
        key = f"{REFERRAL_SUCCESS_EVENT}:{referred_user_id}"
        exists = db.xp_events.find_one({"user_id": referrer_id, "unique_key": key})
        if not exists:
            missing_base.append((referrer_id, referred_user_id))

    for referrer_id, referred_user_id in missing_base:
        if dry_run:
            continue
        _require_legacy_awards_allowed(db)
        grant_referral_rewards(db, db.users, referrer_id, referred_user_id)

    missing_bonus = {}
    for uid, ref_count in ref_counts.items():
        expected_bonus = ref_count // REFERRAL_BONUS_INTERVAL
        existing = current_bonus_count(db.xp_events, uid)
        if expected_bonus > existing:
            missing_bonus[uid] = expected_bonus - existing

    for uid, to_award in missing_bonus.items():
        if dry_run:
            continue
        current = current_bonus_count(db.xp_events, uid)
        for idx in range(current + 1, current + to_award + 1):
            _require_legacy_awards_allowed(db)
            grant_xp(
                db,
                uid,
                REFERRAL_BONUS_EVENT,
                f"{REFERRAL_BONUS_EVENT}:{idx}",
                REFERRAL_BONUS_XP,
            )

    return {
        "missing_base": len(missing_base),
        "missing_bonus": sum(missing_bonus.values()),
        "dry_run": dry_run,
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill missing referral XP")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--commit", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("MONGO_URL is required")

    client = MongoClient(args.mongo_url)
    db = client["referral_bot"]

    summary = backfill(db, dry_run=not args.commit)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
