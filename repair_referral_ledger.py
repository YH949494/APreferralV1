"""Repair invalid referral_revoked events in the referral_events ledger.

Background
----------
A referral_revoked event is only semantically valid when a matching
referral_settled event already exists for the same (inviter_id, invitee_id)
pair — a revocation reverses a settlement. Before this fix,
settle_pending_referrals() wrote referral_revoked for referrals that failed
*initial* qualification (self-invite, not in official channel, missing join
time, already attributed, insufficient engagement, etc.), most of which
never had a prior referral_settled event. Snapshot aggregation subtracts
every referral_revoked as -1, so those referrals could drive an inviter's
weekly/monthly/lifetime referral count below zero.

This script finds those invalid revocations (i.e. no matching prior
referral_settled for the same pair) and, by default, only *reports* them.

With --commit it marks each invalid event with:
    invalidated: true
    invalidated_reason: "revoked_without_prior_settlement"
    invalidated_at: <now>
so the original audit record is preserved (no deletion) while every
referral aggregation (which now filters on "invalidated" via
referral_ledger.with_not_invalidated) ignores it going forward.

Usage
-----
    python3 repair_referral_ledger.py --mongo-url "$MONGO_URL" [--mongo-db referral_bot]
    python3 repair_referral_ledger.py --mongo-url "$MONGO_URL" --commit
    python3 repair_referral_ledger.py --mongo-url "$MONGO_URL" --commit --rebuild-snapshots

Safe to re-run: events already marked invalidated are excluded from the
scan, so a second run finds zero additional invalid events.
"""

from __future__ import annotations

import argparse
import os
from collections import Counter
from datetime import datetime, timezone
from heapq import nlargest

from pymongo import MongoClient, UpdateOne

from referral_ledger import NOT_INVALIDATED_OR


def _find_invalid_revocations(db) -> list[dict]:
    """Revoked events with no matching prior referral_settled for the pair."""
    pipeline = [
        {
            "$match": {
                "event": "referral_revoked",
                "$or": list(NOT_INVALIDATED_OR),
            }
        },
        {
            "$lookup": {
                "from": "referral_events",
                "let": {"inviter": "$inviter_id", "invitee": "$invitee_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$inviter_id", "$$inviter"]},
                                    {"$eq": ["$invitee_id", "$$invitee"]},
                                    {"$eq": ["$event", "referral_settled"]},
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                ],
                "as": "matching_settlement",
            }
        },
        {"$match": {"matching_settlement": {"$size": 0}}},
        {"$project": {"matching_settlement": 0}},
    ]
    return list(db.referral_events.aggregate(pipeline, allowDiskUse=True))


def build_report(invalid_events: list[dict], now_ref: datetime) -> dict:
    week_key = None
    month_key = None
    if invalid_events:
        # week_key/month_key on each doc were computed at write time; use
        # today's for "current window impact" comparisons.
        from datetime import timedelta
        import pytz

        kl = pytz.timezone("Asia/Kuala_Lumpur")
        now_kl = now_ref.astimezone(kl)
        week_start = (now_kl - timedelta(days=now_kl.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        month_start = now_kl.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        week_key = week_start.date().isoformat()
        month_key = month_start.date().isoformat()

    inviter_counter: Counter = Counter()
    invitee_ids = set()
    reasons: Counter = Counter()
    occurred_dates = []
    weekly_impact = 0
    monthly_impact = 0

    for doc in invalid_events:
        inviter_id = doc.get("inviter_id")
        invitee_id = doc.get("invitee_id")
        if inviter_id is not None:
            inviter_counter[inviter_id] += 1
        if invitee_id is not None:
            invitee_ids.add(invitee_id)
        reasons[doc.get("reason") or "unknown"] += 1
        occurred_at = doc.get("occurred_at")
        if occurred_at is not None:
            occurred_dates.append(occurred_at)
        if week_key and doc.get("week_key") == week_key:
            weekly_impact += 1
        if month_key and doc.get("month_key") == month_key:
            monthly_impact += 1

    top_inviters = [
        {"inviter_id": inviter_id, "invalid_revocation_count": count}
        for inviter_id, count in nlargest(20, inviter_counter.items(), key=lambda kv: kv[1])
    ]

    sample_events = [
        {
            "_id": str(doc.get("_id")),
            "inviter_id": doc.get("inviter_id"),
            "invitee_id": doc.get("invitee_id"),
            "reason": doc.get("reason"),
            "occurred_at": doc.get("occurred_at").isoformat() if doc.get("occurred_at") else None,
        }
        for doc in invalid_events[:20]
    ]

    return {
        "invalid_revocation_count": len(invalid_events),
        "affected_inviter_count": len(inviter_counter),
        "affected_invitee_count": len(invitee_ids),
        "total_negative_impact": len(invalid_events),  # each invalid revoke = -1 lifetime
        "top_affected_inviters": top_inviters,
        "event_date_range": {
            "earliest": min(occurred_dates).isoformat() if occurred_dates else None,
            "latest": max(occurred_dates).isoformat() if occurred_dates else None,
        },
        "weekly_impact": weekly_impact,
        "monthly_impact": monthly_impact,
        "lifetime_impact": len(invalid_events),
        "reasons": dict(reasons),
        "sample_invalid_events": sample_events,
    }


def _invalidate(db, invalid_events: list[dict], batch_size: int = 500) -> int:
    now_ts = datetime.now(timezone.utc)
    ids = [doc["_id"] for doc in invalid_events]
    updated = 0
    for i in range(0, len(ids), batch_size):
        chunk = ids[i : i + batch_size]
        ops = [
            UpdateOne(
                {"_id": _id},
                {
                    "$set": {
                        "invalidated": True,
                        "invalidated_reason": "revoked_without_prior_settlement",
                        "invalidated_at": now_ts,
                    }
                },
            )
            for _id in chunk
        ]
        result = db.referral_events.bulk_write(ops, ordered=False)
        updated += int(result.modified_count or 0)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair invalid referral_revoked events (dry-run by default)")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "referral_bot"), help="Mongo database name")
    parser.add_argument("--commit", action="store_true", help="Apply changes (default: dry-run, report only)")
    parser.add_argument(
        "--rebuild-snapshots",
        action="store_true",
        help="After a successful --commit, rebuild referral snapshots (settle_referral_snapshots)",
    )
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("--mongo-url or MONGO_URL is required")

    client = MongoClient(args.mongo_url)
    db = client[args.mongo_db]

    invalid_events = _find_invalid_revocations(db)
    report = build_report(invalid_events, datetime.now(timezone.utc))
    report["dry_run"] = not args.commit

    if args.commit and invalid_events:
        report["invalidated_count"] = _invalidate(db, invalid_events)
    else:
        report["invalidated_count"] = 0

    print(report)

    if args.commit and args.rebuild_snapshots:
        import database
        import scheduler

        database.init_db(args.mongo_url, args.mongo_db)
        scheduler.settle_referral_snapshots()
        print("snapshot rebuild triggered via scheduler.settle_referral_snapshots()")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
