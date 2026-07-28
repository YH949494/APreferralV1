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

This script finds invalid revocations and, by default, only *reports* them.
Two corruption patterns are unambiguous and auto-invalidated with --commit:
    - revoked_without_prior_settlement: no matching referral_settled at all
      for the pair.
    - duplicate_revocation: more than one referral_revoked for a pair that
      does have a settlement (only the earliest is kept valid).
Two more are reported only, under "review_only" in the output, and are
never auto-invalidated — a human should look at the sample rows first:
    - revocation_predates_settlement: a real settlement exists, but the
      revocation's occurred_at is earlier than it (out-of-order/corrupted
      timestamps).
    - malformed_identifier: inviter_id/invitee_id is null or non-numeric.

With --commit, each auto-invalidated event is marked with:
    invalidated: true
    invalidated_reason: "revoked_without_prior_settlement" | "duplicate_revocation"
    invalidated_at: <now>
so the original audit record is preserved (no deletion) while every
referral aggregation (which now filters on "invalidated" via
referral_ledger.with_not_invalidated) ignores it going forward.

Usage
-----
Ledger invalidation and snapshot rebuild are separate operator actions with
separate checkpoints -- --commit and --rebuild-snapshots cannot be combined
in one run (the CLI rejects that combination with a clear error), so a
human reviews the invalidation report before triggering a rebuild:

    # 1. Dry-run report only (no writes)
    python3 repair_referral_ledger.py --mongo-url "$MONGO_URL" --mongo-db referral_bot

    # 2. Invalidate unambiguous corrupt ledger rows only (no snapshot rebuild)
    python3 repair_referral_ledger.py --mongo-url "$MONGO_URL" --mongo-db referral_bot --commit

    # 3. Rebuild snapshots only -- does NOT require --commit, and can be run
    #    any time (e.g. on its own schedule, or right after step 2 once its
    #    report has been reviewed)
    python3 repair_referral_ledger.py --mongo-url "$MONGO_URL" --mongo-db referral_bot --rebuild-snapshots

Safe to re-run: events already marked invalidated are excluded from the
scan, so a second run of --commit finds zero additional invalid events.
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
    events = list(db.referral_events.aggregate(pipeline, allowDiskUse=True))
    for doc in events:
        doc["invalid_reason"] = "revoked_without_prior_settlement"
    return events


def _find_duplicate_revocations(db) -> list[dict]:
    """Extra referral_revoked rows beyond the first *valid* one for the
    same (inviter, invitee) pair. New writes can't produce these —
    revoke_settled_referral checks for an existing revocation, and the
    uniq_referral_event index ((event, inviter_id, invitee_id), unique)
    enforces it at the database level — but rows written before that
    guard/index existed may still have them, and each extra one
    double-subtracts from snapshot aggregation.

    Only revocations at or after their pair's earliest matching settlement
    are considered here — a revocation timestamped *before* its settlement
    is corrupt/out-of-order data (``revocation_predates_settlement``,
    handled separately and review-only, never auto-invalidated) and must
    never be picked as the "valid" survivor just because it happens to be
    the earliest by clock time. A pair with no matching settlement at all
    belongs to ``_find_invalid_revocations`` instead, not here.
    """
    pipeline = [
        {
            "$match": {
                "event": "referral_revoked",
                "$or": list(NOT_INVALIDATED_OR),
                "inviter_id": {"$ne": None},
                "invitee_id": {"$ne": None},
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
                    {"$sort": {"occurred_at": 1}},
                    {"$limit": 1},
                ],
                "as": "matching_settlement",
            }
        },
        {"$match": {"matching_settlement": {"$ne": []}}},
        {
            "$match": {
                "$expr": {
                    "$gte": ["$occurred_at", {"$arrayElemAt": ["$matching_settlement.occurred_at", 0]}]
                }
            }
        },
        {"$project": {"matching_settlement": 0}},
        {"$sort": {"occurred_at": 1, "_id": 1}},
        {
            "$group": {
                "_id": {"inviter_id": "$inviter_id", "invitee_id": "$invitee_id"},
                "docs": {"$push": "$$ROOT"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]
    duplicates = []
    for group in db.referral_events.aggregate(pipeline, allowDiskUse=True):
        # Keep the earliest *valid* (post-settlement) revocation -- the one
        # revoke_settled_referral's existing_revoke check would have found;
        # invalidate the rest.
        for doc in (group.get("docs") or [])[1:]:
            doc["invalid_reason"] = "duplicate_revocation"
            duplicates.append(doc)
    return duplicates


def _find_malformed_events(db) -> list[dict]:
    """referral_settled/referral_revoked rows with a null or non-numeric
    inviter_id/invitee_id. Reported only, never invalidated — a malformed
    inviter_id already cannot match any per-user aggregation ($match on
    inviter_id equality), so it cannot itself be driving a negative count,
    but it signals upstream data-quality problems worth a human look.
    """
    pipeline = [
        {
            "$match": {
                "event": {"$in": ["referral_settled", "referral_revoked"]},
                "$or": [
                    {"inviter_id": None},
                    {"invitee_id": None},
                    {"inviter_id": {"$type": "string"}},
                    {"invitee_id": {"$type": "string"}},
                ],
            }
        }
    ]
    events = list(db.referral_events.aggregate(pipeline, allowDiskUse=True))
    for doc in events:
        doc["invalid_reason"] = "malformed_identifier"
    return events


def _find_premature_revocations(db) -> list[dict]:
    """Revocations whose occurred_at is earlier than the settlement they
    claim to reverse. A revocation can only ever reverse a settlement that
    already happened, so this indicates corrupted/out-of-order legacy data.
    Reported only, never invalidated automatically — unlike a revocation
    with no matching settlement at all, a real settlement does exist here,
    so blanket auto-invalidation risks masking a legitimate (if
    oddly-timed) reversal; needs a human look at the sample rows.
    """
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
                    {"$sort": {"occurred_at": 1}},
                    {"$limit": 1},
                ],
                "as": "matching_settlement",
            }
        },
        {"$match": {"matching_settlement": {"$ne": []}}},
        {
            "$match": {
                "$expr": {
                    "$lt": ["$occurred_at", {"$arrayElemAt": ["$matching_settlement.occurred_at", 0]}]
                }
            }
        },
        {"$project": {"matching_settlement": 0}},
    ]
    events = list(db.referral_events.aggregate(pipeline, allowDiskUse=True))
    for doc in events:
        doc["invalid_reason"] = "revocation_predates_settlement"
    return events


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
        reasons[doc.get("invalid_reason") or doc.get("reason") or "unknown"] += 1
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
            "invalid_reason": doc.get("invalid_reason"),
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
    updated = 0
    for i in range(0, len(invalid_events), batch_size):
        chunk = invalid_events[i : i + batch_size]
        ops = [
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "invalidated": True,
                        "invalidated_reason": doc.get("invalid_reason") or "revoked_without_prior_settlement",
                        "invalidated_at": now_ts,
                    }
                },
            )
            for doc in chunk
        ]
        result = db.referral_events.bulk_write(ops, ordered=False)
        updated += int(result.modified_count or 0)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair invalid referral_revoked events (dry-run by default)")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "referral_bot"), help="Mongo database name")
    parser.add_argument("--commit", action="store_true", help="Invalidate unambiguous corrupt ledger rows (default: dry-run, report only)")
    parser.add_argument(
        "--rebuild-snapshots",
        action="store_true",
        help="Rebuild referral snapshots (settle_referral_snapshots) as its own standalone action. "
        "Does not require --commit, and cannot be combined with --commit in the same run.",
    )
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("--mongo-url or MONGO_URL is required")

    # Ledger invalidation and snapshot rebuild are deliberately separate
    # operator actions/checkpoints -- rejecting the combination (rather than
    # silently running both) forces a human to look at the invalidation
    # report before triggering a rebuild that depends on it.
    if args.commit and args.rebuild_snapshots:
        raise SystemExit(
            "--commit and --rebuild-snapshots cannot be combined in one run. "
            "Run --commit first, review its report, then run --rebuild-snapshots separately."
        )

    client = MongoClient(args.mongo_url)
    db = client[args.mongo_db]

    if args.rebuild_snapshots:
        import database
        import scheduler

        database.init_db(args.mongo_url, args.mongo_db)
        summary = scheduler.settle_referral_snapshots()
        print(summary)
        return 0

    now_ref = datetime.now(timezone.utc)

    # Auto-invalidated on --commit: both cases are unambiguous corruption
    # (no matching settlement at all / more than one revocation for a
    # settled pair) with no legitimate interpretation.
    no_settlement = _find_invalid_revocations(db)
    duplicates = _find_duplicate_revocations(db)
    invalid_events = no_settlement + duplicates
    report = build_report(invalid_events, now_ref)
    report["dry_run"] = not args.commit
    report["no_settlement_count"] = len(no_settlement)
    report["duplicate_revocation_count"] = len(duplicates)

    if args.commit and invalid_events:
        report["invalidated_count"] = _invalidate(db, invalid_events)
    else:
        report["invalidated_count"] = 0

    # Reported only, never auto-invalidated: these need a human look before
    # any action, since (unlike the two cases above) a genuine settlement
    # exists for the pair.
    premature = _find_premature_revocations(db)
    malformed = _find_malformed_events(db)
    report["review_only"] = {
        "premature_revocation_count": len(premature),
        "premature_revocation_samples": [
            {
                "_id": str(doc.get("_id")),
                "inviter_id": doc.get("inviter_id"),
                "invitee_id": doc.get("invitee_id"),
                "occurred_at": doc.get("occurred_at").isoformat() if doc.get("occurred_at") else None,
            }
            for doc in premature[:20]
        ],
        "malformed_identifier_count": len(malformed),
        "malformed_identifier_samples": [
            {
                "_id": str(doc.get("_id")),
                "event": doc.get("event"),
                "inviter_id": doc.get("inviter_id"),
                "invitee_id": doc.get("invitee_id"),
            }
            for doc in malformed[:20]
        ],
    }

    print(report)

    if args.commit:
        print(
            "Ledger invalidation complete. Snapshots were NOT rebuilt -- run "
            "with --rebuild-snapshots (on its own, no --commit) once you have "
            "reviewed this report."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
