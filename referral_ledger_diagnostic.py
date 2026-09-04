"""Read-only per-referral diagnostic for the referral ledger (Issue 2).

Never writes anything. For each requested inviter (or every inviter whose
raw ledger net is negative, with --auto-detect), groups referral_events by
(inviter_id, invitee_id) -- one row per referral lifecycle -- and reports:

  - settled_count / revoked_count (split into valid vs already-invalidated
    by repair_referral_ledger.py), and net
  - each referral_events row's occurred_at, reason, invalidated state
  - the matching referral_flow_events rows for the same pair, whose
    Mongo _id doubles as the idempotency key (see
    affiliate_leaderboard.emit_referral_flow_event), so a settle/revoke can
    be traced across both collections
  - a `violation` flag: net < 0, or a valid revoked_count > 1, or any
    revoked_count without a settled_count -- the exact shapes
    repair_referral_ledger.py's --commit auto-invalidates

Usage:
    # Diagnose specific inviters
    python3 referral_ledger_diagnostic.py --mongo-url "$MONGO_URL" --inviter-ids 111,222

    # Auto-detect every inviter with a currently-negative raw ledger net
    python3 referral_ledger_diagnostic.py --mongo-url "$MONGO_URL" --auto-detect
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime


def _fmt_ts(ts) -> str | None:
    if isinstance(ts, datetime):
        return ts.isoformat()
    return ts


def find_negative_inviters(db) -> list[int]:
    """Every inviter_id whose raw (uninvalidated) settled-revoked net is
    negative right now. Read-only: mirrors scheduler._referral_sign_expr's
    netting logic but never writes users.* snapshots."""
    from referral_ledger import with_not_invalidated

    pipeline = [
        {
            "$match": with_not_invalidated(
                {"inviter_id": {"$ne": None}, "event": {"$in": ["referral_settled", "referral_revoked"]}}
            )
        },
        {
            "$group": {
                "_id": "$inviter_id",
                "net": {
                    "$sum": {
                        "$cond": [{"$eq": ["$event", "referral_settled"]}, 1, -1]
                    }
                },
            }
        },
        {"$match": {"net": {"$lt": 0}}},
    ]
    return [row["_id"] for row in db.referral_events.aggregate(pipeline)]


def build_pair_diagnostic(db, inviter_ids: list[int]) -> list[dict]:
    """One row per (inviter_id, invitee_id) referral lifecycle for the given
    inviters, with settlement/revocation counts, raw event rows, and the
    matching referral_flow_events idempotency-key rows."""
    if not inviter_ids:
        return []

    events = list(
        db.referral_events.find(
            {"inviter_id": {"$in": inviter_ids}, "event": {"$in": ["referral_settled", "referral_revoked"]}}
        )
    )
    pairs: dict[tuple, list[dict]] = defaultdict(list)
    for doc in events:
        pairs[(doc.get("inviter_id"), doc.get("invitee_id"))].append(doc)

    flow_events = list(
        db.referral_flow_events.find(
            {
                "referrer_id": {"$in": inviter_ids},
                "event": {"$in": ["referral_settled", "referral_revoked"]},
            }
        )
    )
    flow_by_pair: dict[tuple, list[dict]] = defaultdict(list)
    for doc in flow_events:
        flow_by_pair[(doc.get("referrer_id"), doc.get("invitee_id"))].append(doc)

    rows = []
    for (inviter_id, invitee_id), docs in sorted(pairs.items(), key=lambda kv: (kv[0][0] or 0, kv[0][1] or 0)):
        settled = [d for d in docs if d.get("event") == "referral_settled"]
        revoked_valid = [
            d for d in docs if d.get("event") == "referral_revoked" and not d.get("invalidated")
        ]
        revoked_invalidated = [
            d for d in docs if d.get("event") == "referral_revoked" and d.get("invalidated")
        ]
        net = len(settled) - len(revoked_valid)

        ledger_rows = [
            {
                "source_collection": "referral_events",
                "event": d.get("event"),
                "occurred_at": _fmt_ts(d.get("occurred_at")),
                "reason": d.get("reason"),
                "invalidated": bool(d.get("invalidated")),
                "invalidated_reason": d.get("invalidated_reason"),
            }
            for d in sorted(docs, key=lambda d: d.get("occurred_at") or datetime.min)
        ]
        flow_rows = [
            {
                "source_collection": "referral_flow_events",
                "event": d.get("event"),
                "ts_utc": _fmt_ts(d.get("ts_utc")),
                "idempotency_key": d.get("_id"),
            }
            for d in sorted(flow_by_pair.get((inviter_id, invitee_id), []), key=lambda d: d.get("ts_utc") or datetime.min)
        ]

        violation = net < 0 or len(revoked_valid) > 1 or (len(revoked_valid) > 0 and len(settled) == 0)

        rows.append(
            {
                "inviter_id": inviter_id,
                "invitee_id": invitee_id,
                "settled_count": len(settled),
                "revoked_count_valid": len(revoked_valid),
                "revoked_count_invalidated": len(revoked_invalidated),
                "net": net,
                "violation": violation,
                "ledger_rows": ledger_rows,
                "flow_event_rows": flow_rows,
            }
        )
    return rows


def build_report(db, inviter_ids: list[int] | None, auto_detect: bool) -> dict:
    if auto_detect:
        inviter_ids = find_negative_inviters(db)
    inviter_ids = inviter_ids or []
    rows = build_pair_diagnostic(db, inviter_ids)
    violations = [r for r in rows if r["violation"]]
    return {
        "generated_at_utc": datetime.utcnow().isoformat(),
        "inviter_ids_scanned": inviter_ids,
        "pair_count": len(rows),
        "violation_count": len(violations),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "referral_bot"), help="Mongo database name")
    parser.add_argument("--inviter-ids", default="", help="Comma-separated inviter user_ids to diagnose")
    parser.add_argument(
        "--auto-detect",
        action="store_true",
        help="Diagnose every inviter whose raw (uninvalidated) ledger net is currently negative",
    )
    parser.add_argument("--json", action="store_true", help="Print the report as JSON")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("--mongo-url or MONGO_URL is required")
    if not args.inviter_ids and not args.auto_detect:
        raise SystemExit("Pass --inviter-ids or --auto-detect")

    from pymongo import MongoClient

    client = MongoClient(args.mongo_url)
    db = client[args.mongo_db]

    inviter_ids = [int(x) for x in args.inviter_ids.split(",") if x.strip()] if args.inviter_ids else None
    report = build_report(db, inviter_ids, args.auto_detect)

    if args.json:
        print(json.dumps(report, default=str, indent=2))
    else:
        print(f"=== Referral Ledger Per-Pair Diagnostic (read-only) ===")
        print(f"generated_at_utc: {report['generated_at_utc']}")
        print(f"inviter_ids_scanned: {report['inviter_ids_scanned']}")
        print(f"pair_count: {report['pair_count']}  violation_count: {report['violation_count']}")
        for row in report["rows"]:
            flag = " <-- VIOLATION" if row["violation"] else ""
            print(
                f"  inviter={row['inviter_id']} invitee={row['invitee_id']} "
                f"settled={row['settled_count']} revoked_valid={row['revoked_count_valid']} "
                f"revoked_invalidated={row['revoked_count_invalidated']} net={row['net']}{flag}"
            )
            for lr in row["ledger_rows"]:
                print(f"      [referral_events] {lr}")
            for fr in row["flow_event_rows"]:
                print(f"      [referral_flow_events] {fr}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
