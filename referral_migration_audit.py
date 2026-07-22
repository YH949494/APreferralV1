"""Referral-channel migration verification tooling (Phase 5 + Phase 12).

Read-only dry-run audit by default. Reports:
  - current referral destination mode + resolved destination chat id
  - bot/channel configuration presence
  - active group vs. channel invite_link_map mapping counts
  - cross-destination duplicate invitees in pending_referrals (Phase 5)
  - pending referrals missing destination metadata / usable timestamps
  - referral_award_events still using the legacy "ref:<group_id>:<uid>"
    destination-scoped award_key format
  - referral_settled events without a matching qualified_events row
  - qualified_events with duplicate invitee ids

Never mutates data unless --commit is passed, and even then the only write
is persisting the computed report snapshot to
``referral_migration_audit_reports`` for record-keeping — no referral data
is ever modified, merged, or deleted by this script (no automatic repair,
per the migration's completion restrictions).

Usage:
    python referral_migration_audit.py [--commit] [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_LEGACY_AWARD_KEY_RE = re.compile(r"^ref:-?\d+:\d+$")


def _connect_db():
    from database import init_db, db

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is not configured")
    init_db(mongo_url)
    return db


def build_report(db) -> dict:
    from referral_destination import (
        COMMUNITY_GROUP_ID,
        OFFICIAL_CHANNEL_ID,
        get_referral_destination,
    )

    dest_chat_id, destination_type = get_referral_destination()

    report: dict = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config": {
            "referral_destination_mode_env": os.getenv("REFERRAL_DESTINATION_MODE") or "(unset -> community_group)",
            "resolved_destination_chat_id": dest_chat_id,
            "resolved_destination_type": destination_type,
            "community_group_id": COMMUNITY_GROUP_ID,
            "official_channel_id": OFFICIAL_CHANNEL_ID,
            "bot_token_present": bool(os.environ.get("BOT_TOKEN")),
            "official_channel_id_configured": OFFICIAL_CHANNEL_ID is not None,
        },
    }

    # --- invite_link_map mapping counts ---
    report["invite_link_map"] = {
        "active_group_mappings": db.invite_link_map.count_documents(
            {"chat_id": COMMUNITY_GROUP_ID, "is_active": True}
        ),
        "active_channel_mappings": (
            db.invite_link_map.count_documents({"chat_id": OFFICIAL_CHANNEL_ID, "is_active": True})
            if OFFICIAL_CHANNEL_ID is not None
            else 0
        ),
        "rows_missing_destination_type": db.invite_link_map.count_documents(
            {"destination_type": {"$exists": False}}
        ),
    }

    # --- Phase 5: cross-destination duplicate invitees in pending_referrals ---
    dup_pipeline = [
        {"$group": {"_id": "$invitee_user_id", "count": {"$sum": 1}, "rows": {"$push": "$$ROOT"}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    duplicates = []
    for group in db.pending_referrals.aggregate(dup_pipeline):
        rows = group.get("rows") or []
        distinct_destinations = {
            row.get("destination_chat_id") or row.get("group_id") for row in rows
        }
        if len(distinct_destinations) < 2:
            # Same-destination duplicates are a pre-existing, harmless data
            # shape (the (group_id, invitee_user_id) unique index already
            # prevents literal dupes going forward) — only cross-destination
            # duplicates are the P0-4 risk this migration cares about.
            continue
        rows_sorted = sorted(rows, key=lambda r: r.get("created_at_utc") or datetime.min)
        any_settled = any(r.get("status") == "awarded" for r in rows)
        survivor = next((r for r in rows_sorted if r.get("status") == "awarded"), rows_sorted[0])
        duplicates.append(
            {
                "invitee_user_id": group["_id"],
                "row_count": group["count"],
                "destinations": list(distinct_destinations),
                "statuses": [r.get("status") for r in rows],
                "any_settled_or_awarded": any_settled,
                "recommended_survivor_id": str(survivor.get("_id")),
            }
        )
    report["cross_destination_duplicate_invitees"] = duplicates

    # --- pending referrals missing destination metadata / timestamps ---
    report["pending_referrals"] = {
        "missing_destination_metadata": db.pending_referrals.count_documents(
            {"destination_type": {"$exists": False}}
        ),
        "missing_usable_timestamp": db.pending_referrals.count_documents(
            {
                "referral_join_seen_at_utc": {"$exists": False},
                "created_at_utc": {"$exists": False},
            }
        ),
    }

    # --- referral_award_events still using the legacy destination-scoped key ---
    legacy_award_keys = []
    for doc in db.referral_award_events.find({}, {"award_key": 1}).limit(5000):
        key = doc.get("award_key") or ""
        if _LEGACY_AWARD_KEY_RE.match(key):
            legacy_award_keys.append(key)
    report["award_events"] = {
        "legacy_destination_scoped_award_key_count": len(legacy_award_keys),
        "sample_legacy_award_keys": legacy_award_keys[:10],
    }

    # --- referral_settled events without a matching qualified_events row ---
    settled_without_qualified = 0
    settled_cursor = db.referral_events.find(
        {"event": "referral_settled"}, {"inviter_id": 1, "invitee_id": 1}
    ).limit(5000)
    for doc in settled_cursor:
        inviter_id = doc.get("inviter_id")
        invitee_id = doc.get("invitee_id")
        if inviter_id is None or invitee_id is None:
            continue
        has_qualified = db.qualified_events.find_one(
            {"referrer_id": inviter_id, "invitee_id": invitee_id}, {"_id": 1}
        )
        if not has_qualified:
            settled_without_qualified += 1
    report["settled_without_qualified_event_count"] = settled_without_qualified

    # --- qualified_events with duplicate invitee ids ---
    qualified_dupe_pipeline = [
        {"$group": {"_id": "$invitee_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    qualified_dupes = list(db.qualified_events.aggregate(qualified_dupe_pipeline))
    report["qualified_events_duplicate_invitee_ids"] = [
        {"invitee_id": row["_id"], "count": row["count"]} for row in qualified_dupes
    ]

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist this report snapshot to referral_migration_audit_reports. "
        "Never mutates any referral/pending/award/qualified data.",
    )
    parser.add_argument("--json", action="store_true", help="Print the report as JSON.")
    args = parser.parse_args()

    db = _connect_db()
    report = build_report(db)

    if args.commit:
        db.referral_migration_audit_reports.insert_one(dict(report))

    if args.json:
        print(json.dumps(report, default=str, indent=2))
    else:
        _print_human(report)
    return 0


def _print_human(report: dict) -> None:
    cfg = report["config"]
    print("=== Referral Channel Migration — Dry-Run Audit ===")
    print(f"generated_at_utc: {report['generated_at_utc']}")
    print()
    print("-- Configuration --")
    for k, v in cfg.items():
        print(f"  {k}: {v}")
    print()
    print("-- invite_link_map --")
    for k, v in report["invite_link_map"].items():
        print(f"  {k}: {v}")
    print()
    dups = report["cross_destination_duplicate_invitees"]
    print(f"-- Cross-destination duplicate invitees: {len(dups)} --")
    for row in dups[:20]:
        print(f"  {row}")
    print()
    print("-- pending_referrals --")
    for k, v in report["pending_referrals"].items():
        print(f"  {k}: {v}")
    print()
    print("-- award_events --")
    for k, v in report["award_events"].items():
        print(f"  {k}: {v}")
    print()
    print(f"settled_without_qualified_event_count: {report['settled_without_qualified_event_count']}")
    print(f"qualified_events_duplicate_invitee_ids: {report['qualified_events_duplicate_invitee_ids']}")


if __name__ == "__main__":
    raise SystemExit(main())
