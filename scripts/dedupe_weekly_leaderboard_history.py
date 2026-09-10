#!/usr/bin/env python3
"""Dedupe weekly_leaderboard_history so uniq_weekly_history_week_start can be created.

Root cause: the deprecated `save_weekly_snapshot()` writer (database.py) used a
bare `insert_one()` with no key check, so any misfire replay / boot catch-up /
second worker invocation could insert another document for the same
`week_start`. The current writer (`main._archive_week_upsert`) is already an
atomic upsert keyed by `week_start`, but a unique index still can't be built
on top of documents that were already duplicated before that fix shipped.

This script finds every `week_start` with more than one document, picks one
canonical record deterministically (see `pick_canonical`), moves every other
document for that week into a backup/audit collection
(`weekly_leaderboard_history_dedupe_backup`), and only then deletes them from
`weekly_leaderboard_history`. Nothing is ever deleted without first being
copied to the backup collection.

Usage:
  MONGO_URL='mongodb://...' python scripts/dedupe_weekly_leaderboard_history.py --dry-run
  MONGO_URL='mongodb://...' python scripts/dedupe_weekly_leaderboard_history.py --apply

Both modes print a full report of every duplicate group (all fields the
canonical decision was based on) before doing anything else. Re-running after
--apply is a safe no-op: once each week_start has exactly one document, the
duplicate-group query returns nothing.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure

HISTORY_COLLECTION_NAME = "weekly_leaderboard_history"
BACKUP_COLLECTION_NAME = "weekly_leaderboard_history_dedupe_backup"
INDEX_NAME = "uniq_weekly_history_week_start"


def _entry_count(doc: dict) -> int:
    return len(doc.get("checkin_leaderboard") or []) + len(doc.get("referral_leaderboard") or [])


def _is_valid_snapshot(doc: dict) -> bool:
    """A snapshot is "valid" if it has a week_end and at least one non-empty
    leaderboard. Legacy/partial writes (e.g. an interrupted insert_one) can
    lack these and must never be preferred over a complete record."""
    has_week_end = bool(doc.get("week_end"))
    has_entries = bool(doc.get("checkin_leaderboard") or doc.get("referral_leaderboard"))
    return has_week_end and has_entries


def _archived_at_sort_value(doc: dict):
    archived_at = doc.get("archived_at")
    if not isinstance(archived_at, datetime):
        return datetime.min.replace(tzinfo=timezone.utc)
    if archived_at.tzinfo is None:
        return archived_at.replace(tzinfo=timezone.utc)
    return archived_at


def _canonical_sort_key(doc: dict):
    """Deterministic ranking, highest wins: valid > most complete > latest
    archived_at > _id (final tiebreak — always unique, so this key never
    produces a tie between two distinct documents)."""
    return (
        1 if _is_valid_snapshot(doc) else 0,
        _entry_count(doc),
        _archived_at_sort_value(doc),
        str(doc.get("_id")),
    )


def pick_canonical(docs: list[dict]) -> dict:
    """Pick the canonical record for a duplicated week_start: prefer the
    latest valid, most complete snapshot; deterministic tiebreak by _id."""
    return max(docs, key=_canonical_sort_key)


def find_duplicate_groups(history_collection) -> list[dict]:
    """Group by the raw week_start value, including null/missing.

    A single-field unique index treats a missing field the same as an
    explicit null, so two documents that both lack week_start collide on
    that index exactly like two documents sharing a string value — they
    must be reported and deduped too, not silently skipped.
    """
    pipeline = [
        {"$group": {"_id": "$week_start", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    return list(history_collection.aggregate(pipeline, allowDiskUse=True))


def build_report(history_collection) -> list[dict]:
    """Fetch full documents for every duplicated week_start and decide the
    canonical one, without mutating anything."""
    groups = find_duplicate_groups(history_collection)
    report = []
    for group in groups:
        week_start = group["_id"]
        docs = list(history_collection.find({"week_start": week_start}))
        canonical = pick_canonical(docs)
        losers = [d for d in docs if d["_id"] != canonical["_id"]]
        report.append({
            "week_start": week_start,
            "doc_count": len(docs),
            "docs": [
                {
                    "_id": d["_id"],
                    "source": d.get("source", "unknown"),
                    "archived_at": d.get("archived_at"),
                    "week_end": d.get("week_end"),
                    "entry_counts": {
                        "checkin": len(d.get("checkin_leaderboard") or []),
                        "referral": len(d.get("referral_leaderboard") or []),
                    },
                    "valid": _is_valid_snapshot(d),
                    "is_canonical": d["_id"] == canonical["_id"],
                }
                for d in docs
            ],
            "canonical_id": canonical["_id"],
            "remove_ids": [d["_id"] for d in losers],
        })
    return report


def print_report(report: list[dict], *, mode: str) -> None:
    if not report:
        print(f"[{mode}] no duplicate week_start values found — nothing to do.")
        return
    print(f"[{mode}] {len(report)} duplicated week_start value(s) found:")
    for group in report:
        print(f"\n  week_start={group['week_start']} doc_count={group['doc_count']}")
        for d in group["docs"]:
            marker = "KEEP (canonical)" if d["is_canonical"] else "MOVE TO BACKUP + DELETE"
            print(
                f"    - _id={d['_id']} source={d['source']} archived_at={d['archived_at']} "
                f"week_end={d['week_end']} entries={d['entry_counts']} valid={d['valid']} -> {marker}"
            )


def dedupe(
    *,
    history_collection,
    backup_collection,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Run one dedupe pass. Returns a summary dict; never raises on expected
    Mongo errors (DuplicateKeyError from a losing concurrent writer, etc.) —
    those just mean the corresponding delete is skipped as already-handled.

    Idempotent: safe to call repeatedly. Once every week_start has a single
    document, find_duplicate_groups() returns [] and this becomes a no-op.
    """
    report = build_report(history_collection)
    print_report(report, mode="DRY-RUN" if dry_run else "APPLY")

    if dry_run:
        return {
            "mode": "dry_run",
            "duplicate_groups": len(report),
            "backed_up": 0,
            "deleted": 0,
            "report": report,
        }

    backed_up = 0
    deleted = 0
    for group in report:
        remove_ids = group["remove_ids"]
        if not remove_ids:
            continue

        docs_to_remove = list(history_collection.find({"_id": {"$in": remove_ids}}))
        for doc in docs_to_remove:
            # Backup write is an upsert keyed by the *original* _id, so a
            # rerun (or a crash between backup and delete) never double-backs-up
            # or loses a record: the same source doc always lands at the same
            # backup _id.
            backup_doc = dict(doc)
            backup_doc["_dedupe_meta"] = {
                "week_start": group["week_start"],
                "canonical_id": group["canonical_id"],
                "backed_up_at": datetime.now(timezone.utc),
            }
            result = backup_collection.replace_one(
                {"_id": doc["_id"]}, backup_doc, upsert=True
            )
            if getattr(result, "acknowledged", True) is False:
                raise RuntimeError(
                    f"backup write not acknowledged for _id={doc['_id']!r}; refusing to delete"
                )
            backed_up += 1

        # Verify every duplicate is now backed up before deleting anything
        # for this group — a partial backup failure must never cause a
        # delete of un-backed-up data.
        backed_up_ids = {
            d["_id"] for d in backup_collection.find({"_id": {"$in": remove_ids}})
        }
        confirmed_remove_ids = [rid for rid in remove_ids if rid in backed_up_ids]
        if len(confirmed_remove_ids) != len(remove_ids):
            missing = set(remove_ids) - backed_up_ids
            print(
                f"  ⚠️ skipping delete for week_start={group['week_start']}: "
                f"backup missing for _id(s)={missing}"
            )
            continue

        del_result = history_collection.delete_many({"_id": {"$in": confirmed_remove_ids}})
        deleted += int(getattr(del_result, "deleted_count", 0) or 0)

    return {
        "mode": "apply",
        "duplicate_groups": len(report),
        "backed_up": backed_up,
        "deleted": deleted,
        "report": report,
    }


def create_unique_index(history_collection) -> dict[str, Any]:
    """Attempt the unique index after cleanup. Never raises — this mirrors
    the non-fatal safe_create_index behavior used at app startup, so this
    script's exit code reflects the dedupe result, not index creation."""
    try:
        history_collection.create_index(
            [("week_start", ASCENDING)], unique=True, name=INDEX_NAME
        )
        return {"status": "created_or_exists", "name": INDEX_NAME}
    except (OperationFailure, DuplicateKeyError) as exc:
        return {"status": "failed", "name": INDEX_NAME, "error": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="referral_bot")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report only (default).")
    mode.add_argument("--apply", action="store_true", help="Back up and delete duplicates.")
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="After --apply, also attempt to create uniq_weekly_history_week_start.",
    )
    args = parser.parse_args()

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is required")

    client = MongoClient(mongo_url)
    db = client[args.db]
    history_collection = db[HISTORY_COLLECTION_NAME]
    backup_collection = db[BACKUP_COLLECTION_NAME]

    dry_run = not args.apply
    result = dedupe(
        history_collection=history_collection,
        backup_collection=backup_collection,
        dry_run=dry_run,
    )

    print(
        f"\n[{result['mode'].upper()}] duplicate_groups={result['duplicate_groups']} "
        f"backed_up={result['backed_up']} deleted={result['deleted']}"
    )

    if args.apply and args.create_index:
        idx_result = create_unique_index(history_collection)
        print(f"[INDEX] {idx_result}")
        if idx_result["status"] != "created_or_exists":
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
