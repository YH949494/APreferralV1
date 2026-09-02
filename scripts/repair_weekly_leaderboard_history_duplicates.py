#!/usr/bin/env python3
"""One-time maintenance repair for duplicate week_start values in
weekly_leaderboard_history, so the uniq_weekly_history_week_start index
(week_start, unique) can finally be created.

Dry-run by default — prints what WOULD be deleted and why, changes nothing.
Pass --commit to actually delete the losing documents.

Keeper selection (highest score wins), in order:
  1. valid schema (week_start/week_end parse as dates, leaderboards are
     lists, archived_at present)
  2. week_start falls on a Monday (the KL Monday->Sunday archive window)
  3. more complete leaderboard data (checkin + referral entry count)
  4. newest legitimate archive (archived_at, falling back to created_at)
  5. deterministic fallback: newest _id (ObjectId)

Usage:
  MONGO_URL='mongodb://...' python -m scripts.repair_weekly_leaderboard_history_duplicates [--db referral_bot]
  MONGO_URL='mongodb://...' python -m scripts.repair_weekly_leaderboard_history_duplicates --commit

Idempotent: once a week's duplicates are removed, re-running reports
duplicate_week_count=0 and deletes nothing.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
from typing import Any

from pymongo import MongoClient

logger = logging.getLogger("weekly_history_dedupe")

CRITERIA_LABELS = [
    "valid_schema",
    "monday_window",
    "leaderboard_completeness",
    "newest_archived_at",
    "_id_tiebreak",
]


def _is_valid_schema(doc: dict[str, Any]) -> bool:
    week_start = doc.get("week_start")
    week_end = doc.get("week_end")
    if not isinstance(week_start, str) or not isinstance(week_end, str):
        return False
    try:
        datetime.date.fromisoformat(week_start)
        datetime.date.fromisoformat(week_end)
    except ValueError:
        return False
    if not isinstance(doc.get("checkin_leaderboard"), list):
        return False
    if not isinstance(doc.get("referral_leaderboard"), list):
        return False
    if doc.get("archived_at") is None and doc.get("created_at") is None:
        return False
    return True


def _is_monday_window(doc: dict[str, Any]) -> bool:
    week_start = doc.get("week_start")
    if not isinstance(week_start, str):
        return False
    try:
        return datetime.date.fromisoformat(week_start).weekday() == 0
    except ValueError:
        return False


def _entry_count(doc: dict[str, Any]) -> int:
    checkins = doc.get("checkin_leaderboard")
    referrals = doc.get("referral_leaderboard")
    return (len(checkins) if isinstance(checkins, list) else 0) + (
        len(referrals) if isinstance(referrals, list) else 0
    )


def _archived_at_key(doc: dict[str, Any]):
    ts = doc.get("archived_at") or doc.get("created_at")
    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return ts
    return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


def _keeper_score(doc: dict[str, Any]):
    return (
        int(_is_valid_schema(doc)),
        int(_is_monday_window(doc)),
        _entry_count(doc),
        _archived_at_key(doc),
        doc["_id"],
    )


def _reason_keeper_selected(keeper: dict[str, Any], runner_up: dict[str, Any] | None) -> str:
    if runner_up is None:
        return "only_document_for_week"
    keeper_score = _keeper_score(keeper)
    runner_up_score = _keeper_score(runner_up)
    for label, keeper_val, runner_val in zip(CRITERIA_LABELS, keeper_score, runner_up_score):
        if keeper_val != runner_val:
            return f"{label} ({keeper_val!r} beats {runner_val!r})"
    return "_id_tiebreak (identical on all other criteria)"


def find_duplicate_groups(collection) -> list[dict[str, Any]]:
    pipeline = [
        {"$match": {"week_start": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": "$week_start", "count": {"$sum": 1}, "doc_ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def plan_repair(collection) -> list[dict[str, Any]]:
    """Return one plan entry per duplicated week_start, without writing anything."""
    plans = []
    for group in find_duplicate_groups(collection):
        week_start = group["_id"]
        docs = list(collection.find({"week_start": week_start}))
        if len(docs) < 2:
            continue
        scored = sorted(docs, key=_keeper_score, reverse=True)
        keeper = scored[0]
        losers = scored[1:]
        runner_up = losers[0] if losers else None
        plans.append(
            {
                "week_start": week_start,
                "document_count": len(docs),
                "document_ids": [d["_id"] for d in docs],
                "keeper_id": keeper["_id"],
                "delete_ids": [d["_id"] for d in losers],
                "reason_keeper_selected": _reason_keeper_selected(keeper, runner_up),
            }
        )
    return plans


def run(*, mongo_url: str, db_name: str, commit: bool) -> dict[str, Any]:
    client = MongoClient(mongo_url)
    collection = client[db_name]["weekly_leaderboard_history"]

    plans = plan_repair(collection)
    duplicate_week_count = len(plans)
    duplicate_document_count = sum(p["document_count"] for p in plans)
    total_deleted = 0

    for plan in plans:
        logger.info(
            "[WEEKLY_HISTORY][DEDUP][%s] week_start=%s document_count=%s keeper_id=%s "
            "delete_ids=%s reason=%s",
            "DRY_RUN" if not commit else "PLAN",
            plan["week_start"],
            plan["document_count"],
            plan["keeper_id"],
            plan["delete_ids"],
            plan["reason_keeper_selected"],
        )
        print(
            f"week_start={plan['week_start']} "
            f"document_count={plan['document_count']} "
            f"document_ids={plan['document_ids']} "
            f"keeper_id={plan['keeper_id']} "
            f"delete_ids={plan['delete_ids']} "
            f"reason_keeper_selected={plan['reason_keeper_selected']}"
        )
        if commit and plan["delete_ids"]:
            result = collection.delete_many({"_id": {"$in": plan["delete_ids"]}})
            deleted = int(result.deleted_count or 0)
            total_deleted += deleted
            logger.warning(
                "[WEEKLY_HISTORY][DEDUP][DELETE] week_start=%s deleted=%s keeper_id=%s",
                plan["week_start"],
                deleted,
                plan["keeper_id"],
            )

    summary = {
        "duplicate_week_count": duplicate_week_count,
        "duplicate_document_count": duplicate_document_count,
        "deleted": total_deleted if commit else 0,
    }
    mode = "COMMIT" if commit else "DRY_RUN"
    print(
        f"[{mode}] duplicate_week_count={summary['duplicate_week_count']} "
        f"duplicate_document_count={summary['duplicate_document_count']} "
        f"deleted={summary['deleted']}"
    )
    return summary


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="referral_bot")
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually delete the losing duplicate documents. Default is dry-run.",
    )
    args = parser.parse_args()

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is required")

    run(mongo_url=mongo_url, db_name=args.db, commit=args.commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
