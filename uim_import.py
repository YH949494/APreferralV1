"""UIM import batch tracking with segment-sync lifecycle.

Wraps the bot_segment_sync flow with:
- batch_id (uuid4 with dashes)
- status: pending -> committed -> (seg_sync lifecycle)
- seg_sync_status: pending / running / completed / failed
- seg_sync_started_at, seg_sync_completed_at, seg_sync_error, seg_sync_rows_synced

Collection: uim_import_batches
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from threading import Thread
from typing import Any
from uuid import uuid4

import database
from bot_segment_sync import (
    _parse_rows,
    _existing_user_ids,
    _write_segment_snapshots,
    _chunks,
    BATCH_SIZE,
)
from pymongo import UpdateOne, DESCENDING

logger = logging.getLogger(__name__)


def _resolve_cols(batches_col=None, users_col=None, segment_snapshots_col=None):
    if any(c is None for c in (batches_col, users_col, segment_snapshots_col)):
        database.init_db()
    if batches_col is None:
        batches_col = database.uim_import_batches_col
    if users_col is None:
        users_col = database.users_collection
    if segment_snapshots_col is None:
        segment_snapshots_col = database.segment_snapshots_col
    return batches_col, users_col, segment_snapshots_col


def commit_batch(
    rows: list[list[Any]],
    *,
    batches_col=None,
    users_col=None,
    segment_snapshots_col=None,
    now: datetime | None = None,
) -> dict:
    batches_col, users_col, segment_snapshots_col = _resolve_cols(batches_col, users_col, segment_snapshots_col)
    now = now or datetime.now(timezone.utc)
    batch_id = str(uuid4())

    summary: dict = {
        "rows_scanned": 0,
        "valid_user_ids": 0,
        "users_matched": 0,
        "users_updated": 0,
        "users_write_attempted": 0,
        "users_modified": 0,
        "users_missing_in_db": 0,
        "blank_segments": 0,
        "unknown_segments": 0,
        "invalid_user_ids": 0,
        "dry_run": False,
        "error": None,
    }

    updates, user_ids = _parse_rows(
        rows,
        summary,
        spreadsheet_id="uim_import",
        worksheet_gid="0",
        now=now,
    )

    existing_ids = _existing_user_ids(users_col, user_ids) if user_ids else set()
    summary["users_missing_in_db"] = max(0, len(set(user_ids)) - len(existing_ids))
    matched_updates = [item for item in updates if item["user_id"] in existing_ids]
    summary["users_matched"] = len(matched_updates)
    summary["users_write_attempted"] = len(matched_updates)

    modified = 0
    if matched_updates:
        ops = [UpdateOne({"user_id": item["user_id"]}, {"$set": item["set"]}, upsert=False) for item in matched_updates]
        for batch in _chunks(ops, BATCH_SIZE):
            result = users_col.bulk_write(batch, ordered=False)
            modified += int(getattr(result, "modified_count", 0) or 0)

    summary["users_modified"] = modified
    summary["users_updated"] = modified

    batch_doc = {
        "batch_id": batch_id,
        "status": "committed",
        "rows_written": modified,
        "rows_scanned": summary["rows_scanned"],
        "users_updated": modified,
        "users_missing": summary["users_missing_in_db"],
        "committed_at": now,
        "user_ids": user_ids,
        "seg_sync_status": "pending",
        "seg_sync_started_at": None,
        "seg_sync_completed_at": None,
        "seg_sync_error": None,
        "seg_sync_rows_synced": None,
    }
    batches_col.insert_one(batch_doc)

    result = {
        "batch_id": batch_id,
        "status": "committed",
        "rows_written": modified,
        "rows_scanned": summary["rows_scanned"],
        "users_updated": modified,
        "users_missing": summary["users_missing_in_db"],
        "seg_sync_status": "pending",
        "ok": True,
    }
    logger.info("[UIM] commit batch_id=%s result=%s", batch_id, result)
    return result


def _build_matched_updates_from_db(user_ids: list[int], *, users_col, now: datetime) -> list[dict]:
    matched_updates = []
    for batch in _chunks(user_ids, BATCH_SIZE):
        cursor = users_col.find(
            {"user_id": {"$in": batch}},
            {"user_id": 1, "for_bot_segment": 1, "for_bot_segment_normalized": 1, "bot_segment_probability": 1, "_id": 0},
        )
        for doc in cursor:
            uid = doc.get("user_id")
            if uid is None:
                continue
            matched_updates.append({
                "user_id": uid,
                "set": {
                    "for_bot_segment": doc.get("for_bot_segment", ""),
                    "for_bot_segment_normalized": doc.get("for_bot_segment_normalized", ""),
                    "bot_segment_probability": doc.get("bot_segment_probability"),
                    "bot_segment_synced_at": now,
                },
            })
    return matched_updates


def run_seg_sync(
    batch_id: str,
    *,
    batches_col=None,
    users_col=None,
    segment_snapshots_col=None,
    now: datetime | None = None,
) -> dict:
    batches_col, users_col, segment_snapshots_col = _resolve_cols(batches_col, users_col, segment_snapshots_col)
    now = now or datetime.now(timezone.utc)

    batch = batches_col.find_one({"batch_id": batch_id})
    if not batch:
        return {"ok": False, "seg_sync_status": "failed", "seg_sync_rows_synced": None, "error": "batch not found"}

    batches_col.update_one(
        {"batch_id": batch_id},
        {"$set": {"seg_sync_status": "running", "seg_sync_started_at": now}},
    )

    user_ids = batch.get("user_ids") or []
    try:
        matched_updates = _build_matched_updates_from_db(user_ids, users_col=users_col, now=now)
        rows_synced = _write_segment_snapshots(segment_snapshots_col, matched_updates, now=now)
        batches_col.update_one(
            {"batch_id": batch_id},
            {"$set": {
                "seg_sync_status": "completed",
                "seg_sync_completed_at": now,
                "seg_sync_rows_synced": rows_synced,
                "seg_sync_error": None,
            }},
        )
        return {"ok": True, "seg_sync_status": "completed", "seg_sync_rows_synced": rows_synced}
    except Exception as exc:
        err = str(exc)
        batches_col.update_one(
            {"batch_id": batch_id},
            {"$set": {
                "seg_sync_status": "failed",
                "seg_sync_completed_at": now,
                "seg_sync_error": err,
            }},
        )
        return {"ok": False, "seg_sync_status": "failed", "seg_sync_rows_synced": None, "error": err}


def trigger_seg_sync_background(
    batch_id: str,
    *,
    batches_col=None,
    users_col=None,
    segment_snapshots_col=None,
) -> None:
    t = Thread(
        target=run_seg_sync,
        kwargs={
            "batch_id": batch_id,
            "batches_col": batches_col,
            "users_col": users_col,
            "segment_snapshots_col": segment_snapshots_col,
        },
        daemon=True,
    )
    t.start()


def get_import_history(*, batches_col=None, limit: int = 50) -> list[dict]:
    if batches_col is None:
        database.init_db()
        batches_col = database.uim_import_batches_col
    cursor = batches_col.find({}, {"_id": 0, "user_ids": 0}).sort("committed_at", DESCENDING).limit(limit)
    return list(cursor)


def render_seg_sync_status(s: str | None) -> str:
    if s in (None, "pending"):
        return "Not Started"
    if s == "running":
        return "Syncing"
    if s == "completed":
        return "Synced"
    if s == "failed":
        return "Failed"
    return "Not Started"
