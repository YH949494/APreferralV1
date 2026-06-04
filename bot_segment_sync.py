from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from config import (
    is_blank_or_unknown_for_bot_segment,
    normalize_for_bot_segment,
    public_pool_probability_for_bot_segment,
)
import database

logger = logging.getLogger(__name__)

DEFAULT_BOT_SEGMENT_SHEET_ID = "1oAKJkxZ_o-uUi6I7jXJdTKRau35nR1wv2B8ShLUkWCg"
DEFAULT_BOT_SEGMENT_SHEET_GID = "1307413730"
BOT_SEGMENT_HEADER = "for_bot_segment"
BATCH_SIZE = 500


def _empty_summary(*, dry_run: bool) -> dict:
    return {
        "ok": False,
        "rows_scanned": 0,
        "valid_user_ids": 0,
        "users_matched": 0,
        "users_updated": 0,  # Backward-compatible alias for users_modified.
        "users_write_attempted": 0,
        "users_modified": 0,
        "users_missing_in_db": 0,
        "blank_segments": 0,
        "unknown_segments": 0,
        "invalid_user_ids": 0,
        "dry_run": bool(dry_run),
        "error": None,
    }


def _service_account_credentials() -> Any | None:
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if raw_json:
        from google.oauth2.service_account import Credentials

        info = json.loads(raw_json)
        return Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    if file_path:
        from google.oauth2.service_account import Credentials

        return Credentials.from_service_account_file(file_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    return None


def _resolve_users_collection(users_col=None):
    if users_col is not None:
        return users_col
    database.init_db()
    return database.users_collection


def fetch_sheet_rows(*, spreadsheet_id: str, worksheet_gid: str) -> list[list[Any]]:
    credentials = _service_account_credentials()
    if credentials is None:
        raise RuntimeError("missing Google service account credentials: set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE")
    import gspread

    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.get_worksheet_by_id(int(worksheet_gid))
    if worksheet is None:
        raise RuntimeError(f"worksheet gid not found: {worksheet_gid}")
    return worksheet.get_all_values()


def _chunks(items: list[int], size: int) -> Iterable[list[int]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _existing_user_ids(users_col, user_ids: list[int]) -> set[int]:
    existing: set[int] = set()
    for batch in _chunks(user_ids, BATCH_SIZE):
        cursor = users_col.find({"user_id": {"$in": batch}}, {"user_id": 1, "_id": 0})
        for doc in cursor:
            try:
                existing.add(int(doc.get("user_id")))
            except (TypeError, ValueError):
                continue
    return existing


def _parse_rows(rows: list[list[Any]], summary: dict, *, spreadsheet_id: str, worksheet_gid: str) -> tuple[list[dict], list[int]]:
    if not rows:
        raise RuntimeError("sheet returned no rows")
    headers = [str(value or "").strip().lower() for value in rows[0]]
    try:
        segment_idx = headers.index(BOT_SEGMENT_HEADER)
    except ValueError as exc:
        raise RuntimeError(f"missing required header: {BOT_SEGMENT_HEADER}") from exc

    updates: list[dict] = []
    user_ids: list[int] = []
    seen: set[int] = set()
    now = datetime.now(timezone.utc)
    for row in rows[1:]:
        summary["rows_scanned"] += 1
        raw_user_id = row[0] if row else ""
        try:
            user_id = int(str(raw_user_id).strip())
        except (TypeError, ValueError):
            summary["invalid_user_ids"] += 1
            continue
        if user_id <= 0:
            summary["invalid_user_ids"] += 1
            continue
        raw_segment = row[segment_idx] if len(row) > segment_idx else ""
        raw_segment_str = str(raw_segment or "").strip()
        if not raw_segment_str:
            summary["blank_segments"] += 1
        elif is_blank_or_unknown_for_bot_segment(raw_segment_str):
            summary["unknown_segments"] += 1
        normalized = normalize_for_bot_segment(raw_segment_str)
        probability = public_pool_probability_for_bot_segment(normalized)
        summary["valid_user_ids"] += 1
        updates.append(
            {
                "user_id": user_id,
                "set": {
                    "for_bot_segment": raw_segment_str,
                    "for_bot_segment_normalized": normalized,
                    "bot_segment_probability": probability,
                    "bot_segment_source": "google_sheet",
                    "bot_segment_sheet_id": spreadsheet_id,
                    "bot_segment_sheet_gid": str(worksheet_gid),
                    "bot_segment_synced_at": now,
                },
            }
        )
        if user_id not in seen:
            seen.add(user_id)
            user_ids.append(user_id)
    return updates, user_ids


def sync_bot_segments_from_sheet(
    *,
    dry_run: bool = True,
    users_col=None,
    rows: list[list[Any]] | None = None,
    spreadsheet_id: str | None = None,
    worksheet_gid: str | None = None,
) -> dict:
    summary = _empty_summary(dry_run=dry_run)
    spreadsheet_id = spreadsheet_id or os.getenv("BOT_SEGMENT_SHEET_ID", DEFAULT_BOT_SEGMENT_SHEET_ID)
    worksheet_gid = str(worksheet_gid or os.getenv("BOT_SEGMENT_SHEET_GID", DEFAULT_BOT_SEGMENT_SHEET_GID))
    try:
        if rows is None:
            rows = fetch_sheet_rows(spreadsheet_id=spreadsheet_id, worksheet_gid=worksheet_gid)
        updates, user_ids = _parse_rows(rows, summary, spreadsheet_id=spreadsheet_id, worksheet_gid=worksheet_gid)
        users_col = _resolve_users_collection(users_col)
        existing_ids = _existing_user_ids(users_col, user_ids) if user_ids else set()
        summary["users_missing_in_db"] = max(0, len(set(user_ids)) - len(existing_ids))
        matched_updates = [item for item in updates if item["user_id"] in existing_ids]
        summary["users_matched"] = len(matched_updates)
        if dry_run or not matched_updates:
            summary["ok"] = True
            logger.info("[BOT_SEGMENT_SYNC] dry_run_summary=%s", summary)
            return summary
        summary["users_write_attempted"] = len(matched_updates)
        ops = [UpdateOne({"user_id": item["user_id"]}, {"$set": item["set"]}, upsert=False) for item in matched_updates]
        modified = 0
        for batch in _chunks(ops, BATCH_SIZE):
            result = users_col.bulk_write(batch, ordered=False)
            modified += int(getattr(result, "modified_count", 0) or 0)
        summary["users_modified"] = modified
        summary["users_updated"] = modified
        summary["ok"] = True
        logger.info("[BOT_SEGMENT_SYNC] commit_summary=%s", summary)
        return summary
    except (RuntimeError, ValueError, json.JSONDecodeError, PyMongoError) as exc:
        summary["error"] = str(exc)
        logger.error("[BOT_SEGMENT_SYNC] failed err=%s", str(exc))
        return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync for_bot_segment from Google Sheets into Mongo users")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Read and report only; do not write MongoDB")
    mode.add_argument("--commit", action="store_true", help="Write matched existing users")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    summary = sync_bot_segments_from_sheet(dry_run=not args.commit)
    print(json.dumps(summary, default=str, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
