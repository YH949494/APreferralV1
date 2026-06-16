"""Read-only sync of UIM claim-risk fields into the bot's ``users`` collection.

Mirrors ``bot_segment_sync.py``'s pattern exactly: reads pre-computed columns
from the same UIM ``user_profile_summary`` tab (no formula re-implementation,
no proxy calculation), and copies the values verbatim onto ``users`` docs for
existing users only. This phase only *syncs* the fields — nothing in the bot
reads or acts on ``claim_risk_level`` yet (that is a later phase).

Source columns (all optional except ``claim_risk_level``, which is required
for the sync to do anything):
  - claim_risk_level
  - claim_risk_reason
  - shared_account_risk_level

If ``claim_risk_level`` is missing from the sheet entirely, the sync reports
``skipped_reason`` and exits cleanly (``ok=True``) instead of crashing. UIM
also exposes these same three concepts on the ``player_detail`` and
``redeem_account_claim_audit`` tabs, but those are grouped by
month/redeem_account rather than by the single ``user_id`` key this sync
needs — if ``user_profile_summary`` ever drops these columns, a future
revision of this module would need a redeem_account-to-user_id join that
does not exist yet, rather than silently guessing thresholds.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from pymongo import UpdateOne
from pymongo.errors import PyMongoError

import bot_segment_sync
import database

logger = logging.getLogger(__name__)

CLAIM_RISK_LEVEL_HEADER = "claim_risk_level"
CLAIM_RISK_REASON_HEADER = "claim_risk_reason"
SHARED_ACCOUNT_RISK_LEVEL_HEADER = "shared_account_risk_level"
BATCH_SIZE = 500


def _empty_summary(*, dry_run: bool) -> dict:
    return {
        "ok": False,
        "rows_scanned": 0,
        "valid_user_ids": 0,
        "users_matched": 0,
        "users_write_attempted": 0,
        "users_modified": 0,
        "users_missing_in_db": 0,
        "invalid_user_ids": 0,
        "history_written": 0,
        "source_columns_present": {},
        "skipped_reason": None,
        "dry_run": bool(dry_run),
        "error": None,
    }


def _resolve_users_collection(users_col=None):
    if users_col is not None:
        return users_col
    database.init_db()
    return database.users_collection


def _resolve_history_collection(history_col=None):
    if history_col is not None:
        return history_col
    return database.user_claim_risk_history_col


def _chunks(items: list, size: int) -> Iterable[list]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _existing_users_with_risk(users_col, user_ids: list[int]) -> dict[int, str | None]:
    existing: dict[int, str | None] = {}
    for batch in _chunks(user_ids, BATCH_SIZE):
        cursor = users_col.find(
            {"user_id": {"$in": batch}},
            {"user_id": 1, "claim_risk_level": 1, "_id": 0},
        )
        for doc in cursor:
            try:
                existing[int(doc.get("user_id"))] = doc.get("claim_risk_level")
            except (TypeError, ValueError):
                continue
    return existing


def _parse_rows(rows: list[list[Any]], summary: dict, *, now: datetime | None = None) -> tuple[list[dict], list[int]]:
    if not rows:
        raise RuntimeError("sheet returned no rows")
    headers = [str(value or "").strip().lower() for value in rows[0]]

    present = {
        CLAIM_RISK_LEVEL_HEADER: CLAIM_RISK_LEVEL_HEADER in headers,
        CLAIM_RISK_REASON_HEADER: CLAIM_RISK_REASON_HEADER in headers,
        SHARED_ACCOUNT_RISK_LEVEL_HEADER: SHARED_ACCOUNT_RISK_LEVEL_HEADER in headers,
    }
    summary["source_columns_present"] = present

    if not present[CLAIM_RISK_LEVEL_HEADER]:
        summary["skipped_reason"] = (
            f"missing required header in user_profile_summary: {CLAIM_RISK_LEVEL_HEADER!r}. "
            "claim_risk_level/claim_risk_reason live on the player_detail and "
            "redeem_account_claim_audit tabs too, but those are keyed by "
            "month/redeem_account, not user_id — no safe join exists yet, so "
            "this sync does not guess one."
        )
        return [], []

    level_idx = headers.index(CLAIM_RISK_LEVEL_HEADER)
    reason_idx = headers.index(CLAIM_RISK_REASON_HEADER) if present[CLAIM_RISK_REASON_HEADER] else None
    shared_idx = headers.index(SHARED_ACCOUNT_RISK_LEVEL_HEADER) if present[SHARED_ACCOUNT_RISK_LEVEL_HEADER] else None

    now = now or datetime.now(timezone.utc)
    updates: list[dict] = []
    user_ids: list[int] = []
    seen: set[int] = set()
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

        level = str(row[level_idx] if len(row) > level_idx else "").strip()
        if not level:
            continue

        reason = str(row[reason_idx] if reason_idx is not None and len(row) > reason_idx else "").strip()
        shared_level = str(row[shared_idx] if shared_idx is not None and len(row) > shared_idx else "").strip()

        summary["valid_user_ids"] += 1
        updates.append(
            {
                "user_id": user_id,
                "new_level": level,
                "set": {
                    "claim_risk_level": level,
                    "claim_risk_reason": reason or None,
                    "shared_account_risk_level": shared_level or None,
                    "claim_risk_synced_at": now,
                    "claim_risk_source": "UIM",
                },
            }
        )
        if user_id not in seen:
            seen.add(user_id)
            user_ids.append(user_id)
    return updates, user_ids


def _write_history(history_col, changed: list[dict], existing: dict[int, str | None], *, now: datetime) -> int:
    ops = []
    for item in changed:
        user_id = item["user_id"]
        ops.append(
            UpdateOne(
                {"_id": f"{user_id}:{now.isoformat()}"},
                {
                    "$setOnInsert": {
                        "user_id": user_id,
                        "telegram_user_id": user_id,
                        "old_claim_risk_level": existing.get(user_id),
                        "new_claim_risk_level": item["new_level"],
                        "reason": item["set"].get("claim_risk_reason"),
                        "source": "UIM",
                        "synced_at": now,
                    }
                },
                upsert=True,
            )
        )
    written = 0
    for batch in _chunks(ops, BATCH_SIZE):
        result = history_col.bulk_write(batch, ordered=False)
        written += int(getattr(result, "upserted_count", 0) or 0)
    return written


def sync_claim_risk_from_sheet(
    *,
    dry_run: bool = True,
    users_col=None,
    history_col=None,
    rows: list[list[Any]] | None = None,
    spreadsheet_id: str | None = None,
    worksheet_gid: str | None = None,
) -> dict:
    summary = _empty_summary(dry_run=dry_run)
    spreadsheet_id = spreadsheet_id or os.getenv("BOT_SEGMENT_SHEET_ID", bot_segment_sync.DEFAULT_BOT_SEGMENT_SHEET_ID)
    worksheet_gid = str(
        worksheet_gid or os.getenv("BOT_SEGMENT_SHEET_GID", bot_segment_sync.DEFAULT_BOT_SEGMENT_SHEET_GID)
    )
    now = datetime.now(timezone.utc)
    try:
        if rows is None:
            rows = bot_segment_sync.fetch_sheet_rows(spreadsheet_id=spreadsheet_id, worksheet_gid=worksheet_gid)
        updates, user_ids = _parse_rows(rows, summary, now=now)

        if summary["skipped_reason"]:
            summary["ok"] = True
            logger.info("[CLAIM_RISK_SYNC] skipped reason=%s", summary["skipped_reason"])
            return summary

        users_col = _resolve_users_collection(users_col)
        existing = _existing_users_with_risk(users_col, user_ids) if user_ids else {}
        summary["users_missing_in_db"] = max(0, len(set(user_ids)) - len(existing))
        matched = [item for item in updates if item["user_id"] in existing]
        summary["users_matched"] = len(matched)

        if dry_run or not matched:
            summary["ok"] = True
            logger.info("[CLAIM_RISK_SYNC] dry_run_summary=%s", summary)
            return summary

        changed = [item for item in matched if existing.get(item["user_id"]) != item["new_level"]]

        summary["users_write_attempted"] = len(matched)
        ops = [UpdateOne({"user_id": item["user_id"]}, {"$set": item["set"]}, upsert=False) for item in matched]
        modified = 0
        for batch in _chunks(ops, BATCH_SIZE):
            result = users_col.bulk_write(batch, ordered=False)
            modified += int(getattr(result, "modified_count", 0) or 0)
        summary["users_modified"] = modified

        if changed:
            history_col = _resolve_history_collection(history_col)
            summary["history_written"] = _write_history(history_col, changed, existing, now=now)

        summary["ok"] = True
        logger.info("[CLAIM_RISK_SYNC] commit_summary=%s", summary)
        return summary
    except (RuntimeError, ValueError, PyMongoError) as exc:
        summary["error"] = str(exc)
        logger.error("[CLAIM_RISK_SYNC] failed err=%s", str(exc))
        return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync claim_risk_level/reason from UIM sheet into Mongo users")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Read and report only; do not write MongoDB")
    mode.add_argument("--commit", action="store_true", help="Write matched existing users")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    summary = sync_claim_risk_from_sheet(dry_run=not args.commit)
    print(json.dumps(summary, default=str, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
