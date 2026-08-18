"""Read-only sync of UIM multi-account risk fields into the bot's ``users``
collection.

Mirrors ``claim_risk_sync.py`` / ``bot_segment_sync.py``'s pattern exactly:
reads pre-computed columns from the same UIM ``user_profile_summary`` tab (no
cluster-detection formula re-implementation, no proxy calculation) and copies
the values verbatim onto ``users`` docs for existing users only.

Source columns (all optional except ``linked_gaming_accounts``, which is
required for the sync to do anything):
  - linked_gaming_accounts   comma/semicolon-separated gaming account ids
  - linked_tg_count          UIM's pre-computed cluster size for this identity
  - multi_account_cluster_member  UIM's pre-computed cluster-membership flag
  - multi_account_voucher_hunter  UIM's pre-computed behavioral+cluster flag
  - voucher_hunter_reasons   comma/semicolon-separated reason codes (e.g.
                              "multiple_account") kept for audit evidence only

Written fields (Telegram-level risk metadata) are kept on a path entirely
separate from canonical segment fields:
  - multi_account_cluster_member
  - multi_account_risk          (mirrors multi_account_cluster_member --
                                  the flag voucher_risk_eligibility.py reads)
  - multi_account_voucher_hunter
  - linked_gaming_accounts
  - linked_tg_count
  - voucher_hunter_reasons
  - multi_account_risk_source
  - multi_account_risk_synced_at

This module never reads or writes ``for_bot_segment``, ``for_bot_segment_normalized``,
``bot_segment_source``, or ``bot_segment_synced_at`` -- those are written
exclusively by the canonical segment path (Databot's segment_sync_job, or the
legacy manual bot_segment_sync.py tool). Risk propagation must never
overwrite canonical segment fields.
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

LINKED_GAMING_ACCOUNTS_HEADER = "linked_gaming_accounts"
LINKED_TG_COUNT_HEADER = "linked_tg_count"
CLUSTER_MEMBER_HEADER = "multi_account_cluster_member"
VOUCHER_HUNTER_HEADER = "multi_account_voucher_hunter"
VOUCHER_HUNTER_REASONS_HEADER = "voucher_hunter_reasons"
BATCH_SIZE = 500

_TRUE_VALUES = {"1", "true", "yes", "y", "t"}


def _empty_summary(*, dry_run: bool) -> dict:
    return {
        "ok": False,
        "rows_scanned": 0,
        "valid_user_ids": 0,
        "invalid_user_ids": 0,
        "users_matched": 0,
        "canonical_segment_users_matched": 0,
        "risk_members_matched": 0,
        "users_to_set_risk_true": 0,
        "users_to_clear_stale_risk": 0,
        "linked_accounts_to_add": 0,
        "linked_accounts_to_prune": 0,
        "canonical_segment_updates": 0,
        "users_missing_in_db": 0,
        "users_write_attempted": 0,
        "users_modified": 0,
        "writes_performed": False,
        "source_columns_present": {},
        "skipped_reason": None,
        "clusters": {},
        "preview": [],
        "dry_run": bool(dry_run),
        "error": None,
    }


def _resolve_users_collection(users_col=None):
    if users_col is not None:
        return users_col
    database.init_db()
    return database.users_collection


def _chunks(items: list, size: int) -> Iterable[list]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _parse_bool(raw: Any) -> bool:
    return str(raw or "").strip().lower() in _TRUE_VALUES


def _parse_list(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = [p.strip() for p in text.replace(";", ",").split(",")]
    seen: set[str] = set()
    out: list[str] = []
    for part in parts:
        if part and part not in seen:
            seen.add(part)
            out.append(part)
    return out


def _parse_int(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _existing_users_with_risk(users_col, user_ids: list[int]) -> dict[int, dict]:
    existing: dict[int, dict] = {}
    for batch in _chunks(user_ids, BATCH_SIZE):
        cursor = users_col.find(
            {"user_id": {"$in": batch}},
            {
                "user_id": 1,
                "multi_account_risk": 1,
                "linked_gaming_accounts": 1,
                "for_bot_segment": 1,
                "_id": 0,
            },
        )
        for doc in cursor:
            try:
                existing[int(doc.get("user_id"))] = doc
            except (TypeError, ValueError):
                continue
    return existing


def _parse_rows(rows: list[list[Any]], summary: dict, *, now: datetime | None = None) -> tuple[list[dict], list[int]]:
    if not rows:
        raise RuntimeError("sheet returned no rows")
    headers = [str(value or "").strip().lower() for value in rows[0]]

    present = {
        LINKED_GAMING_ACCOUNTS_HEADER: LINKED_GAMING_ACCOUNTS_HEADER in headers,
        LINKED_TG_COUNT_HEADER: LINKED_TG_COUNT_HEADER in headers,
        CLUSTER_MEMBER_HEADER: CLUSTER_MEMBER_HEADER in headers,
        VOUCHER_HUNTER_HEADER: VOUCHER_HUNTER_HEADER in headers,
        VOUCHER_HUNTER_REASONS_HEADER: VOUCHER_HUNTER_REASONS_HEADER in headers,
    }
    summary["source_columns_present"] = present

    if not present[LINKED_GAMING_ACCOUNTS_HEADER]:
        summary["skipped_reason"] = (
            f"missing required header in user_profile_summary: {LINKED_GAMING_ACCOUNTS_HEADER!r}. "
            "Multi-account risk fields are read verbatim from UIM's pre-computed "
            "cluster columns -- no cluster-detection formula is re-implemented here."
        )
        return [], []

    linked_idx = headers.index(LINKED_GAMING_ACCOUNTS_HEADER)
    count_idx = headers.index(LINKED_TG_COUNT_HEADER) if present[LINKED_TG_COUNT_HEADER] else None
    member_idx = headers.index(CLUSTER_MEMBER_HEADER) if present[CLUSTER_MEMBER_HEADER] else None
    vh_idx = headers.index(VOUCHER_HUNTER_HEADER) if present[VOUCHER_HUNTER_HEADER] else None
    reasons_idx = headers.index(VOUCHER_HUNTER_REASONS_HEADER) if present[VOUCHER_HUNTER_REASONS_HEADER] else None

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

        linked_accounts = _parse_list(row[linked_idx] if len(row) > linked_idx else "")
        linked_tg_count = _parse_int(row[count_idx] if count_idx is not None and len(row) > count_idx else "")
        cluster_member = _parse_bool(row[member_idx] if member_idx is not None and len(row) > member_idx else "")
        voucher_hunter = _parse_bool(row[vh_idx] if vh_idx is not None and len(row) > vh_idx else "")
        reasons = _parse_list(row[reasons_idx] if reasons_idx is not None and len(row) > reasons_idx else "")

        summary["valid_user_ids"] += 1
        updates.append(
            {
                "user_id": user_id,
                "linked_gaming_accounts": linked_accounts,
                "linked_tg_count": linked_tg_count,
                "multi_account_cluster_member": cluster_member,
                "multi_account_voucher_hunter": voucher_hunter,
                "voucher_hunter_reasons": reasons,
                "set": {
                    "linked_gaming_accounts": linked_accounts,
                    "linked_tg_count": linked_tg_count,
                    "multi_account_cluster_member": cluster_member,
                    "multi_account_risk": cluster_member,
                    "multi_account_voucher_hunter": voucher_hunter,
                    "voucher_hunter_reasons": reasons,
                    "multi_account_risk_source": "UIM",
                    "multi_account_risk_synced_at": now,
                },
            }
        )
        if user_id not in seen:
            seen.add(user_id)
            user_ids.append(user_id)

        for account_id in linked_accounts:
            cluster = summary["clusters"].setdefault(
                account_id, {"member_user_ids": [], "reported_linked_tg_count": linked_tg_count}
            )
            cluster["member_user_ids"].append(user_id)
            if linked_tg_count is not None:
                cluster["reported_linked_tg_count"] = linked_tg_count

    return updates, user_ids


def sync_multi_account_risk_from_sheet(
    *,
    dry_run: bool = True,
    users_col=None,
    rows: list[list[Any]] | None = None,
    spreadsheet_id: str | None = None,
    worksheet_gid: str | None = None,
    preview_limit: int = 50,
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

        # Resolve (and init_db() if needed) BEFORE parsing rows, same fix as
        # bot_segment_sync.py: no Mongo-backed lookup may run before init_db().
        users_col = _resolve_users_collection(users_col)

        updates, user_ids = _parse_rows(rows, summary, now=now)

        if summary["skipped_reason"]:
            summary["ok"] = True
            logger.info("[MULTI_ACCOUNT_RISK_SYNC] skipped reason=%s", summary["skipped_reason"])
            return summary

        existing = _existing_users_with_risk(users_col, user_ids) if user_ids else {}
        summary["users_missing_in_db"] = max(0, len(set(user_ids)) - len(existing))
        matched = [item for item in updates if item["user_id"] in existing]
        summary["users_matched"] = len(matched)
        summary["canonical_segment_users_matched"] = sum(
            1 for item in matched if existing.get(item["user_id"], {}).get("for_bot_segment")
        )
        summary["risk_members_matched"] = sum(1 for item in matched if item["multi_account_cluster_member"])

        preview = []
        for item in matched:
            user_id = item["user_id"]
            current = existing.get(user_id, {})
            current_risk = bool(current.get("multi_account_risk"))
            new_risk = item["multi_account_cluster_member"]
            if new_risk and not current_risk:
                summary["users_to_set_risk_true"] += 1
            if current_risk and not new_risk:
                summary["users_to_clear_stale_risk"] += 1

            current_accounts = set(current.get("linked_gaming_accounts") or [])
            new_accounts = set(item["linked_gaming_accounts"])
            summary["linked_accounts_to_add"] += len(new_accounts - current_accounts)
            summary["linked_accounts_to_prune"] += len(current_accounts - new_accounts)

            if item["multi_account_cluster_member"] and len(preview) < preview_limit:
                preview.append(
                    {
                        "user_id": user_id,
                        "for_bot_segment": current.get("for_bot_segment"),
                        "multi_account_risk_current": current_risk,
                        "multi_account_risk_new": new_risk,
                        "linked_tg_count": item["linked_tg_count"],
                        "linked_gaming_accounts": item["linked_gaming_accounts"],
                        "voucher_hunter_reasons": item["voucher_hunter_reasons"],
                    }
                )
        summary["preview"] = preview

        # Canonical segment fields are never touched by this sync -- proof,
        # not just intent: none of the $set payloads built above contain them.
        segment_fields = {"for_bot_segment", "for_bot_segment_normalized", "bot_segment_source", "bot_segment_synced_at"}
        summary["canonical_segment_updates"] = sum(
            1 for item in matched if segment_fields & set(item["set"].keys())
        )

        if dry_run or not matched:
            summary["ok"] = True
            logger.info("[MULTI_ACCOUNT_RISK_SYNC] dry_run_summary=%s", {k: v for k, v in summary.items() if k not in ("clusters", "preview")})
            return summary

        summary["users_write_attempted"] = len(matched)
        ops = [UpdateOne({"user_id": item["user_id"]}, {"$set": item["set"]}, upsert=False) for item in matched]
        modified = 0
        for batch in _chunks(ops, BATCH_SIZE):
            result = users_col.bulk_write(batch, ordered=False)
            modified += int(getattr(result, "modified_count", 0) or 0)
        summary["users_modified"] = modified
        summary["writes_performed"] = modified > 0
        summary["ok"] = True
        logger.info("[MULTI_ACCOUNT_RISK_SYNC] commit_summary=%s", {k: v for k, v in summary.items() if k not in ("clusters", "preview")})
        return summary
    except (RuntimeError, ValueError, PyMongoError) as exc:
        summary["error"] = str(exc)
        logger.error("[MULTI_ACCOUNT_RISK_SYNC] failed err=%s", str(exc))
        return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync multi-account risk fields from UIM sheet into Mongo users")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Read and report only; do not write MongoDB")
    mode.add_argument("--commit", action="store_true", help="Write matched existing users")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    summary = sync_multi_account_risk_from_sheet(dry_run=not args.commit)
    print(json.dumps(summary, default=str, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
