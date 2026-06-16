"""Phase 2A — Weekly Marketing Raw Data Upload (ingestion only).

APReferral is moving away from UIM as the production source of truth.
Marketing will upload a weekly CSV/XLSX export of player performance data;
this module stores the *entire* uploaded dataset verbatim into
``marketing_raw_data`` (one document per row) plus an audit row per upload
into ``marketing_upload_batches``.

This phase is data ingestion only:
  - No segment is calculated from this data here.
  - Nothing is written to ``users.bot_segment`` / ``for_bot_segment`` /
    ``bot_segment_probability``.
  - Bot behaviour, voucher allocation, and reward logic are untouched.
  - The existing UIM sync (``bot_segment_sync.py`` / ``claim_risk_sync.py``)
    keeps running exactly as before; this is a separate, additive data
    source for a future segment engine (see ``backend_segment_engine.py``)
    to read from once it's ready to use it.

Storage shape
-------------
Every column present in the uploaded file is stored verbatim on the
``marketing_raw_data`` document (the importer never drops a column it
doesn't recognise — "Additional columns may appear in future" per spec).
On top of the raw columns, each document gets:
  - ``upload_batch_id``, ``snapshot_week``, ``snapshot_month``
  - ``dedupe_key`` — the uniqueness key described below
  - ``uploaded_at``, ``uploaded_by``, ``source="manual_upload"``

Weekly snapshots are never overwritten: every upload is its own dataset,
keyed by the ISO week it was uploaded in. Re-running the *same* file in the
same week does not duplicate rows, enforced via a unique index on
``dedupe_key`` (``database.ensure_indexes``) — see ``_build_dedupe_key``.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pymongo.errors import BulkWriteError, PyMongoError

import database

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB

# Only these are required to do anything useful with a row; everything else
# (withdraw_amount, after_total_bet_amount, coupon_code, ...) is optional —
# "Do NOT require every optional field" per spec.
REQUIRED_COLUMNS = ("campaign_id", "campaign_name", "account")

# Stable business key for de-duplicating a re-uploaded weekly file. Falls
# back to empty-string components rather than skipping the row entirely —
# a missing coupon_code is still a meaningful (and dedupe-able) row.
DEDUPE_KEY_FIELDS = ("account", "campaign_id", "coupon_code")


def _normalize_header(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _row_value(row: dict, field: str) -> str:
    return str(row.get(field) or "").strip()


def _row_value_ci(row_lc: dict, field: str) -> str:
    """Look up ``field`` case-insensitively via a pre-lowered key map.

    Exports may use uppercase/mixed-case headers (e.g. ``CAMPAIGN_ID``);
    ``validate_required_columns`` already accepts those case-insensitively,
    so required-field/dedupe-key lookups must match that or every row gets
    wrongly counted as failed.
    """
    return str(row_lc.get(field.lower()) or "").strip()


def _lower_key_map(row: dict) -> dict:
    return {str(k).lower(): v for k, v in row.items()}


def _snapshot_week_key(moment: datetime) -> str:
    iso_year, iso_week, _ = moment.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def _snapshot_month_key(moment: datetime) -> str:
    return f"{moment.year:04d}-{moment.month:02d}"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def validate_file_name(file_name: str) -> str:
    """Return the lowercase extension, or raise ``ValueError`` if unsupported."""
    _, ext = os.path.splitext(file_name or "")
    ext = ext.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(
            f"unsupported file type {ext or '(none)'!r}: expected one of {sorted(ALLOWED_EXTENSIONS)}"
        )
    return ext


def validate_file_size(size_bytes: int) -> None:
    if size_bytes > MAX_FILE_SIZE_BYTES:
        raise ValueError(
            f"file exceeds maximum size of {MAX_FILE_SIZE_BYTES // (1024 * 1024)}MB "
            f"(got {size_bytes / (1024 * 1024):.1f}MB)"
        )


def parse_csv_bytes(content: bytes) -> tuple[list[str], list[dict]]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    headers = [_normalize_header(h) for h in (reader.fieldnames or [])]
    rows = [{_normalize_header(k): v for k, v in row.items()} for row in reader]
    return headers, rows


def parse_xlsx_bytes(content: bytes) -> tuple[list[str], list[dict]]:
    import openpyxl

    workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    worksheet = workbook.worksheets[0]
    rows_iter = worksheet.iter_rows(values_only=True)
    try:
        header_row = next(rows_iter)
    except StopIteration:
        return [], []
    headers = [_normalize_header(h) for h in header_row]
    rows: list[dict] = []
    for raw_row in rows_iter:
        if raw_row is None or all(cell is None for cell in raw_row):
            continue
        row = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            value = raw_row[idx] if idx < len(raw_row) else None
            row[header] = "" if value is None else value
        rows.append(row)
    return headers, rows


def parse_upload(*, content: bytes, file_name: str) -> tuple[list[str], list[dict]]:
    ext = validate_file_name(file_name)
    if ext == ".csv":
        return parse_csv_bytes(content)
    return parse_xlsx_bytes(content)


def validate_required_columns(headers: list[str]) -> list[str]:
    """Return the list of missing required columns (case-insensitive check)."""
    present = {h.strip().lower() for h in headers}
    return [col for col in REQUIRED_COLUMNS if col.lower() not in present]


def _build_dedupe_key(row_lc: dict, *, snapshot_week: str) -> str:
    parts = [snapshot_week] + [_row_value_ci(row_lc, field).lower() for field in DEDUPE_KEY_FIELDS]
    return "|".join(parts)


# Mongo's $in operand must stay well under the 16MB BSON document limit;
# chunk existing-key lookups so a large-but-valid (near 50MB) upload doesn't
# get rejected by one oversized distinct() query.
_DEDUPE_LOOKUP_CHUNK_SIZE = 5000


def _existing_dedupe_keys(marketing_col, keys: list[str]) -> set[str]:
    existing: set[str] = set()
    for i in range(0, len(keys), _DEDUPE_LOOKUP_CHUNK_SIZE):
        chunk = keys[i : i + _DEDUPE_LOOKUP_CHUNK_SIZE]
        existing.update(marketing_col.distinct("dedupe_key", {"dedupe_key": {"$in": chunk}}))
    return existing


def _empty_summary(*, file_name: str) -> dict:
    return {
        "ok": False,
        "upload_batch_id": None,
        "file_name": file_name,
        "snapshot_week": None,
        "snapshot_month": None,
        "rows_total": 0,
        "rows_imported": 0,
        "rows_failed": 0,
        "duplicate_rows": 0,
        "status": "rejected",
        "error": None,
    }


def ingest_upload(
    *,
    content: bytes,
    file_name: str,
    uploaded_by: str,
    now: datetime | None = None,
    marketing_col=None,
    batches_col=None,
) -> dict:
    """Validate, parse, and store one weekly marketing raw-data upload.

    Never raises for "expected" failure modes (bad file type, oversized
    file, missing required columns) — those are reported in the returned
    summary with ``ok=False`` and a human-readable ``error`` so the admin
    endpoint can render a clean 4xx instead of a 500. Unexpected Mongo
    errors are also caught and reported the same way.
    """
    now = now or _utc_now()
    summary = _empty_summary(file_name=file_name)
    try:
        validate_file_size(len(content))
        headers, rows = parse_upload(content=content, file_name=file_name)

        missing = validate_required_columns(headers)
        if missing:
            summary["error"] = f"missing required columns: {', '.join(missing)}"
            return summary

        if marketing_col is None:
            database.init_db()
            marketing_col = database.marketing_raw_data_col
        if batches_col is None:
            batches_col = database.marketing_upload_batches_col

        snapshot_week = _snapshot_week_key(now)
        snapshot_month = _snapshot_month_key(now)
        upload_batch_id = uuid4().hex

        summary.update(
            {
                "upload_batch_id": upload_batch_id,
                "snapshot_week": snapshot_week,
                "snapshot_month": snapshot_month,
                "rows_total": len(rows),
            }
        )

        candidate_docs: list[dict] = []
        seen_keys_this_batch: set[str] = set()
        rows_failed = 0
        duplicate_rows = 0

        for row in rows:
            row_lc = _lower_key_map(row)
            if any(not _row_value_ci(row_lc, field) for field in REQUIRED_COLUMNS):
                rows_failed += 1
                continue
            dedupe_key = _build_dedupe_key(row_lc, snapshot_week=snapshot_week)
            if dedupe_key in seen_keys_this_batch:
                duplicate_rows += 1
                continue
            seen_keys_this_batch.add(dedupe_key)
            doc = dict(row)
            doc.update(
                {
                    "upload_batch_id": upload_batch_id,
                    "snapshot_week": snapshot_week,
                    "snapshot_month": snapshot_month,
                    "dedupe_key": dedupe_key,
                    "uploaded_at": now,
                    "uploaded_by": uploaded_by,
                    "source": "manual_upload",
                }
            )
            candidate_docs.append(doc)

        if candidate_docs:
            existing_keys = _existing_dedupe_keys(marketing_col, [d["dedupe_key"] for d in candidate_docs])
            insert_docs = []
            for doc in candidate_docs:
                if doc["dedupe_key"] in existing_keys:
                    duplicate_rows += 1
                else:
                    insert_docs.append(doc)

            rows_imported = 0
            if insert_docs:
                try:
                    result = marketing_col.insert_many(insert_docs, ordered=False)
                    rows_imported = len(result.inserted_ids)
                except BulkWriteError as exc:
                    # Race condition (concurrent upload landed the same key
                    # between our existing-key check and the insert) — treat
                    # as a duplicate, not a hard failure.
                    write_errors = exc.details.get("writeErrors", [])
                    duplicate_rows += sum(1 for err in write_errors if err.get("code") == 11000)
                    other_errors = [err for err in write_errors if err.get("code") != 11000]
                    rows_failed += len(other_errors)
                    rows_imported = len(insert_docs) - len(write_errors)
        else:
            rows_imported = 0

        summary["rows_imported"] = rows_imported
        summary["rows_failed"] = rows_failed
        summary["duplicate_rows"] = duplicate_rows
        summary["status"] = "completed" if rows_failed == 0 else "completed_with_errors"

        batch_doc = {
            "upload_batch_id": upload_batch_id,
            "file_name": file_name,
            "snapshot_week": snapshot_week,
            "snapshot_month": snapshot_month,
            "rows_total": summary["rows_total"],
            "rows_imported": rows_imported,
            "rows_failed": rows_failed,
            "duplicate_rows": duplicate_rows,
            "uploaded_by": uploaded_by,
            "uploaded_at": now,
            "status": summary["status"],
        }
        batches_col.insert_one(batch_doc)

        summary["ok"] = True
        logger.info("[MARKETING_UPLOAD] commit_summary=%s", {k: v for k, v in summary.items() if k != "error"})
        return summary
    except (ValueError, PyMongoError) as exc:
        summary["error"] = str(exc)
        logger.error("[MARKETING_UPLOAD] failed file=%s err=%s", file_name, str(exc))
        return summary


def get_upload_history(*, batches_col=None, limit: int = 50) -> list[dict]:
    """Most recent upload batches first, for the "Upload History" admin view."""
    if batches_col is None:
        database.init_db()
        batches_col = database.marketing_upload_batches_col
    cursor = batches_col.find({}).sort("uploaded_at", -1).limit(limit)
    return list(cursor)
