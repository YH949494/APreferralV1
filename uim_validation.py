"""Read-only helpers for the Phase 5 "UIM vs Backend Validation" panel.

Reuses the same Google Sheet, credentials and row-fetch helpers as
``bot_segment_sync.py`` (no new auth pattern, no new spreadsheet). The
primary UIM KPI source is the **"dashboard"** tab (selected by title, not
gid — gid=1495862202 on this spreadsheet is "campaign_roi", a different
tab kept here only as a secondary/future reference, not used by Phase 5).

The "dashboard" tab layout is a flat row list with:
  Column A = row type   (only rows where this is "KPI" are KPI metrics)
  Column B = metric label
  Column C = value
  Column D = notes

Never writes anywhere; never touches bot/segment/voucher/reward logic.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import bot_segment_sync

logger = logging.getLogger(__name__)

DEFAULT_VALIDATION_SHEET_ID = bot_segment_sync.DEFAULT_BOT_SEGMENT_SHEET_ID

# Primary Phase 5 KPI source: the "dashboard" tab, selected by title.
DEFAULT_VALIDATION_SHEET_TAB = "dashboard"

# campaign_roi (gid=1495862202) is a different tab on the same spreadsheet.
# Not the Phase 5 KPI source — kept only so a future "campaign quality"
# mapping doesn't have to rediscover the gid.
CAMPAIGN_ROI_SHEET_GID = "1495862202"

_KPI_ROW_TYPE = "kpi"

# Canonical metric key for each confirmed UIM "dashboard" tab KPI label.
# METRIC_KEYS preserves this order (used for the comparison table row order).
_LABEL_TO_KEY: dict[str, str] = {
    "Total Campaign Players": "total_campaign_players",
    "New Users": "new_users",
    "Voucher Claimer Accounts": "voucher_claimer_accounts",
    "Total Claims": "total_claims",
    "Medium Risk Claim Accounts": "medium_risk_claim_accounts",
    "High Risk Claim Accounts": "high_risk_claim_accounts",
    "Abuse / Freeze Claim Accounts": "abuse_freeze_claim_accounts",
    "Actual Players": "actual_players",
    "High Value Players": "high_value_players",
    "New Player Total": "new_player_total",
    "Old Player Total": "old_player_total",
    "Welcome Abuse Invitees": "welcome_abuse_invitees",
    "High Risk Welcome Abuse": "high_risk_welcome_abuse",
    "Self/Farming Risk Invitees": "self_farming_risk_invitees",
}

METRIC_KEYS = list(_LABEL_TO_KEY.values())


def _normalize_label(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    value = re.sub(r"[\s\-/]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


# Built from _LABEL_TO_KEY so the alias map can never drift from METRIC_KEYS.
_METRIC_LABEL_ALIASES: dict[str, str] = {
    _normalize_label(label): key for label, key in _LABEL_TO_KEY.items()
}


def _parse_numeric(raw: Any) -> float | None:
    text = str(raw if raw is not None else "").strip()
    if not text:
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_uim_kpi_rows(rows: list[list[Any]]) -> tuple[dict[str, float], dict[str, str]]:
    """Parse the "dashboard" tab's KPI rows into ``(values, notes)``.

    Only rows whose Column A is "KPI" are considered; Column B is the
    metric label, Column C the value, Column D an optional note. Rows with
    an unrecognized label or a non-numeric value are skipped rather than
    raising — a partially-readable sheet still yields whatever it can.
    """
    values: dict[str, float] = {}
    notes: dict[str, str] = {}
    for row in rows:
        if not row:
            continue
        row_type = row[0] if len(row) > 0 else ""
        if _normalize_label(row_type) != _KPI_ROW_TYPE:
            continue
        label_raw = row[1] if len(row) > 1 else ""
        value_raw = row[2] if len(row) > 2 else ""
        note_raw = row[3] if len(row) > 3 else ""
        key = _METRIC_LABEL_ALIASES.get(_normalize_label(label_raw))
        if key is None:
            continue
        parsed = _parse_numeric(value_raw)
        if parsed is None:
            continue
        values[key] = parsed
        note_text = str(note_raw or "").strip()
        if note_text:
            notes[key] = note_text
    return values, notes


def fetch_uim_validation_metrics(
    *,
    spreadsheet_id: str | None = None,
    worksheet_title: str | None = None,
    rows: list[list[Any]] | None = None,
) -> dict:
    """Fetch + parse the UIM "dashboard" KPI tab. Never raises.

    Returns ``{"ok": bool, "error": str | None, "values": dict[str, float],
    "notes": dict[str, str], "spreadsheet_id": str, "worksheet_title": str}``.
    On any sheet/credential failure this degrades to ``ok=False`` with an
    empty ``values`` dict rather than crashing the caller.
    """
    # Reuse the same configured-sheet fallback as bot_segment_sync's weekly
    # UIM sync (env var first, hardcoded default only as a last resort) so
    # staging/prod deployments pointed at a non-default UIM sheet compare
    # against the *same* spreadsheet the sync job reads, not always the
    # hardcoded default.
    spreadsheet_id = spreadsheet_id or os.getenv("BOT_SEGMENT_SHEET_ID", DEFAULT_VALIDATION_SHEET_ID)
    worksheet_title = worksheet_title or DEFAULT_VALIDATION_SHEET_TAB
    try:
        if rows is None:
            rows = bot_segment_sync.fetch_sheet_rows_by_title(
                spreadsheet_id=spreadsheet_id, worksheet_title=worksheet_title
            )
        values, notes = parse_uim_kpi_rows(rows)
        return {
            "ok": True,
            "error": None,
            "values": values,
            "notes": notes,
            "spreadsheet_id": spreadsheet_id,
            "worksheet_title": worksheet_title,
        }
    except Exception as exc:  # noqa: BLE001 - any sheet/credential failure degrades gracefully
        logger.warning("[UIM_VALIDATION] fetch_failed err=%s", exc)
        return {
            "ok": False,
            "error": str(exc),
            "values": {},
            "notes": {},
            "spreadsheet_id": spreadsheet_id,
            "worksheet_title": worksheet_title,
        }
