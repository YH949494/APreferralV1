"""Read-only helpers for the Phase 5 "UIM vs Backend Validation" panel.

Reuses the same Google Sheet, credentials and row-fetch helper as
``bot_segment_sync.py`` (no new auth pattern, no new spreadsheet). This
module only reads a different tab (gid) from the same spreadsheet — the
"final KPI/output" tab the operations team already maintains — and parses
its two-column ``metric name | value`` layout into a dict keyed by the
canonical metric keys used by ``dashboard_panels.build_validation_panel``.

Never writes anywhere; never touches bot/segment/voucher/reward logic.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import bot_segment_sync

logger = logging.getLogger(__name__)

DEFAULT_VALIDATION_SHEET_ID = bot_segment_sync.DEFAULT_BOT_SEGMENT_SHEET_ID
DEFAULT_VALIDATION_SHEET_GID = "1495862202"

METRIC_KEYS = [
    "total_campaign_players",
    "voucher_claimers",
    "actual_players",
    "high_value_players",
    "normal_actual_players",
    "low_value_players",
    "voucher_hunters",
    "new_players",
    "old_players",
    "claim_risk",
    "campaign_quality",
    "affiliate_quality",
]

# Maps a normalized sheet label -> canonical metric key. Add aliases here as
# the operations sheet's exact wording is confirmed; unrecognized labels are
# ignored (not an error) so extra KPI rows on the sheet don't break parsing.
_METRIC_LABEL_ALIASES: dict[str, str] = {
    "total_campaign_players": "total_campaign_players",
    "total_campaign_player": "total_campaign_players",
    "campaign_players": "total_campaign_players",
    "voucher_claimers": "voucher_claimers",
    "voucher_claimer": "voucher_claimers",
    "actual_players": "actual_players",
    "actual_player": "actual_players",
    "high_value_players": "high_value_players",
    "high_value_player": "high_value_players",
    "normal_actual_players": "normal_actual_players",
    "normal_actual_player": "normal_actual_players",
    "low_value_players": "low_value_players",
    "low_value_player": "low_value_players",
    "voucher_hunters": "voucher_hunters",
    "voucher_hunter": "voucher_hunters",
    "new_players": "new_players",
    "new_player": "new_players",
    "old_players": "old_players",
    "old_player": "old_players",
    "claim_risk": "claim_risk",
    "campaign_quality": "campaign_quality",
    "affiliate_quality": "affiliate_quality",
}


def _normalize_label(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    value = re.sub(r"[\s\-/]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _parse_numeric(raw: Any) -> float | None:
    text = str(raw if raw is not None else "").strip()
    if not text:
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_uim_kpi_rows(rows: list[list[Any]]) -> dict[str, float]:
    """Parse a two-column ``metric name | value`` KPI tab into metric_key -> value.

    Rows that don't match a known metric label, or whose value isn't
    numeric, are skipped rather than raising — a partially-readable sheet
    should still yield whatever values it can.
    """
    values: dict[str, float] = {}
    for row in rows:
        if not row:
            continue
        label_raw = row[0] if len(row) > 0 else ""
        value_raw = row[1] if len(row) > 1 else ""
        key = _METRIC_LABEL_ALIASES.get(_normalize_label(label_raw))
        if key is None:
            continue
        parsed = _parse_numeric(value_raw)
        if parsed is None:
            continue
        values[key] = parsed
    return values


def fetch_uim_validation_metrics(
    *,
    spreadsheet_id: str | None = None,
    worksheet_gid: str | None = None,
    rows: list[list[Any]] | None = None,
) -> dict:
    """Fetch + parse the UIM KPI tab. Never raises — degrades to ``ok=False``.

    Returns ``{"ok": bool, "error": str | None, "values": dict[str, float],
    "spreadsheet_id": str, "worksheet_gid": str}``.
    """
    spreadsheet_id = spreadsheet_id or DEFAULT_VALIDATION_SHEET_ID
    worksheet_gid = str(worksheet_gid or DEFAULT_VALIDATION_SHEET_GID)
    try:
        if rows is None:
            rows = bot_segment_sync.fetch_sheet_rows(
                spreadsheet_id=spreadsheet_id, worksheet_gid=worksheet_gid
            )
        values = parse_uim_kpi_rows(rows)
        return {
            "ok": True,
            "error": None,
            "values": values,
            "spreadsheet_id": spreadsheet_id,
            "worksheet_gid": worksheet_gid,
        }
    except Exception as exc:  # noqa: BLE001 - any sheet/credential failure degrades gracefully
        logger.warning("[UIM_VALIDATION] fetch_failed err=%s", exc)
        return {
            "ok": False,
            "error": str(exc),
            "values": {},
            "spreadsheet_id": spreadsheet_id,
            "worksheet_gid": worksheet_gid,
        }
