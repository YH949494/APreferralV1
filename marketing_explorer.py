"""Phase 2B — Marketing Data Validation & Raw Data Explorer (read-only).

Provides aggregation-based exploration of the ``marketing_raw_data`` collection
so admins can validate uploaded data before any segment calculation occurs.

This module NEVER:
  - Calculates segments.
  - Writes to ``users.bot_segment`` / ``for_bot_segment`` / ``bot_segment_probability``.
  - Modifies bot behaviour, voucher allocation, or reward logic.
  - Writes to any collection (read-only).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import database

logger = logging.getLogger(__name__)

# --- pipeline building helpers ---

def _to_num(field: str) -> dict:
    """MongoDB aggregation expression that converts a field to double (0 on error/null)."""
    return {"$convert": {"input": f"${field}", "to": "double", "onError": 0, "onNull": 0}}


def _is_missing_filter(field: str) -> dict:
    """MongoDB $match filter: field is absent, null, or empty string."""
    return {"$or": [{field: {"$exists": False}}, {field: None}, {field: ""}]}


# --- snapshot resolution ---

def _resolve_snapshot(
    marketing_col,
    *,
    snapshot_week: str | None,
    snapshot_month: str | None,
    period_type: str | None = None,
    period: str | None = None,
) -> tuple[dict, dict]:
    """Return (match_filter, snapshot_info).

    Precedence: snapshot_week > snapshot_month > auto-detect latest week.
    Returns empty filter when the collection is empty (no data uploaded yet).
    """
    if period_type == "weekly" and period:
        snapshot_week = period
        snapshot_month = None
    elif period_type == "monthly" and period:
        snapshot_month = period
        snapshot_week = None

    if snapshot_week:
        return (
            {"snapshot_week": snapshot_week},
            {"snapshot_week": snapshot_week, "snapshot_month": None, "period_type": "weekly", "period": snapshot_week},
        )
    if snapshot_month:
        return (
            {"snapshot_month": snapshot_month},
            {"snapshot_week": None, "snapshot_month": snapshot_month, "period_type": "monthly", "period": snapshot_month},
        )
    latest = marketing_col.find_one(
        {},
        sort=[("snapshot_week", -1)],
        projection={"snapshot_week": 1, "snapshot_month": 1},
    )
    if not latest:
        return {}, {"snapshot_week": None, "snapshot_month": None, "period_type": "weekly", "period": None}
    week = latest.get("snapshot_week")
    month = latest.get("snapshot_month")
    return (
        {"snapshot_week": week},
        {"snapshot_week": week, "snapshot_month": month, "period_type": "weekly", "period": week},
    )


# --- summary aggregation ---

def _run_summary(marketing_col, match_filter: dict) -> dict:
    """Run summary aggregation using $facet for a single collection pass."""
    pipeline = [
        {"$match": match_filter},
        {"$facet": {
            "totals": [
                {"$group": {
                    "_id": None,
                    "rows_total": {"$sum": 1},
                    "total_withdraw": {"$sum": _to_num("withdraw_amount")},
                    "total_after_bet": {"$sum": _to_num("after_total_bet_amount")},
                    "new_players": {"$sum": {
                        "$cond": [{"$in": ["$is_new_player", [1, "1", True]]}, 1, 0]
                    }},
                }}
            ],
            "distinct_accounts": [
                {"$group": {"_id": "$account"}},
                {"$count": "n"},
            ],
            "distinct_campaigns": [
                {"$group": {"_id": "$campaign_id"}},
                {"$count": "n"},
            ],
            "distinct_platforms": [
                {"$group": {"_id": "$platform_code"}},
                {"$count": "n"},
            ],
            "distinct_currencies": [
                {"$group": {"_id": "$currency_code"}},
                {"$count": "n"},
            ],
        }},
    ]
    result = list(marketing_col.aggregate(pipeline))
    if not result:
        return {
            "rows_total": 0,
            "distinct_accounts": 0,
            "campaign_count": 0,
            "platform_count": 0,
            "currency_count": 0,
            "new_players": 0,
            "total_withdraw_amount": 0.0,
            "total_after_bet_amount": 0.0,
        }
    facet = result[0]

    def _first(key: str, field: str, default: Any = 0) -> Any:
        bucket = facet.get(key, [])
        return bucket[0].get(field, default) if bucket else default

    totals = facet.get("totals", [{}])
    t = totals[0] if totals else {}
    return {
        "rows_total": t.get("rows_total", 0),
        "distinct_accounts": _first("distinct_accounts", "n"),
        "campaign_count": _first("distinct_campaigns", "n"),
        "platform_count": _first("distinct_platforms", "n"),
        "currency_count": _first("distinct_currencies", "n"),
        "new_players": t.get("new_players", 0),
        "total_withdraw_amount": round(t.get("total_withdraw", 0.0) or 0.0, 2),
        "total_after_bet_amount": round(t.get("total_after_bet", 0.0) or 0.0, 2),
    }


# --- breakdown aggregations ---

def _breakdown_pipeline(match_filter: dict, group_id: dict, sort_field: str) -> list[dict]:
    """Two-stage group pipeline for (campaign|platform|currency) breakdown.

    First stage groups by (group_id + account) to get distinct accounts per group;
    second stage rolls up to the desired dimension. Avoids $addToSet which can
    use excessive memory on large datasets.
    """
    stage1_id = dict(group_id)
    stage1_id["account"] = "$account"
    return [
        {"$match": match_filter},
        {"$group": {
            "_id": stage1_id,
            "rows": {"$sum": 1},
            "withdraw_amount": {"$sum": _to_num("withdraw_amount")},
            "after_total_bet_amount": {"$sum": _to_num("after_total_bet_amount")},
        }},
        {"$group": {
            "_id": {k: f"$_id.{k}" for k in group_id},
            "rows": {"$sum": "$rows"},
            "accounts": {"$sum": 1},
            "withdraw_amount": {"$sum": "$withdraw_amount"},
            "after_total_bet_amount": {"$sum": "$after_total_bet_amount"},
        }},
        {"$sort": {sort_field: -1}},
    ]


def _run_campaign_breakdown(marketing_col, match_filter: dict) -> list[dict]:
    pipeline = _breakdown_pipeline(
        match_filter,
        group_id={"campaign_id": "$campaign_id", "campaign_name": "$campaign_name"},
        sort_field="after_total_bet_amount",
    )
    out = []
    for doc in marketing_col.aggregate(pipeline):
        id_ = doc.get("_id") or {}
        out.append({
            "campaign_id": id_.get("campaign_id"),
            "campaign_name": id_.get("campaign_name"),
            "rows": doc.get("rows", 0),
            "accounts": doc.get("accounts", 0),
            "withdraw_amount": round(doc.get("withdraw_amount", 0.0) or 0.0, 2),
            "after_total_bet_amount": round(doc.get("after_total_bet_amount", 0.0) or 0.0, 2),
        })
    return out


def _run_platform_breakdown(marketing_col, match_filter: dict) -> list[dict]:
    pipeline = _breakdown_pipeline(
        match_filter,
        group_id={"platform_code": "$platform_code"},
        sort_field="after_total_bet_amount",
    )
    out = []
    for doc in marketing_col.aggregate(pipeline):
        id_ = doc.get("_id") or {}
        out.append({
            "platform_code": id_.get("platform_code"),
            "rows": doc.get("rows", 0),
            "accounts": doc.get("accounts", 0),
            "withdraw_amount": round(doc.get("withdraw_amount", 0.0) or 0.0, 2),
            "after_total_bet_amount": round(doc.get("after_total_bet_amount", 0.0) or 0.0, 2),
        })
    return out


def _run_currency_breakdown(marketing_col, match_filter: dict) -> list[dict]:
    pipeline = _breakdown_pipeline(
        match_filter,
        group_id={"currency_code": "$currency_code"},
        sort_field="after_total_bet_amount",
    )
    out = []
    for doc in marketing_col.aggregate(pipeline):
        id_ = doc.get("_id") or {}
        out.append({
            "currency_code": id_.get("currency_code"),
            "rows": doc.get("rows", 0),
            "accounts": doc.get("accounts", 0),
            "withdraw_amount": round(doc.get("withdraw_amount", 0.0) or 0.0, 2),
            "after_total_bet_amount": round(doc.get("after_total_bet_amount", 0.0) or 0.0, 2),
        })
    return out


# --- upload snapshot summary ---

def _contains_period(batch: dict, *, snapshot_week: str | None, snapshot_month: str | None) -> bool:
    if snapshot_week:
        weeks = (batch.get("coverage") or {}).get("weeks") or []
        return batch.get("snapshot_week") == snapshot_week or snapshot_week in weeks
    if snapshot_month:
        months = (batch.get("coverage") or {}).get("months") or []
        return batch.get("snapshot_month") == snapshot_month or snapshot_month in months
    return True


def _period_count(batch: dict, *, snapshot_week: str | None, snapshot_month: str | None) -> int:
    if snapshot_week:
        return int((batch.get("rows_by_snapshot_week") or {}).get(snapshot_week, 0) or 0)
    if snapshot_month:
        return int((batch.get("rows_by_snapshot_month") or {}).get(snapshot_month, 0) or 0)
    return int(batch.get("rows_imported", batch.get("rows_total", 0)) or 0)


def _run_snapshot_summary(batches_col, *, snapshot_week: str | None, snapshot_month: str | None) -> list[dict]:
    """Return upload batch rows relevant to the current snapshot filter."""
    filt: dict = {}

    rows = []
    cursor = batches_col.find(filt).sort("uploaded_at", -1).limit(50)
    for b in cursor:
        if not _contains_period(b, snapshot_week=snapshot_week, snapshot_month=snapshot_month):
            continue
        uploaded_at = b.get("uploaded_at")
        if hasattr(uploaded_at, "isoformat"):
            uploaded_at = uploaded_at.isoformat()
        rows.append({
            "snapshot_week": b.get("snapshot_week"),
            "snapshot_month": b.get("snapshot_month"),
            "coverage": b.get("coverage") or {},
            "rows_by_snapshot_month": b.get("rows_by_snapshot_month") or {},
            "rows_by_snapshot_week": b.get("rows_by_snapshot_week") or {},
            "rows": _period_count(b, snapshot_week=snapshot_week, snapshot_month=snapshot_month),
            "rows_imported": b.get("rows_imported", b.get("rows_total", 0)),
            "accounts": None,
            "campaigns": None,
            "uploaded_at": uploaded_at,
            "file_name": b.get("file_name"),
            "status": b.get("status"),
        })
    return rows


def _available_periods(marketing_col) -> dict:
    def _distinct(field: str) -> list[str]:
        if not hasattr(marketing_col, "distinct"):
            return []
        try:
            return sorted([str(v) for v in marketing_col.distinct(field) if v], reverse=True)
        except TypeError:
            return sorted([str(v) for v in marketing_col.distinct(field, {}) if v], reverse=True)

    return {
        "weekly": _distinct("snapshot_week"),
        "monthly": _distinct("snapshot_month"),
    }


# --- data quality ---

def _quality_status(count: int, total: int) -> str:
    if count == 0 or total == 0:
        return "green"
    pct = count / total
    return "red" if pct >= 0.05 else "yellow"


def _quality_pct(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(count / total * 100, 2)


def _run_data_quality(marketing_col, match_filter: dict) -> dict:
    """Run all data quality checks in a single $facet aggregation pass."""
    def _missing_stage(field: str) -> list[dict]:
        return [
            {"$match": _is_missing_filter(field)},
            {"$count": "n"},
        ]

    pipeline = [
        {"$match": match_filter},
        {"$facet": {
            "total": [{"$count": "n"}],
            "missing_account": _missing_stage("account"),
            "missing_campaign_id": _missing_stage("campaign_id"),
            "missing_platform_code": _missing_stage("platform_code"),
            "missing_currency_code": _missing_stage("currency_code"),
            "missing_withdraw_amount": _missing_stage("withdraw_amount"),
            "missing_after_bet_amount": _missing_stage("after_total_bet_amount"),
            "dup_accounts": [
                {"$group": {"_id": "$account", "cnt": {"$sum": 1}}},
                {"$match": {"cnt": {"$gt": 1}}},
                {"$group": {"_id": None, "n": {"$sum": {"$subtract": ["$cnt", 1]}}}},
            ],
            "dup_campaign_entries": [
                {"$group": {
                    "_id": {"account": "$account", "campaign_id": "$campaign_id"},
                    "cnt": {"$sum": 1},
                }},
                {"$match": {"cnt": {"$gt": 1}}},
                {"$group": {"_id": None, "n": {"$sum": {"$subtract": ["$cnt", 1]}}}},
            ],
        }},
    ]

    result = list(marketing_col.aggregate(pipeline))
    if not result:
        return _empty_quality()

    facet = result[0]

    def _n(key: str) -> int:
        bucket = facet.get(key, [])
        return (bucket[0].get("n", 0) if bucket else 0) or 0

    total = _n("total")
    checks_raw = {
        "missing_account": _n("missing_account"),
        "missing_campaign_id": _n("missing_campaign_id"),
        "missing_platform_code": _n("missing_platform_code"),
        "missing_currency_code": _n("missing_currency_code"),
        "missing_withdraw_amount": _n("missing_withdraw_amount"),
        "missing_after_bet_amount": _n("missing_after_bet_amount"),
        "duplicate_accounts": _n("dup_accounts"),
        "duplicate_campaign_entries": _n("dup_campaign_entries"),
    }

    checks: dict[str, Any] = {}
    worst = "green"
    for name, count in checks_raw.items():
        status = _quality_status(count, total)
        checks[name] = {
            "count": count,
            "pct": _quality_pct(count, total),
            "status": status,
        }
        if status == "red" or (status == "yellow" and worst == "green"):
            worst = status

    return {
        "total_rows": total,
        "checks": checks,
        "overall_status": worst,
    }


def _empty_quality() -> dict:
    keys = [
        "missing_account", "missing_campaign_id", "missing_platform_code",
        "missing_currency_code", "missing_withdraw_amount", "missing_after_bet_amount",
        "duplicate_accounts", "duplicate_campaign_entries",
    ]
    return {
        "total_rows": 0,
        "checks": {k: {"count": 0, "pct": 0.0, "status": "green"} for k in keys},
        "overall_status": "green",
    }


# --- main entry point ---

def get_raw_explorer(
    *,
    snapshot_week: str | None = None,
    snapshot_month: str | None = None,
    period_type: str | None = None,
    period: str | None = None,
    marketing_col=None,
    batches_col=None,
) -> dict:
    """Return the full raw-explorer payload for the admin dashboard.

    Uses aggregation pipelines throughout — never loads all rows into memory.
    Read-only: does not write to any collection.

    Args:
        snapshot_week: ISO week filter e.g. ``"2024-W20"``. Auto-detects latest if omitted.
        snapshot_month: ISO month filter e.g. ``"2024-05"``. Lower priority than snapshot_week.
        marketing_col: ``marketing_raw_data`` collection (injected for testing).
        batches_col: ``marketing_upload_batches`` collection (injected for testing).
    """
    if marketing_col is None:
        database.init_db()
        marketing_col = database.marketing_raw_data_col
    if batches_col is None:
        batches_col = database.marketing_upload_batches_col

    match_filter, snapshot_info = _resolve_snapshot(
        marketing_col,
        snapshot_week=snapshot_week,
        snapshot_month=snapshot_month,
        period_type=period_type,
        period=period,
    )

    summary = _run_summary(marketing_col, match_filter)
    campaign_breakdown = _run_campaign_breakdown(marketing_col, match_filter)
    platform_breakdown = _run_platform_breakdown(marketing_col, match_filter)
    currency_breakdown = _run_currency_breakdown(marketing_col, match_filter)
    snapshot_summary = _run_snapshot_summary(
        batches_col,
        snapshot_week=snapshot_info.get("snapshot_week"),
        snapshot_month=snapshot_info.get("snapshot_month"),
    )
    data_quality = _run_data_quality(marketing_col, match_filter)

    return {
        "summary": summary,
        "campaign_breakdown": campaign_breakdown,
        "platform_breakdown": platform_breakdown,
        "currency_breakdown": currency_breakdown,
        "snapshot_summary": snapshot_summary,
        "data_quality": data_quality,
        "snapshot_filter": snapshot_info,
        "available_periods": _available_periods(marketing_col),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_source": "marketing_raw_data",
    }
