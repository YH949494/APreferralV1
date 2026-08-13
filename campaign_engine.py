"""Segment-Based Campaign Targeting Engine.

``preview_audience`` below queries ``backend_segment_snapshots`` (the
shadow-only classifier from ``backend_segment_engine.py``) purely to render
an informational, multi-dimension audience *estimate* (segment mix,
claim-risk level, referral/check-in counts) in the campaign builder UI. It is
NOT used to determine live campaign eligibility — segment-targeted campaigns
resolve their actual allow-list via
``campaign_builder._resolve_segment_user_ids``, which reads the canonical
segment source (``users.for_bot_segment``, populated only by Databot's
segment_sync_job). Do not wire this preview into eligibility without first
switching it to the canonical source.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

VALID_SEGMENTS = frozenset({
    "high_value",
    "normal_actual",
    "active_community_player",
    "low_value",
    "voucher_hunter",
    "ghost",
    "unclassified",
})

VALID_PLAYER_AGE_TYPES = frozenset({"new_player", "old_player"})

VALID_CLAIM_RISK_LEVELS = frozenset({
    "normal",
    "medium_risk",
    "high_risk_review",
    "abuse_freeze",
})

# Campaign type suggestions per segment, aligned with the product brief.
SEGMENT_CAMPAIGN_SUGGESTIONS: dict[str, dict] = {
    "high_value": {
        "types": ["VIP Campaign", "Exclusive Voucher", "Retention Reward"],
        "exposure": "full",
        "note": "Prioritise retention. High spend justifies premium vouchers.",
    },
    "normal_actual": {
        "types": ["First-Bet Campaign", "Reload Incentive"],
        "exposure": "full",
        "note": "Encourage repeat deposits. Mid-tier voucher value appropriate.",
    },
    "active_community_player": {
        "types": ["Referral Campaign", "XP Campaign", "Community Event"],
        "exposure": "full",
        "note": "Leverage community engagement. Non-monetary rewards work well.",
    },
    "low_value": {
        "types": ["Turnover Improvement Campaign"],
        "exposure": "reduced",
        "note": "Focus on increasing turnover before upgrading voucher exposure.",
    },
    "voucher_hunter": {
        "types": ["Task-Based Reward", "Anti-Abuse Control"],
        "exposure": "minimal",
        "note": "Reduce direct voucher exposure. Gate rewards behind tasks.",
    },
    "ghost": {
        "types": ["Reactivation Campaign"],
        "exposure": "minimal",
        "note": "Re-engage lapsed players. Low-cost first-touch approach.",
    },
    "unclassified": {
        "types": ["Generic Outreach"],
        "exposure": "minimal",
        "note": "Insufficient data. Use broad, low-cost outreach.",
    },
}


def _get_latest_snapshot_week(snapshots_col) -> str | None:
    doc = snapshots_col.find_one(
        {"user_id": {"$ne": None}},
        sort=[("snapshot_week", -1)],
        projection={"snapshot_week": 1, "_id": 0},
    )
    return doc["snapshot_week"] if doc else None


def _build_snapshot_match(filters: dict) -> dict[str, Any]:
    """Convert campaign targeting filters into a MongoDB $match dict."""
    match: dict[str, Any] = {"user_id": {"$ne": None}}

    segments = filters.get("segments") or []
    if segments:
        valid = [s for s in segments if s in VALID_SEGMENTS]
        if valid:
            match["backend_segment"] = {"$in": valid}

    age_types = filters.get("player_age_types") or []
    if age_types:
        valid_a = [a for a in age_types if a in VALID_PLAYER_AGE_TYPES]
        if valid_a:
            match["player_age_type"] = {"$in": valid_a}

    risk_levels = filters.get("claim_risk_levels") or []
    if risk_levels:
        valid_r = [r for r in risk_levels if r in VALID_CLAIM_RISK_LEVELS]
        if valid_r:
            match["claim_risk_level"] = {"$in": valid_r}

    ref_min = filters.get("referral_count_min")
    ref_max = filters.get("referral_count_max")
    if ref_min is not None or ref_max is not None:
        cond: dict = {}
        if ref_min is not None:
            cond["$gte"] = int(ref_min)
        if ref_max is not None:
            cond["$lte"] = int(ref_max)
        match["metrics_snapshot.referral_count"] = cond

    chk_min = filters.get("checkin_count_min")
    chk_max = filters.get("checkin_count_max")
    if chk_min is not None or chk_max is not None:
        cond = {}
        if chk_min is not None:
            cond["$gte"] = int(chk_min)
        if chk_max is not None:
            cond["$lte"] = int(chk_max)
        match["metrics_snapshot.checkin_count"] = cond

    return match


def preview_audience(db, filters: dict, voucher_value: float = 0.0) -> dict:
    """Compute live audience preview for a set of campaign targeting filters.

    Args:
        db: MongoDB database object.
        filters: dict with optional keys —
            segments, player_age_types, claim_risk_levels,
            referral_count_min, referral_count_max,
            checkin_count_min, checkin_count_max,
            activity_recency_days.
        voucher_value: expected MYR value per voucher (for cost estimate).

    Returns:
        dict with audience_size, segment_distribution, expected_voucher_cost,
        snapshot_week, filters_applied.
    """
    snapshots_col = db["backend_segment_snapshots"]
    users_col = db["users"]

    snapshot_week = _get_latest_snapshot_week(snapshots_col)
    if not snapshot_week:
        return {
            "audience_size": 0,
            "segment_distribution": {},
            "expected_voucher_cost": 0.0,
            "snapshot_week": None,
            "filters_applied": filters,
            "warning": "No snapshot data available. Run the backend segment engine first.",
        }

    match = _build_snapshot_match(filters)
    match["snapshot_week"] = snapshot_week

    # Activity recency: match users active within N days.
    # $or covers all three timestamp fields so users are not excluded just
    # because one field is missing (last_checkin_at is written by some flows,
    # last_checkin by the main check-in path, last_active_at by others).
    recency_days = filters.get("activity_recency_days")
    if recency_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(recency_days))
        active_user_ids = [
            u["user_id"]
            for u in users_col.find(
                {
                    "user_id": {"$ne": None},
                    "$or": [
                        {"last_active_at": {"$gte": cutoff}},
                        {"last_checkin": {"$gte": cutoff}},
                        {"last_checkin_at": {"$gte": cutoff}},
                    ],
                },
                projection={"user_id": 1, "_id": 0},
            )
            if u.get("user_id")
        ]
        if not active_user_ids:
            return {
                "audience_size": 0,
                "segment_distribution": {},
                "expected_voucher_cost": 0.0,
                "snapshot_week": snapshot_week,
                "filters_applied": filters,
            }
        existing_uid_filter = match.get("user_id", {})
        if isinstance(existing_uid_filter, dict):
            match["user_id"] = {"$ne": None, "$in": active_user_ids}
        else:
            match["user_id"] = {"$in": active_user_ids}

    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$backend_segment",
                "count": {"$sum": 1},
                "avg_bet": {"$avg": "$metrics_snapshot.after_total_bet_amount"},
                "avg_claims": {"$avg": "$metrics_snapshot.claim_count"},
            }
        },
    ]

    results = list(snapshots_col.aggregate(pipeline))

    segment_distribution: dict[str, dict] = {}
    total = 0
    for row in results:
        seg = row["_id"] or "unclassified"
        count = row["count"]
        segment_distribution[seg] = {
            "count": count,
            "avg_bet_amount": round(row.get("avg_bet") or 0, 2),
            "avg_claim_count": round(row.get("avg_claims") or 0, 2),
            "suggestions": SEGMENT_CAMPAIGN_SUGGESTIONS.get(seg, {}).get("types", []),
            "exposure": SEGMENT_CAMPAIGN_SUGGESTIONS.get(seg, {}).get("exposure", "full"),
        }
        total += count

    for seg_data in segment_distribution.values():
        seg_data["pct"] = round(seg_data["count"] / total * 100, 1) if total else 0

    expected_cost = round(total * float(voucher_value), 2) if voucher_value else 0.0

    return {
        "audience_size": total,
        "segment_distribution": segment_distribution,
        "expected_voucher_cost": expected_cost,
        "snapshot_week": snapshot_week,
        "filters_applied": filters,
    }


def get_historical_performance(db, filters: dict) -> dict:
    """Return aggregate performance metrics from all historical snapshot weeks
    for players matching the given targeting filters.

    Also returns recent past campaigns that targeted the same segments.
    """
    snapshots_col = db["backend_segment_snapshots"]
    campaigns_col = db["campaigns"]

    match = _build_snapshot_match(filters)

    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$backend_segment",
                "unique_players": {"$addToSet": "$user_id"},
                "avg_bet": {"$avg": "$metrics_snapshot.after_total_bet_amount"},
                "avg_claims": {"$avg": "$metrics_snapshot.claim_count"},
                "avg_referrals": {"$avg": "$metrics_snapshot.referral_count"},
                "weeks_with_data": {"$addToSet": "$snapshot_week"},
            }
        },
    ]

    results = list(snapshots_col.aggregate(pipeline))

    segment_performance: dict[str, dict] = {}
    for row in results:
        seg = row["_id"] or "unclassified"
        segment_performance[seg] = {
            "unique_players": len(row.get("unique_players") or []),
            "avg_bet_amount": round(row.get("avg_bet") or 0, 2),
            "avg_claim_count": round(row.get("avg_claims") or 0, 2),
            "avg_referral_count": round(row.get("avg_referrals") or 0, 2),
            "weeks_with_data": len(row.get("weeks_with_data") or []),
            "campaign_suggestions": SEGMENT_CAMPAIGN_SUGGESTIONS.get(seg, {}).get("types", []),
            "note": SEGMENT_CAMPAIGN_SUGGESTIONS.get(seg, {}).get("note", ""),
        }

    # Recent past campaigns overlapping these segments
    segments_filter = filters.get("segments") or []
    past_campaigns: list[dict] = []
    query: dict = {"status": {"$in": ["active", "ended"]}}
    if segments_filter:
        query["targeting.segments"] = {"$in": list(segments_filter)}

    cursor = campaigns_col.find(
        query,
        projection={"name": 1, "status": 1, "targeting": 1, "created_at": 1, "campaign_type": 1},
        sort=[("created_at", -1)],
        limit=5,
    )
    for c in cursor:
        past_campaigns.append({
            "id": str(c["_id"]),
            "name": c.get("name", ""),
            "status": c.get("status", ""),
            "campaign_type": c.get("campaign_type", ""),
            "created_at": c["created_at"].isoformat() if c.get("created_at") else None,
            "targeted_segments": (c.get("targeting") or {}).get("segments", []),
        })

    return {
        "segment_performance": segment_performance,
        "past_campaigns": past_campaigns,
    }
