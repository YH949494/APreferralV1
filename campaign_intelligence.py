"""Campaign Intelligence — Phase 5 (P5).

Read-only recommendation layer sitting on top of Campaign Performance
Intelligence (``campaign_performance.py``, P4). It answers "what campaign
should I run next?" instead of only "what happened?" — but it never
executes, schedules, or mutates anything.

Architecture (see docs/CAMPAIGN_INTELLIGENCE_P5_IMPLEMENTATION.md):

    Campaign -> Campaign Builder -> Campaign Compiler -> Voucher Drops
        -> vouchers.py executes -> Performance Analytics (P4)
        -> Campaign Intelligence (this module)

Campaign Intelligence never executes claims. It does NOT touch:
  - vouchers.py claim engine / FCFS / pooled / personalised claim paths
  - eligibility engine / anti-abuse
  - affiliate settlement / welcome voucher / referral system
  - the scheduler / P3 batch execution

This module issues only find()/count_documents() reads (via
``campaign_performance.py`` helpers) and derives everything else — rankings,
insights, recommendations, segment ROI, best launch time, playbooks — with
pure, deterministic rule-based arithmetic computed on the fly. Nothing is
stored; every response is recomputed from current data.

All routes are GET-only. There is no POST/PUT/PATCH/DELETE in this module.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime

from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request

import campaign_performance as cp
from config import KL_TZ

logger = logging.getLogger(__name__)

campaign_intelligence_bp = Blueprint("campaign_intelligence", __name__)

KNOWN_QUALITY_SEGMENTS = cp.KNOWN_QUALITY_SEGMENTS
WINDOW_CHOICES = cp.WINDOW_CHOICES

RELEASE_STRATEGY_LABELS = {
    None: "immediate",
    "interval_minutes": "every_x_minutes",
    "hourly": "hourly",
    "daily": "daily",
    "weekly": "weekly",
    "manual": "manual",
    "custom": "custom",
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _require_admin():
    return cp._require_admin()


def _safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _non_draft_campaigns(status: str = "all"):
    candidates = list(cp._campaigns_col().find({}, sort=[("created_at", -1)], limit=1000))
    rows = []
    for c in candidates:
        eff = cp._effective_status(c)
        if eff == "draft":
            continue
        if status != "all" and eff != status:
            continue
        rows.append(c)
    return rows


def _actual_player_pct(quality: dict) -> float | None:
    total = sum(quality.get(s, 0) for s in KNOWN_QUALITY_SEGMENTS)
    if not total:
        return None
    return round(100.0 * (quality.get("high_value", 0) + quality.get("normal_actual", 0)) / total, 1)


def _conversion_pct(conversion: dict, claimed_count: int) -> float | None:
    """Deterministic conversion-rate proxy: share of claimants who took a
    post-claim action (qualified a referral, made a referral, or checked in).
    Same formula everywhere in this module so numbers stay comparable across
    rankings / templates / releases / segments."""
    if not claimed_count:
        return None
    q = conversion.get("qualified_after_claim")
    r = conversion.get("referral_after_claim")
    ci = conversion.get("checkin_after_claim")
    q = q if isinstance(q, int) else 0
    r = r if isinstance(r, int) else 0
    ci = ci if isinstance(ci, int) else 0
    pct = 100.0 * (q + r + ci) / claimed_count
    return round(min(pct, 100.0), 1)


def _speed_score(avg_claim_speed_minutes) -> float:
    """0-100, higher = faster claim velocity. None (no data) is neutral (50)
    so missing speed data never swings a ranking hard in either direction."""
    if avg_claim_speed_minutes is None:
        return 50.0
    if avg_claim_speed_minutes <= 30:
        return 100.0
    if avg_claim_speed_minutes >= 1440:
        return 0.0
    return round(100.0 - (avg_claim_speed_minutes - 30) * 100.0 / 1410.0, 1)


def _grade(pct: float | None) -> str:
    if pct is None:
        return "N/A"
    if pct >= 25:
        return "A"
    if pct >= 15:
        return "B"
    if pct >= 5:
        return "C"
    return "F"


def _release_strategy_label(campaign: dict) -> str:
    rt = campaign.get("release_type")
    return RELEASE_STRATEGY_LABELS.get(rt, rt or "immediate")


def enrich_performance(full: dict) -> dict:
    """Adds the P5-specific derived metrics to a P4 performance dict without
    mutating P4's own fields. Pure function of already-computed P4 output."""
    claimed = full["volume"]["total_claimed"]
    actual_player_pct = _actual_player_pct(full["quality"])
    voucher_hunter_pct = full["abuse_risk"]["voucher_hunter_claim_share_pct"]
    conversion_pct = _conversion_pct(full["conversion_proxy"], claimed)
    speed_score = _speed_score(full["speed"]["average_claim_speed_minutes"])
    campaign_score_norm = _clamp((full["campaign_score"] + 100) / 2.0, 0, 100)

    ranking_score = round(
        0.35 * campaign_score_norm
        + 0.20 * (full["volume"]["claim_rate"] or 0)
        + 0.15 * (actual_player_pct or 0)
        + 0.15 * (conversion_pct or 0)
        + 0.15 * speed_score
        - 0.20 * (voucher_hunter_pct or 0),
        1,
    )

    full["actual_player_pct"] = actual_player_pct
    full["voucher_hunter_pct"] = voucher_hunter_pct
    full["conversion_pct"] = conversion_pct
    full["speed_score"] = speed_score
    full["ranking_score"] = ranking_score
    return full


# ---------------------------------------------------------------------------
# Feature 2 — Campaign Insights (dynamic, not stored)
# ---------------------------------------------------------------------------

def generate_insights(full: dict) -> list[dict]:
    insights = []
    actual_player_pct = full.get("actual_player_pct")
    voucher_hunter_pct = full.get("voucher_hunter_pct")
    conversion_pct = full.get("conversion_pct")
    time_to_50pct = full["speed"]["time_to_50pct_claimed_minutes"]
    referral_after_claim = full["conversion_proxy"].get("referral_after_claim")
    claimed = full["volume"]["total_claimed"]

    if actual_player_pct is not None and actual_player_pct >= 60:
        insights.append({"type": "good", "text": "High actual-player conversion"})
    if time_to_50pct is not None and time_to_50pct <= 120:
        insights.append({"type": "good", "text": "Fast claim velocity"})
    if voucher_hunter_pct is not None and voucher_hunter_pct <= 5:
        insights.append({"type": "good", "text": "Low abuse rate"})
    if isinstance(referral_after_claim, int) and referral_after_claim > 0:
        insights.append({"type": "good", "text": "Strong referral activation"})

    if voucher_hunter_pct is not None and voucher_hunter_pct >= 15:
        insights.append({"type": "bad", "text": "High voucher hunter participation"})
    if time_to_50pct is not None and time_to_50pct >= 240:
        insights.append({"type": "bad", "text": "Slow claim velocity"})
    if claimed > 0 and (referral_after_claim in (0, None)):
        insights.append({"type": "bad", "text": "Low retention"})
    if conversion_pct is not None and conversion_pct < 10:
        insights.append({"type": "bad", "text": "Weak conversion"})

    return insights


# ---------------------------------------------------------------------------
# Feature 3 — Campaign Recommendations (deterministic rule engine)
# ---------------------------------------------------------------------------

def generate_recommendations(full: dict) -> list[str]:
    recs: list[str] = []
    claim_rate = full["volume"]["claim_rate"]
    voucher_hunter_pct = full.get("voucher_hunter_pct")
    actual_player_pct = full.get("actual_player_pct")
    conversion_pct = full.get("conversion_pct")
    ghost = full["quality"].get("ghost", 0)
    claimed = full["volume"]["total_claimed"]
    time_to_50pct = full["speed"]["time_to_50pct_claimed_minutes"]

    if claim_rate is not None and claim_rate >= 70 and (voucher_hunter_pct or 0) <= 5:
        recs.append("increase batch size +25%")

    if voucher_hunter_pct is not None and voucher_hunter_pct >= 15:
        recs.append("reduce voucher count -20%")

    ghost_pct = round(100.0 * ghost / claimed, 1) if claimed else None
    if ghost_pct is not None and ghost_pct >= 10:
        recs.append("remove ghost segment")

    if actual_player_pct is not None and actual_player_pct < 50 and "prioritize normal_actual" not in recs:
        recs.append("prioritize normal_actual")
    elif conversion_pct is not None and conversion_pct < 10 and "prioritize normal_actual" not in recs:
        recs.append("prioritize normal_actual")

    if time_to_50pct is not None and time_to_50pct >= 240:
        recs.append("extend release interval to 2h")

    return recs


# ---------------------------------------------------------------------------
# Feature 4 — Segment Performance Matrix (per campaign)
# ---------------------------------------------------------------------------

def segment_matrix(campaign: dict, window: str = "all") -> list[dict]:
    drops = cp._campaign_drops(campaign)
    _volume, _child_rows, all_claims = cp._volume_and_children(campaign, drops)
    cutoff = cp._window_cutoff(window)
    all_claims = cp._filter_claims_by_window(all_claims, cutoff)
    _quality, _abuse, segment_map = cp._quality_and_abuse(all_claims)

    rows = []
    for seg in KNOWN_QUALITY_SEGMENTS:
        seg_claims = [c for c in all_claims if segment_map.get(_safe_int(c.get("user_id"))) == seg]
        claimed = len(seg_claims)
        if not claimed:
            rows.append({"segment": seg, "claimed": 0, "conversion_pct": None, "score": "N/A"})
            continue
        conv = cp._conversion_proxy(seg_claims)
        pct = _conversion_pct(conv, claimed)
        rows.append({"segment": seg, "claimed": claimed, "conversion_pct": pct, "score": _grade(pct)})
    return rows


# ---------------------------------------------------------------------------
# Feature 5 — Campaign Template Ranking
# ---------------------------------------------------------------------------

def template_ranking(window: str = "all") -> list[dict]:
    campaigns = _non_draft_campaigns("all")
    buckets: dict[str, list[dict]] = defaultdict(list)
    for c in campaigns:
        full = enrich_performance(cp.compute_campaign_performance(c, window=window))
        buckets[c.get("campaign_type") or "unknown"].append(full)

    out = []
    for template, rows in buckets.items():
        n = len(rows)
        avg_score = round(sum(r["campaign_score"] for r in rows) / n, 1)
        claim_rates = [r["volume"]["claim_rate"] for r in rows if r["volume"]["claim_rate"] is not None]
        avg_claim_rate = round(sum(claim_rates) / len(claim_rates), 1) if claim_rates else None
        conv_pcts = [r["conversion_pct"] for r in rows if r["conversion_pct"] is not None]
        avg_conversion = round(sum(conv_pcts) / len(conv_pcts), 1) if conv_pcts else None
        vh_pcts = [r["voucher_hunter_pct"] for r in rows if r["voucher_hunter_pct"] is not None]
        avg_abuse = round(sum(vh_pcts) / len(vh_pcts), 1) if vh_pcts else None
        out.append({
            "template": template,
            "campaign_count": n,
            "avg_score": avg_score,
            "avg_claim_rate": avg_claim_rate,
            "avg_conversion_pct": avg_conversion,
            "avg_abuse_pct": avg_abuse,
        })
    out.sort(key=lambda r: r["avg_score"], reverse=True)
    return out


# ---------------------------------------------------------------------------
# Feature 6 — Release Strategy Ranking
# ---------------------------------------------------------------------------

def release_ranking(window: str = "all") -> list[dict]:
    campaigns = _non_draft_campaigns("all")
    buckets: dict[str, list[dict]] = defaultdict(list)
    for c in campaigns:
        full = enrich_performance(cp.compute_campaign_performance(c, window=window))
        buckets[_release_strategy_label(c)].append(full)

    out = []
    for strategy, rows in buckets.items():
        n = len(rows)
        speeds = [r["speed"]["average_claim_speed_minutes"] for r in rows if r["speed"]["average_claim_speed_minutes"] is not None]
        avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else None
        conv_pcts = [r["conversion_pct"] for r in rows if r["conversion_pct"] is not None]
        avg_conversion = round(sum(conv_pcts) / len(conv_pcts), 1) if conv_pcts else None
        vh_pcts = [r["voucher_hunter_pct"] for r in rows if r["voucher_hunter_pct"] is not None]
        avg_abuse = round(sum(vh_pcts) / len(vh_pcts), 1) if vh_pcts else None
        completions = [r["volume"]["release_completion_pct"] for r in rows if r["volume"]["release_completion_pct"] is not None]
        avg_completion = round(sum(completions) / len(completions), 1) if completions else None
        out.append({
            "release_strategy": strategy,
            "campaign_count": n,
            "avg_claim_speed_minutes": avg_speed,
            "avg_conversion_pct": avg_conversion,
            "avg_abuse_pct": avg_abuse,
            "avg_completion_pct": avg_completion,
        })
    out.sort(key=lambda r: (r["avg_conversion_pct"] if r["avg_conversion_pct"] is not None else -1), reverse=True)
    return out


# ---------------------------------------------------------------------------
# Feature 7 — Segment Recommendation Engine (global ROI)
# ---------------------------------------------------------------------------

def segment_recommendations(window: str = "all") -> dict:
    campaigns = _non_draft_campaigns("all")
    totals = {seg: {"claimed": 0, "converted": 0.0} for seg in KNOWN_QUALITY_SEGMENTS}
    overall_vh_pcts = []

    for c in campaigns:
        full = enrich_performance(cp.compute_campaign_performance(c, window=window))
        if full.get("voucher_hunter_pct") is not None:
            overall_vh_pcts.append(full["voucher_hunter_pct"])
        for row in segment_matrix(c, window=window):
            seg = row["segment"]
            claimed = row["claimed"]
            if not claimed:
                continue
            totals[seg]["claimed"] += claimed
            if row["conversion_pct"] is not None:
                totals[seg]["converted"] += row["conversion_pct"] * claimed / 100.0

    voucher_hunter_exposure = round(sum(overall_vh_pcts) / len(overall_vh_pcts), 1) if overall_vh_pcts else 0.0
    denom = max(voucher_hunter_exposure, 1.0)

    roi_table = []
    for seg, agg in totals.items():
        claimed = agg["claimed"]
        avg_conversion = round(100.0 * agg["converted"] / claimed, 1) if claimed else None
        roi = round(avg_conversion / denom, 2) if avg_conversion is not None else None
        roi_table.append({"segment": seg, "claimed": claimed, "avg_conversion_pct": avg_conversion, "roi": roi})

    roi_table.sort(key=lambda r: (r["roi"] if r["roi"] is not None else -1), reverse=True)

    eligible = [r for r in roi_table if r["segment"] not in ("voucher_hunter", "ghost")]
    recommended_segments = [r["segment"] for r in eligible if r["roi"] is not None and r["roi"] >= 1.0][:2]
    if not recommended_segments:
        recommended_segments = [r["segment"] for r in eligible[:2] if r["claimed"]]

    avoid_segments = ["voucher_hunter", "ghost"]
    for r in eligible:
        if r["segment"] not in recommended_segments and r["segment"] not in avoid_segments and r["roi"] is not None and r["roi"] < 0.5:
            avoid_segments.append(r["segment"])

    return {
        "voucher_hunter_exposure_pct": voucher_hunter_exposure,
        "segment_roi": roi_table,
        "recommended_segments": recommended_segments,
        "avoid_segments": avoid_segments,
    }


# ---------------------------------------------------------------------------
# Feature 8 — Best Time To Launch
# ---------------------------------------------------------------------------

def best_time_to_launch(window: str = "all") -> dict:
    campaigns = _non_draft_campaigns("all")
    hour_claim_rates: dict[int, list[float]] = defaultdict(list)

    for c in campaigns:
        drops = cp._campaign_drops(c)
        _volume, child_rows, _claims = cp._volume_and_children(c, drops)
        for row in child_rows:
            if not row["released"] or row["claim_rate"] is None or not row["release_time"]:
                continue
            release_dt = datetime.fromisoformat(row["release_time"])
            local_hour = release_dt.astimezone(KL_TZ).hour
            hour_claim_rates[local_hour].append(row["claim_rate"])

    table = []
    for hour in range(24):
        rates = hour_claim_rates.get(hour)
        if not rates:
            continue
        avg_rate = sum(rates) / len(rates)
        score = round(_clamp(avg_rate, 0, 100), 1)
        table.append({"hour": hour, "label": _hour_label(hour), "score": score, "sample_size": len(rates)})

    table.sort(key=lambda r: r["hour"])

    if table:
        best = max(table, key=lambda r: r["score"])
        recommendation = f"Best launch time: {best['label']}"
    else:
        best = None
        recommendation = "Insufficient release-time data to recommend a launch hour."

    return {"hours": table, "best_hour": best, "recommendation": recommendation}


def _hour_label(hour: int) -> str:
    suffix = "am" if hour < 12 else "pm"
    display = hour % 12
    if display == 0:
        display = 12
    return f"{display}{suffix}"


# ---------------------------------------------------------------------------
# Feature 1 — Campaign Effectiveness Ranking
# ---------------------------------------------------------------------------

def build_rankings(status: str = "all", window: str = "all") -> list[dict]:
    campaigns = _non_draft_campaigns(status)
    rows = []
    for c in campaigns:
        full = enrich_performance(cp.compute_campaign_performance(c, window=window))
        rows.append(full)

    rows.sort(key=lambda r: (r["ranking_score"], r["campaign_score"], r["volume"]["total_claimed"]), reverse=True)

    out = []
    for i, full in enumerate(rows, start=1):
        out.append({
            "rank": i,
            "campaign_id": full["campaign_id"],
            "campaign_name": full["campaign_name"],
            "campaign_type": full["campaign_type"],
            "status": full["status"],
            "ranking_score": full["ranking_score"],
            "campaign_score": full["campaign_score"],
            "claim_rate": full["volume"]["claim_rate"],
            "actual_player_pct": full["actual_player_pct"],
            "voucher_hunter_pct": full["voucher_hunter_pct"],
            "conversion_pct": full["conversion_pct"],
            "avg_claim_speed_minutes": full["speed"]["average_claim_speed_minutes"],
            "insights": generate_insights(full),
        })
    return out


# ---------------------------------------------------------------------------
# Feature 9 — Campaign Playbook Generator
# ---------------------------------------------------------------------------

def generate_playbook(full: dict, matrix: list[dict]) -> dict:
    claimed = full["volume"]["total_claimed"]
    best_segment_row = max(
        (r for r in matrix if r["claimed"]),
        key=lambda r: (r["conversion_pct"] or 0),
        default=None,
    )
    audience = best_segment_row["segment"] if best_segment_row else "normal_actual"

    total_released = full["volume"]["total_released"]
    avg_speed = full["speed"]["average_claim_speed_minutes"]
    if total_released and avg_speed:
        rate_per_hour = round(total_released / max(avg_speed / 60.0, 1e-6))
    else:
        rate_per_hour = None

    voucher_count = full["volume"]["total_vouchers"] or None
    recs = generate_recommendations(full)
    if voucher_count is not None:
        if "increase batch size +25%" in recs:
            voucher_count = round(voucher_count * 1.25)
        if "reduce voucher count -20%" in recs:
            voucher_count = round(voucher_count * 0.8)

    if claimed >= 100:
        confidence = "High"
    elif claimed >= 30:
        confidence = "Medium"
    else:
        confidence = "Low"

    return {
        "based_on_campaign_id": full["campaign_id"],
        "based_on_campaign_name": full["campaign_name"],
        "template": full["campaign_type"],
        "audience": audience,
        "release": {
            "strategy": RELEASE_STRATEGY_LABELS.get(None) if not rate_per_hour else "every_x_minutes",
            "rate_per_hour": rate_per_hour,
        },
        "voucher_count": voucher_count,
        "expected_claim_rate_pct": full["volume"]["claim_rate"],
        "expected_abuse_pct": full["voucher_hunter_pct"],
        "confidence": confidence,
        "recommendations": recs,
    }


# ---------------------------------------------------------------------------
# Flask routes — all GET, all read-only
# ---------------------------------------------------------------------------

def _window_param() -> str:
    window = (request.args.get("window") or "all").strip()
    return window if window in WINDOW_CHOICES else "all"


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/rankings", methods=["GET"])
def intelligence_rankings():
    _, err = _require_admin()
    if err:
        return err
    status = (request.args.get("status") or "all").strip()
    if status not in cp.STATUS_CHOICES:
        status = "all"
    window = _window_param()
    return jsonify({"status": "ok", "window": window, "rankings": build_rankings(status=status, window=window)})


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/campaign/<campaign_id>", methods=["GET"])
def intelligence_campaign_detail(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    campaign = cp._campaigns_col().find_one({"_id": oid})
    if not campaign:
        return jsonify({"status": "error", "code": "not_found"}), 404

    window = _window_param()
    full = enrich_performance(cp.compute_campaign_performance(campaign, window=window))
    matrix = segment_matrix(campaign, window=window)

    all_rankings = build_rankings(status="all", window=window)
    rank = next((r["rank"] for r in all_rankings if r["campaign_id"] == full["campaign_id"]), None)

    return jsonify({
        "status": "ok",
        "window": window,
        "campaign": {
            "campaign_id": full["campaign_id"],
            "campaign_name": full["campaign_name"],
            "campaign_type": full["campaign_type"],
            "rank": rank,
            "ranking_score": full["ranking_score"],
            "campaign_score": full["campaign_score"],
            "badge": full["badge"],
            "volume": full["volume"],
            "speed": full["speed"],
            "insights": generate_insights(full),
            "recommendations": generate_recommendations(full),
            "segment_matrix": matrix,
            "playbook": generate_playbook(full, matrix),
        },
    })


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/templates", methods=["GET"])
def intelligence_templates():
    _, err = _require_admin()
    if err:
        return err
    window = _window_param()
    return jsonify({"status": "ok", "window": window, "templates": template_ranking(window=window)})


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/releases", methods=["GET"])
def intelligence_releases():
    _, err = _require_admin()
    if err:
        return err
    window = _window_param()
    return jsonify({"status": "ok", "window": window, "releases": release_ranking(window=window)})


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/segments", methods=["GET"])
def intelligence_segments():
    _, err = _require_admin()
    if err:
        return err
    window = _window_param()
    return jsonify({"status": "ok", "window": window, **segment_recommendations(window=window)})


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/best-time", methods=["GET"])
def intelligence_best_time():
    _, err = _require_admin()
    if err:
        return err
    window = _window_param()
    return jsonify({"status": "ok", "window": window, **best_time_to_launch(window=window)})


@campaign_intelligence_bp.route("/api/admin/campaign-builder/intelligence/playbook", methods=["GET"])
def intelligence_playbook():
    _, err = _require_admin()
    if err:
        return err
    window = _window_param()
    campaign_id = (request.args.get("campaign_id") or "").strip()

    if campaign_id:
        try:
            oid = ObjectId(campaign_id)
        except Exception:
            return jsonify({"status": "error", "code": "invalid_id"}), 400
        campaign = cp._campaigns_col().find_one({"_id": oid})
        if not campaign:
            return jsonify({"status": "error", "code": "not_found"}), 404
    else:
        rankings = build_rankings(status="all", window=window)
        if not rankings:
            return jsonify({"status": "error", "code": "no_campaigns"}), 404
        try:
            oid = ObjectId(rankings[0]["campaign_id"])
        except Exception:
            return jsonify({"status": "error", "code": "invalid_id"}), 400
        campaign = cp._campaigns_col().find_one({"_id": oid})
        if not campaign:
            return jsonify({"status": "error", "code": "not_found"}), 404

    full = enrich_performance(cp.compute_campaign_performance(campaign, window=window))
    matrix = segment_matrix(campaign, window=window)
    return jsonify({"status": "ok", "window": window, "playbook": generate_playbook(full, matrix)})
