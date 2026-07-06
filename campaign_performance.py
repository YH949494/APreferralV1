"""Campaign Performance Intelligence — Phase 4 (P4).

Read-only analytics layer on top of the existing Campaign Builder
(``campaign_builder.py``) and voucher-drop engine (``vouchers.py``). This
module never mutates a campaign, a drop, a voucher, or a claim — it only
reads ``campaign_builder_campaigns``, ``drops``, ``vouchers``,
``voucher_claims``, ``users``, ``qualified_events``, and
``pending_referrals`` to compute reporting metrics and a read-only
"campaign score".

It does NOT touch:
  - claim logic / FCFS / pooled or personalised claim paths
  - eligibility evaluation
  - anti-abuse enforcement (rate limits, kill switches, cooldowns)
  - the scheduler
  - affiliate settlement
  - welcome voucher logic
  - P3 batch release execution (compile/pause/resume/cancel/release-next)

Core principle (see product spec): Campaign -> Voucher Drop(s) -> Voucher
Claims -> Performance Analytics. No new claim ledger is introduced; this
module is purely a read/aggregate layer over data other modules already
write.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request
from pymongo import DESCENDING

import database
from config import normalize_for_bot_segment

logger = logging.getLogger(__name__)

campaign_performance_bp = Blueprint("campaign_performance", __name__)

CAMPAIGN_COLLECTION = "campaign_builder_campaigns"

# The five classified quality segments P4 scores explicitly. Any claimant
# whose segment does not resolve to one of these (missing user doc, blank
# segment, or a segment name outside this set) is bucketed as "unknown".
KNOWN_QUALITY_SEGMENTS = (
    "high_value",
    "normal_actual",
    "low_value",
    "voucher_hunter",
    "ghost",
)

WINDOW_CHOICES = ("7d", "30d", "all")
STATUS_CHOICES = ("active", "completed", "cancelled", "all")
SORT_CHOICES = ("score", "claim_rate", "claimed", "created_at")

# Score badge thresholds (read-only; documented in
# docs/CAMPAIGN_PERFORMANCE_P4_IMPLEMENTATION.md). Never affects
# eligibility, claim behavior, or voucher/scheduler logic.
SCORE_BADGES = (
    (50, "High Quality"),
    (20, "Good"),
    (-19, "Neutral"),
    (-49, "Risky"),
)


def score_badge(score: int) -> str:
    if score >= 50:
        return "High Quality"
    if score >= 20:
        return "Good"
    if score >= -19:
        return "Neutral"
    if score >= -49:
        return "Risky"
    return "Bad"


def _require_admin():
    from vouchers import require_admin
    return require_admin()


def _campaigns_col():
    return database.db[CAMPAIGN_COLLECTION]


def _window_cutoff(window: str) -> datetime | None:
    if window == "7d":
        return datetime.now(timezone.utc) - timedelta(days=7)
    if window == "30d":
        return datetime.now(timezone.utc) - timedelta(days=30)
    return None


def _aware(dt) -> datetime | None:
    if not isinstance(dt, datetime):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _is_batch_campaign(campaign: dict) -> bool:
    return bool(campaign.get("release_type"))


def _effective_status(campaign: dict) -> str:
    """Map the two coexisting status vocabularies (P2 ``status`` vs P3
    ``batch_status``) onto the reporting vocabulary the performance API
    exposes (active/completed/cancelled/draft)."""
    if _is_batch_campaign(campaign):
        bs = campaign.get("batch_status")
        if bs in ("scheduled", "active", "paused"):
            return "active"
        if bs == "completed":
            return "completed"
        if bs == "cancelled":
            return "cancelled"
        return "draft"
    st = campaign.get("status")
    if st == "active":
        return "active"
    if st == "archived":
        return "completed"
    if st == "compiled":
        return "active"
    return "draft"


def _drop_is_released(drop: dict) -> bool:
    if drop.get("batch_parent_id"):
        return drop.get("batch_status") in ("released", "paused", "cancelled")
    return drop.get("status") not in ("upcoming", "paused", None)


def _drop_release_time(drop: dict) -> datetime | None:
    if drop.get("batch_actual_release_at"):
        return _aware(drop.get("batch_actual_release_at"))
    return _aware(drop.get("startsAt"))


def _campaign_drops(campaign: dict) -> list[dict]:
    """All drops belonging to this campaign — single P2 drop(s) and P3
    batch children alike. Both compile paths tag every drop they create
    with ``campaign_id`` (see campaign_builder.py), so this single query is
    the ground truth for "which drops belong to this campaign", instead of
    trusting the parent doc's cached id-array fields."""
    db = database.db
    return list(db.drops.find(
        {"campaign_id": str(campaign["_id"])},
        sort=[("batch_index", 1), ("createdAt", 1)],
    ))


def _drop_claims(drop: dict) -> list[dict]:
    db = database.db
    drop_id = drop["_id"]
    return list(db.voucher_claims.find(
        {"drop_id": {"$in": [drop_id, str(drop_id)]}, "status": "claimed"},
    ))


def _volume_and_children(campaign: dict, drops: list[dict]) -> tuple[dict, list[dict], list[dict]]:
    """Returns (volume_metrics, child_rows, all_claim_docs)."""
    from vouchers import PERSONALISED_TYPE_ALIASES

    db = database.db
    total_vouchers = 0
    total_released = 0
    total_claimed = 0
    child_rows: list[dict] = []
    all_claims: list[dict] = []

    for drop in drops:
        drop_id = drop["_id"]
        variants = [drop_id, str(drop_id)]
        is_personalised = drop.get("type") == "personalised"
        if is_personalised:
            total = db.vouchers.count_documents({"type": {"$in": list(PERSONALISED_TYPE_ALIASES)}, "dropId": {"$in": variants}})
            claimed = db.vouchers.count_documents({"type": {"$in": list(PERSONALISED_TYPE_ALIASES)}, "dropId": {"$in": variants}, "status": "claimed"})
        else:
            total = db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": variants}})
            claimed = db.vouchers.count_documents({"type": "pooled", "dropId": {"$in": variants}, "status": {"$ne": "free"}})

        total_vouchers += total
        total_claimed += claimed
        released = _drop_is_released(drop)
        if released:
            total_released += total

        drop_claims = _drop_claims(drop)
        all_claims.extend(drop_claims)

        release_time = _drop_release_time(drop)
        child_rows.append({
            "batch_index": drop.get("batch_index"),
            "drop_id": str(drop_id),
            "drop_name": drop.get("name"),
            "released": released,
            "release_time": release_time.isoformat() if release_time else None,
            "batch_status": drop.get("batch_status"),
            "drop_status": drop.get("status"),
            "total_codes": total,
            "claimed": claimed,
            "remaining": max(0, total - claimed),
            "claim_rate": round(100.0 * claimed / total, 1) if total else None,
            "voucher_hunter_share": None,  # filled in by caller once segments are resolved
        })

    volume = {
        "total_vouchers": total_vouchers,
        "total_released": total_released,
        "total_claimed": total_claimed,
        "total_remaining": max(0, total_vouchers - total_claimed),
        "claim_rate": round(100.0 * total_claimed / total_released, 1) if total_released else None,
        "release_completion_pct": round(100.0 * total_released / total_vouchers, 1) if total_vouchers else None,
    }
    return volume, child_rows, all_claims


def _speed_metrics(drops: list[dict], claims: list[dict], total_released: int) -> dict:
    if not claims or not total_released:
        return {
            "time_to_first_claim_minutes": None,
            "time_to_50pct_claimed_minutes": None,
            "time_to_sold_out_minutes": None,
            "average_claim_speed_minutes": None,
            "reason": "no_claims_yet" if total_released else "no_released_vouchers",
        }

    release_times = [t for t in (_drop_release_time(d) for d in drops if _drop_is_released(d)) if t]
    if not release_times:
        return {
            "time_to_first_claim_minutes": None,
            "time_to_50pct_claimed_minutes": None,
            "time_to_sold_out_minutes": None,
            "average_claim_speed_minutes": None,
            "reason": "no_release_time_recorded",
        }
    earliest_release = min(release_times)

    claimed_ats = sorted(t for t in (_aware(c.get("claimed_at")) for c in claims) if t)
    if not claimed_ats:
        return {
            "time_to_first_claim_minutes": None,
            "time_to_50pct_claimed_minutes": None,
            "time_to_sold_out_minutes": None,
            "average_claim_speed_minutes": None,
            "reason": "claim_timestamps_missing",
        }

    def minutes_to(idx_ts: datetime) -> float:
        return round((idx_ts - earliest_release).total_seconds() / 60.0, 1)

    time_to_first = minutes_to(claimed_ats[0])

    half_idx = -(-total_released // 2) - 1  # ceil(total_released/2) - 1, 0-indexed
    time_to_50pct = minutes_to(claimed_ats[half_idx]) if 0 <= half_idx < len(claimed_ats) else None

    time_to_sold_out = minutes_to(claimed_ats[-1]) if len(claimed_ats) >= total_released else None

    per_batch_speeds = []
    for drop in drops:
        if not _drop_is_released(drop):
            continue
        r_time = _drop_release_time(drop)
        if not r_time:
            continue
        d_claims = sorted(t for t in (_aware(c.get("claimed_at")) for c in claims if c.get("drop_id") in (drop["_id"], str(drop["_id"]))) if t)
        if d_claims:
            per_batch_speeds.append((d_claims[-1] - r_time).total_seconds() / 60.0)
    avg_speed = round(sum(per_batch_speeds) / len(per_batch_speeds), 1) if per_batch_speeds else None

    return {
        "time_to_first_claim_minutes": time_to_first,
        "time_to_50pct_claimed_minutes": time_to_50pct,
        "time_to_sold_out_minutes": time_to_sold_out,
        "average_claim_speed_minutes": avg_speed,
        "reason": None if (time_to_50pct is not None or time_to_sold_out is not None) else "not_enough_claims_yet",
    }


def _resolve_segments(user_ids: list[int]) -> dict[int, str]:
    if not user_ids:
        return {}
    db = database.db
    docs = db.users.find(
        {"user_id": {"$in": user_ids}},
        projection={"user_id": 1, "for_bot_segment": 1, "bot_segment": 1, "_id": 0},
    )
    out: dict[int, str] = {}
    for doc in docs:
        raw = doc.get("for_bot_segment") or doc.get("bot_segment")
        normalized = normalize_for_bot_segment(raw)
        out[int(doc["user_id"])] = normalized if normalized in KNOWN_QUALITY_SEGMENTS else "unknown"
    return out


def _quality_and_abuse(claims: list[dict]) -> tuple[dict, dict, dict[int, str]]:
    user_ids: list[int] = []
    seen = set()
    for c in claims:
        uid = c.get("user_id")
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            continue
        if uid not in seen:
            seen.add(uid)
            user_ids.append(uid)

    segment_map = _resolve_segments(user_ids)

    quality = {seg: 0 for seg in KNOWN_QUALITY_SEGMENTS}
    quality["unknown"] = 0
    unknown_reason = None
    claim_counts_by_user: dict[int, int] = {}
    subnet_counts: dict[str, int] = {}
    suspicious_claims = 0

    for c in claims:
        uid = c.get("user_id")
        try:
            uid_int = int(uid)
        except (TypeError, ValueError):
            uid_int = None
        seg = segment_map.get(uid_int, "unknown") if uid_int is not None else "unknown"
        quality[seg] += 1
        if seg == "unknown":
            unknown_reason = "missing_user_segment"

        if uid_int is not None:
            claim_counts_by_user[uid_int] = claim_counts_by_user.get(uid_int, 0) + 1

        subnet = c.get("claim_subnet")
        if subnet and subnet != "unknown":
            subnet_counts[subnet] = subnet_counts.get(subnet, 0) + 1

        if c.get("public_pool_subnet_pressure"):
            suspicious_claims += 1

    if quality["unknown"] and unknown_reason is None:
        unknown_reason = "missing_user_segment"

    repeat_claimers = sum(1 for cnt in claim_counts_by_user.values() if cnt > 1)
    clustered_subnets = {k: v for k, v in subnet_counts.items() if v > 1}
    same_subnet_claims = sum(clustered_subnets.values())

    total_claims = len(claims)
    abuse = {
        "repeat_claimers": repeat_claimers,
        "same_ip_subnet_claims": same_subnet_claims if clustered_subnets else 0,
        "same_ip_subnet_clusters": len(clustered_subnets),
        "claim_cooldown_hits": {"value": None, "reason": "source_not_available"},
        "voucher_hunter_claim_share_pct": (
            round(100.0 * quality["voucher_hunter"] / (total_claims - quality["unknown"]), 1)
            if (total_claims - quality["unknown"]) > 0 else None
        ),
        "suspicious_claims": suspicious_claims,
        "suspicious_claim_pct": round(100.0 * suspicious_claims / total_claims, 1) if total_claims else None,
    }

    quality_out = dict(quality)
    if quality["unknown"]:
        quality_out["unknown_reason"] = unknown_reason

    return quality_out, abuse, segment_map


def _conversion_proxy(claims: list[dict]) -> dict:
    db = database.db
    claimed_at_by_user: dict[int, datetime] = {}
    for c in claims:
        uid = c.get("user_id")
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            continue
        ts = _aware(c.get("claimed_at"))
        if not ts:
            continue
        if uid not in claimed_at_by_user or ts < claimed_at_by_user[uid]:
            claimed_at_by_user[uid] = ts

    if not claimed_at_by_user:
        return {
            "qualified_after_claim": {"value": None, "reason": "no_claims"},
            "referral_after_claim": {"value": None, "reason": "no_claims"},
            "checkin_after_claim": {"value": None, "reason": "no_claims"},
            "after_bet_or_withdrawal": {"value": None, "reason": "source_not_available"},
        }

    user_ids = list(claimed_at_by_user.keys())

    qualified_after_claim = 0
    for doc in db.qualified_events.find({"invitee_id": {"$in": user_ids}}, projection={"invitee_id": 1, "qualified_at": 1, "_id": 0}):
        uid = doc.get("invitee_id")
        ts = _aware(doc.get("qualified_at"))
        if uid in claimed_at_by_user and ts and ts > claimed_at_by_user[uid]:
            qualified_after_claim += 1

    referral_after_claim = 0
    referred_users = set()
    for doc in db.pending_referrals.find({"referrer_id": {"$in": user_ids}}, projection={"referrer_id": 1, "created_at": 1, "_id": 0}):
        uid = doc.get("referrer_id")
        ts = _aware(doc.get("created_at"))
        if uid in claimed_at_by_user and ts and ts > claimed_at_by_user[uid] and uid not in referred_users:
            referred_users.add(uid)
            referral_after_claim += 1

    checkin_after_claim = 0
    checked_in_users = set()
    checkin_filter = {
        "user_id": {"$in": user_ids},
        "$or": [{"type": "checkin"}, {"reason": "checkin"}],
    }
    for doc in db.xp_events.find(checkin_filter, projection={"user_id": 1, "created_at": 1, "_id": 0}):
        uid = doc.get("user_id")
        ts = _aware(doc.get("created_at"))
        if uid in claimed_at_by_user and ts and ts > claimed_at_by_user[uid] and uid not in checked_in_users:
            checked_in_users.add(uid)
            checkin_after_claim += 1

    return {
        "qualified_after_claim": qualified_after_claim,
        "referral_after_claim": referral_after_claim,
        "checkin_after_claim": checkin_after_claim,
        "after_bet_or_withdrawal": {"value": None, "reason": "source_not_available"},
    }


def compute_campaign_score(quality: dict, abuse: dict, conversion: dict) -> dict:
    quality_score = (
        quality.get("high_value", 0) * 5
        + quality.get("normal_actual", 0) * 3
        + quality.get("low_value", 0) * 1
        - quality.get("voucher_hunter", 0) * 3
        - quality.get("ghost", 0) * 1
    )
    abuse_penalty = abuse.get("suspicious_claims", 0) * 5

    qualified = conversion.get("qualified_after_claim")
    referral = conversion.get("referral_after_claim")
    checkin = conversion.get("checkin_after_claim")
    conversion_bonus = (
        (qualified if isinstance(qualified, int) else 0) * 4
        + (referral if isinstance(referral, int) else 0) * 2
        + (checkin if isinstance(checkin, int) else 0) * 1
    )

    campaign_score = quality_score - abuse_penalty + conversion_bonus
    return {
        "campaign_score": campaign_score,
        "score_breakdown": {
            "quality_score": quality_score,
            "abuse_penalty": abuse_penalty,
            "conversion_bonus": conversion_bonus,
        },
        "badge": score_badge(campaign_score),
    }


def _filter_claims_by_window(claims: list[dict], cutoff: datetime | None) -> list[dict]:
    if cutoff is None:
        return claims
    out = []
    for c in claims:
        ts = _aware(c.get("claimed_at"))
        if ts and ts >= cutoff:
            out.append(c)
    return out


def compute_campaign_performance(campaign: dict, window: str = "all") -> dict:
    """Full performance report for a single campaign. Read-only — issues
    only find()/count_documents() calls against existing collections."""
    drops = _campaign_drops(campaign)
    volume, child_rows, all_claims = _volume_and_children(campaign, drops)

    cutoff = _window_cutoff(window)
    windowed_claims = _filter_claims_by_window(all_claims, cutoff)

    speed = _speed_metrics(drops, windowed_claims, volume["total_released"])
    quality, abuse, segment_map = _quality_and_abuse(windowed_claims)
    conversion = _conversion_proxy(windowed_claims)
    score = compute_campaign_score(quality, abuse, conversion)

    # Backfill per-batch voucher-hunter share on child rows using the
    # already-resolved segment map (avoids a second per-drop DB pass).
    for row in child_rows:
        drop_id = row["drop_id"]
        drop_claims = [c for c in windowed_claims if str(c.get("drop_id")) == drop_id]
        if not drop_claims:
            row["voucher_hunter_share"] = None
            continue
        hunters = 0
        resolved = 0
        for c in drop_claims:
            try:
                uid = int(c.get("user_id"))
            except (TypeError, ValueError):
                continue
            seg = segment_map.get(uid)
            if seg and seg != "unknown":
                resolved += 1
                if seg == "voucher_hunter":
                    hunters += 1
        row["voucher_hunter_share"] = round(100.0 * hunters / resolved, 1) if resolved else None

    return {
        "campaign_id": str(campaign["_id"]),
        "campaign_name": campaign.get("campaign_name"),
        "campaign_type": campaign.get("campaign_type"),
        "is_batch": _is_batch_campaign(campaign),
        "status": _effective_status(campaign),
        "raw_status": campaign.get("status"),
        "batch_status": campaign.get("batch_status"),
        "created_at": campaign.get("created_at").isoformat() if isinstance(campaign.get("created_at"), datetime) else campaign.get("created_at"),
        "window": window,
        "volume": volume,
        "speed": speed,
        "quality": quality,
        "abuse_risk": abuse,
        "conversion_proxy": conversion,
        "campaign_score": score["campaign_score"],
        "score_breakdown": score["score_breakdown"],
        "badge": score["badge"],
        "child_drops": child_rows,
    }


def _summary_only(full: dict) -> dict:
    """Trimmed row for the list endpoint's table view."""
    v = full["volume"]
    return {
        "campaign_id": full["campaign_id"],
        "campaign_name": full["campaign_name"],
        "campaign_type": full["campaign_type"],
        "is_batch": full["is_batch"],
        "status": full["status"],
        "created_at": full["created_at"],
        "total_vouchers": v["total_vouchers"],
        "total_released": v["total_released"],
        "total_claimed": v["total_claimed"],
        "claim_rate": v["claim_rate"],
        "voucher_hunter_claim_share_pct": full["abuse_risk"]["voucher_hunter_claim_share_pct"],
        "actual_player_claim_share_pct": (
            round(100.0 * (full["quality"].get("high_value", 0) + full["quality"].get("normal_actual", 0)) /
                  max(1, sum(full["quality"].get(s, 0) for s in KNOWN_QUALITY_SEGMENTS)), 1)
        ) if any(full["quality"].get(s, 0) for s in KNOWN_QUALITY_SEGMENTS) else None,
        "campaign_score": full["campaign_score"],
        "badge": full["badge"],
    }


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@campaign_performance_bp.route("/api/admin/campaign-builder/performance", methods=["GET"])
def performance_list():
    _, err = _require_admin()
    if err:
        return err

    status = (request.args.get("status") or "active").strip()
    if status not in STATUS_CHOICES:
        status = "active"
    window = (request.args.get("window") or "all").strip()
    if window not in WINDOW_CHOICES:
        window = "all"
    sort = (request.args.get("sort") or "created_at").strip()
    if sort not in SORT_CHOICES:
        sort = "created_at"

    candidates = list(_campaigns_col().find({}, sort=[("created_at", DESCENDING)], limit=500))
    rows = []
    for c in candidates:
        eff_status = _effective_status(c)
        if eff_status == "draft":
            continue
        if status != "all" and eff_status != status:
            continue
        full = compute_campaign_performance(c, window=window)
        rows.append(full)

    if sort == "score":
        rows.sort(key=lambda r: r["campaign_score"], reverse=True)
    elif sort == "claim_rate":
        rows.sort(key=lambda r: (r["volume"]["claim_rate"] if r["volume"]["claim_rate"] is not None else -1), reverse=True)
    elif sort == "claimed":
        rows.sort(key=lambda r: r["volume"]["total_claimed"], reverse=True)
    else:
        rows.sort(key=lambda r: (r["status"] != "active", r["created_at"] or ""), reverse=False)

    return jsonify({
        "status": "ok",
        "window": window,
        "sort": sort,
        "campaigns": [_summary_only(r) for r in rows],
    })


@campaign_performance_bp.route("/api/admin/campaign-builder/performance/<campaign_id>", methods=["GET"])
def performance_detail(campaign_id):
    _, err = _require_admin()
    if err:
        return err
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400
    campaign = _campaigns_col().find_one({"_id": oid})
    if not campaign:
        return jsonify({"status": "error", "code": "not_found"}), 404

    window = (request.args.get("window") or "all").strip()
    if window not in WINDOW_CHOICES:
        window = "all"

    return jsonify({"status": "ok", "performance": compute_campaign_performance(campaign, window=window)})


@campaign_performance_bp.route("/api/admin/campaign-builder/performance/compare", methods=["GET"])
def performance_compare():
    _, err = _require_admin()
    if err:
        return err
    ids_param = (request.args.get("campaign_ids") or "").strip()
    if not ids_param:
        return jsonify({"status": "error", "code": "missing_campaign_ids"}), 400
    requested_ids = [s.strip() for s in ids_param.split(",") if s.strip()]

    window = (request.args.get("window") or "all").strip()
    if window not in WINDOW_CHOICES:
        window = "all"

    results = []
    not_found = []
    for cid in requested_ids:
        try:
            oid = ObjectId(cid)
        except Exception:
            not_found.append(cid)
            continue
        campaign = _campaigns_col().find_one({"_id": oid})
        if not campaign:
            not_found.append(cid)
            continue
        results.append(compute_campaign_performance(campaign, window=window))

    return jsonify({
        "status": "ok",
        "window": window,
        "campaigns": results,
        "not_found": not_found,
    })
