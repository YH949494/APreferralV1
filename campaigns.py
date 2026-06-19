"""Segment-Based Campaign Engine — Flask Blueprint.

Provides admin CRUD endpoints for campaigns and a /preview endpoint
that computes audience size, segment distribution, and expected cost
without saving a campaign record.

Route prefix: registered at root level in main.py.
All routes require admin authentication.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request
from pymongo import ASCENDING, DESCENDING

import database
from campaign_engine import (
    VALID_CLAIM_RISK_LEVELS,
    VALID_PLAYER_AGE_TYPES,
    VALID_SEGMENTS,
    get_historical_performance,
    preview_audience,
)

logger = logging.getLogger(__name__)

campaigns_bp = Blueprint("campaigns", __name__)

CAMPAIGN_TYPES = [
    "vip_campaign",
    "exclusive_voucher",
    "retention_reward",
    "first_bet_campaign",
    "reload_incentive",
    "referral_campaign",
    "xp_campaign",
    "community_event",
    "turnover_improvement",
    "task_based_reward",
    "anti_abuse_control",
    "reactivation_campaign",
]

CAMPAIGN_STATUSES = ["draft", "active", "paused", "ended", "archived"]


def _require_admin():
    from vouchers import require_admin
    return require_admin()


def _parse_targeting(raw: dict) -> tuple[dict | None, str | None]:
    """Validate and normalise targeting filters. Returns (targeting, error_code)."""
    targeting: dict = {}

    segments = raw.get("segments") or []
    if segments:
        invalid = [s for s in segments if s not in VALID_SEGMENTS]
        if invalid:
            return None, f"invalid_segments:{','.join(invalid)}"
        targeting["segments"] = list(segments)

    age_types = raw.get("player_age_types") or []
    if age_types:
        invalid = [a for a in age_types if a not in VALID_PLAYER_AGE_TYPES]
        if invalid:
            return None, f"invalid_player_age_types:{','.join(invalid)}"
        targeting["player_age_types"] = list(age_types)

    risk_levels = raw.get("claim_risk_levels") or []
    if risk_levels:
        invalid = [r for r in risk_levels if r not in VALID_CLAIM_RISK_LEVELS]
        if invalid:
            return None, f"invalid_claim_risk_levels:{','.join(invalid)}"
        targeting["claim_risk_levels"] = list(risk_levels)

    for field in (
        "referral_count_min",
        "referral_count_max",
        "checkin_count_min",
        "checkin_count_max",
        "activity_recency_days",
    ):
        val = raw.get(field)
        if val is not None:
            try:
                targeting[field] = int(val)
            except (TypeError, ValueError):
                return None, f"invalid_{field}"

    return targeting, None


def _ensure_campaigns_indexes() -> None:
    try:
        col = database.db["campaigns"]
        col.create_index([("status", ASCENDING)], name="ix_campaigns_status")
        col.create_index([("created_at", DESCENDING)], name="ix_campaigns_created_at")
        col.create_index(
            [("targeting.segments", ASCENDING)],
            name="ix_campaigns_targeting_segments",
        )
    except Exception:
        logger.warning("[CAMPAIGNS] Failed to create indexes", exc_info=True)


_ensure_campaigns_indexes()


# ---------------------------------------------------------------------------
# Audience preview (no persistence)
# ---------------------------------------------------------------------------

@campaigns_bp.route("/api/admin/campaigns/preview", methods=["POST"])
def preview_campaign_audience():
    """Compute audience size and segment distribution without saving."""
    _, err = _require_admin()
    if err:
        return err

    body = request.get_json(force=True) or {}
    targeting_raw = body.get("targeting") or {}
    targeting, err_code = _parse_targeting(targeting_raw)
    if err_code:
        return jsonify({"status": "error", "code": err_code}), 400

    try:
        voucher_value = float(body.get("voucher_value") or 0)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "code": "invalid_voucher_value"}), 400

    audience = preview_audience(database.db, targeting or {}, voucher_value)
    historical = get_historical_performance(database.db, targeting or {})

    return jsonify({
        "status": "ok",
        "audience": audience,
        "historical": historical,
    })


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

@campaigns_bp.route("/api/admin/campaigns", methods=["GET"])
def list_campaigns():
    _, err = _require_admin()
    if err:
        return err

    status_filter = (request.args.get("status") or "").strip()
    query: dict = {}
    if status_filter and status_filter in CAMPAIGN_STATUSES:
        query["status"] = status_filter
    elif not status_filter:
        query["status"] = {"$ne": "archived"}

    col = database.db["campaigns"]
    docs = list(col.find(query, sort=[("created_at", DESCENDING)], limit=200))
    out = []
    for d in docs:
        d["id"] = str(d.pop("_id"))
        if d.get("created_at"):
            d["created_at"] = d["created_at"].isoformat()
        if d.get("updated_at"):
            d["updated_at"] = d["updated_at"].isoformat()
        out.append(d)

    return jsonify({"status": "ok", "campaigns": out})


@campaigns_bp.route("/api/admin/campaigns", methods=["POST"])
def create_campaign():
    admin, err = _require_admin()
    if err:
        return err

    body = request.get_json(force=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"status": "error", "code": "missing_name"}), 400

    campaign_type = (body.get("campaign_type") or "").strip()
    if campaign_type and campaign_type not in CAMPAIGN_TYPES:
        return jsonify({"status": "error", "code": "invalid_campaign_type"}), 400

    targeting_raw = body.get("targeting") or {}
    targeting, err_code = _parse_targeting(targeting_raw)
    if err_code:
        return jsonify({"status": "error", "code": err_code}), 400

    try:
        voucher_value = float(body.get("voucher_value") or 0)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "code": "invalid_voucher_value"}), 400

    now = datetime.now(timezone.utc)
    doc = {
        "name": name,
        "description": (body.get("description") or "").strip(),
        "campaign_type": campaign_type,
        "status": "draft",
        "targeting": targeting or {},
        "voucher_value": voucher_value,
        "created_at": now,
        "updated_at": now,
        "created_by": admin.get("usernameLower") or str(admin.get("id", "")),
    }

    result = database.db["campaigns"].insert_one(doc)

    return jsonify({
        "status": "ok",
        "campaign_id": str(result.inserted_id),
    }), 201


@campaigns_bp.route("/api/admin/campaigns/<campaign_id>", methods=["GET"])
def get_campaign(campaign_id):
    _, err = _require_admin()
    if err:
        return err

    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400

    doc = database.db["campaigns"].find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    doc["id"] = str(doc.pop("_id"))
    if doc.get("created_at"):
        doc["created_at"] = doc["created_at"].isoformat()
    if doc.get("updated_at"):
        doc["updated_at"] = doc["updated_at"].isoformat()

    targeting = doc.get("targeting") or {}
    voucher_value = doc.get("voucher_value") or 0
    audience = preview_audience(database.db, targeting, float(voucher_value))
    historical = get_historical_performance(database.db, targeting)

    return jsonify({
        "status": "ok",
        "campaign": doc,
        "audience": audience,
        "historical": historical,
    })


@campaigns_bp.route("/api/admin/campaigns/<campaign_id>", methods=["PUT"])
def update_campaign(campaign_id):
    admin, err = _require_admin()
    if err:
        return err

    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400

    body = request.get_json(force=True) or {}
    updates: dict = {"updated_at": datetime.now(timezone.utc)}

    if "name" in body:
        name = (body["name"] or "").strip()
        if not name:
            return jsonify({"status": "error", "code": "missing_name"}), 400
        updates["name"] = name

    if "description" in body:
        updates["description"] = (body["description"] or "").strip()

    if "campaign_type" in body:
        ct = (body["campaign_type"] or "").strip()
        if ct and ct not in CAMPAIGN_TYPES:
            return jsonify({"status": "error", "code": "invalid_campaign_type"}), 400
        updates["campaign_type"] = ct

    if "status" in body:
        new_status = (body["status"] or "").strip()
        if new_status not in CAMPAIGN_STATUSES:
            return jsonify({"status": "error", "code": "invalid_status"}), 400
        updates["status"] = new_status

    if "targeting" in body:
        targeting, err_code = _parse_targeting(body.get("targeting") or {})
        if err_code:
            return jsonify({"status": "error", "code": err_code}), 400
        updates["targeting"] = targeting or {}

    if "voucher_value" in body:
        try:
            updates["voucher_value"] = float(body["voucher_value"])
        except (TypeError, ValueError):
            return jsonify({"status": "error", "code": "invalid_voucher_value"}), 400

    result = database.db["campaigns"].update_one({"_id": oid}, {"$set": updates})
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404

    return jsonify({"status": "ok"})


@campaigns_bp.route("/api/admin/campaigns/<campaign_id>", methods=["DELETE"])
def archive_campaign(campaign_id):
    _, err = _require_admin()
    if err:
        return err

    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return jsonify({"status": "error", "code": "invalid_id"}), 400

    result = database.db["campaigns"].update_one(
        {"_id": oid},
        {"$set": {"status": "archived", "updated_at": datetime.now(timezone.utc)}},
    )
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404

    return jsonify({"status": "ok"})
