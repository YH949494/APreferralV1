"""Tournament provider integration — leaderboard result intake (Phase 9/10)
and provider-facing status API (Phase 16).

Security: every write here is authenticated with a per-provider HMAC-SHA256
signature, protected against replay (nonce + timestamp window), and never
accepts or returns voucher codes. Result processing is idempotent by
(provider_id, tournament_id, result_version).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request

import database
import reward_engine
from campaign_centre import get_campaign, log_funnel_event
from campaign_events import deterministic_event_id
from campaign_providers import get_provider, provider_is_usable_for_results, provider_secret

logger = logging.getLogger(__name__)

tournament_integration_bp = Blueprint("tournament_integration", __name__)

HMAC_TIMESTAMP_TOLERANCE_S = int(os.getenv("CAMPAIGN_PROVIDER_HMAC_TOLERANCE_S", "300"))
NONCE_TTL_S = int(os.getenv("CAMPAIGN_PROVIDER_NONCE_TTL_S", "600"))
MAX_WINNERS = int(os.getenv("TOURNAMENT_MAX_WINNERS", "500"))

RESULT_STATUSES = [
    "pending_review", "approved", "allocating", "assigned",
    "out_of_stock", "rejected", "corrected",
]


def _ensure_indexes() -> None:
    try:
        results_col = database.db["tournament_results"]
        results_col.create_index(
            [("provider_id", 1), ("tournament_id", 1), ("result_version", 1)],
            name="ux_tournament_results_identity", unique=True,
        )
        results_col.create_index([("campaign_id", 1)], name="ix_tournament_results_campaign")
        results_col.create_index([("status", 1)], name="ix_tournament_results_status")
        results_col.create_index([("received_at", -1)], name="ix_tournament_results_received_at")
        results_col.create_index([("tournament_id", 1)], name="ix_tournament_results_tournament_id")

        nonce_col = database.db["tournament_nonces"]
        nonce_col.create_index([("provider_id", 1), ("nonce", 1)], name="ux_tournament_nonces", unique=True)
        nonce_col.create_index([("expires_at", 1)], name="ttl_tournament_nonces", expireAfterSeconds=0)
    except Exception:
        logger.warning("[TOURNAMENT_INTEGRATION] index creation failed", exc_info=True)


_ensure_indexes()


def _constant_time_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def _verify_hmac_request(raw_body: bytes) -> tuple[dict | None, str | None]:
    provider_id = request.headers.get("X-Provider-Id", "")
    timestamp = request.headers.get("X-Timestamp", "")
    nonce = request.headers.get("X-Nonce", "")
    signature = request.headers.get("X-Signature", "")

    if not (provider_id and timestamp and nonce and signature):
        return None, "missing_headers"

    provider = get_provider(provider_id)
    if not provider:
        return None, "unknown_provider"
    if not provider_is_usable_for_results(provider):
        return None, "inactive_provider"

    try:
        ts = int(timestamp)
    except ValueError:
        return None, "invalid_timestamp"
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if abs(now_ts - ts) > HMAC_TIMESTAMP_TOLERANCE_S:
        return None, "stale_timestamp"

    secret = provider_secret(provider)
    if not secret:
        return None, "provider_secret_not_configured"

    signature_input = f"{timestamp}.{nonce}.{raw_body.decode('utf-8', errors='replace')}"
    computed = hmac.new(secret.encode(), signature_input.encode(), hashlib.sha256).hexdigest()
    if not _constant_time_eq(computed, signature.strip().lower()):
        _record_signature_failure(provider_id)
        return None, "invalid_signature"

    now = datetime.now(timezone.utc)
    try:
        database.db["tournament_nonces"].insert_one({
            "provider_id": provider_id,
            "nonce": nonce,
            "created_at": now,
            "expires_at": now + timedelta(seconds=NONCE_TTL_S),
        })
    except Exception:
        _record_nonce_replay(provider_id)
        return None, "nonce_replayed"

    return provider, None


def _record_signature_failure(provider_id: str) -> None:
    try:
        database.db["campaign_provider_integration_status"].update_one(
            {"provider_id": provider_id},
            {"$inc": {"signature_failures": 1}, "$set": {"last_failure_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception:
        logger.warning("[TOURNAMENT_INTEGRATION] status_write_failed", exc_info=True)
    log_funnel_event("provider_signature_failed", campaign_id="", provider_id=provider_id, source="provider", status="fail")


def _record_nonce_replay(provider_id: str) -> None:
    try:
        database.db["campaign_provider_integration_status"].update_one(
            {"provider_id": provider_id},
            {"$inc": {"nonce_replay_attempts": 1}, "$set": {"last_failure_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception:
        logger.warning("[TOURNAMENT_INTEGRATION] status_write_failed", exc_info=True)
    log_funnel_event("provider_nonce_replay", campaign_id="", provider_id=provider_id, source="provider", status="fail")


def _record_success(provider_id: str) -> None:
    try:
        database.db["campaign_provider_integration_status"].update_one(
            {"provider_id": provider_id},
            {"$set": {"last_request_at": datetime.now(timezone.utc), "last_success_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception:
        logger.warning("[TOURNAMENT_INTEGRATION] status_write_failed", exc_info=True)


def _validate_payload(body: dict, campaign: dict, provider_id: str) -> str | None:
    if campaign.get("type") != "tournament":
        return "campaign_not_tournament"
    # Results may still be submitted after a campaign is paused/ended (to
    # process the final leaderboard) but never for draft/scheduled/archived.
    if campaign.get("status") not in ("live", "paused", "ended"):
        return "campaign_not_active"
    if not (campaign.get("destination") or {}).get("ready"):
        return "destination_not_ready"
    if (campaign.get("destination") or {}).get("provider_id") != provider_id:
        return "provider_mismatch"

    if not str(body.get("tournament_id") or "").strip():
        return "missing_tournament_id"
    try:
        version = int(body.get("result_version"))
        if version <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return "invalid_result_version"
    if not body.get("finalized_at"):
        return "missing_finalized_at"

    winners = body.get("winners")
    if not isinstance(winners, list) or not winners:
        return "empty_winners"
    if len(winners) > MAX_WINNERS:
        return "too_many_winners"

    rules = (campaign.get("reward_config") or {}).get("rules") or []
    if not rules:
        return "reward_rules_not_configured"
    allowed_ranks = reward_engine.rank_ranges(rules)

    seen_uids = set()
    seen_ranks = set()
    for w in winners:
        try:
            rank = int(w.get("rank"))
            tg_uid = int(w.get("telegram_user_id"))
            score = float(w.get("score"))
        except (TypeError, ValueError):
            return "invalid_winner_row"
        if rank <= 0:
            return "invalid_rank"
        if tg_uid in seen_uids:
            return "duplicate_telegram_uid"
        seen_uids.add(tg_uid)
        if rank in seen_ranks:
            return "duplicate_rank"
        seen_ranks.add(rank)
        if rank not in allowed_ranks:
            return "winner_rank_outside_reward_rules"
    return None


@tournament_integration_bp.post("/api/integrations/tournaments/results")
def submit_tournament_results():
    raw_body = request.get_data() or b""
    provider, err = _verify_hmac_request(raw_body)
    if err:
        return jsonify({"status": "error", "code": err}), 401 if err != "unknown_provider" else 404

    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"status": "error", "code": "invalid_json"}), 400

    provider_id = provider["provider_id"]
    campaign_id = str(body.get("campaign_id") or "").strip()
    campaign = get_campaign(campaign_id) if campaign_id else None
    if not campaign:
        return jsonify({"status": "error", "code": "campaign_not_found"}), 404

    validation_error = _validate_payload(body, campaign, provider_id)
    if validation_error:
        log_funnel_event("leaderboard_rejected", campaign_id=campaign_id, provider_id=provider_id,
                          campaign_type=campaign.get("type"), source="provider", status="fail", reason=validation_error)
        return jsonify({"status": "error", "code": validation_error}), 400

    payload_hash = hashlib.sha256(raw_body).hexdigest()
    tournament_id = str(body["tournament_id"]).strip()
    result_version = int(body["result_version"])

    existing = database.db["tournament_results"].find_one({
        "provider_id": provider_id,
        "tournament_id": tournament_id,
        "result_version": result_version,
    })
    if existing:
        if existing.get("payload_hash") == payload_hash:
            log_funnel_event("leaderboard_duplicate", campaign_id=campaign_id, provider_id=provider_id,
                              submission_id=existing["submission_id"], source="provider",
                              event_id=deterministic_event_id("leaderboard_duplicate", existing['submission_id']))
            _record_success(provider_id)
            return jsonify({
                "status": "ok",
                "submission_id": existing["submission_id"],
                "status_value": existing.get("status"),
                "winner_count": existing.get("winner_count"),
                "matched_users": existing.get("matched_users"),
                "unmatched_users": existing.get("unmatched_users"),
                "duplicate": True,
            })
        return jsonify({"status": "error", "code": "conflict_same_version_different_payload"}), 409

    higher_existing = database.db["tournament_results"].find_one(
        {"provider_id": provider_id, "tournament_id": tournament_id, "result_version": {"$gt": result_version}}
    )
    if higher_existing:
        log_funnel_event("leaderboard_version_conflict", campaign_id=campaign_id, provider_id=provider_id,
                          source="provider", status="fail", reason="lower_result_version_rejected")
        return jsonify({"status": "error", "code": "lower_result_version_rejected"}), 409

    winners = body["winners"]
    known_uids = {int(w["telegram_user_id"]) for w in winners}
    matched = database.db["users"].count_documents({"user_id": {"$in": list(known_uids)}}) if known_uids else 0

    from bson.objectid import ObjectId

    submission_id = f"tr_{ObjectId()}"
    now = datetime.now(timezone.utc)
    doc = {
        "submission_id": submission_id,
        "provider_id": provider_id,
        "campaign_id": campaign_id,
        "tournament_id": tournament_id,
        "result_version": result_version,
        "status": "pending_review",
        "finalized_at": body.get("finalized_at"),
        "received_at": now,
        "payload_hash": payload_hash,
        "winner_count": len(winners),
        "matched_users": matched,
        "unmatched_users": len(winners) - matched,
        "winners": winners,
        "reviewed_by": None,
        "reviewed_at": None,
        "rejection_reason": None,
    }
    database.db["tournament_results"].insert_one(doc)
    _record_success(provider_id)
    log_funnel_event("leaderboard_received", campaign_id=campaign_id, provider_id=provider_id,
                      campaign_type=campaign.get("type"), submission_id=submission_id, source="provider",
                      event_id=deterministic_event_id("leaderboard_received", submission_id), winner_count=len(winners))

    return jsonify({
        "status": "ok",
        "submission_id": submission_id,
        "status_value": "pending_review",
        "winner_count": len(winners),
        "matched_users": matched,
        "unmatched_users": len(winners) - matched,
        "duplicate": False,
    }), 201


@tournament_integration_bp.get("/api/integrations/tournaments/results/<submission_id>")
def tournament_result_status(submission_id: str):
    raw_body = b""
    provider, err = _verify_hmac_request(raw_body)
    if err:
        return jsonify({"status": "error", "code": err}), 401 if err != "unknown_provider" else 404

    doc = database.db["tournament_results"].find_one({"submission_id": submission_id})
    if not doc or doc.get("provider_id") != provider["provider_id"]:
        return jsonify({"status": "error", "code": "not_found"}), 404

    rewards = list(database.db["campaign_rewards"].find({"submission_id": submission_id}))
    assigned_count = sum(1 for r in rewards if r.get("status") == "assigned")
    out_of_stock_count = sum(1 for r in rewards if r.get("status") == "out_of_stock")
    pending_count = sum(1 for r in rewards if r.get("status") in ("pending_review", "approved", "allocating"))

    return jsonify({
        "submission_id": submission_id,
        "status": doc.get("status"),
        "winner_count": doc.get("winner_count"),
        "assigned_count": assigned_count,
        "out_of_stock_count": out_of_stock_count,
        "pending_review_count": pending_count,
    })
