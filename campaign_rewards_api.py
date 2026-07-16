"""User-facing Campaign Rewards (Phase 14).

Rewards are returned only to their verified Telegram-identity owner. Ownership
is always derived from verified Mini App initData — never from a client-
supplied user_id/uid parameter.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument

import database
from campaign_centre import get_campaign, log_funnel_event

logger = logging.getLogger(__name__)

campaign_rewards_bp = Blueprint("campaign_rewards", __name__)


def _verified_telegram_user_id():
    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data

    init_data_raw = extract_raw_init_data_from_query(request)
    if not init_data_raw:
        return None, (jsonify({"status": "error", "code": "missing_init_data"}), 400)
    ok, data, reason = verify_telegram_init_data(init_data_raw)
    if not ok:
        return None, (jsonify({"status": "error", "code": f"init_data_invalid:{reason}"}), 401)
    try:
        user_json = json.loads(data.get("user", "{}"))
        uid = int(user_json.get("id"))
    except Exception:
        return None, (jsonify({"status": "error", "code": "invalid_user"}), 401)
    return uid, None


def _visible_reward(doc: dict, now: datetime) -> bool:
    if doc.get("status") != "assigned":
        return False
    expires_at = doc.get("expires_at")
    if expires_at and now >= expires_at:
        return False
    return True


@campaign_rewards_bp.get("/api/campaign-rewards/me")
def my_campaign_rewards():
    uid, err = _verified_telegram_user_id()
    if err:
        return err

    now = datetime.now(timezone.utc)
    docs = list(
        database.db["tournament_rewards"].find(
            {"telegram_user_id": uid, "status": "assigned"},
            sort=[("assigned_at", -1)],
            limit=100,
        )
    )
    rewards = []
    for d in docs:
        if not _visible_reward(d, now):
            continue
        campaign = get_campaign(d.get("campaign_id", "")) or {}
        rewards.append({
            "reward_id": d["reward_id"],
            "campaign_id": d.get("campaign_id"),
            "campaign_name": campaign.get("name", ""),
            "tournament_id": d.get("tournament_id"),
            "rank": d.get("rank"),
            "reward_label": d.get("reward_label", ""),
            "voucher_code": d.get("voucher_code"),
            "assigned_at": d["assigned_at"].isoformat() if d.get("assigned_at") else None,
            "expires_at": d["expires_at"].isoformat() if d.get("expires_at") else None,
            "status": d.get("status"),
        })

    return jsonify({"status": "ok", "rewards": rewards})


@campaign_rewards_bp.post("/api/campaign-rewards/<reward_id>/view")
def view_campaign_reward(reward_id: str):
    uid, err = _verified_telegram_user_id()
    if err:
        return err
    doc = database.db["tournament_rewards"].find_one({"reward_id": reward_id})
    if not doc or doc.get("telegram_user_id") != uid:
        return jsonify({"status": "error", "code": "not_found"}), 404

    database.db["tournament_rewards"].find_one_and_update(
        {"reward_id": reward_id, "first_viewed_at": None},
        {"$set": {"first_viewed_at": datetime.now(timezone.utc)}},
        return_document=ReturnDocument.AFTER,
    )
    log_funnel_event("reward_viewed", campaign_id=doc.get("campaign_id", ""), user_id=uid, reward_id=reward_id)
    return jsonify({"status": "ok"})


@campaign_rewards_bp.post("/api/campaign-rewards/<reward_id>/copy")
def copy_campaign_reward(reward_id: str):
    uid, err = _verified_telegram_user_id()
    if err:
        return err
    doc = database.db["tournament_rewards"].find_one({"reward_id": reward_id})
    if not doc or doc.get("telegram_user_id") != uid:
        return jsonify({"status": "error", "code": "not_found"}), 404

    database.db["tournament_rewards"].update_one(
        {"reward_id": reward_id}, {"$set": {"copied_at": datetime.now(timezone.utc)}}
    )
    database.db["campaign_voucher_codes"].update_one(
        {"pool_id": doc.get("pool_id"), "code": doc.get("voucher_code")},
        {"$set": {"copied_at": datetime.now(timezone.utc)}},
    )
    log_funnel_event("voucher_copied", campaign_id=doc.get("campaign_id", ""), user_id=uid, reward_id=reward_id)
    return jsonify({"status": "ok"})
