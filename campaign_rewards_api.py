"""User-facing Campaign Rewards — a generic reward centre (Phase 14).

Not tournament-only: the ``campaign_rewards`` collection holds one document
per reward regardless of category (tournament, referral, lucky_draw,
mission, cashback, welcome, ...). Today only the tournament approval flow
(tournament_rewards.py) writes rows here; adding a new reward-producing
campaign type means writing into the same collection with a different
``category``/``reward_rule_id``, not adding a new API or collection.

Ownership is always derived from the authenticated Mini App session
(miniapp_identity.resolve_authenticated_telegram_user_id) — never from a
client-supplied user_id/uid parameter.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify

import database
from campaign_centre import get_campaign, log_funnel_event
from miniapp_identity import resolve_authenticated_telegram_user_id
from mission_pool import resolve_mechanic

logger = logging.getLogger(__name__)

campaign_rewards_bp = Blueprint("campaign_rewards", __name__)


def _visible_reward(doc: dict, now: datetime) -> bool:
    if doc.get("status") != "assigned":
        return False
    expires_at = doc.get("expires_at")
    if expires_at and now >= expires_at:
        return False
    return True


@campaign_rewards_bp.get("/api/campaign-rewards/me")
def my_campaign_rewards():
    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    now = datetime.now(timezone.utc)
    docs = list(
        database.db["campaign_rewards"].find(
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
        # Every key below the marker existed before Mission Pool and is
        # unchanged; Mission Pool only ADDS keys, so an older Mini App build
        # keeps rendering tournament rewards exactly as it did.
        item = {
            "reward_id": d["reward_id"],
            "category": d.get("category", "tournament"),
            "campaign_id": d.get("campaign_id"),
            "campaign_name": campaign.get("name", ""),
            "tournament_id": d.get("tournament_id"),
            "rank": d.get("rank"),
            "reward_label": d.get("reward_label", ""),
            "voucher_code": d.get("voucher_code"),
            "assigned_at": d["assigned_at"].isoformat() if d.get("assigned_at") else None,
            "expires_at": d["expires_at"].isoformat() if d.get("expires_at") else None,
            "status": d.get("status"),
        }
        # --- additive Mission Pool context (§27.4) ---
        item["mechanic"] = resolve_mechanic(campaign)
        if d.get("category") == "mission_pool":
            item["is_winner"] = True
            item["winner_popup_pending"] = bool(d.get("winner_popup_pending"))
            item["notification_status"] = d.get("notification_status")
        rewards.append(item)

    return jsonify({"status": "ok", "rewards": rewards})


@campaign_rewards_bp.post("/api/campaign-rewards/<reward_id>/ack-popup")
def acknowledge_winner_popup(reward_id: str):
    """Durable one-shot acknowledgement for the Mission Pool winner popup
    (§27.3). The popup is driven by ``winner_popup_pending`` on the reward
    row itself, so it survives app restarts and never re-appears on every
    Mini App open once acknowledged.

    Acknowledging is purely presentational: it never touches
    ``status``/``voucher_code``, so the voucher stays owned by the winner and
    stays visible in Campaign Rewards regardless."""
    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err
    doc = database.db["campaign_rewards"].find_one({"reward_id": reward_id})
    if not doc or doc.get("telegram_user_id") != uid:
        return jsonify({"status": "error", "code": "not_found"}), 404

    now = datetime.now(timezone.utc)
    database.db["campaign_rewards"].update_one(
        {"reward_id": reward_id, "winner_popup_pending": True},
        {"$set": {
            "winner_popup_pending": False,
            "winner_popup_shown_at": doc.get("winner_popup_shown_at") or now,
            "winner_popup_acknowledged_at": now,
            "updated_at": now,
        }},
    )
    log_funnel_event("mission_winner_popup_acknowledged",
                     campaign_id=doc.get("campaign_id", ""), user_id=uid, reward_id=reward_id)
    return jsonify({"status": "ok"})


@campaign_rewards_bp.post("/api/campaign-rewards/<reward_id>/view")
def view_campaign_reward(reward_id: str):
    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err
    doc = database.db["campaign_rewards"].find_one({"reward_id": reward_id})
    if not doc or doc.get("telegram_user_id") != uid:
        return jsonify({"status": "error", "code": "not_found"}), 404

    database.db["campaign_rewards"].update_one(
        {"reward_id": reward_id, "first_viewed_at": None},
        {"$set": {"first_viewed_at": datetime.now(timezone.utc)}},
    )
    log_funnel_event("reward_viewed", campaign_id=doc.get("campaign_id", ""), user_id=uid, reward_id=reward_id)
    return jsonify({"status": "ok"})


@campaign_rewards_bp.post("/api/campaign-rewards/<reward_id>/copy")
def copy_campaign_reward(reward_id: str):
    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err
    doc = database.db["campaign_rewards"].find_one({"reward_id": reward_id})
    if not doc or doc.get("telegram_user_id") != uid:
        return jsonify({"status": "error", "code": "not_found"}), 404

    database.db["campaign_rewards"].update_one(
        {"reward_id": reward_id}, {"$set": {"copied_at": datetime.now(timezone.utc)}}
    )
    if doc.get("pool_id") and doc.get("voucher_code"):
        database.db["voucher_pools"].update_one(
            {"pool_id": doc.get("pool_id"), "code": doc.get("voucher_code")},
            {"$set": {"copied_at": datetime.now(timezone.utc)}},
        )
    log_funnel_event("voucher_copied", campaign_id=doc.get("campaign_id", ""), user_id=uid, reward_id=reward_id)
    return jsonify({"status": "ok"})
