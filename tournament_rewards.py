"""Tournament reward allocation, admin approval, and AP-owned voucher pools
for Campaign Rewards (Phases 8, 11, 12, 13).

Voucher pools/codes here are new collections rather than an extension of the
existing ``voucher_pools``/``vouchers`` collections in vouchers.py: those are
tightly coupled to the public/my-region drop-claim system (personalised
assignment by username, region pools, claim ledgers) and are not safe to
repurpose for rank-based tournament rewards without risking the existing
claim flows. The new collections follow the same production pattern (atomic
``find_one_and_update`` allocation, explicit status machine) under the
Campaign Centre's own namespace, and the Admin Dashboard exposes them as an
extension of the Voucher Centre concept scoped to tournament rewards.

Collections: ``campaign_voucher_pools``, ``campaign_voucher_codes``,
``tournament_rewards``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument

import database
from campaign_centre import get_campaign, log_funnel_event

logger = logging.getLogger(__name__)

tournament_rewards_bp = Blueprint("tournament_rewards", __name__)

REWARD_STATUSES = [
    "pending_review", "approved", "allocating", "assigned",
    "out_of_stock", "rejected", "expired",
]
VOUCHER_CODE_STATUSES = ["available", "reserved", "assigned", "expired", "disabled"]


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        pools = database.db["campaign_voucher_pools"]
        pools.create_index([("pool_id", 1)], name="ux_campaign_voucher_pools_pool_id", unique=True)
        pools.create_index([("campaign_id", 1)], name="ix_campaign_voucher_pools_campaign")

        codes = database.db["campaign_voucher_codes"]
        codes.create_index([("pool_id", 1), ("code", 1)], name="ux_campaign_voucher_codes_pool_code", unique=True)
        codes.create_index([("pool_id", 1), ("status", 1)], name="ix_campaign_voucher_codes_pool_status")
        codes.create_index([("assigned_to_user_id", 1)], name="ix_campaign_voucher_codes_assigned_user")

        rewards = database.db["tournament_rewards"]
        rewards.create_index(
            [("tournament_id", 1), ("telegram_user_id", 1)], name="ux_tournament_rewards_identity", unique=True
        )
        rewards.create_index([("submission_id", 1)], name="ix_tournament_rewards_submission")
        rewards.create_index([("status", 1)], name="ix_tournament_rewards_status")
        rewards.create_index([("campaign_id", 1)], name="ix_tournament_rewards_campaign")
        rewards.create_index([("pool_id", 1)], name="ix_tournament_rewards_pool")
        rewards.create_index([("telegram_user_id", 1)], name="ix_tournament_rewards_user")
    except Exception:
        logger.warning("[TOURNAMENT_REWARDS] index creation failed", exc_info=True)


_ensure_indexes()


def _log_audit(action: str, admin: dict, entity_id: str, details: dict | None = None) -> None:
    try:
        database.db["campaign_admin_audit_log"].insert_one({
            "action": action,
            "entity": "tournament_result",
            "entity_id": entity_id,
            "admin": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
            "details": details or {},
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[TOURNAMENT_REWARDS] audit_write_failed", exc_info=True)


def _rank_to_rule(campaign: dict, rank: int) -> dict | None:
    rules = (campaign.get("reward_config") or {}).get("rules") or []
    for rule in rules:
        if int(rule["min_rank"]) <= rank <= int(rule["max_rank"]):
            return rule
    return None


def _stock_by_pool(pool_ids: set) -> dict:
    out = {}
    for pool_id in pool_ids:
        pool = database.db["campaign_voucher_pools"].find_one({"pool_id": pool_id})
        available = database.db["campaign_voucher_codes"].count_documents({"pool_id": pool_id, "status": "available"})
        out[pool_id] = {
            "available": available,
            "pool_active": bool(pool and pool.get("status") == "active"),
            "pool_exists": bool(pool),
        }
    return out


def _reward_id() -> str:
    return f"rw_{ObjectId()}"


def _create_or_confirm_rewards(submission: dict, campaign: dict) -> list[dict]:
    """Idempotently create pending_review reward rows for a submission's
    winners. Unique index on (tournament_id, telegram_user_id) makes this
    safe to call repeatedly (result replay / re-approval)."""
    created = []
    for w in submission["winners"]:
        rank = int(w["rank"])
        rule = _rank_to_rule(campaign, rank)
        if not rule:
            continue
        existing = database.db["tournament_rewards"].find_one({
            "tournament_id": submission["tournament_id"],
            "telegram_user_id": int(w["telegram_user_id"]),
        })
        if existing:
            created.append(existing)
            continue
        now = datetime.now(timezone.utc)
        doc = {
            "reward_id": _reward_id(),
            "submission_id": submission["submission_id"],
            "campaign_id": submission["campaign_id"],
            "tournament_id": submission["tournament_id"],
            "telegram_user_id": int(w["telegram_user_id"]),
            "rank": rank,
            "score": w.get("score"),
            "reward_rule_id": rule["rule_id"],
            "reward_label": rule.get("reward_label", ""),
            "pool_id": rule["pool_id"],
            "voucher_code": None,
            "status": "pending_review",
            "assigned_at": None,
            "first_viewed_at": None,
            "copied_at": None,
            "created_at": now,
            "updated_at": now,
        }
        try:
            database.db["tournament_rewards"].insert_one(doc)
            log_funnel_event("reward_created", campaign_id=submission["campaign_id"], user_id=doc["telegram_user_id"],
                              reward_id=doc["reward_id"])
            created.append(doc)
        except Exception as exc:
            if "duplicate" in str(exc).lower():
                created.append(database.db["tournament_rewards"].find_one({
                    "tournament_id": submission["tournament_id"],
                    "telegram_user_id": int(w["telegram_user_id"]),
                }))
            else:
                raise
    return created


def _atomic_allocate_voucher(pool_id: str, reward: dict) -> dict:
    """Atomically reserve+assign one available code from a pool to a reward.
    Idempotent: if the reward already has an assigned code, returns it
    unchanged rather than allocating a second one."""
    reward_id = reward["reward_id"]

    already = database.db["tournament_rewards"].find_one({"reward_id": reward_id, "status": "assigned"})
    if already:
        return already

    code_doc = database.db["campaign_voucher_codes"].find_one_and_update(
        {"pool_id": pool_id, "status": "available"},
        {"$set": {
            "status": "reserved",
            "reserved_for_reward_id": reward_id,
            "updated_at": datetime.now(timezone.utc),
        }},
        sort=[("created_at", 1), ("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )
    if not code_doc:
        database.db["tournament_rewards"].update_one(
            {"reward_id": reward_id, "status": {"$in": ["pending_review", "approved", "allocating"]}},
            {"$set": {"status": "out_of_stock", "updated_at": datetime.now(timezone.utc)}},
        )
        log_funnel_event("voucher_out_of_stock", campaign_id=reward["campaign_id"], user_id=reward["telegram_user_id"],
                          reward_id=reward_id, pool_id=pool_id)
        return database.db["tournament_rewards"].find_one({"reward_id": reward_id})

    log_funnel_event("voucher_reserved", campaign_id=reward["campaign_id"], user_id=reward["telegram_user_id"],
                      reward_id=reward_id, pool_id=pool_id)

    now = datetime.now(timezone.utc)
    database.db["campaign_voucher_codes"].update_one(
        {"_id": code_doc["_id"], "status": "reserved", "reserved_for_reward_id": reward_id},
        {"$set": {"status": "assigned", "assigned_to_user_id": reward["telegram_user_id"], "assigned_at": now}},
    )
    database.db["tournament_rewards"].update_one(
        {"reward_id": reward_id, "status": {"$in": ["pending_review", "approved", "allocating"]}},
        {"$set": {"voucher_code": code_doc["code"], "status": "assigned", "assigned_at": now, "updated_at": now}},
    )
    log_funnel_event("voucher_assigned", campaign_id=reward["campaign_id"], user_id=reward["telegram_user_id"],
                      reward_id=reward_id, pool_id=pool_id)
    return database.db["tournament_rewards"].find_one({"reward_id": reward_id})


def approve_submission(submission_id: str, admin: dict) -> tuple[dict | None, str | None]:
    """Full approval flow: lock -> recheck -> create rewards -> allocate.
    Idempotent — repeated calls never allocate a second voucher per reward."""
    locked = database.db["tournament_results"].find_one_and_update(
        {"submission_id": submission_id, "status": {"$in": ["pending_review", "approved", "allocating"]}},
        {"$set": {"status": "allocating"}},
        return_document=ReturnDocument.AFTER,
    )
    if not locked:
        existing = database.db["tournament_results"].find_one({"submission_id": submission_id})
        if not existing:
            return None, "not_found"
        if existing.get("status") == "assigned":
            return existing, None
        return None, "not_pending_review"

    campaign = get_campaign(locked["campaign_id"])
    from campaign_providers import get_provider, provider_is_usable_for_results

    provider = get_provider(locked["provider_id"])
    if not campaign or not provider_is_usable_for_results(provider):
        database.db["tournament_results"].update_one(
            {"submission_id": submission_id}, {"$set": {"status": "pending_review"}}
        )
        return None, "campaign_or_provider_invalid"

    rewards = _create_or_confirm_rewards(locked, campaign)

    pool_ids = {r["pool_id"] for r in rewards if r}
    stock = _stock_by_pool(pool_ids)
    required_by_pool: dict = {}
    for r in rewards:
        if r.get("status") in ("pending_review", "approved"):
            required_by_pool[r["pool_id"]] = required_by_pool.get(r["pool_id"], 0) + 1

    insufficient = [
        pid for pid, needed in required_by_pool.items()
        if not stock.get(pid, {}).get("pool_active") or stock.get(pid, {}).get("available", 0) < needed
    ]
    allow_partial = bool((request.get_json(silent=True) or {}).get("allow_partial_allocation"))
    if insufficient and not allow_partial:
        database.db["tournament_results"].update_one(
            {"submission_id": submission_id}, {"$set": {"status": "pending_review"}}
        )
        return {"insufficient_pools": insufficient, "stock": stock}, "insufficient_stock"

    for r in rewards:
        if r.get("status") in ("pending_review",):
            database.db["tournament_rewards"].update_one(
                {"reward_id": r["reward_id"]}, {"$set": {"status": "approved", "updated_at": datetime.now(timezone.utc)}}
            )

    final_rewards = []
    for r in rewards:
        r = database.db["tournament_rewards"].find_one({"reward_id": r["reward_id"]})
        if r.get("status") in ("approved",):
            r = _atomic_allocate_voucher(r["pool_id"], r)
        final_rewards.append(r)

    any_out_of_stock = any(r.get("status") == "out_of_stock" for r in final_rewards)
    final_status = "out_of_stock" if any_out_of_stock else "assigned"
    database.db["tournament_results"].update_one(
        {"submission_id": submission_id},
        {"$set": {"status": final_status, "reviewed_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
                  "reviewed_at": datetime.now(timezone.utc)}},
    )
    log_funnel_event("result_approved", campaign_id=locked["campaign_id"], submission_id=submission_id)
    _log_audit("result_approved", admin, submission_id, {"final_status": final_status})

    result = database.db["tournament_results"].find_one({"submission_id": submission_id})
    return result, None


# ---------------------------------------------------------------------------
# Admin: tournament results
# ---------------------------------------------------------------------------

def _serialize_result(doc: dict) -> dict:
    out = dict(doc)
    out.pop("_id", None)
    if isinstance(out.get("received_at"), datetime):
        out["received_at"] = out["received_at"].isoformat()
    if isinstance(out.get("reviewed_at"), datetime):
        out["reviewed_at"] = out["reviewed_at"].isoformat()
    return out


@tournament_rewards_bp.get("/api/admin/tournament-results")
def list_tournament_results():
    _, err = _require_admin()
    if err:
        return err
    query = {}
    status_filter = request.args.get("status")
    if status_filter:
        query["status"] = status_filter
    campaign_filter = request.args.get("campaign_id")
    if campaign_filter:
        query["campaign_id"] = campaign_filter
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    docs = list(database.db["tournament_results"].find(query, sort=[("received_at", -1)], limit=limit))
    return jsonify({"status": "ok", "results": [_serialize_result(d) for d in docs]})


@tournament_rewards_bp.get("/api/admin/tournament-results/<submission_id>")
def get_tournament_result(submission_id: str):
    _, err = _require_admin()
    if err:
        return err
    doc = database.db["tournament_results"].find_one({"submission_id": submission_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    rewards = list(database.db["tournament_rewards"].find({"submission_id": submission_id}))
    for r in rewards:
        r.pop("_id", None)
    pool_ids = {r["pool_id"] for r in rewards}
    stock = _stock_by_pool(pool_ids)
    return jsonify({"status": "ok", "result": _serialize_result(doc), "rewards": rewards, "stock_by_pool": stock})


@tournament_rewards_bp.post("/api/admin/tournament-results/<submission_id>/approve")
def approve_tournament_result(submission_id: str):
    admin, err = _require_admin()
    if err:
        return err
    result, error_code = approve_submission(submission_id, admin)
    if error_code == "not_found":
        return jsonify({"status": "error", "code": "not_found"}), 404
    if error_code == "not_pending_review":
        return jsonify({"status": "error", "code": "not_pending_review"}), 409
    if error_code == "insufficient_stock":
        return jsonify({"status": "error", "code": "insufficient_stock", **(result or {})}), 409
    if error_code == "campaign_or_provider_invalid":
        return jsonify({"status": "error", "code": error_code}), 400
    return jsonify({"status": "ok", "result": _serialize_result(result)})


@tournament_rewards_bp.post("/api/admin/tournament-results/<submission_id>/reject")
def reject_tournament_result(submission_id: str):
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    reason = (body.get("reason") or "").strip()
    result = database.db["tournament_results"].find_one_and_update(
        {"submission_id": submission_id, "status": "pending_review"},
        {"$set": {"status": "rejected", "rejection_reason": reason,
                  "reviewed_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
                  "reviewed_at": datetime.now(timezone.utc)}},
        return_document=ReturnDocument.AFTER,
    )
    if not result:
        return jsonify({"status": "error", "code": "not_pending_review"}), 409
    log_funnel_event("result_rejected", campaign_id=result["campaign_id"], submission_id=submission_id, reason=reason)
    _log_audit("result_rejected", admin, submission_id, {"reason": reason})
    return jsonify({"status": "ok", "result": _serialize_result(result)})


@tournament_rewards_bp.post("/api/admin/tournament-results/<submission_id>/request-correction")
def request_correction(submission_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = database.db["tournament_results"].find_one({"submission_id": submission_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    has_assigned = database.db["tournament_rewards"].count_documents(
        {"submission_id": submission_id, "status": "assigned"}
    ) > 0
    database.db["tournament_results"].update_one(
        {"submission_id": submission_id},
        {"$set": {"status": "corrected", "reviewed_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
                  "reviewed_at": datetime.now(timezone.utc)}},
    )
    log_funnel_event("result_corrected", campaign_id=doc["campaign_id"], submission_id=submission_id)
    _log_audit("result_correction_requested", admin, submission_id, {"had_assigned_rewards": has_assigned})
    return jsonify({"status": "ok", "requires_manual_review": has_assigned})


@tournament_rewards_bp.post("/api/admin/tournament-results/<submission_id>/retry-allocation")
def retry_allocation(submission_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = database.db["tournament_results"].find_one({"submission_id": submission_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if doc.get("status") not in ("out_of_stock", "approved", "allocating"):
        return jsonify({"status": "error", "code": "invalid_state_for_retry"}), 409

    rewards = list(database.db["tournament_rewards"].find(
        {"submission_id": submission_id, "status": {"$in": ["approved", "out_of_stock"]}}
    ))
    for r in rewards:
        if r.get("status") == "out_of_stock":
            database.db["tournament_rewards"].update_one({"reward_id": r["reward_id"]}, {"$set": {"status": "approved"}})
            r["status"] = "approved"
        _atomic_allocate_voucher(r["pool_id"], r)

    final_rewards = list(database.db["tournament_rewards"].find({"submission_id": submission_id}))
    any_out_of_stock = any(r.get("status") == "out_of_stock" for r in final_rewards)
    final_status = "out_of_stock" if any_out_of_stock else "assigned"
    database.db["tournament_results"].update_one({"submission_id": submission_id}, {"$set": {"status": final_status}})
    _log_audit("retry_allocation", admin, submission_id, {"final_status": final_status})
    return jsonify({"status": "ok", "result_status": final_status})


# ---------------------------------------------------------------------------
# Admin: campaign-scoped voucher pools (Voucher Centre extension)
# ---------------------------------------------------------------------------

@tournament_rewards_bp.get("/api/admin/campaign-centre/voucher-pools")
def list_voucher_pools():
    _, err = _require_admin()
    if err:
        return err
    campaign_id = request.args.get("campaign_id")
    query = {"campaign_id": campaign_id} if campaign_id else {}
    pools = list(database.db["campaign_voucher_pools"].find(query, sort=[("created_at", -1)], limit=200))
    out = []
    for p in pools:
        p.pop("_id", None)
        counts = {}
        for status in VOUCHER_CODE_STATUSES:
            counts[status] = database.db["campaign_voucher_codes"].count_documents(
                {"pool_id": p["pool_id"], "status": status}
            )
        p["counts"] = counts
        out.append(p)
    return jsonify({"status": "ok", "pools": out})


@tournament_rewards_bp.post("/api/admin/campaign-centre/voucher-pools")
def create_voucher_pool():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    pool_id = (body.get("pool_id") or "").strip()
    name = (body.get("name") or "").strip()
    if not pool_id or not name:
        return jsonify({"status": "error", "code": "missing_fields"}), 400
    now = datetime.now(timezone.utc)
    doc = {
        "pool_id": pool_id,
        "name": name,
        "pool_type": body.get("pool_type") or "tournament_reward",
        "campaign_id": body.get("campaign_id") or "",
        "status": "active",
        "created_at": now,
        "updated_at": now,
    }
    try:
        database.db["campaign_voucher_pools"].insert_one(doc)
    except Exception as exc:
        if "duplicate" in str(exc).lower():
            return jsonify({"status": "error", "code": "duplicate_pool_id"}), 409
        raise
    _log_audit("voucher_pool_created", admin, pool_id)
    return jsonify({"status": "ok", "pool_id": pool_id}), 201


@tournament_rewards_bp.post("/api/admin/campaign-centre/voucher-pools/<pool_id>/upload-codes")
def upload_voucher_codes(pool_id: str):
    admin, err = _require_admin()
    if err:
        return err
    pool = database.db["campaign_voucher_pools"].find_one({"pool_id": pool_id})
    if not pool:
        return jsonify({"status": "error", "code": "pool_not_found"}), 404
    body = request.get_json(force=True, silent=True) or {}
    codes = body.get("codes") or []
    if not isinstance(codes, list) or not codes:
        return jsonify({"status": "error", "code": "missing_codes"}), 400

    now = datetime.now(timezone.utc)
    inserted, skipped = 0, 0
    for raw_code in codes:
        code = str(raw_code).strip()
        if not code:
            continue
        try:
            database.db["campaign_voucher_codes"].insert_one({
                "pool_id": pool_id,
                "code": code,
                "status": "available",
                "reserved_for_reward_id": None,
                "assigned_to_user_id": None,
                "assigned_at": None,
                "first_viewed_at": None,
                "copied_at": None,
                "created_at": now,
                "updated_at": now,
            })
            inserted += 1
        except Exception as exc:
            if "duplicate" in str(exc).lower():
                skipped += 1
            else:
                raise
    _log_audit("voucher_codes_uploaded", admin, pool_id, {"inserted": inserted, "skipped": skipped})
    return jsonify({"status": "ok", "inserted": inserted, "skipped_duplicates": skipped})


# ---------------------------------------------------------------------------
# Admin: reward allocations list (Phase 18 tab)
# ---------------------------------------------------------------------------

@tournament_rewards_bp.get("/api/admin/reward-allocations")
def list_reward_allocations():
    _, err = _require_admin()
    if err:
        return err
    query: dict = {}
    for field in ("campaign_id", "tournament_id", "status", "pool_id"):
        val = request.args.get(field)
        if val:
            query[field] = val
    uid = request.args.get("telegram_user_id")
    if uid:
        try:
            query["telegram_user_id"] = int(uid)
        except ValueError:
            return jsonify({"status": "error", "code": "invalid_telegram_user_id"}), 400

    limit = min(int(request.args.get("limit", 50) or 50), 200)
    docs = list(database.db["tournament_rewards"].find(query, sort=[("created_at", -1)], limit=limit))
    for d in docs:
        d.pop("_id", None)
        for k in ("assigned_at", "first_viewed_at", "copied_at", "created_at", "updated_at"):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
    return jsonify({"status": "ok", "rewards": docs})
