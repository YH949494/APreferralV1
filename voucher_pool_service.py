"""Thin wrapper around the existing Voucher Centre inventory table
(``db.voucher_pools``, owned by ``affiliate_rewards.py`` / ``vouchers.py``)
so Campaign Centre reward allocation reuses the same physical voucher
inventory instead of maintaining a second one.

Voucher Centre remains the owner of:
  - the code inventory itself (``db.voucher_pools`` rows: pool_id, code,
    status, issued_to, issued_at, ...) — this module never redefines that
    shape, it only inserts/queries/atomically-claims rows in it.
  - the unique (pool_id, code) index and pool_id/status index, already
    created by ``affiliate_rewards.ensure_affiliate_indexes``.

This module adds exactly one new, additive concept: a lightweight **pool
registry** (``voucher_pool_registry``) that records catalog-level metadata a
pool doesn't otherwise have — ``pool_type``, ``campaign_id``, the intended
``reward_usage``, and free-form ``reward_metadata``. It is not a second
inventory: no voucher codes are ever stored here, only one small document
per pool_id.

Any consumer (Campaign Centre reward allocation today; affiliate/welcome
tiers already use ``db.voucher_pools`` directly and are untouched) can share
the same pool_id namespace as long as pool_id values don't collide, which
admins control the same way they always have (choosing a pool_id when
uploading codes).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pymongo import ReturnDocument

import database

logger = logging.getLogger(__name__)

POOL_TYPES = ["welcome", "voucher_drop", "tournament_reward", "referral", "vip", "cashback"]
CODE_STATUSES = ["available", "issued"]

# Pool IDs already owned by the legacy affiliate/welcome-tier flow
# (vouchers.py admin_pools_upload_v2 / affiliate_rewards.py). Registering or
# uploading into one of these from Campaign Centre is refused outright so a
# reward pool can never accidentally share a pool_id with — and therefore
# never contend for stock with — an existing affiliate/welcome tier.
RESERVED_LEGACY_POOL_IDS = frozenset({"WELCOME", "T1", "T2", "T3", "T4", "T5"})

# Marker written onto every code row this module inserts. allocate_voucher()
# requires this marker on the filter side too, so even in the hypothetical
# case of a pool_id collision, Campaign Centre allocation can never match
# (and therefore never consume) a code row that a legacy flow inserted
# without this marker, and vice versa the legacy flows' own filters never
# check for it so they keep working unchanged either way.
_POOL_SOURCE = "campaign_centre"


class ReservedPoolIdError(ValueError):
    pass


def _reject_reserved_pool_id(pool_id: str) -> None:
    if str(pool_id).strip().upper() in RESERVED_LEGACY_POOL_IDS:
        raise ReservedPoolIdError(
            f"pool_id {pool_id!r} is reserved for the legacy affiliate/welcome voucher flow"
        )


def _ensure_indexes() -> None:
    try:
        registry = database.db["voucher_pool_registry"]
        registry.create_index([("pool_id", 1)], name="ux_voucher_pool_registry_pool_id", unique=True)
        registry.create_index([("campaign_id", 1)], name="ix_voucher_pool_registry_campaign_id")
        registry.create_index([("pool_type", 1)], name="ix_voucher_pool_registry_pool_type")
    except Exception:
        logger.warning("[VOUCHER_POOL_SERVICE] index creation failed", exc_info=True)


_ensure_indexes()


def register_pool(
    pool_id: str,
    *,
    name: str,
    pool_type: str = "tournament_reward",
    campaign_id: str = "",
    reward_usage: str = "",
    reward_metadata: dict | None = None,
) -> dict:
    """Create or update the catalog entry for a pool_id. Never touches the
    code inventory itself."""
    _reject_reserved_pool_id(pool_id)
    now = datetime.now(timezone.utc)
    update = {
        "$set": {
            "name": name,
            "pool_type": pool_type,
            "campaign_id": campaign_id,
            "reward_usage": reward_usage,
            "reward_metadata": reward_metadata or {},
            "updated_at": now,
        },
        "$setOnInsert": {"pool_id": pool_id, "status": "active", "created_at": now},
    }
    database.db["voucher_pool_registry"].update_one({"pool_id": pool_id}, update, upsert=True)
    return get_pool(pool_id)


def get_pool(pool_id: str) -> dict | None:
    return database.db["voucher_pool_registry"].find_one({"pool_id": pool_id})


def list_pools(campaign_id: str | None = None, pool_type: str | None = None) -> list[dict]:
    query: dict = {}
    if campaign_id:
        query["campaign_id"] = campaign_id
    if pool_type:
        query["pool_type"] = pool_type
    return list(database.db["voucher_pool_registry"].find(query, sort=[("created_at", -1)], limit=200))


def set_pool_status(pool_id: str, status: str) -> None:
    database.db["voucher_pool_registry"].update_one(
        {"pool_id": pool_id}, {"$set": {"status": status, "updated_at": datetime.now(timezone.utc)}}
    )


def pool_is_active(pool_id: str) -> bool:
    pool = get_pool(pool_id)
    return bool(pool and pool.get("status") == "active")


def pool_stock(pool_id: str) -> dict:
    """Counts are scoped to pool_source="campaign_centre" so a stock report
    can never be inflated by an unrelated legacy row that happened to share
    a pool_id (which registration/upload already refuse for reserved ids;
    this is defense in depth for the read side too)."""
    base = {"pool_id": pool_id, "pool_source": _POOL_SOURCE}
    available = database.db["voucher_pools"].count_documents({**base, "status": "available"})
    issued = database.db["voucher_pools"].count_documents({**base, "status": "issued"})
    return {"available": available, "issued": issued}


def upload_codes(pool_id: str, codes: list[str], *, display_label: str = "", value_hint: str = "", currency: str = "") -> dict:
    """Insert codes into the shared Voucher Centre inventory table
    (``db.voucher_pools``) — the exact same collection/shape the existing
    affiliate voucher-pool upload flow writes to, tagged with
    ``pool_source: "campaign_centre"`` so allocate_voucher() can never pick
    up a legacy-inserted row (which never carries that field) even if a
    pool_id were ever reused."""
    _reject_reserved_pool_id(pool_id)
    now = datetime.now(timezone.utc)
    inserted, skipped = 0, 0
    for raw_code in codes:
        code = str(raw_code).strip()
        if not code:
            continue
        try:
            database.db["voucher_pools"].insert_one({
                "pool_id": pool_id,
                "code": code,
                "status": "available",
                "issued_to": None,
                "issued_at": None,
                "ledger_id": None,
                "display_label": display_label or None,
                "value_hint": value_hint or None,
                "currency": currency or None,
                "created_at": now,
                "pool_source": _POOL_SOURCE,
            })
            inserted += 1
        except Exception as exc:
            if "duplicate" in str(exc).lower():
                skipped += 1
            else:
                raise
    return {"inserted": inserted, "skipped_duplicates": skipped}


def allocate_voucher(pool_id: str, *, reward_id: str, telegram_user_id: int, now: datetime | None = None) -> dict | None:
    """Atomically claim one available code from the shared inventory for a
    reward. Mirrors affiliate_rewards._claim_voucher_from_pool's atomic
    find_one_and_update pattern against the same collection, keyed by
    ``issued_for_reward_id`` instead of ``issued_for_ledger_id`` so the two
    consumers never contend over each other's field semantics. The filter
    additionally requires ``pool_source: "campaign_centre"`` (set by
    upload_codes above), so this can only ever claim a code this module
    itself uploaded — never a legacy affiliate/welcome-tier row, even in the
    hypothetical case of a pool_id collision."""
    now = now or datetime.now(timezone.utc)
    return database.db["voucher_pools"].find_one_and_update(
        {
            "pool_id": pool_id,
            "status": "available",
            "pool_source": _POOL_SOURCE,
            "$or": [
                {"issued_for_reward_id": {"$exists": False}},
                {"issued_for_reward_id": None},
            ],
        },
        {
            "$set": {
                "status": "issued",
                "issued_to": telegram_user_id,
                "issued_to_user_id": telegram_user_id,
                "issued_at": now,
                "issued_for_reward_id": reward_id,
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def voucher_already_allocated_for_reward(reward_id: str) -> dict | None:
    return database.db["voucher_pools"].find_one(
        {"issued_for_reward_id": reward_id, "status": "issued", "pool_source": _POOL_SOURCE}
    )
