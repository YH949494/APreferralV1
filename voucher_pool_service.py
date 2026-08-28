"""Thin wrapper around the existing Voucher Centre inventory table
(``db.voucher_pools``, owned by ``affiliate_rewards.py`` / ``vouchers.py``)
so Campaign Centre reward allocation reuses the same physical voucher
inventory instead of maintaining a second one.

Voucher Centre remains the owner of:
  - the code inventory itself (``db.voucher_pools`` rows) — this module
    never redefines that shape, it only inserts/queries/atomically-claims
    rows in it.
  - the unique (pool_id, code) index and pool_id/status index, already
    created by ``affiliate_rewards.ensure_affiliate_indexes``.

This module adds one new, additive concept: a **pool registry**
(``voucher_pool_registry``) that is the canonical, server-side source of
truth for two things every managed pool needs: ``pool_type`` (what kind of
reward it holds) and ``allocation_scope`` (which subsystem is allowed to
allocate from it — the actual isolation control). It is not a second
inventory: no voucher codes are ever stored here, only one small document
per pool_id.

Isolation model (explicit, not naming-based):
  - Every code row this module inserts is stamped, server-side, with the
    registry's own ``pool_type``/``allocation_scope``/``pool_source`` — the
    caller can never supply or override these.
  - ``allocate_voucher()`` (used by Campaign Centre) only ever matches rows
    with ``allocation_scope in ("campaign_rewards", "shared")``.
  - ``affiliate_rewards._claim_voucher_from_pool()`` was given a minimal,
    additive filter (see that module) that only matches rows with no
    ``allocation_scope`` (every pre-existing legacy row) or
    ``allocation_scope in ("affiliate_rewards", "shared")``.
  - The reserved legacy pool-id guard (WELCOME/T1-T5) is kept as
    defense-in-depth, not as the primary control.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pymongo import ReturnDocument

import database
from affiliate_reward_plans import DENOMINATION_POOL_IDS

logger = logging.getLogger(__name__)

POOL_TYPES = [
    "tournament_reward", "affiliate", "welcome", "vip",
    "voucher_drop", "referral", "cashback", "other",
]

ALLOCATION_SCOPES = [
    "campaign_rewards", "affiliate_rewards", "welcome_rewards",
    "voucher_drops", "referral_rewards", "shared",
]

# Scopes Campaign Centre reward allocation may draw from.
CAMPAIGN_ALLOCATABLE_SCOPES = frozenset({"campaign_rewards", "shared"})

CODE_STATUSES = ["available", "issued"]

# Pool IDs already owned by the legacy affiliate/welcome-tier flow
# (vouchers.py admin_pools_upload_v2 / affiliate_rewards.py). Registering or
# uploading into one of these from Campaign Centre is refused outright —
# kept as defense-in-depth alongside the explicit allocation_scope model.
RESERVED_LEGACY_POOL_IDS = frozenset(
    {"WELCOME", "T1", "T2", "T3", "T4", "T5"}
    # The standardized affiliate denomination pools (entitlement month
    # 202609 onward) are owned by the same affiliate flow and reserved on
    # exactly the same terms as the per-tier pools above.
    | set(DENOMINATION_POOL_IDS)
)

# Marker written onto every code row this module inserts, alongside
# pool_type/allocation_scope. Retained for audit/back-compat; the primary
# safety control is allocation_scope, not this field.
_POOL_SOURCE = "campaign_centre"


class VoucherPoolError(ValueError):
    """Raised with a structured .code for API-layer error mapping."""

    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code


class ReservedPoolIdError(VoucherPoolError):
    def __init__(self, pool_id: str):
        super().__init__("reserved_pool_id", f"pool_id {pool_id!r} is reserved for the legacy affiliate/welcome voucher flow")


def _reject_reserved_pool_id(pool_id: str) -> None:
    if str(pool_id).strip().upper() in RESERVED_LEGACY_POOL_IDS:
        raise ReservedPoolIdError(pool_id)


def _ensure_indexes() -> None:
    try:
        registry = database.db["voucher_pool_registry"]
        registry.create_index([("pool_id", 1)], name="ux_voucher_pool_registry_pool_id", unique=True)
        registry.create_index([("campaign_id", 1)], name="ix_voucher_pool_registry_campaign_id")
        registry.create_index([("pool_type", 1)], name="ix_voucher_pool_registry_pool_type")
        registry.create_index([("allocation_scope", 1)], name="ix_voucher_pool_registry_allocation_scope")
    except Exception:
        logger.warning("[VOUCHER_POOL_SERVICE] index creation failed", exc_info=True)


_ensure_indexes()


def _inventory_count(pool_id: str) -> int:
    """Any row at all for this pool_id — available, reserved-in-effect
    (issued_for_reward_id set), or issued. Used to block scope changes on a
    pool that already has inventory."""
    return database.db["voucher_pools"].count_documents({"pool_id": pool_id, "pool_source": _POOL_SOURCE})


def register_pool(
    pool_id: str,
    *,
    name: str,
    pool_type: str = "tournament_reward",
    allocation_scope: str = "campaign_rewards",
    campaign_id: str = "",
    reward_usage: str = "",
    reward_metadata: dict | None = None,
) -> dict:
    """Create or update the catalog entry for a pool_id. Never touches the
    code inventory itself.

    Idempotent for plain metadata (name/campaign_id/reward_usage/
    reward_metadata). Attempting to change ``pool_type`` or
    ``allocation_scope`` on an existing pool is rejected outright
    (``pool_scope_conflict``) — use ``migrate_pool_scope`` for that, which
    is the explicit, admin-only operation this is deliberately not."""
    _reject_reserved_pool_id(pool_id)
    if pool_type not in POOL_TYPES:
        raise VoucherPoolError("invalid_pool_type")
    if allocation_scope not in ALLOCATION_SCOPES:
        raise VoucherPoolError("invalid_allocation_scope")

    existing = get_pool(pool_id)
    if existing and (existing.get("pool_type") != pool_type or existing.get("allocation_scope") != allocation_scope):
        raise VoucherPoolError("pool_scope_conflict")

    now = datetime.now(timezone.utc)
    update = {
        "$set": {
            "name": name,
            "pool_type": pool_type,
            "allocation_scope": allocation_scope,
            "campaign_id": campaign_id,
            "reward_usage": reward_usage,
            "reward_metadata": reward_metadata or {},
            "updated_at": now,
        },
        "$setOnInsert": {"pool_id": pool_id, "status": "active", "created_at": now},
    }
    database.db["voucher_pool_registry"].update_one({"pool_id": pool_id}, update, upsert=True)
    return get_pool(pool_id)


def migrate_pool_scope(pool_id: str, *, pool_type: str | None = None, allocation_scope: str | None = None) -> dict:
    """Explicit admin migration operation: the only way to change an
    existing pool's pool_type/allocation_scope. Always blocked
    (``pool_has_inventory``) if the pool already has any code rows —
    intended for fixing a mis-registered pool before any codes are
    uploaded, never for reassigning live inventory."""
    pool = get_pool(pool_id)
    if not pool:
        raise VoucherPoolError("pool_not_found")
    if pool_type is not None and pool_type not in POOL_TYPES:
        raise VoucherPoolError("invalid_pool_type")
    if allocation_scope is not None and allocation_scope not in ALLOCATION_SCOPES:
        raise VoucherPoolError("invalid_allocation_scope")
    if _inventory_count(pool_id) > 0:
        raise VoucherPoolError("pool_has_inventory")

    updates = {"updated_at": datetime.now(timezone.utc)}
    if pool_type is not None:
        updates["pool_type"] = pool_type
    if allocation_scope is not None:
        updates["allocation_scope"] = allocation_scope
    database.db["voucher_pool_registry"].update_one({"pool_id": pool_id}, {"$set": updates})
    return get_pool(pool_id)


def get_pool(pool_id: str) -> dict | None:
    return database.db["voucher_pool_registry"].find_one({"pool_id": pool_id})


def list_pools(campaign_id: str | None = None, pool_type: str | None = None, allocation_scope: str | None = None) -> list[dict]:
    query: dict = {}
    if campaign_id:
        query["campaign_id"] = campaign_id
    if pool_type:
        query["pool_type"] = pool_type
    if allocation_scope:
        query["allocation_scope"] = allocation_scope
    return list(database.db["voucher_pool_registry"].find(query, sort=[("created_at", -1)], limit=200))


def set_pool_status(pool_id: str, status: str) -> None:
    database.db["voucher_pool_registry"].update_one(
        {"pool_id": pool_id}, {"$set": {"status": status, "updated_at": datetime.now(timezone.utc)}}
    )


def pool_is_active(pool_id: str) -> bool:
    pool = get_pool(pool_id)
    return bool(pool and pool.get("status") == "active")


def pool_stock(pool_id: str) -> dict:
    """Counts are scoped to this module's own rows (pool_source), so a
    stock report can never be inflated by an unrelated legacy row that
    happened to share a pool_id."""
    base = {"pool_id": pool_id, "pool_source": _POOL_SOURCE}
    available = database.db["voucher_pools"].count_documents({**base, "status": "available"})
    issued = database.db["voucher_pools"].count_documents({**base, "status": "issued"})
    return {"available": available, "issued": issued}


def upload_codes(pool_id: str, codes: list[str], *, display_label: str = "", value_hint: str = "", currency: str = "") -> dict:
    """Insert codes into the shared Voucher Centre inventory table
    (``db.voucher_pools``). Ownership metadata (``pool_type``,
    ``allocation_scope``, ``pool_source``) is always loaded from the
    canonical registry record and stamped server-side — this function
    accepts no caller-supplied ownership fields, so there is no parameter
    through which a caller could override them."""
    _reject_reserved_pool_id(pool_id)
    pool = get_pool(pool_id)
    if not pool:
        raise VoucherPoolError("pool_not_found")
    if pool.get("status") != "active":
        raise VoucherPoolError("pool_inactive")
    pool_type = pool.get("pool_type")
    allocation_scope = pool.get("allocation_scope")
    if pool_type not in POOL_TYPES:
        raise VoucherPoolError("invalid_pool_type")
    if allocation_scope not in ALLOCATION_SCOPES:
        raise VoucherPoolError("invalid_allocation_scope")

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
                "pool_type": pool_type,
                "allocation_scope": allocation_scope,
            })
            inserted += 1
        except Exception as exc:
            if "duplicate" in str(exc).lower():
                skipped += 1
            else:
                raise
    return {"inserted": inserted, "skipped_duplicates": skipped}


def allocate_voucher(
    pool_id: str,
    *,
    reward_id: str,
    telegram_user_id: int,
    expected_pool_type: str | None = None,
    now: datetime | None = None,
) -> dict | None:
    """Atomically claim one available code from the shared inventory for a
    reward. The filter's primary safety control is
    ``allocation_scope in ("campaign_rewards", "shared")`` — not pool_id
    naming and not pool_source alone (both are still present as
    defense-in-depth). When the reward rule specifies an expected
    ``pool_type`` it must match too."""
    now = now or datetime.now(timezone.utc)
    query = {
        "pool_id": pool_id,
        "status": "available",
        "pool_source": _POOL_SOURCE,
        "allocation_scope": {"$in": list(CAMPAIGN_ALLOCATABLE_SCOPES)},
        "$or": [
            {"issued_for_reward_id": {"$exists": False}},
            {"issued_for_reward_id": None},
        ],
    }
    if expected_pool_type:
        query["pool_type"] = expected_pool_type

    return database.db["voucher_pools"].find_one_and_update(
        query,
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
