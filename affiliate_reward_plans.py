"""Canonical, versioned affiliate reward-plan configuration.

ONE source of truth for "what does tier X owe, for entitlement month M".
Issuance, retry, reconciliation, admin UI, announcements and reporting all
resolve through here so a reward definition is never duplicated (and never
drifts) across those paths.

Two plans exist:

``legacy_2026_08`` — every entitlement month through ``202608``
    The pre-existing single-denomination configuration, expressed here
    exactly as ``affiliate_rewards.AFFILIATE_REWARD_BUNDLES`` always did:
    each tier draws N codes of one value from its OWN per-tier pool
    (``pool_id == tier``). Reproduced verbatim so historical ledgers keep
    their original meaning:

        T1  $5 x 2  = $10      T2  $5 x 3  = $15
        T3  $10 x 5 = $50      T4  $50 x 3 = $150
        T5  $50 x 5 = $250

``denomination_2026_09`` — every entitlement month from ``202609`` onward
    Tier-specific inventory is replaced by three standardized denomination
    pools (``AFFILIATE_5`` / ``AFFILIATE_10`` / ``AFFILIATE_50``), and a
    tier's reward becomes a multi-denomination recipe drawn from them:

        T1  $10 x 1                       = $10
        T2  $5 x 1  + $10 x 2             = $25
        T3  $10 x 1 + $50 x 1             = $60
        T4  $10 x 3 + $50 x 3             = $180
        T5  $50 x 7                       = $350

    Each row is a SEPARATE milestone reward, not a cumulative total: a user
    who earns every tier in one KL month receives
    $10 + $25 + $60 + $180 + $350 = $625, drawn as 1x$5 + 7x$10 + 11x$50
    (19 physical codes).

Plan selection is ALWAYS keyed on a ledger's stored ``entitlement_month``,
never on the current processing date — so an August entitlement retried in
September (or later) still resolves the legacy bundle. See
``resolve_plan_id`` / ``tier_recipe``.
"""
from __future__ import annotations

# --- Denomination pools (new plan) -----------------------------------------
# Stable pool_id identifiers, consistent with the existing uppercase
# convention already used for "WELCOME"/"T1".."T5".
POOL_AFFILIATE_5 = "AFFILIATE_5"
POOL_AFFILIATE_10 = "AFFILIATE_10"
POOL_AFFILIATE_50 = "AFFILIATE_50"

DENOMINATION_POOL_IDS = (POOL_AFFILIATE_5, POOL_AFFILIATE_10, POOL_AFFILIATE_50)

# pool_id -> the fixed monetary value of every code in that pool.
DENOMINATION_POOL_VALUES = {
    POOL_AFFILIATE_5: 5,
    POOL_AFFILIATE_10: 10,
    POOL_AFFILIATE_50: 50,
}

TIERS = ("T1", "T2", "T3", "T4", "T5")

LEGACY_PLAN_ID = "legacy_2026_08"
DENOMINATION_PLAN_ID = "denomination_2026_09"

# First entitlement month (inclusive) on the denomination plan. Every month
# strictly before this stays on the legacy plan, forever.
DENOMINATION_PLAN_FIRST_MONTH = "202609"

# A recipe is an ordered tuple of (pool_id, quantity) components. Order is
# fixed and meaningful: it is the deterministic order codes are allocated
# and delivered in.
_LEGACY_RECIPES: dict[str, tuple[tuple[str, int], ...]] = {
    "T1": (("T1", 2),),
    "T2": (("T2", 3),),
    "T3": (("T3", 5),),
    "T4": (("T4", 3),),
    "T5": (("T5", 5),),
}

# Legacy per-tier pools hold a single denomination each; the value is a
# property of the TIER, not the pool, and is reproduced from the original
# AFFILIATE_REWARD_BUNDLES table.
_LEGACY_TIER_VALUES = {"T1": 5, "T2": 5, "T3": 10, "T4": 50, "T5": 50}

_DENOMINATION_RECIPES: dict[str, tuple[tuple[str, int], ...]] = {
    "T1": ((POOL_AFFILIATE_10, 1),),
    "T2": ((POOL_AFFILIATE_5, 1), (POOL_AFFILIATE_10, 2)),
    "T3": ((POOL_AFFILIATE_10, 1), (POOL_AFFILIATE_50, 1)),
    "T4": ((POOL_AFFILIATE_10, 3), (POOL_AFFILIATE_50, 3)),
    "T5": ((POOL_AFFILIATE_50, 7),),
}


def normalize_tier(tier) -> str:
    return str(tier or "").strip().upper()


def normalize_month(entitlement_month) -> str | None:
    """A well-formed ``YYYYMM`` string, or ``None`` when unusable."""
    value = str(entitlement_month or "").strip()
    if len(value) != 6 or not value.isdigit():
        return None
    if not (1 <= int(value[4:6]) <= 12):
        return None
    return value


def resolve_plan_id(entitlement_month) -> str:
    """Which reward plan governs ``entitlement_month``.

    A missing/malformed month resolves to the LEGACY plan deliberately:
    every ledger that predates the new-plan fields has no
    ``entitlement_month``, and must keep its original obligation. A new
    ledger always carries a valid month (written at creation), so this
    fallback can never quietly downgrade a September+ entitlement.
    """
    month = normalize_month(entitlement_month)
    if month is None:
        return LEGACY_PLAN_ID
    return DENOMINATION_PLAN_ID if month >= DENOMINATION_PLAN_FIRST_MONTH else LEGACY_PLAN_ID


def is_denomination_plan(plan_id) -> bool:
    return str(plan_id or "").strip() == DENOMINATION_PLAN_ID


def pool_denomination(pool_id) -> int | None:
    """The fixed value of a denomination pool's codes, or ``None`` for a
    per-tier legacy pool (whose value is a property of the tier)."""
    return DENOMINATION_POOL_VALUES.get(str(pool_id or "").strip().upper())


def recipe_components(plan_id, tier) -> tuple[tuple[str, int], ...] | None:
    """The ``((pool_id, qty), ...)`` components for a tier under a plan."""
    tier_key = normalize_tier(tier)
    table = _DENOMINATION_RECIPES if is_denomination_plan(plan_id) else _LEGACY_RECIPES
    return table.get(tier_key)


def component_value(plan_id, tier, pool_id) -> int:
    """Monetary value of ONE code drawn from ``pool_id`` for this tier."""
    if is_denomination_plan(plan_id):
        return int(DENOMINATION_POOL_VALUES.get(str(pool_id or "").strip().upper()) or 0)
    return int(_LEGACY_TIER_VALUES.get(normalize_tier(tier)) or 0)


def tier_recipe(entitlement_month, tier) -> dict | None:
    """The full frozen obligation for one (entitlement_month, tier).

    Returns ``None`` for an unknown tier. The returned dict is the
    "frozen bundle recipe" persisted onto a ledger at creation, so the
    original obligation stays reproducible even if this table later
    changes:

        {
          "reward_plan": "denomination_2026_09",
          "entitlement_month": "202609",
          "tier": "T3",
          "components": [{"pool_id": "AFFILIATE_10", "quantity": 1, "value": 10},
                         {"pool_id": "AFFILIATE_50", "quantity": 1, "value": 50}],
          "expected_code_count": 2,
          "reward_value": 60,
        }
    """
    tier_key = normalize_tier(tier)
    plan_id = resolve_plan_id(entitlement_month)
    components = recipe_components(plan_id, tier_key)
    if not components:
        return None
    resolved = [
        {
            "pool_id": pool_id,
            "quantity": int(quantity),
            "value": component_value(plan_id, tier_key, pool_id),
        }
        for pool_id, quantity in components
    ]
    return {
        "reward_plan": plan_id,
        "entitlement_month": normalize_month(entitlement_month),
        "tier": tier_key,
        "components": resolved,
        "expected_code_count": sum(int(c["quantity"]) for c in resolved),
        "reward_value": sum(int(c["quantity"]) * int(c["value"]) for c in resolved),
    }


def recipe_pool_ids(recipe: dict | None) -> list[str]:
    return [str(c.get("pool_id")) for c in (recipe or {}).get("components") or []]


def recipe_required_by_pool(recipe: dict | None) -> dict[str, int]:
    """``{pool_id: required_quantity}`` for a resolved/frozen recipe."""
    required: dict[str, int] = {}
    for component in (recipe or {}).get("components") or []:
        pool_id = str(component.get("pool_id") or "").strip().upper()
        if not pool_id:
            continue
        required[pool_id] = required.get(pool_id, 0) + int(component.get("quantity") or 0)
    return required


def recipe_value_by_pool(recipe: dict | None) -> dict[str, int]:
    """``{pool_id: per_code_value}`` for a resolved/frozen recipe."""
    values: dict[str, int] = {}
    for component in (recipe or {}).get("components") or []:
        pool_id = str(component.get("pool_id") or "").strip().upper()
        if not pool_id:
            continue
        values[pool_id] = int(component.get("value") or 0)
    return values


def reward_value(entitlement_month, tier) -> int:
    recipe = tier_recipe(entitlement_month, tier)
    return int((recipe or {}).get("reward_value") or 0)


def expected_code_count(entitlement_month, tier) -> int:
    recipe = tier_recipe(entitlement_month, tier)
    return int((recipe or {}).get("expected_code_count") or 0)


def all_pool_ids_for_plan(plan_id) -> tuple[str, ...]:
    """Every pool a plan can draw from — the inventory an operator must
    stock for that plan."""
    return DENOMINATION_POOL_IDS if is_denomination_plan(plan_id) else TIERS
