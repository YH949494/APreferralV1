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

import os

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

# --- Qualified-referral thresholds -------------------------------------------
# THE single definition of what it takes to reach each tier. This lives in the
# lowest-level config module (no heavy imports) so every surface -- the
# evaluator, the admin pending-review calculation, the settings panel, the
# milestone announcement -- reads the same numbers.
#
# They were previously restated with independent os.getenv defaults in three
# modules and had already drifted: affiliate_rewards used T5=250 (correct)
# while vouchers.py and dashboard_panels.py both defaulted T5 to 300, so the
# admin "eligible tier" calculation and the settings panel disagreed with the
# evaluator for anyone on 250-299 qualified referrals.
# test_affiliate_threshold_drift.py fails if any copy reappears.
_THRESHOLD_ENV_DEFAULTS = (
    ("T1", "AFF_T1_THRESHOLD", 10),
    ("T2", "AFF_T2_THRESHOLD", 25),
    ("T3", "AFF_T3_THRESHOLD", 50),
    ("T4", "AFF_T4_THRESHOLD", 150),
    ("T5", "AFF_T5_THRESHOLD", 250),
)


def tier_thresholds(env=None) -> dict:
    """``{tier: qualified_referral_threshold}``.

    ``env`` defaults to the process environment; a mapping may be passed by
    callers that render configuration from a captured environment (e.g. the
    admin settings panel) so they report exactly what this process would use.
    """
    source = os.environ if env is None else env
    out = {}
    for tier, var, default in _THRESHOLD_ENV_DEFAULTS:
        raw = source.get(var)
        try:
            out[tier] = int(raw) if raw not in (None, "") else int(default)
        except (TypeError, ValueError):
            out[tier] = int(default)
    return out


def tier_threshold(tier, env=None) -> int:
    return int(tier_thresholds(env).get(normalize_tier(tier), 0))

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


# --- Operator-facing pool catalogue ------------------------------------------
# THE single source of truth for "which affiliate voucher pools may an
# operator load stock into", shared by every backend validator and mirrored
# by every admin UI control. Previously this list was restated in five
# places (two backends, three frontends) and had already drifted -- T5 and
# the denomination pools were missing from the admin dropdowns, so September
# inventory could not be uploaded at all. test_affiliate_pool_catalogue.py
# parses the static assets and fails if any copy drifts again.
#
# Order is the display order in every dropdown: legacy per-tier pools first
# (still required for August and any historical/back-dated entitlement),
# then the standardized denomination pools, then WELCOME.
ADMIN_AFFILIATE_POOL_IDS = TIERS + DENOMINATION_POOL_IDS + ("WELCOME",)

# Operator-facing labels. The VALUE submitted is always the bare pool_id, so
# a label can be reworded freely without changing what the backend receives.
POOL_DISPLAY_LABELS = {
    "T1": "T1 — Legacy tier pool (Aug 2026 and earlier)",
    "T2": "T2 — Legacy tier pool (Aug 2026 and earlier)",
    "T3": "T3 — Legacy tier pool (Aug 2026 and earlier)",
    "T4": "T4 — Legacy tier pool (Aug 2026 and earlier)",
    "T5": "T5 — Legacy tier pool (Aug 2026 and earlier)",
    POOL_AFFILIATE_5: "Affiliate $5 — shared denomination pool (Sep 2026 onward)",
    POOL_AFFILIATE_10: "Affiliate $10 — shared denomination pool (Sep 2026 onward)",
    POOL_AFFILIATE_50: "Affiliate $50 — shared denomination pool (Sep 2026 onward)",
    "WELCOME": "WELCOME — New User Voucher",
}

# Pools whose batch window MUST be the canonical KL calendar month, chosen
# with an entitlement-month picker rather than free-form start/end fields.
# A hand-typed window off by even a minute fails the full-containment check
# in _find_batches_for_period, so the batch reports zero claimable despite
# holding stock -- the exact defect scripts/fix_affiliate_batch_month_
# boundaries.py exists to repair. WELCOME has no entitlement-month concept
# and keeps free-form scheduling.
ENTITLEMENT_MONTH_POOL_IDS = TIERS + DENOMINATION_POOL_IDS


def pool_display_label(pool_id) -> str:
    key = str(pool_id or "").strip().upper()
    return POOL_DISPLAY_LABELS.get(key, key)


def is_admin_affiliate_pool(pool_id) -> bool:
    return str(pool_id or "").strip().upper() in ADMIN_AFFILIATE_POOL_IDS


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
