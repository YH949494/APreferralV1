import logging
import os
from datetime import datetime, timedelta, timezone

import pytz
import requests
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure
from telegram_utils import send_telegram_http_message
from database import _ensure_equivalent_index
from config import (
    AFFILIATE_GROUP_INVITE_TEXT,
    AFFILIATE_GROUP_INVITE_URL,
    AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS,
)
from referral_ledger import with_not_invalidated
from affiliate_reward_plans import (
    DENOMINATION_POOL_IDS,
    is_denomination_plan,
    normalize_month as _normalize_entitlement_month,
    recipe_required_by_pool,
    recipe_value_by_pool,
    resolve_plan_id,
    tier_recipe,
)

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

TIERS = ("T1", "T2", "T3", "T4", "T5")
POOL_IDS = ("WELCOME",) + TIERS + DENOMINATION_POOL_IDS
FINAL_STATUSES = {"ISSUED", "OUT_OF_STOCK", "REJECTED"}
SETTLING_STATUS = "SETTLING"
AFFILIATE_BUNDLE_REWARD_TYPE = "affiliate_bundle"
# Legacy (entitlement month <= 202608) single-denomination bundles.
# RETAINED AS-IS for historical meaning and for every legacy-plan code path
# below. The canonical, plan-versioned definition now lives in
# affiliate_reward_plans (which reproduces this exact table as its legacy
# plan) — resolve rewards through `_ledger_recipe` / `tier_recipe`, not this
# dict, so a September+ entitlement never reads an August obligation.
AFFILIATE_REWARD_BUNDLES = {
    "T1": {"voucher_count": 2, "voucher_value": 5},
    "T2": {"voucher_count": 3, "voucher_value": 5},
    "T3": {"voucher_count": 5, "voucher_value": 10},
    "T4": {"voucher_count": 3, "voucher_value": 50},
    "T5": {"voucher_count": 5, "voucher_value": 50},
}
AFFILIATE_TIER_ICONS = {"T1": "🎉", "T2": "⭐", "T3": "🔥", "T4": "💎", "T5": "👑"}

# risk_flags values that only ever describe an inventory/config gap for a
# monthly ledger (never an abuse/risk-review signal) — see every
# "$addToSet": {"risk_flags": ...} call site in this module. Only these are
# safe grounds for the auto-recovery re-resolution path below; anything else
# (blocked_user, ip_cluster, subnet_cluster, deny_count_7d,
# risk_flags_calc_failed, ...) must keep routing to manual review.
_INVENTORY_ONLY_RISK_FLAGS = frozenset({
    "pool_empty",
    "missing_pool_config",
    "no_batch_for_entitlement_period",
    "target_batch_not_ready",
    "target_batch_disabled",
    "target_batch_expired_unissued",
    "target_batch_scheduled",
    "target_batch_empty",
    # Denomination-plan shortage: at least one denomination of the tier's
    # recipe could not be filled. Inventory-only by construction (raised
    # solely by the allocation loop), so the existing auto-retry path may
    # resume it once stock is replenished.
    "bundle_denomination_short",
})

WELCOME_REWARD_VISIBLE_DAYS = 3

T1_THRESHOLD = int(os.getenv("AFF_T1_THRESHOLD", "10"))
T2_THRESHOLD = int(os.getenv("AFF_T2_THRESHOLD", "25"))
T3_THRESHOLD = int(os.getenv("AFF_T3_THRESHOLD", "50"))
T4_THRESHOLD = int(os.getenv("AFF_T4_THRESHOLD", "150"))
# 250, matching the confirmed business rule and scheduler.py's own
# REFERRAL_CONGRATS_TIER_LABEL ({..., 250: "T5"}). The previous default of
# 300 disagreed with that table, so a user on 250-299 qualified referrals
# had a T5 milestone announcement configured for a tier the evaluator
# would never grant.
T5_THRESHOLD = int(os.getenv("AFF_T5_THRESHOLD", "250"))
logger = logging.getLogger(__name__)
logger.info(
    "[AFFILIATE][TIER_CONFIG] thresholds=%s",
    {"T1": T1_THRESHOLD, "T2": T2_THRESHOLD, "T3": T3_THRESHOLD, "T4": T4_THRESHOLD, "T5": T5_THRESHOLD},
)


def _is_official_channel_subscribed(user_id: int) -> bool:
    channel_id_raw = os.getenv("OFFICIAL_CHANNEL_ID")
    token = os.getenv("BOT_TOKEN", "")
    if not channel_id_raw or not token:
        return False
    try:
        channel_id = int(str(channel_id_raw).strip())
    except (TypeError, ValueError):
        return False

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": channel_id, "user_id": int(user_id)},
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json() or {}
        if not payload.get("ok"):
            return False
        status = ((payload.get("result") or {}).get("status") or "").strip()
        return status in ("member", "administrator", "creator")
    except Exception:
        return False

def ensure_affiliate_indexes(db):
    db.qualified_events.create_index([("invitee_id", ASCENDING)], unique=True, name="uniq_invitee_id")
    db.qualified_events.create_index([("referrer_id", ASCENDING), ("qualified_at", ASCENDING)], name="qualified_by_referrer_time")

    _ensure_equivalent_index(
        db.voucher_pools,
        [("pool_id", ASCENDING), ("code", ASCENDING)],
        unique=True,
        name="uniq_pool_code",
    )
    db.voucher_pools.create_index([("pool_id", ASCENDING), ("status", ASCENDING)], name="pool_status")
    db.voucher_pools.create_index(
        [("pool_id", ASCENDING), ("status", ASCENDING), ("starts_at", ASCENDING), ("ends_at", ASCENDING)],
        name="pool_status_window",
    )
    db.voucher_pools.create_index([("batch_id", ASCENDING), ("status", ASCENDING)], name="pool_batch_status")
    # Reservation/reconciliation of a multi-denomination bundle reads
    # "every issued row linked to THIS ledger", grouped by pool. Without
    # these, that read is a collection scan on every allocation, retry and
    # admin render.
    db.voucher_pools.create_index(
        [("issued_for_ledger_id", ASCENDING), ("status", ASCENDING), ("pool_id", ASCENDING)],
        name="pool_issued_ledger_status_pool",
    )
    db.voucher_pools.create_index(
        [("ledger_id", ASCENDING), ("status", ASCENDING)],
        name="pool_ledger_status",
    )

    db.affiliate_ledger.create_index([("dedup_key", ASCENDING)], unique=True, name="uniq_affiliate_dedup")
    db.affiliate_ledger.create_index([("status", ASCENDING), ("created_at", ASCENDING)], name="affiliate_status_created")
    db.affiliate_ledger.create_index([("user_id", ASCENDING), ("year_month", ASCENDING)], name="affiliate_user_month")
    # Reporting and reconciliation both slice by entitlement month + plan +
    # tier. Non-unique and additive: historical ledgers simply have no
    # entitlement_month/reward_plan and are absent from it.
    db.affiliate_ledger.create_index(
        [("entitlement_month", ASCENDING), ("reward_plan", ASCENDING), ("tier", ASCENDING), ("status", ASCENDING)],
        name="affiliate_entitlement_plan_tier_status",
    )
    db.affiliate_ledger.create_index(
        [("user_id", ASCENDING), ("invitee_user_id", ASCENDING), ("gate_day", ASCENDING), ("tier", ASCENDING), ("created_at", ASCENDING)],
        unique=True,
        name="uniq_affiliate_simulated_natural",
        partialFilterExpression={"simulate": True, "ledger_type": "AFFILIATE_SIMULATION"},
    )
    try:
        db.affiliate_ledger.create_index(
            [("user_id", ASCENDING), ("year_month", ASCENDING), ("tier", ASCENDING)],
            unique=True,
            name="uniq_affiliate_monthly_user_month_tier",
            partialFilterExpression={"ledger_type": "AFFILIATE_MONTHLY"},
        )
    except DuplicateKeyError:
        logger.warning(
            "[AFFILIATE][INDEX_WARN] name=uniq_affiliate_monthly_user_month_tier reason=duplicate_existing_data"
        )
    except OperationFailure as exc:
        if int(getattr(exc, "code", 0) or 0) == 11000:
            logger.warning(
                "[AFFILIATE][INDEX_WARN] name=uniq_affiliate_monthly_user_month_tier reason=duplicate_existing_data"
            )
        else:
            raise

    db.user_last_seen.create_index([("user_id", ASCENDING)], unique=True, name="uniq_user_last_seen")
    db.affiliate_group_invites.create_index(
        [("user_id", ASCENDING), ("week_key", ASCENDING)],
        unique=True,
        name="uniq_affiliate_group_invite_user_week",
    )


def _month_window_utc(reference_utc: datetime | None = None):
    now_utc = reference_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    ref_kl = now_utc.astimezone(KL_TZ)
    start_kl = ref_kl.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start_kl.month == 12:
        end_kl = start_kl.replace(year=start_kl.year + 1, month=1)
    else:
        end_kl = start_kl.replace(month=start_kl.month + 1)
    return start_kl.astimezone(timezone.utc), end_kl.astimezone(timezone.utc), start_kl.strftime("%Y%m")


def _week_window_utc(reference_utc: datetime | None = None):
    now_utc = reference_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    ref_kl = now_utc.astimezone(KL_TZ)
    start_kl = (ref_kl - timedelta(days=ref_kl.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end_kl = start_kl + timedelta(days=7)
    return start_kl.astimezone(timezone.utc), end_kl.astimezone(timezone.utc), start_kl.date().isoformat()


def _previous_completed_week_window_utc(reference_utc: datetime | None = None):
    current_start_utc, _, _ = _week_window_utc(reference_utc)
    prev_ref = current_start_utc - timedelta(seconds=1)
    return _week_window_utc(prev_ref)


def _tier_for_count(count: int) -> str | None:
    if count >= T5_THRESHOLD:
        return "T5"
    if count >= T4_THRESHOLD:
        return "T4"
    if count >= T3_THRESHOLD:
        return "T3"
    if count >= T2_THRESHOLD:
        return "T2"
    if count >= T1_THRESHOLD:
        return "T1"
    return None


def _tier_rank(tier: str | None) -> int:
    ranks = {"T1": 1, "T2": 2, "T3": 3, "T4": 4, "T5": 5}
    key = (tier or "").strip().upper()  # Normalize legacy/case-variant tier values before ranking.
    return ranks.get(key, 0)


def _affiliate_bundle_spec(tier: str | None) -> dict | None:
    return AFFILIATE_REWARD_BUNDLES.get(str(tier or "").strip().upper())


def _voucher_row_value(row: dict, *, tier: str, value_by_pool: dict) -> int:
    """Monetary value of ONE physical voucher row.

    Resolution order, most authoritative first:
      1. ``voucher_value`` stamped on the row itself at upload time — the
         only correct source for a denomination pool, where one pool serves
         many tiers.
      2. The recipe's per-pool value for that row's ``pool_id``.
      3. The legacy tier-wide value (per-tier pools carry no row value).
    """
    stamped = (row or {}).get("voucher_value")
    if stamped is not None:
        try:
            return int(stamped)
        except (TypeError, ValueError):
            pass
    pool_id = str((row or {}).get("pool_id") or "").strip().upper()
    if pool_id in value_by_pool:
        return int(value_by_pool[pool_id] or 0)
    spec = _affiliate_bundle_spec(tier) or {}
    return int(spec.get("voucher_value") or 0)


def _affiliate_bundle_payload(*, tier: str, vouchers: list[dict], recipe: dict | None = None) -> dict:
    """Bundle payload stored on the ledger and rendered to the user.

    Without ``recipe`` this is byte-for-byte the previous legacy behaviour
    (one tier-wide value applied to every code). With a recipe it prices
    each code from its own denomination, so a mixed bundle such as
    T3 = $10 + $50 totals correctly.
    """
    tier_key = str(tier or "").strip().upper()
    value_by_pool = recipe_value_by_pool(recipe) if recipe else {}
    normalized = []
    for voucher in vouchers:
        code = str((voucher or {}).get("code") or "").strip()
        if not code:
            continue
        entry = {
            "value": _voucher_row_value(voucher, tier=tier_key, value_by_pool=value_by_pool),
            "code": code,
        }
        pool_id = str((voucher or {}).get("pool_id") or "").strip().upper()
        if pool_id:
            entry["pool_id"] = pool_id
        normalized.append(entry)
    return {
        "reward_type": AFFILIATE_BUNDLE_REWARD_TYPE,
        "affiliate_tier": tier_key,
        "voucher_count": len(normalized),
        "total_value": sum(int(v.get("value") or 0) for v in normalized),
        "currency": "$",
        "vouchers": normalized,
    }


def _ledger_entitlement_month(ledger: dict | None) -> str | None:
    """The entitlement month that governs this ledger's obligation.

    ``entitlement_month`` is written on every ledger created from this
    change onward. Historical ledgers only have ``year_month`` (the same
    KL "YYYYMM" value, written by the evaluator), so it is read as a lazy
    backward-compatible fallback — no backfill is required for a historical
    ledger to keep resolving its original plan.
    """
    return _normalize_entitlement_month(
        (ledger or {}).get("entitlement_month") or (ledger or {}).get("year_month")
    )


def _ledger_reward_plan(ledger: dict | None) -> str:
    """The plan governing this ledger — the frozen ``reward_plan`` if one
    was persisted, otherwise resolved from the entitlement month.

    NEVER resolved from the current date: an August entitlement retried in
    September (or any later month) must still resolve the legacy plan.
    """
    stored = str((ledger or {}).get("reward_plan") or "").strip()
    if stored:
        return stored
    return resolve_plan_id(_ledger_entitlement_month(ledger))


def _ledger_recipe(ledger: dict | None) -> dict | None:
    """The tier recipe this ledger owes.

    A ledger created under this change carries an immutable frozen
    ``bundle_recipe``, which always wins — so a later edit to the plan
    tables can never retroactively change what an existing entitlement
    owes. Anything older is resolved lazily from
    (entitlement_month, tier), which for every historical ledger yields
    exactly the legacy bundle it was created under.
    """
    frozen = (ledger or {}).get("bundle_recipe")
    if isinstance(frozen, dict) and frozen.get("components"):
        return frozen
    return tier_recipe(_ledger_entitlement_month(ledger), (ledger or {}).get("tier"))


def _ledger_uses_denomination_plan(ledger: dict | None) -> bool:
    return is_denomination_plan(_ledger_reward_plan(ledger))


def _ledger_has_affiliate_bundle(ledger: dict | None) -> bool:
    if not ledger:
        return False
    if ledger.get("reward_type") != AFFILIATE_BUNDLE_REWARD_TYPE:
        return False
    return bool(ledger.get("vouchers"))


def _affiliate_bundle_codes(ledger: dict | None) -> list[str]:
    codes = []
    for item in (ledger or {}).get("vouchers") or []:
        code = str((item or {}).get("code") or "").strip()
        if code:
            codes.append(code)
    return codes


def _affiliate_bundle_matches_pool_rows(ledger: dict | None, pool_rows: list[dict]) -> bool:
    bundle_codes = set(_affiliate_bundle_codes(ledger))
    pool_codes = {str((row or {}).get("code") or "").strip() for row in pool_rows or [] if (row or {}).get("code")}
    return bool(bundle_codes) and bundle_codes == pool_codes


def _pool_exists(db, pool_id: str) -> bool:
    return (
        db.voucher_pools.find_one(
            {"pool_id": str(pool_id), "status": "available"},
            {"_id": 1},
        )
        is not None
    )


def _mark_missing_pool_config(db, *, ledger_id, now_utc: datetime):
    db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": {"$in": [SETTLING_STATUS, "APPROVED", "PENDING_REVIEW", "PENDING_MANUAL"]}, **_no_voucher_filter()},
        {
            "$set": {"status": "PENDING_MANUAL", "updated_at": now_utc},
            "$addToSet": {"risk_flags": "missing_pool_config"},
        },
    )


def _mask_voucher_code(code) -> str:
    code = str(code or "")
    if len(code) <= 4:
        return "*" * len(code)
    return code[:2] + "*" * (len(code) - 4) + code[-2:]


def _find_active_batch(db, *, pool_id: str, now_utc: datetime) -> dict | None:
    """The one batch whose schedule window currently covers ``now_utc`` for
    this tier — any ``upload_status``/``distribution_disabled`` state; the
    caller decides what to do with that. Batches never overlap for the same
    ``pool_id`` (enforced at creation), so at most one can match.
    """
    for row in db.affiliate_voucher_batches.find({"pool_id": pool_id}):
        starts_at = _as_aware_utc(row.get("starts_at"))
        ends_at = _as_aware_utc(row.get("ends_at"))
        if starts_at is None or ends_at is None:
            continue
        if starts_at <= now_utc < ends_at:
            return row
    return None


def _tier_entered_scheduled_mode(db, *, pool_id: str, reference_utc: datetime) -> bool:
    """True once the earliest batch ever created for this tier (regardless
    of its current status — active, exhausted, disabled, or expired) had
    already started as of ``reference_utc``. This is a permanent, one-way
    cutover for legacy (undated) voucher fallback: it is evaluated against
    the *earliest* start, so uploading next month's batch early does not
    retroactively block this month's legacy stock, and it never flips back
    once tripped, even after that batch later exhausts/expires/is disabled.
    """
    starts = [
        _as_aware_utc(row.get("starts_at"))
        for row in db.affiliate_voucher_batches.find({"pool_id": pool_id})
    ]
    starts = [s for s in starts if s is not None]
    if not starts:
        return False
    return min(starts) <= reference_utc


def _batch_claimable_available_count(db, batch: dict) -> int:
    if batch.get("upload_status") not in (None, "ready"):
        return 0
    if bool(batch.get("distribution_disabled")):
        return 0
    return int(db.voucher_pools.count_documents({"batch_id": batch.get("_id"), "status": "available"}))


def _claim_from_target_batch(db, *, batch_id, pool_id: str, ledger_id, user_id: int, now_utc: datetime):
    """Two-step authoritative claim: the batch document — not any
    denormalized field on the voucher row — is the source of truth for
    ``upload_status``/``distribution_disabled``/schedule window. It is
    re-fetched fresh here and checked *before* ever looking at
    ``voucher_pools``, so a stale/incorrect row-level flag can never grant
    a claim the batch document itself would refuse. The final claim is
    still a single atomic ``find_one_and_update`` keyed on ``batch_id`` +
    ``status``, so two workers can never win the same code.
    Returns ``(voucher_or_None, reason_or_None)``.
    """
    batch = db.affiliate_voucher_batches.find_one({"_id": batch_id})
    if not batch:
        return None, "no_batch_for_entitlement_period"
    if batch.get("upload_status") not in (None, "ready"):
        return None, "target_batch_not_ready"
    if bool(batch.get("distribution_disabled")):
        return None, "target_batch_disabled"
    starts_at = _as_aware_utc(batch.get("starts_at"))
    ends_at = _as_aware_utc(batch.get("ends_at"))
    if starts_at is None or ends_at is None:
        return None, "target_batch_not_ready"
    if now_utc >= ends_at:
        return None, "target_batch_expired_unissued"
    if now_utc < starts_at:
        return None, "target_batch_scheduled"

    voucher = db.voucher_pools.find_one_and_update(
        {
            "batch_id": batch_id,
            "pool_id": pool_id,
            "status": "available",
            "$or": [
                {"issued_for_ledger_id": {"$exists": False}},
                {"issued_for_ledger_id": None},
            ],
        },
        {
            "$set": {
                "status": "issued",
                "issued_to": user_id,
                "issued_to_user_id": user_id,
                "issued_at": now_utc,
                "ledger_id": ledger_id,
                "issued_for_ledger_id": str(ledger_id),
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )
    if voucher:
        logger.info(
            "[AFF_VOUCHER][CLAIM_SELECTED] pool_id=%s ledger_id=%s user_id=%s batch_id=%s code=%s",
            pool_id, ledger_id, user_id, batch_id, _mask_voucher_code(voucher.get("code")),
        )
        return voucher, None
    return None, "target_batch_empty"


def _claim_legacy_voucher(db, *, pool_id: str, ledger_id, user_id: int, now_utc: datetime):
    """Claim from legacy_unbounded rows only (no ``batch_id``) — the
    pre-existing always-claimable behaviour, untouched. Never considers any
    dated batch, even one that happens to be currently active; callers
    decide when legacy is policy-eligible before calling this.
    """
    # Minimal cross-consumption guard (Campaign Centre voucher_pool_service
    # writes an explicit "allocation_scope" onto every row it inserts): a
    # row is claimable here only if it has no allocation_scope at all
    # (every pre-existing legacy affiliate row — untouched, still works
    # exactly as before) or is explicitly "affiliate_rewards"/"shared".
    # Rows scoped "campaign_rewards"/"welcome_rewards"/etc. are never
    # matched, even if a pool_id were ever accidentally shared.
    candidates = list(
        db.voucher_pools.find(
            {
                "pool_id": pool_id,
                "status": "available",
                "batch_id": {"$exists": False},
                "distribution_disabled": {"$ne": True},
                "$or": [
                    {"issued_for_ledger_id": {"$exists": False}},
                    {"issued_for_ledger_id": None},
                ],
                "allocation_scope": {"$nin": ["campaign_rewards", "welcome_rewards", "voucher_drops", "referral_rewards"]},
            }
        )
    )
    candidates.sort(key=lambda row: row.get("_id"))
    for candidate in candidates:
        voucher = db.voucher_pools.find_one_and_update(
            {"_id": candidate["_id"], "status": "available", "distribution_disabled": {"$ne": True}},
            {
                "$set": {
                    "status": "issued",
                    "issued_to": user_id,
                    "issued_to_user_id": user_id,
                    "issued_at": now_utc,
                    "ledger_id": ledger_id,
                    "issued_for_ledger_id": str(ledger_id),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if voucher:
            logger.info(
                "[AFF_VOUCHER][LEGACY_FALLBACK_USED] pool_id=%s ledger_id=%s user_id=%s code=%s",
                pool_id, ledger_id, user_id, _mask_voucher_code(voucher.get("code")),
            )
            return voucher
    return None


def _claim_voucher_from_pool(db, *, pool_id: str, ledger_id, user_id: int, now_utc: datetime, legacy_only: bool = False):
    """Policy (legacy-fallback control): once a tier is on scheduled T1-T4
    batches, an old undated legacy code must never quietly substitute for a
    missing/exhausted/disabled scheduled batch.
      - ``legacy_only=True`` (a ledger permanently pinned to the legacy
        pool): claim only legacy_unbounded rows, regardless of any batch's
        current state.
      - Otherwise: resolve the batch currently active for this tier.
          * If one exists, claim ONLY from it — an active-but-empty,
            disabled, or still-uploading batch never falls through to
            legacy; that's a pool-empty/manual-review outcome.
          * If none exists (no batch's window currently covers ``now_utc``),
            allow the transitional legacy pool unless this tier has already
            permanently entered scheduled-batch mode.
    """
    if not legacy_only:
        active_batch = _find_active_batch(db, pool_id=pool_id, now_utc=now_utc)
        if active_batch is not None:
            voucher, reason = _claim_from_target_batch(
                db, batch_id=active_batch["_id"], pool_id=pool_id, ledger_id=ledger_id, user_id=user_id, now_utc=now_utc,
            )
            if voucher:
                return voucher
            logger.warning(
                "[AFF_VOUCHER][ACTIVE_BATCH_EMPTY] pool_id=%s ledger_id=%s user_id=%s batch_id=%s policy_reason=%s",
                pool_id, ledger_id, user_id, active_batch["_id"], reason,
            )
            return None
        if _tier_entered_scheduled_mode(db, pool_id=pool_id, reference_utc=now_utc):
            logger.warning(
                "[AFF_VOUCHER][LEGACY_FALLBACK_BLOCKED] pool_id=%s ledger_id=%s user_id=%s policy_reason=scheduled_mode_entered",
                pool_id, ledger_id, user_id,
            )
            return None

    return _claim_legacy_voucher(db, pool_id=pool_id, ledger_id=ledger_id, user_id=user_id, now_utc=now_utc)


def _log_pool_claim_miss(db, *, pool_id: str, ledger_id, user_id: int, now_utc: datetime, legacy_only: bool = False):
    """Mirrors ``_claim_voucher_from_pool``'s policy branches purely for
    logging when an upfront inventory count already shows the claim can't
    be fulfilled — never mutates anything.
    """
    if not legacy_only:
        active_batch = _find_active_batch(db, pool_id=pool_id, now_utc=now_utc)
        if active_batch is not None:
            logger.warning(
                "[AFF_VOUCHER][ACTIVE_BATCH_EMPTY] pool_id=%s ledger_id=%s user_id=%s batch_id=%s",
                pool_id, ledger_id, user_id, active_batch["_id"],
            )
            return
        if _tier_entered_scheduled_mode(db, pool_id=pool_id, reference_utc=now_utc):
            logger.warning(
                "[AFF_VOUCHER][LEGACY_FALLBACK_BLOCKED] pool_id=%s ledger_id=%s user_id=%s policy_reason=scheduled_mode_entered",
                pool_id, ledger_id, user_id,
            )
            return
    logger.warning(
        "[AFF_VOUCHER][OUT_OF_STOCK] pool_id=%s ledger_id=%s user_id=%s reason=legacy_pool_empty",
        pool_id, ledger_id, user_id,
    )


def _pool_ledger_filter(ledger_id):
    return {
        "$or": [
            {"issued_for_ledger_id": str(ledger_id)},
            {"ledger_id": ledger_id},
        ],
    }


def _issued_pool_vouchers_for_ledger(db, *, ledger_id) -> list[dict]:
    rows = list(
        db.voucher_pools.find(
            {
                "status": "issued",
                **_pool_ledger_filter(ledger_id),
            }
        )
    )
    rows.sort(key=lambda row: row.get("_id", 0))
    return rows


def _rollback_pool_vouchers(db, *, vouchers: list[dict], ledger_id, reason: str) -> int:
    rolled_back = 0
    for voucher in vouchers or []:
        code = str((voucher or {}).get("code") or "").strip()
        if not code:
            continue
        res = db.voucher_pools.update_one(
            {
                "pool_id": voucher.get("pool_id"),
                "code": code,
                "status": "issued",
                **_pool_ledger_filter(ledger_id),
            },
            {
                "$set": {
                    "status": "available",
                    "rollback_reason": reason,
                    "rolled_back_at": datetime.now(timezone.utc),
                },
                "$unset": {
                    "issued_to": "",
                    "issued_to_user_id": "",
                    "issued_at": "",
                    "ledger_id": "",
                    "issued_for_ledger_id": "",
                },
            },
        )
        if getattr(res, "modified_count", 0) == 1:
            rolled_back += 1
    return rolled_back


def _guarded_rollback_attempt_vouchers(db, *, vouchers: list[dict], ledger_id, reason: str, now_utc: datetime):
    latest = db.affiliate_ledger.find_one({"_id": ledger_id})
    latest = _finalize_issued_if_voucher_exists(db, ledger=latest, now_utc=now_utc)
    if latest and latest.get("status") == "ISSUED" and _ledger_has_affiliate_bundle(latest):
        return latest

    issued_rows = _issued_pool_vouchers_for_ledger(db, ledger_id=ledger_id)
    if _affiliate_bundle_matches_pool_rows(latest, issued_rows):
        return latest

    if latest and latest.get("status") != SETTLING_STATUS:
        return latest

    _rollback_pool_vouchers(db, vouchers=vouchers, ledger_id=ledger_id, reason=reason)
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _available_pool_count(db, *, pool_id: str, now_utc: datetime | None = None, legacy_only: bool = False) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    if not legacy_only:
        active_batch = _find_active_batch(db, pool_id=pool_id, now_utc=now_utc)
        if active_batch is not None:
            return _batch_claimable_available_count(db, active_batch)
        if _tier_entered_scheduled_mode(db, pool_id=pool_id, reference_utc=now_utc):
            return 0
    return int(
        db.voucher_pools.count_documents(
            {
                "pool_id": pool_id,
                "status": "available",
                "batch_id": {"$exists": False},
                "distribution_disabled": {"$ne": True},
                "$or": [
                    {"issued_for_ledger_id": {"$exists": False}},
                    {"issued_for_ledger_id": None},
                ],
            }
        )
    )


def _pool_inventory_blocking_reason(db, *, pool_id: str, now_utc: datetime, legacy_only: bool = False) -> str | None:
    """Why ``_available_pool_count`` came back at (or below) zero for this
    tier right now — mirrors ``_claim_voucher_from_pool``'s own branches so
    the reason always matches what a live claim attempt would hit.
    """
    if not legacy_only:
        active_batch = _find_active_batch(db, pool_id=pool_id, now_utc=now_utc)
        if active_batch is not None:
            if active_batch.get("upload_status") not in (None, "ready"):
                return "target_batch_not_ready"
            if bool(active_batch.get("distribution_disabled")):
                return "target_batch_disabled"
            if _batch_claimable_available_count(db, active_batch) <= 0:
                return "target_batch_empty"
            return None
        if _tier_entered_scheduled_mode(db, pool_id=pool_id, reference_utc=now_utc):
            return "no_batch_for_entitlement_period"
    return "pool_empty"


def _monthly_entitlement_claimable_count_and_reason(db, *, pool_id: str, now_utc: datetime) -> tuple[int, str | None]:
    """T1-T5 claimability as an ``AFFILIATE_MONTHLY`` ledger created right
    now would actually resolve it — mirrors ``_resolve_monthly_ledger_target``
    exactly: a batch only counts if its window *fully contains* the KL
    calendar month containing ``now_utc`` (``_find_batches_for_period``),
    not merely "covers this instant" (``_find_active_batch``). Without this,
    a batch whose window only partially overlaps the month (e.g. starts
    mid-month) would look fully claimable here while real monthly issuance
    — which requires full-month containment — falls back to legacy or
    reports ``no_batch_for_entitlement_period`` for that exact same ledger.
    """
    local_now = now_utc.astimezone(KL_TZ)
    period_start_utc, period_end_utc = _month_window_from_yyyymm(f"{local_now.year:04d}{local_now.month:02d}")
    if period_start_utc is None or period_end_utc is None:
        return 0, "no_batch_for_entitlement_period"

    matches = _find_batches_for_period(db, pool_id=pool_id, period_start_utc=period_start_utc, period_end_utc=period_end_utc)
    if len(matches) > 1:
        return 0, "target_batch_ambiguous"
    if matches:
        batch = matches[0]
        if batch.get("upload_status") not in (None, "ready"):
            return 0, "target_batch_not_ready"
        if bool(batch.get("distribution_disabled")):
            return 0, "target_batch_disabled"
        count = _batch_claimable_available_count(db, batch)
        return count, (None if count > 0 else "target_batch_empty")
    if _tier_entered_scheduled_mode(db, pool_id=pool_id, reference_utc=period_start_utc):
        return 0, "no_batch_for_entitlement_period"
    count = int(_available_pool_count(db, pool_id=pool_id, now_utc=now_utc, legacy_only=True))
    return count, (None if count > 0 else "pool_empty")


def get_claimable_pool_inventory(db, *, pool_id: str, now_utc: datetime | None = None, legacy_only: bool = False) -> dict:
    """Single source of truth for "how many <tier> vouchers can the bot
    actually issue right now" — built on the exact same active-batch,
    entitlement-window, and legacy-fallback rules issuance applies. The
    Admin Dashboard Pool Summary and affiliate issuance must both call this
    (never re-derive their own count), so they can never disagree about
    what's claimable for the same tier at the same moment.

    T1-T5 pools are evaluated with the same full-month-containment rule
    ``AFFILIATE_MONTHLY`` issuance uses (``_resolve_monthly_ledger_target``)
    rather than the looser "covers this instant" check, so a batch that
    only partially overlaps the current entitlement month is never reported
    as claimable here when monthly issuance couldn't actually claim it.
    WELCOME (and any explicit ``legacy_only`` lookup) keeps the existing
    instant-active-batch/legacy-fallback check, which is what their own
    issuance path uses.

    ``raw_available`` is the naive "status: available" row count — useful
    for diagnostics (codes physically exist) but never authoritative for
    what can actually be issued. ``claimable_available`` is authoritative.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    pool_id = str(pool_id or "").strip().upper()

    raw_available = int(db.voucher_pools.count_documents({"pool_id": pool_id, "status": "available"}))
    issued_count = int(db.voucher_pools.count_documents({"pool_id": pool_id, "status": "issued"}))

    if not legacy_only and pool_id in TIERS:
        claimable_available, blocking_reason = _monthly_entitlement_claimable_count_and_reason(
            db, pool_id=pool_id, now_utc=now_utc,
        )
    else:
        claimable_available = int(_available_pool_count(db, pool_id=pool_id, now_utc=now_utc, legacy_only=legacy_only))
        blocking_reason = None
        if claimable_available <= 0:
            blocking_reason = _pool_inventory_blocking_reason(db, pool_id=pool_id, now_utc=now_utc, legacy_only=legacy_only)

    if raw_available != claimable_available:
        logger.warning(
            "[AFF_POOL][INVENTORY_MISMATCH] pool=%s raw_available=%s claimable_available=%s reason=%s",
            pool_id, raw_available, claimable_available, blocking_reason,
        )

    return {
        "pool_id": pool_id,
        "claimable_available": claimable_available,
        "raw_available": raw_available,
        "issued": issued_count,
        "blocking_reason": blocking_reason,
    }


def _claim_affiliate_bundle_from_pool(db, *, pool_id: str, ledger_id, user_id: int, now_utc: datetime, voucher_count: int, legacy_only: bool = False):
    normalized_pool_id = str(pool_id or "").strip().upper()
    if normalized_pool_id in TIERS:
        # Defense-in-depth: even if a future caller reaches this claim
        # function without going through `_issue_affiliate_ledger_from_pool`'s
        # guard, never let a weekly ledger walk out with a T1-T5 bundle.
        owning_ledger = db.affiliate_ledger.find_one({"_id": ledger_id}, {"ledger_type": 1, "user_id": 1, "tier": 1})
        owning_ledger_type = str((owning_ledger or {}).get("ledger_type") or "").strip().upper()
        if owning_ledger_type == "AFFILIATE_WEEKLY":
            logger.error(
                "[AFF_REWARD][BLOCK_WEEKLY_TIER_ISSUE] ledger_id=%s user_id=%s tier=%s pool_id=%s",
                ledger_id, user_id, (owning_ledger or {}).get("tier"), normalized_pool_id,
            )
            return None
    needed = max(1, int(voucher_count))
    if _available_pool_count(db, pool_id=pool_id, now_utc=now_utc, legacy_only=legacy_only) < needed:
        _log_pool_claim_miss(db, pool_id=pool_id, ledger_id=ledger_id, user_id=user_id, now_utc=now_utc, legacy_only=legacy_only)
        return None

    claimed = []
    for _ in range(needed):
        voucher = _claim_voucher_from_pool(db, pool_id=pool_id, ledger_id=ledger_id, user_id=user_id, now_utc=now_utc, legacy_only=legacy_only)
        if not voucher:
            _guarded_rollback_attempt_vouchers(
                db,
                vouchers=claimed,
                ledger_id=ledger_id,
                reason="affiliate_bundle_partial_claim",
                now_utc=now_utc,
            )
            return None
        claimed.append(voucher)
    return claimed


def _claim_affiliate_bundle_from_target_batch(db, *, batch_id, pool_id: str, ledger_id, user_id: int, now_utc: datetime, voucher_count: int):
    """Same bundle-claim shape as ``_claim_affiliate_bundle_from_pool``, but
    pinned to one specific batch (an AFFILIATE_MONTHLY ledger's resolved
    ``target_batch_id``) — never substitutes a different batch or the
    legacy pool. Returns ``(vouchers_or_None, reason_or_None)``.
    """
    needed = max(1, int(voucher_count))
    batch = db.affiliate_voucher_batches.find_one({"_id": batch_id})
    if not batch:
        return None, "no_batch_for_entitlement_period"
    if batch.get("upload_status") not in (None, "ready"):
        return None, "target_batch_not_ready"
    if bool(batch.get("distribution_disabled")):
        return None, "target_batch_disabled"
    starts_at = _as_aware_utc(batch.get("starts_at"))
    ends_at = _as_aware_utc(batch.get("ends_at"))
    if starts_at is None or ends_at is None:
        return None, "target_batch_not_ready"
    if now_utc >= ends_at:
        return None, "target_batch_expired_unissued"
    if now_utc < starts_at:
        return None, "target_batch_scheduled"
    if _batch_claimable_available_count(db, batch) < needed:
        return None, "target_batch_empty"

    claimed = []
    for _ in range(needed):
        voucher, reason = _claim_from_target_batch(
            db, batch_id=batch_id, pool_id=pool_id, ledger_id=ledger_id, user_id=user_id, now_utc=now_utc,
        )
        if not voucher:
            if claimed:
                _guarded_rollback_attempt_vouchers(
                    db,
                    vouchers=claimed,
                    ledger_id=ledger_id,
                    reason="affiliate_bundle_partial_claim",
                    now_utc=now_utc,
                )
            return None, reason
        claimed.append(voucher)
    return claimed, None


def _month_window_from_yyyymm(yyyymm) -> tuple[datetime | None, datetime | None]:
    """KL-calendar-month window [start, end) in UTC for an explicit
    ``"YYYYMM"`` entitlement period — unlike ``_month_window_utc``, this
    resolves the period itself, not "the month containing now".
    """
    yyyymm = str(yyyymm or "").strip()
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        return None, None
    year, month = int(yyyymm[:4]), int(yyyymm[4:6])
    if not (1 <= month <= 12):
        return None, None
    # datetime's valid range is years 1-9999, so a syntactically-valid
    # 6-digit input at either extreme (e.g. "000001", or "999912" whose
    # following-month rolls into year 10000) would otherwise raise
    # ValueError out of this helper instead of the documented (None, None)
    # validation failure — surfacing as an uncaught 500 to any caller
    # (e.g. the admin create/update batch endpoints) instead of a clean
    # "invalid entitlement month" response.
    try:
        start_kl = KL_TZ.localize(datetime(year, month, 1))
        if month == 12:
            end_kl = KL_TZ.localize(datetime(year + 1, 1, 1))
        else:
            end_kl = KL_TZ.localize(datetime(year, month + 1, 1))
    except (ValueError, OverflowError):
        return None, None
    return start_kl.astimezone(timezone.utc), end_kl.astimezone(timezone.utc)


def _find_batches_for_period(db, *, pool_id: str, period_start_utc: datetime, period_end_utc: datetime) -> list[dict]:
    """All batches whose schedule window *fully contains* an entitlement
    month — ``starts_at <= period_start_utc`` and ``ends_at >= period_end_utc``.

    Deliberately full containment, not overlap: a batch that only
    intersects part of the month (e.g. starts mid-month, or ends before
    month-end) must never be treated as "the batch for this month" — that
    is exactly the entitlement-to-batch drift this pinning exists to
    prevent. Same-tier batches can never legitimately overlap (enforced at
    creation/update), so more than one full-containment match should be
    structurally impossible in normal operation; the caller treats it as
    an ambiguous, unsafe-to-guess result rather than picking one.
    """
    matches = []
    for row in db.affiliate_voucher_batches.find({"pool_id": pool_id}):
        starts_at = _as_aware_utc(row.get("starts_at"))
        ends_at = _as_aware_utc(row.get("ends_at"))
        if starts_at is None or ends_at is None:
            continue
        if starts_at <= period_start_utc and ends_at >= period_end_utc:
            matches.append(row)
    return matches


def _resolve_monthly_ledger_target(db, ledger: dict, *, now_utc: datetime) -> dict:
    """Pin an AFFILIATE_MONTHLY ledger to exactly one voucher source — a
    specific batch, or (transitionally) the legacy undated pool — the first
    time it becomes issuable, and persist that choice forever. This is what
    prevents a pending July entitlement from silently drifting onto an
    August batch: once resolved, ``target_mode``/``target_batch_id`` never
    change again, regardless of what batches exist later.
    """
    if ledger.get("target_mode") in ("batch", "legacy"):
        return ledger  # already resolved — never re-resolve or switch

    ledger_id = ledger["_id"]
    user_id = ledger.get("user_id")
    tier = str(ledger.get("tier") or "").strip().upper()
    year_month = ledger.get("year_month")
    period_start_utc, period_end_utc = _month_window_from_yyyymm(year_month)
    if period_start_utc is None or period_end_utc is None:
        # Malformed/missing year_month — leave unresolved; the caller's
        # pool-empty/manual-review path handles it, never legacy or a
        # guessed batch.
        return ledger

    matches = _find_batches_for_period(db, pool_id=tier, period_start_utc=period_start_utc, period_end_utc=period_end_utc)
    if len(matches) > 1:
        # Same-tier batches can't legitimately overlap, so this should be
        # structurally impossible — but never guess between them if it
        # somehow happens. Stay unresolved; manual review, every time.
        logger.error(
            "[AFF_VOUCHER][TARGET_BATCH_AMBIGUOUS] ledger_id=%s user_id=%s pool_id=%s year_month=%s "
            "month_start_utc=%s month_end_utc=%s conflicting_batch_ids=%s",
            ledger_id, user_id, tier, year_month, period_start_utc.isoformat(), period_end_utc.isoformat(),
            [str(m.get("_id")) for m in matches],
        )
        return ledger

    batch = matches[0] if matches else None
    if batch:
        update = {
            "target_mode": "batch",
            "target_batch_id": batch["_id"],
            "target_batch_window_start": batch.get("starts_at"),
            "target_batch_window_end": batch.get("ends_at"),
            "target_resolved_at": now_utc,
        }
    elif _tier_entered_scheduled_mode(db, pool_id=tier, reference_utc=period_start_utc):
        # Scheduled-batch mode had already begun for this tier by this
        # entitlement's month, but no batch fully covers this month — a
        # real gap, not a legacy-eligible ledger. Leave unresolved so the
        # caller routes to manual review; retried later in case the
        # missing batch gets uploaded (resolution is idempotent).
        logger.warning(
            "[AFF_VOUCHER][TARGET_BATCH_NOT_FOUND] ledger_id=%s user_id=%s pool_id=%s year_month=%s "
            "month_start_utc=%s month_end_utc=%s",
            ledger_id, user_id, tier, year_month, period_start_utc.isoformat(), period_end_utc.isoformat(),
        )
        return ledger
    else:
        update = {
            "target_mode": "legacy",
            "target_batch_id": None,
            "target_resolved_at": now_utc,
        }

    res = db.affiliate_ledger.update_one(
        {"_id": ledger_id, "target_mode": {"$exists": False}},
        {"$set": update},
    )
    if getattr(res, "modified_count", 0) == 1:
        logger.info(
            "[AFF_VOUCHER][TARGET_BATCH_RESOLVED] ledger_id=%s user_id=%s pool_id=%s year_month=%s "
            "month_start_utc=%s month_end_utc=%s mode=%s target_batch_id=%s",
            ledger_id, user_id, tier, year_month, period_start_utc.isoformat(), period_end_utc.isoformat(),
            update["target_mode"], update.get("target_batch_id"),
        )
    return db.affiliate_ledger.find_one({"_id": ledger_id}) or ledger


def _merge_monthly_risk_flags(existing_flags, fresh_abuse_flags) -> list:
    """Recomputing abuse/risk flags (``_risk_flags_for_referrer_month``) each
    evaluation is correct — an abuse signal that no longer applies should
    stop showing. But that recomputation only ever produces abuse flags, so
    a blind overwrite of ``risk_flags`` erases any inventory-only marker
    (``pool_empty``, ...) a prior claim attempt left behind, before the
    inventory-retry eligibility check below ever sees it. Carry those
    forward here; they are only ever cleared by
    ``_clear_inventory_only_risk_flags`` on an actual successful issuance.
    """
    preserved_inventory = [f for f in (existing_flags or []) if f in _INVENTORY_ONLY_RISK_FLAGS]
    fresh = list(fresh_abuse_flags or [])
    return fresh + [f for f in preserved_inventory if f not in fresh]


def _monthly_ledger_eligible_for_inventory_retry(ledger: dict) -> bool:
    """True only when an ``AFFILIATE_MONTHLY`` ledger is pinned (legacy or
    batch) purely because of an inventory/config gap, with nothing else
    standing in the way of a fresh same-tier retry: no abuse/risk-review
    signal, and no voucher already issued.

    This is deliberately conservative — an *empty* ``risk_flags`` list is
    not treated as eligible, since that means the ledger has never actually
    failed a claim yet (no evidence the pin is even stale); re-resolution is
    only for a ledger that is known to be stuck on inventory/config.
    """
    if str(ledger.get("ledger_type") or "").strip().upper() != "AFFILIATE_MONTHLY":
        return False
    if _ledger_uses_denomination_plan(ledger):
        # A denomination-plan ledger pins each pool independently under
        # `pool_targets`, and its allocation loop is already idempotent and
        # resumable — it needs no legacy single-pin re-resolution. Treat it
        # as never eligible for that path so `_reresolve_..._for_retry`
        # (which only understands `target_mode`) is never applied to it.
        return False
    if ledger.get("target_mode") not in ("batch", "legacy"):
        return False
    if _ledger_has_affiliate_bundle(ledger) or ledger.get("voucher_code"):
        return False
    flags = set(ledger.get("risk_flags") or [])
    if not flags:
        return False
    return flags <= _INVENTORY_ONLY_RISK_FLAGS


def _reresolve_monthly_ledger_target_for_retry(db, ledger: dict, *, now_utc: datetime) -> dict:
    """Clear a stale legacy/batch pin on an inventory-only-pending
    ``AFFILIATE_MONTHLY`` ledger so ``_resolve_monthly_ledger_target`` picks
    a fresh target for the SAME tier. Never touches ``tier``/``pool_id`` —
    only the previously-resolved target metadata.
    """
    ledger_id = ledger["_id"]
    tier = str(ledger.get("tier") or "").strip().upper()
    old_mode = ledger.get("target_mode")
    old_batch = ledger.get("target_batch_id")

    db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": {"$in": ["PENDING_MANUAL", SETTLING_STATUS]}, **_no_voucher_filter()},
        {
            "$set": {"updated_at": now_utc},
            "$unset": {
                "target_mode": "",
                "target_batch_id": "",
                "target_batch_window_start": "",
                "target_batch_window_end": "",
                "target_resolved_at": "",
            },
        },
    )
    refreshed = db.affiliate_ledger.find_one({"_id": ledger_id}) or ledger
    logger.info(
        "[AFF_RETRY][TARGET_RERESOLVE] ledger_id=%s user_id=%s tier=%s old_mode=%s old_batch=%s "
        "new_mode=%s new_batch=%s reason=%s",
        ledger_id, ledger.get("user_id"), tier, old_mode, old_batch,
        refreshed.get("target_mode"), refreshed.get("target_batch_id"), "inventory_only_retry",
    )
    return refreshed


def _clear_inventory_only_risk_flags(db, *, ledger_id, now_utc: datetime):
    """After a successful issuance, drop only the stale inventory/config
    flags (e.g. ``pool_empty``) — abuse/risk-review flags are never touched
    by this narrow ``$pull``.
    """
    db.affiliate_ledger.update_one(
        {"_id": ledger_id},
        {
            "$pull": {"risk_flags": {"$in": list(_INVENTORY_ONLY_RISK_FLAGS)}},
            "$set": {"updated_at": now_utc},
        },
    )


_WELCOME_TARGET_REASON_MAP = {
    "no_batch_for_entitlement_period": "no_welcome_batch_for_entitlement_time",
    "target_batch_not_ready": "welcome_target_batch_not_ready",
    "target_batch_disabled": "welcome_target_batch_disabled",
    "target_batch_expired_unissued": "welcome_target_batch_expired_unissued",
    "target_batch_scheduled": "welcome_target_batch_not_ready",
    "target_batch_empty": "welcome_target_batch_empty",
}


def _find_batches_covering_instant(db, *, pool_id: str, instant_utc: datetime) -> list[dict]:
    """All batches for ``pool_id`` whose schedule window covers a single
    instant — ``starts_at <= instant_utc < ends_at``. Same-pool batches
    can never legitimately overlap (enforced at creation/update), so more
    than one match should be structurally impossible; callers treat that
    as an ambiguous, unsafe-to-guess result rather than picking one.
    """
    matches = []
    for row in db.affiliate_voucher_batches.find({"pool_id": pool_id}):
        starts_at = _as_aware_utc(row.get("starts_at"))
        ends_at = _as_aware_utc(row.get("ends_at"))
        if starts_at is None or ends_at is None:
            continue
        if starts_at <= instant_utc < ends_at:
            matches.append(row)
    return matches


def _resolve_welcome_ledger_target(db, ledger: dict, *, now_utc: datetime) -> dict:
    """Pin a WELCOME ledger to exactly one voucher source — a specific
    batch, or (transitionally) the legacy undated pool — the first time
    it becomes issuable, and persist that choice forever.

    Source of truth for "when the user earned this entitlement": the
    ledger's own ``created_at`` (stamped once, on first eligibility
    check, via ``$setOnInsert`` in ``issue_welcome_bonus_if_eligible``).
    Resolving against that reference time — not whatever "now" a later
    retry happens to run at — is what prevents an entitlement earned on
    August 31st from silently drifting onto a September batch if the
    retry lands after the rollover. Once resolved, ``target_mode``/
    ``target_batch_id`` never change again, regardless of what batches
    exist later.
    """
    if ledger.get("target_mode") in ("batch", "legacy"):
        return ledger  # already resolved — never re-resolve or switch

    ledger_id = ledger["_id"]
    user_id = ledger.get("user_id")
    reference_utc = _as_aware_utc(ledger.get("created_at"))
    if reference_utc is None:
        # No usable entitlement-time reference — leave unresolved; the
        # caller's pool-empty/manual-review path handles it, never legacy
        # or a guessed batch.
        return ledger

    matches = _find_batches_covering_instant(db, pool_id="WELCOME", instant_utc=reference_utc)
    if len(matches) > 1:
        logger.error(
            "[WELCOME_VOUCHER][TARGET_BATCH_AMBIGUOUS] ledger_id=%s user_id=%s pool_id=WELCOME "
            "entitlement_reference_utc=%s conflicting_batch_ids=%s",
            ledger_id, user_id, reference_utc.isoformat(), [str(m.get("_id")) for m in matches],
        )
        return ledger

    batch = matches[0] if matches else None
    if batch:
        update = {
            "target_mode": "batch",
            "target_batch_id": batch["_id"],
            "target_batch_window_start": batch.get("starts_at"),
            "target_batch_window_end": batch.get("ends_at"),
            "target_resolved_at": now_utc,
        }
    elif _tier_entered_scheduled_mode(db, pool_id="WELCOME", reference_utc=reference_utc):
        # WELCOME had already entered scheduled-batch mode by this
        # entitlement's reference time, but no batch's window covers it —
        # a real gap, not a legacy-eligible ledger. Leave unresolved so
        # the caller routes to its existing pool-empty outcome; retried
        # later in case the missing batch gets uploaded (idempotent).
        logger.warning(
            "[WELCOME_VOUCHER][TARGET_BATCH_NOT_FOUND] ledger_id=%s user_id=%s pool_id=WELCOME "
            "entitlement_reference_utc=%s reason=no_welcome_batch_for_entitlement_time",
            ledger_id, user_id, reference_utc.isoformat(),
        )
        return ledger
    else:
        update = {
            "target_mode": "legacy",
            "target_batch_id": None,
            "target_resolved_at": now_utc,
        }

    res = db.affiliate_ledger.update_one(
        {"_id": ledger_id, "target_mode": {"$exists": False}},
        {"$set": update},
    )
    if getattr(res, "modified_count", 0) == 1:
        logger.info(
            "[WELCOME_VOUCHER][TARGET_BATCH_RESOLVED] ledger_id=%s user_id=%s pool_id=WELCOME "
            "entitlement_reference_utc=%s mode=%s target_batch_id=%s",
            ledger_id, user_id, reference_utc.isoformat(), update["target_mode"], update.get("target_batch_id"),
        )
    return db.affiliate_ledger.find_one({"_id": ledger_id}) or ledger


def _no_voucher_filter():
    return {"$or": [{"voucher_code": None}, {"voucher_code": {"$exists": False}}]}


def _as_aware_utc(value):
    if value is None or not hasattr(value, "tzinfo"):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def welcome_reward_visibility(row: dict, *, now_utc: datetime | None = None) -> dict:
    """Compute display visibility for an affiliate_ledger WELCOME row.

    Issued WELCOME voucher codes must only remain visible for
    WELCOME_REWARD_VISIBLE_DAYS after issuance. This never mutates or
    deletes the ledger row — it only decides whether the caller should
    render it. issued_at is preferred; for legacy rows lacking it,
    updated_at is used only when the row is already ISSUED (updated_at is
    stamped at the same moment status flips to ISSUED), otherwise
    created_at is used as a conservative fallback.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    issued_at = _as_aware_utc(row.get("issued_at"))
    source = "issued_at"
    if issued_at is None:
        updated_at = _as_aware_utc(row.get("updated_at"))
        if updated_at is not None and row.get("status") == "ISSUED":
            issued_at, source = updated_at, "updated_at_fallback"
        else:
            issued_at, source = _as_aware_utc(row.get("created_at")), "created_at_fallback"

    if issued_at is None:
        return {"visible": False, "visible_until": None, "timestamp_source": source, "issued_at": None}

    if issued_at > now_utc:
        # Malformed/future issued_at must never keep a card visible indefinitely — fail closed.
        return {
            "visible": False,
            "visible_until": issued_at,
            "timestamp_source": f"{source}_malformed_future",
            "issued_at": issued_at,
        }

    visible_until = issued_at + timedelta(days=WELCOME_REWARD_VISIBLE_DAYS)
    return {
        "visible": now_utc < visible_until,
        "visible_until": visible_until,
        "timestamp_source": source,
        "issued_at": issued_at,
    }


def _finalize_issued_if_voucher_exists(db, *, ledger, now_utc: datetime):
    if not ledger:
        return None
    if _ledger_has_affiliate_bundle(ledger):
        if ledger.get("status") != "ISSUED":
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "reward_type": AFFILIATE_BUNDLE_REWARD_TYPE, "status": {"$ne": "ISSUED"}},
                {"$set": {"status": "ISSUED", "updated_at": now_utc}},
            )
            return db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        return ledger
    voucher_code = ledger.get("voucher_code")
    if not voucher_code:
        return ledger
    if ledger.get("status") != "ISSUED":
        # Prefer the pool row's own issued_at (the moment the voucher was
        # actually allocated) over now_utc, so a delayed reconciliation
        # retry can't restart the 3-day visibility window.
        pool_row = db.voucher_pools.find_one(
            {"status": "issued", "code": voucher_code, **_pool_ledger_filter(ledger["_id"])}
        )
        issued_at = (pool_row or {}).get("issued_at") or now_utc
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "voucher_code": voucher_code, "status": {"$ne": "ISSUED"}},
            {"$set": {"status": "ISSUED", "updated_at": now_utc, "issued_at": issued_at}},
        )
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})
    return ledger


def _reconcile_ledger_from_issued_pool(db, *, ledger_id, now_utc: datetime):
    ledger = db.affiliate_ledger.find_one({"_id": ledger_id})
    if not ledger or ledger.get("status") == "SIMULATED_PENDING":
        return None
    if _ledger_has_affiliate_bundle(ledger):
        return _finalize_issued_if_voucher_exists(db, ledger=ledger, now_utc=now_utc)

    pool_row = db.voucher_pools.find_one(
        {
            "status": "issued",
            **_pool_ledger_filter(ledger_id),
        }
    )
    if not pool_row or not pool_row.get("code"):
        return None

    # Prefer the pool row's own issued_at (the moment the voucher was
    # actually allocated) over now_utc, so a delayed reconciliation retry
    # can't restart the 3-day visibility window.
    issued_at = pool_row.get("issued_at") or now_utc
    issue_claim = db.affiliate_ledger.update_one(
        {
            "_id": ledger_id,
            "status": {"$in": ["PENDING_MANUAL", "PENDING_REVIEW", "APPROVED", SETTLING_STATUS]},
            **_no_voucher_filter(),
        },
        {"$set": {"status": "ISSUED", "voucher_code": pool_row.get("code"), "updated_at": now_utc, "issued_at": issued_at}},
    )
    if issue_claim.modified_count == 0:
        latest = db.affiliate_ledger.find_one({"_id": ledger_id})
        return _finalize_issued_if_voucher_exists(db, ledger=latest, now_utc=now_utc)
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _has_issued_pool_voucher_for_ledger(db, *, ledger_id) -> bool:
    return (
        db.voucher_pools.find_one(
            {
                "status": "issued",
                **_pool_ledger_filter(ledger_id),
            },
            {"_id": 1},
        )
        is not None
    )


def _store_affiliate_bundle_on_ledger(
    db, *, ledger_id, tier: str, vouchers: list[dict], now_utc: datetime, recipe: dict | None = None,
):
    """Finalize a ledger to ISSUED with its complete bundle.

    The guarded update (``status == SETTLING`` and no ``voucher_code`` yet)
    is the single atomic point at which an entitlement becomes issued, so
    only one worker can ever win it — the loser gets ``modified_count == 0``
    and reconciles against the winner's result instead of writing its own.
    """
    payload = _affiliate_bundle_payload(tier=tier, vouchers=vouchers, recipe=recipe)
    if not payload.get("vouchers"):
        return None
    first_code = payload["vouchers"][0]["code"]
    update = {
        "status": "ISSUED",
        "updated_at": now_utc,
        "issued_at": now_utc,
        # `voucher_code` stays the FIRST code of the bundle purely for
        # backward compatibility with every historical single-code reader
        # (exports, congrats gating, admin lookups). It is never the
        # authoritative record of what was issued — `vouchers` is.
        "voucher_code": first_code,
        **payload,
    }
    if recipe:
        update.update(
            {
                "reward_plan": recipe.get("reward_plan"),
                "expected_code_count": int(recipe.get("expected_code_count") or 0),
                "reward_value": int(recipe.get("reward_value") or 0),
                "issued_code_count": int(payload.get("voucher_count") or 0),
                "issued_value": int(payload.get("total_value") or 0),
            }
        )
        if recipe.get("entitlement_month"):
            update["entitlement_month"] = recipe.get("entitlement_month")
    res = db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": SETTLING_STATUS, **_no_voucher_filter()},
        {
            "$set": update,
            # The bundle is complete: drop any shortage bookkeeping and
            # release the allocation lease.
            "$unset": {
                "missing_by_denomination": "",
                "allocation_lease_id": "",
                "allocation_lease_at": "",
            },
        },
    )
    if getattr(res, "modified_count", 0) != 1:
        return None
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _classify_issued_pool_rows(db, ledger: dict, *, recipe: dict | None = None) -> dict:
    """Group the voucher_pools rows already linked to ``ledger`` against the
    denominations its recipe requires — the single primitive behind both
    reconciliation and idempotent resume.

    Returns::

        {
          "required":  {pool_id: qty},        # what the recipe owes
          "allocated": {pool_id: [rows]},     # accepted rows, per pool
          "missing":   {pool_id: qty},        # still to claim
          "foreign":   [rows],                # explicitly another user's
          "surplus":   [rows],                # off-recipe / over-quota
          "rows":      [rows],                # every linked issued row
          "complete":  bool,                  # exact recipe satisfied
        }

    ``foreign``/``surplus`` are never silently absorbed: any of either means
    the linkage is corrupt or ambiguous, and the caller must refuse to
    finalize (and refuse to claim more) rather than guess.
    """
    ledger_id = (ledger or {}).get("_id")
    user_id = (ledger or {}).get("user_id")
    recipe = recipe if recipe is not None else _ledger_recipe(ledger)
    required = recipe_required_by_pool(recipe)

    rows = _issued_pool_vouchers_for_ledger(db, ledger_id=ledger_id)

    def _row_user(row: dict):
        raw = row.get("issued_to_user_id")
        if raw is None:
            raw = row.get("issued_to")  # legacy field
        return raw

    allocated: dict[str, list] = {pool_id: [] for pool_id in required}
    foreign: list[dict] = []
    surplus: list[dict] = []
    for row in rows:
        # A row with no issued_to/issued_to_user_id at all (older rows
        # written before that field was consistently stamped) is not a
        # mismatch — the ledger link is already per-ledger (hence
        # per-user); only an EXPLICIT different user id disqualifies.
        row_user = _row_user(row)
        if row_user is not None and int(row_user) != int(user_id or 0):
            foreign.append(row)
            continue
        pool_id = str(row.get("pool_id") or "").strip().upper()
        if pool_id not in required:
            # Never let a T1/WELCOME/AFFILIATE_5/... row help complete a
            # bundle that does not call for that pool.
            surplus.append(row)
            continue
        if len(allocated[pool_id]) >= int(required[pool_id]):
            surplus.append(row)
            continue
        allocated[pool_id].append(row)

    for bucket in allocated.values():
        bucket.sort(key=lambda row: row.get("_id"))

    missing = {
        pool_id: int(required[pool_id]) - len(allocated[pool_id])
        for pool_id in required
        if len(allocated[pool_id]) < int(required[pool_id])
    }
    complete = bool(required) and not missing and not foreign and not surplus
    return {
        "required": required,
        "allocated": allocated,
        "missing": missing,
        "foreign": foreign,
        "surplus": surplus,
        "rows": rows,
        "complete": complete,
    }


def _ordered_bundle_rows(recipe: dict | None, allocated: dict) -> list[dict]:
    """Bundle rows in the recipe's own deterministic component order, so a
    user always receives (and an admin always sees) the codes in a stable,
    reproducible sequence."""
    ordered: list[dict] = []
    seen = set()
    for component in (recipe or {}).get("components") or []:
        pool_id = str(component.get("pool_id") or "").strip().upper()
        if pool_id in seen:
            continue
        seen.add(pool_id)
        ordered.extend(allocated.get(pool_id) or [])
    return ordered


def _find_complete_issued_affiliate_bundle(db, ledger: dict) -> list[dict] | None:
    """Bundle-aware replacement for "does one voucher_pools row exist for
    this ledger": determines whether a COMPLETE bundle of already-issued
    voucher_pools rows exists for ``ledger``, matching its recipe exactly,
    without consuming any inventory. Returns the exact rows making up the
    bundle (deterministically ordered) or ``None`` if no complete,
    unambiguous bundle is found — including when linked rows belong to
    another user, come from a pool the recipe does not call for, exceed the
    required quantity, or simply do not add up to the full recipe yet.

    Recipe-driven, so it is correct for BOTH plans: a legacy ledger's
    recipe is a single per-tier pool (identical to the previous
    single-denomination check), while a denomination-plan ledger's recipe
    may span several pools with different quantities.
    """
    if not ledger:
        return None
    tier = str(ledger.get("tier") or "").strip().upper()
    recipe = _ledger_recipe(ledger)
    if not recipe:
        return None
    ledger_id = ledger.get("_id")
    user_id = ledger.get("user_id")

    state = _classify_issued_pool_rows(db, ledger, recipe=recipe)
    if not state["rows"]:
        return None

    required_total = int(recipe.get("expected_code_count") or 0)
    accepted_total = sum(len(v) for v in state["allocated"].values())

    if state["foreign"] or state["surplus"]:
        logger.error(
            "[AFF_RECONCILE][MISMATCH] ledger_id=%s expected_tier=%s found_tiers=%s expected_count=%s found_count=%s",
            ledger_id,
            tier,
            sorted({str(r.get("pool_id") or "").strip().upper() for r in state["rows"]}),
            required_total,
            accepted_total,
        )
        return None

    logger.info(
        "[AFF_RECONCILE][FOUND_BUNDLE] ledger_id=%s user_id=%s tier=%s expected=%s found=%s",
        ledger_id, user_id, tier, required_total, accepted_total,
    )

    if not state["complete"]:
        # Partial bundle — conservative: never finalize, and never let the
        # caller claim a second full bundle on top of it.
        return None

    return _ordered_bundle_rows(recipe, state["allocated"])


def _reconcile_affiliate_bundle_from_issued_pool(db, *, ledger, now_utc: datetime):
    if not ledger or _ledger_has_affiliate_bundle(ledger):
        return _finalize_issued_if_voucher_exists(db, ledger=ledger, now_utc=now_utc)
    tier = str(ledger.get("tier") or "").strip().upper()
    ledger_id = ledger.get("_id")
    bundle_rows = _find_complete_issued_affiliate_bundle(db, ledger)
    if not bundle_rows:
        return None
    stored = _store_affiliate_bundle_on_ledger(
        db,
        ledger_id=ledger_id,
        tier=tier,
        vouchers=bundle_rows,
        now_utc=now_utc,
        recipe=_ledger_recipe(ledger),
    )
    if stored:
        if str(ledger.get("ledger_type") or "").strip().upper() == "AFFILIATE_MONTHLY":
            # Stale inventory/config flags (e.g. pool_empty) no longer apply
            # once reconciliation confirms the bundle is actually issued —
            # never touches abuse/risk flags (the $pull list only ever
            # contains inventory-only ones).
            _clear_inventory_only_risk_flags(db, ledger_id=ledger_id, now_utc=now_utc)
            stored = db.affiliate_ledger.find_one({"_id": ledger_id}) or stored
        logger.info(
            "[AFF_RECONCILE][FINALIZED] ledger_id=%s tier=%s voucher_count=%s",
            ledger_id, tier, len(bundle_rows),
        )
        logger.info("[AFFILIATE][BUNDLE_RECONCILE_OK] ledger_id=%s tier=%s count=%s", ledger_id, tier, len(bundle_rows))
    return stored


# --------------------------------------------------------------------------
# Denomination-plan (entitlement month 202609+) multi-denomination issuance
# --------------------------------------------------------------------------

# How long one worker may hold the per-ledger allocation lease before another
# worker is allowed to take it over. Sized well above a normal allocation
# (a handful of single-document claims) and well below the 5-minute retry
# tick, so a crashed worker's ledger resumes on the very next pass.
_ALLOCATION_LEASE_TTL_SECONDS = 120


def _acquire_allocation_lease(db, *, ledger_id, now_utc: datetime):
    """Serialize bundle allocation for one ledger across concurrent workers.

    Without this, two workers that both observe a ledger in SETTLING would
    each compute "0 of 7 x $50 allocated" and each claim 7 codes — 14 codes
    consumed for a 7-code entitlement. A single atomic ``find_one_and_update``
    picks exactly one winner; every loser returns ``None`` and simply leaves
    the ledger for the next retry pass rather than allocating anything.

    The lease is time-bounded, so a worker that crashes mid-allocation
    cannot wedge the entitlement forever: after the TTL another worker takes
    over, and because allocation is count-then-claim-only-what-is-missing,
    it safely RESUMES rather than restarting.
    """
    lease_id = f"{now_utc.timestamp()}:{os.getpid()}:{id(db)}"
    stale_cutoff = now_utc - timedelta(seconds=_ALLOCATION_LEASE_TTL_SECONDS)
    claimed = db.affiliate_ledger.find_one_and_update(
        {
            "_id": ledger_id,
            "status": SETTLING_STATUS,
            "$or": [
                {"allocation_lease_at": {"$exists": False}},
                {"allocation_lease_at": None},
                {"allocation_lease_at": {"$lt": stale_cutoff}},
            ],
        },
        {"$set": {"allocation_lease_id": lease_id, "allocation_lease_at": now_utc}},
        return_document=ReturnDocument.AFTER,
    )
    if claimed and claimed.get("allocation_lease_id") == lease_id:
        return lease_id
    return None


def _release_allocation_lease(db, *, ledger_id, lease_id):
    if not lease_id:
        return
    db.affiliate_ledger.update_one(
        {"_id": ledger_id, "allocation_lease_id": lease_id},
        {"$unset": {"allocation_lease_id": "", "allocation_lease_at": ""}},
    )


def _resolve_denomination_pool_target(db, *, ledger, pool_id: str, entitlement_month, now_utc: datetime):
    """Pin ONE denomination pool of a multi-denomination entitlement to a
    single voucher source, and never let it drift.

    Same guarantee as ``_resolve_monthly_ledger_target`` gives a legacy
    single-pool ledger, but stored per pool under ``pool_targets.<POOL_ID>``
    because one entitlement now draws from several pools independently.
    Returns ``(target_or_None, reason_or_None)``.
    """
    ledger_id = ledger["_id"]
    existing = ((ledger.get("pool_targets") or {}).get(pool_id) or {})
    if existing.get("mode") in ("batch", "legacy"):
        return existing, None  # already resolved — never re-resolve or switch

    period_start_utc, period_end_utc = _month_window_from_yyyymm(entitlement_month)
    if period_start_utc is None or period_end_utc is None:
        return None, "no_batch_for_entitlement_period"

    matches = _find_batches_for_period(
        db, pool_id=pool_id, period_start_utc=period_start_utc, period_end_utc=period_end_utc
    )
    if len(matches) > 1:
        # Same-pool batches cannot legitimately overlap, so this should be
        # structurally impossible — never guess between them if it happens.
        logger.error(
            "[AFF_VOUCHER][TARGET_BATCH_AMBIGUOUS] ledger_id=%s user_id=%s pool_id=%s year_month=%s "
            "month_start_utc=%s month_end_utc=%s conflicting_batch_ids=%s",
            ledger_id, ledger.get("user_id"), pool_id, entitlement_month,
            period_start_utc.isoformat(), period_end_utc.isoformat(),
            [str(m.get("_id")) for m in matches],
        )
        return None, "no_batch_for_entitlement_period"

    batch = matches[0] if matches else None
    if batch:
        target = {
            "mode": "batch",
            "batch_id": batch["_id"],
            "window_start": batch.get("starts_at"),
            "window_end": batch.get("ends_at"),
            "resolved_at": now_utc,
        }
    elif _tier_entered_scheduled_mode(db, pool_id=pool_id, reference_utc=period_start_utc):
        # Scheduled-batch mode had already begun for this pool by this
        # entitlement's month, but no batch covers the month — a real gap,
        # never a licence to fall back to undated legacy stock.
        logger.warning(
            "[AFF_VOUCHER][TARGET_BATCH_NOT_FOUND] ledger_id=%s user_id=%s pool_id=%s year_month=%s "
            "month_start_utc=%s month_end_utc=%s",
            ledger_id, ledger.get("user_id"), pool_id, entitlement_month,
            period_start_utc.isoformat(), period_end_utc.isoformat(),
        )
        return None, "no_batch_for_entitlement_period"
    else:
        target = {"mode": "legacy", "batch_id": None, "resolved_at": now_utc}

    db.affiliate_ledger.update_one(
        {"_id": ledger_id, f"pool_targets.{pool_id}.mode": {"$exists": False}},
        {"$set": {f"pool_targets.{pool_id}": target, "updated_at": now_utc}},
    )
    refreshed = db.affiliate_ledger.find_one({"_id": ledger_id}) or ledger
    resolved = ((refreshed.get("pool_targets") or {}).get(pool_id) or {})
    if resolved.get("mode") in ("batch", "legacy"):
        logger.info(
            "[AFF_VOUCHER][TARGET_BATCH_RESOLVED] ledger_id=%s user_id=%s pool_id=%s year_month=%s "
            "mode=%s target_batch_id=%s",
            ledger_id, ledger.get("user_id"), pool_id, entitlement_month,
            resolved.get("mode"), resolved.get("batch_id"),
        )
        return resolved, None
    return None, "no_batch_for_entitlement_period"


def _claim_one_denomination_voucher(db, *, target, pool_id: str, ledger_id, user_id: int, now_utc: datetime):
    """Claim exactly ONE code from an already-pinned denomination source.

    Deliberately single-code: the caller loops, so a bundle is built by
    independently atomic per-code claims that can stop and resume at any
    point without ever double-claiming.
    """
    if (target or {}).get("mode") == "batch":
        return _claim_from_target_batch(
            db,
            batch_id=target.get("batch_id"),
            pool_id=pool_id,
            ledger_id=ledger_id,
            user_id=user_id,
            now_utc=now_utc,
        )
    voucher = _claim_legacy_voucher(
        db, pool_id=pool_id, ledger_id=ledger_id, user_id=user_id, now_utc=now_utc,
    )
    if voucher:
        return voucher, None
    return None, _pool_inventory_blocking_reason(db, pool_id=pool_id, now_utc=now_utc, legacy_only=True)


def _issue_denomination_bundle(db, *, ledger, recipe: dict, now_utc: datetime):
    """Issue (or resume issuing) a multi-denomination tier entitlement.

    Contract, in order:
      1. Resolve the plan/recipe from the ledger's stored entitlement month
         (done by the caller) — never from the processing date.
      2. Determine which codes are ALREADY allocated to this ledger.
      3. Reserve only the missing denomination quantities.
      4. Reconcile the allocated count and monetary value.
      5. Mark ISSUED only when the exact recipe is complete.

    Partial allocation is RETAINED, never rolled back. See the module notes
    on ``bundle_denomination_short``: releasing a partial multi-denomination
    bundle under shortage risks losing already-secured codes to other
    ledgers while the shortage persists, and makes progress non-monotonic
    (a T5 needing 7 x $50 that repeatedly secures 5 and gives them back can
    thrash indefinitely). Retaining makes resume deterministic: each pass
    claims strictly what is still missing and nothing else.
    """
    ledger_id = ledger.get("_id")
    user_id = int(ledger.get("user_id"))
    tier = str(ledger.get("tier") or "").strip().upper()
    entitlement_month = _ledger_entitlement_month(ledger)

    lease_id = _acquire_allocation_lease(db, ledger_id=ledger_id, now_utc=now_utc)
    if not lease_id:
        # Another worker is mid-allocation on this exact ledger. Do nothing —
        # the retry sweep re-examines it once the winner finishes or its
        # lease expires.
        logger.info(
            "[AFF_BUNDLE][LEASE_BUSY] ledger_id=%s user_id=%s tier=%s", ledger_id, user_id, tier,
        )
        return db.affiliate_ledger.find_one({"_id": ledger_id})

    try:
        state = _classify_issued_pool_rows(db, ledger, recipe=recipe)

        if state["foreign"] or state["surplus"]:
            # Corrupt/ambiguous linkage — refuse to claim anything more and
            # park for a human, exactly as the legacy path does.
            logger.error(
                "[AFF_RECONCILE][PARTIAL_BUNDLE_BLOCK] ledger_id=%s tier=%s "
                "reason=existing_issued_rows_incomplete_or_mismatched",
                ledger_id, tier,
            )
            db.affiliate_ledger.update_one(
                {"_id": ledger_id, "status": SETTLING_STATUS, **_no_voucher_filter()},
                {
                    "$set": {"status": "PENDING_MANUAL", "updated_at": now_utc},
                    "$addToSet": {"risk_flags": "partial_bundle_conflict"},
                },
            )
            return db.affiliate_ledger.find_one({"_id": ledger_id})

        shortage_reasons: dict[str, str] = {}
        if state["missing"]:
            working = db.affiliate_ledger.find_one({"_id": ledger_id}) or ledger
            for pool_id, still_needed in sorted(state["missing"].items()):
                target, reason = _resolve_denomination_pool_target(
                    db,
                    ledger=working,
                    pool_id=pool_id,
                    entitlement_month=entitlement_month,
                    now_utc=now_utc,
                )
                working = db.affiliate_ledger.find_one({"_id": ledger_id}) or working
                if not target:
                    shortage_reasons[pool_id] = reason or "pool_empty"
                    continue
                for _ in range(int(still_needed)):
                    voucher, claim_reason = _claim_one_denomination_voucher(
                        db,
                        target=target,
                        pool_id=pool_id,
                        ledger_id=ledger_id,
                        user_id=user_id,
                        now_utc=now_utc,
                    )
                    if not voucher:
                        shortage_reasons[pool_id] = claim_reason or "pool_empty"
                        break

        # Re-read the authoritative allocation after claiming.
        final_state = _classify_issued_pool_rows(db, ledger, recipe=recipe)
        allocated_count = sum(len(v) for v in final_state["allocated"].values())

        if final_state["complete"]:
            bundle_rows = _ordered_bundle_rows(recipe, final_state["allocated"])
            issued = _store_affiliate_bundle_on_ledger(
                db, ledger_id=ledger_id, tier=tier, vouchers=bundle_rows, now_utc=now_utc, recipe=recipe,
            )
            if issued:
                _clear_inventory_only_risk_flags(db, ledger_id=ledger_id, now_utc=now_utc)
                issued = db.affiliate_ledger.find_one({"_id": ledger_id}) or issued
                logger.info(
                    "[AFF_BUNDLE][ISSUE_OK] ledger_id=%s user_id=%s tier=%s plan=%s month=%s "
                    "codes=%s value=%s",
                    ledger_id, user_id, tier, recipe.get("reward_plan"), entitlement_month,
                    issued.get("issued_code_count"), issued.get("issued_value"),
                )
                return issued
            # Lost the finalize race — the winner already wrote the bundle.
            latest = _finalize_issued_if_voucher_exists(
                db, ledger=db.affiliate_ledger.find_one({"_id": ledger_id}), now_utc=now_utc,
            )
            return latest or db.affiliate_ledger.find_one({"_id": ledger_id})

        # Incomplete: record exactly what is missing, by denomination, so
        # operators can see which pool to restock and the retry pass can
        # resume without recomputing anything.
        missing = final_state["missing"]
        logger.warning(
            "[AFF_BUNDLE][SHORTAGE] ledger_id=%s user_id=%s tier=%s plan=%s month=%s "
            "allocated=%s expected=%s missing=%s reasons=%s",
            ledger_id, user_id, tier, recipe.get("reward_plan"), entitlement_month,
            allocated_count, recipe.get("expected_code_count"), missing, shortage_reasons,
        )
        db.affiliate_ledger.update_one(
            {"_id": ledger_id, "status": SETTLING_STATUS, **_no_voucher_filter()},
            {
                "$set": {
                    "status": "PENDING_MANUAL",
                    "updated_at": now_utc,
                    "allocated_code_count": allocated_count,
                    "missing_by_denomination": missing,
                    "shortage_reasons": shortage_reasons,
                },
                "$addToSet": {"risk_flags": "bundle_denomination_short"},
            },
        )
        return db.affiliate_ledger.find_one({"_id": ledger_id})
    finally:
        _release_allocation_lease(db, ledger_id=ledger_id, lease_id=lease_id)


def _issue_affiliate_ledger_from_pool(db, ledger, now_utc: datetime):
    if not ledger:
        return None
    ledger_id = ledger.get("_id")
    user_id = ledger.get("user_id")
    tier = str(ledger.get("tier") or "").strip().upper()
    pool_id = str(ledger.get("pool_id") or tier or "").strip().upper()
    if not user_id or not pool_id:
        _mark_missing_pool_config(db, ledger_id=ledger_id, now_utc=now_utc)
        return db.affiliate_ledger.find_one({"_id": ledger_id})
    if pool_id not in TIERS:
        logger.warning("[AFFILIATE][INVALID_TIER] uid=%s ledger_id=%s tier=%s pool_id=%s", user_id, ledger_id, tier, pool_id)
        _mark_missing_pool_config(db, ledger_id=ledger_id, now_utc=now_utc)
        return db.affiliate_ledger.find_one({"_id": ledger_id})
    ledger_type = (ledger.get("ledger_type") or "").strip().upper()
    bundle_spec = _affiliate_bundle_spec(tier)
    if not bundle_spec:
        logger.warning("[AFFILIATE][INVALID_BUNDLE_TIER] uid=%s ledger_id=%s tier=%s pool_id=%s", user_id, ledger_id, tier, pool_id)
        _mark_missing_pool_config(db, ledger_id=ledger_id, now_utc=now_utc)
        return db.affiliate_ledger.find_one({"_id": ledger_id})
    if _ledger_has_affiliate_bundle(ledger):
        # Already-issued rows (including any pre-fix weekly T1-T5 bundles)
        # are historical fact — never touched here, only finalized/read.
        return _finalize_issued_if_voucher_exists(db, ledger=ledger, now_utc=now_utc)
    # T1-T5 voucher bundles are a MONTHLY-only entitlement. This is the
    # single choke point every issuance path funnels through (scheduled
    # weekly/monthly jobs, admin manual approval, retry/reconcile scans),
    # so block here rather than trusting every caller to have already
    # excluded weekly ledgers — a new weekly path added later still can't
    # consume T1-T5 inventory. Only ledgers that have NOT yet claimed a
    # bundle reach this point (the has-bundle case returns above).
    if ledger_type == "AFFILIATE_WEEKLY" and pool_id in TIERS:
        logger.error(
            "[AFF_REWARD][BLOCK_WEEKLY_TIER_ISSUE] ledger_id=%s user_id=%s tier=%s pool_id=%s",
            ledger_id, user_id, tier, pool_id,
        )
        db.affiliate_ledger.update_one(
            {"_id": ledger_id, "status": {"$nin": list(FINAL_STATUSES)}},
            {
                "$set": {
                    "status": "REJECTED",
                    "review_reason": "weekly_tier_pool_blocked",
                    "updated_at": now_utc,
                },
                "$addToSet": {"risk_flags": "weekly_tier_pool_blocked"},
            },
        )
        return db.affiliate_ledger.find_one({"_id": ledger_id})
    if ledger_type == "AFFILIATE_MONTHLY":
        duplicate = db.affiliate_ledger.find_one(
            {
                "_id": {"$ne": ledger_id},
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": int(user_id),
                "year_month": ledger.get("year_month"),
                "tier": tier,
                "status": {"$in": ["ISSUED", "SETTLING", "APPROVED", "PENDING_EOM"]},
            },
            {"_id": 1},
        )
        if duplicate:
            db.affiliate_ledger.update_one(
                {"_id": ledger_id, **_no_voucher_filter()},
                {
                    "$set": {
                        "status": "REJECTED",
                        "review_reason": "duplicate_monthly_tier",
                        "duplicate_of": duplicate.get("_id"),
                        "updated_at": now_utc,
                    }
                },
            )
            logger.warning(
                "[AFFILIATE][DUPLICATE_MONTHLY_TIER] uid=%s year_month=%s tier=%s current_id=%s duplicate_id=%s",
                int(user_id),
                ledger.get("year_month"),
                tier,
                ledger_id,
                duplicate.get("_id"),
            )
            return db.affiliate_ledger.find_one({"_id": ledger_id})

    logger.info(
        "[AFFILIATE][ISSUE_ATTEMPT] ledger_id=%s user_id=%s tier=%s pool_id=%s status=%s",
        ledger_id,
        int(user_id),
        tier,
        pool_id,
        ledger.get("status"),
    )

    # ---- Plan fork -------------------------------------------------------
    # Resolved from the ledger's STORED entitlement month, never from
    # now_utc: an August entitlement retried in September still takes the
    # legacy path below, with the legacy bundle.
    recipe = _ledger_recipe(ledger)
    if recipe and is_denomination_plan(recipe.get("reward_plan")):
        settle_claim = db.affiliate_ledger.update_one(
            {
                "_id": ledger_id,
                "status": {"$in": ["APPROVED", "PENDING_REVIEW", "PENDING_MANUAL", "PENDING_EOM", SETTLING_STATUS]},
                **_no_voucher_filter(),
            },
            {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
        )
        latest = db.affiliate_ledger.find_one({"_id": ledger_id})
        if (latest or {}).get("status") != SETTLING_STATUS:
            # Someone else finalized/rejected it while we looked.
            return _finalize_issued_if_voucher_exists(db, ledger=latest, now_utc=now_utc)
        if settle_claim.modified_count == 0:
            logger.info(
                "[AFFILIATE][SETTLING_PROCEED] ledger_id=%s user_id=%s tier=%s reason=pre_settled",
                ledger_id, int(user_id), tier,
            )
        return _issue_denomination_bundle(db, ledger=latest, recipe=recipe, now_utc=now_utc)

    settle_claim = db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": {"$in": ["APPROVED", "PENDING_REVIEW", "PENDING_MANUAL", "PENDING_EOM", SETTLING_STATUS]}, **_no_voucher_filter()},
        {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
    )
    if settle_claim.modified_count == 0:
        latest = _finalize_issued_if_voucher_exists(db, ledger=db.affiliate_ledger.find_one({"_id": ledger_id}), now_utc=now_utc)
        if latest and latest.get("status") == "ISSUED":
            return latest
        if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger_id):
            latest = _reconcile_affiliate_bundle_from_issued_pool(db, ledger=latest, now_utc=now_utc) or latest
            if latest and latest.get("status") == "ISSUED":
                logger.info("[AFFILIATE][BUNDLE_RECONCILE_ISSUED] ledger_id=%s tier=%s count=%s", ledger_id, tier, latest.get("voucher_count"))
                return latest
            if (latest or {}).get("status") != SETTLING_STATUS:
                return latest
        # When the caller already transitioned the ledger to SETTLING (no-op double-settle),
        # modified_count==0 is not a concurrency conflict — proceed to claim the voucher.
        if (latest or {}).get("status") != SETTLING_STATUS:
            return latest
        logger.info(
            "[AFFILIATE][SETTLING_PROCEED] ledger_id=%s user_id=%s tier=%s reason=pre_settled",
            ledger_id, int(user_id), tier,
        )

    if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger_id):
        latest = _reconcile_affiliate_bundle_from_issued_pool(
            db,
            ledger=db.affiliate_ledger.find_one({"_id": ledger_id}),
            now_utc=now_utc,
        ) or db.affiliate_ledger.find_one({"_id": ledger_id})
        if latest and latest.get("status") == "ISSUED":
            logger.info("[AFFILIATE][BUNDLE_RECONCILE_ISSUED] ledger_id=%s tier=%s count=%s", ledger_id, tier, latest.get("voucher_count"))
            return latest
        if (latest or {}).get("status") != SETTLING_STATUS:
            return latest
        # Reconciliation ran (logging FOUND_BUNDLE/MISMATCH above) but could
        # not finalize this ledger — a partial bundle, cross-tier
        # contamination, or wrong-user rows already linked to it. Never fall
        # through to a fresh claim on top of that: it would risk claiming a
        # second full bundle alongside the stray already-issued rows (e.g. 2
        # old + 3 new = 5 vouchers for a 3-voucher tier). Park for manual
        # review instead.
        logger.error(
            "[AFF_RECONCILE][PARTIAL_BUNDLE_BLOCK] ledger_id=%s tier=%s reason=existing_issued_rows_incomplete_or_mismatched",
            ledger_id, tier,
        )
        db.affiliate_ledger.update_one(
            {"_id": ledger_id, "status": SETTLING_STATUS, **_no_voucher_filter()},
            {"$set": {"status": "PENDING_MANUAL", "updated_at": now_utc}, "$addToSet": {"risk_flags": "partial_bundle_conflict"}},
        )
        return db.affiliate_ledger.find_one({"_id": ledger_id})

    required_count = int(bundle_spec["voucher_count"])
    claim_reason = None
    inventory_retry = False
    if ledger_type == "AFFILIATE_MONTHLY":
        # Pin this entitlement to exactly one voucher source (a specific
        # batch, or transitionally the legacy pool) the first time it's
        # issuable, and never let it drift onto a later batch — UNLESS the
        # existing pin is stuck purely on an inventory/config gap (no
        # abuse/risk flag, no voucher issued yet), in which case clear it so
        # the resolver below picks a fresh target for this SAME tier.
        if _monthly_ledger_eligible_for_inventory_retry(ledger):
            inventory_retry = True
            ledger = _reresolve_monthly_ledger_target_for_retry(db, ledger, now_utc=now_utc)
        ledger = _resolve_monthly_ledger_target(db, ledger, now_utc=now_utc)
        target_mode = ledger.get("target_mode")
        # Same-tier invariant: whatever the resolver just pinned this ledger
        # to must itself belong to `tier` — never claim against it otherwise.
        resolved_pool_id = None
        if target_mode == "batch":
            resolved_batch = db.affiliate_voucher_batches.find_one(
                {"_id": ledger.get("target_batch_id")}, {"pool_id": 1}
            )
            resolved_pool_id = str((resolved_batch or {}).get("pool_id") or "").strip().upper()
        elif target_mode == "legacy":
            resolved_pool_id = str(ledger.get("pool_id") or tier).strip().upper()
        if target_mode in ("batch", "legacy") and resolved_pool_id != tier:
            # Must never happen — the resolver only ever queries this
            # tier's batches/pool. Refuse to claim rather than risk a
            # cross-tier fallback.
            logger.error(
                "[AFF_RETRY][TIER_MISMATCH] ledger_id=%s user_id=%s tier=%s resolved_pool_id=%s target_mode=%s",
                ledger_id, user_id, tier, resolved_pool_id, target_mode,
            )
            target_mode = None
            claim_reason = "tier_pool_mismatch"
        if target_mode == "batch":
            vouchers, claim_reason = _claim_affiliate_bundle_from_target_batch(
                db,
                batch_id=ledger.get("target_batch_id"),
                pool_id=pool_id,
                ledger_id=ledger_id,
                user_id=int(user_id),
                now_utc=now_utc,
                voucher_count=required_count,
            )
        elif target_mode == "legacy":
            vouchers = _claim_affiliate_bundle_from_pool(
                db,
                pool_id=pool_id,
                ledger_id=ledger_id,
                user_id=int(user_id),
                now_utc=now_utc,
                voucher_count=required_count,
                legacy_only=True,
            )
        else:
            vouchers = None
            claim_reason = "no_batch_for_entitlement_period"
    else:
        vouchers = _claim_affiliate_bundle_from_pool(
            db,
            pool_id=pool_id,
            ledger_id=ledger_id,
            user_id=int(user_id),
            now_utc=now_utc,
            voucher_count=required_count,
        )
    if vouchers:
        issued = _store_affiliate_bundle_on_ledger(
            db,
            ledger_id=ledger_id,
            tier=tier,
            vouchers=vouchers,
            now_utc=now_utc,
        )
        if not issued:
            latest = _guarded_rollback_attempt_vouchers(
                db,
                vouchers=vouchers,
                ledger_id=ledger_id,
                reason="affiliate_bundle_ledger_update_failed",
                now_utc=now_utc,
            )
            if latest and latest.get("status") == "ISSUED" and _ledger_has_affiliate_bundle(latest):
                return latest
            return latest or db.affiliate_ledger.find_one({"_id": ledger_id})
        if ledger_type == "AFFILIATE_MONTHLY":
            # Stale inventory/config flags (e.g. pool_empty) no longer apply
            # once the bundle actually cleared — never touches abuse/risk
            # flags, since the $pull list only ever contains inventory ones.
            _clear_inventory_only_risk_flags(db, ledger_id=ledger_id, now_utc=now_utc)
            issued = db.affiliate_ledger.find_one({"_id": ledger_id}) or issued
        logger.info(
            "[AFFILIATE][BUNDLE_ISSUE_OK] ledger_id=%s user_id=%s tier=%s pool_id=%s count=%s total=%s",
            ledger_id,
            int(user_id),
            tier,
            pool_id,
            issued.get("voucher_count"),
            issued.get("total_value"),
        )
        if inventory_retry:
            logger.info(
                "[AFF_RETRY][ISSUED] ledger_id=%s tier=%s bundle_size=%s",
                ledger_id, tier, issued.get("voucher_count"),
            )
        return issued

    latest = _finalize_issued_if_voucher_exists(db, ledger=db.affiliate_ledger.find_one({"_id": ledger_id}), now_utc=now_utc)
    if latest and latest.get("status") == "ISSUED":
        return latest
    if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger_id):
        return latest

    risk_flag = claim_reason or "pool_empty"
    if inventory_retry:
        claimable_count = _available_pool_count(db, pool_id=pool_id, now_utc=now_utc)
        logger.info(
            "[AFF_RETRY][NO_STOCK] ledger_id=%s tier=%s claimable_count=%s bundle_required=%s",
            ledger_id, tier, claimable_count, required_count,
        )
    db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": SETTLING_STATUS, **_no_voucher_filter()},
        {"$set": {"status": "PENDING_MANUAL", "updated_at": now_utc}, "$addToSet": {"risk_flags": risk_flag}},
    )
    logger.info(
        "[AFFILIATE][ISSUE_SKIP] ledger_id=%s user_id=%s tier=%s pool_id=%s reason=%s",
        ledger_id,
        int(user_id),
        tier,
        pool_id,
        risk_flag,
    )
    return db.affiliate_ledger.find_one({"_id": ledger_id})


def _affiliate_simulate_enabled() -> bool:
    return str(os.getenv("AFFILIATE_SIMULATE", "0")).strip() == "1"


def _week_start_kl(reference: datetime | None = None) -> datetime:
    ref = reference or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    ref_kl = ref.astimezone(KL_TZ)
    return (ref_kl - timedelta(days=ref_kl.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def _current_week_key_kl(reference: datetime | None = None) -> str:
    return _week_start_kl(reference).date().isoformat()


def _weekly_valid_referral_count_for_referrer(db, *, referrer_id: int, week_key: str) -> int:
    settled = int(
        db.referral_events.count_documents(
            {"inviter_id": int(referrer_id), "event": "referral_settled", "week_key": week_key}
        )
    )
    revoked = int(
        db.referral_events.count_documents(
            with_not_invalidated(
                {"inviter_id": int(referrer_id), "event": "referral_revoked", "week_key": week_key}
            )
        )
    )
    return max(0, settled - revoked)


def _send_affiliate_group_invite_dm(referrer_id: int, *, invite_url: str) -> tuple[bool, str | None]:
    text = AFFILIATE_GROUP_INVITE_TEXT.format(invite_url=invite_url)
    ok, err, _ = send_telegram_http_message(int(referrer_id), text)
    return ok, err


def _maybe_trigger_affiliate_group_invite(db, *, referrer_id: int | None, now_utc: datetime) -> None:
    if referrer_id is None:
        return
    invite_url = (AFFILIATE_GROUP_INVITE_URL or "").strip()
    if not invite_url:
        logger.info("[AFF_GROUP][SKIP] reason=missing_invite_url referrer=%s", referrer_id)
        return
    if _affiliate_simulate_enabled():
        logger.info("[AFF_GROUP][SKIP] reason=simulate_mode referrer=%s", referrer_id)
        return

    week_key = _current_week_key_kl(now_utc)
    count = _weekly_valid_referral_count_for_referrer(db, referrer_id=int(referrer_id), week_key=week_key)
    threshold = int(AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS)
    logger.info("[AFF_GROUP][CHECK] referrer=%s week_key=%s count=%s", referrer_id, week_key, count)
    if count < threshold:
        logger.info("[AFF_GROUP][SKIP] reason=below_threshold referrer=%s count=%s", referrer_id, count)
        return
    if count > threshold:
        logger.info("[AFF_GROUP][SKIP] reason=above_threshold_no_backfill referrer=%s count=%s", referrer_id, count)
        return

    row_filter = {"user_id": int(referrer_id), "week_key": week_key}
    existing = db.affiliate_group_invites.find_one(row_filter) or {}
    prev_status = existing.get("status")
    if prev_status == "sent":
        logger.info("[AFF_GROUP][SKIP] reason=already_sent referrer=%s week_key=%s", referrer_id, week_key)
        return
    if existing.get("sending") is True:
        logger.info("[AFF_GROUP][SKIP] reason=inflight referrer=%s week_key=%s", referrer_id, week_key)
        return
    if prev_status in {"failed", "pending", "skipped"}:
        logger.info("[AFF_GROUP][RETRY] referrer=%s week_key=%s prev_status=%s", referrer_id, week_key, prev_status)

    claim_filter = {
        "user_id": int(referrer_id),
        "week_key": week_key,
        "$and": [
            {"$or": [{"status": {"$ne": "sent"}}, {"status": {"$exists": False}}]},
            {"$or": [{"sending": {"$ne": True}}, {"sending": {"$exists": False}}]},
        ],
    }
    try:
        claimed = db.affiliate_group_invites.find_one_and_update(
            claim_filter,
            {
                "$setOnInsert": {
                    "user_id": int(referrer_id),
                    "week_key": week_key,
                    "created_at": now_utc,
                },
                "$set": {
                    "trigger_count": int(count),
                    "invite_url": invite_url,
                    "status": "pending",
                    "sending": True,
                    "send_attempted_at": now_utc,
                    "sent_at": None,
                    "error": None,
                    "updated_at": now_utc,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        existing = db.affiliate_group_invites.find_one(row_filter) or {}
        if existing.get("status") == "sent":
            logger.info("[AFF_GROUP][SKIP] reason=already_sent referrer=%s week_key=%s", referrer_id, week_key)
            return
        if existing.get("sending") is True:
            logger.info("[AFF_GROUP][SKIP] reason=inflight referrer=%s week_key=%s", referrer_id, week_key)
            return
        logger.info("[AFF_GROUP][RETRY] referrer=%s week_key=%s prev_status=%s", referrer_id, week_key, existing.get("status"))
        claimed = db.affiliate_group_invites.find_one_and_update(
            claim_filter,
            {
                "$set": {
                    "trigger_count": int(count),
                    "invite_url": invite_url,
                    "status": "pending",
                    "sending": True,
                    "send_attempted_at": now_utc,
                    "sent_at": None,
                    "error": None,
                    "updated_at": now_utc,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    if not claimed:
        latest = db.affiliate_group_invites.find_one(row_filter) or {}
        if latest.get("status") == "sent":
            logger.info("[AFF_GROUP][SKIP] reason=already_sent referrer=%s week_key=%s", referrer_id, week_key)
            return
        logger.info("[AFF_GROUP][SKIP] reason=inflight referrer=%s week_key=%s", referrer_id, week_key)
        return

    ok, err = _send_affiliate_group_invite_dm(int(referrer_id), invite_url=invite_url)
    if ok:
        db.affiliate_group_invites.update_one(
            {"user_id": int(referrer_id), "week_key": week_key},
            {
                "$set": {"status": "sent", "sending": False, "sent_at": now_utc, "updated_at": now_utc},
                "$unset": {"error": ""},
            },
        )
        logger.info("[AFF_GROUP][SENT] referrer=%s week_key=%s count=%s", referrer_id, week_key, count)
        return

    db.affiliate_group_invites.update_one(
        {"user_id": int(referrer_id), "week_key": week_key},
        {"$set": {"status": "failed", "sending": False, "error": err, "updated_at": now_utc}},
    )
    logger.error("[AFF_GROUP][FAIL] referrer=%s week_key=%s err=%s", referrer_id, week_key, err)


def record_user_last_seen(db, *, user_id: int, ip: str | None = None, subnet: str | None = None, session: str | None = None, seen_at: datetime | None = None):
    now_utc = seen_at or datetime.now(timezone.utc)
    db.user_last_seen.update_one(
        {"user_id": int(user_id)},
        {
            "$set": {
                "user_id": int(user_id),
                "ip": ip,
                "subnet": subnet,
                "session": session,
                "seen_at": now_utc,
            }
        },
        upsert=True,
    )


def is_user_blocked_for_self_invite(db, user_id: int) -> bool:
    """True if `user_id` has a referral/referral_event marking them as a self-invite.

    Covers: invitee_user_id == inviter_user_id, reason == "self_invite", and
    status == "skipped" with reason == "self_invite".
    """
    uid = int(user_id)
    try:
        doc = db.referral_audit.find_one(
            {
                "invitee_user_id": uid,
                "$or": [{"inviter_user_id": uid}, {"reason": "self_invite"}],
            },
            {"_id": 1},
        )
    except Exception:
        logger.exception("[WELCOME_BLOCKED_SELF_INVITE] lookup_failed user_id=%s", uid)
        return False
    return doc is not None


def issue_welcome_bonus_if_eligible(db, *, user_id: int, is_new_user: bool, blocked: bool = False, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    if not is_new_user or blocked:
        return {"created": False, "status": "SKIPPED"}
    if is_user_blocked_for_self_invite(db, user_id):
        logger.info("[WELCOME_BLOCKED_SELF_INVITE] user_id=%s", int(user_id))
        return {"created": False, "status": "BLOCKED_SELF_INVITE"}
    if not _is_official_channel_subscribed(int(user_id)):
        return {"created": False, "status": "NOT_SUBSCRIBED"}

    dedup_key = f"WELCOME:{int(user_id)}"
    db.affiliate_ledger.update_one(
        {"dedup_key": dedup_key},
        {
            "$setOnInsert": {
                "ledger_type": "WELCOME",
                "user_id": int(user_id),
                "year_month": None,
                "tier": "WELCOME",
                "pool_id": "WELCOME",
                "status": "PENDING_MANUAL",
                "dedup_key": dedup_key,
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now_utc,
                "updated_at": now_utc,
            }
        },
        upsert=True,
    )
    ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key})
    if not ledger:
        return {"created": False, "status": "ERROR"}
    if _affiliate_simulate_enabled():
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {
                "$set": {
                    "status": "SIMULATED_PENDING",
                    "simulate": True,
                    "would_issue_pool": "WELCOME",
                    "evaluated_at_utc": now_utc,
                    "updated_at": now_utc,
                }
            },
        )
        return {"created": True, "status": "SIMULATED_PENDING"}
    if ledger.get("status") in {"ISSUED", "OUT_OF_STOCK"}:
        return {"created": False, "status": ledger.get("status"), "voucher_code": ledger.get("voucher_code")}

    issue_claim = db.affiliate_ledger.update_one(
        {"_id": ledger["_id"], "status": {"$in": ["PENDING_MANUAL", "PENDING_REVIEW", "APPROVED", SETTLING_STATUS]}, **_no_voucher_filter()},
        {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
    )
    if issue_claim.modified_count == 0:
        latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        latest = _finalize_issued_if_voucher_exists(db, ledger=latest, now_utc=now_utc)
        if latest and not latest.get("voucher_code") and latest.get("status") != "SIMULATED_PENDING":
            latest = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or latest
        if latest and latest.get("status") == "ISSUED":
            return {"created": False, "status": "ISSUED", "voucher_code": latest.get("voucher_code")}
        return {"created": False, "status": (latest or {}).get("status")}

    if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
        latest = _reconcile_ledger_from_issued_pool(db, ledger_id=ledger["_id"], now_utc=now_utc) or db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        if latest and latest.get("status") == "ISSUED":
            return {"created": False, "status": "ISSUED", "voucher_code": latest.get("voucher_code")}

    # Resolve (once, permanently) exactly which WELCOME source this
    # entitlement draws from — a specific scheduled batch pinned to the
    # ledger's own created_at, or (transitionally) the legacy undated pool
    # — using the same authoritative batch resolution as T1-T4, so a claim
    # retry can never silently drift onto a later batch than the one the
    # user actually earned into. See ``_resolve_welcome_ledger_target``.
    ledger = _resolve_welcome_ledger_target(db, ledger, now_utc=now_utc)
    target_mode = ledger.get("target_mode")
    claim_reason = None
    if target_mode == "batch":
        voucher, claim_reason = _claim_from_target_batch(
            db, batch_id=ledger.get("target_batch_id"), pool_id="WELCOME",
            ledger_id=ledger.get("_id"), user_id=int(user_id), now_utc=now_utc,
        )
    elif target_mode == "legacy":
        voucher = _claim_voucher_from_pool(
            db, pool_id="WELCOME", ledger_id=ledger.get("_id"), user_id=int(user_id), now_utc=now_utc, legacy_only=True,
        )
    else:
        voucher = None
        claim_reason = "no_batch_for_entitlement_period"

    if voucher:
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
            {"$set": {"status": "ISSUED", "voucher_code": voucher.get("code"), "updated_at": now_utc, "issued_at": now_utc}},
        )
        return {"created": True, "status": "ISSUED", "voucher_code": voucher.get("code")}

    welcome_reason = _WELCOME_TARGET_REASON_MAP.get(claim_reason, "no_welcome_batch_for_entitlement_time")
    if target_mode == "legacy":
        _log_pool_claim_miss(db, pool_id="WELCOME", ledger_id=ledger.get("_id"), user_id=int(user_id), now_utc=now_utc, legacy_only=True)
    else:
        logger.warning(
            "[WELCOME_VOUCHER][TARGET_BATCH_%s] ledger_id=%s user_id=%s pool_id=WELCOME target_batch_id=%s reason=%s",
            "DISABLED" if welcome_reason == "welcome_target_batch_disabled"
            else "EMPTY" if welcome_reason == "welcome_target_batch_empty"
            else "NOT_FOUND",
            ledger.get("_id"), user_id, ledger.get("target_batch_id"), welcome_reason,
        )
    oos_claim = db.affiliate_ledger.update_one(
        {"_id": ledger["_id"], "status": SETTLING_STATUS, **_no_voucher_filter()},
        {"$set": {"status": "OUT_OF_STOCK", "updated_at": now_utc}, "$addToSet": {"risk_flags": welcome_reason}},
    )
    if oos_claim.modified_count == 0:
        latest = db.affiliate_ledger.find_one({"_id": ledger["_id"]}) or {}
        return {"created": False, "status": latest.get("status"), "voucher_code": latest.get("voucher_code")}
    return {"created": True, "status": "OUT_OF_STOCK"}


def _risk_flags_for_referrer_month(db, *, referrer_id: int, start_utc: datetime, end_utc: datetime):
    flags = []
    for row in db.qualified_events.aggregate([
        {"$match": {"referrer_id": int(referrer_id), "qualified_at": {"$gte": start_utc, "$lt": end_utc}, "ip": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$ip", "invitees": {"$addToSet": "$invitee_id"}}},
        {"$project": {"count": {"$size": "$invitees"}}},
    ]):
        if int(row.get("count", 0)) >= 4:
            flags.append("ip_cluster")
            break

    for row in db.qualified_events.aggregate([
        {"$match": {"referrer_id": int(referrer_id), "qualified_at": {"$gte": start_utc, "$lt": end_utc}, "subnet": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$subnet", "invitees": {"$addToSet": "$invitee_id"}}},
        {"$project": {"count": {"$size": "$invitees"}}},
    ]):
        if int(row.get("count", 0)) >= 6:
            flags.append("subnet_cluster")
            break

    deny_from = end_utc - timedelta(days=7)
    deny_count = db.referral_audit.count_documents(
        {
            "inviter_user_id": int(referrer_id),
            "created_at": {"$gte": deny_from, "$lt": end_utc},
            "reason": {"$in": ["deny", "deny_severe", "blocked", "abuse"]},
        }
    )
    if int(deny_count) >= 3:
        flags.append("deny_count_7d")
    return flags


def _eligible_tiers_for_count(qualified_count: int) -> list[str]:
    return [
        tier_name for tier_name, threshold in (
            ("T1", T1_THRESHOLD),
            ("T2", T2_THRESHOLD),
            ("T3", T3_THRESHOLD),
            ("T4", T4_THRESHOLD),
            ("T5", T5_THRESHOLD),
        ) if int(qualified_count) >= int(threshold)
    ]


def evaluate_monthly_affiliate_reward(db, *, referrer_id: int, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    user_doc = db.users.find_one({"user_id": int(referrer_id)}, {"blocked": 1}) or {}

    start_utc, end_utc, yyyymm = _month_window_utc(now_utc)
    qualified_count = db.qualified_events.count_documents(
        {"referrer_id": int(referrer_id), "qualified_at": {"$gte": start_utc, "$lt": end_utc}}
    )
    logger.info("[AFFILIATE][EVAL_COUNT] user_id=%s year_month=%s qualified_count=%s", int(referrer_id), yyyymm, int(qualified_count))
    logger.info(
        "[AFFILIATE][EVAL_START] user_id=%s qualified_count=%s year_month=%s",
        int(referrer_id),
        int(qualified_count),
        yyyymm,
    )
    tier = _tier_for_count(int(qualified_count))
    if not tier:
        return None
    if user_doc.get("blocked"):
        dedup_key = f"AFF:{int(referrer_id)}:{yyyymm}:{tier}"
        db.affiliate_ledger.update_one(
            {"dedup_key": dedup_key},
            {
                "$setOnInsert": {
                    "ledger_type": "AFFILIATE_MONTHLY",
                    "user_id": int(referrer_id),
                    "year_month": yyyymm,
                    "entitlement_month": yyyymm,
                    "reward_plan": resolve_plan_id(yyyymm),
                    "bundle_recipe": tier_recipe(yyyymm, tier),
                    "expected_code_count": int(
                        (tier_recipe(yyyymm, tier) or {}).get("expected_code_count") or 0
                    ),
                    "reward_value": int((tier_recipe(yyyymm, tier) or {}).get("reward_value") or 0),
                    "tier": tier,
                    "pool_id": tier,
                    "dedup_key": dedup_key,
                    "voucher_code": None,
                    "created_at": now_utc,
                },
                "$set": {
                    "qualified_count": int(qualified_count),
                    "status": "PENDING_REVIEW",
                    "risk_flags": ["blocked_user"],
                    "review_reason": "blocked_user",
                    "updated_at": now_utc,
                },
            },
            upsert=True,
        )
        logger.info(
            "[AFFILIATE][BLOCKED_LEDGER_CREATED] user_id=%s tier=%s year_month=%s",
            int(referrer_id),
            tier,
            yyyymm,
        )
        return db.affiliate_ledger.find_one({"dedup_key": dedup_key})
    eligible_tiers = _eligible_tiers_for_count(int(qualified_count))
    try:
        risk_flags = _risk_flags_for_referrer_month(db, referrer_id=int(referrer_id), start_utc=start_utc, end_utc=end_utc)
    except Exception as exc:
        logger.exception(
            "[AFFILIATE][EVAL_ERROR] referrer_id=%s year_month=%s qualified_count=%s eligible_tiers=%s err_class=%s err_msg=%s",
            int(referrer_id),
            yyyymm,
            int(qualified_count),
            eligible_tiers,
            exc.__class__.__name__,
            str(exc),
            exc_info=True,
        )
        risk_flags = ["risk_flags_calc_failed"]

    last_ledger = None
    for eligible_tier in eligible_tiers:
        dedup_key = f"AFF:{int(referrer_id)}:{yyyymm}:{eligible_tier}"
        logger.info(
            "[AFFILIATE][LEDGER_CREATE_ATTEMPT] referrer_id=%s year_month=%s tier=%s pool_id=%s dedup_key=%s qualified_count=%s eligible_tiers=%s risk_flags=%s",
            int(referrer_id),
            yyyymm,
            eligible_tier,
            eligible_tier,
            dedup_key,
            int(qualified_count),
            eligible_tiers,
            list(risk_flags),
        )
        existing_ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key}, {"risk_flags": 1})
        merged_risk_flags = _merge_monthly_risk_flags((existing_ledger or {}).get("risk_flags"), risk_flags)
        # Freeze the obligation at creation, from the ENTITLEMENT month
        # (not the processing date), so this entitlement stays reproducible
        # forever — even if the plan tables change, and even if it is not
        # fulfilled until months later. `$setOnInsert` guarantees a
        # re-evaluation never rewrites an existing ledger's obligation.
        frozen_recipe = tier_recipe(yyyymm, eligible_tier)
        try:
            db.affiliate_ledger.update_one(
                {"dedup_key": dedup_key},
                {
                "$setOnInsert": {
                    "ledger_type": "AFFILIATE_MONTHLY",
                    "user_id": int(referrer_id),
                    "year_month": yyyymm,
                    "entitlement_month": yyyymm,
                    "reward_plan": (frozen_recipe or {}).get("reward_plan") or resolve_plan_id(yyyymm),
                    "bundle_recipe": frozen_recipe,
                    "expected_code_count": int((frozen_recipe or {}).get("expected_code_count") or 0),
                    "reward_value": int((frozen_recipe or {}).get("reward_value") or 0),
                    "tier": eligible_tier,
                    "pool_id": eligible_tier,
                    "status": "APPROVED",
                    "dedup_key": dedup_key,
                    "voucher_code": None,
                    "created_at": now_utc,
                },
                "$set": {
                    "qualified_count": int(qualified_count),
                    "risk_flags": merged_risk_flags,
                        "updated_at": now_utc,
                    },
                },
                upsert=True,
            )
        except Exception as exc:
            logger.exception(
                "[AFFILIATE][LEDGER_CREATE_ERROR] referrer_id=%s year_month=%s tier=%s dedup_key=%s qualified_count=%s eligible_tiers=%s risk_flags=%s err_class=%s err_msg=%s",
                int(referrer_id),
                yyyymm,
                eligible_tier,
                dedup_key,
                int(qualified_count),
                eligible_tiers,
                list(risk_flags),
                exc.__class__.__name__,
                str(exc),
                exc_info=True,
            )
            raise
        ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key})
        if not ledger:
            continue
        logger.info(
            "[AFFILIATE][LEDGER_CREATE_OK] referrer_id=%s year_month=%s tier=%s pool_id=%s dedup_key=%s qualified_count=%s eligible_tiers=%s risk_flags=%s status=%s",
            int(referrer_id),
            yyyymm,
            eligible_tier,
            eligible_tier,
            dedup_key,
            int(qualified_count),
            eligible_tiers,
            list(risk_flags),
            str(ledger.get("status") or ""),
        )

        status = ledger.get("status")
        if status in FINAL_STATUSES:
            logger.info("[AFFILIATE][LEDGER_SKIP] user_id=%s tier=%s year_month=%s reason=final_status status=%s", int(referrer_id), eligible_tier, yyyymm, status)
            last_ledger = ledger
            continue

        if _affiliate_simulate_enabled():
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {
                    "$set": {
                        "status": "SIMULATED_PENDING",
                        "simulate": True,
                        "would_issue_pool": eligible_tier,
                        "evaluated_at_utc": now_utc,
                        "qualified_count": int(qualified_count),
                        "risk_flags": list(risk_flags),
                        "updated_at": now_utc,
                    }
                },
            )
            last_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            continue

        if status == "SIMULATED_PENDING":
            logger.info("[AFFILIATE][LEDGER_SKIP] user_id=%s tier=%s year_month=%s reason=simulated_pending", int(referrer_id), eligible_tier, yyyymm)
            last_ledger = ledger
            continue

        # If stuck in SETTLING, check whether pool claim already completed and reconcile;
        # if the pool claim never ran, fall through to retry it now.
        if status == SETTLING_STATUS:
            if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger["_id"]):
                last_ledger = _reconcile_affiliate_bundle_from_issued_pool(db, ledger=ledger, now_utc=now_utc) or ledger
                continue
            # Pool claim didn't complete — fall through to claim below

        # Transition any non-final, non-settling status to SETTLING before claiming.
        if status != SETTLING_STATUS:
            settle_res = db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": {"$nin": list(FINAL_STATUSES)}, **_no_voucher_filter()},
                {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
            )
            if settle_res.modified_count == 0:
                refreshed = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
                last_ledger = _finalize_issued_if_voucher_exists(db, ledger=refreshed, now_utc=now_utc)
                if last_ledger and not _ledger_has_affiliate_bundle(last_ledger) and last_ledger.get("status") != "SIMULATED_PENDING":
                    last_ledger = _reconcile_affiliate_bundle_from_issued_pool(db, ledger=last_ledger, now_utc=now_utc) or last_ledger
                continue

        last_ledger = _issue_affiliate_ledger_from_pool(db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc)

    return last_ledger


def evaluate_weekly_affiliate_reward(
    db,
    *,
    referrer_id: int,
    week_start_utc: datetime,
    week_end_utc: datetime,
    week_key: str,
    now_utc: datetime | None = None,
):
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    user_doc = db.users.find_one({"user_id": int(referrer_id)}, {"blocked": 1}) or {}
    qualified_count = db.qualified_events.count_documents(
        {"referrer_id": int(referrer_id), "qualified_at": {"$gte": week_start_utc, "$lt": week_end_utc}}
    )
    logger.info(
        "[AFFILIATE][WEEKLY_EVAL_START] user_id=%s qualified_count=%s week_key=%s",
        int(referrer_id),
        int(qualified_count),
        week_key,
    )
    tier = _tier_for_count(int(qualified_count))
    if not tier:
        return None

    eligible_tiers = _eligible_tiers_for_count(int(qualified_count))
    base_risk_flags = ["blocked_user"] if user_doc.get("blocked") else []
    last_ledger = None
    for eligible_tier in eligible_tiers:
        dedup_key = f"AFFW:{int(referrer_id)}:{week_key}:{eligible_tier}"
        insert_doc = {
            "ledger_type": "AFFILIATE_WEEKLY",
            "user_id": int(referrer_id),
            "week_key": week_key,
            "week_start_utc": week_start_utc,
            "week_end_utc": week_end_utc,
            "year_month": None,
            "tier": eligible_tier,
            "pool_id": eligible_tier,
            "status": "PENDING_REVIEW" if user_doc.get("blocked") else "APPROVED",
            "dedup_key": dedup_key,
            "voucher_code": None,
            "created_at": now_utc,
        }
        set_doc = {
            "qualified_count": int(qualified_count),
            "risk_flags": list(base_risk_flags),
            "updated_at": now_utc,
        }
        if user_doc.get("blocked"):
            set_doc["status"] = "PENDING_REVIEW"
            set_doc["review_reason"] = "blocked_user"
        logger.info(
            "[AFFILIATE][WEEKLY_LEDGER_CREATE_ATTEMPT] user_id=%s tier=%s week_key=%s",
            int(referrer_id),
            eligible_tier,
            week_key,
        )
        try:
            db.affiliate_ledger.update_one(
                {"dedup_key": dedup_key},
                {"$setOnInsert": insert_doc, "$set": set_doc},
                upsert=True,
            )
            logger.info(
                "[AFFILIATE][WEEKLY_LEDGER_CREATE_OK] user_id=%s tier=%s week_key=%s",
                int(referrer_id),
                eligible_tier,
                week_key,
            )
        except Exception as e:
            logger.error(
                "[AFFILIATE][WEEKLY_LEDGER_CREATE_ERROR] user_id=%s tier=%s week_key=%s err_class=%s err_msg=%s",
                int(referrer_id),
                eligible_tier,
                week_key,
                e.__class__.__name__,
                str(e),
                exc_info=True,
            )
            raise
        ledger = db.affiliate_ledger.find_one({"dedup_key": dedup_key})
        if not ledger:
            continue

        status = ledger.get("status")
        if status in FINAL_STATUSES:
            last_ledger = ledger
            continue
        if user_doc.get("blocked"):
            last_ledger = ledger
            continue
        if _affiliate_simulate_enabled():
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {
                    "$set": {
                        "status": "SIMULATED_PENDING",
                        "simulate": True,
                        "would_issue_pool": eligible_tier,
                        "evaluated_at_utc": now_utc,
                        "qualified_count": int(qualified_count),
                        "updated_at": now_utc,
                    }
                },
            )
            last_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
            continue

        if status == "SIMULATED_PENDING":
            last_ledger = ledger
            continue

        if status != SETTLING_STATUS:
            settle_res = db.affiliate_ledger.update_one(
                {"_id": ledger["_id"], "status": {"$nin": list(FINAL_STATUSES)}, **_no_voucher_filter()},
                {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
            )
            if settle_res.modified_count == 0:
                refreshed = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
                last_ledger = _finalize_issued_if_voucher_exists(db, ledger=refreshed, now_utc=now_utc)
                if last_ledger and not _ledger_has_affiliate_bundle(last_ledger) and last_ledger.get("status") != "SIMULATED_PENDING":
                    last_ledger = _reconcile_affiliate_bundle_from_issued_pool(db, ledger=last_ledger, now_utc=now_utc) or last_ledger
                continue

        last_ledger = _issue_affiliate_ledger_from_pool(
            db,
            ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}),
            now_utc=now_utc,
        )

    return last_ledger


def issue_weekly_affiliate_rewards_for_window(
    db,
    *,
    week_start_utc: datetime,
    week_end_utc: datetime,
    week_key: str,
    now_utc: datetime | None = None,
    batch_limit: int = 500,
):
    now_utc = now_utc or datetime.now(timezone.utc)
    limit = max(1, int(batch_limit))
    summary = {
        "week_key": week_key,
        "week_start_utc": week_start_utc,
        "week_end_utc": week_end_utc,
        "scanned_users": 0,
        "eligible_users": 0,
        "created_ledgers": 0,
        "issued_count": 0,
        "skipped_existing": 0,
        "pending_manual": 0,
        "pool_empty": 0,
        "errors": 0,
    }

    pipeline = [
        {"$match": {"qualified_at": {"$gte": week_start_utc, "$lt": week_end_utc}, "referrer_id": {"$ne": None}}},
        {"$group": {"_id": "$referrer_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": T1_THRESHOLD}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": limit},
    ]
    rows = list(db.qualified_events.aggregate(pipeline))
    summary["scanned_users"] = int(len(rows))
    summary["eligible_users"] = int(len(rows))

    for row in rows:
        uid = int(row.get("_id"))
        qualified_count = int(row.get("count") or 0)
        expected_tiers = _eligible_tiers_for_count(qualified_count)
        existing_before = list(
            db.affiliate_ledger.find(
                {
                    "ledger_type": "AFFILIATE_WEEKLY",
                    "user_id": uid,
                    "week_key": week_key,
                    "tier": {"$in": expected_tiers},
                },
                {"tier": 1, "status": 1},
            )
        )
        before_status_by_tier = {str(doc.get("tier")): str(doc.get("status") or "") for doc in existing_before}
        summary["skipped_existing"] += len(existing_before)
        try:
            evaluate_weekly_affiliate_reward(
                db,
                referrer_id=uid,
                week_start_utc=week_start_utc,
                week_end_utc=week_end_utc,
                week_key=week_key,
                now_utc=now_utc,
            )
        except Exception as e:
            summary["errors"] += 1
            logger.error(
                "[AFFILIATE][WEEKLY_BULK_ISSUE_ERROR] user_id=%s week_key=%s err_class=%s err_msg=%s",
                uid,
                week_key,
                e.__class__.__name__,
                str(e),
                exc_info=True,
            )
            continue

        final_ledgers = list(
            db.affiliate_ledger.find(
                {
                    "ledger_type": "AFFILIATE_WEEKLY",
                    "user_id": uid,
                    "week_key": week_key,
                    "tier": {"$in": expected_tiers},
                }
            )
        )
        for ledger in final_ledgers:
            before_status = before_status_by_tier.get(str(ledger.get("tier")))
            status = str(ledger.get("status") or "")
            if status == "ISSUED" and before_status != "ISSUED":
                summary["issued_count"] += 1
            elif status == "PENDING_MANUAL" and before_status != "PENDING_MANUAL":
                summary["pending_manual"] += 1
                if "pool_empty" in (ledger.get("risk_flags") or []):
                    summary["pool_empty"] += 1

    logger.info("[AFFILIATE][WEEKLY_BULK_ISSUE_SUMMARY] %s", summary)
    return summary


def issue_current_week_affiliate_rewards(db, now_utc: datetime | None = None, batch_limit: int = 500):
    now_utc = now_utc or datetime.now(timezone.utc)
    week_start_utc, week_end_utc, week_key = _week_window_utc(now_utc)
    return issue_weekly_affiliate_rewards_for_window(
        db,
        week_start_utc=week_start_utc,
        week_end_utc=week_end_utc,
        week_key=week_key,
        now_utc=now_utc,
        batch_limit=batch_limit,
    )


def issue_previous_week_affiliate_rewards(db, now_utc: datetime | None = None, batch_limit: int = 500):
    now_utc = now_utc or datetime.now(timezone.utc)
    week_start_utc, week_end_utc, week_key = _previous_completed_week_window_utc(now_utc)
    return issue_weekly_affiliate_rewards_for_window(
        db,
        week_start_utc=week_start_utc,
        week_end_utc=week_end_utc,
        week_key=week_key,
        now_utc=now_utc,
        batch_limit=batch_limit,
    )


def _retry_stuck_pending_manual_affiliate_ledgers(db, *, now_utc: datetime, batch_limit: int = 200) -> dict:
    """Direct-scan companion to the qualified-events-driven retry above.

    ``issue_current_month_affiliate_rewards`` only ever re-evaluates
    referrers who have a NEW qualifying event in the current month — it
    never looks at ``affiliate_ledger`` directly. A ledger already stuck in
    PENDING_MANUAL (e.g. one whose full voucher bundle was actually issued
    to ``voucher_pools`` already, but whose own status update lost a race
    or never ran) would otherwise sit there forever unless that same
    referrer happens to qualify again this month.

    This scans PENDING_MANUAL AFFILIATE_MONTHLY ledgers directly and gives
    each one more pass through ``_issue_affiliate_ledger_from_pool``, which
    always reconciles against an already-issued same-tier/same-user bundle
    BEFORE ever attempting a fresh claim (see
    ``_find_complete_issued_affiliate_bundle``) — so this never consumes
    additional inventory, and never touches a ledger carrying an
    abuse/risk-review flag (only ``_INVENTORY_ONLY_RISK_FLAGS`` values, or
    none at all, are eligible).
    """
    scanned = 0
    reconciled = 0
    candidates = list(
        db.affiliate_ledger.find(
            {"ledger_type": "AFFILIATE_MONTHLY", "status": "PENDING_MANUAL", **_no_voucher_filter()}
        )
    )
    # Also recover ledgers STRANDED in SETTLING: a worker that crashed
    # between claiming vouchers and finalizing the ledger (or one that
    # walked away because another worker held the allocation lease) leaves
    # the row mid-flight with no PENDING_MANUAL to find it by. Only rows
    # untouched for longer than the lease TTL are considered, so a worker
    # actively allocating right now is never interrupted; re-entry is safe
    # because allocation only ever claims what is still missing.
    settling_cutoff = now_utc - timedelta(seconds=_ALLOCATION_LEASE_TTL_SECONDS)
    candidates.extend(
        db.affiliate_ledger.find(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "status": SETTLING_STATUS,
                "updated_at": {"$lt": settling_cutoff},
                **_no_voucher_filter(),
            }
        )
    )
    candidates = candidates[: max(1, int(batch_limit))]
    for ledger in candidates:
        if _ledger_has_affiliate_bundle(ledger):
            continue
        flags = set(ledger.get("risk_flags") or [])
        if not flags <= _INVENTORY_ONLY_RISK_FLAGS:
            continue  # abuse/risk-review flag present — never auto-reconcile
        scanned += 1
        current_status = str(ledger.get("status") or "")
        settle_res = db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "status": current_status, **_no_voucher_filter()},
            {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
        )
        if settle_res.modified_count == 0:
            continue
        latest = _issue_affiliate_ledger_from_pool(
            db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc,
        )
        if latest and latest.get("status") == "ISSUED":
            reconciled += 1
    return {"scanned": scanned, "reconciled": reconciled}


def issue_current_month_affiliate_rewards(db, now_utc: datetime | None = None, batch_limit: int = 500):
    now_utc = now_utc or datetime.now(timezone.utc)
    start_utc, end_utc, yyyymm = _month_window_utc(now_utc)
    limit = max(1, int(batch_limit))
    summary = {
        "year_month": yyyymm,
        "scanned_users": 0,
        "eligible_users": 0,
        "created_ledgers": 0,
        "issued_count": 0,
        "skipped_existing": 0,
        "pending_manual": 0,
        "pool_empty": 0,
        "invalid_tier": 0,
        "errors": 0,
        "stuck_pending_manual_scanned": 0,
        "stuck_pending_manual_reconciled": 0,
    }

    pipeline = [
        {"$match": {"qualified_at": {"$gte": start_utc, "$lt": end_utc}, "referrer_id": {"$ne": None}}},
        {"$group": {"_id": "$referrer_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": T1_THRESHOLD}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": limit},
    ]
    rows = list(db.qualified_events.aggregate(pipeline))
    summary["scanned_users"] = int(len(rows))
    summary["eligible_users"] = int(len(rows))

    for row in rows:
        uid = int(row.get("_id"))
        qualified_count = int(row.get("count") or 0)
        expected_tiers = [
            tier_name for tier_name, threshold in (
                ("T1", T1_THRESHOLD),
                ("T2", T2_THRESHOLD),
                ("T3", T3_THRESHOLD),
                ("T4", T4_THRESHOLD),
                ("T5", T5_THRESHOLD),
            ) if qualified_count >= int(threshold)
        ]
        existing_before = list(
            db.affiliate_ledger.find(
                {
                    "ledger_type": "AFFILIATE_MONTHLY",
                    "user_id": uid,
                    "year_month": yyyymm,
                    "tier": {"$in": expected_tiers},
                },
                {"tier": 1, "status": 1},
            )
        )
        before_status_by_tier = {str(doc.get("tier")): str(doc.get("status") or "") for doc in existing_before}
        summary["skipped_existing"] += len(existing_before)
        try:
            evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now_utc)
        except Exception as exc:
            summary["errors"] += 1
            logger.exception(
                "[AFFILIATE][BULK_ISSUE_ERROR] user_id=%s year_month=%s err_class=%s err_msg=%s",
                uid,
                yyyymm,
                exc.__class__.__name__,
                str(exc),
                exc_info=True,
            )
            continue

        final_ledgers = list(
            db.affiliate_ledger.find(
                {
                    "ledger_type": "AFFILIATE_MONTHLY",
                    "user_id": uid,
                    "year_month": yyyymm,
                    "tier": {"$in": expected_tiers},
                }
            )
        )
        created_tiers = {str(doc.get("tier")) for doc in final_ledgers} - set(before_status_by_tier.keys())
        summary["created_ledgers"] += len(created_tiers)
        for ledger in final_ledgers:
            before_status = before_status_by_tier.get(str(ledger.get("tier")))
            status = str(ledger.get("status") or "")
            if status == "ISSUED" and before_status != "ISSUED":
                summary["issued_count"] += 1
            elif status == "PENDING_MANUAL" and before_status != "PENDING_MANUAL":
                summary["pending_manual"] += 1
                if "pool_empty" in (ledger.get("risk_flags") or []):
                    summary["pool_empty"] += 1
            if "missing_pool_config" in (ledger.get("risk_flags") or []):
                summary["invalid_tier"] += 1

    stuck_retry = _retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=now_utc, batch_limit=limit)
    summary["stuck_pending_manual_scanned"] = stuck_retry.get("scanned", 0)
    summary["stuck_pending_manual_reconciled"] = stuck_retry.get("reconciled", 0)

    logger.info("[AFFILIATE][BULK_ISSUE_SUMMARY] %s", summary)
    return summary


def settle_previous_month_affiliate_rewards(db, *, now_utc: datetime | None = None, batch_limit: int = 500):
    now_utc = now_utc or datetime.now(timezone.utc)
    start_utc, _, _ = _month_window_utc(now_utc)
    prev_ref = start_utc - timedelta(seconds=1)
    _, _, prev_yyyymm = _month_window_utc(prev_ref)

    logger.info("affiliate_monthly_settle start prev_yyyymm=%s batch_limit=%s", prev_yyyymm, int(batch_limit))

    stale_cutoff = now_utc - timedelta(minutes=15)
    processed = 0
    settled = 0
    while processed < int(batch_limit):
        ledger = db.affiliate_ledger.find_one_and_update(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "year_month": prev_yyyymm,
                "$or": [
                    {"status": "PENDING_EOM"},
                    {"status": "APPROVED", "updated_at": {"$lt": stale_cutoff}},
                    {"status": SETTLING_STATUS, "updated_at": {"$lt": stale_cutoff}},
                    {"status": "PENDING_MANUAL", "updated_at": {"$lt": stale_cutoff}},
                    {"status": "SIMULATED_PENDING", "updated_at": {"$lt": stale_cutoff}},
                ],
            },
            {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not ledger:
            break

        processed += 1
        latest_ledger = db.affiliate_ledger.find_one({"_id": ledger["_id"]})
        latest_ledger = _finalize_issued_if_voucher_exists(db, ledger=latest_ledger, now_utc=now_utc)
        if latest_ledger and (_ledger_has_affiliate_bundle(latest_ledger) or latest_ledger.get("voucher_code")):
            settled += 1
            continue
        reconciled = _reconcile_affiliate_bundle_from_issued_pool(db, ledger=latest_ledger or ledger, now_utc=now_utc)
        if reconciled and _ledger_has_affiliate_bundle(reconciled):
            settled += 1
            continue
        tier = ledger.get("tier")
        uid = int(ledger.get("user_id"))
        if not tier:
            db.affiliate_ledger.update_one(
                {"_id": ledger["_id"]},
                {"$set": {"status": "REJECTED", "review_reason": "no_tier", "updated_at": now_utc}},
            )
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "REJECTED")
            continue
        kl_dt = KL_TZ.localize(datetime(int(prev_yyyymm[:4]), int(prev_yyyymm[4:6]), 15, 12, 0, 0))
        m_start_utc, m_end_utc, _ = _month_window_utc(kl_dt.astimezone(timezone.utc))
        flags = _risk_flags_for_referrer_month(db, referrer_id=uid, start_utc=m_start_utc, end_utc=m_end_utc)
        merged_flags = _merge_monthly_risk_flags(ledger.get("risk_flags"), flags)
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]},
            {"$set": {"risk_flags": merged_flags, "updated_at": now_utc}},
        )

        issued_row = _issue_affiliate_ledger_from_pool(db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc)
        if issued_row and issued_row.get("status") == "ISSUED":
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "ISSUED")
        elif issued_row and issued_row.get("status") == "PENDING_MANUAL":
            logger.info("affiliate_monthly_settle uid=%s tier=%s status=%s", uid, tier, "PENDING_MANUAL")
        settled += 1

    logger.info("affiliate_monthly_settle end prev_yyyymm=%s processed=%s settled=%s", prev_yyyymm, processed, settled)
    return {"prev_yyyymm": prev_yyyymm, "processed": processed, "settled": settled}


def retry_current_month_pending_manual_ledgers(db, *, now_utc: datetime | None = None, batch_limit: int = 200):
    out = issue_current_month_affiliate_rewards(db, now_utc=now_utc, batch_limit=batch_limit)
    return {"yyyymm": out.get("year_month"), "processed": int(out.get("eligible_users", 0))}


def catch_up_missing_current_month_affiliate_ledgers(db, *, now_utc: datetime | None = None, batch_limit: int = 500):
    now_utc = now_utc or datetime.now(timezone.utc)
    start_utc, end_utc, yyyymm = _month_window_utc(now_utc)
    logger.info("[AFFILIATE][CATCHUP_START] year_month=%s batch_limit=%s", yyyymm, int(batch_limit))
    rows = list(
        db.qualified_events.aggregate(
            [
                {"$match": {"qualified_at": {"$gte": start_utc, "$lt": end_utc}, "referrer_id": {"$ne": None}}},
                {"$group": {"_id": "$referrer_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gte": T1_THRESHOLD}}},
                {"$sort": {"count": -1, "_id": 1}},
                {"$limit": max(1, int(batch_limit))},
            ]
        )
    )
    processed = 0
    for row in rows:
        uid = int(row.get("_id"))
        qcount = int(row.get("count") or 0)
        eligible_tier = _tier_for_count(qcount)
        logger.info("[AFFILIATE][CATCHUP_USER] user_id=%s year_month=%s qualified_count=%s eligible_tier=%s", uid, yyyymm, qcount, eligible_tier)
        evaluate_monthly_affiliate_reward(db, referrer_id=uid, now_utc=now_utc)
        processed += 1
    logger.info("[AFFILIATE][CATCHUP_DONE] year_month=%s processed=%s", yyyymm, processed)
    return {"year_month": yyyymm, "processed": processed}


def mark_invitee_qualified(db, *, invitee_id: int, referrer_id: int | None, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    last_seen = db.user_last_seen.find_one({"user_id": int(invitee_id)}) or {}
    doc = {
        "invitee_id": int(invitee_id),
        "referrer_id": int(referrer_id) if referrer_id is not None else None,
        "qualified_at": now_utc,
        "ip": last_seen.get("ip"),
        "subnet": last_seen.get("subnet"),
        "session": last_seen.get("session"),
    }
    try:
        db.qualified_events.insert_one(doc)
    except DuplicateKeyError:
        if referrer_id is not None:
            evaluate_monthly_affiliate_reward(db, referrer_id=int(referrer_id), now_utc=now_utc)
        return False
    try:
        from affiliate_leaderboard import emit_referral_flow_event
        emit_referral_flow_event(
            db,
            event="affiliate_qualified",
            referrer_id=int(referrer_id) if referrer_id is not None else None,
            invitee_id=int(invitee_id),
            ts_utc=now_utc,
            meta={},
            idempotency_key=f"rf|affiliate_qualified|{int(referrer_id) if referrer_id is not None else None}|{int(invitee_id)}|{now_utc.isoformat()}",
        )
    except Exception:
        logger.exception("affiliate_qualified_event_emit_failed invitee=%s referrer=%s", invitee_id, referrer_id)
    if referrer_id is not None:
        evaluate_monthly_affiliate_reward(db, referrer_id=int(referrer_id), now_utc=now_utc)
    return True


def approve_affiliate_ledger(db, *, ledger_id, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    ledger = db.affiliate_ledger.find_one_and_update(
        {"_id": ledger_id, "status": {"$in": ["PENDING_REVIEW", "PENDING_MANUAL", "APPROVED"]}},
        {"$set": {"status": "APPROVED", "updated_at": now_utc}},
        return_document=ReturnDocument.AFTER,
    )
    if not ledger:
        return None
    ledger = _finalize_issued_if_voucher_exists(db, ledger=ledger, now_utc=now_utc)
    if ledger.get("status") == "ISSUED":
        return ledger
    issue_claim = db.affiliate_ledger.update_one(
        {"_id": ledger["_id"], "status": {"$in": ["APPROVED", SETTLING_STATUS]}, **_no_voucher_filter()},
        {"$set": {"status": SETTLING_STATUS, "updated_at": now_utc}},
    )
    if issue_claim.modified_count == 0:
        latest = _finalize_issued_if_voucher_exists(db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc)
        if latest and not _ledger_has_affiliate_bundle(latest) and latest.get("status") != "SIMULATED_PENDING":
            latest = _reconcile_affiliate_bundle_from_issued_pool(db, ledger=latest, now_utc=now_utc) or latest
        return latest
    return _issue_affiliate_ledger_from_pool(db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc)


def affiliate_bundle_visible_cards(db, *, user_id: int) -> list[dict]:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return []
    rows = list(
        db.affiliate_ledger.find(
            {
                "user_id": uid,
                "status": "ISSUED",
                "reward_type": AFFILIATE_BUNDLE_REWARD_TYPE,
            }
        )
    )
    rows.sort(
        key=lambda row: (
            row.get("updated_at") or row.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
            str(row.get("_id") or ""),
        ),
        reverse=True,
    )
    cards = []
    for row in rows:
        tier = str(row.get("affiliate_tier") or row.get("tier") or "").strip().upper()
        vouchers = []
        for item in row.get("vouchers") or []:
            code = str((item or {}).get("code") or "").strip()
            if not code:
                continue
            vouchers.append({"value": int((item or {}).get("value") or 0), "code": code})
        if not tier or not vouchers:
            continue
        voucher_count = int(row.get("voucher_count") or len(vouchers))
        total_value = int(row.get("total_value") or sum(int(v.get("value") or 0) for v in vouchers))
        issued_at = row.get("updated_at") or row.get("created_at")
        issued_iso = issued_at.isoformat() if hasattr(issued_at, "isoformat") else None
        icon = AFFILIATE_TIER_ICONS.get(tier, "🎁")
        cards.append(
            {
                "dropId": f"affiliate:{row.get('_id')}",
                "type": AFFILIATE_BUNDLE_REWARD_TYPE,
                "reward_type": AFFILIATE_BUNDLE_REWARD_TYPE,
                "name": f"{icon} Affiliate Reward - {tier}",
                "affiliate_tier": tier,
                "tier_icon": icon,
                "voucher_count": voucher_count,
                "total_value": total_value,
                "currency": row.get("currency") or "$",
                "vouchers": vouchers,
                "isActive": True,
                "userClaimed": True,
                "state": "claimed",
                "claimable": False,
                "canClaim": False,
                "visible_remaining": None,
                "issued_at": issued_iso,
                "claimedAt": issued_iso,
            }
        )
    return cards


def reject_affiliate_ledger(db, *, ledger_id, reason: str | None = None, now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    db.affiliate_ledger.update_one(
        {"_id": ledger_id},
        {"$set": {"status": "REJECTED", "review_reason": reason, "updated_at": now_utc}},
    )
