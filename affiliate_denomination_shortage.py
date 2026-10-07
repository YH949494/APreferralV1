"""Pending Manual denomination shortage: aggregate, upload, bulk retry.

Why this exists
---------------
A denomination-plan (entitlement month >= 202609) affiliate bundle that cannot
be fully allocated parks in ``PENDING_MANUAL`` with the ``bundle_denomination_short``
risk flag. Clearing that used to take one "Replenish Historical Batch" modal
per ledger. This module answers the question the operator actually has -- "how
many $5 / $10 / $50 codes do I need to upload in total?" -- and then finishes
the job with one bulk retry.

Design rules (all inherited from the allocator, none re-implemented)
--------------------------------------------------------------------
* WHAT a tier owes is ``affiliate_rewards._ledger_recipe`` (the frozen
  ``bundle_recipe``, else ``affiliate_reward_plans.tier_recipe``). Nothing
  here restates a denomination table.
* WHAT a ledger already holds is ``_classify_issued_pool_rows``. The shortage
  of a ledger is its ``missing`` map -- not the full recipe -- so a ledger that
  already secured its $5 only asks for the $10s it is missing.
* WHERE a denomination may be drawn from is exactly the allocator's rule: the
  batch the ledger is pinned to under ``pool_targets.<POOL>``, else the one
  batch whose window fully contains the entitlement month. Stock in any other
  month's batch is never counted and never consumed (a September ledger is
  never satisfied from October stock).
* HOW a ledger is issued is ``_issue_affiliate_ledger_from_pool`` -- the same
  fenced-lease, all-or-nothing path the 5-minute retry sweep and the row-level
  Approve use. This module never claims a voucher row itself.

Demand vs issuability
---------------------
The summary keeps two questions apart. *Demand* -- what the pending ledgers
still owe, per denomination -- comes only from the recipe minus what each
ledger already holds, so it never depends on the state of any batch. A ledger
whose historical batch is disabled / not ready / scheduled / expired / missing
still owes exactly the same codes. *Issuability* -- whether a retry could
complete it right now -- is the batch gate (``_resolve_source``) plus stock.
A gate failure is reported under ``issuance_blockers`` and removes the ledger
from ``issuable_now`` / ``issuable_after_stock_replenishment``; it never
erases the ledger's ``required`` demand.

Nothing in the read path writes. The only writes are (a) code insertion into
the batch an entitlement month already owns and (b) the bulk retry, which
mutates a ledger only through the allocator.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from bson import ObjectId

import affiliate_rewards as ar
from affiliate_reward_plans import (
    ADMIN_AFFILIATE_POOL_IDS,
    DENOMINATION_PLAN_FIRST_MONTH,
    DENOMINATION_POOL_IDS,
    normalize_month,
    pool_denomination,
)

logger = logging.getLogger(__name__)

PENDING_MANUAL = "PENDING_MANUAL"
SHORTAGE_FLAG = "bundle_denomination_short"

#: Hard ceiling on ledgers examined in one request. The summary and the bulk
#: retry both stop here and say so (``scan_truncated``) instead of silently
#: under-reporting.
DEFAULT_SCAN_LIMIT = 5000

BULK_RETRY_LOCK_COLLECTION = "affiliate_bulk_retry_locks"
BULK_RETRY_AUDIT_COLLECTION = "affiliate_bulk_retry_audit"
_BULK_RETRY_LOCK_ID = "pending_manual_bulk_retry"
_BULK_RETRY_LOCK_TTL_SECONDS = 120

UPLOAD_SOURCE = "admin_pending_manual_upload"

# Evaluation kinds ----------------------------------------------------------
STOCK_SHORT = "stock_short"            # counted in required; fixed by uploading codes
BLOCKED = "blocked"                    # flagged short, but codes alone cannot fix it
RESERVED_COMPLETE = "reserved_complete"  # already holds the whole bundle; retry finalizes
EXCLUDED = "excluded"                  # not this workflow's business

_INVENTORY_FLAGS = ar._INVENTORY_ONLY_RISK_FLAGS


# --------------------------------------------------------------------------
# Evaluation (read-only)
# --------------------------------------------------------------------------

class _Cache:
    """Per-request memo so N ledgers sharing one batch cost one count."""

    def __init__(self):
        self.period_batches: dict = {}
        self.available: dict = {}
        self.batches: dict = {}


def _unissued_filter(batch_id, pool_id) -> dict:
    # The allocator's own claim predicate (``_claim_from_target_batch``): an
    # ``available`` row that is not already linked to a ledger. Issued,
    # reserved (issued_for_ledger_id set) and rolled-back-but-relinked rows
    # are therefore never counted.
    return {
        "batch_id": batch_id,
        "pool_id": pool_id,
        "status": "available",
        "$or": [
            {"issued_for_ledger_id": {"$exists": False}},
            {"issued_for_ledger_id": None},
        ],
    }


def _usable_filter(batch_id, pool_id, now_utc: datetime) -> dict:
    """``_unissued_filter`` AND the code's redemption validity -- character for
    character the predicate ``_claim_from_target_batch`` claims with, so the
    summary can never count a code the allocator would refuse."""
    base = _unissued_filter(batch_id, pool_id)
    return {
        "batch_id": base["batch_id"], "pool_id": base["pool_id"], "status": base["status"],
        "$and": [{"$or": base["$or"]}, ar._redemption_valid_clause(now_utc)],
    }


def _stock_in_batch(db, batch_id, pool_id, now_utc: datetime, cache: _Cache | None) -> dict:
    """``raw`` (unissued rows physically in the DB), ``usable`` (the subset the
    allocator may still issue) and ``expired`` (the rest; kept for audit,
    never counted as stock)."""
    key = (str(batch_id), pool_id)
    if cache is not None and key in cache.available:
        return cache.available[key]
    raw = int(db.voucher_pools.count_documents(_unissued_filter(batch_id, pool_id)))
    usable = int(db.voucher_pools.count_documents(_usable_filter(batch_id, pool_id, now_utc)))
    usable = min(usable, raw)
    stock = {"raw": raw, "usable": usable, "expired": raw - usable}
    if cache is not None:
        cache.available[key] = stock
    return stock


def _available_in_batch(db, batch_id, pool_id, now_utc: datetime, cache: _Cache | None) -> int:
    """USABLE unissued stock in one batch."""
    return _stock_in_batch(db, batch_id, pool_id, now_utc, cache)["usable"]


def _resolve_source(db, ledger: dict, pool_id: str, month: str, *, now_utc: datetime, cache: _Cache | None):
    """``(source_or_None, blocked_reason_or_None)`` for ONE denomination.

    Mirrors ``_resolve_denomination_pool_target`` (where the allocator would
    pin) and ``_claim_from_target_batch`` (every gate before the stock check),
    without writing. A reason here means uploading codes would NOT make the
    claim succeed, so such a ledger is reported as blocked, not as a shortage.

    ``source`` is the batch this ledger's codes must come from whenever that
    batch can be IDENTIFIED -- including when a gate then refuses it (disabled,
    not ready, scheduled, expired). That lets the caller still measure demand
    and compatible stock against the right batch. ``source`` is ``None`` only
    when no compatible batch can be named (none, ambiguous, wrong pool,
    legacy pin).
    """
    target = (ledger.get("pool_targets") or {}).get(pool_id) or {}
    mode = target.get("mode")
    pinned = mode == "batch" and target.get("batch_id") is not None
    if mode == "legacy":
        # The denomination resolver never writes a legacy pin; refuse to guess.
        return None, "unsupported_legacy_pin"

    if pinned:
        batch_key = str(target["batch_id"])
        if cache is not None and batch_key in cache.batches:
            batch = cache.batches[batch_key]
        else:
            batch = db.affiliate_voucher_batches.find_one({"_id": target["batch_id"]})
            if cache is not None:
                cache.batches[batch_key] = batch
        if not batch:
            return None, "no_batch_for_entitlement_period"
    else:
        key = (pool_id, month)
        if cache is not None and key in cache.period_batches:
            matches = cache.period_batches[key]
        else:
            start, end = ar._month_window_from_yyyymm(month)
            if start is None or end is None:
                return None, "no_batch_for_entitlement_period"
            matches = ar._find_batches_for_period(db, pool_id=pool_id, period_start_utc=start, period_end_utc=end)
            if cache is not None:
                cache.period_batches[key] = matches
        if not matches:
            return None, "no_batch_for_entitlement_period"
        if len(matches) > 1:
            return None, "target_batch_ambiguous"
        batch = matches[0]

    if str(batch.get("pool_id") or "").strip().upper() != pool_id:
        return None, "batch_pool_mismatch"

    starts_at = ar._as_aware_utc(batch.get("starts_at"))
    ends_at = ar._as_aware_utc(batch.get("ends_at"))
    source = {
        "batch_id": batch["_id"],
        "batch_name": batch.get("batch_name"),
        "pool_id": pool_id,
        "ends_at": ends_at,
        "historical": ends_at is not None and now_utc >= ends_at,
        "pinned": pinned,
    }
    # Gate order is the allocator's own (``_claim_from_target_batch``).
    if batch.get("upload_status") not in (None, "ready"):
        return source, "target_batch_not_ready"
    if bool(batch.get("distribution_disabled")):
        return source, "target_batch_disabled"
    if starts_at is None or ends_at is None:
        return source, "target_batch_not_ready"
    if now_utc < starts_at:
        return source, "target_batch_scheduled"
    # The allocator only claims past ends_at when continuing an already-pinned
    # allocation (or a retention-released entitlement). A never-pinned ledger
    # of an ended month cannot be completed by ANY upload.
    allow_expired = pinned or ledger.get("retention_completed_at") is not None
    if now_utc >= ends_at and not allow_expired:
        return source, "target_batch_expired_unissued"

    return source, None


def _evaluate(db, ledger: dict, *, now_utc: datetime, cache: _Cache | None, seen_keys: set) -> dict:
    """Classify one PENDING_MANUAL candidate. Pure reads."""
    ev = {
        "ledger": ledger,
        "kind": EXCLUDED,
        "reason": None,
        "missing": {},
        "sources": {},          # pools whose batch passes every gate (claimable)
        "demand_sources": {},   # every missing pool -> its compatible batch, gated or not
        "reward_value": 0,
        "partial": False,
    }

    if ar._ledger_has_affiliate_bundle(ledger) or ledger.get("voucher_code"):
        ev["reason"] = "already_issued"
        return ev
    if bool(ledger.get("simulate")):
        ev["reason"] = "simulated"
        return ev
    flags = set(ledger.get("risk_flags") or [])
    if SHORTAGE_FLAG not in flags or not flags <= _INVENTORY_FLAGS:
        # Abuse / risk-review / any other manual-review reason: not ours.
        ev["reason"] = "other_risk_flags"
        return ev
    if not ar._ledger_uses_denomination_plan(ledger):
        ev["reason"] = "not_denomination_plan"
        return ev

    month = ar._ledger_entitlement_month(ledger)
    tier = str(ledger.get("tier") or "").strip().upper()
    dedup = (ledger.get("user_id"), month, tier)
    if dedup in seen_keys:
        ev["reason"] = "duplicate"
        return ev
    other = db.affiliate_ledger.find_one(
        {
            "_id": {"$ne": ledger["_id"]},
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": ledger.get("user_id"),
            "year_month": ledger.get("year_month"),
            "tier": tier,
            "status": {"$in": ["ISSUED", "SETTLING", "APPROVED", "PENDING_EOM"]},
        },
        {"_id": 1},
    )
    if other:
        ev["reason"] = "duplicate"
        return ev
    seen_keys.add(dedup)

    recipe = ar._ledger_recipe(ledger)
    if not recipe:
        ev.update(kind=BLOCKED, reason="missing_recipe")
        return ev
    ev["reward_value"] = int(recipe.get("reward_value") or 0)
    ev["month"] = month

    state = ar._classify_issued_pool_rows(db, ledger, recipe=recipe)
    if state["foreign"] or state["surplus"]:
        ev.update(kind=BLOCKED, reason="integrity_conflict")
        return ev
    if state["complete"]:
        # Holds every code it owes; needs no stock, only finalization.
        ev.update(kind=RESERVED_COMPLETE, reason="reserved_complete")
        return ev

    ev["missing"] = {p: int(q) for p, q in state["missing"].items()}
    ev["partial"] = any(state["allocated"].values())

    reasons = {}
    for pool_id in ev["missing"]:
        source, why = _resolve_source(db, ledger, pool_id, month, now_utc=now_utc, cache=cache)
        ev["demand_sources"][pool_id] = source
        if why:
            reasons[pool_id] = why
        else:
            ev["sources"][pool_id] = source
    if reasons:
        ev.update(kind=BLOCKED, reason=sorted(set(reasons.values()))[0], pool_reasons=reasons)
        return ev
    ev["kind"] = STOCK_SHORT
    return ev


def _candidates(db, *, limit: int):
    query = {
        "ledger_type": "AFFILIATE_MONTHLY",
        "status": PENDING_MANUAL,
        "risk_flags": SHORTAGE_FLAG,
        **ar._no_voucher_filter(),
    }
    rows = list(db.affiliate_ledger.find(query, sort=[("created_at", 1), ("_id", 1)], limit=limit + 1))
    truncated = len(rows) > limit
    return rows[:limit], truncated


def _evaluate_all(db, *, now_utc: datetime, limit: int):
    rows, truncated = _candidates(db, limit=limit)
    cache = _Cache()
    seen: set = set()
    evals = [_evaluate(db, row, now_utc=now_utc, cache=cache, seen_keys=seen) for row in rows]
    return evals, cache, truncated


# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------

def _group_key(source: dict, pool_id: str):
    return (str(source["batch_id"]), pool_id)


def _rollup(groups: dict) -> dict:
    """Per-denomination totals from a ``{(batch, pool): group}`` map.

    Stock vocabulary (``raw_available == usable_total + expired_excluded``):

    * ``raw_available``    -- unissued ``available`` rows in the DB, expired or not.
    * ``expired_excluded`` -- the part of that whose redemption validity has lapsed.
    * ``available_total``  -- usable stock, unclamped.
    * ``available`` / ``usable_available`` -- usable stock CAPPED at what these
      ledgers need (``min(usable, required)`` per batch), so
      ``shortage == required - usable_available`` holds for the displayed totals.
    """
    out = {}
    for pool_id in DENOMINATION_POOL_IDS:
        out[str(pool_denomination(pool_id))] = {
            "pool_id": pool_id, "required": 0, "available": 0, "available_total": 0,
            "raw_available": 0, "usable_available": 0, "expired_excluded": 0, "shortage": 0,
        }
    for g in groups.values():
        g["shortage"] = max(g["required"] - g["available"], 0)
        usable = min(g["available"], g["required"])
        d = out[str(pool_denomination(g["pool_id"]))]
        d["required"] += g["required"]
        d["available"] += usable
        d["usable_available"] += usable
        d["available_total"] += g["available"]
        d["raw_available"] += g.get("raw_available", g["available"])
        d["expired_excluded"] += g.get("expired_excluded", 0)
        d["shortage"] += g["shortage"]
    return out


def _aggregate(evals, cache: _Cache, db, now_utc: datetime) -> dict:
    """Per-batch required/available, then rolled up per denomination.

    Required is summed per (batch, pool); available is that batch's own
    USABLE stock: unissued ``available`` rows whose ``redemption_expires_at``
    has not lapsed (the allocator's exact claim predicate). Expired rows are
    reported as ``expired_excluded`` -- they stay in the DB for audit and never
    reduce the shortage. Shortage is computed PER BATCH and then summed, so surplus
    sitting in one month's batch can never be netted against another month's
    deficit. The roll-up's ``available`` is the stock actually USABLE by these
    ledgers (``min(available, required)`` per batch), which keeps
    ``shortage == required - available`` true for the displayed totals.

    Two views over the same ledgers:

    * ``groups`` -- ACTIONABLE only (``STOCK_SHORT``: every gate passes, stock
      is the sole obstacle). Drives ``issuable_now`` and the cap on uploads to
      an ended batch, exactly as before.
    * ``demand_groups`` -- every ledger that still owes codes, whether or not
      its batch is currently claimable. This is what ``denominations[*]
      .required / .available / .shortage`` report, so a disabled / scheduled /
      expired / missing batch can never zero the demand. ``available`` there is
      the unissued stock of the batch the ledger is bound to (a disabled
      batch keeps its ``available`` rows); a ledger with NO identifiable batch
      sees none. Stock in any other batch is never counted.
    """
    groups: dict = {}
    demand_groups: dict = {}
    for ev in evals:
        if ev["kind"] not in (STOCK_SHORT, BLOCKED) or not ev["missing"]:
            continue
        actionable = ev["kind"] == STOCK_SHORT
        reasons = ev.get("pool_reasons") or {}
        for pool_id, qty in ev["missing"].items():
            src = ev["demand_sources"].get(pool_id)
            if src is not None:
                key = _group_key(src, pool_id)
                stock = _stock_in_batch(db, src["batch_id"], pool_id, now_utc, cache)
                base = {
                    "batch_id": str(src["batch_id"]),
                    "batch_name": src.get("batch_name"),
                    "historical": bool(src["historical"]),
                    "available": stock["usable"],
                    "raw_available": stock["raw"],
                    "expired_excluded": stock["expired"],
                }
            else:
                # No compatible batch can be named: demand stands, stock is zero.
                key = (f"unresolved:{ev['month']}", pool_id)
                base = {"batch_id": None, "batch_name": None, "historical": False,
                        "available": 0, "raw_available": 0, "expired_excluded": 0}
            g = demand_groups.setdefault(key, {
                "pool_id": pool_id, "entitlement_month": ev["month"], "required": 0, "blockers": {}, **base,
            })
            g["required"] += int(qty)
            if pool_id in reasons:
                g["blockers"][reasons[pool_id]] = g["blockers"].get(reasons[pool_id], 0) + int(qty)
            if actionable:
                a = groups.setdefault(key, {
                    "pool_id": pool_id, "entitlement_month": ev["month"], "required": 0, **base,
                })
                a["required"] += int(qty)

    denominations = _rollup(demand_groups)
    uploadable = _rollup(groups)
    by_month: dict = {}
    for key, g in demand_groups.items():
        denom = str(pool_denomination(g["pool_id"]))
        d = denominations[denom]
        d["available_compatible"] = d["available"]
        entry = by_month.setdefault(g["entitlement_month"], {}).setdefault(denom, {
            "pool_id": g["pool_id"], "batch_id": g["batch_id"], "batch_name": g["batch_name"],
            "historical": g["historical"], "required": 0, "available": 0, "usable_available": 0,
            "raw_available": 0, "expired_excluded": 0, "shortage": 0,
            "uploadable_shortage": 0, "blockers": {},
        })
        entry["required"] += g["required"]
        entry["available"] += min(g["available"], g["required"])
        entry["usable_available"] = entry["available"]
        entry["raw_available"] += g["raw_available"]
        entry["expired_excluded"] += g["expired_excluded"]
        entry["shortage"] += g["shortage"]
        entry["uploadable_shortage"] += groups[key]["shortage"] if key in groups else 0
        for reason, qty in g["blockers"].items():
            entry["blockers"][reason] = entry["blockers"].get(reason, 0) + qty
    for denom, d in denominations.items():
        d.setdefault("available_compatible", d["available"])
        d["uploadable_shortage"] = uploadable[denom]["shortage"]
    return {
        "denominations": denominations,
        "by_month": {m: by_month[m] for m in sorted(by_month)},
        "groups": groups,
        "demand_groups": demand_groups,
    }


def _plan_issuable_now(evals, groups) -> int:
    """FIFO dry run of the bulk retry against current stock: how many
    STOCK_SHORT ledgers could be completed right now. Pure arithmetic."""
    budget = {k: g["available"] for k, g in groups.items()}
    issuable = 0
    for ev in evals:
        if ev["kind"] != STOCK_SHORT:
            continue
        needs = {_group_key(ev["sources"][p], p): q for p, q in ev["missing"].items()}
        if all(budget.get(k, 0) >= q for k, q in needs.items()):
            for k, q in needs.items():
                budget[k] -= q
            issuable += 1
    return issuable


def summarize_pending_manual_shortage(db, *, now_utc: datetime | None = None, limit: int = DEFAULT_SCAN_LIMIT) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    evals, cache, truncated = _evaluate_all(db, now_utc=now_utc, limit=limit)
    agg = _aggregate(evals, cache, db, now_utc)

    pending = [e for e in evals if e["kind"] in (STOCK_SHORT, BLOCKED)]
    short = [e for e in pending if e["kind"] == STOCK_SHORT]
    blocked = [e for e in pending if e["kind"] == BLOCKED]
    blocked_breakdown: dict = {}
    for e in blocked:
        blocked_breakdown[e["reason"]] = blocked_breakdown.get(e["reason"], 0) + 1
    excluded: dict = {}
    for e in evals:
        if e["kind"] in (EXCLUDED, RESERVED_COMPLETE):
            excluded[e["reason"]] = excluded.get(e["reason"], 0) + 1

    return {
        "status": "ok",
        "pending_count": len(pending),
        "total_reward_value": sum(e["reward_value"] for e in pending),
        "denominations": agg["denominations"],
        "by_month": agg["by_month"],
        # Ledgers that WOULD complete once their denomination stock is added,
        # i.e. every gate already passes. A ledger held back by a batch gate is
        # not counted here; it is listed under ``issuance_blockers``.
        "issuable_after_stock_replenishment": len(short),
        "issuable_after_replenishment": len(short),  # legacy alias (UI / existing callers)
        "issuable_now": _plan_issuable_now(evals, agg["groups"]),
        "still_blocked": len(blocked),
        "issuance_blockers": blocked_breakdown,
        "blocked_breakdown": blocked_breakdown,       # legacy alias
        "excluded": excluded,
        "partially_reserved_ledgers": sum(1 for e in short if e["partial"]),
        "scanned": len(evals),
        "scan_truncated": bool(truncated),
        "generated_at": now_utc.isoformat(),
    }


# --------------------------------------------------------------------------
# Upload by denomination
# --------------------------------------------------------------------------

def _fail(reason: str, message: str, **extra) -> dict:
    out = {"status": "error", "reason": reason, "message": message}
    out.update(extra)
    return out


def _resolve_pool(pool_id=None, denomination=None):
    from affiliate_voucher_batches import _resolve_denomination_pool_id

    return _resolve_denomination_pool_id(pool_id=pool_id, denomination=denomination)


def upload_codes_for_denomination(
    db, *, admin_identity: str, codes, entitlement_month, pool_id=None, denomination=None,
    now_utc: datetime | None = None,
) -> dict:
    """Insert codes for ONE denomination into the batch that owns
    ``entitlement_month``. Never issues, never moves a window.

    * Current/future month  -> the normal ``add_codes_to_batch`` flow.
    * Ended month           -> the historical insert (same row shape as
      ``replenish_historical_pinned_batch``), capped at the current shortage
      of that exact batch so an expired batch is never over-filled.
    """
    import affiliate_voucher_batches as av

    now_utc = now_utc or datetime.now(timezone.utc)
    target_pool = _resolve_pool(pool_id, denomination)
    if target_pool is None:
        return _fail("invalid_denomination", "A valid denomination ($5 / $10 / $50) is required.")
    month = normalize_month(entitlement_month)
    if month is None or month < DENOMINATION_PLAN_FIRST_MONTH:
        return _fail("invalid_entitlement_month", "A denomination-plan entitlement month (YYYYMM, 202609 or later) is required.")

    unique_codes, dup_in_upload, invalid = av.normalize_voucher_codes(codes)
    if not unique_codes:
        return _fail("empty_codes", "No valid voucher codes were provided.", invalid=invalid, duplicates=dup_in_upload)

    start, end = ar._month_window_from_yyyymm(month)
    matches = ar._find_batches_for_period(db, pool_id=target_pool, period_start_utc=start, period_end_utc=end)
    if not matches:
        return _fail("no_batch_for_entitlement_period", f"No {target_pool} batch exists for {month}. Create the batch first.")
    if len(matches) > 1:
        return _fail("target_batch_ambiguous", f"More than one {target_pool} batch covers {month}; resolve that first.")
    batch = matches[0]
    if bool(batch.get("distribution_disabled")):
        return _fail("batch_disabled", "The batch for this month is disabled.")
    if (batch.get("upload_status") or "ready") != "ready":
        return _fail("batch_not_ready", "The batch for this month is not in a ready state.")

    holder = str(ObjectId())
    lock_key = str(batch["_id"])
    if not av._acquire_replenish_lock(db, key=lock_key, holder=holder):
        return _fail("replenish_in_progress", "Another upload to this batch is in progress. Try again shortly.")
    try:
        # The same physical code anywhere in the affiliate pools (any pool,
        # any status) is refused: the unique (pool_id, code) index alone would
        # not catch a code pasted into the wrong denomination bucket.
        existing = list(db.voucher_pools.find(
            {"pool_id": {"$in": list(ADMIN_AFFILIATE_POOL_IDS)}, "code": {"$in": unique_codes}},
            projection={"code": 1, "status": 1, "pool_id": 1},
        ))
        existing_codes = {r.get("code") for r in existing}
        already_issued = len({r.get("code") for r in existing if r.get("status") == "issued"})
        wrong_bucket = len({r.get("code") for r in existing if r.get("pool_id") != target_pool})
        new_codes = [c for c in unique_codes if c not in existing_codes]
        duplicates = dup_in_upload + len(existing_codes)
        counts = {
            "submitted": len(unique_codes) + dup_in_upload + invalid,
            "invalid": invalid,
            "duplicates": duplicates,
            "already_issued": already_issued,
            "wrong_bucket": wrong_bucket,
        }
        if not new_codes:
            return _fail("duplicate_code", "No new codes: every submitted code already exists in the affiliate voucher pools.",
                         inserted=0, **counts)

        historical = now_utc >= ar._as_aware_utc(batch["ends_at"])
        if historical:
            shortage = _batch_pool_shortage(db, batch_id=batch["_id"], pool_id=target_pool, now_utc=now_utc)
            if len(new_codes) > shortage:
                return _fail(
                    "quantity_exceeds_shortage",
                    f"This ended batch is short {shortage} {target_pool} code(s); {len(new_codes)} new code(s) were submitted. Nothing was inserted.",
                    replenishable=shortage, inserted=0, **counts,
                )
            inserted, db_error = _insert_historical(db, batch=batch, pool_id=target_pool, codes=new_codes,
                                                    admin_identity=admin_identity, now_utc=now_utc)
            if db_error:
                # Keep the partial count so the operator can see what landed;
                # never report a short upload as a success.
                return _fail(
                    "database_error",
                    "A database error stopped the upload partway. Codes inserted before it remain in the batch; "
                    "re-submit the remaining codes.",
                    inserted=inserted, **counts,
                )
        else:
            res = av.add_codes_to_batch(db, batch["_id"], admin_identity=admin_identity, codes=new_codes, now_utc=now_utc)
            if not res.get("ok") and not res.get("inserted_count"):
                return _fail(res.get("code") or "upload_failed", res.get("message") or "Upload failed.", inserted=0, **counts)
            inserted = int(res.get("inserted_count") or 0)
            counts["duplicates"] += int(res.get("duplicate_count") or 0)

        logger.info(
            "[AFF_PM_SHORTAGE][UPLOAD] source=%s admin=%s pool_id=%s month=%s batch_id=%s historical=%s "
            "submitted=%s inserted=%s duplicates=%s invalid=%s wrong_bucket=%s",
            UPLOAD_SOURCE, admin_identity, target_pool, month, batch["_id"], historical,
            counts["submitted"], inserted, counts["duplicates"], invalid, wrong_bucket,
        )
        return {
            "status": "ok",
            "pool_id": target_pool,
            "denomination": pool_denomination(target_pool),
            "entitlement_month": month,
            "batch_id": str(batch["_id"]),
            "historical": historical,
            "inserted": inserted,
            **counts,
        }
    finally:
        av._release_replenish_lock(db, key=lock_key, holder=holder)


def _batch_pool_shortage(db, *, batch_id, pool_id: str, now_utc: datetime) -> int:
    evals, cache, _ = _evaluate_all(db, now_utc=now_utc, limit=DEFAULT_SCAN_LIMIT)
    agg = _aggregate(evals, cache, db, now_utc)
    group = agg["groups"].get((str(batch_id), pool_id))
    return int(group["shortage"]) if group else 0


def _insert_historical(db, *, batch: dict, pool_id: str, codes, admin_identity: str, now_utc: datetime):
    """Returns ``(inserted, error_class_name_or_None)``."""
    import affiliate_voucher_batches as av

    value = pool_denomination(pool_id)
    inserted = 0
    db_error = None
    for code in codes:
        row = {
            "pool_id": pool_id,
            "code": code,
            "batch_id": batch["_id"],
            "batch_name": batch.get("batch_name"),
            "starts_at": batch.get("starts_at"),
            "ends_at": batch.get("ends_at"),
            "status": "available",
            "created_at": now_utc,
            "distribution_disabled": False,
            "upload_source": UPLOAD_SOURCE,
        }
        if value is not None:
            row["voucher_value"] = value
        try:
            db.voucher_pools.insert_one(row)
            inserted += 1
        except Exception as exc:
            if av._is_duplicate_key_error(exc):
                continue
            db_error = exc.__class__.__name__
            logger.error("[AFF_PM_SHORTAGE][UPLOAD_FAILED] batch_id=%s pool_id=%s inserted_so_far=%s err=%s",
                         batch["_id"], pool_id, inserted, db_error)
            break
    live = av._hydrate_live_counts(db, batch)
    db.affiliate_voucher_batches.update_one(
        {"_id": batch["_id"]},
        {"$set": {
            "available_count": int(live["available_count"]),
            "issued_count": int(live["issued_count"]),
            "uploaded_count": int(live["available_count"]) + int(live["issued_count"]),
            "last_historical_replenish_at": now_utc,
        }, "$inc": {"inserted_count": inserted}},
    )
    return inserted, db_error


# --------------------------------------------------------------------------
# Bulk retry
# --------------------------------------------------------------------------

def _acquire_bulk_lock(db, holder: str) -> bool:
    locks = db[BULK_RETRY_LOCK_COLLECTION]
    wall = datetime.now(timezone.utc)
    cutoff = datetime.fromtimestamp(wall.timestamp() - _BULK_RETRY_LOCK_TTL_SECONDS, tz=timezone.utc)
    locks.delete_one({"_id": _BULK_RETRY_LOCK_ID, "locked_at": {"$lt": cutoff}})
    try:
        locks.insert_one({"_id": _BULK_RETRY_LOCK_ID, "holder": holder, "locked_at": wall})
    except Exception as exc:
        import affiliate_voucher_batches as av

        if av._is_duplicate_key_error(exc):
            return False
        raise
    return True


def _renew_bulk_lock(db, holder: str) -> bool:
    res = db[BULK_RETRY_LOCK_COLLECTION].update_one(
        {"_id": _BULK_RETRY_LOCK_ID, "holder": holder},
        {"$set": {"locked_at": datetime.now(timezone.utc)}},
    )
    return getattr(res, "matched_count", 0) == 1


def _release_bulk_lock(db, holder: str):
    try:
        db[BULK_RETRY_LOCK_COLLECTION].delete_one({"_id": _BULK_RETRY_LOCK_ID, "holder": holder})
    except Exception:
        logger.exception("[AFF_PM_SHORTAGE][LOCK_RELEASE_FAILED]")


def _fits_live_stock(db, ev: dict, now_utc: datetime) -> bool:
    # Fresh counts (no cache): earlier iterations of this same run, the
    # 5-minute sweep and other admins all consume stock between ledgers.
    for pool_id, qty in ev["missing"].items():
        src = ev["sources"][pool_id]
        if _available_in_batch(db, src["batch_id"], pool_id, now_utc, None) < qty:
            return False
    return True


def _attempt_issue(db, ledger_id, *, now_utc: datetime) -> str:
    """Drive one ledger through the allocator. Returns an outcome label."""
    claim = db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": PENDING_MANUAL, **ar._no_voucher_filter()},
        {"$set": {"status": ar.SETTLING_STATUS, "updated_at": now_utc}},
    )
    if getattr(claim, "modified_count", 0) == 0:
        # Another worker (sweep / second admin / Approve click) got it first.
        latest = db.affiliate_ledger.find_one({"_id": ledger_id}) or {}
        if latest.get("status") == "ISSUED" or ar._ledger_has_affiliate_bundle(latest):
            return "already_issued"
        return "in_progress"
    latest = ar._issue_affiliate_ledger_from_pool(
        db, ledger=db.affiliate_ledger.find_one({"_id": ledger_id}), now_utc=now_utc,
    )
    status = str((latest or {}).get("status") or "")
    if status == "ISSUED":
        return "issued"
    if status == PENDING_MANUAL:
        return "still_short"
    if status == ar.SETTLING_STATUS:
        return "in_progress"
    if status == "REJECTED":
        return "rejected"
    return "other"


def retry_all_eligible_pending(
    db, *, admin_identity: str = "system", now_utc: datetime | None = None, limit: int = DEFAULT_SCAN_LIMIT,
) -> dict:
    """Re-run every stock-blocked PENDING_MANUAL denomination ledger against
    current stock, oldest first.

    All-or-nothing per ledger, enforced twice:
      1. PRE-FLIGHT -- a ledger is only handed to the allocator when live
         stock covers EVERY denomination it is still missing. A ledger that
         does not fit is left completely untouched, so a short upload cannot
         scatter its codes across many ledgers as unusable partial bundles.
      2. The allocator itself only ever marks ISSUED on a validated, complete
         bundle under a fenced lease (``_issue_denomination_bundle``).

    Concurrency: one bulk run at a time (lock), a status-conditional
    PENDING_MANUAL -> SETTLING transition per ledger, and the allocator's
    fenced lease underneath. A ledger the sweep or another admin already
    issued is counted ``already_issued`` and never touched.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    limit = max(1, min(int(limit), DEFAULT_SCAN_LIMIT))
    holder = str(ObjectId())
    if not _acquire_bulk_lock(db, holder):
        return _fail("retry_in_progress", "Another bulk retry is already running. Try again shortly.")

    out = {
        "status": "ok", "scanned": 0, "issued": 0, "still_short": 0, "already_issued": 0, "errors": 0,
        "blocked": 0, "skipped": 0, "in_progress": 0, "rejected": 0, "aborted": False,
    }
    try:
        rows, truncated = _candidates(db, limit=limit)
        out["scan_truncated"] = bool(truncated)
        seen: set = set()
        for row in rows:
            out["scanned"] += 1
            if not _renew_bulk_lock(db, holder):
                out["aborted"] = True
                logger.error("[AFF_PM_SHORTAGE][RETRY_LOCK_LOST] scanned=%s", out["scanned"])
                break
            ledger_id = row.get("_id")
            try:
                # Re-read: the row may have moved since the candidate query.
                fresh = db.affiliate_ledger.find_one({"_id": ledger_id}) or row
                if fresh.get("status") == "ISSUED" or ar._ledger_has_affiliate_bundle(fresh):
                    out["already_issued"] += 1
                    continue
                ev = _evaluate(db, fresh, now_utc=now_utc, cache=None, seen_keys=seen)
                if ev["kind"] == EXCLUDED:
                    out["already_issued" if ev["reason"] == "already_issued" else "skipped"] += 1
                    continue
                if ev["kind"] == BLOCKED:
                    out["blocked"] += 1
                    continue
                if ev["kind"] == STOCK_SHORT and not _fits_live_stock(db, ev, now_utc):
                    out["still_short"] += 1
                    continue
                outcome = _attempt_issue(db, ledger_id, now_utc=now_utc)
                key = outcome if outcome in out else "errors"
                out[key] += 1
            except Exception:
                out["errors"] += 1
                logger.exception("[AFF_PM_SHORTAGE][RETRY_LEDGER_FAILED] ledger_id=%s", ledger_id)
    finally:
        _release_bulk_lock(db, holder)

    try:
        db[BULK_RETRY_AUDIT_COLLECTION].insert_one({
            "admin_identity": admin_identity,
            "created_at": now_utc,
            **{k: v for k, v in out.items() if k != "status"},
        })
    except Exception:
        logger.exception("[AFF_PM_SHORTAGE][AUDIT_WRITE_FAILED]")
    logger.info(
        "[AFF_PM_SHORTAGE][RETRY_ALL] admin=%s scanned=%s issued=%s still_short=%s already_issued=%s "
        "blocked=%s skipped=%s in_progress=%s errors=%s aborted=%s",
        admin_identity, out["scanned"], out["issued"], out["still_short"], out["already_issued"],
        out["blocked"], out["skipped"], out["in_progress"], out["errors"], out["aborted"],
    )
    return out
