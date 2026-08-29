#!/usr/bin/env python3
"""READ-ONLY verification of the September 2026 affiliate reward migration.

Writes NOTHING, ever — there is no commit/apply mode, by design. This is the
pre-deployment preflight and the post-deployment validation for the
denomination-plan rollout.

It does not go through ``database.init_db()``, because that calls
``ensure_indexes()`` and would create indexes on the database it is only
meant to inspect. It opens its own connection with a secondary-preferred
read preference instead (see ``_read_only_db``).

No data backfill is required by that rollout: every new ledger field
(``entitlement_month`` / ``reward_plan`` / ``bundle_recipe`` /
``expected_code_count`` / ``reward_value``) is written at ledger creation and
read LAZILY for historical rows (``entitlement_month`` falls back to
``year_month``; an absent ``reward_plan`` resolves to the legacy plan). So
this script exists to CONFIRM that, not to change anything.

Checks
------
1. plan-config          the two plans match the confirmed business rules,
                        including the $625 / 1x$5 + 7x$10 + 11x$50 total.
2. inventory            denomination pool stock and batch coverage for the
                        target entitlement month.
3. ledger-integrity     no user has more than one ledger per (month, tier);
                        no ISSUED ledger disagrees with its own recipe;
                        no voucher code is allocated to two ledgers.
4. plan-assignment      no ledger resolves to a plan its entitlement month
                        does not imply (historical-leakage detector).

Usage
-----
    python scripts/verify_affiliate_reward_plan.py --month 202609
    python scripts/verify_affiliate_reward_plan.py --month 202608 --check ledger-integrity

Exit code 0 = all selected checks passed, 1 = at least one finding.
"""
from __future__ import annotations

import argparse
import collections
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from affiliate_reward_plans import (  # noqa: E402
    DENOMINATION_PLAN_ID,
    DENOMINATION_POOL_IDS,
    LEGACY_PLAN_ID,
    pool_denomination,
    recipe_required_by_pool,
    resolve_plan_id,
    tier_recipe,
)
from affiliate_rewards import _as_aware_utc  # noqa: E402

TIERS = ("T1", "T2", "T3", "T4", "T5")

EXPECTED_DENOMINATION = {
    "T1": ({"AFFILIATE_10": 1}, 10),
    "T2": ({"AFFILIATE_5": 1, "AFFILIATE_10": 2}, 25),
    "T3": ({"AFFILIATE_10": 1, "AFFILIATE_50": 1}, 60),
    "T4": ({"AFFILIATE_10": 3, "AFFILIATE_50": 3}, 180),
    "T5": ({"AFFILIATE_50": 7}, 350),
}
EXPECTED_LEGACY = {
    "T1": ({"T1": 2}, 10), "T2": ({"T2": 3}, 15), "T3": ({"T3": 5}, 50),
    "T4": ({"T4": 3}, 150), "T5": ({"T5": 5}, 250),
}


class Findings:
    def __init__(self) -> None:
        self.items: list[str] = []

    def add(self, msg: str) -> None:
        self.items.append(msg)
        print(f"  [FINDING] {msg}")

    def ok(self, msg: str) -> None:
        print(f"  [ok] {msg}")


def check_plan_config(f: Findings) -> None:
    print("\n== plan-config ==")
    for label, month, table in (
        ("legacy", "202608", EXPECTED_LEGACY),
        ("denomination", "202609", EXPECTED_DENOMINATION),
    ):
        for tier, (pools, value) in table.items():
            recipe = tier_recipe(month, tier)
            actual_pools = recipe_required_by_pool(recipe)
            if actual_pools != pools or int(recipe["reward_value"]) != value:
                f.add(
                    f"{label} {tier}: expected {pools} = ${value}, "
                    f"got {actual_pools} = ${recipe['reward_value']}"
                )
    total = sum(v for _, v in EXPECTED_DENOMINATION.values())
    actual_total = sum(tier_recipe("202609", t)["reward_value"] for t in TIERS)
    if actual_total != total == 625:
        f.add(f"denomination plan T1-T5 total is ${actual_total}, expected $625")
    else:
        f.ok(f"denomination plan T1-T5 total = ${actual_total}")

    consumption: collections.Counter = collections.Counter()
    for tier in TIERS:
        consumption.update(recipe_required_by_pool(tier_recipe("202609", tier)))
    expected_consumption = {"AFFILIATE_5": 1, "AFFILIATE_10": 7, "AFFILIATE_50": 11}
    if dict(consumption) != expected_consumption:
        f.add(f"denomination consumption {dict(consumption)} != {expected_consumption}")
    else:
        f.ok(f"denomination consumption = {dict(consumption)} (19 codes)")

    if resolve_plan_id("202608") != LEGACY_PLAN_ID:
        f.add("202608 does not resolve to the legacy plan")
    if resolve_plan_id("202609") != DENOMINATION_PLAN_ID:
        f.add("202609 does not resolve to the denomination plan")
    f.ok("plan boundary: 202608 -> legacy, 202609 -> denomination")


def check_inventory(db, f: Findings, month: str, expected_demand: dict | None = None) -> None:
    """STRICT preflight for one entitlement month.

    Fails (exit non-zero) on missing batch coverage, ambiguous coverage, a
    non-canonical batch boundary, or insufficient stock for the expected
    demand. Only rows belonging to the matched batch are counted — undated
    legacy stock is deliberately NOT counted toward a scheduled month,
    because the runtime resolver refuses to issue September denomination
    rewards from it (see _resolve_denomination_pool_target).
    """
    print(f"\n== inventory (entitlement month {month}) ==")
    from affiliate_rewards import _month_window_from_yyyymm, _find_batches_for_period

    start_utc, end_utc = _month_window_from_yyyymm(month)
    if start_utc is None:
        f.add(f"invalid month {month!r}")
        return
    print(f"  canonical window: {start_utc.isoformat()} -> {end_utc.isoformat()} (UTC)")

    plan_id = resolve_plan_id(month)
    pools = DENOMINATION_POOL_IDS if plan_id == DENOMINATION_PLAN_ID else TIERS

    # ---- required inventory from expected demand ------------------------
    required_by_pool: dict[str, int] = {}
    if expected_demand:
        for tier, count in expected_demand.items():
            recipe = tier_recipe(month, tier)
            if not recipe:
                f.add(f"unknown tier {tier!r} in expected demand")
                continue
            for pool_id, qty in recipe_required_by_pool(recipe).items():
                required_by_pool[pool_id] = required_by_pool.get(pool_id, 0) + qty * int(count)
        print(f"  expected demand: {expected_demand}")
        print(f"  required inventory: {required_by_pool or '(none)'}")

    for pool_id in pools:
        batches = _find_batches_for_period(
            db, pool_id=pool_id, period_start_utc=start_utc, period_end_utc=end_utc
        )
        undated = int(db.voucher_pools.count_documents(
            {"pool_id": pool_id, "status": "available", "batch_id": {"$exists": False}}
        ))

        if not batches:
            f.add(
                f"{pool_id}: NO scheduled batch covers {month} "
                f"(undated/legacy available={undated} — NOT usable for this month)"
            )
            continue
        if len(batches) > 1:
            f.add(
                f"{pool_id}: {len(batches)} batches cover {month} (ambiguous) — "
                f"batch_ids={[str(b.get('_id')) for b in batches]}"
            )
            continue

        batch = batches[0]
        b_start = _as_aware_utc(batch.get("starts_at"))
        b_end = _as_aware_utc(batch.get("ends_at"))
        if b_start != start_utc or b_end != end_utc:
            f.add(
                f"{pool_id}: batch window {b_start} -> {b_end} is not the canonical "
                f"KL month window {start_utc} -> {end_utc}"
            )
            continue
        if batch.get("upload_status") not in (None, "ready"):
            f.add(f"{pool_id}: batch upload_status={batch.get('upload_status')!r} is not ready")
            continue
        if bool(batch.get("distribution_disabled")):
            f.add(f"{pool_id}: batch distribution is disabled")
            continue

        # Only rows on THIS batch count.
        available = int(db.voucher_pools.count_documents(
            {"batch_id": batch.get("_id"), "status": "available"}
        ))
        required = int(required_by_pool.get(pool_id, 0))
        shortage = max(0, required - available)
        line = (
            f"{pool_id}: batch ok, available={available}"
            + (f" required={required} shortage={shortage}" if expected_demand else "")
            + (f" (undated legacy={undated}, not counted)" if undated else "")
        )
        if expected_demand and shortage:
            f.add(f"{line} — INSUFFICIENT")
        elif available == 0:
            f.add(f"{line} — batch has no available stock")
        else:
            f.ok(line)


def _linked_issued_rows(db, ledger_id) -> list[dict]:
    """Every issued voucher_pools row linked to a ledger through EITHER
    link field: ``issued_for_ledger_id`` (string, written by every current
    claim path) or ``ledger_id`` (ObjectId, present on older rows). Querying
    only one of them silently under-reports surplus."""
    return list(
        db.voucher_pools.find(
            {
                "status": "issued",
                "$or": [
                    {"issued_for_ledger_id": str(ledger_id)},
                    {"ledger_id": ledger_id},
                ],
            }
        )
    )


def check_ledger_integrity(db, f: Findings, month: str | None) -> None:
    print(f"\n== ledger-integrity ({month or 'all months'}) ==")
    query: dict = {"ledger_type": "AFFILIATE_MONTHLY"}
    if month:
        query["$or"] = [{"entitlement_month": month}, {"year_month": month}]
    ledgers = list(db.affiliate_ledger.find(query))
    print(f"  scanned {len(ledgers)} AFFILIATE_MONTHLY ledgers")

    seen: collections.Counter = collections.Counter()
    for lg in ledgers:
        key = (lg.get("user_id"), lg.get("entitlement_month") or lg.get("year_month"), lg.get("tier"))
        seen[key] += 1
    for key, count in seen.items():
        if count > 1:
            f.add(f"duplicate ledgers for user/month/tier {key}: {count}")
    if not any(c > 1 for c in seen.values()):
        f.ok("no duplicate (user, month, tier) ledgers")

    per_user_month: collections.Counter = collections.Counter()
    for (user_id, ym, _tier) in seen:
        per_user_month[(user_id, ym)] += 1
    over = {k: v for k, v in per_user_month.items() if v > 5}
    if over:
        f.add(f"users with more than 5 tier ledgers in one month: {over}")
    else:
        f.ok("no user exceeds 5 tier ledgers in an entitlement month")

    count_mismatch = value_mismatch = surplus_found = denom_wrong = stamp_wrong = 0
    for lg in ledgers:
        ledger_id = lg.get("_id")
        recipe = lg.get("bundle_recipe") or tier_recipe(
            lg.get("entitlement_month") or lg.get("year_month"), lg.get("tier")
        )
        if not recipe:
            continue
        required = recipe_required_by_pool(recipe)
        expected_n = int(recipe.get("expected_code_count") or 0)
        expected_v = int(recipe.get("reward_value") or 0)

        # --- Surplus: linked issued rows beyond the frozen recipe ---------
        linked = _linked_issued_rows(db, ledger_id)
        by_pool: collections.Counter = collections.Counter(
            str(r.get("pool_id") or "").strip().upper() for r in linked
        )
        for pool_id, n in by_pool.items():
            allowed = int(required.get(pool_id, 0))
            if n > allowed:
                surplus_found += 1
                f.add(
                    f"ledger {ledger_id} ({lg.get('tier')} "
                    f"{lg.get('entitlement_month') or lg.get('year_month')}): "
                    f"{n} linked {pool_id} rows but the recipe owes {allowed} "
                    f"— {n - allowed} surplus code(s) stranded"
                )

        # --- Wrong denomination / wrong stamped value on linked rows ------
        for row in linked:
            pool_id = str(row.get("pool_id") or "").strip().upper()
            if pool_id and pool_id not in required:
                denom_wrong += 1
                f.add(
                    f"ledger {ledger_id}: linked row from pool {pool_id}, "
                    f"which its recipe never draws from"
                )
                continue
            implied = pool_denomination(pool_id)
            stamped = row.get("voucher_value")
            if implied is not None and stamped is not None and int(stamped) != int(implied):
                stamp_wrong += 1
                f.add(
                    f"ledger {ledger_id}: code in {pool_id} stamped "
                    f"voucher_value={stamped}, expected {implied}"
                )

        if lg.get("status") != "ISSUED":
            continue
        codes = [v for v in (lg.get("vouchers") or []) if (v or {}).get("code")]
        if not codes and lg.get("voucher_code"):
            continue  # historical single-code ledger — expected shape
        actual_v = sum(int(v.get("value") or 0) for v in codes)
        if len(codes) != expected_n:
            count_mismatch += 1
            f.add(
                f"ledger {ledger_id} ({lg.get('tier')}): {len(codes)} codes, expected {expected_n}"
            )
        if actual_v != expected_v:
            value_mismatch += 1
            f.add(
                f"ledger {ledger_id} ({lg.get('tier')}): issued value ${actual_v}, "
                f"expected frozen reward_value ${expected_v}"
            )
        stored_issued = lg.get("issued_value")
        if stored_issued is not None and int(stored_issued) != expected_v:
            value_mismatch += 1
            f.add(f"ledger {ledger_id}: issued_value={stored_issued} != reward_value {expected_v}")

    if not surplus_found:
        f.ok("no ledger carries surplus linked voucher rows")
    if not denom_wrong:
        f.ok("no linked row comes from a pool outside its recipe")
    if not stamp_wrong:
        f.ok("every linked row's stamped value matches its pool denomination")
    if not count_mismatch and not value_mismatch:
        f.ok("every ISSUED ledger matches its recipe (count and value)")

    # BOTH link fields are collected independently, not `a or b`: a row whose
    # string `issued_for_ledger_id` and ObjectId `ledger_id` disagree is
    # itself the corruption being hunted, and coalescing them would hide it.
    owners: dict[str, set] = collections.defaultdict(set)
    for row in db.voucher_pools.find({"status": "issued"}):
        code = row.get("code")
        if not code:
            continue
        for owner in (row.get("issued_for_ledger_id"), row.get("ledger_id")):
            if owner is not None:
                owners[str(code)].add(str(owner))
    shared = {c: o for c, o in owners.items() if len(o) > 1}
    if shared:
        for code, ledger_ids in list(shared.items())[:20]:
            f.add(f"voucher code {code} is linked to {len(ledger_ids)} ledgers: {sorted(ledger_ids)}")
    else:
        f.ok("no voucher code is allocated to two ledgers")


def check_plan_assignment(db, f: Findings, month: str | None) -> None:
    print(f"\n== plan-assignment ({month or 'all months'}) ==")
    query: dict = {"ledger_type": "AFFILIATE_MONTHLY"}
    if month:
        query["$or"] = [{"entitlement_month": month}, {"year_month": month}]
    leaked = 0
    scanned = 0
    for lg in db.affiliate_ledger.find(query):
        stored_plan = lg.get("reward_plan")
        if not stored_plan:
            continue  # historical ledger — resolved lazily, nothing to check
        scanned += 1
        implied = resolve_plan_id(lg.get("entitlement_month") or lg.get("year_month"))
        if stored_plan != implied:
            leaked += 1
            f.add(
                f"ledger {lg.get('_id')}: reward_plan={stored_plan} but "
                f"month {lg.get('entitlement_month') or lg.get('year_month')} implies {implied}"
            )
    if not leaked:
        f.ok(f"all {scanned} plan-stamped ledgers agree with their entitlement month")


def _read_only_db():
    """A Mongo handle that performs NO writes to reach it.

    ``database.init_db()`` is deliberately NOT used: it calls
    ``ensure_indexes()``, so merely connecting would create indexes — a write
    on the very production database this tool promises only to read. The
    connection is opened directly instead, with a secondary-preferred read
    preference so the work lands off the primary where a replica set allows
    it.
    """
    import os as _os

    from pymongo import MongoClient
    from pymongo.read_preferences import SecondaryPreferred

    mongo_url = _os.environ.get("MONGO_URL")
    if not mongo_url:
        raise SystemExit("MONGO_URL is not configured")
    client = MongoClient(mongo_url, read_preference=SecondaryPreferred())
    return client[_os.environ.get("MONGO_DB", "referral_bot")]


CHECKS = ("plan-config", "inventory", "ledger-integrity", "plan-assignment")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", help="Entitlement month, YYYYMM (e.g. 202609)")
    parser.add_argument("--check", action="append", choices=CHECKS,
                        help="Run only this check (repeatable). Default: all.")
    parser.add_argument(
        "--expect", action="append", metavar="TIER=COUNT",
        help="Expected number of rewards for a tier, e.g. --expect T3=40. "
             "Repeatable. Required inventory is derived from the month's own "
             "recipes and reported as required/available/shortage per denomination.",
    )
    args = parser.parse_args()

    expected_demand: dict[str, int] = {}
    for item in args.expect or []:
        tier, _, count = str(item).partition("=")
        try:
            expected_demand[tier.strip().upper()] = int(count)
        except ValueError:
            parser.error(f"--expect expects TIER=COUNT, got {item!r}")
    selected = args.check or list(CHECKS)

    print(
        "READ-ONLY affiliate reward-plan verification.\n"
        "  Connects directly with a secondary-preferred read preference and never\n"
        "  calls init_db()/ensure_indexes(): no index is created, nothing is\n"
        "  inserted, updated or dropped, and no collection is created."
    )
    f = Findings()

    if "plan-config" in selected:
        check_plan_config(f)

    db_checks = [c for c in selected if c != "plan-config"]
    if db_checks:
        db = _read_only_db()
        if "inventory" in db_checks:
            if not args.month:
                print("\n== inventory ==\n  [skipped] --month is required")
            else:
                check_inventory(db, f, args.month, expected_demand or None)
        if "ledger-integrity" in db_checks:
            check_ledger_integrity(db, f, args.month)
        if "plan-assignment" in db_checks:
            check_plan_assignment(db, f, args.month)

    print(f"\n{'-' * 60}")
    if f.items:
        print(f"RESULT: {len(f.items)} finding(s)")
        return 1
    print("RESULT: all selected checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
