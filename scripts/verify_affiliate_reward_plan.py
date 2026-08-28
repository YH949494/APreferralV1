#!/usr/bin/env python3
"""READ-ONLY verification of the September 2026 affiliate reward migration.

Writes NOTHING, ever — there is no commit/apply mode, by design. This is the
pre-deployment preflight and the post-deployment validation for the
denomination-plan rollout.

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


def check_inventory(db, f: Findings, month: str) -> None:
    print(f"\n== inventory (entitlement month {month}) ==")
    from affiliate_rewards import _find_batches_for_period, _month_window_from_yyyymm

    start_utc, end_utc = _month_window_from_yyyymm(month)
    if start_utc is None:
        f.add(f"invalid month {month!r}")
        return
    print(f"  window: {start_utc.isoformat()} -> {end_utc.isoformat()} (UTC)")

    pools = DENOMINATION_POOL_IDS if resolve_plan_id(month) == DENOMINATION_PLAN_ID else TIERS
    for pool_id in pools:
        available = db.voucher_pools.count_documents({"pool_id": pool_id, "status": "available"})
        issued = db.voucher_pools.count_documents({"pool_id": pool_id, "status": "issued"})
        batches = _find_batches_for_period(
            db, pool_id=pool_id, period_start_utc=start_utc, period_end_utc=end_utc
        )
        note = f"available={available} issued={issued} batches_for_month={len(batches)}"
        if len(batches) > 1:
            f.add(f"{pool_id}: {len(batches)} batches fully cover {month} (ambiguous) — {note}")
        elif available == 0:
            f.add(f"{pool_id}: no available stock — {note}")
        else:
            f.ok(f"{pool_id}: {note}")


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


CHECKS = ("plan-config", "inventory", "ledger-integrity", "plan-assignment")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", help="Entitlement month, YYYYMM (e.g. 202609)")
    parser.add_argument("--check", action="append", choices=CHECKS,
                        help="Run only this check (repeatable). Default: all.")
    args = parser.parse_args()
    selected = args.check or list(CHECKS)

    print("READ-ONLY affiliate reward-plan verification. No writes are performed.")
    f = Findings()

    if "plan-config" in selected:
        check_plan_config(f)

    db_checks = [c for c in selected if c != "plan-config"]
    if db_checks:
        from database import get_db, init_db

        init_db()
        db = get_db()
        if "inventory" in db_checks:
            if not args.month:
                print("\n== inventory ==\n  [skipped] --month is required")
            else:
                check_inventory(db, f, args.month)
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
