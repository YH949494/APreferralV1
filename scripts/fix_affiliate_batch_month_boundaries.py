#!/usr/bin/env python3
"""One-time correction for T1-T4 affiliate voucher batches whose schedule
window was hand-typed as an approximation of a calendar month (e.g.
"2026-08-01 00:01" -> "2026-08-31 23:59") instead of the canonical KL
calendar-month boundary the claimability rule requires (full containment:
start = 1st 00:00:00 KL, end = 1st of next month 00:00:00 KL, exclusive).

A batch off by even one minute on either edge fails
``_find_batches_for_period``'s full-containment check, so
``get_claimable_pool_inventory``/``_resolve_monthly_ledger_target`` treat the
entitlement month as having no batch at all — reporting 0 claimable despite
the batch holding available stock.

This script only rewrites each affected batch's ``starts_at``/``ends_at``
(and the same denormalized fields on its ``voucher_pools`` rows) via the
existing ``affiliate_voucher_batches.update_batch`` — the same function the
Admin Dashboard's "Edit Batch Schedule" action uses, so the same safety
checks apply:
  - a batch with any already-issued voucher is skipped (schedule edits are
    refused once real claims have happened against a window);
  - the corrected window is checked for overlap against other batches for
    the same tier before it's applied;
  - voucher codes, their status, ownership, ledger linkage, and the batch's
    uploaded/available/issued counters are never touched.

Some batches hit exactly that "already has issued vouchers" guard
(``active_batch_edit_restricted``) even though their window is the known
malformed shape — a real production run against a live batch will refuse
those the same way it refuses any other schedule edit on an active batch.
That guard is correct and must stay global. This script instead offers a
narrowly-scoped, opt-in maintenance bypass (``--allow-active-boundary-repair``)
that performs the boundary correction directly — bypassing only the
``update_batch`` schedule-edit guard, never any other safety check — and
only when every one of these holds for that exact batch:
  - its pool_id is T1-T4 (never WELCOME, which has no entitlement-month
    concept and isn't eligible for this repair at all);
  - the batch's entitlement month equals the ``--month`` given on the
    command line (the bypass never runs without an explicit ``--month``);
  - its current ``starts_at``/``ends_at`` are *exactly* the known malformed
    shape (first-of-month 00:01:00 KL -> last-of-month 23:59:00 KL) for
    that month — not merely "close"; any other shape (a deliberately
    custom active-batch schedule, say) is refused;
  - the corrected (canonical) window does not overlap any other batch for
    the same pool_id.
Even when all of that holds, only the batch's own ``starts_at``/``ends_at``
and the matching denormalized fields on its ``voucher_pools`` rows are
touched — voucher codes, status, ``issued_to``, ``ledger_id``,
``issued_for_ledger_id``, and every other issuance field are left exactly
as they are. Without the flag, the script's behavior is unchanged: an
active batch is reported ``active_batch_edit_restricted`` and left alone.

Usage:
  MONGO_URL='mongodb://...' python scripts/fix_affiliate_batch_month_boundaries.py [--db referral_bot] [--month 202608] [--dry-run] [--allow-active-boundary-repair]

--month restricts the correction to a single "YYYYMM" entitlement month
(inferred from each batch's current starts_at); omit it to scan every T1-T4
batch. --dry-run reports what would change without writing anything.
--allow-active-boundary-repair opts into the narrowly-scoped maintenance
bypass described above for batches that would otherwise be reported
``active_batch_edit_restricted``; it has no effect on batches update_batch
can already fix normally, and it requires --month.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from pymongo import MongoClient  # noqa: E402

from affiliate_voucher_batches import (  # noqa: E402
    KL_TZ,
    ENTITLEMENT_MONTH_POOL_IDS,
    _as_aware_utc,
    _bulk_update_rows,
    _entitlement_month_for_batch,
    _find_overlapping_batch,
    _hydrate_live_counts,
    canonical_entitlement_month_window,
    update_batch,
)

# A batch this far or further from the canonical boundary on either edge is
# not the known "typed an approximation of the month" bug shape — leave it
# alone rather than guess. This is what keeps the script from ever touching
# a batch that intentionally spans multiple months (see
# ``_already_covers_a_full_month`` below for the containment guard that
# makes those safe regardless of tolerance).
_MAX_CORRECTION_DRIFT = timedelta(hours=24)


def _months_touched(starts_at: datetime, ends_at: datetime) -> list[str]:
    """Every "YYYYMM" whose KL calendar month overlaps [starts_at, ends_at)."""
    start_kl = starts_at.astimezone(KL_TZ)
    end_kl = ends_at.astimezone(KL_TZ)
    y, m = start_kl.year, start_kl.month
    months = []
    while (y, m) <= (end_kl.year, end_kl.month):
        months.append(f"{y:04d}{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return months


def _already_covers_a_full_month(starts_at: datetime, ends_at: datetime) -> bool:
    """True if this window already fully contains at least one KL calendar
    month — i.e. it is already valid per
    ``affiliate_rewards._find_batches_for_period``'s own full-containment
    rule for that month, regardless of whether the window itself is
    month-aligned. A batch deliberately spanning more than one month (e.g.
    July 15 -> September 15, to guarantee August coverage) must never be
    "corrected" down to a single inferred month — that would strip the
    entitlement coverage it was built for. See the P1 review finding this
    guards against.
    """
    for yyyymm in _months_touched(starts_at, ends_at):
        canonical_start, canonical_end = canonical_entitlement_month_window(yyyymm)
        if canonical_start is None or canonical_end is None:
            continue
        if starts_at <= canonical_start and ends_at >= canonical_end:
            return True
    return False


def find_misaligned_batches(db, *, month: str | None = None) -> list[dict]:
    """T1-T4 batches that are the known bug shape: a window typed as a
    close approximation of a single calendar month (e.g.
    "2026-08-01 00:01" -> "2026-08-31 23:59") that therefore fails full
    containment and is currently unclaimable — never a batch that already
    satisfies full containment for some month (already valid, however it's
    shaped) or one that differs from any single month by more than a small
    typo-sized drift (left alone rather than guessed at).
    """
    query = {"pool_id": {"$in": list(ENTITLEMENT_MONTH_POOL_IDS)}}
    out = []
    for batch in db.affiliate_voucher_batches.find(query):
        starts_at = _as_aware_utc(batch.get("starts_at"))
        ends_at = _as_aware_utc(batch.get("ends_at"))
        if starts_at is None or ends_at is None:
            continue
        if _already_covers_a_full_month(starts_at, ends_at):
            continue  # already claimable for some month — never touch

        yyyymm = _entitlement_month_for_batch(batch)
        if not yyyymm:
            continue
        if month and yyyymm != month:
            continue
        canonical_start, canonical_end = canonical_entitlement_month_window(yyyymm)
        if canonical_start is None or canonical_end is None:
            continue
        if starts_at == canonical_start and ends_at == canonical_end:
            continue
        if (
            abs((starts_at - canonical_start).total_seconds()) > _MAX_CORRECTION_DRIFT.total_seconds()
            or abs((ends_at - canonical_end).total_seconds()) > _MAX_CORRECTION_DRIFT.total_seconds()
        ):
            continue  # too far from a single month to safely auto-correct

        out.append({
            "batch": batch,
            "entitlement_month": yyyymm,
            "current_starts_at": starts_at,
            "current_ends_at": ends_at,
            "canonical_starts_at": canonical_start,
            "canonical_ends_at": canonical_end,
        })
    return out


def _maintenance_repair_eligibility(db, item: dict, *, month: str | None) -> tuple[bool, str]:
    """Whether ``item`` (one row from ``find_misaligned_batches``) qualifies
    for the narrowly-scoped ``--allow-active-boundary-repair`` bypass — see
    the module docstring for the exact conditions. Never called for WELCOME:
    ``find_misaligned_batches`` only ever looks at ``ENTITLEMENT_MONTH_POOL_IDS``
    (T1-T4), so a batch reaching this function already has an eligible
    pool_id; it's re-checked here anyway as a defense-in-depth belt.
    """
    batch = item["batch"]
    pool_id = batch.get("pool_id")
    if pool_id not in ENTITLEMENT_MONTH_POOL_IDS:
        return False, "pool_id_not_eligible"
    if not month or item["entitlement_month"] != month:
        return False, "month_not_pinned"

    canonical_starts_at = item["canonical_starts_at"]
    canonical_ends_at = item["canonical_ends_at"]
    # The one known malformed shape this repair targets: first-of-month
    # 00:01:00 KL -> last-of-month 23:59:00 KL, which is exactly the
    # canonical window nudged in by one minute on each edge (last-of-month
    # 23:59:00 == next-month 00:00:00 minus one minute).
    known_malformed_starts_at = canonical_starts_at + timedelta(minutes=1)
    known_malformed_ends_at = canonical_ends_at - timedelta(minutes=1)
    if (
        item["current_starts_at"] != known_malformed_starts_at
        or item["current_ends_at"] != known_malformed_ends_at
    ):
        return False, "not_known_malformed_shape"

    overlap = _find_overlapping_batch(
        db,
        pool_id=pool_id,
        starts_at_utc=canonical_starts_at,
        ends_at_utc=canonical_ends_at,
        exclude_batch_id=batch["_id"],
    )
    if overlap:
        return False, "batch_window_overlap"

    return True, "eligible"


def _perform_maintenance_boundary_repair(db, *, batch: dict, canonical_starts_at: datetime, canonical_ends_at: datetime) -> None:
    """Directly rewrite only the boundary fields — never touches voucher
    codes, status, ``issued_to``, ``ledger_id``, ``issued_for_ledger_id``,
    or any other issuance data, on either collection.

    The ``voucher_pools`` rows are written first, the batch document second
    (the opposite order from a batch-then-rows sequencing), so a run
    interrupted between the two writes leaves the batch document still
    reporting its old (malformed) window. ``find_misaligned_batches`` keys
    off the batch document, so the batch is still picked up as misaligned
    on a rerun and this function is safely re-entrant: re-applying the same
    ``$set`` to already-canonical ``voucher_pools`` rows is a no-op, and
    the batch document then gets its one remaining write.
    """
    oid = batch["_id"]
    boundary_set = {"starts_at": canonical_starts_at, "ends_at": canonical_ends_at}
    _bulk_update_rows(db.voucher_pools, {"batch_id": oid}, {"$set": boundary_set})
    db.affiliate_voucher_batches.update_one({"_id": oid}, {"$set": boundary_set})


def fix_batches(
    db,
    *,
    admin_identity: str,
    month: str | None = None,
    dry_run: bool = False,
    allow_active_boundary_repair: bool = False,
) -> list[dict]:
    results = []
    for item in find_misaligned_batches(db, month=month):
        batch = item["batch"]
        batch_id = batch["_id"]
        pool_id = batch.get("pool_id")
        label = f"{pool_id} / {batch.get('batch_name')} ({batch_id})"
        row = {
            "batch_id": str(batch_id),
            "label": label,
            "entitlement_month": item["entitlement_month"],
            "before": (item["current_starts_at"], item["current_ends_at"]),
            "after": (item["canonical_starts_at"], item["canonical_ends_at"]),
        }

        if dry_run:
            live_issued_count = int(_hydrate_live_counts(db, batch).get("issued_count") or 0)
            if live_issued_count > 0:
                if allow_active_boundary_repair:
                    eligible, reason = _maintenance_repair_eligibility(db, item, month=month)
                    if eligible:
                        print(
                            f"[maintenance_boundary_repair][DRY_RUN] {pool_id} batch_id={batch_id} "
                            f"month={item['entitlement_month']}"
                        )
                        row["result"] = "dry_run_maintenance_boundary_repair"
                    else:
                        row["result"] = f"active_batch_edit_restricted:{reason}"
                else:
                    row["result"] = "active_batch_edit_restricted"
            else:
                row["result"] = "dry_run"
            results.append(row)
            continue

        result = update_batch(
            db,
            batch_id,
            admin_identity=admin_identity,
            updates={"entitlement_month": item["entitlement_month"]},
            now_utc=datetime.now(timezone.utc),
        )
        if result.get("ok"):
            row["result"] = "ok"
            results.append(row)
            continue

        code = result.get("code")
        if code == "active_batch_edit_restricted" and allow_active_boundary_repair:
            eligible, reason = _maintenance_repair_eligibility(db, item, month=month)
            if eligible:
                print(
                    f"[maintenance_boundary_repair] {pool_id} batch_id={batch_id} "
                    f"month={item['entitlement_month']}"
                )
                _perform_maintenance_boundary_repair(
                    db,
                    batch=batch,
                    canonical_starts_at=item["canonical_starts_at"],
                    canonical_ends_at=item["canonical_ends_at"],
                )
                row["result"] = "maintenance_boundary_repair"
                results.append(row)
                continue
            code = f"active_batch_edit_restricted:{reason}"
        row["result"] = code
        results.append(row)
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.environ.get("MONGO_DB_NAME", "referral_bot"))
    parser.add_argument("--month", default=None, help="Restrict to one entitlement month, e.g. 202608")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-active-boundary-repair",
        action="store_true",
        help=(
            "One-time maintenance bypass: for a batch refused with "
            "active_batch_edit_restricted, repair its boundaries directly "
            "(bypassing only that guard) when the batch's window is exactly "
            "the known malformed shape for the given --month and the "
            "corrected window doesn't overlap another batch. Requires "
            "--month. See the module docstring for the full condition list."
        ),
    )
    args = parser.parse_args()

    if args.allow_active_boundary_repair and not args.month:
        print("--allow-active-boundary-repair requires --month", file=sys.stderr)
        return 1

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        print("MONGO_URL is required", file=sys.stderr)
        return 1

    client = MongoClient(mongo_url)
    db = client[args.db]

    results = fix_batches(
        db, admin_identity="migration:fix_affiliate_batch_month_boundaries",
        month=args.month, dry_run=args.dry_run,
        allow_active_boundary_repair=args.allow_active_boundary_repair,
    )
    if not results:
        print("No misaligned T1-T4 batches found.")
        return 0

    for row in results:
        before_s, before_e = row["before"]
        after_s, after_e = row["after"]
        print(
            f"[{row['result']}] {row['label']} month={row['entitlement_month']} "
            f"before=({before_s} -> {before_e}) after=({after_s} -> {after_e})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
