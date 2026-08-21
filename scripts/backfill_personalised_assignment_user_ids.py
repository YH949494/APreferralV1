#!/usr/bin/env python3
"""One-shot data-repair backfill: bind existing personalised voucher rows to
an immutable assigned_to_user_id where it can be resolved unambiguously.

Context: personalised voucher rows were historically keyed only by
usernameLower. Because Telegram usernames are mutable and can be reused,
visibility/claim checks that matched purely on usernameLower could leak a
voucher to whoever currently holds a username that used to belong to the
intended recipient. vouchers.py now prefers assigned_to_user_id when a row
has one, falling back to usernameLower only for legacy rows without it.
This script performs the one-time repair for existing rows.

For each personalised voucher row with no assigned_to_user_id, this script:
  - looks up `users` by the row's usernameLower
  - if exactly ONE user currently holds that username, sets
    assigned_to_user_id to that user's user_id
  - if ZERO or MULTIPLE users hold that username, leaves the row untouched
    and reports it for manual review (ambiguous — do not guess)

Only touches rows with status "unclaimed". A claimed row's recipient is
already fixed by the claim itself (claimedBy), so it is left alone.

Safe to run multiple times (idempotent — already-bound rows are skipped).
Defaults to --dry-run. Pass --apply to actually write.

Usage:
    python scripts/backfill_personalised_assignment_user_ids.py --dry-run
    python scripts/backfill_personalised_assignment_user_ids.py --apply
    python scripts/backfill_personalised_assignment_user_ids.py --apply --drop-id <id>
"""
from __future__ import annotations

import argparse
import os
import sys

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from bson.objectid import ObjectId  # noqa: E402
from database import init_db, get_db  # noqa: E402

_PERSONALISED_ALIASES = ("personalised", "personalized")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually write changes (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no writes (default)")
    parser.add_argument("--drop-id", help="Limit to a single drop's assignments")
    args = parser.parse_args()
    apply_changes = bool(args.apply) and not args.dry_run

    init_db()
    db = get_db()

    query = {
        "type": {"$in": list(_PERSONALISED_ALIASES)},
        "status": "unclaimed",
        "assigned_to_user_id": None,
    }
    if args.drop_id:
        try:
            oid = ObjectId(args.drop_id) if ObjectId.is_valid(args.drop_id) else args.drop_id
        except Exception:
            oid = args.drop_id
        query["dropId"] = {"$in": [args.drop_id, str(oid)]}

    rows = list(db.vouchers.find(query))
    print(f"Found {len(rows)} unbound unclaimed personalised rows"
          + (f" for dropId={args.drop_id}" if args.drop_id else "") + ".")

    bound = 0
    ambiguous = 0
    unresolved = 0

    for row in rows:
        uname = row.get("usernameLower")
        row_id = row.get("_id")
        drop_id = row.get("dropId")
        if not uname:
            unresolved += 1
            print(f"  SKIP row_id={row_id} dropId={drop_id}: no usernameLower to resolve.")
            continue

        holders = list(db.users.find({"usernameLower": uname}, {"user_id": 1}))
        if len(holders) == 0:
            unresolved += 1
            print(f"  SKIP row_id={row_id} dropId={drop_id} usernameLower={uname!r}: no current user holds this username.")
            continue
        if len(holders) > 1:
            ambiguous += 1
            print(f"  SKIP row_id={row_id} dropId={drop_id} usernameLower={uname!r}: {len(holders)} users currently match — ambiguous, needs manual review.")
            continue

        uid = holders[0].get("user_id")
        if uid is None:
            unresolved += 1
            print(f"  SKIP row_id={row_id} dropId={drop_id} usernameLower={uname!r}: matched user has no user_id.")
            continue

        print(f"  {'APPLY' if apply_changes else 'WOULD BIND'} row_id={row_id} dropId={drop_id} usernameLower={uname!r} -> assigned_to_user_id={uid}")
        if apply_changes:
            db.vouchers.update_one({"_id": row_id}, {"$set": {"assigned_to_user_id": uid}})
        bound += 1

    print(f"\nSummary: bound={bound} ambiguous={ambiguous} unresolved={unresolved} total={len(rows)}")
    if not apply_changes:
        print("Dry run only — no writes made. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
