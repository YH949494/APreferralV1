#!/usr/bin/env python3
"""Data-repair helper for existing personalised voucher rows that predate
assigned_to_user_id binding.

Context: personalised voucher rows were historically keyed only by
usernameLower. Because Telegram usernames are mutable and can be reused,
visibility/claim checks that matched purely on usernameLower could leak a
voucher to whoever currently holds a username that used to belong to the
intended recipient. vouchers.py now prefers assigned_to_user_id when a row
has one, falling back to usernameLower only for legacy rows without it.

IMPORTANT — this script does NOT auto-bind rows based on "whoever currently
holds the username". That heuristic is unsafe: in the exact incident this is
meant to repair, the username has already been taken over by an unrelated
user, so "the current holder" IS the intruder, not the intended recipient.
Binding on that basis would make the wrong assignment permanent.

Modes:
  (default, no flags)   REPORT ONLY. For each unbound unclaimed personalised
                         row, prints the row's usernameLower and who
                         currently holds it (0, 1, or many users), so an
                         admin can independently verify the true recipient
                         (e.g. against the original CSV/affiliate roster used
                         to create the drop, or by asking the recipient) —
                         never against the current-holder lookup alone.

  --apply --confirm-file <path>
                         Binds ONLY the exact (dropId, usernameLower) pairs
                         listed in a JSON file the admin prepares after
                         manually confirming the true recipient out-of-band:
                             [
                               {"dropId": "...", "usernameLower": "...", "user_id": 12345},
                               ...
                             ]
                         Each entry is applied only if a matching unbound,
                         unclaimed row still exists for that dropId +
                         usernameLower. No other rows are touched.

Only touches rows with status "unclaimed". A claimed row's recipient is
already fixed by the claim itself (claimedBy), so it is left alone.

Safe to run multiple times (idempotent — already-bound rows are skipped).

Usage:
    python scripts/backfill_personalised_assignment_user_ids.py
    python scripts/backfill_personalised_assignment_user_ids.py --drop-id <id>
    python scripts/backfill_personalised_assignment_user_ids.py --apply --confirm-file confirmed.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from bson.objectid import ObjectId  # noqa: E402
from database import init_db, get_db  # noqa: E402

_PERSONALISED_ALIASES = ("personalised", "personalized")


def _drop_id_variants(drop_id):
    if drop_id is None:
        return [None]
    variants = [drop_id]
    try:
        oid = ObjectId(drop_id) if ObjectId.is_valid(drop_id) else None
    except Exception:
        oid = None
    if oid is not None and oid not in variants:
        variants.append(oid)
    return variants


def report(db, *, drop_id: str | None) -> None:
    query = {
        "type": {"$in": list(_PERSONALISED_ALIASES)},
        "status": "unclaimed",
        "assigned_to_user_id": None,
    }
    if drop_id:
        query["dropId"] = {"$in": _drop_id_variants(drop_id)}

    rows = list(db.vouchers.find(query))
    print(f"Found {len(rows)} unbound unclaimed personalised rows"
          + (f" for dropId={drop_id}" if drop_id else "") + ".\n")
    print("For each row, verify the TRUE recipient out-of-band (original")
    print("assignment roster / CSV, or asking the recipient directly) before")
    print("adding it to a --confirm-file. Do NOT assume 'current holder' is")
    print("correct — that is exactly how a reused username slips through.\n")

    for row in rows:
        uname = row.get("usernameLower")
        row_id = row.get("_id")
        row_drop_id = row.get("dropId")
        if not uname:
            print(f"  row_id={row_id} dropId={row_drop_id}: no usernameLower on file — no identity to resolve.")
            continue
        holders = list(db.users.find({"usernameLower": uname}, {"user_id": 1}))
        holder_ids = [h.get("user_id") for h in holders]
        print(f"  row_id={row_id} dropId={row_drop_id} usernameLower={uname!r} current_holder_user_ids={holder_ids}")

    print(f"\nTotal unbound rows: {len(rows)}. No writes were made (report-only).")


def apply_confirmed(db, *, confirm_file: str, drop_id: str | None) -> None:
    with open(confirm_file, "r", encoding="utf-8") as f:
        confirmed = json.load(f)

    if not isinstance(confirmed, list):
        print("--confirm-file must contain a JSON list of {dropId, usernameLower, user_id} objects.", file=sys.stderr)
        sys.exit(1)

    bound = 0
    skipped = 0
    for entry in confirmed:
        entry_drop_id = entry.get("dropId")
        uname = entry.get("usernameLower")
        uid = entry.get("user_id")
        if drop_id and str(entry_drop_id) != str(drop_id):
            continue
        if not entry_drop_id or not uname or uid is None:
            print(f"  SKIP malformed entry: {entry}")
            skipped += 1
            continue

        match_filter = {
            "type": {"$in": list(_PERSONALISED_ALIASES)},
            "dropId": {"$in": _drop_id_variants(entry_drop_id)},
            "usernameLower": uname,
            "status": "unclaimed",
            "assigned_to_user_id": None,
        }
        row = db.vouchers.find_one(match_filter)
        if not row:
            print(f"  SKIP dropId={entry_drop_id} usernameLower={uname!r}: no matching unbound unclaimed row found (already bound, claimed, or wrong username?).")
            skipped += 1
            continue

        db.vouchers.update_one({"_id": row["_id"]}, {"$set": {"assigned_to_user_id": uid}})
        print(f"  BOUND row_id={row['_id']} dropId={entry_drop_id} usernameLower={uname!r} -> assigned_to_user_id={uid}")
        bound += 1

    print(f"\nSummary: bound={bound} skipped={skipped} confirmed_entries={len(confirmed)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Write changes from --confirm-file (default is report-only)")
    parser.add_argument("--confirm-file", help="JSON file of admin-verified {dropId, usernameLower, user_id} entries to bind; required with --apply")
    parser.add_argument("--drop-id", help="Limit to a single drop's assignments")
    args = parser.parse_args()

    if args.apply and not args.confirm_file:
        parser.error("--apply requires --confirm-file (this script never auto-binds from current username holder)")

    init_db()
    db = get_db()

    if args.apply:
        apply_confirmed(db, confirm_file=args.confirm_file, drop_id=args.drop_id)
    else:
        report(db, drop_id=args.drop_id)


if __name__ == "__main__":
    main()
