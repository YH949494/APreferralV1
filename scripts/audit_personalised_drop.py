#!/usr/bin/env python3
"""Read-only diagnostic for a single personalised voucher drop.

Usage:
    python scripts/audit_personalised_drop.py --name "Affiliate T2 replacement 260821"
    python scripts/audit_personalised_drop.py --drop-id 66b1f2...

For the matched drop, prints:
  - drop_id, drop_type, status, startsAt/endsAt, eligibility, audience/regions
  - number of personalised voucher rows
  - recipient usernames (usernameLower)
  - recipient user_ids where already bound (assigned_to_user_id)
  - rows missing recipient identity (no usernameLower and no assigned_to_user_id)
  - duplicate assignments (same usernameLower appearing more than once)
  - for each row's usernameLower, the user_id(s) in `users` that CURRENTLY hold
    that username, so an admin can see whether a username has changed hands
    since the row was created (Case C in the audit) vs. the row being
    genuinely mis-assigned (Case B)

This script never prints voucher `code` values. It only reads data — it does
not write anything.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from bson.objectid import ObjectId  # noqa: E402
from database import init_db, get_db  # noqa: E402

_PERSONALISED_ALIASES = ("personalised", "personalized")


def _normalize_drop_type(value) -> str:
    dtype = str(value or "pooled").strip().lower()
    if dtype in _PERSONALISED_ALIASES:
        return "personalised"
    return dtype


def _find_drop(db, *, name: str | None, drop_id: str | None):
    if drop_id:
        try:
            oid = ObjectId(drop_id) if ObjectId.is_valid(drop_id) else drop_id
        except Exception:
            oid = drop_id
        drop = db.drops.find_one({"_id": oid})
        if not drop:
            drop = db.drops.find_one({"_id": drop_id})
        return drop
    if name:
        return db.drops.find_one({"name": name})
    return None


def audit(db, drop: dict) -> None:
    drop_id = drop["_id"]
    drop_id_variants = [drop_id, str(drop_id)]

    dtype = _normalize_drop_type(drop.get("type"))
    print("=== Drop ===")
    print(f"_id: {drop_id}")
    print(f"type (raw): {drop.get('type')!r} -> normalized: {dtype}")
    print(f"status: {drop.get('status')}")
    print(f"startsAt: {drop.get('startsAt')}")
    print(f"endsAt: {drop.get('endsAt')}")
    print(f"eligibility: {drop.get('eligibility')}")
    print(f"audience: {drop.get('audience')}")
    print(f"regions (legacy field, if any): {drop.get('regions')}")
    print(f"allowlist (legacy field, if any): {drop.get('allowlist')}")
    print(f"denylist (legacy field, if any): {drop.get('denylist')}")

    if dtype != "personalised":
        print(f"\nNOTE: drop type normalizes to '{dtype}', not 'personalised'. "
              "Voucher-row audit below still runs against whatever rows exist "
              "for this dropId with a personalised type value, if any.")

    rows = list(db.vouchers.find({
        "type": {"$in": list(_PERSONALISED_ALIASES)},
        "dropId": {"$in": drop_id_variants},
    }))

    print(f"\n=== Voucher rows: {len(rows)} ===")

    by_username = defaultdict(list)
    missing_identity = []
    for r in rows:
        uname = r.get("usernameLower")
        uid = r.get("assigned_to_user_id")
        if not uname and uid is None:
            missing_identity.append(r.get("_id"))
        if uname:
            by_username[uname].append(r)

    print("\n-- Recipients (usernameLower -> row count, assigned_to_user_id, status) --")
    for uname, docs in sorted(by_username.items()):
        for d in docs:
            print(
                f"  usernameLower={uname!r:20} assigned_to_user_id={d.get('assigned_to_user_id')!r:10} "
                f"status={d.get('status')!r:12} row_id={d.get('_id')}"
            )

    duplicates = {u: docs for u, docs in by_username.items() if len(docs) > 1}
    print(f"\n-- Duplicate assignments (same usernameLower, >1 row): {len(duplicates)} --")
    for uname, docs in duplicates.items():
        print(f"  {uname}: {len(docs)} rows -> ids={[d.get('_id') for d in docs]}")

    print(f"\n-- Rows missing recipient identity (no usernameLower and no assigned_to_user_id): {len(missing_identity)} --")
    for rid in missing_identity:
        print(f"  row_id={rid}")

    print("\n-- Cross-reference: who CURRENTLY holds each assigned usernameLower --")
    print("   (a mismatch between a row's assigned_to_user_id and the current")
    print("    holder's user_id means the Telegram username has changed hands")
    print("    since this row was created -- Case C, username reuse.)")
    for uname in sorted(by_username.keys()):
        current_holders = list(db.users.find({"usernameLower": uname}, {"user_id": 1}))
        holder_ids = [h.get("user_id") for h in current_holders]
        rows_for_uname = by_username[uname]
        assigned_ids = {d.get("assigned_to_user_id") for d in rows_for_uname if d.get("assigned_to_user_id") is not None}
        mismatch = bool(assigned_ids) and assigned_ids != set(holder_ids)
        flag = "  <-- MISMATCH (username reuse risk)" if mismatch else ""
        print(f"  usernameLower={uname!r:20} current_holder_user_ids={holder_ids} bound_assigned_to_user_id={sorted(assigned_ids)}{flag}")

    print("\n(No voucher codes were printed by this script.)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", help="Exact drop name to look up")
    parser.add_argument("--drop-id", help="Drop _id (ObjectId or string) to look up")
    args = parser.parse_args()

    if not args.name and not args.drop_id:
        parser.error("Provide --name or --drop-id")

    init_db()
    db = get_db()

    drop = _find_drop(db, name=args.name, drop_id=args.drop_id)
    if not drop:
        print("Drop not found.", file=sys.stderr)
        sys.exit(1)

    audit(db, drop)


if __name__ == "__main__":
    main()
