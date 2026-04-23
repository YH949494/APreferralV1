"""Manual cleanup script to remove deprecated Xmas fields from users collection.

Run manually:
    python cleanup_xmas_fields.py [--dry-run] [--yes]
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

FIELDS_TO_UNSET = [
    "xmas_entry_source",
    "xmas_checked_in",
    "xmas_checkin_at",
    "xmas_week",
    "xmas_year",
    "xmas_is_new_joiner",
]
DRY_RUN = False


def _build_filter() -> dict:
    return {"$or": [{field: {"$exists": True}} for field in FIELDS_TO_UNSET]}


def _build_unset_spec() -> dict:
    return {field: "" for field in FIELDS_TO_UNSET}


def _get_users_collection(mongo_url: str):
    """Prefer existing database.py initialization pattern; fallback to direct client."""
    try:
        from database import get_db, init_db  # local import to keep script standalone

        init_db(mongo_url=mongo_url)
        db = get_db()
        return db["users"], None
    except Exception:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        db = client["referral_bot"]
        return db["users"], client


def run_cleanup(*, dry_run: bool) -> int:
    started = time.perf_counter()
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        print("[CLEANUP] error=MONGO_URL_NOT_SET")
        return 1

    try:
        users_col, client = _get_users_collection(mongo_url)

        # Validate connectivity before attempting count/update.
        users_col.database.client.admin.command("ping")

        query = _build_filter()
        matched = users_col.count_documents(query)
        modified = 0

        if not dry_run and matched > 0:
            result = users_col.update_many(query, {"$unset": _build_unset_spec()})
            matched = result.matched_count
            modified = result.modified_count

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(
            f"[CLEANUP] dry_run={dry_run} matched={matched} modified={modified} elapsed_ms={elapsed_ms}"
        )

        if client is not None:
            client.close()
        return 0
    except PyMongoError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(f"[CLEANUP] error=CONNECTION_OR_QUERY_FAILED detail={exc} elapsed_ms={elapsed_ms}")
        return 2
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(f"[CLEANUP] error=UNEXPECTED detail={exc} elapsed_ms={elapsed_ms}")
        return 3


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unset deprecated Xmas fields from users collection (manual script)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=DRY_RUN,
        help="Count matching documents only; do not modify.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt for non-dry-run execution.",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        confirmation = input("Proceed with unsetting Xmas fields from users? [y/N]: ").strip().lower()
        if confirmation not in {"y", "yes"}:
            print("[CLEANUP] cancelled=true")
            return 0

    return run_cleanup(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
