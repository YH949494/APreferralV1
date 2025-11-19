import argparse
import csv
import os
from datetime import datetime
from typing import Optional
from pymongo import MongoClient


def _previous_month_key(base: Optional[datetime] = None) -> str:
    dt = base or datetime.utcnow()
    year = dt.year
    month = dt.month - 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}-{month:02d}"


def _parse_month(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Month must be in YYYY-MM format") from exc
    return value


def main():
    parser = argparse.ArgumentParser(
        description="Inspect archived monthly XP snapshots. Requires MONGO_URL."
    )
    parser.add_argument(
        "--month",
        default=_previous_month_key(),
        type=_parse_month,
        help="Month to inspect (YYYY-MM). Defaults to the most recent completed month.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="How many rows to print to stdout (default: 25).",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Optional path to export the full snapshot as CSV.",
    )

    args = parser.parse_args()

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise SystemExit("MONGO_URL is not set; cannot query MongoDB.")

    client = MongoClient(mongo_url)
    collection = client["referral_bot"]["monthly_xp_history"]

    cursor = collection.find({"month": args.month}).sort("monthly_xp", -1)
    rows = list(cursor)

    if not rows:
        print(f"No monthly XP snapshot found for {args.month}.")
        return

    print(f"Found {len(rows)} users for month {args.month}.")
    print("Top results:\n------------------------------")
    display_limit = args.limit if args.limit and args.limit > 0 else len(rows)
    for idx, row in enumerate(rows[:display_limit], start=1):
        username = row.get("username") or "(no username)"
        print(
            f"{idx:>3}. {username:<20} | user_id={row['user_id']} | XP={row.get('monthly_xp', 0)} | "
            f"status_before={row.get('status_before_reset')} -> after={row.get('status_after_reset')}"
        )

    if args.csv:
        fieldnames = [
            "user_id",
            "username",
            "month",
            "monthly_xp",
            "status_before_reset",
            "status_after_reset",
            "captured_at_utc",
            "captured_at_kl",
        ]
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field) for field in fieldnames})
        print(f"Snapshot exported to {args.csv}")
if __name__ == "__main__":
    main()
