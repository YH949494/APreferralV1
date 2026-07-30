#!/usr/bin/env python3
"""Manual repair for a missed Sunday weekly referral leaderboard post.

Publishes (or previews) the "Top 5 Growth Leaders This Week" post for a
single completed week, sourced from the frozen weekly_referral_posts record
if one already exists, otherwise from the pre-reset weekly_leaderboard_history
archive written by reset_weekly_xp() (never from live users.weekly_referrals,
which has already been zeroed for a completed week).

Does NOT call reset_weekly_xp() and does NOT modify current-week counters.
Idempotent on the same "weekly_referral_post:{week_key}" key used by the
Sunday 21:00 scheduler job, and uses the same WEEKLY_REF_POST_CHAT_ID
destination — so a rerun after a successful send is a safe no-op.

Usage:
    python -m scripts.publish_weekly_referral_post --week-key 2026-07-20
    python -m scripts.publish_weekly_referral_post --week-key 2026-07-20 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from database import init_db  # noqa: E402 - must come after sys.path fix
from scheduler import publish_weekly_referral_post, render_weekly_referral_post_text  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("publish_weekly_referral_post")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week-key", required=True, help="Monday of the target week, YYYY-MM-DD (Asia/Kuala_Lumpur)")
    parser.add_argument("--dry-run", action="store_true", help="Print the final message without sending it")
    args = parser.parse_args()

    init_db(mongo_url=os.environ.get("MONGO_URL"))

    result = publish_weekly_referral_post(
        week_key=args.week_key, dry_run=args.dry_run, run_id="manual_repair", source="archive"
    )

    entries = result.get("entries") or []
    if not entries:
        print(f"No entries for week_key={args.week_key} (status={result.get('status')}); nothing to publish.")
        return 0

    if args.dry_run:
        print(result.get("preview_text") or render_weekly_referral_post_text(entries))
        return 0

    if result.get("status") == "sent":
        print(f"Published week_key={args.week_key} message_id={result.get('message_id')}")
        return 0

    print(f"Not sent. week_key={args.week_key} status={result.get('status')} failure_reason={result.get('failure_reason')}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
