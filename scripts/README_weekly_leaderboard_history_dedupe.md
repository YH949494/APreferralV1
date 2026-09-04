# weekly_leaderboard_history duplicate repair

Fixes the `[DB][INDEX] create_failed collection=weekly_leaderboard_history
name=uniq_weekly_history_week_start` warning by removing historical
duplicate `week_start` documents so the unique index can be created.

Dry run first (default — nothing is deleted):

```bash
MONGO_URL='mongodb://...' python -m scripts.repair_weekly_leaderboard_history_duplicates --db referral_bot
```

Apply deletions:

```bash
MONGO_URL='mongodb://...' python -m scripts.repair_weekly_leaderboard_history_duplicates --db referral_bot --commit
```

For each duplicated week the script prints `document_count`, `document_ids`,
`keeper_id`, `delete_ids`, and `reason_keeper_selected` before deleting
anything. The keeper is chosen deterministically: valid schema > Monday
week-start window > more complete leaderboard data > newest `archived_at` >
newest `_id`.

The script is idempotent: once a week's duplicates are removed, re-running
it (dry-run or `--commit`) reports `duplicate_week_count=0` and deletes
nothing. After it runs clean, the bot's own boot-time index creation
(`ensure_indexes()` in `main.py`) picks up the now-unique `week_start` field
and creates `uniq_weekly_history_week_start` — no separate index step is
needed here.
