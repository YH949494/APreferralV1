# Mongo index remediation (APReferral)

## Atlas alert context
Atlas reported scan-heavy queries in `referral_bot.users`, `referral_bot.invite_link_map`, and `referral_bot.miniapp_sessions_daily` with high docs scanned and in-memory sorts.

## Query owners found in this repo
- `users` leaderboard sorts by `weekly_xp` and `weekly_referrals` in `main.py` (`leaderboard_data`) and weekly archival in `reset_weekly_xp`.
- `invite_link_map` lookup uses `chat_id + inviter_id + is_active` filter and `created_at DESC` sort in `get_or_create_referral_invite_link_sync`.
- `miniapp_sessions_daily` is queried by retention flow in `retention_kpis.py` using mixed timestamp fallback fields (`date_utc`, `date`, `day`, `ts`, `created_at`).

## Indexes added
- `users_weekly_xp_desc_idx` on `users({weekly_xp:-1})`.
- `users_weekly_referrals_desc_idx` on `users({weekly_referrals:-1})`.
- `invite_link_map_chat_inviter_active_created_desc_idx` on `invite_link_map({chat_id:1,inviter_id:1,is_active:1,created_at:-1})`.

## Intentionally not added
- `users` due-time indexes (`pm1_due_at_utc`, `pm2_due_at_utc`, `pm3_due_at_utc`, `pm4_due_at_utc`, `mywin7_due_at_utc`, `mywin14_due_at_utc`) were not added: no matching query path in this repo.
- `invite_link_map_inviter_created_desc_idx` was not added: no `inviter_id`-only query shape with `created_at` sort found.
- `miniapp_sessions_daily` single-field day/date/ts/created_at indexes were not added: this code path primarily uses `date_utc` with fallback compatibility fields; adding all Atlas suggestions would be redundant.

## miniapp_sessions_daily canonical decision
Canonical read/write field remains `date_utc` in this repo (existing unique index on `(date_utc, user_id)` and retention reads include `date_utc` first).

## Safe index behavior
`safe_create_index` is best-effort and idempotent:
- requires explicit name,
- skips existing name,
- catches `OperationFailure`/`PyMongoError`,
- logs create/skip/failure,
- never crashes startup.

Current scope: this protection is applied to the remediation indexes added in this patch (including `miniapp_sessions_daily_date_utc_user_id_uidx`). Legacy index bootstrap code in `main.py` still contains raw `create_index` calls unless converted in a separate change.

## Post-deploy verification
1. Atlas Profiler → filter namespace `referral_bot.users` and query shapes for weekly leaderboard sorts.
2. Confirm `docsScanned` drops materially and `hasSortStage`/in-memory sort disappears for fixed paths.
3. Check `referral_bot.invite_link_map` profiler for the link-lookup shape and reduced scan.
4. Ensure query execution counts stay stable (no traffic regression).
5. Check index sizes and working set impact.

## Warning
Do **not** blindly accept all Atlas index suggestions; only add indexes proven by production query shapes.

## weekly_leaderboard_history.week_start (2026-08 Past Leaderboard audit)
- `uniq_weekly_history_week_start` on `weekly_leaderboard_history({week_start:1})`, unique.
- Added because `reset_weekly_xp` previously used an unconditional `insert_one`, so any
  retry (misfire replay, boot catch-up, a second worker) could duplicate a week's archive
  or race on the reset step. The archive write is now an idempotent upsert keyed by
  `week_start`; this index makes that guarantee durable at the storage layer too.
- Created via `safe_create_index` (never raises), preceded by a duplicate-`week_start`
  diagnostic (`ensure_indexes()` in `main.py`) that only **logs** — it does not delete
  any existing duplicate records. If duplicates are ever reported, they must be reviewed
  and merged before the index will actually take effect.

### 2026-09 duplicate cleanup (root cause + resolution)
- **Root cause:** the deprecated `database.save_weekly_snapshot()` writer (gated behind
  `ENABLE_LEGACY_WEEKLY_SNAPSHOT`, dead by default but never removed) used a bare
  `insert_one()` with no key check. When it or an earlier ancestor of `reset_weekly_xp`
  ran more than once for the same week (misfire replay / boot catch-up / a second
  worker instance before the scheduler lock + upsert fix landed), it created a second
  `weekly_leaderboard_history` document for the same `week_start`, e.g.
  `{ week_start: "2025-08-25" }`. That pre-existing duplicate is what makes
  `uniq_weekly_history_week_start` fail with `E11000` on every boot — the diagnostic
  step reports it but never deletes anything.
- **Fix:**
  - `database.save_weekly_snapshot()` now does the same atomic
    `update_one(..., {"$setOnInsert": {...}}, upsert=True)` (with a `DuplicateKeyError`
    catch for the losing side of a race) as `main._archive_week_upsert()`, so no writer
    for this collection can ever `insert_one()` a second document for a `week_start`
    again.
  - `scripts/dedupe_weekly_leaderboard_history.py` is a repeatable, dry-run-by-default
    migration: it finds every duplicated `week_start` (not just `2025-08-25`), inspects
    the full documents, picks one canonical record deterministically (latest valid,
    most complete snapshot; `_id` as a final tiebreak), copies every other document to
    `weekly_leaderboard_history_dedupe_backup` (upsert keyed by the original `_id`, so a
    rerun or a crash mid-run never loses or double-backs-up a record), and only then
    deletes the non-canonical duplicates. Once a `week_start` has a single document the
    script is a no-op on rerun.
  - The startup diagnostic log now names the migration script and both flags needed to
    run it, instead of just reporting the duplicate count.
- **Regression coverage:** `test_weekly_leaderboard_history_dedupe.py` covers concurrent
  writers, existing-duplicate cleanup, dry-run no-op, rerun no-op, deterministic
  canonical selection, index creation succeeding after cleanup, and future duplicate
  inserts being rejected once the index exists.
