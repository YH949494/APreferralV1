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
