# Mongo query targeting remediation (2026-05)

## Atlas alert reason
MongoDB Atlas Performance Advisor reported recurring `Query Targeting: Scanned Objects / Returned > 1000` on high-frequency query shapes, with the worst shape on `xp_events` dedupe-style aggregation by `unique_key` + `user_id`.

## Affected collections
- `xp_events`
- `users`
- `invite_link_map`
- `referral_audit`
- `miniapp_sessions_daily`

## Indexes added
- `xp_events_unique_key_user_id_idx` on `{ unique_key: 1, user_id: 1 }` (partial: `unique_key` and `user_id` exist).
- `xp_events_user_created_invalidated_idx` on `{ user_id: 1, created_at: 1, invalidated: 1 }` (partial: `user_id` exists).
- `users_pm1_due_pending_idx` on `{ pm1_due_at_utc: 1, pm1_sent_at_utc: 1, pm1_disabled: 1 }` (partial due+unsent).
- `users_pm2_due_pending_idx` on `{ pm2_due_at_utc: 1, pm2_sent_at_utc: 1, pm2_disabled: 1 }` (partial due+unsent).
- `users_pm3_due_pending_idx` on `{ pm3_due_at_utc: 1, pm3_sent_at_utc: 1, pm3_disabled: 1 }` (partial due+unsent).
- `users_pm4_due_pending_idx` on `{ pm4_due_at_utc: 1, pm4_sent_at_utc: 1, pm4_disabled: 1 }` (partial due+unsent).
- `users_mywin7_due_pending_idx` on `{ mywin7_due_at_utc: 1, mywin7_sent_at_utc: 1, mywin7_disabled: 1 }` (partial due+unsent).
- `users_mywin14_due_pending_idx` on `{ mywin14_due_at_utc: 1, mywin14_sent_at_utc: 1, mywin14_disabled: 1 }` (partial due+unsent).
- `invite_link_map_inviter_created_idx` on `{ inviter_id: 1, created_at: -1 }` (partial: `inviter_id` exists).
- `referral_audit_inviter_created_idx` on `{ inviter_user_id: 1, created_at: 1 }` (partial: `inviter_user_id` exists).
- `miniapp_sessions_daily_date_utc_idx` on `{ date_utc: 1 }`.
- `miniapp_sessions_daily_date_idx` on `{ date: 1 }`.
- `users_snapshot_updated_at_idx` on `{ snapshot_updated_at: 1 }` (partial: `snapshot_updated_at` exists).

All startup index creation remains best-effort via `safe_create_index`, preserving non-fatal behavior for conflict/error cases.

## Query shape improvements applied
- XP dedupe aggregate matcher now also requires `user_id` exists and is not null before grouping by `{user_id, unique_key}`.
- No scheduler timing, referral, voucher, or XP business rules were changed.

## Intentionally not changed
- No materialized summary table for xp dedupe/lifetime aggregates.
- No dashboard/export schema changes.
- No broad query rewrites.

## Post-deploy verification
1. Check Atlas Performance Advisor and Query Insights for 24-48h after deployment.
2. Confirm reduced `docsExamined / nReturned` for:
   - `xp_events` dedupe and snapshot aggregates
   - users PM due reminder filters
   - `invite_link_map` inviter+created sort lookup
3. Confirm no startup failure from index creation logs (index creation remains non-fatal).
4. Confirm voucher claim latency remains stable during scheduler and export windows.
