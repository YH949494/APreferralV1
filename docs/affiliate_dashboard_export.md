# Affiliate Dashboard Export

- `raw_welcome_code_map` is exported from MongoDB `affiliate_ledger`.
- `raw_referral_map` is derived from those same ledger rows.
- `raw_coupon_data` remains manual monthly input and is never written by exporter.
- Join key in Sheets is `voucher_code` / `coupon_code` / `code`.

## Indexes
- `affiliate_ledger_updated_at_idx` on `updated_at`
- `affiliate_ledger_created_at_idx` on `created_at`
- `affiliate_ledger_year_month_status_idx` on `year_month,status`
- `affiliate_ledger_status_updated_at_idx` on `status,updated_at`
- `affiliate_ledger_inviter_idx` on `inviter_user_id`
- `affiliate_ledger_invitee_idx` on `invitee_user_id`
- `affiliate_ledger_code_idx` on `voucher_code`

All are created best-effort via `safe_create_index`.

## Run behavior
- `python -m affiliate_dashboard_export` runs **monthly snapshot mode**.
- Monthly snapshot mode exports the **previous completed month** (`target_year_month`) using `SCHEDULER_CRON_TIMEZONE` (fallback `Asia/Kuala_Lumpur`).
- Monthly snapshot mode does **not** require `ALLOW_FULL_EXPORT_REBUILD`.
- Monthly snapshot mode does **not** use checkpoint gating.
- Monthly snapshot payload only writes `raw_welcome_code_map` and `raw_referral_map`; `raw_coupon_data` is never written.
- `run_affiliate_dashboard_export()` is the incremental/manual internal path and uses checkpoint/full-rebuild logic.

## Scheduler
- Runs monthly on day `1` at `08:00` in `SCHEDULER_CRON_TIMEZONE` (fallback `Asia/Kuala_Lumpur`).
- Logs include `query_mode=monthly_snapshot` and `target_year_month=<YYYYMM>`.

## Verify
- Check exporter logs for monthly start/success/failure and row counts.
- Check Sheets tabs: `raw_welcome_code_map` and `raw_referral_map` updated timestamp.
- Confirm sample inviter/invitee rows and code join fields.
- Atlas scan alerts should reduce due to bounded reads and indexes.
