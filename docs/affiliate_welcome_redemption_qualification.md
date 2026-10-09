# Affiliate qualification: verified Welcome redemption (`welcome_redemption_v1`)

Status: **implemented, disabled by default, blocked on an authoritative redemption source** (see §6).

## 1. Audit — current flow (pre-patch)

| Area | Code | Notes |
|---|---|---|
| Referral attribution + join counting | `main.py` member-join handler: historical-success guard (`has_historical_success`, ~L1899), invitee lock (`referral_invitee_lock.claim`, ~L1942), `pending_referrals` upsert keyed `(group_id, invitee_user_id)` with `$setOnInsert` inviter (~L1993), `join` / `join_counted` flow events (~L2028, `affiliate_leaderboard.should_count_referral_join` L165) | **Unchanged by this patch.** |
| **Qualification writer (legacy)** | `scheduler.settle_pending_referrals` (L3953): after the hold, `check_channel` (L4065) → new-user check → `official_channel_retained` or `check_engagement` (check-in/engagement score, L4367) → `award` (L4426): `referral_award_events`, `grant_xp("referral_award", "ref:<invitee>")`, `referral_events.referral_settled` (L4565), `affiliate_rewards.mark_invitee_qualified` | **Root cause:** qualification = channel retention or check-in engagement. No redemption, no gaming-account identity, so one gaming account behind N Telegram IDs yields N qualifications. |
| `mark_invitee_qualified` | `affiliate_rewards.py` L4479 | Inserts `qualified_events` (unique `invitee_id`) at *processing* time, then `evaluate_monthly_affiliate_reward`. Only caller: the settle loop above (3 sites, incl. two legacy-award recovery branches). |
| Scheduler | `main.tick_5min` → `settle_pending_referrals_with_cache_clear`; `confirm_qualified_invitees` (scheduler L3451) is a no-op | |
| Repair / sync scripts | `sync_referral_counts.py`, `repair_referral_ledger.py`, `app/rebuild_snapshots_from_ledger.py` (counter/ledger repair, no qualification writes); `official_channel_reopen_audit.py` (reopens revoked rows to `pending` → after launch they are parked, never awarded); `backfill_referrals.py` (**grants `ref_success` XP from the legacy `referrals` collection — must not be run after launch**); `rollback_pending_referral_xp.py` (XP removal only) | |
| Welcome issuance + code→recipient | `affiliate_rewards.issue_welcome_bonus_if_eligible` (L3314) → `voucher_pools` (`pool_id=WELCOME`, `issued_to_user_id`, `ledger_id`) + `affiliate_ledger` `dedup_key=WELCOME:<uid>` (`voucher_code`); legacy drop path writes `new_joiner_claims {uid, code}` (`vouchers.py` ~L7154). Gates: channel subscription, self-invite block, check-in progress (`vouchers._issue_or_get_welcome_voucher` L1420) | **Unchanged.** Mapping code → original recipient is reliable (unique `(pool_id, code)`). |
| UIM / redemption ingestion | `uim_import.py` (segments only), `claim_risk_sync.py` / `multi_account_risk_sync.py` (risk + `users.linked_gaming_accounts` from UIM `user_profile_summary`), `marketing_upload.ingest_upload` (L368) → `marketing_raw_data` + `marketing_upload_batches` | Only `marketing_raw_data` carries `account` + `coupon_code` + redeem time. See §6 for why it is not yet authoritative. |
| Gaming account identity | `users.linked_gaming_accounts` (UIM, raw strings, no namespace); `mission_pool_processor.resolve_identity` (L307) uses it as `acct:<id>` | Used here for ownership validation only. |
| Consumers | Monthly tiers `evaluate_monthly_affiliate_reward` (L3484) / weekly (L3754) / `catch_up_missing_current_month_affiliate_ledgers` / `settle_previous_month_affiliate_rewards` (L4377); leaderboard `affiliate_leaderboard._compute_affiliate_weekly_rows` (L222) / `_compute_affiliate_monthly_rows` (L353); dashboard `main.py` admin metrics (~L3422), my-stats `vouchers.py` (~L8229); referral XP + `referral_settled` (`current_month_qualified_referral_count` L3466, weekly referral board) | All window on `qualified_events.qualified_at` (KL month) or `referral_events.month_key`. |
| Issuance / retries / approval | `retry_current_month_pending_manual_ledgers`, `approve_affiliate_ledger` (L4538), bulk approve/reject | Approve only issues an existing ledger; it cannot create a qualification. |

Answer to the linkage question: **code → original Telegram recipient → original inviter** is reliable in-repo (`voucher_pools`/`affiliate_ledger`/`new_joiner_claims` → earliest valid `pending_referrals` row). **→ redeemed gaming Account ID → successful redemption timestamp** is *not* reliably available: see §6.

## 2. Rule and lifecycle

Persisted switch: `affiliate_qualification_control` `{_id: "welcome_redemption_v1", mode, launch_cutoff_utc, source_config, unlinked_account_policy, seed_completed_at, ...}`.

| Mode | Legacy settle (`settle_pending_referrals`) | New rule |
|---|---|---|
| no doc / `disabled`, no cutoff | unchanged | off |
| `active`, now < cutoff (scheduled) | unchanged | off |
| `active`, now ≥ cutoff | **parks** rows as `awaiting_redemption` (no channel/engagement check, no XP, no qualification) | on |
| `paused` (after launch) | still parks — never reactivated | off (effects already qualified still drain) |

* The cutoff is immutable once set (`activate` refuses a second one). Rollback = `pause`.
* Pre-launch qualified records are untouched. Any invitee not qualified at the cutoff can only qualify via v1, even if they joined earlier (their earlier revoked/parked row remains the original attribution unless it was revoked as `invalid_ids` / `self_invite` / `already_in_db`).

## 3. Canonical service (`affiliate_qualification.py`)

Evidence (`welcome_redemption_evidence`, unique `(source, source_ref)`, `source_ref = sha256(source|code_hash|account_key)`) is extracted **only from committed upload batches** (batch doc is written after the rows), resumably (`_id` cursor, 5 000 rows/tick). Re-imports are no-ops. Non-Welcome / unknown coupon rows are counted on the batch, not stored.

Validation order (`evaluate_evidence`): voided → `redemption_successful` → `redeemed_at` → account → Welcome campaign filter → recipient (unknown / non-Welcome / not issued / **ambiguous** → reject/review) → conflicting records for the same code → invitee already qualified → attribution (no record / self-referral / referral after redemption / pre-existing user) → ownership (account linked to inviter = self-referral; invitee linked to other accounts or account linked only to other users = review; no linkage = `unlinked_account_policy`, default accept with `ownership_basis=code_recipient_only`). No deposit/turnover/IP/device/Voucher-Hunter gates.

Atomic qualification without transactions:

1. insert `affiliate_account_registry {account_key, state: reserved, evidence_id}` (unique `account_key`);
2. insert `qualified_events` (unique `invitee_id`, partial unique `account_key`);
3. registry → `committed`.

Crash anywhere ⇒ evidence stays `received` with a lease; re-processing finds its own reservation/event and resumes. Every non-qualifying outcome deletes a reservation owned by that evidence; `reconcile()` commits reservations whose event exists and releases those whose evidence can no longer qualify. Processing refuses to run unless every unique index exists (`index_readiness`).

`qualified_events` v1 fields: `invitee_id`, `referrer_id` (frozen from the original referral record), `qualified_at` (attribution time — see §4), `redeemed_at`, `processed_at`, `rule_version`, `account_key`, `evidence_id`, `redemption_source`, `redemption_ref`, `referral_pending_id`, `attribution_basis`, `ownership_basis`. Logs carry evidence ids and masked codes only.

Canonical account identity: `"<namespace>:<account>"`, NFKC + trim, **case and leading zeros preserved**; numeric spreadsheet cells are refused (`account_numeric_coerced` → review) because zeros may already be lost.

## 4. Consumers, months, late data

* `qualified_at` = actual `redeemed_at` (GMT+8 month attribution via existing `_month_window_utc`). A redemption **before the cutoff** that arrives late is clamped to `qualified_at = launch_cutoff` (`attribution_basis=launch_cutoff_clamp`) so no pre-launch month's count changes.
* Effects (`scheduler.apply_welcome_redemption_effects`, lease + `effects_applied_at`): referral XP (`grant_xp` key `ref:<invitee>`), `referral_settled` (occurred_at = `qualified_at`), `referral_award_events` (`qualification_rule_version`), pending row → `awarded`, lock → `awarded`, monthly tier evaluation. If any legacy award event already exists for the invitee, no XP is paid again.
* Because v1 writes the same `qualified_events` / `referral_events` the dashboard, leaderboards, my-stats and tiers already read, they stay consistent without consumer changes; join definitions and tier thresholds are unchanged.
* **Settled-month policy:** if `qualified_at`'s GMT+8 month is earlier than the processing month, the tier evaluator runs for that month with `closed_month_review=True`: any newly reached tier ledger is created `PENDING_REVIEW` with `review_reason=late_redemption_closed_month`, nothing is issued, month-end settle skips it, and it appears in the admin pending list for approve/reject. Existing ledgers are never re-issued (dedup key per user/month/tier).

## 5. Corrections and rollback of source data

* `void-batch`: batch marked `rolled_back_at` (never re-extracted). Evidence seen only in that batch: unprocessed/review → `voided` (reservation released). Already qualified → **not revoked, account not released**; `qualified_events.evidence_status=voided_pending_reconciliation` for manual review. Issued rewards are never touched.
* A re-observation with a different status/time: unprocessed → `pending_review: conflicting_source_observations`; qualified → flagged `disputed_pending_reconciliation`.
* `requeue` re-runs full validation only; there is no force-qualify path. Admin approval only issues ledgers whose counts already justify them.

## 6. Integration blocker (why the rule stays disabled)

`marketing_raw_data` is the only in-repo data with account + coupon + time, but its contract (`marketing_upload.py`) does not make it authoritative redemption evidence:

1. **No redemption status column** — only `campaign_id`, `campaign_name`, `account` are required; nothing distinguishes a successful redemption from an attempt/failure.
2. **Time is ambiguous** — `REDEEM_TIME_ALIASES` includes `claim_time` / `claimed_at`; naive times are parsed as UTC (`_parse_datetime`) although exports are GMT+8; missing times fall back to upload time.
3. **No provider/tenant namespace** for `account`, and XLSX cells may already be numeric (leading zeros lost).
4. **No rollback/correction mechanism** for uploads.
5. UIM's `redeem_account_claim_audit` tab exists but is keyed by month/redeem_account, has no user join, and is not ingested here (`claim_risk_sync.py` docstring).

Needed from the Marketing/UIM data owner: a per-redemption export with `coupon_code`, `account` as **text**, provider/tenant (if multi-tenant), an explicit **success status**, a timestamp with known timezone (ideally a stable redemption/transaction id). Then `configure-source` records the exact column mapping; `preflight` verifies recent committed uploads actually carry those columns.

## 7. Schema / indexes

New collections: `affiliate_qualification_control`, `welcome_redemption_evidence`, `affiliate_account_registry`. Indexes (`affiliate_qualification.INDEX_SPECS`, created only by the migration `--apply`, never at app startup): `qualified_events.uniq_qualified_account_key_v1` (partial: `account_key` is a string — historical rows unaffected), `aq_qualified_effects_pending`; registry `uniq_account_registry_key`, state/evidence; evidence `uniq_evidence_source_ref`, status, code hash, batches; lookup indexes `pending_referrals.aq_pending_by_invitee`, `voucher_pools.aq_voucher_code_lookup`, `new_joiner_claims.aq_new_joiner_claims_code`, `users.aq_users_linked_gaming_accounts`, `marketing_raw_data.aq_marketing_batch_rows`. Existing `qualified_events.uniq_invitee_id` is reused.

## 8. Launch runbook (run inside the app machine, e.g. `fly ssh console`)

```bash
# 0. Ship the code (rule disabled; nothing changes in behaviour).
# 1. Record the authoritative source mapping once §6 is resolved (dry run, then commit):
python scripts/affiliate_qualification_admin.py configure-source \
  --code-column coupon_code --account-column account \
  --redeemed-at-column coupon_redeem_time --status-column <status_col> \
  --success-values <v1,v2> --source-timezone Asia/Kuala_Lumpur \
  --account-namespace <provider> [--campaign-ids <welcome ids>] [--unlinked-account-policy review]
python scripts/affiliate_qualification_admin.py configure-source ... --commit
# 2. Migration: dry run, review masked report (unresolved / conflicting / historical duplicates), then apply.
python scripts/migrate_affiliate_account_dedupe.py --output /tmp/aq_dry.json
python scripts/migrate_affiliate_account_dedupe.py --apply --output /tmp/aq_apply.json   # must print history_unchanged=true
# 3. Preflight must exit 0 (indexes, integration columns, seed marker, consistency).
python scripts/affiliate_qualification_admin.py preflight
# 4. Activate with an explicit future cutoff (dry run, then commit).
python scripts/affiliate_qualification_admin.py activate --cutoff 2026-11-01T00:00:00+08:00
python scripts/affiliate_qualification_admin.py activate --cutoff 2026-11-01T00:00:00+08:00 --commit
```

Verification after the cutoff: `status` shows `legacy_award_allowed=false`, `new_rule_active=true`; logs show `[SCHED][REFERRAL] settle ... parked=N awarded=0` and `[AFF_QUAL][EVIDENCE_DONE]`; `preflight` consistency stays `ok`; `qualified_events` rows with `rule_version` have `effects_applied_at` within one tick.

Rollback / repair: `pause --reason ... --commit` (new qualifications stop; legacy stays off; credited accounts stay credited), fix data (`void-batch`, `requeue`), `resume --commit` (re-runs preflight). Before the cutoff only: `cancel-scheduled --commit`.
