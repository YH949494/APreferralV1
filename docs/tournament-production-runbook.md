# Tournament Production Runbook

Operational steps for running the Campaign Centre / tournament integration
in production, including the "website isn't ready yet" state and the exact
switch-over once it is.

## 1. Create a provider

Admin Dashboard → **Player Campaigns** (sidebar module; the pre-existing
"Campaign Centre" module is the unrelated segment-audience marketing tool)
→ Providers → Create Provider.

- `provider_id`: short slug, e.g. `mywin-tournament`.
- `type`: `tournament`.
- `base_url`: leave blank or a placeholder until the real site exists.
- `url_mode`: `query_parameter`, `path_parameter`, or `custom_template`
  (see `docs/tournament-provider-integration.md`).
- `secret_env_var`: the environment variable name (not the value) that will
  hold the HMAC secret, e.g. `CAMPAIGN_PROVIDER_SECRET_MYWIN`. Set the
  actual value via your deployment platform's secret manager — never in the
  database or in any admin API response.

New providers are always created **inactive**. Leave it that way until the
website is ready.

## 2. Create a campaign

Player Campaigns → Campaigns → Create Campaign. It starts as `draft`.

Fill in `type: tournament`, the linked `provider_id`, a `destination.path`,
and the official channel (`telegram.channel_username`). Leave
`destination.ready = false` while the site is still being built.

## 3. Register reward pools

Player Campaigns → Rewards → Register Pool, one per reward tier (e.g.
`july-tournament-gold`, `-silver`, `-standard`), each linked to the
`campaign_id`. This does **not** create a new voucher inventory — it tags a
`pool_id` with catalog metadata (`pool_type`, `campaign_id`) in a small
registry; the codes themselves live in the same `db.voucher_pools`
collection the Voucher Centre already owns.

## 4. Upload voucher codes

Pool → Upload Codes (paste one code per line). Duplicate codes within a pool
are silently skipped and reported back (`skipped_duplicates`). These rows
land directly in the shared Voucher Centre inventory table.

## 5. Configure reward rules

On the campaign, set `reward_config.rules` — a rule-based reward engine, not
hardcoded rank logic, so this also supports non-rank triggers
(`participation`, `score_threshold`, `referral_count`, `first_play`, `vip`,
`campaign_tag`) without any backend change:

```json
[
  {"rule_id": "rank-1", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 1}, "pool_id": "july-tournament-gold", "reward_label": "Champion Reward"},
  {"rule_id": "rank-2-3", "condition_type": "rank", "params": {"min_rank": 2, "max_rank": 3}, "pool_id": "july-tournament-silver", "reward_label": "Top 3 Reward"}
]
```
`rank` ranges must not overlap; every `pool_id` must exist. At least one
rule is required before the campaign can be published.

## 6. Keep the campaign admin-only before the website is ready

As long as `status` is `draft`/`scheduled` **or** `destination.ready` is
`false` **or** the provider is inactive, `GET /api/campaigns/active` will
never return the campaign and `POST /api/integrations/tournaments/results`
will reject any submission for it. Use **Preview** (Campaigns tab) to check
the card and visibility explanation at any time without publishing.

## 7. Mark the destination ready

Once the tournament website is live and its UID deep-link handoff works,
edit the campaign: set `destination.path` to the real path and
`destination.ready = true`.

## 8. Publish

Update the provider: set the real `base_url`, set the environment variable
named in `secret_env_var` to the real shared HMAC secret, then **Activate**
the provider. Set the campaign's `schedule.starts_at`/`ends_at`, then
**Publish** (`status → live`). Publishing is blocked if reward rules are
missing, the destination isn't ready, or the provider is inactive.

**No redeploy is needed for any of this** — provider URL/secret and
campaign schedule/readiness are all data changes, not code changes.

## 9. Receive final results

The provider posts to `POST /api/integrations/tournaments/results` (HMAC
signed). It lands as `pending_review` in the Tournament Results tab.

## 10. Review and approve

Open the submission, check matched/unmatched users and stock-per-pool, then
**Approve**. Approval is idempotent — a second click (or a retried request)
never double-allocates. If stock is short, approval is blocked by default
(all-or-nothing); pass "allow partial allocation" only if that's an
intentional business call for this run.

## 11. Handle out of stock

Rewards tab shows `out_of_stock` rewards clearly. Upload more
codes to the relevant pool, then **Retry Allocation** on the submission —
this only touches `out_of_stock`/`approved` rewards, never ones already
`assigned`.

## 12. Handle a result correction

If the provider needs to resubmit a corrected leaderboard: they submit a
**higher** `result_version` for the same `tournament_id`. If no vouchers
were assigned yet, the new version simply supersedes the old one on review.
If vouchers were already assigned, use **Request Correction** — it flags
`requires_manual_review: true` and never silently touches already-assigned
vouchers; handle the delta manually (e.g. approve top-up rewards for newly
qualifying winners).

## 13. Disable a compromised provider

Providers tab → **Deactivate**. This immediately (a) removes every campaign
linked to that provider from `/api/campaigns/active`, and (b) makes every
future `POST /api/integrations/tournaments/results` for that provider return
`401 inactive_provider`. No campaign edits needed.

## 14. Rotate a provider secret

Update the value of the environment variable named in the provider's
`secret_env_var` in your deployment platform, then redeploy/restart the
process picking up the new env var. No database change needed. Coordinate
the cutover time with the provider team — old-secret-signed requests will
start failing (`invalid_signature`) the moment the new value is live.

## 15. Pause a campaign

Campaigns tab → **Pause** (`status → paused`). Immediately removes it from
public visibility. Tournament result submissions are still accepted while
`paused` (to allow final-leaderboard processing after a tournament ends) but
rejected once `archived`.

## 16. Reconcile reward records

Rewards tab supports filtering by campaign/tournament/status/pool
and searching by Telegram user id, showing rank, reward rule, assigned
voucher, and viewed/copied telemetry for manual reconciliation.
