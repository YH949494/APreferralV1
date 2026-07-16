# Campaign Events & Analytics

The canonical, generic, append-only event ledger for every Campaign Centre
flow — campaign lifecycle, public funnel, provider integration, tournament
results, rewards, and pool operations. Not tournament-only: the same
collection and writer serve every campaign type.

Collection: `campaign_events`. Module: `campaign_events.py`.

## Why one ledger

Before this refinement, campaign/subscription events were written ad hoc
from three different places (`campaign_centre.log_funnel_event`,
`subscription_gate._log_event`, a direct insert in
`subscription_verification_api.py`) with inconsistent field names. All
three now go through one writer, `emit_campaign_event` — `log_funnel_event`
is a thin, call-site-compatible wrapper around it, so existing call sites
across `campaign_centre.py`/`tournament_integration.py`/
`tournament_rewards.py`/`campaign_rewards_api.py` didn't need to change
their call shape.

## Event document

```json
{
  "event_id": "ce_...",
  "event_type": "campaign_view",
  "campaign_id": "july-tournament-2026",
  "campaign_type": "tournament",
  "provider_id": "mywin-tournament",
  "telegram_user_id": 123456789,
  "submission_id": null,
  "reward_id": null,
  "pool_id": null,
  "source": "miniapp",
  "status": "success",
  "reason": null,
  "metadata": {},
  "occurred_at": "...",
  "created_at": "..."
}
```

Optional fields are omitted (not stored as `null`) when not applicable, to
keep documents lean.

## Never stored

`emit_campaign_event` strips `bot_token`, `token`, `secret`,
`provider_secret`, `secret_env_var`, `init_data`, `signature`,
`admin_secret`, `password`, `authorization`, `voucher_code`, and `code`
(case-insensitively) from any `metadata` dict passed in, and bounds overall
metadata size (long strings truncated; if the sanitized payload is still
too large, it collapses to `{"_truncated": true}`). No caller in this
codebase passes a full voucher code, raw initData, or a provider secret to
`metadata` — verified by repo-wide search as part of this feature's
production audit. Use `campaign_events.mask_code_suffix(code)` if a caller
ever needs to reference "which code" without the redeemable value.

## Idempotency

Some events must never duplicate on retry/replay:
`leaderboard_received`, `leaderboard_approved`, `leaderboard_duplicate`,
`reward_created`, `voucher_assigned`. These pass a deterministic
`event_id` (`campaign_events.deterministic_event_id(event_type, key)`,
e.g. `deterministic_event_id("reward_created", reward_id)`), and the writer
upserts on `event_id` (`$setOnInsert`) instead of always inserting. Plain
interaction events (views, clicks) are append-only and omit `event_id`.

`campaign_view` (the public `/api/campaigns/active` endpoint) is
unauthenticated by design — public visibility must not require identity —
so it's recorded without a `telegram_user_id`. It's still not spammed: the
Mini App widget fetches that endpoint once per app open, not on a poll
loop, so no additional dedup layer was needed for it.

## Event types

See `campaign_events.EVENT_TYPES` for the full list — campaign lifecycle
(`campaign_created`/`_updated`/`_published`/`_paused`/`_archived`/
`_previewed`), public funnel (`campaign_view`, `campaign_click`,
`subscription_check`/`_pass`/`_fail`, `destination_open`/`_blocked`),
provider integration (`provider_created`/`_updated`/`_activated`/
`_deactivated`/`_signature_failed`/`_nonce_replay`/`_request_rejected`),
tournament result (`leaderboard_received`/`_duplicate`/`_rejected`/
`_approved`/`_correction_requested`/`_version_conflict`), rewards
(`reward_created`/`_rule_matched`/`_rule_unmatched`, `voucher_reserved`/
`_assigned`/`_out_of_stock`, `reward_viewed`, `voucher_copied`,
`reward_expired`), and pool operations (`reward_pool_created`/`_updated`/
`_upload`/`_scope_rejected`/`_allocation_rejected`).

## Admin API

```
GET /api/admin/campaign-events
GET /api/admin/campaign-analytics/summary
```

`list_events` (backing the first endpoint) supports filters — `campaign_id`,
`campaign_type`, `provider_id`, `event_type`, `telegram_user_id`,
`submission_id`, `reward_id`, `pool_id`, `source`, `status`, `date_from`,
`date_to` — with bounded pagination (`page`, `page_size`, capped at 200) and
newest-first sort. `campaign_summary` (backing the second endpoint) is
always filtered by `campaign_id` (required) plus optional date range —
never an unbounded collection scan — and returns:

```json
{
  "campaign_id": "july-tournament-2026",
  "views": 1000, "clicks": 410,
  "subscription_checks": 390, "subscription_passes": 310, "subscription_fails": 80,
  "destination_opens": 295, "leaderboards_received": 1,
  "rewards_assigned": 10, "rewards_viewed": 8, "voucher_copies": 7, "out_of_stock": 0,
  "click_through_rate": 0.41, "subscription_pass_rate": 0.7949, "destination_conversion_rate": 0.7195
}
```

Rates are `numerator / denominator` rounded to 4 decimals, `0.0` when the
denominator is zero (never a division error). Both endpoints require admin
auth (`vouchers.require_admin`), same as every other `/api/admin/*` route.

## Admin Dashboard

Player Campaigns → **Activity Log** and **Rewards** read from this ledger
(via the admin API above) inside the existing dashboard shell — no separate
standalone analytics page.
