# Campaign Rewards (Mini App)

Where users retrieve tournament vouchers AP has allocated to them. No
Telegram DM delivery exists for this feature — the Mini App is the only
retrieval path.

## How users retrieve tournament vouchers

1. AP's admin approval flow (`docs/tournament-reward-flow.md`) atomically
   assigns a voucher code to a `tournament_rewards` row keyed by
   `telegram_user_id`.
2. The reward sits there — visible to nobody — until the user opens the Mini
   App. There is no send queue and nothing needs to happen for the reward to
   "arrive"; it already exists server-side.
3. The Mini App widget (`static/campaign-centre-widget.js`) calls `GET
   /api/campaign-rewards/me` on load and renders a "Campaign Rewards"
   section only if the response contains at least one reward. If there are
   none, the section is not rendered at all — no empty placeholder.

## Authenticated reward ownership

`GET /api/campaign-rewards/me` derives the Telegram user id from verified
Mini App initData (`vouchers.verify_telegram_init_data`), exactly like every
other identity-sensitive endpoint in this feature. It **never** accepts a
`user_id` or `uid` query parameter for ownership purposes — even if one is
present in the query string, it is ignored.

```json
{
  "ok": true,
  "rewards": [
    {
      "reward_id": "rw_...",
      "campaign_id": "july-tournament-2026",
      "campaign_name": "July Tournament",
      "tournament_id": "mywin-july-2026",
      "rank": 1,
      "reward_label": "Champion Reward",
      "voucher_code": "ABC123XYZ",
      "assigned_at": "...",
      "expires_at": null,
      "status": "assigned"
    }
  ]
}
```

## Visibility rules

A reward is only returned when **all** of:

- `telegram_user_id` matches the verified caller.
- `status == "assigned"`.
- `expires_at` is absent, or in the future.

Rewards in `pending_review`, `approved`, `allocating`, `out_of_stock`,
`rejected`, or `expired` are never shown to the user — those states are
admin/internal only (visible in the Admin Dashboard's Reward Allocations
tab).

## Voucher copy flow

`POST /api/campaign-rewards/{reward_id}/copy` and `.../view` both re-check
that `reward.telegram_user_id` equals the verified caller before touching
anything (`404` otherwise — never leaks whether the reward exists for
someone else). They only ever write telemetry timestamps
(`first_viewed_at`, `copied_at`); they never change `status`,
`voucher_code`, or `telegram_user_id` — **viewing/copying can never change
ownership or re-trigger allocation.**

## Expiry handling

`expires_at` is optional per reward (set via
`CAMPAIGN_REWARD_DEFAULT_EXPIRY_DAYS`, 0 = never, or per-pool business
rules). Once passed, the reward silently drops out of
`/api/campaign-rewards/me` — the voucher code itself remains in
`campaign_voucher_codes` for admin reconciliation/audit but is no longer
user-visible.

## No Telegram DM delivery

There is no code path anywhere in `tournament_rewards.py` or
`campaign_rewards_api.py` that calls `sendMessage`/`safe_send_message`. This
is intentional per the product requirement — do not add one.

## Audit and telemetry

Every reward creation, voucher reservation/assignment/out-of-stock event,
and view/copy telemetry hit is logged to the `campaign_events` collection
(`reward_created`, `voucher_reserved`, `voucher_assigned`,
`voucher_out_of_stock`, `reward_viewed`, `voucher_copied`) for the Admin
Dashboard's Activity Logs / analytics.
