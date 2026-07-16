# Campaign Rewards

A **generic reward centre** inside the Mini App — where users retrieve any
campaign-issued voucher, not only tournament prizes. No Telegram DM delivery
exists for this feature — the Mini App is the only retrieval path.

## Generic by design

Reward instances live in one collection, `campaign_rewards`, with a
`category` field: `tournament`, `referral`, `lucky_draw`, `mission`,
`cashback`, `welcome`, or `other`. Today only the tournament approval flow
(`tournament_rewards.py`) writes rows here, but the API and data model don't
need to change to support a future reward-producing campaign type — that
type's admin flow just writes a `campaign_rewards` document with its own
`category`/`reward_rule_id`/`pool_id`.

## How users retrieve rewards

1. An admin approval flow (e.g. `docs/tournament-reward-flow.md`) atomically
   assigns a voucher code to a `campaign_rewards` row keyed by
   `telegram_user_id`, via `voucher_pool_service.allocate_voucher` — pulling
   from the **same Voucher Centre inventory table** (`db.voucher_pools`)
   every other reward pathway in this app already uses. There is no second
   voucher inventory anywhere in this feature.
2. The reward sits there — visible to nobody — until the user opens the Mini
   App. There is no send queue and nothing needs to happen for the reward to
   "arrive"; it already exists server-side.
3. The Mini App widget (`static/campaign-centre-widget.js`) calls `GET
   /api/campaign-rewards/me` on load and renders a "Campaign Rewards"
   section only if the response contains at least one reward. If there are
   none, the section is not rendered at all — no empty placeholder.

## Ownership — the authenticated Mini App session

`GET /api/campaign-rewards/me` derives the Telegram user id via
`miniapp_identity.resolve_authenticated_telegram_user_id()` — the single
shared resolver every identity-sensitive endpoint in this feature uses. It
**never** accepts a `user_id` or `uid` query parameter for ownership
purposes — even if one is present in the query string, it is ignored.

Under the hood this still verifies Telegram's signed `initData` (there is no
separate server-side session for regular Mini App users anywhere in this
codebase — initData *is* the session, reissued by Telegram on every launch),
but that verification now lives in one shared module instead of being
hand-rolled per feature. If the app ever grows a different session
mechanism, only `miniapp_identity.py` needs to change — every caller of
`resolve_authenticated_telegram_user_id()` keeps working unmodified. This
does **not** weaken the "never trust a raw uid" guarantee; it only moves
where the verification code lives.

```json
{
  "ok": true,
  "rewards": [
    {
      "reward_id": "rw_...",
      "category": "tournament",
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
admin/internal only (visible in the Admin Dashboard's Rewards tab).

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
`db.voucher_pools` for admin reconciliation/audit but is no longer
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
Dashboard's Activity Log / analytics.
