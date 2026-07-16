# External Subscription Verification

For the `external_subscription_verification` campaign type. AP verifies
Telegram identity + official-channel subscription only — **AP never issues
or returns a voucher code here.** The external subscription-voucher website
owns voucher inventory, eligibility, duplicate-claim prevention, issuance,
display, and redemption.

## Endpoint

```
POST /api/integrations/subscription/verify
Content-Type: application/json

{
  "campaign_id": "subscribe-voucher-july",
  "init_data": "<Telegram signed initData from the user's Mini App session>"
}
```

## What AP does

1. Loads the campaign; rejects if missing, wrong type
   (`wrong_campaign_type` unless `type == external_subscription_verification`),
   or not publicly active (`campaign_not_active` — same
   `campaign_centre.is_publicly_active` check used everywhere else: live,
   in schedule, destination ready, provider active).
2. Verifies the Telegram `init_data` HMAC signature and freshness
   (`vouchers.verify_telegram_init_data` — the same canonical verifier used
   by the rest of the Mini App backend).
3. Extracts the Telegram user id from the verified payload — **never from a
   request field**, and never keyed by username.
4. Runs the shared subscription gate
   (`subscription_gate.verify_campaign_subscription`) against the campaign's
   configured channel.
5. Logs the check (`campaign_events` collection) and returns an authoritative
   result.

## Response

```json
{
  "ok": true,
  "campaign_id": "subscribe-voucher-july",
  "telegram_user_id": 123456789,
  "subscribed": true,
  "checked_at": "2026-07-15T09:00:00Z"
}
```

Failure responses use `{"ok": false, "code": "..."}` with HTTP 400/401/404/429.
Codes: `missing_fields`, `campaign_not_found`, `wrong_campaign_type`,
`campaign_not_active`, `init_data_invalid:<reason>`, `invalid_user`,
`rate_limited`.

## Protections

- **Rate limiting**: per-client-IP sliding window (20 requests / 60s by
  default; tune via the module constants if the external site needs a
  different ceiling for a specific integration).
- **Replay/staleness**: inherited from `verify_telegram_init_data`'s 24h
  `auth_date` freshness check — the same rule the rest of the Mini App
  enforces on initData.
- **Campaign active-state validation**: enforced on every call, not cached.
- **No username-based identity**: the response is always keyed by the
  verified Telegram user id.
- **No voucher codes**: this endpoint has no code path that reads from or
  writes to any voucher/pool collection.
