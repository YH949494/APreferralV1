# Tournament Provider Integration

Audience: the team building the external tournament website.

## What AP owns vs. what you own

AP owns: campaign gating (identity + channel subscription), the Telegram
deep-link handoff, leaderboard result intake, voucher allocation from
AP-owned pools, and reward delivery inside the AP Mini App.

You own: gameplay, player accounts, mapping the Telegram UID we send you to
your own player account, tournament registration, scores, multi-account
controls, final leaderboard generation, and submitting that leaderboard to
us. **You never send us voucher codes, and we never send voucher codes to
you.**

## Phase 1: UID deep link (current implementation)

When a user taps "Play Tournament" in the Mini App, AP:

1. Derives the Telegram user id from the **authenticated Mini App session**
   of the user tapping the button (never from a client-supplied `uid`) via
   the shared `miniapp_identity.resolve_authenticated_telegram_user_id()`
   resolver.
2. Re-confirms the campaign is publicly active and the user is subscribed to
   the configured official channel (if required).
3. Builds your destination URL using your provider's configured `url_mode`
   and opens it as a Telegram Web App / external URL.

Three URL modes, configured per provider in the Admin Dashboard:

| Mode              | Example                                              |
|-------------------|-------------------------------------------------------|
| `query_parameter`  | `https://domain.com/play?uid={telegram_uid}`          |
| `path_parameter`   | `https://domain.com/{telegram_uid}`                    |
| `custom_template`  | `https://domain.com/play/{telegram_uid}/campaign/{campaign_id}` |

No Telegram initData, signed token, or Login Widget is generated or required
for Phase 1 — the Telegram UID in the URL is the whole handoff. It is your
website's responsibility to map that UID to your own player account, prevent
duplicate registrations, and maintain your own session.

## Submitting the final leaderboard

```
POST /api/integrations/tournaments/results
Headers:
  X-Provider-Id: <your provider_id>
  X-Timestamp:   <unix seconds, request time>
  X-Nonce:       <random string, unique per request>
  X-Signature:   hex(HMAC_SHA256(secret, "{timestamp}.{nonce}.{raw_body}"))
Content-Type: application/json
```

`secret` is the value AP has stored in the environment variable your
provider record's `secret_env_var` points to — it is never returned by any
AP API. The signature input is the exact three fields joined by `.`, where
`raw_body` is the exact bytes of the HTTP request body (compute the
signature **before** any JSON re-serialization on either side).

### Payload

```json
{
  "campaign_id": "july-tournament-2026",
  "tournament_id": "mywin-july-2026",
  "result_version": 1,
  "finalized_at": "2026-07-31T16:00:00Z",
  "winners": [
    {"rank": 1, "telegram_user_id": 123456789, "score": 18500},
    {"rank": 2, "telegram_user_id": 987654321, "score": 17120}
  ]
}
```

Validation (any failure returns `400`/`401`/`404`/`409` with a `code`):

- Campaign must exist, be `type=tournament`, be `status in {live, paused,
  ended}`, have `destination.ready = true`, and its `destination.provider_id`
  must equal your `X-Provider-Id`.
- `tournament_id` non-empty, `result_version` a positive integer,
  `finalized_at` present.
- `winners` non-empty, at most `TOURNAMENT_MAX_WINNERS` (default 500) rows.
- Each winner: positive integer `rank`, integer `telegram_user_id`, numeric
  `score`; no duplicate `telegram_user_id` or duplicate `rank`; every `rank`
  must fall inside one of the campaign's configured reward rank ranges.
- The campaign must have reward rules configured before you can submit.

### Response

```json
{
  "status": "ok",
  "submission_id": "tr_...",
  "status_value": "pending_review",
  "winner_count": 10,
  "matched_users": 9,
  "unmatched_users": 1,
  "duplicate": false
}
```

### Idempotency / versioning rules

Uniqueness is `(provider_id, tournament_id, result_version)`.

- Re-submitting the **same** `result_version` with the **identical** payload
  returns the existing submission with `"duplicate": true` — no new reward
  or voucher is ever created.
- Re-submitting the same `result_version` with a **different** payload is
  rejected with `409 conflict_same_version_different_payload` — bump the
  version instead.
- Submitting a **lower** `result_version` than one already on file is
  rejected with `409 lower_result_version_rejected`.
- A **higher** version is accepted as a new pending submission. If the prior
  version already had vouchers assigned, AP requires manual admin review
  before any reward changes — see `docs/tournament-reward-flow.md`.

## Status API

```
GET /api/integrations/tournaments/results/{submission_id}
```
(Same HMAC headers, signed over an empty body.) Returns:

```json
{
  "submission_id": "tr_...",
  "status": "assigned",
  "winner_count": 10,
  "assigned_count": 10,
  "out_of_stock_count": 0,
  "pending_review_count": 0
}
```

**Voucher codes are never included in this or any provider-facing
response.**

## Error codes

`missing_headers`, `unknown_provider`, `inactive_provider`,
`invalid_timestamp`, `stale_timestamp`, `provider_secret_not_configured`,
`invalid_signature`, `nonce_replayed`, `invalid_json`, `campaign_not_found`,
`campaign_not_tournament`, `campaign_not_active`, `destination_not_ready`,
`provider_mismatch`, `missing_tournament_id`, `invalid_result_version`,
`missing_finalized_at`, `empty_winners`, `too_many_winners`,
`reward_rules_not_configured`, `invalid_winner_row`, `invalid_rank`,
`duplicate_telegram_uid`, `duplicate_rank`, `winner_rank_outside_reward_rules`,
`conflict_same_version_different_payload`, `lower_result_version_rejected`.
