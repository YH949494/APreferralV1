# Official-channel rejoin buffer — manual QA notes

Scope: public/pooled voucher claims only. Does not touch Welcome Reward,
personalised vouchers, affiliate rewards, FCFS allocation, or XP/check-in/
referral/campaign/segment eligibility.

Enforcement is controlled by an admin-editable config (`app_settings` doc,
`_id="rejoin_buffer"`) exposed via:
- `GET /v2/miniapp/admin/rejoin-buffer/settings`
- `POST /v2/miniapp/admin/rejoin-buffer/settings` (`mode`, `hours`, `test_user_ids`)
- Admin dashboard → Settings → "Rejoin Buffer" panel (mode dropdown, hours
  input, test user IDs textarea, Save button, current-config chip).

Default settings: `mode=disabled`, `hours=12`, `test_user_ids=[]`. **With the
default config, this feature affects zero users** — `left_official_channel_at`
/ `rejoin_buffer_until` tracking in `main.py` still runs (cheap bookkeeping),
but `check_rejoin_buffer_for_pooled_claim` short-circuits to `ok=True` before
even reading `rejoin_buffer_until`.

Automated coverage: `test_rejoin_buffer.py` covers all three modes, the
settings get/set helpers (defaults, invalid mode/hours fallback, id
normalization/dedup), and the allow/block/expired/missing-user/None-uid cases
within each mode. The scenarios below extend that coverage to the Telegram
handler (`main.py:member_update_handler`), the admin settings endpoints, and
the `/vouchers/claim` HTTP flow, which need a running bot + Mongo + Flask app
context to exercise end-to-end and are best verified manually against staging.

## Admin toggle scenarios

### A. Mode = disabled (default)
- Set `rejoin_buffer_mode=disabled` (or leave unset).
- Leave and rejoin the official channel as any user (buffer still gets written
  to `users.rejoin_buffer_until` by `main.py`, per the tracking rules below).
- Public/pooled claim proceeds normally for that user regardless of
  `rejoin_buffer_until`. Log line `[CHANNEL][REJOIN_BUFFER_SKIP] mode=disabled
  uid=... reason=mode_disabled`.

### B. Mode = test_users_only
- Set `rejoin_buffer_mode=test_users_only`, `rejoin_buffer_test_user_ids=[<your
  Telegram user id>]`.
- As the listed test user: leave + rejoin the channel, then attempt a public
  pooled claim inside the buffer window → blocked with `rejoin_buffer_active`.
- As any other user with an active `rejoin_buffer_until` (e.g. from testing
  under mode `enabled` earlier): claim proceeds normally. Log line
  `[CHANNEL][REJOIN_BUFFER_SKIP] mode=test_users_only uid=... reason=not_test_user`.

### C. Mode = enabled
- Set `rejoin_buffer_mode=enabled`.
- Every user with an active `rejoin_buffer_until` is blocked, not just test users.

### D. Admin dashboard control
- Open Settings → "Rejoin Buffer" panel; confirm it loads the current mode,
  hours, and test user IDs (one per line) via `GET .../rejoin-buffer/settings`.
- Change mode/hours/test IDs, click Save → `POST .../rejoin-buffer/settings`
  persists and the chip updates to reflect the new config immediately.
- Invalid input (mode outside the 3 allowed values, hours <= 0, non-numeric
  test user id) is rejected with 400 and a clear error, config unchanged.
- Only admins (existing `require_admin()` — admin secret or admin session
  cookie) can read or write this config; unauthenticated calls get 401.

## Tracking scenarios (unaffected by mode)

## 1. First-time official channel subscriber
- Fresh `users` doc, never joined `OFFICIAL_CHANNEL_ID` before.
- Trigger a join event for `OFFICIAL_CHANNEL_ID`.
- Expect: `official_channel_first_subscribed_at` set, `rejoined_official_channel_at`
  set, `official_channel_currently_subscribed=true`, **no** `left_official_channel_at`,
  **no** `rejoin_buffer_until`. Log line `[CHANNEL][FIRST_JOIN] uid=... chat_id=...`.
- Public pooled claim: `check_rejoin_buffer_for_pooled_claim` returns `ok=True`
  (no `rejoin_buffer_until` field) → claim proceeds normally, in any mode.

## 2. Long-time subscriber who never left
- `users` doc has `official_channel_first_subscribed_at` set long ago, no
  `left_official_channel_at`, no `rejoin_buffer_until`.
- Public pooled claim proceeds normally (helper returns `ok=True`), in any mode.

## 3. User leaves official channel
- Trigger a leave/kicked event for `OFFICIAL_CHANNEL_ID`.
- Expect: `left_official_channel_at=now`, `official_channel_currently_subscribed=false`.
- `joined_main_at` unchanged (handler never touches it for channel events).
- Log line `[CHANNEL][LEAVE] uid=... chat_id=...`.
- This write happens regardless of the admin mode.

## 4. User rejoins after leaving
- Following scenario 3, trigger a join event for `OFFICIAL_CHANNEL_ID`.
- Expect: `rejoined_official_channel_at=now`, `rejoin_buffer_until = now +
  rejoin_buffer_hours` (admin-configured, default 12h — read from
  `vouchers.get_rejoin_buffer_settings()["hours"]` at rejoin time, falling
  back to 12h only if that lookup errors), `official_channel_currently_subscribed=true`.
- `joined_main_at` unchanged.
- `official_channel_first_subscribed_at` is **not** reset (keeps its original value).
- Log line `[CHANNEL][REJOIN] uid=... chat_id=... buffer_until=...`.
- This write happens regardless of the admin mode; whether it actually blocks
  a claim later depends on `rejoin_buffer_mode` at claim time.

## 5. Public/pooled claim during active buffer (mode=enabled or test_users_only+listed)
- With `rejoin_buffer_until` in the future and the user in scope for the
  active mode, call `POST /vouchers/claim` for a public pooled drop
  (`audience=public`, `type=pooled`, `eligibility.mode=public`).
- Expect HTTP 403, `code=rejoin_buffer_active`, `reason=rejoin_buffer_active`,
  `retry_after_sec` > 0, and the "recently rejoined" message.
- No voucher document is reserved (`_atomic_claim_pooled_voucher` never runs —
  the block happens before `_pooled_claimability_state`/`claim_pooled`).
- No `voucher_claims` ownership row is created (block happens before
  `_acquire_claim_lock`).
- Log line `[CHANNEL][REJOIN_BUFFER_BLOCK] mode=... uid=... buffer_until=...
  retry_after_sec=...` plus the standard `[CLAIM_BLOCK]
  reason=rejoin_buffer_active ...` line.

## 6. Retry after buffer expires
- Advance time past `rejoin_buffer_until` (or wait it out).
- Repeat the same claim request: proceeds normally through the existing FCFS
  `claim_pooled` path, unaffected by this patch.
- On success, response `message` includes the existing pool retention line plus
  the new line: "Stay subscribed to @AdvantPlayOfficial. Leaving and rejoining
  may delay future public voucher claims."

## 7. Welcome Reward claim
- `audience_type` is `new_joiner`/`new_joiner_48h`. The `is_public_pool(voucher)`
  gate in `api_claim` is `False` for these drops (audience != "public"), so
  `check_rejoin_buffer_for_pooled_claim` is never invoked and the new retention
  line is never appended — regardless of `rejoin_buffer_until` or admin mode.

## 8. Personalised voucher claim
- `drop.type == "personalised"` → `_is_public_pooled_drop`/`is_public_pool`
  return `False` (type check fails before audience is even considered), so the
  buffer check and the new retention message are both skipped, in any mode.

## 9. XP / check-in / referral
- None of `xp.py`, `checkin.py`, `referral.py`, campaign/segment logic, or
  Welcome eligibility rules were touched by this patch. `joined_main_at` is
  explicitly preserved in both the leave and (re)join branches added to
  `member_update_handler`. No scheduler job was added anywhere in this patch.
