# Official-channel rejoin buffer — manual QA notes

Scope: public/pooled voucher claims only. Does not touch Welcome Reward,
personalised vouchers, affiliate rewards, FCFS allocation, or XP/check-in/
referral/campaign/segment eligibility.

Automated coverage: `test_rejoin_buffer.py` exercises
`vouchers.check_rejoin_buffer_for_pooled_claim` directly (allow/block/expired/
missing-user/None-uid cases). The scenarios below extend that coverage to the
Telegram handler (`main.py:member_update_handler`) and the `/vouchers/claim`
HTTP flow, which need a running bot + Mongo + Flask app context to exercise
end-to-end and are best verified manually against staging.

## 1. First-time official channel subscriber
- Fresh `users` doc, never joined `OFFICIAL_CHANNEL_ID` before.
- Trigger a join event for `OFFICIAL_CHANNEL_ID`.
- Expect: `official_channel_first_subscribed_at` set, `rejoined_official_channel_at`
  set, `official_channel_currently_subscribed=true`, **no** `left_official_channel_at`,
  **no** `rejoin_buffer_until`. Log line `[CHANNEL][FIRST_JOIN] uid=... chat_id=...`.
- Public pooled claim: `check_rejoin_buffer_for_pooled_claim` returns `ok=True`
  (no `rejoin_buffer_until` field) → claim proceeds normally.

## 2. Long-time subscriber who never left
- `users` doc has `official_channel_first_subscribed_at` set long ago, no
  `left_official_channel_at`, no `rejoin_buffer_until`.
- Public pooled claim proceeds normally (helper returns `ok=True`).

## 3. User leaves official channel
- Trigger a leave/kicked event for `OFFICIAL_CHANNEL_ID`.
- Expect: `left_official_channel_at=now`, `official_channel_currently_subscribed=false`.
- `joined_main_at` unchanged (handler never touches it for channel events).
- Log line `[CHANNEL][LEAVE] uid=... chat_id=...`.

## 4. User rejoins after leaving
- Following scenario 3, trigger a join event for `OFFICIAL_CHANNEL_ID`.
- Expect: `rejoined_official_channel_at=now`, `rejoin_buffer_until = now + REJOIN_CLAIM_BUFFER_HOURS`
  (default 12h, env-overridable), `official_channel_currently_subscribed=true`.
- `joined_main_at` unchanged.
- `official_channel_first_subscribed_at` is **not** reset (keeps its original value).
- Log line `[CHANNEL][REJOIN] uid=... chat_id=... buffer_until=...`.

## 5. Public/pooled claim during active buffer
- With `rejoin_buffer_until` in the future, call `POST /vouchers/claim` for a
  public pooled drop (`audience=public`, `type=pooled`, `eligibility.mode=public`).
- Expect HTTP 403, `code=rejoin_buffer_active`, `reason=rejoin_buffer_active`,
  `retry_after_sec` > 0, and the "recently rejoined" message.
- No voucher document is reserved (`_atomic_claim_pooled_voucher` never runs —
  the block happens before `_pooled_claimability_state`/`claim_pooled`).
- No `voucher_claims` ownership row is created (block happens before
  `_acquire_claim_lock`).
- Log line `[CHANNEL][REJOIN_BUFFER_BLOCK] uid=... buffer_until=... retry_after_sec=...`
  plus the standard `[CLAIM_BLOCK] reason=rejoin_buffer_active ...` line.

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
  line is never appended — regardless of `rejoin_buffer_until` on the user doc.

## 8. Personalised voucher claim
- `drop.type == "personalised"` → `_is_public_pooled_drop`/`is_public_pool`
  return `False` (type check fails before audience is even considered), so the
  buffer check and the new retention message are both skipped.

## 9. XP / check-in / referral
- None of `xp.py`, `checkin.py`, `referral.py`, campaign/segment logic, or
  Welcome eligibility rules were touched by this patch. `joined_main_at` is
  explicitly preserved in both the leave and (re)join branches added to
  `member_update_handler`.

## Env var
- `REJOIN_CLAIM_BUFFER_HOURS` (default `12`) — read once in `main.py` and used
  to compute `users.rejoin_buffer_until` at rejoin time. `vouchers.py` never
  reads this env var; it only compares `now` against the already-stored
  `rejoin_buffer_until` timestamp, so changing the env var only affects
  buffers computed *after* the change.
