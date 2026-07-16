# Tournament Reward Flow

End-to-end path from a submitted leaderboard to a voucher a user can see and
copy in the Mini App.

## 1. Result intake (`tournament_results`)

A validated, HMAC-authenticated submission is stored as
`status: pending_review`. See `docs/tournament-provider-integration.md`.

## 2. Reward mapping (`reward_config.rules` on the campaign)

Each rule maps an inclusive rank range to a voucher pool:

```json
[
  {"rule_id": "rank-1", "min_rank": 1, "max_rank": 1, "pool_id": "july-tournament-gold", "reward_label": "Champion Reward"},
  {"rule_id": "rank-2-3", "min_rank": 2, "max_rank": 3, "pool_id": "july-tournament-silver", "reward_label": "Top 3 Reward"}
]
```

Rules are validated at save time: no overlapping ranges, every `pool_id`
required, no missing rule ids. A tournament campaign cannot be published
without at least one rule.

## 3. Admin review (`GET /api/admin/tournament-results/{submission_id}`)

Shows winner count, matched/unmatched Telegram users, required vs. available
stock per pool, and any duplicate/version warnings, before an admin acts.

## 4. Approval (`POST /api/admin/tournament-results/{submission_id}/approve`)

`tournament_rewards.approve_submission`:

1. Locks the submission (`status: pending_review|approved|allocating →
   allocating`) — a concurrent second approve call sees a non-matching
   status and is rejected or treated as a no-op, so **double-clicking
   Approve never allocates a second voucher**.
2. Re-validates the campaign and provider are still valid.
3. Idempotently creates one `tournament_rewards` row per winner (unique on
   `(tournament_id, telegram_user_id)` — safe to call again on retry/replay).
4. Recomputes required vs. available stock per pool. If any pool is short
   and the admin didn't pass `allow_partial_allocation: true`, the whole
   approval is rolled back to `pending_review` (all-or-nothing by default).
5. Atomically allocates one voucher code per reward (see below).
6. Sets the submission to `assigned` (all rewards got a code) or
   `out_of_stock` (at least one didn't) — surfaced prominently, never
   silently swallowed.

## 5. Atomic voucher allocation

`tournament_rewards._atomic_allocate_voucher(pool_id, reward)`:

```python
db.campaign_voucher_codes.find_one_and_update(
    {"pool_id": pool_id, "status": "available"},
    {"$set": {"status": "reserved", "reserved_for_reward_id": reward_id, ...}},
    sort=[("created_at", 1), ("_id", 1)],
)
```

This is a single atomic Mongo operation — two workers or two admin clicks
racing for the same reward can never both win it, and a code can never be
handed to two different rewards. If it's already assigned (checked first),
the function returns the existing assignment instead of allocating again.
If no code is available, the reward (and, in aggregate, the submission) is
marked `out_of_stock` and surfaced in the Admin Dashboard's Reward
Allocations tab — never hidden or partially masked.

## 6. Delivery — no Telegram DM, ever

Once a reward is `assigned`, it is simply a document in `tournament_rewards`
keyed by `telegram_user_id`. There is no send queue, no retry-on-DM-failure
logic, and no code path in this feature that calls `sendMessage`/
`safe_send_message` for tournament rewards. The user retrieves it by opening
the Mini App's Campaign Rewards section, which calls `GET
/api/campaign-rewards/me` — see `docs/campaign-rewards.md`.

## 7. Result corrections (versioning)

- Before any voucher in a submission is assigned: a higher `result_version`
  simply supersedes the pending one after admin review.
- After vouchers are assigned: `POST
  /api/admin/tournament-results/{id}/request-correction` marks the
  submission `corrected` and returns `requires_manual_review: true` if any
  reward was already `assigned` — **already-assigned vouchers are never
  auto-revoked or auto-replaced.** An admin must review and act manually
  (e.g. approve a follow-up submission for the delta).
- Same version + different payload: rejected outright (`409`).
- Lower version: rejected outright (`409`).

## 8. Retry / reconciliation

`POST /api/admin/tournament-results/{id}/retry-allocation` re-attempts
allocation only for rewards currently `approved` or `out_of_stock` (e.g.
after uploading more codes to a pool) — it never touches rewards that are
already `assigned`.
