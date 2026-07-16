# Tournament Reward Flow

End-to-end path from a submitted leaderboard to a voucher a user can see and
copy in the Mini App. This is the tournament-specific instance of the
generic Campaign Rewards flow — see `docs/campaign-rewards.md` for the
category-agnostic parts.

## 1. Result intake (`tournament_results`)

A validated, HMAC-authenticated submission is stored as
`status: pending_review`. See `docs/tournament-provider-integration.md`.

## 2. Rule-based reward mapping (`reward_config.rules` on the campaign)

Reward rules are evaluated by the generic `reward_engine.py` module, not
hardcoded rank logic — a rule is a `condition_type` + `params` + `pool_id`:

```json
[
  {"rule_id": "rank-1", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 1}, "pool_id": "july-tournament-gold", "reward_label": "Champion Reward"},
  {"rule_id": "rank-2-3", "condition_type": "rank", "params": {"min_rank": 2, "max_rank": 3}, "pool_id": "july-tournament-silver", "reward_label": "Top 3 Reward"},
  {"rule_id": "participation", "condition_type": "participation", "params": {}, "pool_id": "july-tournament-consolation", "reward_label": "Thanks for Playing"}
]
```

Supported `condition_type` values today: `rank`, `participation`,
`score_threshold`, `referral_count`, `first_play`, `vip`, `campaign_tag`.
`tournament_rewards._create_or_confirm_rewards` builds a context per winner
(`{"rank": ..., "score": ...}`) and calls `reward_engine.match_rule(rules,
context)` — the **first** rule (in list order) whose condition matches
wins. Adding a new rank bracket, score tier, or pool is a data change, not a
backend code change; adding a wholly new *condition type* (e.g. a future
"streak" condition) is the only case that needs a small new evaluator
function in `reward_engine.py`.

Rules are structurally validated at save time
(`reward_engine.validate_reward_rules`): every rule needs a unique
`rule_id`, a valid `condition_type`, and a `pool_id`; `rank` rules may not
overlap. A tournament campaign cannot be published without at least one
rule.

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
3. Idempotently creates one `campaign_rewards` row per winner (`category:
   "tournament"`, unique on `(tournament_id, telegram_user_id)` — safe to
   call again on retry/replay).
4. Recomputes required vs. available stock per pool (via
   `voucher_pool_service.pool_stock`, reading the shared Voucher Centre
   inventory). If any pool is short and the admin didn't pass
   `allow_partial_allocation: true`, the whole approval is rolled back to
   `pending_review` (all-or-nothing by default).
5. Atomically allocates one voucher code per reward (see below).
6. Sets the submission to `assigned` (all rewards got a code) or
   `out_of_stock` (at least one didn't) — surfaced prominently, never
   silently swallowed.

## 5. Atomic voucher allocation — the shared Voucher Centre inventory

Reward voucher codes are **not** a second inventory. `voucher_pool_service.
allocate_voucher()` atomically claims one row from `db.voucher_pools` — the
same collection the existing affiliate/welcome-tier voucher pools already
use — via a single Mongo `find_one_and_update`:

```python
db.voucher_pools.find_one_and_update(
    {"pool_id": pool_id, "status": "available", ...},
    {"$set": {"status": "issued", "issued_to_user_id": uid,
              "issued_at": now, "issued_for_reward_id": reward_id}},
    sort=[("_id", 1)],
)
```

This mirrors `affiliate_rewards._claim_voucher_from_pool`'s atomic pattern
against the same table, keyed by `issued_for_reward_id` instead of
`issued_for_ledger_id` so the two consumers never contend over each other's
semantics. Two workers or two admin clicks racing for the same reward can
never both win it, and a code can never be handed to two different rewards.
If it's already assigned (checked first), the function returns the existing
assignment instead of allocating again. If no code is available, the reward
(and, in aggregate, the submission) is marked `out_of_stock` and surfaced in
the Admin Dashboard's Rewards tab — never hidden or partially masked.

A thin **pool registry** (`voucher_pool_registry` collection, via
`voucher_pool_service.register_pool`) tags a `pool_id` with `pool_type`
(`tournament_reward` / `affiliate` / `welcome` / `vip` / `voucher_drop` /
`referral` / `cashback` / `other`), `allocation_scope` (see below),
`campaign_id`, `reward_usage`, and `reward_metadata` — this is catalog
metadata only, never a copy of the code inventory. See
`docs/voucher-pool-allocation-scope.md` for the full isolation model.

## 6. Delivery — no Telegram DM, ever

Once a reward is `assigned`, it is simply a document in `campaign_rewards`
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
