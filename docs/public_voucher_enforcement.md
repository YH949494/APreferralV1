# Public voucher enforcement: region bypass and multi-account restriction

Scope: `vouchers.py` public-pool path only (`eligibility.mode` public, or segment-targeted `user_id` drops). Welcome / VIP / affiliate /
personalised flows, voucher inventory accounting, cooldowns and rate limits are untouched. Nothing here changes a VH probability, an
admin-configured `voucher_risk_modifiers` / `pool_probabilities` value, or the New Player boost.

## Root cause

`_pooled_claimability_state` ran the probability gate only when `pools == ["public"]`. `get_claimable_pools` returns `["my", "public"]` for a
Malaysia user, so every MY user skipped the gate, including after the MY reserve was empty and the claim fell through to public inventory.
`claim_pooled` had no gate of its own, so the pre-check was the only enforcement point and a concurrent drain of the MY pool between the
pre-check and the allocation also bypassed it. (Regions with other regional pools, e.g. `["sg", "public"]`, were bypassed the same way;
`claim_pooled` can only allocate `my` and `public`, so for them the regional pool was never claimable.)

## Behaviour now

* One decision function, `evaluate_public_pool_gate`, used at **both** the claimability pre-check and the allocation point
  (`claim_pooled(public_gate=...)`, immediately before any attempt on the public pool).
* Region-independent. A user whose claim is served from a non-empty MY reserve is **not** gated (existing reserve behaviour, no roll consumed);
  the moment the claim can reach public inventory (reserve empty, or drained concurrently) the same gate applies.
* The roll is still persisted once per `(user, drop)`, so pre-check + allocation never re-roll. Allocation-time denial raises `NotEligible`
  (HTTP 403 `not_eligible`, claim ownership released, nothing allocated); pre-check denial keeps today's opaque 429 `traffic_limited`.
* **Multi-account hard block** (`multi_account_risk=true`): refused at the backend whenever the claim would use the public pool and the user is
  **outside the New Player exception**. Evaluated at assignment (recorded as `multi_account_hard_block_applied`, probability `0`, source suffix
  `+multi_account_hard_block`) and re-read live from `users.multi_account_risk` on every claim, so a flag set after the assignment still blocks.
  The segment base probability, the configurable multiplier and the audit fields are recorded unchanged.
* Behavioral Voucher Hunters keep their admin-configurable probability band (VH 10%; VH + multi-account 5-10%) and are **not** hard-blocked.

### Decisions you need to know about

1. **New Player exception as implemented.** The only live trigger is `users.for_bot_segment` in `{new_user, new_joiner}` with fewer than 3 prior
   assignments. The `player_age_type == "new_player"` override exists in `assign_public_pool_access_once` but is **unreachable on the live
   claim path**: `check_public_pool_access` does not pass `user_doc`, so `player_age_type` / `backend_segment` are never read. Per instructions
   this was not changed (pinned by `test_live_gate_still_does_not_read_player_age_type`). Wiring it in would change who is a New Player.
2. **VH + multi-account is not hard-blocked** (instructions: no hard block for VH, preserve VH calculations and admin values), so a VH player who
   is also multi-account (5-10%) is less restricted than a high-value multi-account player (0%). To close that inversion set
   `PUBLIC_VOUCHER_MULTI_ACCOUNT_BLOCK_INCLUDE_VH=true`. This is a policy call, not a code defect.
3. With the block on, `multi_account_only_modifier_pct` (default x0.25) is bypassed for non-VH, non-New-Player multi-account users. It still
   applies (and is still recorded) with the block off.

## Flags (defaults shown; none was changed in any environment)

| Env var | Default | Meaning |
|---|---|---|
| `PUBLIC_VOUCHER_MULTI_ACCOUNT_BLOCK_ENABLED` | `true` | Kill switch for the multi-account hard block. `false` = probability-modifier-only behaviour. The region-bypass fix has no flag (it is a bug fix). |
| `PUBLIC_VOUCHER_MULTI_ACCOUNT_BLOCK_INCLUDE_VH` | `false` | `true` also hard-blocks behavioral VH + multi-account. |

## Rollout and rollback

1. Deploy with `PUBLIC_VOUCHER_MULTI_ACCOUNT_BLOCK_ENABLED=false` first if you want to observe the region fix alone: grep
   `[PUBLIC_POOL][PROB_DECISION]` for Malaysia users (previously absent) and `[PUBLIC_POOL][ALLOCATION_GATE_DENY]`.
2. Remove the override to enable the hard block; expect `multi_account_hard_block_applied=true` assignments only for flagged, non-boosted users.
3. Rollback: set `PUBLIC_VOUCHER_MULTI_ACCOUNT_BLOCK_ENABLED=false` (no redeploy of code), or revert the PR. Assignment documents written while the block
   was on keep their stored (zero-probability) decision for that drop; they are per-drop and do not affect later drops.

Expect a visible effect: Malaysia users with an empty MY reserve now get the same denial rate as everyone else (VH ~90% denied), and MY users
who previously always reached public inventory will see more `traffic_limited` responses. That is the fix, not a regression.
