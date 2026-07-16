# Voucher Pool Allocation Scope

How Campaign Centre reward allocation and the legacy affiliate/welcome
voucher flow safely share one physical inventory table
(`db.voucher_pools`) without a second inventory collection and without
relying on pool-naming conventions.

## Why explicit scope, not naming

An earlier iteration protected the shared table using pool-id naming
conventions (reserved legacy ids) plus a `pool_source` tag. That's real
protection, but it's still partly operational discipline: nothing stopped
an admin from registering a reward pool that happened to reuse an
in-use `pool_id`. This model replaces the *primary* control with explicit,
server-stamped metadata; naming/`pool_source` remain as defense-in-depth.

## Canonical pool metadata (`voucher_pool_registry`)

One document per managed `pool_id`:

```json
{
  "pool_id": "july-tournament-gold",
  "name": "July Tournament Gold",
  "pool_type": "tournament_reward",
  "allocation_scope": "campaign_rewards",
  "campaign_id": "july-tournament-2026",
  "reward_usage": "rank_reward",
  "reward_metadata": {},
  "status": "active",
  "created_at": "...",
  "updated_at": "..."
}
```

`pool_type` (what kind of reward) and `allocation_scope` (**who may
allocate from it**) are independent — a pool can be `pool_type: "vip"` with
`allocation_scope: "campaign_rewards"` if a VIP-themed tournament campaign
owns it. Supported values:

- `pool_type`: `tournament_reward`, `affiliate`, `welcome`, `vip`,
  `voucher_drop`, `referral`, `cashback`, `other`
- `allocation_scope`: `campaign_rewards`, `affiliate_rewards`,
  `welcome_rewards`, `voucher_drops`, `referral_rewards`, `shared`

Registering a pool with `pool_id` in the legacy reserved set
(`WELCOME`, `T1`–`T5`) is refused outright
(`voucher_pool_service.ReservedPoolIdError`), and re-registering an
existing `pool_id` with a *different* `pool_type`/`allocation_scope` is
refused as `pool_scope_conflict` — use the explicit migration operation
below instead of silently overwriting a live pool's scope.

## Inventory row tagging (`db.voucher_pools`)

Every code row `voucher_pool_service.upload_codes()` inserts is stamped,
**server-side, from the registry record** — never from caller input:

```json
{
  "pool_id": "july-tournament-gold",
  "code": "ABC123",
  "status": "available",
  "pool_type": "tournament_reward",
  "allocation_scope": "campaign_rewards",
  "pool_source": "campaign_centre"
}
```

`upload_codes()` takes no `pool_type`/`allocation_scope` parameter at all —
there is no argument through which a caller could override them. The admin
API additionally rejects a request body that even *contains* a
`pool_type`/`allocation_scope` key (`override_not_allowed`), so an
attempted override is a hard error, not a silently-ignored no-op. Uploads
are also refused if the pool isn't registered (`pool_not_found`) or isn't
`active` (`pool_inactive`).

## Campaign allocation filter

`voucher_pool_service.allocate_voucher()` (used by
`tournament_rewards._atomic_allocate_voucher`) only ever matches rows where:

- `pool_id` matches
- `status == "available"`
- `allocation_scope in ("campaign_rewards", "shared")`
- `pool_type` matches the reward rule's expected type, when the rule
  specifies one (via `pool_source`, kept as an additional check too)
- no existing `issued_for_reward_id`

It never allocates from `affiliate_rewards`, `welcome_rewards`,
`voucher_drops`, or `referral_rewards` scopes, full stop — not "unless an
admin forgot to tag something."

## Legacy affiliate safety (the other direction)

`affiliate_rewards._claim_voucher_from_pool()` (untouched otherwise) was
given one minimal, additive filter clause:

```python
"allocation_scope": {"$nin": ["campaign_rewards", "welcome_rewards", "voucher_drops", "referral_rewards"]}
```

`$nin` naturally matches documents where the field is **absent** — so every
pre-existing legacy affiliate row (which has no `allocation_scope` field at
all) keeps working exactly as before — plus rows explicitly scoped
`affiliate_rewards` or `shared`. Rows explicitly scoped to another
subsystem are never matched, even if a `pool_id` were ever accidentally
reused. This was the only change made to `affiliate_rewards.py` for this
feature.

## Explicit migration operation

`voucher_pool_service.migrate_pool_scope(pool_id, *, pool_type=None,
allocation_scope=None)` (admin endpoint: `POST
/api/admin/reward-pools/{pool_id}/migrate-scope`) is the *only* way to
change an existing pool's type/scope. It always refuses
(`pool_has_inventory`) if the pool already has any code rows — it exists to
fix a mis-registered pool before any codes are uploaded, not to reassign
live inventory between subsystems.

## Backward compatibility

- No destructive migration runs automatically anywhere.
- Existing legacy affiliate/welcome rows (no `allocation_scope`) continue
  working in affiliate flows unmodified.
- Campaign Centre only ever allocates rows it stamped itself.
- A manual, dry-run-by-default backfill script exists at
  `migrations/backfill_voucher_pool_scope.py` if you want to explicitly
  label legacy rows for clarity (it never guesses ownership from pool name
  for non-reserved ids — those are reported as ambiguous and left
  untouched). See the script's docstring for the rollback query.

## Structured error codes

`invalid_pool_type`, `invalid_allocation_scope`, `pool_scope_conflict`,
`pool_has_inventory`, `reserved_pool_id`, `pool_not_found`, `pool_inactive`,
`override_not_allowed`.
