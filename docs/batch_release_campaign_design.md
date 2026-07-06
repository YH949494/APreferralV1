# APReferral Bot — Phase 3: Batch Release Campaign System

Status: **DESIGN ONLY — not implemented.**

Scope guardrail (unchanged from Phase 2): this system compiles down into the
primitives `vouchers.py` already exposes (`drops` documents + `vouchers`
documents). It does **not** modify the claim engine, pooled-claim FCFS logic,
anti-abuse checks, or affiliate settlement. It builds directly on the Phase 2
Campaign Builder design (`docs/campaign_builder_design.md`) — same
`campaigns`/`drops` collections, same additive-field philosophy, same
`compile_campaign()` entry point — and extends it with a real batch/drip
**scheduler** and the release-type vocabulary requested for Phase 3.

Reused as-is, untouched (see Phase 2 doc's "Reuse Summary" for the full
list): `claim_voucher_for_user`, `claim_pooled`, `claim_personalised`,
`api_claim`, the kill switch / cooldown / dedup anti-abuse stack,
`is_drop_allowed`, `is_user_eligible_for_drop`, `get_claimable_pools`,
`admin_create_drop`'s insert primitive, `admin_add_codes`,
`admin_drop_actions`, `reconcile_drop_statuses`, and all affiliate
settlement code in `affiliate_rewards.py`/`scheduler.py`.

---

# Parent Campaign Model

The parent stays the existing `campaigns` collection (`campaigns.py`) —
Phase 3 adds the batch-specific `release_config` shape on top of Phase 2's
`release_style`/`release_config` fields. No new top-level collection.

Additive fields on `campaigns` (all optional):

| Field | Type | Purpose |
|---|---|---|
| `release_style` | string | `"all_at_once"` \| `"batch_release"` \| `"drip_release"` \| `"manual_release"` — matches the Admin UI radio group verbatim. |
| `release_type` | string | Only meaningful when `release_style="batch_release"`: `"every_x_minutes"` \| `"hourly"` \| `"daily"` \| `"weekly"` \| `"manual"` \| `"custom_schedule"`. |
| `release_config` | dict | Type-specific params — see table below. |
| `total_voucher_count` | int | Total codes/allocations for the whole campaign (e.g. 500). |
| `batch_size` | int | Vouchers per child drop (e.g. 50). `total_voucher_count / batch_size` (ceil) = planned batch count. Last batch takes the remainder if it doesn't divide evenly. |
| `campaign_status` | string | `"draft"` → `"scheduling"` → `"releasing"` → `"completed"` \| `"paused"` \| `"cancelled"`. Orchestration-only; never read by the claim path. |
| `compiled_drop_ids` | list[str] | Same field Phase 2 already defines — the ordered list of child `drops._id`s this campaign has produced so far. |
| `batches_planned` | int | `ceil(total_voucher_count / batch_size)`, computed once at compile time. |
| `batches_released` | int | Count of child drops actually created so far (for open-ended/custom schedules where not all batches are pre-materialized). |

`release_config` per `release_type`:

| `release_type` | `release_config` shape |
|---|---|
| `every_x_minutes` | `{interval_minutes: int}` |
| `hourly` | `{interval_hours: int (default 1)}` |
| `daily` | `{time_of_day: "HH:MM", timezone: "Asia/Kuala_Lumpur"}` |
| `weekly` | `{day_of_week: 0-6, time_of_day: "HH:MM", timezone}` |
| `manual` | `{}` — no timing; admin triggers each batch explicitly. |
| `custom_schedule` | `{cron_expression: str}` **or** `{explicit_times: [datetime, ...]}` — admin supplies either a cron string (reuses `CronTrigger` semantics already used in `main.py`) or an explicit list of release timestamps. |

The parent campaign **never holds vouchers directly**. It is purely an
orchestration record: "500 vouchers, 50/hour" plus bookkeeping of which
child drops have been produced. All claim-affecting state (`public_remaining`,
`status`, `startsAt`/`endsAt`) lives only on the child drops, exactly as
today.

---

# Child Drop Model

Child drops are ordinary `drops` documents (`vouchers.py:6501-6726` schema,
unchanged) plus the additive lineage fields Phase 2 already proposed, plus
two new batch-position fields:

| Field | Type | Purpose |
|---|---|---|
| `campaign_id` | ObjectId (string) | Back-reference to the parent campaign (Phase 2 field, reused). |
| `campaign_template` | string | Denormalised template id (Phase 2 field, reused). |
| `campaign_batch_index` | int | 1-based position (`drop_3` → `3`). |
| `campaign_batch_total` | int \| null | `batches_planned` at the time this drop was created; `null` for open-ended custom schedules where the total isn't fixed in advance. |
| `release_mode` | `"auto"` \| `"manual"` | Phase 2 field, reused unchanged — `"manual"` for the Manual release style and for each not-yet-released batch of a Manual-style campaign. |
| `manuallyReleasedAt` | datetime \| null | Phase 2 field, reused unchanged. |

Naming: internal `_id` is unchanged (ObjectId); the human-facing `name`
field is generated as `"<campaign name> — Batch <index>/<total>"` (or
`"Batch <index>"` when total is unknown), matching the `drop_1, drop_2, ...`
convention from the spec at the display layer only — no new identifier
scheme is introduced at the data layer.

Each child drop is claimed exactly like any pooled drop today — its own
`public_remaining`/`my_remaining`, its own `startsAt`/`endsAt`, its own
`status` lifecycle via `reconcile_drop_statuses`. A batch is not a "child"
in any special runtime sense to the claim engine; it is only a plain drop
that happens to carry a `campaign_id` for lineage. This is why no claim-path
change is required.

**500-voucher / 50-per-hour example**: `compile_campaign()` slices the
500 codes (or `code_count`, reusing Phase 2's `_normalize_codes` reuse) into
10 disjoint groups of 50, and — for `hourly`/`every_x_minutes`/`daily`/
`weekly` (all "fixed cadence, fixed total" styles) — pre-computes all 10
`startsAt` values up front and inserts all 10 `drops` documents in one pass
via the existing `admin_create_drop` insert primitive (factored into the
shared `_insert_drop()` helper Phase 2 already calls for). This mirrors
Phase 2's "Batch release" style exactly; Phase 3 just generalizes the
offset-generation step to the five required cadences.

---

# Batch Scheduler

Two different mechanisms are needed, matching whether the batch count is
fixed and known up front, or open-ended/admin-driven:

**1. Pre-materialized batches** (`every_x_minutes`, `hourly`, `daily`,
`weekly`, and `custom_schedule` with `explicit_times`): all N child drops
are created at compile time with their `startsAt` already set to the
computed future timestamps. No new scheduler job runs these — the existing
`reconcile_drop_statuses` sweep (`main.py:7351-7357`, every 1 minute)
already flips each drop `upcoming → active → expired` purely by comparing
`startsAt`/`endsAt` to now, which is exactly what "release every hour"
needs. This keeps the batch scheduler's blast radius at zero new hot-path
code.

**2. Just-in-time batches** (`manual`, and `custom_schedule` with a
`cron_expression` that has no fixed end / is open-ended): a new, isolated
scheduler job — `campaign_batch_release_tick()` — registered in `main.py`
next to the existing jobs, ticking every 1 minute like
`reconcile_drop_statuses`. Each tick:
1. Loads campaigns with `campaign_status="releasing"` and
   `release_type in ("custom_schedule",)` whose next cron-computed fire
   time has passed and `batches_released < batches_planned` (or unbounded
   for open-ended cron).
2. Creates exactly one new child drop for the next unreleased batch index,
   via the same `_insert_drop()` primitive, keyed by
   `(campaign_id, campaign_batch_index)` uniqueness (see Failure Recovery)
   so a re-run of the tick never double-creates a batch.
3. Increments `batches_released` / appends to `compiled_drop_ids`.

For `manual`, the tick job does nothing on its own — batches are created
one at a time by a new admin action, `release_next_batch`, added to the
existing `admin_drop_actions` endpoint's op set (alongside `start_now`,
`pause`, `end_now`, and Phase 2's `manual_release`). This reuses the same
endpoint pattern rather than introducing a new route.

**Pause**: setting `campaign_status="paused"` causes the tick job to skip
that campaign — no further batches are created or activated. Pausing a
campaign does **not** retroactively pause a batch that is already `active`;
an admin who wants to also stop a live batch does so with the existing
per-drop `pause` op on that specific `drop_id`, unchanged. This separation
keeps "stop future batches" (campaign-level, new) cleanly separate from
"stop an in-flight claim window" (drop-level, existing, untouched).

**Resume**: setting `campaign_status` back to `"releasing"` lets the tick
job continue from `batches_released` where it left off. For pre-materialized
campaigns, "pause" simply means an admin manually `pause`s the individual
upcoming/active drops they don't want live yet; resume is per-drop
`start_now`/unpause on those, since all batches already exist as documents.

**Cancellation**: setting `campaign_status="cancelled"` stops the tick job
from creating any further batches (for just-in-time schedules) permanently.
It does **not** automatically touch already-created child drops — an
already-`active` batch keeps running to its natural `endsAt` (or the admin
explicitly `end_now`s it via the existing per-drop action) and an
already-`upcoming` (not yet started) pre-materialized batch simply never
activates if the admin also `end_now`s it, or is left to expire naturally
via `endsAt`. This is a deliberate non-cascading design: cancellation only
ever *prevents future creation*, never reaches back to mutate live claim
state, so it cannot interact with an in-progress FCFS claim.

---

# Analytics Aggregation

Reporting is a **read-only rollup** over the child drops a campaign already
produced — no new write path, no change to `claim_voucher_for_user` or
`claim_pooled`.

New read-only endpoint, e.g. `GET /admin/campaigns/<id>/report`:
1. Reads `campaigns.compiled_drop_ids` for the campaign.
2. Aggregates `db.drops` for those ids: per-batch `status`,
   `public_remaining`/`my_remaining`, `startsAt`/`endsAt`.
3. Aggregates `db.vouchers` grouped by `dropId in compiled_drop_ids`:
   claimed count, claim velocity (claims per minute since `startsAt`),
   time-to-exhaustion per batch.
4. Rolls these up to campaign-level totals (`total_claimed`,
   `total_remaining`, `% claimed`, `batches_completed / batches_planned`)
   the same way `campaign_engine.get_historical_performance()` already
   aggregates across drops today, just scoped by `campaign_id` instead of
   free-text matching.

**Affiliate settlement**: confirmed to require **zero changes**. Affiliate
tier settlement (`affiliate_rewards.py`) computes referral counts per user
over month/week windows and issues its own bundles from the independent
`voucher_pools` collection — it has no read dependency on `drops`,
`campaign_id`, or claim events from this system at all. A claim against any
child batch drop is indistinguishable to affiliate settlement from a claim
against any manually-created drop today; batch campaigns simply produce
more drops, which affiliate settlement never looks at.

**Anti-abuse**: also requires no new mechanism, but one existing knob needs
to be *pointed at* the new lineage field. Because each batch is its own
`drop_id`, the existing per-drop dedup guarantee (`voucher_claims` unique
index on `(drop_id, user_id)`) only prevents a user from claiming twice
*from the same batch* — by itself it would allow one user to claim once per
batch, e.g. 10 times across a 10-batch campaign. This is the same property
Phase 2's plain "Batch release" style already had, and there is already an
existing, unused-for-this-purpose primitive built for exactly this shape:
`_check_public_pool_campaign_cap()` / `_apply_public_pool_campaign_success()`
(`vouchers.py:1562-1637`). Wiring the campaign's `campaign_id` through as the
cap's scoping key (optional, defaults to no cap = today's per-batch
behavior) lets an admin cap total claims per user across the whole
campaign, without adding any new anti-abuse code — purely an additive,
opt-in use of a function that already exists for this class of problem.

---

# Failure Recovery

1. **Idempotent batch creation.** Add a unique index on
   `drops.(campaign_id, campaign_batch_index)` (partial: only where
   `campaign_id` exists). If `campaign_batch_release_tick()` or the
   compile-time batch loop runs twice for the same batch (crash + retry,
   duplicate cron fire), the second insert attempt fails on the unique
   index instead of creating a duplicate live drop — mirrors the existing
   `uq_claim_drop_user` pattern used for claim idempotency.
2. **Catch-up, not cron-exact.** The just-in-time tick checks "which batch
   *should* exist by now given the schedule" rather than firing an
   in-process timer per batch. If the server restarts or a tick is missed,
   the next tick creates whatever batches are overdue (respecting a sane
   cap, e.g. at most 1 catch-up batch per tick, to avoid a thundering-herd
   burst of simultaneous drops after extended downtime) rather than losing
   the release entirely.
3. **Uneven division.** `total_voucher_count % batch_size != 0` → the final
   batch gets the remainder (e.g. 520 vouchers / 50 = 10 batches of 50 + 1
   of 20), computed once at compile time; never silently drops codes.
4. **Insufficient code pool.** If code generation/allocation for a batch
   fails (e.g. `code_count` requested exceeds what `code_generator` can
   produce), that single batch is marked `campaign_status="partial_failure"`
   at the campaign level and the tick stops advancing further batches for
   that campaign until an admin intervenes — it does not silently create an
   empty or under-provisioned drop that would confuse claimers.
5. **No implicit reclamation.** Cancelling or failing a campaign never
   reaches into `vouchers`/`voucher_claims` to reclaim or reissue codes —
   any code-pool cleanup for an unreleased batch is a deliberate, separate
   admin action on that specific drop (existing `end_now`/pause), keeping
   this system from ever writing into the claim-state collections directly.

---

# Migration Risk

1. **Every new field is additive and optional**, on both `campaigns`
   (`release_type`, `release_config`, `total_voucher_count`, `batch_size`,
   `campaign_status`, `batches_planned`, `batches_released`) and `drops`
   (`campaign_batch_index`, `campaign_batch_total`) — existing drops and
   existing campaigns (pre-Phase-3) simply lack them, and every new read
   path must default to "not campaign-managed" when `campaign_id`/
   `campaign_batch_index` is absent, exactly as Phase 2 already establishes
   for `release_mode`/`drip_schedule`.
2. **One new index, additive only**: unique
   `(campaign_id, campaign_batch_index)` on `drops`, used solely for
   idempotent batch creation — it does not participate in any claim-time
   query (`find_one_and_update` in `_atomic_claim_pooled_voucher` filters
   on `dropId`/`status`/`pool`, never touches these new fields), so it
   cannot affect FCFS claim latency or correctness.
3. **New scheduler job is isolated.** `campaign_batch_release_tick()` is a
   new, independently deployable/feature-flaggable job, not a modification
   of `reconcile_drop_statuses`. It can be disabled at any time and the
   worst case is that no *new* batches get created — already-created
   batches keep functioning as ordinary drops, claimable exactly as today.
4. **Anti-abuse extension is opt-in.** Threading `campaign_id` through
   `_check_public_pool_campaign_cap` is a new optional parameter with a
   default that preserves every existing call site's behavior unchanged
   (no cap unless a campaign explicitly sets one) — consistent with the
   additive-parameter pattern Phase 2 already used for
   `assign_public_pool_access_once`'s `force_probability`.
5. **No backfill required.** Historical drops created before Phase 3 ships
   have no `campaign_id`/`campaign_batch_index` and are simply reported as
   "not campaign-managed" in the new aggregation endpoint; nothing requires
   them to be retrofitted for correctness.
6. **Rollback plan.** Because batches are ordinary `drops` documents
   produced via the existing insert primitive, disabling the batch
   scheduler and the campaign-builder UI at any point leaves behind
   drops that continue to function exactly as manually-created drops —
   no data cleanup or reverse migration needed to "undo" this feature,
   identical to Phase 2's rollback guarantee.
