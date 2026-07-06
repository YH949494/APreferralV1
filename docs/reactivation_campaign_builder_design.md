# APReferral Bot — Phase 4: Reactivation Campaign Builder

Status: **DESIGN ONLY — not implemented.**

Scope guardrail: this document describes a thin, additive layer that turns
"reactivation" from one hardcoded flow (`channel_reactivation.py`) into a
*campaign type* inside the campaign builder already designed in
`docs/campaign_builder_design.md` (Phase 2). It does **not** rebuild:

- `channel_reactivation.py` — its DM/verify/hold/reward loop keeps running
  as-is, and becomes the reference implementation for **one** reactivation
  stage (Stage 1, "left channel" target) rather than being replaced.
- Scheduler jobs — no existing job is rewritten; the one new job this
  design needs follows the exact self-registration pattern
  `app_context.py` already uses for `channel_reactivation.py`.
- XP rewards — every stage grants XP through the existing
  `grant_xp(db, uid, event_type, unique_key, amount)` in `xp.py`, unchanged.
- Voucher allocation — every stage that pays a voucher compiles to a
  `drops`/`vouchers` document through the **existing** Phase 2
  `compile_campaign` → `admin_create_drop`/`_insert_drop` path, unchanged.

Everything new here is: one campaign type value, a stage/reward-journey
schema on top of the existing `campaigns` collection, a segment→target
mapping that reuses `backend_segment_engine.py`'s classifications, a single
new scheduler job that walks users through the stages, and a funnel/
analytics view built the same way `retention_kpis.py`/`funnel_dashboard.py`
already compute other funnels.

---

# Reactivation Types

The brief's six targets map directly onto segments/flags the codebase
**already computes** — no new detection logic is needed:

| Target | Detected by (existing) | Notes |
|---|---|---|
| Left channel | `users.left_channel` flag (set in `main.py` member-update handler, read today by `channel_reactivation.py` and `scheduler._recover_stale_processing`/`referral_rules.py` reason checks) | This is exactly what `channel_reactivation.py` already targets. Becomes "Reactivation Type = Left Channel" in the builder, still executed by the existing file for Stage 1 (see Reward Journey). |
| Ghost | `backend_segment_engine.classify_segment` → `segment == "ghost"` (`after_total_bet == 0 AND referral_count == 0 AND checkin_count == 0`), persisted in `backend_segment_snapshots` | Already the top suggestion in `campaign_engine.SEGMENT_CAMPAIGN_SUGGESTIONS["ghost"] = {"types": ["Reactivation Campaign"], ...}` — the codebase already labels ghost as reactivation's core audience. |
| Churned players | `backend_segment_snapshots` + recency: `low_value`/`normal_actual` segment **and** `activity_recency_days` beyond a configurable threshold (the same `last_active_at`/`last_checkin`/`last_checkin_at` recency lookup `campaign_engine.preview_audience` already joins against `users`) | No new field — this is a segment + recency-window *filter combination* on data already computed, expressed as one more `targeting` shape in `campaigns.py`. |
| Voucher hunters | `backend_segment_engine.classify_segment` → `segment == "voucher_hunter"` (claim_count/bet-per-claim/withdraw-ratio heuristic, "VH v2") | Already a `VALID_SEGMENTS` value in `campaign_engine.py`. Reactivation for this target intentionally uses low-value reward stages (see Reward Journey exposure notes) since `SEGMENT_CAMPAIGN_SUGGESTIONS` already flags this segment as needing controlled exposure. |
| Inactive actual players | `normal_actual`/`active_community_player` segment + recency window, `claim_risk_level == "normal"` | Same pattern as churned players — a segment+recency filter combo, not new detection. Distinguished from "churned" only by segment (still a real, engaged-history player, just currently inactive) vs. "churned" (further gone / lower engagement history). |
| High value inactive players | `segment == "high_value"` + recency window | Reuses the segment already prioritised as `"exposure": "full"` in `SEGMENT_CAMPAIGN_SUGGESTIONS`; reactivation for this target uses the richest reward stages (see below), same principle Phase 2's "VIP" template already applies. |

**Compiles to**: each Reactivation Type is a `targeting` preset (segment list
+ optional `activity_recency_days` range + optional `left_channel: true`
flag), in the exact shape `campaigns.py`'s `targeting` sub-document and
`campaign_engine.preview_audience`'s `$match` builder already accept — no
new query engine. "Left Channel" is the one target that isn't a segment at
all; it's a boolean flag filter (`users.left_channel == true`), added as one
more optional key alongside `segments`/`player_age_types`/`claim_risk_levels`
in the same `targeting` dict, resolved by one additional `$match` clause in
`preview_audience` (additive, same pattern as the existing
`activity_recency_days` clause).

---

# Reward Journey

A reactivation campaign is a **stage ladder**, not a single reward. Each
stage has: a trigger condition (time since previous stage / re-engagement
event), a reward type, and an idempotency key so the existing grant
functions never double-pay.

| Stage | Trigger | Purpose | Default reward type(s) |
|---|---|---|---|
| Stage 1 — Return reward | User re-engages (rejoins channel / makes a check-in / claims a voucher / any activity event after being flagged) | Get them back in the door | XP or Voucher (small) |
| Stage 2 — 7-day retention reward | User still active 7 days after Stage 1 trigger (no repeat of the original target condition — e.g. `left_channel` false again, or a check-in/bet event inside the 7-day window) | Confirm the comeback wasn't a one-off | Voucher or XP |
| Stage 3 — 30-day retention reward | User still active 30 days after Stage 1 trigger | Reward sustained return | Voucher, Affiliate bonus (if they've referred), or Badge |
| Stage 4 — Long-term retention reward | User still active 90 days after Stage 1 trigger (config: `LONG_TERM_RETENTION_DAYS`, default 90) | Reward the highest-value outcome: full reactivation | Surprise reward, Affiliate bonus, or high-tier Voucher |

Each stage is independently on/off and independently value-configured per
campaign (a "Ghost" reactivation campaign might only run Stages 1–2 with
small XP; a "High Value Inactive" campaign might run all 4 stages with
richer vouchers) — this mirrors Phase 2's template pattern of "same
underlying primitives, different defaults per template."

## Reward type → existing primitive mapping

| Reward type | Compiles to | Reused, untouched |
|---|---|---|
| XP | `grant_xp(db, uid, event_type=f"reactivation:{campaign_id}:stage{n}", unique_key=f"{campaign_id}:{uid}:stage{n}", amount=stage_amount)` | `xp.py` — same idempotent event/ledger double-write, same `restrictions.no_xp` opt-out respected automatically. |
| Voucher | One `drops` document per stage-trigger event, built by the Phase 2 compiler (`compile_campaign` → `_insert_drop`), `type="personalised"`, `assignments=[{user_id, code}]` (1:1, since the trigger already identifies exactly one user) | Phase 2 compiler + `admin_create_drop`'s insert path; `claim_voucher_for_user`/`claim_personalised` unchanged. |
| Affiliate bonus | Existing `affiliate_rewards.py` tier-bundle path is **not** re-triggered here (that's earned by referral volume, a different lifecycle). Instead this reward type issues a **voucher** whose `reward_config.source == "affiliate_bonus"` label is purely a UI/reporting tag on the same voucher-drop compile path above — it does not touch `affiliate_ledger`/settlement. | Same voucher primitive as above; `affiliate_rewards.py`/`affiliate_ledger` untouched. |
| Badge | **No existing badge/achievement system exists in the codebase** (confirmed: no `badges` collection, no `award_badge()` anywhere). This reward type is out of scope for "reuse existing logic" — flagged as a real gap. Two honest options, neither implemented here: (a) descope Badge from Phase 4's initial reward-type list until a badge system is designed separately, or (b) the smallest possible additive stand-in — write a `users.badges: [{id, campaign_id, stage, awarded_at}]` array via `$addToSet` (no new collection, no new claim/display logic) purely as a data marker, with actual UI/display work deferred. Recommendation: (a) — ship Stages with XP/Voucher/Affiliate-bonus/Surprise first, add Badge once a real badge system exists, rather than inventing one inside this design. |
| Surprise reward | Reuses the existing "hidden until manual/segment-probability release" mechanic already in `vouchers.py` (`assign_public_pool_access_once` segment-probability gate, or Phase 2's "Surprise Drop" release style: pooled drop hidden until an admin/scheduler flips `manuallyReleasedAt`). For Stage 4, "surprise" = the *reward pool* (which of XP/voucher-value/affiliate-bonus the user gets) is chosen at grant time by a weighted-random pick among the campaign's configured Stage 4 reward options, then dispatched through the same XP/Voucher primitives above. No new randomness primitive — `random.random()`-based selection, same library already used at the pooled-drop-access layer. | `assign_public_pool_access_once` pattern (weighted pick), `grant_xp`/voucher compiler for the actual payout. |

## Idempotency contract

Every stage grant's `unique_key` is `f"{campaign_id}:{uid}:stage{n}"`
(XP path) or the equivalent `campaign_id`+`uid`+`stage` composite key on the
new `reactivation_progress` ledger (see Database Changes) for the voucher
path, so a scheduler re-run or retry can never double-grant a stage — the
same contract `channel_reactivation.py` already uses for its single reward
(`f"{CAMPAIGN_ID}:{uid}"`), generalised with a `:stage{n}` suffix.

---

# Reactivation Funnel

Reuses `channel_reactivation.py`'s existing stage vocabulary
(eligible → sent → verified/subscribed → rewarded → cancelled) and
generalises it across the 4 reward stages, computed the same way
`funnel_dashboard.compute_funnel`/`retention_kpis.compute_retention_kpis`
already build multi-stage funnels: a materialized snapshot, recomputed on a
schedule, read by a dashboard endpoint — not a live query per page load.

```
Targeted   →  Triggered (Stage 1 condition fired, message sent)
           →  Returned   (Stage 1 reward granted)
           →  Retained 7d   (Stage 2 reward granted)
           →  Retained 30d  (Stage 3 reward granted)
           →  Retained 90d+ (Stage 4 reward granted)
           →  Dropped-off (re-lapsed before next stage — tracked, not penalized)
```

Per-target breakdown (Left Channel / Ghost / Churned / Voucher Hunter /
Inactive Actual / High Value Inactive) is a `group by` on the same
`targeting`/segment field already stored on the campaign doc — same
aggregation shape `funnel_dashboard.py`'s cohort helpers already use, just
grouped by `campaign_id` + `reactivation_type` instead of by signup cohort
month.

**Compiles to**: a new `reactivation_funnel_snapshots` collection (one doc
per `campaign_id` + `snapshot_date`), written by the one new scheduler job
below, following `retention_kpis.py`'s exact pattern (`ensure_*_indexes`,
`compute_*_for_*`, cached "last computed" marker in `admin_cache`).

---

# Analytics

Nothing new computationally — this section wires the reactivation funnel
and stage grants into the analytics surfaces that already exist:

- **Audience/expected-cost preview**: `campaign_engine.preview_audience` —
  already returns `audience_size`, `segment_distribution`,
  `expected_voucher_cost` for any `targeting` filter, including the new
  `left_channel`/recency-window filters this design adds. No change needed
  beyond the one additive `$match` clause noted above.
- **Historical performance**: `campaign_engine.get_historical_performance`
  — already looks at past campaigns over matching segments; reactivation
  campaigns show up here automatically once they're persisted as
  `campaigns` docs with `campaign_type="reactivation_campaign"` (already a
  valid `CAMPAIGN_TYPES` entry today).
- **Funnel view**: new `reactivation_funnel_snapshots` (above), surfaced via
  one new read-only admin route (`GET /api/admin/campaigns/<id>/reactivation-funnel`),
  same auth pattern as the existing `GET /api/admin/channel-reactivation/summary`
  in `admin_auth.py`.
- **Cost/ROI**: XP-stage cost is informational (XP has no direct currency
  cost); voucher-stage cost reuses `expected_voucher_cost` math already in
  `preview_audience`, extended to sum across all 4 stages' configured
  voucher values (`reward_config` per stage) rather than a single value.
- **Retention correlation**: since Stage 2/3/4 triggers are literally
  "still active N days later," reactivation campaign success is a subset of
  what `retention_kpis.compute_retention_kpis` already tracks per cohort —
  a reactivation campaign's Stage-2/3/4 conversion rates are one more slice
  of the same retention computation, not a parallel metric definition.

No new event-tracking bus is introduced. Every stage transition is a
document write to `reactivation_progress` (see below), which is exactly the
kind of source-of-truth ledger `retention_kpis.py`/`funnel_dashboard.py`
already aggregate from.

---

# Segment Integration

Reactivation targeting is **one more `targeting` shape** inside the
existing `campaigns` collection/blueprint (`campaigns.py`), not a parallel
targeting system:

- `campaign_type = "reactivation_campaign"` — already a valid value in
  `CAMPAIGN_TYPES`, unchanged.
- `targeting.segments` — reuses `VALID_SEGMENTS` from `campaign_engine.py`
  (`high_value`, `low_value`, `voucher_hunter`, `ghost`, `normal_actual`,
  `active_community_player`, `unclassified`) exactly as today.
- `targeting.left_channel: true` — **one new optional key**, additive, only
  read by the one new `$match` clause described in Reactivation Types.
- `targeting.activity_recency_days` — already exists in `campaign_engine.py`
  for the churned/inactive-actual/high-value-inactive targets; no change.
- `reactivation_config` — **one new optional sub-document** on the
  `campaigns` collection (only present when `campaign_type ==
  "reactivation_campaign"`): `{stages: [{stage_number, reward_type,
  reward_config, trigger_days_after_previous}, ...], reactivation_type}`.
  This is additive-only, following the exact convention Phase 2's
  `audience_overrides`/`release_config`/`reward_config` fields already
  established — absent on every non-reactivation campaign, ignored by every
  existing read path.

At claim-time enforcement (for the voucher-reward stages), this design
**inherits the same gap Phase 2 already flagged and scoped**: writing
`targeting.segments` on a campaign only restricts who the campaign *targets
at compile time* (the compiler resolves the segment membership once, at the
moment it builds the personalised voucher `assignments` list for that
stage's trigger event) — it does not require a new live `audience.segments`
check in `is_drop_allowed`, because every reactivation voucher is
**personalised** (1:1 `assignments`, resolved from the specific user whose
stage condition just fired), not a pooled drop with runtime segment
enforcement. This sidesteps Phase 2's open "Segment audience mode" gap
entirely for reactivation's use case — no new evaluator touch needed here.

---

# Scheduler Integration

One new job, registered exactly like `channel_reactivation.py`'s existing
self-registration in `app_context.py` — not a new entry in `main.py`'s
monolithic `add_job` block, and not a modification of any existing job:

```python
# reactivation_campaigns.py (new, small, orchestration-only module)
def process_reactivation_stage_progression(*, db_ref, now_ref=None) -> dict:
    """
    For each active campaign with campaign_type == "reactivation_campaign":
      1. Stage 1 trigger scan — reuses the exact detection channel_reactivation.py
         already does for the "left channel" reactivation type (delegates to
         channel_reactivation.process_reactivation_campaign for that specific
         type; for the five segment-based types, scans backend_segment_snapshots
         + users the same way campaign_engine.preview_audience's $match does).
      2. Stage 2/3/4 progression scan — for users already in
         reactivation_progress at stage n, check "still active" using the
         SAME activity fields campaign_engine/backend_segment_engine already
         read (last_active_at / last_checkin / checkin_count / bet activity),
         and if the trigger_days_after_previous window has elapsed without
         a relapse into the original target condition, grant the next stage.
      3. Every grant goes through grant_xp() or the Phase 2 voucher compiler —
         no direct writes to xp_ledger/vouchers/drops.
      4. Every transition is written to reactivation_progress (append-only-ish
         upsert keyed by campaign_id+user_id+stage) and snapshotted into
         reactivation_funnel_snapshots on the same run (or a lighter/less
         frequent companion job — see cadence below).
    """
```

Registration (in `app_context.py`, alongside the existing
`_register_reactivation_job` for `channel_reactivation.py`):

```python
def _register_reactivation_stage_job(scheduler):
    if not os.getenv("REACTIVATION_CAMPAIGNS_ENABLED", "1") == "1":
        return
    scheduler.add_job(
        process_reactivation_stage_progression,
        trigger=CronTrigger(minute="*/15"),
        id="reactivation_stage_progression",
        kwargs={"db_ref": db},
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
```

- Cadence `*/15` (vs. `channel_reactivation`'s `*/1`) because stage
  triggers are day-granularity (7/30/90-day windows), not real-time DM
  delivery — no need to compete for the same tight loop.
- Feature-flagged (`REACTIVATION_CAMPAIGNS_ENABLED`), same convention as
  `BOT_SEGMENT_SYNC_ENABLED`/`GROWTH_LEADERBOARD_ENABLED`/`AUTOSCALE_ENABLED`
  — can ship dark and be flipped on independently.
- Uses the existing Mongo-based `acquire_scheduler_lock(job_name, ttl_seconds=...)`
  if deployed across multiple worker replicas, same as `tick_5min`.
- `channel_reactivation.py` itself is **not modified**: its `*/1` job keeps
  running exactly as today and remains the sole authority for the "Left
  Channel" target's Stage-1 detection/DM/verify/hold flow. The new job
  calls into it (or reads its `channel_reactivation_rewards`/`users`
  flags) for that one target rather than re-implementing detection, and
  owns Stage 2–4 progression for all six target types uniformly.

---

# Database Changes

All additive, all optional, following the same migration-safety posture as
Phase 2 (`docs/campaign_builder_design.md` → Migration Safety):

## New: `reactivation_progress` collection

One doc per `(campaign_id, user_id, stage)`, unique index on that triple —
this is the ledger the new scheduler job reads/writes and the funnel
snapshot job aggregates from.

| Field | Type | Purpose |
|---|---|---|
| `campaign_id` | ObjectId (string) | FK to `campaigns._id` |
| `user_id` | int | Telegram user id |
| `reactivation_type` | string | One of the 6 targets, denormalised for grouping |
| `stage` | int (1–4) | Which stage this row represents |
| `status` | string | `pending` \| `triggered` \| `rewarded` \| `relapsed` |
| `triggered_at` | datetime | When the stage's trigger condition fired |
| `reward_type` | string | `xp` \| `voucher` \| `affiliate_bonus` \| `surprise` |
| `reward_ref` | string\|null | `unique_key` used for `grant_xp`, or the compiled voucher `drop_id`/code — enables audit/reconciliation without touching `xp_ledger`/`vouchers` schemas |
| `rewarded_at` | datetime\|null | Set when the grant call returns success |

## New: `reactivation_funnel_snapshots` collection

One doc per `(campaign_id, snapshot_date)`, following `retention_kpis.py`'s
exact snapshot-doc convention.

| Field | Type | Purpose |
|---|---|---|
| `campaign_id` | ObjectId (string) | FK |
| `snapshot_date` | date | Day the snapshot was computed |
| `stage_counts` | dict | `{targeted, triggered_stage1, rewarded_stage1, ..., rewarded_stage4, dropped_off}` |
| `by_reactivation_type` | dict | Same counts, grouped by the 6 target types |

## Additive fields on `campaigns` (extends Phase 2's already-additive fields)

| Field | Type | Purpose |
|---|---|---|
| `reactivation_type` | string\|null | One of the 6 targets. Only present when `campaign_type == "reactivation_campaign"`. |
| `reactivation_config` | dict\|null | `{stages: [{stage_number, reward_type, reward_config, trigger_days_after_previous}]}` — drives the new scheduler job. Absent = not a staged reactivation campaign (today's `channel_reactivation.py` behavior, single implicit stage). |

## Additive field on `campaigns.targeting`

| Field | Type | Purpose |
|---|---|---|
| `left_channel` | bool\|null | New optional targeting key, read by one additive `$match` clause in `campaign_engine.preview_audience`. Absent = no change to existing behavior for any non-reactivation campaign. |

**No changes** to `drops`, `vouchers`, `xp_events`, `xp_ledger`,
`affiliate_ledger`, `channel_reactivation_campaigns`,
`channel_reactivation_messages`, `channel_reactivation_rewards`, or any
existing index. `channel_reactivation.py`'s collections and `users.*`
reactivation flags stay exactly as they are — the new
`reactivation_progress` ledger is a superset concept that runs alongside
them, not a replacement.

---

# Migration Safety

1. Every new collection/field is additive; no existing document requires a
   backfill for correctness, mirroring Phase 2's posture.
2. `channel_reactivation.py`'s `*/1`-minute job, its three campaign
   collections, and its `users.reactivation_*` flags are untouched — this
   design ships as a strictly additional layer, so disabling
   `REACTIVATION_CAMPAIGNS_ENABLED` leaves the existing "left channel"
   reactivation running exactly as it does in production today.
3. The one new evaluator-adjacent behavior — resolving segment membership
   at compile time for personalised reactivation vouchers — reuses
   read-only queries against `backend_segment_snapshots`
   (`campaign_engine.preview_audience`'s existing `$match` pattern); it adds
   no new claim-time check to `is_drop_allowed`/`is_user_eligible_for_drop`,
   so the claim hot path is not touched at all by this design (stronger
   guarantee than Phase 2, which needed 4 small evaluator touches — this
   phase needs zero, by construction, because reactivation rewards are
   always personalised/1:1, never pooled).
4. New indexes (`reactivation_progress` unique on
   `campaign_id+user_id+stage`, `reactivation_funnel_snapshots` unique on
   `campaign_id+snapshot_date`) are new collections, so index creation
   can't contend with existing hot-path indexes.
5. Rollback plan: turning off `REACTIVATION_CAMPAIGNS_ENABLED` stops the
   one new job; `reactivation_progress`/`reactivation_funnel_snapshots`
   data is inert (read only by the funnel dashboard route), so rollback
   requires no data cleanup.

---

# Existing Logic Reuse

Reused as-is, untouched:

- **Detection**: `backend_segment_engine.classify_segment` (ghost, churned,
  voucher_hunter, normal_actual, high_value classifications),
  `users.left_channel` flag (set by `main.py`'s member-update handler).
- **Left-channel campaign implementation**: all of `channel_reactivation.py`
  — its DM send, 72h reward-hold, membership re-verification, and its
  `*/1`-minute scheduler job stay exactly as they are, and remain the
  Stage-1 authority for the Left Channel target.
- **XP grants**: `xp.grant_xp(db, uid, event_type, unique_key, amount)` —
  every stage of every reactivation campaign calls this, unmodified.
- **Voucher allocation**: the Phase 2 `compile_campaign` → shared
  `_insert_drop` → `admin_create_drop`/`claim_personalised` path — every
  voucher-reward stage compiles to a personalised drop through this
  existing pipeline, unmodified.
- **Affiliate bonus payout mechanics**: reactivation's "affiliate bonus"
  reward type is a labeled voucher through the same compiler path above;
  `affiliate_rewards.py`'s tier-bundle/ledger/settlement machinery is not
  invoked and not modified.
- **Segment/audience preview & reporting**: `campaign_engine.preview_audience`,
  `campaign_engine.get_historical_performance`, `campaigns.py`'s CRUD
  blueprint, `VALID_SEGMENTS`, `SEGMENT_CAMPAIGN_SUGGESTIONS` (already names
  "ghost → Reactivation Campaign" today).
- **Retention/funnel computation pattern**: `retention_kpis.py`'s
  snapshot-doc + `admin_cache` "last computed" convention,
  `funnel_dashboard.compute_funnel`'s stage-conversion shape — the new
  `reactivation_funnel_snapshots` job follows these patterns rather than
  inventing a new analytics engine.
- **Scheduler self-registration pattern**: `app_context.py`'s lazy
  singleton hook (`set_scheduler` → `_register_*_job`), the established
  template for how `channel_reactivation.py` itself hooks into the
  scheduler without touching `main.py`'s monolithic `add_job` block.
- **Admin route/auth pattern**: `admin_auth.py`'s
  `_admin_api_authorized`-gated routes, mirrored by the one new read-only
  funnel route.

Net-new (all additive, all reuse-through-composition, zero modifications
to claim engine / anti-abuse / existing scheduler jobs / affiliate
settlement / XP internals):

1. One new orchestration module (`reactivation_campaigns.py`) + one new
   `*/15`-minute scheduler job, self-registered via the existing
   `app_context.py` pattern.
2. Two new collections (`reactivation_progress`,
   `reactivation_funnel_snapshots`).
3. Two new optional fields on `campaigns` (`reactivation_type`,
   `reactivation_config`) and one new optional key on `campaigns.targeting`
   (`left_channel`), all additive.
4. One additive `$match` clause in `campaign_engine.preview_audience` for
   the `left_channel` targeting key.
5. One new read-only admin route for the funnel view.

**Known gap, explicitly not solved here**: there is no existing badge/
achievement system to reuse for the "Badge" reward type. Recommendation is
to launch Stages 1–4 with XP / Voucher / Affiliate-bonus / Surprise reward
types first, and treat Badge as a separate, future design once a real
badge/achievement engine exists — bolting a placeholder `users.badges`
array onto this design would be exactly the kind of premature, half-built
abstraction this codebase's other designs have avoided.
