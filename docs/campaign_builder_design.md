# APReferral Bot — Phase 2: Campaign Builder Design

Status: **DESIGN ONLY — not implemented.**

Scope guardrail: this document describes a layer that sits *above*
`vouchers.py` and compiles down into the primitives it already exposes
(`drops` documents + `vouchers` documents). It does **not** touch the claim
engine, anti-abuse checks, the scheduler, or affiliate settlement — those
stay exactly as they are today. Every template/mode below is defined in
terms of fields `vouchers.py` already reads.

---

## 0. Campaign wizard (UI shape)

| Step | Name | Produces |
|---|---|---|
| 1 | Campaign type | `campaign_template` (one of the 15 templates below) |
| 2 | Audience | `audience_mode` + mode-specific params → compiles to `eligibility` / `audience` |
| 3 | Release style | `release_style` + timing params → compiles to `startsAt`/`endsAt`/code-batching |
| 4 | Rewards | voucher value(s), code source, quantity → compiles to `vouchers` documents |
| 5 | Review | read-only summary + `preview_audience()` call (already exists in `campaign_engine.py`) before submit |

Submitting Step 5 calls one compiler function, `compile_campaign(campaign_doc) -> list[drop_doc]`, which is the only new piece of write logic. It builds one or more `drops` documents (batch/drip templates emit several) and calls the **existing** `admin_create_drop` insertion path (or the same insert logic factored into a shared helper) to create them. No new claim path, no new eligibility check, no new settlement path.

---

# Campaign Templates

Each template is a *preset* — a fixed combination of defaults for audience mode, release style, and eligibility — layered on top of the same underlying drop schema. Templates only constrain which choices are shown/pre-filled in Steps 2–4; the compiler output is always a plain drop.

| Template | Maps to `type` | Default audience mode | Default release style | Notes |
|---|---|---|---|---|
| Smart Default | pooled | Smart segment % | Immediate | Uses `SEGMENT_PROBABILITY_CONFIG` / `BOT_SEGMENT_PROBABILITY_MAP` as-is. This is the fallback template — "just make a normal drop." |
| Public | pooled | No segment filter | Immediate or Scheduled | `eligibility.mode="public"`, no `audience.type`. Today's default pooled drop. |
| Batch Release | pooled | Smart segment % or No filter | Batch release | Same drop, codes inserted in tranches (see Release Styles). |
| Welcome | pooled | Smart segment % (locked to new-joiner window) | Immediate | Reuses the existing `new_joiner` / `new_joiner_48h` audience shortcut and welcome eligibility/progress machinery untouched. |
| VIP | pooled or personalised | VIP | Immediate or Scheduled | `eligibility.mode="tier"`, `allow=["vip", ...]` (reuses `_normalize_tier_value`). |
| Segment | pooled | Smart segment % | Immediate/Scheduled | Explicit `audience.segments` list (backend segments from `backend_segment_snapshots`, e.g. `high_value`, `low_value`), reuses `campaign_engine.preview_audience`. |
| Region | pooled | Region | Immediate/Scheduled | `audience.regions`, reuses `_region_matches_pool` / `get_claimable_pools`. |
| Affiliate | personalised or pooled | Whitelist (affiliate-referred user list) | Immediate/Scheduled | Targets users sourced from `referral_events`/affiliate ledger; does not touch affiliate settlement, only reads existing referral collections to build a whitelist. |
| Personalised | personalised | Whitelist | Immediate/Scheduled | 1:1 assignment, exactly today's `assignments: [{username, code}]` path. |
| FCFS | pooled | No segment filter or Smart % | Immediate | Plain pooled drop, `public_remaining` decremented on claim — nothing new, "FCFS" is purely a UI label for "first-come-first-served pooled drop with no reservation." |
| Surprise Drop | pooled | Smart segment % | Manual release | `status` stays `upcoming`/hidden until an admin action flips a `manualReleaseAt` style trigger (see Release Styles). |
| Reactivation | pooled | Segment (ghost/low_value) | Scheduled | Segment locked to `ghost`/`low_value`, reuses `channel_reactivation.py` targeting data if available. |
| Community Event | pooled | No segment filter / Whitelist | Scheduled or Batch | Same shape as Public/Batch, template only changes hero copy defaults (`hero_title`/`hero_subtitle`). |
| Tournament | pooled or personalised | Whitelist (leaderboard winners) | Scheduled | Whitelist compiled from an external ranking source (e.g. `affiliate_leaderboard.py` output) into `whitelistUsernames` / `assignments`. |
| Test | pooled or personalised | Admin only | Immediate | `eligibility.mode="admin_only"` — already a supported mode, just surfaced as a one-click template for QA. |

No template introduces a new drop `type`. Every one resolves to `pooled` or `personalised`, the two values `_normalize_drop_type` already accepts.

---

# Release Styles

Release style controls **when** and **how** vouchers become claimable. All of them compile to fields the scheduler (`reconcile_drop_statuses`) already understands (`startsAt`, `endsAt`, `status`), plus, for multi-drop styles, multiple drop documents or staged code inserts.

| Style | Compiles to | Reuses |
|---|---|---|
| Immediate | `startsAt = now`, `endsAt = now + duration` | `admin_create_drop` as-is; `status` computed the same way (`"active" if startsAt <= now < endsAt`). |
| Scheduled | `startsAt`/`endsAt` set to future admin-chosen times | Existing `reconcile_drop_statuses` scheduled sweep already flips `upcoming → active → expired` by time; no scheduler change. |
| Batch release | N child drops, each with its own `startsAt` offset (e.g. every 2 hours) and its own code slice | Each batch is a normal drop; codes are split across batches at compile time and inserted via the existing `insert_many` path used in `admin_create_drop`. No new claim logic — each batch is claimed like any pooled drop. |
| Manual release | Drop created with `status="upcoming"` and a far-future/disabled `startsAt`, or with a new `release_mode="manual"` marker (see Database Changes) that the visibility check treats as "never auto-activate." Admin flips it live via the existing `admin_drop_actions` endpoint (already supports admin-triggered drop state changes). | `admin_drop_actions` (existing endpoint), `is_drop_active`. |
| Drip release | One drop, but codes are added incrementally over time using the **existing** `admin_add_codes(drop_id)` endpoint, driven by a new lightweight scheduler job that calls it on a cadence read from campaign metadata. This is the only place a *new* scheduled job is needed, and it only ever calls the existing add-codes path — it does not reimplement claim/allocation. | `admin_add_codes`, `public_remaining`/`my_remaining` increment logic already in `admin_create_drop`/`admin_add_codes`. |

Important boundary: Batch/Drip do not require new claim-time logic, because "how many codes are free right now" is already the pooled-drop `public_remaining`/`my_remaining` counter model. Batch = multiple drops pre-split; Drip = one drop whose counters are topped up on a timer via the existing add-codes call.

---

# Audience System

Audience mode is Step 2 of the wizard. Every mode below maps onto the eligibility/audience fields `vouchers.py` already evaluates in `is_drop_allowed` / `is_user_eligible_for_drop` / `_drop_audience_type` / `get_claimable_pools`.

| Audience mode | Compiles to | Existing evaluator |
|---|---|---|
| Smart segment % | No new eligibility restriction on the drop itself — probability is applied at **assignment/claim** time via `SEGMENT_PROBABILITY_CONFIG` (backend segment) or `BOT_SEGMENT_PROBABILITY_MAP` (bot segment), exactly as today's public-pool assignment (`assign_public_pool_access_once`, `classify_public_pool_segment`). Campaign only records which probability table it opted into (default: current defaults, unmodified) for the Review/reporting step. | `assign_public_pool_access_once`, `backend_segment_probability`, `public_pool_probability_for_bot_segment` |
| Equal chance | Same claim path, but campaign records `audience.equal_chance = true` so the compiler skips applying the segment probability multiplier (i.e., treat every visible/eligible user identically) — this is a **read-only reporting flag**; no change to the underlying assignment RNG code path is required. |  (new: single boolean, no engine change) |
| No segment filter | `eligibility.mode="public"`, no `audience.segments` | Existing default behavior |
| Whitelist | `whitelistUsernames` (pooled) or `assignments` (personalised) | Existing fields, unchanged |
| VIP | `eligibility.mode="tier"`, `allow=["vip"]` | `_normalize_tier_value`, existing tier check |
| Region | `audience.regions: [...]` | `_region_matches_pool`, `get_claimable_pools`/`get_visible_pools` |
| Admin only | `eligibility.mode="admin_only"` | Existing mode in `is_drop_allowed` |

**Segment defaults — unchanged.** The wizard reads (never overwrites) the existing single source of truth in `config.py`:

```
BOT_SEGMENT_PROBABILITY_MAP = {
    "new_user": 0.70, "new_joiner": 0.70, "normal_actual": 0.70,
    "potential": 0.50, "high_value": 0.50,
    "ghost": 0.05→ shown to admin as 30% per product spec* ,
    "low_value": 0.10, "voucher_hunter": 0.10,
    "welcome_abuse": 0.05, "multi_account": 0.05,
}
```

\* Note found during research: the task brief specifies `ghost = 30`, but the current `config.py` value is `0.05` (5%). This is a pre-existing discrepancy between the product brief and the shipped default, **not** something this design changes — flagging it for the team to reconcile in `config.py` directly (outside campaign-builder scope) rather than silently "fixing" it inside the new wizard. The campaign builder must display whatever `config.py` actually contains, so the two never drift further apart.

The wizard never writes to `SEGMENT_PROBABILITY_CONFIG` / `BOT_SEGMENT_PROBABILITY_MAP`. If a future requirement needs per-campaign segment-percentage overrides, that is an explicit opt-in override field on the campaign (see Database Changes → `audience_overrides`), resolved at assignment time as "campaign override, else global default" — never a mutation of the global table.

---

# Compiler Design

```
compile_campaign(campaign_doc: dict) -> list[dict]   # returns drop_doc(s), pre-insert
```

Pipeline:

1. **Resolve template** → pulls the template's default `type`/audience-mode/release-style (Campaign Templates table) as a base, overridden by anything the admin explicitly changed in Steps 2–4.
2. **Resolve audience** → produces `eligibility` + `audience` sub-documents identical in shape to what `admin_create_drop` builds today (`clean_eligibility`, `audience_clean`). Reuses `_normalize_tier_value`, region list handling, and the `new_joiner`/`new_joiner_48h` marker logic verbatim.
3. **Resolve release style**:
   - Immediate/Scheduled/Manual → single drop_doc, `startsAt`/`endsAt` (+ `release_mode` for Manual).
   - Batch → N drop_docs, each with its own `startsAt` offset and a disjoint slice of the requested code pool.
   - Drip → single drop_doc + a `drip_schedule` sub-document (cadence, tranche size, total tranches) that a new scheduler job reads to call `admin_add_codes` repeatedly.
4. **Resolve rewards** → code source (`codes: [...]` provided literally, or `code_count` + `code_generator` to auto-generate, reusing the existing `_normalize_codes` normalisation) and `pool` (`public`/`my`), or `assignments` for personalised.
5. **Attach campaign lineage** → every emitted drop_doc gets `campaign_id` (FK to the `campaigns` collection) and `campaign_template` so admin UI and reporting can group drops back to the campaign that spawned them.
6. **Insert** → for each drop_doc, call the *same* insert path `admin_create_drop` already uses (factor its body-insertion logic — lines ~6606–6726 of `vouchers.py` — into a shared `_insert_drop(drop_doc, vouchers_payload)` helper that both the legacy endpoint and the compiler call, so there is exactly one code path that writes to `db.drops`/`db.vouchers`).

The compiler is a pure translation layer: campaign metadata in, one or more calls to the existing drop-insert primitive out. It never talks to `voucher_claims_col`, `assign_public_pool_access_once`, the scheduler, or affiliate ledgers directly.

---

# Database Changes

New collection: **`campaigns`** (already exists, per `campaigns.py`/`campaign_engine.py` — extend it rather than replace it).

Additive fields on `campaigns` (all optional, all default to today's behavior when absent):

| Field | Type | Purpose |
|---|---|---|
| `campaign_template` | string | One of the 15 template ids. Drives wizard Step 1/2/3 defaults only. |
| `audience_mode` | string | One of the 7 audience modes. |
| `audience_overrides` | dict\|null | Optional per-campaign segment-probability override (`{segment: pct}`), resolved as override-else-global-default at assignment time. Absent = use `config.py` defaults untouched. |
| `release_style` | string | One of the 5 release styles. |
| `release_config` | dict | Style-specific params: `{tranche_count, tranche_interval_minutes}` for Batch/Drip, `{manual: true}` for Manual. |
| `reward_config` | dict | `{voucher_value, code_source, code_count}` — feeds the compiler's rewards step; informational + drives `expected_voucher_cost` in `preview_audience`. |
| `compiled_drop_ids` | list[str] | Populated after compile: the `drops._id`s this campaign produced. Enables "show me all drops from this campaign" in reporting without touching claim/settlement code. |

Additive fields on `drops` (all optional):

| Field | Type | Purpose |
|---|---|---|
| `campaign_id` | ObjectId (string) | Back-reference to `campaigns._id`. Purely informational/reporting — no existing read path needs to know about it, so it is safe to add without touching `is_drop_active`, `is_drop_allowed`, claim logic, or the scheduler. |
| `campaign_template` | string | Denormalised copy of the template, for admin list/filter screens. |
| `release_mode` | string (`"auto"` default \| `"manual"`) | Only new field with behavioral weight: when `"manual"`, `is_drop_active`/visibility logic must additionally require an explicit `manuallyReleasedAt` timestamp before treating the drop as active, even if `startsAt` has passed. Requires one small, additive guard in `is_drop_active` (`and (release_mode != "manual" or manuallyReleasedAt is not None)`); everything else about the function is untouched. |
| `manuallyReleasedAt` | datetime\|null | Set by the existing `admin_drop_actions` endpoint when an admin manually releases a Manual/Surprise Drop campaign. |
| `drip_schedule` | dict\|null | `{tranche_size, interval_minutes, tranches_released, tranches_total}` — read only by the new drip scheduler job, ignored everywhere else. |

No changes to `vouchers`, `voucher_claims`, affiliate ledger, or any settlement/scheduler collection schema.

---

# Migration Safety

1. **Every new field is additive and optional.** Existing drops (created before this feature ships) simply have none of `campaign_id`/`campaign_template`/`release_mode`/`drip_schedule`. All new read paths must default `release_mode` to `"auto"` and treat a missing `campaign_id` as "not campaign-managed" — never require these fields.
2. **`is_drop_active` change is the only touch to a hot path**, and it's a pure AND-guard that only changes behavior for drops that explicitly opt in with `release_mode == "manual"`. For the ~100% of existing drops without that field, the guard short-circuits to today's exact behavior (`release_mode != "manual"` is `True`), so no backfill is required for correctness.
3. **No changes to indexes that back claim-time queries.** New indexes are additive only: `campaigns.campaign_template`, `drops.campaign_id` (for admin reporting lookups), following the same `_ensure_index_if_missing`/`create_index(..., name=...)` pattern already used in `ensure_voucher_indexes()` / `_ensure_campaigns_indexes()`.
4. **New drip scheduler job is a new, isolated function**, not a modification of `reconcile_drop_statuses` or any existing scheduler entry point — it can be deployed, feature-flagged, and rolled back independently of the sweep job that manages `upcoming/active/expired` transitions.
5. **`audience_overrides` never mutates `SEGMENT_PROBABILITY_CONFIG`/`BOT_SEGMENT_PROBABILITY_MAP` in `config.py`.** Those remain process-wide constants. A campaign override is resolved at the call site (`assign_public_pool_access_once` or wherever probability is looked up) as "check campaign override dict first, else fall back to the existing global function" — implemented as a new optional parameter with a default that preserves current call sites unchanged.
6. **Rollback plan**: because the compiler only calls the existing drop-insert primitive, disabling the campaign-builder UI/API at any point leaves behind ordinary `drops`/`vouchers` documents that continue to function exactly as manually-created drops do today — no data cleanup or reverse-migration is needed to "undo" the feature.
7. **Backfill is optional, not required.** If reporting wants historical drops attributed to campaigns, a best-effort backfill script can set `campaign_id` on old drops by matching on `hero_title`/`campaign_type`/date range — but since no read path requires the field, this can ship at any time after launch, independently.

---

# Reuse Summary

Reused as-is, untouched:
- Claim engine: `claim_voucher_for_user`, `claim_pooled`, `claim_personalised`, `api_claim`
- Anti-abuse: kill switch, cooldown, session/IP checks, rate limits (`_check_kill_switch`, `_check_cooldown`, `_check_new_joiner_rate_limits`, etc.)
- Scheduler: `reconcile_drop_statuses`, `sweep_expired_drops`, all referral/affiliate settlement jobs in `scheduler.py`
- Affiliate settlement: everything in `scheduler.py`'s affiliate/referral sections, `affiliate_dashboard_export.py`, `affiliate_leaderboard.py`
- Eligibility/audience evaluators: `is_drop_allowed`, `is_user_eligible_for_drop`, `_drop_audience_type`, `get_claimable_pools`, `get_visible_pools`, `_region_matches_pool`
- Assignment probability: `assign_public_pool_access_once`, `classify_public_pool_segment`, `backend_segment_probability`, `public_pool_probability_for_bot_segment`, and the `config.py` tables
- Drop/voucher insert primitive: the body-insert logic in `admin_create_drop` (to be factored into a shared helper, not rewritten)
- Admin actions: `admin_add_codes`, `admin_drop_actions`
- Audience preview/reporting: `campaign_engine.preview_audience`, `campaign_engine.get_historical_performance`, the existing `campaigns` CRUD blueprint

Net-new: a compiler function, a `drip_schedule` scheduler job, a handful of additive fields, and one additive guard clause in `is_drop_active`.
