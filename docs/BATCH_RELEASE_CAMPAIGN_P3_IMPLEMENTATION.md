# Batch Release Campaign — Phase 3 (P3) Implementation

Status: **Implemented.** Compiles into the existing `vouchers.py` drop
engine — does not redesign claim logic, FCFS allocation, eligibility, the
scheduler core, or affiliate settlement.

---

# Summary

P3 adds **batch release** support on top of the P2 Campaign Builder: one
parent campaign compiles into **N child voucher drops**, released over
time instead of all at once.

```
Campaign (draft, release_type set)
    -> Batch Compiler (compile_batch_campaign)
    -> N child Voucher Drops (vouchers.create_drop_from_spec, one call per batch)
    -> vouchers.py executes normally per child drop (claim/eligibility/scheduler/affiliate untouched)
    -> Batch Release Tick (new, small scheduler job) flips each child drop
       live at its scheduled time, or on manual "Release Next Now"
```

Example: "Weekend Reload", 500 vouchers, 50 every hour → 10 child drops
(`weekend_reload_001` … `weekend_reload_010`), one released per hour.

Non-batch (P2) campaigns are completely unaffected — a campaign is only
routed through the batch path when it has a valid `release_type` field.

---

# Architecture

- **Compiler** (`compile_batch_campaign` in `campaign_builder.py`): splits
  `total_vouchers` into `batch_count = ceil(total_vouchers / batch_size)`
  chunks, computes a release schedule, and calls
  `vouchers.create_drop_from_spec()` once per batch — the exact same
  insert primitive P2 and `admin_create_drop` use. No parallel drop-insert
  logic was added.
- **Release timing**: every child drop is created and then immediately
  forced to `status = "paused"` (an existing drop status the scheduler's
  `reconcile_drop_statuses` sweep already leaves untouched — it only
  auto-transitions `upcoming/live/active` drops). Release timing is
  entirely owned by this module flipping `paused -> active/upcoming`
  through `_release_next_batch()`, either from the new **batch release
  tick** (automatic types) or from the **Release Next Now** admin action
  (manual type, or a manual override on any type). The scheduler core
  itself was never touched.
- **Analytics**: aggregated live from the existing `drops` and `vouchers`
  collections (via `batch_parent_id`/`batch_index` tags on `drops`) — no
  parallel claim ledger.

---

# Fields Added (additive only)

## `campaign_builder_campaigns` (parent campaign document)

| Field | Meaning |
|---|---|
| `release_type` | `interval_minutes\|hourly\|daily\|weekly\|manual\|custom`. Presence of a valid value marks this as a batch campaign. |
| `batch_size` | Vouchers per batch. |
| `total_vouchers` | Total vouchers across all batches. |
| `batch_count` | `ceil(total_vouchers / batch_size)`, computed at compile time. |
| `release_interval_minutes` | Only used for `release_type="interval_minutes"`. |
| `release_schedule` | List of ISO datetime strings, one per batch (input for `custom`, output for all types once compiled). |
| `batch_status` | `draft\|compiling\|scheduled\|active\|paused\|completed\|cancelled`. Kept separate from the existing P2 `status` field so non-batch campaigns are unaffected. |
| `child_drop_ids` | Ordered list of compiled child drop ids. |
| `next_release_at` | UTC datetime of the next due batch (or `null`). |
| `released_batches` | Count of batches released so far. |
| `compiled_at` / `paused_at` / `cancelled_at` | Timestamps. |

## `drops` (child drop documents — additive to the existing schema)

| Field | Meaning |
|---|---|
| `campaign_id` / `campaign_name` | Same as P2. |
| `batch_parent_id` | Parent campaign `_id` (string). |
| `batch_index` | 1-based position in the batch sequence. |
| `batch_count` | Total batches in the parent campaign. |
| `batch_release_at` | This batch's scheduled release time (UTC datetime, or `null` for manual). |
| `batch_status` | `scheduled\|released\|paused\|cancelled` — bookkeeping only, does not replace the drop's own `status` field. |

`_admin_drop_summary` in `vouchers.py` now additionally passes through
these fields (existing drops without them are unaffected).

---

# Endpoints Added

All under the existing `campaign_builder_bp` blueprint, admin-session
protected exactly like every other Campaign Builder route:

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/admin/campaign-builder/campaigns` | Extended: accepts `release_type`, `total_vouchers`, `batch_size`, `release_interval_minutes`, `release_schedule` to create a batch draft. |
| PUT | `/api/admin/campaign-builder/campaigns/<id>` | Extended: same batch fields, only while `batch_status` is `draft`/unset. |
| POST | `/api/admin/campaign-builder/campaigns/<id>/preview` | Extended: returns `preview_batch_campaign()` output when `release_type` is set (total vouchers, batch size, batch count, estimated duration, full release schedule, first/last release time, audience, drop type, region). |
| POST | `/api/admin/campaign-builder/campaigns/<id>/compile` | Extended: routes to `compile_batch_campaign()` when `release_type` is set. Still requires the exact literal `"LAUNCH"` confirmation. |
| POST | `/api/admin/campaign-builder/campaigns/<id>/pause` | **New.** Parent → `paused`. Future releases stop; already-live drops untouched. |
| POST | `/api/admin/campaign-builder/campaigns/<id>/resume` | **New.** Parent → `scheduled`/`active`. Overdue batches are caught up automatically by the next tick. |
| POST | `/api/admin/campaign-builder/campaigns/<id>/cancel` | **New.** Parent → `cancelled`. Unreleased child drops are expired; released/claimed drops untouched. |
| POST | `/api/admin/campaign-builder/campaigns/<id>/release-next` | **New.** Releases exactly one (the next due) child drop immediately. |
| GET | `/api/admin/campaign-builder/campaigns/<id>/analytics` | **New.** Aggregated parent + per-batch child drop table, computed from `drops`/`vouchers`. |

---

# Scheduler Behavior

One new job, `batch_release_tick`, registered in `main.py` next to the
existing `drop_status_reconcile` job (same `CronTrigger(minute="*/1")`
cadence, same scheduler instance — no new scheduler, no change to
scheduler core):

```python
scheduler.add_job(
    batch_release_tick,
    trigger=CronTrigger(minute="*/1", timezone=KL_TZ),
    id="batch_release_tick",
    name="Batch Release Campaign Tick",
    replace_existing=True,
)
```

`batch_release_tick()`:
1. Acquires a lock in the existing `scheduler_locks` collection (same
   `find_one_and_update` CAS + TTL pattern `main.py`'s
   `acquire_scheduler_lock` uses, duplicated locally to avoid a circular
   import between `campaign_builder.py` and `main.py` — same collection,
   same semantics).
2. Finds campaigns with `batch_status in (scheduled, active)`,
   `release_type != manual`, and `next_release_at <= now`.
3. For each, repeatedly calls `_release_next_batch()` while the next batch
   is still due, capped at `batch_count` iterations — this is what makes
   missed ticks (app downtime, a paused campaign that's overdue on
   resume) catch up automatically instead of getting stuck.

`_release_next_batch()` is a compare-and-swap on the child drop's
`batch_status` (`scheduled -> released`), so calling it twice for the
same batch (tick racing a manual "Release Next Now" click, a rerun after
a crash) releases that batch at most once.

Manual-type campaigns are never touched by the tick — every release goes
through the same `_release_next_batch()` via the "Release Next Now"
button.

---

# Safety Rules

- **Preview before launch**: `preview_batch_campaign()` shows total
  vouchers, batch size, batch count, estimated duration, full release
  schedule, first/last release time, audience, drop type, region
  restriction, and any blocking validation errors (`launchable: false`).
- **Exact `LAUNCH` confirmation** required to compile, identical to P2.
- **Insufficient codes blocks launch**: `validate_batch_params()` rejects
  compile if uploaded codes/assignments < `total_vouchers`, before any
  drop is created.
- **Idempotent compile**: a compare-and-swap on `batch_status`
  (`draft -> compiling`) blocks a second concurrent/duplicate LAUNCH
  request. If the process crashes mid-compile, ground truth for "which
  batches already exist" is read from `drops` (`batch_parent_id` +
  `batch_index`), not the parent doc's cache — a retried compile call
  resumes from the first missing batch index instead of duplicating
  drops.
- **Pause is reliable without touching scheduler core**: every child drop
  starts as `status="paused"`; the scheduler's own
  `reconcile_drop_statuses` never auto-activates a paused drop. Only
  `_release_next_batch()` flips it live, and it only runs when the parent
  is `scheduled`/`active` — so pausing the parent immediately and
  permanently stops future auto-releases.
- **Cancel never touches live vouchers**: only child drops still in
  `batch_status="scheduled"` are expired on cancel; already-released
  drops and any claimed vouchers are left exactly as they are.
- **Release Next Now is single-batch and idempotent**: it always targets
  the one child drop with the lowest `batch_index` still `scheduled`, via
  the same CAS as the tick — clicking it twice releases two *different*
  batches, never the same batch twice.

---

# UI Changes

`static/admin-dashboard.html` / `.js` (Campaign Builder wizard, unchanged
step numbering):

- **Step 3 (Release)**: added a "Batch Release Campaign" checkbox that
  reveals Total Vouchers / Batch Size / Release Type / Interval-minutes /
  Custom-schedule fields, additive to the existing Immediate/Scheduled
  release style controls (unchanged for non-batch campaigns).
- **Step 5 (Preview)**: renders the batch-specific safety preview
  (batch count, estimated duration, full schedule, first/last release
  time, launchability) when the campaign is a batch campaign; falls back
  to the existing P2 preview otherwise.
- **Step 6 (Launch)**: unchanged `LAUNCH` confirmation flow; result
  message now reports child drop count + how many released immediately.
- **Active Campaigns view**: batch campaigns show a `batch: <status>`
  badge, released/total batch counter, and **Pause / Resume / Cancel /
  Release Next Now** buttons plus an inline **Analytics** panel (parent
  KPIs + per-batch child drop table), all wired to the new endpoints.

Nothing was removed: Voucher Drop Manager, the legacy MiniApp admin, the
legacy segment-targeting Campaigns dashboard, and P2's own wizard/list
views are all untouched.

---

# Idempotency / Failure Safety — how each required case is handled

| Case | Handling |
|---|---|
| Repeated LAUNCH request | CAS `draft -> compiling` on the parent; a second concurrent call sees `batch_status != draft` and is rejected with `not_draft`. |
| Compiler partially creates child drops | Ground truth is `drops` (`batch_parent_id`+`batch_index`), not the parent cache; retry resumes from the first missing index. |
| Scheduler runs twice | `_release_next_batch()` CAS on the child's `batch_status`; second run finds nothing left in `scheduled` state for that slot. |
| App crashes midway | Same as "partially creates child drops" — `batch_status` stays `compiling`, safe to retry the compile call. |
| Uneven final batch | `split_codes_into_batches()` takes the remainder in the last chunk (e.g. 525/50 → 11 batches, last has 25). |
| Insufficient codes | Blocked pre-compile by `validate_batch_params()`, campaign stays `draft`. |
| Pause during active campaign | Parent → `paused`; tick's query excludes non-`scheduled/active` parents. |
| Cancel after some batches released | Only `scheduled` children are touched; released/claimed ones are untouched. |
| Manual release clicked twice | `_release_next_batch()` CAS — the second click releases the *next* batch, not the same one again. |

---

# Rollback Plan

- Remove the batch UI additions from `admin-dashboard.html`/`.js` (Step 3
  batch fields, Active Campaigns batch actions/analytics panel) to hide
  the feature from admins. The existing P2 wizard/list views are
  untouched and keep working.
- Remove the `batch_release_tick` job registration in `main.py` to stop
  automatic releases (manual "Release Next Now" would still work via the
  API if the blueprint stays registered).
- No data migration needed to roll back: all P3 fields are additive.
  Existing P2 campaigns and drops have none of these fields and are
  completely unaffected either way.
- **No voucher rollback, no scheduler rollback, no claim-engine
  rollback** — child drops are ordinary `drops`/`vouchers` documents and
  keep working exactly like manually-created or P2-compiled drops.

---

# Manual QA Checklist

(Requires a running app + real MongoDB — automated tests in
`test_batch_release_campaign.py` cover the compiler/release logic against
an in-memory fake Mongo.)

- [ ] Campaign Builder Step 3 shows the Batch Release checkbox and fields
- [ ] Preview (Step 5) for a batch campaign shows batch count, schedule,
      first/last release time, and blocks Launch when codes are
      insufficient
- [ ] Launch with 500 vouchers / batch size 50 / hourly compiles 10 child
      drops, first one live immediately
- [ ] Compiled child drops appear in Voucher Drop Manager and Compiled
      Voucher Drops, tagged with the parent campaign
- [ ] Active Campaigns shows the batch status badge and released/total
      counter
- [ ] Pause stops the next scheduled release from going live; Resume
      continues from the next unreleased batch (including catch-up if a
      release was missed while paused)
- [ ] Release Next Now (manual campaign) brings exactly one child drop
      live per click
- [ ] Cancel disables all unreleased child drops but leaves an
      already-claimed voucher/drop intact
- [ ] Analytics panel shows total/released/claimed/remaining vouchers and
      the per-batch child drop table, matching what Voucher Drop Manager
      reports for those same drops
- [ ] Claim flow for a released child drop behaves identically to any
      other voucher drop (existing engine, unchanged)
