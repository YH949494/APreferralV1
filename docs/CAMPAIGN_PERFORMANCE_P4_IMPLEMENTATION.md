# Campaign Performance Intelligence — Phase 4 (P4) Implementation

Status: **Implemented.** Read-only analytics over the existing Campaign
Builder (P2/P3) data model — does not touch claim logic, voucher
allocation, FCFS, pooled/personalised claim, eligibility, anti-abuse,
affiliate settlement, welcome voucher logic, or P3 batch release
execution.

---

## Summary

P4 answers "which campaign is actually effective, not just which campaign
has the most claims" by aggregating existing collections into volume,
speed, quality, abuse-risk, and conversion-proxy metrics, plus a read-only
**campaign score**.

```
Campaign
  -> Voucher Drop(s)          (campaign_builder.py, existing)
  -> Voucher Claims           (vouchers.py, existing)
  -> Performance Analytics    (campaign_performance.py, new — this doc)
```

No new claim ledger is introduced. `campaign_performance.py` only ever
issues `find()` / `count_documents()` calls against:

- `campaign_builder_campaigns` — the campaign doc itself
- `drops` — child drops, matched via `campaign_id` (both P2 single/segment
  drops and P3 batch children are tagged with this field by the existing
  compiler)
- `vouchers` — per-drop inventory (`total` / `claimed` counts, same fields
  `batch_campaign_analytics` in `campaign_builder.py` already reads)
- `voucher_claims` — the canonical claim event ledger (`drop_id`,
  `user_id`, `status`, `claimed_at`, `claim_subnet`,
  `public_pool_subnet_pressure`)
- `users` — `for_bot_segment` / `bot_segment` for quality segmentation
- `qualified_events`, `pending_referrals`, `xp_events` — read-only
  conversion-proxy signals

---

## Architecture

- **Drop discovery**: `_campaign_drops()` queries `drops` by
  `campaign_id == str(campaign["_id"])`. Both `compile_campaign` (P2) and
  `compile_batch_campaign` (P3) already stamp every drop they create with
  this field, so it is the single ground truth for "which drops belong to
  this campaign" — no need to trust the parent doc's cached
  `compiled_drop_ids` / `child_drop_ids` arrays, which can lag on partial
  compiles.
- **Volume**: for each drop, counts vouchers by `type` (`pooled` vs
  personalised aliases) and `status`, identical to the existing
  `batch_campaign_analytics` pattern. A drop counts as "released" once its
  `status`/`batch_status` indicates it went live (see Metrics Definitions).
- **Speed**: derived from `voucher_claims.claimed_at` timestamps sorted
  ascending against the earliest release time recorded on the campaign's
  drops (`batch_actual_release_at` for batch children, `startsAt`
  otherwise).
- **Quality**: every `voucher_claims` row with `status="claimed"` is
  joined to `users.for_bot_segment` (falling back to `bot_segment`),
  normalized with the existing `config.normalize_for_bot_segment`. Only
  the five segments the score formula uses are broken out explicitly;
  everything else (no user doc, blank segment, or an out-of-vocabulary
  segment) is bucketed as `unknown` with an explicit
  `{"unknown_reason": "missing_user_segment"}` marker — never invented.
- **Abuse risk**: repeat claimers (same `user_id` claiming more than once
  across the campaign's drops), same-subnet clusters (`claim_subnet`
  shared by >1 claimant), and suspicious claims (the existing
  `public_pool_subnet_pressure` flag `vouchers.py` already stamps on a
  claim at claim time). Claim-cooldown *hits* are **not** available —
  blocked/cooled-down attempts are never written to `voucher_claims` (only
  successful claims are), so that field always returns
  `{"value": null, "reason": "source_not_available"}`.
- **Conversion proxy**: for each distinct claimant, the *first* claim
  timestamp is compared against `qualified_events.qualified_at`
  (`invitee_id` match), `pending_referrals.created_at`
  (`referrer_id` match), and `xp_events.created_at` where
  `type == "checkin"` (or `reason == "checkin"`) — counting only events
  strictly after the claim. `after_bet_or_withdrawal` has no reliable
  timestamped source in the existing schema and always returns
  `{"value": null, "reason": "source_not_available"}` — P4 never fakes an
  ROI number.
- **Score**: computed from the already-aggregated quality/abuse/conversion
  dicts, per the fixed formula below. Purely a reporting field — nothing
  reads it back into eligibility, claim, or scheduler logic.

---

## Metrics Definitions

### Volume

| Field | Definition |
|---|---|
| `total_vouchers` | Sum of vouchers uploaded across all of the campaign's drops (`vouchers` collection count by `dropId`). |
| `total_released` | Sum of vouchers on drops that have actually gone live (batch: `batch_status` in `released/paused/cancelled`; non-batch: drop `status` not in `upcoming/paused`). |
| `total_claimed` | Sum of vouchers with `status != "free"` (pooled) / `status == "claimed"` (personalised). |
| `total_remaining` | `total_vouchers - total_claimed`, floored at 0. |
| `claim_rate` | `100 * total_claimed / total_released`, or `null` if nothing has been released yet. |
| `release_completion_pct` | `100 * total_released / total_vouchers`. |

### Speed

| Field | Definition |
|---|---|
| `time_to_first_claim_minutes` | Minutes from the earliest release time to the first `voucher_claims.claimed_at`. |
| `time_to_50pct_claimed_minutes` | Minutes from release to the `ceil(total_released/2)`-th claim, sorted by `claimed_at`. `null` until reached. |
| `time_to_sold_out_minutes` | Minutes from release to the `total_released`-th claim (i.e. fully claimed). `null` until reached. |
| `average_claim_speed_minutes` | Average, across the campaign's released drops, of (last claim on that drop − that drop's release time). |

### Quality (claimant segment breakdown)

Counts of successful claims whose claimant resolves (via
`users.for_bot_segment`/`bot_segment`) to `high_value`, `normal_actual`,
`low_value`, `voucher_hunter`, `ghost`, or `unknown` (missing/blank/other).

### Abuse Risk

| Field | Definition |
|---|---|
| `repeat_claimers` | Distinct users who claimed more than once across this campaign's drops. |
| `same_ip_subnet_claims` / `same_ip_subnet_clusters` | Claims that share a non-`"unknown"` `claim_subnet` with at least one other claimant on the same campaign, and the number of such clusters. |
| `claim_cooldown_hits` | Always `{"value": null, "reason": "source_not_available"}` — blocked attempts aren't persisted. |
| `voucher_hunter_claim_share_pct` | `voucher_hunter` claims as a % of claims that resolved to a *known* segment (excludes `unknown` from the denominator). |
| `suspicious_claims` / `suspicious_claim_pct` | Claims flagged `public_pool_subnet_pressure=True` at claim time by the existing anti-abuse code in `vouchers.py`. |

### Conversion Proxy

| Field | Definition |
|---|---|
| `qualified_after_claim` | Claimants who later appear as `qualified_events.invitee_id` with `qualified_at` after their claim. |
| `referral_after_claim` | Claimants who later appear as `pending_referrals.referrer_id` with `created_at` after their claim. |
| `checkin_after_claim` | Claimants with an `xp_events` check-in event after their claim. |
| `after_bet_or_withdrawal` | Always `{"value": null, "reason": "source_not_available"}` — no reliably timestamped bet/withdrawal-after-claim field exists in the synced schema today. |

---

## Scoring Formula

```
quality_score =
    high_value_claims   * 5
  + normal_actual_claims * 3
  + low_value_claims     * 1
  - voucher_hunter_claims * 3
  - ghost_claims          * 1

abuse_penalty = suspicious_claims * 5

conversion_bonus =
    qualified_after_claim * 4
  + referral_after_claim  * 2
  + checkin_after_claim   * 1

campaign_score = quality_score - abuse_penalty + conversion_bonus
```

Badge thresholds (display-only, does not affect eligibility):

| Score range | Badge |
|---|---|
| `>= 50` | High Quality |
| `20`–`49` | Good |
| `-19`–`19` | Neutral |
| `-49`–`-20` | Risky |
| `< -50` | Bad |

`unknown`-segment claims are excluded from `quality_score` entirely (they
carry neither a positive nor negative weight in the formula — they simply
aren't one of the five named segments).

---

## Endpoints

All under the existing Campaign Builder blueprint prefix
(`/api/admin/campaign-builder/...`), matching how P2/P3 endpoints are
already mounted (the product spec's `/admin/campaign-builder/...` path
was adjusted to this existing, already-deployed prefix — see
**Limitations**). Admin-session gated via the same `require_admin()` used
by every other Campaign Builder route.

### `GET /api/admin/campaign-builder/performance`

Query params: `status` (`active|completed|cancelled|all`, default
`active`), `window` (`7d|30d|all`, default `all`), `sort`
(`score|claim_rate|claimed|created_at`, default `created_at`). Returns a
trimmed summary row per campaign (table columns only — see UI section).

### `GET /api/admin/campaign-builder/performance/<campaign_id>`

Query params: `window`. Returns the full performance report: volume,
speed, quality, abuse_risk, conversion_proxy, campaign_score,
score_breakdown, badge, and `child_drops` (per-batch/per-drop rollup).

### `GET /api/admin/campaign-builder/performance/compare?campaign_ids=a,b,c`

Query params: `campaign_ids` (comma-separated), `window`. Returns the full
performance report for each requested id, side by side, plus a
`not_found` list for any id that didn't resolve.

---

## Status Vocabulary Mapping

The product spec's `status=active|completed|cancelled` vocabulary doesn't
map 1:1 onto the two status fields Campaign Builder already has (P2's
`status` field: `draft/compiled/active/archived`; P3's separate
`batch_status`: `draft/compiling/scheduled/active/paused/completed/cancelled`).
`_effective_status()` normalizes both onto the reporting vocabulary:

- Batch campaigns: `scheduled/active/paused -> active`, `completed ->
  completed`, `cancelled -> cancelled`, else `draft`.
- Non-batch campaigns: `active/compiled -> active`, `archived ->
  completed`, else `draft`.

Draft campaigns (nothing compiled/launched yet) are always excluded
unless `status=all` is passed explicitly — there is nothing to analyze
before a campaign has produced a drop.

---

## UI

Under **Campaign Control**, a new **Campaign Performance** nav item opens
a read-only table view (`static/admin-dashboard.html` /
`static/admin-dashboard.js`, prefixed `cp-`/`cp` to avoid colliding with
the existing `cb-`/`cb` Campaign Builder wizard code):

- **Filters**: Status, Window, Sort dropdowns + Refresh.
- **Table columns**: Campaign, Status, Type, Total Vouchers, Released,
  Claimed, Claim Rate, Voucher Hunter %, Actual Player %, Score (with
  badge), Actions (View Details, per-row compare checkbox + Compare
  Selected).
- **Details panel**: summary KPI cards, claim-speed KPIs, segment
  breakdown table, abuse-risk table, conversion-proxy table, score
  explanation table, and (for multi-drop/batch campaigns) a per-batch
  breakdown table.
- **Compare panel**: side-by-side metric table for 2+ selected campaigns.

No button in this view calls a mutating endpoint — every action here is a
`GET`.

---

## DB Fields Used (all pre-existing)

- `campaign_builder_campaigns`: `_id`, `campaign_name`, `campaign_type`,
  `status`, `batch_status`, `release_type`, `created_at`
- `drops`: `_id`, `campaign_id`, `batch_index`, `batch_status`,
  `batch_actual_release_at`, `startsAt`, `status`, `type`, `name`
- `vouchers`: `dropId`, `type`, `status`
- `voucher_claims`: `drop_id`, `user_id`, `status`, `claimed_at`,
  `claim_subnet`, `public_pool_subnet_pressure`
- `users`: `user_id`, `for_bot_segment`, `bot_segment`
- `qualified_events`: `invitee_id`, `qualified_at`
- `pending_referrals`: `referrer_id`, `created_at`
- `xp_events`: `user_id`, `type`/`reason`, `created_at`

---

## Limitations

- **Endpoint prefix**: mounted at the existing
  `/api/admin/campaign-builder/performance...` prefix rather than the
  spec's literal `/admin/campaign-builder/performance`, to match how
  every other Campaign Builder route is actually served in this codebase
  (`main.py` registers `campaign_builder_bp` with no extra prefix; its
  routes already embed the full `/api/admin/campaign-builder/...` path).
- **Segment is "current", not "at claim time"**: quality/abuse
  segmentation reads the claimant's *current* `for_bot_segment`, not a
  historical snapshot as of the claim — no historical segment snapshot
  exists per-claim in the current schema. If a user's segment changes
  after claiming, older campaigns will reflect their latest segment, not
  their segment at claim time.
- **`claim_cooldown_hits` and `after_bet_or_withdrawal`** are always
  `null` with an explicit reason — no source exists for either today (see
  Metrics Definitions). P4 never invents these numbers.
- **N+1 query pattern**: the list endpoint computes full metrics per
  candidate campaign (capped at the 500 most recent). Fine at current
  admin-dashboard scale; would need pre-aggregation if the campaign count
  grows into the thousands.
- **No new indexes were added.** All new queries reuse existing
  fields/indexes (`drops.campaign_id` is already read elsewhere;
  `voucher_claims` is queried by `drop_id`, matching its existing
  `ix_claim_drop_ip_hash`/`ix_claim_drop_subnet` index prefix).

---

## Manual QA Checklist

1. Launch a plain (non-batch) P2 campaign, claim a few vouchers as
   different test users with distinct `for_bot_segment` values, then open
   **Campaign Performance** → confirm Total/Released/Claimed/Claim Rate
   match the Voucher Drop Manager's numbers for that drop.
2. Launch a P3 batch campaign, release 2+ batches, claim some vouchers in
   each, then open **View Details** → confirm the per-batch breakdown
   table matches each child drop's own claim count, and campaign-level
   totals are the sum across batches.
3. Claim as a user with no `users` doc (or blank `for_bot_segment`) →
   confirm they land in the `unknown` bucket with `unknown_reason:
   "missing_user_segment"`, not silently dropped or miscounted.
4. Claim as a user tagged `voucher_hunter`, confirm
   `voucher_hunter_claim_share_pct` and the score's negative
   `voucher_hunter_claims * 3` term move in the expected direction.
5. Trigger a `public_pool_subnet_pressure` claim (shared-subnet burst) →
   confirm `suspicious_claims`/`suspicious_claim_pct` increase and the
   score's `abuse_penalty` reflects it.
6. Select 2+ campaigns via the row checkboxes and click **Compare
   Selected** → confirm the side-by-side table renders all selected
   campaigns' key metrics.
7. Confirm no button/action on this page ever issues anything but a `GET`
   (inspect network tab) — the view has no Launch/Pause/Cancel/Delete
   controls.
8. Confirm existing Campaign Builder / Active Campaigns / Draft Campaigns
   / Compiled Voucher Drops views are visually and functionally unchanged.

---

## Rollback Plan

P4 is fully additive and read-only:

- Remove the `campaign_performance_bp` registration in `main.py` (2
  lines) to disable the endpoints entirely — no data migration, no
  cleanup, since nothing was ever written.
- Remove the **Campaign Performance** nav item / view section from
  `static/admin-dashboard.html` and the corresponding `cp*`
  functions/dispatch lines from `static/admin-dashboard.js` to remove the
  UI.
- Delete `campaign_performance.py` and `test_campaign_performance.py`.
- No index changes to revert (none were added), no schema changes to
  revert (no new fields were written to any collection).
