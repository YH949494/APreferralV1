# Campaign Intelligence & Automation — Phase 5 (P5) Implementation

Status: **Implemented.** Read-only recommendation layer over the existing
Campaign Performance Intelligence (P4). Does not touch claim logic,
voucher allocation, FCFS, pooled/personalised claim, eligibility,
anti-abuse, affiliate settlement, welcome voucher logic, the scheduler,
or P3 batch release execution. Campaign Intelligence never executes a
claim and never launches a campaign.

---

## Summary

P4 answers "what happened?". P5 answers **"what campaign should I run
next?"** — by ranking existing campaigns, generating dynamic insights,
producing a deterministic rule-based recommendation set, scoring segments
and templates and release strategies, finding the best historical launch
hour, and assembling a "playbook" (a recommended next-campaign spec).
Nothing in P5 is stored; every number is recomputed on each request from
`campaign_performance.compute_campaign_performance()` and its own
per-segment breakdowns.

```
Campaign
  -> Campaign Builder                (campaign_builder.py, existing)
  -> Campaign Compiler                (campaign_builder.py, existing)
  -> Voucher Drops                    (campaign_builder.py, existing)
  -> vouchers.py executes             (vouchers.py, existing, untouched)
  -> Performance Analytics (P4)       (campaign_performance.py, existing)
  -> Campaign Intelligence (P5)       (campaign_intelligence.py, new — this doc)
```

`campaign_intelligence.py` never queries a collection directly for claim
data — it reuses `campaign_performance.py`'s existing read helpers
(`_campaign_drops`, `_volume_and_children`, `_quality_and_abuse`,
`_conversion_proxy`, `compute_campaign_performance`) and layers pure,
deterministic arithmetic on top. It issues **zero** `insert_one` /
`update_one` / `update_many` / `delete_one` / `find_one_and_update` calls;
this is asserted directly in `test_campaign_intelligence.py`.

---

## Architecture

- **Enrichment**: `enrich_performance(full)` takes a P4
  `compute_campaign_performance()` result and adds P5-only derived fields
  (`actual_player_pct`, `voucher_hunter_pct`, `conversion_pct`,
  `speed_score`, `ranking_score`) without mutating any P4 field.
- **Rankings** (`build_rankings`): sorts all non-draft campaigns by
  `ranking_score`, tie-broken by `campaign_score` then total claimed.
- **Insights** (`generate_insights`): a fixed set of threshold rules
  evaluated against one campaign's enriched performance dict. Nothing is
  stored — insights are recomputed on every call.
- **Recommendations** (`generate_recommendations`): a deterministic,
  ordered rule table (see below). No ML/AI model — same input always
  produces the same output list.
- **Segment Performance Matrix** (`segment_matrix`): per campaign, re-runs
  `_quality_and_abuse` + `_conversion_proxy` filtered to each of the five
  known quality segments (`high_value`, `normal_actual`, `low_value`,
  `voucher_hunter`, `ghost`) and grades each by conversion rate.
- **Template Ranking** (`template_ranking`): groups all non-draft
  campaigns by `campaign_type` and averages score/claim-rate/conversion/abuse.
- **Release Strategy Ranking** (`release_ranking`): groups by
  `release_type` (P3 batch campaigns) or `"immediate"` (P2 single-drop
  campaigns with no `release_type`), averaging claim speed, conversion,
  abuse, and release completion.
- **Segment Recommendation Engine** (`segment_recommendations`):
  aggregates claim volume and conversion across **all** campaigns per
  segment and computes an ROI figure (see formula below). `voucher_hunter`
  and `ghost` are always forced into `avoid_segments` regardless of their
  computed ROI, per the DO-NOT-TOUCH quality model P4 already encodes.
- **Best Time To Launch** (`best_time_to_launch`): buckets every *released*
  child drop's claim rate by its release hour in `Asia/Kuala_Lumpur` local
  time and recommends the hour with the highest average.
- **Playbook Generator** (`generate_playbook`): assembles a single
  recommended-next-campaign spec from one campaign's enriched performance
  + segment matrix + recommendation list. Output only — nothing is
  launched, scheduled, or written back to any collection.

---

## Ranking Formula

```
campaign_score_norm = clamp((campaign_score + 100) / 2, 0, 100)   # P4's -100..100 score rescaled to 0..100
speed_score          = 100                                  if avg_claim_speed_minutes <= 30
                        0                                    if avg_claim_speed_minutes >= 1440 (24h)
                        100 - (avg_claim_speed_minutes-30)*100/1410   otherwise (linear)
                        50 (neutral)                         if no claim-speed data yet

ranking_score = round(
    0.35 * campaign_score_norm
  + 0.20 * claim_rate
  + 0.15 * actual_player_pct
  + 0.15 * conversion_pct
  + 0.15 * speed_score
  - 0.20 * voucher_hunter_pct
, 1)
```

`actual_player_pct` = share of resolved-segment claims that are
`high_value` or `normal_actual`. `conversion_pct` = share of claimants who
took a tracked post-claim action (qualified a referral, made a referral,
or checked in) — same formula as the segment matrix and template/release
rankings, so numbers are comparable across every P5 view.

Rankings are **not** clamped to 0-100 after the penalty term; they only
need to be internally consistent for sorting, which they are.

---

## Recommendation Rules (deterministic, evaluated in order)

| Condition | Recommendation |
|---|---|
| `claim_rate >= 70` and `voucher_hunter_pct <= 5` | increase batch size +25% |
| `voucher_hunter_pct >= 15` | reduce voucher count -20% |
| `ghost_pct >= 10` (ghost claims / total claimed) | remove ghost segment |
| `actual_player_pct < 50` OR `conversion_pct < 10` | prioritize normal_actual |
| `time_to_50pct_claimed_minutes >= 240` | extend release interval to 2h |

Same input always produces the same recommendation list — no
randomness, no external model calls.

## Insight Rules

GOOD (any that match are shown):
- `actual_player_pct >= 60` → "High actual-player conversion"
- `time_to_50pct_claimed_minutes <= 120` → "Fast claim velocity"
- `voucher_hunter_pct <= 5` → "Low abuse rate"
- `referral_after_claim > 0` → "Strong referral activation"

BAD (any that match are shown):
- `voucher_hunter_pct >= 15` → "High voucher hunter participation"
- `time_to_50pct_claimed_minutes >= 240` → "Slow claim velocity"
- `referral_after_claim in (0, None)` and claims exist → "Low retention"
- `conversion_pct < 10` → "Weak conversion"

## Segment Matrix Grade

```
grade = "A"   if conversion_pct >= 25
        "B"   if conversion_pct >= 15
        "C"   if conversion_pct >= 5
        "F"   otherwise
        "N/A" if the segment has zero claims
```

## Segment ROI (global Segment Recommendation Engine)

```
voucher_hunter_exposure_pct = average voucher_hunter_claim_share_pct across all non-draft campaigns
roi(segment) = avg_conversion_pct(segment) / max(voucher_hunter_exposure_pct, 1.0)
```

`recommended_segments` = top-2 non-abuse segments (excludes
`voucher_hunter`/`ghost` by construction) with `roi >= 1.0`, falling back
to the top-2 by ROI if none clear that bar. `avoid_segments` always
contains `voucher_hunter` and `ghost`, plus any other segment whose ROI
is below `0.5`.

## Playbook Confidence

```
confidence = "High"    if total_claimed >= 100
             "Medium"  if total_claimed >= 30
             "Low"     otherwise
```

---

## Endpoints (all GET, all read-only)

| Endpoint | Purpose |
|---|---|
| `GET /api/admin/campaign-builder/intelligence/rankings` | Campaign Effectiveness Ranking (feature 1) + per-row insights |
| `GET /api/admin/campaign-builder/intelligence/campaign/<id>` | Single campaign: rank, insights (2), recommendations (3), segment matrix (4), playbook (9) |
| `GET /api/admin/campaign-builder/intelligence/templates` | Campaign Template Ranking (feature 5) |
| `GET /api/admin/campaign-builder/intelligence/releases` | Release Strategy Ranking (feature 6) |
| `GET /api/admin/campaign-builder/intelligence/segments` | Segment Recommendation Engine / global ROI (feature 7) |
| `GET /api/admin/campaign-builder/intelligence/best-time` | Best Time To Launch (feature 8) |
| `GET /api/admin/campaign-builder/intelligence/playbook` | Playbook Generator (feature 9); optional `?campaign_id=` (defaults to the top-ranked campaign) |

All endpoints accept `?window=all|7d|30d` (same vocabulary as P4) and
require the existing admin session (`require_admin()` from `vouchers.py`,
unchanged). None of them accept POST/PUT/PATCH/DELETE —
`test_no_mutating_http_methods_registered` asserts every registered rule
in `campaign_intelligence_bp` only exposes GET.

---

## UI

Added under **Campaign Control** in the admin dashboard sidebar:

**Campaign Intelligence** (`static/admin-dashboard.html` /
`static/admin-dashboard.js`, view id `campaignIntelligence`), with tabs:
Rankings, Insights, Recommendations, Segments, Templates, Release
Strategy, Best Time, Playbook. Insights/Recommendations/per-campaign
Segments tabs require picking a campaign first (via "Details" from the
Rankings tab); Segments/Templates/Releases/Best Time/Playbook otherwise
render global, all-campaign views.

---

## Limitations

- Conversion proxy inherits every limitation already documented in P4
  (`after_bet_or_withdrawal` is `source_not_available`; claim-cooldown
  hits are not recorded anywhere).
- Best-time-to-launch only has data for campaigns whose drops recorded a
  release timestamp (`batch_actual_release_at` or `startsAt`); campaigns
  with none return an empty hour table and an "insufficient data" message
  rather than a fabricated recommendation.
- Segment ROI and template/release rankings average across however many
  historical campaigns exist — with very few campaigns these averages are
  noisy by construction; there is no minimum-sample-size gate (documented
  behavior, not a bug).
- The ranking/recommendation weights are a documented, versioned rule set
  — not a statistically fit model. Tuning them is a deliberate, reviewed
  change to this file, not a runtime configuration option.

---

## Manual QA

1. Log in to `/static/admin-dashboard.html` as an admin.
2. Open **Campaign Control → Campaign Intelligence**.
3. **Rankings** tab: confirm campaigns are listed with a rank, score, and
   metrics; confirm the campaign with the best actual-player/claim-rate
   mix ranks above one with heavy voucher-hunter participation.
4. Click **Details** on a campaign → **Insights** tab shows ✓/⚠ badges
   that match its metrics (e.g. high voucher-hunter share triggers "High
   voucher hunter participation").
5. **Recommendations** tab shows a deterministic list (reload the page,
   confirm the same list appears).
6. **Segments** tab: confirm `voucher_hunter` and `ghost` always appear
   under "Limit", never under "Prioritize".
7. **Templates** / **Release Strategy** tabs: confirm one row per
   `campaign_type` / release strategy with averaged metrics.
8. **Best Time** tab: confirm an hour table (or an "insufficient data"
   message if no campaigns have recorded release timestamps yet).
9. **Playbook** tab: confirm a recommended template/audience/release/
   voucher-count/confidence block renders, and that no button anywhere on
   this page performs a POST/launch action.
10. Confirm Campaign Builder, Campaign Performance, Active/Draft
    Campaigns, and Voucher Drop Manager all still behave exactly as
    before (P5 added a new read-only blueprint; it did not modify any
    existing route).

---

## Rollback Plan

P5 is fully additive and isolated to three touch points:

1. `campaign_intelligence.py` — new file, can be deleted outright.
2. `main.py` — two added lines (`from campaign_intelligence import
   campaign_intelligence_bp` / `app.register_blueprint(...)`). Removing
   them fully un-registers all seven P5 endpoints with no effect on any
   other route.
3. `static/admin-dashboard.html` / `static/admin-dashboard.js` — one nav
   button, one `<section>`, and one JS module (view id
   `campaignIntelligence`). Removing the nav button/section/JS block
   removes the UI entry point; the backend endpoints are independently
   safe to leave running or remove.

Because P5 never writes to any collection and never calls into
`vouchers.py`'s claim/eligibility/scheduler code paths, rollback carries
zero data-migration risk — there is nothing to undo in the database.
