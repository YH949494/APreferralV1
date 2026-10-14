# Affiliate V2 reporting snapshot (APReferral -> Databot)

Review package. Nothing here was deployed, activated, migrated or run against
production. The feature is **off by default** (`AFFILIATE_V2_REPORT_ENABLED`).

## Integration today, and the missing return path

| Direction | Mechanism | State |
|---|---|---|
| Databot -> APReferral (evidence) | APReferral's `uim_redemption_sync` pulls Databot's committed UIM batches over `Authorization: Bearer $DATABOT_API_KEY` from `/internal/uim-redemption-feed/v1/*` (Fly private network) into `uim_redemption_*` | unchanged |
| Databot -> APReferral (Mongo) | Databot's shared client reads APReferral source collections directly | unchanged |
| **APReferral -> Databot (V2 results)** | **none** - Databot's Affiliate Performance table could only show the historical `Qualified` figure; V2 decisions live in APReferral (`qualified_events`, `welcome_redemption_evidence`, `pending_referrals`) and the only place that summarised them was the admin Qualification Preview (a full, in-memory replay: not something to run per dashboard load) | **added here** |

This PR adds the missing leg: a precomputed, minimal, read-only snapshot and a
private route Databot reads. See the companion Databot PR for the consumer.

## What is published (and what is not)

Envelope: `contract`, `rule_active`, `rule_mode`, `rule_version`, `cohort`,
`status`, `snapshot.{generation,generated_at,age_seconds,stale}`,
`source.{status,problems,last_sync_success_at}`, `uim_coverage_end` (GMT+8),
`totals`, `affiliates_total`, `truncated`.
Per affiliate: `affiliate_id`, `v2_qualified`, `v2_pending`, `v2_rejected`,
`duplicate_account_excluded`, `seven_day_turnover` (exact decimal string),
`seven_day_turnover_status` (`complete | partial | unavailable`).

Never published, stored in the snapshot or logged by this module: voucher
codes, gaming Account IDs, account keys, tokens, usernames. The snapshot reads
only Telegram invitee/referrer IDs and the exact `after_total_bet_amount`.

## Metric definitions (one state per invitee, precedence 1 -> 4)

1. **V2 Qualified** - a `qualified_events` row with `rule_version == welcome_redemption_v2`. Preview/would-pass rows are not read.
2. **Duplicate Account** - V2 evidence finalised `duplicate_account` (canonical registry refused it), no V2 qualification. The earlier owner is untouched.
3. **V2 Rejected** - V2 evidence finalised `rejected`: only what `assess_evidence` already decided as an authoritative failure (turnover < 80, `coupon_new_player = 0`, unsuccessful redemption, ...). Held evidence (`pending_review`) is never rejected.
4. **V2 Pending** - every other invitee with an active `pending_referrals` row (`ACTIVE_PENDING_STATUSES`): no evidence yet, incomplete/immature/unavailable evidence, feed failure, review/held.

An invitee with a legacy (non-V2) qualification is in no V2 column. Databot
re-implements no gate: the snapshot reports persisted decisions.

**7D Turnover** = exact sum of `after_total_bet_amount` over actual V2-qualified
invitees, read from the evidence row linked by `qualified_events.evidence_id`
(one event per invitee and one evidence per `(source, source_ref)`, so repeated
UIM rows/batches/refreshes cannot multiply it). Summed with `decimal` at 120
digits of precision with Inexact trapped - no floats, no rounding; Databot
rounds to 2dp for display only. A qualified invitee whose amount cannot be read
makes the affiliate's turnover `partial`, never a silent smaller number.
Turnover is `unavailable` (null) when the UIM source is unavailable, and
`partial` when the source is stale/partially synced, the snapshot is stale or
the build was truncated.

## Cohort

The V2 columns use **referral `joined_at`** as the single cohort timestamp: the
`created_at_utc` of the invitee's original attribution-valid `pending_referrals`
row (the record qualification freezes attribution to; for qualified invitees the
row named by `referral_pending_id`), bucketed by **GMT+8 calendar day**. The
route takes inclusive `start_date`/`end_date` days - exactly what Databot's
month/week/range filter resolves to - so month, week and range all work and the
GMT+8 boundary is exact (`2026-09-30T16:00:00Z` is 1 Oct). Pending rows have no
qualification time and are bucketed by join like everything else. The historical
`Qualified` column in Databot keeps its qualification-date cohort; the two are
deliberately not expected to reconcile.

## Disabled / paused

Before launch (`welcome_redemption_v2` control not active/paused with a passed
cutoff) the route returns `rule_active: false`, `status: inactive` and
`totals/affiliates: null`; pending/manual rows are not classified, and the
refresh job stores no rows. Paused-after-launch still reports recorded
decisions with `rule_active: false`, `rule_mode: paused`.

## Snapshot lifecycle and recovery

* `affiliate_v2_report.run_scheduled_refresh` is a new step at the end of the existing 5-minute job. No-op unless `AFFILIATE_V2_REPORT_ENABLED=true`; rebuilds at most every `AFFILIATE_V2_REPORT_REFRESH_SECONDS` (600), immediately when the rule state changes, never more than once a minute.
* Leased (10 min) so overlapping workers cannot interleave. Cost: one pass over V2 qualified events, V2 terminal evidence and active pending referrals (cap `AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING`, default 250k, truncation is reported as `partial`), plus chunked lookups by existing indexes.
* **Replace, not accumulate**: rows of a new generation are written, then one pointer swap on `affiliate_v2_report_state`, then older generations are deleted. A failed build leaves the previous generation current (`last_error` recorded). Re-running is idempotent; an invitee moving pending -> terminal simply appears in its new column next generation.
* The read path never builds, previews or touches source collections. If a refresh swaps generations mid-read the reader retries instead of reporting zeros.
* Recovery: nothing to repair. Delete `affiliate_v2_report_state`/`_rows` at any time (they are derived) and the next scheduled pass rebuilds them. Turning the flag off stops the job and makes the route 404.
* Writes are confined to `affiliate_v2_report_state` and `affiliate_v2_report_rows`. No `qualified_events`, `referral_events`, `affiliate_ledger`, XP, registry or evidence document is written.
* Indexes: two, both on the **new** rows collection (`(generation, affiliate_id, day)` unique and `(generation, day)`), created lazily by the first flag-on refresh. No index is added to any existing collection - the queries use `qualified_events.rule_version` (`aq_qualified_effects_pending` prefix), `welcome_redemption_evidence.status` (`evidence_status_created`), `pending_referrals.status` (`pending_by_time`) and `aq_pending_by_invitee`, all existing. Not verified with `explain` against production data.

## Authentication and network

`GET /internal/affiliate-v2-report/v1/summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD[&limit=n]`

* 404 unless `AFFILIATE_V2_REPORT_ENABLED=true` **and** a secret exists: `AFFILIATE_V2_REPORT_TOKEN`, else the shared `DATABOT_API_KEY` (already on both services).
* `Authorization: Bearer`, constant-time compare, else 401.
* Requests carrying Fly public-edge headers (`Fly-Client-IP`, `Fly-Forwarded-Port`) get 403 unless `AFFILIATE_V2_REPORT_ALLOW_PUBLIC=true` - the same rule as Databot's UIM feed.
* `Cache-Control: no-store`; response capped by `limit` (default 2000, max 5000, `truncated` flag; totals always cover every affiliate); window <= 400 days; errors are generic.
* **Deployment prerequisite (not changed here):** `fly.toml` binds gunicorn to `0.0.0.0:8080` (IPv4). Fly `.internal` addressing is IPv6 (6PN), so for Databot to reach `http://apreferralv1.internal:8080` the web process must also listen on `[::]:8080` (e.g. add `--bind [::]:8080`). That is a deploy-affecting change and was deliberately left out; until then the only reachable path is the public edge with `AFFILIATE_V2_REPORT_ALLOW_PUBLIC=true` (token-authenticated TLS), which is not recommended.

## Tests

`test_affiliate_v2_report.py` (added to the PR gate): real pipeline (Databot-shaped rows -> sync -> live validator -> snapshot) for qualified / legacy exclusion / 79.99, 80, 80.00 / incomplete seven-day data / feed failure / `coupon_new_player = 0` / duplicate account / exact Decimal sums / repeated rows and batches / GMT+8 month boundaries / inactive, scheduled and paused / replace-not-accumulate / failed-refresh keeps last good / stale and partial coverage / generation-swap race / no raw code or Account ID or token anywhere / writes confined / read path never runs the preview / route auth, edge refusal and bad input.
