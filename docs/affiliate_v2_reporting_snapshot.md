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
`source.{status,problems,last_sync_success_at}`, `uim_coverage` (below), `totals`, `affiliates_total`, `truncated`.

`uim_coverage` keeps **four different timestamps apart**: *final coverage* (`final_coverage_start/end`: the inclusive
GMT+8 days the data owner declared as a complete final UIM report, per `basis`), `latest_observed_redemption_at`
(newest redemption present in rows - **not** coverage), `source.last_sync_success_at` and `snapshot.generated_at`.
Also `status` (`complete | inferred_unverified | partial | coverage_gap | unavailable | none`), `intervals`, `gaps`
and `absence_rejection.{active,blockers}`. `uim_coverage_end` is a **deprecated alias** that now means the *final*
coverage end (`<date>T23:59:59+08:00`) and is `null` unless the status is `complete`; it no longer carries the
latest row. Contract and rules: [uim_coverage_contract.md](uim_coverage_contract.md).
Per affiliate: `affiliate_id`, `v2_qualified`, `v2_pending`, `v2_rejected`,
`duplicate_account_excluded`, `seven_day_turnover` (exact decimal string),
`seven_day_turnover_status` (`complete | partial | unavailable`).

Field names above are the **live** vocabulary; in `report_mode: preview` the same five
metrics are `would_qualify`, `would_remain_pending`, `would_reject`, `would_be_duplicate`,
`projected_7d_turnover` (+ `projected_turnover_status`), and the envelope always carries
`report_mode`, `rule_active` and `effects_written`.

Never published, stored in the snapshot or logged by this module: voucher
codes, gaming Account IDs, account keys, tokens, usernames. The snapshot reads
only Telegram invitee/referrer IDs and the exact `after_total_bet_amount`.

## Metric definitions - live mode (one state per invitee, precedence 1 -> 4)

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

## Pre-launch preview (`report_mode: preview`)

While V2 is **not launched** (never activated, or activated with a cutoff still in
the future) the same refresh job builds a **preview** instead of publishing nothing.
Every response says so: `report_mode: "preview"`, `rule_active: false`,
`effects_written: false`. Preview rows use their own field names - `would_qualify`,
`would_remain_pending`, `would_reject`, `would_be_duplicate`,
`projected_7d_turnover` / `projected_turnover_status` - and never `v2_*`, so a preview
can not be read as an actual result. A snapshot of one mode is never served as the
other.

* **Evaluator:** `affiliate_qualification_preview._replay`, the block that used to live
  inside `build_preview` (extracted unchanged; the admin preview calls it too). It
  replays committed Databot evidence through `affiliate_qualification.assess_evidence` -
  the validator live processing runs - with the lifetime Account ID registry simulated
  (earliest redemption wins, later ones are duplicates). Nothing re-implements a gate.
* **Population:** every active pending/manual referral (`ACTIVE_PENDING_STATUSES`,
  pre-cutoff joins included). Invitees with an existing qualification are unchanged
  history and excluded.
* **One state per invitee**, Qualified > Duplicate > Rejected > Pending. No evidence,
  held/incomplete evidence stay pending. Confirmed `coupon_new_player = 0`
  and completed turnover below 80 are rejected. An invitee with **no row at all** is rejected only inside proven
  complete coverage under the owner's attested absence policy (the same `uim_coverage.decide_absences` live uses);
  otherwise pending. If the source is blocked, stale,
  partially synced, its last sync failed, or the build was truncated, **everyone stays
  pending** - exactly how live processing holds.
* **7D turnover** is the exact Decimal sum of the amounts on the evidence rows that
  would qualify (`partial` when the source is not fully healthy, `unavailable` when it
  is blocked).
* **No writes:** the build phase receives `ReadOnlyDb`, a handle whose collections expose
  reads only (any write raises). A test also snapshots every other collection before and
  after. The only writes are the replaced snapshot generation in
  `affiliate_v2_report_state` / `_rows`.
* **Never in a request:** the replay runs only in the leased job (`run_scheduled_refresh`);
  the route reads the stored snapshot (tests make the replay fail if it is reached).
  Cost is bounded by `PREVIEW_MAX_SOURCE_ROWS` (200k committed rows) and
  `AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING`; truncation is reported as `partial`.
* **Activation:** the first refresh after the rule launches (the job refreshes at once
  when the rule state changes) keeps the last preview generation as an **audit
  snapshot** - rows stay in `affiliate_v2_report_rows` (`mode: preview`) and
  `affiliate_v2_report_state.preview_audit` records generation, generated/preserved
  times, source status and totals; the live response carries it as
  `pre_launch_preview_audit`. The report then switches to actual persisted decisions
  and `v2_*` names on its own. The audit is the last preview built before activation
  (up to one refresh interval, default 10 minutes, older than the activation instant).
* A passed cutoff that is not a launched V2 rule (e.g. explicitly disabled afterwards)
  is `report_mode: inactive`: no numbers, nothing guessed. Paused-after-launch keeps
  reporting the recorded decisions (`report_mode: live`, `rule_active: false`).

## Snapshot lifecycle and recovery

* `affiliate_v2_report.run_scheduled_refresh` is a new step at the end of the existing 5-minute job. No-op unless `AFFILIATE_V2_REPORT_ENABLED=true`; rebuilds at most every `AFFILIATE_V2_REPORT_REFRESH_SECONDS` (600), immediately when the rule state changes, never more than once a minute.
* The scheduler step is the only caller of the (potentially heavy) preview replay.
* Leased (10 min) so overlapping workers cannot interleave. Cost: one pass over V2 qualified events, V2 terminal evidence and active pending referrals (cap `AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING`, default 250k, truncation is reported as `partial`), plus chunked lookups by existing indexes.
* **Replace, not accumulate**: rows of a new generation are written, then one pointer swap on `affiliate_v2_report_state`, then older generations are deleted. A failed build leaves the previous generation current (`last_error` recorded). Re-running is idempotent; an invitee moving pending -> terminal simply appears in its new column next generation.
* The read path never builds, previews or touches source collections. If a refresh swaps generations mid-read the reader retries instead of reporting zeros.
* Recovery: nothing to repair. Delete `affiliate_v2_report_state`/`_rows` at any time (they are derived) and the next scheduled pass rebuilds them. Turning the flag off stops the job and makes the route 404.
* Writes are confined to `affiliate_v2_report_state` and `affiliate_v2_report_rows`. No `qualified_events`, `referral_events`, `affiliate_ledger`, XP, registry or evidence document is written.
* Indexes: two, both on the **new** rows collection (`(generation, affiliate_id, day)` unique and `(generation, day)`), created lazily by the first flag-on refresh. No index is added to any existing collection - the queries use `qualified_events.rule_version` (`aq_qualified_effects_pending` prefix), `welcome_redemption_evidence.status` (`evidence_status_created`), `pending_referrals.status` (`pending_by_time`) and `aq_pending_by_invitee`, all existing. Not verified with `explain` against production data.

## Authentication and network

`GET /internal/affiliate-v2-report/v1/summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD[&limit=n]`

* 404 unless `AFFILIATE_V2_REPORT_ENABLED=true` **and** a secret exists: `AFFILIATE_V2_REPORT_TOKEN`, else the shared `DATABOT_API_KEY`.
* `Authorization: Bearer`, constant-time compare, else 401. Requests with Fly public-edge headers (`Fly-Client-IP`, `Fly-Forwarded-Port`) get 403 unless `AFFILIATE_V2_REPORT_ALLOW_PUBLIC=true`.
* `Cache-Control: no-store`; capped by `limit` (default 2000, max 5000, `truncated` flag; totals cover everyone); window <= 400 days; generic errors.

### Private IPv6 listener (Fly `.internal`)

Fly private networking is IPv6 (6PN). The web process used to bind IPv4 only, so
`apreferralv1.internal` could not reach it. Fix, designed not to touch the public
listener:

* `fly.toml` web command: `--bind 0.0.0.0:8080` is replaced by `-c gunicorn_web.conf.py`
  (everything else - workers, threads, timeouts, log format - is byte-identical).
  `gunicorn_web.conf.py` always binds **`0.0.0.0:8080` first**, exactly as before, so
  the edge `[[services]]` mapping (80/443 -> 8080) and the `/healthz` check are unchanged.
* A **second, separate** listener `[::]:<AFFILIATE_V2_REPORT_PRIVATE_PORT>` is added only if
  `AFFILIATE_V2_REPORT_ENABLED=true` **and** the port is set (1024-65535, not 8080). With
  those unset the process is byte-for-byte the old behaviour. IPv6-only binding is not
  assumed to keep IPv4: the IPv4 public socket is a distinct listener and the tests check both.
* If the container has no IPv6 the private listener is skipped with a warning; the public
  listener and health check never depend on it.
* Not reachable from the internet: `[[services]]` maps only 8080 (a test fails if the private
  port ever appears there). Defence in depth in the app: the report route answers **404 on any
  other port** (the listener is identified by the accepting socket's port, not the Host header),
  and the private port answers **404 for every other path** (`install_private_listener_guard`),
  so it never exposes the rest of the app. Bearer auth and the Fly-edge-header refusal still apply.
* Tests: `test_web_listeners.py` runs real gunicorn with the real config (public IPv4 `/healthz`,
  private `[::1]` report 401/403/200, other paths 404, no-IPv6 container); the CI job
  `web-listener-image-smoke` repeats it in the **built image** (`scripts/smoke_web_listeners_image.py`).
  The smoke app (`scripts/listener_smoke_app.py`) uses a stub database because `main:app`
  needs a live MongoDB at import.

### Environment variables (APReferral web + scheduler)

| Variable | Required | Value |
|---|---|---|
| `AFFILIATE_V2_REPORT_ENABLED` | yes | `true` (default off: no job, no route, no private listener) |
| `AFFILIATE_V2_REPORT_PRIVATE_PORT` | yes, for the private listener | e.g. `8081` |
| `DATABOT_API_KEY` or `AFFILIATE_V2_REPORT_TOKEN` | yes | the bearer secret (must equal Databot's) |
| `AFFILIATE_V2_REPORT_REFRESH_SECONDS` | no | 600 |
| `AFFILIATE_V2_REPORT_STALE_AFTER_SECONDS` | no | 1800 |
| `AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING` | no | 250000 |
| `AFFILIATE_V2_REPORT_ALLOW_PUBLIC` | no | leave unset |

The web machines and the scheduler/worker process both need the variables (the worker
builds the snapshot, the web process serves it). Databot's side is in its own doc;
its URL is `http://apreferralv1.internal:<AFFILIATE_V2_REPORT_PRIVATE_PORT>`.

## Tests

`test_affiliate_v2_report.py` and `test_affiliate_v2_preview_report.py` (both in the PR gate; the second adds preview/live parity for 79.99, 80.00, new-player false, incomplete data, feed failure, duplicate Account ID and exact Decimal turnover, read-only proof, activation switch and audit) and `test_web_listeners.py`: real pipeline (Databot-shaped rows -> sync -> live validator -> snapshot) for qualified / legacy exclusion / 79.99, 80, 80.00 / incomplete seven-day data / feed failure / `coupon_new_player = 0` / duplicate account / exact Decimal sums / repeated rows and batches / GMT+8 month boundaries / inactive, scheduled and paused / replace-not-accumulate / failed-refresh keeps last good / stale and partial coverage / generation-swap race / no raw code or Account ID or token anywhere / writes confined / read path never runs the preview / route auth, edge refusal and bad input.
