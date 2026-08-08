# Databot Segmentation Migration — Design (not implemented)

Status: **design only**. No classifier rule, priority order, or threshold in
this repository has been modified to produce this document. This builds on
the two prior audit passes (production-vs-shadow system trace, and the
Player Account Details screen trace) and the confirmed architectural
decision that Databot's Backend Segment Engine becomes the canonical source
of behavioural segmentation, with `users.for_bot_segment` demoted to a
legacy comparison field during migration.

## 0. What exists today (baseline, already proven)

- **System A (current production)**: `users.for_bot_segment` /
  `users.bot_segment`, written only by `bot_segment_sync.py` from an
  external Google Sheet, weekly (`main.py:8804-8815`, Wed 09:30
  Asia/Kuala_Lumpur). Read by `vouchers.py:_load_user_bot_segment` /
  `assign_public_pool_access_once` for live voucher probability.
- **System B (shadow, in-repo classifier)**: `backend_segment_engine.py`,
  writes only to `backend_segment_snapshots`, triggered only by an admin
  clicking "Run" (`POST /api/admin/dashboard/backend-segment-engine/run`).
  Read by `campaign_engine.py` / `campaign_builder.py` for Campaign Centre
  targeting, and now (P0 patch) by the drilldown's `segment_observability`
  block for display only.
- **Databot**: an external HTTP service (`databot_client.py`,
  `config.DATABOT_BASE_URL`/`DATABOT_API_KEY`/`DATABOT_ENABLED`), currently
  disabled by default and called from nowhere in production code
  (`databot_service.py:9-11`). Its repository is not in this session's
  scope — none of its internal classifier logic can be inspected from here.
- Confirmed today: no rolling 7-day/30-day metric exists anywhere in this
  repo; `claim_count` is lifetime; `referral_count`/`checkin_count` are
  live point-in-time reads at classification time; `after_total_bet_amount`
  /`withdraw_amount` are whatever period tag the last marketing upload row
  carried.

## 1. Target architecture (per the confirmed decision)

```
Databot (external, canonical)
  - owns classifier + rule version
  - owns rolling/period metric construction (7D/30D/lifetime, explicitly labeled)
  - owns Telegram <-> gaming-account identity reconciliation
  - writes: canonical segment, reason, data_status, calculation window, rule_version
        |
        | (contract below)
        v
APReferral (consumer + operational owner)
  - owns: voucher claims, eligibility, inventory, referrals, runtime controls
  - consumes latest valid canonical Databot segment for probability/targeting
  - records canonical segment + rule_version at claim time (audit trail)
  - never independently classifies users going forward
```

## 2. Canonical collection / schema

New collection: **`databot_segment_snapshots`** (deliberately not reusing
`backend_segment_snapshots` — that collection stays as the in-repo shadow
engine's own audit trail per Q9's rollback requirement; conflating the two
would make rollback impossible to reason about).

```jsonc
{
  "_id": ObjectId,
  "telegram_user_id": 7981036271,         // primary consumer key — the ONLY key vouchers.py/campaign_engine.py should query on
  "gaming_account": "7981036271",         // as reconciled by Databot; may differ from telegram_user_id string form
  "segment": "ghost",                     // canonical value — one of a versioned enum Databot publishes
  "segment_reason": "after_bet=0, withdraw=0, referral_count=0, checkin_count=0 over last 30d window",
  "data_status": "classified" | "classified_unclassified" | "insufficient_data" | "identity_unresolved",
  "rule_version": "databot-v3.2.1",       // REQUIRED — every doc must carry the classifier version that produced it
  "calculation_window": {
    "kind": "rolling_30d" | "rolling_7d" | "lifetime" | "calendar_month" | "snapshot_period",
    "start": ISODate, "end": ISODate,
    "label": "2026-06-24..2026-07-24"     // human-readable, must be accurate — never "(7D)" unless kind == rolling_7d
  },
  "calculated_at": ISODate,               // when Databot computed this
  "synced_at": ISODate,                   // when APReferral ingested it
  "source": "databot_api" | "databot_api_fallback_cache",
  "identity_match_confidence": "confirmed" | "probable" | "unresolved",
  "metrics": { /* Databot's own inputs, opaque to APReferral, stored verbatim for audit */ }
}
```

Indexes: unique on `telegram_user_id` (latest-wins) **plus** an append-only
history collection `databot_segment_snapshot_history` (same shape, insert-
only, indexed on `(telegram_user_id, calculated_at)`) so "segment at claim
time" and rollback/reconciliation both have real history to query, matching
the pattern already used for `segment_snapshots`/`user_claim_risk_history`.

## 3. Freshness rules

- A snapshot is **valid** for consumption only if `now - calculated_at <=
  MAX_SEGMENT_STALENESS` (proposed default: 26 hours, i.e. tolerate one
  missed daily sync before treating data as stale — configurable, not
  hardcoded, mirroring `BOT_SEGMENT_SYNC_*` env var pattern in
  `main.py:8807-8810`).
- Staleness must be a **read-time check**, not a write-time assumption — a
  consumer computes `is_fresh = (now - calculated_at) <= max_staleness` on
  every read, never trusts a boolean stored at write time (which could
  itself go stale).
- `data_status` must always be read alongside `segment` — a consumer must
  never treat `segment` as meaningful without checking `data_status !=
  "insufficient_data"` and `data_status != "identity_unresolved"` first.

## 4. Missing-data behaviour (this is the exact bug class from Q6/Q7 of prior audit passes)

| Condition | Required behaviour | Explicitly forbidden |
|---|---|---|
| No `databot_segment_snapshots` doc exists at all for this `telegram_user_id` | Consumer must report `"no_segment_available"` / equivalent to `data_status="insufficient_data"` — **never silently substitute `"unclassified"`** | `doc.get("segment") or "unclassified"` pattern (the exact pattern found at ~10 call sites in `dashboard_panels.py` today) |
| Doc exists but is stale (`calculated_at` beyond `MAX_SEGMENT_STALENESS`) | Use it, but mark `stale=true` in every consumer response/log; trigger an alert if staleness exceeds 2x the threshold | Silently treating a stale segment as current, and never re-attempting sync |
| Databot API/sync call fails | Keep the last valid cached snapshot (with `stale` flag), fall back to `unclassified`-equivalent behaviour **only** in probability/targeting logic (never in display), log `fallback_used` (mirrors `databot_service._fallback`, `databot_service.py:25-27`, which already exists and should be reused) | Silently retrying forever without surfacing a health signal; blocking voucher issuance on Databot being reachable |
| Identity reconciliation fails (Databot can't map telegram_user_id <-> account) | `data_status="identity_unresolved"`, `segment=null` — consumer treats this identically to "no segment available" for probability purposes, but the two are logged distinctly for ops | Guessing an account by numeric string coincidence (the exact risk flagged in the identity-mapping trace for user 7981036271) |

## 5. Sync / API contract

Prefer **push** (Databot calls APReferral) over the current **pull-on-cron**
(`bot_segment_sync.py`'s Wednesday sheet fetch) because weekly polling is
precisely the staleness class causing your reported bug. Two acceptable
shapes, either is fine as long as freshness rules (§3) hold:

- **Pull, more frequent**: APReferral polls `GET
  /api/v1/segments/user/{telegram_user_id}` (this endpoint already exists
  in `databot_service.get_user_segment`, `databot_service.py:34-53` — it's
  wired but unused) on a tighter schedule (hourly, not weekly), writing into
  `databot_segment_snapshots`. Minimal new code: mostly re-enabling and
  scheduling what's already stubbed.
- **Push, event-driven**: Databot POSTs to a new `POST
  /api/internal/databot/segment-update` webhook whenever a user's segment
  changes, authenticated the same way `DATABOT_API_KEY` auth is used
  outbound today (reverse it inbound). Lower latency, but requires Databot
  to initiate outbound calls into APReferral's network — an infra decision
  outside this repo's control.

Either way, response contract must include at minimum: `telegram_user_id`,
`segment`, `segment_reason`, `data_status`, `rule_version`,
`calculation_window`, `calculated_at`. APReferral must reject/quarantine
(log + skip, not crash) any payload missing `rule_version` or
`calculation_window` — those two fields are exactly what's missing from
today's `backend_segment_snapshots` schema and were unprovable in the
diagnostic script above.

## 6. Migration-compatibility fields

During migration, keep on `users`:
- `for_bot_segment` / `bot_segment` — **frozen as legacy comparison only**,
  no longer read by `vouchers.py` probability logic once Phase 2 (below)
  ships. Keep the weekly sync running read-only for comparison until
  Phase 4.
- Add `legacy_segment_at_migration_cutover: <value>` (one-time, written
  once) so reconciliation reports (§8) always have a fixed legacy baseline
  to diff against, independent of the legacy sync continuing to drift.
- Add `databot_segment_synced_at` / `databot_segment_value` denormalized
  onto `users` (mirroring the existing `for_bot_segment`/`bot_segment_*`
  pattern) purely as a **read-optimization cache** of the latest
  `databot_segment_snapshots` doc — never the source of truth, always
  reconstructable from the snapshot collection.

## 7. Rollback plan

- Every consumer change (vouchers.py, campaign_engine.py, dashboard) must
  be gated by a single settings flag, e.g. `SEGMENT_SOURCE = "legacy" |
  "databot"`, read via the existing `settings_service.get_setting()`
  pattern already used for `pool_probabilities` (`config.py:151-152`) — not
  an env var requiring redeploy.
- Rollback = flip `SEGMENT_SOURCE` back to `"legacy"`. Because
  `for_bot_segment`/`bot_segment` and their weekly sync keep running
  untouched through Phase 4 (§6), rollback requires **zero data
  backfill** — the legacy path never stopped being populated.
- `databot_segment_snapshots` and its history collection are additive only
  — deleting them (if the migration is fully abandoned) cannot corrupt
  `users` or any operational collection, since nothing legacy reads them.
- Keep the existing shadow engine (`backend_segment_engine.py`,
  `backend_segment_snapshots`) running unmodified throughout — it remains
  useful as a **third, independent cross-check** during migration
  (legacy vs. Databot vs. in-repo shadow), and its removal is out of scope
  for this migration.

## 8. Reconciliation report (legacy UIM vs. Databot)

New read-only report, structurally identical to the existing
`compare_with_uim()` pattern (`backend_segment_engine.py:378-392`) which
already proves this kind of comparison is cheap to build:

```python
def compare_legacy_vs_databot(*, users_col, databot_snapshots_col, now) -> dict:
    # For every user with both a legacy for_bot_segment/bot_segment value
    # AND a fresh databot_segment_snapshots doc:
    #   - normalize both through the same alias table
    #   - report match/mismatch counts per (legacy_segment, databot_segment) pair
    #   - report coverage: % of users with a legacy value but no Databot doc,
    #     and % with a Databot doc but no legacy value
    #   - flag users where the mismatch would change voucher probability by
    #     >= a configurable delta (this is the number that actually matters
    #     for a go/no-go decision, not raw mismatch %)
```

Run this report continuously from the start of migration (Phase 1) through
Phase 4, surfaced on the existing UIM-comparison dashboard panel family
(`dashboard_panels.py` ~1600-1780) rather than a new one, so operators have
one place to watch mismatch trend over time before cutover.

## 9. Consumer-by-consumer migration order

Ordered by blast radius, smallest/most-reversible first — matches the
existing internal "Phase 7B/7C" staging language already present in
`dashboard_panels.py:1868-1869`, which this design supersedes with a
concrete plan:

1. **Dashboard/observability only** (this session's P0 patch is the first
   step of this phase) — `segment_observability` block, reconciliation
   report. Zero behavioural risk; ships first, stays on indefinitely as a
   monitoring surface.
2. **Campaign Centre targeting** (`campaign_engine.py:84-92`,
   `campaign_builder.py:251`) — already reads a backend-computed segment
   (System B) today, so switching its source collection to
   `databot_segment_snapshots` is a smaller behavioural change than
   touching live voucher probability, and campaign audiences are
   reviewed by an operator before sending — a built-in human check.
3. **Claim-risk / abuse review tooling** — read-only admin surfaces
   (`claim_risk_sync.py` consumers), no direct player-facing effect if wrong.
4. **Voucher probability / public-pool access**
   (`vouchers.py:_load_user_bot_segment`,
   `assign_public_pool_access_once`) — highest blast radius, moved last,
   only after §8's reconciliation report shows sustained low mismatch and
   the `SEGMENT_SOURCE` flag has been battle-tested via phases 1-3.
   New-player SVD boost logic (`vouchers.py:3474-3496`) is re-validated
   separately here since it currently keys off `is_new_user_segment()`
   against the legacy alias table, which must be confirmed to have a
   Databot-side equivalent before cutover.
5. **Retire the weekly Google Sheet sync** (`bot_segment_sync.py`,
   `claim_risk_sync.py`) and demote `for_bot_segment`/`bot_segment` to
   read-only legacy fields — only after phase 4 has been stable for an
   agreed soak period and the rollback flag has not been used.

No phase here modifies `backend_segment_engine.py`'s rule chain, priority
order, or thresholds — this plan is purely about *which system's output* is
consumed where, not about changing what any classifier computes.
