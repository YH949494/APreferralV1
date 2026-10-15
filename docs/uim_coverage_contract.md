# UIM coverage contract (APReferral side)

Companion (Databot side): `docs/uim_coverage_contract.md` in
[YH949494/Databot](https://github.com/YH949494/Databot) — PR [#482](https://github.com/YH949494/Databot/pull/482). This PR: [#541](https://github.com/YH949494/APreferralV1/pull/541).

Status: **implemented; nothing deployed, activated or written to production.** `welcome_redemption_v2` stays
disabled; the reporting snapshot stays `report_mode=preview`, `rule_active=false`, `effects_written=false`.

## 1. Root cause

1. Databot recorded no coverage; APReferral invented one: `uim_redemption_sync.sync_health` set
   `coverage_end = max(coverage_max_redeemed_at)` — the **latest row**, not what the report covered — and the V2
   snapshot published it as `uim_coverage_end`, which Databot printed as "UIM coverage to …". A final report for
   28 Sep–4 Oct whose last redemption was 2 Oct read as ending 2 Oct.
2. That value was also injected as `source_coverage_end_utc` into `assess_evidence`, whose `outside_source_coverage`
   hold compared a row with the maximum of its own batches (vacuous, and fragile at sub-second precision for
   `source_offset` rows because the maximum was truncated to whole seconds).
3. Nothing could conclude anything from a **missing** row. An invitee who never redeemed stayed pending forever,
   and without declared coverage nothing *could* soundly reject one.

There was **no second seven-day timer** in the evaluator (`assess_evidence` has none; `joined_at + 7d` appeared only
in a diagnostic counter). This change keeps it that way and pins it with tests.

## 2. Contract

The seven-day measurement is performed **before** a UIM file is uploaded. A committed final batch is evaluated as
soon as it reaches APReferral — no `joined_at + 7 days`, no `upload_time + 7 days`, no join/upload age anywhere.
Databot additionally declares, per batch, which inclusive GMT+8 days it completely reports (see Databot doc §3);
a day inside a `complete` interval with no row is a confirmed zero-result day.

**Four timestamps, never merged** (names are identical in code, API and UI):

| Concept | Field |
|---|---|
| final UIM coverage | `uim_coverage.final_coverage_start/end`, `intervals`, `gaps` |
| latest observed redemption | `uim_coverage.latest_observed_redemption_at`; `sync_health.observed_start/end` |
| last successful sync | `source.last_sync_success_at` |
| snapshot generation | `snapshot.generated_at` |

`sync_health` no longer has `coverage_start/coverage_end`; the observed range is `observed_start/observed_end`, and
`final_coverage` is a separate object.

## 3. What is persisted (all idempotent, all APReferral-owned)

| Where | What |
|---|---|
| `uim_redemption_batches.coverage` | Databot's per-batch block, **whitelisted and validated** on every listing pass: status/source/basis enums, dates, and `start_utc`/`end_utc_exclusive` must equal the GMT+8 midnights of the dates (an off-by-one-day or timezone drift ⇒ the block is dropped, i.e. *no coverage*). Re-listing picks up an upstream correction without touching rows or cursors. |
| `uim_redemption_coverage` | The ledger from `GET /internal/uim-redemption-feed/v1/coverage`, validated the same way and stored with a fingerprint; `last-fetched` refreshes only `fetched_at` when unchanged. |
| `uim_redemption_coverage_history` | Append-only record of each *change* of the ledger (audit of what was authoritative when). |
| `uim_redemption_coverage` `_id=absence_pass` | Live pass bookkeeping (§5). |

A batch contributes a day **only if**: committed upstream and locally, not rolled back, **all its rows synced**
(`rows_complete`), and its block is `complete` + `operator_declared` with a recognised basis. `inferred_unverified`,
`partial`, `voided`, undeclared-legacy and malformed blocks contribute nothing. Overlapping and adjacent intervals
of the same basis merge; a day between two intervals is a gap and is never covered, and a later batch cannot hide it.
The local result is cross-checked against Databot's ledger on every complete batch; a missing, invalid or
**disagreeing** ledger suspends absence conclusions until the next sync reconciles them (an older Databot without
`/coverage` therefore degrades safely, rows still sync).

## 4. Qualification behaviour (preview and live are the same code)

A final committed row is evaluated immediately by the unchanged validator `assess_evidence`; coverage does not alter
any row verdict. The result matrix:

| Situation | Result |
|---|---|
| Correct own Welcome redemption, `coupon_new_player=1`, exact `Decimal(after_total_bet_amount) ≥ 80`, canonical Account ID unused | **qualify** |
| `coupon_new_player = 0` | confirmed **rejection** |
| `79.99` (any `< 80`, exact Decimal) | confirmed **rejection** |
| Canonical Account ID already credited | **duplicate** exclusion |
| No row, whole redemption window inside proven complete coverage, attested policy, healthy source (§5) | confirmed **rejection** (`no_welcome_redemption_in_complete_coverage`) |
| No row, window leaves coverage / touches a gap / coverage inferred-unverified / partial batch / no policy / basis unattested | **pending** |
| Temporary Databot/feed error, stale or partially-synced source, unsynced batch, truncated build | **pending** (everyone) |
| Source unavailable | **pending** (and turnover `unavailable`, never `0`) |

Never converted to zero or to a rejection: missing, partial or unavailable evidence. Preserved: Account ID case and
leading zeros, namespace rules, Decimal turnover, one decision per invitee, one lifetime owner per Account ID,
idempotent effects, all historical `qualified_events`, `referral_events`, rewards and XP (asserted in tests).
The `outside_source_coverage` hold was removed: a completed UIM row is never held because the latest observed
redemption is earlier than declared coverage.

## 5. Absence-based rejection (the only conclusion drawn from a missing row)

An invitee is rejected for a missing row **only if all** hold (`uim_coverage.decide_absences`):

1. **Policy attested** in the control document (`source_config.absence_rejection`, set only through
   `configure-coverage-policy`, dry-run by default, refused while the rule is active, history appended):
   `coverage_basis = voucher_redemption_date` (the only supported basis), `welcome_redemption_window_days`
   (how long a Welcome voucher stays redeemable after issuance, **from the data owner**) and an attestation
   (`by` + `reference`).
2. **Issuance proven**: one canonical Welcome code (`affiliate_ledger WELCOME:<uid>`), recipient decision `ok`
   for that invitee, supported `issued_at`.
3. **No evidence at all** for that code (any non-voided evidence — held, rejected, removed — counts as present).
4. **Window covered**: every GMT+8 day from the issuance day through `issuance day + window` lies inside **one**
   merged `complete` interval of the policy basis (the brief's example: coverage 28 Sep–4 Oct, latest row 2 Oct,
   window 3 days ⇒ an invitee issued 1 Oct concludes on 4 Oct; one issued 2 Oct stays pending).
5. **Source healthy** (no failed last attempt, not stale, no unsynced batch, no truncation) and the ledger valid and
   matching.

Live: `process_pending_evidence` runs the pass after the evidence loop, only if extraction of every committed batch
is complete. It writes one terminal evidence document per invitee (`source=databot_uim_absence`, unique
`(source, source_ref)`) carrying the window, basis, coverage fingerprint and attestation reference, and parks the
active referral as `rejected` (the same transition `_finalize` makes). No qualification, registry entry, XP,
reward or ledger is written. It is bounded (5,000 referrals per tick), re-examines a referral only when the
coverage/policy fingerprint changed (plus a 6-hour safety re-scan), and is idempotent (a retried pass re-applies the
parking). A correction that later *adds* a row for an already-rejected invitee goes to review
(`referral_not_active_at_rollout`); decided history is never reopened or paid automatically.

**Without an attested policy the system makes no absence-based decision at all**; positive rows are unaffected.
This is the activation blocker in §8: the code cannot know the Welcome voucher's redemption window or prove which
filter the export used (Databot doc §2).

## 6. Admin

```bash
# read-only; now also reports coverage (status, gaps, gaps affecting the launch cohort, absence policy, blockers)
fly ssh console -a apreferralv1 -C "python scripts/affiliate_qualification_admin.py preflight"
fly ssh console -a apreferralv1 -C "python scripts/affiliate_qualification_admin.py sync-status"

# attest the policy — DRY RUN (default); the figures come from the UIM data owner, not from this repo
fly ssh console -a apreferralv1 -C "python scripts/affiliate_qualification_admin.py configure-coverage-policy \
  --basis voucher_redemption_date --welcome-redemption-window-days <N> \
  --attested-by '<data owner>' --attestation-ref '<ticket>' --reason '<why>' --operator '<you>'"
# … add --commit to write; --disable switches it off (history kept). Refused while the rule is active.
```

`preflight` now fails (`ok=false`) on: no complete coverage / inferred-only / partial batches, a gap inside the launch
cohort, a missing or disagreeing ledger, or an unattested policy. `pending_maturity` reports the four facts separately
and keeps the join-age counters strictly diagnostic.

## 7. Monthly attribution of delayed uploads (audit result)

* A V2 `qualified_events.qualified_at` is the UIM **redemption time** (`redeemed_at`, read as GMT+8 wall clock for
  naive source times), clamped **up** to the launch cutoff for a pre-launch redemption
  (`attribution_basis=launch_cutoff_clamp`). Every consumer (monthly tiers, weekly tiers, leaderboards, dashboard)
  windows on that field in GMT+8 calendar months.
* It does **not** depend on upload, commit, sync or processing time. Tested: the same redemption processed on
  1 Oct, 12 Oct and 31 Oct yields the same `qualified_at`; `30 Sep 23:59:59` → `202609`, `1 Oct 00:00:00` → `202610`.
* A redemption for an already-closed month that arrives late is evaluated for **that** month with
  `closed_month_review=True`: a newly reached tier ledger is created `PENDING_REVIEW`
  (`late_redemption_closed_month`), never auto-issued, never moved into the processing month; existing ledgers are
  never re-issued.
* Rows without a trustworthy time (`date_only`, `ambiguous_day_month`, unknown legacy basis) go to review; nothing is
  attributed to a guessed month.
* The V2 reporting **cohort** (referral `joined_at`) is a different dimension, used for the dashboard filter only. It
  is **not** compatible with reward policy (rewards window on `qualified_at`) and is deliberately not used for
  attribution. Absence rejections create no qualification and therefore no month.

Decision: one deterministic attribution timestamp (`redeemed_at`, GMT+8). No entitlement rule changed. Gate: **PASS**.

## 8. Activation gates — current state (V2 is NOT activated)

| Gate | State |
|---|---|
| Coverage basis proven and documented | **OPEN** — the repository proves rows are dated by redemption time but not the export filter; the data owner's attestation (basis + Welcome redemption window) must be recorded with `configure-coverage-policy`. Positive rows are unaffected meanwhile. |
| Declared complete intervals persisted | Code **ready**; production has none until the existing batch is attested (Databot doc §8) |
| Zero-row dates handled correctly | **PASS** in code/tests; effective only with the attested policy |
| No coverage gap affects the launch cohort | **UNKNOWN until production** — `preflight.coverage.gaps_affecting_cohort` |
| No second seven-day wait | **PASS** (removed hold; tests) |
| Delayed-upload attribution deterministic | **PASS** (§7) |
| Required indexes exist | Databot: verify `uim_coverage_revisions` unique index (Databot doc §10). APReferral: no new index on an existing collection; new queries use existing indexes (`evidence_code_hash`, `dedup_key`, `aq_pending_by_invitee`, status index) |
| Historical migration proof valid | unchanged (`observed_end` kept as the stricter bound; not relaxed) |
| Consistency checks, preview credibility, history unchanged, idempotency | verified in tests; **production preview comparison pending** (needs deploy + attestation) |
| Explicit activation cutoff reviewed and authorised | **not requested** |

## 9. Rollout, rollback, verification

Order (each step separately authorised): Databot deployed → APReferral deployed → verify sync + reporting →
attest the existing batch on Databot (Databot doc §8) → `sync-evidence` → regenerate the preview → compare →
`configure-coverage-policy` (only with the owner's figures) → regenerate → `preflight`.

```bash
# after deploy (APReferral keeps working against an older Databot; absence just stays off)
fly status -a apreferralv1
fly ssh console -a apreferralv1 -C "python scripts/affiliate_qualification_admin.py sync-evidence --commit"   # one bounded, leased pass
fly ssh console -a apreferralv1 -C "python scripts/affiliate_qualification_admin.py sync-status"
#   expect observed_end (latest row), final_coverage.status/final_coverage_end (declared), problems: []

# private reporting (Databot's token; from a Fly machine on 6PN). Expect uim_coverage.*, report_mode=preview,
# rule_active=false, effects_written=false, and no voucher code / Account ID anywhere:
curl -s -H "Authorization: Bearer $DATABOT_API_KEY" \
  "http://apreferralv1.internal:8081/internal/affiliate-v2-report/v1/summary?start_date=2026-09-01&end_date=2026-10-31" | head -c 1500
```

Mature-cohort comparison after attestation + `sync-evidence` + next snapshot (≤ 10 min): pending → would qualify /
would reject / would be duplicate for invitees whose window is inside 28 Sep–4 Oct; anything uncovered, in a gap, in a
partial/inferred batch stays pending. Compare `uim_coverage.absence_rejection.blockers` (must be empty), the
`build.absence_rejected` / `build.absence_pending_<reason>` counters in `affiliate_v2_report_state`, and masked samples
from `activation-preview`.

Rollback (APReferral): `fly releases -a apreferralv1` → `fly deploy -a apreferralv1 --image <previous>`. The added
fields/collections are inert for the previous code. `configure-coverage-policy --disable --commit` switches absence
rejection off without a deploy. **Never** reset cursors, replay, or delete `qualified_events`, `referral_events`,
`affiliate_ledger`, XP, registry or evidence; if absence decisions were already written live, `pause` (the existing
command) stops new ones and decided history stays.

## 10. Tests

`test_uim_coverage_contract.py` (67, in the PR gate): block validation (UTC bounds vs GMT+8 dates), declared coverage
beyond the latest row, trailing/zero-row days, inferred/partial/no-policy/unattested cases, gaps and merging,
overlap, bases never merging, voided/corrected batches, idempotent sync, ledger missing/mismatch/malformed, older
Databot, stale/failed feed, GMT+8 issuance boundaries, preview == live (incl. effects confined to the rejection
documents), bounded live pass, immediate evaluation without any seven-day wait, coverage never changing a row verdict
(79.99 / 80.00 / `coupon_new_player=0` / long Decimal / duplicate Account ID), delayed-upload month attribution,
privacy of the reporting payload and snapshot state, read path never computing, policy command, preflight gates.
Existing suites updated only where they encoded the old "coverage = latest row" meaning.
