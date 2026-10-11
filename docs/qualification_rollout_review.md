# Welcome qualification rollout review

## Problem and resulting behavior

Welcome qualification and historical account seeding could accept redemption evidence without proving that the invitee held their own Welcome voucher at redemption time. Seeding also lacked the live future-time check. The legacy referral XP backfill had a broken import and no launch-cutoff guard.

This PR makes live qualification, preview and seeding check issuance chronology; rejects implausibly future-dated evidence from historical seeding; and prevents committed legacy XP backfills after launch, including while qualification is paused.

**Draft for review. Merging main triggers deployment. Do not merge, deploy, migrate or activate until the production evidence and rollout package are reviewed.**

## Root causes and solutions

### Recipient ownership did not prove issuance chronology

`decide_recipient` resolved the original recipient from the Welcome pool/ledger or legacy claim but omitted issuance time. `assess_evidence` could therefore accept an otherwise valid redemption before that recipient received the code. Preview shared the gap, and migration could seed a lifetime account from the same unsupported evidence.

For example, an otherwise valid redemption at 11:00 for a code issued at 12:00 could proceed. It now requires review, without creating a qualification or consuming an account reservation.

- Resolve issuance from `voucher_pools.issued_at`, matching `affiliate_ledger.issued_at`, or legacy `new_joiner_claims.claimed_at`.
- Preserve recipient ambiguity, ledger-code and cross-pool checks. Record the earliest supported issuance and its timestamp sources; generic created/updated timestamps are not substitutes.
- Add shared `welcome_issuance_problem` validation. Missing issuance becomes `missing_welcome_issued_at`; reversed chronology becomes `redemption_before_welcome_issuance`.
- Use that decision in live qualification and migration. Update the bulk preview resolver to supply the same timestamps to the shared validator.

### Historical seeding lacked live's future-time check

Live already sends redemptions more than five minutes ahead of processing time to review. Migration parsed successful committed observations without that constraint, allowing future-dated evidence to consume a historical lifetime account.

Pass one consistent `now_utc` from migration execution through plan construction/source evaluation and apply the shared `FUTURE_REDEMPTION_TOLERANCE`. Tests cover the five-minute boundary and exclusion beyond it. Existing removal/correction handling and account normalization remain intact.

### Repairing the backfill import would otherwise reopen an award bypass

`backfill_referrals.py` imported `current_bonus_count`, which no longer existed in `referral_rules`, so its entrypoint failed to import. It also lacked a persisted-cutoff check: fixing only the import would allow legacy qualification-independent XP after launch.

- Restore bonus counting using the existing XP event type and preserve existing reward/idempotency keys.
- Call `legacy_award_allowed(get_control(db), now)` before committed work and immediately before every base/bonus award.
- Refuse awards once the persisted cutoff arrives, regardless of active, paused or disabled mode. Control-read errors propagate before awarding.
- Keep dry runs read-only after launch and preserve prelaunch behavior. A regression test activates the cutoff during a pass and verifies that the following award stops.

## Changed files

| File | Change |
|---|---|
| `affiliate_qualification.py` | Issuance snapshots and shared chronology validation. |
| `affiliate_qualification_preview.py` | Bulk ledger/claim timestamp reads matching live resolution. |
| `scripts/migrate_affiliate_account_dedupe.py` | Issuance checks, consistent run time and future-time tolerance. |
| `backfill_referrals.py` | Import repair, bonus counting and launch-cutoff guards. |
| `test_affiliate_qualification_rollout_guards.py` | Chronology, seed-time boundaries, cutoff modes, mid-pass launch, dry-run and prelaunch idempotency regressions. |
| `test_affiliate_qualification_rollout_real_mongo.py` | Real MongoDB uniqueness, concurrency, recovery, retry and no-write checks. |
| `test_affiliate_welcome_redemption_qualification.py` | Real-database-capable fixture and explicit issuance timestamps. |
| `test_uim_redemption_sync.py` | Explicit fixture clock for migration source evaluation. |
| `.github/workflows/affiliate-migration-ci.yml` | Include the new guard suite in the required gate. |
| `docs/affiliate_welcome_redemption_qualification.md` | Document chronology, legacy backfill refusal and older evidence handling. |
| `docs/qualification_rollout_review.md` | Preserve this complete root-cause, solution, validation and rollout review on the branch. |

## Validation

Recorded on 9 October 2026 against commit `eaae915023caa595969496a3156fc539f8b02ed9`:

| Check | Result |
|---|---|
| Required AP affiliate CI gate | **911 passed**. |
| Full AP baseline comparator | **4,090 passed, seven skipped**; existing **85 failures and one error** exactly match the baseline. **Zero new failures; the full suite is not wholly green.** |
| AP real MongoDB qualification/sync | **Seven passed**: six new qualification tests plus the existing real sync test. |
| Databot feed suite with real MongoDB configured | **21 passed**; no Databot source changes in this PR. |
| Diff whitespace check | Passed. |

Real MongoDB tests verify partial unique indexes over unchanged legacy rows; two invitees competing for one account; one invitee competing on two accounts with losing-reservation cleanup; recovery after account reservation; read-only preview/migration; and retry after XP/event writes without duplicate payouts or settlement events.

Tests use disposable local databases and synthetic examples, never production credentials. Temporary databases are removed afterward. Real tests skip when their URI is absent and fail if a configured server is unreachable. The seven full-suite skips are separate from the successful explicit real-Mongo run.

Reproduce from APReferral with its dependencies installed:

```bash
python -m pytest -q test_affiliate_qualification_rollout_guards.py \
  test_affiliate_welcome_redemption_qualification.py test_uim_redemption_sync.py
python ci/full_suite_baseline.py

# Isolated localhost MongoDB only; adjust the local port if needed.
QUALIFICATION_TEST_MONGO_URI='mongodb://127.0.0.1:27018/?directConnection=true' \
UIM_SYNC_TEST_MONGO_URI='mongodb://127.0.0.1:27018/?directConnection=true' \
python -m pytest -q test_affiliate_qualification_rollout_real_mongo.py \
  test_uim_redemption_sync_real_mongo.py
```

## Compatibility and fixed scope

Previously extracted evidence lacks the new issuance snapshot and will require review. For a specifically reviewed row, existing `requeue` refreshes recipient/source evidence before full validation. This PR does not automatically replay evidence, reset cursors or requeue production rows.

Historical qualifications and earned XP/rewards are preserved. Welcome issuance eligibility, platform redemption, referral join tracking, segmentation, tier thresholds, invite caps, deposit/turnover requirements and voucher-hunter gates are unchanged. Account case/leading zeros and numeric-cell provenance restrictions remain intact.

No production configuration, evidence, registry, qualification, index or cursor writes were performed during the audit. No deployment, migration apply or activation occurred. Current production mode/cutoff has not been freshly verified; the handoff's disabled state is historical reporting.

## Production findings and remaining blockers

Fresh Git checks on 11 October confirm AP main `98e421b25057b567d8270ae5c73babcfd016d09e` (#525) and Databot main `41a27fa3e7e57a0da55e14070e7a41117f1bfdd9` (#459). Git history does not establish deployed versions. AP #525 already contains token-guarded scheduler lease release; this PR does not reimplement it. Actual deployment, run durations and cadence remain unverified.

Atlas document tools and production Mongo/Fly access were unavailable in the audit session. No fresh production document reads, real Welcome examples, production preview or migration dry-run report were obtained. Local validation does not establish production readiness.

Before launch:

1. Verify deployed hashes, mode/cutoff, document reads in both databases, feed authentication, sync progress, batch completeness and scheduler health.
2. Obtain truthful owner attestation that the export lists successful redemptions only, with author/reference, stable account namespace and verified source/legacy timezone basis. Row presence, deposit, turnover and new-player flags do not establish success.
3. Trace masked real own-Welcome redemption, scoped absence, and distinct invitees sharing an exact account if such an example exists. Check issuance/referral chronology, corrections, account provenance and prior credit through canonical validation and the full preview.
4. Reconcile historical lifetime coverage. The handoff reported **21,348 and 22,647 code-less observations** in two June batches. Recovery through later cumulative imports remains unproven. Scanned flags, upload ranges, a seed marker or preflight exit zero do not establish complete account coverage. Surface unresolved/conflicting identities without inventing mappings or rewriting earned history.
5. Review the concrete migration dry-run and history-preservation evidence before separately authorizing apply and an explicit GMT+8 cutoff.

Complete standalone read-only PowerShell diagnostics were prepared separately in the audit handoff for state/progress, example tracing and bounded historical reconciliation. Their Python payloads passed local no-write checks; Windows/Fly transport was not exercised. These follow-up artifacts are not production results or additional service changes in this PR.

## Rollout and pause

Keep qualification disabled pending production evidence and review. After separate approval: deploy reviewed code → record valid source configuration/attestation → review migration dry-run → apply with history unchanged → require preflight exit zero **and** resolved coverage/example blockers → activate at the approved cutoff → verify legacy awards stay off and new awards are exact-once.

After launch, rollback means **pause new qualifications**. Never restore legacy awarding, release credited accounts, delete earned events or reset all cursors. Evidence sync is independent; already-qualified effects may finish while paused. Resume only after the specific issue and preflight have been reviewed.
