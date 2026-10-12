# Historical affiliate Account ID migration review

Review only. Do not merge/deploy, run production writes, activate qualification,
reset cursors, replay data, apply migrations or alter earned rewards without
separate authorization. Default CLI behavior remains read-only dry-run.

## Audited sources and root causes

Audited APReferral main `b618728` and the current migration/source parsers.
No production connection was used. The supplied dry-run numbers and feed
coverage date are observations supplied by the operator, not refreshed here.

Historical `affiliate_rewards.mark_invitee_qualified` writes invitee/referrer,
qualification time and IP/subnet/session; it does not snapshot a gaming Account
ID or Welcome code. `scheduler.settle_pending_referrals` calls it after legacy
channel retention/community check-in and award recovery. Welcome redemption
was not a requirement of those historical awards. Consequently, a historical
qualification can legitimately have no Welcome issuance or redemption row.
The missing-code count does not by itself establish lost data or an account.

| Source inspected | Supported identity evidence | Treatment |
| --- | --- | --- |
| `users.linked_gaming_accounts` | Materialized UIM linkage, written by `multi_account_risk_sync` from the `user_profile_summary` source | Existing opt-in `--seed-from-linkage`; exactly one canonical text account, no malformed array/cell or normalization loss. Multiple accounts conflict even when a redemption matches one of them. |
| Historical linkage/audit | The audited multi-account sync overwrites current linkage, with no retained account-change audit. `user_claim_risk_history` records risk changes, not account history. | No invented history collection or account join. Previously pruned linkage requires separately supplied, attested evidence. |
| `new_joiner_claims` | Telegram recipient + voucher code + claim/issuance time, not a gaming account | Join to committed successful redemption; preserve own-code and chronology checks. A Telegram claim alone never maps a gaming account. |
| `voucher_pools` | Issued Welcome recipient/code/time | Same successful committed-redemption join, with ambiguity/cross-pool checks. |
| `affiliate_ledger` `WELCOME:<uid>` | Issued Welcome recipient/code/time | Now discover this supported issuance store as well; a matching issued ledger can outlive the pool row. Code owners, ledger key/user ID and cross-pool stores must agree. Missing issuance time is not fabricated. |
| Committed Databot UIM observations | Exact gaming account and redemption provenance, via the private feed and APReferral sync | Existing canonical parser/namespace. Do not join an exported gaming identity to a Telegram ID. Removed/rolled-back or unusable account evidence is not trusted. Historical seeding ignores the new coupon/turnover gates. |
| `qualified_events`, referral/award/flow snapshots | Legacy snapshots contain Telegram attribution/time and risk/session metadata; no audited legacy account snapshot writer | Do not accept an arbitrary `account_id` field, IP, session, username or risk flag as identity evidence. |
| `affiliate_account_registry` | Existing seeded/legacy-credit account with matching invitee and an actual historical `qualified_event_id` | Reuse only canonical, same-namespace, final earned records with an exact owner/event pointer. Never use reservations, foreign pointers or arbitrary account keys. |
| Databot canonical/derived identity and linkage diagnostics | Databot resolves direct claim/code identities for analytics; some paths normalize account case and treat numeric aliases differently. The reporting bridge/APReferral sync does not transport a historical account mapping into this migration. | Do not import derived analytics or reimplement its joins as an exact-text ownership proof. No Databot or segmentation change in this patch. |

The prior migration only discovered Welcome pool/legacy claim codes and would
stop on unusable redemption evidence before considering single-account
linkage. It also accepted a redemption account merely contained in a larger
linked-account list. Those behaviors under-resolve independently verified
single linkage in time-only holds, while potentially overlooking contradictions
in multi-account linkage. The old apply path could write a completion marker
with unresolved historical invitees and no source-coverage proof.

**Why 31,365 lack Welcome codes and only 269 resolve:** the first count means
no code in the stores inspected by the old planner; legacy qualification did
not require one. The second means only 269 historical events survived that
planner's evidence/linkage checks, not that every other user has no gaming
account. Exact production contributions from absent user docs, missing linkage,
multiple accounts, ledger-only issuance and unusable timestamps cannot be
established from the supplied summary. New diagnostics separate those causes
without fabricating recoverable identity data.

## Planner behavior and output

Combine all available reliable identity candidates before resolving. Exactly
one canonical Account ID must remain; any disagreement stays unresolved.
Leading zeros and case survive. Numeric/coerced cells, malformed linkage and
account text altered by normalization are held rather than reconstructed.
Existing namespace rules remain: Databot uses `uim_global`; platform code is
not part of identity. A seeded namespace mismatch refuses apply.

With the existing linkage opt-in, a verified single account can reserve earned
history when redemption evidence is absent or held only for unverified/missing
time. Trustworthy source account text still participates in contradiction
checks. This **does not** validate that redemption time, change either review
policy, or make the row qualify under the new rule. Account-cell problems,
source removals, reversed chronology and other unreliable redemption evidence
are not bypassed with a guessed identity.

Reports contain:

- `historical_events` and `historical_invitees` (separate event/user grains),
  `resolved_invitees`, `unresolved_events`, original reason counters;
- `resolution_sources` for primary mapping basis and overlapping
  `resolution_evidence_sources` for supported evidence legs;
- `source_diagnostics`: missing user document/link field, zero/single/multiple
  linked accounts and presence/absence of Welcome codes;
- `source_coverage`: UTC coverage end, latest historical qualification, exact
  gap start/end/seconds, freshness/partial/unknown-coverage problems;
- `apply_safe`, `apply_blockers`, bounded masked samples and duplicate counts.

Output excludes internal seed rows, raw account IDs/codes and full Telegram
IDs. Duplicate samples cap both account samples and invitee samples. CLI
failures report only exception type, because raw Mongo errors can expose keys.

The coverage end is derived from complete committed-source metadata in its
explicit GMT+8 wall-clock timezone, or valid committed marketing timestamps.
This is an extent check, not a proof that every earlier file was exhaustive.
All historical events still need deterministic mapping: a recent maximum
source timestamp cannot erase an unresolved user. Unknown/malformed/future
coverage, stale or incomplete sync, and coverage ending before the latest
historical qualification block apply. No legacy-time policy changes.

## Duplicate and apply protocol

Group by canonical Account ID; sort by qualification UTC time, then event ID
for ties. The earliest event reserves it once; later historical qualifications
are reported, never modified or revoked. If a registry entry is already held
by a different/later event, report `held_by_other` and withhold completion;
never reassign earned registry ownership automatically.

`--apply` remains explicit. It refuses unsafe plans **before any index or
registry write** and requires a disabled, unlaunched rule and matching frozen
namespace. A control-document lease serializes apply. Completion is removed
while an authorized apply is in progress; each registry insert is protected by
the unique account index. Partial inserts survive a crash and are recognized
on retry. A token-scoped cleanup cannot remove another worker's lease.

Before writing completion, re-plan identity ownership, verify unchanged
historical counts/XP/rewards, check indexes/consistency, and compare-and-set the
unchanged control/config and live lease. No completion marker is written for
partial/changed/held ownership. This durable insert-and-completion protocol
works with standalone MongoDB and does not require multi-document transactions.
It never calls qualification/XP/reward effects or mutates historical records.

Completion summaries now use `historical_account_coverage_v2`. Preflight
requires this version, verified coverage, zero unresolved events, and matching
historical event count/latest qualification. Old partial markers are not
silently grandfathered. A new historical qualification after seeding makes
preflight fail, requiring a fresh reviewed migration. Existing index,
integration, identity-freeze and consistency checks remain in force.

## Before/after dry-run expectations

Supplied baseline: 33,494 qualifications, 269 resolved, 127 candidate accounts;
31,365 no Welcome code, 823 no redemption evidence, 1,037 unusable account;
69 duplicate accounts and 142 extra qualifications; history unchanged;
no seed marker; all 14 rollout indexes missing; rule disabled; healthy feed
ending 2026-09-27.

After this patch, ledger-only issuance and independently verified single
linkage in time-only holds may resolve more accounts. Strict cross-source
conflict checks can also reduce the resolved set. **No new production count
is promised without another dry-run.** Those with no trustworthy identity stay
unresolved. Apply remains blocked while any such users remain, and reports
any gap from the supplied coverage end to the actual latest qualification.
The latter timestamp was not supplied, so its exact gap cannot be computed
honestly here. Default dry-run leaves all 14 missing indexes and every earned
history/reward record unchanged. It does not turn on qualification.

## Production verification after separately authorized deployment

Run on the APReferral host using its existing read-only connection. These
commands have no `--apply` or `--commit` and do not create indexes:

```sh
python scripts/migrate_affiliate_account_dedupe.py --seed-from-linkage --output /tmp/affiliate-account-dry-run.json
python scripts/affiliate_qualification_admin.py sync-status
python scripts/affiliate_qualification_admin.py status
python scripts/affiliate_qualification_admin.py preflight
```

Review the new source/reason counters and masked samples against the baseline;
confirm `history_unchanged=true`, `seed_marker_written=false`, disabled mode,
no cutoff and unchanged missing-index list. Read the exact coverage UTC gap
and unresolved counts. A healthy feed alone is not enough; do not reset
cursors or replay data to make the report pass.

Recover missing identities only through authoritative source exports/history
with owner-attested deterministic joins. Resolve conflicts explicitly; never
choose the latest/first account or waive unknown users as zero-account users.
Original-data recovery, any new adapter, production index/seeding writes and
activation each need separate review/authorization. No command to apply or
activate is prescribed by this review package.

## Isolated validation

The focused tests cover each supported issuance/current-linkage/registry
source, committed UIM parsing, conflicts, exact text/leading zeros, masked
reports, stale/unknown coverage, refusal before writes, unchanged earned
history, duplicate ownership, stale preflight markers and idempotent reruns.
Real local MongoDB tests exercise partial-insert interruption/retry and
concurrent-apply lease refusal. Use `QUALIFICATION_TEST_MONGO_URI` pointing only
to an isolated local server; never application/production credentials.
