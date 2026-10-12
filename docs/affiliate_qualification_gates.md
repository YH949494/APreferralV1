# Affiliate qualification gates: review package

**Not activated. No merge, deployment, production write, migration, requeue or cursor reset is authorized by this patch.** Both repositories deploy on main changes; merge requires separate authorization.

## Audit and confirmed contract

Audited APReferral main `98e421b25057b567d8270ae5c73babcfd016d09e` and Databot main `41a27fa3e7e57a0da55e14070e7a41117f1bfdd9`. APReferral's existing `codex/qualification-rollout-audit` commits `eaae915` and `11e736e` are included: shared issuance chronology, future-time seeding checks, legacy XP backfill cutoff guard, and their tests. GitHub PR-list API access was unavailable in the build session; remote branch/code inspection is not verification of PR review status.

YH attested on 2026-10-12 that a UIM export claim row represents successful gaming redemption, timestamps use GMT+8, accounts are globally unique, and `after_total_bet_amount` is the completed seven-day dollar result. No supplied source spreadsheet was inspected in this session; these business semantics come from the owner contract, not inferred analytics. No conversion, accumulation, growth flow or extra waiting timer is added.

| Stage | Audited gap | Patch |
| --- | --- | --- |
| UIM import | `is_new_player` mapped; no separate coupon flag. Turnover converted to float; missing/invalid values become analytics zero. | Exact coupon header mapped separately; original turnover text retained as `after_total_bet_amount_raw`. Existing analytics float unchanged. Source headers/version recorded. |
| Committed evidence | Immutable `uim_import_rollback_logs.new_document`/`previous_document` include normalized importer output. | New raw fields survive commit, correction and rollback. Real Mongo tests verify all three. |
| Private feed | Neither qualification input serialized. | Add coupon flag, raw turnover under feed `after_total_bet_amount`, source columns, and `qualification_fields_version=1`. Old logs have nulls, never guessed from analytics floats. |
| APReferral sync | Insert-only per-observation refs and completed cursors. | Allowlist retains additive fields on new observations. Existing documents are intentionally not rewritten by normal re-sync. |
| Canonical evidence | Fingerprint originally contained only success/time. | Retain both values and provenance; fingerprint includes gate values; committed observation ordering guards against stale replay. |
| Live and preview | Shared validator lacked the new gates. | Both use `assess_qualification_fields` after ownership, original attribution, issuance and referral chronology, before account reservation/effects. |

## Versions and compatibility

Control identity, lifecycle, existing idempotency keys and earned event rule version remain `welcome_redemption_v1`. New evidence, prospective qualified events and preview identify `qualification_gate_version=coupon_new_player_turnover_v1`. This is an additive gate revision, not a second concurrently active rule. Feed endpoint/envelope stay `databot.uim_redemption_feed.v1`; row qualification fields are additive and versioned separately.

The supplied source configuration (`databot_uim`, `uim_global`, `Asia/Kuala_Lumpur`, successful-only YH attestation, disabled, no cutoff, both review policies) is compatible and must remain unchanged during review. Databot source configuration now refuses other account namespaces and namespace columns. `platform_code` never participates in identity. Case and leading zeros remain intact; numeric spreadsheet account cells still require review. Existing namespace freezing/seed checks remain; if a deployment has seeded a different namespace, **stop for a separately reviewed identity compatibility plan** rather than editing it automatically.

Coupon flag representations documented for this revision: numeric 0/1, or stripped text `"0"`/`"1"`. Boolean values, words, fractional/other flags, missing and ambiguous input require review. `is_new_player` is never a substitute. Turnover accepts ordinary decimal/scientific numeric text, finite nonnegative numbers, and compares with `Decimal('9.99')` without rounding. Currency symbols, grouping separators, underscores, booleans, negatives, NaN, infinity and missing values require review. Explicit flag 0 fails; valid turnover <=9.99 fails. Both fields must be valid before a gate-failure verdict is reported.

Historical registry seeding continues to use reliable ownership, successful redemption and chronology; it never filters history on new gate values. Earned counts, XP, reward ledgers and account credits are not recalculated or revoked. The preview's historical simulation is hypothetical and writes nothing.

## Repeated observations and corrections

Committed Databot batch timestamp, batch identity and immutable row ref provide a total observation order. For the same code/account/success/time, a newer authoritative gate report replaces an unprocessed, unleased evidence snapshot using compare-and-set. Older replay cannot restore stale values. Identical reports never sum turnover or duplicate awards; no maximum is taken. Changes to redemption time/success remain manual source-conflict review; source deletions remain tombstones. A rolled-back batch never supplies fresh qualifying evidence.

A source conflict persists through requeue, so requeue alone cannot resurrect stale passing data. Corrections to already finalized/credited evidence, or evidence being processed, use the existing conservative manual-reconciliation path. Earned events are marked disputed rather than revoked. The validator re-reads canonical evidence, and the award decision compare-and-sets its validated fingerprint before reserving an account. A conflict concurrent with event insertion is also propagated to the earned event for manual review.

Live processing holds while committed source extraction is incomplete or Databot sync readiness is stale/partial/unavailable; it cannot award an earlier passing report before later known corrections are extracted. Preview retains capped source reads, bulk lookups and explicit incomplete indicators. Truncated figures are provisional, **not lower bounds**: unseen corrections may reduce them.

## Already-synced rows: targeted enrichment plan (dry run only)

Run on the APReferral host with its existing read-only connection:

```sh
python scripts/plan_uim_qualification_enrichment.py --limit 1000
```

This bounded read-only inventory returns candidate immutable refs/batch IDs, missing fields and an explicit incomplete flag; it has no apply option. It does not establish that values can be recovered. If incomplete, prepare a separately reviewed bounded pagination/export workflow rather than treating the page as all candidates. No application secret values are printed.

For a proposed manifest, reviewers must:

1. Join each explicit allowlisted APReferral ref to its exact Databot committed log and source export; verify coupon/account, batch/ref, original time/provenance, commit status and rollback/removal history. Never join loosely by account or mix other vouchers.
2. If the log already contains the exact qualification fields, transport them from that same immutable observation. If it predates this importer, **the original spreadsheet/export and owner-attested mapping are required**. An analytics float cannot reconstruct exact decimal text; `is_new_player` cannot reconstruct the coupon flag. Unrecoverable rows remain review.
3. Prepare a manifest containing immutable identities, original-data hashes/provenance and recovered raw values. Show a dry-run diff/counts; separately list associated canonical evidence, finalized events, source conflicts and removed/rolled-back candidates. Exclude ambiguous or noncommitted candidates.
4. After separate write authorization, a targeted enrichment tool should compare-and-set only missing fields on each allowlisted sync ref, verifying identity and provenance again. Existing non-null fields and earned effects must not be overwritten. Idempotent retries should produce zero changes. No cursor changes, evidence deletions, batch-wide replay, new qualifications or XP/reward writes belong in enrichment.
5. Missing-field canonical evidence needs its own explicit, reviewed enrichment/reconciliation before requeue; enriching only synced rows does not update `$setOnInsert` evidence or reopen extracted batches. Keep old/new fingerprints and field versions in the reconciliation report. Requeue is a separately authorized operation through full validation. Source conflicts/tombstones cannot simply be cleared.

No enrichment or production inventory was applied in this session.

## Production blockers and next operator steps

Supplied (not refreshed) observations: 5,037 recipient matches; 4,997 `redeemed_at_timezone_unverified`; 40 `source_row_removed_by_later_import`; zero would qualify; sync stale with `feed_unreachable`/ConnectionError. No runtime production binding was used to verify these observations. Adding gate fields does not resolve private networking or legacy timezone provenance. The review policies remain review.

1. Review both patch branches and the included rollout safeguards. Do not merge until deployment authorization is explicit.
2. On the authorized production hosts, run existing read-only source audit, qualification status/preflight and the enrichment inventory. Establish current feed availability, row field completeness and source attestation. Repair the private feed connectivity separately; do not expose it publicly as a shortcut.
3. Review legacy timezone evidence and source-removal cases individually under the existing policies. Do not automatically reinterpret legacy timestamps because GMT+8 was attested for the source.
4. Approve a recoverable targeted enrichment manifest if required; authorize its writes/requeues separately. Review historical account seed plan without using the new gates to exclude historical accounts.
5. Recheck fresh, complete preview and preflight with both patches deployed only after deployment approval. Select an explicit timezone-aware launch cutoff in a separate activation decision. No cutoff is invented here; mode remains disabled.

## Validation

Use separate dependency environments for the repositories (their pymongo pins differ). APReferral's curated affiliate CI now includes `test_affiliate_qualification_gates.py`; standalone real-Mongo tests additionally cover failing gates and an authoritative correction between validation and atomic award decision. The existing rollout real-Mongo suite covers account/invitee races, crash recovery and exactly-once effects. Databot's new tests cover source mapping, precision, malformed/missing data, legacy compatibility, immutable committed corrections and rollback/removal.

All real Mongo tests must use an isolated local database URI, never `MONGO_URL`/production credentials. See each test module for its dedicated test URI variable. The build-session validation results are in the branch/PR review description; production behavior and fresh-task restoration are unverified.
