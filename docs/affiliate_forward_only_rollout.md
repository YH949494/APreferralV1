# Forward-only affiliate qualification v2

Review package only. No merge, deployment, activation, production writes,
historical requalification or reward recalculation was performed. Merging to
main triggers deployment and requires separate authorization.

## Rule and historical coverage

Every new qualification requires successful redemption of the invitee's own
Welcome voucher, supported `coupon_new_player=true/1`, exact completed seven-day
`after_total_bet_amount >= Decimal('80')`, and canonical Account ID lifetime
uniqueness. `79.99` fails; `80` and `80.00` pass. Decimal comparison does not
round, accumulate reports, convert currencies, or take a maximum.

Transport v1 identifies After Total Bet as the owner-attested completed
seven-day result. A completed report does not wait another seven days. An
explicit `seven_day_complete=false`, missing/invalid result, unsupported
transport, unavailable source row, ambiguous identity or temporary feed failure
stays pending/review. Supported flags are numeric 0/1, exact stripped strings
`0`, `1`, `true`, boolean true/false and stripped case-insensitive `false`.
Other flags are unknown; `is_new_player` is never a substitute. Databot requires
qualification transport version integer exactly `1`.

Authoritative unsuccessful redemption, flag 0 with valid data, valid completed
turnover below 80, a duplicate reserved/credited account, or definite invalid
attribution/self-referral fails without qualification, XP or rewards. Configured
success values prove success; statuses `failed`, `failure`, `unsuccessful`,
`rejected` prove failure. Empty/unknown statuses cannot prove failure. Existing
issuance chronology, original referral attribution, source attestation and
ownership checks remain mandatory. Conflicting identity is never guessed.

`strict_complete_history` remains the default migration policy. Unresolved
identities or incomplete historical coverage block completion and activation.
`known_history_plus_post_cutoff` requires an explicit policy flag and
`--acknowledge-unknown-history`, including CLI dry runs. It seeds only the
deterministic accounts resolved by the existing planner. The existing optional
`--seed-from-linkage` uses verified single-account linkage with conflict checks;
it never invents an identity. The completion marker records unresolved counts,
coverage gaps, acknowledgement, seeded ownership fingerprints and this limit:

> Historical unknown Account IDs are not fully covered. Uniqueness is guaranteed
> for deterministically seeded accounts and every qualification after cutoff.

Forward-only mode accepts unknown historical identities and historical coverage
gaps. It does not waive valid source configuration, current sync health,
canonical identity, required indexes, ownership consistency or new-player gates.
Historical qualifications, settlement, XP and earned reward records are not
modified. Deterministic accounts found later remain reserved as legacy credits.

## Compatible revision transition

The new semantic revision is `welcome_redemption_v2`, with gate version
`coupon_new_player_turnover_v2`. The control document identity remains
`welcome_redemption_v1`: a parallel switch could let old readers miss the cutoff
and reopen legacy awarding. `upgrade-rule` compare-and-sets that same document,
is dry-run by default, and writes only with `--commit`. It refuses unless mode
is explicitly disabled, no cutoff field exists, zero v1 qualifications exist,
and no writer or migration is in flight. It preserves source configuration and
historical records and records operator/time/from/to. Repeating it is a no-op.
An active v1 installation requires a separately reviewed compatibility plan.

V2 evidence source refs and fingerprints include the rule revision. Extraction
uses fresh `welcome_v2_evidence_*` cursors/leases/completion markers; old
`welcome_evidence_*` metadata and v1 evidence remain intact. Old passing verdicts
cannot supply v2 awards. Registry keys and the partial unique account index
remain shared across revisions; the index's `_v1` name is its original schema
name, not a rule-version restriction.

## Activation, pending population and crash safety

Activation includes every active pending/manual referral, including pre-cutoff
joins and operational-error rows. Awarded history is left alone. Old terminal
rows without an active referral remain excluded; an active later row never
changes the original inviter. Pre-cutoff redemption qualification times clamp
to the cutoff so old reward windows are not credited retroactively.

Legacy settlement and committed XP backfill hold the same exclusive writer
token on the control document throughout their write pipeline. Activation's
atomic compare-and-set requires no token/migration in flight, compares validated
source/seed/policy state, closes legacy admission, and persists the immutable
cutoff. Legacy awards invalidate the seed marker before writing; a writer
finishing between preflight and activation cannot leave stale coverage usable.

For a future scheduled cutoff, **legacy admission closes when activation is
committed**, before the cutoff. Rows wait for v2 until cutoff. This conservative
behavior prevents a previously admitted worker from straddling a future cutoff.
Prefer immediate activation after quiescing legacy writers. Pre-cutoff
`cancel-scheduled` restores captured pending statuses; after cutoff, rollback is
pause-only and cannot reopen legacy awards.

Pending marking is repeatable after the switch CAS. If marking crashes, the
next scheduler pass repairs it. Validation reads active referrals directly, so
interrupted marking cannot exclude old pending rows. The existing account
reservation → unique qualification → committed registry protocol and idempotent
XP/settlement/reward keys handle restart and duplicate workers. Rejected/held
rows never fabricate `referral_revoked` events for nonexistent settlements.

Writer tokens have no automatic expiry: a suspended worker must not wake and
award after timeout takeover. A crashed writer blocks activation until an
operator confirms that exact process is stopped. Review this recovery dry run;
append `--commit` only after separate recovery authorization:

```sh
python scripts/affiliate_qualification_admin.py release-legacy-writer \
  --token '<token from status>' --acknowledge-writer-stopped
```

## Later evidence and freshness

Newer ordered committed Databot observations can replace gate fields on held,
unleased, undecided evidence and requeue full validation. Incomplete → pass and
incomplete → fail are supported; older replay cannot restore superseded values.
A later successful sync also retries nonconflicted held rows with available
committed source rows. Removed observations, core success/time conflicts,
in-flight decisions and earned corrections still require manual reconciliation.
Earned effects are not automatically revoked. Preview retains persisted holds.

Last supplied production coverage, **not refreshed here**, is
2026-10-02 15:19:03 GMT+8. Runtime preflight reports actual source coverage,
pending/manual count, joins beyond coverage and young joins. Rows beyond
coverage remain pending until later committed imports arrive. A failed feed
attempt holds processing even after a recent successful sync. No cursor resets
or guessed enrichment are used.

`activation-preview` is read-only. It counts the entire active population once
per invitee: pass, reject, pending/review, duplicate-account exclusion, and
already-qualified history left unchanged. Missing evidence is pending.
Unavailable/stale/partial/truncated evidence is explicitly reported and held;
period totals remain hypothetical diagnostics rather than readiness proof.

## Deployment order and operator commands

Run production commands only on the authorized host with its existing secure
Mongo configuration. These commands were not run against production.
Read-only verification:

```sh
python scripts/affiliate_qualification_admin.py status
python scripts/affiliate_qualification_admin.py sync-status
python scripts/affiliate_qualification_admin.py preflight
python scripts/affiliate_qualification_admin.py activation-preview
python scripts/migrate_affiliate_account_dedupe.py
```

After separate deployment approval:

1. Verify disabled mode, absent cutoff, zero v1 events, source attestation and
   namespace, healthy sync and coverage. Different transition prerequisites
   require a new plan; never rewrite earned history to make them pass.
2. Deploy reviewed code to every app/scheduler process. Verify old unguarded
   workers/backfills have stopped. No old version may coexist at activation.
   Keep qualification disabled and preserve source configuration.
3. Review `upgrade-rule` dry run, then separately authorize `upgrade-rule
   --commit`. This changes revision only, not mode/cutoff.
4. Quiesce legacy settlement/backfill before final seeding and activation;
   verify the writer token is absent. Review migration unknowns, masked
   conflicts, preservation counts and source gaps before authorizing apply.
5. Recheck fresh preflight and activation preview. Any later legacy award
   invalidates completion: quiesce, re-plan and re-seed instead of bypassing it.
6. Separately authorize an explicit timezone-aware cutoff. Verify admission
   remains closed, incomplete rows are held, and effects are exact-once.

Forward-only migration dry run and activation preview:

```sh
python scripts/affiliate_qualification_admin.py upgrade-rule
python scripts/migrate_affiliate_account_dedupe.py \
  --history-policy known_history_plus_post_cutoff \
  --acknowledge-unknown-history --seed-from-linkage
python scripts/affiliate_qualification_admin.py activation-preview
```

After separate migration approval, append `--apply` to the reviewed seed command.
Strict historical mode remains available by omitting policy/acknowledgement
flags or choosing `--history-policy strict_complete_history` explicitly.

Final activation command template — **do not execute during this PR task**:

```sh
python scripts/affiliate_qualification_admin.py activate \
  --cutoff "$APPROVED_ACTIVATION_ISO_WITH_OFFSET" \
  --acknowledge-unknown-history --operator "$APPROVED_OPERATOR" --commit
```

Use the separately approved instant with an explicit offset, such as `+08:00`.
This change does not select a cutoff. Omit `--commit` for the dry run. After
cutoff, separately authorize `pause --reason '<reviewed reason>' --commit`;
investigate and rerun preflight before `resume --commit`. Never reset the cutoff,
release credited accounts, or recalculate historical rewards.

## Local validation

The required affiliate CI gate includes `test_affiliate_forward_only_rollout.py`.
The full baseline comparator retains its existing failures and exclusions;
do not describe the legacy full suite as green. Tests cover Decimal boundaries,
unsupported transport, incomplete corrections, feed failure, all pending dates,
history preservation, cutoff/mark crashes, no double award/fake reversal,
reservation races, revision isolation, acknowledgement and strict mode,
seed ownership loss, preview parity and restart/rerun idempotency.

Real Mongo tests require a disposable localhost server and create unique
temporary databases deleted by fixtures. Never use production `MONGO_URL`:

```sh
QUALIFICATION_TEST_MONGO_URI='mongodb://127.0.0.1:27018/?directConnection=true' \
UIM_SYNC_TEST_MONGO_URI='mongodb://127.0.0.1:27018/?directConnection=true' \
python -m pytest -q test_affiliate_forward_only_rollout_real_mongo.py \
  test_affiliate_qualification_rollout_real_mongo.py \
  test_affiliate_qualification_gates_real_mongo.py \
  test_historical_affiliate_account_migration_real_mongo.py \
  test_uim_redemption_sync_real_mongo.py
```

Exact test results and changed files are recorded in the PR description.
