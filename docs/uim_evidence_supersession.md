# UIM evidence supersession (final batch re-observing an older record)

Status: **implemented; nothing deployed, activated or written to production.** `welcome_redemption_v2` stays disabled,
the legacy worker stays stopped. Companion to `docs/uim_coverage_contract.md`. Databot is unchanged.

## 1. Root cause

Evidence identity is `source_ref = sha256(rule | source | code_hash | account_key)`, so a final batch that re-exports a
record an older batch already carried lands on the **same** evidence. The older observation was unverified (legacy
`redeemed_at_basis=unknown_legacy` -> `redeemed_at=None`, `redeemed_at_reason=redeemed_at_timezone_unverified`; no
completeness marker). The newer one is attested (time with provenance, `seven_day_complete`, declared coverage). Both
paths then treated "unverified -> verified" as a **disagreement**:

| Path | Code | Effect |
|---|---|---|
| Live extraction | `record_redemption_evidence`: `same_redemption` compared `None` with the real instant, so it fell to `fingerprint differs -> _flag_conflicting_observation` | permanent `source_conflict_at`, status `pending_review`, old unverified `redeemed_at` kept |
| Preview | `_build_evidences`: same comparison, `prior["_conflicting_observation"] = True`, **old** doc kept | `conflicting_source_observations` |
| Evidence never upgraded | `redeemed_at`, `qualification_fields`, `evidence_basis` are `$setOnInsert` only | `redeemed_at_timezone_unverified` / completeness holds survive any later re-import |

Reproduced before changing code (old `unknown_legacy` row + final attested row -> preview `conflicting_source_observations`,
live evidence `pending_review`, `redeemed_at=None`).

## 2. The rule (`affiliate_qualification.reobservation_verdict`)

One pure function, used by live extraction, the Qualification Preview (`fold_observation`) and the repair command.

A newer observation is **attested** only if all hold: committed with an ordering; `redeemed_at` established (basis
`source_offset`/`naive_wallclock`); text account (no `account_reason`); successful per the attested source contract;
transport v1 with `seven_day_complete is True`; valid `coupon_new_player`; exact-Decimal `after_total_bet_amount`; and the
batch has operator-declared **complete** `voucher_redemption_date` coverage containing the redemption's
**Asia/Kuala_Lumpur** day (source timezone must be Asia/Kuala_Lumpur).

Against an older, **unattested** observation of the same record:

* every fact the old one established (instant, success, `coupon_new_player`, Decimal turnover) agrees -> **UPGRADE**:
  evidence moves onto the attested observation, `status=received` (the unchanged validator decides), old provenance is
  appended to `supersession_history`;
* any established fact differs -> **CONFLICT** (review, as before);
* old one was already attested and equal -> **IDENTICAL** (no conflict, one evidence document, turnover counted once).

Unchanged: attested-vs-attested corrections follow the existing policy (newest commit wins before a decision, flagged
after one). A removed observation stays removed (also against a later re-add). An observation outside declared coverage
(or partial/inferred coverage, other timezone) never upgrades anything. Evidence with a decision, lease, void,
qualification or registry entry is never rewritten. Account ID case, leading zeros and namespace are never touched
(`account_key` is identity). Turnover is only ever compared/stored as exact Decimal.

Not covered by design: an old observation whose *account* was unverifiable (`account_reason`) has a different
`source_ref` and is not merged with the verified-account record.

## 3. Repair command (already-synced evidence)

```
# 0. deploy this code first (stops new spurious conflicts); worker stays stopped; V2 stays inactive
# 1. baseline
python scripts/affiliate_qualification_admin.py activation-preview > preview_before.json
# 2. DRY RUN (default; read-only connection; counts + masked samples only)
python scripts/affiliate_qualification_admin.py reconcile-uim-evidence --limit 500
# 3. COMMIT, only after reading the dry-run counts. Page with the printed next_after_id until it is null.
MONGO_URL=... python scripts/affiliate_qualification_admin.py reconcile-uim-evidence --limit 500 --commit --operator <you>
MONGO_URL=... python scripts/affiliate_qualification_admin.py reconcile-uim-evidence --limit 500 --after-id <next_after_id> --commit --operator <you>
# 4. verify
python scripts/affiliate_qualification_admin.py activation-preview > preview_after.json
python scripts/affiliate_qualification_admin.py preflight
```

Safety: dry run unless `--commit`; refused while V2 is active; `--limit` <= 5000 per run; a replay larger than
`--max-scan-rows` (default 500000) aborts before writing; idempotent (repaired evidence carries
`evidence_basis.coverage_attested=true` and stops being a candidate; a re-run changes nothing). Writes only
`welcome_redemption_evidence` candidates and one audit document per committing run in `uim_evidence_reconciliations`.
It never touches `qualified_events`, `referral_events`, `affiliate_ledger`, rewards, XP, `pending_referrals`, the
registry or the control document, and never qualifies/rejects anyone.

Dry-run `actions`: `would_repair`, `conflict_kept_for_review`, `removed_stays_removed`,
`not_attested_remains_pending` (+ `not_attested_reasons`), `no_committed_source_observation`, `skipped_has_effects`;
`evidence_not_repairable_state` counts evidence in other states (left alone).

**Candidate selection and diagnostics.** A guard field that is absent *or stored as `null`* counts as "not set"
(`$exists: False` does not match `null`; that silently excluded `pending_review` evidence in production and the dry run
returned `ok=true, candidates=0`). A lease protects only while `lease_until > now`. The report carries
`candidate_guards`: `scope_documents` (unattested v2 UIM evidence), `excluded_by_guard`
(`status_not_repairable`, `voided`, `active_lease`, `qualification_decision_recorded`; one document counts under every
guard it trips), `excluded_any_guard`, `evidence_total` and `evidence_by_source_rule_status` (shows evidence stored
under a different source/rule_version). Registry / qualified-event ownership is reported per document as
`skipped_has_effects`.
**Mismatch refusal.** On the first page, zero candidates while the preview still reports more repairable legacy reasons
(`redeemed_at_timezone_unverified`, `conflicting_source_observations`, `seven_day_data_incomplete`) returns
`ok=false`, `error=candidate_selection_preview_mismatch` (CLI exit 1) when any pending source record has no in-scope
evidence document. The check is by `source_ref` (a guard-protected document explains only its own record, never
another's), bounded by `--max-scan-rows`, and reports `source_records_without_evidence` plus masked samples - never a
silent no-op. A repair also unsets `lease_until`, so the evaluator (which selects an absent or expired lease) picks it up.

Interpreting the preview afterwards: `unverified_completeness_synced_rows` is a raw count of every historical synced
row (including superseded ones) and is informational; the decision-relevant figure is
`pending_maturity.unverified_completeness_effective_evidence`.
**Residual by construction:** records the final batch did *not* re-observe keep their unverified provenance; this
change cannot (and must not) invent a timezone for them. The dry run's `not_attested_remains_pending` /
`no_committed_source_observation` counts say how many; resolving them needs a data-owner decision
(`configure-source --legacy-time-basis naive_wallclock` with attestation) or a re-upload that covers them.

## 4. Rollback / recovery

* Code: revert the PR. Evidence already repaired remains valid (`received`, attested provenance); nothing else depends
  on the new code.
* Data: every repaired document keeps `supersession_history[]` (append-only) with the replaced `redeemed_at`,
  `redeemed_at_reason`, `redemption_successful`, `qualification_fields`, `evidence_basis`, `authoritative_order`,
  `observation_fingerprint`, `status`, `reason`, `attempts`, plus `cleared_conflict_reason`/`cleared_conflict_at` when a
  hold was lifted, and the `run` id (matching the `uim_evidence_reconciliations` audit row). Restoring one document =
  `$set` the entry's `from.*` fields and, if present, `source_conflict_at`/`source_conflict_reason` from the
  `cleared_*` fields (verified round trip in `test_audit_entry_is_enough_to_restore_the_replaced_evidence`).
  Do it per `_id`, only while V2 is inactive.
* Source batches/rows are never modified or deleted by any of this.
* If a dry run looks wrong (counts far from the preview's conflict/unverified figures), do not commit: the dry run
  writes nothing.

## 5. Diagnosing "redemption before Welcome issuance"

`python scripts/affiliate_qualification_admin.py diagnose-issuance-order` (read-only, secondary-preferred connection,
no `--commit`). For every record the validator flags `redemption_before_welcome_issuance` it reports the
issued-minus-redeemed gap histogram, which source supplied the earliest issuance time, and how many records each
clock-basis hypothesis would clear (`claim_time_is_kl_wall_clock_stored_as_utc`,
`all_issuance_times_are_kl_wall_clock_stored_as_utc`, `redemption_time_is_utc_not_kl`). Gaps inside 0-8h with a
hypothesis clearing most of them point at a timezone mix-up; large or scattered gaps are real data. It changes no rule.
