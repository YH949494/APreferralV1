# Affiliate retention rollback runbook

Restores pre-PR-505 behaviour: a T1–T5 monthly tier reward is issued in the
same evaluation that reaches the tier (`APPROVED -> SETTLING -> ISSUED`),
with no 7-day Official Channel retention wait. Rows already held by the gate
(`PENDING_RETENTION` / `RETENTION_BROKEN`) are drained once by
`scripts/release_affiliate_retention_holds.py`.

App: `apreferralv1` (process groups `web`, `worker`). A push to `main`
deploys (`.github/workflows/deploy.yml`). Nothing below mutates production
data except step 6.

## 1. Disable the gate everywhere (before/with the deploy)

The code default is now `0`, but a Fly secret overrides it.

```bash
fly secrets list -a apreferralv1 | grep -i AFFILIATE_REWARD_RETENTION_DAYS
# If it is listed (any value other than 0), stage its removal so the deploy
# below picks it up in the same restart:
fly secrets unset AFFILIATE_REWARD_RETENTION_DAYS --stage -a apreferralv1
# (Or set it explicitly: fly secrets set AFFILIATE_REWARD_RETENTION_DAYS=0 --stage -a apreferralv1)
```

## 2. Deploy

Merge the PR to `main` (CI deploys). If the secret was changed after the
deploy instead of staged, `fly secrets unset ... -a apreferralv1` (without
`--stage`) restarts the machines with it applied.

## 3. Verify the gate is off in the running machines

```bash
fly logs -a apreferralv1 --no-tail | grep "AFFILIATE\]\[RETENTION_CONFIG"
#   expect: gate=disabled release=immediate source=default   (on web AND worker)
fly ssh console -a apreferralv1 --process-group worker -C 'printenv AFFILIATE_REWARD_RETENTION_DAYS'
#   expect: empty (or 0)
```

Then confirm new entitlements are no longer gated (mongosh; DEPLOY_AT = the
deploy time in UTC):

```js
db.affiliate_ledger.countDocuments({
  ledger_type: "AFFILIATE_MONTHLY",
  created_at: { $gte: ISODate("DEPLOY_AT") },
  retention_required_seconds: { $exists: true }
})   // expect 0
db.affiliate_ledger.find(
  { ledger_type: "AFFILIATE_MONTHLY", created_at: { $gte: ISODate("DEPLOY_AT") } },
  { user_id: 1, tier: 1, year_month: 1, status: 1, created_at: 1, issued_at: 1 }
).sort({ created_at: -1 }).limit(20)  // expect ISSUED (or PENDING_MANUAL on stock), issued_at ~= created_at
```

## 4. Dry run (read-only, secondary-preferred connection, no index creation)

```bash
fly ssh console -a apreferralv1 --process-group worker \
  -C 'python scripts/release_affiliate_retention_holds.py --output /tmp/retention_dry_run.json'
```

stderr carries the summary; stdout/`--output` the full JSON. Review:

* `held_total`, `held_by_status`
* `class_counts` — RELEASE / REVIEW_BLOCKED / REVIEW_RISK / EXCLUDE_*
* `by_tier`, `by_entitlement_month`
* `demand_release` and `inventory[<month>:<pool>]` — `required`,
  `available` (claimable in that entitlement month's own batch),
  `shortfall`. September (`202609`) rows draw from the September batches even
  though they closed on 1 Oct; restock those batches if `shortfall > 0`.
* `rows[]` — masked user ids, never voucher codes.

## 5. Go criteria for the commit

* `class_counts.EXCLUDE_INTEGRITY == 0` (the script refuses otherwise).
* `class_counts.EXCLUDE_ALREADY_ISSUED == 0` and
  `class_counts.EXCLUDE_DUPLICATE_TIER == 0` — anything else is investigated
  first; excluded rows are never written and stay under the (now idle)
  retention worker.
* `shortfall == 0` for every pool, or accept that those rows land in
  `PENDING_MANUAL` (`bundle_denomination_short`) and finish via the existing
  retry sweep after restock.

## 6. Commit

```bash
fly ssh console -a apreferralv1 --process-group worker \
  -C 'python scripts/release_affiliate_retention_holds.py --commit --expect-release <RELEASE count from step 4> --output /tmp/retention_commit.json'
```

Refuses (exit 2, no writes) if the gate is still enabled in the machine's
environment, `AFFILIATE_SIMULATE=1`, any EXCLUDE_INTEGRITY row exists, or the
RELEASE count no longer equals `--expect-release`. Safe to re-run.

## 7-10. Verify

```js
// 7. Held population drained (only EXCLUDE_* rows, if any, may remain)
db.affiliate_ledger.countDocuments({ ledger_type: "AFFILIATE_MONTHLY",
  status: { $in: ["PENDING_RETENTION", "RETENTION_BROKEN"] } })

// Outcome of the backfill
db.affiliate_ledger.aggregate([
  { $match: { retention_release_source: "retention_rollback" } },
  { $group: { _id: { status: "$status", review_reason: "$review_reason" }, n: { $sum: 1 } } }
])

// 8. Issued pool rows == ledger bundle == frozen recipe (expect [])
db.affiliate_ledger.aggregate([
  { $match: { ledger_type: "AFFILIATE_MONTHLY", status: "ISSUED",
              retention_release_source: "retention_rollback" } },
  { $lookup: { from: "voucher_pools", let: { lid: { $toString: "$_id" } }, as: "p",
      pipeline: [ { $match: { $expr: { $and: [
        { $eq: ["$issued_for_ledger_id", "$$lid"] }, { $eq: ["$status", "issued"] } ] } } } ] } },
  { $project: { tier: 1, year_month: 1, expected: "$expected_code_count",
      ledger_codes: { $size: { $ifNull: ["$vouchers", []] } }, pool_rows: { $size: "$p" } } },
  { $match: { $expr: { $or: [ { $ne: ["$ledger_codes", "$pool_rows"] },
                              { $ne: ["$ledger_codes", "$expected"] } ] } } }
])

// 9. No duplicate tier entitlement (expect [])
db.affiliate_ledger.aggregate([
  { $match: { ledger_type: "AFFILIATE_MONTHLY" } },
  { $group: { _id: { u: "$user_id", m: "$year_month", t: "$tier" }, n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } }
])
```

10. UI: open the Mini App as a released user — the reward appears as an
`Affiliate Reward - T<n>` card with its codes (`/vouchers/visible`), and My
Stats shows the tier as `Issued`. Logs: `[AFF_REWARD_RETENTION] action=sweep_done`
reports `candidates: 0` once drained.
