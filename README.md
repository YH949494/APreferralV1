# AdvantPlay Referral Notes

## Referral XP rules (as implemented)
- Base referral XP: `REFERRAL_REWARD_XP` env var (default **30 XP**) per successful referred user.
- Bonus XP: `REFERRAL_BONUS_XP` env var (default **200 XP**) every `REFERRAL_BONUS_INTERVAL` referrals (default **3**).
- A referral becomes **success** only when a pending record exists and the referred user is confirmed as a member of the official channel. The same gate is used when setting `status="success"` and when awarding XP.
- XP events are idempotent via `unique_key`:
  - Base: `ref_success:<referred_user_id>`
  - Bonus: `ref_bonus_triplet:<n>` where `n` is the sequential bonus chunk for that referrer.

## Reconciliation
1) Configure `MONGO_URL`.
2) Run the script (JSON by default, pass a `.csv` path for CSV):
   ```bash
   python reconcile_referrals.py --output referral_reconciliation.json
   ```
3) The script prints a summary and writes per-user mismatches with `ref_count`, `expected_xp`, `xp_from_referrals`, and sample events.

## Backfill (safe + idempotent)
1) Dry-run (recommended first):
   ```bash
   python backfill_referrals.py
   ```
2) Apply changes once reviewed:
   ```bash
   python backfill_referrals.py --commit
   ```
3) The backfill grants any missing `ref_success` events, fills missing bonus chunks, and bumps `referral_count` to at least the number of successful referrals. Duplicate grants are ignored via `unique_key`.

## Tests
Run unit tests (XP idempotency + referral flows):
```bash
python -m pytest
```
