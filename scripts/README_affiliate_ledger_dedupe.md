# Simulated affiliate ledger dedupe

Run a dry-run first:

```bash
MONGO_URL='mongodb://...' python scripts/dedupe_simulated_affiliate_ledgers.py --db referral_bot --dry-run
```

Apply deletion:

```bash
MONGO_URL='mongodb://...' python scripts/dedupe_simulated_affiliate_ledgers.py --db referral_bot
```

The script is idempotent: once duplicates are deleted, re-running reports zero deletions.
