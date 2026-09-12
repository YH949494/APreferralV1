"""
Read-only diagnostic for missing T2 (or any tier) affiliate reward.

Usage:
    USER_ID=<referrer_user_id> python scripts/diagnose_t2_reward.py

Connects to Mongo directly (does NOT call database.init_db(), which would
also run index-creation writes) and only ever calls find()/find_one()/
count_documents(). No documents are modified.
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient  # noqa: E402

from affiliate_rewards import (  # noqa: E402
    KL_TZ,
    TIERS,
    T1_THRESHOLD,
    T2_THRESHOLD,
    T3_THRESHOLD,
    T4_THRESHOLD,
    T5_THRESHOLD,
    _as_aware_utc,
    _month_window_from_yyyymm,
)

USER_ID = os.getenv("USER_ID")
if not USER_ID:
    raise SystemExit("Set USER_ID env var, e.g. USER_ID=123456 python scripts/diagnose_t2_reward.py")

USER_ID = int(USER_ID)

MONGO_URL = os.environ.get("MONGO_URL")
if not MONGO_URL:
    raise SystemExit("MONGO_URL is not set")
DB_NAME = os.environ.get("MONGO_DB_NAME", "referral_bot")

client = MongoClient(MONGO_URL)
db = client[DB_NAME]

now_kl = datetime.now(timezone.utc).astimezone(KL_TZ)
yyyymm = now_kl.strftime("%Y%m")

print("=" * 70)
print(f"USER_ID = {USER_ID}   month = {yyyymm}")
print("=" * 70)

# 1. qualified_count this month
qualified_count = db.qualified_events.count_documents(
    {
        "referrer_id": USER_ID,
        "qualified_at": {
            "$gte": KL_TZ.localize(datetime(now_kl.year, now_kl.month, 1)).astimezone(timezone.utc),
        },
    }
)
print(f"\nqualified_count_this_month: {qualified_count}")

thresholds = {"T1": T1_THRESHOLD, "T2": T2_THRESHOLD, "T3": T3_THRESHOLD, "T4": T4_THRESHOLD, "T5": T5_THRESHOLD}
expected_tiers = [t for t in TIERS if qualified_count >= thresholds[t]]
print(f"Expected tiers (cumulative, threshold-based): {expected_tiers}")

# 2. affiliate_ledger rows for this user+month
print("\n--- affiliate_ledger (this user, this month) ---")
ledger_rows = list(
    db.affiliate_ledger.find(
        {"user_id": USER_ID, "year_month": yyyymm},
    ).sort("tier", 1)
)
if not ledger_rows:
    print("  (no ledger rows found for this user/month)")
for row in ledger_rows:
    print(
        f"  tier={row.get('tier')!r:6} pool_id={row.get('pool_id')!r:10} "
        f"status={row.get('status')!r:16} voucher_code={row.get('voucher_code')!r} "
        f"dedup_key={row.get('dedup_key')!r} "
        f"target_mode={row.get('target_mode')!r} target_batch_id={row.get('target_batch_id')!r} "
        f"risk_flags={row.get('risk_flags')!r} "
        f"created_at={row.get('created_at')} updated_at={row.get('updated_at')}"
    )

# 3. voucher pool availability for T1 / T2 — scoped to what each ledger can
# actually claim from (its resolved target batch, or the batch that would
# resolve for this month, or legacy unbounded stock), not raw tier totals.
print("\n--- voucher pool availability (scoped to actual claimable source) ---")
period_start_utc, period_end_utc = _month_window_from_yyyymm(yyyymm)
ledger_by_tier = {str(r.get("tier") or "").strip().upper(): r for r in ledger_rows}

for tier in ("T1", "T2"):
    ledger = ledger_by_tier.get(tier)
    target_mode = ledger.get("target_mode") if ledger else None
    target_batch_id = ledger.get("target_batch_id") if ledger else None

    if target_mode == "batch" and target_batch_id:
        available = db.voucher_pools.count_documents({"batch_id": target_batch_id, "status": "available"})
        print(f"  {tier}: ledger already pinned to batch_id={target_batch_id} -> available={available}")
        continue
    if target_mode == "legacy":
        available = db.voucher_pools.count_documents({"pool_id": tier, "batch_id": {"$exists": False}, "status": "available"})
        print(f"  {tier}: ledger pinned to legacy pool -> available={available}")
        continue

    # Not yet resolved (or no ledger yet this month): show what WOULD resolve.
    matches = []
    if period_start_utc is not None and period_end_utc is not None:
        for batch in db.affiliate_voucher_batches.find({"pool_id": tier}):
            starts_at = _as_aware_utc(batch.get("starts_at"))
            ends_at = _as_aware_utc(batch.get("ends_at"))
            if starts_at is None or ends_at is None:
                continue
            if starts_at <= period_start_utc and ends_at >= period_end_utc:
                matches.append(batch)

    if len(matches) > 1:
        print(f"  {tier}: AMBIGUOUS — {len(matches)} batches fully cover this month: "
              f"{[str(b.get('_id')) for b in matches]}")
    elif matches:
        batch = matches[0]
        available = db.voucher_pools.count_documents({"batch_id": batch["_id"], "status": "available"})
        print(f"  {tier}: would resolve to batch_id={batch['_id']} (starts_at={batch.get('starts_at')} "
              f"ends_at={batch.get('ends_at')}) -> available={available}")
    else:
        legacy_available = db.voucher_pools.count_documents({"pool_id": tier, "batch_id": {"$exists": False}, "status": "available"})
        print(f"  {tier}: NO batch fully covers this month for pool_id={tier}. "
              f"legacy_unbounded_available={legacy_available}")

# 4. issued pool rows tied to this user's ledgers
print("\n--- issued pool rows for this user ---")
ledger_ids = [row["_id"] for row in ledger_rows]
if ledger_ids:
    issued_rows = list(db.voucher_pools.find({"ledger_id": {"$in": ledger_ids}}))
    if not issued_rows:
        print("  (no pool rows reference these ledger ids)")
    for row in issued_rows:
        print(
            f"  pool_id={row.get('pool_id')!r} batch_id={row.get('batch_id')!r} "
            f"code={row.get('code')!r} ledger_id={row.get('ledger_id')!r} "
            f"issued_at={row.get('issued_at')}"
        )
else:
    print("  (no ledger rows to cross-reference)")

print("\n" + "=" * 70)
print("Diagnostic complete. No documents were modified.")
print("=" * 70)
