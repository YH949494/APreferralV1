"""
Read-only diagnostic for missing T2 (or any tier) affiliate reward.

Usage:
    USER_ID=<referrer_user_id> python scripts/diagnose_t2_reward.py

No writes are performed. Only find()/count_documents()/aggregate() reads.
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db  # noqa: E402
from affiliate_rewards import (  # noqa: E402
    KL_TZ,
    TIERS,
    T1_THRESHOLD,
    T2_THRESHOLD,
    T3_THRESHOLD,
    T4_THRESHOLD,
    T5_THRESHOLD,
)

USER_ID = os.getenv("USER_ID")
if not USER_ID:
    raise SystemExit("Set USER_ID env var, e.g. USER_ID=123456 python scripts/diagnose_t2_reward.py")

USER_ID = int(USER_ID)
db = get_db()

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
        f"risk_flags={row.get('risk_flags')!r} "
        f"created_at={row.get('created_at')} updated_at={row.get('updated_at')}"
    )

# 3. voucher pool availability for T1 / T2 (unclaimed codes count)
print("\n--- voucher_pools availability ---")
for tier in ("T1", "T2"):
    available = db.voucher_pools.count_documents(
        {"pool_id": tier, "status": "available", "claimed_by": {"$in": [None, 0, ""]}}
    )
    # fall back count without claimed_by filter in case schema differs
    total = db.voucher_pools.count_documents({"pool_id": tier})
    print(f"  {tier}: available={available}  total_rows_in_pool={total}")

# 4. issued pool rows tied to this user's ledgers
print("\n--- issued pool rows for this user ---")
ledger_ids = [row["_id"] for row in ledger_rows]
if ledger_ids:
    issued_rows = list(
        db.voucher_pools.find({"claimed_by_ledger_id": {"$in": ledger_ids}})
    )
    if not issued_rows:
        # try alternate field name some schemas use
        issued_rows = list(db.voucher_pools.find({"ledger_id": {"$in": ledger_ids}}))
    if not issued_rows:
        print("  (no pool rows reference these ledger ids)")
    for row in issued_rows:
        print(
            f"  pool_id={row.get('pool_id')!r} code={row.get('code') or row.get('voucher_code')!r} "
            f"ledger_id={row.get('claimed_by_ledger_id') or row.get('ledger_id')!r} "
            f"issued_at={row.get('claimed_at') or row.get('issued_at')}"
        )
else:
    print("  (no ledger rows to cross-reference)")

print("\n" + "=" * 70)
print("Diagnostic complete. No documents were modified.")
print("=" * 70)
