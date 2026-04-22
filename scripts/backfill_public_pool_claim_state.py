#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
import os

from database import init_db, get_db
from vouchers import _as_aware_utc, is_public_pool, now_utc


WINDOW_DAYS = 30
CAP = 50


def _prune_recent(values, ref):
    cutoff = ref - timedelta(days=WINDOW_DAYS)
    kept = []
    for value in values:
        parsed = _as_aware_utc(value)
        if parsed and parsed >= cutoff:
            kept.append(parsed)
    kept.sort()
    if len(kept) > CAP:
        kept = kept[-CAP:]
    return kept


def main():
    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("MONGO_DB_NAME", "referral_bot")
    init_db(mongo_url=mongo_url, db_name=db_name)
    db = get_db()

    now_ref = now_utc()
    drops_cache = {}
    by_user = defaultdict(list)

    voucher_pool_cache = {}
    cursor = db["voucher_claims"].find(
        {
            "status": "claimed",
            "voucher_code": {"$exists": True, "$nin": [None, ""]},
        },
        {"user_id": 1, "drop_id": 1, "claimed_at": 1, "updated_at": 1, "created_at": 1, "voucher_code": 1},
    )
    scanned = matched_public_claims = 0
    for claim in cursor:
        scanned += 1
        user_id = claim.get("user_id")
        drop_id = claim.get("drop_id")
        if user_id in (None, "") or drop_id in (None, ""):
            continue
        cache_key = str(drop_id)
        drop_doc = drops_cache.get(cache_key)
        if drop_doc is None:
            drop_doc = db["drops"].find_one({"_id": drop_id}) or db["drops"].find_one({"_id": str(drop_id)}) or {}
            drops_cache[cache_key] = drop_doc
        if not is_public_pool(drop_doc):
            continue
        voucher_code = str(claim.get("voucher_code") or "").strip()
        if voucher_code:
            cache_key = f"{drop_id}:{voucher_code}"
            pool_name = voucher_pool_cache.get(cache_key)
            if pool_name is None:
                voucher_doc = db["vouchers"].find_one({"dropId": {"$in": [drop_id, str(drop_id)]}, "code": voucher_code}, {"pool": 1}) or {}
                pool_name = str(voucher_doc.get("pool") or "public").strip().lower()
                voucher_pool_cache[cache_key] = pool_name
            if pool_name != "public":
                continue
        claim_at = _as_aware_utc(claim.get("claimed_at")) or _as_aware_utc(claim.get("updated_at")) or _as_aware_utc(claim.get("created_at"))
        if not claim_at:
            continue
        by_user[int(user_id)].append(claim_at)
        matched_public_claims += 1

    updated = 0
    for user_id, timestamps in by_user.items():
        recent = _prune_recent(timestamps, now_ref)
        db["users"].update_one(
            {"user_id": int(user_id)},
            {
                "$set": {
                    "has_ever_claimed_public_pool": True,
                    "public_claim_timestamps_recent": recent,
                    "recent_public_claim_count_30d": len(recent),
                    "public_pool_claim_state_updated_at": now_ref,
                }
            },
            upsert=True,
        )
        updated += 1

    print(
        f"backfill_public_pool_claim_state scanned={scanned} matched_public_claims={matched_public_claims} users_updated={updated}"
    )


if __name__ == "__main__":
    main()
