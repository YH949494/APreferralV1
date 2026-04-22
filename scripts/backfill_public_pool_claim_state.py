#!/usr/bin/env python3
"""One-shot backfill for public-pool claim state fields on the users collection.

Designed to run as a Fly.io release_command. Deliberately self-contained:
does NOT import from vouchers, flask, telegram, or any other heavy app module.
"""
from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# Ensure /app is on sys.path regardless of working directory or PYTHONPATH.
_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from database import init_db, get_db  # noqa: E402 – must come after sys.path fix

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("backfill_public_pool_claim_state")

WINDOW_DAYS = 30
CAP = 50

# ---------------------------------------------------------------------------
# Inline helpers — copied from vouchers.py to avoid importing the entire
# Flask/Telegram/APScheduler stack, which would OOM the release_command VM.
# ---------------------------------------------------------------------------

_PERSONALISED_ALIASES = ("personalised", "personalized")


def _normalize_drop_type(value) -> str:
    dtype = str(value or "pooled").strip().lower()
    if dtype in _PERSONALISED_ALIASES:
        return "personalised"
    return dtype


def _drop_audience_type(drop: dict) -> str:
    if not isinstance(drop, dict):
        return "public"
    audience = drop.get("audience")
    if isinstance(audience, str):
        atype = audience.strip().lower()
        if atype:
            return atype
    elif isinstance(audience, dict):
        atype = (audience.get("type") or audience.get("audience") or "").strip().lower()
        if atype:
            return atype
    wl = drop.get("whitelistUsernames") or []
    for item in wl:
        if isinstance(item, str):
            lowered = item.strip().lower()
            if lowered in ("new_joiner_48h", "new joiner 48h", "newjoiner48h"):
                return "new_joiner_48h"
            if lowered in ("new_joiner", "new joiner", "newjoiner"):
                return "new_joiner"
    return "public"


def _is_public_pooled_drop(drop: dict) -> bool:
    if _normalize_drop_type((drop or {}).get("type", "pooled")) != "pooled":
        return False
    audience_type = _drop_audience_type(drop)
    if audience_type != "public":
        return False
    elig_mode = str(((drop or {}).get("eligibility") or {}).get("mode") or "public").strip().lower()
    return elig_mode == "public"


def is_public_pool(pool_doc: dict) -> bool:
    if not isinstance(pool_doc, dict):
        return False
    if not _is_public_pooled_drop(pool_doc):
        return False
    if _drop_audience_type(pool_doc) != "public":
        return False
    if pool_doc.get("internal_only") is True:
        return False
    return True


def _as_aware_utc(dt) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(dt, str):
        try:
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------


def _prune_recent(values: list, ref: datetime) -> list:
    cutoff = ref - timedelta(days=WINDOW_DAYS)
    kept = [v for v in (_as_aware_utc(v) for v in values) if v and v >= cutoff]
    kept.sort()
    return kept[-CAP:] if len(kept) > CAP else kept


def main() -> None:
    logger.info("backfill_public_pool_claim_state starting")

    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("MONGO_DB_NAME", "referral_bot")

    if not mongo_url:
        logger.error("MONGO_URL is not set — cannot run backfill")
        sys.exit(1)

    logger.info("Connecting to MongoDB db=%s", db_name)
    init_db(mongo_url=mongo_url, db_name=db_name)
    db = get_db()
    logger.info("Connected to MongoDB")

    now_ref = now_utc()
    drops_cache: dict = {}
    voucher_pool_cache: dict = {}
    by_user: dict = defaultdict(list)

    cursor = db["voucher_claims"].find(
        {
            "status": "claimed",
            "voucher_code": {"$exists": True, "$nin": [None, ""]},
        },
        {
            "user_id": 1,
            "drop_id": 1,
            "claimed_at": 1,
            "updated_at": 1,
            "created_at": 1,
            "voucher_code": 1,
        },
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
            drop_doc = (
                db["drops"].find_one({"_id": drop_id})
                or db["drops"].find_one({"_id": str(drop_id)})
                or {}
            )
            drops_cache[cache_key] = drop_doc
        if not is_public_pool(drop_doc):
            continue
        voucher_code = str(claim.get("voucher_code") or "").strip()
        if voucher_code:
            vk = f"{drop_id}:{voucher_code}"
            pool_name = voucher_pool_cache.get(vk)
            if pool_name is None:
                voucher_doc = (
                    db["vouchers"].find_one(
                        {"dropId": {"$in": [drop_id, str(drop_id)]}, "code": voucher_code},
                        {"pool": 1},
                    )
                    or {}
                )
                pool_name = str(voucher_doc.get("pool") or "public").strip().lower()
                voucher_pool_cache[vk] = pool_name
            if pool_name != "public":
                continue
        claim_at = (
            _as_aware_utc(claim.get("claimed_at"))
            or _as_aware_utc(claim.get("updated_at"))
            or _as_aware_utc(claim.get("created_at"))
        )
        if not claim_at:
            continue
        by_user[int(user_id)].append(claim_at)
        matched_public_claims += 1

    logger.info("Scan complete: scanned=%d matched_public_claims=%d", scanned, matched_public_claims)

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

    logger.info(
        "backfill_public_pool_claim_state done: scanned=%d matched_public_claims=%d users_updated=%d",
        scanned,
        matched_public_claims,
        updated,
    )


if __name__ == "__main__":
    main()
