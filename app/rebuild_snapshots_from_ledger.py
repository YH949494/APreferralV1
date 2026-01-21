#!/usr/bin/env python3
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pytz
from pymongo import MongoClient, UpdateOne

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur") 
DEFAULT_DRY_RUN = "1"

logger = logging.getLogger("rebuild_snapshots")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _parse_iso_datetime(value: str) -> datetime:
    if not value:
        raise ValueError("CUTOFF_UTC is required")
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _compute_windows(cutoff_utc: datetime) -> tuple[datetime, datetime, datetime, datetime]:
    cutoff_local = cutoff_utc.astimezone(KL_TZ)
    week_start_local = (cutoff_local - timedelta(days=cutoff_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    month_start_local = cutoff_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return (
        week_start_local.astimezone(timezone.utc),
        month_start_local.astimezone(timezone.utc),
        week_start_local,
        month_start_local,
    )


def _ensure_write_enabled(dry_run: bool) -> None:
    if dry_run:
        return
    if os.environ.get("CONFIRM") != "YES":
        raise RuntimeError("CONFIRM=YES is required when DRY_RUN=0")


def _iter_chunks(items: list[int], chunk_size: int = 1000):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _count_missing_users(users_collection, user_ids: list[int]) -> int:
    if not user_ids:
        return 0
    found = 0
    for chunk in _iter_chunks(user_ids):
        found += users_collection.count_documents({"user_id": {"$in": chunk}})
    return max(len(user_ids) - found, 0)


def _aggregate_xp(db, cutoff_utc: datetime, week_start_utc: datetime, month_start_utc: datetime):
    event_time = {"$ifNull": ["$created_at", "$ts"]}
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"$expr": {"$lte": [event_time, cutoff_utc]}},
                    {"user_id": {"$ne": None}},
                    {"$or": [{"invalidated": {"$exists": False}}, {"invalidated": False}]},
                ]
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "weekly_xp": {"$sum": {"$cond": [{"$gte": [event_time, week_start_utc]}, "$xp", 0]}},
                "monthly_xp": {"$sum": {"$cond": [{"$gte": [event_time, month_start_utc]}, "$xp", 0]}},
            }
        },
    ]
    return list(db.xp_events.aggregate(pipeline, allowDiskUse=True))


def _summarize_top(results: list[dict], key: str, limit: int = 20) -> list[tuple[int, int]]:
    rows = [(int(row.get("_id")), int(row.get(key, 0))) for row in results if row.get("_id") is not None]
    rows = [row for row in rows if row[1] > 0]
    rows.sort(key=lambda item: item[1], reverse=True)
    return rows[:limit]


def _restore_referrals(db, cutoff_utc: datetime, dry_run: bool) -> dict:
    restore_ref = os.environ.get("RESTORE_REF") == "1"
    if not restore_ref:
        return {
            "ref_updates_planned": 0,
            "ref_updates_applied": 0,
            "ref_missing_users": 0,
        }

    collection_name = os.environ.get("REF_COLLECTION")
    if not collection_name:
        raise RuntimeError("REF_COLLECTION is required when RESTORE_REF=1")

    if collection_name not in db.list_collection_names():
        raise RuntimeError(f"REF_COLLECTION '{collection_name}' not found")

    collection = db[collection_name]
    sample = collection.find_one()
    if not sample:
        logger.info("[referral] no documents in %s", collection_name)
        return {
            "ref_updates_planned": 0,
            "ref_updates_applied": 0,
            "ref_missing_users": 0,
        }

    referrer_field = os.environ.get("REF_REFERRER_FIELD")
    if not referrer_field:
        for candidate in ("referrer_id", "inviter_id", "user_id"):
            if candidate in sample:
                referrer_field = candidate
                break
    if not referrer_field:
        raise RuntimeError("Unable to infer referrer field; set REF_REFERRER_FIELD")

    time_field = os.environ.get("REF_TIME_FIELD")
    if not time_field:
        for candidate in ("occurred_at", "created_at", "ts"):
            if candidate in sample:
                time_field = candidate
                break
    if not time_field:
        raise RuntimeError("Unable to infer referral time field; set REF_TIME_FIELD")

    success_field = os.environ.get("REF_SUCCESS_FIELD")
    success_value = os.environ.get("REF_SUCCESS_VALUE")
    if not success_field and "event" in sample:
        success_field = "event"
        success_value = "referral_settled"

    match_filters = [
        {referrer_field: {"$ne": None}},
        {"$expr": {"$lte": [f"${time_field}", cutoff_utc]}},
    ]
    if success_field and success_value is not None:
        match_filters.append({success_field: success_value})

    pipeline = [
        {"$match": {"$and": match_filters}},
        {"$group": {"_id": f"${referrer_field}", "referral_count": {"$sum": 1}}},
    ]
    results = list(collection.aggregate(pipeline, allowDiskUse=True))
    updates = []
    for row in results:
        uid = row.get("_id")
        if uid is None:
            continue
        updates.append(
            UpdateOne(
                {"user_id": uid},
                {"$set": {"referral_count": int(row.get("referral_count", 0)), "snapshot_updated_at": datetime.now(timezone.utc)}},
                upsert=False,
            )
        )

    ref_updates_planned = len(updates)
    ref_missing_users = 0
    ref_updates_applied = 0
    if updates:
        if dry_run:
            user_ids = [row.get("_id") for row in results if row.get("_id") is not None]
            ref_missing_users = _count_missing_users(db.users, user_ids)
        else:
            _ensure_write_enabled(dry_run)
            result = db.users.bulk_write(updates, ordered=False)
            ref_updates_applied = result.matched_count
            ref_missing_users = max(ref_updates_planned - result.matched_count, 0)

    logger.info(
        "[referral] collection=%s planned=%s applied=%s missing=%s",
        collection_name,
        ref_updates_planned,
        ref_updates_applied,
        ref_missing_users,
    )
    return {
        "ref_updates_planned": ref_updates_planned,
        "ref_updates_applied": ref_updates_applied,
        "ref_missing_users": ref_missing_users,
    }


def main() -> int:
    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        logger.error("MONGO_URL is required")
        return 1

    dry_run = os.environ.get("DRY_RUN", DEFAULT_DRY_RUN) != "0"
    cutoff_value = os.environ.get("CUTOFF_UTC")
    if not cutoff_value:
        logger.error("CUTOFF_UTC is required")
        return 1

    cutoff_utc = _parse_iso_datetime(cutoff_value)
    week_start_utc, month_start_utc, week_start_local, month_start_local = _compute_windows(cutoff_utc)

    logger.info("cutoff_utc=%s", cutoff_utc.isoformat())
    logger.info("week_start_kl=%s week_start_utc=%s", week_start_local.isoformat(), week_start_utc.isoformat())
    logger.info("month_start_kl=%s month_start_utc=%s", month_start_local.isoformat(), month_start_utc.isoformat())
    logger.info("dry_run=%s", dry_run)

    client = MongoClient(mongo_url)
    db = client["referral_bot"]

    candidates = [name for name in db.list_collection_names() if "referral" in name]
    if candidates:
        logger.info("referral collections detected: %s", ", ".join(sorted(candidates)))

    results = _aggregate_xp(db, cutoff_utc, week_start_utc, month_start_utc)
    users_scanned = len(results)
    weekly_users = sum(1 for row in results if int(row.get("weekly_xp", 0)) > 0)
    monthly_users = sum(1 for row in results if int(row.get("monthly_xp", 0)) > 0)

    top_weekly = _summarize_top(results, "weekly_xp")
    top_monthly = _summarize_top(results, "monthly_xp")

    logger.info("top_weekly_xp=%s", top_weekly)
    logger.info("top_monthly_xp=%s", top_monthly)

    now_utc = datetime.now(timezone.utc)
    updates = []
    for row in results:
        uid = row.get("_id")
        if uid is None:
            continue
        updates.append(
            UpdateOne(
                {"user_id": uid},
                {
                    "$set": {
                        "weekly_xp": int(row.get("weekly_xp", 0)),
                        "monthly_xp": int(row.get("monthly_xp", 0)),
                        "snapshot_updated_at": now_utc,
                    }
                },
                upsert=False,
            )
        )

    updates_planned = len(updates)
    updates_applied = 0
    missing_users = 0
    errors = 0

    if updates:
        try:
            if dry_run:
                user_ids = [row.get("_id") for row in results if row.get("_id") is not None]
                missing_users = _count_missing_users(db.users, user_ids)
            else:
                _ensure_write_enabled(dry_run)
                result = db.users.bulk_write(updates, ordered=False)
                updates_applied = result.matched_count
                missing_users = max(updates_planned - result.matched_count, 0)
        except Exception as exc:
            logger.exception("bulk_write failed")
            errors += 1
            return 1

    ref_summary = _restore_referrals(db, cutoff_utc, dry_run)

    summary = {
        "users_scanned": users_scanned,
        "weekly_users": weekly_users,
        "monthly_users": monthly_users,
        "updates_planned": updates_planned,
        "updates_applied": updates_applied,
        "missing_users": missing_users,
        "errors": errors,
    }
    summary.update(ref_summary)

    logger.info("summary=%s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
