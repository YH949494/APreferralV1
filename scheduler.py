from datetime import datetime, timezone
import logging
import pytz
from pymongo import UpdateOne
from database import db
from referral_rules import _referral_time_expr, _week_window_utc

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

logger = logging.getLogger(__name__)

def sync_weekly_counts_from_referrals():
    week_start_utc, week_end_utc = _week_window_utc()
    week_start_kl = week_start_utc.astimezone(KL_TZ)
    week_end_kl = week_end_utc.astimezone(KL_TZ)

    pipeline = [
        {
            "$match": {
                "$and": [
                    {"$or": [
                        {"status": {"$exists": False}},
                        {"status": {"$nin": ["pending", "inactive"]}},
                    ]},
                    {"$or": [
                        {"referrer_user_id": {"$exists": True, "$ne": None}},
                        {"referrer_id": {"$exists": True, "$ne": None}},
                    ]},
                    {"$or": [
                        {"invitee_user_id": {"$exists": True, "$ne": None}},
                        {"referred_user_id": {"$exists": True, "$ne": None}},
                    ]},
                ]
            }
        },
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": [_referral_time_expr(), week_start_utc]},
                        {"$lt": [_referral_time_expr(), week_end_utc]},
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {"$ifNull": ["$referrer_user_id", "$referrer_id"]},
                "count": {"$sum": 1},
            }
        },
    ]

    results = [row for row in db.referrals.aggregate(pipeline) if row.get("_id") is not None]
    update_ops = [
        UpdateOne(
            {"user_id": row["_id"]},
            {"$set": {"weekly_referral_count": int(row.get("count", 0))}},
            upsert=True,
        )
        for row in results
    ]

    updated = 0
    if update_ops:
        bulk_res = db.users.bulk_write(update_ops, ordered=False)
        updated = int(getattr(bulk_res, "modified_count", 0)) + int(getattr(bulk_res, "upserted_count", 0))

    top3 = sorted(
        [{"user_id": row["_id"], "count": int(row.get("count", 0))} for row in results],
        key=lambda x: x["count"],
        reverse=True,
    )[:3]
    logger.info(
        "[LEADERBOARD][SYNC] week_start=%s week_end=%s inviters=%s users_updated=%s top3=%s",
        week_start_kl.isoformat(),
        week_end_kl.isoformat(),
        len(results),
        updated,
        top3,
    )
    
def sweep_expired_drops():
    """
    Marks voucher drops as expired once their endsAt passes.
    Safe to run every minute. No side effects beyond status flip.
    """
    now = datetime.now(timezone.utc)
    # Use the same collection name as in vouchers.py ("drops" or "voucher_drops")
    db.drops.update_many(  # change to db.voucher_drops if you renamed it
        {"endsAt": {"$lte": now}, "status": {"$ne": "expired"}},
        {"$set": {"status": "expired"}}
    )

def archive_weekly_leaderboard():
    """
    Snapshots weekly leaderboards and resets weekly counters.
    Trigger this at Monday 00:00 KL (schedule in main.py).
    """
    sync_weekly_counts_from_referrals()    
    now_utc = datetime.now(timezone.utc)
    now_kl = now_utc.astimezone(KL_TZ)
    week_key = now_kl.strftime("%Y-%W")  # e.g., "2025-42"

    checkin_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_xp": 1}))
    referral_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_referral_count": 1}))

    snapshot = {
        "week": week_key,
        "timestampUtc": now_utc,
        "timestampKl": now_kl.isoformat(),
        "checkin": checkin_list,
        "referral": referral_list,
    }

    db.leaderboard_weekly.insert_one(snapshot)
    logger.info(
        "[LEADERBOARD] weekly_snapshot week=%s checkin_count=%s referral_count=%s",
        week_key,
        len(checkin_list),
        len(referral_list),
    )    
    db.users.update_many({}, {"$set": {"weekly_xp": 0, "weekly_referral_count": 0}})
