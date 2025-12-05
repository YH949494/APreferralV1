from datetime import datetime, timezone
import pytz
from database import db

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

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
    db.users.update_many({}, {"$set": {"weekly_xp": 0, "weekly_referral_count": 0}})
