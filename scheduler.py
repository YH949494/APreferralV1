from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

def archive_weekly_leaderboard():
    now = datetime.utcnow()
    week_key = now.strftime("%Y-%W")

    snapshot = {
        "week": week_key,
        "checkin": list(users_collection.find({}, {"username": 1, "weekly_xp": 1, "_id": 0})),
        "referral": list(users_collection.find({}, {"username": 1, "referral_count": 1, "_id": 0})),
        "timestamp": now
    }

    leaderboard_collection.insert_one(snapshot)

    # Reset weekly values
    users_collection.update_many({}, {"$set": {"weekly_xp": 0, "referral_count": 0}})
    print(f"Leaderboard archived for {week_key}")

scheduler = BackgroundScheduler()
scheduler.add_job(archive_weekly_leaderboard, 'cron', day_of_week='mon', hour=0)
scheduler.start()
