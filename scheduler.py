from datetime import datetime
from database import users_collection, history_collection

def archive_and_reset_weekly_leaderboard():
    now = datetime.utcnow()
    snapshot = []

    users = users_collection.find()
    for user in users:
        data = {
            "user_id": user["user_id"],
            "username": user.get("username", ""),
            "xp": user.get("XP", 0),
            "weekly_xp": user.get("PastWeekXP", 0),
            "weekly_referrals": user.get("weekly_referrals", 0),
            "timestamp": now
        }
        snapshot.append(data)

    if snapshot:
        history_collection.insert_many(snapshot)

    # Reset weekly counters
    users_collection.update_many({}, {
        "$set": {
            "PastWeekXP": 0,
            "weekly_referrals": 0
        }
    })

    print("✅ Weekly leaderboard archived and reset.")
