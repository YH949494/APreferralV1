from datetime import datetime, timedelta, time

from flask import jsonify, request

from config import KL_TZ, XP_BASE_PER_CHECKIN, STREAK_MILESTONES, FIRST_CHECKIN_BONUS
from database import db, get_collection, init_db
from onboarding import record_first_checkin
from xp import grant_xp

users_collection = get_collection("users")

# Timezone & XP come from a single source of truth (config.py)

def handle_checkin():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    now = datetime.now(KL_TZ)
    today = now.date()
    # next local midnight in KL
    next_midnight = datetime.combine(today + timedelta(days=1), time(0, 0, 0, tzinfo=KL_TZ))

    user = users_collection.find_one({"user_id": user_id})
    first_checkin = not user

    last_checkin = user.get("last_checkin") if user else None
    streak = user.get("streak", 0) if user else 0

    if last_checkin:
        last_date = last_checkin.astimezone(KL_TZ).date()

        if last_date == today:
            # Already checked in today
            return jsonify({
                "success": False,
                "message": "⏳ You’ve already checked in today!",
                "next_checkin_time": next_midnight.isoformat()
            })

        if (today - last_date) == timedelta(days=1):
            streak += 1  # Continue streak
        else:
            streak = 1  # Missed a day → reset streak
    else:
        streak = 1  # First check-in ever

    # Bonus XP from unified milestone table
    bonus_xp = STREAK_MILESTONES.get(streak, 0)

    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "last_checkin": now,
                "streak": streak
            },
            "$setOnInsert": {
                "status": "Normal",               
            }
        },
        upsert=True,
    )

    checkin_key = f"checkin:{today.strftime('%Y%m%d')}"
    total_xp = XP_BASE_PER_CHECKIN + bonus_xp
    grant_xp(db, user_id, "checkin", checkin_key, total_xp)

    if first_checkin:
        grant_xp(db, user_id, "first_checkin", "first_checkin", FIRST_CHECKIN_BONUS)

    record_first_checkin(user_id)
    
    bonus_text = f" 🎉 Streak Bonus: +{bonus_xp} XP!" if bonus_xp else ""
    first_bonus_text = (
        f" 🎁 First-time bonus: +{FIRST_CHECKIN_BONUS} XP" if first_checkin else ""
    )
    return jsonify({
        "success": True,
        "message": (
            f"✅ Check-in successful! +{XP_BASE_PER_CHECKIN} XP"
            f"{bonus_text}{first_bonus_text}"
        ),
        "next_checkin_time": next_midnight.isoformat()
    })


def main():
    init_db()


if __name__ == "__main__":
    main()
