from datetime import datetime, timedelta
from flask import request, jsonify
from database import users_collection

CHECKIN_EXP = 20
COOLDOWN_HOURS = 24

def handle_checkin():
    try:
        user_id = int(request.args.get("user_id"))
        username = request.args.get("username", "")
        now = datetime.utcnow()

        user = users_collection.find_one({"user_id": user_id})

        if not user:
            users_collection.insert_one({
                "user_id": user_id,
                "username": username,
                "xp": CHECKIN_EXP,
                "weekly_xp": CHECKIN_EXP,
                "last_checkin": now.isoformat(),
                "referral_count": 0
            })
            return jsonify({
                "success": True,
                "message": f"🎉 Check-in successful! +{CHECKIN_EXP} XP",
                "can_checkin": False,
                "next_checkin_time": (now + timedelta(hours=COOLDOWN_HOURS)).isoformat()
            })

        last_checkin_str = user.get("last_checkin")
        if last_checkin_str:
            last_checkin = datetime.fromisoformat(last_checkin_str)
            elapsed = (now - last_checkin).total_seconds()
            if elapsed < COOLDOWN_HOURS * 3600:
                remaining = last_checkin + timedelta(hours=COOLDOWN_HOURS)
                return jsonify({
                    "success": False,
                    "message": "✅ You’ve already checked in. Come back later!",
                    "can_checkin": False,
                    "next_checkin_time": remaining.isoformat()
                })

        users_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "last_checkin": now.isoformat(),
                    "username": username
                },
                "$inc": {
                    "xp": CHECKIN_EXP,
                    "weekly_xp": CHECKIN_EXP
                }
            }
        )
        return jsonify({
            "success": True,
            "message": f"🎉 Check-in successful! +{CHECKIN_EXP} XP",
            "can_checkin": False,
            "next_checkin_time": (now + timedelta(hours=COOLDOWN_HOURS)).isoformat()
        })

    except Exception as e:
        print(f"[Check-in Error] {e}")
        return jsonify({"success": False, "message": "❌ Server error"})
