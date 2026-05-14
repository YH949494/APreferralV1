import logging
from datetime import datetime, timedelta, time, timezone

from flask import jsonify, request

from config import (
    KL_TZ,
    XP_BASE_PER_CHECKIN,
    STREAK_MILESTONES,
    FIRST_CHECKIN_BONUS,
    STREAK_FREEZE_DEFAULT_TOKENS,
    STREAK_FREEZE_MAX_TOKENS,
)
from database import db, get_collection, init_db
from onboarding import record_first_checkin
from xp import grant_xp
from affiliate_rewards import record_user_last_seen

users_collection = get_collection("users")
logger = logging.getLogger(__name__)
_DATETIME_CLASS = datetime

# Timezone & XP come from a single source of truth (config.py)


def _to_aware_utc(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            parsed = _DATETIME_CLASS.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return None
        dt = parsed
    if not isinstance(dt, _DATETIME_CLASS):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KL_TZ).astimezone(timezone.utc)
    return dt.astimezone(timezone.utc)


def _kl_local_date(dt):
    aware_utc = _to_aware_utc(dt)
    if aware_utc is None:
        return None
    return aware_utc.astimezone(KL_TZ).date()


def _safe_freeze_tokens(value, default=0):
    try:
        tokens = int(value)
    except (TypeError, ValueError):
        tokens = default
    if tokens < 0:
        return 0
    if tokens > STREAK_FREEZE_MAX_TOKENS:
        return STREAK_FREEZE_MAX_TOKENS
    return tokens


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
    if user:
        tokens = _safe_freeze_tokens(user.get("streak_freeze_tokens"), default=0)
    else:
        tokens = _safe_freeze_tokens(STREAK_FREEZE_DEFAULT_TOKENS, default=STREAK_FREEZE_DEFAULT_TOKENS)
    tokens_before_freeze = tokens
    tokens_after_freeze = tokens
    freeze_used = False
    freeze_earned = False
    previous_streak = streak

    if last_checkin is not None:
        last_date = _kl_local_date(last_checkin)
        if last_date is None:
            logger.warning(
                "[CHECKIN][BAD_LAST_CHECKIN] uid=%s value_type=%s",
                user_id,
                type(last_checkin).__name__,
            )
            streak = 1
        elif last_date == today:
            # Already checked in today
            return jsonify({
                "success": False,
                "message": "⏳ You’ve already checked in today!",
                "next_checkin_time": next_midnight.isoformat()
            })
        else:
            gap_days = (today - last_date).days
            if gap_days == 1:
                streak += 1  # Continue streak
            elif gap_days == 2 and tokens > 0:
                tokens -= 1
                tokens_after_freeze = tokens
                streak += 1
                freeze_used = True
            else:
                streak = 1  # Missed a day → reset streak
    else:
        streak = 1  # First check-in ever

    if streak in STREAK_MILESTONES:
        new_tokens = min(STREAK_FREEZE_MAX_TOKENS, tokens + 1)
        freeze_earned = new_tokens > tokens
        tokens = new_tokens

    # Bonus XP from unified milestone table
    bonus_xp = STREAK_MILESTONES.get(streak, 0)

    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "last_checkin": now,
                "streak": streak,
                "streak_freeze_tokens": tokens,
            },
            "$setOnInsert": {
                "status": "Normal",
            }
        },
        upsert=True,
    )

    if freeze_used:
        freeze_event_key = f"streak_freeze:{user_id}:{today.strftime('%Y%m%d')}"
        db.streak_events.update_one(
            {"_id": freeze_event_key},
            {
                "$setOnInsert": {
                    "_id": freeze_event_key,
                    "user_id": user_id,
                    "type": "streak_freeze_used",
                    "previous_streak": previous_streak,
                    "new_streak": streak,
                    "tokens_before": tokens_before_freeze,
                    "tokens_after": tokens_after_freeze,
                    "last_checkin_date": last_date.isoformat(),
                    "checkin_date": today.isoformat(),
                    "created_at": now,
                }
            },
            upsert=True,
        )

    checkin_key = f"checkin:{today.strftime('%Y%m%d')}"
    total_xp = XP_BASE_PER_CHECKIN + bonus_xp
    grant_xp(db, user_id, "checkin", checkin_key, total_xp)

    record_user_last_seen(
        db,
        user_id=int(user_id),
        ip=request.headers.get("Fly-Client-IP") or request.remote_addr,
        subnet=request.headers.get("X-Forwarded-For"),
        session=request.headers.get("X-Session-Id") or request.cookies.get("session") or request.headers.get("User-Agent"),
    )

    if first_checkin:
        grant_xp(db, user_id, "first_checkin", "first_checkin", FIRST_CHECKIN_BONUS)

    record_first_checkin(user_id)
    
    bonus_text = f" 🎉 Streak Bonus: +{bonus_xp} XP!" if bonus_xp else ""
    first_bonus_text = (
        f" 🎁 First-time bonus: +{FIRST_CHECKIN_BONUS} XP" if first_checkin else ""
    )
    payload = {
        "success": True,
        "message": (
            f"✅ Check-in successful! +{XP_BASE_PER_CHECKIN} XP"
            f"{bonus_text}{first_bonus_text}"
        ),
        "next_checkin_time": next_midnight.isoformat(),
        "streak": streak,
        "streak_freeze_tokens": tokens,
        "streak_freeze_used": freeze_used,
        "streak_freeze_earned": freeze_earned,
    }
    if freeze_used:
        payload["message"] += " 🧊 Streak Freeze used! Your streak continues."
    return jsonify(payload)


def main():
    init_db()


if __name__ == "__main__":
    main()
