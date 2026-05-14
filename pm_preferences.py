from __future__ import annotations

from typing import Any

PM_PREFERENCE_DEFAULTS = {
    "referral_updates": True,
    "checkin_reminders": True,
    "streak_alerts": True,
    "winback": True,
    "announcements": True,
}


def merged_pm_preferences(raw: dict[str, Any] | None) -> dict[str, bool]:
    merged = dict(PM_PREFERENCE_DEFAULTS)
    if not isinstance(raw, dict):
        return merged
    for key in PM_PREFERENCE_DEFAULTS:
        value = raw.get(key)
        if value is True:
            merged[key] = True
        elif value is False:
            merged[key] = False
    return merged


def pm_allowed(user_id: int | None, preference_key: str, default: bool = True, *, users_collection=None, logger=None) -> bool:
    if not user_id:
        return default
    if users_collection is None:
        return default
    try:
        user_doc = users_collection.find_one({"user_id": int(user_id)}, {"pm_preferences": 1})
    except Exception as exc:
        if logger is not None:
            logger.warning("[PM_PREF][ERROR] uid=%s key=%s err=%s", user_id, preference_key, exc)
        return default
    if not user_doc:
        return default
    prefs = user_doc.get("pm_preferences")
    if isinstance(prefs, dict) and prefs.get(preference_key) is False:
        return False
    return True
