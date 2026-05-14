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
    return True
