import logging
import os
import re
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

# Timezone
KL_TZ = ZoneInfo("Asia/Kuala_Lumpur")

MYWIN_CHAT_ID = int(os.getenv("MYWIN_CHAT_ID", "0"))  # 0 means "not configured"

BOT_SEGMENT_DEFAULT_PROBABILITY = 0.70
BOT_SEGMENT_PROBABILITY_MAP = {
    "new_user": 0.70,
    "new_joiner": 0.70,
    "potential": 0.50,
    "high_value": 0.50,
    "active_player": 0.30,
    "normal_actual": 0.70,
    "low_value": 0.10,
    "voucher_hunter": 0.10,
    "welcome_abuse": 0.05,
    "multi_account": 0.05,
    "unclassified": BOT_SEGMENT_DEFAULT_PROBABILITY,
}

_BOT_SEGMENT_ALIASES = {
    "new": "new_user",
    "newuser": "new_user",
    "new_users": "new_user",
    "new_player": "new_user",
    "new_players": "new_user",
    "new_joiners": "new_joiner",
    "joiner": "new_joiner",
    "potential_user": "potential",
    "potential_users": "potential",
    "highvalue": "high_value",
    "high_value_user": "high_value",
    "active": "active_player",
    "active_players": "active_player",
    "normal": "normal_actual",
    "normal_actuals": "normal_actual",
    "lowvalue": "low_value",
    "low_value_user": "low_value",
    "voucherhunter": "voucher_hunter",
    "voucher_hunters": "voucher_hunter",
    "welcome_abuser": "welcome_abuse",
    "welcome_abusers": "welcome_abuse",
    "multiaccount": "multi_account",
    "multi_accounts": "multi_account",
    "multiple_account": "multi_account",
    "multiple_accounts": "multi_account",
    "unclassed": "unclassified",
    "unknown": "unclassified",
    "na": "unclassified",
    "n_a": "unclassified",
    "none": "unclassified",
    "null": "unclassified",
}


def _canonicalize_for_bot_segment(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    value = re.sub(r"[\s\-/]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def normalize_for_bot_segment(raw: Any) -> str:
    canonical = _canonicalize_for_bot_segment(raw)
    if not canonical:
        return "unclassified"
    canonical = _BOT_SEGMENT_ALIASES.get(canonical, canonical)
    if canonical in BOT_SEGMENT_PROBABILITY_MAP:
        return canonical
    return "unclassified"


def public_pool_probability_for_bot_segment(raw: Any) -> float:
    normalized = normalize_for_bot_segment(raw)
    return float(BOT_SEGMENT_PROBABILITY_MAP.get(normalized, BOT_SEGMENT_DEFAULT_PROBABILITY))


def is_new_user_segment(raw_or_normalized: Any) -> bool:
    return normalize_for_bot_segment(raw_or_normalized) in {"new_user", "new_joiner"}


def is_blank_or_unknown_for_bot_segment(raw: Any) -> bool:
    canonical = _canonicalize_for_bot_segment(raw)
    if not canonical:
        return True
    canonical = _BOT_SEGMENT_ALIASES.get(canonical, canonical)
    return canonical not in BOT_SEGMENT_PROBABILITY_MAP or canonical == "unclassified"


# Check-in XP settings (single source of truth)
XP_BASE_PER_CHECKIN = 20
FIRST_CHECKIN_BONUS = 200
STREAK_MILESTONES = {
    7: 50,
    14: 150,
    28: 300,
    56: 600,
    84: 900,
    112: 1200,    
}
STREAK_FREEZE_DEFAULT_TOKENS = 1
STREAK_FREEZE_MAX_TOKENS = 3

# Announcement milestone buckets
WEEKLY_XP_BUCKET = 1000
WEEKLY_REFERRAL_BUCKET = 10

# Bump MINIAPP_VERSION each deploy to bust Telegram Desktop cache.
BOOT_ID = uuid4().hex[:12]
_override = os.getenv("MINIAPP_VERSION")
_fly_image_ref = os.getenv("FLY_IMAGE_REF")
_fly_machine_version = os.getenv("FLY_MACHINE_VERSION")
_derived = None
if _fly_image_ref:
    _derived = _fly_image_ref.split("@")[-1].replace("sha256:", "")
elif _fly_machine_version:
    _derived = _fly_machine_version
if _derived:
    _derived = _derived[:12]

_source = "boot"
if _override:
    MINIAPP_VERSION = _override
    _source = "override"
elif _derived:
    MINIAPP_VERSION = _derived
    _source = "derived"
elif os.getenv("FLASK_ENV") == "development":
    MINIAPP_VERSION = "dev"
    _source = "dev"
else:
    MINIAPP_VERSION = BOOT_ID

logging.getLogger(__name__).info(
    "[MINIAPP_VERSION] resolved=%s source=%s", MINIAPP_VERSION, _source
)

AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS = int(
    os.getenv("AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS", "5")
)
AFFILIATE_GROUP_INVITE_TEXT = os.getenv(
    "AFFILIATE_GROUP_INVITE_TEXT",
    "🔥 You’ve reached 5 valid referrals this week.\nYou’re invited to join our affiliate group and start earning now:\n{invite_url}",
)
AFFILIATE_GROUP_UNLOCK_REFERRALS = int(os.getenv("AFFILIATE_GROUP_UNLOCK_REFERRALS", "5"))
AFFILIATE_GROUP_INVITE_URL = os.getenv("AFFILIATE_GROUP_INVITE_URL", "https://t.me/+2415x7eUHOcwNzE9")
AFFILIATE_GROUP_DM_ENABLED = os.getenv("AFFILIATE_GROUP_DM_ENABLED", "1") == "1"

GROWTH_LEADERBOARD_ENABLED = os.getenv("GROWTH_LEADERBOARD_ENABLED", "0") == "1"
GROWTH_LEADERBOARD_CHANNEL_ID = os.getenv("GROWTH_LEADERBOARD_CHANNEL_ID", "").strip()
GROWTH_LEADERBOARD_CRON_DAY = os.getenv("GROWTH_LEADERBOARD_CRON_DAY", "SUN")
GROWTH_LEADERBOARD_CRON_HOUR = int(os.getenv("GROWTH_LEADERBOARD_CRON_HOUR", "21"))
GROWTH_LEADERBOARD_CRON_MINUTE = int(os.getenv("GROWTH_LEADERBOARD_CRON_MINUTE", "0"))
GROWTH_LEADERBOARD_TIMEZONE = os.getenv("GROWTH_LEADERBOARD_TIMEZONE", "Asia/Kuala_Lumpur")
