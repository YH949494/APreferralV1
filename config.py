import logging
import os
from uuid import uuid4
from zoneinfo import ZoneInfo

# Timezone
KL_TZ = ZoneInfo("Asia/Kuala_Lumpur")

MYWIN_CHAT_ID = int(os.getenv("MYWIN_CHAT_ID", "0"))  # 0 means "not configured"

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
