import logging
import os
from uuid import uuid4
from zoneinfo import ZoneInfo
from config import MYWIN_CHAT_ID

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
logging.getLogger(__name__).info("[CHAT_IDS] MYWIN_CHAT_ID=%s", MYWIN_CHAT_ID)
if MYWIN_CHAT_ID == 0:
    logging.getLogger(__name__).warning("[CHAT_IDS] MYWIN_CHAT_ID is not set; #mywin posting disabled")

