import logging
import os
import re
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

# Timezone
KL_TZ = ZoneInfo("Asia/Kuala_Lumpur")

MYWIN_CHAT_ID = int(os.getenv("MYWIN_CHAT_ID", "0"))  # 0 means "not configured"

# ---------------------------------------------------------------------------
# Backend segment probability configuration (single source of truth).
# Probabilities are integers (0–100). Applied when backend_segment is available
# on a user doc. New-player override (100%) is enforced separately in
# assign_public_pool_access_once when player_age_type == "new_player" and the
# user is within their eligible new-player window (first 3 assignments).
# ---------------------------------------------------------------------------
SEGMENT_PROBABILITY_CONFIG: dict[str, int] = {
    "high_value": 50,
    "normal_actual": 30,
    "active_community_player": 20,
    "low_value": 10,
    "voucher_hunter": 10,
    "ghost": 5,
    "unclassified": 10,
}

SEGMENT_PROBABILITY_DESCRIPTIONS: dict[str, str] = {
    "high_value": "High-value spenders — highest priority after new players",
    "normal_actual": "Primary active player segment",
    "active_community_player": "Community-engaged players (check-in / XP activity)",
    "low_value": "Low engagement — withdrew but low bet ratio",
    "voucher_hunter": "Claim-focused users with low play conversion",
    "ghost": "Inactive users — no bets, withdrawals, referrals or check-ins",
    "unclassified": "Segment data unavailable or not yet classified",
}

BOT_SEGMENT_DEFAULT_PROBABILITY = 0.70
BOT_SEGMENT_PROBABILITY_MAP = {
    "new_user": 0.70,
    "new_joiner": 0.70,
    "potential": 0.50,
    "high_value": 0.50,
    "active_player": 0.30,
    "active_community_player": 0.20,
    "normal_actual": 0.70,
    "low_value": 0.10,
    "voucher_hunter": 0.10,
    "welcome_abuse": 0.05,
    "multi_account": 0.05,
    # Ghost: no after-campaign play/withdrawal signal — reactivation-only,
    # conservative default so public-pool exposure stays low until UIM
    # reports renewed activity.
    "ghost": 0.05,
    # Old Player: UIM hasn't defined a different rule for this bucket yet,
    # so it stays at the same default as the other "normal" segments.
    "old_player": BOT_SEGMENT_DEFAULT_PROBABILITY,
    "unclassified": BOT_SEGMENT_DEFAULT_PROBABILITY,
}


def backend_segment_probability(segment: str) -> float:
    """Return probability (0.0–1.0) for a backend segment using SEGMENT_PROBABILITY_CONFIG."""
    return SEGMENT_PROBABILITY_CONFIG.get(segment, SEGMENT_PROBABILITY_CONFIG["unclassified"]) / 100.0

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
    "ghosts": "ghost",
    "ghost_player": "ghost",
    "ghost_players": "ghost",
    "ghost_user": "ghost",
    "ghost_users": "ghost",
    "oldplayer": "old_player",
    "old_players": "old_player",
    "old_user": "old_player",
    "old_users": "old_player",
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

# ---------------------------------------------------------------------------
# Databot integration (shadow / read-only — Phase 1).
# All Databot calls are non-blocking; APReferral falls back to local logic
# whenever Databot is disabled or unreachable.
# ---------------------------------------------------------------------------
DATABOT_BASE_URL = os.getenv("DATABOT_BASE_URL", "").rstrip("/")
DATABOT_API_KEY = os.getenv("DATABOT_API_KEY", "")
DATABOT_ENABLED = os.getenv("DATABOT_ENABLED", "false").lower() == "true"
def _parse_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except (ValueError, TypeError):
        logging.getLogger(__name__).warning(
            "[DATABOT] invalid env %s — using default %d", name, default
        )
        return default

DATABOT_TIMEOUT_SECONDS = _parse_int_env("DATABOT_TIMEOUT_SECONDS", 5)

GROWTH_LEADERBOARD_ENABLED = os.getenv("GROWTH_LEADERBOARD_ENABLED", "0") == "1"
GROWTH_LEADERBOARD_CHANNEL_ID = os.getenv("GROWTH_LEADERBOARD_CHANNEL_ID", "").strip()
GROWTH_LEADERBOARD_CRON_DAY = os.getenv("GROWTH_LEADERBOARD_CRON_DAY", "SUN")
GROWTH_LEADERBOARD_CRON_HOUR = int(os.getenv("GROWTH_LEADERBOARD_CRON_HOUR", "21"))
GROWTH_LEADERBOARD_CRON_MINUTE = int(os.getenv("GROWTH_LEADERBOARD_CRON_MINUTE", "0"))
GROWTH_LEADERBOARD_TIMEZONE = os.getenv("GROWTH_LEADERBOARD_TIMEZONE", "Asia/Kuala_Lumpur")
