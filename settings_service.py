"""Central, schema-driven runtime settings service.

All operational/runtime configuration (abuse protection, verification queue,
scheduler jobs, feature flags, Telegram config, welcome journey, public pool
probabilities, referral configuration, notification templates, and copy/URLs)
is stored in the ``app_settings`` Mongo collection, one document per group,
and is manageable from the Admin Dashboard without a redeploy.

True infrastructure secrets (BOT_TOKEN, MONGO_URL, FLASK_SECRET_KEY,
ADMIN_SECRET, ADMIN_PANEL_SECRET, API keys, webhook secrets, salts) are
intentionally NOT part of this service — they remain environment-only and are
only ever surfaced as a "configured: true/false" flag (see dashboard_panels).

Backward compatibility contract: every field falls back, in order, to
  1) a value stored in Mongo (admin-edited),
  2) the pre-existing environment variable (if any),
  3) the hardcoded default that shipped before this service existed.
So an untouched deployment behaves exactly as before.

Reads are cached in-process for SETTINGS_CACHE_TTL_SECONDS to avoid hitting
Mongo on every request; writes invalidate the cache for that group.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from database import db

logger = logging.getLogger(__name__)

COLLECTION_NAME = "app_settings"
SETTINGS_CACHE_TTL_SECONDS = 45


def _col(db_ref=None):
    return (db_ref if db_ref is not None else db)[COLLECTION_NAME]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _env_default(env_name: str | None, fallback):
    if not env_name:
        return fallback
    raw = os.getenv(env_name)
    if raw is None:
        return fallback
    return raw


def _coerce(value: Any, field_type: str, fallback: Any):
    try:
        if value is None:
            return fallback
        if field_type == "int":
            return int(value)
        if field_type == "float":
            return float(value)
        if field_type == "bool":
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("1", "true", "yes", "on")
        if field_type == "str":
            return str(value)
        if field_type in ("json", "list"):
            return value
        return value
    except (TypeError, ValueError):
        return fallback


# ---------------------------------------------------------------------------
# Settings UI categories: the canonical list of Admin Dashboard Settings tabs.
# Every group below carries a "category" (the tab it renders under). Groups
# whose fields span more than one tab (e.g. message_templates, urls) instead
# carry a "field_categories" override dict mapping field name -> category,
# taking priority over the group-level category for that field. This is the
# single source of truth the Settings UI filters against — a given field
# belongs to exactly one category.
# ---------------------------------------------------------------------------

SETTINGS_CATEGORIES: list[str] = [
    "general",
    "feature_flags",
    "xp",
    "rewards",
    "voucher_rules",
    "referral",
    "affiliate",
    "welcome_journey",
    "reactivation",
    "security",
    "integrations",
    "segment_probability",
]

# ---------------------------------------------------------------------------
# Schema: one entry per settings group. Each field carries its type, label,
# default, optional env var (for backward compatibility) and optional numeric
# bounds used for both server-side validation and the auto-generated admin UI.
# ---------------------------------------------------------------------------

SETTINGS_SCHEMA: dict[str, dict[str, Any]] = {
    "abuse_protection": {
        "label": "Abuse Protection",
        "description": "Cooldowns and kill-switch thresholds that protect claim/referral flows from abuse.",
        "category": "security",
        "fields": {
            "claim_cooldown_seconds": {"type": "int", "label": "Claim Cooldown (seconds)", "default": 180, "env": "CLAIM_COOLDOWN_SECONDS", "min": 0, "max": 86400},
            "session_cooldown_seconds": {"type": "int", "label": "Session Cooldown (seconds)", "default": 30, "env": "SESSION_COOLDOWN_SEC", "min": 0, "max": 86400},
            "ip_kill_window_seconds": {"type": "int", "label": "IP Kill Window (seconds)", "default": 600, "env": "IP_KILL_WINDOW_SECONDS", "min": 1, "max": 604800},
            "ip_kill_max_successes": {"type": "int", "label": "Max Successful Claims per IP", "default": 2, "env": "IP_KILL_MAX_SUCCESSES", "min": 1, "max": 100000},
            "subnet_kill_max_successes": {"type": "int", "label": "Max Successful Claims per Subnet", "default": 4, "env": "SUBNET_KILL_MAX_SUCCESSES", "min": 1, "max": 100000},
            "kill_block_seconds": {"type": "int", "label": "Kill Block Duration (seconds)", "default": 86400, "env": "KILL_BLOCK_SECONDS", "min": 0, "max": 2592000},
            "public_pool_ip_max_success": {"type": "int", "label": "Public Pool Max Successes per IP", "default": 3, "env": "PUBLIC_POOL_IP_MAX_SUCCESS", "min": 1, "max": 100000},
            "public_pool_subnet_max_success": {"type": "int", "label": "Public Pool Max Successes per Subnet", "default": 8, "env": "PUBLIC_POOL_SUBNET_MAX_SUCCESS", "min": 1, "max": 100000},
            "public_pool_ip_window_seconds": {"type": "int", "label": "Public Pool IP Window (seconds)", "default": 86400, "env": "PUBLIC_POOL_IP_WINDOW_SECONDS", "min": 1, "max": 2592000},
            "public_pool_ip_block_seconds": {"type": "int", "label": "Public Pool IP Block Duration (seconds)", "default": 86400, "env": "PUBLIC_POOL_IP_BLOCK_SECONDS", "min": 0, "max": 2592000},
            "public_pool_subnet_hard_block": {"type": "bool", "label": "Public Pool Subnet Hard Block", "default": False, "env": "PUBLIC_POOL_SUBNET_HARD_BLOCK"},
        },
    },
    "verification_queue": {
        "label": "Verification Queue",
        "description": "Retry/backoff behaviour for the Telegram membership verification queue sweep.",
        "category": "integrations",
        "fields": {
            "max_retry_attempts": {"type": "int", "label": "Max Retry Attempts", "default": 3, "env": "VERIFY_QUEUE_MAX_ATTEMPTS", "min": 1, "max": 50},
            "base_retry_delay_seconds": {"type": "int", "label": "Base Retry Delay (seconds)", "default": 30, "env": "VERIFY_QUEUE_BACKOFF_BASE_SECONDS", "min": 1, "max": 86400},
            "max_retry_delay_seconds": {"type": "int", "label": "Max Retry Delay (seconds)", "default": 300, "env": "VERIFY_QUEUE_BACKOFF_MAX_SECONDS", "min": 1, "max": 86400},
            "batch_size": {"type": "int", "label": "Verification Batch Size", "default": 50, "env": None, "min": 1, "max": 5000},
            "scheduler_enabled": {"type": "bool", "label": "Verification Scheduler Enabled", "default": True, "env": None},
        },
    },
    "scheduler": {
        "label": "Scheduler",
        "description": "Enable/disable and tune each APScheduler job without a redeploy.",
        "category": "general",
        "fields": {
            "xp_snapshot": {"type": "job", "label": "XP Snapshot", "default": {"enabled": True, "cron": "0 0 * * 1", "batch_size": None}},
            "referral_snapshot": {"type": "job", "label": "Referral Snapshot", "default": {"enabled": True, "cron": "0 0 1 * *", "batch_size": None}},
            "pending_referral_settlement": {"type": "job", "label": "Pending Referral Settlement", "default": {"enabled": True, "cron": "*/5 * * * *", "batch_size": None}},
            "verification_queue": {"type": "job", "label": "Verification Queue", "default": {"enabled": True, "cron": "*/2 * * * *", "batch_size": 50}},
            "welcome_reminder": {"type": "job", "label": "Welcome Reminder", "default": {"enabled": True, "cron": "0 * * * *", "batch_size": 200}},
            "reactivation_journey": {"type": "job", "label": "Reactivation Journey", "default": {"enabled": True, "cron": "*/30 * * * *", "batch_size": 200}},
            "affiliate_monthly_settlement": {"type": "job", "label": "Affiliate Monthly Settlement", "default": {"enabled": True, "cron": "10 0 1 * *", "batch_size": None}},
            "invite_subscription_audit": {"type": "job", "label": "Invite Subscription Audit", "default": {"enabled": True, "cron": None, "batch_size": None}},
            "bot_segment_sheet_sync": {"type": "job", "label": "Bot Segment Sheet Sync", "default": {"enabled": False, "cron": "30 9 * * 3", "batch_size": None}},
            "growth_leaderboard_weekly": {"type": "job", "label": "Growth Leaderboard Weekly", "default": {"enabled": False, "cron": "0 21 * * 0", "batch_size": None}},
            "telegram_member_counts_refresh": {"type": "job", "label": "Telegram Member Counts Refresh", "default": {"enabled": True, "cron": None, "batch_size": None, "interval_minutes": 60}},
            "autoscale_web_for_drop": {"type": "job", "label": "Autoscale Web For Drop", "default": {"enabled": True, "cron": None, "batch_size": None, "interval_seconds": 30}},
            "community_centre_tick": {"type": "job", "label": "Community Centre Tick", "default": {"enabled": True, "cron": None, "batch_size": None, "interval_seconds": 20}},
            "mission_pool_processor": {"type": "job", "label": "Mission Reward Pool Processor", "default": {"enabled": True, "cron": None, "batch_size": None, "interval_seconds": 120}},
        },
    },
    "feature_flags": {
        "label": "Feature Flags",
        "description": "Boolean switches for major product features. Off means the feature is fully disabled.",
        "category": "feature_flags",
        "fields": {
            "welcome_journey": {"type": "bool", "label": "Welcome Journey", "default": True, "env": None},
            "welcome_reward": {"type": "bool", "label": "Welcome Reward", "default": True, "env": None},
            "voucher_drops": {"type": "bool", "label": "Voucher Drops", "default": True, "env": None},
            "leaderboard": {"type": "bool", "label": "Leaderboard", "default": True, "env": None},
            "affiliate": {"type": "bool", "label": "Affiliate", "default": True, "env": None},
            "tournament": {"type": "bool", "label": "Tournament", "default": False, "env": None},
            "mission_pool": {"type": "bool", "label": "Mission Reward Pool", "default": False, "env": "MISSION_POOL_ENABLED"},
            "reactivation": {"type": "bool", "label": "Reactivation", "default": True, "env": None},
            "region_selection": {"type": "bool", "label": "Region Selection", "default": False, "env": None},
            "growth_leaderboard": {"type": "bool", "label": "Growth Leaderboard", "default": False, "env": "GROWTH_LEADERBOARD_ENABLED"},
            "admin_web_login": {"type": "bool", "label": "Admin Web Login", "default": True, "env": "ADMIN_WEB_LOGIN_ENABLED"},
            "community_post_approval_enabled": {"type": "bool", "label": "Community Post Approval Required", "default": False, "env": None},
            "community_post_self_approval_allowed": {"type": "bool", "label": "Community Post Self-Approval Allowed", "default": False, "env": None},
        },
    },
    "telegram_config": {
        "label": "Telegram Configuration",
        "description": "Non-secret Telegram configuration. BOT_TOKEN always stays in environment variables.",
        "category": "integrations",
        "fields": {
            "bot_username": {"type": "str", "label": "Bot Username", "default": "", "env": "BOT_USERNAME"},
            "official_channel_username": {"type": "str", "label": "Official Channel Username", "default": "advantplayofficial", "env": "OFFICIAL_CHANNEL_USERNAME"},
            "official_channel_id": {"type": "str", "label": "Official Channel ID", "default": "", "env": "OFFICIAL_CHANNEL_ID"},
            "main_group_id": {"type": "str", "label": "Main Group ID", "default": "-1002304653063", "env": "MAIN_GROUP_ID"},
            "community_chat_id": {"type": "str", "label": "#mywin Chat ID", "default": "0", "env": "MYWIN_CHAT_ID"},
            "miniapp_version": {"type": "str", "label": "MiniApp Version", "default": "", "env": "MINIAPP_VERSION"},
            "timezone": {"type": "str", "label": "Timezone", "default": "Asia/Kuala_Lumpur", "env": "SCHEDULER_CRON_TIMEZONE"},
        },
    },
    "welcome_journey": {
        "label": "Welcome Journey",
        "description": "Onboarding window, check-in requirements and reminder cadence for new users.",
        "category": "welcome_journey",
        "fields": {
            "checkin_days_required": {"type": "int", "label": "Check-in Days Required", "default": 3, "env": None, "min": 1, "max": 90},
            "welcome_window_hours": {"type": "int", "label": "Welcome Window (hours)", "default": 48, "env": "WELCOME_WINDOW_HOURS", "min": 1, "max": 8760},
            "reminder_after_hours": {"type": "int", "label": "Reminder Hours (after start)", "default": 12, "env": "WELCOME_REMINDER_AFTER_HOURS", "min": 0, "max": 8760},
            "final_reminder_hours": {"type": "int", "label": "Final Reminder Hours", "default": 36, "env": "WELCOME_FINAL_WARNING_HOURS", "min": 0, "max": 8760},
            "visible_days_after_claim": {"type": "int", "label": "Visible Days After Claim", "default": 3, "env": "WELCOME_CLAIMED_VISIBLE_DAYS", "min": 0, "max": 365},
            "unclaimed_window_days": {"type": "int", "label": "Unclaimed Window (days)", "default": 7, "env": "WELCOME_UNCLAIMED_WINDOW_DAYS", "min": 1, "max": 365},
            "reward_display_value": {"type": "str", "label": "Reward Display Value", "default": "", "env": None},
            "countdown_behaviour": {"type": "str", "label": "Countdown Behaviour", "default": "hide_when_expired", "env": None, "choices": ["hide_when_expired", "show_expired_badge", "freeze_at_zero"]},
        },
    },
    "pool_probabilities": {
        "label": "Public Pool Distribution",
        "description": "Segment probabilities (0-100%) for public voucher pool access, plus the reserved pool percentage.",
        "category": "segment_probability",
        "fields": {
            "new_user": {"type": "float", "label": "New User", "default": 70.0, "env": None, "min": 0, "max": 100},
            "new_joiner": {"type": "float", "label": "New Joiner", "default": 70.0, "env": None, "min": 0, "max": 100},
            "normal_player": {"type": "float", "label": "Normal Player", "default": 70.0, "env": None, "min": 0, "max": 100},
            "high_value": {"type": "float", "label": "High Value", "default": 50.0, "env": None, "min": 0, "max": 100},
            "ghost": {"type": "float", "label": "Ghost", "default": 5.0, "env": None, "min": 0, "max": 100},
            "low_value": {"type": "float", "label": "Low Value", "default": 10.0, "env": None, "min": 0, "max": 100},
            "voucher_hunter": {"type": "float", "label": "Voucher Hunter", "default": 10.0, "env": None, "min": 0, "max": 100},
            "abuse": {"type": "float", "label": "Abuse", "default": 5.0, "env": None, "min": 0, "max": 100},
            "unclassified": {"type": "float", "label": "Unclassified / Unknown", "default": 10.0, "env": None, "min": 0, "max": 100},
            "reserved_pool_pct": {"type": "float", "label": "Reserved Pool %", "default": 0.0, "env": None, "min": 0, "max": 100},
        },
    },
    "voucher_risk_modifiers": {
        "label": "Multi-Account Risk Modifiers",
        "description": (
            "Risk modifiers applied on top of the segment-derived (pool_probabilities) voucher "
            "probability when a Telegram identity carries multi_account_risk. Kept independent of "
            "canonical segment (for_bot_segment) -- these never change a user's segment, only the "
            "final probability used at claim time. See voucher_risk_eligibility.apply_risk_modifier, "
            "the single resolver used both by the live claim gate (vouchers.assign_public_pool_access_once) "
            "and by Databot's Management Dashboard / Player Operations for parity."
        ),
        "category": "segment_probability",
        "fields": {
            "multi_account_only_modifier_pct": {"type": "float", "label": "Multi-Account Risk Only Modifier (%)", "default": 25.0, "env": None, "min": 0, "max": 100},
            "behavioral_and_multi_account_modifier_pct": {"type": "float", "label": "Behavioral VH + Multi-Account Modifier (%)", "default": 100.0, "env": None, "min": 0, "max": 100},
            "behavioral_and_multi_account_min_pct": {"type": "float", "label": "Behavioral + Multi-Account Min Probability (%)", "default": 5.0, "env": None, "min": 0, "max": 100},
            "behavioral_and_multi_account_max_pct": {"type": "float", "label": "Behavioral + Multi-Account Max Probability (%)", "default": 10.0, "env": None, "min": 0, "max": 100},
        },
    },
    "referral_config": {
        "label": "Referral Configuration",
        "description": "XP rewards, holds and status classification for the referral program.",
        "category": "referral",
        "fields": {
            "xp_per_referral": {"type": "int", "label": "XP Per Referral", "default": 60, "env": None, "min": 0, "max": 1000000},
            "bonus_xp": {"type": "int", "label": "Bonus XP", "default": 400, "env": None, "min": 0, "max": 1000000},
            "bonus_interval": {"type": "int", "label": "Bonus Interval", "default": 3, "env": None, "min": 1, "max": 10000},
            "qualify_hold_hours": {"type": "int", "label": "Qualify Hold Hours", "default": 48, "env": "REFERRAL_QUALIFY_HOURS", "min": 0, "max": 8760},
            "near_miss_dm_enabled": {"type": "bool", "label": "Near Miss DM Enabled", "default": True, "env": None},
            "near_miss_dm_cooldown_hours": {"type": "int", "label": "Near Miss DM Cooldown (hours)", "default": 24, "env": None, "min": 1, "max": 8760},
            "reminder_timing_hours": {"type": "int", "label": "Reminder Timing (hours)", "default": 24, "env": None, "min": 0, "max": 8760},
            "pending_statuses": {"type": "list", "label": "Pending Statuses", "default": ["pending", "unverified"], "env": None},
            "qualified_statuses": {"type": "list", "label": "Qualified Statuses", "default": ["qualified", "verified", "rewarded"], "env": None},
            "revoked_statuses": {"type": "list", "label": "Revoked Statuses", "default": ["revoked", "rejected", "banned"], "env": None},
        },
    },
    "message_templates": {
        "label": "Notification Templates",
        "description": "Editable copy for outbound Telegram notifications.",
        # This group spans several product domains, so each field is
        # categorised individually rather than the group as a whole.
        "field_categories": {
            "welcome_success": "welcome_journey",
            "checkin_reminder": "welcome_journey",
            "day2_reminder": "welcome_journey",
            "day3_reminder": "welcome_journey",
            "voucher_claimed": "voucher_rules",
            "referral_near_miss": "referral",
            "affiliate_unlock": "affiliate",
            "reactivation_reminder": "reactivation",
        },
        "fields": {
            "welcome_success": {"type": "str", "label": "Welcome Success", "default": "🎉 Welcome! Your account is verified.", "env": None, "multiline": True},
            "checkin_reminder": {"type": "str", "label": "Check-in Reminder", "default": "🎁 Your AdvantPlay Welcome Voucher is waiting.\n\nFinish your check-ins to claim it before it expires.\n{link}", "env": None, "multiline": True},
            "day2_reminder": {"type": "str", "label": "Day 2 Reminder", "default": "👋 Don't forget to check in today to keep your streak alive!", "env": None, "multiline": True},
            "day3_reminder": {"type": "str", "label": "Day 3 Reminder", "default": "⏳ Last chance!\n\nYour AdvantPlay Welcome Voucher is about to expire — don't miss out.\n{link}", "env": None, "multiline": True},
            "voucher_claimed": {"type": "str", "label": "Voucher Claimed", "default": "✅ Your voucher has been claimed successfully!", "env": None, "multiline": True},
            "referral_near_miss": {"type": "str", "label": "Referral Near Miss", "default": "🔥 You're 1 referral away from your next bonus!", "env": None, "multiline": True},
            "affiliate_unlock": {"type": "str", "label": "Affiliate Unlock", "default": "🔥 You've reached 5 valid referrals this week.\nYou're invited to join our affiliate group and start earning now:\n{invite_url}", "env": "AFFILIATE_GROUP_INVITE_TEXT", "multiline": True},
            "reactivation_reminder": {"type": "str", "label": "Reactivation Reminder", "default": "🎁 Welcome back! Your Comeback Voucher is ready.", "env": None, "multiline": True},
        },
    },
    "requirements": {
        "label": "Requirements",
        "description": "Eligibility requirements gating claims and rewards.",
        "category": "welcome_journey",
        "fields": {
            "welcome_reward_checkins_required": {"type": "int", "label": "Welcome Reward Check-ins Required", "default": 3, "env": None, "min": 1, "max": 30},
        },
    },
    "share_content": {
        "label": "Referral Share Content",
        "description": "Fallback copy used by Referral Centre -> Share Content when no active caption hook exists.",
        "category": "referral",
        "fields": {
            "fallback_hook_text": {"type": "str", "label": "Fallback Hook Text", "default": "🎬 Fresh replays just dropped!", "env": None},
        },
    },
    "urls": {
        "label": "Copy / URLs",
        "description": "Editable links referenced by bot copy and the Mini App.",
        # Most links are general site/bot copy; the affiliate invite link
        # belongs to the Affiliate category instead.
        "category": "general",
        "field_categories": {
            "affiliate_group_invite_url": "affiliate",
        },
        "fields": {
            "official_channel_url": {"type": "str", "label": "Official Channel URL", "default": "", "env": "OFFICIAL_CHANNEL_URL"},
            "community_url": {"type": "str", "label": "Community URL", "default": "https://t.me/advantplaychat", "env": None},
            "miniapp_url": {"type": "str", "label": "MiniApp URL", "default": "https://apreferralv1.fly.dev/miniapp", "env": "WELCOME_REMINDER_LINK"},
            "tournament_url": {"type": "str", "label": "Tournament URL", "default": "", "env": None},
            "faq_url": {"type": "str", "label": "FAQ URL", "default": "https://t.me/advantplayofficial/714", "env": None},
            "support_url": {"type": "str", "label": "Support URL", "default": "", "env": None},
            "affiliate_group_invite_url": {"type": "str", "label": "Affiliate Group Invite URL", "default": "https://t.me/+2415x7eUHOcwNzE9", "env": "AFFILIATE_GROUP_INVITE_URL"},
        },
    },
}


def _default_for(field_def: dict) -> Any:
    if field_def["type"] == "job":
        return dict(field_def["default"])
    return _env_default(field_def.get("env"), field_def["default"])


def _group_defaults(group: str) -> dict:
    schema = SETTINGS_SCHEMA[group]
    out = {}
    for name, field_def in schema["fields"].items():
        raw = _default_for(field_def)
        if field_def["type"] == "job":
            out[name] = dict(raw)
        else:
            out[name] = _coerce(raw, field_def["type"], field_def["default"])
    return out


_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict]] = {}


def _merge_stored(group: str, stored: dict) -> dict:
    schema = SETTINGS_SCHEMA[group]
    merged = _group_defaults(group)
    for name, field_def in schema["fields"].items():
        if name not in stored:
            continue
        if field_def["type"] == "job":
            if isinstance(stored[name], dict):
                merged[name] = {**merged[name], **stored[name]}
        else:
            merged[name] = _coerce(stored[name], field_def["type"], merged[name])
    return merged


def get_settings(group: str, *, db_ref=None, force_refresh: bool = False) -> dict:
    """Return the current settings for a group, merged over defaults, cached."""
    if group not in SETTINGS_SCHEMA:
        raise KeyError(f"unknown settings group: {group}")

    if not force_refresh:
        with _cache_lock:
            cached = _cache.get(group)
        if cached and (time.monotonic() - cached[0]) < SETTINGS_CACHE_TTL_SECONDS:
            return dict(cached[1])

    try:
        stored = _col(db_ref).find_one({"_id": group}) or {}
    except Exception:
        logger.exception("[SETTINGS] failed to load group=%s, falling back to defaults", group)
        stored = {}

    merged = _merge_stored(group, stored)
    with _cache_lock:
        _cache[group] = (time.monotonic(), dict(merged))
    return merged


def get_setting(group: str, field: str, *, db_ref=None):
    """Convenience accessor for a single field within a group."""
    return get_settings(group, db_ref=db_ref).get(field)


def invalidate_cache(group: str | None = None) -> None:
    with _cache_lock:
        if group is None:
            _cache.clear()
        else:
            _cache.pop(group, None)


def _validate_field(group: str, name: str, field_def: dict, value: Any) -> tuple[Any, str | None]:
    ftype = field_def["type"]
    if ftype == "job":
        if not isinstance(value, dict):
            return None, f"bad_{name}"
        cleaned = {}
        if "enabled" in value:
            if not isinstance(value["enabled"], bool):
                return None, f"bad_{name}_enabled"
            cleaned["enabled"] = value["enabled"]
        if "cron" in value:
            cron = value["cron"]
            if cron is not None and not isinstance(cron, str):
                return None, f"bad_{name}_cron"
            cleaned["cron"] = cron
        if "batch_size" in value:
            batch = value["batch_size"]
            if batch is not None:
                try:
                    batch = int(batch)
                except (TypeError, ValueError):
                    return None, f"bad_{name}_batch_size"
                if batch < 1 or batch > 100000:
                    return None, f"bad_{name}_batch_size"
            cleaned["batch_size"] = batch
        for extra in ("interval_minutes", "interval_seconds"):
            if extra in value:
                try:
                    cleaned[extra] = int(value[extra]) if value[extra] is not None else None
                except (TypeError, ValueError):
                    return None, f"bad_{name}_{extra}"
        return cleaned, None

    if ftype == "bool":
        if not isinstance(value, bool):
            return None, f"bad_{name}"
        return value, None

    if ftype in ("int", "float"):
        try:
            num = int(value) if ftype == "int" else float(value)
        except (TypeError, ValueError):
            return None, f"bad_{name}"
        if isinstance(value, bool):
            return None, f"bad_{name}"
        lo, hi = field_def.get("min"), field_def.get("max")
        if lo is not None and num < lo:
            return None, f"bad_{name}_below_min"
        if hi is not None and num > hi:
            return None, f"bad_{name}_above_max"
        return num, None

    if ftype == "str":
        if not isinstance(value, str):
            return None, f"bad_{name}"
        choices = field_def.get("choices")
        if choices and value not in choices:
            return None, f"bad_{name}_choice"
        return value, None

    if ftype == "list":
        if isinstance(value, str):
            value = [v.strip() for v in value.split(",") if v.strip()]
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            return None, f"bad_{name}"
        return value, None

    return value, None


AUDIT_COLLECTION_NAME = "app_settings_audit"


def _write_audit_log(group: str, changed: dict, *, updated_by: str | None, db_ref=None) -> None:
    """Best-effort audit trail entry: who changed what, from what, to what, when.

    ``changed`` maps field name -> {"old": ..., "new": ...} for fields whose
    value actually changed. Never raises — audit logging must not block a save.
    """
    if not changed:
        return
    try:
        (db_ref if db_ref is not None else db)[AUDIT_COLLECTION_NAME].insert_one({
            "group": group,
            "admin": updated_by,
            "changes": changed,
            "created_at": now_utc(),
        })
    except Exception:
        logger.exception("[SETTINGS] failed to write audit log for group=%s", group)


def update_settings(group: str, updates: dict, *, updated_by: str | None = None, db_ref=None) -> dict:
    """Validate and persist a partial update for a settings group."""
    if group not in SETTINGS_SCHEMA:
        return {"success": False, "reason": "unknown_group"}

    schema = SETTINGS_SCHEMA[group]
    before = get_settings(group, db_ref=db_ref, force_refresh=True)
    changes = {}
    for name, value in (updates or {}).items():
        field_def = schema["fields"].get(name)
        if field_def is None:
            return {"success": False, "reason": f"unknown_field:{name}"}
        cleaned, err = _validate_field(group, name, field_def, value)
        if err:
            return {"success": False, "reason": err}
        changes[name] = cleaned

    if not changes:
        return {"success": True, "settings": get_settings(group, db_ref=db_ref)}

    changed_fields = {
        name: {"old": before.get(name), "new": value}
        for name, value in changes.items()
        if before.get(name) != value
    }

    doc_updates = {f"{k}": v for k, v in changes.items()}
    doc_updates["updated_at"] = now_utc()
    doc_updates["updated_by"] = updated_by
    _col(db_ref).update_one({"_id": group}, {"$set": doc_updates}, upsert=True)
    _write_audit_log(group, changed_fields, updated_by=updated_by, db_ref=db_ref)
    invalidate_cache(group)
    return {"success": True, "settings": get_settings(group, db_ref=db_ref, force_refresh=True)}


def field_category(group: str, field: str) -> str | None:
    """Resolve the Settings-UI category a given (group, field) belongs to.

    A per-field override in ``field_categories`` wins; otherwise the group's
    own ``category`` applies. Returns None if neither is set (i.e. the group
    was added without categorising it).
    """
    schema = SETTINGS_SCHEMA[group]
    overrides = schema.get("field_categories") or {}
    if field in overrides:
        return overrides[field]
    return schema.get("category")


def category_map() -> dict[str, str]:
    """Map every "group.field" key to its resolved Settings-UI category."""
    out: dict[str, str] = {}
    for group, schema in SETTINGS_SCHEMA.items():
        for field in schema["fields"]:
            out[f"{group}.{field}"] = field_category(group, field)
    return out


def list_schema() -> dict:
    """Return the full schema (labels/types/defaults/bounds) for the admin UI."""
    return SETTINGS_SCHEMA


def all_settings(*, db_ref=None) -> dict:
    return {group: get_settings(group, db_ref=db_ref) for group in SETTINGS_SCHEMA}
