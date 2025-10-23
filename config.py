from zoneinfo import ZoneInfo

# Timezone
KL_TZ = ZoneInfo("Asia/Kuala_Lumpur")

# Admin / Bot secrets
ADMIN_PANEL_SECRET = os.getenv("ADMIN_PANEL_SECRET", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
# Optional: comma-separated fallback tokens if you rotated
BOT_TOKEN_FALLBACKS = [t.strip() for t in os.getenv("BOT_TOKEN_FALLBACKS", "").split(",") if t.strip()]

# Convenience helper
def admin_secret_ok(value: str) -> bool:
    if not value or not ADMIN_PANEL_SECRET:
        return False
    return value.strip() == ADMIN_PANEL_SECRET.strip()

# Check-in XP settings (single source of truth)
XP_BASE_PER_CHECKIN = 20
STREAK_MILESTONES = {
    7: 50,
    14: 150,
    28: 300,
}

# Announcement milestone buckets
WEEKLY_XP_BUCKET = 1000
WEEKLY_REFERRAL_BUCKET = 10
