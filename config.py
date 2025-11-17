from zoneinfo import ZoneInfo

# Timezone
KL_TZ = ZoneInfo("Asia/Kuala_Lumpur")

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
