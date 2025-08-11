# One-time backfill for weekly_xp and monthly_xp
from database import users_collection  # adjust import if needed

result = users_collection.update_many(
    {},
    {"$set": {"weekly_xp": 0, "monthly_xp": 0}}
)

print(f"✅ Updated {result.modified_count} users with weekly_xp and monthly_xp fields")
