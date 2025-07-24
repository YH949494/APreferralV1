from telegram import Bot
from pymongo import MongoClient
import os

# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

GROUP_ID = -1002723991859  # Replace with your actual group ID

async def get_or_create_referral_link(bot: Bot, user_id: int, username: str = "unknown") -> str:
    # Fallback if username is None or empty
    username = username or "unknown"

    user = users_collection.find_one({"user_id": user_id})

    if user and user.get("referral_link"):
        return user["referral_link"]

    # Create a new invite link named "ref-{user_id}"
    invite_link = await bot.create_chat_invite_link(
        chat_id=GROUP_ID,
        name=f"ref-{user_id}",
        creates_join_request=True,
        member_limit=0
    )
    
print("[Referral] New link created:", invite_link.invite_link)

    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "referral_link": invite_link.invite_link,
                "username": username
            },
            "$setOnInsert": {
                "xp": 0,
                "weekly_xp": 0,
                "referral_count": 0,
                "last_checkin": None
            }
        },
        upsert=True
    )

    return invite_link.invite_link

    # Save it to the database
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "referral_link": invite_link.invite_link,
                "username": username
            },
            "$setOnInsert": {
                "xp": 0,
                "weekly_xp": 0,
                "referral_count": 0,
                "last_checkin": None
            }
        },
        upsert=True
    )

    return invite_link.invite_link
