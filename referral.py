from telegram import Bot, ChatInviteLink
from pymongo import MongoClient
import os
import datetime

# Mongo setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# Group ID
GROUP_ID = -1002723991859  # Replace with your actual group ID if needed

async def get_or_create_referral_link(bot: Bot, user_id: int, source: str = "default") -> str:
    try:
        # Check if user exists, if not create
        user = users_collection.find_one({"user_id": user_id})
        if not user:
            users_collection.insert_one({
                "user_id": user_id,
                "username": None,
                "xp": 0,
                "referral_count": 0,
                "last_checkin": None
            })

        # Generate a Telegram invite link with 24-hour expiry
        invite_link: ChatInviteLink = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            member_limit=0,
            creates_join_request=True,
            expire_date=datetime.datetime.utcnow() + datetime.timedelta(hours=24),
            name=f"ref-{user_id}"
        )
        return invite_link.invite_link

    except Exception as e:
        print(f"[Referral Error] {e}")
        return ""
