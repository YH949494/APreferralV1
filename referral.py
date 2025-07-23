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
GROUP_ID = -1002723991859  # Replace with your actual group ID

async def get_or_create_referral_link(bot: Bot, user_id: int, username: str = None) -> str:
    try:
        now = datetime.datetime.utcnow()

        # Find user record
        user = users_collection.find_one({"user_id": user_id})

        # Create user if not exists
        if not user:
            users_collection.insert_one({
                "user_id": user_id,
                "username": username,
                "xp": 0,
                "referral_count": 0,
                "last_checkin": None,
                "referral_link": None,
                "referral_created_at": None
            })
            user = users_collection.find_one({"user_id": user_id})
        else:
            # Update username if it’s available
            if username:
                users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"username": username}}
                )

        # Reuse existing link if it's less than 24 hours old
        if user.get("referral_link") and user.get("referral_created_at"):
            created_at = user["referral_created_at"]
            if (now - created_at).total_seconds() < 86400:
                return user["referral_link"]

        # Generate a new invite link
        invite_link: ChatInviteLink = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            member_limit=0,
            creates_join_request=True,
            expire_date=now + datetime.timedelta(hours=24),
            name=f"ref-{user_id}"
        )

        # Save new link to DB
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "referral_link": invite_link.invite_link,
                "referral_created_at": now
            }}
        )

        return invite_link.invite_link

    except Exception as e:
        print(f"[Referral Error] {e}")
        return ""
