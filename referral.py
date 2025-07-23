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
                "referral_created_at": now
            })
            user = users_collection.find_one({"user_id": user_id})
        else:
            if username:
                users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"username": username}}
                )

        # Reuse existing link if less than 24 hours old
        referral_link = user.get("referral_link")
        created_at = user.get("referral_created_at")

        if referral_link and created_at:
            try:
                # If datetime already — no need to parse
                if isinstance(created_at, str):
                    created_at = datetime.datetime.fromisoformat(created_at)

                if (now - created_at).total_seconds() < 86400:
                    return referral_link
            except Exception as e:
                print(f"[Datetime Error] {e}")

        # Generate new invite link
        invite_link: ChatInviteLink = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            member_limit=0,
            creates_join_request=True,
            expire_date=now + datetime.timedelta(hours=24),
            name=f"ref-{user_id}"
        )

        # Save to DB
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

