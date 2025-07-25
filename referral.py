from datetime import datetime, timedelta
from telegram import Bot
from pymongo import MongoClient
import os

# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

GROUP_ID = -1002723991859  # Replace with your actual group ID

async def get_or_create_referral_link(bot: Bot, user_id: int, username: str):
    user = users_collection.find_one({"user_id": user_id})
    
    now = datetime.utcnow()
    expire_cutoff = now - timedelta(hours=24)

    if user and "referral_link" in user and "referral_generated_at" in user:
        if user["referral_generated_at"] > expire_cutoff:
            return user["referral_link"]  # still valid

    # Delete old invite link if any
    if user and "referral_link" in user:
        try:
            await bot.revoke_chat_invite_link(chat_id="@YourChannelOrGroupUsername", invite_link=user["referral_link"])
        except:
            pass  # ignore if already revoked or invalid

    # Create new link
    link_name = f"ref-{user_id}"
    invite_link = await bot.create_chat_invite_link(
        chat_id="@YourChannelOrGroupUsername",  # Replace with your actual group/channel
        name=link_name,
        expire_date=int((now + timedelta(hours=24)).timestamp())
    )

    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "referral_link": invite_link.invite_link,
            "referral_generated_at": now,
            "username": username
        }},
        upsert=True
    )

    return invite_link.invite_link
