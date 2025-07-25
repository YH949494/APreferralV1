from datetime import datetime, timedelta
from telegram import Bot
from pymongo import MongoClient
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB setup
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# Your Telegram group ID (make sure the bot is admin here)
GROUP_ID = -1002723991859

async def get_or_create_referral_link(bot: Bot, user_id: int, username: str) -> str:
    try:
        logger.info(f"Fetching referral link for user_id={user_id}, username={username}")
        user = users_collection.find_one({"user_id": user_id})

        now = datetime.utcnow()
        expire_cutoff = now - timedelta(hours=24)

        # Use existing valid link if still fresh
        if user and "referral_link" in user and "referral_generated_at" in user:
            if user["referral_generated_at"] > expire_cutoff:
                logger.info("Reusing existing referral link")
                return user["referral_link"]

        # Revoke old link if exists
        if user and "referral_link" in user:
            try:
                logger.info("Revoking old referral link")
                await bot.revoke_chat_invite_link(chat_id=GROUP_ID, invite_link=user["referral_link"])
            except Exception as e:
                logger.warning(f"Failed to revoke old invite link: {e}")

        # Create new referral link
        logger.info("Creating new referral link...")
        link_name = f"ref-{user_id}"
        invite_link = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=link_name,
            expire_date=int((now + timedelta(hours=24)).timestamp())
        )

        # Save to DB
        users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "referral_link": invite_link.invite_link,
                "referral_generated_at": now,
                "username": username
            }},
            upsert=True
        )

        logger.info(f"Referral link created: {invite_link.invite_link}")
        return invite_link.invite_link

    except Exception as e:
        logger.error(f"[get_or_create_referral_link] Error for user {user_id}: {e}")
        raise
