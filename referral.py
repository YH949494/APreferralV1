from datetime import datetime, timedelta
from telegram import Bot, Update
from telegram.ext import ContextTypes
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

# Your Telegram group ID
GROUP_ID = int(os.environ.get("GROUP_ID", "-1002304653063"))  # fallback to your current hardcoded ID


### ✅ 1. Generate or reuse referral link
async def get_or_create_referral_link(bot: Bot, user_id: int, username: str) -> str:
    try:
        logger.info(f"Fetching referral link for user_id={user_id}, username={username}")
        user = users_collection.find_one({"user_id": user_id})

        now = datetime.utcnow()
        expire_cutoff = now - timedelta(hours=24)

        # Reuse if not expired
        if user and "referral_link" in user and "referral_generated_at" in user:
            if user["referral_generated_at"] > expire_cutoff:
                logger.info("✅ Reusing existing referral link")
                return user["referral_link"]

        # Create new link
        logger.info("🔄 Creating new referral link...")
        link_name = f"ref-{user_id}"
        expire_time = now + timedelta(hours=24)

        invite_link = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=link_name,
            expire_date=int(expire_time.timestamp()),
            creates_join_request=True,
            member_limit=None
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

        logger.info(f"✅ Referral link created: {invite_link.invite_link}")
        return invite_link.invite_link

    except Exception as e:
        logger.error(f"[get_or_create_referral_link] Error for user {user_id}: {e}")
        raise


### ✅ 2. Handle new user joins (called by main.py)
def is_recent_leave(user_id):
    record = users_collection.find_one({"user_id": user_id})
    if record and "left_at" in record:
        left_time = record["left_at"]
        if datetime.utcnow() - left_time < timedelta(hours=12):
            return True
    return False

async def handle_new_join(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    for new_member in update.message.new_chat_members:
        referred_by = context.user_data.get("referred_by")
        user_id = new_member.id
        username = new_member.username or ""

        if referred_by and referred_by != user_id:
            if is_recent_leave(user_id):
                logger.info(f"❌ Referral blocked for abuse: user {user_id}")
            else:
                users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "username": username,
                        "referred_by": referred_by,
                        "joined_at": datetime.utcnow()
                    }},
                    upsert=True
                )

                # Bonus XP to the referrer
                users_collection.update_one(
                    {"user_id": referred_by},
                    {"$inc": {"xp": 50}}
                )

                logger.info(f"🎉 {user_id} joined via referral from {referred_by}")
