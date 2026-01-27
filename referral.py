from datetime import datetime, timedelta
import logging

from telegram import Bot
from pymongo.errors import DuplicateKeyError

from database import db, get_collection

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

users_collection = get_collection("users")
invite_link_map_collection = db["invite_link_map"]

# Your Telegram group ID (make sure the bot is admin here)
GROUP_ID = -1002304653063

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
        try:
            invite_link_map_collection.insert_one(
                {
                    "inviter_id": user_id,
                    "inviter_uid": user_id,
                    "chat_id": GROUP_ID,
                    "invite_link": invite_link.invite_link,
                    "is_active": True,
                    "created_at": now,
                }
            )
            logger.info(
                "[REFERRAL][LINK_CREATE_OK] inviter=%s chat_id=%s invite_link=%s db=ok",
                user_id,
                GROUP_ID,
                invite_link.invite_link,
            )
        except DuplicateKeyError:
            logger.info(
                "[REFERRAL][LINK_CREATE_OK] inviter=%s chat_id=%s invite_link=%s db=duplicate",
                user_id,
                GROUP_ID,
                invite_link.invite_link,
            )
        except Exception as e:
            logger.exception(
                "[REFERRAL][LINK_CREATE_DB_FAIL] inviter=%s chat_id=%s invite_link=%s err=%s",
                user_id,
                GROUP_ID,
                invite_link.invite_link,
                e,
            )
        
        logger.info(f"✅ Referral link created: {invite_link.invite_link}")
        return invite_link.invite_link

    except Exception as e:
        logger.error(f"[get_or_create_referral_link] Error for user {user_id}: {e}")
        raise
