from telegram import Bot
from datetime import datetime, timedelta
from database import db, users_collection
from pymongo.errors import DuplicateKeyError

# Replace with your actual group ID
GROUP_CHAT_ID = -1002304653063
invite_link_map_collection = db["invite_link_map"]

async def get_or_create_referral_link(bot: Bot, user_id: int, username: str):
    user_data = users_collection.find_one({"user_id": user_id})

    # If the user already has a referral link, return it
    if user_data and "referral_link" in user_data:
        return user_data["referral_link"]

    try:
        # Create a new invite link that expires in 24 hours
        invite_link_obj = await bot.create_chat_invite_link(
            chat_id=GROUP_CHAT_ID,
            member_limit=0,
            expire_date=datetime.utcnow() + timedelta(days=1),
            creates_join_request=False,
            name=f"Referral from {username or user_id}"
        )

        referral_link = invite_link_obj.invite_link

        # Save the new referral link to the database
        users_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "referral_link": referral_link
                }
            },
            upsert=True
        )
        try:
            invite_link_map_collection.insert_one(
                {
                    "inviter_id": user_id,
                    "inviter_uid": user_id,
                    "chat_id": GROUP_CHAT_ID,
                    "invite_link": referral_link,
                    "is_active": True,
                    "created_at": datetime.utcnow(),
                }
            )
            print(
                "[REFERRAL][LINK_CREATE_OK] inviter=%s chat_id=%s invite_link=%s db=ok"
                % (user_id, GROUP_CHAT_ID, referral_link)
            )
        except DuplicateKeyError:
            print(
                "[REFERRAL][LINK_CREATE_OK] inviter=%s chat_id=%s invite_link=%s db=duplicate"
                % (user_id, GROUP_CHAT_ID, referral_link)
            )
        except Exception as e:
            print(
                "[REFERRAL][LINK_CREATE_DB_FAIL] inviter=%s chat_id=%s invite_link=%s err=%s"
                % (user_id, GROUP_CHAT_ID, referral_link, e)
            )
        
        return referral_link

    except Exception as e:
        print(f"❌ Failed to create invite link: {e}")
        return None
