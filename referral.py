from telegram import Bot
from datetime import datetime, timedelta
from database import users_collection

# Replace with your actual group ID
GROUP_CHAT_ID = -1002723991859

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

        return referral_link

    except Exception as e:
        print(f"❌ Failed to create invite link: {e}")
        return None

