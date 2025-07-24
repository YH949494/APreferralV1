from telegram import Bot
from database import users_collection

async def get_or_create_referral_link(bot: Bot, user_id: int, username: str):
    invite_name = f"ref-{user_id}"

    # Check if user exists, else insert
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {"username": username},
            "$setOnInsert": {
                "xp": 0,
                "weekly_xp": 0,
                "referral_count": 0,
                "last_checkin": None
            }
        },
        upsert=True
    )

    # Reuse or create a new link
    links = await bot.get_chat_invite_links(chat_id=-1002723991859)
    for link in links:
        if link.name == invite_name:
            return link.invite_link

    new_link = await bot.create_chat_invite_link(
        chat_id=-1002723991859,
        name=invite_name,
        creates_join_request=True
    )
    return new_link.invite_link
