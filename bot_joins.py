import os
from datetime import datetime, timedelta

from telegram.ext import ApplicationBuilder, ChatMemberHandler
from telegram.request import HTTPXRequest
from telegram import ChatMemberUpdated

from database import db, init_db

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WELCOME_DROP_ID = os.environ.get("WELCOME_DROP_ID")
ELIGIBILITY_TTL_HOURS = int(os.environ.get("WELCOME_ELIG_TTL_HOURS", "72"))

def save_join(cm: ChatMemberUpdated):
    user = cm.new_chat_member.user
    chat = cm.chat
    invite = getattr(cm, "invite_link", None)
    doc = {
        "event": "join",
        "user_id": user.id,
        "username": (user.username or "").lower(),
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_bot": user.is_bot,
        "chat_id": chat.id,
        "chat_title": getattr(chat, "title", None),
        "joined_at": datetime.utcnow(),
        "via_invite": invite.invite_link if invite else None,
        "invite_name": invite.name if invite else None,
    }
    db.joins.insert_one(doc)

    if WELCOME_DROP_ID:
        if not user.id:
            return        
        now = datetime.utcnow()
        db.welcome_eligibility.update_one(
            {"user_id": user.id, "dropId": WELCOME_DROP_ID},
            {
                "$setOnInsert": {
                    "user_id": user.id,
                    "dropId": WELCOME_DROP_ID,
                    "joined_at": now,
                    "expires_at": now + timedelta(hours=ELIGIBILITY_TTL_HOURS),
                    "consumed": False,
                }
            },
            upsert=True,
        )


def save_leave(cm: ChatMemberUpdated):
    user = cm.new_chat_member.user
    chat = cm.chat
    db.joins.insert_one(
        {
            "event": "leave",
            "user_id": user.id,
            "chat_id": chat.id,
            "at": datetime.utcnow(),
        }
    )


async def on_member(update: ChatMemberUpdated, _):
    old = update.old_chat_member.status
    new = update.new_chat_member.status
    if old in ("left", "kicked") and new in ("member", "administrator"):
        save_join(update)
    elif old in ("member", "administrator") and new in ("left", "kicked"):
        save_leave(update)


def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required")
    init_db()
    httpx_request = HTTPXRequest(
        connect_timeout=10,
        read_timeout=20,
        write_timeout=20,
        pool_timeout=10,
        connection_pool_size=8,
    )
    app = ApplicationBuilder().token(BOT_TOKEN).request(httpx_request).build()
    app.add_handler(ChatMemberHandler(on_member, ChatMemberHandler.CHAT_MEMBER))
    app.run_polling()


if __name__ == "__main__":
    main()
