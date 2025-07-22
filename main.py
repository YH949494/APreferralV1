from flask import Flask
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from pymongo import MongoClient
import os
import datetime

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
GROUP_ID = -1002723991859  # Replace with your actual group ID

client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id

    # Allow both group and private chats
    if user is None:
        return

    user_data = users_collection.find_one({"user_id": user.id})

    if not user_data:
        users_collection.insert_one({
            "user_id": user.id,
            "username": user.username,
            "referral_count": 0
        })

    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            member_limit=0,
            creates_join_request=False,
            expire_date=datetime.datetime.utcnow() + datetime.timedelta(hours=24),
            name=f"ref-{user.id}"
        )
    except Exception as e:
        await update.message.reply_text("❌ Failed to generate invite link. Make sure the bot is admin.")
        return

    keyboard = [
        [InlineKeyboardButton("👉 Join Group", url=invite_link.invite_link)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"👋 Welcome! Here is your referral link:\n\n{invite_link.invite_link}\n\nShare this with your friends. When they join, you’ll earn referral rewards!",
        reply_markup=reply_markup
    )

async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    invite_link = update.chat_join_request.invite_link

    if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
        referrer_id = int(invite_link.name.split("-")[1])
        users_collection.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referral_count": 1}}
        )
        await context.bot.approve_chat_join_request(update.chat_join_request.chat.id, update.chat_join_request.from_user.id)


if __name__ == '__main__':
    app_thread = Thread(target=lambda: app.run(host="0.0.0.0", port=8080))
    app_thread.start()

    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("referral", start))
    app_bot.add_handler(CommandHandler("link", start))
    app_bot.add_handler(CommandHandler("getlink", start))
    app_bot.add_handler(CommandHandler("invite", start))
    app_bot.add_handler(CommandHandler("ref", start))

    app_bot.add_handler(
        telegram.ext.ChatJoinRequestHandler(join_request_handler)
    )

    app_bot.run_polling()
