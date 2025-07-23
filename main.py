from flask import Flask
from threading import Thread
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ChatJoinRequestHandler
)
from referral import referral_handler, join_request_handler
from checkin import checkin_handler
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

if __name__ == "__main__":
    # Start Flask on separate thread for uptime monitoring
    flask_thread = Thread(target=lambda: app.run(host="0.0.0.0", port=8080))
    flask_thread.start()

    # Start Telegram bot
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    # Command handlers
    app_bot.add_handler(CommandHandler("start", referral_handler))  # For referral link
    app_bot.add_handler(CommandHandler("ref", referral_handler))    # Alias
    app_bot.add_handler(CommandHandler("checkin", checkin_handler)) # Check-in logic

    # Chat join approval (for referrals)
    app_bot.add_handler(ChatJoinRequestHandler(join_request_handler))

    # Run bot
    app_bot.run_polling()
