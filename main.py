from telegram.ext import ApplicationBuilder
from checkin import register_checkin_handlers
from referral import register_referral_handlers
from database import init_db
from flask import Flask
from threading import Thread
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Flask app for Fly.io health check / Mini App (optional)
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

if __name__ == "__main__":
    # Step 1: Start Flask in a separate thread
    flask_thread = Thread(target=lambda: app.run(host="0.0.0.0", port=8080))
    flask_thread.start()

    # Step 2: Initialize MongoDB connection
    init_db()

    # Step 3: Set up Telegram bot
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    # Step 4: Register handlers
    register_checkin_handlers(app_bot)
    register_referral_handlers(app_bot)

    # Step 5: Start polling
    app_bot.run_polling()
