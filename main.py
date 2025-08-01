import os
import asyncio
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory
from telegram import Bot, Update, ChatMemberUpdated
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ChatJoinRequestHandler,
    ChatMemberHandler
)
from pymongo import MongoClient
from threading import Thread

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Flask Setup ===
app = Flask(__name__)

# === MongoDB Setup ===
MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
leaderboard_collection = db["weekly_leaderboard"]

# === Bot & Group ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GROUP_ID = int(os.environ.get("GROUP_ID", "-1002304653063"))
bot = Bot(BOT_TOKEN)

# === Flask Routes ===
@app.route("/")
def home():
    return "Bot is running!"

@app.route("/miniapp")
def miniapp():
    return send_from_directory(".", "index.html")

@app.route("/api/checkin")
def handle_checkin():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    user = users_collection.find_one({"user_id": user_id})
    now = datetime.utcnow()

    if not user:
        user = {
            "user_id": user_id,
            "username": username,
            "xp": 0,
            "weekly_xp": 0,
            "referral_count": 0,
            "last_checkin": None,
            "streak_count": 0,
            "badges": []
        }

    last_checkin = user.get("last_checkin")
    streak_count = user.get("streak_count", 0)
    badges = user.get("badges", [])

    if last_checkin:
        elapsed = now - last_checkin
        if elapsed < timedelta(hours=24):
            remaining = timedelta(hours=24) - elapsed
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            return jsonify({
                "success": False,
                "message": f"⏳ Come back in {hours}h {minutes}m to check in again!"
            })
        elif elapsed < timedelta(hours=48):
            streak_count += 1
        else:
            streak_count = 1
            badges = []
    else:
        streak_count = 1

    # Award badges
    if streak_count >= 28 and "gold" not in badges:
        badges.append("gold")
    elif streak_count >= 14 and "silver" not in badges and "gold" not in badges:
        badges.append("silver")
    elif streak_count >= 7 and "bronze" not in badges and "silver" not in badges and "gold" not in badges:
        badges.append("bronze")

    users_collection.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "username": username,
                "last_checkin": now,
                "streak_count": streak_count,
                "badges": badges
            },
            "$inc": {
                "xp": 20,
                "weekly_xp": 20
            }
        },
        upsert=True
    )

    return jsonify({
        "success": True,
        "message": f"✅ Check-in successful! +20 XP\n🔥 Current streak: {streak_count} days"
    })

@app.route("/api/stats")
def get_user_stats():
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    user = users_collection.find_one({"user_id": user_id}) or {}
    return jsonify({
        "xp": user.get("xp", 0),
        "referral_count": user.get("referral_count", 0),
        "streak_count": user.get("streak_count", 0),
        "badges": user.get("badges", [])
    })

@app.route("/api/referral")
async def generate_referral_link():
    user_id = request.args.get("user_id", type=int)
    username = request.args.get("username", default="")

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    now = datetime.utcnow()
    user = users_collection.find_one({"user_id": user_id})
    expire_cutoff = now - timedelta(hours=24)

    if user and "referral_link" in user and user.get("referral_generated_at", now - timedelta(days=1)) > expire_cutoff:
        return jsonify({"link": user["referral_link"]})

    link_name = f"ref-{user_id}"
    expire_time = now + timedelta(hours=24)

    invite_link = await bot.create_chat_invite_link(
        chat_id=GROUP_ID,
        name=link_name,
        expire_date=int(expire_time.timestamp()),
        creates_join_request=True
    )

    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "referral_link": invite_link.invite_link,
            "referral_generated_at": now,
            "username": username
        }},
        upsert=True
    )

    return jsonify({"link": invite_link.invite_link})

# === Telegram Bot Handlers ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Welcome! Use the Mini App to check in and refer friends!")

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user
    invite_link = update.chat_join_request.invite_link
    referred_by = None

    if invite_link and invite_link.name and invite_link.name.startswith("ref-"):
        referred_by = int(invite_link.name.split("-")[1])

    if referred_by and referred_by != user.id:
        if is_recent_leave(user.id):
            logger.info(f"Referral blocked: {user.id} recently left")
        else:
            users_collection.update_one(
                {"user_id": user.id},
                {"$set": {
                    "referred_by": referred_by,
                    "joined_at": datetime.utcnow()
                }},
                upsert=True
            )
            users_collection.update_one(
                {"user_id": referred_by},
                {"$inc": {"xp": 50, "referral_count": 1}}
            )
            logger.info(f"{user.id} joined via referral from {referred_by}")

    await update.chat_join_request.approve()

def is_recent_leave(user_id):
    user = users_collection.find_one({"user_id": user_id})
    if user and "left_at" in user:
        return datetime.utcnow() - user["left_at"] < timedelta(hours=12)
    return False

async def handle_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_member.old_chat_member.user
    status = update.chat_member.new_chat_member.status
    if status == "left":
        users_collection.update_one(
            {"user_id": user.id},
            {"$set": {"left_at": datetime.utcnow()}}
        )

# === Launch Bot + Flask ===
def run_flask():
    app.run(host="0.0.0.0", port=8080)

def run_bot():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    application.add_handler(ChatMemberHandler(handle_leave, ChatMemberHandler.MY_CHAT_MEMBER))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.start())
    loop.run_forever()

if __name__ == "__main__":
    Thread(target=run_flask).start()
    run_bot() 
