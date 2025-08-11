import os
from pymongo import MongoClient

MONGO_URL = os.environ.get("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["referral_bot"]

users_collection = db["users"]

result = users_collection.update_many(
    {"monthly_xp": {"$exists": False}},
    {"$set": {"monthly_xp": 0}}
)

print(f"Patched {result.modified_count} users missing monthly_xp.")
