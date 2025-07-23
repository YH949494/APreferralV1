from database import users_collection

# Use this if you want to give generic bot deep link
BASE_REFERRAL_LINK = "t.me/APreferralV1_bot"  

def get_or_create_referral_link(user_id: int):
    user = users_collection.find_one({"user_id": user_id})

    if user:
        referral_code = str(user_id)
        referral_count = user.get("referral_count", 0)
    else:
        referral_code = str(user_id)
        users_collection.insert_one({
            "user_id": user_id,
            "username": None,
            "xp": 0,
            "weekly_xp": 0,
            "last_checkin": None,
            "referral_count": 0
        })
        referral_count = 0

    return {
        "referral_link": BASE_REFERRAL_LINK + referral_code,
        "referral_count": referral_count
    }
