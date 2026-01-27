from database import db, get_collection, init_db

def main():
    init_db()
    users_collection = get_collection("users")
    result = users_collection.update_many(
        {"monthly_xp": {"$exists": False}},
        {"$set": {"monthly_xp": 0}},
    )
    print(f"Patched {result.modified_count} users missing monthly_xp.")

if __name__ == "__main__":
    main()
