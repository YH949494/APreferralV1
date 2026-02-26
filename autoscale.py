import os, subprocess
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, ASCENDING

APP_NAME = os.environ["FLY_APP_NAME"]
MONGO_URL = os.environ["MONGO_URL"]

LEAD_MINUTES = int(os.getenv("LEAD_MINUTES", "2"))
DURATION_MINUTES = int(os.getenv("DURATION_MINUTES", "10"))
PEAK_WEB_COUNT = int(os.getenv("PEAK_WEB_COUNT", "5"))
BASE_WEB_COUNT = int(os.getenv("BASE_WEB_COUNT", "1"))

def now_utc():
    return datetime.now(timezone.utc)

def fly_scale_web(count: int):
    subprocess.check_call([
        "fly", "scale", "count", str(count),
        "--process-group", "web",
        "--app", APP_NAME
    ])

def main():
    client = MongoClient(MONGO_URL)
    db = client.get_default_database()
    now = now_utc()

    # next upcoming drop by startsAt
    drop = db.drops.find_one(
        {"startsAt": {"$gte": now}},
        sort=[("startsAt", ASCENDING)],
        projection={"startsAt": 1, "name": 1},
    )

    if not drop or not drop.get("startsAt"):
        fly_scale_web(BASE_WEB_COUNT)
        print("[autoscale] no upcoming drop -> web=%d" % BASE_WEB_COUNT)
        return

    starts_at = drop["startsAt"]
    if starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=timezone.utc)

    window_start = starts_at - timedelta(minutes=LEAD_MINUTES)
    window_end = starts_at + timedelta(minutes=DURATION_MINUTES)

    if window_start <= now <= window_end:
        fly_scale_web(PEAK_WEB_COUNT)
        print(f"[autoscale] PEAK web={PEAK_WEB_COUNT} now={now.isoformat()} start={starts_at.isoformat()}")
    else:
        fly_scale_web(BASE_WEB_COUNT)
        print(f"[autoscale] BASE web={BASE_WEB_COUNT} now={now.isoformat()} start={starts_at.isoformat()}")

if __name__ == "__main__":
    main()
