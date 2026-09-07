#!/usr/bin/env python3
"""One-time migration: seed the ``lucky_games`` collection (lucky_games.py)
from the game list that was previously only ever hardcoded in the Mini App
/ main.py (``DAILY_GAME_SLOTS``).

This does NOT touch ``DAILY_GAME_SLOTS`` or the existing `/v2/miniapp/
daily-game` rotation endpoint — those keep working exactly as before. It
only populates the new, separate admin-managed catalogue that backs the
new `GET /api/lucky-games` public endpoint and the "Lucky Games" admin
dashboard section, so the Mini App's Lucky Games card list has real data
from day one instead of starting empty.

The list below is a literal copy of main.py's DAILY_GAME_SLOTS at the time
this migration was written (not imported from main.py, since importing
that module pulls in the whole app/bot bootstrap as a side effect — this
script only needs the plain data). Each ``tag`` value is mapped onto the
lucky_games volatility enum (Low, Low-Med, Medium, High-Med, High); "Med"
becomes "Medium", everything else passes through unchanged.

Idempotent: matches candidates by ``name`` (case-sensitive, exact) and
skips any that already exist in the collection, so running this twice
never creates duplicates. Dry-run by default; requires --commit to write.

Usage:
  MONGO_URL='mongodb://...' python migrations/seed_lucky_games.py [--db referral_bot] [--commit]

Rollback:
  Every row this script inserts is stamped with
  ``seed_source: "daily_game_slots_seed_v1"``. To roll back:

    db.lucky_games.delete_many({"seed_source": "daily_game_slots_seed_v1"})
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from database import init_db, get_db  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
logger = logging.getLogger("seed_lucky_games")

SEED_SOURCE = "daily_game_slots_seed_v1"

_TAG_TO_VOLATILITY = {
    "Low": "Low",
    "Low-Med": "Low-Med",
    "Med": "Medium",
    "Medium": "Medium",
    "High-Med": "High-Med",
    "High": "High",
}

# Literal copy of main.py's DAILY_GAME_SLOTS (id/weight dropped — those are
# specific to the daily-pick rotation and have no meaning here).
_DAILY_GAME_SLOTS = [
    {"name": "Dragon Chi's Quest 2", "tag": "Med", "maxwin": "100000x"},
    {"name": "Piggy Bank Gold 2", "tag": "High-Med", "maxwin": "150000x"},
    {"name": "Zeustrike Xmas", "tag": "High", "maxwin": "30000x"},
    {"name": "Aztec: Bonus Hunt 2 Xmas", "tag": "High-Med", "maxwin": "12000x"},
    {"name": "Zeustrike", "tag": "High", "maxwin": "30000x"},
    {"name": "Fighting Bull", "tag": "Med", "maxwin": "8000x"},
    {"name": "Cat & Mouse", "tag": "High-Med", "maxwin": "5000x"},
    {"name": "Pinata Fest", "tag": "Med", "maxwin": "80000x"},
    {"name": "Buffalo Rush HIGHROLLER", "tag": "High", "maxwin": "15120x"},
    {"name": "Golden Egypt", "tag": "Med", "maxwin": "6000x"},
    {"name": "Mahjong Roar", "tag": "Med", "maxwin": "2500x"},
    {"name": "Maya: Elemental Totem 2", "tag": "High-Med", "maxwin": "2500x"},
    {"name": "Big Net Bass", "tag": "Med", "maxwin": "16000x"},
    {"name": "Sugar Crush", "tag": "Med", "maxwin": "20000x"},
    {"name": "Disco 777 Hold and Win", "tag": "High-Med", "maxwin": "512000x"},
    {"name": "Piggy Bank Gold", "tag": "Med", "maxwin": "30000x"},
    {"name": "Leprechaun's Fortune", "tag": "Med", "maxwin": "28500x"},
    {"name": "BlackJack 21", "tag": "Low-Med", "maxwin": "100000x"},
    {"name": "Pirate Treasure Hunt", "tag": "Low-Med", "maxwin": "1500x"},
    {"name": "Aztec: Gold Temple", "tag": "Med", "maxwin": "10000x"},
    {"name": "Cai Shen Fortune", "tag": "High-Med", "maxwin": "8262x"},
    {"name": "Crazy Bounty: Jackpot", "tag": "High-Med", "maxwin": "50000x"},
    {"name": "Rush Hour Gold", "tag": "Med", "maxwin": "1500x"},
    {"name": "Buffalo Rush", "tag": "Med", "maxwin": "4915x"},
    {"name": "Jumanji Bonanza", "tag": "Low", "maxwin": "150x"},
    {"name": "Phantom Multiplier", "tag": "High-Med", "maxwin": "120000x"},
    {"name": "Starry Adventure", "tag": "Low-Med", "maxwin": "25000x"},
    {"name": "Rhapsody of Muertos", "tag": "High-Med", "maxwin": "250000x"},
    {"name": "Kingyo Riches", "tag": "High-Med", "maxwin": "18600x"},
    {"name": "Fish Prawn Crab Bonanza", "tag": "High-Med", "maxwin": "20000x"},
    {"name": "Ramakien Blessing", "tag": "Med", "maxwin": "100x"},
    {"name": "Aztec: Bonus Hunt 2", "tag": "High-Med", "maxwin": "12000x"},
    {"name": "Football Fever", "tag": "High", "maxwin": "70000x"},
    {"name": "Firefly Hunter", "tag": "High-Med", "maxwin": "4027x"},
    {"name": "Dark Ritual", "tag": "High", "maxwin": "20000x"},
    {"name": "Hungry Slime", "tag": "High-Med", "maxwin": "50000x"},
    {"name": "Crazy Bounty", "tag": "Med", "maxwin": "10000x"},
    {"name": "Maya: Elemental Totem", "tag": "Med", "maxwin": "1180x"},
    {"name": "Dragon Chi's Quest", "tag": "Med", "maxwin": "80000x"},
    {"name": "Xmas Gift Delight", "tag": "Med", "maxwin": "20000x"},
    {"name": "Cookie Hunter", "tag": "Low-Med", "maxwin": "268x"},
    {"name": "Xiang Qi Ways 2", "tag": "Med", "maxwin": "2500x"},
    {"name": "DJ Fever", "tag": "Med", "maxwin": "5000x"},
    {"name": "Mace of Hercules", "tag": "High-Med", "maxwin": "16128x"},
    {"name": "Jewel Mastermind", "tag": "Med", "maxwin": "162x"},
    {"name": "Last Samurai", "tag": "High-Med", "maxwin": "15000x"},
    {"name": "Scale of Heaven: Anubis", "tag": "High-Med", "maxwin": "1000x"},
    {"name": "Infinity Ocean", "tag": "High-Med", "maxwin": "250000x"},
    {"name": "Fantastic Beast", "tag": "Med", "maxwin": "1200x"},
    {"name": "Aztec: Bonus Hunt", "tag": "Med", "maxwin": "800x"},
    {"name": "Bunny to the Moon", "tag": "Med", "maxwin": "1100x"},
    {"name": "Genie Mystery", "tag": "High", "maxwin": "15000x"},
    {"name": "Boom of Prosperity", "tag": "Med", "maxwin": "730x"},
    {"name": "Slotto 4D", "tag": "Med", "maxwin": "10050x"},
    {"name": "World Cup Final", "tag": "Med", "maxwin": "1180x"},
    {"name": "Disco 777", "tag": "Med", "maxwin": "28500x"},
]


def run(*, mongo_url: str, db_name: str, commit: bool) -> dict:
    init_db(mongo_url, db_name)
    db = get_db()
    col = db["lucky_games"]

    existing_names = set(col.distinct("name"))
    now = datetime.now(timezone.utc)

    to_insert = []
    for idx, slot in enumerate(_DAILY_GAME_SLOTS):
        name = slot["name"]
        if name in existing_names:
            continue
        to_insert.append({
            "name": name,
            "label": "Lucky Game",
            "volatility": _TAG_TO_VOLATILITY.get(slot.get("tag"), "Medium"),
            "max_win": slot.get("maxwin", ""),
            "image_url": "",
            "game_url": "",
            "provider": "",
            "sort_order": idx * 10,
            "is_published": True,
            "created_at": now,
            "updated_at": now,
            "seed_source": SEED_SOURCE,
        })

    report = {
        "total_source_rows": len(_DAILY_GAME_SLOTS),
        "already_present": len(_DAILY_GAME_SLOTS) - len(to_insert),
        "to_insert": len(to_insert),
        "committed": False,
        "inserted_count": 0,
    }

    logger.info(
        "[SEED] source_rows=%s already_present=%s to_insert=%s",
        report["total_source_rows"], report["already_present"], report["to_insert"],
    )

    if not to_insert:
        logger.info("[SEED] nothing to do — every source row already has a matching lucky_games doc by name")
        return report

    if not commit:
        logger.info("[SEED] DRY-RUN — would insert %s row(s). Re-run with --commit to apply.", len(to_insert))
        return report

    for doc in to_insert:
        col.insert_one(doc)
    report["committed"] = True
    report["inserted_count"] = len(to_insert)
    logger.info("[SEED] APPLIED inserted_count=%s", len(to_insert))
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.getenv("MONGO_DB_NAME", "referral_bot"))
    parser.add_argument("--commit", action="store_true", help="Apply changes (default: dry-run report only)")
    args = parser.parse_args()

    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        logger.error("[SEED] MONGO_URL env var is required")
        return 1

    report = run(mongo_url=mongo_url, db_name=args.db, commit=args.commit)
    logger.info("[SEED] report=%s", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
