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

Real one-time migration, not a recurring startup seed
-------------------------------------------------------
``run_lucky_games_migration(db)`` is called once from main.py at process
boot (every Gunicorn worker / Fly machine calls it — that's fine, see
below). Completion is tracked with a marker document in
``db.migrations`` keyed by MIGRATION_ID ("seed_lucky_games_v1"). Once that
marker exists, every future boot is a single indexed find_one and returns
immediately — the legacy list is never re-applied, so an Admin delete,
rename, or edit of a seeded row survives every future restart.

Concurrency safety does NOT depend on the marker check (that's just an
optimization to skip repeat work). It depends on two things that hold
regardless of how many Gunicorn workers / Fly machines call this at once:

  1. A partial unique index on ``seed_id`` (a stable, immutable identity —
     "legacy_daily_game_001" .. "legacy_daily_game_056" — never the
     mutable ``name`` field an admin can rename). Every insert goes
     through ``update_one({"seed_id": ...}, {"$setOnInsert": doc},
     upsert=True)``, which MongoDB executes atomically per document: if
     two processes race on the same seed_id, exactly one insert wins and
     the other becomes a no-op match (or, in the tighter race where both
     attempt the insert before either commits, the loser gets a
     DuplicateKeyError from the unique index, which is caught and treated
     as "already seeded by a concurrent worker", not a failure).
  2. The completion marker itself is written the same way — ``update_one
     ({"_id": MIGRATION_ID}, {"$setOnInsert": {...}}, upsert=True)`` — so
     concurrent marker writes also collapse to one winner safely.

No distinct("name") + insert_one anywhere: matching by name would treat an
admin's rename of a legacy row as "not migrated yet" and recreate it under
the old name on the next boot. seed_id is immutable precisely so renames
(and any other edit) never look like a missing row.

Usage (manual/CLI, e.g. to pre-seed a fresh DB or verify status):
  MONGO_URL='mongodb://...' python migrations/seed_lucky_games.py [--db referral_bot]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

logger = logging.getLogger("seed_lucky_games")

MIGRATION_ID = "seed_lucky_games_v1"
COLLECTION = "lucky_games"
MIGRATIONS_COLLECTION = "migrations"
SEED_SOURCE = "daily_game_slots_seed_v1"
SEED_INDEX_NAME = "ix_lucky_games_seed_id"

_TAG_TO_VOLATILITY = {
    "Low": "Low",
    "Low-Med": "Low-Med",
    "Med": "Medium",
    "Medium": "Medium",
    "High-Med": "High-Med",
    "High": "High",
}

# Literal copy of main.py's DAILY_GAME_SLOTS (id/weight dropped — those are
# specific to the daily-pick rotation and have no meaning here). Order is
# preserved 1:1 so seed_id "legacy_daily_game_NNN" is stable and matches
# each row's original position in DAILY_GAME_SLOTS.
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

LEGACY_GAME_COUNT = len(_DAILY_GAME_SLOTS)


def build_seed_docs(now: datetime | None = None) -> list[dict]:
    """Builds the 56 seed documents, each stamped with a stable, immutable
    ``seed_id`` ("legacy_daily_game_001".."056") derived from position in
    the source list — never from ``name``, which an admin can rename."""
    now = now or datetime.now(timezone.utc)
    docs = []
    for idx, slot in enumerate(_DAILY_GAME_SLOTS):
        docs.append({
            "seed_id": f"legacy_daily_game_{idx + 1:03d}",
            "name": slot["name"],
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
    return docs


def ensure_seed_index(col) -> None:
    """Partial unique index on seed_id — the sole guarantee (independent of
    the migration marker) that concurrent Gunicorn workers / Fly machines
    can never create duplicate legacy rows."""
    col.create_index(
        [("seed_id", 1)],
        unique=True,
        partialFilterExpression={"seed_id": {"$type": "string"}},
        name=SEED_INDEX_NAME,
    )


def run_lucky_games_migration(db, *, log=None) -> dict:
    """Idempotent, concurrency-safe, one-time migration entry point. Safe
    to call from every Gunicorn worker / Fly machine at every boot:

      - If the completion marker already exists, this is one indexed
        find_one and returns immediately (SKIP_COMPLETED) — legacy values
        are never re-applied, so Admin deletes/renames/edits survive.
      - Otherwise it seeds any missing legacy rows via atomic
        upsert-with-$setOnInsert against a partial unique index on
        seed_id, verifies the result, and only then writes the
        completion marker.
      - Never raises — any failure is logged as
        [LUCKY_GAMES][MIGRATION][FAILED] and the marker is left unwritten
        so a later boot (this process or another) can retry. App startup
        always continues either way.
    """
    log = log or logger
    try:
        migrations_col = db[MIGRATIONS_COLLECTION]
        games_col = db[COLLECTION]

        existing_marker = migrations_col.find_one({"_id": MIGRATION_ID})
        if existing_marker:
            log.info("[LUCKY_GAMES][MIGRATION][SKIP_COMPLETED] marker=%s", MIGRATION_ID)
            return {"status": "skipped", "reason": "already_completed"}

        log.info("[LUCKY_GAMES][MIGRATION][START] migration_id=%s", MIGRATION_ID)

        try:
            ensure_seed_index(games_col)
        except Exception:
            log.warning("[LUCKY_GAMES][MIGRATION] seed_index_creation_failed", exc_info=True)

        seed_docs = build_seed_docs()
        upserted = 0
        already_present = 0
        for doc in seed_docs:
            try:
                result = games_col.update_one(
                    {"seed_id": doc["seed_id"]},
                    {"$setOnInsert": doc},
                    upsert=True,
                )
                if getattr(result, "upserted_id", None) is not None:
                    upserted += 1
                else:
                    already_present += 1
            except DuplicateKeyError:
                # Lost a tight race against a concurrent worker inserting
                # the same seed_id at the same instant — the row exists
                # either way, so this is a benign no-op, not a failure.
                already_present += 1

        verified_count = games_col.count_documents({
            "seed_id": {"$in": [d["seed_id"] for d in seed_docs]},
        })
        if verified_count != LEGACY_GAME_COUNT:
            log.error(
                "[LUCKY_GAMES][MIGRATION][FAILED] verification_mismatch expected=%s found=%s",
                LEGACY_GAME_COUNT, verified_count,
            )
            return {
                "status": "failed",
                "reason": "verification_mismatch",
                "expected": LEGACY_GAME_COUNT,
                "found": verified_count,
            }

        now = datetime.now(timezone.utc)
        migrations_col.update_one(
            {"_id": MIGRATION_ID},
            {"$setOnInsert": {
                "_id": MIGRATION_ID,
                "completed_at": now,
                "seeded_count": LEGACY_GAME_COUNT,
            }},
            upsert=True,
        )

        log.info(
            "[LUCKY_GAMES][MIGRATION][COMPLETED] migration_id=%s upserted=%s already_present=%s total=%s",
            MIGRATION_ID, upserted, already_present, verified_count,
        )
        return {
            "status": "completed",
            "upserted": upserted,
            "already_present": already_present,
            "total": verified_count,
        }
    except Exception as exc:
        log.error("[LUCKY_GAMES][MIGRATION][FAILED] unexpected_error=%s", exc, exc_info=True)
        return {"status": "failed", "reason": "exception", "error": str(exc)}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.getenv("MONGO_DB_NAME", "referral_bot"))
    args = parser.parse_args()

    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        logger.error("[SEED] MONGO_URL env var is required")
        return 1

    from database import init_db, get_db

    init_db(mongo_url, args.db)
    db = get_db()
    report = run_lucky_games_migration(db)
    logger.info("[SEED] report=%s", report)
    return 0 if report.get("status") in ("completed", "skipped") else 1


if __name__ == "__main__":
    raise SystemExit(main())
