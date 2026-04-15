from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Any

from config import KL_TZ
from database import db

logger = logging.getLogger(__name__)


def telegram_week_window_kl(reference_utc: datetime | None = None) -> dict[str, datetime]:
    ref_utc = reference_utc or datetime.now(timezone.utc)
    if ref_utc.tzinfo is None:
        ref_utc = ref_utc.replace(tzinfo=timezone.utc)
    ref_kl = ref_utc.astimezone(KL_TZ)
    week_start_kl = (ref_kl - timedelta(days=ref_kl.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end_kl = week_start_kl + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
    return {
        "week_start_kl": week_start_kl,
        "week_end_kl": week_end_kl,
        "week_start_utc": week_start_kl.astimezone(timezone.utc),
        "week_end_utc": week_end_kl.astimezone(timezone.utc),
    }


async def refresh_telegram_channel_stats(bot, chat_id: int, *, db_ref=None) -> dict[str, Any]:
    db_ref = db_ref or db
    now_utc = datetime.now(timezone.utc)

    chat = await bot.get_chat(chat_id=chat_id)
    member_count = await bot.get_chat_member_count(chat_id=chat_id)

    admin_count = None
    try:
        admins = await bot.get_chat_administrators(chat_id=chat_id)
        admin_count = len(admins or [])
    except Exception as exc:  # noqa: BLE001
        logger.info("[TG_STATS][CHANNEL] chat_id=%s admins_unavailable err=%s", chat_id, exc.__class__.__name__)

    payload = {
        "chat_id": int(chat_id),
        "type": getattr(chat, "type", None),
        "title": getattr(chat, "title", None),
        "username": getattr(chat, "username", None),
        "member_count": int(member_count or 0),
        "refreshed_at": now_utc,
    }
    if admin_count is not None:
        payload["administrator_count"] = int(admin_count)

    db_ref["telegram_channel_stats"].update_one(
        {"chat_id": int(chat_id)},
        {"$set": payload},
        upsert=True,
    )
    logger.info(
        "[TG_STATS][CHANNEL] refreshed chat_id=%s members=%s admins=%s",
        chat_id,
        payload["member_count"],
        payload.get("administrator_count"),
    )
    return payload


def aggregate_telegram_post_stats_weekly(*, db_ref=None, reference_utc: datetime | None = None) -> dict[str, Any]:
    db_ref = db_ref or db
    window = telegram_week_window_kl(reference_utc)
    week_start_utc = window["week_start_utc"]
    week_end_utc = window["week_end_utc"]

    try:
        collections = set(db_ref.list_collection_names())
    except Exception:
        collections = set()

    raw_collection_name = "telegram_post_stats_raw"
    if collections and raw_collection_name not in collections:
        logger.info(
            "[TG_STATS][WEEKLY] skipped reason=no_raw_collection week_start_utc=%s",
            week_start_utc.isoformat(),
        )
        return {
            "ok": True,
            "week_start_utc": week_start_utc,
            "week_end_utc": week_end_utc,
            "chats": 0,
            "posts": 0,
            "skipped": "no_raw_collection",
        }

    docs = list(
        db_ref[raw_collection_name].find(
            {"posted_at_utc": {"$gte": week_start_utc, "$lte": week_end_utc}},
            {
                "_id": 0,
                "chat_id": 1,
                "message_id": 1,
                "views": 1,
                "forwards": 1,
                "reactions_total": 1,
            },
        )
    )

    by_chat: dict[int, dict[str, int]] = defaultdict(lambda: {
        "posts": 0,
        "views": 0,
        "forwards": 0,
        "reactions_total": 0,
    })

    for doc in docs:
        chat_id_raw = doc.get("chat_id")
        if chat_id_raw is None:
            continue
        chat_id = int(chat_id_raw)
        row = by_chat[chat_id]
        row["posts"] += 1
        row["views"] += int(doc.get("views", 0) or 0)
        row["forwards"] += int(doc.get("forwards", 0) or 0)
        row["reactions_total"] += int(doc.get("reactions_total", 0) or 0)

    upserts = 0
    for chat_id, row in by_chat.items():
        db_ref["telegram_post_stats_weekly"].update_one(
            {"week_start_utc": week_start_utc, "chat_id": chat_id},
            {
                "$set": {
                    "week_start_utc": week_start_utc,
                    "week_end_utc": week_end_utc,
                    "chat_id": chat_id,
                    "posts": int(row["posts"]),
                    "views": int(row["views"]),
                    "forwards": int(row["forwards"]),
                    "reactions_total": int(row["reactions_total"]),
                    "aggregated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
        upserts += 1

    logger.info(
        "[TG_STATS][WEEKLY] week_start_utc=%s week_end_utc=%s chats=%s posts=%s",
        week_start_utc.isoformat(),
        week_end_utc.isoformat(),
        len(by_chat),
        len(docs),
    )
    return {
        "ok": True,
        "week_start_utc": week_start_utc,
        "week_end_utc": week_end_utc,
        "chats": len(by_chat),
        "posts": len(docs),
        "upserts": upserts,
    }
