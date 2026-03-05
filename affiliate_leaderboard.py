from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Any

from pymongo import ASCENDING, DESCENDING, ReturnDocument

MAX_COUNTED_JOINS_PER_DAY = 10
MIN_SECONDS_BETWEEN_COUNTED_JOINS = 120
MIN_CONVERSION_THRESHOLD = 0.20
LIVE_TTL_SECONDS = 60
logger = logging.getLogger(__name__)


def _coerce_datetime_utc(value) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def week_window_utc(reference_utc: datetime | None = None) -> tuple[datetime, datetime]:
    ref = reference_utc or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    ref = ref.astimezone(timezone.utc)
    start = datetime(ref.year, ref.month, ref.day, tzinfo=timezone.utc) - timedelta(days=ref.weekday())
    end = start + timedelta(days=7)
    return start, end


def ensure_affiliate_leaderboard_indexes(db_ref) -> None:
    db_ref.affiliate_referral_cooldown.create_index([("updated_at", ASCENDING)], name="cooldown_updated_at")
    db_ref.referral_flow_events.create_index([("event", ASCENDING), ("ts_utc", ASCENDING)], name="rfe_event_ts")
    db_ref.referral_flow_events.create_index([("referrer_id", ASCENDING), ("ts_utc", ASCENDING)], name="rfe_referrer_ts")
    db_ref.referral_flow_events.create_index([("referrer_id", ASCENDING), ("event", ASCENDING), ("ts_utc", ASCENDING)], name="rfe_referrer_event_ts")
    db_ref.referral_flow_events.create_index([("invitee_id", ASCENDING), ("ts_utc", ASCENDING)], name="rfe_invitee_ts")
    db_ref.affiliate_weekly_kpis.create_index([("week_start_utc", ASCENDING)], unique=True, name="uniq_affiliate_week_start")
    db_ref.affiliate_weekly_kpis.create_index([("generated_at", DESCENDING)], name="affiliate_weekly_generated_desc")
    db_ref.affiliate_weekly_kpis_live.create_index([("week_start_utc", ASCENDING)], unique=True, name="uniq_affiliate_week_start_live")
    db_ref.affiliate_weekly_kpis_live.create_index([("generated_at", DESCENDING)], name="affiliate_weekly_generated_desc_live")


def emit_referral_flow_event(
    db_ref,
    *,
    event: str,
    referrer_id: int | None,
    invitee_id: int | None,
    ts_utc: datetime,
    meta: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> bool:
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    ts_utc = ts_utc.astimezone(timezone.utc)
    if idempotency_key:
        event_id = idempotency_key
    else:
        if event in {"join", "join_counted", "join_ignored"}:
            bucket = ts_utc.strftime("%Y-%m-%d")
        else:
            bucket = ts_utc.isoformat()
        event_id = f"rf|{event}|{referrer_id}|{invitee_id}|{bucket}"
    db_ref.referral_flow_events.update_one(
        {"_id": event_id},
        {
            "$setOnInsert": {
                "_id": event_id,
                "event": event,
                "referrer_id": referrer_id,
                "invitee_id": invitee_id,
                "ts_utc": ts_utc,
                "meta": meta or {},
            }
        },
        upsert=True,
    )
    return True


def should_count_referral_join(db_ref, referrer_id: int, now_utc: datetime) -> tuple[bool, str | None]:
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)
    day_utc = now_utc.strftime("%Y-%m-%d")
    now_ts = now_utc.timestamp()
    doc_id = f"{int(referrer_id)}:{day_utc}"

    updated = db_ref.affiliate_referral_cooldown.find_one_and_update(
        {
            "_id": doc_id,
            "$and": [
                {
                    "$or": [
                        {"counted_joins_today": {"$exists": False}},
                        {"counted_joins_today": {"$lt": MAX_COUNTED_JOINS_PER_DAY}},
                    ]
                },
                {
                    "$or": [
                        {"last_counted_join_ts": {"$exists": False}},
                        {"last_counted_join_ts": {"$lte": now_ts - MIN_SECONDS_BETWEEN_COUNTED_JOINS}},
                    ]
                },
            ],
        },
        {
            "$inc": {"counted_joins_today": 1},
            "$set": {"last_counted_join_ts": now_ts, "updated_at": now_utc},
            "$setOnInsert": {"referrer_id": int(referrer_id), "day_utc": day_utc},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if updated is not None:
        return True, None

    current = db_ref.affiliate_referral_cooldown.find_one({"_id": doc_id}, {"counted_joins_today": 1, "last_counted_join_ts": 1}) or {}
    if int(current.get("counted_joins_today", 0) or 0) >= MAX_COUNTED_JOINS_PER_DAY:
        return False, "daily_cap"
    return False, "cooldown"


def compute_affiliate_weekly_kpis_snapshot(db_ref, *, reference_utc: datetime | None = None) -> dict:
    return compute_affiliate_weekly_kpis_live(db_ref, reference_utc=reference_utc)


def _build_affiliate_weekly_payload(db_ref, *, reference_utc: datetime | None = None) -> tuple[datetime, dict]:
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)

    week_start_utc, week_end_utc = week_window_utc(now_utc_ts)

    joins_rows = list(
        db_ref.pending_referrals.aggregate(
            [
                {"$match": {"created_at_utc": {"$gte": week_start_utc, "$lt": week_end_utc}, "inviter_user_id": {"$ne": None}}},
                {"$group": {"_id": "$inviter_user_id", "joins_week_raw": {"$sum": 1}}},
            ]
        )
    )
    counted_rows = list(
        db_ref.referral_flow_events.aggregate(
            [
                {"$match": {"event": "join_counted", "ts_utc": {"$gte": week_start_utc, "$lt": week_end_utc}, "referrer_id": {"$ne": None}}},
                {"$group": {"_id": "$referrer_id", "joins_week_counted": {"$sum": 1}}},
            ]
        )
    )
    qualified_rows = list(
        db_ref.qualified_events.aggregate(
            [
                {"$match": {"qualified_at": {"$gte": week_start_utc, "$lt": week_end_utc}, "referrer_id": {"$ne": None}}},
                {"$group": {"_id": "$referrer_id", "qualified_week": {"$sum": 1}}},
            ]
        )
    )

    board_map: dict[str, dict[str, Any]] = {}

    def _touch(referrer_raw):
        referrer_key = str(referrer_raw)
        if referrer_key not in board_map:
            board_map[referrer_key] = {
                "referrer_id": referrer_key,
                "joins_week_raw": 0,
                "joins_week_counted": 0,
                "qualified_week": 0,
            }
        return board_map[referrer_key]

    for row in joins_rows:
        ref_id = row.get("_id")
        if ref_id is None:
            continue
        _touch(ref_id)["joins_week_raw"] = int(row.get("joins_week_raw", 0) or 0)

    for row in counted_rows:
        ref_id = row.get("_id")
        if ref_id is None:
            continue
        _touch(ref_id)["joins_week_counted"] = int(row.get("joins_week_counted", 0) or 0)

    for row in qualified_rows:
        ref_id = row.get("_id")
        if ref_id is None:
            continue
        _touch(ref_id)["qualified_week"] = int(row.get("qualified_week", 0) or 0)

    leaderboard = []
    for item in board_map.values():
        joins_week_raw = int(item.get("joins_week_raw", 0) or 0)
        qualified_week = int(item.get("qualified_week", 0) or 0)
        conversion = float(qualified_week / joins_week_raw) if joins_week_raw > 0 else 0.0
        if joins_week_raw < 10:
            quality_flag = "new"
        elif conversion < MIN_CONVERSION_THRESHOLD:
            quality_flag = "low_quality"
        else:
            quality_flag = "ok"
        leaderboard.append(
            {
                "referrer_id": item["referrer_id"],
                "joins_week_raw": joins_week_raw,
                "joins_week_counted": int(item.get("joins_week_counted", 0) or 0),
                "qualified_week": qualified_week,
                "conversion_week": round(conversion, 4),
                "quality_flag": quality_flag,
            }
        )

    leaderboard.sort(
        key=lambda r: (
            -int(r.get("qualified_week", 0)),
            -int(r.get("joins_week_raw", 0)),
            -float(r.get("conversion_week", 0.0)),
        )
    )
    top50 = []
    for idx, row in enumerate(leaderboard[:50], start=1):
        copied = dict(row)
        copied["rank"] = idx
        top50.append(copied)

    payload = {
        "week_start_utc": week_start_utc,
        "week_end_utc": week_end_utc,
        "generated_at": now_utc_ts,
        "rules": {
            "cap_per_day": MAX_COUNTED_JOINS_PER_DAY,
            "cooldown_seconds": MIN_SECONDS_BETWEEN_COUNTED_JOINS,
            "min_conversion_threshold": MIN_CONVERSION_THRESHOLD,
        },
        "affiliate_leaderboard_week": top50,
        "affiliate_weekly_by_referrer": {
            str(item.get("referrer_id")): {
                "joins_week_raw": int(item.get("joins_week_raw", 0) or 0),
                "joins_week_counted": int(item.get("joins_week_counted", 0) or 0),
                "qualified_week": int(item.get("qualified_week", 0) or 0),
                "conversion_week": float(item.get("conversion_week", 0.0) or 0.0),
                "quality_flag": item.get("quality_flag") or "new",
            }
            for item in sorted(
                leaderboard,
                key=lambda r: (-int(r.get("joins_week_raw", 0)), -int(r.get("qualified_week", 0))),
            )[:500]
        },
    }
    return week_start_utc, payload


def compute_affiliate_weekly_kpis_live(db_ref, *, reference_utc: datetime | None = None) -> dict:
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)
    week_start_utc, week_end_utc = week_window_utc(now_utc_ts)
    existing = db_ref.affiliate_weekly_kpis_live.find_one({"week_start_utc": week_start_utc})
    if existing:
        existing_generated_at = _coerce_datetime_utc(existing.get("generated_at"))
        if existing_generated_at is None and existing.get("generated_at") is not None:
            logger.warning("[AFFILIATE][LIVE_KPI] stale_generated_at_unparseable value=%s", existing.get("generated_at"))
        if existing_generated_at is not None:
            if (now_utc_ts - existing_generated_at).total_seconds() < LIVE_TTL_SECONDS:
                return existing

    _, payload = _build_affiliate_weekly_payload(db_ref, reference_utc=now_utc_ts)
    payload["week_start_utc"] = _coerce_datetime_utc(payload.get("week_start_utc")) or week_start_utc
    payload["week_end_utc"] = _coerce_datetime_utc(payload.get("week_end_utc")) or week_end_utc
    payload["generated_at"] = _coerce_datetime_utc(payload.get("generated_at")) or now_utc_ts

    db_ref.affiliate_weekly_kpis_live.update_one({"week_start_utc": week_start_utc}, {"$set": payload}, upsert=True)
    return db_ref.affiliate_weekly_kpis_live.find_one({"week_start_utc": week_start_utc}) or payload


def compute_affiliate_weekly_kpis_final(db_ref, *, reference_utc: datetime | None = None) -> dict:
    week_start_utc, payload = _build_affiliate_weekly_payload(db_ref, reference_utc=reference_utc)
    existing = db_ref.affiliate_weekly_kpis.find_one({"week_start_utc": week_start_utc})
    if existing:
        return existing
    db_ref.affiliate_weekly_kpis.update_one({"week_start_utc": week_start_utc}, {"$setOnInsert": payload}, upsert=True)
    return db_ref.affiliate_weekly_kpis.find_one({"week_start_utc": week_start_utc}) or payload
