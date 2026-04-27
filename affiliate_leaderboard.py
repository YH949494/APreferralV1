from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import os
from typing import Any
from zoneinfo import ZoneInfo

from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

MAX_COUNTED_JOINS_PER_DAY = 10
MIN_SECONDS_BETWEEN_COUNTED_JOINS = 120
MIN_CONVERSION_THRESHOLD = 0.20
LIVE_TTL_SECONDS = 60
logger = logging.getLogger(__name__)
KL_TZ = ZoneInfo("Asia/Kuala_Lumpur")
try:
    AFFILIATE_SNAPSHOT_TOP_N = int(os.getenv("AFFILIATE_SNAPSHOT_TOP_N", "100"))
except (TypeError, ValueError):
    AFFILIATE_SNAPSHOT_TOP_N = 100


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


def affiliate_week_window_utc_from_reference(reference_utc: datetime | None = None) -> tuple[datetime, datetime, datetime]:
    ref = reference_utc or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    ref_local = ref.astimezone(KL_TZ)
    start_local = datetime(
        ref_local.year,
        ref_local.month,
        ref_local.day,
        tzinfo=KL_TZ,
    ) - timedelta(days=ref_local.weekday())
    end_local = start_local + timedelta(days=7)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), start_local


def affiliate_previous_completed_week_window_kl(reference_utc: datetime | None = None) -> dict[str, Any]:
    current_start_utc, _, current_start_local = affiliate_week_window_utc_from_reference(reference_utc=reference_utc)
    prev_start_local = current_start_local - timedelta(days=7)
    prev_end_local = prev_start_local + timedelta(days=6)
    prev_start_utc = current_start_utc - timedelta(days=7)
    prev_end_utc = current_start_utc
    return {
        "week_key": prev_start_local.date().isoformat(),
        "week_start_local": prev_start_local.date().isoformat(),
        "week_end_local": prev_end_local.date().isoformat(),
        "week_start_utc": prev_start_utc,
        "week_end_utc": prev_end_utc,
    }


def affiliate_week_window_from_week_key_kl(week_key: str) -> dict[str, Any] | None:
    try:
        parsed = datetime.strptime(week_key, "%Y-%m-%d").replace(tzinfo=KL_TZ)
    except ValueError:
        return None
    if parsed.weekday() != 0:
        return None
    end_local = parsed + timedelta(days=6)
    return {
        "week_key": week_key,
        "week_start_local": parsed.date().isoformat(),
        "week_end_local": end_local.date().isoformat(),
        "week_start_utc": parsed.astimezone(timezone.utc),
        "week_end_utc": (parsed + timedelta(days=7)).astimezone(timezone.utc),
    }


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


def ensure_affiliate_snapshot_indexes(db_ref) -> None:
    db_ref.affiliate_leaderboard_snapshots.create_index([("week_key", ASCENDING)], unique=True, name="uniq_affiliate_snapshot_week_key")
    db_ref.affiliate_leaderboard_snapshots.create_index([("snapshot_at", DESCENDING)], name="affiliate_snapshot_at_desc")


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

    query = {
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
    }
    update = {
        "$inc": {"counted_joins_today": 1},
        "$set": {"last_counted_join_ts": now_ts, "updated_at": now_utc},
        "$setOnInsert": {"referrer_id": int(referrer_id), "day_utc": day_utc},
    }
    try:
        updated = db_ref.affiliate_referral_cooldown.find_one_and_update(
            query,
            update,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        updated = db_ref.affiliate_referral_cooldown.find_one_and_update(
            query,
            update,
            upsert=False,
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


def _compute_affiliate_weekly_rows(db_ref, week_start_utc: datetime, week_end_utc: datetime) -> list[dict[str, Any]]:
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
    qualified_rows = []
    qualified_collection = getattr(db_ref, "qualified_events", None)
    if qualified_collection is not None:
        qualified_rows = list(
            qualified_collection.aggregate(
                [
                    {
                        "$match": {
                            "qualified_at": {"$gte": week_start_utc, "$lt": week_end_utc},
                            "referrer_id": {"$ne": None},
                        }
                    },
                    {"$group": {"_id": "$referrer_id", "qualified_week": {"$sum": 1}}},
                ]
            )
        )
    flow_settled_match = {
        "event": "referral_settled",
        "ts_utc": {"$gte": week_start_utc, "$lt": week_end_utc},
        "referrer_id": {"$ne": None},
    }
    if not qualified_rows:
        qualified_rows = list(
            db_ref.referral_flow_events.aggregate(
                [
                    {"$match": flow_settled_match},
                    {"$group": {"_id": "$referrer_id", "qualified_week": {"$sum": 1}}},
                ]
            )
        )
    if not qualified_rows:
        qualified_rows = list(
            db_ref.referral_events.aggregate(
                [
                    {
                        "$match": {
                            "event": "referral_settled",
                            "occurred_at": {"$gte": week_start_utc, "$lt": week_end_utc},
                            "inviter_id": {"$ne": None},
                        }
                    },
                    {"$group": {"_id": "$inviter_id", "qualified_week": {"$sum": 1}}},
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
    for idx, row in enumerate(leaderboard, start=1):
        copied = dict(row)
        copied["rank"] = idx
        leaderboard[idx - 1] = copied

    return leaderboard


def _build_affiliate_weekly_payload(db_ref, *, reference_utc: datetime | None = None) -> tuple[datetime, dict]:
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)

    week_start_utc, week_end_utc, _ = affiliate_week_window_utc_from_reference(now_utc_ts)
    leaderboard = _compute_affiliate_weekly_rows(db_ref, week_start_utc, week_end_utc)
    top50 = leaderboard[:50]

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


def build_affiliate_leaderboard_snapshot(
    db_ref,
    *,
    week_window: dict[str, Any] | None = None,
    reference_utc: datetime | None = None,
    force: bool = False,
    mode: str = "scheduler",
    top_n: int | None = None,
    user_identity_map: dict[str, dict[str, Any]] | None = None,
    user_identity_loader=None,
    window_type: str | None = None,
) -> dict[str, Any]:
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)
    ensure_affiliate_snapshot_indexes(db_ref)
    window = week_window or affiliate_previous_completed_week_window_kl(reference_utc=now_utc_ts)
    week_key = window["week_key"]
    logger.info("[AFF_SNAPSHOT][START] week_key=%s mode=%s", week_key, mode)
    try:
        existing = db_ref.affiliate_leaderboard_snapshots.find_one({"week_key": week_key}, {"_id": 1})
        if existing and not force:
            logger.info("[AFF_SNAPSHOT][SKIP_EXISTS] week_key=%s", week_key)
            return {"ok": True, "week_key": week_key, "status": "skipped_exists", "entry_count": None}
        if existing and force:
            logger.info("[AFF_SNAPSHOT][OVERWRITE] week_key=%s mode=%s", week_key, mode)

        if top_n is None:
            row_limit = AFFILIATE_SNAPSHOT_TOP_N
        else:
            try:
                row_limit = int(top_n)
            except (TypeError, ValueError):
                row_limit = AFFILIATE_SNAPSHOT_TOP_N
        if row_limit < 0:
            row_limit = 0

        leaderboard = _compute_affiliate_weekly_rows(db_ref, window["week_start_utc"], window["week_end_utc"])
        entries = []
        selected_rows = leaderboard[:row_limit]
        identity_by_id = user_identity_map or {}
        if not identity_by_id and callable(user_identity_loader):
            try:
                identity_by_id = user_identity_loader([int(r.get("referrer_id", 0) or 0) for r in selected_rows]) or {}
            except Exception:
                identity_by_id = {}
        for row in selected_rows:
            row_user_id = int(row.get("referrer_id", 0) or 0)
            frozen = identity_by_id.get(str(row_user_id)) if isinstance(identity_by_id, dict) else None
            entries.append(
                {
                    "rank": int(row.get("rank", 0) or 0),
                    "user_id": row_user_id,
                    "username": (frozen or {}).get("username"),
                    "display_name": (frozen or {}).get("display_name"),
                    "metric_value": int(row.get("qualified_week", 0) or 0),
                    "extra": {
                        "joins_week_raw": int(row.get("joins_week_raw", 0) or 0),
                        "joins_week_counted": int(row.get("joins_week_counted", 0) or 0),
                        "qualified_week": int(row.get("qualified_week", 0) or 0),
                        "conversion_week": float(row.get("conversion_week", 0.0) or 0.0),
                        "quality_flag": row.get("quality_flag") or "new",
                    },
                }
            )

        payload = {
            "week_key": week_key,
            "week_start_local": window["week_start_local"],
            "week_end_local": window["week_end_local"],
            "week_start_utc": window["week_start_utc"],
            "week_end_utc": window["week_end_utc"],
            "window_type": window_type or ("previous_completed_week" if week_window is None else "explicit_week_key"),
            "snapshot_at": now_utc_ts,
            "entry_count": len(entries),
            "metric_name": "qualified_week",
            "generated_by": mode,
            "source_version": 1,
            "entries": entries,
        }
        db_ref.affiliate_leaderboard_snapshots.update_one({"week_key": week_key}, {"$set": payload}, upsert=True)
        logger.info("[AFF_SNAPSHOT][DONE] week_key=%s entry_count=%s", week_key, len(entries))
        return {"ok": True, "week_key": week_key, "status": "regenerated" if force else "created", "entry_count": len(entries)}
    except Exception as exc:
        logger.exception("[AFF_SNAPSHOT][ERROR] week_key=%s err=%s", week_key, exc)
        raise



def serialize_affiliate_snapshot_entries_for_viewer(
    entries: list[dict[str, Any]],
    *,
    current_user_id: int,
    is_admin: bool,
    format_username_fn,
    mask_username_fn,
) -> list[dict[str, Any]]:
    out = []
    for raw in entries or []:
        row = dict(raw)
        try:
            row_user_id = int(row.get("user_id"))
        except Exception:
            row_user_id = 0

        raw_username = row.get("username")
        raw_display = row.get("display_name")

        display = None
        if callable(format_username_fn):
            display = format_username_fn(
                {
                    "user_id": row_user_id,
                    "username": raw_username,
                    "first_name": raw_display,
                },
                current_user_id,
                is_admin,
            )
        if display is not None:
            row["display_name"] = display

        if not is_admin and row_user_id != int(current_user_id or 0):
            if raw_username:
                row["username"] = mask_username_fn(str(raw_username)) if callable(mask_username_fn) else raw_username
            row.pop("user_id", None)
            extra = row.get("extra")
            if isinstance(extra, dict):
                extra_copy = dict(extra)
                extra_copy.pop("quality_flag", None)
                row["extra"] = extra_copy

        out.append(row)
    return out

def compute_affiliate_weekly_kpis_live(db_ref, *, reference_utc: datetime | None = None) -> dict:
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)
    week_start_utc, week_end_utc, _ = affiliate_week_window_utc_from_reference(now_utc_ts)
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
