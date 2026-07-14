"""Incremental XP snapshot settlement.

Replaces the old "re-aggregate all of xp_events every 5 minutes" job with a
cursor-based incremental pass: each run only reads xp_events inserted (or
invalidated) since the last run, and applies deltas to users.total_xp /
weekly_xp / monthly_xp with $inc. xp_events remains the immutable source of
truth; this module only maintains derived counters.

Correctness properties (see docs/xp_snapshot_incremental.md for the full
write-up):
  * Idempotent: each user update is guarded by a per-user watermark
    (``xp_snapshot_cursor`` / ``xp_snapshot_correction_cursor``) so replaying
    the same batch (crash-retry, or two workers racing) never double-applies
    an increment — the guarded update simply matches 0 documents the second
    time.
  * Crash-safe: every single-document update (the unit of atomicity in
    MongoDB) either fully applies or doesn't apply at all, so a crash
    mid-batch leaves a consistent (partially-advanced) state that a retry
    can safely complete.
  * Reversible XP (admin rollback / invalidation) is picked up via a
    dedicated correction pass keyed off ``invalidated_at`` using a small
    partial index, independent of collection size.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from pymongo import UpdateOne

from config import KL_TZ
from database import safe_create_index

logger = logging.getLogger(__name__)

CURSOR_ID = "xp_cursor"
DEFAULT_BATCH_LIMIT = 2000


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _week_start_kl(reference: datetime) -> datetime:
    ref_local = reference.astimezone(KL_TZ)
    return (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _week_window_utc(reference: datetime) -> tuple[datetime, datetime]:
    start_local = _week_start_kl(reference)
    end_local = start_local + timedelta(days=7)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _month_start_kl(reference: datetime) -> datetime:
    ref_local = reference.astimezone(KL_TZ)
    return ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _month_window_utc(reference: datetime) -> tuple[datetime, datetime]:
    start_local = _month_start_kl(reference)
    if start_local.month == 12:
        end_local = start_local.replace(year=start_local.year + 1, month=1)
    else:
        end_local = start_local.replace(month=start_local.month + 1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def ensure_xp_snapshot_indexes(db) -> None:
    """Small, targeted indexes for the incremental pass only.

    The cursor range scan on xp_events uses the default ``_id`` index. The
    only new index needed is a partial one for the (rare) correction scan,
    bounded to invalidated docs regardless of total collection size.
    """
    safe_create_index(
        db.xp_events,
        [("invalidated", 1), ("invalidated_at", 1)],
        name="xp_events_invalidated_at_idx",
        partialFilterExpression={"invalidated": True},
    )


def _get_cursor(db) -> dict | None:
    return db.xp_snapshot_state.find_one({"_id": CURSOR_ID})


def bootstrap_cursor_if_missing(db, now_utc_ts: datetime) -> dict:
    """One-time migration boundary.

    Runs the legacy full-history rebuild exactly once (so existing totals
    are known-correct), then pins the incremental cursor to the current max
    ``xp_events._id`` so no historical event is ever replayed into already
    -populated counters. Every user gets stamped with that same cursor value
    so the per-user idempotency guard is consistent from day one.
    """
    existing = _get_cursor(db)
    if existing:
        return existing

    from scheduler import _settle_xp_snapshots_full_rebuild

    logger.info("[XP_SNAPSHOT][MIGRATION] bootstrap_start")
    _settle_xp_snapshots_full_rebuild(now_utc_ts=now_utc_ts)

    last_doc = db.xp_events.find({}, {"_id": 1}).sort("_id", -1).limit(1)
    last_doc = next(iter(last_doc), None)
    last_event_id = last_doc["_id"] if last_doc else None

    week_start_utc, _ = _week_window_utc(now_utc_ts)
    month_start_utc, _ = _month_window_utc(now_utc_ts)

    cursor_doc = {
        "_id": CURSOR_ID,
        "last_event_id": last_event_id,
        "last_correction_at": now_utc_ts,
        "week_key": week_start_utc.date().isoformat(),
        "month_key": month_start_utc.date().isoformat(),
        "created_at": now_utc_ts,
        "updated_at": now_utc_ts,
    }
    db.xp_snapshot_state.update_one({"_id": CURSOR_ID}, {"$setOnInsert": cursor_doc}, upsert=True)
    db.users.update_many(
        {},
        {"$set": {"xp_snapshot_cursor": last_event_id, "xp_snapshot_correction_cursor": now_utc_ts}},
    )
    # Stamp every event the full rebuild already summed as xp_counted so a
    # later invalidation of a pre-migration event is still eligible for the
    # correction pass (otherwise it would look "never counted" and be
    # silently skipped, leaking XP that should have been reversed).
    if last_event_id is not None:
        db.xp_events.update_many(
            {
                "_id": {"$lte": last_event_id},
                "user_id": {"$ne": None},
                "$or": [{"invalidated": {"$exists": False}}, {"invalidated": False}],
            },
            {"$set": {"xp_counted": True}},
        )
        db.xp_events.update_many(
            {"_id": {"$lte": last_event_id}, "invalidated": True},
            {"$set": {"xp_counted": False}},
        )
    logger.info("[XP_SNAPSHOT][MIGRATION] bootstrap_done last_event_id=%s", last_event_id)
    return _get_cursor(db) or cursor_doc


def settle_xp_snapshots_incremental(
    db, now_utc_ts: datetime | None = None, batch_limit: int = DEFAULT_BATCH_LIMIT
) -> dict:
    now_utc_ts = now_utc_ts or now_utc()
    run_started = time.monotonic()
    ensure_xp_snapshot_indexes(db)
    cursor = bootstrap_cursor_if_missing(db, now_utc_ts)

    week_start_utc, week_end_utc = _week_window_utc(now_utc_ts)
    month_start_utc, month_end_utc = _month_window_utc(now_utc_ts)
    week_key = week_start_utc.date().isoformat()
    month_key = month_start_utc.date().isoformat()

    if cursor.get("week_key") != week_key:
        db.users.update_many({}, {"$set": {"weekly_xp": 0}})
        db.xp_snapshot_state.update_one({"_id": CURSOR_ID}, {"$set": {"week_key": week_key}})
        logger.info("[XP_SNAPSHOT] weekly_rollover week_key=%s", week_key)
    if cursor.get("month_key") != month_key:
        db.users.update_many({}, {"$set": {"monthly_xp": 0}})
        db.xp_snapshot_state.update_one({"_id": CURSOR_ID}, {"$set": {"month_key": month_key}})
        logger.info("[XP_SNAPSHOT] monthly_rollover month_key=%s", month_key)

    cursor = _get_cursor(db) or cursor
    last_event_id = cursor.get("last_event_id")

    query: dict = {}
    if last_event_id is not None:
        query["_id"] = {"$gt": last_event_id}
    events = list(
        db.xp_events.find(
            query, {"_id": 1, "user_id": 1, "xp": 1, "created_at": 1, "ts": 1, "invalidated": 1}
        )
        .sort("_id", 1)
        .limit(batch_limit)
    )

    scanned = len(events)
    processed = 0
    skipped_invalid = 0
    new_max_id = last_event_id
    per_user_delta: dict = {}
    event_marks: list[tuple] = []

    for ev in events:
        eid = ev["_id"]
        new_max_id = eid
        uid = ev.get("user_id")
        if uid is None:
            continue
        invalidated = bool(ev.get("invalidated"))
        if invalidated:
            skipped_invalid += 1
            event_marks.append((eid, False))
            continue

        processed += 1
        amount = int(ev.get("xp", 0) or 0)
        ts_utc = _coerce_utc(ev.get("created_at") or ev.get("ts"))
        entry = per_user_delta.setdefault(uid, {"total": 0, "weekly": 0, "monthly": 0, "max_id": eid})
        entry["total"] += amount
        entry["max_id"] = eid
        if ts_utc is not None:
            if week_start_utc <= ts_utc < week_end_utc:
                entry["weekly"] += amount
            if month_start_utc <= ts_utc < month_end_utc:
                entry["monthly"] += amount
        event_marks.append((eid, True))

    modified_users = 0
    for uid, delta in per_user_delta.items():
        inc_fields = {}
        if delta["total"]:
            inc_fields["total_xp"] = delta["total"]
            inc_fields["xp"] = delta["total"]
        if delta["weekly"]:
            inc_fields["weekly_xp"] = delta["weekly"]
        if delta["monthly"]:
            inc_fields["monthly_xp"] = delta["monthly"]
        update: dict = {"$set": {"xp_snapshot_cursor": delta["max_id"], "snapshot_updated_at": now_utc_ts}}
        if inc_fields:
            update["$inc"] = inc_fields
        res = db.users.update_one(
            {
                "user_id": uid,
                "$or": [
                    {"xp_snapshot_cursor": {"$exists": False}},
                    {"xp_snapshot_cursor": {"$lt": delta["max_id"]}},
                ],
            },
            update,
            upsert=True,
        )
        if getattr(res, "modified_count", 0) or getattr(res, "upserted_id", None):
            modified_users += 1

    if event_marks:
        ops = [
            UpdateOne({"_id": eid, "xp_counted": {"$exists": False}}, {"$set": {"xp_counted": counted}})
            for eid, counted in event_marks
        ]
        db.xp_events.bulk_write(ops, ordered=False)

    if new_max_id is not None and new_max_id != last_event_id:
        db.xp_snapshot_state.update_one(
            {"_id": CURSOR_ID, "last_event_id": last_event_id},
            {"$set": {"last_event_id": new_max_id, "updated_at": now_utc_ts}},
        )

    corrections_applied = _apply_invalidation_corrections(
        db,
        now_utc_ts=now_utc_ts,
        week_start_utc=week_start_utc,
        week_end_utc=week_end_utc,
        month_start_utc=month_start_utc,
        month_end_utc=month_end_utc,
        batch_limit=batch_limit,
    )

    # Cheap, user-count-scaled "publish" touch — preserves existing
    # freshness-monitoring behavior (snapshot_updated_at / snapshot_version)
    # without re-scanning xp_events history.
    db.users.update_many(
        {}, {"$set": {"snapshot_updated_at": now_utc_ts, "snapshot_published_at": now_utc_ts}, "$inc": {"snapshot_version": 1}}
    )
    _write_snapshot_heartbeat(db, now_utc_ts)

    elapsed_ms = int((time.monotonic() - run_started) * 1000)
    summary = {
        "scanned": scanned,
        "processed": processed,
        "skipped_invalid": skipped_invalid,
        "modified_users": modified_users,
        "corrections_applied": corrections_applied,
        "cursor_before": str(last_event_id) if last_event_id else None,
        "cursor_after": str(new_max_id) if new_max_id else None,
        "elapsed_ms": elapsed_ms,
    }
    logger.info(
        "[XP_SNAPSHOT][INCREMENTAL] scanned=%s processed=%s skipped_invalid=%s modified_users=%s "
        "corrections=%s cursor_before=%s cursor_after=%s elapsed_ms=%s",
        scanned,
        processed,
        skipped_invalid,
        modified_users,
        corrections_applied,
        summary["cursor_before"],
        summary["cursor_after"],
        elapsed_ms,
    )
    return summary


def _apply_invalidation_corrections(
    db,
    *,
    now_utc_ts: datetime,
    week_start_utc: datetime,
    week_end_utc: datetime,
    month_start_utc: datetime,
    month_end_utc: datetime,
    batch_limit: int,
) -> int:
    """Reverse XP for events that were counted, then invalidated afterward.

    Bounded by the partial index on {invalidated, invalidated_at}: touches
    only currently-invalidated docs, never the full xp_events history.
    """
    cursor = _get_cursor(db) or {}
    last_correction_at = cursor.get("last_correction_at") or now_utc_ts

    correction_docs = list(
        db.xp_events.find(
            {
                "invalidated": True,
                "invalidated_at": {"$gt": last_correction_at},
                "xp_counted": True,
            },
            {"_id": 1, "user_id": 1, "xp": 1, "created_at": 1, "ts": 1, "invalidated_at": 1},
        )
        .sort("invalidated_at", 1)
        .limit(batch_limit)
    )

    corrections_applied = 0
    new_correction_at = last_correction_at
    for ev in correction_docs:
        inv_at = _coerce_utc(ev.get("invalidated_at")) or now_utc_ts
        if inv_at > new_correction_at:
            new_correction_at = inv_at
        uid = ev.get("user_id")
        if uid is None:
            continue
        amount = int(ev.get("xp", 0) or 0)
        if not amount:
            continue
        ts_utc = _coerce_utc(ev.get("created_at") or ev.get("ts"))
        inc_fields = {"total_xp": -amount, "xp": -amount}
        if ts_utc is not None:
            if week_start_utc <= ts_utc < week_end_utc:
                inc_fields["weekly_xp"] = -amount
            if month_start_utc <= ts_utc < month_end_utc:
                inc_fields["monthly_xp"] = -amount

        res = db.users.update_one(
            {
                "user_id": uid,
                "$or": [
                    {"xp_snapshot_correction_cursor": {"$exists": False}},
                    {"xp_snapshot_correction_cursor": {"$lt": inv_at}},
                ],
            },
            {"$inc": inc_fields, "$set": {"xp_snapshot_correction_cursor": inv_at, "snapshot_updated_at": now_utc_ts}},
        )
        if getattr(res, "modified_count", 0) or getattr(res, "upserted_id", None):
            corrections_applied += 1

    if new_correction_at != last_correction_at:
        db.xp_snapshot_state.update_one({"_id": CURSOR_ID}, {"$set": {"last_correction_at": new_correction_at}})

    return corrections_applied


def _write_snapshot_heartbeat(db, now_utc_ts: datetime) -> None:
    try:
        db.admin_cache.update_one(
            {"_id": "snapshot_heartbeat"},
            {"$set": {"ts_utc": now_utc_ts, "ts_kl": now_utc_ts.astimezone(KL_TZ), "source": "xp"}},
            upsert=True,
        )
    except Exception:
        logger.exception("[XP_SNAPSHOT][HEARTBEAT] failed")
