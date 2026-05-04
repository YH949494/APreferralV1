from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pymongo import ASCENDING


RETENTION_COLLECTION = "retention_cohort_kpis"


def month_start(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def add_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def diagnosis_for(doc: dict[str, Any]) -> str:
    cohort_size = int(doc.get("cohort_size", 0) or 0)
    d7_eligible = int(doc.get("d7_eligible", 0) or 0)
    d14_eligible = int(doc.get("d14_eligible", 0) or 0)
    d30_eligible = int(doc.get("d30_eligible", 0) or 0)
    d7 = float(doc.get("d7_retention_rate", 0.0) or 0.0)
    d14 = float(doc.get("d14_retention_rate", 0.0) or 0.0)
    d30 = float(doc.get("d30_retention_rate", 0.0) or 0.0)

    if cohort_size == 0:
        return "No cohort data"
    if d7_eligible > 0 and d7 < 0.15:
        return "Activation problem"
    if d14_eligible > 0 and d7 > 0 and (d14 / d7) < 0.5:
        return "Week-2 drop"
    if d30_eligible > 0 and d14 > 0 and (d30 / d14) < 0.5:
        return "Long-term habit problem"
    if d30_eligible > 0 and d30 >= 0.10:
        return "Healthy cohort"
    return "Monitor"


def ensure_retention_indexes(db) -> None:
    db[RETENTION_COLLECTION].create_index([("cohort_month", ASCENDING)], unique=True)
    db.users.create_index([("joined_main_at", ASCENDING)])
    try:
        db.voucher_claims.create_index([("user_id", ASCENDING), ("created_at", ASCENDING)])
    except Exception:
        pass
    try:
        db.xp_events.create_index([("user_id", ASCENDING), ("type", ASCENDING), ("created_at", ASCENDING)])
    except Exception:
        pass
    try:
        db.xp_events.create_index([("user_id", ASCENDING), ("created_at", ASCENDING)])
    except Exception:
        pass
    try:
        db.new_joiner_claims.create_index([("uid", ASCENDING), ("claimed_at", ASCENDING)])
    except Exception:
        pass
    try:
        db.new_joiner_claims.create_index([("user_id", ASCENDING), ("claimed_at", ASCENDING)])
    except Exception:
        pass
    try:
        db.miniapp_sessions_daily.create_index([("user_id", ASCENDING), ("date_utc", ASCENDING)])
    except Exception:
        pass
    try:
        db.miniapp_sessions_daily.create_index([("uid", ASCENDING), ("date_utc", ASCENDING)])
    except Exception:
        pass


def _rate(n: int, d: int) -> float:
    return float(n) / float(d) if d > 0 else 0.0


def _coerce_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            try:
                d = datetime.strptime(raw, "%Y-%m-%d")
                return d.replace(tzinfo=timezone.utc, hour=12, minute=0, second=0, microsecond=0)
            except Exception:
                return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _bounded_or_timestamp_filter(fields: tuple[str, ...], start: datetime, end: datetime) -> dict[str, Any]:
    return {"$or": [{f: {"$gte": start, "$lt": end}} for f in fields]}


def compute_retention_for_month(db, cohort_start: datetime, now_utc: datetime) -> dict[str, Any]:
    cohort_end = add_months(cohort_start, 1)
    activity_start = cohort_start + timedelta(days=7)
    activity_end = cohort_end + timedelta(days=31)
    cohort_month = cohort_start.strftime("%Y-%m")
    users = list(db.users.find(
        {"joined_main_at": {"$gte": cohort_start, "$lt": cohort_end}},
        {"_id": 0, "user_id": 1, "joined_main_at": 1, "first_checkin_at": 1, "last_visible_at": 1},
    ))
    by_uid = {}
    for u in users:
        uid = u.get("user_id")
        joined = u.get("joined_main_at")
        if uid is None or not isinstance(joined, datetime):
            continue
        by_uid[int(uid)] = u
    uids = list(by_uid.keys())

    activity_by_uid: dict[int, set[datetime]] = {uid: set() for uid in uids}
    def _append(uid_value: Any, ts: Any) -> None:
        try:
            uid_int = int(uid_value)
        except Exception:
            return
        if uid_int in activity_by_uid and isinstance(ts, datetime):
            activity_by_uid[uid_int].add(ts)

    def _pick_ts(row: dict, fields: tuple[str, ...]) -> datetime | None:
        for f in fields:
            val = _coerce_dt(row.get(f))
            if val is not None:
                if f in ("date_utc", "date", "day") and val.hour == 0 and val.minute == 0 and val.second == 0 and val.microsecond == 0:
                    val = val.replace(hour=12)
                return val
        return None

    if uids:
        try:
            xp_cur = db.xp_events.find(
                {
                    "user_id": {"$in": uids},
                    "$and": [
                        {
                            "$or": [
                                {"created_at": {"$gte": activity_start, "$lt": activity_end}},
                                {"createdAt": {"$gte": activity_start, "$lt": activity_end}},
                                {"ts": {"$gte": activity_start, "$lt": activity_end}},
                            ]
                        },
                        {
                            "$or": [
                                {"type": "checkin"},
                                {"unique_key": {"$regex": r"^checkin:"}},
                            ]
                        },
                    ],
                },
                {"_id": 0, "user_id": 1, "created_at": 1, "createdAt": 1, "ts": 1},
            )
            for row in xp_cur:
                _append(row.get("user_id"), _pick_ts(row, ("created_at", "createdAt", "ts")))
        except Exception:
            pass
        try:
            cur = db.voucher_claims.find(
                {"user_id": {"$in": uids}, **_bounded_or_timestamp_filter(("claimed_at", "claimedAt", "created_at", "createdAt", "ts"), activity_start, activity_end)},
                {"_id": 0, "user_id": 1, "claimed_at": 1, "claimedAt": 1, "created_at": 1, "createdAt": 1, "ts": 1},
            )
            for row in cur:
                _append(row.get("user_id"), _pick_ts(row, ("claimed_at", "claimedAt", "created_at", "createdAt", "ts")))
        except Exception:
            pass
        try:
            nj_cur = db.new_joiner_claims.find(
                {
                    "$or": [{"uid": {"$in": uids}}, {"user_id": {"$in": uids}}],
                    **_bounded_or_timestamp_filter(("claimed_at", "claimedAt", "created_at", "createdAt", "ts"), activity_start, activity_end),
                },
                {"_id": 0, "uid": 1, "user_id": 1, "claimed_at": 1, "claimedAt": 1, "created_at": 1, "createdAt": 1, "ts": 1},
            )
            for row in nj_cur:
                uid_val = row.get("uid")
                if uid_val is None:
                    uid_val = row.get("user_id")
                _append(uid_val, _pick_ts(row, ("claimed_at", "claimedAt", "created_at", "createdAt", "ts")))
        except Exception:
            pass
        try:
            ms_cur = db.miniapp_sessions_daily.find(
                {
                    "$or": [{"user_id": {"$in": uids}}, {"uid": {"$in": uids}}],
                    **_bounded_or_timestamp_filter(("date_utc", "date", "day", "ts", "created_at"), activity_start, activity_end),
                },
                {"_id": 0, "user_id": 1, "uid": 1, "date_utc": 1, "date": 1, "day": 1, "ts": 1, "created_at": 1},
            )
            for row in ms_cur:
                uid_val = row.get("user_id")
                if uid_val is None:
                    uid_val = row.get("uid")
                ts = _pick_ts(row, ("date_utc", "date", "day", "ts", "created_at"))
                _append(uid_val, ts)
        except Exception:
            pass

    out = {
        "cohort_month": cohort_month,
        "cohort_start_utc": cohort_start,
        "cohort_end_utc": cohort_end,
        "cohort_size": len(by_uid),
    }

    for day in (7, 14, 30):
        eligible = 0
        retained = 0
        for uid, u in by_uid.items():
            joined = u.get("joined_main_at")
            if not isinstance(joined, datetime):
                continue
            age = now_utc - joined
            if age < timedelta(days=day):
                continue
            eligible += 1
            win_start = joined + timedelta(days=day)
            win_end = win_start + timedelta(days=1)
            hits = list(activity_by_uid.get(uid, set()))
            for field in ("first_checkin_at", "last_visible_at"):
                ts = u.get(field)
                if isinstance(ts, datetime):
                    hits.append(ts)
            if any((h >= win_start and h < win_end) for h in hits):
                retained += 1
        out[f"d{day}_eligible"] = eligible
        out[f"d{day}_retained"] = retained
        out[f"d{day}_retention_rate"] = _rate(retained, eligible)

    out["diagnosis"] = diagnosis_for(out)
    out["computed_at_utc"] = now_utc
    return out


def compute_retention_kpis(db, months: int = 12, now_utc: datetime | None = None) -> list[dict[str, Any]]:
    now_utc = now_utc or datetime.now(timezone.utc)
    ensure_retention_indexes(db)
    start_this_month = month_start(now_utc)
    docs = []
    for i in range(months):
        cohort_start = add_months(start_this_month, -(months - 1 - i))
        doc = compute_retention_for_month(db, cohort_start, now_utc)
        db[RETENTION_COLLECTION].update_one(
            {"cohort_month": doc["cohort_month"]},
            {"$set": doc},
            upsert=True,
        )
        docs.append(doc)
    return docs
