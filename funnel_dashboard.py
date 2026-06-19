"""Activation Funnel Dashboard — backend computation module.

Stages:
  1.  Join Channel       — joined_main_at in window
  2.  Start Bot          — first_private_interaction_at
  3.  First Check-in     — ≥1 distinct check-in day
  4.  Welcome Progress 1/3 — ≥1 distinct check-in day (alias of 3)
  5.  Welcome Progress 2/3 — ≥2 distinct check-in days
  6.  Welcome Progress 3/3 — ≥3 distinct check-in days (= unlock eligible)
  7.  Welcome Unlock     — eligible to claim welcome voucher
  8.  Welcome Claim      — voucher successfully claimed
  9.  First Bet          — after_total_bet_amount > 0 (heuristic via coupon identity)
  10. D7 Retention       — activity on day 7 post-join (eligible-gated)
  11. D30 Retention      — activity on day 30 post-join (eligible-gated)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


# ---- helpers ----

def _coerce_uid(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _rate(n: int, d: int) -> float:
    return round(100.0 * n / d, 1) if d > 0 else 0.0


def _coerce_float(v: Any) -> float:
    """Coerce a value that may be a number or a CSV-string to float.

    Returns 0.0 for None, empty string, or any non-numeric value so callers
    can safely compare against 0 without special-casing the type.
    """
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (ValueError, TypeError):
        return 0.0


def _is_explicit_true(v: Any) -> bool:
    """Return True only for an unambiguously truthy is_new_player value."""
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y")
    return v in (True, 1)


def _is_explicit_false(v: Any) -> bool:
    """Return True only for an unambiguously falsy is_new_player value."""
    if isinstance(v, str):
        return v.strip().lower() in ("0", "false", "no", "n")
    return v in (False, 0)


def _date_range_filter(field: str, start: datetime | None, end: datetime) -> dict:
    """MongoDB filter constraining *field* to the half-open interval [start, end).

    Uses $lt (exclusive upper bound) so callers can pass midnight of the next
    day when normalising a date-only custom date_to input, without accidentally
    including events timestamped at exactly that midnight.
    """
    f: dict = {"$lt": end}
    if start is not None:
        f["$gte"] = start
    return {field: f}


def _coerce_dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    if isinstance(v, str):
        raw = v.strip()
        if not raw:
            return None
        if len(raw) == 10:
            try:
                return datetime.strptime(raw, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc, hour=12
                )
            except Exception:
                pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _build_stage(
    stage_id: str,
    name: str,
    count: int | None,
    top_count: int,
    prev_count: int | None,
    *,
    data_quality: str = "exact",
    note: str | None = None,
) -> dict:
    s: dict = {
        "id": stage_id,
        "name": name,
        "count": count,
        "data_quality": data_quality,
    }
    if note:
        s["note"] = note

    if count is None:
        s["conversion_from_top"] = None
        s["dropoff_from_top"] = None
        s["prev_stage_conversion"] = None
        s["prev_stage_dropoff"] = None
        return s

    top = max(top_count, 0)
    conv = _rate(count, top)
    s["conversion_from_top"] = conv
    s["dropoff_from_top"] = round(100.0 - conv, 1) if top > 0 else 0.0

    if prev_count is not None and prev_count > 0:
        pc = _rate(count, prev_count)
        s["prev_stage_conversion"] = pc
        s["prev_stage_dropoff"] = round(100.0 - pc, 1)
    else:
        s["prev_stage_conversion"] = None
        s["prev_stage_dropoff"] = None

    if count > top > 0:
        s["data_quality"] = "invalid"
        s["note"] = "Count exceeds top of funnel — query needs audit."

    return s


# ---- retention helper ----

def _compute_retention_for_cohort(
    db,
    cohort_by_uid: dict[int, dict],
    now: datetime,
    day: int,
) -> tuple[int | None, int]:
    """Returns (retained_count, eligible_count) for Dn retention."""
    eligible: dict[int, datetime] = {}
    for uid, doc in cohort_by_uid.items():
        joined = _coerce_dt(doc.get("joined_main_at"))
        if joined and (now - joined) >= timedelta(days=day):
            eligible[uid] = joined

    if not eligible:
        return None, 0

    eligible_list = list(eligible.keys())
    retained: set[int] = set()

    def _check(uid_val: Any, ts_val: Any) -> None:
        uid = _coerce_uid(uid_val)
        if uid not in eligible:
            return
        ts = _coerce_dt(ts_val)
        if ts is None:
            return
        joined = eligible[uid]
        win_start = joined + timedelta(days=day)
        win_end = joined + timedelta(days=day + 1)
        if win_start <= ts < win_end:
            retained.add(uid)

    try:
        for doc in db.xp_events.find(
            {
                "user_id": {"$in": eligible_list},
                "$or": [
                    {"type": "checkin"},
                    {"reason": "checkin"},
                    {"unique_key": {"$regex": r"^checkin:"}},
                ],
            },
            {"user_id": 1, "created_at": 1, "createdAt": 1, "ts": 1},
        ):
            ts = doc.get("created_at") or doc.get("createdAt") or doc.get("ts")
            _check(doc.get("user_id"), ts)
    except Exception:
        pass

    try:
        for doc in db.voucher_claims.find(
            {"user_id": {"$in": eligible_list}},
            {"user_id": 1, "claimed_at": 1, "claimedAt": 1, "created_at": 1},
        ):
            ts = doc.get("claimed_at") or doc.get("claimedAt") or doc.get("created_at")
            _check(doc.get("user_id"), ts)
    except Exception:
        pass

    try:
        for doc in db.miniapp_sessions_daily.find(
            {"$or": [{"user_id": {"$in": eligible_list}}, {"uid": {"$in": eligible_list}}]},
            {"user_id": 1, "uid": 1, "date_utc": 1, "date": 1, "ts": 1, "created_at": 1},
        ):
            uid_val = doc.get("user_id") or doc.get("uid")
            ts = (
                doc.get("date_utc") or doc.get("date") or
                doc.get("ts") or doc.get("created_at")
            )
            _check(uid_val, ts)
    except Exception:
        pass

    return len(retained), len(eligible)


# ---- main computation ----

def compute_funnel(
    db,
    start: datetime | None,
    end: datetime,
    now: datetime,
) -> dict:
    """Full activation funnel computation for users who joined in [start, end]."""

    # ---- Cohort ----
    cohort_q: dict = {"joined_main_at": {"$lt": end}}
    if start is not None:
        cohort_q["joined_main_at"]["$gte"] = start

    cohort_docs = list(db.users.find(
        cohort_q,
        {"user_id": 1, "joined_main_at": 1, "first_private_interaction_at": 1},
    ))

    cohort_by_uid: dict[int, dict] = {}
    for doc in cohort_docs:
        uid = _coerce_uid(doc.get("user_id"))
        if uid is not None:
            cohort_by_uid[uid] = doc

    cohort_uids = set(cohort_by_uid.keys())
    cohort_list = list(cohort_uids)
    join_count = len(cohort_uids)

    # ---- Stage 2: Start Bot ----
    # Bound by [start, end] so historical custom windows don't count later activity.
    start_bot_uids: set[int] = set()
    if cohort_uids:
        bot_ts: dict = {"$exists": True, "$ne": None, "$lt": end}
        if start is not None:
            bot_ts["$gte"] = start
        for doc in db.users.find(
            {"user_id": {"$in": cohort_list}, "first_private_interaction_at": bot_ts},
            {"user_id": 1},
        ):
            uid = _coerce_uid(doc.get("user_id"))
            if uid in cohort_uids:
                start_bot_uids.add(uid)
    start_bot_count = len(start_bot_uids)

    # ---- Stages 3–6: Check-in progress ----
    # Count distinct check-in days per user in the window
    checkin_days_by_uid: dict[int, set[str]] = {uid: set() for uid in cohort_uids}

    if cohort_uids:
        # Apply both lower and upper bounds so custom historical windows don't pull
        # check-ins that occurred after the selected end date.
        checkin_ts: dict = {"$lt": end}
        if start is not None:
            checkin_ts["$gte"] = start
        checkin_q: dict = {
            "user_id": {"$in": cohort_list},
            "created_at": checkin_ts,
            "$or": [
                {"type": "checkin"},
                {"reason": "checkin"},
                {"unique_key": {"$regex": r"^checkin:"}},
            ],
        }

        for doc in db.xp_events.find(checkin_q, {"user_id": 1, "created_at": 1}):
            uid = _coerce_uid(doc.get("user_id"))
            if uid not in checkin_days_by_uid:
                continue
            ts = _coerce_dt(doc.get("created_at"))
            if ts:
                checkin_days_by_uid[uid].add(ts.strftime("%Y-%m-%d"))

    progress_1_count = sum(1 for d in checkin_days_by_uid.values() if len(d) >= 1)
    progress_2_count = sum(1 for d in checkin_days_by_uid.values() if len(d) >= 2)
    progress_3_count = sum(1 for d in checkin_days_by_uid.values() if len(d) >= 3)

    # ---- Stage 7: Welcome Unlock ----
    eligible_uids: set[int] = set()
    if cohort_uids:
        for doc in db.welcome_eligibility.find(
            {"$or": [{"uid": {"$in": cohort_list}}, {"user_id": {"$in": cohort_list}}]},
            {"uid": 1, "user_id": 1},
        ):
            uid = _coerce_uid(doc.get("uid") or doc.get("user_id"))
            if uid in cohort_uids:
                eligible_uids.add(uid)

    welcome_unlock_count = max(progress_3_count, len(eligible_uids))

    # ---- Stage 8: Welcome Claim ----
    # Use {"$not": {"$gte": end}} so documents without a timestamp field are included
    # (legacy records) while claims that occurred after end are excluded.
    claim_uids: set[int] = set()
    if cohort_uids:
        # affiliate_ledger WELCOME ISSUED
        for doc in db.affiliate_ledger.find(
            {
                "status": "ISSUED",
                "user_id": {"$in": cohort_list},
                "updated_at": {"$not": {"$gte": end}},
                "$or": [
                    {"ledger_type": "WELCOME"},
                    {"tier": "WELCOME"},
                    {"pool_id": "WELCOME"},
                ],
            },
            {"user_id": 1},
        ):
            uid = _coerce_uid(doc.get("user_id"))
            if uid in cohort_uids:
                claim_uids.add(uid)

        # welcome_eligibility claimed=True
        for doc in db.welcome_eligibility.find(
            {
                "claimed": True,
                "claimed_at": {"$not": {"$gte": end}},
                "$or": [
                    {"uid": {"$in": cohort_list}},
                    {"user_id": {"$in": cohort_list}},
                ],
            },
            {"uid": 1, "user_id": 1},
        ):
            uid = _coerce_uid(doc.get("uid") or doc.get("user_id"))
            if uid in cohort_uids:
                claim_uids.add(uid)

        # new_joiner_claims
        for doc in db.new_joiner_claims.find(
            {
                "claimed_at": {"$not": {"$gte": end}},
                "$or": [{"uid": {"$in": cohort_list}}, {"user_id": {"$in": cohort_list}}],
            },
            {"uid": 1, "user_id": 1},
        ):
            uid = _coerce_uid(doc.get("uid") or doc.get("user_id"))
            if uid in cohort_uids:
                claim_uids.add(uid)

        # welcome_tickets claimed
        for doc in db.welcome_tickets.find(
            {
                "status": "claimed",
                "claimed_at": {"$not": {"$gte": end}},
                "$or": [
                    {"uid": {"$in": cohort_list}},
                    {"user_id": {"$in": cohort_list}},
                ],
            },
            {"uid": 1, "user_id": 1},
        ):
            uid = _coerce_uid(doc.get("uid") or doc.get("user_id"))
            if uid in cohort_uids:
                claim_uids.add(uid)

    welcome_claim_count = len(claim_uids)

    # ---- Stage 9: First Bet ----
    first_bet_uids: set[int] = set()
    first_bet_quality = "heuristic"
    first_bet_note = (
        "Via coupon_code → voucher_claims identity resolution. "
        "Users without coupon claims are excluded from count."
    )
    if cohort_uids:
        try:
            code_to_uid: dict[str, int] = {}
            for doc in db.voucher_claims.find(
                {"user_id": {"$in": cohort_list}},
                {"user_id": 1, "coupon_code": 1, "voucher_code": 1},
            ):
                code = doc.get("coupon_code") or doc.get("voucher_code")
                uid = _coerce_uid(doc.get("user_id"))
                if code and uid and uid in cohort_uids:
                    code_to_uid[str(code)] = uid

            if code_to_uid:
                # Fetch all matched records without a server-side numeric filter because
                # after_total_bet_amount is often stored as a string from CSV uploads;
                # {"$gt": 0} would silently skip those rows. Convert to float in Python.
                mkt_q: dict = {"coupon_code": {"$in": list(code_to_uid.keys())}}
                end_date_str = end.strftime("%Y-%m-%d")
                if start is not None:
                    start_date_str = start.strftime("%Y-%m-%d")
                    mkt_q["$or"] = [
                        {"week_start": {"$gte": start_date_str, "$lt": end_date_str}},
                        {"created_at": {"$gte": start, "$lt": end}},
                    ]
                else:
                    mkt_q["$or"] = [
                        {"week_start": {"$lt": end_date_str}},
                        {"created_at": {"$lt": end}},
                    ]
                for doc in db.marketing_raw_data.find(
                    mkt_q, {"coupon_code": 1, "after_total_bet_amount": 1}
                ):
                    bet_val = _coerce_float(
                        doc.get("after_total_bet_amount") or doc.get("after_bet_amount")
                    )
                    if bet_val <= 0:
                        continue
                    code = str(doc.get("coupon_code") or "")
                    uid = code_to_uid.get(code)
                    if uid:
                        first_bet_uids.add(uid)
        except Exception:
            first_bet_quality = "missing"
            first_bet_note = "First Bet query failed — check marketing_raw_data connectivity."

    first_bet_count: int | None = len(first_bet_uids) if first_bet_uids else None
    if first_bet_count == 0:
        first_bet_count = None
        first_bet_note = (
            "No first bet signal found for this cohort. "
            "Requires voucher-claimed users with matching marketing data (after_total_bet_amount > 0)."
        )

    # ---- Stages 10–11: D7 / D30 Retention ----
    d7_retained, d7_eligible = _compute_retention_for_cohort(db, cohort_by_uid, now, 7)
    d30_retained, d30_eligible = _compute_retention_for_cohort(db, cohort_by_uid, now, 30)

    # ---- Assemble stages ----
    # Sequence: join → start_bot → checkin(1/3) → 2/3 → 3/3 → unlock → claim → first_bet
    ordered: list[dict] = [
        _build_stage("join_channel", "Join Channel", join_count, join_count, None,
                     data_quality="exact"),
        _build_stage("start_bot", "Start Bot", start_bot_count, join_count, join_count,
                     data_quality="exact",
                     note="Counted via first_private_interaction_at from /start or first private message."),
        _build_stage("first_checkin", "First Check-in (1/3)", progress_1_count,
                     join_count, start_bot_count, data_quality="exact"),
        _build_stage("progress_2_3", "Welcome Progress 2/3", progress_2_count,
                     join_count, progress_1_count, data_quality="exact"),
        _build_stage("progress_3_3", "Welcome Progress 3/3", progress_3_count,
                     join_count, progress_2_count, data_quality="exact"),
        _build_stage("welcome_unlock", "Welcome Unlock", welcome_unlock_count,
                     join_count, progress_3_count, data_quality="heuristic",
                     note="Based on ≥3 distinct check-in days or welcome_eligibility record."),
        _build_stage("welcome_claim", "Welcome Claim", welcome_claim_count,
                     join_count, welcome_unlock_count, data_quality="exact",
                     note="Aggregated from affiliate_ledger, welcome_eligibility, new_joiner_claims, welcome_tickets."),
        _build_stage("first_bet", "First Bet", first_bet_count,
                     join_count, welcome_claim_count,
                     data_quality=first_bet_quality, note=first_bet_note),
    ]

    # D7 retention
    if d7_retained is not None and d7_eligible > 0:
        d7_s = _build_stage(
            "d7_retention", "D7 Retention",
            d7_retained, d7_eligible, d7_eligible,
            data_quality="heuristic",
            note=f"Activity on day 7 post-join. Eligible: {d7_eligible} users joined ≥7 days ago.",
        )
        d7_s["eligible_count"] = d7_eligible
        d7_s["retention_rate"] = _rate(d7_retained, d7_eligible)
        ordered.append(d7_s)
    else:
        ordered.append({
            "id": "d7_retention",
            "name": "D7 Retention",
            "count": None,
            "data_quality": "missing",
            "note": "No cohort users are ≥7 days old yet.",
            "conversion_from_top": None,
            "dropoff_from_top": None,
            "prev_stage_conversion": None,
            "prev_stage_dropoff": None,
            "eligible_count": 0,
            "retention_rate": None,
        })

    if d30_retained is not None and d30_eligible > 0:
        d30_s = _build_stage(
            "d30_retention", "D30 Retention",
            d30_retained, d30_eligible, d30_eligible,
            data_quality="heuristic",
            note=f"Activity on day 30 post-join. Eligible: {d30_eligible} users joined ≥30 days ago.",
        )
        d30_s["eligible_count"] = d30_eligible
        d30_s["retention_rate"] = _rate(d30_retained, d30_eligible)
        ordered.append(d30_s)
    else:
        ordered.append({
            "id": "d30_retention",
            "name": "D30 Retention",
            "count": None,
            "data_quality": "missing",
            "note": "No cohort users are ≥30 days old yet.",
            "conversion_from_top": None,
            "dropoff_from_top": None,
            "prev_stage_conversion": None,
            "prev_stage_dropoff": None,
            "eligible_count": 0,
            "retention_rate": None,
        })

    # ---- Largest drop-off ----
    largest_dropoff: dict | None = None
    for s in ordered:
        if s.get("count") is None or s.get("id") == "join_channel":
            continue
        drop = s.get("prev_stage_dropoff")
        if drop is not None and (
            largest_dropoff is None or drop > largest_dropoff["dropoff_pct"]
        ):
            largest_dropoff = {
                "stage_id": s["id"],
                "stage_name": s["name"],
                "dropoff_pct": drop,
                "prev_stage_conversion": s.get("prev_stage_conversion"),
            }

    # ---- New vs Returning player split (via marketing_raw_data) ----
    new_player_uids: set[int] = set()
    returning_player_uids: set[int] = set()
    new_player_checkin_days: dict[int, set[str]] = {}
    returning_player_checkin_days: dict[int, set[str]] = {}

    try:
        if cohort_uids:
            code_map: dict[str, int] = {}
            for doc in db.voucher_claims.find(
                {"user_id": {"$in": cohort_list}},
                {"user_id": 1, "coupon_code": 1, "voucher_code": 1},
            ):
                code = doc.get("coupon_code") or doc.get("voucher_code")
                uid = _coerce_uid(doc.get("user_id"))
                if code and uid and uid in cohort_uids:
                    code_map[str(code)] = uid

            if code_map:
                for doc in db.marketing_raw_data.find(
                    {"coupon_code": {"$in": list(code_map.keys())}},
                    {"coupon_code": 1, "is_new_player": 1},
                ):
                    code = str(doc.get("coupon_code") or "")
                    uid = code_map.get(code)
                    if uid:
                        is_new = doc.get("is_new_player")
                        if _is_explicit_true(is_new):
                            new_player_uids.add(uid)
                        elif _is_explicit_false(is_new):
                            returning_player_uids.add(uid)
                        # else: missing/blank/ambiguous → unknown (neither set)
    except Exception:
        pass

    def _sub_funnel_stages(sub_uids: set[int]) -> list[dict]:
        """Build simplified funnel for a subset of cohort users."""
        if not sub_uids:
            return []
        sub_list = list(sub_uids)
        sub_n = len(sub_uids)

        sub_bot = len(sub_uids & start_bot_uids)
        sub_p1 = sum(1 for uid in sub_uids if len(checkin_days_by_uid.get(uid, set())) >= 1)
        sub_p2 = sum(1 for uid in sub_uids if len(checkin_days_by_uid.get(uid, set())) >= 2)
        sub_p3 = sum(1 for uid in sub_uids if len(checkin_days_by_uid.get(uid, set())) >= 3)
        sub_claim = len(claim_uids & sub_uids)
        sub_bet = len(first_bet_uids & sub_uids) if first_bet_uids else None

        return [
            _build_stage("join_channel", "Join Channel", sub_n, sub_n, None),
            _build_stage("start_bot", "Start Bot", sub_bot, sub_n, sub_n),
            _build_stage("first_checkin", "First Check-in", sub_p1, sub_n, sub_bot),
            _build_stage("progress_2_3", "Progress 2/3", sub_p2, sub_n, sub_p1),
            _build_stage("progress_3_3", "Progress 3/3", sub_p3, sub_n, sub_p2),
            _build_stage("welcome_claim", "Welcome Claim", sub_claim, sub_n, sub_p3),
            _build_stage("first_bet", "First Bet",
                         sub_bet if sub_bet else None, sub_n, sub_claim,
                         data_quality="heuristic"),
        ]

    new_player_stages = _sub_funnel_stages(new_player_uids)
    returning_player_stages = _sub_funnel_stages(returning_player_uids)

    return {
        "cohort_size": join_count,
        "stages": ordered,
        "largest_dropoff": largest_dropoff,
        "welcome_voucher_effectiveness": {
            "unlock_count": welcome_unlock_count,
            "claim_count": welcome_claim_count,
            "unlock_to_claim_rate": _rate(welcome_claim_count, welcome_unlock_count),
            "join_to_unlock_rate": _rate(welcome_unlock_count, join_count),
            "join_to_claim_rate": _rate(welcome_claim_count, join_count),
            "checkin_progress": {
                "at_1_of_3": progress_1_count,
                "at_2_of_3": progress_2_count,
                "at_3_of_3": progress_3_count,
            },
        },
        "player_split": {
            "new_player_count": len(new_player_uids),
            "returning_player_count": len(returning_player_uids),
            "unknown_count": len(cohort_uids - new_player_uids - returning_player_uids),
            "note": "Identified via coupon_code → marketing_raw_data. Unknown: no marketing record or ambiguous is_new_player value.",
        },
        "new_player_funnel": new_player_stages,
        "returning_player_funnel": returning_player_stages,
    }
