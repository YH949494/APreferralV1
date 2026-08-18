"""Pure, read-only builders for the browser Admin Dashboard panels.

This module holds the query/shaping logic for the Vouchers, Referrals,
Affiliate, Audit, User-drilldown and Settings tabs of the admin dashboard.
Every function is *read-only*: it only ever issues find/count/aggregate
queries against the collections it is handed. It never writes, and it never
touches bot business logic (voucher issuance, referral qualification,
affiliate settlement, XP/check-in, scheduler) — it only reports on the data
those systems already produced.

Design goals (mirrors ``dashboard_telegram.py``):

* **Pure & injectable** — collections are passed in, so the panels can be
  unit-tested with fakes and without importing ``main.py`` (which has heavy
  import-time side effects).
* **Never invent data** — when a value cannot be computed the metric is
  returned with ``data_quality="missing"`` and a human-readable note. A
  best-effort/approximate value is flagged ``heuristic``/``approx``; a value
  served from a periodically-refreshed source is flagged ``delayed``.
* **Isolated failures** — a single failing query degrades one metric to
  ``missing`` rather than failing the whole panel.

Field names used here were verified against the live insert/update sites in
``vouchers.py``, ``main.py``, ``scheduler.py`` and ``affiliate_rewards.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping

try:  # ObjectId is only needed to build drop-id query variants.
    from bson import ObjectId
except Exception:  # pragma: no cover - bson always present in prod
    ObjectId = None  # type: ignore


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Any) -> datetime | None:
    """Coerce a datetime/ISO-string to an aware UTC datetime, else None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _iso(value: Any) -> str | None:
    dt = _as_utc(value)
    return dt.isoformat() if dt else None


def metric(fn: Callable[[], Any], *, quality: str = "exact", note: str | None = None) -> dict:
    """Run ``fn`` and wrap the result in a data-quality envelope.

    On any exception the metric degrades to ``data_quality="missing"`` with the
    error recorded in ``note`` — a failing query never crashes the panel and is
    never silently shown as a real value.
    """
    try:
        value = fn()
    except Exception as exc:  # noqa: BLE001 - isolate per-metric failures
        return {"value": None, "data_quality": "missing", "note": f"query failed: {exc}"}
    out = {"value": value, "data_quality": quality}
    if note:
        out["note"] = note
    return out


def _pct(numerator: Any, denominator: Any) -> float | None:
    try:
        n = float(numerator)
        d = float(denominator)
    except (TypeError, ValueError):
        return None
    if d <= 0:
        return None
    return round(100.0 * n / d, 1)


def _drop_id_variants(drop_id: Any) -> list:
    """Return the id forms a drop may be stored under across collections.

    ``vouchers.dropId`` is a *string* while ``voucher_claims.drop_id`` is an
    ``ObjectId`` (see ``_coerce_id`` in vouchers.py). Querying by both forms
    keeps the counts correct regardless of which collection we read.
    """
    variants: list = []
    if drop_id is None:
        return variants
    variants.append(drop_id)
    s = str(drop_id)
    if s not in variants:
        variants.append(s)
    if ObjectId is not None and not isinstance(drop_id, ObjectId):
        try:
            if ObjectId.is_valid(s):
                variants.append(ObjectId(s))
        except Exception:
            pass
    return variants


# ---------------------------------------------------------------------------
# 1. Vouchers panel
# ---------------------------------------------------------------------------

_DASHBOARD_WINDOW_DAYS = {"today": 1, "7d": 7, "30d": 30, "90d": 90, "all": None}
_DEFAULT_DASHBOARD_WINDOW = "7d"


def _normalize_dashboard_window(window: Any) -> str:
    w = str(window or _DEFAULT_DASHBOARD_WINDOW).strip().lower()
    return w if w in _DASHBOARD_WINDOW_DAYS else _DEFAULT_DASHBOARD_WINDOW


def _dashboard_window_start(window: str, now: datetime) -> datetime | None:
    window = _normalize_dashboard_window(window)
    if window == "all":
        return None
    if window == "today":
        return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return now - timedelta(days=int(window[:-1]))


def _windowed_claim_filter(status: str, field: str, window_start: datetime | None) -> dict:
    query = {"status": status}
    if window_start is not None:
        query[field] = {"$gte": window_start}
    return query


# Computed drop status follows vouchers.py: a stored "paused"/"expired" wins,
# otherwise it is derived from startsAt/endsAt relative to ``now``.
def compute_drop_status(doc: Mapping, now: datetime) -> str:
    stored = doc.get("status", "upcoming")
    if stored in ("paused", "expired"):
        return stored
    starts = _as_utc(doc.get("startsAt"))
    ends = _as_utc(doc.get("endsAt"))
    if ends and now >= ends:
        return "expired"
    if starts and starts <= now < (ends or starts):
        return "active"
    return "upcoming"


def build_vouchers_panel(
    *,
    drops_col,
    vouchers_col,
    voucher_claims_col,
    welcome_eligibility_col,
    now: datetime | None = None,
    window: str = _DEFAULT_DASHBOARD_WINDOW,
    max_campaigns: int = 200,
) -> dict:
    now = now or _utc_now()
    window = _normalize_dashboard_window(window)
    window_start = _dashboard_window_start(window, now)
    claimed_filter = _windowed_claim_filter("claimed", "claimed_at", window_start)
    failed_filter = _windowed_claim_filter("failed", "created_at", window_start)
    errors: list[str] = []

    def grab(fn):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
            return None

    # ---- Campaign rows (with per-drop detail for expandable rows) ----
    rows: list[dict] = []
    status_counts = {"active": 0, "upcoming": 0, "expired": 0, "paused": 0}
    # Include campaigns that overlap the selected window:
    #   campaign.startsAt <= window_end  AND  campaign.endsAt >= window_start
    # For "All Time" (window_start is None) no date filter is applied.
    drop_query: dict = {}
    if window_start is not None:
        drop_query = {
            "startsAt": {"$lte": now},
            "endsAt": {"$gte": window_start},
        }
    try:
        drop_docs = list(drops_col.find(drop_query).limit(max_campaigns))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"drops: {exc}")
        drop_docs = []

    for doc in drop_docs:
        drop_id = doc.get("_id")
        status = compute_drop_status(doc, now)
        status_counts[status] = status_counts.get(status, 0) + 1
        variants = _drop_id_variants(drop_id)

        total_codes = grab(lambda v=str(drop_id): vouchers_col.count_documents({"dropId": v}))
        claimed = grab(
            lambda v=variants, base=claimed_filter: voucher_claims_col.count_documents(
                {**base, "drop_id": {"$in": v}}
            )
        )
        failed = grab(
            lambda v=variants, base=failed_filter: voucher_claims_col.count_documents(
                {**base, "drop_id": {"$in": v}}
            )
        )
        remaining = None
        if doc.get("type") == "pooled":
            # Pooled drops keep their own remaining counters.
            pub = doc.get("public_remaining")
            my = doc.get("my_remaining")
            if isinstance(pub, int) or isinstance(my, int):
                remaining = (pub or 0) + (my or 0)
        if remaining is None:
            remaining = grab(
                lambda v=str(drop_id): vouchers_col.count_documents(
                    {"dropId": v, "status": {"$in": ["unclaimed", "free"]}}
                )
            )

        # Failure-reason breakdown for the expandable row.
        failure_reasons = []
        try:
            agg = voucher_claims_col.aggregate(
                [
                    {"$match": {**failed_filter, "drop_id": {"$in": variants}}},
                    {"$group": {"_id": "$error", "n": {"$sum": 1}}},
                    {"$sort": {"n": -1}},
                    {"$limit": 20},
                ]
            )
            failure_reasons = [
                {"reason": (a.get("_id") or "unknown"), "count": int(a.get("n", 0))}
                for a in agg
            ]
        except Exception as exc:  # noqa: BLE001
            errors.append(f"failure_reasons[{drop_id}]: {exc}")

        rows.append(
            {
                "drop_id": str(drop_id),
                "name": doc.get("name") or "(unnamed)",
                "status": status,
                "type": doc.get("type"),
                "starts_at": _iso(doc.get("startsAt")),
                "ends_at": _iso(doc.get("endsAt")),
                "total_codes": total_codes,
                "claimed": claimed,
                "remaining": remaining,
                "claim_rate_pct": _pct(claimed, total_codes),
                "detail": {
                    "drop_id": str(drop_id),
                    "claim_attempts": {
                        "claimed": claimed,
                        "failed": failed,
                        "total_codes": total_codes,
                    },
                    "failure_reasons": failure_reasons,
                    "pool_breakdown": {
                        "type": doc.get("type"),
                        "public_remaining": doc.get("public_remaining"),
                        "my_remaining": doc.get("my_remaining"),
                    },
                    "metadata": {
                        "priority": doc.get("priority"),
                        "visibility_mode": doc.get("visibilityMode"),
                        "eligibility": doc.get("eligibility"),
                        "audience": doc.get("audience"),
                        "whitelist_count": len(doc.get("whitelistUsernames") or [])
                        if doc.get("whitelistUsernames") is not None
                        else None,
                    },
                },
            }
        )

    # ---- Aggregate code totals across all drops ----
    total_codes_all = metric(lambda: int(vouchers_col.count_documents({})))
    claimed_codes = metric(
        lambda: int(voucher_claims_col.count_documents(claimed_filter))
    )
    remaining_codes_all = metric(
        lambda: int(vouchers_col.count_documents({"status": {"$in": ["unclaimed", "free"]}}))
    )
    claim_rate = None
    if (
        total_codes_all["value"] is not None
        and claimed_codes["value"] is not None
    ):
        claim_rate = _pct(claimed_codes["value"], total_codes_all["value"])

    failed_claims = metric(
        lambda: int(
            voucher_claims_col.count_documents(failed_filter)
        ),
        note="Voucher claim attempts that failed in the selected window.",
    )

    def _repeat_claimers():
        agg = voucher_claims_col.aggregate(
            [
                {"$match": claimed_filter},
                {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}},
                {"$count": "c"},
            ]
        )
        agg_list = list(agg)
        return int(agg_list[0]["c"]) if agg_list else 0

    repeat_claimers = metric(
        _repeat_claimers,
        quality="heuristic",
        note="Distinct users with >1 successful claim in the selected window.",
    )

    welcome_query = {"claimed": True}
    if window_start is not None:
        welcome_query["claimed_at"] = {"$gte": window_start}
    welcome_claims = metric(
        lambda: int(
            welcome_eligibility_col.count_documents(welcome_query)
        ),
        note="welcome_eligibility records claimed in the selected window.",
    )

    return {
        "success": True,
        "as_of": now.isoformat(),
        "window": window,
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": now.isoformat(),
        "summary": {
            "active_campaigns": {"value": status_counts.get("active", 0), "data_quality": "exact"},
            "upcoming_campaigns": {"value": status_counts.get("upcoming", 0), "data_quality": "exact"},
            "ended_campaigns": {"value": status_counts.get("expired", 0), "data_quality": "exact"},
            "paused_campaigns": {"value": status_counts.get("paused", 0), "data_quality": "exact"},
            "total_codes": total_codes_all,
            "claimed_codes": claimed_codes,
            "remaining_codes": remaining_codes_all,
            "claim_rate_pct": {
                "value": claim_rate,
                "data_quality": "exact" if claim_rate is not None else "missing",
            },
            "failed_claims": failed_claims,
            "repeat_claimers": repeat_claimers,
            "welcome_claims": welcome_claims,
        },
        "campaigns": rows,
        "partial_errors": errors or None,
    }


# ---------------------------------------------------------------------------
# 1b. Welcome Voucher Progress journey (V2) panel
# ---------------------------------------------------------------------------

def build_welcome_journey_panel(
    *,
    welcome_eligibility_col,
    welcome_analytics_events_col,
    now: datetime | None = None,
    window: str = _DEFAULT_DASHBOARD_WINDOW,
) -> dict:
    """KPIs for the Welcome Voucher Progress journey (V2).

    Reads only from ``welcome_eligibility`` (eligibility/claim state) and
    ``welcome_analytics_events`` (the append-only V2 event log written by
    ``vouchers.log_welcome_event``); never touches claim/voucher issuance.
    """
    now = now or _utc_now()
    window = _normalize_dashboard_window(window)
    window_start = _dashboard_window_start(window, now)

    def _event_user_count(event: str) -> int:
        match: dict = {"event": event}
        if window_start is not None:
            match["created_at"] = {"$gte": window_start}
        return len(welcome_analytics_events_col.distinct("user_id", match))

    def _eligible_users() -> int:
        query: dict = {}
        if window_start is not None:
            query["created_at"] = {"$gte": window_start}
        return int(welcome_eligibility_col.count_documents(query))

    def _claimed_users() -> int:
        query = {"claimed": True}
        if window_start is not None:
            query["claimed_at"] = {"$gte": window_start}
        return int(welcome_eligibility_col.count_documents(query))

    _REMINDER_STAGE_EVENTS = (
        "welcome_reminder_20h_sent",
        "welcome_reminder_28h_sent",
        "welcome_reminder_day2_sent",
        "welcome_recovery_sent",
    )

    def _reminder_recipients() -> set:
        recipients: set = set()
        for reminder_event in _REMINDER_STAGE_EVENTS:
            match: dict = {"event": reminder_event}
            if window_start is not None:
                match["created_at"] = {"$gte": window_start}
            recipients.update(welcome_analytics_events_col.distinct("user_id", match))
        return recipients

    def _reminder_recovery_rate():
        recipients = _reminder_recipients()
        if not recipients:
            return None
        completed_match: dict = {"event": "welcome_completed", "user_id": {"$in": list(recipients)}}
        completed = set(welcome_analytics_events_col.distinct("user_id", completed_match))
        return _pct(len(completed), len(recipients))

    eligible = metric(_eligible_users, note="Distinct welcome_eligibility records created in the selected window.")
    d1_users = metric(lambda: _event_user_count("welcome_checkin_d1"), note="Distinct users who logged a Day 1 check-in.")
    d2_users = metric(lambda: _event_user_count("welcome_checkin_d2"), note="Distinct users who logged a Day 2 check-in.")
    d3_users = metric(lambda: _event_user_count("welcome_checkin_d3"), note="Distinct users who logged a Day 3 check-in.")
    completed_users = metric(lambda: _event_user_count("welcome_completed"), note="Distinct users who unlocked the welcome reward.")
    claimed_users = metric(_claimed_users, note="welcome_eligibility records claimed in the selected window.")

    # Reminder funnel (Phase 2). All stages reuse the append-only
    # welcome_analytics_events log written by vouchers.log_welcome_event - no
    # new tracking system. "Delivered" is not modeled: the Telegram Bot API
    # does not confirm message delivery, only send success/failure, so there
    # is no reliable signal to report between "Sent" and "MiniApp Open".
    reminder_sent_users = metric(lambda: len(_reminder_recipients()), note="Distinct users sent any Welcome reminder (20h/28h/day2/recovery stages combined).")
    miniapp_open_users = metric(
        lambda: _event_user_count("welcome_progress_view"),
        note="Distinct users who opened the mini-app while the Welcome card was visible (proxy via welcome_progress_view, fired by /api/welcome-progress).",
    )
    recovery_sent_users = metric(lambda: _event_user_count("welcome_recovery_sent"), note="Distinct users sent the Smart Recovery Journey nudge (stalled well past the normal reminder window).")

    d2_rate = _pct(d2_users["value"], d1_users["value"]) if d1_users["value"] and d2_users["value"] is not None else None
    d3_rate = _pct(d3_users["value"], d2_users["value"]) if d2_users["value"] and d3_users["value"] is not None else None
    completion_rate = _pct(completed_users["value"], eligible["value"]) if eligible["value"] else None
    claim_rate = _pct(claimed_users["value"], completed_users["value"]) if completed_users["value"] else None
    reminder_recovery_rate = metric(_reminder_recovery_rate, quality="heuristic", note="Share of reminder recipients who reached welcome_completed afterwards.")
    reminder_to_open_rate = _pct(miniapp_open_users["value"], reminder_sent_users["value"]) if reminder_sent_users["value"] else None

    return {
        "success": True,
        "as_of": now.isoformat(),
        "window": window,
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": now.isoformat(),
        "summary": {
            "welcome_eligible_users": eligible,
            "welcome_d2_rate_pct": {"value": d2_rate, "data_quality": "exact" if d2_rate is not None else "missing"},
            "welcome_d3_rate_pct": {"value": d3_rate, "data_quality": "exact" if d3_rate is not None else "missing"},
            "welcome_completion_rate_pct": {"value": completion_rate, "data_quality": "exact" if completion_rate is not None else "missing"},
            "welcome_claim_rate_pct": {"value": claim_rate, "data_quality": "exact" if claim_rate is not None else "missing"},
            "reminder_recovery_rate_pct": reminder_recovery_rate,
            "welcome_reminder_sent_users": reminder_sent_users,
            "welcome_recovery_sent_users": recovery_sent_users,
            "welcome_miniapp_open_users": miniapp_open_users,
            # PROXY, not a causal reminder-open rate: the numerator
            # (welcome_progress_view) fires on ANY mini-app open where the
            # Welcome card is visible, not only opens caused by tapping a
            # reminder - there is no reminder-specific click-through
            # attribution today. Never report this as "reminder effectiveness"
            # without that caveat.
            "reminder_to_miniapp_open_rate_pct": {"value": reminder_to_open_rate, "data_quality": "heuristic" if reminder_to_open_rate is not None else "missing", "note": "Proxy only: numerator is any Welcome-card mini-app open (welcome_progress_view), not opens attributed to a specific reminder tap."},
            "welcome_first_deposit_users": {"value": None, "data_quality": "unavailable", "note": "Deposit/bet activity is not present in this backend (see uim_kpi_mapping.py) - requires a feed from the platform's deposit ledger before this can be tracked."},
        },
    }


# ---------------------------------------------------------------------------
# 2. Referrals panel
# ---------------------------------------------------------------------------

_PENDING_STATUSES = ["pending", "pending_channel", "processing"]
_QUALIFIED_STATUSES = ["awarded", "qualified", "settled", "success"]
_REVOKED_STATUSES = ["revoked", "failed", "rejected", "expired"]

# Supported time-filter windows for the referrals panel. Maps window -> number
# of days back from "now"; ``all`` means no time filter and ``today`` is handled
# specially (start of the current UTC day).
_REFERRAL_WINDOW_DAYS = {"today": 1, "7d": 7, "30d": 30, "90d": 90, "all": None}
_DEFAULT_REFERRAL_WINDOW = "7d"


def _normalize_referral_window(window: Any) -> str:
    w = str(window or "").strip().lower()
    return w if w in _REFERRAL_WINDOW_DAYS else _DEFAULT_REFERRAL_WINDOW


def _referral_window_start(window: str, now: datetime) -> datetime | None:
    """Return the inclusive start cutoff for a window, or None for ``all``."""
    if window == "all":
        return None
    if window == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    days = _REFERRAL_WINDOW_DAYS.get(window) or _REFERRAL_WINDOW_DAYS[_DEFAULT_REFERRAL_WINDOW]
    return now - timedelta(days=days)


def _classify_referral_status(status: Any) -> str:
    s = str(status or "").lower()
    if s in _QUALIFIED_STATUSES:
        return "qualified"
    if s in _REVOKED_STATUSES:
        return "revoked"
    return "pending"


def build_referrals_panel(
    *,
    pending_referrals_col,
    qualified_events_col,
    users_col,
    welcome_eligibility_col,
    now: datetime | None = None,
    window: str = _DEFAULT_REFERRAL_WINDOW,
    top_n: int = 25,
    invitees_per_referrer_cap: int = 200,
) -> dict:
    now = now or _utc_now()
    errors: list[str] = []

    window = _normalize_referral_window(window)
    window_start = _referral_window_start(window, now)
    # Time filter applied to every referral metric below. ``all`` => no filter.
    time_filter: dict = (
        {} if window_start is None else {"created_at_utc": {"$gte": window_start}}
    )

    def _with_time(extra: dict) -> dict:
        return {**time_filter, **extra}

    total_referrers = metric(
        lambda: len(pending_referrals_col.distinct("inviter_user_id", time_filter or None))
    )
    total_invitees = metric(
        lambda: len(pending_referrals_col.distinct("invitee_user_id", time_filter or None))
    )
    qualified = metric(
        lambda: int(pending_referrals_col.count_documents(_with_time({"status": {"$in": _QUALIFIED_STATUSES}})))
    )
    pending = metric(
        lambda: int(pending_referrals_col.count_documents(_with_time({"status": {"$in": _PENDING_STATUSES}})))
    )
    revoked = metric(
        lambda: int(pending_referrals_col.count_documents(_with_time({"status": {"$in": _REVOKED_STATUSES}})))
    )

    # ---- Top referrers (bounded aggregation) ----
    rows: list[dict] = []
    invitee_ids: set = set()
    try:
        agg = pending_referrals_col.aggregate(
            [
                {"$match": _with_time({"inviter_user_id": {"$ne": None}})},
                {
                    "$group": {
                        "_id": "$inviter_user_id",
                        "invitees": {"$sum": 1},
                        "qualified": {
                            "$sum": {"$cond": [{"$in": ["$status", _QUALIFIED_STATUSES]}, 1, 0]}
                        },
                        "pending": {
                            "$sum": {"$cond": [{"$in": ["$status", _PENDING_STATUSES]}, 1, 0]}
                        },
                        "revoked": {
                            "$sum": {"$cond": [{"$in": ["$status", _REVOKED_STATUSES]}, 1, 0]}
                        },
                        "invitee_ids": {"$push": "$invitee_user_id"},
                    }
                },
                {"$sort": {"invitees": -1}},
                {"$limit": top_n},
            ]
        )
        agg_rows = list(agg)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"referrer_aggregate: {exc}")
        agg_rows = []

    for r in agg_rows:
        ids = [i for i in (r.get("invitee_ids") or []) if i is not None][:invitees_per_referrer_cap]
        invitee_ids.update(ids)
        rows.append(
            {
                "referrer_id": r.get("_id"),
                "invitees": int(r.get("invitees", 0)),
                "qualified": int(r.get("qualified", 0)),
                "pending": int(r.get("pending", 0)),
                "revoked": int(r.get("revoked", 0)),
                "_invitee_ids": ids,  # stripped before return
            }
        )

    # Resolve check-in and welcome-claim sets for the bounded invitee pool.
    checkin_set: set = set()
    welcome_set: set = set()
    referrer_names: dict = {}
    if invitee_ids:
        id_list = list(invitee_ids)
        try:
            for u in users_col.find(
                {"user_id": {"$in": id_list}, "first_checkin_at": {"$ne": None}},
                {"user_id": 1},
            ):
                checkin_set.add(u.get("user_id"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"checkin_lookup: {exc}")
        try:
            for w in welcome_eligibility_col.find(
                {
                    "claimed": True,
                    "$or": [{"uid": {"$in": id_list}}, {"user_id": {"$in": id_list}}],
                },
                {"uid": 1, "user_id": 1},
            ):
                welcome_set.add(w.get("uid"))
                welcome_set.add(w.get("user_id"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"welcome_lookup: {exc}")

    # Referrer usernames (best-effort).
    ref_ids = [r["referrer_id"] for r in rows if r["referrer_id"] is not None]
    if ref_ids:
        try:
            for u in users_col.find({"user_id": {"$in": ref_ids}}, {"user_id": 1, "username": 1}):
                referrer_names[u.get("user_id")] = u.get("username")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"referrer_names: {exc}")

    for r in rows:
        ids = r.pop("_invitee_ids")
        welcome_claimed = sum(1 for i in ids if i in welcome_set)
        checkin_completed = sum(1 for i in ids if i in checkin_set)
        r["username"] = referrer_names.get(r["referrer_id"])
        r["welcome_claimed"] = welcome_claimed
        r["checkin_completed"] = checkin_completed
        r["quality_pct"] = _pct(r["qualified"], r["invitees"])

    # ---- Cohort-wide invitee rates (bounded to the same top-referrer pool) ----
    checkin_rate = None
    welcome_rate = None
    if invitee_ids:
        checkin_rate = _pct(len(checkin_set), len(invitee_ids))
        welcome_rate = _pct(
            len({i for i in invitee_ids if i in welcome_set}), len(invitee_ids)
        )

    return {
        "success": True,
        "window": window,
        "as_of": now.isoformat(),
        "summary": {
            "total_referrers": total_referrers,
            "total_invitees": total_invitees,
            "qualified_referrals": qualified,
            "pending_referrals": pending,
            "revoked_referrals": revoked,
            "invitee_checkin_rate_pct": {
                "value": checkin_rate,
                "data_quality": "heuristic" if checkin_rate is not None else "missing",
                "note": "Share of top-referrer invitees with a recorded first check-in.",
            },
            "invitee_welcome_claim_rate_pct": {
                "value": welcome_rate,
                "data_quality": "heuristic" if welcome_rate is not None else "missing",
                "note": "Share of top-referrer invitees who claimed the welcome bonus.",
            },
        },
        "referrers": rows,
        "note": (
            f"Referrer table shows the top {top_n} referrers by invitee count; "
            "per-invitee rates are computed over that bounded pool."
        ),
        "partial_errors": errors or None,
    }


def build_referral_detail(
    *,
    referrer_id: int,
    pending_referrals_col,
    users_col,
    welcome_eligibility_col,
    now: datetime | None = None,
    limit: int = 500,
) -> dict:
    now = now or _utc_now()
    errors: list[str] = []
    invitees: list[dict] = []
    try:
        docs = list(
            pending_referrals_col.find({"inviter_user_id": referrer_id}).limit(limit)
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "success": False,
            "referrer_id": referrer_id,
            "message": f"lookup failed: {exc}",
        }

    ids = [d.get("invitee_user_id") for d in docs if d.get("invitee_user_id") is not None]
    checkin_map: dict = {}
    welcome_map: dict = {}
    if ids:
        try:
            for u in users_col.find(
                {"user_id": {"$in": ids}}, {"user_id": 1, "first_checkin_at": 1, "username": 1}
            ):
                checkin_map[u.get("user_id")] = u
        except Exception as exc:  # noqa: BLE001
            errors.append(f"checkin_lookup: {exc}")
        try:
            for w in welcome_eligibility_col.find(
                {"$or": [{"uid": {"$in": ids}}, {"user_id": {"$in": ids}}]},
                {"uid": 1, "user_id": 1, "claimed": 1, "claimed_at": 1},
            ):
                welcome_map[w.get("uid")] = w
                welcome_map[w.get("user_id")] = w
        except Exception as exc:  # noqa: BLE001
            errors.append(f"welcome_lookup: {exc}")

    for d in docs:
        iid = d.get("invitee_user_id")
        u = checkin_map.get(iid) or {}
        w = welcome_map.get(iid) or {}
        invitees.append(
            {
                "invitee_id": iid,
                "username": u.get("username"),
                "join_date": _iso(d.get("created_at_utc")),
                "referral_status": _classify_referral_status(d.get("status")),
                "raw_status": d.get("status"),
                "revoked_reason": d.get("revoked_reason"),
                "checkin_completed": bool(_as_utc(u.get("first_checkin_at"))),
                "welcome_claimed": bool(w.get("claimed")),
                "welcome_claimed_at": _iso(w.get("claimed_at")),
            }
        )

    return {
        "success": True,
        "referrer_id": referrer_id,
        "as_of": now.isoformat(),
        "invitees": invitees,
        "partial_errors": errors or None,
    }


# ---------------------------------------------------------------------------
# 3. Affiliate panel
# ---------------------------------------------------------------------------

_AFF_PENDING = ["PENDING_REVIEW", "PENDING_MANUAL", "PENDING_EOM", "SIMULATED_PENDING"]
_AFF_APPROVED = ["APPROVED"]
_AFF_ISSUED = ["ISSUED", "SETTLING"]
_AFF_REJECTED = ["REJECTED", "OUT_OF_STOCK"]
_AFF_TIERS = ["WELCOME", "T1", "T2", "T3", "T4", "T5"]


def build_affiliate_panel(
    *,
    affiliate_ledger_col,
    voucher_pools_col,
    now: datetime | None = None,
    top_n: int = 50,
) -> dict:
    now = now or _utc_now()
    month_key = now.strftime("%Y%m")
    errors: list[str] = []

    pending = metric(
        lambda: int(affiliate_ledger_col.count_documents({"status": {"$in": _AFF_PENDING}}))
    )
    approved = metric(
        lambda: int(affiliate_ledger_col.count_documents({"status": {"$in": _AFF_APPROVED}}))
    )
    issued = metric(
        lambda: int(affiliate_ledger_col.count_documents({"status": {"$in": _AFF_ISSUED}}))
    )
    rejected = metric(
        lambda: int(affiliate_ledger_col.count_documents({"status": {"$in": _AFF_REJECTED}}))
    )

    # ---- Pool availability per tier ----
    pool_availability = []
    for pool_id in _AFF_TIERS:
        avail = metric(
            lambda p=pool_id: int(
                voucher_pools_col.count_documents({"pool_id": p, "status": "available"})
            )
        )
        issued_n = metric(
            lambda p=pool_id: int(
                voucher_pools_col.count_documents({"pool_id": p, "status": "issued"})
            )
        )
        pool_availability.append(
            {"pool_id": pool_id, "available": avail["value"], "issued": issued_n["value"]}
        )

    # ---- Monthly issuance summary (current month) ----
    monthly_issuance = []
    try:
        agg = affiliate_ledger_col.aggregate(
            [
                {"$match": {"ledger_type": "AFFILIATE_MONTHLY", "year_month": month_key}},
                {"$group": {"_id": "$status", "n": {"$sum": 1}}},
                {"$sort": {"n": -1}},
            ]
        )
        monthly_issuance = [
            {"status": a.get("_id"), "count": int(a.get("n", 0))} for a in agg
        ]
    except Exception as exc:  # noqa: BLE001
        errors.append(f"monthly_issuance: {exc}")

    # ---- Affiliate table (top users by qualified_count) ----
    rows: list[dict] = []
    try:
        cursor = (
            affiliate_ledger_col.find(
                {"ledger_type": {"$ne": "WELCOME"}},
                {
                    "user_id": 1,
                    "tier": 1,
                    "status": 1,
                    "qualified_count": 1,
                    "updated_at": 1,
                    "would_issue_pool": 1,
                },
            )
            .sort("updated_at", -1)
            .limit(top_n)
        )
        for d in cursor:
            qualified_count = d.get("qualified_count")
            rows.append(
                {
                    "user_id": d.get("user_id"),
                    "tier": d.get("tier") or d.get("would_issue_pool"),
                    "status": d.get("status"),
                    "qualified_count": qualified_count,
                    "conversion_pct": None,  # see note: no per-user invite base recorded here
                    "updated_at": _iso(d.get("updated_at")),
                }
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"affiliate_table: {exc}")

    return {
        "success": True,
        "as_of": now.isoformat(),
        "month_key": month_key,
        "summary": {
            "pending_review": pending,
            "approved": approved,
            "issued": issued,
            "rejected": rejected,
        },
        "pool_availability": pool_availability,
        "monthly_issuance": {
            "month_key": month_key,
            "by_status": monthly_issuance,
            "data_quality": "exact" if monthly_issuance else "missing",
        },
        "affiliates": rows,
        "note": (
            "Conversion % is not stored per ledger row (no per-user invite base "
            "is persisted on the ledger); shown as Data Not Available."
        ),
        "partial_errors": errors or None,
    }


def build_affiliate_detail(
    *,
    user_id: int,
    affiliate_ledger_col,
    now: datetime | None = None,
    limit: int = 200,
) -> dict:
    now = now or _utc_now()
    try:
        docs = list(
            affiliate_ledger_col.find({"user_id": user_id}).sort("updated_at", -1).limit(limit)
        )
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "user_id": user_id, "message": f"lookup failed: {exc}"}

    ledger = []
    status_history = []
    vouchers_issued = []
    for d in docs:
        ledger.append(
            {
                "ledger_id": str(d.get("_id")),
                "ledger_type": d.get("ledger_type"),
                "tier": d.get("tier"),
                "pool_id": d.get("pool_id"),
                "status": d.get("status"),
                "year_month": d.get("year_month"),
                "qualified_count": d.get("qualified_count"),
                "risk_flags": d.get("risk_flags") or [],
                "review_reason": d.get("review_reason"),
                "created_at": _iso(d.get("created_at")),
                "updated_at": _iso(d.get("updated_at")),
            }
        )
        status_history.append(
            {
                "status": d.get("status"),
                "ledger_type": d.get("ledger_type"),
                "at": _iso(d.get("updated_at")),
            }
        )
        if d.get("voucher_code"):
            vouchers_issued.append(
                {
                    "voucher_code": d.get("voucher_code"),
                    "pool_id": d.get("pool_id"),
                    "tier": d.get("tier"),
                    "issued_at": _iso(d.get("updated_at")),
                }
            )

    return {
        "success": True,
        "user_id": user_id,
        "as_of": now.isoformat(),
        "ledger": ledger,
        "vouchers_issued": vouchers_issued,
        "status_history": status_history,
    }


# ---------------------------------------------------------------------------
# 4. Audit panel
# ---------------------------------------------------------------------------

def _audit_row(*, time, actor, action, target, result, payload=None, related=None, error=None):
    return {
        "time": time,
        "actor": actor,
        "action": action,
        "target": target,
        "result": result,
        "detail": {
            "payload": payload or {},
            "related_ids": related or {},
            "error": error,
        },
    }


def build_audit_panel(
    *,
    admin_login_audit_col,
    audit_events_col,
    referral_audit_col,
    admin_cache_col,
    now: datetime | None = None,
    limit: int = 100,
) -> dict:
    now = now or _utc_now()
    errors: list[str] = []
    rows: list[dict] = []

    # ---- Admin logins / dashboard auth events ----
    login_ok = 0
    auth_events = 0
    try:
        for d in admin_login_audit_col.find({}).sort("at", -1).limit(limit):
            event = d.get("event")
            auth_events += 1
            if event == "login_ok":
                login_ok += 1
            uname = d.get("username") or ""
            actor = f"@{uname}" if uname else (str(d.get("user_id")) if d.get("user_id") else "unknown")
            rows.append(
                _audit_row(
                    time=_iso(d.get("at")),
                    actor=actor,
                    action=event,
                    target="admin_dashboard",
                    result="ok" if event in ("login_ok", "logout") else (d.get("reason") or "denied"),
                    payload={
                        "event": event,
                        "reason": d.get("reason"),
                        "ip": d.get("ip"),
                        "user_agent": d.get("user_agent"),
                    },
                    related={"user_id": d.get("user_id")},
                )
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"admin_login_audit: {exc}")

    # ---- Scheduler / monthly job events (audit_events) ----
    scheduler_events = 0
    try:
        for d in audit_events_col.find({}).limit(limit):
            scheduler_events += 1
            ts = d.get("run_at_utc") or d.get("ts_utc")
            rows.append(
                _audit_row(
                    time=_iso(ts),
                    actor="scheduler",
                    action=d.get("type") or str(d.get("_id")),
                    target=d.get("month") or "—",
                    result="ok",
                    payload={
                        "id": str(d.get("_id")),
                        "type": d.get("type"),
                        "total_processed": d.get("total_processed"),
                        "promoted": d.get("promoted"),
                        "demoted": d.get("demoted"),
                        "run_id": d.get("run_id"),
                    },
                )
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"audit_events: {exc}")

    # ---- Referral processing audit (voucher/referral operations) ----
    referral_ops = 0
    try:
        for d in referral_audit_col.find({}).sort("ts_utc", -1).limit(limit):
            referral_ops += 1
            rows.append(
                _audit_row(
                    time=_iso(d.get("ts_utc")),
                    actor=str(d.get("inviter_user_id") or "—"),
                    action=f"referral:{d.get('status') or 'event'}",
                    target=str(d.get("invitee_user_id") or "—"),
                    result=d.get("reason") or d.get("status") or "—",
                    payload={
                        "chat_id": d.get("chat_id"),
                        "invite_link": d.get("invite_link"),
                        "status": d.get("status"),
                        "reason": d.get("reason"),
                        "extra": d.get("extra"),
                    },
                    related={
                        "inviter_user_id": d.get("inviter_user_id"),
                        "invitee_user_id": d.get("invitee_user_id"),
                    },
                    error=d.get("error"),
                )
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"referral_audit: {exc}")

    # Sort the merged feed newest-first; rows with no timestamp sink to the end.
    rows.sort(key=lambda r: r.get("time") or "", reverse=True)
    rows = rows[:limit]

    # Last scheduler heartbeat (for the "scheduler events" summary card).
    last_heartbeat = None
    try:
        hb = admin_cache_col.find_one({"_id": "snapshot_heartbeat"}, {"ts_utc": 1})
        last_heartbeat = _iso((hb or {}).get("ts_utc"))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"heartbeat: {exc}")

    return {
        "success": True,
        "as_of": now.isoformat(),
        "summary": {
            "admin_logins": {"value": login_ok, "data_quality": "exact",
                             "note": f"login_ok events in the last {limit} auth records."},
            "auth_events": {"value": auth_events, "data_quality": "exact"},
            "voucher_operations": {"value": None, "data_quality": "missing",
                                   "note": "No dedicated voucher-operation audit collection exists; "
                                           "voucher claims are tracked under the Vouchers tab."},
            "affiliate_status_changes": {"value": None, "data_quality": "missing",
                                         "note": "Affiliate status changes are not written to a "
                                                 "separate audit log; see Affiliate tab ledger history."},
            "scheduler_events": {"value": scheduler_events, "data_quality": "exact"},
            "referral_operations": {"value": referral_ops, "data_quality": "exact"},
            "last_scheduler_heartbeat": {"value": last_heartbeat,
                                         "data_quality": "delayed" if last_heartbeat else "missing"},
        },
        "events": rows,
        "partial_errors": errors or None,
    }


# ---------------------------------------------------------------------------
# 4b. Segment overview (read-only — reports on segment fields the bot/UIM
# already write; never computes or assigns a segment).
# ---------------------------------------------------------------------------

_TOP_SEGMENTS_LIMIT = 15

_SEGMENT_MODES = {"snapshot", "this_month", "last_month", "month", "snapshot_month"}
_DEFAULT_SEGMENT_MODE = "snapshot"


def _normalize_segment_mode(mode: Any) -> str:
    m = str(mode or _DEFAULT_SEGMENT_MODE).strip().lower()
    return m if m in _SEGMENT_MODES else _DEFAULT_SEGMENT_MODE


def _parse_segment_month(month: Any) -> tuple[datetime | None, datetime | None, str | None]:
    """Parse an explicit "YYYY-MM" month string into UTC [start, end) bounds."""
    raw = str(month or "").strip()
    try:
        start = datetime.strptime(raw, "%Y-%m").replace(tzinfo=timezone.utc)
    except ValueError:
        return None, None, None
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end, raw


def _month_bounds(now: datetime, *, months_back: int) -> tuple[datetime, datetime]:
    """Return [start, end) UTC bounds for the month ``months_back`` months before ``now``."""
    year, month = now.year, now.month
    for _ in range(months_back):
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def _segment_mode_label(mode: str, selected_month: str | None = None) -> str:
    if mode == "month" and selected_month:
        return f"Synced in {selected_month}"
    if mode == "snapshot_month" and selected_month:
        return f"Segment snapshot history for {selected_month}"
    return {
        "snapshot": "Current segment snapshot",
        "this_month": "Synced this month",
        "last_month": "Synced last month",
        "month": "Synced in selected month",
        "snapshot_month": "Segment snapshot history (monthly)",
    }.get(mode, mode)


def _segment_is_blank(raw: Any) -> bool:
    return raw is None or str(raw).strip() == ""


def build_monthly_segment_distribution(*, segment_snapshots_col, month: str) -> dict:
    """Monthly segment distribution from segment_snapshots (latest snapshot per user).

    ``month`` is a "YYYY-MM" string. Uses the most recent snapshot per
    ``user_id`` within that month (multiple weekly snapshots in the same
    month are collapsed to one), grouped by ``normalized_segment``.
    Returns a clear empty state when no snapshots exist for the month.
    """
    docs = list(segment_snapshots_col.find({"snapshot_month": month}))
    latest_by_user: dict[Any, dict] = {}
    for doc in docs:
        user_id = doc.get("user_id")
        existing = latest_by_user.get(user_id)
        if existing is None or (doc.get("created_at") or datetime.min) >= (existing.get("created_at") or datetime.min):
            latest_by_user[user_id] = doc

    normalized_counts: dict[str, int] = {}
    for doc in latest_by_user.values():
        normalized = doc.get("normalized_segment") or "unclassified"
        normalized_counts[normalized] = normalized_counts.get(normalized, 0) + 1

    top_segments = sorted(normalized_counts.items(), key=lambda kv: kv[1], reverse=True)[:_TOP_SEGMENTS_LIMIT]
    return {
        "month": month,
        "has_data": bool(latest_by_user),
        "total_users": len(latest_by_user),
        "top_segments": [{"segment": name, "count": count} for name, count in top_segments],
        "segment_counts": normalized_counts,
    }


def build_segments_panel(
    *,
    users_col,
    now: datetime | None = None,
    mode: str = _DEFAULT_SEGMENT_MODE,
    segment_filter: str | None = None,
    month: str | None = None,
    segment_snapshots_col=None,
) -> dict:
    """Read-only segment distribution built from existing user fields.

    Only reports on ``for_bot_segment`` / ``bot_segment`` (normalized via the
    existing ``config.normalize_for_bot_segment``) and the existing
    ``has_ever_claimed_public_pool`` flag. No new segment classification is
    introduced here.

    ``mode`` controls which users are counted:
      - "snapshot": all current users, no date filter (segment distribution
        is a point-in-time snapshot, not a time-windowed metric).
      - "this_month" / "last_month": only users whose ``bot_segment_synced_at``
        falls within that calendar month (UTC), grouped by normalized segment.
      - "month": an explicit "YYYY-MM" month (via ``month``), for browsing
        any past month rather than just this/last month.
    """
    from config import (  # local import avoids a hard dep at module load
        is_blank_or_unknown_for_bot_segment,
        normalize_for_bot_segment,
    )

    now = now or _utc_now()
    mode = _normalize_segment_mode(mode)
    errors: list[str] = []

    if mode == "snapshot_month":
        _, _, selected_month = _parse_segment_month(month)
        if selected_month is None or segment_snapshots_col is None:
            return {
                "success": True,
                "mode": "snapshot_month",
                "mode_label": _segment_mode_label("snapshot_month", selected_month),
                "selected_month": selected_month,
                "as_of": now.isoformat(),
                "generated_at": now.isoformat(),
                "month_start": None,
                "month_end": None,
                "data_source": "segment_snapshots collection (read only)",
                "summary": {
                    "total_users": {"value": 0, "data_quality": "missing", "note": "No month specified or no snapshot collection available."},
                },
                "top_segments": [],
                "segment_filter": segment_filter or None,
                "filtered_count": None,
                "has_data": False,
                "partial_errors": ["snapshot_month requires a valid 'month' (YYYY-MM)"] if selected_month is None else None,
            }
        distribution = build_monthly_segment_distribution(segment_snapshots_col=segment_snapshots_col, month=selected_month)
        filtered_count = None
        if segment_filter:
            filtered_count = distribution["segment_counts"].get(normalize_for_bot_segment(segment_filter))
        return {
            "success": True,
            "mode": "snapshot_month",
            "mode_label": _segment_mode_label("snapshot_month", selected_month),
            "selected_month": selected_month,
            "as_of": now.isoformat(),
            "generated_at": now.isoformat(),
            "month_start": None,
            "month_end": None,
            "data_source": "segment_snapshots collection (latest snapshot per user in month — read only)",
            "summary": {
                "total_users": {
                    "value": distribution["total_users"],
                    "data_quality": "exact" if distribution["has_data"] else "missing",
                    "note": "No snapshots recorded for this month yet." if not distribution["has_data"] else None,
                },
            },
            "top_segments": distribution["top_segments"],
            "segment_filter": segment_filter or None,
            "filtered_count": filtered_count,
            "has_data": distribution["has_data"],
            "partial_errors": None,
        }

    month_start: datetime | None = None
    month_end: datetime | None = None
    selected_month: str | None = None
    if mode == "this_month":
        month_start, month_end = _month_bounds(now, months_back=0)
    elif mode == "last_month":
        month_start, month_end = _month_bounds(now, months_back=1)
    elif mode == "month":
        month_start, month_end, selected_month = _parse_segment_month(month)
        if month_start is None:
            mode = _DEFAULT_SEGMENT_MODE

    sync_filter: dict = {}
    if month_start is not None:
        sync_filter = {"bot_segment_synced_at": {"$gte": month_start, "$lt": month_end}}

    total_users = metric(lambda: int(users_col.count_documents(sync_filter)))

    without_segment = None
    normalized_counts: dict[str, int] = {}
    try:
        blank_count = 0
        for doc in users_col.find(sync_filter, {"for_bot_segment": 1, "bot_segment": 1}):
            raw = doc.get("for_bot_segment")
            if _segment_is_blank(raw):
                raw = doc.get("bot_segment")
            if is_blank_or_unknown_for_bot_segment(raw):
                blank_count += 1
                continue
            normalized = normalize_for_bot_segment(raw)
            normalized_counts[normalized] = normalized_counts.get(normalized, 0) + 1
        without_segment = blank_count
    except Exception as exc:  # noqa: BLE001
        errors.append(f"segment_breakdown: {exc}")

    with_segment = None
    if total_users["value"] is not None and without_segment is not None:
        with_segment = int(total_users["value"]) - int(without_segment)

    top_segments = sorted(normalized_counts.items(), key=lambda kv: kv[1], reverse=True)[:_TOP_SEGMENTS_LIMIT]
    top_segments_rows = [{"segment": name, "count": count} for name, count in top_segments]

    filtered_count = None
    if segment_filter:
        filtered_count = normalized_counts.get(normalize_for_bot_segment(segment_filter))

    public_pool_claimed = metric(
        lambda: int(users_col.count_documents({**sync_filter, "has_ever_claimed_public_pool": True})),
        note="Users with has_ever_claimed_public_pool=true (existing field; no new classification).",
    )
    public_pool_not_claimed = None
    if total_users["value"] is not None and public_pool_claimed["value"] is not None:
        public_pool_not_claimed = int(total_users["value"]) - int(public_pool_claimed["value"])

    synced_count = metric(
        lambda: int(
            users_col.count_documents(
                sync_filter if month_start is not None else {"bot_segment_synced_at": {"$exists": True}}
            )
        ),
        quality="approx",
        note=f"Users with bot_segment_synced_at in {_segment_mode_label(mode, selected_month).lower()}." if month_start is not None
        else "Users with a bot_segment_synced_at timestamp recorded (all time).",
    )

    return {
        "success": True,
        "mode": mode,
        "mode_label": _segment_mode_label(mode, selected_month),
        "selected_month": selected_month,
        "as_of": now.isoformat(),
        "generated_at": now.isoformat(),
        "month_start": month_start.isoformat() if month_start else None,
        "month_end": month_end.isoformat() if month_end else None,
        "data_source": "users collection (for_bot_segment / bot_segment / bot_segment_synced_at — read only)",
        "summary": {
            "total_users": total_users,
            "users_with_segment": {"value": with_segment, "data_quality": "exact" if with_segment is not None else "missing",
                                    "note": "Users with a non-blank for_bot_segment or bot_segment value."},
            "users_without_segment": {"value": without_segment, "data_quality": "exact" if without_segment is not None else "missing",
                                       "note": "Users missing both for_bot_segment and bot_segment."},
            "public_pool_claimed": public_pool_claimed,
            "public_pool_not_claimed": {"value": public_pool_not_claimed,
                                         "data_quality": "exact" if public_pool_not_claimed is not None else "missing"},
            "recently_updated": synced_count,
        },
        "top_segments": top_segments_rows,
        "segment_filter": segment_filter or None,
        "filtered_count": filtered_count,
        "partial_errors": errors or None,
    }


# ---------------------------------------------------------------------------
# 4b. Validation panel (UIM vs Backend) — Phase 5
# ---------------------------------------------------------------------------

# Only these metrics have a clear, already-existing backend equivalent
# (total user count / existing segment counts). Everything else in
# uim_validation.METRIC_KEYS has no backend equivalent yet and is reported
# with backend_value=None / status="gray" rather than guessed at — in
# particular the claim-risk tiers, welcome-abuse/farming-risk invitee
# counts and "actual"/"old" player totals, none of which map to an
# existing segment or query without inventing new classification rules.
_VALIDATION_SEGMENT_METRIC_KEYS = {
    "high_value_players": ("high_value",),
    "new_player_total": ("new_user", "new_joiner"),
}


def _current_segment_counts(users_col) -> dict[str, int]:
    """Current (point-in-time) normalized segment counts from ``users``.

    Mirrors the counting logic in ``build_segments_panel`` (mode="snapshot")
    so the validation panel's backend numbers match what the Segment
    Overview tab already shows for "now".
    """
    from config import is_blank_or_unknown_for_bot_segment, normalize_for_bot_segment

    counts: dict[str, int] = {}
    for doc in users_col.find({}, {"for_bot_segment": 1, "bot_segment": 1}):
        raw = doc.get("for_bot_segment")
        if _segment_is_blank(raw):
            raw = doc.get("bot_segment")
        if is_blank_or_unknown_for_bot_segment(raw):
            continue
        normalized = normalize_for_bot_segment(raw)
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _compute_backend_validation_metrics(*, users_col) -> dict[str, int | None]:
    """Current backend-side values for the UIM "dashboard" KPI metrics.

    Only ``total_campaign_players`` and the segment-derived counts in
    ``_VALIDATION_SEGMENT_METRIC_KEYS`` are computable from existing data;
    every other metric in ``uim_validation.METRIC_KEYS`` is intentionally
    ``None`` (rendered as the "gray / missing source data" status) rather
    than invented, per the read-only/no-new-classification constraint.

    Always reads the live ``users`` collection — this release only compares
    "now vs now", matching the always-live UIM "dashboard" tab.
    """
    from uim_validation import METRIC_KEYS

    values: dict[str, int | None] = {key: None for key in METRIC_KEYS}
    values["total_campaign_players"] = int(users_col.count_documents({}))
    segment_counts = _current_segment_counts(users_col)
    for metric_key, segment_names in _VALIDATION_SEGMENT_METRIC_KEYS.items():
        values[metric_key] = sum(segment_counts.get(name, 0) for name in segment_names)
    return values


def _validation_compare(uim_value: Any, backend_value: Any) -> tuple[float | None, float | None, str]:
    """Return ``(difference, difference_pct, status)`` for one metric.

    Status thresholds: green <=1% variance, yellow >1% and <=5%, red >5%,
    gray when either side is missing. When the UIM value is exactly 0 but
    the backend differs, percentage variance is undefined (division by
    zero) so we report ``difference_pct=None`` but still flag red since an
    absolute mismatch exists.
    """
    if uim_value is None or backend_value is None:
        return None, None, "gray"
    difference = backend_value - uim_value
    if uim_value == 0:
        if backend_value == 0:
            return 0.0, 0.0, "green"
        return float(difference), None, "red"
    difference_pct = round(100.0 * difference / abs(uim_value), 2)
    abs_pct = abs(difference_pct)
    if abs_pct <= 1:
        status = "green"
    elif abs_pct <= 5:
        status = "yellow"
    else:
        status = "red"
    return float(difference), difference_pct, status


def _validation_metric_gap(metric_key: str) -> dict | None:
    """Phase 5B gap/mapping summary for one validation metric, or ``None``
    if this metric hasn't been documented yet (see ``uim_kpi_mapping.py``).
    Lets the Validation page explain *why* a metric is missing/mismatched
    instead of just showing a red/gray status with no context.
    """
    from uim_kpi_mapping import get_kpi_mapping_by_key

    entry = get_kpi_mapping_by_key(metric_key)
    if entry is None:
        return None
    return {
        "implementation_status": entry["implementation_status"],
        "backend_gap": entry["backend_gap"],
        "source_tab": entry["source_tab"],
        "confirmed": entry["confirmed"],
    }


def build_kpi_gap_report_panel(*, now: datetime | None = None) -> dict:
    """Phase 5B: read-only UIM formula mapping / backend KPI gap report.

    Documents, per UIM KPI, how UIM defines the metric, which sheet tab/
    columns it should come from, what (if anything) the backend currently
    uses in its place, and the resulting gap. Pure documentation — computes
    nothing live, touches no collections, and is safe to call with no DB at
    all (used by the admin dashboard's "Validation" page to explain why a
    metric is red/gray).
    """
    from uim_kpi_mapping import get_kpi_mapping

    now = now or _utc_now()
    mapping = get_kpi_mapping()
    status_counts: dict[str, int] = {}
    for entry in mapping:
        status_counts[entry["implementation_status"]] = (
            status_counts.get(entry["implementation_status"], 0) + 1
        )
    return {
        "success": True,
        "generated_at": now.isoformat(),
        "data_source": (
            "Phase 5B diagnostic mapping — documents intended UIM formulas "
            "and backend gaps; read only, no live computation"
        ),
        "summary": {
            "total_kpis_documented": len(mapping),
            "exact_available": status_counts.get("exact_available", 0),
            "backend_missing": status_counts.get("backend_missing", 0),
            "definition_mismatch": status_counts.get("definition_mismatch", 0),
            "source_missing": status_counts.get("source_missing", 0),
        },
        "kpis": mapping,
    }


def build_validation_panel(
    *,
    users_col,
    uim_result: dict,
    now: datetime | None = None,
) -> dict:
    """Build the UIM-vs-Backend validation comparison (Phase 5, read-only).

    ``uim_result`` is the dict returned by
    ``uim_validation.fetch_uim_validation_metrics`` — fetching/parsing the
    sheet is the caller's responsibility so this function (and its tests)
    never need real Google credentials or network access.

    Always compares the live UIM "dashboard" tab values against the live
    backend (``users`` collection) values, both as of "now". There is no
    historical/period mode in this release.
    """
    from uim_validation import METRIC_KEYS

    now = now or _utc_now()
    uim_ok = bool(uim_result.get("ok"))
    uim_values: dict = uim_result.get("values") or {}
    uim_notes: dict = uim_result.get("notes") or {}

    backend_values = _compute_backend_validation_metrics(users_col=users_col)

    metrics: list[dict] = []
    counts = {"green": 0, "yellow": 0, "red": 0, "gray": 0}
    for key in METRIC_KEYS:
        uim_v = uim_values.get(key)
        backend_v = backend_values.get(key)
        difference, difference_pct, status = _validation_compare(uim_v, backend_v)
        counts[status] += 1
        metrics.append(
            {
                "metric": key,
                "uim_value": uim_v,
                "backend_value": backend_v,
                "difference": difference,
                "difference_pct": difference_pct,
                "status": status,
                "uim_note": uim_notes.get(key),
                "gap": _validation_metric_gap(key),
            }
        )

    partial_errors = []
    if not uim_ok and uim_result.get("error"):
        partial_errors.append(uim_result["error"])

    return {
        "success": True,
        "generated_at": now.isoformat(),
        "data_source": 'UIM Google Sheet "dashboard" KPI tab vs backend dashboard calculations — read only',
        "uim_source": {
            "ok": uim_ok,
            "error": uim_result.get("error"),
            "spreadsheet_id": uim_result.get("spreadsheet_id"),
            "worksheet_title": uim_result.get("worksheet_title"),
        },
        "summary": {
            "total_metrics_compared": len(metrics),
            "matched_metrics": counts["green"],
            "warning_metrics": counts["yellow"],
            "failed_metrics": counts["red"],
            "missing_metrics": counts["gray"],
        },
        "metrics": metrics,
        "partial_errors": partial_errors or None,
    }


def build_backend_segment_engine_panel(
    *,
    snapshots_col,
    segment_snapshots_col=None,
    now: datetime | None = None,
    month: str | None = None,
    snapshot_week: str | None = None,
) -> dict:
    """Phase 3/4: read-only summary of the latest ``backend_segment_snapshots``.

    Shadow-mode dashboard view only — reads the snapshot collection written
    by ``backend_segment_engine.run_shadow_segment_engine``; never queries
    or writes ``users``, never touches segment classification, voucher
    allocation, public-pool probability, or reward logic.

    When ``snapshot_week`` (e.g. "2026-W24") is provided, filter by that week.
    Otherwise, filter by ``month`` (YYYY-MM), defaulting to the current month.

    Phase 4 additions (all read-only):
      - ``backend_counts_by_week``: backend snapshot counts grouped by week.
      - ``uim_counts_by_week``: UIM snapshot counts grouped by week (requires
        ``segment_snapshots_col``; empty dict when not supplied).
      - ``match_rate`` / ``mismatch_rate``: surfaced at top level for convenience.
      - ``mismatch_by_segment_pair``: mismatch count per (backend, uim) pair.
      - ``mismatch_details``: up to 200 mismatch rows with full metric fields.
    """
    now = now or _utc_now()

    if snapshot_week:
        query: dict = {"snapshot_week": snapshot_week}
        resolved_month: str | None = None
    else:
        resolved_month = month or f"{now.year:04d}-{now.month:02d}"
        query = {"snapshot_month": resolved_month}

    docs = list(snapshots_col.find(query))

    segment_counts: dict[str, int] = {}
    claim_risk_counts: dict[str, int] = {}
    age_distribution: dict[str, int] = {"new_player": 0, "old_player": 0, "unknown": 0}
    backend_counts_by_week: dict[str, int] = {}
    matches = 0
    mismatches = 0
    compared = 0
    mismatch_pair_counts: dict[tuple[str, str], int] = {}
    comparison_rows: list[dict] = []
    mismatch_details: list[dict] = []

    for doc in docs:
        seg = doc.get("backend_segment", "unclassified")
        segment_counts[seg] = segment_counts.get(seg, 0) + 1

        risk = doc.get("claim_risk_level", "normal")
        claim_risk_counts[risk] = claim_risk_counts.get(risk, 0) + 1

        age_type = doc.get("player_age_type")
        if age_type in age_distribution:
            age_distribution[age_type] += 1
        else:
            age_distribution["unknown"] += 1

        w = doc.get("snapshot_week") or "unknown"
        backend_counts_by_week[w] = backend_counts_by_week.get(w, 0) + 1

        comparison = doc.get("uim_comparison")
        if comparison is not None:
            compared += 1
            is_match = bool(comparison.get("match"))
            b_seg = comparison.get("backend_segment") or seg
            u_seg = comparison.get("uim_segment") or "unknown"
            if is_match:
                matches += 1
            else:
                mismatches += 1
                pair = (b_seg, u_seg)
                mismatch_pair_counts[pair] = mismatch_pair_counts.get(pair, 0) + 1
                if len(mismatch_details) < 200:
                    ms = doc.get("metrics_snapshot") or {}
                    mismatch_details.append(
                        {
                            "account": doc.get("account"),
                            "backend_segment": b_seg,
                            "uim_segment": u_seg,
                            "match": False,
                            "confidence": doc.get("confidence"),
                            "reason": doc.get("segment_reason"),
                            "after_total_bet_amount": ms.get("after_total_bet_amount"),
                            "withdraw_amount": ms.get("withdraw_amount"),
                            "claim_count": ms.get("claim_count"),
                            "referral_count": ms.get("referral_count"),
                            "checkin_count": ms.get("checkin_count"),
                        }
                    )
            if len(comparison_rows) < 50:
                comparison_rows.append(
                    {
                        "account": doc.get("account"),
                        "backend_segment": b_seg,
                        "uim_segment": u_seg,
                        "match": is_match,
                        "confidence": doc.get("confidence"),
                        "reason": doc.get("segment_reason"),
                    }
                )

    # UIM counts by week — graceful when no collection supplied.
    uim_counts_by_week: dict[str, int] = {}
    if segment_snapshots_col is not None:
        for udoc in segment_snapshots_col.find(query):
            uw = udoc.get("snapshot_week") or "unknown"
            uim_counts_by_week[uw] = uim_counts_by_week.get(uw, 0) + 1

    mismatch_by_segment_pair = sorted(
        [
            {"backend_segment": k[0], "uim_segment": k[1], "count": v}
            for k, v in mismatch_pair_counts.items()
        ],
        key=lambda x: x["count"],
        reverse=True,
    )

    total = len(docs)
    hv = segment_counts.get("high_value", 0)
    lv = segment_counts.get("low_value", 0)
    na = segment_counts.get("normal_actual", 0)
    actual_players = hv + lv + na
    match_rate = round(100.0 * matches / compared, 2) if compared else None
    mismatch_rate = round(100.0 * mismatches / compared, 2) if compared else None

    return {
        "success": True,
        "generated_at": now.isoformat(),
        "data_source": (
            "Phase 3 backend segment engine — SHADOW MODE. Read-only "
            "comparison/audit view; does not drive bot behaviour, voucher "
            "allocation, or reward logic. UIM for_bot_segment remains the "
            "production source of truth."
        ),
        "snapshot_month": resolved_month,
        "snapshot_week": snapshot_week or None,
        "summary": {
            "total_users_evaluated": total,
            "high_value": hv,
            "low_value": lv,
            "normal_actual": na,
            "actual_players": actual_players,
            "voucher_hunter": segment_counts.get("voucher_hunter", 0),
            "ghost": segment_counts.get("ghost", 0),
            "active_community_player": segment_counts.get("active_community_player", 0),
            "unclassified": segment_counts.get("unclassified", 0),
            "uim_compared": compared,
            "uim_matches": matches,
            "uim_mismatches": mismatches,
            "match_rate": match_rate,
            "mismatch_rate": mismatch_rate,
        },
        "match_rate": match_rate,
        "mismatch_rate": mismatch_rate,
        "segment_distribution": segment_counts,
        "claim_risk_distribution": claim_risk_counts,
        "player_age_distribution": age_distribution,
        "backend_counts_by_week": backend_counts_by_week,
        "uim_counts_by_week": uim_counts_by_week,
        "mismatch_by_segment_pair": mismatch_by_segment_pair,
        "mismatch_details": mismatch_details,
        "comparison_rows": comparison_rows,
    }


# ---------------------------------------------------------------------------
# Phase 5 — Backend vs UIM comparison analysis
# ---------------------------------------------------------------------------

def _takeover_static_dependency_inventory() -> list[dict]:
    """Phase 7A static audit of segment reads used for operating decisions."""
    return [
        {
            "file": "bot_segment_sync.py",
            "function": "sync_bot_segments_from_sheet() / _prepare_user_update()",
            "purpose": "Imports UIM Google Sheet for_bot_segment values into users and writes segment_snapshots history.",
            "reads": ["for_bot_segment", "Google Sheet segment", "UIM segment"],
            "current_source": "UIM user_profile_summary Google Sheet",
            "recommended_backend_source": "backend_segment_snapshots.backend_segment, later materialized as users.final_segment",
            "business_decision": "Upstream feed for all bot-facing segment decisions.",
            "risk": "high",
        },
        {
            "file": "vouchers.py",
            "function": "_load_user_bot_segment()",
            "purpose": "Loads a user's bot-facing segment and derives public-pool probability.",
            "reads": ["for_bot_segment", "bot_segment", "for_bot_segment_normalized", "bot_segment_probability"],
            "current_source": "users fields populated by bot_segment_sync.py from UIM Google Sheet",
            "recommended_backend_source": "latest backend_segment_snapshots.backend_segment; Phase 7B materialized users.final_segment",
            "business_decision": "Public Pool / SVD traffic shaping.",
            "risk": "high",
        },
        {
            "file": "vouchers.py",
            "function": "assign_public_pool_access_once()",
            "purpose": "Applies segment probability and new-user SVD boost before creating access assignment.",
            "reads": ["for_bot_segment", "bot_segment"],
            "current_source": "users fields populated by UIM Google Sheet sync or injected user_doc",
            "recommended_backend_source": "users.final_segment with backend_segment_snapshots fallback while dual-running",
            "business_decision": "Public Pool allocation, delayed eligibility, and SVD boost.",
            "risk": "high",
        },
        {
            "file": "config.py",
            "function": "normalize_for_bot_segment() / public_pool_probability_for_bot_segment() / is_new_user_segment()",
            "purpose": "Maps manual/UIM labels to canonical segment names and allocation probabilities.",
            "reads": ["for_bot_segment", "bot_segment"],
            "current_source": "Manual label vocabulary shared by UIM Sheet sync and voucher logic",
            "recommended_backend_source": "Backend segment vocabulary plus a backend-owned probability policy",
            "business_decision": "Segment normalization, public-pool probability, new-user SVD boost.",
            "risk": "high",
        },
        {
            "file": "main.py",
            "function": "bot_segment_sheet_sync_scheduled()",
            "purpose": "Scheduled UIM Sheet sync keeps users.for_bot_segment current.",
            "reads": ["Google Sheet segment", "UIM segment"],
            "current_source": "UIM Google Sheet via bot_segment_sync.sync_bot_segments_from_sheet()",
            "recommended_backend_source": "Backend segment engine scheduled run plus users.final_segment read model",
            "business_decision": "Operational freshness of production segment labels.",
            "risk": "high",
        },
        {
            "file": "main.py",
            "function": "_count_segment() / legacy admin summary metrics",
            "purpose": "Counts users by for_bot_segment/bot_segment for dashboard abuse and segment KPIs.",
            "reads": ["for_bot_segment", "bot_segment", "UIM segment"],
            "current_source": "users collection bot-facing segment fields",
            "recommended_backend_source": "backend_segment_snapshots.backend_segment or users.final_segment",
            "business_decision": "Admin monitoring and operational interpretation.",
            "risk": "medium",
        },
        {
            "file": "dashboard_panels.py",
            "function": "build_segments_panel() / _current_segment_counts()",
            "purpose": "Builds Segment Overview and validation counts from legacy user segment fields.",
            "reads": ["for_bot_segment", "bot_segment"],
            "current_source": "users collection bot-facing segment fields",
            "recommended_backend_source": "backend_segment_snapshots.backend_segment; users.final_segment after Phase 7B",
            "business_decision": "Admin reporting and validation dashboard.",
            "risk": "medium",
        },
        {
            "file": "backend_segment_engine.py",
            "function": "run_shadow_segment_engine() / compare_with_uim()",
            "purpose": "Reads legacy UIM segment only to compare backend output with current production labels.",
            "reads": ["for_bot_segment", "bot_segment", "UIM segment"],
            "current_source": "users fields populated from UIM Google Sheet",
            "recommended_backend_source": "Keep only as dual-run comparison in Phase 7A/7B; remove in Phase 7C",
            "business_decision": "No production behavior; migration validation only.",
            "risk": "low",
        },
        {
            "file": "uim_validation.py",
            "function": "fetch_uim_validation_metrics()",
            "purpose": "Reads UIM dashboard tab for KPI comparison.",
            "reads": ["UIM segment", "Google Sheet segment"],
            "current_source": "UIM Google Sheet dashboard tab",
            "recommended_backend_source": "Backend KPI read models and backend_segment_snapshots",
            "business_decision": "Validation/reporting only.",
            "risk": "medium",
        },
        {
            "file": "uim_kpi_mapping.py",
            "function": "get_kpi_mapping()",
            "purpose": "Documents formula/source gaps between UIM and backend metrics.",
            "reads": ["UIM segment", "marketing segment", "manual segment mapping"],
            "current_source": "Static mapping of UIM definitions and backend gaps",
            "recommended_backend_source": "Backend-owned KPI definitions plus final_segment mapping",
            "business_decision": "Migration planning only.",
            "risk": "low",
        },
    ]


def _takeover_voucher_allocation_audit() -> list[dict]:
    return [
        {"system": "Public Pool", "current_dependency": "UIM/Sheet via users.for_bot_segment or users.bot_segment", "current_logic": "Segment probability map in config.py controls access probability and delay.", "recommended_replacement": "Use users.final_segment from backend engine; keep UIM fallback and mismatch logs in Phase 7B.", "migration_risk": "high", "safe_to_migrate_first": False},
        {"system": "SVD", "current_dependency": "Manual new-user logic derived from normalized for_bot_segment/new_joiner labels", "current_logic": "assign_public_pool_access_once() boosts first three assignments for new_user/new_joiner to 100%.", "recommended_replacement": "Backend final_segment plus backend player_age_type for new-player handling.", "migration_risk": "high", "safe_to_migrate_first": False},
        {"system": "Welcome Voucher", "current_dependency": "No direct UIM segment dependency found in claim gate; uses welcome_eligibility and joined-main timing.", "current_logic": "Eligibility and lifecycle collections drive visibility/claiming.", "recommended_replacement": "No segment replacement required; optionally compare backend player_age_type for audit only.", "migration_risk": "low", "safe_to_migrate_first": True},
        {"system": "Special Voucher", "current_dependency": "No direct for_bot_segment/bot_segment dependency found; uses drop audience/eligibility allow lists and restrictions.", "current_logic": "Manual admin-created drop eligibility and personalized/pooled voucher assignment.", "recommended_replacement": "If future segment allow lists are added, read users.final_segment only.", "migration_risk": "medium", "safe_to_migrate_first": True},
        {"system": "Campaign Voucher", "current_dependency": "No direct segment filtering found except Public Pool shaping path for public pooled drops.", "current_logic": "Drop audience, pool availability, channel subscription, manual restrictions, and public-pool shaping.", "recommended_replacement": "Separate campaign audience from segment; where segment is required, read backend final_segment.", "migration_risk": "medium", "safe_to_migrate_first": True},
    ]


def _takeover_campaign_eligibility_audit() -> list[dict]:
    return [
        {"campaign": "VIP Campaigns", "current_source": "XP/referral-derived vip_tier/status, not UIM segment", "future_source": "Keep VIP source separate; optionally enrich with backend final_segment for targeting.", "migration_risk": "low"},
        {"campaign": "High Value Campaigns", "current_source": "Legacy for_bot_segment only where operators manually target high_value; dashboard counts use UIM labels.", "future_source": "backend_segment_snapshots.backend_segment == high_value, materialized to users.final_segment.", "migration_risk": "medium"},
        {"campaign": "Low Value Reactivation", "current_source": "channel_reactivation.py uses subscription/reward status, not segment; any low-value targeting is manual/operator-side.", "future_source": "users.final_segment == low_value plus backend recency/activity guardrails.", "migration_risk": "medium"},
        {"campaign": "Voucher Hunter Restrictions", "current_source": "UIM/Sheet for_bot_segment currently informs voucher_hunter probability; restrictions.no_campaign is separate manual user flag.", "future_source": "backend voucher_hunter final_segment plus explicit restrictions flag where needed.", "migration_risk": "high"},
        {"campaign": "XP Campaigns", "current_source": "XP/check-in/referral collections; no direct UIM segment gate found.", "future_source": "Keep XP source separate; use final_segment only for campaign audience reporting.", "migration_risk": "low"},
    ]


def _takeover_segment_readiness() -> list[dict]:
    return [
        {"segment": "high_value", "status": "READY", "explanation": "Backend rule has high-confidence after-bet/withdrawal source fields and deterministic 8x rule."},
        {"segment": "low_value", "status": "READY", "explanation": "Backend rule has high-confidence after-bet/withdrawal source fields and deterministic below-8x rule."},
        {"segment": "normal_actual", "status": "READY", "explanation": "Backend rule has high-confidence play activity rule after VH and withdrawal buckets are excluded."},
        {"segment": "voucher_hunter", "status": "PARTIAL", "explanation": "VH v2 exists, but prior quality/false-positive analysis shows this is the highest-risk behavior gate and needs dual-run mismatch logging before primary use."},
        {"segment": "active_community_player", "status": "PARTIAL", "explanation": "Backend emits active_community_player from provisional XP/check-in rules with low confidence; needs business sign-off."},
        {"segment": "ghost", "status": "PARTIAL", "explanation": "Rule is deterministic when marketing data is present, but inactivity semantics are sensitive to missing marketing snapshots."},
        {"segment": "unclassified", "status": "NOT READY", "explanation": "Fallback for missing or insufficient data; should not drive production targeting except as an explicit fallback bucket."},
    ]


def _takeover_rollout_plan() -> list[dict]:
    return [
        {"phase": "7A", "name": "Dual-read readiness", "behavior": "Read backend and UIM side-by-side, log mismatches, no behavior changes.", "deliverables": ["dependency inventory", "mismatch logging plan", "coverage dashboard", "risk-ranked rollout order"]},
        {"phase": "7B", "name": "Backend primary with UIM fallback", "behavior": "Operational reads prefer users.final_segment; UIM remains fallback when backend segment is missing or stale.", "deliverables": ["final_segment read model", "fallback counters", "per-system migration toggles"]},
        {"phase": "7C", "name": "Backend only", "behavior": "Remove UIM dependency from operational reads after mismatch and stale-data thresholds are acceptable.", "deliverables": ["remove bot_segment production reads", "retire UIM sync from runtime path", "keep historical snapshots read-only"]},
    ]


def _nested_get(doc: dict, dotted: str, default=None):
    current = doc
    for part in dotted.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current.get(part)
    return current


def build_backend_segment_takeover_readiness_panel(
    *,
    users_col=None,
    snapshots_col=None,
    snapshot_week: str | None = None,
    now: datetime | None = None,
) -> dict:
    """Phase 7A: read-only migration report for backend segment takeover."""
    now = now or _utc_now()
    live_errors: list[str] = []
    users_with_for_bot_segment = None
    users_with_bot_segment = None
    backend_snapshot_count = None
    backend_compared = None
    backend_mismatches = None
    latest_snapshot_week = None

    if users_col is not None:
        try:
            users_with_for_bot_segment = int(users_col.count_documents({"for_bot_segment": {"$exists": True, "$ne": ""}}))
            users_with_bot_segment = int(users_col.count_documents({"bot_segment": {"$exists": True, "$ne": ""}}))
        except Exception as exc:
            live_errors.append(f"users_segment_counts: {exc}")

    if snapshots_col is not None:
        try:
            if snapshot_week:
                latest_snapshot_week = snapshot_week
            else:
                distinct_weeks = sorted(w for w in snapshots_col.distinct("snapshot_week") if w)
                latest_snapshot_week = distinct_weeks[-1] if distinct_weeks else None
            query = {"snapshot_week": latest_snapshot_week} if latest_snapshot_week else {}
            docs = list(snapshots_col.find(query, {"uim_comparison": 1, "backend_segment": 1}))
            backend_snapshot_count = len(docs)
            compared_docs = [d for d in docs if _nested_get(d, "uim_comparison.match") is not None]
            backend_compared = len(compared_docs)
            backend_mismatches = sum(1 for d in compared_docs if _nested_get(d, "uim_comparison.match") is False)
        except Exception as exc:
            live_errors.append(f"backend_snapshot_counts: {exc}")

    dependency_inventory = _takeover_static_dependency_inventory()
    return {
        "success": True,
        "generated_at": now.isoformat(),
        "phase": "7A",
        "data_source": "Read-only code audit plus live segment coverage counters. No writes, no segment changes, no voucher changes, no allocation changes.",
        "live_coverage": {
            "users_with_for_bot_segment": users_with_for_bot_segment,
            "users_with_bot_segment": users_with_bot_segment,
            "backend_snapshot_count": backend_snapshot_count,
            "latest_snapshot_week": latest_snapshot_week,
            "backend_uim_compared": backend_compared,
            "backend_uim_mismatches": backend_mismatches,
            "partial_errors": live_errors or None,
        },
        "section_1_segment_dependency_audit": dependency_inventory,
        "section_2_voucher_allocation": _takeover_voucher_allocation_audit(),
        "section_3_campaign_eligibility": _takeover_campaign_eligibility_audit(),
        "section_4_segment_readiness_assessment": _takeover_segment_readiness(),
        "section_5_dual_run_plan": _takeover_rollout_plan(),
        "success_criteria_answers": {
            "where_is_uim_still_used": [
                "Production segment feed: bot_segment_sync.py -> users.for_bot_segment.",
                "Voucher public-pool/SVD shaping: vouchers.py reads users.for_bot_segment/users.bot_segment.",
                "Admin reporting and validation: main.py/dashboard_panels.py/uim_validation.py.",
                "Backend segment engine comparison only: backend_segment_engine.py reads UIM fields for mismatch analysis.",
            ],
            "safe_to_migrate_first": [
                "Read-only dashboards and validation panels.",
                "Welcome Voucher reporting, because claim gates do not directly depend on UIM segment.",
                "VIP/XP campaign reporting, because primary eligibility is XP/referral/status based.",
            ],
            "high_risk_systems": [
                "Public Pool allocation",
                "SVD new-user boost",
                "Voucher Hunter restrictions/probability",
                "Any operator-managed high_value/low_value campaign audience that currently relies on UIM labels",
            ],
            "blockers_before_backend_source_of_truth": [
                "Create users.final_segment read model populated from backend_segment_snapshots.",
                "Define stale/missing backend segment fallback behavior.",
                "Add per-decision mismatch logging in Phase 7A.",
                "Sign off active_community_player and ghost production semantics.",
                "Set acceptable mismatch thresholds for voucher_hunter before using it in restrictions.",
            ],
            "estimated_effort_to_remove_uim_entirely": "Medium: about 3-5 focused engineering days after Phase 7A data confirms acceptable mismatch rates; longer if voucher_hunter or active_community_player require rule changes.",
        },
        "risk_assessment": {
            "high": [d for d in dependency_inventory if d["risk"] == "high"],
            "medium": [d for d in dependency_inventory if d["risk"] == "medium"],
            "low": [d for d in dependency_inventory if d["risk"] == "low"],
        },
        "recommended_rollout_order": [
            "Dashboards and validation reads",
            "Welcome/VIP/XP reporting surfaces",
            "High-value and low-value campaign audience reads",
            "Public Pool and SVD with UIM fallback",
            "Voucher Hunter restrictions after mismatch thresholds are accepted",
            "Remove UIM sync from operational runtime",
        ],
    }


_UIMC_PROJ = {
    "_id": 0,
    "account": 1,
    "backend_segment": 1,
    "player_age_type": 1,
    "claim_risk_level": 1,
    "confidence": 1,
    "segment_reason": 1,
    "uim_comparison": 1,
    "metrics_snapshot": 1,
}


def build_uim_comparison_panel(
    *,
    snapshots_col,
    segment_snapshots_col=None,
    snapshot_week: str,
    filter_backend_segment: str | None = None,
    filter_uim_segment: str | None = None,
    filter_match: bool | None = None,
    filter_claim_risk_level: str | None = None,
    page: int = 1,
    per_page: int = 200,
    now: datetime | None = None,
) -> dict:
    """Phase 5: read-only Backend vs UIM comparison panel.

    Reads backend_segment_snapshots for snapshot_week and returns:
    - Comparison summary (match/mismatch rates, totals)
    - Full cross-tab matrix (backend_segment rows × uim_segment columns)
    - Paginated, filterable detail rows (with player_age_type, claim_risk_level)
    - Rule audit (per-segment average input metrics for manual tuning)

    Summary stats and rule audit always reflect the full unfiltered dataset.
    Filters only narrow the detail row list.

    Never writes, never touches segment classification or production fields.
    """
    now = now or _utc_now()
    page = max(1, int(page))
    per_page = max(1, min(500, int(per_page)))

    # --- Load all docs for the week with a tight projection ---
    all_docs = list(snapshots_col.find({"snapshot_week": snapshot_week}, _UIMC_PROJ))
    total_backend_users = len(all_docs)

    # UIM total: from segment_snapshots_col if provided; else count backend
    # docs that had a UIM comparison (i.e. had for_bot_segment at engine time).
    if segment_snapshots_col is not None:
        total_uim_users: int = segment_snapshots_col.count_documents(
            {"snapshot_week": snapshot_week}
        )
    else:
        total_uim_users = sum(1 for d in all_docs if d.get("uim_comparison") is not None)

    compared = 0
    matched = 0
    mismatched = 0

    # cross-tab: matrix[backend_seg][uim_seg] = count (all comparisons, not just mismatches)
    matrix: dict[str, dict[str, int]] = {}
    backend_segs_seen: set[str] = set()
    uim_segs_seen: set[str] = set()
    mismatch_pair_counts: dict[tuple[str, str], int] = {}

    # rule audit accumulators (per backend_segment, over all docs regardless of UIM match)
    audit_acc: dict[str, dict] = {}

    for doc in all_docs:
        b_seg = doc.get("backend_segment") or "unclassified"
        ms = doc.get("metrics_snapshot") or {}

        if b_seg not in audit_acc:
            audit_acc[b_seg] = {
                "count": 0,
                "sum_atb": 0.0, "n_atb": 0,
                "sum_wd": 0.0,  "n_wd": 0,
                "sum_claim": 0, "sum_ref": 0, "sum_checkin": 0,
            }
        a = audit_acc[b_seg]
        a["count"] += 1

        atb = ms.get("after_total_bet_amount")
        if atb is not None:
            try:
                a["sum_atb"] += float(atb)
                a["n_atb"] += 1
            except (TypeError, ValueError):
                pass

        wd = ms.get("withdraw_amount")
        if wd is not None:
            try:
                a["sum_wd"] += float(wd)
                a["n_wd"] += 1
            except (TypeError, ValueError):
                pass

        try:
            a["sum_claim"]   += int(ms.get("claim_count",   0) or 0)
            a["sum_ref"]     += int(ms.get("referral_count", 0) or 0)
            a["sum_checkin"] += int(ms.get("checkin_count",  0) or 0)
        except (TypeError, ValueError):
            pass

        cmp = doc.get("uim_comparison")
        if cmp is None:
            continue

        compared += 1
        is_match = bool(cmp.get("match"))
        u_seg = cmp.get("uim_segment") or "unknown"

        if is_match:
            matched += 1
        else:
            mismatched += 1
            key = (b_seg, u_seg)
            mismatch_pair_counts[key] = mismatch_pair_counts.get(key, 0) + 1

        backend_segs_seen.add(b_seg)
        uim_segs_seen.add(u_seg)

        if b_seg not in matrix:
            matrix[b_seg] = {}
        matrix[b_seg][u_seg] = matrix[b_seg].get(u_seg, 0) + 1

    match_rate    = round(100.0 * matched    / compared, 2) if compared else None
    mismatch_rate = round(100.0 * mismatched / compared, 2) if compared else None

    top_mismatch_pairs = sorted(
        [
            {
                "backend_segment": k[0],
                "uim_segment": k[1],
                "count": v,
                "percentage_of_compared_users": round(v / compared * 100, 1) if compared else 0.0,
            }
            for k, v in mismatch_pair_counts.items()
        ],
        key=lambda x: x["count"],
        reverse=True,
    )[:20]

    # --- Apply filters, build paginated detail list ---
    detail_all: list[dict] = []
    for doc in all_docs:
        b_seg   = doc.get("backend_segment") or "unclassified"
        risk    = doc.get("claim_risk_level") or "normal"
        cmp     = doc.get("uim_comparison")
        u_seg   = (cmp.get("uim_segment") if cmp else None) or None
        is_match = bool(cmp.get("match")) if cmp is not None else None

        if filter_backend_segment and b_seg != filter_backend_segment:
            continue
        if filter_uim_segment and u_seg != filter_uim_segment:
            continue
        if filter_match is not None:
            if is_match is None or is_match != filter_match:
                continue
        if filter_claim_risk_level and risk != filter_claim_risk_level:
            continue

        ms = doc.get("metrics_snapshot") or {}
        detail_all.append({
            "account":               doc.get("account"),
            "backend_segment":       b_seg,
            "uim_segment":           u_seg,
            "match":                 is_match,
            "confidence":            doc.get("confidence"),
            "reason":                doc.get("segment_reason"),
            "after_total_bet_amount": ms.get("after_total_bet_amount"),
            "withdraw_amount":       ms.get("withdraw_amount"),
            "claim_count":           ms.get("claim_count"),
            "referral_count":        ms.get("referral_count"),
            "checkin_count":         ms.get("checkin_count"),
            "player_age_type":       doc.get("player_age_type"),
            "claim_risk_level":      risk,
        })

    total_details = len(detail_all)
    start = (page - 1) * per_page
    paged = detail_all[start : start + per_page]

    # --- Rule audit output ---
    rule_audit: dict[str, dict] = {}
    for seg, a in audit_acc.items():
        n = a["count"]
        rule_audit[seg] = {
            "count": n,
            "avg_after_total_bet_amount": round(a["sum_atb"] / a["n_atb"], 4) if a["n_atb"] else None,
            "avg_withdraw_amount":        round(a["sum_wd"]  / a["n_wd"],  4) if a["n_wd"]  else None,
            "avg_claim_count":            round(a["sum_claim"]   / n, 4) if n else None,
            "avg_referral_count":         round(a["sum_ref"]     / n, 4) if n else None,
            "avg_checkin_count":          round(a["sum_checkin"] / n, 4) if n else None,
        }

    # --- Mismatch matrix output ---
    all_b = sorted(backend_segs_seen)
    all_u = sorted(uim_segs_seen)
    matrix_rows = [
        {
            "backend_segment":   b,
            "by_uim_segment":    {u: matrix.get(b, {}).get(u, 0) for u in all_u},
        }
        for b in all_b
    ]

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "filters": {
            "snapshot_week":       snapshot_week,
            "backend_segment":     filter_backend_segment,
            "uim_segment":         filter_uim_segment,
            "match":               filter_match,
            "claim_risk_level":    filter_claim_risk_level,
        },
        "summary": {
            "total_backend_users": total_backend_users,
            "total_uim_users":     total_uim_users,
            "compared_users":      compared,
            "matched_users":       matched,
            "mismatched_users":    mismatched,
            "match_rate":          match_rate,
            "mismatch_rate":       mismatch_rate,
        },
        "mismatch_matrix": {
            "backend_segments": all_b,
            "uim_segments":     all_u,
            "rows":             matrix_rows,
        },
        "top_mismatch_pairs": top_mismatch_pairs,
        "details":       paged,
        "total_details": total_details,
        "page":          page,
        "per_page":      per_page,
        "has_more":      start + per_page < total_details,
        "rule_audit":    rule_audit,
    }


# ---------------------------------------------------------------------------
# 5b. Voucher Hunter Mismatch Audit (Phase 5B)
# ---------------------------------------------------------------------------

_FOCUS_SEGMENTS = {
    "unclassified", "normal_actual", "low_value",
    "high_value", "ghost", "active_community_player",
}

_VHMA_PROJ = {
    "_id": 0,
    "account": 1,
    "backend_segment": 1,
    "player_age_type": 1,
    "claim_risk_level": 1,
    "confidence": 1,
    "segment_reason": 1,
    "uim_comparison": 1,
    "metrics_snapshot": 1,
}


def build_voucher_hunter_mismatch_audit(
    *,
    snapshots_col,
    snapshot_week: str,
    sample_limit: int = 20,
    now: datetime | None = None,
) -> dict:
    """Phase 5B: read-only audit explaining why users with uim_segment=voucher_hunter
    were not classified as voucher_hunter by the backend engine.

    Queries backend_segment_snapshots for rows where uim_comparison.uim_segment
    equals 'voucher_hunter'. Groups by backend_segment, computing average input
    metrics and player age counts. Returns top-20 sample users per mismatch group.

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    query: dict = {
        "snapshot_week": snapshot_week,
        "uim_comparison.uim_segment": "voucher_hunter",
    }
    docs = list(snapshots_col.find(query, _VHMA_PROJ))

    total_voucher_hunter = len(docs)

    # --- Accumulators per backend_segment ---
    acc: dict[str, dict] = {}
    samples: dict[str, list] = {}

    for doc in docs:
        b_seg = doc.get("backend_segment") or "unclassified"
        ms = doc.get("metrics_snapshot") or {}
        cmp = doc.get("uim_comparison") or {}
        is_match = bool(cmp.get("match"))

        if b_seg not in acc:
            acc[b_seg] = {
                "user_count": 0,
                "sum_atb": 0.0, "n_atb": 0,
                "sum_wd": 0.0, "n_wd": 0,
                "sum_claim": 0,
                "sum_ref": 0,
                "sum_checkin": 0,
                "new_player_count": 0,
                "old_player_count": 0,
                "mismatch_count": 0,
            }
        a = acc[b_seg]
        a["user_count"] += 1
        if not is_match:
            a["mismatch_count"] += 1

        atb = ms.get("after_total_bet_amount")
        if atb is not None:
            try:
                a["sum_atb"] += float(atb)
                a["n_atb"] += 1
            except (TypeError, ValueError):
                pass

        wd = ms.get("withdraw_amount")
        if wd is not None:
            try:
                a["sum_wd"] += float(wd)
                a["n_wd"] += 1
            except (TypeError, ValueError):
                pass

        try:
            a["sum_claim"]   += int(ms.get("claim_count",   0) or 0)
            a["sum_ref"]     += int(ms.get("referral_count", 0) or 0)
            a["sum_checkin"] += int(ms.get("checkin_count",  0) or 0)
        except (TypeError, ValueError):
            pass

        age = doc.get("player_age_type") or ""
        if age == "new_player":
            a["new_player_count"] += 1
        elif age == "old_player":
            a["old_player_count"] += 1

        # Collect samples (up to sample_limit per segment, mismatches first)
        if b_seg not in samples:
            samples[b_seg] = []
        if not is_match and len(samples[b_seg]) < sample_limit:
            samples[b_seg].append({
                "account":                doc.get("account"),
                "backend_segment":        b_seg,
                "uim_segment":            cmp.get("uim_segment"),
                "after_total_bet_amount": ms.get("after_total_bet_amount"),
                "withdraw_amount":        ms.get("withdraw_amount"),
                "claim_count":            ms.get("claim_count"),
                "referral_count":         ms.get("referral_count"),
                "checkin_count":          ms.get("checkin_count"),
                "player_age_type":        doc.get("player_age_type"),
                "claim_risk_level":       doc.get("claim_risk_level"),
                "confidence":             doc.get("confidence"),
                "reason":                 doc.get("segment_reason"),
            })

    total_mismatches = sum(
        a["mismatch_count"] for a in acc.values()
        if a.get("mismatch_count", 0) > 0
    )

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    segment_breakdown: list[dict] = []
    for seg, a in sorted(acc.items(), key=lambda x: -x[1]["mismatch_count"]):
        n = a["user_count"]
        mc = a["mismatch_count"]
        segment_breakdown.append({
            "backend_segment":            seg,
            "user_count":                 n,
            "mismatch_count":             mc,
            "avg_after_total_bet_amount": round(a["sum_atb"] / a["n_atb"], 4) if a["n_atb"] else None,
            "avg_withdraw_amount":        round(a["sum_wd"]  / a["n_wd"],  4) if a["n_wd"]  else None,
            "avg_claim_count":            round(a["sum_claim"]   / n, 4) if n else None,
            "avg_referral_count":         round(a["sum_ref"]     / n, 4) if n else None,
            "avg_checkin_count":          round(a["sum_checkin"] / n, 4) if n else None,
            "avg_claim_risk_score":       None,  # not stored as numeric — see claim_risk_level in samples
            "new_player_count":           a["new_player_count"],
            "old_player_count":           a["old_player_count"],
            "pct_of_voucher_hunter":      _pct(mc, total_voucher_hunter),
            "pct_of_mismatches":          _pct(mc, total_mismatches),
        })

    summary_table = [
        {
            "backend_segment":       row["backend_segment"],
            "users":                 row["mismatch_count"],
            "pct_of_voucher_hunter": row["pct_of_voucher_hunter"],
            "pct_of_mismatches":     row["pct_of_mismatches"],
        }
        for row in segment_breakdown
        if row["backend_segment"] in _FOCUS_SEGMENTS and row["mismatch_count"] > 0
    ]

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "totals": {
            "total_voucher_hunter_uim_users": total_voucher_hunter,
            "total_mismatches": total_mismatches,
        },
        "segment_breakdown": segment_breakdown,
        "summary_table": summary_table,
        "sample_users": {
            seg: lst
            for seg, lst in samples.items()
            if seg in _FOCUS_SEGMENTS
        },
    }


# ---------------------------------------------------------------------------
# 4b. Unclassified Audit (Phase 5C)
# ---------------------------------------------------------------------------

_UNCA_PROJ = {
    "_id": 0,
    "account": 1,
    "backend_segment": 1,
    "player_age_type": 1,
    "claim_risk_level": 1,
    "confidence": 1,
    "segment_reason": 1,
    "metrics_snapshot": 1,
}


def build_unclassified_audit(
    *,
    snapshots_col,
    snapshot_week: str,
    sample_limit: int = 20,
    now: datetime | None = None,
) -> dict:
    """Phase 5C: read-only audit explaining why users fell into 'unclassified'.

    Queries backend_segment_snapshots for rows where backend_segment='unclassified'.
    Returns summary KPIs, claim risk breakdown, activity bucket breakdown,
    top reasons, and sample users per bucket.

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    docs = list(snapshots_col.find(
        {"snapshot_week": snapshot_week, "backend_segment": "unclassified"},
        _UNCA_PROJ,
    ))

    total_backend = snapshots_col.count_documents({"snapshot_week": snapshot_week})
    total_unclassified = len(docs)

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    # --- Summary KPIs ---
    sum_atb = 0.0; n_atb = 0
    sum_wd = 0.0; n_wd = 0
    sum_claim = 0; sum_ref = 0; sum_checkin = 0
    new_count = 0; old_count = 0

    for doc in docs:
        ms = doc.get("metrics_snapshot") or {}
        atb = ms.get("after_total_bet_amount")
        wd  = ms.get("withdraw_amount")
        try:
            if atb is not None:
                sum_atb += float(atb); n_atb += 1
        except (TypeError, ValueError):
            pass
        try:
            if wd is not None:
                sum_wd += float(wd); n_wd += 1
        except (TypeError, ValueError):
            pass
        try:
            sum_claim   += int(ms.get("claim_count",   0) or 0)
            sum_ref     += int(ms.get("referral_count", 0) or 0)
            sum_checkin += int(ms.get("checkin_count",  0) or 0)
        except (TypeError, ValueError):
            pass
        age = doc.get("player_age_type") or ""
        if age == "new_player":
            new_count += 1
        elif age == "old_player":
            old_count += 1

    n = total_unclassified or 1  # avoid div/0

    summary_kpis = {
        "total_backend_users":  total_backend,
        "unclassified_users":   total_unclassified,
        "unclassified_pct":     _pct(total_unclassified, total_backend),
        "new_players":          new_count,
        "old_players":          old_count,
        "avg_after_bet":        round(sum_atb / n_atb, 4) if n_atb else 0.0,
        "avg_withdraw":         round(sum_wd  / n_wd,  4) if n_wd  else 0.0,
        "avg_claims":           round(sum_claim   / n, 4),
        "avg_referrals":        round(sum_ref     / n, 4),
        "avg_checkins":         round(sum_checkin / n, 4),
    }

    # --- Claim Risk Breakdown ---
    risk_acc: dict[str, int] = {}
    for doc in docs:
        risk = doc.get("claim_risk_level") or "normal"
        risk_acc[risk] = risk_acc.get(risk, 0) + 1

    claim_risk_breakdown = sorted(
        [
            {
                "claim_risk": risk,
                "users": cnt,
                "percentage": _pct(cnt, total_unclassified),
            }
            for risk, cnt in risk_acc.items()
        ],
        key=lambda x: -x["users"],
    )

    # --- Activity Bucket Classification ---
    def _bucket(ms: dict) -> str:
        try:
            atb  = float(ms.get("after_total_bet_amount") or 0)
            wd   = float(ms.get("withdraw_amount") or 0)
            cl   = int(ms.get("claim_count", 0) or 0)
            chk  = int(ms.get("checkin_count", 0) or 0)
        except (TypeError, ValueError):
            return "other"
        if wd > 0:
            return "withdraw_user"
        if atb > 0 and wd == 0:
            return "play_no_withdraw"
        if atb == 0 and cl > 2:
            return "claim_only"
        if atb == 0 and wd == 0 and cl <= 2 and chk <= 2:
            return "inactive_light"
        return "other"

    bucket_acc: dict[str, int] = {}
    bucket_samples: dict[str, list] = {}

    for doc in docs:
        ms = doc.get("metrics_snapshot") or {}
        bkt = _bucket(ms)
        bucket_acc[bkt] = bucket_acc.get(bkt, 0) + 1
        if bkt not in bucket_samples:
            bucket_samples[bkt] = []
        if len(bucket_samples[bkt]) < sample_limit:
            bucket_samples[bkt].append({
                "account":    doc.get("account"),
                "after_bet":  ms.get("after_total_bet_amount"),
                "withdraw":   ms.get("withdraw_amount"),
                "claims":     ms.get("claim_count"),
                "referrals":  ms.get("referral_count"),
                "checkins":   ms.get("checkin_count"),
                "age_type":   doc.get("player_age_type"),
                "claim_risk": doc.get("claim_risk_level"),
                "confidence": doc.get("confidence"),
                "reason":     doc.get("segment_reason"),
            })

    _BUCKET_ORDER = ["inactive_light", "claim_only", "play_no_withdraw", "withdraw_user", "other"]
    activity_buckets = [
        {
            "bucket":     bkt,
            "users":      bucket_acc.get(bkt, 0),
            "percentage": _pct(bucket_acc.get(bkt, 0), total_unclassified),
        }
        for bkt in _BUCKET_ORDER
        if bucket_acc.get(bkt, 0) > 0
    ]

    # --- Top Reasons ---
    reason_acc: dict[str, int] = {}
    for doc in docs:
        reason = (doc.get("segment_reason") or "").strip() or "unknown"
        reason_acc[reason] = reason_acc.get(reason, 0) + 1

    top_reasons = sorted(
        [
            {
                "reason":     r,
                "users":      cnt,
                "percentage": _pct(cnt, total_unclassified),
            }
            for r, cnt in reason_acc.items()
        ],
        key=lambda x: -x["users"],
    )[:20]

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "summary_kpis": summary_kpis,
        "claim_risk_breakdown": claim_risk_breakdown,
        "activity_buckets": activity_buckets,
        "top_reasons": top_reasons,
        "sample_users": bucket_samples,
    }


# ---------------------------------------------------------------------------
# 4g. VH Priority Impact Analysis (Phase 7C)
# ---------------------------------------------------------------------------

_VHPI_PROJ = _VHMA_PROJ  # account, backend_segment, metrics_snapshot, uim_comparison, etc.

# VH v2 thresholds mirrored here to avoid importing the engine module.
_VH_CLAIM_MIN   = 10
_VH_AFTER_BET_MAX = 100.0
_VH_REF_MAX     = 20


def _vhpi_simulated_segment(current_seg: str, ms: dict) -> str:
    """Re-classify one user with VH promoted above Low Value.

    Proposed priority order:
      1. high_value  (unchanged)
      2. voucher_hunter  ← NEW: checked before low_value
      3. low_value
      4. normal_actual / ghost / active_community_player / unclassified (unchanged)

    Only users that currently have marketing data can move (their current_seg
    is already one of the marketing-resolved segments). Users without
    marketing data are unchanged.
    """
    try:
        atb  = float(ms.get("after_total_bet_amount") or 0)
    except (TypeError, ValueError):
        atb = 0.0
    try:
        wd   = float(ms.get("withdraw_amount") or 0)
    except (TypeError, ValueError):
        wd = 0.0
    try:
        cl   = int(ms.get("claim_count") or 0)
    except (TypeError, ValueError):
        cl = 0
    try:
        refs = int(ms.get("referral_count") or 0)
    except (TypeError, ValueError):
        refs = 0

    marketing_available = ms.get("after_total_bet_amount") is not None and ms.get("withdraw_amount") is not None
    if not marketing_available:
        return current_seg  # no marketing data — result unchanged

    # 1. High Value (unchanged — highest priority)
    if wd > 0:
        ratio = atb / wd
        if ratio >= 8.0:
            return "high_value"

    # 2. Voucher Hunter v2 — now checked BEFORE low_value
    if cl >= _VH_CLAIM_MIN and atb < _VH_AFTER_BET_MAX and refs < _VH_REF_MAX:
        return "voucher_hunter"

    # 3. Low Value (withdraw > 0, ratio < 8x, didn't qualify for VH)
    if wd > 0:
        return "low_value"

    # 4-7: play / ghost / community / unclassified (unchanged)
    return current_seg


def build_vh_priority_impact(
    *,
    snapshots_col,
    snapshot_week: str,
    candidate_limit: int = 200,
    now: datetime | None = None,
) -> dict:
    """Phase 7C: read-only simulation of promoting VH above Low Value.

    Loads backend_segment_snapshots for the week, re-classifies each user
    with the proposed priority order (VH before Low Value), and returns
    summary metrics, migration breakdown, low-value impact, extreme-case
    count, candidate table, and decision metrics.

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    docs = list(snapshots_col.find({"snapshot_week": snapshot_week}, _VHPI_PROJ))
    total = len(docs)

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    # --- Simulation pass ---
    users_changed = 0
    current_lv_count = 0
    remaining_lv_count = 0
    migration: dict[tuple[str, str], int] = {}
    lv_to_vh = 0
    na_to_vh = 0
    other_to_vh = 0
    extreme_vh = 0
    candidates: list[dict] = []

    for doc in docs:
        current_seg = doc.get("backend_segment") or "unclassified"
        ms = doc.get("metrics_snapshot") or {}

        if current_seg == "low_value":
            current_lv_count += 1

        sim_seg = _vhpi_simulated_segment(current_seg, ms)

        if sim_seg == "low_value":
            remaining_lv_count += 1

        if sim_seg != current_seg:
            users_changed += 1
            key = (current_seg, sim_seg)
            migration[key] = migration.get(key, 0) + 1

            if sim_seg == "voucher_hunter":
                if current_seg == "low_value":
                    lv_to_vh += 1
                elif current_seg == "normal_actual":
                    na_to_vh += 1
                else:
                    other_to_vh += 1

            # Build candidate row
            try:
                atb  = float(ms.get("after_total_bet_amount") or 0)
            except (TypeError, ValueError):
                atb = 0.0
            try:
                wd   = float(ms.get("withdraw_amount") or 0)
            except (TypeError, ValueError):
                wd = 0.0
            try:
                cl   = int(ms.get("claim_count") or 0)
            except (TypeError, ValueError):
                cl = 0

            candidates.append({
                "account":          doc.get("account"),
                "current_segment":  current_seg,
                "simulated_segment": sim_seg,
                "claim_count":      ms.get("claim_count"),
                "after_bet":        ms.get("after_total_bet_amount"),
                "withdrawal":       ms.get("withdraw_amount"),
                "after_bet_multiple": round(atb / wd, 4) if wd else None,
                "after_bet_per_claim": round(atb / max(cl, 1), 4),
                "claim_risk_level": doc.get("claim_risk_level"),
                "player_age_type":  doc.get("player_age_type"),
            })

        # Extreme VH: claim_count >= 20 AND after_bet_per_claim < 2
        try:
            cl  = int(ms.get("claim_count") or 0)
            atb = float(ms.get("after_total_bet_amount") or 0)
        except (TypeError, ValueError):
            cl = 0; atb = 0.0
        if cl >= 20 and (atb / max(cl, 1)) < 2:
            extreme_vh += 1

    # Sort candidates by claim_count desc
    candidates.sort(key=lambda r: -(r["claim_count"] or 0))
    candidates = candidates[:candidate_limit]

    # Migration breakdown (sorted by users desc)
    migration_breakdown = sorted(
        [{"from_segment": k[0], "to_segment": k[1], "users": v}
         for k, v in migration.items()],
        key=lambda x: -x["users"],
    )

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "summary": {
            "users_scanned":                total,
            "users_changed":                users_changed,
            "low_value_to_voucher_hunter":  lv_to_vh,
            "normal_actual_to_voucher_hunter": na_to_vh,
            "other_to_voucher_hunter":      other_to_vh,
        },
        "migration_breakdown": migration_breakdown,
        "low_value_impact": {
            "current_low_value":       current_lv_count,
            "remaining_low_value":     remaining_lv_count,
            "moved_to_voucher_hunter": lv_to_vh,
            "pct_removed":             _pct(lv_to_vh, current_lv_count),
        },
        "extreme_vh": {"extreme_vh": extreme_vh},
        "decision_metrics": {
            "low_value_removed_pct":    _pct(lv_to_vh, current_lv_count),
            "voucher_hunter_growth_pct": _pct(lv_to_vh + na_to_vh + other_to_vh, total),
            "extreme_vh_count":         extreme_vh,
        },
        "candidates": candidates,
    }


# ---------------------------------------------------------------------------
# 4f. Voucher Hunter Rule Simulator (Phase 6A)
# ---------------------------------------------------------------------------

_VHRS_PROJ = _VHMA_PROJ  # same fields as mismatch audit projection


def build_voucher_hunter_rule_simulator(
    *,
    snapshots_col,
    snapshot_week: str,
    claim_threshold: int = 10,
    after_bet_threshold: float = 100.0,
    referral_threshold: int = 20,
    withdrawal_protection: bool = True,
    high_bet_protection: bool = True,
    top_n: int = 50,
    now: datetime | None = None,
) -> dict:
    """Phase 6A: read-only simulation of a refined voucher_hunter rule.

    Loads all backend_segment_snapshots for the given week, applies the
    simulated rule in memory, and returns:
    - simulation summary
    - match rate impact vs UIM
    - segment migration table
    - false positive review (protected users that UIM called VH)
    - false negative review (newly captured users not currently VH)

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    docs = list(snapshots_col.find({"snapshot_week": snapshot_week}, _VHRS_PROJ))
    total = len(docs)

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    def _sf(v):
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _si(v):
        try:
            return int(v) if v is not None else 0
        except (TypeError, ValueError):
            return 0

    # --- Simulation pass ---
    current_vh_count = 0
    simulated_vh_count = 0
    compared = 0
    current_matched = 0
    simulated_matched = 0

    migration: dict[tuple[str, str], int] = {}
    fp_candidates: list[dict] = []   # sim=False, uim=voucher_hunter
    fn_candidates: list[dict] = []   # sim=True,  backend != voucher_hunter

    for doc in docs:
        ms = doc.get("metrics_snapshot") or {}
        cmp = doc.get("uim_comparison") or {}

        current_seg = doc.get("backend_segment") or "unclassified"
        uim_seg = cmp.get("uim_segment") or ""

        atb    = _sf(ms.get("after_total_bet_amount"))
        wd     = _sf(ms.get("withdraw_amount"))
        claims = _si(ms.get("claim_count"))
        refs   = _si(ms.get("referral_count"))

        # Current backend VH count
        if current_seg == "voucher_hunter":
            current_vh_count += 1

        # Simulate
        sim_vh = (claims >= claim_threshold) and (atb < after_bet_threshold)
        if withdrawal_protection and wd > 0:
            sim_vh = False
        if high_bet_protection and atb >= 1000:
            sim_vh = False
        if refs >= referral_threshold:
            sim_vh = False

        sim_seg = "voucher_hunter" if sim_vh else current_seg
        if sim_seg == "voucher_hunter":
            simulated_vh_count += 1

        # Migration
        key = (current_seg, sim_seg)
        migration[key] = migration.get(key, 0) + 1

        # Match rate vs UIM
        if uim_seg:
            compared += 1
            if current_seg == uim_seg:
                current_matched += 1
            if sim_seg == uim_seg:
                simulated_matched += 1

        # False positive candidates: sim=False AND uim=voucher_hunter
        if not sim_vh and uim_seg == "voucher_hunter":
            fp_candidates.append({
                "account":         doc.get("account"),
                "backend_segment": current_seg,
                "uim_segment":     uim_seg,
                "after_bet":       ms.get("after_total_bet_amount"),
                "withdrawal":      ms.get("withdraw_amount"),
                "claims":          claims,
                "referrals":       refs,
                "checkins":        _si(ms.get("checkin_count")),
                "player_age_type": doc.get("player_age_type"),
                "claim_risk_level": doc.get("claim_risk_level"),
                "confidence":      doc.get("confidence"),
            })

        # False negative candidates: sim=True AND current backend != voucher_hunter
        if sim_vh and current_seg != "voucher_hunter":
            fn_candidates.append({
                "account":         doc.get("account"),
                "backend_segment": current_seg,
                "uim_segment":     uim_seg,
                "after_bet":       ms.get("after_total_bet_amount"),
                "withdrawal":      ms.get("withdraw_amount"),
                "claims":          claims,
                "referrals":       refs,
                "checkins":        _si(ms.get("checkin_count")),
                "player_age_type": doc.get("player_age_type"),
                "claim_risk_level": doc.get("claim_risk_level"),
                "confidence":      doc.get("confidence"),
            })

    # UIM VH count from docs
    uim_vh_count = sum(
        1 for doc in docs
        if (doc.get("uim_comparison") or {}).get("uim_segment") == "voucher_hunter"
    )

    # Section 1 — Simulation Summary
    simulation_summary = {
        "total_users":                      total,
        "simulated_voucher_hunter_count":   simulated_vh_count,
        "simulated_voucher_hunter_pct":     _pct(simulated_vh_count, total),
        "current_backend_voucher_hunter_count": current_vh_count,
        "current_uim_voucher_hunter_count": uim_vh_count,
    }

    # Section 2 — Match Rate Simulation
    cur_rate = _pct(current_matched, compared)
    sim_rate = _pct(simulated_matched, compared)
    match_rate_simulation = {
        "compared_users":      compared,
        "current_matches":     current_matched,
        "current_mismatches":  compared - current_matched,
        "previous_match_rate": cur_rate,
        "simulated_matches":   simulated_matched,
        "simulated_mismatches": compared - simulated_matched,
        "match_rate":          sim_rate,
        "delta":               round((sim_rate or 0) - (cur_rate or 0), 2) if (cur_rate is not None and sim_rate is not None) else None,
    }

    # Section 3 — Segment Migration
    segment_migration = sorted(
        [{"from_segment": k[0], "to_segment": k[1], "users": v}
         for k, v in migration.items() if k[0] != k[1]],
        key=lambda x: -x["users"],
    )

    # Section 4 — False Positive Review (top_n by after_bet → refs → wd)
    fp_review = sorted(
        fp_candidates,
        key=lambda r: (-(_sf(r["after_bet"])), -r["referrals"], -(_sf(r["withdrawal"]))),
    )[:top_n]

    # Section 5 — False Negative Review (top_n by claims desc)
    fn_review = sorted(fn_candidates, key=lambda r: -r["claims"])[:top_n]

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "params_used": {
            "claim_threshold":      claim_threshold,
            "after_bet_threshold":  after_bet_threshold,
            "referral_threshold":   referral_threshold,
            "withdrawal_protection": withdrawal_protection,
            "high_bet_protection":  high_bet_protection,
        },
        "simulation_summary":    simulation_summary,
        "match_rate_simulation": match_rate_simulation,
        "segment_migration":     segment_migration,
        "false_positive_review": fp_review,
        "false_negative_review": fn_review,
    }


# ---------------------------------------------------------------------------
# 4e. Voucher Hunter False Positive Analysis (Phase 5E-FP)
# ---------------------------------------------------------------------------

_VHFP_PROJ = _VHMA_PROJ  # same fields as voucher hunter mismatch audit projection


def build_voucher_hunter_false_positive_analysis(
    *,
    snapshots_col,
    snapshot_week: str,
    top_n: int = 50,
    now: datetime | None = None,
) -> dict:
    """Phase 5E-FP: read-only false-positive analysis of uim_segment=voucher_hunter.

    Provides after_bet / withdrawal / referral / checkin distribution tables,
    a false-positive candidate list (top_n sorted by after_bet → referrals →
    checkins), and a backend-segment evidence matrix.

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    docs = list(snapshots_col.find(
        {"snapshot_week": snapshot_week, "uim_comparison.uim_segment": "voucher_hunter"},
        _VHFP_PROJ,
    ))

    total = len(docs)

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    def _safe_float(v):
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _safe_int(v):
        try:
            return int(v) if v is not None else 0
        except (TypeError, ValueError):
            return 0

    # Pre-extract metrics once
    rows = []
    for doc in docs:
        ms = doc.get("metrics_snapshot") or {}
        rows.append({
            "doc":      doc,
            "atb":      _safe_float(ms.get("after_total_bet_amount")),
            "wd":       _safe_float(ms.get("withdraw_amount")),
            "claims":   _safe_int(ms.get("claim_count")),
            "refs":     _safe_int(ms.get("referral_count")),
            "checkins": _safe_int(ms.get("checkin_count")),
        })

    # ---- Analysis 1: After Bet Distribution ----
    _atb_buckets = [
        ("after_bet = 0",           lambda v: v == 0),
        ("0 < after_bet < 100",     lambda v: 0 < v < 100),
        ("100 <= after_bet < 1000", lambda v: 100 <= v < 1000),
        ("1000 <= after_bet < 5000",lambda v: 1000 <= v < 5000),
        ("after_bet >= 5000",       lambda v: v >= 5000),
    ]
    after_bet_distribution = []
    for label, predicate in _atb_buckets:
        bucket_rows = [r for r in rows if predicate(r["atb"])]
        n = len(bucket_rows)
        after_bet_distribution.append({
            "bucket":        label,
            "users":         n,
            "percentage":    _pct(n, total),
            "avg_claims":    round(sum(r["claims"]   for r in bucket_rows) / n, 2) if n else None,
            "avg_referrals": round(sum(r["refs"]     for r in bucket_rows) / n, 2) if n else None,
            "avg_checkins":  round(sum(r["checkins"] for r in bucket_rows) / n, 2) if n else None,
        })

    # ---- Analysis 2: Withdrawal Distribution ----
    _wd_buckets = [
        ("withdraw = 0",          lambda v: v == 0),
        ("0 < withdraw < 10",     lambda v: 0 < v < 10),
        ("10 <= withdraw < 100",  lambda v: 10 <= v < 100),
        ("withdraw >= 100",       lambda v: v >= 100),
    ]
    withdrawal_distribution = []
    for label, predicate in _wd_buckets:
        bucket_rows = [r for r in rows if predicate(r["wd"])]
        n = len(bucket_rows)
        withdrawal_distribution.append({
            "bucket":       label,
            "users":        n,
            "percentage":   _pct(n, total),
            "avg_after_bet": round(sum(r["atb"] for r in bucket_rows) / n, 2) if n else None,
        })

    # ---- Analysis 3: Referral Distribution ----
    _ref_buckets = [
        ("0 referrals",    lambda v: v == 0),
        ("1-5 referrals",  lambda v: 1 <= v <= 5),
        ("6-20 referrals", lambda v: 6 <= v <= 20),
        ("21-50 referrals",lambda v: 21 <= v <= 50),
        ("> 50 referrals", lambda v: v > 50),
    ]
    referral_distribution = []
    for label, predicate in _ref_buckets:
        bucket_rows = [r for r in rows if predicate(r["refs"])]
        n = len(bucket_rows)
        referral_distribution.append({
            "bucket":       label,
            "users":        n,
            "percentage":   _pct(n, total),
            "avg_after_bet": round(sum(r["atb"]    for r in bucket_rows) / n, 2) if n else None,
            "avg_claims":    round(sum(r["claims"] for r in bucket_rows) / n, 2) if n else None,
        })

    # ---- Analysis 4: Check-in Distribution ----
    _chk_buckets = [
        ("0 checkins",    lambda v: v == 0),
        ("1-7 checkins",  lambda v: 1 <= v <= 7),
        ("8-30 checkins", lambda v: 8 <= v <= 30),
        ("> 30 checkins", lambda v: v > 30),
    ]
    checkin_distribution = []
    for label, predicate in _chk_buckets:
        bucket_rows = [r for r in rows if predicate(r["checkins"])]
        n = len(bucket_rows)
        checkin_distribution.append({
            "bucket":      label,
            "users":       n,
            "percentage":  _pct(n, total),
            "avg_claims":  round(sum(r["claims"] for r in bucket_rows) / n, 2) if n else None,
        })

    # ---- Analysis 5: False Positive Candidates ----
    sorted_fp = sorted(rows, key=lambda r: (-r["atb"], -r["refs"], -r["checkins"]))[:top_n]
    false_positive_candidates = []
    for r in sorted_fp:
        doc = r["doc"]
        ms = doc.get("metrics_snapshot") or {}
        cmp = doc.get("uim_comparison") or {}
        false_positive_candidates.append({
            "account":        doc.get("account"),
            "backend_segment": doc.get("backend_segment") or "unclassified",
            "uim_segment":    cmp.get("uim_segment"),
            "after_bet":      ms.get("after_total_bet_amount"),
            "withdrawal":     ms.get("withdraw_amount"),
            "claims":         ms.get("claim_count"),
            "referrals":      ms.get("referral_count"),
            "checkins":       ms.get("checkin_count"),
            "player_age_type": doc.get("player_age_type"),
            "claim_risk_level": doc.get("claim_risk_level"),
            "confidence":     doc.get("confidence"),
        })

    # ---- Analysis 6: Segment Evidence Matrix ----
    _ev_seg_order = [
        "voucher_hunter", "normal_actual", "low_value", "high_value",
        "active_community_player", "ghost", "unclassified",
    ]

    ev_acc: dict[str, dict] = {}
    for r in rows:
        seg = r["doc"].get("backend_segment") or "unclassified"
        if seg not in ev_acc:
            ev_acc[seg] = {"n": 0, "atb": 0.0, "wd": 0.0, "cl": 0, "rf": 0, "chk": 0}
        a = ev_acc[seg]
        a["n"] += 1; a["atb"] += r["atb"]; a["wd"] += r["wd"]
        a["cl"] += r["claims"]; a["rf"] += r["refs"]; a["chk"] += r["checkins"]

    def _seg_ev_order(seg):
        try:
            return _ev_seg_order.index(seg)
        except ValueError:
            return 99

    evidence_matrix = []
    for seg in sorted(ev_acc.keys(), key=_seg_ev_order):
        a = ev_acc[seg]
        n = a["n"] or 1
        evidence_matrix.append({
            "backend_segment": seg,
            "count":           a["n"],
            "percentage":      _pct(a["n"], total),
            "avg_after_bet":   round(a["atb"] / n, 2),
            "avg_withdrawal":  round(a["wd"]  / n, 2),
            "avg_claims":      round(a["cl"]  / n, 2),
            "avg_referrals":   round(a["rf"]  / n, 2),
            "avg_checkins":    round(a["chk"] / n, 2),
        })

    # ---- Summary KPIs ----
    users_with_bet    = sum(1 for r in rows if r["atb"] > 0)
    users_with_wd     = sum(1 for r in rows if r["wd"]  > 0)
    users_with_refs   = sum(1 for r in rows if r["refs"] > 0)
    users_bet_gte1k   = sum(1 for r in rows if r["atb"] >= 1000)

    summary_kpis = {
        "total_uim_voucher_hunter": total,
        "users_with_any_bet":        users_with_bet,
        "users_with_any_bet_pct":    _pct(users_with_bet, total),
        "users_with_any_withdrawal": users_with_wd,
        "users_with_any_withdrawal_pct": _pct(users_with_wd, total),
        "users_with_any_referral":   users_with_refs,
        "users_with_any_referral_pct": _pct(users_with_refs, total),
        "users_bet_gte_1000":        users_bet_gte1k,
        "users_bet_gte_1000_pct":    _pct(users_bet_gte1k, total),
    }

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "summary_kpis": summary_kpis,
        "after_bet_distribution":   after_bet_distribution,
        "withdrawal_distribution":  withdrawal_distribution,
        "referral_distribution":    referral_distribution,
        "checkin_distribution":     checkin_distribution,
        "false_positive_candidates": false_positive_candidates,
        "evidence_matrix":          evidence_matrix,
    }


# ---------------------------------------------------------------------------
# 4d. Voucher Hunter Rule Quality Analysis (Phase 5E)
# ---------------------------------------------------------------------------

_VHQA_PROJ = {
    "_id": 0,
    "account": 1,
    "backend_segment": 1,
    "player_age_type": 1,
    "claim_risk_level": 1,
    "confidence": 1,
    "segment_reason": 1,
    "uim_comparison": 1,
    "metrics_snapshot": 1,
}


def build_voucher_hunter_quality_analysis(
    *,
    snapshots_col,
    snapshot_week: str,
    top_n: int = 20,
    now: datetime | None = None,
) -> dict:
    """Phase 5E: read-only analysis of users where uim_segment=voucher_hunter.

    Groups by backend_segment and computes avg metrics to determine whether
    the UIM voucher_hunter classification is over-classifying real players.
    Also returns top-N users by claim_count, after_bet, and referral_count,
    plus claim threshold breakdown (>=10, >=20, >=50).

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    docs = list(snapshots_col.find(
        {"snapshot_week": snapshot_week, "uim_comparison.uim_segment": "voucher_hunter"},
        _VHQA_PROJ,
    ))

    total = len(docs)

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    # --- Per-group accumulators ---
    acc: dict[str, dict] = {}

    for doc in docs:
        seg = doc.get("backend_segment") or "unclassified"
        ms = doc.get("metrics_snapshot") or {}

        if seg not in acc:
            acc[seg] = {
                "user_count": 0,
                "sum_atb": 0.0, "n_atb": 0,
                "sum_wd": 0.0, "n_wd": 0,
                "sum_claim": 0, "sum_ref": 0, "sum_checkin": 0,
                "new_player": 0, "old_player": 0,
                "claim_risk_counts": {},
            }
        a = acc[seg]
        a["user_count"] += 1

        try:
            atb = ms.get("after_total_bet_amount")
            if atb is not None:
                a["sum_atb"] += float(atb); a["n_atb"] += 1
        except (TypeError, ValueError):
            pass
        try:
            wd = ms.get("withdraw_amount")
            if wd is not None:
                a["sum_wd"] += float(wd); a["n_wd"] += 1
        except (TypeError, ValueError):
            pass
        try:
            a["sum_claim"]   += int(ms.get("claim_count",   0) or 0)
            a["sum_ref"]     += int(ms.get("referral_count", 0) or 0)
            a["sum_checkin"] += int(ms.get("checkin_count",  0) or 0)
        except (TypeError, ValueError):
            pass

        age = doc.get("player_age_type") or ""
        if age == "new_player":
            a["new_player"] += 1
        elif age == "old_player":
            a["old_player"] += 1

        risk = doc.get("claim_risk_level") or "normal"
        a["claim_risk_counts"][risk] = a["claim_risk_counts"].get(risk, 0) + 1

    # Build group breakdown sorted by user_count desc
    _seg_order = [
        "normal_actual", "low_value", "high_value", "voucher_hunter",
        "active_community_player", "ghost", "unclassified",
    ]

    def _seg_sort_key(seg):
        try:
            return _seg_order.index(seg)
        except ValueError:
            return 99

    group_breakdown = []
    for seg in sorted(acc.keys(), key=_seg_sort_key):
        a = acc[seg]
        n = a["user_count"] or 1
        # Dominant claim risk
        dominant_risk = max(a["claim_risk_counts"], key=a["claim_risk_counts"].get) if a["claim_risk_counts"] else None
        group_breakdown.append({
            "backend_segment":   seg,
            "user_count":        a["user_count"],
            "pct_of_total":      _pct(a["user_count"], total),
            "avg_after_bet":     round(a["sum_atb"] / a["n_atb"], 2) if a["n_atb"] else None,
            "avg_withdraw":      round(a["sum_wd"]  / a["n_wd"],  2) if a["n_wd"]  else None,
            "avg_claims":        round(a["sum_claim"]   / n, 2),
            "avg_referrals":     round(a["sum_ref"]     / n, 2),
            "avg_checkins":      round(a["sum_checkin"] / n, 2),
            "dominant_claim_risk": dominant_risk,
            "new_players":       a["new_player"],
            "old_players":       a["old_player"],
        })

    # --- Top-N lists ---
    def _ms_float(doc, key):
        try:
            return float(doc.get("metrics_snapshot", {}).get(key) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _ms_int(doc, key):
        try:
            return int(doc.get("metrics_snapshot", {}).get(key) or 0)
        except (TypeError, ValueError):
            return 0

    def _sample_row(doc):
        ms = doc.get("metrics_snapshot") or {}
        return {
            "account":    doc.get("account"),
            "backend_segment": doc.get("backend_segment") or "unclassified",
            "after_bet":  ms.get("after_total_bet_amount"),
            "withdraw":   ms.get("withdraw_amount"),
            "claims":     ms.get("claim_count"),
            "referrals":  ms.get("referral_count"),
            "checkins":   ms.get("checkin_count"),
            "age_type":   doc.get("player_age_type"),
            "claim_risk": doc.get("claim_risk_level"),
            "confidence": doc.get("confidence"),
            "reason":     doc.get("segment_reason"),
        }

    top_by_claims = [
        _sample_row(d)
        for d in sorted(docs, key=lambda x: _ms_int(x, "claim_count"), reverse=True)[:top_n]
    ]
    top_by_after_bet = [
        _sample_row(d)
        for d in sorted(docs, key=lambda x: _ms_float(x, "after_total_bet_amount"), reverse=True)[:top_n]
    ]
    top_by_referrals = [
        _sample_row(d)
        for d in sorted(docs, key=lambda x: _ms_int(x, "referral_count"), reverse=True)[:top_n]
    ]

    # --- Claim threshold breakdown ---
    thresholds = [10, 20, 50]
    claim_threshold_breakdown = []
    for t in thresholds:
        cnt = sum(1 for d in docs if _ms_int(d, "claim_count") >= t)
        claim_threshold_breakdown.append({
            "threshold":  f">= {t} claims",
            "count":      cnt,
            "percentage": _pct(cnt, total),
        })

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "total_uim_voucher_hunter": total,
        "group_breakdown": group_breakdown,
        "top_by_claims":     top_by_claims,
        "top_by_after_bet":  top_by_after_bet,
        "top_by_referrals":  top_by_referrals,
        "claim_threshold_breakdown": claim_threshold_breakdown,
    }


# ---------------------------------------------------------------------------
# 4c. Segment Rule Simulator (Phase 5D)
# ---------------------------------------------------------------------------

_SRS_PROJ = {
    "_id": 0,
    "account": 1,
    "backend_segment": 1,
    "player_age_type": 1,
    "metrics_snapshot": 1,
    "uim_comparison": 1,
}

_SRS_ALL_SEGMENTS = [
    "high_value", "low_value", "normal_actual",
    "voucher_hunter", "ghost", "active_community_player", "unclassified",
]

_SRS_HIGH_VALUE_RATIO = 8.0


def _srs_simulate_segment(ms: dict, params: dict) -> str:
    """Re-classify a single user using simulated thresholds.

    Mirrors the production priority order:
      high_value > low_value > normal_actual > voucher_hunter > ghost
      > active_community_player > unclassified

    Only ghost, voucher_hunter, and active_community_player thresholds are
    configurable via params. High/low/normal_actual rules are unchanged.
    """
    try:
        atb = float(ms.get("after_total_bet_amount") or 0)
    except (TypeError, ValueError):
        atb = 0.0
    try:
        wd = float(ms.get("withdraw_amount") or 0)
    except (TypeError, ValueError):
        wd = 0.0
    try:
        claims = int(ms.get("claim_count") or 0)
    except (TypeError, ValueError):
        claims = 0
    try:
        refs = int(ms.get("referral_count") or 0)
    except (TypeError, ValueError):
        refs = 0
    try:
        checkins = int(ms.get("checkin_count") or 0)
    except (TypeError, ValueError):
        checkins = 0

    marketing_available = ms.get("after_total_bet_amount") is not None and ms.get("withdraw_amount") is not None

    if marketing_available:
        if wd > 0:
            ratio = atb / wd if wd else 0.0
            return "high_value" if ratio >= _SRS_HIGH_VALUE_RATIO else "low_value"
        if atb > 0:
            return "normal_actual"
        # after_bet == 0, withdraw == 0
        vh_min_claims = params["vh_min_claims"]
        vh_max_after_bet = params["vh_max_after_bet"]
        vh_max_checkins = params["vh_max_checkins"]
        if claims >= vh_min_claims and atb <= vh_max_after_bet and checkins <= vh_max_checkins:
            return "voucher_hunter"
        g_max_checkins = params["ghost_max_checkins"]
        g_max_refs = params["ghost_max_referrals"]
        g_max_claims = params["ghost_max_claims"]
        if refs <= g_max_refs and checkins <= g_max_checkins and claims <= g_max_claims:
            return "ghost"

    # Attribute-based (no marketing data or fell through ghost/vh)
    ac_min_checkins = params["ac_min_checkins"]
    ac_min_refs = params["ac_min_referrals"]
    if checkins >= ac_min_checkins or refs >= ac_min_refs:
        return "active_community_player"

    return "unclassified"


def build_segment_rule_simulator(
    *,
    snapshots_col,
    snapshot_week: str,
    # Ghost thresholds
    ghost_max_checkins: int = 0,
    ghost_max_referrals: int = 0,
    ghost_max_claims: int = 0,
    # Voucher hunter thresholds
    vh_min_claims: int = 3,
    vh_max_after_bet: float = 0.0,
    vh_max_checkins: int = 9999,
    # Active community thresholds
    ac_min_checkins: int = 14,
    ac_min_referrals: int = 1,
    now: datetime | None = None,
) -> dict:
    """Phase 5D: read-only simulation of segment rule changes.

    Loads backend_segment_snapshots for the given week, re-classifies every
    user using the provided thresholds (in memory only), and returns:
    - segment distribution comparison (current vs simulated)
    - match rate impact against UIM segments
    - top segment movements
    - production impact summary

    Never writes, never modifies segments or rewards.
    """
    now = now or _utc_now()

    docs = list(snapshots_col.find({"snapshot_week": snapshot_week}, _SRS_PROJ))
    total = len(docs)

    params = {
        "ghost_max_checkins": ghost_max_checkins,
        "ghost_max_referrals": ghost_max_referrals,
        "ghost_max_claims": ghost_max_claims,
        "vh_min_claims": vh_min_claims,
        "vh_max_after_bet": vh_max_after_bet,
        "vh_max_checkins": vh_max_checkins,
        "ac_min_checkins": ac_min_checkins,
        "ac_min_referrals": ac_min_referrals,
    }

    # Accumulators
    current_dist: dict[str, int] = {s: 0 for s in _SRS_ALL_SEGMENTS}
    simulated_dist: dict[str, int] = {s: 0 for s in _SRS_ALL_SEGMENTS}
    movements: dict[tuple[str, str], int] = {}

    current_matched = 0
    simulated_matched = 0
    compared = 0

    for doc in docs:
        current_seg = doc.get("backend_segment") or "unclassified"
        ms = doc.get("metrics_snapshot") or {}
        sim_seg = _srs_simulate_segment(ms, params)

        current_dist[current_seg] = current_dist.get(current_seg, 0) + 1
        simulated_dist[sim_seg] = simulated_dist.get(sim_seg, 0) + 1

        if current_seg != sim_seg:
            key = (current_seg, sim_seg)
            movements[key] = movements.get(key, 0) + 1

        cmp = doc.get("uim_comparison") or {}
        uim_seg = cmp.get("uim_segment")
        if uim_seg:
            compared += 1
            if current_seg == uim_seg:
                current_matched += 1
            if sim_seg == uim_seg:
                simulated_matched += 1

    def _pct(num: int, den: int) -> float | None:
        return round(num / den * 100, 2) if den else None

    # Output 1: segment distribution comparison
    distribution = []
    for seg in _SRS_ALL_SEGMENTS:
        cur = current_dist.get(seg, 0)
        sim = simulated_dist.get(seg, 0)
        diff = sim - cur
        distribution.append({
            "segment": seg,
            "current_users": cur,
            "simulated_users": sim,
            "difference": diff,
            "difference_str": ("+" if diff > 0 else "") + str(diff),
        })

    # Output 2: match rate impact
    cur_match_rate = _pct(current_matched, compared)
    sim_match_rate = _pct(simulated_matched, compared)
    match_rate_impact = {
        "compared_users": compared,
        "current_matched": current_matched,
        "current_mismatched": compared - current_matched,
        "current_match_rate": cur_match_rate,
        "current_mismatch_rate": _pct(compared - current_matched, compared),
        "simulated_matched": simulated_matched,
        "simulated_mismatched": compared - simulated_matched,
        "simulated_match_rate": sim_match_rate,
        "simulated_mismatch_rate": _pct(compared - simulated_matched, compared),
        "match_rate_delta": round((sim_match_rate or 0) - (cur_match_rate or 0), 2) if (cur_match_rate is not None and sim_match_rate is not None) else None,
    }

    # Output 3: top segment movements
    top_movements = sorted(
        [
            {"from_segment": k[0], "to_segment": k[1], "users": v}
            for k, v in movements.items()
        ],
        key=lambda x: -x["users"],
    )[:20]

    # Output 4: production impact summary
    production_impact = {
        "current_unclassified":    current_dist.get("unclassified", 0),
        "simulated_unclassified":  simulated_dist.get("unclassified", 0),
        "current_ghost":           current_dist.get("ghost", 0),
        "simulated_ghost":         simulated_dist.get("ghost", 0),
        "current_voucher_hunter":  current_dist.get("voucher_hunter", 0),
        "simulated_voucher_hunter": simulated_dist.get("voucher_hunter", 0),
        "current_match_rate":      cur_match_rate,
        "simulated_match_rate":    sim_match_rate,
    }

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "snapshot_week": snapshot_week,
        "total_users": total,
        "params_used": params,
        "segment_distribution": distribution,
        "match_rate_impact": match_rate_impact,
        "top_movements": top_movements,
        "production_impact": production_impact,
    }


# ---------------------------------------------------------------------------
# 5. User drilldown
# ---------------------------------------------------------------------------

def build_user_drilldown(
    *,
    query: str,
    users_col,
    welcome_eligibility_col,
    voucher_claims_col,
    affiliate_ledger_col,
    pending_referrals_col,
    qualified_events_col,
    now: datetime | None = None,
) -> dict:
    now = now or _utc_now()
    q = (query or "").strip()
    if not q:
        return {"success": False, "message": "Empty search query."}

    # Resolve the user by numeric id or (case-insensitive) username.
    user = None
    try:
        if q.isdigit():
            user = users_col.find_one({"user_id": int(q)})
        if user is None:
            uname = q[1:] if q.startswith("@") else q
            user = users_col.find_one(
                {"username": {"$regex": f"^{_escape_regex(uname)}$", "$options": "i"}}
            )
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "message": f"lookup failed: {exc}"}

    if not user:
        return {"success": False, "data_quality": "missing", "message": "No user found for that id/username."}

    from effective_segment import resolve_effective_segment  # local import avoids a hard dep at module load

    uid = user.get("user_id")
    errors: list[str] = []

    def grab(fn, default=None):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
            return default

    welcome = grab(
        lambda: welcome_eligibility_col.find_one({"$or": [{"uid": uid}, {"user_id": uid}]})
    ) or {}

    voucher_history = []
    for d in grab(
        lambda: list(voucher_claims_col.find({"user_id": uid}).sort("created_at", -1).limit(50)),
        default=[],
    ) or []:
        voucher_history.append(
            {
                "drop_id": str(d.get("drop_id")),
                "status": d.get("status"),
                "voucher_code": d.get("voucher_code"),
                "created_at": _iso(d.get("created_at")),
                "claimed_at": _iso(d.get("claimed_at")),
                "error": d.get("error"),
            }
        )

    affiliate_history = []
    for d in grab(
        lambda: list(affiliate_ledger_col.find({"user_id": uid}).sort("updated_at", -1).limit(50)),
        default=[],
    ) or []:
        affiliate_history.append(
            {
                "ledger_type": d.get("ledger_type"),
                "tier": d.get("tier"),
                "status": d.get("status"),
                "year_month": d.get("year_month"),
                "qualified_count": d.get("qualified_count"),
                "risk_flags": d.get("risk_flags") or [],
                "updated_at": _iso(d.get("updated_at")),
            }
        )

    referrals_made = grab(
        lambda: int(pending_referrals_col.count_documents({"inviter_user_id": uid})), default=None
    )
    referrals_qualified = grab(
        lambda: int(
            pending_referrals_col.count_documents(
                {"inviter_user_id": uid, "status": {"$in": _QUALIFIED_STATUSES}}
            )
        ),
        default=None,
    )
    was_referred = grab(
        lambda: qualified_events_col.find_one({"invitee_id": uid}) is not None, default=None
    )

    # Risk flags: union of bot-segment label and any affiliate ledger risk flags.
    risk_flags = []
    seg = (user.get("for_bot_segment") or user.get("bot_segment") or "").strip().lower()
    if seg in ("voucher_hunter", "welcome_abuse", "multi_account", "low_value"):
        risk_flags.append(f"segment:{seg}")
    if user.get("multi_account_voucher_hunter") is True:
        risk_flags.append("multi_account_voucher_hunter")
    for h in affiliate_history:
        for f in h.get("risk_flags") or []:
            tag = f"affiliate:{f}"
            if tag not in risk_flags:
                risk_flags.append(tag)

    # "segment" stays the canonical BEHAVIORAL label (for_bot_segment/bot_segment,
    # unchanged by multi-account risk -- analytics/reporting must keep reading
    # this). behavioral_segment/effective_segment/voucher_hunter_reasons are
    # additive fields exposing the same distinction explicitly: effective_segment
    # is what campaign/voucher-hunter OPERATIONAL eligibility actually resolves
    # this user as (see effective_segment.py), which can differ from the
    # canonical segment when multi_account_voucher_hunter=True.
    behavioral_segment = user.get("for_bot_segment") or user.get("bot_segment")

    return {
        "success": True,
        "as_of": now.isoformat(),
        "profile": {
            "user_id": uid,
            "username": user.get("username"),
            "first_name": user.get("first_name"),
            "status": user.get("status"),
            "vip_tier": user.get("vip_tier"),
            "created_at": _iso(user.get("created_at")),
            "joined_main_at": _iso(user.get("joined_main_at")),
            "last_checkin": _iso(user.get("last_checkin")),
        },
        "segment": behavioral_segment,
        "behavioral_segment": behavioral_segment,
        "effective_segment": resolve_effective_segment(user),
        "voucher_hunter_reasons": user.get("voucher_hunter_reasons") or [],
        "xp": {
            "total_xp": user.get("total_xp"),
            "weekly_xp": user.get("weekly_xp"),
            "monthly_xp": user.get("monthly_xp"),
        },
        "checkin": {
            "streak": user.get("streak"),
            "streak_freeze_tokens": user.get("streak_freeze_tokens"),
            "first_checkin_at": _iso(user.get("first_checkin_at")),
            "last_checkin": _iso(user.get("last_checkin")),
        },
        "referral_stats": {
            "total_referrals_snapshot": user.get("total_referrals"),
            "referrals_made": referrals_made,
            "referrals_qualified": referrals_qualified,
            "was_referred": was_referred,
        },
        "welcome_status": {
            "eligible": bool(welcome),
            "claimed": bool(welcome.get("claimed")),
            "claimed_at": _iso(welcome.get("claimed_at")),
            "lifecycle_state": welcome.get("lifecycle_state"),
            "eligible_until": _iso(welcome.get("eligible_until")),
        },
        "voucher_history": voucher_history,
        "affiliate_history": affiliate_history,
        "risk_flags": risk_flags,
        "partial_errors": errors or None,
    }


def _escape_regex(s: str) -> str:
    import re

    return re.escape(s)


# ---------------------------------------------------------------------------
# 6. Settings panel (read-only, secrets masked)
# ---------------------------------------------------------------------------

# Names of env vars whose *presence* may be reported but whose value must never
# be exposed. Anything containing these tokens is masked defensively too.
_SECRET_ENV_NAMES = {
    "BOT_TOKEN",
    "FLASK_SECRET_KEY",
    "MONGO_URL",
    "ADMIN_PANEL_SECRET",
    "ADMIN_USER_IDS",
    "ADMIN_SECRET",
    "PUBLIC_POOL_FINGERPRINT_SALT",
    "SHEETS_WEBHOOK_URL",
}
_SECRET_TOKENS = ("token", "secret", "password", "salt", "mongo_url", "webhook")


def _is_secret_name(name: str) -> bool:
    if name in _SECRET_ENV_NAMES:
        return True
    low = name.lower()
    return any(tok in low for tok in _SECRET_TOKENS)


def _env(env: Mapping, name: str, default: Any = None) -> Any:
    """Read a non-secret env var; secret names are never returned as values."""
    if _is_secret_name(name):
        return {"configured": bool(env.get(name)), "masked": True}
    val = env.get(name)
    return val if val not in (None, "") else default


def build_segment_roi_panel(
    *,
    snapshots_col,
    snapshot_month: str | None = None,
    snapshot_week: str | None = None,
    now: datetime | None = None,
    trend_months: int = 3,
) -> dict:
    """Segment ROI Dashboard — read-only aggregation of backend_segment_snapshots.

    Groups users by their backend_segment and sums the metrics_snapshot fields
    stored by the shadow segment engine.  Never writes, never classifies users,
    never touches voucher/reward logic.

    ROI score = after_bet_amount / max(claim_count, 1).
    A higher score means each voucher claim generated more betting activity.

    Period resolution (in priority order):
      1. snapshot_week if supplied (e.g. "2026-W24")
      2. snapshot_month if supplied (e.g. "2026-06")
      3. Current calendar month (default)
    """
    now = now or _utc_now()

    if snapshot_week:
        match_stage: dict = {"snapshot_week": snapshot_week}
        period_label = snapshot_week
        period_type = "weekly"
    elif snapshot_month:
        match_stage = {"snapshot_month": snapshot_month}
        period_label = snapshot_month
        period_type = "monthly"
    else:
        resolved_month = f"{now.year:04d}-{now.month:02d}"
        match_stage = {"snapshot_month": resolved_month}
        period_label = resolved_month
        period_type = "monthly"

    # Deduplicate within the period: for each (account, user_id) pair keep the
    # latest week's metrics, then aggregate by segment.
    pipeline: list[dict] = [
        {"$match": match_stage},
        {"$sort": {"snapshot_week": -1}},
        # One row per account — the latest snapshot in the period.
        {"$group": {
            "_id": "$account",
            "segment": {"$first": "$backend_segment"},
            "after_bet": {"$first": {"$ifNull": ["$metrics_snapshot.after_total_bet_amount", 0]}},
            "withdraw": {"$first": {"$ifNull": ["$metrics_snapshot.withdraw_amount", 0]}},
            "claims": {"$first": {"$ifNull": ["$metrics_snapshot.claim_count", 0]}},
            "referrals": {"$first": {"$ifNull": ["$metrics_snapshot.referral_count", 0]}},
            "checkins": {"$first": {"$ifNull": ["$metrics_snapshot.checkin_count", 0]}},
        }},
        {"$group": {
            "_id": "$segment",
            "users": {"$sum": 1},
            "after_bet_amount": {"$sum": "$after_bet"},
            "withdrawal_amount": {"$sum": "$withdraw"},
            "claim_count": {"$sum": "$claims"},
            "referral_count": {"$sum": "$referrals"},
            "checkin_count": {"$sum": "$checkins"},
        }},
    ]

    try:
        raw_segments = list(snapshots_col.aggregate(pipeline))
    except Exception:
        raw_segments = []

    segments: list[dict] = []
    for raw in raw_segments:
        seg = raw.get("_id") or "unclassified"
        users = int(raw.get("users") or 0)
        after_bet = float(raw.get("after_bet_amount") or 0.0)
        withdrawal = float(raw.get("withdrawal_amount") or 0.0)
        claims = int(raw.get("claim_count") or 0)
        referrals = int(raw.get("referral_count") or 0)
        checkins = int(raw.get("checkin_count") or 0)

        cost_per_user = round(claims / users, 2) if users > 0 else 0.0
        bet_per_user = round(after_bet / users, 2) if users > 0 else 0.0
        claim_per_user = round(claims / users, 2) if users > 0 else 0.0
        referral_per_user = round(referrals / users, 2) if users > 0 else 0.0
        roi_score = round(after_bet / max(claims, 1), 2)

        segments.append({
            "segment": seg,
            "users": users,
            "claim_count": claims,
            "voucher_cost": claims,
            "after_bet_amount": round(after_bet, 2),
            "withdrawal_amount": round(withdrawal, 2),
            "referral_count": referrals,
            "checkin_count": checkins,
            "cost_per_user": cost_per_user,
            "bet_per_user": bet_per_user,
            "claim_per_user": claim_per_user,
            "referral_per_user": referral_per_user,
            "roi_score": roi_score,
        })

    segments.sort(key=lambda x: x["roi_score"], reverse=True)
    ranking = [s["segment"] for s in segments]
    top_value = ranking[0] if ranking else None
    lowest_value = ranking[-1] if len(ranking) > 1 else (ranking[0] if ranking else None)

    recommendations = _build_roi_recommendations(segments)
    trend = _build_roi_trend(snapshots_col, now=now, months=trend_months)

    available_months: list[str] = []
    try:
        available_months = sorted(
            (m for m in snapshots_col.distinct("snapshot_month") if m),
            reverse=True,
        )
    except Exception:
        pass

    return {
        "ok": True,
        "period": period_label,
        "period_type": period_type,
        "segments": segments,
        "ranking": ranking,
        "top_value_segment": top_value,
        "lowest_value_segment": lowest_value,
        "recommendations": recommendations,
        "trend": trend,
        "available_months": available_months,
        "generated_at": now.isoformat(),
    }


def _build_roi_recommendations(segments: list[dict]) -> dict:
    if not segments:
        return {
            "deserves_more_rewards": None,
            "fewer_vouchers": None,
            "produces_real_betting": [],
            "mainly_cost": [],
        }

    by_roi = sorted(segments, key=lambda x: x["roi_score"], reverse=True)
    by_bet = sorted(segments, key=lambda x: x["after_bet_amount"], reverse=True)

    produces_real_betting = [
        s["segment"] for s in by_bet if s["after_bet_amount"] > 0
    ][:3]

    mainly_cost = [
        s["segment"]
        for s in segments
        if s["claim_count"] > 0 and s["after_bet_amount"] == 0.0
    ]

    # "fewer vouchers" — highest claims but zero (or near-zero) betting
    fewer_vouchers_candidates = [
        s for s in sorted(segments, key=lambda x: x["claim_count"], reverse=True)
        if s["roi_score"] < 5.0 and s["claim_count"] > 0
    ]
    fewer_vouchers = fewer_vouchers_candidates[0]["segment"] if fewer_vouchers_candidates else None

    return {
        "deserves_more_rewards": by_roi[0]["segment"] if by_roi else None,
        "fewer_vouchers": fewer_vouchers,
        "produces_real_betting": produces_real_betting,
        "mainly_cost": mainly_cost,
    }


def _build_roi_trend(snapshots_col, *, now: datetime, months: int = 3) -> list[dict]:
    """Month-over-month ROI metrics per segment for the last N available months."""
    try:
        all_months = sorted(
            (m for m in snapshots_col.distinct("snapshot_month") if m),
            reverse=True,
        )
    except Exception:
        return []

    selected = all_months[:months]
    if not selected:
        return []

    pipeline: list[dict] = [
        {"$match": {"snapshot_month": {"$in": selected}}},
        {"$sort": {"snapshot_week": -1}},
        # One row per (account, month)
        {"$group": {
            "_id": {"month": "$snapshot_month", "account": "$account"},
            "segment": {"$first": "$backend_segment"},
            "after_bet": {"$first": {"$ifNull": ["$metrics_snapshot.after_total_bet_amount", 0]}},
            "claims": {"$first": {"$ifNull": ["$metrics_snapshot.claim_count", 0]}},
            "referrals": {"$first": {"$ifNull": ["$metrics_snapshot.referral_count", 0]}},
        }},
        {"$group": {
            "_id": {"month": "$_id.month", "segment": "$segment"},
            "users": {"$sum": 1},
            "after_bet_amount": {"$sum": "$after_bet"},
            "claim_count": {"$sum": "$claims"},
            "referral_count": {"$sum": "$referrals"},
        }},
        {"$sort": {"_id.month": 1}},
    ]

    try:
        raw = list(snapshots_col.aggregate(pipeline))
    except Exception:
        return []

    by_month: dict[str, dict] = {}
    for row in raw:
        month = (row.get("_id") or {}).get("month") or "unknown"
        seg = (row.get("_id") or {}).get("segment") or "unclassified"
        users = int(row.get("users") or 0)
        after_bet = float(row.get("after_bet_amount") or 0.0)
        claims = int(row.get("claim_count") or 0)
        roi = round(after_bet / max(claims, 1), 2)
        if month not in by_month:
            by_month[month] = {}
        by_month[month][seg] = {
            "users": users,
            "after_bet_amount": round(after_bet, 2),
            "claim_count": claims,
            "referral_count": int(row.get("referral_count") or 0),
            "roi_score": roi,
        }

    return [{"month": m, "segments": by_month[m]} for m in sorted(by_month)]


def _live_feature_flag(flag_name: str, env: Mapping, env_name: str, env_default: str):
    try:
        from settings_service import get_setting as _live_setting
        value = _live_setting("feature_flags", flag_name)
        if value is not None:
            return "1" if value else "0"
    except Exception:
        pass
    return _env(env, env_name, env_default)


def build_settings_panel(env: Mapping | None = None, *, constants: Mapping | None = None) -> dict:
    import os

    env = env if env is not None else os.environ
    c = dict(constants or {})

    voucher_settings = {
        "welcome_window_hours": _env(env, "WELCOME_WINDOW_HOURS", "48"),
        "welcome_reminder_after_hours": _env(env, "WELCOME_REMINDER_AFTER_HOURS", "12"),
        "welcome_final_warning_hours": _env(env, "WELCOME_FINAL_WARNING_HOURS", "36"),
        "welcome_expiry_hours": _env(env, "WELCOME_EXPIRY_HOURS", "48"),
        "welcome_unclaimed_window_days": _env(env, "WELCOME_UNCLAIMED_WINDOW_DAYS", "7"),
        "welcome_claimed_visible_days": _env(env, "WELCOME_CLAIMED_VISIBLE_DAYS", "3"),
        "public_pool_fingerprint_salt": _env(env, "PUBLIC_POOL_FINGERPRINT_SALT"),
        "campaign_status_values": ["active", "upcoming", "expired", "paused"],
        "eligibility_modes": ["public", "tier", "user_id", "admin_only"],
    }

    try:
        from settings_service import get_setting as _live_setting
        _live_qualify_hold_hours = _live_setting("referral_config", "qualify_hold_hours")
    except Exception:
        _live_qualify_hold_hours = None

    referral_settings = {
        "qualify_hold_hours": (
            _live_qualify_hold_hours
            if _live_qualify_hold_hours is not None
            else _env(env, "REFERRAL_QUALIFY_HOURS", _env(env, "REFERRAL_HOLD_HOURS", "48"))
        ),
        "hourly_limit": _env(env, "REFERRAL_HOURLY_LIMIT", "20"),
        "daily_limit": _env(env, "REFERRAL_DAILY_LIMIT", "200"),
        "qualified_statuses": _QUALIFIED_STATUSES,
        "pending_statuses": _PENDING_STATUSES,
        "revoked_statuses": _REVOKED_STATUSES,
    }

    affiliate_settings = {
        "tier_thresholds": {
            "T1": _env(env, "AFF_T1_THRESHOLD", "10"),
            "T2": _env(env, "AFF_T2_THRESHOLD", "25"),
            "T3": _env(env, "AFF_T3_THRESHOLD", "50"),
            "T4": _env(env, "AFF_T4_THRESHOLD", "150"),
            "T5": _env(env, "AFF_T5_THRESHOLD", "300"),
        },
        "simulate_mode": _env(env, "AFFILIATE_SIMULATE", "0"),
        "group_trigger_weekly_valid_referrals": _env(
            env, "AFFILIATE_GROUP_TRIGGER_WEEKLY_VALID_REFERRALS", "5"
        ),
        "group_unlock_referrals": _env(env, "AFFILIATE_GROUP_UNLOCK_REFERRALS", "5"),
        "group_dm_enabled": _env(env, "AFFILIATE_GROUP_DM_ENABLED", "1"),
        "pools": _AFF_TIERS,
    }

    xp_checkin_settings = {
        "xp_base_per_checkin": c.get("XP_BASE_PER_CHECKIN"),
        "first_checkin_bonus": c.get("FIRST_CHECKIN_BONUS"),
        "streak_milestones": c.get("STREAK_MILESTONES"),
        "streak_freeze_default_tokens": c.get("STREAK_FREEZE_DEFAULT_TOKENS"),
        "streak_freeze_max_tokens": c.get("STREAK_FREEZE_MAX_TOKENS"),
        "weekly_xp_bucket": c.get("WEEKLY_XP_BUCKET"),
        "weekly_referral_bucket": c.get("WEEKLY_REFERRAL_BUCKET"),
    }

    bot_settings = {
        "main_group_id": c.get("GROUP_ID") or _env(env, "MAIN_GROUP_ID") or _env(env, "GROUP_ID"),
        "official_channel_id": c.get("OFFICIAL_CHANNEL_ID") or _env(env, "OFFICIAL_CHANNEL_ID"),
        "community_chat_id": c.get("COMMUNITY_CHAT_ID") or _env(env, "COMMUNITY_CHAT_ID") or _env(env, "MYWIN_CHAT_ID"),
        "bot_username": _env(env, "BOT_USERNAME"),
        "channel_username": c.get("CHANNEL_USERNAME"),
        "miniapp_version": c.get("MINIAPP_VERSION"),
        "feature_flags": {
            "growth_leaderboard_enabled": _live_feature_flag("growth_leaderboard", env, "GROWTH_LEADERBOARD_ENABLED", "0"),
            "affiliate_group_dm_enabled": _env(env, "AFFILIATE_GROUP_DM_ENABLED", "1"),
            "admin_web_login_enabled": _live_feature_flag("admin_web_login", env, "ADMIN_WEB_LOGIN_ENABLED", "1"),
        },
        "scheduler": {
            "growth_leaderboard_cron_day": _env(env, "GROWTH_LEADERBOARD_CRON_DAY", "SUN"),
            "growth_leaderboard_cron_hour": _env(env, "GROWTH_LEADERBOARD_CRON_HOUR", "21"),
            "growth_leaderboard_cron_minute": _env(env, "GROWTH_LEADERBOARD_CRON_MINUTE", "0"),
            "timezone": _env(env, "GROWTH_LEADERBOARD_TIMEZONE", "Asia/Kuala_Lumpur"),
        },
    }

    security = {
        "secrets_configured": {
            name: bool(env.get(name)) for name in sorted(_SECRET_ENV_NAMES)
        },
        "note": "Secret values are never exposed by this endpoint — only whether they are configured.",
    }

    return {
        "success": True,
        "read_only": True,
        "as_of": _utc_now().isoformat(),
        "sections": {
            "voucher_settings": voucher_settings,
            "referral_settings": referral_settings,
            "affiliate_settings": affiliate_settings,
            "xp_checkin_settings": xp_checkin_settings,
            "bot_settings": bot_settings,
            "security": security,
        },
    }
