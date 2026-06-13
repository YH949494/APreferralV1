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
    max_campaigns: int = 200,
) -> dict:
    now = now or _utc_now()
    d7 = now - timedelta(days=7)
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
    try:
        drop_docs = list(drops_col.find({}).limit(max_campaigns))
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
            lambda v=variants: voucher_claims_col.count_documents(
                {"drop_id": {"$in": v}, "status": "claimed"}
            )
        )
        failed = grab(
            lambda v=variants: voucher_claims_col.count_documents(
                {"drop_id": {"$in": v}, "status": "failed"}
            )
        )
        remaining = None
        if doc.get("type") == "pooled":
            # Pooled drops keep their own remaining counters.
            pub = doc.get("public_remaining")
            my = doc.get("my_remaining")
            if isinstance(pub, int) or isinstance(my, int):
                remaining = (pub or 0) + (my or 0)
        if remaining is None and isinstance(total_codes, int) and isinstance(claimed, int):
            remaining = max(total_codes - claimed, 0)

        # Failure-reason breakdown for the expandable row.
        failure_reasons = []
        try:
            agg = voucher_claims_col.aggregate(
                [
                    {"$match": {"drop_id": {"$in": variants}, "status": "failed"}},
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
    claimed_codes_all = metric(
        lambda: int(voucher_claims_col.count_documents({"status": "claimed"}))
    )
    remaining_codes_all = metric(
        lambda: int(vouchers_col.count_documents({"status": {"$in": ["unclaimed", "free"]}}))
    )
    claim_rate = None
    if (
        total_codes_all["value"] is not None
        and claimed_codes_all["value"] is not None
    ):
        claim_rate = _pct(claimed_codes_all["value"], total_codes_all["value"])

    failed_7d = metric(
        lambda: int(
            voucher_claims_col.count_documents(
                {"status": "failed", "created_at": {"$gte": d7}}
            )
        ),
        note="Voucher claim attempts that failed in the last 7 days.",
    )

    def _repeat_claimers_7d():
        agg = voucher_claims_col.aggregate(
            [
                {"$match": {"status": "claimed", "claimed_at": {"$gte": d7}}},
                {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}},
                {"$count": "c"},
            ]
        )
        agg_list = list(agg)
        return int(agg_list[0]["c"]) if agg_list else 0

    repeat_claimers_7d = metric(
        _repeat_claimers_7d,
        quality="heuristic",
        note="Distinct users with >1 successful claim in the last 7 days.",
    )

    welcome_claims_7d = metric(
        lambda: int(
            welcome_eligibility_col.count_documents(
                {"claimed": True, "claimed_at": {"$gte": d7}}
            )
        ),
        note="welcome_eligibility records claimed in the last 7 days.",
    )

    return {
        "success": True,
        "as_of": now.isoformat(),
        "summary": {
            "active_campaigns": {"value": status_counts.get("active", 0), "data_quality": "exact"},
            "upcoming_campaigns": {"value": status_counts.get("upcoming", 0), "data_quality": "exact"},
            "ended_campaigns": {"value": status_counts.get("expired", 0), "data_quality": "exact"},
            "paused_campaigns": {"value": status_counts.get("paused", 0), "data_quality": "exact"},
            "total_codes": total_codes_all,
            "claimed_codes": claimed_codes_all,
            "remaining_codes": remaining_codes_all,
            "claim_rate_pct": {
                "value": claim_rate,
                "data_quality": "exact" if claim_rate is not None else "missing",
            },
            "failed_claims_7d": failed_7d,
            "repeat_claimers_7d": repeat_claimers_7d,
            "welcome_claims_7d": welcome_claims_7d,
        },
        "campaigns": rows,
        "partial_errors": errors or None,
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
    for h in affiliate_history:
        for f in h.get("risk_flags") or []:
            tag = f"affiliate:{f}"
            if tag not in risk_flags:
                risk_flags.append(tag)

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
        "segment": user.get("for_bot_segment") or user.get("bot_segment"),
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

    referral_settings = {
        "qualify_hold_hours": _env(env, "REFERRAL_QUALIFY_HOURS", _env(env, "REFERRAL_HOLD_HOURS", "48")),
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
            "growth_leaderboard_enabled": _env(env, "GROWTH_LEADERBOARD_ENABLED", "0"),
            "affiliate_group_dm_enabled": _env(env, "AFFILIATE_GROUP_DM_ENABLED", "1"),
            "admin_web_login_enabled": _env(env, "ADMIN_WEB_LOGIN_ENABLED", "1"),
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
