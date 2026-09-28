"""Continuous Official Channel retention gate for AFFILIATE_MONTHLY tier rewards.

Scope: ONLY affiliate/referral tier-reward entitlements
(``affiliate_ledger`` rows with ``ledger_type == "AFFILIATE_MONTHLY"``). The
WELCOME voucher, weekly ledgers, public/pooled/personalised drops, VIP,
Surprise, Mission, Campaign and admin-issued vouchers never pass through this
module, and no shared voucher-allocation helper is gated by it.

Lifecycle (statuses owned here are non-final and are excluded from every
existing issuance path in ``affiliate_rewards``)::

    tier reached ──► PENDING_RETENTION ──(unlock_at passed, no leave recorded
         │               │    ▲            since retention_started_at, live
         │         leave │    │ rejoin     check MEMBER, atomic acquire)──►
         │               ▼    │                                  SETTLING ──►
         └─(already out)► RETENTION_BROKEN               existing issuance:
                                                         ISSUED / PENDING_MANUAL
                                                         (stock) / reconcile

Membership evidence, all reused (nothing parallel is recorded):

* ``users.left_official_channel_at`` / ``rejoined_official_channel_at`` /
  ``official_channel_currently_subscribed`` — stamped by
  ``main.member_update_handler`` on every Official Channel transition. This
  is the same canonical record the referral hold already uses to prove
  "continuously subscribed" (scheduler.settle_pending_referrals).
* ``scheduler._get_official_channel_member_status`` — the canonical,
  uncached getChatMember check against ``referral_destination``'s
  OFFICIAL_CHANNEL_ID, adapted here to MEMBER / NOT_MEMBER / UNKNOWN.

Voucher inventory is never read or touched while an entitlement is held; the
only path to inventory is ``affiliate_rewards._issue_affiliate_ledger_from_pool``
after this module has won the PENDING_RETENTION -> SETTLING transition.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from pymongo import ASCENDING

from affiliate_rewards import (
    RETENTION_BROKEN_STATUS,
    RETENTION_HOLD_STATUSES,
    RETENTION_PENDING_STATUS,
    SETTLING_STATUS,
    _as_aware_utc,
    _has_issued_pool_voucher_for_ledger,
    _issue_affiliate_ledger_from_pool,
    _ledger_has_affiliate_bundle,
    _month_window_utc,
    _no_voucher_filter,
    affiliate_retention_period,
)

logger = logging.getLogger(__name__)

LEDGER_TYPE = "AFFILIATE_MONTHLY"
LOG_TAG = "[AFF_REWARD_RETENTION]"

MEMBERSHIP_MEMBER = "member"
MEMBERSHIP_NOT_MEMBER = "not_member"
MEMBERSHIP_UNKNOWN = "unknown"

_MEMBER_STATUSES = frozenset({"member", "administrator", "creator"})
# scheduler._get_official_channel_member_status already maps
# restricted(is_member=True) -> "member" and restricted(is_member!=True) ->
# "kicked", so these two are the only definitive negatives it can return.
_NOT_MEMBER_STATUSES = frozenset({"left", "kicked"})

RATE_LIMITED_REASON = "telegram_429"
# Inventory-shortage flags the existing allocator raises when it parks a
# ledger in PENDING_MANUAL — reported as out_of_stock by this worker; the
# existing stuck-ledger retry sweep resumes them once stock is replenished.
_OUT_OF_STOCK_FLAGS = frozenset({
    "pool_empty",
    "bundle_denomination_short",
    "target_batch_empty",
    "no_batch_for_entitlement_period",
})

_HOOK_ROW_LIMIT = 50


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def retention_batch_limit() -> int:
    return max(1, _env_int("AFFILIATE_RETENTION_BATCH_LIMIT", 100))


def retention_max_runtime_seconds() -> int:
    return max(1, _env_int("AFFILIATE_RETENTION_MAX_RUNTIME_SECONDS", 60))


def _now(now_utc: datetime | None) -> datetime:
    return _as_aware_utc(now_utc) or datetime.now(timezone.utc)


def _event_ts(event_at, now_utc: datetime) -> datetime:
    """The membership transition's own time (Telegram ``ChatMemberUpdated
    .date``), which is identical across duplicate deliveries of one update —
    that is what makes the hooks idempotent. Missing or future values fall
    back to ``now_utc``."""
    ts = _as_aware_utc(event_at)
    if ts is None or ts > now_utc:
        return now_utc
    return ts


def _iso(value) -> str | None:
    value = _as_aware_utc(value)
    return value.isoformat() if value else None


def _required_seconds(ledger: dict) -> int:
    try:
        seconds = int(ledger.get("retention_required_seconds") or 0)
    except (TypeError, ValueError):
        seconds = 0
    if seconds > 0:
        return seconds
    period = affiliate_retention_period()
    return int(period.total_seconds()) if period else 7 * 86400


# ---------------------------------------------------------------------------
# Membership (tri-state)
# ---------------------------------------------------------------------------

def official_channel_membership_state(user_id: int) -> tuple[str, str | None, int | None]:
    """``(state, reason, retry_after_seconds)`` from the canonical scheduler
    getChatMember check. Only a definitive left/kicked is NOT_MEMBER; every
    timeout, 429, 5xx, malformed/not-ok response, missing config or unknown
    status is UNKNOWN — a Telegram failure must never cost a user their
    retention streak."""
    try:
        import scheduler
    except Exception as exc:  # pragma: no cover - import failure is environmental
        return MEMBERSHIP_UNKNOWN, f"checker_unavailable_{exc.__class__.__name__}", None
    try:
        status = scheduler._get_official_channel_member_status(int(user_id))
    except scheduler.ReferralRetryableError as exc:
        message = str(exc)
        if "rate_limited" in message:
            return MEMBERSHIP_UNKNOWN, RATE_LIMITED_REASON, getattr(exc, "retry_after", None)
        return MEMBERSHIP_UNKNOWN, message or "telegram_retryable", getattr(exc, "retry_after", None)
    except scheduler.ReferralTelegramError as exc:
        return MEMBERSHIP_UNKNOWN, f"telegram_not_ok_{getattr(exc, 'kind', 'user')}", None
    except requests.Timeout:
        return MEMBERSHIP_UNKNOWN, "telegram_timeout", None
    except requests.RequestException as exc:
        return MEMBERSHIP_UNKNOWN, f"network_error_{exc.__class__.__name__}", None
    except Exception as exc:
        return MEMBERSHIP_UNKNOWN, f"error_{exc.__class__.__name__}", None
    status = str(status or "").strip()
    if status in _MEMBER_STATUSES:
        return MEMBERSHIP_MEMBER, None, None
    if status in _NOT_MEMBER_STATUSES:
        return MEMBERSHIP_NOT_MEMBER, status, None
    return MEMBERSHIP_UNKNOWN, f"unknown_status_{status or 'none'}", None


# ---------------------------------------------------------------------------
# State transitions (every one is conditional on the exact state it read)
# ---------------------------------------------------------------------------

def _break_retention(db, ledger: dict, *, leave_at: datetime, reason: str, now_utc: datetime) -> bool:
    """PENDING_RETENTION -> RETENTION_BROKEN. The entitlement is kept (never
    REJECTED, no inventory touched); only a later rejoin restarts the window."""
    res = db.affiliate_ledger.update_one(
        {
            "_id": ledger["_id"],
            "status": RETENTION_PENDING_STATUS,
            "retention_started_at": ledger.get("retention_started_at"),
        },
        {
            "$set": {
                "status": RETENTION_BROKEN_STATUS,
                "last_leave_at": leave_at,
                "unlock_at": None,
                "retention_broken_reason": reason,
                "retention_broken_at": now_utc,
                "updated_at": now_utc,
            },
            "$unset": {"retention_next_check_at": ""},
            "$inc": {"retention_break_count": 1},
        },
    )
    if getattr(res, "modified_count", 0) != 1:
        return False
    logger.info(
        "%s uid=%s tier=%s year_month=%s action=retention_broken reason=%s leave_at=%s",
        LOG_TAG, ledger.get("user_id"), ledger.get("tier"), ledger.get("year_month"), reason, leave_at.isoformat(),
    )
    return True


def _restart_retention(db, ledger: dict, *, start_at: datetime, source: str, now_utc: datetime) -> bool:
    """Start a fresh continuous window at ``start_at`` (a rejoin)."""
    unlock_at = start_at + timedelta(seconds=_required_seconds(ledger))
    res = db.affiliate_ledger.update_one(
        {
            "_id": ledger["_id"],
            "status": ledger.get("status"),
            "retention_started_at": ledger.get("retention_started_at"),
            **_no_voucher_filter(),
        },
        {
            "$set": {
                "status": RETENTION_PENDING_STATUS,
                "retention_started_at": start_at,
                "last_rejoin_at": start_at,
                "unlock_at": unlock_at,
                "retention_restart_source": source,
                "updated_at": now_utc,
            },
            "$unset": {"retention_next_check_at": "", "retention_broken_reason": ""},
            "$inc": {"retention_restart_count": 1},
        },
    )
    if getattr(res, "modified_count", 0) != 1:
        return False
    logger.info(
        "%s uid=%s tier=%s year_month=%s action=rejoined source=%s retention_started_at=%s unlock_at=%s",
        LOG_TAG, ledger.get("user_id"), ledger.get("tier"), ledger.get("year_month"), source,
        start_at.isoformat(), unlock_at.isoformat(),
    )
    return True


def _held_rows_for_user(db, user_id: int) -> list[dict]:
    return list(
        db.affiliate_ledger.find(
            {
                "user_id": int(user_id),
                "ledger_type": LEDGER_TYPE,
                "status": {"$in": sorted(RETENTION_HOLD_STATUSES)},
            },
            projection={"vouchers": 0},
            limit=_HOOK_ROW_LIMIT,
        )
    )


# ---------------------------------------------------------------------------
# Official Channel leave / rejoin hooks (called from main.member_update_handler)
# ---------------------------------------------------------------------------

def on_official_channel_leave(db, *, user_id: int, event_at=None, now_utc: datetime | None = None) -> int:
    """Break every open retention window this leave falls inside.

    Idempotent: a duplicate delivery finds the row already broken and at
    most re-asserts the same ``last_leave_at``. A stale/out-of-order leave
    that predates the current window (e.g. a Day-5 leave processed after the
    Day-6 rejoin) is ignored. Returns the number of windows broken.
    """
    now = _now(now_utc)
    ts = _event_ts(event_at, now)
    broken = 0
    for row in _held_rows_for_user(db, user_id):
        if row.get("status") == RETENTION_PENDING_STATUS:
            start = _as_aware_utc(row.get("retention_started_at"))
            if start is not None and ts < start:
                continue
            logger.info(
                "%s uid=%s tier=%s year_month=%s action=leave_detected leave_at=%s",
                LOG_TAG, user_id, row.get("tier"), row.get("year_month"), ts.isoformat(),
            )
            if _break_retention(db, row, leave_at=ts, reason="left_channel", now_utc=now):
                broken += 1
        else:
            last_leave = _as_aware_utc(row.get("last_leave_at"))
            if last_leave is None or ts > last_leave:
                db.affiliate_ledger.update_one(
                    {"_id": row["_id"], "status": RETENTION_BROKEN_STATUS, "last_leave_at": row.get("last_leave_at")},
                    {"$set": {"last_leave_at": ts, "updated_at": now}},
                )
    return broken


def on_official_channel_join(db, *, user_id: int, event_at=None, now_utc: datetime | None = None) -> int:
    """(Re)start the continuous window for every held entitlement.

    * RETENTION_BROKEN: restarts when the join is later than the recorded
      leave. The new window starts at the join — or at the original start if
      the join precedes it, since then the user was already continuously
      subscribed from that start.
    * PENDING_RETENTION: a join strictly after the window start proves the
      user was absent in between (even if that leave was never recorded), so
      the window restarts from the join. This is what closes
      exit -> rejoin-just-before-Day-7.

    Idempotent: a duplicate delivery carries the same Telegram ``date``,
    which is then ``<= retention_started_at`` and is skipped — so unlock_at
    is never pushed later by a repeat of the same transition.
    """
    now = _now(now_utc)
    ts = _event_ts(event_at, now)
    restarted = 0
    for row in _held_rows_for_user(db, user_id):
        start = _as_aware_utc(row.get("retention_started_at"))
        last_leave = _as_aware_utc(row.get("last_leave_at"))
        if row.get("status") == RETENTION_PENDING_STATUS:
            if start is not None and ts <= start:
                continue
            new_start = ts
        else:
            if last_leave is not None and ts <= last_leave:
                continue
            new_start = max(ts, start) if start is not None else ts
        if _restart_retention(db, row, start_at=new_start, source="rejoin_event", now_utc=now):
            restarted += 1
    return restarted


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def _retry_delay_seconds(failures: int, retry_after: int | None) -> int:
    delay = min(3600, 300 * (2 ** max(0, min(int(failures), 4))))
    try:
        if retry_after is not None:
            delay = max(delay, int(retry_after))
    except (TypeError, ValueError):
        pass
    return delay


def _recorded_leave_after(user_doc: dict, ledger: dict, start: datetime) -> datetime | None:
    """A recorded Official Channel leave strictly after the window start,
    from the canonical users record or the ledger's own hook-written field."""
    for value in (user_doc.get("left_official_channel_at"), ledger.get("last_leave_at")):
        leave_at = _as_aware_utc(value)
        if leave_at is not None and leave_at > start:
            return leave_at
    return None


def _membership_user_doc(db, user_id) -> dict:
    return db.users.find_one(
        {"user_id": int(user_id)},
        {
            "left_official_channel_at": 1,
            "rejoined_official_channel_at": 1,
            "official_channel_currently_subscribed": 1,
        },
    ) or {}


def _recover_broken_rows(db, *, now_utc: datetime, batch_limit: int, stats: dict) -> None:
    """DB-only backstop for a rejoin whose hook never ran (process restart,
    Mongo blip): the canonical users record already shows the user back in
    the channel after the recorded leave, so restart from that rejoin."""
    rows = list(
        db.affiliate_ledger.find(
            {"ledger_type": LEDGER_TYPE, "status": RETENTION_BROKEN_STATUS},
            sort=[("retention_checked_at", ASCENDING), ("_id", ASCENDING)],
            limit=batch_limit,
        )
    )
    for row in rows:
        try:
            user_doc = _membership_user_doc(db, row.get("user_id"))
            rejoined = _as_aware_utc(user_doc.get("rejoined_official_channel_at"))
            left = _as_aware_utc(user_doc.get("left_official_channel_at"))
            last_leave = _as_aware_utc(row.get("last_leave_at"))
            start = _as_aware_utc(row.get("retention_started_at"))
            if (
                user_doc.get("official_channel_currently_subscribed") is True
                and rejoined is not None
                and (last_leave is None or rejoined > last_leave)
                and (left is None or rejoined > left)
            ):
                new_start = max(rejoined, start) if start is not None else rejoined
                if _restart_retention(db, row, start_at=new_start, source="recovery_sweep", now_utc=now_utc):
                    stats["recovered"] += 1
        except Exception:
            stats["errors"] += 1
            logger.exception("%s ledger_id=%s action=recovery_failed", LOG_TAG, row.get("_id"))
        finally:
            db.affiliate_ledger.update_one({"_id": row["_id"]}, {"$set": {"retention_checked_at": now_utc}})


def _revert_release(db, ledger_id, *, token: str, leave_at: datetime, now_utc: datetime) -> bool:
    """Undo our own SETTLING acquisition (before any inventory was touched)."""
    if _has_issued_pool_voucher_for_ledger(db, ledger_id=ledger_id):
        return False
    res = db.affiliate_ledger.update_one(
        {"_id": ledger_id, "status": SETTLING_STATUS, "retention_release_token": token, **_no_voucher_filter()},
        {
            "$set": {
                "status": RETENTION_BROKEN_STATUS,
                "last_leave_at": leave_at,
                "unlock_at": None,
                "retention_broken_reason": "leave_recorded_during_release",
                "retention_broken_at": now_utc,
                "updated_at": now_utc,
            },
            "$unset": {"retention_completed_at": "", "retention_release_token": ""},
            "$inc": {"retention_break_count": 1},
        },
    )
    return getattr(res, "modified_count", 0) == 1


def _process_matured_row(db, row: dict, *, now_utc: datetime, checker, stats: dict) -> None:
    ledger = db.affiliate_ledger.find_one({"_id": row["_id"]})
    if not ledger or ledger.get("status") != RETENTION_PENDING_STATUS:
        stats["skipped_state"] += 1
        return
    uid = ledger.get("user_id")
    tier = ledger.get("tier")
    if _ledger_has_affiliate_bundle(ledger) or ledger.get("voucher_code"):
        # Can't happen through this module; never re-issue — let the existing
        # finalize/reconcile paths own it.
        stats["skipped_state"] += 1
        logger.error("%s uid=%s tier=%s action=held_row_has_voucher ledger_id=%s", LOG_TAG, uid, tier, ledger["_id"])
        return
    start = _as_aware_utc(ledger.get("retention_started_at"))
    unlock = _as_aware_utc(ledger.get("unlock_at"))
    if start is None or unlock is None:
        stats["skipped_state"] += 1
        logger.error("%s uid=%s tier=%s action=malformed_retention ledger_id=%s", LOG_TAG, uid, tier, ledger["_id"])
        return
    if now_utc < unlock:
        stats["waiting"] += 1
        logger.info(
            "%s uid=%s tier=%s action=waiting remaining_sec=%s",
            LOG_TAG, uid, tier, int((unlock - now_utc).total_seconds()),
        )
        return

    # (3) Continuity from recorded history — never trust current membership
    # alone: leave for days, rejoin just before Day 7, still subscribed at
    # unlock must NOT pass.
    leave_at = _recorded_leave_after(_membership_user_doc(db, uid), ledger, start)
    if leave_at is not None:
        logger.info("%s uid=%s tier=%s action=leave_detected leave_at=%s", LOG_TAG, uid, tier, leave_at.isoformat())
        if _break_retention(db, ledger, leave_at=leave_at, reason="leave_recorded", now_utc=now_utc):
            stats["broken"] += 1
        return

    # (2) Final live membership check.
    state, reason, retry_after = checker(int(uid))
    if state == MEMBERSHIP_NOT_MEMBER:
        if _break_retention(db, ledger, leave_at=now_utc, reason=f"not_member_at_unlock_{reason}", now_utc=now_utc):
            stats["broken"] += 1
        return
    if state != MEMBERSHIP_MEMBER:
        failures = int(ledger.get("retention_check_failures") or 0)
        delay = _retry_delay_seconds(failures, retry_after)
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"], "status": RETENTION_PENDING_STATUS, "retention_started_at": ledger.get("retention_started_at")},
            {
                "$set": {
                    "retention_checked_at": now_utc,
                    "retention_next_check_at": now_utc + timedelta(seconds=delay),
                    "retention_last_check_reason": reason,
                },
                "$inc": {"retention_check_failures": 1},
            },
        )
        stats["membership_retry"] += 1
        logger.warning(
            "%s uid=%s tier=%s action=membership_retry reason=%s retry_in_sec=%s",
            LOG_TAG, uid, tier, reason, delay,
        )
        if reason == RATE_LIMITED_REASON:
            stats["rate_limited"] = True
        return

    # (4)/(5) Atomic release: only the worker that wins this transition may
    # ever reach inventory for this entitlement.
    token = uuid.uuid4().hex
    acquired = db.affiliate_ledger.update_one(
        {
            "_id": ledger["_id"],
            "status": RETENTION_PENDING_STATUS,
            "retention_started_at": ledger.get("retention_started_at"),
            "unlock_at": ledger.get("unlock_at"),
            **_no_voucher_filter(),
        },
        {
            "$set": {
                "status": SETTLING_STATUS,
                "retention_completed_at": now_utc,
                "retention_release_token": token,
                "updated_at": now_utc,
            },
            "$unset": {"retention_next_check_at": ""},
        },
    )
    if getattr(acquired, "modified_count", 0) != 1:
        stats["lost_race"] += 1
        return

    # Close the window between the history read above and the acquisition:
    # a leave the handler recorded in the meantime still wins.
    late_leave = _recorded_leave_after(_membership_user_doc(db, uid), ledger, start)
    if late_leave is not None and _revert_release(db, ledger["_id"], token=token, leave_at=late_leave, now_utc=now_utc):
        stats["broken"] += 1
        logger.info("%s uid=%s tier=%s action=retention_broken reason=leave_recorded_during_release", LOG_TAG, uid, tier)
        return

    stats["released"] += 1
    result = _issue_affiliate_ledger_from_pool(
        db, ledger=db.affiliate_ledger.find_one({"_id": ledger["_id"]}), now_utc=now_utc,
    ) or {}
    status = result.get("status")
    if status == "ISSUED":
        stats["issued"] += 1
        logger.info("%s uid=%s tier=%s year_month=%s action=issued", LOG_TAG, uid, tier, ledger.get("year_month"))
    elif status in ("PENDING_MANUAL", "OUT_OF_STOCK") and set(result.get("risk_flags") or []) & _OUT_OF_STOCK_FLAGS:
        stats["out_of_stock"] += 1
        logger.warning(
            "%s uid=%s tier=%s year_month=%s action=out_of_stock status=%s",
            LOG_TAG, uid, tier, ledger.get("year_month"), status,
        )
    else:
        logger.info(
            "%s uid=%s tier=%s year_month=%s action=released status=%s",
            LOG_TAG, uid, tier, ledger.get("year_month"), status,
        )


def process_affiliate_retention_entitlements(
    db,
    *,
    now_utc: datetime | None = None,
    batch_limit: int | None = None,
    membership_checker=None,
    max_runtime_seconds: int | None = None,
    clock=time.monotonic,
) -> dict:
    """One bounded pass of the retention worker (run from the 5-minute tick).

    1. Recovery sweep over RETENTION_BROKEN rows (DB only).
    2. Matured PENDING_RETENTION rows, oldest unlock first: verify recorded
       continuity, verify live membership, atomically acquire SETTLING,
       then hand to the existing issuance path.

    Bounded by ``batch_limit`` rows per phase and a wall-clock budget for the
    Telegram phase; a 429 ends the Telegram phase for this tick. Safe to run
    concurrently and repeatedly — every transition is conditional.
    """
    now = _now(now_utc)
    limit = max(1, int(batch_limit or retention_batch_limit()))
    budget = max_runtime_seconds or retention_max_runtime_seconds()
    checker = membership_checker or official_channel_membership_state
    deadline = clock() + budget
    stats = {
        "recovered": 0, "candidates": 0, "waiting": 0, "broken": 0, "membership_retry": 0,
        "released": 0, "issued": 0, "out_of_stock": 0, "lost_race": 0, "skipped_state": 0,
        "errors": 0, "rate_limited": False, "budget_exhausted": False,
    }

    try:
        _recover_broken_rows(db, now_utc=now, batch_limit=limit, stats=stats)
    except Exception:
        stats["errors"] += 1
        logger.exception("%s action=recovery_sweep_failed", LOG_TAG)

    try:
        rows = list(
            db.affiliate_ledger.find(
                {
                    "ledger_type": LEDGER_TYPE,
                    "status": RETENTION_PENDING_STATUS,
                    "unlock_at": {"$lte": now},
                    "$or": [
                        {"retention_next_check_at": None},
                        {"retention_next_check_at": {"$lte": now}},
                    ],
                },
                sort=[("unlock_at", ASCENDING)],
                limit=limit,
            )
        )
    except Exception:
        stats["errors"] += 1
        logger.exception("%s action=query_failed", LOG_TAG)
        rows = []
    stats["candidates"] = len(rows)

    for row in rows:
        if stats["rate_limited"]:
            break
        if clock() > deadline:
            stats["budget_exhausted"] = True
            break
        try:
            _process_matured_row(db, row, now_utc=now, checker=checker, stats=stats)
        except Exception:
            stats["errors"] += 1
            logger.exception("%s ledger_id=%s action=process_failed", LOG_TAG, row.get("_id"))

    logger.info("%s action=sweep_done stats=%s", LOG_TAG, stats)
    return stats


# ---------------------------------------------------------------------------
# User-facing status (no voucher codes, ever)
# ---------------------------------------------------------------------------

_DISPLAY_STATE_BY_STATUS = {
    RETENTION_PENDING_STATUS: "pending_retention",
    RETENTION_BROKEN_STATUS: "retention_broken",
    SETTLING_STATUS: "processing",
    "APPROVED": "processing",
    "PENDING_MANUAL": "processing",
    "PENDING_EOM": "processing",
    "PENDING_REVIEW": "under_review",
    "OUT_OF_STOCK": "processing",
    "ISSUED": "issued",
    "REJECTED": "rejected",
}


def affiliate_reward_retention_view(db, *, user_id: int, now_utc: datetime | None = None) -> list[dict]:
    """Retention-gated tier entitlements for the current and previous KL
    month (a September reward can still be pending in October), newest
    tier first. Only rows created under the retention rule are listed."""
    now = _now(now_utc)
    current_start, _, current_month = _month_window_utc(now)
    _, _, previous_month = _month_window_utc(current_start - timedelta(seconds=1))
    rows = db.affiliate_ledger.find(
        {
            "user_id": int(user_id),
            "year_month": {"$in": [current_month, previous_month]},
            "ledger_type": LEDGER_TYPE,
            "retention_required_seconds": {"$exists": True},
        },
        projection={
            "tier": 1, "year_month": 1, "status": 1, "reward_value": 1, "earned_at": 1,
            "unlock_at": 1, "retention_required_seconds": 1, "last_rejoin_at": 1,
        },
        limit=20,
    )
    out = []
    for row in rows:
        status = str(row.get("status") or "")
        unlock = _as_aware_utc(row.get("unlock_at"))
        remaining = None
        if status == RETENTION_PENDING_STATUS and unlock is not None:
            remaining = max(0, int((unlock - now).total_seconds()))
        out.append(
            {
                "tier": row.get("tier"),
                "year_month": row.get("year_month"),
                "status": status,
                "retention_state": _DISPLAY_STATE_BY_STATUS.get(status, "processing"),
                "reward_value": int(row.get("reward_value") or 0),
                "currency": "$",
                "retention_days": round(_required_seconds(row) / 86400, 2),
                "earned_at": _iso(row.get("earned_at")),
                "unlock_at": _iso(unlock) if status == RETENTION_PENDING_STATUS else None,
                "remaining_seconds": remaining,
                # Served through the public My Stats endpoint: a flag, not the
                # user's exact leave/rejoin times.
                "window_restarted": bool(row.get("last_rejoin_at")),
            }
        )
    out.sort(key=lambda r: (str(r.get("year_month") or ""), str(r.get("tier") or "")), reverse=True)
    return out
