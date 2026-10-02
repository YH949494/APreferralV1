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

The gate is OFF by default (AFFILIATE_REWARD_RETENTION_DAYS unset/0): no new
row is held, the worker and hooks idle, and ``release_retention_holds``
drains rows held while it was on (scripts/release_affiliate_retention_holds.py).
"""
from __future__ import annotations

import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from pymongo import ASCENDING, ReturnDocument

from affiliate_reward_plans import recipe_required_by_pool
from affiliate_rewards import (
    RETENTION_BROKEN_STATUS,
    RETENTION_HOLD_STATUSES,
    RETENTION_PENDING_STATUS,
    RETENTION_ROLLBACK_REVIEW_REASONS,
    SETTLING_STATUS,
    TIERS,
    _INVENTORY_ONLY_RISK_FLAGS,
    _affiliate_simulate_enabled,
    _as_aware_utc,
    _available_pool_count,
    _batch_claimable_available_count,
    _eligible_tiers_for_count,
    _find_batches_for_period,
    _has_issued_pool_voucher_for_ledger,
    _issue_affiliate_ledger_from_pool,
    _ledger_entitlement_month,
    _ledger_has_affiliate_bundle,
    _ledger_recipe,
    _ledger_uses_denomination_plan,
    _merge_monthly_risk_flags,
    _month_window_from_yyyymm,
    _month_window_utc,
    _no_voucher_filter,
    _risk_flags_for_referrer_month,
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

def official_channel_membership_state(user_id: int, chat_id: int | None = None) -> tuple[str, str | None, int | None]:
    """``(state, reason, retry_after_seconds)`` from the canonical scheduler
    getChatMember check, against the entitlement's frozen ``chat_id`` (the
    scheduler defaults to OFFICIAL_CHANNEL_ID when it is ``None``). Only a
    definitive left/kicked is NOT_MEMBER; every timeout, 429, 5xx,
    malformed/not-ok response, missing config or unknown status is UNKNOWN —
    a Telegram failure must never cost a user their retention streak."""
    try:
        import scheduler
    except Exception as exc:  # pragma: no cover - import failure is environmental
        return MEMBERSHIP_UNKNOWN, f"checker_unavailable_{exc.__class__.__name__}", None
    try:
        status = scheduler._get_official_channel_member_status(
            int(user_id), chat_id=int(chat_id) if chat_id is not None else None,
        )
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

def _break_retention(db, ledger: dict, *, leave_at: datetime, reason: str, now_utc: datetime,
                     extra_set: dict | None = None) -> bool:
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
                **(extra_set or {}),
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

def _for_retention_chat(row: dict, chat_id) -> bool:
    """Whether a membership event on ``chat_id`` concerns this entitlement.
    Rows without a frozen chat, or events without a chat id, always match."""
    frozen = row.get("retention_chat_id")
    return frozen is None or chat_id is None or int(frozen) == int(chat_id)


def on_official_channel_leave(db, *, user_id: int, event_at=None, now_utc: datetime | None = None,
                              chat_id: int | None = None) -> int:
    """Break every open retention window this leave falls inside.

    ``now_utc`` must be the same instant main.member_update_handler just
    wrote to ``users.left_official_channel_at``: it is recorded on every held
    row as ``retention_leave_seen_at`` so the worker's users-record backstop
    never re-judges a leave this hook already adjudicated (e.g. a stale
    Day-5 leave delivered after the Day-6 rejoin, which is ignored here).

    Idempotent: a duplicate delivery finds the row already broken and at
    most re-asserts the same ``last_leave_at``. Returns windows broken.
    """
    now = _now(now_utc)
    ts = _event_ts(event_at, now)
    broken = 0
    for row in _held_rows_for_user(db, user_id):
        seen = {"retention_leave_seen_at": now}
        applies = _for_retention_chat(row, chat_id)
        if row.get("status") == RETENTION_PENDING_STATUS:
            start = _as_aware_utc(row.get("retention_started_at"))
            if not applies or (start is not None and ts < start):
                db.affiliate_ledger.update_one(
                    {"_id": row["_id"], "status": RETENTION_PENDING_STATUS}, {"$set": seen},
                )
                continue
            logger.info(
                "%s uid=%s tier=%s year_month=%s action=leave_detected leave_at=%s",
                LOG_TAG, user_id, row.get("tier"), row.get("year_month"), ts.isoformat(),
            )
            if _break_retention(db, row, leave_at=ts, reason="left_channel", now_utc=now, extra_set=seen):
                broken += 1
        else:
            last_leave = _as_aware_utc(row.get("last_leave_at"))
            update = dict(seen)
            if applies and (last_leave is None or ts > last_leave):
                update.update({"last_leave_at": ts, "updated_at": now})
            db.affiliate_ledger.update_one(
                {"_id": row["_id"], "status": RETENTION_BROKEN_STATUS, "last_leave_at": row.get("last_leave_at")},
                {"$set": update},
            )
    return broken


def on_official_channel_join(db, *, user_id: int, event_at=None, now_utc: datetime | None = None,
                             chat_id: int | None = None) -> int:
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
        if not _for_retention_chat(row, chat_id):
            continue
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
    """A recorded Official Channel leave strictly after the window start.

    The ledger's own ``last_leave_at`` (Telegram event time, written by the
    hook) always counts. The users record is a BACKSTOP for a leave the hook
    never processed: it stores processing time, so it is only trusted when
    it is newer than the last leave the hook adjudicated for this row
    (``retention_leave_seen_at``, the same instant the handler wrote)."""
    ledger_leave = _as_aware_utc(ledger.get("last_leave_at"))
    if ledger_leave is not None and ledger_leave > start:
        return ledger_leave
    users_leave = _as_aware_utc(user_doc.get("left_official_channel_at"))
    seen = _as_aware_utc(ledger.get("retention_leave_seen_at"))
    if users_leave is not None and users_leave > start and (seen is None or users_leave > seen):
        return users_leave
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
    state, reason, retry_after = checker(int(uid), ledger.get("retention_chat_id"))
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
    late_leave = _recorded_leave_after(
        _membership_user_doc(db, uid), db.affiliate_ledger.find_one({"_id": ledger["_id"]}) or ledger, start,
    )
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
# Retention rollback: one-shot release of rows held while the gate was on
# ---------------------------------------------------------------------------
#
# The gate now defaults OFF (immediate issuance, as before it existed). Rows
# created while it was on are still PENDING_RETENTION / RETENTION_BROKEN and
# no other path will issue them, so this drains them exactly once:
#
#   RELEASE         -> CAS held -> SETTLING, then the canonical allocator
#                      (_issue_affiliate_ledger_from_pool: recipe, entitlement-
#                      month batch pin, lease/fencing, reconciliation). Out of
#                      stock lands in the allocator's own PENDING_MANUAL.
#   REVIEW_BLOCKED  -> CAS held -> PENDING_REVIEW (admin approve/reject), no
#   REVIEW_RISK        inventory touched.
#   EXCLUDE_ALREADY_ISSUED / _DUPLICATE_TIER / _BELOW_THRESHOLD / _INVALID
#                   -> CAS held -> PENDING_REVIEW, review_reason
#                      retention_rollback_<class>; nothing issued, nothing
#                      else rewritten. Left held they would still be released
#                      by the retention worker at unlock_at.
#   EXCLUDE_INTEGRITY -> a commit REFUSES to run at all while any exists
#                      (issued pool rows already linked to a held ledger need
#                      a human before anything moves).
#
# Every PENDING_REVIEW written here carries a reason in
# affiliate_rewards.RETENTION_ROLLBACK_REVIEW_REASONS, which the evaluator
# never settles on its own: admin approve/reject is the only way out.
#
# Every write is conditional on the row still being held, so a re-run, the
# retention worker, the leave/rejoin hooks and the evaluator can race it
# safely: exactly one actor ever moves a row out of a held status, and the
# allocator's lease decides who allocates.

BACKFILL_RELEASE = "RELEASE"
BACKFILL_REVIEW_BLOCKED = "REVIEW_BLOCKED"
BACKFILL_REVIEW_RISK = "REVIEW_RISK"
BACKFILL_EXCLUDE_ALREADY_ISSUED = "EXCLUDE_ALREADY_ISSUED"
BACKFILL_EXCLUDE_INTEGRITY = "EXCLUDE_INTEGRITY"
BACKFILL_EXCLUDE_DUPLICATE_TIER = "EXCLUDE_DUPLICATE_TIER"
BACKFILL_EXCLUDE_BELOW_THRESHOLD = "EXCLUDE_BELOW_THRESHOLD"
BACKFILL_EXCLUDE_INVALID = "EXCLUDE_INVALID"
BACKFILL_CLASSES = (
    BACKFILL_RELEASE,
    BACKFILL_REVIEW_BLOCKED,
    BACKFILL_REVIEW_RISK,
    BACKFILL_EXCLUDE_ALREADY_ISSUED,
    BACKFILL_EXCLUDE_INTEGRITY,
    BACKFILL_EXCLUDE_DUPLICATE_TIER,
    BACKFILL_EXCLUDE_BELOW_THRESHOLD,
    BACKFILL_EXCLUDE_INVALID,
)
_REVIEW_REASON_BY_CLASS = {
    BACKFILL_REVIEW_BLOCKED: "retention_rollback_blocked_user",
    BACKFILL_REVIEW_RISK: "retention_rollback_risk_flags",
}
# Excluded rows that must not stay worker-releasable: parked, never issued.
_PARKED_EXCLUDE_CLASSES = (
    BACKFILL_EXCLUDE_ALREADY_ISSUED,
    BACKFILL_EXCLUDE_DUPLICATE_TIER,
    BACKFILL_EXCLUDE_BELOW_THRESHOLD,
    BACKFILL_EXCLUDE_INVALID,
)
_REVIEW_REASON_BY_CLASS.update({cls: f"retention_rollback_{cls.lower()}" for cls in _PARKED_EXCLUDE_CLASSES})
# The evaluator's "never auto-settle" list must cover every reason written here.
assert set(_REVIEW_REASON_BY_CLASS.values()) <= RETENTION_ROLLBACK_REVIEW_REASONS
ROLLBACK_SOURCE = "retention_rollback"


def _mask_user_id(user_id) -> str:
    text = str(user_id if user_id is not None else "")
    if len(text) <= 4:
        return "*" * len(text)
    return f"{text[:2]}{'*' * (len(text) - 4)}{text[-2:]}"


_YYYYMM_RE = re.compile(r"\d{4}(0[1-9]|1[0-2])")


def validate_entitlement_month_filter(value) -> str | None:
    """``None`` (no filter) or a strict ``YYYYMM`` string; anything else
    (``2026-10``, ``20261001``, ``abc``, ``""``, month 13) raises ValueError.
    Strict on purpose: unlike ``_normalize_entitlement_month`` it does not
    strip, so a typo can never silently widen or empty the population."""
    if value is None:
        return None
    text = value if isinstance(value, str) else ""
    if not _YYYYMM_RE.fullmatch(text):
        raise ValueError(f"entitlement_month must be YYYYMM (e.g. 202610), got {value!r}")
    return text


def _entitlement_month_filter(entitlement_month: str) -> dict:
    """Mongo filter equivalent to ``_ledger_entitlement_month(row) == m``:
    the stored ``entitlement_month``, else the historical ``year_month``."""
    return {
        "$or": [
            {"entitlement_month": entitlement_month},
            {"entitlement_month": {"$in": [None, ""]}, "year_month": entitlement_month},
        ]
    }


def _held_cas_filter(ledger_id, statuses, entitlement_month: str | None = None) -> dict:
    """The row is still held, still a monthly tier entitlement, and carries
    no voucher of any kind — the only state either backfill write accepts.
    With ``entitlement_month`` the row must also still belong to that month."""
    cas = {
        "_id": ledger_id,
        "ledger_type": LEDGER_TYPE,
        "status": {"$in": sorted(statuses)},
        "$and": [_no_voucher_filter(), {"vouchers": {"$in": [None, []]}}],
    }
    if entitlement_month is not None:
        cas["$and"].append(_entitlement_month_filter(entitlement_month))
    return cas


def _classify_held_row(db, row: dict) -> tuple[str, dict]:
    """``(class, detail)`` for one held row. Reads only."""
    detail: dict = {"reasons": []}
    reasons = detail["reasons"]
    uid = row.get("user_id")
    tier = str(row.get("tier") or "").strip().upper()
    year_month = str(row.get("year_month") or "")
    entitlement_month = _ledger_entitlement_month(row)
    detail.update({"tier": tier or None, "entitlement_month": entitlement_month})

    # -- shape --------------------------------------------------------------
    if row.get("ledger_type") != LEDGER_TYPE:
        reasons.append("not_affiliate_monthly")
    if tier not in TIERS:
        reasons.append("invalid_tier")
    if not isinstance(uid, int) or isinstance(uid, bool):
        reasons.append("invalid_user_id")
    if entitlement_month is None or entitlement_month != year_month:
        reasons.append("entitlement_month_mismatch")
    if str(row.get("pool_id") or "").strip().upper() != tier:
        reasons.append("pool_id_mismatch")
    if not reasons and row.get("dedup_key") != f"AFF:{uid}:{year_month}:{tier}":
        reasons.append("dedup_key_mismatch")
    if reasons:
        return BACKFILL_EXCLUDE_INVALID, detail

    # -- already issued / integrity ----------------------------------------
    has_code = bool(str(row.get("voucher_code") or "").strip())
    voucher_rows = len(row.get("vouchers") or [])
    detail.update({"has_voucher_code": has_code, "ledger_voucher_count": voucher_rows})
    if has_code or voucher_rows or _ledger_has_affiliate_bundle(row):
        reasons.append("ledger_carries_voucher")
        return BACKFILL_EXCLUDE_ALREADY_ISSUED, detail
    linked = db.voucher_pools.count_documents(
        {"status": "issued", "$or": [{"issued_for_ledger_id": str(row["_id"])}, {"ledger_id": row["_id"]}]}
    )
    detail["linked_issued_pool_rows"] = int(linked)
    if linked:
        reasons.append("issued_pool_rows_linked_to_held_ledger")
        return BACKFILL_EXCLUDE_INTEGRITY, detail

    # -- one entitlement per user/month/tier -------------------------------
    siblings = list(
        db.affiliate_ledger.find(
            {"_id": {"$ne": row["_id"]}, "ledger_type": LEDGER_TYPE, "user_id": uid,
             "year_month": year_month, "tier": tier},
            projection={"status": 1},
        )
    )
    if siblings:
        detail["duplicate_statuses"] = sorted(str(s.get("status") or "") for s in siblings)
        reasons.append("another_ledger_for_same_user_month_tier")
        return BACKFILL_EXCLUDE_DUPLICATE_TIER, detail

    # -- still earned (the evaluator's own count + tier rule) ---------------
    start_utc, end_utc = _month_window_from_yyyymm(entitlement_month)
    qualified_now = db.qualified_events.count_documents(
        {"referrer_id": uid, "qualified_at": {"$gte": start_utc, "$lt": end_utc}}
    )
    detail["qualified_count_now"] = int(qualified_now)
    if tier not in _eligible_tiers_for_count(int(qualified_now)):
        reasons.append("qualified_count_below_tier_threshold")
        return BACKFILL_EXCLUDE_BELOW_THRESHOLD, detail

    # -- genuine review signals (inventory flags are NOT abuse) -------------
    user_doc = db.users.find_one({"user_id": uid}, {"blocked": 1}) or {}
    try:
        fresh = _risk_flags_for_referrer_month(db, referrer_id=uid, start_utc=start_utc, end_utc=end_utc)
    except Exception:
        logger.exception("%s uid=%s action=rollback_risk_calc_failed", LOG_TAG, uid)
        fresh = ["risk_flags_calc_failed"]
    stored = list(row.get("risk_flags") or [])
    abuse = sorted({f for f in list(stored) + list(fresh) if f not in _INVENTORY_ONLY_RISK_FLAGS})
    detail.update({
        "blocked": bool(user_doc.get("blocked")),
        "abuse_flags": abuse,
        "inventory_flags": sorted(f for f in stored if f in _INVENTORY_ONLY_RISK_FLAGS),
        "merged_risk_flags": _merge_monthly_risk_flags(stored, sorted(set(fresh) | set(abuse))),
    })
    if user_doc.get("blocked"):
        reasons.append("user_blocked")
        return BACKFILL_REVIEW_BLOCKED, detail
    if abuse:
        reasons.append("abuse_risk_flags")
        return BACKFILL_REVIEW_RISK, detail
    return BACKFILL_RELEASE, detail


def _mark_for_review(
    db, row: dict, cls: str, detail: dict, *, statuses, now_utc: datetime, entitlement_month: str | None = None,
) -> dict | None:
    """CAS held -> PENDING_REVIEW. Never issues, never touches inventory.

    REVIEW_* rows must also carry no voucher and get their merged risk flags.
    Parked EXCLUDE_* rows keep everything they already carry (an
    EXCLUDE_ALREADY_ISSUED row HAS a voucher, so that guard cannot apply);
    only the status, the reason and the retention bookkeeping change.
    """
    set_doc = {
        "status": "PENDING_REVIEW",
        "review_reason": _REVIEW_REASON_BY_CLASS[cls],
        # The retention phase is over (waived, not served). Recorded so an
        # admin approval of a previous-month entitlement still draws from
        # that month's own batch (see _issue_denomination_bundle), and so no
        # announcement quotes a hold the user never served.
        "retention_completed_at": now_utc,
        "retention_waived_at": now_utc,
        "retention_release_source": ROLLBACK_SOURCE,
        "updated_at": now_utc,
    }
    if cls in _PARKED_EXCLUDE_CLASSES:
        cas = {"_id": row["_id"], "ledger_type": LEDGER_TYPE, "status": {"$in": sorted(statuses)}}
        if entitlement_month is not None:
            cas["$and"] = [_entitlement_month_filter(entitlement_month)]
        set_doc["retention_rollback_reasons"] = list(detail.get("reasons") or [])
    else:
        cas = _held_cas_filter(row["_id"], statuses, entitlement_month)
        flags = list(detail.get("merged_risk_flags") or [])
        if cls == BACKFILL_REVIEW_BLOCKED and "blocked_user" not in flags:
            flags.append("blocked_user")
        set_doc["risk_flags"] = flags
    return db.affiliate_ledger.find_one_and_update(
        cas,
        {"$set": set_doc, "$unset": {"unlock_at": "", "retention_next_check_at": ""}},
        return_document=ReturnDocument.BEFORE,
    )


def _release_held_row(
    db, row: dict, *, statuses, now_utc: datetime, entitlement_month: str | None = None,
) -> tuple[dict | None, dict | None]:
    """CAS held -> SETTLING, then the canonical allocator. Returns
    ``(before, after)``; ``before`` is None when another actor moved the row
    first (nothing was written)."""
    before = db.affiliate_ledger.find_one_and_update(
        _held_cas_filter(row["_id"], statuses, entitlement_month),
        {
            "$set": {
                "status": SETTLING_STATUS,
                # Set BEFORE allocation: a previous-month entitlement (e.g.
                # 202609 released in October) may then draw from its OWN
                # month's batch past that batch's end — the allocator's
                # existing retention-release exception. Never a later batch.
                "retention_completed_at": now_utc,
                "retention_waived_at": now_utc,
                "retention_release_source": ROLLBACK_SOURCE,
                "updated_at": now_utc,
            },
            "$unset": {"unlock_at": "", "retention_next_check_at": ""},
        },
        return_document=ReturnDocument.BEFORE,
    )
    if before is None:
        return None, None
    after = _issue_affiliate_ledger_from_pool(
        db, ledger=db.affiliate_ledger.find_one({"_id": row["_id"]}), now_utc=now_utc,
    )
    return before, after or db.affiliate_ledger.find_one({"_id": row["_id"]})


def _entitlement_inventory(db, *, release_demand: dict, review_demand: dict, now_utc: datetime) -> dict:
    """Claimable stock in the batch each entitlement month is pinned to,
    against RELEASE demand (issued now) and RELEASE + REVIEW demand (if every
    review row were later approved). ``shortfall`` is for RELEASE only."""
    out = {}
    for (month, pool_id, legacy) in sorted(set(release_demand) | set(review_demand)):
        key = f"{month}:{pool_id}"
        start_utc, end_utc = _month_window_from_yyyymm(month)
        matches = (
            _find_batches_for_period(db, pool_id=pool_id, period_start_utc=start_utc, period_end_utc=end_utc)
            if start_utc is not None else []
        )
        entry = {
            "entitlement_month": month,
            "pool_id": pool_id,
            "required": int(release_demand.get((month, pool_id, legacy), 0)),
            "required_including_review": int(review_demand.get((month, pool_id, legacy), 0)),
            "batches": len(matches),
        }
        if len(matches) == 1:
            batch = matches[0]
            ends_at = _as_aware_utc(batch.get("ends_at"))
            entry.update({
                "source": "entitlement_batch",
                "batch_id": str(batch.get("_id")),
                "batch_name": batch.get("batch_name"),
                "batch_window_closed": bool(ends_at and now_utc >= ends_at),
                "available": _batch_claimable_available_count(db, batch),
            })
        elif not matches and legacy:
            entry.update({"source": "legacy_undated_pool",
                          "available": _available_pool_count(db, pool_id=pool_id, now_utc=now_utc, legacy_only=True)})
        else:
            entry.update({"source": "ambiguous_batches" if matches else "no_batch_for_entitlement_period",
                          "available": 0})
        entry["shortfall"] = max(0, entry["required"] - int(entry["available"]))
        out[key] = entry
    return out


def release_retention_holds(
    db,
    *,
    dry_run: bool = True,
    include_broken: bool = True,
    now_utc: datetime | None = None,
    limit: int | None = None,
    entitlement_month: str | None = None,
) -> dict:
    """One-shot rollback of the retention gate for rows it is holding.

    ``dry_run=True`` (the default) performs reads only. With
    ``dry_run=False`` RELEASE rows are issued through the canonical
    allocator; REVIEW_* rows and the EXCLUDE_* classes in
    ``_PARKED_EXCLUDE_CLASSES`` are parked in PENDING_REVIEW without issuing
    anything. Refuses to write anything while ``AFFILIATE_SIMULATE=1`` or
    while any EXCLUDE_INTEGRITY row exists. Idempotent: a second run finds
    no held rows to act on.

    ``entitlement_month="YYYYMM"`` scopes the whole run — the DB query, every
    report section, the integrity refusal and every write CAS — to rows of
    that entitlement month; rows of any other month are never loaded. ``None``
    (the default) is every month, exactly as before. Raises ValueError for a
    malformed month.
    """
    entitlement_month = validate_entitlement_month_filter(entitlement_month)
    now = _now(now_utc)
    statuses = {RETENTION_PENDING_STATUS} | ({RETENTION_BROKEN_STATUS} if include_broken else set())
    simulate = _affiliate_simulate_enabled()
    report: dict = {
        "generated_at": now.isoformat(),
        "dry_run": bool(dry_run),
        "include_broken": bool(include_broken),
        "simulate_mode": simulate,
        "refused": None,
        "source_statuses": sorted(statuses),
        "held_total": 0,
        "held_by_status": {},
        "class_counts": {cls: 0 for cls in BACKFILL_CLASSES},
        "by_tier": {},
        "by_entitlement_month": {},
        "demand_release": {},
        "demand_including_review": {},
        "inventory": {},
        "outcomes": {},
        "rows": [],
    }
    if entitlement_month is not None:
        report["entitlement_month_filter"] = entitlement_month
    if simulate and not dry_run:
        report["refused"] = "affiliate_simulate_enabled"
        logger.warning("%s action=rollback_refused reason=affiliate_simulate_enabled", LOG_TAG)
        return report

    query = {"ledger_type": LEDGER_TYPE, "status": {"$in": sorted(statuses)}}
    if entitlement_month is not None:
        query.update(_entitlement_month_filter(entitlement_month))
    # limit=0 is "no limit" for pymongo (which rejects None).
    rows = list(db.affiliate_ledger.find(query, sort=[("created_at", ASCENDING), ("_id", ASCENDING)],
                                         limit=max(0, int(limit or 0))))
    release_demand: dict = {}
    review_demand: dict = {}

    def _bump(bucket: dict, key, cls):
        per = bucket.setdefault(str(key), {c: 0 for c in BACKFILL_CLASSES})
        per[cls] += 1

    def _bump_outcome(name):
        report["outcomes"][name] = report["outcomes"].get(name, 0) + 1

    # Pass 1 classifies everything (reads only); pass 2 writes, so a commit
    # can be refused as a whole before any row moves.
    classified: list = []
    for row in rows:
        if entitlement_month is not None and _ledger_entitlement_month(row) != entitlement_month:
            # Defensive: the query already scoped this; never let a row the
            # filter does not cover into the report or the commit population.
            continue
        status = str(row.get("status") or "")
        report["held_total"] += 1
        report["held_by_status"][status] = report["held_by_status"].get(status, 0) + 1
        try:
            cls, detail = _classify_held_row(db, row)
        except Exception as exc:
            logger.exception("%s ledger_id=%s action=rollback_classify_failed", LOG_TAG, row.get("_id"))
            cls, detail = BACKFILL_EXCLUDE_INVALID, {"reasons": [f"classify_error_{exc.__class__.__name__}"]}
        report["class_counts"][cls] += 1
        _bump(report["by_tier"], detail.get("tier") or "?", cls)
        _bump(report["by_entitlement_month"], detail.get("entitlement_month") or "?", cls)

        if cls in (BACKFILL_RELEASE, BACKFILL_REVIEW_BLOCKED, BACKFILL_REVIEW_RISK):
            legacy = not _ledger_uses_denomination_plan(row)
            for pool_id, qty in recipe_required_by_pool(_ledger_recipe(row)).items():
                key = (detail.get("entitlement_month"), pool_id, legacy)
                review_demand[key] = review_demand.get(key, 0) + int(qty)
                if cls == BACKFILL_RELEASE:
                    release_demand[key] = release_demand.get(key, 0) + int(qty)

        item = {
            "ledger_id": str(row.get("_id")),
            "user": _mask_user_id(row.get("user_id")),
            "tier": detail.get("tier"),
            "entitlement_month": detail.get("entitlement_month"),
            "status": status,
            "class": cls,
            "reasons": list(detail.get("reasons") or []),
            "qualified_count_stored": row.get("qualified_count"),
            "qualified_count_now": detail.get("qualified_count_now"),
            "reward_value": int(row.get("reward_value") or 0),
            "has_voucher_code": detail.get("has_voucher_code", False),
            "ledger_voucher_count": detail.get("ledger_voucher_count", 0),
            "linked_issued_pool_rows": detail.get("linked_issued_pool_rows", 0),
            "blocked": detail.get("blocked"),
            "abuse_flags": detail.get("abuse_flags", []),
            "inventory_flags": detail.get("inventory_flags", []),
            "earned_at": _iso(row.get("earned_at")),
            "unlock_at": _iso(row.get("unlock_at")),
            "retention_broken_reason": row.get("retention_broken_reason"),
        }
        if "duplicate_statuses" in detail:
            item["duplicate_statuses"] = detail["duplicate_statuses"]
        report["rows"].append(item)
        classified.append((row, cls, detail, item))

    if not dry_run and report["class_counts"][BACKFILL_EXCLUDE_INTEGRITY]:
        report["refused"] = "integrity_conflicts"
        logger.warning(
            "%s action=rollback_refused reason=integrity_conflicts count=%s",
            LOG_TAG, report["class_counts"][BACKFILL_EXCLUDE_INTEGRITY],
        )
        classified = []

    for row, cls, detail, item in ([] if dry_run else classified):
        try:
            if cls == BACKFILL_RELEASE:
                before, after = _release_held_row(
                    db, row, statuses=statuses, now_utc=now, entitlement_month=entitlement_month,
                )
                if before is None:
                    item["outcome"] = "lost_race"
                else:
                    final = str((after or {}).get("status") or "")
                    item["outcome"] = {"ISSUED": "issued", "PENDING_MANUAL": "pending_manual",
                                       SETTLING_STATUS: "in_progress"}.get(final, f"status_{final.lower()}")
                    item["final_status"] = final
                    if final == "PENDING_MANUAL":
                        item["inventory_flags_after"] = sorted(
                            f for f in (after or {}).get("risk_flags") or [] if f in _INVENTORY_ONLY_RISK_FLAGS
                        )
                logger.info(
                    "%s uid=%s tier=%s year_month=%s action=rollback_release outcome=%s",
                    LOG_TAG, row.get("user_id"), detail.get("tier"), detail.get("entitlement_month"),
                    item["outcome"],
                )
            elif cls in _REVIEW_REASON_BY_CLASS:
                before = _mark_for_review(
                    db, row, cls, detail, statuses=statuses, now_utc=now, entitlement_month=entitlement_month,
                )
                item["outcome"] = "lost_race" if before is None else "pending_review"
                logger.info(
                    "%s uid=%s tier=%s year_month=%s action=rollback_review class=%s outcome=%s",
                    LOG_TAG, row.get("user_id"), detail.get("tier"), detail.get("entitlement_month"),
                    cls, item["outcome"],
                )
            else:
                item["outcome"] = "untouched"
        except Exception as exc:
            # A crash after the SETTLING CAS is recovered by the existing
            # stale-SETTLING retry sweep (no lease => eligible after TTL).
            logger.exception("%s ledger_id=%s action=rollback_failed", LOG_TAG, row.get("_id"))
            item["outcome"] = f"error_{exc.__class__.__name__}"
        _bump_outcome(item["outcome"])

    def _fmt(demand: dict) -> dict:
        return {f"{m}:{p}": int(q) for (m, p, _legacy), q in sorted(demand.items())}

    report["demand_release"] = _fmt(release_demand)
    report["demand_including_review"] = _fmt(review_demand)
    report["inventory"] = _entitlement_inventory(
        db, release_demand=release_demand, review_demand=review_demand, now_utc=now,
    )
    logger.info(
        "%s action=rollback_done dry_run=%s entitlement_month=%s held_total=%s class_counts=%s outcomes=%s",
        LOG_TAG, dry_run, entitlement_month or "*", report["held_total"], report["class_counts"],
        report["outcomes"],
    )
    return report


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
