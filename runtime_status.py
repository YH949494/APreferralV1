"""Pure, read-only builders for the Admin "Runtime Status" dashboard.

This module answers one question only: *is this feature actually running in
production right now* — not whether it is configured or whether the code
exists. Every status below is derived at request time from live signals
(scheduler lock/heartbeat documents, feature-flag settings, and counts of
recently-written tracking fields). Nothing here is a hardcoded status
string; a feature reports 🟢 Online only when fresh runtime evidence for it
was found, and reports 🟡/🟠/🔴 when that evidence is absent, disabled, or
stale.

Mirrors the design of ``dashboard_panels.py``: every function is pure and
injectable (collections/settings are passed in) so it can be unit-tested
with fakes and imported without main.py's import-time side effects. Nothing
here writes to the database or touches bot business logic.

Status legend:
  🟢 Online       - enabled and has fresh runtime evidence
  🟡 Waiting      - implemented but disabled, or not yet triggered
  🟠 Warning      - enabled but evidence is stale / inconsistent
  🔴 Offline      - enabled (or expected to run) but no reachable code path
  ⚫ Deprecated   - superseded / manual-only / intentionally retired
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

ONLINE = "🟢 Online"
WAITING = "🟡 Waiting"
WARNING = "🟠 Warning"
OFFLINE = "🔴 Offline"
DEPRECATED = "⚫ Deprecated"


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------

def _aware(ts: datetime | None) -> datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _age_seconds(now: datetime, ts: datetime | None) -> float | None:
    ts = _aware(ts)
    if ts is None:
        return None
    return max((now - ts).total_seconds(), 0.0)


def _fmt_ts(ts: datetime | None) -> str | None:
    ts = _aware(ts)
    return ts.isoformat() if ts else None


def _get_path(doc: Mapping[str, Any] | None, path: str) -> Any:
    """Resolve a dotted Mongo field path (e.g. 'pm_sent.pm0_welcome') against
    a document dict. pymongo returns nested subdocuments as-is; it does not
    flatten dotted paths back into top-level keys, so callers that query by
    a dotted field must walk the path themselves to read the value back."""
    val: Any = doc
    for part in path.split("."):
        val = val.get(part) if isinstance(val, dict) else None
    return val


def _safe_get(fn: Callable[[], Any], default=None):
    """Isolate a single evidence lookup so one bad query degrades one row,
    not the whole panel (same philosophy as dashboard_panels.py)."""
    try:
        return fn()
    except Exception:
        return default


def job_runtime_status(
    now: datetime,
    *,
    enabled: bool | None,
    last_run: datetime | None,
    expected_interval_seconds: float | None,
    stale_multiplier: float = 3.0,
    min_grace_seconds: float = 900.0,
) -> tuple[str, str]:
    """Derive a job's status purely from its enabled flag + last-run evidence."""
    if enabled is False:
        return WAITING, "disabled via settings"
    age = _age_seconds(now, last_run)
    if age is None:
        return (WAITING, "no run recorded yet") if enabled is not False else (WAITING, "disabled, never run")
    threshold = min_grace_seconds
    if expected_interval_seconds:
        threshold = max(expected_interval_seconds * stale_multiplier, min_grace_seconds)
    if age <= threshold:
        return ONLINE, f"last ran {int(age)}s ago"
    return WARNING, f"stale — last ran {int(age)}s ago (expected every ~{int(expected_interval_seconds or 0)}s)"


def flag_gated_status(
    now: datetime,
    *,
    flag_enabled: bool | None,
    evidence_at: datetime | None,
    lookback_seconds: float,
) -> tuple[str, str]:
    """For event-driven features gated by a feature flag: online only if the
    flag is on AND we can find recent evidence it actually fired."""
    if flag_enabled is False:
        return WAITING, "feature flag disabled"
    age = _age_seconds(now, evidence_at)
    if age is None:
        if flag_enabled is None:
            return WAITING, "no runtime evidence found (flag state unknown)"
        return WAITING, "flag enabled but no send has been recorded yet"
    if age <= lookback_seconds:
        return ONLINE, f"last fired {int(age)}s ago"
    return WARNING, f"flag enabled but nothing fired in the last {int(lookback_seconds/86400)}d (last: {int(age)}s ago)"


def unwired_status(evidence_at: datetime | None) -> tuple[str, str]:
    """For code paths confirmed (by audit) to have no caller anywhere in the
    codebase. We still check for evidence at request time in case that
    changes after a future patch, rather than hardcoding the verdict."""
    if evidence_at is not None:
        return WARNING, "runtime evidence found, but no caller was located in the code path audit — re-verify wiring"
    return OFFLINE, "implemented but never invoked anywhere in the codebase (dead code path)"


# ---------------------------------------------------------------------------
# 1) Scheduler Health
# ---------------------------------------------------------------------------

# key: (label, cron_description, lock_source, settings_field, flag_field, interval_seconds)
# lock_source: (collection_key, doc_id, timestamp_field)
SCHEDULER_JOBS: list[dict[str, Any]] = [
    dict(key="weekly_reset", label="XP Snapshot / Weekly Reset", cron="mon 00:00 Asia/Kuala_Lumpur",
         settings_field="xp_snapshot", lock_source=None, interval_seconds=7 * 86400),
    dict(key="monthly_vip", label="Monthly VIP Tier Update", cron="day=1 00:00 Asia/Kuala_Lumpur",
         settings_field=None, lock_source=("audit_events", "monthly_job:last_run", "run_at_utc"),
         interval_seconds=30 * 86400),
    dict(key="affiliate_monthly_settle", label="Affiliate Monthly Settlement", cron="day=1 00:10 Asia/Kuala_Lumpur",
         settings_field="affiliate_monthly_settlement", lock_source=("scheduler_locks", "affiliate_monthly_settle", "updatedAt"),
         interval_seconds=30 * 86400),
    dict(key="affiliate_weekly_settle", label="Affiliate Weekly Settlement", cron="mon 00:15 Asia/Kuala_Lumpur",
         settings_field=None, lock_source=("scheduler_locks", "affiliate_weekly_settle", "updatedAt"),
         interval_seconds=7 * 86400),
    dict(key="affiliate_current_week_issue", label="Affiliate Current-Week Issue", cron="*/30 * * * *",
         settings_field=None, lock_source=("scheduler_locks", "affiliate_current_week_issue", "updatedAt"),
         interval_seconds=1800),
    dict(key="tick_5min", label="Core 5-Minute Tick (Retention KPIs, Referral/XP Snapshot Settlement, Affiliate Snapshot)",
         cron="*/5 * * * *", settings_field="pending_referral_settlement",
         lock_source=("scheduler_locks", "tick_5min", "updatedAt"), interval_seconds=300),
    dict(key="process_verification_queue", label="Verification Queue Sweep", cron="*/2 * * * *",
         settings_field="verification_queue", lock_source=("scheduler_locks", "verification_queue", "updatedAt"),
         interval_seconds=120),
    dict(key="onboarding_due_tick", label="Onboarding Due Tick (PM1-4 sweep)", cron="*/1 * * * *",
         settings_field=None, lock_source=None, interval_seconds=60),
    dict(key="welcome_voucher_lifecycle", label="Welcome Voucher Lifecycle", cron="*/30 * * * *",
         settings_field=None, flag_field="welcome_reward",
         lock_source=("scheduler_locks", "welcome_voucher_lifecycle", "updatedAt"), interval_seconds=1800),
    dict(key="welcome_progress_reminders", label="Welcome Progress Reminders", cron="0 * * * * (hourly)",
         settings_field="welcome_reminder", flag_field="welcome_journey",
         lock_source=("scheduler_locks", "welcome_progress_reminders", "updatedAt"), interval_seconds=3600),
    dict(key="reactivation_journey_evaluate", label="Reactivation Journey Evaluate", cron="*/30 * * * *",
         settings_field="reactivation_journey", flag_field="reactivation", lock_source=None, interval_seconds=1800),
    dict(key="drop_status_reconcile", label="Voucher Drop Status Reconcile / Expiry", cron="*/1 * * * *",
         settings_field=None, lock_source=None, interval_seconds=60),
    dict(key="batch_release_tick", label="Campaign Batch Release Tick", cron="*/1 * * * *",
         settings_field=None, lock_source=None, interval_seconds=60),
    dict(key="affiliate_daily_kpi", label="Affiliate Daily KPI", cron="00:20 UTC",
         settings_field=None, lock_source=None, interval_seconds=86400),
    dict(key="affiliate_weekly_kpi", label="Affiliate Weekly KPI", cron="mon 00:05 UTC",
         settings_field=None, lock_source=None, interval_seconds=7 * 86400),
    dict(key="bot_segment_sheet_sync", label="Bot Segment Sheet Sync", cron="wed 09:30 Asia/Kuala_Lumpur",
         settings_field="bot_segment_sheet_sync", lock_source=("scheduler_locks", "bot_segment_sheet_sync", "updatedAt"),
         interval_seconds=7 * 86400),
    dict(key="growth_leaderboard_weekly", label="Growth Leaderboard Weekly", cron="configurable (default sun 21:00 UTC)",
         settings_field="growth_leaderboard_weekly", flag_field="growth_leaderboard", lock_source=None,
         interval_seconds=7 * 86400),
    dict(key="telegram_member_counts_refresh", label="Telegram Member Counts Refresh", cron="interval (env-configurable, default 60m)",
         settings_field="telegram_member_counts_refresh", lock_source=None, interval_seconds=3600),
]

# Jobs identified in the audit as defined but never wired to any scheduler.
MANUAL_ONLY_SCRIPTS: list[dict[str, str]] = [
    dict(key="cleanup_stale_bse_snapshots", label="Cleanup Stale BSE Snapshots", note="argparse CLI script, no scheduler/import wiring found"),
    dict(key="claim_risk_sync", label="Claim Risk Sync", note="only referenced by its own test file"),
    dict(key="monthly_xp_report", label="Monthly XP Report", note="argparse CLI script, manual only"),
    dict(key="sync_referral_counts", label="Sync Referral Counts", note="docstring states manual-run only; no callers found"),
]


def _lookup_lock_ts(collections: Mapping[str, Any], source: tuple[str, str, str] | None) -> datetime | None:
    if not source:
        return None
    col_key, doc_id, field = source
    col = collections.get(col_key)
    if col is None:
        return None

    def _q():
        doc = col.find_one({"_id": doc_id}) or {}
        return doc.get(field)

    return _safe_get(_q)


def build_scheduler_health(
    collections: Mapping[str, Any],
    scheduler_settings: Mapping[str, Any],
    feature_flags: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for job in SCHEDULER_JOBS:
        enabled = None
        if job.get("settings_field"):
            job_cfg = (scheduler_settings or {}).get(job["settings_field"]) or {}
            enabled = job_cfg.get("enabled")
        if job.get("flag_field") is not None:
            flag_val = (feature_flags or {}).get(job["flag_field"])
            if flag_val is False:
                enabled = False
            elif enabled is None:
                enabled = flag_val

        last_run = _lookup_lock_ts(collections, job.get("lock_source"))
        status, note = job_runtime_status(
            now, enabled=enabled, last_run=last_run,
            expected_interval_seconds=job.get("interval_seconds"),
        )
        rows.append({
            "key": job["key"],
            "job_name": job["label"],
            "cron": job["cron"],
            "enabled": enabled if enabled is not None else "unknown",
            "worker_only": True,
            "last_run": _fmt_ts(last_run),
            "next_run": None,  # APScheduler's live next-run time is only known inside the worker process
            "status": status,
            "notes": note,
        })
    for script in MANUAL_ONLY_SCRIPTS:
        rows.append({
            "key": script["key"],
            "job_name": script["label"],
            "cron": "manual only — no schedule",
            "enabled": "n/a",
            "worker_only": False,
            "last_run": None,
            "next_run": None,
            "status": DEPRECATED,
            "notes": script["note"],
        })
    return rows


# ---------------------------------------------------------------------------
# 2) PM Automation
# ---------------------------------------------------------------------------

def _count_today(collections: Mapping[str, Any], col_key: str, field: str, now: datetime, extra_filter: dict | None = None) -> int | None:
    col = collections.get(col_key)
    if col is None:
        return None
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    def _q():
        filt = {field: {"$gte": day_start}}
        if extra_filter:
            filt.update(extra_filter)
        return int(col.count_documents(filt))

    return _safe_get(_q)


def _last_value(collections: Mapping[str, Any], col_key: str, field: str, extra_filter: dict | None = None) -> datetime | None:
    col = collections.get(col_key)
    if col is None:
        return None

    def _q():
        filt = {field: {"$exists": True, "$ne": None}}
        if extra_filter:
            filt.update(extra_filter)
        doc = col.find_one(filt, sort=[(field, -1)])
        return _get_path(doc, field)

    return _safe_get(_q)


def _skip_reason_breakdown_today(
    collections: Mapping[str, Any], col_key: str, event: str, now: datetime,
    *, stage: str | None = None,
) -> dict[str, int] | None:
    """Group today's skip-tracking events by ``reason`` (top-level field,
    not parsed from ``event``), optionally scoped to one normalized
    ``stage``. Returns None (not zeros) when the source collection is
    unavailable, so callers can tell "no data source" apart from "zero
    skips today"."""
    col = collections.get(col_key)
    if col is None:
        return None
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    def _q():
        match: dict[str, Any] = {"event": event, "created_at": {"$gte": day_start}}
        if stage is not None:
            match["stage"] = stage
        pipeline = [
            {"$match": match},
            {"$group": {"_id": "$reason", "count": {"$sum": 1}}},
        ]
        out: dict[str, int] = {}
        for doc in col.aggregate(pipeline):
            out[str(doc.get("_id") or "unknown")] = int(doc.get("count", 0))
        return out

    return _safe_get(_q)


PM_REGISTRY: list[dict[str, Any]] = [
    dict(key="pm0", label="PM0 Welcome", trigger="/start command (event-driven)",
         users_field="pm_sent.pm0_welcome"),
    dict(key="pm1", label="PM1 Reminder (check-in tip)",
         trigger="scheduled ~1min after Mini App open, swept by onboarding_due_tick (*/1min)",
         users_field="pm1_sent_at_utc"),
    dict(key="pm2", label="PM2 Reminder (MyWin tip)",
         trigger="scheduled 24h after onboarding start, swept by onboarding_due_tick",
         users_field="pm2_sent_at_utc"),
    dict(key="pm3", label="PM3 Reminder (referral tip)",
         trigger="scheduled 48h after onboarding start, swept by onboarding_due_tick",
         users_field="pm3_sent_at_utc"),
    dict(key="pm4", label="PM4 Reminder (72h re-engage)",
         trigger="scheduled 72h after onboarding start, swept by onboarding_due_tick",
         users_field="pm4_sent_at_utc"),
    dict(key="pm5", label="PM5 Reminder", trigger="not implemented", users_field=None, not_implemented=True),
    dict(key="referral_near_miss", label="Referral Near Miss", trigger="event (referral settlement) — code exists, never called",
         col_key="referral_notifications", ts_field="created_at", type_filter={"type": "ref_near_miss"}, unwired=True),
    dict(key="referral_success", label="Referral Success", trigger="event (referral settlement)",
         col_key="referral_notifications", ts_field="created_at", type_filter={"type": "ref_qualified"}),
    dict(key="welcome_unlock", label="Welcome Unlock (VIP1)", trigger="event (check-in progress)",
         users_field="pm_sent.pm_vip_unlocked"),
    # NOTE: "welcome_unlock" above is the monthly-deposit VIP1 tier-unlock PM
    # (onboarding.send_vip_unlock_if_needed / PM4_VIP_UNLOCK_TEXT) — a
    # different feature that happens to share the word "unlock". It is NOT
    # the Welcome Voucher check-in journey's unlock celebration; see
    # "welcome_checkin_unlock" below for that.
    dict(key="welcome_checkin_d2", label="Welcome Check-in D2 Reminder",
         trigger="scheduler (welcome_progress_reminders, hourly) — fires ~20h after Day-1 check-in if still 1/3",
         col_key="welcome_analytics_events", ts_field="created_at",
         type_filter={"event": "welcome_reminder_20h_sent", "stage": "20h"},
         stage="20h", job_key="welcome_progress_reminders", flag_field="welcome_journey"),
    dict(key="welcome_checkin_d2_followup", label="Welcome Check-in D2 Follow-up",
         trigger="scheduler (welcome_progress_reminders, hourly) — fires ~28h after Day-1 check-in if still 1/3",
         col_key="welcome_analytics_events", ts_field="created_at",
         type_filter={"event": "welcome_reminder_28h_sent", "stage": "28h"},
         stage="28h", job_key="welcome_progress_reminders", flag_field="welcome_journey"),
    dict(key="welcome_checkin_d3", label="Welcome Check-in D3 Reminder",
         trigger="scheduler (welcome_progress_reminders, hourly) — fires ~20h after Day-2 check-in if still 2/3 "
                 "(final reminder before the welcome window ends)",
         col_key="welcome_analytics_events", ts_field="created_at",
         type_filter={"event": "welcome_reminder_day2_sent", "stage": "day3"},
         stage="day3", job_key="welcome_progress_reminders", flag_field="welcome_journey"),
    dict(key="welcome_checkin_unlock", label="Welcome Unlock Celebration",
         trigger="event (check-in flow) — fires in the /checkin response when 3/3 check-ins are completed",
         col_key="welcome_analytics_events", ts_field="created_at",
         type_filter={"event": "welcome_completed", "stage": "completed"},
         flag_field="welcome_journey"),
    dict(key="welcome_expiry", label="Welcome Expiry", trigger="scheduler (welcome_voucher_lifecycle, */30min)",
         col_key="welcome_eligibility", ts_field="final_warning_sent_at"),
    dict(key="reactivation", label="Reactivation", trigger="scheduler (reactivation_journey_evaluate, */30min)",
         col_key="reactivation_journey", ts_field="tier1_completed_at"),
    dict(key="tournament_reminder", label="Tournament Reminder", trigger="not implemented",
         users_field=None, not_implemented=True),
    dict(key="affiliate_reward", label="Affiliate Reward (group unlock)", trigger="event (referral settlement)",
         users_field="affiliate_group_unlocked_at"),
]


def _welcome_stage_job_status(
    now: datetime,
    *,
    flag_enabled: bool | None,
    job_last_run: datetime | None,
    job_interval_seconds: float | None,
    sent_today: int | None,
    failed_today: int | None,
) -> tuple[str, str]:
    """Status for a Welcome Check-in reminder-stage row (one that is backed
    by the ``welcome_progress_reminders`` scheduler job). Deliberately does
    NOT report Online just because the job is registered/enabled — Online
    requires evidence of a recent successful *run*, and additionally either
    a send today or an explicit "no eligible users" Waiting state, never a
    guess. Reuses the same status vocabulary as the rest of this module:
    Online=🟢, Waiting=🟡, Degraded/Warning=🟠, Offline=🔴."""
    if flag_enabled is False:
        return WAITING, "feature flag disabled"
    age = _age_seconds(now, job_last_run)
    if age is None:
        return WAITING, "job registered but no run recorded yet"
    stale_threshold = max((job_interval_seconds or 3600) * 3, 900)
    if age > stale_threshold:
        return OFFLINE, f"stale — scheduler last ran {int(age)}s ago (expected every ~{int(job_interval_seconds or 0)}s)"
    if failed_today:
        return WARNING, f"degraded — scheduler ran {int(age)}s ago, {failed_today} send failure(s) today"
    if sent_today:
        return ONLINE, f"scheduler ran {int(age)}s ago, {sent_today} sent today"
    return WAITING, f"scheduler ran {int(age)}s ago, no eligible users for this stage today"


def build_pm_automation(
    collections: Mapping[str, Any],
    feature_flags: Mapping[str, Any],
    referral_config: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    users_col = collections.get("users")
    rows: list[dict[str, Any]] = []
    for pm in PM_REGISTRY:
        if pm.get("not_implemented"):
            rows.append({
                "key": pm["key"], "name": pm["label"], "enabled": False,
                "last_sent": None, "sent_today": 0, "failed_today": None,
                "skipped_today": None, "skip_breakdown": None, "last_run_age_s": None,
                "queue_size": None, "trigger": pm["trigger"],
                "status": OFFLINE, "notes": "no implementing code found anywhere in the codebase",
            })
            continue

        if pm.get("users_field"):
            field = pm["users_field"]
            last_sent = _last_value(collections, "users", field) if users_col is not None else None
            sent_today = _count_today(collections, "users", field, now)
        else:
            col_key = pm["col_key"]
            filt = pm.get("type_filter")
            last_sent = _last_value(collections, col_key, pm["ts_field"], extra_filter=filt)
            sent_today = _count_today(collections, col_key, pm["ts_field"], now, extra_filter=filt)

        # Optional richer counters — only populated for rows whose registry
        # entry declares a normalized "stage" (the Welcome Check-in
        # D2/D2-followup/D3 reminder rows). Failure/skip events are filtered
        # by the persisted ``stage`` field, never by parsing ``event``
        # strings, so a skip that predates stage-eligibility is correctly
        # excluded from a stage's count. Everything else keeps reporting
        # None, same as before, rather than a fabricated 0.
        failed_today = None
        skipped_today = None
        skip_breakdown = None
        stage = pm.get("stage")
        if stage:
            failed_today = _count_today(
                collections, "welcome_analytics_events", "created_at", now,
                extra_filter={"event": "welcome_reminder_failed", "stage": stage},
            )
            skip_breakdown = _skip_reason_breakdown_today(
                collections, "welcome_analytics_events", "welcome_reminder_skipped", now, stage=stage,
            )
            if skip_breakdown is not None:
                skipped_today = sum(skip_breakdown.values())

        # "Last run age" for stage rows must reflect the scheduler job
        # actually running, not just being registered — and must survive
        # the 1h TTL on scheduler_locks (acquire_scheduler_lock), so it
        # prefers the retained admin_cache run-stats doc and only falls
        # back to the (expiring) lock timestamp if that doc doesn't exist
        # yet.
        last_run_age_s = None
        job_last_run = None
        job = None
        if pm.get("job_key"):
            job = next((j for j in SCHEDULER_JOBS if j["key"] == pm["job_key"]), None)
            if job:
                stats_doc = _welcome_run_stats_doc(collections, pm["job_key"])
                job_last_run = _aware(stats_doc.get("lastRunAt"))
                if job_last_run is None:
                    job_last_run = _lookup_lock_ts(collections, job.get("lock_source"))
                last_run_age_s = _age_seconds(now, job_last_run)

        if pm.get("unwired"):
            status, note = unwired_status(last_sent)
        elif job is not None:
            flag_val = (feature_flags or {}).get(pm.get("flag_field"))
            status, note = _welcome_stage_job_status(
                now, flag_enabled=flag_val, job_last_run=job_last_run,
                job_interval_seconds=job.get("interval_seconds"),
                sent_today=sent_today, failed_today=failed_today,
            )
        else:
            flag_field = pm.get("flag_field")
            flag_val = (feature_flags or {}).get(flag_field) if flag_field else None
            lookback = 14 * 86400 if pm["key"] in ("pm0",) else 30 * 86400
            status, note = flag_gated_status(now, flag_enabled=flag_val, evidence_at=last_sent, lookback_seconds=lookback)

        rows.append({
            "key": pm["key"],
            "name": pm["label"],
            "enabled": (feature_flags or {}).get(pm.get("flag_field")) if pm.get("flag_field") else True,
            "last_sent": _fmt_ts(last_sent),
            "sent_today": sent_today,
            "failed_today": failed_today,
            "skipped_today": skipped_today,
            "skip_breakdown": skip_breakdown,
            "last_run_age_s": last_run_age_s,
            "queue_size": None,
            "trigger": pm["trigger"],
            "status": status,
            "notes": note,
        })
    return rows


# ---------------------------------------------------------------------------
# 3) Queue Status
# ---------------------------------------------------------------------------

# Real pending statuses used by the affiliate ledger (dashboard_panels.py:733).
_AFFILIATE_PENDING_STATUSES = ["PENDING_REVIEW", "PENDING_MANUAL", "PENDING_EOM", "SIMULATED_PENDING"]

QUEUE_REGISTRY: list[dict[str, Any]] = [
    dict(key="verification_queue", label="Verification Queue", col_key="tg_verification_queue",
         pending_filter={"status": "queued"}),
    dict(key="voucher_queue", label="Voucher Queue (pending affiliate ledger entries)", col_key="affiliate_ledger",
         pending_filter={"status": {"$in": _AFFILIATE_PENDING_STATUSES}}),
    dict(key="pm_queue", label="PM Queue (onboarding due, not yet sent)", col_key="users",
         pending_filter=None),  # computed specially below using the request-time "now"
    dict(key="reactivation_queue", label="Reactivation Queue (active journeys)", col_key="reactivation_journey",
         pending_filter={"status": "active"}),
    dict(key="affiliate_queue", label="Affiliate Queue (pending settlements)", col_key="affiliate_ledger",
         pending_filter={"status": {"$in": _AFFILIATE_PENDING_STATUSES}}),
]


def build_queue_status(collections: Mapping[str, Any], now: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for q in QUEUE_REGISTRY:
        col = collections.get(q["col_key"])
        if col is None:
            rows.append({"key": q["key"], "name": q["label"], "size": None, "status": WAITING,
                         "notes": "collection not available in this environment"})
            continue

        def _q(col=col, key=q["key"], pending_filter=q["pending_filter"]):
            if key == "pm_queue":
                return int(col.count_documents({
                    "$or": [
                        {"pm1_due_at_utc": {"$lte": now}, "pm1_sent_at_utc": {"$exists": False}},
                        {"pm2_due_at_utc": {"$lte": now}, "pm2_sent_at_utc": {"$exists": False}},
                        {"pm3_due_at_utc": {"$lte": now}, "pm3_sent_at_utc": {"$exists": False}},
                        {"pm4_due_at_utc": {"$lte": now}, "pm4_sent_at_utc": {"$exists": False}},
                    ],
                }))
            return int(col.count_documents(pending_filter))

        size = _safe_get(_q)
        if size is None:
            status, notes = WAITING, "could not read queue size"
        elif size == 0:
            status, notes = ONLINE, "empty"
        else:
            status, notes = ONLINE, f"{size} item(s) pending"
        rows.append({"key": q["key"], "name": q["label"], "size": size, "status": status, "notes": notes})
    return rows


# ---------------------------------------------------------------------------
# 4) Worker Health
# ---------------------------------------------------------------------------

def build_worker_health(
    collections: Mapping[str, Any],
    now: datetime,
    *,
    mongo_ping: Callable[[], bool] | None = None,
    telegram_get_me: Callable[[], bool] | None = None,
    deployment_version: str | None = None,
    git_commit: str | None = None,
) -> dict[str, Any]:
    mongo_ok = _safe_get(mongo_ping, default=False) if mongo_ping else None
    telegram_ok = _safe_get(telegram_get_me, default=False) if telegram_get_me else None

    tick_ts = _lookup_lock_ts(collections, ("scheduler_locks", "tick_5min", "updatedAt"))
    tick_age = _age_seconds(now, tick_ts)
    scheduler_running = (tick_age is not None and tick_age <= 900)

    heartbeat_ts = _safe_get(lambda: (collections.get("admin_cache").find_one({"_id": "snapshot_heartbeat"}) or {}).get("ts_utc")) \
        if collections.get("admin_cache") is not None else None
    heartbeat_age = _age_seconds(now, heartbeat_ts)

    return {
        "worker_running": scheduler_running,
        "scheduler_running": scheduler_running,
        "scheduler_last_tick": _fmt_ts(tick_ts),
        "mongo_connected": mongo_ok,
        "telegram_connected": telegram_ok,
        "snapshot_freshness_seconds": heartbeat_age,
        "snapshot_last_heartbeat": _fmt_ts(heartbeat_ts),
        "deployment_version": deployment_version,
        "git_commit": git_commit,
        "last_heartbeat": _fmt_ts(heartbeat_ts),
        "checked_at": _fmt_ts(now),
    }


# ---------------------------------------------------------------------------
# 5) Feature Overview (top-level rollup table)
# ---------------------------------------------------------------------------

def _worst_status(rows: list[dict[str, Any]], key: str = "status") -> str:
    order = [OFFLINE, WARNING, WAITING, DEPRECATED, ONLINE]
    present = {r.get(key) for r in rows if r.get(key)}
    for s in order:
        if s in present:
            return s
    return WAITING


def build_feature_overview(
    scheduler_rows: list[dict[str, Any]],
    pm_rows: list[dict[str, Any]],
    queue_rows: list[dict[str, Any]],
    worker_health: dict[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    by_key = {r["key"]: r for r in scheduler_rows}
    pm_by_key = {r["key"]: r for r in pm_rows}

    def sched(key: str) -> dict[str, Any]:
        return by_key.get(key, {})

    def pm(key: str) -> dict[str, Any]:
        return pm_by_key.get(key, {})

    rows: list[dict[str, Any]] = []

    def add(name: str, trigger: str, status: str, last_run, next_run, last_success, notes: str):
        rows.append({
            "feature": name, "status": status, "trigger": trigger,
            "last_run": last_run, "next_run": next_run,
            "last_success": last_success, "notes": notes,
        })

    onboarding_status = _worst_status([pm("pm0"), pm("pm1"), pm("pm2"), pm("pm3"), pm("pm4")])
    add("Onboarding / PM Automation (PM0-PM4)", "event + scheduler (onboarding_due_tick)",
        onboarding_status, pm("pm1").get("last_sent"), None, pm("pm1").get("last_sent"),
        "PM5 does not exist in the codebase")

    wj = sched("welcome_progress_reminders")
    add("Welcome Journey", "scheduler (*/30min & hourly)", wj.get("status", WAITING),
        wj.get("last_run"), wj.get("next_run"), wj.get("last_run"), wj.get("notes", ""))

    wr = sched("welcome_voucher_lifecycle")
    add("Welcome Reward / Voucher", "scheduler (*/30min)", wr.get("status", WAITING),
        wr.get("last_run"), wr.get("next_run"), wr.get("last_run"), wr.get("notes", ""))

    vq = sched("process_verification_queue")
    add("Verification Queue", "scheduler (*/2min)", vq.get("status", WAITING),
        vq.get("last_run"), vq.get("next_run"), vq.get("last_run"), vq.get("notes", ""))

    tick = sched("tick_5min")
    add("Referral Snapshot / Settlement", "scheduler (tick_5min, */5min)", tick.get("status", WAITING),
        tick.get("last_run"), tick.get("next_run"), tick.get("last_run"), tick.get("notes", ""))

    aff_snap = sched("affiliate_current_week_issue")
    add("Affiliate Snapshot / Leaderboard", "scheduler (*/30min)", aff_snap.get("status", WAITING),
        aff_snap.get("last_run"), aff_snap.get("next_run"), aff_snap.get("last_run"), aff_snap.get("notes", ""))

    aff_month = sched("affiliate_monthly_settle")
    add("Affiliate Monthly Rewards", "scheduler (day=1 00:10 KL)", aff_month.get("status", WAITING),
        aff_month.get("last_run"), aff_month.get("next_run"), aff_month.get("last_run"), aff_month.get("notes", ""))

    react = sched("reactivation_journey_evaluate")
    add("Reactivation Campaign", "scheduler (*/30min)", react.get("status", WAITING),
        react.get("last_run"), react.get("next_run"), react.get("last_run"), react.get("notes", ""))

    nm = pm("referral_near_miss")
    add("Referral Near Miss DM", "event (referral settlement) — unwired", nm.get("status", OFFLINE),
        nm.get("last_sent"), None, nm.get("last_sent"), nm.get("notes", ""))

    add("Tournament (banner/page/API/countdown/leaderboard)", "n/a — not implemented", OFFLINE,
        None, None, None, "feature flag exists (default off) but no implementing code, routes, or templates were found")

    seg = sched("bot_segment_sheet_sync")
    add("Segment Dashboard / Bot Segment Sync", "scheduler (wed 09:30 KL)", seg.get("status", WAITING),
        seg.get("last_run"), seg.get("next_run"), seg.get("last_run"), seg.get("notes", ""))

    vip = sched("monthly_vip")
    add("VIP Tier Assignment", "scheduler (day=1 00:00 KL)", vip.get("status", WAITING),
        vip.get("last_run"), vip.get("next_run"), vip.get("last_run"), vip.get("notes", "") + "; not lock-guarded — no multi-instance protection")

    mongo_status = ONLINE if worker_health.get("mongo_connected") else OFFLINE
    add("MongoDB", "n/a (connection check)", mongo_status, None, None, None, "live ping at request time")

    tg_status = ONLINE if worker_health.get("telegram_connected") else (WAITING if worker_health.get("telegram_connected") is None else OFFLINE)
    add("Telegram Bot (long polling, worker process)", "n/a (getMe check)", tg_status,
        worker_health.get("scheduler_last_tick"), None, worker_health.get("scheduler_last_tick"),
        "runs via long-polling in a separate worker dyno; the web dyno cannot observe the poll loop directly, so this infers liveness from the shared tick_5min lock")

    return rows


# ---------------------------------------------------------------------------
# 6) Welcome Journey Runtime (observability only — reuses SCHEDULER_JOBS for
#    the heartbeat/status verdict; never re-derives or alters reminder/
#    eligibility logic). Run history/stats are read from ``admin_cache``
#    (doc "welcome_run_stats:<job>"), NOT ``scheduler_locks`` — the latter has
#    a TTL index on ``expireAt`` (see main.py's create_index(...,
#    expireAfterSeconds=0)) and would silently delete this history the moment
#    a job stops running for longer than its lock TTL, which is exactly the
#    failure this dashboard needs to keep showing.
# ---------------------------------------------------------------------------

_WELCOME_JOBS_BY_KEY = {"reminders": "welcome_progress_reminders", "lifecycle": "welcome_voucher_lifecycle"}


def _welcome_scheduler_row(scheduler_rows: list[dict[str, Any]], job_key: str) -> dict[str, Any]:
    for row in scheduler_rows:
        if row.get("key") == job_key:
            return row
    return {}


def _welcome_run_stats_doc(collections: Mapping[str, Any], job_key: str) -> dict[str, Any]:
    col = collections.get("admin_cache")
    if col is None:
        return {}
    return _safe_get(lambda: col.find_one({"_id": f"welcome_run_stats:{job_key}"})) or {}


def build_welcome_journey_scheduler(
    collections: Mapping[str, Any],
    scheduler_rows: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    """Scheduler-health block for the Welcome Journey Runtime card. Reuses the
    same ``job_runtime_status`` verdicts already computed in
    ``build_scheduler_health`` — this never re-derives Online/Offline."""
    out: dict[str, Any] = {}
    for ui_key, job_key in _WELCOME_JOBS_BY_KEY.items():
        row = _welcome_scheduler_row(scheduler_rows, job_key)
        job = next((j for j in SCHEDULER_JOBS if j["key"] == job_key), {})
        last_run = _aware(_lookup_lock_ts(collections, job.get("lock_source")))
        interval = job.get("interval_seconds")
        next_run = last_run + timedelta(seconds=interval) if (last_run and interval) else None
        stats_doc = _welcome_run_stats_doc(collections, job_key)
        out[ui_key] = {
            "job_name": job.get("label"),
            "status": row.get("status", WAITING),
            "notes": row.get("notes"),
            "cron": job.get("cron"),
            "last_run": _fmt_ts(last_run),
            "next_run": _fmt_ts(next_run),
            "last_run_duration_s": stats_doc.get("lastRunDurationS"),
        }
    return out


def build_welcome_journey_last_run(collections: Mapping[str, Any]) -> dict[str, Any]:
    """Latest ``process_welcome_reminders`` run stats, persisted onto
    ``admin_cache`` (doc "welcome_run_stats:welcome_progress_reminders") by
    ``main._record_welcome_run_stats``. Returns an empty/zeroed shape (not an
    error) if no run has landed yet."""
    doc = _welcome_run_stats_doc(collections, "welcome_progress_reminders")
    stats = doc.get("lastRunStats") or {}
    skip = stats.get("skip_breakdown") or {}
    return {
        "at": _fmt_ts(doc.get("lastRunAt")),
        "duration_s": doc.get("lastRunDurationS"),
        "users_scanned": stats.get("scanned", 0),
        "eligible_20h": stats.get("eligible_20h", 0),
        "eligible_28h": stats.get("eligible_28h", 0),
        "eligible_day3": stats.get("eligible_day3", 0),
        "reminders_20h_sent": stats.get("reminder_20h_sent", 0),
        "reminders_28h_sent": stats.get("reminder_28h_sent", 0),
        "day3_reminders_sent": stats.get("day2_reminder_sent", 0),
        "telegram_failed": stats.get("send_failed", 0),
        "blocked_users": stats.get("blocked_users", 0),
        "skipped_users": {
            "total": stats.get("skipped_abuse", 0),
            "already_claimed": skip.get("already_claimed", 0),
            "expired": skip.get("expired", 0),
            "already_sent": skip.get("already_sent", 0),
            "risk_blocked": skip.get("risk_blocked", 0),
            "multi_account": skip.get("multi_account", 0),
            "left_channel": skip.get("left_channel", 0),
            "bot_blocked": skip.get("bot_blocked", 0),
            "missing_data": skip.get("missing_data", 0),
        },
    }


def build_welcome_journey_recent_runs(collections: Mapping[str, Any], limit: int = 20) -> list[dict[str, Any]]:
    """Last N reminder-job runs, latest first. Sourced from the capped
    ``recentRuns`` array maintained on the ``admin_cache`` doc (not
    ``scheduler_locks`` — see the module-level note above on its TTL index).
    No new collection, no extra scans."""
    doc = _welcome_run_stats_doc(collections, "welcome_progress_reminders")
    runs = doc.get("recentRuns") or []
    rows = []
    for run in reversed(runs[-limit:]):
        stats = run.get("stats") or {}
        rows.append({
            "time": _fmt_ts(run.get("at")),
            "users_scanned": stats.get("scanned", 0),
            "sent_20h": stats.get("reminder_20h_sent", 0),
            "sent_28h": stats.get("reminder_28h_sent", 0),
            "sent_day3": stats.get("day2_reminder_sent", 0),
            "failed": stats.get("send_failed", 0),
            "duration_s": run.get("duration_s"),
        })
    return rows


def build_welcome_journey_alerts(
    now: datetime,
    *,
    scheduler: dict[str, Any],
    last_run: dict[str, Any],
    funnel_summary: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    """Automatic warnings for the Welcome Journey Runtime card. Purely a read
    of already-computed scheduler/stats/funnel values — adds no new queries
    and never changes reminder timing, eligibility or voucher rules."""
    alerts: list[dict[str, str]] = []
    reminders = scheduler.get("reminders", {})

    last_run_at = None
    try:
        if reminders.get("last_run"):
            last_run_at = datetime.fromisoformat(reminders["last_run"])
    except Exception:
        last_run_at = None
    age_h = _age_seconds(now, last_run_at)
    if age_h is not None and age_h > 2 * 3600:
        alerts.append({"level": "critical", "message": f"Scheduler has not run for {int(age_h // 3600)} hour(s)."})
    if reminders.get("status") == OFFLINE:
        alerts.append({"level": "critical", "message": "Scheduler failed."})

    eligible_total = (last_run.get("eligible_20h", 0) + last_run.get("eligible_28h", 0) + last_run.get("eligible_day3", 0))
    sent_total = (last_run.get("reminders_20h_sent", 0) + last_run.get("reminders_28h_sent", 0) + last_run.get("day3_reminders_sent", 0))
    if eligible_total > 0 and sent_total == 0:
        alerts.append({"level": "warning", "message": "Eligible users > 0 but reminders sent = 0."})

    failed = last_run.get("telegram_failed", 0)
    attempted = sent_total + failed
    if attempted > 0 and (failed / attempted) > 0.05:
        alerts.append({"level": "warning", "message": f"Telegram failure rate {round(failed / attempted * 100, 1)}% (> 5%)."})

    if last_run.get("blocked_users", 0) > max(5, int(0.1 * max(last_run.get("users_scanned", 0), 1))):
        alerts.append({"level": "warning", "message": "Blocked users unusually high."})

    if funnel_summary:
        d2_rate = (funnel_summary.get("welcome_d2_rate_pct") or {}).get("value")
        if isinstance(d2_rate, (int, float)) and d2_rate < 40:
            alerts.append({"level": "warning", "message": f"Day 2 completion rate is low ({d2_rate}%)."})
        claim_rate = (funnel_summary.get("welcome_claim_rate_pct") or {}).get("value")
        if isinstance(claim_rate, (int, float)) and claim_rate < 50:
            alerts.append({"level": "warning", "message": f"Welcome claim conversion below threshold ({claim_rate}%)."})

    return alerts
