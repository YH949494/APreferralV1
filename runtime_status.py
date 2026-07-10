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
    dict(key="welcome_expiry", label="Welcome Expiry", trigger="scheduler (welcome_voucher_lifecycle, */30min)",
         col_key="welcome_eligibility", ts_field="final_warning_sent_at"),
    dict(key="reactivation", label="Reactivation", trigger="scheduler (reactivation_journey_evaluate, */30min)",
         col_key="reactivation_journey", ts_field="tier1_completed_at"),
    dict(key="tournament_reminder", label="Tournament Reminder", trigger="not implemented",
         users_field=None, not_implemented=True),
    dict(key="affiliate_reward", label="Affiliate Reward (group unlock)", trigger="event (referral settlement)",
         users_field="affiliate_group_unlocked_at"),
]


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

        if pm.get("unwired"):
            status, note = unwired_status(last_sent)
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
            "failed_today": None,  # no failure-tracking field exists in the codebase today
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
