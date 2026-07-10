# Runtime Feature Health Audit — AP Referral Bot

Audit date: 2026-07-10. Scope: full codebase trace of every major feature,
PM, scheduled job, and background worker, classifying each by **what is
actually executing in production today**, not by whether code/config exists.

Legend: 🟢 Online · 🟡 Waiting · 🟠 Warning/Misconfigured · 🔴 Offline · ⚫ Deprecated

---

## 1. Runtime architecture

```
                 ┌───────────────────────────┐
                 │        fly.toml           │
                 │  two process groups        │
                 └───────────┬───────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                            │
 ┌──────▼───────┐                          ┌─────────▼─────────┐
 │  web process  │                          │  worker process    │
 │ gunicorn      │                          │ RUNNER_MODE=worker  │
 │ RUNNER_MODE=  │                          │ python main.py      │
 │   web         │                          └─────────┬─────────┘
 └──────┬───────┘                                      │
        │ Flask routes                        ┌────────▼────────┐
        │ /api/admin/*                         │ app_bot.run_    │
        │ /miniapp API                         │ polling()       │
        │ admin dashboard (static SPA)          │ (long-polling,  │
        │                                       │  NOT webhook)   │
        │                                       └────────┬────────┘
        │                                                 │
        │                                       ┌─────────▼─────────┐
        │                                       │ APScheduler         │
        │                                       │ BackgroundScheduler │
        │                                       │ (all cron jobs live │
        │                                       │  here — see §4)     │
        │                                       └─────────┬─────────┘
        │                                                 │
        └───────────────────┬─────────────────────────────┘
                             │
                    ┌────────▼─────────┐
                    │     MongoDB       │
                    │ users, vouchers,  │
                    │ scheduler_locks,  │
                    │ referral_*, etc.  │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Telegram Bot API │
                    └───────────────────┘
```

Key structural fact confirmed by the audit: **all scheduled jobs and the
Telegram polling loop run only in the `worker` process** (`fly.toml`
`worker` group, `RUNNER_MODE=worker`, `main.py:7995-7996`). The `web`
process serves the Admin Dashboard/API and the Mini App only — it never
runs a scheduler tick and cannot see the polling loop directly. This is why
the new Runtime Status page infers scheduler/bot liveness from **shared
Mongo state** (`scheduler_locks`, `admin_cache.snapshot_heartbeat`) rather
than from an in-process flag, and why "Next Run" for jobs is not shown
(APScheduler's live schedule state exists only inside the worker process
and isn't persisted anywhere queryable).

No webhook is configured anywhere in the codebase — the bot is long-polling
only (`main.py:7951`, `run_polling(...)`).

---

## 2. Feature Overview

| Feature | Status | Trigger | Notes |
|---|---|---|---|
| Onboarding PM0 (`/start`) | 🟢 Online | event | Fires on every `/start`, no gating |
| PM1 (check-in tip) | 🟠 Warning | scheduled 1min + due-tick sweep | **Bug**: prod/test delay branches are identical (`onboarding.py:573`) — always ~1 min, likely meant to be hours in prod |
| PM2 (MyWin tip) | 🟢 Online | scheduled 24h + due-tick sweep | Correct delay branching |
| PM3 (referral tip) | 🟢 Online | scheduled 48h + due-tick sweep | |
| PM4 (72h re-engage) | 🟢 Online | scheduled 72h + due-tick sweep | |
| PM5 | ⚫ N/A | — | **Does not exist anywhere in the codebase** |
| PM1-4 reachability | 🟠 Warning | — | Only scheduled via `record_onboarding_start()`, called solely from the Mini App "visible" telemetry endpoint (`vouchers.py:5189`) — users who only send `/start` and never open the Mini App never enter the PM1-4 chain |
| Welcome Journey (progress reminders) | 🟢 Online | scheduler `*/30min` + hourly, flag `welcome_journey`/`welcome_reward` | |
| Region Selection | 🟡 Waiting | event | Feature flag `region_selection` defaults **False** — off in production unless toggled |
| Profile Photo Verification | 🟢 Online | event, feeds verification queue | |
| Welcome Reward / Voucher lifecycle | 🟢 Online | scheduler `*/30min` | |
| Welcome Claim | 🟢 Online | event (user tap) | |
| Referral Link generation | 🟢 Online | event | |
| Referral Qualification / Hold Period | 🟢 Online | scheduler `tick_5min` `*/5min` | |
| Referral Snapshot | 🟠 Warning | scheduler (via `tick_5min`) | Its **dedicated** settings-driven cron entry (`referral_snapshot` in the `scheduler` settings group) has no matching `scheduler.add_job` id — that specific cron control is dead/unreachable, even though the snapshot itself runs via `tick_5min` |
| Referral Leaderboard | 🟢 Online | event + cache | Flag `leaderboard` |
| Referral Rewards / XP | 🟢 Online | event (settlement) | |
| **Referral Near Miss DM** | 🔴 Offline | code exists, **never called** | `_maybe_send_near_miss_dm` (`main.py:1183`) and its wrapper `_maybe_send_near_miss_dm_web` (`scheduler.py:1102`) are fully implemented but have **zero callers** anywhere in the codebase. Also ignores its own configured cooldown setting (`near_miss_dm_cooldown_hours`) in favor of a hardcoded 24h. Dead code path. |
| Affiliate Qualification/Snapshot | 🟢 Online | event + scheduler `*/30min` | |
| Affiliate Weekly Leaderboard | 🟢 Online | scheduler (mon 00:05/00:15) | |
| Affiliate Monthly Rewards | 🟢 Online | scheduler (day=1 00:10 KL) | |
| Affiliate Pool Assignment | 🟠 Warning | event | Silently no-ops when pool inventory is missing (`_mark_missing_pool_config`) — no alerting |
| Affiliate reward / group-unlock DM | 🟢 Online | event (settlement) | |
| "Affiliate XP" | ⚫ N/A | — | Not a distinct system — affiliate rewards are voucher/cash-based, not XP. Likely a naming overlap with referral XP earned by affiliate-tier users |
| Daily Check-in / Streak Bonus / XP Grant | 🟢 Online | event | |
| Voucher Drops (Public/Personalised/Pooled) | 🟢 Online | event + `drop_status_reconcile` `*/1min` | |
| VIP Voucher | 🟠 Warning | scheduler `monthly_vip` (day=1 00:00 KL) | Not a distinct system — a tier-gated drop. The tier-assignment job itself has **no lock and no last-run tracking field**, so it's the one monthly job with no multi-instance protection and no dashboard-visible evidence beyond a single `audit_events` doc |
| Claim Flow | 🟢 Online | event | |
| Verification Queue | 🟢 Online | scheduler `*/2min` | |
| Voucher Cleanup | 🟡 Waiting (passive) | Mongo TTL indexes only | No recurring job exists; relies entirely on TTL indexes — not an observable "job", surfaced as queue-size instead |
| Voucher Expiry | 🟢 Online | read-time check + `*/1min` reconcile | |
| Rejoin Buffer | 🟢 Online | event | |
| Reactivation Campaign | 🟢 Online | scheduler `*/30min`, flag `reactivation` | |
| **Tournament** (Banner/Page/API/Countdown/Leaderboard) | 🔴 Offline | n/a | **Not implemented.** Only a feature flag (`tournament`, default False) and a URL setting placeholder exist. No routes, templates, or logic anywhere in the repo. |
| Community Join/Leave Tracking | 🟢 Online | Telegram `chat_member`/`my_chat_member`/`new_chat_members` events | Wired via `ChatMemberHandler`/`MessageHandler`, in `allowed_updates` |
| Channel/Chat Verification | 🟢 Online | event + cache | |
| Admin Dashboard (SPA) | 🟢 Online | — | |
| Segment Dashboard / Bot Segment Sync | 🟢 Online | scheduler (wed 09:30 KL) | Only wiring point for `bot_segment_sync.py` |
| Feature Flags / Settings | 🟢 Online | admin-editable, 45s cache | |
| Validation panel (UIM vs Backend) | 🟢 Online | on-demand | |
| MongoDB | derived live | ping at request time | |
| Telegram Bot (long-polling) | derived live | `getMe`, cached 60s | Web dyno cannot see the poll loop directly — inferred via `tick_5min` lock freshness |

---

## 3. Features confirmed actually 🟢 Online in production

Onboarding PM0, PM2, PM3, PM4 · Welcome Journey (reminders, reward lifecycle,
claim) · Profile Photo Verification · Referral link/qualification/hold/
leaderboard/rewards · Affiliate qualification/snapshot/weekly leaderboard/
monthly rewards/group-unlock DM · Daily check-in/streak/XP · Voucher drops
(public, personalised, pooled)/claim flow/verification queue/expiry/rejoin
buffer · Reactivation campaign · Community join/leave/verification ·
Admin Dashboard · Segment dashboard/bot segment sync · Settings/Feature
Flags · Validation panel.

## 4. Implemented but never executed (dead/unreachable code paths)

- **Referral Near Miss DM** (`main.py:1183`, `scheduler.py:1102`) — fully coded, zero callers.
- **`referral_snapshot` dedicated cron setting** — schema entry with no matching scheduled job id; the snapshot runs via `tick_5min` instead, making the dedicated cron control dead.
- **Pooled voucher reconciliation** (`vouchers.py:4716 reconcile_pooled_remaining`) — no scheduler wiring found; admin-triggered only, drift can accumulate unnoticed.
- **PM1-4 chain for `/start`-only users** — never enters scheduling because it's gated behind a Mini App telemetry endpoint, not the `/start` handler.
- **Tournament** feature flag — toggled but backed by zero implementation.
- **Region Selection** — implemented, but the feature flag defaults off.

## 5. Dead / manual-only code (no scheduler or caller wiring at all)

- `cleanup_stale_bse_snapshots.py` — argparse CLI, no imports elsewhere.
- `claim_risk_sync.py` — referenced only by its own test file.
- `monthly_xp_report.py` — argparse CLI, manual only.
- `sync_referral_counts.py` — docstring states manual-run only, no callers.
- `bot_joins.py` — orphaned alternate entry point with its own `run_polling`; superseded by the handlers registered directly in `main.py`.

## 6. Scheduler jobs (all live only in the `worker` process)

| Job | Cron | Lock/heartbeat source |
|---|---|---|
| weekly_reset (XP Snapshot) | mon 00:00 KL | none found — recommend adding |
| monthly_vip | day=1 00:00 KL | `audit_events["monthly_job:last_run"]` (no lock) |
| affiliate_monthly_settle | day=1 00:10 KL | `scheduler_locks["affiliate_monthly_settle"]` |
| affiliate_weekly_settle | mon 00:15 KL | `scheduler_locks["affiliate_weekly_settle"]` |
| affiliate_current_week_issue | */30min | `scheduler_locks["affiliate_current_week_issue"]` |
| tick_5min (Retention KPIs, Referral/XP Snapshot Settlement, Affiliate Snapshot) | */5min | `scheduler_locks["tick_5min"]` |
| process_verification_queue | */2min | `scheduler_locks["verification_queue"]` |
| onboarding_due_tick | */1min | none |
| welcome_voucher_lifecycle | */30min | `scheduler_locks["welcome_voucher_lifecycle"]` (flag `welcome_reward`) |
| welcome_progress_reminders | hourly | `scheduler_locks["welcome_progress_reminders"]` (flag `welcome_journey`) |
| reactivation_journey_evaluate | */30min | none (flag `reactivation`) |
| drop_status_reconcile | */1min | none |
| batch_release_tick | */1min | none |
| affiliate_simulate_daily | 01:15 daily | none |
| affiliate_dashboard_monthly_export | day=1 08:00 | none |
| affiliate_daily_kpi | 00:20 UTC | none |
| affiliate_weekly_kpi | mon 00:05 UTC | none |
| bot_segment_sheet_sync | wed 09:30 KL | `scheduler_locks["bot_segment_sheet_sync"]` |
| growth_leaderboard_weekly | configurable | none (flag `growth_leaderboard`) |
| telegram_member_counts_refresh | interval, default 60m | none |
| autoscale_web_for_drop | interval, default 30s | none |
| settings_scheduler_sync | every 1min | none (meta-job) |

## 7. PM journeys

PM0 · PM1 · PM2 · PM3 · PM4 · (PM5 does not exist) · Referral Near Miss
(unwired) · Referral Success · Welcome Unlock (VIP1) · Welcome Expiry ·
Reactivation · Tournament Reminder (not implemented) · Affiliate Reward.

## 8. Implementation delivered (Phase 2)

New module **`runtime_status.py`** — pure, injectable, unit-tested builders
(mirrors `dashboard_panels.py`'s design) that compute every status shown
below **at request time** from live Mongo/settings state — nothing is
hardcoded:

- `build_scheduler_health()` — reads `scheduler` settings group + `scheduler_locks`/`admin_cache`/`audit_events` timestamps.
- `build_pm_automation()` — reads `feature_flags`/`referral_config` + last-sent evidence per PM/journey (`users` fields, `referral_notifications`, `welcome_eligibility`, `reactivation_journey`).
- `build_queue_status()` — live `count_documents` against `tg_verification_queue`, `affiliate_ledger`, `reactivation_journey`, `users` (PM due-but-unsent).
- `build_worker_health()` — Mongo ping, cached Telegram `getMe`, `tick_5min` lock freshness as a scheduler/worker liveness proxy, deployment version (`MINIAPP_VERSION`), git commit (`GITHUB_SHA`/`FLY_IMAGE_REF`), snapshot heartbeat freshness.
- `build_feature_overview()` — rolls the above into the top-level feature table.

New route: `GET /api/admin/dashboard/runtime-status` (`main.py`, guarded by
the existing `require_admin_from_query()`, cached via the existing
`_panel_cached` helper — same pattern as every other dashboard panel).

New standalone admin page: `static/runtime-status.html` +
`static/runtime-status.js`, following the existing `funnel-dashboard.html`
convention (self-contained page using `admin-dashboard.css`, same-origin
cookie auth, redirect to login on 401). Linked from the main Admin
Dashboard sidebar as "Runtime Status (standalone)".

No existing business logic, scheduler behavior, or bot flows were changed.

## 9. Safe production patch — files touched

- `runtime_status.py` (new)
- `test_runtime_status.py` (new, 25 tests)
- `main.py` (additive: one new route + 3 small helper functions, no existing route/logic modified)
- `static/runtime-status.html` (new)
- `static/runtime-status.js` (new)
- `static/admin-dashboard.html` (additive: one new sidebar link)
- `docs/RUNTIME_STATUS_AUDIT.md` (this file)

## 10. Testing checklist

- [x] `job_runtime_status`: disabled flag → Waiting; never run → Waiting; fresh run → Online; stale run → Warning; naive timestamps handled as UTC.
- [x] `flag_gated_status`: flag off → Waiting; flag on + no evidence → Waiting; flag on + recent evidence → Online; flag on + stale evidence → Warning.
- [x] `unwired_status`: no evidence → Offline; unexpected evidence → Warning (self-correcting if wiring is fixed later).
- [x] Scheduler health: online job with fresh lock; disabled job reports Waiting; manual-only scripts report Deprecated; missing collections degrade to Waiting instead of raising.
- [x] PM automation: PM5 and Tournament Reminder report Offline (not implemented); PM0 reports Online given a recent send; Referral Near Miss reports Offline with an explanatory note.
- [x] Queue status: empty queue → Online (0 pending); pending items counted correctly; missing collection reports unknown/Waiting rather than crashing.
- [x] Worker health: all checks reported when probes succeed; a failing probe reports `False`/absent rather than propagating an exception.
- [x] Feature overview: produces a row for every major area, including structurally-dead ones (Tournament → Offline).
- [ ] Manual: load `/static/runtime-status.html` in a staging admin session and confirm the 5 sections render against live Mongo data (requires a deployed environment; not exercised in this sandbox since `main.py` has network-dependent import-time side effects that this repo's own test suite avoids as well).
