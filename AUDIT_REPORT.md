# APReferralV1 — Production Readiness Audit

**Scope:** full repository (`/home/user/APreferralV1`) — Telegram bot (python-telegram-bot 20.8, long-polling), Flask/gunicorn backend, Telegram Mini App frontend, MongoDB, APScheduler background jobs, Fly.io deployment (`web` + `worker` process groups).
**Method:** static/read-only inspection — full-repo grep/AST sweep, dependency install + `ruff`/`bandit`/`pip-audit` runs, real `pytest` execution against `fake_mongo`/`mongomock`, and six parallel deep-dive traces (architecture, onboarding/welcome, referral, XP/voucher, auth/security, scheduler/Mongo integrity), all evidence cross-checked against file:line citations. No production code or data was modified.

---

## 1. Executive Summary

**Overall production risk: HIGH**, driven by one clearly exploitable authentication gap (Critical) and a cluster of Medium/High reward-integrity and operational-reliability gaps. The core settlement/idempotency engineering (referral ledger, XP ledger, voucher pool allocation) is **unusually well designed** — atomic claims, unique-index-backed idempotency, explicit stuck-record recovery — but it sits behind a security layer with a real hole, and next to several newer subsystems (welcome voucher v2, legacy job registrations) that have not been fully reconciled with the rest of the system.

**Findings by severity:** Critical: 1 · High: 6 · Medium: 14 · Low: 11 (32 total, detailed in §3)

**Top five risks (see §3 for full evidence):**
1. **[SEC-01] Unauthenticated user-ID substitution on 7+ endpoints**, including one (`/api/welcome-progress/<user_id>`) that can be used to read and even *trigger issuance of* another user's real voucher code, with zero auth and wildcard CORS amplifying the blast radius to any third-party web page.
2. **[WELCOME-01] Two parallel welcome-reward inventories** (the new `affiliate_ledger`/`voucher_pools` v2 ledger vs. the legacy `db.drops`/`db.vouchers` claim system the frontend actually uses to reveal a code) are not cross-linked — a user can be marked `ISSUED` server-side with no way to ever see their code if the legacy campaign card isn't also kept alive.
3. **[WELCOME-02] Welcome vouchers that hit `OUT_OF_STOCK` are permanently stuck** — no retry/backfill path exists to reissue after restock, unlike the equivalent (and correctly self-healing) `PENDING_MANUAL` path used by affiliate T1–T4 tiers.
4. **[SCHED-01] Scheduler locks are never explicitly released**, and the lock TTL for `tick_5min` (900s) is already 3× its own cron interval (`*/5`, 300s) — meaning under **normal, healthy operation** a fast run at minute 0 holds the lock until minute 15, silently no-opping the minute-5 and minute-10 ticks and throttling referral/XP snapshot freshness to ~15 minutes instead of the intended 5. Under additional Telegram API degradation, worst-case runtime (~33 min) can exceed even that, opening a real double-execution window.
5. **[REF-01] A referral notification helper (`_maybe_send_referral_join_ack_dm`) references an unimported name (`pm_allowed`)** — currently masked as a no-op by a second, independently-wrong guard, meaning the feature has never actually sent a DM in production, and fixing the obviously-wrong guard without also fixing the missing import will immediately start raising `NameError` inside the referral-join critical section, incorrectly revoking a just-created invitee lock on every referral.

**Is reward integrity trustworthy?** *Mostly, with two carve-outs.* Referral settlement, XP grants, and voucher-pool/affiliate-tier allocation are all backed by atomic MongoDB CAS operations and unique-index idempotency (no read-then-write races found in the core paths). The two exceptions are the welcome-voucher inventory split (**WELCOME-01/02**) and a metadata-only rate-limiter quirk (**REF-04**) — neither corrupts core counters, but WELCOME-01/02 can silently deny/delay a real reward.

**Are referral counts trustworthy?** *Yes, with known and self-documented historical scar tissue.* The current settlement pipeline (idempotent claim → atomic award → clamped snapshot rebuild) is correct and was clearly hardened after a real historical bug (see `repair_referral_ledger.py`'s own docstring). One live maintenance script (`sync_referral_counts.py`) lacks the same negative-count clamp the main pipeline has (**MONGO-05**), so it should not be run against ledger data that hasn't first been repaired.

**Is authentication trustworthy?** *No — the Mini App initData HMAC verification itself is correct and unforgeable, but it is simply not called at all on several endpoints.* This is the single most urgent finding in this audit.

**Are scheduled jobs safe under duplicate execution?** *Mostly.* The distributed Mongo-lock primitive is atomic and well-used across ~8 jobs; per-item idempotency (award keys, dedup keys, claim states) provides defense-in-depth even if a lock were ever bypassed. The gaps are lock-TTL sizing under worst-case Telegram latency (**SCHED-01**) and one dormant-but-live legacy job with no crash recovery (**SCHED-02**).

---

## 2. Architecture Map

### 2.1 Processes and entry points

| Process (fly.toml) | Command | What it does |
|---|---|---|
| `web` | `gunicorn main:app --workers 2 --worker-class gthread --threads 4 ...` | Serves Flask routes/blueprints only. Imports `main.py` as a module — `if __name__=="__main__"` never runs, so `run_web()`/`run_worker()` are never invoked here. |
| `worker` | `env RUNNER_MODE=worker python main.py` | Runs `main.py` as `__main__` → `run_worker()`: builds the PTB `Application`, registers **all** Telegram handlers, starts `app_bot.run_polling(...)` (long polling — no webhook anywhere in the repo), and starts the single `APScheduler BackgroundScheduler` (`job_defaults={"max_instances":1}`, no custom executor → default 10-thread pool shared by 20+ jobs). |

Only `web` is exposed publicly (`fly.toml` `[[services]]` block covers `web` only). Both processes import the same `main.py` module (registering all Flask blueprints regardless of process), but only `worker` actually polls Telegram or runs the scheduler.

**RUNNER_MODE resolution bug (SCHED-03):** `RUNNER_MODE = os.getenv("RUNNER_MODE") or ("web" if _running_under_gunicorn() else "worker")` (`main.py:306-309`). `_running_under_gunicorn()` (`main.py:202-203`) checks `os.environ.get("SERVER_SOFTWARE")`, which is a **per-request WSGI environ key**, not a process-level env var gunicorn sets at startup — and `fly.toml` never sets `RUNNER_MODE` for the `web` process group. At module-import time inside the real gunicorn `web` process, this resolves to `RUNNER_MODE="worker"` unless Fly's dashboard/secrets set it out-of-band (not visible in this repo). This does not duplicate the scheduler/bot polling (those are additionally gated by the `__main__` guard, which gunicorn never triggers), but it does silently flip several `if RUNNER_MODE == "web"` / `!= "worker"` checks scattered through request-handling code — see **SCHED-03**.

### 2.2 Flask blueprints (representative, ~20 registered, all module-level in `main.py`)

`vouchers_bp` (`/v2/miniapp`), `admin_auth_bp`, `campaigns_bp`, `campaign_builder_bp`, `campaign_performance_bp`, `campaign_intelligence_bp`, `campaign_providers_bp`, `campaign_centre_bp`/`campaign_public_bp`, `subscription_verification_bp`, `tournament_integration_bp`/`tournament_rewards_bp`, `campaign_rewards_bp`, `event_banner_admin_bp`/`event_banner_public_bp`, `campaign_events_bp`, `referral_share_content_bp`, `creator_share_bp`, `community_centre_bp`, `community_media_bp`, affiliate-voucher-batch routes, and a large inline `admin_bp` (~60 routes) plus ~30 top-level `@app.route` handlers in `main.py` itself.

### 2.3 Telegram handlers (all registered only in `run_worker()`, `main.py:8792-8822`)

`CommandHandler("start")`, `CommandHandler("creator")`, `ChatJoinRequestHandler`, `ChatMemberHandler` (both `CHAT_MEMBER` and `MY_CHAT_MEMBER`), `MessageHandler` (new-chat-members, mywin-chat, admin-media-upload, private catch-all), `CallbackQueryHandler` (referral-link generation, media rename, generic button handler), plus `community_centre.register_handlers()` (poll answers) and a group `-1` priority handler for channel-reactivation verification registered lazily via `app_context.py`.

### 2.4 APScheduler jobs (all `add_job` calls live in `main.py`, not `scheduler.py`; `scheduler.py` is purely a function library)

24 jobs spanning 20s to monthly cadence — full inventory in **SCHED-Inventory** below. Notable: a self-guarding mutual exclusion between the deprecated `growth_leaderboard_weekly` job and the current `weekly_referral_post` job (`main.py:9059-9115`) — good defensive pattern, but the deprecated job is still live code with weaker crash-safety (**SCHED-02**).

### 2.5 MongoDB collections — ownership

| Concept | Ledger (source of truth) | Snapshot/cache (derived) |
|---|---|---|
| Referrals | `referral_events` (settled/revoked rows), `qualified_events`, `referral_award_events` | `users.total_referrals`/`weekly_referrals`/`monthly_referrals`, rebuilt by `scheduler.settle_referral_snapshots()` |
| XP | `xp_events` (dedup log), `xp_ledger` (per-source log) | `users.total_xp`/`weekly_xp`/`monthly_xp`, rebuilt by `xp_snapshot.settle_xp_snapshots_incremental()` |
| Vouchers/rewards | `vouchers` (code documents), `affiliate_ledger`, `voucher_pools` | `drops.public_remaining`/`my_remaining` (best-effort cache, reconciled from `vouchers` counts) |
| Scheduler coordination | `scheduler_locks` (TTL, no explicit release) | — |

### 2.6 Manual-only scripts confirmed NOT in the runtime import graph

`backfill_drop_remaining.py`, `backfill_referrals.py`, `claim_risk_sync.py`, `cleanup_stale_bse_snapshots.py`, `cleanup_xmas_fields.py`, `monthly_xp_report.py`, `patch_monthly_xp.py`, `official_channel_reopen_audit.py`, `reconcile_referrals.py`, `reconstruct_checkin_streak.py`, `referral_migration_audit.py`, `rollback_pending_referral_xp.py`, `sync_referral_counts.py`, `repair_referral_ledger.py`, `migrations/backfill_voucher_pool_scope.py`, `scripts/*.py`, `app/rebuild_snapshots_from_ledger.py`. `databot_client.py`/`databot_service.py` are wired for a future integration but explicitly documented as not gating anything yet.

### 2.7 Confirmed dead code (self-documented or verified by exhaustive grep)

`bot_joins.py` (own file-header: "LEGACY/DEAD... not started by any live process"), `referralbot.py` (standalone legacy poller, hardcoded chat ID, never imported), `checkin.py` (own file-header: "not registered as a Flask route anywhere... only test_checkin_streaks.py wires it up"). All three implement **materially different, superseded logic** (different welcome-eligibility model, cruder first-checkin heuristic, different timezone handling) — safe today only because nothing calls them; a landmine if anyone "helpfully" re-wires them.

---

## 3. Findings

Findings are ordered by severity. "Confidence: Confirmed" means I traced the exact executable code and/or reproduced it. "High confidence" means the evidence is a direct code citation but a production-data or out-of-band-config check would remove all doubt. "Needs production-data verification" means the defect is a real gap in the code but whether it has manifested depends on data/environment this audit cannot see.

---

### [SEC-01] Multiple endpoints trust a client-supplied `user_id` with zero authentication — full account impersonation for reads and some writes

- **Severity:** Critical
- **Confidence:** Confirmed
- **Category:** authentication / IDOR
- **Files/functions:**
  - `vouchers.py:1634-1636` `api_welcome_progress` (mounted at `/v2/miniapp/api/welcome-progress/<user_id>`)
  - `main.py:6808-6819` `welcome_progress_api` (second, independent registration of the **same path** `/api/welcome-progress/<user_id>` on the root app)
  - `main.py:5977-5998` `api_set_region` (`POST /api/set-region/<user_id>`)
  - `main.py:5890-5915` `api_checkin` (`POST /api/checkin`, `user_id` from JSON body)
  - `main.py:5875-5885` `api_streak` (`GET /api/streak/<user_id>`)
  - `main.py:6774-6806` `api_checkin_status` (`GET /api/checkin-status/<user_id>`)
  - `main.py:5940-5946` (`GET /api/region-status/<user_id>`)
  - `main.py:6066-6114` `api_referral` (`GET /api/referral?user_id=...`)
- **Trigger:** a plain unauthenticated HTTP request (GET for most; the `set-region`/`checkin` writes are POST but still require no credential) with an attacker-chosen numeric `user_id`.
- **Root cause:** none of these handlers call `extract_raw_init_data_from_query`/`verify_telegram_init_data`/`resolve_authenticated_telegram_user_id` (the pattern used correctly elsewhere, e.g. `miniapp_identity.py:26-49`, `vouchers.py:5910 api_claim`). They take `user_id` directly from the URL path, query string, or JSON body and query/mutate on it with no cross-check against any verified identity.
- **Exact failure scenario:** `GET /api/welcome-progress/123456789` for any numeric Telegram ID returns that user's welcome-eligibility/check-in progress; if the target user is fully eligible, the same call **actively issues** their WELCOME voucher via `_issue_or_get_welcome_voucher()` (`vouchers.py:1544`) and returns the code in the JSON body (`vouchers.py:1486-1488`, `1546-1549`) — before the real user ever opens the Mini App. Separately, `POST /api/set-region/<user_id>` with an arbitrary target ID performs a "first write wins, then locked" region write (`main.py:5977-5998`), letting an attacker race and permanently lock a victim's region ahead of them. `GET /api/referral?user_id=<id>&username=<name>` additionally calls the **live Telegram Bot API** (`createChatInviteLink`) attributed to the attacker-chosen identity, with no rate limit.
- **User/business impact:** private profile/progress disclosure for any user; theft/premature issuance of another user's welcome voucher; ability to lock a victim's region setting; Telegram Bot API quota exhaustion via an unauthenticated loop.
- **Data affected:** `users` (region, streak, check-in fields), `affiliate_ledger`/`voucher_pools` (WELCOME entitlement + code), `invite_link_map` (referral link attribution).
- **Abuse potential:** trivial, unauthenticated, scriptable enumeration across the numeric Telegram ID space; wildcard CORS (`main.py:2581`, `CORS(app, resources={r"/*": {"origins": "*"}})`) means a third-party web page a victim merely visits can also trigger these reads/writes cross-origin from the victim's browser (no cookies needed since these routes use no session).
- **Existing protections:** none on these specific routes.
- **Why those protections are insufficient:** N/A — there are none. The properly-built sibling endpoints (`api_claim`, `api_referral_status`, `api_referral_progress`) prove the correct pattern exists elsewhere in the same file but was not applied here.
- **Minimal safe remediation:** require verified initData on all seven routes and derive `user_id` from the verified payload (reusing `resolve_authenticated_telegram_user_id`), rejecting the request if the path/body `user_id` doesn't match. For `api_checkin`, drop the body `user_id` entirely in favor of the verified identity.
- **Tests required:** a new test per route asserting a 401/403 when init_data is absent or belongs to a different user than the path parameter; regression test confirming `build_welcome_progress_response` is never invoked with an unverified `user_id`.
- **Production verification:** check access logs for `/api/welcome-progress/`, `/api/set-region/`, `/api/referral` for sequential/enumerated `user_id` patterns from a small set of IPs.
- **Rollback considerations:** none — this is a pure hardening fix (adding a check), safe to deploy immediately; verify the frontend always sends `init_data` on these calls first (`static/index.html` fetch call sites) so the fix doesn't break legitimate usage.

---

### [WELCOME-01] Welcome voucher entitlement (v2 ledger) and the frontend's claim UI (legacy drops system) are two unlinked inventories

- **Severity:** High
- **Confidence:** High confidence (code-traced both sides; whether it has manifested in production depends on whether ops currently maintains both inventories in parallel)
- **Category:** reward-integrity / cross-system consistency
- **Files/functions:** `affiliate_rewards.py:1414-1529` (`issue_welcome_bonus_if_eligible`, authoritative issuance into `affiliate_ledger`/`voucher_pools` pool `WELCOME`), `vouchers.py:1401-1631` (`build_welcome_progress_response`), `vouchers.py:4612-4683` (`user_visible_drops`, only uses welcome progress to decide *whether to hide* a legacy `db.drops` card), `static/index.html:3758-3766` (`claimWelcomeRewardOneTap`, drives the legacy `/vouchers/claim` flow by clicking a `[data-welcome-card="true"]` button).
- **Trigger:** operations lets the legacy `db.drops` document with `audience: "new_joiner"`/`"new_joiner_48h"` expire or never creates one, while the v2 `voucher_pools`(`pool_id="WELCOME"`) batch inventory is kept current.
- **Root cause:** the frontend's actual "reveal my code" action is hard-wired to the legacy pooled/personalised claim system (`db.drops` + `db.vouchers` + `/vouchers/claim`), not to the v2 ledger the eligibility/issuance logic was migrated to. `build_welcome_progress_response` computes and returns a real `voucher_code` from the v2 ledger, but the frontend **never renders `data.voucher_code`** (`static/index.html` welcome-status card only shows a status badge) — it depends on finding a live legacy drop card to click.
- **Exact failure scenario:** a user completes check-ins, `affiliate_ledger.status` flips to `ISSUED` with a real code allocated from `voucher_pools`, the UI shows "🎁 Ready to Claim" → user taps "Claim Now" → no `[data-welcome-card]` element exists (legacy `db.drops` card absent/expired) → `highlightWelcomeRewardCard()` no-ops → user is shown nothing and never learns their code.
- **User/business impact:** users become server-side "rewarded" but receive nothing, with no error surfaced — support-ticket generator and quiet reward-integrity failure (paid-out-on-paper, never-delivered-in-practice).
- **Data affected:** `affiliate_ledger` (WELCOME rows stuck `ISSUED` with an unseen code), `voucher_pools`.
- **Abuse potential:** none (this under-delivers rather than over-delivers), but it is a genuine business-integrity gap.
- **Existing protections:** none — no test asserts a `db.drops` `new_joiner` card exists whenever `build_welcome_progress_response` reports `status="issued"`, confirmed by the onboarding-flow research pass.
- **Why those protections are insufficient:** the two inventories are maintained by different admin workflows with no code-level link or alert.
- **Minimal safe remediation:** either (a) have the frontend render `data.voucher_code` directly from `/api/welcome-progress` when `status=="issued"` (bypassing the legacy claim button entirely), or (b) add a startup/scheduled health check asserting an active `new_joiner` drop exists whenever the WELCOME batch pipeline is active, alerting ops if not.
- **Tests required:** an integration test asserting that when `affiliate_ledger.status=="ISSUED"`, the mini-app response contains a renderable code path independent of `db.drops` state.
- **Production verification:** check whether a `db.drops` document with `audience` in `{"new_joiner","new_joiner_48h"}` and `status` active currently exists; cross-reference `affiliate_ledger` WELCOME rows with `status=="ISSUED"` against whether their `voucher_code` was ever fetched by the claim endpoint.
- **Rollback considerations:** frontend-only change (option a) is low-risk and immediately reversible.

---

### [WELCOME-02] Welcome voucher ledger rows that hit `OUT_OF_STOCK` have no automatic or manual recovery path

- **Severity:** High
- **Confidence:** High confidence
- **Category:** reward-integrity
- **Files/functions:** `affiliate_rewards.py:22` (`FINAL_STATUSES = {"ISSUED","OUT_OF_STOCK","REJECTED"}`), `affiliate_rewards.py:1461-1462` (short-circuits if already in a final status), `affiliate_rewards.py:1522-1529` (transitions to terminal `OUT_OF_STOCK` when the WELCOME pool is empty at claim time).
- **Trigger:** a user completes welcome eligibility at the exact moment the WELCOME voucher pool is empty.
- **Root cause:** `OUT_OF_STOCK` is treated as a **final** status with no re-attempt job, unlike the parallel and structurally similar `PENDING_MANUAL` status used by affiliate T1–T4 tiers, which **is** retried automatically (`issue_current_month_affiliate_rewards()`/`retry_current_month_pending_manual_ledgers()`, `affiliate_rewards.py:2025-2117,2189-2191`) and has an explicit admin retry endpoint for tournament/campaign rewards (`tournament_rewards.py:393-418`). No equivalent admin endpoint or scheduled sweep exists for WELCOME.
- **Exact failure scenario:** WELCOME pool empties → next eligible user's ledger row becomes `OUT_OF_STOCK` → admin restocks the pool → that user's entitlement is never revisited because `issue_welcome_bonus_if_eligible` treats `OUT_OF_STOCK` as done (`affiliate_rewards.py:1461-1462`).
- **User/business impact:** permanent denial of a reward the user legitimately earned, for as long as the pool happened to be empty at their exact claim moment.
- **Data affected:** `affiliate_ledger` (WELCOME rows).
- **Abuse potential:** none — this is a false-negative/under-delivery bug, not exploitable for gain.
- **Existing protections:** none.
- **Minimal safe remediation:** add `OUT_OF_STOCK` to the retry sweep already used for `PENDING_MANUAL`/`SETTLING` (or build a small WELCOME-specific equivalent), or add an admin "retry allocation" endpoint mirroring the tournament-rewards one.
- **Tests required:** a test asserting that a WELCOME ledger row transitions out of `OUT_OF_STOCK` once new stock is uploaded and the sweep runs.
- **Production verification:** `db.affiliate_ledger.count_documents({"ledger_type":"WELCOME","status":"OUT_OF_STOCK"})` (read-only) to size the current backlog.
- **Rollback considerations:** additive change, no rollback risk.

---

### [SCHED-01] Scheduler locks are never explicitly released — `tick_5min`'s lock TTL already exceeds its own cron interval, throttling referral/XP settlement cadence even under healthy operation, and can also cause true double-execution under Telegram degradation

- **Severity:** High
- **Confidence:** Confirmed
- **Category:** concurrency / scheduler safety
- **Files/functions:** `main.py:729-751` (`acquire_scheduler_lock`), `main.py:781-856` (`tick_5min`, ttl=900s, registered on cron `*/5` i.e. every 300s — `main.py:8953-8959`), `scheduler.py:3078` / `scheduler.py:884-890` (`getChatMember`, `timeout=10` per row), `scheduler.py:2997-3015` (claim batch, `batch_limit=200` in `settle_pending_referrals`).
- **Trigger — everyday case, no degradation needed:** `acquire_scheduler_lock`'s TTL (900s = 15 min) is 3× `tick_5min`'s own cron interval (300s = 5 min), and the lock is never explicitly released on completion. A normal, fast run that finishes in seconds still holds the lock until its 15-minute TTL expires. Concretely: a run starting at minute 0 (however long it actually takes) holds the lock through minute 15; the ticks scheduled for minute 5 and minute 10 both find the lock still held and silently no-op (`main.py:757-763`-style "lock_not_acquired" log, no error). The *effective* settlement cadence is therefore ~15 minutes under completely normal operation, not the intended 5 — this is not a hypothetical edge case, it is the steady-state behavior of the current TTL/cron combination.
- **Trigger — additional, worse case:** a period of Telegram API slowness/degradation coinciding with a run that has to check membership for close to its full 200-row batch limit can push actual runtime *past* the 15-minute TTL itself (200 rows × up to 10s each ≈ 2000s / 33 min worst case), at which point a genuinely-still-running job's lock can be re-acquired by the next tick, opening a true concurrent-execution window (not just a throttled cadence).
- **Root cause:** no corresponding lock-release call exists anywhere in the codebase (grepped clean) — TTL is the only release mechanism, and it was sized without accounting for its relationship to the job's own cron interval.
- **Exact failure scenario (routine):** referral settlement, XP snapshot rebuild, and referral snapshot rebuild (all invoked from within `tick_5min`) run roughly every 15 minutes in practice, not every 5 — meaning newly-settled referrals/XP/leaderboard positions can lag reality by up to 15 minutes even with Telegram fully healthy, directly affecting `_clear_leaderboard_cache`-driven UI freshness (`main.py:916`) and any downstream logic that assumes 5-minute freshness.
- **Exact failure scenario (degraded):** as previously described — Telegram slowness pushes a run past 15 minutes, the lock expires mid-run, and a second concurrent pass can begin.
- **User/business impact:** the settlement logic underneath is independently idempotent (atomic claim-then-process, unique award keys) so double-grants are unlikely even in the degraded case, but (a) referral/XP/leaderboard staleness is a real, everyday UX and support-ticket generator, not just a rare-degradation risk, and (b) the lock's purpose of serializing snapshot rebuilds (`scheduler.py:2469-2483`'s two-phase compute-then-publish design) is defeated exactly when serialization matters most.
- **Data affected:** `pending_referrals`, `scheduler_locks`; `users` snapshot fields (staleness in the routine case, rare overlapping-rebuild risk in the degraded case).
- **Abuse potential:** none — availability/freshness concern, not exploitable.
- **Existing protections:** the atomic CAS lock itself (good), plus downstream idempotency in the settlement functions (good) — these bound the degraded-case impact to "redundant work" rather than "double reward," but do nothing for the routine-case staleness, which isn't a race at all, just a mis-sized TTL.
- **Why those protections are insufficient:** idempotency protects against double-processing; it does not address the far more common problem that the job simply doesn't run as often as its cron schedule implies.
- **Minimal safe remediation:** (1) add an explicit lock-release on successful completion so a fast run frees the slot immediately for the next scheduled tick; (2) independently, either lower the TTL to something realistic for the *typical* run (with release-on-success handling the normal case) while keeping a higher ceiling as a dead-worker safety net, or reduce the per-run Telegram-call budget (lower `batch_limit`, or a wall-clock cutoff inside the loop) so the TTL is sized against a true worst case rather than 3× the intended cadence.
- **Tests required:** (1) a test asserting that a fast, successful `tick_5min` run releases its lock such that the very next scheduled invocation can acquire it immediately, not 15 minutes later; (2) a test simulating a run exceeding its TTL, asserting the reclaim path doesn't double-process the same `pending_referrals` rows already claimed by the first run (should already hold via the `processing` status guard, but not currently asserted for the lock-expiry-during-run case specifically).
- **Production verification:** check scheduler/job logs for the actual gap between consecutive successful `tick_5min` completions (expect ~15 min, not ~5 min, confirming the routine-case throttling) and for any runs with `elapsed_s` approaching or exceeding 900s (confirming exposure to the degraded-case double-execution window).
- **Rollback considerations:** low risk, additive.

---

### [REF-01] Referral join-ack DM: missing import (`pm_allowed`) is latent behind an inverted `RUNNER_MODE` guard — feature is currently dead, but fixing the guard alone will crash the referral-join critical path

- **Severity:** High (as a latent defect waiting to activate) / currently Medium in practice (feature silently non-functional)
- **Confidence:** Confirmed
- **Category:** correctness / dead code / test-gap
- **Files/functions:** `main.py:1273-1292` (`_maybe_send_near_miss_dm`, never called anywhere — dead), `main.py:1341-1392` (`_maybe_send_referral_join_ack_dm`), call site `main.py:1972` (inside `_confirm_referral_on_main_join`, itself only ever invoked from `member_update_handler`, `main.py:8270,8283`, which is registered **only** inside `run_worker()`).
- **Root cause (two stacked bugs):**
  1. `pm_allowed(...)` is called at `main.py:1278` and `main.py:1352` but **`pm_preferences` (where `pm_allowed` is defined, `pm_preferences.py:27`) is never imported anywhere in `main.py`** — confirmed by `ruff --select=F821` (`Undefined name 'pm_allowed'`) and by direct grep of every import statement in `main.py`. Calling either function as written raises `NameError` immediately.
  2. Both functions early-return `if RUNNER_MODE != "web": return` (`main.py:1274`, `1346`). But their only real call path is inside a Telegram `ChatMemberHandler` callback, which only runs inside the `worker` process, where `RUNNER_MODE` is explicitly `"worker"` (set by `fly.toml`'s `worker` process command). So in the current architecture, this guard **always** returns before ever reaching the `pm_allowed` call — the `NameError` is currently dormant, and as a side effect the "referral join ack DM" / "near-miss DM" features have **never actually sent a Telegram message in production**, regardless of the missing import.
- **Exact failure scenario:** if a future change "fixes" the guard (e.g., changes it to `!= "worker"`, which is what someone would naturally do noticing it never fires) without also adding the missing `pm_preferences` import, `_maybe_send_referral_join_ack_dm` will raise `NameError` on **every successful referral pending-creation**. That call happens inside a broad `except Exception as e:` block (`main.py:1986-2015`) whose comment explicitly assumes "no pending row was actually created" — but by the time this call fires, the pending referral row **has** already been committed. The handler will then call `referral_invitee_lock.release(..., status="revoked", ...)` (`main.py:1993-1999`), erroneously revoking the just-created invitee lock for a referral that, from the ledger's perspective, succeeded — leaving the lock released while a `pending_referrals` row (or a subsequent duplicate one) exists, undermining the very guarantee (§ referral flow, one-invitee-one-inviter) that lock exists to enforce.
- **User/business impact:** currently, only a missing notification (no functional harm). If "fixed" incorrectly, a production-breaking regression in the referral pipeline's core integrity guard.
- **Data affected:** `referral_invitee_locks`, `pending_referrals` (if the guard is ever "fixed" without the import fix).
- **Abuse potential:** none currently; would become a real duplicate-credit vector if the guard is naively "fixed" first.
- **Existing protections:** none against the stacked-bug scenario — no test exercises the real call chain end-to-end with both fixes representative of what would happen if only one is applied.
- **Why those protections are insufficient:** `test_referral_ack_dm.py` extracts *only* the function body via `ast.parse` + `exec()` and manually injects `pm_allowed` into the isolated namespace's globals (`test_referral_ack_dm.py:70-83`, `fn.__globals__.update({..., "pm_allowed": lambda *a,**k: True, ...})`) — this pattern (also used in `test_referral_near_miss_dm.py`, `test_referral_qualified_dm.py`, `test_referral_status_api.py`, `test_share_rank_caption.py`, `test_start_referral_button.py`, `test_start_referral_deeplink.py`, `test_start_referral_url.py`, `test_welcome_pm_flow.py` — 8 files, confirmed via bandit `B102 exec_used` scan) is a systemic anti-pattern: it proves the function's *logic* is correct given its dependencies, but can never catch a genuinely missing import in the real module, because the test supplies that exact name by hand.
- **Minimal safe remediation:** add `from pm_preferences import pm_allowed` to `main.py`'s imports, and separately decide/fix the `RUNNER_MODE` guard to match where this function is actually meant to run (worker, given its only call site) — do these as one atomic change, not sequentially.
- **Tests required:** an import-level smoke test (`python -c "import main"` in CI, or a static `ruff --select=F821` gate) that would have caught this immediately; an integration test that imports `main` as a real module (not AST-isolated) and drives `_confirm_referral_on_main_join` end-to-end.
- **Production verification:** N/A — confirmed dormant by call-site tracing; no production data needed.
- **Rollback considerations:** trivial one-line import fix; low risk.

---

### [SEC-02] Admin panel secret compared with `==` instead of constant-time comparison

- **Severity:** Medium
- **Confidence:** Confirmed
- **Category:** authentication
- **Files/functions:** `vouchers.py:493-497` (`_admin_secret_ok`, used by `require_admin`/`require_admin_from_query`, the guard on essentially every `/api/admin/*` route) vs. `vouchers.py:4253-4264` (`_has_valid_admin_secret`, used only by `_is_admin_preview`), which correctly uses `hmac.compare_digest`.
- **Trigger:** any request presenting an `admin_secret` (via `Authorization: Bearer`, `X-Admin-Secret`, or `?admin_secret=` query param).
- **Root cause:** `value.strip() == _ADMIN_PANEL_SECRET.strip()` short-circuits on the first mismatching byte — theoretically a timing side channel, and inconsistent with the constant-time comparison used two hundred lines away for the same conceptual secret.
- **Exact failure scenario:** in principle, an attacker measuring response-time variance across many requests could narrow down the secret byte-by-byte; in practice, network jitter over the internet makes this hard but not impossible, especially against a colocated attacker.
- **User/business impact:** admin-panel compromise if exploited.
- **Data affected:** all admin-gated data/actions.
- **Abuse potential:** low-to-moderate in practice given network noise, but zero-cost to fix.
- **Existing protections:** none on this path; the correct pattern exists in the same file.
- **Minimal safe remediation:** replace the `==` with `hmac.compare_digest` in `_admin_secret_ok`, matching `_has_valid_admin_secret`.
- **Tests required:** none beyond the fix itself (behavior is unchanged for valid/invalid secrets).
- **Production verification:** not applicable.
- **Rollback considerations:** none.

---

### [SEC-03] Wildcard CORS (`origins: "*"`) on every route amplifies every unauthenticated endpoint's exploitability

- **Severity:** Medium (amplifier for SEC-01, not independently critical since credentials are not enabled alongside it)
- **Confidence:** Confirmed
- **Category:** configuration
- **Files/functions:** `main.py:2580-2581` `CORS(app, resources={r"/*": {"origins": "*"}})`.
- **Root cause:** blanket wildcard CORS with no per-route restriction, applied to the entire Flask app including admin and user-data endpoints.
- **Exact failure scenario:** because `supports_credentials` is not set (confirmed: no occurrence anywhere in the repo), browsers won't attach the wildcard-ACAO response to a credentialed request, so this does not by itself leak session-cookie-authenticated admin data. It does, however, mean any third-party page can `fetch()` the unauthenticated endpoints in **SEC-01** on a victim's behalf directly from the victim's browser.
- **User/business impact:** lowers the bar for exploiting SEC-01 from "attacker's own script" to "any page the victim visits."
- **Minimal safe remediation:** scope CORS to the actual Mini App origin(s) for user-data routes; keep it open only where genuinely needed (e.g., public read-only leaderboard data), and prioritize fixing SEC-01 regardless.
- **Tests required:** none beyond config change verification.
- **Production verification:** not applicable.
- **Rollback considerations:** verify the Mini App's actual origin(s) before tightening, to avoid breaking legitimate access.

---

### [XP-01] `xp_events` idempotency key is not scoped by event type — a caller-suppliable admin `unique_key` can collide with an unrelated event type

- **Severity:** Medium
- **Confidence:** Confirmed
- **Category:** reward-integrity
- **Files/functions:** `xp.py:32-109` (`grant_xp`), unique index `xp.py:147-155` on `(user_id, unique_key)` only (no `source`/type field), contrast with `xp_ledger`'s unique index on `(user_id, source, source_id)` (`xp.py:176-181`); admin caller `main.py:7109-7110` (`api_admin_add_xp` passes a caller-suppliable `idempotency_key`/`unique_key` straight through).
- **Trigger:** an admin XP adjustment tool (or any future caller) supplies a `unique_key` string that happens to match another event type's key convention for the same user (e.g. `checkin:20260731`).
- **Root cause:** the dedup guarantee is per-`(user, unique_key)` string, not per-`(user, type, unique_key)` — disjointness across event types (`checkin:`, `first_checkin`, `welcome_bonus`, `ref:`, `admin:`, `reactivation_journey:`, campaign-specific prefixes) is enforced only by naming convention, not by a DB constraint.
- **Exact failure scenario:** an admin tool call with `unique_key="checkin:20260731"` for a user who also legitimately checked in that day would collide in the unique index — whichever write lands second is silently rejected as a "duplicate," and if that's the real check-in event, the user's real check-in XP could be silently dropped (or vice versa, blocking a legitimate admin adjustment).
- **User/business impact:** a rare but possible silent XP-grant loss, hard to detect since it looks like normal idempotent-duplicate suppression in logs.
- **Data affected:** `xp_events`, `xp_ledger`.
- **Abuse potential:** low (requires either an admin-tool misconfiguration or a deliberately crafted admin request) but the guard rail that should prevent it structurally doesn't exist.
- **Existing protections:** the `restrictions.no_xp` check and rollback-on-race logic in `grant_xp` are solid; they just don't cover this specific cross-type collision because the index itself doesn't scope by type.
- **Minimal safe remediation:** namespace-prefix all `unique_key` values with their event type before they reach the unique index (or add `source`/`event_type` into the unique index key), and validate that admin-supplied `unique_key`s cannot collide with the reserved prefixes.
- **Tests required:** a test asserting an admin-supplied key colliding with a `checkin:`-prefixed key is rejected/namespaced rather than silently shadowing a real event.
- **Production verification:** scan `xp_events` for `unique_key` values that don't match any of the known prefixes, indicating an admin tool has already used a colliding convention.
- **Rollback considerations:** index/key-format change requires a migration if deployed; can be done additively (validate at write time first, backfill the index later).

---

### [VOUCHER-01] A crash between claim-lock acquisition and code issuance can permanently strand a user's claim slot (unique `(drop_id,user_id)` index blocks all retries)

- **Severity:** Medium-High
- **Confidence:** High confidence (code-traced; whether it has occurred depends on production crash history)
- **Category:** reward-integrity / data consistency
- **Files/functions:** `vouchers.py:2695-2701` (`_acquire_claim_lock`, sets `status: "claimed_pending_code"`), unique index `uq_claim_drop_user` on `(drop_id, user_id)` (`vouchers.py:735-750`), retry branch at `vouchers.py:2726-2735` (only reopens a `status=="failed"` claim).
- **Trigger:** a worker/request-handling process crash (OOM, deploy restart, unhandled exception) between the claim-lock insert and the point where a real voucher code is assigned/the claim is marked `failed`.
- **Root cause:** unlike the affiliate ledger's `SETTLING` status (which has a documented 15-minute stale-cutoff reclaim sweep, `affiliate_rewards.py:2128-2186`), no equivalent sweeper exists for a `voucher_claims` row stuck at `"claimed_pending_code"`. Because `(drop_id,user_id)` is uniquely indexed, that stuck row **permanently blocks any future claim attempt** by that user for that drop.
- **Exact failure scenario:** process crashes mid-request after `_acquire_claim_lock` succeeds but before code allocation/failure-marking completes → the user's `voucher_claims` row is stuck at `claimed_pending_code` forever → every subsequent claim attempt by that user for that drop hits the unique index and is rejected, with no self-service or automatic path to recovery (would need manual DB intervention).
- **User/business impact:** a legitimate user is permanently denied a reward they should be able to claim, indistinguishable from "already claimed" in the UI.
- **Data affected:** `voucher_claims`.
- **Abuse potential:** none (under-delivery, not exploitable for gain).
- **Existing protections:** a `status=="failed"` retry path exists but nothing transitions a crashed `claimed_pending_code` row to `failed` automatically.
- **Minimal safe remediation:** add a stale-cutoff sweep (mirroring the affiliate ledger's 15-minute `SETTLING` reclaim) that flips `claimed_pending_code` rows older than N minutes to `failed`, enabling the existing retry path.
- **Tests required:** a test simulating a crash between lock-acquire and code-issuance, asserting the row is reclaimable after the timeout.
- **Production verification:** `db.voucher_claims.find({"status":"claimed_pending_code","created_at":{"$lt": <cutoff>}})` (read-only) to size any existing backlog.
- **Rollback considerations:** additive, low risk.

---

### [SCHED-02] Legacy `post_growth_leaderboard_weekly` job can leave a permanently stuck record with no reconciliation path

- **Severity:** Medium (dormant unless `GROWTH_LEADERBOARD_ENABLED=1` and no `WEEKLY_REF_POST_CHAT_ID` conflict — but it is live, reachable code)
- **Confidence:** Confirmed
- **Category:** scheduler safety / dead-record risk
- **Files/functions:** `scheduler.py:1316-1414`, `growth_leaderboard_posts` collection.
- **Root cause:** the Telegram send (`requests.post`/`raise_for_status`) is not wrapped in try/except; any exception propagates uncaught, leaving the week's document at `status:"posting"` forever, and the job's own "already posted, skip" guard (`scheduler.py:1338-1341`) treats **any** existing document for that week — regardless of status — as done, so the stuck record is never retried.
- **Contrast:** the current, non-deprecated `publish_weekly_referral_post` (`scheduler.py:1512-1707`) handles the identical scenario correctly via an explicit `frozen→sending→sent|failed` state machine with a 10-minute lease reclaim.
- **Exact failure scenario:** a transient Telegram/network failure during the legacy job's send → that week's leaderboard post never publishes and is never retried, silently, for the rest of that week.
- **User/business impact:** low if this job is genuinely unused in the current environment (the mutual-exclusion guard at `main.py:9059-9115` suggests it usually isn't registered), but it is live code that could be re-enabled by a config change without anyone re-reviewing its crash-safety.
- **Data affected:** `growth_leaderboard_posts`.
- **Minimal safe remediation:** either delete the job entirely (its replacement is production-ready) or bring its error handling and state machine up to the same standard as `publish_weekly_referral_post`.
- **Tests required:** if kept, a test asserting a send failure doesn't permanently block that week's retry.
- **Production verification:** confirm `GROWTH_LEADERBOARD_ENABLED` is unset/false in the live Fly config; `db.growth_leaderboard_posts.find({"status":"posting"})` if it might ever have run.
- **Rollback considerations:** deleting dead code is safe; fixing it in place is additive.

---

### [SCHED-03] `_running_under_gunicorn()` check is unreliable — `RUNNER_MODE` may silently resolve to `"worker"` inside the real `web` process

- **Severity:** Medium
- **Confidence:** High confidence (code logic confirmed; actual production impact depends on whether Fly's dashboard/secrets set `RUNNER_MODE` for the `web` process out-of-band, which this audit cannot see)
- **Category:** correctness / configuration
- **Files/functions:** `main.py:202-203` (`_running_under_gunicorn`), `main.py:306-309` (`RUNNER_MODE` resolution), gated behaviors: `main.py:661-663` (`recompute_xp_totals`, guarded `if RUNNER_MODE == "web"`), `main.py:1124-1125` (`_check_snapshot_freshness`, guarded `if RUNNER_MODE != "worker": return`).
- **Root cause:** `SERVER_SOFTWARE` is a per-request WSGI environ key that gunicorn injects into each request's environ dict — it is not present in `os.environ` at module-import time, which is when this check runs. `fly.toml`'s `web` process command sets no `RUNNER_MODE` env var either. Absent an out-of-band override, `RUNNER_MODE` resolves to `"worker"` inside the gunicorn `web` process too.
- **Exact failure scenario:** `_check_snapshot_freshness()` (intended to run only in the worker) would additionally execute inside every gunicorn `web` gthread (2 workers × 4 threads = 8 concurrent contexts) wherever it's called from a request path; `recompute_xp_totals`'s "block this under web" guard would never actually fire in the real web process, silently defeating its intended protection.
- **User/business impact:** likely benign in practice for both cited call sites (one is a read-only heartbeat check, the other is a safety block that just doesn't block) but this is exactly the kind of silent-config-drift bug that becomes a real problem the next time someone adds a genuinely process-sensitive check using this same guard, trusting it to work.
- **Data affected:** none directly; behavioral only.
- **Minimal safe remediation:** set `RUNNER_MODE=web` explicitly in `fly.toml`'s `web` process command (mirroring how `worker` already does it for itself), removing the fragile auto-detection entirely.
- **Tests required:** a test asserting `RUNNER_MODE` resolves correctly when `SERVER_SOFTWARE`/`GUNICORN_CMD_ARGS` are absent from `os.environ` (i.e., proving the current auto-detect is broken) plus confirmation the explicit env var takes precedence.
- **Production verification:** check Fly's actual configured env vars for the `web` machine (`fly secrets list` / dashboard) for `RUNNER_MODE`.
- **Rollback considerations:** trivial one-line `fly.toml` change; no code risk.

---

### [MONGO-01] `users.username` has no unique index — `update_user_xp`'s regex username lookup can silently target the wrong account

- **Severity:** Medium
- **Confidence:** Confirmed
- **Category:** data integrity
- **Files/functions:** `database.py:219,224-230` (both `username` indexes are non-unique, one with a case-insensitive collation), `database.py:448-451` (`update_user_xp` resolves a user by case-insensitive regex on `username`, taking the first `find_one` match).
- **Trigger:** two user documents happen to share a username (case-insensitively) — plausible given Telegram usernames can be released and re-registered by a different person, and this system apparently doesn't enforce uniqueness at signup.
- **Root cause:** no unique constraint exists on `username` at any collation.
- **Exact failure scenario:** an admin XP grant addressed by username routes to whichever of the two matching documents Mongo returns first (non-deterministic ordering without an explicit sort), potentially crediting/debiting the wrong account.
- **User/business impact:** misdirected admin XP adjustments; support/trust impact if discovered.
- **Data affected:** `users`, `xp_events`, `xp_ledger`.
- **Minimal safe remediation:** either enforce username uniqueness at write time (reject/merge on collision) and add a unique index, or change `update_user_xp` to require `user_id` and treat username as a lookup convenience only, never the sole key for a mutating operation.
- **Tests required:** a test with two users sharing a case-insensitive username, asserting the admin tool refuses to proceed ambiguously rather than picking one silently.
- **Production verification:** `db.users.aggregate([{"$group":{"_id":{"$toLower":"$username"},"count":{"$sum":1},"ids":{"$push":"$user_id"}}},{"$match":{"count":{"$gt":1}}}])` (read-only) to find existing collisions.
- **Rollback considerations:** a uniqueness enforcement change needs a data cleanup pass first if collisions already exist.

---

### [MONGO-02] Most of `ensure_indexes()`'s index-creation calls bypass the safer conflict-resolution helper that was built after a real production crash

- **Severity:** Medium
- **Confidence:** Confirmed
- **Category:** data integrity / deployment safety
- **Files/functions:** `database.py:88-125` (`safe_create_index`), `database.py:128-206` (`_ensure_equivalent_index`), `database.py:214-291` (the bulk of `ensure_indexes()`, using bare `create_index()` for `voucher_whitelist`, `users`, `user_snapshots`, `segment_snapshots`, `marketing_raw_data`, `monthly_xp_history`, `channel_subscription_cache`), vs. lines 302,305,310 which do use `safe_create_index`.
- **Root cause/evidence:** `test_index_conflict_resolution.py`'s own docstring documents that production **already crashed once** with `OperationFailure` code 85 (`IndexOptionsConflict`) because `voucher_claims`/`voucher_pools` carried unique indexes on the same key pattern under a different name than the code requested. The fix (`safe_create_index`/`_ensure_equivalent_index`) is only applied to 3 of the ~15 index-creation call sites in `ensure_indexes()`.
- **Exact failure scenario:** if any future migration or manual `create_index` call introduces a differently-named index with the same key pattern on any of the unprotected collections (`users`, `user_snapshots`, `segment_snapshots`, `marketing_raw_data`, `monthly_xp_history`, `channel_subscription_cache`, `voucher_whitelist`), the next deploy's `ensure_indexes()` call will throw `OperationFailure` on startup — the exact class of incident this helper function exists to prevent, just not applied uniformly.
- **User/business impact:** a full application-startup crash (both `web` and `worker` call `ensure_indexes()`), i.e. an outage, triggered by an easily-avoidable index-naming mismatch.
- **Minimal safe remediation:** route all `create_index` calls in `ensure_indexes()` through `safe_create_index`/`_ensure_equivalent_index` for consistency.
- **Tests required:** extend `test_index_conflict_resolution.py`'s coverage to the currently-unprotected collections.
- **Production verification:** not applicable (this is a latent deploy-time risk, not a current-data issue).
- **Rollback considerations:** none — purely additive robustness.

---

### [REF-02] `sync_referral_counts.py` lacks the negative-count clamp the main settlement pipeline has

- **Severity:** Medium
- **Confidence:** Confirmed (read the exact code)
- **Category:** data integrity
- **Files/functions:** `sync_referral_counts.py:74-104` (`computed_total = int(ledger_totals.get(uid, 0))`, written straight to `users.total_referrals` with no floor), contrast with `scheduler.py:2382-2384` (`clamped_weekly/monthly/total = max(0, ...)`, explicitly commented "a referral count must never be negative... corrupted legacy ledger rows... could otherwise depress the tier calculation").
- **Trigger:** an operator runs `sync_referral_counts.py --commit` against `referral_events` data that still contains orphaned/unrepaired `referral_revoked` rows (the exact historical corruption class `repair_referral_ledger.py` exists to fix).
- **Root cause:** this maintenance script re-derives the same ledger aggregation as the main pipeline but omits the `max(0, ...)` floor the main pipeline was specifically hardened with.
- **Exact failure scenario:** running this script before `repair_referral_ledger.py --commit` on data with unrepaired orphan revocations could write a negative `total_referrals` to a user document.
- **User/business impact:** a negative referral count is a visible data-integrity red flag (could affect tier calculations, leaderboard display, or trigger downstream assertion failures in code that assumes non-negative counts).
- **Data affected:** `users.total_referrals`.
- **Minimal safe remediation:** add the same `max(0, computed_total)` clamp used in `settle_referral_snapshots`, and/or have the script refuse to run (or warn loudly) if `repair_referral_ledger.py`'s invalidation step hasn't been run first.
- **Tests required:** a test asserting the script never writes a negative value even when fed a ledger with unrepaired orphan revocations.
- **Production verification:** `db.users.find({"total_referrals": {"$lt": 0}})` (read-only) — see §5 for the full query.
- **Rollback considerations:** additive clamp, no risk.

---

### [REF-03] Stale invite-link mapping has no independent expiry check; dead fallback field mismatch

- **Severity:** Low-Medium
- **Confidence:** Confirmed
- **Category:** data consistency
- **Files/functions:** `referral.py:28-46` (no `revoke_chat_invite_link` call when a new link is created; no `member_limit` set), `main.py:1697-1704` (attribution match on `{invite_link, chat_id, is_active: {$ne: False}}` with no time-window check), `main.py:1467-1481` (`_resolve_referrer_id_from_invite_link`'s fallback queries `users.referral_invite_link`, a field never written by `referral.py`, which writes `referral_link` instead).
- **Root cause:** old invite-link mapping rows are left `is_active: True` indefinitely (Telegram itself enforces the 24h link expiry, but the app-side mapping table has no independent secondary check), and a fallback lookup path references a field name that doesn't match what's actually written, making that fallback permanently dead.
- **Exact failure scenario:** low practical risk today since Telegram enforces the link's own 24h expiry before the app-side row would matter; the dead fallback field is purely a maintainability trap, not an active bug.
- **User/business impact:** minor — mostly a code-hygiene/maintainability risk (a future engineer might rely on the dead fallback thinking it works).
- **Minimal safe remediation:** align the fallback field name with what `referral.py` actually writes (or remove the dead fallback path), and consider stamping/checking an explicit expiry on `invite_link_map` rows independent of Telegram's own enforcement.
- **Tests required:** a test asserting the fallback lookup path actually matches current write behavior (currently it cannot, by construction).
- **Production verification:** `db.users.count_documents({"referral_invite_link": {"$exists": true}})` — likely zero, confirming the field is dead.
- **Rollback considerations:** none, cleanup-only.

---

### [REF-04] Referral rate limiter increments its hourly counter before checking the limit, and fails open on its own errors (opposite direction from the duplicate-prevention guards)

- **Severity:** Low-Medium
- **Confidence:** Confirmed (with test evidence)
- **Category:** abuse-resistance / consistency
- **Files/functions:** `referral_rate_limit.py:26-88` (`consume_referral_rate_limits`), confirmed by `test_referral_rate_limit.py:48-61` (the 21st, blocked call still leaves `hour_doc.count == 21`); caller `main.py:1771-1785` wraps the call in try/except and **fails open** (`allowed = True`) on any exception.
- **Root cause:** the `$inc` happens inside the same `find_one_and_update` used for the limit check, so a call that ends up blocked has still permanently consumed one unit of quota; separately, a rate-limiter DB error is treated as "allow the referral," the opposite fail-safe direction from the historical-success and invitee-lock guards (which fail closed).
- **Exact failure scenario:** in production this function is invoked once per real join event, so it's not exploitable by repeated "probing" today — but it does mean the rate limiter itself is not abuse-hardened against a caller that could trigger repeated invocations for the same event (e.g. a future retry-on-duplicate-webhook scenario), and the fail-open behavior on DB errors means a rate-limiter outage silently disables referral rate limiting entirely rather than blocking (safely) until the DB recovers.
- **User/business impact:** low under current single-invocation-per-event usage; the inconsistency is a design smell that could bite if the call pattern ever changes.
- **Minimal safe remediation:** check-then-increment (or increment only after confirming the call will not be blocked) for the hourly counter; make the rate-limiter's own failure mode fail closed to match the other referral guards, or explicitly document why fail-open is intentional here.
- **Tests required:** already partially covered by `test_referral_rate_limit.py`; add a test for the DB-error fail-open path making an explicit assertion about intended behavior.
- **Production verification:** not needed — behavior confirmed by existing test.
- **Rollback considerations:** low risk, logic-only change.

---

### [DEP-01] Pinned dependencies have known CVEs, including a CORS-bypass class directly relevant to this app's wildcard CORS usage

- **Severity:** Medium
- **Confidence:** Confirmed (`pip-audit -r requirements.txt`)
- **Category:** dependency vulnerability
- **Evidence:**
  - `flask-cors==4.0.0` — 5 advisories, including **PYSEC-2026-1383/1384/1385** (regex-based path-matching bypass allowing CORS policy to be applied to the wrong path, and case-insensitive path matching) and **PYSEC-2024-260/271** (private-network header defaulted on; log-injection via CRLF in request path). Fix: ≥4.0.2 (or 6.0.0 for the 2026 CVEs).
  - `gunicorn==21.2.0` — **PYSEC-2026-1433/1434**, HTTP request smuggling via inconsistent `Transfer-Encoding` handling, allowing bypass of endpoint restrictions. Fix: ≥22.0.0.
  - `pymongo==4.6.1` — PYSEC-2026-1826. Fix: 4.6.3.
  - `flask==3.0.3` — PYSEC-2026-2151. Fix: 3.1.3.
- **User/business impact:** the gunicorn request-smuggling CVE is particularly relevant given this app sits behind Fly's proxy — request smuggling could in principle be used to reach endpoints intended to be restricted at a proxy layer, compounding the missing-auth issues in SEC-01.
- **Minimal safe remediation:** bump `flask-cors` to ≥4.0.2 (ideally 6.0.0), `gunicorn` to ≥22.0.0, `pymongo` to ≥4.6.3, `flask` to ≥3.1.3; re-run the full test suite after each bump (fix TEST-04's missing `app.app_context()` wrappers first so that pass gives a clean signal).
- **Tests required:** full regression pass after the bump.
- **Production verification:** not applicable — this is a static dependency fact.
- **Rollback considerations:** Flask 3.0→3.1 and gunicorn 21→22 are typically safe minor/major bumps but should go through staging first.

---

### [DEAD-01] `bot_joins.py`, `referralbot.py`, `checkin.py` are confirmed-dead but implement materially different, superseded logic

- **Severity:** Low (as-is) / would be High if ever reconnected
- **Confidence:** Confirmed
- **Category:** legacy code / landmine
- **Evidence:** each file's own header comment states it is not wired into any live process; verified independently via exhaustive grep (no import, no `add_job`, no route registration, no `python <file>.py` in `Dockerfile`/`fly.toml`). `checkin.py` additionally computes "first checkin" via `first_checkin = not user` — a cruder heuristic that would misfire for pre-existing users, and treats naive datetimes as KL-local where the live `main.py` path treats them as UTC.
- **Minimal safe remediation:** delete these files (or move to an `archive/` directory clearly out of the import path) rather than leaving them live-but-dormant in the main source tree, since their mere presence and plausible-looking function names invite accidental reuse.
- **Tests required:** none (deletion is safe given confirmed zero callers); if kept for reference, add a CI guard that fails if any of these modules is ever imported by a runtime module.
- **Production verification:** not applicable.
- **Rollback considerations:** trivial to restore from git history if ever needed.

---

### [DEAD-02] `app_context.py` contains a second, unused `BackgroundScheduler`/`CronTrigger` bootstrap

- **Severity:** Low
- **Confidence:** High confidence (no current caller found; a full call-graph proof would require checking every import of `app_context.py`'s scheduler helper across the whole repo, which this audit did to the extent grep allows but cannot certify at 100%)
- **Category:** legacy code / landmine
- **Files/functions:** `app_context.py:105-137`.
- **Minimal safe remediation:** remove it, or add a code comment/assertion making explicit that it must never be called alongside `main.py`'s own `BackgroundScheduler` construction (to avoid ever running two independent schedulers against the same `scheduler_locks` collection, which the lock design assumes won't happen).
- **Production verification:** grep the full repo (including any downstream forks/scripts not in this audit's scope) for callers before deleting.

---

### [DUP-01] `/api/welcome-progress/<user_id>` is registered twice (root app + `/v2/miniapp` blueprint)

- **Severity:** Low
- **Confidence:** Confirmed
- **Category:** dead/duplicate routing
- **Files/functions:** `main.py:6808-6819` and `vouchers.py:1634-1636` — both call the identical `build_welcome_progress_response`, so this is redundant routing, not a logic fork, but it doubles the surface area of **SEC-01**'s exposure under two distinct URL paths that must both be fixed together.
- **Minimal safe remediation:** remove one registration (keep whichever the frontend actually calls — confirmed to be the bare `/api/welcome-progress/<user_id>` path, `static/index.html:3634`).

---

### [TEST-01] Systemic "AST-extract single function + hand-built fake globals" test pattern masks missing-import bugs

- **Severity:** Medium
- **Confidence:** Confirmed
- **Category:** test-gap
- **Evidence:** 8 test files use `ast.parse` + `compile` + `exec` to pull a single function's source out of `main.py`/`scheduler.py` and run it against a manually-constructed globals dict (`test_referral_ack_dm.py`, `test_referral_near_miss_dm.py`, `test_referral_qualified_dm.py`, `test_referral_status_api.py`, `test_share_rank_caption.py`, `test_start_referral_button.py`, `test_start_referral_deeplink.py`, `test_start_referral_url.py`, `test_welcome_pm_flow.py`), confirmed via `bandit`'s `B102 exec_used` scan.
- **Why this is a real test gap:** this pattern is exactly how **REF-01** (missing `pm_allowed` import) survived undetected — the test supplies the exact global name the real module is missing, so the test can never fail for that reason. It's a reasonable technique for isolating logic from a 9,000+ line monolith, but it needs to be paired with a cheap import-level smoke test that would catch what it structurally cannot.
- **Minimal safe remediation:** add a basic `import main` / `import scheduler` smoke test (or a `ruff --select=F821` CI gate) so a genuinely undefined name at module scope is always caught, independent of the AST-isolation tests.
- **Tests required:** the smoke test itself.
- **Production verification:** not applicable.

---

### [TEST-02] Test-only dependencies (`mongomock`) are not declared anywhere, so a clean checkout cannot run ~8 test files

- **Severity:** Low-Medium
- **Confidence:** Confirmed (reproduced live)
- **Category:** test infrastructure
- **Evidence:** `requirements.txt` has no `mongomock`/test-requirements file; a fresh `pip install -r requirements.txt` followed by `pytest` fails to even **collect** `test_checkin_streak_integrity.py`, `test_official_channel_reopen_audit.py`, `test_referral_channel_migration.py`, `test_referral_error_hardening.py`, `test_weekly_referral_post_integration.py`, `test_welcome_checkin_progress_main.py`, `test_welcome_run_stats_telemetry.py` (7 files) with `ModuleNotFoundError: No module named 'mongomock'`.
- **User/business impact:** anyone (including CI, if CI doesn't have an undocumented separate dependency list) attempting to validate this codebase from the committed `requirements.txt` alone gets a false "everything's fine" signal for ~7 files worth of tests that simply never ran.
- **Minimal safe remediation:** add a `requirements-dev.txt` (or `requirements-test.txt`) listing `pytest`, `mongomock`, and any other test-only packages, and document its use in CI/README.
- **Tests required:** none — this is itself the fix.
- **Production verification:** not applicable.

---

### [TEST-03] `test_ugc_growth_referral.py` cannot be collected — imports a function that no longer exists in `scheduler.py`

- **Severity:** Low
- **Confidence:** Confirmed (reproduced live)
- **Category:** test-gap / dead test
- **Evidence:** `test_ugc_growth_referral.py:5` does `from scheduler import _eligible_referrer_tiers`; `_eligible_referrer_tiers` does not exist anywhere in `scheduler.py` (confirmed by exhaustive grep) — the test fails at collection with `ImportError`, meaning **zero** of its assertions have run for however long this has been broken.
- **User/business impact:** whatever behavior this file was meant to protect (UGC growth-referral tier eligibility) currently has no working regression coverage, silently.
- **Minimal safe remediation:** update the import to match the current function name/location (or delete the file if the feature itself was removed and this is genuinely obsolete).
- **Tests required:** the fix itself; add this file to whatever CI collection-health check would have caught this immediately (see TEST-01's smoke-test recommendation, extended to test collection).
- **Production verification:** not applicable.

---

### [TEST-04] A cluster of `test_vouchers.py` tests omit `app.app_context()` and fail outside a fresh install; `requirements.txt` also lacks a full transitive-dependency lockfile

- **Severity:** Low
- **Confidence:** Confirmed (reproduced live; root cause corrected after review — see note below)
- **Category:** test infrastructure
- **Evidence:** `_atomic_claim_pooled_voucher` (`vouchers.py:5030`) calls `current_app.logger.info(...)` unconditionally. Several `VoucherAntiHunterTests` methods that exercise this path — e.g. `test_claim_pooled_prefers_my_pool_then_public`, `test_claim_internal_drop_without_ledger_rejected`, `test_claim_new_joiner_drop_requires_subscription`, `test_claim_pooled_my_pool_decrements_when_available`, `test_claim_pooled_non_my_uses_public_only` — call `claim_pooled(...)` directly with **no** `app.app_context()`/`Flask(__name__).app_context()` wrapper, unlike sibling tests in the very same file that correctly do (`test_vouchers.py:415,460,519,571,899,947,995,...`). `current_app` requires an active Flask application context by design, regardless of Flask/Werkzeug version — so these specific tests fail with `RuntimeError: Working outside of application context` purely because of an inconsistent test setup, not a dependency-version issue. *(An earlier draft of this finding incorrectly attributed this failure to an unpinned `Werkzeug` version; that attribution was wrong and has been corrected here — the failure reproduces identically regardless of Werkzeug version, since `current_app`'s app-context requirement is fundamental Flask behavior.)*
- **User/business impact:** these specific tests provide no real coverage of `claim_pooled`'s atomic-claim path in a fresh environment (they error before any assertion runs) — a narrower version of the same "did we actually run this test" concern raised elsewhere in this audit's TEST section. Separately, `requirements.txt` still has no full lockfile for transitive dependencies, so other environment-drift-induced failures (distinct from this specific `current_app` case) remain possible and shouldn't be assumed to be real regressions without individual triage.
- **Minimal safe remediation:** add the missing `app.app_context()` wrapper to the affected test methods (matching their siblings in the same file); separately, and independently of this specific bug, add a lockfile/constraints file for the full transitive dependency set so future triage can distinguish real regressions from environment drift for *other* failures.
- **Tests required:** the fix itself (wrap the affected tests in `app.app_context()`); confirm they then exercise and assert on the real `_atomic_claim_pooled_voucher` behavior.
- **Production verification:** not applicable — this is a test-only defect, not a production code or runtime issue.

---

### [MONGO-03] `growth_leaderboard_posts.week_key` upsert has no backing unique index

- **Severity:** Low
- **Confidence:** Confirmed
- **Category:** data integrity
- **Files/functions:** `scheduler.py:1378-1391` (upsert on `{"week_key": week_key}` with `$setOnInsert`), no unique index on `week_key` found anywhere for this collection.
- **Exact failure scenario:** a concurrent write outside the normal upsert path (e.g. a manual repair insert) could create a second document with the same `week_key`, and the "already posted" dedup `find_one` check (`scheduler.py:1338-1341`) would then non-deterministically see only one of the two.
- **Minimal safe remediation:** add a unique index on `week_key` for `growth_leaderboard_posts` (moot if DEAD/SCHED-02's recommendation to delete the job is taken instead).
- **Production verification:** `db.growth_leaderboard_posts.aggregate([{"$group":{"_id":"$week_key","count":{"$sum":1}}},{"$match":{"count":{"$gt":1}}}])`.

---

### [MONGO-04] KL timezone object defined independently in 6+ files (`pytz` vs `zoneinfo`) — no live bug found, but a landmine for future edits

- **Severity:** Low
- **Confidence:** Confirmed (verified the specific pytz-direct-tzinfo-assignment gotcha does NOT currently occur anywhere in this codebase — empirically reproduced the bug pattern in isolation to confirm what it would look like, then confirmed no call site combines the unsafe `.replace(tzinfo=KL_TZ)`/`datetime(...,tzinfo=KL_TZ)` idiom with a pytz-typed `KL_TZ`)
- **Category:** maintainability
- **Files/functions:** canonical definition `config.py:9` (`ZoneInfo("Asia/Kuala_Lumpur")`, used correctly with direct-tzinfo-assignment in 6 files); independently redefined as `pytz.timezone("Asia/Kuala_Lumpur")` in `scheduler.py:785`, `database.py:11`, `affiliate_rewards.py:18`, `affiliate_voucher_batches.py:32`, `community_centre.py:54`, `app/rebuild_snapshots_from_ledger.py:10` — all of which currently and correctly use `.localize()`/`.astimezone()` rather than direct assignment.
- **Exact risk:** `pytz.timezone(...)` objects require `.localize()` discipline; naively writing `datetime(...).replace(tzinfo=KL_TZ)` against a pytz object silently produces the zone's historical/LMT offset (verified: **+06:55** instead of **+08:00**, a 65-minute error) rather than raising an error — and because the same variable name (`KL_TZ`) refers to two structurally different objects depending on which file you're in, a reviewer cannot tell from a call site alone which discipline applies.
- **Minimal safe remediation:** consolidate on `config.KL_TZ` (the `ZoneInfo` version) everywhere and delete the six independent `pytz.timezone(...)` re-definitions.
- **Production verification:** not needed — this is a structural risk, not a currently-manifesting bug.

---

## 4. Cross-System Inconsistencies

| # | Inconsistency | Files | Risk |
|---|---|---|---|
| 1 | Welcome voucher: v2 issuance ledger (`affiliate_ledger`/`voucher_pools`) vs. legacy claim system (`db.drops`/`db.vouchers`) the frontend actually reveals codes through | `affiliate_rewards.py`, `vouchers.py`, `static/index.html` | High — WELCOME-01 |
| 2 | Referral qualification logic spread across `referral_rules.py`, `scheduler.py` (settlement), `affiliate_rewards.py` (`mark_invitee_qualified`), and `ugc_growth_referral.py` (`is_expansion_qualified`) — three/four independently-maintained "is this referral good" concepts | multiple | Medium — confirm they can't silently diverge on edge cases |
| 3 | Two independently-computed "leaderboards": `affiliate_leaderboard.py` computes live off ledger-adjacent event collections; `main.py`'s user-facing rank/tier reads the periodically-rebuilt `users` snapshot fields — not guaranteed to agree at any instant (up to one 5-minute scheduler cycle of lag) | `affiliate_leaderboard.py`, `main.py` | Low-Medium — documented staleness, not a bug, but worth knowing which one is "the" number in any given report |
| 4 | `xp.grant_xp()` is the single authoritative XP-grant path everywhere **except** `referral_rules.grant_referral_rewards()`, which writes directly to `xp_events`, bypassing the `no_xp` restriction check and the `xp_ledger` write — currently reachable only from the manual `backfill_referrals.py` CLI, not the live request path | `referral_rules.py`, `xp.py`, `backfill_referrals.py` | Low (manual-only today) but a real landmine if this function is ever called from a live path |
| 5 | Legacy `db.referrals`/`xp_ledger`(old schema)-oriented tooling (`reconcile_referrals.py`, `backfill_referrals.py`, `rollback_pending_referral_xp.py`) operates on a schema that predates `pending_referrals`/`referral_events`/`referral_invitee_locks`/`qualified_events` — running these against current data would not understand the current schema's guards | multiple manual scripts | Medium — operational risk if run by someone unfamiliar with which era of tooling applies |
| 6 | KL week/month boundary computation duplicated independently in `scheduler.py`, `repair_referral_ledger.py`, and `rollback_pending_referral_xp.py` rather than shared from one function | 3 files | Low — drift risk if the week/month definition (e.g. Monday-start) ever changes in one but not the others |
| 7 | `KL_TZ` defined as `ZoneInfo` in `config.py` (canonical) vs. independently as `pytz.timezone(...)` in 6 other files | see MONGO-04 | Low today, landmine for future edits |
| 8 | `PENDING_MANUAL` (affiliate T1-T4) self-heals via a retry sweep; the structurally identical `OUT_OF_STOCK` (WELCOME) does not | `affiliate_rewards.py` | High — WELCOME-02 |
| 9 | `WELCOME_WINDOW_HOURS` (48h) is defined in both `vouchers.py` and `main.py` but is dead/unused — the real, live window is the 7-day `WELCOME_UNCLAIMED_WINDOW_DAYS` constant | `vouchers.py:231`, `main.py:116` | Low — confusing dead constant, not a live bug |
| 10 | `_admin_secret_ok` (plain `==`) vs. `_has_valid_admin_secret` (constant-time `hmac.compare_digest`) — two comparison functions for conceptually the same secret | `vouchers.py` | Medium — SEC-02 |

---

## 5. Data Reconciliation Queries (read-only / dry-run)

All queries are read-only `find`/`aggregate` calls; none mutate data. Run via `mongosh` or `pymongo` against a read replica where possible.

```javascript
// 1. Negative referral counts (should never happen — see REF-02, MONGO clamp gaps)
db.users.find({ "total_referrals": { $lt: 0 } }, { user_id: 1, total_referrals: 1, weekly_referrals: 1, monthly_referrals: 1 });
db.users.find({ $or: [ { weekly_referrals: { $lt: 0 } }, { monthly_referrals: { $lt: 0 } } ] });

// 2. Duplicate rewarded invitees — same invitee_id qualified/awarded more than once
db.qualified_events.aggregate([
  { $group: { _id: "$invitee_id", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } }
]);
db.referral_award_events.aggregate([
  { $group: { _id: "$invitee_user_id", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } }
]);

// 3. Settled referrals without a corresponding qualified_events row
db.referral_events.aggregate([
  { $match: { event: "referral_settled" } },
  { $lookup: { from: "qualified_events", localField: "invitee_id", foreignField: "invitee_id", as: "q" } },
  { $match: { q: { $size: 0 } } }
]);

// 4. Qualified events without a matching referral_settled ledger row
db.qualified_events.aggregate([
  { $lookup: { from: "referral_events", let: { iid: "$invitee_id" },
      pipeline: [ { $match: { $expr: { $and: [ { $eq: ["$invitee_id", "$$iid"] }, { $eq: ["$event", "referral_settled"] } ] } } } ],
      as: "settled" } },
  { $match: { settled: { $size: 0 } } }
]);

// 5. referral_events ledger vs. users snapshot divergence (sample-based; full check requires the aggregation used by settle_referral_snapshots)
db.referral_events.aggregate([
  { $match: { event: { $in: ["referral_settled", "referral_revoked"] }, invalidated_at: { $exists: false } } },
  { $group: { _id: "$inviter_id", ledger_total: { $sum: { $cond: [ { $eq: ["$event", "referral_settled"] }, 1, -1 ] } } } },
  { $lookup: { from: "users", localField: "_id", foreignField: "user_id", as: "u" } },
  { $unwind: "$u" },
  { $project: { ledger_total: 1, snapshot_total: "$u.total_referrals", diff: { $subtract: ["$ledger_total", "$u.total_referrals"] } } },
  { $match: { diff: { $ne: 0 } } }
]);

// 6. Duplicate XP grants — same (user_id, unique_key) appearing more than once (would indicate the dedupe index failed/was bypassed)
db.xp_events.aggregate([
  { $group: { _id: { user_id: "$user_id", unique_key: "$unique_key" }, n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } }
]);

// 7. xp_ledger / xp_events mismatch (an xp_ledger row with no corresponding xp_events row, or vice versa)
// Must match on the full (user_id, source/type, source_id/unique_key) identity, not just user_id,
// and must check both directions independently.
db.xp_ledger.aggregate([
  { $lookup: { from: "xp_events", let: { uid: "$user_id", src: "$source", sid: "$source_id" },
      pipeline: [ { $match: { $expr: { $and: [
          { $eq: ["$user_id", "$$uid"] },
          { $eq: ["$type", "$$src"] },
          { $eq: ["$unique_key", "$$sid"] }
      ] } } } ], as: "ev" } },
  { $match: { ev: { $size: 0 } } },
  { $limit: 200 }
]);
db.xp_events.aggregate([
  { $lookup: { from: "xp_ledger", let: { uid: "$user_id", typ: "$type", ukey: "$unique_key" },
      pipeline: [ { $match: { $expr: { $and: [
          { $eq: ["$user_id", "$$uid"] },
          { $eq: ["$source", "$$typ"] },
          { $eq: ["$source_id", "$$ukey"] }
      ] } } } ], as: "led" } },
  { $match: { led: { $size: 0 } } },
  { $limit: 200 }
]);

// 8. Issued vouchers without a corresponding ledger row (affiliate/welcome)
db.affiliate_ledger.find({ status: "ISSUED", $or: [ { voucher_code: { $exists: false } }, { voucher_code: null }, { voucher_code: "" } ] });

// 9. Ledgers marked ISSUED but voucher_code missing (same as above, explicit welcome scope)
db.affiliate_ledger.find({ ledger_type: "WELCOME", status: "ISSUED", voucher_code: { $in: [null, ""] } });

// 10. Voucher codes assigned to more than one user (should be impossible given atomic claim — verifies that guarantee)
db.vouchers.aggregate([
  { $match: { status: "claimed" } },
  { $group: { _id: "$code", users: { $addToSet: "$claimedByUserId" }, n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } }
]);

// 11. Welcome rewards issued to ineligible users
// NOTE: issue_welcome_bonus_if_eligible() (affiliate_rewards.py) verifies channel membership by
// calling Telegram's getChatMember directly (_is_official_channel_subscribed) and does NOT write to
// channel_subscription_cache (that collection is populated by a different, dormant subscription-audit
// path) or to vouchers.py's separate subscription_cache_col. Neither cache is evidence of what the
// issuance path actually checked, so a query keyed on either cache produces false positives (flags
// legitimately-issued rewards en masse) rather than a reliable signal. Treat any output as an
// INCONCLUSIVE candidate list requiring a live getChatMember spot-check per user, not a confirmed
// violation list:
db.affiliate_ledger.aggregate([
  { $match: { ledger_type: "WELCOME", status: "ISSUED" } },
  { $lookup: { from: "subscription_cache", localField: "user_id", foreignField: "user_id", as: "sub" } },
  { $match: { sub: { $size: 0 } } }
]);
// Prefer, if available, cross-checking against referral_audit / self-invite flags
// (is_user_blocked_for_self_invite) and the WELCOME dedup_key's own created_at vs. the user's
// joined_main_at window, which are the actual gates issue_welcome_bonus_if_eligible enforces.

// 12. Expired batch inventory still being issued from (cross-check issued_at against the batch window;
// affiliate_voucher_batches documents store starts_at/ends_at, NOT window_start/window_end)
db.voucher_pools.aggregate([
  { $match: { status: "issued" } },
  { $lookup: { from: "affiliate_voucher_batches", localField: "batch_id", foreignField: "_id", as: "batch" } },
  { $unwind: "$batch" },
  { $match: { $expr: { $or: [
      { $lt: ["$batch.ends_at", "$issued_at"] },
      { $gt: ["$batch.starts_at", "$issued_at"] }
  ] } } }
]);

// 13. Stuck processing/settling records
db.pending_referrals.find({ status: "processing", processing_at_utc: { $lt: new Date(Date.now() - 10*60*1000) } });
db.affiliate_ledger.find({ status: "SETTLING", updated_at: { $lt: new Date(Date.now() - 15*60*1000) } });
db.voucher_claims.find({ status: "claimed_pending_code", created_at: { $lt: new Date(Date.now() - 30*60*1000) } });   // see VOUCHER-01
db.growth_leaderboard_posts.find({ status: "posting" });                                                              // see SCHED-02
db.weekly_referral_posts.find({ status: "sending", attempted_at: { $lt: new Date(Date.now() - 10*60*1000) } });

// 14. Scheduler locks that appear stale (held past their expiry with no matching activity — investigate rather than clear)
db.scheduler_locks.find({ expireAt: { $lt: new Date() } });   // TTL monitor should clear these; a persistent presence indicates something's wrong
db.scheduler_locks.find({});  // review "owner"/"updatedAt" fields for locks that look abnormally long-held relative to their job's expected cadence

// 15. Duplicate username collisions (see MONGO-01)
db.users.aggregate([
  { $group: { _id: { $toLower: "$username" }, count: { $sum: 1 }, ids: { $push: "$user_id" } } },
  { $match: { count: { $gt: 1 } } }
]);
```

A companion read-only Python script pattern (for any of the above requiring pagination) should reuse `sync_referral_counts.py`'s existing `--dry-run` default and batch-cursor pattern rather than writing a new one from scratch.

---

## 6. Test Gaps — Critical Flow Coverage Map

| Flow | Existing coverage | Missing / weak coverage |
|---|---|---|
| Onboarding / welcome eligibility (v2) | Strong — `test_welcome_v2_progress.py`, `test_welcome_progress_journey_v2.py`, `test_welcome_pending_visibility.py`, `test_welcome_self_invite_guard.py` all exercise real production functions against `fake_mongo`/`mongomock` | **No test asserts the frontend-visible legacy `db.drops` card exists whenever the v2 ledger reports `status="issued"`** — this is exactly the seam where WELCOME-01 lives. No test covers `OUT_OF_STOCK` → restock → recovery (because no recovery path exists — WELCOME-02). |
| Referral settlement/idempotency | Strong — `test_referral_ledger_integrity.py`, `test_referral_snapshot_negative_guard.py`, `test_repair_referral_ledger_cli.py`, `test_referral_telegram_membership.py` import real `scheduler.py` functions | No test exercises the **stacked-bug scenario** in REF-01 (guard-fixed-without-import-fixed); no test for lock-expiry-during-still-running-job (SCHED-01); `test_ugc_growth_referral.py` cannot even collect (TEST-03). |
| XP idempotency | Strong — `test_xp.py`, `test_xp_snapshot.py` import real `xp.py`/`xp_snapshot.py` | No test for the cross-event-type `unique_key` collision described in XP-01. |
| Voucher/reward allocation atomicity | Strong — `test_voucher_pool_service.py` has explicit concurrency tests (`test_allocate_voucher_is_atomic_and_idempotent`, `test_concurrent_double_allocation_on_same_campaign_pool_never_double_issues`) against a `DuplicateKeyError`-faithful fake Mongo | No test for the crash-between-lock-and-issuance scenario in VOUCHER-01 (stuck `claimed_pending_code`). |
| Scheduler duplicate-execution | Partial — `acquire_scheduler_lock`'s atomicity is implicitly relied on but I found no dedicated test simulating "lock TTL expires while job is still genuinely running" | Add: TTL-expiry-during-run test for `tick_5min`; crash-recovery test for `post_growth_leaderboard_weekly`'s stuck `"posting"` state (currently untested because the job has no recovery to test). |
| Authentication substitution | **Missing entirely** — no test asserts that `welcome-progress`/`set-region`/`checkin`/`streak`/`region-status`/`api/referral` reject a `user_id` that doesn't match a verified identity, because currently none of them even attempt to check | This is the single highest-priority test gap in the codebase — see SEC-01 remediation. |
| Race-condition / retry / restart tests | Present for referral settlement (`_recover_stale_processing`) and affiliate ledger `SETTLING` reclaim; **absent** for `voucher_claims.claimed_pending_code` (no sweep exists to test) and for the legacy growth-leaderboard job | Add sweeps + tests once VOUCHER-01/SCHED-02 remediations land. |
| Time-window boundary tests | Present for KL week/month boundaries in the referral snapshot path (`test_scheduler_referral_snapshot_logging.py`) | Add a boundary test for the 24h vs 5-min vs 30-min initData/admin-login freshness windows described in the auth audit, and one confirming the three independent KL-boundary implementations (MONGO-04's cross-cutting entry #6) agree. |

---

## 7. Prioritized Remediation Plan

### P0 — Fix immediately (unauthorized access, double rewards, voucher leakage, incorrect settlements, data corruption, outage risk)

| # | Item | Impact | Difficulty | Regression risk | Depends on |
|---|---|---|---|---|---|
| 1 | **SEC-01** — add authenticated-identity checks to `welcome-progress` (both registrations), `set-region`, `checkin`, `streak`, `checkin-status`, `region-status`, `api/referral` | Critical — closes the account-impersonation/voucher-leak hole | Medium (7 routes, same pattern each time, `resolve_authenticated_telegram_user_id` already exists as the template) | Low if the Mini App frontend already sends `init_data` on these calls (verify first) | none |
| 2 | **WELCOME-01** — link the v2 welcome ledger's `voucher_code` directly into the frontend response/UI so a user is never "issued" server-side with no way to see the code | High — stops silent reward non-delivery | Low-Medium (frontend rendering change) | Low | none |
| 3 | **REF-01** — add the missing `pm_preferences` import AND correct the `RUNNER_MODE` guard together, as one change, with a new integration test proving both are consistent | High-latent (currently dormant, but a ticking regression) | Low | Low, but must be done as **one** atomic fix, not sequential (fixing the guard alone before the import is the exact scenario that triggers the crash) | none |
| 4 | **WELCOME-02** — add a retry/backfill path for `OUT_OF_STOCK` WELCOME ledger rows, mirroring the existing `PENDING_MANUAL` sweep | High | Medium (reuse existing affiliate-ledger sweep pattern) | Low | none |
| 5 | **SCHED-01** — add explicit lock release on job completion; re-size `tick_5min`'s TTL (or cap its per-run Telegram-call budget) so it comfortably exceeds worst-case runtime | High | Low-Medium | Low | none |

### P1 — Fix next (reliability, reconciliation, abuse, major observability gaps)

| # | Item | Impact | Difficulty | Regression risk | Depends on |
|---|---|---|---|---|---|
| 6 | **VOUCHER-01** — add a stale `claimed_pending_code` reclaim sweep | Medium-High (prevents permanent user lockout) | Low-Medium | Low | none |
| 7 | **SEC-02** — switch `_admin_secret_ok` to `hmac.compare_digest` | Medium | Trivial | None | none |
| 8 | **MONGO-01** — enforce/verify `users.username` uniqueness before `update_user_xp` trusts a username-only lookup | Medium | Medium (needs a data-collision check first) | Medium if collisions already exist in prod (needs a decision on merge/reject) | run the MONGO-01 verification query first |
| 9 | **MONGO-02** — route all of `ensure_indexes()` through `safe_create_index`/`_ensure_equivalent_index` | Medium (prevents a startup-crash class that has already happened once) | Low | Low | none |
| 10 | **REF-02** — add `max(0, ...)` clamp to `sync_referral_counts.py` | Medium | Trivial | None | none |
| 11 | **SCHED-02** — delete or harden the legacy `post_growth_leaderboard_weekly` job's error handling | Medium | Low (delete) / Medium (harden) | Low | confirm it's genuinely unused in prod config first |
| 12 | **SCHED-03** — explicitly set `RUNNER_MODE=web` in `fly.toml`'s `web` process command | Medium | Trivial | Low | none |
| 13 | **SEC-03** — scope CORS away from wildcard for user-data routes | Medium | Medium (needs to enumerate the real Mini App origin(s)) | Medium if the Mini App is ever loaded from an origin not on the allowlist | do after SEC-01, since it's an amplifier not the root cause |
| 14 | **DEP-01** — bump `flask-cors`, `gunicorn`, `pymongo`, `flask` to patched versions | Medium | Low-Medium | Medium (full regression pass needed) | fix TEST-04's missing app-context wrappers first so the regression pass is meaningful |
| 15 | **XP-01** — namespace `xp_events.unique_key` by event type | Medium | Medium (index/migration) | Medium | none |
| 16 | **TEST-01** — add an import-level smoke test / `ruff --select=F821` CI gate | Medium (prevents recurrence of REF-01-class bugs) | Trivial | None | none |
| 17 | **TEST-02/TEST-04** — add `requirements-dev.txt` with `mongomock`; fix the missing `app.app_context()` wrappers in the affected `test_vouchers.py` tests | Low-Medium | Trivial | None | none |
| 18 | **TEST-03** — fix or delete `test_ugc_growth_referral.py` | Low | Trivial | None | none |

### P2 — Improve later (maintainability, performance, lower-impact UX)

| # | Item | Impact | Difficulty |
|---|---|---|---|
| 19 | **DEAD-01/DEAD-02** — delete `bot_joins.py`, `referralbot.py`, `checkin.py`, the unused `app_context.py` scheduler bootstrap | Low | Trivial |
| 20 | **DUP-01** — remove the duplicate `/api/welcome-progress` route registration | Low | Trivial |
| 21 | **MONGO-03/MONGO-04** — add missing `week_key` unique index; consolidate `KL_TZ` on the `ZoneInfo` definition | Low | Low |
| 22 | **REF-03** — remove/fix the dead `referral_invite_link` fallback field mismatch | Low | Trivial |
| 23 | **REF-04** — make the rate limiter check-before-increment and decide its fail-open/closed posture deliberately | Low | Low |
| 24 | Frontend: audit the 794 `innerHTML` usages and add `AbortController`-based fetch timeouts (currently absent across nearly all Mini App network calls) | Low-Medium | Medium |

---

## Ten Highest-Value Fixes (final list, in do-first order)

1. **SEC-01** — Authenticate `welcome-progress`, `set-region`, `checkin`, `streak`, `checkin-status`, `region-status`, `api/referral` against verified Telegram initData instead of trusting a client-supplied `user_id`.
2. **REF-01** — Fix the missing `pm_preferences` import and the inverted `RUNNER_MODE` guard together, as a single atomic change, with a new end-to-end test.
3. **WELCOME-01** — Make the frontend render the v2 ledger's `voucher_code` directly, decoupling reward delivery from the legacy `db.drops` claim-card system.
4. **WELCOME-02** — Add a retry/backfill sweep for `OUT_OF_STOCK` WELCOME ledger rows.
5. **SCHED-01** — Add explicit scheduler-lock release on success and re-size `tick_5min`'s TTL/Telegram-call budget so it can't be outrun by its own worst case.
6. **VOUCHER-01** — Add a stale-`claimed_pending_code` reclaim sweep to prevent permanent per-user claim lockout after a crash.
7. **MONGO-02** — Route all `ensure_indexes()` calls through the existing safe-index-creation helper to close off a startup-crash class that has already occurred once in production.
8. **SEC-02** — Switch the admin-secret comparison to `hmac.compare_digest`.
9. **MONGO-01** — Verify and, if needed, enforce `users.username` uniqueness before trusting username-only admin XP lookups.
10. **TEST-01/TEST-02/TEST-04** — Add an import-level smoke test / static-analysis CI gate, a proper test-dependency setup (`mongomock`), and fix the missing `app.app_context()` wrappers in the affected `test_vouchers.py` tests, so the next `pm_allowed`-class bug and the next silently-not-running test are both caught automatically instead of by manual audit.
