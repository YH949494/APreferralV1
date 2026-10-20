# 1. Admin feature inventory and workflow audit (Phases 1 + Deliverable 1)

## 1.0 Conventions

- **Interactions (est.)** = clicks + each required field edit/select/paste + submit + each confirmation, along the *shortest valid* path. Opening a module = 1 click (lands on its first tab); every other tab = +1. Scrolling and typing length are not counted. **Estimates from code reading, not measurements.**
- **Trace depth**: ● code-traced end to end · ○ route + form-field level only.
- **Auth** (all rows unless noted): one flat gate — `require_admin()` (`vouchers.py:4768`) / `require_admin_from_query()` (`main.py:2725`) / `_admin_api_authorized()` (`admin_auth.py`). Accepts **any one** of: Telegram-login session cookie, Telegram-ID allowlist (`admin_cache`), or the shared `ADMIN_PANEL_SECRET`. **No roles**, so every admin can do everything, including money-moving actions. (`BYPASS_ADMIN` is hard-set `False` at `vouchers.py:529`.) RBAC is out of scope here (see `docs/ADMIN_SAFETY_ARCHITECTURE.md`).
- **Audit trail**: several modules write `campaign_admin_audit_log` (campaign_centre, providers, event banners, lucky games, mission pool, tournament rewards, campaign display). Voucher drops (except deletion → `drop_deletion_audit`), affiliate approve/reject, XP, and Builder launch have **no** per-action admin audit row in the code inspected.
- *Where today* uses `Module › Tab` from `MODULES` (`admin-dashboard.js:12513`). "Legacy" = the admin panel inside the player Mini App (`static/index.html:1848`).

## 1.1 Inventory by module

### Dashboard (Home)

| # | Workflow | Where today | Inputs | Int. | API | Collections | Notes / mistakes |
|---|---|---|---|---|---|---|---|
| D1 ● | Executive overview | Dashboard › Overview | window 7d/30d/all | 1 | `GET /api/admin/dashboard/summary`, `telegram-counts/cache|refresh` | users, referrals, vouchers… (cached panel) | Five card groups; no action buttons. |
| D2 ● | Attention Required + bell | Home; topbar 🔔 | — | 0 | `GET /v2/miniapp/admin/pools/summary`, `GET /api/admin/campaign-builder/campaigns?status=active`, `GET /v2/miniapp/admin/affiliate/pending` | voucher_pools, campaign_builder_campaigns, affiliate_ledger | 3 signals only; pool-low threshold hard-coded 10; pending capped at 200; Home rows not clickable. |
| D3 ○ | Live activity / audit | Dashboard › Live Activity | filters | 2 | `GET /api/admin/dashboard/audit` | admin_login_audit, audit_events, referral_audit | Named "audit" but it is login/events, not an admin-action log. |
| D4 ○ | Alerts (abuse overview) | Dashboard › Alerts | window | 2 | `GET /api/admin/dashboard/abuse` | claim/risk collections | Tab called "Alerts" opens a report, not alerts. |
| D5 ○ | System health | Dashboard › System Health ↗ | — | 2 | `GET /api/admin/dashboard/runtime-status` | runtime status | Opens a separate page in a new tab. |

### Campaign Centre (voucher-drop campaigns and audiences)

| # | Workflow | Where today | Inputs | Int. | API | Collections | Server validation / failure | Mistakes |
|---|---|---|---|---|---|---|---|---|
| C1 ● | Saved audience ("Legacy Targeting"): create/edit/archive/preview | Campaign Centre › Legacy Targeting | name*, type (12 enum, optional), segments, player age, claim-risk, referral/check-in ranges, recency, voucher value | ~8 | `POST/PUT/DELETE /api/admin/campaigns[/<id>]`, `POST …/preview` | campaigns | `missing name`, invalid type; archive via `confirm()` | Name is "Campaign" but it is an audience filter; `Save` does not require `Preview`. |
| C2 ● | **Campaign Builder**: draft → preview → launch | Campaign Centre › Templates | name*, type (9), audience mode (+params), release style/batch, reward type (+codes/assignments/XP), `LAUNCH` | typical ~16 · min ~11 | `GET …/campaign-builder/meta`, `POST/PUT …/campaigns`, `POST …/<id>/preview`, `POST …/<id>/compile` | campaign_builder_campaigns → **drops, vouchers** | `missing_campaign_name`, `invalid_campaign_type`, `confirmation_required`; compile warnings array | Free-text `YYYY-MM-DD HH:MM:SS`; steps are free-jump tabs (launch without preview); *Preview* persists the draft; XP-only compiles to nothing; VIP tier is free text defaulting `VIP1`. |
| C3 ● | Batch campaign control: pause/resume/cancel/release-next | Campaign Centre › Campaigns | button | 3 | `POST …/<id>/pause|resume|cancel|release-next` | campaign_builder_campaigns, drops | Cancel via native `confirm()`; released drops unaffected | "Cancel" leaves already-released drops live (message says so only in the confirm text). |
| C4 ○ | Active campaigns / compiled drops (read) | Campaign Centre › Campaigns, Voucher Centre › Voucher Pools | — | 2-3 | `GET …/campaign-builder/campaigns`, `…/<id>/analytics` | — | | Compiled drops appear in two modules. |
| C5 ○ | Performance, Intelligence (read) | Campaign Centre › Performance / Intelligence | filters | 2 | `GET …/campaign-builder/performance*`, `…/intelligence/*` | — | | |

### Player Campaigns (`gc_campaigns`: Tournament, Registration/Giveaway, External, Mission)

| # | Workflow | Where today | Inputs | Int. | API | Collections | Validation / failure | Mistakes |
|---|---|---|---|---|---|---|---|---|
| P1 ● | Create campaign (5-step wizard → draft) | Player Campaigns › Campaigns › + New | type card, name*, start*, end/"no end", registration fields or provider+path | **9** to draft | `POST /api/admin/gc-campaigns` | gc_campaigns | name/ID slug; end>start | Lands in *Detail* still needing destination + publish. Start has no default. |
| P2 ● | Publish / pause / archive / duplicate / delete / preview | Campaign Detail, list menu | button | 2–3 | `POST …/<id>/publish|pause|archive|duplicate`, `DELETE /api/admin/campaign-centre/campaigns/<id>`, `GET …/preview` | gc_campaigns (delete → tombstone) | Server publish gates: `mission_config_required`, `mission_pool_config_required`, `reward_rules_required`, `destination_not_ready`, `provider_inactive`, `provider_base_url_required`; delete only draft/archived/ended | Readiness is learned from the error code *after* clicking Publish. |
| P3 ● | **Mission**: create (4-step wizard), edit, lifecycle | Player Campaigns › Mission Reward Pool | name, ID, type, prompt, options, correct answer, pool (existing/new+codes), winners, eligibility ×5, schedule, selection mode, auto-draw/issue | ~14–18 | `POST/PUT gc-campaigns`, `POST /api/admin/mission-pool/<id>/close|cancel|resume|process|end-rewards|pause-auto-draw|resume-auto-draw|extend-deadline|draw-now`, `GET …/inventory-check|pools|edit-state|summary` | gc_campaigns, voucher_pools, mission_entries, campaign_rewards | Inventory gate (`winners ≤ available`) client **and** server-side publish gate | Eligibility checkboxes labelled with raw field names; "end"/"cancel"/"close" are three different buttons. |
| P4 ○ | Registration config / list / export | › Registrations | enable, mini-app visible, fields, subscription | ~6 | `GET/PUT … campaign-registration`, `GET /api/admin/campaign-registrations[/summary|/export]` | campaign_registration_state, campaign_registrations | | Separate tab from the campaign it configures. |
| P5 ○ | Deep links | › Deep Links | campaign | 3 | `GET /api/admin/deep-links/bot-username` | — | | |
| P6 ○ | Providers CRUD / activate | › Providers | provider_id, name, type, base URL, auth mode, secret *name* | ~8 | `…/api/admin/providers…` | gc_providers | audit-logged | `provider_id`, `CAMPAIGN_PROVIDER_SECRET_XXX` placeholders. |
| P7 ● | **Tournament results**: approve / reject / request correction / retry allocation | › Tournament Results | button (+reason) | 3–4 | `POST /api/admin/tournament-results/<id>/approve|reject|request-correction|retry-allocation` | tournament_results, campaign_rewards, voucher_pools | `not_pending_review`, `insufficient_stock` (409); audit-logged | **Approve allocates real vouchers**; reject reason optional via `prompt()`-style input. |
| P8 ○ | Reward pools: register, upload codes, migrate scope; allocations list | › Rewards | pool_id, name, type (8 enums), scope (6 enums), campaign link | ~7 | `GET/POST /api/admin/reward-pools`, `…/<id>/upload-codes`, `…/<id>/migrate-scope`, `GET /api/admin/rewards` | voucher_pools registry | Scope is immutable once codes exist | Enum values shown verbatim (`campaign_rewards`, `shared`). |
| P9 ○ | Verification status; Activity log + analytics | › Verification, › Activity Log | campaign_id | 2–3 | `/api/admin/campaign-events`, `/api/admin/campaign-analytics/summary` | campaign_events | | Needs raw `campaign_id` typed. |
| P10 ○ | Event banners CRUD/schedule | › Event Banner | event_id slug, image URL, destination URL, alt, priority, status, regions | ~8 | `/api/admin/event-banners…` | event_banners | delete via `confirm()`; audit-logged | No image preview; slug typed by hand. |
| P11 ○ | Lucky games CRUD, bulk weight, analytics | › Lucky Games | name, label, provider, volatility, max win, image/game URL, order, weight | ~9 | `/api/admin/lucky-games…`, `/api/admin/lucky-game/analytics` | lucky_games, lucky_game_events | delete `confirm()` | |

### Voucher Centre

| # | Workflow | Where today | Inputs | Int. | API | Collections | Validation / failure | Mistakes |
|---|---|---|---|---|---|---|---|---|
| V1 ● | **Create drop** (pooled/personalised) | Voucher Centre › Active Drops (form below the list) | name*, type, start*, end, priority, pool, codes | **6** | `POST /v2/miniapp/admin/drops` | drops, vouchers | `bad_request`, `bad_start/bad_end`, `end_before_start`, `empty_codes`, `duplicate_code` (409, **after** insert), end defaults to +24h | See F-2. Raw code in error; no preview; no confirm; ignores eligibility/regions/hero/whitelist. |
| V2 ● | Add codes to drop | same tab, second form | drop*, pool, codes | 5 | `POST …/drops/<id>/codes` | vouchers, drops (`*_remaining` counters) | `not_found`, `bad_type`, `bad_pool`, `empty_codes`, `duplicate_code` (409 partial insert), `drop_deleting` | Partial insert on duplicates; counters updated for the inserted subset. |
| V3 ● | Start / Pause / End drop | Active Drops row | button | 3 | `POST …/drops/<id>/actions` `{op}` | drops | End has `confirm()`; Start/Pause none | `start_now` resets end to now+24h. |
| V4 ● | **Edit dates / rename** (`set_dates`) | **Legacy panel only** (`index.html:6632`) | name, start, end | — | same `actions` route | drops | `end>start`, name non-empty | Not in the new dashboard. |
| V5 ● | **Delete drop** | **Legacy panel only** (`index.html:6683`) | confirm | — | `actions {op:"delete"}`, `POST …/drops/reconcile-deletions` (no UI) | drops, vouchers, voucher_claims, drop_deletion_audit | Blocked while any code is claimed/reserved/allocated/personalised; resumable, idempotent audit | Mission rows in the same table have their own Delete. |
| V6 ○ | Existing drops list (+ missions merged client-side) | Active Drops | — | 2 | `GET /v2/miniapp/admin/drops_v2`, `GET /api/admin/mission-pool/campaigns` | drops, vouchers, gc_campaigns | Expired drops hidden (`skip_expired`) | "Submissions/Winners" columns are `—` for standard drops. |
| V7 ○ | Voucher codes / claims overview | › Voucher Codes | window | 3 | `GET /api/admin/dashboard/vouchers` | vouchers, voucher_claims | | |
| V8 ○ | **Bonus voucher (global)** | **Legacy panel only** (`index.html:8022`) | code, release time | — | `POST /api/admin/set_bonus` | bonus_voucher (single doc, upsert `{}`) | Overwrites the one active code, +6h window, no diff | Silent overwrite. |
| V9 ○ | Rejoin-buffer settings | Settings (Voucher Rules) | mode, test user IDs | ~4 | `GET/POST /v2/miniapp/admin/rejoin-buffer/settings` | app_settings | | |

### Affiliate Centre

| # | Workflow | Where today | Inputs | Int. | API | Collections | Validation / failure | Mistakes |
|---|---|---|---|---|---|---|---|---|
| A1 ● | Affiliate overview + per-user ledger drill-down | Affiliate › Overview | filter text | 3–4 | `GET /api/admin/dashboard/affiliate`, `…/affiliate/detail?user_id=` | affiliate_ledger, voucher_pools, affiliate_voucher_batches | Table = 50 most recently updated; conversion always N/A | Cannot find a given affiliate unless recently updated. |
| A2 ● | **Approve / reject one reward** | Affiliate › Pending Approval | status tab; per row Approve / Reject | 4 / 4 | `POST /v2/miniapp/admin/affiliate/<id>/approve|reject` | affiliate_ledger, voucher_pools | approve: `confirm()`; 409 `affiliate_approve_refusal_reason`; may return `ok` but `issued:false` | **Reject: Cancel still rejects.** IDs only, no username. |
| A3 ● | Approve all / Reject all | same | button | 4 / 6 | `POST …/approve-bulk`, `…/reject-bulk` (max 200, per-item status re-check) | affiliate_ledger | approve-all `confirm()`; reject-all `confirm` + typed `REJECT n` + `prompt` | Operates on *loaded* ids only (≤200), not "all pending". |
| A4 ● | Replenish historical batch | Pending row (Out of Stock) | modal | ~5 | `GET/POST …/affiliate/<id>/historical-batch[/replenish]` | affiliate_voucher_batches, voucher_pools | locked, audited | Needs knowing which month/pool. |
| A5 ● | **Voucher Stock Required**: upload + retry-all | Pending Approval › Pending Manual | denomination, month, codes | **8** | `GET …/pending-manual/summary`, `POST …/upload-codes`, `POST …/retry-all` | voucher_pools, affiliate_ledger | upload does *not* issue; retry only completes full bundles | Two separate confirms; admin must remember step 2. |
| A6 ● | **Voucher batches**: create / edit schedule / disable / reconcile / add codes | Affiliate › Voucher Batches | name*, pool*, entitlement month* (or start/end for WELCOME), codes*, notes | **6–7** (create) | `GET/POST /api/admin/affiliate-voucher-batches`, `GET/PATCH …/<id>`, `POST …/<id>/add-codes|reconcile` | affiliate_voucher_batches, voucher_pools | `invalid_pool_id`, `batch_window_overlap`, `no_codes`, `duplicate_codes`, `active_batch_edit_restricted`, `upload_failed`… (all mapped to plain text ✔) | Best-in-repo example: preview mirrors server parser. Pool/month errors still cost a failed upload. |
| A7 ○ | Pool inventory (read-only) | › Voucher Pools | — | 2 | `GET /v2/miniapp/admin/pools/summary` | voucher_pools | | Duplicates info in A1 and A6. |
| A8 ○ | Qualification preview (shadow) | › Qualification Preview | month, optional source mapping (10 raw fields) | ~3 | `GET /api/admin/dashboard/affiliate/qualification-preview` | read-only | | Mapping fields expose column names. |
| A9 ○ | Campaign display override | › Campaign Display | campaign_id, participants (display name, joins, qualified, XP) | ~10 | `/api/admin/campaign-display…` | campaign_display_overrides | audit-logged; "disable" via `confirm()` | Fabricated display numbers are a policy-sensitive feature. |
| A10 ○ | Issue current month; leaderboard snapshot regenerate; history rebuild; legacy pool upload/replace; KPI daily | **No dashboard UI** | | — | `POST /v2/miniapp/admin/affiliate/issue-current-month`, `POST /api/admin/leaderboard/affiliate/snapshot/regenerate`, `POST /api/admin/leaderboard/history/rebuild`, `POST /v2/miniapp/admin/pools/upload|replace`, `GET …/affiliate/kpis`, `…/metrics/daily` | | | Live, unguarded by any UI (CLI/secret only). Must stay reachable. |

### Referral, Community, Welcome, Reactivation

| # | Workflow | Where today | Inputs | Int. | API | Collections | Notes |
|---|---|---|---|---|---|---|---|
| R1 ○ | Referral performance + per-referrer invitees | Referral › Performance | window | 3 | `GET /api/admin/dashboard/referrals[/detail]` | pending_referrals, qualified_events | Window 7d default. |
| R2 ○ | Share content hooks/playback CRUD + bulk import | Referral › Share Content | text, active toggle, bulk paste | ~5 | `/api/admin/referral/share-content/…` (15 routes) | caption_hooks, playback_pool | Live copy shown to players. |
| R3 ○ | Share engagement | Referral › Share Engagement | — | 3 | `GET /api/admin/referral-engagement` | referral_engagement_events | |
| R4 ○ | Creator access: add / bulk / suspend / activate / delete; group verify | Referral › Share Content › "Creator Access" sub-tab | user IDs, group | ~5 | `/api/admin/referral/creators…`, `/creator-settings…` | creator_members, app_settings | delete `confirm()` |
| M1 ○ | Community composer → schedule/publish; calendar; board; poll results; destinations; media library | Community Centre | title, type (7), destination, text, media, schedule | ~8 | `/api/admin/community/…` (19 routes) + `/media…` (6) | community_posts, community_destinations, community_media_library, … | Reject uses `prompt()`; stop-poll uses modal; destination test exists. |
| W1 ○ | Welcome journey + funnel | Welcome Journey | — | 2 | `GET /api/admin/dashboard/welcome-journey[-runtime]`, `…/funnel` | | read-only |
| X1 ○ | Channel reactivation start/pause (+ per-run limit), journey config, pool upload | Reactivation › Campaigns | limit, pool, codes | ~5 | `POST /api/admin/channel-reactivation/start|pause`, `/api/admin/reactivation/journey/…` | users, voucher_pools | Registered in two blueprints (`admin_auth.py:397`, `main.py:2897`). |

### Players, Analytics, Automation, Settings

| # | Workflow | Where today | Inputs | Int. | API | Collections | Notes |
|---|---|---|---|---|---|---|---|
| S1 ● | Player drill-down (id / @username) | Segments › All Players | query | 3 | `GET /api/admin/dashboard/user?query=` | users, welcome_eligibility, voucher_claims, affiliate_ledger, pending_referrals, qualified_events | **Already returns** profile, XP, check-in, referral counts, risk flags, welcome, voucher + affiliate history. Best data source for a global search. |
| S2 ● | **Add / reduce XP** | Segments › Adjust XP | @username*, amount* | **5** | `POST /api/add_xp` | users (`update_user_xp`) | Per-admin cooldown (429); `amount==0` rejected; negative allowed; no cap; supports `idempotency_key` (UI sends none). |
| S3 ○ | Join requests (read) | Automation › Join Requests | — | 3 | `GET /api/join_requests` (Telegram API) | — | Under *Automation*; read-only. |
| S4 ○ | Segment/VH audits + simulators (8 views) | Segments › VH:… | params | 3 each | `GET …/backend-segment-engine/*` | segment snapshots | 6 tabs prefixed "VH:" — jargon. |
| S5 ○ | Backend Segment Engine run; UIM import commit/resync; upload player performance | Analytics › Cohorts; Automation › Upload Data | file/params | ~4 | `POST …/backend-segment-engine/run`, `POST /api/admin/data/upload-player-performance`, `POST …/uim-import/commit|<id>/resync` | segment/uim collections | Operational, heavy; commit/resync act on real data. |
| S6 ○ | Settings (≈14 groups), audit log | Settings › 10 category tabs | per field | ~4 | `GET/POST /api/admin/settings[/<group>]`, `…/audit-log`, `/schema` | app_settings, app_settings_audit | Server-side min/max per field ✔; env-var names are field keys. |
| S7 ○ | Retention KPIs (+recompute), joins daily/export, CSV export | **Legacy panel only** | months | — | `GET /api/admin/retention-kpis[/export]`, `POST …/recompute`, `GET /api/admin/joins/daily|export`, `GET /api/export_csv` | | Must stay reachable. |
| S8 ○ | Past leaderboards (weeks / affiliate snapshots) | **Legacy panel only** | week | — | `GET /api/leaderboard/history/…`, `/api/leaderboard/affiliate/snapshot[s]` | | Dashboard "Leaderboard" tab is an external link into the Mini App. |
| Z1 ● | Admin login / session | `/admin`, `admin-login.html` | Telegram Login Widget | 2 | `/api/admin/auth/telegram-login|logout|me` | admin_login_audit | |

## 1.2 Traceability: where each *existing* capability lands

Nothing is removed. Capabilities currently reachable only from the legacy Mini App panel (V4, V5, V8, S7, S8) or only via API (A10, V5-reconcile, backfill-status) get a home in **Tools** (table in `04-navigation-and-design-system.md §1.3`). The Mini App itself is untouched.

## 1.3 Cross-cutting audit results

| Aspect | Result |
|---|---|
| Authorization | One flat gate; shared secret grants everything; **no per-action audit** for drops, affiliate approve/reject, XP, Builder launch. |
| Confirmation | 5 idioms (see README). Strictest on bulk reject; none on single reject, start drop, pause drop, XP, create drop, Builder-published drops beyond `LAUNCH`. |
| Error text | Voucher batches and campaign actions map to plain English (`AFF_BATCH_ERROR_MESSAGES`, `gcActionErrorMessage`); drops, XP, affiliate queue, Builder, community show raw codes. |
| Time entry | KL time everywhere (good); input forms vary: `datetime-local`, `YYYY-MM-DD HH:MM:SS` text, `month` picker. |
| Code parsing | 3 distinct rules across 6 upload endpoints (README). |
| Tests | ~40 `test_admin_*.test.js` + Python tests slice `admin-dashboard.js` by text markers inside a `node:vm` sandbox, and a views-sync test ties `VIEWS` ↔ HTML sections (`test_admin_dashboard_views_sync.test.js`). Constrains how the shell can change (roadmap §R1). |
