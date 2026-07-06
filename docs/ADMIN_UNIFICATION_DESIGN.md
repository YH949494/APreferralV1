# APReferral — Unified Admin Dashboard Architecture (Phase 1 Design)

Status: design only, no code changes. P0 (segment production wiring) is complete and untouched.
Scope guardrail: this document does not modify `vouchers.py` claim engine, affiliate settlement
(`affiliate_rewards.py`, `affiliate_dashboard_export.py`, `affiliate_leaderboard.py`,
`affiliate_group_access.py`), `scheduler.py`, or the anti-abuse pipeline
(`claim_risk_sync.py`, `backend_segment_engine.py`). Those are consumed by the new UI, not rewritten.

---

# Current Navigation

| Current Page | Keep | Merge | Remove |
|---|---|---|---|
| Admin UI A — Executive Summary (`summary`) | Keep | — | — |
| Admin UI A — Activation Funnel (`funnel`) + `funnel-dashboard.html` | Merge | Fold standalone `funnel-dashboard.html` into in-app Funnel view | Remove standalone HTML file once folded in |
| Admin UI A — Abuse Overview (`abuse`) | Keep | Merge with voucher-hunter audits below into one "Abuse & Risk" section | — |
| Admin UI A — Campaigns (`campaigns`) | Keep | — | — |
| Admin UI A — Vouchers (`vouchers`) | Merge | Merge with UI B's Voucher Drops + Create Bonus Voucher into one Campaign Control area | — |
| Admin UI A — Referrals (`referrals`) | Keep | Merge into Community Dashboard | — |
| Admin UI A — Affiliate (`affiliate`) | Merge | Merge with UI B's Affiliate Voucher Pools + Affiliate Leaderboard control into one Affiliate section | — |
| Admin UI A — Channel Reactivation (`reactivation`) | Keep | — | — |
| Admin UI A — Audit (`audit`) | Merge | Fold into Community Dashboard as an "Audit" tab | — |
| Admin UI A — Segment Probability Config (`segmentProbabilityConfig`) | Merge | Merge into Segments section | — |
| Admin UI A — Segment ROI (`segmentRoi`) | Merge | Merge into Segments section | — |
| Admin UI A — Segment Overview (`segments`) | Keep | Anchor page for Segments section | — |
| Admin UI A — Validation (`validation`) | Keep | — | — |
| Admin UI A — Backend Segment Engine (`backendSegmentEngine` + sub-tabs: uim-comparison, identity-match-audit) | Keep | Anchor page for Automation section | — |
| Admin UI A — Voucher Hunter Mismatch Audit (`voucherHunterAudit`) | Merge | Merge into Abuse & Risk | — |
| Admin UI A — Unclassified Audit (`unclassifiedAudit`) | Merge | Merge into Abuse & Risk | — |
| Admin UI A — Segment Rule Simulator (`segmentRuleSimulator`) | Merge | Merge into Abuse & Risk (simulators subgroup) | — |
| Admin UI A — VH Rule Quality (`voucherHunterQuality`) | Merge | Merge into Abuse & Risk | — |
| Admin UI A — VH False Positive (`voucherHunterFalsePositive`) | Merge | Merge into Abuse & Risk | — |
| Admin UI A — VH Rule Simulator (`voucherHunterRuleSimulator`) | Merge | Merge into Abuse & Risk | — |
| Admin UI A — VH Priority Impact (`vhPriorityImpact`) | Merge | Merge into Abuse & Risk | — |
| Admin UI A — Upload Player Performance (`uploadPlayerPerformance`) | Merge | Merge into Automation → Data Ops | — |
| Admin UI A — Upload History (`uploadHistory`) | Merge | Merge into Automation → Data Ops | — |
| Admin UI A — Raw Data Explorer (`rawExplorer`) | Merge | Merge into Automation → Data Ops | — |
| Admin UI A — User Drilldown (`users`) | Merge | Merge into Community Dashboard as a drill-in view | — |
| Admin UI A — Settings (`settings`) | Merge | Merge into Admin Settings | — |
| UI B — Add/Reduce XP | Merge | Merge into Community Dashboard (per-user actions) | — |
| UI B — View Join Requests | Merge | Merge into Community Dashboard | — |
| UI B — View Past Leaderboard | Merge | Merge into Community Dashboard | — |
| UI B — Create Bonus Voucher | Merge | Merge into Campaign Control | — |
| UI B — Voucher Drops (create/add-codes/list/actions) | Merge | Merge into Campaign Control — this is the highest-risk mutation surface, gets extra confirmation UX | — |
| UI B — Affiliate Voucher Pools (upload/summary/pending/KPI) | Merge | Merge into Affiliate section | — |
| UI B — Past Week Affiliate Leaderboard regenerate | Merge | Merge into Affiliate section, gated as a Super Admin action | — |
| UI B — non-admin player sections (region overlay, progress, check-in, leaderboard, referral tabs) | Keep, but separate | Not part of admin unification — remains player-facing Mini App, unchanged | — |
| `admin-login.html` | Keep | Single shared login for the unified portal | — |
| `mockup.html` | — | — | Remove (unwired design mockup, dead file) |
| `index.visual-ref.html.html` | — | — | Remove (dead reference copy, not routed) |
| `templates/index.html` (9-line redirect stub) | Keep | — | — |

---

# Proposed Navigation

1. **Home** — Executive Summary (from `summary`) + system health at a glance
2. **Campaign Control** — Campaigns builder/list, Vouchers overview, Voucher Drops (create/add codes/actions), Create Bonus Voucher
3. **Community Dashboard** — Referrals, Audit, User Drilldown, Add/Reduce XP, Join Requests, Past Leaderboards
4. **Funnel** — Activation Funnel (replaces standalone `funnel-dashboard.html`)
5. **Reactivation** — Channel Reactivation (start/pause) — unchanged
6. **Affiliate** — Affiliate dashboard, Affiliate Voucher Pools, Affiliate KPIs, Pending approvals/rejects, Leaderboard regenerate
7. **Segments** — Segment Overview, Segment Probability Config, Segment ROI
8. **Abuse & Risk** — Abuse Overview, Voucher Hunter audits/simulators (mismatch, unclassified, quality, false-positive, rule simulator, priority impact), Segment Rule Simulator
9. **Validation** — UIM validation panel (unchanged) + KPI Gap Report
10. **Automation** — Backend Segment Engine (run/status/available-periods/takeover-readiness/uim-comparison/identity-match-audit), Data Ops (Upload Player Performance, Upload History, Raw Data Explorer, UIM import commit/history/resync)
11. **Admin Settings** — Settings panel, role/user management (new), audit log of admin actions (new, optional Phase 2)

---

# RBAC Matrix

Today every route collapses to a single boolean `is_admin`, sourced from one of three interchangeable credentials (static `ADMIN_PANEL_SECRET`, Telegram allowlist `admin_cache`/`HARDCODED_ADMIN_USERNAMES`, or a Flask-session cookie from the Telegram Login Widget). There are no tiers. The matrix below is the **target** state — a mapping of existing endpoints onto four proposed roles. Implementing it is a Phase 2 concern (introduces a real `role` field); Phase 1 only needs this mapping to inform navigation/visibility decisions and to flag which mutating endpoints most urgently need a tier above "any admin."

Roles, least to most privileged:

- **Viewer** — read-only dashboards, no mutation, no PII exports
- **Operator** — Viewer + day-to-day player-support actions (XP adjustment, join requests, welcome voucher issuance)
- **Campaign Manager** — Operator + campaign/voucher-drop/segment-config authoring, affiliate pool uploads
- **Super Admin** — full access: affiliate reward approval/issuance, backend segment engine runs, data imports, settings, role management

| Endpoint (method) | Current Nav Section | Proposed Role |
|---|---|---|
| `GET /api/admin/dashboard/summary` | Home | Viewer |
| `GET /api/admin/dashboard/telegram-counts/cache` | Home | Viewer |
| `POST /api/admin/dashboard/telegram-counts/refresh` | Home | Operator |
| `GET /api/admin/dashboard/funnel`, `GET /api/admin/funnel-dashboard` | Funnel | Viewer |
| `GET /api/admin/dashboard/abuse` | Abuse & Risk | Viewer |
| `GET .../backend-segment-engine/*` (audits, simulators, mismatch, quality, false-positive, priority-impact, unclassified, segment-rule-simulator) | Abuse & Risk / Automation | Viewer |
| `POST .../backend-segment-engine/run` | Automation | Super Admin |
| `GET .../backend-segment-engine/run-status`, `available-periods`, `takeover-readiness` | Automation | Viewer |
| `GET .../uim-comparison`, `/uim-comparison/export`, `/identity-match-audit` | Automation | Viewer (export → Operator, contains exportable data) |
| `GET /api/admin/campaigns`, `GET /.../<id>` | Campaign Control | Viewer |
| `POST /api/admin/campaigns/preview` | Campaign Control | Campaign Manager |
| `POST /api/admin/campaigns`, `PUT /.../<id>`, `DELETE /.../<id>` | Campaign Control | Campaign Manager |
| `GET /api/admin/dashboard/vouchers` | Campaign Control | Viewer |
| `POST /v2/miniapp/admin/drops` (create drop) | Campaign Control | Campaign Manager |
| `POST /v2/miniapp/admin/drops/<id>/codes` | Campaign Control | Campaign Manager |
| `POST /v2/miniapp/admin/drops/<id>/actions` (e.g. `start_now`) | Campaign Control | **Super Admin** (live mutation with immediate player-facing impact) |
| `GET /v2/miniapp/admin/drops`, `/drops_v2` | Campaign Control | Viewer |
| `GET /api/bonus_voucher`, `/api/affiliate_bonus_vouchers`, `/api/campaign_bonus_voucher` | Campaign Control | Viewer |
| `POST /api/admin/set_bonus` | Campaign Control | Campaign Manager |
| `GET /api/admin/dashboard/referrals`, `/referrals/detail` | Community Dashboard | Viewer |
| `GET /api/admin/dashboard/audit` | Community Dashboard | Viewer |
| `GET /api/admin/dashboard/user` | Community Dashboard | Viewer |
| `POST /api/add_xp` | Community Dashboard | Operator |
| `GET /api/join_requests` | Community Dashboard | Operator |
| `GET /api/admin/joins/daily`, `/joins/export` | Community Dashboard | Viewer / Operator (export) |
| `GET /api/admin/retention-kpis`, `/retention-kpis/export` | Community Dashboard | Viewer / Operator (export) |
| `POST /api/admin/retention-kpis/recompute` | Community Dashboard | Super Admin |
| `GET /api/admin/dashboard/welcome-journey` | Community Dashboard | Viewer |
| `GET /api/admin/channel-reactivation/summary` | Reactivation | Viewer |
| `POST /api/admin/channel-reactivation/start`, `/pause` | Reactivation | Campaign Manager |
| `GET /api/admin/dashboard/affiliate`, `/affiliate/detail` | Affiliate | Viewer |
| `GET /v2/miniapp/admin/pools/summary`, `/affiliate/kpis`, `/metrics/daily`, `/affiliate/pending` | Affiliate | Viewer |
| `POST /v2/miniapp/admin/pools/upload` | Affiliate | Campaign Manager |
| `POST /v2/miniapp/admin/affiliate/<id>/approve`, `/reject` | Affiliate | **Super Admin** (moves real money/rewards) |
| `POST /v2/miniapp/admin/affiliate/issue-current-month` | Affiliate | **Super Admin** |
| `POST /api/admin/leaderboard/affiliate/snapshot/regenerate` | Affiliate | Super Admin |
| `GET /api/admin/dashboard/segments`, `/segment-probability-config`, `/segment-roi` | Segments | Viewer |
| `GET /api/admin/dashboard/validation` | Validation | Viewer |
| `GET /api/admin/dashboard/kpi-gap-report` | Validation | Viewer |
| `POST /api/admin/data/upload-player-performance` | Automation | Super Admin |
| `GET /api/admin/data/upload-history` | Automation | Viewer |
| `GET /api/admin/data/raw-explorer` | Automation | Operator |
| `POST /api/admin/data/uim-import/commit` | Automation | Super Admin |
| `GET /api/admin/data/uim-import/history` | Automation | Viewer |
| `POST /api/admin/data/uim-import/<batch_id>/resync` | Automation | Super Admin |
| `GET /api/admin/dashboard/settings` | Admin Settings | Viewer |
| `POST /api/admin/backfill-status` | Admin Settings | Super Admin |
| `GET /api/is_admin`, `/api/admin/auth/me` | (all) | Viewer (identity check only) |
| `POST /api/admin/auth/telegram-login`, `/auth/logout` | (all) | n/a — authentication, not authorization |
| `GET /api/export_csv` | cross-cutting | Operator |

Notes:
- The current `require_admin()` / `require_admin_from_query()` / `_admin_api_authorized()` helpers all need to grow a role parameter rather than being replaced — they already centralize the check in three files (`vouchers.py`, `main.py`, `admin_auth.py`), so adding a `min_role=` argument to each is a contained change.
- The `BYPASS_ADMIN` flag in `vouchers.py` (hardcoded `False` today) should be removed or hard-gated behind a non-production environment check before RBAC ships — as-is it's a silent full-bypass footgun.
- `admin_cache` (Mongo) and `HARDCODED_ADMIN_USERNAMES`/`ADMIN_USER_IDS` need a `role` field added per admin identity; the shared static `ADMIN_PANEL_SECRET` should probably be retired to Super Admin-only bootstrap/emergency use once per-user roles exist, since it currently grants blanket access with no identity at all.

---

# Migration Plan

**Step 0 — Inventory freeze (done)**
Confirm the endpoint/page map above is complete and accurate before any UI work starts.

**Step 1 — Shared shell**
Build one HTML shell (nav sidebar + auth check + role context) reusing `admin-login.html` and the existing `admin_auth.py` session flow unchanged. Mount the 11 proposed nav sections as routes/views in this shell. No backend changes yet — this step only touches presentation.

**Step 2 — Migrate unchanged pages**
Move Home, Funnel, Reactivation, Segments, Validation, Automation (Backend Segment Engine) into the shell as direct ports of existing UI A views. These already have 1:1 backend endpoints and need no merging logic — lowest risk, do first to validate the shell.

**Step 3 — Merge Campaign Control**
Combine UI A's Campaigns + Vouchers views with UI B's Voucher Drops, Create Bonus Voucher, and Add-to-Existing-Drop flows into one section. This is the highest-traffic mutation surface (`POST /admin/drops`, `/admin/drops/<id>/actions`) — build with explicit confirm-before-submit dialogs and a visible "this is live" indicator, since UI B's original design already treats these as live mutation controls.

**Step 4 — Merge Affiliate**
Combine UI A's Affiliate dashboard with UI B's Affiliate Voucher Pools, pending approval/rejection actions, and leaderboard regenerate. Approval/rejection and issue-current-month touch real reward payouts — keep these behind an extra confirmation step even before RBAC enforcement lands.

**Step 5 — Merge Community Dashboard**
Combine Referrals, Audit, User Drilldown (UI A) with Add/Reduce XP, Join Requests, Past Leaderboard (UI B) into one section with per-user drill-in.

**Step 6 — Merge Abuse & Risk**
Combine Abuse Overview with all voucher-hunter audit/simulator sub-views and Segment Rule Simulator into one section with sub-tabs — these are read-only analytics, safe to consolidate purely as a navigation/IA change.

**Step 7 — Admin Settings**
Move Settings panel in; add placeholders for role management and admin action audit log (implementation of RBAC enforcement itself is Phase 2, tracked separately from this navigation unification).

**Step 8 — Decommission**
Once the unified shell covers all sections and has been validated in production for a burn-in period, remove `static/admin-dashboard.html`, the admin-panel portion of `static/index.html` (leaving the player-facing Mini App parts intact), `funnel-dashboard.html`, `mockup.html`, and `index.visual-ref.html.html`. Do not touch `templates/index.html` (unrelated redirect stub) or any non-admin route.

**Step 9 — RBAC enforcement (Phase 2, separate effort)**
Add `role` to admin identity records, extend `require_admin()`/`require_admin_from_query()`/`_admin_api_authorized()` with a `min_role` argument, apply per the matrix above, retire `ADMIN_PANEL_SECRET` to break-glass use only, and remove `BYPASS_ADMIN`.

Each step ships independently and is revertible by re-pointing nav links back to the legacy pages, which stay live and untouched until Step 8.

---

# Risk Assessment

| Risk | Where | Mitigation |
|---|---|---|
| Live mutation controls (voucher drop `start_now`, affiliate approve/reject/issue) get accidentally easier to trigger once merged into one dense nav | Campaign Control, Affiliate | Keep explicit confirm dialogs; in Step 9 gate these specifically at Super Admin tier rather than blanket admin |
| No existing role concept — today "any credential = full access" | All endpoints | RBAC matrix above defines target tiers now, even though enforcement is Phase 2, so the nav/IA work doesn't need to be redone later |
| `BYPASS_ADMIN` flag in `vouchers.py` | Auth | Flag for removal/env-gating regardless of this project's timeline — it's a pre-existing full-bypass risk, not introduced by unification |
| Static shared `ADMIN_PANEL_SECRET` gives no per-user identity or audit trail | Auth (all three auth helpers) | Long-term: restrict to break-glass; short-term (Phase 1): no change needed since scope is nav-only |
| UI B's admin panel is embedded inside the 6500-line player-facing `index.html` | UI B | Extraction must be careful not to break the non-admin Mini App flows (region overlay, check-in, leaderboard, referral tabs) that share the same file — treat as a surgical extraction, not a rewrite |
| Segment/abuse consolidation touches many read-only analytics endpoints feeding `backend_segment_engine.py` | Abuse & Risk, Automation | Read-only — safe to merge in the UI without backend risk; do not alter `backend_segment_engine.py` itself |
| Dead files (`mockup.html`, `index.visual-ref.html.html`) removed at Step 8 | Cleanup | Confirm no route or external link references them before deletion (inventory shows none currently wired) |
| Parallel-running legacy + unified UI during migration could double-report metrics if both call mutating endpoints from different sessions | Campaign Control, Affiliate | No backend duplication risk since both UIs call the same endpoints — but train admins to use only one UI at a time during the transition window |
| Decommissioning UI A/B before unified shell has full parity | All sections | Gate Step 8 behind a completeness checklist matching every row in "Current Navigation" above to a live equivalent in the new shell |
