# Admin UI Unification — Smoke Test Checklist

Run this after any change to `static/admin-dashboard.html`, `admin-dashboard.js`, or
`admin-dashboard.css`. Companion to `docs/ADMIN_UNIFICATION_DESIGN.md` (P1 nav-only
implementation — no backend/RBAC changes).

## 0. Setup
- [ ] Log in at `/admin` with a valid admin credential (Telegram login, admin cache, or
      `ADMIN_PANEL_SECRET`) and confirm the shell loads with the **11-section** sidebar
      (Home, Campaign Control, Community Dashboard, Funnel, Reactivation, Affiliate,
      Segments, Abuse & Risk, Validation, Automation, Admin Settings) instead of the old
      flat list.
- [ ] Confirm the legend at the top of the sidebar shows all four badges: Live Control,
      Read Only, Legacy, Hidden Tool.

## 1. Navigation shell
- [ ] Every one of the 11 group headers expands/collapses on click.
- [ ] Clicking a nav item inside a collapsed group auto-expands that group and loads the
      view (no dead clicks).
- [ ] Reload the page — default view is Home / Executive Summary.
- [ ] Refresh (↻) button reloads data for whichever view is currently open.
- [ ] Logout button clears the session and redirects to `/admin` (login screen).

## 2. Every legacy view still renders (no view was removed)
For each item below, click it and confirm the section renders without a console error:
- [ ] Home → Executive Summary
- [ ] Campaign Control → Campaigns (list + New Campaign editor + Preview Audience)
- [ ] Campaign Control → Vouchers Overview
- [ ] Campaign Control → Legacy MiniApp Admin (opens `/static/index.html#admin-panel` in
      a new tab, admin panel visible)
- [ ] Community Dashboard → Referrals
- [ ] Community Dashboard → Audit
- [ ] Community Dashboard → User Drilldown (search a known user id)
- [ ] Community Dashboard → Legacy MiniApp Admin link opens correctly
- [ ] Funnel → Activation Funnel
- [ ] Funnel → Funnel Dashboard (standalone) opens `/static/funnel-dashboard.html` in a
      new tab and still renders
- [ ] Reactivation → Channel Reactivation (status chip populates, Start/Pause visible)
- [ ] Affiliate → Affiliate Dashboard
- [ ] Affiliate → Legacy MiniApp Admin link opens correctly
- [ ] Segments → Segment Overview
- [ ] Segments → Segment Probability Config
- [ ] Segments → Segment ROI
- [ ] Abuse & Risk → Abuse Overview
- [ ] Abuse & Risk → Voucher Hunter Mismatch
- [ ] Abuse & Risk → Unclassified Audit
- [ ] Abuse & Risk → VH Rule Quality
- [ ] Abuse & Risk → VH False Positive
- [ ] Abuse & Risk → Segment Rule Simulator (Run Simulation)
- [ ] Abuse & Risk → VH Rule Simulator (Run Simulation)
- [ ] Abuse & Risk → VH Priority Impact (Run Analysis)
- [ ] Validation → Validation / UIM Compare
- [ ] Automation → Backend Segment Engine (dropdowns populate, Dry Run works)
- [ ] Automation → Upload Player Performance (file picker present)
- [ ] Automation → Upload History
- [ ] Automation → Raw Data Explorer
- [ ] Admin Settings → Settings

## 3. Mutation surfaces (extra care — these are labeled "Live Control")
- [ ] Campaigns: creating/editing/deleting a campaign still calls
      `/api/admin/campaigns*` and behaves identically to before the nav change.
- [ ] Reactivation Start/Pause still calls `/api/admin/channel-reactivation/start|pause`.
- [ ] Backend Segment Engine Dry Run / Commit Run still calls
      `/api/admin/dashboard/backend-segment-engine/run` and is clearly marked shadow-mode.
- [ ] Confirm `vouchers.py` claim logic, voucher allocation, and `scheduler.py` jobs were
      **not** touched by this change (`git diff` should show none of those files).

## 4. MiniApp (`static/index.html`) untouched
- [ ] Open the Mini App as a normal (non-admin) player — referral, check-in, leaderboard,
      and region-overlay flows work exactly as before.
- [ ] Open the Mini App as an admin — `#admin-panel` still appears with Add/Reduce XP,
      Join Requests, Voucher Drops, Affiliate Pools, and Past Leaderboard controls intact.

## 5. Regression check
- [ ] `git diff` touches only `static/admin-dashboard.html`, `static/admin-dashboard.js`,
      `static/admin-dashboard.css`, and `docs/` — no changes to `vouchers.py`,
      `affiliate_rewards.py`, `scheduler.py`, `claim_risk_sync.py`, or
      `backend_segment_engine.py`.
