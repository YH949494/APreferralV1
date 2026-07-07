# Dashboard V2.1 — UX Polish & Consistency Pass

Second phase of the admin dashboard UX overhaul. UI/UX only — no API,
route, database, scheduler, or business-logic changes. Builds on
`docs/DASHBOARD_V2_UX_OVERHAUL.md` (nav restructure + toast framework).

## Summary

This pass makes existing feedback/empty/loading patterns consistent across
the whole dashboard by upgrading a handful of shared functions
(`statePanel`, `banner`) rather than rewriting each of the ~40 views
individually, plus a few targeted, high-visibility additions: a topbar
notification bell, a standardized button color hierarchy, and one table→card
conversion (Affiliate Voucher Pools) called out by name in the brief.

## Components added

- **Notification bell** (`#notif-bell` / `#notif-dropdown` in the topbar) —
  shows a badge count of the same signals as the Home "Attention Required"
  panel (low voucher pools, paused campaign batches, pending affiliate
  approvals), refreshed every 90s. Clicking an item navigates to the
  relevant Center. Read-only, reuses existing endpoints — no websocket.
- **`computeAttentionSignals()`** — extracted from the V2 Home panel so the
  bell and the Home panel share one source of truth instead of two
  divergent implementations.
- **`emptyState(msg)`** — renders either a plain message (legacy string
  callers keep working unchanged) or a richer `{icon, title, sub, ctaHtml}`
  empty state with an action button.
- **`goToViewAndClick(view, btnId)`** — lets an empty-state CTA button
  living on one view (e.g. "No active campaigns" on Overview) navigate to
  another view and press its primary action button (e.g. Create Campaign
  on Campaign Builder).
- Skeleton loading rows (`.skeleton-stack` / `.skeleton-row`), empty-state
  card (`.empty-state`), button hierarchy class (`.btn.danger`, alongside
  the existing `.btn` / `.btn.primary`), progress-row with label
  (`.progress-row`), and `:focus-visible` outlines for keyboard navigation.

## Components updated

- **`statePanel(elId, kind, msg)`** — `"loading"` now renders skeleton
  placeholder rows instead of a "Loading…" text string; `"empty"` now
  renders through `emptyState()`. This single change upgrades loading and
  empty states across every view that calls it (referrals, audit,
  segments, validation, VH audits, uploads, join requests, settings, etc.)
  with no per-view edits.
- **Button color hierarchy** applied consistently: primary/orange for
  Launch, Save, Release Next, Resume, Approve, Upload, Create; secondary/
  gray (default `.btn`) for Refresh, Analytics, View, Edit, Cancel-form;
  danger/red for Delete, Cancel (batch), Reject, Pause, Archive, End Drop.
- **Campaign cards** (Running/Drafts) — the batch-release progress bar
  added in V2 is unchanged; button classes above are now consistent
  red/orange/gray instead of ad-hoc inline `background:transparent` styles.
- **Status badges** — added `.pill.healthy/.warning/.critical` and mapped
  the existing risk-level values (`medium_risk`, `high_risk_review`,
  `abuse_freeze`) onto the same warm/red palette used elsewhere, and a
  `.status-dot.critical/.warning` pair alongside the existing
  healthy/stale/unknown dots.
- **Empty states with a call-to-action** applied to the three places named
  in the brief: Campaigns (legacy targeting) → "+ Create Campaign", Draft
  Campaigns → "+ New Campaign", Overview/Running with zero active
  campaigns → "+ Create Campaign" (navigates to Campaign Builder first).

## Pages / screens improved

- **Affiliate Voucher Pools** — the pool summary table was replaced with a
  responsive card grid (Priority 1): each card shows the pool ID, a
  Healthy/Watch/Low status badge (based on remaining code count), the
  available/issued counts, a "% used" progress bar, and the existing
  label/value-hint/currency fields. No information was removed.
- **Home** — now also drives the notification bell (shared data source).
- **Campaign Center (Overview / Drafts / Legacy Targeting)** — richer
  empty states with a create-campaign CTA instead of a plain sentence.
- **Topbar** (global, every page) — notification bell.
- **Responsive layout** — `.card-grid` already used `auto-fill` for
  natural reflow; added explicit breakpoints (`≤1024px` → 2 columns,
  `≤560px` → 1 column, notification dropdown narrows) so card counts per
  row match the brief's desktop/tablet/mobile expectation without
  horizontal scrolling.
- **Accessibility** — visible focus rings on all interactive elements,
  34px minimum height on buttons/inputs/pills/nav-items for touch targets.

## Manual QA checklist

- [ ] Sidebar still shows the 8 groups from V2.1's predecessor pass; every
      nav item still loads its view.
- [ ] Home → Attention Required panel renders the same signals as before.
- [ ] Topbar bell badge count matches the number of Attention Required
      rows on Home; clicking a bell item navigates to the right Center.
- [ ] Campaign Center → Overview with zero active campaigns shows the new
      empty state and the "+ Create Campaign" button opens the wizard.
- [ ] Campaign Builder → Drafts with zero drafts shows the empty state and
      "+ New Campaign" opens the wizard on the same page.
- [ ] Affiliate Center → Voucher Pools renders as cards with correct
      available/issued numbers and % used bar (spot-check against the old
      table values, e.g. via `/v2/miniapp/admin/pools/summary` directly).
- [ ] Deleting a draft campaign still requires typing `DELETE`; Pause/
      Cancel/Reject/Archive buttons are now red, Approve/Release/Resume/
      Save/Create buttons are orange, Refresh/Edit/Analytics stay gray.
- [ ] Any view that shows "Loading…" (referrals, audit, segments,
      validation, VH audits, uploads, settings, join requests) now shows
      skeleton placeholder rows instead of plain text, and still populates
      correctly once data arrives.
- [ ] Tab/keyboard navigation shows a visible focus outline on buttons,
      nav items, and inputs.
- [ ] Resize to tablet/mobile widths — card grids reflow to 2/1 columns,
      no horizontal scrollbar appears anywhere.

## Files changed

- `static/admin-dashboard.html` — notification bell markup in the topbar;
  two `Save` buttons promoted to primary for consistency.
- `static/admin-dashboard.js` — `statePanel`/`emptyState` upgrade,
  `computeAttentionSignals`/notification bell, `goToViewAndClick`,
  richer empty states for the three named campaign lists, Affiliate
  Voucher Pools card-grid rendering, button class updates (danger/
  primary) across campaign, affiliate, reactivation, and drop actions.
- `static/admin-dashboard.css` — button hierarchy (`.btn.danger`),
  skeleton rows, empty-state card, notification bell/dropdown, progress
  row, additional status pill/dot colors, focus-visible outlines,
  responsive `.card-grid` breakpoints.
- `docs/DASHBOARD_V2_1_UX_POLISH.md` — this document.

No Python files, API routes, or database models were touched.

## Rollback plan

Same as V2: this change set is confined to the three static asset files
plus documentation. To roll back, `git revert` the commit(s) on this
branch, or redeploy the previous versions of
`static/admin-dashboard.{html,js,css}`. No data migration, backfill, or
feature flag is needed in either direction.

## Deferred to a future pass

Given the "UI/UX only, no rewrite of business logic" constraint and the
size of the existing dashboard, the following brief items are intentionally
out of scope for this pass:

- Full card-grid conversion of every remaining table (Reactivation,
  Segments, Validation, User Intelligence, Risk & Abuse detail views) —
  these already benefit from the skeleton/empty-state upgrade, but keep
  their table layout for now.
- Lightweight charts (claim-rate line, segment donut, status stacked bar) —
  would need either a small charting dependency or a hand-rolled SVG
  component; deferred to keep this pass dependency-free and low-risk.
- KPI header rows for Affiliate/Community/Risk/Operations beyond what
  already exists (`cards-affiliate-summary`, `cards-reactivation-summary`,
  `cards-roi-summary`, `cards-abuse` already provide this pattern on their
  respective pages).
