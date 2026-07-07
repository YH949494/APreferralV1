# Dashboard V2 — UX/UI Overhaul

Scope: navigation, layout, information architecture, and feedback framework
for `static/admin-dashboard.html` / `.js` / `.css`. No backend, claim engine,
scheduler, or database changes.

## 1. New navigation structure

The sidebar was regrouped from 11 flat groups into the 8 groups specified by
the design brief. View IDs (`data-view="..."`) were kept unchanged, so no
JS routing, API calls, or business logic needed to change — only which
group each nav button lives in and its label.

```
🏠 Home                 → summary
🚀 Campaign Center      → activeCampaigns (Overview), campaignBuilder (Create),
                          draftCampaigns, campaignPerformance, campaignIntelligence,
                          compiledDrops, [Advanced] campaigns, vouchers
👥 Community Center     → referrals, funnel, audit, joinRequests
💰 Affiliate Center     → affiliate, affiliatePending, affiliatePools
♻ Reactivation Center   → reactivation
👤 User Intelligence    → segments, users (Search User), segmentRoi,
                          segmentProbabilityConfig
🛡 Risk & Abuse         → abuse, voucherHunterAudit, unclassifiedAudit,
                          validation, xpAdjust, [Advanced] rule-quality/
                          simulator tools
⚙ Operations           → drops, backendSegmentEngine, uploadPlayerPerformance,
                          uploadHistory, rawExplorer, settings,
                          legacy MiniApp Admin ↗, Funnel Dashboard (standalone) ↗
```

Rationale: everything an admin needs to run day-to-day (campaigns,
community, affiliates, reactivation, users, risk) is a top-level group;
one-off/legacy/automation tooling that a tired admin should rarely need to
touch is consolidated into **Operations**. Items marked "Advanced ▾" in the
brief were kept in-tree with an "Advanced ▾" label prefix rather than a
collapsible drawer, to respect Rule 2 (no page deeper than Sidebar → Tab).

## 2. Home — "What needs my attention?"

Added an **⚠ Attention Required** panel at the top of the Home view
(`#attention-required` in `admin-dashboard.html`, `loadAttentionRequired()`
in `admin-dashboard.js`). It calls existing read-only endpoints already used
elsewhere in the dashboard (voucher pool summary, active campaign list,
pending affiliate rewards) and renders traffic-light rows:

- 🔴 Voucher pool low (available code count < 10)
- 🟡 Campaign batch paused (needs a Resume click)
- 🟡 N affiliate reward(s) pending approval

No new backend endpoint was added — this is a client-side aggregation of
data the dashboard already fetches for other views.

## 3. Button feedback framework

Every mutating button already had *some* disable-on-click behavior; this
pass makes the pattern consistent and adds a non-blocking toast on top of
the existing inline `banner()` messages, so status is visible even after a
user has scrolled or the view has re-rendered.

- `toast(message, kind)` — floating, auto-dismissing notification
  (`kind`: `"success" | "error" | "warn"`). Appends to a `#toast-stack`
  container that's created lazily on first use.
- `banner(msg, kind)` was extended to also emit a toast for `"ok"`/`"error"`
  kinds, so every existing call site (~30 across campaign builder, drops,
  affiliate, reactivation) gets a toast for free with no per-call-site edit.
- `btnStart(btn, loadingText)` / `btnStop(btn)` — reusable spinner + disable
  helpers (generalized from the campaign-builder-only `cbBtnStart`/`cbBtnStop`
  that already existed). Applied to Refresh and Reactivation Start/Pause.
- `confirmTyped(word, title, message)` — typed-confirmation modal
  (`.modal-overlay`/`.modal-box` in CSS) for the highest-risk destructive
  action that only had a plain `confirm()` before: deleting a draft
  campaign now requires typing `DELETE`. Campaign compile/launch already
  required typing `LAUNCH` (existing behavior, unchanged).

CSS added: `.toast-stack`, `.toast`, `.toast-success/-error/-warn`,
`.modal-overlay`, `.modal-box`, `.attention-list`, `.attention-item`
(`sev-red`/`sev-yellow`/`sev-green`).

## 4. Card system

Campaign lists (Running / Drafts) already rendered as `.campaign-card`
components rather than tables. This pass adds a batch-release progress bar
(reusing the existing `.bar-wrap`/`.bar` funnel styles) to each running
batch campaign card, so progress is visible at a glance without opening
Analytics.

## 5. Files changed

- `static/admin-dashboard.html` — sidebar regrouped into 8 sections;
  Home view gained the Attention Required panel.
- `static/admin-dashboard.js` — toast framework, generalized button
  loading helpers, typed-confirm modal, attention-required loader, toast
  wiring added to drop/affiliate/reactivation/XP action handlers, batch
  progress bar in campaign cards.
- `static/admin-dashboard.css` — toast, modal, and attention-card styles.
- `docs/DASHBOARD_V2_UX_OVERHAUL.md` — this document.

No Python files, API routes, or database models were touched.

## 6. Migration plan

This was designed to ship as a single non-breaking deploy:

1. All `data-view` IDs, element IDs, and API calls are unchanged — every
   existing view continues to load exactly the data it did before.
2. The nav restructure is presentation-only (which `<div class="nav-group">`
   a button lives in); `switchView()` looks up buttons by `data-view`
   globally, not by group, so it required zero JS changes.
3. Toasts are additive to the existing `banner()` inline messages — nothing
   that previously showed feedback stops showing it.
4. Deploy static assets only (no server restart required beyond normal
   static file cache invalidation).

## 7. Rollback plan

Because the changes are confined to three static files with no schema or
API changes:

- `git revert` the commit(s) on this branch, or
- Redeploy the previous `static/admin-dashboard.{html,js,css}` from the
  prior commit/tag.

No data migration, backfill, or feature flag is required in either
direction — the change set is purely front-end presentation and does not
alter what any button calls or what any endpoint returns.

## 8. Follow-up (not in this pass)

Given the "UI/UX/navigation only" constraint and the size of the existing
~4,500-line dashboard script, the following brief items are intentionally
left for a follow-up pass rather than attempted wholesale in one change:

- Converting every remaining data table (referrals, audit, segments, etc.)
  to card-based layouts.
- A guided step-by-step "Create Campaign" wizard redesign (the existing
  6-step wizard in Campaign Builder already matches the brief's step names
  — Type, Audience, Release, Reward, Preview, Launch — and was left as-is).
- Extending the typed-confirmation modal to Cancel/Pause actions (currently
  a plain `confirm()`, which the brief allows for non-irreversible pauses).
