# 3. Before/after measurement (Phase 9) and regression risk assessment

## 1. Before-and-after comparison

**Method.** *Current* = shortest valid click/field path read from the code (module click lands on its first tab; each further tab +1; each required field +1; submit +1; each confirmation dialog +1). *Proposed* = the same counting, **obtained by scripting the prototype's shortest path** (Playwright run recorded in the PR; each scripted click/fill/select counts 1), assuming the admin lands on **Home** and uses the action bar, row buttons or Home tiles. Fields that default (start = *now*, pool = *Everyone*, end = +24 h) cost 0. **All numbers are estimates — not stopwatch measurements. No time saving is claimed.** Where the redesign adds a deliberate safety interaction, the count shows it.

### 1.1 Interaction counts

| # | Flow | Current | Proposed | Δ (est.) | Note |
|---|---|---:|---:|---:|---|
| 1 | Create pooled voucher drop (default audience) | 6 | 5 | −1 (−17%) | adds Review stage; start defaults to *now* but is shown |
| 2 | Add codes to an existing drop (from the drop's row) | 5 | 4 | −1 (−20%) | adds a confirmation; via action bar: 5 |
| 3 | Upload $10/$50 affiliate batch | 7 | 6 | −1 (−14%) | $5 case: 6 → 6; adds confirm; month chosen from chips |
| 4 | Fix an affiliate stock shortage (upload **and** release waiting rewards) | 8 | 6 | −2 (−25%) | stays two explicit confirmations by design |
| 5 | Approve one affiliate reward | 4 | 3 | −1 (−25%) | from Home tile |
| 6 | Reject one affiliate reward | 4 | 4 | 0 | **fixes Cancel-still-rejects**; adds required reason |
| 7 | Bulk approve | 4 | 4 | 0 | typed phrase + exact preview of what is skipped |
| 8 | Bulk reject | 6 | 6 | 0 | three dialogs → one modal; selecting rows is now explicit (select-all = 1) |
| 9 | Inspect one affiliate (ledger + risk + invitees) | 11 | 2 | −9 (−82%) | Affiliate 360 |
| 10 | Inspect **and** approve one affiliate | 16 | 4 | −12 (−75%) | search → open → approve → confirm |
| 11 | Player campaign → saved draft | 9 | 5 | −4 (−44%) | registration type; +1 to schedule a start |
| 12 | Registration campaign → live | 11 | 5 | −6 (−55%) | review stage is the confirmation; publish chained to create |
| 13 | Tournament campaign → live (with active partner) | ≥14 | 8 | ≥ −6 (≈ −43%) | readiness checklist replaces error-then-fix |
| 14 | Mission create + publish (multiple choice, existing pool) | ~15 | 7 | ~ −8 (−53%) | one Details page replaces 4 steps; logic unchanged |
| 15 | Voucher campaign via Builder, launched (typical, with preview) | ~16 | 7 | ~ −9 (−56%) | min path today ~11 (skips preview, which the UI allows); preview now automatic |
| 16 | Add/remove XP | 5 | 5 | 0 | adds lookup, preview, confirm, idempotency key |
| 17 | Pause a live drop | 3 | 2 | −1 (−33%) | reversible: toast with Undo |
| 18 | Start a scheduled drop | 3 | 3 | 0 | adds disclosure that end time resets |
| 19 | End a live drop that has claims | 4 | 4 | 0 | adds typed `END`; an unclaimed drop needs 3 |

**Summary of the 19 measured flows:** 13 need fewer interactions, 6 stay level (and gain safeguards: 6, 7, 8, 16, 18, 19), none increase. The big reductions (9, 10, 11–15) are the tasks that today **cross modules, run a multi-step wizard or hide state until an error**. Money-moving single actions stay level on purpose.
Not re-measured (label/confirm changes only): providers, banners, lucky games, community composer, settings, segment/abuse reports.

### 1.2 Manual input, navigation, error prevention, remaining risk

| # | Manual fields today → proposed | Navigation change | Error prevention added | Remaining risk |
|---|---|---|---|---|
| 1 | name, start, codes (3) → name, codes (2) + explicit start choice | 2 clicks → action bar | Preview with errors/warnings (`buildDropPreview`), plain errors, "who can claim" restored, orphan-drop explanation | Server still inserts the drop before validating codes; duplicate against another drop only caught after ➕ check-codes ships |
| 2 | drop, pool, codes (3) → codes (1) | row action | destination-specific parser, confirm | partial insert on duplicate stays server behaviour |
| 3 | name, pool, month, codes (4) → pool chip, month chip, codes (3) | action bar | month cannot default; overlap/duplicate messages already plain | duplicates already in DB only known after upload (until ➕) |
| 4 | upload form + remember Retry → none (month/pool prefilled from shortage) | Home tile | next-step chip, counts before/after | stock mix-ups between denominations: preview shows value per code group |
| 5–8 | none beyond reason | Home tile / 360 | summary of risk flags + shortage, **required reason**, typed phrase for bulk, "skipped because…" list | rewards are irreversible once issued; nothing in the UI can undo |
| 9–10 | filter text, query, filter (3) → one search | search bar | single page with labelled sources and a ⚠ when two qualified counts diverge | search is exact ID/@username only; list capped by endpoints (200) |
| 11–13 | name, start, [provider, path], … (4–7) → name (+ provider, path, reward tick for tournaments) (1–4) | action bar | readiness checklist mirroring publish gates; ID auto | provider/destination still configured elsewhere (Providers) |
| 14 | ~10 → 3 (name, question, options; pool and winners default) | action bar | inventory bar, humanised eligibility | none new |
| 15 | ~7 → 2 (name, codes; template, release, start default) | action bar | Launch disabled until preview OK; auto-preview; typed `LAUNCH` kept | compile is irreversible: rollback deletes only the campaign doc, drops keep running |
| 16 | 2 → 2 | player card/search | name/ID + current→new XP, threshold T3, idempotency key | no reason field (API lacks one) |
| 17–19 | 0 | row menu | impact copy; end-time reset disclosed; claimed-count shown | `start_now` semantics unchanged |

## 2. Regression risk assessment

Likelihood/impact are my judgement from the code, **not** from production data. "Test" names the check that should exist before release.

| ID | Risk | L | I | Mitigation | Test / evidence |
|---|---|---|---|---|---|
| R-1 | **JS monolith + source-slicing tests.** ~40 JS tests slice `admin-dashboard.js` by text markers; a rewrite or moved function breaks them silently or loudly. | High | High | **Additive shell** (new `admin-shell.js` wrapping the existing view loaders and keeping every function the tests slice); migrate one flow at a time behind a flag; keep `VIEWS`/`MODULES` until the last flow moves | `node ci/run_js_tests.js` green on each PR; views-sync test updated in the same PR as nav changes |
| R-2 | **Capability loss** — features only in legacy panel (V4 edit dates, V5 delete drop, V8 bonus voucher, S7 retention KPIs/CSV, S8 past leaderboards) or without any UI (A10, reconcile-deletions, backfill-status). | Med | High | Traceability matrix (`04…§1.3`) enforced by a script that diffs every admin route in `routes` against the nav manifest; **Tools** area hosts the rest | CI check "every `/api/admin/*` and `/v2/miniapp/admin/*` route is referenced by a nav entry or is on an explicit allow-list" |
| R-3 | **Preview ≠ backend** (3 parser rules; two qualified-count sources). | Med | High | Port each server parser to JS **with shared golden vectors** run against Python and JS (`_normalize_codes`, `normalize_voucher_codes`, pool upload trim); reuse `voucher_drop_preview.js` unchanged | vector test file consumed by `test_vouchers.py`-style and node tests; fails if either side changes |
| R-4 | **No server-side idempotency** for create-drop/upload; two tabs or a retry after a network blip creates a second drop. | Med | High | UI guard + retained form + pending-submission lock; **recommend** server idempotency key (additive) as a separate approved change | manual double-click/two-tab script; later server test |
| R-5 | **Orphan drop on validation/duplicate failure** (insert before validate). | Med | Med | UI validates fully before send; failure screen links to the created drop; **recommend** reorder server validation (separate approval) | test with a duplicate code against fake Mongo (`fake_mongo.py`) |
| R-6 | **Confirmation fatigue** — more modals trained into reflex clicking. | Med | Med | Tiering: only T2/T3 confirm; T1 uses Undo; confirmations show *consequence*, not "Are you sure?"; typed phrase only for irreversible/at-scale | review after two weeks of use: count T3 cancels vs completions (client event) |
| R-7 | **Qualification-number confusion** — showing both `qualified_events` and referral-status counts looks like a rule change. | Med | Med | Distinct labels, ⚠ on divergence, tooltip naming the source; no rule touched | content review with ops/finance before launch |
| R-8 | **Dashboard load.** `GET /admin/affiliate/pending` runs 3 `count_documents` per row for up to 200 rows (≈600 queries per call) and is already polled every 90 s per open dashboard by the bell (`admin-dashboard.js:349-396`). Adding more Home tiles multiplies this. | Med | Med | Tiles use cheap count endpoints; ➕ aggregated `attention` endpoint with a short cache; keep poll ≥ 90 s and pause when tab hidden | measure in staging with the existing query-plan checklist (`test_affiliate_query_plan_checklist.py`) |
| R-9 | **Dangerous routes become more discoverable** (issue-current-month, snapshot regenerate, history rebuild, reconcile-deletions) and every admin can call them (no roles). | Low | High | Place in *Tools › Maintenance*, collapsed, T3 with typed phrase and a dry-run default where the API has one (`history/rebuild` defaults to dry run); **RBAC decision is yours** (out of scope) | route-diff CI check (R-2) lists them as "maintenance" |
| R-10 | **Time-zone presets** ("Tonight 8 pm") produce wrong instants. | Low | High | Reuse existing converters (`toKLLocalString`, `ccKlInputToUtcIso`) and always echo the resulting KL time in the review | unit tests around midnight/DST-free KL (+08:00) boundaries |
| R-11 | **Shared secret in URL.** The dashboard forwards `window.location.search` to some calls (`admin-dashboard.js:1626`). New shell must not widen this. | Low | Med | No new places read the query string; keep cookie session | grep gate in CI |
| R-12 | **Two engines, one picker.** "Create voucher drop" can route to direct create or Builder compile; behaviours differ (Builder stamps campaign metadata, supports batch release). | Med | Med | Router rule documented and visible ("Using: scheduled release engine"); each engine's payload built by its own existing code path | contract tests per route, unchanged |
| R-13 | **Mobile / Telegram in-app browser.** Admins may use phones; current layout is desktop-first. | Med | Low | Responsive design system (prototype is single-column ≤ 640 px) | manual check at 375 px |
| R-14 | **Mini App untouched** — legacy admin panel remains as fallback; do not remove it in this effort. | — | — | Out of scope by instruction | n/a |
| R-15 | **Rollback.** | Low | High | Per-flow feature flag in `app_settings` feature_flags group (existing mechanism), old views stay until two releases pass | flag test |

Data/production safety: nothing in this proposal runs against production; the prototype is a static file with mock data and no network calls (verified by a Playwright check recorded in the PR description).
