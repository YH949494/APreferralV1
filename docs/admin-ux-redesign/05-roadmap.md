# 5. Implementation roadmap (proposal — nothing here starts without your approval)

Sizing is relative (S ≈ a day or two of focused work, M ≈ a week, L ≈ several weeks); these are **planning guesses, not measurements**.

## 0. Principles
- **Additive shell**, not a rewrite: new files wrap existing view loaders; old tabs keep working until each flow is migrated (risk R-1).
- **One flow per PR**, each behind a flag in the existing `feature_flags` settings group, each independently revertible (R-15).
- No change to business logic, reward/qualification rules, issuance, schemas, auth/permissions, API contracts or the Mini App unless an item is explicitly listed under *API work* and approved.
- Every phase ends green on `ci/run_js_tests.js` and the Python suite; nothing deploys or merges without you.

## 1. API work — the complete list

| ID | Item | Type | Needed for | Approval needed |
|---|---|---|---|---|
| A-1 | `POST /admin/vouchers/check-codes` — dry-run: which of these codes already exist (per destination collection/unique index), no writes | additive, read-only | exact duplicate preview (Upload codes, Create drop) | ok to add? |
| A-2 | `GET /api/admin/dashboard/attention` — one cached read returning counts (rewards to review, shortages, results to approve, low stock, failed posts, drops ending) | additive, read-only | Home tiles without N calls / the 200-row cap (R-8) | ok to add? |
| A-3 | `username` (and `first_name`) on `GET /admin/affiliate/pending` rows | additive field | names in the review queue | ok to add? |
| A-4 | `GET /admin/drops/<id>/claims` | additive, read-only | "who claimed" from a drop | optional |
| A-5 | Server-side idempotency for `POST /admin/drops` and code uploads (honour `idempotency_key`) | **behaviour** | true duplicate-submit protection (R-4) | **yes** — changes a production write path |
| A-6 | Validate codes **before** inserting the drop (or roll back on failure); until then return `dropId`, `inserted`, `duplicates` on **every** failure response of `POST /admin/drops` | **behaviour** (the field addition is additive) | removes orphan drops (R-5); lets the UI open the leftover drop | **yes** |
| A-8 | `resume` op on `POST /admin/drops/<id>/actions` that restores a paused drop **without** touching its dates | **behaviour** | a true Undo for drop Pause; avoids the silent end-time reset of `start_now` | **yes** |
| A-7 | Per-action admin audit rows for drops / affiliate approve-reject / XP / Builder launch | **behaviour (audit)** | "recent activity" with who did what | **yes** — touches audit/permissions area |

Everything else in the prototype maps to endpoints that exist today (`02 §H`).

## 2. Phases

### Phase 0 — Safety hotfixes (independent of the redesign; each ≤ S)
| # | Change | Where | Test |
|---|---|---|---|
| 0.1 | Single reject: Cancel must abort; add required reason | `admin-dashboard.js:12417` | new JS test beside `test_admin_dashboard_p0_8_action_hardening` |
| 0.2 | Drops: hide **Start** on live rows; confirm Start/Pause with end-time-reset copy; map `duplicate_code`, `bad_start`, `bad_end`, `end_before_start`, `empty_codes` to sentences | `admin-dashboard.js:4754-4771, 4960, 4988, 5002` | extend `test_admin_mini_app_existing_drops.test.js` |
| 0.3 | XP: resolve player, show current → new, confirm, send `idempotency_key` | `admin-dashboard.js:12474` | new JS test |
| 0.4 | Dashboard Create Drop: reuse `static/js/voucher_drop_preview.js` (preview + confirm + `DropSubmissionController`) and restore eligibility/regions/hero/whitelist fields | `admin-dashboard.html:472-553`, `.js:4900-4965` | existing `voucher_drop_preview.test.js` + new wiring test |
| 0.5 | Restore *Edit dates* and *Delete drop* in the dashboard (endpoints exist) | same | new JS tests |
Value: closes all five High findings without waiting for the shell.

### Phase 1 — Foundations (additive, no visible change) — M
- `admin-ui-kit.js`: tiered confirm component (wrapping existing `confirmTyped`/`confirmSimple`), error dictionary (merging existing maps), code-box with pluggable parser, date-time presets, result panel, toast+Undo.
- **Parser parity vectors**: one JSON file of inputs/expected outputs consumed by a Python test (`_normalize_codes`, `normalize_voucher_codes`, pool trim) **and** a Node test of the JS ports (R-3).
- **Route manifest check** (R-2): script lists every `/api/admin/*` and `/v2/miniapp/admin/*` route, fails CI if one is neither mapped to a nav entry nor on an explicit allow-list.
- Optional A-1 / A-2 / A-3 land here (additive).
Exit: tests green; no UI behaviour change.

### Phase 2 — Shell, Home, Search — M
New nav (8 destinations) rendering existing views through `activateTab` unchanged; Home tiles; global search over `dashboard/user`; Tools › Maintenance with T3. Flag `admin_shell_v2` per admin browser preference first.
Exit: every inventory row reachable (route manifest check), views-sync test updated.

### Phase 3 — Vouchers — L
Upload voucher codes (all destinations), Create drop (3-stage), Drops manage menu, stock strip. Needs A-1 for exact duplicates (degrades gracefully without).
Exit: golden-vector tests; manual double-submit scripts; staging run against a copy of data.

### Phase 4 — Affiliates — L
Affiliate 360, Review rewards (username, reasons, safe-bulk), shortage → upload → release chain. Uses existing endpoints; A-3 optional.
Exit: finance/ops review of the two-count labels (R-7).

### Phase 5 — Campaigns — L
Picker + 3-stage flows for Player Campaigns and Builder, Mission humanised, readiness checklist. No changes to `_VALID_STATUS_TRANSITIONS` or compile logic.

### Phase 6 — Players, Content, Reports, Settings relabel/regroup — M
Mostly navigation + label dictionary + T-tier adoption for remaining `confirm()/prompt()` sites.

### Phase 7 — Burn-in and retirement — S
Two releases with old and new available; then remove old nav only (keep the legacy Mini App panel until you decide separately).

## 3. Validation plan (this is where the "zero-training" claim gets measured)
No timing data exists today, so the first deliverable of Phase 2 is **instrumentation + a usability protocol**, not a claim:
1. **Unmoderated task test** with 5 non-technical staff, no briefing, on staging with seeded data: *upload 50 codes to a drop · create a drop for tomorrow 9 am · approve one reward · find an affiliate and see why they have no reward · add 100 XP · fix a shortage*.
2. Record per task: completed unaided (Y/N), wrong-path clicks, errors shown, hesitation >10 s points, cancelled T3 confirms (client events, no PII).
3. **Acceptance:** ≥ 80% unaided completion per task and zero incorrect irreversible actions; anything below → redesign that step, not training.
4. Compare to the same six tasks on the current dashboard (same staff, counterbalanced).

## 4. Open questions for you
1. Approve IA (8 destinations) and the label dictionary — or edit it.
2. Which Phase-0 hotfixes may proceed now?
3. A-1…A-4 additive endpoints OK? A-5…A-8 (behaviour) — separate approval each.
4. Confirmation tiers and the XP threshold (suggested 1,000).
5. VIP tier list to offer as chips (no canonical server list exists).
6. Should *Maintenance* actions (issue-current-month, snapshot regenerate…) stay UI-hidden and CLI-only instead?
7. Keep the legacy Mini App admin panel after Phase 7?
