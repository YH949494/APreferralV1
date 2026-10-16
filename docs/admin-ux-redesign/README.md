# Admin Dashboard — Zero-Training UX Redesign

**Status: design only. No production code, schema, API contract, permission or data was changed.**
Everything in this folder is documentation plus one self-contained prototype that makes no network calls.
Implementation needs your approval (see "Decisions needed").

| # | Deliverable | File |
|---|---|---|
| 1 | Admin feature inventory + Phase 1 workflow audit | [01-audit-inventory.md](01-audit-inventory.md) |
| 2 | Current workflow map | [02-current-and-proposed-workflows.md §A](02-current-and-proposed-workflows.md) |
| 3 | Proposed workflow map (Phases 2, 5, 6, 7) | [02-current-and-proposed-workflows.md §B–F](02-current-and-proposed-workflows.md) |
| 4 | Before-and-after comparison (Phase 9) | [03-before-after-and-risk.md §1](03-before-after-and-risk.md) |
| 5 | Navigation redesign (Phase 3) | [04-navigation-and-design-system.md §1](04-navigation-and-design-system.md) |
| 6 | UI design system + smart-form rules (Phase 4) | [04-navigation-and-design-system.md §2–3](04-navigation-and-design-system.md) |
| 7 | Interactive prototype (Phase 8) | [prototype/index.html](prototype/index.html) — open in a browser |
| 8 | Regression risk assessment | [03-before-after-and-risk.md §2](03-before-after-and-risk.md) |
| 9 | Implementation roadmap + required API work | [05-roadmap.md](05-roadmap.md) |

## Prototype scope

`prototype/index.html` is one self-contained file (no libraries, no fonts, no network; it has no code path that can reach an API). It implements, with mock data: the Home overview with attention tiles and live-drop controls; **Upload voucher codes** (6 destinations, per-destination parsers, duplicate pre-check, confirm, result, release-waiting-rewards chain); **Create voucher drop**; **Create campaign** (picker, Details, readiness checklist, Review; Tournament, Registration, Mission, Link and Voucher campaign with typed `LAUNCH`); **Affiliate 360**; **Review rewards** (single, bulk-safe, bulk-reject with reason); **Add/remove XP**; tournament result approval. Other destinations show where today's tabs land.
Use **Demo controls** to trigger: network failure, expired session, a colleague handling the reward first, XP cooldown, duplicate pre-check on/off. Simplifications: the Builder's nine templates and spread-out release are shown as a reduced form; audience parameters, Mission automation options and the Settings/Content/Reports destinations are not built.
Proposed interaction counts in `03-before-after-and-risk.md` come from a scripted run of this file's shortest paths.

## How this was produced, and its limits

- Source of truth is the code at `main` (`1eb591f`): `static/admin-dashboard.{html,js}` (14.9k-line JS monolith, 13 modules / ~60 tabs), `static/mission-admin.js`, the legacy Mini App admin panel in `static/index.html`, and ~60 admin routes across `main.py`, `vouchers.py`, `campaign_*.py`, `affiliate_voucher_batches.py`, `tournament_rewards.py`, etc.
- **Code-traced in full** (UI handler → route → validation → collection): voucher drops, pool/batch uploads, affiliate pending/approve/reject/bulk/shortage, campaign wizard (gc_campaigns), Campaign Builder, Mission wizard, XP adjust, auth.
  **Inventoried at route + form-field level only** (not re-traced end to end): community composer, creator access, share content, lucky games, event banners, providers, segment/abuse analytics, settings. They are flagged as such in the inventory.
- I could not run the app or a database in this session. **Interaction counts are derived by reading the minimum happy path in the code** and are labelled *estimate*. **No time savings are claimed anywhere.**
- Existing docs (`docs/ADMIN_UNIFICATION_DESIGN.md`, `ADMIN_SAFETY_ARCHITECTURE.md`, `DASHBOARD_V2*.md`) already cover IA grouping and RBAC. This work builds on them rather than repeating them; RBAC stays out of scope (a "never modify" item).

## Headline findings (by severity, each with code evidence)

| Sev | Finding | Evidence |
|---|---|---|
| **High** | Rejecting a single affiliate reward cannot be cancelled: `prompt(...) \|\| ""` turns *Cancel* into an empty reason and the irreversible POST still fires. Bulk reject, by contrast, has three dialogs. | `admin-dashboard.js:12417` vs `:12315-12320` |
| **High** | Creating a pooled drop is not atomic. The drop document is inserted first; codes are validated afterwards. `db.vouchers.code` has a **global** unique index, so one already-used code returns `409 duplicate_code` *after* the drop (and the non-duplicate codes) already exist. The UI shows the raw string `duplicate_code`; a retry creates a **second** drop. | `vouchers.py:7612` (insert) → `:7639-7697` (codes), index `vouchers.py:745`, UI `admin-dashboard.js:4961` |
| **High** | `start_now` silently overwrites the drop's scheduled end with *now + 24 h* and goes live. Start and Pause have no confirmation; the **Start** button is rendered on every row, including already-active drops. | `vouchers.py:8849-8853`, `admin-dashboard.js:4767-4769,5002` |
| **High** | Duplicate-submit protection is client-only. The legacy panel sends an `idempotency_key` with the create-drop payload; the server never reads it. The new dashboard form sends none. | no `idempotency_key` handling in `vouchers.py`; `voucher_drop_preview.js` `DropSubmissionController` |
| **High** | The dashboard's "Create Drop" is a regression from the legacy Mini App panel it replaced: no Preview/Confirm, no eligibility mode (Welcome/VIP tier/User IDs/Admin-only), no region restriction, no hero title/subtitle/image, no whitelist, no CSV import, and no UI for *edit dates* or *delete drop* (both exist server-side and in the legacy panel only). | `admin-dashboard.html:472-553` vs `index.html:1848-1990`, `:6632`, `:6683` |
| Med | Voucher-code parsing differs per upload endpoint (3 distinct rules across 6 endpoints). Drops: per line, trims commas, silent dedupe, no whitespace check. Batches: splits on newline **and comma**, rejects internal whitespace. Legacy pool, reward-pool and reactivation uploads: per line, trim only (duplicates skipped by DB index). The same pasted text yields different counts per screen. | `vouchers.py:4577`, `affiliate_voucher_batches.py:154`, `vouchers.py:7826` |
| Med | XP adjust: no player lookup, no current→new preview, no confirmation, no idempotency key (server supports one). Reachable only via *Segments → Adjust XP*. | `admin-dashboard.js:12474-12497`, `main.py:7626-7656` |
| Med | The affiliate queue shows raw numeric user IDs only; the affiliate overview lists the 50 most recently updated ledgers with a client-side filter; the "Conversion" column is hard-wired to *Data Not Available*. Qualified counts come from two different sources (`qualified_events` in the queue, `pending_referrals` statuses in the player drill-down). | `vouchers.py:8231-8300`, `dashboard_panels.py:909`, `:3866` |
| Med | Five confirmation idioms with no consistent rule: ~23 native `confirm()`, ~13 `prompt()`, ~23 uses of two in-page modal helpers, plus typed `LAUNCH` and typed `REJECT n`. Grep counts, helper definitions included. | `admin-dashboard.js` (grep) |
| Med | ~43 sites interpolate raw backend codes into user-visible errors (`duplicate_code`, `bad_start`, `end_before_start`, `unknown`). | `admin-dashboard.js` (grep `d.code \|\| "unknown"`) |
| Med | Five different things are called "campaign" across three modules (saved audiences, Builder templates, Player Campaigns, Mission pools, Campaign Display). Builder steps are free-jump tabs, so a launch is possible without ever running Preview; *Preview* silently persists the draft; the "XP" reward type launches and creates nothing. | `admin-dashboard.js:12513-12612`, `:4661`, `campaign_builder.py:408-410` |
| Low | Jargon labels: raw enum values as option labels (`tournament_reward`, `campaign_rewards`, `external_subscription_verification`), mission eligibility checkboxes labelled `require_correct_answer`/`auto_draw`, free-text datetimes `YYYY-MM-DD HH:MM:SS` in Builder. | `admin-dashboard.html:2142-2160`, `mission-admin.js:955-959`, `admin-dashboard.html:314,318` |
| Low | Load: the affiliate pending endpoint runs 3 `count_documents` per row (≤ 200 rows ≈ 600 queries/call) and the notification bell calls it every 90 s per open dashboard. | `vouchers.py:8256-8262`, `admin-dashboard.js:349-396` |
| Low | Home "Attention Required" has 3 signals (pool low, paused batch campaign, pending rewards) — pending count is capped at 200 by the endpoint's `limit(200)`; Home items are not clickable (bell items are). Shortage, tournament results awaiting review, failed community posts, live drops about to end are not surfaced. | `admin-dashboard.js:280-340`, `vouchers.py:8256` |

## What the redesign does (one paragraph)

Replace 13 module-centric menus with **8 task-centric destinations** (Home, Campaigns, Vouchers, Affiliates, Players, Content & Community, Reports, Settings & Tools) topped by a **"What do you want to do?"** action bar and a global **player/affiliate search**. Routine work becomes 1–3 stage flows with live inline validation, a plain-language review screen and one consistent confirmation component whose strictness scales with irreversibility. Everything maps onto **existing endpoints**; the gaps are named: 4 optional additive read-only endpoints/fields and 3 behavioural hardening items that need your separate approval (see roadmap §API work). All existing capabilities remain reachable (traceability matrix in the inventory).

## Decisions needed before any implementation

1. **Approve the 8-destination IA** (or amend) — drives everything else.
2. **Fix the five High findings first as a standalone, tiny PR series** regardless of the redesign? (Recommended: yes — each is a few lines on the client or an additive server check. These are changes to production behaviour, so I have not touched them.)
3. **Server-side duplicate-code pre-check endpoint** (additive, read-only) — required for the voucher preview to be fully faithful. OK to add?
4. **Shell strategy**: additive new shell that wraps existing view loaders (recommended, keeps the ~40 JS test files green) vs rewrite.
5. Confirm that the `LAUNCH` typed confirmation (Builder) and the bulk-reject typed phrase should be **kept** as the "irreversible" tier.
