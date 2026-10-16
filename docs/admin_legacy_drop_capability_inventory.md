# Legacy Mini App drop management vs. admin dashboard — capability inventory

Scope: Phase 1 / P1 "preserve missing legacy drop-management capabilities". **Inventory and compatibility preparation only.** No dashboard feature work, no backend behaviour change. The full UI redesign (PR #554 direction) is a later phase.

Sources read: `static/index.html` (Mini App "Voucher Drops" admin panel: create form ~1860–1955, list `adm_dropRowHtml`, preview/confirm, edit-dates and delete modals, add-data), `static/js/voucher_drop_preview.js`, `static/admin-dashboard.html` / `static/admin-dashboard.js` (Voucher Drops view), `vouchers.py` admin drop endpoints.

## 1. Backend surface both UIs share (unchanged)

| Endpoint | Used by legacy panel | Used by dashboard |
|---|---|---|
| `GET /v2/miniapp/admin/drops_v2` | list | list |
| `POST /v2/miniapp/admin/drops` | create (after preview/confirm) | create (no preview) |
| `POST /v2/miniapp/admin/drops/<id>/codes` | add data | add codes |
| `POST …/<id>/actions` `op=start_now / pause / end_now` | yes | yes |
| `POST …/<id>/actions` `op=set_dates` (name + startsAt + endsAt) | **yes (Edit)** | **no** |
| `POST …/<id>/actions` `op=delete` (resumable, orphan recovery) | **yes (Delete)** | **no** |
| `POST /v2/miniapp/admin/drops/reconcile-deletions` | – | – (server tool) |

## 2. Capability matrix

| # | Legacy capability | Dashboard today | Gap | Backend ready? |
|---|---|---|---|---|
| 1 | Create **preview + confirm** modal (summary, calculated state, duration, sample entries, blocking errors, warnings: start in past, ends <1h, no region restriction, public pool without whitelist, tiny inventory) | Direct "Create Drop" button, no preview | **Missing** | Pure client logic in `voucher_drop_preview.js` (reusable as is) |
| 2 | Eligibility mode: public / **Welcome-New joiner** / VIP tier / User IDs / Admin-preview-only, with values | Not exposed (always public) | **Missing** | Yes (`eligibility`, new_joiner shortcut) |
| 3 | **Allowed regions** (Malaysia/Thailand/Indonesia/Other) | Not exposed | **Missing** | Yes (`audience.regions`) |
| 4 | **Hero** title / subtitle / image | Not exposed | **Missing** | Yes (`hero_*`) |
| 5 | **Whitelist usernames** (pooled) | Not exposed | **Missing** | Yes (`whitelistUsernames`) |
| 6 | **CSV/TXT import** for pairs and codes | Paste only | **Missing** | Client only |
| 7 | Client-side duplicate detection (usernames, codes, malformed rows) | Server-side only (explained in plain language since the atomic-create PR) | Partial | – |
| 8 | **Edit name + start/end** | No UI | **Missing** | Yes (`set_dates`) |
| 9 | **Delete drop** with typed-name confirmation, claimed/total counts, "Retry delete" state, partial-cleanup message | No UI; "deleting…" state not shown | **Missing** | Yes (`delete`, resumable) |
| 10 | **Copy drop ID** | No | **Missing** | – |
| 11 | Add data to a drop: pooled **and personalised assignments** | Pooled only | Personalised **broken on both** (see §3) | **No** |
| 12 | Start / Pause / End | Yes; Start/Resume semantics fixed in the lifecycle PR | Covered | Yes |
| 13 | Row stats: free/total, claimed/assigned, priority | Yes | Covered | – |
| 14 | Mission Pool rows merged into the same list | Yes | Covered (dashboard has more) | – |

Dashboard-only capabilities (must not be lost either): mission-pool actions in the same list, idempotent writes, plain-language failure text, rollback-safe creation.

## 3. Pre-existing defect found during the inventory (not fixed here)

`POST /admin/drops/<id>/codes` accepts **pooled drops only** (`400 bad_request / bad_type` otherwise), but the Mini App "Personalised assignments" add-data option posts `{type:"personalised", assignments:[…]}` to it. So adding assignments to an existing personalised drop has been failing in the legacy panel. Pinned as a strict `xfail` in `test_legacy_drop_admin_contract.py::test_known_gap_adding_personalised_assignments_to_an_existing_drop` (it will start XPASS-failing the day someone fixes it, forcing this doc to be updated). Fixing it is feature work (needs collision handling for `uniq_personalised_assignment`, audit, atomicity per the creation design) and a product call, so it is intentionally **out of Phase 1**. Workaround today: create a new personalised drop.

## 4. Compatibility work done in this PR

* `test_legacy_drop_admin_contract.py`: 18 tests + 1 strict xfail that pin the contract the Mini App panel relies on — endpoints and ops referenced by `index.html`, every eligibility mode, new-joiner shortcut mapping, regions on `audience`, hero/whitelist fields, 24h default end, `drops_v2` row keys, `set_dates` validation, delete counts and `codes_claimed` 409, add pooled codes. They assert contract, not behaviour the hotfixes intentionally changed, and pass on `main` and on each Phase 1 hotfix branch (verified individually).
* Nothing in `static/index.html` or any API was modified. **Do not remove or restyle the Mini App panel until rows 1–11 above have dashboard equivalents.**

## 5. Recommended order for the dashboard parity work (next phase, not started)

1. Row actions that need no new design: **Edit name/dates** (8), **Copy ID** (10), **Delete with typed confirmation** (9) — all backend-ready; reuse the lifecycle PR's `availableOps` and the confirm pattern.
2. **Create form parity** (2–5) behind the existing payload shape; reuse `voucher_drop_preview.js` for **preview + warnings** (1, 7).
3. **CSV import** (6).
4. Decide on row 11 (personalised add-data) as a product + backend change.

Each step ships with the contract tests above kept green.
