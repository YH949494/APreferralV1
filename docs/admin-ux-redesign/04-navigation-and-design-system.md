# 4. Navigation redesign, design system, smart-form rules (Phases 3 + 4)

Scope guard: **labels only**. No route, JSON field, collection, enum value, `data-*` hook or `MODULES` key is renamed by this document. The label dictionary is a UI-side lookup.

## 1. Navigation

### 1.1 Principles
1. Organise by **what the admin is trying to do**, not by which backend module owns the data.
2. **Top-level = 8 destinations** (today: 13 modules in 5 groups, ~60 tabs). Each destination ≤ 6 sections.
3. Every page offers the **next likely action** inline; cross-module hops disappear.
4. Two always-visible shortcuts: **What do you want to do?** (action bar, 6 verbs) and **Search** (player / affiliate / drop / campaign).
5. Anything dangerous or rare lives in **Settings & Tools › Maintenance**, collapsed, with T3 confirmation.

### 1.2 Map (current → proposed)

```
Home                          ← Dashboard › Overview, Live Activity (as "Recent activity"), Alerts tiles
 ├─ Needs your attention      (rewards to review · waiting for vouchers · results to approve · low stock · failed posts · drops ending soon)
 ├─ Live now & coming up      (drops, campaigns, missions)  ← inline Pause / End / Start
 └─ Quick actions             Create campaign · Upload voucher codes · Create voucher drop · Review rewards · Find a player · Add/remove XP

Campaigns                     ← Campaign Centre + Player Campaigns + Reactivation
 ├─ All campaigns             (one list; type filter: Tournament · Registration · Mission · Link · Voucher · Win-back)
 ├─ Create campaign           (picker → 3-stage flow)
 ├─ Results & rewards         ← Tournament Results, Rewards allocations
 ├─ Sign-ups & links          ← Registrations, Deep Links
 ├─ Partners                  ← Providers, Verification
 ├─ Saved audiences           ← "Legacy Targeting"
 └─ Insights                  ← Performance, Intelligence, Activity Log

Vouchers                      ← Voucher Centre + parts of Affiliate/Rewards
 ├─ Upload codes              (single front door)
 ├─ Drops                     (live · scheduled · paused · ended; manage menu)
 ├─ Stock & pools             ← Affiliate Voucher Pools, Reward pools registry, pool summary
 ├─ Code usage                ← Voucher Codes
 └─ Batches                   ← Affiliate Voucher Batches (history, edit schedule, disable, reconcile)

Affiliates                    ← Affiliate Centre
 ├─ Find an affiliate         (Affiliate 360)
 ├─ Review rewards            ← Pending Approval (queue)
 ├─ Voucher stock             (shortages → upload → release)
 ├─ Check qualification rules ← Qualification Preview
 └─ Leaderboard display       ← Campaign Display

Players                       ← Segments + Referral Centre + Welcome Journey + Join Requests
 ├─ Find a player             ← All Players
 ├─ Add or remove XP
 ├─ Join requests
 ├─ Player groups             ← Segments overview, Segment probability
 ├─ Suspicious accounts       ← the 6 "VH:" tabs + Simulator, grouped
 ├─ Referrals                 ← Referral Performance
 └─ New-player journey        ← Welcome Journey, Funnel

Content & Community           ← Community Centre + Share Content + banners/games
 ├─ Write a post / Calendar / All posts / Poll results
 ├─ Media library, Destinations
 ├─ Share messages & creators ← Referral Share Content, Creator Access, Share Engagement
 └─ Home-screen banners · Lucky games

Reports                       ← Analytics + parts of Dashboard/Referral
 ├─ Business summary          ← Overview cards
 ├─ Return on rewards         ← Segment ROI
 ├─ Player cohorts            ← Backend Segment Engine (read views)
 ├─ Data checks               ← Validation, KPI gap report
 └─ Retention & exports       ← retention KPIs, joins export, CSV, past leaderboards

Settings & Tools              ← Settings (10 tabs) + Automation
 ├─ Settings                  (grouped by task; default vs current shown)
 ├─ Data imports              ← Upload Player Performance, UIM import, history, Raw Explorer
 ├─ Engine runs               ← Backend Segment Engine run/status
 ├─ System health             ← Runtime status
 ├─ Admin activity            ← login/audit panels
 └─ Maintenance (collapsed)   ← issue-current-month · rebuild affiliate leaderboard snapshot · rebuild weekly history · reconcile drop deletions · backfill status · bonus voucher · legacy pool upload/replace · Legacy Mini App panel ↗
```

### 1.3 Traceability — every existing capability has a home

| Inventory | Capability | New location |
|---|---|---|
| D1–D5 | Overview, attention, activity, alerts, health | Home; Reports › Business summary; Settings & Tools › Admin activity / System health |
| C1 | Saved audiences | Campaigns › Saved audiences |
| C2, C3, C4 | Builder, batch controls, compiled drops | Campaigns › Create campaign (Voucher); Campaigns › All campaigns › Manage |
| C5 | Performance / Intelligence | Campaigns › Insights |
| P1–P3 | Player campaign create/publish; Mission | Campaigns › Create campaign; All campaigns › Manage |
| P4, P5 | Registrations, deep links | Campaigns › Sign-ups & links |
| P6, P9 | Providers, verification, activity log | Campaigns › Partners / Insights |
| P7, P8 | Tournament results, reward pools, allocations | Campaigns › Results & rewards; Vouchers › Stock & pools |
| P10, P11 | Event banners, lucky games | Content & Community |
| V1, V2, V3 | Create drop, add codes, start/pause/end | Vouchers › Upload codes / Drops |
| **V4, V5** | **Edit dates, delete drop (legacy-only today)** | **Vouchers › Drops › Manage** |
| V6, V7 | Drop list, code usage | Vouchers › Drops / Code usage |
| **V8** | **Bonus voucher (legacy-only)** | **Settings & Tools › Maintenance** (T3: shows the code being replaced) |
| V9 | Rejoin buffer | Settings › Voucher limits |
| A1–A5 | Affiliate overview, approve/reject, bulk, replenish, shortage | Affiliates › Find / Review rewards / Voucher stock |
| A6, A7 | Batches, pools | Vouchers › Upload codes, Batches, Stock & pools |
| A8, A9 | Qualification preview, display override | Affiliates |
| **A10** | **issue-current-month, snapshot regenerate, history rebuild, legacy pool upload/replace, KPI daily** | **Settings & Tools › Maintenance / Reports** |
| R1–R4 | Referral performance, share content, engagement, creators | Players › Referrals; Content & Community › Share messages & creators |
| M1 | Community Centre | Content & Community |
| W1, X1 | Welcome journey, reactivation | Players › New-player journey; Campaigns (Win-back) |
| S1, S2, S3 | Player drill-down, XP, join requests | Players |
| S4, S5 | Audits/simulators; engine run, UIM, uploads | Players › Suspicious accounts; Settings & Tools |
| S6 | Settings + audit log | Settings & Tools › Settings |
| **S7, S8** | **Retention KPIs/export, CSV, past leaderboards (legacy-only)** | **Reports › Retention & exports** |
| Z1 | Login | unchanged |

### 1.4 Label dictionary (interface text only)

| Today (shown) | Proposed | Why |
|---|---|---|
| Dashboard / Overview | Home | |
| Campaign Centre + Player Campaigns | Campaigns | five "campaign" words → one place |
| Legacy Targeting | Saved audiences | it is an audience filter, not a launchable campaign |
| Templates (Builder) | Voucher campaigns | |
| Intelligence | Insights | |
| Deep Links | Campaign links | |
| Mission Reward Pool | Missions | |
| Providers · Verification | Partners · Partner connection status | |
| Tournament Results | Tournament results to review | |
| Rewards (pool registry + allocations) | Reward stock & winners | |
| Activity Log | Campaign activity | |
| Event Banner | Home-screen banners | |
| Voucher Centre | Vouchers | |
| Active Drops | Drops | includes scheduled/paused |
| Voucher Pools (compiled drops) | Drops made by campaigns | the tab does not show pools |
| Voucher Codes | Code usage | |
| Pool Bulk Import / Voucher Batches / Add Codes (5 forms) | **Upload voucher codes** | one front door |
| Affiliate Centre | Affiliates | |
| Pending Approval | **Review rewards** | |
| PENDING_REVIEW · PENDING_MANUAL · OUT_OF_STOCK · SIMULATED_PENDING | Needs your review · Waiting for vouchers · Ran out of vouchers · Simulated (no reward paid) | enum values stay in the payload |
| Voucher Pools (affiliate, read-only) | Voucher stock | |
| Qualification Preview | Check qualification rules (preview) | |
| Campaign Display | Leaderboard display | |
| Entitlement Month | Reward month — the month the affiliate earned the reward | |
| Segments / Adjust XP / All Players | Players / Add or remove XP / Find a player | |
| VH: Mismatch Audit / Unclassified / Rule Quality / False Positive / Rule Simulator / Priority Impact | Suspicious accounts: mismatches / unsorted players / rule quality / wrongly flagged / what-if rules / review order | "VH" is jargon |
| Automation | Settings & Tools | |
| Join Requests (under Automation) | Players › Join requests | |
| Cohorts / ROI / Data Validation | Player cohorts / Return on rewards / Data checks | |
| Feature Flags / Voucher Rules / Security / Segment Probability | Switches / Voucher limits / Claim protection / Reward odds by player type | |
| Pool: `public` / `my` | Everyone / Malaysia reserve | matches legacy "MY Reserve" |
| Priority (drops) | Display order (higher shows first) | |
| Eligibility Mode: `tier`, `user_id`, `admin_only`, `new_joiner` | A VIP tier · Specific player IDs · Admins only (test) · New joiners | |
| `require_correct_answer` … `auto_issue` (Mission) | sentences in `02 §E.3` | |
| Pool type `tournament_reward`, scope `campaign_rewards`, `shared` | Tournament reward · "Only campaign rewards can use these codes" · "Several features can use these codes" | scope is irreversible once codes exist → add warning |
| Campaign ID / `campaign_id` inputs | Campaign (picker by name) | typed IDs only under Advanced |
| Start / End "(KL)" | Starts / Ends (Malaysia time) | |

## 2. Design system

### 2.1 Tokens (prototype implements these; light + dark)
| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg` | #f6f7f9 | #0f1216 | page |
| `--panel` | #ffffff | #171b21 | cards, modals |
| `--ink` / `--ink-2` | #14181f / #4a5361 | #e8ebf0 / #a3adbb | text / secondary |
| `--line` | #dde1e7 | #2a313b | borders |
| `--brand` | #2557d6 | #6f97ff | primary actions, focus ring |
| `--ok` `--warn` `--bad` `--info` | #1a7f4b `#b26b00` `#c0322b` `#2557d6` | brighter equivalents | status (always **icon + text**, never colour alone) |
| `--live` | #c0322b | #ff7b72 | the "LIVE — players see this now" marker, the only red used for non-errors |
Spacing 4/8/12/16/24/32 · radius 10 (cards) / 8 (controls) · type: system UI, 14/16 body, 20/24 titles, tabular numerals for counts.
Motion: ≤ 150 ms, disabled under `prefers-reduced-motion`.

### 2.2 Components
| Component | Rules |
|---|---|
| **Action bar** | 6 verbs max; each opens a flow, never a menu of menus |
| **Needs-attention tile** | big number + plain sentence + one button; severity icon; sorted by money/risk first |
| **Status pill** | fixed vocabulary: Live · Scheduled · Paused · Ended · Draft · Needs review · Waiting · Failed; icon + text |
| **Stage header** | "Step 2 of 3 · Vouchers & timing"; only on flows with > 1 stage; click-back allowed, forward gated by validation |
| **Field** | label (verb/noun, no jargon) · one-line help · control · inline error; required marked in words ("Required"), not only `*` |
| **Code box** | monospace textarea + live parse table (usable / skipped / invalid with reasons, first 5 samples) + file drop; counts equal server counts |
| **Date-time** | presets + picker; always echoes "Thu 22 Oct, 9:00 am (Malaysia time)" |
| **Review panel** | read-only summary in sentences; blocking errors (✖) vs warnings (⚠) separated; primary button = the action |
| **Confirm modal (T2/T3)** | title = consequence; list of what changes; counts; typed phrase field (T3) with exact phrase shown; focus trapped; Esc = cancel; primary disabled until valid; in-flight state locks the modal |
| **Toast + Undo (T1)** | 8 s; only where a true inverse endpoint exists (campaign/mission/batch pause→resume, banner). **Drops have none** (resume = `start_now`, which resets dates), so drop Pause is T2 |
| **Result summary** | what happened, numbers, next-step buttons, "copy summary" |
| **Empty / loading / error** | skeleton ≤ 400 ms; empty states say what to do; errors say what is *known*: after a server rejection "Nothing was changed"; after a dropped connection or unknown error **"we can't tell whether this was saved. Check the list before retrying"** (a write may have committed before the response was lost). Retry is offered, with that warning on create flows |
| **Search** | `/` focuses; accepts @username / numeric ID; shows what it can't do ("exact match only") |

### 2.3 Copy rules
1. Verb + object on buttons ("Upload 480 codes"), never "Submit"/"OK".
2. Say **who is affected and when** ("1,284 players can claim from 9:00 am").
3. Numbers always with units and source ("qualified this month", "codes left").
4. No raw codes, enum values or IDs in sentences; IDs available via "Copy ID".
5. Errors = what happened + what is known about the result + what to do. Say "nothing was changed" only when the server clearly rejected the request.
6. Time always "Malaysia time"; relative time only as a secondary hint.

### 2.4 Error dictionary (server code → message; source of each code verified in repo)
| Code | Message |
|---|---|
| `duplicate_code` (409, drops) | "Some of these codes are already used in another voucher drop. A drop named {name} may have been created with part of your codes. Open Drops and check before trying again." *(server inserts before it fails and returns no `dropId`)* |
| `empty_codes` · `no_codes` | "No usable codes found. Paste one code per line." |
| `bad_start` · `bad_end` · `invalid_start_at` · `invalid_end_at` | "That date or time isn't valid. Pick it from the calendar." |
| `end_before_start` | "The end must be after the start." |
| `bad_pool` · `bad_type` | "This drop doesn't accept codes in that group." |
| `drop_deleting` | "This drop is being deleted — wait for it to finish." |
| `not_found` | "That item no longer exists. Refresh the list." |
| `batch_window_overlap` etc. | existing `AFF_BATCH_ERROR_MESSAGES` entries reused verbatim |
| `denomination_pool_requires_scheduled_batch` | "$5/$10/$50 codes must be uploaded for a specific reward month." |
| `reward_rules_required` · `destination_not_ready` · `provider_inactive` · `provider_base_url_required` · `mission_config_required` · `mission_pool_config_required` | readiness checklist items with *Fix* links |
| `invalid_status_transition` | "This campaign can't move from {from} to {to}." |
| `not_pending_review` | "Someone else already handled this." |
| `insufficient_stock` | "Not enough vouchers: need {need}, have {have} in {pool}." |
| `confirmation_required` | (never shown; typed phrase checked client-side first) |
| `ledger_ids_required` · `too_many_ledger_ids` · `bad_ledger_id` | "Select 1–200 rewards." |
| `replace_in_progress` | "Another upload to this pool is running. Try again in a minute." |
| HTTP 429 (XP cooldown) | "You can adjust XP again in {n} seconds." |
| `unauthorized` · `auth_failed` · 401/403 | "Your session expired. Sign in again." |
| *network error / timeout / unknown* | "The connection dropped, so we can't tell whether this was saved. Check {list} before you try again. Support code: {code}." |

## 3. Smart-form checklist (Phase 4) — how each rule is applied

| Rule | Application |
|---|---|
| Context-sensitive fields | Show only what the chosen option needs: month chips only for $5/$10/$50; window only for Welcome; provider only for Tournament/Link; automation only for random-draw missions (these conditions already exist in the code). |
| Safe defaults | Pool *Everyone*; end +24 h (server default, stated); priority 100; "No end date" for player campaigns; campaign ID auto; batch name auto; new campaigns **draft**; `distribution_disabled=false`. |
| Must stay explicit | Reward month, destination of codes, who can claim, the codes, start choice (*now* vs scheduled, shown in review), winners count, publish/launch, any reject reason. |
| Inline validation | Ports of server rules (parsers, end>start, window overlap hint from the batches list, inventory ≥ winners, ID slug rules `gcCampaignIdIsLinkSafe`). |
| Clear descriptions | One-line help under each non-obvious field; no tooltips-only information. |
| Plain errors | §2.4 dictionary. |
| Automatic formatting | trim whitespace; strip leading `@`; strip leading/trailing commas on drop codes; split commas on batch codes; month → `YYYYMM`; local date-time → `YYYY-MM-DD HH:MM:SS`; slug generation. Never changes a code's characters or case. |
| Preview before important actions | Drops (existing `buildDropPreview`), uploads (parse table), Builder (existing server preview), publish (readiness), XP (current → new), approvals (what will be issued). |
| Confirmation | Tiers T1–T3 (`02 §B.1`). |
| Success summaries | Result panel with counts and next-step buttons. |
| No unnecessary wizards | Single screen for simple tasks (upload codes, XP, approve); ≤ 3 stages otherwise (create drop, campaigns). |
| Direct-edit option | "Show request" toggle on create/edit forms displays the exact payload; fields the API accepts can be edited as JSON; **server validation still decides**; hidden unless admin enables *Expert mode* (per-browser preference). |
