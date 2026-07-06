# Campaign Builder — Phase 2 (P2) Implementation

Status: **Implemented.** Single-admin, production-safe. Compiles into the
existing `vouchers.py` drop engine — does not redesign it.

---

# Summary

P2 adds a Campaign Builder authoring layer on top of the existing voucher
drop engine:

```
Campaign (draft)
    -> Campaign Compiler
    -> existing Voucher Drop(s) (same insert primitive admin_create_drop uses)
    -> vouchers.py executes normally (claim/eligibility/scheduler/affiliate untouched)
```

Nothing in the claim path, FCFS allocation, eligibility engine, anti-abuse
logic, scheduler core, affiliate settlement, or welcome voucher logic was
modified. The only change inside `vouchers.py` is a pure, behavior-preserving
refactor: the body of `admin_create_drop` was factored into a new function
`create_drop_from_spec()` so the compiler can create a drop without going
through HTTP — the HTTP endpoint calls the same function and returns the
identical response it always did.

---

# Files Changed

| File | Change |
|---|---|
| `vouchers.py` | Extracted `create_drop_from_spec(data) -> (json, status)` out of `admin_create_drop` (pure refactor, identical behavior). Added `campaign_id`/`campaign_name` to the additive pass-through keys in `_admin_drop_summary` so Voucher Drop Manager / Compiled Voucher Drops can show them. |
| `campaign_builder.py` | **New.** Campaign templates, wizard vocabulary, audience/reward resolution, the compiler (`compile_campaign`), preview (`preview_campaign`), and the Flask blueprint (`campaign_builder_bp`). |
| `main.py` | Registered `campaign_builder_bp`. |
| `static/admin-dashboard.html` | Added 4 nav items (Campaign Builder, Active Campaigns, Draft Campaigns, Compiled Voucher Drops) under Campaign Control; relabeled (not removed) the legacy items for clarity. Added the 4 corresponding view sections + 6-step wizard markup. |
| `static/admin-dashboard.js` | Added the Campaign Builder wizard/list client logic (`cb*` functions), wired into the existing view-switching framework. |
| `test_campaign_builder.py` | **New.** Unit tests for the compiler against an in-memory fake Mongo (no real DB needed). |
| `docs/CAMPAIGN_BUILDER_P2_IMPLEMENTATION.md` | This document. |

Nothing was removed: Voucher Drop Manager, the legacy segment-targeting
"Campaigns" dashboard (`campaigns.py`/`campaign_engine.py`), the legacy
MiniApp admin, and existing drop APIs are all untouched and still reachable.

---

# Campaign Templates

All 9 required templates, each a preset of Step 2–4 defaults an admin can
still override before compiling (see `TEMPLATE_DEFAULTS` in
`campaign_builder.py`):

| Template | Audience default | Release default | Reward default | Notes |
|---|---|---|---|---|
| Smart Default | Smart Segment % | Immediate | Voucher Pool | Uses `config.py`'s existing `BOT_SEGMENT_PROBABILITY_MAP`/`SEGMENT_PROBABILITY_CONFIG` as-is, displayed read-only in Preview. |
| Public | No Segment Filter | Immediate | Voucher Pool | Plain public pooled drop. |
| Welcome | (locked) | Immediate | Voucher Pool | Reuses the existing `eligibility.mode="new_joiner"` shortcut verbatim — same welcome-voucher machinery, untouched. |
| Segment | Smart Segment % + segment picker | Immediate/Scheduled | Voucher Pool | Restricts to chosen backend segments by resolving `backend_segment_snapshots` members into `eligibility.mode="user_id"` (an enforcement path `vouchers.py` already implements) — one drop per segment. |
| Affiliate | Whitelist | Immediate | Affiliate Reward Pool | Admin pastes affiliate-referred usernames (from the existing Affiliate Dashboard); does not read/write affiliate settlement collections. |
| Personalised | (locked) | Immediate | Personalised Voucher | 1:1 `assignments`, identical to today's path. |
| FCFS | No Segment Filter | Immediate | Voucher Pool | Plain pooled drop; "FCFS" is a UI label, not new claim behavior. |
| Surprise | Smart Segment % | Scheduled | Voucher Pool | Drop stays `upcoming` until `startsAt`, flipped by the existing scheduler sweep — no new "manual reveal" (that's P3). |
| Test | Admin Only (locked) | Immediate | Voucher Pool | `eligibility.mode="admin_only"` — hidden from normal users, safe for real claim testing. |

---

# Campaign Wizard

Implemented exactly as specified (Steps 1–6): Campaign Type → Audience →
Release → Reward → Preview → Launch. Release (Step 3) supports **only**
Immediate and Scheduled, as required — no Batch/Drip/Manual (P3).

Audience modes map to evaluators `vouchers.py` already enforces — no new
eligibility code was added:

| Audience mode | Compiles to | Existing evaluator reused |
|---|---|---|
| Smart Segment % | `eligibility.mode="public"` | claim-time probability weighting via `assign_public_pool_access_once` (unchanged) |
| Equal Chance | `eligibility.mode="public"` + warning | **Known limitation** — see below |
| No Segment Filter | `eligibility.mode="public"` | default |
| Whitelist | `eligibility.mode="user_id"`, `allow=[ids]` (usernames resolved via `users` collection) | existing `user_id` mode in `is_user_eligible_for_drop` |
| VIP | `eligibility.mode="tier"`, `allow=["VIP"]` | existing tier check |
| Region | `audience.regions=[...]` | existing region check |
| Admin Only | `eligibility.mode="admin_only"` | existing admin gate |

**Known, documented limitation (Equal Chance):** true bypass of
segment-derived claim probability would require a new optional parameter on
`assign_public_pool_access_once`. That function is claim-path logic the hard
rules say not to touch, so P2 ships Equal Chance as a reporting label only —
the compiler surfaces a warning in Preview so the admin is never misled about
enforcement. This mirrors the same conclusion the pre-existing design doc
(`docs/campaign_builder_design.md`) reached.

---

# Campaign Compiler Design

`campaign_builder.compile_campaign(campaign_doc) -> (result, http_status)`

1. Guard: only `status == "draft"` campaigns compile (prevents double-compile).
2. Resolve audience → `eligibility` + `audience` dict, reusing only
   already-enforced fields (`user_id` allow-list, `tier`, `admin_only`,
   `regions`).
3. Resolve reward → `pooled` (codes + pool) or `personalised` (assignments).
   Pure XP reward produces **no drop** (voucher drops are the only thing
   this compiler is allowed to generate) — the admin is told to use the
   existing Add/Reduce XP tool for the resolved audience.
4. Segment campaigns with N segments selected → N drop specs, one per
   segment, named `{slug}_001`, `{slug}_002`, … matching the "Weekend
   Surprise → drop_001, drop_002" example. All other templates → 1 drop.
5. Each drop spec is passed to `vouchers.create_drop_from_spec()` — the
   **exact same insert primitive** `admin_create_drop` has always used. This
   is the only write path into the voucher engine.
6. After insert, each drop is tagged with additive metadata only
   (`campaign_id`, `campaign_name`) via `db.drops.update_one($set)`.
   `campaign_type` is deliberately **not** overwritten here — it's already
   set correctly by `create_drop_from_spec` (including the "welcome_voucher"
   marker override for Welcome campaigns) and re-setting it would silently
   break the welcome-drop recognition `vouchers.py` reads elsewhere. This was
   caught by the test suite during implementation.
7. Campaign status moves to `active` with `compiled_drop_ids` populated
   (or stays `draft` with an error list if every drop insert failed).

---

# Database Changes

New collection: **`campaign_builder_campaigns`** (deliberately *not* named
`campaigns` — that name is already owned by the legacy segment-targeting
engine in `campaigns.py`/`campaign_engine.py`, which the hard rule "do not
remove existing campaign dashboards" requires stays intact. Sharing one
collection between two incompatible schemas would corrupt that dashboard's
listing/filtering, so P2 uses its own collection instead of the name
literally suggested).

```json
{
  "_id": "...",
  "campaign_name": "...",
  "campaign_type": "smart_default|public|welcome|segment|affiliate|personalised|fcfs|surprise|test",
  "status": "draft|active|archived",
  "audience_mode": "...",
  "audience_params": {},
  "release_style": "immediate|scheduled",
  "release_params": {},
  "reward_type": "voucher_pool|personalised_voucher|affiliate_reward_pool|xp|combined",
  "reward_params": {},
  "compiled_drop_ids": [],
  "created_at": "...",
  "updated_at": "...",
  "launched_at": "...",
  "feature_version": "P2"
}
```

Additive-only fields on `drops` (all optional, existing drops unaffected):
`campaign_id`, `campaign_name` (both set post-compile). `campaign_type` is
reused as-is (existing field, existing semantics preserved).

---

# Generated Voucher Drop Mapping

Example (Segment campaign "Weekend Surprise" targeting `normal_actual` +
`high_value`):

```
Campaign: Weekend Surprise
Compiler Output: WeekendSurprise_001, WeekendSurprise_002
  WeekendSurprise_001 -> eligibility.mode=user_id, allow=[ids in normal_actual]
  WeekendSurprise_002 -> eligibility.mode=user_id, allow=[ids in high_value]
```

Both drops behave identically to manually-created drops in Voucher Drop
Manager — same claim, eligibility, and scheduler code paths.

---

# Safety Checks

Step 5 Preview (`preview_campaign`) computes, read-only:

- `estimated_reach` — resolved audience size (exact for restrictive modes;
  snapshot-based total for unrestricted modes)
- `segment_distribution`
- `expected_voucher_count` (codes × drop count, or assignment count)
- `campaign_duration_hours`
- `estimated_voucher_usage_pct` (reward count vs. reach, pooled only)
- `generated_drop_count` / `expected_drop_names`
- `warnings` — surfaced whenever a choice has a known enforcement limitation
  (e.g. Equal Chance, unresolved whitelist usernames, empty codes)

Launch requires typing **LAUNCH** exactly (`POST .../compile` with
`{"confirm": "LAUNCH"}`, rejected otherwise with `confirmation_required`).

---

# Manual QA

Automated (`test_campaign_builder.py`, 10/10 passing against an in-memory
fake Mongo, exercising the real `vouchers.create_drop_from_spec`):

- Public/Smart Default campaign compiles 1 pooled drop with the requested codes
- Segment campaign compiles 1 drop per segment, each restricted to that
  segment's resolved user ids
- Whitelist campaign resolves usernames → user ids (and warns on unresolved ones)
- Personalised campaign compiles an `assignments`-based drop
- Test campaign is `admin_only`
- Welcome campaign preserves the `welcome_voucher` / `new_joiner` markers
- XP-only reward produces no drop (by design)
- Compiling twice on the same campaign is rejected (`not_draft`)
- Rollback (delete campaign doc) leaves already-generated drops untouched
- Preview reports correct reach / voucher-count math

Manual checklist (requires a running app + real MongoDB — not run in this
sandbox, which has no live Mongo):

- [ ] Campaign Builder view loads under Campaign Control
- [ ] Draft saves and reloads via Resume
- [ ] Preview renders reach/segment/voucher numbers
- [ ] Compile requires typing LAUNCH; rejects any other input
- [ ] Compiled drop appears in Voucher Drop Manager (`drops` view)
- [ ] Compiled drop appears in Compiled Voucher Drops view
- [ ] Compiled drop also visible/claimable in Legacy MiniApp (`/static/index.html`)
- [ ] Claim succeeds for an eligible user; duplicate claim is blocked (existing engine, unchanged)
- [ ] Affiliate settlement dashboards show no change for non-campaign drops
- [ ] Welcome voucher journey (new-joiner detection) still fires for a Welcome campaign drop
- [ ] Scheduler sweep still flips a Scheduled campaign drop from `upcoming` → `active` at its `startsAt`

---

# Rollback Plan

- Remove the 4 Campaign Builder nav items / view sections from
  `admin-dashboard.html`/`.js`, and unregister `campaign_builder_bp` in
  `main.py`, to remove the UI.
- Delete documents from `campaign_builder_campaigns` to remove campaign
  records.
- **No voucher rollback, no scheduler rollback, no claim-engine rollback** —
  generated drops are ordinary `drops`/`vouchers` documents and keep working
  exactly like manually-created drops after the Campaign Builder UI is gone.
- The one-line refactor in `vouchers.py` (`create_drop_from_spec`) is
  behavior-preserving for the existing `/admin/drops` POST endpoint; it does
  not need to be reverted for a Campaign Builder rollback, since it's exactly
  as safe to keep as the original.
