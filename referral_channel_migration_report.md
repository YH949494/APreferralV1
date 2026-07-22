# AP Referral Bot — Channel Migration: Implementation Report

> **Note on source-of-truth.** The task specified `referral_channel_migration_audit.md`
> as the audit report to treat as source of truth. That file does not exist in this
> repository (`git log --all` shows no commit ever added it). This report therefore
> also serves as the audit record: root causes below were derived directly from
> reading `main.py`, `scheduler.py`, `vouchers.py`, `funnel_dashboard.py`, and their
> test suites, not from a pre-existing document.

## 1. Root causes fixed

| ID | Blocker | Fix |
|----|---------|-----|
| P0-1 | Referral destination (group vs. channel) was resolved independently and inconsistently in `main.py` (hardcoded `GROUP_ID`, no env override) and `scheduler.py` (env-driven with a different fallback chain) — no single source of truth, no supported way to point new links at the channel. | New `referral_destination.py` module: `get_referral_destination()` resolves `(chat_id, destination_type)` from `REFERRAL_DESTINATION_MODE` / `REFERRAL_DESTINATION_CHAT_ID`, defaulting to `community_group`. `main.py` and `scheduler.py` both import `COMMUNITY_GROUP_ID` / `OFFICIAL_CHANNEL_ID` from this one module. |
| P0-2 | Channel `ChatMemberUpdated` events never ran referral attribution — `_confirm_referral_on_main_join` was only called for `chat_id == GROUP_ID`, and `member_update_handler`'s channel branch only updated subscription bookkeeping. | Attribution is now destination-neutral: `_confirm_referral_on_main_join` (aliased as `_confirm_referral_join`) derives `destination_type` from the event's own `chat_id` and is called for both the group and channel branches in `member_update_handler`. `handle_user_join()` is now explicitly gated to `chat_id == GROUP_ID` at the call site (previously it relied only on its own internal early-return) so it structurally can never run for a channel event. |
| P0-3 | Settlement's join-time check (`joined_main_at or created_at` vs. pending's `created_at_utc`, with an `already_in_db` staleness revoke) assumed a group-style timeline. Applied to a channel join, an existing bot user or existing chatroom user would be revoked as "already in DB" even though they were a genuinely new channel subscriber, and `joined_main_at` (group-only field) was being treated as load-bearing. | `settle_pending_referrals` now branches on `destination_type` (derived from the pending row, falling back to chat-id comparison for legacy rows). For `official_channel`, the reference timestamp is `referral_join_seen_at_utc` (falling back to `created_at_utc`) — `joined_main_at` is never consulted and the `already_in_db` staleness comparison against the user's account-creation time is skipped entirely, since attribution already required a genuine `became_member` transition. Group-origin rows are untouched (unchanged `joined_main_at`/`already_in_db` logic). |
| P0-4 | `award_key = f"ref:{group_id}:{invitee_user_id}"` was destination-scoped, so the same invitee referred once via a group link and once via a channel link would generate two distinct award keys — `grant_xp`'s idempotency (keyed on `unique_key`) would not catch the second, producing double XP and double affiliate-qualification credit. | Two layers: (1) `award_key` is now `f"ref:{invitee_user_id}"` (invitee-scoped only) in `settle_pending_referrals`, so the existing unique index on `referral_award_events.award_key` and `grant_xp`'s `xp_events` idempotency check now naturally reject a second award for the same invitee regardless of destination. (2) A new `referral_invitee_lock` atomic claim (unique index on `invitee_user_id`) blocks a *second pending referral from even being created* for an invitee who already has an active/awarded referral in the other destination — defense in depth, not just a settlement-time catch. |

## 2. Files and functions changed

**New files**
- `referral_destination.py` — `get_referral_destination()`, `destination_type_for_chat_id()`, `COMMUNITY_GROUP_ID`, `OFFICIAL_CHANNEL_ID`. Canonical config resolver (Phase 1).
- `referral_invitee_lock.py` — `claim()`, `release()`, `ensure_indexes()`. Atomic cross-destination duplicate-referral guard (Phase 5).
- `referral_migration_audit.py` — `build_report()`, `main()`. Read-only dry-run audit CLI; `--commit` only persists the report snapshot, never mutates referral data (Phase 5 duplicate-data audit + Phase 12 verification tooling).
- `test_referral_channel_migration.py` — 35 new tests (Phase 11).
- `referral_channel_migration_report.md` — this report.

**Modified files**
- `main.py`
  - Import block: `GROUP_ID`/`OFFICIAL_CHANNEL_ID` now sourced from `referral_destination` instead of independently parsed; `CHANNEL_ID` kept as a backward-compatible alias.
  - `get_or_create_referral_invite_link_sync()` — targets `get_referral_destination()`'s resolved chat id instead of the hardcoded group; reuse query scoped to `(chat_id, inviter_id, is_active)` so a destination-mode switch never reuses a stale-destination link; new rows get `destination_type` + `schema_version: 2`; `creates_join_request: False` made explicit; structured `[REFERRAL][LINK_CREATED]` / `[REFERRAL][LINK_REUSED]` / `[REFERRAL][LINK_CREATE_FAILED]` logs added.
  - `_confirm_referral_on_main_join()` (kept under its original name — `test_main_referral_error.py` extracts it by name via AST — with a new `_confirm_referral_join()` alias for the preferred destination-neutral name) — derives `destination_type` from the event chat id; adds the `referral_invitee_lock.claim()` cross-destination guard before pending creation (fail-open on lock-collection errors); new pending rows carry `destination_chat_id`, `destination_type`, `referral_join_seen_at_utc`, `schema_version: 2` in addition to the legacy `group_id`; adds `[REFERRAL][JOIN_UPDATE]`/`[LINK_RESOLVED]`/`[LINK_UNKNOWN]`/`[NO_INVITE_LINK]`/`[PENDING_CREATED]`/`[PENDING_DUPLICATE]` logs.
  - `member_update_handler()` — `became_member` no longer misclassifies a `restricted → member` transition as a new join (added `present_statuses` including `"restricted"`); the group and channel `became_member` branches are now `if/elif` and both call `_confirm_referral_join`; the trailing `handle_user_join()` call is now explicitly gated to `chat_id == GROUP_ID` so it structurally cannot run for the channel.
  - `ensure_indexes()` — isolated `referral_invitee_lock.ensure_indexes(db)` call (own try/except, cannot block indexes created after it).
- `scheduler.py`
  - Import block: `GROUP_ID`/`OFFICIAL_CHANNEL_ID`/`COMMUNITY_GROUP`/`OFFICIAL_CHANNEL` now sourced from `referral_destination`.
  - `settle_pending_referrals()` — derives `destination_chat_id`/`destination_type` per pending row (own metadata → `group_id` → `GROUP_ID` fallback, exactly as specified); join-time validation branches by `destination_type` (channel: `referral_join_seen_at_utc`/`created_at_utc`, no `joined_main_at` requirement, no `already_in_db` staleness check; group: unchanged legacy behavior); `award_key` changed to invitee-scoped `ref:{invitee_user_id}`; `award_doc` gained `destination_chat_id`/`destination_type`; every revoke and award path now calls `referral_invitee_lock.release()` to un-block the invitee for a future genuinely-new referral.
- `vouchers.py` — `api_referral_progress()`'s `invite_link_map_collection.find_one(...)` (used only for the link-expiry countdown) is now scoped to the currently-configured destination chat id + `is_active: True`, so a stale/inactive link from a previous destination mode can no longer drive the countdown.
- `funnel_dashboard.py` / `static/funnel-dashboard.js` — Phase 9: the `"join_channel"` stage's **label** renamed from the misleading "Join Channel" to "Join Community Chat" (the underlying metric, `joined_main_at`, is and remains group-only — the stage `id` was left unchanged for API/consumer compatibility).

## 3. Schema and index changes

**Additive fields**
- `invite_link_map`: `destination_type`, `schema_version` (new rows only).
- `pending_referrals`: `destination_chat_id`, `destination_type`, `referral_join_seen_at_utc`, `schema_version` (new rows only). `group_id` is still written on every new row for legacy-reader compatibility.
- `referral_award_events`: `destination_chat_id`, `destination_type` (new rows only; `group_id` also retained, now holding the resolved destination chat id rather than always the community group).
- New collection `referral_invitee_locks`: `{invitee_user_id, inviter_user_id, chat_id, destination_type, status, created_at_utc, updated_at_utc}`.
- New collection `referral_migration_audit_reports` (only ever written by `referral_migration_audit.py --commit`, and only as an append-only report snapshot).

**New indexes**
- `referral_invitee_locks.uniq_referral_invitee_lock` — unique on `invitee_user_id`, created in isolation (`referral_invitee_lock.ensure_indexes`, own try/except) via `main.py:ensure_indexes()`.

**Indexes deliberately *not* added**
- No new unique index on `pending_referrals.invitee_user_id` was added. The Phase 5 requirement to "isolate the stricter index until data is proven clean" is satisfied by *not* attempting it in this patch — the atomic `referral_invitee_locks` claim document is the enforcement mechanism instead (Option 2 from the spec, not Option 1). Run `python referral_migration_audit.py` in production first; if `cross_destination_duplicate_invitees` comes back empty, a future patch can safely add the stricter index as a second, independent enforcement layer.

**Legacy fields retained**
- `invite_link_map.chat_id` (no rename — now holds the resolved destination chat id rather than always the group id).
- `pending_referrals.group_id` (still written on every new row; existing unique index `(group_id, invitee_user_id)` untouched).
- `referral_award_events.group_id` / `.award_key` field names unchanged (only the award_key's *value format* changed, going forward).
- `users.joined_main_at` — never written by a channel event; the community-group `handle_user_join()` remains its only writer.

**No historical rows were migrated, bulk-updated, or deleted.**

## 4. Backward compatibility

- **Historical group links still resolve**: `get_or_create_referral_invite_link_sync`'s reuse query is `{chat_id: <resolved>, inviter_id, is_active: True}` — with `REFERRAL_DESTINATION_MODE` unset/`community_group` (the default), `<resolved>` is `COMMUNITY_GROUP_ID`, identical to the pre-migration hardcoded value, so existing group `invite_link_map` rows are found exactly as before. Verified by `test_existing_group_link_continues_resolving_and_is_reused`.
- **Historical pending rows still settle**: rows without `destination_type`/`destination_chat_id` fall back to `pending.get("group_id") or GROUP_ID`, classified as `community_group` unless that chat id equals `OFFICIAL_CHANNEL_ID` — the pre-existing group settlement code path (`joined_main_at`/`already_in_db` check) runs unchanged. Verified by `test_group_origin_legacy_referral_settlement_still_works` and `test_group_origin_legacy_referral_still_uses_joined_main_at_staleness_check`.
- **Channel links remain processable after rollback**: rolling back only changes *new* link generation; existing `invite_link_map` channel rows and in-flight `pending_referrals` channel rows are untouched and keep resolving/settling off their own stored `chat_id`/`destination_type`, independent of the live `REFERRAL_DESTINATION_MODE`. Verified by `test_channel_link_continues_resolving_after_rollback_to_group` and `test_in_flight_channel_pending_settles_after_rollback_to_group_mode`.
- **Group onboarding remains group-only**: `handle_user_join()` itself was not modified, and its one call site is now explicitly gated to `chat_id == GROUP_ID`. Verified by `test_channel_branch_never_calls_handle_user_join` (source-level) plus the existing (unmodified) group-onboarding test suite (`test_onboarding.py`, `test_welcome_*`) showing no new failures.
- **Affiliate and voucher logic remain downstream-compatible**: `grant_xp`, `mark_invitee_qualified`, `maybe_unlock_affiliate_group`, `maybe_handle_first_referral`, `referral_award_events`/`referral_events`/`referral_flow_events` writes are called with the same shapes as before — only the `award_key` *value* changed. None of `affiliate_rewards.py`, `affiliate_leaderboard.py`, `affiliate_group_access.py`, or `vouchers.py`'s claim/voucher logic were modified. Full `test_affiliate_*`, `test_vouchers.py`, and `test_channel_reactivation.py` suites pass with zero new failures.

## 5. Test results

Full suite (`python -m pytest -q`, excluding `test_ugc_growth_referral.py` which fails at **collection** both before and after this change — pre-existing `ImportError: cannot import name '_eligible_referrer_tiers' from 'scheduler'`, unrelated to this migration). Run 3x consecutively to confirm stability; identical counts every time:

| | Before (baseline) | After (this change) |
|---|---|---|
| Tests run | 1321 passed + 62 failed + 20 subtests passed | 1356 passed + 62 failed + 20 subtests passed |
| New tests added | — | 35 (all passing) |
| Baseline failures | 62 (listed below) | same 62, unchanged |
| New regressions | — | **0** |
| Collection errors | `test_uim_comparison.py::test` (1, pre-existing, unrelated) | same, unchanged |

**Baseline failures (all pre-existing, confirmed present before any of this migration's changes and unaffected by it — see per-file causes)**: `test_admin_metrics_daily.py` (3), `test_affiliate_admin_auth.py` (3), `test_affiliate_daily_kpis.py` (3), `test_affiliate_monthly_highest_tier.py` (2), `test_frontend_bonus_voucher_fetch.py` (3), `test_onboarding.py` (1), `test_referral_status_api.py` (10 — all `NameError: name 'ContextTypes' is not defined`, an environment/import-order issue unrelated to referral logic), `test_scheduler_pending_channel.py` (6 — a pre-existing, never-wired-up two-stage `pending_channel` retry design whose tests don't match the current single-stage `settle_pending_referrals` implementation; see inline comment in that test file's `pending_channel` references), `test_share_rank_caption.py` (2), `test_vouchers.py` (21), `test_welcome_pending_visibility.py` (1), `test_welcome_pm_flow.py` (5).

I did not attempt to fix these — they predate this migration, are outside its scope ("do not redesign unrelated systems"), and fixing them (especially the `ContextTypes` NameError, which looks like a Python-version/import-order issue affecting many unrelated test files) risks exactly the kind of broad, non-additive change the task explicitly rules out.

**New test coverage → Phase 11 checklist mapping**: items 1–14, 15–19, 20–21 (source-level), 22–24, 26–27 (source-level), 28–30 (via `get_or_create_referral_invite_link_sync`), 31–32 are covered in `test_referral_channel_migration.py`. Item 25 (affiliate leaderboard inclusion) is not independently re-tested — `maybe_unlock_affiliate_group`/`emit_referral_flow_event` call sites and their existing test coverage (`test_affiliate_leaderboard_counting.py`, `test_scheduler_affiliate_simulation.py`) are unmodified and still pass.

## 6. Deployment steps

1. Deploy this code with `REFERRAL_DESTINATION_MODE` unset (or explicitly `community_group`) — behavior is identical to pre-migration production.
2. Run `python referral_migration_audit.py` (no `--commit`) against production data. Confirm `cross_destination_duplicate_invitees` is empty or small/explainable, and note `award_events.legacy_destination_scoped_award_key_count` for awareness (old-format keys coexist fine; no migration needed).
3. Verify one existing group referral end-to-end (generate link → have a test account join → confirm settlement) to confirm no regression from the `_confirm_referral_on_main_join` refactor.
4. Confirm the bot is an administrator of the official channel with "Invite users via link" permission (required for `createChatInviteLink` against `OFFICIAL_CHANNEL_ID` to succeed).
5. Set `REFERRAL_DESTINATION_MODE=official_channel`.
6. Generate one test channel referral link (Mini App / `/start` / share-content — all three call the same `get_or_create_referral_invite_link_sync`).
7. Join with a clean Telegram test account via that link.
8. Confirm exactly one `pending_referrals` row was created with `destination_type: "official_channel"` and `referral_join_seen_at_utc` set — **this is the first point at which a real `ChatMemberUpdated` payload for the channel, carrying the exact invite link, should be observed; until then, channel attribution is code-reviewed and unit-tested but not live-verified.**
9. Monitor the first settlement cycle (`settle_pending_referrals`) for that pending row: official-channel membership check → engagement qualification → award with `award_key = "ref:<invitee_id>"`.

## 7. Rollback steps

Set `REFERRAL_DESTINATION_MODE=community_group` (or unset it).

- **New links**: `get_or_create_referral_invite_link_sync` immediately resumes generating community-group links (no code change, no restart-order dependency beyond the env var taking effect).
- **Existing channel links**: untouched in `invite_link_map`; remain resolvable by `_confirm_referral_join` for any channel `ChatMemberUpdated` events that still arrive (e.g. a user joining via a previously-shared channel link after rollback).
- **In-flight channel referrals**: `pending_referrals` rows created while in `official_channel` mode carry their own `destination_type`/`destination_chat_id`, so `settle_pending_referrals` continues to settle them exactly as before rollback — settlement never re-reads the live `REFERRAL_DESTINATION_MODE`.
- **Historical group links**: entirely unaffected at every step.

## 8. Remaining risks

**Code-confirmed resolved**
- Configuration single-sourcing (Phase 1).
- Link generation destination-scoping and reuse isolation (Phase 2).
- Attribution logic is destination-neutral and preserves self-referral/rate-limit/exact-lookup guarantees (Phase 3).
- Cross-destination duplicate-XP protection at both creation-time (lock) and settlement-time (invitee-scoped award key) (Phase 5).
- Settlement join-time/membership logic no longer penalizes existing users for genuinely-new channel subscriptions (Phase 6).
- `handle_user_join()` structurally cannot run for channel events (Phase 3/7).
- `restricted → member` is no longer misclassified as a new join (Phase 7).
- Dashboard label fixed without a redesign (Phase 9).

**Requires live Telegram verification** (cannot be proven by unit tests against mocked Telegram responses)
- That a real official-channel `ChatMemberUpdated` webhook payload actually contains `invite_link` in the shape this code expects (`getattr(member, "invite_link", None)`), for a link created via `createChatInviteLink` against a channel rather than a group. Telegram's behavior for channels vs. groups has not been observed live in this environment.
- That the bot's admin permissions in the official channel are sufficient for `createChatInviteLink` / `getChatMember` (this migration assumes parity with the existing group permissions but does not verify channel-specific permission scoping).
- End-to-end timing of the first live settlement cycle for a channel-origin referral (engagement-qualification signals were only exercised via the existing, already-passing `evaluate_referral_engagement` test suite, not a live channel subscriber).

**Requires production data verification**
- Run `referral_migration_audit.py` against the real `pending_referrals`/`referral_award_events`/`qualified_events` collections before enabling `official_channel` mode, per deployment step 2. This patch does not know the true state of production data.

**Optional cleanup** (not required for this migration, safe to defer)
- If `referral_migration_audit.py` reports zero cross-destination duplicates in production, a follow-up patch could add a unique partial index on `pending_referrals.invitee_user_id` (status in the blocking set) as a second enforcement layer alongside `referral_invitee_locks`.
- The pre-existing, never-wired-up `pending_channel` two-stage retry design (six baseline test failures in `test_scheduler_pending_channel.py`) is out of scope for this migration but is worth a separate cleanup pass — either wire it up or remove the dead status/tests.
- The `test_ugc_growth_referral.py` and `ContextTypes`-NameError test failures are pre-existing environment/import issues outside this migration's scope.
