# Creator Share Centre

A creator-only Mini App surface that lets approved referrers generate a
copy-ready share post, copy it in one tap, generate a variation, and see
their current referral results. It is deliberately thin: it reuses the
existing Referral Centre → Share Content system end to end and adds no new
database, referral, XP, or invite-link system.

## What is reused (not rebuilt)

- **Hook/playback selection**: `referral_share_content.select_hook()` /
  `select_playback_for_user()` — same active-only pools, same
  no-immediate-repeat logic.
- **Canonical invite link**: `main.get_or_create_referral_invite_link_sync()`
  — the Creator Share Centre never creates a second/alternate invite link.
- **Generation + persistence**: `referral_share_content.generate_share_package()`
  writes to the existing `share_generations` collection, with
  `generated_by="creator_generated_share"` (analytics-metadata only; historical
  documents written before the Mini App / Creator Share Centre content split
  used `generated_by="creator_share_centre"` and are still matched by
  `creator_share_results()` for backward compatibility).
- **Referral qualification/results**: the existing `pending_referrals`
  collection and the same status buckets used by the admin Referrals panel
  (`dashboard_panels._QUALIFIED_STATUSES` / `_PENDING_STATUSES` /
  `_REVOKED_STATUSES`) — qualified vs. pending vs. revoked classification is
  never redefined here.

What's new is access control (`creator_members`), a creator-facing UI
(`static/creator-share.html`), a small set of `/api/creator/...` endpoints,
and a few additive fields on `share_generations` for copy/share telemetry.

## Access model

Every `/api/creator/...` request requires, in order:

1. Valid Telegram Mini App `initData` (verified server-side via
   `vouchers.verify_telegram_init_data`). `user_id` is always taken from the
   verified payload — a `user_id` in the JSON body or query string is never
   trusted. This is still mandatory, unconditionally.
2. **Confirmed membership in the configured Creator Access Chat grants
   access automatically.** `creator_members` is no longer a mandatory
   allowlist — it is an override/profile collection:
   - An existing record with `status == "suspended"` or `status == "removed"`
     always denies, *even if the user is still in the chat*.
   - An existing record with `status == "active"` is honoured once chat
     membership is confirmed.
   - No record at all: access is granted once chat membership is confirmed,
     and an `active` creator profile is **lazily created** (see below) —
     concurrent first requests never create duplicates.
   - A user who is **not** in the configured chat remains denied regardless
     of any `creator_members` record.
3. If membership verification is explicitly **disabled** by an admin
   (`membership_check_enabled == false`), access falls back to requiring an
   existing `creator_members.status == "active"` record — a user with no
   record stays denied (`creator_not_authorized`). This is intentional:
   disabling verification must never open access broadly to anyone who's
   never been recorded.

### Lazy creator profile creation

On a creator's first successful access via confirmed Creator Access Chat
membership, if no `creator_members` record exists yet, one is upserted with
`$setOnInsert` (so a race between concurrent first requests for the same
user can never create two documents — the unique index on `user_id` is the
actual guard):

```json
{
  "user_id": 123456789,
  "username": "somecreator",
  "status": "active",
  "creator_tier": "pilot",
  "source_group_id": -1001234567890,
  "approved_at": "ISODate",
  "approval_source": "creator_access_chat_membership",
  "created_at": "ISODate",
  "updated_at": "ISODate",
  "last_membership_verified_at": "ISODate",
  "last_membership_verified_config_version": 3
}
```

`last_membership_verified_config_version` is stamped at creation time (not
left unset) so the very next request's membership cache check — which is
keyed on `config_version`, see **Cache invalidation on group change** below
— hits immediately instead of every lazily-created profile paying for one
redundant Telegram call right after creation.

An existing `suspended`/`removed` record is never touched by this path —
lazy creation only ever runs when no record exists at all, so a previously
suspended/removed creator can never be silently reactivated by rejoining
the chat.

### `creator_members` schema

```json
{
  "user_id": 123456789,
  "status": "active",           // active | suspended | removed
  "source_group_id": -1001234567890,
  "creator_tier": "pilot",
  "approval_source": "creator_access_chat_membership",  // creator_access_chat_membership | manual | bulk_import
  "approved_at": "ISODate",
  "approved_by": 987654321,
  "last_membership_verified_at": "ISODate",
  "created_at": "ISODate",
  "updated_at": "ISODate"
}
```

Indexes: unique `user_id`, `status`, and `(source_group_id, status)`.

### Response codes

| Code | Meaning |
|---|---|
| `invalid_telegram_auth` | Missing/invalid Telegram initData |
| `creator_not_authorized` | No `creator_members` record, and either not a confirmed chat member or membership verification is disabled |
| `creator_suspended` | Record exists but `status == "suspended"` |
| `creator_membership_required` | Telegram confirmed the user left/was kicked from the creator group; an existing record is marked `removed` |
| `creator_membership_unresolvable` | Telegram membership lookup temporarily unavailable (HTTP 503) |
| `creator_group_not_configured` | `membership_check_enabled=true` but no valid group is configured (HTTP 503) — fails closed |
| `creator_generation_rate_limited` | Hit the 20/hour generation cap (HTTP 429) |

## Creator Access Chat settings (admin-configurable)

`CREATOR_GROUP_CHAT_ID` is no longer read directly from the environment by
creator access checks. It is now amendable from the Admin Dashboard
(Referral Centre → Share Content → Creator Access → **Creator Access
Chat**) and stored in MongoDB, so it can be changed without a Fly.io
secret update or redeploy.

Supported Telegram chat types: `group`, `supergroup`, or `channel`. The
setting's internal key (`creator_group_access`) and the `creator_group_*`
field/env-var names are unchanged for backward compatibility — only the
admin-facing labels reflect that a channel is now a valid choice, so admins
aren't misled into thinking a selected channel is invalid.

### Storage: `app_settings` / `creator_group_access`

One canonical document, in the same `app_settings` collection
`settings_service.py` already writes to (`_id: "creator_group_access"`):

```json
{
  "_id": "creator_group_access",
  "creator_group_chat_id": -1001234567890,
  "membership_check_enabled": true,
  "chat_title": "AdvantPlay Creators",
  "chat_type": "supergroup",
  "bot_membership_status": "administrator",
  "verified_at": "ISODate",
  "updated_at": "ISODate",
  "updated_by": 987654321,
  "config_version": 3
}
```

Config changes are also written to `app_settings_audit` (admin ID, old/new
chat ID, `config_version`, `force_save`, and — when unverified —
`verify_error`); this reuses the same collection `settings_service.py`'s
own audit log writes to.

### Canonical reader: `get_creator_group_access_settings()`

Creator access checks call this — never `os.getenv("CREATOR_GROUP_CHAT_ID")`
directly. Resolution order:

1. **Explicit MongoDB setting** — once a `creator_group_access` document has
   been saved (even one that clears the chat ID back to `null`), it is
   authoritative. The env var never "wins back" after that.
2. **`CREATOR_GROUP_CHAT_ID` environment fallback** — only used when no DB
   document has ever been saved.
3. **Unconfigured** — `membership_check_enabled=false`, `creator_group_chat_id=None`.

If `membership_check_enabled=true` but there is no valid configured group,
access checks fail closed with `creator_group_not_configured` (HTTP 503)
rather than silently skipping the check. If `membership_check_enabled=false`,
access is gated by `creator_members.status` alone.

The reader is cached in-process for 30s (`CREATOR_GROUP_SETTINGS_CACHE_TTL_SEC`)
so a save on one process (web or worker) becomes visible on the others
without a redeploy, bounded by that TTL; a save also calls
`invalidate_creator_group_settings_cache()` immediately on the process that
made it.

### Cache invalidation on group change

Every membership verdict cached on a `creator_members` record (both a
confirmed check and an "unresolvable" outage placeholder) is stamped with
the `config_version` that produced it. A save always increments
`config_version`, which immediately invalidates every previously cached
verdict for every creator — a change of group can never leave a stale
"confirmed member" verdict from the old group still granting access, and a
creator who was in the old group but isn't in the new one is re-checked
(and denied) on their very next request.

### Admin API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/admin/referral/creator-settings` | Read current resolved settings |
| POST | `/api/admin/referral/creator-settings/verify-group` | Verify a candidate chat ID against Telegram without saving |
| PUT | `/api/admin/referral/creator-settings` | Save (verifies by default; `force_save: true` to override) |

All three are gated by the existing `vouchers.require_admin()` — same
session-cookie auth as every other admin route, unrelated to creator
membership. The bot token is never returned to the browser; the chat ID
itself is shown unmasked (it's an identifier, not a secret).

### Validation

- The chat ID is parsed as an integer; zero and any positive value are
  rejected outright (`invalid_creator_group_chat_id`).
- A negative ID that doesn't start with `-100` is *not* rejected — it only
  produces a `chat_id_prefix_unusual` warning in the verify response —
  because the real validation is the Telegram call, not the prefix.
- Verification calls Telegram `getChat` (existence + chat type), then the
  bot's own `getChatMember` (via `getMe` to resolve the bot's own user ID).
  `group`, `supergroup`, and `channel` chat types are all accepted; any other
  type (e.g. a private chat) is rejected with `creator_group_wrong_chat_type`.
- Stable codes: `invalid_creator_group_chat_id`, `creator_group_not_found`,
  `creator_group_bot_access_denied`, `creator_group_wrong_chat_type`,
  `creator_group_verification_failed`.
- An unverified group is never saved by default — a verification failure
  returns the error and leaves the last valid, already-saved setting
  untouched. `force_save: true` (admin-only, since it's gated by the same
  `require_admin()` as every other field here) saves anyway, with
  `verified_at` left `null` and a clear `unverified: true` /
  `verify_error: "<code>"` entry in the audit log.

### Structured logs

```
[CREATOR_GROUP_SETTINGS][VERIFIED]      admin_id=... chat_id=...
[CREATOR_GROUP_SETTINGS][UPDATED]       admin_id=... old_chat_id=... new_chat_id=... config_version=... force_save=... unverified=...
[CREATOR_GROUP_SETTINGS][VERIFY_FAILED] admin_id=... old_chat_id=... new_chat_id=... config_version=... reason_code=...
```

Never the bot token, never a full Telegram API response body.

### Membership verification caching

- A **confirmed** membership check is cached for 15 minutes
  (`MEMBERSHIP_VERIFY_CACHE_SEC`) before Telegram is queried again.
- If Telegram is temporarily unavailable (network error, non-200, malformed
  response), access is **not** denied and the creator record is **not**
  removed. Instead, the previous state is kept for a short grace window
  (`MEMBERSHIP_UNRESOLVABLE_GRACE_SEC` = 120s), after which a fresh attempt
  is made on the next request.
- Only a **definitive** Telegram verdict (`status` in `left`/`kicked`) marks
  the creator `removed`.
- Both the confirmed and the unresolvable cache are additionally keyed to
  `config_version`, so a group change invalidates them immediately (see
  **Cache invalidation on group change** above) — a config change is never
  masked by an in-flight cache window.
- The outage cache is checked *before* calling Telegram, so repeated
  requests inside the 120s grace window never each pay the (up to 8s) HTTP
  timeout — only the first request after an outage begins, and the first
  one after the window expires, actually call Telegram.
- A chat member with **no** `creator_members` record yet has nothing to
  cache a verdict on: each request calls Telegram directly until the first
  confirmed membership creates the lazy profile, after which the normal
  cache above applies.

## Creator API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/creator/share/status` | Access check + creator tier |
| POST | `/api/creator/share/generate` | Generate a share package (`{"platform": "generic\|whatsapp\|facebook\|x\|telegram"}`) |
| POST | `/api/creator/share/<package_id>/copied` | Record a successful clipboard copy (interaction only) |
| POST | `/api/creator/share/<package_id>/share-clicked` | Record a share action was initiated (interaction only) |
| GET | `/api/creator/share/results` | Creator's own referral results |

`copied`/`share-clicked` are **interaction telemetry only** — they never
grant referral rewards, XP, or otherwise touch qualification state, and both
endpoints are ownership-checked (`package.user_id == authenticated user_id`);
a non-owner or a nonexistent `package_id` both return the same
`404 not_found`, so package existence is never leaked.

### Share text format (fixed)

```
{hook_text}
{playback_url}

Want more replays like this—and rewards too?
Join AdvantPlay for:
🎟️ Free welcome voucher
⚡️ Daily voucher drops
🏆 Weekly rewards

Start here 👇
{canonical_referral_link}
```

- `hook_text` and `playback_url` are each independently omitted (no line,
  no orphan blank line, never the literal string `"None"`) when no active
  hook/playback exists.
- The transition line, the three fixed benefits, and the referral link
  always render. This is intentionally the *compressed* three-benefit
  block (voucher / daily drops / weekly rewards) — never the Mini App's
  full five-benefit block (`bonus campaigns` / `VIP-only announcements` are
  Mini-App-only and never appear here).
- The canonical referral link is always present; if it cannot be produced,
  generation fails outright (HTTP 502) and no `share_generations` document
  is written for that attempt.

Built by `referral_share_content.build_creator_share_text()`.

## `share_generations` additive fields

Every new document (bot or creator) now also carries:

```json
{
  "platform": "whatsapp",
  "package_id": "unguessable-url-safe-token",
  "copied_at": null,
  "copy_count": 0,
  "share_clicked_at": null,
  "share_click_count": 0
}
```

`package_id` has a **partial** unique index
(`partialFilterExpression: {"package_id": {"$exists": true}}`), so legacy
documents written before this change (which have no `package_id` field at
all) remain untouched and valid.

## Rate limiting

Generation is capped at 20 packages/creator/hour via the existing
`referral_rate_limit.consume_referral_rate_limits()` helper, using a
dedicated `creator_generation_rate_limits` collection (not shared with the
general referral rate limiter). Exceeding it returns HTTP 429 with
`creator_generation_rate_limited`.

## Admin controls

Under Referral Centre → Share Content → **Creator Access** (in
`static/admin-dashboard.html`): "Members of the configured Creator Access
Chat receive access automatically. This table is used to view profiles and
suspend or remove access." — active creator count (this includes creators
who were lazily created by opening the Creator Centre, not just manually
approved ones), search by Telegram user ID or username, an **Approval
Source** column (`Chat membership` / `Manual` / `Bulk import`), and
suspend / activate / remove controls. Approve Creator and Bulk Import
remain available as **optional manual overrides** — for use when membership
verification is disabled, or for testing — not as the primary way creators
get access. All admin routes go through the existing
`vouchers.require_admin()` — same session-cookie auth as the rest of the
admin dashboard, unrelated to creator membership.

**Important limitation**: this table does **not**, and cannot, list every
member of the configured Creator Access Chat. The Telegram Bot API has no
reliable endpoint to enumerate all members of a group, supergroup, or
channel, so there is no bulk sync/import of chat members — rows appear only
for users who have actually opened `/creator` or the Creator Share Centre
(lazily created), or who were added manually.

The Creator Access Chat ID itself is edited directly in the browser (it's
an identifier, not a secret — see **Creator Access Chat settings** above);
only the bot token stays environment-only and is never returned to the
browser. Fly.io secrets/env vars are never written from the browser —
`CREATOR_GROUP_CHAT_ID` remains a fallback only, used before any explicit
database setting has ever been saved.

Admin endpoints:

```
GET    /api/admin/referral/creators
POST   /api/admin/referral/creators
POST   /api/admin/referral/creators/bulk
POST   /api/admin/referral/creators/<user_id>/suspend
POST   /api/admin/referral/creators/<user_id>/activate
DELETE /api/admin/referral/creators/<user_id>   (soft: sets status="removed")

GET    /api/admin/referral/creator-settings
POST   /api/admin/referral/creator-settings/verify-group
PUT    /api/admin/referral/creator-settings
```

## Telegram entry point

- `/creator` bot command, and `/start creator` deep link — both handled by
  `main.send_creator_share_entry_point()`, which calls the same
  `creator_share_centre._verify_creator_access()` used by the Mini App API
  (minus the initData check, since the bot command already knows the
  authenticated Telegram user). Confirmed Creator Access Chat membership
  gets the "💰 Turn Shares Into Cash" Web App button
  (`https://apreferralv1.fly.dev/creator-share`), lazily creating a profile
  on first success exactly as the Mini App path does; everyone else gets a
  short denial message. The general `/start` welcome menu is unchanged.
- `_verify_creator_access()` is dispatched via `asyncio.to_thread()` from
  this handler, since it can perform a synchronous Telegram `getChatMember`
  HTTP call (up to an 8s timeout) whenever the membership cache is cold —
  running it inline would stall the bot's shared polling event loop, and
  therefore every other update, for up to that long.

## Deployment

No new Fly.io app, no new database, no fly.toml changes. Web (`gunicorn
main:app`) and worker processes are unchanged.

`CREATOR_GROUP_CHAT_ID` as a Fly.io secret is now **optional** — it only
matters as a fallback before any chat has ever been configured from the
Admin Dashboard. It is not required for this deployment; the chat is set
through Admin Dashboard → Referral Centre → Share Content → Creator Access
→ Creator Access Chat instead:

```
# Optional fallback only — the Admin Dashboard "Creator Access Chat" panel
# is the primary, no-redeploy way to configure/change this.
fly secrets set CREATOR_GROUP_CHAT_ID="<numeric-chat-id>" -a apreferralv1
fly deploy -a apreferralv1
```

If neither the DB setting nor `CREATOR_GROUP_CHAT_ID` is configured,
`membership_check_enabled` resolves to `false` and access is gated entirely
by `creator_members.status`.

### Post-deployment checks

- `GET /creator-share` returns the HTML shell.
- An unauthorized creator API call returns the expected denial code.
- An approved creator can generate a package containing the canonical
  invite link.
- Copy Post works inside Telegram.
- The existing admin Share Content page and the general Mini App still work
  unchanged.
- Web and worker machines remain healthy; no duplicate scheduler/polling
  process starts.
