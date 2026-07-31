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
  `generated_by="creator_share_centre"`.
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
   trusted.
2. An `creator_members` record for that `user_id` with `status == "active"`.
3. If `CREATOR_GROUP_CHAT_ID` is configured, a live (short-cached) Telegram
   membership check against that group.

### `creator_members` schema

```json
{
  "user_id": 123456789,
  "status": "active",           // active | suspended | removed
  "source_group_id": -1001234567890,
  "creator_tier": "pilot",
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
| `creator_not_authorized` | No active `creator_members` record |
| `creator_suspended` | Record exists but `status == "suspended"` |
| `creator_membership_required` | Telegram confirmed the user left/was kicked from the creator group; record is marked `removed` |
| `creator_membership_unresolvable` | Telegram membership lookup temporarily unavailable (HTTP 503) |
| `creator_generation_rate_limited` | Hit the 20/hour generation cap (HTTP 429) |

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

### Share text format (V1, fixed)

```
{hook_text}
{playback_url}

More player replays and rewards inside AdvantPlay:
{canonical_referral_link}
```

- `hook_text` and `playback_url` are each independently omitted (no line,
  no orphan blank line, never the literal string `"None"`) when no active
  hook/playback exists.
- The CTA line is always exactly `More player replays and rewards inside
  AdvantPlay:`.
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
`static/admin-dashboard.html`): active creator count, search by Telegram
user ID or username, approve / suspend / activate / remove, and bulk import
(newline/comma-separated Telegram user IDs, deduplicated). All admin routes
go through the existing `vouchers.require_admin()` — same session-cookie
auth as the rest of the admin dashboard, unrelated to creator membership.

`CREATOR_GROUP_CHAT_ID` is shown to admins only as a configured/not-configured
status flag — never as an editable value in the browser.

Admin endpoints:

```
GET    /api/admin/referral/creators
POST   /api/admin/referral/creators
POST   /api/admin/referral/creators/bulk
POST   /api/admin/referral/creators/<user_id>/suspend
POST   /api/admin/referral/creators/<user_id>/activate
DELETE /api/admin/referral/creators/<user_id>   (soft: sets status="removed")
```

## Telegram entry point

- `/creator` bot command, and `/start creator` deep link — both handled by
  `main.send_creator_share_entry_point()`.
- Only `creator_members` records with `status == "active"` get the
  "🎬 Creator Share Centre" Web App button
  (`https://apreferralv1.fly.dev/creator-share`); everyone else gets a short
  denial message. The general `/start` welcome menu is unchanged.

## Deployment

No new Fly.io app, no new database, no fly.toml changes. Web (`gunicorn
main:app`) and worker processes are unchanged.

One new secret is required:

```
fly secrets set CREATOR_GROUP_CHAT_ID="<numeric-chat-id>" -a apreferralv1
fly deploy -a apreferralv1
```

If `CREATOR_GROUP_CHAT_ID` is left unset, live membership verification is
simply skipped — access is still gated entirely by `creator_members.status`.

### Post-deployment checks

- `GET /creator-share` returns the HTML shell.
- An unauthorized creator API call returns the expected denial code.
- An approved creator can generate a package containing the canonical
  invite link.
- Copy All works inside Telegram.
- The existing admin Share Content page and the general Mini App still work
  unchanged.
- Web and worker machines remain healthy; no duplicate scheduler/polling
  process starts.
