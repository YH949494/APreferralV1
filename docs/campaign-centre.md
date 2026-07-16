# Campaign Centre

The Campaign Centre is a **generic marketing-campaign gateway** for the Mini
App — not a tournament-only feature. It owns campaign configuration,
provider configuration, identity/subscription gating, and (via
`reward_engine.py` + `tournament_rewards.py`) rule-based reward allocation
into the existing Voucher Centre inventory, delivered through the generic
Campaign Rewards section in the Mini App.

Ships today with three campaign types (`tournament`,
`external_subscription_verification`, `external_website`). Nothing in the
architecture — collection name, module names, route names, the reward
engine — is tournament-specific, so future types (lucky draw, referral
contest, cashback, mission, survey, seasonal event, partner campaign, VIP
event, ...) are added by extending `CAMPAIGN_TYPES` and, if a new reward
trigger is needed, `reward_engine.CONDITION_TYPES` — not by redesigning
this module. See `docs/campaign-rewards.md` for the reward side of this.

It does **not** own gameplay, player accounts, tournament registration,
scores, or leaderboard generation (the tournament website owns those), and
it does **not** own voucher inventory/eligibility/issuance for the external
subscription-voucher campaign type (that website owns those — AP only
verifies Telegram identity + channel subscription for it).

## Namespacing note

This repository already has an unrelated `campaigns` collection and
`/api/admin/campaigns` blueprint (`campaigns.py` / `campaign_engine.py`) — a
segment-audience marketing/voucher-targeting tool. To avoid colliding with
that production feature, Campaign Centre's campaign admin routes live at
`/api/admin/gc-campaigns` (short and flat like the rest of the admin API —
the only reason it isn't `/api/admin/campaigns` is that path is taken).
Provider and reward admin routes use the flat `/api/admin/providers` and
`/api/admin/rewards` paths since those don't collide with anything
pre-existing. Public and integration routes use the exact paths from the
product spec.

Collections: `gc_campaigns`, `gc_providers`.

## Campaign types

- `tournament` — reward rules reference AP-owned voucher pools (shared with
  the Voucher Centre), results submitted by the tournament provider,
  admin-approved allocation, delivered via Campaign Rewards.
- `external_subscription_verification` — AP verifies Telegram identity +
  channel subscription only; the external website issues its own voucher.
- `external_website` — generic outbound campaign card with a destination URL.

There is no public `coming_soon` type. A campaign that isn't ready yet stays
`draft`, or `scheduled` with `destination.ready = false` — both are
admin-only.

## Campaign status machine

`draft → scheduled → live → paused/ended → archived` (see
`campaign_centre._VALID_STATUS_TRANSITIONS` for the exact adjacency matrix
enforced server-side on every `PUT`/publish/pause/archive call).

## Public visibility (single source of truth)

`campaign_centre.is_publicly_active(campaign, provider, now)` is the **only**
function that decides whether a campaign is shown to end users. A campaign is
publicly active only when **all** of:

1. `status == "live"`
2. `now >= schedule.starts_at`
3. `schedule.ends_at` is absent, or `now < schedule.ends_at`
4. `destination.ready == true`
5. the linked provider exists and `provider.active == true`

`GET /api/campaigns/active` calls this function server-side for every
candidate and only serializes public-safe fields (`campaign_id`, `name`,
`type`, `description`, `button_text`, `banner_url`, `priority`,
`telegram.require_identity`, `telegram.require_subscription`,
`telegram.channel_username`, `open_mode`). No provider internals, no reward
config, no admin metadata. If nothing is active, the response is
`{"status":"ok","campaigns":[]}` and the Mini App widget hides the whole
section — no "Coming Soon" cards.

## Admin preview

`GET /api/admin/gc-campaigns/{id}/preview` (admin-only) returns the
rendered card plus `admin_badges` (`draft`, `scheduled`,
`destination_not_ready`, `provider_inactive`) and an `effective_visibility`
explanation (`campaign_centre.visibility_explanation`). Preview never writes
anything and never changes what the public endpoint returns.

## Admin API surface

```
GET    /api/admin/gc-campaigns
POST   /api/admin/gc-campaigns
GET    /api/admin/gc-campaigns/{campaign_id}
PUT    /api/admin/gc-campaigns/{campaign_id}
POST   /api/admin/gc-campaigns/{campaign_id}/publish
POST   /api/admin/gc-campaigns/{campaign_id}/pause
POST   /api/admin/gc-campaigns/{campaign_id}/archive
POST   /api/admin/gc-campaigns/{campaign_id}/duplicate
GET    /api/admin/gc-campaigns/{campaign_id}/preview

GET    /api/admin/providers
POST   /api/admin/providers
GET    /api/admin/providers/{provider_id}
PUT    /api/admin/providers/{provider_id}
POST   /api/admin/providers/{provider_id}/activate
POST   /api/admin/providers/{provider_id}/deactivate
GET    /api/admin/providers/{provider_id}/preview
```

All of these require the same admin auth already used across the codebase
(`vouchers.require_admin()` — Telegram MiniApp initData admin allowlist,
`X-Admin-Secret`/bearer secret, or the browser admin session from
`admin_auth.py`). Every mutation writes a row to `campaign_admin_audit_log`.

## Public / user-facing API surface

```
GET  /api/campaigns/active
POST /api/campaigns/{campaign_id}/play
POST /api/campaigns/{campaign_id}/subscribe-check
```

`play` and `subscribe-check` derive the Telegram user id from the
**authenticated Mini App session** — `miniapp_identity.
resolve_authenticated_telegram_user_id()`, the same shared resolver every
identity-sensitive endpoint in this feature uses — never from a raw
`uid`/`user_id` parameter. See `docs/campaign-rewards.md` for why this is
phrased as "authenticated session" rather than "initData": it's the same
underlying verification, just centralized in one module.

## Admin Dashboard UI

Campaign Centre is a module inside the existing Admin Dashboard
(`static/admin-dashboard.html` / `admin-dashboard.js`), listed in the
sidebar as **Player Campaigns** (not "Campaign Centre" — that label was
already taken by the pre-existing segment-audience module) with the six
required tabs: Campaigns, Providers, Tournament Results, Rewards,
Verification, Activity Log. It reuses the dashboard's existing nav/sidebar,
admin session auth, layout primitives (`.data-table`, `.pill`, `.btn`,
`emptyState()`, `statePanel()`), and view-switching machinery — there is no
separate standalone admin page for this feature.
