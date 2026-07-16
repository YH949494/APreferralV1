# Campaign Centre

The Campaign Centre is AP's Telegram campaign gateway. It owns campaign
configuration, provider configuration, identity/subscription gating,
tournament result intake, AP-owned voucher pools for tournament rewards, and
Campaign Rewards delivery inside the Mini App.

It does **not** own gameplay, player accounts, tournament registration,
scores, or leaderboard generation (the tournament website owns those), and it
does **not** own voucher inventory/eligibility/issuance for the external
subscription-voucher campaign type (that website owns those — AP only
verifies Telegram identity + channel subscription for it).

## Namespacing note

This repository already has an unrelated `campaigns` collection and
`/api/admin/campaigns` blueprint (`campaigns.py` / `campaign_engine.py`) — a
segment-audience marketing/voucher-targeting tool. To avoid colliding with
that production feature, all Campaign Centre admin routes live under
`/api/admin/campaign-centre/...` and a new `gc_campaigns` / `gc_providers`
collection pair. Public and integration routes use the exact paths from the
product spec since those don't collide with anything pre-existing.

## Campaign types

- `tournament` — AP-owned voucher pools, results submitted by the tournament
  provider, admin-approved allocation, delivered via Campaign Rewards.
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

`GET /api/admin/campaign-centre/campaigns/{id}/preview` (admin-only) returns
the rendered card plus `admin_badges` (`draft`, `scheduled`,
`destination_not_ready`, `provider_inactive`) and an `effective_visibility`
explanation (`campaign_centre.visibility_explanation`). Preview never writes
anything and never changes what the public endpoint returns.

## Admin API surface

```
GET    /api/admin/campaign-centre/campaigns
POST   /api/admin/campaign-centre/campaigns
GET    /api/admin/campaign-centre/campaigns/{campaign_id}
PUT    /api/admin/campaign-centre/campaigns/{campaign_id}
POST   /api/admin/campaign-centre/campaigns/{campaign_id}/publish
POST   /api/admin/campaign-centre/campaigns/{campaign_id}/pause
POST   /api/admin/campaign-centre/campaigns/{campaign_id}/archive
POST   /api/admin/campaign-centre/campaigns/{campaign_id}/duplicate
GET    /api/admin/campaign-centre/campaigns/{campaign_id}/preview

GET    /api/admin/campaign-centre/providers
POST   /api/admin/campaign-centre/providers
GET    /api/admin/campaign-centre/providers/{provider_id}
PUT    /api/admin/campaign-centre/providers/{provider_id}
POST   /api/admin/campaign-centre/providers/{provider_id}/activate
POST   /api/admin/campaign-centre/providers/{provider_id}/deactivate
GET    /api/admin/campaign-centre/providers/{provider_id}/preview
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

`play` and `subscribe-check` derive the Telegram user id from verified Mini
App initData (`vouchers.verify_telegram_init_data`) passed as `init_data` in
the query string or JSON body — never from a raw `uid`/`user_id` parameter.

## Admin Dashboard UI

`static/campaign-centre.html` (linked from the main Admin Dashboard sidebar
as "Campaign Centre") implements the six required tabs: Campaigns,
Providers, Tournament Results, Reward Allocations, Verification
Integrations, Activity Logs. It's a small self-contained page (not merged
into the 5k-line `admin-dashboard.js`) to avoid destabilizing the existing
dashboard; it authenticates via the same same-origin admin session cookie.
