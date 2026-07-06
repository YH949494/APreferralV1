# APReferral Bot — Phase 5: Admin Safety Architecture

Status: **design only, no code changes**. This document specifies the target
architecture for RBAC, action confirmation, audit logging, and emergency
controls. It builds on the endpoint inventory already captured in
`docs/ADMIN_UNIFICATION_DESIGN.md`, but narrows the role model to the six
roles requested for this phase and adds the confirmation/audit/emergency
layers that document left as "Phase 2, unbuilt."

## Why this phase exists — current state

The audit that triggered this doc found, and this survey confirms against
the code:

- **No RBAC.** Every admin surface collapses to a single boolean. `admin_auth.py:323`
  `_admin_api_authorized()` accepts *any* of three interchangeable credentials —
  a signed Telegram-login session cookie (`admin_auth.py:138`), a Telegram
  user-id/username allowlist (`vouchers.py:267` `_is_cached_admin()`), or a
  static shared secret `ADMIN_PANEL_SECRET` (`vouchers.py:337`
  `_admin_secret_ok()`). Whoever holds any one of these can call every route.
- **Almost no confirmations.** The only `confirm()` in the admin frontend is
  `static/admin-dashboard.js:2812` before archiving a campaign. Ending a
  campaign, starting a voucher drop (`start_now`), granting XP, overwriting a
  bonus voucher, approving affiliate payouts, and committing a backend
  segment run all execute on a single click with no re-confirmation.
- **Hidden APIs.** Several mutating routes (e.g. `/v2/miniapp/admin/drops/<id>/actions`,
  `/api/admin/leaderboard/affiliate/snapshot/regenerate`) exist and are callable
  by anyone holding the shared secret, without being surfaced or guarded in the
  UI that's supposed to be the only path to them.
- **Unlimited XP grant.** `POST /api/add_xp` (`main.py:6097`) has a per-admin
  cooldown (`admin_xp_cooldowns`) but no cap on grant size, no second
  approval, and no structured audit entry beyond whatever the caller logs.
- **Global bonus overwrite.** `POST /api/admin/set_bonus` (`main.py:6191`)
  overwrites the active bonus voucher value with no versioning, no diff
  shown to the admin, and no confirmation step.
- **Destructive actions with no guardrail.** `DELETE /api/admin/campaigns/<id>`
  (`campaigns.py:311`), drop `start_now`, and affiliate approve/reject
  (`main.py`, `/v2/miniapp/admin/affiliate/<id>/approve|reject`) are
  one-shot, irreversible, and un-typed-confirmation-gated.
- **Audit trail is login-only.** `admin_auth.py:174` `_audit_sink()` writes to
  `admin_login_audit` for login/logout/rate-limit events only. There is no
  before/after mutation log for campaigns, bonuses, XP, or affiliate payouts.

Stack for reference: Flask 3.0 blueprints (`main.py`, `campaigns.py`,
`admin_auth.py`, `channel_reactivation.py`), MongoDB via PyMongo
(`database.py`), server-rendered admin UI (`static/admin-dashboard.html/.js`),
Fly.io deployment.

---

# RBAC

## Role hierarchy

Six roles, ordered least → most privileged. Roles are **additive tiers**
except Risk Manager and Affiliate Manager, which are **lateral specialist**
roles that sit above Operator but do not inherit Campaign Manager's
authoring rights — they exist to separate "can end/pause anything
financial or risk-related" from "can author campaigns."

| Role | Inherits | Grants |
|---|---|---|
| **Viewer** | — | Read-only: dashboards, audit log (own scope), reports, exports of non-PII aggregates |
| **Operator** | Viewer | Day-to-day support: XP grant (capped), join-request handling, welcome-voucher issuance, viewing PII on a single user record |
| **Campaign Manager** | Operator | Author/edit campaigns and voucher drops, pause/resume campaigns, segment-config edits, bonus voucher edits (bounded) |
| **Risk Manager** | Operator | Voucher-hunter / abuse review, claim-risk overrides, segment rule simulation and backend-segment-commit approval, emergency claim disable |
| **Affiliate Manager** | Operator | Affiliate pool upload, affiliate payout approval/rejection, leaderboard regeneration, affiliate-issue-current-month |
| **Super Admin** | all of the above | End campaign, batch release, dual-confirmation actions, kill-switches, role management, audit-log administration, override of any role gate |

Design rationale for the lateral split: Campaign Manager should not be able
to unilaterally approve affiliate payouts (financial control separation),
and Affiliate Manager should not be able to author campaigns or touch
abuse/risk tooling (least privilege — a compromised affiliate-ops account
can't reshape campaigns or waive risk holds). Both report up to Super
Admin, who can act as any role plus the actions reserved for it alone.

## Data model

New Mongo collection `admin_roles`:

```
{
  _id: ObjectId,
  admin_id: <telegram user id>,          # keys off existing admin_cache identity
  role: "viewer" | "operator" | "campaign_manager" | "risk_manager"
        | "affiliate_manager" | "super_admin",
  granted_by: <admin_id>,
  granted_at: datetime,
  revoked_at: datetime | null,
  notes: str
}
```

Replaces the current flat `admin_cache` boolean allowlist — `admin_cache`
becomes "is this identity a known admin at all" (authentication), and
`admin_roles` answers "what can they do" (authorization). The static
`ADMIN_PANEL_SECRET` bypass path (`vouchers.py:337`) is removed for all
mutating routes once RBAC ships (see Migration Strategy) — a shared secret
cannot carry a role, so it's structurally incompatible with RBAC and is the
biggest hole to close.

## Enforcement

A single decorator, `@require_role(min_role)`, wraps every mutating admin
route, replacing today's `_admin_api_authorized()` boolean check:

```
@require_role("campaign_manager")
def end_campaign(...): ...
```

`require_role` resolves the caller's role from `admin_roles` (looked up by
the authenticated session's admin_id — session auth becomes mandatory, the
shared-secret path is retired for role-gated routes), checks it against the
tier ordering above (with Risk Manager / Affiliate Manager treated as
their own lateral tier, satisfying only routes explicitly tagged for that
specialty or Super Admin), and short-circuits with `403` + an audit entry
(`role_denied`) on failure — denials are audited too, since a pattern of
denied attempts is itself a signal.

## Endpoint → role mapping (safety-relevant subset)

| Action | Endpoint | Minimum role |
|---|---|---|
| End campaign | `PUT /api/admin/campaigns/<id>` (status→ended) | Super Admin |
| Pause campaign | `PUT /api/admin/campaigns/<id>` (status→paused) | Campaign Manager |
| Batch release (voucher drop `start_now`) | `POST /v2/miniapp/admin/drops/<id>/actions` | Super Admin |
| Affiliate payout | `POST /v2/miniapp/admin/affiliate/<id>/approve`, `/issue-current-month` | Affiliate Manager |
| Bonus voucher overwrite | `POST /api/admin/set_bonus` | Campaign Manager |
| XP grant | `POST /api/add_xp` | Operator (Super Admin above per-grant cap, see Confirmation Matrix) |
| Segment sync | `POST .../backend-segment-engine/run` | Risk Manager |
| Backend segment commit | (new) segment-snapshot promote-to-production | Super Admin |
| Reactivation launch | `POST /api/admin/channel-reactivation/start` | Risk Manager |

---

# Confirmation Matrix

Four confirmation tiers, escalating with blast radius and reversibility.
Each tier is a reusable frontend component plus a backend assertion — the
backend must independently verify the confirmation artifact was presented,
not just trust a client-side flag, otherwise a scripted client can skip the
dialog.

| Tier | UX | Backend requirement |
|---|---|---|
| **Small** — single confirm | One modal: "Pause campaign 'Summer Promo'? This can be undone." OK/Cancel. | Request carries `confirm_token` minted when the modal opened (short TTL, single-use) |
| **Medium** — type-to-confirm | Modal requires typing the exact campaign/entity name before the action button enables. | Backend re-validates the typed string server-side against the entity's current name before executing — not just client-side gating |
| **High** — OTP | 6-digit OTP sent to the admin's registered Telegram account via the bot (reuses existing bot messaging, no new channel), 5-minute expiry, single use. | Backend verifies OTP against a short-lived hashed code stored server-side (`admin_otp_challenges` collection), rate-limited attempts |
| **Critical** — dual confirmation | Actor submits the action, which enters a `pending_second_approval` state; a **second, different** admin holding an equal-or-higher role must approve within a window (default 30 min) before it executes. | Backend enforces `approver_id != requester_id`; both identities and both timestamps are recorded in the audit entry |

## Action → tier mapping

| Action | Tier | Rationale |
|---|---|---|
| Pause campaign | Small | Reversible (resume), single-entity, no money movement |
| Segment sync (dry-run engine run) | Small | Read-heavy, produces a snapshot, doesn't touch player-facing state |
| Bonus voucher overwrite | Medium (type campaign/voucher name) | Overwrites a value every future claim reads — cheap to typo, expensive to mis-click |
| XP grant above a threshold (e.g. >500 XP or >10x the Operator cap) | Medium (type target username + amount) | Bypasses the automatic cooldown/cap path, so needs an explicit human check |
| Reactivation launch | Medium (type campaign name) | Sends messages to a live audience; reversible via pause but not un-sendable |
| End campaign | High (OTP) | Terminal state — no "undo," cuts off any still-eligible claims immediately |
| Backend segment commit (promote snapshot to production targeting) | High (OTP) | Changes who a large batch of live campaigns targets |
| Affiliate payout | High (OTP) | Moves real money/rewards to a third party |
| Batch release (voucher drop `start_now`) | Critical (dual confirmation) | Irreversible mass-issuance to an unbounded audience in one action — the design doc already flags this as "highest-risk mutation surface" |
| XP grant at bulk/segment scale (multi-user batch grant, if/when built) | Critical (dual confirmation) | Unlimited-XP risk called out in the audit — a batch grant multiplies a single mistake across many accounts |

Small and Medium confirmations are enforced entirely within the existing
request/response cycle (mint token → submit with token). High and Critical
require a second round trip (OTP submission; second-approver action) and so
introduce a `status: pending_otp | pending_approval` field on the
underlying resource, surfaced in the admin UI as an explicit "awaiting
confirmation" state rather than a silent in-flight request.

---

# Audit System

## Schema

New collection `admin_audit_log` (distinct from the existing
login-only `admin_login_audit`, which stays scoped to session events):

```
{
  _id: ObjectId,
  actor_id: <telegram admin id>,
  actor_role: str,                 # role at time of action, not looked up later
  action: str,                     # e.g. "campaign.end", "bonus.set", "xp.grant"
  resource_type: str,              # "campaign" | "voucher_drop" | "affiliate" | ...
  resource_id: str,
  before: <doc snapshot | null>,    # full prior state for mutations; null for creates
  after: <doc snapshot | null>,     # full new state; null for deletes
  confirmation_tier: "none" | "small" | "medium" | "high" | "critical",
  confirmation_ref: str | null,     # OTP challenge id / dual-approval record id
  second_approver_id: str | null,   # populated for critical-tier actions
  ip: str,
  user_agent: str,
  session_id: str,
  at: datetime,
  result: "success" | "denied" | "error",
  error_detail: str | null
}
```

Every route wrapped by `@require_role` also gets wrapped by
`@audit_action(action_name, resource_type)`, which:

1. Snapshots the resource **before** the handler runs.
2. Executes the handler.
3. Snapshots the resource **after** (or marks `after: null` on delete).
4. Writes one `admin_audit_log` entry regardless of success/failure —
   denials and errors are audited too, not just successful mutations.

This is a single decorator applied at the route layer, not scattered
logging calls, so coverage is enforced by code review (any new mutating
route missing the decorator is a lint/review finding) rather than by
developer memory.

## Access and retention

- Audit log is **append-only** — no update/delete API is exposed; Mongo-level
  write permissions on `admin_audit_log` are restricted to the app's service
  account, with no admin-facing delete route at any role, including Super
  Admin (a Super Admin can *view* everything, including entries about their
  own actions, but cannot edit history).
- Viewer role sees audit entries scoped to read-only/non-sensitive actions;
  full audit visibility (who/before/after on financial and destructive
  actions) requires Risk Manager, Affiliate Manager, or Super Admin,
  matching the principle that audit visibility should mirror action
  authority.
- Retention: indefinite for Critical/High tier actions; standard 18-month
  rolling window for Small/Medium, configurable, matching whatever
  data-retention policy already governs `voucher_claims`/`xp_events`.

---

# Emergency Controls

A dedicated "kill switch" panel, gated to **Super Admin only** (Risk
Manager can *propose* a claims/affiliate freeze but Super Admin must
confirm — freezing player-facing systems is high blast radius even though
it's the "safe" direction), backed by a single collection so all switches
are visible in one place:

```
admin_kill_switches: {
  _id: "claims" | "affiliate" | "welcome" | "reactivation" | "<campaign_id>",
  disabled: bool,
  disabled_by: str,
  disabled_at: datetime,
  reason: str,
  re_enabled_by: str | null,
  re_enabled_at: datetime | null
}
```

| Switch | Effect | Checked at |
|---|---|---|
| Kill campaign | Sets a specific campaign to `status: ended` immediately, bypassing normal edit flow | `campaigns.py` status resolution, checked on every claim-eligibility lookup |
| Disable claims | Global flag checked at the top of the voucher-claim path in `vouchers.py`; all claim attempts short-circuit with a user-facing "claims paused" message instead of 500ing | `vouchers.py` claim entrypoints |
| Disable affiliate | Blocks new affiliate approvals/payouts and pool uploads; existing pending payouts stay pending, don't auto-cancel | affiliate approve/issue routes |
| Disable welcome | Stops new-joiner welcome ticket issuance (`welcome_eligibility`/`welcome_tickets` path) | welcome-claim entrypoint in `vouchers.py` |
| Disable reactivation | Calls the existing `channel_reactivation.set_campaign_active(db, active=False, ...)` (`channel_reactivation.py:84`) — this mechanism already exists, the emergency panel just gives it a one-click Super-Admin-only front door with audit + confirmation | `channel_reactivation.py` |

Emergency controls are **Small-tier confirmation to disable** (speed
matters when something's actively wrong) but **Medium-tier (type the
switch name) to re-enable** — asymmetric on purpose, since re-enabling a
switch that was flipped for a real incident is the action most likely to be
done in error under time pressure once the incident "feels" over. Every
flip, in either direction, is a `Critical`-severity audit entry regardless
of confirmation tier used, and triggers a notification to all Super Admins
(reusing the existing Telegram bot messaging used for OTP delivery).

---

# Admin Security

- **Retire the shared-secret bypass for mutation.** `ADMIN_PANEL_SECRET`
  (`vouchers.py:337`) and the `BYPASS_ADMIN` flag (`vouchers.py:206/336`,
  currently hardcoded to `False` but present as a landmine) are removed from
  every route that gets an `@require_role` decorator. A shared static
  secret cannot express "which role," so once RBAC lands, mutating routes
  must accept only the authenticated session. The secret path may remain
  for narrow, read-only service-to-service calls (if any exist), but not for
  anything in the Confirmation Matrix above.
- **Session hardening.** Continue using the existing Telegram Login Widget
  HMAC verification (`admin_auth.py:67`) as the identity proof, but bind
  `admin_roles` lookups to that verified identity only — no route should
  accept a role or admin-id from a client-supplied header/body field.
- **OTP delivery channel reuse.** High-tier confirmations reuse the existing
  Telegram bot (python-telegram-bot) rather than adding SMS/email
  infrastructure — the bot already has a verified channel to every admin's
  Telegram account via the login widget.
- **Rate limiting on sensitive actions.** Extend the existing
  `admin_xp_cooldowns`-style per-admin rate limiting pattern to OTP requests
  (prevent OTP-spam as a denial-of-service against an admin's Telegram) and
  to dual-confirmation submissions (cap pending approvals per requester).
- **Least-privilege defaults.** New admins default to Viewer; role grants
  are themselves a Super-Admin-only, Medium-tier-confirmed, audited action
  (`admin.role_grant` / `admin.role_revoke` entries in `admin_audit_log`).
- **Separation of duty enforcement.** The dual-confirmation approver check
  (`approver_id != requester_id`) plus the Campaign Manager / Affiliate
  Manager / Risk Manager lateral split together prevent any single
  compromised or careless account from reaching end-to-end control over a
  campaign, its risk review, and its payout in one session.

---

# Migration Strategy

Phased so existing admin workflows never go fully dark, and so each phase
is independently revertible if it surfaces a blocker.

**Phase 5.1 — Foundations (additive, no behavior change)**
- Add `admin_roles` and `admin_audit_log` collections.
- Backfill `admin_roles` from the current `admin_cache`/hardcoded-username
  allowlist: every existing admin identity gets `super_admin` initially
  (preserves current de-facto access — nobody is locked out on cutover day).
- Wrap all mutating routes with `@audit_action` in **log-only** mode (no
  enforcement yet) to validate before/after snapshotting works correctly
  against real traffic before it's load-bearing.

**Phase 5.2 — RBAC enforcement**
- Ship the admin-facing role-management UI (Super Admin only) and have a
  human re-triage the initial all-`super_admin` backfill down to the actual
  intended roles per person.
- Flip `@require_role` from log-only to enforcing, route by route, starting
  with the lowest-traffic/highest-risk routes (batch release, affiliate
  approve) and ending with the highest-traffic/lowest-risk ones (XP grant),
  so any integration gap is caught on a route with few daily calls first.
- Keep `ADMIN_PANEL_SECRET` working in parallel during this phase as a
  fallback, logged loudly (`admin_audit_log` entry tagged
  `legacy_secret_auth`) every time it's used, to make the cutover date
  data-driven rather than guessed.

**Phase 5.3 — Confirmation tiers**
- Ship Small/Medium tiers first (pure frontend + short-TTL token, no new
  infra) across the action list in the Confirmation Matrix.
- Ship OTP (High tier) once the bot-messaging integration for OTP delivery
  is in place; gate End Campaign, Backend Segment Commit, and Affiliate
  Payout behind it.
- Ship dual confirmation (Critical tier) last, since it's the only tier
  requiring a second human in the loop and needs the role-population work
  from 5.2 to be accurate (an org with only one Super Admin can't use it —
  flag this as a rollout prerequisite, not an edge case to code around).

**Phase 5.4 — Emergency controls + secret retirement**
- Ship the kill-switch panel (Super Admin only), wiring `disable_claims`,
  `disable_affiliate`, `disable_welcome` checks into their respective
  entrypoints, and the existing `channel_reactivation.set_campaign_active`
  into the same panel.
- Remove `ADMIN_PANEL_SECRET` acceptance from all mutating routes once
  Phase 5.2 telemetry shows zero `legacy_secret_auth` audit entries for a
  full rolling window (e.g. 30 days) — data-gated removal, not a fixed date.
- Delete the now-dead `BYPASS_ADMIN` flag and its branches in `vouchers.py`
  entirely rather than leaving a disabled landmine in the code.

**Rollback posture at every phase**: each phase is additive or route-scoped
enforcement, so rolling back means disabling the `@require_role`/tier
enforcement flag for the affected routes (falling back to Phase 5.1's
log-only behavior) without touching data — no phase performs a destructive
migration that can't be paused.
