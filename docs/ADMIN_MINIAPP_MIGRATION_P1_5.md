# P1.5 — Migrate Critical MiniApp Admin Controls into Unified /admin

Status: implemented (frontend/admin-integration only). No backend routes, claim logic,
voucher allocation, FCFS logic, scheduler jobs, affiliate settlement logic, or player-facing
MiniApp flows were touched. All existing `/admin` views and Legacy MiniApp Admin links are
preserved.

## What moved into `/admin`

All controls below reuse the exact backend endpoints already used by
`static/index.html#admin-panel` (`vouchers.py` under `/v2/miniapp/admin/*` and `main.py`
under `/api/*`). Auth is unchanged: those endpoints already accept the same admin session
cookie (`admin_auth.session_admin()`) that `/admin` uses, in addition to `admin_secret` and
Telegram initData, so no backend auth changes were required.

| Control | Nav location (`data-view`) | Endpoint(s) reused |
|---|---|---|
| Voucher Drops (list + start/pause/end) | Campaign Control → `drops` | `GET /v2/miniapp/admin/drops_v2`, `POST /v2/miniapp/admin/drops/<id>/actions` |
| Create Drop | Campaign Control → `drops` | `POST /v2/miniapp/admin/drops` |
| Add Codes to Existing Drop | Campaign Control → `drops` | `POST /v2/miniapp/admin/drops/<id>/codes` |
| Affiliate Voucher Pools (upload + summary) | Affiliate → `affiliatePools` | `POST /v2/miniapp/admin/pools/upload`, `GET /v2/miniapp/admin/pools/summary` |
| Affiliate Pending Review / Manual / Simulated Pending | Affiliate → `affiliatePending` | `GET /v2/miniapp/admin/affiliate/pending?status=...` |
| Affiliate Approve / Reject | Affiliate → `affiliatePending` | `POST /v2/miniapp/admin/affiliate/<id>/approve`, `POST /v2/miniapp/admin/affiliate/<id>/reject` |
| Add / Reduce XP | Abuse & Risk → `xpAdjust` | `POST /api/add_xp` |
| Join Requests (view only) | Community Dashboard → `joinRequests` | `GET /api/join_requests` |

The Legacy MiniApp Admin links remain in the Campaign Control, Affiliate, and Community
Dashboard groups for anything not yet migrated or for cross-checking behavior during rollout.

## Simplifications vs. the legacy panel

The legacy Create Drop form also supports eligibility modes (`tier`/`user_id`/`admin_only`),
region targeting, hero image/title/subtitle, and a "new_joiner" welcome-voucher shortcut. The
migrated Create Drop form only covers the common path (name, type, start/end, priority, pool,
codes/assignments) to keep this step minimal and safe. Advanced drop configuration should
still be done via the Legacy MiniApp Admin link until a follow-up migrates it.

## Controls Not Migrated

| Control | Reason |
|---|---|
| Affiliate Approve/Reject UI in the *legacy* panel | There was none — `index.html` only ever exposed viewing pending ledger rows, never approve/reject buttons, even though the backend endpoints existed. The new `/admin` Pending Affiliate Rewards view is the first UI for this, not a relocation. |
| Join Requests approve/reject | No backend endpoint exists for it in either UI. Telegram join requests are auto-processed by the bot's `ChatJoinRequestHandler` (`main.py`); `GET /api/join_requests` only lists the current pending queue. Adding manual approve/reject would require a new backend route, which is out of scope for this frontend-only migration. |
| Drop eligibility modes (tier / user_id / admin_only), region targeting, hero image/title/subtitle, "new_joiner" welcome-voucher shortcut | Kept on the Legacy MiniApp Admin link. These are advanced/rarely-used drop configuration options; migrating them safely needs more UI real estate than this step's scope allows. |
| Create Bonus Voucher (`/api/admin/set_bonus`) | Not in the P1.5 scope list; still only on the Legacy MiniApp Admin link. |
