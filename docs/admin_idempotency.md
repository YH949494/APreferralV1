# Admin write idempotency

Module: `admin_idempotency.py`. Collection: `admin_idempotency` (`_id = "<scope>:<key>"`, TTL 24h on `expires_at`, index created best-effort at startup — no migration, nothing is read from it until a key is sent).

## Contract

| | |
|---|---|
| Key | `Idempotency-Key` header (new), or `idempotency_key` / `unique_key` body field (what the legacy Mini App panel already sends). 8–128 chars `[A-Za-z0-9._:-]`. Header wins. |
| No key | Behaviour is exactly what it was before this change. |
| First request | Runs. A **2xx** result is stored. |
| Retry, same key + same payload | Stored result is returned with `Idempotent-Replay: true`; the write is not run again. |
| Same key, different payload | `422 idempotency_key_reused`. Never guesses. |
| Same key while the first is still running | `409 request_in_progress` + `Retry-After`. Two simultaneous submissions cannot both execute. |
| First attempt failed (non-2xx / exception) | Key is released; retry runs again (the guarded writes roll back on failure — see `drop_creation_atomicity.md`). |
| Worker died holding the key | After the 120s lease, the next retry takes over (compare-and-swap). |
| Idempotency store unavailable | Fails open (logged). The operation's own guards still apply (unique `vouchers.code` index, drop creation markers). |

## Where it applies

| Endpoint | Scope | Notes |
|---|---|---|
| `POST /v2/miniapp/admin/drops` | `create_drop` | Additionally the **drop document stores the key** (`admin_request_key`, sparse index). A retry finds the drop it already made even if the idempotency record expired, was wiped, or the store was down: exactly-once does not depend on the 24h record. A key whose drop is still `creating` answers `409 drop_creating`. |
| `POST /v2/miniapp/admin/drops/<id>/codes` | `add_codes:<drop_id>` | Replays the first answer. After a crash mid-insert, the retry reports the honest `duplicate_code` (with the codes that did land) rather than inserting twice. |
| `POST /api/add_xp` | `xp_adjust` | Header key only. Ledger key is `adminreq:<key>`, so even without the record the XP ledger refuses a second grant. The fat-finger cooldown is kept (`update_user_xp(..., enforce_cooldown=True)`); soft failures that return HTTP 200 (`success:false`) are never cached. The legacy body-key path is unchanged (keyed ⇒ no cooldown), including its `Duplicate admin XP grant ignored.` answer. |

Not wrapped, on purpose: drop lifecycle ops (conditional updates already make a repeat a harmless `drop_changed`/no-op, see `drop_lifecycle.md`), affiliate reject (already answers `already_rejected` for a repeat), delete (resumable by design).

## Dashboard

`static/admin-dashboard.js` generates one request id per submission *intent* (`adminRequestKey(scope, payload)`): a retry of the same payload reuses it; success or a changed payload starts a new one. After a dropped connection the UI now says the retry is safe.

## Deployment

No migration. The TTL index and the sparse `ix_drops_admin_request_key` index are created by `ensure_voucher_indexes()` at startup (best-effort). Old clients without a key keep working unchanged. Rollback: revert the PR; stored records are inert and expire by themselves.

## Known limits

* Records are per key, not per business meaning: two *different* keys for the same intent are still two requests — that is what the unique code index and the XP cooldown are for.
* Mongo TTL deletion runs about once a minute; expiry is not exact and correctness does not rely on it.
