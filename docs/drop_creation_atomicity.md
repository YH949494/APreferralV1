# Voucher drop creation: atomicity and recovery

Applies to `POST /v2/miniapp/admin/drops` (`vouchers.create_drop_from_spec`, also used by the Campaign Builder compiler) and, for the duplicate pre-check only, `POST /v2/miniapp/admin/drops/<id>/codes`.

## What changed
| Before | After |
|---|---|
| Drop document inserted first; codes validated afterwards (empty list, bad pool, duplicate codes) | Everything is validated before the first write |
| `db.vouchers.code` is globally unique, so a reused code returned `409 duplicate_code` after the drop and the other codes already existed | Existing codes are looked up first; a clash returns 409 with `inserted: 0`, `duplicates` and up to 20 `duplicateCodes`, and writes nothing |
| Personalised path had no error handling (collisions surfaced as unhandled errors with a drop left behind) | Same validation and rollback as pooled; duplicate usernames and codes are rejected up front |
| A retry after any failure created a second drop | A failed call removes exactly what it wrote, so a retry is safe |

## How atomicity is achieved without transactions
Pre-validation cannot close the window where another request takes a code between the check and the insert, so the write is undoable instead:

1. The drop is inserted **hidden**: `status: "paused"`, `internal_only: true` (both already exclude a drop from every claim and listing path) and `creation_state: "creating"` with `creation_started_at`.
2. Vouchers are inserted in chunks under the new, never-before-seen drop id.
3. One conditional update (`creation_state` must still be `creating`) publishes the drop: sets the final status and remaining counters and `$unset`s the three markers. The published document has exactly the shape it had before this change.
4. Any failure at step 2 or 3 deletes the vouchers carrying that drop id and the hidden drop.

While a drop is `creating`/`failed`, add-codes and every drop action (including delete) return `409 drop_creating`, and the admin list shows status `creating` / `creation_failed`.

## Recovery
If the process dies mid-creation (or the rollback itself fails) the drop stays hidden and unclaimable. It is removed automatically by the next create call once it is `failed` or older than `DROP_CREATION_STALE_MINUTES` (10), so a retry with the same codes is never blocked. A drop that holds a claimed/reserved voucher is never removed (reported as `protected`).

Manual inspection / cleanup: `POST /v2/miniapp/admin/drops/reconcile-creations`
- body `{}` or `{"dryRun": true}`: lists what would be removed (default);
- body `{"dryRun": false}`: removes it; optional `staleMinutes` (default 10; values below 2 are raised to 2 so a request that is still inserting is never reclaimed — records already marked `failed` are cleaned immediately regardless).
Response: `{status, removed[], in_flight[], protected[], dry_run}`.

## Known limits
- Add-codes to an existing drop: known duplicates are refused with no writes; a code taken by a concurrent request *between* the check and the insert still takes the older partial-insert path (counters stay correct).
- No server-side idempotency key yet (separate change): a response lost after a successful create can still be re-submitted by the admin; the dashboard now tells the admin to check Existing Drops first.
- Campaign Builder segment campaigns hand every segment drop the same code list; under the unique index only the first can be created. That was already true (the second used to leave an empty live drop behind); it now fails cleanly and is reported in `errors`.
