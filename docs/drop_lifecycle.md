# Voucher drop lifecycle ops

`POST /v2/miniapp/admin/drops/<id>/actions` with `{"op": ...}`. A drop's schedule (`startsAt`, `endsAt`) is changed only by `set_dates` or by an explicit `endsAt` on `start_now`.

| op | Allowed when | Effect | Refused with |
|---|---|---|---|
| `start_now` | upcoming, paused, or ended (ended needs `endsAt`) | status `active`; `startsAt` moves to now only if still in the future; **`endsAt` is kept** (or set to the explicit `endsAt`, KL local time, which must be in the future) | `409 already_live` (live drop; nothing changes), `409 end_time_passed` (ended, no `endsAt`), `400 bad_end` / `end_in_past` |
| `pause` | upcoming or live | status `paused`; dates untouched | `409 drop_ended`; paused is a no-op (`unchanged: true`) |
| `resume` | paused | back to `active` (or `upcoming` if its start is still ahead); dates untouched | `409 not_paused`, `409 drop_ended` |
| `end_now` | any | `endsAt = now`, status `expired`; already-expired is a no-op | |
| `set_dates` / `delete` | unchanged | unchanged | unchanged |

Every lifecycle write is conditional on the status that was read; if another admin changed the drop in between, the second request gets `409 drop_changed` and the first change stands.

Response additions (additive): `dropStatus`, `startsAt`, `endsAt`, `unchanged`. Errors carry a human `message`.
`GET /admin/drops_v2` and `GET /admin/drops` rows gain `availableOps` (`start_now|pause|resume|end_now`), the single source of truth for which buttons to show; drops still being created have none.

## Behavior change versus before
`start_now` used to set `startsAt = now` and `endsAt = now + 24h` unconditionally (shortening or extending the scheduled window, and extending the expiry of an already-live drop), and was the only way to un-pause, so un-pausing also reset the dates. The legacy Mini App panel (not modified) still sends `{"op":"start_now"}` from its Start button on every row; on a live drop that now returns `409 already_live` with a message the panel displays, and on a paused drop it behaves like Resume.
