# XP snapshot: incremental settlement (2026-07)

## Root cause (Atlas alert: Query Targeting > 1000 on `referral_bot.xp_events`)

`scheduler.settle_xp_snapshots()` ran every 5 minutes via `main.tick_5min()`
and re-aggregated **the entire history** of `xp_events` on every run:

```python
pipeline = [
    {"$match": {"user_id": {"$ne": None}, "$or": [...invalidated...]}},
    {"$group": {"_id": "$user_id", "total_xp": {"$sum": "$xp"}, "weekly_xp": ..., "monthly_xp": ...}},
]
db.xp_events.aggregate(pipeline)
```

The `$match` has no time bound (it excludes almost nothing — only null
`user_id` and invalidated docs), and the week/month bucketing happens
*inside* `$group`'s `$cond`, not in `$match`, so no index on `created_at`
can narrow the scan. Every one of the ~2.7M+ documents in `xp_events` was
read on every single run, regardless of the `(user_id, created_at desc)` or
`(user_id, created_at, invalidated)` indexes already present — those
indexes support point lookups by user, not an unbounded full-collection
`$group`. This matches Atlas's reported 283M documents examined over 106
executions (~2.67M per run) and grows without bound as history accumulates.
Adding another index would not have helped: the query shape itself scans
the collection by design.

## Fix

Replace the full re-aggregation with an incremental, cursor-based settler
(`xp_snapshot.py`). Each run only reads `xp_events` inserted (or
invalidated) since the last run and applies `$inc` deltas to
`users.total_xp` / `weekly_xp` / `monthly_xp`. `xp_events` remains the
immutable source of truth; nothing about the write path (`grant_xp`) or the
event schema changed.

Key properties (see module docstring in `xp_snapshot.py`):

- **Idempotent & crash-safe without distributed transactions.** Every user
  update is guarded by a per-user watermark (`xp_snapshot_cursor` /
  `xp_snapshot_correction_cursor`) compared with `$lt` in the same atomic
  single-document update that applies the `$inc`. Replaying the same batch
  (crash-retry, or a second worker racing the same tick) matches zero
  documents the second time — no double count, no lost update.
- **Weekly/monthly rollover** is detected by comparing a stored `week_key`
  /`month_key` against the current KL week/month; only on a rollover does
  the (cheap, O(users)) `weekly_xp`/`monthly_xp` reset run — not on every
  tick, and never by rescanning `xp_events`.
- **Invalidation / rollback** (e.g. `rollback_pending_referral_xp.py`
  retroactively invalidating an already-counted event) is handled by a
  dedicated correction pass keyed off `invalidated_at`, bounded by a small
  partial index `{invalidated: 1, invalidated_at: 1}` — independent of
  total collection size regardless of how large `xp_events` grows.
- **Freshness/monitoring behavior preserved**: `snapshot_updated_at`,
  `snapshot_version`, and the `admin_cache.snapshot_heartbeat` doc are
  still touched for all users every run (cheap, O(users)), so
  `main._check_snapshot_freshness()` keeps working unchanged.

## Migration boundary

On first run after deploy (cursor collection `xp_snapshot_state` empty),
`xp_snapshot.bootstrap_cursor_if_missing()`:

1. Runs the **legacy full-history rebuild exactly once**
   (`scheduler._settle_xp_snapshots_full_rebuild`, the old code path,
   preserved verbatim) so current totals are known-correct.
2. Pins the incremental cursor to the current max `xp_events._id` — no
   historical event is ever replayed into the now-correct counters.
3. Stamps every user with that cursor value and every existing event with
   `xp_counted` (true/false, matching what the rebuild just summed) so a
   later invalidation of a *pre-migration* event is still eligible for the
   correction pass.

This is a one-time O(history) cost (identical to one existing run of the
job today), never repeated. No destructive cleanup, no data migration
script to run by hand — it happens automatically on first tick after
deploy.

## Rollback

Set `XP_SNAPSHOT_INCREMENTAL=0` to make `settle_xp_snapshots()` dispatch
back to `_settle_xp_snapshots_full_rebuild()` (unchanged legacy behavior).
No code revert needed; the legacy function is kept in `scheduler.py`
specifically as the rollback path and the migration bootstrap step.

## Second query: `referral_audit` find

`affiliate_rewards.is_user_blocked_for_self_invite()` ran:

```python
docs = db.referral_audit.find({"invitee_user_id": uid})
for doc in docs: ...
```

on every welcome-bonus eligibility check (new user `/start`, first
check-in), with **no supporting index on `invitee_user_id`** — the only
index on this collection was `referral_audit_inviter_created_idx` on
`{inviter_user_id, created_at}`, which doesn't match this query's filter
field, and `created_at` doesn't even exist on `referral_audit` documents
(they're written with `ts_utc` — see `main._write_referral_audit`), so that
index was dead weight for this workload. Every call was a full collection
scan returning every one of that user's audit rows, iterated in Python for
a boolean check the database can already answer in one indexed lookup.

Classified as: **missing filter/index** (not a dashboard/pagination issue —
`dashboard_panels.build_audit_panel`'s own `referral_audit` query already
has `.sort().limit(100)` and is unrelated).

Fix: added `referral_audit_invitee_user_id_idx` on `{invitee_user_id: 1}`
and rewrote the lookup as a single `find_one(..., {"_id": 1})` with the
equivalent `$or` condition (the original three-way check reduces to
`inviter_user_id == uid OR reason == "self_invite"` — the third branch was
strictly redundant), so at most one small document is read and returned
instead of the whole matching set.
