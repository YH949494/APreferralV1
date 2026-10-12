# Manual entries in the regular leaderboards

## Root cause and change

The Affiliate page previously substituted `campaign_activity.leaderboard` for
its actual monthly leaderboard when a campaign was active. Those genuine rows
used the campaign date window, so genuine monthly scores could differ. The
regular `/api/leaderboard` XP and Ref results did not include manual rows at all.

The regular `/api/affiliate/leaderboard` response now appends active, visible
manual participants to the genuine monthly snapshot, sorts using the existing
qualified/joins/conversion ranking, and applies its existing top-50 limit.
The frontend renders this ordinary `leaderboard` directly, without a campaign
banner or substitution. Genuine My Stats and reward progress use the original
snapshot. Manual legacy joins and conversion remain unknown (—).

The regular `/api/leaderboard` XP and Ref results now append explicitly entered
manual XP and Ref scores after reading genuine cached results. Each ranking
sorts by its existing score and retains its existing top-15 limit. Stable sorts
retain genuine tie order; no genuine cache entry is mutated. Existing campaign
selection, visibility, enable/disable and schedule gates control the overlay.
Hidden, disabled or expired entries disappear from both regular leaderboards.

## Admin and schema/API impact

Create and edit forms have Display name, Joins count, Qualified count, plus
optional XP points and Ref count. Storage remains in `campaign_display_overrides`
participants, with new optional integer fields `xp_count` and `ref_count`.
Both new fields use existing strict whole-number validation and count ceiling.
Explicit null, booleans, decimals, negatives and strings are rejected server-side.
Omitted XP/Ref fields remain absent and do not invent a score or enter that ranking.
Name-only edits preserve existing values; set an explicit zero to enter at zero.
Joins and qualified retain their existing constraints, including qualified <= joins.
Explicit null counts are rejected; omitted legacy joins remain unknown.
The admin parser rejects integers outside JavaScript's safe integer range.

Manual rows use `manual:<entry_id>` IDs and genuine rows use `genuine:<user_id>`.
Matching names never merge. Public manual rows carry `display_only=true` and
render as '(manual · display only)'; they are not presented as verified activity.
The genuine-only monthly and weekly snapshots, ledgers, XP, referrals, reward
eligibility, payouts, analytics and historical leaderboard archives are untouched.
There is no migration or production write in this change.

## Example

Genuine: 10 joins, 4 qualified, 100 XP, 4 Ref.
Manual: 30 joins, 20 qualified, 900 XP, 25 Ref.

The manual participant ranks above that genuine participant in the regular
Affiliate, XP and Ref rankings. The genuine participant retains exactly their
original values. A display limit can hide a lower genuine row without deleting
it. Removing the manual overlay restores the genuine-only results.

## Files and verification

- `campaign_display_override.py`: optional score validation/persistence and
  read-only merge helpers; existing display-only announcement notice.
- `main.py`: merge into regular Affiliate and XP/Ref response rows, preserving
  genuine My Stats and caches.
- `static/admin-dashboard.html`, `static/admin-dashboard.js`: create/edit fields,
  participant score columns and strict frontend parsing.
- `static/index.html`: ordinary Affiliate response rendering, unknown joins and
  manual row identification in Affiliate and XP/Ref.
- `static/creator-share.html`: existing display-only notice in Money Room.
- Focused Python and Node tests cover actual endpoint results, cached scores,
  duplicate names, ranking limits, zero scores, invalid inputs, legacy rows,
  edits, hidden/disabled/expired overlays, and unchanged genuine records.

Dedicated campaign activity APIs, Money Room campaign activity and campaign
announcement previews retain their existing campaign-window semantics. They
are not substituted for the regular monthly or weekly leaderboards. This
repository has no campaign-specific Telegram sender; the shared text generator
and previews are tested. No Telegram messages, campaign activation, production
writes, merge or deployment were performed.
