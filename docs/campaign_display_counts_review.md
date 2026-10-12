# Campaign manual joins and qualified counts

Audited current main `680213b`. The core feature already exists in merged PR
#521 (`884b32e`): both admin forms contain Joins count; storage uses separate
`joins_count` and `qualified_count`; the shared builder computes manual
conversion, enforces counts, and supplies Affiliate, Money Room and preview.
An admin UI still showing only qualified counts does not match these source
assets. No deployed version/cache was inspected; do not claim a runtime cause
or deploy automatically.

The smallest follow-up fixes remaining count-input gaps and makes the existing
display-only boundary explicit. Explicit null joins were accepted as unknown,
and null update counts were ignored rather than validated. Extremely large
digit strings in the admin form could become Infinity, which JSON serialization
turns into null. The form parser now rejects unsafe numbers; the server rejects
explicit null counts on create/update while preserving omitted legacy joins.

## Source trace and behavior

- `static/admin-dashboard.html`: existing create fields (display name, joins,
  qualified). `static/admin-dashboard.js`: existing edit fields and stable
  participant entry-ID requests; strict integer parser now also rejects unsafe
  numbers. Server errors are already shown via their clear `message` text.
- `campaign_display_override.py`: shared validation and participant persistence
  in `campaign_display_overrides`. New private omitted-value sentinel separates
  a missing count from an explicitly invalid null. No database schema change.
- `build_public_campaign_activity`: existing ranking (qualified, joins,
  conversion, name/entry-ID tie break), date window, visible-row inclusion and
  genuine calculations are unchanged. Manual rows use prefixed stable IDs,
  never names. Public rows keep the existing convention and contain no genuine
  user IDs or invented verification flags.
- `static/index.html`: Affiliate campaign table uses builder counts/rates;
  genuine My Stats still uses genuine referrals only. Campaign banner now
  identifies display-only counts and genuine reward progress.
- `static/creator-share.html`: Money Room's existing campaign renderer uses the
  same values, preserves duplicate names as separate rows, and now identifies
  the display-only section.
- `render_campaign_activity_announcement_text`: the existing announcement text
  generator consumes the same builder output as both previews. Active campaign
  text now includes the same display-only/reward boundary. No send action.

Counts remain strict non-negative integers bounded by the existing ceiling.
Boolean, floating-point (including 20.0), string and negative JSON values are
rejected; qualified greater than known joins is rejected after merging updates
with stored values. The API now additionally rejects explicitly supplied null
counts with HTTP 400. Existing omitted-joins API requests remain compatible.
Old documents with absent/null joins still read as unknown; name-only edits do
not invent joins, and editing them to add joins remains supported. Known joins
cannot be cleared with null to bypass the consistency check.

Manual conversion is qualified/joins for positive joins, otherwise unknown.
The existing public total is null with `joins_total_complete=false` whenever
any included row's joins are unknown; the admin's known partial sum is labeled
partial. Hidden/disabled/expired rows keep their existing inclusion behavior.
No migration, live activation, genuine-record update or analytics change.

## Examples

Manual entries are additional participants. Genuine rows retain their separate
`genuine:<id>` identities and original scores; manual rows use `manual:<entry_id>`.
The builder appends visible manual rows, sorts the combined ranking, and leaves
the existing surface display limit to the consumer. Matching names never merge.
Ten genuine rows plus a higher-scoring manual row produce eleven ranked rows;
Money Room displays the first ten, while the displaced genuine row remains intact.
Hiding the manual row, disabling the campaign or expiring it restores all ten
genuine rows. Regression tests verify this and unchanged genuine database records.

| Row | Joins | Qualified | Conversion |
| --- | ---: | ---: | ---: |
| Genuine | 10 | 4 | 40.0% |
| Manual | 30 | 20 | 66.7% |
| Manual zero | 0 | 0 | — |
| Legacy manual | — | 7 | — |

0 joins/20 qualified, or any qualified greater than joins, is rejected. A legacy
7-qualified row may be edited to 30 joins/20 qualified; the prior missing joins
were never assumed to be zero or seven.

## Verification and limits

Focused Python suites exercise actual admin routes/shared builders, invalid
inputs, legacy editing, totals, schedule/disable/expiry, duplicate names,
announcement text parity, and no changes to genuine ledgers/XP/rewards.
Node tests execute the production Affiliate and Money Room render functions,
admin parsing, duplicate-row rendering and existing genuine My Stats rendering.
They use isolated in-memory data/DOM fixtures; no production or Telegram calls.

There is no campaign-specific announcement sender implementation in this
repository. The shared text generator and previews are verified; actual
Telegram delivery cannot be tested or wired without that implementation.
Existing weekly-growth and milestone senders concern genuine activity and
remain untouched. This patch does not invent a replacement delivery system.
A deployed admin still lacking the existing joins form requires a separate
asset/version investigation; no deployment was performed here.
