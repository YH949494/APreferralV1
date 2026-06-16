"""Phase 5B — UIM Formula Mapping / Backend KPI Gap Report (read-only).

Phase 5 (the "Data -> Validation / UIM Compare" panel) proved backend KPI
*values* don't match UIM, e.g.:

    Total Campaign Players   UIM 970     vs Backend 123,715
    High Value Players       UIM 20      vs Backend 0
    New Player Total         UIM 15      vs Backend 121

The gap isn't a bug in arithmetic — it's that the backend is answering a
*different question* than UIM for several metrics (wrong universe, wrong
field, or a rule that doesn't exist in the backend at all). Before any
bot-facing segment write happens, that gap needs to be documented metric by
metric so the Validation page can say *why* a number is off instead of
just *that* it's off.

This module is purely descriptive. It does not:
  - fetch the UIM sheet itself (Phase 5's ``uim_validation.py`` already does
    that; this module documents the *intended* tabs/columns per metric,
    which still need to be confirmed against the live sheet by someone with
    sheet access — see ``confirmed`` below)
  - compute any new backend numbers
  - create proxy/approximate calculations standing in for a missing metric
  - touch segment classification, voucher allocation, public-pool
    probability, or reward logic

``implementation_status`` values:
  - "exact_available"    backend has the exact field/rule UIM uses
  - "backend_missing"    UIM's source data has no backend equivalent at all
  - "definition_mismatch" backend computes *something* under this name, but
                          a different universe/rule than UIM's definition
  - "source_missing"     this codebase can't confirm the UIM source
                          tab/columns without live sheet access (sandbox has
                          no Google credentials configured); the mapping
                          below is the best-known label match and must be
                          verified against the real sheet before treating it
                          as ground truth
"""

from __future__ import annotations

KPI_MAPPING: list[dict] = [
    {
        "uim_metric_key": "total_campaign_players",
        "uim_display_label": "Total Campaign Players",
        "source_tab": "dashboard",
        "source_columns": "Column A=KPI, B=\"Total Campaign Players\", C=value",
        "confirmed": True,
        "uim_rule_plain_english": (
            "Count of accounts that participated in *this* referral campaign "
            "specifically (joined via a tracked campaign source/channel), "
            "not every registered account in the system. UIM's figure (970) "
            "is a campaign-scoped cohort, not the whole user base."
        ),
        "backend_current_metric_used": (
            "dashboard_panels._compute_backend_validation_metrics() currently "
            "uses users_col.count_documents({}) — every document in the "
            "`users` collection, i.e. ALL registered users regardless of "
            "campaign/source."
        ),
        "backend_gap": (
            "Wrong universe. Backend counts the entire `users` collection "
            "(123,715) instead of campaign-scoped accounts (UIM: 970). The "
            "backend has no concept of 'this campaign' as a filter — there is "
            "no field on `users` (or any join table) that records which "
            "campaign/referral source a user's join event belongs to in a way "
            "that matches UIM's campaign cohort definition."
        ),
        "implementation_status": "definition_mismatch",
        "recommended_backend_implementation": (
            "Do NOT default Total Campaign Players to users_col.count_documents({}). "
            "Identify the campaign-scoping field UIM uses (likely a "
            "join/referral-source tag, e.g. `joins.event`/`referral_source` "
            "filtered to this campaign's tracked channels/links) and confirm "
            "it against the `dashboard`/`player_detail` tabs. Until that field "
            "exists and is confirmed, report backend_value=None (gray/missing) "
            "instead of substituting the all-users count — a wrong number "
            "that 'looks like an answer' is worse than an honest gap."
        ),
    },
    {
        "uim_metric_key": "voucher_claimer_accounts",
        "uim_display_label": "Voucher Claimer Accounts",
        "source_tab": "dashboard / redeem_account_claim_audit",
        "source_columns": (
            "dashboard: A=KPI, B=\"Voucher Claimer Accounts\", C=value. "
            "redeem_account_claim_audit (not yet inspected — no sheet access "
            "in this environment): expected per-account claim audit rows, "
            "likely an account/user-id column plus a claim-status column."
        ),
        "confirmed": False,
        "uim_rule_plain_english": (
            "Distinct count of accounts with at least one successful voucher "
            "claim. Likely cross-referenced against redeem_account_claim_audit "
            "for the authoritative claim ledger rather than the dashboard "
            "tab's own count."
        ),
        "backend_current_metric_used": (
            "Not currently mapped in dashboard_panels._VALIDATION_SEGMENT_METRIC_KEYS "
            "(reported as backend_value=None / gray today)."
        ),
        "backend_gap": (
            "Backend has the right raw data (`voucher_claims` collection, one "
            "doc per claim, with `user_id` and `status`) but no aggregation "
            "wired into the validation panel yet. This is a missing wiring "
            "gap, not a missing data gap."
        ),
        "implementation_status": "backend_missing",
        "recommended_backend_implementation": (
            "Add a distinct-user aggregation over `voucher_claims` filtered to "
            "successful/claimed status (mirror the status filter already used "
            "by dashboard_panels._windowed_claim_filter / the funnel panel's "
            "claimed_filter), e.g. "
            "`voucher_claims_col.distinct('user_id', {'status': {'$in': [...claimed variants...]}})`. "
            "Needs the redeem_account_claim_audit tab inspected to confirm "
            "whether UIM's count includes claims this codebase doesn't track "
            "(e.g. claims made outside this bot)."
        ),
    },
    {
        "uim_metric_key": "actual_players",
        "uim_display_label": "Actual Players",
        "source_tab": "player_detail / dashboard",
        "source_columns": (
            "dashboard: A=KPI, B=\"Actual Players\", C=value. player_detail "
            "(not yet inspected): expected a per-player row with a "
            "deposit/bet activity flag or amount column distinguishing "
            "players who actually played from accounts that only registered."
        ),
        "confirmed": False,
        "uim_rule_plain_english": (
            "Accounts that placed at least one real-money bet/deposit after "
            "registration — i.e. registered but never played accounts are "
            "excluded. This is a behavioral/activity definition, not a "
            "registration-count definition."
        ),
        "backend_current_metric_used": (
            "Not currently mapped (backend_value=None / gray today)."
        ),
        "backend_gap": (
            "Backend `users` documents have no bet/deposit/activity field at "
            "all (confirmed by inspecting config.py and the `users` schema "
            "used elsewhere in this codebase — only segment/classification "
            "fields like `for_bot_segment`/`bot_segment` exist, no wagering "
            "or deposit ledger). This data does not exist in the backend; it "
            "would have to come from the platform's bet/deposit ledger, which "
            "this bot does not currently ingest."
        ),
        "implementation_status": "source_missing",
        "recommended_backend_implementation": (
            "Do not approximate with an existing segment (e.g. do not treat "
            "`high_value` or `normal_actual` segment membership as a stand-in "
            "for 'played' — those are UIM-classification-derived labels, not "
            "activity facts, and conflating them would be a proxy "
            "calculation). Requires either (a) UIM's player_detail tab being "
            "synced into a new backend collection with a real activity flag, "
            "or (b) the platform's wagering data being exposed to this "
            "backend. Out of scope for Phase 5B; report as backend_missing "
            "(gray) until then."
        ),
    },
    {
        "uim_metric_key": "high_value_players",
        "uim_display_label": "High Value Players",
        "source_tab": "high_value_player_detail / dashboard",
        "source_columns": (
            "dashboard: A=KPI, B=\"High Value Players\", C=value. "
            "high_value_player_detail (not yet inspected): expected columns "
            "for post-bet activity and/or withdrawal amount per account, "
            "since the label implies an after-bet vs withdrawal comparison."
        ),
        "confirmed": False,
        "uim_rule_plain_english": (
            "UIM classifies a player as 'high value' using an after-bet vs "
            "withdrawal rule: the account's net position after wagering "
            "activity (deposits/bets) is compared against amount withdrawn, "
            "and accounts clearing some threshold under that comparison are "
            "tagged high value. This is a *financial activity* rule, "
            "completely independent of this bot's `for_bot_segment` label."
        ),
        "backend_current_metric_used": (
            "dashboard_panels._VALIDATION_SEGMENT_METRIC_KEYS maps "
            "high_value_players -> count of users where "
            "normalize_for_bot_segment(for_bot_segment/bot_segment) == "
            "'high_value'. That field is a label written by the weekly UIM "
            "bot-segment *sync* (bot_segment_sync.py), itself sourced from a "
            "different UIM sheet/tab (the bot-segment sheet, not "
            "high_value_player_detail) for a different purpose (controlling "
            "this bot's public-pool probability weighting)."
        ),
        "backend_gap": (
            "Definition mismatch, not missing data: backend currently reports "
            "0 because (most likely) very few/no users in `users` currently "
            "carry `for_bot_segment='high_value'` from the bot-segment sync, "
            "while UIM's High Value Players KPI (20) comes from the "
            "after-bet/withdrawal financial rule on a different tab entirely. "
            "Even if the counts happened to match by coincidence, this would "
            "be comparing two unrelated definitions."
        ),
        "implementation_status": "definition_mismatch",
        "recommended_backend_implementation": (
            "Do not reuse the bot-segment `for_bot_segment='high_value'` label "
            "for this KPI — that field exists to drive public-pool "
            "probability, not to reproduce UIM's financial high-value rule, "
            "and reusing it here would risk a feedback loop between the "
            "validation report and segment-driven bot behavior if anyone "
            "later 'fixes' the gap by writing back into segments. The correct "
            "fix is to ingest high_value_player_detail's after-bet/withdrawal "
            "columns (once confirmed) into a dedicated backend field/collection "
            "and compute High Value Players from that, independent of "
            "`for_bot_segment`. Until that exists, report backend_value=None."
        ),
    },
    {
        "uim_metric_key": "new_player_total",
        "uim_display_label": "New Player Total",
        "source_tab": "normal_player_detail / dashboard",
        "source_columns": (
            "dashboard: A=KPI, B=\"New Player Total\", C=value. "
            "normal_player_detail (not yet inspected): expected a Marketing "
            "raw_data column `is_new_player` (1/0) per account, per the "
            "confirmed UIM rule below."
        ),
        "confirmed": False,
        "uim_rule_plain_english": (
            "UIM counts an account as a 'new player' when Marketing's "
            "raw_data field `is_new_player` equals 1 for that account — a "
            "flag set by Marketing's own attribution logic at acquisition "
            "time, not something derivable from this bot's join/registration "
            "timestamp."
        ),
        "backend_current_metric_used": (
            "dashboard_panels._VALIDATION_SEGMENT_METRIC_KEYS maps "
            "new_player_total -> count of users where "
            "normalize_for_bot_segment(...) is in {'new_user','new_joiner'} — "
            "i.e. this bot's own segment classification of 'new', not "
            "Marketing's `is_new_player` flag."
        ),
        "backend_gap": (
            "Definition mismatch: backend's 'new_user'/'new_joiner' segment is "
            "set by the weekly bot-segment sync's own rules (see "
            "bot_segment_sync.py / config.py canonical segments), which were "
            "designed for *this bot's* public-pool probability weighting, not "
            "to mirror Marketing's `is_new_player` acquisition flag. The two "
            "concepts can diverge in both directions (a UIM-new player might "
            "not be bot-segment-new and vice versa), which is consistent with "
            "UIM 15 vs backend 121 — backend's 'new' bucket is much broader."
        ),
        "implementation_status": "definition_mismatch",
        "recommended_backend_implementation": (
            "Ingest Marketing's `is_new_player` flag (from "
            "normal_player_detail, once confirmed) into a dedicated backend "
            "field separate from `for_bot_segment`/`bot_segment`, and compute "
            "New Player Total from that flag directly — not from the bot's "
            "own new_user/new_joiner segment labels. Reusing the segment "
            "field would be a proxy calculation, which is explicitly out of "
            "scope."
        ),
    },
    {
        "uim_metric_key": "old_player_total",
        "uim_display_label": "Old Player Total",
        "source_tab": "normal_player_detail / dashboard",
        "source_columns": (
            "dashboard: A=KPI, B=\"Old Player Total\", C=value. "
            "normal_player_detail (not yet inspected): expected the same "
            "Marketing raw_data record as New Player Total, with "
            "`is_new_player = 0` (or an equivalent 'old'/'existing' flag) "
            "denoting the complement set."
        ),
        "confirmed": False,
        "uim_rule_plain_english": (
            "The complement of New Player Total within Marketing's "
            "raw_data: accounts where `is_new_player` is 0 (or an explicit "
            "'old'/'existing player' flag if Marketing tracks it separately "
            "rather than purely as the inverse of is_new_player)."
        ),
        "backend_current_metric_used": (
            "Not currently mapped in "
            "dashboard_panels._VALIDATION_SEGMENT_METRIC_KEYS "
            "(backend_value=None / gray today)."
        ),
        "backend_gap": (
            "Same root cause as New Player Total: this backend has no "
            "ingestion of Marketing's is_new_player flag at all, so neither "
            "the 'new' nor 'old' side of that classification can be computed "
            "honestly. There is no existing backend segment that maps to "
            "'old player' either — the bot-segment vocabulary "
            "(normal_actual/low_value/voucher_hunter/etc.) cuts the user base "
            "along a different axis (behavioral risk/value) than new-vs-old."
        ),
        "implementation_status": "backend_missing",
        "recommended_backend_implementation": (
            "Once Marketing's is_new_player flag is ingested for New Player "
            "Total (above), Old Player Total falls out as the same field's "
            "complement — no separate ingestion needed. Do not derive it as "
            "'total minus new_player_total minus unknowns' using a different, "
            "unrelated total (e.g. all of `users`), since that total is "
            "itself the wrong universe (see Total Campaign Players)."
        ),
    },
    {
        "uim_metric_key": "claim_risk",
        "uim_display_label": "Claim Risk (Medium/High Risk Claim Accounts, Abuse/Freeze Claim Accounts)",
        "source_tab": "dashboard / redeem_account_claim_audit",
        "source_columns": (
            "dashboard: A=KPI, B in {\"Medium Risk Claim Accounts\", "
            "\"High Risk Claim Accounts\", \"Abuse / Freeze Claim Accounts\"}, "
            "C=value per tier. redeem_account_claim_audit (not yet "
            "inspected): expected a per-account claim-count or claim-velocity "
            "column that the tier thresholds are computed from."
        ),
        "confirmed": False,
        "uim_rule_plain_english": (
            "UIM appears to bucket accounts into risk tiers (medium/high/"
            "abuse-freeze) by claim-count thresholds against "
            "redeem_account_claim_audit — e.g. accounts whose total "
            "voucher-claim count exceeds tier-specific cutoffs get bucketed "
            "into progressively higher risk. The exact thresholds are not "
            "confirmed from this codebase (no sheet access in this "
            "environment) — they must be read off redeem_account_claim_audit "
            "or the dashboard tab's notes column directly."
        ),
        "backend_current_metric_used": (
            "Not currently mapped in dashboard_panels (backend_value=None / "
            "gray today for all three claim-risk metrics)."
        ),
        "backend_gap": (
            "Backend has the raw ingredient (per-user claim count, derivable "
            "from `voucher_claims_col.aggregate([{'$group': {'_id': "
            "'$user_id', 'count': {'$sum': 1}}}])`, the same data already "
            "used elsewhere for repeat-claimer detection in "
            "dashboard_panels.py) but has no risk-tier thresholds defined "
            "anywhere in this codebase. Inventing thresholds without "
            "confirming UIM's actual cutoffs would be exactly the kind of "
            "proxy calculation this phase must avoid."
        ),
        "implementation_status": "backend_missing",
        "recommended_backend_implementation": (
            "Confirm UIM's exact claim-count cutoffs per tier from "
            "redeem_account_claim_audit (or the dashboard tab's per-row "
            "notes, which uim_validation.py already parses into "
            "`uim_note` — check whether any KPI row's note documents the "
            "thresholds). Only after the cutoffs are confirmed, add a "
            "claim-count aggregation + tier bucketing function. Do not guess "
            "thresholds, and do not write tier labels back onto `users` or "
            "feed them into segment classification/voucher logic — this "
            "report is diagnostic only."
        ),
    },
]


# uim_validation.METRIC_KEYS has more entries than this phase's 7 focus
# KPIs (e.g. the three claim-risk tiers are separate dashboard-tab rows but
# share one mapping/gap writeup here, since they're all governed by the same
# undocumented claim-count thresholds). Map each related metric key onto the
# KPI_MAPPING entry that documents it; metric keys with no entry below
# (new_users, welcome_abuse_invitees, high_risk_welcome_abuse,
# self_farming_risk_invitees) are out of scope for this phase and simply
# have no mapping/gap data yet.
_METRIC_KEY_ALIASES: dict[str, str] = {
    "medium_risk_claim_accounts": "claim_risk",
    "high_risk_claim_accounts": "claim_risk",
    "abuse_freeze_claim_accounts": "claim_risk",
}


def get_kpi_mapping() -> list[dict]:
    """Return the full Phase 5B KPI mapping/gap report. Never raises."""
    return [dict(entry) for entry in KPI_MAPPING]


def get_kpi_mapping_by_key(uim_metric_key: str) -> dict | None:
    """Look up the mapping/gap entry for a ``uim_validation.METRIC_KEYS``
    key, resolving claim-risk-tier aliases onto the shared "claim_risk"
    entry. Returns ``None`` for metric keys not yet documented (out of
    scope for this phase) rather than guessing.
    """
    lookup_key = _METRIC_KEY_ALIASES.get(uim_metric_key, uim_metric_key)
    for entry in KPI_MAPPING:
        if entry["uim_metric_key"] == lookup_key:
            return dict(entry)
    return None
