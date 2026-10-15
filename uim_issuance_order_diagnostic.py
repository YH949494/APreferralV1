"""Read-only: why do Welcome records look redeemed BEFORE their voucher was issued?

``welcome_issuance_problem`` rejects a redemption earlier than the recipient's earliest issuance time
(``voucher_pools.issued_at`` / the WELCOME ledger row / ``new_joiner_claims.claimed_at``). When that is caused by a
clock-basis mix-up rather than real data, the gap clusters inside a +/-8h (GMT+8) window. This module replays the
committed UIM rows exactly like the preview (same fold), resolves each record's recipient with the production
``resolve_welcome_recipient``, and for every record the validator flags as ``redemption_before_welcome_issuance``
reports the gap, the source that supplied the earliest issuance time, and whether each hypothesis would clear it:

* ``claim_time_is_kl_wall_clock_stored_as_utc``  - ``new_joiner_claims.claimed_at`` is 8h too late
* ``all_issuance_times_are_kl_wall_clock_stored_as_utc`` - every issuance source is 8h too late
* ``redemption_time_is_utc_not_kl``              - UIM wall clock was really UTC (redemption 8h too early)

It NEVER writes, changes no rule, and prints counts plus masked samples only. Hypotheses are evidence, not fixes.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta

import affiliate_qualification as aq

SAMPLE_SIZE = 10
DEFAULT_MAX_SCAN_ROWS = 500_000
SHIFT = timedelta(hours=8)
BEFORE_ISSUANCE = "redemption_before_welcome_issuance"
_BUCKETS = ((timedelta(hours=1), "0-1h"), (timedelta(hours=4), "1-4h"), (timedelta(hours=8), "4-8h"),
            (timedelta(hours=12), "8-12h"), (timedelta(hours=24), "12-24h"), (timedelta(days=7), "1-7d"))


def _bucket(gap: timedelta) -> str:
    for limit, label in _BUCKETS:
        if gap <= limit:
            return label
    return ">7d"


def _fold_all(db, config: dict, now_utc: datetime, max_scan_rows: int):
    folded: dict[str, dict] = {}
    codes: dict[str, str] = {}
    scanned = 0
    for _batch_id, row in aq.iter_committed_source_rows(db, config):
        scanned += 1
        if scanned > max_scan_rows:
            return None, None, scanned
        obs = aq.parse_source_row(row, config)
        if obs is None:
            continue
        obs["code"] = aq.normalize_code(obs["code"])
        if not obs["code"]:
            continue
        doc = aq.build_evidence_doc(source=aq.SOURCE_DATABOT_UIM, recipient={}, now_utc=now_utc, **obs)
        codes[doc["source_ref"]] = obs["code"]
        aq.fold_observation(folded, doc, removed=obs.get("observation") == aq.OBS_REMOVED)
    return folded, codes, scanned


def _issuance_times(db, code: str, uid: int) -> list[tuple[datetime, str]]:
    """Per-source issuance times, mirroring ``decide_recipient`` (which keeps only the earliest)."""
    times: list[tuple[datetime, str]] = []

    def add(value, source):
        when = aq._aware_utc(value)
        if when is not None:
            times.append((when, source))

    pool = db.voucher_pools.find_one({"pool_id": aq.WELCOME_POOL_ID, "code": code}) or {}
    add(pool.get("issued_at"), "voucher_pools.issued_at")
    ledger = db.affiliate_ledger.find_one({"dedup_key": f"WELCOME:{uid}"}, {"voucher_code": 1, "issued_at": 1}) or {}
    if ledger.get("voucher_code") == code:
        add(ledger.get("issued_at"), "affiliate_ledger.issued_at")
    for claim in db.new_joiner_claims.find({"code": code}, {"uid": 1, "claimed_at": 1}):
        add(claim.get("claimed_at"), "new_joiner_claims.claimed_at")
    return times


def diagnose(db, *, config: dict | None, now_utc: datetime, max_scan_rows: int = DEFAULT_MAX_SCAN_ROWS) -> dict:
    out = {"ok": True, "read_only": True, "scanned_source_rows": 0, "records_with_redemption_time": 0,
           "issuance_problems": {}, "before_issuance": 0}
    if (config or {}).get("source") != aq.SOURCE_DATABOT_UIM:
        return {**out, "ok": False, "error": "source_is_not_databot_uim"}
    folded, codes, scanned = _fold_all(db, config, now_utc, int(max_scan_rows))
    out["scanned_source_rows"] = scanned
    if folded is None:
        return {**out, "ok": False, "error": "scan_exceeds_max_scan_rows"}

    problems: Counter = Counter()
    gaps: Counter = Counter()
    sources: Counter = Counter()
    clears: Counter = Counter()
    samples = []
    for ref, f in folded.items():
        redeemed = aq._aware_utc(f.get("redeemed_at"))
        if f.get("_source_removed") or redeemed is None:
            continue
        out["records_with_redemption_time"] += 1
        recipient = aq.resolve_welcome_recipient(db, codes[ref])
        problem = aq.welcome_issuance_problem(recipient, redeemed)
        if problem is None:
            continue
        problems[problem] += 1
        if problem != BEFORE_ISSUANCE:
            continue
        times = _issuance_times(db, codes[ref], recipient["user_id"])
        issued = min(t for t, _ in times)
        gap = issued - redeemed
        dominant = sorted({s for t, s in times if t == issued})
        gaps[_bucket(gap)] += 1
        for s in dominant:
            sources[s] += 1
        shifted_claim = min((t - SHIFT if s == "new_joiner_claims.claimed_at" else t) for t, s in times)
        clears["claim_time_is_kl_wall_clock_stored_as_utc"] += redeemed >= shifted_claim
        clears["all_issuance_times_are_kl_wall_clock_stored_as_utc"] += redeemed >= issued - SHIFT
        clears["redemption_time_is_utc_not_kl"] += redeemed + SHIFT >= issued
        if len(samples) < SAMPLE_SIZE:
            samples.append({"code": f.get("code_masked"), "account": f.get("account_key_masked"),
                            "gap_hours": round(gap.total_seconds() / 3600, 2), "earliest_issuance_sources": dominant})
    out["issuance_problems"] = dict(problems)
    out["before_issuance"] = problems[BEFORE_ISSUANCE]
    out["gap_issued_minus_redeemed"] = {label: gaps[label] for _, label in _BUCKETS} | {">7d": gaps[">7d"]}
    out["earliest_issuance_source"] = dict(sources)
    out["would_clear_if"] = {k: int(clears[k]) for k in (
        "claim_time_is_kl_wall_clock_stored_as_utc", "all_issuance_times_are_kl_wall_clock_stored_as_utc",
        "redemption_time_is_utc_not_kl")}
    out["samples"] = samples
    return out
