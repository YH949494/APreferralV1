"""UIM coverage on the APReferral side: what Databot's FINAL reports have covered.

Business contract
-----------------
The seven-day measurement is completed *before* a UIM file reaches Databot, so
a committed final batch is evaluated as soon as it arrives - there is no
``joined_at + 7 days`` and no ``upload_time + 7 days`` anywhere in this flow.
Databot additionally records, per batch, which GMT+8 calendar days the data
owner declared as a COMPLETE final report. A day inside such an interval with
no row is a confirmed zero-result day.

Three facts that must never be merged (the old ``coverage_end`` was the third
one mislabelled as the first):

* final coverage            - declared complete days (this module)
* latest observed redemption - the newest redemption present in synced rows
* sync / snapshot freshness  - ``uim_redemption_sync_state`` / the V2 snapshot

What counts as covered
----------------------
A batch contributes a day only if it is committed upstream AND locally, all of
its rows are synced (``rows_complete``), and its coverage block is
``complete`` + ``operator_declared`` with a recognised basis and dates whose
UTC bounds equal GMT+8 midnights. ``inferred_unverified``, ``partial``,
``voided``, undeclared-legacy and malformed blocks contribute nothing.
Overlapping and adjacent intervals of the SAME basis merge; a day between two
intervals is a gap and is never covered, whatever a later batch says.

Absence-based rejection (the only conclusion drawn from a MISSING row)
----------------------------------------------------------------------
An invitee with a known Welcome issuance and no redemption evidence is
REJECTED only when the data owner has attested, in the control document, that
the dates measure ``voucher_redemption_date`` and how many days a Welcome
voucher stays redeemable (``welcome_redemption_window_days``), AND every GMT+8
day from the issuance day through ``issuance day + window`` lies inside ONE
merged complete interval, AND the evidence source is fully healthy. Everything
else - no policy, unverified/partial/inferred coverage, a gap, a window that
runs past the last covered day, a feed error, an unsynced batch - stays
PENDING. Missing evidence is never converted to zero or to a rejection by
anything else in the system.

Preview and live call the same :func:`decide_absences`.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import affiliate_qualification as aq

logger = logging.getLogger(__name__)

LEDGER_CONTRACT = "databot.uim_coverage.v1"
COVERAGE_TIMEZONE = "Asia/Kuala_Lumpur"
LEDGER_COLLECTION = "uim_redemption_coverage"
LEDGER_HISTORY_COLLECTION = "uim_redemption_coverage_history"
LEDGER_ID = aq.SOURCE_DATABOT_UIM

STATUS_COMPLETE = "complete"
STATUS_INFERRED = "inferred_unverified"
STATUS_PARTIAL = "partial"
STATUS_VOIDED = "voided"
BLOCK_STATUSES = (STATUS_COMPLETE, STATUS_INFERRED, STATUS_PARTIAL, STATUS_VOIDED)
SOURCE_OPERATOR = "operator_declared"
SOURCE_OBSERVED = "observed_rows"

BASIS_REDEMPTION = "voucher_redemption_date"
BASIS_JOIN = "referral_join_date"
BASIS_REPORT = "uim_report_cohort_date"
BASIS_UNVERIFIED = "unverified"
OPERATOR_BASES = (BASIS_REDEMPTION, BASIS_JOIN, BASIS_REPORT)
# Only a redemption-date interval says anything about a MISSING redemption.
ABSENCE_BASES = (BASIS_REDEMPTION,)

# Report-level coverage status (what the dashboard shows).
REPORT_COMPLETE = "complete"
REPORT_INFERRED = STATUS_INFERRED
REPORT_PARTIAL = "partial"
REPORT_GAP = "coverage_gap"
REPORT_UNAVAILABLE = "unavailable"
REPORT_NONE = "none"

ABSENCE_REASON = "no_welcome_redemption_in_complete_coverage"
ABSENCE_SOURCE = "databot_uim_absence"
MAX_WINDOW_DAYS = 366
_CHUNK = 2000


# ---------------------------------------------------------------------------
# Parsing (Databot -> local). Strict: anything malformed is "no coverage".
# ---------------------------------------------------------------------------

def _day(value) -> date | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def kl_midnight_utc(day: date) -> datetime:
    return aq.KL_TZ.localize(datetime(day.year, day.month, day.day)).astimezone(timezone.utc)


def day_bounds_utc(start: date, end: date) -> tuple[datetime, datetime]:
    """Inclusive GMT+8 dates -> half-open UTC ``[start, end_exclusive)``."""
    return kl_midnight_utc(start), kl_midnight_utc(end + timedelta(days=1))


def _instant(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return aq._aware_utc(parsed) if parsed.tzinfo else None


def parse_batch_coverage(raw) -> dict | None:
    """One batch's ``coverage`` block from Databot's batch listing -> the
    whitelisted form stored on ``uim_redemption_batches.coverage`` (None when
    absent or malformed). A ``complete`` block must be internally consistent:
    its UTC bounds have to equal the GMT+8 midnights of its inclusive dates,
    so an off-by-one-day or timezone drift on either side is caught here."""
    if not isinstance(raw, dict):
        return None
    status, source, basis = raw.get("status"), raw.get("source"), raw.get("basis")
    if status not in BLOCK_STATUSES or source not in (SOURCE_OPERATOR, SOURCE_OBSERVED):
        return None
    if basis not in (*OPERATOR_BASES, BASIS_UNVERIFIED):
        return None
    if raw.get("timezone") not in (None, COVERAGE_TIMEZONE):
        return None
    revision = raw.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        return None
    start, end = _day(raw.get("start_date")), _day(raw.get("end_date"))
    if (start is None) != (end is None) or (start and end and start > end):
        return None
    if status == STATUS_COMPLETE:
        if source != SOURCE_OPERATOR or basis not in OPERATOR_BASES or start is None:
            return None
        lo, hi = day_bounds_utc(start, end)
        if _instant(raw.get("start_utc")) != lo or _instant(raw.get("end_utc_exclusive")) != hi:
            return None
    observed = _instant(raw.get("latest_observed_redemption_at"))
    return {
        "revision": revision, "status": status, "source": source, "basis": basis,
        "start_date": start.isoformat() if start else None, "end_date": end.isoformat() if end else None,
        "latest_observed_redemption_at": observed.isoformat() if observed else None,
        "fingerprint": raw.get("fingerprint") if isinstance(raw.get("fingerprint"), str) else None,
        "undeclared_legacy": raw.get("undeclared_legacy") is True,
    }


def merge_days(items: list[tuple[date, date, str]]) -> tuple[list[dict], list[dict]]:
    """Merge inclusive day intervals (overlapping AND adjacent); list the gaps."""
    merged: list[dict] = []
    for start, end, batch_id in sorted(items):
        if merged and start <= merged[-1]["end"] + timedelta(days=1):
            top = merged[-1]
            top["end"] = max(top["end"], end)
            top["batches"].append(batch_id)
        else:
            merged.append({"start": start, "end": end, "batches": [batch_id]})
    gaps = [{"start": merged[i]["end"] + timedelta(days=1), "end": merged[i + 1]["start"] - timedelta(days=1)}
            for i in range(len(merged) - 1)]
    return merged, gaps


def _public_ranges(items: list[dict]) -> list[dict]:
    return [{"start_date": i["start"].isoformat(), "end_date": i["end"].isoformat()} for i in items]


# ---------------------------------------------------------------------------
# Effective coverage (from the locally persisted per-batch blocks)
# ---------------------------------------------------------------------------

def _wallclock_to_instant(text) -> datetime | None:
    """``coverage_max_redeemed_at`` is GMT+8 wall-clock text (YYYY-MM-DDTHH:MM:SS)."""
    return aq.parse_redeemed_at(text, COVERAGE_TIMEZONE) if text else None


def effective_coverage(db, *, now_utc: datetime | None = None) -> dict:
    """Final coverage, per basis, from local batch documents; plus the stored
    Databot ledger cross-check. Read-only and bounded by the number of batches."""
    import uim_redemption_sync as sync  # noqa: PLC0415 - sync imports this module

    batches = list(db[sync.BATCHES_COLLECTION].find(
        {}, {"status": 1, "upstream_status": 1, "rows_complete": 1, "coverage": 1, "rolled_back_at": 1,
             "coverage_max_redeemed_at": 1, "coverage_min_redeemed_at": 1}))
    excluded = defaultdict(int)
    contributing: dict[str, list[tuple[date, date, str]]] = defaultdict(list)
    all_complete: dict[str, list[tuple[date, date, str]]] = defaultdict(list)    # ignores local row completeness
    latest = None
    committed = 0
    for b in batches:
        if b.get("status") != "committed" or b.get("upstream_status") != "committed" or b.get("rolled_back_at"):
            continue
        committed += 1
        observed = _wallclock_to_instant(b.get("coverage_max_redeemed_at"))
        if observed and (latest is None or observed > latest):
            latest = observed
        block = b.get("coverage")
        if not isinstance(block, dict):
            excluded["no_coverage_block"] += 1
            continue
        status = block.get("status")
        if status != STATUS_COMPLETE:
            excluded[status if status in BLOCK_STATUSES else "invalid"] += 1
            continue
        start, end = _day(block.get("start_date")), _day(block.get("end_date"))
        if start is None or end is None or block.get("basis") not in OPERATOR_BASES:
            excluded["invalid"] += 1
            continue
        all_complete[block["basis"]].append((start, end, b["_id"]))
        if b.get("rows_complete") is True:
            contributing[block["basis"]].append((start, end, b["_id"]))
        else:
            excluded["rows_not_fully_synced"] += 1
    bases, gaps_any = {}, False
    for basis, items in sorted(contributing.items()):
        merged, gaps = merge_days(items)
        gaps_any = gaps_any or bool(gaps)
        bases[basis] = {"intervals": merged, "gaps": gaps}
    ledger = ledger_state(db, all_complete)
    partialish = bool(excluded.get("partial") or excluded.get("rows_not_fully_synced") or excluded.get("invalid"))
    if not committed:
        status = REPORT_NONE
    elif gaps_any:
        status = REPORT_GAP
    elif partialish:
        status = REPORT_PARTIAL
    elif bases:
        status = REPORT_COMPLETE
    elif excluded.get(STATUS_INFERRED) or excluded.get("no_coverage_block"):
        status = REPORT_INFERRED
    else:
        status = REPORT_UNAVAILABLE
    primary = bases.get(BASIS_REDEMPTION) or (next(iter(bases.values())) if bases else None)
    primary_basis = BASIS_REDEMPTION if BASIS_REDEMPTION in bases else (next(iter(bases)) if bases else None)
    fp_material = json.dumps({b: [[i["start"].isoformat(), i["end"].isoformat()] for i in v["intervals"]]
                              for b, v in bases.items()}, sort_keys=True)
    return {
        "status": status,
        "bases": bases,
        "primary_basis": primary_basis,
        "final_start": primary["intervals"][0]["start"] if primary else None,
        "final_end": primary["intervals"][-1]["end"] if primary else None,
        "committed_batches": committed,
        "excluded": dict(excluded),
        "latest_observed_redemption_at": latest,
        "ledger": ledger,
        "fingerprint": hashlib.sha256(fp_material.encode()).hexdigest()[:20],
    }


def covered(cov: dict, basis: str, first: date, last: date) -> str:
    """``covered`` when every day of [first, last] is inside ONE merged complete
    interval of ``basis``; else why not: ``no_complete_coverage`` |
    ``coverage_gap`` | ``outside_coverage``."""
    block = cov["bases"].get(basis)
    if not block or not block["intervals"]:
        return "no_complete_coverage"
    for interval in block["intervals"]:
        if interval["start"] <= first and last <= interval["end"]:
            return "covered"
    for gap in block["gaps"]:
        if not (last < gap["start"] or first > gap["end"]):
            return "coverage_gap"
    return "outside_coverage"


# ---------------------------------------------------------------------------
# Databot's ledger (persisted idempotently, cross-checked, never trusted alone)
# ---------------------------------------------------------------------------

def parse_ledger(raw) -> dict | None:
    """Databot ``/coverage`` body -> {basis: [(start, end)], ...} + metadata."""
    if not isinstance(raw, dict) or raw.get("contract") != LEDGER_CONTRACT or raw.get("timezone") != COVERAGE_TIMEZONE:
        return None
    bases = {}
    for basis, block in (raw.get("bases") or {}).items():
        if basis not in OPERATOR_BASES or not isinstance(block, dict):
            return None
        intervals = []
        for item in block.get("intervals") or []:
            start, end = _day((item or {}).get("start_date")), _day((item or {}).get("end_date"))
            if start is None or end is None or start > end:
                return None
            lo, hi = day_bounds_utc(start, end)
            if _instant(item.get("start_utc")) != lo or _instant(item.get("end_utc_exclusive")) != hi:
                return None
            intervals.append([start.isoformat(), end.isoformat()])
        bases[basis] = intervals
    generated = _instant(raw.get("generated_at"))
    if generated is None:
        return None
    observed = _instant(raw.get("latest_observed_redemption_at"))
    return {
        "bases": bases, "generated_at": generated,
        "latest_observed_redemption_at": observed,
        "partial_batches": len(raw.get("partial_batch_ids") or []),
        "inferred_batches": len(raw.get("inferred_batch_ids") or []),
        "databot_fingerprint": raw.get("ledger_fingerprint") if isinstance(raw.get("ledger_fingerprint"), str) else None,
    }


def store_ledger(db, raw, *, now_utc: datetime) -> dict:
    """Persist Databot's authoritative ledger. Idempotent: an unchanged ledger
    only refreshes ``fetched_at``; a changed one is appended (audit) before the
    current document moves. A malformed body is recorded as unusable, and the
    previous valid ledger is NOT kept as authority (fail closed)."""
    parsed = parse_ledger(raw)
    if parsed is None:
        db[LEDGER_COLLECTION].update_one(
            {"_id": LEDGER_ID}, {"$set": {"fetched_at": now_utc, "valid": False, "ledger": None, "fingerprint": None}},
            upsert=True)
        return {"valid": False}
    fingerprint = hashlib.sha256(json.dumps(parsed["bases"], sort_keys=True).encode()).hexdigest()[:20]
    previous = db[LEDGER_COLLECTION].find_one({"_id": LEDGER_ID}) or {}
    if previous.get("fingerprint") != fingerprint:
        try:
            db[LEDGER_HISTORY_COLLECTION].insert_one({
                "_id": f"{fingerprint}:{uuid.uuid4().hex[:8]}", "fingerprint": fingerprint, "ledger": parsed["bases"],
                "databot_generated_at": parsed["generated_at"], "recorded_at": now_utc,
                "previous_fingerprint": previous.get("fingerprint")})
        except Exception:                                   # audit trail must not block the sync
            logger.warning("[UIM_COVERAGE] could not append ledger history", exc_info=True)
    db[LEDGER_COLLECTION].update_one(
        {"_id": LEDGER_ID},
        {"$set": {"fetched_at": now_utc, "valid": True, "ledger": parsed["bases"], "fingerprint": fingerprint,
                  "databot_generated_at": parsed["generated_at"],
                  "latest_observed_redemption_at": parsed["latest_observed_redemption_at"],
                  "databot_fingerprint": parsed["databot_fingerprint"],
                  "partial_batches": parsed["partial_batches"], "inferred_batches": parsed["inferred_batches"]}},
        upsert=True)
    return {"valid": True, "changed": previous.get("fingerprint") != fingerprint, "fingerprint": fingerprint}


def invalidate_ledger(db, *, now_utc: datetime, error: str) -> None:
    """The ledger could not be refreshed this pass: mark the stored one unusable (fail closed)."""
    db[LEDGER_COLLECTION].update_one(
        {"_id": LEDGER_ID},
        {"$set": {"fetched_at": now_utc, "valid": False, "ledger": None, "fingerprint": None, "last_error": error}},
        upsert=True)


def ledger_state(db, local_all_complete: dict[str, list[tuple[date, date, str]]]) -> dict:
    """Does Databot's stored ledger agree with the per-batch blocks we hold?
    Compared on EVERY complete batch regardless of local row completeness (the
    ledger knows nothing about our sync). A disagreement - or no ledger - means
    absence conclusions are suspended until the next sync reconciles them."""
    doc = db[LEDGER_COLLECTION].find_one({"_id": LEDGER_ID}) or {}
    fetched = aq._aware_utc(doc.get("fetched_at"))
    local = {b: [[i["start"].isoformat(), i["end"].isoformat()] for i in merge_days(items)[0]]
             for b, items in local_all_complete.items() if items}
    if not doc or not doc.get("valid"):
        return {"present": bool(doc), "valid": False, "matches": None, "fetched_at": fetched}
    remote = {b: v for b, v in (doc.get("ledger") or {}).items() if v}
    return {"present": True, "valid": True, "matches": remote == local, "fetched_at": fetched,
            "fingerprint": doc.get("fingerprint")}


# ---------------------------------------------------------------------------
# Absence policy + decision
# ---------------------------------------------------------------------------

def absence_policy(control: dict | None) -> tuple[dict | None, list[str]]:
    """The data owner's attested policy in ``source_config.absence_rejection``."""
    cfg = ((control or {}).get("source_config") or {}).get("absence_rejection")
    if not isinstance(cfg, dict):
        return None, ["absence_policy_not_configured"]
    problems = []
    if cfg.get("enabled") is not True:
        problems.append("absence_policy_disabled")
    if cfg.get("coverage_basis") not in ABSENCE_BASES:
        problems.append("absence_basis_unsupported")
    window = cfg.get("welcome_redemption_window_days")
    if isinstance(window, bool) or not isinstance(window, int) or not 1 <= window <= MAX_WINDOW_DAYS:
        problems.append("absence_window_invalid")
    attestation = cfg.get("attestation") if isinstance(cfg.get("attestation"), dict) else {}
    if not (attestation.get("by") and attestation.get("reference")):
        problems.append("coverage_basis_not_attested")
    return (None, problems) if problems else (cfg, [])


def absence_blockers(control: dict | None, cov: dict) -> list[str]:
    """Why NO absence-based conclusion may be drawn right now (empty = may)."""
    policy, blockers = absence_policy(control)
    if policy is None:
        return blockers
    block = cov["bases"].get(policy["coverage_basis"])
    if not block or not block["intervals"]:
        blockers.append("no_complete_coverage")
    ledger = cov["ledger"]
    if not ledger.get("valid"):
        blockers.append("coverage_ledger_missing")
    elif ledger.get("matches") is not True:
        blockers.append("coverage_ledger_mismatch")
    return blockers


def policy_fingerprint(control: dict | None, cov: dict) -> str:
    policy = (((control or {}).get("source_config") or {}).get("absence_rejection")) or {}
    material = json.dumps([cov["fingerprint"], policy.get("coverage_basis"),
                           policy.get("welcome_redemption_window_days"), policy.get("enabled")], sort_keys=True)
    return hashlib.sha256(material.encode()).hexdigest()[:16]


def assess_absence(*, issuance: dict, evidence_present: bool, policy: dict | None, cov: dict,
                   source_ok: bool) -> dict:
    """Pure verdict for ONE invitee with no redemption evidence.

    ``rejected`` only when the invitee's whole redemption window is inside one
    merged complete interval; otherwise ``pending`` with the reason.
    """
    def pending(reason, **extra):
        return {"status": "pending", "reason": reason, **extra}

    if not source_ok:
        return pending("source_not_ready")
    if policy is None:
        return pending("absence_policy_not_configured")
    if evidence_present:
        return pending("redemption_evidence_present")
    if issuance.get("status") != "ok" or aq._aware_utc(issuance.get("issued_at")) is None:
        return pending(issuance.get("reason") or "welcome_issuance_unknown")
    first = aq._aware_utc(issuance["issued_at"]).astimezone(aq.KL_TZ).date()
    last = first + timedelta(days=int(policy["welcome_redemption_window_days"]))
    state = covered(cov, policy["coverage_basis"], first, last)
    window = {"window_start": first.isoformat(), "window_end": last.isoformat()}
    if state != "covered":
        return pending(state, **window)
    return {"status": "rejected", "reason": ABSENCE_REASON, **window}


# ---------------------------------------------------------------------------
# Bulk resolution shared by the preview and the live path
# ---------------------------------------------------------------------------

def _chunks(items, size=_CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def welcome_issuances(db, invitees) -> dict[int, dict]:
    """invitee -> {"status": "ok", "issued_at", "code_hash"} or
    {"status": "unknown", "reason"}. One canonical Welcome code per invitee
    (``affiliate_ledger`` ``WELCOME:<uid>``), proven by the same recipient
    decision the validator uses; anything ambiguous is "unknown" (pending)."""
    import affiliate_qualification_preview as aqp  # noqa: PLC0415 - avoids an import cycle

    ids = sorted({int(i) for i in invitees})
    ledger = {}
    for chunk in _chunks(f"WELCOME:{i}" for i in ids):
        for row in db.affiliate_ledger.find({"dedup_key": {"$in": chunk}}, {"dedup_key": 1, "voucher_code": 1}):
            ledger[row["dedup_key"]] = row
    codes = {aq.normalize_code(r.get("voucher_code")) for r in ledger.values()} - {None}
    recipients = aqp._resolve_recipients(db, codes) if codes else {}
    out = {}
    for invitee in ids:
        row = ledger.get(f"WELCOME:{invitee}")
        code = aq.normalize_code((row or {}).get("voucher_code"))
        if not code:
            out[invitee] = {"status": "unknown", "reason": "no_welcome_issuance_record"}
            continue
        decision = recipients.get(code) or {}
        if decision.get("status") != "ok" or decision.get("user_id") != invitee:
            out[invitee] = {"status": "unknown", "reason": "welcome_recipient_unproven"}
        elif aq._aware_utc(decision.get("issued_at")) is None:
            out[invitee] = {"status": "unknown", "reason": "missing_welcome_issued_at"}
        else:
            out[invitee] = {"status": "ok", "issued_at": decision["issued_at"], "code_hash": aq.code_hash(code)}
    return out


def code_hashes_with_evidence(db, hashes, *, in_memory: set[str] | None = None) -> set[str]:
    """Hashes that have ANY non-voided redemption evidence (stored, or in the
    preview's in-memory replay). A held, rejected or removed observation is
    still evidence: absence is only ever "no row at all"."""
    seen = set(in_memory or ()) & set(hashes)
    pending = [h for h in hashes if h not in seen]
    for chunk in _chunks(pending):
        for row in db[aq.EVIDENCE_COLLECTION].find({"code_hash": {"$in": chunk}, "status": {"$ne": aq.EV_VOIDED}},
                                                   {"code_hash": 1}):
            seen.add(row["code_hash"])
    return seen


def decide_absences(db, invitees, *, control: dict, cov: dict, source_ok: bool,
                    evidence_hashes: set[str] | None = None) -> dict[int, dict]:
    """Verdict per invitee. THE shared decision for preview and live."""
    invitees = sorted({int(i) for i in invitees})
    policy, _ = absence_policy(control)
    ok = bool(source_ok and policy is not None and not absence_blockers(control, cov))
    if not invitees:
        return {}
    issuances = welcome_issuances(db, invitees) if ok else {i: {"status": "unknown", "reason": "not_evaluated"} for i in invitees}
    hashes = [v["code_hash"] for v in issuances.values() if v.get("status") == "ok"]
    seen = code_hashes_with_evidence(db, hashes, in_memory=evidence_hashes) if hashes else set()
    out = {}
    for invitee in invitees:
        issuance = issuances[invitee]
        out[invitee] = assess_absence(issuance=issuance, evidence_present=issuance.get("code_hash") in seen,
                                      policy=policy if ok else None, cov=cov, source_ok=source_ok)
        if not ok and source_ok:
            blockers = absence_blockers(control, cov)
            out[invitee]["reason"] = blockers[0] if blockers else out[invitee]["reason"]
    return out


# ---------------------------------------------------------------------------
# Live effect: one terminal decision per invitee, idempotent
# ---------------------------------------------------------------------------

def _decision_doc(invitee: int, verdict: dict, cov: dict, policy: dict, *, now_utc: datetime) -> dict:
    return {
        "source": ABSENCE_SOURCE,
        "source_ref": hashlib.sha256(f"{aq.RULE_VERSION}|absence|{invitee}".encode()).hexdigest()[:40],
        "rule_version": aq.RULE_VERSION,
        "qualification_gate_version": aq.QUALIFICATION_GATE_VERSION,
        "status": aq.EV_REJECTED, "reason": ABSENCE_REASON,
        "invitee_id": invitee,
        "recipient_resolution": {"status": "ok", "user_id": invitee, "sources": ["affiliate_ledger"]},
        "absence": {
            "coverage_basis": policy["coverage_basis"], "window_start": verdict["window_start"],
            "window_end": verdict["window_end"], "window_days": policy["welcome_redemption_window_days"],
            "coverage_fingerprint": cov["fingerprint"],
            "attested_by": (policy.get("attestation") or {}).get("by"),
            "attestation_ref": (policy.get("attestation") or {}).get("reference"),
        },
        "created_at": now_utc, "processed_at": now_utc, "updated_at": now_utc,
    }


def _park_rejected(db, invitee: int, now_utc: datetime) -> None:
    db.pending_referrals.update_many(
        {"invitee_user_id": invitee, "status": {"$in": list(aq.ACTIVE_PENDING_STATUSES)}},
        {"$set": {"status": "rejected", "qualification_rule_version": aq.RULE_VERSION,
                  "qualification_outcome": aq.EV_REJECTED, "qualification_reason": ABSENCE_REASON,
                  "qualification_checked_at": now_utc}})


ABSENCE_BATCH = 5000                         # referrals examined per scheduler tick
ABSENCE_RECHECK = timedelta(hours=6)         # safety re-scan even if nothing changed (re-opened referrals ...)
PASS_STATE_ID = "absence_pass"


def process_absence_rejections(db, *, control: dict, now_utc: datetime, limit: int = ABSENCE_BATCH) -> dict:
    """Live pass (called from process_pending_evidence once the source is
    healthy and extraction is complete). An active referral is re-examined only
    when the coverage/policy fingerprint changed since it was last checked, so a
    steady state costs one small read per tick; a changed fingerprint is worked
    off ``limit`` referrals at a time."""
    out = {"checked": 0, "rejected": 0, "pending": {}}
    policy, _ = absence_policy(control)
    cov = effective_coverage(db, now_utc=now_utc)
    blockers = absence_blockers(control, cov)
    if policy is None or blockers:
        out["skipped"] = blockers[0] if blockers else "absence_policy_not_configured"
        return out
    # "No evidence for this code" is only meaningful once EVERY committed row has been turned into evidence.
    config = (control or {}).get("source_config")
    if db[aq.source_spec(config)["batches"]].find_one(
            {**aq.committed_batch_filter(config),
             "$or": [{"welcome_v2_evidence_rule_version": {"$ne": aq.RULE_VERSION}},
                     {"welcome_v2_evidence_extracted_at": {"$exists": False}}]}, {"_id": 1}):
        out["skipped"] = "source_extraction_incomplete"
        return out
    fp = policy_fingerprint(control, cov)
    state = db[LEDGER_COLLECTION].find_one({"_id": PASS_STATE_ID}) or {}
    done_at = aq._aware_utc(state.get("completed_at"))
    if state.get("fp") == fp and state.get("complete") and done_at and now_utc - done_at < ABSENCE_RECHECK:
        out["skipped"] = "unchanged"
        return out
    rows = list(db.pending_referrals.find(
        {"status": {"$in": list(aq.ACTIVE_PENDING_STATUSES)}, "absence_check_fp": {"$ne": fp}},
        {"invitee_user_id": 1}).limit(int(limit)))
    invitees = []
    for row in rows:
        try:
            invitees.append(int(row["invitee_user_id"]))
        except (KeyError, TypeError, ValueError):
            continue
    qualified = {int(q["invitee_id"]) for chunk in _chunks(invitees)
                 for q in db.qualified_events.find({"invitee_id": {"$in": chunk}}, {"invitee_id": 1})}
    verdicts = decide_absences(db, [i for i in invitees if i not in qualified], control=control, cov=cov, source_ok=True)
    from pymongo.errors import DuplicateKeyError  # noqa: PLC0415

    keep_pending = []
    for invitee, verdict in verdicts.items():
        out["checked"] += 1
        if verdict["status"] == "rejected":
            try:
                db[aq.EVIDENCE_COLLECTION].insert_one(_decision_doc(invitee, verdict, cov, policy, now_utc=now_utc))
                out["rejected"] += 1
            except DuplicateKeyError:
                pass                                    # decided before; (re)apply the idempotent parking below
            _park_rejected(db, invitee, now_utc)
        else:
            out["pending"][verdict["reason"]] = out["pending"].get(verdict["reason"], 0) + 1
            keep_pending.append(invitee)
    # Mark everyone examined (pending and already-qualified alike) with the fingerprint in bulk.
    for chunk in _chunks(keep_pending + sorted(qualified & set(invitees))):
        db.pending_referrals.update_many(
            {"invitee_user_id": {"$in": chunk}, "status": {"$in": list(aq.ACTIVE_PENDING_STATUSES)}},
            {"$set": {"absence_check_fp": fp, "absence_checked_at": now_utc}})
    more = len(rows) >= int(limit)
    db[LEDGER_COLLECTION].update_one(
        {"_id": PASS_STATE_ID},
        {"$set": {"fp": fp, "complete": not more, "updated_at": now_utc,
                  **({"completed_at": now_utc} if not more else {})}}, upsert=True)
    out["more"] = more
    return out


# ---------------------------------------------------------------------------
# Reporting view (what the V2 snapshot publishes)
# ---------------------------------------------------------------------------

def public_summary(cov: dict) -> dict:
    """JSON-safe coverage facts (no batch ids needed by consumers, no rows)."""
    primary = cov["bases"].get(cov["primary_basis"]) if cov["primary_basis"] else None
    latest = cov["latest_observed_redemption_at"]
    ledger = cov["ledger"]
    return {
        "status": cov["status"],
        "basis": (cov["primary_basis"] or BASIS_UNVERIFIED) if cov["status"] != REPORT_NONE else None,
        "timezone": COVERAGE_TIMEZONE,
        "final_coverage_start": cov["final_start"].isoformat() if cov["final_start"] else None,
        "final_coverage_end": cov["final_end"].isoformat() if cov["final_end"] else None,
        "intervals": _public_ranges(primary["intervals"]) if primary else [],
        "gaps": _public_ranges(primary["gaps"]) if primary else [],
        "latest_observed_redemption_at": latest.astimezone(aq.KL_TZ).isoformat() if latest else None,
        "excluded_batches": dict(cov["excluded"]),
        "ledger": {"present": ledger.get("present"), "valid": ledger.get("valid"), "matches": ledger.get("matches"),
                   "fetched_at": ledger["fetched_at"].isoformat() if ledger.get("fetched_at") else None},
    }


def report_view(db, control: dict | None, *, now_utc: datetime) -> dict:
    """The ``uim_coverage`` object of the reporting API: final coverage, latest
    observed redemption and the absence-rejection readiness - kept apart."""
    cov = effective_coverage(db, now_utc=now_utc)
    blockers = absence_blockers(control, cov)
    summary = public_summary(cov)
    summary.pop("excluded_batches")
    summary.pop("ledger")
    return {**summary, "absence_rejection": {"active": not blockers, "blockers": blockers}}


# ---------------------------------------------------------------------------
# Activation readiness (read-only; consumed by the admin preflight)
# ---------------------------------------------------------------------------

def preflight_section(db, control: dict | None, *, now_utc: datetime) -> dict:
    """Coverage gates of ``preflight``: declared complete intervals persisted,
    no gap inside the launch cohort, ledger agreement, and an attested absence
    policy (the zero-row-date rule). The marketing source has no UIM coverage
    contract, so it is not applicable there."""
    config = (control or {}).get("source_config") or {}
    if config.get("source") != aq.SOURCE_DATABOT_UIM:
        return {"applicable": False, "ok": True, "blockers": []}
    cov = effective_coverage(db, now_utc=now_utc)
    summary = public_summary(cov)
    blockers = []
    if cov["status"] in (REPORT_NONE, REPORT_UNAVAILABLE, REPORT_INFERRED):
        blockers.append(f"coverage_{cov['status']}")
    elif cov["status"] == REPORT_PARTIAL:
        blockers.append("coverage_partial_batches")
    for item in absence_blockers(control, cov):
        if item not in blockers:
            blockers.append(item)
    gaps_in_cohort = []
    first_join = next(iter(db.pending_referrals.find(
        {"status": {"$in": list(aq.ACTIVE_PENDING_STATUSES)}}, {"created_at_utc": 1}).sort("created_at_utc", 1).limit(1)), None)
    earliest = aq._aware_utc((first_join or {}).get("created_at_utc"))
    primary = cov["bases"].get(cov["primary_basis"]) if cov["primary_basis"] else None
    if primary and earliest:
        start_day = earliest.astimezone(aq.KL_TZ).date()
        for gap in primary["gaps"]:
            if gap["end"] >= start_day:
                gaps_in_cohort.append({"start_date": gap["start"].isoformat(), "end_date": gap["end"].isoformat()})
    if gaps_in_cohort:
        blockers.append("coverage_gaps_affect_launch_cohort")
    policy, policy_problems = absence_policy(control)
    return {
        "applicable": True, "ok": not blockers, "blockers": blockers, **summary,
        "gaps_affecting_cohort": gaps_in_cohort,
        "absence_policy": {"configured": policy is not None, "problems": policy_problems,
                           "basis": (policy or {}).get("coverage_basis"),
                           "welcome_redemption_window_days": (policy or {}).get("welcome_redemption_window_days"),
                           "attested_by": ((policy or {}).get("attestation") or {}).get("by"),
                           "attestation_ref": ((policy or {}).get("attestation") or {}).get("reference")},
    }
