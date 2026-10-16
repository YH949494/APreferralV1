"""HYPOTHETICAL RETROSPECTIVE audit: would pending affiliate reward requests
blocked by ``bundle_denomination_short`` still reach their claimed tier if
their qualified invitees were judged by ``welcome_redemption_v2``?

STRICTLY READ-ONLY. Only ``find`` / ``count_documents`` are issued; nothing
here creates, updates or deletes a document, and nothing here can change
eligibility, qualifications, reward claims, ledgers, XP, referrals, registry
ownership or voucher inventory. The result is a projection, never a decision.

Scope and bounds (this is an operator CLI tool, never an HTTP request path):

* pending requests: ``affiliate_ledger`` ``status + risk_flags`` (index
  ``affiliate_status_created``) narrowed to one month, ``--max-requests`` cap.
* per affiliate: exact ``count_documents`` + at most ``--max-events-per-affiliate``
  ``qualified_events`` via ``qualified_by_referrer_time``. Events beyond the cap
  are NOT classified; they are counted ``unknown_or_incomplete``.
* evidence: ``welcome_redemption_evidence`` by ``_id`` (V2 qualifications) or by
  ``evidence_code_hash`` after resolving the invitee's WELCOME ledger
  (``uniq_affiliate_dedup``) for legacy qualifications.
* account uniqueness: ``affiliate_account_registry.uniq_account_registry_key`` and
  ``qualified_events.uniq_qualified_account_key_v1``.
  All ``$in`` lookups are chunked.

Evidence rules reuse the live validator's own gate function
(:func:`affiliate_qualification.assess_qualification_fields`) so the 80 /
``coupon_new_player`` semantics cannot drift. Missing, stale, partial,
conflicting or unavailable evidence is ALWAYS ``unknown_or_incomplete`` and is
never counted as a failure: a request is only reported ineligible when even
passes + unknowns cannot reach the claimed tier.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone

import affiliate_qualification as aq
from affiliate_qualification_preview import month_window
from affiliate_reward_plans import (
    DENOMINATION_POOL_VALUES,
    normalize_month,
    normalize_tier,
    tier_recipe,
    tier_thresholds,
)

AUDIT_VERSION = "affiliate_reward_v2_audit_v1"
LABEL = (
    "HYPOTHETICAL RETROSPECTIVE AUDIT - read-only projection of welcome_redemption_v2 "
    "over existing committed evidence. It does not modify eligibility, qualifications, "
    "reward claims, ledgers, XP, referrals, registry ownership or voucher inventory."
)

PENDING_MANUAL = "PENDING_MANUAL"
SHORTAGE_FLAG = "bundle_denomination_short"
TIERS = ("T1", "T2", "T3", "T4", "T5")

PASS = "would_pass_v2"
FAIL_NEW_PLAYER = "would_fail_new_player"
FAIL_TURNOVER = "would_fail_turnover"
FAIL_DUPLICATE = "would_fail_duplicate_account"
UNKNOWN = "unknown_or_incomplete"
CLASSES = (PASS, FAIL_NEW_PLAYER, FAIL_TURNOVER, FAIL_DUPLICATE, UNKNOWN)

ELIGIBLE = "eligible_under_v2"
INELIGIBLE = "not_eligible_under_v2"
INDETERMINATE = "indeterminate"

DEFAULT_MAX_REQUESTS = 200
DEFAULT_MAX_EVENTS = 500
DEFAULT_SAMPLE_SIZE = 3
_CHUNK = 500

_EVIDENCE_PROJECTION = {
    "rule_version": 1, "qualification_gate_version": 1, "voided_at": 1, "source_conflict_at": 1,
    "source_conflict_reason": 1, "redeemed_at": 1, "redemption_successful": 1,
    "redemption_status_known": 1, "account_key": 1, "account_reason": 1,
    "qualification_fields": 1, "recipient_resolution": 1, "source": 1, "code_hash": 1,
    "observation_fingerprint": 1,
}


# --------------------------------------------------------------------------
# Pure classification
# --------------------------------------------------------------------------

def tier_for_count(count: int, thresholds: dict | None = None) -> str | None:
    thresholds = thresholds or tier_thresholds()
    best = None
    for tier in TIERS:
        if int(count) >= int(thresholds[tier]):
            best = tier
    return best


def classify_evidence(evidence: dict | None, invitee_id: int) -> dict:
    """Verdict on ONE invitee from ONE evidence row, before account
    uniqueness. ``{"cls": PASS|FAIL_*|UNKNOWN, "reason": str|None}``; a PASS
    here still has to survive the canonical Account ID check."""
    def out(cls, reason=None):
        return {"cls": cls, "reason": reason}

    if not evidence:
        return out(UNKNOWN, "no_committed_evidence")
    if evidence.get("voided_at"):
        return out(UNKNOWN, "evidence_voided")
    if evidence.get("source_conflict_at"):
        return out(UNKNOWN, evidence.get("source_conflict_reason") or "conflicting_source_observations")
    if (evidence.get("rule_version") != aq.RULE_VERSION
            or evidence.get("qualification_gate_version") != aq.QUALIFICATION_GATE_VERSION):
        return out(UNKNOWN, "evidence_rule_version_mismatch")
    recipient = evidence.get("recipient_resolution") or {}
    if recipient.get("status") != "ok":
        return out(UNKNOWN, "welcome_recipient_unproven")
    try:
        if int(recipient.get("user_id")) != int(invitee_id):
            return out(UNKNOWN, "evidence_belongs_to_another_user")
    except (TypeError, ValueError):
        return out(UNKNOWN, "welcome_recipient_unproven")
    if aq._aware_utc(evidence.get("redeemed_at")) is None:
        return out(UNKNOWN, evidence.get("redeemed_at_reason") or "missing_redeemed_at")
    if not evidence.get("redemption_successful"):
        # A recorded non-success is not a V2 verdict this audit may impose.
        return out(UNKNOWN, "redemption_status_unknown" if evidence.get("redemption_status_known") is not True
                   else "redemption_not_successful_in_evidence")

    gates = aq.assess_qualification_fields(
        evidence.get("qualification_fields"),
        require_transport_version=evidence.get("source") == aq.SOURCE_DATABOT_UIM,
    )
    if gates["status"] == aq.EV_REJECTED:
        if gates["reason"] == "not_a_new_coupon_player":
            return out(FAIL_NEW_PLAYER, gates["reason"])
        if gates["reason"] == "seven_day_turnover_below_80":
            return out(FAIL_TURNOVER, gates["reason"])
        return out(UNKNOWN, gates["reason"])
    if gates["status"] != aq.ELIGIBLE:
        return out(UNKNOWN, gates["reason"])
    if not evidence.get("account_key"):
        return out(UNKNOWN, evidence.get("account_reason") or "missing_account")
    return out(PASS, None)


def _evidence_signature(evidence: dict) -> tuple:
    fields = evidence.get("qualification_fields") or {}
    return (evidence.get("account_key"), str(fields.get("coupon_new_player")),
            str(fields.get("after_total_bet_amount")), str(fields.get("seven_day_complete")))


def _pick_evidence(candidates: list[dict]) -> tuple[dict | None, str | None]:
    """One evidence row per invitee, or ``(None, reason)`` when the invitee's
    rows disagree (never resolved by guessing)."""
    live = [e for e in candidates if not e.get("voided_at")]
    if not live:
        return (candidates[0] if candidates else None), None
    if len({_evidence_signature(e) for e in live}) > 1:
        return None, "conflicting_evidence_rows"
    return live[0], None


def _recipe_counts(ledger: dict) -> dict:
    recipe = ledger.get("bundle_recipe")
    if not (isinstance(recipe, dict) and recipe.get("components")):
        recipe = tier_recipe(normalize_month(ledger.get("entitlement_month") or ledger.get("year_month")),
                             ledger.get("tier"))
    counts: dict = {"5": 0, "10": 0, "50": 0, "other": 0}
    for comp in (recipe or {}).get("components") or []:
        value = DENOMINATION_POOL_VALUES.get(str(comp.get("pool_id") or "").strip().upper())
        qty = int(comp.get("quantity") or 0)
        key = str(value) if str(value) in counts else "other"
        counts[key] += qty
    return counts


def _usd(counts: dict) -> int:
    return 5 * counts["5"] + 10 * counts["10"] + 50 * counts["50"]


# --------------------------------------------------------------------------
# Bounded reads
# --------------------------------------------------------------------------

def _chunks(values, size=_CHUNK):
    values = list(values)
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _pending_requests(db, month: str, *, limit: int, user_id: int | None):
    query = {
        "ledger_type": "AFFILIATE_MONTHLY",
        "status": PENDING_MANUAL,
        "risk_flags": SHORTAGE_FLAG,
        "$or": [{"year_month": month}, {"entitlement_month": month}],
        "voucher_code": {"$in": [None, ""]},
    }
    if user_id is not None:
        query["user_id"] = int(user_id)
    rows = list(db.affiliate_ledger.find(
        query,
        {"user_id": 1, "tier": 1, "year_month": 1, "entitlement_month": 1, "qualified_count": 1,
         "risk_flags": 1, "bundle_recipe": 1, "simulate": 1, "created_at": 1},
        sort=[("created_at", 1), ("_id", 1)], limit=limit + 1,
    ))
    return rows[:limit], len(rows) > limit


def _load_events(db, referrer_id: int, window: dict, cap: int):
    flt = {"referrer_id": int(referrer_id), "qualified_at": {"$gte": window["start_utc"], "$lt": window["end_utc"]}}
    total = int(db.qualified_events.count_documents(flt))
    events = list(db.qualified_events.find(
        flt,
        {"invitee_id": 1, "qualified_at": 1, "redeemed_at": 1, "rule_version": 1, "evidence_id": 1, "account_key": 1},
        sort=[("qualified_at", 1), ("_id", 1)], limit=cap,
    ))
    return total, events


def _load_evidence(db, events: list[dict]):
    """``({invitee_id: (evidence|None, link, conflict_reason)}``."""
    by_id = {}
    ids = [e["evidence_id"] for e in events if e.get("evidence_id") is not None]
    for chunk in _chunks(ids):
        for doc in db[aq.EVIDENCE_COLLECTION].find({"_id": {"$in": chunk}}, _EVIDENCE_PROJECTION):
            by_id[doc["_id"]] = doc

    legacy = [int(e["invitee_id"]) for e in events if e.get("evidence_id") is None]
    code_by_invitee: dict[int, str] = {}
    for chunk in _chunks(legacy):
        for row in db.affiliate_ledger.find(
                {"dedup_key": {"$in": [f"WELCOME:{uid}" for uid in chunk]}}, {"user_id": 1, "voucher_code": 1}):
            code = aq.normalize_code(row.get("voucher_code"))
            if code and row.get("user_id") is not None:
                code_by_invitee[int(row["user_id"])] = code
    hash_to_invitees: dict[str, set] = defaultdict(set)
    for uid, code in code_by_invitee.items():
        hash_to_invitees[aq.code_hash(code)].add(uid)
    by_hash: dict[str, list] = defaultdict(list)
    for chunk in _chunks(hash_to_invitees):
        for doc in db[aq.EVIDENCE_COLLECTION].find({"code_hash": {"$in": chunk}}, _EVIDENCE_PROJECTION):
            by_hash[doc.get("code_hash")].append(doc)

    resolved = {}
    for e in events:
        uid = int(e["invitee_id"])
        if e.get("evidence_id") is not None:
            doc = by_id.get(e["evidence_id"])
            resolved[uid] = (doc, "event_evidence_id", None if doc else "evidence_row_missing")
            continue
        code = code_by_invitee.get(uid)
        if not code:
            resolved[uid] = (None, "welcome_code_hash", "no_welcome_code_on_record")
            continue
        mine = [d for d in by_hash.get(aq.code_hash(code), [])
                if str((d.get("recipient_resolution") or {}).get("user_id")) == str(uid)]
        doc, conflict = _pick_evidence(mine)
        resolved[uid] = (doc, "welcome_code_hash", conflict)
    return resolved


def _account_holders(db, keys: list[str]):
    registry: dict[str, dict] = {}
    events: dict[str, set] = defaultdict(set)
    for chunk in _chunks(keys):
        for row in db[aq.REGISTRY_COLLECTION].find(
                {"account_key": {"$in": chunk}}, {"account_key": 1, "state": 1, "invitee_id": 1}):
            registry[row["account_key"]] = row
        for row in db.qualified_events.find({"account_key": {"$in": chunk}}, {"account_key": 1, "invitee_id": 1}):
            events[row["account_key"]].add(row.get("invitee_id"))
    return registry, events


# --------------------------------------------------------------------------
# Per-affiliate evaluation
# --------------------------------------------------------------------------

def _evaluate_affiliate(db, referrer_id: int, window: dict, cap: int) -> dict:
    total, events = _load_events(db, referrer_id, window, cap)
    resolved = _load_evidence(db, events)

    rows = []
    for ev in events:
        uid = int(ev["invitee_id"])
        evidence, link, problem = resolved[uid]
        verdict = {"cls": UNKNOWN, "reason": problem} if (problem and evidence is None) \
            else classify_evidence(evidence, uid)
        rows.append({
            "invitee_id": uid,
            "legacy": ev.get("rule_version") != aq.RULE_VERSION,
            "link": link,
            "cls": verdict["cls"],
            "reason": verdict["reason"],
            "account_key": (evidence or {}).get("account_key"),
            "when": aq._aware_utc((evidence or {}).get("redeemed_at") or ev.get("redeemed_at") or ev.get("qualified_at")),
        })

    # Canonical Account ID uniqueness among invitees that cleared every other gate.
    keys = sorted({r["account_key"] for r in rows if r["cls"] == PASS})
    if keys:
        registry, holders = _account_holders(db, keys)
        groups: dict[str, list] = defaultdict(list)
        for r in rows:
            if r["cls"] == PASS:
                groups[r["account_key"]].append(r)
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        for key, members in groups.items():
            reg = registry.get(key)
            outside = {h for h in holders.get(key, set()) if h is not None}
            if reg is not None:
                if reg.get("state") not in aq.CREDITED_STATES:
                    # In-flight reservation: not a final holder, never a failure.
                    if reg.get("invitee_id") not in {m["invitee_id"] for m in members}:
                        for m in members:
                            m["cls"], m["reason"] = UNKNOWN, "account_reservation_in_flight"
                        continue
                outside.add(reg.get("invitee_id"))
            member_ids = {m["invitee_id"] for m in members}
            if outside - member_ids:
                winner = None  # someone outside this affiliate's month already owns the account
            elif outside & member_ids:
                winner = next(iter(outside & member_ids))
            else:
                winner = min(members, key=lambda m: (m["when"] or epoch, m["invitee_id"]))["invitee_id"]
            for m in members:
                if m["invitee_id"] != winner:
                    m["cls"], m["reason"] = FAIL_DUPLICATE, "account_already_credited"

    counts = Counter(r["cls"] for r in rows)
    reasons = Counter(r["reason"] for r in rows if r["cls"] != PASS)
    unclassified = max(0, total - len(rows))
    if unclassified:
        counts[UNKNOWN] += unclassified
        reasons["event_scan_cap_exceeded"] += unclassified
    return {
        "total": total, "rows": rows, "counts": counts, "reasons": reasons,
        "legacy_events": sum(1 for r in rows if r["legacy"]),
        "legacy_without_evidence": sum(1 for r in rows if r["legacy"] and r["cls"] == UNKNOWN),
        "truncated": unclassified > 0,
    }


def _masked_sample(row: dict) -> dict:
    return {
        "invitee": aq.mask_value(row["invitee_id"]),
        "account": aq.mask_value(row["account_key"]) if row.get("account_key") else None,
        "class": row["cls"], "reason": row["reason"],
        "legacy_qualification": row["legacy"], "evidence_link": row["link"],
    }


# --------------------------------------------------------------------------
# Audit
# --------------------------------------------------------------------------

def build_audit(
    db,
    *,
    month: str,
    now_utc: datetime | None = None,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    max_events_per_affiliate: int = DEFAULT_MAX_EVENTS,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    user_id: int | None = None,
) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    window = month_window(month, now_utc=now_utc)
    month = window["month"]
    thresholds = tier_thresholds()
    ledgers, scan_truncated = _pending_requests(db, month, limit=int(max_requests), user_id=user_id)

    cache: dict[int, dict] = {}
    requests = []
    skipped_simulated = 0
    totals = Counter()
    reasons_total = Counter()
    verdicts = Counter()
    exposure = {k: {"5": 0, "10": 0, "50": 0, "other": 0, "usd": 0}
                for k in ("all_pending", ELIGIBLE, INELIGIBLE, INDETERMINATE)}

    for ledger in ledgers:
        if ledger.get("simulate"):
            skipped_simulated += 1
            continue
        uid = int(ledger["user_id"])
        if uid not in cache:
            cache[uid] = _evaluate_affiliate(db, uid, window, int(max_events_per_affiliate))
        ev = cache[uid]
        claimed = normalize_tier(ledger.get("tier"))
        needed = int(thresholds.get(claimed, 0))
        passes = ev["counts"].get(PASS, 0)
        unknown = ev["counts"].get(UNKNOWN, 0)
        if needed and passes >= needed:
            verdict = ELIGIBLE
        elif needed and passes + unknown < needed:
            verdict = INELIGIBLE
        else:
            verdict = INDETERMINATE
        counts = _recipe_counts(ledger)
        for bucket in ("all_pending", verdict):
            for k in ("5", "10", "50", "other"):
                exposure[bucket][k] += counts[k]
            exposure[bucket]["usd"] += _usd(counts)
        verdicts[verdict] += 1
        for cls in CLASSES:
            totals[cls] += ev["counts"].get(cls, 0)
        reasons_total.update(ev["reasons"])
        samples = []
        per_cls = Counter()
        for r in ev["rows"]:
            if r["cls"] != PASS and per_cls[r["cls"]] < sample_size:
                per_cls[r["cls"]] += 1
                samples.append(_masked_sample(r))
        requests.append({
            "affiliate_user_id": uid,
            "ledger_id": str(ledger["_id"]),
            "month": month,
            "claimed_tier": claimed,
            "claimed_tier_threshold": needed,
            "other_risk_flags": sorted(set(ledger.get("risk_flags") or []) - {SHORTAGE_FLAG}),
            "current_qualified_count": ev["total"],
            "ledger_qualified_count": ledger.get("qualified_count"),
            "current_eligible_tier": tier_for_count(ev["total"], thresholds),
            "projected_v2_qualified_count": passes,
            "projected_v2_tier": tier_for_count(passes, thresholds),
            "projected_v2_upper_bound_count": passes + unknown,
            "projected_v2_upper_bound_tier": tier_for_count(passes + unknown, thresholds),
            "classification_counts": {c: int(ev["counts"].get(c, 0)) for c in CLASSES},
            "reason_counts": dict(sorted(ev["reasons"].items(), key=lambda kv: str(kv[0]))),
            "legacy_qualification_events": ev["legacy_events"],
            "legacy_events_without_evidence": ev["legacy_without_evidence"],
            "events_scan_truncated": ev["truncated"],
            "requested_reward_v2_verdict": verdict,
            "requested_reward_voucher_counts": {k: counts[k] for k in ("5", "10", "50", "other")},
            "requested_reward_usd": _usd(counts),
            "masked_samples": samples,
        })

    return {
        "audit_version": AUDIT_VERSION,
        "label": LABEL,
        "hypothetical": True,
        "writes_performed": 0,
        "month": month,
        "window_utc": {"start": window["start_utc"].isoformat(), "end": window["end_utc"].isoformat()},
        "generated_at": now_utc.isoformat(),
        "rule_version": aq.RULE_VERSION,
        "gate": {"coupon_new_player": 1, "after_total_bet_amount_gte": "80"},
        "thresholds": thresholds,
        "bounds": {
            "max_requests": int(max_requests), "max_events_per_affiliate": int(max_events_per_affiliate),
            "requests_scan_truncated": scan_truncated, "simulated_ledgers_skipped": skipped_simulated,
        },
        "aggregate": {
            "pending_requests": len(requests),
            "request_verdicts": {v: verdicts.get(v, 0) for v in (ELIGIBLE, INELIGIBLE, INDETERMINATE)},
            "invitee_classification_counts": {c: int(totals.get(c, 0)) for c in CLASSES},
            "reason_counts": dict(sorted(reasons_total.items(), key=lambda kv: str(kv[0]))),
            "voucher_exposure_by_denomination": exposure,
        },
        "requests": requests,
    }


def render_text(report: dict) -> str:
    agg = report["aggregate"]
    lines = [
        report["label"], "",
        f"month={report['month']} requests={agg['pending_requests']} rule={report['rule_version']} "
        f"writes_performed={report['writes_performed']}",
    ]
    b = report["bounds"]
    if b["requests_scan_truncated"]:
        lines.append(f"WARNING: request scan truncated at {b['max_requests']}; totals are partial.")
    lines += ["", "affiliate  claimed  current(count/tier)  v2(pass/tier)  upper(count/tier)  verdict"]
    for r in report["requests"]:
        lines.append(
            f"{r['affiliate_user_id']:<10} {r['claimed_tier']:<8} "
            f"{r['current_qualified_count']}/{r['current_eligible_tier']}  "
            f"{r['projected_v2_qualified_count']}/{r['projected_v2_tier']}  "
            f"{r['projected_v2_upper_bound_count']}/{r['projected_v2_upper_bound_tier']}  "
            f"{r['requested_reward_v2_verdict']}  {r['classification_counts']}"
        )
    lines += ["", f"verdicts: {agg['request_verdicts']}",
              f"invitees: {agg['invitee_classification_counts']}",
              f"reasons: {agg['reason_counts']}", "voucher exposure (count per $ denomination, usd):"]
    for bucket, e in agg["voucher_exposure_by_denomination"].items():
        lines.append(f"  {bucket:<22} $5x{e['5']} $10x{e['10']} $50x{e['50']} = ${e['usd']}")
    return "\n".join(lines)
