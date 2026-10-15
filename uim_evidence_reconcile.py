"""Repair already-synced Welcome evidence that a newer, fully attested Databot UIM batch re-observed.

Why it exists
-------------
Before ``affiliate_qualification.reobservation_verdict`` a newly committed final UIM batch that re-observed a logical
record (same coupon code + Account ID) was treated as a CONFLICT with the older, unverified observation, and the old
evidence kept its unverified provenance (``redeemed_at_timezone_unverified``, completeness not verified) or a permanent
``source_conflict_at`` hold. New extraction no longer does that; this module repairs the evidence that was already
written.

What it does, per candidate evidence document
---------------------------------------------
It replays EVERY committed, non-rolled-back source observation of the record in commit order (the same fold the
Qualification Preview and live extraction use) and only then decides:

* attested final observation, every fact the old evidence established agrees -> move the evidence onto it
  (``status=received`` so the existing validator decides; the replaced provenance is appended to
  ``supersession_history``; a ``conflicting_source_observations`` hold that the replay shows to be spurious is lifted);
* any disagreement (redemption instant, coupon_new_player, exact Decimal turnover, success) -> untouched, stays review;
* a removal observation anywhere in the history -> untouched, stays removed;
* final observation not attested (no declared coverage for that day, timezone unverified, ...) -> untouched, pending;
* evidence with a decision, lease, void, qualification or registry entry -> untouched.

It writes ONLY ``welcome_redemption_evidence`` (candidate documents) and, with ``--commit``, one audit document in
``uim_evidence_reconciliations``. It never qualifies, rejects, reserves an Account ID, or touches ``qualified_events``,
``referral_events``, ``affiliate_ledger``, rewards, XP, ``pending_referrals`` or the control document. Dry run by default;
bounded by ``limit`` per run and ``max_scan_rows`` (a scan that would exceed it aborts without writing); idempotent
(repaired evidence carries ``evidence_basis.coverage_attested`` and is no longer a candidate). Output is counts plus
masked samples only.
"""
from __future__ import annotations

import uuid
from collections import Counter
from datetime import datetime

import affiliate_qualification as aq

RUN_COLLECTION = "uim_evidence_reconciliations"
DEFAULT_LIMIT = 500
MAX_LIMIT = 5000
DEFAULT_MAX_SCAN_ROWS = 500_000
SAMPLE_SIZE = 10
REPAIRABLE_STATUSES = (aq.EV_RECEIVED, aq.EV_REVIEW)
# Preview review reasons a newer attested observation repairs (see reobservation_verdict).
PREVIEW_REPAIRABLE_REASONS = ("redeemed_at_timezone_unverified", "conflicting_source_observations",
                              "seven_day_data_incomplete")


def _scope_filter() -> dict:
    """Every not-yet-attested v2 UIM evidence document: the population the guards below partition."""
    return {"source": aq.SOURCE_DATABOT_UIM, "rule_version": aq.RULE_VERSION,
            "evidence_basis.coverage_attested": {"$ne": True}}


def _guard_filters(now_utc: datetime) -> dict[str, dict]:
    """Each candidate guard as the Mongo predicate for the documents it EXCLUDES. A guard field that is absent OR
    stored as ``null`` is "not set" (``{"f": None}`` matches both; ``$exists: False`` does not match ``null``, which
    is how a pending_review document carrying ``lease_until: null`` silently dropped out of every run). A lease only
    protects while it is live."""
    return {
        "status_not_repairable": {"status": {"$nin": list(REPAIRABLE_STATUSES)}},
        "voided": {"voided_at": {"$ne": None}},
        "active_lease": {"lease_until": {"$gt": now_utc}},
        "qualification_decision_recorded": {"qualification_decision_at": {"$ne": None}},
    }


def _unguarded(now_utc: datetime) -> dict:
    """The same guards expressed as the conditions a candidate (and the compare-and-set write) must satisfy."""
    return {
        "status": {"$in": list(REPAIRABLE_STATUSES)},
        "voided_at": None, "qualification_decision_at": None,
        "$or": [{"lease_until": None}, {"lease_until": {"$lte": now_utc}}],
    }


def _candidate_filter(now_utc: datetime, after_id=None) -> dict:
    flt = {**_scope_filter(), **_unguarded(now_utc)}
    if after_id is not None:
        flt["_id"] = {"$gt": after_id}
    return flt


def _guard_diagnostics(db, now_utc: datetime) -> dict:
    """How many documents of the scope each guard excludes (a document tripping several counts under each), how many
    the scope holds in total, and where evidence that is NOT in scope lives (wrong source/rule_version shows here)."""
    col = db[aq.EVIDENCE_COLLECTION]
    scope = _scope_filter()
    excluded = {name: col.count_documents({**scope, **flt}) for name, flt in _guard_filters(now_utc).items()}
    return {
        "scope_documents": col.count_documents(scope),
        "excluded_by_guard": excluded,
        "excluded_any_guard": col.count_documents({**scope, "$or": list(_guard_filters(now_utc).values())}),
        "evidence_total": col.count_documents({}),
        "evidence_by_source_rule_status": [
            {"source": g["_id"].get("source"), "rule_version": g["_id"].get("rule_version"),
             "status": g["_id"].get("status"), "n": g["n"]}
            for g in col.aggregate([{"$group": {"_id": {"source": "$source", "rule_version": "$rule_version",
                                                         "status": "$status"}, "n": {"$sum": 1}}}])],
    }


def _refs_without_scoped_evidence(db, config: dict, now_utc: datetime, max_scan_rows: int):
    """Identity-level correlation: source records whose folded observation is one the preview reports as
    review-pending (final not attested, or observations disagree) but that have NO in-scope evidence document at all.
    A guard-protected document explains only its own source_ref. ``(None, n)`` = scan cap exceeded."""
    folded, scanned = _replay(db, config, None, now_utc=now_utc, max_scan_rows=max_scan_rows)
    if folded is None:
        return None, scanned
    control = {**(aq.get_control(db) or {}), "source_config": config}
    lookup = aq.DbLookup(db)

    def preview_reason(f: dict) -> str | None:
        if f.get("_source_removed"):
            return None
        if f.get("_conflicting_observation"):
            return "conflicting_source_observations"
        try:
            return aq.assess_evidence(f, control=control, now_utc=now_utc, lookup=lookup).get("reason")
        except Exception:
            return PREVIEW_REPAIRABLE_REASONS[0]      # cannot tell: stay conservative, treat as relevant

    # Only records whose preview outcome is itself one of the repairable reasons: an unrelated review reason
    # (missing fields, unsupported transport, ...) is not what this command repairs nor what the preview mismatch is about.
    pending = {ref: f for ref, f in folded.items() if preview_reason(f) in PREVIEW_REPAIRABLE_REASONS}
    have: set[str] = set()
    refs = list(pending)
    for i in range(0, len(refs), 1000):
        have.update(d["source_ref"] for d in db[aq.EVIDENCE_COLLECTION].find(
            {**_scope_filter(), "source_ref": {"$in": refs[i:i + 1000]}}, {"source_ref": 1}))
    missing = [pending[r] for r in refs if r not in have]
    return missing, scanned


def _preview_repairable_legacy(db, config: dict, now_utc: datetime) -> dict:
    """The read-only Qualification Preview's count of review reasons this command exists to repair."""
    from affiliate_qualification_preview import build_preview
    summary = (build_preview(db, month=None, now_utc=now_utc, source_config=config).get("evidence_summary") or {})
    reasons = summary.get("review_reasons") or {}
    return {k: int(reasons[k]) for k in PREVIEW_REPAIRABLE_REASONS if reasons.get(k)}


def _why_not_attested(final: dict) -> str:
    if final.get("redeemed_at_reason"):
        return str(final["redeemed_at_reason"])
    if final.get("account_reason") or not final.get("account_key"):
        return str(final.get("account_reason") or "missing_account")
    if final.get("redeemed_at") is None:
        return "missing_redeemed_at"
    fields = final.get("qualification_fields") or {}
    if fields.get("seven_day_complete") is not True:
        return "seven_day_completeness_unverified"
    core = aq.observation_core(final)
    if core["coupon_new_player"] is None or core["after_total_bet_amount"] is None:
        return "missing_or_invalid_qualification_fields"
    if not (final.get("evidence_basis") or {}).get("coverage_attested"):
        return "outside_declared_complete_coverage"
    return "not_attested"


def _has_effects(db, evidence_id) -> bool:
    return bool(db.qualified_events.find_one({"evidence_id": evidence_id}, {"_id": 1})
                or db[aq.REGISTRY_COLLECTION].find_one({"evidence_id": evidence_id}, {"_id": 1}))


def _replay(db, config: dict, wanted: set[str] | None, *, now_utc: datetime, max_scan_rows: int) -> tuple[dict | None, int]:
    """source_ref -> folded final observation, for the wanted refs only (every ref when ``wanted`` is None).
    ``(None, n)`` = scan cap exceeded."""
    folded: dict[str, dict] = {}
    scanned = 0
    for _batch_id, row in aq.iter_committed_source_rows(db, config):
        scanned += 1
        if scanned > max_scan_rows:
            return None, scanned
        obs = aq.parse_source_row(row, config)
        if obs is None:
            continue
        obs["code"] = aq.normalize_code(obs["code"])
        if not obs["code"]:
            continue
        doc = aq.build_evidence_doc(source=aq.SOURCE_DATABOT_UIM, recipient={}, now_utc=now_utc, **obs)
        if wanted is not None and doc["source_ref"] not in wanted:
            continue
        aq.fold_observation(folded, doc, removed=obs.get("observation") == aq.OBS_REMOVED)
    return folded, scanned


def reconcile(db, *, config: dict | None, now_utc: datetime, commit: bool = False, limit: int = DEFAULT_LIMIT,
              after_id=None, max_scan_rows: int = DEFAULT_MAX_SCAN_ROWS, operator: str = "unknown") -> dict:
    """One bounded pass. Returns counts and masked samples; writes nothing unless ``commit``."""
    out = {"ok": True, "dry_run": not commit, "limit": int(limit), "candidates": 0, "scanned_source_rows": 0,
           "actions": {}, "not_attested_reasons": {}, "samples": {}, "next_after_id": None}
    if (config or {}).get("source") != aq.SOURCE_DATABOT_UIM:
        return {**out, "ok": False, "error": "source_is_not_databot_uim"}
    limit = max(1, min(int(limit), MAX_LIMIT))
    out["limit"] = limit
    candidates = list(db[aq.EVIDENCE_COLLECTION].find(_candidate_filter(now_utc, after_id)).sort("_id", 1).limit(limit))
    out["candidates"] = len(candidates)
    if len(candidates) == limit:
        out["next_after_id"] = str(candidates[-1]["_id"])
    guards = _guard_filters(now_utc)
    out["evidence_not_repairable_state"] = db[aq.EVIDENCE_COLLECTION].count_documents(
        {**_scope_filter(), **guards["status_not_repairable"]})
    out["candidate_guards"] = _guard_diagnostics(db, now_utc)
    if not candidates:
        if after_id is None:
            # An empty first page while the preview still reports repairable legacy reasons is a mismatch, never
            # "nothing to do": the candidate selection and the preview disagree about the same evidence.
            expected = _preview_repairable_legacy(db, config, now_utc)
            if expected:
                # A guard-protected document explains only the preview reason of its OWN source record, so correlate
                # by source_ref, not by count: any pending source record without in-scope evidence is a mismatch.
                missing, scanned = _refs_without_scoped_evidence(db, config, now_utc, int(max_scan_rows))
                out["scanned_source_rows"] = scanned
                out["preview_repairable_legacy_reasons"] = expected
                if missing is None:
                    return {**out, "ok": False, "error": "scan_exceeds_max_scan_rows"}
                if missing:
                    out["source_records_without_evidence"] = len(missing)
                    out["samples"] = {"source_records_without_evidence": [
                        {"code": f.get("code_masked"), "account": f.get("account_key_masked")}
                        for f in missing[:SAMPLE_SIZE]]}
                    return {**out, "ok": False, "error": "candidate_selection_preview_mismatch",
                            "detail": "no candidate evidence, the preview reports repairable legacy reasons, and "
                                      "source records with no in-scope evidence exist; see candidate_guards "
                                      "(excluded_by_guard, evidence_by_source_rule_status)"}
        return out
    folded, scanned = _replay(db, config, {c["source_ref"] for c in candidates}, now_utc=now_utc,
                              max_scan_rows=int(max_scan_rows))
    out["scanned_source_rows"] = scanned
    if folded is None:
        return {**out, "ok": False, "error": "scan_exceeds_max_scan_rows", "actions": {}}

    actions: Counter = Counter()
    reasons: Counter = Counter()
    samples: dict[str, list] = {}
    plans = []

    def note(action: str, ev: dict, reason: str | None = None) -> None:
        actions[action] += 1
        bucket = samples.setdefault(action, [])
        if len(bucket) < SAMPLE_SIZE:
            bucket.append({"evidence_id": str(ev["_id"]), "code": ev.get("code_masked"),
                           "account": ev.get("account_key_masked"), "reason": reason})

    for ev in candidates:
        final = folded.get(ev["source_ref"])
        if _has_effects(db, ev["_id"]):
            note("skipped_has_effects", ev)
        elif ev.get("removed_in_batch_ids") or ev.get("source_conflict_reason") == "source_row_removed_by_later_import" \
                or (final or {}).get("_source_removed"):
            note("removed_stays_removed", ev)
        elif final is None:
            note("no_committed_source_observation", ev)
        elif final.get("_conflicting_observation"):
            note("conflict_kept_for_review", ev, "source_observations_disagree")
        elif not aq.observation_attested(final):
            why = _why_not_attested(final)
            reasons[why] += 1
            note("not_attested_remains_pending", ev, why)
        else:
            verdict = aq.reobservation_verdict(ev, final)
            if verdict == aq.REOBS_CONFLICT:
                note("conflict_kept_for_review", ev, "stored_evidence_disagrees_with_attested_observation")
            elif verdict in (aq.REOBS_UPGRADE, aq.REOBS_IDENTICAL) or verdict is None:
                plans.append((ev, final, verdict or aq.REOBS_UPGRADE))
                note("would_repair" if not commit else "repair_planned", ev,
                     "lift_spurious_conflict_hold" if ev.get("source_conflict_at") else "supersede_unverified")
    out["not_attested_reasons"] = dict(reasons)

    if commit:
        run_id = uuid.uuid4().hex
        repaired = raced = 0
        for ev, final, verdict in plans:
            update = aq.supersession_update(ev, final, source_batch_id=(ev.get("source_batch_ids") or [None])[-1],
                                            now_utc=now_utc, verdict=verdict, run=run_id)
            if ev.get("source_conflict_at"):
                update["$unset"].update({"source_conflict_at": "", "source_conflict_reason": ""})
                entry = update["$push"]["supersession_history"]
                entry["cleared_conflict_reason"] = ev.get("source_conflict_reason")
                entry["cleared_conflict_at"] = ev.get("source_conflict_at")
            update["$set"]["evidence_basis"] = {**(final.get("evidence_basis") or {}), "reconciled_run": run_id}
            # An absent lease is what the evaluator selects on; a null or expired one would leave the repaired
            # (status=received) evidence permanently unevaluated.
            update["$unset"]["lease_until"] = ""
            applied = db[aq.EVIDENCE_COLLECTION].update_one(
                {**_unguarded(now_utc), "_id": ev["_id"], "status": ev.get("status"),
                 "observation_fingerprint": ev.get("observation_fingerprint"),
                 "source_conflict_at": ev.get("source_conflict_at") or None}, update)
            if getattr(applied, "modified_count", 0):
                repaired += 1
            else:
                raced += 1
        actions.pop("repair_planned", None)
        if repaired:
            actions["repaired"] = repaired
        if raced:
            actions["raced_skipped"] = raced
        db[RUN_COLLECTION].insert_one({
            "run_id": run_id, "at": now_utc, "operator": operator, "limit": limit,
            "after_id": str(after_id) if after_id is not None else None, "counts": dict(actions),
            "candidates": len(candidates), "scanned_source_rows": scanned})
        out["run_id"] = run_id
    out["actions"] = dict(actions)
    out["samples"] = samples
    return out
