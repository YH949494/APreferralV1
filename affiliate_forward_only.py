"""Forward-only V2 history policy: launch-cohort snapshot, marker and reporting.

Policy ``forward_only_from_activation_cutoff`` (docs/affiliate_forward_only_rollout.md):

* qualified history is never re-evaluated or rewritten;
* every active pending/manual referral at activation is evaluated under V2
  whatever its join date or UIM upload date, plus every future referral;
* Account ID uniqueness covers the deterministically seeded historical
  identities and every V2 candidate; unresolved historical identities are
  outside guaranteed duplicate coverage (never "complete lifetime uniqueness").

The launch cohort is a snapshot of invitee ids (one document per invitee,
``_id = invitee_user_id``), so it is idempotent and crash-safe: an upsert per
invitee can be replayed any number of times and a crash simply leaves the
control marker in ``building`` until a later pass finishes.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from pymongo import UpdateOne

import affiliate_qualification as aq

CHUNK = 500
STATE_BUILDING = "building"
STATE_READY = "ready"

_SNAPSHOT_STATUSES = list(aq.ACTIVE_PENDING_STATUSES)


def is_forward_only(control: dict | None) -> bool:
    return (control or {}).get("history_policy") == aq.HISTORY_FORWARD


def report_fingerprint(*, planner_version: str, history_policy: str, identity_config: dict,
                       historical_events: int, unresolved_events: int, seed_fingerprints: list[str],
                       activation_cutoff_utc: str | None = None) -> str:
    """Stable digest of what a migration dry run found and would seed.

    The operator copies it from the reviewed dry run into ``--apply``; the
    marker stores it so preflight can prove the seeded state is the reviewed one.
    """
    payload = {"planner_version": planner_version, "history_policy": history_policy,
               "identity_config": identity_config, "historical_events": int(historical_events),
               "unresolved_events": int(unresolved_events),
               "accounts_to_seed": len(seed_fingerprints), "seed_fingerprints": sorted(seed_fingerprints),
               # the reviewed cutoff is part of the reviewed artifact: a different cutoff = a different fingerprint
               "activation_cutoff_utc": activation_cutoff_utc}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def kl_cutoff(text: str) -> datetime:
    """Parse an activation cutoff. A naive value is Asia/Kuala_Lumpur wall clock."""
    value = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = aq.KL_TZ.localize(value)
    return value.astimezone(timezone.utc)


def kl_iso(moment: datetime) -> str:
    return aq._aware_utc(moment).astimezone(aq.KL_TZ).isoformat()


# ---------------------------------------------------------------------------
# Launch cohort snapshot
# ---------------------------------------------------------------------------

def _active_pending(db):
    return db.pending_referrals.find(
        {"$or": [{"status": {"$in": _SNAPSHOT_STATUSES}},
                 {"status": aq.PENDING_AWAITING_REDEMPTION, "qualification_rule_version": aq.RULE_VERSION}]},
        {"invitee_user_id": 1, "created_at_utc": 1})


def _valid_invitee(raw) -> int | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or not str(raw).isdigit():
        return None
    return int(raw) if int(raw) > 0 else None


def snapshot_launch_cohort(db, *, now_utc: datetime, cutoff: datetime | None = None) -> dict:
    """Idempotently snapshot the ids of all active pending/manual referrals.

    One document per invitee (counted once even with several referral rows);
    invitees that already hold a qualification are historical and excluded.
    Safe to re-run at any time, including after activation (top-up).
    """
    col = db[aq.COHORT_COLLECTION]
    control_col = db[aq.CONTROL_COLLECTION]
    control_col.update_one({"_id": aq.CONTROL_ID}, {"$set": {"launch_cohort.state": STATE_BUILDING,
                                                             "launch_cohort.build_started_at": now_utc}})
    by_invitee: dict[int, list] = {}
    skipped_invalid = 0
    for row in _active_pending(db):
        invitee = _valid_invitee(row.get("invitee_user_id"))
        if invitee is None:
            skipped_invalid += 1
            continue
        by_invitee.setdefault(invitee, []).append(row)
    if cutoff:  # rows already marked for THIS launch belong to the population even once V2 decided them
        for row in db.pending_referrals.find({"activation_cutoff_utc": cutoff}, {"invitee_user_id": 1, "created_at_utc": 1}):
            invitee = _valid_invitee(row.get("invitee_user_id"))
            if invitee is not None and all(r["_id"] != row["_id"] for r in by_invitee.get(invitee, [])):
                by_invitee.setdefault(invitee, []).append(row)
    qualified = set()  # HISTORICAL qualifications only: a V2 qualification of a cohort member keeps its membership
    ids = list(by_invitee)
    for start in range(0, len(ids), CHUNK):
        chunk = ids[start:start + CHUNK]
        for q in db.qualified_events.find({"invitee_id": {"$in": chunk}, "rule_version": {"$ne": aq.RULE_VERSION}},
                                          {"invitee_id": 1}):
            qualified.add(int(q["invitee_id"]))
    added = 0
    ops = []
    for invitee, rows in by_invitee.items():
        if invitee in qualified:
            continue
        update = {"$setOnInsert": {"snapshotted_at": now_utc},
                  "$addToSet": {"pending_ids": {"$each": [r["_id"] for r in rows]}}}
        if cutoff:
            update["$set"] = {"cutoff_utc": cutoff}
        ops.append(UpdateOne({"_id": invitee}, update, upsert=True))
    for start in range(0, len(ops), CHUNK):  # bounded round trips; each op is individually idempotent
        added += col.bulk_write(ops[start:start + CHUNK], ordered=False).upserted_count
    # Rebuild, not just add: a member whose referral became terminal before the cutoff is not part of
    # the activation population. After activation, rows marked for THIS cutoff stay members even once
    # V2 has decided them (a later supported requeue relies on that).
    keep = {i for i in by_invitee if i not in qualified}
    stale = [d["_id"] for d in col.find({}, {"_id": 1}) if d["_id"] not in keep]
    pruned = 0
    for start in range(0, len(stale), CHUNK):
        pruned += col.delete_many({"_id": {"$in": stale[start:start + CHUNK]}}).deleted_count
    size = col.count_documents({})
    marker = {"state": STATE_READY, "built_at": now_utc, "invitees": size,
              "excluded_already_qualified": len(qualified), "skipped_invalid_invitee": skipped_invalid,
              "pruned_stale": pruned}
    control_col.update_one({"_id": aq.CONTROL_ID}, {"$set": {
        "launch_cohort": {**marker, **({"cutoff_utc": cutoff} if cutoff else {})}}})
    return {**marker, "newly_snapshotted": added}


def cohort_status(db, control: dict | None, *, live: bool = False) -> dict:
    """Snapshot readiness. ``live`` also counts active invitees not yet snapshotted
    (they are topped up at activation; shown so the operator can see the delta)."""
    marker = (control or {}).get("launch_cohort") or {}
    out = {"state": marker.get("state") or "not_built", "size": int(marker.get("invitees") or 0),
           "built_at": marker.get("built_at").isoformat() if isinstance(marker.get("built_at"), datetime) else None,
           "excluded_already_qualified": int(marker.get("excluded_already_qualified") or 0)}
    out["ready"] = out["state"] == STATE_READY
    if live:
        snap = {d["_id"] for d in db[aq.COHORT_COLLECTION].find({}, {"_id": 1})}
        out["size"] = len(snap)
        live_ids = {i for i in (_valid_invitee(r.get("invitee_user_id")) for r in _active_pending(db)) if i}
        out["active_not_yet_snapshotted"] = len(live_ids - snap)
    return out


# ---------------------------------------------------------------------------
# Reporting (status / preflight / activation preview / Databot report)
# ---------------------------------------------------------------------------

def forward_only_summary(db, control: dict | None, *, now_utc: datetime, live: bool = False) -> dict:
    """The visible forward-only facts. ``live`` recounts from collections
    (admin CLI); otherwise it reads the migration marker only (cheap, Databot)."""
    control = control or {}
    marker = control.get("forward_only_marker") or {}
    summary = control.get("seed_summary") or {}
    policy = control.get("history_policy", aq.HISTORY_STRICT)
    forward = policy == aq.HISTORY_FORWARD
    qualified = marker.get("qualified_history_count", summary.get("historical_events"))
    seeded = marker.get("seeded_count", summary.get("accounts_seeded"))
    unresolved = marker.get("unresolved_count", summary.get("unresolved_events"))
    if live:
        qualified = db.qualified_events.count_documents({"rule_version": {"$ne": aq.RULE_VERSION}})
        seeded = db[aq.REGISTRY_COLLECTION].count_documents({"state": aq.REG_SEEDED})
    cutoff = aq.launch_cutoff(control)
    return {
        "history_policy": policy,
        "limitation": aq.HISTORY_LIMITATION if forward else None,
        "historical_qualified_count": qualified,
        "deterministic_historical_identities_seeded": seeded,
        "unresolved_historical_identities": unresolved,
        "pending_manual_cohort": cohort_status(db, control, live=live),
        "v2_mode": "actual" if aq.new_rule_active(control, now_utc) else "preview",
        "activation_cutoff_utc": (cutoff or aq._aware_utc(marker.get("activation_cutoff_utc"))).isoformat()
        if (cutoff or marker.get("activation_cutoff_utc")) else None,
        "activation_cutoff_kl": marker.get("activation_cutoff_kl"),
        "marker_version": marker.get("version"),
        "report_fingerprint": marker.get("fingerprint"),
        "legacy_writer": aq.legacy_writer_state(control, now_utc),
    }
