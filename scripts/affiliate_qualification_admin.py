#!/usr/bin/env python3
"""Operate welcome_redemption_v2 using the original v1 control identity.

DRY RUN BY DEFAULT. Every state-changing subcommand prints what it would do
and writes nothing unless ``--commit`` is given. ``status`` and ``preflight``
are always read-only (secondary-preferred connection, no index creation).

Subcommands
-----------
status                      control doc, readiness, evidence/registry counts
preflight                   exit 0 only if activation is safe right now
configure-source ...        persist the authoritative redemption source mapping
configure-coverage-policy   attest WHAT the UIM coverage dates measure and the Welcome voucher's redemption
                            window - the only thing that lets a MISSING row reject (dry run by default)
diagnose-issuance-order     read-only: gap histogram + clock-basis hypotheses for "redeemed before issued" records
reconcile-uim-evidence      repair already-synced evidence a newer fully attested UIM batch re-observed (evidence only;
                            dry run by default; --limit/--after-id bound each run; counts + masked samples only)
snapshot-launch-cohort      snapshot the ids of all active pending/manual referrals (idempotent; dry run by default)
recover-legacy-writer       clear a dead legacy-writer lease (never an active one); also: release-legacy-writer
activate --cutoff ISO       persist the launch cutoff (immutable) + mode=active
pause --reason R            stop NEW qualifications (legacy stays disabled)
resume                      mode=active again (re-runs preflight)
cancel-scheduled            undo an activation whose cutoff has not arrived yet
requeue --evidence-id ID    re-run FULL validation for a reviewed/rejected row
void-batch --upload-batch-id ID --reason R
                            source rollback/correction (documented policy)

There is deliberately no "disable" after launch: once a cutoff is persisted,
the legacy check-in/channel award never runs again from that instant, so a
rollback can only pause new qualifications. Credited accounts are never
released by this tool.

Usage
-----
    MONGO_URL=... python scripts/affiliate_qualification_admin.py status
    MONGO_URL=... python scripts/affiliate_qualification_admin.py preflight
    MONGO_URL=... python scripts/affiliate_qualification_admin.py activate \\
        --cutoff 2026-11-01T00:00:00+08:00 --commit
    MONGO_URL=... python scripts/affiliate_qualification_admin.py pause --reason "source fix" --commit
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import affiliate_forward_only as fwd  # noqa: E402
import affiliate_qualification as aq  # noqa: E402
import uim_coverage  # noqa: E402


def _write_db():
    """Primary connection WITHOUT database.init_db() (which creates indexes)."""
    from pymongo import MongoClient

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise SystemExit("MONGO_URL is not configured")
    return MongoClient(mongo_url)[os.environ.get("MONGO_DB", "referral_bot")]


def _read_only_db():
    from scripts.verify_affiliate_reward_plan import _read_only_db as factory

    return factory()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_cutoff(text: str) -> datetime:
    value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("cutoff must carry an explicit offset, e.g. 2026-11-01T00:00:00+08:00")
    return value.astimezone(timezone.utc)


def _public_control(control: dict | None) -> dict | None:
    if not control:
        return None
    result = {k: v for k, v in control.items() if k != "_id"}
    if isinstance(result.get("seed_summary"), dict):
        result["seed_summary"] = {k: v for k, v in result["seed_summary"].items() if k != "seed_identity_fingerprints"}
    return result


def forward_only_preflight(db, control: dict, *, now_utc: datetime, cutoff: datetime | None) -> dict:
    """Extra gates that apply only under forward_only_from_activation_cutoff."""
    summary = fwd.forward_only_summary(db, control, now_utc=now_utc, live=True)
    marker = control.get("forward_only_marker") or {}
    configured = aq._aware_utc(marker.get("activation_cutoff_utc"))
    blockers = []
    if not summary["pending_manual_cohort"]["ready"]:
        blockers.append("launch_cohort_snapshot_not_ready")
    if cutoff is not None and configured is not None and cutoff != configured:
        blockers.append("activation_cutoff_mismatch")
    if configured is None:
        blockers.append("activation_cutoff_not_configured")
    return {**summary, "configured_activation_cutoff_utc": configured.isoformat() if configured else None,
            "proposed_activation_cutoff_utc": cutoff.isoformat() if cutoff else None,
            "blockers": blockers, "ok": not blockers}


def preflight_report(db, *, now_utc: datetime, cutoff: datetime | None = None) -> dict:
    from scripts.migrate_affiliate_account_dedupe import completion_readiness

    control = aq.get_control(db) or {}
    report = {
        "migration_completion": completion_readiness(db, control),
        "indexes": aq.index_readiness(db),
        "integration": aq.integration_readiness(db, control.get("source_config"), now_utc=now_utc),
        "migration_seeded": bool(control.get("seed_completed_at")),
        "identity_config_matches_seed": bool(control.get("seed_completed_at"))
        and control.get("seeded_identity_config") == aq.identity_config(control.get("source_config")),
        "consistency": aq.consistency_report(db, now_utc=now_utc),
        "rule_revision_ready": control.get("rule_version") == aq.RULE_VERSION,
        "legacy_writer_in_flight": bool(control.get("legacy_writer_token")),
        "legacy_writer": aq.legacy_writer_state(control, now_utc),
        # Durable in-flight-award barrier on the control document (the same document activation's CAS
        # reads). A paused/crashed worker leaves it behind: activation must not overtake it.
        "legacy_award_inflight": bool(control.get("legacy_award_inflight")),
        "cancellation_repair_pending": bool(control.get("cancelled_cohort_cutoff")),
        "v1_qualified_events": db.qualified_events.count_documents({"rule_version": aq.LEGACY_RULE_VERSION}),
        "pending_maturity": aq.pending_maturity_report(db, config=control.get("source_config"), now_utc=now_utc),
        # Final UIM coverage + attested absence policy: a missing row may only reject inside proven complete
        # coverage, and activation does not proceed on unverified/partial/gapped coverage.
        "coverage": uim_coverage.preflight_section(db, control, now_utc=now_utc),
    }
    forward = fwd.is_forward_only(control)
    if forward:
        report["forward_only"] = forward_only_preflight(db, control, now_utc=now_utc, cutoff=cutoff)
    report["ok"] = bool(
        (not forward or report["forward_only"]["ok"])
        and report["coverage"]["ok"]
        and report["indexes"]["ok"]
        and report["integration"]["ok"]
        and report["migration_seeded"]
        and report["migration_completion"]["ok"]
        and report["identity_config_matches_seed"]
        and report["consistency"]["ok"]
        and report["rule_revision_ready"]
        and not report["legacy_writer_in_flight"]
        and not report["legacy_award_inflight"]
        and not report["cancellation_repair_pending"]
        and report["v1_qualified_events"] == 0
    )
    return report


def status_report(db, *, now_utc: datetime) -> dict:
    control = aq.get_control(db)
    evidence = {}
    for row in db[aq.EVIDENCE_COLLECTION].aggregate(
        [{"$group": {"_id": {"status": "$status", "reason": "$reason"}, "n": {"$sum": 1}}}]
    ):
        key = f"{row['_id'].get('status')}:{row['_id'].get('reason') or '-'}"
        evidence[key] = row["n"]
    registry = {}
    for row in db[aq.REGISTRY_COLLECTION].aggregate([{"$group": {"_id": "$state", "n": {"$sum": 1}}}]):
        registry[str(row["_id"])] = row["n"]
    return {
        "now_utc": now_utc.isoformat(),
        "control": _public_control(control),
        "legacy_award_allowed": aq.legacy_award_allowed(control, now_utc),
        "new_rule_active": aq.new_rule_active(control, now_utc),
        "evidence_by_status_reason": evidence,
        "registry_by_state": registry,
        "v1_qualified_events": db.qualified_events.count_documents({"rule_version": aq.LEGACY_RULE_VERSION}),
        "v2_qualified_events": db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}),
        "history_limitation": control.get("history_limitation") if control else None,
        "forward_only": fwd.forward_only_summary(db, control, now_utc=now_utc, live=True),
        "preflight": preflight_report(db, now_utc=now_utc),
    }


def _apply(db, args, update: dict, *, upsert: bool = False) -> int:
    if not args.commit:
        print(json.dumps({"dry_run": True, "would_update": update}, indent=2, default=str))
        return 0
    db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, update, upsert=upsert)
    print(json.dumps({"committed": True, "control": _public_control(aq.get_control(db))}, indent=2, default=str))
    return 0


def main(argv=None, *, db_factory=None, read_only_db_factory=None, now_fn=_now) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("preflight")
    sub.add_parser("activation-preview", help="read-only preview of all active pending/manual referrals")
    upgrade = sub.add_parser("upgrade-rule", help="CAS transition the existing disabled/unlaunched switch to v2")
    release = sub.add_parser("release-legacy-writer", help="recover a crashed legacy barrier after stopping its process")
    release.add_argument("--token", required=True)
    release.add_argument("--acknowledge-writer-stopped", action="store_true")
    recover = sub.add_parser("recover-legacy-writer", help="clear an EXPIRED legacy-writer lease (never an active one)")
    recover.add_argument("--token")
    recover.add_argument("--acknowledge-writer-stopped", action="store_true",
                         help="required only for a pre-lease token whose liveness cannot be proven")
    recover.add_argument("--release-awarding", action="store_true",
                         help="also clear a dead worker's in-flight-award barrier (requires --acknowledge-writer-stopped; "
                              "awards are idempotent per invitee, so a completed one is skipped on retry)")
    sub.add_parser("snapshot-launch-cohort", help="idempotently snapshot active pending/manual referral invitees")
    pf = sub.choices["preflight"]
    pf.add_argument("--cutoff", help="proposed activation cutoff (ISO with offset); must match the forward-only marker")

    cfg = sub.add_parser("configure-source")
    cfg.add_argument("--code-column", required=True)
    cfg.add_argument("--account-column", required=True)
    cfg.add_argument("--redeemed-at-column", required=True)
    cfg.add_argument("--status-column", required=True)
    cfg.add_argument("--success-values", required=True, help="comma-separated, case-insensitive")
    cfg.add_argument("--source-timezone", default="Asia/Kuala_Lumpur")
    cfg.add_argument("--account-namespace", default=None)
    cfg.add_argument("--namespace-column", default=None)
    cfg.add_argument("--campaign-ids", default=None, help="comma-separated Welcome campaign ids (optional filter)")
    cfg.add_argument("--unlinked-account-policy", choices=[aq.UNLINKED_POLICY_ACCEPT, aq.UNLINKED_POLICY_REVIEW],
                     default=aq.UNLINKED_POLICY_ACCEPT)

    dcfg = sub.add_parser(
        "configure-databot-source",
        help="use Databot's committed UIM imports (synced by uim_redemption_sync) as the redemption source",
    )
    dcfg.add_argument("--account-namespace", required=True, help="provider/tenant of the UIM gaming accounts")
    dcfg.add_argument("--source-timezone", default="Asia/Kuala_Lumpur",
                      help="timezone of UIM's naive Coupon Redeem Time wall clock")
    dcfg.add_argument("--attested-by", required=True,
                      help="data owner attesting the UIM export lists SUCCESSFUL redemptions only")
    dcfg.add_argument("--attestation-ref", required=True, help="where that attestation is recorded (ticket/email)")
    dcfg.add_argument("--legacy-time-basis", choices=[aq.LEGACY_TIME_REVIEW, aq.LEGACY_TIME_NAIVE],
                      default=aq.LEGACY_TIME_REVIEW,
                      help="rows imported before Databot recorded time provenance: review (default) or "
                           "naive_wallclock (attested: Coupon Redeem Time is naive wall clock in --source-timezone)")
    dcfg.add_argument("--campaign-ids", default=None, help="comma-separated UIM campaign labels (optional filter)")
    dcfg.add_argument("--unlinked-account-policy", choices=[aq.UNLINKED_POLICY_ACCEPT, aq.UNLINKED_POLICY_REVIEW],
                      default=aq.UNLINKED_POLICY_ACCEPT)
    cpol = sub.add_parser(
        "configure-coverage-policy",
        help="attest the coverage basis + Welcome redemption window that allow absence-based rejection",
    )
    cpol.add_argument("--basis", choices=list(uim_coverage.ABSENCE_BASES),
                      help="what Databot's declared coverage dates measure (only voucher_redemption_date is supported)")
    cpol.add_argument("--welcome-redemption-window-days", type=int,
                      help="days a Welcome voucher stays redeemable after issuance (data owner's figure)")
    cpol.add_argument("--attested-by", help="data owner attesting the basis and the window")
    cpol.add_argument("--attestation-ref", help="where that attestation is recorded (ticket/email)")
    cpol.add_argument("--disable", action="store_true", help="switch absence-based rejection off (history is kept)")
    cpol.add_argument("--reason", required=True)
    sync = sub.add_parser("sync-evidence", help="one Databot evidence sync pass (writes only uim_redemption_*)")
    sync.add_argument("--max-rows", type=int, default=5000)
    sub.add_parser("sync-status")
    dio = sub.add_parser("diagnose-issuance-order",
                         help="read-only: why do records look redeemed before their voucher was issued (clock-basis test)")
    dio.add_argument("--max-scan-rows", type=int, default=500_000, help="abort above this many source rows")
    dio.add_argument("--reveal-samples", type=int, default=0,
                     help="print the full voucher code for the first N samples (0 = masked only); look one up at the source")
    rec = sub.add_parser("reconcile-uim-evidence",
                         help="repair evidence superseded by a newer attested UIM batch (dry run by default)")
    rec.add_argument("--limit", type=int, default=500, help="max evidence documents examined this run (<=5000)")
    rec.add_argument("--after-id", default=None, help="resume after this evidence _id (from next_after_id)")
    rec.add_argument("--max-scan-rows", type=int, default=500_000, help="abort without writing above this many source rows")

    act = sub.add_parser("activate")
    act.add_argument("--cutoff", required=True, help="ISO-8601 with offset")
    act.add_argument("--acknowledge-unknown-history", action="store_true")
    pause = sub.add_parser("pause")
    pause.add_argument("--reason", required=True)
    sub.add_parser("resume")
    sub.add_parser("cancel-scheduled")
    rq = sub.add_parser("requeue")
    rq.add_argument("--evidence-id", required=True)
    vb = sub.add_parser("void-batch")
    vb.add_argument("--upload-batch-id", required=True)
    vb.add_argument("--reason", required=True)
    for p in (cfg, dcfg, cpol, sync, rec, act, upgrade, release, recover, sub.choices["snapshot-launch-cohort"], pause, sub.choices["resume"], sub.choices["cancel-scheduled"], rq, vb):
        p.add_argument("--commit", action="store_true", help="write (default: dry run)")
        p.add_argument("--operator", default=os.environ.get("USER") or "unknown")
    args = parser.parse_args(argv)
    now_utc = now_fn()

    if args.cmd == "sync-status":
        import uim_redemption_sync

        db = (read_only_db_factory or _read_only_db)()
        print(json.dumps(uim_redemption_sync.sync_health(db, now_utc=now_utc), indent=2, default=str))
        return 0

    if args.cmd == "diagnose-issuance-order":
        import uim_issuance_order_diagnostic

        db = (read_only_db_factory or _read_only_db)()
        report = uim_issuance_order_diagnostic.diagnose(
            db, config=(aq.get_control(db) or {}).get("source_config"), now_utc=now_utc, max_scan_rows=args.max_scan_rows,
            reveal_samples=max(0, min(args.reveal_samples, 10)))
        print(json.dumps(report, indent=2, default=str))
        return 0 if report["ok"] else 1

    if args.cmd == "reconcile-uim-evidence":
        import uim_evidence_reconcile

        if args.limit < 1 or args.limit > uim_evidence_reconcile.MAX_LIMIT:
            print(f"refused: --limit must be 1..{uim_evidence_reconcile.MAX_LIMIT}", file=sys.stderr)
            return 2
        after = None
        if args.after_id:
            from bson import ObjectId
            try:
                after = ObjectId(args.after_id)
            except Exception:
                print("refused: --after-id is not an evidence _id", file=sys.stderr)
                return 2
        # Dry run reads through the secondary-preferred, no-index connection; --commit needs the primary.
        db = (db_factory or _write_db)() if args.commit else (read_only_db_factory or _read_only_db)()
        control = aq.get_control(db) or {}
        if args.commit and aq.new_rule_active(control, now_utc):
            print("refused: welcome_redemption_v2 is active; repair evidence before activation", file=sys.stderr)
            return 2
        report = uim_evidence_reconcile.reconcile(
            db, config=control.get("source_config"), now_utc=now_utc, commit=args.commit, limit=args.limit,
            after_id=after, max_scan_rows=args.max_scan_rows, operator=args.operator)
        print(json.dumps(report, indent=2, default=str))
        return 0 if report["ok"] else 1

    if args.cmd == "activation-preview":
        from affiliate_qualification_preview import build_preview
        db = (read_only_db_factory or _read_only_db)()
        report = build_preview(db, month=None, now_utc=now_utc)
        print(json.dumps(report, indent=2, default=str))
        return 0 if report["ok"] and not report["source"]["blocked"] else 1

    if args.cmd in ("status", "preflight"):
        db = (read_only_db_factory or _read_only_db)()
        if args.cmd == "preflight" and args.cutoff:
            try:
                proposed = _parse_cutoff(args.cutoff)
            except ValueError as exc:
                print(f"refused: {exc}", file=sys.stderr)
                return 2
        else:
            proposed = None
        report = (status_report(db, now_utc=now_utc) if args.cmd == "status"
                  else preflight_report(db, now_utc=now_utc, cutoff=proposed))
        print(json.dumps(report, indent=2, default=str))
        return 0 if args.cmd == "status" or report["ok"] else 1

    db = (db_factory or _write_db)()
    control = aq.get_control(db) or {}
    mode = control.get("mode") or aq.MODE_DISABLED
    cutoff = aq.launch_cutoff(control)

    if args.cmd in ("release-legacy-writer", "recover-legacy-writer"):
        state = aq.legacy_writer_state(control, now_utc)
        if args.cmd == "release-legacy-writer":
            if not args.acknowledge_writer_stopped:
                print("refused: requires --acknowledge-writer-stopped; never expire a live writer", file=sys.stderr)
                return 2
            if control.get("legacy_writer_token") != args.token:
                print("refused: writer token changed or absent", file=sys.stderr)
                return 2
        awarding = 1 if control.get("legacy_award_inflight") else 0
        if not args.commit:
            print(json.dumps({"dry_run": True, "writer": state, "award_inflight": control.get("legacy_award_inflight")}, default=str))
            return 0 if state["in_flight"] or awarding else 2
        if args.cmd == "recover-legacy-writer" and args.release_awarding:
            if not args.acknowledge_writer_stopped or state["state"] == aq.WRITER_ACTIVE:
                print("refused: --release-awarding needs --acknowledge-writer-stopped and no active lease", file=sys.stderr)
                return 2
            released = db[aq.CONTROL_COLLECTION].update_one(
                {"_id": aq.CONTROL_ID, "legacy_award_inflight": control.get("legacy_award_inflight")},
                {"$unset": {"legacy_award_inflight": ""},
                 "$push": {"legacy_writer_recoveries": {"released_award_inflight": True, "operator": args.operator,
                                                        "at": now_utc}}})
            print(json.dumps({"awarding_released": released.modified_count}))
            if not state["in_flight"]:
                return 0
        result = aq.recover_legacy_writer(db, now_utc=now_utc, operator=args.operator,
                                          acknowledged_stopped=args.acknowledge_writer_stopped, token=args.token)
        print(json.dumps({"released": result["recovered"], "writer": result}, default=str))
        return 0 if result["recovered"] else 2

    if args.cmd == "snapshot-launch-cohort":
        if not args.commit:
            print(json.dumps({"dry_run": True, "cohort": fwd.cohort_status(db, control, live=True)}, default=str))
            return 0
        print(json.dumps({"committed": True, "cohort": fwd.snapshot_launch_cohort(db, now_utc=now_utc, cutoff=cutoff)},
                         default=str))
        return 0

    if args.cmd == "upgrade-rule":
        if (control.get("mode") != aq.MODE_DISABLED or "launch_cutoff_utc" in control or control.get("legacy_writer_token")
                or control.get("migration_lease_token")
                or db.qualified_events.count_documents({"rule_version": aq.LEGACY_RULE_VERSION})):
            print("refused: transition requires disabled, unlaunched, zero v1 qualifications and no in-flight writer/migration", file=sys.stderr)
            return 2
        if control.get("rule_version") == aq.RULE_VERSION:
            print(json.dumps({"already_current": True, "rule_version": aq.RULE_VERSION}))
            return 0
        if not control:
            print("refused: configure an authoritative source first", file=sys.stderr)
            return 2
        update = {"$set": {"rule_version": aq.RULE_VERSION,
                            "qualification_gate_version": aq.QUALIFICATION_GATE_VERSION, "updated_at": now_utc},
                  "$push": {"rule_revision_transitions": {"from": control.get("rule_version", aq.LEGACY_RULE_VERSION),
                            "to": aq.RULE_VERSION, "at": now_utc, "operator": args.operator}}}
        if not args.commit:
            print(json.dumps({"dry_run": True, "would_update": update}, default=str))
            return 0
        result = db[aq.CONTROL_COLLECTION].update_one(
            {"_id": aq.CONTROL_ID, "mode": aq.MODE_DISABLED, "launch_cutoff_utc": {"$exists": False},
             "rule_version": control.get("rule_version"), "source_config": control.get("source_config"),
             "legacy_writer_token": {"$exists": False}, "migration_lease_token": {"$exists": False}}, update)
        print(json.dumps({"transitioned": bool(result.modified_count), "rule_version": aq.RULE_VERSION}))
        return 0 if result.modified_count else 2

    if args.cmd == "configure-source":
        if mode == aq.MODE_ACTIVE:
            print("refused: pause the rule before changing its source", file=sys.stderr)
            return 2
        source_config = {
            "source": aq.SOURCE_MARKETING,
            "code_column": args.code_column,
            "account_column": args.account_column,
            "redeemed_at_column": args.redeemed_at_column,
            "status_column": args.status_column,
            "success_values": [v.strip().lower() for v in args.success_values.split(",") if v.strip()],
            "source_timezone": args.source_timezone,
            "account_namespace": aq.normalize_namespace(args.account_namespace) if args.account_namespace else None,
            "namespace_column": args.namespace_column,
            "campaign_ids": [c.strip() for c in (args.campaign_ids or "").split(",") if c.strip()],
            "configured_at": now_utc,
            "configured_by": args.operator,
        }
        problems = aq.validate_source_config(source_config)
        if problems:
            print(f"refused: invalid source config {problems}", file=sys.stderr)
            return 2
        frozen = control.get("seeded_identity_config")
        if (control.get("seed_completed_at") or cutoff is not None) and aq.identity_config(source_config) != (
            frozen if frozen is not None else aq.identity_config(control.get("source_config"))
        ):
            print("refused: account identity (account/code column, namespace) is frozen once the registry is "
                  "seeded or the rule launched; changing it would let credited accounts qualify again",
                  file=sys.stderr)
            return 2
        return _apply(
            db, args,
            {"$set": {"source_config": source_config, "unlinked_account_policy": args.unlinked_account_policy,
                      "updated_at": now_utc},
             "$setOnInsert": {"mode": aq.MODE_DISABLED, "rule_version": aq.RULE_VERSION, "created_at": now_utc}},
            upsert=True,
        )

    if args.cmd == "configure-databot-source":
        if mode == aq.MODE_ACTIVE:
            print("refused: pause the rule before changing its source", file=sys.stderr)
            return 2
        source_config = {
            "source": aq.SOURCE_DATABOT_UIM,
            **aq.DATABOT_FIXED_COLUMNS,
            "namespace_column": None,
            "account_namespace": aq.normalize_namespace(args.account_namespace),
            "source_timezone": args.source_timezone,
            "success_basis": aq.SUCCESS_BASIS_SOURCE_CONTRACT,
            "success_attestation": {"by": args.attested_by, "reference": args.attestation_ref,
                                    "recorded_at": now_utc, "recorded_by": args.operator},
            "legacy_time_basis": args.legacy_time_basis,
            "campaign_ids": [c.strip() for c in (args.campaign_ids or "").split(",") if c.strip()],
            "configured_at": now_utc,
            "configured_by": args.operator,
        }
        problems = aq.validate_source_config(source_config)
        if problems:
            print(f"refused: invalid source config {problems}", file=sys.stderr)
            return 2
        frozen = control.get("seeded_identity_config")
        if (control.get("seed_completed_at") or cutoff is not None) and aq.identity_config(source_config) != (
            frozen if frozen is not None else aq.identity_config(control.get("source_config"))
        ):
            print("refused: account identity is frozen once the registry is seeded or the rule launched",
                  file=sys.stderr)
            return 2
        return _apply(
            db, args,
            {"$set": {"source_config": source_config, "unlinked_account_policy": args.unlinked_account_policy,
                      "updated_at": now_utc},
             "$setOnInsert": {"mode": aq.MODE_DISABLED, "rule_version": aq.RULE_VERSION, "created_at": now_utc}},
            upsert=True,
        )

    if args.cmd == "configure-coverage-policy":
        if mode == aq.MODE_ACTIVE:
            print("refused: pause the rule before changing a decision policy", file=sys.stderr)
            return 2
        if (control.get("source_config") or {}).get("source") != aq.SOURCE_DATABOT_UIM:
            print("refused: configure the Databot UIM source first", file=sys.stderr)
            return 2
        existing = (control.get("source_config") or {}).get("absence_rejection") or {}
        if args.disable:
            policy = {**existing, "enabled": False, "disabled_at": now_utc, "disabled_by": args.operator}
        else:
            policy = {
                "enabled": True,
                "coverage_basis": args.basis,
                "welcome_redemption_window_days": args.welcome_redemption_window_days,
                "attestation": {"by": args.attested_by, "reference": args.attestation_ref,
                                "recorded_at": now_utc, "recorded_by": args.operator},
            }
            _, problems = uim_coverage.absence_policy({"source_config": {"absence_rejection": policy}})
            if problems:
                print(f"refused: invalid coverage policy {problems}", file=sys.stderr)
                return 2
        same = (existing.get("enabled"), existing.get("coverage_basis"), existing.get("welcome_redemption_window_days"),
                (existing.get("attestation") or {}).get("by"), (existing.get("attestation") or {}).get("reference")) == (
            policy.get("enabled"), policy.get("coverage_basis"), policy.get("welcome_redemption_window_days"),
            (policy.get("attestation") or {}).get("by"), (policy.get("attestation") or {}).get("reference"))
        if same:
            print(json.dumps({"no_change": True, "absence_rejection": existing}, default=str))
            return 0
        return _apply(
            db, args,
            {"$set": {"source_config.absence_rejection": policy, "updated_at": now_utc},
             "$push": {"absence_policy_history": {"policy": policy, "at": now_utc, "operator": args.operator,
                                                   "reason": args.reason}}},
        )

    if args.cmd == "sync-evidence":
        import uim_redemption_sync

        if not args.commit:
            print(json.dumps({"dry_run": True, "sync": uim_redemption_sync.sync_health(db, now_utc=now_utc)},
                             indent=2, default=str))
            return 0
        cfg = uim_redemption_sync.feed_config()
        try:
            fetch = uim_redemption_sync.http_fetcher(cfg["base_url"], cfg["token"], timeout=cfg["timeout"])
        except uim_redemption_sync.FeedUnavailable as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        result = uim_redemption_sync.sync_once(db, fetch=fetch, now_utc=now_utc, max_rows=args.max_rows)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("ok") else 1

    if args.cmd in ("activate", "resume"):
        if args.cmd == "activate" and cutoff is not None:
            print(f"refused: launch cutoff already persisted ({cutoff.isoformat()}); it is immutable — use resume",
                  file=sys.stderr)
            return 2
        if args.cmd == "resume" and (cutoff is None or mode != aq.MODE_PAUSED):
            print("refused: resume only applies to a launched, paused rule", file=sys.stderr)
            return 2
        proposed = None
        if args.cmd == "activate":
            try:
                proposed = _parse_cutoff(args.cutoff)
            except ValueError as exc:
                print(f"refused: {exc}", file=sys.stderr)
                return 2
        report = preflight_report(db, now_utc=now_utc, cutoff=proposed)
        if not report["ok"]:
            print(json.dumps({"refused": "preflight_failed", "preflight": report}, indent=2, default=str))
            return 2
        if args.cmd == "resume":
            return _apply(db, args, {"$set": {"mode": aq.MODE_ACTIVE, "resumed_at": now_utc,
                                              "resumed_by": args.operator, "updated_at": now_utc}})
        if mode != aq.MODE_DISABLED:
            print("refused: activation requires disabled mode", file=sys.stderr)
            return 2
        if fwd.is_forward_only(control) and not args.acknowledge_unknown_history:
            print("refused: forward-only activation requires --acknowledge-unknown-history", file=sys.stderr)
            return 2
        try:
            new_cutoff = _parse_cutoff(args.cutoff)
        except ValueError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        if new_cutoff < now_utc:
            print("refused: cutoff is in the past; legacy awards already made after it would contradict it",
                  file=sys.stderr)
            return 2
        # Close admission and persist the immutable cutoff in ONE write. For
        # scheduled launch this conservatively parks legacy rows immediately;
        # no admitted writer can straddle the future cutoff.
        update = {"$set": {"mode": aq.MODE_ACTIVE, "launch_cutoff_utc": new_cutoff,
                            "legacy_admission_closed": True, "pending_cohort_policy": "all_active_pending_manual",
                            "rule_version": aq.RULE_VERSION, "activated_at": now_utc,
                            "activated_by": args.operator, "updated_at": now_utc}}
        if not args.commit:
            print(json.dumps({"dry_run": True, "would_update": update, "preflight": report}, default=str))
            return 0
        cas = {"_id": aq.CONTROL_ID, "mode": aq.MODE_DISABLED, "rule_version": aq.RULE_VERSION,
             "launch_cutoff_utc": {"$exists": False}, "legacy_writer_token": {"$exists": False},
             "migration_lease_token": {"$exists": False}, "source_config": control.get("source_config"),
             # same document, same atomic step: no legacy award may be in flight
             "legacy_award_inflight": {"$exists": False},
             "cancelled_cohort_cutoff": {"$exists": False},
             "seed_summary": control.get("seed_summary"), "seed_completed_at": control.get("seed_completed_at"),
             "forward_only_marker": control.get("forward_only_marker"),
             "history_policy": control.get("history_policy")}
        if fwd.is_forward_only(control):
            cas["launch_cohort.state"] = fwd.STATE_READY  # snapshot not mid-rebuild at the instant of the CAS
        result = db[aq.CONTROL_COLLECTION].update_one(cas, update)
        if not result.modified_count:
            print("refused: control changed or writer/migration in flight; rerun preflight", file=sys.stderr)
            return 2
        marked = aq.mark_activation_cohort(db, control=aq.get_control(db), now_utc=now_utc)
        print(json.dumps({"committed": True, "pending_marked": marked, "control": _public_control(aq.get_control(db))}, default=str))
        return 0

    if args.cmd == "pause":
        if cutoff is None:
            print("refused: nothing launched to pause", file=sys.stderr)
            return 2
        return _apply(db, args, {"$set": {"mode": aq.MODE_PAUSED, "paused_at": now_utc, "paused_by": args.operator,
                                          "pause_reason": args.reason, "updated_at": now_utc}})

    if args.cmd == "cancel-scheduled":
        if cutoff is None or now_utc >= cutoff:
            print("refused: only an activation whose cutoff is still in the future can be cancelled", file=sys.stderr)
            return 2
        if db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}):
            print("refused: v1 qualifications already exist", file=sys.stderr)
            return 2
        if not args.commit:
            print(json.dumps({"dry_run": True, "would_cancel_cutoff": cutoff.isoformat()}))
            return 0
        with aq.cohort_writer_guard(db, cutoff=cutoff, now_utc=now_utc) as token:
            if token is None:
                print("refused: cohort/control writer in flight or cutoff changed; retry", file=sys.stderr)
                return 2
            result = db[aq.CONTROL_COLLECTION].update_one(
                {"_id": aq.CONTROL_ID, "launch_cutoff_utc": cutoff, "legacy_writer_token": token,
                 "$and": [{"launch_cutoff_utc": {"$gt": now_fn()}}]},
                {"$set": {"mode": aq.MODE_DISABLED, "updated_at": now_utc,
                          "cancelled_cohort_cutoff": cutoff},
                 "$unset": {"launch_cutoff_utc": "", "legacy_admission_closed": "",
                            "launch_handoff_completed_cutoff": "", "launch_handoff_completed_at": ""}})
            if not result.modified_count:
                return 2
            aq.restore_activation_cohort(db, cutoff=cutoff)
            # The snapshot belongs to the cancelled cutoff; a later launch rebuilds it from scratch.
            db[aq.COHORT_COLLECTION].delete_many({})
            db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$unset": {"launch_cohort": ""}})
            db[aq.CONTROL_COLLECTION].update_one(
                {"_id": aq.CONTROL_ID, "legacy_writer_token": token, "cancelled_cohort_cutoff": cutoff},
                {"$unset": {"cancelled_cohort_cutoff": ""}})
        print(json.dumps({"cancelled": True}))
        return 0

    if args.cmd == "requeue":
        from bson import ObjectId

        evidence_id = ObjectId(args.evidence_id)
        row = db[aq.EVIDENCE_COLLECTION].find_one({"_id": evidence_id}, {"status": 1, "reason": 1})
        if not row:
            print("refused: evidence not found", file=sys.stderr)
            return 2
        if not args.commit:
            print(json.dumps({"dry_run": True, "evidence": row}, default=str))
            return 0
        ok = aq.requeue_evidence(db, evidence_id=evidence_id, now_utc=now_utc)
        print(json.dumps({"requeued": ok}))
        return 0 if ok else 2

    if args.cmd == "void-batch":
        batch = db.marketing_upload_batches.find_one({"upload_batch_id": args.upload_batch_id}, {"_id": 1})
        if not batch:
            print("refused: upload batch not found", file=sys.stderr)
            return 2
        affected = db[aq.EVIDENCE_COLLECTION].count_documents({"source_batch_ids": args.upload_batch_id})
        if not args.commit:
            print(json.dumps({"dry_run": True, "evidence_rows_referencing_batch": affected}))
            return 0
        print(json.dumps(aq.void_upload_batch(db, upload_batch_id=args.upload_batch_id, reason=args.reason,
                                              now_utc=now_utc)))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
