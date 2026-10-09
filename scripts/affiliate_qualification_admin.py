#!/usr/bin/env python3
"""Operate the welcome_redemption_v1 affiliate qualification rule.

DRY RUN BY DEFAULT. Every state-changing subcommand prints what it would do
and writes nothing unless ``--commit`` is given. ``status`` and ``preflight``
are always read-only (secondary-preferred connection, no index creation).

Subcommands
-----------
status                      control doc, readiness, evidence/registry counts
preflight                   exit 0 only if activation is safe right now
configure-source ...        persist the authoritative redemption source mapping
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

import affiliate_qualification as aq  # noqa: E402


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
    return {k: v for k, v in control.items() if k != "_id"}


def preflight_report(db, *, now_utc: datetime) -> dict:
    control = aq.get_control(db) or {}
    report = {
        "indexes": aq.index_readiness(db),
        "integration": aq.integration_readiness(db, control.get("source_config"), now_utc=now_utc),
        "migration_seeded": bool(control.get("seed_completed_at")),
        "identity_config_matches_seed": bool(control.get("seed_completed_at"))
        and control.get("seeded_identity_config") == aq.identity_config(control.get("source_config")),
        "consistency": aq.consistency_report(db, now_utc=now_utc),
    }
    report["ok"] = bool(
        report["indexes"]["ok"]
        and report["integration"]["ok"]
        and report["migration_seeded"]
        and report["identity_config_matches_seed"]
        and report["consistency"]["ok"]
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
        "v1_qualified_events": db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}),
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
    sync = sub.add_parser("sync-evidence", help="one Databot evidence sync pass (writes only uim_redemption_*)")
    sync.add_argument("--max-rows", type=int, default=5000)
    sub.add_parser("sync-status")

    act = sub.add_parser("activate")
    act.add_argument("--cutoff", required=True, help="ISO-8601 with offset")
    pause = sub.add_parser("pause")
    pause.add_argument("--reason", required=True)
    sub.add_parser("resume")
    sub.add_parser("cancel-scheduled")
    rq = sub.add_parser("requeue")
    rq.add_argument("--evidence-id", required=True)
    vb = sub.add_parser("void-batch")
    vb.add_argument("--upload-batch-id", required=True)
    vb.add_argument("--reason", required=True)
    for p in (cfg, dcfg, sync, act, pause, sub.choices["resume"], sub.choices["cancel-scheduled"], rq, vb):
        p.add_argument("--commit", action="store_true", help="write (default: dry run)")
        p.add_argument("--operator", default=os.environ.get("USER") or "unknown")
    args = parser.parse_args(argv)
    now_utc = now_fn()

    if args.cmd == "sync-status":
        import uim_redemption_sync

        db = (read_only_db_factory or _read_only_db)()
        print(json.dumps(uim_redemption_sync.sync_health(db, now_utc=now_utc), indent=2, default=str))
        return 0

    if args.cmd in ("status", "preflight"):
        db = (read_only_db_factory or _read_only_db)()
        report = status_report(db, now_utc=now_utc) if args.cmd == "status" else preflight_report(db, now_utc=now_utc)
        print(json.dumps(report, indent=2, default=str))
        return 0 if args.cmd == "status" or report["ok"] else 1

    db = (db_factory or _write_db)()
    control = aq.get_control(db) or {}
    mode = control.get("mode") or aq.MODE_DISABLED
    cutoff = aq.launch_cutoff(control)

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
        report = preflight_report(db, now_utc=now_utc)
        if not report["ok"]:
            print(json.dumps({"refused": "preflight_failed", "preflight": report}, indent=2, default=str))
            return 2
        if args.cmd == "resume":
            return _apply(db, args, {"$set": {"mode": aq.MODE_ACTIVE, "resumed_at": now_utc,
                                              "resumed_by": args.operator, "updated_at": now_utc}})
        try:
            new_cutoff = _parse_cutoff(args.cutoff)
        except ValueError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        if new_cutoff < now_utc - timedelta(minutes=1):
            print("refused: cutoff is in the past; legacy awards already made after it would contradict it",
                  file=sys.stderr)
            return 2
        return _apply(db, args, {"$set": {"mode": aq.MODE_ACTIVE, "launch_cutoff_utc": new_cutoff,
                                          "rule_version": aq.RULE_VERSION, "activated_at": now_utc,
                                          "activated_by": args.operator, "updated_at": now_utc}})

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
        return _apply(db, args, {"$set": {"mode": aq.MODE_DISABLED, "updated_at": now_utc},
                                 "$unset": {"launch_cutoff_utc": ""}})

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
