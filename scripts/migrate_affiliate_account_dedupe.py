#!/usr/bin/env python3
"""Seed the lifetime gaming-account registry from historical qualifications.

DRY RUN BY DEFAULT: read-only, secondary-preferred connection, no index
creation, no writes. ``--apply`` uses a primary connection and:

1. creates the welcome_redemption_v1 indexes that are missing;
2. seeds ``affiliate_account_registry`` (state ``seeded``) with every gaming
   account that historical evidence RELIABLY ties to an already-qualified
   invitee;
3. stamps ``seed_completed_at`` + a counts-only summary on the control doc
   (activation refuses until this exists).

It never modifies or deletes ``qualified_events``, ``referral_events``,
``affiliate_ledger`` or ``xp_events``: historical counts and earned rewards are
reported before and after and must be identical.

"Reliable" means: the invitee's Welcome code(s) map to exactly one recipient
(that invitee), the committed source rows for those codes report a successful
redemption on exactly one canonical account, and that account does not
contradict the invitee's verified UIM linkage. Everything else is reported as
unresolved or conflicting and left out — no mapping is guessed.
``--seed-from-linkage`` additionally seeds invitees with no redemption rows
but exactly one verified linked account (off by default).

Historical duplicates (one account behind several qualified invitees) are
reported; the earliest qualification holds the registry entry and none of the
qualifications is touched.

Output is counts plus masked samples only: no voucher codes, no raw account
ids, masked user ids. Idempotent: a re-run seeds nothing new.

Usage
-----
    MONGO_URL=... python scripts/migrate_affiliate_account_dedupe.py              # dry run
    MONGO_URL=... python scripts/migrate_affiliate_account_dedupe.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import affiliate_qualification as aq  # noqa: E402
from pymongo.errors import DuplicateKeyError  # noqa: E402

SAMPLE_LIMIT = 20


def _write_db():
    from pymongo import MongoClient

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise SystemExit("MONGO_URL is not configured")
    return MongoClient(mongo_url)[os.environ.get("MONGO_DB", "referral_bot")]


def _read_only_db():
    from scripts.verify_affiliate_reward_plan import _read_only_db as factory

    return factory()


def _mask_uid(uid) -> str:
    return aq.mask_value(str(uid))


def history_snapshot(db, *, as_of: datetime) -> dict:
    """Counts the migration must leave unchanged. Scoped to records that
    existed before the run started, so a live app writing new
    qualifications meanwhile cannot make the comparison flap."""
    by_month = defaultdict(int)
    for q in db.qualified_events.find({"qualified_at": {"$lt": as_of}}, {"qualified_at": 1}):
        by_month[aq.kl_month_key(q["qualified_at"])] += 1
    return {
        "qualified_events_total": db.qualified_events.count_documents({"qualified_at": {"$lt": as_of}}),
        "qualified_events_by_kl_month": dict(sorted(by_month.items())),
        "referral_settled_total": db.referral_events.count_documents(
            {"event": "referral_settled", "occurred_at": {"$lt": as_of}}
        ),
        "referral_award_events_total": db.referral_award_events.count_documents({"created_at_utc": {"$lt": as_of}}),
        "affiliate_ledger_issued_created_before": db.affiliate_ledger.count_documents(
            {"status": "ISSUED", "created_at": {"$lt": as_of}}
        ),
    }


def _welcome_codes(db) -> dict[str, set[int]]:
    """code -> recipients, from both Welcome issuance stores."""
    codes: dict[str, set[int]] = defaultdict(set)
    for row in db.voucher_pools.find({"pool_id": aq.WELCOME_POOL_ID, "status": "issued"},
                                     {"code": 1, "issued_to_user_id": 1, "issued_to": 1}):
        raw = row.get("issued_to_user_id")
        if raw in (None, ""):
            raw = row.get("issued_to")
        code = aq.normalize_code(row.get("code"))
        try:
            if code:
                codes[code].add(int(raw))
        except (TypeError, ValueError):
            continue
    for row in db.new_joiner_claims.find({"code": {"$nin": [None, ""]}}, {"code": 1, "uid": 1}):
        code = aq.normalize_code(row.get("code"))
        try:
            if code:
                codes[code].add(int(row.get("uid")))
        except (TypeError, ValueError):
            continue
    return codes


def _redemptions_by_code(db, config: dict, welcome_codes: dict) -> dict[str, set]:
    """code -> {account_key | "!<reason>"} from successful, committed rows of
    the configured source. Uses the live parser (aq.parse_source_row) and the
    live identity rule (aq.build_evidence_doc), so a seeded account key is
    byte-identical to the key live processing later computes."""
    out: dict[str, set] = defaultdict(set)
    for _batch_id, row in aq.iter_committed_source_rows(db, config):
        obs = aq.parse_source_row(row, config)
        if obs is None or obs.get("observation") == aq.OBS_REMOVED:
            continue
        code = aq.normalize_code(obs["code"])
        if not code or code not in welcome_codes or not obs["redemption_successful"]:
            continue
        doc = aq.build_evidence_doc(
            source=aq.source_name(config), code=code, recipient={}, now_utc=datetime.now(timezone.utc),
            **{k: v for k, v in obs.items() if k != "code"},
        )
        out[code].add(doc["account_key"] or f"!{doc['account_reason']}")
    return out


def build_plan(db, *, config: dict | None, seed_from_linkage: bool) -> dict:
    problems = aq.validate_source_config(config)
    welcome_codes = _welcome_codes(db)
    codes_by_user: dict[int, set[str]] = defaultdict(set)
    for code, uids in welcome_codes.items():
        for uid in uids:
            codes_by_user[uid].add(code)
    redemptions = _redemptions_by_code(db, config, welcome_codes) if not problems else {}

    counts = defaultdict(int)
    samples = defaultdict(list)
    resolved = []  # (account_key, qualified_event, basis)

    def note(kind, uid):
        counts[kind] += 1
        if len(samples[kind]) < SAMPLE_LIMIT:
            samples[kind].append(_mask_uid(uid))

    for q in db.qualified_events.find({"rule_version": {"$ne": aq.RULE_VERSION}},
                                      {"invitee_id": 1, "referrer_id": 1, "qualified_at": 1}):
        uid = q.get("invitee_id")
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            note("unresolved_invalid_invitee_id", uid)
            continue
        user_codes = codes_by_user.get(uid, set())
        if any(len(welcome_codes[c]) > 1 for c in user_codes):
            note("conflicting_code_recipient", uid)
            continue
        accounts = set()
        for code in user_codes:
            accounts |= redemptions.get(code, set())
        bad = {a for a in accounts if a.startswith("!")}
        accounts -= bad
        user_doc = db.users.find_one({"user_id": uid}, {"linked_gaming_accounts": 1})
        links = aq._linked_accounts(user_doc)
        basis = "redemption_evidence"
        if not accounts:
            if bad:
                note("unresolved_unusable_account_value", uid)
                continue
            if seed_from_linkage and len(links) == 1:
                ns = (config or {}).get("account_namespace")
                key, _ = aq.canonical_account_key(next(iter(links)), ns)
                if key:
                    accounts, basis = {key}, "verified_linkage_single_account"
            if not accounts:
                note("unresolved_no_redemption_evidence" if user_codes else "unresolved_no_welcome_code", uid)
                continue
        if len(accounts) > 1:
            note("conflicting_multiple_accounts", uid)
            continue
        account_key = next(iter(accounts))
        if links and aq.account_id_from_key(account_key) not in links:
            note("conflicting_linkage", uid)
            continue
        resolved.append((account_key, q, basis))
        counts["resolved"] += 1

    by_account = defaultdict(list)
    for account_key, q, basis in resolved:
        by_account[account_key].append((q, basis))
    seeds = []
    duplicate_accounts = 0
    duplicate_qualifications = 0
    for account_key, items in by_account.items():
        items.sort(key=lambda it: (aq._aware_utc(it[0].get("qualified_at")) or datetime.max.replace(tzinfo=timezone.utc),
                                   str(it[0].get("_id"))))
        if len(items) > 1:
            duplicate_accounts += 1
            duplicate_qualifications += len(items) - 1
            if len(samples["historical_duplicate_account"]) < SAMPLE_LIMIT:
                samples["historical_duplicate_account"].append(
                    {"account": aq.mask_value(account_key), "invitees": [_mask_uid(i[0].get("invitee_id")) for i in items]}
                )
        holder, basis = items[0]
        seeds.append({
            "account_key": account_key,
            "state": aq.REG_SEEDED,
            "invitee_id": int(holder.get("invitee_id")),
            "inviter_id": holder.get("referrer_id"),
            "qualified_event_id": holder.get("_id"),
            "seed_basis": basis,
            "historical_qualification_count": len(items),
            "rule_version": aq.RULE_VERSION,
        })
    return {
        "source_config_problems": problems,
        "counts": dict(sorted(counts.items())),
        "accounts_to_seed": len(seeds),
        "historical_duplicate_accounts": duplicate_accounts,
        "historical_duplicate_extra_qualifications": duplicate_qualifications,
        "samples_masked": dict(samples),
        "_seeds": seeds,
    }


def apply_seeds(db, seeds: list[dict], *, now_utc: datetime) -> dict:
    out = {"seeded": 0, "already_seeded": 0, "held_by_other": 0}
    for seed in seeds:
        try:
            db[aq.REGISTRY_COLLECTION].insert_one({**seed, "seeded_at": now_utc})
            out["seeded"] += 1
        except DuplicateKeyError:
            holder = db[aq.REGISTRY_COLLECTION].find_one({"account_key": seed["account_key"]}, {"invitee_id": 1}) or {}
            if holder.get("invitee_id") == seed["invitee_id"]:
                out["already_seeded"] += 1
            else:
                out["held_by_other"] += 1
    return out


def run(db, *, apply: bool, seed_from_linkage: bool, now_utc: datetime) -> dict:
    control = aq.get_control(db) or {}
    report = {"apply": apply, "now_utc": now_utc.isoformat(), "history_before": history_snapshot(db, as_of=now_utc)}
    report["indexes_before"] = aq.index_readiness(db)
    if apply:
        report["indexes_created"] = aq.ensure_indexes(db)
    plan = build_plan(db, config=control.get("source_config"), seed_from_linkage=seed_from_linkage)
    seeds = plan.pop("_seeds")
    report["plan"] = plan
    if apply:
        report["seed_result"] = apply_seeds(db, seeds, now_utc=now_utc)
    report["indexes_after"] = aq.index_readiness(db)
    report["consistency"] = aq.consistency_report(db, now_utc=now_utc)
    report["history_after"] = history_snapshot(db, as_of=now_utc)
    report["history_unchanged"] = report["history_after"] == report["history_before"]
    if apply and report["history_unchanged"] and report["indexes_after"]["ok"] and not plan["source_config_problems"]:
        db[aq.CONTROL_COLLECTION].update_one(
            {"_id": aq.CONTROL_ID},
            {
                "$set": {
                    "seed_completed_at": now_utc,
                    "seeded_identity_config": aq.identity_config(control.get("source_config")),
                    "seed_summary": {
                        "accounts_seeded": report["seed_result"]["seeded"],
                        "accounts_already_seeded": report["seed_result"]["already_seeded"],
                        "counts": plan["counts"],
                        "historical_duplicate_accounts": plan["historical_duplicate_accounts"],
                    },
                    "updated_at": now_utc,
                },
                "$setOnInsert": {"mode": aq.MODE_DISABLED, "rule_version": aq.RULE_VERSION, "created_at": now_utc},
            },
            upsert=True,
        )
        report["seed_marker_written"] = True
    else:
        report["seed_marker_written"] = False
    return report


def main(argv=None, *, db_factory=None, read_only_db_factory=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="create indexes, seed registry (default: dry run)")
    parser.add_argument("--seed-from-linkage", action="store_true",
                        help="also seed invitees with no redemption rows but exactly one verified linked account")
    parser.add_argument("--output", help="also write the JSON report to this path")
    args = parser.parse_args(argv)
    db = (db_factory or _write_db)() if args.apply else (read_only_db_factory or _read_only_db)()
    report = run(db, apply=args.apply, seed_from_linkage=args.seed_from_linkage, now_utc=datetime.now(timezone.utc))
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text)
    print(text)
    if args.apply and not report["history_unchanged"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
