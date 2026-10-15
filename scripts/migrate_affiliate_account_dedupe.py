#!/usr/bin/env python3
"""Seed the lifetime gaming-account registry from historical qualifications.

DRY RUN BY DEFAULT: read-only, secondary-preferred connection, no index
creation, no writes. ``--apply`` uses a primary connection and:

1. refuses incomplete identity/source coverage before any write;
2. creates required indexes and idempotently reserves deterministic historical
   accounts under an exclusive migration lease;
3. rechecks coverage/history/ownership and writes a versioned completion marker
   only when every historical qualification is mapped safely.

It never modifies or deletes ``qualified_events``, ``referral_events``,
``affiliate_ledger`` or ``xp_events``: historical counts and earned rewards are
reported before and after and must be identical.

"Reliable" means: the invitee's Welcome code(s) map to exactly one recipient
(that invitee), the committed source rows for those codes report a successful
redemption on exactly one canonical account, and that account does not
contradict the invitee's verified UIM linkage. Issuance must precede redemption,
and redemption must not exceed the live five-minute future-time tolerance.
Everything else is reported as
unresolved or conflicting and left out — no mapping is guessed.
``--seed-from-linkage`` additionally seeds from exactly one verified linked
account when redemption is missing or held only for time provenance (off by
default). All usable account observations still participate in conflict checks.
See docs/historical_affiliate_account_migration.md for supported sources,
coverage safety, masked reports and the crash/retry protocol.

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
import hashlib
import json
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import affiliate_forward_only as fwd  # noqa: E402
import affiliate_qualification as aq  # noqa: E402
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError  # noqa: E402

SAMPLE_LIMIT = 20
PLANNER_VERSION = "historical_account_coverage_v3"
BULK_SIZE = 500


def _cutoff_iso(text):
    """Normalized UTC form of an operator-supplied cutoff, or None (absent/unparseable)."""
    if isinstance(text, datetime):
        return aq._aware_utc(text).isoformat()
    try:
        return fwd.kl_cutoff(text).isoformat() if text not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _seed_identity_fingerprint(row):
    # Audit seeded coverage without exposing raw account or invitee IDs in
    # admin output. Additional discoveries after launch do not invalidate it.
    return hashlib.sha256(json.dumps([row.get("account_key"), row.get("invitee_id"),
                                      str(row.get("qualified_event_id"))]).encode()).hexdigest()


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
        "xp_events_created_before": db.xp_events.count_documents({"created_at": {"$lt": as_of}}),
        "referral_award_events_total": db.referral_award_events.count_documents({"created_at_utc": {"$lt": as_of}}),
        "affiliate_ledger_issued_created_before": db.affiliate_ledger.count_documents(
            {"status": "ISSUED", "created_at": {"$lt": as_of}}
        ),
    }


def _recipient_id(raw):
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or not str(raw).isdigit():
        return None
    return int(raw) if int(raw) > 0 else None


def _welcome_codes(db) -> dict[str, set[int]]:
    """code -> recipients, from pool, legacy claims and issued Welcome ledger."""
    codes: dict[str, set[int]] = defaultdict(set)
    for row in db.voucher_pools.find({"pool_id": aq.WELCOME_POOL_ID, "status": "issued"},
                                     {"code": 1, "issued_to_user_id": 1, "issued_to": 1}):
        code = aq.normalize_code(row.get("code"))
        for raw in (row.get("issued_to_user_id"), row.get("issued_to")):
            try:
                if code and raw not in (None, ""):
                    uid = _recipient_id(raw)
                    codes[code].add(uid if uid is not None else -1)
            except (TypeError, ValueError):
                if code:
                    codes[code].add(-1)  # ambiguous/unparseable recipient; never a guessed owner
    for row in db.new_joiner_claims.find({"code": {"$nin": [None, ""]}}, {"code": 1, "uid": 1}):
        code = aq.normalize_code(row.get("code"))
        try:
            if code:
                uid = _recipient_id(row.get("uid"))
                codes[code].add(uid if uid is not None else -1)
        except (TypeError, ValueError):
            continue
    for row in db.affiliate_ledger.find({"status": "ISSUED", "dedup_key": {"$regex": r"^WELCOME:"}},
                                        {"dedup_key": 1, "voucher_code": 1, "user_id": 1}):
        code = aq.normalize_code(row.get("voucher_code"))
        raw_ids = [row.get("user_id"), str(row.get("dedup_key", "")).partition(":")[2]]
        for raw in raw_ids:
            try:
                if code and raw not in (None, "") and not isinstance(raw, bool):
                    uid = _recipient_id(raw)
                    codes[code].add(uid if uid is not None else -1)
            except (TypeError, ValueError):
                continue
    return codes


def _migration_recipient(db, code, welcome_codes):
    recipient = aq.resolve_welcome_recipient(db, code)
    if recipient.get("status") != "unknown" or len(welcome_codes.get(code, set())) != 1:
        return recipient
    uid = next(iter(welcome_codes[code]))
    rows = list(db.affiliate_ledger.find(
        {"dedup_key": f"WELCOME:{uid}", "status": "ISSUED", "voucher_code": code},
        {"user_id": 1, "issued_at": 1}).limit(2))
    # Audited Welcome ledger writer is an issuance store. It can outlive the
    # pool row; ambiguity, missing issuance time and cross-pool codes still fail.
    if len(rows) != 1 or str(rows[0].get("user_id", uid)) != str(uid):
        return {"status": "ambiguous"}
    return {"status": "ok", "user_id": uid, "issued_at": aq._aware_utc(rows[0].get("issued_at")),
            "sources": ["affiliate_ledger"]}


def _redemptions_by_code(db, config: dict, welcome_codes: dict, *, now_utc: datetime, identity_candidates=None, resolution_sources=None) -> dict[str, set]:
    """code -> {account_key | "!<reason>"} from successful, committed rows of
    the configured source. Uses the live parser (aq.parse_source_row) and the
    live identity rule (aq.build_evidence_doc), so a seeded account key is
    byte-identical to the key live processing later computes.

    A later committed ``removed`` observation (Databot stale deletion)
    cancels that code+account, exactly as live/preview route it to review
    (``source_row_removed_by_later_import``): it is never seeded, and stays
    out even if a later batch re-observes it."""
    out: dict[str, set] = defaultdict(set)
    removed: dict[str, set] = defaultdict(set)
    recipients: dict[str, dict] = {}
    for _batch_id, row in aq.iter_committed_source_rows(db, config):
        obs = aq.parse_source_row(row, config)
        if obs is None:
            continue
        code = aq.normalize_code(obs["code"])
        is_removal = obs.get("observation") == aq.OBS_REMOVED
        if not code or code not in welcome_codes or not (is_removal or obs["redemption_successful"]):
            continue
        # Preserve exact text before considering time: a numeric/coerced or
        # transformed account must not become a trusted linkage corroboration.
        raw = obs["account_raw"]
        key, reason = aq.canonical_account_key(raw, obs["namespace"])
        reason = obs.get("account_reason") or reason
        if reason or not isinstance(raw, str) or aq.account_id_from_key(key) != raw.strip():
            out[code].add("!" + (reason or "account_text_not_exact"))
            continue
        # Unusable time evidence can still contradict independent linkage.
        if identity_candidates is not None and not is_removal:
            identity_candidates[code].add(key)
        if not is_removal:
            if code not in recipients:
                recipients[code] = _migration_recipient(db, code, welcome_codes)
            problem = aq.welcome_issuance_problem(recipients[code], obs.get("redeemed_at"))
            if problem:
                if problem == "missing_redeemed_at":
                    problem = obs.get("redeemed_at_reason") or problem
                out[code].add(f"!{problem}")
                continue
            if obs["redeemed_at"] > now_utc + aq.FUTURE_REDEMPTION_TOLERANCE:
                out[code].add("!future_redemption_time")
                continue
        doc = aq.build_evidence_doc(
            source=aq.source_name(config), code=code, recipient={}, now_utc=now_utc,
            **{k: v for k, v in obs.items() if k != "code"},
        )
        key = doc["account_key"] or f"!{doc['account_reason']}"
        if (key and not key.startswith("!") and
                (not isinstance(obs["account_raw"], str) or aq.account_id_from_key(key) != obs["account_raw"].strip())):
            key = "!account_text_not_exact"
        if resolution_sources is not None and not is_removal and not key.startswith("!"):
            resolution_sources[code].update(recipients[code].get("sources", []))
        if is_removal:
            removed[code].add(key)
        else:
            out[code].add(key)
    for code, keys in removed.items():
        if out.get(code, set()) & keys:
            out[code] -= keys
            out[code].add("!source_row_removed_by_later_import")
    return out


def _chunks(values):
    values = list(values)
    for start in range(0, len(values), BULK_SIZE):
        yield values[start:start + BULK_SIZE]


def _linked_keys(user_doc, config):
    raw = (user_doc or {}).get("linked_gaming_accounts")
    if raw is None:
        return set(), None
    if not isinstance(raw, list):
        return set(), "unresolved_invalid_verified_linkage"
    keys = set()
    for account in raw:
        if not isinstance(account, str):
            return set(), "unresolved_invalid_verified_linkage"
        key, reason = aq.canonical_account_key(account, (config or {}).get("account_namespace"))
        if reason or aq.account_id_from_key(key) != account.strip():
            return set(), "unresolved_invalid_verified_linkage"
        keys.add(key)
    return keys, None


def _source_coverage(db, config, latest, *, now_utc):
    end = None
    problems = []
    if aq.source_name(config) == aq.SOURCE_DATABOT_UIM:
        import uim_redemption_sync as sync
        health = sync.sync_health(db, now_utc=now_utc)
        problems.extend(health["problems"])
        # The sync metadata is explicitly GMT+8 wall clock. This is the latest redemption OBSERVED in the rows
        # (deliberately the stricter bound for history proof - declared final coverage is not used here), NOT
        # permission to reinterpret legacy redemption timestamps.
        end = aq.parse_redeemed_at(health.get("observed_end"), "Asia/Kuala_Lumpur")
    elif not aq.validate_source_config(config):
        for _, row in aq.iter_committed_source_rows(db, config):
            obs = aq.parse_source_row(row, config)
            when = aq._aware_utc((obs or {}).get("redeemed_at"))
            if when and when <= now_utc + aq.FUTURE_REDEMPTION_TOLERANCE:
                end = max(end, when) if end else when
    if end and end > now_utc + aq.FUTURE_REDEMPTION_TOLERANCE:
        problems.append("source_coverage_in_future")
    gap = max(0, (latest - end).total_seconds()) if latest and end else None
    if latest and end is None:
        problems.append("source_coverage_unknown")
    elif latest and end < latest:
        problems.append("source_coverage_older_than_history")
    return {"ok": not problems, "problems": sorted(set(problems)),
            "coverage_end_utc": end.isoformat() if end else None,
            "latest_historical_qualification_utc": latest.isoformat() if latest else None,
            "gap_seconds": gap,
            "gap_start_utc": end.isoformat() if gap else None,
            "gap_end_utc": latest.isoformat() if gap else None}


def build_plan(db, *, config: dict | None, seed_from_linkage: bool, now_utc: datetime | None = None,
               history_policy: str = aq.HISTORY_STRICT, activation_cutoff: str | None = None) -> dict:
    if history_policy not in (aq.HISTORY_STRICT, aq.HISTORY_FORWARD):
        raise ValueError("unsupported history policy")
    now_utc = aq._aware_utc(now_utc or datetime.now(timezone.utc))
    problems = aq.validate_source_config(config)
    welcome_codes = _welcome_codes(db)
    codes_by_user = defaultdict(set)
    for code, uids in welcome_codes.items():
        for uid in uids:
            codes_by_user[uid].add(code)
    identity_candidates, code_sources = defaultdict(set), defaultdict(set)
    redemptions = _redemptions_by_code(db, config, welcome_codes, now_utc=now_utc,
                                     identity_candidates=identity_candidates, resolution_sources=code_sources) if not problems else {}
    historical = list(db.qualified_events.find({"rule_version": {"$ne": aq.RULE_VERSION}},
                           {"invitee_id": 1, "referrer_id": 1, "qualified_at": 1}))
    users, registry = {}, defaultdict(list)
    uids = {int(q["invitee_id"]) for q in historical
            if isinstance(q.get("invitee_id"), (int, str)) and str(q["invitee_id"]).isdigit()}
    for chunk in _chunks(uids):
        for user in db.users.find({"user_id": {"$in": chunk}}, {"user_id": 1, "linked_gaming_accounts": 1}):
            uid = int(user["user_id"])
            if uid in users:
                users[uid] = {"linked_gaming_accounts": "ambiguous duplicate user documents"}
            else:
                users[uid] = user
        for row in db[aq.REGISTRY_COLLECTION].find(
            {"invitee_id": {"$in": chunk},
             "state": {"$in": [aq.REG_SEEDED, aq.REG_CREDITED_LEGACY]}},
            {"invitee_id": 1, "qualified_event_id": 1, "account_key": 1}):
            registry[int(row["invitee_id"])].append(row)
    historical_ids = defaultdict(set)
    for q in historical:
        if str(q.get("invitee_id")).isdigit():
            historical_ids[int(q["invitee_id"])].add(q["_id"])
    counts, samples, resolution_sources = defaultdict(int), defaultdict(list), defaultdict(int)
    diagnostics, evidence_sources = defaultdict(int), defaultdict(int)
    resolved, latest = [], None

    def note(kind, uid):
        counts[kind] += 1
        if len(samples[kind]) < SAMPLE_LIMIT:
            samples[kind].append(_mask_uid(uid))

    time_only = {"!missing_redeemed_at", "!missing_welcome_issued_at", "!redeemed_at_timezone_unverified",
                 "!redeemed_at_date_only", "!redeemed_at_ambiguous_day_month"}
    for q in historical:
        uid = q.get("invitee_id")
        when = aq._aware_utc(q.get("qualified_at"))
        if when is not None:
            latest = max(latest, when) if latest else when
        if when is None or when > now_utc:
            note("unresolved_invalid_qualification_time", uid)
            continue
        try:
            if isinstance(uid, bool) or not str(uid).isdigit():
                raise ValueError()
            uid = int(uid)
        except (TypeError, ValueError):
            note("unresolved_invalid_invitee_id", uid)
            continue
        user_codes = codes_by_user.get(uid, set())
        diagnostics["with_welcome_code" if user_codes else "without_welcome_code"] += 1
        if any(len(welcome_codes[c]) != 1 for c in user_codes):
            note("conflicting_code_recipient", uid)
            continue
        accounts, bad, observed = set(), set(), set()
        for code in user_codes:
            values = redemptions.get(code, set())
            accounts |= {a for a in values if not a.startswith("!")}
            bad |= {a for a in values if a.startswith("!")}
            observed |= identity_candidates.get(code, set())
        if uid not in users:
            diagnostics["users_document_missing"] += 1
        elif "linked_gaming_accounts" not in users[uid]:
            diagnostics["linked_field_missing"] += 1
        links, link_problem = _linked_keys(users.get(uid), config)
        diagnostics["linked_account_count_" + ("multiple" if len(links) > 1 else str(len(links)))] += 1
        if link_problem:
            note(link_problem, uid)
            continue
        registered = set()
        for row in registry.get(uid, []):
            if row.get("qualified_event_id") not in historical_ids[uid]:
                continue  # a foreign/missing event pointer is not an ownership proof
            key = row.get("account_key")
            if not isinstance(key, str) or ":" not in key:
                registered.add("!invalid")
                break
            canonical, reason = aq.canonical_account_key(aq.account_id_from_key(key), (config or {}).get("account_namespace"))
            if reason or canonical != key:
                registered.add("!invalid")
                break
            registered.add(key)
        if "!invalid" in registered:
            # One final unresolved reason per event.
            note("unresolved_invalid_registry_identity", uid)
            continue
        # Union ALL reliable sources before choosing: never select first/latest.
        identities = accounts | links | registered | observed
        if len(identities) > 1:
            note("conflicting_multiple_accounts", uid)
            continue
        basis = "redemption_evidence"
        if registered:
            accounts, basis = registered, "existing_credited_registry"
        elif not accounts or bad:
            if seed_from_linkage and len(links) == 1 and (not bad or bad <= time_only):
                accounts, basis = links, "verified_linkage_single_account"
            elif bad:
                note("unresolved_unusable_account_value", uid)
                continue
        if not accounts:
            note("unresolved_no_redemption_evidence" if user_codes else "unresolved_no_welcome_code", uid)
            continue
        account_key = next(iter(accounts))  # union cardinality was proven exactly one
        resolved.append((account_key, q, basis))
        counts["resolved"] += 1
        resolution_sources[basis] += 1
        if basis == "redemption_evidence":
            evidence_sources[aq.source_name(config)] += 1
            for source in {s for code in user_codes for s in code_sources.get(code, set())}:
                evidence_sources[source] += 1
        else:
            evidence_sources[basis] += 1

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
                    {"account": aq.mask_value(account_key), "qualification_count": len(items),
                     "invitees": [_mask_uid(i[0].get("invitee_id")) for i in items[:SAMPLE_LIMIT]]}
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
    coverage = _source_coverage(db, config, latest, now_utc=now_utc)
    unresolved = len(historical) - counts["resolved"]
    blockers = list(problems) + list(coverage["problems"])
    if unresolved:
        blockers.append("unresolved_historical_identities")
    if history_policy == aq.HISTORY_FORWARD:
        # These gaps are explicitly accepted, never reported as full coverage.
        # Feed health, malformed config and invalid future coverage still block.
        blockers = [b for b in blockers if b not in {
            "unresolved_historical_identities", "source_coverage_unknown", "source_coverage_older_than_history"}]
    seed_fps = [_seed_identity_fingerprint(sd) for sd in seeds]
    return {
        "planner_version": PLANNER_VERSION,
        "history_policy": history_policy,
        "history_limitation": aq.HISTORY_LIMITATION if history_policy == aq.HISTORY_FORWARD else None,
        "report_fingerprint": fwd.report_fingerprint(
            planner_version=PLANNER_VERSION, history_policy=history_policy,
            identity_config=aq.identity_config(config), historical_events=len(historical),
            unresolved_events=unresolved, seed_fingerprints=seed_fps,
            activation_cutoff_utc=_cutoff_iso(activation_cutoff)),
        "identity_config": aq.identity_config(config),
        "historical_events": len(historical), "unresolved_events": unresolved,
        "historical_invitees": len(uids),
        "resolved_invitees": len({int(q["invitee_id"]) for _, q, _ in resolved}),
        "resolution_sources": dict(sorted(resolution_sources.items())),
        "resolution_evidence_sources": dict(sorted(evidence_sources.items())),
        "source_diagnostics": dict(sorted(diagnostics.items())),
        "source_coverage": coverage, "apply_safe": not blockers,
        "apply_blockers": sorted(set(blockers)),
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
            holder = db[aq.REGISTRY_COLLECTION].find_one({"account_key": seed["account_key"]}, {"invitee_id": 1, "qualified_event_id": 1}) or {}
            if (holder.get("invitee_id") == seed["invitee_id"]
                    and holder.get("qualified_event_id") == seed["qualified_event_id"]):
                out["already_seeded"] += 1
            else:
                out["held_by_other"] += 1
    return out


FORWARD_FIELDS = ("activation_cutoff", "operator", "reason", "report_fingerprint", "unresolved_count",
                  "qualified_history_count", "seeded_count", "account_namespace")


def _forward_blockers_expected(plan, control):
    cfg = (control or {}).get("source_config")
    return {"report_fingerprint": plan["report_fingerprint"], "unresolved_count": plan["unresolved_events"],
            "qualified_history_count": plan["historical_events"], "seeded_count": plan["accounts_to_seed"],
            "account_namespace": aq.normalize_namespace((cfg or {}).get("account_namespace"))}


def _forward_only_blockers(forward_only, plan, control) -> list[str]:
    """Every operator input must be present, and every figure must equal the dry run."""
    fo = forward_only or {}
    blockers = [f"forward_only_missing_{f}" for f in FORWARD_FIELDS if fo.get(f) in (None, "")]
    for field, expected in _forward_blockers_expected(plan, control).items():
        if fo.get(field) not in (None, "") and str(fo[field]) != str(expected):
            blockers.append(f"forward_only_{field}_mismatch")
    if fo.get("activation_cutoff") not in (None, ""):
        try:
            fwd.kl_cutoff(fo["activation_cutoff"])
        except (ValueError, TypeError):
            blockers.append("forward_only_activation_cutoff_invalid")
    return blockers


def _forward_marker(plan, seeds, fo, report, now_utc) -> dict:
    cutoff = fwd.kl_cutoff(fo["activation_cutoff"])
    before = report["history_before"]
    return {
        "version": aq.FORWARD_MARKER_VERSION, "history_policy": aq.HISTORY_FORWARD,
        "fingerprint": plan["report_fingerprint"], "planner_version": PLANNER_VERSION,
        "activation_cutoff_utc": cutoff, "activation_cutoff_kl": fwd.kl_iso(cutoff),
        "operator": fo["operator"], "reason": fo["reason"], "authorized_at": now_utc,
        "unknown_history_acknowledged": True,
        "unresolved_count": plan["unresolved_events"], "qualified_history_count": plan["historical_events"],
        "seeded_count": len(seeds), "account_namespace": plan["identity_config"],
        "limitation": aq.HISTORY_LIMITATION,
        "history_counts": {k: before[k] for k in ("qualified_events_total", "referral_settled_total",
                                                  "xp_events_created_before", "referral_award_events_total",
                                                  "affiliate_ledger_issued_created_before")},
    }


def run(db, *, apply: bool, seed_from_linkage: bool, now_utc: datetime,
        history_policy: str = aq.HISTORY_STRICT, acknowledge_unknown_history: bool = False,
        forward_only: dict | None = None) -> dict:
    """``forward_only`` (apply under forward_only_from_activation_cutoff): operator-authorized
    inputs - activation_cutoff, operator, reason, report_fingerprint, unresolved_count,
    qualified_history_count, seeded_count, account_namespace. All are required and
    each must equal what the reviewed dry run found, or nothing is written."""
    control = aq.get_control(db) or {}
    report = {"apply": apply, "now_utc": now_utc.isoformat(), "history_before": history_snapshot(db, as_of=now_utc)}
    report["indexes_before"] = aq.index_readiness(db)
    cutoff_text = (forward_only or {}).get("activation_cutoff") if history_policy == aq.HISTORY_FORWARD else None
    plan = build_plan(db, config=control.get("source_config"), seed_from_linkage=seed_from_linkage,
                      now_utc=now_utc, history_policy=history_policy, activation_cutoff=cutoff_text)
    seeds = plan.pop("_seeds")
    report["plan"], report["seed_marker_written"] = plan, False
    token = None
    if apply:
        blockers = list(plan["apply_blockers"])
        if history_policy == aq.HISTORY_FORWARD:
            if not acknowledge_unknown_history:
                blockers.append("forward_only_requires_unknown_history_acknowledgement")
            blockers.extend(_forward_only_blockers(forward_only, plan, control))
        if control.get("mode", aq.MODE_DISABLED) != aq.MODE_DISABLED or aq.launch_cutoff(control) is not None:
            blockers.append("migration_requires_disabled_unlaunched_rule")
        if (control.get("seeded_identity_config") and
                control["seeded_identity_config"] != aq.identity_config(control.get("source_config"))):
            blockers.append("seeded_account_namespace_mismatch")
        if blockers:
            report["apply_refused"] = sorted(set(blockers))
        else:
            token = uuid.uuid4().hex
            locked = db[aq.CONTROL_COLLECTION].find_one_and_update(
                {"_id": aq.CONTROL_ID, "mode": aq.MODE_DISABLED, "launch_cutoff_utc": {"$exists": False},
                 "source_config": control.get("source_config"),
                 # mutual exclusion with legacy writers for the WHOLE plan/seed/complete interval
                 "legacy_writer_token": {"$exists": False},
                 "legacy_award_inflight": {"$exists": False},
                 "$or": [{"migration_lease_until": {"$exists": False}}, {"migration_lease_until": {"$lt": now_utc}}]},
                {"$set": {"migration_lease_token": token, "migration_lease_until": now_utc + timedelta(minutes=30)},
                 "$unset": {"seed_completed_at": "", "forward_only_marker": ""}}, return_document=ReturnDocument.AFTER,
            )
            if locked is None:
                report["apply_refused"] = ["migration_lease_unavailable_or_control_changed"]
                token = None
    try:
        if token:
            report["indexes_created"] = aq.ensure_indexes(db)
            report["seed_result"] = apply_seeds(db, seeds, now_utc=now_utc)
        report["indexes_after"] = aq.index_readiness(db)
        report["consistency"] = aq.consistency_report(db, now_utc=now_utc)
        report["history_after"] = history_snapshot(db, as_of=now_utc)
        report["history_unchanged"] = report["history_after"] == report["history_before"]
        if token and not report["history_unchanged"]:
            report["apply_refused"] = ["historical_counts_changed"]
        if (token and report["history_unchanged"] and report["indexes_after"]["ok"]
                and report["consistency"]["ok"]):
            # Re-plan before marking complete: the identity sources may have
            # changed during seeding. A crash or race leaves no completion marker.
            final = build_plan(db, config=control.get("source_config"), seed_from_linkage=seed_from_linkage,
                               now_utc=now_utc, history_policy=history_policy, activation_cutoff=cutoff_text)
            def ownership_plan(rows):
                return sorted((r["account_key"], r["invitee_id"], str(r["qualified_event_id"]),
                               r["historical_qualification_count"]) for r in rows)
            if (final["apply_safe"] and final["historical_events"] == plan["historical_events"]
                    and final["unresolved_events"] == plan["unresolved_events"]
                    and final["source_coverage"] == plan["source_coverage"]
                    and ownership_plan(final["_seeds"]) == ownership_plan(seeds)
                    and not report["seed_result"]["held_by_other"]):
                marker_update = ({"forward_only_marker": _forward_marker(plan, seeds, forward_only, report, now_utc)}
                                 if history_policy == aq.HISTORY_FORWARD else {})
                result = db[aq.CONTROL_COLLECTION].update_one(
                    {"_id": aq.CONTROL_ID, "migration_lease_token": token,
                     "migration_lease_until": {"$gt": datetime.now(timezone.utc)},
                     "mode": aq.MODE_DISABLED, "launch_cutoff_utc": {"$exists": False},
                     "legacy_writer_token": {"$exists": False},
                     "legacy_award_inflight": {"$exists": False},
                     "source_config": control.get("source_config")},
                    {"$set": {"seed_completed_at": now_utc,
                              "seeded_identity_config": aq.identity_config(control.get("source_config")),
                              "history_policy": history_policy,
                              "unknown_history_acknowledged_at": now_utc if acknowledge_unknown_history else None,
                              "history_limitation": aq.HISTORY_LIMITATION if history_policy == aq.HISTORY_FORWARD else None,
                              **marker_update,
                              "seed_summary": {"planner_version": PLANNER_VERSION, "counts": plan["counts"],
                                               "history_policy": history_policy,
                                               "unresolved_events": plan["unresolved_events"],
                                               "accounts_seeded": len(seeds),
                                               "seed_identity_fingerprints": sorted(_seed_identity_fingerprint(s) for s in seeds),
                                               "historical_events": plan["historical_events"],
                                               "report_fingerprint": plan["report_fingerprint"],
                                               "source_coverage": plan["source_coverage"],
                                               "historical_duplicate_accounts": plan["historical_duplicate_accounts"]},
                              "updated_at": now_utc}},
                )
                report["seed_marker_written"] = bool(result.modified_count)
            if not report["seed_marker_written"]:
                report["apply_refused"] = ["completion_verification_failed"]
        if token and not report["seed_marker_written"]:
            report.setdefault("apply_refused", ["completion_verification_failed"])
        return report
    finally:
        if token:
            db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID, "migration_lease_token": token},
                {"$unset": {"migration_lease_token": "", "migration_lease_until": ""}})


def _marker_problems(control, summary) -> list[str]:
    """Versioned marker present, internally consistent and bound to the reviewed dry run."""
    marker = (control or {}).get("forward_only_marker") or {}
    if marker.get("version") != aq.FORWARD_MARKER_VERSION:
        return ["forward_only_marker_version_mismatch"]
    problems = []
    expected = fwd.report_fingerprint(
        planner_version=summary.get("planner_version"), history_policy=aq.HISTORY_FORWARD,
        identity_config=aq.identity_config((control or {}).get("source_config")),
        historical_events=summary.get("historical_events", -1), unresolved_events=summary.get("unresolved_events", -1),
        seed_fingerprints=summary.get("seed_identity_fingerprints") or [],
        activation_cutoff_utc=_cutoff_iso(marker.get("activation_cutoff_utc")))
    if marker.get("fingerprint") != expected or summary.get("report_fingerprint") != expected:
        problems.append("forward_only_fingerprint_mismatch")
    if (marker.get("seeded_count") != summary.get("accounts_seeded")
            or marker.get("unresolved_count") != summary.get("unresolved_events")
            or marker.get("qualified_history_count") != summary.get("historical_events")):
        problems.append("forward_only_marker_counts_mismatch")
    if marker.get("account_namespace") != aq.identity_config((control or {}).get("source_config")):
        problems.append("forward_only_namespace_mismatch")
    if not marker.get("activation_cutoff_utc"):
        problems.append("forward_only_marker_cutoff_missing")
    return problems


def completion_readiness(db, control):
    summary = (control or {}).get("seed_summary") or {}
    problems = []
    if summary.get("planner_version") != PLANNER_VERSION:
        problems.append("migration_summary_unverified")
    policy = (control or {}).get("history_policy", aq.HISTORY_STRICT)
    if policy == aq.HISTORY_FORWARD:
        if (summary.get("history_policy") != policy or not control.get("unknown_history_acknowledged_at")
                or control.get("history_limitation") != aq.HISTORY_LIMITATION):
            problems.append("forward_only_policy_unacknowledged")
    elif policy != aq.HISTORY_STRICT or summary.get("unresolved_events") != 0 or not (summary.get("source_coverage") or {}).get("ok"):
        problems.append("migration_coverage_unverified")
    if policy == aq.HISTORY_FORWARD:
        problems.extend(_marker_problems(control, summary))
    query = {"rule_version": {"$ne": aq.RULE_VERSION}}
    fingerprints = summary.get("seed_identity_fingerprints") or []
    if len(fingerprints) != summary.get("accounts_seeded", 0):
        problems.append("seeded_registry_manifest_unverified")
    if fingerprints:
        present = {_seed_identity_fingerprint(row) for row in db[aq.REGISTRY_COLLECTION].find(
            {"state": {"$in": [aq.REG_SEEDED, aq.REG_CREDITED_LEGACY]}})}
        if not set(fingerprints).issubset(present):
            problems.append("seeded_registry_ownership_changed")
    count = db.qualified_events.count_documents(query)
    if summary.get("historical_events") != count:
        problems.append("historical_qualifications_changed_since_seed")
    latest = db.qualified_events.find_one(query, {"qualified_at": 1}, sort=[("qualified_at", -1)])
    when = aq._aware_utc((latest or {}).get("qualified_at"))
    current = when.isoformat() if when else None
    if (count and when is None) or (summary.get("source_coverage") or {}).get("latest_historical_qualification_utc") != current:
        problems.append("historical_qualification_time_changed_since_seed")
    return {"ok": not problems, "problems": sorted(set(problems)), "history_policy": policy,
            "unresolved_historical_events": summary.get("unresolved_events"),
            "history_limitation": aq.HISTORY_LIMITATION if policy == aq.HISTORY_FORWARD else None}


def main(argv=None, *, db_factory=None, read_only_db_factory=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="create indexes, seed registry (default: dry run)")
    parser.add_argument("--seed-from-linkage", action="store_true",
                        help="also seed invitees with no redemption rows but exactly one verified linked account")
    parser.add_argument("--history-policy", choices=[aq.HISTORY_STRICT, aq.HISTORY_FORWARD], default=aq.HISTORY_STRICT)
    parser.add_argument("--acknowledge-unknown-history", action="store_true",
                        help="explicitly accept that unknown historical Account IDs cannot be protected")
    parser.add_argument("--output", help="also write the JSON report to this path")
    fo = parser.add_argument_group("forward_only_from_activation_cutoff --apply inputs (all required, must equal the reviewed dry run)")
    fo.add_argument("--activation-cutoff", help="ISO-8601; a value without offset is Asia/Kuala_Lumpur")
    fo.add_argument("--operator")
    fo.add_argument("--reason")
    fo.add_argument("--expect-report-fingerprint")
    fo.add_argument("--expect-unresolved", type=int)
    fo.add_argument("--expect-qualified-history", type=int)
    fo.add_argument("--expect-seeded", type=int)
    fo.add_argument("--account-namespace", help="exact account namespace; must equal the configured source namespace")
    args = parser.parse_args(argv)
    if args.history_policy == aq.HISTORY_FORWARD and not args.acknowledge_unknown_history:
        parser.error(f"{aq.HISTORY_FORWARD} requires --acknowledge-unknown-history, including dry runs")
    forward_only = {"activation_cutoff": args.activation_cutoff, "operator": args.operator, "reason": args.reason,
                    "report_fingerprint": args.expect_report_fingerprint, "unresolved_count": args.expect_unresolved,
                    "qualified_history_count": args.expect_qualified_history, "seeded_count": args.expect_seeded,
                    "account_namespace": aq.normalize_namespace(args.account_namespace) if args.account_namespace else None}
    try:
        db = (db_factory or _write_db)() if args.apply else (read_only_db_factory or _read_only_db)()
        report = run(db, apply=args.apply, seed_from_linkage=args.seed_from_linkage, now_utc=datetime.now(timezone.utc),
                     history_policy=args.history_policy, acknowledge_unknown_history=args.acknowledge_unknown_history,
                     forward_only=forward_only)
    except Exception as exc:
        # Mongo errors can contain duplicate key values (raw account/user ids).
        print(json.dumps({"apply": args.apply, "migration_failed": type(exc).__name__, "seed_marker_written": False}))
        return 1
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text)
    print(text)
    if args.apply and (not report["history_unchanged"] or not report["seed_marker_written"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
