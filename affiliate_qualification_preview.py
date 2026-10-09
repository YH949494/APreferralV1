"""Read-only shadow preview of the welcome_redemption_v1 qualification rule.

Powers the admin dashboard "Qualification Preview" view. It performs ONLY
reads (find / aggregate / count / list_indexes): no evidence rows, no
qualified_events, no registry reservations, no XP, no reward ledgers, and it
never touches the control document or the launch cutoff. Current live
qualification behaviour is unaffected by anything here.

Same rules as live: committed source rows go through the same
:func:`affiliate_qualification.parse_source_row`, the same recipient decision
(:func:`decide_recipient`) and evidence builder (:func:`build_evidence_doc`),
and every row is judged by the same :func:`assess_evidence`. Only the data
access differs: lookups are bulk-loaded into memory, and the lifetime account
registry is simulated instead of written.

Two simulations, kept separate in the payload:

* ``historical`` — "had the new rule always applied": every redemption to
  date replayed chronologically from an empty registry, ignoring legacy
  qualifications; counts are for redemptions inside the preview period.
* ``at_launch`` — what activation would newly qualify now: legacy
  qualifications stay (those invitees are excluded, their verified accounts
  count as credited) and the real registry is honoured. Not period-bound;
  pre-launch redemptions are attributed to the launch month.
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

import affiliate_qualification as aq

logger = logging.getLogger(__name__)

PREVIEW_MAX_SOURCE_ROWS = 200_000
REVIEW_CASE_LIMIT = 200
AFFILIATE_ROW_LIMIT = 500
_CHUNK = 5000
_MONTH_RE = re.compile(r"^(\d{4})(\d{2})$")
UNATTRIBUTED = "unattributed"

OVERRIDE_FIELDS = (
    "code_column", "account_column", "redeemed_at_column", "status_column", "success_values",
    "source_timezone", "account_namespace", "namespace_column", "campaign_ids",
    "source", "success_basis", "legacy_time_basis",
)
# Problems that make Databot evidence incomplete but still usable (shown as a
# stale/partial banner); every other problem blocks the new-rule figures.
NON_BLOCKING_PROBLEMS = frozenset({"evidence_sync_stale", "uim_batches_partially_synced"})
PREVIEW_ASSUMED_ATTESTATION = {"by": "preview-only override", "reference": "NOT ATTESTED — assumed for this preview",
                               "preview_only": True}


def month_window(yyyymm: str | None, *, now_utc: datetime) -> dict:
    """GMT+8 calendar month window. Defaults to the current month."""
    if not yyyymm:
        yyyymm = aq.kl_month_key(now_utc)
    m = _MONTH_RE.match(str(yyyymm).strip())
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise ValueError("month must be YYYYMM")
    year, month = int(m.group(1)), int(m.group(2))
    start_kl = aq.KL_TZ.localize(datetime(year, month, 1))
    end_kl = aq.KL_TZ.localize(datetime(year + (month == 12), month % 12 + 1, 1))
    return {
        "month": f"{year:04d}{month:02d}",
        "label": start_kl.strftime("%B %Y") + " (GMT+8)",
        "start_utc": start_kl.astimezone(timezone.utc),
        "end_utc": end_kl.astimezone(timezone.utc),
        "start_kl": start_kl.isoformat(),
        "end_kl": end_kl.isoformat(),
    }


def source_config_from_args(args, saved: dict | None) -> tuple[dict | None, bool]:
    """Merge preview-only mapping overrides (never persisted) over the saved
    source config. Returns ``(config, overridden)``."""
    provided = {}
    for field in OVERRIDE_FIELDS:
        raw = args.get(field)
        if raw in (None, ""):
            continue
        if field in ("success_values", "campaign_ids"):
            values = [v.strip() for v in str(raw).split(",") if v.strip()]
            provided[field] = [v.lower() for v in values] if field == "success_values" else values
        elif field == "account_namespace":
            provided[field] = aq.normalize_namespace(raw)
        else:
            provided[field] = str(raw).strip()
    if not provided:
        return (dict(saved) if saved else None), False
    if provided.get("source") not in (None, aq.SOURCE_MARKETING, aq.SOURCE_DATABOT_UIM):
        provided.pop("source")
    base = saved or {}
    if provided.get("source") and provided["source"] != base.get("source", aq.SOURCE_MARKETING):
        base = {}  # switching source: never mix another source's saved mapping in
    merged = {"source": aq.SOURCE_MARKETING, "source_timezone": "Asia/Kuala_Lumpur", **base, **provided}
    if merged.get("source") == aq.SOURCE_DATABOT_UIM:
        merged.update(aq.DATABOT_FIXED_COLUMNS)
        if merged.get("success_basis") == aq.SUCCESS_BASIS_SOURCE_CONTRACT and not (
            (merged.get("success_attestation") or {}).get("reference")
        ):
            # Lets an admin see the numbers before the data owner attests, but
            # the payload labels success as ASSUMED, never as attested.
            merged["success_attestation"] = dict(PREVIEW_ASSUMED_ATTESTATION)
    return merged, True


def _chunks(items, size=_CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


class ShadowLookup:
    """In-memory implementation of :class:`affiliate_qualification.DbLookup`."""

    def __init__(self, *, pending_by_invitee, users, account_links, real_qualifications, code_accounts):
        self._pending = pending_by_invitee
        self._users = users
        self._links = account_links
        self._real = real_qualifications
        self._code_accounts = code_accounts
        self.simulated = {}

    def pending_referrals(self, invitee_id):
        return self._pending.get(int(invitee_id), [])

    def user(self, uid):
        return self._users.get(int(uid))

    def account_linked_to_other_user(self, account_id, invitee_id):
        return bool(self._links.get(account_id, set()) - {int(invitee_id)})

    def existing_qualification(self, invitee_id):
        return self.simulated.get(int(invitee_id)) or (self._real or {}).get(int(invitee_id))

    def conflicting_record(self, evidence):
        return bool(self._code_accounts.get(evidence["code_hash"], set()) - {evidence.get("account_key")})


def _load_observations(db, config, out_stats) -> list[dict]:
    """Committed (non-rolled-back) source rows -> parsed observations, in
    commit order (the same rows, order and parser the live pipeline uses)."""
    observations = []
    batches = set()
    for batch_id, row in aq.iter_committed_source_rows(db, config):
        batches.add(batch_id)
        out_stats["batches"] = len(batches)
        if out_stats["rows_scanned"] >= PREVIEW_MAX_SOURCE_ROWS:
            out_stats["truncated"] = True
            return observations
        out_stats["rows_scanned"] += 1
        obs = aq.parse_source_row(row, config)
        if obs is None:
            out_stats["skipped_campaign"] += 1
            continue
        obs["code"] = aq.normalize_code(obs["code"])
        if not obs["code"]:
            out_stats["missing_code"] += 1
            continue
        observations.append(obs)
    return observations


def _resolve_recipients(db, codes: set[str]) -> dict[str, dict]:
    """Bulk form of :func:`affiliate_qualification.resolve_welcome_recipient`."""
    pool_rows, claims, other_pool = {}, defaultdict(list), set()
    for chunk in _chunks(codes):
        for row in db.voucher_pools.find({"pool_id": aq.WELCOME_POOL_ID, "code": {"$in": chunk}}):
            pool_rows[row["code"]] = row
        for row in db.new_joiner_claims.find({"code": {"$in": chunk}}, {"code": 1, "uid": 1}):
            claims[row["code"]].append(row.get("uid"))
        for row in db.voucher_pools.find({"code": {"$in": chunk}, "pool_id": {"$ne": aq.WELCOME_POOL_ID}}, {"code": 1}):
            other_pool.add(row["code"])
    ledger_codes = {}
    uids = set()
    for row in pool_rows.values():
        raw = row.get("issued_to_user_id") if row.get("issued_to_user_id") not in (None, "") else row.get("issued_to")
        try:
            uids.add(int(raw))
        except (TypeError, ValueError):
            continue
    for chunk in _chunks(f"WELCOME:{u}" for u in uids):
        for row in db.affiliate_ledger.find({"dedup_key": {"$in": chunk}}, {"dedup_key": 1, "voucher_code": 1}):
            ledger_codes[row["dedup_key"]] = row.get("voucher_code")
    return {
        code: aq.decide_recipient(
            code=code,
            pool_row=pool_rows.get(code),
            ledger_code_for=lambda uid: ledger_codes.get(f"WELCOME:{uid}"),
            claim_uids=claims.get(code, []),
            other_pool_exists=code in other_pool,
        )
        for code in codes
    }


def _build_evidences(observations, recipients, *, now_utc, stats, source=aq.SOURCE_MARKETING) -> list[dict]:
    """In-memory evidence, deduplicated exactly as the unique
    ``(source, source_ref)`` index would; a changed re-observation is
    marked for review (correction policy)."""
    by_ref: dict[str, dict] = {}
    for obs in observations:
        recipient = recipients[obs["code"]]
        if recipient["status"] in ("unknown", "non_welcome"):
            stats[f"{recipient['status']}_code"] += 1
            continue
        doc = aq.build_evidence_doc(source=source, recipient=recipient, now_utc=now_utc, **obs)
        prior = by_ref.get(doc["source_ref"])
        if obs.get("observation") == aq.OBS_REMOVED:
            # A later committed import dropped this row: correction policy
            # (same as live record_redemption_evidence) -> review.
            stats["removed_observations"] += 1
            if prior is not None:
                prior["_source_removed"] = True
            continue
        if prior is None:
            doc["_id"] = "shadow:" + doc["source_ref"]
            by_ref[doc["source_ref"]] = doc
        elif prior["observation_fingerprint"] != doc["observation_fingerprint"]:
            prior["_conflicting_observation"] = True
    stats["welcome_evidence_rows"] = len(by_ref)
    return list(by_ref.values())


def _simulate(evidences, *, control, now_utc, lookup, credited: dict) -> list[dict]:
    """Replay evidence chronologically through assess_evidence, with the
    lifetime registry simulated in ``credited`` (account_key -> origin)."""
    order = sorted(
        evidences,
        key=lambda e: (aq._aware_utc(e.get("redeemed_at")) or datetime.max.replace(tzinfo=timezone.utc), e["source_ref"]),
    )
    outcomes = []
    for ev in order:
        if ev.get("_source_removed"):
            a = {"status": aq.EV_REVIEW, "reason": "source_row_removed_by_later_import"}
        elif ev.get("_conflicting_observation"):
            a = {"status": aq.EV_REVIEW, "reason": "conflicting_source_observations"}
        else:
            a = aq.assess_evidence(ev, control=control, now_utc=now_utc, lookup=lookup)
        if a["status"] == aq.EV_INVITEE_ALREADY_QUALIFIED and a.get("legacy_credit"):
            credited.setdefault(ev["account_key"], aq.REG_CREDITED_LEGACY)
        status, reason = a["status"], a["reason"]
        if status == aq.ELIGIBLE:
            if ev["account_key"] in credited:
                status, reason = aq.EV_DUPLICATE_ACCOUNT, "account_already_credited"
            else:
                credited[ev["account_key"]] = "simulated"
                lookup.simulated[a["invitee_id"]] = {
                    "invitee_id": a["invitee_id"], "referrer_id": a["inviter_id"],
                    "rule_version": aq.RULE_VERSION, "evidence_id": ev["_id"],
                }
                status = aq.EV_QUALIFIED
        outcomes.append({
            "status": status,
            "reason": reason,
            "invitee_id": a.get("invitee_id") or (ev.get("recipient_resolution") or {}).get("user_id"),
            "inviter_id": a.get("inviter_id"),
            "redeemed_at": aq._aware_utc(ev.get("redeemed_at")),
            "qualified_at": a.get("qualified_at"),
            "account_masked": ev.get("account_key_masked"),
            "code_masked": ev.get("code_masked"),
            "successful": bool(ev.get("redemption_successful")),
        })
    return outcomes


def _original_referral(rows, invitee) -> tuple[int, datetime] | None:
    """(inviter, created_at) of the earliest attribution-valid referral
    record — the same record resolve_attribution freezes."""
    for row in rows:
        try:
            inviter = int(row.get("inviter_user_id"))
        except (TypeError, ValueError):
            continue
        if inviter == invitee:
            continue
        if row.get("status") == "revoked" and row.get("revoked_reason") in aq.ATTRIBUTION_INVALID_REVOKE_REASONS:
            continue
        created = aq._aware_utc(row.get("created_at_utc"))
        if created is not None:
            return inviter, created
    return None


def _attribution_cohort(pending_by_invitee, window) -> dict[int, int]:
    """invitee -> inviter for invitees whose original referral record was
    created in the window."""
    cohort = {}
    for invitee, rows in pending_by_invitee.items():
        original = _original_referral(rows, invitee)
        if original and window["start_utc"] <= original[1] < window["end_utc"]:
            cohort[invitee] = original[0]
    return cohort


def build_preview(db, *, month: str | None, now_utc: datetime, source_config: dict | None = None,
                  config_overridden: bool = False) -> dict:
    window = month_window(month, now_utc=now_utc)
    control = aq.get_control(db) or {}
    config = source_config if source_config is not None else control.get("source_config")
    cutoff = aq.launch_cutoff(control)
    payload = {
        "ok": True,
        "read_only": True,
        "writes": "none — preview never writes evidence, qualified_events, registry, XP, ledgers or the cutoff",
        "rule_version": aq.RULE_VERSION,
        "generated_at": now_utc.isoformat(),
        "control": {
            "mode": control.get("mode") or aq.MODE_DISABLED,
            "launch_cutoff_utc": cutoff.isoformat() if cutoff else None,
            "new_rule_active": aq.new_rule_active(control, now_utc),
            "legacy_award_allowed": aq.legacy_award_allowed(control, now_utc),
        },
        "period": {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in window.items()},
        "launch_assumption": (
            f"persisted cutoff {cutoff.isoformat()}" if cutoff
            else "no cutoff persisted: computed as if activated now"
        ),
    }

    # Live rule, period-scoped: the same qualified_events window tiers/leaderboards use.
    current = Counter()
    for row in db.qualified_events.aggregate([
        {"$match": {"qualified_at": {"$gte": window["start_utc"], "$lt": window["end_utc"]}, "referrer_id": {"$ne": None}}},
        {"$group": {"_id": "$referrer_id", "n": {"$sum": 1}}},
    ]):
        current[int(row["_id"])] = int(row["n"])
    joined = Counter()
    cohort_invitees = set()
    for row in db.pending_referrals.find(
        {"created_at_utc": {"$gte": window["start_utc"], "$lt": window["end_utc"]}, "inviter_user_id": {"$ne": None}},
        {"inviter_user_id": 1, "invitee_user_id": 1},
    ):
        joined[int(row["inviter_user_id"])] += 1
        if row.get("invitee_user_id") is not None:
            cohort_invitees.add(int(row["invitee_user_id"]))

    problems = aq.validate_source_config(config)
    integration = (aq.integration_readiness(db, config, now_utc=now_utc) if not problems
                   else {"ok": False, "problems": problems})
    all_problems = sorted(set(problems + list(integration.get("problems") or [])))
    blocking = [p for p in all_problems if p not in NON_BLOCKING_PROBLEMS]
    blocked = bool(blocking)
    is_databot = (config or {}).get("source") == aq.SOURCE_DATABOT_UIM
    attestation = (config or {}).get("success_attestation") or {}
    payload["source"] = {
        "name": aq.source_name(config),
        "label": ("Databot UIM imports (committed batches)" if is_databot
                  else "APReferral marketing uploads (marketing_raw_data)"),
        "configured": bool(control.get("source_config")),
        "using_preview_mapping": bool(config_overridden),
        "mapping": {k: (config or {}).get(k) for k in OVERRIDE_FIELDS},
        "problems": all_problems,
        "blocked": blocked,
        "blocker": (
            "No authoritative Welcome redemption data is available: " + "; ".join(blocking)
            + ". New-rule figures are unavailable (not zero) until this is resolved."
        ) if blocked else None,
        # Evidence incomplete (stale sync / batch mid-sync): figures shown, flagged.
        "evidence_incomplete": [p for p in all_problems if p in NON_BLOCKING_PROBLEMS],
        "sync": integration.get("sync"),
        "success_basis": (
            {"basis": (config or {}).get("success_basis"), "attested": bool(attestation.get("reference"))
             and not attestation.get("preview_only"), "attested_by": attestation.get("by"),
             "reference": attestation.get("reference")}
            if is_databot else {"basis": "status_column", "status_column": (config or {}).get("status_column")}
        ),
    }

    if blocked:
        payload["affiliates"] = [
            {"referrer_id": str(r), "joined": joined[r], "current_qualified": current[r],
             "historical": None, "at_launch": None}
            for r in sorted(set(current) | set(joined), key=lambda r: (-current[r], -joined[r], r))[:AFFILIATE_ROW_LIMIT]
        ]
        payload["affiliates_total"] = len(set(current) | set(joined))
        payload["totals"] = {"joined": sum(joined.values()), "current_qualified": sum(current.values())}
        payload["review_cases"] = []
        payload["outcome_reasons"] = {}
        return payload

    stats = Counter(rows_scanned=0, batches=0, skipped_campaign=0, missing_code=0, unknown_code=0, non_welcome_code=0,
                    removed_observations=0)
    stats["truncated"] = False
    observations = _load_observations(db, config, stats)
    recipients = _resolve_recipients(db, {o["code"] for o in observations})
    evidences = _build_evidences(observations, recipients, now_utc=now_utc, stats=stats,
                                 source=aq.source_name(config))

    evidence_invitees = {
        int(e["recipient_resolution"]["user_id"]) for e in evidences
        if (e.get("recipient_resolution") or {}).get("status") == "ok"
    }
    invitees = evidence_invitees | cohort_invitees
    pending_by_invitee = defaultdict(list)
    for chunk in _chunks(invitees):
        for row in db.pending_referrals.find(
            {"invitee_user_id": {"$in": chunk}},
            {"invitee_user_id": 1, "inviter_user_id": 1, "status": 1, "revoked_reason": 1,
             "created_at_utc": 1, "destination_type": 1},
        ):
            pending_by_invitee[int(row["invitee_user_id"])].append(row)
    for rows in pending_by_invitee.values():
        rows.sort(key=lambda r: aq._aware_utc(r.get("created_at_utc")) or datetime.max.replace(tzinfo=timezone.utc))
    user_ids = set(invitees)
    for rows in pending_by_invitee.values():
        for row in rows:
            try:
                user_ids.add(int(row.get("inviter_user_id")))
            except (TypeError, ValueError):
                pass
    users = {}
    for chunk in _chunks(user_ids):
        for row in db.users.find({"user_id": {"$in": chunk}},
                                 {"user_id": 1, "joined_main_at": 1, "created_at": 1, "linked_gaming_accounts": 1}):
            users[int(row["user_id"])] = row
    account_ids = {aq.account_id_from_key(e["account_key"]) for e in evidences if e.get("account_key")}
    account_links = defaultdict(set)
    for chunk in _chunks(account_ids):
        for row in db.users.find({"linked_gaming_accounts": {"$in": chunk}}, {"user_id": 1, "linked_gaming_accounts": 1}):
            for acct in aq._linked_accounts(row):
                if acct in account_ids:
                    account_links[acct].add(int(row["user_id"]))
    code_accounts = defaultdict(set)
    for e in evidences:
        if e.get("redemption_successful"):
            code_accounts[e["code_hash"]].add(e.get("account_key"))
    hashes = list(code_accounts)
    for chunk in _chunks(hashes):
        for row in db[aq.EVIDENCE_COLLECTION].find(
            {"code_hash": {"$in": chunk}, "redemption_successful": True, "status": {"$ne": aq.EV_VOIDED}},
            {"code_hash": 1, "account_key": 1},
        ):
            code_accounts[row["code_hash"]].add(row.get("account_key"))
    real_q = {}
    for chunk in _chunks(invitees):
        for row in db.qualified_events.find({"invitee_id": {"$in": chunk}}):
            real_q[int(row["invitee_id"])] = row
    registry = {}
    for chunk in _chunks({e["account_key"] for e in evidences if e.get("account_key")}):
        for row in db[aq.REGISTRY_COLLECTION].find({"account_key": {"$in": chunk}}, {"account_key": 1, "state": 1}):
            registry[row["account_key"]] = row.get("state")

    def lookup(real):
        return ShadowLookup(pending_by_invitee=pending_by_invitee, users=users, account_links=account_links,
                            real_qualifications=real, code_accounts=code_accounts)

    hist_control = {**control, "source_config": config, "launch_cutoff_utc": None}
    historical = _simulate(evidences, control=hist_control, now_utc=now_utc, lookup=lookup(None), credited={})
    launch_cutoff_assumed = cutoff or now_utc
    launch_control = {**control, "source_config": config, "launch_cutoff_utc": launch_cutoff_assumed}
    at_launch = _simulate(evidences, control=launch_control, now_utc=now_utc, lookup=lookup(real_q),
                          credited=dict(registry))

    def in_period(o):
        return o["redeemed_at"] is not None and window["start_utc"] <= o["redeemed_at"] < window["end_utc"]

    def key(o):
        # Report a non-qualifying row under its original inviter even when the
        # rule stopped before attribution (e.g. a failed redemption). Display
        # only: no decision above depends on this.
        if o["inviter_id"] is not None:
            return o["inviter_id"]
        if o["invitee_id"] is not None:
            original = _original_referral(pending_by_invitee.get(int(o["invitee_id"]), []), int(o["invitee_id"]))
            if original:
                return original[0]
        return UNATTRIBUTED

    hist = defaultdict(Counter)
    for o in historical:
        if in_period(o):
            hist[key(o)][o["status"]] += 1
    launch = defaultdict(Counter)
    for o in at_launch:
        launch[key(o)][o["status"]] += 1

    redeemed_ok = {o["invitee_id"] for o in historical if o["successful"] and o["invitee_id"] is not None}
    cohort = _attribution_cohort(pending_by_invitee, window)
    awaiting = Counter()
    for invitee, inviter in cohort.items():
        if invitee not in redeemed_ok:
            awaiting[inviter] += 1
    qualified_without_redemption = Counter()
    for row in db.qualified_events.find(
        {"qualified_at": {"$gte": window["start_utc"], "$lt": window["end_utc"]}, "referrer_id": {"$ne": None}},
        {"invitee_id": 1, "referrer_id": 1},
    ):
        try:
            if int(row["invitee_id"]) not in redeemed_ok:
                qualified_without_redemption[int(row["referrer_id"])] += 1
        except (TypeError, ValueError):
            continue

    referrers = set(current) | set(joined) | set(hist) | set(launch) | set(awaiting)
    rows = []
    for r in referrers:
        h, l = hist.get(r, Counter()), launch.get(r, Counter())
        rows.append({
            "referrer_id": str(r),
            "joined": joined.get(r, 0) if r != UNATTRIBUTED else None,
            "current_qualified": current.get(r, 0) if r != UNATTRIBUTED else None,
            "current_qualified_without_redemption": qualified_without_redemption.get(r, 0) if r != UNATTRIBUTED else None,
            "historical": {
                "would_qualify": h[aq.EV_QUALIFIED],
                "duplicate_account_excluded": h[aq.EV_DUPLICATE_ACCOUNT],
                "review": h[aq.EV_REVIEW],
                "rejected": h[aq.EV_REJECTED],
                "awaiting_redemption": awaiting.get(r, 0) if r != UNATTRIBUTED else None,
            },
            "at_launch": {
                "eligible": l[aq.EV_QUALIFIED],
                "duplicate_account_excluded": l[aq.EV_DUPLICATE_ACCOUNT],
                "already_qualified": l[aq.EV_INVITEE_ALREADY_QUALIFIED],
                "review": l[aq.EV_REVIEW],
            },
        })
    rows.sort(key=lambda x: (
        x["referrer_id"] == UNATTRIBUTED,
        -((x["current_qualified"] or 0) + x["historical"]["would_qualify"] + x["at_launch"]["eligible"]),
        x["referrer_id"],
    ))
    payload["affiliates_total"] = len(rows)
    payload["affiliates"] = rows[:AFFILIATE_ROW_LIMIT]

    def total(field, group=None):
        return sum(((x[group] or {}).get(field) or 0) if group else (x[field] or 0) for x in rows)

    payload["totals"] = {
        "joined": sum(joined.values()),
        "current_qualified": sum(current.values()),
        "current_qualified_without_redemption": sum(qualified_without_redemption.values()),
        "historical_would_qualify": total("would_qualify", "historical"),
        "historical_duplicate_account_excluded": total("duplicate_account_excluded", "historical"),
        "historical_review": total("review", "historical"),
        "historical_awaiting_redemption": total("awaiting_redemption", "historical"),
        "at_launch_eligible": total("eligible", "at_launch"),
        "at_launch_duplicate_account_excluded": total("duplicate_account_excluded", "at_launch"),
        "at_launch_already_qualified": total("already_qualified", "at_launch"),
        "at_launch_review": total("review", "at_launch"),
    }
    reasons = Counter()
    cases = []
    for o in historical:
        if not in_period(o) or o["status"] not in (aq.EV_REVIEW, aq.EV_REJECTED, aq.EV_DUPLICATE_ACCOUNT):
            continue
        reasons[f"{o['status']}:{o['reason']}"] += 1
        if o["status"] == aq.EV_REVIEW and len(cases) < REVIEW_CASE_LIMIT:
            referrer = key(o)
            cases.append({
                "referrer_id": None if referrer == UNATTRIBUTED else str(referrer),
                "invitee_id": o["invitee_id"],
                "reason": o["reason"],
                "redeemed_at": o["redeemed_at"].isoformat() if o["redeemed_at"] else None,
                "account": o["account_masked"],
                "code": o["code_masked"],
            })
    payload["outcome_reasons"] = dict(reasons.most_common())
    payload["review_cases"] = cases
    payload["review_cases_total"] = sum(1 for o in historical if in_period(o) and o["status"] == aq.EV_REVIEW)
    payload["source_stats"] = dict(stats)
    payload["evidence_summary"] = _evidence_summary(stats, evidences, historical, payload["source"].get("sync"))
    if stats["truncated"]:
        # Row cap hit: every new-rule figure above is a lower bound, not complete.
        payload["source"]["evidence_incomplete"].append("preview_row_limit_reached")
        payload["evidence_summary"]["truncated"] = True
        payload["evidence_summary"]["basis"] = (
            f"first {PREVIEW_MAX_SOURCE_ROWS:,} committed rows only (row limit reached) — figures are lower bounds"
        )
    return payload


def _evidence_summary(stats, evidences, historical, sync) -> dict:
    """Source-level reconciliation, all time (not period-bound): every row
    received is either unmatched (not a Welcome code), matched to a Welcome
    recipient, or skipped; matched rows then end qualified / duplicate /
    review / rejected. Counts only — nothing here is written anywhere."""
    sync_counters = (sync or {}).get("row_counters") or {}
    status = Counter(o["status"] for o in historical)
    matched = sum(1 for e in evidences if (e.get("recipient_resolution") or {}).get("status") == "ok")
    return {
        # Databot rows reach the preview only if their code is in a Welcome
        # store; the rest were counted at sync time.
        "rows_received": int(sync_counters.get("received", 0)) + int(sync_counters.get("skipped_no_code", 0))
        if sync else stats["rows_scanned"],
        "rows_without_code": int(sync_counters.get("skipped_no_code", 0)) + stats["missing_code"],
        "unmatched_not_welcome": int(sync_counters.get("unknown_code", 0)) + int(sync_counters.get("non_welcome_code", 0))
        + stats["unknown_code"] + stats["non_welcome_code"],
        "welcome_observations": len(evidences),
        "matched_to_one_welcome_recipient": matched,
        "ambiguous_or_unissued_recipient": len(evidences) - matched,
        "removed_by_later_import": stats["removed_observations"],
        "would_qualify": status[aq.EV_QUALIFIED],
        "duplicate_account_excluded": status[aq.EV_DUPLICATE_ACCOUNT],
        "requiring_review": status[aq.EV_REVIEW],
        "rejected": status[aq.EV_REJECTED],
        # All time, including evidence with no usable redemption time (which
        # the period-scoped outcome_reasons cannot place in any month).
        "review_reasons": dict(Counter(o["reason"] for o in historical if o["status"] == aq.EV_REVIEW).most_common()),
        "basis": "all synced redemptions to date, replayed as if the rule had always applied",
    }
