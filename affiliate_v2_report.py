"""Read-only reporting snapshot of ``welcome_redemption_v2`` for Databot.

Databot's Community Dashboard -> Affiliate Performance table shows, per
affiliate, how many invitees are V2 qualified / pending / rejected /
duplicate-account excluded and the exact seven-day turnover of the V2
qualified ones. APReferral is the only decision authority, so this module
*reports persisted decisions*; it never re-evaluates a gate:

* **V2 Qualified**  - ``qualified_events`` with ``rule_version == v2``.
* **Duplicate Account** - V2 evidence finalised ``duplicate_account`` (the
  canonical Account ID registry refused it), no V2 qualification.
* **V2 Rejected** - V2 evidence finalised ``rejected`` (authoritative failure
  only: below 80, ``coupon_new_player = 0``, unsuccessful redemption ...).
  Held evidence (``pending_review``) is never a rejection.
* **V2 Pending** - every other active pending/manual invitee: no evidence yet,
  incomplete / immature / unavailable evidence, feed trouble, review/held.

One current state per invitee, precedence Qualified > Duplicate > Rejected >
Pending, so no invitee is in two columns. Invitees whose qualification is a
legacy (non-V2) event are excluded from every V2 column.

Cohort: the common timestamp is the referral ``joined_at`` - the
``created_at_utc`` of the invitee's ORIGINAL attribution-valid
``pending_referrals`` row (same record ``affiliate_qualification`` freezes
attribution to) - bucketed by GMT+8 calendar day. A month, week or range is
the union of its days, exactly like Databot's date filter.

Snapshot: ``refresh_snapshot`` (scheduler, leased, bounded, flag-gated) replaces
the previous state wholesale - a new generation of ``affiliate_v2_report_rows``
(one row per affiliate x GMT+8 join day), then one pointer swap on
``affiliate_v2_report_state``. Counters are rebuilt, never incremented, so
re-runs/retries are idempotent. The read path (:func:`build_report`) never
runs the activation preview or any aggregation over source collections.

Writes (and only these): ``affiliate_v2_report_state`` and
``affiliate_v2_report_rows``. No raw voucher codes or gaming Account IDs are
read into, stored in or returned by this module.
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, InvalidOperation, Inexact

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError

import affiliate_qualification as aq
import affiliate_qualification_preview as aqp

logger = logging.getLogger(__name__)

CONTRACT = "apreferral.affiliate_v2_report.v1"
STATE_COLLECTION = "affiliate_v2_report_state"
ROWS_COLLECTION = "affiliate_v2_report_rows"
STATE_ID = "current"

COHORT_BASIS = "referral_joined_at"
COHORT_TIMEZONE = "Asia/Kuala_Lumpur (GMT+8)"

REFRESH_LEASE = timedelta(minutes=10)
MIN_ATTEMPT_GAP = timedelta(seconds=60)
DEFAULT_REFRESH_SECONDS = 600
DEFAULT_STALE_AFTER_SECONDS = 1800
CHUNK = 2000
INSERT_BATCH = 1000
DEFAULT_MAX_ACTIVE_PENDING = 250_000
DEFAULT_LIMIT = 2000
MAX_LIMIT = 5000
MAX_WINDOW_DAYS = 400

ST_QUALIFIED = "qualified"
ST_DUPLICATE = "duplicate"
ST_REJECTED = "rejected"
ST_PENDING = "pending"
_PRECEDENCE = {ST_QUALIFIED: 0, ST_DUPLICATE: 1, ST_REJECTED: 2, ST_PENDING: 3}
_FIELD = {ST_QUALIFIED: "qualified", ST_DUPLICATE: "duplicate", ST_REJECTED: "rejected", ST_PENDING: "pending"}

# Exact arithmetic: wide enough that summing source amounts can never round;
# an operation that would be inexact raises instead of silently rounding.
_EXACT = Context(prec=120, traps=[Inexact, InvalidOperation])
_DECIMAL_TEXT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")

SOURCE_OK = "ok"
SOURCE_STALE = "stale"
SOURCE_PARTIAL = "partial"
SOURCE_UNAVAILABLE = "unavailable"

TURNOVER_COMPLETE = "complete"
TURNOVER_PARTIAL = "partial"
TURNOVER_UNAVAILABLE = "unavailable"


def enabled() -> bool:
    return os.getenv("AFFILIATE_V2_REPORT_ENABLED", "false").strip().lower() == "true"


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, "") or default))
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int(value):
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def exact_decimal(raw) -> Decimal | None:
    """Source ``after_total_bet_amount`` -> Decimal, parsed from its text with
    the same grammar the qualification gate accepts. No float arithmetic."""
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float, Decimal)):
        return None
    text = str(raw).strip()
    if not _DECIMAL_TEXT.fullmatch(text):
        return None
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() and value >= 0 else None


def _plain(value: Decimal) -> str:
    return format(value, "f")


def gmt8_day(moment: datetime) -> str:
    return aq._aware_utc(moment).astimezone(aq.KL_TZ).strftime("%Y-%m-%d")


def rule_status(control: dict | None, now_utc: datetime) -> dict:
    """``launched``: V2 has a persisted cutoff that has passed and was not
    disabled - decisions exist and are reportable (also while paused).
    ``rule_active``: V2 is currently processing (aq.new_rule_active)."""
    mode = (control or {}).get("mode") or aq.MODE_DISABLED
    cutoff = aq.launch_cutoff(control)
    launched = bool(
        control and control.get("rule_version") == aq.RULE_VERSION and cutoff is not None
        and aq._aware_utc(now_utc) >= cutoff and mode in (aq.MODE_ACTIVE, aq.MODE_PAUSED)
    )
    return {"rule_active": aq.new_rule_active(control, now_utc), "launched": launched, "rule_mode": mode}


def source_status(db, control: dict | None, now_utc: datetime) -> dict:
    """Freshness/completeness of the Databot evidence the decisions rest on.
    Same classification the Qualification Preview uses (read-only)."""
    config = (control or {}).get("source_config")
    problems = aq.validate_source_config(config)
    integration = aq.integration_readiness(db, config, now_utc=now_utc) if not problems else {}
    all_problems = sorted(set(problems + list(integration.get("problems") or [])))
    sync = integration.get("sync") or {}
    coverage = aq.parse_redeemed_at(sync.get("coverage_end"), "Asia/Kuala_Lumpur")
    blocking = [p for p in all_problems if p not in aqp.NON_BLOCKING_PROBLEMS]
    if blocking:
        status = SOURCE_UNAVAILABLE
    elif "uim_batches_partially_synced" in all_problems:
        status = SOURCE_PARTIAL
    elif all_problems:
        status = SOURCE_STALE
    else:
        status = SOURCE_OK
    last_ok = aq.parse_redeemed_at(sync.get("last_success_at"), "UTC")
    return {
        "status": status,
        "problems": all_problems,
        "last_sync_success_at": last_ok.isoformat() if last_ok else None,
        "uim_coverage_end": coverage.astimezone(aq.KL_TZ).isoformat() if coverage else None,
    }


# ---------------------------------------------------------------------------
# Snapshot build (scheduler path)
# ---------------------------------------------------------------------------

def _collect_states(db, *, max_active: int, stats: Counter) -> dict[int, dict]:
    """invitee -> {state, ...}: one current V2 state per invitee."""
    state: dict[int, dict] = {}
    for ev in db.qualified_events.find(
        {"rule_version": aq.RULE_VERSION},
        {"invitee_id": 1, "referrer_id": 1, "evidence_id": 1, "referral_pending_id": 1},
    ):
        invitee = _int(ev.get("invitee_id"))
        if invitee is None:
            stats["qualified_without_invitee"] += 1
            continue
        state[invitee] = {"state": ST_QUALIFIED, "referrer": _int(ev.get("referrer_id")),
                          "pending_id": ev.get("referral_pending_id"), "evidence_id": ev.get("evidence_id")}
    for ev in db[aq.EVIDENCE_COLLECTION].find(
        {"rule_version": aq.RULE_VERSION, "status": {"$in": [aq.EV_DUPLICATE_ACCOUNT, aq.EV_REJECTED]}},
        {"status": 1, "invitee_id": 1, "recipient_resolution.status": 1, "recipient_resolution.user_id": 1},
    ):
        invitee = _int(ev.get("invitee_id"))
        if invitee is None:
            recipient = ev.get("recipient_resolution") or {}
            invitee = _int(recipient.get("user_id")) if recipient.get("status") == "ok" else None
        if invitee is None:
            stats["terminal_without_invitee"] += 1
            continue
        new = ST_DUPLICATE if ev.get("status") == aq.EV_DUPLICATE_ACCOUNT else ST_REJECTED
        current = state.get(invitee)
        if current is None or _PRECEDENCE[new] < _PRECEDENCE[current["state"]]:
            state[invitee] = {"state": new}
    active = 0
    for row in db.pending_referrals.find({"status": {"$in": list(aq.ACTIVE_PENDING_STATUSES)}},
                                         {"invitee_user_id": 1}):
        invitee = _int(row.get("invitee_user_id"))
        if invitee is None or invitee in state:
            continue
        if active >= max_active:
            stats["truncated_active_pending"] = 1
            break
        state[invitee] = {"state": ST_PENDING}
        active += 1
    return state


def _chunks(items, size=CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def build_rows(db, *, now_utc: datetime, max_active: int | None = None) -> tuple[list[dict], dict]:
    """One current state per invitee -> rows keyed by (affiliate, GMT+8 join
    day). Pure function of the persisted decisions; reads only."""
    stats: Counter = Counter()
    state = _collect_states(db, max_active=max_active or DEFAULT_MAX_ACTIVE_PENDING, stats=stats)

    # A legacy (non-V2) qualification is historical and unchanged: it belongs
    # to no V2 column.
    for chunk in _chunks([i for i, s in state.items() if s["state"] != ST_QUALIFIED]):
        for ev in db.qualified_events.find({"invitee_id": {"$in": chunk}, "rule_version": {"$ne": aq.RULE_VERSION}},
                                           {"invitee_id": 1}):
            invitee = _int(ev.get("invitee_id"))
            if invitee in state:
                del state[invitee]
                stats["legacy_qualified_excluded"] += 1

    referral_rows: dict[int, list[dict]] = defaultdict(list)
    for chunk in _chunks(state):
        for row in db.pending_referrals.find(
            {"invitee_user_id": {"$in": chunk}},
            {"invitee_user_id": 1, "inviter_user_id": 1, "status": 1, "revoked_reason": 1, "created_at_utc": 1},
        ):
            invitee = _int(row.get("invitee_user_id"))
            if invitee is not None:
                referral_rows[invitee].append(row)
    far = datetime.max.replace(tzinfo=timezone.utc)

    placed: dict[int, tuple[int, str]] = {}
    for invitee, info in state.items():
        rows = sorted(referral_rows.get(invitee, []), key=lambda r: aq._aware_utc(r.get("created_at_utc")) or far)
        original = aqp._original_referral(rows, invitee)
        affiliate, joined = (original if original else (None, None))
        if info["state"] == ST_QUALIFIED:
            # Frozen attribution on the event itself wins; the referral row it
            # points at supplies the join time.
            affiliate = info.get("referrer") if info.get("referrer") is not None else affiliate
            linked = next((r for r in rows if r.get("_id") == info.get("pending_id")), None)
            if linked is not None and aq._aware_utc(linked.get("created_at_utc")) is not None:
                joined = aq._aware_utc(linked["created_at_utc"])
        if affiliate is None or joined is None:
            stats[f"unattributed_{info['state']}"] += 1
            continue
        placed[invitee] = (int(affiliate), gmt8_day(joined))

    turnover: dict[int, Decimal | None] = {}
    qualified = [i for i in placed if state[i]["state"] == ST_QUALIFIED]
    by_evidence = {state[i]["evidence_id"]: i for i in qualified if state[i].get("evidence_id") is not None}
    for chunk in _chunks(by_evidence):
        for ev in db[aq.EVIDENCE_COLLECTION].find({"_id": {"$in": chunk}},
                                                  {"qualification_fields.after_total_bet_amount": 1}):
            amount = exact_decimal(((ev.get("qualification_fields") or {}).get("after_total_bet_amount")))
            turnover[by_evidence[ev["_id"]]] = amount

    cells: dict[tuple[str, str], dict] = {}
    for invitee, (affiliate, day) in placed.items():
        cell = cells.setdefault((str(affiliate), day), {
            "affiliate_id": str(affiliate), "day": day, "qualified": 0, "pending": 0, "rejected": 0,
            "duplicate": 0, "turnover": Decimal(0), "turnover_unresolved": 0})
        cell[_FIELD[state[invitee]["state"]]] += 1
        if state[invitee]["state"] == ST_QUALIFIED:
            amount = turnover.get(invitee)
            if amount is None:
                cell["turnover_unresolved"] += 1
            else:
                try:
                    cell["turnover"] = _EXACT.add(cell["turnover"], amount)
                except (Inexact, InvalidOperation):
                    cell["turnover_unresolved"] += 1
    out = []
    for cell in cells.values():
        cell["turnover"] = _plain(cell["turnover"])
        out.append(cell)
    stats["invitees"] = len(state)
    stats["placed"] = len(placed)
    for st in _FIELD:
        stats[f"state_{st}"] = sum(1 for i in placed if state[i]["state"] == st)
    return out, dict(stats)


def ensure_indexes(db) -> None:
    """Indexes on this module's OWN new collections only; no existing
    collection is touched. Called lazily by the (flag-gated) refresh."""
    db[ROWS_COLLECTION].create_index([("generation", ASCENDING), ("affiliate_id", ASCENDING), ("day", ASCENDING)],
                                     unique=True, name="v2_report_gen_affiliate_day")
    db[ROWS_COLLECTION].create_index([("generation", ASCENDING), ("day", ASCENDING)], name="v2_report_gen_day")


def _acquire(db, now_utc: datetime) -> str | None:
    from pymongo import ReturnDocument

    owner = uuid.uuid4().hex
    try:
        doc = db[STATE_COLLECTION].find_one_and_update(
            {"_id": STATE_ID, "$or": [{"lease_until": {"$exists": False}}, {"lease_until": None},
                                      {"lease_until": {"$lt": now_utc}}]},
            {"$set": {"lease_until": now_utc + REFRESH_LEASE, "lease_owner": owner, "last_attempt_at": now_utc}},
            upsert=True, return_document=ReturnDocument.AFTER)
    except DuplicateKeyError:
        return None
    return owner if doc and doc.get("lease_owner") == owner else None


def refresh_snapshot(db, *, now_utc: datetime) -> dict:
    """Rebuild the snapshot from persisted decisions and swap it in. The
    previous generation stays current until the new one is fully written, so
    a failure leaves the last good snapshot in place."""
    out = {"ok": False}
    owner = _acquire(db, now_utc)
    if owner is None:
        out["skipped"] = "lease_held"
        return out
    generation = None
    try:
        control = aq.get_control(db)
        rule = rule_status(control, now_utc)
        meta = {"rule_mode": rule["rule_mode"], "rule_active": rule["rule_active"], "launched": rule["launched"],
                "rule_version": aq.RULE_VERSION, "generated_at": now_utc, "last_error": None,
                "last_success_at": now_utc, "lease_until": None}
        if not rule["launched"]:
            # Nothing to report; never publish operational zeros before launch.
            db[STATE_COLLECTION].update_one({"_id": STATE_ID, "lease_owner": owner},
                                            {"$set": {**meta, "generation": None, "source": None, "build": None}})
            db[ROWS_COLLECTION].delete_many({})
            out.update(ok=True, launched=False)
            return out
        ensure_indexes(db)
        source = source_status(db, control, now_utc)
        rows, stats = build_rows(db, now_utc=now_utc,
                                 max_active=_env_int("AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING", DEFAULT_MAX_ACTIVE_PENDING))
        generation = uuid.uuid4().hex
        for chunk in _chunks(rows, INSERT_BATCH):
            db[ROWS_COLLECTION].insert_many([{**r, "generation": generation} for r in chunk], ordered=False)
        swapped = db[STATE_COLLECTION].update_one(
            {"_id": STATE_ID, "lease_owner": owner},
            {"$set": {**meta, "generation": generation, "source": source,
                      "build": {**stats, "rows": len(rows), "truncated": bool(stats.get("truncated_active_pending"))}}})
        if not swapped.modified_count:
            raise RuntimeError("lease_lost")
        db[ROWS_COLLECTION].delete_many({"generation": {"$ne": generation}})
        out.update(ok=True, launched=True, generation=generation, rows=len(rows), invitees=stats.get("placed", 0))
        return out
    except Exception as exc:
        logger.warning("[V2_REPORT][REFRESH_FAILED] err=%s", exc.__class__.__name__)
        if generation:
            try:
                db[ROWS_COLLECTION].delete_many({"generation": generation})
            except Exception:
                pass
        db[STATE_COLLECTION].update_one({"_id": STATE_ID, "lease_owner": owner},
                                        {"$set": {"last_error": exc.__class__.__name__, "last_failure_at": now_utc,
                                                  "lease_until": None}})
        out["error"] = exc.__class__.__name__
        return out
    finally:
        db[STATE_COLLECTION].update_one({"_id": STATE_ID, "lease_owner": owner}, {"$set": {"lease_until": None}})


def run_scheduled_refresh(db, *, now_utc: datetime | None = None) -> dict:
    """Scheduler entry point. No-op unless AFFILIATE_V2_REPORT_ENABLED=true;
    skips while the snapshot is fresh and the rule state is unchanged."""
    if not enabled():
        return {"skipped": "report_disabled"}
    now_utc = now_utc or datetime.now(timezone.utc)
    state = db[STATE_COLLECTION].find_one({"_id": STATE_ID}) or {}
    attempt = aq._aware_utc(state.get("last_attempt_at"))
    if attempt and now_utc - attempt < MIN_ATTEMPT_GAP:
        return {"skipped": "recent_attempt"}
    rule = rule_status(aq.get_control(db), now_utc)
    success = aq._aware_utc(state.get("last_success_at"))
    interval = timedelta(seconds=_env_int("AFFILIATE_V2_REPORT_REFRESH_SECONDS", DEFAULT_REFRESH_SECONDS))
    unchanged = (state.get("rule_mode") == rule["rule_mode"] and bool(state.get("launched")) == rule["launched"])
    if success and unchanged and now_utc - success < interval:
        return {"skipped": "fresh"}
    return refresh_snapshot(db, now_utc=now_utc)


# ---------------------------------------------------------------------------
# Read path (API) - never builds, never touches source collections
# ---------------------------------------------------------------------------

class WindowError(ValueError):
    pass


def parse_window(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    """Inclusive GMT+8 calendar days (the dashboard's ``resolve_date_range``)."""
    try:
        start = datetime.strptime(str(start_date), "%Y-%m-%d")
        end = datetime.strptime(str(end_date), "%Y-%m-%d")
    except ValueError:
        raise WindowError("start_date and end_date must be YYYY-MM-DD") from None
    if start > end:
        raise WindowError("start_date must be <= end_date")
    if (end - start).days >= MAX_WINDOW_DAYS:
        raise WindowError(f"window exceeds {MAX_WINDOW_DAYS} days")
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _iso(value) -> str | None:
    value = aq._aware_utc(value)
    return value.isoformat() if value else None


def build_report(db, *, start_date: str, end_date: str, now_utc: datetime, limit: int = DEFAULT_LIMIT) -> dict:
    start, end = parse_window(start_date, end_date)
    limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    control = aq.get_control(db)
    rule = rule_status(control, now_utc)
    report = {
        "contract": CONTRACT,
        "rule_active": rule["rule_active"],
        "rule_mode": rule["rule_mode"],
        "rule_version": aq.RULE_VERSION,
        "cohort": {"basis": COHORT_BASIS, "timezone": COHORT_TIMEZONE, "start_date": start, "end_date": end},
        "status": "inactive",
        "snapshot": None,
        "source": None,
        "uim_coverage_end": None,
        "totals": None,
        "affiliates": None,
        "affiliates_total": None,
        "truncated": False,
    }
    if not rule["launched"]:
        return report

    rows = None
    state = None
    for _ in range(2):
        state = db[STATE_COLLECTION].find_one({"_id": STATE_ID}) or {}
        generation = state.get("generation")
        if not generation or not state.get("launched"):
            break
        rows = list(db[ROWS_COLLECTION].find(
            {"generation": generation, "day": {"$gte": start, "$lte": end}},
            {"_id": 0, "affiliate_id": 1, "qualified": 1, "pending": 1, "rejected": 1, "duplicate": 1,
             "turnover": 1, "turnover_unresolved": 1}))
        # A refresh may swap generations between the pointer read and the row
        # read (the old rows are then deleted): retry rather than report zeros.
        latest = db[STATE_COLLECTION].find_one({"_id": STATE_ID}, {"generation": 1}) or {}
        if latest.get("generation") == generation:
            break
        rows = None
    generated_at = aq._aware_utc(state.get("generated_at")) if state else None
    if rows is None or generated_at is None:
        report["status"] = "no_snapshot"
        return report

    age = max(0, int((now_utc - generated_at).total_seconds()))
    stale = age > _env_int("AFFILIATE_V2_REPORT_STALE_AFTER_SECONDS", DEFAULT_STALE_AFTER_SECONDS)
    source = state.get("source") or {"status": SOURCE_UNAVAILABLE, "problems": ["source_status_missing"]}
    build = state.get("build") or {}
    truncated_build = bool(build.get("truncated"))
    report["snapshot"] = {
        "generation": state["generation"], "generated_at": generated_at.isoformat(), "age_seconds": age,
        "stale": stale, "unattributed_invitees": sum(int(v) for k, v in build.items() if k.startswith("unattributed_")),
    }
    report["source"] = {k: source.get(k) for k in ("status", "problems", "last_sync_success_at")}
    report["uim_coverage_end"] = source.get("uim_coverage_end")

    per: dict[str, dict] = {}
    for r in rows:
        a = per.setdefault(r["affiliate_id"], {"v2_qualified": 0, "v2_pending": 0, "v2_rejected": 0,
                                               "duplicate_account_excluded": 0, "_turnover": Decimal(0),
                                               "_unresolved": 0})
        a["v2_qualified"] += int(r.get("qualified") or 0)
        a["v2_pending"] += int(r.get("pending") or 0)
        a["v2_rejected"] += int(r.get("rejected") or 0)
        a["duplicate_account_excluded"] += int(r.get("duplicate") or 0)
        amount = exact_decimal(r.get("turnover"))
        if amount is None:
            a["_unresolved"] += max(1, int(r.get("qualified") or 0))
        else:
            a["_turnover"] = _EXACT.add(a["_turnover"], amount)
        a["_unresolved"] += int(r.get("turnover_unresolved") or 0)

    source_ok = source.get("status") == SOURCE_OK
    unavailable = source.get("status") == SOURCE_UNAVAILABLE

    def finish(a: dict) -> dict:
        complete = source_ok and not truncated_build and not a["_unresolved"] and not stale
        if unavailable:
            value, tstatus = None, TURNOVER_UNAVAILABLE
        else:
            value, tstatus = _plain(a["_turnover"]), TURNOVER_COMPLETE if complete else TURNOVER_PARTIAL
        return {"v2_qualified": a["v2_qualified"], "v2_pending": a["v2_pending"], "v2_rejected": a["v2_rejected"],
                "duplicate_account_excluded": a["duplicate_account_excluded"],
                "seven_day_turnover": value, "seven_day_turnover_status": tstatus}

    totals_acc = {"v2_qualified": 0, "v2_pending": 0, "v2_rejected": 0, "duplicate_account_excluded": 0,
                  "_turnover": Decimal(0), "_unresolved": 0}
    for a in per.values():
        for k in ("v2_qualified", "v2_pending", "v2_rejected", "duplicate_account_excluded", "_unresolved"):
            totals_acc[k] += a[k]
        totals_acc["_turnover"] = _EXACT.add(totals_acc["_turnover"], a["_turnover"])
    report["totals"] = finish(totals_acc)

    ordered = sorted(per.items(), key=lambda kv: (-kv[1]["v2_qualified"], -kv[1]["v2_pending"], kv[0]))
    report["affiliates_total"] = len(ordered)
    report["truncated"] = len(ordered) > limit
    report["affiliates"] = [{"affiliate_id": aid, **finish(a)} for aid, a in ordered[:limit]]

    if unavailable:
        report["status"] = SOURCE_UNAVAILABLE
    elif stale:
        report["status"] = "stale"
    elif truncated_build:
        report["status"] = SOURCE_PARTIAL
    else:
        report["status"] = source.get("status") or SOURCE_UNAVAILABLE  # ok | partial | stale
    return report
