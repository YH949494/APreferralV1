"""Canonical affiliate qualification: verified Welcome voucher redemption.

Rule ``welcome_redemption_v1`` (see docs/affiliate_welcome_redemption_qualification.md):

* An invitee qualifies only on committed, verified, *successful* redemption of
  their own Welcome voucher. Joining, checking in, claiming, receiving or
  copying a Welcome voucher never qualifies anyone once the rule is launched.
* One qualification per invitee Telegram ID (``qualified_events.uniq_invitee_id``)
  and one per canonical gaming account, lifetime, across every Telegram user,
  affiliate and month (``affiliate_account_registry.uniq_account_registry_key``
  plus the partial unique ``qualified_events.uniq_qualified_account_key_v1``).
* Attribution is frozen to the ORIGINAL referral record (earliest
  attribution-valid ``pending_referrals`` row created before the redemption),
  never to a mutable "current inviter" field, and never to whoever merely holds
  the code: the credited invitee is always the code's original recipient.

Lifecycle
---------
``affiliate_qualification_control`` (one document, ``_id = RULE_VERSION``) is
the persisted switch. It carries ``mode`` (disabled / active / paused) and,
once launched, an immutable ``launch_cutoff_utc``. From the cutoff on the
legacy check-in/channel settlement in ``scheduler.settle_pending_referrals``
no longer awards anything — permanently, even while ``paused`` — so a
rollback can only ever pause new qualifications, never reopen the old rule.

Evidence flows ``marketing_upload_batches`` (committed) -> ``welcome_redemption_evidence``
(idempotent on ``(source, source_ref)``) -> :func:`process_pending_evidence`.
Qualification is a two-write protocol without multi-document transactions:

1. reserve the account in the registry (unique ``account_key``, owner =
   evidence ``_id``);
2. insert the ``qualified_events`` row (unique ``invitee_id`` and partial
   unique ``account_key``);
3. flip the reservation to ``committed``.

A crash between any two steps is recoverable: the evidence stays ``received``
with an expiring lease, and re-processing it finds its *own* reservation /
event and resumes. Any non-qualifying outcome releases a reservation owned by
that evidence, and :func:`reconcile` releases stale reservations whose evidence
can no longer qualify, so a reservation can never be consumed permanently
without its qualification.

Voucher codes and gaming account ids never reach logs: evidence stores a code
hash plus a masked form; logs use masked values and evidence ids only.
"""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from datetime import datetime, timedelta, timezone

import pytz
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)

RULE_VERSION = "welcome_redemption_v1"
CONTROL_COLLECTION = "affiliate_qualification_control"
EVIDENCE_COLLECTION = "welcome_redemption_evidence"
REGISTRY_COLLECTION = "affiliate_account_registry"
CONTROL_ID = RULE_VERSION

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

MODE_DISABLED = "disabled"
MODE_ACTIVE = "active"
MODE_PAUSED = "paused"

SOURCE_MARKETING = "marketing_raw_data"
WELCOME_POOL_ID = "WELCOME"

EV_RECEIVED = "received"
EV_QUALIFIED = "qualified"
EV_REJECTED = "rejected"
EV_REVIEW = "pending_review"
EV_DUPLICATE_ACCOUNT = "duplicate_account"
EV_INVITEE_ALREADY_QUALIFIED = "invitee_already_qualified"
EV_VOIDED = "voided"

REG_RESERVED = "reserved"
REG_COMMITTED = "committed"
# The account belongs to an invitee whose qualification predates this rule
# (discovered live from verified redemption evidence). It counts as credited.
REG_CREDITED_LEGACY = "credited_legacy"
# Seeded by scripts/migrate_affiliate_account_dedupe.py from historical evidence.
REG_SEEDED = "seeded"
CREDITED_STATES = (REG_COMMITTED, REG_CREDITED_LEGACY, REG_SEEDED)

PENDING_AWAITING_REDEMPTION = "awaiting_redemption"
# A legacy settlement revocation that says the referral record itself is not
# a valid attribution (as opposed to "did not meet the old qualification
# rule", e.g. not_in_official_channel / insufficient_engagement, which the new
# rule deliberately ignores).
ATTRIBUTION_INVALID_REVOKE_REASONS = frozenset({"invalid_ids", "self_invite", "already_in_db"})

UNLINKED_POLICY_ACCEPT = "accept"
UNLINKED_POLICY_REVIEW = "review"

EVIDENCE_LEASE = timedelta(minutes=5)
BATCH_EXTRACT_LEASE = timedelta(minutes=10)
MAX_EVIDENCE_ATTEMPTS = 10
STALE_RESERVATION_AFTER = timedelta(minutes=15)

# Column aliases marketing_upload accepts as a "redeem time" but which are
# claim times, not redemption times. Never acceptable as redeemed_at.
_CLAIM_TIME_COLUMNS = frozenset({"claim_time", "claimed_at"})
_NULLISH = frozenset({"none", "null", "nan", "n/a", "na", "-"})
_NAMESPACE_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
)


# ---------------------------------------------------------------------------
# Control document
# ---------------------------------------------------------------------------

def get_control(db) -> dict | None:
    """The persisted rule switch, or None when it has never been written.

    Raises on a real database error so callers fail closed; a test double
    without the collection reads as "never configured" (legacy behaviour).
    """
    col = getattr(db, CONTROL_COLLECTION, None)
    if col is None:
        return None
    doc = col.find_one({"_id": CONTROL_ID})
    return doc if isinstance(doc, dict) else None


def _aware_utc(value) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def launch_cutoff(control: dict | None) -> datetime | None:
    return _aware_utc((control or {}).get("launch_cutoff_utc"))


def legacy_award_allowed(control: dict | None, now_utc: datetime) -> bool:
    """True while the legacy check-in/channel settlement may still qualify.

    Once a cutoff is persisted this is False from the cutoff on, whatever the
    mode — pausing the new rule never reactivates the old one.
    """
    cutoff = launch_cutoff(control)
    return cutoff is None or _aware_utc(now_utc) < cutoff


def new_rule_active(control: dict | None, now_utc: datetime) -> bool:
    cutoff = launch_cutoff(control)
    return bool(
        control
        and control.get("mode") == MODE_ACTIVE
        and cutoff is not None
        and _aware_utc(now_utc) >= cutoff
    )


# ---------------------------------------------------------------------------
# Identity normalisation (no voucher code / account id ever logged raw)
# ---------------------------------------------------------------------------

def mask_value(value) -> str:
    text = str(value or "")
    if len(text) <= 4:
        return "*" * len(text)
    return text[:2] + "*" * (len(text) - 4) + text[-2:]


def normalize_namespace(raw) -> str | None:
    if raw is None or isinstance(raw, (int, float)):
        return None
    text = unicodedata.normalize("NFKC", str(raw)).strip().lower()
    return text if _NAMESPACE_RE.match(text) else None


def canonical_account_key(raw_account, namespace) -> tuple[str | None, str | None]:
    """``(account_key, None)`` or ``(None, reason)``.

    Leading zeros and case are preserved: the account id is compared exactly
    as the provider issued it, inside an explicit provider/tenant namespace
    (``"<namespace>:<account>"``). A numeric cell (an XLSX value already
    coerced to int/float) is refused, because its leading zeros may already be
    gone and there is no way to recover them.
    """
    if raw_account is None:
        return None, "missing_account"
    if isinstance(raw_account, bool):
        return None, "invalid_account"
    if isinstance(raw_account, (int, float)):
        return None, "account_numeric_coerced"
    text = unicodedata.normalize("NFKC", str(raw_account)).strip()
    if not text or text.lower() in _NULLISH:
        return None, "missing_account"
    if any(ch.isspace() or unicodedata.category(ch).startswith("C") for ch in text):
        return None, "invalid_account"
    ns = normalize_namespace(namespace)
    if ns is None:
        return None, "missing_account_namespace"
    return f"{ns}:{text}", None


def account_id_from_key(account_key: str) -> str:
    return str(account_key).split(":", 1)[1]


def normalize_code(raw) -> str | None:
    if raw is None or isinstance(raw, (bool, float)):
        return None
    text = unicodedata.normalize("NFKC", str(raw)).strip()
    if not text or text.lower() in _NULLISH:
        return None
    return text


def code_hash(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def kl_month_key(moment: datetime) -> str:
    return _aware_utc(moment).astimezone(KL_TZ).strftime("%Y%m")


def parse_redeemed_at(value, source_timezone: str) -> datetime | None:
    """Parse a source redemption timestamp into aware UTC.

    Naive values are interpreted in the source's declared timezone (exports
    are GMT+8 wall-clock), never silently as UTC. Date-only values are refused:
    a day without a time cannot place a redemption on the right side of a
    GMT+8 month boundary.
    """
    tz = pytz.timezone(source_timezone)
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else tz.localize(value).astimezone(timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= 10:
        return None
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            return tz.localize(parsed).astimezone(timezone.utc)
        return parsed.astimezone(timezone.utc)
    for fmt in _DATETIME_FORMATS:
        try:
            return tz.localize(datetime.strptime(text, fmt)).astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _norm_column(name) -> str:
    return str(name if name is not None else "").strip().lower().replace(" ", "_").replace("-", "_")


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

# (collection, name, keys, options). Uniqueness is what the dedupe guarantees
# rest on; the non-unique ones keep every per-evidence lookup off a scan.
INDEX_SPECS = (
    ("qualified_events", "uniq_invitee_id", [("invitee_id", ASCENDING)], {"unique": True}),
    (
        "qualified_events",
        "uniq_qualified_account_key_v1",
        [("account_key", ASCENDING)],
        {"unique": True, "partialFilterExpression": {"account_key": {"$type": "string"}}},
    ),
    (
        "qualified_events",
        "aq_qualified_effects_pending",
        [("rule_version", ASCENDING), ("effects_applied_at", ASCENDING)],
        {},
    ),
    (REGISTRY_COLLECTION, "uniq_account_registry_key", [("account_key", ASCENDING)], {"unique": True}),
    (REGISTRY_COLLECTION, "account_registry_state", [("state", ASCENDING), ("reserved_at", ASCENDING)], {}),
    (REGISTRY_COLLECTION, "account_registry_evidence", [("evidence_id", ASCENDING)], {}),
    (
        EVIDENCE_COLLECTION,
        "uniq_evidence_source_ref",
        [("source", ASCENDING), ("source_ref", ASCENDING)],
        {"unique": True},
    ),
    (EVIDENCE_COLLECTION, "evidence_status_created", [("status", ASCENDING), ("created_at", ASCENDING)], {}),
    (EVIDENCE_COLLECTION, "evidence_code_hash", [("code_hash", ASCENDING)], {}),
    (EVIDENCE_COLLECTION, "evidence_batches", [("source_batch_ids", ASCENDING)], {}),
    (
        "pending_referrals",
        "aq_pending_by_invitee",
        [("invitee_user_id", ASCENDING), ("created_at_utc", ASCENDING)],
        {},
    ),
    (
        "marketing_raw_data",
        "aq_marketing_batch_rows",
        [("upload_batch_id", ASCENDING), ("_id", ASCENDING)],
        {},
    ),
    ("voucher_pools", "aq_voucher_code_lookup", [("code", ASCENDING)], {}),
    ("new_joiner_claims", "aq_new_joiner_claims_code", [("code", ASCENDING)], {}),
    ("users", "aq_users_linked_gaming_accounts", [("linked_gaming_accounts", ASCENDING)], {}),
)


def _index_matches(info: dict, keys, options: dict) -> bool:
    key = info.get("key") or {}
    key_items = list(key.items()) if isinstance(key, dict) else list(key)
    if [(k, int(v)) for k, v in key_items] != [(k, int(v)) for k, v in keys]:
        return False
    if options.get("unique") and not info.get("unique"):
        return False
    if "partialFilterExpression" in options and info.get("partialFilterExpression") != options["partialFilterExpression"]:
        return False
    return True


def index_readiness(db) -> dict:
    """Which required indexes exist (matched by key pattern + uniqueness,
    so an equivalent index under another name also counts)."""
    missing = []
    for col_name, name, keys, options in INDEX_SPECS:
        try:
            existing = list(db[col_name].list_indexes())
        except Exception:
            existing = []
        if not any(_index_matches(info, keys, options) for info in existing):
            missing.append(f"{col_name}.{name}")
    return {"ok": not missing, "missing": missing}


def ensure_indexes(db) -> list[str]:
    """Create every required index that is not already present. Only the
    migration script calls this (``--apply``); the app never builds these
    implicitly at startup."""
    created = []
    for col_name, name, keys, options in INDEX_SPECS:
        existing = list(db[col_name].list_indexes())
        if any(_index_matches(info, keys, options) for info in existing):
            continue
        db[col_name].create_index(keys, name=name, **options)
        created.append(f"{col_name}.{name}")
    return created


# ---------------------------------------------------------------------------
# Source configuration and integration readiness
# ---------------------------------------------------------------------------

REQUIRED_SOURCE_KEYS = (
    "code_column",
    "account_column",
    "redeemed_at_column",
    "status_column",
    "success_values",
    "source_timezone",
)


def validate_source_config(config: dict | None) -> list[str]:
    """Problems that make a redemption source unusable as authoritative
    evidence. Empty list = structurally usable."""
    cfg = config or {}
    problems = [f"missing:{key}" for key in REQUIRED_SOURCE_KEYS if not cfg.get(key)]
    if cfg.get("source") not in (None, SOURCE_MARKETING):
        problems.append("unsupported_source")
    if cfg.get("redeemed_at_column") and _norm_column(cfg["redeemed_at_column"]) in _CLAIM_TIME_COLUMNS:
        problems.append("redeemed_at_column_is_a_claim_time")
    if cfg.get("source_timezone"):
        try:
            pytz.timezone(cfg["source_timezone"])
        except Exception:
            problems.append("invalid_source_timezone")
    if not cfg.get("namespace_column") and normalize_namespace(cfg.get("account_namespace")) is None:
        problems.append("missing:account_namespace_or_namespace_column")
    if cfg.get("success_values") is not None and not isinstance(cfg.get("success_values"), (list, tuple)):
        problems.append("success_values_not_a_list")
    return problems


def integration_readiness(db, config: dict | None, *, sample_batches: int = 3) -> dict:
    """Structural config check plus proof that recent committed uploads carry
    every configured column. Read-only."""
    problems = validate_source_config(config)
    cfg = config or {}
    batches = list(
        db.marketing_upload_batches.find(
            {"status": {"$in": ["completed", "completed_with_errors"]}, "rolled_back_at": {"$exists": False}},
            {"upload_batch_id": 1},
        ).sort("uploaded_at", -1).limit(int(sample_batches))
    )
    if not batches:
        problems.append("no_committed_marketing_batches")
    wanted = [cfg.get(k) for k in ("code_column", "account_column", "redeemed_at_column", "status_column")]
    if cfg.get("namespace_column"):
        wanted.append(cfg["namespace_column"])
    wanted = [_norm_column(w) for w in wanted if w]
    for batch in batches:
        row = db.marketing_raw_data.find_one({"upload_batch_id": batch.get("upload_batch_id")})
        if not row:
            continue
        present = {_norm_column(k) for k in row.keys()}
        for col in wanted:
            if col not in present:
                problems.append(f"column_absent_in_recent_batch:{col}")
    return {"ok": not problems, "problems": sorted(set(problems))}


# ---------------------------------------------------------------------------
# Code -> original recipient
# ---------------------------------------------------------------------------

def resolve_welcome_recipient(db, code: str) -> dict:
    """Map a redeemed code to its ORIGINAL Welcome recipient.

    Sources: the canonical WELCOME pool (``voucher_pools`` issued_to_user_id,
    cross-checked against the user's ``affiliate_ledger`` WELCOME row) and the
    legacy ``new_joiner_claims`` record. Any disagreement is ``ambiguous``.
    """
    recipients: dict[int, set] = {}
    pool_row = db.voucher_pools.find_one({"pool_id": WELCOME_POOL_ID, "code": code})
    if pool_row:
        raw_uid = pool_row.get("issued_to_user_id")
        if raw_uid in (None, ""):
            raw_uid = pool_row.get("issued_to")
        if pool_row.get("status") != "issued" or raw_uid in (None, ""):
            return {"status": "not_issued"}
        try:
            uid = int(raw_uid)
        except (TypeError, ValueError):
            return {"status": "ambiguous", "detail": "unparseable_recipient"}
        recipients.setdefault(uid, set()).add("voucher_pools")
        ledger = db.affiliate_ledger.find_one({"dedup_key": f"WELCOME:{uid}"}, {"voucher_code": 1})
        ledger_code = (ledger or {}).get("voucher_code")
        if ledger_code and ledger_code != code:
            return {"status": "ambiguous", "detail": "ledger_code_mismatch"}
    for claim in db.new_joiner_claims.find({"code": code}, {"uid": 1}):
        try:
            recipients.setdefault(int(claim.get("uid")), set()).add("new_joiner_claims")
        except (TypeError, ValueError):
            return {"status": "ambiguous", "detail": "unparseable_recipient"}
    other_pool = db.voucher_pools.find_one({"code": code, "pool_id": {"$ne": WELCOME_POOL_ID}}, {"pool_id": 1})
    if not recipients:
        return {"status": "non_welcome" if other_pool else "unknown"}
    if len(recipients) > 1:
        return {"status": "ambiguous", "detail": "multiple_recipients"}
    if other_pool:
        return {"status": "ambiguous", "detail": "code_in_multiple_pools"}
    uid, sources = next(iter(recipients.items()))
    return {"status": "ok", "user_id": uid, "sources": sorted(sources)}


# ---------------------------------------------------------------------------
# Evidence ingestion (idempotent)
# ---------------------------------------------------------------------------

def _observation_fingerprint(successful: bool, redeemed_at: datetime | None) -> str:
    stamp = redeemed_at.isoformat() if redeemed_at else ""
    return hashlib.sha256(f"{int(bool(successful))}|{stamp}".encode()).hexdigest()[:16]


def record_redemption_evidence(
    db,
    *,
    source: str,
    code,
    account_raw,
    namespace,
    redeemed_at: datetime | None,
    redemption_successful: bool,
    source_batch_id: str,
    source_row_id=None,
    campaign_id=None,
    now_utc: datetime,
) -> dict:
    """Upsert one redemption observation. Re-importing the same row is a
    no-op; a conflicting re-observation (status or time changed) is routed
    to review per the correction policy instead of silently overwriting."""
    code_n = normalize_code(code)
    if not code_n:
        return {"recorded": False, "reason": "missing_code"}
    recipient = resolve_welcome_recipient(db, code_n)
    if recipient["status"] in ("unknown", "non_welcome"):
        # Not a Welcome code: rejected here and only counted, so the
        # evidence collection does not mirror every campaign's coupon rows.
        return {"recorded": False, "reason": f"{recipient['status']}_code"}
    account_key, account_reason = canonical_account_key(account_raw, namespace)
    chash = code_hash(code_n)
    source_ref = hashlib.sha256(f"{source}|{chash}|{account_key or account_reason}".encode()).hexdigest()[:40]
    fingerprint = _observation_fingerprint(redemption_successful, redeemed_at)
    insert_doc = {
        "source": source,
        "source_ref": source_ref,
        "rule_version": RULE_VERSION,
        "code_hash": chash,
        "code_masked": mask_value(code_n),
        "account_key": account_key,
        "account_key_masked": mask_value(account_key) if account_key else None,
        "account_reason": account_reason,
        "redeemed_at": redeemed_at,
        "redemption_successful": bool(redemption_successful),
        "observation_fingerprint": fingerprint,
        "campaign_id": None if campaign_id is None else str(campaign_id),
        "recipient_resolution": recipient,
        "status": EV_RECEIVED,
        "attempts": 0,
        "created_at": now_utc,
    }
    update = {
        "$setOnInsert": insert_doc,
        "$addToSet": {"source_batch_ids": source_batch_id},
        "$set": {"last_seen_at": now_utc},
    }
    if source_row_id is not None:
        update["$addToSet"]["source_row_ids"] = source_row_id
    try:
        before = db[EVIDENCE_COLLECTION].find_one_and_update(
            {"source": source, "source_ref": source_ref},
            update,
            upsert=True,
            return_document=ReturnDocument.BEFORE,
        )
    except DuplicateKeyError:
        # Concurrent upsert of the same observation: the other writer won.
        before = db[EVIDENCE_COLLECTION].find_one({"source": source, "source_ref": source_ref})
        db[EVIDENCE_COLLECTION].update_one({"_id": before["_id"]}, {k: v for k, v in update.items() if k != "$setOnInsert"})
    if before is None:
        return {"recorded": True, "new": True}
    if before.get("observation_fingerprint") != fingerprint:
        _flag_conflicting_observation(db, before, now_utc=now_utc)
        return {"recorded": True, "new": False, "conflict": True}
    return {"recorded": True, "new": False}


def _flag_conflicting_observation(db, evidence: dict, *, now_utc: datetime) -> None:
    """Correction policy: a changed observation never rewrites evidence.
    Unprocessed evidence goes to review; a qualification it already produced
    is flagged for manual reconciliation (never auto-revoked)."""
    ev_id = evidence["_id"]
    if evidence.get("status") == EV_RECEIVED:
        db[EVIDENCE_COLLECTION].update_one(
            {"_id": ev_id, "status": EV_RECEIVED},
            {"$set": {"status": EV_REVIEW, "reason": "conflicting_source_observations", "updated_at": now_utc}},
        )
        _release_reservation_for(db, ev_id)
    else:
        db[EVIDENCE_COLLECTION].update_one(
            {"_id": ev_id},
            {"$set": {"source_conflict_at": now_utc, "updated_at": now_utc}},
        )
        if evidence.get("status") == EV_QUALIFIED:
            db.qualified_events.update_one(
                {"evidence_id": ev_id},
                {"$set": {"evidence_status": "disputed_pending_reconciliation", "evidence_disputed_at": now_utc}},
            )
    logger.warning(
        "[AFF_QUAL][EVIDENCE_CONFLICT] evidence_id=%s status=%s code=%s",
        ev_id, evidence.get("status"), evidence.get("code_masked"),
    )


def extract_marketing_batch(
    db, *, upload_batch_id: str, config: dict, now_utc: datetime, after_id=None, max_rows: int | None = None
) -> dict:
    """Turn (part of) one committed marketing upload into evidence, in
    ``_id`` order from ``after_id``. Idempotent, so a resumed or repeated
    pass over the same rows changes nothing."""
    cfg = config or {}
    code_col = _norm_column(cfg["code_column"])
    account_col = _norm_column(cfg["account_column"])
    time_col = _norm_column(cfg["redeemed_at_column"])
    status_col = _norm_column(cfg["status_column"])
    ns_col = _norm_column(cfg["namespace_column"]) if cfg.get("namespace_column") else None
    success_values = {str(v).strip().lower() for v in cfg.get("success_values") or []}
    campaign_ids = {str(c).strip() for c in cfg.get("campaign_ids") or []}
    summary = {"rows": 0, "recorded": 0, "new": 0, "conflicts": 0, "missing_code": 0,
               "unknown_code": 0, "non_welcome_code": 0, "skipped_campaign": 0}
    query = {"upload_batch_id": upload_batch_id}
    if after_id is not None:
        query["_id"] = {"$gt": after_id}
    cursor = db.marketing_raw_data.find(query).sort("_id", 1)
    if max_rows:
        cursor = cursor.limit(int(max_rows))
    last_id = after_id
    for row in cursor:
        last_id = row.get("_id")
        summary["rows"] += 1
        values = {_norm_column(k): v for k, v in row.items()}
        campaign_id = values.get("campaign_id")
        if campaign_ids and str(campaign_id or "").strip() not in campaign_ids:
            summary["skipped_campaign"] += 1
            continue
        status_raw = str(values.get(status_col) or "").strip().lower()
        out = record_redemption_evidence(
            db,
            source=SOURCE_MARKETING,
            code=values.get(code_col),
            account_raw=values.get(account_col),
            namespace=values.get(ns_col) if ns_col else cfg.get("account_namespace"),
            redeemed_at=parse_redeemed_at(values.get(time_col), cfg["source_timezone"]),
            redemption_successful=bool(status_raw) and status_raw in success_values,
            source_batch_id=upload_batch_id,
            source_row_id=row.get("_id"),
            campaign_id=campaign_id,
            now_utc=now_utc,
        )
        if not out.get("recorded"):
            summary[out["reason"]] += 1
            continue
        summary["recorded"] += 1
        summary["new"] += int(bool(out.get("new")))
        summary["conflicts"] += int(bool(out.get("conflict")))
    summary["last_id"] = last_id
    summary["done"] = not max_rows or summary["rows"] < int(max_rows)
    return summary


def extract_committed_batches(db, *, config: dict, now_utc: datetime, max_rows: int = 5000) -> dict:
    """Extract evidence from committed uploads, at most ``max_rows`` rows per
    call so one large upload never stalls the scheduler tick; progress is a
    persisted ``_id`` cursor on the batch doc. A batch doc is only written by
    marketing_upload after its rows are inserted, so only committed data is
    read; the lease makes a crashed pass retryable."""
    out = {"batches_completed": 0, "rows": 0, "new": 0, "conflicts": 0}
    budget = int(max_rows)
    while budget > 0:
        batch = db.marketing_upload_batches.find_one_and_update(
            {
                "status": {"$in": ["completed", "completed_with_errors"]},
                "rolled_back_at": {"$exists": False},
                "welcome_evidence_extracted_at": {"$exists": False},
                "$or": [
                    {"welcome_evidence_lease_until": {"$exists": False}},
                    {"welcome_evidence_lease_until": {"$lt": now_utc}},
                ],
            },
            {"$set": {"welcome_evidence_lease_until": now_utc + BATCH_EXTRACT_LEASE}},
            sort=[("uploaded_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not batch:
            break
        summary = extract_marketing_batch(
            db, upload_batch_id=batch["upload_batch_id"], config=config, now_utc=now_utc,
            after_id=batch.get("welcome_evidence_cursor"), max_rows=budget,
        )
        budget -= summary["rows"]
        counters = {f"welcome_evidence_summary.{k}": v for k, v in summary.items() if k not in ("last_id", "done")}
        done_fields = {"welcome_evidence_extracted_at": now_utc} if summary["done"] else {}
        db.marketing_upload_batches.update_one(
            {"_id": batch["_id"]},
            {
                "$set": {"welcome_evidence_cursor": summary["last_id"], **done_fields},
                "$inc": counters,
                "$unset": {"welcome_evidence_lease_until": ""},
            },
        )
        out["batches_completed"] += int(summary["done"])
        out["rows"] += summary["rows"]
        out["new"] += summary["new"]
        out["conflicts"] += summary["conflicts"]
        logger.info(
            "[AFF_QUAL][BATCH_EXTRACT] upload_batch_id=%s done=%s rows=%s new=%s",
            batch["upload_batch_id"], summary["done"], summary["rows"], summary["new"],
        )
        if not summary["done"]:
            break
    return out


# ---------------------------------------------------------------------------
# Attribution and ownership
# ---------------------------------------------------------------------------

def _preexisting_community_user(pending: dict, user_doc: dict | None) -> bool:
    """Same rule scheduler.settle_pending_referrals applies to community-group
    referrals ("already_in_db"): the invitee existed well before the join."""
    if (pending.get("destination_type") or "community_group") == "official_channel":
        return False
    join_seen = _aware_utc(pending.get("created_at_utc"))
    reference = _aware_utc((user_doc or {}).get("joined_main_at")) or _aware_utc((user_doc or {}).get("created_at"))
    if join_seen is None or reference is None:
        return False
    return reference < join_seen - timedelta(minutes=10)


def resolve_attribution(db, *, invitee_id: int, redeemed_at: datetime) -> dict:
    """The original referral record for this invitee, frozen.

    Earliest ``pending_referrals`` row that is a valid attribution (any
    status except a revocation that invalidates the record itself). Its
    inviter is used verbatim; nothing reads a mutable "current inviter".
    """
    rows = list(
        db.pending_referrals.find(
            {"invitee_user_id": int(invitee_id)},
            {"inviter_user_id": 1, "status": 1, "revoked_reason": 1, "created_at_utc": 1, "destination_type": 1},
        ).sort("created_at_utc", 1)
    )
    saw_self = False
    for row in rows:
        inviter = row.get("inviter_user_id")
        if inviter is None:
            continue
        try:
            inviter = int(inviter)
        except (TypeError, ValueError):
            continue
        if inviter == int(invitee_id):
            saw_self = True
            continue
        if row.get("status") == "revoked" and row.get("revoked_reason") in ATTRIBUTION_INVALID_REVOKE_REASONS:
            continue
        created = _aware_utc(row.get("created_at_utc"))
        if created is None:
            continue
        if created > _aware_utc(redeemed_at):
            return {"ok": False, "status": EV_REJECTED, "reason": "referral_after_redemption"}
        user_doc = db.users.find_one({"user_id": int(invitee_id)}, {"joined_main_at": 1, "created_at": 1})
        if _preexisting_community_user(row, user_doc):
            return {"ok": False, "status": EV_REJECTED, "reason": "invitee_preexisting_user"}
        return {"ok": True, "inviter_id": inviter, "pending_id": row.get("_id"), "referral_created_at": created}
    if saw_self:
        return {"ok": False, "status": EV_REJECTED, "reason": "self_referral"}
    return {"ok": False, "status": EV_REJECTED, "reason": "no_referral_attribution"}


def _linked_accounts(user_doc: dict | None) -> set[str]:
    out = set()
    for raw in (user_doc or {}).get("linked_gaming_accounts") or []:
        if isinstance(raw, str) and raw.strip():
            out.add(unicodedata.normalize("NFKC", raw).strip())
    return out


def check_account_ownership(db, *, invitee_id: int, inviter_id: int | None, account_key: str, policy: str) -> dict:
    """Validate the redeemed account against existing verified linkage
    (``users.linked_gaming_accounts``, synced from UIM). Conflicts never
    qualify; they stay in review."""
    account_id = account_id_from_key(account_key)
    if inviter_id is not None:
        inviter_doc = db.users.find_one({"user_id": int(inviter_id)}, {"linked_gaming_accounts": 1})
        if account_id in _linked_accounts(inviter_doc):
            return {"ok": False, "status": EV_REJECTED, "reason": "self_referral_account"}
    invitee_doc = db.users.find_one({"user_id": int(invitee_id)}, {"linked_gaming_accounts": 1})
    invitee_links = _linked_accounts(invitee_doc)
    if invitee_links:
        if account_id in invitee_links:
            return {"ok": True, "basis": "verified_linkage"}
        return {"ok": False, "status": EV_REVIEW, "reason": "account_not_linked_to_invitee"}
    other = db.users.find_one(
        {"linked_gaming_accounts": account_id, "user_id": {"$ne": int(invitee_id)}}, {"_id": 1}
    )
    if other:
        return {"ok": False, "status": EV_REVIEW, "reason": "account_linked_to_other_users"}
    if policy == UNLINKED_POLICY_REVIEW:
        return {"ok": False, "status": EV_REVIEW, "reason": "unverified_account_ownership"}
    return {"ok": True, "basis": "code_recipient_only"}


# ---------------------------------------------------------------------------
# Qualification
# ---------------------------------------------------------------------------

def _release_reservation_for(db, evidence_id) -> None:
    db[REGISTRY_COLLECTION].delete_one({"evidence_id": evidence_id, "state": REG_RESERVED})


def _finalize(db, evidence: dict, status: str, reason: str | None, *, now_utc: datetime, extra: dict | None = None) -> dict:
    if status != EV_QUALIFIED:
        _release_reservation_for(db, evidence["_id"])
    fields = {"status": status, "reason": reason, "processed_at": now_utc, "updated_at": now_utc}
    fields.update(extra or {})
    db[EVIDENCE_COLLECTION].update_one(
        {"_id": evidence["_id"]},
        {"$set": fields, "$unset": {"lease_until": ""}},
    )
    logger.info(
        "[AFF_QUAL][EVIDENCE_DONE] evidence_id=%s status=%s reason=%s code=%s",
        evidence["_id"], status, reason, evidence.get("code_masked"),
    )
    return {"status": status, "reason": reason}


def _record_legacy_credit(db, evidence: dict, existing_q: dict, *, now_utc: datetime) -> None:
    """A verified redemption by an invitee whose qualification predates the
    rule ties that account to an already-credited qualification."""
    try:
        db[REGISTRY_COLLECTION].insert_one(
            {
                "account_key": evidence["account_key"],
                "state": REG_CREDITED_LEGACY,
                "invitee_id": existing_q.get("invitee_id"),
                "inviter_id": existing_q.get("referrer_id"),
                "evidence_id": evidence["_id"],
                "qualified_event_id": existing_q.get("_id"),
                "rule_version": RULE_VERSION,
                "recorded_at": now_utc,
            }
        )
    except DuplicateKeyError:
        pass


def evaluate_evidence(db, evidence: dict, *, control: dict, now_utc: datetime) -> dict:
    """Validate one evidence row and, if eligible, qualify atomically."""
    if evidence.get("voided_at"):
        return _finalize(db, evidence, EV_VOIDED, evidence.get("void_reason") or "voided", now_utc=now_utc)
    if not evidence.get("redemption_successful"):
        return _finalize(db, evidence, EV_REJECTED, "redemption_not_successful", now_utc=now_utc)
    redeemed_at = _aware_utc(evidence.get("redeemed_at"))
    if redeemed_at is None:
        return _finalize(db, evidence, EV_REVIEW, "missing_redeemed_at", now_utc=now_utc)
    account_key = evidence.get("account_key")
    if not account_key:
        reason = evidence.get("account_reason") or "missing_account"
        status = EV_REJECTED if reason == "missing_account" else EV_REVIEW
        return _finalize(db, evidence, status, reason, now_utc=now_utc)
    cfg_campaigns = {str(c) for c in ((control.get("source_config") or {}).get("campaign_ids") or [])}
    if cfg_campaigns and str(evidence.get("campaign_id") or "") not in cfg_campaigns:
        return _finalize(db, evidence, EV_REJECTED, "non_welcome_campaign", now_utc=now_utc)

    recipient = evidence.get("recipient_resolution") or {}
    rstatus = recipient.get("status")
    if rstatus == "unknown":
        return _finalize(db, evidence, EV_REJECTED, "unknown_code", now_utc=now_utc)
    if rstatus == "non_welcome":
        return _finalize(db, evidence, EV_REJECTED, "non_welcome_code", now_utc=now_utc)
    if rstatus == "not_issued":
        return _finalize(db, evidence, EV_REVIEW, "welcome_code_not_issued", now_utc=now_utc)
    if rstatus != "ok":
        return _finalize(db, evidence, EV_REVIEW, "ambiguous_recipient", now_utc=now_utc)
    invitee_id = int(recipient["user_id"])

    other = db[EVIDENCE_COLLECTION].find_one(
        {
            "code_hash": evidence["code_hash"],
            "_id": {"$ne": evidence["_id"]},
            "account_key": {"$ne": account_key},
            "redemption_successful": True,
            "status": {"$ne": EV_VOIDED},
        },
        {"_id": 1},
    )
    if other:
        return _finalize(db, evidence, EV_REVIEW, "conflicting_redemption_records", now_utc=now_utc)

    policy = control.get("unlinked_account_policy") or UNLINKED_POLICY_ACCEPT
    existing_q = db.qualified_events.find_one({"invitee_id": invitee_id})
    if existing_q and existing_q.get("evidence_id") != evidence["_id"]:
        own = check_account_ownership(
            db, invitee_id=invitee_id, inviter_id=existing_q.get("referrer_id"), account_key=account_key, policy=policy
        )
        if own["ok"] and existing_q.get("rule_version") != RULE_VERSION:
            _record_legacy_credit(db, evidence, existing_q, now_utc=now_utc)
        return _finalize(
            db, evidence, EV_INVITEE_ALREADY_QUALIFIED, "invitee_already_qualified",
            now_utc=now_utc, extra={"invitee_id": invitee_id},
        )

    attribution = resolve_attribution(db, invitee_id=invitee_id, redeemed_at=redeemed_at)
    if not attribution["ok"]:
        return _finalize(db, evidence, attribution["status"], attribution["reason"], now_utc=now_utc,
                         extra={"invitee_id": invitee_id})
    inviter_id = attribution["inviter_id"]
    own = check_account_ownership(db, invitee_id=invitee_id, inviter_id=inviter_id, account_key=account_key, policy=policy)
    if not own["ok"]:
        return _finalize(db, evidence, own["status"], own["reason"], now_utc=now_utc,
                         extra={"invitee_id": invitee_id, "inviter_id": inviter_id})

    cutoff = launch_cutoff(control)
    if cutoff is not None and redeemed_at < cutoff:
        qualified_at, basis = cutoff, "launch_cutoff_clamp"
    else:
        qualified_at, basis = redeemed_at, "redeemed_at"
    return _qualify(
        db,
        evidence,
        invitee_id=invitee_id,
        inviter_id=inviter_id,
        pending_id=attribution.get("pending_id"),
        redeemed_at=redeemed_at,
        qualified_at=qualified_at,
        attribution_basis=basis,
        ownership_basis=own["basis"],
        now_utc=now_utc,
    )


def _qualify(
    db,
    evidence: dict,
    *,
    invitee_id: int,
    inviter_id: int,
    pending_id,
    redeemed_at: datetime,
    qualified_at: datetime,
    attribution_basis: str,
    ownership_basis: str,
    now_utc: datetime,
) -> dict:
    ev_id = evidence["_id"]
    account_key = evidence["account_key"]
    registry = db[REGISTRY_COLLECTION]
    try:
        registry.insert_one(
            {
                "account_key": account_key,
                "state": REG_RESERVED,
                "invitee_id": invitee_id,
                "inviter_id": inviter_id,
                "evidence_id": ev_id,
                "rule_version": RULE_VERSION,
                "reserved_at": now_utc,
            }
        )
    except DuplicateKeyError:
        holder = registry.find_one({"account_key": account_key}) or {}
        if holder.get("evidence_id") != ev_id:
            return _finalize(
                db, evidence, EV_DUPLICATE_ACCOUNT, "account_already_credited", now_utc=now_utc,
                extra={"invitee_id": invitee_id, "inviter_id": inviter_id, "account_holder_state": holder.get("state")},
            )
        # Our own reservation from an interrupted run: resume.

    last_seen = db.user_last_seen.find_one({"user_id": invitee_id}) or {}
    q_doc = {
        "invitee_id": invitee_id,
        "referrer_id": inviter_id,
        # Attribution timestamp every consumer windows on (dashboard,
        # leaderboards, monthly/weekly tiers): the actual redemption time, or
        # the launch cutoff for a redemption that predates launch.
        "qualified_at": qualified_at,
        "redeemed_at": redeemed_at,
        "processed_at": now_utc,
        "rule_version": RULE_VERSION,
        "attribution_basis": attribution_basis,
        "ownership_basis": ownership_basis,
        "account_key": account_key,
        "evidence_id": ev_id,
        "redemption_source": evidence.get("source"),
        "redemption_ref": evidence.get("source_ref"),
        "referral_pending_id": pending_id,
        "ip": last_seen.get("ip"),
        "subnet": last_seen.get("subnet"),
        "session": last_seen.get("session"),
    }
    try:
        db.qualified_events.insert_one(q_doc)
        q_id = q_doc["_id"]
    except DuplicateKeyError:
        existing = db.qualified_events.find_one({"invitee_id": invitee_id}) or {}
        if existing.get("evidence_id") == ev_id:
            q_id = existing["_id"]
        elif existing:
            _release_reservation_for(db, ev_id)
            if existing.get("rule_version") != RULE_VERSION:
                _record_legacy_credit(db, evidence, existing, now_utc=now_utc)
            return _finalize(db, evidence, EV_INVITEE_ALREADY_QUALIFIED, "invitee_already_qualified",
                             now_utc=now_utc, extra={"invitee_id": invitee_id})
        else:
            # account_key collided in qualified_events without a registry
            # holder: registry and events disagree. Never qualify on that.
            return _finalize(db, evidence, EV_REVIEW, "registry_event_inconsistency", now_utc=now_utc,
                             extra={"invitee_id": invitee_id, "inviter_id": inviter_id})

    registry.update_one(
        {"account_key": account_key, "evidence_id": ev_id, "state": REG_RESERVED},
        {"$set": {"state": REG_COMMITTED, "committed_at": now_utc, "qualified_event_id": q_id}},
    )
    try:
        from affiliate_leaderboard import emit_referral_flow_event

        emit_referral_flow_event(
            db,
            event="affiliate_qualified",
            referrer_id=inviter_id,
            invitee_id=invitee_id,
            ts_utc=qualified_at,
            meta={"rule_version": RULE_VERSION},
            idempotency_key=f"rf|affiliate_qualified|{inviter_id}|{invitee_id}|{RULE_VERSION}",
        )
    except Exception:
        logger.exception("[AFF_QUAL][FLOW_EVENT_FAILED] invitee=%s referrer=%s", invitee_id, inviter_id)
    return _finalize(
        db, evidence, EV_QUALIFIED, None, now_utc=now_utc,
        extra={"invitee_id": invitee_id, "inviter_id": inviter_id, "qualified_event_id": q_id},
    )


def process_pending_evidence(db, *, now_utc: datetime, control: dict | None = None, batch_limit: int = 200) -> dict:
    """Evaluate received evidence. No-op unless the rule is active and every
    required unique index exists (a check-then-insert alone is not safe)."""
    control = control if control is not None else get_control(db)
    out = {"processed": 0, "by_status": {}}
    if not new_rule_active(control, now_utc):
        out["skipped"] = "rule_not_active"
        return out
    readiness = index_readiness(db)
    if not readiness["ok"]:
        logger.error("[AFF_QUAL][REFUSED] reason=indexes_not_ready missing=%s", readiness["missing"])
        out["skipped"] = "indexes_not_ready"
        return out
    col = db[EVIDENCE_COLLECTION]
    for _ in range(int(batch_limit)):
        evidence = col.find_one_and_update(
            {
                "status": EV_RECEIVED,
                "$or": [{"lease_until": {"$exists": False}}, {"lease_until": {"$lt": now_utc}}],
            },
            {"$set": {"lease_until": now_utc + EVIDENCE_LEASE}, "$inc": {"attempts": 1}},
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not evidence:
            break
        if int(evidence.get("attempts") or 0) > MAX_EVIDENCE_ATTEMPTS:
            result = _finalize(db, evidence, EV_REVIEW, "processing_error_retries_exhausted", now_utc=now_utc)
            out["processed"] += 1
            out["by_status"][result["status"]] = out["by_status"].get(result["status"], 0) + 1
            continue
        try:
            result = evaluate_evidence(db, evidence, control=control, now_utc=now_utc)
        except Exception:
            # Left "received" with its lease: retried after expiry, resuming
            # any reservation it already owns.
            logger.exception("[AFF_QUAL][EVIDENCE_ERROR] evidence_id=%s", evidence["_id"])
            continue
        out["processed"] += 1
        out["by_status"][result["status"]] = out["by_status"].get(result["status"], 0) + 1
    return out


def reconcile(db, *, now_utc: datetime, stale_after: timedelta = STALE_RESERVATION_AFTER, limit: int = 500) -> dict:
    """Crash recovery for the reservation protocol. Idempotent.

    * reservation + matching qualified_events row -> commit it;
    * reservation whose evidence can no longer qualify -> release it;
    * reservation whose evidence is still ``received`` -> leave it: the
      processing loop resumes it after the lease expires.
    """
    out = {"committed": 0, "released": 0, "left_for_resume": 0}
    stale = list(
        db[REGISTRY_COLLECTION].find(
            {"state": REG_RESERVED, "reserved_at": {"$lt": now_utc - stale_after}}
        ).limit(int(limit))
    )
    for reg in stale:
        q = db.qualified_events.find_one({"invitee_id": reg.get("invitee_id")}, {"evidence_id": 1})
        if q and q.get("evidence_id") == reg.get("evidence_id"):
            db[REGISTRY_COLLECTION].update_one(
                {"_id": reg["_id"], "state": REG_RESERVED},
                {"$set": {"state": REG_COMMITTED, "committed_at": now_utc, "qualified_event_id": q["_id"]}},
            )
            db[EVIDENCE_COLLECTION].update_one(
                {"_id": reg.get("evidence_id"), "status": EV_RECEIVED},
                {"$set": {"status": EV_QUALIFIED, "processed_at": now_utc, "updated_at": now_utc,
                          "qualified_event_id": q["_id"]}, "$unset": {"lease_until": ""}},
            )
            out["committed"] += 1
            continue
        evidence = db[EVIDENCE_COLLECTION].find_one({"_id": reg.get("evidence_id")}, {"status": 1})
        if evidence and evidence.get("status") == EV_RECEIVED:
            out["left_for_resume"] += 1
            continue
        db[REGISTRY_COLLECTION].delete_one({"_id": reg["_id"], "state": REG_RESERVED})
        out["released"] += 1
    if any(out.values()):
        logger.warning("[AFF_QUAL][RECONCILE] %s", out)
    return out


# ---------------------------------------------------------------------------
# Legacy settlement hand-off and corrections
# ---------------------------------------------------------------------------

def park_pending_referral(db, *, pending_id, invitee_user_id, inviter_user_id, now_utc: datetime) -> None:
    """After launch, a pending referral that passed the record-validity
    checks waits for verified redemption instead of being awarded. Join
    tracking is untouched; the invitee lock becomes non-blocking exactly as
    a legacy revocation would leave it."""
    db.pending_referrals.update_one(
        {"_id": pending_id},
        {
            "$set": {
                "status": PENDING_AWAITING_REDEMPTION,
                "awaiting_redemption_since": now_utc,
                "qualification_rule_version": RULE_VERSION,
            },
            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
        },
    )
    import referral_invitee_lock

    referral_invitee_lock.release(
        db,
        invitee_user_id=invitee_user_id,
        status=PENDING_AWAITING_REDEMPTION,
        now_utc_ts=now_utc,
        expected_inviter_user_id=inviter_user_id,
    )


def void_upload_batch(db, *, upload_batch_id: str, reason: str, now_utc: datetime) -> dict:
    """Rollback / correction of a source upload (documented policy).

    * The batch is marked ``rolled_back_at`` so it is never (re)extracted.
    * Evidence seen ONLY in this batch is voided: unprocessed or in-review
      evidence can then never qualify (its reservation, if any, is released).
    * Evidence that already produced a qualification is NOT revoked and its
      account reservation is NOT released: the qualification is flagged
      ``evidence_status=voided_pending_reconciliation`` for manual review.
      Issued rewards are never touched.
    """
    db.marketing_upload_batches.update_one(
        {"upload_batch_id": upload_batch_id},
        {"$set": {"rolled_back_at": now_utc, "rollback_reason": reason}},
    )
    out = {"voided": 0, "flagged_qualified": 0, "kept_other_sources": 0}
    for ev in db[EVIDENCE_COLLECTION].find({"source_batch_ids": upload_batch_id}):
        if [b for b in ev.get("source_batch_ids") or [] if b != upload_batch_id]:
            out["kept_other_sources"] += 1
            continue
        if ev.get("status") == EV_QUALIFIED:
            db[EVIDENCE_COLLECTION].update_one(
                {"_id": ev["_id"]}, {"$set": {"voided_at": now_utc, "void_reason": reason, "updated_at": now_utc}}
            )
            db.qualified_events.update_one(
                {"evidence_id": ev["_id"]},
                {"$set": {"evidence_status": "voided_pending_reconciliation", "evidence_voided_at": now_utc}},
            )
            out["flagged_qualified"] += 1
            continue
        db[EVIDENCE_COLLECTION].update_one(
            {"_id": ev["_id"]},
            {"$set": {"status": EV_VOIDED, "voided_at": now_utc, "void_reason": reason, "updated_at": now_utc},
             "$unset": {"lease_until": ""}},
        )
        _release_reservation_for(db, ev["_id"])
        out["voided"] += 1
    logger.warning("[AFF_QUAL][BATCH_VOIDED] upload_batch_id=%s result=%s", upload_batch_id, out)
    return out


def requeue_evidence(db, *, evidence_id, now_utc: datetime) -> bool:
    """Send a reviewed/rejected evidence row back through FULL validation
    (after the underlying data was fixed). Never forces a qualification."""
    res = db[EVIDENCE_COLLECTION].update_one(
        {"_id": evidence_id, "status": {"$in": [EV_REVIEW, EV_REJECTED]}},
        {"$set": {"status": EV_RECEIVED, "requeued_at": now_utc, "updated_at": now_utc},
         "$unset": {"lease_until": "", "reason": ""}},
    )
    return bool(getattr(res, "modified_count", 0))


def consistency_report(db, *, now_utc: datetime) -> dict:
    """Registry <-> qualified_events consistency for v1 (read-only)."""
    report = {
        "v1_events_without_committed_registry": 0,
        "committed_registry_without_event": 0,
        "stale_reservations": 0,
        "v1_events_effects_pending": 0,
        "v1_events_flagged_for_reconciliation": 0,
    }
    for q in db.qualified_events.find({"rule_version": RULE_VERSION}, {"account_key": 1, "effects_applied_at": 1, "evidence_status": 1}):
        reg = db[REGISTRY_COLLECTION].find_one({"account_key": q.get("account_key")}, {"state": 1, "qualified_event_id": 1})
        if not reg or reg.get("state") != REG_COMMITTED:
            report["v1_events_without_committed_registry"] += 1
        if not q.get("effects_applied_at"):
            report["v1_events_effects_pending"] += 1
        if q.get("evidence_status"):
            report["v1_events_flagged_for_reconciliation"] += 1
    for reg in db[REGISTRY_COLLECTION].find({"state": REG_COMMITTED}, {"qualified_event_id": 1}):
        if not db.qualified_events.find_one({"_id": reg.get("qualified_event_id")}, {"_id": 1}):
            report["committed_registry_without_event"] += 1
    report["stale_reservations"] = db[REGISTRY_COLLECTION].count_documents(
        {"state": REG_RESERVED, "reserved_at": {"$lt": now_utc - STALE_RESERVATION_AFTER}}
    )
    report["ok"] = not any(
        report[k] for k in ("v1_events_without_committed_registry", "committed_registry_without_event", "stale_reservations")
    )
    return report
