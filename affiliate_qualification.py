"""Canonical affiliate qualification: verified Welcome voucher redemption.

Rule ``welcome_redemption_v2`` (see docs/affiliate_forward_only_rollout.md):

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
``affiliate_qualification_control`` (one document, ``_id = welcome_redemption_v1``) is
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
import json
import logging
import os
import re
import unicodedata
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import pytz
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)

LEGACY_RULE_VERSION = "welcome_redemption_v1"
RULE_VERSION = "welcome_redemption_v2"
QUALIFICATION_GATE_VERSION = "coupon_new_player_turnover_v2"
CONTROL_COLLECTION = "affiliate_qualification_control"
EVIDENCE_COLLECTION = "welcome_redemption_evidence"
REGISTRY_COLLECTION = "affiliate_account_registry"
# Keep the deployed switch identity: a second control document could reopen
# legacy awards when an old worker reads v1. Transition this document by CAS.
CONTROL_ID = LEGACY_RULE_VERSION

HISTORY_STRICT = "strict_complete_history"
HISTORY_FORWARD = "forward_only_from_activation_cutoff"
HISTORY_LIMITATION = (
    "Forward-only from activation cutoff: Account ID uniqueness is enforced for the deterministically "
    "seeded historical identities, every pending/manual referral evaluated under V2 and every future V2 "
    "referral. Historical identities that could not be resolved are outside guaranteed duplicate coverage; "
    "this is NOT complete pre-cutoff lifetime Account ID uniqueness. coupon_new_player=1 remains mandatory."
)
FORWARD_MARKER_VERSION = "forward_only_marker_v1"
COHORT_COLLECTION = "affiliate_v2_launch_cohort"
ABSENCE_REJECTION_REASON = "no_welcome_redemption_in_complete_coverage"  # == uim_coverage.ABSENCE_REASON
ACTIVE_PENDING_STATUSES = ("pending", "pending_channel", "pending_manual", "manual",
                           "processing", "error", "awaiting_redemption")

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

MODE_DISABLED = "disabled"
MODE_ACTIVE = "active"
MODE_PAUSED = "paused"

SOURCE_MARKETING = "marketing_raw_data"
# Databot's committed UIM imports, pulled by uim_redemption_sync into
# uim_redemption_batches / uim_redemption_rows (see that module).
SOURCE_DATABOT_UIM = "databot_uim"
WELCOME_POOL_ID = "WELCOME"

# Where each source's committed rows live. Batches carry the same
# extraction cursor/lease fields (welcome_v2_evidence_*) and the same
# ``rolled_back_at`` correction marker whatever the source.
SOURCE_SPECS = {
    SOURCE_MARKETING: {
        "batches": "marketing_upload_batches",
        "rows": "marketing_raw_data",
        "batch_key": "upload_batch_id",
        "committed": {"status": {"$in": ["completed", "completed_with_errors"]}},
        "order": "uploaded_at",
    },
    SOURCE_DATABOT_UIM: {
        "batches": "uim_redemption_batches",
        "rows": "uim_redemption_rows",
        "batch_key": "batch_id",
        # rows_complete: every row of the Databot batch has been synced, so
        # an extraction pass can never mark a half-synced batch done.
        "committed": {"status": "committed", "rows_complete": True},
        "order": "committed_at",
    },
}

# Databot UIM rows carry no status column. A row counts as a successful
# redemption ONLY under this basis, which requires a recorded attestation
# by the data owner that the UIM export lists successful redemptions only.
SUCCESS_BASIS_SOURCE_CONTRACT = "source_contract_successful_only"
# Rows imported into Databot before redeem-time provenance was recorded:
# "review" (default) or "naive_wallclock" (data owner attests the export's
# Coupon Redeem Time is naive wall clock in source_timezone).
LEGACY_TIME_REVIEW = "review"
LEGACY_TIME_NAIVE = "naive_wallclock"

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
FUTURE_REDEMPTION_TOLERANCE = timedelta(minutes=5)
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
    return not (control or {}).get("legacy_admission_closed") and (
        cutoff is None or _aware_utc(now_utc) < cutoff)


def new_rule_active(control: dict | None, now_utc: datetime) -> bool:
    cutoff = launch_cutoff(control)
    return bool(
        control
        and control.get("rule_version") == RULE_VERSION
        and control.get("mode") == MODE_ACTIVE
        and cutoff is not None
        and _aware_utc(now_utc) >= cutoff
    )


# Legacy-writer barrier -----------------------------------------------------
#
# The token is a *bounded lease*, not a forever-lock. A crashed/killed writer
# (deploy, OOM, machine restart) never reaches the ``finally`` that releases
# it; with no expiry the barrier then blocks activation indefinitely. A live
# writer renews the lease on every loop pass and immediately before each award
# (``WriterLease.renew``), and the renewal is a compare-and-set on token AND an
# unexpired lease, so a writer that was suspended past its lease can never
# renew, and therefore never awards, after recovery has cleared its token.
# Recovery additionally waits a grace period beyond expiry, so the old and V2
# writers cannot overlap: activation still requires the token to be absent.
LEGACY_WRITER_LEASE = timedelta(minutes=10)
LEGACY_WRITER_RECOVERY_GRACE = timedelta(minutes=5)
# Tokens written by pre-lease code carry no expiry, so liveness is unprovable;
# they are recoverable only by an operator who confirms the process is stopped,
# and only once they are older than any plausible settlement run.
LEGACY_UNLEASED_MIN_AGE = timedelta(hours=1)

WRITER_NONE = "none"
WRITER_ACTIVE = "active"
WRITER_EXPIRED_GRACE = "expired_within_grace"
WRITER_EXPIRED_RECOVERABLE = "expired_recoverable"
WRITER_UNLEASED_RECENT = "unleased_recent"
WRITER_UNLEASED_STALE = "unleased_stale"
_WRITER_FIELDS = ("legacy_writer_token", "legacy_writer_started_at", "legacy_writer_lease_until",
                  "legacy_writer_heartbeat_at", "legacy_writer_owner")


def legacy_writer_state(control: dict | None, now_utc: datetime) -> dict:
    """Classify the writer barrier: genuinely active, stale lease, or unprovable."""
    control = control or {}
    if not control.get("legacy_writer_token"):
        return {"state": WRITER_NONE, "in_flight": False, "recoverable": False}
    now = _aware_utc(now_utc)
    started = _aware_utc(control.get("legacy_writer_started_at"))
    lease_until = _aware_utc(control.get("legacy_writer_lease_until"))
    out = {"in_flight": True, "owner": control.get("legacy_writer_owner"),
           "started_at": started.isoformat() if started else None,
           "lease_until": lease_until.isoformat() if lease_until else None,
           "heartbeat_at": (lambda h: h.isoformat() if h else None)(_aware_utc(control.get("legacy_writer_heartbeat_at"))),
           "age_seconds": int((now - started).total_seconds()) if started else None}
    if lease_until is None:
        old_enough = started is None or now - started >= LEGACY_UNLEASED_MIN_AGE
        out.update(state=WRITER_UNLEASED_STALE if old_enough else WRITER_UNLEASED_RECENT,
                   recoverable=old_enough, requires_operator_ack=True)
    elif now <= lease_until:
        out.update(state=WRITER_ACTIVE, recoverable=False)
    elif now - lease_until < LEGACY_WRITER_RECOVERY_GRACE:
        out.update(state=WRITER_EXPIRED_GRACE, recoverable=False)
    else:
        out.update(state=WRITER_EXPIRED_RECOVERABLE, recoverable=True, requires_operator_ack=False)
    return out


class WriterLease:
    """Handle on an admitted writer token. Truthy while admitted."""

    def __init__(self, db, token: str, ttl: timedelta = LEGACY_WRITER_LEASE):
        self.db, self.token, self.ttl, self.lost = db, token, ttl, False

    def __bool__(self) -> bool:
        return True

    def renew(self, now_utc: datetime | None = None, also_unset: tuple = ()) -> bool:
        """Extend the lease iff this token is still the owner and unexpired."""
        now = _aware_utc(now_utc) if now_utc else datetime.now(timezone.utc)
        update = {"$set": {"legacy_writer_lease_until": now + self.ttl, "legacy_writer_heartbeat_at": now}}
        if also_unset:
            update["$unset"] = {f: "" for f in also_unset}
        res = self.db[CONTROL_COLLECTION].update_one(
            {"_id": CONTROL_ID, "legacy_writer_token": self.token, "legacy_writer_lease_until": {"$gt": now}},
            update)
        # matched, not modified: a renewal within the same clock tick rewrites identical values.
        if not res.matched_count:
            self.lost = True
            logger.error("[AFF_QUAL][WRITER_LEASE_LOST] a legacy writer lost its lease; it must stop writing")
        return bool(res.matched_count)


def _begin_award(self, now_utc: datetime | None = None) -> bool:
    """Atomically open the in-flight-award barrier on the control document.

    ONE compare-and-set that requires this exact token with an unexpired lease, legacy admission still
    open, no cutoff, and no other award in flight; it renews the lease, invalidates the seed and sets
    ``legacy_award_inflight``. Activation's own CAS on the same document requires that marker to be
    absent, so the two are mutually exclusive on one document: either the marker lands first (activation
    refuses) or activation lands first (this refuses). A worker paused after this point therefore holds
    activation off instead of being overtaken; a dead worker's marker is released only by an operator.
    """
    now = _aware_utc(now_utc) if now_utc else datetime.now(timezone.utc)
    res = self.db[CONTROL_COLLECTION].update_one(
        {"_id": CONTROL_ID, "legacy_writer_token": self.token, "legacy_writer_lease_until": {"$gt": now},
         "legacy_admission_closed": {"$ne": True}, "launch_cutoff_utc": {"$exists": False},
         "legacy_award_inflight": {"$exists": False}},
        {"$set": {"legacy_writer_lease_until": now + self.ttl, "legacy_writer_heartbeat_at": now,
                  "legacy_award_inflight": {"token": self.token, "since": now}},
         "$unset": {"seed_completed_at": ""}})
    if not res.matched_count:
        self.lost = True
        logger.error("[AFF_QUAL][AWARD_FENCE_REFUSED] legacy award not admitted (lease, admission or in-flight barrier)")
    return bool(res.matched_count)


def _end_award(self) -> None:
    self.db[CONTROL_COLLECTION].update_one({"_id": CONTROL_ID, "legacy_award_inflight.token": self.token},
                                           {"$unset": {"legacy_award_inflight": ""}})


WriterLease.begin_award = _begin_award
WriterLease.end_award = _end_award


def _lease_fields(token: str, now_utc: datetime) -> dict:
    now = _aware_utc(now_utc)
    return {"legacy_writer_token": token, "legacy_writer_started_at": now_utc,
            "legacy_writer_lease_until": now + LEGACY_WRITER_LEASE, "legacy_writer_heartbeat_at": now,
            "legacy_writer_owner": f"{os.environ.get('FLY_MACHINE_ID') or os.uname().nodename}:{os.getpid()}"}


def recover_legacy_writer(db, *, now_utc: datetime, operator: str = "auto",
                          acknowledged_stopped: bool = False, token: str | None = None) -> dict:
    """Clear a dead writer's token. Never touches an active or in-grace lease.

    Compare-and-set on the exact token and on the expiry predicate itself, so
    a writer that renewed in the meantime is not released. Pre-lease tokens
    additionally need ``acknowledged_stopped``.
    """
    col = db[CONTROL_COLLECTION]
    control = col.find_one({"_id": CONTROL_ID}) or {}
    info = legacy_writer_state(control, now_utc)
    cur = control.get("legacy_writer_token")
    if not info["in_flight"]:
        return {"recovered": False, "reason": "no_writer", **info}
    if token is not None and token != cur:
        return {"recovered": False, "reason": "token_changed", **info}
    if not info["recoverable"]:
        return {"recovered": False, "reason": "writer_not_recoverable", **info}
    now = _aware_utc(now_utc)
    if info["state"] == WRITER_EXPIRED_RECOVERABLE:
        guard = {"legacy_writer_lease_until": {"$lt": now - LEGACY_WRITER_RECOVERY_GRACE}}
    else:
        if not acknowledged_stopped:
            return {"recovered": False, "reason": "requires_acknowledgement_writer_stopped", **info}
        guard = {"legacy_writer_lease_until": {"$exists": False}}
        if control.get("legacy_writer_started_at") is not None:
            guard["legacy_writer_started_at"] = {"$lte": now - LEGACY_UNLEASED_MIN_AGE}
    res = col.update_one(
        {"_id": CONTROL_ID, "legacy_writer_token": cur, **guard},
        {"$unset": {f: "" for f in _WRITER_FIELDS},
         "$push": {"legacy_writer_recoveries": {"token": cur, "state": info["state"], "operator": operator,
                                                "at": now_utc, "started_at": control.get("legacy_writer_started_at"),
                                                "lease_until": control.get("legacy_writer_lease_until")}}})
    if res.modified_count:
        logger.warning("[AFF_QUAL][WRITER_RECOVERED] state=%s operator=%s", info["state"], operator)
    return {"recovered": bool(res.modified_count), "reason": None if res.modified_count else "token_changed", **info}


def _admit_writer(db, query: dict, *, now_utc: datetime):
    """CAS the writer token; one auto-recovery attempt for an expired lease."""
    col = db[CONTROL_COLLECTION]
    for attempt in (0, 1):
        token = uuid.uuid4().hex
        locked = col.find_one_and_update(
            {**query, "legacy_writer_token": {"$exists": False}}, {"$set": _lease_fields(token, now_utc)},
            return_document=ReturnDocument.AFTER)
        if locked is not None:
            return token
        if attempt == 0 and not recover_legacy_writer(db, now_utc=now_utc).get("recovered"):
            return None
    return None


def _release_writer(db, token: str) -> None:
    db[CONTROL_COLLECTION].update_one({"_id": CONTROL_ID, "legacy_award_inflight.token": token},
                                      {"$unset": {"legacy_award_inflight": ""}})
    db[CONTROL_COLLECTION].update_one({"_id": CONTROL_ID, "legacy_writer_token": token},
                                      {"$unset": {f: "" for f in _WRITER_FIELDS}})


@contextmanager
def legacy_writer_guard(db, *, now_utc: datetime):
    """Serialize legacy settlement/backfill with the atomic activation CAS.

    Yields a :class:`WriterLease` (truthy) when admitted, else ``False``. The
    lease is bounded (see above): a suspended worker cannot wake and award
    after expiry, and a crashed one self-heals after expiry + grace, while a
    genuinely active writer is never displaced. The deployed switch is also
    initialized here for installations without a control document.
    """
    col = db[CONTROL_COLLECTION]
    col.update_one({"_id": CONTROL_ID}, {"$setOnInsert": {"mode": MODE_DISABLED}}, upsert=True)
    token = _admit_writer(
        db, {"_id": CONTROL_ID, "legacy_admission_closed": {"$ne": True},
             "$and": [{"$or": [{"launch_cutoff_utc": {"$exists": False}}, {"launch_cutoff_utc": {"$gt": now_utc}}]},
                      # mutual exclusion with a running migration (its lease is held for the whole
                      # plan/seed/complete interval, and an expired one no longer blocks)
                      {"$or": [{"migration_lease_until": {"$exists": False}},
                               {"migration_lease_until": {"$lt": now_utc}}]}]},
        now_utc=now_utc)
    try:
        yield WriterLease(db, token) if token else False
    finally:
        if token:
            _release_writer(db, token)


@contextmanager
def cohort_writer_guard(db, *, cutoff: datetime, now_utc: datetime):
    """Use the same bounded control barrier for marking/cancellation.

    Matching the current cutoff rejects a stale scheduler snapshot. Sharing
    the legacy token also makes existing stopped-writer recovery applicable.
    """
    token = _admit_writer(db, {"_id": CONTROL_ID, "launch_cutoff_utc": cutoff}, now_utc=now_utc)
    try:
        yield token
    finally:
        if token:
            _release_writer(db, token)


def mark_activation_cohort(db, *, control: dict, now_utc: datetime) -> int:
    cutoff = launch_cutoff(control)
    if cutoff is None:
        return 0
    with cohort_writer_guard(db, cutoff=cutoff, now_utc=now_utc) as token:
        if token is None:
            return 0
        marked = _mark_activation_cohort(db, control=control, now_utc=now_utc)
        # Top up the launch-cohort snapshot under the same barrier. Replayable:
        # the scheduler repeats this hand-off until it completes after a crash.
        import affiliate_forward_only

        affiliate_forward_only.snapshot_launch_cohort(db, now_utc=now_utc, cutoff=cutoff)
        return marked


def restore_activation_cohort(db, *, cutoff: datetime) -> None:
    """Caller holds the control writer barrier, including after cancellation."""
    for prior in ACTIVE_PENDING_STATUSES:
        db.pending_referrals.update_many(
            {"activation_cutoff_utc": cutoff, "activation_prior_status": prior,
             "status": PENDING_AWAITING_REDEMPTION},
            # Marking cleared processing ownership/timestamps. No admitted
            # legacy writer remains, so resume an orphaned claim as pending.
            {"$set": {"status": "pending" if prior == "processing" else prior}, "$unset": {"activation_cutoff_utc": "",
                "activation_prior_status": "", "qualification_rule_version": "", "awaiting_redemption_since": ""}})


def repair_cancelled_cohort(db) -> None:
    """Under legacy_writer_guard, repair cancellation interrupted after CAS."""
    control = get_control(db) or {}
    if launch_cutoff(control) is not None or not control.get("cancelled_cohort_cutoff"):
        return
    cutoff = control["cancelled_cohort_cutoff"]
    restore_activation_cohort(db, cutoff=cutoff)
    db[CONTROL_COLLECTION].update_one(
        {"_id": CONTROL_ID, "cancelled_cohort_cutoff": cutoff},
        {"$unset": {"cancelled_cohort_cutoff": ""}})


def _mark_activation_cohort(db, *, control: dict, now_utc: datetime) -> int:
    """Repeatable hand-off after the switch CAS; never rewrite awarded history.

    The switch itself closes legacy admission atomically. This marking may be
    interrupted and repeated; validation reads active rows directly, so an
    unmarked row is still included after a crash, including pre-cutoff joins.
    """
    if launch_cutoff(control) is None:
        return 0
    marked = 0
    for status in ACTIVE_PENDING_STATUSES:
        result = db.pending_referrals.update_many(
            {"status": status, "qualification_rule_version": {"$ne": RULE_VERSION}},
            {"$set": {"status": PENDING_AWAITING_REDEMPTION, "activation_prior_status": status,
                      "qualification_rule_version": RULE_VERSION,
                      "activation_cutoff_utc": launch_cutoff(control), "awaiting_redemption_since": now_utc},
             "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""}},
        )
        marked += result.modified_count
    return marked


def invalidate_history_seed(db, lease: "WriterLease | None" = None, now_utc: datetime | None = None) -> bool:
    """A legacy writer invalidates the seed BEFORE it can change history.

    Activation's CAS includes seed_completed_at, so a worker that finishes
    between preflight and the CAS cannot leave a stale coverage marker usable.
    This call runs inside legacy_writer_guard, never after activation. Given
    the writer's lease it is also the fence immediately before an award: it
    returns False (and invalidates nothing) when the lease is lost.
    """
    if lease is not None:
        # ONE compare-and-set on this lease's exact token and an unexpired lease: renews it and
        # invalidates the seed together, and its match result (not a prior renewal) is the verdict.
        return lease.begin_award(now_utc)
    db[CONTROL_COLLECTION].update_one({"_id": CONTROL_ID, "legacy_writer_token": {"$exists": True}},
                                     {"$unset": {"seed_completed_at": ""}})
    return True


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


IDENTITY_CONFIG_KEYS = ("account_column", "account_namespace", "namespace_column", "code_column")


def identity_config(config: dict | None) -> dict:
    """The part of the source config that defines canonical account keys.
    Changing it after the registry is seeded would mint new keys for
    already-credited accounts, so it is frozen from seeding on."""
    cfg = config or {}
    return {k: cfg.get(k) or None for k in IDENTITY_CONFIG_KEYS}


def source_name(config: dict | None) -> str:
    return (config or {}).get("source") or SOURCE_MARKETING


def source_spec(config: dict | None) -> dict:
    return SOURCE_SPECS[source_name(config)]


def committed_batch_filter(config: dict | None) -> dict:
    return {**source_spec(config)["committed"], "rolled_back_at": {"$exists": False}}


# Fixed by the Databot feed contract (databot.uim_redemption_feed.v1): the
# column names are the synced row's field names, recorded in the config so
# identity_config() stays meaningful and frozen like any other source.
DATABOT_FIXED_COLUMNS = {
    "code_column": "coupon_code",
    "account_column": "account",
    "redeemed_at_column": "redeemed_at_utc",
}


def _validate_databot_config(cfg: dict) -> list[str]:
    problems = []
    for key, expected in DATABOT_FIXED_COLUMNS.items():
        if cfg.get(key) != expected:
            problems.append(f"databot_fixed_column:{key}")
    if cfg.get("namespace_column"):
        problems.append("databot_has_no_namespace_column")
    if normalize_namespace(cfg.get("account_namespace")) is None:
        problems.append("missing:account_namespace")
    elif normalize_namespace(cfg.get("account_namespace")) != "uim_global":
        problems.append("databot_requires_global_account_namespace")
    if not cfg.get("source_timezone"):
        problems.append("missing:source_timezone")
    else:
        try:
            pytz.timezone(cfg["source_timezone"])
        except Exception:
            problems.append("invalid_source_timezone")
    if cfg.get("success_basis") != SUCCESS_BASIS_SOURCE_CONTRACT:
        problems.append("missing:success_basis")
    attestation = cfg.get("success_attestation") or {}
    if not (isinstance(attestation, dict) and attestation.get("by") and attestation.get("reference")):
        problems.append("missing:success_attestation")
    if (cfg.get("legacy_time_basis") or LEGACY_TIME_REVIEW) not in (LEGACY_TIME_REVIEW, LEGACY_TIME_NAIVE):
        problems.append("invalid_legacy_time_basis")
    return problems


def validate_source_config(config: dict | None) -> list[str]:
    """Problems that make a redemption source unusable as authoritative
    evidence. Empty list = structurally usable."""
    cfg = config or {}
    if cfg.get("source") == SOURCE_DATABOT_UIM:
        return _validate_databot_config(cfg)
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


def integration_readiness(db, config: dict | None, *, sample_batches: int = 3, now_utc: datetime | None = None) -> dict:
    """Structural config check plus proof that recent committed uploads carry
    every configured column. Read-only."""
    problems = validate_source_config(config)
    cfg = config or {}
    if cfg.get("source") == SOURCE_DATABOT_UIM:
        import uim_redemption_sync

        sync = uim_redemption_sync.sync_health(db, now_utc=now_utc or datetime.now(timezone.utc))
        problems.extend(sync["problems"])
        return {"ok": not problems, "problems": sorted(set(problems)), "sync": sync}
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


def pending_maturity_report(db, *, config: dict | None, now_utc: datetime) -> dict:
    """Source coverage is an availability bound, never a failed qualification.

    There is exactly ONE seven-day measurement and it is completed BEFORE the
    UIM file reaches Databot: a committed final result is evaluated as soon as
    it arrives. No join age and no upload age is ever added to it; the counts
    below are diagnostics only. Missing, partial or uncovered results stay
    pending (a missing row is only a rejection inside proven complete coverage,
    see ``uim_coverage``).
    """
    health = integration_readiness(db, config, now_utc=now_utc)
    sync = health.get("sync") or {}
    final = sync.get("final_coverage") or {}
    final_end_day = final.get("final_coverage_end")
    final_end = (KL_TZ.localize(datetime.strptime(final_end_day, "%Y-%m-%d") + timedelta(days=1))
                 .astimezone(timezone.utc) if final_end_day else None)          # exclusive end of the last covered day
    observed = parse_redeemed_at(sync.get("observed_end"), "Asia/Kuala_Lumpur")
    query = {"status": {"$in": list(ACTIVE_PENDING_STATUSES)}}
    return {
        "active_pending_manual_rows": db.pending_referrals.count_documents(query),
        "unverified_completeness_synced_rows": db.uim_redemption_rows.count_documents(
            {"seven_day_complete": {"$exists": False}, "seven_day_completeness_retained_version": {"$ne": 1}})
            if source_name(config) == SOURCE_DATABOT_UIM else None,
        # Three different facts, never one "coverage end":
        "final_coverage_status": final.get("status"),
        "final_coverage_start_gmt8": final.get("final_coverage_start"),
        "final_coverage_end_gmt8": final_end_day,
        "latest_observed_redemption_utc": observed.isoformat() if observed else None,
        "latest_observed_redemption_gmt8": observed.astimezone(KL_TZ).isoformat() if observed else None,
        "last_sync_success_at": sync.get("last_success_at"),
        "joined_after_final_coverage": db.pending_referrals.count_documents(
            {**query, "created_at_utc": {"$gte": final_end}}) if final_end else None,
        "joins_less_than_seven_days_old": db.pending_referrals.count_documents(
            {**query, "created_at_utc": {"$gt": now_utc - timedelta(days=7)}}),
        "policy": ("a completed seven-day report needs no extra timer; missing, incomplete or uncovered results "
                   "stay pending; join age is diagnostic only"),
    }


# ---------------------------------------------------------------------------
# Code -> original recipient
# ---------------------------------------------------------------------------

def decide_recipient(*, code: str, pool_row: dict | None, ledger_for, claims, other_pool_exists: bool) -> dict:
    """Map a redeemed code to its ORIGINAL Welcome recipient (pure decision,
    shared by live per-code lookups and the bulk shadow preview).

    Sources: the canonical WELCOME pool (``voucher_pools`` issued_to_user_id,
    cross-checked against the user's ``affiliate_ledger`` WELCOME row via
    ``ledger_for(uid)``) and the legacy ``new_joiner_claims`` rows.
    Any disagreement is ``ambiguous``.
    """
    recipients: dict[int, set] = {}
    issuance_times: list[tuple[datetime, str]] = []

    def issued(value, source):
        when = _aware_utc(value)
        if when is not None:
            issuance_times.append((when, source))

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
        issued(pool_row.get("issued_at"), "voucher_pools.issued_at")
        ledger = ledger_for(uid) or {}
        ledger_code = ledger.get("voucher_code")
        if ledger_code and ledger_code != code:
            return {"status": "ambiguous", "detail": "ledger_code_mismatch"}
        if ledger_code == code:
            issued(ledger.get("issued_at"), "affiliate_ledger.issued_at")
    for claim in claims:
        try:
            recipients.setdefault(int(claim.get("uid")), set()).add("new_joiner_claims")
        except (TypeError, ValueError):
            return {"status": "ambiguous", "detail": "unparseable_recipient"}
        issued(claim.get("claimed_at"), "new_joiner_claims.claimed_at")
    if not recipients:
        return {"status": "non_welcome" if other_pool_exists else "unknown"}
    if len(recipients) > 1:
        return {"status": "ambiguous", "detail": "multiple_recipients"}
    if other_pool_exists:
        return {"status": "ambiguous", "detail": "code_in_multiple_pools"}
    uid, sources = next(iter(recipients.items()))
    # A matching issuance/claim timestamp proves when this recipient first
    # held the code. Generic created_at/updated_at fields are not issuance.
    return {"status": "ok", "user_id": uid, "sources": sorted(sources),
            "issued_at": min(t for t, _ in issuance_times) if issuance_times else None,
            "issuance_time_sources": sorted({s for _, s in issuance_times})}


def resolve_welcome_recipient(db, code: str) -> dict:
    """Live, per-code form of :func:`decide_recipient`."""
    return decide_recipient(
        code=code,
        pool_row=db.voucher_pools.find_one({"pool_id": WELCOME_POOL_ID, "code": code}),
        ledger_for=lambda uid: db.affiliate_ledger.find_one(
            {"dedup_key": f"WELCOME:{uid}"}, {"voucher_code": 1, "issued_at": 1}),
        claims=list(db.new_joiner_claims.find({"code": code}, {"uid": 1, "claimed_at": 1})),
        other_pool_exists=bool(
            db.voucher_pools.find_one({"code": code, "pool_id": {"$ne": WELCOME_POOL_ID}}, {"pool_id": 1})
        ),
    )


def welcome_issuance_problem(recipient: dict, redeemed_at: datetime | None) -> str | None:
    """Shared, read-only chronology check for live, preview and history seeding."""
    if recipient.get("status") != "ok":
        return "unproven_welcome_recipient"
    issued_at = _aware_utc(recipient.get("issued_at"))
    if issued_at is None:
        return "missing_welcome_issued_at"
    when = _aware_utc(redeemed_at)
    if when is None:
        return "missing_redeemed_at"
    if when < issued_at:
        return "redemption_before_welcome_issuance"
    return None


# ---------------------------------------------------------------------------
# Evidence ingestion (idempotent)
# ---------------------------------------------------------------------------

def _observation_fingerprint(successful: bool, redeemed_at: datetime | None, fields=None,
                             *, redemption_status_known: bool = True) -> str:
    stamp = redeemed_at.isoformat() if redeemed_at else ""
    gate_values = {k: (fields or {}).get(k) for k in (
        "coupon_new_player", "after_total_bet_amount", "transport_version", "seven_day_complete")}
    encoded = json.dumps(gate_values, sort_keys=True, default=str)
    return hashlib.sha256(f"{RULE_VERSION}|{QUALIFICATION_GATE_VERSION}|{int(bool(successful))}|{redemption_status_known}|{stamp}|{encoded}".encode()).hexdigest()[:16]


def build_evidence_doc(
    *,
    source: str,
    code: str,
    recipient: dict,
    account_raw,
    namespace,
    redeemed_at: datetime | None,
    redemption_successful: bool,
    campaign_id=None,
    now_utc: datetime,
    account_reason: str | None = None,
    redeemed_at_reason: str | None = None,
    evidence_basis: dict | None = None,
    observation: str | None = None,
    qualification_fields: dict | None = None,
    observation_order: str | None = None,
    redemption_status_known: bool = True,
) -> dict:
    """The evidence document for one normalised observation (no I/O).
    ``source_ref`` is its idempotency key: same code + account = same row.

    ``account_reason`` / ``redeemed_at_reason`` come from source provenance
    (e.g. an account that was a numeric spreadsheet cell, a redeem time whose
    timezone is unknown): the value is then not trusted and the reason routes
    the evidence to review. ``evidence_basis`` records how success, time and
    account were established. ``observation`` is consumed by the caller."""
    if account_reason:
        account_key = None
    else:
        account_key, account_reason = canonical_account_key(account_raw, namespace)
    chash = code_hash(code)
    return {
        "source": source,
        "source_ref": hashlib.sha256(f"{RULE_VERSION}|{source}|{chash}|{account_key or account_reason}".encode()).hexdigest()[:40],
        "rule_version": RULE_VERSION,
        "code_hash": chash,
        "code_masked": mask_value(code),
        "account_key": account_key,
        "account_key_masked": mask_value(account_key) if account_key else None,
        "account_reason": account_reason,
        "redeemed_at": redeemed_at,
        "redeemed_at_reason": None if redeemed_at else (redeemed_at_reason or None),
        "redemption_successful": bool(redemption_successful),
        "redemption_status_known": redemption_status_known,
        "evidence_basis": evidence_basis,
        "qualification_fields": qualification_fields,
        "authoritative_order": observation_order,
        "qualification_gate_version": QUALIFICATION_GATE_VERSION,
        "observation_fingerprint": _observation_fingerprint(redemption_successful, redeemed_at, qualification_fields,
                                                            redemption_status_known=redemption_status_known),
        "campaign_id": None if campaign_id is None else str(campaign_id),
        "recipient_resolution": recipient,
        "status": EV_RECEIVED,
        "attempts": 0,
        "created_at": now_utc,
    }


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
    account_reason: str | None = None,
    redeemed_at_reason: str | None = None,
    evidence_basis: dict | None = None,
    observation: str | None = None,
    qualification_fields: dict | None = None,
    observation_order: str | None = None,
    redemption_status_known: bool = True,
) -> dict:
    """Upsert one redemption observation. Re-importing the same row is a
    no-op; a conflicting re-observation (status or time changed) is routed
    to review per the correction policy instead of silently overwriting.
    A ``removed`` observation (a later source import dropped the row) never
    creates evidence; it flags existing evidence for review instead."""
    code_n = normalize_code(code)
    if not code_n:
        return {"recorded": False, "reason": "missing_code"}
    recipient = resolve_welcome_recipient(db, code_n)
    if recipient["status"] in ("unknown", "non_welcome"):
        # Not a Welcome code: rejected here and only counted, so the
        # evidence collection does not mirror every campaign's coupon rows.
        return {"recorded": False, "reason": f"{recipient['status']}_code"}
    insert_doc = build_evidence_doc(
        source=source, code=code_n, recipient=recipient, account_raw=account_raw, namespace=namespace,
        redeemed_at=redeemed_at, redemption_successful=redemption_successful, campaign_id=campaign_id,
        now_utc=now_utc, account_reason=account_reason, redeemed_at_reason=redeemed_at_reason,
        evidence_basis=evidence_basis, qualification_fields=qualification_fields, observation_order=observation_order,
        redemption_status_known=redemption_status_known,
    )
    if observation == OBS_REMOVED:
        existing = db[EVIDENCE_COLLECTION].find_one({"source": source, "source_ref": insert_doc["source_ref"]})
        if existing is None:
            return {"recorded": False, "reason": "removal_without_evidence"}
        db[EVIDENCE_COLLECTION].update_one(
            {"_id": existing["_id"]},
            {"$addToSet": {"removed_in_batch_ids": source_batch_id}, "$set": {"last_seen_at": now_utc}},
        )
        _flag_conflicting_observation(db, existing, now_utc=now_utc, reason="source_row_removed_by_later_import")
        return {"recorded": True, "new": False, "conflict": True, "removed": True}
    source_ref = insert_doc["source_ref"]
    fingerprint = insert_doc["observation_fingerprint"]
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
    # Only committed Databot observations carry a trustworthy total order.
    # CAS protects concurrent extracts/retries from replacing a newer observation.
    if source == SOURCE_DATABOT_UIM and observation_order and before.get("authoritative_order"):
        if observation_order < before["authoritative_order"]:
            return {"recorded": True, "new": False, "stale": True}
        same_redemption = (_aware_utc(before.get("redeemed_at")) == _aware_utc(redeemed_at)
                           and before.get("redemption_successful") == redemption_successful)
        changed = before.get("observation_fingerprint") != fingerprint
        if observation_order > before["authoritative_order"] and same_redemption:
            if not changed:
                db[EVIDENCE_COLLECTION].update_one(
                    {"_id": before["_id"], "authoritative_order": before["authoritative_order"],
                     "observation_fingerprint": fingerprint},
                    {"$set": {"authoritative_order": observation_order}},
                )
                return {"recorded": True, "new": False}
            # Retain earned effects; changed credited evidence is reconciled manually.
            retryable = before.get("status") == EV_REVIEW and not before.get("source_conflict_at")
            if changed and (before.get("status") not in (EV_RECEIVED, EV_REVIEW) or before.get("lease_until")
                            or before.get("qualification_decision_at") or not (retryable or before.get("status") == EV_RECEIVED)):
                _flag_conflicting_observation(db, before, now_utc=now_utc)
                return {"recorded": True, "new": False, "conflict": True}
            replaced = db[EVIDENCE_COLLECTION].find_one_and_update(
                {"_id": before["_id"], "authoritative_order": before["authoritative_order"],
                 "observation_fingerprint": before.get("observation_fingerprint"),
                 "status": before.get("status"), "lease_until": {"$exists": False},
                 "qualification_decision_at": {"$exists": False}, "source_conflict_at": {"$exists": False}},
                {"$set": {"qualification_fields": qualification_fields, "authoritative_order": observation_order,
                          "observation_fingerprint": fingerprint, "evidence_basis": evidence_basis,
                          "status": EV_RECEIVED, "attempts": 0}, "$unset": {"reason": ""}},
                return_document=ReturnDocument.AFTER,
            )
            if replaced is not None:
                return {"recorded": True, "new": False, "corrected": changed}
            # Lost a race to evaluation or a newer correction. Never silently pass
            # the old values; the existing manual correction path is conservative.
            current = db[EVIDENCE_COLLECTION].find_one({"_id": before["_id"]})
            if current and (current.get("authoritative_order") or "") >= observation_order:
                return {"recorded": True, "new": False, "stale": True}
            _flag_conflicting_observation(db, current or before, now_utc=now_utc)
            return {"recorded": True, "new": False, "conflict": True}
    if before.get("observation_fingerprint") != fingerprint:
        _flag_conflicting_observation(db, before, now_utc=now_utc)
        return {"recorded": True, "new": False, "conflict": True}
    return {"recorded": True, "new": False}


def _flag_conflicting_observation(db, evidence: dict, *, now_utc: datetime,
                                  reason: str = "conflicting_source_observations") -> None:
    """Correction policy: a changed observation never rewrites evidence.
    Unprocessed evidence goes to review; a qualification it already produced
    is flagged for manual reconciliation (never auto-revoked)."""
    ev_id = evidence["_id"]
    # Persist the blocker even on unprocessed evidence so requeue cannot make
    # a stale passing snapshot eligible again without reconciliation.
    db[EVIDENCE_COLLECTION].update_one(
        {"_id": ev_id}, {"$set": {"source_conflict_at": now_utc, "source_conflict_reason": reason}})
    if evidence.get("status") == EV_RECEIVED:
        db[EVIDENCE_COLLECTION].update_one(
            {"_id": ev_id, "status": EV_RECEIVED},
            {"$set": {"status": EV_REVIEW, "reason": reason, "updated_at": now_utc}},
        )
        _release_reservation_for(db, ev_id)
    else:
        db[EVIDENCE_COLLECTION].update_one(
            {"_id": ev_id},
            {"$set": {"source_conflict_at": now_utc, "updated_at": now_utc}},
        )
    db.qualified_events.update_one(
        {"evidence_id": ev_id},
        {"$set": {"evidence_status": "disputed_pending_reconciliation", "evidence_disputed_at": now_utc}},
    )
    logger.warning(
        "[AFF_QUAL][EVIDENCE_CONFLICT] evidence_id=%s status=%s code=%s",
        ev_id, evidence.get("status"), evidence.get("code_masked"),
    )


def parse_source_row(row: dict, config: dict) -> dict | None:
    """One committed source row -> observation kwargs for
    :func:`record_redemption_evidence` / :func:`build_evidence_doc`, or None
    when the row is outside the configured Welcome campaigns. Pure."""
    cfg = config or {}
    if cfg.get("source") == SOURCE_DATABOT_UIM:
        return _parse_databot_row(row, cfg)
    values = {_norm_column(k): v for k, v in row.items()}
    campaign_id = values.get("campaign_id")
    campaign_ids = {str(c).strip() for c in cfg.get("campaign_ids") or []}
    if campaign_ids and str(campaign_id or "").strip() not in campaign_ids:
        return None
    success_values = {str(v).strip().lower() for v in cfg.get("success_values") or []}
    status_raw = str(values.get(_norm_column(cfg["status_column"])) or "").strip().lower()
    ns_col = _norm_column(cfg["namespace_column"]) if cfg.get("namespace_column") else None
    return {
        "code": values.get(_norm_column(cfg["code_column"])),
        "account_raw": values.get(_norm_column(cfg["account_column"])),
        "namespace": values.get(ns_col) if ns_col else cfg.get("account_namespace"),
        "redeemed_at": parse_redeemed_at(values.get(_norm_column(cfg["redeemed_at_column"])), cfg["source_timezone"]),
        "redemption_successful": bool(status_raw) and status_raw in success_values,
        "redemption_status_known": status_raw in success_values or status_raw in {
            "failed", "failure", "unsuccessful", "rejected"},
        "campaign_id": campaign_id,
        "qualification_fields": {"coupon_new_player": values.get("coupon_new_player"),
                                 "after_total_bet_amount": values.get("after_total_bet_amount"),
                                 **({"seven_day_complete": values["seven_day_complete"]}
                                    if "seven_day_complete" in values else {})},
    }


OBS_REMOVED = "removed"
_DIGITS_ACCOUNT_RE = re.compile(r"^\d+(\.0+)?$")


def _databot_account_reason(row: dict) -> str | None:
    """Why a synced UIM account string cannot be trusted as text, from the
    import provenance Databot records (``account_basis``):
    ``numeric`` = an XLSX numeric cell (leading zeros may be gone).
    For rows imported before provenance existed the file type decides:
    Databot's CSV import keeps every cell as text (csv.DictReader), so a CSV
    account is exact; an all-digit account from an XLSX file may have been a
    numeric cell and cannot be verified."""
    account = row.get("account")
    if not isinstance(account, str):
        return None  # canonical_account_key refuses non-text itself
    basis = row.get("account_basis")
    if basis == "numeric":
        return "account_numeric_coerced"
    if basis == "text":
        return None
    if row.get("file_type") == "csv":
        return None
    if _DIGITS_ACCOUNT_RE.match(account.strip()):
        return "account_numeric_unverifiable"
    return None


def _databot_redeemed_at(row: dict, cfg: dict) -> tuple[datetime | None, str | None]:
    """Redemption instant from a synced UIM row, honouring how Databot says
    the stored value relates to the source text. Never guesses: a time whose
    timezone, day/month order or time-of-day is unknown is ``None`` + reason."""
    if _norm_column(row.get("redeemed_at_column")) in _CLAIM_TIME_COLUMNS:
        return None, "redeemed_at_column_is_a_claim_time"
    basis = row.get("redeemed_at_basis")
    if basis == "source_offset":
        return parse_redeemed_at(row.get("redeemed_at_utc"), cfg["source_timezone"]), None
    if basis == "unknown_legacy":
        if (cfg.get("legacy_time_basis") or LEGACY_TIME_REVIEW) != LEGACY_TIME_NAIVE:
            return None, "redeemed_at_timezone_unverified"
        basis = "naive_wallclock"
    if basis == "naive_wallclock":
        parsed = parse_redeemed_at(row.get("redeemed_at_wallclock"), cfg["source_timezone"])
        return parsed, None if parsed else "missing_redeemed_at"
    if basis in ("date_only", "ambiguous_day_month"):
        return None, f"redeemed_at_{basis}"
    return None, "missing_redeemed_at"


def _parse_databot_row(row: dict, cfg: dict) -> dict | None:
    """A synced Databot UIM row (feed contract v1) -> observation kwargs."""
    campaign = row.get("campaign")
    campaign_ids = {str(c).strip() for c in cfg.get("campaign_ids") or []}
    if campaign_ids and str(campaign or "").strip() not in campaign_ids:
        return None
    removed = row.get("observation") == "deleted"
    redeemed_at, redeemed_at_reason = _databot_redeemed_at(row, cfg)
    return {
        "code": row.get("coupon_code"),
        "account_raw": row.get("account"),
        "namespace": cfg.get("account_namespace"),
        "redeemed_at": redeemed_at,
        # No status column exists: success rests solely on the attested
        # source contract (validate_source_config refuses the source without it).
        "redemption_successful": (not removed) and cfg.get("success_basis") == SUCCESS_BASIS_SOURCE_CONTRACT,
        "campaign_id": campaign,
        "account_reason": _databot_account_reason(row),
        "redeemed_at_reason": redeemed_at_reason,
        "observation": OBS_REMOVED if removed else None,
        "observation_order": row.get("_observation_order"),
        "qualification_fields": {
            "coupon_new_player": row.get("coupon_new_player"),
            "after_total_bet_amount": row.get("after_total_bet_amount"),
            "source_columns": row.get("qualification_source_columns"),
            "transport_version": row.get("qualification_fields_version"),
            "seven_day_complete": (row.get("seven_day_complete") if "seven_day_complete" in row else
                True if type(row.get("seven_day_completeness_retained_version")) is int
                and row["seven_day_completeness_retained_version"] == 1 else None),
        },
        "evidence_basis": {
            "success": cfg.get("success_basis"),
            "success_attestation_ref": (cfg.get("success_attestation") or {}).get("reference"),
            "redeemed_at": row.get("redeemed_at_basis"),
            "account": row.get("account_basis"),
            "provenance": row.get("provenance"),
            "source_row_ref": row.get("_id"),
        },
    }


def databot_observation_order(batch: dict, row: dict) -> str | None:
    stamp = _aware_utc(batch.get("committed_at"))
    if stamp is None or not row.get("_id"):
        return None
    return f"{stamp.isoformat(timespec='microseconds')}|{batch.get('_id')}|{row['_id']}"


def iter_committed_source_rows(db, config: dict | None):
    """``(batch_id, row)`` for every row of every committed, not rolled-back
    batch of the configured source, batches in commit order, rows in ``_id``
    order (the order corrections were made in). Read-only."""
    spec = source_spec(config)
    key = spec["batch_key"]
    batches = db[spec["batches"]].find(committed_batch_filter(config), {key: 1, spec["order"]: 1}).sort([(spec["order"], 1), (key, 1)])
    for batch in list(batches):
        for row in db[spec["rows"]].find({key: batch.get(key)}).sort("_id", 1):
            if source_name(config) == SOURCE_DATABOT_UIM:
                row["_observation_order"] = databot_observation_order(batch, row)
            yield batch.get(key), row


def extract_marketing_batch(
    db, *, upload_batch_id: str, config: dict, now_utc: datetime, after_id=None, max_rows: int | None = None
) -> dict:
    """Turn (part of) one committed source batch (a marketing upload, or a
    synced Databot UIM batch — ``config["source"]``) into evidence, in
    ``_id`` order from ``after_id``. Idempotent, so a resumed or repeated
    pass over the same rows changes nothing."""
    summary = {"rows": 0, "recorded": 0, "new": 0, "conflicts": 0, "missing_code": 0,
               "unknown_code": 0, "non_welcome_code": 0, "skipped_campaign": 0}
    spec = source_spec(config)
    query = {spec["batch_key"]: upload_batch_id}
    if after_id is not None:
        query["_id"] = {"$gt": after_id}
    cursor = db[spec["rows"]].find(query).sort("_id", 1)
    if max_rows:
        cursor = cursor.limit(int(max_rows))
    batch = db[spec["batches"]].find_one({spec["batch_key"]: upload_batch_id}) or {}
    last_id = after_id
    for row in cursor:
        if source_name(config) == SOURCE_DATABOT_UIM:
            row["_observation_order"] = databot_observation_order(batch, row)
        last_id = row.get("_id")
        summary["rows"] += 1
        obs = parse_source_row(row, config)
        if obs is None:
            summary["skipped_campaign"] += 1
            continue
        out = record_redemption_evidence(
            db,
            source=source_name(config),
            source_batch_id=upload_batch_id,
            source_row_id=row.get("_id"),
            now_utc=now_utc,
            **obs,
        )
        if not out.get("recorded"):
            summary[out["reason"]] = summary.get(out["reason"], 0) + 1
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
    spec = source_spec(config)
    batches = db[spec["batches"]]
    while budget > 0:
        batch = batches.find_one_and_update(
            {
                **committed_batch_filter(config),
                "$and": [{"$or": [{"welcome_v2_evidence_rule_version": {"$ne": RULE_VERSION}},
                                    {"welcome_v2_evidence_extracted_at": {"$exists": False}}]}],
                "$or": [
                    {"welcome_v2_evidence_lease_until": {"$exists": False}},
                    {"welcome_v2_evidence_lease_until": {"$lt": now_utc}},
                ],
            },
            {"$set": {"welcome_v2_evidence_lease_until": now_utc + BATCH_EXTRACT_LEASE}},
            sort=[(spec["order"], 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not batch:
            break
        summary = extract_marketing_batch(
            db, upload_batch_id=batch[spec["batch_key"]], config=config, now_utc=now_utc,
            after_id=(batch.get("welcome_v2_evidence_cursor")
                      if batch.get("welcome_v2_evidence_rule_version") == RULE_VERSION else None), max_rows=budget,
        )
        budget -= summary["rows"]
        counters = {f"welcome_v2_evidence_summary.{k}": v for k, v in summary.items() if k not in ("last_id", "done")}
        done_fields = {"welcome_v2_evidence_extracted_at": now_utc} if summary["done"] else {}
        batches.update_one(
            {"_id": batch["_id"]},
            {
                "$set": {"welcome_v2_evidence_cursor": summary["last_id"],
                          "welcome_v2_evidence_rule_version": RULE_VERSION, **done_fields},
                "$inc": counters,
                "$unset": {"welcome_v2_evidence_lease_until": ""},
            },
        )
        out["batches_completed"] += int(summary["done"])
        out["rows"] += summary["rows"]
        out["new"] += summary["new"]
        out["conflicts"] += summary["conflicts"]
        logger.info(
            "[AFF_QUAL][BATCH_EXTRACT] source=%s batch_id=%s done=%s rows=%s new=%s",
            spec["rows"], batch[spec["batch_key"]], summary["done"], summary["rows"], summary["new"],
        )
        if not summary["done"]:
            break
    return out


# ---------------------------------------------------------------------------
# Attribution and ownership
# ---------------------------------------------------------------------------

class DbLookup:
    """Read-only lookups the validation rules need, one query each (live).
    The shadow preview supplies an in-memory implementation of the same
    methods, so both run identical rules."""

    def __init__(self, db):
        self.db = db

    def pending_referrals(self, invitee_id: int) -> list[dict]:
        return list(
            self.db.pending_referrals.find(
                {"invitee_user_id": int(invitee_id)},
                {"inviter_user_id": 1, "status": 1, "revoked_reason": 1, "created_at_utc": 1, "destination_type": 1,
                 "qualification_reason": 1, "qualification_rule_version": 1},
            ).sort("created_at_utc", 1)
        )

    def user(self, uid: int) -> dict | None:
        return self.db.users.find_one(
            {"user_id": int(uid)}, {"joined_main_at": 1, "created_at": 1, "linked_gaming_accounts": 1}
        )

    def account_linked_to_other_user(self, account_id: str, invitee_id: int) -> bool:
        return bool(self.db.users.find_one(
            {"linked_gaming_accounts": account_id, "user_id": {"$ne": int(invitee_id)}}, {"_id": 1}
        ))

    def existing_qualification(self, invitee_id: int) -> dict | None:
        return self.db.qualified_events.find_one({"invitee_id": int(invitee_id)})

    def conflicting_record(self, evidence: dict) -> bool:
        return bool(self.db[EVIDENCE_COLLECTION].find_one(
            {
                "code_hash": evidence["code_hash"],
                "_id": {"$ne": evidence.get("_id")},
                "account_key": {"$nin": [None, evidence.get("account_key")]},
                "redemption_successful": True,
                "status": {"$ne": EV_VOIDED},
            },
            {"_id": 1},
        ))


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


def resolve_attribution(lookup, *, invitee_id: int, redeemed_at: datetime) -> dict:
    """The original referral record for this invitee, frozen.

    Earliest ``pending_referrals`` row that is a valid attribution (any
    status except a revocation that invalidates the record itself). Its
    inviter is used verbatim; nothing reads a mutable "current inviter".
    """
    rows = lookup.pending_referrals(invitee_id)
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
        if _preexisting_community_user(row, lookup.user(invitee_id)):
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


def check_account_ownership(lookup, *, invitee_id: int, inviter_id: int | None, account_key: str, policy: str) -> dict:
    """Validate the redeemed account against existing verified linkage
    (``users.linked_gaming_accounts``, synced from UIM). Conflicts never
    qualify; they stay in review."""
    account_id = account_id_from_key(account_key)
    if inviter_id is not None:
        if account_id in _linked_accounts(lookup.user(inviter_id)):
            return {"ok": False, "status": EV_REJECTED, "reason": "self_referral_account"}
    invitee_links = _linked_accounts(lookup.user(invitee_id))
    if invitee_links:
        if account_id in invitee_links:
            return {"ok": True, "basis": "verified_linkage"}
        return {"ok": False, "status": EV_REVIEW, "reason": "account_not_linked_to_invitee"}
    if lookup.account_linked_to_other_user(account_id, invitee_id):
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
    # A failed/held qualification is not a reversal. No settlement, XP or
    # referral_revoked event is manufactured for a never-qualified invitee.
    invitee = fields.get("invitee_id") or (evidence.get("recipient_resolution") or {}).get("user_id")
    if invitee and status in (EV_REVIEW, EV_REJECTED, EV_DUPLICATE_ACCOUNT):
        db.pending_referrals.update_many(
            {"invitee_user_id": invitee, "status": {"$in": list(ACTIVE_PENDING_STATUSES)}},
            {"$set": {"status": PENDING_AWAITING_REDEMPTION if status == EV_REVIEW else "rejected",
                      "qualification_rule_version": RULE_VERSION, "qualification_outcome": status,
                      "qualification_reason": reason, "qualification_checked_at": now_utc}},
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


ELIGIBLE = "eligible"


def assess_qualification_fields(fields: dict | None, *, require_transport_version: bool = False) -> dict:
    """Completed seven-day dollar result; no timer, sums, conversion or rounding.

    Supported true/1 and false/0 representations are explicit. Unknown words
    cannot establish the new-player condition.
    Missing/invalid input is review.
    """
    if not isinstance(fields, dict):
        return {"status": EV_REVIEW, "reason": "missing_or_invalid_qualification_fields"}
    if require_transport_version:
        version = fields.get("transport_version")
        # JSON integer 1 is the supported Databot qualification contract.
        # True, "1", 1.0 and missing/future versions are not that contract.
        if type(version) is not int or version != 1:
            return {"status": EV_REVIEW, "reason": "unsupported_qualification_transport_version"}
        if "seven_day_complete" not in fields:
            return {"status": EV_REVIEW, "reason": "seven_day_completeness_unverified"}
    if "seven_day_complete" in fields and fields["seven_day_complete"] is not True:
        return {"status": EV_REVIEW, "reason": "seven_day_data_incomplete"}
    flag = fields.get("coupon_new_player")
    explicit_false = flag is False or (isinstance(flag, str) and flag.strip().lower() == "false")
    flag_ok = (type(flag) in (int, float) and flag in (0, 1)) or (
        isinstance(flag, str) and flag.strip() in ("0", "1", "true")) or explicit_false or flag is True
    raw = fields.get("after_total_bet_amount")
    amount = None
    if not isinstance(raw, bool) and isinstance(raw, (str, int, float, Decimal)):
        text = str(raw).strip()
        # Decimal accepts underscores and unusual forms; only ordinary source
        # decimal/scientific notation is documented, no currency/comma rewriting.
        if re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", text):
            try:
                candidate = Decimal(text)
                if candidate.is_finite() and candidate >= 0:
                    amount = candidate
            except (InvalidOperation, ValueError):
                pass
    if not flag_ok or amount is None:
        return {"status": EV_REVIEW, "reason": "missing_or_invalid_qualification_fields"}
    if explicit_false or str(flag).strip() == "0" or flag == 0:
        return {"status": EV_REJECTED, "reason": "not_a_new_coupon_player"}
    if amount < Decimal("80"):
        return {"status": EV_REJECTED, "reason": "seven_day_turnover_below_80"}
    return {"status": ELIGIBLE, "reason": None}


def assess_evidence(evidence: dict, *, control: dict, now_utc: datetime, lookup) -> dict:
    """Every validation rule of welcome_redemption_v2, with NO writes.

    Returns ``{"status": ELIGIBLE, ...qualification fields}`` or a final
    evidence status + reason. Live processing (:func:`evaluate_evidence`) and
    the admin shadow preview both call exactly this function; only what they
    do with the verdict differs. The lifetime account check is not here: live
    it is the registry insert in :func:`_qualify`, in the preview a simulated
    registry.
    """
    def verdict(status, reason, **fields):
        return {"status": status, "reason": reason, **fields}

    if evidence.get("voided_at"):
        return verdict(EV_VOIDED, evidence.get("void_reason") or "voided")
    if (evidence.get("rule_version") != RULE_VERSION
            or evidence.get("qualification_gate_version") != QUALIFICATION_GATE_VERSION):
        return verdict(EV_REVIEW, "evidence_rule_version_mismatch")
    if evidence.get("source_conflict_at"):
        return verdict(EV_REVIEW, evidence.get("source_conflict_reason") or "conflicting_source_observations")
    redeemed_at = _aware_utc(evidence.get("redeemed_at"))
    if redeemed_at is None:
        return verdict(EV_REVIEW, evidence.get("redeemed_at_reason") or "missing_redeemed_at")
    if redeemed_at > _aware_utc(now_utc) + FUTURE_REDEMPTION_TOLERANCE:
        # A redemption cannot postdate its own processing; never attribute
        # (or reward) a future month on a bad source timestamp.
        return verdict(EV_REVIEW, "redeemed_at_in_future")
    if not evidence.get("redemption_successful"):
        if evidence.get("redemption_status_known") is not True:
            return verdict(EV_REVIEW, "redemption_status_unknown")
        return verdict(EV_REJECTED, "redemption_not_successful")
    account_key = evidence.get("account_key")
    if not account_key:
        reason = evidence.get("account_reason") or "missing_account"
        return verdict(EV_REVIEW, reason)
    cfg_campaigns = {str(c) for c in ((control.get("source_config") or {}).get("campaign_ids") or [])}
    if cfg_campaigns and str(evidence.get("campaign_id") or "") not in cfg_campaigns:
        return verdict(EV_REJECTED, "non_welcome_campaign")

    recipient = evidence.get("recipient_resolution") or {}
    rstatus = recipient.get("status")
    if rstatus == "unknown":
        return verdict(EV_REJECTED, "unknown_code")
    if rstatus == "non_welcome":
        return verdict(EV_REJECTED, "non_welcome_code")
    if rstatus == "not_issued":
        return verdict(EV_REVIEW, "welcome_code_not_issued")
    if rstatus != "ok":
        return verdict(EV_REVIEW, "ambiguous_recipient")
    invitee_id = int(recipient["user_id"])

    issuance_problem = welcome_issuance_problem(recipient, redeemed_at)
    if issuance_problem:
        return verdict(EV_REVIEW, issuance_problem, invitee_id=invitee_id)

    if lookup.conflicting_record(evidence):
        return verdict(EV_REVIEW, "conflicting_redemption_records", invitee_id=invitee_id)

    policy = control.get("unlinked_account_policy") or UNLINKED_POLICY_ACCEPT
    existing_q = lookup.existing_qualification(invitee_id)
    if existing_q and existing_q.get("evidence_id") != evidence.get("_id"):
        own = check_account_ownership(
            lookup, invitee_id=invitee_id, inviter_id=existing_q.get("referrer_id"), account_key=account_key, policy=policy
        )
        return verdict(
            EV_INVITEE_ALREADY_QUALIFIED, "invitee_already_qualified", invitee_id=invitee_id,
            inviter_id=existing_q.get("referrer_id"), existing_qualification=existing_q,
            # The account is tied to a pre-rule qualification: it counts as credited.
            legacy_credit=bool(own["ok"] and existing_q.get("rule_version") != RULE_VERSION),
        )

    # An old terminal referral is not part of the activation population. If
    # another active row exists, attribution still stays with the ORIGINAL
    # inviter below; never redirect an old attribution to a later inviter.
    pending_rows = lookup.pending_referrals(invitee_id)
    # Rollout membership is read from the referral rows themselves, never from the cohort snapshot (which
    # may be stale during the activation window). A row carries qualification_rule_version=V2 only if it
    # was active when activation marked it, was parked, or was decided under V2 while active, so a later V2
    # status change (e.g. a rule rejection followed by a supported requeue) cannot erase membership;
    # referrals created at/after the cutoff are future referrals and always in scope. An absence-based
    # rejection is the one decision deliberately never reopened automatically.
    cutoff_at = launch_cutoff(control)
    in_cohort = (any(r.get("qualification_rule_version") == RULE_VERSION
                     or (cutoff_at is not None and (_aware_utc(r.get("created_at_utc")) or cutoff_at - timedelta(days=1)) >= cutoff_at)
                     for r in pending_rows)
                 and not any(r.get("qualification_reason") == ABSENCE_REJECTION_REASON for r in pending_rows))
    if (not existing_q and not in_cohort and launch_cutoff(control) is not None and pending_rows
            and not any(r.get("status") in ACTIVE_PENDING_STATUSES for r in pending_rows)):
        return verdict(EV_REVIEW, "referral_not_active_at_rollout", invitee_id=invitee_id)

    attribution = resolve_attribution(lookup, invitee_id=invitee_id, redeemed_at=redeemed_at)
    if not attribution["ok"]:
        return verdict(attribution["status"], attribution["reason"], invitee_id=invitee_id)
    inviter_id = attribution["inviter_id"]
    own = check_account_ownership(lookup, invitee_id=invitee_id, inviter_id=inviter_id, account_key=account_key, policy=policy)
    if not own["ok"]:
        return verdict(own["status"], own["reason"], invitee_id=invitee_id, inviter_id=inviter_id)

    # Existing earned events and historical seeding are handled above/outside
    # this validator. Every prospective award under the launched rule uses gates.
    gates = assess_qualification_fields(
        evidence.get("qualification_fields"),
        require_transport_version=evidence.get("source") == SOURCE_DATABOT_UIM,
    )
    if gates["status"] != ELIGIBLE:
        return verdict(gates["status"], gates["reason"], invitee_id=invitee_id, inviter_id=inviter_id)

    cutoff = launch_cutoff(control)
    if cutoff is not None and redeemed_at < cutoff:
        qualified_at, basis = cutoff, "launch_cutoff_clamp"
    else:
        qualified_at, basis = redeemed_at, "redeemed_at"
    return verdict(
        ELIGIBLE, None, invitee_id=invitee_id, inviter_id=inviter_id, pending_id=attribution.get("pending_id"),
        redeemed_at=redeemed_at, qualified_at=qualified_at, attribution_basis=basis, ownership_basis=own["basis"],
    )


def evaluate_evidence(db, evidence: dict, *, control: dict, now_utc: datetime) -> dict:
    """Validate one evidence row and, if eligible, qualify atomically."""
    # Workers can hold an earlier snapshot while another extractor observes a
    # correction. Re-read the canonical row before making any award decision.
    evidence = db[EVIDENCE_COLLECTION].find_one({"_id": evidence["_id"]}) or evidence
    if evidence.get("source_row_ids") and _code_from_source_rows(db, evidence, control.get("source_config")) is None:
        return _finalize(db, evidence, EV_REVIEW, "source_row_unavailable", now_utc=now_utc)
    a = assess_evidence(evidence, control=control, now_utc=now_utc, lookup=DbLookup(db))
    extra = {k: a[k] for k in ("invitee_id", "inviter_id") if a.get(k) is not None}
    if a["status"] == EV_INVITEE_ALREADY_QUALIFIED:
        if a["legacy_credit"]:
            _record_legacy_credit(db, evidence, a["existing_qualification"], now_utc=now_utc)
        return _finalize(db, evidence, a["status"], a["reason"], now_utc=now_utc, extra={"invitee_id": a["invitee_id"]})
    if a["status"] != ELIGIBLE:
        return _finalize(db, evidence, a["status"], a["reason"], now_utc=now_utc, extra=extra)
    return _qualify(
        db,
        evidence,
        invitee_id=a["invitee_id"],
        inviter_id=a["inviter_id"],
        pending_id=a["pending_id"],
        redeemed_at=a["redeemed_at"],
        qualified_at=a["qualified_at"],
        attribution_basis=a["attribution_basis"],
        ownership_basis=a["ownership_basis"],
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
    # Compare-and-set freezes the version actually validated. A correction
    # observed before this decision cannot slip between validation and reserve.
    current = db[EVIDENCE_COLLECTION].find_one_and_update(
        {"_id": ev_id, "status": EV_RECEIVED, "source_conflict_at": {"$exists": False},
         "observation_fingerprint": evidence.get("observation_fingerprint")},
        {"$set": {"qualification_decision_at": now_utc}},
        return_document=ReturnDocument.AFTER,
    )
    if current is None:
        latest = db[EVIDENCE_COLLECTION].find_one({"_id": ev_id}) or {}
        return {"status": latest.get("status", EV_REVIEW),
                "reason": latest.get("reason") or "source_observation_changed"}
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
        "qualification_gate_version": QUALIFICATION_GATE_VERSION,
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
    correction = db[EVIDENCE_COLLECTION].find_one({"_id": ev_id}, {"source_conflict_at": 1}) or {}
    if correction.get("source_conflict_at"):
        db.qualified_events.update_one(
            {"_id": q_id}, {"$set": {"evidence_status": "disputed_pending_reconciliation",
                                      "evidence_disputed_at": correction["source_conflict_at"]}},
        )
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
    config = (control or {}).get("source_config") or {}
    spec = source_spec(config)
    if db[spec["batches"]].find_one({**committed_batch_filter(config),
                                   "$or": [{"welcome_v2_evidence_rule_version": {"$ne": RULE_VERSION}},
                                           {"welcome_v2_evidence_extracted_at": {"$exists": False}}]}, {"_id": 1}):
        out["skipped"] = "source_extraction_incomplete"
        return out
    if source_name(config) == SOURCE_DATABOT_UIM:
        health = integration_readiness(db, config, now_utc=now_utc)
        if not health["ok"]:
            out["skipped"] = "source_evidence_incomplete"
            return out
        sync_at = parse_redeemed_at(health["sync"].get("last_success_at"), "UTC")
        if sync_at:
            for held in db[EVIDENCE_COLLECTION].find(
                {"rule_version": RULE_VERSION, "status": EV_REVIEW,
                 "source_conflict_at": {"$exists": False}, "processed_at": {"$lt": sync_at}}
            ).limit(int(batch_limit)):
                requeue_evidence(db, evidence_id=held["_id"], now_utc=now_utc, config=config)
    col = db[EVIDENCE_COLLECTION]
    for _ in range(int(batch_limit)):
        evidence = col.find_one_and_update(
            {
                "status": EV_RECEIVED,
                "rule_version": RULE_VERSION,
                "$or": [{"lease_until": {"$exists": False}}, {"lease_until": {"$lt": now_utc}}],
            },
            {"$set": {"lease_until": now_utc + EVIDENCE_LEASE}, "$inc": {"attempts": 1}},
            # Earliest redemption first (then arrival, then id): when several V2
            # candidates share one account, the same deterministic order the
            # preview simulates decides who owns it, not the sync arrival order.
            sort=[("redeemed_at", 1), ("created_at", 1), ("_id", 1)],
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
    if source_name(config) == SOURCE_DATABOT_UIM:
        # The only conclusion drawn from a MISSING row: inside proven complete coverage and an attested policy.
        # The source is fully healthy here (integration_readiness above) and extraction is complete.
        import uim_coverage

        out["absence"] = uim_coverage.process_absence_rejections(db, control=control, now_utc=now_utc)
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


def void_upload_batch(db, *, upload_batch_id: str, reason: str, now_utc: datetime,
                      source: str = SOURCE_MARKETING) -> dict:
    """Rollback / correction of a source upload (documented policy).

    * The batch is marked ``rolled_back_at`` so it is never (re)extracted.
    * Evidence seen ONLY in this batch is voided: unprocessed or in-review
      evidence can then never qualify (its reservation, if any, is released).
    * Evidence that already produced a qualification is NOT revoked and its
      account reservation is NOT released: the qualification is flagged
      ``evidence_status=voided_pending_reconciliation`` for manual review.
      Issued rewards are never touched.
    """
    spec = SOURCE_SPECS[source]
    db[spec["batches"]].update_one(
        {spec["batch_key"]: upload_batch_id},
        {"$set": {"rolled_back_at": now_utc, "rollback_reason": reason}},
    )
    out = {"voided": 0, "flagged_qualified": 0, "kept_other_sources": 0}
    for ev in db[EVIDENCE_COLLECTION].find({"source": source, "source_batch_ids": upload_batch_id}):
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


def _code_from_source_rows(db, evidence: dict, config: dict | None) -> str | None:
    """The raw code for an evidence row, re-read from its committed source
    row (evidence itself stores only a hash). Verified against that hash."""
    code_col = "coupon_code" if evidence.get("source") == SOURCE_DATABOT_UIM else _norm_column((config or {}).get("code_column"))
    rows_col = SOURCE_SPECS.get(evidence.get("source") or SOURCE_MARKETING, SOURCE_SPECS[SOURCE_MARKETING])["rows"]
    for row_id in evidence.get("source_row_ids") or []:
        row = db[rows_col].find_one({"_id": row_id})
        if not row:
            continue
        spec = SOURCE_SPECS.get(evidence.get("source") or SOURCE_MARKETING, SOURCE_SPECS[SOURCE_MARKETING])
        if not db[spec["batches"]].find_one({**committed_batch_filter(config),
                                            spec["batch_key"]: row.get(spec["batch_key"])}):
            continue
        code = normalize_code({_norm_column(k): v for k, v in row.items()}.get(code_col))
        if code and code_hash(code) == evidence.get("code_hash"):
            return code
    return None


def requeue_evidence(db, *, evidence_id, now_utc: datetime, config: dict | None = None) -> bool:
    """Send a reviewed/rejected evidence row back through FULL validation
    (after the underlying data was fixed). The code -> recipient mapping is
    re-resolved from current voucher records, since that is usually what was
    fixed; if the source row can no longer be read, the row is not requeued.
    Never forces a qualification."""
    evidence = db[EVIDENCE_COLLECTION].find_one({"_id": evidence_id, "status": {"$in": [EV_REVIEW, EV_REJECTED]}})
    if not evidence:
        return False
    if config is None:
        config = (get_control(db) or {}).get("source_config")
    code = _code_from_source_rows(db, evidence, config)
    if code is None:
        logger.warning("[AFF_QUAL][REQUEUE_REFUSED] evidence_id=%s reason=source_row_unavailable", evidence_id)
        return False
    res = db[EVIDENCE_COLLECTION].update_one(
        {"_id": evidence_id, "status": {"$in": [EV_REVIEW, EV_REJECTED]}},
        {"$set": {"status": EV_RECEIVED, "requeued_at": now_utc, "updated_at": now_utc,
                  "recipient_resolution": resolve_welcome_recipient(db, code), "attempts": 0},
         "$unset": {"lease_until": "", "reason": ""}},
    )
    return bool(getattr(res, "modified_count", 0))


def consistency_report(db, *, now_utc: datetime) -> dict:
    """Registry <-> qualified_events consistency for v2 (read-only)."""
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
