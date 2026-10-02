"""Scheduled voucher batches for T1-T4 affiliate tiers and the WELCOME pool.

Adds a first-class ``affiliate_voucher_batches`` collection so admins can
upload a future T1-T4/WELCOME voucher pool with an explicit KL start/end
window, ahead of time, without touching the existing reward tiers,
qualification rules, ledger dedup keys or settlement/eligibility logic in
``affiliate_rewards.py``.

Each uploaded voucher code becomes its own ``voucher_pools`` row carrying a
denormalized copy of ``batch_id``/``batch_name``/``starts_at``/``ends_at``
so the hot claim path in ``affiliate_rewards._claim_voucher_from_pool`` can
stay a single ``find_one_and_update`` without joining back to this
collection.

Legacy ``voucher_pools`` rows that predate this feature (no ``batch_id``)
are treated as ``legacy_unbounded``: they keep their pre-existing
always-claimable behaviour until an admin explicitly migrates them into a
batch. Nothing here deletes or mutates those rows automatically.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

import pytz
from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, jsonify, request

from affiliate_rewards import _month_window_from_yyyymm as _entitlement_month_window_utc
# `database` is already in this module's import graph (affiliate_rewards
# imports it), so this is not a new cycle, and importing it has no side
# effect: database.py opens no connection at import time.
from database import _ensure_equivalent_index
from affiliate_reward_plans import (
    ADMIN_AFFILIATE_POOL_IDS,
    ENTITLEMENT_MONTH_POOL_IDS as _CANONICAL_ENTITLEMENT_MONTH_POOL_IDS,
    pool_denomination,
)

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

# Schedulable pools and entitlement-month pools both come from the single
# canonical catalogue in affiliate_reward_plans -- never restated here, so a
# backend validator and an admin dropdown can no longer drift apart (which
# is how T5 and the denomination pools ended up unuploadable).
BATCH_POOL_IDS = ADMIN_AFFILIATE_POOL_IDS

# Affiliate pools whose claimability requires a batch window that *fully
# contains* a KL calendar month, so their schedule must always be exactly
# that canonical month window -- never an admin-typed approximation (e.g.
# "00:01"/"23:59"). WELCOME has no monthly-entitlement concept and keeps its
# existing free-form start/end scheduling untouched.
ENTITLEMENT_MONTH_POOL_IDS = _CANONICAL_ENTITLEMENT_MONTH_POOL_IDS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _mask_code(code) -> str:
    code = str(code or "")
    if len(code) <= 4:
        return "*" * len(code)
    return code[:2] + "*" * (len(code) - 4) + code[-2:]


def _is_duplicate_key_error(exc: Exception) -> bool:
    # Works against both pymongo.errors.DuplicateKeyError (production,
    # real MongoDB) and any test double that names its exception the same
    # way, without requiring tests to depend on pymongo internals.
    return exc.__class__.__name__ == "DuplicateKeyError"


def parse_kl_local_to_utc(local_str: str, tz_name: str | None = None) -> datetime | None:
    """Parse an admin-entered local ("YYYY-MM-DD HH:MM:SS") datetime string
    in the given IANA timezone (default Asia/Kuala_Lumpur) into an aware
    UTC datetime. Returns None on any parse failure.
    """
    if not local_str or not str(local_str).strip():
        return None
    try:
        tz = pytz.timezone(str(tz_name).strip()) if tz_name else KL_TZ
    except Exception:
        return None
    raw = str(local_str).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            naive = datetime.strptime(raw, fmt)
            break
        except ValueError:
            naive = None
    if naive is None:
        return None
    try:
        localized = tz.localize(naive)
    except Exception:
        return None
    return localized.astimezone(timezone.utc)


def canonical_entitlement_month_window(entitlement_month: str) -> tuple[datetime | None, datetime | None]:
    """The one true KL-calendar-month window [start, end) in UTC for a
    ``"YYYYMM"`` entitlement month — first day of that month at 00:00:00 KL
    through the first day of the following month at 00:00:00 KL (end
    exclusive). Delegates to ``affiliate_rewards``'s own resolver so the
    boundary a batch is created with can never drift from the boundary
    ``_resolve_monthly_ledger_target``/``get_claimable_pool_inventory`` use
    to decide claimability. Returns ``(None, None)`` on any invalid input.
    """
    return _entitlement_month_window_utc(entitlement_month)


def _as_aware_utc(dt: datetime | None) -> datetime | None:
    """``database.py`` opens ``MongoClient`` without ``tz_aware=True``, so a
    datetime read back from a real MongoDB is naive (but always a UTC
    instant). Every value fetched from ``voucher_pools``/
    ``affiliate_voucher_batches`` must pass through here before it's
    compared against (aware) ``now_utc`` or converted with
    ``.astimezone()`` — otherwise a naive-vs-aware comparison raises
    ``TypeError``, and ``.astimezone()`` on a naive value would wrongly
    treat it as local time instead of UTC.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_kl_iso(dt: datetime | None) -> str | None:
    dt = _as_aware_utc(dt)
    if dt is None:
        return None
    return dt.astimezone(KL_TZ).isoformat()


def _to_utc_iso(dt: datetime | None) -> str | None:
    dt = _as_aware_utc(dt)
    if dt is None:
        return None
    return dt.isoformat()


def _fail(code: str, message: str) -> dict:
    return {"ok": False, "code": code, "message": message}


def normalize_voucher_codes(codes) -> tuple[list, int, int]:
    """Split/trim/dedupe voucher codes.

    Accepts either a list of strings (one code per item, possibly still
    comma/CSV-joined) or a single string blob. Returns
    ``(unique_codes_in_order, duplicates_in_upload, invalid_count)``.
    ``invalid`` counts trimmed tokens that contain internal whitespace
    (malformed codes) — plain blank lines from newline-splitting are
    dropped silently, not counted as invalid.
    """
    if isinstance(codes, str):
        raw_items = re.split(r"[\r\n,]+", codes)
    else:
        raw_items = []
        for item in codes or []:
            raw_items.extend(re.split(r"[\r\n,]+", str(item)))

    seen = set()
    unique = []
    duplicates = 0
    invalid = 0
    for raw in raw_items:
        code = raw.strip()
        if not code:
            continue
        if re.search(r"\s", code):
            invalid += 1
            continue
        if code in seen:
            duplicates += 1
            continue
        seen.add(code)
        unique.append(code)
    return unique, duplicates, invalid


def derive_batch_status(batch: dict, now_utc: datetime | None = None) -> str:
    """Status derived from time + inventory + emergency controls + upload
    lifecycle — never trust a manually maintained status field as the
    source of truth. Priority (highest first): failed, uploading
    (staging), disabled, scheduled, active, exhausted, expired. A
    ``staging``/``failed`` batch can never appear as Active regardless of
    its schedule window or inventory.
    """
    now_utc = _as_aware_utc(now_utc) or datetime.now(timezone.utc)
    upload_status = batch.get("upload_status")
    if upload_status == "failed":
        return "failed"
    if upload_status == "staging":
        return "uploading"
    if bool(batch.get("distribution_disabled")):
        return "disabled"
    starts_at = _as_aware_utc(batch.get("starts_at"))
    ends_at = _as_aware_utc(batch.get("ends_at"))
    if starts_at and now_utc < starts_at:
        return "scheduled"
    if ends_at and now_utc >= ends_at:
        return "expired"
    if int(batch.get("available_count") or 0) > 0:
        return "active"
    return "exhausted"


_STATUS_ORDER = {
    "active": 0, "scheduled": 1, "exhausted": 2, "expired": 3, "disabled": 4,
    "uploading": 5, "failed": 6,
}


def _sort_key(batch: dict, status: str):
    order = _STATUS_ORDER.get(status, 5)
    starts_at = _as_aware_utc(batch.get("starts_at"))
    ends_at = _as_aware_utc(batch.get("ends_at"))
    if status == "expired" and ends_at:
        secondary = -ends_at.timestamp()
    elif starts_at:
        secondary = starts_at.timestamp()
    else:
        secondary = 0.0
    return (order, secondary)


def _entitlement_month_for_batch(batch: dict) -> str | None:
    """Best-effort "YYYYMM" label (KL calendar) for the batch's starts_at —
    purely informational, so the dashboard can show which entitlement
    month a batch is presumed to correspond to.
    """
    starts_at = _as_aware_utc(batch.get("starts_at"))
    if starts_at is None:
        return None
    return starts_at.astimezone(KL_TZ).strftime("%Y%m")


def _serialize_batch(batch: dict, *, now_utc: datetime | None = None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    status = derive_batch_status(batch, now_utc)
    uploaded = int(batch.get("uploaded_count") or 0)
    available = int(batch.get("available_count") or 0)
    issued = int(batch.get("issued_count") or 0)
    out = {
        "batch_id": str(batch.get("_id")),
        "batch_name": batch.get("batch_name"),
        "pool_id": batch.get("pool_id"),
        "starts_at_utc": _to_utc_iso(batch.get("starts_at")),
        "ends_at_utc": _to_utc_iso(batch.get("ends_at")),
        "starts_at_kl": _to_kl_iso(batch.get("starts_at")),
        "ends_at_kl": _to_kl_iso(batch.get("ends_at")),
        "entitlement_month": _entitlement_month_for_batch(batch),
        "status": status,
        "uploaded_count": uploaded,
        "available_count": available,
        "issued_count": issued,
        "distribution_disabled": bool(batch.get("distribution_disabled")),
        "created_at": _to_utc_iso(batch.get("created_at")),
        "created_by": batch.get("created_by"),
        "notes": batch.get("notes"),
        "upload_status": batch.get("upload_status") or "ready",
        "submitted_count": int(batch.get("submitted_count") or 0),
        "inserted_count": int(batch.get("inserted_count") or 0),
        "duplicate_count": int(batch.get("duplicate_count") or 0),
        "invalid_count": int(batch.get("invalid_count") or 0),
        "upload_started_at": _to_utc_iso(batch.get("upload_started_at")),
        "upload_completed_at": _to_utc_iso(batch.get("upload_completed_at")),
        "upload_failed_at": _to_utc_iso(batch.get("upload_failed_at")),
        "upload_error_code": batch.get("upload_error_code"),
    }
    if status in ("exhausted", "expired"):
        out["exhausted_count"] = max(0, uploaded - available)
    return out


def _hydrate_live_counts(db, batch: dict) -> dict:
    """``available_count``/``issued_count`` are cached on the batch document
    for the initial upload, but the claim path (a single
    ``find_one_and_update`` on ``voucher_pools`` for atomicity) never writes
    back to this collection. Re-derive both counts from the actual
    ``voucher_pools`` rows so status derivation and the
    ``active_batch_edit_restricted`` check are always correct, never stale.
    """
    batch_id = batch.get("_id")
    if batch_id is None:
        return batch
    available = db.voucher_pools.count_documents({"batch_id": batch_id, "status": "available"})
    issued = db.voucher_pools.count_documents({"batch_id": batch_id, "status": "issued"})
    out = dict(batch)
    out["available_count"] = int(available)
    out["issued_count"] = int(issued)
    return out


def _serialize_voucher_row(row: dict) -> dict:
    return {
        "code": row.get("code"),
        "status": row.get("status"),
        "issued_to_user_id": row.get("issued_to_user_id"),
        "issued_at": _to_utc_iso(row.get("issued_at")),
        "created_at": _to_utc_iso(row.get("created_at")),
    }


def _bulk_update_rows(collection, query: dict, update: dict):
    """update_many when the driver supports it (real MongoDB); otherwise a
    find + update_one loop so this also works against the lightweight
    FakeCollection test doubles used across this codebase's test suite.
    """
    if hasattr(collection, "update_many"):
        return collection.update_many(query, update)
    count = 0
    for row in collection.find(query, projection={"_id": 1}):
        collection.update_one({"_id": row["_id"]}, update)
        count += 1
    return count


def _bulk_delete_rows(collection, query: dict) -> int:
    """delete_many when the driver supports it (real MongoDB); otherwise a
    find + delete_one loop for the lightweight FakeCollection test doubles.
    """
    if hasattr(collection, "delete_many"):
        result = collection.delete_many(query)
        return int(getattr(result, "deleted_count", 0) or 0)
    count = 0
    for row in list(collection.find(query, projection={"_id": 1})):
        collection.delete_one({"_id": row["_id"]})
        count += 1
    return count


def _find_overlapping_batch(db, *, pool_id: str, starts_at_utc: datetime, ends_at_utc: datetime, exclude_batch_id=None):
    query = {
        "pool_id": pool_id,
        "starts_at": {"$lt": ends_at_utc},
        "ends_at": {"$gt": starts_at_utc},
    }
    if exclude_batch_id is not None:
        query["_id"] = {"$ne": exclude_batch_id}
    return db.affiliate_voucher_batches.find_one(query)


def _legacy_unbounded_summary(db, *, pool_id: str | None = None) -> list:
    match = {"batch_id": {"$exists": False}}
    match["pool_id"] = str(pool_id).strip().upper() if pool_id else {"$in": list(BATCH_POOL_IDS) + ["T5"]}
    buckets: dict = {}
    for row in db.voucher_pools.find(match, projection={"pool_id": 1, "status": 1}):
        pid = row.get("pool_id")
        bucket = buckets.setdefault(pid, {"pool_id": pid, "available": 0, "issued": 0, "total": 0})
        bucket["total"] += 1
        if row.get("status") == "available":
            bucket["available"] += 1
        elif row.get("status") == "issued":
            bucket["issued"] += 1
    return sorted(buckets.values(), key=lambda b: b["pool_id"])


def _as_object_id(batch_id) -> ObjectId | None:
    try:
        return ObjectId(str(batch_id))
    except (InvalidId, TypeError):
        return None


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

#: The canonical ``affiliate_voucher_batches`` catalogue: the key pattern
#: each query needs, and the name a FRESH database should end up with.
#:
#: A name here is an intention, not a guarantee. On a database that already
#: carries an equivalent index under some other name — a production catalogue
#: predating this module, or one left behind by an intermediate migration
#: commit — the existing index is REUSED under its own name and no second
#: index is created. Only an empty database ends up with exactly these names.
AFFILIATE_VOUCHER_BATCH_INDEXES = (
    # Batch resolution / claimability: _find_batches_for_period,
    # _find_active_batch, _tier_entered_scheduled_mode. The pool_id-only
    # lookup rides this index's leading prefix, so it needs no index of
    # its own.
    ([("pool_id", 1), ("starts_at", 1), ("ends_at", 1)], "batch_pool_window"),
    # Expiry scans.
    ([("ends_at", 1)], "batch_ends_at"),
    # Operator "distribution paused" filter.
    ([("distribution_disabled", 1)], "batch_distribution_disabled"),
)


def ensure_affiliate_voucher_batch_indexes(db):
    """Bring ``affiliate_voucher_batches`` up to the catalogue above.

    Every creation goes through ``database._ensure_equivalent_index`` rather
    than a raw ``create_index``. That matters because this collection is
    reachable in several partially-migrated states, and a raw create is not
    safe in any of them:

      * An intermediate commit of this migration created
        ``aff_batch_pool_window`` over ``(pool_id, starts_at, ends_at)`` —
        the exact key pattern of ``batch_pool_window``. A database that ran
        that commit still carries the extra name. Asking MongoDB for a
        SECOND name over an already-indexed key pattern fails with
        ``IndexOptionsConflict`` (code 85), and because this runs at startup
        the process dies before serving anything.
      * The same commit created ``aff_batch_pool`` over ``(pool_id,)``.

    The helper resolves this by matching on the KEY PATTERN rather than the
    name: an equivalent index already present is adopted whatever it is
    called, so a stale name satisfies the requirement instead of colliding
    with it. It never drops and never renames — a leftover index is dead
    weight, not a correctness problem, and removing one is a deliberate
    operator decision rather than a side effect of a deploy. An index whose
    key pattern matches but whose OPTIONS differ (uniqueness, partial filter,
    collation) is a real conflict the helper raises on, because silently
    adopting it would mean running against an index that does not do what
    this code believes it does.

    Idempotent: re-running against a database already in the target state
    creates nothing and drops nothing.

    voucher_pools ``(pool_id, status, starts_at, ends_at)`` and
    ``(batch_id, status)`` are created in
    ``affiliate_rewards.ensure_affiliate_indexes`` alongside the pre-existing
    uniq_pool_code/pool_status indexes, so all voucher_pools index management
    stays in one place.
    """
    collection = db.affiliate_voucher_batches
    for keys, name in AFFILIATE_VOUCHER_BATCH_INDEXES:
        _ensure_equivalent_index(collection, keys, name=name)


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------

def create_batch(
    db,
    *,
    admin_identity: str,
    batch_name: str,
    pool_id: str,
    starts_at_local: str | None = None,
    ends_at_local: str | None = None,
    timezone_name: str | None = None,
    entitlement_month: str | None = None,
    codes,
    notes=None,
    now_utc: datetime | None = None,
) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    pool_id = str(pool_id or "").strip().upper()
    batch_name = str(batch_name or "").strip()

    logger.info(
        "[AFF_VOUCHER_BATCH][CREATE] admin=%s pool_id=%s batch_name=%s",
        admin_identity, pool_id, batch_name,
    )

    if pool_id not in BATCH_POOL_IDS:
        return _fail(
            "invalid_pool_id",
            f"'{pool_id}' is not a schedulable voucher pool "
            f"(expected one of: {', '.join(BATCH_POOL_IDS)}).",
        )
    if not batch_name:
        return _fail("invalid_batch_name", "Batch name is required.")

    if entitlement_month:
        # Entitlement month is authoritative when supplied: the window is
        # always the exact canonical KL-calendar-month boundary, never an
        # admin-typed start/end (which is how the "00:01"/"23:59"
        # off-by-one-minute schedules — invisible to a human, but a full
        # miss for the claimability helper's exact-containment check —
        # happened in the first place). Any starts_at_local/ends_at_local
        # passed alongside entitlement_month is ignored.
        starts_at_utc, ends_at_utc = canonical_entitlement_month_window(entitlement_month)
        if starts_at_utc is None or ends_at_utc is None:
            return _fail("invalid_entitlement_month", "Entitlement month must be a valid 'YYYYMM' value.")
    else:
        # No entitlement_month supplied — explicit start/end window (used by
        # WELCOME, and by tests/tools that deliberately construct a
        # non-canonical window to exercise the claimability edge cases).
        starts_at_utc = parse_kl_local_to_utc(starts_at_local, timezone_name)
        if starts_at_utc is None:
            return _fail("invalid_start_at", "Start date/time could not be parsed.")
        ends_at_utc = parse_kl_local_to_utc(ends_at_local, timezone_name)
        if ends_at_utc is None:
            return _fail("invalid_end_at", "End date/time could not be parsed.")
        if ends_at_utc <= starts_at_utc:
            return _fail("end_before_start", "End time must be after start time.")

    overlap = _find_overlapping_batch(db, pool_id=pool_id, starts_at_utc=starts_at_utc, ends_at_utc=ends_at_utc)
    if overlap:
        logger.warning(
            "[AFF_VOUCHER_BATCH][OVERLAP_BLOCK] admin=%s pool_id=%s starts_at=%s ends_at=%s conflicting_batch_id=%s",
            admin_identity, pool_id, starts_at_utc.isoformat(), ends_at_utc.isoformat(), overlap.get("_id"),
        )
        return {
            "ok": False,
            "code": "batch_window_overlap",
            "conflicting_batch_id": str(overlap.get("_id")),
            "message": f"This {pool_id} batch overlaps an existing scheduled or active batch.",
        }

    unique_codes, duplicate_in_upload, invalid_count = normalize_voucher_codes(codes)
    submitted = len(unique_codes) + duplicate_in_upload + invalid_count
    if not unique_codes:
        return _fail("no_codes", "No valid voucher codes were provided.")

    batch_doc = {
        "batch_name": batch_name,
        "pool_id": pool_id,
        "starts_at": starts_at_utc,
        "ends_at": ends_at_utc,
        "uploaded_count": len(unique_codes),
        "available_count": 0,
        "issued_count": 0,
        "created_at": now_utc,
        "created_by": admin_identity,
        "notes": notes or None,
        "distribution_disabled": False,
        # Upload lifecycle (Risk 3): a batch is never claimable until it
        # reaches "ready" — a process crash mid-upload leaves it stuck at
        # "staging" (non-claimable, auditable, repairable via reconcile_batch),
        # never silently exposed as Active.
        "upload_status": "staging",
        "submitted_count": submitted,
        "inserted_count": 0,
        "duplicate_count": 0,
        "invalid_count": invalid_count,
        "upload_started_at": now_utc,
        "upload_completed_at": None,
        "upload_failed_at": None,
        "upload_error_code": None,
    }
    batch_id = db.affiliate_voucher_batches.insert_one(batch_doc).inserted_id
    logger.info(
        "[AFF_VOUCHER_BATCH][UPLOAD_START] admin=%s batch_id=%s pool_id=%s submitted=%s",
        admin_identity, batch_id, pool_id, submitted,
    )

    denomination = pool_denomination(pool_id)
    inserted = 0
    duplicate_in_db = 0
    for code in unique_codes:
        row = {
            "pool_id": pool_id,
            "code": code,
            "batch_id": batch_id,
            "batch_name": batch_name,
            "starts_at": starts_at_utc,
            "ends_at": ends_at_utc,
            "status": "available",
            "created_at": now_utc,
            "distribution_disabled": False,
        }
        # Denomination pools carry their value on every physical row, so a
        # code stays independently identifiable (and priceable) no matter
        # which tier's bundle later consumes it. Per-tier legacy pools are
        # left exactly as before: their value is a property of the tier,
        # read from the legacy plan, never from the row.
        if denomination is not None:
            row["voucher_value"] = denomination
        try:
            db.voucher_pools.insert_one(row)
            inserted += 1
        except Exception as exc:
            if _is_duplicate_key_error(exc):
                duplicate_in_db += 1
                continue
            # A genuine write failure (or a process crash resuming here on
            # retry) must never leave a batch that looks claimable. Mark it
            # "failed" with a safe summary and *keep* it — and whatever rows
            # made it in — for audit/reconciliation instead of silently
            # deleting evidence; upload_status != "ready" already keeps
            # every row non-claimable regardless of the schedule window.
            error_code = exc.__class__.__name__
            db.affiliate_voucher_batches.update_one(
                {"_id": batch_id},
                {
                    "$set": {
                        "upload_status": "failed",
                        "available_count": inserted,
                        "uploaded_count": inserted,
                        "inserted_count": inserted,
                        "duplicate_count": duplicate_in_upload + duplicate_in_db,
                        "upload_failed_at": now_utc,
                        "upload_error_code": error_code,
                    }
                },
            )
            logger.error(
                "[AFF_VOUCHER_BATCH][UPLOAD_FAILED] admin=%s pool_id=%s batch_id=%s inserted_so_far=%s reason=insert_error err=%s",
                admin_identity, pool_id, batch_id, inserted, error_code,
            )
            return {
                "ok": False,
                "code": "upload_failed",
                "batch_id": str(batch_id),
                "message": "The upload failed partway through and was marked Failed for review. No codes from this batch can be distributed; use Reconcile/Retry from the dashboard.",
                "submitted": submitted,
                "inserted": inserted,
                "duplicates": duplicate_in_upload + duplicate_in_db,
                "invalid": invalid_count,
            }

    total_duplicates = duplicate_in_upload + duplicate_in_db

    if inserted == 0:
        db.affiliate_voucher_batches.delete_one({"_id": batch_id})
        logger.warning(
            "[AFF_VOUCHER_BATCH][CREATE_FAIL] admin=%s pool_id=%s submitted=%s duplicates=%s invalid=%s reason=zero_inserted",
            admin_identity, pool_id, submitted, total_duplicates, invalid_count,
        )
        return {
            "ok": False,
            "code": "duplicate_codes" if total_duplicates and not invalid_count else "no_codes",
            "message": "No new voucher codes were inserted — all submitted codes were duplicates or invalid.",
            "submitted": submitted,
            "inserted": 0,
            "duplicates": total_duplicates,
            "invalid": invalid_count,
            "total_batch_inventory": 0,
        }

    # Close the race between two concurrent same-tier create requests that
    # both passed the pre-insert overlap check before either had committed:
    # re-check for an overlapping batch now that this one is fully visible.
    # Deterministic tie-break so exactly one side survives — the batch
    # created later (the larger _id) is the one that self-aborts, and the
    # earlier batch's own post-check will simply find nothing (its insert
    # already happened first) and proceed normally.
    post_overlap = _find_overlapping_batch(
        db, pool_id=pool_id, starts_at_utc=starts_at_utc, ends_at_utc=ends_at_utc, exclude_batch_id=batch_id
    )
    if post_overlap and post_overlap.get("_id") < batch_id:
        _bulk_delete_rows(db.voucher_pools, {"batch_id": batch_id, "status": "available"})
        db.affiliate_voucher_batches.delete_one({"_id": batch_id})
        logger.warning(
            "[AFF_VOUCHER_BATCH][OVERLAP_BLOCK] admin=%s pool_id=%s batch_id=%s conflicting_batch_id=%s reason=post_commit_race",
            admin_identity, pool_id, batch_id, post_overlap.get("_id"),
        )
        return {
            "ok": False,
            "code": "batch_window_overlap",
            "conflicting_batch_id": str(post_overlap.get("_id")),
            "message": f"This {pool_id} batch overlaps an existing scheduled or active batch.",
        }

    db.affiliate_voucher_batches.update_one(
        {"_id": batch_id},
        {
            "$set": {
                "available_count": inserted,
                "uploaded_count": inserted,
                "upload_status": "ready",
                "inserted_count": inserted,
                "duplicate_count": total_duplicates,
                "upload_completed_at": now_utc,
            }
        },
    )
    logger.info(
        "[AFF_VOUCHER_BATCH][CREATE_OK] admin=%s batch_id=%s pool_id=%s starts_at=%s ends_at=%s submitted=%s inserted=%s duplicates=%s invalid=%s",
        admin_identity, batch_id, pool_id, starts_at_utc.isoformat(), ends_at_utc.isoformat(),
        submitted, inserted, total_duplicates, invalid_count,
    )
    logger.info(
        "[AFF_VOUCHER_BATCH][UPLOAD_READY] admin=%s batch_id=%s pool_id=%s inserted=%s",
        admin_identity, batch_id, pool_id, inserted,
    )
    batch = db.affiliate_voucher_batches.find_one({"_id": batch_id})
    return {
        "ok": True,
        "batch": _serialize_batch(batch, now_utc=now_utc),
        "counts": {
            "submitted": submitted,
            "inserted": inserted,
            "duplicates": total_duplicates,
            "invalid": invalid_count,
            "total_batch_inventory": inserted,
        },
    }


def add_codes_to_batch(db, batch_id, *, admin_identity: str, codes, now_utc: datetime | None = None) -> dict:
    """Top up an existing batch with additional voucher codes without
    touching its schedule, pool, or previously-inserted rows. Reuses the
    exact same normalize/insert/duplicate-handling path as ``create_batch``
    so this never becomes a parallel voucher-writing implementation.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    oid = _as_object_id(batch_id)
    if oid is None:
        return _fail("batch_not_found", "Batch not found.")
    batch = db.affiliate_voucher_batches.find_one({"_id": oid})
    if not batch:
        return _fail("batch_not_found", "Batch not found.")

    if bool(batch.get("distribution_disabled")):
        return _fail("batch_disabled", "This batch is disabled. Re-enable it before adding codes.")
    upload_status = batch.get("upload_status") or "ready"
    if upload_status != "ready":
        return _fail(
            "batch_not_ready",
            f"This batch is currently '{upload_status}' and cannot accept new codes. Reconcile or wait for the upload to finish first.",
        )
    # A batch whose window has already ended can never distribute again (the
    # claim path rejects it past ends_at), its schedule can't be moved once
    # any voucher was issued, and the unique (pool_id, code) index means
    # freshly-added codes couldn't be reused in a new batch either — so
    # newly inserted codes here would be permanently stranded. Block it
    # before insert rather than after.
    ends_at = _as_aware_utc(batch.get("ends_at"))
    if ends_at and now_utc >= ends_at:
        return _fail("batch_expired", "This batch's schedule window has already ended and can no longer accept new codes.")

    unique_codes, duplicate_in_upload, invalid_count = normalize_voucher_codes(codes)
    submitted = len(unique_codes) + duplicate_in_upload + invalid_count
    if submitted == 0:
        return _fail("no_codes", "No voucher codes were provided.")
    if not unique_codes:
        return _fail("no_codes", "No valid voucher codes were provided.")

    pool_id = batch["pool_id"]
    batch_name = batch["batch_name"]
    starts_at = batch["starts_at"]
    ends_at = batch["ends_at"]

    logger.info(
        "[AFF_VOUCHER_BATCH][ADD_CODES_START] admin=%s batch_id=%s pool_id=%s submitted=%s",
        admin_identity, oid, pool_id, submitted,
    )

    denomination = pool_denomination(pool_id)
    inserted = 0
    duplicate_in_db = 0
    for code in unique_codes:
        row = {
            "pool_id": pool_id,
            "code": code,
            "batch_id": oid,
            "batch_name": batch_name,
            "starts_at": starts_at,
            "ends_at": ends_at,
            "status": "available",
            "created_at": now_utc,
            "distribution_disabled": False,
        }
        if denomination is not None:
            row["voucher_value"] = denomination
        try:
            db.voucher_pools.insert_one(row)
            inserted += 1
        except Exception as exc:
            if _is_duplicate_key_error(exc):
                duplicate_in_db += 1
                continue
            # A genuine write failure partway through the top-up: stop here,
            # keep whatever already-inserted rows exist (never overwritten or
            # rolled back — they're valid, committed vouchers on this same
            # batch), and refresh the batch's cached counts to the
            # authoritative DB state before reporting the failure.
            error_code = exc.__class__.__name__
            live = _hydrate_live_counts(db, batch)
            db.affiliate_voucher_batches.update_one(
                {"_id": oid},
                {
                    "$set": {
                        "available_count": live["available_count"],
                        "issued_count": live["issued_count"],
                        "uploaded_count": live["available_count"] + live["issued_count"],
                    },
                    "$inc": {
                        "submitted_count": submitted,
                        "inserted_count": inserted,
                        "duplicate_count": duplicate_in_upload + duplicate_in_db,
                        "invalid_count": invalid_count,
                    },
                },
            )
            logger.error(
                "[AFF_VOUCHER_BATCH][ADD_CODES_FAILED] admin=%s batch_id=%s pool_id=%s inserted_so_far=%s reason=insert_error err=%s",
                admin_identity, oid, pool_id, inserted, error_code,
            )
            return {
                "ok": False,
                "code": "database_error",
                "message": "A database error occurred while adding codes. Codes already inserted before the failure remain saved; please retry with the remaining codes.",
                "submitted_count": submitted,
                "inserted_count": inserted,
                "duplicate_count": duplicate_in_upload + duplicate_in_db,
                "invalid_count": invalid_count,
            }

    total_duplicates = duplicate_in_upload + duplicate_in_db

    # Authoritative counts, never a blindly-incremented frontend number: the
    # same live-recount used by list/detail/reconcile, re-derived from the
    # actual voucher_pools rows for this batch after the inserts above.
    live = _hydrate_live_counts(db, batch)
    available_count = int(live["available_count"])
    issued_count = int(live["issued_count"])
    uploaded_count = available_count + issued_count

    db.affiliate_voucher_batches.update_one(
        {"_id": oid},
        {
            "$set": {
                "available_count": available_count,
                "issued_count": issued_count,
                "uploaded_count": uploaded_count,
            },
            "$inc": {
                "submitted_count": submitted,
                "inserted_count": inserted,
                "duplicate_count": total_duplicates,
                "invalid_count": invalid_count,
            },
        },
    )

    if inserted == 0:
        logger.warning(
            "[AFF_VOUCHER_BATCH][ADD_CODES_ZERO] admin=%s batch_id=%s pool_id=%s submitted=%s duplicates=%s invalid=%s",
            admin_identity, oid, pool_id, submitted, total_duplicates, invalid_count,
        )
        return {
            "ok": False,
            "code": "duplicate_codes" if total_duplicates and not invalid_count else "no_codes",
            "message": f"No new codes added. All {submitted} submitted codes already exist." if total_duplicates
                       else "No new voucher codes were inserted — all submitted codes were invalid.",
            "submitted_count": submitted,
            "inserted_count": 0,
            "duplicate_count": total_duplicates,
            "invalid_count": invalid_count,
            "available_count": available_count,
            "uploaded_count": uploaded_count,
        }

    logger.info(
        "[AFF_VOUCHER_BATCH][ADD_CODES_OK] admin=%s batch_id=%s pool_id=%s submitted=%s inserted=%s duplicates=%s invalid=%s",
        admin_identity, oid, pool_id, submitted, inserted, total_duplicates, invalid_count,
    )
    updated = db.affiliate_voucher_batches.find_one({"_id": oid})
    return {
        "ok": True,
        "submitted_count": submitted,
        "inserted_count": inserted,
        "duplicate_count": total_duplicates,
        "invalid_count": invalid_count,
        "available_count": available_count,
        "uploaded_count": uploaded_count,
        "batch": _serialize_batch(updated, now_utc=now_utc),
    }


def _pool_entered_scheduled_mode(db, *, pool_id: str, reference_utc: datetime) -> bool:
    """True once the earliest batch ever created for this pool (T1-T4 or
    WELCOME, any status) had already started as of ``reference_utc`` — the
    same permanent, one-way legacy-fallback cutover used by the claim path
    in ``affiliate_rewards.py``, kept here too so the dashboard can show it.
    """
    starts = [
        _as_aware_utc(row.get("starts_at"))
        for row in db.affiliate_voucher_batches.find({"pool_id": pool_id})
    ]
    starts = [s for s in starts if s is not None]
    if not starts:
        return False
    return min(starts) <= reference_utc


def _legacy_fallback_status(db, *, pool_id: str | None, now_utc: datetime) -> list:
    pools = [str(pool_id).strip().upper()] if pool_id else list(BATCH_POOL_IDS)
    out = []
    for pid in pools:
        entered = _pool_entered_scheduled_mode(db, pool_id=pid, reference_utc=now_utc)
        out.append({
            "pool_id": pid,
            "entered_scheduled_mode": entered,
            "legacy_fallback_allowed": not entered,
        })
    return out


def list_batches(db, *, pool_id=None, status=None, month=None, include_expired=False, now_utc: datetime | None = None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    query = {}
    if pool_id:
        query["pool_id"] = str(pool_id).strip().upper()

    entries = []
    for raw_doc in db.affiliate_voucher_batches.find(query):
        doc = _hydrate_live_counts(db, raw_doc)
        derived = derive_batch_status(doc, now_utc)
        if month:
            starts_at = _as_aware_utc(doc.get("starts_at"))
            if not starts_at or starts_at.astimezone(KL_TZ).strftime("%Y-%m") != str(month).strip():
                continue
        if derived == "expired" and not include_expired and status != "expired":
            continue
        if status and str(status).strip().lower() != derived:
            continue
        entries.append((doc, derived))

    entries.sort(key=lambda pair: _sort_key(pair[0], pair[1]))
    items = [_serialize_batch(doc, now_utc=now_utc) for doc, _status in entries]
    return {
        "ok": True,
        "items": items,
        "legacy_summary": _legacy_unbounded_summary(db, pool_id=pool_id),
        "legacy_fallback": _legacy_fallback_status(db, pool_id=pool_id, now_utc=now_utc),
        "server_now_utc": now_utc.isoformat(),
    }


def get_batch_detail(db, batch_id, *, page: int = 1, page_size: int = 50, now_utc: datetime | None = None) -> dict | None:
    now_utc = now_utc or datetime.now(timezone.utc)
    oid = _as_object_id(batch_id)
    if oid is None:
        return None
    batch = db.affiliate_voucher_batches.find_one({"_id": oid})
    if not batch:
        return None
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 50), 200))
    skip = (page - 1) * page_size
    rows = db.voucher_pools.find({"batch_id": oid}, sort=[("_id", 1)], skip=skip, limit=page_size)
    total_rows = db.voucher_pools.count_documents({"batch_id": oid})

    out = _serialize_batch(_hydrate_live_counts(db, batch), now_utc=now_utc)
    out["vouchers"] = [_serialize_voucher_row(r) for r in rows]
    out["vouchers_page"] = page
    out["vouchers_page_size"] = page_size
    out["vouchers_total"] = total_rows
    return out


def update_batch(db, batch_id, *, admin_identity: str, updates: dict, now_utc: datetime | None = None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    oid = _as_object_id(batch_id)
    if oid is None:
        return _fail("batch_not_found", "Batch not found.")
    batch = db.affiliate_voucher_batches.find_one({"_id": oid})
    if not batch:
        return _fail("batch_not_found", "Batch not found.")

    set_fields = {}
    if "batch_name" in updates:
        name = str(updates.get("batch_name") or "").strip()
        if not name:
            return _fail("invalid_batch_name", "Batch name cannot be blank.")
        set_fields["batch_name"] = name
    if "notes" in updates:
        set_fields["notes"] = updates.get("notes")

    wants_date_change = (
        "starts_at_local" in updates or "ends_at_local" in updates or "entitlement_month" in updates
    )
    new_starts_at = _as_aware_utc(batch.get("starts_at"))
    new_ends_at = _as_aware_utc(batch.get("ends_at"))
    if wants_date_change:
        live_issued_count = int(_hydrate_live_counts(db, batch).get("issued_count") or 0)
        if live_issued_count > 0:
            return _fail(
                "active_batch_edit_restricted",
                "This batch already has issued vouchers; its schedule can no longer be changed.",
            )
        if updates.get("entitlement_month"):
            # Same authoritative-source-of-truth rule as create_batch: when
            # an entitlement month is given, it always wins over any
            # starts_at_local/ends_at_local passed alongside it — this is
            # the safe corrective path for existing batches whose window
            # was hand-typed (e.g. "00:01"/"23:59") instead of matching the
            # canonical KL calendar month.
            new_starts_at, new_ends_at = canonical_entitlement_month_window(updates.get("entitlement_month"))
            if new_starts_at is None or new_ends_at is None:
                return _fail("invalid_entitlement_month", "Entitlement month must be a valid 'YYYYMM' value.")
        else:
            tz_name = updates.get("timezone") or "Asia/Kuala_Lumpur"
            if "starts_at_local" in updates:
                new_starts_at = parse_kl_local_to_utc(updates.get("starts_at_local"), tz_name)
                if new_starts_at is None:
                    return _fail("invalid_start_at", "Start date/time could not be parsed.")
            if "ends_at_local" in updates:
                new_ends_at = parse_kl_local_to_utc(updates.get("ends_at_local"), tz_name)
                if new_ends_at is None:
                    return _fail("invalid_end_at", "End date/time could not be parsed.")
            if new_ends_at <= new_starts_at:
                return _fail("end_before_start", "End time must be after start time.")
        overlap = _find_overlapping_batch(
            db, pool_id=batch["pool_id"], starts_at_utc=new_starts_at, ends_at_utc=new_ends_at, exclude_batch_id=oid
        )
        if overlap:
            logger.warning(
                "[AFF_VOUCHER_BATCH][OVERLAP_BLOCK] admin=%s pool_id=%s batch_id=%s conflicting_batch_id=%s",
                admin_identity, batch["pool_id"], oid, overlap.get("_id"),
            )
            return {
                "ok": False,
                "code": "batch_window_overlap",
                "conflicting_batch_id": str(overlap.get("_id")),
                "message": f"This {batch['pool_id']} batch overlaps an existing scheduled or active batch.",
            }
        set_fields["starts_at"] = new_starts_at
        set_fields["ends_at"] = new_ends_at

    if not set_fields:
        return {"ok": True, "batch": _serialize_batch(_hydrate_live_counts(db, batch), now_utc=now_utc)}

    db.affiliate_voucher_batches.update_one({"_id": oid}, {"$set": set_fields})

    row_set = {}
    if "starts_at" in set_fields:
        row_set["starts_at"] = set_fields["starts_at"]
    if "ends_at" in set_fields:
        row_set["ends_at"] = set_fields["ends_at"]
    if "batch_name" in set_fields:
        row_set["batch_name"] = set_fields["batch_name"]
    if row_set:
        _bulk_update_rows(db.voucher_pools, {"batch_id": oid}, {"$set": row_set})

    logger.info(
        "[AFF_VOUCHER_BATCH][UPDATE_OK] admin=%s batch_id=%s fields=%s",
        admin_identity, oid, sorted(set_fields.keys()),
    )
    updated = db.affiliate_voucher_batches.find_one({"_id": oid})
    return {"ok": True, "batch": _serialize_batch(_hydrate_live_counts(db, updated), now_utc=now_utc)}


def set_batch_distribution_disabled(db, batch_id, *, admin_identity: str, disabled: bool, now_utc: datetime | None = None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    oid = _as_object_id(batch_id)
    if oid is None:
        return _fail("batch_not_found", "Batch not found.")
    batch = db.affiliate_voucher_batches.find_one({"_id": oid})
    if not batch:
        return _fail("batch_not_found", "Batch not found.")

    if not disabled and batch.get("upload_status") == "failed":
        return _fail(
            "target_batch_failed_cannot_enable",
            "This batch failed to upload and cannot be re-enabled. Use Reconcile or re-upload instead.",
        )

    db.affiliate_voucher_batches.update_one(
        {"_id": oid}, {"$set": {"distribution_disabled": bool(disabled), "updated_at": now_utc}}
    )
    _bulk_update_rows(
        db.voucher_pools,
        {"batch_id": oid, "status": "available"},
        {"$set": {"distribution_disabled": bool(disabled)}},
    )
    logger.info(
        "[AFF_VOUCHER_BATCH][%s] admin=%s batch_id=%s pool_id=%s",
        "DISABLE" if disabled else "ENABLE", admin_identity, oid, batch.get("pool_id"),
    )
    updated = db.affiliate_voucher_batches.find_one({"_id": oid})
    return {"ok": True, "batch": _serialize_batch(_hydrate_live_counts(db, updated), now_utc=now_utc)}


def reconcile_batch(db, batch_id, *, admin_identity: str | None = None, now_utc: datetime | None = None) -> dict:
    """Recount ``voucher_pools`` rows for this batch and repair its upload
    lifecycle:
      - recomputes available/issued/uploaded counts from the actual rows
        (the authoritative source — never trusted from the cached fields)
      - a ``staging`` batch with any rows found becomes ``ready`` (the
        crash-after-partial-insert recovery case)
      - a ``staging`` batch with zero rows becomes ``failed`` (nothing to
        distribute, not recoverable)
      - ``ready``/``failed``/``disabled`` batches just get their counts
        refreshed — this is always safe to call, including repeatedly.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    oid = _as_object_id(batch_id)
    if oid is None:
        return _fail("batch_not_found", "Batch not found.")
    batch = db.affiliate_voucher_batches.find_one({"_id": oid})
    if not batch:
        return _fail("batch_not_found", "Batch not found.")

    available = int(db.voucher_pools.count_documents({"batch_id": oid, "status": "available"}))
    issued = int(db.voucher_pools.count_documents({"batch_id": oid, "status": "issued"}))
    total = available + issued

    update = {"available_count": available, "issued_count": issued, "uploaded_count": total}
    new_status = batch.get("upload_status")
    if new_status == "staging":
        if total > 0:
            new_status = "ready"
            update["upload_status"] = "ready"
            update["upload_completed_at"] = now_utc
        else:
            new_status = "failed"
            update["upload_status"] = "failed"
            update["upload_failed_at"] = now_utc
            update["upload_error_code"] = batch.get("upload_error_code") or "no_rows_found_on_reconcile"

    db.affiliate_voucher_batches.update_one({"_id": oid}, {"$set": update})
    logger.info(
        "[AFF_VOUCHER_BATCH][RECONCILE] admin=%s batch_id=%s pool_id=%s available=%s issued=%s upload_status=%s",
        admin_identity, oid, batch.get("pool_id"), available, issued, new_status,
    )
    updated = db.affiliate_voucher_batches.find_one({"_id": oid})
    return {"ok": True, "batch": _serialize_batch(updated, now_utc=now_utc)}


# ---------------------------------------------------------------------------
# Historical pinned-batch replenishment (admin-only, per ledger)
# ---------------------------------------------------------------------------
#
# ``add_codes_to_batch`` refuses an ended batch, and the allocator never lets
# a later month's batch satisfy an earlier entitlement. A September
# denomination ledger whose pinned September batch ran dry is therefore stuck
# until codes land in THAT batch. This is the one narrow way to put them
# there: driven by a ledger, never by a batch id, so it can only top up the
# exact batch that ledger is already pinned to (``pool_targets.<POOL>``), only
# for a denomination it is actually short of, and only by that shortfall.
#
# It never issues anything, never changes a ledger, and never moves a batch's
# window. Issuance stays with the normal Approve -> allocator path, which is
# already allowed to continue a pinned allocation past ``ends_at``.

HISTORICAL_REPLENISH_SOURCE = "admin_historical_replenish"
HISTORICAL_REPLENISH_STATUSES = ("PENDING_MANUAL", "OUT_OF_STOCK")
HISTORICAL_REPLENISH_AUDIT_COLLECTION = "affiliate_historical_replenish_audit"
HISTORICAL_REPLENISH_LOCK_COLLECTION = "affiliate_historical_replenish_locks"
_HISTORICAL_REPLENISH_LOCK_TTL_SECONDS = 120


def _replenish_fail(reason: str, message: str, **extra) -> dict:
    out = {"status": "error", "reason": reason, "message": message}
    out.update(extra)
    return out


def _month_label(entitlement_month: str) -> str:
    try:
        return datetime.strptime(str(entitlement_month), "%Y%m").strftime("%B %Y")
    except ValueError:
        return str(entitlement_month)


def _historical_ledger_gate(db, ledger_id, *, now_utc: datetime):
    """Ledger-level checks shared by the read-only context and the write.
    Returns ``(failure_or_None, ledger, recipe, entitlement_month, state)``.
    """
    # Lazy: these are private allocator helpers, imported here so the
    # module-level import surface stays exactly what it was.
    from affiliate_rewards import (
        _classify_issued_pool_rows,
        _ledger_entitlement_month,
        _ledger_has_affiliate_bundle,
        _ledger_recipe,
        _ledger_uses_denomination_plan,
    )

    ledger = db.affiliate_ledger.find_one({"_id": ledger_id}) if ledger_id is not None else None
    if not ledger:
        return _replenish_fail("ledger_not_found", "Ledger not found."), None, None, None, None
    if str(ledger.get("ledger_type") or "").strip().upper() != "AFFILIATE_MONTHLY":
        return _replenish_fail(
            "unsupported_ledger_type",
            "Only monthly affiliate tier entitlements can use historical batch replenishment.",
            ledger_type=ledger.get("ledger_type"),
        ), ledger, None, None, None
    status = str(ledger.get("status") or "")
    if status == "ISSUED" or ledger.get("voucher_code") or _ledger_has_affiliate_bundle(ledger):
        return _replenish_fail("already_issued", "This entitlement is already issued.", ledger_status=status), ledger, None, None, None
    if status == "REJECTED":
        return _replenish_fail("rejected", "This entitlement was rejected.", ledger_status=status), ledger, None, None, None
    if status not in HISTORICAL_REPLENISH_STATUSES:
        return _replenish_fail(
            "invalid_status",
            f"Replenishment is only allowed for {' / '.join(HISTORICAL_REPLENISH_STATUSES)} ledgers.",
            ledger_status=status,
        ), ledger, None, None, None
    if not _ledger_uses_denomination_plan(ledger):
        return _replenish_fail(
            "not_denomination_plan",
            "Only denomination-plan (Sep 2026 onward) entitlements use pinned denomination batches.",
        ), ledger, None, None, None
    entitlement_month = _ledger_entitlement_month(ledger)
    _, month_end = canonical_entitlement_month_window(entitlement_month)
    if month_end is None or now_utc < month_end:
        return _replenish_fail(
            "ledger_not_historical",
            "This entitlement month has not ended yet. Use the normal batch Add Codes flow for current stock.",
            entitlement_month=entitlement_month,
        ), ledger, None, entitlement_month, None
    recipe = _ledger_recipe(ledger)
    state = _classify_issued_pool_rows(db, ledger, recipe=recipe)
    if state["foreign"] or state["surplus"]:
        # Replenishing cannot fix a corrupt linkage; it needs a human.
        return _replenish_fail(
            "historical_replenish_not_allowed",
            "Codes linked to this ledger do not match its recipe (foreign or surplus rows). Resolve that first.",
        ), ledger, recipe, entitlement_month, state
    if not state["missing"]:
        return _replenish_fail(
            "no_shortage",
            "Every denomination of this bundle is already allocated. Retry Approve to finalize.",
        ), ledger, recipe, entitlement_month, state
    return None, ledger, recipe, entitlement_month, state


def _historical_pool_gate(db, *, ledger, state, entitlement_month, pool_id: str, now_utc: datetime,
                          client_batch_id=None):
    """Per-denomination checks. The target batch is ALWAYS the ledger's own
    ``pool_targets.<pool_id>.batch_id``; a client-supplied id is only ever
    compared against it, never used to look anything up.
    Returns ``(failure_or_None, batch, replenishable_quantity, available)``.
    """
    if pool_id not in (state.get("required") or {}):
        return _replenish_fail(
            "invalid_denomination",
            "This denomination is not part of the ledger's reward bundle.",
            pool_id=pool_id,
        ), None, 0, 0
    missing = int((state.get("missing") or {}).get(pool_id) or 0)
    if missing <= 0:
        return _replenish_fail(
            "denomination_not_short",
            "This denomination is already fully allocated for this ledger.",
            pool_id=pool_id,
        ), None, 0, 0
    target = (ledger.get("pool_targets") or {}).get(pool_id) or {}
    pinned_batch_id = target.get("batch_id")
    if target.get("mode") != "batch" or pinned_batch_id is None:
        return _replenish_fail(
            "missing_pinned_batch",
            "This denomination was never pinned to a batch for this entitlement; there is no historical batch to replenish.",
            pool_id=pool_id,
        ), None, 0, 0
    if client_batch_id not in (None, "") and str(client_batch_id) != str(pinned_batch_id):
        return _replenish_fail(
            "batch_not_pinned_to_ledger",
            "The supplied batch is not the batch this ledger is pinned to.",
            pool_id=pool_id,
        ), None, 0, 0
    batch = db.affiliate_voucher_batches.find_one({"_id": pinned_batch_id})
    if not batch:
        return _replenish_fail("batch_not_found", "The pinned batch no longer exists.", pool_id=pool_id), None, 0, 0
    if str(batch.get("pool_id") or "").strip().upper() != pool_id:
        return _replenish_fail(
            "batch_not_pinned_to_ledger",
            "The pinned batch belongs to a different pool.",
            pool_id=pool_id,
        ), None, 0, 0
    # Same full-containment rule the allocator pinned with, plus the batch
    # must START in the entitlement month — so a batch belonging to any
    # other month can never be topped up through this path.
    month_start, month_end = canonical_entitlement_month_window(entitlement_month)
    starts_at = _as_aware_utc(batch.get("starts_at"))
    ends_at = _as_aware_utc(batch.get("ends_at"))
    if (
        starts_at is None or ends_at is None
        or not (starts_at <= month_start and ends_at >= month_end)
        or _entitlement_month_for_batch(batch) != entitlement_month
    ):
        return _replenish_fail(
            "batch_month_mismatch",
            "The pinned batch does not correspond to this ledger's entitlement month.",
            pool_id=pool_id,
        ), None, 0, 0
    if now_utc < ends_at:
        return _replenish_fail(
            "batch_not_historical",
            "The pinned batch is still within its window. Use the normal batch Add Codes flow.",
            pool_id=pool_id,
        ), None, 0, 0
    if (batch.get("upload_status") or "ready") != "ready":
        return _replenish_fail("batch_not_ready", "The pinned batch is not in a ready state.", pool_id=pool_id), None, 0, 0
    if bool(batch.get("distribution_disabled")):
        return _replenish_fail("batch_disabled", "The pinned batch is disabled.", pool_id=pool_id), None, 0, 0
    available = int(db.voucher_pools.count_documents(
        {"batch_id": batch["_id"], "pool_id": pool_id, "status": "available"}
    ))
    replenishable = missing - available
    if replenishable <= 0:
        return _replenish_fail(
            "stock_already_sufficient",
            f"The pinned batch already holds {available} available code(s) for this denomination. Retry Approve first.",
            pool_id=pool_id,
            available_in_batch=available,
        ), batch, 0, available
    return None, batch, replenishable, available


def historical_replenish_context(db, ledger_id, *, now_utc: datetime | None = None) -> dict:
    """Read-only view for the admin modal: the ledger, its pinned batch per
    denomination, what is missing, and whether each denomination can be
    replenished right now (with the exact refusal reason if not)."""
    now_utc = now_utc or datetime.now(timezone.utc)
    failure, ledger, recipe, entitlement_month, state = _historical_ledger_gate(db, ledger_id, now_utc=now_utc)
    if failure and state is None:
        out = dict(failure, eligible=False, pools=[])
        if ledger:
            out.update({
                "ledger_id": str(ledger.get("_id")),
                "user_id": ledger.get("user_id"),
                "tier": ledger.get("tier"),
                "ledger_status": ledger.get("status"),
                "entitlement_month": entitlement_month or ledger.get("entitlement_month") or ledger.get("year_month"),
                "shortage_reasons": ledger.get("shortage_reasons") or {},
            })
        return out
    pools = []
    for pool_id in sorted((state or {}).get("required") or {}):
        target = (ledger.get("pool_targets") or {}).get(pool_id) or {}
        missing = int((state.get("missing") or {}).get(pool_id) or 0)
        entry = {
            "pool_id": pool_id,
            "denomination": pool_denomination(pool_id),
            "required": int(state["required"][pool_id]),
            "allocated": len(state["allocated"].get(pool_id) or []),
            "missing": missing,
            "pinned_batch_id": str(target["batch_id"]) if target.get("batch_id") is not None else None,
            "replenishable": 0,
            "available_in_batch": None,
            "blocked_reason": None,
        }
        if failure is None and missing > 0:
            pool_failure, batch, replenishable, available = _historical_pool_gate(
                db, ledger=ledger, state=state, entitlement_month=entitlement_month, pool_id=pool_id, now_utc=now_utc,
            )
            entry["replenishable"] = replenishable
            entry["available_in_batch"] = available if batch is not None else None
            if batch is not None:
                entry["batch_name"] = batch.get("batch_name")
                entry["batch_ends_at_kl"] = _to_kl_iso(batch.get("ends_at"))
            if pool_failure:
                entry["blocked_reason"] = pool_failure["reason"]
        elif failure is not None:
            entry["blocked_reason"] = failure["reason"]
        pools.append(entry)
    return {
        "status": "ok" if failure is None else "error",
        "reason": None if failure is None else failure["reason"],
        "message": None if failure is None else failure["message"],
        "eligible": failure is None and any(p["replenishable"] > 0 for p in pools),
        "ledger_id": str(ledger.get("_id")),
        "user_id": ledger.get("user_id"),
        "tier": ledger.get("tier"),
        "ledger_status": ledger.get("status"),
        "entitlement_month": entitlement_month,
        "shortage_reasons": ledger.get("shortage_reasons") or {},
        "missing_by_denomination": dict((state or {}).get("missing") or {}),
        "pools": pools,
    }


def _resolve_denomination_pool_id(*, pool_id=None, denomination=None) -> str | None:
    if pool_id not in (None, ""):
        key = str(pool_id).strip().upper()
        return key if pool_denomination(key) is not None else None
    try:
        value = int(str(denomination).strip())
    except (TypeError, ValueError):
        return None
    for key in ADMIN_AFFILIATE_POOL_IDS:
        if pool_denomination(key) == value:
            return key
    return None


def _acquire_replenish_lock(db, *, key: str, holder: str) -> bool:
    """One replenishment per batch at a time, so two admins cannot both pass
    the shortfall check and overfill an expired batch. A crashed holder's
    lock is reclaimable after the TTL."""
    locks = db[HISTORICAL_REPLENISH_LOCK_COLLECTION]
    wall = datetime.now(timezone.utc)
    cutoff = datetime.fromtimestamp(wall.timestamp() - _HISTORICAL_REPLENISH_LOCK_TTL_SECONDS, tz=timezone.utc)
    locks.delete_one({"_id": key, "locked_at": {"$lt": cutoff}})
    try:
        locks.insert_one({"_id": key, "holder": holder, "locked_at": wall})
    except Exception as exc:
        if _is_duplicate_key_error(exc):
            return False
        raise
    return True


def _release_replenish_lock(db, *, key: str, holder: str):
    try:
        db[HISTORICAL_REPLENISH_LOCK_COLLECTION].delete_one({"_id": key, "holder": holder})
    except Exception:
        logger.exception("[AFF_HIST_REPLENISH][LOCK_RELEASE_FAILED] key=%s", key)


def replenish_historical_pinned_batch(
    db,
    ledger_id,
    *,
    admin_identity: str,
    codes,
    pool_id=None,
    denomination=None,
    batch_id=None,
    now_utc: datetime | None = None,
) -> dict:
    """Insert new physical codes into the exact expired batch a denomination
    ledger is already pinned to, for ONE denomination it is short of, up to
    that shortfall. Never issues, never touches the ledger, never edits the
    batch's identity or window. Retry Approve afterwards to issue.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    failure, ledger, recipe, entitlement_month, state = _historical_ledger_gate(db, ledger_id, now_utc=now_utc)
    if failure:
        return failure

    target_pool = _resolve_denomination_pool_id(pool_id=pool_id, denomination=denomination)
    if target_pool is None:
        return _replenish_fail("invalid_denomination", "A valid denomination ($5 / $10 / $50) is required.")

    unique_codes, duplicate_in_upload, invalid_count = normalize_voucher_codes(codes)
    if invalid_count:
        return _replenish_fail("invalid_code", f"{invalid_count} submitted code(s) contain whitespace. Nothing was inserted.")
    if not unique_codes:
        return _replenish_fail("empty_codes", "No voucher codes were provided.")

    # Server-derived target only (see _historical_pool_gate). Resolved once
    # here so the lock key is the pinned batch, then re-checked under lock.
    pre_failure, pre_batch, _, _ = _historical_pool_gate(
        db, ledger=ledger, state=state, entitlement_month=entitlement_month, pool_id=target_pool,
        now_utc=now_utc, client_batch_id=batch_id,
    )
    if pre_batch is None:
        return pre_failure

    lock_key = str(pre_batch["_id"])
    holder = str(ObjectId())
    if not _acquire_replenish_lock(db, key=lock_key, holder=holder):
        return _replenish_fail(
            "replenish_in_progress",
            "Another historical replenishment of this batch is in progress. Try again shortly.",
        )
    try:
        # Everything re-read under the lock: the ledger may have been
        # approved/issued, and the batch may have gained stock, meanwhile.
        failure, ledger, recipe, entitlement_month, state = _historical_ledger_gate(db, ledger_id, now_utc=now_utc)
        if failure:
            return failure
        failure, batch, replenishable, available = _historical_pool_gate(
            db, ledger=ledger, state=state, entitlement_month=entitlement_month, pool_id=target_pool,
            now_utc=now_utc, client_batch_id=batch_id,
        )
        if failure:
            return failure

        # The same physical code anywhere in the affiliate pools (any pool,
        # any status) is refused — the unique (pool_id, code) index alone
        # would only catch it within this one pool.
        existing = list(db.voucher_pools.find(
            {"pool_id": {"$in": list(ADMIN_AFFILIATE_POOL_IDS)}, "code": {"$in": unique_codes}},
            projection={"code": 1, "status": 1},
        ))
        issued_codes = {r.get("code") for r in existing if r.get("status") == "issued"}
        existing_codes = {r.get("code") for r in existing}
        new_codes = [c for c in unique_codes if c not in existing_codes]
        already_issued = len(issued_codes)
        duplicates = duplicate_in_upload + len(existing_codes - issued_codes)

        if not new_codes:
            reason = "code_already_issued" if already_issued else "duplicate_code"
            return _replenish_fail(
                reason,
                "No new codes: every submitted code already exists in the affiliate voucher pools.",
                inserted=0, duplicates=duplicates, already_issued=already_issued,
            )
        if len(new_codes) > replenishable:
            return _replenish_fail(
                "quantity_exceeds_shortage",
                f"This ledger is short {replenishable} code(s) for this denomination; {len(new_codes)} new code(s) "
                "were submitted. Nothing was inserted.",
                replenishable=replenishable,
            )

        oid = batch["_id"]
        denomination_value = pool_denomination(target_pool)
        audit_id = ObjectId()
        audit = db[HISTORICAL_REPLENISH_AUDIT_COLLECTION]
        audit.insert_one({
            "_id": audit_id,
            "source": HISTORICAL_REPLENISH_SOURCE,
            "state": "in_progress",
            "ledger_id": ledger["_id"],
            "user_id": ledger.get("user_id"),
            "tier": ledger.get("tier"),
            "entitlement_month": entitlement_month,
            "batch_id": oid,
            "pool_id": target_pool,
            "denomination": denomination_value,
            "admin_identity": admin_identity,
            "created_at": now_utc,
            "submitted": len(unique_codes) + duplicate_in_upload,
            "missing_before": int(state["missing"].get(target_pool) or 0),
            "available_before": available,
        })

        inserted = 0
        inserted_masked = []
        raced = 0
        db_error = None
        for code in new_codes:
            # Same row shape add_codes_to_batch writes, plus provenance.
            row = {
                "pool_id": target_pool,
                "code": code,
                "batch_id": oid,
                "batch_name": batch.get("batch_name"),
                "starts_at": batch.get("starts_at"),
                "ends_at": batch.get("ends_at"),
                "status": "available",
                "created_at": now_utc,
                "distribution_disabled": False,
                "upload_source": HISTORICAL_REPLENISH_SOURCE,
                "historical_replenish_audit_id": audit_id,
            }
            if denomination_value is not None:
                row["voucher_value"] = denomination_value
            try:
                db.voucher_pools.insert_one(row)
                inserted += 1
                inserted_masked.append(_mask_code(code))
            except Exception as exc:
                if _is_duplicate_key_error(exc):
                    raced += 1  # inserted elsewhere since the pre-check
                    continue
                db_error = exc.__class__.__name__
                break
        duplicates += raced

        live = _hydrate_live_counts(db, batch)
        db.affiliate_voucher_batches.update_one(
            {"_id": oid},
            {
                "$set": {
                    "available_count": int(live["available_count"]),
                    "issued_count": int(live["issued_count"]),
                    "uploaded_count": int(live["available_count"]) + int(live["issued_count"]),
                    "last_historical_replenish_at": now_utc,
                },
                "$inc": {
                    "submitted_count": len(unique_codes) + duplicate_in_upload,
                    "inserted_count": inserted,
                    "duplicate_count": duplicates + already_issued,
                },
            },
        )
        denominations_added = {str(denomination_value): inserted} if inserted else {}
        audit.update_one(
            {"_id": audit_id},
            {"$set": {
                "state": "failed" if db_error else "completed",
                "error_code": db_error,
                "inserted": inserted,
                "duplicates": duplicates,
                "already_issued": already_issued,
                "denominations_added": denominations_added,
                "inserted_codes_masked": inserted_masked,
                "completed_at": now_utc,
            }},
        )
        logger.info(
            "[AFF_HIST_REPLENISH][%s] source=%s admin=%s ledger_id=%s user_id=%s entitlement_month=%s "
            "batch_id=%s pool_id=%s inserted=%s duplicates=%s already_issued=%s audit_id=%s",
            "FAILED" if db_error else "OK", HISTORICAL_REPLENISH_SOURCE, admin_identity, ledger["_id"],
            ledger.get("user_id"), entitlement_month, oid, target_pool, inserted, duplicates, already_issued, audit_id,
        )
        if db_error:
            return _replenish_fail(
                "database_error",
                "A database error stopped the upload partway. Codes inserted before it remain in the batch.",
                inserted=inserted, duplicates=duplicates, audit_id=str(audit_id),
            )
        if not inserted:
            return _replenish_fail(
                "duplicate_code",
                "No new codes: every submitted code was inserted concurrently by another request.",
                inserted=0, duplicates=duplicates, already_issued=already_issued,
            )
        return {
            "status": "ok",
            "ledger_id": str(ledger["_id"]),
            "batch_id": str(oid),
            "entitlement_month": entitlement_month,
            "pool_id": target_pool,
            "inserted": inserted,
            "duplicates": duplicates,
            "already_issued": already_issued,
            "denominations_added": denominations_added,
            "remaining_shortfall": max(0, replenishable - inserted),
            "audit_id": str(audit_id),
            "message": f"Historical {_month_label(entitlement_month)} batch replenished. Retry Approve to complete issuance.",
        }
    finally:
        _release_replenish_lock(db, key=lock_key, holder=holder)


# ---------------------------------------------------------------------------
# Admin API
# ---------------------------------------------------------------------------

def _reject_missing_entitlement_month(pool_id: str, entitlement_month) -> dict | None:
    """HTTP-layer guard: an entitlement-month pool (T1-T5 and the
    denomination pools) must always be scheduled from a valid
    ``entitlement_month``, never from client-supplied starts_at_local/
    ends_at_local -- an admin-typed "2026-09-01 00:01" .. "2026-09-30
    23:59" LOOKS like September but is not the canonical boundary
    ``_find_batches_for_period``'s exact-containment check requires, and a
    direct API client could send anything. WELCOME has no
    entitlement-month concept and is exempt.

    Deliberately enforced only at this HTTP boundary, not inside
    ``create_batch``/``update_batch`` themselves: those lower-level
    functions are also used by internal maintenance tooling (see
    ``scripts/fix_affiliate_batch_month_boundaries.py``) and by the test
    suite's own edge-case fixtures, which legitimately construct
    non-canonical windows to exercise the claimability logic that must
    defend against exactly this kind of misalignment. A real admin or
    external API client only ever reaches this through the routes below.
    """
    if pool_id in ENTITLEMENT_MONTH_POOL_IDS and not entitlement_month:
        return _fail(
            "entitlement_month_required",
            f"'{pool_id}' requires a valid entitlement_month ('YYYYMM'); "
            "free-form start/end dates are not accepted for this pool.",
        )
    return None


def _status_response(result: dict):
    if result.get("ok"):
        return jsonify(result), 200
    code = str(result.get("code") or "")
    if code == "batch_not_found":
        status_code = 404
    elif code in ("batch_window_overlap", "batch_disabled", "batch_not_ready", "batch_expired"):
        status_code = 409
    elif code == "database_error":
        status_code = 500
    else:
        status_code = 400
    return jsonify(result), status_code


def register_routes(require_admin_from_query, admin_identity_fn, db_ref):
    """Build a fresh Blueprint wired against this app's auth/db and return
    it for the caller to register. A brand new Blueprint per call (rather
    than decorating one shared module-level instance) is what lets tests
    build multiple independent Flask apps against this module without
    Flask's "blueprint already registered" guard tripping.
    """
    affiliate_voucher_batches_bp = Blueprint("affiliate_voucher_batches", __name__)

    @affiliate_voucher_batches_bp.get("/api/admin/affiliate-voucher-batches")
    def api_list_affiliate_voucher_batches():
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"ok": False, "code": "unauthorized", "message": msg}), code
        result = list_batches(
            db_ref(),
            pool_id=request.args.get("pool_id"),
            status=request.args.get("status"),
            month=request.args.get("month"),
            include_expired=str(request.args.get("include_expired") or "").strip().lower() in ("1", "true", "yes"),
        )
        return jsonify(result), 200

    @affiliate_voucher_batches_bp.post("/api/admin/affiliate-voucher-batches")
    def api_create_affiliate_voucher_batch():
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"ok": False, "code": "unauthorized", "message": msg}), code
        data = request.get_json(silent=True) or {}
        pool_id = str(data.get("pool_id") or "").strip().upper()
        rejection = _reject_missing_entitlement_month(pool_id, data.get("entitlement_month"))
        if rejection:
            return _status_response(rejection)
        result = create_batch(
            db_ref(),
            admin_identity=admin_identity_fn(),
            batch_name=data.get("batch_name"),
            pool_id=data.get("pool_id"),
            starts_at_local=data.get("starts_at_local"),
            ends_at_local=data.get("ends_at_local"),
            timezone_name=data.get("timezone"),
            entitlement_month=data.get("entitlement_month"),
            codes=data.get("codes"),
            notes=data.get("notes"),
        )
        return _status_response(result)

    @affiliate_voucher_batches_bp.get("/api/admin/affiliate-voucher-batches/<batch_id>")
    def api_get_affiliate_voucher_batch(batch_id):
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"ok": False, "code": "unauthorized", "message": msg}), code
        detail = get_batch_detail(
            db_ref(),
            batch_id,
            page=request.args.get("page", default=1, type=int),
            page_size=request.args.get("page_size", default=50, type=int),
        )
        if detail is None:
            return jsonify(_fail("batch_not_found", "Batch not found.")), 404
        return jsonify({"ok": True, "batch": detail}), 200

    @affiliate_voucher_batches_bp.patch("/api/admin/affiliate-voucher-batches/<batch_id>")
    def api_update_affiliate_voucher_batch(batch_id):
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"ok": False, "code": "unauthorized", "message": msg}), code
        data = request.get_json(silent=True) or {}
        if "distribution_disabled" in data:
            result = set_batch_distribution_disabled(
                db_ref(), batch_id, admin_identity=admin_identity_fn(), disabled=bool(data.get("distribution_disabled"))
            )
            return _status_response(result)
        wants_date_change = (
            "starts_at_local" in data or "ends_at_local" in data or "entitlement_month" in data
        )
        if wants_date_change and not data.get("entitlement_month"):
            # Same HTTP-boundary guard as create: an entitlement-month
            # pool's schedule must never be movable to a free-form window
            # via a direct API client, even on an existing batch -- that
            # would silently convert a canonical batch into an arbitrary
            # one. Look the batch's own pool_id up rather than trusting
            # anything client-supplied.
            oid = _as_object_id(batch_id)
            existing_batch = db_ref().affiliate_voucher_batches.find_one({"_id": oid}) if oid is not None else None
            if existing_batch:
                rejection = _reject_missing_entitlement_month(existing_batch.get("pool_id"), None)
                if rejection:
                    return _status_response(rejection)
        result = update_batch(db_ref(), batch_id, admin_identity=admin_identity_fn(), updates=data)
        return _status_response(result)

    @affiliate_voucher_batches_bp.post("/api/admin/affiliate-voucher-batches/<batch_id>/add-codes")
    def api_add_codes_to_affiliate_voucher_batch(batch_id):
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"ok": False, "code": "unauthorized", "message": msg}), code
        data = request.get_json(silent=True) or {}
        result = add_codes_to_batch(
            db_ref(), batch_id, admin_identity=admin_identity_fn(), codes=data.get("codes")
        )
        return _status_response(result)

    @affiliate_voucher_batches_bp.post("/api/admin/affiliate-voucher-batches/<batch_id>/reconcile")
    def api_reconcile_affiliate_voucher_batch(batch_id):
        ok, err = require_admin_from_query()
        if not ok:
            msg, code = err
            return jsonify({"ok": False, "code": "unauthorized", "message": msg}), code
        result = reconcile_batch(db_ref(), batch_id, admin_identity=admin_identity_fn())
        return _status_response(result)

    return affiliate_voucher_batches_bp
