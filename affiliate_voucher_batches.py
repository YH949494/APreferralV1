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
from affiliate_reward_plans import DENOMINATION_POOL_IDS, pool_denomination

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

# Schedulable pools (matches the Admin Dashboard pool dropdown):
#   - T1-T5: the legacy per-tier pools, used by every entitlement month
#     through 202608 (see affiliate_reward_plans.LEGACY_PLAN_ID). T5 was
#     previously excluded here, which left T5 the only tier unable to take a
#     scheduled batch at all; it is included now on the same terms as T1-T4.
#   - AFFILIATE_5 / AFFILIATE_10 / AFFILIATE_50: the standardized
#     denomination pools introduced for entitlement month 202609 onward.
#     One pool serves every tier whose recipe draws that denomination, so
#     the value of a code is a property of the POOL, stamped onto each row
#     as ``voucher_value`` at upload time.
#   - WELCOME: unchanged, free-form scheduling, no entitlement month.
BATCH_POOL_IDS = ("T1", "T2", "T3", "T4", "T5") + DENOMINATION_POOL_IDS + ("WELCOME",)

# Affiliate monthly-entitlement tiers: their claimability
# (``affiliate_rewards._resolve_monthly_ledger_target`` /
# ``get_claimable_pool_inventory``) requires a batch window that *fully
# contains* a KL calendar month, so their schedule must always be exactly
# that canonical month window — never an admin-typed approximation (e.g.
# "00:01"/"23:59"). WELCOME has no monthly-entitlement concept and keeps its
# existing free-form start/end scheduling untouched.
ENTITLEMENT_MONTH_POOL_IDS = ("T1", "T2", "T3", "T4", "T5") + DENOMINATION_POOL_IDS

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

def ensure_affiliate_voucher_batch_indexes(db):
    db.affiliate_voucher_batches.create_index(
        [("pool_id", 1), ("starts_at", 1), ("ends_at", 1)], name="batch_pool_window"
    )
    db.affiliate_voucher_batches.create_index([("ends_at", 1)], name="batch_ends_at")
    db.affiliate_voucher_batches.create_index(
        [("distribution_disabled", 1)], name="batch_distribution_disabled"
    )
    # voucher_pools (pool_id, status, starts_at, ends_at) and (batch_id,
    # status) are created in affiliate_rewards.ensure_affiliate_indexes
    # alongside the pre-existing uniq_pool_code/pool_status indexes so all
    # voucher_pools index management stays in one place.


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
# Admin API
# ---------------------------------------------------------------------------

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
