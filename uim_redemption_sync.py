"""Synchronise Welcome redemption evidence from Databot's committed UIM imports.

Databot supplies redemption observations through its private feed
(``databot.uim_redemption_feed.v1``, token-authenticated, read-only). This
module copies them into APReferral's OWN evidence-sync collections; it never
qualifies, reserves an Account ID, grants XP or creates rewards. Whether an
observation qualifies anyone is decided later, by
``affiliate_qualification`` (live, only once the rule is active) or by the
read-only Qualification Preview — both through the same canonical validator.

Writes (and only these):

* ``uim_redemption_sync_state``  — one doc: lease, last attempt/success/error.
* ``uim_redemption_batches``     — one doc per Databot import batch
  (``_id`` = batch id): upstream status, per-batch row cursor, counters,
  coverage. Shaped like ``marketing_upload_batches`` so the qualification
  service extracts it the same way (``affiliate_qualification.SOURCE_SPECS``).
* ``uim_redemption_rows``        — one doc per Welcome-candidate observation
  (``_id`` = Databot's stable per-observation ref): insert-only, never
  rewritten, so re-syncs, retries and overlapping runs are no-ops.

A Databot batch that is later rolled back is propagated through the existing
correction policy (``affiliate_qualification.void_upload_batch``): unprocessed
evidence seen only in that batch is voided; a qualification it already
produced is flagged for manual reconciliation, never revoked, and its Account
ID is never released.

Rows whose code is not in any Welcome issuance store are counted on the
batch, not stored (same policy as the evidence collection). Codes and
accounts never reach logs.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

import affiliate_qualification as aq

logger = logging.getLogger(__name__)

STATE_COLLECTION = "uim_redemption_sync_state"
BATCHES_COLLECTION = aq.SOURCE_SPECS[aq.SOURCE_DATABOT_UIM]["batches"]
ROWS_COLLECTION = aq.SOURCE_SPECS[aq.SOURCE_DATABOT_UIM]["rows"]
STATE_ID = aq.SOURCE_DATABOT_UIM
FEED_CONTRACT = "databot.uim_redemption_feed.v1"
FEED_PREFIX = "/internal/uim-redemption-feed/v1"

SYNC_LEASE = timedelta(minutes=10)
DEFAULT_MAX_ROWS = 5000
ROW_PAGE = 1000
BATCH_PAGE = 200
MAX_BATCH_PAGES = 50
# Sync health: older than this since the last successful sync = stale. The
# evidence may be incomplete; it is never read as "zero redemptions".
DEFAULT_STALE_AFTER = timedelta(minutes=int(os.getenv("UIM_REDEMPTION_STALE_AFTER_MINUTES", "60")))


class FeedUnavailable(RuntimeError):
    """Databot feed unreachable / refused / malformed. Message is safe to log."""


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

def feed_config() -> dict:
    """Feed transport settings. The existing Databot integration settings
    (``DATABOT_BASE_URL`` / ``DATABOT_API_KEY``, see ``databot_client``) are the
    default, so no second URL/token is needed; the dedicated
    ``UIM_REDEMPTION_FEED_*`` variables remain as optional overrides. The
    enable switch stays separate and OFF by default — ``DATABOT_ENABLED`` (the
    Phase-1 shadow client) deliberately does not turn this sync on."""
    return {
        "enabled": os.getenv("UIM_REDEMPTION_SYNC_ENABLED", "false").strip().lower() == "true",
        "base_url": (os.getenv("UIM_REDEMPTION_FEED_URL", "").strip()
                     or os.getenv("DATABOT_BASE_URL", "").strip()).rstrip("/"),
        "token": os.getenv("UIM_REDEMPTION_FEED_TOKEN", "").strip() or os.getenv("DATABOT_API_KEY", "").strip(),
        "timeout": float(os.getenv("UIM_REDEMPTION_FEED_TIMEOUT_SECONDS", "15")),
    }


def http_fetcher(base_url: str, token: str, *, timeout: float = 15.0):
    """``fetch(path, params) -> dict`` over the private feed. Errors carry
    the path and status only (never the token or response body)."""
    import requests

    if not base_url or not token:
        raise FeedUnavailable("feed_not_configured")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

    def fetch(path: str, params: dict | None = None) -> dict:
        try:
            resp = session.get(f"{base_url}{FEED_PREFIX}{path}", params=params or {}, timeout=timeout)
        except requests.RequestException as exc:
            raise FeedUnavailable(f"feed_unreachable path={path} err={exc.__class__.__name__}") from None
        if resp.status_code != 200:
            raise FeedUnavailable(f"feed_http_{resp.status_code} path={path}")
        try:
            body = resp.json()
        except ValueError:
            raise FeedUnavailable(f"feed_bad_json path={path}") from None
        if body.get("contract") != FEED_CONTRACT:
            raise FeedUnavailable(f"feed_contract_mismatch path={path}")
        return body

    return fetch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _coverage_wallclock(row: dict) -> str | None:
    """Redeem time as GMT+8 wall clock text ("YYYY-MM-DDTHH:MM:SS"), for the
    coverage display only. Naive/legacy source times ARE wall clock; an
    explicit-offset instant is converted to GMT+8. Fixed-width, so string
    $min/$max order is chronological."""
    if row.get("redeemed_at_basis") == "source_offset":
        when = _parse_iso(row.get("redeemed_at_utc"))
        return when.astimezone(aq.KL_TZ).strftime("%Y-%m-%dT%H:%M:%S") if when else None
    wall = row.get("redeemed_at_wallclock")
    return str(wall)[:19] if wall else None


def _iso(value) -> str | None:
    value = aq._aware_utc(value)
    return value.isoformat() if value else None


_indexes_ready = False


def ensure_sync_indexes(db) -> None:
    """Paging/ordering indexes on this module's own collections only."""
    global _indexes_ready
    if _indexes_ready:
        return
    db[ROWS_COLLECTION].create_index([("batch_id", ASCENDING), ("_id", ASCENDING)], name="uim_rows_batch")
    db[BATCHES_COLLECTION].create_index([("status", ASCENDING), ("committed_at", ASCENDING)],
                                        name="uim_batches_status_committed")
    _indexes_ready = True


def _welcome_candidates(db, codes: set[str]) -> tuple[set[str], set[str]]:
    """(welcome_candidates, codes_in_other_pools). A candidate is any code in
    a Welcome issuance store; whether it really resolves to one recipient is
    decided later by affiliate_qualification.decide_recipient."""
    codes = {c for c in codes if c}
    if not codes:
        return set(), set()
    chunk = list(codes)
    welcome = {r["code"] for r in db.voucher_pools.find({"pool_id": aq.WELCOME_POOL_ID, "code": {"$in": chunk}},
                                                        {"code": 1})}
    welcome |= {r["code"] for r in db.new_joiner_claims.find({"code": {"$in": chunk}}, {"code": 1})}
    other = {r["code"] for r in db.voucher_pools.find({"pool_id": {"$ne": aq.WELCOME_POOL_ID},
                                                       "code": {"$in": chunk}}, {"code": 1})}
    return welcome, other - welcome


def _acquire(db, now_utc: datetime) -> str | None:
    owner = uuid.uuid4().hex
    try:
        doc = db[STATE_COLLECTION].find_one_and_update(
            {"_id": STATE_ID, "$or": [{"lease_until": {"$exists": False}}, {"lease_until": None},
                                      {"lease_until": {"$lt": now_utc}}]},
            {"$set": {"lease_until": now_utc + SYNC_LEASE, "lease_owner": owner, "last_attempt_at": now_utc}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return None  # the doc exists and its lease is held
    return owner if doc and doc.get("lease_owner") == owner else None


def _release(db, owner: str, fields: dict) -> None:
    db[STATE_COLLECTION].update_one(
        {"_id": STATE_ID, "lease_owner": owner},
        {"$set": {**fields, "lease_until": None}},
    )


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def _sync_batches(db, fetch, *, now_utc: datetime, out: dict) -> None:
    """Upsert every upstream batch's metadata; propagate rollbacks."""
    after = None
    for _ in range(MAX_BATCH_PAGES):
        page = fetch("/batches", {"limit": BATCH_PAGE, **({"after": after} if after else {})})
        for meta in page.get("batches") or []:
            batch_id = meta.get("batch_id")
            if not batch_id:
                continue
            status = meta.get("status")
            fields = {
                "upstream_status": status,
                "created_at_upstream": _parse_iso(meta.get("created_at")),
                "committed_at": _parse_iso(meta.get("committed_at")),
                "upstream_rolled_back_at": _parse_iso(meta.get("rolled_back_at")),
                "file_type": meta.get("file_type"),
                "upstream_rows_written": meta.get("rows_written"),
                "upstream_rows_deleted_stale": meta.get("rows_deleted_stale"),
                "upstream_invalid_date_count": meta.get("invalid_date_count"),
                "last_listed_at": now_utc,
            }
            if status == "committed":
                fields["status"] = "committed"
            db[BATCHES_COLLECTION].update_one(
                {"_id": batch_id},
                {"$set": fields,
                 "$setOnInsert": {"batch_id": batch_id, "first_seen_at": now_utc, "rows_complete": False,
                                  "rows_cursor": None, "counters": {}}},
                upsert=True,
            )
            out["batches_listed"] += 1
            if status == "rolled_back":
                local = db[BATCHES_COLLECTION].find_one({"_id": batch_id}, {"rollback_propagated_at": 1})
                if not (local or {}).get("rollback_propagated_at"):
                    # Existing correction policy; flags (never revokes) any
                    # qualification and never releases a credited account.
                    result = aq.void_upload_batch(
                        db, upload_batch_id=batch_id, reason="databot_uim_batch_rolled_back",
                        now_utc=now_utc, source=aq.SOURCE_DATABOT_UIM,
                    )
                    db[BATCHES_COLLECTION].update_one(
                        {"_id": batch_id},
                        {"$set": {"rollback_propagated_at": now_utc, "rollback_result": result,
                                  "status": "rolled_back"}},
                    )
                    out["rollbacks_propagated"] += 1
        after = page.get("next_after")
        if not after:
            return
    out["batch_listing_truncated"] = True


def _sync_rows(db, fetch, *, now_utc: datetime, max_rows: int, out: dict) -> None:
    """Page rows of committed, incomplete batches, oldest commit first, within
    ``max_rows``. The cursor only advances if nobody else advanced it
    (conditional update), and only after the page's rows are stored, so a
    crash or an overlapping run re-reads a page instead of skipping it."""
    budget = int(max_rows)
    pending = list(db[BATCHES_COLLECTION].find(
        {"status": "committed", "rows_complete": False, "rolled_back_at": {"$exists": False}},
        {"rows_cursor": 1, "committed_at": 1},
    ).sort([("committed_at", ASCENDING), ("_id", ASCENDING)]))
    for batch in pending:
        cursor = batch.get("rows_cursor")
        while budget > 0:
            params = {"limit": min(ROW_PAGE, budget)}
            if cursor:
                params["after"] = cursor
            page = fetch(f"/batches/{batch['_id']}/rows", params)
            if not page.get("rows_available", True):
                out["batches_unavailable"] += 1
                break
            rows = page.get("rows") or []
            welcome, other = _welcome_candidates(db, {r.get("coupon_code") for r in rows if r.get("coupon_code")})
            counters = {"received": len(rows), "skipped_no_code": int(page.get("skipped_no_code") or 0),
                        "stored": 0, "already_stored": 0, "non_welcome_code": 0, "unknown_code": 0,
                        "removed_observations": 0}
            coverage = []
            for row in rows:
                code = row.get("coupon_code")
                when = _coverage_wallclock(row)
                if when is not None:
                    coverage.append(when)
                if row.get("observation") == "deleted":
                    counters["removed_observations"] += 1
                if code not in welcome:
                    counters["non_welcome_code" if code in other else "unknown_code"] += 1
                    continue
                doc = {k: row.get(k) for k in (
                    "observation", "coupon_code", "account", "account_basis", "file_type", "redeemed_at_utc",
                    "redeemed_at_wallclock", "redeemed_at_basis", "redeemed_at_raw", "redeemed_at_column",
                    "campaign", "source_period", "provenance", "provenance_version",
                )}
                doc.update({"_id": row["ref"], "batch_id": batch["_id"], "synced_at": now_utc})
                try:
                    db[ROWS_COLLECTION].insert_one(doc)
                    counters["stored"] += 1
                except DuplicateKeyError:
                    counters["already_stored"] += 1
            update = {
                "$set": {"rows_cursor": page.get("next_after") or cursor, "rows_synced_at": now_utc},
                "$inc": {f"counters.{k}": v for k, v in counters.items() if v},
            }
            if coverage:
                update["$min"] = {"coverage_min_redeemed_at": min(coverage)}
                update["$max"] = {"coverage_max_redeemed_at": max(coverage)}
            if page.get("done"):
                update["$set"].update({"rows_complete": True, "rows_completed_at": now_utc})
            res = db[BATCHES_COLLECTION].update_one(
                {"_id": batch["_id"], "rows_cursor": cursor, "rows_complete": False}, update
            )
            if not getattr(res, "modified_count", 0):
                out["cursor_races"] += 1  # another run advanced it; its counters stand
                break
            budget -= max(1, int(page.get("rows_in_page") or len(rows)))
            out["rows_received"] += len(rows)
            out["rows_stored"] += counters["stored"]
            if page.get("done"):
                out["batches_completed"] += 1
                break
            cursor = page.get("next_after")
        if budget <= 0:
            out["budget_exhausted"] = True
            return


def sync_once(db, *, fetch, now_utc: datetime, max_rows: int = DEFAULT_MAX_ROWS) -> dict:
    """One bounded, leased, idempotent sync pass. Safe to run concurrently
    and to crash at any point."""
    out = {"ok": False, "batches_listed": 0, "rollbacks_propagated": 0, "rows_received": 0, "rows_stored": 0,
           "batches_completed": 0, "batches_unavailable": 0, "cursor_races": 0}
    owner = _acquire(db, now_utc)
    if owner is None:
        out["skipped"] = "lease_held"
        return out
    try:
        ensure_sync_indexes(db)
        _sync_batches(db, fetch, now_utc=now_utc, out=out)
        _sync_rows(db, fetch, now_utc=now_utc, max_rows=max_rows, out=out)
    except Exception as exc:
        safe = str(exc) if isinstance(exc, FeedUnavailable) else exc.__class__.__name__
        logger.warning("[UIM_SYNC][FAILED] err=%s", safe)
        _release(db, owner, {"last_failure_at": now_utc, "last_error": safe[:200]})
        out["error"] = safe
        return out
    _release(db, owner, {"last_success_at": now_utc, "last_error": None, "last_result": dict(out, ok=True)})
    out["ok"] = True
    logger.info("[UIM_SYNC][DONE] %s", out)
    return out


def run_scheduled_sync(db, *, now_utc: datetime | None = None) -> dict:
    """Scheduler entry point. No-op unless UIM_REDEMPTION_SYNC_ENABLED=true.
    Independent of the qualification rule's mode."""
    cfg = feed_config()
    if not cfg["enabled"]:
        return {"skipped": "sync_disabled"}
    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        fetch = http_fetcher(cfg["base_url"], cfg["token"], timeout=cfg["timeout"])
    except FeedUnavailable as exc:
        db[STATE_COLLECTION].update_one(
            {"_id": STATE_ID}, {"$set": {"last_attempt_at": now_utc, "last_failure_at": now_utc,
                                         "last_error": str(exc)}}, upsert=True)
        return {"ok": False, "error": str(exc)}
    return sync_once(db, fetch=fetch, now_utc=now_utc)


# ---------------------------------------------------------------------------
# Health (read-only)
# ---------------------------------------------------------------------------

def sync_health(db, *, now_utc: datetime, stale_after: timedelta = DEFAULT_STALE_AFTER) -> dict:
    """What the preview / preflight show about the evidence feed. ``stale``
    or ``never synced`` means incomplete or unavailable evidence — never zero."""
    state = db[STATE_COLLECTION].find_one({"_id": STATE_ID}) or {}
    last_success = aq._aware_utc(state.get("last_success_at"))
    last_failure = aq._aware_utc(state.get("last_failure_at"))
    stale = last_success is None or now_utc - last_success > stale_after
    committed = list(db[BATCHES_COLLECTION].find(
        {"status": "committed", "rolled_back_at": {"$exists": False}},
        {"rows_complete": 1, "coverage_min_redeemed_at": 1, "coverage_max_redeemed_at": 1, "committed_at": 1,
         "counters": 1},
    ))
    incomplete = sum(1 for b in committed if not b.get("rows_complete"))
    mins = [str(b["coverage_min_redeemed_at"]) for b in committed if b.get("coverage_min_redeemed_at")]
    maxs = [str(b["coverage_max_redeemed_at"]) for b in committed if b.get("coverage_max_redeemed_at")]
    commits = [aq._aware_utc(b.get("committed_at")) for b in committed if b.get("committed_at")]
    totals: dict[str, int] = {}
    for b in committed:
        for k, v in (b.get("counters") or {}).items():
            totals[k] = totals.get(k, 0) + int(v or 0)
    problems = []
    if last_success is None:
        problems.append("evidence_sync_never_succeeded")
    elif stale:
        problems.append("evidence_sync_stale")
    if not committed:
        problems.append("no_committed_uim_batches_synced")
    if incomplete:
        problems.append("uim_batches_partially_synced")
    return {
        "source": "Databot UIM imports (committed batches)",
        "last_success_at": _iso(last_success),
        "last_attempt_at": _iso(state.get("last_attempt_at")),
        "last_failure_at": _iso(last_failure),
        "last_error": state.get("last_error"),
        "stale": stale,
        "stale_after_minutes": int(stale_after.total_seconds() // 60),
        "committed_batches": len(committed),
        "batches_partially_synced": incomplete,
        "latest_batch_committed_at": max(commits).isoformat() if commits else None,
        "coverage_start": min(mins) if mins else None,
        "coverage_end": max(maxs) if maxs else None,
        "coverage_timezone": "GMT+8 wall clock (UIM export time)",
        "row_counters": totals,
        "problems": problems,
    }
