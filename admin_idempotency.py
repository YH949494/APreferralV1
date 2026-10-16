"""Server-side idempotency for admin write requests.

Why: a response can be lost after the server has committed a write (dropped
connection, timeout, double click, two tabs). The admin's natural reaction is to
send the same request again. Without a server-side record the second request is
either executed twice or answered with a confusing error such as
``duplicate_code`` for work that actually succeeded.

How: the client sends a stable request id (``Idempotency-Key`` header or an
``idempotency_key`` field in the JSON body, which the legacy Mini App panel already
sends). The first request with a key runs; its **successful** result is stored and
every retry with the same key and the same payload gets that stored result back
(``Idempotent-Replay: true``) instead of running again.

  * key reused with a different payload -> 422 ``idempotency_key_reused`` (never
    guess which one the client meant)
  * same key while the first request is still running -> 409 ``request_in_progress``
    (``Retry-After``), so two simultaneous submissions cannot both execute
  * the first request failed (non-2xx or an exception) -> the key is released, so a
    retry runs again (the guarded operations roll back on failure, so that is safe)
  * the worker died while holding the key -> after the lease expires the next retry
    takes over; an optional ``recover`` callback lets the caller detect work that
    already completed (exactly-once for drop creation)
  * the idempotency store itself being unavailable never blocks the operation
    (fail open, logged): the underlying writes keep their own guards, e.g. the
    unique voucher-code index.

Storage: collection ``admin_idempotency``, ``_id = "<scope>:<key>"``. Records expire
after ``TTL_HOURS`` through a TTL index created best-effort at startup; correctness
does not depend on the index.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone

from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger(__name__)

COLLECTION = "admin_idempotency"
TTL_HOURS = 24
LEASE_SECONDS = 120
KEY_BODY_FIELDS = ("idempotency_key", "unique_key")
_KEY_RE = re.compile(r"^[A-Za-z0-9._:\-]{8,128}$")

IN_PROGRESS = "in_progress"
COMPLETED = "completed"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def ensure_indexes(db) -> None:
    """Best-effort TTL index. Safe to call repeatedly; never raises."""
    try:
        db[COLLECTION].create_index([("expires_at", 1)], expireAfterSeconds=0, name="ttl_admin_idempotency_expires_at")
    except Exception:  # noqa: BLE001 - housekeeping only
        logger.warning("[IDEMPOTENCY] ttl index not created", exc_info=True)


def extract_key(request, body: dict | None):
    """Return ``(key | None, error_dict | None)``. Header wins over body."""
    raw = None
    try:
        raw = request.headers.get("Idempotency-Key")
    except Exception:  # noqa: BLE001
        raw = None
    if not raw and isinstance(body, dict):
        for field in KEY_BODY_FIELDS:
            if body.get(field):
                raw = body.get(field)
                break
    if raw is None or raw == "":
        return None, None
    raw = str(raw).strip()
    if not _KEY_RE.match(raw):
        return None, {
            "status": "error", "code": "bad_idempotency_key",
            "message": "The request id must be 8-128 characters: letters, digits and . _ : -",
        }
    return raw, None


def fingerprint(payload) -> str:
    """Stable hash of the request payload, ignoring the key fields themselves."""
    if isinstance(payload, dict):
        payload = {k: v for k, v in payload.items() if k not in KEY_BODY_FIELDS}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _record_id(scope: str, key: str) -> str:
    return f"{scope}:{key}"


def run(db, *, scope: str, key: str, payload, handler, recover=None, should_cache=None, admin: str | None = None, now=None):
    """Run ``handler()`` at most once per (scope, key).

    ``handler`` returns ``(body_dict, status_code)``. Returns
    ``(body, status_code, extra_headers)``. ``recover`` (optional) is called only
    when taking over an abandoned in-progress record and may return
    ``(body, status_code)`` if the work had in fact completed. ``should_cache``
    (optional ``(body, status) -> bool``) overrides the default "cache 2xx only"
    for endpoints that answer failures with HTTP 200.
    """
    now = now or _utcnow()
    rec_id = _record_id(scope, key)
    fp = fingerprint(payload)
    owner = uuid.uuid4().hex

    try:
        col = db[COLLECTION]
        decision = _begin(col, rec_id, scope, key, fp, owner, admin, now, recover)
    except (PyMongoError, AttributeError, TypeError):
        logger.exception("[IDEMPOTENCY] store unavailable scope=%s; running without protection", scope)
        body, status = handler()
        return body, status, {}

    kind = decision[0]
    if kind == "replay":
        _, body, status = decision
        return body, status, {"Idempotent-Replay": "true"}
    if kind == "conflict":
        return ({
            "status": "error", "code": "idempotency_key_reused",
            "message": "This request id was already used with different data. Start a new request.",
        }, 422, {})
    if kind == "in_progress":
        return ({
            "status": "error", "code": "request_in_progress",
            "message": "This request is still being processed. Wait a moment, then check the result before sending it again.",
        }, 409, {"Retry-After": "2"})

    try:
        body, status = handler()
    except BaseException:
        _release(col, rec_id, owner)
        raise
    cacheable = should_cache(body, status) if should_cache else 200 <= int(status) < 300
    if cacheable:
        _complete(col, rec_id, owner, body, int(status), now)
    else:
        _release(col, rec_id, owner)
    return body, status, {}


def _begin(col, rec_id, scope, key, fp, owner, admin, now, recover):
    doc = {
        "_id": rec_id, "scope": scope, "key": key, "fingerprint": fp, "state": IN_PROGRESS, "owner": owner,
        "admin": admin, "created_at": now, "lease_expires_at": now + timedelta(seconds=LEASE_SECONDS),
        "expires_at": now + timedelta(hours=TTL_HOURS),
    }
    try:
        col.insert_one(doc)
        return ("new",)
    except DuplicateKeyError:
        pass
    existing = col.find_one({"_id": rec_id})
    if not existing:  # expired between insert and read: just try once more
        try:
            col.insert_one(doc)
            return ("new",)
        except DuplicateKeyError:
            existing = col.find_one({"_id": rec_id})
            if not existing:
                return ("in_progress",)
    if existing.get("fingerprint") != fp:
        return ("conflict",)
    if existing.get("state") == COMPLETED:
        return ("replay", existing.get("response"), int(existing.get("status_code") or 200))
    lease = _aware(existing.get("lease_expires_at"))
    if lease and lease > now:
        return ("in_progress",)
    # Abandoned by a worker that died: take the lease (compare-and-swap so only one retry wins).
    taken = col.update_one(
        {"_id": rec_id, "state": IN_PROGRESS, "lease_expires_at": existing.get("lease_expires_at")},
        {"$set": {"owner": owner, "lease_expires_at": now + timedelta(seconds=LEASE_SECONDS), "admin": admin}},
    )
    if getattr(taken, "modified_count", 0) != 1:
        return ("in_progress",)
    if recover is not None:
        try:
            recovered = recover()
        except Exception:  # noqa: BLE001
            logger.exception("[IDEMPOTENCY] recover callback failed scope=%s", scope)
            recovered = None
        if recovered:
            body, status = recovered
            _complete(col, rec_id, owner, body, int(status), now)
            return ("replay", body, int(status))
    return ("new",)


def _complete(col, rec_id, owner, body, status, now) -> None:
    update = {"$set": {"state": COMPLETED, "response": body, "status_code": status, "completed_at": now}}
    for attempt in range(3):
        try:
            res = col.update_one({"_id": rec_id, "owner": owner}, update)
            if getattr(res, "matched_count", 1) != 1:
                logger.warning("[IDEMPOTENCY] lost the lease before completing %s", rec_id)
            return
        except PyMongoError:
            logger.warning("[IDEMPOTENCY] complete failed (attempt %s) %s", attempt + 1, rec_id, exc_info=True)
    logger.error("[IDEMPOTENCY] could not store result for %s; a retry will rely on recover()/guards", rec_id)


def _release(col, rec_id, owner) -> None:
    try:
        col.delete_one({"_id": rec_id, "owner": owner, "state": IN_PROGRESS})
    except PyMongoError:
        logger.warning("[IDEMPOTENCY] release failed %s", rec_id, exc_info=True)
