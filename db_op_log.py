"""Structured, write-free logging around heavy MongoDB operations.

One ``[DB_OP]`` line per wrapped operation, so oplog-heavy jobs can be
measured from Fly logs alone (``fly logs | grep '\\[DB_OP\\]'``) and
correlated with Atlas oplog GB/hour without adding any database writes.

Only aggregate counts are logged: never per-user ids, filters, documents,
voucher codes, account identifiers or exception messages (which can echo
query values). A failure records the exception class name only.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone

logger = logging.getLogger("db_op")

_COUNT_ATTRS = (
    ("matched", "matched_count"),
    ("modified", "modified_count"),
    ("upserted", "upserted_count"),
    ("inserted", "inserted_count"),
    ("deleted", "deleted_count"),
)


class DbOpRecord:
    __slots__ = ("job", "collection", "op", "batch_size", "retry", "counts", "skipped_unchanged", "extra")

    def __init__(self, job: str, collection: str, op: str, batch_size, retry: int):
        self.job = job
        self.collection = collection
        self.op = op
        self.batch_size = batch_size
        self.retry = retry
        self.counts: dict[str, int] = {}
        self.skipped_unchanged = None
        self.extra: dict[str, object] = {}

    def record(self, result) -> None:
        """Pull counters off a pymongo result (Update/Delete/BulkWrite/InsertMany)."""
        if result is None:
            return
        for key, attr in _COUNT_ATTRS:
            try:
                value = getattr(result, attr, None)
            except Exception:  # unacknowledged writes raise on access
                value = None
            if isinstance(value, int):
                self.counts[key] = self.counts.get(key, 0) + value
        # UpdateResult carries upserted_id (one per call), not a count.
        try:
            if not isinstance(getattr(result, "upserted_count", None), int) and \
                    getattr(result, "upserted_id", None) is not None:
                self.counts["upserted"] = self.counts.get("upserted", 0) + 1
        except Exception:
            pass
        inserted_ids = getattr(result, "inserted_ids", None)
        if "inserted" not in self.counts and isinstance(inserted_ids, list):
            self.counts["inserted"] = len(inserted_ids)


@contextmanager
def db_op(job: str, collection: str, op: str, *, batch_size=None, retry: int = 0):
    rec = DbOpRecord(job, collection, op, batch_size, retry)
    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    failure = None
    try:
        yield rec
    except Exception as exc:
        failure = exc.__class__.__name__
        raise
    finally:
        duration_ms = int((time.monotonic() - started) * 1000)
        fields = [
            f"job={rec.job}",
            f"collection={rec.collection}",
            f"op={rec.op}",
            f"started_at={started_at.isoformat()}",
            f"ended_at={datetime.now(timezone.utc).isoformat()}",
            f"duration_ms={duration_ms}",
        ]
        for key, _ in _COUNT_ATTRS:
            fields.append(f"{key}={rec.counts.get(key, 0)}")
        fields.append(f"batch_size={rec.batch_size if rec.batch_size is not None else '-'}")
        fields.append(f"retry={rec.retry}")
        if rec.skipped_unchanged is not None:
            fields.append(f"skipped_unchanged={rec.skipped_unchanged}")
        for key, value in rec.extra.items():
            fields.append(f"{key}={value}")
        fields.append(f"status={'failed' if failure else 'ok'}")
        if failure:
            fields.append(f"failure={failure}")
        try:
            logger.info("[DB_OP] %s", " ".join(fields))
        except Exception:
            pass
