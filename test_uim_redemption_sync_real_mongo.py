"""Real-MongoDB concurrency check for uim_redemption_sync (needs an ISOLATED
server, never production):  UIM_SYNC_TEST_MONGO_URI=mongodb://127.0.0.1:27018/?directConnection=true

Not in the CI gate (the gate refuses skips and CI has no MongoDB); the gated
suite covers the same race deterministically
(test_uim_redemption_sync.py::test_interleaved_runs_never_double_count_or_skip).
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest

import affiliate_qualification as aq
import uim_redemption_sync as usync
from test_uim_redemption_sync import NOW, FakeFeed, urow


def test_concurrent_syncs_on_real_mongo_are_idempotent():
    uri = os.environ.get("UIM_SYNC_TEST_MONGO_URI")
    if not uri:
        pytest.skip("UIM_SYNC_TEST_MONGO_URI not set (isolated MongoDB required)")
    from pymongo import MongoClient

    client = MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=2000)
    db = client[f"uimsync_{uuid.uuid4().hex[:10]}"]
    try:
        aq.ensure_indexes(db)
        usync._indexes_ready = False
        for i in range(500):
            db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": f"W{i}", "status": "issued", "issued_to_user_id": i})
        feed = FakeFeed()
        feed.add_batch([urow(f"W{i}", f"0{i:05d}", "2026-10-05 10:00:00") for i in range(500)])
        feed.add_batch([urow(f"W{i}", f"0{i:05d}", "2026-10-05 10:00:00") for i in range(0, 500, 2)])
        page = usync.ROW_PAGE
        usync.ROW_PAGE = 37
        errors = []

        def worker(k):
            try:
                out = {"batches_listed": 0, "rollbacks_propagated": 0, "rows_received": 0, "rows_stored": 0,
                       "batches_completed": 0, "batches_unavailable": 0, "cursor_races": 0}
                usync._sync_batches(db, feed, now_utc=NOW, out=out)   # bypass the lease on purpose
                usync._sync_rows(db, feed, now_utc=NOW, max_rows=100000, out=out)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
        try:
            [t.start() for t in threads]
            [t.join() for t in threads]
            out = {"batches_listed": 0, "rollbacks_propagated": 0, "rows_received": 0, "rows_stored": 0,
                   "batches_completed": 0, "batches_unavailable": 0, "cursor_races": 0}
            usync._sync_rows(db, feed, now_utc=NOW, max_rows=100000, out=out)  # finish anything a race left
        finally:
            usync.ROW_PAGE = page
        assert not errors
        assert db[usync.ROWS_COLLECTION].count_documents({}) == 750
        batches = list(db[usync.BATCHES_COLLECTION].find())
        assert all(b["rows_complete"] for b in batches)
        # Counters advance with the cursor only: exactly one count per row, no double counting.
        assert sum(b["counters"]["received"] for b in batches) == 750
        evidence_keys = {(r["coupon_code"], r["account"]) for r in db[usync.ROWS_COLLECTION].find()}
        assert len(evidence_keys) == 500 and ("W7", "000007") in evidence_keys
    finally:
        client.drop_database(db.name)
        client.close()
