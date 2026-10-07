"""``POST /admin/pools/replace`` (``vouchers.admin_pools_replace_v2``):
replaces undated *available* stock only; never touches issued history or
scheduled-batch rows; inserts before deleting."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from flask import Flask

from fake_mongo import FakeDb

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _db():
    # real Mongo always enforces _id uniqueness; the fake only enforces declared keys
    return FakeDb({"voucher_pools": [("pool_id", "code")], "admin_locks": [("_id",)]})


def _row(pool_id, code, status="available", **extra):
    return {"pool_id": pool_id, "code": code, "status": status, "created_at": NOW, **extra}


class AdminPoolsReplaceTests(unittest.TestCase):
    def _post(self, db, **payload):
        import vouchers as m
        app = Flask(__name__)
        orig_db, orig_bypass = m.db, m.BYPASS_ADMIN
        try:
            m.db, m.BYPASS_ADMIN = db, True
            with app.test_request_context("/admin/pools/replace", method="POST", json=payload):
                resp = m.admin_pools_replace_v2()
            body, status = resp if isinstance(resp, tuple) else (resp, 200)
            return status, body.get_json()
        finally:
            m.db, m.BYPASS_ADMIN = orig_db, orig_bypass

    def test_replaces_available_keeps_issued_and_batch_rows(self):
        db = _db()
        db.voucher_pools.insert_one(_row("T1", "OLD1"))
        db.voucher_pools.insert_one(_row("T1", "OLD2"))
        db.voucher_pools.insert_one(_row("T1", "ISSUED1", status="issued"))
        db.voucher_pools.insert_one(_row("T1", "BATCHED", batch_id="b1"))
        db.voucher_pools.insert_one(_row("T2", "OTHERPOOL"))

        status, p = self._post(db, pool_id="t1", codes_text="NEW1\r\nNEW2\nNEW1\n\n")
        self.assertEqual(status, 200)
        self.assertEqual((p["received"], p["inserted"], p["duplicates"], p["old_available_removed"]), (2, 2, 0, 2))
        codes = {r["code"] for r in db.voucher_pools.find({"pool_id": "T1"})}
        self.assertEqual(codes, {"NEW1", "NEW2", "ISSUED1", "BATCHED"})
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T2"}), 1)

    def test_overlap_with_existing_available_is_kept_not_deleted(self):
        db = _db()
        db.voucher_pools.insert_one(_row("WELCOME", "KEEP"))
        db.voucher_pools.insert_one(_row("WELCOME", "DROP"))
        status, p = self._post(db, pool_id="WELCOME", codes_text="KEEP\nFRESH")
        self.assertEqual(status, 200)
        self.assertEqual((p["inserted"], p["duplicates"], p["old_available_removed"]), (1, 1, 1))
        codes = {r["code"] for r in db.voucher_pools.find({"pool_id": "WELCOME"})}
        self.assertEqual(codes, {"KEEP", "FRESH"})

    def test_code_already_issued_counts_as_duplicate_and_is_not_resurrected(self):
        db = _db()
        db.voucher_pools.insert_one(_row("T3", "USED", status="issued"))
        status, p = self._post(db, pool_id="T3", codes_text="USED\nNEW")
        self.assertEqual((status, p["inserted"], p["duplicates"]), (200, 1, 1))
        self.assertEqual(db.voucher_pools.find_one({"code": "USED"})["status"], "issued")

    def test_rejects_bad_pool_denomination_pool_and_empty(self):
        db = _db()
        db.voucher_pools.insert_one(_row("T1", "OLD"))
        for pid in ("NOPE", "AFFILIATE_10", ""):
            status, p = self._post(db, pool_id=pid, codes_text="X")
            self.assertEqual((status, p["reason"]), (400, "bad_pool_id"))
        status, p = self._post(db, pool_id="T1", codes_text=" \n\r\n")
        self.assertEqual((status, p["reason"]), (400, "empty_codes"))
        # nothing deleted on any rejected request
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 1)

    def test_concurrent_replace_is_rejected_and_stock_untouched(self):
        db = _db()
        db.voucher_pools.insert_one(_row("T1", "OLD"))
        db.admin_locks.insert_one({
            "_id": "pool_replace:T1", "owner": "other",
            "expiresAt": datetime.now(timezone.utc) + timedelta(seconds=60),
        })
        status, p = self._post(db, pool_id="T1", codes_text="NEW")
        self.assertEqual((status, p["reason"]), (409, "replace_in_progress"))
        self.assertEqual({r["code"] for r in db.voucher_pools.find({"pool_id": "T1"})}, {"OLD"})
        # the other holder's lock is not released by the rejected request
        self.assertEqual(db.admin_locks.find_one({"_id": "pool_replace:T1"})["owner"], "other")

    def test_expired_lock_is_taken_over_and_released_after(self):
        db = _db()
        db.voucher_pools.insert_one(_row("T1", "OLD"))
        db.admin_locks.insert_one({
            "_id": "pool_replace:T1", "owner": "crashed",
            "expiresAt": datetime.now(timezone.utc) - timedelta(seconds=5),
        })
        status, p = self._post(db, pool_id="T1", codes_text="NEW")
        self.assertEqual((status, p["inserted"], p["old_available_removed"]), (200, 1, 1))
        self.assertIsNone(db.admin_locks.find_one({"_id": "pool_replace:T1"}))

    def test_lock_released_on_success_and_pools_lock_independent(self):
        db = _db()
        db.admin_locks.insert_one({
            "_id": "pool_replace:T2", "owner": "other",
            "expiresAt": datetime.now(timezone.utc) + timedelta(seconds=60),
        })
        status, _ = self._post(db, pool_id="T1", codes_text="A")
        self.assertEqual(status, 200)
        self.assertIsNone(db.admin_locks.find_one({"_id": "pool_replace:T1"}))


if __name__ == "__main__":
    unittest.main()
