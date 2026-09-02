"""P2 regression: the generic/undated admin pool upload endpoint
(``POST /admin/pools/upload``, ``vouchers.admin_pools_upload_v2``) must
reject the September+ denomination pools (AFFILIATE_5/10/50).

``_resolve_denomination_pool_target`` (affiliate_rewards.py) requires a
canonical scheduled monthly batch and deliberately has NO undated-stock
fallback for these pools, so a bare ``voucher_pools`` row with no
``batch_id`` inserted through this endpoint would look like a successful
upload while being permanently unissuable dead inventory -- and could even
block a later, correct scheduled upload of the same code via the
``(pool_id, code)`` uniqueness index.

Legacy T1-T5 tier pools and WELCOME must keep uploading through this same
endpoint exactly as before (they have a legacy undated fallback).
"""
from __future__ import annotations

import unittest

from flask import Flask

from fake_mongo import FakeDb

UNIQUE_KEYS = {"voucher_pools": [("pool_id", "code")]}


def _db():
    return FakeDb(UNIQUE_KEYS)


class DenominationUndatedUploadRejectedTests(unittest.TestCase):
    def _post_upload(self, m, db, *, pool_id: str, codes_text: str):
        app = Flask(__name__)
        orig_db = m.db
        orig_bypass = m.BYPASS_ADMIN
        try:
            m.db = db
            m.BYPASS_ADMIN = True
            with app.test_request_context(
                "/admin/pools/upload",
                method="POST",
                json={"pool_id": pool_id, "codes_text": codes_text},
            ):
                resp = m.admin_pools_upload_v2()
            if isinstance(resp, tuple):
                body, status = resp
            else:
                body, status = resp, 200
            return status, body.get_json()
        finally:
            m.db = orig_db
            m.BYPASS_ADMIN = orig_bypass

    def test_affiliate_5_rejected(self):
        import vouchers as m
        db = _db()
        status, payload = self._post_upload(m, db, pool_id="AFFILIATE_5", codes_text="F0001\nF0002")
        self.assertEqual(status, 400)
        self.assertFalse(payload.get("ok"))
        self.assertEqual(payload.get("code"), "denomination_pool_requires_scheduled_batch")
        self.assertEqual(db.voucher_pools.count_documents({}), 0)

    def test_affiliate_10_rejected(self):
        import vouchers as m
        db = _db()
        status, payload = self._post_upload(m, db, pool_id="AFFILIATE_10", codes_text="T0001")
        self.assertEqual(status, 400)
        self.assertFalse(payload.get("ok"))
        self.assertEqual(payload.get("code"), "denomination_pool_requires_scheduled_batch")
        self.assertEqual(db.voucher_pools.count_documents({}), 0)

    def test_affiliate_50_rejected(self):
        import vouchers as m
        db = _db()
        status, payload = self._post_upload(m, db, pool_id="AFFILIATE_50", codes_text="H0001")
        self.assertEqual(status, 400)
        self.assertFalse(payload.get("ok"))
        self.assertEqual(payload.get("code"), "denomination_pool_requires_scheduled_batch")
        self.assertEqual(db.voucher_pools.count_documents({}), 0)

    def test_legacy_t1_upload_still_succeeds(self):
        import vouchers as m
        db = _db()
        status, payload = self._post_upload(m, db, pool_id="T1", codes_text="L0001\nL0002")
        self.assertEqual(status, 200)
        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("inserted"), 2)
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 2)
        # Legacy row has no batch_id -- exactly the pre-existing undated
        # behaviour, untouched by this fix.
        row = db.voucher_pools.find_one({"pool_id": "T1"})
        self.assertNotIn("batch_id", row)

    def test_welcome_upload_still_succeeds(self):
        import vouchers as m
        db = _db()
        status, payload = self._post_upload(m, db, pool_id="WELCOME", codes_text="W0001")
        self.assertEqual(status, 200)
        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("inserted"), 1)

    def test_scheduled_denomination_batch_upload_still_succeeds(self):
        """The fix is scoped to the UNDATED endpoint only -- the scheduled
        batch upload path (affiliate_voucher_batches.create_batch) for the
        same denomination pools must be entirely unaffected."""
        import affiliate_voucher_batches as avb
        db = _db()
        db._unique_keys_by_collection["affiliate_voucher_batches"] = []
        result = avb.create_batch(
            db,
            admin_identity="admin1",
            batch_name="AFFILIATE_10 Sep 2026",
            pool_id="AFFILIATE_10",
            entitlement_month="202609",
            codes=["T0001", "T0002"],
        )
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["counts"]["inserted"], 2)
        row = db.voucher_pools.find_one({"pool_id": "AFFILIATE_10"})
        self.assertIn("batch_id", row)


if __name__ == "__main__":
    unittest.main()
