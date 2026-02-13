import os
from datetime import datetime, timedelta, timezone
from unittest import TestCase
from unittest.mock import patch

from pymongo.errors import WriteError

import scheduler


class _Cursor:
    def __init__(self, docs):
        self._docs = docs

    def limit(self, n):
        return self._docs[:n]


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    def _match_value(self, value, cond):
        if isinstance(cond, dict):
            for op, expected in cond.items():
                if op == "$exists":
                    exists = value is not None
                    if bool(expected) != exists:
                        return False
                elif op == "$in":
                    if value not in expected:
                        return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for k, cond in (filt or {}).items():
            if not self._match_value(doc.get(k), cond):
                return False
        return True

    def find(self, filt=None, proj=None):
        out = []
        for d in self.docs:
            if self._match(d, filt or {}):
                if proj:
                    out.append({k: d.get(k) for k in proj.keys()})
                else:
                    out.append(dict(d))
        return _Cursor(out)

    def find_one(self, filt=None, proj=None):
        for d in self.docs:
            if self._match(d, filt or {}):
                if proj:
                    return {k: d.get(k) for k in proj.keys()}
                return dict(d)
        return None

    def update_one(self, filt, update, upsert=False):
        set_paths = set(update.get("$set", {}).keys())
        soi_paths = set(update.get("$setOnInsert", {}).keys())
        if set_paths & soi_paths:
            raise WriteError("Updating the path 'status' would create a conflict at 'status'", code=40)

        for d in self.docs:
            if self._match(d, filt):
                d.update(update.get("$set", {}))
                return

        if upsert:
            row = dict(filt)
            row.update(update.get("$setOnInsert", {}))
            row.update(update.get("$set", {}))
            self.docs.append(row)


class _FakeDb:
    def __init__(self, now_ts):
        self.pending_referrals = _FakeCollection(
            [
                {
                    "status": "awarded",
                    "invitee_user_id": 101,
                    "inviter_user_id": 202,
                }
            ]
        )
        self.users = _FakeCollection(
            [
                {
                    "user_id": 101,
                    "joined_main_at": now_ts - timedelta(days=40),
                    "total_xp": 123,
                    "monthly_xp": 12,
                }
            ]
        )
        self.affiliate_ledger = _FakeCollection()
        self.referral_audit = _FakeCollection()
        self.claim_rate_limits = _FakeCollection()


class EvaluateAffiliateSimulatedLedgersTests(TestCase):
    def test_upsert_path_has_no_status_update_conflict(self):
        now_ts = datetime(2026, 1, 20, tzinfo=timezone.utc)
        fake_db = _FakeDb(now_ts)

        with patch.object(scheduler, "db", fake_db), patch.object(scheduler, "now_utc", return_value=now_ts), patch.object(
            scheduler, "_get_chat_member_status", return_value="member"
        ):
            os.environ["AFFILIATE_SIMULATE"] = "1"
            try:
                processed = scheduler.evaluate_affiliate_simulated_ledgers()
            finally:
                os.environ.pop("AFFILIATE_SIMULATE", None)

        self.assertEqual(processed, 4)
        self.assertEqual(len(fake_db.affiliate_ledger.docs), 4)
        for doc in fake_db.affiliate_ledger.docs:
            self.assertEqual(doc.get("status"), "SIMULATED_PENDING")


if __name__ == "__main__":
    import unittest

    unittest.main()
