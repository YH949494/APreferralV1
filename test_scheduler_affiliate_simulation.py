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
                elif op == "$nin":
                    if value in expected:
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
                before = dict(d)
                d.update(update.get("$set", {}))
                modified = 1 if d != before else 0
                return type("_UpdateResult", (), {"upserted_id": None, "modified_count": modified})()

        if upsert:
            row = dict(filt)
            row.update(update.get("$setOnInsert", {}))
            row.update(update.get("$set", {}))
            self.docs.append(row)
            return type("_UpdateResult", (), {"upserted_id": "fake_upsert", "modified_count": 0})()

        return type("_UpdateResult", (), {"upserted_id": None, "modified_count": 0})()


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

    def test_second_run_updates_existing_simulated_ledger_without_inserting_duplicate(self):
        now_ts = datetime(2026, 1, 20, tzinfo=timezone.utc)
        fake_db = _FakeDb(now_ts)

        with patch.object(scheduler, "db", fake_db), patch.object(scheduler, "now_utc", return_value=now_ts), patch.object(
            scheduler, "_get_chat_member_status", return_value="member"
        ):
            os.environ["AFFILIATE_SIMULATE"] = "1"
            try:
                scheduler.evaluate_affiliate_simulated_ledgers()
                first_count = len(fake_db.affiliate_ledger.docs)
                fake_db.users.docs[0]["total_xp"] = 999
                scheduler.evaluate_affiliate_simulated_ledgers()
            finally:
                os.environ.pop("AFFILIATE_SIMULATE", None)

        self.assertEqual(first_count, 4)
        self.assertEqual(len(fake_db.affiliate_ledger.docs), 4)
        self.assertEqual(
            len(
                {
                    (doc.get("user_id"), doc.get("invitee_user_id"), doc.get("gate_day"), doc.get("tier"), doc.get("created_at"))
                    for doc in fake_db.affiliate_ledger.docs
                }
            ),
            4,
        )
        self.assertEqual(max(doc.get("xp_total") for doc in fake_db.affiliate_ledger.docs), 999)

    def test_does_not_overwrite_non_simulated_final_ledger(self):
        now_ts = datetime(2026, 1, 20, tzinfo=timezone.utc)
        fake_db = _FakeDb(now_ts)
        joined_main_at = fake_db.users.docs[0]["joined_main_at"]
        fake_db.affiliate_ledger.docs.append(
            {
                "user_id": 202,
                "invitee_user_id": 101,
                "gate_day": 3,
                "tier": "T1",
                "created_at": joined_main_at,
                "simulate": False,
                "ledger_type": "AFFILIATE_MONTHLY",
                "status": "ISSUED",
            }
        )

        with patch.object(scheduler, "db", fake_db), patch.object(scheduler, "now_utc", return_value=now_ts), patch.object(
            scheduler, "_get_chat_member_status", return_value="member"
        ):
            os.environ["AFFILIATE_SIMULATE"] = "1"
            try:
                scheduler.evaluate_affiliate_simulated_ledgers()
            finally:
                os.environ.pop("AFFILIATE_SIMULATE", None)

        non_sim = [d for d in fake_db.affiliate_ledger.docs if d.get("simulate") is False and d.get("status") == "ISSUED"]
        self.assertEqual(len(non_sim), 1)
        self.assertEqual(non_sim[0].get("ledger_type"), "AFFILIATE_MONTHLY")
        simulated = [d for d in fake_db.affiliate_ledger.docs if d.get("simulate") is True and d.get("status") == "SIMULATED_PENDING"]
        self.assertEqual(len(simulated), 4)


if __name__ == "__main__":
    import unittest

    unittest.main()
