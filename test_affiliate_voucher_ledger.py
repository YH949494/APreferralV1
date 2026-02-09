import os
import unittest
from datetime import datetime, timedelta, timezone

from pymongo.errors import DuplicateKeyError

import scheduler as scheduler_module


class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeVoucherLedgerCollection:
    def __init__(self):
        self.docs = []
        self._id = 1

    def insert_one(self, doc):
        for existing in self.docs:
            if doc.get("source_uid") is not None:
                if (
                    existing.get("affiliate_uid") == doc.get("affiliate_uid")
                    and existing.get("reward_type") == doc.get("reward_type")
                    and existing.get("source_uid") == doc.get("source_uid")
                ):
                    raise DuplicateKeyError("duplicate key")
            if doc.get("period") is not None:
                if (
                    existing.get("affiliate_uid") == doc.get("affiliate_uid")
                    and existing.get("period") == doc.get("period")
                ):
                    raise DuplicateKeyError("duplicate key")
        new_doc = dict(doc)
        new_doc["_id"] = self._id
        self._id += 1
        self.docs.append(new_doc)
        return FakeInsertResult(new_doc["_id"])

    def update_one(self, filt, update):
        for doc in self.docs:
            if doc.get("_id") != filt.get("_id"):
                continue
            status_filter = filt.get("status")
            if isinstance(status_filter, dict) and "$ne" in status_filter:
                if doc.get("status") == status_filter["$ne"]:
                    continue
            for key, value in update.get("$set", {}).items():
                doc[key] = value
            return type("UpdateResult", (), {"modified_count": 1})
        return type("UpdateResult", (), {"modified_count": 0})

    def find(self, filt):
        status = filt.get("status")
        reward_type = filt.get("reward_type")
        results = []
        for doc in self.docs:
            if status and doc.get("status") != status:
                continue
            if isinstance(reward_type, dict) and "$in" in reward_type:
                if doc.get("reward_type") not in reward_type["$in"]:
                    continue
            results.append(dict(doc))
        return results

    def find_one(self, filt):
        for doc in self.docs:
            if doc.get("_id") == filt.get("_id"):
                return dict(doc)
        return None


class FakeEventsCollection:
    def __init__(self, active_days_count):
        self.active_days_count = active_days_count

    def aggregate(self, pipeline):  # noqa: ARG002
        return [{"_id": f"day-{idx}"} for idx in range(self.active_days_count)]


class FakeReferralEventsCollection:
    def __init__(self, rows):
        self.rows = rows

    def aggregate(self, pipeline):  # noqa: ARG002
        return list(self.rows)


class FakeUsersCollection:
    def __init__(self, users):
        self.users = list(users)

    def find(self, query, projection=None):  # noqa: ARG002
        if "level3_confirmed_at" in query:
            return [u for u in self.users if u.get("level3_confirmed_at")]
        if isinstance(query.get("user_id"), dict) and "$in" in query.get("user_id"):
            ids = set(query["user_id"]["$in"])
            return [u for u in self.users if u.get("user_id") in ids]
        return []

    def find_one(self, query, projection=None):  # noqa: ARG002
        for u in self.users:
            if u.get("user_id") == query.get("user_id"):
                return dict(u)
        return None


class FakeDB:
    def __init__(self, *, ledger=None, users=None, events=None, referral_events=None):
        self.voucher_ledger = ledger or FakeVoucherLedgerCollection()
        self.users = users or FakeUsersCollection([])
        self.events = events or FakeEventsCollection(0)
        self.referral_events = referral_events or FakeReferralEventsCollection([])


class VoucherLedgerTests(unittest.TestCase):
    def setUp(self):
        self.original_db = scheduler_module.db
        self.original_claim = scheduler_module.claim_voucher_for_user

    def tearDown(self):
        scheduler_module.db = self.original_db
        scheduler_module.claim_voucher_for_user = self.original_claim
        os.environ.pop("AFFILIATE_L2_DROP_ID", None)

    def test_ledger_dedup(self):
        fake_db = FakeDB()
        scheduler_module.db = fake_db
        payload = {
            "beneficiary_uid": 100,
            "affiliate_uid": 100,
            "reward_type": "DOWNLINE_L2_SMALL",
            "source_uid": 200,
            "period": None,
            "status": "PENDING",
            "created_at": datetime.now(timezone.utc),
        }
        first = scheduler_module._insert_voucher_ledger(payload)
        second = scheduler_module._insert_voucher_ledger(payload)
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(len(fake_db.voucher_ledger.docs), 1)

    def test_auto_issue_sets_issued(self):
        ledger = FakeVoucherLedgerCollection()
        entry = {
            "_id": 1,
            "beneficiary_uid": 123,
            "affiliate_uid": 123,
            "reward_type": "DOWNLINE_L2_SMALL",
            "status": "PENDING",
        }
        ledger.docs.append(entry)
        users = FakeUsersCollection([{"user_id": 123, "username": "tester"}])
        fake_db = FakeDB(ledger=ledger, users=users)
        scheduler_module.db = fake_db
        os.environ["AFFILIATE_L2_DROP_ID"] = "drop-1"
        scheduler_module.claim_voucher_for_user = lambda **kwargs: {"code": "CODE123"}  # noqa: ARG005
        scheduler_module.issue_voucher_ledger_entry(entry, now_utc_ts=datetime.now(timezone.utc))
        updated = ledger.find_one({"_id": 1})
        self.assertEqual(updated.get("status"), "ISSUED")
        self.assertEqual(updated.get("issued_voucher_code"), "CODE123")

    def test_monthly_entries_stay_pending(self):
        now_utc = datetime(2025, 3, 10, tzinfo=timezone.utc)
        invitee_rows = [
            {"_id": 2001, "inviter_id": 1001},
            {"_id": 2002, "inviter_id": 1001},
            {"_id": 2003, "inviter_id": 1001},
        ]
        referral_events = FakeReferralEventsCollection(invitee_rows)
        period_start = datetime(2025, 2, 1, tzinfo=timezone.utc)
        users = FakeUsersCollection(
            [
                {"user_id": 2001, "level3_confirmed_at": period_start + timedelta(days=1)},
                {"user_id": 2002, "level3_confirmed_at": period_start + timedelta(days=2)},
                {"user_id": 2003, "level3_confirmed_at": period_start + timedelta(days=3)},
                {"user_id": 1001, "affiliate_status": "active", "flags": []},
            ]
        )
        events = FakeEventsCollection(active_days_count=7)
        fake_db = FakeDB(users=users, events=events, referral_events=referral_events)
        scheduler_module.db = fake_db
        scheduler_module._create_monthly_voucher_ledgers(now_utc)
        self.assertEqual(len(fake_db.voucher_ledger.docs), 1)
        entry = fake_db.voucher_ledger.docs[0]
        self.assertEqual(entry.get("status"), "PENDING")
        self.assertEqual(entry.get("reward_type"), "MONTHLY_20")


if __name__ == "__main__":
    unittest.main()
