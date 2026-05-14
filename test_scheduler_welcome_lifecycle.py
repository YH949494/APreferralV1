import unittest
from datetime import datetime, timedelta, timezone

import scheduler


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    def find(self, filt=None, projection=None):
        class _Cursor(list):
            def limit(self, n):
                return _Cursor(self[:n])
        out = []
        for d in self.docs:
            if d.get("claimed") is True:
                continue
            if "expired_at" in d:
                continue
            out.append(d)
        return _Cursor(out)

    def find_one(self, filt=None, projection=None):
        filt = filt or {}
        for d in self.docs:
            ok = True
            for k, v in filt.items():
                if d.get(k) != v:
                    ok = False
                    break
            if ok:
                return d
        return None

    def update_one(self, filt, update, upsert=False):
        modified = 0
        for d in self.docs:
            if d.get("_id") != filt.get("_id"):
                continue
            set_doc = update.get("$set", {})
            for k, v in set_doc.items():
                d[k] = v
            modified = 1
            break
        class _Res:
            modified_count = modified
        return _Res()


class _FakeDb:
    def __init__(self, welcome_docs, claims=None):
        self.welcome_eligibility = _FakeCollection(welcome_docs)
        self.new_joiner_claims = _FakeCollection(claims or [])


class WelcomeLifecycleTests(unittest.TestCase):
    def test_lifecycle_paths_and_idempotency(self):
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        docs = [
            {"_id": 1, "uid": 1, "first_seen_at": now - timedelta(hours=13)},
            {"_id": 2, "uid": 2, "first_seen_at": now - timedelta(hours=37)},
            {"_id": 3, "uid": 3, "first_seen_at": now - timedelta(hours=49)},
            {"_id": 4, "uid": 4, "first_seen_at": now - timedelta(hours=49)},
            {"_id": 5, "uid": 5, "first_seen_at": now - timedelta(hours=13), "claimed": True},
            {"_id": 6, "uid": 6, "first_seen_at": now - timedelta(hours=13), "expired_at": now},
        ]
        db = _FakeDb(docs, claims=[{"uid": 4, "claimed_at": now - timedelta(hours=1)}])
        sent = []

        def _send(uid, text):
            sent.append(uid)
            if uid == 2:
                return False, "telegram_http_error", False
            return True, None, False

        out1 = scheduler.process_welcome_voucher_lifecycle(now_ref=now, db_ref=db, send_fn=_send, batch_limit=20)
        self.assertEqual(out1["reminder_sent"], 1)
        self.assertEqual(out1["expired"], 1)
        self.assertEqual(out1["send_failed"], 1)
        self.assertIn(1, sent)
        self.assertIn(2, sent)
        self.assertEqual(db.welcome_eligibility.find_one({"_id": 1}).get("lifecycle_state"), "reminded")
        self.assertEqual(db.welcome_eligibility.find_one({"_id": 3}).get("lifecycle_state"), "expired")
        self.assertEqual(db.welcome_eligibility.find_one({"_id": 4}).get("lifecycle_state"), "claimed")

        sent.clear()
        out2 = scheduler.process_welcome_voucher_lifecycle(now_ref=now, db_ref=db, send_fn=_send, batch_limit=20)
        self.assertEqual(out2["reminder_sent"], 0)
        self.assertEqual(out2["expired"], 0)
        self.assertEqual(sent, [2])


if __name__ == "__main__":
    unittest.main()
