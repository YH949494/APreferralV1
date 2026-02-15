import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pymongo import ReturnDocument

import scheduler


class _Result:
    def __init__(self, modified_count=0):
        self.modified_count = modified_count


class _PendingCollection:
    def __init__(self, docs):
        self.docs = docs

    def _match(self, doc, filt):
        for key, val in filt.items():
            if key == "$or":
                if not any(self._match(doc, branch) for branch in val):
                    return False
                continue
            if isinstance(val, dict):
                if "$in" in val:
                    if doc.get(key) not in val["$in"]:
                        return False
                    continue
                if "$lte" in val:
                    if doc.get(key) is None or not (doc.get(key) <= val["$lte"]):
                        return False
                    continue
                if "$exists" in val:
                    exists = key in doc
                    if bool(val["$exists"]) != exists:
                        return False
                    continue
            if doc.get(key) != val:
                return False
        return True

    def _apply_update(self, doc, update):
        for k, v in update.get("$set", {}).items():
            doc[k] = v
        for k, v in update.get("$inc", {}).items():
            doc[k] = doc.get(k, 0) + v
        for k in update.get("$unset", {}).keys():
            doc.pop(k, None)

    def find_one_and_update(self, filt, update, sort=None, return_document=None):
        matches = [d for d in self.docs if self._match(d, filt)]
        if not matches:
            return None
        if sort:
            field, direction = sort[0]
            matches.sort(key=lambda d: d.get(field), reverse=direction < 0)
        doc = matches[0]
        original = dict(doc)
        self._apply_update(doc, update)
        if return_document == ReturnDocument.BEFORE:
            return original
        return dict(doc)

    def update_one(self, filt, update):
        for doc in self.docs:
            if self._match(doc, filt):
                self._apply_update(doc, update)
                return _Result(modified_count=1)
        return _Result(modified_count=0)

    def update_many(self, filt, update):
        modified = 0
        for doc in self.docs:
            if self._match(doc, filt):
                self._apply_update(doc, update)
                modified += 1
        return _Result(modified_count=modified)


class _UsersCollection:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, filt, projection=None):
        return self.docs.get(filt.get("user_id"))


class _AwardEvents:
    def __init__(self):
        self.docs = []

    def insert_one(self, doc):
        self.docs.append(dict(doc))


class _ReferralEvents:
    def aggregate(self, pipeline):
        return []


class _FakeSchedulerDB:
    def __init__(self, pending_docs, user_docs):
        self.pending_referrals = _PendingCollection(pending_docs)
        self.users = _UsersCollection(user_docs)
        self.referral_award_events = _AwardEvents()
        self.referral_events = _ReferralEvents()


class SchedulerPendingChannelTests(unittest.TestCase):
    def setUp(self):
        self.orig_db = scheduler.db
        self.orig_get_status = scheduler._get_chat_member_status
        self.orig_check_channel = scheduler._check_official_channel_subscribed_sync
        self.orig_record_event = scheduler._record_referral_event
        self.orig_grant_xp = scheduler.grant_xp
        self.orig_award = scheduler.calc_referral_award
        self.orig_first = scheduler.maybe_handle_first_referral
        self.orig_confirm = scheduler.confirm_qualified_invitees
        self.orig_now_utc = scheduler.now_utc
        self.orig_now_kl = scheduler.now_kl
        self.orig_requests_get = scheduler.requests.get
        self.orig_time_sleep = scheduler.time.sleep
        self.orig_official_channel_id = scheduler.OFFICIAL_CHANNEL_ID
        self.orig_bot_token = scheduler.BOT_TOKEN
        self.fixed_now = datetime(2025, 1, 10, tzinfo=timezone.utc)
        scheduler.now_utc = lambda: self.fixed_now
        scheduler.now_kl = lambda: self.fixed_now
        scheduler._get_chat_member_status = lambda uid: "member"
        scheduler._record_referral_event = lambda *args, **kwargs: True
        scheduler.grant_xp = lambda *args, **kwargs: True
        scheduler.calc_referral_award = lambda total: (10, 0)
        scheduler.maybe_handle_first_referral = lambda *args, **kwargs: None
        scheduler.confirm_qualified_invitees = lambda: 0
        scheduler.OFFICIAL_CHANNEL_ID = -100123
        scheduler.BOT_TOKEN = "token"

    def tearDown(self):
        scheduler.db = self.orig_db
        scheduler._get_chat_member_status = self.orig_get_status
        scheduler._check_official_channel_subscribed_sync = self.orig_check_channel
        scheduler._record_referral_event = self.orig_record_event
        scheduler.grant_xp = self.orig_grant_xp
        scheduler.calc_referral_award = self.orig_award
        scheduler.maybe_handle_first_referral = self.orig_first
        scheduler.confirm_qualified_invitees = self.orig_confirm
        scheduler.now_utc = self.orig_now_utc
        scheduler.now_kl = self.orig_now_kl
        scheduler.requests.get = self.orig_requests_get
        scheduler.time.sleep = self.orig_time_sleep
        scheduler.OFFICIAL_CHANNEL_ID = self.orig_official_channel_id
        scheduler.BOT_TOKEN = self.orig_bot_token

    def _base_pending(self, status="pending"):
        return {
            "_id": 1,
            "group_id": scheduler.GROUP_ID,
            "status": status,
            "inviter_user_id": 11,
            "invitee_user_id": 22,
            "created_at_utc": self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1),
        }

    def _user_doc(self):
        return {
            22: {
                "user_id": 22,
                "joined_main_at": self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1),
                "created_at": self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1),
            }
        }

    def test_pending_after_hold_not_subscribed_moves_to_pending_channel(self):
        scheduler._check_official_channel_subscribed_sync = lambda uid: (False, "not_subscribed")
        doc = self._base_pending("pending")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending_channel")
        self.assertIn("pending_channel_since_utc", doc)
        self.assertEqual(doc["last_fail_reason"], "channel_not_subscribed")
        self.assertEqual(doc["next_retry_at_utc"], self.fixed_now + timedelta(hours=12))
        self.assertEqual(doc["retry_count"], 1)

    def test_pending_channel_not_subscribed_within_expiry_retries(self):
        scheduler._check_official_channel_subscribed_sync = lambda uid: (False, "not_subscribed")
        doc = self._base_pending("pending_channel")
        doc["pending_channel_since_utc"] = self.fixed_now - timedelta(days=2)
        doc["next_retry_at_utc"] = self.fixed_now - timedelta(minutes=1)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending_channel")
        self.assertEqual(doc["next_retry_at_utc"], self.fixed_now + timedelta(hours=12))
        self.assertEqual(doc["retry_count"], 1)

    def test_pending_channel_not_subscribed_after_expiry_revokes(self):
        scheduler._check_official_channel_subscribed_sync = lambda uid: (False, "not_subscribed")
        doc = self._base_pending("pending_channel")
        doc["pending_channel_since_utc"] = self.fixed_now - timedelta(days=8)
        doc["next_retry_at_utc"] = self.fixed_now - timedelta(minutes=1)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "revoked")
        self.assertEqual(doc["revoked_reason"], "channel_not_subscribed_expired")

    def test_subscribed_awards_referral(self):
        scheduler._check_official_channel_subscribed_sync = lambda uid: (True, "")
        doc = self._base_pending("pending")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "awarded")
        self.assertEqual(doc["xp_added"], 10)

    def test_channel_sync_helper_retries_once_on_429_then_succeeds(self):
        class _Resp:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload

            def json(self):
                return self._payload

        calls = []

        def _fake_get(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                return _Resp(429, {"ok": False, "error_code": 429, "parameters": {"retry_after": 1}})
            return _Resp(200, {"ok": True, "result": {"status": "member"}})

        slept = []
        scheduler.requests.get = _fake_get
        scheduler.time.sleep = lambda seconds: slept.append(seconds)

        ok, reason = scheduler._check_official_channel_subscribed_sync(22)

        self.assertTrue(ok)
        self.assertEqual(reason, "status:member")
        self.assertEqual(len(calls), 2)
        self.assertEqual(slept, [1])

    def test_channel_sync_helper_returns_rate_limited_after_retry(self):
        class _Resp:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload

            def json(self):
                return self._payload

        scheduler.requests.get = lambda *args, **kwargs: _Resp(429, {"ok": False, "error_code": 429, "parameters": {"retry_after": 10}})
        sleeps = []
        scheduler.time.sleep = lambda seconds: sleeps.append(seconds)

        ok, reason = scheduler._check_official_channel_subscribed_sync(22)

        self.assertFalse(ok)
        self.assertEqual(reason, "rate_limited")
        self.assertEqual(sleeps, [5])


class MemberUpdateHandlerStatusFilterTests(unittest.TestCase):
    def test_leave_group_revoke_queries_pending_and_pending_channel(self):
        source = Path("main.py").read_text(encoding="utf-8")
        self.assertIn('"status": {"$in": ["pending", "pending_channel"]}', source)


if __name__ == "__main__":
    unittest.main()
