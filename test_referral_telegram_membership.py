"""Regression tests for the getChatMember HTTP 400 audit (Issue 1).

Covers: settle_pending_referrals() checks membership against the referral's
own resolved destination (community_group -> GROUP_ID, official_channel ->
OFFICIAL_CHANNEL_ID) instead of always defaulting to OFFICIAL_CHANNEL_ID;
Telegram's JSON error body is parsed and logged without leaking the bot
token or full API URL; 429/5xx/network failures stay retryable; permanent
config/permission 400s become a bounded operational error instead of an
endless per-invitee retry; other 400s get a bounded retry then a terminal
revoke; and one bad Telegram response never aborts the rest of the batch.
"""

import logging
import unittest
from datetime import datetime, timedelta, timezone

import requests
from pymongo import ReturnDocument

import scheduler

# Captured at import time, before any test in any module has executed and
# possibly monkeypatched scheduler._get_official_channel_member_status
# without restoring it (several other test modules in this suite do,
# e.g. test_official_channel_reopen_audit.py). setUp forces this back onto
# the module so these tests exercise the real HTTP-parsing/classification
# logic regardless of prior test-execution order.
_REAL_GET_OFFICIAL_CHANNEL_MEMBER_STATUS = scheduler._get_official_channel_member_status


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
                    if bool(val["$exists"]) != (key in doc):
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
        self._invitees = set()

    def find_one(self, filt, projection=None):
        return None

    def insert_one(self, doc):
        self.docs.append(dict(doc))


class _ReferralEvents:
    def aggregate(self, pipeline):
        return []

    def count_documents(self, filt, limit=None):
        return 0


class _FakeSchedulerDB:
    def __init__(self, pending_docs, user_docs):
        self.pending_referrals = _PendingCollection(pending_docs)
        self.users = _UsersCollection(user_docs)
        self.referral_award_events = _AwardEvents()
        self.referral_events = _ReferralEvents()


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class ReferralTelegramMembershipTests(unittest.TestCase):
    def setUp(self):
        self.orig_db = scheduler.db
        self.orig_record_event = scheduler._record_referral_event
        self.orig_grant_xp = scheduler.grant_xp
        self.orig_award = scheduler.calc_referral_award
        self.orig_first = scheduler.maybe_handle_first_referral
        self.orig_mark_qualified = scheduler.mark_invitee_qualified
        self.orig_confirm = scheduler.confirm_qualified_invitees
        self.orig_eval_engagement = scheduler.evaluate_referral_engagement
        self.orig_now_utc = scheduler.now_utc
        self.orig_now_kl = scheduler.now_kl
        self.orig_requests_get = scheduler.requests.get
        self.orig_official_channel_id = scheduler.OFFICIAL_CHANNEL_ID
        self.orig_group_id = scheduler.GROUP_ID
        self.orig_bot_token = scheduler.BOT_TOKEN
        self.orig_dm = scheduler._maybe_send_referral_qualified_dm
        # Some other test modules call logging.disable(logging.CRITICAL) at
        # import time and never re-enable it, which would otherwise make
        # assertLogs() below fail depending on test run order.
        self.orig_disable_level = logging.root.manager.disable
        logging.disable(logging.NOTSET)
        scheduler._get_official_channel_member_status = _REAL_GET_OFFICIAL_CHANNEL_MEMBER_STATUS

        self.fixed_now = datetime(2025, 1, 10, tzinfo=timezone.utc)
        scheduler.now_utc = lambda: self.fixed_now
        scheduler.now_kl = lambda: self.fixed_now
        scheduler._record_referral_event = lambda *a, **kw: True
        scheduler.grant_xp = lambda *a, **kw: True
        scheduler.calc_referral_award = lambda total: (10, 0)
        scheduler.maybe_handle_first_referral = lambda *a, **kw: None
        scheduler.mark_invitee_qualified = lambda *a, **kw: True
        scheduler.confirm_qualified_invitees = lambda: 0
        scheduler._maybe_send_referral_qualified_dm = lambda *a, **kw: None
        scheduler.evaluate_referral_engagement = lambda **kw: {
            "qualified": True,
            "score": 3,
            "signals": {},
            "points": {},
            "window_start": self.fixed_now - timedelta(hours=1),
            "window_end": self.fixed_now,
        }
        scheduler.OFFICIAL_CHANNEL_ID = -1002396761021
        scheduler.GROUP_ID = -1002304653063
        scheduler.BOT_TOKEN = "TESTTOKEN123"

        self.requested_chat_ids = []

    def tearDown(self):
        scheduler.db = self.orig_db
        scheduler._record_referral_event = self.orig_record_event
        scheduler.grant_xp = self.orig_grant_xp
        scheduler.calc_referral_award = self.orig_award
        scheduler.maybe_handle_first_referral = self.orig_first
        scheduler.mark_invitee_qualified = self.orig_mark_qualified
        scheduler.confirm_qualified_invitees = self.orig_confirm
        scheduler.evaluate_referral_engagement = self.orig_eval_engagement
        scheduler.now_utc = self.orig_now_utc
        scheduler.now_kl = self.orig_now_kl
        scheduler.requests.get = self.orig_requests_get
        scheduler.OFFICIAL_CHANNEL_ID = self.orig_official_channel_id
        scheduler.GROUP_ID = self.orig_group_id
        scheduler.BOT_TOKEN = self.orig_bot_token
        scheduler._maybe_send_referral_qualified_dm = self.orig_dm
        scheduler._get_official_channel_member_status = _REAL_GET_OFFICIAL_CHANNEL_MEMBER_STATUS
        logging.disable(self.orig_disable_level)

    def _base_pending(self, destination_type="community_group", **overrides):
        doc = {
            "_id": 1,
            "status": "pending",
            "inviter_user_id": 11,
            "invitee_user_id": 22,
            "destination_type": destination_type,
            "destination_chat_id": (
                scheduler.GROUP_ID if destination_type == "community_group" else scheduler.OFFICIAL_CHANNEL_ID
            ),
            "created_at_utc": self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1),
            "retry_count": 0,
        }
        doc.update(overrides)
        return doc

    def _user_doc(self, uid=22):
        return {
            uid: {
                "user_id": uid,
                "joined_main_at": self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1),
                "created_at": self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1),
            }
        }

    def _mock_get(self, responder):
        def fake_get(url, params=None, timeout=None):
            self.requested_chat_ids.append(params.get("chat_id"))
            return responder(params)

        scheduler.requests.get = fake_get

    # 1. Community-group referral checks GROUP_ID.
    def test_community_group_checks_group_id_not_official_channel(self):
        self._mock_get(lambda params: _FakeResponse(200, {"ok": True, "result": {"status": "member"}}))
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(self.requested_chat_ids, [scheduler.GROUP_ID])
        self.assertNotIn(scheduler.OFFICIAL_CHANNEL_ID, self.requested_chat_ids)
        self.assertEqual(doc["status"], "awarded")

    # 2. Official-channel referral checks OFFICIAL_CHANNEL_ID.
    def test_official_channel_checks_official_channel_id(self):
        self._mock_get(lambda params: _FakeResponse(200, {"ok": True, "result": {"status": "member"}}))
        doc = self._base_pending("official_channel", referral_join_seen_at_utc=self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1))
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(self.requested_chat_ids, [scheduler.OFFICIAL_CHANNEL_ID])
        self.assertEqual(doc["status"], "awarded")

    # 3. Telegram 200 + member settles normally.
    def test_200_member_settles(self):
        self._mock_get(lambda params: _FakeResponse(200, {"ok": True, "result": {"status": "member"}}))
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "awarded")
        self.assertEqual(doc["xp_added"], 10)

    # 4. Telegram 200 + left/kicked fails qualification normally.
    def test_200_left_fails_qualification(self):
        self._mock_get(lambda params: _FakeResponse(200, {"ok": True, "result": {"status": "left"}}))
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "revoked")
        self.assertEqual(doc["revoked_reason"], "not_in_official_channel")

    # 5. Telegram 429 remains retryable and respects retry_after.
    def test_429_retryable_respects_retry_after(self):
        self._mock_get(
            lambda params: _FakeResponse(
                429, {"ok": False, "error_code": 429, "parameters": {"retry_after": 7}}
            )
        )
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending")
        self.assertEqual(doc["next_retry_at_utc"], self.fixed_now + timedelta(seconds=7))
        self.assertEqual(doc["retry_last_reason"], "telegram_429")

    # 6. Network timeout remains retryable.
    def test_network_timeout_retryable(self):
        def fake_get(url, params=None, timeout=None):
            raise requests.exceptions.Timeout("boom")

        scheduler.requests.get = fake_get
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending")
        self.assertEqual(doc["retry_last_reason"], "telegram_request_failed")

    # 7. Telegram 400 invalid chat/configuration is not retried forever per invitee.
    def test_400_config_error_bounded_not_endless_retry(self):
        self._mock_get(
            lambda params: _FakeResponse(400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"})
        )
        doc = self._base_pending("community_group", retry_count=scheduler.MAX_TELEGRAM_CONFIG_RETRIES)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "error")
        self.assertEqual(doc["error_reason"], "membership_check_unresolvable")
        self.assertEqual(doc["tg_description"], "Bad Request: chat not found")

    def test_400_config_error_retries_before_terminal(self):
        self._mock_get(
            lambda params: _FakeResponse(400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"})
        )
        doc = self._base_pending("community_group", retry_count=0)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending")
        self.assertEqual(doc["retry_last_reason"], "telegram_config_error")
        self.assertEqual(doc["next_retry_at_utc"], self.fixed_now + timedelta(seconds=scheduler.TELEGRAM_CONFIG_ERROR_BACKOFF_SEC))

    def test_400_user_specific_bounded_then_terminal_error_not_revoked(self):
        # An unresolved Telegram check is operational uncertainty, never
        # proof the invitee left/was never subscribed -- it must terminate
        # as status="error", not "revoked". No XP, no referral_settled, no
        # referral_revoked ledger event either way.
        self._mock_get(
            lambda params: _FakeResponse(400, {"ok": False, "error_code": 400, "description": "Bad Request: user not found"})
        )
        doc = self._base_pending("community_group", retry_count=scheduler.MAX_TELEGRAM_USER_RETRIES)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "error")
        self.assertEqual(doc["error_reason"], "membership_check_unresolvable")
        self.assertNotIn("xp_added", doc)
        self.assertNotIn("revoked_reason", doc)

    def test_401_unauthorized_classified_as_config_not_user(self):
        # A bad/expired bot token is a global outage, not anything specific
        # to this invitee -- must be classified/bounded like other config
        # errors (long backoff), never treated as a per-invitee 400.
        self._mock_get(
            lambda params: _FakeResponse(401, {"ok": False, "error_code": 401, "description": "Unauthorized"})
        )
        doc = self._base_pending("community_group", retry_count=0)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending")
        self.assertEqual(doc["retry_last_reason"], "telegram_config_error")
        self.assertEqual(
            doc["next_retry_at_utc"], self.fixed_now + timedelta(seconds=scheduler.TELEGRAM_CONFIG_ERROR_BACKOFF_SEC)
        )

    def test_malformed_json_body_retryable_not_user_specific(self):
        class _BadJsonResponse:
            status_code = 200

            def json(self):
                raise ValueError("not json")

        scheduler.requests.get = lambda url, params=None, timeout=None: _BadJsonResponse()
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "pending")
        # Malformed/transient body is treated like any other transient
        # Telegram hiccup -- unconditionally retryable, never a per-invitee
        # bounded-then-terminal classification.
        self.assertNotEqual(doc.get("status"), "error")
        self.assertNotEqual(doc.get("status"), "revoked")

    # 8. Definitive "kicked" membership result still revokes correctly.
    def test_200_kicked_revokes_with_confirmed_reason(self):
        self._mock_get(lambda params: _FakeResponse(200, {"ok": True, "result": {"status": "kicked"}}))
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "revoked")
        self.assertEqual(doc["revoked_reason"], "not_in_official_channel")

    # Restricted-but-still-a-member (is_member=True) must settle normally,
    # not be treated as a definitive negative verdict.
    def test_restricted_with_is_member_true_settles_normally(self):
        self._mock_get(
            lambda params: _FakeResponse(
                200, {"ok": True, "result": {"status": "restricted", "is_member": True}}
            )
        )
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "awarded")

    # Restricted with is_member explicitly not true IS a definitive
    # negative verdict and must revoke with the existing confirmed reason.
    def test_restricted_with_is_member_false_revokes(self):
        self._mock_get(
            lambda params: _FakeResponse(
                200, {"ok": True, "result": {"status": "restricted", "is_member": False}}
            )
        )
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "revoked")
        self.assertEqual(doc["revoked_reason"], "not_in_official_channel")

    def test_operational_error_produces_no_xp_no_settlement_no_revocation(self):
        record_calls = []
        scheduler._record_referral_event = lambda *a, **kw: record_calls.append(a) or True
        grant_calls = []
        scheduler.grant_xp = lambda *a, **kw: grant_calls.append(a) or True

        self._mock_get(
            lambda params: _FakeResponse(400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"})
        )
        doc = self._base_pending("community_group", retry_count=scheduler.MAX_TELEGRAM_CONFIG_RETRIES)
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        scheduler.settle_pending_referrals(batch_limit=1)

        self.assertEqual(doc["status"], "error")
        self.assertEqual(record_calls, [])
        self.assertEqual(grant_calls, [])
        # Raw status is still visible to operators via the pending_referrals
        # row itself, even though it isn't "awarded" or a confirmed failure.
        self.assertNotIn(doc["status"], {"awarded", "revoked"})

        # main.py's _map_referral_status() must keep falling back to
        # "pending" for this new, unmapped raw status -- checked via source
        # inspection rather than importing main.py, which has heavy
        # import-time side effects (real Mongo connection/index creation)
        # that make it unsafe to import in a unit test.
        with open("main.py", "r", encoding="utf-8") as fh:
            source = fh.read()
        start = source.index("def _map_referral_status(")
        body = source[start : source.index("\ndef ", start + 1)]
        self.assertNotIn('"error"', body)
        self.assertIn('return "pending"', body)

    # 8. Telegram response description is logged without exposing the bot token.
    def test_error_logged_without_bot_token_or_full_url(self):
        self._mock_get(
            lambda params: _FakeResponse(
                400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}
            )
        )
        doc = self._base_pending("community_group")
        scheduler.db = _FakeSchedulerDB([doc], self._user_doc())

        with self.assertLogs("scheduler", level="ERROR") as captured:
            scheduler.settle_pending_referrals(batch_limit=1)

        joined = "\n".join(captured.output)
        self.assertIn("chat not found", joined)
        self.assertNotIn(scheduler.BOT_TOKEN, joined)
        self.assertNotIn(f"bot{scheduler.BOT_TOKEN}", joined)
        self.assertNotIn("api.telegram.org", joined)

    # 9. One failed membership check does not abort the remaining batch.
    def test_one_bad_row_does_not_abort_batch(self):
        def fake_get(url, params=None, timeout=None):
            chat_id = params.get("chat_id")
            if chat_id == scheduler.GROUP_ID and params.get("user_id") == 22:
                raise RuntimeError("unexpected boom")
            return _FakeResponse(200, {"ok": True, "result": {"status": "member"}})

        scheduler.requests.get = fake_get

        doc_bad = self._base_pending("community_group", inviter_user_id=11, invitee_user_id=22)
        doc_bad["_id"] = 1
        doc_good = self._base_pending("community_group", inviter_user_id=33, invitee_user_id=44)
        doc_good["_id"] = 2
        doc_good["created_at_utc"] = self.fixed_now - timedelta(hours=scheduler.REFERRAL_HOLD_HOURS + 1, seconds=1)

        scheduler.db = _FakeSchedulerDB([doc_bad, doc_good], self._user_doc(22) | self._user_doc(44))

        scheduler.settle_pending_referrals(batch_limit=2)

        self.assertEqual(doc_bad["status"], "pending")
        self.assertEqual(doc_good["status"], "awarded")


if __name__ == "__main__":
    unittest.main()
