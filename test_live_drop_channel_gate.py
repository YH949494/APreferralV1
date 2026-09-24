"""Regression tests for the Live Drop channel-gate audit (P1, P3).

Root cause: check_channel_subscribed() used to collapse every non-"member/
administrator/creator" outcome — including missing config, network errors,
Telegram 429s, non-200/invalid/ok=false responses, and unrecognized member
statuses — into a single `False`, which every caller then treated as "not
subscribed". That produced a false "Join Official Channel" state (and a
false claim block) for already-subscribed users whenever Telegram merely
hiccuped.

get_channel_subscription_state() now returns one of three explicit states —
"subscribed", "confirmed_not_subscribed", "verification_failed" — and only
an explicit left/kicked getChatMember status resolves to
confirmed_not_subscribed. check_channel_subscribed() stays a boolean
compatibility wrapper around it for callers that only need pass/fail
(main.py's check-in flow, the unrelated new-joiner welcome claim gate).

Also covers P3: vouchers.py's OFFICIAL_CHANNEL_ID must be the canonical
value resolved by referral_destination.py (the same one main.py/scheduler.py
use), not an independently re-parsed env var.
"""
import importlib
import os
import unittest
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask

import vouchers as m
import referral_destination


class _FakeResp:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("invalid json body")
        return self._payload


def _fake_resp(status_code, payload):
    return _FakeResp(status_code, payload)


class _FakeSubscriptionCacheCollection:
    def __init__(self):
        self.docs = {}

    def find_one(self, filt, projection=None):
        doc = self.docs.get(filt.get("_id"))
        return dict(doc) if doc is not None else None

    def update_one(self, filt, update, upsert=False):
        _id = filt.get("_id")
        doc = dict(self.docs.get(_id) or {"_id": _id})
        for key, value in (update.get("$set") or {}).items():
            doc[key] = value
        if "$setOnInsert" in update and _id not in self.docs:
            for key, value in update["$setOnInsert"].items():
                doc.setdefault(key, value)
        self.docs[_id] = doc


class ChannelSubscriptionStateTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.ctx = self.app.test_request_context()
        self.ctx.push()

        self._orig_requests_get = m.requests.get
        self._orig_sleep = m.time.sleep
        self._orig_sub_cache_col = m.subscription_cache_col
        self._orig_channel_id = m.OFFICIAL_CHANNEL_ID
        self._orig_channel_username = m.OFFICIAL_CHANNEL_USERNAME
        self._orig_bot_token = os.environ.get("BOT_TOKEN")
        self._orig_ensure_durable = m._ensure_durable_first_subscribed_at

        m.time.sleep = lambda *_a, **_kw: None  # no real backoff delay in tests
        m.subscription_cache_col = _FakeSubscriptionCacheCollection()
        m.OFFICIAL_CHANNEL_ID = -1002396761021
        m.OFFICIAL_CHANNEL_USERNAME = "@advantplayofficial"
        os.environ["BOT_TOKEN"] = "TEST_BOT_TOKEN"
        m._ensure_durable_first_subscribed_at = lambda *a, **kw: None

    def tearDown(self):
        self.ctx.pop()
        m.requests.get = self._orig_requests_get
        m.time.sleep = self._orig_sleep
        m.subscription_cache_col = self._orig_sub_cache_col
        m.OFFICIAL_CHANNEL_ID = self._orig_channel_id
        m.OFFICIAL_CHANNEL_USERNAME = self._orig_channel_username
        if self._orig_bot_token is None:
            os.environ.pop("BOT_TOKEN", None)
        else:
            os.environ["BOT_TOKEN"] = self._orig_bot_token
        m._ensure_durable_first_subscribed_at = self._orig_ensure_durable

    def _mock_get(self, fn):
        m.requests.get = fn

    # ---------------------------------------------------------------
    # Positive statuses -> subscribed
    # ---------------------------------------------------------------

    def test_member_status_is_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "member"}}))
        self.assertEqual(m.get_channel_subscription_state(1001), {"state": "subscribed"})

    def test_administrator_status_is_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "administrator"}}))
        self.assertEqual(m.get_channel_subscription_state(1002)["state"], "subscribed")

    def test_creator_status_is_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "creator"}}))
        self.assertEqual(m.get_channel_subscription_state(1003)["state"], "subscribed")

    # ---------------------------------------------------------------
    # Confirmed negative statuses -> confirmed_not_subscribed
    # ---------------------------------------------------------------

    def test_left_status_is_confirmed_not_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "left"}}))
        result = m.get_channel_subscription_state(1004)
        self.assertEqual(result["state"], "confirmed_not_subscribed")

    def test_kicked_status_is_confirmed_not_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "kicked"}}))
        result = m.get_channel_subscription_state(1005)
        self.assertEqual(result["state"], "confirmed_not_subscribed")

    # ---------------------------------------------------------------
    # Every ambiguous/failure case -> verification_failed, NEVER
    # confirmed_not_subscribed
    # ---------------------------------------------------------------

    def test_telegram_429_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(429, {"ok": False}))
        result = m.get_channel_subscription_state(1006)
        self.assertEqual(result["state"], "verification_failed")
        self.assertIn("retry_after_sec", result)

    def test_network_exception_is_verification_failed(self):
        def _raise(*_a, **_kw):
            raise requests.RequestException("connection reset")

        self._mock_get(_raise)
        result = m.get_channel_subscription_state(1007)
        self.assertEqual(result["state"], "verification_failed")
        self.assertEqual(result["reason"], "network_error")

    def test_http_400_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(400, {"ok": False}))
        result = m.get_channel_subscription_state(1008)
        self.assertEqual(result["state"], "verification_failed")

    def test_http_403_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(403, {"ok": False}))
        result = m.get_channel_subscription_state(1009)
        self.assertEqual(result["state"], "verification_failed")

    def test_http_5xx_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(500, {"ok": False}))
        result = m.get_channel_subscription_state(1010)
        self.assertEqual(result["state"], "verification_failed")

    def test_invalid_json_body_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, None))
        result = m.get_channel_subscription_state(1011)
        self.assertEqual(result["state"], "verification_failed")
        self.assertEqual(result["reason"], "invalid_response")

    def test_telegram_ok_false_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": False, "description": "Bad Request"}))
        result = m.get_channel_subscription_state(1012)
        self.assertEqual(result["state"], "verification_failed")

    def test_missing_channel_config_is_verification_failed(self):
        m.OFFICIAL_CHANNEL_ID = None
        m.OFFICIAL_CHANNEL_USERNAME = ""
        result = m.get_channel_subscription_state(1013)
        self.assertEqual(result["state"], "verification_failed")
        self.assertEqual(result["reason"], "channel_config_missing")

    def test_missing_bot_token_is_verification_failed(self):
        os.environ.pop("BOT_TOKEN", None)
        result = m.get_channel_subscription_state(1014)
        self.assertEqual(result["state"], "verification_failed")
        self.assertEqual(result["reason"], "channel_config_missing")

    def test_unrecognized_member_status_is_verification_failed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "restricted"}}))
        result = m.get_channel_subscription_state(1015)
        self.assertEqual(result["state"], "verification_failed")

    # ---------------------------------------------------------------
    # Caching behaviour
    # ---------------------------------------------------------------

    def test_positive_cache_hit_avoids_telegram_request(self):
        calls = []

        def _get(*_a, **_kw):
            calls.append(1)
            return _fake_resp(200, {"ok": True, "result": {"status": "member"}})

        self._mock_get(_get)
        first = m.get_channel_subscription_state(1016)
        self.assertEqual(first["state"], "subscribed")
        self.assertEqual(len(calls), 1)

        second = m.get_channel_subscription_state(1016)
        self.assertEqual(second["state"], "subscribed")
        self.assertEqual(len(calls), 1, "a cached positive result must skip the live Telegram lookup")

    def test_stale_negative_cache_performs_live_verification(self):
        calls = []
        uid = 1017
        # Seed a cache doc recording a previous not-subscribed determination.
        m.subscription_cache_col.docs[m._subscription_cache_key(uid)] = {
            "_id": m._subscription_cache_key(uid),
            "subscribed": False,
            "checked_at": datetime.now(timezone.utc) - timedelta(days=30),
        }

        def _get(*_a, **_kw):
            calls.append(1)
            return _fake_resp(200, {"ok": True, "result": {"status": "member"}})

        self._mock_get(_get)
        result = m.get_channel_subscription_state(uid)
        self.assertEqual(result["state"], "subscribed")
        self.assertEqual(len(calls), 1, "a stale negative cache entry must never skip live verification")

    # ---------------------------------------------------------------
    # check_channel_subscribed() boolean compatibility wrapper
    # ---------------------------------------------------------------

    def test_boolean_wrapper_true_only_when_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "member"}}))
        self.assertTrue(m.check_channel_subscribed(1018))

    def test_boolean_wrapper_false_on_confirmed_not_subscribed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(200, {"ok": True, "result": {"status": "left"}}))
        self.assertFalse(m.check_channel_subscribed(1019))

    def test_boolean_wrapper_false_on_verification_failed_stays_fail_closed(self):
        self._mock_get(lambda *a, **kw: _fake_resp(500, {"ok": False}))
        self.assertFalse(m.check_channel_subscribed(1020))


class CanonicalChannelConfigTests(unittest.TestCase):
    """P3: vouchers.py must resolve OFFICIAL_CHANNEL_ID from the same single
    source of truth as main.py/scheduler.py (referral_destination.py),
    instead of independently re-parsing OFFICIAL_CHANNEL_ID/USERNAME env
    vars, which could silently diverge."""

    def test_official_channel_id_matches_canonical_resolver(self):
        self.assertEqual(m.OFFICIAL_CHANNEL_ID, referral_destination.OFFICIAL_CHANNEL_ID)

    def test_official_channel_id_matches_expected_production_channel(self):
        self.assertEqual(m.OFFICIAL_CHANNEL_ID, -1002396761021)

    def test_official_channel_username_normalized_with_at_prefix(self):
        self.assertEqual(m.OFFICIAL_CHANNEL_USERNAME, "@advantplayofficial")

    def test_username_only_override_is_preserved_not_replaced_by_canonical_id(self):
        # Codex review on PR #498 (P1): a deployment that configures ONLY
        # OFFICIAL_CHANNEL_USERNAME (no OFFICIAL_CHANNEL_ID) must keep using
        # that username, not be silently switched to
        # referral_destination.OFFICIAL_CHANNEL_ID's hardcoded fallback ID.
        orig_id_env = os.environ.get("OFFICIAL_CHANNEL_ID")
        orig_username_env = os.environ.get("OFFICIAL_CHANNEL_USERNAME")
        try:
            os.environ.pop("OFFICIAL_CHANNEL_ID", None)
            os.environ["OFFICIAL_CHANNEL_USERNAME"] = "@customchannel"
            importlib.reload(m)
            self.assertIsNone(m.OFFICIAL_CHANNEL_ID)
            self.assertEqual(m.OFFICIAL_CHANNEL_USERNAME, "@customchannel")
        finally:
            if orig_id_env is None:
                os.environ.pop("OFFICIAL_CHANNEL_ID", None)
            else:
                os.environ["OFFICIAL_CHANNEL_ID"] = orig_id_env
            if orig_username_env is None:
                os.environ.pop("OFFICIAL_CHANNEL_USERNAME", None)
            else:
                os.environ["OFFICIAL_CHANNEL_USERNAME"] = orig_username_env
            importlib.reload(m)


if __name__ == "__main__":
    unittest.main()
