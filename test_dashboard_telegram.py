"""Unit tests for the worker-cached Telegram member-count helpers.

These cover the substance of the dashboard fix: the worker refreshes both
counts into a cache document (preserving the last known value when one Telegram
call fails), and the dashboard reads that cached document only — it never calls
Telegram, because ``read_member_count`` takes a plain cached dict and no fetcher
at all. The full /api/admin/dashboard/summary Flask endpoint is not exercised
here because importing main.py requires a live MongoDB and builds the Telegram
application (the existing suite never imports main.py for the same reason); the
endpoint simply reads admin_cache and maps ``read_member_count`` results into
users{}.
"""

import inspect
import unittest
from datetime import datetime, timedelta, timezone

import dashboard_telegram
from dashboard_telegram import (
    MEMBER_COUNT_STALE_AFTER_S,
    TELEGRAM_COUNTS_DOC_ID,
    read_member_count,
    refresh_member_counts,
)

OFFICIAL = -1002396761021
CHATROOM = -1002743212540
NOW = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)

METRICS = [
    ("official_channel_subscribers", OFFICIAL),
    ("chatroom_members", CHATROOM),
]


class RefreshMemberCountsTests(unittest.TestCase):
    def test_writes_both_counts_successfully(self):
        def fetcher(chat_id):
            return {OFFICIAL: 24830, CHATROOM: 5120}[chat_id]

        doc = refresh_member_counts(METRICS, fetcher, existing={}, now=NOW)

        self.assertEqual(doc["updated_at"], NOW)
        official = doc["counts"]["official_channel_subscribers"]
        chatroom = doc["counts"]["chatroom_members"]
        self.assertEqual(official["count"], 24830)
        self.assertTrue(official["ok"])
        self.assertIsNone(official["error"])
        self.assertEqual(official["updated_at"], NOW)
        self.assertEqual(official["chat_id"], OFFICIAL)
        self.assertEqual(chatroom["count"], 5120)
        self.assertTrue(chatroom["ok"])

    def test_one_failure_preserves_previous_count(self):
        # Seed a previous successful refresh for both metrics.
        prev_ts = NOW - timedelta(hours=1)
        existing = {
            "updated_at": prev_ts,
            "counts": {
                "official_channel_subscribers": {
                    "chat_id": OFFICIAL, "count": 24000,
                    "updated_at": prev_ts, "ok": True, "error": None,
                },
                "chatroom_members": {
                    "chat_id": CHATROOM, "count": 5000,
                    "updated_at": prev_ts, "ok": True, "error": None,
                },
            },
        }

        def fetcher(chat_id):
            if chat_id == OFFICIAL:
                return 24830
            raise RuntimeError("telegram timeout")

        doc = refresh_member_counts(METRICS, fetcher, existing=existing, now=NOW)

        official = doc["counts"]["official_channel_subscribers"]
        chatroom = doc["counts"]["chatroom_members"]
        # Official refreshed.
        self.assertEqual(official["count"], 24830)
        self.assertTrue(official["ok"])
        # Chatroom failed: previous count preserved, marked not-ok with error.
        self.assertEqual(chatroom["count"], 5000)
        self.assertFalse(chatroom["ok"])
        self.assertIn("telegram timeout", chatroom["error"])
        self.assertEqual(chatroom["updated_at"], prev_ts)  # last success time kept
        self.assertEqual(chatroom["error_at"], NOW)

    def test_failure_with_no_previous_keeps_none(self):
        def fetcher(_chat_id):
            raise RuntimeError("boom")

        doc = refresh_member_counts(METRICS, fetcher, existing={}, now=NOW)
        for entry in doc["counts"].values():
            self.assertIsNone(entry["count"])
            self.assertFalse(entry["ok"])

    def test_missing_chat_id_is_isolated_failure(self):
        def fetcher(_chat_id):
            return 999

        doc = refresh_member_counts(
            [("official_channel_subscribers", None)], fetcher, existing={}, now=NOW
        )
        entry = doc["counts"]["official_channel_subscribers"]
        self.assertFalse(entry["ok"])
        self.assertIsNone(entry["count"])

    def test_refresh_does_not_raise_on_fetcher_error(self):
        def fetcher(_chat_id):
            raise RuntimeError("boom")

        # Must not propagate — each metric is isolated.
        doc = refresh_member_counts(METRICS, fetcher, existing={}, now=NOW)
        self.assertEqual(set(doc["counts"]), {m[0] for m in METRICS})


class ReadMemberCountTests(unittest.TestCase):
    def _entry(self, *, count=24830, age_s=60, ok=True):
        return {
            "chat_id": OFFICIAL,
            "count": count,
            "updated_at": NOW - timedelta(seconds=age_s),
            "ok": ok,
            "error": None,
        }

    def test_fresh_ok_is_not_stale(self):
        res = read_member_count(self._entry(age_s=60), now=NOW)
        self.assertEqual(res["count"], 24830)
        self.assertFalse(res["stale"])
        self.assertEqual(res["status"], "ok")
        self.assertIsNotNone(res["cached_at"])

    def test_missing_cache_is_stale(self):
        for entry in (None, {}, {"count": None}):
            res = read_member_count(entry, now=NOW)
            self.assertTrue(res["stale"])
            self.assertEqual(res["status"], "missing")
            self.assertIsNone(res["cached_at"])

    def test_older_than_two_hours_is_stale(self):
        res = read_member_count(
            self._entry(age_s=MEMBER_COUNT_STALE_AFTER_S + 1), now=NOW
        )
        self.assertTrue(res["stale"])
        self.assertEqual(res["status"], "stale")
        # Last known count still surfaced for the UI.
        self.assertEqual(res["count"], 24830)

    def test_just_under_two_hours_is_fresh(self):
        res = read_member_count(
            self._entry(age_s=MEMBER_COUNT_STALE_AFTER_S - 1), now=NOW
        )
        self.assertFalse(res["stale"])

    def test_not_ok_is_stale_even_when_recent(self):
        res = read_member_count(self._entry(age_s=10, ok=False), now=NOW)
        self.assertTrue(res["stale"])
        self.assertEqual(res["status"], "stale")
        self.assertEqual(res["count"], 24830)

    def test_iso_string_updated_at_is_accepted(self):
        entry = self._entry(age_s=60)
        entry["updated_at"] = (NOW - timedelta(seconds=60)).isoformat()
        res = read_member_count(entry, now=NOW)
        self.assertFalse(res["stale"])

    def test_reader_takes_no_fetcher_so_cannot_call_telegram(self):
        # The dashboard read path is structurally decoupled from Telegram:
        # read_member_count accepts only a cached dict — there is no callable
        # parameter through which a network call could be made.
        params = list(inspect.signature(read_member_count).parameters)
        self.assertEqual(params[0], "entry")
        self.assertNotIn("fetcher", params)


class ContractTests(unittest.TestCase):
    def test_doc_id_constant(self):
        self.assertEqual(TELEGRAM_COUNTS_DOC_ID, "telegram_member_counts")

    def test_stale_threshold_is_two_hours(self):
        self.assertEqual(MEMBER_COUNT_STALE_AFTER_S, 7200)

    def test_module_exposes_no_member_count_cached(self):
        # The web-side fetch helper has been removed; reading is cache-only.
        self.assertFalse(hasattr(dashboard_telegram, "get_member_count_cached"))


if __name__ == "__main__":
    unittest.main()
