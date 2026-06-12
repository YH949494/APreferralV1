"""Unit tests for the dashboard Telegram member-count helper.

These cover the substance of the dashboard summary patch: two chats cached
independently, graceful stale fallback per chat, and a missing count never
breaking the lookup. The full /api/admin/dashboard/summary Flask endpoint is
not exercised here because importing main.py requires a live MongoDB and
builds the Telegram application (the existing suite never imports main.py for
the same reason); the endpoint simply maps these results into users{}.
"""

import unittest

from dashboard_telegram import get_member_count_cached

OFFICIAL = -1002396761021
CHATROOM = -1002743212540


def _raises(_chat_id):
    raise RuntimeError("telegram timeout")


class GetMemberCountCachedTests(unittest.TestCase):
    def test_both_counts_returned_successfully(self):
        cache = {}
        calls = {}

        def fetcher(chat_id):
            calls[chat_id] = calls.get(chat_id, 0) + 1
            return {OFFICIAL: 24830, CHATROOM: 5120}[chat_id]

        official = get_member_count_cached(OFFICIAL, fetcher, cache=cache, now=1000.0)
        chatroom = get_member_count_cached(CHATROOM, fetcher, cache=cache, now=1000.0)

        self.assertEqual(official["count"], 24830)
        self.assertFalse(official["stale"])
        self.assertIsNotNone(official["cached_at"])
        self.assertEqual(chatroom["count"], 5120)
        self.assertFalse(chatroom["stale"])
        # Cached independently by chat_id.
        self.assertEqual(set(cache.keys()), {OFFICIAL, CHATROOM})

    def test_official_channel_fails_uses_cached_value(self):
        # Seed a cached official-channel value, then expire it and fail.
        cache = {OFFICIAL: {"count": 24000, "ts": 0.0}}
        res = get_member_count_cached(OFFICIAL, _raises, cache=cache, ttl=3600, now=10_000.0)
        self.assertEqual(res["count"], 24000)
        self.assertTrue(res["stale"])
        self.assertIsNotNone(res["cached_at"])

    def test_chatroom_fails_uses_cached_value(self):
        cache = {CHATROOM: {"count": 5000, "ts": 0.0}}
        res = get_member_count_cached(CHATROOM, _raises, cache=cache, ttl=3600, now=10_000.0)
        self.assertEqual(res["count"], 5000)
        self.assertTrue(res["stale"])

    def test_one_count_missing_does_not_break(self):
        # Official succeeds; chatroom has no cache and the API fails.
        cache = {}

        def fetcher(chat_id):
            if chat_id == OFFICIAL:
                return 24830
            raise RuntimeError("boom")

        official = get_member_count_cached(OFFICIAL, fetcher, cache=cache, now=1000.0)
        chatroom = get_member_count_cached(CHATROOM, fetcher, cache=cache, now=1000.0)

        self.assertEqual(official["count"], 24830)
        self.assertFalse(official["stale"])
        # Missing count degrades gracefully — no exception, count is None.
        self.assertIsNone(chatroom["count"])
        self.assertTrue(chatroom["stale"])
        self.assertIsNone(chatroom["cached_at"])

    def test_fresh_cache_served_without_calling_fetcher(self):
        cache = {OFFICIAL: {"count": 24830, "ts": 1000.0}}

        def fetcher(_chat_id):
            raise AssertionError("fetcher must not be called when cache is fresh")

        res = get_member_count_cached(OFFICIAL, fetcher, cache=cache, ttl=3600, now=1500.0)
        self.assertEqual(res["count"], 24830)
        self.assertFalse(res["stale"])

    def test_expired_cache_triggers_refetch(self):
        cache = {OFFICIAL: {"count": 100, "ts": 0.0}}
        res = get_member_count_cached(OFFICIAL, lambda _c: 200, cache=cache, ttl=3600, now=10_000.0)
        self.assertEqual(res["count"], 200)
        self.assertFalse(res["stale"])
        self.assertEqual(cache[OFFICIAL]["count"], 200)

    def test_falsy_chat_id_returns_none(self):
        res = get_member_count_cached(None, _raises, cache={})
        self.assertIsNone(res["count"])
        self.assertTrue(res["stale"])


if __name__ == "__main__":
    unittest.main()
