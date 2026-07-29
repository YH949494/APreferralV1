import unittest
from datetime import datetime, timedelta, timezone

import vouchers as vouchers_module
from vouchers import (
    check_rejoin_buffer_for_pooled_claim,
    get_rejoin_buffer_settings,
    set_rejoin_buffer_settings,
)


class FakeUsersCollection:
    def __init__(self, docs=None):
        self.docs = {d["user_id"]: dict(d) for d in (docs or [])}

    def find_one(self, filt, projection=None):
        uid = filt.get("user_id")
        doc = self.docs.get(uid)
        if not doc:
            return None
        if not projection:
            return dict(doc)
        return {k: doc.get(k) for k in projection if k in doc}


class FakeSettingsCollection:
    def __init__(self, doc=None):
        self.doc = dict(doc) if doc else None

    def find_one(self, filt):
        return dict(self.doc) if self.doc is not None else None

    def update_one(self, filt, update, upsert=False):
        self.doc = dict(update.get("$set", {}))
        self.doc["_id"] = filt.get("_id")


class RejoinBufferSettingsMixin:
    def setUp(self):
        self.orig_users = vouchers_module.users_collection
        self.orig_settings = vouchers_module.app_settings_col

    def tearDown(self):
        vouchers_module.users_collection = self.orig_users
        vouchers_module.app_settings_col = self.orig_settings

    def _set_settings(self, **kwargs):
        doc = dict(vouchers_module.DEFAULT_REJOIN_BUFFER_SETTINGS)
        doc.update(kwargs)
        vouchers_module.app_settings_col = FakeSettingsCollection(doc)


class RejoinBufferSettingsHelperTests(RejoinBufferSettingsMixin, unittest.TestCase):
    def test_defaults_when_no_doc_stored(self):
        vouchers_module.app_settings_col = FakeSettingsCollection(None)
        settings = get_rejoin_buffer_settings()
        self.assertEqual(settings, {"mode": "disabled", "hours": 12, "test_user_ids": []})

    def test_defaults_when_lookup_raises(self):
        class BoomCollection:
            def find_one(self, filt):
                raise RuntimeError("Database not initialized")

        vouchers_module.app_settings_col = BoomCollection()
        settings = get_rejoin_buffer_settings()
        self.assertEqual(settings["mode"], "disabled")

    def test_invalid_mode_falls_back_to_disabled(self):
        self._set_settings(mode="not_a_real_mode")
        settings = get_rejoin_buffer_settings()
        self.assertEqual(settings["mode"], "disabled")

    def test_invalid_hours_falls_back_to_default(self):
        self._set_settings(mode="enabled", hours=-5)
        settings = get_rejoin_buffer_settings()
        self.assertEqual(settings["hours"], 12)

    def test_set_and_get_round_trip(self):
        vouchers_module.app_settings_col = FakeSettingsCollection(None)
        set_rejoin_buffer_settings(
            mode="test_users_only",
            hours=6,
            test_user_ids=[111, 222],
            updated_by="admin1",
        )
        settings = get_rejoin_buffer_settings()
        self.assertEqual(settings, {"mode": "test_users_only", "hours": 6, "test_user_ids": [111, 222]})

    def test_test_user_ids_normalized_to_ints_and_deduped(self):
        self._set_settings(mode="test_users_only", test_user_ids=["5", 5, "7"])
        settings = get_rejoin_buffer_settings()
        self.assertEqual(settings["test_user_ids"], [5, 7])


class RejoinBufferHelperTests(RejoinBufferSettingsMixin, unittest.TestCase):
    def test_mode_disabled_allows_claim_even_with_active_buffer(self):
        now = datetime.now(timezone.utc)
        self._set_settings(mode="disabled")
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 3, "rejoin_buffer_until": now + timedelta(hours=5)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(3, now)
        self.assertTrue(result["ok"])

    def test_default_mode_is_disabled(self):
        now = datetime.now(timezone.utc)
        vouchers_module.app_settings_col = FakeSettingsCollection(None)
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 3, "rejoin_buffer_until": now + timedelta(hours=5)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(3, now)
        self.assertTrue(result["ok"])

    def test_mode_enabled_first_time_subscriber_no_buffer_allows_claim(self):
        self._set_settings(mode="enabled")
        vouchers_module.users_collection = FakeUsersCollection([{"user_id": 1}])
        result = check_rejoin_buffer_for_pooled_claim(1, datetime.now(timezone.utc))
        self.assertTrue(result["ok"])

    def test_mode_enabled_missing_user_doc_allows_claim(self):
        self._set_settings(mode="enabled")
        vouchers_module.users_collection = FakeUsersCollection([])
        result = check_rejoin_buffer_for_pooled_claim(999, datetime.now(timezone.utc))
        self.assertTrue(result["ok"])

    def test_mode_enabled_expired_buffer_allows_claim(self):
        now = datetime.now(timezone.utc)
        self._set_settings(mode="enabled")
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 2, "rejoin_buffer_until": now - timedelta(hours=1)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(2, now)
        self.assertTrue(result["ok"])

    def test_mode_enabled_active_buffer_blocks_claim(self):
        now = datetime.now(timezone.utc)
        self._set_settings(mode="enabled")
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 3, "rejoin_buffer_until": now + timedelta(hours=5)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(3, now)
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "rejoin_buffer_active")
        self.assertEqual(result["reason"], "rejoin_buffer_active")
        self.assertGreater(result["retry_after_sec"], 0)
        self.assertLessEqual(result["retry_after_sec"], 5 * 3600)
        self.assertEqual(result["buffer_until"], (now + timedelta(hours=5)).isoformat())
        self.assertIn("rejoined @AdvantPlayOfficial", result["message"])

    def test_mode_test_users_only_blocks_listed_user(self):
        now = datetime.now(timezone.utc)
        self._set_settings(mode="test_users_only", test_user_ids=[3])
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 3, "rejoin_buffer_until": now + timedelta(hours=5)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(3, now)
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "rejoin_buffer_active")

    def test_mode_test_users_only_allows_non_listed_user(self):
        now = datetime.now(timezone.utc)
        self._set_settings(mode="test_users_only", test_user_ids=[999])
        vouchers_module.users_collection = FakeUsersCollection([
            {"user_id": 3, "rejoin_buffer_until": now + timedelta(hours=5)},
        ])
        result = check_rejoin_buffer_for_pooled_claim(3, now)
        self.assertTrue(result["ok"])

    def test_none_uid_allows_claim_regardless_of_mode(self):
        self._set_settings(mode="enabled")
        vouchers_module.users_collection = FakeUsersCollection([])
        result = check_rejoin_buffer_for_pooled_claim(None, datetime.now(timezone.utc))
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
