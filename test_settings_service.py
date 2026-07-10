import os
import time
import unittest

import settings_service as svc


class _FakeCollection:
    def __init__(self):
        self.docs = {}

    def find_one(self, filt):
        return self.docs.get(filt.get("_id"))

    def update_one(self, filt, update, upsert=False):
        _id = filt.get("_id")
        doc = self.docs.get(_id, {"_id": _id})
        doc.update(update.get("$set", {}))
        self.docs[_id] = doc


class _FakeDb(dict):
    def __getitem__(self, name):
        if name not in self:
            super().__setitem__(name, _FakeCollection())
        return super().__getitem__(name)


class SettingsServiceTests(unittest.TestCase):
    def setUp(self):
        self.db = _FakeDb()
        svc.invalidate_cache()

    def test_defaults_when_nothing_stored(self):
        settings = svc.get_settings("abuse_protection", db_ref=self.db)
        self.assertEqual(settings["claim_cooldown_seconds"], 180)
        self.assertEqual(settings["kill_block_seconds"], 86400)

    def test_env_fallback_used_before_hardcoded_default(self):
        os.environ["CLAIM_COOLDOWN_SECONDS"] = "999"
        try:
            svc.invalidate_cache()
            settings = svc.get_settings("abuse_protection", db_ref=self.db)
            self.assertEqual(settings["claim_cooldown_seconds"], 999)
        finally:
            del os.environ["CLAIM_COOLDOWN_SECONDS"]
            svc.invalidate_cache()

    def test_update_persists_and_validates_bounds(self):
        result = svc.update_settings(
            "abuse_protection", {"claim_cooldown_seconds": 45}, db_ref=self.db
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["settings"]["claim_cooldown_seconds"], 45)

        bad = svc.update_settings(
            "abuse_protection", {"claim_cooldown_seconds": -5}, db_ref=self.db
        )
        self.assertFalse(bad["success"])

    def test_update_rejects_unknown_field(self):
        result = svc.update_settings("abuse_protection", {"nope": 1}, db_ref=self.db)
        self.assertFalse(result["success"])
        self.assertEqual(result["reason"], "unknown_field:nope")

    def test_cache_ttl_avoids_repeat_reads(self):
        calls = {"n": 0}
        orig_find_one = self.db["app_settings"].find_one

        def counting_find_one(filt):
            calls["n"] += 1
            return orig_find_one(filt)

        self.db["app_settings"].find_one = counting_find_one
        svc.get_settings("abuse_protection", db_ref=self.db)
        svc.get_settings("abuse_protection", db_ref=self.db)
        self.assertEqual(calls["n"], 1)

    def test_scheduler_job_partial_update_merges(self):
        result = svc.update_settings(
            "scheduler", {"xp_snapshot": {"enabled": False}}, db_ref=self.db
        )
        self.assertTrue(result["success"])
        job = result["settings"]["xp_snapshot"]
        self.assertFalse(job["enabled"])
        self.assertEqual(job["cron"], "0 0 * * 1")  # untouched field kept

    def test_unknown_group_raises(self):
        with self.assertRaises(KeyError):
            svc.get_settings("not_a_group", db_ref=self.db)


if __name__ == "__main__":
    unittest.main()
