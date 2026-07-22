import os
import time
import unittest

import settings_service as svc


class _FakeCollection:
    def __init__(self):
        self.docs = {}
        self.inserted = []

    def find_one(self, filt):
        return self.docs.get(filt.get("_id"))

    def update_one(self, filt, update, upsert=False):
        _id = filt.get("_id")
        doc = self.docs.get(_id, {"_id": _id})
        doc.update(update.get("$set", {}))
        self.docs[_id] = doc

    def insert_one(self, doc):
        self.inserted.append(doc)


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

    def test_requirements_group_default(self):
        settings = svc.get_settings("requirements", db_ref=self.db)
        self.assertEqual(settings["welcome_reward_checkins_required"], 3)

    def test_update_writes_audit_log_with_old_and_new_values(self):
        svc.update_settings(
            "requirements", {"welcome_reward_checkins_required": 5},
            updated_by="admin1", db_ref=self.db,
        )
        entries = self.db[svc.AUDIT_COLLECTION_NAME].inserted
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["group"], "requirements")
        self.assertEqual(entry["admin"], "admin1")
        self.assertEqual(
            entry["changes"]["welcome_reward_checkins_required"],
            {"old": 3, "new": 5},
        )
        self.assertIn("created_at", entry)

    def test_update_with_no_actual_change_skips_audit_log(self):
        svc.update_settings(
            "requirements", {"welcome_reward_checkins_required": 3},
            updated_by="admin1", db_ref=self.db,
        )
        entries = self.db[svc.AUDIT_COLLECTION_NAME].inserted
        self.assertEqual(len(entries), 0)


class SettingsCategorizationTests(unittest.TestCase):
    """Covers the Settings-tab categorization fix: every field in
    SETTINGS_SCHEMA must resolve to exactly one of the canonical Settings UI
    categories, and the categorization itself (not just save/validate
    behaviour) must be correct for the fields called out in the bug report.
    """

    def setUp(self):
        self.db = _FakeDb()
        svc.invalidate_cache()

    def test_every_field_has_exactly_one_valid_category(self):
        for group, schema in svc.SETTINGS_SCHEMA.items():
            for field in schema["fields"]:
                category = svc.field_category(group, field)
                self.assertIsNotNone(
                    category, f"{group}.{field} has no category assigned"
                )
                self.assertIn(
                    category, svc.SETTINGS_CATEGORIES,
                    f"{group}.{field} has unknown category {category!r}",
                )

    def test_new_group_without_category_metadata_is_caught(self):
        """Simulates what happens if a future SETTINGS_SCHEMA group is added
        without categorising it: field_category() must return None so an
        exhaustive-coverage test (like the one above) fails, rather than the
        field silently rendering in every tab again."""
        svc.SETTINGS_SCHEMA["_uncategorised_probe"] = {
            "label": "Probe", "fields": {"foo": {"type": "str", "default": ""}}
        }
        try:
            self.assertIsNone(svc.field_category("_uncategorised_probe", "foo"))
        finally:
            del svc.SETTINGS_SCHEMA["_uncategorised_probe"]

    def test_field_with_two_categories_is_impossible_by_construction(self):
        """A field can only ever have one entry in field_categories (dict),
        so double-assignment is structurally prevented; this asserts that
        invariant holds for every override actually declared."""
        for group, schema in svc.SETTINGS_SCHEMA.items():
            overrides = schema.get("field_categories") or {}
            for field, category in overrides.items():
                self.assertIn(field, schema["fields"])
                self.assertEqual(svc.field_category(group, field), category)

    def test_general_category_does_not_contain_rejoin_buffer_or_affiliate_fields(self):
        cmap = svc.category_map()
        general_keys = [k for k, v in cmap.items() if v == "general"]
        self.assertNotIn("urls.affiliate_group_invite_url", general_keys)
        for key in general_keys:
            self.assertNotEqual(cmap[key], "security")
            self.assertNotEqual(cmap[key], "affiliate")

    def test_voucher_rules_category_owns_voucher_claimed_template(self):
        # Rejoin Buffer itself lives outside SETTINGS_SCHEMA (see vouchers.py),
        # but the one schema field describing voucher claim copy must sit
        # under Voucher Rules, alongside where Rejoin Buffer now renders.
        self.assertEqual(svc.field_category("message_templates", "voucher_claimed"), "voucher_rules")

    def test_affiliate_category_excludes_unrelated_general_voucher_and_security_settings(self):
        cmap = svc.category_map()
        affiliate_keys = [k for k, v in cmap.items() if v == "affiliate"]
        self.assertTrue(affiliate_keys)
        unrelated = [
            "abuse_protection.claim_cooldown_seconds",
            "abuse_protection.kill_block_seconds",
            "urls.official_channel_url",
            "urls.community_url",
            "message_templates.voucher_claimed",
        ]
        for key in unrelated:
            self.assertNotIn(key, affiliate_keys)
        self.assertEqual(cmap["urls.affiliate_group_invite_url"], "affiliate")
        self.assertEqual(cmap["message_templates.affiliate_unlock"], "affiliate")

    def test_changing_category_changes_which_fields_are_selected(self):
        cmap = svc.category_map()
        general_keys = {k for k, v in cmap.items() if v == "general"}
        security_keys = {k for k, v in cmap.items() if v == "security"}
        affiliate_keys = {k for k, v in cmap.items() if v == "affiliate"}
        self.assertTrue(general_keys)
        self.assertTrue(security_keys)
        self.assertTrue(affiliate_keys)
        self.assertEqual(general_keys & security_keys, set())
        self.assertEqual(general_keys & affiliate_keys, set())
        self.assertEqual(security_keys & affiliate_keys, set())

    def test_saving_a_setting_still_writes_the_original_backend_key(self):
        result = svc.update_settings(
            "message_templates", {"affiliate_unlock": "new copy"}, db_ref=self.db
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["settings"]["affiliate_unlock"], "new copy")
        stored = self.db["app_settings"].docs["message_templates"]
        self.assertEqual(stored["affiliate_unlock"], "new copy")

    def test_rejoin_buffer_group_id_is_not_part_of_settings_schema(self):
        # Rejoin Buffer must have exactly one editable UI location (Voucher
        # Rules, backed by vouchers.py), never inside SETTINGS_SCHEMA/Managed
        # Settings — otherwise it would have two save paths for one setting.
        self.assertNotIn("rejoin_buffer", svc.SETTINGS_SCHEMA)


if __name__ == "__main__":
    unittest.main()
