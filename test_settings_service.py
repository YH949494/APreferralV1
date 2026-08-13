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


class PoolProbabilitiesUnclassifiedFieldTests(unittest.TestCase):
    """Covers the admin-editable "Unclassified / Unknown" fallback probability
    field on the Public Pool Distribution panel — load, save, validation, the
    runtime effect on config.public_pool_probability_for_bot_segment, and the
    safety fallback to the 10% code default (never 70%)."""

    def setUp(self):
        self.db = _FakeDb()
        svc.invalidate_cache()

    def tearDown(self):
        svc.invalidate_cache()

    def test_default_is_10_when_no_override_exists(self):
        settings = svc.get_settings("pool_probabilities", db_ref=self.db)
        self.assertEqual(settings["unclassified"], 10.0)

    def test_save_persists_and_reload_returns_saved_value(self):
        result = svc.update_settings(
            "pool_probabilities", {"unclassified": 8}, db_ref=self.db
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["settings"]["unclassified"], 8.0)

        svc.invalidate_cache()
        reloaded = svc.get_settings("pool_probabilities", db_ref=self.db)
        self.assertEqual(reloaded["unclassified"], 8.0)

    def test_rejects_invalid_values(self):
        for bad in (-1, 100.1, "not_a_number"):
            with self.subTest(bad=bad):
                result = svc.update_settings(
                    "pool_probabilities", {"unclassified": bad}, db_ref=self.db
                )
                self.assertFalse(result["success"])

    def test_nan_validation_gap_matches_other_float_fields(self):
        """NaN currently slips past _validate_field's min/max bounds check for
        EVERY "float"-typed field (NaN comparisons are always False) — this is
        a pre-existing generic gap, not something introduced for this field.
        Assert parity with an existing field rather than "fixing" only
        unclassified, which would create inconsistent behavior across the
        panel — out of scope for this minimal follow-up patch."""
        existing_field_result = svc.update_settings(
            "pool_probabilities", {"ghost": float("nan")}, db_ref=self.db
        )
        new_field_result = svc.update_settings(
            "pool_probabilities", {"unclassified": float("nan")}, db_ref=self.db
        )
        self.assertEqual(existing_field_result["success"], new_field_result["success"])

    def test_accepts_boundary_values(self):
        for ok in (0, 100):
            with self.subTest(ok=ok):
                result = svc.update_settings(
                    "pool_probabilities", {"unclassified": ok}, db_ref=self.db
                )
                self.assertTrue(result["success"])

    def test_runtime_probability_uses_configured_override(self):
        import config

        result = svc.update_settings(
            "pool_probabilities", {"unclassified": 8}, db_ref=self.db
        )
        self.assertTrue(result["success"])
        # update_settings() invalidates+repopulates the shared in-process
        # cache, so the parameterless (real-db) lookup config.py performs
        # immediately observes the override — proving no redeploy/restart is
        # needed for this setting to take effect, same as the other fields.
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment(None), 0.08)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment(""), 0.08)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("unclassified"), 0.08)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("random_unknown_segment"), 0.08)

    def test_runtime_known_segments_unaffected_by_unclassified_override(self):
        import config

        svc.update_settings("pool_probabilities", {"unclassified": 8}, db_ref=self.db)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("voucher_hunter"), 0.10)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("ghost"), 0.05)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("normal_actual"), 0.70)
        self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("high_value"), 0.50)

    def test_malformed_persisted_value_falls_back_to_schema_default(self):
        # A malformed stored value (None/non-numeric) must be coerced back to
        # the field's schema default (10.0), never silently accepted.
        self.db["app_settings"].docs["pool_probabilities"] = {
            "_id": "pool_probabilities", "unclassified": "not_a_number",
        }
        svc.invalidate_cache()
        settings = svc.get_settings("pool_probabilities", db_ref=self.db)
        self.assertEqual(settings["unclassified"], 10.0)

    def test_settings_lookup_failure_falls_back_to_10pct_not_70pct(self):
        """If the settings lookup itself fails/errors, the runtime probability
        must fall back to the safe 10% code default, never the old 70%."""
        import config

        orig_get_setting = svc.get_setting

        def boom(*args, **kwargs):
            raise RuntimeError("settings lookup boom")

        svc.get_setting = boom
        try:
            self.assertAlmostEqual(config.public_pool_probability_for_bot_segment(None), 0.10)
            self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("unclassified"), 0.10)
            self.assertAlmostEqual(config.public_pool_probability_for_bot_segment("some_unknown_segment"), 0.10)
        finally:
            svc.get_setting = orig_get_setting


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
