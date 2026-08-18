"""Regression tests for segment-authority P0 fixes:

1. The legacy Google Sheet writer (bot_segment_sync.py) must default to
   disabled when BOT_SEGMENT_SYNC_ENABLED is unset, so Databot's
   segment_sync_job remains the only automatic writer of
   users.for_bot_segment.
2. Live campaign eligibility (campaign_builder._resolve_segment_user_ids)
   must resolve users from the canonical source (users.for_bot_segment,
   written only by Databot's sync) and must NOT be swayed by the
   shadow-only backend_segment_engine.py classifier
   (backend_segment_snapshots), whose thresholds differ from canonical.
"""

import importlib
import os
import unittest
from unittest.mock import patch

import database
import campaign_builder
import settings_service
from test_campaign_builder import FakeDB


class LegacyWriterDefaultDisabledTests(unittest.TestCase):
    def test_default_is_disabled_when_env_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BOT_SEGMENT_SYNC_ENABLED", None)
            import config
            importlib.reload(config)
            try:
                self.assertFalse(
                    config.BOT_SEGMENT_SYNC_ENABLED,
                    "Legacy sheet-sync writer must default to disabled "
                    "when BOT_SEGMENT_SYNC_ENABLED is unset, so a missing "
                    "env var cannot re-enable a competing writer.",
                )
            finally:
                importlib.reload(config)

    def test_explicit_opt_in_still_works_for_manual_use(self):
        with patch.dict(os.environ, {"BOT_SEGMENT_SYNC_ENABLED": "1"}):
            import config
            importlib.reload(config)
            try:
                self.assertTrue(config.BOT_SEGMENT_SYNC_ENABLED)
            finally:
                importlib.reload(config)

    def test_untouched_settings_db_also_defaults_the_scheduler_job_off(self):
        """The scheduler's live Settings-DB toggle (Admin Dashboard ->
        Scheduler) merges over a SETTINGS_SCHEMA default for every job,
        including bot_segment_sheet_sync. On a fresh/untouched deployment
        with nothing explicitly saved to that settings doc,
        get_setting('scheduler', 'bot_segment_sheet_sync') returns the
        SCHEMA default outright -- so that default must also be `enabled:
        False`, or the env-var-level fix above is silently overridden and
        the legacy writer runs anyway. (Codex review finding on PR #408.)"""

        class _FakeCollection:
            def find_one(self, filt):
                return None

        class _FakeDb(dict):
            def __getitem__(self, name):
                if name not in self:
                    super().__setitem__(name, _FakeCollection())
                return super().__getitem__(name)

        settings_service.invalidate_cache()
        job_cfg = settings_service.get_setting(
            "scheduler", "bot_segment_sheet_sync", db_ref=_FakeDb()
        )
        self.assertIsInstance(job_cfg, dict)
        self.assertFalse(
            job_cfg.get("enabled"),
            "bot_segment_sheet_sync's SETTINGS_SCHEMA default must be "
            "enabled=False so an untouched deployment's live scheduler "
            "toggle agrees with BOT_SEGMENT_SYNC_ENABLED's off-by-default.",
        )


class CanonicalEligibilityTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = FakeDB()
        self._orig_db = database._db
        database._db = self.fake_db

    def tearDown(self):
        database._db = self._orig_db

    def test_eligibility_follows_canonical_segment_not_shadow(self):
        """A player whose canonical segment is voucher_hunter but whose
        shadow (backend_segment_snapshots) classification differs must
        still be resolved as voucher_hunter for campaign eligibility."""
        # Canonical: written only by Databot's segment_sync_job.
        self.fake_db["users"].insert_one({
            "user_id": 501,
            "for_bot_segment": "voucher_hunter",
            "for_bot_segment_normalized": "voucher_hunter",
        })
        # Shadow-only classifier disagrees; must be ignored for eligibility.
        self.fake_db["backend_segment_snapshots"].insert_one({
            "user_id": 501,
            "backend_segment": "high_value",
            "snapshot_week": "2026-W30",
        })

        ids = campaign_builder._resolve_segment_user_ids(self.fake_db, ["voucher_hunter"])
        self.assertEqual(ids, [501])

        ids_high_value = campaign_builder._resolve_segment_user_ids(self.fake_db, ["high_value"])
        self.assertEqual(
            ids_high_value, [],
            "campaign eligibility must not read backend_segment_snapshots "
            "(shadow-only classifier); high_value here only exists in the "
            "shadow collection, not on users.for_bot_segment_normalized.",
        )

    def test_shadow_collection_alone_yields_no_eligible_users(self):
        self.fake_db["backend_segment_snapshots"].insert_one({
            "user_id": 777,
            "backend_segment": "voucher_hunter",
            "snapshot_week": "2026-W30",
        })
        ids = campaign_builder._resolve_segment_user_ids(self.fake_db, ["voucher_hunter"])
        self.assertEqual(ids, [])

    def test_multi_account_voucher_hunter_resolves_operationally_as_voucher_hunter(self):
        """Case 2 from the effective-segment spec: canonical behavioral
        segment is high_value, but multi_account_voucher_hunter=True must
        make this user resolve into the voucher_hunter campaign audience
        and NOT into the high_value audience -- without for_bot_segment_normalized
        itself ever being changed."""
        self.fake_db["users"].insert_one({
            "user_id": 601,
            "for_bot_segment": "High Value",
            "for_bot_segment_normalized": "high_value",
            "multi_account_voucher_hunter": True,
        })
        self.assertEqual(campaign_builder._resolve_segment_user_ids(self.fake_db, ["voucher_hunter"]), [601])
        self.assertEqual(campaign_builder._resolve_segment_user_ids(self.fake_db, ["high_value"]), [])
        # Canonical field is untouched.
        stored = self.fake_db["users"].find_one({"user_id": 601})
        self.assertEqual(stored["for_bot_segment_normalized"], "high_value")

    def test_cluster_members_all_resolve_voucher_hunter_regardless_of_canonical_segment(self):
        """Case 5: >3 linked identities each flagged multi_account_voucher_hunter
        must all resolve as voucher_hunter for campaign eligibility, regardless
        of their individual canonical behavioral segments, and none may leak
        into another segment's audience."""
        cluster = [
            {"user_id": 701, "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": True},
            {"user_id": 702, "for_bot_segment_normalized": "normal_actual", "multi_account_voucher_hunter": True},
            {"user_id": 703, "for_bot_segment_normalized": "low_value", "multi_account_voucher_hunter": True},
            {"user_id": 704, "for_bot_segment_normalized": "voucher_hunter", "multi_account_voucher_hunter": True},
        ]
        for doc in cluster:
            self.fake_db["users"].insert_one(doc)
        # A non-cluster control user who should stay in their own segment.
        self.fake_db["users"].insert_one(
            {"user_id": 705, "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": False}
        )

        vh_ids = campaign_builder._resolve_segment_user_ids(self.fake_db, ["voucher_hunter"])
        self.assertEqual(sorted(vh_ids), [701, 702, 703, 704])

        for segment in ("high_value", "normal_actual", "low_value"):
            ids = campaign_builder._resolve_segment_user_ids(self.fake_db, [segment])
            self.assertNotIn(701, ids)
            self.assertNotIn(702, ids)
            self.assertNotIn(703, ids)

        self.assertEqual(campaign_builder._resolve_segment_user_ids(self.fake_db, ["high_value"]), [705])


if __name__ == "__main__":
    unittest.main()
