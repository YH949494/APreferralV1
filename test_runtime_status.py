import unittest
from datetime import datetime, timedelta, timezone

import runtime_status as rs


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find_one(self, filt, sort=None):
        matches = [d for d in self.docs if self._matches(d, filt)]
        if sort:
            field, direction = sort[0]
            matches.sort(key=lambda d: self._get_path(d, field), reverse=(direction < 0))
        return matches[0] if matches else None

    @staticmethod
    def _get_path(doc, path):
        val = doc
        for part in path.split("."):
            val = val.get(part) if isinstance(val, dict) else None
        return val

    def count_documents(self, filt):
        return len([d for d in self.docs if self._matches(d, filt)])

    def _matches(self, doc, filt):
        for key, cond in filt.items():
            val = self._get_path(doc, key)
            if isinstance(cond, dict):
                if "$exists" in cond:
                    exists = val is not None
                    if cond["$exists"] != exists:
                        return False
                    continue
                if "$ne" in cond and val == cond["$ne"]:
                    return False
                if "$gte" in cond and (val is None or val < cond["$gte"]):
                    return False
                if "$lte" in cond and (val is None or val > cond["$lte"]):
                    return False
            else:
                if val != cond:
                    return False
        return True


NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=timezone.utc)


class JobRuntimeStatusTests(unittest.TestCase):
    def test_disabled_is_waiting(self):
        status, note = rs.job_runtime_status(NOW, enabled=False, last_run=NOW, expected_interval_seconds=300)
        self.assertEqual(status, rs.WAITING)

    def test_never_run_is_waiting(self):
        status, note = rs.job_runtime_status(NOW, enabled=True, last_run=None, expected_interval_seconds=300)
        self.assertEqual(status, rs.WAITING)

    def test_fresh_run_is_online(self):
        last_run = NOW - timedelta(seconds=60)
        status, note = rs.job_runtime_status(NOW, enabled=True, last_run=last_run, expected_interval_seconds=300)
        self.assertEqual(status, rs.ONLINE)

    def test_stale_run_is_warning(self):
        last_run = NOW - timedelta(days=10)
        status, note = rs.job_runtime_status(NOW, enabled=True, last_run=last_run, expected_interval_seconds=300)
        self.assertEqual(status, rs.WARNING)

    def test_naive_timestamp_is_treated_as_utc(self):
        last_run = (NOW - timedelta(seconds=60)).replace(tzinfo=None)
        status, note = rs.job_runtime_status(NOW, enabled=True, last_run=last_run, expected_interval_seconds=300)
        self.assertEqual(status, rs.ONLINE)


class FlagGatedStatusTests(unittest.TestCase):
    def test_flag_off_is_waiting(self):
        status, note = rs.flag_gated_status(NOW, flag_enabled=False, evidence_at=NOW, lookback_seconds=86400)
        self.assertEqual(status, rs.WAITING)

    def test_flag_on_no_evidence_is_waiting(self):
        status, note = rs.flag_gated_status(NOW, flag_enabled=True, evidence_at=None, lookback_seconds=86400)
        self.assertEqual(status, rs.WAITING)

    def test_flag_on_recent_evidence_is_online(self):
        status, note = rs.flag_gated_status(NOW, flag_enabled=True, evidence_at=NOW - timedelta(hours=1), lookback_seconds=86400)
        self.assertEqual(status, rs.ONLINE)

    def test_flag_on_stale_evidence_is_warning(self):
        status, note = rs.flag_gated_status(NOW, flag_enabled=True, evidence_at=NOW - timedelta(days=90), lookback_seconds=86400)
        self.assertEqual(status, rs.WARNING)


class UnwiredStatusTests(unittest.TestCase):
    def test_no_evidence_is_offline(self):
        status, note = rs.unwired_status(None)
        self.assertEqual(status, rs.OFFLINE)

    def test_unexpected_evidence_is_warning(self):
        status, note = rs.unwired_status(NOW)
        self.assertEqual(status, rs.WARNING)


class BuildSchedulerHealthTests(unittest.TestCase):
    def test_online_job_with_fresh_lock(self):
        collections = {
            "scheduler_locks": _FakeCollection([
                {"_id": "tick_5min", "updatedAt": NOW - timedelta(seconds=30)},
            ]),
        }
        settings = {"pending_referral_settlement": {"enabled": True}}
        rows = rs.build_scheduler_health(collections, settings, {}, NOW)
        row = next(r for r in rows if r["key"] == "tick_5min")
        self.assertEqual(row["status"], rs.ONLINE)
        self.assertIsNotNone(row["last_run"])

    def test_disabled_job_is_waiting(self):
        collections = {"scheduler_locks": _FakeCollection([])}
        settings = {"pending_referral_settlement": {"enabled": False}}
        rows = rs.build_scheduler_health(collections, settings, {}, NOW)
        row = next(r for r in rows if r["key"] == "tick_5min")
        self.assertEqual(row["status"], rs.WAITING)

    def test_manual_scripts_are_deprecated(self):
        rows = rs.build_scheduler_health({}, {}, {}, NOW)
        row = next(r for r in rows if r["key"] == "sync_referral_counts")
        self.assertEqual(row["status"], rs.DEPRECATED)

    def test_missing_collection_degrades_gracefully(self):
        rows = rs.build_scheduler_health({}, {}, {}, NOW)
        row = next(r for r in rows if r["key"] == "monthly_vip")
        self.assertEqual(row["status"], rs.WAITING)


class BuildPmAutomationTests(unittest.TestCase):
    def test_pm5_reports_offline_not_implemented(self):
        rows = rs.build_pm_automation({}, {}, {}, NOW)
        row = next(r for r in rows if r["key"] == "pm5")
        self.assertEqual(row["status"], rs.OFFLINE)

    def test_tournament_reminder_reports_offline(self):
        rows = rs.build_pm_automation({}, {}, {}, NOW)
        row = next(r for r in rows if r["key"] == "tournament_reminder")
        self.assertEqual(row["status"], rs.OFFLINE)

    def test_pm0_online_with_recent_send(self):
        collections = {
            "users": _FakeCollection([
                {"_id": 1, "pm_sent": {"pm0_welcome": NOW - timedelta(minutes=5)}},
            ]),
        }
        rows = rs.build_pm_automation(collections, {}, {}, NOW)
        row = next(r for r in rows if r["key"] == "pm0")
        self.assertEqual(row["status"], rs.ONLINE)
        self.assertEqual(row["sent_today"], 1)

    def test_referral_near_miss_reports_unwired(self):
        rows = rs.build_pm_automation({}, {}, {}, NOW)
        row = next(r for r in rows if r["key"] == "referral_near_miss")
        self.assertEqual(row["status"], rs.OFFLINE)
        self.assertIn("never invoked", row["notes"])


class BuildQueueStatusTests(unittest.TestCase):
    def test_empty_queue_is_online(self):
        collections = {"tg_verification_queue": _FakeCollection([])}
        rows = rs.build_queue_status(collections, NOW)
        row = next(r for r in rows if r["key"] == "verification_queue")
        self.assertEqual(row["size"], 0)
        self.assertEqual(row["status"], rs.ONLINE)

    def test_pending_items_are_counted(self):
        collections = {
            "tg_verification_queue": _FakeCollection([
                {"status": "queued"}, {"status": "queued"}, {"status": "done"},
            ]),
        }
        rows = rs.build_queue_status(collections, NOW)
        row = next(r for r in rows if r["key"] == "verification_queue")
        self.assertEqual(row["size"], 2)

    def test_missing_collection_reports_unknown(self):
        rows = rs.build_queue_status({}, NOW)
        row = next(r for r in rows if r["key"] == "verification_queue")
        self.assertIsNone(row["size"])
        self.assertEqual(row["status"], rs.WAITING)


class BuildWorkerHealthTests(unittest.TestCase):
    def test_all_checks_reported(self):
        health = rs.build_worker_health(
            {}, NOW,
            mongo_ping=lambda: True,
            telegram_get_me=lambda: True,
            deployment_version="abc123",
            git_commit="deadbeef",
        )
        self.assertTrue(health["mongo_connected"])
        self.assertTrue(health["telegram_connected"])
        self.assertEqual(health["deployment_version"], "abc123")

    def test_failing_ping_reports_false_not_exception(self):
        def _boom():
            raise RuntimeError("mongo down")
        health = rs.build_worker_health({}, NOW, mongo_ping=_boom)
        self.assertFalse(health["mongo_connected"])


class BuildFeatureOverviewTests(unittest.TestCase):
    def test_produces_rows_for_every_major_area(self):
        scheduler_rows = rs.build_scheduler_health({}, {}, {}, NOW)
        pm_rows = rs.build_pm_automation({}, {}, {}, NOW)
        queue_rows = rs.build_queue_status({}, NOW)
        worker_health = rs.build_worker_health({}, NOW)
        rows = rs.build_feature_overview(scheduler_rows, pm_rows, queue_rows, worker_health, NOW)
        names = [r["feature"] for r in rows]
        self.assertIn("Onboarding / PM Automation (PM0-PM4)", names)
        self.assertIn("Tournament (banner/page/API/countdown/leaderboard)", names)
        tournament_row = next(r for r in rows if r["feature"].startswith("Tournament"))
        self.assertEqual(tournament_row["status"], rs.OFFLINE)


if __name__ == "__main__":
    unittest.main()
