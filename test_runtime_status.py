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

    def aggregate(self, pipeline):
        docs = self.docs
        for stage in pipeline:
            if "$match" in stage:
                filt = stage["$match"]
                docs = [d for d in docs if self._matches(d, filt)]
            elif "$group" in stage:
                group = stage["$group"]
                key_field = group["_id"]
                assert isinstance(key_field, str) and key_field.startswith("$")
                key_field = key_field[1:]
                counts: dict = {}
                for d in docs:
                    key = self._get_path(d, key_field)
                    counts[key] = counts.get(key, 0) + 1
                docs = [{"_id": k, "count": v} for k, v in counts.items()]
        return docs

    def _matches(self, doc, filt):
        for key, cond in filt.items():
            if key == "$or":
                if not any(self._matches(doc, sub) for sub in cond):
                    return False
                continue
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
                if "$in" in cond and val not in cond["$in"]:
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


class WelcomeCheckinStageRowTests(unittest.TestCase):
    """The 4 new PM Automation rows (Welcome Check-in D2/D2-followup/D3 +
    Unlock Celebration) must derive last_run_age_s from persisted runtime
    data (admin_cache run-stats, not the TTL'd scheduler_locks doc alone),
    attribute failed/skipped counts to the correct normalized ``stage``, and
    never report Online purely because the job is registered."""

    FLAGS = {"welcome_journey": True}

    def _collections(self, *, admin_cache_doc=None, lock_doc=None, events=None):
        return {
            "admin_cache": _FakeCollection([admin_cache_doc] if admin_cache_doc else []),
            "scheduler_locks": _FakeCollection([lock_doc] if lock_doc else []),
            "welcome_analytics_events": _FakeCollection(events or []),
        }

    def test_stale_heartbeat_is_offline(self):
        collections = self._collections(
            lock_doc={"_id": "welcome_progress_reminders", "updatedAt": NOW - timedelta(hours=5)},
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        row = next(r for r in rows if r["key"] == "welcome_checkin_d2")
        self.assertEqual(row["status"], rs.OFFLINE)

    def test_recent_run_zero_eligible_is_waiting(self):
        collections = self._collections(
            admin_cache_doc={"_id": "welcome_run_stats:welcome_progress_reminders", "lastRunAt": NOW - timedelta(minutes=5)},
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        row = next(r for r in rows if r["key"] == "welcome_checkin_d2")
        self.assertEqual(row["status"], rs.WAITING)
        self.assertEqual(row["sent_today"], 0)
        self.assertIsNotNone(row["last_run_age_s"])

    def test_recent_run_with_failures_is_degraded(self):
        collections = self._collections(
            admin_cache_doc={"_id": "welcome_run_stats:welcome_progress_reminders", "lastRunAt": NOW - timedelta(minutes=5)},
            events=[
                {"event": "welcome_reminder_failed", "stage": "20h", "status": "failed", "reason": "timeout", "created_at": NOW},
            ],
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        row = next(r for r in rows if r["key"] == "welcome_checkin_d2")
        self.assertEqual(row["status"], rs.WARNING)
        self.assertEqual(row["failed_today"], 1)

    def test_recent_run_with_send_is_online(self):
        collections = self._collections(
            admin_cache_doc={"_id": "welcome_run_stats:welcome_progress_reminders", "lastRunAt": NOW - timedelta(minutes=5)},
            events=[
                {"event": "welcome_reminder_20h_sent", "stage": "20h", "status": "sent", "created_at": NOW},
            ],
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        row = next(r for r in rows if r["key"] == "welcome_checkin_d2")
        self.assertEqual(row["status"], rs.ONLINE)
        self.assertEqual(row["sent_today"], 1)

    def test_last_run_age_survives_expired_scheduler_lock(self):
        # scheduler_locks doc has a 3600s TTL (acquire_scheduler_lock) and
        # can be reaped by Mongo; admin_cache run-stats are retained
        # separately and must be preferred.
        collections = self._collections(
            admin_cache_doc={"_id": "welcome_run_stats:welcome_progress_reminders", "lastRunAt": NOW - timedelta(minutes=5)},
            lock_doc=None,
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        row = next(r for r in rows if r["key"] == "welcome_checkin_d2")
        self.assertAlmostEqual(row["last_run_age_s"], 300, delta=2)

    def test_skip_event_only_attributed_to_its_own_stage(self):
        collections = self._collections(
            admin_cache_doc={"_id": "welcome_run_stats:welcome_progress_reminders", "lastRunAt": NOW - timedelta(minutes=5)},
            events=[
                {"event": "welcome_reminder_skipped", "stage": "20h", "status": "skipped", "reason": "multi_account", "created_at": NOW},
            ],
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        d2_row = next(r for r in rows if r["key"] == "welcome_checkin_d2")
        followup_row = next(r for r in rows if r["key"] == "welcome_checkin_d2_followup")
        d3_row = next(r for r in rows if r["key"] == "welcome_checkin_d3")
        self.assertEqual(d2_row["skipped_today"], 1)
        self.assertEqual(d2_row["skip_breakdown"], {"multi_account": 1})
        self.assertEqual(followup_row["skipped_today"], 0)
        self.assertEqual(d3_row["skipped_today"], 0)

    def test_unlock_celebration_row_uses_completed_event(self):
        collections = self._collections(
            events=[
                {"event": "welcome_completed", "stage": "completed", "status": "sent", "created_at": NOW - timedelta(minutes=10)},
            ],
        )
        rows = rs.build_pm_automation(collections, self.FLAGS, {}, NOW)
        row = next(r for r in rows if r["key"] == "welcome_checkin_unlock")
        self.assertEqual(row["sent_today"], 1)
        self.assertIsNotNone(row["last_sent"])


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

    def test_affiliate_queues_use_real_ledger_statuses(self):
        collections = {
            "affiliate_ledger": _FakeCollection([
                {"status": "PENDING_REVIEW"},
                {"status": "PENDING_MANUAL"},
                {"status": "PENDING_EOM"},
                {"status": "SIMULATED_PENDING"},
                {"status": "ISSUED"},
                {"status": "pending"},  # legacy lowercase value should NOT be double-counted
            ]),
        }
        rows = rs.build_queue_status(collections, NOW)
        voucher_row = next(r for r in rows if r["key"] == "voucher_queue")
        affiliate_row = next(r for r in rows if r["key"] == "affiliate_queue")
        self.assertEqual(voucher_row["size"], 4)
        self.assertEqual(affiliate_row["size"], 4)

    def test_pm_queue_counts_pm2_through_pm4(self):
        collections = {
            "users": _FakeCollection([
                {"_id": 1, "pm2_due_at_utc": NOW - timedelta(minutes=5)},  # PM1 already sent, PM2 due & unsent
                {"_id": 2, "pm4_due_at_utc": NOW - timedelta(minutes=5)},  # PM4 due & unsent
                {"_id": 3, "pm1_due_at_utc": NOW - timedelta(minutes=5), "pm1_sent_at_utc": NOW},  # fully sent
            ]),
        }
        rows = rs.build_queue_status(collections, NOW)
        pm_row = next(r for r in rows if r["key"] == "pm_queue")
        self.assertEqual(pm_row["size"], 2)


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


class BuildWelcomeJourneyRuntimeTests(unittest.TestCase):
    """Heartbeat/status comes from ``scheduler_locks``; run stats/history are
    persisted to ``admin_cache`` (doc "welcome_run_stats:<job>") specifically
    because ``scheduler_locks`` has a TTL index and would silently drop this
    history whenever a job stops running long enough for its lock to expire."""

    def _collections(self, lock_doc=None, stats_doc=None):
        locks = [lock_doc] if lock_doc else []
        cache = [stats_doc] if stats_doc else []
        return {
            "scheduler_locks": _FakeCollection(locks),
            "admin_cache": _FakeCollection(cache),
        }

    def test_scheduler_block_uses_persisted_duration(self):
        lock_doc = {
            "_id": "welcome_progress_reminders",
            "updatedAt": NOW - timedelta(minutes=10),
        }
        stats_doc = {
            "_id": "welcome_run_stats:welcome_progress_reminders",
            "lastRunDurationS": 2.1,
        }
        collections = self._collections(lock_doc, stats_doc)
        scheduler_rows = rs.build_scheduler_health(collections, {}, {}, NOW)
        block = rs.build_welcome_journey_scheduler(collections, scheduler_rows, NOW)
        self.assertEqual(block["reminders"]["status"], rs.ONLINE)
        self.assertEqual(block["reminders"]["last_run_duration_s"], 2.1)
        self.assertIsNotNone(block["reminders"]["next_run"])

    def test_scheduler_status_survives_stats_doc_eviction(self):
        # Even if the admin_cache stats doc were somehow missing, the
        # heartbeat-derived status (from scheduler_locks) must still work —
        # this is the exact TTL-eviction scenario the fix guards against.
        lock_doc = {
            "_id": "welcome_progress_reminders",
            "updatedAt": NOW - timedelta(minutes=10),
        }
        collections = self._collections(lock_doc, stats_doc=None)
        scheduler_rows = rs.build_scheduler_health(collections, {}, {}, NOW)
        block = rs.build_welcome_journey_scheduler(collections, scheduler_rows, NOW)
        self.assertEqual(block["reminders"]["status"], rs.ONLINE)
        self.assertIsNone(block["reminders"]["last_run_duration_s"])

    def test_last_run_reads_persisted_stats(self):
        stats_doc = {
            "_id": "welcome_run_stats:welcome_progress_reminders",
            "lastRunAt": NOW,
            "lastRunDurationS": 2.1,
            "lastRunStats": {
                "scanned": 100,
                "eligible_20h": 10,
                "reminder_20h_sent": 9,
                "send_failed": 1,
                "blocked_users": 2,
                "skipped_abuse": 3,
                "skip_breakdown": {"already_claimed": 2, "bot_blocked": 1},
            },
        }
        last_run = rs.build_welcome_journey_last_run(self._collections(stats_doc=stats_doc))
        self.assertEqual(last_run["users_scanned"], 100)
        self.assertEqual(last_run["reminders_20h_sent"], 9)
        self.assertEqual(last_run["telegram_failed"], 1)
        self.assertEqual(last_run["skipped_users"]["already_claimed"], 2)
        self.assertEqual(last_run["skipped_users"]["bot_blocked"], 1)
        self.assertEqual(last_run["skipped_users"]["total"], 3)

    def test_last_run_defaults_when_no_run_yet(self):
        last_run = rs.build_welcome_journey_last_run(self._collections())
        self.assertEqual(last_run["users_scanned"], 0)
        self.assertIsNone(last_run["at"])

    def test_recent_runs_latest_first_capped(self):
        # $push with $slice:-20 appends in chronological order, so index 0 is
        # oldest and the last element is the most recent run.
        runs = [{"at": NOW - timedelta(hours=24 - i), "duration_s": 1.0, "stats": {"scanned": i}} for i in range(25)]
        stats_doc = {"_id": "welcome_run_stats:welcome_progress_reminders", "recentRuns": runs}
        rows = rs.build_welcome_journey_recent_runs(self._collections(stats_doc=stats_doc))
        self.assertEqual(len(rows), 20)
        self.assertEqual(rows[0]["users_scanned"], 24)

    def test_alerts_fire_when_scheduler_stale(self):
        scheduler = {"reminders": {"status": rs.WARNING, "last_run": (NOW - timedelta(hours=3)).isoformat()}}
        last_run = {"users_scanned": 0, "eligible_20h": 0, "eligible_28h": 0, "eligible_day3": 0,
                    "reminders_20h_sent": 0, "reminders_28h_sent": 0, "day3_reminders_sent": 0,
                    "telegram_failed": 0, "blocked_users": 0}
        alerts = rs.build_welcome_journey_alerts(NOW, scheduler=scheduler, last_run=last_run, funnel_summary=None)
        self.assertTrue(any("has not run" in a["message"] for a in alerts))

    def test_alerts_fire_when_eligible_but_none_sent(self):
        scheduler = {"reminders": {"status": rs.ONLINE, "last_run": NOW.isoformat()}}
        last_run = {"users_scanned": 10, "eligible_20h": 5, "eligible_28h": 0, "eligible_day3": 0,
                    "reminders_20h_sent": 0, "reminders_28h_sent": 0, "day3_reminders_sent": 0,
                    "telegram_failed": 0, "blocked_users": 0}
        alerts = rs.build_welcome_journey_alerts(NOW, scheduler=scheduler, last_run=last_run, funnel_summary=None)
        self.assertTrue(any("reminders sent = 0" in a["message"] for a in alerts))

    def test_no_alerts_when_healthy(self):
        scheduler = {"reminders": {"status": rs.ONLINE, "last_run": NOW.isoformat()}}
        last_run = {"users_scanned": 10, "eligible_20h": 5, "eligible_28h": 0, "eligible_day3": 0,
                    "reminders_20h_sent": 5, "reminders_28h_sent": 0, "day3_reminders_sent": 0,
                    "telegram_failed": 0, "blocked_users": 0}
        alerts = rs.build_welcome_journey_alerts(NOW, scheduler=scheduler, last_run=last_run, funnel_summary=None)
        self.assertEqual(alerts, [])


if __name__ == "__main__":
    unittest.main()
