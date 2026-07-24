import logging
import unittest
from datetime import datetime, timezone

import scheduler


class _FakeReferralEventsWithRows:
    """Fake referral_events collection whose aggregate() returns pre-baked rows."""

    def __init__(self, rows):
        self._rows = rows

    def aggregate(self, pipeline):
        return list(self._rows)


class _FakeUsers:
    def __init__(self, bulk_write_error=None):
        self.docs = {}
        self._bulk_write_error = bulk_write_error
        self.bulk_write_calls = []

    def update_many(self, filt, update):
        if isinstance(update, list):
            stage = update[0].get("$set", {}) if update else {}
            modified = 0
            for doc in self.docs.values():
                for k, v in stage.items():
                    if isinstance(v, str) and v.startswith("$"):
                        doc[k] = doc.get(v[1:])
                    else:
                        doc[k] = v
                modified += 1
            return type("Result", (), {"modified_count": modified})()

        if "$set" in update:
            for doc in self.docs.values():
                for k, v in update["$set"].items():
                    doc[k] = v
        if "$inc" in update:
            for doc in self.docs.values():
                for k, v in update["$inc"].items():
                    doc[k] = doc.get(k, 0) + v
        return type("Result", (), {"modified_count": len(self.docs)})()

    def bulk_write(self, updates, ordered=False):
        self.bulk_write_calls.append(updates)
        if self._bulk_write_error is not None:
            raise self._bulk_write_error
        for op in updates:
            user_id = op._filter["user_id"]
            doc = self.docs.setdefault(user_id, {"user_id": user_id})
            for k, v in op._doc.get("$set", {}).items():
                doc[k] = v


class _FakeDB:
    def __init__(self, rows, bulk_write_error=None):
        self.referral_events = _FakeReferralEventsWithRows(rows)
        self.users = _FakeUsers(bulk_write_error=bulk_write_error)


def _row(uid, weekly=0, monthly=0, total=0):
    return {"_id": uid, "weekly_referrals": weekly, "monthly_referrals": monthly, "total_referrals": total}


class _SnapshotLoggingTestBase(unittest.TestCase):
    def _run_with_db(self, fake_db):
        original_db = scheduler.db
        original_heartbeat = scheduler._write_snapshot_heartbeat
        scheduler.db = fake_db
        scheduler._write_snapshot_heartbeat = lambda source, ts: None
        try:
            with self.assertLogs("scheduler", level="DEBUG") as captured:
                scheduler.settle_referral_snapshots()
        finally:
            scheduler.db = original_db
            scheduler._write_snapshot_heartbeat = original_heartbeat
        return captured.output


class NoPerUserInfoLogTests(_SnapshotLoggingTestBase):
    def test_successful_publish_does_not_log_one_info_line_per_user(self):
        rows = [_row(uid, weekly=1, monthly=1, total=1) for uid in range(1, 51)]
        fake_db = _FakeDB(rows)
        output = self._run_with_db(fake_db)

        per_user_lines = [
            line for line in output
            if "[SCHED][REFERRAL_SNAPSHOT]" in line and "uid=" in line and "[DONE]" not in line
            and "[NEGATIVE]" not in line
        ]
        self.assertEqual(per_user_lines, [])

    def test_final_batch_summary_is_logged_exactly_once(self):
        rows = [_row(uid, weekly=1, monthly=1, total=1) for uid in range(1, 6)]
        fake_db = _FakeDB(rows)
        output = self._run_with_db(fake_db)

        done_lines = [line for line in output if "[SCHED][REFERRAL_SNAPSHOT][DONE]" in line]
        self.assertEqual(len(done_lines), 1)
        self.assertIn("scanned=5", done_lines[0])
        self.assertIn("updated=5", done_lines[0])
        self.assertIn("errors=0", done_lines[0])
        self.assertIn("negative_rows=0", done_lines[0])
        self.assertIn("weekly_sum=5", done_lines[0])
        self.assertIn("monthly_sum=5", done_lines[0])
        self.assertIn("total_sum=5", done_lines[0])


class NegativeRowLoggingTests(_SnapshotLoggingTestBase):
    def test_negative_rows_produce_warning_logs(self):
        rows = [_row(1, weekly=-1, monthly=-6, total=-36)]
        fake_db = _FakeDB(rows)
        output = self._run_with_db(fake_db)

        warning_lines = [
            line for line in output
            if line.startswith("WARNING") and "[SCHED][REFERRAL_SNAPSHOT][NEGATIVE]" in line
        ]
        self.assertEqual(len(warning_lines), 1)
        self.assertIn("uid=1", warning_lines[0])
        self.assertIn("weekly=-1", warning_lines[0])
        self.assertIn("monthly=-6", warning_lines[0])
        self.assertIn("total=-36", warning_lines[0])

    def test_negative_warning_examples_are_capped_at_20_but_summary_has_full_count(self):
        rows = [_row(uid, weekly=-1, monthly=0, total=-1) for uid in range(1, 38)]  # 37 negative rows
        fake_db = _FakeDB(rows)
        output = self._run_with_db(fake_db)

        negative_warning_lines = [
            line for line in output
            if line.startswith("WARNING") and "[SCHED][REFERRAL_SNAPSHOT][NEGATIVE]" in line
        ]
        self.assertEqual(len(negative_warning_lines), 20)

        done_lines = [line for line in output if "[SCHED][REFERRAL_SNAPSHOT][DONE]" in line]
        self.assertEqual(len(done_lines), 1)
        self.assertIn("negative_rows=37", done_lines[0])
        self.assertIn("negative_examples_logged=20", done_lines[0])
        self.assertIn("min_weekly=-1", done_lines[0])
        self.assertIn("min_total=-1", done_lines[0])


class DatabaseFailureLoggingTests(_SnapshotLoggingTestBase):
    def test_bulk_write_failure_logs_error_and_aborts_before_publish(self):
        # A batch-write failure must not fall through to publish: every
        # user's *_referrals_next field was already reset to 0 before the
        # write, so publishing after a failed write would wipe out valid
        # referral counts. The function should log ERROR and re-raise,
        # leaving the live snapshot fields untouched.
        rows = [_row(uid, weekly=1, monthly=1, total=1) for uid in range(1, 4)]
        fake_db = _FakeDB(rows, bulk_write_error=RuntimeError("write failed"))

        original_db = scheduler.db
        original_heartbeat = scheduler._write_snapshot_heartbeat
        scheduler.db = fake_db
        scheduler._write_snapshot_heartbeat = lambda source, ts: None
        try:
            with self.assertLogs("scheduler", level="DEBUG") as captured:
                with self.assertRaises(RuntimeError):
                    scheduler.settle_referral_snapshots()
        finally:
            scheduler.db = original_db
            scheduler._write_snapshot_heartbeat = original_heartbeat

        error_lines = [
            line for line in captured.output
            if line.startswith("ERROR") and "[SCHED][REFERRAL_SNAPSHOT][WRITE_FAILED]" in line
        ]
        self.assertEqual(len(error_lines), 1)

        # publish never ran: no [DONE] summary, and live referral fields
        # were never overwritten with the reset-to-zero "next" values.
        done_lines = [line for line in captured.output if "[SCHED][REFERRAL_SNAPSHOT][DONE]" in line]
        self.assertEqual(done_lines, [])
        for doc in fake_db.users.docs.values():
            self.assertNotIn("weekly_referrals", doc)


class SnapshotCalculationUnchangedTests(_SnapshotLoggingTestBase):
    def test_stored_snapshot_values_unchanged_by_logging_patch(self):
        rows = [_row(1, weekly=2, monthly=3, total=4)]
        fake_db = _FakeDB(rows)
        self._run_with_db(fake_db)

        self.assertEqual(fake_db.users.docs[1]["weekly_referrals"], 2)
        self.assertEqual(fake_db.users.docs[1]["monthly_referrals"], 3)
        self.assertEqual(fake_db.users.docs[1]["total_referrals"], 4)


class ApSchedulerNoiseTests(unittest.TestCase):
    def test_main_configures_apscheduler_executor_logger_to_warning(self):
        # main.py has heavy import-time side effects (real Mongo connection,
        # index creation) that make importing it in a unit test unsafe, so we
        # check the configuration statement is present in source instead of
        # importing the module.
        with open("main.py", "r", encoding="utf-8") as fh:
            source = fh.read()
        self.assertIn(
            'logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)',
            source,
        )

    def test_warning_level_suppresses_info_but_keeps_errors_visible(self):
        # Exercises the same effect main.py's configuration line has: routine
        # "Running job"/"executed successfully" INFO noise is dropped, while
        # job exceptions logged at ERROR remain visible.
        test_logger = logging.getLogger("apscheduler.executors.default")
        original_level = test_logger.level
        test_logger.setLevel(logging.WARNING)
        try:
            with self.assertLogs(test_logger, level="WARNING") as captured:
                test_logger.info("Running job foo")
                test_logger.info("Job foo executed successfully")
                test_logger.error("Job foo raised an exception")
            self.assertEqual(len(captured.output), 1)
            self.assertIn("raised an exception", captured.output[0])
        finally:
            test_logger.setLevel(original_level)


if __name__ == "__main__":
    unittest.main()
