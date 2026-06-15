import unittest
from datetime import datetime, timedelta, timezone

import channel_reactivation as campaign


class _Result:
    def __init__(self, upserted_id=None, modified_count=0):
        self.upserted_id = upserted_id
        self.modified_count = modified_count


class _Cursor(list):
    def limit(self, n):
        return _Cursor(self[:n])


_MISSING = object()


def _match_value(value, condition):
    if isinstance(condition, dict):
        exists = value is not _MISSING
        if "$exists" in condition and bool(condition["$exists"]) != exists:
            return False
        if "$ne" in condition and exists and value == condition["$ne"]:
            return False
        if "$in" in condition and (not exists or value not in condition["$in"]):
            return False
        if "$lte" in condition and (not exists or not (value <= condition["$lte"])):
            return False
        return True
    return value == condition


def _matches(doc, filt):
    for key, value in (filt or {}).items():
        if key == "$and":
            if not all(_matches(doc, branch) for branch in value):
                return False
            continue
        if key == "$or":
            if not any(_matches(doc, branch) for branch in value):
                return False
            continue
        if not _match_value(doc[key] if key in doc else _MISSING, value):
            return False
    return True


class _Collection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.next_id = 1

    def create_index(self, *args, **kwargs):  # noqa: ARG002
        return kwargs.get("name") or "idx"

    def find_one(self, filt, projection=None):  # noqa: ARG002
        for doc in self.docs:
            if _matches(doc, filt):
                return dict(doc)
        return None

    def find(self, filt=None, projection=None):  # noqa: ARG002
        return _Cursor([dict(doc) for doc in self.docs if _matches(doc, filt or {})])

    def count_documents(self, filt):
        return len([doc for doc in self.docs if _matches(doc, filt)])

    def insert_one(self, doc):
        for existing in self.docs:
            if (
                existing.get("campaign_id") == doc.get("campaign_id")
                and existing.get("user_id") == doc.get("user_id")
            ):
                raise campaign.DuplicateKeyError("duplicate")
        self.docs.append(dict(doc, _id=self.next_id))
        self.next_id += 1
        return _Result(upserted_id=self.next_id - 1)

    def update_one(self, filt, update, upsert=False):
        for doc in self.docs:
            if _matches(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key, value in update.get("$setOnInsert", {}).items():
                    doc.setdefault(key, value)
                return _Result(modified_count=1)
        if not upsert:
            return _Result()
        new_doc = dict(filt)
        for key, value in update.get("$setOnInsert", {}).items():
            new_doc[key] = value
        for key, value in update.get("$set", {}).items():
            new_doc[key] = value
        new_doc.setdefault("_id", self.next_id)
        self.next_id += 1
        self.docs.append(new_doc)
        return _Result(upserted_id=new_doc["_id"], modified_count=1)


class _XPEvents(_Collection):
    pass


class _Ledger(_Collection):
    def update_one(self, filt, update, upsert=False):
        for doc in self.docs:
            if (
                doc.get("user_id") == filt.get("user_id")
                and doc.get("source") == filt.get("source")
                and doc.get("source_id") == filt.get("source_id")
            ):
                return _Result()
        return super().update_one(filt, update, upsert=upsert)

    def delete_one(self, filt):  # noqa: ARG002
        return None


class _DB:
    def __init__(self, users):
        self.users = _Collection(users)
        self.channel_reactivation_campaigns = _Collection()
        self.channel_reactivation_messages = _Collection()
        self.channel_reactivation_rewards = _Collection()
        self.xp_events = _XPEvents()
        self.xp_ledger = _Ledger()


class ChannelReactivationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 6, 15, 1, 0, tzinfo=timezone.utc)
        self.orig_daily = campaign.DAILY_SEND_LIMIT
        self.orig_minute = campaign.MINUTE_SEND_LIMIT
        campaign.DAILY_SEND_LIMIT = 1000
        campaign.MINUTE_SEND_LIMIT = 20

    def tearDown(self):
        campaign.DAILY_SEND_LIMIT = self.orig_daily
        campaign.MINUTE_SEND_LIMIT = self.orig_minute

    def test_process_sends_only_not_subscribed_not_banned_not_rewarded(self):
        db = _DB(
            [
                {"user_id": 1, "telegram_user_id": 1},
                {"user_id": 2, "telegram_user_id": 2, "banned": True},
                {"user_id": 3, "telegram_user_id": 3, "reactivation_reward_claimed": True},
                {"user_id": 4, "telegram_user_id": 4},
            ]
        )
        campaign.set_campaign_active(db, True)
        sent = []

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            membership_checker=lambda uid: (uid == 4, "status:member" if uid == 4 else "status:left"),
            send_fn=lambda uid: sent.append(uid) or (True, None),
            now_ref=self.now,
        )

        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["skipped_subscribed"], 1)
        self.assertEqual(sent, [1])
        self.assertEqual(db.channel_reactivation_messages.count_documents({"status": "sent"}), 1)

    def test_process_respects_minute_and_daily_limits(self):
        campaign.MINUTE_SEND_LIMIT = 2
        db = _DB([{"user_id": i, "telegram_user_id": i} for i in range(1, 6)])
        campaign.set_campaign_active(db, True)
        sent = []

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            batch_limit=10,
            membership_checker=lambda uid: (False, "status:left"),
            send_fn=lambda uid: sent.append(uid) or (True, None),
            now_ref=self.now,
        )

        self.assertEqual(result["sent"], 2)
        self.assertEqual(sent, [1, 2])

    def test_start_per_run_limit_100_processes_only_100_candidates(self):
        db = _DB([{"user_id": i, "telegram_user_id": i} for i in range(1, 151)])
        summary = campaign.set_campaign_active(db, True, per_run_limit=100)
        sent = []

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            membership_checker=lambda uid: (False, "status:left"),
            send_fn=lambda uid: sent.append(uid) or (True, None),
            now_ref=self.now,
        )

        self.assertEqual(summary["per_run_limit"], 100)
        self.assertEqual(result["per_run_limit"], 100)
        self.assertEqual(result["scanned"], 100)
        self.assertEqual(result["sent"], 100)
        self.assertEqual(len(sent), 100)

    def test_already_subscribed_users_excluded_before_send(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])
        campaign.set_campaign_active(db, True)
        sent = []

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            membership_checker=lambda uid: (True, "status:member"),
            send_fn=lambda uid: sent.append(uid) or (True, None),
            now_ref=self.now,
        )

        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["skipped_subscribed"], 1)
        self.assertEqual(sent, [])
        self.assertEqual(db.channel_reactivation_messages.count_documents({"status": "skipped_subscribed"}), 1)

    def test_process_skips_existing_reward_record(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])
        campaign.set_campaign_active(db, True)
        db.channel_reactivation_rewards.insert_one(
            {"campaign_id": campaign.CAMPAIGN_ID, "user_id": 10, "status": "claimed", "xp_awarded": 50}
        )

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            membership_checker=lambda uid: (False, "status:left"),
            send_fn=lambda uid: (True, None),
            now_ref=self.now,
        )

        self.assertEqual(result["sent"], 0)
        user_doc = db.users.find_one({"user_id": 10})
        self.assertTrue(user_doc["reactivation_reward_claimed"])

    def test_verify_requires_current_subscription(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])

        result = campaign.verify_reactivation_claim(
            db,
            10,
            membership_checker=lambda uid: (False, "status:left"),
            now_ref=self.now,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["code"], "not_subscribed")
        self.assertEqual(db.channel_reactivation_rewards.count_documents({}), 0)

    def test_verify_rejects_banned_user(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10, "banned": True}])

        result = campaign.verify_reactivation_claim(
            db,
            10,
            membership_checker=lambda uid: (True, "status:member"),
            now_ref=self.now,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["code"], "ineligible")
        self.assertEqual(db.channel_reactivation_rewards.count_documents({}), 0)
        self.assertEqual(db.xp_events.count_documents({}), 0)

    def test_verify_marks_pending_and_does_not_award_immediately(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])

        result = campaign.verify_reactivation_claim(
            db,
            10,
            membership_checker=lambda uid: (True, "status:member"),
            now_ref=self.now,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["code"], "pending")
        self.assertIn("Stay subscribed for 72 hours", result["message"])
        self.assertEqual(db.xp_events.count_documents({}), 0)
        reward = db.channel_reactivation_rewards.find_one({"user_id": 10})
        self.assertEqual(reward["status"], "pending")
        self.assertEqual(reward["verified_at"], self.now)
        self.assertEqual(reward["reward_due_at"], self.now + timedelta(hours=72))
        self.assertIsNone(reward["rewarded_at"])
        self.assertIsNone(reward["cancelled_at"])

    def test_pending_reward_granted_after_hold_period(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])
        campaign.verify_reactivation_claim(
            db,
            10,
            membership_checker=lambda uid: (True, "status:member"),
            now_ref=self.now,
        )
        due = self.now + timedelta(hours=72, seconds=1)

        result = campaign.process_pending_reactivation_rewards(
            db_ref=db,
            membership_checker=lambda uid: (True, "status:member"),
            now_ref=due,
        )

        self.assertEqual(result["rewarded"], 1)
        reward = db.channel_reactivation_rewards.find_one({"user_id": 10})
        self.assertEqual(reward["status"], "rewarded")
        self.assertEqual(reward["rewarded_at"], due)
        self.assertEqual(reward["xp_awarded"], 50)
        self.assertEqual(db.xp_events.count_documents({}), 1)

    def test_pending_reward_cancelled_if_user_leaves_channel(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])
        campaign.verify_reactivation_claim(
            db,
            10,
            membership_checker=lambda uid: (True, "status:member"),
            now_ref=self.now,
        )
        due = self.now + timedelta(hours=72, seconds=1)

        result = campaign.process_pending_reactivation_rewards(
            db_ref=db,
            membership_checker=lambda uid: (False, "status:left"),
            now_ref=due,
        )

        self.assertEqual(result["cancelled"], 1)
        reward = db.channel_reactivation_rewards.find_one({"user_id": 10})
        self.assertEqual(reward["status"], "cancelled")
        self.assertEqual(reward["cancelled_at"], due)
        self.assertIsNone(reward["rewarded_at"])
        self.assertEqual(reward["xp_awarded"], 0)
        self.assertEqual(db.xp_events.count_documents({}), 0)

    def test_blocked_user_marked_failed_blocked(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10}])
        campaign.set_campaign_active(db, True)

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            membership_checker=lambda uid: (False, "status:left"),
            send_fn=lambda uid: (False, "Forbidden: bot was blocked by the user"),
            now_ref=self.now,
        )

        self.assertEqual(result["failed_blocked"], 1)
        self.assertEqual(db.channel_reactivation_messages.count_documents({"status": "failed_blocked"}), 1)
        user_doc = db.users.find_one({"user_id": 10})
        self.assertEqual(user_doc["reactivation_failure_reason"], "Forbidden: bot was blocked by the user")
        self.assertEqual(user_doc["reactivation_failed_blocked_at"], self.now)

    def test_blocked_user_not_retried(self):
        db = _DB([{"user_id": 10, "telegram_user_id": 10, "reactivation_failed_blocked_at": self.now}])
        campaign.set_campaign_active(db, True)
        sent = []

        result = campaign.process_reactivation_campaign(
            db_ref=db,
            membership_checker=lambda uid: (False, "status:left"),
            send_fn=lambda uid: sent.append(uid) or (True, None),
            now_ref=self.now + timedelta(minutes=1),
        )

        self.assertEqual(result["scanned"], 0)
        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
