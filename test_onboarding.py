import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from flask import Flask

import asyncio
import importlib
import os

import onboarding
import scheduler
import vouchers
from app_context import set_scheduler
from onboarding import record_first_checkin, record_first_mywin
from pymongo.errors import DuplicateKeyError

class FakeUpdateResult:
    def __init__(self, modified_count=0, upserted_id=None):
        self.modified_count = modified_count
        self.upserted_id = upserted_id

class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, key, direction):  # noqa: ARG002
        self._docs.sort(key=lambda doc: doc.get(key))
        return self

    def limit(self, count):
        self._docs = self._docs[:count]
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeUsersCollection:
    def __init__(self):
        self.docs = {}
        self._next_id = 1

    def _match(self, doc, filt):
        for key, value in filt.items():
            if isinstance(value, dict) and "$exists" in value:
                exists = key in doc
                if value["$exists"] != exists:
                    return False
            elif isinstance(value, dict) and "$lte" in value:
                if key not in doc or doc.get(key) > value["$lte"]:
                    return False
            elif isinstance(value, dict) and "$ne" in value:
                if doc.get(key) == value["$ne"]:
                    return False                    
            elif doc.get(key) != value:
                return False
        return True

    def find_one(self, filt, projection=None):
        for doc in self.docs.values():
            if self._match(doc, filt):
                if projection is None:
                    return dict(doc)
                projected = {}
                include_id = projection.get("_id", 1)
                for key, flag in projection.items():
                    if not flag or key == "_id":
                        continue
                    if key in doc:
                        projected[key] = doc[key]
                if include_id and "_id" in doc:
                    projected["_id"] = doc["_id"]
                return projected
        return None

    def find(self, filt, projection=None):
        matches = []
        for doc in self.docs.values():
            if self._match(doc, filt):
                if projection is None:
                    matches.append(dict(doc))
                    continue
                projected = {}
                include_id = projection.get("_id", 1)
                for key, flag in projection.items():
                    if not flag or key == "_id":
                        continue
                    if key in doc:
                        projected[key] = doc[key]
                if include_id and "_id" in doc:
                    projected["_id"] = doc["_id"]
                matches.append(projected)
        return FakeCursor(matches)

    def count_documents(self, filt):
        return sum(1 for doc in self.docs.values() if self._match(doc, filt))
    
    def update_one(self, filt, update, upsert=False):
        for doc in self.docs.values():
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    self._apply_set(doc, key, value)
                for key, value in update.get("$setOnInsert", {}).items():
                    if key not in doc:
                        self._apply_set(doc, key, value)                    
                for key in update.get("$unset", {}).keys():
                    self._apply_unset(doc, key)                        
                return FakeUpdateResult(modified_count=1)
        if not upsert:
            return FakeUpdateResult(modified_count=0)
        new_doc = {"user_id": filt.get("user_id")}
        for key, value in update.get("$set", {}).items():
            self._apply_set(new_doc, key, value)
        for key, value in update.get("$setOnInsert", {}).items():
            self._apply_set(new_doc, key, value)            
        for key in update.get("$unset", {}).keys():
            self._apply_unset(new_doc, key)            
        new_doc["_id"] = self._next_id
        self._next_id += 1
        self.docs[new_doc["_id"]] = new_doc
        return FakeUpdateResult(modified_count=1, upserted_id=new_doc["_id"])

    def _apply_set(self, doc, key, value):
        if "." not in key:
            doc[key] = value
            return
        parts = key.split(".")
        cursor = doc
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = value

    def _apply_unset(self, doc, key):
        if "." not in key:
            doc.pop(key, None)
            return
        parts = key.split(".")
        cursor = doc
        for part in parts[:-1]:
            cursor = cursor.get(part, {})
        if isinstance(cursor, dict):
            cursor.pop(parts[-1], None)
    
    def insert_one(self, doc):
        user_id = doc.get("user_id")
        if user_id is not None:
            for existing in self.docs.values():
                if existing.get("user_id") == user_id:
                    raise DuplicateKeyError("duplicate user_id")
        new_doc = dict(doc)
        new_doc.setdefault("_id", self._next_id)
        if new_doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate _id")
        self._next_id = max(self._next_id + 1, new_doc["_id"] + 1)
        self.docs[new_doc["_id"]] = new_doc
        return new_doc

class FakeScheduler:
    def __init__(self):
        self.jobs = {}
        self.add_calls = []
        self.remove_calls = []

    def add_job(self, func, trigger, id=None, replace_existing=False, kwargs=None, **job_kwargs):  # noqa: ARG002
        self.jobs[id] = {"func": func, "trigger": trigger, "kwargs": kwargs, **job_kwargs}
        self.add_calls.append(id)

    def remove_job(self, job_id):
        self.jobs.pop(job_id, None)
        self.remove_calls.append(job_id)


class FakeBot:
    def __init__(self):
        self.sent = []

    def send_message(self, chat_id=None, text=None, reply_markup=None):  # noqa: ARG002
        self.sent.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return True

class FakeEventsCollection:
    def __init__(self):
        self.docs = {}

    def insert_one(self, doc):
        doc_id = doc.get("_id")
        if doc_id in self.docs:
            raise DuplicateKeyError("duplicate _id")
        self.docs[doc_id] = dict(doc)
        return doc


class OnboardingTests(unittest.TestCase):
    def setUp(self):
        self.users = FakeUsersCollection()
        self.scheduler = FakeScheduler()
        self.events = FakeEventsCollection()        
        onboarding.users_collection = self.users
        onboarding.onboarding_events = self.events        
        set_scheduler(self.scheduler)

    def test_e0_idempotent_api_visible(self):
        app = Flask(__name__)
        uid = 123
        parsed = {"user": {"id": uid, "username": "tester"}}
        fixed_now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with (
            patch.object(vouchers, "verify_telegram_init_data", return_value=(True, parsed, None)),
            patch.object(vouchers, "user_visible_drops", return_value=([], None)),
            patch.object(vouchers, "users_collection", self.users),
            patch.object(onboarding, "now_utc", return_value=fixed_now),
        ):
            with app.test_request_context("/vouchers/visible?init_data=abc"):
                vouchers.api_visible()
            with app.test_request_context("/vouchers/visible?init_data=abc"):
                vouchers.api_visible()

        user_doc = self.users.find_one({"user_id": uid})
        self.assertIsNotNone(user_doc)
        self.assertIn("onboarding_started_at", user_doc)
        self.assertEqual(self.scheduler.add_calls.count(f"pm1:{uid}"), 1)
        trigger = self.scheduler.jobs[f"pm1:{uid}"]["trigger"]
        self.assertEqual(trigger.run_date, fixed_now + timedelta(hours=2))

    def test_record_onboarding_existing_user_id_with_different_id(self):
        uid = 555
        fixed_now = datetime(2024, 1, 4, tzinfo=timezone.utc)
        self.users.insert_one({"_id": 999, "user_id": uid, "status": "Normal"})
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            created = onboarding.record_onboarding_start(uid, source="visible")
        user_doc = self.users.find_one({"user_id": uid})
        self.assertTrue(created)
        self.assertEqual(user_doc.get("onboarding_started_at"), fixed_now)
        self.assertEqual(self.scheduler.add_calls.count(f"pm1:{uid}"), 1)
        self.assertIn(f"E0:{uid}", self.events.docs)

    def test_record_onboarding_idempotent(self):
        uid = 554
        fixed_now = datetime(2024, 1, 4, tzinfo=timezone.utc)
        self.users.insert_one({"_id": 500, "user_id": uid, "status": "Normal"})
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            created = onboarding.record_onboarding_start(uid, source="visible")
            created_again = onboarding.record_onboarding_start(uid, source="visible")
        self.assertTrue(created)
        self.assertFalse(created_again)

    def test_record_onboarding_race_duplicate_insert(self):
        uid = 777
        fixed_now = datetime(2024, 1, 5, tzinfo=timezone.utc)

        class RaceUsersCollection(FakeUsersCollection):
            def __init__(self, skip_uid):
                super().__init__()
                self._skip_uid_once = skip_uid

            def find_one(self, filt, projection=None):
                if filt.get("user_id") == self._skip_uid_once:
                    self._skip_uid_once = None
                    return None
                return super().find_one(filt, projection)

        race_users = RaceUsersCollection(uid)
        race_users.insert_one({"user_id": uid, "status": "Normal"})
        onboarding.users_collection = race_users
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            created = onboarding.record_onboarding_start(uid, source="visible")
        user_doc = race_users.find_one({"user_id": uid})
        self.assertTrue(created)
        self.assertEqual(user_doc.get("onboarding_started_at"), fixed_now)
        self.assertEqual(self.scheduler.add_calls.count(f"pm1:{uid}"), 1)

    def test_record_onboarding_event_idempotent(self):
        uid = 778
        fixed_now = datetime(2024, 1, 6, tzinfo=timezone.utc)
        self.users.insert_one({"user_id": uid, "status": "Normal"})
        self.events.docs[f"E0:{uid}"] = {"_id": f"E0:{uid}", "uid": uid}
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            created = onboarding.record_onboarding_start(uid, source="visible")
        self.assertTrue(created)
        self.assertIn(f"E0:{uid}", self.events.docs)

    def test_visible_ping_throttled(self):
        uid = 888
        fixed_now = datetime(2024, 1, 7, tzinfo=timezone.utc)
        created = onboarding.record_visible_ping(uid, ref=fixed_now)
        throttled = onboarding.record_visible_ping(uid, ref=fixed_now + timedelta(seconds=30))
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(created["action"], "created_minimal")
        self.assertEqual(throttled["action"], "throttled")
        self.assertEqual(user_doc.get("last_visible_at"), fixed_now)


    def test_visible_ping_handles_naive_last_visible_at(self):
        uid = 889
        naive_last_visible = datetime(2024, 1, 7, 0, 0, 0)
        self.users.insert_one({"user_id": uid, "last_visible_at": naive_last_visible})
        fixed_now = datetime(2024, 1, 7, 0, 0, 30, tzinfo=timezone.utc)
        result = onboarding.record_visible_ping(uid, ref=fixed_now)
        self.assertEqual(result["action"], "throttled")

    def test_visible_ping_handles_aware_last_visible_at(self):
        uid = 890
        aware_last_visible = datetime(2024, 1, 7, 0, 0, 0, tzinfo=timezone.utc)
        self.users.insert_one({"user_id": uid, "last_visible_at": aware_last_visible})
        fixed_now = datetime(2024, 1, 7, 0, 2, 0, tzinfo=timezone.utc)
        result = onboarding.record_visible_ping(uid, ref=fixed_now)
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(result["action"], "updated")
        self.assertEqual(user_doc.get("last_visible_at"), fixed_now)
        
    def test_e2_idempotent_first_checkin(self):
        uid = 456
        self.users.insert_one({"user_id": uid})
        fixed_now = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with patch("onboarding.random.uniform", return_value=8.5), patch.object(
            onboarding, "now_utc", return_value=fixed_now
        ):
            record_first_checkin(uid)
            record_first_checkin(uid)
        self.assertIn("first_checkin_at", self.users.find_one({"user_id": uid}))
        self.assertEqual(self.scheduler.remove_calls.count(f"pm1:{uid}"), 1)
        self.assertEqual(self.scheduler.add_calls.count(f"pm2:{uid}"), 1)
        trigger = self.scheduler.jobs[f"pm2:{uid}"]["trigger"]
        self.assertEqual(trigger.run_date, fixed_now + timedelta(hours=8.5))

    def test_first_checkin_sets_mywin_due_at_in_test_mode(self):
        uid = 457
        self.users.insert_one({"user_id": uid})
        fixed_now = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with patch.dict(os.environ, {"ONBOARDING_TEST_MODE": "1"}), patch.object(
            onboarding, "now_utc", return_value=fixed_now
        ):
            record_first_checkin(uid)
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(user_doc.get("mywin7_due_at_utc"), fixed_now + timedelta(minutes=7))
        self.assertEqual(user_doc.get("mywin14_due_at_utc"), fixed_now + timedelta(minutes=14))
        
    def test_e3_record_first_mywin(self):
        uid = 789
        self.users.insert_one({"user_id": uid})
        fixed_now = datetime(2024, 1, 3, tzinfo=timezone.utc)
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            record_first_mywin(uid, -1002743212540, 111)
            record_first_mywin(uid, -1002743212540, 111)
        self.assertIn("first_mywin_at", self.users.find_one({"user_id": uid}))
        self.assertEqual(self.scheduler.remove_calls.count(f"pm2:{uid}"), 1)
        self.assertEqual(self.scheduler.add_calls.count(f"pm3:{uid}"), 1)
        trigger = self.scheduler.jobs[f"pm3:{uid}"]["trigger"]
        self.assertEqual(trigger.run_date, fixed_now + timedelta(hours=24))

    def test_schedule_pm1_due_at_idempotent(self):
        uid = 111
        fixed_now = datetime(2024, 2, 1, tzinfo=timezone.utc)
        self.users.insert_one({"user_id": uid})
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            onboarding.schedule_pm1(uid)
            onboarding.schedule_pm1(uid)
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(user_doc.get("pm1_due_at_utc"), fixed_now + timedelta(hours=2))

    def test_onboarding_due_tick_marks_sent(self):
        uid = 222
        fixed_now = datetime(2024, 2, 2, tzinfo=timezone.utc)
        self.users.insert_one(
            {
                "user_id": uid,
                "pm1_due_at_utc": fixed_now - timedelta(minutes=1),
                "pm1_last_error": "old_error",
                "pm1_last_error_at_utc": fixed_now - timedelta(minutes=2),
            }
        )
        with (
            patch.object(onboarding, "now_utc", return_value=fixed_now),
            patch.object(onboarding, "_acquire_onboarding_lock", return_value=(True, None)),
            patch.object(onboarding, "send_pm1_if_needed", return_value=(True, None, None)),
        ):
            onboarding.onboarding_due_tick()
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(user_doc.get("pm1_sent_at_utc"), fixed_now)
        self.assertNotIn("pm1_last_error", user_doc)


    def test_onboarding_due_tick_marks_mywin7_sent(self):
        uid = 223
        fixed_now = datetime(2024, 2, 2, tzinfo=timezone.utc)
        self.users.insert_one(
            {
                "user_id": uid,
                "mywin7_due_at_utc": fixed_now - timedelta(minutes=1),
            }
        )
        with (
            patch.object(onboarding, "now_utc", return_value=fixed_now),
            patch.object(onboarding, "_acquire_onboarding_lock", return_value=(True, None)),
            patch.object(onboarding, "send_mywin7_if_needed", return_value=(True, None, None)),
        ):
            onboarding.onboarding_due_tick()
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(user_doc.get("mywin7_sent_at_utc"), fixed_now)

    def test_send_mywin7_skips_if_already_mywin(self):
        uid = 224
        fixed_now = datetime(2024, 2, 2, tzinfo=timezone.utc)
        self.users.insert_one({"user_id": uid, "first_mywin_at": fixed_now})
        ok, err, skipped = onboarding.send_mywin7_if_needed(uid, return_error=True)
        self.assertTrue(ok)
        self.assertIsNone(err)
        self.assertEqual(skipped, "already_mywin")
        
    def test_e3_handler_filters(self):
        called = []

        class FakeMessage:
            def __init__(self, text=None, caption=None, photo=None, document=None, message_id=1):
                self.text = text
                self.caption = caption
                self.photo = photo
                self.document = document
                self.message_id = message_id

        class FakeChat:
            def __init__(self, chat_id):
                self.id = chat_id

        class FakeUser:
            def __init__(self, uid):
                self.id = uid

        class FakeUpdate:
            def __init__(self, message, chat_id):
                self.effective_message = message
                self.effective_chat = FakeChat(chat_id)
                self.effective_user = FakeUser(999)

        class FakeCollection:
            def create_index(self, *args, **kwargs):  # noqa: ARG002
                return None

            def aggregate(self, *args, **kwargs):  # noqa: ARG002
                return []

            def delete_many(self, *args, **kwargs):  # noqa: ARG002
                return None

        class FakeDB:
            def __getitem__(self, name):  # noqa: ARG002
                return FakeCollection()

            def __getattr__(self, name):  # noqa: ARG002
                return FakeCollection()

        with patch.dict(os.environ, {"BOT_TOKEN": "123:ABC", "MONGO_URL": "mongodb://localhost/test"}):
            import database

            with patch.object(database, "init_db", return_value=None), patch.object(
                database, "get_db", return_value=FakeDB()
            ):
                main = importlib.import_module("main")

        with patch.object(main, "record_first_mywin", side_effect=lambda *args: called.append(args)):
            update = FakeUpdate(FakeMessage(text="#mywin", photo=[object()]), -1002743212540)
            asyncio.run(main.mywin_message_handler(update, None))

            update_wrong = FakeUpdate(FakeMessage(text="#mywin", photo=[object()]), -1)
            asyncio.run(main.mywin_message_handler(update_wrong, None))

            update_no_tag = FakeUpdate(FakeMessage(text="hello", photo=[object()]), -1002743212540)
            asyncio.run(main.mywin_message_handler(update_no_tag, None))

        self.assertEqual(len(called), 1)

    def test_e4_first_referral_detection(self):
        with patch("onboarding.record_first_referral", return_value=True) as record_first, patch(
            "onboarding.maybe_unlock_vip1"
        ) as maybe_unlock:
            scheduler.maybe_handle_first_referral(1001, 0, 1, datetime.now(timezone.utc))
            scheduler.maybe_handle_first_referral(1001, 1, 2, datetime.now(timezone.utc))
        record_first.assert_called_once()
        maybe_unlock.assert_called_once()

    def test_vip_unlock_sends_pm4_once(self):
        uid = 2020
        self.users.insert_one({
            "user_id": uid,
            "onboarding_started_at": datetime.now(timezone.utc) - timedelta(days=1),
            "first_checkin_at": datetime.now(timezone.utc) - timedelta(days=1),
            "first_mywin_at": datetime.now(timezone.utc) - timedelta(days=1),
            "first_referral_at": datetime.now(timezone.utc) - timedelta(days=1),
            "monthly_xp": 900,
            "status": "Normal",
        })
        bot = FakeBot()
        async def _fake_safe_send_message(bot, chat_id=None, text=None, **kwargs):  # noqa: ARG001
            bot.send_message(chat_id=chat_id, text=text)
            return True

        def _run(coro, **kwargs):  # noqa: ARG001
            return asyncio.run(coro)

        with (
            patch("onboarding.get_bot", return_value=bot),
            patch("onboarding.safe_send_message", side_effect=_fake_safe_send_message),
            patch("onboarding.run_bot_coroutine", side_effect=_run),
        ):
            onboarding.maybe_unlock_vip1(uid)
            onboarding.maybe_unlock_vip1(uid)
        self.assertEqual(len(bot.sent), 1)
        user_doc = self.users.find_one({"user_id": uid})
        self.assertEqual(user_doc["status"], "VIP1")
        self.assertIn("pm_vip_unlocked", user_doc.get("pm_sent", {}))


if __name__ == "__main__":
    unittest.main()
