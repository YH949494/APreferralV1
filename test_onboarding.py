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


class FakeUpdateResult:
    def __init__(self, modified_count=0, upserted_id=None):
        self.modified_count = modified_count
        self.upserted_id = upserted_id


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
            elif doc.get(key) != value:
                return False
        return True

    def find_one(self, filt, projection=None):  # noqa: ARG002
        for doc in self.docs.values():
            if self._match(doc, filt):
                return dict(doc)
        return None

    def update_one(self, filt, update, upsert=False):
        for doc in self.docs.values():
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    self._apply_set(doc, key, value)
                return FakeUpdateResult(modified_count=1)
        if not upsert:
            return FakeUpdateResult(modified_count=0)
        uid = filt.get("user_id")
        if uid in self.docs:
            return FakeUpdateResult(modified_count=0)
        new_doc = {"user_id": uid}
        for key, value in update.get("$set", {}).items():
            self._apply_set(new_doc, key, value)
        new_doc["_id"] = self._next_id
        self._next_id += 1
        self.docs[uid] = new_doc
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


class OnboardingTests(unittest.TestCase):
    def setUp(self):
        self.users = FakeUsersCollection()
        self.scheduler = FakeScheduler()
        onboarding.users_collection = self.users
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

        self.assertIn(uid, self.users.docs)
        self.assertIn("onboarding_started_at", self.users.docs[uid])
        self.assertEqual(self.scheduler.add_calls.count(f"pm1:{uid}"), 1)
        trigger = self.scheduler.jobs[f"pm1:{uid}"]["trigger"]
        self.assertEqual(trigger.run_date, fixed_now + timedelta(hours=2))

    def test_e2_idempotent_first_checkin(self):
        uid = 456
        self.users.docs[uid] = {"user_id": uid}
        fixed_now = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with patch("onboarding.random.uniform", return_value=8.5), patch.object(
            onboarding, "now_utc", return_value=fixed_now
        ):
            record_first_checkin(uid)
            record_first_checkin(uid)
        self.assertIn("first_checkin_at", self.users.docs[uid])
        self.assertEqual(self.scheduler.remove_calls.count(f"pm1:{uid}"), 1)
        self.assertEqual(self.scheduler.add_calls.count(f"pm2:{uid}"), 1)
        trigger = self.scheduler.jobs[f"pm2:{uid}"]["trigger"]
        self.assertEqual(trigger.run_date, fixed_now + timedelta(hours=8.5))

    def test_e3_record_first_mywin(self):
        uid = 789
        self.users.docs[uid] = {"user_id": uid}
        fixed_now = datetime(2024, 1, 3, tzinfo=timezone.utc)
        with patch.object(onboarding, "now_utc", return_value=fixed_now):
            record_first_mywin(uid, -1002743212540, 111)
            record_first_mywin(uid, -1002743212540, 111)
        self.assertIn("first_mywin_at", self.users.docs[uid])
        self.assertEqual(self.scheduler.remove_calls.count(f"pm2:{uid}"), 1)
        self.assertEqual(self.scheduler.add_calls.count(f"pm3:{uid}"), 1)
        trigger = self.scheduler.jobs[f"pm3:{uid}"]["trigger"]
        self.assertEqual(trigger.run_date, fixed_now + timedelta(hours=24))

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
        self.users.docs[uid] = {
            "user_id": uid,
            "onboarding_started_at": datetime.now(timezone.utc) - timedelta(days=1),
            "first_checkin_at": datetime.now(timezone.utc) - timedelta(days=1),
            "first_mywin_at": datetime.now(timezone.utc) - timedelta(days=1),
            "first_referral_at": datetime.now(timezone.utc) - timedelta(days=1),
            "monthly_xp": 900,
            "status": "Normal",
        }
        bot = FakeBot()
        with patch("onboarding.get_bot", return_value=bot), patch(
            "onboarding.run_bot_coroutine", side_effect=lambda coro: coro
        ):
            onboarding.maybe_unlock_vip1(uid)
            onboarding.maybe_unlock_vip1(uid)
        self.assertEqual(len(bot.sent), 1)
        self.assertEqual(self.users.docs[uid]["status"], "VIP1")
        self.assertIn("pm_vip_unlocked", self.users.docs[uid].get("pm_sent", {}))


if __name__ == "__main__":
    unittest.main()
