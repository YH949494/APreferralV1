import os
import time
import json
import hmac
import hashlib
import importlib
import sys
import types
import unittest


def build_init_data(token: str, payload: dict) -> str:
    pairs = []
    for key in sorted(payload.keys()):
        pairs.append(f"{key}={payload[key]}")
    check_string = "\n".join(pairs)
    secret_key = hashlib.sha256(token.encode()).digest()
    signature = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()

    query = payload.copy()
    query["hash"] = signature
    return "&".join(f"{k}={query[k]}" for k in query)


class VerifyInitDataTests(unittest.TestCase):
    def setUp(self):
        self.orig_database_module = sys.modules.get("database")

        class _FakeCollection:
            def find_one(self, *args, **kwargs):
                return None

            def create_index(self, *args, **kwargs):
                return None

            def find(self, *args, **kwargs):
                return []

            def sort(self, *args, **kwargs):
                return self

            def limit(self, *args, **kwargs):
                return []

            def count_documents(self, *args, **kwargs):
                return 0

            def find_one_and_update(self, *args, **kwargs):
                return None

            def update_one(self, *args, **kwargs):
                return None

        class _FakeDB:
            def __getitem__(self, name):
                return _FakeCollection()

            def __getattr__(self, name):
                return _FakeCollection()

        fake_database_module = types.ModuleType("database")
        fake_database_module.db = _FakeDB()
        sys.modules["database"] = fake_database_module

        if "vouchers" in sys.modules:
            self.vouchers = importlib.reload(sys.modules["vouchers"])
        else:
            self.vouchers = importlib.import_module("vouchers")

        self.orig_bot_token = self.vouchers._BOT_TOKEN
        self.orig_fallbacks = list(self.vouchers._BOT_TOKEN_FALLBACKS)
        self.env_bot_token = os.environ.get("BOT_TOKEN")
        self.env_fallbacks = os.environ.get("BOT_TOKEN_FALLBACKS")

    def tearDown(self):
        self.vouchers._BOT_TOKEN = self.orig_bot_token
        self.vouchers._BOT_TOKEN_FALLBACKS = self.orig_fallbacks
        if self.env_bot_token is None:
            os.environ.pop("BOT_TOKEN", None)
        else:
            os.environ["BOT_TOKEN"] = self.env_bot_token
        if self.env_fallbacks is None:
            os.environ.pop("BOT_TOKEN_FALLBACKS", None)
        else:
            os.environ["BOT_TOKEN_FALLBACKS"] = self.env_fallbacks

        if self.orig_database_module is None:
            sys.modules.pop("database", None)
        else:
            sys.modules["database"] = self.orig_database_module

    def test_verify_uses_configured_bot_token(self):
        token = "123:ABC"
        self.vouchers._BOT_TOKEN = token
        self.vouchers._BOT_TOKEN_FALLBACKS = []
        os.environ.pop("BOT_TOKEN", None)
        os.environ.pop("BOT_TOKEN_FALLBACKS", None)

        payload = {
            "auth_date": str(int(time.time())),
            "user": json.dumps({"id": 42, "username": "SampleUser"}),
        }
        init_data = build_init_data(token, payload)

        ok, data, reason = self.vouchers.verify_telegram_init_data(init_data)

        self.assertTrue(ok, reason)
        self.assertEqual(json.loads(data["user"])["id"], 42)

    def test_verify_reports_missing_token(self):
        self.vouchers._BOT_TOKEN = ""
        self.vouchers._BOT_TOKEN_FALLBACKS = []
        os.environ.pop("BOT_TOKEN", None)
        os.environ.pop("BOT_TOKEN_FALLBACKS", None)

        payload = {
            "auth_date": str(int(time.time())),
            "user": json.dumps({"id": 1}),
        }
        init_data = build_init_data("token-will-not-match", payload)

        ok, _, reason = self.vouchers.verify_telegram_init_data(init_data)

        self.assertFalse(ok)
        self.assertEqual(reason, "bot_token_missing")


if __name__ == "__main__":
    unittest.main()
