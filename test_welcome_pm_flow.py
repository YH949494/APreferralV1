import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
import asyncio


def _load_defs(*names):
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    body = [node for node in module.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    isolated = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return [env[n] for n in names]


def test_mark_private_interaction_sets_pm_reachable():
    (fn,) = _load_defs("_mark_private_interaction")
    calls = []
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    class Users:
        def find_one(self, *args, **kwargs):
            return {}

    fn.__globals__.update({
        "now_utc": lambda: now,
        "users_collection": Users(),
        "_users_update_one": lambda *a, **k: calls.append((a, k)),
    })
    fn(10, "alice")
    update_doc = calls[0][0][1]
    assert update_doc["$set"]["pm_reachable"] is True
    assert update_doc["$set"]["bot_started_at"] == now


def test_welcome_reminder_gates_and_sets_sent_at():
    (fn,) = _load_defs("_send_welcome_unclaimed_reminder_if_needed")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    updates = []

    class Users:
        def find_one(self, *args, **kwargs):
            return {"pm_reachable": True}

    class Ctx:
        bot = object()

    async def safe_send_message(*args, **kwargs):
        return True, None

    fn.__globals__.update({
        "users_collection": Users(),
        "welcome_eligibility": lambda *a, **k: (True, "ok", {}),
        "_welcome_bonus_claimed": lambda *a, **k: False,
        "safe_send_message": safe_send_message,
        "_users_update_one": lambda *a, **k: updates.append((a, k)),
        "InlineKeyboardMarkup": lambda x: x,
        "InlineKeyboardButton": lambda *a, **k: (a, k),
        "WebAppInfo": lambda url: url,
        "WEBAPP_URL": "https://example.com",
        "logger": type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None})(),
        "now_utc": lambda: now,
        "datetime": datetime,
        "timedelta": timedelta,
        "Forbidden": Exception,
        "BadRequest": Exception,
    })
    ok = asyncio.run(fn(Ctx(), 11))
    assert ok is True
    assert any("welcome_unclaimed_reminder_sent_at" in c[0][1].get("$set", {}) for c in updates)


def test_welcome_reminder_skips_for_start_source():
    (fn,) = _load_defs("_send_welcome_unclaimed_reminder_if_needed")
    sent = []

    class Users:
        def find_one(self, *args, **kwargs):
            return {"pm_reachable": True}

    class Ctx:
        bot = object()

    async def safe_send_message(*args, **kwargs):
        sent.append(True)
        return True, None

    fn.__globals__.update({
        "users_collection": Users(),
        "welcome_eligibility": lambda *a, **k: (True, "ok", {}),
        "_welcome_bonus_claimed": lambda *a, **k: False,
        "safe_send_message": safe_send_message,
        "InlineKeyboardMarkup": lambda x: x,
        "InlineKeyboardButton": lambda *a, **k: (a, k),
        "WebAppInfo": lambda url: url,
        "WEBAPP_URL": "https://example.com",
        "logger": type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
        "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
        "datetime": datetime,
        "timedelta": timedelta,
        "Forbidden": Exception,
        "BadRequest": Exception,
    })
    ok = asyncio.run(fn(Ctx(), 11, source="start"))
    assert ok is False
    assert sent == []


def test_welcome_reminder_not_sent_when_claimed():
    (fn,) = _load_defs("_send_welcome_unclaimed_reminder_if_needed")
    sent = []

    class Users:
        def find_one(self, *args, **kwargs):
            return {"pm_reachable": True}

    class Ctx:
        bot = object()

    async def safe_send_message(*args, **kwargs):
        sent.append(True)
        return True, None

    fn.__globals__.update({
        "users_collection": Users(),
        "welcome_eligibility": lambda *a, **k: (True, "ok", {}),
        "_welcome_bonus_claimed": lambda *a, **k: True,
        "safe_send_message": safe_send_message,
        "InlineKeyboardMarkup": lambda x: x,
        "InlineKeyboardButton": lambda *a, **k: (a, k),
        "WebAppInfo": lambda url: url,
        "WEBAPP_URL": "https://example.com",
        "logger": type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None})(),
        "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
        "datetime": datetime,
        "timedelta": timedelta,
        "Forbidden": Exception,
        "BadRequest": Exception,
    })
    ok = asyncio.run(fn(Ctx(), 12))
    assert ok is False
    assert sent == []


def test_group_join_no_private_send():
    (fn,) = _load_defs("handle_user_join")
    sent = []

    class Users:
        def find_one(self, *args, **kwargs):
            return {}

    class Bot:
        async def send_message(self, **kwargs):
            sent.append(kwargs)

    class Ctx:
        bot = Bot()

    fn.__globals__.update({
        "logger": type("L", (), {"info": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
        "GROUP_ID": 100,
        "users_collection": Users(),
        "_users_update_one": lambda *a, **k: None,
        "_ensure_welcome_eligibility": lambda *a, **k: None,
        "issue_welcome_bonus_if_eligible": lambda *a, **k: {"status": "ISSUED"},
        "db": object(),
        "datetime": datetime,
        "KL_TZ": timezone.utc,
    })
    asyncio.run(fn(1, "u", 100, source="chat_member", context=Ctx()))
    assert sent == []


def test_forbidden_marks_pm_blocked_non_fatal():
    (fn,) = _load_defs("_send_welcome_unclaimed_reminder_if_needed")
    updates = []

    class Users:
        def find_one(self, *args, **kwargs):
            return {"pm_reachable": True}

    class Ctx:
        bot = object()

    async def safe_send_message(*args, **kwargs):
        return False, "Forbidden: bot can't initiate conversation with a user"

    fn.__globals__.update({
        "users_collection": Users(),
        "welcome_eligibility": lambda *a, **k: (True, "ok", {}),
        "_welcome_bonus_claimed": lambda *a, **k: False,
        "safe_send_message": safe_send_message,
        "_users_update_one": lambda *a, **k: updates.append((a, k)),
        "InlineKeyboardMarkup": lambda x: x,
        "InlineKeyboardButton": lambda *a, **k: (a, k),
        "WebAppInfo": lambda url: url,
        "WEBAPP_URL": "https://example.com",
        "logger": type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None})(),
        "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
        "datetime": datetime,
        "timedelta": timedelta,
        "Forbidden": Exception,
        "BadRequest": Exception,
    })
    ok = asyncio.run(fn(Ctx(), 13))
    assert ok is False
    assert any(c[0][1].get("$set", {}).get("pm_blocked") is True for c in updates)


def test_welcome_bonus_claimed_affiliate_ledger_issued():
    (fn,) = _load_defs("_welcome_bonus_claimed")
    fn.__globals__.update({
        "welcome_eligibility_collection": type("C", (), {"find_one": lambda *a, **k: {}})(),
        "db": {"welcome_tickets": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "affiliate_ledger": type("C", (), {"find_one": lambda *a, **k: {"status": "ISSUED", "pool_id": "WELCOME"}})(),
               "voucher_claims": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "voucher_pools": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "new_joiner_claims": type("C", (), {"find_one": lambda *a, **k: {}})()},
        "logger": type("L", (), {"info": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
    })
    assert fn(42) is True


def test_welcome_bonus_claimed_affiliate_ledger_voucher_code():
    (fn,) = _load_defs("_welcome_bonus_claimed")
    fn.__globals__.update({
        "welcome_eligibility_collection": type("C", (), {"find_one": lambda *a, **k: {}})(),
        "db": {"welcome_tickets": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "affiliate_ledger": type("C", (), {"find_one": lambda *a, **k: {"status": "PENDING", "voucher_code": "ABC123"}})(),
               "voucher_claims": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "voucher_pools": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "new_joiner_claims": type("C", (), {"find_one": lambda *a, **k: {}})()},
        "logger": type("L", (), {"info": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
    })
    assert fn(42) is True


def test_welcome_bonus_claimed_voucher_pools_welcome_issued_to_uid():
    (fn,) = _load_defs("_welcome_bonus_claimed")
    fn.__globals__.update({
        "welcome_eligibility_collection": type("C", (), {"find_one": lambda *a, **k: {}})(),
        "db": {"welcome_tickets": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "affiliate_ledger": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "voucher_claims": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "voucher_pools": type("C", (), {"find_one": lambda *a, **k: {"status": "issued"}})(),
               "new_joiner_claims": type("C", (), {"find_one": lambda *a, **k: {}})()},
        "logger": type("L", (), {"info": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
    })
    assert fn(42) is True


def test_welcome_bonus_claimed_new_joiner_claimed_at():
    (fn,) = _load_defs("_welcome_bonus_claimed")
    fn.__globals__.update({
        "welcome_eligibility_collection": type("C", (), {"find_one": lambda *a, **k: {}})(),
        "db": {"welcome_tickets": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "affiliate_ledger": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "voucher_claims": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "voucher_pools": type("C", (), {"find_one": lambda *a, **k: {}})(),
               "new_joiner_claims": type("C", (), {"find_one": lambda *a, **k: {"claimed_at": datetime(2026, 1, 1, tzinfo=timezone.utc)}})()},
        "logger": type("L", (), {"info": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
    })
    assert fn(42) is True


def test_welcome_bonus_claimed_collection_errors_non_fatal():
    (fn,) = _load_defs("_welcome_bonus_claimed")

    class Err:
        def find_one(self, *args, **kwargs):
            raise RuntimeError("boom")

    fn.__globals__.update({
        "welcome_eligibility_collection": Err(),
        "db": {"welcome_tickets": Err(), "affiliate_ledger": Err(), "voucher_claims": Err(), "voucher_pools": Err(), "new_joiner_claims": Err()},
        "logger": type("L", (), {"info": lambda *a, **k: None, "exception": lambda *a, **k: None})(),
    })
    assert fn(42) is False
