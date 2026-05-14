import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path


class _UsersCollection:
    def __init__(self, doc=None):
        self.doc = doc
        self.update_calls = []

    def find_one(self, *args, **kwargs):  # noqa: ARG002
        return self.doc

    def update_one(self, *args, **kwargs):
        self.update_calls.append((args, kwargs))


class _Bot:
    def __init__(self):
        self.sent = []

    def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return None


class _Logger:
    def __init__(self):
        self.infos = []

    def info(self, *args, **kwargs):
        self.infos.append((args, kwargs))

    def warning(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


def _load_near_miss_func():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "_maybe_send_near_miss_dm"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["_maybe_send_near_miss_dm"]


def _bind(fn, users, bot, logger, pm_allowed_fn):
    now_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fn.__globals__.update(
        {
            "RUNNER_MODE": "web",
            "users_collection": users,
            "pm_allowed": pm_allowed_fn,
            "logger": logger,
            "now_utc": lambda: now_ts,
            "timedelta": timedelta,
            "datetime": datetime,
            "timezone": timezone,
            "call_bot_in_loop": lambda coro: coro,
            "app_bot": type("AB", (), {"bot": bot})(),
            "Forbidden": type("Forbidden", (Exception,), {}),
            "BadRequest": type("BadRequest", (Exception,), {}),
        }
    )


def test_near_miss_sends_when_preference_allowed():
    fn = _load_near_miss_func()
    users = _UsersCollection(doc={})
    bot = _Bot()
    logger = _Logger()
    _bind(fn, users, bot, logger, lambda *args, **kwargs: True)

    fn(123, 2)

    assert len(bot.sent) == 1
    assert len(users.update_calls) == 1


def test_near_miss_suppresses_when_preference_disabled_without_updating_cooldown():
    fn = _load_near_miss_func()
    users = _UsersCollection(doc={})
    bot = _Bot()
    logger = _Logger()
    _bind(fn, users, bot, logger, lambda *args, **kwargs: False)

    fn(123, 2)

    assert not bot.sent
    assert not users.update_calls
    assert logger.infos


def test_near_miss_db_error_in_pm_allowed_fails_open_and_sends():
    fn = _load_near_miss_func()
    users = _UsersCollection(doc={})
    bot = _Bot()
    logger = _Logger()

    _bind(fn, users, bot, logger, lambda *args, **kwargs: True)

    fn(123, 2)

    assert len(bot.sent) == 1
    assert len(users.update_calls) == 1


def test_missing_inviter_skips():
    fn = _load_near_miss_func()
    users = _UsersCollection(doc={})
    bot = _Bot()
    logger = _Logger()
    _bind(fn, users, bot, logger, lambda *args, **kwargs: True)

    fn(None, 2)

    assert not bot.sent
    assert not users.update_calls
