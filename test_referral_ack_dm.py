import ast
from datetime import datetime, timezone
from pathlib import Path


class _Result:
    def __init__(self, upserted_id=None):
        self.upserted_id = upserted_id


class _NotificationsCollection:
    def __init__(self, first_insert=True):
        self.first_insert = first_insert
        self.calls = 0

    def update_one(self, *args, **kwargs):  # noqa: ARG002
        self.calls += 1
        if self.first_insert and self.calls == 1:
            return _Result(upserted_id="new")
        return _Result(upserted_id=None)


class _Bot:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.sent = []

    def send_message(self, **kwargs):
        if self.should_fail:
            raise RuntimeError("send_failed")
        self.sent.append(kwargs)
        return None


class _Logger:
    def __init__(self):
        self.warnings = []
        self.exceptions = []

    def warning(self, *args, **kwargs):
        self.warnings.append((args, kwargs))

    def exception(self, *args, **kwargs):
        self.exceptions.append((args, kwargs))


def _load_ack_func():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "_maybe_send_referral_join_ack_dm"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["_maybe_send_referral_join_ack_dm"]


def test_ack_dm_sent_once_and_duplicate_skips():
    fn = _load_ack_func()
    bot = _Bot()
    logger = _Logger()
    notifications = _NotificationsCollection(first_insert=True)

    fn.__globals__.update(
        {
            "RUNNER_MODE": "web",
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "referral_notifications_collection": notifications,
            "call_bot_in_loop": lambda coro: coro,
            "app_bot": type("AB", (), {"bot": bot})(),
            "Forbidden": type("Forbidden", (Exception,), {}),
            "BadRequest": type("BadRequest", (Exception,), {}),
            "logger": logger,
        }
    )

    fn(123, 456, "newuser")
    fn(123, 456, "newuser")

    assert len(bot.sent) == 1
    assert notifications.calls == 2


def test_ack_dm_failure_does_not_raise():
    fn = _load_ack_func()
    bot = _Bot(should_fail=True)
    logger = _Logger()
    notifications = _NotificationsCollection(first_insert=True)

    fn.__globals__.update(
        {
            "RUNNER_MODE": "web",
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "referral_notifications_collection": notifications,
            "call_bot_in_loop": lambda coro: coro,
            "app_bot": type("AB", (), {"bot": bot})(),
            "Forbidden": type("Forbidden", (Exception,), {}),
            "BadRequest": type("BadRequest", (Exception,), {}),
            "logger": logger,
        }
    )

    fn(123, 456, "newuser")

    assert logger.exceptions


def test_missing_inviter_or_invitee_skips():
    fn = _load_ack_func()
    bot = _Bot()
    logger = _Logger()
    notifications = _NotificationsCollection(first_insert=True)

    fn.__globals__.update(
        {
            "RUNNER_MODE": "web",
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "referral_notifications_collection": notifications,
            "call_bot_in_loop": lambda coro: coro,
            "app_bot": type("AB", (), {"bot": bot})(),
            "Forbidden": type("Forbidden", (Exception,), {}),
            "BadRequest": type("BadRequest", (Exception,), {}),
            "logger": logger,
        }
    )

    fn(None, 456, "newuser")
    fn(123, None, "newuser")

    assert notifications.calls == 0
    assert not bot.sent
