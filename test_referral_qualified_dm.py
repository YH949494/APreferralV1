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


class _Resp:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.content = b'{"ok": true}'

    def raise_for_status(self):
        if self.should_fail:
            raise RuntimeError("send_failed")

    def json(self):
        return {"ok": True}


class _Requests:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return _Resp(should_fail=self.should_fail)


class _Logger:
    def __init__(self):
        self.exceptions = []

    def exception(self, *args, **kwargs):
        self.exceptions.append((args, kwargs))


def _load_func():
    source = Path("scheduler.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "_maybe_send_referral_qualified_dm"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="scheduler.py", mode="exec"), env)  # noqa: S102
    return env["_maybe_send_referral_qualified_dm"]


def test_sends_once_after_qualification():
    fn = _load_func()
    notifications = _NotificationsCollection(first_insert=True)
    requests = _Requests()
    logger = _Logger()

    fn.__globals__.update(
        {
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "db": type("DB", (), {"referral_notifications": notifications})(),
            "requests": requests,
            "API_BASE": "https://api.telegram.org/botTEST",
            "logger": logger,
        }
    )

    fn(123, 456, "inviteeuser")
    fn(123, 456, "inviteeuser")

    assert len(requests.calls) == 1
    assert notifications.calls == 2


def test_duplicate_suppressed():
    fn = _load_func()
    notifications = _NotificationsCollection(first_insert=False)
    requests = _Requests()

    fn.__globals__.update(
        {
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "db": type("DB", (), {"referral_notifications": notifications})(),
            "requests": requests,
            "API_BASE": "https://api.telegram.org/botTEST",
            "logger": _Logger(),
        }
    )

    fn(123, 456, None)

    assert len(requests.calls) == 0


def test_failure_does_not_crash():
    fn = _load_func()
    notifications = _NotificationsCollection(first_insert=True)
    requests = _Requests(should_fail=True)
    logger = _Logger()

    fn.__globals__.update(
        {
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "db": type("DB", (), {"referral_notifications": notifications})(),
            "requests": requests,
            "API_BASE": "https://api.telegram.org/botTEST",
            "logger": logger,
        }
    )

    fn(123, 456, None)

    assert logger.exceptions


def test_no_send_when_missing_required_ids():
    fn = _load_func()
    notifications = _NotificationsCollection(first_insert=True)
    requests = _Requests()

    fn.__globals__.update(
        {
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "db": type("DB", (), {"referral_notifications": notifications})(),
            "requests": requests,
            "API_BASE": "https://api.telegram.org/botTEST",
            "logger": _Logger(),
        }
    )

    fn(None, 456, None)
    fn(123, None, None)

    assert notifications.calls == 0
    assert len(requests.calls) == 0
