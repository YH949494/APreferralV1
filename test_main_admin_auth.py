import ast
from pathlib import Path
from datetime import datetime, timezone


def _load_require_admin_from_query():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "require_admin_from_query"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["require_admin_from_query"]


class _RequestArgs:
    def __init__(self, args):
        self._args = args

    def get(self, key, type=None):  # noqa: A002
        value = self._args.get(key)
        if type is not None and value is not None:
            return type(value)
        return value


class _Request:
    def __init__(self, args):
        self.args = _RequestArgs(args)
        self.query_string = b"init_data=ok"
        self.headers = {}


class _AdminCache:
    def __init__(self, ids):
        self.ids = ids

    def find_one(self, filt):  # noqa: ARG002
        return {"ids": self.ids}


def test_require_admin_denies_query_user_id_spoof():
    fn = _load_require_admin_from_query()
    fn.__globals__.update(
        {
            "request": _Request({"user_id": "9999"}),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1111}}, "ok"),
            "_get_admin_secret": lambda req: "",
            "_admin_secret_ok": lambda secret: False,
            "admin_cache_col": _AdminCache([2222]),
            "json": __import__("json"),
        }
    )
    ok, err = fn()
    assert ok is False
    assert err[1] == 403


def test_require_admin_allows_verified_admin_identity():
    fn = _load_require_admin_from_query()
    fn.__globals__.update(
        {
            "request": _Request({"user_id": "9999"}),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 2222}}, "ok"),
            "_get_admin_secret": lambda req: "",
            "_admin_secret_ok": lambda secret: False,
            "admin_cache_col": _AdminCache([2222]),
            "json": __import__("json"),
        }
    )
    ok, err = fn()
    assert ok is True
    assert err is None


def test_require_admin_allows_admin_secret():
    fn = _load_require_admin_from_query()
    fn.__globals__.update(
        {
            "request": _Request({"admin_secret": "s"}),
            "extract_raw_init_data_from_query": lambda req: "",
            "verify_telegram_init_data": lambda raw: (False, {}, "bad"),
            "_get_admin_secret": lambda req: req.args.get("admin_secret"),
            "_admin_secret_ok": lambda secret: secret == "s",
            "admin_cache_col": _AdminCache([]),
            "json": __import__("json"),
        }
    )
    ok, err = fn()
    assert ok is True
    assert err is None


def _load_api_is_admin():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "api_is_admin"
    )
    fn_node.decorator_list = []
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["api_is_admin"]


def _load_main_functions(*names):
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    nodes = []
    for name in names:
        fn_node = next(
            node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == name
        )
        fn_node.decorator_list = []
        nodes.append(fn_node)
    isolated = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return [env[name] for name in names]


class _TelegramCountsCache:
    def __init__(self, doc=None):
        self.doc = doc

    def find_one(self, filt, projection=None):  # noqa: ARG002
        if filt == {"_id": "telegram_member_counts"}:
            return self.doc
        return None


def test_api_is_admin_verified_admin_true():
    fn = _load_api_is_admin()
    calls = []

    fn.__globals__.update(
        {
            "request": _Request({"user_id": "9999"}),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 2222}}, "ok"),
            "_get_admin_secret": lambda req: "",
            "_admin_secret_ok": lambda secret: False,
            "admin_cache_col": _AdminCache([2222]),
            "_users_update_one": lambda *args, **kwargs: calls.append((args, kwargs)),
            "datetime": __import__("datetime").datetime,
            "timezone": __import__("datetime").timezone,
            "json": __import__("json"),
            "jsonify": lambda payload: payload,
            "traceback": __import__("traceback"),
        }
    )

    body = fn()
    assert body["success"] is True
    assert body["is_admin"] is True
    assert calls


def test_api_is_admin_verified_non_admin_false():
    fn = _load_api_is_admin()

    fn.__globals__.update(
        {
            "request": _Request({"user_id": "2222"}),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1111}}, "ok"),
            "_get_admin_secret": lambda req: "",
            "_admin_secret_ok": lambda secret: False,
            "admin_cache_col": _AdminCache([2222]),
            "_users_update_one": lambda *args, **kwargs: None,
            "datetime": __import__("datetime").datetime,
            "timezone": __import__("datetime").timezone,
            "json": __import__("json"),
            "jsonify": lambda payload: payload,
            "traceback": __import__("traceback"),
        }
    )

    body = fn()
    assert body["success"] is True
    assert body["is_admin"] is False


def test_api_is_admin_ignores_spoofed_query_user_id():
    fn = _load_api_is_admin()

    fn.__globals__.update(
        {
            "request": _Request({"user_id": "2222"}),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1111}}, "ok"),
            "_get_admin_secret": lambda req: "",
            "_admin_secret_ok": lambda secret: False,
            "admin_cache_col": _AdminCache([2222]),
            "_users_update_one": lambda *args, **kwargs: None,
            "datetime": __import__("datetime").datetime,
            "timezone": __import__("datetime").timezone,
            "json": __import__("json"),
            "jsonify": lambda payload: payload,
            "traceback": __import__("traceback"),
        }
    )

    body = fn()
    assert body["success"] is True
    assert body["is_admin"] is False


def test_api_is_admin_admin_secret_override():
    fn = _load_api_is_admin()

    fn.__globals__.update(
        {
            "request": _Request({"admin_secret": "s"}),
            "extract_raw_init_data_from_query": lambda req: "",
            "verify_telegram_init_data": lambda raw: (False, {}, "bad"),
            "_get_admin_secret": lambda req: req.args.get("admin_secret"),
            "_admin_secret_ok": lambda secret: secret == "s",
            "admin_cache_col": _AdminCache([]),
            "_users_update_one": lambda *args, **kwargs: None,
            "datetime": __import__("datetime").datetime,
            "timezone": __import__("datetime").timezone,
            "json": __import__("json"),
            "jsonify": lambda payload: payload,
            "traceback": __import__("traceback"),
        }
    )

    body = fn()
    assert body["success"] is True
    assert body["is_admin"] is True


def test_dashboard_telegram_counts_cache_returns_missing_doc():
    fn = _load_main_functions("dashboard_telegram_counts_cache")[0]
    fn.__globals__.update(
        {
            "require_admin_from_query": lambda: (True, None),
            "admin_cache_col": _TelegramCountsCache(None),
            "TELEGRAM_COUNTS_DOC_ID": "telegram_member_counts",
            "sanitize_telegram_counts_cache": __import__(
                "dashboard_telegram"
            ).sanitize_telegram_counts_cache,
            "jsonify": lambda payload: payload,
        }
    )

    body = fn()

    assert body == {"success": True, "exists": False, "updated_at": None, "counts": {}}


def test_dashboard_telegram_counts_cache_returns_sanitized_doc():
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    fn = _load_main_functions("dashboard_telegram_counts_cache")[0]
    fn.__globals__.update(
        {
            "require_admin_from_query": lambda: (True, None),
            "admin_cache_col": _TelegramCountsCache(
                {
                    "_id": "telegram_member_counts",
                    "updated_at": now,
                    "counts": {
                        "official_channel_subscribers": {
                            "chat_id": -1001,
                            "count": 123,
                            "ok": True,
                            "updated_at": now,
                            "error": None,
                            "secret": "ignored",
                        }
                    },
                }
            ),
            "TELEGRAM_COUNTS_DOC_ID": "telegram_member_counts",
            "sanitize_telegram_counts_cache": __import__(
                "dashboard_telegram"
            ).sanitize_telegram_counts_cache,
            "jsonify": lambda payload: payload,
        }
    )

    body = fn()

    assert body["success"] is True
    assert body["exists"] is True
    assert body["updated_at"] == now.isoformat()
    assert body["counts"]["official_channel_subscribers"] == {
        "chat_id": "-1001",
        "count": 123,
        "ok": True,
        "updated_at": now.isoformat(),
        "error": None,
    }


def test_dashboard_telegram_counts_cache_requires_admin():
    fn = _load_main_functions("dashboard_telegram_counts_cache")[0]
    fn.__globals__.update(
        {
            "require_admin_from_query": lambda: (False, ("Admins only", 403)),
            "jsonify": lambda payload: payload,
        }
    )

    body, status = fn()

    assert status == 403
    assert body == {"success": False, "message": "Admins only"}


def test_dashboard_telegram_counts_cache_does_not_call_telegram():
    called = []
    fn = _load_main_functions("dashboard_telegram_counts_cache")[0]
    fn.__globals__.update(
        {
            "require_admin_from_query": lambda: (True, None),
            "admin_cache_col": _TelegramCountsCache(None),
            "TELEGRAM_COUNTS_DOC_ID": "telegram_member_counts",
            "sanitize_telegram_counts_cache": __import__(
                "dashboard_telegram"
            ).sanitize_telegram_counts_cache,
            "refresh_telegram_member_counts": lambda: called.append("telegram"),
            "_tg_fetch_member_count": lambda chat_id: called.append(chat_id),
            "jsonify": lambda payload: payload,
        }
    )

    fn()

    assert called == []


def test_dashboard_telegram_counts_refresh_web_mode_is_worker_only():
    called = []
    fn = _load_main_functions("dashboard_telegram_counts_refresh")[0]
    fn.__globals__.update(
        {
            "require_admin_from_query": lambda: (True, None),
            "RUNNER_MODE": "web",
            "refresh_telegram_member_counts": lambda: called.append("telegram"),
            "jsonify": lambda payload: payload,
        }
    )

    body = fn()

    assert body == {
        "success": False,
        "code": "worker_only",
        "message": "Telegram count refresh runs in worker; check scheduler logs.",
    }
    assert called == []


def test_dashboard_telegram_refresh_log_strings_include_required_fields():
    source = Path("main.py").read_text(encoding="utf-8")
    for expected in (
        "[DASHBOARD_TG_REFRESH][REGISTERED]",
        "runner_mode={RUNNER_MODE}",
        "official_channel_id={OFFICIAL_CHANNEL_ID}",
        "community_chat_id={_COMMUNITY_CHAT_ID}",
        "interval_minutes={tg_refresh_minutes}",
        "first_run_at={tg_first_run_at.isoformat()}",
        "[DASHBOARD_TG_REFRESH][START]",
        "[DASHBOARD_TG_REFRESH][WRITE]",
        "matched={getattr(result, 'matched_count', None)}",
        "modified={getattr(result, 'modified_count', None)}",
        "upserted_id={getattr(result, 'upserted_id', None)}",
        "counts_keys=official_channel_subscribers,chatroom_members",
    ):
        assert expected in source
