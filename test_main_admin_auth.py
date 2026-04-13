import ast
from pathlib import Path


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
