import ast
from pathlib import Path

from pm_preferences import PM_PREFERENCE_DEFAULTS, merged_pm_preferences, pm_allowed


class _UsersCollection:
    def __init__(self, doc=None, fail=False):
        self.doc = doc
        self.fail = fail
        self.updated = None

    def find_one(self, *args, **kwargs):  # noqa: ARG002
        if self.fail:
            raise RuntimeError("db down")
        return self.doc

    def update_one(self, query, update, upsert=False):
        self.updated = (query, update, upsert)


class _Logger:
    def __init__(self):
        self.warnings = []

    def warning(self, *args, **kwargs):
        self.warnings.append((args, kwargs))


def test_missing_user_defaults_true():
    users = _UsersCollection(doc=None)
    assert pm_allowed(123, "referral_updates", users_collection=users) is True
    assert merged_pm_preferences(None) == PM_PREFERENCE_DEFAULTS


def test_saved_false_suppresses_referral_updates():
    users = _UsersCollection(doc={"pm_preferences": {"referral_updates": False}})
    assert pm_allowed(123, "referral_updates", users_collection=users) is False


def test_db_error_returns_allowed_true():
    logger = _Logger()
    users = _UsersCollection(fail=True)
    assert pm_allowed(123, "referral_updates", users_collection=users, logger=logger) is True
    assert logger.warnings


def _load_api_funcs():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {"api_me_pm_preferences_get", "api_me_pm_preferences_post"}
    body = [n for n in module.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    for node in body:
        node.decorator_list = []
    iso = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(iso)
    env = {}
    exec(compile(iso, filename="main.py", mode="exec"), env)  # noqa: S102
    return env


def test_get_endpoint_returns_merged_defaults_and_saved_values():
    env = _load_api_funcs()
    users = _UsersCollection(doc={"pm_preferences": {"referral_updates": False}})
    env.update({
        "extract_raw_init_data_from_query": lambda req: "ok",
        "verify_telegram_init_data": lambda init_data: (True, {"user": {"id": 42}}, None),
        "request": type("R", (), {"get_json": lambda self, silent=True: {}})(),
        "jsonify": lambda payload: payload,
        "users_collection": users,
        "merged_pm_preferences": merged_pm_preferences,
        "PM_PREFERENCE_DEFAULTS": PM_PREFERENCE_DEFAULTS,
        "json": __import__("json"),
    })
    out = env["api_me_pm_preferences_get"]()
    assert out["ok"] is True
    assert out["pm_preferences"]["referral_updates"] is False
    assert out["pm_preferences"]["announcements"] is True


def test_post_only_updates_known_keys_unknown_ignored():
    env = _load_api_funcs()
    users = _UsersCollection(doc={"pm_preferences": {"referral_updates": False}})
    body = {"pm_preferences": {"referral_updates": False, "unknown": False, "winback": "false", "announcements": True}}
    env.update({
        "extract_raw_init_data_from_query": lambda req: "ok",
        "verify_telegram_init_data": lambda init_data: (True, {"user": {"id": 42}}, None),
        "request": type("R", (), {"get_json": lambda self, silent=True: body})(),
        "jsonify": lambda payload: payload,
        "users_collection": users,
        "merged_pm_preferences": merged_pm_preferences,
        "PM_PREFERENCE_DEFAULTS": PM_PREFERENCE_DEFAULTS,
        "json": __import__("json"),
    })
    out = env["api_me_pm_preferences_post"]()
    assert out["ok"] is True
    assert users.updated[1]["$set"] == {"pm_preferences.referral_updates": False, "pm_preferences.announcements": True}
