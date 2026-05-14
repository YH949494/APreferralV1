import ast
from pathlib import Path


def _load_symbols():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {
        "IDENTITY_TIERS",
        "_safe_non_negative_int",
        "derive_identity_tier",
        "compute_next_tier_progress",
        "compute_weekly_rank",
        "api_me_identity",
    }
    body = []
    for node in module.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in wanted:
                    body.append(node)
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            node.decorator_list = []
            body.append(node)
    isolated = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    env["datetime"] = __import__("datetime").datetime
    env["STREAK_FREEZE_MAX_TOKENS"] = 3
    return env


class _Users:
    def __init__(self, doc, higher_count=0):
        self.doc = doc
        self.higher_count = higher_count

    def find_one(self, _f, _p):
        return self.doc

    def count_documents(self, _q):
        return self.higher_count


class _Req:
    args = {}
    headers = {}


def test_tier_boundary_thresholds():
    env = _load_symbols()
    derive = env["derive_identity_tier"]
    assert derive(0, 0)["name"] == "Rookie"
    assert derive(1, 0)["name"] == "Bronze"
    assert derive(0, 1500)["name"] == "Bronze"
    assert derive(5, 0)["name"] == "Silver"
    assert derive(0, 5000)["name"] == "Silver"
    assert derive(20, 0)["name"] == "Captain"
    assert derive(0, 15000)["name"] == "Captain"
    assert derive(50, 0)["name"] == "Elite"
    assert derive(0, 40000)["name"] == "Elite"
    assert derive(100, 0)["name"] == "Legend"
    assert derive(0, 100000)["name"] == "Legend"


def test_highest_eligible_tier_wins():
    env = _load_symbols()
    derive = env["derive_identity_tier"]
    assert derive(100, 10)["name"] == "Legend"
    assert derive(30, 7000)["name"] == "Captain"


def test_normalize_malformed_and_negative_values():
    env = _load_symbols()
    n = env["_safe_non_negative_int"]
    assert n(None) == 0
    assert n("bad") == 0
    assert n(float("nan")) == 0
    assert n("inf") == 0
    assert n("-inf") == 0
    assert n("Infinity") == 0
    assert n("1e309") == 0
    assert n(float("inf")) == 0
    assert n(float("-inf")) == 0
    assert n(-999) == 0


def test_endpoint_alias_fallback_and_shape():
    env = _load_symbols()
    fn = env["api_me_identity"]
    env.update(
        {
            "request": _Req(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1001, "username": "tg_u"}}, "ok"),
            "users_collection": _Users(
                {
                    "user_id": 1001,
                    "name": "Alias Name",
                    "lifetime_xp": 5000,
                    "lifetime_referrals": 2,
                    "checkin_streak": 7,
                    "streak_freeze_tokens": 2,
                    "vip_tier": "VIP9",
                    "weekly_xp": "inf",
                    "monthly_xp": None,
                    "weekly_referrals": -3,
                    "monthly_referrals": "2",
                }
            ),
            "jsonify": lambda payload: payload,
            "json": __import__("json"),
        }
    )
    body = fn()
    assert body["display_name"] == "Alias Name"
    assert body["total_xp"] == 5000
    assert body["total_referrals"] == 2
    assert body["streak_days"] == 7
    assert body["tier_name"] == "Silver"
    assert body["weekly_xp"] == 0
    assert body["source_vip_tier"] == "VIP9"
    assert body["streak_freeze_tokens"] == 2
    assert body["streak_freeze_max_tokens"] == 3
    for k in (
        "user_id", "display_name", "tier_name", "tier_icon", "weekly_xp", "monthly_xp", "total_xp",
        "weekly_referrals", "monthly_referrals", "total_referrals", "streak_days", "next_tier_name",
        "next_tier_progress_pct", "next_tier_hint", "source_vip_tier", "weekly_rank", "streak",
        "streak_freeze_tokens", "streak_freeze_max_tokens",
    ):
        assert k in body


def test_weekly_rank_present_when_activity_exists():
    env = _load_symbols()
    fn = env["api_me_identity"]
    env.update(
        {
            "request": _Req(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1001, "username": "tg_u"}}, "ok"),
            "users_collection": _Users({"user_id": 1001, "weekly_xp": 10, "weekly_referrals": 2}, higher_count=3),
            "jsonify": lambda payload: payload,
            "json": __import__("json"),
            "datetime": __import__("datetime").datetime,
        }
    )
    body = fn()
    assert body["weekly_rank"] == 4


def test_weekly_rank_null_when_no_weekly_activity():
    env = _load_symbols()
    fn = env["api_me_identity"]
    env.update(
        {
            "request": _Req(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1001, "username": "tg_u"}}, "ok"),
            "users_collection": _Users({"user_id": 1001, "weekly_xp": 0, "weekly_referrals": 0}, higher_count=99),
            "jsonify": lambda payload: payload,
            "json": __import__("json"),
            "datetime": __import__("datetime").datetime,
        }
    )
    body = fn()
    assert body["weekly_rank"] is None


def test_missing_user_doc_safe_defaults_and_no_crash():
    env = _load_symbols()
    fn = env["api_me_identity"]
    env.update(
        {
            "request": _Req(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 555, "username": "tg_user"}}, "ok"),
            "users_collection": _Users(None),
            "jsonify": lambda payload: payload,
            "json": __import__("json"),
        }
    )
    body = fn()
    assert body["user_id"] == 555
    assert body["display_name"] == "tg_user"
    assert body["tier_name"] == "Rookie"
    assert body["streak_freeze_tokens"] == 0
    assert body["streak_freeze_max_tokens"] == 3


def test_freeze_tokens_clamped_to_max():
    env = _load_symbols()
    fn = env["api_me_identity"]
    env.update(
        {
            "request": _Req(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 555, "username": "tg_user"}}, "ok"),
            "users_collection": _Users({"user_id": 555, "streak_freeze_tokens": 999}),
            "jsonify": lambda payload: payload,
            "json": __import__("json"),
        }
    )
    body = fn()
    assert body["streak_freeze_tokens"] == 3


def test_legend_next_tier_values():
    env = _load_symbols()
    fn = env["compute_next_tier_progress"]
    nxt, pct, hint = fn(100, 100000, "Legend")
    assert nxt is None
    assert pct == 100
    assert hint == "Top tier unlocked"
