import ast
from pathlib import Path
from datetime import datetime, timezone


def _load_share_rank_symbols():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {
        "_safe_non_negative_int",
        "compute_weekly_rank",
        "_extract_verified_telegram_user_id",
        "choose_share_rank_achievement",
        "build_share_rank_caption",
        "_load_share_rank_user_snapshot",
        "_compute_share_rank",
        "api_share_rank_caption",
    }
    body = []
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            node.decorator_list = []
            body.append(node)
    isolated = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    env["datetime"] = datetime
    env["json"] = __import__("json")

    class DummyPyMongoError(Exception):
        pass

    env["PyMongoError"] = DummyPyMongoError
    return env


class _Users:
    def __init__(self, doc=None, higher_count=0):
        self.doc = doc
        self.higher_count = higher_count
        self.find_one_calls = []

    def find_one(self, filt, projection=None):
        self.find_one_calls.append((filt, projection))
        if self.doc is None:
            return None
        expected = filt.get("user_id")
        if expected is not None and self.doc.get("user_id") != expected:
            return None
        return self.doc

    def count_documents(self, query):  # noqa: ARG002
        return self.higher_count


class _Request:
    remote_addr = "127.0.0.1"
    args = {}
    headers = {}
    query_string = b"init_data=ok"


def _logger():
    return type(
        "Logger",
        (),
        {
            "info": lambda *args, **kwargs: None,
            "warning": lambda *args, **kwargs: None,
            "exception": lambda *args, **kwargs: None,
        },
    )()


def test_caption_generation_ranked_format():
    env = _load_share_rank_symbols()
    caption = env["build_share_rank_caption"](2, 750, "Podium Holder", "Top 3 This Week")
    assert caption == (
        "🏆 Currently Ranked #2\n\n"
        "⚡ Weekly XP: 750\n"
        "🎖️ Podium Holder\n"
        "✨ Top 3 This Week\n\n"
        "Still climbing."
    )


def test_title_highlight_priority_rank_wins_over_streak_and_referrals():
    env = _load_share_rank_symbols()
    choose = env["choose_share_rank_achievement"]
    assert choose(1, 1500, 100, 99) == ("Leader of the Pack", "Currently Ranked #1")
    assert choose(3, 1500, 100, 99) == ("Podium Holder", "Top 3 This Week")
    assert choose(10, 1500, 100, 99) == ("Elite Player", "Top 10 This Week")
    assert choose(None, 1500, 56, 99) == ("Iron Will", "56-Day Check-in Streak")
    assert choose(None, 1500, 28, 99) == ("Unstoppable", "28-Day Check-in Streak")
    assert choose(None, 1500, 14, 99) == ("Consistent Challenger", "14-Day Check-in Streak")
    assert choose(None, 1500, 7, 99) == ("Hot Streak", "7-Day Check-in Streak")
    assert choose(None, 1500, 0, 10) == ("Community Builder", "10 Successful Referrals")
    assert choose(None, 1500, 0, 3) == ("Referral Machine", "3 Successful Referrals")
    assert choose(None, 1000, 0, 0) == ("Momentum Builder", "Earned 1000+ XP This Week")
    assert choose(None, 500, 0, 0) == ("XP Hunter", "Earned 500+ XP This Week")
    assert choose(None, 0, 0, 0) == ("Rising Star", "Climbing the Leaderboard")


def test_unranked_user_fallback_caption_and_payload():
    env = _load_share_rank_symbols()
    fn = env["api_share_rank_caption"]
    env.update(
        {
            "request": _Request(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 1001}}, "ok"),
            "users_collection": _Users({"user_id": 1001, "weekly_xp": None, "total_referrals": 0, "streak": 0}),
            "jsonify": lambda payload: payload,
            "logger": _logger(),
        }
    )
    body = fn()
    assert body["ok"] is True
    assert body["rank"] is None
    assert body["weekly_xp"] == 0
    assert "Currently Ranked Unranked" in body["caption"]
    assert body["title"] == "Rising Star"


def test_unauthorized_missing_init_data():
    env = _load_share_rank_symbols()
    fn = env["api_share_rank_caption"]
    env.update(
        {
            "request": _Request(),
            "extract_raw_init_data_from_query": lambda req: "",
            "verify_telegram_init_data": lambda raw: (False, {}, "bad"),
            "jsonify": lambda payload: payload,
            "logger": _logger(),
        }
    )
    body, status = fn()
    assert status == 400
    assert body == {"ok": False, "error": "Missing init_data"}


def test_unauthorized_invalid_init_data():
    env = _load_share_rank_symbols()
    fn = env["api_share_rank_caption"]
    env.update(
        {
            "request": _Request(),
            "extract_raw_init_data_from_query": lambda req: "tampered",
            "verify_telegram_init_data": lambda raw: (False, {}, "bad"),
            "jsonify": lambda payload: payload,
            "logger": _logger(),
        }
    )
    body, status = fn()
    assert status == 403
    assert body == {"ok": False, "error": "Unauthorized"}


def test_missing_user_returns_404_safe_message():
    env = _load_share_rank_symbols()
    fn = env["api_share_rank_caption"]
    env.update(
        {
            "request": _Request(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda raw: (True, {"user": {"id": 404}}, "ok"),
            "users_collection": _Users(None),
            "jsonify": lambda payload: payload,
            "logger": _logger(),
        }
    )
    body, status = fn()
    assert status == 404
    assert body == {"ok": False, "error": "User not found"}
