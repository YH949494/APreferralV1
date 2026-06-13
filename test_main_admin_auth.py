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


class _CountCollection:
    def __init__(self, value=0):
        self.value = value
        self.filters = []

    def count_documents(self, filt):
        self.filters.append(filt)
        if callable(self.value):
            return self.value(filt)
        return self.value


class _DocCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.count_filters = []
        self.find_filters = []
        self.distinct_calls = []

    def _match_value(self, value, condition):
        if isinstance(condition, dict):
            for op, expected in condition.items():
                if op == "$gte":
                    if value is None or value < expected:
                        return False
                elif op == "$in":
                    if value not in expected:
                        return False
                elif op == "$regex":
                    import re

                    if value is None or re.search(expected, str(value)) is None:
                        return False
                else:
                    raise AssertionError(f"unsupported op {op}")
            return True
        return value == condition

    def _matches(self, doc, filt):
        for key, condition in (filt or {}).items():
            if key == "$or":
                if not any(self._matches(doc, sub) for sub in condition):
                    return False
                continue
            if not self._match_value(doc.get(key), condition):
                return False
        return True

    def find(self, filt, projection=None):  # noqa: ARG002
        self.find_filters.append(filt)
        return [doc for doc in self.docs if self._matches(doc, filt)]

    def count_documents(self, filt):
        self.count_filters.append(filt)
        return len(self.find(filt))

    def distinct(self, field, filt):
        self.distinct_calls.append((field, filt))
        return sorted({doc.get(field) for doc in self.find(filt) if doc.get(field) is not None})


class _FunnelDb:
    def __init__(
        self,
        *,
        welcome_tickets=None,
        affiliate_ledger=None,
        new_joiner_claims=None,
        voucher_claims=None,
    ):
        self.collections = {
            "welcome_tickets": welcome_tickets or _DocCollection(),
            "affiliate_ledger": affiliate_ledger or _DocCollection(),
            "new_joiner_claims": new_joiner_claims or _DocCollection(),
            "voucher_claims": voucher_claims or _DocCollection(),
        }

    def __getitem__(self, name):
        return self.collections[name]


class _DistinctCollection:
    def __init__(self, values=None):
        self.values = values or []
        self.calls = []

    def distinct(self, field, filt):
        self.calls.append((field, filt))
        return self.values


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


def test_dashboard_telegram_counts_cache_uses_browser_compatible_guard():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    for name in ("dashboard_telegram_counts_cache", "dashboard_telegram_counts_refresh"):
        fn_node = next(
            node for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == name
        )
        calls = [
            node.func.id for node in ast.walk(fn_node)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]

        assert "require_admin_from_query" in calls
        assert "require_admin" not in calls


def test_require_admin_allows_browser_admin_session_without_init_data():
    fn = _load_require_admin_from_query()
    fn.__globals__.update(
        {
            "request": _Request({}),
            "extract_raw_init_data_from_query": lambda req: "",
            "verify_telegram_init_data": lambda raw: (False, {}, "bad"),
            "_get_admin_secret": lambda req: "",
            "_admin_secret_ok": lambda secret: False,
            "admin_cache_col": _AdminCache([]),
            "json": __import__("json"),
        }
    )
    import sys
    import types

    admin_auth = types.ModuleType("admin_auth")
    admin_auth.session_admin = lambda: {"id": 2222, "username": "admin"}
    previous = sys.modules.get("admin_auth")
    sys.modules["admin_auth"] = admin_auth
    try:
        ok, err = fn()
    finally:
        if previous is None:
            sys.modules.pop("admin_auth", None)
        else:
            sys.modules["admin_auth"] = previous

    assert ok is True
    assert err is None


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


def _run_dashboard_funnel(
    *,
    users,
    xp_events=None,
    welcome_eligibility=None,
    affiliate_ledger=None,
    new_joiner_claims=None,
    welcome_tickets=None,
    voucher_claims=None,
    window="7d",
):
    now = datetime(2026, 6, 13, 12, 0, 0, tzinfo=timezone.utc)
    cached = []
    fn = _load_main_functions("dashboard_funnel")[0]
    fn.__globals__.update(
        {
            "require_admin_from_query": lambda: (True, None),
            "request": _Request({"window": window, "refresh": "1"}),
            "_dashboard_cache_get": lambda key: None,
            "_dashboard_cache_set": lambda key, payload: cached.append((key, payload)),
            "_utc_now": lambda: now,
            "_utc_today_start": lambda ref: datetime(ref.year, ref.month, ref.day, tzinfo=timezone.utc),
            "timedelta": __import__("datetime").timedelta,
            "db": _FunnelDb(
                welcome_tickets=welcome_tickets,
                affiliate_ledger=affiliate_ledger,
                new_joiner_claims=new_joiner_claims,
                voucher_claims=voucher_claims,
            ),
            "users_collection": users,
            "xp_events_collection": xp_events or _DocCollection(),
            "welcome_eligibility_collection": welcome_eligibility or _DocCollection(),
            "jsonify": lambda payload: payload,
        }
    )
    return fn()


def test_dashboard_funnel_counts_checkin_distinct_cohort_users():
    users = _DocCollection(
        {"user_id": uid, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc)}
        for uid in range(1, 11)
    )
    events = []
    for index in range(30):
        events.append(
            {
                "user_id": [1, 2, 3, 4, 999][index % 5],
                "created_at": datetime(2026, 6, 12, tzinfo=timezone.utc),
                "type": "checkin" if index % 3 == 0 else "other",
                "reason": "checkin" if index % 3 == 1 else "other",
                "unique_key": "checkin:20260612" if index % 3 == 2 else "other",
            }
        )

    body = _run_dashboard_funnel(users=users, xp_events=_DocCollection(events))
    stages = {stage["name"]: stage for stage in body["stages"]}

    assert stages["Join Group"]["count"] == 10
    assert stages["Check-in"]["count"] == 4
    assert stages["Check-in"]["conversion_pct"] == 40.0
    assert stages["Check-in"]["dropoff_pct"] == 60.0
    assert stages["Check-in"]["data_quality"] != "invalid"


def test_dashboard_funnel_pm_start_only_counts_cohort_users():
    users = _DocCollection(
        [
            {"user_id": 1, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc), "first_private_interaction_at": datetime(2026, 6, 11, tzinfo=timezone.utc)},
            {"user_id": 2, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc)},
            {"user_id": 3, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc), "first_private_interaction_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
            {"user_id": 999, "first_private_interaction_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
        ]
    )

    body = _run_dashboard_funnel(users=users)
    stages = {stage["name"]: stage for stage in body["stages"]}

    assert stages["Join Group"]["count"] == 3
    assert stages["PM Start"]["count"] == 2
    assert stages["PM Start"]["conversion_pct"] == 66.7


def test_dashboard_funnel_welcome_claim_uses_affiliate_welcome_issued_for_cohort():
    users = _DocCollection(
        {"user_id": uid, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc)}
        for uid in range(1, 11)
    )
    affiliate = _DocCollection(
        [
            {"user_id": 1, "ledger_type": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
            {"user_id": 2, "tier": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
            {"user_id": 3, "pool_id": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
            {"user_id": 999, "pool_id": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
            {"user_id": 4, "pool_id": "WELCOME", "status": "PENDING_MANUAL", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
        ]
    )

    body = _run_dashboard_funnel(users=users, affiliate_ledger=affiliate)
    stages = {stage["name"]: stage for stage in body["stages"]}

    assert stages["Welcome Claim"]["count"] == 3
    assert stages["Welcome Claim"]["data_quality"] == "exact"


def test_dashboard_funnel_welcome_eligible_counts_distinct_cohort_users():
    users = _DocCollection(
        {"user_id": uid, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc)}
        for uid in range(1, 6)
    )
    created_at = datetime(2026, 6, 12, tzinfo=timezone.utc)
    welcome_eligibility = _DocCollection(
        [
            {"uid": 1, "created_at": created_at},
            {"user_id": 2, "created_at": created_at},
            {"uid": 2, "created_at": created_at},
            {"uid": 999, "created_at": created_at},
        ]
    )

    body = _run_dashboard_funnel(users=users, welcome_eligibility=welcome_eligibility)
    stage_names = [stage["name"] for stage in body["stages"]]
    stages = {stage["name"]: stage for stage in body["stages"]}

    assert "Welcome Eligible" in stage_names
    assert "Welcome Unlock" not in stage_names
    assert stage_names == [
        "Join Group",
        "Welcome Eligible",
        "PM Start",
        "Check-in",
        "Welcome Claim",
        "First Play",
    ]
    assert stages["Welcome Eligible"]["count"] == 2
    assert stages["Welcome Eligible"]["conversion_pct"] == 40.0
    assert stages["Welcome Eligible"]["note"] == "Eligibility record created on join; not final unlock state."


def test_dashboard_funnel_legacy_welcome_claim_sources_dedupe_same_user():
    users = _DocCollection(
        {"user_id": uid, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc)}
        for uid in range(1, 5)
    )
    claimed_at = datetime(2026, 6, 12, tzinfo=timezone.utc)

    body = _run_dashboard_funnel(
        users=users,
        welcome_eligibility=_DocCollection(
            [
                {"uid": 1, "claimed": True, "claimed_at": claimed_at, "created_at": claimed_at},
                {"user_id": 2, "claimed": True, "claimed_at": claimed_at, "created_at": claimed_at},
            ]
        ),
        new_joiner_claims=_DocCollection(
            [
                {"uid": 1, "claimed_at": claimed_at},
                {"user_id": 3, "claimed_at": claimed_at},
            ]
        ),
        welcome_tickets=_DocCollection(
            [
                {"uid": 3, "status": "claimed", "claimed_at": claimed_at},
                {"user_id": 999, "status": "claimed", "claimed_at": claimed_at},
            ]
        ),
        voucher_claims=_DocCollection(
            [
                {"user_id": 4, "status": "claimed", "claimed_at": claimed_at, "pool_id": "PUBLIC"},
            ]
        ),
    )
    stages = {stage["name"]: stage for stage in body["stages"]}

    assert stages["Welcome Claim"]["count"] == 3


def test_dashboard_funnel_empty_cohort_is_safe():
    body = _run_dashboard_funnel(
        users=_DocCollection(),
        xp_events=_DocCollection(
            [{"user_id": 1, "created_at": datetime(2026, 6, 12, tzinfo=timezone.utc), "type": "checkin"}]
        ),
        affiliate_ledger=_DocCollection(
            [{"user_id": 1, "ledger_type": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)}]
        ),
    )

    assert body["method"] == "join_cohort"
    assert body["cohort_size"] == 0
    for stage in body["stages"]:
        if stage["count"] is not None:
            assert stage["count"] == 0
            assert stage["conversion_pct"] in (0.0, None)
            assert stage["dropoff_pct"] in (0.0, None)


def test_dashboard_funnel_no_stage_conversion_exceeds_100():
    users = _DocCollection(
        {"user_id": uid, "joined_main_at": datetime(2026, 6, 10, tzinfo=timezone.utc)}
        for uid in (1, 2)
    )
    body = _run_dashboard_funnel(
        users=users,
        xp_events=_DocCollection(
            [
                {"user_id": 1, "created_at": datetime(2026, 6, 12, tzinfo=timezone.utc), "type": "checkin"},
                {"user_id": 2, "created_at": datetime(2026, 6, 12, tzinfo=timezone.utc), "reason": "checkin"},
                {"user_id": 999, "created_at": datetime(2026, 6, 12, tzinfo=timezone.utc), "unique_key": "checkin:20260612"},
            ]
        ),
        affiliate_ledger=_DocCollection(
            [
                {"user_id": 1, "ledger_type": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
                {"user_id": 2, "ledger_type": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
                {"user_id": 999, "ledger_type": "WELCOME", "status": "ISSUED", "updated_at": datetime(2026, 6, 12, tzinfo=timezone.utc)},
            ]
        ),
    )

    for stage in body["stages"]:
        if stage["conversion_pct"] is not None:
            assert stage["conversion_pct"] <= 100.0
            assert stage["data_quality"] != "invalid"


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
