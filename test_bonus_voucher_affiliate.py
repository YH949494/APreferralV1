import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _load_get_bonus_voucher():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "get_bonus_voucher"
    )
    fn_node.decorator_list = []
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["get_bonus_voucher"]


def _load_get_campaign_bonus_voucher():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "get_campaign_bonus_voucher"
    )
    fn_node.decorator_list = []
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["get_campaign_bonus_voucher"]


def _load_get_affiliate_bonus_vouchers():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "get_affiliate_bonus_vouchers"
    )
    fn_node.decorator_list = []
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["get_affiliate_bonus_vouchers"]


class _UsersCollection:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, filt, proj=None):  # noqa: ARG002
        return self.docs.get(filt.get("user_id"))


class _AffiliateLedgerCollection:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, filt, sort=None):
        candidates = []
        for doc in self.docs:
            if doc.get("user_id") != filt.get("user_id"):
                continue
            if doc.get("status") != filt.get("status"):
                continue
            if not doc.get("voucher_code"):
                continue
            candidates.append(doc)
        if not candidates:
            return None

        def _sort_key(doc):
            return (
                doc.get("updated_at", datetime.min.replace(tzinfo=timezone.utc)),
                doc.get("created_at", datetime.min.replace(tzinfo=timezone.utc)),
                doc.get("_id", 0),
            )

        candidates.sort(key=_sort_key, reverse=True)
        return candidates[0]

    def find(self, filt):
        class _Cursor:
            def __init__(self, docs):
                self.docs = docs

            def sort(self, fields):
                def _normalize_ts(value):
                    if isinstance(value, datetime):
                        return value
                    return datetime.min.replace(tzinfo=timezone.utc)

                def _sort_key(doc):
                    return (
                        _normalize_ts(doc.get("updated_at")),
                        _normalize_ts(doc.get("created_at")),
                        doc.get("_id", 0),
                    )

                self.docs = sorted(self.docs, key=_sort_key, reverse=True)
                return self

            def __iter__(self):
                return iter(self.docs)

        rows = []
        for doc in self.docs:
            if doc.get("user_id") != filt.get("user_id"):
                continue
            if doc.get("status") != filt.get("status"):
                continue
            if not doc.get("voucher_code"):
                continue
            rows.append(doc)
        return _Cursor(rows)


class _BonusVoucherCollection:
    def __init__(self, voucher):
        self.voucher = voucher

    def find_one(self):
        return self.voucher


class _Logger:
    def __init__(self):
        self.info_calls = []
        self.exception_calls = []

    def info(self, msg, *args):
        self.info_calls.append((msg, args))

    def exception(self, msg, *args):
        self.exception_calls.append((msg, args))


def _jsonify(payload):
    return payload


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


def _build_globals(*, users, affiliate_rows, bonus_voucher, request_user_id, verified_user_id=None, secret_ok=False):
    logger = _Logger()
    fn = _load_get_bonus_voucher()
    verified_user_id = request_user_id if verified_user_id is None else verified_user_id
    fn.__globals__.update(
        {
            "request": _Request({"user_id": str(request_user_id)}),
            "jsonify": _jsonify,
            "users_collection": _UsersCollection(users),
            "affiliate_ledger_collection": _AffiliateLedgerCollection(affiliate_rows),
            "bonus_voucher_collection": _BonusVoucherCollection(bonus_voucher),
            "logger": logger,
            "datetime": datetime,
            "timezone": timezone,
            "pytz": type("Pytz", (), {"UTC": timezone.utc}),
            "DESCENDING": -1,
            "extract_raw_init_data_from_query": lambda req: "init",
            "verify_telegram_init_data": lambda init_data: (True, {"user": {"id": verified_user_id}}, "ok"),
            "_get_admin_secret": lambda req: req.args.get("admin_secret"),
            "_admin_secret_ok": lambda secret: bool(secret_ok and secret),
            "json": __import__("json"),
        }
    )
    return fn, logger


def _build_campaign_globals(*, bonus_voucher, init_ok=True):
    logger = _Logger()
    fn = _load_get_campaign_bonus_voucher()
    fn.__globals__.update(
        {
            "request": _Request({}),
            "jsonify": _jsonify,
            "bonus_voucher_collection": _BonusVoucherCollection(bonus_voucher),
            "logger": logger,
            "datetime": datetime,
            "timezone": timezone,
            "pytz": type("Pytz", (), {"UTC": timezone.utc}),
            "extract_raw_init_data_from_query": lambda req: "init",
            "verify_telegram_init_data": lambda init_data: (init_ok, {"user": {"id": 1001}}, "ok"),
        }
    )
    return fn, logger


def _build_affiliate_history_globals(*, users, affiliate_rows, request_user_id, verified_user_id=None, secret_ok=False):
    logger = _Logger()
    fn = _load_get_affiliate_bonus_vouchers()
    verified_user_id = request_user_id if verified_user_id is None else verified_user_id
    fn.__globals__.update(
        {
            "request": _Request({"user_id": str(request_user_id)}),
            "jsonify": _jsonify,
            "users_collection": _UsersCollection(users),
            "affiliate_ledger_collection": _AffiliateLedgerCollection(affiliate_rows),
            "logger": logger,
            "datetime": datetime,
            "timezone": timezone,
            "pytz": type("Pytz", (), {"UTC": timezone.utc}),
            "DESCENDING": -1,
            "extract_raw_init_data_from_query": lambda req: "init",
            "verify_telegram_init_data": lambda init_data: (True, {"user": {"id": verified_user_id}}, "ok"),
            "_get_admin_secret": lambda req: req.args.get("admin_secret"),
            "_admin_secret_ok": lambda secret: bool(secret_ok and secret),
            "json": __import__("json"),
        }
    )
    return fn, logger


def test_affiliate_user_gets_code():
    fn, _ = _build_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[
            {"_id": 1, "user_id": 9999, "status": "ISSUED", "voucher_code": "OTHER-CODE"},
            {"_id": 2, "user_id": 1001, "status": "ISSUED", "voucher_code": "MINE-CODE"},
        ],
        bonus_voucher=None,
        request_user_id=1001,
    )
    assert fn() == {"code": "MINE-CODE"}


def test_spoofed_user_id_ignored():
    fn, _ = _build_globals(
        users={1001: {"user_id": 1001}, 9999: {"user_id": 9999}},
        affiliate_rows=[
            {"_id": 1, "user_id": 9999, "status": "ISSUED", "voucher_code": "SPOOF-CODE"},
            {"_id": 2, "user_id": 1001, "status": "ISSUED", "voucher_code": "REAL-CODE"},
        ],
        bonus_voucher=None,
        request_user_id=9999,
        verified_user_id=1001,
    )
    assert fn() == {"code": "REAL-CODE"}


def test_latest_affiliate_code_selected():
    now = datetime.now(timezone.utc)
    fn, _ = _build_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[
            {
                "_id": 1,
                "user_id": 1001,
                "status": "ISSUED",
                "voucher_code": "OLDER-CODE",
                "updated_at": now - timedelta(minutes=5),
                "created_at": now - timedelta(minutes=5),
            },
            {
                "_id": 2,
                "user_id": 1001,
                "status": "ISSUED",
                "voucher_code": "NEWEST-CODE",
                "updated_at": now,
                "created_at": now,
            },
        ],
        bonus_voucher=None,
        request_user_id=1001,
    )
    assert fn() == {"code": "NEWEST-CODE"}


def test_non_affiliate_user_gets_null():
    fn, logger = _build_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[
            {"_id": 1, "user_id": 1001, "status": "ISSUED", "voucher_code": ""},
            {"_id": 2, "user_id": 1001, "status": "ISSUED", "voucher_code": None},
        ],
        bonus_voucher=None,
        request_user_id=1001,
    )
    assert fn() == {"code": None}
    assert any("[BONUS][AFFILIATE_MISS]" in c[0] for c in logger.info_calls)


def test_no_cross_user_leak():
    fn, _ = _build_globals(
        users={5000: {"user_id": 5000, "is_admin": True}},
        affiliate_rows=[
            {"_id": 1, "user_id": 1001, "status": "ISSUED", "voucher_code": "USER-A-CODE"},
            {"_id": 2, "user_id": 5000, "status": "ISSUED", "voucher_code": "ADMIN-CODE"},
        ],
        bonus_voucher=None,
        request_user_id=5000,
    )
    assert fn() == {"code": "ADMIN-CODE"}


def test_campaign_code_visible_to_all_users():
    now = datetime.now(timezone.utc)
    fn, logger = _build_campaign_globals(
        bonus_voucher={
            "code": "GLOBAL-CODE",
            "release_time": now - timedelta(minutes=1),
            "expiry": now + timedelta(minutes=1),
        }
    )
    assert fn() == {"code": "GLOBAL-CODE"}
    assert any("[BONUS][CAMPAIGN_HIT]" in c[0] for c in logger.info_calls)


def test_campaign_hidden_if_not_live():
    now = datetime.now(timezone.utc)
    fn, logger = _build_campaign_globals(
        bonus_voucher={
            "code": "GLOBAL-CODE",
            "release_time": now + timedelta(minutes=10),
            "expiry": now + timedelta(minutes=20),
        }
    )
    assert fn() == {"code": None}
    assert any("[BONUS][CAMPAIGN_MISS]" in c[0] for c in logger.info_calls)


def test_init_data_required_for_both_endpoints():
    fn_affiliate, _ = _build_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[{"_id": 1, "user_id": 1001, "status": "ISSUED", "voucher_code": "CODE"}],
        bonus_voucher=None,
        request_user_id=1001,
    )
    fn_affiliate.__globals__["verify_telegram_init_data"] = lambda init_data: (False, {}, "bad")
    assert fn_affiliate() == {"code": None}

    fn_campaign, _ = _build_campaign_globals(
        bonus_voucher={"code": "GLOBAL-CODE", "release_time": datetime.now(timezone.utc) - timedelta(minutes=1)},
        init_ok=False,
    )
    assert fn_campaign() == {"code": None}


def test_affiliate_history_returns_multiple_rewards():
    now = datetime.now(timezone.utc)
    fn, _ = _build_affiliate_history_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[
            {"_id": 1, "user_id": 1001, "status": "ISSUED", "tier": "T1", "voucher_code": "CODE-1", "updated_at": now - timedelta(minutes=3)},
            {"_id": 2, "user_id": 1001, "status": "ISSUED", "tier": "T2", "voucher_code": "CODE-2", "updated_at": now - timedelta(minutes=2)},
            {"_id": 3, "user_id": 1001, "status": "ISSUED", "tier": "T3", "voucher_code": "CODE-3", "updated_at": now - timedelta(minutes=1)},
        ],
        request_user_id=1001,
    )
    payload = fn()
    assert [item["code"] for item in payload["rewards"]] == ["CODE-3", "CODE-2", "CODE-1"]


def test_affiliate_history_no_cross_user_leak_and_spoof_ignored():
    now = datetime.now(timezone.utc)
    fn, _ = _build_affiliate_history_globals(
        users={1001: {"user_id": 1001}, 2002: {"user_id": 2002}},
        affiliate_rows=[
            {"_id": 1, "user_id": 2002, "status": "ISSUED", "tier": "T2", "voucher_code": "OTHER-CODE", "updated_at": now},
            {"_id": 2, "user_id": 1001, "status": "ISSUED", "tier": "T1", "voucher_code": "MY-CODE", "updated_at": now - timedelta(minutes=1)},
        ],
        request_user_id=2002,
        verified_user_id=1001,
    )
    payload = fn()
    assert payload == {"rewards": [{"tier": "T1", "code": "MY-CODE", "issued_at": (now - timedelta(minutes=1)).isoformat()}]}


def test_affiliate_history_empty_when_no_rewards():
    fn, _ = _build_affiliate_history_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[{"_id": 1, "user_id": 1001, "status": "PENDING", "tier": "T1", "voucher_code": "NOPE"}],
        request_user_id=1001,
    )
    assert fn() == {"rewards": []}


def test_affiliate_history_dedup_keeps_latest_same_tier_and_code():
    now = datetime.now(timezone.utc)
    fn, _ = _build_affiliate_history_globals(
        users={1001: {"user_id": 1001}},
        affiliate_rows=[
            {"_id": 1, "user_id": 1001, "status": "ISSUED", "tier": "T2", "voucher_code": "DUP", "updated_at": now - timedelta(minutes=3)},
            {"_id": 2, "user_id": 1001, "status": "ISSUED", "tier": "T2", "voucher_code": "DUP", "updated_at": now - timedelta(minutes=1)},
            {"_id": 3, "user_id": 1001, "status": "ISSUED", "tier": "T3", "voucher_code": "UNIQ", "updated_at": now - timedelta(minutes=2)},
        ],
        request_user_id=1001,
    )
    payload = fn()
    assert [item["code"] for item in payload["rewards"]] == ["DUP", "UNIQ"]
