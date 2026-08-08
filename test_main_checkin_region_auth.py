"""Identity-source tests for /api/checkin and /api/set-region.

Both routes write reward/profile state (XP grants, streak advance, users
upserts, and a permanently write-once region). Before the fix they took the
acting user straight from the request body / path with no Telegram initData
verification at all, so any unauthenticated caller could act as any Telegram
id. These tests pin the identity source to verified initData.

Follows the isolated-AST-exec convention used by test_main_admin_auth.py so
main.py does not have to be imported (it opens Mongo at import time).
"""

import ast
from pathlib import Path

import pytest


def _load_route(name):
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    # Drop @app.route so the function can be exec'd standalone.
    fn_node.decorator_list = []
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    return fn_node, isolated


class _Request:
    def __init__(self, body=None):
        self._body = body or {}
        self.headers = {}
        self.cookies = {}
        self.remote_addr = "203.0.113.7"
        self.json = self._body

    def get_json(self, silent=False):  # noqa: ARG002
        return self._body


class _Users:
    def __init__(self, docs=None):
        self.docs = docs or {}
        self.updates = []

    def find_one(self, filt, projection=None):  # noqa: ARG002
        return self.docs.get(filt.get("user_id"))


def _jsonify(payload):
    return payload


# --------------------------------------------------------------------------
# /api/checkin
# --------------------------------------------------------------------------

VERIFIED_UID = 1111
ATTACKER_BODY_UID = 2222


def _run_checkin(verified, body):
    _, isolated = _load_route("api_checkin")
    checked_in_as = {}

    async def fake_process_checkin(user_id, username, region, source=None, request_id=None):  # noqa: ARG001
        checked_in_as["user_id"] = user_id
        return {"success": True, "streak": 1, "total_xp": 20}

    class _FakeAsyncio:
        @staticmethod
        def run(coro):
            import asyncio as _a

            return _a.new_event_loop().run_until_complete(coro)

    import datetime as _dt

    import pytz as _pytz

    env = {
        "request": _Request(body),
        "jsonify": _jsonify,
        "users_collection": _Users({VERIFIED_UID: {"user_id": VERIFIED_UID, "region": "MY"}}),
        "record_user_last_seen": lambda *a, **k: None,
        "db": object(),
        "_extract_verified_telegram_user_id": lambda: verified,
        "process_checkin": fake_process_checkin,
        "asyncio": _FakeAsyncio,
        "pytz": _pytz,
        "datetime": _dt.datetime,
        "timedelta": _dt.timedelta,
        "traceback": __import__("traceback"),
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["api_checkin"](), checked_in_as


def test_checkin_ignores_body_user_id_and_uses_verified_initdata():
    body = {"user_id": ATTACKER_BODY_UID, "username": "attacker"}
    result, checked_in_as = _run_checkin((VERIFIED_UID, None), body)

    assert checked_in_as["user_id"] == VERIFIED_UID, (
        "check-in must act on the signed initData user, never the body's user_id"
    )
    assert checked_in_as["user_id"] != ATTACKER_BODY_UID


def test_checkin_without_init_data_is_rejected():
    body = {"user_id": ATTACKER_BODY_UID, "username": "attacker"}
    (payload, status), checked_in_as = _run_checkin(
        (None, ({"ok": False, "error": "Missing init_data"}, 400)), body
    )

    assert status == 400
    assert payload["success"] is False
    assert checked_in_as == {}, "no check-in may run for an unauthenticated request"


def test_checkin_with_invalid_init_data_is_rejected():
    body = {"user_id": ATTACKER_BODY_UID}
    (payload, status), checked_in_as = _run_checkin(
        (None, ({"ok": False, "error": "Unauthorized"}, 403)), body
    )

    assert status == 403
    assert checked_in_as == {}


def test_checkin_route_no_longer_reads_identity_from_body():
    fn_node, _ = _load_route("api_checkin")
    src = ast.unparse(fn_node)
    assert "data.get('user_id')" not in src, (
        "body user_id must not be re-introduced as the identity source"
    )
    assert "_extract_verified_telegram_user_id" in src


# --------------------------------------------------------------------------
# /api/set-region
# --------------------------------------------------------------------------


def _run_set_region(verified, path_user_id, region="MY"):
    _, isolated = _load_route("api_set_region")
    users = _Users()
    writes = []

    env = {
        "request": _Request({"region": region}),
        "jsonify": _jsonify,
        "users_collection": users,
        "get_app_setting": lambda *a, **k: True,
        "_extract_verified_telegram_user_id": lambda: verified,
        "_users_update_one": lambda filt, update, upsert=False, context=None: writes.append(
            (filt, update)
        ),
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["api_set_region"](path_user_id), writes


def test_set_region_rejects_locking_another_users_region():
    """Region is write-once, so cross-user writes are irreversible griefing."""
    (payload, status), writes = _run_set_region((VERIFIED_UID, None), ATTACKER_BODY_UID)

    assert status == 403
    assert payload["success"] is False
    assert writes == [], "must not write a region for a user the caller is not"


def test_set_region_allows_own_user():
    result, writes = _run_set_region((VERIFIED_UID, None), VERIFIED_UID)

    assert result["success"] is True
    assert len(writes) == 1
    assert writes[0][0]["user_id"] == VERIFIED_UID


def test_set_region_without_init_data_is_rejected():
    (payload, status), writes = _run_set_region(
        (None, ({"ok": False, "error": "Missing init_data"}, 400)), VERIFIED_UID
    )

    assert status == 400
    assert writes == []


if __name__ == "__main__":
    pytest.main([__file__])
