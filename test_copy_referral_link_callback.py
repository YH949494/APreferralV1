import ast
import asyncio
from pathlib import Path

import pytest
from telegram import Update
from telegram.ext import ContextTypes


def _load_callback_func():
    """Extract copy_referral_link_callback from main.py via AST, mirroring
    the isolation pattern used by test_referral_link_callback.py, so we
    avoid importing main.py's heavy module-level side effects.
    """
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_nodes = [
        node
        for node in module.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "copy_referral_link_callback"
    ]
    isolated = ast.Module(body=fn_nodes, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {"Update": Update, "ContextTypes": ContextTypes}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["copy_referral_link_callback"]


class _Logger:
    def __init__(self):
        self.errors = []

    def error(self, *args, **kwargs):
        self.errors.append(args)


class _FakeUser:
    def __init__(self, user_id, username="tester"):
        self.id = user_id
        self.username = username


class _FakeQuery:
    def __init__(self, user_id):
        self.from_user = _FakeUser(user_id)
        self.answer_calls = []

    async def answer(self, *args, **kwargs):
        self.answer_calls.append({"args": args, "kwargs": kwargs})


class _FakeUpdate:
    def __init__(self, user_id):
        self.callback_query = _FakeQuery(user_id)


class _FakeContext:
    pass


@pytest.fixture
def copy_callback_env():
    fn = _load_callback_func()
    logger = _Logger()
    state = {"link": "https://t.me/+existingHash", "raise_error": None}
    get_or_create_calls = []

    def fake_get_or_create(user_id, username=""):
        get_or_create_calls.append({"user_id": user_id, "username": username})
        if state["raise_error"] is not None:
            raise state["raise_error"]
        return state["link"]

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class _FakeAsyncioModule:
        to_thread = staticmethod(fake_to_thread)

    fn.__globals__.update(
        {
            "logger": logger,
            "get_or_create_referral_invite_link_sync": fake_get_or_create,
            "asyncio": _FakeAsyncioModule(),
        }
    )
    fn._logger = logger
    fn._state = state
    fn._get_or_create_calls = get_or_create_calls
    return fn


def _run(fn, uid):
    update = _FakeUpdate(uid)
    asyncio.run(fn(update, _FakeContext()))
    return update.callback_query


def test_copy_link_answers_with_existing_link_reusing_canonical_generator(copy_callback_env):
    copy_callback_env._state["link"] = "https://t.me/+preexistingHash"
    query = _run(copy_callback_env, uid=301)

    assert len(copy_callback_env._get_or_create_calls) == 1
    assert copy_callback_env._get_or_create_calls[0]["user_id"] == 301

    assert len(query.answer_calls) == 1
    call = query.answer_calls[0]
    assert call["args"][0] == "https://t.me/+preexistingHash"
    assert call["kwargs"].get("show_alert") is True


def test_copy_link_never_mints_a_second_link_it_reuses_generator(copy_callback_env):
    _run(copy_callback_env, uid=302)
    _run(copy_callback_env, uid=302)

    # Two taps call the canonical reuse/creation function twice, same as any
    # other caller -- get_or_create_referral_invite_link_sync itself owns
    # dedup via invite_link_map, so this route creates no state of its own.
    assert len(copy_callback_env._get_or_create_calls) == 2


def test_copy_link_failure_shows_retry_alert_and_logs_error(copy_callback_env):
    copy_callback_env._state["raise_error"] = RuntimeError("boom")
    query = _run(copy_callback_env, uid=303)

    assert len(query.answer_calls) == 1
    call = query.answer_calls[0]
    assert "try again" in call["args"][0].lower()
    assert call["kwargs"].get("show_alert") is True

    errored = [a for a in copy_callback_env._logger.errors if a and a[0] == "[REFERRAL_DEEPLINK][ERROR] uid=%s err=%s"]
    assert errored
    assert errored[-1][1] == 303
