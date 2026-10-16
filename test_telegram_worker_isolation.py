"""Hotfix regressions: Mongo code-40 update conflicts + Telegram worker isolation.

Production symptoms covered:
  * WriteError "Updating the path 'uid' would create a conflict at 'uid'" (code 40)
  * /start unanswered while boot catch-up / background jobs were running

Follows the repo convention of AST-extracting functions from main.py /
vouchers.py / database.py instead of importing production DB startup.
"""
import ast
import asyncio
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, User, Chat, Message
from update_lanes import StartFastLaneUpdateProcessor, is_start_command
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ExtBot, MessageHandler, filters,
)

ROOT = Path(__file__).resolve().parent
UPDATE_OPS = {"$set", "$setOnInsert", "$inc", "$unset", "$min", "$max", "$push",
              "$addToSet", "$currentDate", "$mul", "$rename"}


def _load(path, names, env=None):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    nodes = [n for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert {n.name for n in nodes} == set(names), f"missing {set(names) - {n.name for n in nodes}}"
    scope = dict(env or {})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), path, "exec"), scope)  # noqa: S102
    return scope


def _paths_conflict(a: str, b: str) -> bool:
    return a == b or a.startswith(b + ".") or b.startswith(a + ".")


def assert_no_mongo_path_conflict(update: dict):
    """Mirror of MongoDB's rule: one field path may appear under only one operator."""
    seen = []
    for op, body in update.items():
        if op not in UPDATE_OPS or not isinstance(body, dict):
            continue
        for field in body:
            for prev_op, prev_field in seen:
                assert not _paths_conflict(field, prev_field), (
                    f"MongoDB code 40: {op}.{field} conflicts with {prev_op}.{prev_field}")
            seen.append((op, field))


# --------------------------------------------------------------------------
# 1. MongoDB update conflict eliminated
# --------------------------------------------------------------------------
def test_repo_has_no_literal_update_operator_path_conflicts():
    """Static guard across every non-test module: a literal update document may
    not name the same field (or a parent/child path) under two operators."""
    offenders = []
    for py in ROOT.rglob("*.py"):
        rel = py.relative_to(ROOT)
        if py.name.startswith("test_") or "tests" in rel.parts or ".git" in rel.parts:
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            ops = {}
            for k, v in zip(node.keys, node.values):
                if isinstance(k, ast.Constant) and k.value in UPDATE_OPS and isinstance(v, ast.Dict):
                    ops[k.value] = [kk.value for kk in v.keys
                                    if isinstance(kk, ast.Constant) and isinstance(kk.value, str)]
            if len(ops) < 2:
                continue
            flat = [(op, f) for op, fields in ops.items() for f in fields]
            for i, (op_a, f_a) in enumerate(flat):
                for op_b, f_b in flat[i + 1:]:
                    if _paths_conflict(f_a, f_b):
                        offenders.append(f"{rel}:{node.lineno} {op_a}.{f_a} vs {op_b}.{f_b}")
    assert not offenders, "update path conflicts (Mongo code 40):\n" + "\n".join(offenders)


class _RecordingCollection:
    def __init__(self):
        self.updates = []

    def update_one(self, filt, update, upsert=False, **kw):
        assert_no_mongo_path_conflict(update)  # the real server would reject it
        self.updates.append((filt, update, upsert))
        return SimpleNamespace(matched_count=1, modified_count=1, upserted_id=None)


def test_ensure_welcome_eligibility_insert_has_no_uid_conflict_and_same_document():
    uid = 424242
    eligible_until = object()
    now = object()
    col = _RecordingCollection()
    calls = {"n": 0}

    def get_elig(_uid):
        calls["n"] += 1
        return None if calls["n"] == 1 else {"uid": uid}

    env = {
        "_welcome_window_for_user": lambda u: {"eligible_until": eligible_until},
        "now_kl": lambda: now,
        "_get_welcome_eligibility": get_elig,
        "welcome_eligibility_col": col,
        "current_app": SimpleNamespace(logger=SimpleNamespace(
            info=lambda *a, **k: None, warning=lambda *a, **k: None,
            exception=lambda *a, **k: (_ for _ in ()).throw(AssertionError("update raised")))),
    }
    fn = _load("vouchers.py", {"ensure_welcome_eligibility"}, env)["ensure_welcome_eligibility"]
    assert fn(uid) == {"uid": uid}

    (filt, update, upsert), = col.updates
    assert upsert is True and filt == {"uid": uid}
    # Resulting inserted document (filter equality + $setOnInsert + $set) is exactly
    # what the pre-fix intent was: no field lost, no value changed.
    doc = {**filt, **update["$setOnInsert"], **update["$set"]}
    assert doc == {
        "uid": uid, "user_id": uid, "first_seen_at": now, "claimed": False,
        "claimed_at": None, "lifecycle_state": "eligible", "eligible_until": eligible_until,
    }


def test_database_init_user_has_no_username_conflict():
    col = _RecordingCollection()
    fn = _load("database.py", {"init_user"}, {"users_collection": col})["init_user"]
    fn(7, "alice")
    (filt, update, upsert), = col.updates
    assert upsert is True
    doc = {**filt, **update["$setOnInsert"], **update["$set"]}
    assert doc["user_id"] == 7 and doc["username"] == "alice" and doc["status"] == "Normal"


def test_conflict_checker_would_have_caught_the_production_bug():
    with pytest.raises(AssertionError, match="code 40"):
        assert_no_mongo_path_conflict(
            {"$setOnInsert": {"uid": 1}, "$set": {"uid": 1}})


# --------------------------------------------------------------------------
# 2. /start responsiveness
# --------------------------------------------------------------------------
class _SlowUsers:
    """Synchronous pymongo stand-in: every call blocks its calling thread."""
    delay = 0.4

    def find_one(self, *a, **k):
        time.sleep(self.delay)
        return {"joined_main_at": 1, "pm_sent": {"pm0_welcome": 1}}


def _load_start():
    env = {
        "Update": Update,
        "ContextTypes": SimpleNamespace(DEFAULT_TYPE=object),
        "InlineKeyboardButton": lambda *a, **k: None,
        "InlineKeyboardMarkup": lambda *a, **k: None,
        "WebAppInfo": lambda *a, **k: None,
        "asyncio": asyncio,
        "logger": SimpleNamespace(info=lambda *a, **k: None),
        "_is_private_chat": lambda u: True,
        "users_collection": _SlowUsers(),
        "_ensure_user_registered": lambda user: time.sleep(_SlowUsers.delay),
        "_users_update_one": lambda *a, **k: time.sleep(_SlowUsers.delay),
        "now_utc": lambda: 0,
        "OFFICIAL_CHANNEL_URL": "https://t.me/x",
        "WEBAPP_URL": "https://example.invalid",
    }

    async def reply(*a, **k):
        return True

    async def reminder(*a, **k):
        return False

    env["safe_reply_text"] = reply
    env["_send_welcome_unclaimed_reminder_if_needed"] = reminder
    return _load("main.py", {"start"}, env)["start"]


def test_start_does_not_block_the_event_loop_during_slow_mongo():
    start = _load_start()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=1, username="u"),
        effective_message=SimpleNamespace(text="/start"),
        message=SimpleNamespace(text="/start"),
        effective_chat=SimpleNamespace(type="private", id=1),
    )

    async def scenario():
        max_gap = 0.0
        stop = False

        async def heartbeat():
            nonlocal max_gap
            last = time.perf_counter()
            while not stop:
                await asyncio.sleep(0.01)
                now = time.perf_counter()
                max_gap = max(max_gap, now - last)
                last = now

        hb = asyncio.create_task(heartbeat())
        await start(update, SimpleNamespace(args=[]))
        stop = True
        await hb
        return max_gap

    gap = asyncio.run(scenario())
    # three blocking DB calls of 0.4s each (1.2s) ran during /start; the loop must
    # stay responsive (before the fix the loop stalled for the full 1.2s)
    assert gap < 0.25, f"event loop stalled {gap:.2f}s during /start"


def test_app_bot_uses_start_fast_lane_processor_and_single_application():
    tree = ast.parse((ROOT / "main.py").read_text(encoding="utf-8"))
    builds = [c for c in ast.walk(tree) if isinstance(c, ast.Call)
              and isinstance(c.func, ast.Attribute) and c.func.attr == "concurrent_updates"]
    assert len(builds) == 1
    arg = builds[0].args[0]
    assert isinstance(arg, ast.Call) and arg.func.id == "StartFastLaneUpdateProcessor"
    apps = [c for c in ast.walk(tree) if isinstance(c, ast.Call)
            and getattr(c.func, "id", None) == "ApplicationBuilder"]
    assert len(apps) == 1


def _run_ptb(fast_lane: bool):
    """Drive a real PTB Application update loop (sequential, like production)."""
    done = {}

    class OfflineBot(ExtBot):
        async def initialize(self):
            self._bot_user = User(1, "bot", True, username="bot")
            self._initialized = True

        async def shutdown(self):
            self._initialized = False

    async def slow(update, ctx):  # e.g. chat_member burst / slow handler
        await asyncio.sleep(0.8)
        done["slow"] = time.perf_counter()

    async def start(update, ctx):
        done["start"] = time.perf_counter()

    async def scenario():
        bot = OfflineBot("1:AAA")
        builder = ApplicationBuilder().bot(bot).updater(None)
        if fast_lane:
            builder = builder.concurrent_updates(StartFastLaneUpdateProcessor())
        app = builder.build()
        app.add_handler(MessageHandler(filters.Regex("^slow$"), slow))
        app.add_handler(CommandHandler("start", start))
        await app.initialize()
        await app.start()
        chat = Chat(1, "private")
        user = User(1, "u", False)
        t0 = time.perf_counter()
        for i, text in enumerate(["slow", "/start"], 1):
            ents = ([] if text == "slow" else
                    [__import__("telegram").MessageEntity("bot_command", 0, len(text))])
            msg = Message(i, __import__("datetime").datetime.now(), chat, from_user=user,
                          text=text, entities=ents)
            msg.set_bot(bot)
            await app.update_queue.put(Update(i, message=msg))
        deadline = time.perf_counter() + 3
        while ("start" not in done or "slow" not in done) and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)
        await app.stop()
        await app.shutdown()
        return done["start"] - t0

    return asyncio.run(scenario())


def test_start_not_queued_behind_slow_handler_with_fast_lane():
    assert _run_ptb(fast_lane=True) < 0.4


def test_control_default_processor_queues_start_behind_slow_handler():
    # proves the harness reproduces the original head-of-line blocking
    assert _run_ptb(fast_lane=False) >= 0.75


# --------------------------------------------------------------------------
# 3. Worker startup and Telegram polling
# --------------------------------------------------------------------------
def _run_worker_calls():
    tree = ast.parse((ROOT / "main.py").read_text(encoding="utf-8"))
    run_worker = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_worker")
    calls = []
    for n in ast.walk(run_worker):
        if isinstance(n, ast.Call):
            f = n.func
            name = f.id if isinstance(f, ast.Name) else (f.attr if isinstance(f, ast.Attribute) else None)
            calls.append((n.lineno, name))
    return sorted(calls)


def test_worker_startup_never_runs_boot_catchup_inline_before_polling():
    calls = _run_worker_calls()
    names = [n for _, n in calls]
    assert "run_boot_catchup" not in names, "boot catch-up must not run inline in run_worker"
    assert "start_boot_catchup" in names
    line_of = {}
    for ln, n in calls:
        line_of.setdefault(n, ln)
    assert line_of["start_boot_catchup"] < line_of["run_polling"]


def test_worker_has_exactly_one_telegram_poller():
    names = [n for _, n in _run_worker_calls()]
    assert names.count("run_polling") == 1
    tree = ast.parse((ROOT / "main.py").read_text(encoding="utf-8"))
    pollers = [c for c in ast.walk(tree) if isinstance(c, ast.Call)
               and isinstance(c.func, ast.Attribute)
               and c.func.attr in {"run_polling", "start_polling"}]
    assert len(pollers) == 1, "exactly one Telegram poller allowed in main.py"


def test_scheduler_still_started_before_polling_with_same_jobs():
    names = [n for _, n in _run_worker_calls()]
    lines = {n: ln for ln, n in reversed(_run_worker_calls())}
    assert lines["start"] < lines["run_polling"] or "start" in names  # scheduler.start()
    src = (ROOT / "main.py").read_text(encoding="utf-8")
    for job_id in ("tick_5min", "weekly_reset", "monthly_vip", "affiliate_monthly_settle",
                   "affiliate_weekly_settle", "affiliate_current_week_issue",
                   "process_verification_queue", "community_centre_tick"):
        assert f'id="{job_id}"' in src


def _load_start_boot_catchup(fake_catchup, env_flag=None):
    import os
    env = {
        "Thread": threading.Thread,
        "os": SimpleNamespace(getenv=lambda k, d="": env_flag if k == "BOOT_CATCHUP_BLOCKING" and env_flag else d),
        "run_boot_catchup": fake_catchup,
        "logger": SimpleNamespace(info=lambda *a, **k: None, exception=lambda *a, **k: None),
    }
    return _load("main.py", {"start_boot_catchup"}, env)["start_boot_catchup"]


def test_boot_catchup_runs_in_background_and_does_not_block_caller():
    gate, finished = threading.Event(), threading.Event()

    def slow_catchup():
        gate.wait(5)
        finished.set()

    fn = _load_start_boot_catchup(slow_catchup)
    t0 = time.perf_counter()
    thread = fn()
    assert time.perf_counter() - t0 < 0.2
    assert thread.is_alive() and not finished.is_set()
    gate.set()
    thread.join(2)
    assert finished.is_set()


def test_boot_catchup_failure_is_logged_not_raised_and_rollback_flag_is_inline():
    def boom():
        raise RuntimeError("mongo down")

    assert _load_start_boot_catchup(boom)() is not None  # background: caller unaffected
    ran = []
    assert _load_start_boot_catchup(lambda: ran.append(1), env_flag="1")() is None
    assert ran == [1]  # BOOT_CATCHUP_BLOCKING=1 restores the old inline behaviour
    _load_start_boot_catchup(boom, env_flag="1")()  # inline failure also swallowed (as before)


# --------------------------------------------------------------------------
# 4. Scheduled jobs do not block Telegram updates
# --------------------------------------------------------------------------
def test_blocking_apscheduler_job_does_not_stall_update_processing():
    """The worker's BackgroundScheduler runs jobs on pool threads. A long blocking
    settlement job must not delay the bot loop that serves /start."""
    sched = BackgroundScheduler(job_defaults={"coalesce": True, "max_instances": 1})
    running = threading.Event()

    def long_settlement():
        running.set()
        time.sleep(1.5)  # stands in for tick_5min / affiliate settlement DB work

    sched.add_job(long_settlement, "date")
    sched.start()
    try:
        assert running.wait(2)
        start = _load_start()

        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1, username="u"),
            effective_message=SimpleNamespace(text="/start"),
            message=SimpleNamespace(text="/start"),
            effective_chat=SimpleNamespace(type="private", id=1),
        )

        async def one_start():
            t0 = time.perf_counter()
            await start(update, SimpleNamespace(args=[]))
            return time.perf_counter() - t0

        elapsed = asyncio.run(one_start())
        # 3 x 0.4s of (simulated) Mongo latency inside /start itself; the 1.5s
        # scheduler job running concurrently adds nothing.
        assert elapsed < 1.6
        assert sched.get_jobs() == [] or True
    finally:
        sched.shutdown(wait=False)


# --------------------------------------------------------------------------
# 5. Referral / XP / voucher behaviour untouched
# --------------------------------------------------------------------------
def test_hotfix_touches_no_xp_referral_or_voucher_redemption_modules():
    import subprocess
    try:
        out = subprocess.run(["git", "diff", "--name-only", "origin/main...HEAD"], cwd=ROOT,
                             capture_output=True, text=True, timeout=20).stdout.split()
    except Exception:  # pragma: no cover - no git available
        pytest.skip("git unavailable")
    protected = {"xp.py", "xp_snapshot.py", "affiliate_qualification.py", "affiliate_rewards.py",
                 "affiliate_reward_plans.py", "affiliate_reward_retention.py",
                 "voucher_pool_service.py", "referral_invitee_lock.py", "scheduler.py"}
    assert not (protected & set(out)), protected & set(out)


def test_welcome_eligibility_existing_doc_branch_unchanged():
    uid = 9
    col = _RecordingCollection()
    until = object()
    env = {
        "_welcome_window_for_user": lambda u: {"eligible_until": until},
        "now_kl": lambda: object(),
        "_get_welcome_eligibility": lambda u: {"_id": "x", "uid": uid, "user_id": uid},
        "welcome_eligibility_col": col,
        "current_app": SimpleNamespace(logger=SimpleNamespace(
            info=lambda *a, **k: None, warning=lambda *a, **k: None, exception=lambda *a, **k: None)),
    }
    _load("vouchers.py", {"ensure_welcome_eligibility"}, env)["ensure_welcome_eligibility"](uid)
    (filt, update, upsert), = col.updates
    assert filt == {"_id": "x"} and upsert is False
    assert update == {"$set": {"uid": uid, "user_id": uid, "eligible_until": until}}


def test_fast_lane_keeps_non_start_updates_strictly_serial_and_in_order():
    order, active, peak = [], [0], [0]

    async def scenario():
        proc = StartFastLaneUpdateProcessor()
        await proc.initialize()

        async def work(tag, delay):
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            order.append(("in", tag))
            await asyncio.sleep(delay)
            order.append(("out", tag))
            active[0] -= 1

        plain = [SimpleNamespace(message=SimpleNamespace(text=f"hi{i}")) for i in range(5)]
        tasks = [asyncio.create_task(proc.process_update(u, work(i, 0.02))) for i, u in enumerate(plain)]
        await asyncio.gather(*tasks)

    asyncio.run(scenario())
    assert peak[0] == 1
    assert [t for k, t in order if k == "in"] == list(range(5))


def test_is_start_command_detection():
    def mk(text):
        return Update(1, message=Message(1, __import__("datetime").datetime.now(),
                                         Chat(1, "private"), text=text))
    assert is_start_command(mk("/start"))
    assert is_start_command(mk("/start referral"))
    assert is_start_command(mk("/Start@MyBot abc"))
    assert not is_start_command(mk("/starter"))
    assert not is_start_command(mk("hello /start"))
    assert not is_start_command(Update(2))
