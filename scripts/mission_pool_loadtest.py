#!/usr/bin/env python3
"""NON-PRODUCTION synthetic load test for the Mission Reward Pool (spec §39).

Refuses to run against a real MongoDB by default. It exercises four things
separately, against an in-memory Mongo double plus the real Flask handlers:

  A. mission submission (the request hot path)
  B. eligibility / finalization
  C. voucher allocation
  D. notification processing (Telegram send stubbed — no outbound traffic)

Usage
-----
    python scripts/mission_pool_loadtest.py --scenario 1000
    python scripts/mission_pool_loadtest.py --scenario 5000  --concurrency 64
    python scripts/mission_pool_loadtest.py --scenario 10000 --concurrency 128
    python scripts/mission_pool_loadtest.py --burst-rps 200 --burst-seconds 5

What the numbers do and do not mean
-----------------------------------
Latency here measures the handler + validation + index-enforced write path
against an in-memory store. It is a faithful measure of the *work the request
does* (how many round trips, how much CPU, whether anything unbounded creeps
in) and of contention behaviour, and it is NOT a substitute for a staging run
against real MongoDB and real network latency: absolute p95/p99 on production
hardware will be higher. Treat a regression in these numbers as a real
regression; treat the absolute values as a floor, not a forecast.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CAMPAIGN_ID = "loadtest-mission"
POOL_ID = "LOADTEST-POOL"


def _guard_against_production() -> None:
    if os.getenv("MISSION_LOADTEST_ALLOW_PRODUCTION") == "1":
        return
    mongo_url = os.getenv("MONGO_URL", "")
    if mongo_url and not any(h in mongo_url for h in ("localhost", "127.0.0.1")):
        sys.exit(
            "Refusing to run: MONGO_URL points at a non-local database.\n"
            "This tool is for non-production load testing only. It never writes to\n"
            "the configured database (it uses an in-memory double), but the guard\n"
            "stays in place so it can never be pointed at production by accident."
        )


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def _stats(latencies_ms: list[float]) -> dict:
    if not latencies_ms:
        return {"p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "mean_ms": 0, "max_ms": 0}
    return {
        "p50_ms": round(_percentile(latencies_ms, 50), 3),
        "p95_ms": round(_percentile(latencies_ms, 95), 3),
        "p99_ms": round(_percentile(latencies_ms, 99), 3),
        "mean_ms": round(statistics.fmean(latencies_ms), 3),
        "max_ms": round(max(latencies_ms), 3),
    }


def _resources() -> dict:
    """CPU/RAM where the platform exposes it, honestly reported as
    unavailable where it does not (§39)."""
    out: dict = {}
    try:
        usage = __import__("resource").getrusage(__import__("resource").RUSAGE_SELF)
        out["max_rss_mb"] = round(usage.ru_maxrss / 1024.0, 1)
        out["cpu_user_s"] = round(usage.ru_utime, 3)
        out["cpu_sys_s"] = round(usage.ru_stime, 3)
    except Exception:
        out["max_rss_mb"] = "unavailable"
        out["cpu_user_s"] = "unavailable"
    out["note"] = (
        "Connection-pool behaviour is not measured: this harness uses an "
        "in-memory Mongo double, so there is no pool to observe. Measure it "
        "in staging against real MongoDB."
    )
    return out


class _HashedUniqueCollection:
    """Mixin that replaces FakeCollection's O(n) unique-index scan with an
    O(1) hashed lookup.

    Without it the harness itself is quadratic in the number of entries and
    the reported submission latency measures the *test double*, not the
    handler. Real MongoDB enforces a unique index with a B-tree lookup, so
    this makes the measurement closer to production behaviour, not less
    faithful: the semantics ("exactly one insert wins") are unchanged and
    still enforced under the same lock.
    """

    def _hashed_setup(self):
        self._unique_sets = {i: set() for i, _ in enumerate(self._unique_keys)}

    def _check_unique(self, doc, exclude=None):
        from fake_mongo import DuplicateKeyError, _matches

        for i, spec in enumerate(self._unique_keys):
            if isinstance(spec, tuple) and len(spec) == 2 and isinstance(spec[1], dict):
                keyset, partial_filter = spec
            else:
                keyset, partial_filter = spec, None
            if partial_filter is not None and not _matches(doc, partial_filter):
                continue
            key = tuple(repr(doc.get(k)) for k in keyset)
            if key in self._unique_sets[i]:
                raise DuplicateKeyError(f"duplicate key on {keyset}")
            self._unique_sets[i].add(key)


# Per-collection database-operation counters. The wall clock of this harness
# is bounded by the test double's linear scans, not by production index
# behaviour, so the operation COUNT is the production-relevant number: it says
# how many round trips each entry costs, which is what actually determines
# cost against real MongoDB.
DB_OPS: dict = {}
_DB_OPS_LOCK = threading.Lock()
_COUNTED_METHODS = ("find", "find_one", "insert_one", "update_one", "update_many",
                    "find_one_and_update", "count_documents", "distinct")


def reset_db_ops() -> None:
    with _DB_OPS_LOCK:
        DB_OPS.clear()


def db_ops_snapshot() -> dict:
    with _DB_OPS_LOCK:
        return {k: dict(v) for k, v in sorted(DB_OPS.items())}


def _make_indexed_db(unique_keys_by_collection: dict):
    from fake_mongo import FakeCollection, FakeDb

    class IndexedCollection(_HashedUniqueCollection, FakeCollection):
        def __init__(self, name, unique_keys=None):
            FakeCollection.__init__(self, unique_keys)
            self._hashed_setup()
            self._name = name

        def _count(self, method):
            with _DB_OPS_LOCK:
                DB_OPS.setdefault(self._name, {})
                DB_OPS[self._name][method] = DB_OPS[self._name].get(method, 0) + 1

    def _wrap(method_name):
        original = getattr(FakeCollection, method_name)

        def wrapper(self, *args, **kwargs):
            self._count(method_name)
            return original(self, *args, **kwargs)

        return wrapper

    for method_name in _COUNTED_METHODS:
        setattr(IndexedCollection, method_name, _wrap(method_name))

    class IndexedDb(FakeDb):
        def __getitem__(self, name):
            if name not in self._collections:
                self._collections[name] = IndexedCollection(
                    name, self._unique_keys_by_collection.get(name)
                )
            return self._collections[name]

    return IndexedDb(unique_keys_by_collection)


def _build_env(voucher_count: int):
    import database
    import mission_pool as mp
    import mission_pool_processor as mpp
    import voucher_pool_service as vps
    from flask import Flask

    fdb = _make_indexed_db({
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
        "voucher_pools": [("pool_id", "code")],
        "campaign_rewards": [
            ("reward_id",),
            (("campaign_id", "mission_entry_id"), {"category": "mission_pool"}),
            (("campaign_id", "identity_key"), {"category": "mission_pool"}),
        ],
    })
    database.db = fdb
    for module in (mp, mpp, vps):
        module.database = database
    mp.mission_pool_enabled = lambda: True
    mpp.mp.mission_pool_enabled = lambda: True

    now = datetime.now(timezone.utc)
    fdb["gc_campaigns"].insert_one({
        "campaign_id": CAMPAIGN_ID, "name": "Load Test Mission",
        "type": "mission_pool", "mechanic": "mission_pool", "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=6)},
        "mission_config": {
            "mission_type": "multiple_choice", "prompt": "Pick one",
            "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "correct_answer": "a",
        },
        "mission_pool": {
            "pool_id": POOL_ID, "pool_type": "voucher_drop",
            "winner_count": 300, "allocation_method": "random_qualified",
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "processing_stage": mp.STAGE_PENDING, "processing_generation": 0,
        },
    })
    fdb["voucher_pool_registry"].insert_one({
        "pool_id": POOL_ID, "name": "Load", "pool_type": "voucher_drop",
        "allocation_scope": "campaign_rewards", "status": "active",
    })
    for i in range(voucher_count):
        fdb["voucher_pools"].insert_one({
            "pool_id": POOL_ID, "code": f"LT{i:06d}", "status": "available",
            "issued_to": None, "issued_at": None, "pool_source": "campaign_centre",
            "pool_type": "voucher_drop", "allocation_scope": "campaign_rewards",
        })

    app = Flask(__name__)
    app.register_blueprint(mp.mission_pool_bp)
    return fdb, app, mp, mpp


def _stub_initdata_verification():
    """Patch Telegram signature verification ONCE, for the whole run, and
    derive the uid from the init_data value the request carries. Per-request
    ``mock.patch`` would otherwise dominate the measurement — this keeps the
    timing on the handler, the validation and the indexed write."""
    def verify(raw):
        return True, {"user": '{"id": %s}' % str(raw).split(":", 1)[-1]}, "ok"

    return patch("vouchers.verify_telegram_init_data", side_effect=verify)


def scenario_a_submissions(app, mp, total: int, concurrency: int, *,
                           duplicate_ratio: float = 0.1) -> dict:
    """A. Mission submission under concurrency, including a duplicate-retry
    share so the unique-index path is exercised too."""
    latencies: list[float] = []
    lock = threading.Lock()
    counters = {"ok": 0, "duplicate": 0, "error": 0}
    dup_every = max(2, int(1 / duplicate_ratio)) if duplicate_ratio else 0

    def one(i: int):
        uid = 10_000_000 + (i - 1 if dup_every and i % dup_every == 0 else i)
        client = app.test_client()
        started = time.perf_counter()
        resp = client.post(
            f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=uid:{uid}", json={"answer": "a"}
        )
        elapsed_ms = (time.perf_counter() - started) * 1000
        body = resp.get_json() or {}
        with lock:
            latencies.append(elapsed_ms)
            if resp.status_code != 200:
                counters["error"] += 1
            elif body.get("state") == "already_submitted":
                counters["duplicate"] += 1
            else:
                counters["ok"] += 1

    reset_db_ops()
    started = time.perf_counter()
    with _stub_initdata_verification():
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(one, range(total)))
    duration = time.perf_counter() - started
    ops = db_ops_snapshot()

    return {
        "stage": "A_submission",
        "requests": total,
        "concurrency": concurrency,
        "successful": counters["ok"],
        "idempotent_duplicates": counters["duplicate"],
        "errors": counters["error"],
        "error_rate": round(counters["error"] / total, 6) if total else 0,
        "duration_s": round(duration, 3),
        "throughput_rps": round(total / duration, 1) if duration else 0,
        "latency": _stats(latencies),
        "db_ops": ops,
        "db_ops_per_request": {
            col: {m: round(n / total, 3) for m, n in methods.items()}
            for col, methods in ops.items()
        },
    }


def scenario_burst(app, mp, rps: int, seconds: int) -> dict:
    """Short sustained burst at a target rate, reporting achieved rate."""
    latencies: list[float] = []
    lock = threading.Lock()
    errors = {"n": 0}
    sent = {"n": 0}

    def one(uid: int):
        client = app.test_client()
        started = time.perf_counter()
        resp = client.post(
            f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=uid:{uid}", json={"answer": "a"}
        )
        with lock:
            latencies.append((time.perf_counter() - started) * 1000)
            sent["n"] += 1
            if resp.status_code != 200:
                errors["n"] += 1

    started = time.perf_counter()
    uid = 20_000_000
    with _stub_initdata_verification():
        with ThreadPoolExecutor(max_workers=min(256, rps)) as pool:
            for _tick in range(seconds):
                tick_start = time.perf_counter()
                for _ in range(rps):
                    uid += 1
                    pool.submit(one, uid)
                elapsed = time.perf_counter() - tick_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
    duration = time.perf_counter() - started

    return {
        "stage": "burst",
        "target_rps": rps,
        "seconds": seconds,
        "requests": sent["n"],
        "errors": errors["n"],
        "achieved_rps": round(sent["n"] / duration, 1) if duration else 0,
        "duration_s": round(duration, 3),
        "latency": _stats(latencies),
    }


def scenario_bcd_processing(fake_db, mp, mpp) -> dict:
    """B/C/D. Close the campaign, then time eligibility, allocation and
    notification separately by driving the state machine stage by stage."""
    fake_db["gc_campaigns"].update_one({"campaign_id": CAMPAIGN_ID}, {"$set": {"status": "ended"}})
    submitted = fake_db[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": CAMPAIGN_ID})

    timings: dict = {}
    stage_marks: list[tuple[str, float]] = []
    real_set_stage = mpp._set_stage

    def timed_set_stage(fence, stage, now, **extra):
        stage_marks.append((stage, time.perf_counter()))
        return real_set_stage(fence, stage, now, **extra)

    reset_db_ops()
    started = time.perf_counter()
    with patch("telegram_utils.send_telegram_http_message", return_value=(True, None, False)):
        with patch.object(mpp, "_set_stage", side_effect=timed_set_stage):
            for _ in range(50):
                result = mpp.process_campaign(CAMPAIGN_ID)
                block = fake_db["gc_campaigns"].find_one(
                    {"campaign_id": CAMPAIGN_ID})["mission_pool"]
                if block.get("processing_stage") == mp.STAGE_COMPLETED:
                    break
    total_duration = time.perf_counter() - started

    previous = started
    for stage, mark in stage_marks:
        label = {
            mp.STAGE_QUALIFIED_SNAPSHOT_READY: "B_eligibility_s",
            mp.STAGE_WINNERS_SELECTED: "selection_s",
            mp.STAGE_NOTIFYING: "C_allocation_s",
            mp.STAGE_COMPLETED: "D_notification_s",
        }.get(stage)
        if label:
            timings[label] = round(mark - previous, 3)
        previous = mark

    block = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["mission_pool"]
    return {
        "stage": "BCD_processing",
        "submitted_entries": submitted,
        "qualified_count": block.get("qualified_count"),
        "winner_count_requested": block.get("winner_count_requested"),
        "winner_count_actual": block.get("winner_count_actual"),
        "vouchers_issued": fake_db["voucher_pools"].count_documents({"status": "issued"}),
        "rewards_assigned": fake_db["campaign_rewards"].count_documents({"status": "assigned"}),
        "notifications_sent": fake_db["campaign_rewards"].count_documents(
            {"notification_status": "sent"}),
        "final_stage": block.get("processing_stage"),
        "campaign_processing_duration_s": round(total_duration, 3),
        "campaign_processing_duration_note": (
            "Wall clock here is dominated by the in-memory double's linear scans "
            "(every find/find_one is O(collection)), NOT by production index "
            "behaviour. Use db_ops / db_ops_per_entry below as the production-"
            "relevant cost signal, and measure wall clock in staging."
        ),
        "db_ops": db_ops_snapshot(),
        "db_ops_per_entry": {
            col: {m: round(n / max(1, submitted), 3) for m, n in methods.items()}
            for col, methods in db_ops_snapshot().items()
        },
        "stage_timings": timings,
        "worker_backlog_after_run": fake_db[mp.ENTRIES_COLLECTION].count_documents(
            {"campaign_id": CAMPAIGN_ID, "status": mp.ENTRY_STATUS_SUBMITTED}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", type=int, default=1000,
                        help="number of submissions (1000 / 5000 / 10000)")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--vouchers", type=int, default=300,
                        help="pool size; the pilot uses a ~300-voucher pool")
    parser.add_argument("--burst-rps", type=int, default=0)
    parser.add_argument("--burst-seconds", type=int, default=3)
    parser.add_argument("--json", action="store_true", help="machine-readable output only")
    args = parser.parse_args()

    _guard_against_production()

    fake_db, app, mp, mpp = _build_env(args.vouchers)

    report: dict = {
        "scenario_submissions": args.scenario,
        "concurrency": args.concurrency,
        "voucher_pool_size": args.vouchers,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    report["A_submission"] = scenario_a_submissions(app, mp, args.scenario, args.concurrency)
    if args.burst_rps:
        report["burst"] = scenario_burst(app, mp, args.burst_rps, args.burst_seconds)
    report["BCD_processing"] = scenario_bcd_processing(fake_db, mp, mpp)
    report["resources"] = _resources()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    print(json.dumps(report, indent=2, default=str))
    a = report["A_submission"]
    bcd = report["BCD_processing"]
    print("\n--- summary ---")
    print(f"submissions      : {a['requests']} @ {a['throughput_rps']} rps, "
          f"error_rate={a['error_rate']}")
    print(f"submission p50/p95/p99 (ms): {a['latency']['p50_ms']} / "
          f"{a['latency']['p95_ms']} / {a['latency']['p99_ms']}")
    print(f"campaign processing: {bcd['campaign_processing_duration_s']}s "
          f"({bcd['submitted_entries']} entries -> {bcd['winner_count_actual']} winners, "
          f"{bcd['vouchers_issued']} vouchers)")
    print(f"final stage      : {bcd['final_stage']}, backlog={bcd['worker_backlog_after_run']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
