"""Phase 3C tests: backend segment engine admin run endpoint.

Tests:
  1. endpoint rejects missing snapshot_week
  2. endpoint rejects invalid snapshot_week format
  3. dry_run does not write snapshots
  4. commit writes backend_segment_snapshots
  5. repeated commit for same week is idempotent
  6. dashboard panel can display generated snapshots (via build_backend_segment_engine_panel)
"""

from __future__ import annotations

import json
import re
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from flask import Blueprint, Flask, jsonify, request

import backend_segment_engine as engine


# ---------------------------------------------------------------------------
# Shared fake DB helpers
# ---------------------------------------------------------------------------

_SNAPSHOT_WEEK_RE = re.compile(r"^\d{4}-W(?:0[1-9]|[1-4]\d|5[0-3])$")


class _FakeSnapshotsCollection:
    """Minimal upsert-tracking collection stub."""

    def __init__(self):
        self.docs: dict[tuple, dict] = {}

    def bulk_write(self, ops, ordered=False):
        upserted = 0
        modified = 0
        for op in ops:
            filt = getattr(op, "_filter", {})
            update = getattr(op, "_doc", {})
            key = (filt.get("account"), filt.get("snapshot_week"))
            if key in self.docs:
                self.docs[key].update(update.get("$set", {}))
                modified += 1
            else:
                self.docs[key] = update.get("$set", {})
                upserted += 1
        return type("R", (), {"upserted_count": upserted, "modified_count": modified})()

    def count_documents(self, filt=None):
        filt = filt or {}
        wk = filt.get("snapshot_week")
        if wk is None:
            return len(self.docs)
        return sum(1 for (_, w) in self.docs if w == wk)


class _FakeEmptyCollection:
    def find(self, filt=None):
        return []

    def aggregate(self, pipeline):
        return []

    def count_documents(self, filt=None):
        return 0


class _FakeMarketingCollection:
    def __init__(self, docs_by_week: dict[str, list[dict]]):
        self._docs = docs_by_week

    def find(self, filt=None):
        filt = filt or {}
        wk = filt.get("snapshot_week")
        if wk is None:
            return [d for rows in self._docs.values() for d in rows]
        return list(self._docs.get(wk, []))

    def count_documents(self, filt=None):
        filt = filt or {}
        wk = filt.get("snapshot_week")
        return len(self.find(filt)) if wk else sum(len(v) for v in self._docs.values())


def _mkt_col_with_week(week: str):
    return _FakeMarketingCollection({
        week: [
            {"account": "alice", "after_total_bet_amount": 800, "withdraw_amount": 100, "snapshot_week": week},
            {"account": "bob", "after_total_bet_amount": 200, "withdraw_amount": 50, "snapshot_week": week},
        ]
    })


def _users_col():
    return type("UC", (), {"find": lambda self, f=None: []})()


# ---------------------------------------------------------------------------
# Minimal Flask app that replicates the endpoint validation logic
# (avoids importing main.py which triggers DB/bot startup)
# ---------------------------------------------------------------------------

def _make_test_app(marketing_col, snapshots_col):
    """Build a minimal Flask app that mirrors the Phase 3C endpoint."""
    app = Flask(__name__)
    app.secret_key = "test-secret"
    bp = Blueprint("test_bse", __name__)

    @bp.post("/api/admin/dashboard/backend-segment-engine/run")
    def bse_run():
        # Simplified auth: always accept (admin auth tested separately)
        body = request.get_json(silent=True) or {}
        snapshot_week = body.get("snapshot_week")
        dry_run = bool(body.get("dry_run", True))

        if not snapshot_week:
            return jsonify({"ok": False, "error": "snapshot_week is required. Do not omit or default."}), 400
        if not _SNAPSHOT_WEEK_RE.match(str(snapshot_week)):
            return jsonify({"ok": False, "error": f"Invalid snapshot_week format '{snapshot_week}'."}), 400

        mkt_count = marketing_col.count_documents({"snapshot_week": snapshot_week})
        if mkt_count == 0:
            return jsonify({"ok": False, "error": f"No marketing_raw_data found for snapshot_week '{snapshot_week}'."}), 422

        summary = engine.run_shadow_segment_engine(
            users_col=_users_col(),
            voucher_claims_col=_FakeEmptyCollection(),
            marketing_col=marketing_col,
            snapshots_col=snapshots_col,
            snapshot_week=snapshot_week,
            dry_run=dry_run,
        )
        return jsonify({
            "ok": summary.get("ok", False),
            "snapshot_week": snapshot_week,
            "dry_run": dry_run,
            "users_evaluated": summary.get("users_evaluated", 0),
            "snapshots_written": summary.get("snapshots_written", 0),
            "segment_counts": summary.get("segment_distribution", {}),
            "claim_risk_counts": summary.get("claim_risk_distribution", {}),
            "error": summary.get("error"),
        })

    app.register_blueprint(bp)
    return app


# ---------------------------------------------------------------------------
# Tests: HTTP endpoint validation (tests 1 & 2)
# ---------------------------------------------------------------------------

class EndpointValidationTests(unittest.TestCase):
    def setUp(self):
        week = "2026-W25"
        self.mkt = _mkt_col_with_week(week)
        self.snaps = _FakeSnapshotsCollection()
        self.client = _make_test_app(self.mkt, self.snaps).test_client()

    def _post(self, body):
        return self.client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps(body),
            content_type="application/json",
        )

    def test_missing_snapshot_week_returns_400(self):
        r = self._post({"dry_run": True})
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertFalse(d["ok"])
        self.assertIn("snapshot_week", d["error"])

    def test_null_snapshot_week_returns_400(self):
        r = self._post({"snapshot_week": None, "dry_run": True})
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertFalse(d["ok"])

    def test_invalid_week_format_returns_400(self):
        for bad in ["2026-25", "2026W25", "2026-w25", "26-W25", "2026-W00", "2026-W54", "not-a-week"]:
            with self.subTest(bad=bad):
                r = self._post({"snapshot_week": bad, "dry_run": True})
                self.assertEqual(r.status_code, 400, f"Expected 400 for '{bad}'")
                d = json.loads(r.data)
                self.assertFalse(d["ok"])

    def test_valid_week_boundary_values_are_accepted(self):
        for good in ["2026-W01", "2026-W53", "2026-W25", "2099-W12"]:
            with self.subTest(good=good):
                mkt = _mkt_col_with_week(good)
                client = _make_test_app(mkt, _FakeSnapshotsCollection()).test_client()
                r = client.post(
                    "/api/admin/dashboard/backend-segment-engine/run",
                    data=json.dumps({"snapshot_week": good, "dry_run": True}),
                    content_type="application/json",
                )
                self.assertNotEqual(r.status_code, 400, f"Valid week '{good}' should not return 400")

    def test_no_marketing_data_returns_422(self):
        r = self._post({"snapshot_week": "2099-W01", "dry_run": True})
        self.assertEqual(r.status_code, 422)
        d = json.loads(r.data)
        self.assertFalse(d["ok"])
        self.assertIn("No marketing_raw_data", d["error"])


# ---------------------------------------------------------------------------
# Tests: dry_run does not write snapshots (test 3)
# ---------------------------------------------------------------------------

class DryRunTests(unittest.TestCase):
    def test_dry_run_does_not_write_snapshots(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        snaps = _FakeSnapshotsCollection()
        client = _make_test_app(mkt, snaps).test_client()

        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d["ok"])
        self.assertTrue(d["dry_run"])
        self.assertEqual(d["snapshots_written"], 0)
        self.assertEqual(len(snaps.docs), 0, "dry_run must not write any snapshots")

    def test_dry_run_still_evaluates_users(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        snaps = _FakeSnapshotsCollection()
        client = _make_test_app(mkt, snaps).test_client()

        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        d = json.loads(r.data)
        self.assertTrue(d["ok"])
        self.assertGreater(d["users_evaluated"], 0)


# ---------------------------------------------------------------------------
# Tests: commit writes snapshots (test 4)
# ---------------------------------------------------------------------------

class CommitRunTests(unittest.TestCase):
    def _run(self, week, dry_run, snaps=None):
        if snaps is None:
            snaps = _FakeSnapshotsCollection()
        mkt = _mkt_col_with_week(week)
        client = _make_test_app(mkt, snaps).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": dry_run}),
            content_type="application/json",
        )
        return json.loads(r.data), snaps

    def test_commit_writes_backend_segment_snapshots(self):
        week = "2026-W25"
        d, snaps = self._run(week, dry_run=False)
        self.assertTrue(d["ok"])
        self.assertFalse(d["dry_run"])
        self.assertGreater(d["snapshots_written"], 0)
        self.assertGreater(len(snaps.docs), 0)

    def test_commit_returns_correct_users_evaluated(self):
        week = "2026-W25"
        d, _ = self._run(week, dry_run=False)
        self.assertEqual(d["users_evaluated"], 2)  # alice + bob in _mkt_col_with_week

    def test_commit_returns_segment_counts(self):
        week = "2026-W25"
        d, _ = self._run(week, dry_run=False)
        segs = d["segment_counts"]
        self.assertIsInstance(segs, dict)
        total = sum(segs.values())
        self.assertEqual(total, d["users_evaluated"])

    def test_commit_response_has_all_required_fields(self):
        week = "2026-W25"
        d, _ = self._run(week, dry_run=False)
        for field in ("ok", "snapshot_week", "dry_run", "users_evaluated", "snapshots_written", "segment_counts", "claim_risk_counts", "error"):
            self.assertIn(field, d, f"Missing field: {field}")


# ---------------------------------------------------------------------------
# Tests: idempotency for same week (test 5)
# ---------------------------------------------------------------------------

class IdempotencyTests(unittest.TestCase):
    def test_repeated_commit_same_week_is_idempotent(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        snaps = _FakeSnapshotsCollection()

        # First run
        client = _make_test_app(mkt, snaps).test_client()
        r1 = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": False}),
            content_type="application/json",
        )
        d1 = json.loads(r1.data)
        self.assertTrue(d1["ok"])
        count_after_first = len(snaps.docs)

        # Second run (same week, same snaps collection)
        client2 = _make_test_app(mkt, snaps).test_client()
        r2 = client2.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": False}),
            content_type="application/json",
        )
        d2 = json.loads(r2.data)
        self.assertTrue(d2["ok"])
        count_after_second = len(snaps.docs)

        self.assertEqual(count_after_first, count_after_second, "Re-running same week must not add extra docs")
        self.assertEqual(d1["users_evaluated"], d2["users_evaluated"])

    def test_different_weeks_create_separate_snapshots(self):
        week1, week2 = "2026-W24", "2026-W25"
        mkt = _FakeMarketingCollection({
            week1: [{"account": "alice", "after_total_bet_amount": 800, "withdraw_amount": 100, "snapshot_week": week1}],
            week2: [{"account": "alice", "after_total_bet_amount": 500, "withdraw_amount": 50, "snapshot_week": week2}],
        })
        snaps = _FakeSnapshotsCollection()

        for wk in (week1, week2):
            client = _make_test_app(mkt, snaps).test_client()
            r = client.post(
                "/api/admin/dashboard/backend-segment-engine/run",
                data=json.dumps({"snapshot_week": wk, "dry_run": False}),
                content_type="application/json",
            )
            d = json.loads(r.data)
            self.assertTrue(d["ok"], f"Run failed for week {wk}: {d}")

        self.assertEqual(len(snaps.docs), 2)


# ---------------------------------------------------------------------------
# Tests: dashboard panel can display generated snapshots (test 6)
# ---------------------------------------------------------------------------

class DashboardPanelDisplayTests(unittest.TestCase):
    def test_panel_shows_snapshots_after_commit_run(self):
        import dashboard_panels as panels

        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        snaps = _FakeSnapshotsCollection()
        now = datetime(2026, 6, 17, tzinfo=timezone.utc)

        # Run the engine to populate snapshots
        engine.run_shadow_segment_engine(
            users_col=_users_col(),
            voucher_claims_col=_FakeEmptyCollection(),
            marketing_col=mkt,
            snapshots_col=snaps,
            snapshot_week=week,
            now=now,
        )

        # Build a fake panel-query-compatible collection from snaps.docs
        class _PanelSnapshotsCol:
            def __init__(self, docs_dict):
                self._docs = list(docs_dict.values())

            def find(self, filt=None, projection=None):
                filt = filt or {}
                results = self._docs
                if filt.get("snapshot_week"):
                    results = [d for d in results if d.get("snapshot_week") == filt["snapshot_week"]]
                elif filt.get("snapshot_month"):
                    results = [d for d in results if d.get("snapshot_month") == filt["snapshot_month"]]
                return list(results)

            def count_documents(self, filt=None):
                return len(self.find(filt))

            def aggregate(self, pipeline):
                return []

        col = _PanelSnapshotsCol(snaps.docs)
        result = panels.build_backend_segment_engine_panel(
            snapshots_col=col,
            snapshot_week=week,
            now=now,
        )

        self.assertTrue(result.get("success"), f"Panel build failed: {result}")
        summary = result.get("summary", {})
        self.assertGreater(summary.get("total_users_evaluated", 0), 0)
        self.assertIn("segment_distribution", result)
        self.assertIn("claim_risk_distribution", result)

    def test_panel_empty_when_no_snapshots(self):
        import dashboard_panels as panels

        week = "2026-W99"
        now = datetime(2026, 6, 17, tzinfo=timezone.utc)

        class _EmptyCol:
            def find(self, filt=None, projection=None):
                return []
            def count_documents(self, filt=None):
                return 0
            def aggregate(self, pipeline):
                return []

        result = panels.build_backend_segment_engine_panel(
            snapshots_col=_EmptyCol(),
            snapshot_week=week,
            now=now,
        )
        self.assertTrue(result.get("success"))
        summary = result.get("summary", {})
        self.assertEqual(summary.get("total_users_evaluated", 0), 0)


# ---------------------------------------------------------------------------
# Async job infrastructure: shared fakes and factory
# ---------------------------------------------------------------------------

class _FakeRunsCollection:
    """In-memory backend_segment_engine_runs collection."""

    def __init__(self):
        self.docs: dict[str, dict] = {}

    def insert_one(self, doc):
        self.docs[doc["job_id"]] = dict(doc)

    def update_one(self, filt, update):
        job_id = filt.get("job_id")
        if job_id and job_id in self.docs:
            self.docs[job_id].update(update.get("$set", {}))

    def find_one(self, filt, projection=None):
        # Lookup by job_id
        job_id = filt.get("job_id")
        if job_id is not None:
            doc = self.docs.get(job_id)
            return dict(doc) if doc else None
        # Lookup by (snapshot_week, dry_run, status.$in) for duplicate check
        sw = filt.get("snapshot_week")
        dr = filt.get("dry_run")
        status_filt = filt.get("status", {})
        in_list = status_filt.get("$in", []) if isinstance(status_filt, dict) else [status_filt]
        for doc in self.docs.values():
            if doc.get("snapshot_week") == sw and doc.get("dry_run") == dr:
                if doc.get("status") in in_list:
                    return dict(doc)
        return None


def _make_async_test_app(marketing_col, snapshots_col, runs_col, run_bg_synchronously=True):
    """Minimal Flask app mirroring the async Phase 3C POST + GET status endpoints."""
    import uuid as _uuid

    app = Flask(__name__)
    app.secret_key = "test-secret"
    bp = Blueprint("test_bse_async", __name__)

    def _bg_job(job_id, snapshot_week, dry_run):
        runs_col.update_one({"job_id": job_id}, {"$set": {"status": "running"}})
        try:
            summary = engine.run_shadow_segment_engine(
                users_col=_users_col(),
                voucher_claims_col=_FakeEmptyCollection(),
                marketing_col=marketing_col,
                snapshots_col=snapshots_col,
                snapshot_week=snapshot_week,
                dry_run=dry_run,
            )
            status = "success" if summary.get("ok") else "failed"
            runs_col.update_one({"job_id": job_id}, {"$set": {
                "status": status,
                "summary": {
                    "users_evaluated": summary.get("users_evaluated", 0),
                    "snapshots_written": summary.get("snapshots_written", 0),
                    "segment_distribution": summary.get("segment_distribution", {}),
                    "claim_risk_distribution": summary.get("claim_risk_distribution", {}),
                },
                "error": summary.get("error"),
            }})
        except Exception as exc:
            runs_col.update_one({"job_id": job_id}, {"$set": {"status": "failed", "error": str(exc)}})

    @bp.post("/api/admin/dashboard/backend-segment-engine/run")
    def bse_run():
        body = request.get_json(silent=True) or {}
        snapshot_week = body.get("snapshot_week")
        dry_run = bool(body.get("dry_run", True))

        if not snapshot_week:
            return jsonify({"ok": False, "error": "snapshot_week is required. Do not omit or default."}), 400
        if not _SNAPSHOT_WEEK_RE.match(str(snapshot_week)):
            return jsonify({"ok": False, "error": f"Invalid snapshot_week format '{snapshot_week}'."}), 400

        mkt_count = marketing_col.count_documents({"snapshot_week": snapshot_week})
        if mkt_count == 0:
            return jsonify({"ok": False, "error": f"No marketing_raw_data found for snapshot_week '{snapshot_week}'."}), 422

        existing = runs_col.find_one({
            "snapshot_week": snapshot_week,
            "dry_run": dry_run,
            "status": {"$in": ["queued", "running"]},
        })
        if existing:
            return jsonify({
                "ok": False,
                "error": "Job already in progress.",
                "job_id": existing["job_id"],
            }), 409

        job_id = str(_uuid.uuid4())
        runs_col.insert_one({
            "job_id": job_id,
            "snapshot_week": snapshot_week,
            "dry_run": dry_run,
            "status": "queued",
            "summary": None,
            "error": None,
        })
        if run_bg_synchronously:
            _bg_job(job_id, snapshot_week, dry_run)
        return jsonify({
            "ok": True,
            "job_id": job_id,
            "status": "queued",
            "snapshot_week": snapshot_week,
            "dry_run": dry_run,
        })

    @bp.get("/api/admin/dashboard/backend-segment-engine/run-status")
    def bse_run_status():
        job_id = request.args.get("job_id", "").strip()
        if not job_id:
            return jsonify({"ok": False, "error": "job_id is required"}), 400
        doc = runs_col.find_one({"job_id": job_id})
        if doc is None:
            return jsonify({"ok": False, "error": "Job not found"}), 404
        return jsonify({
            "ok": True,
            "job_id": doc["job_id"],
            "status": doc["status"],
            "snapshot_week": doc.get("snapshot_week"),
            "dry_run": doc.get("dry_run"),
            "summary": doc.get("summary"),
            "error": doc.get("error"),
        })

    app.register_blueprint(bp)
    return app


# ---------------------------------------------------------------------------
# Tests: async endpoint returns JSON immediately (test 7)
# ---------------------------------------------------------------------------

class AsyncRunEndpointTests(unittest.TestCase):
    """POST returns job_id + status=queued immediately; never blocks."""

    def test_returns_json_with_job_id_immediately(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        runs = _FakeRunsCollection()
        client = _make_async_test_app(mkt, _FakeSnapshotsCollection(), runs, run_bg_synchronously=False).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d["ok"])
        self.assertIn("job_id", d)
        self.assertIsNotNone(d["job_id"])
        self.assertEqual(d["status"], "queued")
        self.assertEqual(d["snapshot_week"], week)
        self.assertTrue(d["dry_run"])

    def test_returns_valid_json_on_validation_error(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        runs = _FakeRunsCollection()
        client = _make_async_test_app(mkt, _FakeSnapshotsCollection(), runs).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": "bad-format"}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)  # must not raise SyntaxError
        self.assertFalse(d["ok"])
        self.assertIn("error", d)

    def test_missing_week_returns_valid_json(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        runs = _FakeRunsCollection()
        client = _make_async_test_app(mkt, _FakeSnapshotsCollection(), runs).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertFalse(d["ok"])

    def test_no_marketing_data_returns_valid_json(self):
        runs = _FakeRunsCollection()
        client = _make_async_test_app(_mkt_col_with_week("2026-W25"), _FakeSnapshotsCollection(), runs).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": "2099-W01", "dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 422)
        d = json.loads(r.data)
        self.assertFalse(d["ok"])


# ---------------------------------------------------------------------------
# Tests: status endpoint returns job state (test 8)
# ---------------------------------------------------------------------------

class StatusEndpointTests(unittest.TestCase):
    def setUp(self):
        week = "2026-W25"
        self.mkt = _mkt_col_with_week(week)
        self.runs = _FakeRunsCollection()
        self.client = _make_async_test_app(
            self.mkt, _FakeSnapshotsCollection(), self.runs, run_bg_synchronously=False
        ).test_client()

    def _submit(self, week, dry_run):
        r = self.client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": dry_run}),
            content_type="application/json",
        )
        return json.loads(r.data)

    def _status(self, job_id):
        r = self.client.get(
            f"/api/admin/dashboard/backend-segment-engine/run-status?job_id={job_id}"
        )
        return r.status_code, json.loads(r.data)

    def test_missing_job_id_returns_400(self):
        r = self.client.get("/api/admin/dashboard/backend-segment-engine/run-status")
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertFalse(d["ok"])

    def test_unknown_job_id_returns_404(self):
        code, d = self._status("nonexistent-id")
        self.assertEqual(code, 404)
        self.assertFalse(d["ok"])

    def test_queued_job_status(self):
        d = self._submit("2026-W25", dry_run=True)
        job_id = d["job_id"]
        code, s = self._status(job_id)
        self.assertEqual(code, 200)
        self.assertTrue(s["ok"])
        self.assertEqual(s["status"], "queued")
        self.assertEqual(s["job_id"], job_id)

    def test_status_response_has_required_fields(self):
        d = self._submit("2026-W25", dry_run=True)
        _, s = self._status(d["job_id"])
        for field in ("ok", "job_id", "status", "snapshot_week", "dry_run", "summary", "error"):
            self.assertIn(field, s, f"Missing field: {field}")


# ---------------------------------------------------------------------------
# Tests: success job stores summary (test 9)
# ---------------------------------------------------------------------------

class SuccessJobTests(unittest.TestCase):
    def _full_run(self, week, dry_run):
        mkt = _mkt_col_with_week(week)
        snaps = _FakeSnapshotsCollection()
        runs = _FakeRunsCollection()
        client = _make_async_test_app(mkt, snaps, runs, run_bg_synchronously=True).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": dry_run}),
            content_type="application/json",
        )
        job_id = json.loads(r.data)["job_id"]
        s_r = client.get(f"/api/admin/dashboard/backend-segment-engine/run-status?job_id={job_id}")
        return json.loads(s_r.data), snaps

    def test_dry_run_success_stores_summary(self):
        d, snaps = self._full_run("2026-W25", dry_run=True)
        self.assertEqual(d["status"], "success")
        self.assertIsNone(d.get("error"))
        summary = d.get("summary") or {}
        self.assertGreater(summary.get("users_evaluated", 0), 0)
        self.assertEqual(len(snaps.docs), 0, "dry_run must not write snapshots")

    def test_commit_success_stores_snapshots_written(self):
        d, snaps = self._full_run("2026-W25", dry_run=False)
        self.assertEqual(d["status"], "success")
        summary = d.get("summary") or {}
        self.assertGreater(summary.get("snapshots_written", 0), 0)
        self.assertGreater(len(snaps.docs), 0)

    def test_success_summary_has_segment_distribution(self):
        d, _ = self._full_run("2026-W25", dry_run=True)
        summary = d.get("summary") or {}
        self.assertIn("segment_distribution", summary)
        segs = summary["segment_distribution"]
        self.assertEqual(sum(segs.values()), summary["users_evaluated"])


# ---------------------------------------------------------------------------
# Tests: failed job returns JSON error (test 10)
# ---------------------------------------------------------------------------

class _FailingFindMarketingCol:
    """count_documents succeeds (returns non-zero); find() always raises."""
    def __init__(self, week):
        self._week = week

    def count_documents(self, filt=None):
        wk = (filt or {}).get("snapshot_week")
        return 2 if wk == self._week else 0

    def find(self, filt=None):
        raise RuntimeError("Simulated engine failure")


class FailedJobTests(unittest.TestCase):
    def test_failed_job_has_error_and_status_failed(self):
        week = "2026-W25"
        failing_mkt = _FailingFindMarketingCol(week)
        runs = _FakeRunsCollection()
        client = _make_async_test_app(
            failing_mkt, _FakeSnapshotsCollection(), runs, run_bg_synchronously=True
        ).test_client()

        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 200)
        job_id = json.loads(r.data)["job_id"]

        s_r = client.get(f"/api/admin/dashboard/backend-segment-engine/run-status?job_id={job_id}")
        d = json.loads(s_r.data)
        self.assertEqual(d["status"], "failed")
        self.assertIsNotNone(d.get("error"))

    def test_failed_job_post_itself_returns_valid_json(self):
        week = "2026-W25"
        failing_mkt = _FailingFindMarketingCol(week)
        runs = _FakeRunsCollection()
        client = _make_async_test_app(
            failing_mkt, _FakeSnapshotsCollection(), runs, run_bg_synchronously=True
        ).test_client()
        r = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)  # must not raise
        self.assertTrue(d["ok"])
        self.assertIn("job_id", d)


# ---------------------------------------------------------------------------
# Tests: duplicate running job rejected (test 11)
# ---------------------------------------------------------------------------

class DuplicateJobTests(unittest.TestCase):
    def setUp(self):
        week = "2026-W25"
        self.mkt = _mkt_col_with_week(week)
        self.runs = _FakeRunsCollection()
        self.client = _make_async_test_app(
            self.mkt, _FakeSnapshotsCollection(), self.runs, run_bg_synchronously=False
        ).test_client()

    def _post(self, week, dry_run):
        r = self.client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": dry_run}),
            content_type="application/json",
        )
        return r.status_code, json.loads(r.data)

    def test_duplicate_queued_job_rejected_409(self):
        code1, d1 = self._post("2026-W25", dry_run=True)
        self.assertEqual(code1, 200)
        self.assertTrue(d1["ok"])
        first_job_id = d1["job_id"]

        code2, d2 = self._post("2026-W25", dry_run=True)
        self.assertEqual(code2, 409)
        self.assertFalse(d2["ok"])
        self.assertEqual(d2["job_id"], first_job_id)

    def test_duplicate_commit_job_rejected(self):
        self._post("2026-W25", dry_run=False)
        code2, d2 = self._post("2026-W25", dry_run=False)
        self.assertEqual(code2, 409)
        self.assertFalse(d2["ok"])

    def test_different_dry_run_flag_allowed(self):
        code1, _ = self._post("2026-W25", dry_run=True)
        self.assertEqual(code1, 200)
        code2, d2 = self._post("2026-W25", dry_run=False)
        self.assertEqual(code2, 200)
        self.assertTrue(d2["ok"])

    def test_different_week_allowed(self):
        code1, _ = self._post("2026-W25", dry_run=True)
        self.assertEqual(code1, 200)
        code2, d2 = self._post("2026-W24", dry_run=True)
        # 2026-W24 has no marketing data in this client
        self.assertEqual(code2, 422)  # rejected for missing data, not for duplicate

    def test_completed_job_allows_rerun(self):
        week = "2026-W25"
        mkt = _mkt_col_with_week(week)
        snaps = _FakeSnapshotsCollection()
        runs = _FakeRunsCollection()
        # First run completes synchronously
        client = _make_async_test_app(mkt, snaps, runs, run_bg_synchronously=True).test_client()
        client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        # Second run should be accepted (first job is success, not queued/running)
        r2 = client.post(
            "/api/admin/dashboard/backend-segment-engine/run",
            data=json.dumps({"snapshot_week": week, "dry_run": True}),
            content_type="application/json",
        )
        self.assertEqual(r2.status_code, 200)
        d2 = json.loads(r2.data)
        self.assertTrue(d2["ok"])


if __name__ == "__main__":
    unittest.main()
