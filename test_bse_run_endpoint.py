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


if __name__ == "__main__":
    unittest.main()
