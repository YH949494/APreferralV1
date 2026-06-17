"""
Phase 5: Backend vs UIM comparison panel — unit tests.

No MongoDB required: uses in-memory list-backed fake collections.

Run with:  python3 test_uim_comparison.py
"""
import io
import csv
import sys

# ---------------------------------------------------------------------------
# Inline the function under test so the file is self-contained.
# ---------------------------------------------------------------------------
from datetime import datetime, timezone

# Add parent dir to path so we can import dashboard_panels without main.py.
sys.path.insert(0, ".")

# Minimal stubs so dashboard_panels imports cleanly without a live DB.
import types

_fake_config = types.ModuleType("config")
_fake_config.normalize_for_bot_segment = lambda x: (x or "").lower().strip()
sys.modules.setdefault("config", _fake_config)

_fake_db = types.ModuleType("database")
sys.modules.setdefault("database", _fake_db)

_fake_xp = types.ModuleType("xp")
_fake_xp.grant_xp = lambda *a, **kw: None
sys.modules.setdefault("xp", _fake_xp)

_fake_tu = types.ModuleType("time_utils")
_fake_tu.as_aware_utc = lambda x: x
sys.modules.setdefault("time_utils", _fake_tu)

from dashboard_panels import build_uim_comparison_panel  # noqa: E402

# ---------------------------------------------------------------------------
# Fake collection
# ---------------------------------------------------------------------------

class _FakeCol:
    """Minimal MongoDB collection stub backed by a Python list."""

    def __init__(self, docs):
        self._docs = list(docs)

    def find(self, query=None, projection=None):
        query = query or {}
        results = []
        for doc in self._docs:
            if all(doc.get(k) == v for k, v in query.items()):
                if projection:
                    out = {k: doc.get(k) for k in projection if k != "_id"}
                    results.append(out)
                else:
                    results.append(dict(doc))
        return results

    def count_documents(self, query=None):
        return len(self.find(query))


def _make_doc(
    account, backend_segment, uim_segment=None, match=None,
    after_bet=None, withdraw=None, claim=0, ref=0, checkin=0,
    player_age_type="old_player", risk="normal", confidence="high",
    reason="test",
):
    doc = {
        "account": account,
        "backend_segment": backend_segment,
        "player_age_type": player_age_type,
        "claim_risk_level": risk,
        "confidence": confidence,
        "segment_reason": reason,
        "snapshot_week": "2026-W25",
        "metrics_snapshot": {
            "after_total_bet_amount": after_bet,
            "withdraw_amount": withdraw,
            "claim_count": claim,
            "referral_count": ref,
            "checkin_count": checkin,
        },
    }
    if uim_segment is not None:
        doc["uim_comparison"] = {
            "uim_segment": uim_segment,
            "backend_segment": backend_segment,
            "match": bool(match),
        }
    return doc


WEEK = "2026-W25"

# ---------------------------------------------------------------------------
# Test runner helpers
# ---------------------------------------------------------------------------

passed = failed = 0


def test(name, fn):
    global passed, failed
    try:
        fn()
        print("  PASS", name)
        passed += 1
    except AssertionError as e:
        print("  FAIL", name, "—", e)
        failed += 1
    except Exception as e:
        print("  ERROR", name, "—", type(e).__name__, e)
        failed += 1


# ---------------------------------------------------------------------------
# 1. Match-rate calculation
# ---------------------------------------------------------------------------

def test_match_rate():
    docs = [
        _make_doc("u1", "high_value",  "high_value",  match=True),
        _make_doc("u2", "high_value",  "high_value",  match=True),
        _make_doc("u3", "low_value",   "high_value",  match=False),
        _make_doc("u4", "ghost",       "ghost",        match=True),
        _make_doc("u5", "voucher_hunter", "ghost",     match=False),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK)
    s = r["summary"]
    assert s["compared_users"] == 5
    assert s["matched_users"] == 3
    assert s["mismatched_users"] == 2
    assert s["match_rate"] == 60.0
    assert s["mismatch_rate"] == 40.0


# ---------------------------------------------------------------------------
# 2. Mismatch matrix
# ---------------------------------------------------------------------------

def test_mismatch_matrix():
    docs = [
        _make_doc("u1", "high_value",  "high_value",  match=True),
        _make_doc("u2", "high_value",  "low_value",   match=False),
        _make_doc("u3", "high_value",  "low_value",   match=False),
        _make_doc("u4", "ghost",       "ghost",        match=True),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK)
    m = r["mismatch_matrix"]
    assert "high_value" in m["backend_segments"]
    assert "ghost" in m["backend_segments"]
    assert "high_value" in m["uim_segments"]
    assert "low_value" in m["uim_segments"]

    rows_by_seg = {row["backend_segment"]: row["by_uim_segment"] for row in m["rows"]}
    assert rows_by_seg["high_value"]["high_value"] == 1
    assert rows_by_seg["high_value"]["low_value"] == 2
    assert rows_by_seg["ghost"]["ghost"] == 1


# ---------------------------------------------------------------------------
# 3. Filter: backend_segment
# ---------------------------------------------------------------------------

def test_filter_backend_segment():
    docs = [
        _make_doc("u1", "high_value",   "high_value",  match=True),
        _make_doc("u2", "low_value",    "low_value",   match=True),
        _make_doc("u3", "ghost",        "ghost",        match=True),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(
        snapshots_col=col, snapshot_week=WEEK, filter_backend_segment="ghost"
    )
    assert r["total_details"] == 1
    assert r["details"][0]["account"] == "u3"
    # summary still reflects full dataset
    assert r["summary"]["compared_users"] == 3


# ---------------------------------------------------------------------------
# 4. Filter: uim_segment
# ---------------------------------------------------------------------------

def test_filter_uim_segment():
    docs = [
        _make_doc("u1", "high_value",   "high_value",  match=True),
        _make_doc("u2", "high_value",   "ghost",        match=False),
        _make_doc("u3", "ghost",        "ghost",        match=True),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(
        snapshots_col=col, snapshot_week=WEEK, filter_uim_segment="ghost"
    )
    assert r["total_details"] == 2
    accounts = {d["account"] for d in r["details"]}
    assert accounts == {"u2", "u3"}


# ---------------------------------------------------------------------------
# 5. Filter: match / mismatch only
# ---------------------------------------------------------------------------

def test_filter_match_true():
    docs = [
        _make_doc("u1", "high_value",  "high_value",  match=True),
        _make_doc("u2", "low_value",   "high_value",  match=False),
        _make_doc("u3", "ghost",       "ghost",        match=True),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(
        snapshots_col=col, snapshot_week=WEEK, filter_match=True
    )
    assert r["total_details"] == 2
    assert all(d["match"] is True for d in r["details"])


def test_filter_mismatch_only():
    docs = [
        _make_doc("u1", "high_value",  "high_value",  match=True),
        _make_doc("u2", "low_value",   "high_value",  match=False),
        _make_doc("u3", "ghost",       "high_value",  match=False),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(
        snapshots_col=col, snapshot_week=WEEK, filter_match=False
    )
    assert r["total_details"] == 2
    assert all(d["match"] is False for d in r["details"])


# ---------------------------------------------------------------------------
# 6. Filter: claim_risk_level
# ---------------------------------------------------------------------------

def test_filter_claim_risk_level():
    docs = [
        _make_doc("u1", "high_value",  "high_value",  match=True,  risk="normal"),
        _make_doc("u2", "high_value",  "low_value",   match=False, risk="high_risk_review"),
        _make_doc("u3", "ghost",       "ghost",        match=True,  risk="high_risk_review"),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(
        snapshots_col=col, snapshot_week=WEEK, filter_claim_risk_level="high_risk_review"
    )
    assert r["total_details"] == 2
    assert all(d["claim_risk_level"] == "high_risk_review" for d in r["details"])


# ---------------------------------------------------------------------------
# 7. Export CSV format
# ---------------------------------------------------------------------------

def test_export_csv_fields():
    docs = [
        _make_doc("alice", "high_value", "low_value", match=False,
                  after_bet=1000.0, withdraw=200.0, claim=3, ref=1, checkin=5,
                  player_age_type="new_player", risk="normal"),
        _make_doc("bob",   "ghost",      "ghost",     match=True),
    ]
    col = _FakeCol(docs)
    result = build_uim_comparison_panel(
        snapshots_col=col, snapshot_week=WEEK, per_page=500
    )

    _FIELDS = [
        "account", "backend_segment", "uim_segment", "match",
        "confidence", "reason",
        "after_total_bet_amount", "withdraw_amount",
        "claim_count", "referral_count", "checkin_count",
        "player_age_type", "claim_risk_level",
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in result["details"]:
        writer.writerow({k: ("" if row.get(k) is None else row[k]) for k in _FIELDS})

    out.seek(0)
    reader = list(csv.DictReader(out))
    assert len(reader) == 2
    assert set(reader[0].keys()) == set(_FIELDS)

    alice_row = next(r for r in reader if r["account"] == "alice")
    assert alice_row["backend_segment"] == "high_value"
    assert alice_row["uim_segment"] == "low_value"
    assert alice_row["match"] == "False"
    assert alice_row["after_total_bet_amount"] == "1000.0"
    assert alice_row["player_age_type"] == "new_player"


# ---------------------------------------------------------------------------
# 8. Missing UIM segment handling
# ---------------------------------------------------------------------------

def test_missing_uim_segment():
    """Docs without uim_comparison are included in detail rows (uim_segment=None)
    but excluded from match/mismatch counts and the cross-tab matrix."""
    docs = [
        _make_doc("u1", "high_value",  "high_value",  match=True),
        _make_doc("u2", "ghost"),            # no UIM comparison
        _make_doc("u3", "unclassified"),     # no UIM comparison
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK)

    # Only 1 doc was compared to UIM
    assert r["summary"]["compared_users"] == 1
    assert r["summary"]["matched_users"] == 1
    assert r["summary"]["match_rate"] == 100.0

    # All 3 docs appear in detail rows (no filter applied)
    assert r["total_details"] == 3

    # Docs without UIM comparison have uim_segment=None and match=None
    no_uim = [d for d in r["details"] if d["uim_segment"] is None]
    assert len(no_uim) == 2
    assert all(d["match"] is None for d in no_uim)

    # Matrix only contains the compared doc
    assert r["mismatch_matrix"]["backend_segments"] == ["high_value"]


# ---------------------------------------------------------------------------
# 9. total_backend_users vs total_uim_users
# ---------------------------------------------------------------------------

def test_totals():
    docs = [
        _make_doc("u1", "high_value", "high_value", match=True),
        _make_doc("u2", "ghost"),   # no UIM
        _make_doc("u3", "ghost"),   # no UIM
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK)
    assert r["summary"]["total_backend_users"] == 3
    # Without segment_snapshots_col, total_uim_users = count with uim_comparison
    assert r["summary"]["total_uim_users"] == 1


# ---------------------------------------------------------------------------
# 10. Rule audit averages
# ---------------------------------------------------------------------------

def test_rule_audit():
    docs = [
        _make_doc("u1", "high_value", after_bet=800.0, withdraw=100.0, claim=2, ref=1, checkin=3),
        _make_doc("u2", "high_value", after_bet=400.0, withdraw=50.0,  claim=0, ref=0, checkin=1),
        _make_doc("u3", "ghost",      after_bet=0.0,   withdraw=0.0,   claim=0, ref=0, checkin=0),
    ]
    col = _FakeCol(docs)
    r = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK)
    audit = r["rule_audit"]

    hv = audit["high_value"]
    assert hv["count"] == 2
    assert hv["avg_after_total_bet_amount"] == 600.0
    assert hv["avg_withdraw_amount"] == 75.0
    assert hv["avg_claim_count"] == 1.0
    assert hv["avg_referral_count"] == 0.5
    assert hv["avg_checkin_count"] == 2.0

    gh = audit["ghost"]
    assert gh["count"] == 1
    assert gh["avg_after_total_bet_amount"] == 0.0


# ---------------------------------------------------------------------------
# 11. Pagination
# ---------------------------------------------------------------------------

def test_pagination():
    docs = [_make_doc(f"u{i}", "ghost") for i in range(10)]
    col = _FakeCol(docs)

    r1 = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK, per_page=3, page=1)
    assert len(r1["details"]) == 3
    assert r1["has_more"] is True
    assert r1["total_details"] == 10

    r4 = build_uim_comparison_panel(snapshots_col=col, snapshot_week=WEEK, per_page=3, page=4)
    assert len(r4["details"]) == 1
    assert r4["has_more"] is False


# ---------------------------------------------------------------------------

print("\nPhase 5 UIM comparison panel tests\n")
for fn in [
    test_match_rate,
    test_mismatch_matrix,
    test_filter_backend_segment,
    test_filter_uim_segment,
    test_filter_match_true,
    test_filter_mismatch_only,
    test_filter_claim_risk_level,
    test_export_csv_fields,
    test_missing_uim_segment,
    test_totals,
    test_rule_audit,
    test_pagination,
]:
    test(fn.__name__, fn)

print(f"\n{passed} passed, {failed} failed\n")
if failed:
    sys.exit(1)
