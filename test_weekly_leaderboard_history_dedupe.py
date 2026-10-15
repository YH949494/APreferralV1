"""Coverage for the weekly_leaderboard_history duplicate-key incident:

- `scripts/dedupe_weekly_leaderboard_history.py` finds every duplicated
  week_start, keeps the verified pre-reset snapshot (never a zeroed latest
  one), backs up every document before deleting any, defaults to dry-run,
  aborts on ambiguity, and survives interrupted backup/cleanup.
- Canonical selection (`pick_canonical`) is deterministic regardless of
  document order.
- uniq_weekly_history_week_start can be created after cleanup, and rejects
  a future duplicate insert.
- The atomic-upsert writers (main._archive_week_upsert and the fixed
  database.save_weekly_snapshot) never create two documents for the same
  week_start when two processes race.
"""
import os
import random
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from pymongo.errors import DuplicateKeyError

from scripts.dedupe_weekly_leaderboard_history import (
    BACKUP_COLLECTION_NAME,
    AmbiguousCanonicalError,
    UnsafeCleanupError,
    INDEX_NAME,
    create_unique_index,
    dedupe,
    _verify_backups,
    pick_canonical,
)

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import database  # noqa: E402

if database._db is None:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
else:  # pragma: no cover
    import main  # noqa: E402


@pytest.fixture(autouse=True)
def _ensure_db_initialized():
    """Other test modules in the full suite (e.g. test_import_side_effects.py)
    reset database._db to None in setUp and never restore it, which breaks
    main.history_collection/database.db — both lazy proxies that re-resolve
    get_db() on every access — for any test module collected afterward.
    Reinitialize with a fresh mongomock backend before each test here so this
    file's results don't depend on what ran before it."""
    if database._db is None:
        with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
            database.init_db(os.environ["MONGO_URL"])
    yield


def _client_and_collections():
    client = mongomock.MongoClient()
    db = client["referral_bot"]
    return db["weekly_leaderboard_history"], db[BACKUP_COLLECTION_NAME]


def _board(values, field):
    return [{"user_id": 1000 + i, "username": f"u{i}", field: v} for i, v in enumerate(values)]


def _doc(week_start, *, source="live_counters", archived_at=None, xp=(10, 5), refs=(2, 1),
         week_end="2025-08-31", _id=None, ref_field="weekly_referrals"):
    """Snapshot in the persisted schema: weekly_xp / weekly_referrals entries."""
    d = {
        "week_start": week_start,
        "week_end": week_end,
        "checkin_leaderboard": _board(xp, "weekly_xp"),
        "referral_leaderboard": _board(refs, ref_field),
        "archived_at": archived_at or datetime.utcnow(),
        "source": source,
    }
    if _id is not None:
        d["_id"] = _id
    return d


T0 = datetime(2025, 9, 1, 0, 0, 0, 16000)  # naive, as pymongo returns it


def _incident(n_zero=20):
    """Pre-reset snapshot first, then a partial reset, then many zeroed copies;
    the LATEST snapshot is zeroed (production shape for week 2025-08-25)."""
    docs = [_doc("2025-08-25", xp=(160, 90, 40), refs=(5, 3, 1), archived_at=T0, _id="pre")]
    docs.append(_doc("2025-08-25", xp=(70, 0, 0), refs=(5, 3, 1), archived_at=T0 + timedelta(minutes=1), _id="partial"))
    for i in range(n_zero):
        docs.append(_doc("2025-08-25", xp=(0, 0, 0), refs=(0, 0, 0),
                         archived_at=T0 + timedelta(minutes=2 + i), _id=f"zero{i:02d}"))
    return docs


# ---------------------------------------------------------------------------
# Canonical selection
# ---------------------------------------------------------------------------

def test_pick_canonical_selects_pre_reset_not_latest_zeroed():
    docs = _incident()
    assert max(docs, key=lambda d: d["archived_at"])["_id"] == "zero19"  # latest is zeroed
    assert pick_canonical(docs)["_id"] == "pre"
    assert pick_canonical(list(reversed(docs)))["_id"] == "pre"


def test_pick_canonical_not_chosen_by_earliest_timestamp_alone():
    """Earliest snapshot is a degraded one; a later snapshot holds the real standings."""
    docs = [
        _doc("w", xp=(50, 0), refs=(1, 0), archived_at=T0, _id="early_degraded"),
        _doc("w", xp=(160, 90), refs=(5, 3), archived_at=T0 + timedelta(minutes=5), _id="later_real"),
        _doc("w", xp=(0, 0), refs=(0, 0), archived_at=T0 + timedelta(minutes=9), _id="zero"),
    ]
    assert pick_canonical(docs)["_id"] == "later_real"


def test_pick_canonical_ignores_zeroed_even_if_most_complete_and_latest():
    docs = [
        _doc("w", xp=(9, 4), refs=(1, 0), archived_at=T0, _id="real"),
        _doc("w", xp=(0,) * 100, refs=(0,) * 100, archived_at=T0 + timedelta(hours=2), _id="zero"),
    ]
    assert pick_canonical(docs)["_id"] == "real"


def test_pick_canonical_all_zeroed_is_ambiguous():
    docs = [_doc("w", xp=(0, 0), refs=(0,), _id="a"), _doc("w", xp=(0,), refs=(0,), _id="b")]
    with pytest.raises(AmbiguousCanonicalError):
        pick_canonical(docs)


def test_pick_canonical_conflicting_rankings_is_ambiguous():
    """Check-in intact in A, referral intact in B: no snapshot dominates."""
    docs = [
        _doc("w", xp=(160, 90), refs=(0, 0), archived_at=T0, _id="A"),
        _doc("w", xp=(0, 0), refs=(5, 3), archived_at=T0 + timedelta(minutes=1), _id="B"),
    ]
    with pytest.raises(AmbiguousCanonicalError, match="conflicting"):
        pick_canonical(docs)


def test_pick_canonical_conflicting_users_is_ambiguous():
    a = _doc("w", archived_at=T0, _id="A")
    b = _doc("w", archived_at=T0 + timedelta(minutes=1), _id="B")
    b["checkin_leaderboard"] = [{"user_id": 1000, "username": "u0", "weekly_xp": 3},
                                {"user_id": 7777, "username": "z", "weekly_xp": 99}]
    with pytest.raises(AmbiguousCanonicalError):
        pick_canonical([a, b])


def test_pick_canonical_unparseable_snapshot_is_ambiguous():
    bad = _doc("w", _id="bad")
    bad["checkin_leaderboard"] = [{"user_id": 1, "xp": 5}]  # wrong field name for persisted schema
    with pytest.raises(AmbiguousCanonicalError, match="unparseable"):
        pick_canonical([_doc("w", _id="ok"), bad])


def test_pick_canonical_identical_duplicates_resolve_deterministically():
    docs = [
        _doc("w", archived_at=T0 + timedelta(minutes=3), _id="x2"),
        _doc("w", archived_at=T0, _id="x1"),
        _doc("w", archived_at=T0 + timedelta(minutes=3), _id="x3"),
    ]
    results = set()
    for _ in range(20):
        random.shuffle(docs)
        results.add(pick_canonical(docs)["_id"])
    assert results == {"x1"}


def test_pick_canonical_tolerates_topn_truncation_and_new_week_noise():
    pre = _doc("w", xp=(160, 90, 40), refs=(5, 3), _id="pre", archived_at=T0)
    noisy = _doc("w", xp=(0, 0), refs=(0, 0), _id="noisy", archived_at=T0 + timedelta(minutes=1))
    noisy["checkin_leaderboard"] = [{"user_id": 9999, "username": "n", "weekly_xp": 10}] + noisy["checkin_leaderboard"]
    # 10 <= pre's lowest entry (40): consistent with a user cut off by top-N, not a conflict
    assert pick_canonical([pre, noisy])["_id"] == "pre"


def test_pinned_canonical_must_still_be_eligible():
    docs = _incident(3)
    assert pick_canonical(docs, pinned_id="pre")["_id"] == "pre"
    with pytest.raises(AmbiguousCanonicalError):
        pick_canonical(docs, pinned_id="zero00")
    with pytest.raises(AmbiguousCanonicalError):
        pick_canonical(docs, pinned_id="missing")


# ---------------------------------------------------------------------------
# Dedupe script: dry-run, cleanup, idempotent rerun, backup preservation
# ---------------------------------------------------------------------------

def test_dry_run_makes_no_database_changes():
    history, backup = _client_and_collections()
    history.insert_many(_incident(5) + [_doc("2025-09-01", _id="lonely")])

    result = dedupe(history_collection=history, backup_collection=backup)  # default = dry-run

    assert result["mode"] == "dry_run"
    assert result["duplicate_groups"] == 1
    assert history.count_documents({}) == 8
    assert backup.count_documents({}) == 0


def test_apply_retains_pre_reset_standings_and_backs_up_every_document():
    history, backup = _client_and_collections()
    docs = _incident(298)  # 300 documents, like production
    assert len(docs) == 300
    originals = {d["_id"]: dict(d) for d in docs}
    history.insert_many(docs)
    history.insert_one(_doc("2025-09-01", _id="lonely"))

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    assert result["mode"] == "apply"
    assert result["backed_up"] == 300 and result["deleted"] == 299
    remaining = list(history.find({"week_start": "2025-08-25"}))
    assert [d["_id"] for d in remaining] == ["pre"]
    assert remaining[0]["checkin_leaderboard"][0]["weekly_xp"] == 160  # not the zeroed latest
    assert history.count_documents({"week_start": "2025-09-01"}) == 1

    assert backup.count_documents({}) == 300
    for b in backup.find({}):
        meta = b.pop("_dedupe_meta")
        assert b == originals[b["_id"]]
        assert meta["canonical_id"] == "pre"
        assert meta["role"] == ("canonical" if b["_id"] == "pre" else "removed")


def test_rerun_after_apply_is_a_no_op():
    history, backup = _client_and_collections()
    history.insert_many(_incident(3))
    first = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert first["deleted"] == 4

    second = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert second["duplicate_groups"] == 0
    assert second["deleted"] == 0 and second["backed_up"] == 0
    assert backup.count_documents({}) == 5
    assert history.count_documents({}) == 1


def test_ambiguous_week_aborts_everything_without_any_write():
    history, backup = _client_and_collections()
    ok_week = [_doc("2025-09-08", xp=(9, 1), refs=(1,), _id="ok1", archived_at=T0),
               _doc("2025-09-08", xp=(0, 0), refs=(0,), _id="ok2", archived_at=T0 + timedelta(minutes=1))]
    conflicting = [_doc("2025-08-25", xp=(160, 90), refs=(0, 0), _id="A", archived_at=T0),
                   _doc("2025-08-25", xp=(0, 0), refs=(5, 3), _id="B", archived_at=T0 + timedelta(minutes=1))]
    history.insert_many(ok_week + conflicting)

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    assert result["mode"] == "aborted"
    assert result["ambiguous_weeks"] == ["2025-08-25"]
    assert history.count_documents({}) == 4  # even the unambiguous week is untouched
    assert backup.count_documents({}) == 0


def test_operator_pin_resolves_ambiguity():
    history, backup = _client_and_collections()
    history.insert_many([_doc("w", xp=(160, 90), refs=(0, 0), _id="A", archived_at=T0),
                         _doc("w", xp=(0, 0), refs=(5, 3), _id="B", archived_at=T0 + timedelta(minutes=1))])
    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False, pinned={"w": "A"})
    assert result["mode"] == "apply"
    assert [d["_id"] for d in history.find({})] == ["A"]
    assert backup.count_documents({}) == 2


def test_pin_bypasses_malformed_losers_and_report_survives_scalar_board():
    history, backup = _client_and_collections()
    bad = _doc("w", _id="bad", archived_at=T0 + timedelta(minutes=1))
    bad["checkin_leaderboard"] = 123  # scalar board
    history.insert_many([_doc("w", _id="good", archived_at=T0), bad])

    result = dedupe(history_collection=history, backup_collection=backup)  # must not crash
    assert result["mode"] == "aborted"

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False, pinned={"w": "good"})
    assert result["mode"] == "apply"
    assert [d["_id"] for d in history.find({})] == ["good"]
    assert backup.count_documents({}) == 2


LEGACY = "weekly_referral_count"


def _incident_legacy(n_zero=20, ref_field=LEGACY):
    docs = [_doc("2025-08-25", xp=(160, 90, 40), refs=(5, 3, 1), archived_at=T0, _id="pre", ref_field=ref_field)]
    docs.append(_doc("2025-08-25", xp=(70, 0, 0), refs=(5, 3, 1), archived_at=T0 + timedelta(minutes=1),
                     _id="partial", ref_field=ref_field))
    for i in range(n_zero):
        docs.append(_doc("2025-08-25", xp=(0, 0, 0), refs=(0, 0, 0), archived_at=T0 + timedelta(minutes=2 + i),
                         _id=f"zero{i:02d}", ref_field=ref_field))
    return docs


def test_legacy_weekly_referral_count_snapshots_are_parsed():
    assert pick_canonical(_incident_legacy())["_id"] == "pre"


def test_modern_weekly_referrals_snapshots_still_parsed():
    assert pick_canonical(_incident_legacy(ref_field="weekly_referrals"))["_id"] == "pre"


def test_mixed_legacy_and_modern_snapshots_compare_per_user():
    docs = _incident_legacy(5)
    docs[1] = _doc("2025-08-25", xp=(70, 0, 0), refs=(5, 3, 1), archived_at=T0 + timedelta(minutes=1),
                   _id="partial", ref_field="weekly_referrals")  # modern partial vs legacy pre
    docs[2] = _doc("2025-08-25", xp=(0, 0, 0), refs=(0, 0, 0), archived_at=T0 + timedelta(minutes=2),
                   _id="zero00", ref_field="weekly_referrals")
    assert pick_canonical(docs)["_id"] == "pre"
    # and the reverse: modern pre-reset beats legacy zeroed
    docs = [_doc("w", refs=(5, 3), _id="modern_pre", archived_at=T0),
            _doc("w", xp=(0, 0), refs=(0, 0), _id="legacy_zero", archived_at=T0 + timedelta(minutes=1), ref_field=LEGACY)]
    assert pick_canonical(docs)["_id"] == "modern_pre"


def test_dual_field_equal_values_accepted_conflicting_values_ambiguous():
    ok = _doc("w", _id="ok", archived_at=T0)
    for e in ok["referral_leaderboard"]:
        e[LEGACY] = e["weekly_referrals"]
    assert pick_canonical([ok, _doc("w", xp=(0, 0), refs=(0, 0), _id="z", archived_at=T0 + timedelta(minutes=1))])["_id"] == "ok"

    bad = _doc("w", _id="bad", archived_at=T0 + timedelta(minutes=1))
    for e in bad["referral_leaderboard"]:
        e[LEGACY] = e["weekly_referrals"] + 1
    with pytest.raises(AmbiguousCanonicalError, match="unparseable"):
        pick_canonical([ok, bad])


def test_missing_referral_value_is_not_treated_as_zero():
    bad = _doc("w", _id="bad", archived_at=T0 + timedelta(minutes=1))
    del bad["referral_leaderboard"][0]["weekly_referrals"]
    with pytest.raises(AmbiguousCanonicalError, match="unparseable"):
        pick_canonical([_doc("w", _id="ok", archived_at=T0), bad])
    bad["referral_leaderboard"][0]["weekly_referral_count"] = None
    with pytest.raises(AmbiguousCanonicalError):
        pick_canonical([_doc("w", _id="ok", archived_at=T0), bad])


def test_legacy_strict_numeric_and_sort_validation_preserved():
    ok = _doc("w", _id="ok", archived_at=T0, ref_field=LEGACY)
    junk = _doc("w", _id="junk", archived_at=T0 + timedelta(minutes=1), ref_field=LEGACY)
    junk["referral_leaderboard"][0][LEGACY] = "5"
    with pytest.raises(AmbiguousCanonicalError):
        pick_canonical([ok, junk])
    unsorted = _doc("w", refs=(1, 9), _id="unsorted", archived_at=T0, ref_field=LEGACY)
    with pytest.raises(AmbiguousCanonicalError, match="no valid"):
        pick_canonical([unsorted, _doc("w", xp=(0, 0), refs=(0, 0), _id="z", ref_field=LEGACY)])


def test_legacy_conflicting_rankings_abort_without_writes():
    history, backup = _client_and_collections()
    history.insert_many([
        _doc("w", xp=(160, 90), refs=(0, 0), _id="A", archived_at=T0, ref_field=LEGACY),
        _doc("w", xp=(0, 0), refs=(5, 3), _id="B", archived_at=T0 + timedelta(minutes=1), ref_field=LEGACY),
    ])
    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert result["mode"] == "aborted"
    assert history.count_documents({}) == 2 and backup.count_documents({}) == 0


def test_production_incident_shape_300_snapshots_50_referral_entries_each():
    """300 snapshots x 50 referral entries = 15,000 legacy entries; earliest 160 XP, latest 0 XP."""
    history, backup = _client_and_collections()
    docs = []
    for i in range(300):
        pre = i == 0
        xp = tuple([160 - k for k in range(50)]) if pre else tuple([0] * 50)
        refs = tuple([50 - k for k in range(50)]) if i < 3 else tuple([0] * 50)
        docs.append(_doc("2025-08-25", xp=xp, refs=refs, archived_at=T0 + timedelta(seconds=i * 27),
                         _id=f"s{i:03d}", ref_field=LEGACY))
    assert sum(len(d["referral_leaderboard"]) for d in docs) == 15000
    history.insert_many(docs)
    before = {d["_id"]: d for d in history.find({})}

    dry = dedupe(history_collection=history, backup_collection=backup)
    assert dry["mode"] == "dry_run" and backup.count_documents({}) == 0 and history.count_documents({}) == 300

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert result["mode"] == "apply" and result["backed_up"] == 300 and result["deleted"] == 299
    kept = history.find_one({"week_start": "2025-08-25"})
    assert kept["_id"] == "s000" and kept["checkin_leaderboard"][0]["weekly_xp"] == 160
    # stored documents are never rewritten (no field migration)
    assert kept == before["s000"] and "weekly_referrals" not in kept["referral_leaderboard"][0]
    assert backup.count_documents({}) == 300
    assert backup.find_one({"_id": "s000"})["_dedupe_meta"]["role"] == "canonical"


def test_earliest_with_160xp_is_not_trusted_if_it_does_not_dominate():
    docs = _incident_legacy(3)
    docs[0] = _doc("2025-08-25", xp=(160, 0, 0), refs=(1, 0, 0), archived_at=T0, _id="pre", ref_field=LEGACY)
    docs[1] = _doc("2025-08-25", xp=(100, 90, 40), refs=(5, 3, 1), archived_at=T0 + timedelta(minutes=1),
                   _id="partial", ref_field=LEGACY)
    with pytest.raises(AmbiguousCanonicalError, match="conflicting"):
        pick_canonical(docs)


def test_canonical_backup_is_read_back_before_any_delete():
    history, backup = _client_and_collections()
    history.insert_many(_incident_legacy(5))

    class _DropCanonical:
        """Backup store whose read-back silently loses the canonical's backup."""
        def __init__(self, coll):
            self._c = coll

        def __getattr__(self, n):
            return getattr(self._c, n)

        def find(self, flt=None, *a, **kw):
            return [d for d in self._c.find(flt, *a, **kw) if d["_id"] != "pre"]

    with pytest.raises(UnsafeCleanupError, match="backup missing for _id='pre'"):
        dedupe(history_collection=history, backup_collection=_DropCanonical(backup), dry_run=False)
    assert history.count_documents({}) == 7  # nothing deleted

    # tampered canonical backup (stale content) is also caught
    group = {"all_ids": ["pre"], "canonical_id": "pre", "remove_ids": [], "week_start": "2025-08-25"}
    backup.replace_one({"_id": "pre"}, {"_id": "pre", "week_start": "tampered"}, upsert=True)
    with pytest.raises(UnsafeCleanupError, match="does not match"):
        _verify_backups(history, backup, group)


def test_interrupted_cleanup_on_legacy_incident_resumes():
    history, backup = _client_and_collections()
    docs = _incident_legacy(30)
    history.insert_many(docs)
    crashing = _FailingAfter(history, "delete_many", 1)
    with pytest.raises(RuntimeError, match="simulated crash"):
        dedupe(history_collection=crashing, backup_collection=backup, dry_run=False, delete_batch_size=10)
    assert history.count_documents({"_id": "pre"}) == 1
    dedupe(history_collection=history, backup_collection=backup, dry_run=False, delete_batch_size=10)
    assert [d["_id"] for d in history.find({})] == ["pre"]
    assert backup.count_documents({}) == len(docs)


class _FailingAfter:
    """Proxy that lets `n` calls to `method` through, then raises."""

    def __init__(self, coll, method, n):
        self._coll, self._method, self._n, self.calls = coll, method, n, 0

    def __getattr__(self, name):
        attr = getattr(self._coll, name)
        if name != self._method:
            return attr

        def wrapper(*a, **kw):
            self.calls += 1
            if self.calls > self._n:
                raise RuntimeError("simulated crash")
            return attr(*a, **kw)
        return wrapper


def test_interrupted_cleanup_loses_nothing_and_rerun_completes():
    history, backup = _client_and_collections()
    docs = _incident(30)
    originals = {d["_id"]: dict(d) for d in docs}
    history.insert_many(docs)

    crashing = _FailingAfter(history, "delete_many", 1)  # first delete batch succeeds, second crashes
    with pytest.raises(RuntimeError, match="simulated crash"):
        dedupe(history_collection=crashing, backup_collection=backup, dry_run=False, delete_batch_size=10)

    # Partial delete: canonical still present, every original recoverable from live+backup.
    assert history.count_documents({"_id": "pre"}) == 1
    assert 1 < history.count_documents({}) < len(docs)
    assert backup.count_documents({}) == len(docs)
    for d in docs:
        assert history.find_one({"_id": d["_id"]}) is not None or backup.find_one({"_id": d["_id"]}) is not None

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False, delete_batch_size=10)
    assert result["mode"] == "apply"
    assert [d["_id"] for d in history.find({})] == ["pre"]
    assert backup.count_documents({}) == len(docs)  # idempotent: no duplicates, no growth
    for b in backup.find({}):
        b.pop("_dedupe_meta")
        assert b == originals[b["_id"]]


def test_interrupted_backup_deletes_nothing_and_rerun_completes():
    history, backup = _client_and_collections()
    docs = _incident(10)
    history.insert_many(docs)

    crashing = _FailingAfter(backup, "replace_one", 4)
    with pytest.raises(RuntimeError, match="simulated crash"):
        dedupe(history_collection=history, backup_collection=crashing, dry_run=False)
    assert history.count_documents({}) == len(docs)  # nothing deleted before backups finished
    assert backup.count_documents({}) == 4

    dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert [d["_id"] for d in history.find({})] == ["pre"]
    assert backup.count_documents({}) == len(docs)


def test_mismatched_existing_backup_aborts_before_delete():
    history, backup = _client_and_collections()
    history.insert_many(_incident(3))
    backup.insert_one({"_id": "zero00", "week_start": "tampered"})
    with pytest.raises(UnsafeCleanupError):
        dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert history.count_documents({}) == 5


def test_dedupe_handles_multiple_distinct_duplicate_weeks():
    history, backup = _client_and_collections()
    history.insert_many([
        _doc("2025-08-25", _id="a-keep", archived_at=T0),
        _doc("2025-08-25", xp=(0, 0), refs=(0, 0), _id="a-dupe", archived_at=T0 + timedelta(minutes=1)),
        _doc("2025-09-01", _id="b-keep", archived_at=T0),
        _doc("2025-09-01", xp=(0, 0), refs=(0, 0), _id="b-dupe", archived_at=T0 + timedelta(minutes=1)),
        _doc("2025-09-08", _id="c-solo"),
    ])
    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert result["duplicate_groups"] == 2 and result["deleted"] == 2
    assert {d["_id"] for d in history.find({})} == {"a-keep", "b-keep", "c-solo"}


def test_dedupe_also_covers_null_and_missing_week_start():
    history, backup = _client_and_collections()
    keep = _doc("x", _id="explicit-null", archived_at=T0)
    keep["week_start"] = None
    missing = _doc("x", xp=(0,), refs=(0,), _id="no-field", archived_at=T0 + timedelta(minutes=1))
    del missing["week_start"]
    history.insert_many([keep, missing, _doc("2025-08-25", _id="solo")])

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    assert result["duplicate_groups"] == 1 and result["deleted"] == 1
    assert history.count_documents({"_id": "explicit-null"}) == 1
    assert backup.count_documents({"_id": "no-field"}) == 1
    assert history.count_documents({"_id": "solo"}) == 1


# ---------------------------------------------------------------------------
# Unique index: succeeds after cleanup, rejects future duplicates
# ---------------------------------------------------------------------------

def test_unique_index_creation_fails_before_cleanup_and_succeeds_after():
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many(_incident(2))

    before = create_unique_index(history)
    assert before["status"] == "failed"

    dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    after = create_unique_index(history)
    assert after["status"] == "created_or_exists"
    assert INDEX_NAME in history.index_information()


def test_future_duplicate_insertion_rejected_after_index_created():
    history, backup = _client_and_collections()
    history.insert_one(_doc("2025-08-25", _id="a"))
    create_unique_index(history)

    with pytest.raises(DuplicateKeyError):
        history.insert_one({"week_start": "2025-08-25", "week_end": "x"})


# ---------------------------------------------------------------------------
# Concurrent writers must never create a second document for the same week
# ---------------------------------------------------------------------------

def test_archive_week_upsert_race_is_handled_gracefully():
    """Simulate two processes racing on the same week: the second hits the
    unique index and must resolve to 'already_exists', never raise or
    duplicate the archive."""
    call_count = {"n": 0}
    real_update_one = main.history_collection.update_one

    def racy_update_one(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise DuplicateKeyError("simulated concurrent winner")
        return real_update_one(*args, **kwargs)

    main.history_collection.delete_many({})
    with mock.patch.object(main.history_collection, "update_one", side_effect=racy_update_one):
        result = main._archive_week_upsert(
            main.datetime.now(timezone.utc).date(),
            main.datetime.now(timezone.utc).date(),
            [],
            [],
            source="test",
        )
    assert result["status"] == "already_exists"


def test_legacy_save_weekly_snapshot_upsert_never_duplicates_on_double_call():
    """database.save_weekly_snapshot() used to be a raw insert_one(); it is
    now an atomic upsert keyed by week_start, same guarantee as
    main._archive_week_upsert."""
    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    database.db["weekly_leaderboard_history"].delete_many({"week_start": week_start})
    database.users_collection.delete_many({"user_id": 999001})
    database.users_collection.insert_one(
        {"user_id": 999001, "username": "dedupe_test", "weekly_xp": 10, "weekly_referrals": 2}
    )
    try:
        with mock.patch.dict(os.environ, {"ENABLE_LEGACY_WEEKLY_SNAPSHOT": "1"}):
            database.save_weekly_snapshot()
            database.save_weekly_snapshot()  # simulate a retried/duplicate call

        count = database.db["weekly_leaderboard_history"].count_documents({"week_start": week_start})
        assert count == 1
    finally:
        database.db["weekly_leaderboard_history"].delete_many({"week_start": week_start})
        database.users_collection.delete_many({"user_id": 999001})


def test_legacy_save_weekly_snapshot_does_not_reset_counters_on_retry():
    """A retry that finds the week already archived (matched, not upserted)
    must not reset weekly_xp/weekly_referrals a second time — that would
    erase progress earned between the first successful run and the retry."""
    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    database.db["weekly_leaderboard_history"].delete_many({"week_start": week_start})
    database.users_collection.delete_many({"user_id": 999002})
    database.users_collection.insert_one(
        {"user_id": 999002, "username": "dedupe_test2", "weekly_xp": 10, "weekly_referrals": 2}
    )
    try:
        with mock.patch.dict(os.environ, {"ENABLE_LEGACY_WEEKLY_SNAPSHOT": "1"}):
            database.save_weekly_snapshot()

            # New activity accrues after the archive was created...
            database.users_collection.update_one(
                {"user_id": 999002}, {"$set": {"weekly_xp": 99, "weekly_referrals": 7}}
            )
            database.save_weekly_snapshot()  # ...a retry must not zero it out again.

        user = database.users_collection.find_one({"user_id": 999002})
        assert user["weekly_xp"] == 99
        assert user["weekly_referrals"] == 7
    finally:
        database.db["weekly_leaderboard_history"].delete_many({"week_start": week_start})
        database.users_collection.delete_many({"user_id": 999002})
