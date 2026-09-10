"""Coverage for the weekly_leaderboard_history duplicate-key incident:

- `scripts/dedupe_weekly_leaderboard_history.py` finds every duplicated
  week_start, backs up every non-canonical document before deleting it,
  is a no-op dry-run, and is idempotent on rerun.
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
    INDEX_NAME,
    create_unique_index,
    dedupe,
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


def _doc(week_start, *, source, archived_at, checkin=1, referral=1, week_end="w-end", _id=None):
    d = {
        "week_start": week_start,
        "week_end": week_end,
        "checkin_leaderboard": [{"user_id": i} for i in range(checkin)],
        "referral_leaderboard": [{"user_id": i} for i in range(referral)],
        "archived_at": archived_at,
        "source": source,
    }
    if _id is not None:
        d["_id"] = _id
    return d


# ---------------------------------------------------------------------------
# Canonical selection determinism
# ---------------------------------------------------------------------------

def test_pick_canonical_prefers_most_complete_valid_latest():
    now = datetime.now(timezone.utc)
    older = _doc("2025-08-25", source="live_counters", archived_at=now - timedelta(days=1), checkin=1, referral=1)
    incomplete = {"week_start": "2025-08-25", "source": "legacy", "_id": "incomplete"}  # no week_end/entries
    newest_complete = _doc("2025-08-25", source="live_counters", archived_at=now, checkin=2, referral=2)

    docs = [older, incomplete, newest_complete]
    for _id, d in zip(["a", "b", "c"], docs):
        d["_id"] = _id
    # incomplete overwritten _id above; keep it distinct
    incomplete["_id"] = "b"

    canonical = pick_canonical(docs)
    assert canonical["_id"] == "c"


def test_pick_canonical_is_order_independent_and_deterministic():
    now = datetime.now(timezone.utc)
    docs = [
        _doc("2025-08-25", source="s1", archived_at=now, _id="x1"),
        _doc("2025-08-25", source="s2", archived_at=now, _id="x2"),  # exact tie except _id
    ]
    results = set()
    for _ in range(20):
        shuffled = list(docs)
        random.shuffle(shuffled)
        results.add(pick_canonical(shuffled)["_id"])
    assert len(results) == 1  # always resolves to the same doc via the _id tiebreak


# ---------------------------------------------------------------------------
# Dedupe script: dry-run, cleanup, idempotent rerun, backup preservation
# ---------------------------------------------------------------------------

def test_dry_run_makes_no_database_changes():
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many([
        _doc("2025-08-25", source="live_counters", archived_at=now, _id="keep"),
        _doc("2025-08-25", source="legacy", archived_at=now - timedelta(days=1), _id="dupe"),
        _doc("2025-09-01", source="live_counters", archived_at=now, _id="lonely"),
    ])

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=True)

    assert result["mode"] == "dry_run"
    assert result["duplicate_groups"] == 1
    assert history.count_documents({}) == 3
    assert backup.count_documents({}) == 0


def test_apply_backs_up_and_removes_only_non_canonical_duplicates():
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many([
        _doc("2025-08-25", source="live_counters", archived_at=now, checkin=5, referral=5, _id="keep"),
        _doc("2025-08-25", source="legacy", archived_at=now - timedelta(days=1), checkin=1, referral=1, _id="dupe1"),
        _doc("2025-08-25", source="legacy", archived_at=now - timedelta(days=2), checkin=1, referral=1, _id="dupe2"),
        _doc("2025-09-01", source="live_counters", archived_at=now, _id="lonely"),
    ])

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    assert result["mode"] == "apply"
    assert result["duplicate_groups"] == 1
    assert result["deleted"] == 2
    assert result["backed_up"] == 2

    remaining = list(history.find({"week_start": "2025-08-25"}))
    assert len(remaining) == 1
    assert remaining[0]["_id"] == "keep"

    # lonely week untouched
    assert history.count_documents({"week_start": "2025-09-01"}) == 1

    backed_up_ids = {d["_id"] for d in backup.find({})}
    assert backed_up_ids == {"dupe1", "dupe2"}
    for d in backup.find({}):
        assert d["_dedupe_meta"]["canonical_id"] == "keep"
        assert d["_dedupe_meta"]["week_start"] == "2025-08-25"


def test_rerun_after_apply_is_a_no_op():
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many([
        _doc("2025-08-25", source="live_counters", archived_at=now, _id="keep"),
        _doc("2025-08-25", source="legacy", archived_at=now - timedelta(days=1), _id="dupe1"),
    ])

    first = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert first["deleted"] == 1

    second = dedupe(history_collection=history, backup_collection=backup, dry_run=False)
    assert second["duplicate_groups"] == 0
    assert second["deleted"] == 0
    assert second["backed_up"] == 0
    # Backup collection still holds the original record — rerun never re-deletes valid data.
    assert backup.count_documents({}) == 1
    assert history.count_documents({}) == 1


def test_dedupe_handles_multiple_distinct_duplicate_weeks():
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many([
        _doc("2025-08-25", source="live_counters", archived_at=now, _id="a-keep"),
        _doc("2025-08-25", source="legacy", archived_at=now - timedelta(days=1), _id="a-dupe"),
        _doc("2025-09-01", source="live_counters", archived_at=now, _id="b-keep"),
        _doc("2025-09-01", source="legacy", archived_at=now - timedelta(days=1), _id="b-dupe"),
        _doc("2025-09-08", source="live_counters", archived_at=now, _id="c-solo"),
    ])

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    assert result["duplicate_groups"] == 2
    assert result["deleted"] == 2
    assert history.count_documents({}) == 3
    assert {d["week_start"] for d in history.find({})} == {"2025-08-25", "2025-09-01", "2025-09-08"}


def test_dedupe_also_covers_null_and_missing_week_start():
    """A unique index treats a missing field as null, so two documents that
    both lack week_start collide on it exactly like two matching strings —
    they must be found and deduped too, not silently skipped by a
    string-only filter."""
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many([
        {"_id": "no-field-1", "week_end": "w", "checkin_leaderboard": [], "referral_leaderboard": [{"x": 1}], "archived_at": now - timedelta(days=1), "source": "legacy"},
        {"_id": "explicit-null", "week_start": None, "week_end": "w", "checkin_leaderboard": [{"x": 1}, {"x": 2}], "referral_leaderboard": [], "archived_at": now, "source": "legacy"},
        _doc("2025-08-25", source="live_counters", archived_at=now, _id="solo"),
    ])

    result = dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    assert result["duplicate_groups"] == 1
    assert result["deleted"] == 1
    # The most complete of the two null/missing week_start docs survives.
    assert history.count_documents({"_id": "explicit-null"}) == 1
    assert history.count_documents({"_id": "no-field-1"}) == 0
    assert backup.count_documents({"_id": "no-field-1"}) == 1
    # Unrelated real week_start untouched.
    assert history.count_documents({"_id": "solo"}) == 1


# ---------------------------------------------------------------------------
# Unique index: succeeds after cleanup, rejects future duplicates
# ---------------------------------------------------------------------------

def test_unique_index_creation_fails_before_cleanup_and_succeeds_after():
    history, backup = _client_and_collections()
    now = datetime.now(timezone.utc)
    history.insert_many([
        _doc("2025-08-25", source="live_counters", archived_at=now, _id="keep"),
        _doc("2025-08-25", source="legacy", archived_at=now - timedelta(days=1), _id="dupe"),
    ])

    before = create_unique_index(history)
    assert before["status"] == "failed"

    dedupe(history_collection=history, backup_collection=backup, dry_run=False)

    after = create_unique_index(history)
    assert after["status"] == "created_or_exists"
    assert INDEX_NAME in history.index_information()


def test_future_duplicate_insertion_rejected_after_index_created():
    history, backup = _client_and_collections()
    history.insert_one(_doc("2025-08-25", source="live_counters", archived_at=datetime.now(timezone.utc), _id="a"))
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
