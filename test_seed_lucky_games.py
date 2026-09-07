"""Tests for migrations/seed_lucky_games.py: the one-time Lucky Games
legacy-data migration (not a recurring startup seed).

Covers: fresh-DB migration, rerun idempotency, concurrent-call dedup via
the seed_id identity + upsert pattern, marker-gated skip behavior,
verification-gated marker writes, failure handling that never crashes the
caller, and that a completed migration never touches Admin-edited rows
(delete/rename/other-field-edit survive restart).
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

import migrations.seed_lucky_games as seed
from fake_mongo import FakeDb


@pytest.fixture
def fake_db():
    return FakeDb()


# ---------------------------------------------------------------------------
# 1. Empty DB -> exactly 56 games migrated
# ---------------------------------------------------------------------------


def test_fresh_db_migrates_exactly_56_games(fake_db):
    report = seed.run_lucky_games_migration(fake_db)
    assert report["status"] == "completed"
    assert report["upserted"] == 56
    assert fake_db["lucky_games"].count_documents({}) == 56
    assert seed.LEGACY_GAME_COUNT == 56


# ---------------------------------------------------------------------------
# 2. Migration rerun -> still exactly 56 seeded games
# ---------------------------------------------------------------------------


def test_rerun_keeps_exactly_56_games(fake_db):
    seed.run_lucky_games_migration(fake_db)
    # Clear the marker to force a second real migration pass (simulates a
    # second process racing before either has written the marker).
    fake_db["migrations"].delete_many({"_id": seed.MIGRATION_ID})
    report = seed.run_lucky_games_migration(fake_db)
    assert report["status"] == "completed"
    assert report["upserted"] == 0
    assert report["already_present"] == 56
    assert fake_db["lucky_games"].count_documents({}) == 56


def test_rerun_with_marker_present_skips_completely(fake_db):
    seed.run_lucky_games_migration(fake_db)
    with patch.object(fake_db["lucky_games"], "update_one") as mock_update:
        report = seed.run_lucky_games_migration(fake_db)
    assert report == {"status": "skipped", "reason": "already_completed"}
    mock_update.assert_not_called()
    assert fake_db["lucky_games"].count_documents({}) == 56


# ---------------------------------------------------------------------------
# 3. Concurrent migration calls cannot create duplicates
# ---------------------------------------------------------------------------


def test_concurrent_migration_calls_do_not_duplicate(fake_db):
    fake_db["migrations"].delete_many({})
    fake_db["lucky_games"].delete_many({})

    errors = []

    def worker():
        try:
            seed.run_lucky_games_migration(fake_db)
        except Exception as exc:  # pragma: no cover - defensive
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert fake_db["lucky_games"].count_documents({}) == 56
    seed_ids = [d["seed_id"] for d in fake_db["lucky_games"].find({})]
    assert len(seed_ids) == len(set(seed_ids))
    assert fake_db["migrations"].count_documents({"_id": seed.MIGRATION_ID}) == 1


# ---------------------------------------------------------------------------
# 4. Stable seed IDs are unique
# ---------------------------------------------------------------------------


def test_seed_ids_are_unique_and_stable():
    docs = seed.build_seed_docs()
    seed_ids = [d["seed_id"] for d in docs]
    assert len(seed_ids) == 56
    assert len(seed_ids) == len(set(seed_ids))
    assert seed_ids[0] == "legacy_daily_game_001"
    assert seed_ids[-1] == "legacy_daily_game_056"
    # Rebuilding must produce the exact same identities (immutable, not
    # derived from anything that could change between runs).
    assert [d["seed_id"] for d in seed.build_seed_docs()] == seed_ids


# ---------------------------------------------------------------------------
# 5. Existing Admin-edited seeded row is not overwritten
# ---------------------------------------------------------------------------


def test_admin_edited_row_not_overwritten_on_rerun(fake_db):
    seed.run_lucky_games_migration(fake_db)
    games_col = fake_db["lucky_games"]
    doc = games_col.find_one({"seed_id": "legacy_daily_game_001"})
    games_col.update_one(
        {"_id": doc["_id"]},
        {"$set": {"name": "Admin Renamed Game", "provider": "PG Soft", "is_published": False}},
    )

    fake_db["migrations"].delete_many({"_id": seed.MIGRATION_ID})
    seed.run_lucky_games_migration(fake_db)

    reloaded = games_col.find_one({"seed_id": "legacy_daily_game_001"})
    assert reloaded["name"] == "Admin Renamed Game"
    assert reloaded["provider"] == "PG Soft"
    assert reloaded["is_published"] is False
    assert games_col.count_documents({}) == 56


# ---------------------------------------------------------------------------
# 6-8. After migration completion, Admin delete/rename/edit survive restart
# ---------------------------------------------------------------------------


def test_admin_delete_survives_restart(fake_db):
    seed.run_lucky_games_migration(fake_db)
    games_col = fake_db["lucky_games"]
    doc = games_col.find_one({"seed_id": "legacy_daily_game_010"})
    games_col.delete_one({"_id": doc["_id"]})

    # Marker still present (real restart never re-runs migration).
    seed.run_lucky_games_migration(fake_db)

    assert games_col.find_one({"seed_id": "legacy_daily_game_010"}) is None
    assert games_col.count_documents({}) == 55


def test_admin_rename_survives_restart(fake_db):
    seed.run_lucky_games_migration(fake_db)
    games_col = fake_db["lucky_games"]
    doc = games_col.find_one({"seed_id": "legacy_daily_game_002"})
    games_col.update_one({"_id": doc["_id"]}, {"$set": {"name": "Renamed Forever"}})

    seed.run_lucky_games_migration(fake_db)

    reloaded = games_col.find_one({"seed_id": "legacy_daily_game_002"})
    assert reloaded["name"] == "Renamed Forever"


def test_admin_field_edits_survive_restart(fake_db):
    seed.run_lucky_games_migration(fake_db)
    games_col = fake_db["lucky_games"]
    doc = games_col.find_one({"seed_id": "legacy_daily_game_003"})
    edits = {
        "provider": "PG Soft",
        "label": "Featured",
        "volatility": "High",
        "max_win": "999999x",
        "image_url": "https://cdn.example.com/x.webp",
        "game_url": "https://games.example.com/x",
        "sort_order": 42,
        "is_published": False,
    }
    games_col.update_one({"_id": doc["_id"]}, {"$set": edits})

    seed.run_lucky_games_migration(fake_db)

    reloaded = games_col.find_one({"seed_id": "legacy_daily_game_003"})
    for k, v in edits.items():
        assert reloaded[k] == v


# ---------------------------------------------------------------------------
# 9-10. Completion marker only written after successful verification
# ---------------------------------------------------------------------------


def test_marker_written_only_after_successful_verification(fake_db):
    assert fake_db["migrations"].find_one({"_id": seed.MIGRATION_ID}) is None
    seed.run_lucky_games_migration(fake_db)
    marker = fake_db["migrations"].find_one({"_id": seed.MIGRATION_ID})
    assert marker is not None
    assert marker.get("completed_at") is not None
    assert marker.get("seeded_count") == 56


def test_failed_verification_does_not_write_marker(fake_db):
    # Simulate a verification mismatch (e.g. an insert silently failed) by
    # making count_documents under-report.
    games_col = fake_db["lucky_games"]
    real_count_documents = games_col.count_documents
    with patch.object(games_col, "count_documents", side_effect=lambda q=None: 3 if q else real_count_documents(q)):
        report = seed.run_lucky_games_migration(fake_db)
    assert report["status"] == "failed"
    assert report["reason"] == "verification_mismatch"
    assert fake_db["migrations"].find_one({"_id": seed.MIGRATION_ID}) is None


# ---------------------------------------------------------------------------
# 11. Migration failure does not stop app startup (never raises)
# ---------------------------------------------------------------------------


def test_migration_exception_does_not_raise(fake_db):
    with patch.object(fake_db["lucky_games"], "update_one", side_effect=RuntimeError("boom")):
        report = seed.run_lucky_games_migration(fake_db)
    assert report["status"] == "failed"
    assert report["reason"] == "exception"
    assert fake_db["migrations"].find_one({"_id": seed.MIGRATION_ID}) is None


def test_marker_check_exception_does_not_raise(fake_db):
    with patch.object(fake_db["migrations"], "find_one", side_effect=RuntimeError("db down")):
        report = seed.run_lucky_games_migration(fake_db)
    assert report["status"] == "failed"


# ---------------------------------------------------------------------------
# 12. Admin API lists the migrated games
# ---------------------------------------------------------------------------


def test_admin_api_lists_migrated_games(fake_db, monkeypatch):
    import database
    import lucky_games as lg

    monkeypatch.setattr(database, "db", fake_db)
    monkeypatch.setattr(lg, "database", database)
    seed.run_lucky_games_migration(fake_db)

    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_admin_bp)
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None)):
        resp = client.get("/api/admin/lucky-games")
    assert resp.status_code == 200
    body = resp.get_json()
    assert len(body["games"]) == 56
    names = {g["name"] for g in body["games"]}
    assert "Infinity Ocean" in names


# ---------------------------------------------------------------------------
# 13. Public GET /api/lucky-games returns published games in display order
# ---------------------------------------------------------------------------


def test_public_endpoint_returns_migrated_games_in_order(fake_db, monkeypatch):
    import database
    import lucky_games as lg

    monkeypatch.setattr(database, "db", fake_db)
    monkeypatch.setattr(lg, "database", database)
    seed.run_lucky_games_migration(fake_db)

    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_public_bp)
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    assert resp.status_code == 200
    body = resp.get_json()
    assert len(body["games"]) == 56

    expected_first_name = seed.build_seed_docs()[0]["name"]
    assert body["games"][0]["name"] == expected_first_name
    sort_orders = [d["sort_order"] for d in fake_db["lucky_games"].find({}, sort=[("sort_order", 1)])]
    assert sort_orders == sorted(sort_orders)
