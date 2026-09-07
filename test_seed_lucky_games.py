"""Tests for migrations/seed_lucky_games.py — the idempotent backfill of the
legacy DAILY_GAME_SLOTS pool into the admin-managed lucky_games collection,
and the automatic run_on_startup() hook called from main.py on every boot.
"""

from unittest.mock import patch

import pytest

import database
import lucky_games as lg
from fake_mongo import FakeDb
from migrations.seed_lucky_games import (
    SEED_SOURCE,
    STARTUP_MIGRATIONS_COLLECTION,
    STARTUP_MIGRATION_ID,
    _DAILY_GAME_SLOTS,
    run_on_startup,
    seed_collection,
)


@pytest.fixture
def fake_db(monkeypatch):
    # startup_migrations relies on a real unique index on _id (MongoDB's
    # implicit default) to make run_on_startup's claim atomic — FakeDb only
    # enforces uniqueness for keys explicitly registered here.
    fdb = FakeDb(unique_keys_by_collection={STARTUP_MIGRATIONS_COLLECTION: ["_id"]})
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(lg, "database", database)
    # run_on_startup() calls the module-level get_db() (the same shared
    # connection main.py already initialized) rather than database.db —
    # point it at the same fake so both paths see one collection.
    monkeypatch.setattr("migrations.seed_lucky_games.get_db", lambda: fdb)
    return fdb


def test_migration_inserts_every_existing_game(fake_db):
    col = fake_db["lucky_games"]
    report = seed_collection(col, commit=True)
    assert report["inserted_count"] == len(_DAILY_GAME_SLOTS)
    assert col.count_documents({}) == len(_DAILY_GAME_SLOTS)


def test_migration_is_idempotent_no_duplicates_on_second_run(fake_db):
    col = fake_db["lucky_games"]
    seed_collection(col, commit=True)
    second_report = seed_collection(col, commit=True)
    assert second_report["inserted_count"] == 0
    assert second_report["already_present"] == len(_DAILY_GAME_SLOTS)
    assert col.count_documents({}) == len(_DAILY_GAME_SLOTS)


def test_migration_preserves_field_values(fake_db):
    col = fake_db["lucky_games"]
    seed_collection(col, commit=True)
    doc = col.find_one({"name": "Zeustrike Xmas"})
    assert doc is not None
    assert doc["volatility"] == "High"
    assert doc["max_win"] == "30000x"
    assert doc["is_published"] is True
    assert doc["seed_source"] == SEED_SOURCE
    # Source order preserved via sort_order (index * 10) for the fallback
    # ordering used before any admin reorders games.
    names_in_source_order = [slot["name"] for slot in _DAILY_GAME_SLOTS]
    assert doc["sort_order"] == names_in_source_order.index("Zeustrike Xmas") * 10


def test_migration_maps_med_tag_to_medium_volatility(fake_db):
    col = fake_db["lucky_games"]
    seed_collection(col, commit=True)
    doc = col.find_one({"name": "Dragon Chi's Quest 2"})
    assert doc["volatility"] == "Medium"


def test_migration_does_not_overwrite_admin_edited_game(fake_db):
    # A game already present (e.g. edited by an admin, or from a previous
    # run) must never be touched by a later migration pass.
    col = fake_db["lucky_games"]
    col.insert_one({
        "name": "Zeustrike Xmas",
        "label": "Custom Label",
        "volatility": "Low",
        "max_win": "999x",
        "image_url": "https://cdn.example.com/z.webp",
        "game_url": "https://games.example.com/z",
        "provider": "Custom Provider",
        "sort_order": 5,
        "is_published": False,
    })
    seed_collection(col, commit=True)
    doc = col.find_one({"name": "Zeustrike Xmas"})
    assert doc["label"] == "Custom Label"
    assert doc["is_published"] is False
    assert doc["sort_order"] == 5
    # Every other source row still got inserted.
    assert col.count_documents({}) == len(_DAILY_GAME_SLOTS)


def test_migration_dry_run_does_not_write(fake_db):
    col = fake_db["lucky_games"]
    report = seed_collection(col, commit=False)
    assert report["committed"] is False
    assert report["to_insert"] == len(_DAILY_GAME_SLOTS)
    assert col.count_documents({}) == 0


def test_run_on_startup_seeds_and_admin_lists_migrated_games(fake_db):
    run_on_startup()
    docs = list(fake_db["lucky_games"].find({}))
    assert len(docs) == len(_DAILY_GAME_SLOTS)

    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_admin_bp)
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None)):
        resp = client.get("/api/admin/lucky-games")
    assert resp.status_code == 200
    names = {g["name"] for g in resp.get_json()["games"]}
    assert names == {slot["name"] for slot in _DAILY_GAME_SLOTS}


def test_run_on_startup_is_idempotent_across_repeated_boots(fake_db):
    first = run_on_startup()
    second = run_on_startup()
    third = run_on_startup()
    assert first["inserted_count"] == len(_DAILY_GAME_SLOTS)
    # Second/third boot find the migration ledger already claimed and never
    # even look at lucky_games again.
    assert second is None
    assert third is None
    assert fake_db["lucky_games"].count_documents({}) == len(_DAILY_GAME_SLOTS)


def test_run_on_startup_claims_ledger_row_exactly_once(fake_db):
    run_on_startup()
    ledger_docs = list(fake_db[STARTUP_MIGRATIONS_COLLECTION].find({"_id": STARTUP_MIGRATION_ID}))
    assert len(ledger_docs) == 1


def test_run_on_startup_concurrent_boots_never_duplicate_games(fake_db):
    # Simulates the real Fly deployment: 2 gunicorn "web" workers + a
    # separate "worker" process all call run_on_startup() within moments of
    # each other against an initially empty collection (fly.toml). Only one
    # process's atomic ledger claim can win, so only one seed pass ever
    # touches lucky_games — no game should be inserted more than once.
    reports = [run_on_startup() for _ in range(3)]
    successful = [r for r in reports if r is not None]
    assert len(successful) == 1
    names = [d["name"] for d in fake_db["lucky_games"].find({})]
    assert len(names) == len(set(names)) == len(_DAILY_GAME_SLOTS)


def test_run_on_startup_never_raises_on_db_error(fake_db, monkeypatch):
    def _boom():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("migrations.seed_lucky_games.get_db", _boom)
    result = run_on_startup()
    assert result is None


def test_deleted_migrated_game_is_not_resurrected_by_later_boot(fake_db):
    run_on_startup()
    doc = fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas"})
    fake_db["lucky_games"].delete_one({"_id": doc["_id"]})

    # A later restart (the migration ledger is already claimed) must not
    # bring the deleted game back.
    run_on_startup()
    run_on_startup()

    assert fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas"}) is None
    assert fake_db["lucky_games"].count_documents({}) == len(_DAILY_GAME_SLOTS) - 1


def test_renamed_migrated_game_is_not_duplicated_by_later_boot(fake_db):
    run_on_startup()
    doc = fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas"})
    fake_db["lucky_games"].update_one({"_id": doc["_id"]}, {"$set": {"name": "Zeustrike Xmas Reloaded"}})

    # A later restart must not re-add "Zeustrike Xmas" under its old name
    # alongside the admin's renamed copy.
    run_on_startup()

    assert fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas"}) is None
    assert fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas Reloaded"}) is not None
    assert fake_db["lucky_games"].count_documents({}) == len(_DAILY_GAME_SLOTS)


def test_public_endpoint_serves_migrated_games_in_source_order(fake_db):
    run_on_startup()

    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_public_bp)
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    names = [g["name"] for g in resp.get_json()["games"]]
    assert names == [slot["name"] for slot in _DAILY_GAME_SLOTS]


def test_editing_migrated_game_via_admin_updates_public_result(fake_db):
    run_on_startup()

    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_admin_bp)
    app.register_blueprint(lg.lucky_games_public_bp)
    client = app.test_client()

    doc = fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas"})
    game_id = str(doc["_id"])
    with patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None)):
        resp = client.patch(f"/api/admin/lucky-games/{game_id}", json={"max_win": "999999x"})
    assert resp.status_code == 200

    public_resp = client.get("/api/lucky-games")
    game = next(g for g in public_resp.get_json()["games"] if g["name"] == "Zeustrike Xmas")
    assert game["max_win"] == "999999x"


def test_unpublishing_migrated_game_hides_it_without_deleting(fake_db):
    run_on_startup()

    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_admin_bp)
    app.register_blueprint(lg.lucky_games_public_bp)
    client = app.test_client()

    doc = fake_db["lucky_games"].find_one({"name": "Zeustrike Xmas"})
    game_id = str(doc["_id"])
    with patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None)):
        resp = client.patch(f"/api/admin/lucky-games/{game_id}", json={"is_published": False})
    assert resp.status_code == 200

    public_resp = client.get("/api/lucky-games")
    names = [g["name"] for g in public_resp.get_json()["games"]]
    assert "Zeustrike Xmas" not in names
    # Still present in the DB / admin listing, just unpublished.
    assert fake_db["lucky_games"].find_one({"_id": doc["_id"]}) is not None
