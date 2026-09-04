"""Tests for lucky_games.py: the admin-managed Lucky Games card catalogue
shown in the Mini App.

Covers admin auth, CRUD, publish/unpublish, validation (including the
volatility enum, image/game URL rules, and ObjectId rejection), the PATCH
field allowlist, and the public endpoint's publish-filtering + sort order.
"""

from unittest.mock import patch

import pytest
from bson import ObjectId
from flask import Flask

import database
import lucky_games as lg
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(lg, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_admin_bp)
    app.register_blueprint(lg.lucky_games_public_bp)
    return app


def _mock_admin():
    return patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None))


def _game_doc(**overrides):
    base = {
        "name": "Infinity Ocean",
        "label": "Lucky Game",
        "volatility": "High-Med",
        "max_win": "25000x",
        "image_url": "https://cdn.example.com/infinity-ocean.webp",
        "game_url": "https://games.example.com/infinity-ocean",
        "provider": "PG Soft",
        "sort_order": 10,
        "is_published": True,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Admin auth
# ---------------------------------------------------------------------------


def test_list_lucky_games_requires_admin(fake_db):
    app = _app()
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.get("/api/admin/lucky-games")
    assert resp.status_code == 401


def test_create_lucky_game_requires_admin(fake_db):
    app = _app()
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.post("/api/admin/lucky-games", json=_game_doc())
    assert resp.status_code == 401


def test_update_lucky_game_requires_admin(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={"name": "New"})
    assert resp.status_code == 401


def test_delete_lucky_game_requires_admin(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.delete(f"/api/admin/lucky-games/{doc_id}")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Create / validation
# ---------------------------------------------------------------------------


def test_create_lucky_game_success(fake_db):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=_game_doc())
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["game"]["name"] == "Infinity Ocean"
    assert body["game"]["volatility"] == "High-Med"
    assert "id" in body["game"]


def test_create_lucky_game_missing_name_rejected(fake_db):
    body = _game_doc()
    body["name"] = "   "
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "missing_name"


def test_create_lucky_game_trims_text_fields(fake_db):
    body = _game_doc(name="  Infinity Ocean  ", label="  Lucky Game  ", provider="  PG Soft  ", max_win="  25000x  ")
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 201
    game = resp.get_json()["game"]
    assert game["name"] == "Infinity Ocean"
    assert game["label"] == "Lucky Game"
    assert game["provider"] == "PG Soft"
    assert game["max_win"] == "25000x"


def test_create_lucky_game_defaults_label_and_volatility(fake_db):
    body = {"name": "Bare Game"}
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 201
    game = resp.get_json()["game"]
    assert game["label"] == "Lucky Game"
    assert game["volatility"] == "Medium"
    assert game["sort_order"] == 0
    assert game["is_published"] is False


@pytest.mark.parametrize("volatility", ["Low", "Low-Med", "Medium", "High-Med", "High"])
def test_create_lucky_game_accepts_all_volatility_options(fake_db, volatility):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=_game_doc(volatility=volatility))
    assert resp.status_code == 201
    assert resp.get_json()["game"]["volatility"] == volatility


def test_create_lucky_game_invalid_volatility_rejected(fake_db):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=_game_doc(volatility="Extreme"))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_volatility"


@pytest.mark.parametrize("field,code", [("image_url", "invalid_image_url"), ("game_url", "invalid_game_url")])
def test_create_lucky_game_malformed_url_rejected(fake_db, field, code):
    body = _game_doc()
    body[field] = "javascript:alert(1)"
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == code


def test_create_lucky_game_tg_image_url_rejected(fake_db):
    body = _game_doc(image_url="tg://resolve?domain=advantplay")
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_image_url"


def test_create_lucky_game_tg_game_url_accepted(fake_db):
    body = _game_doc(game_url="tg://resolve?domain=advantplay")
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 201


def test_create_lucky_game_blank_urls_allowed(fake_db):
    body = _game_doc(image_url="", game_url="")
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 201


@pytest.mark.parametrize("bad_sort", [10.5, "ten", True, None])
def test_create_lucky_game_non_integer_sort_order_rejected(fake_db, bad_sort):
    body = _game_doc(sort_order=bad_sort)
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_sort_order"


def test_create_lucky_game_integer_sort_order_accepted(fake_db):
    body = _game_doc(sort_order=5)
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 201
    assert resp.get_json()["game"]["sort_order"] == 5


@pytest.mark.parametrize("bad_published", ["false", "true", 0, 1, "yes", None])
def test_create_lucky_game_non_boolean_is_published_rejected(fake_db, bad_published):
    # A JSON string like "false" is truthy in Python — bool("false") is
    # True — so a naive bool() coercion would silently publish a game the
    # caller meant to keep unpublished. Must be rejected outright instead.
    body = _game_doc(is_published=bad_published)
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_is_published"


def test_patch_non_boolean_is_published_rejected(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc(is_published=False)).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={"is_published": "false"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_is_published"
    # Never silently applied — the stored value must be untouched.
    assert fake_db["lucky_games"].find_one({"_id": doc_id})["is_published"] is False


def test_create_lucky_game_non_object_json_body_returns_clean_400(fake_db):
    # A JSON array/scalar body must not crash the invalid-config logging
    # path (which used to call body.get(...) unconditionally) — it should
    # return a normal JSON 400, never an unhandled 500.
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.post("/api/admin/lucky-games", json=["not", "an", "object"])
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_body"


# ---------------------------------------------------------------------------
# Invalid ObjectId handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_id", ["not-an-object-id", "123", "zzzzzzzzzzzzzzzzzzzzzzzz"])
def test_patch_invalid_object_id_rejected_cleanly(fake_db, bad_id):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{bad_id}", json={"name": "New"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_id"


def test_delete_invalid_object_id_rejected_cleanly(fake_db):
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.delete("/api/admin/lucky-games/not-an-object-id")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_id"


def test_patch_nonexistent_valid_object_id_returns_not_found(fake_db):
    missing_id = ObjectId()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{missing_id}", json={"name": "New"})
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"


def test_delete_nonexistent_valid_object_id_returns_not_found(fake_db):
    missing_id = ObjectId()
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.delete(f"/api/admin/lucky-games/{missing_id}")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"


# ---------------------------------------------------------------------------
# Edit / publish / unpublish / delete
# ---------------------------------------------------------------------------


def test_edit_lucky_game_updates_fields(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={"name": "Infinity Ocean 2", "max_win": "50000x"})
    assert resp.status_code == 200
    body = resp.get_json()["game"]
    assert body["name"] == "Infinity Ocean 2"
    assert body["max_win"] == "50000x"
    # Untouched fields preserved.
    assert body["label"] == "Lucky Game"
    assert body["volatility"] == "High-Med"


def test_publish_and_unpublish_toggle(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc(is_published=False)).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={"is_published": True})
    assert resp.status_code == 200
    assert resp.get_json()["game"]["is_published"] is True

    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={"is_published": False})
    assert resp.status_code == 200
    assert resp.get_json()["game"]["is_published"] is False


def test_delete_lucky_game(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.delete(f"/api/admin/lucky-games/{doc_id}")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}
    assert fake_db["lucky_games"].find_one({"_id": doc_id}) is None


def test_edit_with_no_recognized_fields_rejected(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "no_fields_to_update"


# ---------------------------------------------------------------------------
# PATCH field allowlist — arbitrary fields must never reach the database
# ---------------------------------------------------------------------------


def test_patch_rejects_arbitrary_fields_silently(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(
            f"/api/admin/lucky-games/{doc_id}",
            json={"name": "Renamed", "_id": "hijacked", "created_by": "attacker", "is_admin": True, "role": "superadmin"},
        )
    assert resp.status_code == 200
    stored = fake_db["lucky_games"].find_one({"_id": doc_id})
    assert stored["name"] == "Renamed"
    assert stored["_id"] == doc_id  # unchanged, never overwritten
    assert "is_admin" not in stored
    assert "role" not in stored
    assert stored.get("created_by") != "attacker"


def test_patch_only_arbitrary_fields_rejected_as_no_op(fake_db):
    doc_id = fake_db["lucky_games"].insert_one(_game_doc()).inserted_id
    app = _app()
    client = app.test_client()
    with _mock_admin():
        resp = client.patch(f"/api/admin/lucky-games/{doc_id}", json={"is_admin": True, "role": "superadmin"})
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "no_fields_to_update"
    stored = fake_db["lucky_games"].find_one({"_id": doc_id})
    assert "is_admin" not in stored
    assert "role" not in stored


# ---------------------------------------------------------------------------
# Public endpoint
# ---------------------------------------------------------------------------


def test_public_endpoint_excludes_unpublished_games(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(name="Published Game", is_published=True))
    fake_db["lucky_games"].insert_one(_game_doc(name="Draft Game", is_published=False))
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    assert resp.status_code == 200
    body = resp.get_json()
    names = [g["name"] for g in body["games"]]
    assert names == ["Published Game"]


def test_public_endpoint_sorts_by_sort_order_then_created_at(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(name="Third", sort_order=30))
    fake_db["lucky_games"].insert_one(_game_doc(name="First", sort_order=10))
    fake_db["lucky_games"].insert_one(_game_doc(name="Second", sort_order=20))
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    names = [g["name"] for g in resp.get_json()["games"]]
    assert names == ["First", "Second", "Third"]


def test_public_endpoint_ties_broken_by_created_at(fake_db):
    import time

    fake_db["lucky_games"].insert_one(_game_doc(name="Older", sort_order=10))
    time.sleep(0.01)
    fake_db["lucky_games"].insert_one(_game_doc(name="Newer", sort_order=10))
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    names = [g["name"] for g in resp.get_json()["games"]]
    assert names == ["Older", "Newer"]


def test_public_endpoint_never_exposes_admin_only_fields(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(is_published=True))
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    game = resp.get_json()["games"][0]
    assert "is_published" not in game
    assert "sort_order" not in game
    assert "created_at" not in game
    assert "updated_at" not in game
    assert "created_by" not in game
    assert set(game.keys()) == {"id", "name", "label", "volatility", "max_win", "image_url", "game_url", "provider"}


def test_public_endpoint_empty_when_no_games(fake_db):
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "games": []}


def test_public_endpoint_has_no_store_cache_header(fake_db):
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    assert resp.headers.get("Cache-Control") == "no-store"


def test_public_endpoint_fills_missing_optional_fields_with_empty_string(fake_db):
    # A doc that only ever set the required minimum (name) plus
    # is_published — mirrors a game created via the admin form defaults.
    fake_db["lucky_games"].insert_one({"name": "Bare Game", "is_published": True})
    app = _app()
    client = app.test_client()
    resp = client.get("/api/lucky-games")
    game = resp.get_json()["games"][0]
    assert game["name"] == "Bare Game"
    assert game["image_url"] == ""
    assert game["game_url"] == ""
    assert game["provider"] == ""
