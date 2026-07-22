"""Tests for referral_share_content.py: Referral Centre -> Share Content
(Caption Hooks / Playback Pool CRUD + bulk import + selection/generation)."""

from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

import database
import referral_share_content as rsc
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    return fdb


@pytest.fixture
def app(fake_db, monkeypatch):
    monkeypatch.setattr(rsc, "_require_admin", lambda: ({"id": 999, "usernameLower": "admin"}, None))
    flask_app = Flask(__name__)
    flask_app.register_blueprint(rsc.referral_share_content_bp)
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def _hook(fake_db, text="Fresh replays!", status="active"):
    now = rsc.now_utc()
    doc = {
        "text": text, "status": status, "times_selected": 0, "last_selected_at": None,
        "created_at": now, "updated_at": now, "created_by": None,
    }
    result = fake_db["caption_hooks"].insert_one(doc)
    return result.inserted_id


def _playback(fake_db, playback_id="Abc123", status="active", times_selected=0, last_selected_at=None):
    now = rsc.now_utc()
    doc = {
        "playback_id": playback_id,
        "playback_url": rsc.canonical_playback_url(playback_id),
        "game_name": "", "status": status, "times_selected": times_selected,
        "last_selected_at": last_selected_at, "created_at": now, "updated_at": now, "created_by": None,
    }
    result = fake_db["playback_pool"].insert_one(doc)
    return result.inserted_id


# ---------------------------------------------------------------------------
# Playback validation and canonicalization
# ---------------------------------------------------------------------------

class TestPlaybackValidation:
    def test_bare_id_valid_charset_and_length(self):
        assert rsc.validate_playback_id("Abc-123_XYZ") == "Abc-123_XYZ"

    def test_bare_id_too_short_rejected(self):
        assert rsc.validate_playback_id("abcd") is None

    def test_bare_id_too_long_rejected(self):
        assert rsc.validate_playback_id("a" * 101) is None

    def test_bare_id_invalid_chars_rejected(self):
        assert rsc.validate_playback_id("abc$123") is None
        assert rsc.validate_playback_id("abc 123") is None

    def test_canonical_url_construction_preserves_case(self):
        assert rsc.canonical_playback_url("AbC123") == "https://rx.apreplay.com/AbC123"

    def test_parse_accepts_bare_id(self):
        assert rsc.parse_playback_url("  AbC123  ") == "AbC123"

    def test_parse_accepts_canonical_url(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/AbC123") == "AbC123"

    def test_parse_rejects_http(self):
        assert rsc.parse_playback_url("http://rx.apreplay.com/AbC123") is None

    def test_parse_rejects_other_domain(self):
        assert rsc.parse_playback_url("https://evil.com/AbC123") is None

    def test_parse_rejects_subdomain(self):
        assert rsc.parse_playback_url("https://sub.rx.apreplay.com/AbC123") is None

    def test_parse_rejects_userinfo(self):
        assert rsc.parse_playback_url("https://user:pass@rx.apreplay.com/AbC123") is None

    def test_parse_rejects_custom_port(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com:8443/AbC123") is None

    def test_parse_rejects_malformed_port_without_crashing(self):
        # A non-numeric/out-of-range port makes urlparse's `.port` property
        # raise ValueError rather than returning a value — must be treated
        # as invalid input, not bubble up as a 500.
        assert rsc.parse_playback_url("https://rx.apreplay.com:bad/AbC123") is None
        assert rsc.parse_playback_url("https://rx.apreplay.com:999999/AbC123") is None

    def test_parse_rejects_query_string(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/AbC123?x=1") is None

    def test_parse_rejects_fragment(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/AbC123#frag") is None

    def test_parse_rejects_empty_path(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com") is None
        assert rsc.parse_playback_url("https://rx.apreplay.com/") is None

    def test_parse_rejects_additional_path_segments(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/AbC123/extra") is None
        assert rsc.parse_playback_url("https://rx.apreplay.com/AbC123/") is None

    def test_parse_rejects_malformed_id_in_url(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/a$") is None

    def test_parse_rejects_empty_and_non_string(self):
        assert rsc.parse_playback_url("") is None
        assert rsc.parse_playback_url(None) is None

    def test_parse_rejects_embedded_whitespace_and_control_chars(self):
        # Bare-ID path: internal whitespace/control chars fail the charset regex directly.
        assert rsc.parse_playback_url("Abc\t12345") is None
        assert rsc.parse_playback_url("Abc 12345") is None
        assert rsc.parse_playback_url("Abc\n12345") is None
        # Full-URL path: urlparse silently strips embedded tab/newline/CR from
        # anywhere in the string, so without an explicit up-front check this
        # would otherwise resolve to a different, seemingly-valid ID instead
        # of being rejected.
        assert rsc.parse_playback_url("https://rx.apreplay.com/Ab\tc12345") is None
        assert rsc.parse_playback_url("https://rx.apreplay.com/Ab\nc12345") is None
        assert rsc.parse_playback_url("https://rx.apreplay.com/Ab c12345") is None

    def test_parse_rejects_explicit_default_port_443(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com:443/AbC123") is None

    def test_parse_rejects_encoded_slash_in_path(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/Abc%2F123") is None

    def test_parse_rejects_backslash_in_path(self):
        assert rsc.parse_playback_url("https://rx.apreplay.com/Abc\\123") is None


# ---------------------------------------------------------------------------
# Caption Hooks CRUD
# ---------------------------------------------------------------------------

class TestHookCrud:
    def test_create_list_and_search(self, client, fake_db):
        r = client.post("/api/admin/referral/share-content/hooks", json={"text": "Big win incoming!"})
        assert r.status_code == 201
        r = client.get("/api/admin/referral/share-content/hooks")
        hooks = r.get_json()["hooks"]
        assert len(hooks) == 1
        assert hooks[0]["text"] == "Big win incoming!"
        assert hooks[0]["status"] == "active"

        r = client.get("/api/admin/referral/share-content/hooks?q=nomatch")
        assert r.get_json()["hooks"] == []

    def test_status_filter(self, client, fake_db):
        _hook(fake_db, "A", status="active")
        _hook(fake_db, "B", status="inactive")
        r = client.get("/api/admin/referral/share-content/hooks?status=inactive")
        hooks = r.get_json()["hooks"]
        assert len(hooks) == 1 and hooks[0]["text"] == "B"

    def test_create_missing_text_rejected(self, client):
        r = client.post("/api/admin/referral/share-content/hooks", json={"text": "   "})
        assert r.status_code == 400
        assert r.get_json()["code"] == "missing_text"

    def test_create_text_too_long_rejected(self, client):
        r = client.post("/api/admin/referral/share-content/hooks", json={"text": "x" * 501})
        assert r.status_code == 400
        assert r.get_json()["code"] == "text_too_long"

    def test_edit_hook(self, client, fake_db):
        hook_id = _hook(fake_db, "Old text")
        r = client.put(f"/api/admin/referral/share-content/hooks/{hook_id}", json={"text": "New text"})
        assert r.status_code == 200
        assert fake_db["caption_hooks"].find_one({"_id": hook_id})["text"] == "New text"

    def test_edit_text_too_long_rejected(self, client, fake_db):
        hook_id = _hook(fake_db, "Old text")
        r = client.put(f"/api/admin/referral/share-content/hooks/{hook_id}", json={"text": "x" * 501})
        assert r.status_code == 400
        assert r.get_json()["code"] == "text_too_long"

    def test_edit_preserves_created_at_and_updates_updated_at(self, client, fake_db):
        hook_id = _hook(fake_db, "Old text")
        before = fake_db["caption_hooks"].find_one({"_id": hook_id})
        client.put(f"/api/admin/referral/share-content/hooks/{hook_id}", json={"text": "New text"})
        after = fake_db["caption_hooks"].find_one({"_id": hook_id})
        assert after["created_at"] == before["created_at"]
        assert after["updated_at"] >= before["updated_at"]

    def test_activate_deactivate_does_not_reset_counters(self, client, fake_db):
        hook_id = _hook(fake_db, "A", status="active")
        fake_db["caption_hooks"].update_one({"_id": hook_id}, {"$set": {"times_selected": 7}})
        client.post(f"/api/admin/referral/share-content/hooks/{hook_id}/deactivate")
        client.post(f"/api/admin/referral/share-content/hooks/{hook_id}/activate")
        assert fake_db["caption_hooks"].find_one({"_id": hook_id})["times_selected"] == 7

    def test_activate_deactivate(self, client, fake_db):
        hook_id = _hook(fake_db, "A", status="active")
        client.post(f"/api/admin/referral/share-content/hooks/{hook_id}/deactivate")
        assert fake_db["caption_hooks"].find_one({"_id": hook_id})["status"] == "inactive"
        client.post(f"/api/admin/referral/share-content/hooks/{hook_id}/activate")
        assert fake_db["caption_hooks"].find_one({"_id": hook_id})["status"] == "active"

    def test_delete_hook(self, client, fake_db):
        hook_id = _hook(fake_db, "A")
        r = client.delete(f"/api/admin/referral/share-content/hooks/{hook_id}")
        assert r.status_code == 200
        assert fake_db["caption_hooks"].find_one({"_id": hook_id}) is None

    def test_delete_missing_hook_404(self, client):
        r = client.delete("/api/admin/referral/share-content/hooks/000000000000000000000000")
        assert r.status_code == 404


class TestHookBulkImport:
    def test_bulk_import_one_per_line_with_blank_lines(self, client, fake_db):
        blob = "Hook one\n\n  Hook two  \n\nHook one\n"
        r = client.post("/api/admin/referral/share-content/hooks/bulk-import", json={"lines": blob})
        body = r.get_json()
        assert body["inserted"] == 2
        assert body["skipped"] == 1  # in-batch duplicate ("Hook one" repeated)
        assert body["rejected"] == 0
        texts = {d["text"] for d in fake_db["caption_hooks"].find({})}
        assert texts == {"Hook one", "Hook two"}

    def test_bulk_import_dedupes_against_existing_db_rows(self, client, fake_db):
        _hook(fake_db, "Existing hook")
        r = client.post(
            "/api/admin/referral/share-content/hooks/bulk-import",
            json={"lines": "Existing hook\nBrand new hook"},
        )
        body = r.get_json()
        assert body["inserted"] == 1
        assert body["skipped"] == 1

    def test_bulk_import_rejects_too_long_line(self, client):
        r = client.post(
            "/api/admin/referral/share-content/hooks/bulk-import",
            json={"lines": "x" * 501},
        )
        body = r.get_json()
        assert body["rejected"] == 1
        assert body["results"][0]["reason"] == "too_long"

    def test_bulk_import_reconciles_totals_with_per_line_results(self, client, fake_db):
        _hook(fake_db, "Existing hook")
        blob = "Existing hook\nNew hook one\nNew hook one\n" + "y" * 501 + "\nNew hook two"
        r = client.post("/api/admin/referral/share-content/hooks/bulk-import", json={"lines": blob})
        body = r.get_json()
        assert len(body["results"]) == 5
        assert body["inserted"] + body["skipped"] + body["rejected"] == len(body["results"])
        assert body["inserted"] == 2
        assert body["skipped"] == 2
        assert body["rejected"] == 1

    def test_bulk_import_over_line_cap_rejected_before_processing(self, client, fake_db):
        blob = "\n".join(f"hook {i}" for i in range(rsc.MAX_BULK_IMPORT_LINES + 1))
        r = client.post("/api/admin/referral/share-content/hooks/bulk-import", json={"lines": blob})
        assert r.status_code == 400
        assert r.get_json()["code"] == "too_many_lines"
        assert fake_db["caption_hooks"].count_documents({}) == 0

    def test_bulk_import_at_line_cap_is_allowed(self, client, fake_db):
        blob = "\n".join(f"hook {i}" for i in range(rsc.MAX_BULK_IMPORT_LINES))
        r = client.post("/api/admin/referral/share-content/hooks/bulk-import", json={"lines": blob})
        assert r.status_code == 200
        assert r.get_json()["inserted"] == rsc.MAX_BULK_IMPORT_LINES


# ---------------------------------------------------------------------------
# Playback Pool CRUD
# ---------------------------------------------------------------------------

class TestPlaybackCrud:
    def test_create_rejects_malformed_url(self, client):
        r = client.post("/api/admin/referral/share-content/playback", json={"url": "not a url"})
        assert r.status_code == 400
        assert r.get_json()["code"] == "invalid_playback_url"

    def test_create_accepts_bare_id_and_stores_canonical_url(self, client, fake_db):
        r = client.post("/api/admin/referral/share-content/playback", json={"url": "Repl4y01"})
        assert r.status_code == 201
        doc = fake_db["playback_pool"].find_one({"playback_id": "Repl4y01"})
        assert doc["playback_url"] == "https://rx.apreplay.com/Repl4y01"

    def test_duplicate_playback_id_rejected(self, client, fake_db):
        _playback(fake_db, "Dup12345")
        r = client.post("/api/admin/referral/share-content/playback", json={"url": "Dup12345"})
        assert r.status_code == 409
        assert r.get_json()["code"] == "duplicate_playback"

    def test_duplicate_canonical_url_rejected(self, client, fake_db):
        _playback(fake_db, "Dup12345")
        r = client.post(
            "/api/admin/referral/share-content/playback",
            json={"url": "https://rx.apreplay.com/Dup12345"},
        )
        assert r.status_code == 409

    def test_edit_url_and_game_name(self, client, fake_db):
        pid = _playback(fake_db, "Original1")
        r = client.put(
            f"/api/admin/referral/share-content/playback/{pid}",
            json={"url": "Renamed01", "game_name": "Crash Royale"},
        )
        assert r.status_code == 200
        doc = fake_db["playback_pool"].find_one({"_id": pid})
        assert doc["playback_id"] == "Renamed01"
        assert doc["game_name"] == "Crash Royale"

    def test_edit_to_conflicting_url_rejected(self, client, fake_db):
        _playback(fake_db, "Taken0001")
        pid = _playback(fake_db, "Free00001")
        r = client.put(f"/api/admin/referral/share-content/playback/{pid}", json={"url": "Taken0001"})
        assert r.status_code == 409

    def test_activate_deactivate_and_delete(self, client, fake_db):
        pid = _playback(fake_db, "Aaaa1111", status="active")
        client.post(f"/api/admin/referral/share-content/playback/{pid}/deactivate")
        assert fake_db["playback_pool"].find_one({"_id": pid})["status"] == "inactive"
        r = client.delete(f"/api/admin/referral/share-content/playback/{pid}")
        assert r.status_code == 200
        assert fake_db["playback_pool"].find_one({"_id": pid}) is None

    def test_search_by_game_name(self, client, fake_db):
        pid = _playback(fake_db, "Search001")
        fake_db["playback_pool"].update_one({"_id": pid}, {"$set": {"game_name": "Mines"}})
        r = client.get("/api/admin/referral/share-content/playback?q=mines")
        assert len(r.get_json()["playback"]) == 1

    def test_edit_game_name_too_long_rejected(self, client, fake_db):
        pid = _playback(fake_db, "Gname0001")
        r = client.put(f"/api/admin/referral/share-content/playback/{pid}", json={"game_name": "x" * 201})
        assert r.status_code == 400
        assert r.get_json()["code"] == "game_name_too_long"

    def test_create_game_name_too_long_rejected(self, client):
        r = client.post(
            "/api/admin/referral/share-content/playback",
            json={"url": "GnameCreate1", "game_name": "x" * 201},
        )
        assert r.status_code == 400
        assert r.get_json()["code"] == "game_name_too_long"

    def test_edit_race_past_precheck_returns_409_not_500(self, client, fake_db, monkeypatch):
        # Simulate a race where the pre-check find_one misses a concurrent
        # conflicting write, but the real unique index still rejects the
        # update at the database layer — must surface as 409, not a 500.
        import pymongo.errors

        pid = _playback(fake_db, "RaceEdit1")

        def _raise_duplicate(*args, **kwargs):
            raise pymongo.errors.DuplicateKeyError("duplicate key")

        monkeypatch.setattr(fake_db["playback_pool"], "update_one", _raise_duplicate)
        r = client.put(f"/api/admin/referral/share-content/playback/{pid}", json={"url": "SomeNewId1"})
        assert r.status_code == 409
        assert r.get_json()["code"] == "duplicate_playback"


class TestPlaybackBulkImport:
    def test_bulk_import_one_per_line(self, client, fake_db):
        blob = "Line0001\n\nhttps://rx.apreplay.com/Line0002\nLine0001\n"
        r = client.post("/api/admin/referral/share-content/playback/bulk-import", json={"lines": blob})
        body = r.get_json()
        assert body["inserted"] == 2
        assert body["skipped"] == 1
        assert body["rejected"] == 0

    def test_bulk_import_rejects_malformed_lines(self, client):
        r = client.post(
            "/api/admin/referral/share-content/playback/bulk-import",
            json={"lines": "not a valid id!!\nhttp://rx.apreplay.com/BadScheme"},
        )
        body = r.get_json()
        assert body["rejected"] == 2
        assert body["inserted"] == 0
        assert all(item["reason"] == "invalid_format" for item in body["results"])

    def test_bulk_import_dedupes_against_existing_rows(self, client, fake_db):
        _playback(fake_db, "Existing1")
        r = client.post(
            "/api/admin/referral/share-content/playback/bulk-import",
            json={"lines": "Existing1\nBrandNew1"},
        )
        body = r.get_json()
        assert body["inserted"] == 1
        assert body["skipped"] == 1

    def test_bulk_import_reconciles_totals_with_per_line_results(self, client, fake_db):
        _playback(fake_db, "Existing1")
        blob = "Existing1\nNewOne0001\nNewOne0001\nnot valid!!\nNewTwo0001"
        r = client.post("/api/admin/referral/share-content/playback/bulk-import", json={"lines": blob})
        body = r.get_json()
        assert len(body["results"]) == 5
        assert body["inserted"] + body["skipped"] + body["rejected"] == len(body["results"])
        assert body["inserted"] == 2
        assert body["skipped"] == 2
        assert body["rejected"] == 1

    def test_bulk_import_over_line_cap_rejected_before_processing(self, client, fake_db):
        blob = "\n".join(f"Pool{i:07d}" for i in range(rsc.MAX_BULK_IMPORT_LINES + 1))
        r = client.post("/api/admin/referral/share-content/playback/bulk-import", json={"lines": blob})
        assert r.status_code == 400
        assert r.get_json()["code"] == "too_many_lines"
        assert fake_db["playback_pool"].count_documents({}) == 0

    def test_edit_preserves_created_at_and_updates_updated_at(self, client, fake_db):
        pid = _playback(fake_db, "Preserve1")
        before = fake_db["playback_pool"].find_one({"_id": pid})
        client.put(f"/api/admin/referral/share-content/playback/{pid}", json={"game_name": "New Name"})
        after = fake_db["playback_pool"].find_one({"_id": pid})
        assert after["created_at"] == before["created_at"]
        assert after["updated_at"] >= before["updated_at"]

    def test_activate_deactivate_does_not_reset_counters(self, client, fake_db):
        pid = _playback(fake_db, "Counter01", status="active", times_selected=9)
        client.post(f"/api/admin/referral/share-content/playback/{pid}/deactivate")
        client.post(f"/api/admin/referral/share-content/playback/{pid}/activate")
        assert fake_db["playback_pool"].find_one({"_id": pid})["times_selected"] == 9


# ---------------------------------------------------------------------------
# Admin authorization
# ---------------------------------------------------------------------------

class TestAdminAuthorization:
    def test_unauthorized_request_rejected(self, fake_db, monkeypatch):
        from flask import jsonify as flask_jsonify
        monkeypatch.setattr(
            rsc, "_require_admin", lambda: (None, (flask_jsonify({"status": "error", "code": "auth_failed"}), 401))
        )
        flask_app = Flask(__name__)
        flask_app.register_blueprint(rsc.referral_share_content_bp)
        client = flask_app.test_client()

        r = client.get("/api/admin/referral/share-content/hooks")
        assert r.status_code == 401
        r = client.post("/api/admin/referral/share-content/hooks", json={"text": "x"})
        assert r.status_code == 401
        r = client.post("/api/admin/referral/share-content/playback", json={"url": "Abcde123"})
        assert r.status_code == 401

    def test_every_registered_route_requires_admin(self, fake_db, monkeypatch):
        # Exhaustively walk every route this blueprint registers (create,
        # edit, status, delete, bulk-import, and both list/read endpoints)
        # and confirm none of them are reachable without the admin guard.
        from flask import jsonify as flask_jsonify
        monkeypatch.setattr(
            rsc, "_require_admin", lambda: (None, (flask_jsonify({"status": "error", "code": "auth_failed"}), 401))
        )
        flask_app = Flask(__name__)
        flask_app.register_blueprint(rsc.referral_share_content_bp)
        client = flask_app.test_client()

        placeholder_id = "000000000000000000000000"
        checked = 0
        for rule in flask_app.url_map.iter_rules():
            if not rule.rule.startswith("/api/admin/referral/share-content"):
                continue
            path = rule.rule
            for arg in rule.arguments:
                path = path.replace(f"<{arg}>", placeholder_id)
            for method in rule.methods - {"HEAD", "OPTIONS"}:
                if method == "GET":
                    resp = client.get(path)
                elif method == "POST":
                    resp = client.post(path, json={})
                elif method == "PUT":
                    resp = client.put(path, json={})
                elif method == "DELETE":
                    resp = client.delete(path)
                else:
                    continue
                assert resp.status_code == 401, f"{method} {path} did not enforce admin auth (got {resp.status_code})"
                checked += 1
        assert checked >= 14  # sanity: make sure the walk actually found all the routes


# ---------------------------------------------------------------------------
# Selection algorithm
# ---------------------------------------------------------------------------

class TestHookSelection:
    def test_only_active_hooks_selected(self, fake_db):
        _hook(fake_db, "Inactive", status="inactive")
        active_id = _hook(fake_db, "Active", status="active")
        for _ in range(10):
            doc = rsc.select_hook()
            assert doc["_id"] == active_id

    def test_no_active_hooks_returns_none(self, fake_db):
        _hook(fake_db, "Inactive", status="inactive")
        assert rsc.select_hook() is None

    def test_selection_atomically_bumps_counter_and_timestamp(self, fake_db):
        hook_id = _hook(fake_db, "A")
        rsc.select_hook()
        doc = fake_db["caption_hooks"].find_one({"_id": hook_id})
        assert doc["times_selected"] == 1
        assert doc["last_selected_at"] is not None


class TestPlaybackSelection:
    def test_only_active_records_selected(self, fake_db):
        _playback(fake_db, "Inactive1", status="inactive")
        active_id = _playback(fake_db, "Active001", status="active")
        for _ in range(5):
            doc = rsc.select_playback_for_user(user_id=1)
            assert doc["_id"] == active_id

    def test_empty_pool_returns_none(self, fake_db):
        assert rsc.select_playback_for_user(user_id=1) is None

    def test_least_used_preferred(self, fake_db):
        heavily_used = _playback(fake_db, "Heavy0001", times_selected=50)
        rarely_used = _playback(fake_db, "Rare00001", times_selected=1)
        doc = rsc.select_playback_for_user(user_id=1)
        assert doc["_id"] == rarely_used

    def test_atomic_counter_increment_on_selection(self, fake_db):
        pid = _playback(fake_db, "Abcde111", times_selected=0)
        rsc.select_playback_for_user(user_id=1)
        doc = fake_db["playback_pool"].find_one({"_id": pid})
        assert doc["times_selected"] == 1
        assert doc["last_selected_at"] is not None

    def test_consecutive_repeat_avoided_when_alternative_exists(self, fake_db):
        first = _playback(fake_db, "First0001")
        second = _playback(fake_db, "Second001")
        # Simulate that `first` was the user's most recently generated playback.
        fake_db["share_generations"].insert_one({
            "user_id": 42, "playback_record_id": first, "generated_at": rsc.now_utc(),
        })
        doc = rsc.select_playback_for_user(user_id=42)
        assert doc["_id"] == second

    def test_single_playback_reused_when_it_is_the_last_one(self, fake_db):
        only = _playback(fake_db, "Only00001")
        fake_db["share_generations"].insert_one({
            "user_id": 7, "playback_record_id": only, "generated_at": rsc.now_utc(),
        })
        doc = rsc.select_playback_for_user(user_id=7)
        assert doc["_id"] == only
        assert doc["times_selected"] == 1

    def test_selection_is_a_single_atomic_find_one_and_update_never_find_then_update(self):
        # The concurrency-safety requirement is that the pick (filter+sort)
        # and the mutation ($inc/$set) happen as ONE atomic MongoDB
        # find_one_and_update call, never a separate find() followed by a
        # later update_one() on the chosen document (a fake in-memory
        # collection can't reproduce MongoDB's real single-document
        # atomicity under true concurrent threads, so this is asserted at
        # the implementation level instead of via a racing-threads test).
        import inspect

        src = inspect.getsource(rsc.select_playback_for_user)
        assert src.count("find_one_and_update(") == 2  # exclusion attempt + no-alternative fallback
        assert ".update_one(" not in src

    def test_repeated_sequential_selection_sums_counters_exactly(self, fake_db):
        _playback(fake_db, "Pool00001")
        _playback(fake_db, "Pool00002")
        n_calls = 25
        for i in range(n_calls):
            assert rsc.select_playback_for_user(user_id=1000 + i) is not None
        total = sum(d["times_selected"] for d in fake_db["playback_pool"].find({}))
        assert total == n_calls


# ---------------------------------------------------------------------------
# Full package generation
# ---------------------------------------------------------------------------

class TestGenerateSharePackage:
    def _patch_invite_link(self, monkeypatch, link="https://t.me/+canonicalHash", raise_error=None):
        import types
        import sys

        fake_main = types.ModuleType("main")

        def _fake_get_or_create(user_id, username=""):
            if raise_error is not None:
                raise raise_error
            return link

        fake_main.get_or_create_referral_invite_link_sync = _fake_get_or_create
        monkeypatch.setitem(sys.modules, "main", fake_main)

    def test_exact_message_and_whitespace_formatting(self, fake_db, monkeypatch):
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _hook(fake_db, "🔥 Big wins today!")
        _playback(fake_db, "Play00001")

        result = rsc.generate_share_package(555, "tester")
        assert result["ok"] is True
        expected = (
            "🔥 Big wins today!\n"
            "https://rx.apreplay.com/Play00001\n\n"
            "More player replays and rewards inside AdvantPlay:\n"
            "👉 https://t.me/+abc123"
        )
        assert result["message"] == expected

    def test_writes_share_generations_only_on_full_success(self, fake_db, monkeypatch):
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _hook(fake_db, "Hook text")
        _playback(fake_db, "Play00002")

        rsc.generate_share_package(1, "user1")
        rows = list(fake_db["share_generations"].find({}))
        assert len(rows) == 1
        row = rows[0]
        assert row["user_id"] == 1
        assert row["playback_id"] == "Play00002"
        assert row["playback_url"] == "https://rx.apreplay.com/Play00002"
        assert row["invite_link"] == "https://t.me/+abc123"
        assert row["hook_text"] == "Hook text"
        assert row["generated_at"] is not None

    def test_default_hook_used_when_none_active_and_hook_id_null(self, fake_db, monkeypatch):
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _playback(fake_db, "Play00003")

        result = rsc.generate_share_package(2, "user2")
        assert result["ok"] is True
        assert result["hook_text"] == rsc.DEFAULT_FALLBACK_HOOK_TEXT
        row = fake_db["share_generations"].find_one({"user_id": 2})
        assert row["hook_id"] is None

    def test_no_active_playback_returns_retryable_error_and_no_history(self, fake_db, monkeypatch):
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _hook(fake_db, "Hook text")
        # no playback records at all

        result = rsc.generate_share_package(3, "user3")
        assert result == {"ok": False, "code": "no_active_playback"}
        assert fake_db["share_generations"].count_documents({}) == 0

    def test_invite_link_failure_returns_retryable_error_and_no_history(self, fake_db, monkeypatch):
        self._patch_invite_link(monkeypatch, raise_error=RuntimeError("createChatInviteLink failed"))
        _hook(fake_db, "Hook text")
        _playback(fake_db, "Play00004")

        result = rsc.generate_share_package(4, "user4")
        assert result == {"ok": False, "code": "invite_link_failed"}
        assert fake_db["share_generations"].count_documents({}) == 0
        # Playback counter was still incremented (documented, accepted discrepancy) —
        # not rolled back since this codebase has no multi-document transactions.
        doc = fake_db["playback_pool"].find_one({"playback_id": "Play00004"})
        assert doc["times_selected"] == 1

    def test_never_fabricates_a_link_on_failure(self, fake_db, monkeypatch):
        self._patch_invite_link(monkeypatch, raise_error=RuntimeError("boom"))
        _hook(fake_db, "Hook text")
        _playback(fake_db, "Play00005")

        result = rsc.generate_share_package(5, "user5")
        assert "invite_link" not in result
        assert "message" not in result
