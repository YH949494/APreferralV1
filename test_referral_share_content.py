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
# Bulk management (Hooks / Playback Links) -- activate_all / deactivate_all /
# delete_selected, and the shared deletion service backing individual delete.
# ---------------------------------------------------------------------------

class TestBulkActivateDeactivate:
    def test_activate_all_hooks_does_not_affect_playback(self, client, fake_db):
        _hook(fake_db, "A", status="inactive")
        _hook(fake_db, "B", status="inactive")
        _playback(fake_db, "Playback01", status="inactive")

        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "hook", "action": "activate_all"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["total_count"] == 2
        assert body["matched_count"] == 2
        assert body["eligible_count"] == 2
        assert body["modified_count"] == 2
        assert body["active_count"] == 2

        assert all(h["status"] == "active" for h in fake_db["caption_hooks"].find({}))
        # Playback pool is a completely separate collection -- untouched.
        assert fake_db["playback_pool"].find_one({"playback_id": "Playback01"})["status"] == "inactive"

    def test_activate_all_playback_does_not_affect_hooks(self, client, fake_db):
        _hook(fake_db, "A", status="inactive")
        _playback(fake_db, "Playback01", status="inactive")
        _playback(fake_db, "Playback02", status="inactive")

        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "playback_link", "action": "activate_all"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["matched_count"] == 2
        assert body["modified_count"] == 2

        assert all(p["status"] == "active" for p in fake_db["playback_pool"].find({}))
        assert fake_db["caption_hooks"].find_one({"text": "A"})["status"] == "inactive"

    def test_activate_all_only_matches_records_not_already_active(self, client, fake_db):
        # Query itself is filtered to status != target -- an already-active
        # record's updated_at must never be touched by activate_all.
        already_active_id = _hook(fake_db, "A", status="active")
        _hook(fake_db, "B", status="inactive")
        before = fake_db["caption_hooks"].find_one({"_id": already_active_id})["updated_at"]

        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "hook", "action": "activate_all"})
        body = r.get_json()
        assert body["total_count"] == 2
        assert body["matched_count"] == 1  # only B was eligible
        assert body["modified_count"] == 1
        assert body["active_count"] == 2

        after = fake_db["caption_hooks"].find_one({"_id": already_active_id})["updated_at"]
        assert after == before, "already-active record's updated_at must not change"

    def test_repeated_activate_all_call_performs_no_further_mutation(self, client, fake_db):
        _hook(fake_db, "A", status="active")
        _hook(fake_db, "B", status="inactive")
        r1 = client.post("/api/admin/referral/share-content/bulk-action",
                          json={"resource_type": "hook", "action": "activate_all"})
        assert r1.get_json()["modified_count"] == 1
        assert r1.get_json()["active_count"] == 2

        snapshot = {d["_id"]: dict(d) for d in fake_db["caption_hooks"].find({})}

        r2 = client.post("/api/admin/referral/share-content/bulk-action",
                          json={"resource_type": "hook", "action": "activate_all"})
        body2 = r2.get_json()
        assert body2["matched_count"] == 0
        assert body2["modified_count"] == 0
        assert body2["active_count"] == 2
        assert body2["total_count"] == 2

        after = {d["_id"]: dict(d) for d in fake_db["caption_hooks"].find({})}
        assert after == snapshot, "a repeated identical request must not mutate any document"

    def test_repeated_deactivate_all_call_performs_no_further_mutation(self, client, fake_db):
        _hook(fake_db, "A", status="active")
        _hook(fake_db, "B", status="inactive")
        r1 = client.post("/api/admin/referral/share-content/bulk-action",
                          json={"resource_type": "hook", "action": "deactivate_all"})
        assert r1.get_json()["modified_count"] == 1
        assert r1.get_json()["active_count"] == 0

        snapshot = {d["_id"]: dict(d) for d in fake_db["caption_hooks"].find({})}

        r2 = client.post("/api/admin/referral/share-content/bulk-action",
                          json={"resource_type": "hook", "action": "deactivate_all"})
        body2 = r2.get_json()
        assert body2["matched_count"] == 0
        assert body2["modified_count"] == 0
        assert body2["active_count"] == 0

        after = {d["_id"]: dict(d) for d in fake_db["caption_hooks"].find({})}
        assert after == snapshot, "a repeated identical request must not mutate any document"

    def test_repeated_activate_all_playback_performs_no_further_mutation(self, client, fake_db):
        _playback(fake_db, "Playback01", status="inactive")
        _playback(fake_db, "Playback02", status="active")
        r1 = client.post("/api/admin/referral/share-content/bulk-action",
                          json={"resource_type": "playback_link", "action": "activate_all"})
        assert r1.get_json()["modified_count"] == 1
        r2 = client.post("/api/admin/referral/share-content/bulk-action",
                          json={"resource_type": "playback_link", "action": "activate_all"})
        body2 = r2.get_json()
        assert body2["matched_count"] == 0
        assert body2["modified_count"] == 0
        assert body2["active_count"] == 2

    def test_deactivate_all_hooks_returns_matched_and_modified_counts(self, client, fake_db):
        _hook(fake_db, "A", status="active")
        _hook(fake_db, "B", status="active")
        _hook(fake_db, "C", status="inactive")
        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "hook", "action": "deactivate_all"})
        body = r.get_json()
        assert body["total_count"] == 3
        assert body["matched_count"] == 2  # only the two active ones were eligible
        assert body["modified_count"] == 2
        assert body["active_count"] == 0
        assert all(h["status"] == "inactive" for h in fake_db["caption_hooks"].find({}))

    def test_deactivate_all_playback_scoped_to_playback_only(self, client, fake_db):
        _hook(fake_db, "A", status="active")
        _playback(fake_db, "Playback01", status="active")
        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "playback_link", "action": "deactivate_all"})
        body = r.get_json()
        assert body["matched_count"] == 1
        assert body["active_count"] == 0
        assert fake_db["caption_hooks"].find_one({"text": "A"})["status"] == "active"

    def test_invalid_resource_type_rejected(self, client, fake_db):
        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "share_generations", "action": "activate_all"})
        assert r.status_code == 400
        assert r.get_json()["code"] == "invalid_resource_type"
        # No collection-name injection: the whitelisted map is the only route in.
        assert "share_generations" not in rsc.RESOURCE_COLLECTIONS

    def test_invalid_action_rejected(self, client, fake_db):
        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "hook", "action": "delete_all"})
        assert r.status_code == 400
        assert r.get_json()["code"] == "invalid_action"

    def test_unauthenticated_bulk_action_rejected(self, fake_db, monkeypatch):
        from flask import jsonify as flask_jsonify
        monkeypatch.setattr(
            rsc, "_require_admin", lambda: (None, (flask_jsonify({"status": "error", "code": "auth_failed"}), 401))
        )
        flask_app = Flask(__name__)
        flask_app.register_blueprint(rsc.referral_share_content_bp)
        client = flask_app.test_client()
        r = client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "hook", "action": "activate_all"})
        assert r.status_code == 401


class TestBulkDeleteSelected:
    def test_delete_selected_valid_ids(self, client, fake_db):
        id1 = _hook(fake_db, "A")
        id2 = _hook(fake_db, "B")
        id3 = _hook(fake_db, "C")
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
            "selected_ids": [str(id1), str(id2)],
        })
        assert r.status_code == 200
        body = r.get_json()
        assert body["matched_count"] == 2
        assert body["deleted_count"] == 2
        assert fake_db["caption_hooks"].count_documents({}) == 1
        assert fake_db["caption_hooks"].find_one({"_id": id3}) is not None

    def test_delete_selected_playback_valid_ids(self, client, fake_db):
        id1 = _playback(fake_db, "Playback01")
        id2 = _playback(fake_db, "Playback02")
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "playback_link", "action": "delete_selected",
            "selected_ids": [str(id1), str(id2)],
        })
        body = r.get_json()
        assert body["deleted_count"] == 2
        assert fake_db["playback_pool"].count_documents({}) == 0

    def test_empty_selection_rejected(self, client, fake_db):
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected", "selected_ids": [],
        })
        assert r.status_code == 400
        assert r.get_json()["code"] == "empty_selection"

    def test_missing_selected_ids_rejected(self, client, fake_db):
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
        })
        assert r.status_code == 400
        assert r.get_json()["code"] == "empty_selection"

    def test_malformed_id_rejected_no_partial_delete(self, client, fake_db):
        id1 = _hook(fake_db, "A")
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
            "selected_ids": [str(id1), "not-an-object-id"],
        })
        assert r.status_code == 400
        assert r.get_json()["code"] == "malformed_ids"
        # The valid id in the same batch must NOT have been deleted.
        assert fake_db["caption_hooks"].find_one({"_id": id1}) is not None

    def test_duplicate_ids_rejected_no_partial_delete(self, client, fake_db):
        id1 = _hook(fake_db, "A")
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
            "selected_ids": [str(id1), str(id1)],
        })
        assert r.status_code == 400
        assert r.get_json()["code"] == "duplicate_ids"
        assert fake_db["caption_hooks"].find_one({"_id": id1}) is not None

    def test_unknown_id_rejected_no_partial_delete(self, client, fake_db):
        id1 = _hook(fake_db, "A")
        unknown = "0" * 24
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
            "selected_ids": [str(id1), unknown],
        })
        assert r.status_code == 400
        assert r.get_json()["code"] == "unknown_ids"
        # Nothing deleted -- including the one valid id in the batch.
        assert fake_db["caption_hooks"].find_one({"_id": id1}) is not None

    def test_cross_resource_id_rejected(self, client, fake_db):
        # FakeCollection ids are per-collection sequential counters starting
        # at 1, so a hook and a playback record can coincidentally get the
        # *same* ObjectId value (harmless in prod against real Mongo, where
        # ids are globally unique). Insert a throwaway playback record first
        # so the id under test is guaranteed to not already coincide with
        # any id in the hooks collection.
        hook_id = _hook(fake_db, "A")
        _playback(fake_db, "filler")
        playback_id = _playback(fake_db, "Playback01")
        assert hook_id != playback_id
        assert fake_db["caption_hooks"].find_one({"_id": playback_id}) is None
        # A real, existing id -- but from the *other* collection.
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
            "selected_ids": [str(hook_id), str(playback_id)],
        })
        assert r.status_code == 400
        assert r.get_json()["code"] == "unknown_ids"
        assert fake_db["caption_hooks"].find_one({"_id": hook_id}) is not None
        assert fake_db["playback_pool"].find_one({"_id": playback_id}) is not None

    def test_delete_selected_uses_single_bulk_operation(self, client, fake_db, monkeypatch):
        id1 = _hook(fake_db, "A")
        id2 = _hook(fake_db, "B")
        calls = []
        orig_delete_many = fake_db["caption_hooks"].delete_many
        def spy(query):
            calls.append(query)
            return orig_delete_many(query)
        monkeypatch.setattr(fake_db["caption_hooks"], "delete_many", spy)
        r = client.post("/api/admin/referral/share-content/bulk-action", json={
            "resource_type": "hook", "action": "delete_selected",
            "selected_ids": [str(id1), str(id2)],
        })
        assert r.status_code == 200
        assert len(calls) == 1  # one delete_many call, never a per-id loop


class TestIndividualDeleteSharesBulkService:
    def test_individual_hook_delete_uses_shared_service(self, client, fake_db, monkeypatch):
        hook_id = _hook(fake_db, "A")
        calls = []
        orig = rsc.delete_resource_ids
        def spy(resource_type, raw_ids, *, admin):
            calls.append((resource_type, raw_ids))
            return orig(resource_type, raw_ids, admin=admin)
        monkeypatch.setattr(rsc, "delete_resource_ids", spy)
        r = client.delete(f"/api/admin/referral/share-content/hooks/{hook_id}")
        assert r.status_code == 200
        assert calls == [("hook", [str(hook_id)])]

    def test_individual_playback_delete_uses_shared_service(self, client, fake_db, monkeypatch):
        pid = _playback(fake_db, "Playback01")
        calls = []
        orig = rsc.delete_resource_ids
        def spy(resource_type, raw_ids, *, admin):
            calls.append((resource_type, raw_ids))
            return orig(resource_type, raw_ids, admin=admin)
        monkeypatch.setattr(rsc, "delete_resource_ids", spy)
        r = client.delete(f"/api/admin/referral/share-content/playback/{pid}")
        assert r.status_code == 200
        assert calls == [("playback_link", [str(pid)])]

    def test_individual_delete_malformed_id_still_400(self, client, fake_db):
        r = client.delete("/api/admin/referral/share-content/hooks/not-an-id")
        assert r.status_code == 400
        assert r.get_json()["code"] == "invalid_id"

    def test_individual_delete_missing_id_still_404(self, client, fake_db):
        r = client.delete("/api/admin/referral/share-content/hooks/" + "0" * 24)
        assert r.status_code == 404
        assert r.get_json()["code"] == "not_found"

    def test_individual_activate_deactivate_still_work(self, client, fake_db):
        # Existing individual controls must keep working unmodified by this change.
        hook_id = _hook(fake_db, "A", status="active")
        r = client.post(f"/api/admin/referral/share-content/hooks/{hook_id}/deactivate")
        assert r.status_code == 200
        assert fake_db["caption_hooks"].find_one({"_id": hook_id})["status"] == "inactive"
        pid = _playback(fake_db, "Playback01", status="active")
        r = client.post(f"/api/admin/referral/share-content/playback/{pid}/deactivate")
        assert r.status_code == 200
        assert fake_db["playback_pool"].find_one({"_id": pid})["status"] == "inactive"


class TestBulkActionAuditLogging:
    def test_delete_selected_success_logged(self, client, fake_db, caplog):
        import logging
        id1 = _hook(fake_db, "A")
        with caplog.at_level(logging.INFO, logger="referral_share_content"):
            client.post("/api/admin/referral/share-content/bulk-action", json={
                "resource_type": "hook", "action": "delete_selected", "selected_ids": [str(id1)],
            })
        messages = [r.message for r in caplog.records]
        assert any("[SHARE_CONTENT][ADMIN_ACTION]" in m and "action=delete_selected" in m and "success=True" in m
                   for m in messages)

    def test_delete_selected_failure_logged_with_reason(self, client, fake_db, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="referral_share_content"):
            client.post("/api/admin/referral/share-content/bulk-action", json={
                "resource_type": "hook", "action": "delete_selected", "selected_ids": [],
            })
        messages = [r.message for r in caplog.records]
        assert any("success=False" in m and "reason=empty_selection" in m for m in messages)

    def test_deactivate_all_logged_with_counts(self, client, fake_db, caplog):
        import logging
        _hook(fake_db, "A", status="active")
        _hook(fake_db, "B", status="active")
        with caplog.at_level(logging.INFO, logger="referral_share_content"):
            client.post("/api/admin/referral/share-content/bulk-action",
                         json={"resource_type": "hook", "action": "deactivate_all"})
        messages = [r.message for r in caplog.records]
        assert any("action=deactivate_all" in m and "result_count=2" in m for m in messages)


class TestPoolEmptyAdminLogging:
    def _patch_invite_link(self, monkeypatch, link="https://t.me/+canonicalHash"):
        import types
        import sys
        fake_main = types.ModuleType("main")
        fake_main.get_or_create_referral_invite_link_sync = lambda user_id, username="": link
        monkeypatch.setitem(sys.modules, "main", fake_main)

    def test_generate_logs_hook_pool_empty(self, fake_db, monkeypatch, caplog):
        import logging
        self._patch_invite_link(monkeypatch)
        _playback(fake_db, "Playback01", status="active")
        with caplog.at_level(logging.WARNING, logger="referral_share_content"):
            result = rsc.generate_share_package(123, "user")
        assert result["ok"] is True
        assert result["hook_text"] is None
        messages = [r.message for r in caplog.records]
        assert any("[SHARE_CONTENT][POOL_EMPTY]" in m and "hook_pool_empty=True" in m and "playback_pool_empty=False" in m
                   for m in messages)

    def test_generate_logs_playback_pool_empty(self, fake_db, monkeypatch, caplog):
        import logging
        self._patch_invite_link(monkeypatch)
        _hook(fake_db, "A", status="active")
        with caplog.at_level(logging.WARNING, logger="referral_share_content"):
            result = rsc.generate_share_package(456, "user2")
        assert result["ok"] is True
        assert result["playback_url"] is None
        messages = [r.message for r in caplog.records]
        assert any("[SHARE_CONTENT][POOL_EMPTY]" in m and "playback_pool_empty=True" in m and "hook_pool_empty=False" in m
                   for m in messages)

    def test_generate_does_not_log_pool_empty_when_both_pools_populated(self, fake_db, monkeypatch, caplog):
        import logging
        self._patch_invite_link(monkeypatch)
        _hook(fake_db, "A", status="active")
        _playback(fake_db, "Playback01", status="active")
        with caplog.at_level(logging.WARNING, logger="referral_share_content"):
            result = rsc.generate_share_package(789, "user3")
        assert result["ok"] is True
        messages = [r.message for r in caplog.records]
        assert not any("[SHARE_CONTENT][POOL_EMPTY]" in m for m in messages)

    def test_generate_logs_both_pools_empty_independently_and_still_succeeds(self, fake_db, monkeypatch, caplog):
        # Explicit product decision (superseding an earlier draft requirement
        # to hard-fail generation when a pool is empty): referral sharing
        # must stay available even with *both* pools empty, because
        # disabling the whole Creator Centre has more business impact than
        # omitting one optional content component. Generation still
        # succeeds; the admin gets a WARNING identifying each empty pool
        # independently, not a single generic flag.
        import logging
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        with caplog.at_level(logging.WARNING, logger="referral_share_content"):
            result = rsc.generate_share_package(999, "user4")
        assert result["ok"] is True
        assert result["hook_text"] is None
        assert result["playback_url"] is None
        assert result["invite_link"] == "https://t.me/+abc123"
        messages = [r.message for r in caplog.records]
        assert any("[SHARE_CONTENT][POOL_EMPTY]" in m and "hook_pool_empty=True" in m and "playback_pool_empty=True" in m
                   for m in messages)

    def test_generate_message_is_well_formed_when_both_pools_empty(self, fake_db, monkeypatch):
        # The referral-share caption (built the same way for every surface)
        # must always be a clean, valid post -- never crash, never contain
        # literal "undefined"/"null" text, never a duplicated referral link,
        # and never a dangling blank-line placeholder where the omitted
        # hook/playback section used to be.
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        result = rsc.generate_share_package(1000, "user5")
        assert result["ok"] is True
        message = result["message"]
        assert "undefined" not in message.lower()
        assert "null" not in message.lower()
        assert message.count("https://t.me/+abc123") == 1
        assert "\n\n\n" not in message  # no orphan double-blank-line gap
        assert not message.startswith("\n")
        assert message.strip() == message
        assert message.endswith("https://t.me/+abc123")

    def test_creator_share_text_is_well_formed_when_both_pools_empty(self, fake_db, monkeypatch):
        # Same well-formedness guarantee for the Creator Share Centre's
        # copy-ready text (build_creator_share_text), which independently
        # omits hook/playback exactly like build_referral_share_caption.
        share_text = rsc.build_creator_share_text(
            hook_text=None, playback_url=None, referral_link="https://t.me/+abc123",
        )
        assert "undefined" not in share_text.lower()
        assert "null" not in share_text.lower()
        assert share_text.count("https://t.me/+abc123") == 1
        assert "\n\n\n" not in share_text
        assert share_text.strip() == share_text
        assert share_text.endswith("https://t.me/+abc123")


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

    def test_pool_free_generation_does_not_reset_last_playback(self, fake_db):
        # A Creator Share Centre generation picks `first`, then the same
        # user generates a Mini App caption (include_content_pools=False,
        # so its share_generations doc has playback_record_id=None and is
        # newer). The next Creator Centre generation must still avoid
        # repeating `first` -- the pool-free doc must not look like "no
        # last playback".
        first = _playback(fake_db, "First0002")
        second = _playback(fake_db, "Second002")
        fake_db["share_generations"].insert_one({
            "user_id": 43, "playback_record_id": first, "generated_at": rsc.now_utc(),
        })
        fake_db["share_generations"].insert_one({
            "user_id": 43, "playback_record_id": None, "generated_at": rsc.now_utc(),
        })
        doc = rsc.select_playback_for_user(user_id=43)
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
            "Want more replays like this—and rewards too?\n\n"
            "👋 Welcome to AdvantPlay Community!\n\n"
            "Join our channel to get 👇\n\n"
            "🎟️ FREE Welcome Voucher — No deposit required\n"
            "⚡️ Daily voucher drops\n"
            "🎁 Bonus campaigns\n"
            "👑 VIP-only announcements\n"
            "🏆 Weekly ranking rewards\n\n"
            "Start here 👇\n"
            "https://t.me/+abc123"
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

    def test_no_active_hook_is_omitted_not_substituted(self, fake_db, monkeypatch):
        """No active hook -> hook section is omitted entirely; the
        admin-configurable fallback_hook_text is NOT silently substituted
        (that was the old behavior and violated the empty-state contract)."""
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _playback(fake_db, "Play00003")

        result = rsc.generate_share_package(2, "user2")
        assert result["ok"] is True
        assert result["hook_text"] is None
        assert rsc.DEFAULT_FALLBACK_HOOK_TEXT not in result["message"]
        assert result["message"].startswith("https://rx.apreplay.com/Play00003")
        row = fake_db["share_generations"].find_one({"user_id": 2})
        assert row["hook_id"] is None
        assert row["hook_text"] is None

    def test_no_active_playback_still_succeeds_with_playback_omitted(self, fake_db, monkeypatch):
        """No active playback link is no longer a hard failure -- the
        playback section is simply omitted and the referral link is still
        generated, tracked, and returned normally."""
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _hook(fake_db, "Hook text")
        # no playback records at all

        result = rsc.generate_share_package(3, "user3")
        assert result["ok"] is True
        assert result["playback_url"] is None
        assert result["invite_link"] == "https://t.me/+abc123"
        assert "https://rx.apreplay.com" not in result["message"]
        row = fake_db["share_generations"].find_one({"user_id": 3})
        assert row is not None
        assert row["playback_record_id"] is None
        assert row["playback_id"] is None
        assert row["playback_url"] is None
        assert row["invite_link"] == "https://t.me/+abc123"

    def test_all_records_inactive_returns_static_benefits_and_link_only(self, fake_db, monkeypatch):
        """No active hook AND no active playback: still a valid, non-empty
        caption -- static benefits section + the user's referral link,
        never an error, never 'None', never an orphan separator."""
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _hook(fake_db, "Inactive hook", status="inactive")
        _playback(fake_db, "InactivePB", status="inactive")

        result = rsc.generate_share_package(6, "user6")
        assert result["ok"] is True
        assert result["hook_text"] is None
        assert result["playback_url"] is None
        assert result["invite_link"] == "https://t.me/+abc123"
        expected = (
            "👋 Welcome to AdvantPlay Community!\n\n"
            "Join our channel to get 👇\n\n"
            "🎟️ FREE Welcome Voucher — No deposit required\n"
            "⚡️ Daily voucher drops\n"
            "🎁 Bonus campaigns\n"
            "👑 VIP-only announcements\n"
            "🏆 Weekly ranking rewards\n\n"
            "Start here 👇\n"
            "https://t.me/+abc123"
        )
        assert result["message"] == expected
        assert "None" not in result["message"]
        assert "\n\n\n" not in result["message"]
        assert not result["message"].startswith("\n")

    def test_no_active_hook_ignores_configured_fallback_hook_text(self, fake_db, monkeypatch):
        """Even when share_content.fallback_hook_text IS configured, no
        active hook must still omit the hook section -- the setting is not
        the empty-state protection (build_referral_share_caption's own
        omit-when-blank logic is), so it must never leak into the caption
        just because an admin configured it."""
        import settings_service

        monkeypatch.setattr(
            settings_service, "get_setting",
            lambda category, key: "🎬 Configured hook copy!" if (category, key) == ("share_content", "fallback_hook_text") else None,
        )
        self._patch_invite_link(monkeypatch, link="https://t.me/+abc123")
        _playback(fake_db, "Play00006")

        result = rsc.generate_share_package(7, "user7")
        assert result["ok"] is True
        assert result["hook_text"] is None
        assert "Configured hook copy" not in result["message"]

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


# ---------------------------------------------------------------------------
# _default_hook_text() / share_content.fallback_hook_text — documents the
# setting's current (read-only) behavior now that generate_share_package no
# longer calls it to substitute a missing hook. Kept as an accessor only;
# see the docstring on _default_hook_text for the audit rationale.
# ---------------------------------------------------------------------------

class TestDefaultHookTextSetting:
    def test_empty_fallback_hook_text_uses_hardcoded_default(self, monkeypatch):
        import settings_service

        monkeypatch.setattr(settings_service, "get_setting", lambda category, key: "")
        assert rsc._default_hook_text() == rsc.DEFAULT_FALLBACK_HOOK_TEXT

    def test_configured_fallback_hook_text_is_returned_verbatim(self, monkeypatch):
        import settings_service

        monkeypatch.setattr(
            settings_service, "get_setting",
            lambda category, key: "🎬 Custom copy" if (category, key) == ("share_content", "fallback_hook_text") else None,
        )
        assert rsc._default_hook_text() == "🎬 Custom copy"

    def test_settings_lookup_failure_falls_back_to_hardcoded_default(self, monkeypatch):
        import settings_service

        def _boom(category, key):
            raise RuntimeError("db unavailable")

        monkeypatch.setattr(settings_service, "get_setting", _boom)
        assert rsc._default_hook_text() == rsc.DEFAULT_FALLBACK_HOOK_TEXT


# ---------------------------------------------------------------------------
# build_referral_share_caption — shared template used by every active
# caption-generation surface (bot reply, share-button prefill, Mini App).
# ---------------------------------------------------------------------------

EXPECTED_BENEFITS_BLOCK = (
    "Want more replays like this—and rewards too?\n\n"
    "👋 Welcome to AdvantPlay Community!\n\n"
    "Join our channel to get 👇\n\n"
    "🎟️ FREE Welcome Voucher — No deposit required\n"
    "⚡️ Daily voucher drops\n"
    "🎁 Bonus campaigns\n"
    "👑 VIP-only announcements\n"
    "🏆 Weekly ranking rewards\n\n"
    "Start here 👇"
)


class TestBuildReferralShareCaption:
    def test_normal_hook_playback_and_referral_url(self):
        result = rsc.build_referral_share_caption(
            hook_text="Walao, wait for the ending 👀",
            playback_url="https://rx.apreplay.com/87FWMgJ5kL",
            referral_url="https://t.me/+y7BPw5Sv7KJhODc1",
        )
        expected = (
            "Walao, wait for the ending 👀\n"
            "https://rx.apreplay.com/87FWMgJ5kL\n\n"
            f"{EXPECTED_BENEFITS_BLOCK}\n"
            "https://t.me/+y7BPw5Sv7KJhODc1"
        )
        assert result == expected

    def test_exact_line_breaks_and_ordering(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
        )
        lines = result.split("\n")
        assert lines == [
            "Hook",
            "https://rx.apreplay.com/Abc12345",
            "",
            "Want more replays like this—and rewards too?",
            "",
            "👋 Welcome to AdvantPlay Community!",
            "",
            "Join our channel to get 👇",
            "",
            "🎟️ FREE Welcome Voucher — No deposit required",
            "⚡️ Daily voucher drops",
            "🎁 Bonus campaigns",
            "👑 VIP-only announcements",
            "🏆 Weekly ranking rewards",
            "",
            "Start here 👇",
            "https://t.me/+ref",
        ]

    def test_referral_url_is_the_final_line(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook", playback_url="https://rx.apreplay.com/Abc12345", referral_url="https://t.me/+ref"
        )
        assert result.split("\n")[-1] == "https://t.me/+ref"

    def test_trims_surrounding_whitespace_from_dynamic_values(self):
        result = rsc.build_referral_share_caption(
            hook_text="  Hook  \n",
            playback_url="  https://rx.apreplay.com/Abc12345  ",
            referral_url="  https://t.me/+ref  ",
        )
        assert result.startswith("Hook\nhttps://rx.apreplay.com/Abc12345\n\n")
        assert result.endswith("https://t.me/+ref")
        assert "  " not in result.split("\n")[0]

    def test_missing_hook_is_omitted_not_substituted(self):
        result = rsc.build_referral_share_caption(
            hook_text="", playback_url="https://rx.apreplay.com/Abc12345", referral_url="https://t.me/+ref"
        )
        assert result.startswith("https://rx.apreplay.com/Abc12345\n\n")
        assert "Wait for the ending" not in result

        result_none = rsc.build_referral_share_caption(
            hook_text=None, playback_url="https://rx.apreplay.com/Abc12345", referral_url="https://t.me/+ref"
        )
        assert result_none.startswith("https://rx.apreplay.com/Abc12345\n\n")
        assert "Wait for the ending" not in result_none

    def test_missing_playback_produces_referral_only_caption_no_blank_line(self, caplog):
        result = rsc.build_referral_share_caption(hook_text="Hook", playback_url="", referral_url="https://t.me/+ref")
        expected = f"Hook\n\n{EXPECTED_BENEFITS_BLOCK}\nhttps://t.me/+ref"
        assert result == expected
        assert "https://rx.apreplay.com" not in result
        # No blank/malformed URL line: exactly one blank line separates the
        # hook from the benefits block, not two.
        assert "\n\n\n" not in result

    def test_no_hook_and_no_playback_returns_static_benefits_and_link_only(self):
        """The core empty-state fix: no active hook AND no active playback
        link must still produce a valid, non-empty caption -- the static
        benefits section plus the referral link, with no orphan separators,
        no leading blank line, and no placeholder/"None" text."""
        result = rsc.build_referral_share_caption(hook_text=None, playback_url=None, referral_url="https://t.me/+ref")
        expected = (
            "👋 Welcome to AdvantPlay Community!\n\n"
            "Join our channel to get 👇\n\n"
            "🎟️ FREE Welcome Voucher — No deposit required\n"
            "⚡️ Daily voucher drops\n"
            "🎁 Bonus campaigns\n"
            "👑 VIP-only announcements\n"
            "🏆 Weekly ranking rewards\n\n"
            "Start here 👇\n"
            "https://t.me/+ref"
        )
        assert result == expected
        assert not result.startswith("\n")
        assert not result.startswith("=")
        assert "\n\n\n" not in result
        assert "None" not in result
        assert result.strip() != ""

    def test_no_hook_and_no_playback_in_html_mode_is_still_valid_markup(self):
        result = rsc.build_referral_share_caption(
            hook_text="", playback_url="", referral_url="https://t.me/+ref", format_mode="telegram_html",
        )
        assert result.count("<blockquote>") == 1
        assert result.count("</blockquote>") == 1
        assert not result.startswith("\n")
        assert "\n\n\n" not in result
        assert "None" not in result
        assert result.strip() != ""

    def test_missing_referral_url_raises(self):
        with pytest.raises(ValueError):
            rsc.build_referral_share_caption(hook_text="Hook", playback_url="https://rx.apreplay.com/Abc12345", referral_url="")

        with pytest.raises(ValueError):
            rsc.build_referral_share_caption(hook_text="Hook", playback_url="https://rx.apreplay.com/Abc12345", referral_url=None)

    def test_unicode_emoji_and_punctuation_preserved(self):
        result = rsc.build_referral_share_caption(
            hook_text="It's 🔥 today's biggest win!",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
        )
        assert "It's 🔥 today's biggest win!" in result
        assert "⚡️" in result and "🎁" in result and "🏆" in result and "👑" in result
        assert "👇" in result

    def test_output_never_contains_none_or_undefined(self):
        result = rsc.build_referral_share_caption(hook_text=None, playback_url=None, referral_url="https://t.me/+ref")
        assert "None" not in result
        assert "undefined" not in result

    def test_no_duplicate_referral_url_when_link_excluded(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
            include_referral_link=False,
        )
        assert result.count("https://t.me/+ref") == 0
        assert result.endswith("Start here 👇")

    def test_invalid_format_mode_raises(self):
        with pytest.raises(ValueError):
            rsc.build_referral_share_caption(
                hook_text="Hook",
                playback_url="https://rx.apreplay.com/Abc12345",
                referral_url="https://t.me/+ref",
                format_mode="bogus",
            )

    def test_pool_selection_still_returns_only_active_entries(self, fake_db):
        _hook(fake_db, "Active hook", status="active")
        _hook(fake_db, "Inactive hook", status="inactive")
        for _ in range(20):
            picked = rsc.select_hook()
            assert picked["text"] == "Active hook"

        _playback(fake_db, "ActivePB01", status="active")
        _playback(fake_db, "InactivePB", status="inactive")
        for i in range(20):
            picked = rsc.select_playback_for_user(user_id=9000 + i)
            assert picked["playback_id"] == "ActivePB01"


EXPECTED_HTML_BLOCKQUOTE = (
    "<blockquote><b>👋 Welcome to AdvantPlay Community!</b>\n"
    "Join our channel to get 👇\n\n"
    "🎟️ FREE Welcome Voucher — No deposit required\n"
    "⚡️ Daily voucher drops\n"
    "🎁 Bonus campaigns\n"
    "👑 VIP-only announcements\n"
    "🏆 Weekly ranking rewards</blockquote>"
)


class TestBuildReferralShareCaptionTelegramHtml:
    def test_exact_html_payload(self):
        result = rsc.build_referral_share_caption(
            hook_text="Walao, wait for the ending 👀",
            playback_url="https://rx.apreplay.com/87FWMgJ5kL",
            referral_url="https://t.me/+y7BPw5Sv7KJhODc1",
            format_mode="telegram_html",
        )
        expected = (
            "Walao, wait for the ending 👀\n"
            "https://rx.apreplay.com/87FWMgJ5kL\n\n"
            "Want more replays like this—and rewards too?\n\n"
            f"{EXPECTED_HTML_BLOCKQUOTE}\n\n"
            "Start here 👇\n"
            "https://t.me/+y7BPw5Sv7KJhODc1"
        )
        assert result == expected

    def test_contains_exactly_one_blockquote(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
            format_mode="telegram_html",
        )
        assert result.count("<blockquote>") == 1
        assert result.count("</blockquote>") == 1

    def test_benefits_are_inside_blockquote(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
            format_mode="telegram_html",
        )
        start = result.index("<blockquote>")
        end = result.index("</blockquote>") + len("</blockquote>")
        quoted = result[start:end]
        assert "👋 Welcome to AdvantPlay Community!" in quoted
        assert "Join our channel to get 👇" in quoted
        assert "🎟️ FREE Welcome Voucher — No deposit required" in quoted
        assert "⚡️ Daily voucher drops" in quoted
        assert "🎁 Bonus campaigns" in quoted
        assert "🏆 Weekly ranking rewards" in quoted
        assert "👑 VIP-only announcements" in quoted
        assert quoted.index("FREE Welcome Voucher") < quoted.index("Daily voucher drops")

    def test_hook_and_playback_are_outside_blockquote(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
            format_mode="telegram_html",
        )
        start = result.index("<blockquote>")
        end = result.index("</blockquote>") + len("</blockquote>")
        assert "Hook" in result[:start]
        assert "https://rx.apreplay.com/Abc12345" in result[:start]
        assert "Hook" not in result[start:end]
        assert "https://rx.apreplay.com/Abc12345" not in result[start:end]

    def test_referral_cta_and_url_are_outside_blockquote(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
            format_mode="telegram_html",
        )
        start = result.index("<blockquote>")
        end = result.index("</blockquote>") + len("</blockquote>")
        assert "Start here 👇" in result[end:]
        assert "https://t.me/+ref" in result[end:]
        assert "Start here" not in result[start:end]
        assert "https://t.me/+ref" not in result[start:end]

    def test_dynamic_values_are_html_escaped(self):
        result = rsc.build_referral_share_caption(
            hook_text='<script>alert("x")</script> & win!',
            playback_url="https://rx.apreplay.com/Abc12345?a=1&b=2",
            referral_url="https://t.me/+ref?x=1&y=2",
            format_mode="telegram_html",
        )
        assert "<script>" not in result
        assert "&lt;script&gt;" in result
        assert "&amp;" in result
        # Static tags remain unescaped.
        assert "<blockquote><b>👋 Welcome to AdvantPlay Community!</b>" in result
        assert result.count("<blockquote>") == 1

    def test_missing_hook_is_omitted_in_html_mode(self):
        result = rsc.build_referral_share_caption(
            hook_text="", playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref", format_mode="telegram_html",
        )
        assert result.startswith("https://rx.apreplay.com/Abc12345\n\n")
        assert "Wait for the ending" not in result

    def test_missing_playback_omits_line_but_stays_valid_html(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook", playback_url="", referral_url="https://t.me/+ref",
            format_mode="telegram_html",
        )
        assert "https://rx.apreplay.com" not in result
        assert result.count("<blockquote>") == 1
        assert "\n\n\n" not in result

    def test_missing_referral_url_raises_in_html_mode(self):
        with pytest.raises(ValueError):
            rsc.build_referral_share_caption(
                hook_text="Hook",
                playback_url="https://rx.apreplay.com/Abc12345",
                referral_url="",
                format_mode="telegram_html",
            )

    def test_plain_mode_default_produces_no_html_tags(self):
        result = rsc.build_referral_share_caption(
            hook_text="Hook",
            playback_url="https://rx.apreplay.com/Abc12345",
            referral_url="https://t.me/+ref",
        )
        assert "<blockquote>" not in result
        assert "<b>" not in result
