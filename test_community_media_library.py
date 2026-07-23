"""Tests for community_media_library.py: admin media capture -> Community
Centre Media Library (extraction, dedup, resolution, CRUD API), plus the
composer-side integration hooks in community_centre.py (media_library_id
priority/validation, publish-time resolution, usage_count bookkeeping).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from flask import Flask
from pymongo.errors import DuplicateKeyError as RealDuplicateKeyError

import database
import community_centre as cc
import community_media_library as cml
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "community_media_library": [("file_unique_id", "media_type")],
        "community_destinations": [("key",)],
        "community_post_runs": [("community_post_id", "run_key")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr("fake_mongo.DuplicateKeyError", RealDuplicateKeyError, raising=False)
    monkeypatch.setattr(cml, "DuplicateKeyError", RealDuplicateKeyError, raising=False)
    monkeypatch.setattr(cc.settings_service, "get_settings", lambda group, **kw: {
        "official_channel_id": "", "official_channel_username": "", "main_group_id": "0", "community_chat_id": "0",
    })
    return fdb


# ---------------------------------------------------------------------------
# Fake Telegram message objects
# ---------------------------------------------------------------------------

def _photo_message(sizes=None, chat_id=111, message_id=1, caption=None):
    sizes = sizes or [
        SimpleNamespace(file_id="small_fid", file_unique_id="uniq_small", file_size=1000, width=90, height=90),
        SimpleNamespace(file_id="large_fid", file_unique_id="uniq_large", file_size=50000, width=1080, height=1080),
    ]
    return SimpleNamespace(
        photo=sizes, animation=None, video=None, document=None,
        chat_id=chat_id, message_id=message_id, caption=caption,
    )


def _animation_message(file_id="anim_fid", file_unique_id="anim_uniq", mime_type="video/mp4", file_size=1000):
    animation = SimpleNamespace(
        file_id=file_id, file_unique_id=file_unique_id, file_name="clip.mp4",
        mime_type=mime_type, file_size=file_size, width=320, height=240, duration=5,
    )
    return SimpleNamespace(photo=[], animation=animation, video=None, document=None,
                            chat_id=1, message_id=1, caption=None)


def _video_message(file_id="vid_fid", file_unique_id="vid_uniq", file_size=1000):
    video = SimpleNamespace(
        file_id=file_id, file_unique_id=file_unique_id, file_name="clip.mp4",
        mime_type="video/mp4", file_size=file_size, width=640, height=480, duration=30,
    )
    return SimpleNamespace(photo=[], animation=None, video=video, document=None,
                            chat_id=1, message_id=1, caption=None)


def _document_message(mime_type="image/png", file_size=1000, file_id="doc_fid", file_unique_id="doc_uniq"):
    document = SimpleNamespace(
        file_id=file_id, file_unique_id=file_unique_id, file_name="banner.png",
        mime_type=mime_type, file_size=file_size,
    )
    return SimpleNamespace(photo=[], animation=None, video=None, document=document,
                            chat_id=1, message_id=1, caption=None)


def _text_message():
    return SimpleNamespace(photo=[], animation=None, video=None, document=None,
                            chat_id=1, message_id=1, caption=None)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def test_extract_photo_uses_largest_photosize():
    extracted, err = cml.extract_media_from_message(_photo_message())
    assert err is None
    assert extracted["media_type"] == "photo"
    assert extracted["file_id"] == "large_fid"
    assert extracted["file_unique_id"] == "uniq_large"
    assert extracted["width"] == 1080


def test_extract_animation():
    extracted, err = cml.extract_media_from_message(_animation_message())
    assert err is None
    assert extracted["media_type"] == "animation"
    assert extracted["file_id"] == "anim_fid"
    assert extracted["duration"] == 5


def test_extract_video():
    extracted, err = cml.extract_media_from_message(_video_message())
    assert err is None
    assert extracted["media_type"] == "video"
    assert extracted["file_id"] == "vid_fid"


def test_extract_document_supported_mime():
    extracted, err = cml.extract_media_from_message(_document_message(mime_type="image/png"))
    assert err is None
    assert extracted["media_type"] == "document"
    assert extracted["mime_type"] == "image/png"


def test_extract_document_invalid_mime_rejected():
    extracted, err = cml.extract_media_from_message(_document_message(mime_type="application/pdf"))
    assert extracted is None
    assert err == "unsupported_mime"


def test_extract_no_supported_media_type():
    extracted, err = cml.extract_media_from_message(_text_message())
    assert extracted is None
    assert err == "unsupported_media_type"


def test_extract_oversized_media_rejected():
    extracted, err = cml.extract_media_from_message(_video_message(file_size=200 * 1024 * 1024))
    assert extracted is None
    assert err == "too_large"


# ---------------------------------------------------------------------------
# Dedup / save_media
# ---------------------------------------------------------------------------

def test_save_media_creates_new_record(fake_db):
    extracted, _ = cml.extract_media_from_message(_photo_message())
    doc, created = cml.save_media(extracted, uploaded_by=42, source_chat_id=42, source_message_id=7, caption="hi")
    assert created is True
    assert doc["file_id"] == "large_fid"
    assert doc["file_unique_id"] == "uniq_large"
    assert doc["status"] == "active"
    assert doc["usage_count"] == 0
    assert doc["reupload_count"] == 0
    assert doc["uploaded_by"] == 42


def test_save_media_dedup_by_file_unique_id_and_type_refreshes_file_id(fake_db):
    extracted, _ = cml.extract_media_from_message(_video_message(file_id="fid_v1", file_unique_id="stable_uniq"))
    first, created1 = cml.save_media(extracted, uploaded_by=1, source_chat_id=1, source_message_id=1)
    assert created1 is True

    extracted2, _ = cml.extract_media_from_message(_video_message(file_id="fid_v2", file_unique_id="stable_uniq"))
    second, created2 = cml.save_media(extracted2, uploaded_by=1, source_chat_id=1, source_message_id=2)

    assert created2 is False
    assert second["_id"] == first["_id"]
    assert second["file_id"] == "fid_v2"
    assert second["reupload_count"] == 1
    # original record preserved (uploaded_by/uploaded_at/internal_name unchanged)
    assert second["uploaded_by"] == first["uploaded_by"]
    assert second["internal_name"] == first["internal_name"]

    assert cml._media().count_documents({}) == 1


def test_save_media_different_media_type_same_unique_id_is_separate_row(fake_db):
    """file_unique_id + media_type together form the dedup key."""
    doc_a, created_a = cml.save_media(
        {"file_id": "a", "file_unique_id": "shared", "media_type": "video", "filename": None,
         "mime_type": "video/mp4", "file_size": 1, "width": 1, "height": 1, "duration": 1},
        uploaded_by=1, source_chat_id=1, source_message_id=1,
    )
    doc_b, created_b = cml.save_media(
        {"file_id": "b", "file_unique_id": "shared", "media_type": "animation", "filename": None,
         "mime_type": "video/mp4", "file_size": 1, "width": 1, "height": 1, "duration": 1},
        uploaded_by=1, source_chat_id=1, source_message_id=2,
    )
    assert created_a and created_b
    assert doc_a["_id"] != doc_b["_id"]
    assert cml._media().count_documents({}) == 2


# ---------------------------------------------------------------------------
# Resolution / usage
# ---------------------------------------------------------------------------

def _seed_photo(fake_db, **overrides):
    extracted, _ = cml.extract_media_from_message(_photo_message())
    doc, _ = cml.save_media(extracted, uploaded_by=1, source_chat_id=1, source_message_id=1)
    if overrides:
        cml._media().update_one({"_id": doc["_id"]}, {"$set": overrides})
        doc = cml.get_media(doc["_id"])
    return doc


def test_resolve_for_publish_active_media(fake_db):
    doc = _seed_photo(fake_db)
    resolved, err = cml.resolve_for_publish(str(doc["_id"]), expected_media_type="photo")
    assert err is None
    assert resolved["file_id"] == doc["file_id"]


def test_resolve_for_publish_rejects_archived(fake_db):
    doc = _seed_photo(fake_db, status="archived")
    resolved, err = cml.resolve_for_publish(str(doc["_id"]), expected_media_type="photo")
    assert resolved is None
    assert err == "media_not_active"


def test_resolve_for_publish_rejects_type_mismatch(fake_db):
    doc = _seed_photo(fake_db)
    resolved, err = cml.resolve_for_publish(str(doc["_id"]), expected_media_type="video")
    assert resolved is None
    assert err == "media_type_mismatch"


def test_resolve_for_publish_not_found(fake_db):
    resolved, err = cml.resolve_for_publish("000000000000000000000000")
    assert resolved is None
    assert err == "media_not_found"


def test_increment_usage(fake_db):
    doc = _seed_photo(fake_db)
    cml.increment_usage(str(doc["_id"]))
    refreshed = cml.get_media(doc["_id"])
    assert refreshed["usage_count"] == 1
    assert refreshed["last_used_at_utc"] is not None


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def test_update_media_rename(fake_db):
    doc = _seed_photo(fake_db)
    updated, err = cml.update_media(str(doc["_id"]), {"internal_name": "Weekend Banner"}, actor_id=1)
    assert err is None
    assert updated["internal_name"] == "Weekend Banner"


def test_update_media_rejects_empty_name(fake_db):
    doc = _seed_photo(fake_db)
    updated, err = cml.update_media(str(doc["_id"]), {"internal_name": "   "}, actor_id=1)
    assert updated is None
    assert err == "missing_internal_name"


def test_archive_then_restore(fake_db):
    doc = _seed_photo(fake_db)
    archived, err = cml.archive_media(str(doc["_id"]), actor_id=1)
    assert err is None
    assert archived["status"] == "archived"
    restored, err = cml.restore_media(str(doc["_id"]), actor_id=1)
    assert err is None
    assert restored["status"] == "active"


def test_archived_media_cannot_be_selected_by_composer(fake_db):
    _make_destination()
    doc = _seed_photo(fake_db, status="archived")
    payload = _photo_payload(media_library_id=str(doc["_id"]))
    post, err = cc.create_post(payload, actor_id=1)
    assert post is None
    assert err == "media_not_active"


def test_delete_media_blocked_when_referenced_by_draft(fake_db):
    _make_destination()
    doc = _seed_photo(fake_db)
    payload = _photo_payload(media_library_id=str(doc["_id"]))
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None

    ok, code = cml.delete_media(str(doc["_id"]), actor_id=1)
    assert ok is False
    assert code == "media_in_use"


def test_delete_media_allowed_when_unreferenced(fake_db):
    doc = _seed_photo(fake_db)
    ok, code = cml.delete_media(str(doc["_id"]), actor_id=1)
    assert ok is True
    assert code is None
    assert cml.get_media(doc["_id"]) is None


# ---------------------------------------------------------------------------
# Composer integration: media_library_id priority + validation
# ---------------------------------------------------------------------------

def _make_destination(key="official_channel", **overrides):
    payload = {
        "key": key, "name": "Official Channel", "chat_id": -1001234567890,
        "chat_type": "channel", "enabled": True, "allow_posts": True,
        "allow_polls": True, "allow_pin": True,
    }
    payload.update(overrides)
    dest, err = cc.upsert_destination(payload, actor_id=1)
    assert err is None, err
    return dest


def _photo_payload(*, media_library_id=None, source_url=None, telegram_file_id=None, **overrides):
    media_item = {"type": "photo"}
    if media_library_id:
        media_item["media_library_id"] = media_library_id
    if source_url:
        media_item["source_url"] = source_url
    if telegram_file_id:
        media_item["telegram_file_id"] = telegram_file_id
    payload = {
        "title": "Weekend Banner", "content_type": "photo", "destination_key": "official_channel",
        "text": "", "parse_mode": "HTML", "media": [media_item],
    }
    payload.update(overrides)
    return payload


def test_composer_resolves_library_media_and_ignores_client_file_id(fake_db):
    _make_destination()
    doc = _seed_photo(fake_db)
    payload = _photo_payload(media_library_id=str(doc["_id"]), telegram_file_id="attacker_supplied_fid")
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None
    assert post["media"][0]["telegram_file_id"] == doc["file_id"]
    assert post["media"][0]["telegram_file_id"] != "attacker_supplied_fid"
    assert post["media"][0]["media_library_id"] == str(doc["_id"])


def test_composer_rejects_ambiguous_source_url_and_file_id(fake_db):
    _make_destination()
    payload = _photo_payload(source_url="https://example.com/a.jpg", telegram_file_id="raw_fid")
    post, err = cc.create_post(payload, actor_id=1)
    assert post is None
    assert err == "ambiguous_media_source"


def test_composer_media_type_mismatch_rejected(fake_db):
    _make_destination()
    extracted, _ = cml.extract_media_from_message(_video_message())
    doc, _ = cml.save_media(extracted, uploaded_by=1, source_chat_id=1, source_message_id=1)
    payload = _photo_payload(media_library_id=str(doc["_id"]))  # composer content_type=photo, media is video
    post, err = cc.create_post(payload, actor_id=1)
    assert post is None
    assert err == "media_type_mismatch"


def _sync_run_coro(coro, timeout=20):
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_successful_publish_increments_usage_count(fake_db, monkeypatch):
    _make_destination()
    doc = _seed_photo(fake_db)
    payload = _photo_payload(media_library_id=str(doc["_id"]))
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None

    async def fake_send(_post, _step=None):
        return {"message_ids": [123], "poll_id": None, "poll_message_id": None}

    monkeypatch.setattr(cc, "_do_send", fake_send)
    monkeypatch.setattr(cc, "_run_coro", _sync_run_coro)

    cc._execute_publish({**post, "next_run_at_utc": cc.now_utc()})

    refreshed = cml.get_media(doc["_id"])
    assert refreshed["usage_count"] == 1


def test_failed_publish_does_not_increment_usage_count(fake_db, monkeypatch):
    _make_destination()
    doc = _seed_photo(fake_db)
    payload = _photo_payload(media_library_id=str(doc["_id"]))
    post, err = cc.create_post(payload, actor_id=1)
    assert err is None

    async def fake_send(_post, _step=None):
        raise RuntimeError("network_timeout")

    monkeypatch.setattr(cc, "_do_send", fake_send)
    monkeypatch.setattr(cc, "_run_coro", _sync_run_coro)

    cc._execute_publish({**post, "next_run_at_utc": cc.now_utc()})

    refreshed = cml.get_media(doc["_id"])
    assert refreshed["usage_count"] == 0


# ---------------------------------------------------------------------------
# Admin API auth
# ---------------------------------------------------------------------------

def _app():
    app = Flask(__name__)
    app.register_blueprint(cml.community_media_bp)
    return app


def test_media_api_requires_admin(fake_db, monkeypatch):
    from flask import jsonify as flask_jsonify
    monkeypatch.setattr(cml, "_require_admin", lambda: (None, (flask_jsonify({"success": False, "code": "auth_failed"}), 401)))
    client = _app().test_client()
    assert client.get("/api/admin/community/media").status_code == 401
    assert client.patch("/api/admin/community/media/000000000000000000000000", json={}).status_code == 401
    assert client.post("/api/admin/community/media/000000000000000000000000/archive").status_code == 401


def test_media_api_list_and_get(fake_db, monkeypatch):
    monkeypatch.setattr(cml, "_require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    doc = _seed_photo(fake_db)
    client = _app().test_client()

    resp = client.get("/api/admin/community/media")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert len(body["media"]) == 1

    resp = client.get(f"/api/admin/community/media/{doc['_id']}")
    assert resp.status_code == 200
    assert resp.get_json()["media"]["file_id"] == doc["file_id"]


def test_media_api_archive_hides_from_default_active_listing(fake_db, monkeypatch):
    monkeypatch.setattr(cml, "_require_admin", lambda: ({"id": 1, "usernameLower": "admin"}, None))
    doc = _seed_photo(fake_db)
    client = _app().test_client()
    resp = client.post(f"/api/admin/community/media/{doc['_id']}/archive")
    assert resp.status_code == 200
    resp = client.get("/api/admin/community/media")
    assert resp.get_json()["media"] == []
    resp = client.get("/api/admin/community/media?status=all")
    assert len(resp.get_json()["media"]) == 1
