"""Tests for creator_share_centre.py generation / copy / share-click / results
APIs: reuse of generate_share_package(), canonical invite link, package
ownership, rate limiting, and share_generations backward compatibility."""

from __future__ import annotations

import sys
import types

import pytest
from flask import Flask

import creator_share_centre as csc
import database
import referral_share_content as rsc
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    csc.invalidate_creator_group_settings_cache()
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _fake_vouchers_module(monkeypatch, *, user_id=555, username="creator1"):
    fake_vouchers = types.ModuleType("vouchers")

    def _extract_raw_init_data_from_query(request):
        return "init_data=ok"

    def _verify_telegram_init_data(init_data):
        return True, {"user": {"id": user_id, "username": username}}, None

    fake_vouchers.extract_raw_init_data_from_query = _extract_raw_init_data_from_query
    fake_vouchers.verify_telegram_init_data = _verify_telegram_init_data
    fake_vouchers.require_admin = lambda: ({"id": 1, "usernameLower": "admin"}, None)
    monkeypatch.setitem(sys.modules, "vouchers", fake_vouchers)


def _patch_invite_link(monkeypatch, link="https://t.me/+canonicalHash", raise_error=None, calls=None):
    fake_main = types.ModuleType("main")

    def _fake_get_or_create(user_id, username=""):
        if calls is not None:
            calls.append((user_id, username))
        if raise_error is not None:
            raise raise_error
        return link

    fake_main.get_or_create_referral_invite_link_sync = _fake_get_or_create
    monkeypatch.setitem(sys.modules, "main", fake_main)


@pytest.fixture
def app(fake_db):
    flask_app = Flask(__name__)
    flask_app.register_blueprint(csc.creator_share_bp)
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def _creator(fake_db, user_id=555, status="active"):
    now = csc.now_utc()
    fake_db["creator_members"].insert_one(
        {
            "user_id": user_id,
            "status": status,
            "source_group_id": None,
            "creator_tier": "pilot",
            "approved_at": now,
            "approved_by": 1,
            "last_membership_verified_at": None,
            "created_at": now,
            "updated_at": now,
        }
    )


def _hook(fake_db, text="🔥 Big wins today!", status="active"):
    now = rsc.now_utc()
    fake_db["caption_hooks"].insert_one(
        {"text": text, "status": status, "times_selected": 0, "last_selected_at": None, "created_at": now, "updated_at": now, "created_by": None}
    )


def _playback(fake_db, playback_id="Play00001", status="active"):
    now = rsc.now_utc()
    fake_db["playback_pool"].insert_one(
        {
            "playback_id": playback_id,
            "playback_url": rsc.canonical_playback_url(playback_id),
            "game_name": "",
            "status": status,
            "times_selected": 0,
            "last_selected_at": None,
            "created_at": now,
            "updated_at": now,
            "created_by": None,
        }
    )


class TestGenerate:
    def test_uses_generate_share_package_and_canonical_invite_link(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        calls = []
        _patch_invite_link(monkeypatch, link="https://t.me/+abc123", calls=calls)
        _hook(fake_db)
        _playback(fake_db)

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "whatsapp"})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["referral_link"] == "https://t.me/+abc123"
        assert calls == [(555, "creator1")]

        doc = fake_db["share_generations"].find_one({"package_id": body["package_id"]})
        assert doc is not None
        assert doc["generated_by"] == "creator_generated_share"
        assert doc["platform"] == "whatsapp"
        assert doc["user_id"] == 555

    def test_no_alternate_invite_link_created(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        calls = []
        _patch_invite_link(monkeypatch, link="https://t.me/+onlyOne", calls=calls)
        _hook(fake_db)
        _playback(fake_db)

        client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        # Exactly one call into the canonical invite-link function -- no
        # separate/alternate link creation path in the creator module.
        assert len(calls) == 1

    def test_active_hook_and_playback_selected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, link="https://t.me/+xyz")
        _hook(fake_db, "Active hook", status="active")
        _hook(fake_db, "Inactive hook", status="inactive")
        _playback(fake_db, "ActiveOne", status="active")
        _playback(fake_db, "InactiveOne", status="inactive")

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        body = resp.get_json()
        assert body["hook_text"] == "Active hook"
        assert body["playback_url"] == rsc.canonical_playback_url("ActiveOne")

    def test_no_hook_still_produces_valid_post(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, link="https://t.me/+nohook")
        _playback(fake_db, "OnlyPlayback")

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        body = resp.get_json()
        assert body["status"] == "ok"
        assert "None" not in body["share_text"]
        assert body["share_text"].startswith(rsc.canonical_playback_url("OnlyPlayback"))
        assert body["share_text"].endswith("https://t.me/+nohook")

    def test_no_playback_still_produces_valid_post(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, link="https://t.me/+noplayback")
        _hook(fake_db, "Only hook")

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        body = resp.get_json()
        assert body["status"] == "ok"
        assert "None" not in body["share_text"]
        assert body["share_text"].startswith("Only hook")

    def test_no_hook_no_playback_still_produces_valid_post(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, link="https://t.me/+bare")

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["share_text"] == (
            "Want more replays like this—and rewards too?\n"
            "Join AdvantPlay for:\n"
            "🎟️ Free welcome voucher\n"
            "⚡️ Daily voucher drops\n"
            "🏆 Weekly rewards\n"
            "\n"
            "Start here 👇\n"
            "https://t.me/+bare"
        )

    def test_fixed_cta_and_three_benefits_are_exact(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, link="https://t.me/+cta")
        _hook(fake_db)
        _playback(fake_db)

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        body = resp.get_json()
        assert "Want more replays like this—and rewards too?" in body["share_text"]
        assert "🎟️ Free welcome voucher" in body["share_text"]
        assert "⚡️ Daily voucher drops" in body["share_text"]
        assert "🏆 Weekly rewards" in body["share_text"]
        # The Mini App's lower-priority benefits must never appear here.
        assert "Bonus campaigns" not in body["share_text"]
        assert "VIP-only announcements" not in body["share_text"]
        # And never the Mini App's full five-benefit block wholesale.
        assert "👋 Welcome to AdvantPlay Community!" not in body["share_text"]

    def test_missing_referral_link_fails_without_partial_package(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, raise_error=RuntimeError("telegram down"))
        _hook(fake_db)

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        assert resp.status_code == 502
        assert fake_db["share_generations"].count_documents({}) == 0

    def test_invalid_platform_rejected(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch)

        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "instagram"})
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "invalid_platform"

    def test_rate_limit_enforced(self, fake_db, monkeypatch, client):
        # FakeCollection.find_one_and_update doesn't implement upsert=True
        # (only update_one does) -- swap in a minimal upsert-capable fake
        # for just this one collection, matching test_referral_rate_limit.py's
        # own fixture for the same reason.
        class _UpsertCollection:
            def __init__(self):
                self.docs = {}

            def find_one_and_update(self, filt, update, upsert=False, return_document=None):
                key = filt["key"]
                doc = self.docs.get(key)
                if doc is None and upsert:
                    doc = dict(update.get("$setOnInsert", {}))
                    doc.setdefault("count", 0)
                    self.docs[key] = doc
                if doc is None:
                    return None
                for field, inc_value in update.get("$inc", {}).items():
                    doc[field] = int(doc.get(field, 0)) + int(inc_value)
                return dict(doc)

        fake_db._collections["creator_generation_rate_limits"] = _UpsertCollection()

        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch)
        _hook(fake_db)
        _playback(fake_db)

        monkeypatch.setattr(csc, "GENERATE_HOURLY_LIMIT", 2)
        r1 = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        r2 = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        r3 = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r3.status_code == 429
        assert r3.get_json()["code"] == "creator_generation_rate_limited"

    def test_old_share_generations_documents_remain_compatible(self, fake_db, monkeypatch, client):
        # Legacy document with no package_id/platform/copy fields at all.
        fake_db["share_generations"].insert_one(
            {
                "user_id": 555,
                "hook_id": None,
                "hook_text": "legacy hook",
                "playback_record_id": None,
                "playback_id": None,
                "playback_url": None,
                "invite_link": "https://t.me/+legacy",
                "generated_at": rsc.now_utc(),
                "generated_by": "bot",
                "requested_by_admin": None,
            }
        )
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.get("/api/creator/share/results?init_data=ok")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"


class TestCopyAndShareEvents:
    def _generate(self, fake_db, monkeypatch, client, user_id=555):
        _fake_vouchers_module(monkeypatch, user_id=user_id)
        _creator(fake_db, user_id=user_id)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch, link="https://t.me/+own")
        _hook(fake_db)
        _playback(fake_db)
        resp = client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        return resp.get_json()["package_id"]

    def test_owner_can_mark_copied(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        resp = client.post(f"/api/creator/share/{package_id}/copied?init_data=ok", json={"platform": "whatsapp"})
        assert resp.status_code == 200
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["copy_count"] == 1
        assert doc["copied_at"] is not None
        assert doc["latest_copy_platform"] == "whatsapp"

    def test_copy_count_increments_atomically_across_calls(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        for _ in range(3):
            client.post(f"/api/creator/share/{package_id}/copied?init_data=ok", json={})
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["copy_count"] == 3

    def test_share_click_count_increments(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        client.post(f"/api/creator/share/{package_id}/share-clicked?init_data=ok", json={"platform": "telegram"})
        client.post(f"/api/creator/share/{package_id}/share-clicked?init_data=ok", json={"platform": "telegram"})
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["share_click_count"] == 2
        assert doc["share_clicked_at"] is not None

    def test_non_owner_receives_404_without_leaking_existence(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client, user_id=555)

        # A second, different creator authenticates and tries to mark the
        # first creator's package as copied.
        _fake_vouchers_module(monkeypatch, user_id=777)
        _creator(fake_db, user_id=777)

        resp = client.post(f"/api/creator/share/{package_id}/copied?init_data=ok", json={})
        assert resp.status_code == 404
        assert resp.get_json()["code"] == "not_found"

        # And an unrelated, definitely-nonexistent package_id gets the exact
        # same response shape.
        resp2 = client.post("/api/creator/share/does-not-exist/copied?init_data=ok", json={})
        assert resp2.status_code == 404
        assert resp2.get_json()["code"] == "not_found"

        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["copy_count"] == 0

    def test_copy_and_share_events_never_touch_referral_or_xp(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        fake_db["pending_referrals"].insert_one(
            {"inviter_user_id": 555, "invitee_user_id": 42, "status": "qualified", "created_at_utc": rsc.now_utc()}
        )
        client.post(f"/api/creator/share/{package_id}/copied?init_data=ok", json={})
        client.post(f"/api/creator/share/{package_id}/share-clicked?init_data=ok", json={})

        referral_doc = fake_db["pending_referrals"].find_one({"inviter_user_id": 555})
        assert referral_doc["status"] == "qualified"
        assert fake_db["pending_referrals"].count_documents({}) == 1


class TestResults:
    def test_uses_authoritative_status_buckets_and_excludes_revoked_from_qualified(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        now = rsc.now_utc()
        for doc in (
            {"inviter_user_id": 555, "invitee_user_id": 1, "status": "qualified", "created_at_utc": now},
            {"inviter_user_id": 555, "invitee_user_id": 2, "status": "pending", "created_at_utc": now},
            {"inviter_user_id": 555, "invitee_user_id": 3, "status": "revoked", "created_at_utc": now},
            # Different inviter -- must never leak into this creator's results.
            {"inviter_user_id": 999, "invitee_user_id": 4, "status": "qualified", "created_at_utc": now},
        ):
            fake_db["pending_referrals"].insert_one(doc)

        resp = client.get("/api/creator/share/results?init_data=ok")
        assert resp.status_code == 200
        results = resp.get_json()["results"]
        assert results["total_referral_joins"] == 3
        assert results["qualified_referrals"] == 1
        assert results["pending_referrals"] == 1
        assert results["revoked_referrals"] == 1

    def test_total_packages_generated_and_latest_generated_at(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        _patch_invite_link(monkeypatch)
        _hook(fake_db)
        _playback(fake_db)

        client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})
        client.post("/api/creator/share/generate?init_data=ok", json={"platform": "generic"})

        resp = client.get("/api/creator/share/results?init_data=ok")
        results = resp.get_json()["results"]
        assert results["total_packages_generated"] == 2
        assert results["latest_generated_at"] is not None


class TestBuildCreatorShareText:
    def test_requires_referral_link(self):
        with pytest.raises(ValueError):
            rsc.build_creator_share_text(hook_text="x", playback_url="y", referral_link="")

    def test_omits_missing_sections_without_orphan_separators(self):
        text = rsc.build_creator_share_text(hook_text=None, playback_url=None, referral_link="https://t.me/+abc")
        assert text == (
            "Want more replays like this—and rewards too?\n"
            "Join AdvantPlay for:\n"
            "🎟️ Free welcome voucher\n"
            "⚡️ Daily voucher drops\n"
            "🏆 Weekly rewards\n"
            "\n"
            "Start here 👇\n"
            "https://t.me/+abc"
        )
        assert "\n\n\n" not in text

    def test_includes_hook_and_playback_and_three_fixed_benefits(self):
        text = rsc.build_creator_share_text(
            hook_text="Thought it was done already 👀",
            playback_url="https://rx.apreplay.com/Abc123",
            referral_link="https://t.me/+abc",
        )
        assert text.startswith("Thought it was done already 👀\nhttps://rx.apreplay.com/Abc123\n\n")
        assert text.endswith("https://t.me/+abc")
        assert "🎟️ Free welcome voucher" in text
        assert "⚡️ Daily voucher drops" in text
        assert "🏆 Weekly rewards" in text
        assert text.count("https://t.me/+abc") == 1
        assert "Bonus campaigns" not in text
        assert "VIP-only announcements" not in text
        assert "👋 Welcome to AdvantPlay Community!" not in text
