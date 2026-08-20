"""Tests for creator_share_centre.py generation / copy / share-click / results
APIs: reuse of generate_share_package(), canonical invite link, package
ownership, rate limiting, and share_generations backward compatibility."""

from __future__ import annotations

import sys
import types
from datetime import timedelta

import pytest
from flask import Flask

import creator_share_centre as csc
import database
import referral_share_content as rsc
import scheduler
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    csc.invalidate_creator_group_settings_cache()
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    # scheduler.py binds `db` via `from database import db`, so patching
    # database.db alone does not reach it -- current_month_qualified_referral_count()
    # (used by creator_share_results() for reward-tier progress) reads
    # scheduler.db directly and must see the same fake collections.
    monkeypatch.setattr(scheduler, "db", fdb)
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

    def test_copy_method_auto_recorded(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        resp = client.post(
            f"/api/creator/share/{package_id}/copied?init_data=ok",
            json={"platform": "generic", "copy_method": "auto"},
        )
        assert resp.status_code == 200
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["latest_copy_method"] == "auto"
        assert doc["copy_count"] == 1

    def test_copy_method_manual_recorded(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        resp = client.post(
            f"/api/creator/share/{package_id}/copied?init_data=ok",
            json={"platform": "generic", "copy_method": "manual"},
        )
        assert resp.status_code == 200
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["latest_copy_method"] == "manual"

    def test_missing_copy_method_stays_valid_and_does_not_break_existing_fields(self, fake_db, monkeypatch, client):
        # Historical/older clients never send copy_method -- copy_count and
        # copied_at must keep working exactly as before, with no
        # latest_copy_method field written at all (never defaulted to
        # "manual" or any other guessed value).
        package_id = self._generate(fake_db, monkeypatch, client)
        resp = client.post(f"/api/creator/share/{package_id}/copied?init_data=ok", json={"platform": "whatsapp"})
        assert resp.status_code == 200
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert doc["copy_count"] == 1
        assert doc["copied_at"] is not None
        assert "latest_copy_method" not in doc

    def test_invalid_copy_method_ignored(self, fake_db, monkeypatch, client):
        package_id = self._generate(fake_db, monkeypatch, client)
        resp = client.post(
            f"/api/creator/share/{package_id}/copied?init_data=ok",
            json={"copy_method": "bogus"},
        )
        assert resp.status_code == 200
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert "latest_copy_method" not in doc

    def test_non_string_copy_method_ignored_without_500(self, fake_db, monkeypatch, client):
        # An unhashable copy_method (dict/list) must be silently ignored,
        # not raise TypeError from the ALLOWED_COPY_METHODS membership check.
        package_id = self._generate(fake_db, monkeypatch, client)
        resp = client.post(
            f"/api/creator/share/{package_id}/copied?init_data=ok",
            json={"copy_method": {"nested": "object"}},
        )
        assert resp.status_code == 200
        doc = fake_db["share_generations"].find_one({"package_id": package_id})
        assert "latest_copy_method" not in doc
        assert doc["copy_count"] == 1

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
        # next_reward_tier is intentionally NOT derived from qualified_referrals
        # (lifetime pending_referrals) -- it mirrors the current-reward-month
        # ledger scheduler.maybe_shout_referral_congrats() actually evaluates
        # REFERRAL_CONGRATS_TIERS against. No referral_events settlements were
        # seeded this month, so 10 more are needed for the first tier.
        assert results["next_reward_tier"] == {"qualified_needed": 10, "reward_amount": 10}

    def _set_month_settled_count(self, fake_db, now, count, inviter_user_id=555):
        month_key = scheduler._month_start_kl(now).date().isoformat()
        fake_db["referral_events"].delete_many({})
        for i in range(count):
            fake_db["referral_events"].insert_one(
                {
                    "inviter_id": inviter_user_id,
                    "invitee_id": i,
                    "event": "referral_settled",
                    "occurred_at": now,
                    "month_key": month_key,
                }
            )

    def test_next_reward_tier_uses_current_month_settled_ledger_not_lifetime_pending_referrals(
        self, fake_db, monkeypatch, client
    ):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = rsc.now_utc()

        # 40 lifetime qualified referrals in pending_referrals (would suggest
        # the 50-tier is close), but only 3 settled this reward month --
        # next_reward_tier must follow the monthly ledger, not this lifetime count.
        for i in range(40):
            fake_db["pending_referrals"].insert_one(
                {"inviter_user_id": 555, "invitee_user_id": i, "status": "qualified", "created_at_utc": now}
            )
        self._set_month_settled_count(fake_db, now, 3)

        resp = client.get("/api/creator/share/results?init_data=ok")
        results = resp.get_json()["results"]
        assert results["qualified_referrals"] == 40
        assert results["next_reward_tier"] == {"qualified_needed": 7, "reward_amount": 10}

    def test_next_reward_tier_progresses_through_the_ladder_and_caps_at_highest(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = rsc.now_utc()

        def _set_qualified_count(count):
            self._set_month_settled_count(fake_db, now, count)

        _set_qualified_count(10)
        resp = client.get("/api/creator/share/results?init_data=ok")
        assert resp.get_json()["results"]["next_reward_tier"] == {"qualified_needed": 15, "reward_amount": 15}

        _set_qualified_count(250)
        resp = client.get("/api/creator/share/results?init_data=ok")
        results = resp.get_json()["results"]
        assert results["next_reward_tier"] is None
        assert results["current_month_qualified"] == 250

    def test_current_month_invited_excludes_previous_month_referrals(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = rsc.now_utc()
        month_start_utc, _month_end_utc = scheduler.current_month_window_utc(now)

        in_month_ts = month_start_utc + timedelta(seconds=1)
        prev_month_ts = month_start_utc - timedelta(seconds=1)

        for doc in (
            {"inviter_user_id": 555, "invitee_user_id": 1, "status": "pending", "created_at_utc": in_month_ts},
            {"inviter_user_id": 555, "invitee_user_id": 2, "status": "pending", "created_at_utc": prev_month_ts},
            # Different inviter -- must never leak into this creator's count either.
            {"inviter_user_id": 999, "invitee_user_id": 3, "status": "pending", "created_at_utc": in_month_ts},
        ):
            fake_db["pending_referrals"].insert_one(doc)

        resp = client.get("/api/creator/share/results?init_data=ok")
        results = resp.get_json()["results"]
        assert results["current_month_referrals"] == 1
        # Lifetime total_referral_joins is unaffected by the month window.
        assert results["total_referral_joins"] == 2

    def test_month_window_is_kl_calendar_month_inclusive_start_exclusive_end(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = rsc.now_utc()
        month_start_utc, month_end_utc = scheduler.current_month_window_utc(now)

        fake_db["pending_referrals"].insert_one(
            {"inviter_user_id": 555, "invitee_user_id": 1, "status": "pending", "created_at_utc": month_start_utc}
        )
        fake_db["pending_referrals"].insert_one(
            {"inviter_user_id": 555, "invitee_user_id": 2, "status": "pending", "created_at_utc": month_end_utc}
        )

        resp = client.get("/api/creator/share/results?init_data=ok")
        results = resp.get_json()["results"]
        assert results["current_month_referrals"] == 1

    def test_current_month_qualified_matches_canonical_helper_and_reward_progress_shares_it(
        self, fake_db, monkeypatch, client
    ):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = rsc.now_utc()
        self._set_month_settled_count(fake_db, now, 7)

        resp = client.get("/api/creator/share/results?init_data=ok")
        results = resp.get_json()["results"]

        expected = scheduler.current_month_qualified_referral_count(555, now)
        assert expected == 7
        assert results["current_month_qualified"] == expected
        # Consistency requirement: the displayed Qualified figure and the
        # reward-progress calculation must come from the same value -- no
        # scenario where the UI shows N qualified but computes progress
        # from a different number.
        assert results["next_reward_tier"]["qualified_needed"] == 10 - results["current_month_qualified"]

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


class TestRecentWin:
    """GET /api/creator/recent-win reads the same referral_tier_congrats
    collection scheduler.maybe_shout_referral_congrats() writes as its
    Telegram-announcement dedup guard -- these tests cover the read side
    (endpoint) and, separately, that the write side (congrats/dedup/mask)
    still behaves as documented."""

    def _seed_issued_ledger(self, fake_db, *, user_id=555, tier_label="T2", now=None, voucher_code="AFFCODE1"):
        """The public announcement now only fires once affiliate_rewards.py's
        issuance flow has durably confirmed the voucher (affiliate_ledger
        status == ISSUED with a real voucher_code) -- seed that row so these
        write-path tests exercise the same gate maybe_shout_referral_congrats()
        actually evaluates."""
        now = now or csc.now_utc()
        year_month = scheduler._month_start_kl(now).strftime("%Y%m")
        fake_db["affiliate_ledger"].insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": user_id,
                "year_month": year_month,
                "tier": tier_label,
                "status": "ISSUED",
                "voucher_code": voucher_code,
            }
        )

    def _congrats_doc(self, fake_db, *, user_id=555, tier=25, amount=15, username="TheJone9", sent_at=None):
        now = sent_at or csc.now_utc()
        fake_db["referral_tier_congrats"].insert_one(
            {
                "user_id": user_id,
                "month_key": scheduler._month_start_kl(now).date().isoformat(),
                "tier": tier,
                "sent_at": now,
                "username": username,
                "display_name": username,
                "qualified_referrals": tier,
                "reward_amount": amount,
            }
        )

    def test_empty_state_hides_block_when_no_achievement_exists(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)

        resp = client.get("/api/creator/recent-win?init_data=ok")
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "ok", "win": None}

    def test_empty_state_ignores_legacy_docs_missing_display_fields(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = csc.now_utc()
        # Pre-migration dedup-only doc (no username/display_name/reward_amount).
        fake_db["referral_tier_congrats"].insert_one(
            {
                "user_id": 999,
                "month_key": scheduler._month_start_kl(now).date().isoformat(),
                "tier": 10,
                "sent_at": now,
            }
        )

        resp = client.get("/api/creator/recent-win?init_data=ok")
        assert resp.get_json() == {"status": "ok", "win": None}

    def test_returns_latest_achievement_with_masked_identity(self, fake_db, monkeypatch, client):
        _fake_vouchers_module(monkeypatch)
        _creator(fake_db)
        monkeypatch.delenv("CREATOR_GROUP_CHAT_ID", raising=False)
        now = csc.now_utc()
        self._congrats_doc(fake_db, user_id=111, tier=10, amount=10, username="OlderWin", sent_at=now - timedelta(hours=2))
        self._congrats_doc(fake_db, user_id=222, tier=25, amount=15, username="TheJone9", sent_at=now)

        resp = client.get("/api/creator/recent-win?init_data=ok")
        win = resp.get_json()["win"]
        assert win["display_name"] == "The***"
        assert win["qualified_referrals"] == 25
        assert win["reward_amount"] == 15
        assert win["achieved_at"] is not None

    def test_username_masking_keeps_two_to_three_leading_chars(self):
        assert csc._mask_display_name("TheJone9") == "The***"
        assert csc._mask_display_name("As_Offline") == "As_***"
        assert csc._mask_display_name("Al") == "Al***"
        assert csc._mask_display_name("") == "Someone"
        assert csc._mask_display_name(None) == "Someone"

    def test_duplicate_reward_unlock_does_not_duplicate_social_proof_event(self, fake_db, monkeypatch):
        """maybe_shout_referral_congrats() is idempotent per (user_id, month_key,
        tier) via the existing unique index -- calling it twice for the same
        settled tier must not create a second referral_tier_congrats doc, so
        the recent-win endpoint never has duplicate rows to pick from."""
        fake_db["referral_tier_congrats"]._unique_keys = [("user_id", "month_key", "tier")]
        fake_db["users"].insert_one({"user_id": 555, "username": "TheJone9", "first_name": "The"})
        now = csc.now_utc()
        month_key = scheduler._month_start_kl(now).date().isoformat()
        for i in range(25):
            fake_db["referral_events"].insert_one(
                {"inviter_id": 555, "invitee_id": i, "event": "referral_settled", "occurred_at": now, "month_key": month_key}
            )
        self._seed_issued_ledger(fake_db, now=now)

        sent = {"count": 0}

        class _Resp:
            ok = True
            status_code = 200
            text = ""

        def _fake_post(*args, **kwargs):
            sent["count"] += 1
            return _Resp()

        monkeypatch.setattr(scheduler.requests, "post", _fake_post)

        scheduler.maybe_shout_referral_congrats(555, now)
        scheduler.maybe_shout_referral_congrats(555, now)

        assert sent["count"] == 1
        assert fake_db["referral_tier_congrats"].count_documents({"user_id": 555, "month_key": month_key, "tier": 25}) == 1

    def test_congrats_write_persists_display_fields_for_recent_win(self, fake_db, monkeypatch):
        fake_db["users"].insert_one({"user_id": 555, "username": "TheJone9", "first_name": "The"})
        now = csc.now_utc()
        month_key = scheduler._month_start_kl(now).date().isoformat()
        for i in range(25):
            fake_db["referral_events"].insert_one(
                {"inviter_id": 555, "invitee_id": i, "event": "referral_settled", "occurred_at": now, "month_key": month_key}
            )
        self._seed_issued_ledger(fake_db, now=now)

        class _Resp:
            ok = True
            status_code = 200
            text = ""

        monkeypatch.setattr(scheduler.requests, "post", lambda *a, **kw: _Resp())

        scheduler.maybe_shout_referral_congrats(555, now)

        doc = fake_db["referral_tier_congrats"].find_one({"user_id": 555, "tier": 25})
        assert doc["username"] == "TheJone9"
        assert doc["display_name"] == "TheJone9"
        assert doc["qualified_referrals"] == 25
        assert doc["reward_amount"] == 15

    def test_telegram_announcement_text_unchanged(self, fake_db, monkeypatch):
        """The existing Telegram announcement text/format (beyond the
        unlocked->issued wording change) is untouched by the new persistence
        fields."""
        fake_db["users"].insert_one({"user_id": 555, "username": "TheJone9", "first_name": "The"})
        now = csc.now_utc()
        month_key = scheduler._month_start_kl(now).date().isoformat()
        for i in range(25):
            fake_db["referral_events"].insert_one(
                {"inviter_id": 555, "invitee_id": i, "event": "referral_settled", "occurred_at": now, "month_key": month_key}
            )
        self._seed_issued_ledger(fake_db, now=now)

        captured = {}

        class _Resp:
            ok = True
            status_code = 200
            text = ""

        def _fake_post(url, json=None, timeout=None):
            captured["json"] = json
            return _Resp()

        monkeypatch.setattr(scheduler.requests, "post", _fake_post)

        scheduler.maybe_shout_referral_congrats(555, now)

        text = captured["json"]["text"]
        assert "just hit <b>25 valid referrals</b> this month" in text
        assert "<b>$15 voucher issued!</b>" in text
        assert "Next: 50 refs = $50!" in text


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
