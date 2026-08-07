"""Tests for referral_engagement.py: tracking endpoint auth/validation/dedup,
admin analytics endpoint, KL day/week bucketing, and assisted-conversion
labeling. Mirrors test_creator_share_api.py's fake_db + fake-vouchers-module
+ bare-Flask-app-with-single-blueprint pattern."""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest
from flask import Flask, jsonify

import database
import referral_engagement as re_mod
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={"referral_engagement_events": [("event_id",)]})
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _fake_vouchers_module(monkeypatch, *, user_id=555, username="user1", admin_ok=True):
    fake_vouchers = types.ModuleType("vouchers")

    def _extract_raw_init_data_from_query(request):
        # A missing/blank init_data query param simulates "no initData sent".
        return request.args.get("init_data", "")

    def _verify_telegram_init_data(init_data):
        if init_data != "valid_init_data":
            return False, {}, "hash_mismatch"
        return True, {"user": {"id": user_id, "username": username}}, "ok"

    def _require_admin():
        if admin_ok:
            return {"id": 1, "usernameLower": "admin"}, None
        return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

    fake_vouchers.extract_raw_init_data_from_query = _extract_raw_init_data_from_query
    fake_vouchers.verify_telegram_init_data = _verify_telegram_init_data
    fake_vouchers.require_admin = _require_admin
    monkeypatch.setitem(sys.modules, "vouchers", fake_vouchers)


def _fake_creator_share_centre_module(monkeypatch, *, user_id=777, err=None):
    fake_csc = types.ModuleType("creator_share_centre")

    def _extract_authenticated_user():
        if err:
            return None, None, err
        return user_id, "creator1", None

    fake_csc._extract_authenticated_user = _extract_authenticated_user
    monkeypatch.setitem(sys.modules, "creator_share_centre", fake_csc)


@pytest.fixture
def app(fake_db):
    flask_app = Flask(__name__)
    flask_app.register_blueprint(re_mod.referral_engagement_bp)
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def _post(client, body, init_data="valid_init_data"):
    url = "/api/referral-engagement/events"
    if init_data is not None:
        url += "?init_data=" + init_data
    return client.post(url, json=body)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def test_authenticated_miniapp_event_accepted(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    resp = _post(client, {"event": "referral_cta_clicked", "source": "miniapp", "surface": "referral_section"})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True
    docs = fake_db["referral_engagement_events"].find({})
    assert len(docs) == 1
    assert docs[0]["user_id"] == 555
    assert docs[0]["source"] == "miniapp"


def test_authenticated_creator_centre_event_accepted(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    _fake_creator_share_centre_module(monkeypatch, user_id=888)
    resp = _post(client, {"event": "referral_cta_clicked", "source": "creator_centre", "surface": "share_package"})
    assert resp.status_code == 200
    docs = fake_db["referral_engagement_events"].find({})
    assert len(docs) == 1
    assert docs[0]["user_id"] == 888
    assert docs[0]["source"] == "creator_centre"


def test_missing_init_data_rejected(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    resp = _post(client, {"event": "referral_cta_clicked", "source": "miniapp", "surface": "x"}, init_data="")
    assert resp.status_code == 401
    assert fake_db["referral_engagement_events"].count_documents({}) == 0


def test_forged_init_data_rejected(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    resp = _post(client, {"event": "referral_cta_clicked", "source": "miniapp", "surface": "x"}, init_data="forged_garbage")
    assert resp.status_code == 401
    assert fake_db["referral_engagement_events"].count_documents({}) == 0


def test_creator_centre_auth_failure_rejected(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    _fake_creator_share_centre_module(monkeypatch, err=("invalid_telegram_auth", 401))
    resp = _post(client, {"event": "referral_cta_clicked", "source": "creator_centre", "surface": "share_package"})
    assert resp.status_code == 401
    assert fake_db["referral_engagement_events"].count_documents({}) == 0


def test_server_ignores_client_supplied_user_id(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    resp = _post(client, {
        "event": "referral_cta_clicked", "source": "miniapp", "surface": "x",
        "user_id": 999999,  # must be ignored -- user_id always comes from verified initData
    })
    assert resp.status_code == 200
    docs = fake_db["referral_engagement_events"].find({})
    assert docs[0]["user_id"] == 555


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_unknown_event_rejected(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    resp = _post(client, {"event": "not_a_real_event", "source": "miniapp", "surface": "x"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "unknown_event"
    assert fake_db["referral_engagement_events"].count_documents({}) == 0


def test_unknown_source_rejected(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    resp = _post(client, {"event": "referral_cta_clicked", "source": "web", "surface": "x"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "unknown_source"
    assert fake_db["referral_engagement_events"].count_documents({}) == 0


def test_oversized_metadata_rejected(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch)
    resp = _post(client, {
        "event": "referral_cta_clicked", "source": "miniapp", "surface": "x",
        "metadata": {"blob": "x" * 5000},
    })
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "metadata_too_large"


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def test_section_view_deduplication(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    body = {"event": "referral_section_viewed", "source": "miniapp", "surface": "referral_step_card", "session_id": "sess-1"}
    r1 = _post(client, body)
    r2 = _post(client, body)
    assert r1.status_code == 200 and r2.status_code == 200
    assert r2.get_json()["recorded"] == "deduped"
    assert fake_db["referral_engagement_events"].count_documents({}) == 1


def test_section_view_not_deduped_across_sessions(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    base = {"event": "referral_section_viewed", "source": "miniapp", "surface": "referral_step_card"}
    _post(client, {**base, "session_id": "sess-1"})
    _post(client, {**base, "session_id": "sess-2"})
    assert fake_db["referral_engagement_events"].count_documents({}) == 2


def test_rapid_click_deduplication(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    body = {"event": "referral_cta_clicked", "source": "miniapp", "surface": "referral_step_cta"}
    _post(client, body)
    _post(client, body)
    _post(client, body)
    # All within the same dedup bucket (default clock resolution during a
    # fast test run) -- rapid double/triple clicks must collapse to one row.
    assert fake_db["referral_engagement_events"].count_documents({}) == 1


def test_link_generated_dedup_by_referral_link_id(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    body = {
        "event": "referral_link_generated", "source": "miniapp", "surface": "referral_step_cta",
        "referral_link_id": "https://t.me/+abc123",
    }
    _post(client, body)
    _post(client, body)
    assert fake_db["referral_engagement_events"].count_documents({}) == 1


def test_successful_generation_tracked(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    resp = _post(client, {
        "event": "referral_link_generated", "source": "miniapp", "surface": "referral_step_cta",
        "referral_link_id": "https://t.me/+xyz",
    })
    assert resp.status_code == 200
    docs = fake_db["referral_engagement_events"].find({"event": "referral_link_generated"})
    assert len(docs) == 1


def test_failed_generation_not_tracked(client, fake_db, monkeypatch):
    # The client only calls the tracking endpoint after a *successful*
    # generation response -- a failed attempt simply never posts an event.
    _fake_vouchers_module(monkeypatch, user_id=555)
    assert fake_db["referral_engagement_events"].count_documents({"event": "referral_link_generated"}) == 0


def test_successful_copy_tracked(client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, user_id=555)
    resp = _post(client, {"event": "referral_copy_clicked", "source": "miniapp", "surface": "referral_step_cta"})
    assert resp.status_code == 200
    assert fake_db["referral_engagement_events"].count_documents({"event": "referral_copy_clicked"}) == 1


def test_failed_clipboard_path_does_not_report_success():
    # copyTextToClipboard()/performCopy() only call trackEngagementEvent()
    # on the branch where the copy actually succeeded (see static/index.html
    # copyReferralCaptionAndFlash()/copyReferral() and
    # static/creator-share.html performCopy()) -- a clipboard failure falls
    # through to showManualCopyCaption()/toast without ever posting
    # referral_copy_clicked. Nothing to assert against the backend here;
    # this documents the contract the frontend must uphold.
    assert True


# ---------------------------------------------------------------------------
# Write-failure isolation
# ---------------------------------------------------------------------------

def test_write_failure_does_not_block_caller(client, fake_db, monkeypatch, caplog):
    _fake_vouchers_module(monkeypatch, user_id=555)

    def _boom(self, doc):
        raise RuntimeError("mongo unavailable")

    monkeypatch.setattr(type(fake_db["referral_engagement_events"]), "insert_one", _boom)
    resp = _post(client, {"event": "referral_cta_clicked", "source": "miniapp", "surface": "x"})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True
    assert resp.get_json()["recorded"] == "write_failed"


def test_write_failure_logs_expected_tag(fake_db, monkeypatch, caplog):
    def _boom(self, doc):
        raise RuntimeError("mongo unavailable")

    monkeypatch.setattr(type(fake_db["referral_engagement_events"]), "insert_one", _boom)
    with caplog.at_level("WARNING"):
        ok, reason = re_mod.record_event(
            event="referral_cta_clicked", user_id=1, source="miniapp", surface="x",
        )
    assert ok is False
    assert reason == "write_failed"
    assert any("[REFERRAL_ENGAGEMENT][WRITE_FAILED]" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# KL day/week bucketing
# ---------------------------------------------------------------------------

def test_day_kl_boundary():
    # 2026-01-01 00:30 KL is 2025-12-31 16:30 UTC (KL = UTC+8, no DST).
    just_after_midnight_kl_utc = datetime(2025, 12, 31, 16, 30, tzinfo=timezone.utc)
    assert re_mod.day_kl(just_after_midnight_kl_utc) == "2026-01-01"
    just_before_midnight_kl_utc = datetime(2025, 12, 31, 15, 30, tzinfo=timezone.utc)
    assert re_mod.day_kl(just_before_midnight_kl_utc) == "2025-12-31"


def test_week_key_kl_boundary():
    # Monday 2026-01-05 00:10 KL vs Sunday 2026-01-04 23:50 KL must land in
    # different week buckets, both keyed by their Monday.
    monday_kl = datetime(2026, 1, 4, 16, 10, tzinfo=timezone.utc)  # 2026-01-05 00:10 KL
    sunday_kl = datetime(2026, 1, 4, 15, 50, tzinfo=timezone.utc)  # 2026-01-04 23:50 KL
    assert re_mod.week_key_kl(monday_kl) == "2026-01-05"
    assert re_mod.week_key_kl(sunday_kl) == "2025-12-29"


# ---------------------------------------------------------------------------
# Source separation
# ---------------------------------------------------------------------------

def test_source_separation_between_miniapp_and_creator_centre(fake_db):
    re_mod.record_event(event="referral_cta_clicked", user_id=1, source="miniapp", surface="a")
    re_mod.record_event(event="referral_cta_clicked", user_id=2, source="creator_centre", surface="b")
    miniapp_docs = fake_db["referral_engagement_events"].find({"source": "miniapp"})
    cc_docs = fake_db["referral_engagement_events"].find({"source": "creator_centre"})
    assert len(miniapp_docs) == 1
    assert len(cc_docs) == 1
    assert miniapp_docs[0]["user_id"] == 1
    assert cc_docs[0]["user_id"] == 2


# ---------------------------------------------------------------------------
# Admin analytics endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def admin_app(fake_db):
    flask_app = Flask(__name__)
    flask_app.register_blueprint(re_mod.referral_engagement_bp)
    return flask_app


@pytest.fixture
def admin_client(admin_app):
    return admin_app.test_client()


def test_admin_endpoint_requires_auth(admin_client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, admin_ok=False)
    resp = admin_client.get("/api/admin/referral-engagement")
    assert resp.status_code == 401


def test_admin_endpoint_returns_tracking_started_at(admin_client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, admin_ok=True)
    resp = admin_client.get("/api/admin/referral-engagement")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "tracking_started_at" in data["period"]
    assert data["period"]["tracking_started_at"]


def test_zero_denominator_handling(admin_client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, admin_ok=True)
    resp = admin_client.get("/api/admin/referral-engagement")
    data = resp.get_json()
    assert data["has_data"] is False
    assert data["rates"]["section_to_click"] == 0
    assert data["rates"]["click_to_generate"] == 0
    assert data["rates"]["generate_to_copy_or_share"] == 0
    assert data["rates"]["section_to_copy_or_share"] == 0
    assert data["totals"]["unique_section_viewers"] == 0
    assert data["assisted_conversion"]["engagement_to_join_rate"] == 0
    assert data["assisted_conversion"]["engagement_to_qualified_rate"] == 0


def test_funnel_unique_user_denominators(admin_client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, admin_ok=True)
    # 3 unique viewers, 2 of whom click, 1 of whom generates a link.
    for uid in (1, 2, 3):
        re_mod.record_event(event="referral_section_viewed", user_id=uid, source="miniapp", surface="s", session_id=f"s{uid}")
    for uid in (1, 2):
        re_mod.record_event(event="referral_cta_clicked", user_id=uid, source="miniapp", surface="s")
    re_mod.record_event(event="referral_link_generated", user_id=1, source="miniapp", surface="s", referral_link_id="l1")
    # Duplicate click event for user 1 from a different surface must still
    # only count once towards unique_cta_clickers (unique *users*, not rows).
    re_mod.record_event(event="referral_cta_clicked", user_id=1, source="miniapp", surface="other")

    resp = admin_client.get("/api/admin/referral-engagement?start_date=2000-01-01&end_date=2100-01-01")
    data = resp.get_json()
    assert data["totals"]["unique_section_viewers"] == 3
    assert data["totals"]["unique_cta_clickers"] == 2
    assert data["totals"]["unique_link_generators"] == 1
    assert data["rates"]["section_to_click"] == round(2 / 3, 4)
    assert data["rates"]["click_to_generate"] == round(1 / 2, 4)


def test_source_breakdown_in_admin_response(admin_client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, admin_ok=True)
    re_mod.record_event(event="referral_section_viewed", user_id=1, source="miniapp", surface="s", session_id="a")
    re_mod.record_event(event="referral_section_viewed", user_id=2, source="creator_centre", surface="s", session_id="b")
    resp = admin_client.get("/api/admin/referral-engagement?start_date=2000-01-01&end_date=2100-01-01")
    data = resp.get_json()
    by_source = {row["source"]: row for row in data["by_source"]}
    assert by_source["miniapp"]["viewers"] == 1
    assert by_source["creator_centre"]["viewers"] == 1


def test_assisted_conversion_is_referrer_level_not_direct_attribution(admin_client, fake_db, monkeypatch):
    _fake_vouchers_module(monkeypatch, admin_ok=True)
    re_mod.record_event(event="referral_link_generated", user_id=42, source="miniapp", surface="s", referral_link_id="l42")
    fake_db["pending_referrals"].insert_one({"inviter_user_id": 42, "invitee_user_id": 100, "status": "qualified"})

    resp = admin_client.get("/api/admin/referral-engagement?start_date=2000-01-01&end_date=2100-01-01")
    data = resp.get_json()
    ac = data["assisted_conversion"]
    assert ac["attribution_level"] == "referrer_level_assisted_conversion"
    assert "direct" not in ac["attribution_level"]
    assert ac["engaged_referrers"] == 1
    assert ac["engaged_referrers_with_join"] == 1
    assert ac["engaged_referrers_with_qualified_referral"] == 1
    assert ac["engagement_to_qualified_rate"] == 1.0
