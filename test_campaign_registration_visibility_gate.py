"""Regression tests for the two Player Campaigns production issues:

1. Discovery: a live registration campaign was auto-prompted to every Mini
   App user (GET /api/campaign-registration/active with no campaign_ref).
   ``registration.listing = "unlisted"`` keeps a campaign out of every public
   discovery surface while its direct link keeps working; draft/archived
   campaigns stay closed to registration regardless of listing.
2. Channel gate: registration verifies membership server-side with a live
   getChatMember against the campaign's configured channel, with three
   distinct outcomes, and writes the registration (+ its base entries)
   exactly once under repeated/concurrent submissions.

Uses the same FakeDb + Flask test-client harness as test_campaign_registration.py,
but drives the REAL subscription_gate (only requests.get is mocked) so the
channel normalization and tri-state mapping are exercised end to end.
"""

import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests
from flask import Flask

import campaign_centre as cc
import campaign_registration as cr
import database
import subscription_gate as sg
from fake_mongo import FakeDb

CAMPAIGN_ID = "test260925"
UID = 777001


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb({
        "gc_campaigns": [("campaign_id",)],
        # ("_id",) mirrors MongoDB's implicit unique _id index.
        cr.REGISTRATIONS_COLLECTION: [("_id",), ("campaign_id", "telegram_user_id")],
        cr.STATE_COLLECTION: [("campaign_id", "telegram_user_id")],
        "campaign_subscription_cache": [("channel_id", "user_id")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setenv("BOT_TOKEN", "test-token")
    monkeypatch.setattr("subscription_gate.time.sleep", lambda *_: None)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cr.campaign_registration_bp)
    app.register_blueprint(cr.campaign_registration_admin_bp)
    app.register_blueprint(cc.campaign_centre_bp)
    app.register_blueprint(cc.campaign_public_bp)
    return app


def _verified(uid: int = UID, username: str = "tester"):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": '{"id": %d, "username": "%s"}' % (uid, username)}, "ok"),
    )


def _admin_ok():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "ops"}, None))


def _registration(**overrides):
    reg = cr.default_registration_config()
    reg["enabled"] = True
    reg.update(overrides)
    return reg


def _insert_campaign(fake_db, *, campaign_id=CAMPAIGN_ID, legacy_no_listing=False, **overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": campaign_id,
        "name": "Test260925",
        "type": "external_website",
        "status": "live",
        "priority": 100,
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(days=7)},
        # Stored exactly as the admin wizard stores it: bare, no "@".
        "telegram": {"require_identity": True, "require_subscription": True,
                     "channel_id": None, "channel_username": "advantplayofficial"},
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
        "registration": _registration(require_channel_subscription=True),
    }
    doc.update(overrides)
    if legacy_no_listing:
        doc["registration"].pop("listing", None)
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _payload(**overrides):
    body = {"full_name": "Jane Doe", "contact_number": "+60 12-345 6789",
            "country_region": "Malaysia", "delivery_address": "1 Jalan Test"}
    body.update(overrides)
    return body


def _tg(status_code=200, body=None, json_raises=False):
    resp = MagicMock()
    resp.status_code = status_code
    if json_raises:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = body if body is not None else {}
    return resp


def _member(status="member", **extra):
    return _tg(200, {"ok": True, "result": {"status": status, **extra}})


def _register(client, campaign_id=CAMPAIGN_ID, **payload):
    return client.post(f"/api/campaign-registration/{campaign_id}/register?init_data=x", json=_payload(**payload))


def _rows(fake_db):
    return fake_db[cr.REGISTRATIONS_COLLECTION].count_documents({})


# ===========================================================================
# Issue 1 — discovery vs direct link
# ===========================================================================

def test_legacy_campaign_without_listing_key_stays_listed(fake_db):
    """Existing campaigns keep their current behavior: no key == listed ==
    auto-prompted on every Mini App open (this is the incident path)."""
    _insert_campaign(fake_db, legacy_no_listing=True)
    assert "listing" not in fake_db["gc_campaigns"].find_one({})["registration"]
    with _app().test_client() as client, _verified():
        data = client.get("/api/campaign-registration/active?init_data=x").get_json()
    assert data["campaign"]["campaign_id"] == CAMPAIGN_ID
    assert data["should_prompt"] is True
    assert data["campaign"]["listing"] == "listed"


def test_unlisted_campaign_absent_from_global_discovery(fake_db):
    _insert_campaign(fake_db, registration=_registration(listing="unlisted"))
    with _app().test_client() as client, _verified():
        data = client.get("/api/campaign-registration/active?init_data=x").get_json()
    assert data["campaign"] is None
    assert data["should_prompt"] is False


def test_unlisted_campaign_resolved_via_direct_link(fake_db):
    _insert_campaign(fake_db, registration=_registration(listing="unlisted"))
    with _app().test_client() as client, _verified():
        data = client.get(f"/api/campaign-registration/active?init_data=x&campaign_ref={CAMPAIGN_ID}").get_json()
    assert data["campaign"]["campaign_id"] == CAMPAIGN_ID
    assert data["campaign"]["listing"] == "unlisted"
    assert data["should_prompt"] is True


def test_unlisted_never_leaks_through_fallback_even_with_higher_priority(fake_db):
    _insert_campaign(fake_db, campaign_id="secret-test", priority=999,
                     registration=_registration(listing="unlisted"))
    _insert_campaign(fake_db, campaign_id="public-draw", priority=1)
    with _app().test_client() as client, _verified():
        default = client.get("/api/campaign-registration/active?init_data=x").get_json()
        bad_ref = client.get("/api/campaign-registration/active?init_data=x&campaign_ref=nope").get_json()
    assert default["campaign"]["campaign_id"] == "public-draw"
    assert bad_ref["campaign"]["campaign_id"] == "public-draw"


def test_public_card_list_excludes_unlisted_registration_campaign(fake_db):
    ready_dest = {"provider_id": "p1", "open_mode": "telegram_web_app", "path": "", "ready": True}
    _insert_campaign(fake_db, campaign_id="listed-card", destination=dict(ready_dest))
    _insert_campaign(fake_db, campaign_id="unlisted-card", destination=dict(ready_dest),
                     registration=_registration(listing="unlisted"))
    with _app().test_client() as client, \
            patch("campaign_centre.get_provider", return_value={"provider_id": "p1", "active": True,
                                                                "base_url": "https://example.com"}):
        cards = client.get("/api/campaigns/active").get_json()["campaigns"]
    ids = {c["campaign_id"] for c in cards}
    assert "listed-card" in ids
    assert "unlisted-card" not in ids


def test_unlisted_direct_link_registration_succeeds(fake_db):
    _insert_campaign(fake_db, registration=_registration(listing="unlisted", require_channel_subscription=True))
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member()):
        r = _register(client)
    assert r.status_code == 201
    assert _rows(fake_db) == 1


@pytest.mark.parametrize("status", ["draft", "scheduled", "paused", "ended", "archived"])
@pytest.mark.parametrize("listing", ["listed", "unlisted"])
def test_non_live_campaign_rejects_registration_and_discovery(fake_db, status, listing):
    _insert_campaign(fake_db, status=status, registration=_registration(listing=listing))
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member()) as tg:
        via_ref = client.get(f"/api/campaign-registration/active?init_data=x&campaign_ref={CAMPAIGN_ID}").get_json()
        r = _register(client)
    assert via_ref["campaign"] is None
    assert r.status_code == 404
    assert r.get_json()["code"] == "registration_unavailable"
    assert _rows(fake_db) == 0
    tg.assert_not_called()


def test_archived_campaign_returns_existing_registration_without_new_entry(fake_db):
    _insert_campaign(fake_db, status="archived")
    fake_db[cr.REGISTRATIONS_COLLECTION].insert_one({
        "_id": cr.registration_doc_id(CAMPAIGN_ID, UID), "campaign_id": CAMPAIGN_ID,
        "telegram_user_id": UID, "base_entries": 1, "status": "registered",
        "registered_at": datetime.now(timezone.utc),
    })
    with _app().test_client() as client, _verified():
        r = _register(client)
    assert r.status_code == 200
    assert r.get_json()["already_registered"] is True
    assert r.get_json()["registration"]["base_entries"] == 1
    assert _rows(fake_db) == 1


def test_validate_registration_config_listing():
    cfg, err = cr.validate_registration_config({})
    assert err is None and cfg["listing"] == "listed"
    cfg, err = cr.validate_registration_config({"listing": "unlisted"})
    assert err is None and cfg["listing"] == "unlisted"
    assert cr.validate_registration_config({"listing": "hidden"}) == (None, "invalid_listing")


def test_admin_can_set_unlisted_and_omitted_listing_is_preserved(fake_db):
    _insert_campaign(fake_db, legacy_no_listing=True)
    with _app().test_client() as client, _admin_ok():
        r = client.put(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}",
                       json={"registration": {"enabled": True, "require_channel_subscription": True,
                                              "listing": "unlisted"}})
        assert r.status_code == 200, r.get_json()
        assert fake_db["gc_campaigns"].find_one({})["registration"]["listing"] == "unlisted"

        # A save from an older dashboard build that doesn't know the field.
        r = client.put(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}",
                       json={"registration": {"enabled": True, "require_channel_subscription": True,
                                              "reminder_hours": 12}})
        assert r.status_code == 200, r.get_json()
        stored = fake_db["gc_campaigns"].find_one({})["registration"]
        assert stored["listing"] == "unlisted"
        assert stored["reminder_hours"] == 12

        r = client.put(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}",
                       json={"registration": {"enabled": True, "require_channel_subscription": True,
                                              "listing": "listed"}})
        assert r.status_code == 200
        assert fake_db["gc_campaigns"].find_one({})["registration"]["listing"] == "listed"


def test_admin_preview_works_for_draft_and_reports_listing(fake_db):
    _insert_campaign(fake_db, status="draft", registration=_registration(listing="unlisted"))
    with _app().test_client() as client, _admin_ok():
        r = client.get(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}/preview")
    assert r.status_code == 200
    data = r.get_json()
    assert data["registration_listing"] == "unlisted"
    assert "draft" in data["admin_badges"]


# ===========================================================================
# Issue 2 — backend channel gate
# ===========================================================================

@pytest.mark.parametrize("status", ["member", "administrator", "creator"])
def test_confirmed_member_registers_with_exactly_one_base_entry(fake_db, status):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member(status)) as tg:
        r = _register(client)
    assert r.status_code == 201, r.get_json()
    reg = r.get_json()["registration"]
    assert reg["channel_verified"] is True
    assert reg["base_entries"] == 1
    rows = fake_db[cr.REGISTRATIONS_COLLECTION].find({})
    assert len(rows) == 1
    assert rows[0]["_id"] == f"{CAMPAIGN_ID}:{UID}"
    assert rows[0]["telegram_user_id"] == UID
    # Root-cause regression: the bare stored username must reach Telegram
    # as "@advantplayofficial", with the initData-verified user id.
    assert tg.call_args.kwargs["params"] == {"chat_id": "@advantplayofficial", "user_id": UID}


def test_restricted_but_still_member_is_allowed(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member("restricted", is_member=True)):
        assert _register(client).status_code == 201


@pytest.mark.parametrize("member", [{"status": "left"}, {"status": "kicked"},
                                    {"status": "restricted", "is_member": False}])
def test_confirmed_non_member_rejected_without_writes(fake_db, member):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_tg(200, {"ok": True, "result": member})):
        r = _register(client)
    assert r.status_code == 403
    data = r.get_json()
    assert data["code"] == "channel_subscription_required"
    assert data["channel_url"] == "https://t.me/advantplayofficial"
    assert _rows(fake_db) == 0


@pytest.mark.parametrize("label,tg_kwargs", [
    ("timeout", {"side_effect": requests.exceptions.Timeout("boom")}),
    ("connection", {"side_effect": requests.exceptions.ConnectionError("down")}),
    ("429", {"return_value": _tg(429, {"ok": False, "description": "Too Many Requests"})}),
    ("400_chat_not_found", {"return_value": _tg(400, {"ok": False, "description": "Bad Request: chat not found"})}),
    ("403_bot_not_admin", {"return_value": _tg(403, {"ok": False, "description": "Forbidden"})}),
    ("500", {"return_value": _tg(500, json_raises=True)}),
    ("bad_json", {"return_value": _tg(200, json_raises=True)}),
    ("non_dict_json", {"return_value": _tg(200, ["unexpected"])}),
    ("ok_false", {"return_value": _tg(200, {"ok": False, "description": "weird"})}),
    ("missing_result", {"return_value": _tg(200, {"ok": True})}),
    ("unknown_status", {"return_value": _member("some_future_status")}),
])
def test_unverifiable_is_retry_not_unsubscribed_and_writes_nothing(fake_db, label, tg_kwargs):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), patch("subscription_gate.requests.get", **tg_kwargs):
        r = _register(client)
    assert r.status_code == 503, (label, r.get_json())
    assert r.get_json()["code"] == "subscription_check_failed"
    assert _rows(fake_db) == 0


def test_missing_bot_token_is_unverifiable(fake_db, monkeypatch):
    monkeypatch.delenv("BOT_TOKEN", raising=False)
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), patch("subscription_gate.requests.get") as tg:
        r = _register(client)
    assert r.status_code == 503
    assert r.get_json()["code"] == "subscription_check_failed"
    tg.assert_not_called()
    assert _rows(fake_db) == 0


def test_missing_or_invalid_channel_config_is_unverifiable(fake_db):
    # Written directly (bypassing the admin save-time guard) to model a
    # legacy/hand-edited document.
    _insert_campaign(fake_db, telegram={"channel_id": None, "channel_username": "not a channel!"})
    with _app().test_client() as client, _verified(), patch("subscription_gate.requests.get") as tg:
        r = _register(client)
    assert r.status_code == 503
    assert r.get_json()["code"] == "subscription_check_failed"
    tg.assert_not_called()
    assert _rows(fake_db) == 0


def test_client_supplied_subscribed_flag_is_ignored(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member("left")):
        r = _register(client, subscribed=True, channel_verified=True, base_entries=50,
                      telegram_user_id=1, is_member=True)
    assert r.status_code == 403
    assert _rows(fake_db) == 0


def test_registration_ignores_stale_positive_cache(fake_db):
    """A positive cache row (e.g. from /play within the last 5 min) must not
    satisfy the registration gate — registration always checks live."""
    _insert_campaign(fake_db)
    sg._cache_set("@advantplayofficial", UID, True, 300)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member("left")) as tg:
        r = _register(client)
    tg.assert_called_once()
    assert r.status_code == 403
    assert _rows(fake_db) == 0


def test_verify_error_then_retry_after_joining_registers_once(fake_db):
    """The incident sequence: first submit hits an unverifiable check, the
    retry succeeds — exactly one row, one base entry."""
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified():
        with patch("subscription_gate.requests.get", side_effect=requests.exceptions.Timeout()):
            assert _register(client).status_code == 503
        assert _rows(fake_db) == 0
        with patch("subscription_gate.requests.get", return_value=_member()):
            assert _register(client).status_code == 201
    row = fake_db[cr.REGISTRATIONS_COLLECTION].find_one({})
    assert row["base_entries"] == 1


# ===========================================================================
# Idempotency / concurrency
# ===========================================================================

def test_duplicate_submissions_return_existing_state_without_new_entry(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member()) as tg:
        first = _register(client)
        again = [_register(client, full_name="Changed Name") for _ in range(3)]
    assert first.status_code == 201
    for r in again:
        assert r.status_code == 200
        body = r.get_json()
        assert body["already_registered"] is True
        assert body["registration"]["base_entries"] == 1
        assert body["registration"]["full_name"] == "Jane Doe"
    assert _rows(fake_db) == 1
    # Already-registered short-circuits before the Telegram call.
    assert tg.call_count == 1


def test_concurrent_submissions_write_exactly_one_registration(fake_db):
    workers = 6
    _insert_campaign(fake_db)
    app = _app()
    # Every request is held inside getChatMember until all of them have
    # passed the "already registered?" read, so they all race the insert.
    barrier = threading.Barrier(workers, timeout=10)

    def slow_member(*_args, **_kwargs):
        barrier.wait()
        return _member()

    results = []
    lock = threading.Lock()

    def worker():
        with app.test_client() as client:
            r = _register(client)
            with lock:
                results.append((r.status_code, r.get_json()))

    with _verified(), patch("subscription_gate.requests.get", side_effect=slow_member):
        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

    assert len(results) == workers
    codes = sorted(code for code, _ in results)
    assert codes.count(201) == 1, results
    assert codes.count(200) == workers - 1, results
    for _code, body in results:
        assert body["status"] == "ok"
        assert body["registration"]["base_entries"] == 1
    assert _rows(fake_db) == 1


def test_insert_failure_leaves_no_row_and_retry_succeeds(fake_db):
    _insert_campaign(fake_db)
    col = fake_db[cr.REGISTRATIONS_COLLECTION]
    real_insert = col.insert_one
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member()):
        with patch.object(col, "insert_one", side_effect=RuntimeError("primary stepped down")):
            r = _register(client)
        assert r.status_code == 500
        assert _rows(fake_db) == 0
        with patch.object(col, "insert_one", side_effect=real_insert):
            assert _register(client).status_code == 201
    assert _rows(fake_db) == 1


def test_post_commit_side_effect_failure_does_not_fail_registration(fake_db):
    _insert_campaign(fake_db)
    with _app().test_client() as client, _verified(), \
            patch("subscription_gate.requests.get", return_value=_member()), \
            patch.object(fake_db[cr.STATE_COLLECTION], "delete_one", side_effect=RuntimeError("x")), \
            patch("campaign_centre.log_funnel_event", side_effect=RuntimeError("y")):
        r = _register(client)
    assert r.status_code == 201
    assert _rows(fake_db) == 1


def test_invalid_stored_base_entries_never_persisted(fake_db):
    _insert_campaign(fake_db, registration=_registration(require_channel_subscription=False, base_entries="lots"))
    with _app().test_client() as client, _verified():
        r = _register(client)
    assert r.status_code == 201
    assert fake_db[cr.REGISTRATIONS_COLLECTION].find_one({})["base_entries"] == 1


# ===========================================================================
# Channel normalization unit tests
# ===========================================================================

@pytest.mark.parametrize("cfg,expected", [
    ({"channel_username": "advantplayofficial"}, "@advantplayofficial"),
    ({"channel_username": "@AdvantPlayOfficial"}, "@AdvantPlayOfficial"),
    ({"channel_username": " https://t.me/advantplayofficial "}, "@advantplayofficial"),
    ({"channel_username": "t.me/advantplayofficial?start=1"}, "@advantplayofficial"),
    ({"channel_id": -1001234567890, "channel_username": "ignored"}, -1001234567890),
    ({"channel_id": "-1001234567890"}, "-1001234567890"),
    ({"channel_id": "@advantplayofficial"}, "@advantplayofficial"),
    ({"channel_id": None, "channel_username": ""}, None),
    ({"channel_username": "bad name!"}, None),
    ({}, None),
    (None, None),
])
def test_resolve_channel_chat_id(cfg, expected):
    assert sg.resolve_channel_chat_id(cfg) == expected
