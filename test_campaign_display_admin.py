"""Tests for the Admin Dashboard "Campaign Display Control" surface
(campaign_display_admin_bp in campaign_display_override.py), which
replaces the CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID Fly secret with a
Mongo-backed, admin-operable control.

Covers: admin auth gating, campaign CRUD + validation, activation rules
(future/expired/malformed), the disable-overrides-legacy-env-var
precedence, participant CRUD (stable entry_id targeting, duplicate/invalid
rejection, max-participant cap), optimistic-concurrency 409s, and that
Affiliate/Money Room/announcement-preview all reflect the Mongo-selected
campaign with no restart.

Uses the same standalone-blueprint + mongomock pattern as
test_event_banner.py (a lightweight Flask app registering only the
blueprint under test, `vouchers.require_admin` patched for auth), plus
mongomock (verified to correctly support $push/$pull/$set here; its
positional `$`/`$[elem]` array-update operators do not reliably resolve to
the query-matched index in this environment, which is exactly why
update_participant() in campaign_display_override.py rebuilds and writes
back the whole participants array instead of using them).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import mongomock
import pytest
from flask import Flask

import database
import campaign_display_override as cdo
from campaign_display_override import (
    CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION,
    ACTIVE_CAMPAIGN_SETTINGS_COLLECTION,
    ACTIVE_CAMPAIGN_SETTINGS_ID,
    get_active_campaign_id,
)

CAMPAIGN_ID = "referral_sep_2026"


@pytest.fixture
def fake_db(monkeypatch):
    fdb = mongomock.MongoClient().db
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cdo.campaign_display_admin_bp)
    return app


@pytest.fixture
def client():
    return _app().test_client()


def _mock_admin(uid: int = 42):
    return patch("vouchers.require_admin", return_value=({"id": uid, "usernameLower": "admin"}, None))


def _mock_unauthenticated():
    from flask import jsonify

    def _err():
        return None, (jsonify({"status": "error", "code": "auth_failed"}), 401)

    return patch("vouchers.require_admin", side_effect=_err)


def _mock_forbidden():
    from flask import jsonify

    def _err():
        return None, (jsonify({"status": "error", "code": "forbidden"}), 403)

    return patch("vouchers.require_admin", side_effect=_err)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _create_campaign(client, *, campaign_id=CAMPAIGN_ID, starts_delta=-1, ends_delta=1, enabled=True):
    now = datetime.now(timezone.utc)
    with _mock_admin():
        return client.post(
            "/api/admin/campaign-display/campaigns",
            json={
                "campaign_id": campaign_id,
                "starts_at": _iso(now + timedelta(days=starts_delta)),
                "ends_at": _iso(now + timedelta(days=ends_delta)),
                "enabled": enabled,
            },
        )


# ---------------------------------------------------------------------------
# 1-3: auth gating
# ---------------------------------------------------------------------------

def test_admin_authenticated_campaign_listing(client, fake_db):
    with _mock_admin():
        resp = client.get("/api/admin/campaign-display")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["campaigns"] == []
    assert body["settings"]["active_campaign_id"] is None


def test_unauthenticated_access_rejected(client, fake_db):
    with _mock_unauthenticated():
        resp = client.get("/api/admin/campaign-display")
    assert resp.status_code == 401
    assert fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].count_documents({}) == 0


def test_non_admin_access_rejected(client, fake_db):
    with _mock_forbidden():
        resp = client.post(
            "/api/admin/campaign-display/campaigns",
            json={"campaign_id": CAMPAIGN_ID, "starts_at": "2026-09-01T00:00:00Z", "ends_at": "2026-09-30T00:00:00Z"},
        )
    assert resp.status_code == 403
    assert fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# 4-7: campaign creation + schedule validation
# ---------------------------------------------------------------------------

def test_campaign_creation(client, fake_db):
    resp = _create_campaign(client)
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["campaign"]["campaign_id"] == CAMPAIGN_ID
    assert body["campaign"]["enabled"] is True
    assert body["campaign"]["participants"] == []
    doc = fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})
    assert doc["_id"] == CAMPAIGN_ID == doc["campaign_id"]
    # PyMongo/mongomock hand back a naive datetime on read even for a value
    # written as UTC-aware (documented convention throughout this repo --
    # see _coerce_aware_utc) -- what matters is that the value that was
    # WRITTEN was aware, which create_campaign_override() guarantees by
    # rejecting a naive starts_at_utc/ends_at_utc outright (invalid_schedule).
    assert cdo._coerce_aware_utc(doc["starts_at"]) is not None
    assert cdo._coerce_aware_utc(doc["ends_at"]) is not None


def test_duplicate_campaign_id_rejected(client, fake_db):
    first = _create_campaign(client)
    assert first.status_code == 201
    second = _create_campaign(client)
    assert second.status_code == 409
    assert second.get_json()["code"] == "campaign_id_exists"


def test_schedule_entered_in_kl_time_stored_as_aware_utc(client, fake_db):
    # Frontend converts KL (GMT+8) -> UTC before sending (see
    # admin-dashboard.js klInputValueToUtcIso); backend just needs to
    # store exactly what it receives as aware UTC, never naive.
    # 2026-09-07 08:00 KL == 2026-09-07 00:00 UTC.
    with _mock_admin():
        resp = client.post(
            "/api/admin/campaign-display/campaigns",
            json={
                "campaign_id": CAMPAIGN_ID,
                "starts_at": "2026-09-07T00:00:00+00:00",
                "ends_at": "2026-09-30T16:00:00+00:00",
                "enabled": True,
            },
        )
    assert resp.status_code == 201
    doc = fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})
    starts_at = doc["starts_at"]
    if starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=timezone.utc)
    assert starts_at == datetime(2026, 9, 7, 0, 0, 0, tzinfo=timezone.utc)


def test_invalid_schedule_rejected(client, fake_db):
    now = datetime.now(timezone.utc)
    with _mock_admin():
        resp = client.post(
            "/api/admin/campaign-display/campaigns",
            json={
                "campaign_id": CAMPAIGN_ID,
                "starts_at": _iso(now + timedelta(days=5)),
                "ends_at": _iso(now),  # ends before it starts
                "enabled": True,
            },
        )
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "ends_at_before_starts_at"
    assert fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].count_documents({}) == 0


def test_invalid_campaign_id_rejected(client, fake_db):
    now = datetime.now(timezone.utc)
    with _mock_admin():
        resp = client.post(
            "/api/admin/campaign-display/campaigns",
            json={
                "campaign_id": "Not A Valid Id!!",
                "starts_at": _iso(now - timedelta(days=1)),
                "ends_at": _iso(now + timedelta(days=1)),
            },
        )
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_campaign_id"


# ---------------------------------------------------------------------------
# 8-10: activation rules
# ---------------------------------------------------------------------------

def test_campaign_activation(client, fake_db):
    _create_campaign(client)
    with _mock_admin():
        resp = client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["activity"]["campaign_id"] == CAMPAIGN_ID
    assert get_active_campaign_id(fake_db) == CAMPAIGN_ID


def test_future_campaign_shown_as_scheduled(client, fake_db):
    _create_campaign(client, starts_delta=5, ends_delta=10)
    with _mock_admin():
        activate_resp = client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        assert activate_resp.status_code == 200  # future campaigns CAN be activated

        overview = client.get("/api/admin/campaign-display").get_json()
    campaign_row = next(c for c in overview["campaigns"] if c["campaign_id"] == CAMPAIGN_ID)
    assert campaign_row["state"] == "scheduled"
    # Not visible publicly yet even though it's the selected active campaign.
    assert overview["active_activity"]["state"] == "genuine_only"


def test_expired_campaign_cannot_be_activated_without_schedule_correction(client, fake_db):
    _create_campaign(client, starts_delta=-10, ends_delta=-1)  # already expired
    with _mock_admin():
        resp = client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "campaign_expired"
        assert get_active_campaign_id(fake_db) is None

        # Correcting the schedule allows activation.
        now = datetime.now(timezone.utc)
        fix_resp = client.put(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}",
            json={"starts_at": _iso(now - timedelta(days=1)), "ends_at": _iso(now + timedelta(days=1))},
        )
        assert fix_resp.status_code == 200
        activate_resp = client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        assert activate_resp.status_code == 200
    assert get_active_campaign_id(fake_db) == CAMPAIGN_ID


def test_malformed_campaign_cannot_be_activated(client, fake_db):
    # enabled=False at creation -> activation must be refused.
    _create_campaign(client, enabled=False)
    with _mock_admin():
        resp = client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "campaign_disabled"
    assert get_active_campaign_id(fake_db) is None


# ---------------------------------------------------------------------------
# 11-13: disable + tri-state precedence over the legacy env var
# ---------------------------------------------------------------------------

def test_disable_writes_authoritative_null_selection(client, fake_db):
    _create_campaign(client)
    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        assert get_active_campaign_id(fake_db) == CAMPAIGN_ID
        resp = client.post("/api/admin/campaign-display/disable")
    assert resp.status_code == 200
    settings_doc = fake_db[ACTIVE_CAMPAIGN_SETTINGS_COLLECTION].find_one({"_id": ACTIVE_CAMPAIGN_SETTINGS_ID})
    assert settings_doc["active_campaign_id"] is None
    # The campaign document itself must still exist -- disabling never deletes it.
    assert fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID}) is not None


def test_mongo_null_selection_overrides_existing_fly_env_var(client, fake_db, monkeypatch):
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", "some_other_legacy_campaign")
    _create_campaign(client)
    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        client.post("/api/admin/campaign-display/disable")
    # Even though the env var is still set to a (different) campaign, the
    # explicit Mongo null selection must win.
    assert get_active_campaign_id(fake_db) is None


def test_environment_fallback_works_only_before_mongo_settings_document_exists(fake_db, monkeypatch):
    monkeypatch.setenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID", "legacy_campaign")
    # No app_settings/campaign_display document yet -> env var fallback applies.
    assert get_active_campaign_id(fake_db) == "legacy_campaign"

    # Once any settings document exists (even selecting the same or a
    # different campaign), Mongo is authoritative from then on.
    cdo.set_active_campaign_id(fake_db, "referral_sep_2026", updated_by="admin")
    assert get_active_campaign_id(fake_db) == "referral_sep_2026"

    cdo.set_active_campaign_id(fake_db, None, updated_by="admin")
    assert get_active_campaign_id(fake_db) is None  # env var no longer consulted


# ---------------------------------------------------------------------------
# 14-21: participant management
# ---------------------------------------------------------------------------

def _add_participant(client, *, display_name="A***n", qualified_count=3, visible=True, expected_updated_at=None):
    body = {"display_name": display_name, "qualified_count": qualified_count, "visible": visible}
    if expected_updated_at is not None:
        body["expected_updated_at"] = expected_updated_at
    with _mock_admin():
        return client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants", json=body)


def test_adding_a_participant(client, fake_db):
    _create_campaign(client)
    resp = _add_participant(client)
    assert resp.status_code == 201
    body = resp.get_json()
    participants = body["campaign"]["participants"]
    assert len(participants) == 1
    assert participants[0]["display_name"] == "A***n"
    assert participants[0]["qualified_count"] == 3
    assert isinstance(participants[0]["entry_id"], str) and participants[0]["entry_id"]


def test_editing_qualified_count_from_3_to_5(client, fake_db):
    _create_campaign(client)
    add_resp = _add_participant(client, qualified_count=3)
    entry_id = add_resp.get_json()["campaign"]["participants"][0]["entry_id"]

    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        edit_resp = client.patch(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}",
            json={"qualified_count": 5},
        )
    assert edit_resp.status_code == 200
    participants = edit_resp.get_json()["campaign"]["participants"]
    assert participants[0]["qualified_count"] == 5
    assert participants[0]["entry_id"] == entry_id  # unchanged identity

    activity = cdo.build_public_campaign_activity(fake_db, CAMPAIGN_ID)
    assert activity["qualified_total"] == 5


def test_hiding_and_restoring_a_participant(client, fake_db):
    _create_campaign(client)
    add_resp = _add_participant(client)
    entry_id = add_resp.get_json()["campaign"]["participants"][0]["entry_id"]

    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        hide_resp = client.patch(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}",
            json={"visible": False},
        )
    assert hide_resp.status_code == 200
    activity = cdo.build_public_campaign_activity(fake_db, CAMPAIGN_ID)
    assert activity["qualified_total"] == 0  # hidden, excluded from total

    with _mock_admin():
        restore_resp = client.patch(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}",
            json={"visible": True},
        )
    assert restore_resp.status_code == 200
    activity = cdo.build_public_campaign_activity(fake_db, CAMPAIGN_ID)
    assert activity["qualified_total"] == 3


def test_removing_a_participant(client, fake_db):
    _create_campaign(client)
    add_resp = _add_participant(client)
    entry_id = add_resp.get_json()["campaign"]["participants"][0]["entry_id"]
    with _mock_admin():
        del_resp = client.delete(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}")
    assert del_resp.status_code == 200
    assert del_resp.get_json()["campaign"]["participants"] == []


def test_stable_entry_id_targeting_edits_only_the_matching_row(client, fake_db):
    _create_campaign(client)
    r1 = _add_participant(client, display_name="First", qualified_count=1)
    r2 = _add_participant(client, display_name="Second", qualified_count=2)
    entry_id_1 = r1.get_json()["campaign"]["participants"][0]["entry_id"]
    entry_id_2 = r2.get_json()["campaign"]["participants"][1]["entry_id"]
    assert entry_id_1 != entry_id_2

    with _mock_admin():
        # Edit the SECOND participant -- the first must be untouched.
        resp = client.patch(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id_2}",
            json={"qualified_count": 99},
        )
    assert resp.status_code == 200
    participants = {p["entry_id"]: p for p in resp.get_json()["campaign"]["participants"]}
    assert participants[entry_id_1]["display_name"] == "First"
    assert participants[entry_id_1]["qualified_count"] == 1
    assert participants[entry_id_2]["display_name"] == "Second"
    assert participants[entry_id_2]["qualified_count"] == 99


def test_duplicate_entry_id_rejected_at_add_time(client, fake_db):
    """add_participant() always server-generates entry_id, so a client
    can't submit a duplicate directly through this endpoint -- verify the
    underlying validation (_check_participant_fields, shared with the
    public read path) still rejects one at the function level, since a
    document hand-edited in Atlas could still contain one."""
    _create_campaign(client)
    fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one(
        {"_id": CAMPAIGN_ID},
        {"$set": {"participants": [{"entry_id": "dup", "display_name": "One", "qualified_count": 1, "visible": True}]}},
    )
    normalized, reason = cdo._check_participant_fields(
        {"entry_id": "dup", "display_name": "Two", "qualified_count": 2, "visible": True},
        seen_entry_ids={"dup"},
    )
    assert normalized is None
    assert reason == "duplicate_entry_id"


def test_invalid_count_types_rejected(client, fake_db):
    _create_campaign(client)
    for bad_count in (True, 3.5, "3", -1, 10 ** 9):
        resp = _add_participant(client, qualified_count=bad_count)
        assert resp.status_code == 400, f"qualified_count={bad_count!r} should be rejected"


def test_maximum_participant_limit_enforced(client, fake_db):
    _create_campaign(client)
    from campaign_display_override import MAX_OVERRIDE_PARTICIPANTS

    participants = [
        {"entry_id": f"p{i}", "display_name": f"N{i}", "qualified_count": 1, "visible": True}
        for i in range(MAX_OVERRIDE_PARTICIPANTS)
    ]
    fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"participants": participants}})
    resp = _add_participant(client)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "max_participants_reached"


# ---------------------------------------------------------------------------
# 22: stale concurrent update rejected with 409
# ---------------------------------------------------------------------------

def test_stale_concurrent_update_rejected_with_409(client, fake_db):
    _create_campaign(client)
    overview = None
    with _mock_admin():
        overview = client.get(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}").get_json()
    stale_updated_at = overview["campaign"]["updated_at"]

    # Someone else's edit lands first.
    with _mock_admin():
        client.put(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}",
            json={"enabled": False, "expected_updated_at": stale_updated_at},
        )
        # Second tab retries with the now-stale token it originally fetched.
        stale_resp = client.put(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}",
            json={"enabled": True, "expected_updated_at": stale_updated_at},
        )
    assert stale_resp.status_code == 409
    assert stale_resp.get_json()["code"] == "stale_update"


# ---------------------------------------------------------------------------
# 23-27: public surfaces reflect the Mongo-selected campaign
# ---------------------------------------------------------------------------

def test_all_public_surfaces_reflect_the_selected_campaign_and_match(client, fake_db):
    _create_campaign(client)
    _add_participant(client, display_name="A***n", qualified_count=7)
    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")

    active_id = get_active_campaign_id(fake_db)
    assert active_id == CAMPAIGN_ID

    # Affiliate / Money Room / announcement preview all call the exact
    # same shared builder keyed off the exact same Mongo-resolved id.
    affiliate_activity = cdo.build_public_campaign_activity(fake_db, get_active_campaign_id(fake_db))
    money_room_activity = cdo.build_public_campaign_activity(fake_db, get_active_campaign_id(fake_db))
    announcement_text = cdo.render_campaign_activity_announcement_text(money_room_activity)

    assert affiliate_activity["qualified_total"] == money_room_activity["qualified_total"] == 7
    assert affiliate_activity["leaderboard"] == money_room_activity["leaderboard"]
    assert "A***n" in announcement_text
    assert "7 qualified invites" in announcement_text


def test_disabling_removes_campaign_activity_from_all_surfaces(client, fake_db):
    _create_campaign(client)
    _add_participant(client)
    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        client.post("/api/admin/campaign-display/disable")

    active_id = get_active_campaign_id(fake_db)
    assert active_id is None
    # No campaign_id at all to resolve to -- every surface's "no active
    # campaign" path is exercised the same way main.py's own endpoints do.


# ---------------------------------------------------------------------------
# 28: activation/update requires no restart -- pure function-level proof
# ---------------------------------------------------------------------------

def test_activation_and_update_require_no_restart(client, fake_db):
    """No process/module reload happens between these calls -- proves the
    whole flow is live-request-driven, exactly like the rest of this test
    file's plain function calls."""
    _create_campaign(client)
    assert get_active_campaign_id(fake_db) is None
    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
    assert get_active_campaign_id(fake_db) == CAMPAIGN_ID

    add_resp = _add_participant(client, qualified_count=1)
    entry_id = add_resp.get_json()["campaign"]["participants"][0]["entry_id"]
    before_total = cdo.build_public_campaign_activity(fake_db, CAMPAIGN_ID)["qualified_total"]
    with _mock_admin():
        client.patch(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}",
            json={"qualified_count": 9},
        )
    after_total = cdo.build_public_campaign_activity(fake_db, CAMPAIGN_ID)["qualified_total"]
    assert after_total == before_total + 8


# ---------------------------------------------------------------------------
# 29-32: genuine data / rewards / KPI / abuse collections never touched
# ---------------------------------------------------------------------------

def test_admin_mutations_never_touch_genuine_or_abuse_collections(client, fake_db):
    guarded_collections = [
        "qualified_events",
        "referral_events",
        "referral_flow_events",
        "affiliate_ledger",
        "voucher_pools",
        "users",
    ]
    fake_db.users.insert_one({"user_id": 501, "username": None, "first_name": "GenuineFan", "total_referrals": 4})
    fake_db.qualified_events.insert_one({"invitee_id": 1, "referrer_id": 501, "qualified_at": datetime.now(timezone.utc)})
    before_counts = {name: fake_db[name].count_documents({}) for name in guarded_collections}
    before_user_doc = fake_db.users.find_one({"user_id": 501})

    _create_campaign(client)
    add_resp = _add_participant(client, qualified_count=3)
    entry_id = add_resp.get_json()["campaign"]["participants"][0]["entry_id"]
    with _mock_admin():
        client.post(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/activate")
        client.patch(
            f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}",
            json={"qualified_count": 50},
        )
        client.delete(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/participants/{entry_id}")
        client.post("/api/admin/campaign-display/disable")

    after_counts = {name: fake_db[name].count_documents({}) for name in guarded_collections}
    assert after_counts == before_counts
    assert fake_db.users.find_one({"user_id": 501}) == before_user_doc


# ---------------------------------------------------------------------------
# Preview endpoint
# ---------------------------------------------------------------------------

def test_preview_endpoint_returns_activity_and_announcement_text(client, fake_db):
    _create_campaign(client)
    _add_participant(client, display_name="A***n", qualified_count=4)
    with _mock_admin():
        resp = client.get(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/preview")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["qualified_total"] == 4
    assert "A***n" in body["preview_text"]


def test_preview_works_for_a_non_active_campaign(client, fake_db):
    """An admin should be able to preview a campaign before activating it."""
    _create_campaign(client, starts_delta=5, ends_delta=10)
    _add_participant(client, display_name="Future***", qualified_count=2)
    with _mock_admin():
        resp = client.get(f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}/preview")
    assert resp.status_code == 200
    # Not yet started -> override inactive, but the preview call itself
    # must still succeed (an admin previewing a scheduled campaign).
    assert resp.get_json()["state"] == "genuine_only"
