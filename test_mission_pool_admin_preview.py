"""Admin-preview visibility for Mission Pool campaigns.

Mirrors the existing standard-drop admin-preview contract (vouchers.py
``/vouchers/visible``): an authorized admin viewing the normal user-facing
Mini App sees admin-only content, a normal user never does, and admin
identity is always resolved server-side via the SAME canonical helper
(``vouchers._is_cached_admin``) — never a frontend-supplied ``is_admin``
flag, never Telegram username alone.

Fixture matches the stated production record: mechanic=mission_pool,
status=live, visibility=admin_only, destination.ready=false, with a
visibility_reason of "linked provider does not exist".
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
import mission_pool_ux as ux
from fake_mongo import FakeDb

CAMPAIGN_ID = "mission-admin-only-1"
PUBLIC_CAMPAIGN_ID = "mission-public-1"
ADMIN_UID = 900001
USER_UID = 900002


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        "gc_campaigns": [("campaign_id",)],
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys())
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(mp, "database", database)
    monkeypatch.setattr(ux, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(mp.mission_pool_bp)
    app.register_blueprint(ux.mission_pool_ux_bp)
    return app


def _verified(uid: int, username: str = ""):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}, "username": "{username}"}}'}, "ok"),
    )


def _admin(is_admin: bool):
    return patch("vouchers._is_cached_admin", return_value=(is_admin, "cache" if is_admin else None))


def _campaign(campaign_id, **overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": campaign_id,
        "name": "Staff QA Mission",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "destination": {"ready": False},
        "mission_config": {
            "mission_type": "multiple_choice",
            "prompt": "Which game?",
            "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "correct_answer": "a",
        },
        "mission_pool": {
            "pool_id": "MISSION-ADMIN-ONLY",
            "pool_type": "voucher_drop",
            "winner_count": 3,
            "allocation_method": "random_qualified",
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "processing_stage": mp.STAGE_PENDING,
            "processing_generation": 0,
        },
    }
    doc.update(overrides)
    return doc


def _seed_admin_only(fake_db):
    doc = _campaign(
        CAMPAIGN_ID,
        visibility="admin_only",
        visibility_reason="linked provider does not exist",
    )
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _seed_public(fake_db):
    doc = _campaign(PUBLIC_CAMPAIGN_ID, name="Public Mission")
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


# ---------------------------------------------------------------------------
# 1. Authorized admin receives the admin-only campaign
# ---------------------------------------------------------------------------

def test_admin_sees_admin_only_mission_in_active_list(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    data = resp.get_json()
    ids = [m["campaign_id"] for m in data["missions"]]
    assert CAMPAIGN_ID in ids
    card = next(m for m in data["missions"] if m["campaign_id"] == CAMPAIGN_ID)
    assert card["preview_mode"] is True
    assert card["visibility"] == "admin-only"
    assert card["visibility_reason"] == "Not visible to users: linked provider does not exist"


# ---------------------------------------------------------------------------
# 2. Normal user does not receive that campaign
# ---------------------------------------------------------------------------

def test_normal_user_does_not_see_admin_only_mission(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(USER_UID), _admin(False):
        resp = client.get("/api/mission-pool/active?init_data=x")
    data = resp.get_json()
    ids = [m["campaign_id"] for m in data["missions"]]
    assert CAMPAIGN_ID not in ids


# ---------------------------------------------------------------------------
# 3. Normal user cannot bypass visibility using is_admin=true
# ---------------------------------------------------------------------------

def test_is_admin_query_param_is_never_trusted(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(USER_UID), _admin(False):
        resp = client.get("/api/mission-pool/active?init_data=x&is_admin=true")
    data = resp.get_json()
    ids = [m["campaign_id"] for m in data["missions"]]
    assert CAMPAIGN_ID not in ids


def test_is_admin_body_flag_is_never_trusted_on_view(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(USER_UID), _admin(False):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x&is_admin=true")
    assert resp.status_code == 404


def test_normal_user_cannot_submit_to_admin_only_mission_directly(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(USER_UID), _admin(False):
        resp = client.post(
            f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=x",
            json={"answer": "a"},
        )
    assert resp.status_code == 404
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# 4. Public mission remains visible to both users
# ---------------------------------------------------------------------------

def test_public_mission_visible_to_admin_and_normal_user(fake_db):
    _seed_public(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    admin_ids = [m["campaign_id"] for m in resp.get_json()["missions"]]
    assert PUBLIC_CAMPAIGN_ID in admin_ids

    with app.test_client() as client, _verified(USER_UID), _admin(False):
        resp = client.get("/api/mission-pool/active?init_data=x")
    user_ids = [m["campaign_id"] for m in resp.get_json()["missions"]]
    assert PUBLIC_CAMPAIGN_ID in user_ids


# ---------------------------------------------------------------------------
# 5. Deleted/cancelled mission remains hidden even for admins
# ---------------------------------------------------------------------------

def test_cancelled_admin_only_mission_stays_hidden_from_admin(fake_db):
    doc = _campaign(
        CAMPAIGN_ID,
        visibility="admin_only",
        visibility_reason="linked provider does not exist",
    )
    doc["mission_pool"]["cancelled"] = True
    fake_db["gc_campaigns"].insert_one(doc)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    ids = [m["campaign_id"] for m in resp.get_json()["missions"]]
    assert CAMPAIGN_ID not in ids


def test_archived_admin_only_mission_stays_hidden_from_admin(fake_db):
    doc = _campaign(
        CAMPAIGN_ID,
        status="archived",
        visibility="admin_only",
        visibility_reason="linked provider does not exist",
    )
    fake_db["gc_campaigns"].insert_one(doc)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    ids = [m["campaign_id"] for m in resp.get_json()["missions"]]
    assert CAMPAIGN_ID not in ids


# ---------------------------------------------------------------------------
# 6. Admin-preview response contains no sensitive mission fields
# ---------------------------------------------------------------------------

def test_admin_preview_card_has_no_sensitive_fields(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    card = next(m for m in resp.get_json()["missions"] if m["campaign_id"] == CAMPAIGN_ID)
    blob = str(card)
    assert "correct_answer" not in blob
    assert "pool_id" not in blob
    assert "MISSION-ADMIN-ONLY" not in blob
    assert "eligibility_policy" not in blob


def test_admin_preview_view_has_no_sensitive_fields(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    data = resp.get_json()
    assert data["preview_mode"] is True
    blob = str(data)
    assert "correct_answer" not in blob
    assert "pool_id" not in blob
    assert "eligibility_policy" not in blob


# ---------------------------------------------------------------------------
# 7. Admin preview cannot create a real submission or consume a reward
# ---------------------------------------------------------------------------

def test_admin_preview_cannot_submit(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.post(
            f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=x",
            json={"answer": "a"},
        )
    assert resp.status_code == 403
    assert resp.get_json()["code"] == "admin_preview_read_only"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_view_reports_submissions_closed_for_admin_preview(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    assert resp.get_json()["submissions_open"] is False


# ---------------------------------------------------------------------------
# 8. Standard-drop visibility (vouchers.py) is untouched by this feature
# ---------------------------------------------------------------------------

def test_mission_pool_change_does_not_touch_vouchers_module():
    import vouchers  # noqa: F401 — importable/unaffected, no assertions needed


# ---------------------------------------------------------------------------
# 10. No-store caching on the user-specific/admin-specific response
# ---------------------------------------------------------------------------

def test_active_missions_response_is_never_cached(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    cache_control = resp.headers.get("Cache-Control", "")
    assert "no-store" in cache_control
    assert "private" in cache_control


def test_view_response_is_never_cached(fake_db):
    _seed_admin_only(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    cache_control = resp.headers.get("Cache-Control", "")
    assert "no-store" in cache_control
    assert "private" in cache_control


def test_public_mission_card_has_no_preview_fields(fake_db):
    _seed_public(fake_db)
    app = _app()
    with app.test_client() as client, _verified(ADMIN_UID), _admin(True):
        resp = client.get("/api/mission-pool/active?init_data=x")
    card = next(m for m in resp.get_json()["missions"] if m["campaign_id"] == PUBLIC_CAMPAIGN_ID)
    assert "preview_mode" not in card
    assert "visibility" not in card
