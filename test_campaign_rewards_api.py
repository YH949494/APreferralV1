"""Tests for campaign_rewards_api.py: verified-owner-only reward visibility.

Ownership must always come from verified Telegram initData, never from a
client-supplied user_id query parameter.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import database
import campaign_centre as cc
import campaign_rewards_api as cra
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(cra, "database", database)
    monkeypatch.setattr(cc, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cra.campaign_rewards_bp)
    return app


def _mock_verified_user(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _reward(**overrides):
    base = {
        "reward_id": "rw_1",
        "campaign_id": "c1",
        "tournament_id": "t1",
        "telegram_user_id": 111,
        "rank": 1,
        "reward_label": "Champion",
        "voucher_code": "ABC123",
        "status": "assigned",
        "assigned_at": datetime.now(timezone.utc),
        "first_viewed_at": None,
        "copied_at": None,
    }
    base.update(overrides)
    return base


def test_verified_owner_sees_reward(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward())
    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().get("/api/campaign-rewards/me")
    rewards = resp.get_json()["rewards"]
    assert len(rewards) == 1
    assert rewards[0]["voucher_code"] == "ABC123"


def test_another_user_cannot_see_reward(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward(telegram_user_id=111))
    with _mock_verified_user(222), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().get("/api/campaign-rewards/me")
    assert resp.get_json()["rewards"] == []


def test_raw_user_id_query_param_does_not_grant_access(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward(telegram_user_id=111))
    with _mock_verified_user(222), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().get("/api/campaign-rewards/me?user_id=111&uid=111")
    assert resp.get_json()["rewards"] == []


def test_missing_init_data_rejected(fake_db):
    with patch("vouchers.extract_raw_init_data_from_query", return_value=""):
        resp = _app().test_client().get("/api/campaign-rewards/me")
    assert resp.status_code == 401
    assert resp.get_json()["code"] == "not_authenticated"


def test_no_rewards_hides_section(fake_db):
    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().get("/api/campaign-rewards/me")
    assert resp.get_json()["rewards"] == []


def test_expired_reward_hidden(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward(expires_at=datetime.now(timezone.utc) - timedelta(days=1)))
    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().get("/api/campaign-rewards/me")
    assert resp.get_json()["rewards"] == []


def test_pending_status_not_shown(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward(status="pending_review"))
    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().get("/api/campaign-rewards/me")
    assert resp.get_json()["rewards"] == []


def test_view_telemetry_confirms_ownership(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward())
    with _mock_verified_user(222), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().post("/api/campaign-rewards/rw_1/view")
    assert resp.status_code == 404

    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().post("/api/campaign-rewards/rw_1/view")
    assert resp.status_code == 200
    doc = fake_db["campaign_rewards"].find_one({"reward_id": "rw_1"})
    assert doc["first_viewed_at"] is not None


def test_copy_telemetry_confirms_ownership_and_does_not_change_ownership(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward())
    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        resp = _app().test_client().post("/api/campaign-rewards/rw_1/copy")
    assert resp.status_code == 200
    doc = fake_db["campaign_rewards"].find_one({"reward_id": "rw_1"})
    assert doc["copied_at"] is not None
    assert doc["telegram_user_id"] == 111


def test_reopening_mini_app_does_not_duplicate_reward(fake_db):
    fake_db["campaign_rewards"].insert_one(_reward())
    with _mock_verified_user(111), patch("vouchers.extract_raw_init_data_from_query", return_value="raw"):
        client = _app().test_client()
        client.get("/api/campaign-rewards/me")
        resp = client.get("/api/campaign-rewards/me")
    assert len(resp.get_json()["rewards"]) == 1
