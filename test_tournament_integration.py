"""Tests for tournament_integration.py: HMAC-authenticated leaderboard
submission API, replay/staleness protection, and payload validation."""

import hashlib
import hmac as hmac_lib
import json
from datetime import datetime, timezone

import pytest
from flask import Flask

import database
import campaign_centre as cc
import campaign_providers as cp
import tournament_integration as ti
from fake_mongo import FakeDb


PROVIDER_ID = "mywin-tournament"
SECRET = "s3cr3t-hmac-key"


def _provider(**overrides):
    base = {"provider_id": PROVIDER_ID, "active": True, "secret_env_var": "TEST_PROVIDER_SECRET"}
    base.update(overrides)
    return base


def _campaign(**overrides):
    base = {
        "campaign_id": "july-tournament-2026",
        "type": "tournament",
        "status": "live",
        "destination": {"provider_id": PROVIDER_ID, "ready": True},
        "reward_config": {"rules": [{"rule_id": "rank-1", "min_rank": 1, "max_rank": 1, "pool_id": "pool-gold"}]},
    }
    base.update(overrides)
    return base


def _sign(timestamp: str, nonce: str, body: bytes, secret: str = SECRET) -> str:
    signature_input = f"{timestamp}.{nonce}.{body.decode()}"
    return hmac_lib.new(secret.encode(), signature_input.encode(), hashlib.sha256).hexdigest()


def _headers(body: bytes, *, timestamp=None, nonce="nonce-1", secret=SECRET, provider_id=PROVIDER_ID):
    timestamp = timestamp or str(int(datetime.now(timezone.utc).timestamp()))
    return {
        "X-Provider-Id": provider_id,
        "X-Timestamp": timestamp,
        "X-Nonce": nonce,
        "X-Signature": _sign(timestamp, nonce, body, secret),
        "Content-Type": "application/json",
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "tournament_results": [("provider_id", "tournament_id", "result_version")],
        "tournament_nonces": [("provider_id", "nonce")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(ti, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(cp, "database", database)
    monkeypatch.setenv("TEST_PROVIDER_SECRET", SECRET)
    fdb["gc_providers"].insert_one(_provider())
    fdb["gc_campaigns"].insert_one(_campaign())
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(ti.tournament_integration_bp)
    return app


def _payload():
    return {
        "campaign_id": "july-tournament-2026",
        "tournament_id": "mywin-july-2026",
        "result_version": 1,
        "finalized_at": "2026-07-31T16:00:00Z",
        "winners": [{"rank": 1, "telegram_user_id": 123456789, "score": 18500}],
    }


def test_valid_hmac_accepted(fake_db):
    body = json.dumps(_payload()).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 201
    assert resp.get_json()["status"] == "ok"


def test_invalid_hmac_rejected(fake_db):
    body = json.dumps(_payload()).encode()
    headers = _headers(body, secret="wrong-secret")
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=headers)
    assert resp.status_code == 401
    assert resp.get_json()["code"] == "invalid_signature"


def test_missing_headers_rejected(fake_db):
    body = json.dumps(_payload()).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body)
    assert resp.status_code == 401
    assert resp.get_json()["code"] == "missing_headers"


def test_stale_timestamp_rejected(fake_db):
    body = json.dumps(_payload()).encode()
    old_ts = str(int(datetime.now(timezone.utc).timestamp()) - 10_000)
    headers = _headers(body, timestamp=old_ts)
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=headers)
    assert resp.status_code == 401
    assert resp.get_json()["code"] == "stale_timestamp"


def test_reused_nonce_rejected(fake_db):
    body = json.dumps(_payload()).encode()
    headers = _headers(body, nonce="dupe-nonce")
    client = _app().test_client()
    first = client.post("/api/integrations/tournaments/results", data=body, headers=headers)
    assert first.status_code == 201

    payload2 = _payload()
    payload2["result_version"] = 2
    body2 = json.dumps(payload2).encode()
    headers2 = dict(headers)
    headers2["X-Signature"] = _sign(headers["X-Timestamp"], "dupe-nonce", body2)
    second = client.post("/api/integrations/tournaments/results", data=body2, headers=headers2)
    assert second.status_code == 401
    assert second.get_json()["code"] == "nonce_replayed"


def test_unknown_provider_rejected(fake_db):
    body = json.dumps(_payload()).encode()
    headers = _headers(body, provider_id="ghost-provider")
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=headers)
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "unknown_provider"


def test_inactive_provider_rejected(fake_db):
    fake_db["gc_providers"].update_one({"provider_id": PROVIDER_ID}, {"$set": {"active": False}})
    body = json.dumps(_payload()).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 401
    assert resp.get_json()["code"] == "inactive_provider"


def test_duplicate_identical_submission_returns_existing(fake_db):
    body = json.dumps(_payload()).encode()
    client = _app().test_client()
    first = client.post("/api/integrations/tournaments/results", data=body, headers=_headers(body, nonce="n1"))
    second = client.post("/api/integrations/tournaments/results", data=body, headers=_headers(body, nonce="n2"))
    assert second.status_code == 200
    assert second.get_json()["duplicate"] is True
    assert second.get_json()["submission_id"] == first.get_json()["submission_id"]


def test_same_version_different_payload_conflict(fake_db):
    client = _app().test_client()
    body1 = json.dumps(_payload()).encode()
    client.post("/api/integrations/tournaments/results", data=body1, headers=_headers(body1, nonce="n1"))

    payload2 = _payload()
    payload2["winners"][0]["score"] = 99999
    body2 = json.dumps(payload2).encode()
    resp = client.post("/api/integrations/tournaments/results", data=body2, headers=_headers(body2, nonce="n2"))
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "conflict_same_version_different_payload"


def test_lower_result_version_rejected(fake_db):
    client = _app().test_client()
    p2 = _payload()
    p2["result_version"] = 2
    body2 = json.dumps(p2).encode()
    client.post("/api/integrations/tournaments/results", data=body2, headers=_headers(body2, nonce="n1"))

    body1 = json.dumps(_payload()).encode()
    resp = client.post("/api/integrations/tournaments/results", data=body1, headers=_headers(body1, nonce="n2"))
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "lower_result_version_rejected"


def test_duplicate_telegram_uid_rejected(fake_db):
    payload = _payload()
    payload["winners"].append({"rank": 2, "telegram_user_id": 123456789, "score": 100})
    payload["reward_config"] = None
    fake_db["gc_campaigns"].update_one({"campaign_id": "july-tournament-2026"}, {"$set": {
        "reward_config": {"rules": [
            {"rule_id": "r1", "min_rank": 1, "max_rank": 1, "pool_id": "p1"},
            {"rule_id": "r2", "min_rank": 2, "max_rank": 2, "pool_id": "p2"},
        ]}
    }})
    body = json.dumps(payload).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "duplicate_telegram_uid"


def test_winner_rank_outside_reward_rules_rejected(fake_db):
    payload = _payload()
    payload["winners"] = [{"rank": 5, "telegram_user_id": 1, "score": 1}]
    body = json.dumps(payload).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "winner_rank_outside_reward_rules"


def test_provider_mismatch_rejected(fake_db):
    fake_db["gc_providers"].insert_one(_provider(provider_id="other-provider", secret_env_var="OTHER_SECRET"))
    import os
    os.environ["OTHER_SECRET"] = SECRET
    body = json.dumps(_payload()).encode()
    headers = _headers(body, provider_id="other-provider")
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=headers)
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "provider_mismatch"


def test_destination_not_ready_rejected(fake_db):
    fake_db["gc_campaigns"].update_one({"campaign_id": "july-tournament-2026"}, {"$set": {"destination.ready": False}})
    body = json.dumps(_payload()).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "destination_not_ready"


def test_inactive_campaign_rejected(fake_db):
    fake_db["gc_campaigns"].update_one({"campaign_id": "july-tournament-2026"}, {"$set": {"status": "draft"}})
    body = json.dumps(_payload()).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "campaign_not_active"


def test_too_many_winners_rejected(fake_db, monkeypatch):
    monkeypatch.setattr(ti, "MAX_WINNERS", 2)
    payload = _payload()
    payload["winners"] = [{"rank": 1, "telegram_user_id": i, "score": 1} for i in range(5)]
    fake_db["gc_campaigns"].update_one({"campaign_id": "july-tournament-2026"}, {"$set": {
        "reward_config": {"rules": [{"rule_id": "r1", "min_rank": 1, "max_rank": 1, "pool_id": "p1"}]}
    }})
    body = json.dumps(payload).encode()
    resp = _app().test_client().post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "too_many_winners"


def test_provider_never_returns_voucher_code_in_status(fake_db):
    body = json.dumps(_payload()).encode()
    client = _app().test_client()
    submit_resp = client.post("/api/integrations/tournaments/results", data=body, headers=_headers(body))
    submission_id = submit_resp.get_json()["submission_id"]

    status_headers = _headers(b"", nonce="status-check")
    resp = client.get(f"/api/integrations/tournaments/results/{submission_id}", headers=status_headers)
    assert resp.status_code == 200
    assert "voucher_code" not in resp.get_json()
    assert "code" not in json.dumps(resp.get_json())
