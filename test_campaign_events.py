"""Tests for campaign_events.py: the canonical, generic, append-only
Campaign Centre event ledger — writer sanitization/idempotency, the admin
list/filter API, and the analytics summary endpoint."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import database
import campaign_events as ce
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    # No unique-key declaration for campaign_events here: in real MongoDB
    # the event_id uniqueness is a *partial* index (only documents that
    # have the field are constrained) — plain events without an event_id
    # are append-only and must never collide with each other.
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(ce, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(ce.campaign_events_bp)
    return app


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def test_emit_inserts_one_event(fake_db):
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1")
    assert fake_db["campaign_events"].count_documents({"event_type": "campaign_view"}) == 1


def test_deterministic_event_id_is_idempotent(fake_db):
    event_id = ce.deterministic_event_id("reward_created", "rw_1")
    ce.emit_campaign_event(event_type="reward_created", campaign_id="c1", reward_id="rw_1", event_id=event_id)
    ce.emit_campaign_event(event_type="reward_created", campaign_id="c1", reward_id="rw_1", event_id=event_id)
    assert fake_db["campaign_events"].count_documents({"event_id": event_id}) == 1


def test_append_only_events_without_event_id_create_separate_records(fake_db):
    ce.emit_campaign_event(event_type="campaign_click", campaign_id="c1", telegram_user_id=1)
    ce.emit_campaign_event(event_type="campaign_click", campaign_id="c1", telegram_user_id=1)
    assert fake_db["campaign_events"].count_documents({"event_type": "campaign_click"}) == 2


def test_sensitive_metadata_keys_are_removed(fake_db):
    ce.emit_campaign_event(
        event_type="provider_signature_failed",
        metadata={"provider_secret": "s3cr3t", "init_data": "raw-init-data", "safe_field": "ok"},
    )
    doc = fake_db["campaign_events"].find_one({"event_type": "provider_signature_failed"})
    assert "provider_secret" not in doc["metadata"]
    assert "init_data" not in doc["metadata"]
    assert doc["metadata"]["safe_field"] == "ok"


def test_voucher_code_key_never_persisted(fake_db):
    ce.emit_campaign_event(event_type="voucher_copied", metadata={"code": "ABC123XYZ", "voucher_code": "ABC123XYZ"})
    doc = fake_db["campaign_events"].find_one({"event_type": "voucher_copied"})
    assert "code" not in doc["metadata"]
    assert "voucher_code" not in doc["metadata"]
    assert "ABC123XYZ" not in str(doc)


def test_mask_code_suffix_never_exposes_full_code():
    assert ce.mask_code_suffix("ABCDEFGH1234") == "***1234"
    assert ce.mask_code_suffix(None) is None


def test_oversized_metadata_is_bounded(fake_db):
    huge = {"blob": "x" * 5000}
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1", metadata=huge)
    doc = fake_db["campaign_events"].find_one({"event_type": "campaign_view"})
    assert len(doc["metadata"]["blob"]) < 500  # bounded, not the full 5000 chars


def test_oversized_metadata_with_many_keys_falls_back_to_truncated_marker(fake_db):
    huge = {f"key_{i}": "x" * 250 for i in range(30)}
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1", metadata=huge)
    doc = fake_db["campaign_events"].find_one({"event_type": "campaign_view"})
    assert doc["metadata"] == {"_truncated": True}


def test_emit_never_raises_on_db_failure():
    class ExplodingDb:
        def __getitem__(self, name):
            raise RuntimeError("boom")

    with patch.object(ce, "database") as mock_db:
        mock_db.db = ExplodingDb()
        # Must not raise — a logging failure can never break the caller's
        # real business action.
        ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1")


def test_analytics_failure_does_not_fail_business_operation(fake_db, monkeypatch):
    """A broken campaign_events write (e.g. Mongo error) must never
    propagate out of emit_campaign_event — verified through
    campaign_centre.log_funnel_event, the call site every business action
    uses, with the underlying collection forced to explode."""
    import campaign_centre as cc

    class ExplodingCollection:
        def insert_one(self, *a, **k):
            raise RuntimeError("boom")

        def update_one(self, *a, **k):
            raise RuntimeError("boom")

    class ExplodingDb:
        def __getitem__(self, name):
            if name == "campaign_events":
                return ExplodingCollection()
            return fake_db[name]

    monkeypatch.setattr(database, "db", ExplodingDb())
    monkeypatch.setattr(ce, "database", database)
    # Must not raise — this is exactly the guarantee the business flow
    # (e.g. campaign publish) relies on.
    cc.log_funnel_event("campaign_view", campaign_id="c1")


def test_no_full_voucher_code_ever_stored_via_reward_flow(fake_db):
    ce.emit_campaign_event(event_type="voucher_assigned", campaign_id="c1", reward_id="rw_1", pool_id="gold")
    doc = fake_db["campaign_events"].find_one({"event_type": "voucher_assigned"})
    assert "voucher_code" not in doc
    assert "code" not in doc


# ---------------------------------------------------------------------------
# list_events / admin API
# ---------------------------------------------------------------------------

def test_list_events_filters_by_campaign_and_type(fake_db):
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1")
    ce.emit_campaign_event(event_type="campaign_click", campaign_id="c1")
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c2")
    result = ce.list_events(campaign_id="c1", event_type="campaign_view")
    assert len(result["events"]) == 1
    assert result["events"][0]["campaign_id"] == "c1"


def test_list_events_pagination(fake_db):
    for i in range(5):
        ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1", occurred_at=datetime.now(timezone.utc) + timedelta(seconds=i))
    page1 = ce.list_events(campaign_id="c1", page=1, page_size=2)
    page2 = ce.list_events(campaign_id="c1", page=2, page_size=2)
    assert len(page1["events"]) == 2
    assert len(page2["events"]) == 2
    assert page1["total"] == 5
    assert all("_id" not in e for e in page1["events"])


def test_list_events_sorts_newest_first(fake_db):
    now = datetime.now(timezone.utc)
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1", occurred_at=now - timedelta(minutes=5))
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1", occurred_at=now)
    result = ce.list_events(campaign_id="c1")
    assert result["events"][0]["occurred_at"] > result["events"][1]["occurred_at"]


def test_admin_campaign_events_endpoint_requires_auth(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: (None, ("no", 403)))
    resp = _app().test_client().get("/api/admin/campaign-events")
    assert resp.status_code == 403


def test_admin_campaign_events_endpoint_returns_filtered_results(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1")
    ce.emit_campaign_event(event_type="campaign_click", campaign_id="c1")
    resp = _app().test_client().get("/api/admin/campaign-events?campaign_id=c1&event_type=campaign_view")
    body = resp.get_json()
    assert body["status"] == "ok"
    assert len(body["events"]) == 1


def test_admin_campaign_events_endpoint_no_secrets_or_codes(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    ce.emit_campaign_event(event_type="provider_signature_failed", metadata={"provider_secret": "s3cr3t"})
    resp = _app().test_client().get("/api/admin/campaign-events")
    assert "s3cr3t" not in str(resp.get_json())


def test_admin_campaign_events_endpoint_rejects_bad_telegram_user_id(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    resp = _app().test_client().get("/api/admin/campaign-events?telegram_user_id=not-a-number")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# campaign_summary / analytics endpoint
# ---------------------------------------------------------------------------

def test_campaign_summary_calculates_rates_correctly(fake_db):
    for _ in range(10):
        ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1")
    for _ in range(4):
        ce.emit_campaign_event(event_type="campaign_click", campaign_id="c1")
    for _ in range(3):
        ce.emit_campaign_event(event_type="destination_open", campaign_id="c1")
    summary = ce.campaign_summary("c1")
    assert summary["views"] == 10
    assert summary["clicks"] == 4
    assert summary["click_through_rate"] == 0.4
    assert summary["destination_conversion_rate"] == 0.3


def test_campaign_summary_empty_returns_zeros_without_division_error(fake_db):
    summary = ce.campaign_summary("nonexistent-campaign")
    assert summary["views"] == 0
    assert summary["click_through_rate"] == 0.0
    assert summary["subscription_pass_rate"] == 0.0
    assert summary["destination_conversion_rate"] == 0.0


def test_campaign_analytics_summary_endpoint_requires_auth(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: (None, ("no", 403)))
    resp = _app().test_client().get("/api/admin/campaign-analytics/summary?campaign_id=c1")
    assert resp.status_code == 403


def test_campaign_analytics_summary_endpoint_requires_campaign_id(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    resp = _app().test_client().get("/api/admin/campaign-analytics/summary")
    assert resp.status_code == 400


def test_campaign_analytics_summary_endpoint_ok(fake_db, monkeypatch):
    monkeypatch.setattr("vouchers.require_admin", lambda: ({"id": 1}, None))
    ce.emit_campaign_event(event_type="campaign_view", campaign_id="c1")
    resp = _app().test_client().get("/api/admin/campaign-analytics/summary?campaign_id=c1")
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["views"] == 1
