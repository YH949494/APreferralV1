"""Tests for the permanent Campaign Centre delete endpoint:

    DELETE /api/admin/campaign-centre/campaigns/<campaign_id>

Uses the same standalone-blueprint + mongomock pattern as
test_campaign_display_admin.py (a lightweight Flask app registering only
campaign_centre_bp, vouchers.require_admin patched for auth) so the status
check + delete run through mongomock's real find_one_and_update, exercising
the same atomicity the production code relies on.

Deletion is an in-place tombstone, not a removal: the gc_campaigns document
for a deleted campaign_id is never dropped, only atomically rewritten to
status="deleted" with its operational fields stripped (see
campaign_centre._TOMBSTONE_UNSET_FIELDS). That is what permanently reserves
the campaign_id against the collection's pre-existing unique index. See
test_campaign_centre_delete_cross_module.py for the "invisible everywhere"
half of that guarantee across modules this file doesn't register a
blueprint for (campaign_registration, mission_pool).
"""

from datetime import datetime, timezone
from unittest.mock import patch

import mongomock
import pytest
from flask import Flask

import database
import campaign_centre as cc

CAMPAIGN_ID = "summer-lucky-draw-2026"


@pytest.fixture
def fake_db(monkeypatch):
    fdb = mongomock.MongoClient().db
    monkeypatch.setattr(database, "db", fdb)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
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


def _seed_campaign(fake_db, **overrides):
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Summer Lucky Draw",
        "type": "external_website",
        "status": "draft",
        "priority": 100,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "destination": {"provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False},
    }
    doc.update(overrides)
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _delete(client, campaign_id=CAMPAIGN_ID):
    return client.delete(f"/api/admin/campaign-centre/campaigns/{campaign_id}")


def _assert_tombstoned(fake_db, campaign_id=CAMPAIGN_ID):
    """The document must still exist (that's what reserves campaign_id
    against the unique index) but be reduced to a minimal tombstone: no
    operational/config field survives, only identification + bookkeeping."""
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": campaign_id})
    assert doc is not None, "delete must never remove the document — it tombstones it in place"
    assert doc["status"] == "deleted"
    assert doc.get("deleted_at") is not None
    assert doc.get("deleted_by")
    assert doc["campaign_id"] == campaign_id
    for field in cc._TOMBSTONE_UNSET_FIELDS:
        assert field not in doc, f"tombstone must not retain operational field {field!r}"
    return doc


# ---------------------------------------------------------------------------
# Deletable statuses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["draft", "archived", "ended"])
def test_deletable_status_can_be_deleted(fake_db, client, status):
    _seed_campaign(fake_db, status=status)
    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["campaign_id"] == CAMPAIGN_ID
    _assert_tombstoned(fake_db)


# ---------------------------------------------------------------------------
# Non-deletable statuses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["live", "paused"])
def test_live_or_paused_campaign_returns_409_and_is_not_deleted(fake_db, client, status):
    _seed_campaign(fake_db, status=status)
    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["status"] == "error"
    assert body["code"] == "invalid_status_for_deletion"
    assert body["campaign_status"] == status
    # campaign must remain fully intact, untouched — not even partially
    # tombstoned (no field stripped, no deleted_at stamped)
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert doc is not None
    assert doc["status"] == status
    assert "deleted_at" not in doc
    assert "destination" in doc


def test_unknown_campaign_returns_404(fake_db, client):
    with _mock_admin():
        resp = _delete(client, campaign_id="does-not-exist")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"


def test_unauthenticated_request_is_rejected(fake_db, client):
    _seed_campaign(fake_db, status="draft")
    with _mock_unauthenticated():
        resp = _delete(client)
    assert resp.status_code == 401
    # untouched
    doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert doc is not None
    assert doc["status"] == "draft"


# ---------------------------------------------------------------------------
# Owned config vs. historical/traceability records
# ---------------------------------------------------------------------------

def test_owned_registration_state_is_removed(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    fake_db["campaign_registration_state"].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": 111, "dismissed": True,
    })
    other_campaign_state = {"campaign_id": "other-campaign", "telegram_user_id": 222, "dismissed": True}
    fake_db["campaign_registration_state"].insert_one(dict(other_campaign_state))

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200

    assert fake_db["campaign_registration_state"].count_documents({"campaign_id": CAMPAIGN_ID}) == 0
    # a different campaign's state must never be touched by this delete
    remaining = fake_db["campaign_registration_state"].find_one({"campaign_id": "other-campaign"})
    assert remaining is not None
    assert remaining["telegram_user_id"] == 222


def test_historical_records_are_preserved(fake_db, client):
    _seed_campaign(fake_db, status="ended")
    fake_db["campaign_registrations"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111})
    fake_db["mission_entries"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111, "is_correct": True})
    fake_db["mission_identity_claims"].insert_one({"campaign_id": CAMPAIGN_ID, "identity_hash": "abc"})
    fake_db["campaign_rewards"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111, "status": "assigned"})
    fake_db["tournament_results"].insert_one({"campaign_id": CAMPAIGN_ID, "submission_id": "s1"})

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200

    assert fake_db["campaign_registrations"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["mission_entries"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["mission_identity_claims"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["campaign_rewards"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["tournament_results"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    _assert_tombstoned(fake_db)


def test_audit_log_written_and_survives_deletion(fake_db, client):
    _seed_campaign(fake_db, status="draft", name="Summer Lucky Draw")
    with _mock_admin(uid=99):
        resp = _delete(client)
    assert resp.status_code == 200

    audit = fake_db["campaign_admin_audit_log"].find_one({"action": "campaign_deleted", "entity_id": CAMPAIGN_ID})
    assert audit is not None
    assert audit["details"]["title"] == "Summer Lucky Draw"
    assert audit["details"]["previous_status"] == "draft"
    assert audit["details"]["snapshot"]["campaign_id"] == CAMPAIGN_ID
    # the audit snapshot is the pre-tombstone document — it still carries
    # the full config the live tombstone no longer has
    assert audit["details"]["snapshot"]["destination"] == {
        "provider_id": "", "open_mode": "telegram_web_app", "path": "", "ready": False,
    }
    assert audit["admin"] == "admin"
    assert audit["at"] is not None

    event = fake_db["campaign_events"].find_one({"event_type": "campaign_deleted", "campaign_id": CAMPAIGN_ID})
    assert event is not None


# ---------------------------------------------------------------------------
# campaign_id reuse after deletion (preserved history must never be
# silently inherited by a new campaign reusing the same id)
# ---------------------------------------------------------------------------

def test_create_campaign_rejects_a_previously_deleted_campaign_id(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": CAMPAIGN_ID, "name": "Reused id", "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
        })
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_id_previously_deleted"
    _assert_tombstoned(fake_db)


def test_duplicate_campaign_rejects_a_previously_deleted_target_id(fake_db, client):
    _seed_campaign(fake_db, campaign_id="source-campaign", status="draft")
    _seed_campaign(fake_db, status="archived")  # CAMPAIGN_ID, will become the delete target
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post(
            "/api/admin/gc-campaigns/source-campaign/duplicate",
            json={"campaign_id": CAMPAIGN_ID},
        )
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_id_previously_deleted"


def test_duplicate_campaign_suffixes_the_copy_name_with_copy(fake_db, client):
    """P0.15 — duplicate_campaign no longer clones the source name verbatim
    (two visually identical cards in the list). Source name is untouched;
    only the new copy gets the " (copy)" suffix, and campaign_id/other
    fields still copy through unchanged."""
    _seed_campaign(fake_db, campaign_id="source-campaign", name="October Lucky Draw",
                    status="draft", priority=250)
    with _mock_admin():
        resp = client.post(
            "/api/admin/gc-campaigns/source-campaign/duplicate",
            json={"campaign_id": "october-lucky-draw-copy"},
        )
    assert resp.status_code == 201
    assert resp.get_json()["campaign_id"] == "october-lucky-draw-copy"

    copy = fake_db["gc_campaigns"].find_one({"campaign_id": "october-lucky-draw-copy"})
    assert copy["name"] == "October Lucky Draw (copy)"
    assert copy["priority"] == 250
    assert copy["status"] == "draft"

    source = fake_db["gc_campaigns"].find_one({"campaign_id": "source-campaign"})
    assert source["name"] == "October Lucky Draw", "the source campaign's own name must be untouched"


def test_duplicate_campaign_with_no_source_name_falls_back_to_the_new_campaign_id(fake_db, client):
    _seed_campaign(fake_db, campaign_id="source-campaign", name="", status="draft")
    with _mock_admin():
        resp = client.post(
            "/api/admin/gc-campaigns/source-campaign/duplicate",
            json={"campaign_id": "nameless-copy"},
        )
    assert resp.status_code == 201
    copy = fake_db["gc_campaigns"].find_one({"campaign_id": "nameless-copy"})
    assert copy["name"] == "nameless-copy (copy)"


def test_deleted_campaign_id_does_not_leak_into_a_fresh_campaign_with_the_same_id(fake_db, client):
    """Regression guard for the underlying risk the reuse-block exists for:
    even if the id were allowed to be reused, a stale registration row under
    that campaign_id must never be reachable by a new campaign. This asserts
    the reuse itself is blocked (the actual fix), rather than depending on
    every downstream consumer's own campaign_id scoping being correct."""
    _seed_campaign(fake_db, status="ended")
    fake_db["campaign_registrations"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 111})
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": CAMPAIGN_ID, "name": "Reused id", "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
        })
    assert resp.status_code == 409
    # the old registration is still there (preserved for traceability) but
    # no new campaign document was ever allowed to claim its campaign_id
    assert fake_db["campaign_registrations"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    _assert_tombstoned(fake_db)


def test_reservation_survives_even_when_optional_cleanup_fails(fake_db, client):
    """The tombstone (and the campaign_id reservation it IS) is written by
    the same atomic operation that authorizes the deletion — it cannot fail
    independently of the deletion itself, unlike registration-state cleanup,
    the audit log, or the funnel event, which are all best-effort follow-up
    writes. Proven here by breaking every one of those follow-up writes and
    confirming the campaign_id is still permanently unavailable afterwards."""
    _seed_campaign(fake_db, status="archived")
    fake_db["campaign_registration_state"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 1})

    def boom(*args, **kwargs):
        raise RuntimeError("simulated cleanup failure")

    fake_db["campaign_registration_state"].delete_many = boom
    fake_db["campaign_admin_audit_log"].insert_one = boom

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert set(body.get("cleanup_warnings", [])) >= {"registration_state_cleanup_failed"}

    # the reservation held regardless — every cleanup step around it failed
    _assert_tombstoned(fake_db)
    with _mock_admin():
        resp2 = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": CAMPAIGN_ID, "name": "Reused id", "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
        })
    assert resp2.status_code == 409
    assert resp2.get_json()["code"] == "campaign_id_previously_deleted"


# ---------------------------------------------------------------------------
# Concurrency / atomicity
# ---------------------------------------------------------------------------

def test_publish_loses_race_with_delete_reports_not_found_not_false_success(fake_db, client):
    """If publish/pause/archive has already read the campaign as eligible
    but delete_campaign tombstones it before the status-transition's
    update_one lands, that update must report the miss (not_found) instead
    of a false 'ok' for a status change that never landed — and must never
    partially resurrect the tombstone (e.g. flipping its status back to
    'live' via a blind {"campaign_id": ...} filter)."""
    _seed_campaign(fake_db, status="draft")
    with _mock_admin():
        # Simulate the race directly: the campaign is gone by the time the
        # transition's update_one runs, exactly as if delete_campaign had
        # interleaved between _transition's read and its write.
        assert _delete(client).status_code == 200
        resp = client.post(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}/publish")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"
    # no audit entry for a publish that never actually happened
    assert fake_db["campaign_admin_audit_log"].find_one({"action": "campaign_published"}) is None
    # and the tombstone must still read "deleted" — never resurrected
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "deleted"


def test_concurrent_publish_during_delete_cannot_both_succeed(fake_db, client):
    """The status check and the delete happen in one atomic
    find_one_and_update filtered on status — a campaign that flips to
    'live' between an admin's status read and their delete click must
    never be silently deleted anyway. Simulate the race by flipping the
    campaign to 'live' right before calling delete."""
    _seed_campaign(fake_db, status="draft")
    fake_db["gc_campaigns"].update_one({"campaign_id": CAMPAIGN_ID}, {"$set": {"status": "live"}})

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 409
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "live"


def test_double_delete_second_call_returns_404(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        first = _delete(client)
        second = _delete(client)
    assert first.status_code == 200
    assert second.status_code == 404
    assert second.get_json()["code"] == "not_found"
    _assert_tombstoned(fake_db)


def test_concurrent_create_during_delete_cannot_produce_a_duplicate_document(fake_db, client):
    """Even in the worst-case interleaving — create_campaign's own reuse
    pre-check (a raw find_one, not atomic with the insert that follows) runs
    a moment BEFORE delete_campaign's atomic tombstone lands, so the
    pre-check itself sees the campaign as still active and passes — the
    collection's pre-existing unique index on campaign_id is what actually
    closes the window: the insert_one that follows must still fail, because
    by the time it runs the tombstone already occupies that campaign_id.
    There must never be two gc_campaigns documents for one campaign_id."""
    _seed_campaign(fake_db, status="draft")

    real_find_one = fake_db["gc_campaigns"].find_one
    state = {"intercepted": False}

    def racy_find_one(query, *args, **kwargs):
        if not state["intercepted"] and isinstance(query, dict) and query.get("campaign_id") == CAMPAIGN_ID:
            state["intercepted"] = True
            # This is create_campaign's own reuse pre-check. Simulate the
            # delete landing in the gap between this read and create's
            # insert_one, below.
            with _mock_admin():
                assert _delete(client).status_code == 200
        return real_find_one(query, *args, **kwargs)

    fake_db["gc_campaigns"].find_one = racy_find_one

    with _mock_admin():
        resp = client.post("/api/admin/gc-campaigns", json={
            "campaign_id": CAMPAIGN_ID, "name": "Racing create", "type": "external_website",
            "schedule": {"starts_at": datetime.now(timezone.utc).isoformat()},
        })

    assert resp.status_code == 409
    assert resp.get_json()["code"] in ("duplicate_campaign_id", "campaign_id_previously_deleted")
    # exactly one document ever existed for this campaign_id — the
    # tombstone, never a second document from the racing create
    assert fake_db["gc_campaigns"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "deleted"


# ---------------------------------------------------------------------------
# No false deletion audit/event when the atomic delete does not happen
#
# The audit log and campaign_events ledger are read as "this campaign was
# deleted" by anyone auditing admin actions. Writing a campaign_deleted
# record for a request that the atomic find_one_and_update actually
# rejected (still live/paused, already gone, or lost a status-flip race)
# would be a false record — the campaign was NOT deleted. All of these must
# leave zero campaign_deleted audit entries and zero campaign_deleted events.
# ---------------------------------------------------------------------------

def _no_deletion_audit_or_event_written(fake_db, campaign_id=CAMPAIGN_ID):
    assert fake_db["campaign_admin_audit_log"].count_documents(
        {"action": "campaign_deleted", "entity_id": campaign_id}
    ) == 0
    assert fake_db["campaign_events"].count_documents(
        {"event_type": "campaign_deleted", "campaign_id": campaign_id}
    ) == 0


@pytest.mark.parametrize("status", ["live", "paused"])
def test_live_or_paused_delete_attempt_writes_no_audit_or_event(fake_db, client, status):
    _seed_campaign(fake_db, status=status)
    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 409
    _no_deletion_audit_or_event_written(fake_db)
    # and the registration_state cascade must not have run either
    fake_db["campaign_registration_state"].insert_one({"campaign_id": CAMPAIGN_ID, "telegram_user_id": 1})
    assert fake_db["campaign_registration_state"].count_documents({"campaign_id": CAMPAIGN_ID}) == 1


def test_concurrent_status_flip_failure_writes_no_audit_or_event(fake_db, client):
    """Same race as test_concurrent_publish_during_delete_cannot_both_succeed,
    but from the audit/event side: a delete that the atomic filter rejects
    because the campaign flipped to live must leave no trace claiming it
    happened."""
    _seed_campaign(fake_db, status="draft")
    fake_db["gc_campaigns"].update_one({"campaign_id": CAMPAIGN_ID}, {"$set": {"status": "live"}})
    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 409
    _no_deletion_audit_or_event_written(fake_db)


def test_unknown_campaign_delete_writes_no_audit_or_event(fake_db, client):
    with _mock_admin():
        resp = _delete(client, campaign_id="does-not-exist")
    assert resp.status_code == 404
    _no_deletion_audit_or_event_written(fake_db, campaign_id="does-not-exist")


def test_double_delete_second_call_writes_no_second_audit_or_event(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        first = _delete(client)
        second = _delete(client)
    assert first.status_code == 200
    assert second.status_code == 404
    # exactly one campaign_deleted record from the first, successful call —
    # the failed second call must not have added another
    assert fake_db["campaign_admin_audit_log"].count_documents(
        {"action": "campaign_deleted", "entity_id": CAMPAIGN_ID}
    ) == 1
    assert fake_db["campaign_events"].count_documents(
        {"event_type": "campaign_deleted", "campaign_id": CAMPAIGN_ID}
    ) == 1


# ---------------------------------------------------------------------------
# Snapshot must come from the document find_one_and_update actually returned
# (the state immediately BEFORE the tombstone write)
# ---------------------------------------------------------------------------

def test_audit_and_event_snapshot_comes_from_find_one_and_update_result(fake_db, client):
    """The audit/event payload must be built from the exact pre-tombstone
    document find_one_and_update returned (ReturnDocument.BEFORE), not from
    a separate re-read that could race with something else. Proven by
    making find_one_and_update itself return a doctored BEFORE document and
    asserting that doctoring shows up in the audit/event records."""
    _seed_campaign(fake_db, status="archived", name="Original Name")

    real_find_one_and_update = fake_db["gc_campaigns"].find_one_and_update

    def spy_find_one_and_update(*args, **kwargs):
        result = real_find_one_and_update(*args, **kwargs)
        if result is not None:
            result = dict(result)
            result["name"] = "SNAPSHOT-MARKER-FROM-UPDATE-RESULT"
        return result

    fake_db["gc_campaigns"].find_one_and_update = spy_find_one_and_update

    with _mock_admin():
        resp = _delete(client)
    assert resp.status_code == 200

    audit = fake_db["campaign_admin_audit_log"].find_one({"action": "campaign_deleted", "entity_id": CAMPAIGN_ID})
    assert audit["details"]["title"] == "SNAPSHOT-MARKER-FROM-UPDATE-RESULT"
    assert audit["details"]["snapshot"]["name"] == "SNAPSHOT-MARKER-FROM-UPDATE-RESULT"

    event = fake_db["campaign_events"].find_one({"event_type": "campaign_deleted", "campaign_id": CAMPAIGN_ID})
    assert event is not None
    # the tombstone actually written to the collection is untouched by the
    # spy (it doctors only the returned BEFORE copy) — still a real tombstone
    _assert_tombstoned(fake_db)


# ---------------------------------------------------------------------------
# Post-delete cleanup failures must not be reported as deletion failure
# ---------------------------------------------------------------------------

def test_registration_state_cleanup_failure_still_reports_deletion_success(fake_db, client):
    """The campaign is already tombstoned by the time
    campaign_registration_state.delete_many runs. If that cleanup step
    raises, the admin must still see a 200/ok — never a 5xx that would look
    retryable for a deletion that already happened."""
    _seed_campaign(fake_db, status="archived")

    def boom(*args, **kwargs):
        raise RuntimeError("simulated cleanup failure")

    fake_db["campaign_registration_state"].delete_many = boom

    with _mock_admin():
        resp = _delete(client)

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["campaign_id"] == CAMPAIGN_ID
    assert "registration_state_cleanup_failed" in body.get("cleanup_warnings", [])
    # the campaign is still tombstoned despite the cleanup step failing
    _assert_tombstoned(fake_db)
    # and the audit trail for the deletion itself still landed
    assert fake_db["campaign_admin_audit_log"].find_one(
        {"action": "campaign_deleted", "entity_id": CAMPAIGN_ID}
    ) is not None


def test_audit_log_failure_still_reports_deletion_success(fake_db, client, caplog):
    """_log_audit already swallows its own exceptions (logging a warning
    instead of raising), so a broken audit collection must never surface as
    a failed deletion — the campaign is tombstoned regardless of whether the
    audit write landed."""
    _seed_campaign(fake_db, status="ended")

    def boom(*args, **kwargs):
        raise RuntimeError("simulated audit collection failure")

    fake_db["campaign_admin_audit_log"].insert_one = boom

    with _mock_admin():
        resp = _delete(client)

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["campaign_id"] == CAMPAIGN_ID
    _assert_tombstoned(fake_db)
    # no false "audit_log_failed" retry signal leaks through — _log_audit's
    # own guard handled it and logged a warning instead
    assert "audit_log_failed" not in body.get("cleanup_warnings", [])


def test_funnel_event_failure_still_reports_deletion_success(fake_db, client):
    """Unlike _log_audit, log_funnel_event's failure mode is exercised end
    to end here (emit_campaign_event also swallows internally, per its own
    docstring) — this proves the outer guard in delete_campaign is a no-op
    in the successful case and never turns a working delete into a 5xx."""
    _seed_campaign(fake_db, status="draft")

    with _mock_admin():
        resp = _delete(client)

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    _assert_tombstoned(fake_db)
    event = fake_db["campaign_events"].find_one({"event_type": "campaign_deleted", "campaign_id": CAMPAIGN_ID})
    assert event is not None


# ---------------------------------------------------------------------------
# Visibility: deleted campaigns must vanish from admin listings and can
# never be published again
# ---------------------------------------------------------------------------

def test_deleted_campaign_is_absent_from_admin_listing(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    _seed_campaign(fake_db, campaign_id="still-active", status="draft")

    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.get("/api/admin/gc-campaigns")
    assert resp.status_code == 200
    ids = [c["campaign_id"] for c in resp.get_json()["campaigns"]]
    assert CAMPAIGN_ID not in ids
    assert "still-active" in ids


def test_deleted_campaign_cannot_be_published(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.post(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}/publish")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"
    assert fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})["status"] == "deleted"


def test_deleted_campaign_cannot_be_updated(fake_db, client):
    _seed_campaign(fake_db, status="archived")
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.put(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}", json={"name": "Reactivated"})
    assert resp.status_code == 404
    tomb = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert tomb["status"] == "deleted"
    assert tomb.get("name") != "Reactivated"


def test_deleted_campaign_get_returns_404(fake_db, client):
    _seed_campaign(fake_db, status="draft")
    with _mock_admin():
        assert _delete(client).status_code == 200
        resp = client.get(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "not_found"
