"""Manual joins_count + qualified_count on Campaign Display Control
participants (campaign_display_override.py).

Covers: server-side validation (non-negative ints only, qualified <= joins),
legacy rows without joins (unknown, never zero, never inferred), supplying
joins to a legacy row, conversion = qualified / joins only when joins > 0,
identical values on the leaderboard / preview / announcement text, joins
totals that refuse to pose as complete when any row's joins are unknown,
stable-id identity (duplicate names never merge), disable/expiry removing the
overlay, and that nothing outside the display-override collections is touched.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import mongomock
import pytest
from flask import Flask

import campaign_display_override as cdo
import database
from campaign_display_override import (
    CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION,
    build_public_campaign_activity,
    load_active_campaign_override,
    public_campaign_activity_view,
    render_campaign_activity_announcement_text,
)

CAMPAIGN_ID = "referral_oct_2026"
BASE = f"/api/admin/campaign-display/campaigns/{CAMPAIGN_ID}"


@pytest.fixture
def fake_db(monkeypatch):
    fdb = mongomock.MongoClient().db
    monkeypatch.setattr(database, "db", fdb)
    return fdb


@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(cdo.campaign_display_admin_bp)
    return app.test_client()


def _admin():
    return patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None))


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def _create_campaign(client, *, starts_delta=-1, ends_delta=1):
    now = datetime.now(timezone.utc)
    with _admin():
        resp = client.post(
            "/api/admin/campaign-display/campaigns",
            json={
                "campaign_id": CAMPAIGN_ID,
                "starts_at": _iso(now + timedelta(days=starts_delta)),
                "ends_at": _iso(now + timedelta(days=ends_delta)),
                "enabled": True,
            },
        )
    assert resp.status_code == 201
    return resp


def _add(client, **body):
    body.setdefault("display_name", "A***n")
    body.setdefault("visible", True)
    with _admin():
        return client.post(f"{BASE}/participants", json=body)


def _patch(client, entry_id, **body):
    with _admin():
        return client.patch(f"{BASE}/participants/{entry_id}", json=body)


def _participants(resp):
    return resp.get_json()["campaign"]["participants"]


def _activity(fake_db):
    return build_public_campaign_activity(fake_db, CAMPAIGN_ID)


def _row(activity, name):
    return next(r for r in activity["leaderboard"] if r["display_name"] == name)


def _seed_genuine(fake_db, referrer_id, *, joins, qualified, first_name="GenuineFan"):
    """`joins` pending_referrals cohort rows, the first `qualified` of which
    also have a qualified_event -- the real aggregation computes the rest."""
    now = datetime.now(timezone.utc)
    fake_db.users.insert_one({"user_id": referrer_id, "username": None, "first_name": first_name})
    for i in range(joins):
        invitee = referrer_id * 1000 + i
        fake_db.pending_referrals.insert_one(
            {"inviter_user_id": referrer_id, "invitee_user_id": invitee, "created_at_utc": now}
        )
        if i < qualified:
            fake_db.qualified_events.insert_one(
                {"invitee_id": invitee, "referrer_id": referrer_id, "qualified_at": now}
            )


# ---------------------------------------------------------------------------
# Create / validation
# ---------------------------------------------------------------------------

def test_manual_30_joins_20_qualified_displays_everywhere(client, fake_db):
    _create_campaign(client)
    resp = _add(client, joins_count=30, qualified_count=20)
    assert resp.status_code == 201
    p = _participants(resp)[0]
    assert (p["joins_count"], p["qualified_count"], p["conversion_rate"]) == (30, 20, 0.6667)

    activity = _activity(fake_db)
    row = _row(activity, "A***n")
    assert (row["joins_count"], row["qualified_count"], row["conversion_rate"]) == (30, 20, 0.6667)
    assert activity["qualified_total"] == 20
    assert activity["joins_total"] == 30 and activity["joins_total_complete"] is True

    text = render_campaign_activity_announcement_text(activity)
    assert "A***n — 30 joins · 20 qualified invites · 66.7% conversion" in text


def test_zero_joins_zero_qualified_is_accepted_and_conversion_is_dash(client, fake_db):
    _create_campaign(client)
    assert _add(client, joins_count=0, qualified_count=0).status_code == 201
    row = _row(_activity(fake_db), "A***n")
    assert (row["joins_count"], row["qualified_count"], row["conversion_rate"]) == (0, 0, None)
    text = render_campaign_activity_announcement_text(_activity(fake_db))
    assert "A***n — 0 joins · 0 qualified invites · — conversion" in text


def test_zero_joins_with_qualified_is_rejected_with_clear_message(client, fake_db):
    _create_campaign(client)
    resp = _add(client, joins_count=0, qualified_count=20)
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["code"] == "qualified_exceeds_joins"
    assert "cannot be greater than joins" in body["message"]
    assert fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"] == []


def test_equal_joins_and_qualified_is_accepted(client, fake_db):
    _create_campaign(client)
    assert _add(client, joins_count=5, qualified_count=5).status_code == 201


def test_qualified_greater_than_joins_rejected_on_update(client, fake_db):
    _create_campaign(client)
    entry_id = _participants(_add(client, joins_count=30, qualified_count=20))[0]["entry_id"]

    # Raise qualified above the stored joins (joins not in the payload).
    resp = _patch(client, entry_id, qualified_count=31)
    assert resp.status_code == 400 and resp.get_json()["code"] == "qualified_exceeds_joins"
    # Lower joins below the stored qualified.
    resp = _patch(client, entry_id, joins_count=19)
    assert resp.status_code == 400 and resp.get_json()["code"] == "qualified_exceeds_joins"
    # Both together, consistently, succeeds.
    resp = _patch(client, entry_id, joins_count=19, qualified_count=19)
    assert resp.status_code == 200
    stored = fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"][0]
    assert (stored["joins_count"], stored["qualified_count"]) == (19, 19)


@pytest.mark.parametrize("bad", [True, False, 2.5, 20.0, "3", "", -1, 10 ** 9, [], {}])
def test_invalid_joins_rejected_server_side_on_create_and_update(client, fake_db, bad):
    _create_campaign(client)
    resp = _add(client, joins_count=bad, qualified_count=0)
    assert resp.status_code == 400, f"joins_count={bad!r} should be rejected on create"
    assert resp.get_json()["code"] in {"bad_joins_count_type", "joins_count_out_of_range"}

    entry_id = _participants(_add(client, joins_count=5, qualified_count=1))[0]["entry_id"]
    resp = _patch(client, entry_id, joins_count=bad)
    assert resp.status_code == 400, f"joins_count={bad!r} should be rejected on update"
    stored = fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"][0]
    assert stored["joins_count"] == 5


@pytest.mark.parametrize("bad", [True, 2.5, "3", -1, 10 ** 9])
def test_invalid_qualified_still_rejected_when_joins_supplied(client, fake_db, bad):
    _create_campaign(client)
    assert _add(client, joins_count=50, qualified_count=bad).status_code == 400


# ---------------------------------------------------------------------------
# Legacy rows: unknown joins
# ---------------------------------------------------------------------------

def _seed_legacy(fake_db, **overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "_id": CAMPAIGN_ID,
        "campaign_id": CAMPAIGN_ID,
        "enabled": True,
        "starts_at": now - timedelta(days=1),
        "ends_at": now + timedelta(days=1),
        "participants": [
            {"entry_id": "legacy-1", "display_name": "L***y", "qualified_count": 7, "visible": True},
        ],
        "created_at": now,
        "updated_at": now.replace(microsecond=0),
    }
    doc.update(overrides)
    fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].insert_one(doc)


def test_legacy_row_without_joins_shows_unknown_not_zero(fake_db):
    _seed_legacy(fake_db)
    activity = _activity(fake_db)
    row = _row(activity, "L***y")
    assert row["joins_count"] is None and row["conversion_rate"] is None
    assert row["qualified_count"] == 7  # joins NOT invented from qualified
    text = render_campaign_activity_announcement_text(activity)
    assert "L***y — — joins · 7 qualified invites · — conversion" in text


def test_joins_total_is_not_presented_as_complete_when_a_row_is_unknown(fake_db):
    _seed_legacy(
        fake_db,
        participants=[
            {"entry_id": "legacy-1", "display_name": "L***y", "qualified_count": 7, "visible": True},
            {"entry_id": "m-known", "display_name": "K***n", "joins_count": 30, "qualified_count": 20, "visible": True},
        ],
    )
    activity = _activity(fake_db)
    assert activity["qualified_total"] == 27
    assert activity["joins_total"] is None
    assert activity["joins_total_complete"] is False
    assert activity["joins_known_total"] == 30 and activity["joins_unknown_count"] == 1
    # The partial sum is admin-only; the public view never carries it.
    public = public_campaign_activity_view(activity)
    assert public["joins_total"] is None and public["joins_total_complete"] is False
    assert "joins_known_total" not in public and "joins_unknown_count" not in public


def test_hidden_legacy_row_does_not_make_the_total_partial(fake_db):
    _seed_legacy(
        fake_db,
        participants=[
            {"entry_id": "legacy-1", "display_name": "L***y", "qualified_count": 7, "visible": False},
            {"entry_id": "m-known", "display_name": "K***n", "joins_count": 30, "qualified_count": 20, "visible": True},
        ],
    )
    activity = _activity(fake_db)
    assert activity["joins_total"] == 30 and activity["joins_total_complete"] is True


def test_admin_serialization_marks_legacy_joins_unknown(client, fake_db):
    _seed_legacy(fake_db)
    with _admin():
        body = client.get(BASE).get_json()
    p = body["campaign"]["participants"][0]
    assert p["joins_count"] is None and p["conversion_rate"] is None


def test_editing_a_legacy_row_to_add_joins_works(client, fake_db):
    _seed_legacy(fake_db)
    resp = _patch(client, "legacy-1", joins_count=10)
    assert resp.status_code == 200
    p = _participants(resp)[0]
    assert (p["joins_count"], p["qualified_count"], p["conversion_rate"]) == (10, 7, 0.7)
    assert _row(_activity(fake_db), "L***y")["joins_count"] == 10


def test_editing_a_legacy_row_with_joins_below_qualified_is_rejected(client, fake_db):
    _seed_legacy(fake_db)
    resp = _patch(client, "legacy-1", joins_count=6)
    assert resp.status_code == 400 and resp.get_json()["code"] == "qualified_exceeds_joins"
    assert "joins_count" not in fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"][0]


def test_editing_other_fields_of_a_legacy_row_keeps_joins_unknown(client, fake_db):
    _seed_legacy(fake_db)
    assert _patch(client, "legacy-1", qualified_count=9).status_code == 200
    stored = fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"][0]
    assert stored["qualified_count"] == 9
    assert "joins_count" not in stored  # not coerced to 0 / null / qualified


def test_add_without_joins_remains_backward_compatible_and_unknown(client, fake_db):
    _create_campaign(client)
    resp = _add(client, qualified_count=4)
    assert resp.status_code == 201
    assert _participants(resp)[0]["joins_count"] is None


def test_hand_edited_document_with_qualified_over_joins_is_dropped_on_read(fake_db):
    _seed_legacy(
        fake_db,
        participants=[
            {"entry_id": "bad", "display_name": "B***d", "joins_count": 1, "qualified_count": 5, "visible": True},
            {"entry_id": "ok", "display_name": "O***k", "joins_count": 5, "qualified_count": 1, "visible": True},
        ],
    )
    result = load_active_campaign_override(fake_db, CAMPAIGN_ID)
    assert [p["entry_id"] for p in result["participants"]] == ["ok"]


# ---------------------------------------------------------------------------
# Genuine rows, ranking, identity
# ---------------------------------------------------------------------------

def test_genuine_row_10_joins_4_qualified_is_unchanged(client, fake_db):
    _create_campaign(client)
    _seed_genuine(fake_db, 501, joins=10, qualified=4)
    _add(client, display_name="M***l", joins_count=30, qualified_count=20)
    activity = _activity(fake_db)
    genuine = _row(activity, "GenuineFan")
    assert (genuine["joins_count"], genuine["qualified_count"], genuine["conversion_rate"]) == (10, 4, 0.4)
    assert activity["qualified_total"] == 24
    assert activity["joins_total"] == 40 and activity["joins_total_complete"] is True
    genuine_ranked = next(r for r in activity["_combined_rows"] if r["_source"] == "genuine")
    assert genuine_ranked["entry_id"] == "genuine:501"


def test_ranking_tiebreaks_on_joins_then_conversion_like_genuine_rows(client, fake_db):
    _create_campaign(client)
    _add(client, display_name="Low", joins_count=10, qualified_count=5)
    _add(client, display_name="High", joins_count=40, qualified_count=5)
    _add(client, display_name="Top", joins_count=6, qualified_count=6)
    names = [r["display_name"] for r in _activity(fake_db)["leaderboard"]]
    assert names == ["Top", "High", "Low"]


def test_legacy_ranking_is_unchanged_by_the_new_field(fake_db):
    _seed_legacy(
        fake_db,
        participants=[
            {"entry_id": "b", "display_name": "Beta", "qualified_count": 5, "visible": True},
            {"entry_id": "a", "display_name": "Alpha", "qualified_count": 5, "visible": True},
            {"entry_id": "c", "display_name": "Top", "qualified_count": 9, "visible": True},
        ],
    )
    assert [r["display_name"] for r in _activity(fake_db)["leaderboard"]] == ["Top", "Alpha", "Beta"]


def test_duplicate_display_names_do_not_merge_or_overwrite(client, fake_db):
    _create_campaign(client)
    _seed_genuine(fake_db, 501, joins=10, qualified=4, first_name="Dup")
    first = _participants(_add(client, display_name="Dup", joins_count=30, qualified_count=20))[0]["entry_id"]
    second = _participants(_add(client, display_name="Dup", joins_count=8, qualified_count=2))[1]["entry_id"]
    assert first != second

    assert _patch(client, second, joins_count=9, qualified_count=3).status_code == 200
    rows = [r for r in _activity(fake_db)["leaderboard"] if r["display_name"] == "Dup"]
    assert sorted((r["joins_count"], r["qualified_count"]) for r in rows) == [(9, 3), (10, 4), (30, 20)]
    stored = {p["entry_id"]: p for p in fake_db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})["participants"]}
    assert (stored[first]["joins_count"], stored[first]["qualified_count"]) == (30, 20)


# ---------------------------------------------------------------------------
# Surfaces agree; lifecycle; isolation
# ---------------------------------------------------------------------------

def test_preview_endpoint_and_announcement_generation_use_identical_values(client, fake_db):
    _create_campaign(client)
    _add(client, display_name="M***l", joins_count=30, qualified_count=20)
    with _admin():
        preview = client.get(f"{BASE}/preview").get_json()
    activity = _activity(fake_db)
    # Preview text IS the shared renderer's output for the shared builder's result.
    assert preview["preview_text"] == render_campaign_activity_announcement_text(activity)
    assert preview["leaderboard"] == activity["leaderboard"]
    assert (preview["joins_total"], preview["qualified_total"]) == (30, 20)
    assert "30 joins · 20 qualified invites · 66.7% conversion" in preview["preview_text"]


def test_disable_and_expiry_remove_the_overlay(client, fake_db):
    _create_campaign(client)
    _add(client, joins_count=30, qualified_count=20)
    assert _activity(fake_db)["state"] == "active"

    with _admin():
        client.put(BASE, json={"enabled": False})
    disabled = _activity(fake_db)
    assert disabled["state"] == "genuine_only" and disabled["leaderboard"] == []
    assert disabled["joins_total"] == 0 and disabled["qualified_total"] == 0

    with _admin():
        client.put(BASE, json={"enabled": True})
    assert _activity(fake_db)["state"] == "active"
    after_expiry = build_public_campaign_activity(
        fake_db, CAMPAIGN_ID, reference_utc=datetime.now(timezone.utc) + timedelta(days=3)
    )
    assert after_expiry["state"] == "genuine_only"
    assert all(r["_source"] != "manual" for r in after_expiry["_combined_rows"])


def test_manual_joins_never_touch_genuine_or_reward_collections(client, fake_db):
    guarded = [
        "qualified_events", "pending_referrals", "referral_events", "referral_flow_events",
        "affiliate_ledger", "affiliate_rewards", "voucher_pools", "xp_events", "users",
    ]
    _seed_genuine(fake_db, 501, joins=10, qualified=4)
    snapshot = {n: sorted(map(str, fake_db[n].find({}))) for n in guarded}

    _create_campaign(client)
    entry_id = _participants(_add(client, joins_count=30, qualified_count=20))[0]["entry_id"]
    _patch(client, entry_id, joins_count=40, qualified_count=25)
    _patch(client, entry_id, joins_count=1, qualified_count=2)  # rejected
    _activity(fake_db)

    assert {n: sorted(map(str, fake_db[n].find({}))) for n in guarded} == snapshot
    touched = {n for n in fake_db.list_collection_names() if n not in guarded}
    assert touched <= {CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION, "campaign_admin_audit_log"}
