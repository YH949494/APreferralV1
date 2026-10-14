"""Mission Reward Pool schedule timezone contract.

Admin inputs are Asia/Kuala_Lumpur (UTC+8) wall-clock time; the database
holds UTC; every admin API response carries an explicit UTC offset so no
client ever has to guess a zone.

Root cause this guards (production): PyMongo returns naive-UTC datetimes and
``campaign_centre._serialize`` emitted them with a bare ``isoformat()``
("2026-10-14T14:00:00", no offset). Browsers parse such strings as LOCAL
time, so a GMT+8 admin saw the stored 14:00Z instant as 14:00 instead of
22:00 — exactly 8 hours early. The stored value was always correct.

The browser-side half (fixed GMT+8 conversion independent of the browser
zone) is covered by test_mission_schedule_timezone.test.js.
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

KL = timezone(timedelta(hours=8))
STARTS_UTC = datetime(2026, 10, 14, 14, 0, 0, tzinfo=timezone.utc)   # 22:00 GMT+8
ENDS_UTC = datetime(2026, 10, 16, 2, 0, 0, tzinfo=timezone.utc)      # 10:00 GMT+8


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb({"gc_campaigns": [("campaign_id",)]})
    monkeypatch.setattr(database, "db", fdb)
    for module in (mp, ux, cc):
        monkeypatch.setattr(module, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _client():
    app = Flask(__name__)
    app.register_blueprint(cc.campaign_centre_bp)
    app.register_blueprint(ux.mission_pool_ux_admin_bp)
    return app.test_client()


def _admin():
    return patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "ops"}, None))


def _as_pymongo_would_return(fdb, campaign_id):
    """PyMongo (tz_aware=False, as in database.py) hands back naive UTC."""
    doc = fdb["gc_campaigns"].find_one({"campaign_id": campaign_id})
    sched = dict(doc["schedule"])
    for k in ("starts_at", "ends_at"):
        if sched.get(k) is not None:
            sched[k] = sched[k].astimezone(timezone.utc).replace(tzinfo=None)
    fdb["gc_campaigns"].update_one({"campaign_id": campaign_id}, {"$set": {"schedule": sched}})


def _mission_body(campaign_id="kl-mission", schedule=None, **extra):
    body = {
        "campaign_id": campaign_id,
        "name": "KL Mission",
        "type": "mission_pool",
        "schedule": schedule or {"starts_at": "2026-10-14T22:00:00+08:00",
                                 "ends_at": "2026-10-16T10:00:00+08:00"},
        "destination": {"open_mode": "telegram_web_app", "ready": False},
        "telegram": {"channel_username": ""},
        "mission_config": {
            "mission_type": "multiple_choice",
            "prompt": "Which feature do you prefer?",
            "options": [{"id": "a", "label": "Free Spins"}, {"id": "b", "label": "Cashback"}],
            "correct_answer": "a",
        },
        "mission_pool": {
            "pool_id": "POOL-1", "pool_type": "voucher_drop", "winner_count": 3,
            "allocation_method": "random_qualified",
        },
    }
    body.update(extra)
    return body


def _create(client, **kw):
    resp = client.post("/api/admin/gc-campaigns", json=_mission_body(**kw))
    assert resp.status_code == 201, resp.get_json()


def _stored(fdb, campaign_id="kl-mission"):
    return fdb["gc_campaigns"].find_one({"campaign_id": campaign_id})


def _utc(dt):
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# GMT+8 input -> UTC storage
# --------------------------------------------------------------------------

@pytest.mark.parametrize("starts,ends", [
    ("2026-10-14T14:00:00Z", "2026-10-16T02:00:00Z"),
    ("2026-10-14T14:00:00+00:00", "2026-10-16T02:00:00+00:00"),
    ("2026-10-14T22:00:00+08:00", "2026-10-16T10:00:00+08:00"),
    ("2026-10-14T14:00:00.000Z", "2026-10-16T02:00:00.000Z"),  # JS toISOString()
])
def test_create_stores_the_exact_utc_instants(fake_db, starts, ends):
    with _admin():
        _create(_client(), schedule={"starts_at": starts, "ends_at": ends})
    sched = _stored(fake_db)["schedule"]
    assert _utc(sched["starts_at"]) == STARTS_UTC
    assert _utc(sched["ends_at"]) == ENDS_UTC
    assert sched["timezone"] == "Asia/Kuala_Lumpur"


# --------------------------------------------------------------------------
# Serialization: explicit UTC offset, whatever PyMongo returns
# --------------------------------------------------------------------------

@pytest.mark.parametrize("stored", [
    datetime(2026, 10, 14, 14, 0, 0),                       # naive (PyMongo read)
    datetime(2026, 10, 14, 14, 0, 0, tzinfo=timezone.utc),  # aware UTC
    datetime(2026, 10, 14, 22, 0, 0, tzinfo=KL),            # aware non-UTC
])
def test_serialize_always_carries_an_explicit_utc_offset(stored):
    out = cc._serialize({"_id": "x", "schedule": {"starts_at": stored, "ends_at": None}})
    assert out["schedule"]["starts_at"] == "2026-10-14T14:00:00+00:00"
    assert out["schedule"]["ends_at"] is None


def test_serialize_does_not_mutate_the_stored_document():
    doc = {"_id": "x", "schedule": {"starts_at": datetime(2026, 10, 14, 14, 0)}}
    cc._serialize(doc)
    assert isinstance(doc["schedule"]["starts_at"], datetime)


def test_reload_after_save_reads_back_as_malaysia_2200(fake_db):
    client = _client()
    with _admin():
        _create(client)
        _as_pymongo_would_return(fake_db, "kl-mission")
        data = client.get("/api/admin/gc-campaigns/kl-mission").get_json()
        listed = client.get("/api/admin/gc-campaigns").get_json()
    for payload in (data["campaign"] if "campaign" in data else data,
                    next(c for c in (listed.get("campaigns") or listed.get("items") or [])
                         if c["campaign_id"] == "kl-mission")):
        s = payload["schedule"]
        # Parsed as an absolute instant (what any JS Date now does) and
        # shown in KL: 22:00 / 10:00, not 14:00 / 02:00.
        assert datetime.fromisoformat(s["starts_at"]).astimezone(KL).strftime("%Y-%m-%d %H:%M") == "2026-10-14 22:00"
        assert datetime.fromisoformat(s["ends_at"]).astimezone(KL).strftime("%Y-%m-%d %H:%M") == "2026-10-16 10:00"
        assert datetime.fromisoformat(s["starts_at"]).utcoffset() == timedelta(0)


# --------------------------------------------------------------------------
# Edit / repeated save / other-field edits never drift the schedule
# --------------------------------------------------------------------------

def test_resaving_the_served_schedule_repeatedly_is_a_fixed_point(fake_db):
    client = _client()
    with _admin():
        _create(client)
        for _ in range(4):
            _as_pymongo_would_return(fake_db, "kl-mission")
            served = client.get("/api/admin/gc-campaigns/kl-mission").get_json()
            served = served.get("campaign", served)
            r = client.put("/api/admin/gc-campaigns/kl-mission", json={
                "schedule": {"starts_at": served["schedule"]["starts_at"],
                             "ends_at": served["schedule"]["ends_at"]}})
            assert r.status_code == 200, r.get_json()
    sched = _stored(fake_db)["schedule"]
    assert _utc(sched["starts_at"]) == STARTS_UTC
    assert _utc(sched["ends_at"]) == ENDS_UTC


def test_legacy_naive_utc_string_resend_is_read_as_utc(fake_db):
    """An older client may resend the bare string the old serializer produced;
    it was always naive-UTC, so it must keep meaning UTC (Rule 8)."""
    client = _client()
    with _admin():
        _create(client)
        r = client.put("/api/admin/gc-campaigns/kl-mission", json={"schedule": {
            "starts_at": "2026-10-14T14:00:00", "ends_at": "2026-10-16T02:00:00"}})
    assert r.status_code == 200
    sched = _stored(fake_db)["schedule"]
    assert _utc(sched["starts_at"]) == STARTS_UTC
    assert _utc(sched["ends_at"]) == ENDS_UTC


def test_editing_other_settings_leaves_schedule_and_state_untouched(fake_db):
    client = _client()
    with _admin():
        _create(client)
        fake_db["gc_campaigns"].update_one({"campaign_id": "kl-mission"}, {"$set": {
            "mission_pool.processing_stage": mp.STAGE_PENDING,
            "mission_pool.closed_at": ENDS_UTC,
        }})
        before = _stored(fake_db)
        r = client.put("/api/admin/gc-campaigns/kl-mission", json={"name": "Renamed"})
    assert r.status_code == 200
    after = _stored(fake_db)
    assert after["name"] == "Renamed"
    assert after["schedule"] == before["schedule"]
    assert after["status"] == before["status"]
    assert after["mission_pool"] == before["mission_pool"]
    assert after["_id"] == before["_id"]


def test_editing_schedule_changes_only_the_schedule(fake_db):
    client = _client()
    with _admin():
        _create(client)
        before = _stored(fake_db)
        r = client.put("/api/admin/gc-campaigns/kl-mission", json={"schedule": {
            "starts_at": "2026-10-15T09:30:00+08:00", "ends_at": "2026-10-16T10:00:00+08:00"}})
    assert r.status_code == 200
    after = _stored(fake_db)
    assert _utc(after["schedule"]["starts_at"]) == datetime(2026, 10, 15, 1, 30, tzinfo=timezone.utc)
    assert _utc(after["schedule"]["ends_at"]) == ENDS_UTC
    for key in ("status", "mission_pool", "mission_config", "campaign_id", "type"):
        assert after[key] == before[key]


# --------------------------------------------------------------------------
# Cross-midnight / cross-month / cross-year
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kl_start,kl_end,utc_start,utc_end", [
    # GMT+8 00:30 is the previous UTC day.
    ("2026-10-15T00:30:00+08:00", "2026-10-15T07:59:00+08:00",
     datetime(2026, 10, 14, 16, 30), datetime(2026, 10, 14, 23, 59)),
    # month boundary
    ("2026-10-31T23:00:00+08:00", "2026-11-01T01:00:00+08:00",
     datetime(2026, 10, 31, 15, 0), datetime(2026, 10, 31, 17, 0)),
    # year boundary; KL is already 2027 while UTC is still 2026
    ("2026-12-31T23:30:00+08:00", "2027-01-01T08:00:00+08:00",
     datetime(2026, 12, 31, 15, 30), datetime(2027, 1, 1, 0, 0)),
    # leap day
    ("2028-02-29T07:00:00+08:00", "2028-03-01T07:00:00+08:00",
     datetime(2028, 2, 28, 23, 0), datetime(2028, 2, 29, 23, 0)),
])
def test_boundary_crossing_schedules_round_trip(fake_db, kl_start, kl_end, utc_start, utc_end):
    client = _client()
    with _admin():
        _create(client, schedule={"starts_at": kl_start, "ends_at": kl_end})
        _as_pymongo_would_return(fake_db, "kl-mission")
        served = client.get("/api/admin/gc-campaigns/kl-mission").get_json()
    served = served.get("campaign", served)["schedule"]
    assert datetime.fromisoformat(served["starts_at"]) == utc_start.replace(tzinfo=timezone.utc)
    assert datetime.fromisoformat(served["ends_at"]) == utc_end.replace(tzinfo=timezone.utc)
    assert datetime.fromisoformat(served["starts_at"]).astimezone(KL) == datetime.fromisoformat(kl_start)


# --------------------------------------------------------------------------
# Invalid / missing / ordering
# --------------------------------------------------------------------------

@pytest.mark.parametrize("schedule,code", [
    ({"starts_at": None, "ends_at": "2026-10-16T02:00:00Z"}, "missing_starts_at"),
    ({"ends_at": "2026-10-16T02:00:00Z"}, "missing_starts_at"),
    ({"starts_at": "not-a-date", "ends_at": "2026-10-16T02:00:00Z"}, "missing_starts_at"),
    ({"starts_at": "2026-10-16T02:00:00Z", "ends_at": "2026-10-14T14:00:00Z"}, "ends_at_before_starts_at"),
    ({"starts_at": "2026-10-14T14:00:00Z", "ends_at": "2026-10-14T14:00:00Z"}, "ends_at_before_starts_at"),
    # equal instants expressed in different zones are still equal
    ({"starts_at": "2026-10-14T22:00:00+08:00", "ends_at": "2026-10-14T14:00:00Z"}, "ends_at_before_starts_at"),
])
def test_invalid_schedules_are_rejected_and_nothing_is_written(fake_db, schedule, code):
    with _admin():
        resp = _client().post("/api/admin/gc-campaigns", json=_mission_body(schedule=schedule))
    assert resp.status_code == 400
    assert resp.get_json()["code"] == code
    assert _stored(fake_db) is None


def test_invalid_edit_leaves_stored_schedule_unchanged(fake_db):
    client = _client()
    with _admin():
        _create(client)
        r = client.put("/api/admin/gc-campaigns/kl-mission", json={"schedule": {
            "starts_at": "2026-10-16T10:00:00+08:00", "ends_at": "2026-10-14T22:00:00+08:00"}})
    assert r.status_code == 400
    sched = _stored(fake_db)["schedule"]
    assert _utc(sched["starts_at"]) == STARTS_UTC
    assert _utc(sched["ends_at"]) == ENDS_UTC


# --------------------------------------------------------------------------
# Scheduling executes at the right UTC instants; state fields are unaffected
# --------------------------------------------------------------------------

def _live_campaign(fake_db):
    with _admin():
        _create(_client())
    fake_db["gc_campaigns"].update_one({"campaign_id": "kl-mission"}, {"$set": {"status": "live"}})
    _as_pymongo_would_return(fake_db, "kl-mission")
    return _stored(fake_db)


@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 10, 14, 13, 59, 59, tzinfo=timezone.utc), (False, "campaign_not_started")),
    (datetime(2026, 10, 14, 14, 0, 0, tzinfo=timezone.utc), (True, "open")),     # 22:00 GMT+8
    (datetime(2026, 10, 15, 6, 0, 0, tzinfo=timezone.utc), (True, "open")),
    (datetime(2026, 10, 16, 1, 59, 59, tzinfo=timezone.utc), (True, "open")),
    (datetime(2026, 10, 16, 2, 0, 0, tzinfo=timezone.utc), (False, "campaign_closed")),  # 10:00 GMT+8
])
def test_mission_opens_and_closes_at_the_utc_instants(fake_db, now, expected):
    campaign = _live_campaign(fake_db)
    assert mp.submission_state(campaign, now) == expected


def test_close_cutoff_and_processing_gate_use_the_saved_ends_at(fake_db):
    campaign = _live_campaign(fake_db)
    assert mp.close_cutoff(campaign) == ENDS_UTC
    assert not mp.is_closed_for_processing(campaign, ENDS_UTC - timedelta(seconds=1))
    assert mp.is_closed_for_processing(campaign, ENDS_UTC)


def test_admin_summary_scheduled_close_has_explicit_offset(fake_db):
    campaign = _live_campaign(fake_db)
    summary = mp._random_pool_summary(campaign, campaign.get("mission_pool") or {})
    assert summary["scheduled_close_at"] == "2026-10-16T02:00:00+00:00"
