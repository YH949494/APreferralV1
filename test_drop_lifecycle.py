"""Drop Start / Pause / Resume / End keep the drop's schedule (P1 hotfix).

Root cause pinned here: `start_now` rewrote the drop window unconditionally
(startsAt=now, endsAt=now+24h), so "Start" on a scheduled drop shortened or
lengthened its expiry, "Start" on an already-live drop silently extended it,
and because the API had no real resume, un-pausing a drop (the only way back
was start_now) reset its dates too.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from flask import Flask

import vouchers
from vouchers import vouchers_bp

H = timedelta(hours=1)


def _utc(d):
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


@pytest.fixture()
def mdb(monkeypatch):
    db = mongomock.MongoClient()["drop_lifecycle"]
    monkeypatch.setattr(vouchers, "db", db)
    return db


@pytest.fixture()
def client(mdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    return app.test_client()


def _drop(db, *, start_h, end_h, status="upcoming", **extra):
    now = datetime.now(timezone.utc)
    doc = {"name": "D", "type": "pooled", "status": status, "priority": 100,
           "startsAt": now + start_h * H, "endsAt": now + end_h * H}
    doc.update(extra)
    return db.drops.insert_one(doc).inserted_id


def _op(client, drop_id, op, **body):
    return client.post(f"/v2/miniapp/admin/drops/{drop_id}/actions", json={"op": op, **body})


def _schedule(db, drop_id):
    d = db.drops.find_one({"_id": drop_id})
    return _utc(d["startsAt"]), _utc(d["endsAt"]), d["status"]


# ------------------------------------------------------------------ start_now
def test_start_now_on_an_upcoming_drop_keeps_its_end(client, mdb):
    did = _drop(mdb, start_h=2, end_h=3)
    _, end0, _ = _schedule(mdb, did)
    before = datetime.now(timezone.utc)
    res = _op(client, did, "start_now")
    assert res.status_code == 200 and res.get_json()["status"] == "ok"
    start, end, status = _schedule(mdb, did)
    assert status == "active"
    assert before - timedelta(seconds=2) <= start <= datetime.now(timezone.utc) + timedelta(seconds=2)
    assert end == end0, "expiry must not be silently reset"
    assert abs((end - start) - timedelta(hours=24)) > timedelta(minutes=30), "no hidden +24h window"
    body = res.get_json()
    assert body["dropStatus"] == "active" and body["endsAt"] == end0.isoformat()


def test_start_now_on_an_already_live_drop_is_refused_and_changes_nothing(client, mdb):
    did = _drop(mdb, start_h=-1, end_h=2, status="active")
    before = _schedule(mdb, did)
    res = _op(client, did, "start_now")  # exactly what the legacy panel's Start button sends
    assert res.status_code == 409 and res.get_json()["code"] == "already_live"
    assert _schedule(mdb, did) == before


def test_start_now_on_a_paused_drop_keeps_a_start_that_already_happened(client, mdb):
    did = _drop(mdb, start_h=-5, end_h=7, status="paused")
    start0, end0, _ = _schedule(mdb, did)
    assert _op(client, did, "start_now").status_code == 200
    start, end, status = _schedule(mdb, did)
    assert (start, end, status) == (start0, end0, "active")


def test_start_now_on_a_paused_drop_that_has_not_started_moves_only_the_start(client, mdb):
    did = _drop(mdb, start_h=4, end_h=9, status="paused")
    _, end0, _ = _schedule(mdb, did)
    assert _op(client, did, "start_now").status_code == 200
    start, end, status = _schedule(mdb, did)
    assert status == "active" and end == end0
    assert start <= datetime.now(timezone.utc) + timedelta(seconds=2)


def test_start_now_on_an_ended_drop_requires_an_explicit_new_end(client, mdb):
    did = _drop(mdb, start_h=-10, end_h=-1, status="expired")
    before = _schedule(mdb, did)
    res = _op(client, did, "start_now")
    assert res.status_code == 409 and res.get_json()["code"] == "end_time_passed"
    assert _schedule(mdb, did) == before


def test_start_now_with_an_explicit_end_applies_exactly_that_end(client, mdb):
    did = _drop(mdb, start_h=-10, end_h=-1, status="expired")
    start0, _, _ = _schedule(mdb, did)
    res = _op(client, did, "start_now", endsAt="2099-01-02 03:04:05")
    assert res.status_code == 200
    start, end, status = _schedule(mdb, did)
    assert status == "active" and start == start0
    assert end == datetime(2099, 1, 1, 19, 4, 5, tzinfo=timezone.utc)  # KL = UTC+8


def test_explicit_end_can_also_change_the_end_of_an_upcoming_drop(client, mdb):
    did = _drop(mdb, start_h=2, end_h=3)
    assert _op(client, did, "start_now", endsAt="2099-06-01 00:00:00").status_code == 200
    assert _schedule(mdb, did)[1] == datetime(2099, 5, 31, 16, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("bad,code", [("not a date", "bad_end"), ("2001-01-01 00:00:00", "end_in_past")])
def test_start_now_rejects_a_bad_explicit_end_without_writing(client, mdb, bad, code):
    did = _drop(mdb, start_h=2, end_h=3)
    before = _schedule(mdb, did)
    res = _op(client, did, "start_now", endsAt=bad)
    assert res.status_code == 400 and res.get_json()["code"] == code
    assert _schedule(mdb, did) == before


# ---------------------------------------------------------------------- pause
@pytest.mark.parametrize("start_h,end_h,status", [(-1, 2, "active"), (2, 5, "upcoming")])
def test_pause_freezes_without_touching_dates(client, mdb, start_h, end_h, status):
    did = _drop(mdb, start_h=start_h, end_h=end_h, status=status)
    start0, end0, _ = _schedule(mdb, did)
    assert _op(client, did, "pause").status_code == 200
    assert _schedule(mdb, did) == (start0, end0, "paused")


def test_pause_twice_is_idempotent(client, mdb):
    did = _drop(mdb, start_h=-1, end_h=2, status="paused")
    res = _op(client, did, "pause")
    assert res.status_code == 200 and res.get_json()["unchanged"] is True


def test_pause_on_an_ended_drop_is_refused(client, mdb):
    did = _drop(mdb, start_h=-9, end_h=-1, status="expired")
    res = _op(client, did, "pause")
    assert res.status_code == 409 and res.get_json()["code"] == "drop_ended"
    assert _schedule(mdb, did)[2] == "expired"


# --------------------------------------------------------------------- resume
def test_pause_then_resume_round_trip_leaves_the_schedule_byte_identical(client, mdb):
    did = _drop(mdb, start_h=-3, end_h=5, status="active")
    raw0 = mdb.drops.find_one({"_id": did})
    assert _op(client, did, "pause").status_code == 200
    res = _op(client, did, "resume")
    assert res.status_code == 200 and res.get_json()["dropStatus"] == "active"
    raw1 = mdb.drops.find_one({"_id": did})
    assert raw1["startsAt"] == raw0["startsAt"] and raw1["endsAt"] == raw0["endsAt"]
    assert raw1["status"] == "active"


def test_resume_of_a_drop_paused_before_its_start_goes_back_to_upcoming_not_live(client, mdb):
    did = _drop(mdb, start_h=3, end_h=6, status="paused")
    start0, end0, _ = _schedule(mdb, did)
    res = _op(client, did, "resume")
    assert res.status_code == 200 and res.get_json()["dropStatus"] == "upcoming"
    assert _schedule(mdb, did) == (start0, end0, "upcoming")


def test_resume_after_the_end_passed_is_refused(client, mdb):
    did = _drop(mdb, start_h=-9, end_h=-1, status="paused")
    res = _op(client, did, "resume")
    assert res.status_code == 409 and res.get_json()["code"] == "drop_ended"
    assert _schedule(mdb, did)[2] == "paused"


@pytest.mark.parametrize("status,start_h,end_h", [("active", -1, 2), ("upcoming", 1, 2), ("expired", -5, -1)])
def test_resume_only_applies_to_paused_drops(client, mdb, status, start_h, end_h):
    did = _drop(mdb, start_h=start_h, end_h=end_h, status=status)
    before = _schedule(mdb, did)
    res = _op(client, did, "resume")
    assert res.status_code == 409 and res.get_json()["code"] == "not_paused"
    assert _schedule(mdb, did) == before


# ------------------------------------------------------------------- end_now
def test_end_now_ends_a_live_or_paused_drop_and_is_idempotent(client, mdb):
    for status in ("active", "paused"):
        did = _drop(mdb, start_h=-1, end_h=4, status=status)
        assert _op(client, did, "end_now").status_code == 200
        assert _schedule(mdb, did)[2] == "expired"
        assert _schedule(mdb, did)[1] <= datetime.now(timezone.utc) + timedelta(seconds=2)
        again = _op(client, did, "end_now")
        assert again.status_code == 200 and again.get_json()["unchanged"] is True


# --------------------------------------------------------------- concurrency
def test_a_concurrent_change_between_read_and_write_is_not_overwritten(client, mdb, monkeypatch):
    did = _drop(mdb, start_h=-1, end_h=4, status="active")
    real = mdb.drops

    class Racing:
        def __getattr__(self, name):
            return getattr(real, name)

        def update_one(self, flt, upd, *a, **k):
            # another admin ends the drop after our read, before our write
            real.update_one({"_id": did}, {"$set": {"status": "expired"}})
            return real.update_one(flt, upd, *a, **k)

    class Db:
        def __getattr__(self, name):
            return Racing() if name == "drops" else getattr(mdb, name)

    monkeypatch.setattr(vouchers, "db", Db())
    res = _op(client, did, "pause")
    assert res.status_code == 409 and res.get_json()["code"] == "drop_changed"
    assert real.find_one({"_id": did})["status"] == "expired", "the other admin's change must win"


def _racing_set_dates(mdb, monkeypatch, did, new_end):
    """Another admin runs set_dates after our read and before our write (status untouched)."""
    real = mdb.drops

    class Racing:
        def __getattr__(self, name):
            return getattr(real, name)

        def update_one(self, flt, upd, *a, **k):
            real.update_one({"_id": did}, {"$set": {"endsAt": new_end}})
            return real.update_one(flt, upd, *a, **k)

    class Db:
        def __getattr__(self, name):
            return Racing() if name == "drops" else getattr(mdb, name)

    monkeypatch.setattr(vouchers, "db", Db())


@pytest.mark.parametrize("op,status,start_h", [("start_now", "upcoming", 2), ("pause", "active", -1), ("resume", "paused", -1)])
def test_a_concurrent_set_dates_is_never_reverted_nor_slips_past_the_checks(client, mdb, monkeypatch, op, status, start_h):
    did = _drop(mdb, start_h=start_h, end_h=5, status=status)
    changed_end = datetime.now(timezone.utc) + timedelta(days=30)
    _racing_set_dates(mdb, monkeypatch, did, changed_end)
    res = _op(client, did, op)
    assert res.status_code == 409 and res.get_json()["code"] == "drop_changed"
    stored = mdb.drops.find_one({"_id": did})
    assert _utc(stored["endsAt"]) == changed_end.replace(microsecond=changed_end.microsecond // 1000 * 1000), \
        "the other admin's new end time must survive"
    assert stored["status"] == status


def test_start_now_only_writes_the_dates_it_changes(client, mdb):
    did = _drop(mdb, start_h=2, end_h=9, status="upcoming")
    _, end0, _ = _schedule(mdb, did)
    res = _op(client, did, "start_now")
    body = res.get_json()
    assert res.status_code == 200 and body["endsAt"] == end0.isoformat() and body["startsAt"]
    assert _schedule(mdb, did)[1] == end0


def test_two_simultaneous_start_requests_only_one_applies(mdb, monkeypatch):
    import threading
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    did = _drop(mdb, start_h=2, end_h=3)
    _, end0, _ = _schedule(mdb, did)
    out, gate = [], threading.Barrier(2)

    def run():
        c = app.test_client()
        gate.wait()
        out.append(c.post(f"/v2/miniapp/admin/drops/{did}/actions", json={"op": "start_now"}).status_code)

    ts = [threading.Thread(target=run) for _ in range(2)]
    [t.start() for t in ts]
    [t.join(10) for t in ts]
    assert sorted(out)[0] == 200 and set(out) <= {200, 409}
    assert _schedule(mdb, did)[1] == end0


# ------------------------------------------------------------- UI contract
@pytest.mark.parametrize("start_h,end_h,status,ops", [
    (-1, 2, "active", ["pause", "end_now"]),
    (2, 3, "upcoming", ["start_now", "pause", "end_now"]),
    (-1, 2, "paused", ["resume", "end_now"]),
    (-5, -1, "expired", []),
    (-5, -1, "active", []),  # stored "active" but the window is over
])
def test_admin_list_exposes_only_the_ops_that_make_sense(client, mdb, start_h, end_h, status, ops):
    _drop(mdb, start_h=start_h, end_h=end_h, status=status)
    items = client.get("/v2/miniapp/admin/drops").get_json()["items"]
    assert items[0]["availableOps"] == ops


def test_drop_still_being_created_offers_no_ops(client, mdb):
    _drop(mdb, start_h=-1, end_h=2, status="paused", creation_state="creating", internal_only=True)
    assert client.get("/v2/miniapp/admin/drops").get_json()["items"][0]["availableOps"] == []


def test_unknown_op_and_missing_drop_keep_their_answers(client, mdb):
    did = _drop(mdb, start_h=-1, end_h=2, status="active")
    assert _op(client, did, "explode").status_code == 400
    from bson import ObjectId
    assert _op(client, ObjectId(), "pause").status_code == 404


def test_set_dates_still_changes_dates_only_when_explicitly_asked(client, mdb):
    did = _drop(mdb, start_h=-1, end_h=2, status="active")
    res = _op(client, did, "set_dates", startsAt="2099-01-01 00:00:00", endsAt="2099-01-02 00:00:00", name="Renamed")
    assert res.status_code == 200
    d = mdb.drops.find_one({"_id": did})
    assert d["name"] == "Renamed" and _utc(d["endsAt"]) == datetime(2099, 1, 1, 16, tzinfo=timezone.utc)
