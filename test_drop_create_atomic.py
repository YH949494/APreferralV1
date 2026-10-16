"""Voucher drop creation is all-or-nothing (P0 hotfix).

Root cause pinned here: create_drop_from_spec inserted the drop document FIRST
and validated/inserted the codes afterwards. An empty code list, a bad pool, a
duplicate code (db.vouchers.code is globally unique) or a personalised
assignment collision therefore answered with an error while a live-looking drop
(sometimes holding part of the codes) stayed behind, and the admin's natural
retry created a second one.

These tests run against mongomock WITH the production unique indexes, so the
database really refuses duplicates, and cover: validation before any write,
rollback of exactly what a failed call wrote, a hidden-until-published window,
concurrent requests, stale/crashed attempts and reconcile.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from flask import Flask

import vouchers
from vouchers import vouchers_bp

NOW = datetime.now(timezone.utc)


@pytest.fixture()
def mdb(monkeypatch):
    db = mongomock.MongoClient()["drop_atomic"]
    db.vouchers.create_index([("code", 1)], unique=True)
    db.vouchers.create_index(
        [("type", 1), ("dropId", 1), ("usernameLower", 1)], unique=True,
        partialFilterExpression={"type": {"$in": list(vouchers.PERSONALISED_TYPE_ALIASES)}},
    )
    monkeypatch.setattr(vouchers, "db", db)
    return db


@pytest.fixture()
def client(mdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    return app.test_client()


def _spec(**over):
    spec = {"name": "Weekend", "type": "pooled", "startsAtLocal": "2035-01-01 10:00:00", "codes": ["A1", "A2", "A3"]}
    spec.update(over)
    return spec


def _create(client, **over):
    return client.post("/v2/miniapp/admin/drops", json=_spec(**over))


def _nothing_written(db):
    assert db.drops.count_documents({}) == 0
    assert db.vouchers.count_documents({}) == 0


# ---------------------------------------------------------------- happy path
def test_pooled_success_publishes_a_clean_visible_drop(client, mdb):
    res = _create(client)
    body = res.get_json()
    assert res.status_code == 200 and body["status"] == "ok"
    drop = mdb.drops.find_one({})
    assert str(drop["_id"]) == body["dropId"]
    # Exactly the document shape the old code produced: no creation markers, not hidden.
    for field in ("creation_state", "creation_started_at", "internal_only"):
        assert field not in drop
    assert drop["status"] == "upcoming"
    assert drop["public_remaining"] == 3 and drop["my_remaining"] == 0
    rows = list(mdb.vouchers.find({"dropId": body["dropId"]}))
    assert sorted(r["code"] for r in rows) == ["A1", "A2", "A3"]
    assert all(r["status"] == "free" and r["pool"] == "public" and r["type"] == "pooled" for r in rows)


def test_my_pool_counts_go_to_my_remaining(client, mdb):
    assert _create(client, pool="my").status_code == 200
    drop = mdb.drops.find_one({})
    assert drop["my_remaining"] == 3 and drop["public_remaining"] == 0


def test_live_window_publishes_as_active(client, mdb):
    start = (datetime.now(timezone.utc) - timedelta(hours=1)).astimezone(timezone(timedelta(hours=8)))
    res = _create(client, startsAtLocal=start.strftime("%Y-%m-%d %H:%M:%S"))
    assert res.status_code == 200
    assert mdb.drops.find_one({})["status"] == "active"


def test_pooled_codes_are_normalised_and_deduped_as_before(client, mdb):
    assert _create(client, codes=[" A1 ", "A1,", "A2", "", "A2"]).status_code == 200
    assert sorted(r["code"] for r in mdb.vouchers.find({})) == ["A1", "A2"]
    assert mdb.drops.find_one({})["public_remaining"] == 2


# ------------------------------------------------- validation before any write
@pytest.mark.parametrize("over,reason", [
    ({"codes": []}, "empty_codes"),
    ({"codes": [" ", ","]}, "empty_codes"),
    ({"codes": None}, "codes_must_be_list"),
    ({"codes": "ABC"}, "codes_must_be_list"),
    ({"pool": "vip"}, "bad_pool"),
    ({"priority": "high"}, "bad_priority"),
])
def test_invalid_input_is_refused_with_nothing_written(client, mdb, over, reason):
    res = _create(client, **over)
    assert res.status_code == 400
    assert res.get_json()["reason"] == reason
    _nothing_written(mdb)


@pytest.mark.parametrize("over,code", [
    ({"name": ""}, "bad_request"), ({"startsAtLocal": "nope"}, "bad_start"),
    ({"endsAtLocal": "nope"}, "bad_end"),
    ({"endsAtLocal": "2034-01-01 10:00:00"}, "end_before_start"),
])
def test_existing_validation_errors_keep_their_codes(client, mdb, over, code):
    res = _create(client, **over)
    assert res.status_code == 400 and res.get_json()["code"] == code
    _nothing_written(mdb)


# ------------------------------------------------------------ duplicate codes
def test_code_already_used_by_another_drop_creates_nothing(client, mdb):
    first = _create(client, name="First", codes=["A1", "A2"])
    assert first.status_code == 200
    before_drops, before_vouchers = mdb.drops.count_documents({}), mdb.vouchers.count_documents({})
    res = _create(client, name="Second", codes=["B1", "A2", "B2", "A1"])
    body = res.get_json()
    assert res.status_code == 409 and body["code"] == "duplicate_code"
    assert body["inserted"] == 0 and body["duplicates"] == 2
    assert sorted(body["duplicateCodes"]) == ["A1", "A2"]
    # The orphan the old code left behind (drop + the non-duplicate codes B1/B2) must not exist.
    assert mdb.drops.count_documents({}) == before_drops
    assert mdb.vouchers.count_documents({}) == before_vouchers
    assert mdb.vouchers.count_documents({"code": {"$in": ["B1", "B2"]}}) == 0


def test_retry_after_fixing_the_duplicate_creates_exactly_one_drop(client, mdb):
    _create(client, name="First", codes=["A1"])
    assert _create(client, name="Second", codes=["A1", "B1"]).status_code == 409
    assert _create(client, name="Second", codes=["B1"]).status_code == 200
    assert mdb.drops.count_documents({"name": "Second"}) == 1


def test_duplicate_sample_is_capped(client, mdb):
    _create(client, name="First", codes=[f"X{i}" for i in range(60)])
    body = _create(client, name="Second", codes=[f"X{i}" for i in range(60)]).get_json()
    assert body["duplicates"] == 60 and len(body["duplicateCodes"]) == 20


# ---------------------------------------------------------- personalised drops
def test_personalised_success(client, mdb):
    res = _create(client, type="personalised", codes=None,
                  assignments=[{"username": "@Alice", "code": "P1"}, {"username": "bob", "code": "P2"}])
    assert res.status_code == 200
    drop = mdb.drops.find_one({})
    assert "creation_state" not in drop and "internal_only" not in drop and drop["status"] == "upcoming"
    rows = {r["usernameLower"]: r for r in mdb.vouchers.find({})}
    assert set(rows) == {"alice", "bob"} and rows["alice"]["status"] == "unclaimed"


@pytest.mark.parametrize("assignments,status,reason", [
    ([], 400, "empty_assignments"),
    (None, 400, "empty_assignments"),
    ([{"username": "", "code": "P1"}, {"code": "P2"}], 400, "empty_assignments"),
    ("alice,P1", 400, "assignments_must_be_list"),
    ([{"username": "a", "code": "P1"}, {"username": "b", "code": "P1"}], 409, None),
    ([{"username": "a", "code": "P1"}, {"username": "@A", "code": "P2"}], 400, "duplicate_username"),
])
def test_personalised_invalid_input_writes_nothing(client, mdb, assignments, status, reason):
    res = _create(client, type="personalised", codes=None, assignments=assignments)
    assert res.status_code == status
    if reason:
        assert res.get_json()["reason"] == reason
    else:
        assert res.get_json()["code"] == "duplicate_code"
    _nothing_written(mdb)


def test_personalised_code_clash_with_existing_drop_writes_nothing(client, mdb):
    _create(client, name="Pool", codes=["P1"])
    n_drops, n_v = mdb.drops.count_documents({}), mdb.vouchers.count_documents({})
    res = _create(client, type="personalised", codes=None,
                  assignments=[{"username": "a", "code": "P0"}, {"username": "b", "code": "P1"}])
    assert res.status_code == 409
    assert (mdb.drops.count_documents({}), mdb.vouchers.count_documents({})) == (n_drops, n_v)


# --------------------------------------------- failure AFTER pre-validation passed
def test_code_taken_by_a_concurrent_request_rolls_back_everything_this_call_wrote(client, mdb, monkeypatch):
    """The window pre-validation cannot close: another request inserts a code
    after the duplicate check but before our insert."""
    real = vouchers._existing_voucher_codes

    def check_then_lose_the_race(codes):
        found = real(codes)
        mdb.vouchers.insert_one({"type": "pooled", "dropId": "someone-else", "code": "A2", "status": "free"})
        return found  # [] -- the check itself saw nothing

    monkeypatch.setattr(vouchers, "_existing_voucher_codes", check_then_lose_the_race)
    res = _create(client, codes=["A1", "A2", "A3"])
    assert res.status_code == 409 and res.get_json()["code"] == "duplicate_code"
    assert mdb.drops.count_documents({}) == 0
    # A1/A3 were inserted before A2 collided; they must be gone. Only the winner's row remains.
    assert [r["code"] for r in mdb.vouchers.find({})] == ["A2"]


def test_database_error_mid_insert_rolls_back_and_reports_500(client, mdb, monkeypatch):
    monkeypatch.setattr(vouchers, "_VOUCHER_INSERT_CHUNK", 2)
    calls = {"n": 0}
    real_col = mdb.vouchers

    class Flaky:
        def __getattr__(self, name):
            return getattr(real_col, name)

        def insert_many(self, docs, ordered=True):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("connection reset")
            return real_col.insert_many(docs, ordered=ordered)

    class Db:
        def __getattr__(self, name):
            return Flaky() if name == "vouchers" else getattr(mdb, name)

    monkeypatch.setattr(vouchers, "db", Db())
    res = _create(client, codes=["A1", "A2", "A3", "A4", "A5"])
    assert res.status_code == 500 and res.get_json()["code"] == "server_error"
    assert calls["n"] == 2  # first chunk landed, second failed
    _nothing_written(mdb)


def test_drop_is_invisible_to_players_and_untouchable_while_being_created(client, mdb, monkeypatch):
    seen = {}
    real_col = mdb.vouchers

    class Spy:
        def __getattr__(self, name):
            return getattr(real_col, name)

        def insert_many(self, docs, ordered=True):
            drop = mdb.drops.find_one({})
            seen["drop"] = drop
            seen["active"] = vouchers.get_active_drops(datetime.now(timezone.utc) + timedelta(days=3650))
            seen["summary_status"] = vouchers._admin_drop_summary(drop)["status"]
            seen["add_codes"] = client.post(f"/v2/miniapp/admin/drops/{drop['_id']}/codes", json={"codes": ["Z9"]})
            seen["action"] = client.post(f"/v2/miniapp/admin/drops/{drop['_id']}/actions", json={"op": "start_now"})
            seen["delete"] = client.post(f"/v2/miniapp/admin/drops/{drop['_id']}/actions", json={"op": "delete"})
            return real_col.insert_many(docs, ordered=ordered)

    class Db:
        def __getattr__(self, name):
            return Spy() if name == "vouchers" else getattr(mdb, name)

    monkeypatch.setattr(vouchers, "db", Db())
    # far-future window so "upcoming" would otherwise be listed once active
    assert _create(client, startsAtLocal="2020-01-01 00:00:00", endsAtLocal="2045-01-01 00:00:00").status_code == 200
    d = seen["drop"]
    assert d["status"] == "paused" and d["internal_only"] is True and d["creation_state"] == "creating"
    assert seen["active"] == [], "no claim/list path may see a drop that is still being created"
    assert seen["summary_status"] == "creating"
    for key in ("add_codes", "action", "delete"):
        assert seen[key].status_code == 409 and seen[key].get_json()["code"] == "drop_creating", key
    assert mdb.vouchers.count_documents({"code": "Z9"}) == 0
    # ...and once published it IS active and claimable.
    final = mdb.drops.find_one({})
    assert final["status"] == "active" and "internal_only" not in final


def test_publish_step_fails_closed_if_the_creation_was_reconciled_mid_flight(client, mdb, monkeypatch):
    real_col = mdb.vouchers

    class Reaper:
        def __getattr__(self, name):
            return getattr(real_col, name)

        def insert_many(self, docs, ordered=True):
            out = real_col.insert_many(docs, ordered=ordered)
            # a reconcile removes the (apparently stale) drop right before publish
            mdb.drops.delete_many({})
            return out

    class Db:
        def __getattr__(self, name):
            return Reaper() if name == "vouchers" else getattr(mdb, name)

    monkeypatch.setattr(vouchers, "db", Db())
    res = _create(client)
    assert res.status_code == 500 and res.get_json()["reason"] == "creation_not_completed"
    _nothing_written(mdb)


# ----------------------------------------------- crashed attempts / reconcile
def _stale_creation(mdb, *, age_minutes, state="creating", codes=("S1", "S2"), claimed=False):
    drop_id = mdb.drops.insert_one({
        "name": "Crashed", "type": "pooled", "status": "paused", "internal_only": True, "creation_state": state,
        "creation_started_at": datetime.now(timezone.utc) - timedelta(minutes=age_minutes),
        "startsAt": NOW, "endsAt": NOW + timedelta(days=1),
    }).inserted_id
    for i, c in enumerate(codes):
        mdb.vouchers.insert_one({
            "type": "pooled", "dropId": str(drop_id), "code": c,
            "status": "claimed" if (claimed and i == 0) else "free",
            "claimedBy": "u1" if (claimed and i == 0) else None,
        })
    return drop_id


def test_retry_after_a_crash_is_not_blocked_by_the_crashed_attempts_codes(client, mdb):
    stale = _stale_creation(mdb, age_minutes=30)
    res = _create(client, codes=["S1", "S2", "S3"])
    assert res.status_code == 200, res.get_json()
    assert mdb.drops.find_one({"_id": stale}) is None
    assert mdb.drops.count_documents({}) == 1
    assert mdb.vouchers.count_documents({"dropId": res.get_json()["dropId"]}) == 3
    assert mdb.vouchers.count_documents({}) == 3


def test_failed_rollback_leaves_a_hidden_marked_drop_that_the_next_attempt_cleans_up(client, mdb, monkeypatch):
    real_col = mdb.vouchers

    class Broken:
        def __getattr__(self, name):
            return getattr(real_col, name)

        def insert_many(self, docs, ordered=True):
            real_col.insert_many(docs[:1], ordered=ordered)
            raise RuntimeError("primary stepped down")

        def delete_many(self, *a, **k):
            raise RuntimeError("still down")

    class Db:
        def __getattr__(self, name):
            return Broken() if name == "vouchers" else getattr(mdb, name)

    monkeypatch.setattr(vouchers, "db", Db())
    assert _create(client, codes=["R1", "R2"]).status_code == 500
    leftover = mdb.drops.find_one({})
    assert leftover["creation_state"] == "failed" and leftover["internal_only"] is True and leftover["status"] == "paused"
    assert vouchers.get_active_drops(NOW + timedelta(days=3650)) == []
    assert vouchers._admin_drop_summary(leftover)["status"] == "creation_failed"
    # database back: the very next attempt with the same codes succeeds and removes the leftover
    monkeypatch.setattr(vouchers, "db", mdb)
    res = _create(client, codes=["R1", "R2"])
    assert res.status_code == 200
    assert mdb.drops.count_documents({}) == 1 and mdb.vouchers.count_documents({}) == 2


def test_in_flight_creation_is_never_disturbed_by_reconcile(client, mdb):
    inflight = _stale_creation(mdb, age_minutes=1)
    res = _create(client, codes=["S1", "NEW"])
    assert res.status_code == 409 and res.get_json()["code"] == "duplicate_code"
    assert mdb.drops.find_one({"_id": inflight}) is not None
    assert mdb.vouchers.count_documents({"dropId": str(inflight)}) == 2


def test_reconcile_never_deletes_a_claimed_voucher(client, mdb):
    stale = _stale_creation(mdb, age_minutes=60, claimed=True)
    summary = vouchers.reconcile_incomplete_drop_creations()
    assert summary["protected"] == [str(stale)] and summary["removed"] == []
    assert mdb.drops.find_one({"_id": stale}) is not None
    assert mdb.vouchers.count_documents({"dropId": str(stale)}) == 2


def test_reconcile_never_touches_normal_drops(client, mdb):
    assert _create(client, codes=["N1"]).status_code == 200
    assert vouchers.reconcile_incomplete_drop_creations(stale_minutes=0) == {
        "removed": [], "in_flight": [], "protected": [], "dry_run": False}
    assert mdb.drops.count_documents({}) == 1 and mdb.vouchers.count_documents({}) == 1


def test_reconcile_endpoint_is_a_dry_run_by_default(client, mdb):
    stale = _stale_creation(mdb, age_minutes=45)
    dry = client.post("/v2/miniapp/admin/drops/reconcile-creations", json={}).get_json()
    assert dry["dry_run"] is True and dry["removed"] == [str(stale)]
    assert mdb.drops.find_one({"_id": stale}) is not None
    real = client.post("/v2/miniapp/admin/drops/reconcile-creations", json={"dryRun": False}).get_json()
    assert real["dry_run"] is False and real["removed"] == [str(stale)]
    assert mdb.drops.count_documents({}) == 0 and mdb.vouchers.count_documents({}) == 0


@pytest.mark.parametrize("window", [0, -5, 1])
def test_reconcile_endpoint_never_reclaims_a_creation_that_is_still_running(client, mdb, window):
    inflight = _stale_creation(mdb, age_minutes=0)
    res = client.post("/v2/miniapp/admin/drops/reconcile-creations", json={"dryRun": False, "staleMinutes": window}).get_json()
    assert res["removed"] == [] and res["in_flight"] == [str(inflight)]
    assert mdb.drops.find_one({"_id": inflight}) is not None
    assert mdb.vouchers.count_documents({"dropId": str(inflight)}) == 2


def test_reconcile_endpoint_with_a_small_window_still_cleans_what_is_old_enough(client, mdb):
    old = _stale_creation(mdb, age_minutes=30)
    res = client.post("/v2/miniapp/admin/drops/reconcile-creations", json={"dryRun": False, "staleMinutes": 0}).get_json()
    assert res["removed"] == [str(old)]


def test_reconcile_endpoint_requires_admin(mdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: (None, ({"status": "error"}, 403)))
    assert app.test_client().post("/v2/miniapp/admin/drops/reconcile-creations", json={}).status_code == 403


# ----------------------------------------------------------------- concurrency
def test_concurrent_creates_never_leave_partial_or_hidden_drops(mdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    shared = [f"C{i}" for i in range(20)]
    specs = []
    for t in range(8):
        # overlapping windows of the shared codes + a few private ones
        codes = shared[t * 2:t * 2 + 8] + [f"own-{t}-{i}" for i in range(3)]
        specs.append(_spec(name=f"drop-{t}", codes=codes))
    start = threading.Barrier(len(specs))
    out = [None] * len(specs)

    def run(i):
        c = app.test_client()
        start.wait()
        out[i] = c.post("/v2/miniapp/admin/drops", json=specs[i])

    threads = [threading.Thread(target=run, args=(i,)) for i in range(len(specs))]
    [t.start() for t in threads]
    [t.join(30) for t in threads]
    assert all(r is not None for r in out)
    assert {r.status_code for r in out} <= {200, 409}

    drops = list(mdb.drops.find({}))
    ok_ids = {r.get_json()["dropId"] for r in out if r.status_code == 200}
    assert {str(d["_id"]) for d in drops} == ok_ids, "a failed request left a drop behind"
    for d in drops:
        assert "creation_state" not in d and "internal_only" not in d
    # every voucher belongs to a drop that exists, each drop holds ALL of its codes, no code twice
    all_rows = list(mdb.vouchers.find({}))
    assert {r["dropId"] for r in all_rows} <= ok_ids
    assert len({r["code"] for r in all_rows}) == len(all_rows)
    for r, spec in zip(out, specs):
        if r.status_code == 200:
            held = {v["code"] for v in mdb.vouchers.find({"dropId": r.get_json()["dropId"]})}
            assert held == set(spec["codes"])
            assert mdb.drops.find_one({"name": spec["name"]})["public_remaining"] == len(spec["codes"])


def test_identical_concurrent_requests_create_at_most_one_drop(mdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    barrier = threading.Barrier(6)
    out = []

    def run():
        c = app.test_client()
        barrier.wait()
        out.append(c.post("/v2/miniapp/admin/drops", json=_spec(codes=["Q1", "Q2", "Q3"])))

    threads = [threading.Thread(target=run) for _ in range(6)]
    [t.start() for t in threads]
    [t.join(30) for t in threads]
    assert sum(r.status_code == 200 for r in out) <= 1
    assert mdb.drops.count_documents({}) == sum(r.status_code == 200 for r in out)
    assert mdb.vouchers.count_documents({}) in (0, 3)


# ------------------------------------------------------- the code builder shares
def test_builder_style_second_drop_with_the_same_codes_reports_cleanly(client, mdb):
    """campaign_builder gives every segment drop the same code list. The unique
    index refuses the 2nd; before this fix that left an empty live drop behind."""
    first, c1 = vouchers.create_drop_from_spec(_spec(name="seg-1", codes=["C1", "C2"]))
    second, c2 = vouchers.create_drop_from_spec(_spec(name="seg-2", codes=["C1", "C2"]))
    assert c1 == 200 and c2 == 409 and second["code"] == "duplicate_code"
    assert [d["name"] for d in mdb.drops.find({})] == ["seg-1"]
    assert mdb.vouchers.count_documents({}) == 2


# ------------------------------------------------------------------- add codes
def _drop_with(client, codes=("A1", "A2")):
    return _create(client, codes=list(codes)).get_json()["dropId"]


def test_add_codes_success_updates_counters(client, mdb):
    drop_id = _drop_with(client)
    res = client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": ["B1", "B2"], "pool": "my"})
    assert res.status_code == 200 and res.get_json()["inserted"] == 2
    d = mdb.drops.find_one({})
    assert d["public_remaining"] == 2 and d["my_remaining"] == 2


def test_add_codes_with_a_known_duplicate_writes_nothing(client, mdb):
    drop_id = _drop_with(client)
    other = _create(client, name="Other", codes=["Z1"]).get_json()["dropId"]
    res = client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": ["B1", "Z1", "B2"]})
    body = res.get_json()
    assert res.status_code == 409 and body["code"] == "duplicate_code" and body["inserted"] == 0
    assert body["duplicateCodes"] == ["Z1"]
    assert mdb.vouchers.count_documents({"dropId": drop_id}) == 2
    assert mdb.vouchers.count_documents({"code": {"$in": ["B1", "B2"]}}) == 0
    assert mdb.drops.find_one({"_id": __import__("bson").ObjectId(drop_id)})["public_remaining"] == 2
    assert other


@pytest.mark.parametrize("codes,reason", [(None, "codes_must_be_list"), ("A,B", "codes_must_be_list"), ([], "empty_codes")])
def test_add_codes_bad_input(client, mdb, codes, reason):
    drop_id = _drop_with(client)
    res = client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": codes})
    assert res.status_code == 400 and res.get_json()["reason"] == reason
    assert mdb.vouchers.count_documents({}) == 2
