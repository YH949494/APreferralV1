"""Server-side idempotency for admin writes (P1).

Root cause pinned here: nothing on the server recognised a retried admin write.
After a lost response (timeout, dropped connection, double click, second tab) the
natural retry either ran the write twice (XP granted twice once the cooldown
lapsed) or answered with a confusing error for work that had succeeded
(``duplicate_code`` for a drop that was in fact created).

Runs against mongomock with the production unique indexes.
"""
from __future__ import annotations

import ast
import threading
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from flask import Flask, jsonify, request

import admin_idempotency as idem
import vouchers
from vouchers import vouchers_bp


# ------------------------------------------------------------------ module
@pytest.fixture()
def mdb():
    return mongomock.MongoClient()["idem"]


def _run(mdb, key="key-0001-abcd", payload=None, handler=None, **kw):
    calls = kw.pop("calls", None)

    def default():
        if calls is not None:
            calls.append(1)
        return {"status": "ok", "n": 1}, 200

    return idem.run(mdb, scope="s", key=key, payload=payload or {"a": 1}, handler=handler or default, **kw)


def test_first_call_runs_and_retry_replays_without_running_again(mdb):
    calls = []
    b1, s1, h1 = _run(mdb, calls=calls)
    b2, s2, h2 = _run(mdb, calls=calls)
    assert (b1, s1, h1) == ({"status": "ok", "n": 1}, 200, {})
    assert (b2, s2) == (b1, s1)
    assert h2 == {"Idempotent-Replay": "true"}
    assert len(calls) == 1


def test_same_key_different_payload_is_refused_not_guessed(mdb):
    _run(mdb, payload={"a": 1})
    body, status, _ = _run(mdb, payload={"a": 2})
    assert status == 422 and body["code"] == "idempotency_key_reused"


def test_key_fields_do_not_change_the_fingerprint():
    assert idem.fingerprint({"a": 1, "idempotency_key": "x"}) == idem.fingerprint({"a": 1})
    assert idem.fingerprint({"a": 1, "b": 2}) == idem.fingerprint({"b": 2, "a": 1})


def test_failed_attempt_releases_the_key_so_a_retry_runs(mdb):
    calls = []

    def failing():
        calls.append("f")
        return {"status": "error", "code": "duplicate_code"}, 409

    assert _run(mdb, handler=failing)[1] == 409
    assert mdb[idem.COLLECTION].count_documents({}) == 0
    body, status, _ = _run(mdb, calls=calls)
    assert status == 200 and calls == ["f", 1]


def test_exception_releases_the_key_and_propagates(mdb):
    def boom():
        raise RuntimeError("db down")

    with pytest.raises(RuntimeError):
        _run(mdb, handler=boom)
    assert mdb[idem.COLLECTION].count_documents({}) == 0
    assert _run(mdb)[1] == 200


def test_concurrent_duplicate_while_running_is_refused_with_409(mdb):
    inner = {}

    def slow():
        inner["res"] = _run(mdb)  # the "second tab" arrives while the first is mid-flight
        return {"status": "ok"}, 200

    body, status, _ = _run(mdb, handler=slow)
    assert status == 200
    b, s, headers = inner["res"]
    assert s == 409 and b["code"] == "request_in_progress" and headers.get("Retry-After")


def test_threaded_race_executes_the_handler_once(mdb):
    calls, results = [], []
    gate = threading.Barrier(6)

    def handler():
        calls.append(1)
        return {"status": "ok"}, 200

    def worker():
        gate.wait()
        results.append(_run(mdb, handler=handler)[1])

    threads = [threading.Thread(target=worker) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(calls) == 1
    assert sorted(results).count(200) >= 1 and set(results) <= {200, 409}


def test_abandoned_lease_is_taken_over_and_recover_gives_exactly_once(mdb):
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    mdb[idem.COLLECTION].insert_one({
        "_id": "s:key-0001-abcd", "scope": "s", "key": "key-0001-abcd", "fingerprint": idem.fingerprint({"a": 1}),
        "state": idem.IN_PROGRESS, "owner": "dead", "created_at": past, "lease_expires_at": past,
        "expires_at": past + timedelta(days=1),
    })
    calls = []
    body, status, headers = _run(
        mdb, calls=calls, recover=lambda: ({"status": "ok", "dropId": "already"}, 200),
    )
    assert body["dropId"] == "already" and status == 200 and calls == []
    assert headers == {"Idempotent-Replay": "true"}
    # and the recovered answer is now the stored one
    assert _run(mdb, calls=calls)[0]["dropId"] == "already"


def test_abandoned_lease_without_recovery_runs_the_handler(mdb):
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    mdb[idem.COLLECTION].insert_one({
        "_id": "s:key-0001-abcd", "fingerprint": idem.fingerprint({"a": 1}), "state": idem.IN_PROGRESS,
        "owner": "dead", "lease_expires_at": past, "expires_at": past + timedelta(days=1),
    })
    calls = []
    assert _run(mdb, calls=calls)[1] == 200 and calls == [1]


def test_live_lease_is_not_taken_over(mdb):
    soon = datetime.now(timezone.utc) + timedelta(minutes=1)
    mdb[idem.COLLECTION].insert_one({
        "_id": "s:key-0001-abcd", "fingerprint": idem.fingerprint({"a": 1}), "state": idem.IN_PROGRESS,
        "owner": "alive", "lease_expires_at": soon, "expires_at": soon,
    })
    body, status, _ = _run(mdb)
    assert status == 409 and body["code"] == "request_in_progress"


def test_store_outage_fails_open(mdb):
    class Broken:
        def __getitem__(self, name):
            raise AttributeError("no db")

    calls = []
    body, status, headers = idem.run(
        Broken(), scope="s", key="key-0001-abcd", payload={}, handler=lambda: (calls.append(1) or {"ok": 1}, 200),
    )
    assert status == 200 and calls == [1] and headers == {}


def test_should_cache_override_for_http_200_failures(mdb):
    calls = []

    def soft_fail():
        calls.append(1)
        return {"success": False}, 200

    kw = dict(should_cache=lambda b, s: bool(b.get("success")))
    _run(mdb, handler=soft_fail, **kw)
    _run(mdb, handler=soft_fail, **kw)
    assert calls == [1, 1]  # not cached, so the retry really ran again


@pytest.mark.parametrize("bad", ["short", "has space here", "x" * 200, "semi;colon-key"])
def test_bad_keys_are_rejected(bad):
    app = Flask(__name__)
    with app.test_request_context(headers={"Idempotency-Key": bad}):
        key, err = idem.extract_key(request, None)
    assert key is None and err["code"] == "bad_idempotency_key"


def test_header_wins_over_body_and_no_key_means_none():
    app = Flask(__name__)
    with app.test_request_context(headers={"Idempotency-Key": "header-key-0001"}):
        assert idem.extract_key(request, {"idempotency_key": "body-key-00001"})[0] == "header-key-0001"
    with app.test_request_context():
        assert idem.extract_key(request, {"idempotency_key": "body-key-00001"})[0] == "body-key-00001"
        assert idem.extract_key(request, {}) == (None, None)


def test_ensure_indexes_never_raises():
    class Bad:
        def __getitem__(self, name):
            raise RuntimeError("nope")

    idem.ensure_indexes(Bad())


# ------------------------------------------------------------------ routes
@pytest.fixture()
def vdb(monkeypatch):
    db = mongomock.MongoClient()["idem_routes"]
    db.vouchers.create_index([("code", 1)], unique=True)
    monkeypatch.setattr(vouchers, "db", db)
    return db


@pytest.fixture()
def client(vdb, monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(vouchers_bp, url_prefix="/v2/miniapp")
    monkeypatch.setattr(vouchers, "require_admin", lambda: ({"usernameLower": "boss"}, None))
    return app.test_client()


SPEC = {"name": "Weekend", "type": "pooled", "startsAtLocal": "2035-01-01 10:00:00", "codes": ["A1", "A2"]}
KEY = {"Idempotency-Key": "create-0001-abcd"}


def test_retried_create_returns_the_same_drop_instead_of_duplicate_code(client, vdb):
    first = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    retry = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    assert first.status_code == retry.status_code == 200
    assert retry.get_json()["dropId"] == first.get_json()["dropId"]
    assert retry.headers.get("Idempotent-Replay") == "true"
    assert vdb.drops.count_documents({}) == 1 and vdb.vouchers.count_documents({}) == 2


def test_create_without_key_behaves_exactly_as_before(client, vdb):
    assert client.post("/v2/miniapp/admin/drops", json=SPEC).status_code == 200
    again = client.post("/v2/miniapp/admin/drops", json=SPEC)
    assert again.status_code == 409 and again.get_json()["code"] == "duplicate_code"
    assert vdb.drops.count_documents({}) == 1


def test_body_key_is_honoured_like_the_header(client, vdb):
    body = dict(SPEC, idempotency_key="body-key-0001")
    a = client.post("/v2/miniapp/admin/drops", json=body).get_json()
    b = client.post("/v2/miniapp/admin/drops", json=body).get_json()
    assert a["dropId"] == b["dropId"] and vdb.drops.count_documents({}) == 1


def test_key_reuse_with_different_spec_is_422_and_writes_nothing_new(client, vdb):
    client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    res = client.post("/v2/miniapp/admin/drops", json=dict(SPEC, codes=["Z9"]), headers=KEY)
    assert res.status_code == 422 and res.get_json()["code"] == "idempotency_key_reused"
    assert vdb.drops.count_documents({}) == 1 and vdb.vouchers.count_documents({}) == 2


def test_bad_key_is_400(client, vdb):
    res = client.post("/v2/miniapp/admin/drops", json=SPEC, headers={"Idempotency-Key": "short"})
    assert res.status_code == 400 and res.get_json()["code"] == "bad_idempotency_key"
    assert vdb.drops.count_documents({}) == 0


def test_failed_create_is_retryable_with_the_same_key(client, vdb):
    vdb.vouchers.insert_one({"code": "A2", "type": "pooled", "dropId": "other", "status": "free"})
    bad = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    assert bad.status_code == 409
    vdb.vouchers.delete_one({"code": "A2"})
    ok = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    assert ok.status_code == 200 and vdb.drops.count_documents({}) == 1


def test_lost_idempotency_record_still_cannot_create_a_second_drop(client, vdb):
    """Exactly-once survives the idempotency store being wiped/expired: the drop carries the key."""
    first = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY).get_json()
    vdb[idem.COLLECTION].delete_many({})
    again = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    assert again.status_code == 200 and again.get_json()["dropId"] == first["dropId"]
    assert vdb.drops.count_documents({}) == 1


def test_key_for_a_drop_still_being_created_is_not_answered_as_done(client, vdb):
    vdb.drops.insert_one({
        "name": "x", "admin_request_key": "create-0001-abcd", "creation_state": "creating",
        "status": "paused", "internal_only": True,
    })
    res = client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    assert res.status_code == 409 and res.get_json()["code"] == "drop_creating"
    assert vdb.drops.count_documents({}) == 1


def test_create_key_is_stored_on_the_published_drop(client, vdb):
    client.post("/v2/miniapp/admin/drops", json=SPEC, headers=KEY)
    assert vdb.drops.find_one({})["admin_request_key"] == "create-0001-abcd"


def _drop_with_pool(vdb):
    res = vdb.drops.insert_one({"name": "d", "type": "pooled", "status": "upcoming", "public_remaining": 0, "my_remaining": 0})
    return str(res.inserted_id)


def test_retried_add_codes_inserts_once_and_replays_the_answer(client, vdb):
    drop_id = _drop_with_pool(vdb)
    headers = {"Idempotency-Key": "addcodes-0001-ab"}
    first = client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": ["C1", "C2"]}, headers=headers)
    retry = client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": ["C1", "C2"]}, headers=headers)
    assert first.status_code == retry.status_code == 200
    assert retry.get_json() == first.get_json() and retry.get_json()["inserted"] == 2
    assert vdb.vouchers.count_documents({"dropId": drop_id}) == 2
    assert vdb.drops.find_one({})["public_remaining"] == 2


def test_add_codes_without_key_is_unchanged(client, vdb):
    drop_id = _drop_with_pool(vdb)
    assert client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": ["C1"]}).status_code == 200
    dup = client.post(f"/v2/miniapp/admin/drops/{drop_id}/codes", json={"codes": ["C1"]})
    assert dup.status_code == 409 and dup.get_json()["code"] == "duplicate_code"
    missing = client.post("/v2/miniapp/admin/drops/000000000000000000000000/codes", json={"codes": ["C9"]})
    assert missing.status_code == 404


def test_add_codes_key_is_scoped_per_drop(client, vdb):
    d1, d2 = _drop_with_pool(vdb), _drop_with_pool(vdb)
    headers = {"Idempotency-Key": "addcodes-0001-ab"}
    assert client.post(f"/v2/miniapp/admin/drops/{d1}/codes", json={"codes": ["C1"]}, headers=headers).status_code == 200
    r = client.post(f"/v2/miniapp/admin/drops/{d2}/codes", json={"codes": ["C2"]}, headers=headers)
    assert r.status_code == 200 and r.headers.get("Idempotent-Replay") is None
    assert vdb.vouchers.count_documents({}) == 2


def test_idempotency_indexes_are_created(vdb):
    vouchers._ensure_admin_idempotency_indexes()
    assert "ix_drops_admin_request_key" in vdb.drops.index_information()
    assert "ttl_admin_idempotency_expires_at" in vdb[idem.COLLECTION].index_information()


def test_ensure_voucher_indexes_calls_the_idempotency_index_helper():
    import inspect

    assert "_ensure_admin_idempotency_indexes()" in inspect.getsource(vouchers.ensure_voucher_indexes)


# ------------------------------------------------------------------ XP route
def _load_api_add_xp(monkeypatch, mdb, fake_update):
    """Extract api_add_xp from main.py (importing main boots the whole app)."""
    src = open("main.py", encoding="utf-8").read()
    tree = ast.parse(src)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "api_add_xp")
    fn.decorator_list = []
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    import database

    monkeypatch.setattr(database, "update_user_xp", fake_update)
    ns = {
        "request": request, "jsonify": jsonify, "db": mdb,
        "require_admin_from_query": lambda: (True, None),
    }
    exec(compile(mod, "main.py", "exec"), ns)
    app = Flask(__name__)
    app.add_url_rule("/api/add_xp", view_func=ns["api_add_xp"], methods=["POST"])
    return app.test_client()


class FakeXP:
    def __init__(self):
        self.calls = []
        self.cooldown_hits = False

    def __call__(self, username, amount, unique_key=None, enforce_cooldown=False):
        self.calls.append((username, amount, unique_key, enforce_cooldown))
        if self.cooldown_hits:
            return {"ok": False, "code": "cooldown", "message": "Please wait 60 seconds before granting again."}
        return True, f"XP added by {amount}."


def test_xp_header_key_grants_once_and_replays(monkeypatch, mdb):
    fake = FakeXP()
    c = _load_api_add_xp(monkeypatch, mdb, fake)
    h = {"Idempotency-Key": "xp-key-0001-abcd"}
    a = c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10}, headers=h)
    b = c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10}, headers=h)
    assert a.get_json() == b.get_json() == {"success": True, "message": "XP added by 10."}
    assert b.headers.get("Idempotent-Replay") == "true"
    assert fake.calls == [("alice", 10, "adminreq:xp-key-0001-abcd", True)]


def test_xp_cooldown_refusal_is_not_cached(monkeypatch, mdb):
    fake = FakeXP()
    fake.cooldown_hits = True
    c = _load_api_add_xp(monkeypatch, mdb, fake)
    h = {"Idempotency-Key": "xp-key-0001-abcd"}
    assert c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10}, headers=h).status_code == 429
    fake.cooldown_hits = False
    ok = c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10}, headers=h)
    assert ok.status_code == 200 and ok.get_json()["success"] is True and len(fake.calls) == 2


def test_xp_key_reuse_with_other_amount_is_refused(monkeypatch, mdb):
    fake = FakeXP()
    c = _load_api_add_xp(monkeypatch, mdb, fake)
    h = {"Idempotency-Key": "xp-key-0001-abcd"}
    c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10}, headers=h)
    res = c.post("/api/add_xp", json={"user_id": "@alice", "xp": 500}, headers=h)
    assert res.status_code == 422 and res.get_json()["success"] is False and len(fake.calls) == 1


def test_xp_legacy_body_key_path_is_unchanged(monkeypatch, mdb):
    fake = FakeXP()
    c = _load_api_add_xp(monkeypatch, mdb, fake)
    res = c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10, "idempotency_key": "adminui:1:alice:10:1"})
    assert res.get_json() == {"success": True, "message": "XP added by 10."}
    assert fake.calls == [("alice", 10, "adminui:1:alice:10:1", False)]
    c.post("/api/add_xp", json={"user_id": "@alice", "xp": 10})
    assert fake.calls[-1] == ("alice", 10, None, False)
