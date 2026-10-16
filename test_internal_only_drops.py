"""internal_only drops: never discoverable or claimable by ordinary users.

Regression coverage for the P0 restored after the UGC subsystem removal took
the guard with it. Runs the real Mongo query semantics (mongomock), so a
filter that silently stops matching fails here rather than passing against a
hand-rolled fake.

Covered:
  * get_active_drops / user_visible_drops exclude internal_only=True, keep
    drops whose flag is false or missing
  * /drop_mode never surfaces an internal drop id
  * /vouchers/claim (real and check_only) rejects an internal drop with 403
    before any lock, claim row or voucher write
  * claim_voucher_for_user refuses internal drops (defense in depth)
  * admin preview still lists internal drops
"""
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from flask import Flask

import database
import vouchers as m

UID = 4242


@pytest.fixture
def mdb(monkeypatch):
    saved = (database._client, database._db)
    database._client = mongomock.MongoClient(tz_aware=True)
    database._db = database._client["referral_bot"]
    monkeypatch.setattr(m, "_get_setting", None, raising=False)
    try:
        yield database._db
    finally:
        database._client, database._db = saved


def _drop(drop_id, now, **extra):
    doc = {
        "_id": drop_id,
        "name": drop_id,
        "type": "pooled",
        "status": "active",
        "startsAt": now - timedelta(minutes=5),
        "endsAt": now + timedelta(hours=1),
        "public_remaining": 1,
        "my_remaining": 0,
    }
    doc.update(extra)
    return doc


def _seed(mdb, now):
    mdb.drops.insert_many([
        _drop("drop-internal", now, internal_only=True, priority=999),
        _drop("drop-flag-false", now, internal_only=False, priority=1),
        _drop("drop-no-flag", now, priority=1),
    ])
    for drop_id in ("drop-internal", "drop-flag-false", "drop-no-flag"):
        mdb.vouchers.insert_one(
            {"type": "pooled", "dropId": drop_id, "code": f"{drop_id}-C1", "status": "free", "pool": "public"}
        )


def _auth_as(monkeypatch, uid=UID):
    monkeypatch.setattr(m, "extract_raw_init_data_from_query", lambda req: "signed")
    monkeypatch.setattr(
        m, "verify_telegram_init_data",
        lambda raw: (True, {"user": '{"id": %d, "username": "u%d"}' % (uid, uid)}, "ok"),
    )


def test_get_active_drops_excludes_only_internal_only_true(mdb):
    now = datetime.now(timezone.utc)
    _seed(mdb, now)
    ids = {d["_id"] for d in m.get_active_drops(now)}
    assert ids == {"drop-flag-false", "drop-no-flag"}


def test_user_visible_drops_never_lists_internal_drop(mdb, monkeypatch):
    now = datetime.now(timezone.utc)
    _seed(mdb, now)
    monkeypatch.setattr(m, "is_drop_allowed", lambda *a, **k: True)
    monkeypatch.setattr(m, "is_user_eligible_for_drop", lambda *a, **k: True)
    monkeypatch.setattr(m, "load_user_context", lambda **k: {})
    with Flask(__name__).app_context():
        cards, _ = m.user_visible_drops(
            {"usernameLower": "u4242", "userId": str(UID)}, now, tg_user={"id": UID, "username": "u4242"}
        )
    listed = {c["dropId"] for c in cards}
    assert "drop-internal" not in listed
    assert {"drop-flag-false", "drop-no-flag"} <= listed


def test_drop_mode_never_surfaces_internal_drop(mdb, monkeypatch):
    now = datetime.now(timezone.utc)
    # Highest-priority active drop is the internal one.
    mdb.drops.insert_one(_drop("drop-internal", now, internal_only=True, priority=999))
    _auth_as(monkeypatch)
    monkeypatch.setattr(m, "_user_ctx_or_preview", lambda req, **k: ({"user": {"id": UID}}, False))
    monkeypatch.setattr(m, "_DROP_MODE_CACHE", {})
    app = Flask(__name__)
    with app.test_request_context("/drop_mode?init_data=signed"):
        body = m.api_drop_mode().get_json()
    assert body["active_drop_id"] is None
    assert body["drop_mode"] is False


@pytest.mark.parametrize("check_only", [False, True])
def test_direct_claim_of_internal_drop_is_rejected_without_side_effects(mdb, monkeypatch, check_only):
    now = datetime.now(timezone.utc)
    _seed(mdb, now)
    _auth_as(monkeypatch)
    app = Flask(__name__)
    with app.test_request_context(
        "/vouchers/claim?init_data=signed", method="POST",
        json={"dropId": "drop-internal", "check_only": check_only},
    ):
        resp, status = m.api_claim()
    assert status == 403
    assert resp.get_json() == {"status": "error", "code": "not_eligible", "reason": "internal_only"}
    # Nothing was written: code still free, no claim row, no dedup lock.
    assert mdb.vouchers.find_one({"dropId": "drop-internal"})["status"] == "free"
    assert mdb.voucher_claims.count_documents({}) == 0
    assert mdb.request_dedup.count_documents({}) == 0


def test_internal_guard_does_not_block_ordinary_drops(mdb, monkeypatch):
    """The same request against a non-internal drop gets past the guard (it
    may still be stopped by the normal gates, but never as internal_only)."""
    now = datetime.now(timezone.utc)
    _seed(mdb, now)
    _auth_as(monkeypatch)
    app = Flask(__name__)
    for drop_id in ("drop-flag-false", "drop-no-flag"):
        with app.test_request_context(
            "/vouchers/claim?init_data=signed", method="POST", json={"dropId": drop_id, "check_only": True}
        ):
            rv = m.api_claim()
        body = (rv[0] if isinstance(rv, tuple) else rv).get_json()
        assert body.get("reason") != "internal_only"


def test_claim_voucher_for_user_refuses_internal_drop(mdb):
    now = datetime.now(timezone.utc)
    _seed(mdb, now)
    with Flask(__name__).app_context():
        with pytest.raises(m.NotEligible, match="internal_only"):
            m.claim_voucher_for_user(user_id=str(UID), drop_id="drop-internal", username="u4242")
    assert mdb.vouchers.find_one({"dropId": "drop-internal"})["status"] == "free"


def test_admin_preview_still_lists_internal_drop(mdb, monkeypatch):
    now = datetime.now(timezone.utc)
    _seed(mdb, now)
    _auth_as(monkeypatch)
    monkeypatch.setattr(m, "_user_ctx_or_preview", lambda req, **k: ({"userId": "0"}, True))
    app = Flask(__name__)
    with app.test_request_context("/vouchers/visible?init_data=signed"):
        resp = m.api_visible()
    assert resp.status_code == 200
    assert "drop-internal" in {d["dropId"] for d in resp.get_json()["drops"]}
