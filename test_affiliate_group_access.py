from datetime import datetime, timezone

import affiliate_group_access as aga


class _Result:
    def __init__(self, modified_count=0):
        self.modified_count = modified_count


class _Users:
    def __init__(self, docs):
        self.docs = {int(k): dict(v) for k, v in docs.items()}

    def find_one_and_update(self, filt, update, return_document=None):
        uid = int(filt["user_id"])
        doc = self.docs.get(uid)
        if doc is None:
            return None
        unlocked_filter = filt.get("affiliate_group_unlocked_at")
        if isinstance(unlocked_filter, dict) and unlocked_filter.get("$exists") is False:
            if "affiliate_group_unlocked_at" in doc:
                return None
        for k, v in update.get("$set", {}).items():
            doc[k] = v
        for k in update.get("$unset", {}).keys():
            doc.pop(k, None)
        return dict(doc)

    def update_one(self, filt, update):
        uid = int(filt["user_id"])
        doc = self.docs.get(uid)
        if doc is None:
            return _Result(0)
        for k, v in update.get("$set", {}).items():
            doc[k] = v
        for k, v in update.get("$inc", {}).items():
            doc[k] = int(doc.get(k, 0) or 0) + int(v)
        for k in update.get("$unset", {}).keys():
            doc.pop(k, None)
        return _Result(1)


class _DB:
    def __init__(self, users_docs):
        self.users = _Users(users_docs)


def test_unlock_on_4_to_5(monkeypatch):
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_DM_ENABLED", True)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_UNLOCK_REFERRALS", 5)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_INVITE_URL", "https://t.me/+2415x7eUHOcwNzE9")
    monkeypatch.setattr(aga, "_send_affiliate_group_dm", lambda **kwargs: (True, None))

    db = _DB({101: {"user_id": 101, "total_referrals": 4}})
    out = aga.maybe_unlock_affiliate_group(
        db=db,
        user_id=101,
        current_ref_total=4,
        new_ref_total=5,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert out["unlocked"] is True
    assert out["dm_sent"] is True
    assert "affiliate_group_unlocked_at" in db.users.docs[101]
    assert "affiliate_group_dm_sent_at" in db.users.docs[101]


def test_no_unlock_on_5_to_6_if_already_unlocked(monkeypatch):
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_DM_ENABLED", True)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_UNLOCK_REFERRALS", 5)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_INVITE_URL", "https://t.me/+2415x7eUHOcwNzE9")

    called = {"n": 0}

    def _sender(**kwargs):
        called["n"] += 1
        return True, None

    monkeypatch.setattr(aga, "_send_affiliate_group_dm", _sender)
    db = _DB({101: {"user_id": 101, "affiliate_group_unlocked_at": datetime.now(timezone.utc)}})
    out = aga.maybe_unlock_affiliate_group(
        db=db,
        user_id=101,
        current_ref_total=5,
        new_ref_total=6,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert out["unlocked"] is False
    assert called["n"] == 0


def test_no_unlock_on_6_to_7_without_unlock_field_no_backfill(monkeypatch):
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_DM_ENABLED", True)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_UNLOCK_REFERRALS", 5)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_INVITE_URL", "https://t.me/+2415x7eUHOcwNzE9")

    called = {"n": 0}

    def _sender(**kwargs):
        called["n"] += 1
        return True, None

    monkeypatch.setattr(aga, "_send_affiliate_group_dm", _sender)
    db = _DB({101: {"user_id": 101, "total_referrals": 6}})
    out = aga.maybe_unlock_affiliate_group(
        db=db,
        user_id=101,
        current_ref_total=6,
        new_ref_total=7,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert out["unlocked"] is False
    assert "affiliate_group_unlocked_at" not in db.users.docs[101]
    assert called["n"] == 0


def test_duplicate_execution_does_not_send_multiple_dms(monkeypatch):
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_DM_ENABLED", True)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_UNLOCK_REFERRALS", 5)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_INVITE_URL", "https://t.me/+2415x7eUHOcwNzE9")

    called = {"n": 0}

    def _sender(**kwargs):
        called["n"] += 1
        return True, None

    monkeypatch.setattr(aga, "_send_affiliate_group_dm", _sender)
    db = _DB({101: {"user_id": 101, "total_referrals": 4}})

    first = aga.maybe_unlock_affiliate_group(
        db=db,
        user_id=101,
        current_ref_total=4,
        new_ref_total=5,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    second = aga.maybe_unlock_affiliate_group(
        db=db,
        user_id=101,
        current_ref_total=4,
        new_ref_total=5,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert first["dm_sent"] is True
    assert second["dm_sent"] is False
    assert called["n"] == 1


def test_dm_failure_stores_error_and_keeps_unlocked(monkeypatch):
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_DM_ENABLED", True)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_UNLOCK_REFERRALS", 5)
    monkeypatch.setattr(aga, "AFFILIATE_GROUP_INVITE_URL", "https://t.me/+2415x7eUHOcwNzE9")
    monkeypatch.setattr(aga, "_send_affiliate_group_dm", lambda **kwargs: (False, "bot_blocked"))

    db = _DB({101: {"user_id": 101, "total_referrals": 4}})
    out = aga.maybe_unlock_affiliate_group(
        db=db,
        user_id=101,
        current_ref_total=4,
        new_ref_total=5,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert out["unlocked"] is True
    assert out["dm_sent"] is False
    assert db.users.docs[101]["affiliate_group_dm_error"] == "bot_blocked"
    assert db.users.docs[101]["affiliate_group_dm_attempts"] == 1
    assert "affiliate_group_unlocked_at" in db.users.docs[101]
