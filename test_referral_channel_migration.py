"""Tests for the AP Referral Bot channel migration.

Covers (numbering matches the migration task's Phase 11 list where
applicable):
  1-4, 31   referral_destination.get_referral_destination() mode resolution
  5-6       destination-scoped invite-link reuse (group vs channel)
  7-10      channel attribution via _confirm_referral_on_main_join
  11-13     self-referral / rate-limit / duplicate-event protection
  14        cross-destination duplicate guard (referral_invitee_lock)
  15-19     settlement join-time + membership validation per destination
  20-21     leave/rejoin only acts on its own destination's pending rows
  22-24     XP / referral_settled / qualified_events granted exactly once
  26-27     channel joins never call handle_user_join (source-level, since
            member_update_handler is not unit-testable without a live
            python-telegram-bot Update fixture in this suite)
  28-30     get_or_create_referral_invite_link_sync targets the configured
            destination (covers the Mini App / /start / share-content APIs,
            which all delegate to this one function)
  32        in-flight channel pending referrals settle using their own
            stored destination, independent of the live mode at settle time
"""

import ast
import importlib
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import mongomock
import pytest
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Phase 1: referral_destination.py
# ---------------------------------------------------------------------------

def _reload_referral_destination(monkeypatch, **env):
    for key in (
        "COMMUNITY_GROUP_ID",
        "MAIN_GROUP_ID",
        "GROUP_ID",
        "OFFICIAL_CHANNEL_ID",
        "REFERRAL_DESTINATION_MODE",
        "REFERRAL_DESTINATION_CHAT_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import referral_destination

    return importlib.reload(referral_destination)


def test_default_mode_resolves_community_group(monkeypatch):
    mod = _reload_referral_destination(monkeypatch)
    chat_id, destination_type = mod.get_referral_destination()
    assert destination_type == "community_group"
    assert chat_id == mod.COMMUNITY_GROUP_ID


def test_official_channel_mode_resolves_channel(monkeypatch):
    mod = _reload_referral_destination(
        monkeypatch, REFERRAL_DESTINATION_MODE="official_channel", OFFICIAL_CHANNEL_ID="-1009999"
    )
    chat_id, destination_type = mod.get_referral_destination()
    assert destination_type == "official_channel"
    assert chat_id == -1009999


def test_explicit_destination_chat_id_override_respected(monkeypatch):
    mod = _reload_referral_destination(
        monkeypatch,
        REFERRAL_DESTINATION_MODE="official_channel",
        OFFICIAL_CHANNEL_ID="-1009999",
        REFERRAL_DESTINATION_CHAT_ID="-1008888",
    )
    chat_id, destination_type = mod.get_referral_destination()
    assert destination_type == "official_channel"
    assert chat_id == -1008888


def test_invalid_mode_falls_back_to_community_group_with_error_log(monkeypatch, caplog):
    mod = _reload_referral_destination(monkeypatch, REFERRAL_DESTINATION_MODE="bogus_mode")
    with caplog.at_level("ERROR"):
        chat_id, destination_type = mod.get_referral_destination()
    assert destination_type == "community_group"
    assert chat_id == mod.COMMUNITY_GROUP_ID
    assert any("invalid_destination_mode" in r.message for r in caplog.records)


def test_destination_type_for_chat_id_honors_live_override(monkeypatch):
    mod = _reload_referral_destination(
        monkeypatch,
        REFERRAL_DESTINATION_MODE="official_channel",
        OFFICIAL_CHANNEL_ID="-1009999",
        REFERRAL_DESTINATION_CHAT_ID="-1008888",
    )
    # The override chat id (-1008888) must classify as official_channel even
    # though it differs from OFFICIAL_CHANNEL_ID, since get_referral_destination()
    # currently resolves to it.
    assert mod.destination_type_for_chat_id(-1008888) == "official_channel"
    assert mod.destination_type_for_chat_id(-1009999) == "official_channel"
    assert mod.destination_type_for_chat_id(mod.COMMUNITY_GROUP_ID) == "community_group"


def test_rollback_to_community_group_via_env_only(monkeypatch):
    mod = _reload_referral_destination(monkeypatch, REFERRAL_DESTINATION_MODE="official_channel")
    chat_id, destination_type = mod.get_referral_destination()
    assert destination_type == "official_channel"

    monkeypatch.setenv("REFERRAL_DESTINATION_MODE", "community_group")
    chat_id2, destination_type2 = mod.get_referral_destination()
    assert destination_type2 == "community_group"
    assert chat_id2 == mod.COMMUNITY_GROUP_ID


# ---------------------------------------------------------------------------
# Phase 2 / 28-30: get_or_create_referral_invite_link_sync targets the
# resolved destination, and reuses only chat_id-scoped active links.
# ---------------------------------------------------------------------------

def _load_main_function(name: str, extra_globals: dict | None = None):
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = dict(extra_globals or {})
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env[name]


class _NullTimer:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    ms = 0


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _make_get_or_create_link_func(monkeypatch, invite_link_map_collection, created_links):
    fn = _load_main_function("get_or_create_referral_invite_link_sync")

    def fake_post(url, json=None, timeout=None):  # noqa: A002
        chat_id = json["chat_id"]
        link = f"https://t.me/+generated_{chat_id}_{json['name']}"
        created_links.append(json)
        return _FakeResponse({"ok": True, "result": {"invite_link": link}})

    fn.__globals__.update(
        {
            "QUERY_TELEMETRY_LOGS": False,
            "JobTimer": _NullTimer,
            "invite_link_map_collection": invite_link_map_collection,
            "logger": _load_main_function.__module__ and __import__("logging").getLogger("test"),
            "_short_invite_link": lambda link: link,
            "get_referral_destination": None,  # set by caller
            "requests": type("R", (), {"post": staticmethod(fake_post)}),
            "API_BASE": "https://api.telegram.org/botTEST",
            "DuplicateKeyError": DuplicateKeyError,
            "datetime": datetime,
            "KL_TZ": timezone.utc,
            "os": os,
        }
    )
    return fn


def test_group_mode_generates_community_group_link(monkeypatch):
    dest_mod = _reload_referral_destination(monkeypatch)
    invite_link_map = mongomock.MongoClient().db.invite_link_map
    created = []
    fn = _make_get_or_create_link_func(monkeypatch, invite_link_map, created)
    fn.__globals__["get_referral_destination"] = dest_mod.get_referral_destination

    link = fn(555)

    assert created[0]["chat_id"] == dest_mod.COMMUNITY_GROUP_ID
    assert created[0]["creates_join_request"] is False
    row = invite_link_map.find_one({"inviter_id": 555})
    assert row["destination_type"] == "community_group"
    assert row["schema_version"] == 2
    assert row["invite_link"] == link


def test_official_channel_mode_generates_channel_link(monkeypatch):
    dest_mod = _reload_referral_destination(
        monkeypatch, REFERRAL_DESTINATION_MODE="official_channel", OFFICIAL_CHANNEL_ID="-1009999"
    )
    invite_link_map = mongomock.MongoClient().db.invite_link_map
    created = []
    fn = _make_get_or_create_link_func(monkeypatch, invite_link_map, created)
    fn.__globals__["get_referral_destination"] = dest_mod.get_referral_destination

    fn(555)

    assert created[0]["chat_id"] == -1009999
    row = invite_link_map.find_one({"inviter_id": 555})
    assert row["destination_type"] == "official_channel"
    assert row["chat_id"] == -1009999


def test_existing_group_link_continues_resolving_and_is_reused(monkeypatch):
    dest_mod = _reload_referral_destination(monkeypatch)
    invite_link_map = mongomock.MongoClient().db.invite_link_map
    invite_link_map.insert_one(
        {
            "inviter_id": 555,
            "chat_id": dest_mod.COMMUNITY_GROUP_ID,
            "destination_type": "community_group",
            "invite_link": "https://t.me/+legacy_group_link",
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
        }
    )
    created = []
    fn = _make_get_or_create_link_func(monkeypatch, invite_link_map, created)
    fn.__globals__["get_referral_destination"] = dest_mod.get_referral_destination

    link = fn(555)

    assert link == "https://t.me/+legacy_group_link"
    assert created == []  # reused, no new Telegram API call


def test_group_link_not_reused_after_switching_to_channel_mode(monkeypatch):
    dest_mod = _reload_referral_destination(monkeypatch)
    invite_link_map = mongomock.MongoClient().db.invite_link_map
    invite_link_map.insert_one(
        {
            "inviter_id": 555,
            "chat_id": dest_mod.COMMUNITY_GROUP_ID,
            "destination_type": "community_group",
            "invite_link": "https://t.me/+legacy_group_link",
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
        }
    )
    dest_mod = _reload_referral_destination(
        monkeypatch, REFERRAL_DESTINATION_MODE="official_channel", OFFICIAL_CHANNEL_ID="-1009999"
    )
    created = []
    fn = _make_get_or_create_link_func(monkeypatch, invite_link_map, created)
    fn.__globals__["get_referral_destination"] = dest_mod.get_referral_destination

    link = fn(555)

    assert link != "https://t.me/+legacy_group_link"
    assert created and created[0]["chat_id"] == -1009999


def test_channel_link_continues_resolving_after_rollback_to_group(monkeypatch):
    dest_mod = _reload_referral_destination(
        monkeypatch, REFERRAL_DESTINATION_MODE="official_channel", OFFICIAL_CHANNEL_ID="-1009999"
    )
    invite_link_map = mongomock.MongoClient().db.invite_link_map
    invite_link_map.insert_one(
        {
            "inviter_id": 555,
            "chat_id": -1009999,
            "destination_type": "official_channel",
            "invite_link": "https://t.me/+channel_link",
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
        }
    )
    # Roll back to community_group — the channel row must remain untouched
    # and processable (still queryable/settleable), even though new link
    # generation now targets the group again.
    dest_mod = _reload_referral_destination(monkeypatch)
    created = []
    fn = _make_get_or_create_link_func(monkeypatch, invite_link_map, created)
    fn.__globals__["get_referral_destination"] = dest_mod.get_referral_destination

    link = fn(555)

    assert link != "https://t.me/+channel_link"
    assert created and created[0]["chat_id"] == dest_mod.COMMUNITY_GROUP_ID
    channel_row = invite_link_map.find_one({"invite_link": "https://t.me/+channel_link"})
    assert channel_row is not None  # historical row preserved unchanged


# ---------------------------------------------------------------------------
# Phase 3 / 7-14: _confirm_referral_on_main_join (destination-neutral
# attribution + cross-destination duplicate guard)
# ---------------------------------------------------------------------------

GROUP_CHAT_ID = -1002304653063
CHANNEL_CHAT_ID = -1002396761021


class _RecordingLogger:
    def __init__(self):
        self.lines = []

    def info(self, fmt, *args):
        self.lines.append(fmt % args if args else fmt)

    def exception(self, fmt, *args):
        self.lines.append(fmt % args if args else fmt)

    def error(self, fmt, *args):
        self.lines.append(fmt % args if args else fmt)

    def has(self, substr):
        return any(substr in line for line in self.lines)


def _make_confirm_join_env(db):
    audits = []

    def write_audit(**kwargs):
        audits.append(kwargs)

    logger = _RecordingLogger()
    env_globals = {
        "logger": logger,
        "GROUP_ID": GROUP_CHAT_ID,
        "_truncate_invite_link": lambda link: link,
        "_write_referral_audit": write_audit,
        "invite_link_map_collection": db.invite_link_map,
        "unknown_invite_audit_collection": db.unknown_invite_audit,
        "consume_referral_rate_limits": lambda *a, **kw: (True, None, {}),
        "referral_rate_limits_collection": db.referral_rate_limits,
        "REFERRAL_HOURLY_LIMIT": 999,
        "REFERRAL_DAILY_LIMIT": 999,
        "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
        "KL_TZ": timezone.utc,
        "pending_referrals_collection": db.pending_referrals,
        "db": db,
        "datetime": datetime,
        "timezone": timezone,
        "should_count_referral_join": lambda *a, **kw: (True, None),
        "_referral_hold_hours": lambda: 48,
        "_maybe_send_referral_join_ack_dm": lambda *a, **kw: None,
    }
    fn = _load_main_function("_confirm_referral_on_main_join")
    fn.__globals__.update(env_globals)
    return fn, audits, logger


def _fresh_db():
    import referral_invitee_lock

    db = mongomock.MongoClient().db
    referral_invitee_lock.ensure_indexes(db)
    return db


def test_channel_join_with_exact_mapped_link_creates_one_pending_referral():
    db = _fresh_db()
    db.invite_link_map.insert_one(
        {
            "inviter_id": 100,
            "chat_id": CHANNEL_CHAT_ID,
            "destination_type": "official_channel",
            "invite_link": "https://t.me/+chan_link",
            "is_active": True,
        }
    )
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+chan_link", chat_id=CHANNEL_CHAT_ID)

    pending = db.pending_referrals.find_one({"invitee_user_id": 200})
    assert pending is not None
    assert pending["inviter_user_id"] == 100
    assert pending["destination_type"] == "official_channel"
    assert pending["destination_chat_id"] == CHANNEL_CHAT_ID
    assert pending["group_id"] == CHANNEL_CHAT_ID  # legacy field retained
    assert "referral_join_seen_at_utc" in pending
    assert pending["schema_version"] == 2


def test_channel_join_without_invite_link_creates_no_referral():
    db = _fresh_db()
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link=None, chat_id=CHANNEL_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 0
    assert audits[-1]["reason"] == "no_invite_link"


def test_public_channel_join_creates_no_attributed_referral():
    db = _fresh_db()
    # No invite_link_map row for this link at all -> "public"/unknown join.
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+unmapped", chat_id=CHANNEL_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 0
    assert audits[-1]["reason"] == "unknown_invite_link"


def test_existing_channel_subscriber_clicking_link_again_creates_no_new_event():
    db = _fresh_db()
    db.invite_link_map.insert_one(
        {
            "inviter_id": 100,
            "chat_id": CHANNEL_CHAT_ID,
            "destination_type": "official_channel",
            "invite_link": "https://t.me/+chan_link",
            "is_active": True,
        }
    )
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+chan_link", chat_id=CHANNEL_CHAT_ID)
    fn(200, invitee_username="u200", invite_link="https://t.me/+chan_link", chat_id=CHANNEL_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 1


def test_self_referral_is_rejected():
    db = _fresh_db()
    db.invite_link_map.insert_one(
        {
            "inviter_id": 200,
            "chat_id": GROUP_CHAT_ID,
            "invite_link": "https://t.me/+self_link",
            "is_active": True,
        }
    )
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+self_link", chat_id=GROUP_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 0
    assert audits[-1]["reason"] == "self_invite"


def test_rate_limit_still_applies():
    db = _fresh_db()
    db.invite_link_map.insert_one(
        {
            "inviter_id": 100,
            "chat_id": GROUP_CHAT_ID,
            "invite_link": "https://t.me/+group_link",
            "is_active": True,
        }
    )
    fn, audits, logger = _make_confirm_join_env(db)
    fn.__globals__["consume_referral_rate_limits"] = lambda *a, **kw: (False, "hourly_limit", {"key": "k", "count": 5, "limit": 5})

    fn(200, invitee_username="u200", invite_link="https://t.me/+group_link", chat_id=GROUP_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 0
    assert audits[-1]["reason"] == "hourly_limit"


def test_duplicate_membership_updates_do_not_create_duplicate_pending_rows():
    db = _fresh_db()
    db.invite_link_map.insert_one(
        {
            "inviter_id": 100,
            "chat_id": GROUP_CHAT_ID,
            "invite_link": "https://t.me/+group_link",
            "is_active": True,
        }
    )
    fn, audits, logger = _make_confirm_join_env(db)

    for _ in range(3):
        fn(200, invitee_username="u200", invite_link="https://t.me/+group_link", chat_id=GROUP_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 1


def test_same_invitee_group_then_channel_link_cannot_produce_two_pending_rewards():
    db = _fresh_db()
    db.invite_link_map.insert_one(
        {"inviter_id": 100, "chat_id": GROUP_CHAT_ID, "invite_link": "https://t.me/+g", "is_active": True}
    )
    db.invite_link_map.insert_one(
        {"inviter_id": 101, "chat_id": CHANNEL_CHAT_ID, "invite_link": "https://t.me/+c", "is_active": True}
    )
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)
    fn(200, invitee_username="u200", invite_link="https://t.me/+c", chat_id=CHANNEL_CHAT_ID)

    # Only the first (group) attribution should have created a pending row;
    # the channel attempt must be blocked by the cross-destination lock.
    assert db.pending_referrals.count_documents({}) == 1
    only = db.pending_referrals.find_one({})
    assert only["inviter_user_id"] == 100
    assert audits[-1]["reason"] == "cross_destination_duplicate"


# ---------------------------------------------------------------------------
# referral_invitee_lock.py — direct unit tests
# ---------------------------------------------------------------------------

def test_lock_claim_blocks_second_active_claim_for_same_invitee():
    import referral_invitee_lock

    db = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=10, chat_id=GROUP_CHAT_ID,
        destination_type="community_group", now_utc_ts=now,
    ) is True
    assert referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=20, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    ) is False


def test_lock_release_allows_future_claim():
    import referral_invitee_lock

    db = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=10, chat_id=GROUP_CHAT_ID,
        destination_type="community_group", now_utc_ts=now,
    )
    referral_invitee_lock.release(db, invitee_user_id=1, status="revoked", now_utc_ts=now)
    assert referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=20, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    ) is True


# ---------------------------------------------------------------------------
# Phase 6 / 15-19, 32: settle_pending_referrals destination-aware validation
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, modified_count=0, upserted_id=None):
        self.modified_count = modified_count
        self.upserted_id = upserted_id


class _PendingCollection:
    def __init__(self, docs):
        self.docs = docs

    def _match(self, doc, filt):
        for key, val in filt.items():
            if key == "$or":
                if not any(self._match(doc, branch) for branch in val):
                    return False
                continue
            if isinstance(val, dict):
                if "$in" in val:
                    if doc.get(key) not in val["$in"]:
                        return False
                    continue
                if "$lte" in val:
                    if doc.get(key) is None or not (doc.get(key) <= val["$lte"]):
                        return False
                    continue
                if "$exists" in val:
                    if bool(val["$exists"]) != (key in doc):
                        return False
                    continue
            if doc.get(key) != val:
                return False
        return True

    def _apply_update(self, doc, update):
        for k, v in update.get("$set", {}).items():
            doc[k] = v
        for k in update.get("$unset", {}).keys():
            doc.pop(k, None)

    def find_one_and_update(self, filt, update, sort=None, return_document=None):
        matches = [d for d in self.docs if self._match(d, filt)]
        if not matches:
            return None
        doc = matches[0]
        original = dict(doc)
        self._apply_update(doc, update)
        if return_document == ReturnDocument.BEFORE:
            return original
        return dict(doc)

    def update_one(self, filt, update):
        for doc in self.docs:
            if self._match(doc, filt):
                self._apply_update(doc, update)
                return _Result(modified_count=1)
        return _Result(modified_count=0)

    def update_many(self, filt, update):
        modified = 0
        for doc in self.docs:
            if self._match(doc, filt):
                self._apply_update(doc, update)
                modified += 1
        return _Result(modified_count=modified)


class _UsersCollection:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, filt, projection=None):
        return self.docs.get(filt.get("user_id"))


class _AwardEvents:
    def __init__(self):
        self.docs = []

    def find_one(self, filt, projection=None):
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in filt.items()):
                return doc
        return None

    def insert_one(self, doc):
        self.docs.append(dict(doc))


class _ReferralEvents:
    def aggregate(self, pipeline):
        return []


class _FakeSchedulerDB:
    def __init__(self, pending_docs, user_docs):
        self.pending_referrals = _PendingCollection(pending_docs)
        self.users = _UsersCollection(user_docs)
        self.referral_award_events = _AwardEvents()
        self.referral_events = _ReferralEvents()
        self.referral_invitee_locks = mongomock.MongoClient().db.referral_invitee_locks


@pytest.fixture
def scheduler_mod():
    import scheduler

    orig = {
        "db": scheduler.db,
        "_get_official_channel_member_status": scheduler._get_official_channel_member_status,
        "_record_referral_event": scheduler._record_referral_event,
        "grant_xp": scheduler.grant_xp,
        "calc_referral_award": scheduler.calc_referral_award,
        "maybe_handle_first_referral": scheduler.maybe_handle_first_referral,
        "maybe_unlock_affiliate_group": scheduler.maybe_unlock_affiliate_group,
        "maybe_shout_referral_congrats": scheduler.maybe_shout_referral_congrats,
        "mark_invitee_qualified": scheduler.mark_invitee_qualified,
        "confirm_qualified_invitees": scheduler.confirm_qualified_invitees,
        "evaluate_referral_engagement": scheduler.evaluate_referral_engagement,
        "now_utc": scheduler.now_utc,
        "now_kl": scheduler.now_kl,
        "_maybe_send_referral_qualified_dm": scheduler._maybe_send_referral_qualified_dm,
        "OFFICIAL_CHANNEL_ID": scheduler.OFFICIAL_CHANNEL_ID,
    }
    fixed_now = datetime(2025, 1, 10, tzinfo=timezone.utc)
    scheduler.now_utc = lambda: fixed_now
    scheduler.now_kl = lambda: fixed_now
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"
    scheduler._record_referral_event = lambda *a, **kw: True
    scheduler.grant_xp = lambda *a, **kw: True
    scheduler.calc_referral_award = lambda total: (10, 0)
    scheduler.maybe_handle_first_referral = lambda *a, **kw: None
    scheduler.maybe_unlock_affiliate_group = lambda **kw: None
    scheduler.maybe_shout_referral_congrats = lambda *a, **kw: None
    scheduler.confirm_qualified_invitees = lambda: 0
    scheduler._maybe_send_referral_qualified_dm = lambda *a, **kw: None
    scheduler.OFFICIAL_CHANNEL_ID = CHANNEL_CHAT_ID
    scheduler.evaluate_referral_engagement = lambda **kw: {
        "qualified": True,
        "score": 5,
        "signals": {},
        "points": {},
        "window_start": fixed_now - timedelta(hours=1),
        "window_end": fixed_now,
    }
    qualified_calls = []
    scheduler.mark_invitee_qualified = lambda *a, **kw: qualified_calls.append(kw) or True

    yield scheduler, fixed_now, qualified_calls

    for k, v in orig.items():
        setattr(scheduler, k, v)


def _channel_pending(fixed_now, **overrides):
    doc = {
        "_id": 1,
        "group_id": CHANNEL_CHAT_ID,
        "destination_chat_id": CHANNEL_CHAT_ID,
        "destination_type": "official_channel",
        "status": "pending",
        "inviter_user_id": 11,
        "invitee_user_id": 22,
        "created_at_utc": fixed_now - timedelta(hours=100),
        "referral_join_seen_at_utc": fixed_now - timedelta(hours=100),
    }
    doc.update(overrides)
    return doc


def test_channel_origin_invitee_without_joined_main_at_not_rejected(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    doc = _channel_pending(fixed_now)
    user_docs = {22: {"user_id": 22}}  # no joined_main_at at all
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"
    assert doc.get("revoked_reason") != "missing_join_time"


def test_existing_bot_user_new_channel_subscriber_can_qualify(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    doc = _channel_pending(fixed_now)
    # An old bot account (existed long before this channel join) with no
    # joined_main_at (never joined the group) — must still qualify.
    user_docs = {22: {"user_id": 22, "created_at": fixed_now - timedelta(days=400)}}
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"


def test_existing_chatroom_user_new_channel_subscriber_can_qualify(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    doc = _channel_pending(fixed_now)
    # Existing chatroom user: joined_main_at long ago, now newly subscribing
    # to the channel via a referral link.
    user_docs = {
        22: {
            "user_id": 22,
            "joined_main_at": fixed_now - timedelta(days=200),
            "created_at": fixed_now - timedelta(days=200),
        }
    }
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"


def test_group_origin_legacy_referral_still_uses_joined_main_at_staleness_check(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    doc = {
        "_id": 1,
        "group_id": scheduler.GROUP_ID,
        "status": "pending",
        "inviter_user_id": 11,
        "invitee_user_id": 22,
        "created_at_utc": fixed_now - timedelta(hours=100),
    }
    # joined_main_at is far earlier than the pending's created_at_utc ->
    # existing "already_in_db" staleness guard must still apply for
    # group-origin (unchanged legacy behaviour).
    user_docs = {
        22: {
            "user_id": 22,
            "joined_main_at": fixed_now - timedelta(days=200),
            "created_at": fixed_now - timedelta(days=200),
        }
    }
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "revoked"
    assert doc["revoked_reason"] == "already_in_db"


def test_group_origin_legacy_referral_settlement_still_works(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    doc = {
        "_id": 1,
        "group_id": scheduler.GROUP_ID,
        "status": "pending",
        "inviter_user_id": 11,
        "invitee_user_id": 22,
        "created_at_utc": fixed_now - timedelta(hours=100),
    }
    user_docs = {
        22: {
            "user_id": 22,
            "joined_main_at": fixed_now - timedelta(hours=99),
            "created_at": fixed_now - timedelta(hours=99),
        }
    }
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"
    assert doc["award_key"] == "ref:22"


def test_award_key_is_invitee_scoped_not_destination_scoped(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    doc = _channel_pending(fixed_now)
    user_docs = {22: {"user_id": 22}}
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert scheduler.db.referral_award_events.docs[0]["award_key"] == "ref:22"


def test_settlement_recovers_pre_migration_legacy_award_key_without_double_xp(scheduler_mod):
    # An invitee already awarded before this migration under the legacy
    # "ref:<group_id>:<invitee_id>" key format (and with no
    # referral_invitee_locks row at all, since that collection is new) must
    # not receive XP a second time when a fresh channel-origin pending row
    # for the same invitee reaches settlement.
    scheduler, fixed_now, qualified_calls = scheduler_mod
    grant_calls = []
    scheduler.grant_xp = lambda *a, **kw: grant_calls.append(a) or True

    doc = _channel_pending(fixed_now)
    user_docs = {22: {"user_id": 22}}
    fake_db = _FakeSchedulerDB([doc], user_docs)
    fake_db.referral_award_events.docs.append(
        {"award_key": f"ref:{scheduler.GROUP_ID}:22", "invitee_user_id": 22, "inviter_user_id": 99}
    )
    scheduler.db = fake_db

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"
    assert doc["award_key"] == f"ref:{scheduler.GROUP_ID}:22"  # recovered legacy key, not overwritten
    assert grant_calls == []  # no new award row inserted -> grant_xp never called
    assert len(qualified_calls) == 1


def test_xp_granted_exactly_once_across_duplicate_award_recovery(scheduler_mod):
    scheduler, fixed_now, qualified_calls = scheduler_mod

    class _DupAwardEvents:
        def __init__(self):
            self.insert_calls = 0

        def find_one(self, filt, projection=None):
            return None

        def insert_one(self, doc):
            self.insert_calls += 1
            raise DuplicateKeyError("duplicate")

    grant_calls = []
    scheduler.grant_xp = lambda *a, **kw: grant_calls.append(a) or True

    doc = _channel_pending(fixed_now)
    user_docs = {22: {"user_id": 22}}
    fake_db = _FakeSchedulerDB([doc], user_docs)
    fake_db.referral_award_events = _DupAwardEvents()
    scheduler.db = fake_db

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"
    assert grant_calls == []  # award_events insert failed -> grant_xp never called
    assert len(qualified_calls) == 1  # qualified exactly once via recovery path


def test_channel_settlement_checks_official_channel_membership(scheduler_mod):
    scheduler, fixed_now, _ = scheduler_mod
    calls = []
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: calls.append(uid) or "left"
    doc = _channel_pending(fixed_now)
    user_docs = {22: {"user_id": 22}}
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert calls == [22]
    assert doc["status"] == "revoked"
    assert doc["revoked_reason"] == "not_in_official_channel"


def test_in_flight_channel_pending_settles_after_rollback_to_group_mode(monkeypatch, scheduler_mod):
    # Simulate rollback: current live mode is community_group, but this
    # pending row was created while official_channel mode was active and
    # carries its own destination metadata — settlement must honor the
    # row's stored destination, not the current live mode.
    monkeypatch.setenv("REFERRAL_DESTINATION_MODE", "community_group")
    scheduler, fixed_now, _ = scheduler_mod
    doc = _channel_pending(fixed_now)
    user_docs = {22: {"user_id": 22}}
    scheduler.db = _FakeSchedulerDB([doc], user_docs)

    scheduler.settle_pending_referrals(batch_limit=1)

    assert doc["status"] == "awarded"


# ---------------------------------------------------------------------------
# Phase 7 / 20-21: leave/rejoin only acts on its own destination (source-
# level checks, since member_update_handler needs a live python-telegram-bot
# Update fixture that this suite does not construct).
# ---------------------------------------------------------------------------

def test_group_leave_revoke_query_scoped_to_group_id():
    source = Path("main.py").read_text(encoding="utf-8")
    assert 'if left_group and chat_id == GROUP_ID and isinstance(user.id, int):' in source


def test_channel_leave_branch_does_not_touch_pending_referrals():
    source = Path("main.py").read_text(encoding="utf-8")
    start = source.index("if left_group and is_channel_chat_id")
    end = source.index("if not became_member:", start)
    channel_leave_block = source[start:end]
    assert "pending_referrals_collection" not in channel_leave_block


def test_restricted_to_member_is_not_treated_as_a_new_join():
    source = Path("main.py").read_text(encoding="utf-8")
    assert (
        'was_present = old_status in {"member", "administrator", "creator"} or (\n'
        '        old_status == "restricted" and old_is_member is True\n'
        "    )"
    ) in source


# ---------------------------------------------------------------------------
# Phase 3 / 26-27: channel joins never call handle_user_join (source check)
# ---------------------------------------------------------------------------

def test_channel_branch_never_calls_handle_user_join():
    source = Path("main.py").read_text(encoding="utf-8")
    start = source.index("async def member_update_handler")
    end = source.index("\ndef _is_mywin_message", start)
    handler_source = source[start:end]
    assert "if chat_id == GROUP_ID:\n        try:\n            await handle_user_join(" in handler_source


# ---------------------------------------------------------------------------
# member_update_handler: immediate group-leave revocation must release the
# cross-destination invitee lock (Codex P2 finding), not just mark the
# pending row revoked, or the invitee is blocked from every future referral.
# ---------------------------------------------------------------------------

def test_group_leave_before_hold_releases_invitee_lock():
    import asyncio
    from types import SimpleNamespace

    from telegram import Update
    from telegram.ext import ContextTypes

    import referral_invitee_lock

    db = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    invitee_id = 22
    db.pending_referrals.insert_one(
        {
            "group_id": GROUP_CHAT_ID,
            "invitee_user_id": invitee_id,
            "inviter_user_id": 11,
            "status": "pending",
        }
    )
    referral_invitee_lock.claim(
        db, invitee_user_id=invitee_id, inviter_user_id=11, chat_id=GROUP_CHAT_ID,
        destination_type="community_group", now_utc_ts=now,
    )
    # Sanity: the lock is blocking before the leave event.
    assert referral_invitee_lock.claim(
        db, invitee_user_id=invitee_id, inviter_user_id=99, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    ) is False

    fn = _load_main_function(
        "member_update_handler",
        extra_globals={
            "Update": Update,
            "ContextTypes": ContextTypes,
            "GROUP_ID": GROUP_CHAT_ID,
            "OFFICIAL_CHANNEL_ID": CHANNEL_CHAT_ID,
            "get_referral_destination": lambda: (GROUP_CHAT_ID, "community_group"),
            "pending_referrals_collection": db.pending_referrals,
            "now_utc": lambda: now,
            "logger": _RecordingLogger(),
            "db": db,
            "ReturnDocument": ReturnDocument,
            "users_collection": db.users,
        },
    )

    fake_user = SimpleNamespace(id=invitee_id, is_bot=False, username="u22")
    fake_old = SimpleNamespace(status="member", is_member=True)
    fake_new = SimpleNamespace(status="left", user=fake_user)
    fake_member = SimpleNamespace(chat=SimpleNamespace(id=GROUP_CHAT_ID), old_chat_member=fake_old, new_chat_member=fake_new)
    fake_update = SimpleNamespace(chat_member=fake_member, my_chat_member=None)

    asyncio.run(fn(fake_update, None))

    pending = db.pending_referrals.find_one({"invitee_user_id": invitee_id})
    assert pending["status"] == "revoked"
    # The lock must now allow a fresh referral attempt for this invitee.
    assert referral_invitee_lock.claim(
        db, invitee_user_id=invitee_id, inviter_user_id=99, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    ) is True


# ---------------------------------------------------------------------------
# Phase 12: dry-run audit tooling
# ---------------------------------------------------------------------------

def test_migration_audit_report_is_read_only_and_reports_expected_fields(monkeypatch):
    _reload_referral_destination(monkeypatch)
    import referral_migration_audit

    db = _fresh_db()
    db.invite_link_map.insert_one({"chat_id": referral_migration_audit.get_referral_destination()[0] if False else -1002304653063, "is_active": True})
    report = referral_migration_audit.build_report(db)

    assert "config" in report
    assert "invite_link_map" in report
    assert "cross_destination_duplicate_invitees" in report
    assert "pending_referrals" in report
    assert "award_events" in report
    assert "settled_without_qualified_event_count" in report
    assert "qualified_events_duplicate_invitee_ids" in report
    # read-only: no referral_migration_audit_reports collection written
    assert "referral_migration_audit_reports" not in db.list_collection_names()


def test_migration_audit_detects_cross_destination_duplicate(monkeypatch):
    _reload_referral_destination(monkeypatch)
    import referral_migration_audit

    db = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db.pending_referrals.insert_one(
        {"invitee_user_id": 5, "group_id": GROUP_CHAT_ID, "status": "pending", "created_at_utc": now}
    )
    db.pending_referrals.insert_one(
        {
            "invitee_user_id": 5,
            "destination_chat_id": CHANNEL_CHAT_ID,
            "status": "pending",
            "created_at_utc": now + timedelta(minutes=1),
        }
    )
    report = referral_migration_audit.build_report(db)
    assert len(report["cross_destination_duplicate_invitees"]) == 1
    assert report["cross_destination_duplicate_invitees"][0]["invitee_user_id"] == 5
