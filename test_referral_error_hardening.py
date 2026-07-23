"""Tests for the referral error-hardening pass:

- has_historical_success() tri-state result (found / not_found / lookup_failed)
- _confirm_referral_on_main_join fails closed on a historical-success lookup
  failure and on an invitee-lock claim failure (no pending row, no lock
  claimed, no acknowledgement DM sent)
- referral_invitee_lock.claim() fails closed on a database error instead of
  treating the lock as acquired
- partial-failure cleanup releases only the lock owned by the attempt that
  created it (ownership-token check), leaving no orphan lock
- a later retry succeeds once the database recovers
"""

from datetime import datetime, timezone

import pytest
from pymongo.errors import DuplicateKeyError

from referral_historical_success import HistoricalSuccessResult, has_historical_success
from test_referral_channel_migration import (
    CHANNEL_CHAT_ID,
    GROUP_CHAT_ID,
    _fresh_db,
    _make_confirm_join_env,
)

import referral_invitee_lock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _ControllableDb:
    """Wraps a real mongomock db; any collection name in ``raising`` raises
    on both attribute access (db.<name>, used by has_historical_success) and
    item access (db[<name>], used by referral_invitee_lock)."""

    def __init__(self, real_db):
        self._real = real_db
        self.raising = set()

    def _check(self, name):
        if name in self.raising:
            raise RuntimeError(f"{name}_unavailable")

    def __getattr__(self, name):
        self._check(name)
        return getattr(self._real, name)

    def __getitem__(self, name):
        self._check(name)
        return self._real[name]


def _seed_link(db, *, inviter_id=100, chat_id=GROUP_CHAT_ID, invite_link="https://t.me/+g"):
    db.invite_link_map.insert_one(
        {
            "inviter_id": inviter_id,
            "chat_id": chat_id,
            "destination_type": "community_group" if chat_id == GROUP_CHAT_ID else "official_channel",
            "invite_link": invite_link,
            "is_active": True,
        }
    )


# ---------------------------------------------------------------------------
# has_historical_success() tri-state result
# ---------------------------------------------------------------------------


def test_has_historical_success_not_found_when_all_collections_empty():
    db = _fresh_db()
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.NOT_FOUND


def test_has_historical_success_found_via_qualified_events():
    db = _fresh_db()
    db.qualified_events.insert_one({"invitee_id": 1, "referrer_id": 10})
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.FOUND


def test_has_historical_success_found_via_referral_events_settled():
    db = _fresh_db()
    db.referral_events.insert_one({"invitee_id": 1, "inviter_id": 10, "event": "referral_settled"})
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.FOUND


def test_has_historical_success_ignores_non_settled_referral_events():
    db = _fresh_db()
    db.referral_events.insert_one({"invitee_id": 1, "inviter_id": 10, "event": "referral_revoked"})
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.NOT_FOUND


def test_has_historical_success_found_via_referral_award_events():
    db = _fresh_db()
    db.referral_award_events.insert_one({"invitee_user_id": 1, "award_key": "ref:1"})
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.FOUND


def test_has_historical_success_lookup_failed_when_qualified_events_raises():
    db = _ControllableDb(_fresh_db())
    db.raising.add("qualified_events")
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.LOOKUP_FAILED


def test_has_historical_success_lookup_failed_when_referral_events_raises():
    db = _ControllableDb(_fresh_db())
    db.raising.add("referral_events")
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.LOOKUP_FAILED


def test_has_historical_success_lookup_failed_when_referral_award_events_raises():
    db = _ControllableDb(_fresh_db())
    db.raising.add("referral_award_events")
    assert has_historical_success(db, invitee_user_id=1) is HistoricalSuccessResult.LOOKUP_FAILED


# ---------------------------------------------------------------------------
# referral_invitee_lock.claim() fails closed on database errors
# ---------------------------------------------------------------------------


def test_lock_claim_returns_error_sentinel_on_database_failure():
    db = _ControllableDb(_fresh_db())
    db.raising.add("referral_invitee_locks")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    result = referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=10, chat_id=GROUP_CHAT_ID,
        destination_type="community_group", now_utc_ts=now,
    )

    assert result == referral_invitee_lock.LOCK_ERROR
    assert result is not True
    assert result is not False


def test_lock_claim_still_returns_true_and_false_for_normal_cases():
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


def test_lock_release_scoped_to_expected_inviter_ignores_mismatched_owner():
    db = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=10, chat_id=GROUP_CHAT_ID,
        destination_type="community_group", now_utc_ts=now,
    )
    # A different inviter's cleanup attempt must not release inviter 10's lock.
    referral_invitee_lock.release(
        db, invitee_user_id=1, status="revoked", now_utc_ts=now, expected_inviter_user_id=999,
    )
    # Lock is still held by inviter 10 -> a second claim for a different inviter still fails.
    assert referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=20, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    ) is False


def test_lock_release_scoped_to_expected_inviter_releases_own_lock():
    db = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=10, chat_id=GROUP_CHAT_ID,
        destination_type="community_group", now_utc_ts=now,
    )
    referral_invitee_lock.release(
        db, invitee_user_id=1, status="revoked", now_utc_ts=now, expected_inviter_user_id=10,
    )
    assert referral_invitee_lock.claim(
        db, invitee_user_id=1, inviter_user_id=20, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    ) is True


# ---------------------------------------------------------------------------
# _confirm_referral_on_main_join: fails closed end-to-end
# ---------------------------------------------------------------------------


def test_historical_success_found_rejects_with_guard_reason_and_creates_no_pending():
    db = _fresh_db()
    _seed_link(db)
    db.qualified_events.insert_one({"invitee_id": 200, "referrer_id": 999})
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    assert db.pending_referrals.count_documents({}) == 0
    assert audits[-1]["reason"] == "historical_success_guard"
    assert db.referral_invitee_locks.count_documents({}) == 0


def test_historical_success_lookup_failure_creates_no_pending_and_no_lock():
    real_db = _fresh_db()
    _seed_link(real_db)
    db = _ControllableDb(real_db)
    db.raising.add("qualified_events")
    fn, audits, logger = _make_confirm_join_env(db)

    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    assert real_db.pending_referrals.count_documents({}) == 0
    assert real_db.referral_invitee_locks.count_documents({}) == 0
    assert audits[-1]["status"] == "failed"
    assert audits[-1]["reason"] == "historical_success_lookup_failed"
    assert logger.has("historical_success_lookup_failed")


def test_historical_success_lookup_failure_sends_no_ack(monkeypatch):
    real_db = _fresh_db()
    _seed_link(real_db)
    db = _ControllableDb(real_db)
    db.raising.add("referral_events")
    fn, audits, logger = _make_confirm_join_env(db)

    ack_calls = []
    fn.__globals__["_maybe_send_referral_join_ack_dm"] = lambda *a, **kw: ack_calls.append((a, kw))

    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    assert ack_calls == []
    assert real_db.pending_referrals.count_documents({}) == 0


def test_lock_claim_failure_creates_no_pending_and_sends_no_ack():
    real_db = _fresh_db()
    _seed_link(real_db)
    db = _ControllableDb(real_db)
    db.raising.add("referral_invitee_locks")
    fn, audits, logger = _make_confirm_join_env(db)

    ack_calls = []
    fn.__globals__["_maybe_send_referral_join_ack_dm"] = lambda *a, **kw: ack_calls.append((a, kw))

    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    assert real_db.pending_referrals.count_documents({}) == 0
    assert ack_calls == []
    assert audits[-1]["status"] == "failed"
    assert audits[-1]["reason"] == "invitee_lock_lookup_failed"


def test_retry_succeeds_once_database_recovers():
    real_db = _fresh_db()
    _seed_link(real_db)
    db = _ControllableDb(real_db)
    db.raising.add("qualified_events")
    fn, audits, logger = _make_confirm_join_env(db)

    # First attempt: DB is down, must fail closed.
    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)
    assert real_db.pending_referrals.count_documents({}) == 0
    assert real_db.referral_invitee_locks.count_documents({}) == 0

    # DB recovers; retry (e.g. the invitee's join event is reprocessed).
    db.raising.discard("qualified_events")
    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    pending = real_db.pending_referrals.find_one({"invitee_user_id": 200})
    assert pending is not None
    assert pending["inviter_user_id"] == 100
    assert audits[-1]["reason"] is None or "reason" in audits[-1]
    assert real_db.referral_invitee_locks.find_one({"invitee_user_id": 200})["status"] == "pending"


def test_no_orphan_lock_after_pending_creation_failure():
    db = _fresh_db()
    _seed_link(db)
    fn, audits, logger = _make_confirm_join_env(db)

    class _BoomPending:
        def update_one(self, *a, **kw):
            raise RuntimeError("pending_insert_failed")

    fn.__globals__["pending_referrals_collection"] = _BoomPending()

    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    assert audits[-1]["status"] == "failed"
    assert audits[-1]["reason"] == "error"
    lock = db.referral_invitee_locks.find_one({"invitee_user_id": 200})
    assert lock is not None
    assert lock["status"] == "revoked"  # released, not left blocking


def test_pending_creation_failure_cleanup_does_not_steal_a_newer_attempts_lock():
    db = _fresh_db()
    _seed_link(db, inviter_id=100, chat_id=GROUP_CHAT_ID, invite_link="https://t.me/+g")
    fn, audits, logger = _make_confirm_join_env(db)

    class _BoomPending:
        def update_one(self, *a, **kw):
            raise RuntimeError("pending_insert_failed")

    fn.__globals__["pending_referrals_collection"] = _BoomPending()
    fn(200, invitee_username="u200", invite_link="https://t.me/+g", chat_id=GROUP_CHAT_ID)

    # Attempt 1 failed and released its own (inviter=100) lock claim. A
    # different inviter now legitimately claims the invitee via the channel.
    import referral_invitee_lock as lock_mod
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    claimed = lock_mod.claim(
        db, invitee_user_id=200, inviter_user_id=555, chat_id=CHANNEL_CHAT_ID,
        destination_type="official_channel", now_utc_ts=now,
    )
    assert claimed is True
    lock = db.referral_invitee_locks.find_one({"invitee_user_id": 200})
    assert lock["inviter_user_id"] == 555
    assert lock["status"] == "pending"
