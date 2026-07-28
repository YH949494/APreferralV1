"""Tests for official_channel_reopen_audit.py — the dry-run-by-default
audit + repair tool for historically revoked official_channel referrals
(status="revoked", reason="insufficient_engagement") left behind by the
first_checkin qualification requirement that scheduler.py no longer
applies to official_channel destinations.
"""

from datetime import datetime, timedelta, timezone

import mongomock

import official_channel_reopen_audit as audit_mod
import referral_invitee_lock
import scheduler


def _fresh_db():
    db = mongomock.MongoClient().db
    referral_invitee_lock.ensure_indexes(db)
    return db


def _revoked_row(db, *, inviter=11, invitee=22, chat_id=-100999, referral_join_seen_at_utc=None):
    doc = {
        "destination_type": "official_channel",
        "status": "revoked",
        "revoked_reason": "insufficient_engagement",
        "inviter_user_id": inviter,
        "invitee_user_id": invitee,
        "destination_chat_id": chat_id,
    }
    if referral_join_seen_at_utc is not None:
        doc["referral_join_seen_at_utc"] = referral_join_seen_at_utc
        doc["created_at_utc"] = referral_join_seen_at_utc
    db.pending_referrals.insert_one(doc)


def test_finds_only_official_channel_insufficient_engagement_rows():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    # community_group revocation for the same reason must be ignored.
    db.pending_referrals.insert_one(
        {
            "destination_type": "community_group",
            "status": "revoked",
            "revoked_reason": "insufficient_engagement",
            "inviter_user_id": 11,
            "invitee_user_id": 23,
        }
    )
    # official_channel revoked for a different reason must be ignored.
    db.pending_referrals.insert_one(
        {
            "destination_type": "official_channel",
            "status": "revoked",
            "revoked_reason": "not_in_official_channel",
            "inviter_user_id": 11,
            "invitee_user_id": 24,
        }
    )
    rows = audit_mod._find_candidate_rows(db)
    assert len(rows) == 1
    assert rows[0]["invitee_user_id"] == 22


def test_currently_subscribed_invitee_with_no_history_is_eligible():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, datetime.now(timezone.utc))

    assert report["eligible_count"] == 1
    assert report["ineligible_count"] == 0
    assert report["eligible_rows"][0]["invitee_user_id"] == 22


def test_left_channel_invitee_is_ineligible():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "left"

    report = audit_mod.build_report(db, datetime.now(timezone.utc))

    assert report["eligible_count"] == 0
    assert report["ineligible_rows"][0]["reason"] == "not_currently_subscribed:left"


def test_self_referral_row_is_ineligible():
    db = _fresh_db()
    _revoked_row(db, inviter=22, invitee=22)
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, datetime.now(timezone.utc))

    assert report["eligible_count"] == 0
    assert report["ineligible_rows"][0]["reason"] == "self_invite"


def test_already_settled_historically_is_ineligible():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    db.referral_award_events.insert_one({"invitee_user_id": 22, "award_key": "ref:22"})
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, datetime.now(timezone.utc))

    assert report["eligible_count"] == 0
    assert report["ineligible_rows"][0]["reason"] == "already_settled_historically"


def test_abuse_blocked_invitee_is_ineligible():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    now = datetime.now(timezone.utc)
    db.referral_audit.insert_one(
        {"invitee_user_id": 22, "created_at": now - timedelta(days=1), "reason": "abuse"}
    )
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, now)

    assert report["eligible_count"] == 0
    assert report["ineligible_rows"][0]["reason"] == "abuse_blocked"


def test_dry_run_never_mutates_rows():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, datetime.now(timezone.utc))
    assert report["eligible_count"] == 1
    # build_report alone (no --commit / _reopen call) must not touch the row.
    row = db.pending_referrals.find_one({"invitee_user_id": 22})
    assert row["status"] == "revoked"


def test_commit_reopens_only_eligible_rows_with_audit_metadata():
    db = _fresh_db()
    _revoked_row(db, invitee=22)
    _revoked_row(db, invitee=33, inviter=12)
    scheduler._get_official_channel_member_status = (
        lambda uid, chat_id=None: "member" if uid == 22 else "left"
    )
    now = datetime.now(timezone.utc)

    report = audit_mod.build_report(db, now)
    result = audit_mod._reopen(db, report["eligible_rows"], now)

    assert result["reopened_count"] == 1
    assert result["lock_blocked_pending_ids"] == []
    reopened_row = db.pending_referrals.find_one({"invitee_user_id": 22})
    assert reopened_row["status"] == "pending"
    assert reopened_row["reopened_reason"] == "policy_change_remove_checkin_requirement"
    assert reopened_row["original_status"] == "revoked"
    assert reopened_row["original_reason"] == "insufficient_engagement"
    assert reopened_row["reopened_at"] is not None
    # Live revocation fields must be cleared, not just left stale, or
    # build_public_referral_status() keeps showing "Not eligible" to the
    # inviter even after this row later settles.
    assert "revoked_reason" not in reopened_row
    assert "qualification_failure_reason" not in reopened_row


def test_left_during_original_hold_window_is_ineligible_even_if_rejoined():
    # Invitee is subscribed again today, but the leave/rejoin cache shows
    # they left partway through their *original* hold window — reopening
    # would just have the next settle_pending_referrals() pass re-revoke
    # the row as left_before_hold, so the audit must not count it eligible.
    db = _fresh_db()
    now = datetime.now(timezone.utc)
    join_time = now - timedelta(hours=200)  # well past a 48h hold
    _revoked_row(db, invitee=22, referral_join_seen_at_utc=join_time)
    db.users.insert_one(
        {"user_id": 22, "left_official_channel_at": join_time + timedelta(hours=20)}
    )
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, now)

    assert report["eligible_count"] == 0
    assert report["ineligible_rows"][0]["reason"] == "left_during_hold"


def test_left_after_original_hold_window_completed_is_still_eligible():
    # A leave recorded well after the original hold already completed
    # (e.g. the invitee left recently, long after retaining through their
    # actual hold) must not block reopening.
    db = _fresh_db()
    now = datetime.now(timezone.utc)
    join_time = now - timedelta(hours=200)
    _revoked_row(db, invitee=22, referral_join_seen_at_utc=join_time)
    db.users.insert_one(
        {"user_id": 22, "left_official_channel_at": now - timedelta(hours=1)}
    )
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"

    report = audit_mod.build_report(db, now)

    assert report["eligible_count"] == 1


def test_reopen_skips_row_when_invitee_lock_owned_by_newer_referral():
    # The invitee started a brand-new active referral (any destination)
    # after the historical row was revoked — that newer attribution now
    # owns the invitee-scoped lock, so the historical row must not be
    # reopened alongside it (would create two active pending rows for the
    # same invitee, and settlement processes oldest created_at_utc first,
    # letting the stale inviter win the award over the legitimate one).
    db = _fresh_db()
    _revoked_row(db, invitee=22, inviter=11)
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"
    now = datetime.now(timezone.utc)

    referral_invitee_lock.claim(
        db,
        invitee_user_id=22,
        inviter_user_id=99,
        chat_id=-100555,
        destination_type="community_group",
        now_utc_ts=now,
    )

    report = audit_mod.build_report(db, now)
    result = audit_mod._reopen(db, report["eligible_rows"], now)

    assert result["reopened_count"] == 0
    assert len(result["lock_blocked_pending_ids"]) == 1
    row = db.pending_referrals.find_one({"invitee_user_id": 22})
    assert row["status"] == "revoked"
    # The newer referral's lock ownership must be untouched.
    lock = db.referral_invitee_locks.find_one({"invitee_user_id": 22})
    assert lock["inviter_user_id"] == 99


def test_reopen_succeeds_when_lock_is_free():
    db = _fresh_db()
    _revoked_row(db, invitee=22, inviter=11)
    scheduler._get_official_channel_member_status = lambda uid, chat_id=None: "member"
    now = datetime.now(timezone.utc)

    report = audit_mod.build_report(db, now)
    result = audit_mod._reopen(db, report["eligible_rows"], now)

    assert result["reopened_count"] == 1
    lock = db.referral_invitee_locks.find_one({"invitee_user_id": 22})
    assert lock["inviter_user_id"] == 11
    assert lock["status"] == "pending"
