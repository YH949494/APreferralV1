"""Verification cases for the affiliate milestone announcement gate: the
public Money Room "voucher issued" post must only ever fire once the
matching affiliate_ledger row is durably ISSUED with a real voucher_code —
never on the referral-count threshold alone — and must never double-post
under retry or concurrent-worker conditions.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import database
import scheduler
from fake_mongo import FakeDb


NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


class _OkResp:
    ok = True
    status_code = 200
    text = "ok"


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    fdb["referral_tier_congrats"]._unique_keys = [("user_id", "month_key", "tier")]
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(scheduler, "db", fdb)
    return fdb


def _seed_referrals(fake_db, uid, count, *, now=NOW):
    fake_db["users"].insert_one({"user_id": uid, "username": f"user{uid}", "first_name": "U"})
    month_key = scheduler._month_start_kl(now).date().isoformat()
    for i in range(count):
        fake_db["referral_events"].insert_one(
            {"inviter_id": uid, "invitee_id": i, "event": "referral_settled", "occurred_at": now, "month_key": month_key}
        )


def _seed_ledger(fake_db, uid, tier_label, *, status="ISSUED", voucher_code="AFFCODE", now=NOW):
    year_month = scheduler._month_start_kl(now).strftime("%Y%m")
    fake_db["affiliate_ledger"].insert_one(
        {
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": uid,
            "year_month": year_month,
            "tier": tier_label,
            "status": status,
            "voucher_code": voucher_code,
        }
    )


def _sent_count(monkeypatch):
    sent = {"count": 0}

    def _fake_post(*args, **kwargs):
        sent["count"] += 1
        return _OkResp()

    monkeypatch.setattr(scheduler.requests, "post", _fake_post)
    return sent


# 1. Threshold reached + voucher issued -> announcement sent once.
def test_issued_voucher_sends_announcement_once(fake_db, monkeypatch):
    _seed_referrals(fake_db, 501, 10)
    _seed_ledger(fake_db, 501, "T1")
    sent = _sent_count(monkeypatch)

    scheduler.maybe_shout_referral_congrats(501, NOW)

    assert sent["count"] == 1
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 501, "tier": 10}) == 1


# 2. Threshold reached + OUT_OF_STOCK -> no announcement.
def test_out_of_stock_ledger_blocks_announcement(fake_db, monkeypatch):
    _seed_referrals(fake_db, 502, 50)
    _seed_ledger(fake_db, 502, "T3", status="OUT_OF_STOCK", voucher_code=None)
    sent = _sent_count(monkeypatch)

    scheduler.maybe_shout_referral_congrats(502, NOW)

    assert sent["count"] == 0
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 502}) == 0


# 3. Threshold reached + SETTLING -> no announcement.
def test_settling_ledger_blocks_announcement(fake_db, monkeypatch):
    _seed_referrals(fake_db, 503, 50)
    _seed_ledger(fake_db, 503, "T3", status="SETTLING", voucher_code=None)
    sent = _sent_count(monkeypatch)

    scheduler.maybe_shout_referral_congrats(503, NOW)

    assert sent["count"] == 0


# 4. Threshold reached + APPROVED but no voucher -> no announcement.
def test_approved_without_voucher_blocks_announcement(fake_db, monkeypatch):
    _seed_referrals(fake_db, 504, 25)
    _seed_ledger(fake_db, 504, "T2", status="APPROVED", voucher_code=None)
    sent = _sent_count(monkeypatch)

    scheduler.maybe_shout_referral_congrats(504, NOW)

    assert sent["count"] == 0


# 5. ISSUED but voucher_code missing -> no announcement, warning log.
def test_issued_without_voucher_code_blocks_announcement_and_warns(fake_db, monkeypatch, caplog):
    _seed_referrals(fake_db, 505, 150)
    _seed_ledger(fake_db, 505, "T4", status="ISSUED", voucher_code="")
    sent = _sent_count(monkeypatch)

    with caplog.at_level("WARNING", logger="scheduler"):
        scheduler.maybe_shout_referral_congrats(505, NOW)

    assert sent["count"] == 0
    assert any(
        "missing_voucher_code" in rec.getMessage() for rec in caplog.records
    )


# 6/7. Previously OUT_OF_STOCK -> later reconciled to ISSUED: the retry sweep
# (which the 5-min scheduler job runs right after reward reconciliation)
# picks it up and sends exactly once, and repeated sweeps never duplicate.
def test_retry_sweep_sends_once_after_later_issuance(fake_db, monkeypatch):
    _seed_referrals(fake_db, 506, 50)
    _seed_ledger(fake_db, 506, "T3", status="OUT_OF_STOCK", voucher_code=None)
    sent = _sent_count(monkeypatch)

    # First settle-time attempt: voucher not issued yet -> no announcement.
    scheduler.maybe_shout_referral_congrats(506, NOW)
    assert sent["count"] == 0

    # Reconciliation later issues the voucher.
    fake_db["affiliate_ledger"]._docs[0]["status"] = "ISSUED"
    fake_db["affiliate_ledger"]._docs[0]["voucher_code"] = "AFFCODE-LATE"

    # Scheduler retry sweep picks it up.
    result = scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=NOW)
    assert result["scanned"] == 1
    assert sent["count"] == 1

    # Running the sweep again (and re-evaluating the settle path) must not
    # duplicate the public post.
    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=NOW)
    scheduler.maybe_shout_referral_congrats(506, NOW)
    assert sent["count"] == 1
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 506, "tier": 50}) == 1


# 8. Two workers race -> at most one public announcement (unique index on
# (user_id, month_key, tier) makes the claim insert atomic).
def test_concurrent_claim_race_sends_only_once(fake_db, monkeypatch):
    _seed_referrals(fake_db, 508, 10)
    _seed_ledger(fake_db, 508, "T1")
    sent = _sent_count(monkeypatch)

    scheduler._attempt_affiliate_milestone_congrats(508, 10, 10, NOW)
    # A second concurrent worker re-attempting the same milestone must lose
    # the atomic insert race and skip.
    scheduler._attempt_affiliate_milestone_congrats(508, 10, 10, NOW)

    assert sent["count"] == 1
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 508, "tier": 10}) == 1


# 9. T1 already announced, later T2 issued -> only T2 posts, T1 does not repost.
def test_prior_tier_does_not_repost_when_next_tier_announces(fake_db, monkeypatch):
    _seed_referrals(fake_db, 509, 25)
    _seed_ledger(fake_db, 509, "T1")
    _seed_ledger(fake_db, 509, "T2")
    sent = _sent_count(monkeypatch)

    # T1 already announced earlier this month.
    scheduler._attempt_affiliate_milestone_congrats(509, 10, 10, NOW)
    assert sent["count"] == 1

    # New referral settlement pushes the count to 25 (T2).
    scheduler.maybe_shout_referral_congrats(509, NOW)

    assert sent["count"] == 2
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 509, "tier": 10}) == 1
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 509, "tier": 25}) == 1


# 10. Username masking remains unchanged by the gating.
def test_username_masking_still_applied_when_gated_send_succeeds(fake_db, monkeypatch):
    fake_db["users"].insert_one({"user_id": 510, "username": "kamilszs", "first_name": "Kamil"})
    month_key = scheduler._month_start_kl(NOW).date().isoformat()
    for i in range(10):
        fake_db["referral_events"].insert_one(
            {"inviter_id": 510, "invitee_id": i, "event": "referral_settled", "occurred_at": NOW, "month_key": month_key}
        )
    _seed_ledger(fake_db, 510, "T1")

    captured = {}

    def _fake_post(url, json=None, timeout=None):
        captured["text"] = json["text"]
        return _OkResp()

    monkeypatch.setattr(scheduler.requests, "post", _fake_post)

    scheduler.maybe_shout_referral_congrats(510, NOW)

    assert "kami****" in captured["text"]
    assert "kamilszs" not in captured["text"]
    assert "@" not in captured["text"]
    assert "voucher issued!" in captured["text"]


# --- Late issuance across a month boundary (7-day retention gate) ----------
# A milestone earned Sep 28 (KL) is held PENDING_RETENTION and only ISSUED
# ~Oct 5, still filed under year_month="202609". The sweep must announce it
# under September, exactly once, with copy that doesn't claim "this month".

EARNED_SEP = datetime(2026, 9, 28, 4, 0, tzinfo=timezone.utc)   # 12:00 KL
ISSUED_OCT = datetime(2026, 10, 5, 4, 0, tzinfo=timezone.utc)   # 12:00 KL


def _seed_entitlement(fake_db, uid, tier_label, *, year_month, status="ISSUED", issued_at=None, voucher_code="AFFCODE", **extra):
    fake_db["affiliate_ledger"].insert_one(
        {
            **extra,
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": uid,
            "year_month": year_month,
            "entitlement_month": year_month,
            "tier": tier_label,
            "status": status,
            "voucher_code": voucher_code,
            "issued_at": issued_at,
        }
    )


def _capture_posts(monkeypatch):
    posts = []

    def _fake_post(url, json=None, timeout=None):
        posts.append(json["text"])
        return _OkResp()

    monkeypatch.setattr(scheduler.requests, "post", _fake_post)
    return posts


def test_previous_month_milestone_issued_next_month_announced_once(fake_db, monkeypatch):
    _seed_referrals(fake_db, 601, 10, now=EARNED_SEP)
    _seed_entitlement(
        fake_db, 601, "T1", year_month="202609", status="PENDING_RETENTION", voucher_code=None,
        retention_required_seconds=7 * 86400,
    )
    posts = _capture_posts(monkeypatch)

    # Sep 28: tier reached, but the reward is held for retention -> no post.
    scheduler.maybe_shout_referral_congrats(601, EARNED_SEP)
    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=EARNED_SEP)
    assert posts == []

    # Oct 5: retention completes and the voucher is issued.
    ledger = fake_db["affiliate_ledger"]._docs[0]
    ledger.update({"status": "ISSUED", "voucher_code": "AFFCODE-SEP", "issued_at": ISSUED_OCT})

    result = scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)
    assert result == {"scanned": 1, "attempted": 1}
    assert len(posts) == 1
    assert "in September" in posts[0]
    assert "this month" not in posts[0]
    # September is closed: no "Next:" nudge toward an unreachable tier.
    assert "Next:" not in posts[0]
    assert "Held the Official Channel for 7 days" in posts[0]

    claim = fake_db["referral_tier_congrats"].find_one({"user_id": 601, "tier": 10})
    assert claim["month_key"] == "2026-09-01"
    assert claim["sent_at"] == ISSUED_OCT  # timestamps stay on real now

    # Repeat sweeps (same tick, and later that day) never re-announce.
    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)
    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=datetime(2026, 10, 5, 10, 0, tzinfo=timezone.utc))
    assert len(posts) == 1
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 601}) == 1


def test_current_month_milestone_announcement_unchanged(fake_db, monkeypatch):
    _seed_referrals(fake_db, 602, 10, now=ISSUED_OCT)
    _seed_entitlement(fake_db, 602, "T1", year_month="202610", issued_at=ISSUED_OCT)
    posts = _capture_posts(monkeypatch)

    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)
    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)

    assert len(posts) == 1
    assert "just hit <b>10 valid referrals</b> this month" in posts[0]
    assert "Next: 25 refs" in posts[0]
    claim = fake_db["referral_tier_congrats"].find_one({"user_id": 602, "tier": 10})
    assert claim["month_key"] == "2026-10-01"


def test_previous_month_milestone_already_announced_is_not_reposted(fake_db, monkeypatch):
    # Issued and announced within September by the eager path; the October
    # sweep sees the same row but dedups on the September slot.
    _seed_referrals(fake_db, 603, 10, now=EARNED_SEP)
    _seed_entitlement(fake_db, 603, "T1", year_month="202609", issued_at=EARNED_SEP)
    posts = _capture_posts(monkeypatch)

    scheduler.maybe_shout_referral_congrats(603, EARNED_SEP)
    assert len(posts) == 1

    # Even a row re-stamped as issued in October must not repost.
    fake_db["affiliate_ledger"]._docs[0]["issued_at"] = ISSUED_OCT
    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)
    assert len(posts) == 1
    assert fake_db["referral_tier_congrats"].count_documents({"user_id": 603, "tier": 10}) == 1


def test_previous_month_rows_outside_late_window_are_not_swept(fake_db, monkeypatch):
    posts = _capture_posts(monkeypatch)
    fake_db["users"].insert_one({"user_id": 604, "username": "user604"})
    # Issued inside its own month: the in-month sweep already owned it.
    _seed_entitlement(fake_db, 604, "T1", year_month="202609", issued_at=EARNED_SEP)
    # Issued after month close but older than the late window: stale news.
    _seed_entitlement(fake_db, 604, "T2", year_month="202609", issued_at=datetime(2026, 10, 1, 4, 0, tzinfo=timezone.utc))
    # Two months back: never swept.
    _seed_entitlement(fake_db, 604, "T3", year_month="202608", issued_at=ISSUED_OCT)

    result = scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)

    assert result == {"scanned": 0, "attempted": 0}
    assert posts == []


@pytest.mark.parametrize(
    "retention_seconds, expected",
    [
        (86400, "Held the Official Channel for 1 day to unlock it"),
        (3 * 86400, "Held the Official Channel for 3 days to unlock it"),
        (None, "Reward unlocked"),  # late for another reason (e.g. restock): no invented hold
    ],
)
def test_late_announcement_quotes_the_ledgers_frozen_hold(fake_db, monkeypatch, retention_seconds, expected):
    fake_db["users"].insert_one({"user_id": 605, "username": "user605"})
    extra = {"retention_required_seconds": retention_seconds} if retention_seconds else {}
    _seed_entitlement(fake_db, 605, "T1", year_month="202609", issued_at=ISSUED_OCT, **extra)
    posts = _capture_posts(monkeypatch)

    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)

    assert len(posts) == 1
    assert expected in posts[0]
    if retention_seconds is None:
        assert "Held the Official Channel" not in posts[0]


def test_rollback_waived_reward_uses_neutral_copy(fake_db, monkeypatch):
    """Retention rollback: a held September row released in October still
    carries its frozen 7-day retention_required_seconds, but it never served
    the hold — the announcement must not claim it did."""
    fake_db["users"].insert_one({"user_id": 609, "username": "user609"})
    _seed_entitlement(
        fake_db, 609, "T1", year_month="202609", issued_at=ISSUED_OCT,
        retention_required_seconds=7 * 86400, retention_waived_at=ISSUED_OCT,
    )
    posts = _capture_posts(monkeypatch)

    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)

    assert len(posts) == 1
    assert "Reward unlocked" in posts[0]
    assert "Held the Official Channel" not in posts[0]
    assert "7 days" not in posts[0]


def test_immediate_release_reward_uses_neutral_copy(fake_db, monkeypatch):
    """Post-rollback immediate issuance carries no retention fields at all."""
    fake_db["users"].insert_one({"user_id": 610, "username": "user610"})
    _seed_entitlement(fake_db, 610, "T1", year_month="202609", issued_at=ISSUED_OCT)
    posts = _capture_posts(monkeypatch)

    scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT)

    assert len(posts) == 1
    assert "Held the Official Channel" not in posts[0]


def test_batch_limit_does_not_starve_behind_already_announced_rows(fake_db, monkeypatch):
    # Announced rows stay ISSUED forever; if the limit were applied before
    # dropping them, every run would get the same done prefix and a late
    # previous-month row would age out of its window unannounced.
    for uid in (606, 607, 608):
        fake_db["users"].insert_one({"user_id": uid, "username": f"user{uid}"})
        _seed_entitlement(fake_db, uid, "T1", year_month="202610", issued_at=ISSUED_OCT)
    fake_db["users"].insert_one({"user_id": 609, "username": "user609"})
    _seed_entitlement(fake_db, 609, "T1", year_month="202609", issued_at=ISSUED_OCT)
    posts = _capture_posts(monkeypatch)

    first = scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT, batch_limit=1)
    # The expiring previous-month row goes first.
    assert first["attempted"] == 1
    assert "in September" in posts[0]

    for _ in range(3):
        scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT, batch_limit=1)
    assert len(posts) == 4
    assert fake_db["referral_tier_congrats"].count_documents({}) == 4

    # Nothing left: a further run scans nothing and posts nothing.
    assert scheduler.retry_pending_affiliate_milestone_congrats(now_utc_ts=ISSUED_OCT, batch_limit=1) == {
        "scanned": 0, "attempted": 0,
    }
    assert len(posts) == 4
