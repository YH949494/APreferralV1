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
