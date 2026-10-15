"""Final UIM coverage vs latest observed redemption, and what a MISSING row may conclude.

Business contract under test: the seven-day measurement is completed BEFORE a
UIM file reaches Databot, so a committed final batch is evaluated at once (no
joined_at + 7 days, no upload + 7 days). A batch also declares which GMT+8 days
it completely covers; days inside that interval with no row are confirmed
zero-result days. The brief's example: the 12 Oct upload covers 28 Sep-4 Oct
while its latest redemption row is 2 Oct.

Synthetic mongomock databases and Databot's real wire contract (FakeFeed).
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

import affiliate_qualification as aq
import affiliate_v2_report as rep
import uim_coverage as ucov
import uim_redemption_sync as usync
from scripts import affiliate_qualification_admin as admin
from test_affiliate_v2_preview_report import PREVIEW, LIVE, counts
from test_affiliate_v2_report import (
    NOW, FakeFeed, _snapshot, activate, disabled_world, invitee, report, run, snapshot as live_snapshot, sync, urow,
)
from test_affiliate_welcome_redemption_qualification import World
from test_uim_redemption_sync import DATABOT_CONFIG, coverage_block


def utc(y, m, d, h=0, mi=0):
    return datetime(y, m, d, h, mi, tzinfo=timezone.utc)


def kl(y, m, d, h=0, mi=0):
    """A GMT+8 wall-clock instant."""
    return aq.KL_TZ.localize(datetime(y, m, d, h, mi)).astimezone(timezone.utc)


BRIEF = coverage_block("2026-09-28", "2026-10-04", latest_observed="2026-10-02T10:00:00+08:00")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in ("AFFILIATE_V2_REPORT_ENABLED", "AFFILIATE_V2_REPORT_STALE_AFTER_SECONDS",
                 "AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING"):
        monkeypatch.delenv(name, raising=False)


def policy(days=3, **over):
    p = {"enabled": True, "coverage_basis": "voucher_redemption_date", "welcome_redemption_window_days": days,
         "attestation": {"by": "UIM data owner", "reference": "OPS-4242"}}
    p.update(over)
    return p


def set_policy(w, value):
    w.db[aq.CONTROL_COLLECTION].update_one(
        {"_id": aq.CONTROL_ID}, {"$set": {"source_config.absence_rejection": value}} if value
        else {"$unset": {"source_config.absence_rejection": ""}})


def brief_world(*, coverage=BRIEF, days=3, extra=()):
    """12 Oct upload: rows 28 Sep-2 Oct only; the report covers 28 Sep-4 Oct (GMT+8).

    11 redeems 2 Oct and qualifies. 12/13 never redeemed and their whole window is covered -> would reject.
    14, 15, 16 never redeemed but their window leaves coverage -> stay pending. 17 has held evidence.
    """
    w = disabled_world()
    set_policy(w, policy(days) if days else None)
    feed = FakeFeed()
    rows = [
        invitee(w, feed, 1, 11, account="Acct11", wall="2026-10-02 10:00:00", at=kl(2026, 9, 30, 9),
                fields={"after_total_bet_amount": "80.00"}),
        invitee(w, feed, 1, 12, at=kl(2026, 9, 28, 10)),     # window 28 Sep-1 Oct
        invitee(w, feed, 1, 13, at=kl(2026, 10, 1, 10)),     # window 1-4 Oct: ends on a trailing zero-result day
        invitee(w, feed, 1, 14, at=kl(2026, 10, 3, 11)),     # window 3-6 Oct: leaves coverage
        invitee(w, feed, 1, 15, at=kl(2026, 10, 2, 9)),      # window 2-5 Oct: leaves coverage by one day
        invitee(w, feed, 1, 16, at=kl(2026, 9, 25, 9)),      # window 25-28 Sep: starts before coverage
        invitee(w, feed, 1, 17, account="Acct17", wall="2026-10-01 10:00:00", at=kl(2026, 9, 30, 9),
                fields={"seven_day_complete": False, "after_total_bet_amount": "900"}),
        *extra,
    ]
    feed.add_batch([r for r in rows if r], coverage=coverage)
    sync(w, feed)
    return w, feed


WIDE = {"start": "2026-09-01", "end": "2026-10-31"}      # the referral-join cohort window used by Databot's filter


def states(w, now=NOW):
    live_snapshot(w, now=now)
    return report(w, now=now, **WIDE)


def totals(body, names=PREVIEW):
    return counts(body["totals"], names)


def pending_status(w, uid):
    return w.db.pending_referrals.find_one({"invitee_user_id": uid})["status"]


# ── Parsing Databot's blocks ─────────────────────────────────────────────────

def test_batch_block_with_utc_bounds_matching_gmt8_midnights_is_accepted():
    block = ucov.parse_batch_coverage(coverage_block("2026-09-28", "2026-10-04"))
    assert block["status"] == "complete" and (block["start_date"], block["end_date"]) == ("2026-09-28", "2026-10-04")
    raw = coverage_block("2026-09-28", "2026-10-04")
    assert raw["start_utc"].startswith("2026-09-27T16:00:00") and raw["end_utc_exclusive"].startswith("2026-10-04T16:00:00")


@pytest.mark.parametrize("mutate", [
    lambda b: b.update(start_utc="2026-09-28T00:00:00+00:00"),                 # treated GMT+8 midnight as UTC midnight
    lambda b: b.update(end_utc_exclusive="2026-10-04T16:00:00+00:00".replace("10-04", "10-05")),   # off by one day
    lambda b: b.update(end_date="2026-09-27"),                                  # end before start
    lambda b: b.update(status="complete", source="observed_rows"),              # inferred rows cannot be 'complete'
    lambda b: b.update(basis="unverified"),                                     # complete without a basis
    lambda b: b.update(timezone="UTC"),
    lambda b: b.update(status="brand_new"),
    lambda b: b.update(revision="1"),
    lambda b: b.update(start_date="28/09/2026"),
])
def test_malformed_or_inconsistent_complete_blocks_are_rejected_not_trusted(mutate):
    raw = coverage_block("2026-09-28", "2026-10-04")
    mutate(raw)
    assert ucov.parse_batch_coverage(raw) is None
    assert ucov.parse_batch_coverage(None) is None and ucov.parse_batch_coverage("x") is None


def test_sync_stores_only_the_whitelisted_block_idempotently():
    w, feed = brief_world()
    (batch,) = list(w.db[usync.BATCHES_COLLECTION].find())
    assert batch["coverage"]["status"] == "complete" and batch["coverage"]["end_date"] == "2026-10-04"
    assert set(batch["coverage"]) == {"revision", "status", "source", "basis", "start_date", "end_date",
                                      "latest_observed_redemption_at", "fingerprint", "undeclared_legacy"}
    again = sync(w, feed, now=NOW + timedelta(minutes=1))
    assert again["ok"] and w.db[usync.BATCHES_COLLECTION].count_documents({}) == 1
    assert w.db[usync.BATCHES_COLLECTION].find_one()["coverage"] == batch["coverage"]
    assert w.db[ucov.LEDGER_HISTORY_COLLECTION].count_documents({}) == 1        # unchanged ledger: no new audit row


# ── Final coverage vs latest observed vs sync vs snapshot ────────────────────

def test_final_coverage_extends_beyond_the_latest_observed_redemption():
    w, _ = brief_world()
    cov = ucov.effective_coverage(w.db, now_utc=NOW)
    assert cov["status"] == "complete" and cov["primary_basis"] == "voucher_redemption_date"
    assert (cov["final_start"].isoformat(), cov["final_end"].isoformat()) == ("2026-09-28", "2026-10-04")
    assert cov["latest_observed_redemption_at"] == kl(2026, 10, 2, 10)             # not 4 Oct
    health = usync.sync_health(w.db, now_utc=NOW)
    assert health["observed_end"] == "2026-10-02T10:00:00" and "coverage_end" not in health
    assert health["final_coverage"]["final_coverage_end"] == "2026-10-04"
    assert health["last_success_at"] == NOW.isoformat()                            # the third and fourth facts are separate too


def test_report_keeps_the_four_timestamps_apart():
    w, _ = brief_world()
    later = NOW + timedelta(minutes=5)
    body = states(w, now=later)
    cov = body["uim_coverage"]
    assert cov["status"] == "complete" and cov["basis"] == "voucher_redemption_date"
    assert (cov["final_coverage_start"], cov["final_coverage_end"]) == ("2026-09-28", "2026-10-04")
    assert cov["intervals"] == [{"start_date": "2026-09-28", "end_date": "2026-10-04"}] and cov["gaps"] == []
    assert cov["latest_observed_redemption_at"] == "2026-10-02T10:00:00+08:00"
    assert body["source"]["last_sync_success_at"] == NOW.isoformat()
    assert body["snapshot"]["generated_at"] == later.isoformat()
    assert body["uim_coverage_end"] == "2026-10-04T23:59:59+08:00"                 # deprecated alias = FINAL end only
    assert cov["absence_rejection"] == {"active": True, "blockers": []}


def test_inferred_unverified_coverage_never_reports_a_final_end():
    w, _ = brief_world(coverage=coverage_block("2026-09-28", "2026-10-02", status="inferred_unverified",
                                               source="observed_rows", basis="unverified"))
    body = states(w)
    cov = body["uim_coverage"]
    assert cov["status"] == "inferred_unverified" and cov["final_coverage_end"] is None
    assert body["uim_coverage_end"] is None
    assert cov["latest_observed_redemption_at"] == "2026-10-02T10:00:00+08:00"
    assert "no_complete_coverage" in cov["absence_rejection"]["blockers"] and not cov["absence_rejection"]["active"]


# ── Absence-based rejection: only inside proven complete coverage ────────────

def test_absent_row_under_complete_coverage_rejects_and_outside_coverage_stays_pending():
    w, _ = brief_world()
    body = states(w)
    assert totals(body) == [1, 4, 2, 0]                     # 11 | 14 15 16 17 | 12 13
    assert body["totals"][PREVIEW["turnover"]] == "80.00"


def test_trailing_zero_result_days_are_covered_not_pending_forever():
    # 13's window ends on 4 Oct although the latest redemption row is 2 Oct. With coverage_end = latest row (the
    # old behaviour) that invitee could never be concluded.
    w, _ = brief_world()
    verdicts = ucov.decide_absences(w.db, [13], control=aq.get_control(w.db), cov=ucov.effective_coverage(w.db),
                                    source_ok=True)
    assert verdicts[13] == {"status": "rejected", "reason": ucov.ABSENCE_REASON,
                            "window_start": "2026-10-01", "window_end": "2026-10-04"}
    shrunk, _ = brief_world(coverage=coverage_block("2026-09-28", "2026-10-02"))     # coverage ends at the latest row
    again = ucov.decide_absences(shrunk.db, [13], control=aq.get_control(shrunk.db),
                                 cov=ucov.effective_coverage(shrunk.db), source_ok=True)
    assert again[13]["status"] == "pending" and again[13]["reason"] == "outside_coverage"


def test_an_entirely_zero_row_window_is_confirmed_empty():
    w = disabled_world()
    set_policy(w, policy(1))
    feed = FakeFeed()
    feed.add_batch([invitee(w, feed, 1, 11, account="A11", wall="2026-09-28 09:00:00", at=kl(2026, 9, 27, 9)),
                    invitee(w, feed, 1, 21, at=kl(2026, 10, 3, 11)), None][:1], coverage=BRIEF)
    sync(w, feed)
    # 21 was issued 3 Oct; window 3-4 Oct: the file has no row at all on either day.
    verdicts = ucov.decide_absences(w.db, [21], control=aq.get_control(w.db), cov=ucov.effective_coverage(w.db),
                                    source_ok=True)
    assert verdicts[21]["status"] == "rejected" and (verdicts[21]["window_start"], verdicts[21]["window_end"]) == ("2026-10-03", "2026-10-04")


def test_absent_row_under_unverified_inferred_or_partial_coverage_stays_pending():
    for block in (coverage_block("2026-09-28", "2026-10-04", status="inferred_unverified", source="observed_rows",
                                 basis="unverified"),
                  coverage_block("2026-09-28", "2026-10-04", status="partial"),
                  None):
        w, _ = brief_world(coverage=block)
        body = states(w)
        assert totals(body) == [1, 6, 0, 0], block                      # nothing rejected for a missing row
        assert not body["uim_coverage"]["absence_rejection"]["active"]


def test_no_policy_means_no_absence_rejection_even_with_complete_coverage():
    w, _ = brief_world(days=None)
    body = states(w)
    assert totals(body) == [1, 6, 0, 0]
    assert body["uim_coverage"]["absence_rejection"]["blockers"] == ["absence_policy_not_configured"]


@pytest.mark.parametrize("bad", [
    policy(enabled=False), policy(coverage_basis="referral_join_date"), policy(0), policy(367), policy(True),
    policy(3, attestation={"by": "x"}), policy(3, attestation=None), "yes",
])
def test_an_unattested_or_invalid_policy_is_never_honoured(bad):
    assert ucov.absence_policy({"source_config": {"absence_rejection": bad}})[0] is None
    w, _ = brief_world()
    set_policy(w, bad)
    assert totals(states(w)) == [1, 6, 0, 0]


def test_a_coverage_gap_is_never_treated_as_covered():
    w = disabled_world()
    set_policy(w, policy(2))
    feed = FakeFeed()
    first = [invitee(w, feed, 1, 11, account="A11", wall="2026-09-15 10:00:00", at=kl(2026, 9, 14, 9)),
             invitee(w, feed, 1, 12, at=kl(2026, 9, 16, 9)),         # window 16-18 Sep: inside batch 1
             invitee(w, feed, 1, 13, at=kl(2026, 9, 19, 9)),         # window 19-21 Sep: runs into the gap
             invitee(w, feed, 1, 14, at=kl(2026, 9, 29, 9))]         # window 29 Sep-1 Oct: inside batch 2
    feed.add_batch(first[:1], coverage=coverage_block("2026-09-14", "2026-09-20"))
    feed.add_batch([], coverage=coverage_block("2026-09-28", "2026-10-04"))       # 21-27 Sep were never reported
    sync(w, feed)
    cov = ucov.effective_coverage(w.db)
    assert cov["status"] == "coverage_gap"
    assert [(g["start"].isoformat(), g["end"].isoformat()) for g in cov["bases"]["voucher_redemption_date"]["gaps"]] == [
        ("2026-09-21", "2026-09-27")]
    v = ucov.decide_absences(w.db, [12, 13, 14], control=aq.get_control(w.db), cov=cov, source_ok=True)
    assert (v[12]["status"], v[13]["status"], v[14]["status"]) == ("rejected", "pending", "rejected")
    assert v[13]["reason"] == "coverage_gap"
    body = states(w)
    assert body["status"] != "ok" or body["uim_coverage"]["status"] == "coverage_gap"
    assert body["uim_coverage"]["gaps"] == [{"start_date": "2026-09-21", "end_date": "2026-09-27"}]


def test_overlapping_and_contiguous_complete_batches_merge_into_one_interval():
    w = disabled_world()
    set_policy(w, policy(3))
    feed = FakeFeed()
    for start, end in (("2026-09-28", "2026-10-04"), ("2026-10-05", "2026-10-11"), ("2026-10-08", "2026-10-10")):
        feed.add_batch([], coverage=coverage_block(start, end))
    sync(w, feed)
    cov = ucov.effective_coverage(w.db)
    block = cov["bases"]["voucher_redemption_date"]
    assert [(i["start"].isoformat(), i["end"].isoformat()) for i in block["intervals"]] == [("2026-09-28", "2026-10-11")]
    assert block["gaps"] == [] and cov["status"] == "complete" and cov["ledger"]["matches"] is True


def test_a_later_batch_cannot_conceal_an_earlier_gap_and_partial_batches_add_nothing():
    w = disabled_world()
    feed = FakeFeed()
    feed.add_batch([], coverage=coverage_block("2026-09-01", "2026-09-10"))
    feed.add_batch([], coverage=coverage_block("2026-09-20", "2026-09-30"))
    feed.add_batch([], coverage=coverage_block("2026-09-01", "2026-09-30", status="partial"))   # claims everything
    sync(w, feed)
    cov = ucov.effective_coverage(w.db)
    gaps = cov["bases"]["voucher_redemption_date"]["gaps"]
    assert [(g["start"].isoformat(), g["end"].isoformat()) for g in gaps] == [("2026-09-11", "2026-09-19")]
    assert ucov.covered(cov, "voucher_redemption_date", datetime(2026, 9, 12).date(), datetime(2026, 9, 14).date()) == "coverage_gap"


def test_different_bases_never_merge_and_only_redemption_dates_support_absence():
    w = disabled_world()
    set_policy(w, policy(2))
    feed = FakeFeed()
    feed.add_batch([], coverage=coverage_block("2026-09-28", "2026-10-04", basis="referral_join_date"))
    sync(w, feed)
    cov = ucov.effective_coverage(w.db)
    assert set(cov["bases"]) == {"referral_join_date"}
    assert "no_complete_coverage" in ucov.absence_blockers(aq.get_control(w.db), cov)


def test_batch_whose_rows_are_not_fully_synced_contributes_no_coverage():
    w, feed = brief_world()
    w.db[usync.BATCHES_COLLECTION].update_one({}, {"$set": {"rows_complete": False}})
    cov = ucov.effective_coverage(w.db)
    assert cov["status"] == "partial" and cov["bases"] == {} and cov["excluded"]["rows_not_fully_synced"] == 1
    assert totals(states(w)) == [0, 7, 0, 0]                                     # source partially synced: all held


def test_temporary_feed_failure_keeps_everyone_pending_including_absent_rows():
    w, feed = brief_world()
    feed.fail_after = feed.calls
    assert sync(w, feed, now=NOW + timedelta(minutes=1)).get("error")
    body = states(w, now=NOW + timedelta(minutes=1))
    assert body["source"]["status"] == rep.SOURCE_PARTIAL or "evidence_feed_last_attempt_failed" in body["source"]["problems"]
    assert totals(body) == [0, 7, 0, 0]


def test_stale_sync_keeps_everyone_pending():
    w, _ = brief_world()
    assert totals(states(w, now=NOW + timedelta(hours=3))) == [0, 7, 0, 0]


def test_gmt8_boundary_of_the_issuance_day_decides_whether_the_window_starts_inside():
    w = disabled_world()
    set_policy(w, policy(3))
    feed = FakeFeed()
    feed.add_batch([], coverage=BRIEF)
    for uid, issued in ((31, utc(2026, 9, 27, 16, 30)),     # 00:30 on 28 Sep GMT+8 -> window 28 Sep-1 Oct
                        (32, utc(2026, 9, 27, 15, 59)),     # 23:59 on 27 Sep GMT+8 -> window starts a day early
                        (33, utc(2026, 10, 1, 15, 59))):    # 23:59 on 1 Oct GMT+8   -> window ends 4 Oct
        w.join(1, uid, at=issued)
        w.welcome_code(uid, f"BOUND{uid}", issued_at=issued)
    sync(w, feed)
    v = ucov.decide_absences(w.db, [31, 32, 33], control=aq.get_control(w.db), cov=ucov.effective_coverage(w.db), source_ok=True)
    assert [v[i]["status"] for i in (31, 32, 33)] == ["rejected", "pending", "rejected"]
    assert v[32]["window_start"] == "2026-09-27" and v[33]["window_end"] == "2026-10-04"
    # Day arithmetic is calendar-day based on GMT+8: a window may cross a month boundary and still be covered ...
    cov = ucov.effective_coverage(w.db)
    crossing = ucov.assess_absence(issuance={"status": "ok", "issued_at": kl(2026, 9, 30, 23, 59)}, evidence_present=False,
                                   policy=policy(2), cov=cov, source_ok=True)
    assert (crossing["status"], crossing["window_start"], crossing["window_end"]) == ("rejected", "2026-09-30", "2026-10-02")
    # ... and one issued at 23:59 on 3 Oct reaches 5 Oct, one day past the last covered day.
    past = ucov.assess_absence(issuance={"status": "ok", "issued_at": kl(2026, 10, 3, 23, 59)}, evidence_present=False,
                               policy=policy(2), cov=cov, source_ok=True)
    assert (past["status"], past["reason"]) == ("pending", "outside_coverage")


def test_unknown_or_ambiguous_issuance_stays_pending():
    w, _ = brief_world()
    w.join(1, 41, at=kl(2026, 9, 28, 10))                            # joined, never issued a Welcome voucher
    cov = ucov.effective_coverage(w.db)
    v = ucov.decide_absences(w.db, [41], control=aq.get_control(w.db), cov=cov, source_ok=True)
    assert v[41] == {"status": "pending", "reason": "no_welcome_issuance_record"}
    w.db.voucher_pools.insert_one({"pool_id": "OTHER", "code": "CODE12", "status": "issued"})   # same code in a 2nd pool
    v = ucov.decide_absences(w.db, [12], control=aq.get_control(w.db), cov=cov, source_ok=True)
    assert v[12] == {"status": "pending", "reason": "welcome_recipient_unproven"}


# ── Corrections / voiding / idempotent sync ──────────────────────────────────

def test_a_corrected_or_voided_batch_removes_its_coverage_without_touching_rows():
    w, feed = brief_world()
    assert totals(states(w)) == [1, 4, 2, 0]
    rows_before = w.db[usync.ROWS_COLLECTION].count_documents({})
    (batch_id,) = [b["batch_id"] for b in feed.batches]
    feed.set_coverage(batch_id, coverage_block("2026-09-28", "2026-10-04", status="voided", revision=2))
    sync(w, feed, now=NOW + timedelta(minutes=1))
    assert ucov.effective_coverage(w.db)["bases"] == {}
    body = states(w, now=NOW + timedelta(minutes=2))
    assert totals(body) == [1, 6, 0, 0]
    assert w.db[usync.ROWS_COLLECTION].count_documents({}) == rows_before
    assert w.db[ucov.LEDGER_HISTORY_COLLECTION].count_documents({}) == 2         # audit: ledger changed once


def test_correcting_coverage_upstream_then_syncing_restores_the_conclusions():
    w, feed = brief_world(coverage=coverage_block("2026-09-28", "2026-10-02"))   # first recorded as the latest row
    assert totals(states(w)) == [1, 5, 1, 0]                                     # only 12 concludes (window ends 1 Oct)
    (batch_id,) = [b["batch_id"] for b in feed.batches]
    feed.set_coverage(batch_id, coverage_block("2026-09-28", "2026-10-04", revision=2))   # audited attestation
    sync(w, feed, now=NOW + timedelta(minutes=1))
    assert totals(states(w, now=NOW + timedelta(minutes=2))) == [1, 4, 2, 0]


def test_ledger_missing_or_disagreeing_with_batch_blocks_suspends_absence_conclusions():
    w2, _ = brief_world()
    w2.db[ucov.LEDGER_COLLECTION].delete_many({})
    cov = ucov.effective_coverage(w2.db)
    assert "coverage_ledger_missing" in ucov.absence_blockers(aq.get_control(w2.db), cov)
    assert totals(states(w2)) == [1, 6, 0, 0]
    w3, _ = brief_world()
    w3.db[ucov.LEDGER_COLLECTION].update_one({}, {"$set": {"ledger": {"voucher_redemption_date": [["2026-09-28", "2026-10-11"]]}}})
    assert "coverage_ledger_mismatch" in ucov.absence_blockers(aq.get_control(w3.db), ucov.effective_coverage(w3.db))
    assert totals(states(w3)) == [1, 6, 0, 0]


def test_malformed_ledger_is_recorded_unusable_and_fails_closed():
    w, feed = brief_world()
    out = ucov.store_ledger(w.db, {"contract": "x"}, now_utc=NOW)
    assert out == {"valid": False}
    cov = ucov.effective_coverage(w.db)
    assert cov["ledger"]["valid"] is False and "coverage_ledger_missing" in ucov.absence_blockers(aq.get_control(w.db), cov)
    bad_bounds = feed.ledger()
    bad_bounds["bases"]["voucher_redemption_date"]["intervals"][0]["end_utc_exclusive"] = "2026-10-04T00:00:00+00:00"
    assert ucov.parse_ledger(bad_bounds) is None


# ── Preview == live ──────────────────────────────────────────────────────────

def test_preview_and_live_make_the_same_absence_decisions(monkeypatch):
    w, _ = brief_world()
    when = NOW + timedelta(minutes=1)
    preview = states(w, now=when)
    assert preview["report_mode"] == "preview" and totals(preview) == [1, 4, 2, 0]
    activate(w)
    activate_source = {"$set": {"source_config.absence_rejection": policy(3)}}
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, activate_source)
    before = _snapshot(w.db)
    result = run(w, monkeypatch, now=when)
    assert result["evidence"]["absence"]["rejected"] == 2
    live = states(w, now=when + timedelta(minutes=11))
    assert live["report_mode"] == "live" and counts(live["totals"], LIVE) == [1, 4, 2, 0]
    assert live["totals"][LIVE["turnover"]] == preview["totals"][PREVIEW["turnover"]] == "80.00"
    assert {u: pending_status(w, u) for u in (12, 13)} == {12: "rejected", 13: "rejected"}
    assert all(pending_status(w, u) != "rejected" for u in (14, 15, 16, 17))
    docs = {d["invitee_id"]: d for d in w.db[aq.EVIDENCE_COLLECTION].find({"source": ucov.ABSENCE_SOURCE})}
    assert set(docs) == {12, 13}
    d = docs[13]
    assert (d["status"], d["reason"]) == (aq.EV_REJECTED, ucov.ABSENCE_REASON)
    assert d["absence"]["window_start"] == "2026-10-01" and d["absence"]["window_end"] == "2026-10-04"
    assert d["absence"]["coverage_basis"] == "voucher_redemption_date" and d["absence"]["attestation_ref"] == "OPS-4242"
    # Effects: ONLY the rejection documents and the parked referrals. No qualification, XP, reward or registry write
    # for them, and no history is touched.
    after = _snapshot(w.db)
    needles = ("ref:12", "ref:13", "'invitee_id': 12", "'invitee_id': 13")         # the two absence-rejected invitees
    for name in ("affiliate_ledger", "voucher_pools"):                                  # reward inventory untouched
        assert after.get(name, []) == before.get(name, []), name
    for name in ("xp_events", "referral_events", "referral_award_events", "qualified_events", aq.REGISTRY_COLLECTION):
        mentions = [d for d in after.get(name, []) if any(t in d for t in needles)]
        assert mentions == [], (name, mentions)
    assert {q["invitee_id"] for q in w.db.qualified_events.find()} == {11}
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({}) == 1


def test_live_absence_pass_is_idempotent_and_rechecks_only_when_coverage_changes(monkeypatch):
    w, feed = brief_world()
    activate(w)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"source_config.absence_rejection": policy(3)}})
    first = run(w, monkeypatch, now=NOW)["evidence"]["absence"]
    assert first["rejected"] == 2 and first["checked"] >= 5
    again = run(w, monkeypatch, now=NOW)["evidence"]["absence"]
    assert again == {"checked": 0, "rejected": 0, "pending": {}, "skipped": "unchanged"}   # fingerprints unchanged
    assert w.db[aq.EVIDENCE_COLLECTION].count_documents({"source": ucov.ABSENCE_SOURCE}) == 2
    # Later the data owner extends the final report to cover 14/15's windows; only then do they conclude.
    (batch_id,) = [b["batch_id"] for b in feed.batches]
    feed.set_coverage(batch_id, coverage_block("2026-09-28", "2026-10-09", revision=2))
    sync(w, feed, now=NOW + timedelta(minutes=2))
    later = run(w, monkeypatch, now=NOW + timedelta(minutes=2))["evidence"]["absence"]
    assert later["rejected"] == 2 and {pending_status(w, u) for u in (14, 15)} == {"rejected"}
    assert pending_status(w, 16) != "rejected" and pending_status(w, 17) != "rejected"


def test_absence_decision_for_an_invitee_that_later_gets_a_row_goes_to_review_never_to_a_reward(monkeypatch):
    w, feed = brief_world()
    activate(w)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"source_config.absence_rejection": policy(3)}})
    run(w, monkeypatch, now=NOW)
    assert pending_status(w, 12) == "rejected"
    # A corrected batch later shows a qualifying redemption for the already-rejected invitee.
    w.user(12, linked=["Late12"], created=kl(2026, 9, 28, 10))
    late = urow("CODE12", "Late12", "2026-09-30 09:00:00")
    late["after_total_bet_amount"] = "500"
    feed.add_batch([late], coverage=BRIEF)
    sync(w, feed, now=NOW + timedelta(minutes=1))
    run(w, monkeypatch, now=NOW + timedelta(minutes=1))
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one({"code_hash": aq.code_hash("CODE12"), "source": {"$ne": ucov.ABSENCE_SOURCE}})
    assert ev["status"] == aq.EV_REVIEW and ev["reason"] == "referral_not_active_at_rollout"
    assert w.db.qualified_events.count_documents({"invitee_id": 12}) == 0           # decided history is never reopened
    assert w.db[aq.REGISTRY_COLLECTION].count_documents({"account_key": "uim_global:Late12"}) == 0


# ── Immediate evaluation: no second seven-day wait ───────────────────────────

def test_a_completed_row_is_evaluated_at_once_whatever_its_age_or_the_upload_age(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    just_joined = NOW - timedelta(hours=2)
    row = invitee(w, feed, 1, 11, account="Fresh11", wall=(NOW - timedelta(hours=1)).astimezone(aq.KL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                  at=just_joined, fields={"after_total_bet_amount": "80.00"})
    # Committed a minute ago, redeemed an hour ago, joined two hours ago: far less than seven days on every clock.
    feed.add_batch([row], committed_at=NOW - timedelta(minutes=1),
                   coverage=coverage_block("2026-10-19", "2026-10-20"))
    sync(w, feed)
    run(w, monkeypatch, now=NOW)
    assert w.db.qualified_events.find_one({"invitee_id": 11})["rule_version"] == aq.RULE_VERSION
    assert w.db[aq.EVIDENCE_COLLECTION].find_one({"invitee_id": 11})["status"] == aq.EV_QUALIFIED


def test_latest_observed_redemption_earlier_than_declared_coverage_does_not_hold_a_row(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    rows = [invitee(w, feed, 1, 11, account="A11", wall="2026-10-02 09:00:00", fields={"after_total_bet_amount": "80"}),
            invitee(w, feed, 1, 12, account="A12", wall="2026-10-04 23:30:00",
                    fields={"after_total_bet_amount": "79.99"})]
    feed.add_batch(rows, coverage=BRIEF)
    sync(w, feed)
    run(w, monkeypatch)
    status = {e["invitee_id"]: (e["status"], e.get("reason")) for e in w.db[aq.EVIDENCE_COLLECTION].find()}
    assert status == {11: (aq.EV_QUALIFIED, None), 12: (aq.EV_REJECTED, "seven_day_turnover_below_80")}


@pytest.mark.parametrize("amount,flag,expected", [
    ("79.99", "1", ("rejected", "seven_day_turnover_below_80")), ("80.00", "1", ("qualified", None)),
    ("500", "0", ("rejected", "not_a_new_coupon_player")),
    ("80.0000000000000000000000001", "1", ("qualified", None)),
])
def test_row_verdicts_are_unchanged_by_coverage(monkeypatch, amount, flag, expected):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    row = invitee(w, feed, 1, 11, account="A11", wall="2026-10-03 09:00:00",
                  fields={"after_total_bet_amount": amount, "coupon_new_player": flag})
    feed.add_batch([row], coverage=coverage_block("2026-10-04", "2026-10-04"))    # row lies OUTSIDE the declared day
    sync(w, feed)
    run(w, monkeypatch)
    ev = w.db[aq.EVIDENCE_COLLECTION].find_one({"invitee_id": 11})
    assert (ev["status"], ev.get("reason")) == expected


def test_duplicate_account_exclusion_is_unchanged(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    rows = [invitee(w, feed, 1, 11, account="000Same", wall="2026-10-02 09:00:00"),
            invitee(w, feed, 2, 12, account="000Same", wall="2026-10-03 09:00:00")]
    feed.add_batch(rows, coverage=BRIEF)
    sync(w, feed)
    run(w, monkeypatch)
    assert {e["invitee_id"]: e["status"] for e in w.db[aq.EVIDENCE_COLLECTION].find()} == {
        11: aq.EV_QUALIFIED, 12: aq.EV_DUPLICATE_ACCOUNT}


# ── Monthly attribution of delayed uploads ───────────────────────────────────

def _month_world(*, redeemed_wall, processed_at, committed_at):
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"source_config": dict(DATABOT_CONFIG)}})
    activate(w, cutoff=utc(2026, 9, 1))
    feed = FakeFeed()
    row = invitee(w, feed, 1, 11, account="AcctM", wall=redeemed_wall, at=utc(2026, 9, 5),
                  fields={"after_total_bet_amount": "100"})
    feed.add_batch([row], committed_at=committed_at, coverage=coverage_block("2026-09-01", "2026-10-02"))
    sync(w, feed, now=processed_at)
    return w


@pytest.mark.parametrize("wall,month", [("2026-09-30 23:30:00", "202609"), ("2026-10-01 00:30:00", "202610"),
                                        ("2026-09-30 23:59:59", "202609"), ("2026-10-01 00:00:00", "202610")])
def test_delayed_upload_keeps_the_redemption_month_on_the_gmt8_calendar(monkeypatch, wall, month):
    # Same redemption, uploaded and processed on three different days: the month never moves.
    results = []
    for committed, processed in ((utc(2026, 10, 1, 2), utc(2026, 10, 1, 3)),
                                 (utc(2026, 10, 12, 4), utc(2026, 10, 12, 5)),
                                 (utc(2026, 10, 31, 20), utc(2026, 10, 31, 21))):
        w = _month_world(redeemed_wall=wall, processed_at=processed, committed_at=committed)
        run(w, monkeypatch, now=processed)
        q = w.db.qualified_events.find_one({"invitee_id": 11})
        results.append((aq.kl_month_key(q["qualified_at"]), q["attribution_basis"],
                        aq._aware_utc(q["qualified_at"]), aq._aware_utc(q["redeemed_at"])))
    assert {r[0] for r in results} == {month}                                     # processing/upload time is irrelevant
    assert {r[1] for r in results} == {"redeemed_at"}
    assert len({r[2] for r in results}) == 1 and results[0][2] == results[0][3]


def test_late_redemption_for_a_closed_month_is_reviewed_not_moved(monkeypatch):
    w = _month_world(redeemed_wall="2026-09-30 23:30:00", processed_at=utc(2026, 10, 12, 5), committed_at=utc(2026, 10, 12, 4))
    result = run(w, monkeypatch, now=utc(2026, 10, 12, 5))
    q = w.db.qualified_events.find_one({"invitee_id": 11})
    assert aq.kl_month_key(q["qualified_at"]) == "202609" and aq._aware_utc(q["processed_at"]) == utc(2026, 10, 12, 5)
    assert result["evidence"]["by_status"] == {aq.EV_QUALIFIED: 1}


# ── Privacy, read path, history ──────────────────────────────────────────────

def test_reporting_payload_has_no_raw_voucher_codes_account_ids_or_attestation_identity():
    w, _ = brief_world()
    body = states(w)
    text = json.dumps(body, default=str)
    for secret in ("CODE11", "CODE12", "Acct11", "Acct17", "UIM data owner", "OPS-4242"):
        assert secret not in text
    stored = json.dumps(list(w.db[rep.STATE_COLLECTION].find()), default=str)
    for secret in ("CODE11", "Acct11", "UIM data owner"):
        assert secret not in stored


def test_report_read_path_never_computes_coverage_or_replays(monkeypatch):
    w, _ = brief_world()
    live_snapshot(w)

    def boom(*_a, **_k):
        raise AssertionError("heavy work reached the HTTP read path")

    for name in ("effective_coverage", "decide_absences", "welcome_issuances", "process_absence_rejections"):
        monkeypatch.setattr(ucov, name, boom)
    monkeypatch.setattr(rep.aqp, "_replay", boom)
    body = report(w, **WIDE)
    assert body["uim_coverage"]["status"] == "complete" and totals(body) == [1, 4, 2, 0]


def test_preview_build_writes_nothing_but_its_own_snapshot_collections():
    w, _ = brief_world()
    before = {k: v for k, v in _snapshot(w.db).items() if k not in (rep.STATE_COLLECTION, rep.ROWS_COLLECTION)}
    live_snapshot(w)
    after = {k: v for k, v in _snapshot(w.db).items() if k not in (rep.STATE_COLLECTION, rep.ROWS_COLLECTION)}
    assert after == before


# ── Admin: policy command + preflight gates ──────────────────────────────────

def _cli(w, *argv, now=NOW):
    import io
    from contextlib import redirect_stdout, redirect_stderr

    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = admin.main(list(argv), db_factory=lambda: w.db, read_only_db_factory=lambda: w.db, now_fn=lambda: now)
    return code, out.getvalue(), err.getvalue()


POLICY_ARGS = ("configure-coverage-policy", "--basis", "voucher_redemption_date", "--welcome-redemption-window-days", "3",
               "--attested-by", "UIM data owner", "--attestation-ref", "OPS-4242", "--reason", "owner confirmed",
               "--operator", "yh")


def test_policy_command_is_dry_run_by_default_audited_and_idempotent():
    w, _ = brief_world(days=None)
    code, out, _ = _cli(w, *POLICY_ARGS)
    assert code == 0 and json.loads(out)["dry_run"] is True
    assert "absence_rejection" not in aq.get_control(w.db)["source_config"]
    code, out, _ = _cli(w, *POLICY_ARGS, "--commit")
    control = aq.get_control(w.db)
    assert code == 0 and control["source_config"]["absence_rejection"]["welcome_redemption_window_days"] == 3
    assert control["absence_policy_history"][0]["operator"] == "yh" and control["absence_policy_history"][0]["reason"] == "owner confirmed"
    code, out, _ = _cli(w, *POLICY_ARGS, "--commit")
    assert json.loads(out)["no_change"] is True and len(aq.get_control(w.db)["absence_policy_history"]) == 1
    assert totals(states(w)) == [1, 4, 2, 0]


@pytest.mark.parametrize("argv", [
    ("configure-coverage-policy", "--basis", "voucher_redemption_date", "--attested-by", "x", "--attestation-ref", "y",
     "--reason", "r"),                                                                  # no window
    ("configure-coverage-policy", "--basis", "voucher_redemption_date", "--welcome-redemption-window-days", "3",
     "--reason", "r"),                                                                  # no attestation
])
def test_policy_command_refuses_an_unattested_or_incomplete_policy(argv):
    w, _ = brief_world(days=None)
    code, _, err = _cli(w, *argv, "--commit")
    assert code == 2 and "invalid coverage policy" in err
    assert "absence_rejection" not in aq.get_control(w.db)["source_config"]


def test_policy_command_refuses_while_the_rule_is_active():
    w, _ = brief_world(days=None)
    activate(w)
    code, _, err = _cli(w, *POLICY_ARGS, "--commit")
    assert code == 2 and "pause" in err


def test_preflight_requires_complete_gap_free_coverage_and_an_attested_policy():
    w, _ = brief_world()
    section = admin.preflight_report(w.db, now_utc=NOW)["coverage"]
    assert section["ok"] is True and section["status"] == "complete" and section["gaps_affecting_cohort"] == []
    assert section["absence_policy"]["configured"] is True and section["absence_policy"]["welcome_redemption_window_days"] == 3
    no_policy, _ = brief_world(days=None)
    assert "absence_policy_not_configured" in admin.preflight_report(no_policy.db, now_utc=NOW)["coverage"]["blockers"]
    unverified, _ = brief_world(coverage=coverage_block("2026-09-28", "2026-10-02", status="inferred_unverified",
                                                        source="observed_rows", basis="unverified"))
    pre = admin.preflight_report(unverified.db, now_utc=NOW)
    assert pre["coverage"]["blockers"][0] == "coverage_inferred_unverified" and pre["ok"] is False


def test_preflight_blocks_when_a_gap_sits_inside_the_launch_cohort():
    w = disabled_world()
    set_policy(w, policy(2))
    feed = FakeFeed()
    w.join(1, 11, at=kl(2026, 9, 15, 9))
    feed.add_batch([], coverage=coverage_block("2026-09-14", "2026-09-20"))
    feed.add_batch([], coverage=coverage_block("2026-09-28", "2026-10-04"))
    sync(w, feed)
    section = admin.preflight_report(w.db, now_utc=NOW)["coverage"]
    assert "coverage_gaps_affect_launch_cohort" in section["blockers"]
    assert section["gaps_affecting_cohort"] == [{"start_date": "2026-09-21", "end_date": "2026-09-27"}]


def test_marketing_source_has_no_uim_coverage_contract():
    w = World()
    assert admin.preflight_report(w.db, now_utc=NOW)["coverage"] == {"applicable": False, "ok": True, "blockers": []}


def test_an_older_databot_without_the_coverage_endpoint_syncs_rows_but_draws_no_absence_conclusion():
    w = disabled_world()
    set_policy(w, policy(3))
    feed = FakeFeed()
    feed.coverage_unavailable = True
    invitee(w, feed, 1, 12, at=kl(2026, 9, 28, 10))                                # joined + Welcome code, no row
    feed.add_batch([], coverage=BRIEF)
    result = sync(w, feed)
    assert result["ok"] and "feed_http_404" in result["coverage_ledger_error"]
    cov = ucov.effective_coverage(w.db)
    assert cov["status"] == "complete" and "coverage_ledger_missing" in ucov.absence_blockers(aq.get_control(w.db), cov)
    verdict = ucov.decide_absences(w.db, [12], control=aq.get_control(w.db), cov=cov, source_ok=True)
    assert verdict[12]["status"] == "pending" and verdict[12]["reason"] == "coverage_ledger_missing"


def test_a_large_changed_population_is_worked_off_in_bounded_slices(monkeypatch):
    w, _ = brief_world()
    activate(w)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"source_config.absence_rejection": policy(3)}})
    control = aq.get_control(w.db)
    # Absence means "no evidence at all": refused until every committed row has been extracted into evidence.
    assert ucov.process_absence_rejections(w.db, control=control, now_utc=NOW)["skipped"] == "source_extraction_incomplete"
    assert w.db[aq.EVIDENCE_COLLECTION].count_documents({"source": ucov.ABSENCE_SOURCE}) == 0
    aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    seen, rejected = 0, 0
    for tick in range(10):
        out = ucov.process_absence_rejections(w.db, control=control, now_utc=NOW + timedelta(seconds=tick), limit=2)
        seen += out["checked"]
        rejected += out["rejected"]
        if not out.get("more"):
            break
    assert rejected == 2 and out.get("more") is False
    assert ucov.process_absence_rejections(w.db, control=control, now_utc=NOW + timedelta(minutes=5), limit=2)["skipped"] == "unchanged"
    # The periodic safety re-scan still happens (e.g. referrals re-opened since).
    again = ucov.process_absence_rejections(w.db, control=control, now_utc=NOW + ucov.ABSENCE_RECHECK + timedelta(minutes=1), limit=2)
    assert "skipped" not in again
