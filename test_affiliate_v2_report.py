"""Affiliate V2 reporting snapshot for Databot (read-only return path).

Every V2 figure comes from the REAL pipeline: Databot-shaped UIM rows are
synced, the live validator decides, and only then is the snapshot built - the
report must reproduce the persisted decisions, never re-derive a gate.
Synthetic mongomock databases only; nothing external is touched.
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from flask import Flask

import affiliate_qualification as aq
import affiliate_qualification_preview as aqp
import affiliate_v2_report as rep
import affiliate_v2_report_api as api
from test_affiliate_welcome_redemption_qualification import (
    CUTOFF, NOW, World, _ReadOnlyDb, _scheduler, _snapshot, run,
)
from test_uim_redemption_sync import DATABOT_CONFIG, FakeFeed, disabled_world, sync, urow

START, END = "2026-10-01", "2026-10-31"
OWN = (rep.STATE_COLLECTION, rep.ROWS_COLLECTION)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in ("AFFILIATE_V2_REPORT_ENABLED", "AFFILIATE_V2_REPORT_TOKEN", "DATABOT_API_KEY",
                 "AFFILIATE_V2_REPORT_ALLOW_PUBLIC", "AFFILIATE_V2_REPORT_STALE_AFTER_SECONDS"):
        monkeypatch.delenv(name, raising=False)


def activate(w, *, mode=aq.MODE_ACTIVE, cutoff=CUTOFF):
    set_fields = {"source_config": dict(DATABOT_CONFIG), "mode": mode, "seed_completed_at": CUTOFF,
                  "seeded_identity_config": aq.identity_config(DATABOT_CONFIG)}
    update = {"$set": set_fields}
    if cutoff is None:
        update["$unset"] = {"launch_cutoff_utc": ""}
    else:
        set_fields["launch_cutoff_utc"] = cutoff
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, update)


def invitee(w, feed, inviter, uid, *, code=None, account=None, fields=None, wall="2026-10-05 10:00:00",
            at=CUTOFF + timedelta(days=1)):
    """Join + Welcome code (+ one UIM row when ``account`` is given)."""
    code = code or f"CODE{uid}"
    w.join(inviter, uid, at=at)
    w.welcome_code(uid, code, issued_at=at)
    if account:
        w.user(uid, linked=[account], created=at)
        row = urow(code, account, wall)
        row.update(fields or {})
        return row
    return None


def scenario(monkeypatch):
    """Affiliate 1: 11 qualified (80.00), 12 rejected (79.99), 13 rejected
    (not a new player), 14 pending (incomplete 7d), 15 pending (no redemption),
    16 duplicate of 11's account, 31 legacy-qualified. Affiliate 2: 21
    qualified (123.456)."""
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    rows = [
        invitee(w, feed, 1, 11, account="000AcctEleven", fields={"after_total_bet_amount": "80.00"}),
        invitee(w, feed, 1, 12, account="AcctTwelve", fields={"after_total_bet_amount": "79.99"}),
        invitee(w, feed, 1, 13, account="AcctThirteen", fields={"coupon_new_player": "0", "after_total_bet_amount": "500"}),
        invitee(w, feed, 1, 14, account="AcctFourteen", fields={"seven_day_complete": False, "after_total_bet_amount": "10"}),
        invitee(w, feed, 1, 15),
        invitee(w, feed, 1, 16, account="000AcctEleven", wall="2026-10-06 10:00:00",
                fields={"after_total_bet_amount": "900"}),
        invitee(w, feed, 2, 21, account="AcctTwentyOne", fields={"after_total_bet_amount": "123.456"}),
    ]
    # Legacy (v1-era) qualification: historical, never part of a V2 column.
    w.join(1, 31, at=CUTOFF - timedelta(days=30), status="awarded")
    w.db.qualified_events.insert_one({"invitee_id": 31, "referrer_id": 1, "qualified_at": CUTOFF - timedelta(days=20),
                                      "rule_version": aq.LEGACY_RULE_VERSION})
    feed.add_batch([r for r in rows if r])
    sync(w, feed)
    run(w, monkeypatch)
    return w, feed


def snapshot(w, now=NOW):
    out = rep.refresh_snapshot(w.db, now_utc=now)
    assert out["ok"], out
    return out


def report(w, *, start=START, end=END, now=NOW, **kw):
    return rep.build_report(w.db, start_date=start, end_date=end, now_utc=now, **kw)


def by_affiliate(body):
    return {a["affiliate_id"]: a for a in body["affiliates"]}


# ---------------------------------------------------------------------------
# Metric definitions, taken from the real pipeline
# ---------------------------------------------------------------------------

def test_every_v2_state_is_counted_exactly_once(monkeypatch):
    w, _ = scenario(monkeypatch)
    statuses = sorted((e.get("invitee_id") or e["recipient_resolution"]["user_id"], e["status"])
                      for e in w.db[aq.EVIDENCE_COLLECTION].find())
    assert dict(statuses) == {11: aq.EV_QUALIFIED, 12: aq.EV_REJECTED, 13: aq.EV_REJECTED, 14: aq.EV_REVIEW,
                              16: aq.EV_DUPLICATE_ACCOUNT, 21: aq.EV_QUALIFIED}
    snapshot(w)
    body = report(w)
    a = by_affiliate(body)
    assert a["1"]["v2_qualified"] == 1            # 11 only: legacy 31 excluded
    assert a["1"]["v2_rejected"] == 2             # 12 (79.99), 13 (coupon_new_player=0)
    assert a["1"]["v2_pending"] == 2              # 14 incomplete seven-day data, 15 no redemption yet
    assert a["1"]["duplicate_account_excluded"] == 1
    assert (a["2"]["v2_qualified"], a["2"]["v2_pending"], a["2"]["v2_rejected"]) == (1, 0, 0)
    t = body["totals"]
    # Mutually exclusive: 7 distinct V2 invitees (31 is legacy) occupy exactly 7 slots.
    assert t["v2_qualified"] + t["v2_pending"] + t["v2_rejected"] + t["duplicate_account_excluded"] == 7
    assert w.db.pending_referrals.distinct("invitee_user_id").__len__() == 8
    assert body["status"] == "ok" and body["source"]["status"] == rep.SOURCE_OK


def test_legacy_qualification_is_not_a_v2_qualification(monkeypatch):
    w, _ = scenario(monkeypatch)
    assert w.db.qualified_events.count_documents({}) == 3  # legacy 31 + v2 11, 21
    snapshot(w)
    assert report(w)["totals"]["v2_qualified"] == 2
    # And the legacy row is untouched.
    assert w.db.qualified_events.find_one({"invitee_id": 31})["rule_version"] == aq.LEGACY_RULE_VERSION


@pytest.mark.parametrize("amount,state", [("79.99", "v2_rejected"), ("80", "v2_qualified"), ("80.00", "v2_qualified")])
def test_threshold_boundary_is_reported_as_upstream_decided(monkeypatch, amount, state):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    feed.add_batch([invitee(w, feed, 1, 11, account="AcctX", fields={"after_total_bet_amount": amount})])
    sync(w, feed)
    run(w, monkeypatch)
    snapshot(w)
    row = by_affiliate(report(w))["1"]
    assert row[state] == 1 and sum(row[k] for k in ("v2_qualified", "v2_pending", "v2_rejected",
                                                    "duplicate_account_excluded")) == 1


def test_missing_or_incomplete_evidence_stays_pending_never_rejected(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    feed.add_batch([
        invitee(w, feed, 1, 11, account="A11", fields={"seven_day_complete": False}),
        invitee(w, feed, 1, 12, account="A12", fields={"after_total_bet_amount": None}),
        invitee(w, feed, 1, 13, account="A13", fields={"coupon_new_player": None}),
    ])
    invitee(w, feed, 1, 14)  # joined, voucher issued, no UIM row at all
    sync(w, feed)
    run(w, monkeypatch)
    snapshot(w)
    row = by_affiliate(report(w))["1"]
    assert (row["v2_pending"], row["v2_rejected"], row["v2_qualified"]) == (4, 0, 0)


def test_temporary_feed_failure_keeps_invitees_pending(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    feed.add_batch([invitee(w, feed, 1, 11, account="A11", fields={"after_total_bet_amount": "80"})])
    feed.fail_after = 0  # every call fails: nothing synced, last attempt failed
    out = sync(w, feed)
    assert out.get("error")
    run(w, monkeypatch)
    snapshot(w)
    body = report(w)
    row = by_affiliate(body)["1"]
    assert (row["v2_pending"], row["v2_rejected"], row["v2_qualified"]) == (1, 0, 0)
    assert body["source"]["status"] == rep.SOURCE_UNAVAILABLE
    assert row["seven_day_turnover"] is None and row["seven_day_turnover_status"] == rep.TURNOVER_UNAVAILABLE


def test_duplicate_account_does_not_touch_the_original_owner(monkeypatch):
    w, _ = scenario(monkeypatch)
    before = w.db.qualified_events.find_one({"invitee_id": 11})
    snapshot(w)
    assert by_affiliate(report(w))["1"]["duplicate_account_excluded"] == 1
    assert w.db.qualified_events.find_one({"invitee_id": 11}) == before
    assert w.db.qualified_events.count_documents({"invitee_id": 16}) == 0


# ---------------------------------------------------------------------------
# Turnover
# ---------------------------------------------------------------------------

def test_turnover_is_exact_decimal_over_actual_v2_qualified_only(monkeypatch):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    a = by_affiliate(report(w))
    assert a["1"]["seven_day_turnover"] == "80.00"      # 79.99, 500, 10 and 900 are NOT counted
    assert a["2"]["seven_day_turnover"] == "123.456"    # exact; display rounding is Databot's job
    assert report(w)["totals"]["seven_day_turnover"] == "203.456"
    assert a["1"]["seven_day_turnover_status"] == rep.TURNOVER_COMPLETE


def test_turnover_sum_never_uses_floats(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    amounts = ["80.10", "80.20", "80.0000000000000000000000001"]
    feed.add_batch([invitee(w, feed, 1, 10 + i, account=f"AA{i}", fields={"after_total_bet_amount": amount})
                    for i, amount in enumerate(amounts)])
    sync(w, feed)
    run(w, monkeypatch)
    snapshot(w)
    assert by_affiliate(report(w))["1"]["seven_day_turnover"] == str(
        Decimal("80.10") + Decimal("80.20") + Decimal("80.0000000000000000000000001"))
    assert 0.1 + 0.2 != 0.3  # the float trap this guards against


def test_repeated_uim_rows_and_batches_do_not_multiply_turnover(monkeypatch):
    w = disabled_world()
    activate(w)
    feed = FakeFeed()
    row = invitee(w, feed, 1, 11, account="000AcctEleven", fields={"after_total_bet_amount": "91.50"})
    feed.add_batch([row, dict(row)])       # same observation twice in one batch
    feed.add_batch([dict(row)])            # and re-imported in a later batch
    sync(w, feed)
    sync(w, feed)                          # re-sync is a no-op
    run(w, monkeypatch)
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 1
    snapshot(w)
    snapshot(w, now=NOW + timedelta(minutes=11))
    body = report(w, now=NOW + timedelta(minutes=11))
    assert by_affiliate(body)["1"]["seven_day_turnover"] == "91.50"
    assert by_affiliate(body)["1"]["v2_qualified"] == 1


def test_unresolvable_turnover_is_partial_not_a_complete_zero(monkeypatch):
    w, _ = scenario(monkeypatch)
    w.db[aq.EVIDENCE_COLLECTION].update_many({"status": aq.EV_QUALIFIED},
                                             {"$unset": {"qualification_fields.after_total_bet_amount": ""}})
    snapshot(w)
    row = by_affiliate(report(w))["1"]
    assert row["v2_qualified"] == 1 and row["seven_day_turnover_status"] == rep.TURNOVER_PARTIAL


# ---------------------------------------------------------------------------
# Cohort / month / GMT+8
# ---------------------------------------------------------------------------

def test_cohort_is_referral_join_day_in_gmt8(monkeypatch):
    w = disabled_world()
    activate(w, cutoff=datetime(2026, 9, 1, tzinfo=timezone.utc))
    feed = FakeFeed()
    # 23:59:59 GMT+8 on 30 Sep vs 00:00:00 GMT+8 on 1 Oct.
    last_sep = datetime(2026, 9, 30, 15, 59, 59, tzinfo=timezone.utc)
    first_oct = datetime(2026, 9, 30, 16, 0, 0, tzinfo=timezone.utc)
    invitee(w, feed, 1, 11, at=last_sep)
    invitee(w, feed, 1, 12, at=first_oct)
    invitee(w, feed, 1, 13, at=datetime(2026, 10, 31, 15, 59, 59, tzinfo=timezone.utc))   # 31 Oct 23:59:59
    invitee(w, feed, 1, 14, at=datetime(2026, 10, 31, 16, 0, 0, tzinfo=timezone.utc))     # 1 Nov 00:00:00
    snapshot(w, now=datetime(2026, 11, 2, tzinfo=timezone.utc))
    now = datetime(2026, 11, 2, tzinfo=timezone.utc)
    sep = report(w, start="2026-09-01", end="2026-09-30", now=now)
    octo = report(w, start="2026-10-01", end="2026-10-31", now=now)
    nov = report(w, start="2026-11-01", end="2026-11-30", now=now)
    assert [sep["totals"]["v2_pending"], octo["totals"]["v2_pending"], nov["totals"]["v2_pending"]] == [1, 2, 1]
    week = report(w, start="2026-10-01", end="2026-10-07", now=now)   # a week/range uses the same days
    assert week["totals"]["v2_pending"] == 1
    assert octo["cohort"] == {"basis": "referral_joined_at", "timezone": "Asia/Kuala_Lumpur (GMT+8)",
                              "start_date": "2026-10-01", "end_date": "2026-10-31"}


def test_pending_rows_are_bucketed_by_join_not_a_missing_qualification_time(monkeypatch):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    # Everyone joined 2 Oct in the scenario; a window not containing it is empty, not zero-filled.
    other = report(w, start="2026-11-01", end="2026-11-30")
    assert other["affiliates"] == [] and other["totals"]["v2_pending"] == 0


@pytest.mark.parametrize("start,end", [("2026-10-31", "2026-10-01"), ("bad", "2026-10-01"), ("2026-10-01", None),
                                        ("2020-01-01", "2026-10-01")])
def test_bad_windows_are_refused(start, end):
    with pytest.raises(rep.WindowError):
        rep.parse_window(start, end)


# ---------------------------------------------------------------------------
# Disabled / paused / not launched
# ---------------------------------------------------------------------------

def test_explicitly_disabled_after_a_passed_cutoff_reports_nothing_not_zeros():
    w = _world_with(mode=aq.MODE_DISABLED, cutoff=CUTOFF)
    w.join(1, 11)
    out = rep.refresh_snapshot(w.db, now_utc=NOW)
    assert out["ok"] and out["report_mode"] == rep.MODE_INACTIVE and out["launched"] is False
    body = report(w)
    assert body["report_mode"] == "inactive" and body["rule_active"] is False and body["status"] == "inactive"
    assert body["totals"] is None and body["affiliates"] is None and body["snapshot"] is None
    assert body["effects_written"] is False
    assert w.db[rep.ROWS_COLLECTION].count_documents({}) == 0


def _world_with(*, mode, cutoff):
    w = disabled_world()
    activate(w, mode=mode, cutoff=cutoff)
    return w


def test_paused_after_launch_still_reports_recorded_decisions(monkeypatch):
    w, _ = scenario(monkeypatch)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"mode": aq.MODE_PAUSED}})
    snapshot(w)
    body = report(w)
    assert body["rule_active"] is False and body["rule_mode"] == aq.MODE_PAUSED and body["report_mode"] == "live"
    assert body["totals"]["v2_qualified"] == 2


def test_activation_without_a_snapshot_is_no_snapshot_not_zero(monkeypatch):
    w, _ = scenario(monkeypatch)
    body = report(w)
    assert body["status"] == "no_snapshot" and body["totals"] is None and body["affiliates"] is None


def test_snapshot_taken_before_activation_is_never_served_as_data(monkeypatch):
    w = disabled_world()
    rep.refresh_snapshot(w.db, now_utc=NOW)
    activate(w)
    body = report(w)
    assert body["status"] == "no_snapshot" and body["totals"] is None


# ---------------------------------------------------------------------------
# Snapshot lifecycle: replace, idempotent, last-known-good, freshness
# ---------------------------------------------------------------------------

def test_refresh_replaces_state_instead_of_accumulating(monkeypatch):
    w, feed = scenario(monkeypatch)
    first = snapshot(w)
    again = snapshot(w, now=NOW + timedelta(minutes=11))
    assert first["generation"] != again["generation"]
    assert report(w, now=NOW + timedelta(minutes=11))["totals"]["v2_pending"] == 2
    assert w.db[rep.ROWS_COLLECTION].distinct("generation") == [again["generation"]]

    # Invitee 15 now redeems: pending -> qualified, exactly one terminal column.
    row = urow("CODE15", "AcctFifteen", "2026-10-07 09:00:00")
    row["after_total_bet_amount"] = "200"
    w.user(15, linked=["AcctFifteen"], created=CUTOFF + timedelta(days=1))
    feed.add_batch([row])
    later = NOW + timedelta(minutes=30)
    sync(w, feed, now=later)
    run(w, monkeypatch, now=later)
    snapshot(w, now=later)
    a = by_affiliate(report(w, now=later))["1"]
    assert (a["v2_pending"], a["v2_qualified"]) == (1, 2)
    assert a["seven_day_turnover"] == "280.00"
    # Old generations are gone: the table holds exactly the current snapshot.
    assert w.db[rep.ROWS_COLLECTION].distinct("generation") == [snapshot_generation(w)]
    assert by_affiliate(report(w, now=later))["2"]["v2_qualified"] == 1  # untouched affiliate not re-added


def snapshot_generation(w):
    return w.db[rep.STATE_COLLECTION].find_one({"_id": rep.STATE_ID})["generation"]


def test_failed_refresh_keeps_the_last_good_snapshot(monkeypatch):
    w, _ = scenario(monkeypatch)
    good = snapshot(w)
    monkeypatch.setattr(rep, "build_rows", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    out = rep.refresh_snapshot(w.db, now_utc=NOW + timedelta(minutes=20))
    assert out["ok"] is False and out["error"] == "RuntimeError"
    body = report(w, now=NOW + timedelta(minutes=20))
    assert body["snapshot"]["generation"] == good["generation"] and body["totals"]["v2_qualified"] == 2
    state = w.db[rep.STATE_COLLECTION].find_one({"_id": rep.STATE_ID})
    assert state["last_error"] == "RuntimeError" and state["lease_until"] is None


def test_stale_snapshot_is_flagged_and_turnover_is_partial(monkeypatch):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    fresh = report(w)
    assert fresh["snapshot"]["stale"] is False
    old = report(w, now=NOW + timedelta(hours=2))
    assert old["status"] == "stale" and old["snapshot"]["stale"] and old["snapshot"]["age_seconds"] == 7200
    assert by_affiliate(old)["1"]["seven_day_turnover_status"] == rep.TURNOVER_PARTIAL
    assert old["totals"]["v2_qualified"] == 2  # recorded decisions still shown, flagged


def test_partial_uim_sync_is_reported_not_hidden(monkeypatch):
    w, feed = scenario(monkeypatch)
    w.db[feed_batches()].update_one({}, {"$set": {"rows_complete": False}})
    snapshot(w)
    body = report(w)
    assert body["source"]["status"] == rep.SOURCE_PARTIAL and body["status"] == rep.SOURCE_PARTIAL
    assert "uim_batches_partially_synced" in body["source"]["problems"]
    assert by_affiliate(body)["1"]["seven_day_turnover_status"] == rep.TURNOVER_PARTIAL
    # Nothing was declared complete upstream: the latest row is reported as such and is NOT presented as coverage.
    assert body["uim_coverage_end"] is None
    cov = body["uim_coverage"]
    assert cov["status"] == "inferred_unverified" and cov["final_coverage_end"] is None
    assert cov["latest_observed_redemption_at"].endswith("+08:00")


def feed_batches():
    return aq.SOURCE_SPECS[aq.SOURCE_DATABOT_UIM]["batches"]


def test_lease_prevents_concurrent_refresh(monkeypatch):
    w, _ = scenario(monkeypatch)
    owner = rep._acquire(w.db, NOW)
    assert owner
    assert rep.refresh_snapshot(w.db, now_utc=NOW)["skipped"] == "lease_held"
    assert w.db[rep.ROWS_COLLECTION].count_documents({}) == 0


def test_reader_never_reports_zeros_when_a_refresh_swaps_generations(monkeypatch):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    fired = {"n": 0}

    class _SwapRows:
        """Row reads see a refresh land between the pointer read and the row
        read: the old generation is deleted, so the first read comes back empty."""

        def __init__(self, col):
            self._col = col

        def find(self, *a, **k):
            if not fired["n"]:
                fired["n"] += 1
                rep.refresh_snapshot(w.db, now_utc=NOW + timedelta(minutes=11))
                return iter(list(self._col.find(*a, **k)))   # old generation: now empty
            return self._col.find(*a, **k)

    class _SwapDb:
        def __getitem__(self, name):
            return _SwapRows(w.db[name]) if name == rep.ROWS_COLLECTION else w.db[name]

        def __getattr__(self, name):
            return getattr(w.db, name)

    body = rep.build_report(_SwapDb(), start_date=START, end_date=END, now_utc=NOW + timedelta(minutes=11))
    assert fired["n"] == 1
    assert body["totals"]["v2_qualified"] == 2 and body["totals"]["v2_pending"] == 2


# ---------------------------------------------------------------------------
# Safety: scope of writes, privacy, bounded read path
# ---------------------------------------------------------------------------

def test_refresh_writes_only_its_own_collections(monkeypatch):
    w, _ = scenario(monkeypatch)
    before = {k: v for k, v in _snapshot(w.db).items() if k not in OWN}
    snapshot(w)
    after = {k: v for k, v in _snapshot(w.db).items() if k not in OWN}
    assert after == before


def test_no_raw_codes_accounts_or_secrets_in_any_published_data(monkeypatch):
    w, _ = scenario(monkeypatch)
    monkeypatch.setenv("AFFILIATE_V2_REPORT_TOKEN", "s3cret-token")
    snapshot(w)
    published = json.dumps([report(w), list(w.db[rep.ROWS_COLLECTION].find({}, {"_id": 0})),
                            w.db[rep.STATE_COLLECTION].find_one({}, {"_id": 0})], default=str)
    for secret in ("CODE11", "CODE12", "000AcctEleven", "AcctTwelve", "AcctThirteen", "AcctTwentyOne",
                   "advantplay", "uim_global:", "s3cret-token"):
        assert secret not in published
    assert set(report(w)["affiliates"][0]) == {
        "affiliate_id", "v2_qualified", "v2_pending", "v2_rejected", "duplicate_account_excluded",
        "seven_day_turnover", "seven_day_turnover_status"}


def test_read_path_is_bounded_never_runs_the_preview_or_a_rebuild(monkeypatch):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    for name in ("build_preview", "_load_observations", "_simulate"):
        monkeypatch.setattr(aqp, name, lambda *a, **k: pytest.fail("preview executed on the read path"))
    monkeypatch.setattr(rep, "build_rows", lambda *a, **k: pytest.fail("snapshot rebuilt on the read path"))
    before = _snapshot(w.db)
    body = rep.build_report(_ReadOnlyDb(w.db), start_date=START, end_date=END, now_utc=NOW)
    assert body["totals"]["v2_qualified"] == 2
    assert _snapshot(w.db) == before


def test_response_is_capped_with_truncation_flag(monkeypatch):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    body = report(w, limit=1)
    assert len(body["affiliates"]) == 1 and body["affiliates_total"] == 2 and body["truncated"] is True
    assert body["totals"]["v2_qualified"] == 2  # totals cover every affiliate, not just the page


def test_existing_historical_records_are_not_modified(monkeypatch):
    w, _ = scenario(monkeypatch)
    before = {name: _snapshot(w.db)[name] for name in
              ("qualified_events", "referral_events", "affiliate_ledger", "xp_events", aq.REGISTRY_COLLECTION,
               aq.EVIDENCE_COLLECTION, "pending_referrals")
              if name in _snapshot(w.db)}
    snapshot(w)
    report(w)
    for name, rows in before.items():
        assert _snapshot(w.db)[name] == rows


# ---------------------------------------------------------------------------
# Scheduler hook
# ---------------------------------------------------------------------------

def test_scheduler_is_a_noop_unless_enabled_and_respects_freshness(monkeypatch):
    w, _ = scenario(monkeypatch)
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW) == {"skipped": "report_disabled"}
    assert w.db[rep.STATE_COLLECTION].count_documents({}) == 0
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW)["ok"]
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW + timedelta(seconds=5))["skipped"] == "recent_attempt"
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW + timedelta(minutes=5))["skipped"] == "fresh"
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW + timedelta(minutes=11))["ok"]
    # A rule-state change (pause) refreshes immediately, not after the interval.
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"mode": aq.MODE_PAUSED}})
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW + timedelta(minutes=13))["ok"]


# ---------------------------------------------------------------------------
# HTTP route
# ---------------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch):
    def make(w):
        app = Flask(__name__)
        app.register_blueprint(api.make_blueprint(lambda: w.db))
        return app.test_client()
    return make


URL = f"{api.PREFIX}/summary?start_date={START}&end_date={END}"


def test_route_fails_closed_without_enable_flag_or_secret(monkeypatch, client):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    c = client(w)
    monkeypatch.setenv("DATABOT_API_KEY", "k")
    assert c.get(URL, headers={"Authorization": "Bearer k"}).status_code == 404      # flag off
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    monkeypatch.delenv("DATABOT_API_KEY")
    assert c.get(URL, headers={"Authorization": "Bearer k"}).status_code == 404      # no secret configured


def test_route_authentication_and_private_network_only(monkeypatch, client):
    w, _ = scenario(monkeypatch)
    snapshot(w)
    c = client(w)
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    monkeypatch.setenv("DATABOT_API_KEY", "shared-key")
    assert c.get(URL).status_code == 401
    assert c.get(URL, headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert c.get(URL, headers={"Authorization": "shared-key"}).status_code == 401
    edge = c.get(URL, headers={"Authorization": "Bearer shared-key", "Fly-Client-IP": "1.2.3.4"})
    assert edge.status_code == 403
    ok = c.get(URL, headers={"Authorization": "Bearer shared-key"})
    assert ok.status_code == 200 and ok.headers["Cache-Control"] == "no-store"
    assert ok.get_json()["contract"] == rep.CONTRACT and ok.get_json()["totals"]["v2_qualified"] == 2
    # A dedicated token overrides the shared key; the shared key then stops working.
    monkeypatch.setenv("AFFILIATE_V2_REPORT_TOKEN", "dedicated")
    assert c.get(URL, headers={"Authorization": "Bearer shared-key"}).status_code == 401
    assert c.get(URL, headers={"Authorization": "Bearer dedicated"}).status_code == 200
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ALLOW_PUBLIC", "true")
    assert c.get(URL, headers={"Authorization": "Bearer dedicated", "Fly-Client-IP": "1.2.3.4"}).status_code == 200


def test_route_rejects_bad_input_and_reports_preview_without_snapshot(monkeypatch, client):
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    monkeypatch.setenv("DATABOT_API_KEY", "k")
    headers = {"Authorization": "Bearer k"}
    w = disabled_world()
    c = client(w)
    assert c.get(f"{api.PREFIX}/summary?start_date=x&end_date=y", headers=headers).status_code == 400
    assert c.get(URL + "&limit=abc", headers=headers).status_code == 400
    body = c.get(URL, headers=headers).get_json()
    # Never activated: preview mode, no snapshot built yet -> no numbers, and the request wrote nothing.
    assert body["report_mode"] == "preview" and body["rule_active"] is False and body["effects_written"] is False
    assert body["status"] == "no_snapshot" and body["totals"] is None
    assert w.db[rep.ROWS_COLLECTION].count_documents({}) == 0 and w.db[rep.STATE_COLLECTION].count_documents({}) == 0


def test_route_hides_internal_errors(monkeypatch, client):
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    monkeypatch.setenv("DATABOT_API_KEY", "k")
    w = disabled_world()
    monkeypatch.setattr(rep, "build_report", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("mongo://secret")))
    res = client(w).get(URL, headers={"Authorization": "Bearer k"})
    assert res.status_code == 503 and "secret" not in res.get_data(as_text=True)
