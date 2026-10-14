"""Pre-launch preview of welcome_redemption_v2 for Databot (report_mode=preview).

While V2 is disabled the leased snapshot job replays committed evidence through
the production validator and reports what activation WOULD do. These tests pin:
preview/live parity of the evaluator, one state per invitee, no writes of any
kind outside the derived snapshot, no preview inside a request, preview never
mixed with actual results, and the audit snapshot kept at activation.
"""
import json
from datetime import timedelta
from decimal import Decimal

import pytest

import affiliate_qualification as aq
import affiliate_qualification_preview as aqp
import affiliate_v2_report as rep
from test_affiliate_v2_report import (
    CUTOFF, NOW, OWN, START, END, FakeFeed, _ReadOnlyDb, _scheduler, _snapshot, activate, by_affiliate, disabled_world,
    invitee, report, run, sync, urow,
)

PREVIEW = rep.FIELDS[rep.MODE_PREVIEW]
LIVE = rep.FIELDS[rep.MODE_LIVE]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in ("AFFILIATE_V2_REPORT_ENABLED", "AFFILIATE_V2_REPORT_STALE_AFTER_SECONDS",
                 "AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING"):
        monkeypatch.delenv(name, raising=False)


def configure_disabled(w):
    """Source configured, V2 NOT activated (no cutoff, mode disabled)."""
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {
        "source_config": dict(__import__("test_uim_redemption_sync").DATABOT_CONFIG), "mode": aq.MODE_DISABLED}})


def preview_scenario():
    """Same population as test_affiliate_v2_report.scenario, V2 left disabled."""
    w = disabled_world()
    configure_disabled(w)
    feed = FakeFeed()
    rows = [
        invitee(w, feed, 1, 11, account="000AcctEleven", fields={"after_total_bet_amount": "80.00"}),
        invitee(w, feed, 1, 12, account="AcctTwelve", fields={"after_total_bet_amount": "79.99"}),
        invitee(w, feed, 1, 13, account="AcctThirteen", fields={"coupon_new_player": "0", "after_total_bet_amount": "500"}),
        invitee(w, feed, 1, 14, account="AcctFourteen", fields={"seven_day_complete": False, "after_total_bet_amount": "10"}),
        invitee(w, feed, 1, 15),
        invitee(w, feed, 1, 16, account="000AcctEleven", wall="2026-10-06 10:00:00", fields={"after_total_bet_amount": "900"}),
        invitee(w, feed, 2, 21, account="AcctTwentyOne", fields={"after_total_bet_amount": "123.456"}),
    ]
    w.join(1, 31, at=CUTOFF - timedelta(days=30), status="awarded")
    w.db.qualified_events.insert_one({"invitee_id": 31, "referrer_id": 1, "qualified_at": CUTOFF - timedelta(days=20),
                                      "rule_version": aq.LEGACY_RULE_VERSION})
    feed.add_batch([r for r in rows if r])
    sync(w, feed)
    return w, feed


def snap(w, now=NOW):
    out = rep.refresh_snapshot(w.db, now_utc=now)
    assert out["ok"], out
    return out


def counts(row, names):
    return [row[names[k]] for k in ("qualified", "pending", "rejected", "duplicate")]


# --- shape and meaning -------------------------------------------------------------

def test_preview_reports_would_columns_and_labels_itself_inactive_without_effects():
    w, _ = preview_scenario()
    snap(w)
    body = report(w)
    assert (body["report_mode"], body["rule_active"], body["effects_written"]) == ("preview", False, False)
    assert body["status"] == "ok" and body["rule_mode"] == aq.MODE_DISABLED
    a = by_affiliate(body)
    assert counts(a["1"], PREVIEW) == [1, 2, 2, 1]        # 11 | 14,15 | 12,13 | 16 ; legacy 31 excluded
    assert counts(a["2"], PREVIEW) == [1, 0, 0, 0]
    assert a["1"][PREVIEW["turnover"]] == "80.00" and a["2"][PREVIEW["turnover"]] == "123.456"
    assert body["totals"][PREVIEW["turnover"]] == "203.456" and body["totals"][PREVIEW["tstatus"]] == "complete"
    # Never the live vocabulary: a preview cannot be read as an actual V2 result.
    assert not any(k.startswith("v2_") or k == "duplicate_account_excluded" for k in a["1"])
    assert not any(k.startswith("v2_") for k in body["totals"])


def test_every_active_pending_or_manual_referral_is_in_the_population_and_one_state_each():
    w = disabled_world()
    configure_disabled(w)
    feed = FakeFeed()
    for uid, status in enumerate(aq.ACTIVE_PENDING_STATUSES, start=100):
        w.join(1, uid, at=CUTOFF + timedelta(days=2), status=status)
    w.join(1, 200, at=CUTOFF + timedelta(days=2), status="awarded")          # not active: not in the population
    w.join(1, 201, at=CUTOFF + timedelta(days=2), status="revoked")
    feed.add_batch([invitee(w, feed, 2, 300, account="AcctX")])
    sync(w, feed)
    snap(w)
    body = report(w)
    total = sum(body["totals"][PREVIEW[k]] for k in ("qualified", "pending", "rejected", "duplicate"))
    assert total == len(aq.ACTIVE_PENDING_STATUSES) + 1                          # + invitee 300 (status pending)
    assert by_affiliate(body)["1"][PREVIEW["pending"]] == len(aq.ACTIVE_PENDING_STATUSES)
    assert w.db.pending_referrals.distinct("invitee_user_id").__len__() == total + 2


def test_precedence_qualified_over_duplicate_over_rejected_over_pending(monkeypatch):
    # One invitee with a rejected AND a held observation, one with a qualifying one and a failing one.
    w = disabled_world()
    configure_disabled(w)
    feed = FakeFeed()
    base = invitee(w, feed, 1, 11, account="A11", fields={"after_total_bet_amount": "50"})          # rejected
    feed.add_batch([base])
    sync(w, feed)
    snap(w)
    body = report(w)
    assert counts(by_affiliate(body)["1"], PREVIEW) == [0, 0, 1, 0]
    # Force several outcomes for one invitee through the shared replay and check the chosen state.
    outcomes = [
        {"invitee_id": 11, "status": aq.EV_REVIEW, "after_total_bet": None},
        {"invitee_id": 11, "status": aq.EV_REJECTED, "after_total_bet": "10"},
        {"invitee_id": 11, "status": aq.EV_DUPLICATE_ACCOUNT, "after_total_bet": "90"},
        {"invitee_id": 11, "status": aq.EV_QUALIFIED, "after_total_bet": "85.5"},
    ]
    real = aqp._replay

    def fake_replay(*a, **k):
        out = real(*a, **k)
        out["at_launch"] = outcomes
        return out

    monkeypatch.setattr(aqp, "_replay", fake_replay)
    snap(w, now=NOW + timedelta(minutes=11))
    row = by_affiliate(report(w, now=NOW + timedelta(minutes=11)))["1"]
    assert counts(row, PREVIEW) == [1, 0, 0, 0] and row[PREVIEW["turnover"]] == "85.5"
    monkeypatch.setattr(aqp, "_replay", lambda *a, **k: {**real(*a, **k), "at_launch": outcomes[:3]})
    snap(w, now=NOW + timedelta(minutes=22))
    assert counts(by_affiliate(report(w, now=NOW + timedelta(minutes=22)))["1"], PREVIEW) == [0, 0, 0, 1]
    monkeypatch.setattr(aqp, "_replay", lambda *a, **k: {**real(*a, **k), "at_launch": outcomes[:2]})
    snap(w, now=NOW + timedelta(minutes=33))
    assert counts(by_affiliate(report(w, now=NOW + timedelta(minutes=33)))["1"], PREVIEW) == [0, 0, 1, 0]
    monkeypatch.setattr(aqp, "_replay", lambda *a, **k: {**real(*a, **k), "at_launch": outcomes[:1]})
    snap(w, now=NOW + timedelta(minutes=44))
    assert counts(by_affiliate(report(w, now=NOW + timedelta(minutes=44)))["1"], PREVIEW) == [0, 1, 0, 0]


@pytest.mark.parametrize("mode,cutoff", [(aq.MODE_DISABLED, None), (aq.MODE_ACTIVE, NOW + timedelta(days=3))])
def test_not_yet_launched_is_preview_including_a_scheduled_future_cutoff(mode, cutoff):
    w = disabled_world()
    activate(w, mode=mode, cutoff=cutoff)
    w.join(1, 11, at=CUTOFF + timedelta(days=1))
    snap(w)
    body = report(w)
    assert (body["report_mode"], body["rule_active"], body["effects_written"]) == ("preview", False, False)
    assert by_affiliate(body)["1"][PREVIEW["pending"]] == 1


# --- parity with the live evaluator -------------------------------------------------

def _parity_world(case):
    w = disabled_world()
    configure_disabled(w)
    feed = FakeFeed()
    rows = []
    if case == "79.99":
        rows.append(invitee(w, feed, 1, 11, account="A11", fields={"after_total_bet_amount": "79.99"}))
    elif case == "80.00":
        rows.append(invitee(w, feed, 1, 11, account="A11", fields={"after_total_bet_amount": "80.00"}))
    elif case == "new_player_false":
        rows.append(invitee(w, feed, 1, 11, account="A11", fields={"coupon_new_player": "0", "after_total_bet_amount": "900"}))
    elif case == "incomplete":
        rows.append(invitee(w, feed, 1, 11, account="A11", fields={"seven_day_complete": False, "after_total_bet_amount": "900"}))
        rows.append(invitee(w, feed, 1, 12, account="A12", fields={"after_total_bet_amount": None}))
        invitee(w, feed, 1, 13)
    elif case == "duplicate":
        rows.append(invitee(w, feed, 1, 11, account="SameAcct", fields={"after_total_bet_amount": "100"}))
        rows.append(invitee(w, feed, 2, 12, account="SameAcct", wall="2026-10-06 10:00:00",
                            fields={"after_total_bet_amount": "100"}))
    elif case == "decimal":
        for i, amount in enumerate(["80.10", "80.20", "80.0000000000000000000000001"]):
            rows.append(invitee(w, feed, 1, 11 + i, account=f"D{i}", fields={"after_total_bet_amount": amount}))
    elif case == "feed_failure":
        rows.append(invitee(w, feed, 1, 11, account="A11", fields={"after_total_bet_amount": "500"}))
    feed.add_batch([r for r in rows if r])
    sync(w, feed)
    if case == "feed_failure":
        feed.fail_after = feed.calls        # the next sync attempt fails after a good one: evidence now unverified
        assert sync(w, feed, now=NOW + timedelta(minutes=1)).get("error")
    return w


@pytest.mark.parametrize("case", ["79.99", "80.00", "new_player_false", "incomplete", "feed_failure", "duplicate", "decimal"])
def test_preview_equals_what_live_processing_then_decides(case, monkeypatch):
    w = _parity_world(case)
    when = NOW + timedelta(minutes=1)
    snap(w, now=when)
    preview = report(w, now=when)
    assert preview["report_mode"] == "preview"

    # Activate the SAME world and let the real pipeline decide.
    activate(w)
    run(w, monkeypatch, now=when)
    snap(w, now=when + timedelta(minutes=11))
    live = report(w, now=when + timedelta(minutes=11))
    assert live["report_mode"] == "live" and live["effects_written"] is True

    for aid, p in by_affiliate(preview).items():
        l = by_affiliate(live)[aid]
        assert counts(p, PREVIEW) == counts(l, LIVE), (case, aid)
        assert p[PREVIEW["turnover"]] == l[LIVE["turnover"]], (case, aid)
    assert counts(preview["totals"], PREVIEW) == counts(live["totals"], LIVE)
    assert preview["totals"][PREVIEW["turnover"]] == live["totals"][LIVE["turnover"]]

    expected = {"79.99": [0, 0, 1, 0], "80.00": [1, 0, 0, 0], "new_player_false": [0, 0, 1, 0],
                "incomplete": [0, 3, 0, 0], "feed_failure": [0, 1, 0, 0], "duplicate": [1, 0, 0, 1],
                "decimal": [3, 0, 0, 0]}[case]
    assert counts(preview["totals"], PREVIEW) == expected
    if case == "decimal":
        assert preview["totals"][PREVIEW["turnover"]] == str(
            Decimal("80.10") + Decimal("80.20") + Decimal("80.0000000000000000000000001"))


def test_stale_or_unverified_source_keeps_everyone_pending_even_when_data_would_pass():
    w, _ = preview_scenario()
    later = NOW + timedelta(hours=3)                   # sync older than 60 min: evidence_sync_stale
    snap(w, now=later)
    body = report(w, now=later)
    assert body["source"]["status"] == rep.SOURCE_STALE and "evidence_sync_stale" in body["source"]["problems"]
    assert counts(body["totals"], PREVIEW) == [0, 7, 0, 0]       # held, exactly like live processing
    assert body["totals"][PREVIEW["tstatus"]] != rep.TURNOVER_COMPLETE


def test_unconfigured_source_is_unavailable_and_everyone_pending():
    w = disabled_world()
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$unset": {"source_config": ""}})
    w.join(1, 11, at=CUTOFF + timedelta(days=1))
    snap(w)
    body = report(w)
    assert body["source"]["status"] == rep.SOURCE_UNAVAILABLE
    assert counts(body["totals"], PREVIEW) == [0, 1, 0, 0]
    assert body["totals"][PREVIEW["turnover"]] is None


def test_active_pending_cap_truncation_holds_everyone_and_says_partial(monkeypatch):
    w, _ = preview_scenario()
    monkeypatch.setenv("AFFILIATE_V2_REPORT_MAX_ACTIVE_PENDING", "2")
    snap(w)
    body = report(w)
    assert body["status"] == "partial" and counts(body["totals"], PREVIEW)[0] == 0
    assert body["totals"][PREVIEW["tstatus"]] == rep.TURNOVER_PARTIAL


# --- read-only guarantees -----------------------------------------------------------

def test_preview_build_writes_nothing_outside_its_two_derived_collections():
    w, _ = preview_scenario()
    before = {k: v for k, v in _snapshot(w.db).items() if k not in OWN}
    snap(w)
    after = {k: v for k, v in _snapshot(w.db).items() if k not in OWN}
    assert after == before
    for name in ("xp_events", "referral_events", "referral_award_events", aq.REGISTRY_COLLECTION, aq.EVIDENCE_COLLECTION):
        assert w.db[name].count_documents({}) == 0
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 0
    assert w.db.affiliate_ledger.count_documents({"ledger_type": {"$ne": "WELCOME"}}) == 0
    control = aq.get_control(w.db)
    assert control["mode"] == aq.MODE_DISABLED and "launch_cutoff_utc" not in control
    assert {r["status"] for r in w.db.pending_referrals.find()} == {"pending", "awarded"}   # no status flipped


def test_build_phase_handle_is_structurally_read_only(monkeypatch):
    w, _ = preview_scenario()
    seen = []
    real = rep.build_preview_rows

    def spy(db, **kw):
        seen.append(db)
        for attempt in (lambda: db.qualified_events.insert_one({"x": 1}),
                        lambda: db.pending_referrals.update_many({}, {"$set": {"status": "rejected"}}),
                        lambda: db[aq.REGISTRY_COLLECTION].delete_many({}),
                        lambda: db.xp_events.insert_one({"x": 1}),
                        lambda: db.affiliate_ledger.update_one({}, {"$set": {"x": 1}}, upsert=True),
                        lambda: db[rep.ROWS_COLLECTION].insert_one({"x": 1})):
            with pytest.raises(PermissionError):
                attempt()
        return real(db, **kw)

    monkeypatch.setattr(rep, "build_preview_rows", spy)
    snap(w)
    assert isinstance(seen[0], rep.ReadOnlyDb)


def test_preview_is_never_computed_inside_a_request(monkeypatch):
    w, _ = preview_scenario()
    snap(w)
    for target in ("_replay", "build_preview", "_simulate", "_load_observations"):
        monkeypatch.setattr(aqp, target, lambda *a, **k: pytest.fail("preview ran on the read path"))
    monkeypatch.setattr(rep, "build_preview_rows", lambda *a, **k: pytest.fail("preview rebuilt on the read path"))
    monkeypatch.setattr(rep, "build_rows", lambda *a, **k: pytest.fail("rebuilt on the read path"))
    before = _snapshot(w.db)
    body = rep.build_report(_ReadOnlyDb(w.db), start_date=START, end_date=END, now_utc=NOW)
    assert body["report_mode"] == "preview" and body["totals"][PREVIEW["qualified"]] == 2
    assert _snapshot(w.db) == before


def test_no_raw_codes_accounts_or_ids_beyond_affiliate_ids_in_preview_data():
    w, _ = preview_scenario()
    snap(w)
    published = json.dumps([report(w), list(w.db[rep.ROWS_COLLECTION].find({}, {"_id": 0})),
                            w.db[rep.STATE_COLLECTION].find_one({}, {"_id": 0})], default=str)
    for secret in ("CODE11", "CODE12", "000AcctEleven", "AcctTwelve", "AcctThirteen", "AcctTwentyOne", "advantplay",
                   "uim_global:"):
        assert secret not in published


# --- preview vs actual, activation, audit ------------------------------------------

def test_activation_switches_mode_without_ever_serving_preview_as_actual(monkeypatch):
    w, _ = preview_scenario()
    snap(w)
    assert report(w)["report_mode"] == "preview"
    activate(w)
    run(w, monkeypatch)
    before_refresh = report(w)                                # preview snapshot is still the stored one
    assert before_refresh["report_mode"] == "live" and before_refresh["status"] == "no_snapshot"
    assert before_refresh["totals"] is None and before_refresh["affiliates"] is None
    snap(w, now=NOW + timedelta(minutes=11))
    live = report(w, now=NOW + timedelta(minutes=11))
    assert live["report_mode"] == "live" and live["rule_active"] is True and live["effects_written"] is True
    assert counts(live["totals"], LIVE) == [2, 2, 2, 1]
    assert not any(k in live["totals"] for k in PREVIEW.values() if k not in LIVE.values())


def test_final_preview_is_preserved_as_an_audit_snapshot_at_activation(monkeypatch):
    w, _ = preview_scenario()
    snap(w)
    final_preview = rep.refresh_snapshot(w.db, now_utc=NOW + timedelta(minutes=11))["generation"]
    activate(w)
    run(w, monkeypatch, now=NOW + timedelta(minutes=12))
    live_at = NOW + timedelta(minutes=13)
    snap(w, now=live_at)
    state = w.db[rep.STATE_COLLECTION].find_one({"_id": rep.STATE_ID})
    audit = state["preview_audit"]
    assert audit["kind"] == "pre_launch_preview" and audit["generation"] == final_preview
    assert audit["totals"][PREVIEW["qualified"]] == 2 and audit["totals"][PREVIEW["turnover"]] == "203.456"
    kept = list(w.db[rep.ROWS_COLLECTION].find({"generation": final_preview}))
    assert kept and {r["mode"] for r in kept} == {"preview"}               # rows survive, labelled preview
    assert set(w.db[rep.ROWS_COLLECTION].distinct("generation")) == {final_preview, state["generation"]}
    live = report(w, now=live_at)
    assert live["pre_launch_preview_audit"]["generation"] == final_preview
    assert live["pre_launch_preview_audit"]["totals"][PREVIEW["qualified"]] == 2
    # Later live refreshes keep it and keep replacing only the live generation.
    snap(w, now=live_at + timedelta(minutes=11))
    assert w.db[rep.STATE_COLLECTION].find_one({})["preview_audit"]["generation"] == final_preview
    assert len(w.db[rep.ROWS_COLLECTION].distinct("generation")) == 2
    # And the audit generation is never served as the live table.
    assert report(w, now=live_at + timedelta(minutes=11))["snapshot"]["generation"] != final_preview


def test_scheduler_refreshes_immediately_when_the_rule_state_changes(monkeypatch):
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    w, _ = preview_scenario()
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW)["report_mode"] == "preview"
    assert rep.run_scheduled_refresh(w.db, now_utc=NOW + timedelta(minutes=5))["skipped"] == "fresh"
    activate(w)
    out = rep.run_scheduled_refresh(w.db, now_utc=NOW + timedelta(minutes=6))
    assert out["ok"] and out["report_mode"] == "live"          # not "fresh": the mode changed


def test_failed_preview_refresh_keeps_last_good_preview(monkeypatch):
    w, _ = preview_scenario()
    good = snap(w)
    monkeypatch.setattr(rep, "build_preview_rows", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert rep.refresh_snapshot(w.db, now_utc=NOW + timedelta(minutes=11))["ok"] is False
    body = report(w, now=NOW + timedelta(minutes=11))
    assert body["snapshot"]["generation"] == good["generation"] and body["totals"][PREVIEW["qualified"]] == 2
