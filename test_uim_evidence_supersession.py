"""A newly committed, fully attested Databot UIM observation supersedes older unverified evidence of the same
logical record (coupon code + account); genuine disagreements stay review; the repair command is bounded,
dry-run by default and idempotent. See uim_evidence_reconcile.py and affiliate_qualification.reobservation_verdict.

Decisions always come from the unchanged validator (assess_evidence): nothing here qualifies or rejects anyone.
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

import affiliate_qualification as aq
import uim_evidence_reconcile as rec
from scripts import affiliate_qualification_admin as admin_cli
from test_affiliate_welcome_redemption_qualification import CUTOFF, KL, NOW, World, _ReadOnlyDb, _snapshot  # noqa: F401
from test_uim_redemption_sync import (  # noqa: F401  (_scheduler is an autouse fixture)
    DATABOT_CONFIG, FakeFeed, _scheduler, coverage_block, disabled_world, preview, sync, urow,
)

FINAL = ("2026-08-31", "2026-10-04")           # the final batch's operator-declared voucher_redemption_date coverage
WALL = "2026-10-03 12:00:00"                    # GMT+8 wall clock -> 04:00Z


def final_cov():
    return coverage_block(*FINAL)


def world(n=1, *, active=False):
    w = World(mode=aq.MODE_ACTIVE, cutoff=CUTOFF) if active else disabled_world()
    if active:
        w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"source_config": dict(DATABOT_CONFIG)}})
    for invitee in range(11, 11 + n):
        w.join(1, invitee)
        w.welcome_code(invitee, f"WELC{invitee}")
    return w


def row(code="WELC11", account="0AbC1", wall=WALL, *, amount="80", flag="1", **kw):
    r = urow(code, account, wall, **kw)
    r["after_total_bet_amount"], r["coupon_new_player"] = amount, flag
    return r


def legacy(**kw):
    """What the older batch carried: redemption time without timezone provenance."""
    return row(basis="unknown_legacy", **kw)


def extract(w):
    return aq.extract_committed_batches(w.db, config=DATABOT_CONFIG, now_utc=NOW)


def evidence(w):
    return list(w.db[aq.EVIDENCE_COLLECTION].find())


def hist(out):
    return out["evidence_summary"]


def reasons(out):
    return hist(out)["review_reasons"]


def verdicts(w):
    """Verdict the production validator gives each stored evidence document (no writes)."""
    control = {**aq.get_control(w.db), "source_config": DATABOT_CONFIG}
    return [aq.assess_evidence(e, control=control, now_utc=NOW, lookup=aq.DbLookup(w.db)) for e in evidence(w)]


# 1 ---------------------------------------------------------------------------

def test_old_unverified_plus_identical_attested_becomes_usable_in_preview_and_live():
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([legacy()])
    sync(w, feed)
    extract(w)                                    # the old evidence exists first (unverified)
    [old] = evidence(w)
    assert old["redeemed_at"] is None and old["redeemed_at_reason"] == "redeemed_at_timezone_unverified"
    assert verdicts(w)[0]["reason"] == "redeemed_at_timezone_unverified"

    feed.add_batch([row()], coverage=final_cov())
    sync(w, feed)
    out = preview(w.db, month="202610")
    assert reasons(out) == {} and out["totals"]["historical_would_qualify"] == 1
    assert out["totals"]["at_launch_eligible"] == 1

    extract(w)
    [ev] = evidence(w)
    assert ev["_id"] == old["_id"] and ev["source_ref"] == old["source_ref"]          # same logical record
    assert ev["redeemed_at"] == datetime(2026, 10, 3, 4, 0) and ev["redeemed_at_reason"] is None
    assert ev["status"] == aq.EV_RECEIVED and "source_conflict_at" not in ev
    assert ev["evidence_basis"]["coverage_attested"] is True and ev["authoritative_order"] > old["authoritative_order"]
    [h] = ev["supersession_history"]                                                  # audit: what was replaced
    assert h["from"]["redeemed_at"] is None and h["from"]["redeemed_at_reason"] == "redeemed_at_timezone_unverified"
    assert h["verdict"] == aq.REOBS_UPGRADE and len(ev["source_batch_ids"]) == 2
    # The unchanged validator now decides - and the live evaluator qualifies it.
    control = {**aq.get_control(w.db), "source_config": DATABOT_CONFIG}
    assert aq.evaluate_evidence(w.db, ev, control=control, now_utc=NOW)["status"] == aq.EV_QUALIFIED
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 1


def test_old_row_without_completeness_marker_is_superseded_too():
    w = world()
    feed = FakeFeed()
    feed.add_batch([row()])                                      # verified time but pre-transport: no completeness
    sync(w, feed)
    w.db[usync_rows()].update_many({}, {"$unset": {"seven_day_completeness_retained_version": ""}})
    assert preview(w.db, month="202610")["evidence_summary"]["review_reasons"] == {"seven_day_data_incomplete": 1}
    feed.add_batch([row()], coverage=final_cov())
    sync(w, feed)
    out = preview(w.db, month="202610")
    assert reasons(out) == {} and out["totals"]["historical_would_qualify"] == 1


def usync_rows():
    import uim_redemption_sync
    return uim_redemption_sync.ROWS_COLLECTION


# 2 ---------------------------------------------------------------------------

def test_duplicate_upload_does_not_conflict_or_multiply_turnover():
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([row(amount="123.45")], coverage=final_cov())
    feed.add_batch([row(amount="123.45")], coverage=final_cov())      # identical re-upload
    feed.add_batch([row(amount="123.450")], coverage=final_cov())     # same Decimal, different text
    sync(w, feed)
    extract(w)
    [ev] = evidence(w)                                                  # one logical record, one evidence document
    assert ev["status"] == aq.EV_RECEIVED and "source_conflict_at" not in ev and len(ev["source_batch_ids"]) == 3
    out = preview(w.db, month="202610")
    assert reasons(out) == {} and out["totals"]["historical_would_qualify"] == 1
    control = {**aq.get_control(w.db), "source_config": DATABOT_CONFIG}
    assert aq.evaluate_evidence(w.db, ev, control=control, now_utc=NOW)["status"] == aq.EV_QUALIFIED
    assert w.db.qualified_events.count_documents({}) == 1
    q = w.db.qualified_events.find_one()
    assert aq._amount_value(w.db[aq.EVIDENCE_COLLECTION].find_one({"_id": q["evidence_id"]})
                            ["qualification_fields"]["after_total_bet_amount"]) == aq._amount_value("123.45")


def test_duplicate_older_unverified_does_not_block_a_newer_attested_pair():
    w = world()
    feed = FakeFeed()
    feed.add_batch([legacy()])
    feed.add_batch([legacy()])                                          # the same unverified record uploaded twice
    feed.add_batch([row()], coverage=final_cov())
    feed.add_batch([row()], coverage=final_cov())
    sync(w, feed)
    out = preview(w.db, month="202610")
    assert reasons(out) == {} and out["totals"]["historical_would_qualify"] == 1


# 3 / 4 -----------------------------------------------------------------------

@pytest.mark.parametrize("amount,flag,expect", [
    ("79.99", "1", "rejected"), ("80.00", "1", "qualified"), ("80", "1", "qualified"),
    ("1000", "0", "rejected"), ("80", "false", "rejected"),
])
def test_existing_validator_decides_after_supersession(amount, flag, expect):
    w = world()
    feed = FakeFeed()
    feed.add_batch([legacy(amount=amount, flag=flag)])
    feed.add_batch([row(amount=amount, flag=flag)], coverage=final_cov())
    sync(w, feed)
    out = preview(w.db, month="202610")
    t = out["totals"]
    assert reasons(out) == {}
    assert (t["historical_would_qualify"] == 1) == (expect == "qualified")
    assert (t["historical_rejected"] if "historical_rejected" in t else 1 - t["historical_would_qualify"]) == (expect == "rejected")
    # exact reason from the validator: 79.99 -> turnover gate, coupon_new_player=0 -> not a new player
    w2 = world(active=True)
    f2 = FakeFeed()
    f2.add_batch([row(amount=amount, flag=flag)], coverage=final_cov())
    sync(w2, f2)
    extract(w2)
    [v] = verdicts(w2)
    if expect == "qualified":
        assert v["status"] == aq.ELIGIBLE
    else:
        assert v["status"] == aq.EV_REJECTED
        assert v["reason"] == ("not_a_new_coupon_player" if flag in ("0", "false") else "seven_day_turnover_below_80")


# 5 ---------------------------------------------------------------------------

@pytest.mark.parametrize("old,new,old_time_established", [
    (dict(amount="79.99"), dict(amount="80.00"), False),                           # turnover
    (dict(flag="0"), dict(flag="1"), False),                                       # coupon_new_player
    (dict(wall="2026-10-03 11:00:00"), dict(wall="2026-10-03 12:00:00"), True),    # redemption instant
])
def test_genuine_disagreement_with_established_fact_stays_conflict(old, new, old_time_established):
    w = world(active=True)
    feed = FakeFeed()
    # An unknown_legacy time establishes nothing; a timezone-bearing one does (so two instants can disagree).
    feed.add_batch([(row if old_time_established else legacy)(**old)])
    sync(w, feed)
    extract(w)
    feed.add_batch([row(**new)], coverage=final_cov())
    sync(w, feed)
    assert reasons(preview(w.db, month="202610")) == {"conflicting_source_observations": 1}
    extract(w)
    [ev] = evidence(w)
    assert ev["status"] == aq.EV_REVIEW and ev["source_conflict_reason"] == "conflicting_source_observations"
    assert "supersession_history" not in ev
    assert (ev["redeemed_at"] is not None) == old_time_established
    assert verdicts(w)[0]["status"] == aq.EV_REVIEW


def test_disagreeing_account_stays_review_not_merged():
    w = world()
    feed = FakeFeed()
    feed.add_batch([legacy(account="ACCT-ONE")])
    feed.add_batch([row(account="ACCT-TWO")], coverage=final_cov())      # a different account redeemed the same code
    sync(w, feed)
    out = preview(w.db, month="202610")
    assert out["totals"]["historical_would_qualify"] == 0 and out["totals"]["at_launch_eligible"] == 0
    assert reasons(out) == {"conflicting_redemption_records": 2} or reasons(out).get("conflicting_redemption_records")


# 6 ---------------------------------------------------------------------------

def test_corrected_latest_observation_is_deterministic():
    feed = FakeFeed()
    feed.add_batch([row(amount="79.99")], coverage=final_cov())
    feed.add_batch([row(amount="80.00")], coverage=final_cov())           # operator re-uploaded a corrected report
    results = []
    for _ in range(2):                                                     # same input, same output, every time
        w = world(active=True)
        sync(w, feed)
        results.append((preview(w.db, month="202610")["totals"]["historical_would_qualify"], reasons(preview(w.db, month="202610"))))
        extract(w)
        [ev] = evidence(w)
        assert ev["qualification_fields"]["after_total_bet_amount"] == "80.00" and ev["status"] == aq.EV_RECEIVED
    assert results[0] == results[1] == (1, {})

    # After a decision the evidence is never rewritten: the later correction is flagged for manual reconciliation.
    w = world(active=True)
    f2 = FakeFeed()
    f2.add_batch([row(amount="79.99")], coverage=final_cov())
    sync(w, f2)
    extract(w)
    control = {**aq.get_control(w.db), "source_config": DATABOT_CONFIG}
    assert aq.evaluate_evidence(w.db, evidence(w)[0], control=control, now_utc=NOW)["status"] == aq.EV_REJECTED
    f2.add_batch([row(amount="80.00")], coverage=final_cov())
    sync(w, f2)
    extract(w)
    [ev] = evidence(w)
    assert ev["status"] == aq.EV_REJECTED and ev["qualification_fields"]["after_total_bet_amount"] == "79.99"
    assert ev["source_conflict_reason"] == "conflicting_source_observations"


# 7 ---------------------------------------------------------------------------

def test_timezone_conversion_and_coverage_boundaries_use_asia_kuala_lumpur():
    # 23:59:59+08:00 on the last declared day is covered; 00:00:00+08:00 the next day is not.
    last = datetime.strptime("2026-10-04 23:59:59", "%Y-%m-%d %H:%M:%S")
    assert aq.batch_attests_redemption(final_cov(), KL.localize(last), "Asia/Kuala_Lumpur")
    assert not aq.batch_attests_redemption(final_cov(), KL.localize(last + timedelta(seconds=1)), "Asia/Kuala_Lumpur")
    assert aq.batch_attests_redemption(final_cov(), KL.localize(datetime(2026, 8, 31, 0, 0, 0)), "Asia/Kuala_Lumpur")
    assert not aq.batch_attests_redemption(final_cov(), KL.localize(datetime(2026, 8, 30, 23, 59, 59)), "Asia/Kuala_Lumpur")
    assert not aq.batch_attests_redemption(final_cov(), KL.localize(last), "UTC")            # other source timezone
    for bad in (None, {}, coverage_block(*FINAL, status="partial"), coverage_block(*FINAL, status="inferred_unverified",
                source="observed_rows", basis="unverified"), coverage_block(*FINAL, basis="referral_join_date"),
                coverage_block(*FINAL, undeclared_legacy=True)):
        assert not aq.batch_attests_redemption(bad, KL.localize(last), "Asia/Kuala_Lumpur")

    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([row(wall="2026-10-04 23:59:59")], coverage=final_cov())
    sync(w, feed)
    extract(w)
    [ev] = evidence(w)
    assert ev["redeemed_at"] == datetime(2026, 10, 4, 15, 59, 59)       # 23:59:59 GMT+8 == 15:59:59Z
    assert ev["evidence_basis"]["coverage_attested"] is True


def test_uncovered_observation_does_not_supersede_and_stays_pending():
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([legacy(wall="2026-10-05 00:00:00")])
    feed.add_batch([row(wall="2026-10-05 00:00:00")], coverage=final_cov())      # day after declared coverage
    feed.add_batch([row(wall="2026-10-05 00:00:00")], coverage=coverage_block(*FINAL, status="partial"))
    sync(w, feed)
    out = preview(w.db, month="202610")
    assert out["totals"]["historical_would_qualify"] == 0 and out["totals"]["at_launch_eligible"] == 0
    extract(w)
    [ev] = evidence(w)
    assert ev["redeemed_at"] is None and "supersession_history" not in ev and ev["status"] == aq.EV_REVIEW
    assert verdicts(w)[0]["status"] == aq.EV_REVIEW


# 8 ---------------------------------------------------------------------------

def test_removed_observation_stays_removed_even_if_a_later_batch_re_adds_it():
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([legacy()])
    feed.add_batch([row(observation="deleted")])                          # a later import dropped the row
    feed.add_batch([row()], coverage=final_cov())                         # and a still later one re-adds it
    sync(w, feed)
    out = preview(w.db, month="202610")
    assert out["totals"]["historical_would_qualify"] == 0
    assert reasons(out) == {"source_row_removed_by_later_import": 1}
    extract(w)
    [ev] = evidence(w)
    assert ev["status"] == aq.EV_REVIEW and ev["source_conflict_reason"] == "source_row_removed_by_later_import"
    assert ev["removed_in_batch_ids"] and "supersession_history" not in ev and ev["redeemed_at"] is None
    r = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    assert r["actions"] == {"removed_stays_removed": 1} and evidence(w)[0]["status"] == aq.EV_REVIEW


# 9 / 10 / 11 -----------------------------------------------------------------

def stuck_world(n=3):
    """Evidence written before this fix: unverified evidence flagged as a spurious conflict by a later final batch.
    V2 is not activated (the production state)."""
    w = world(n)
    feed = FakeFeed()
    feed.add_batch([legacy(code=f"WELC{11 + i}", account=f"0{i}Ac") for i in range(n)])
    sync(w, feed)
    extract(w)
    feed.add_batch([row(code=f"WELC{11 + i}", account=f"0{i}Ac") for i in range(n)], coverage=final_cov())
    sync(w, feed)
    now = NOW
    w.db[aq.EVIDENCE_COLLECTION].update_many({}, {"$set": {"source_conflict_at": now,
                                                           "source_conflict_reason": "conflicting_source_observations",
                                                           "status": aq.EV_REVIEW, "reason": "conflicting_source_observations"}})
    extract(w)                                    # new code: held, never silently clears an existing hold
    return w, feed


def protected(db):
    return {name: sorted(json.dumps(d, default=str, sort_keys=True) for d in db[name].find())
            for name in ("qualified_events", "referral_events", "affiliate_ledger", "xp_events", "user_xp_events",
                         "pending_referrals", "voucher_pools", "users", aq.REGISTRY_COLLECTION, aq.CONTROL_COLLECTION,
                         "uim_redemption_rows", "uim_redemption_batches")}


def test_command_dry_run_then_commit_is_bounded_idempotent_and_touches_only_evidence():
    w, _ = stuck_world()
    # a historical qualification that must never change
    w.db.qualified_events.insert_one({"invitee_id": 99, "referrer_id": 1, "rule_version": aq.LEGACY_RULE_VERSION,
                                      "qualified_at": NOW, "account_key": "legacy:1"})
    w.db.xp_events.insert_one({"user_id": 1, "xp": 5, "type": "referral"})
    w.db.referral_events.insert_one({"event": "referral_success", "inviter_id": 1, "invitee_id": 99})
    w.db.affiliate_ledger.insert_one({"dedup_key": "REWARD:1", "ledger_type": "AFFILIATE", "user_id": 1})
    before = protected(w.db)
    evid_before = copy.deepcopy(evidence(w))
    pre = preview(w.db, month="202610")
    assert reasons(pre) == {"conflicting_source_observations": 3}            # persisted holds block V2

    dry = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)            # dry run is the default
    assert dry["dry_run"] and dry["actions"] == {"would_repair": 3} and dry["candidates"] == 3
    assert evidence(w) == evid_before and protected(w.db) == before and w.db[rec.RUN_COLLECTION].count_documents({}) == 0
    blob = json.dumps(dry, default=str)
    for secret in ("WELC1", "0AAc", "00Ac", "01Ac", "02Ac"):                 # masked samples only
        assert secret not in blob
    assert all(s["code"] and "*" in s["code"] for s in dry["samples"]["would_repair"])

    bounded = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True, limit=2)
    assert bounded["actions"]["repaired"] == 2 and bounded["candidates"] == 2 and bounded["next_after_id"]
    done = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True, limit=500)
    assert done["actions"] == {"repaired": 1} and done["candidates"] == 1
    again = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    assert again["candidates"] == 0 and again["actions"] == {}
    assert w.db[rec.RUN_COLLECTION].count_documents({}) == 2          # one audit row per committing run that examined candidates

    for ev in evidence(w):
        assert ev["status"] == aq.EV_RECEIVED and "source_conflict_at" not in ev and ev["redeemed_at"] == datetime(2026, 10, 3, 4, 0)
        [h] = ev["supersession_history"]
        assert h["cleared_conflict_reason"] == "conflicting_source_observations" and h["from"]["redeemed_at"] is None
        old = next(e for e in evid_before if e["_id"] == ev["_id"])
        for stable in ("source_ref", "code_hash", "account_key", "recipient_resolution", "created_at"):
            assert ev[stable] == old[stable]
    # Everything outside the candidate evidence documents is byte-for-byte untouched (no reward/referral/XP change).
    assert protected(w.db) == before
    assert w.db.qualified_events.count_documents({}) == 1
    # Preview now uses the repaired evidence, and the unchanged validator qualifies all three.
    post = preview(w.db, month="202610")
    assert reasons(post) == {} and post["totals"]["historical_would_qualify"] == 3
    assert all(v["status"] == aq.ELIGIBLE for v in verdicts(w))


def test_command_preserves_account_case_leading_zeros_and_namespace():
    w, _ = stuck_world(1)
    rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    [ev] = evidence(w)
    assert ev["account_key"] == "uim_global:00Ac"
    w2 = world(1)
    f = FakeFeed()
    f.add_batch([legacy(account="00AbC1")])
    f.add_batch([row(account="00AbC1")], coverage=final_cov())
    sync(w2, f)
    preview(w2.db, month="202610")
    extract(w2)
    [ev2] = evidence(w2)
    assert ev2["account_key"] == "uim_global:00AbC1" and ev2["account_key_masked"].startswith("ui")


def test_command_never_repairs_conflicts_decided_or_effectful_evidence():
    w = world(3, active=True)
    feed = FakeFeed()
    feed.add_batch([legacy(code=f"WELC{11 + i}", account=f"a{i}", amount="79.99") for i in range(3)])
    sync(w, feed)
    extract(w)
    feed.add_batch([row(code="WELC11", account="a0", amount="80"),                 # genuine disagreement
                    row(code="WELC12", account="a1", amount="79.99"),              # agrees, but evidence is decided below
                    row(code="WELC13", account="a2", amount="79.99", wall="2026-10-05 12:00:00")],  # uncovered
                   coverage=final_cov())
    sync(w, feed)
    by_code = {e["code_masked"]: e for e in evidence(w)}
    decided = next(e for e in evidence(w) if e["recipient_resolution"]["user_id"] == 12)
    w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": decided["_id"]}, {"$set": {"status": aq.EV_REJECTED}})
    r = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    assert r["actions"].get("repaired") is None
    assert r["actions"] == {"conflict_kept_for_review": 2} and r["evidence_not_repairable_state"] == 1
    assert next(e for e in evidence(w) if e["_id"] == decided["_id"]).get("supersession_history") is None
    # an effectful (qualified-event-backed) evidence document is skipped as well
    w.db.qualified_events.insert_one({"invitee_id": 11, "evidence_id": by_code[next(iter(by_code))]["_id"]})
    assert rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)["actions"].get("skipped_has_effects") == 1


def test_command_scan_cap_aborts_without_writing():
    w, _ = stuck_world(2)
    before = evidence(w)
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True, max_scan_rows=1)
    assert out["ok"] is False and out["error"] == "scan_exceeds_max_scan_rows"
    assert evidence(w) == before and w.db[rec.RUN_COLLECTION].count_documents({}) == 0


# CLI ---------------------------------------------------------------------------

def test_cli_is_dry_run_by_default_and_commit_is_explicit(capsys):
    w, _ = stuck_world(2)
    ro = lambda: _ReadOnlyDb(w.db)                                                # noqa: E731
    args = ["reconcile-uim-evidence", "--limit", "10"]
    assert admin_cli.main(args, db_factory=lambda: w.db, read_only_db_factory=ro, now_fn=lambda: NOW) == 0
    assert json.loads(capsys.readouterr().out)["dry_run"] is True
    assert all(e["status"] == aq.EV_REVIEW for e in evidence(w))
    assert admin_cli.main(args + ["--limit", "0"], db_factory=lambda: w.db, read_only_db_factory=ro, now_fn=lambda: NOW) == 2
    capsys.readouterr()
    assert admin_cli.main(args + ["--commit"], db_factory=lambda: w.db, read_only_db_factory=ro, now_fn=lambda: NOW) == 0
    assert json.loads(capsys.readouterr().out)["actions"]["repaired"] == 2
    assert all(e["status"] == aq.EV_RECEIVED for e in evidence(w))


def test_cli_refuses_to_commit_once_v2_is_active(capsys):
    w, _ = stuck_world(1)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID}, {"$set": {"mode": aq.MODE_ACTIVE,
                                            "launch_cutoff_utc": NOW - timedelta(days=1)}})
    rc = admin_cli.main(["reconcile-uim-evidence", "--commit"], db_factory=lambda: w.db,
                        read_only_db_factory=lambda: _ReadOnlyDb(w.db), now_fn=lambda: NOW)
    assert rc == 2 and evidence(w)[0]["status"] == aq.EV_REVIEW


def test_audit_entry_is_enough_to_restore_the_replaced_evidence():
    """Rollback contract (docs/uim_evidence_supersession.md): the last supersession_history entry restores the exact
    pre-repair provenance, including the status/reason and the lifted hold."""
    w, _ = stuck_world(1)
    [old] = evidence(w)
    rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    [new] = evidence(w)
    entry = new["supersession_history"][-1]
    restore = {k: v for k, v in entry["from"].items()}
    restore.update(source_conflict_at=entry["cleared_conflict_at"], source_conflict_reason=entry["cleared_conflict_reason"])
    restored = {**new, **restore}
    for k in ("redeemed_at", "redeemed_at_reason", "qualification_fields", "evidence_basis", "authoritative_order",
              "observation_fingerprint", "status", "reason", "source_conflict_at", "source_conflict_reason"):
        assert restored[k] == old[k], k
    assert verdicts_for(w, restored)["reason"] == "conflicting_source_observations"


def verdicts_for(w, ev):
    control = {**aq.get_control(w.db), "source_config": DATABOT_CONFIG}
    return aq.assess_evidence(ev, control=control, now_utc=NOW, lookup=aq.DbLookup(w.db))


# Production shape: pending_review, redeemed_at=None ------------------------------------------------------------

def _prod_shape_world():
    """One logical record: old unverified observation evaluated into pending_review, then a newer attested final batch."""
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([legacy()])
    sync(w, feed)
    extract(w)
    [old] = evidence(w)
    aq._finalize(w.db, old, aq.EV_REVIEW, "redeemed_at_timezone_unverified", now_utc=NOW)
    feed.add_batch([row()], coverage=final_cov())
    sync(w, feed)
    [ev] = evidence(w)
    assert ev["status"] == aq.EV_REVIEW and ev["redeemed_at"] is None and "source_conflict_at" not in ev
    return w, ev


def test_pending_review_unverified_evidence_is_selected_and_upgraded():
    w, ev = _prod_shape_world()
    dry = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert dry["ok"] and dry["dry_run"] and dry["candidates"] == 1 and dry["actions"] == {"would_repair": 1}
    assert evidence(w)[0] == ev                                               # dry run wrote nothing
    done = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    assert done["actions"] == {"repaired": 1}
    [fixed] = evidence(w)
    assert fixed["_id"] == ev["_id"] and fixed["source_ref"] == ev["source_ref"]
    assert fixed["status"] == aq.EV_RECEIVED and fixed["redeemed_at"] == datetime(2026, 10, 3, 4, 0)
    assert fixed["redeemed_at_reason"] is None and fixed["evidence_basis"]["coverage_attested"] is True
    assert fixed["supersession_history"][0]["from"]["redeemed_at_reason"] == "redeemed_at_timezone_unverified"
    control = {**aq.get_control(w.db), "source_config": DATABOT_CONFIG}
    assert aq.assess_evidence(fixed, control=control, now_utc=NOW, lookup=aq.DbLookup(w.db))["status"] != aq.EV_REVIEW


@pytest.mark.parametrize("stored", [
    pytest.param({}, id="guard-fields-absent"),
    pytest.param({"lease_until": None, "voided_at": None, "qualification_decision_at": None}, id="guard-fields-null"),
    pytest.param({"lease_until": NOW - timedelta(minutes=5)}, id="expired-lease"),
])
def test_pending_review_selected_whatever_unset_guard_fields_look_like(stored):
    """Production reported candidates=0 for exactly this record: null/expired guard fields are 'not set'."""
    w, ev = _prod_shape_world()
    w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": ev["_id"]}, {"$set": stored})
    dry = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert dry["ok"] and dry["candidates"] == 1 and dry["actions"] == {"would_repair": 1}
    assert dry["candidate_guards"]["excluded_by_guard"] == {
        "status_not_repairable": 0, "voided": 0, "active_lease": 0, "qualification_decision_recorded": 0}
    assert rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)["actions"] == {"repaired": 1}
    [fixed] = evidence(w)
    assert fixed["status"] == aq.EV_RECEIVED and fixed["redeemed_at"] == datetime(2026, 10, 3, 4, 0)


@pytest.mark.parametrize("protect, guard, expect_status", [
    ({"status": aq.EV_QUALIFIED}, "status_not_repairable", aq.EV_QUALIFIED),
    ({"status": aq.EV_REJECTED}, "status_not_repairable", aq.EV_REJECTED),
    ({"status": aq.EV_VOIDED, "voided_at": NOW}, "status_not_repairable", aq.EV_VOIDED),
    ({"voided_at": NOW}, "voided", aq.EV_REVIEW),
    ({"lease_until": NOW + timedelta(minutes=5)}, "active_lease", aq.EV_REVIEW),
    ({"qualification_decision_at": NOW}, "qualification_decision_recorded", aq.EV_REVIEW),
])
def test_final_or_in_flight_evidence_stays_protected_and_is_counted_per_guard(protect, guard, expect_status):
    w, ev = _prod_shape_world()
    w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": ev["_id"]}, {"$set": protect})
    before = evidence(w)
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    assert out["candidates"] == 0 and out["candidate_guards"]["excluded_by_guard"][guard] == 1
    assert out["candidate_guards"]["scope_documents"] == 1
    # the preview folds the attested observation, reports no legacy reason, so the exclusion is legitimate (ok)
    assert out["ok"] is True and out["actions"] == {}
    assert evidence(w) == before and evidence(w)[0]["status"] == expect_status
    assert w.db[rec.RUN_COLLECTION].count_documents({}) == 0


def test_registry_or_qualified_event_ownership_still_protects_pending_review_evidence():
    w, ev = _prod_shape_world()
    w.db[aq.REGISTRY_COLLECTION].insert_one({"evidence_id": ev["_id"], "state": aq.REG_RESERVED})
    before = evidence(w)
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    assert out["actions"] == {"skipped_has_effects": 1} and evidence(w) == before
    w.db[aq.REGISTRY_COLLECTION].delete_many({})
    w.db.qualified_events.insert_one({"invitee_id": 11, "evidence_id": ev["_id"]})
    assert rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)["actions"] == {"skipped_has_effects": 1}
    assert evidence(w) == before


def test_zero_candidates_with_preview_legacy_reasons_is_refused_not_ok(capsys):
    """Preview sees an unverified record but the evidence collection holds nothing for it (or it sits outside the
    source/rule_version scope): ok=true/candidates=0 would hide the contradiction."""
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([legacy()])
    sync(w, feed)                                                               # no extract: no evidence documents
    assert reasons(preview(w.db, month="202610")) == {"redeemed_at_timezone_unverified": 1}
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["ok"] is False and out["error"] == "candidate_selection_preview_mismatch"
    assert out["candidates"] == 0 and out["preview_repairable_legacy_reasons"] == {"redeemed_at_timezone_unverified": 1}
    assert out["candidate_guards"]["evidence_total"] == 0 and out["dry_run"] is True
    rc = admin_cli.main(["reconcile-uim-evidence"], db_factory=lambda: w.db, read_only_db_factory=lambda: w.db,
                        now_fn=lambda: NOW)
    assert rc == 1 and json.loads(capsys.readouterr().out)["error"] == "candidate_selection_preview_mismatch"


def test_evidence_outside_source_scope_is_visible_in_diagnostics_and_refused():
    w = world(active=True)
    feed = FakeFeed()
    feed.add_batch([legacy()])
    sync(w, feed)
    extract(w)
    w.db[aq.EVIDENCE_COLLECTION].update_many({}, {"$set": {"rule_version": "welcome_redemption_v1"}})
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["ok"] is False and out["error"] == "candidate_selection_preview_mismatch"
    assert out["candidate_guards"]["scope_documents"] == 0 and out["candidate_guards"]["evidence_total"] == 1
    assert out["candidate_guards"]["evidence_by_source_rule_status"] == [
        {"source": "databot_uim", "rule_version": "welcome_redemption_v1", "status": aq.EV_RECEIVED, "n": 1}]


def test_nothing_to_repair_is_still_ok_and_a_resumed_empty_page_is_not_a_mismatch():
    w = world(active=True)
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["ok"] is True and out["candidates"] == 0
    w, ev = _prod_shape_world()
    rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)
    again = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True, after_id=evidence(w)[0]["_id"])
    assert again["ok"] is True and again["candidates"] == 0


def test_repair_clears_null_lease_so_the_evaluator_picks_the_evidence_up():
    """The evaluator selects ``lease_until`` absent or < now; a surviving null would strand the repaired evidence."""
    w, ev = _prod_shape_world()
    w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": ev["_id"]}, {"$set": {"lease_until": None}})
    assert rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW, commit=True)["actions"] == {"repaired": 1}
    [fixed] = evidence(w)
    assert fixed["status"] == aq.EV_RECEIVED and "lease_until" not in fixed
    claimed = w.db[aq.EVIDENCE_COLLECTION].find_one(
        {"status": aq.EV_RECEIVED, "rule_version": aq.RULE_VERSION,
         "$or": [{"lease_until": {"$exists": False}}, {"lease_until": {"$lt": NOW}}]})
    assert claimed and claimed["_id"] == ev["_id"]


def test_unrelated_protected_evidence_cannot_mask_a_record_that_has_no_evidence():
    """Correlation is by source_ref, not by counts: one voided in-scope document must not excuse a different
    pending source record whose evidence is missing."""
    w = world(2, active=True)
    feed = FakeFeed()
    feed.add_batch([legacy(code="WELC11", account="a0"), legacy(code="WELC12", account="a1")])
    sync(w, feed)
    extract(w)
    docs = {d["recipient_resolution"]["user_id"]: d for d in evidence(w)}
    w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": docs[11]["_id"]}, {"$set": {"voided_at": NOW}})
    w.db[aq.EVIDENCE_COLLECTION].delete_one({"_id": docs[12]["_id"]})
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["candidates"] == 0 and out["candidate_guards"]["excluded_any_guard"] == 1
    assert out["ok"] is False and out["error"] == "candidate_selection_preview_mismatch"
    assert out["source_records_without_evidence"] == 1 and len(out["samples"]["source_records_without_evidence"]) == 1


def test_protected_evidence_for_the_same_record_still_explains_its_preview_reason():
    w = world(1, active=True)
    feed = FakeFeed()
    feed.add_batch([legacy()])
    sync(w, feed)
    extract(w)
    w.db[aq.EVIDENCE_COLLECTION].update_many({}, {"$set": {"voided_at": NOW}})
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["ok"] is True and out["candidates"] == 0 and out["candidate_guards"]["excluded_by_guard"]["voided"] == 1


def test_unrelated_unattested_record_without_evidence_is_not_a_preview_mismatch():
    """Only records whose preview reason is a repairable one are correlated; a record rejected for an unrelated reason
    (here: missing qualification fields) with no evidence must not turn a fully explained state into a refusal."""
    w = world(2, active=True)
    feed = FakeFeed()
    feed.add_batch([legacy(code="WELC11", account="a0"), row(code="WELC12", account="a1", amount="", flag="")])
    sync(w, feed)
    extract(w)
    docs = {d["recipient_resolution"]["user_id"]: d for d in evidence(w)}
    reasons_ = reasons(preview(w.db, month="202610"))
    assert reasons_.get("redeemed_at_timezone_unverified") and set(reasons_) - {"redeemed_at_timezone_unverified"}
    w.db[aq.EVIDENCE_COLLECTION].update_one({"_id": docs[11]["_id"]}, {"$set": {"voided_at": NOW}})
    w.db[aq.EVIDENCE_COLLECTION].delete_one({"_id": docs[12]["_id"]})
    out = rec.reconcile(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["ok"] is True and out["candidates"] == 0
